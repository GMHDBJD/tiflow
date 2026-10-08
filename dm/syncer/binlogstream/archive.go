// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogstream

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// ErrArchiveComplete means the configured end was reached exactly. An early
// EOF, missing rotation target or partial event is an error, never completion.
var ErrArchiveComplete = errors.New("local binlog archive complete")

var errArchiveRotate = errors.New("advance to next archive file")

// Match the go-mysql client's default read buffer and BinlogSyncer's default
// event cache. This aligns buffering, not the file and replication protocols.
const (
	archiveReadBufferSize  = 64 * 1024
	archiveEventCacheCount = 10240
)

type archiveResult struct {
	event *replication.BinlogEvent
	err   error
}

type archiveReader struct {
	directory     string
	end           mysql.Position
	cfg           replication.BinlogSyncerConfig
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	results       chan archiveResult
	terminalError error
}

// NewArchiveController reads a finite, immutable archive without contacting a
// database or falling back to a remote reader. Rotation filenames are confined
// to directory. Start and end must be transaction boundaries.
func NewArchiveController(directory string, end mysql.Position, cfg replication.BinlogSyncerConfig, logger log.Logger) *StreamerController {
	c := NewStreamerController(cfg, false, nil, "", nil, nil, logger)
	c.initBinlogType, c.currentBinlogType = ArchiveBinlog, ArchiveBinlog
	c.archive = &archiveReader{directory: directory, end: end, cfg: cfg}
	c.serverIDUpdated = true // no replication registration/server ID allocation
	return c
}

func (r *archiveReader) GenerateStreamFrom(location binlog.Location) (reader.Streamer, error) {
	r.Close()
	start := location.Position
	if filepath.Base(start.Name) != start.Name || !utils.VerifyFilename(start.Name) || start.Pos < 4 || start.Compare(r.end) > 0 {
		return nil, fmt.Errorf("invalid local binlog start %s (end %s)", start, r.end)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	cacheCount := r.cfg.EventCacheCount
	if cacheCount <= 0 {
		cacheCount = archiveEventCacheCount
	}
	r.results = make(chan archiveResult, cacheCount)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := r.read(ctx, start)
		r.terminalError = err
		select {
		case r.results <- archiveResult{err: err}:
		case <-ctx.Done():
		}
		close(r.results)
	}()
	return r, nil
}

func (r *archiveReader) read(ctx context.Context, start mysql.Position) error {
	if start.Compare(r.end) == 0 {
		return ErrArchiveComplete
	}
	name, offset := start.Name, start.Pos
	for {
		parser := replication.NewBinlogParser()
		parser.SetVerifyChecksum(true)
		parser.SetFlavor(r.cfg.Flavor)
		parser.SetParseTime(r.cfg.ParseTime)
		parser.SetUseDecimal(r.cfg.UseDecimal)
		parser.SetTimestampStringLocation(r.cfg.TimestampStringLocation)
		parser.SetRowsEventDecodeFunc(r.cfg.RowsEventDecodeFunc)
		next := ""
		first := true
		err := parseArchiveFile(parser, filepath.Join(r.directory, name), int64(offset), func(e *replication.BinlogEvent) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			// ParseFile emits the FDE before seeking to a nonzero start. It supplies
			// parser state, not a new position in the requested workload.
			if first && offset > 4 && e.Header.EventType == replication.FORMAT_DESCRIPTION_EVENT {
				first = false
				e.Header.LogPos = 0
				select {
				case r.results <- archiveResult{event: e}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			first = false
			pos := mysql.Position{Name: name, Pos: e.Header.LogPos}
			if pos.Compare(r.end) > 0 {
				return fmt.Errorf("local binlog end %s splits an event ending at %s", r.end, pos)
			}
			if ev, ok := e.Event.(*replication.RotateEvent); ok {
				next = string(ev.NextLogName)
				current, err := utils.ParseFilename(name)
				if err != nil {
					return err
				}
				target, err := utils.ParseFilename(next)
				if err != nil || filepath.Base(next) != next || current.BaseName != target.BaseName || target.SeqInt64 != current.SeqInt64+1 || ev.Position != 4 {
					return fmt.Errorf("invalid local binlog rotation from %s to %q", name, next)
				}
			}
			select {
			case r.results <- archiveResult{event: e}:
			case <-ctx.Done():
				return ctx.Err()
			}
			if pos.Compare(r.end) == 0 {
				return ErrArchiveComplete
			}
			if next != "" {
				return errArchiveRotate
			}
			return nil
		})
		switch {
		case errors.Is(err, ErrArchiveComplete):
			return ErrArchiveComplete
		case errors.Is(err, errArchiveRotate):
			name, offset = next, 4
		case err != nil:
			return fmt.Errorf("read local binlog %s: %w", name, err)
		default:
			return fmt.Errorf("local binlog %s ended before configured end %s: %w", name, r.end, io.ErrUnexpectedEOF)
		}
	}
}

// parseArchiveFile preserves ParseFile's format-description bootstrap when
// resuming from a nonzero offset, but buffers small header/body reads. The
// parser still owns full event decoding and checksum verification.
func parseArchiveFile(parser *replication.BinlogParser, name string, offset int64, onEvent replication.OnEventFunc) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	buffered := bufio.NewReaderSize(f, archiveReadBufferSize)
	var magic [4]byte
	if _, err = io.ReadFull(buffered, magic[:]); err != nil {
		return err
	}
	if !bytes.Equal(magic[:], replication.BinLogFileHeader) {
		return fmt.Errorf("invalid binlog file header in %s", name)
	}
	if offset > 4 {
		// Initial FDE supplies decoding state; read() marks its position as zero
		// so it cannot advance the requested workload's location.
		eof, err := parser.ParseSingleEvent(buffered, onEvent)
		if err != nil {
			return err
		}
		if eof {
			return io.ErrUnexpectedEOF
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		// A seek invalidates any prefetched bytes, including bytes beyond FDE.
		buffered.Reset(f)
	}
	return parser.ParseReader(buffered, onEvent)
}

func (r *archiveReader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	select {
	case result, ok := <-r.results:
		if !ok {
			return nil, r.terminalError
		}
		return result.event, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *archiveReader) Close() {
	if r.cancel != nil {
		r.cancel()
		r.wg.Wait()
		r.cancel = nil
	}
}
