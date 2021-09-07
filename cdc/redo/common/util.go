//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package common

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// InitS3storage init a storage used for s3,
// s3URI should be like s3URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
func InitS3storage(ctx context.Context, s3URI *url.URL) (storage.ExternalStorage, error) {
	if len(s3URI.Host) == 0 {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, errors.Errorf("please specify the bucket for s3 in %s", s3URI))
	}

	prefix := strings.Trim(s3URI.Path, "/")
	s3 := &backuppb.S3{Bucket: s3URI.Host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(s3URI, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, err)
	}

	// we should set this to true, since br set it by default in parseBackend
	s3.ForcePathStyle = true
	backend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: s3},
	}
	s3storage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		HTTPClient:      nil,
		SkipCheckPath:   true,
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, err)
	}

	return s3storage, nil
}

// ParseLogFileName extract the commitTs, fileType from log fileName
func ParseLogFileName(name string) (uint64, string, error) {
	ext := filepath.Ext(name)
	if ext == MetaEXT {
		return 0, DefaultMetaFileType, nil
	}

	if ext != LogEXT && ext != TmpEXT {
		return 0, "", nil
	}

	var commitTs, d1 uint64
	var s1, s2, fileType string
	// fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileType, w.commitTS.Load(), redo.LogEXT)
	formatStr := "%s %s %d %s %d" + LogEXT
	if ext == TmpEXT {
		formatStr += TmpEXT
	}
	name = strings.ReplaceAll(name, "_", " ")
	_, err := fmt.Sscanf(name, formatStr, &s1, &s2, &d1, &fileType, &commitTs)
	if err != nil {
		return 0, "", errors.Annotatef(err, "bad log name: %s", name)
	}

	return commitTs, fileType, nil
}
