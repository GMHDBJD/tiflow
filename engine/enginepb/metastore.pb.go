// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: metastore.proto

package enginepb

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type StoreType int32

const (
	StoreType_ServiceDiscovery StoreType = 0
	StoreType_SystemMetaStore  StoreType = 1
	StoreType_AppMetaStore     StoreType = 2
)

var StoreType_name = map[int32]string{
	0: "ServiceDiscovery",
	1: "SystemMetaStore",
	2: "AppMetaStore",
}

var StoreType_value = map[string]int32{
	"ServiceDiscovery": 0,
	"SystemMetaStore":  1,
	"AppMetaStore":     2,
}

func (x StoreType) String() string {
	return proto.EnumName(StoreType_name, int32(x))
}

func (StoreType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_f7f6a0570779d42a, []int{0}
}

type RegisterMetaStoreRequest struct {
	Address string    `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty"`
	Tp      StoreType `protobuf:"varint,2,opt,name=tp,proto3,enum=enginepb.StoreType" json:"tp,omitempty"`
}

func (m *RegisterMetaStoreRequest) Reset()         { *m = RegisterMetaStoreRequest{} }
func (m *RegisterMetaStoreRequest) String() string { return proto.CompactTextString(m) }
func (*RegisterMetaStoreRequest) ProtoMessage()    {}
func (*RegisterMetaStoreRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f7f6a0570779d42a, []int{0}
}
func (m *RegisterMetaStoreRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RegisterMetaStoreRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RegisterMetaStoreRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RegisterMetaStoreRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegisterMetaStoreRequest.Merge(m, src)
}
func (m *RegisterMetaStoreRequest) XXX_Size() int {
	return m.Size()
}
func (m *RegisterMetaStoreRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_RegisterMetaStoreRequest.DiscardUnknown(m)
}

var xxx_messageInfo_RegisterMetaStoreRequest proto.InternalMessageInfo

func (m *RegisterMetaStoreRequest) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func (m *RegisterMetaStoreRequest) GetTp() StoreType {
	if m != nil {
		return m.Tp
	}
	return StoreType_ServiceDiscovery
}

type RegisterMetaStoreResponse struct {
	Err *Error `protobuf:"bytes,1,opt,name=err,proto3" json:"err,omitempty"`
}

func (m *RegisterMetaStoreResponse) Reset()         { *m = RegisterMetaStoreResponse{} }
func (m *RegisterMetaStoreResponse) String() string { return proto.CompactTextString(m) }
func (*RegisterMetaStoreResponse) ProtoMessage()    {}
func (*RegisterMetaStoreResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f7f6a0570779d42a, []int{1}
}
func (m *RegisterMetaStoreResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RegisterMetaStoreResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RegisterMetaStoreResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RegisterMetaStoreResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RegisterMetaStoreResponse.Merge(m, src)
}
func (m *RegisterMetaStoreResponse) XXX_Size() int {
	return m.Size()
}
func (m *RegisterMetaStoreResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_RegisterMetaStoreResponse.DiscardUnknown(m)
}

var xxx_messageInfo_RegisterMetaStoreResponse proto.InternalMessageInfo

func (m *RegisterMetaStoreResponse) GetErr() *Error {
	if m != nil {
		return m.Err
	}
	return nil
}

type QueryMetaStoreRequest struct {
	Tp StoreType `protobuf:"varint,1,opt,name=tp,proto3,enum=enginepb.StoreType" json:"tp,omitempty"`
}

func (m *QueryMetaStoreRequest) Reset()         { *m = QueryMetaStoreRequest{} }
func (m *QueryMetaStoreRequest) String() string { return proto.CompactTextString(m) }
func (*QueryMetaStoreRequest) ProtoMessage()    {}
func (*QueryMetaStoreRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_f7f6a0570779d42a, []int{2}
}
func (m *QueryMetaStoreRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *QueryMetaStoreRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_QueryMetaStoreRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *QueryMetaStoreRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryMetaStoreRequest.Merge(m, src)
}
func (m *QueryMetaStoreRequest) XXX_Size() int {
	return m.Size()
}
func (m *QueryMetaStoreRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryMetaStoreRequest.DiscardUnknown(m)
}

var xxx_messageInfo_QueryMetaStoreRequest proto.InternalMessageInfo

func (m *QueryMetaStoreRequest) GetTp() StoreType {
	if m != nil {
		return m.Tp
	}
	return StoreType_ServiceDiscovery
}

type QueryMetaStoreResponse struct {
	Err     *Error `protobuf:"bytes,1,opt,name=err,proto3" json:"err,omitempty"`
	Address string `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
}

func (m *QueryMetaStoreResponse) Reset()         { *m = QueryMetaStoreResponse{} }
func (m *QueryMetaStoreResponse) String() string { return proto.CompactTextString(m) }
func (*QueryMetaStoreResponse) ProtoMessage()    {}
func (*QueryMetaStoreResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_f7f6a0570779d42a, []int{3}
}
func (m *QueryMetaStoreResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *QueryMetaStoreResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_QueryMetaStoreResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *QueryMetaStoreResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueryMetaStoreResponse.Merge(m, src)
}
func (m *QueryMetaStoreResponse) XXX_Size() int {
	return m.Size()
}
func (m *QueryMetaStoreResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_QueryMetaStoreResponse.DiscardUnknown(m)
}

var xxx_messageInfo_QueryMetaStoreResponse proto.InternalMessageInfo

func (m *QueryMetaStoreResponse) GetErr() *Error {
	if m != nil {
		return m.Err
	}
	return nil
}

func (m *QueryMetaStoreResponse) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}

func init() {
	proto.RegisterEnum("enginepb.StoreType", StoreType_name, StoreType_value)
	proto.RegisterType((*RegisterMetaStoreRequest)(nil), "enginepb.RegisterMetaStoreRequest")
	proto.RegisterType((*RegisterMetaStoreResponse)(nil), "enginepb.RegisterMetaStoreResponse")
	proto.RegisterType((*QueryMetaStoreRequest)(nil), "enginepb.QueryMetaStoreRequest")
	proto.RegisterType((*QueryMetaStoreResponse)(nil), "enginepb.QueryMetaStoreResponse")
}

func init() { proto.RegisterFile("metastore.proto", fileDescriptor_f7f6a0570779d42a) }

var fileDescriptor_f7f6a0570779d42a = []byte{
	// 280 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x91, 0xbf, 0x4e, 0xc3, 0x30,
	0x10, 0xc6, 0xe3, 0x20, 0x01, 0xbd, 0x22, 0x12, 0xb9, 0x80, 0x0c, 0x83, 0x55, 0xc2, 0x52, 0x31,
	0x64, 0x28, 0x2b, 0x42, 0x02, 0x81, 0xc4, 0xc2, 0x80, 0x03, 0x03, 0x63, 0xda, 0x9e, 0xaa, 0x0c,
	0x8d, 0xcd, 0xd9, 0xad, 0x94, 0xb7, 0xe0, 0xb1, 0x18, 0x3b, 0x32, 0xa2, 0xe4, 0x45, 0x50, 0x52,
	0xb5, 0xe5, 0x9f, 0x10, 0xa3, 0xcf, 0xfa, 0x7e, 0xbf, 0xfb, 0x74, 0x10, 0x4c, 0xd0, 0xa5, 0xd6,
	0x69, 0xc2, 0xd8, 0x90, 0x76, 0x9a, 0x6f, 0x63, 0x3e, 0xce, 0x72, 0x34, 0x83, 0xa3, 0x36, 0x12,
	0x69, 0x5a, 0x8c, 0xa3, 0x27, 0x10, 0x0a, 0xc7, 0x99, 0x75, 0x48, 0x77, 0xe8, 0xd2, 0xa4, 0x4e,
	0x28, 0x7c, 0x9e, 0xa2, 0x75, 0x5c, 0xc0, 0x56, 0x3a, 0x1a, 0x11, 0x5a, 0x2b, 0x58, 0x97, 0xf5,
	0x5a, 0x6a, 0xf9, 0xe4, 0x27, 0xe0, 0x3b, 0x23, 0xfc, 0x2e, 0xeb, 0xed, 0xf6, 0x3b, 0xf1, 0x92,
	0x1c, 0x37, 0xe9, 0x87, 0xc2, 0xa0, 0xf2, 0x9d, 0x89, 0x2e, 0xe0, 0xf0, 0x17, 0xb4, 0x35, 0x3a,
	0xb7, 0xc8, 0x8f, 0x61, 0x03, 0x89, 0x1a, 0x6e, 0xbb, 0x1f, 0xac, 0x11, 0x37, 0xf5, 0x6e, 0xaa,
	0xfe, 0x8b, 0xce, 0x61, 0xff, 0x7e, 0x8a, 0x54, 0xfc, 0xd8, 0x6b, 0x61, 0x67, 0x7f, 0xdb, 0x1f,
	0xe1, 0xe0, 0x7b, 0xfa, 0xdf, 0xea, 0xcf, 0xcd, 0xfd, 0x2f, 0xcd, 0x4f, 0x6f, 0xa1, 0xb5, 0xf2,
	0xf0, 0x3d, 0x08, 0x13, 0xa4, 0x59, 0x36, 0xc4, 0xeb, 0xcc, 0x0e, 0xf5, 0x0c, 0xa9, 0x08, 0x3d,
	0xde, 0x81, 0x20, 0x29, 0xac, 0xc3, 0xc9, 0x4a, 0x1d, 0x32, 0x1e, 0xc2, 0xce, 0xa5, 0x31, 0xeb,
	0x89, 0x7f, 0x25, 0x5e, 0x4b, 0xc9, 0xe6, 0xa5, 0x64, 0xef, 0xa5, 0x64, 0x2f, 0x95, 0xf4, 0xe6,
	0x95, 0xf4, 0xde, 0x2a, 0xe9, 0x0d, 0x36, 0x9b, 0xd3, 0x9c, 0x7d, 0x04, 0x00, 0x00, 0xff, 0xff,
	0x35, 0x84, 0xd0, 0xb4, 0xc4, 0x01, 0x00, 0x00,
}

func (m *RegisterMetaStoreRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RegisterMetaStoreRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RegisterMetaStoreRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Tp != 0 {
		i = encodeVarintMetastore(dAtA, i, uint64(m.Tp))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Address) > 0 {
		i -= len(m.Address)
		copy(dAtA[i:], m.Address)
		i = encodeVarintMetastore(dAtA, i, uint64(len(m.Address)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *RegisterMetaStoreResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RegisterMetaStoreResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RegisterMetaStoreResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Err != nil {
		{
			size, err := m.Err.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintMetastore(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *QueryMetaStoreRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *QueryMetaStoreRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *QueryMetaStoreRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Tp != 0 {
		i = encodeVarintMetastore(dAtA, i, uint64(m.Tp))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *QueryMetaStoreResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *QueryMetaStoreResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *QueryMetaStoreResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Address) > 0 {
		i -= len(m.Address)
		copy(dAtA[i:], m.Address)
		i = encodeVarintMetastore(dAtA, i, uint64(len(m.Address)))
		i--
		dAtA[i] = 0x12
	}
	if m.Err != nil {
		{
			size, err := m.Err.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintMetastore(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintMetastore(dAtA []byte, offset int, v uint64) int {
	offset -= sovMetastore(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *RegisterMetaStoreRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovMetastore(uint64(l))
	}
	if m.Tp != 0 {
		n += 1 + sovMetastore(uint64(m.Tp))
	}
	return n
}

func (m *RegisterMetaStoreResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Err != nil {
		l = m.Err.Size()
		n += 1 + l + sovMetastore(uint64(l))
	}
	return n
}

func (m *QueryMetaStoreRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Tp != 0 {
		n += 1 + sovMetastore(uint64(m.Tp))
	}
	return n
}

func (m *QueryMetaStoreResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Err != nil {
		l = m.Err.Size()
		n += 1 + l + sovMetastore(uint64(l))
	}
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovMetastore(uint64(l))
	}
	return n
}

func sovMetastore(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozMetastore(x uint64) (n int) {
	return sovMetastore(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *RegisterMetaStoreRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetastore
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RegisterMetaStoreRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RegisterMetaStoreRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMetastore
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthMetastore
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tp", wireType)
			}
			m.Tp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Tp |= StoreType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetastore(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMetastore
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RegisterMetaStoreResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetastore
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RegisterMetaStoreResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RegisterMetaStoreResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Err", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetastore
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMetastore
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Err == nil {
				m.Err = &Error{}
			}
			if err := m.Err.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetastore(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMetastore
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *QueryMetaStoreRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetastore
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: QueryMetaStoreRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: QueryMetaStoreRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tp", wireType)
			}
			m.Tp = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Tp |= StoreType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetastore(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMetastore
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *QueryMetaStoreResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetastore
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: QueryMetaStoreResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: QueryMetaStoreResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Err", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetastore
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMetastore
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Err == nil {
				m.Err = &Error{}
			}
			if err := m.Err.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMetastore
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthMetastore
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetastore(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthMetastore
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipMetastore(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMetastore
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetastore
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthMetastore
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupMetastore
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthMetastore
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthMetastore        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMetastore          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupMetastore = fmt.Errorf("proto: unexpected end of group")
)
