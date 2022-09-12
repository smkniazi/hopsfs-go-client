//*
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//*
// These .proto interfaces are private and stable.
// Please see http://wiki.apache.org/hadoop/Compatibility
// for what changes are allowed for a *stable* .proto interface.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
<<<<<<< HEAD
// 	protoc-gen-go v1.25.0
// 	protoc        (unknown)
=======
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.4
>>>>>>> c67abc1
// source: Security.proto

package hadoop_common

import (
<<<<<<< HEAD
	proto "github.com/golang/protobuf/proto"
=======
>>>>>>> c67abc1
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

<<<<<<< HEAD
// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

=======
>>>>>>> c67abc1
//*
// Security token identifier
type TokenProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Identifier []byte  `protobuf:"bytes,1,req,name=identifier" json:"identifier,omitempty"`
	Password   []byte  `protobuf:"bytes,2,req,name=password" json:"password,omitempty"`
	Kind       *string `protobuf:"bytes,3,req,name=kind" json:"kind,omitempty"`
	Service    *string `protobuf:"bytes,4,req,name=service" json:"service,omitempty"`
}

func (x *TokenProto) Reset() {
	*x = TokenProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_Security_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TokenProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TokenProto) ProtoMessage() {}

func (x *TokenProto) ProtoReflect() protoreflect.Message {
	mi := &file_Security_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TokenProto.ProtoReflect.Descriptor instead.
func (*TokenProto) Descriptor() ([]byte, []int) {
	return file_Security_proto_rawDescGZIP(), []int{0}
}

func (x *TokenProto) GetIdentifier() []byte {
	if x != nil {
		return x.Identifier
	}
	return nil
}

func (x *TokenProto) GetPassword() []byte {
	if x != nil {
		return x.Password
	}
	return nil
}

func (x *TokenProto) GetKind() string {
	if x != nil && x.Kind != nil {
		return *x.Kind
	}
	return ""
}

func (x *TokenProto) GetService() string {
	if x != nil && x.Service != nil {
		return *x.Service
	}
	return ""
}

type CredentialsKVProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Alias  *string     `protobuf:"bytes,1,req,name=alias" json:"alias,omitempty"`
	Token  *TokenProto `protobuf:"bytes,2,opt,name=token" json:"token,omitempty"`
	Secret []byte      `protobuf:"bytes,3,opt,name=secret" json:"secret,omitempty"`
}

func (x *CredentialsKVProto) Reset() {
	*x = CredentialsKVProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_Security_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CredentialsKVProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CredentialsKVProto) ProtoMessage() {}

func (x *CredentialsKVProto) ProtoReflect() protoreflect.Message {
	mi := &file_Security_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CredentialsKVProto.ProtoReflect.Descriptor instead.
func (*CredentialsKVProto) Descriptor() ([]byte, []int) {
	return file_Security_proto_rawDescGZIP(), []int{1}
}

func (x *CredentialsKVProto) GetAlias() string {
	if x != nil && x.Alias != nil {
		return *x.Alias
	}
	return ""
}

func (x *CredentialsKVProto) GetToken() *TokenProto {
	if x != nil {
		return x.Token
	}
	return nil
}

func (x *CredentialsKVProto) GetSecret() []byte {
	if x != nil {
		return x.Secret
	}
	return nil
}

type CredentialsProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tokens  []*CredentialsKVProto `protobuf:"bytes,1,rep,name=tokens" json:"tokens,omitempty"`
	Secrets []*CredentialsKVProto `protobuf:"bytes,2,rep,name=secrets" json:"secrets,omitempty"`
}

func (x *CredentialsProto) Reset() {
	*x = CredentialsProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_Security_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CredentialsProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CredentialsProto) ProtoMessage() {}

func (x *CredentialsProto) ProtoReflect() protoreflect.Message {
	mi := &file_Security_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CredentialsProto.ProtoReflect.Descriptor instead.
func (*CredentialsProto) Descriptor() ([]byte, []int) {
	return file_Security_proto_rawDescGZIP(), []int{2}
}

func (x *CredentialsProto) GetTokens() []*CredentialsKVProto {
	if x != nil {
		return x.Tokens
	}
	return nil
}

func (x *CredentialsProto) GetSecrets() []*CredentialsKVProto {
	if x != nil {
		return x.Secrets
	}
	return nil
}

type GetDelegationTokenRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Renewer *string `protobuf:"bytes,1,req,name=renewer" json:"renewer,omitempty"`
}

func (x *GetDelegationTokenRequestProto) Reset() {
	*x = GetDelegationTokenRequestProto{}
	if protoimpl.UnsafeEnabled {
<<<<<<< HEAD
		mi := &file_Security_proto_msgTypes[1]
=======
		mi := &file_Security_proto_msgTypes[3]
>>>>>>> c67abc1
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetDelegationTokenRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetDelegationTokenRequestProto) ProtoMessage() {}

func (x *GetDelegationTokenRequestProto) ProtoReflect() protoreflect.Message {
<<<<<<< HEAD
	mi := &file_Security_proto_msgTypes[1]
=======
	mi := &file_Security_proto_msgTypes[3]
>>>>>>> c67abc1
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetDelegationTokenRequestProto.ProtoReflect.Descriptor instead.
func (*GetDelegationTokenRequestProto) Descriptor() ([]byte, []int) {
<<<<<<< HEAD
	return file_Security_proto_rawDescGZIP(), []int{1}
=======
	return file_Security_proto_rawDescGZIP(), []int{3}
>>>>>>> c67abc1
}

func (x *GetDelegationTokenRequestProto) GetRenewer() string {
	if x != nil && x.Renewer != nil {
		return *x.Renewer
	}
	return ""
}

type GetDelegationTokenResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Token *TokenProto `protobuf:"bytes,1,opt,name=token" json:"token,omitempty"`
}

func (x *GetDelegationTokenResponseProto) Reset() {
	*x = GetDelegationTokenResponseProto{}
	if protoimpl.UnsafeEnabled {
<<<<<<< HEAD
		mi := &file_Security_proto_msgTypes[2]
=======
		mi := &file_Security_proto_msgTypes[4]
>>>>>>> c67abc1
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetDelegationTokenResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetDelegationTokenResponseProto) ProtoMessage() {}

func (x *GetDelegationTokenResponseProto) ProtoReflect() protoreflect.Message {
<<<<<<< HEAD
	mi := &file_Security_proto_msgTypes[2]
=======
	mi := &file_Security_proto_msgTypes[4]
>>>>>>> c67abc1
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetDelegationTokenResponseProto.ProtoReflect.Descriptor instead.
func (*GetDelegationTokenResponseProto) Descriptor() ([]byte, []int) {
<<<<<<< HEAD
	return file_Security_proto_rawDescGZIP(), []int{2}
=======
	return file_Security_proto_rawDescGZIP(), []int{4}
>>>>>>> c67abc1
}

func (x *GetDelegationTokenResponseProto) GetToken() *TokenProto {
	if x != nil {
		return x.Token
	}
	return nil
}

type RenewDelegationTokenRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Token *TokenProto `protobuf:"bytes,1,req,name=token" json:"token,omitempty"`
}

func (x *RenewDelegationTokenRequestProto) Reset() {
	*x = RenewDelegationTokenRequestProto{}
	if protoimpl.UnsafeEnabled {
<<<<<<< HEAD
		mi := &file_Security_proto_msgTypes[3]
=======
		mi := &file_Security_proto_msgTypes[5]
>>>>>>> c67abc1
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RenewDelegationTokenRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RenewDelegationTokenRequestProto) ProtoMessage() {}

func (x *RenewDelegationTokenRequestProto) ProtoReflect() protoreflect.Message {
<<<<<<< HEAD
	mi := &file_Security_proto_msgTypes[3]
=======
	mi := &file_Security_proto_msgTypes[5]
>>>>>>> c67abc1
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RenewDelegationTokenRequestProto.ProtoReflect.Descriptor instead.
func (*RenewDelegationTokenRequestProto) Descriptor() ([]byte, []int) {
<<<<<<< HEAD
	return file_Security_proto_rawDescGZIP(), []int{3}
=======
	return file_Security_proto_rawDescGZIP(), []int{5}
>>>>>>> c67abc1
}

func (x *RenewDelegationTokenRequestProto) GetToken() *TokenProto {
	if x != nil {
		return x.Token
	}
	return nil
}

type RenewDelegationTokenResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NewExpiryTime *uint64 `protobuf:"varint,1,req,name=newExpiryTime" json:"newExpiryTime,omitempty"`
}

func (x *RenewDelegationTokenResponseProto) Reset() {
	*x = RenewDelegationTokenResponseProto{}
	if protoimpl.UnsafeEnabled {
<<<<<<< HEAD
		mi := &file_Security_proto_msgTypes[4]
=======
		mi := &file_Security_proto_msgTypes[6]
>>>>>>> c67abc1
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RenewDelegationTokenResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RenewDelegationTokenResponseProto) ProtoMessage() {}

func (x *RenewDelegationTokenResponseProto) ProtoReflect() protoreflect.Message {
<<<<<<< HEAD
	mi := &file_Security_proto_msgTypes[4]
=======
	mi := &file_Security_proto_msgTypes[6]
>>>>>>> c67abc1
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RenewDelegationTokenResponseProto.ProtoReflect.Descriptor instead.
func (*RenewDelegationTokenResponseProto) Descriptor() ([]byte, []int) {
<<<<<<< HEAD
	return file_Security_proto_rawDescGZIP(), []int{4}
=======
	return file_Security_proto_rawDescGZIP(), []int{6}
>>>>>>> c67abc1
}

func (x *RenewDelegationTokenResponseProto) GetNewExpiryTime() uint64 {
	if x != nil && x.NewExpiryTime != nil {
		return *x.NewExpiryTime
	}
	return 0
}

type CancelDelegationTokenRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Token *TokenProto `protobuf:"bytes,1,req,name=token" json:"token,omitempty"`
}

func (x *CancelDelegationTokenRequestProto) Reset() {
	*x = CancelDelegationTokenRequestProto{}
	if protoimpl.UnsafeEnabled {
<<<<<<< HEAD
		mi := &file_Security_proto_msgTypes[5]
=======
		mi := &file_Security_proto_msgTypes[7]
>>>>>>> c67abc1
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CancelDelegationTokenRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CancelDelegationTokenRequestProto) ProtoMessage() {}

func (x *CancelDelegationTokenRequestProto) ProtoReflect() protoreflect.Message {
<<<<<<< HEAD
	mi := &file_Security_proto_msgTypes[5]
=======
	mi := &file_Security_proto_msgTypes[7]
>>>>>>> c67abc1
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CancelDelegationTokenRequestProto.ProtoReflect.Descriptor instead.
func (*CancelDelegationTokenRequestProto) Descriptor() ([]byte, []int) {
<<<<<<< HEAD
	return file_Security_proto_rawDescGZIP(), []int{5}
=======
	return file_Security_proto_rawDescGZIP(), []int{7}
>>>>>>> c67abc1
}

func (x *CancelDelegationTokenRequestProto) GetToken() *TokenProto {
	if x != nil {
		return x.Token
	}
	return nil
}

type CancelDelegationTokenResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CancelDelegationTokenResponseProto) Reset() {
	*x = CancelDelegationTokenResponseProto{}
	if protoimpl.UnsafeEnabled {
<<<<<<< HEAD
		mi := &file_Security_proto_msgTypes[6]
=======
		mi := &file_Security_proto_msgTypes[8]
>>>>>>> c67abc1
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CancelDelegationTokenResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CancelDelegationTokenResponseProto) ProtoMessage() {}

func (x *CancelDelegationTokenResponseProto) ProtoReflect() protoreflect.Message {
<<<<<<< HEAD
	mi := &file_Security_proto_msgTypes[6]
=======
	mi := &file_Security_proto_msgTypes[8]
>>>>>>> c67abc1
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CancelDelegationTokenResponseProto.ProtoReflect.Descriptor instead.
func (*CancelDelegationTokenResponseProto) Descriptor() ([]byte, []int) {
<<<<<<< HEAD
	return file_Security_proto_rawDescGZIP(), []int{6}
=======
	return file_Security_proto_rawDescGZIP(), []int{8}
>>>>>>> c67abc1
}

var File_Security_proto protoreflect.FileDescriptor

var file_Security_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x53, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x0d, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x22,
<<<<<<< HEAD
	0x51, 0x0a, 0x0a, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x12, 0x0a,
	0x0a, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x18, 0x01, 0x20, 0x02, 0x28,
	0x0c, 0x12, 0x10, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x02, 0x20,
	0x02, 0x28, 0x0c, 0x12, 0x0c, 0x0a, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x18, 0x03, 0x20, 0x02, 0x28,
	0x09, 0x12, 0x0f, 0x0a, 0x07, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x18, 0x04, 0x20, 0x02,
	0x28, 0x09, 0x22, 0x31, 0x0a, 0x1e, 0x47, 0x65, 0x74, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50,
	0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0f, 0x0a, 0x07, 0x72, 0x65, 0x6e, 0x65, 0x77, 0x65, 0x72, 0x18,
	0x01, 0x20, 0x02, 0x28, 0x09, 0x22, 0x4b, 0x0a, 0x1f, 0x47, 0x65, 0x74, 0x44, 0x65, 0x6c, 0x65,
	0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x28, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65,
	0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x4c, 0x0a, 0x20, 0x52, 0x65, 0x6e, 0x65, 0x77, 0x44, 0x65, 0x6c, 0x65, 0x67,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x28, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18,
	0x01, 0x20, 0x02, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x3a, 0x0a, 0x21, 0x52, 0x65, 0x6e, 0x65, 0x77, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x15, 0x0a, 0x0d, 0x6e, 0x65, 0x77, 0x45, 0x78, 0x70, 0x69,
	0x72, 0x79, 0x54, 0x69, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x02, 0x28, 0x04, 0x22, 0x4d, 0x0a, 0x21,
	0x43, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x28, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x01, 0x20, 0x02, 0x28, 0x0b,
	0x32, 0x19, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e,
	0x2e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x24, 0x0a, 0x22, 0x43,
	0x61, 0x6e, 0x63, 0x65, 0x6c, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54,
	0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x42, 0x38, 0x0a, 0x20, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65, 0x2e,
	0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x73, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x0e, 0x53, 0x65, 0x63, 0x75, 0x72, 0x69, 0x74, 0x79, 0x50,
	0x72, 0x6f, 0x74, 0x6f, 0x73, 0x88, 0x01, 0x01, 0xa0, 0x01, 0x01,
=======
	0x76, 0x0a, 0x0a, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1e, 0x0a,
	0x0a, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x18, 0x01, 0x20, 0x02, 0x28,
	0x0c, 0x52, 0x0a, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x12, 0x1a, 0x0a,
	0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x02, 0x20, 0x02, 0x28, 0x0c, 0x52,
	0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6b, 0x69, 0x6e,
	0x64, 0x18, 0x03, 0x20, 0x02, 0x28, 0x09, 0x52, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x12, 0x18, 0x0a,
	0x07, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x18, 0x04, 0x20, 0x02, 0x28, 0x09, 0x52, 0x07,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x22, 0x73, 0x0a, 0x12, 0x43, 0x72, 0x65, 0x64, 0x65,
	0x6e, 0x74, 0x69, 0x61, 0x6c, 0x73, 0x4b, 0x56, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x14, 0x0a,
	0x05, 0x61, 0x6c, 0x69, 0x61, 0x73, 0x18, 0x01, 0x20, 0x02, 0x28, 0x09, 0x52, 0x05, 0x61, 0x6c,
	0x69, 0x61, 0x73, 0x12, 0x2f, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x19, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d,
	0x6f, 0x6e, 0x2e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x05, 0x74,
	0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x22, 0x8a, 0x01, 0x0a,
	0x10, 0x43, 0x72, 0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x73, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x39, 0x0a, 0x06, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x21, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f,
	0x6e, 0x2e, 0x43, 0x72, 0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x73, 0x4b, 0x56, 0x50,
	0x72, 0x6f, 0x74, 0x6f, 0x52, 0x06, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x73, 0x12, 0x3b, 0x0a, 0x07,
	0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x21, 0x2e,
	0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x43, 0x72,
	0x65, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x61, 0x6c, 0x73, 0x4b, 0x56, 0x50, 0x72, 0x6f, 0x74, 0x6f,
	0x52, 0x07, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x22, 0x3a, 0x0a, 0x1e, 0x47, 0x65, 0x74,
	0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x18, 0x0a, 0x07, 0x72,
	0x65, 0x6e, 0x65, 0x77, 0x65, 0x72, 0x18, 0x01, 0x20, 0x02, 0x28, 0x09, 0x52, 0x07, 0x72, 0x65,
	0x6e, 0x65, 0x77, 0x65, 0x72, 0x22, 0x52, 0x0a, 0x1f, 0x47, 0x65, 0x74, 0x44, 0x65, 0x6c, 0x65,
	0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x2f, 0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65,
	0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70,
	0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x52, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x22, 0x53, 0x0a, 0x20, 0x52, 0x65, 0x6e,
	0x65, 0x77, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x2f, 0x0a,
	0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x01, 0x20, 0x02, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x68,
	0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x6f, 0x6b,
	0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x22, 0x49,
	0x0a, 0x21, 0x52, 0x65, 0x6e, 0x65, 0x77, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x24, 0x0a, 0x0d, 0x6e, 0x65, 0x77, 0x45, 0x78, 0x70, 0x69, 0x72, 0x79,
	0x54, 0x69, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x02, 0x28, 0x04, 0x52, 0x0d, 0x6e, 0x65, 0x77, 0x45,
	0x78, 0x70, 0x69, 0x72, 0x79, 0x54, 0x69, 0x6d, 0x65, 0x22, 0x54, 0x0a, 0x21, 0x43, 0x61, 0x6e,
	0x63, 0x65, 0x6c, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b,
	0x65, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x2f,
	0x0a, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x01, 0x20, 0x02, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x54, 0x6f,
	0x6b, 0x65, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x05, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x22,
	0x24, 0x0a, 0x22, 0x43, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x44, 0x65, 0x6c, 0x65, 0x67, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x76, 0x0a, 0x20, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61,
	0x63, 0x68, 0x65, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x73, 0x65, 0x63, 0x75, 0x72,
	0x69, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x0e, 0x53, 0x65, 0x63, 0x75, 0x72,
	0x69, 0x74, 0x79, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x5a, 0x3c, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63, 0x6f, 0x6c, 0x69, 0x6e, 0x6d, 0x61, 0x72, 0x63, 0x2f,
	0x68, 0x64, 0x66, 0x73, 0x2f, 0x76, 0x32, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2f, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70,
	0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x88, 0x01, 0x01, 0xa0, 0x01, 0x01,
>>>>>>> c67abc1
}

var (
	file_Security_proto_rawDescOnce sync.Once
	file_Security_proto_rawDescData = file_Security_proto_rawDesc
)

func file_Security_proto_rawDescGZIP() []byte {
	file_Security_proto_rawDescOnce.Do(func() {
		file_Security_proto_rawDescData = protoimpl.X.CompressGZIP(file_Security_proto_rawDescData)
	})
	return file_Security_proto_rawDescData
}

<<<<<<< HEAD
var file_Security_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_Security_proto_goTypes = []interface{}{
	(*TokenProto)(nil),                         // 0: hadoop.common.TokenProto
	(*GetDelegationTokenRequestProto)(nil),     // 1: hadoop.common.GetDelegationTokenRequestProto
	(*GetDelegationTokenResponseProto)(nil),    // 2: hadoop.common.GetDelegationTokenResponseProto
	(*RenewDelegationTokenRequestProto)(nil),   // 3: hadoop.common.RenewDelegationTokenRequestProto
	(*RenewDelegationTokenResponseProto)(nil),  // 4: hadoop.common.RenewDelegationTokenResponseProto
	(*CancelDelegationTokenRequestProto)(nil),  // 5: hadoop.common.CancelDelegationTokenRequestProto
	(*CancelDelegationTokenResponseProto)(nil), // 6: hadoop.common.CancelDelegationTokenResponseProto
}
var file_Security_proto_depIdxs = []int32{
	0, // 0: hadoop.common.GetDelegationTokenResponseProto.token:type_name -> hadoop.common.TokenProto
	0, // 1: hadoop.common.RenewDelegationTokenRequestProto.token:type_name -> hadoop.common.TokenProto
	0, // 2: hadoop.common.CancelDelegationTokenRequestProto.token:type_name -> hadoop.common.TokenProto
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
=======
var file_Security_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_Security_proto_goTypes = []interface{}{
	(*TokenProto)(nil),                         // 0: hadoop.common.TokenProto
	(*CredentialsKVProto)(nil),                 // 1: hadoop.common.CredentialsKVProto
	(*CredentialsProto)(nil),                   // 2: hadoop.common.CredentialsProto
	(*GetDelegationTokenRequestProto)(nil),     // 3: hadoop.common.GetDelegationTokenRequestProto
	(*GetDelegationTokenResponseProto)(nil),    // 4: hadoop.common.GetDelegationTokenResponseProto
	(*RenewDelegationTokenRequestProto)(nil),   // 5: hadoop.common.RenewDelegationTokenRequestProto
	(*RenewDelegationTokenResponseProto)(nil),  // 6: hadoop.common.RenewDelegationTokenResponseProto
	(*CancelDelegationTokenRequestProto)(nil),  // 7: hadoop.common.CancelDelegationTokenRequestProto
	(*CancelDelegationTokenResponseProto)(nil), // 8: hadoop.common.CancelDelegationTokenResponseProto
}
var file_Security_proto_depIdxs = []int32{
	0, // 0: hadoop.common.CredentialsKVProto.token:type_name -> hadoop.common.TokenProto
	1, // 1: hadoop.common.CredentialsProto.tokens:type_name -> hadoop.common.CredentialsKVProto
	1, // 2: hadoop.common.CredentialsProto.secrets:type_name -> hadoop.common.CredentialsKVProto
	0, // 3: hadoop.common.GetDelegationTokenResponseProto.token:type_name -> hadoop.common.TokenProto
	0, // 4: hadoop.common.RenewDelegationTokenRequestProto.token:type_name -> hadoop.common.TokenProto
	0, // 5: hadoop.common.CancelDelegationTokenRequestProto.token:type_name -> hadoop.common.TokenProto
	6, // [6:6] is the sub-list for method output_type
	6, // [6:6] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
>>>>>>> c67abc1
}

func init() { file_Security_proto_init() }
func file_Security_proto_init() {
	if File_Security_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_Security_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TokenProto); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
<<<<<<< HEAD
			switch v := v.(*GetDelegationTokenRequestProto); i {
=======
			switch v := v.(*CredentialsKVProto); i {
>>>>>>> c67abc1
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
<<<<<<< HEAD
			switch v := v.(*GetDelegationTokenResponseProto); i {
=======
			switch v := v.(*CredentialsProto); i {
>>>>>>> c67abc1
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
<<<<<<< HEAD
			switch v := v.(*RenewDelegationTokenRequestProto); i {
=======
			switch v := v.(*GetDelegationTokenRequestProto); i {
>>>>>>> c67abc1
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
<<<<<<< HEAD
			switch v := v.(*RenewDelegationTokenResponseProto); i {
=======
			switch v := v.(*GetDelegationTokenResponseProto); i {
>>>>>>> c67abc1
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
<<<<<<< HEAD
			switch v := v.(*CancelDelegationTokenRequestProto); i {
=======
			switch v := v.(*RenewDelegationTokenRequestProto); i {
>>>>>>> c67abc1
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
<<<<<<< HEAD
=======
			switch v := v.(*RenewDelegationTokenResponseProto); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CancelDelegationTokenRequestProto); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_Security_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
>>>>>>> c67abc1
			switch v := v.(*CancelDelegationTokenResponseProto); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_Security_proto_rawDesc,
			NumEnums:      0,
<<<<<<< HEAD
			NumMessages:   7,
=======
			NumMessages:   9,
>>>>>>> c67abc1
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_Security_proto_goTypes,
		DependencyIndexes: file_Security_proto_depIdxs,
		MessageInfos:      file_Security_proto_msgTypes,
	}.Build()
	File_Security_proto = out.File
	file_Security_proto_rawDesc = nil
	file_Security_proto_goTypes = nil
	file_Security_proto_depIdxs = nil
}
