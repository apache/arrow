// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow/go/arrow/internal/debug"
	"github.com/apache/arrow/go/arrow/internal/flatbuf"
	"github.com/apache/arrow/go/arrow/memory"
)

// MetadataVersion represents the Arrow metadata version.
type MetadataVersion flatbuf.MetadataVersion

const (
	MetadataV1 = MetadataVersion(flatbuf.MetadataVersionV1) // version for Arrow-0.1.0
	MetadataV2 = MetadataVersion(flatbuf.MetadataVersionV2) // version for Arrow-0.2.0
	MetadataV3 = MetadataVersion(flatbuf.MetadataVersionV3) // version for Arrow-0.3.0 to 0.7.1
	MetadataV4 = MetadataVersion(flatbuf.MetadataVersionV4) // version for >= Arrow-0.8.0
)

func (m MetadataVersion) String() string {
	if v, ok := flatbuf.EnumNamesMetadataVersion[int16(m)]; ok {
		return v
	}
	return fmt.Sprintf("MetadataVersion(%d)", int16(m))
}

// MessageType represents the type of Message in an Arrow format.
type MessageType flatbuf.MessageHeader

const (
	MessageNone            = MessageType(flatbuf.MessageHeaderNONE)
	MessageSchema          = MessageType(flatbuf.MessageHeaderSchema)
	MessageDictionaryBatch = MessageType(flatbuf.MessageHeaderDictionaryBatch)
	MessageRecordBatch     = MessageType(flatbuf.MessageHeaderRecordBatch)
	MessageTensor          = MessageType(flatbuf.MessageHeaderTensor)
	MessageSparseTensor    = MessageType(flatbuf.MessageHeaderSparseTensor)
)

func (m MessageType) String() string {
	if v, ok := flatbuf.EnumNamesMessageHeader[byte(m)]; ok {
		return v
	}
	return fmt.Sprintf("MessageType(%d)", int(m))
}

const (
	// maxNestingDepth is an arbitrary value to catch user mistakes.
	// For deeply nested schemas, it is expected the user will indicate
	// explicitly the maximum allowed recursion depth.
	maxNestingDepth = 64
)

// Message is an IPC message, including metadata and body.
type Message struct {
	refCount int64
	msg      *flatbuf.Message
	meta     *memory.Buffer
	body     *memory.Buffer
}

func NewMessage(meta, body *memory.Buffer) *Message {
	meta.Retain()
	body.Retain()
	return &Message{
		refCount: 1,
		msg:      flatbuf.GetRootAsMessage(meta.Bytes(), 0),
		meta:     meta,
		body:     body,
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (msg *Message) Retain() {
	atomic.AddInt64(&msg.refCount, 1)
}

// Release decreases the reference count by 1.
// Release may be called simultaneously from multiple goroutines.
// When the reference count goes to zero, the memory is freed.
func (msg *Message) Release() {
	debug.Assert(atomic.LoadInt64(&msg.refCount) > 0, "too many releases")

	if atomic.AddInt64(&msg.refCount, -1) == 0 {
		msg.meta.Release()
		msg.body.Release()
		msg.msg = nil
		msg.meta = nil
		msg.body = nil
	}
}

func (msg Message) Version() MetadataVersion {
	return MetadataVersion(msg.msg.Version())
}

func (msg Message) Type() MessageType {
	return MessageType(msg.msg.HeaderType())
}

func (msg Message) BodyLen() int64 {
	return msg.msg.BodyLength()
}
