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

package arrow

import (
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v13/arrow/endian"
)

var StringHeaderTraits stringHeaderTraits

const (
	StringHeaderSizeBytes = int(unsafe.Sizeof(StringHeader{}))
)

type stringHeaderTraits struct{}

func (stringHeaderTraits) BytesRequired(n int) int { return StringHeaderSizeBytes * n }

func (stringHeaderTraits) PutValue(b []byte, v StringHeader) {
	endian.Native.PutUint32(b, v.size)
	copy(b[4:], v.data[:])
}

func (stringHeaderTraits) CastFromBytes(b []byte) (res []StringHeader) {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / StringHeaderSizeBytes
	s.Cap = h.Cap / StringHeaderSizeBytes

	return
}

func (stringHeaderTraits) CastToBytes(b []StringHeader) (res []byte) {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * StringHeaderSizeBytes
	s.Cap = h.Cap * StringHeaderSizeBytes

	return
}

func (stringHeaderTraits) Copy(dst, src []StringHeader) { copy(dst, src) }
