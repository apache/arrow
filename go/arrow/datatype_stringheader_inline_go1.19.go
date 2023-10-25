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

//go:build !go1.20 && !tinygo

package arrow

import (
	"reflect"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow/internal/debug"
)

func (sh *StringHeader) InlineData() (data string) {
	debug.Assert(sh.IsInline(), "calling InlineData on non-inline StringHeader")

	h := (*reflect.StringHeader)(unsafe.Pointer(&data))
	h.Data = uintptr(unsafe.Pointer(&sh.data))
	h.Len = int(sh.size)
	return
}
