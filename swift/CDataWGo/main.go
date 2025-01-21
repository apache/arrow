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
package main

/*
#cgo CFLAGS: -I./include
#cgo LDFLAGS: -L. libgo-swift.a -L/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib/swift/macosx
#cgo LDFLAGS: -L/usr/lib/swift/linux
#include <stdlib.h>
#include "go_swift.h"
*/
import "C"
import (
	"strconv"
	"unsafe"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/cdata"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func stringTypeFromSwift() {
	arrowSchema := &cdata.CArrowSchema{}
	swSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(arrowSchema))
	C.stringTypeFromSwift(swSchema)
	gofield, _ := cdata.ImportCArrowField(arrowSchema)
	if gofield.Name != "col1" {
		panic("Imported type has incorrect name")
	}
}

func stringTypeToSwift() {
	arrowSchema := &cdata.CArrowSchema{}
	swSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(arrowSchema))
	C.stringTypeFromSwift(swSchema)
	gofield, _ := cdata.ImportCArrowField(arrowSchema)
	if gofield.Name != "col1" {
		panic("Imported type has incorrect name")
	}
}

func arrayStringFromSwift() {
	arrowArray := &cdata.CArrowArray{}
	swarray := (*C.struct_ArrowArray)(unsafe.Pointer(arrowArray))
	C.arrayStringFromSwift(swarray)
	arr, _ := cdata.ImportCArrayWithType(arrowArray, arrow.BinaryTypes.String)
	if arr.Len() != 100 {
		panic("Array length is incorrect")
	}

	for i := 0; i < 100; i++ {
		if arr.ValueStr(i) != ("test" + strconv.Itoa(i)) {
			panic("Array value is incorrect")
		}
	}
}

func arrayIntFromSwift() {
	arrowArray := &cdata.CArrowArray{}
	swarray := (*C.struct_ArrowArray)(unsafe.Pointer(arrowArray))
	C.arrayIntFromSwift(swarray)
	arr, _ := cdata.ImportCArrayWithType(arrowArray, arrow.PrimitiveTypes.Int32)
	if arr.Len() != 100 {
		panic("Array length is incorrect")
	}

	vals := arr.(*array.Int32).Int32Values()
	// and that the values are correct
	for i, v := range vals {
		if v != int32(i) {
			panic("Array value is incorrect")
		}
	}
}

func arrayIntToSwift() {
	bld := array.NewUint32Builder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues([]uint32{1, 2, 3, 4}, []bool{true, true, true, true})
	goarray := bld.NewUint32Array()
	var carray cdata.CArrowArray
	cdata.ExportArrowArray(goarray, &carray, nil)
	swarray := (*C.struct_ArrowArray)(unsafe.Pointer(&carray))
	C.arrayIntToSwift(swarray)

	if swarray.release != nil {
		panic("Release was not called by swift to deallocate C array")
	}
}

func arrayStringToSwift() {
	bld := array.NewStringBuilder(memory.DefaultAllocator)
	defer bld.Release()
	bld.AppendValues([]string{"test0", "test1", "test2", "test3"}, []bool{true, true, true, true})
	goarray := bld.NewStringArray()
	var carray cdata.CArrowArray
	cdata.ExportArrowArray(goarray, &carray, nil)
	swarray := (*C.struct_ArrowArray)(unsafe.Pointer(&carray))
	C.arrayStringToSwift(swarray)

	if swarray.release != nil {
		panic("Release was not called by swift to deallocate C array")
	}
}

func main() {
	stringTypeFromSwift()
	stringTypeToSwift()
	arrayStringFromSwift()
	arrayIntFromSwift()
	arrayIntToSwift()
	arrayStringToSwift()
}
