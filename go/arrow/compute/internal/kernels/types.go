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

package kernels

import (
	"github.com/apache/arrow/go/v10/arrow"
)

var (
	unsignedIntTypes = []arrow.DataType{
		arrow.PrimitiveTypes.Uint8,
		arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32,
		arrow.PrimitiveTypes.Uint64,
	}
	signedIntTypes = []arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32,
		arrow.PrimitiveTypes.Int64,
	}
	intTypes      = append(unsignedIntTypes, signedIntTypes...)
	floatingTypes = []arrow.DataType{
		arrow.FixedWidthTypes.Float16,
		arrow.PrimitiveTypes.Float32,
		arrow.PrimitiveTypes.Float64,
	}
	numericTypes = append(intTypes, floatingTypes...)
	// binary types without fixedsize binary
	baseBinaryTypes = []arrow.DataType{
		arrow.BinaryTypes.Binary,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.String,
		arrow.BinaryTypes.LargeString}
	// non-parametric, non-nested types
	primitiveTypes = append(append([]arrow.DataType{
		arrow.Null, arrow.FixedWidthTypes.Boolean,
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Date64},
		numericTypes...), baseBinaryTypes...)
)
