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

package testutils

import (
	"reflect"

	"github.com/apache/arrow/go/v13/parquet"
)

var typeToParquetTypeMap = map[reflect.Type]parquet.Type{
	reflect.TypeOf(true):                        parquet.Types.Boolean,
	reflect.TypeOf(int32(0)):                    parquet.Types.Int32,
	reflect.TypeOf(int64(0)):                    parquet.Types.Int64,
	reflect.TypeOf(float32(0)):                  parquet.Types.Float,
	reflect.TypeOf(float64(0)):                  parquet.Types.Double,
	reflect.TypeOf(parquet.ByteArray{}):         parquet.Types.ByteArray,
	reflect.TypeOf(parquet.Int96{}):             parquet.Types.Int96,
	reflect.TypeOf(parquet.FixedLenByteArray{}): parquet.Types.FixedLenByteArray,
}

func TypeToParquetType(typ reflect.Type) parquet.Type {
	ret, ok := typeToParquetTypeMap[typ]
	if !ok {
		panic("invalid type for parquet type")
	}
	return ret
}
