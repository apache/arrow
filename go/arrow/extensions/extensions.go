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

package extensions

import (
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/parquet/schema"
)

var canonicalExtensionTypes = []arrow.ExtensionType{
	&Bool8Type{},
	&UUIDType{},
	&OpaqueType{},
	&JSONType{},
}

func init() {
	for _, extType := range canonicalExtensionTypes {
		if err := arrow.RegisterExtensionType(extType); err != nil {
			panic(err)
		}
	}
}

// CustomParquetType is an interface that Arrow ExtensionTypes may implement
// to specify the target LogicalType to use when converting to Parquet.
//
// The PrimitiveType is not configurable, and is determined by a fixed mapping from
// the extension's StorageType to a Parquet type (see getParquetType in pqarrow source).
type CustomParquetType interface {
	ParquetLogicalType() schema.LogicalType
}
