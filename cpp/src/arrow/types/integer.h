// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_TYPES_INTEGER_H
#define ARROW_TYPES_INTEGER_H

#include <cstdint>
#include <string>

#include "arrow/types/primitive.h"
#include "arrow/type.h"

namespace arrow {

// Array containers

typedef PrimitiveArrayImpl<UInt8Type> UInt8Array;
typedef PrimitiveArrayImpl<Int8Type> Int8Array;

typedef PrimitiveArrayImpl<UInt16Type> UInt16Array;
typedef PrimitiveArrayImpl<Int16Type> Int16Array;

typedef PrimitiveArrayImpl<UInt32Type> UInt32Array;
typedef PrimitiveArrayImpl<Int32Type> Int32Array;

typedef PrimitiveArrayImpl<UInt64Type> UInt64Array;
typedef PrimitiveArrayImpl<Int64Type> Int64Array;

// Builders

typedef PrimitiveBuilder<UInt8Type, UInt8Array> UInt8Builder;
typedef PrimitiveBuilder<UInt16Type, UInt16Array> UInt16Builder;
typedef PrimitiveBuilder<UInt32Type, UInt32Array> UInt32Builder;
typedef PrimitiveBuilder<UInt64Type, UInt64Array> UInt64Builder;

typedef PrimitiveBuilder<Int8Type, Int8Array> Int8Builder;
typedef PrimitiveBuilder<Int16Type, Int16Array> Int16Builder;
typedef PrimitiveBuilder<Int32Type, Int32Array> Int32Builder;
typedef PrimitiveBuilder<Int64Type, Int64Array> Int64Builder;

} // namespace arrow

#endif // ARROW_TYPES_INTEGER_H
