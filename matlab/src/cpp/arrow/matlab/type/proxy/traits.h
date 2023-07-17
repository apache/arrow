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

#include "arrow/type_fwd.h"
#include "arrow/matlab/type/primitive_ctype.h"
#include "arrow/matlab/type/timestamp_type.h"
#include "arrow/matlab/type/string_type.h"

#include <string_view>

namespace arrow::matlab::type::proxy {
    
     // A type traits class mapping Arrow types to MATLAB types.
    template <typename ArrowType>
    struct Traits;

    template <>
    struct Traits<arrow::FloatType> {
        using TypeProxy = PrimitiveCType<float>;

    };

    template <>
    struct Traits<arrow::DoubleType> {
        using TypeProxy = PrimitiveCType<double>;
    };

    template <>
    struct Traits<arrow::Int8Type> {
        using TypeProxy = PrimitiveCType<int8_t>;
    };

    template <>
    struct Traits<arrow::Int16Type> {
        using TypeProxy = PrimitiveCType<int16_t>;
    };

    template <>
    struct Traits<arrow::Int32Type> {
        using TypeProxy = PrimitiveCType<int32_t>;
    };

    template <>
    struct Traits<arrow::Int64Type> {
        using TypeProxy = PrimitiveCType<int64_t>;
    };

    template <>
    struct Traits<arrow::UInt8Type> {
        using TypeProxy = PrimitiveCType<uint8_t>;
    };

    template <>
    struct Traits<arrow::UInt16Type> {
        using TypeProxy = PrimitiveCType<uint16_t>;
    };

    template <>
    struct Traits<arrow::UInt32Type> {
        using TypeProxy = PrimitiveCType<uint32_t>;
    };

    template <>
    struct Traits<arrow::UInt64Type> {
        using TypeProxy = PrimitiveCType<uint64_t>;
    };

    template <>
    struct Traits<arrow::String> {
        using TypeProxy = StringType;
    };

    template <>
    struct Traits<arrow::TimestampType> {
        using TypeProxy = TimestampType;
    };
}
