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

#pragma once

#include "arrow/type_fwd.h"

#include "arrow/matlab/type/proxy/primitive_ctype.h"
#include "arrow/matlab/type/proxy/timestamp_type.h"
#include "arrow/matlab/type/proxy/time32_type.h"
#include "arrow/matlab/type/proxy/time64_type.h"
#include "arrow/matlab/type/proxy/date32_type.h"
#include "arrow/matlab/type/proxy/date64_type.h"
#include "arrow/matlab/type/proxy/string_type.h"

namespace arrow::matlab::type::proxy {

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
    struct Traits<arrow::StringType> {
        using TypeProxy = StringType;
    };

    template <>
    struct Traits<arrow::TimestampType> {
        using TypeProxy = TimestampType;
    };

    template <>
    struct Traits<arrow::Time32Type> {
        using TypeProxy = Time32Type;
    };

    template <>
    struct Traits<arrow::Time64Type> {
        using TypeProxy = Time64Type;
    };

    template <>
    struct Traits<arrow::Date32Type> {
        using TypeProxy = Date32Type;
    };

    template <>
    struct Traits<arrow::Date64Type> {
        using TypeProxy = Date64Type;
    };
}
