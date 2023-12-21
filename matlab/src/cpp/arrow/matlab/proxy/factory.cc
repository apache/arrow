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

#include "arrow/matlab/array/proxy/boolean_array.h"
#include "arrow/matlab/array/proxy/numeric_array.h"
#include "arrow/matlab/array/proxy/string_array.h"
#include "arrow/matlab/array/proxy/timestamp_array.h"
#include "arrow/matlab/array/proxy/time32_array.h"
#include "arrow/matlab/array/proxy/time64_array.h"
#include "arrow/matlab/array/proxy/struct_array.h"
#include "arrow/matlab/array/proxy/list_array.h"
#include "arrow/matlab/array/proxy/chunked_array.h"
#include "arrow/matlab/tabular/proxy/record_batch.h"
#include "arrow/matlab/tabular/proxy/table.h"
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/type/proxy/primitive_ctype.h"
#include "arrow/matlab/type/proxy/string_type.h"
#include "arrow/matlab/type/proxy/timestamp_type.h"
#include "arrow/matlab/type/proxy/date32_type.h"
#include "arrow/matlab/type/proxy/date64_type.h"
#include "arrow/matlab/type/proxy/time32_type.h"
#include "arrow/matlab/type/proxy/time64_type.h"
#include "arrow/matlab/type/proxy/struct_type.h"
#include "arrow/matlab/type/proxy/list_type.h"
#include "arrow/matlab/type/proxy/field.h"
#include "arrow/matlab/io/feather/proxy/writer.h"
#include "arrow/matlab/io/feather/proxy/reader.h"
#include "arrow/matlab/io/csv/proxy/table_writer.h"
#include "arrow/matlab/io/csv/proxy/table_reader.h"
#include "arrow/matlab/buffer/proxy/buffer.h"

#include "factory.h"

namespace arrow::matlab::proxy {

libmexclass::proxy::MakeResult Factory::make_proxy(const ClassName& class_name, const FunctionArguments& constructor_arguments) {
    REGISTER_PROXY(arrow.array.proxy.Float32Array  , arrow::matlab::array::proxy::NumericArray<arrow::FloatType>);
    REGISTER_PROXY(arrow.array.proxy.Float64Array  , arrow::matlab::array::proxy::NumericArray<arrow::DoubleType>);
    REGISTER_PROXY(arrow.array.proxy.UInt8Array    , arrow::matlab::array::proxy::NumericArray<arrow::UInt8Type>);
    REGISTER_PROXY(arrow.array.proxy.UInt16Array   , arrow::matlab::array::proxy::NumericArray<arrow::UInt16Type>);
    REGISTER_PROXY(arrow.array.proxy.UInt32Array   , arrow::matlab::array::proxy::NumericArray<arrow::UInt32Type>);
    REGISTER_PROXY(arrow.array.proxy.UInt64Array   , arrow::matlab::array::proxy::NumericArray<arrow::UInt64Type>);
    REGISTER_PROXY(arrow.array.proxy.Int8Array     , arrow::matlab::array::proxy::NumericArray<arrow::Int8Type>);
    REGISTER_PROXY(arrow.array.proxy.Int16Array    , arrow::matlab::array::proxy::NumericArray<arrow::Int16Type>);
    REGISTER_PROXY(arrow.array.proxy.Int32Array    , arrow::matlab::array::proxy::NumericArray<arrow::Int32Type>);
    REGISTER_PROXY(arrow.array.proxy.Int64Array    , arrow::matlab::array::proxy::NumericArray<arrow::Int64Type>);
    REGISTER_PROXY(arrow.array.proxy.BooleanArray  , arrow::matlab::array::proxy::BooleanArray);
    REGISTER_PROXY(arrow.array.proxy.StringArray   , arrow::matlab::array::proxy::StringArray);
    REGISTER_PROXY(arrow.array.proxy.StructArray   , arrow::matlab::array::proxy::StructArray);
    REGISTER_PROXY(arrow.array.proxy.ListArray     , arrow::matlab::array::proxy::ListArray);
    REGISTER_PROXY(arrow.array.proxy.TimestampArray, arrow::matlab::array::proxy::NumericArray<arrow::TimestampType>);
    REGISTER_PROXY(arrow.array.proxy.Time32Array   , arrow::matlab::array::proxy::NumericArray<arrow::Time32Type>);
    REGISTER_PROXY(arrow.array.proxy.Time64Array   , arrow::matlab::array::proxy::NumericArray<arrow::Time64Type>);
    REGISTER_PROXY(arrow.array.proxy.Date32Array   , arrow::matlab::array::proxy::NumericArray<arrow::Date32Type>);
    REGISTER_PROXY(arrow.array.proxy.Date64Array   , arrow::matlab::array::proxy::NumericArray<arrow::Date64Type>);
    REGISTER_PROXY(arrow.array.proxy.ChunkedArray  , arrow::matlab::array::proxy::ChunkedArray);
    REGISTER_PROXY(arrow.buffer.proxy.Buffer       , arrow::matlab::buffer::proxy::Buffer);
    REGISTER_PROXY(arrow.tabular.proxy.RecordBatch , arrow::matlab::tabular::proxy::RecordBatch);
    REGISTER_PROXY(arrow.tabular.proxy.Table       , arrow::matlab::tabular::proxy::Table);
    REGISTER_PROXY(arrow.tabular.proxy.Schema      , arrow::matlab::tabular::proxy::Schema);
    REGISTER_PROXY(arrow.type.proxy.Field          , arrow::matlab::type::proxy::Field);
    REGISTER_PROXY(arrow.type.proxy.Float32Type    , arrow::matlab::type::proxy::PrimitiveCType<float>);
    REGISTER_PROXY(arrow.type.proxy.Float64Type    , arrow::matlab::type::proxy::PrimitiveCType<double>);
    REGISTER_PROXY(arrow.type.proxy.UInt8Type      , arrow::matlab::type::proxy::PrimitiveCType<uint8_t>);
    REGISTER_PROXY(arrow.type.proxy.UInt16Type     , arrow::matlab::type::proxy::PrimitiveCType<uint16_t>);
    REGISTER_PROXY(arrow.type.proxy.UInt32Type     , arrow::matlab::type::proxy::PrimitiveCType<uint32_t>);
    REGISTER_PROXY(arrow.type.proxy.UInt64Type     , arrow::matlab::type::proxy::PrimitiveCType<uint64_t>);
    REGISTER_PROXY(arrow.type.proxy.Int8Type       , arrow::matlab::type::proxy::PrimitiveCType<int8_t>);
    REGISTER_PROXY(arrow.type.proxy.Int16Type      , arrow::matlab::type::proxy::PrimitiveCType<int16_t>);
    REGISTER_PROXY(arrow.type.proxy.Int32Type      , arrow::matlab::type::proxy::PrimitiveCType<int32_t>);
    REGISTER_PROXY(arrow.type.proxy.Int64Type      , arrow::matlab::type::proxy::PrimitiveCType<int64_t>);
    REGISTER_PROXY(arrow.type.proxy.BooleanType    , arrow::matlab::type::proxy::PrimitiveCType<bool>);
    REGISTER_PROXY(arrow.type.proxy.StringType     , arrow::matlab::type::proxy::StringType);
    REGISTER_PROXY(arrow.type.proxy.TimestampType  , arrow::matlab::type::proxy::TimestampType);
    REGISTER_PROXY(arrow.type.proxy.Time32Type     , arrow::matlab::type::proxy::Time32Type);
    REGISTER_PROXY(arrow.type.proxy.Time64Type     , arrow::matlab::type::proxy::Time64Type);
    REGISTER_PROXY(arrow.type.proxy.Date32Type     , arrow::matlab::type::proxy::Date32Type);
    REGISTER_PROXY(arrow.type.proxy.Date64Type     , arrow::matlab::type::proxy::Date64Type);
    REGISTER_PROXY(arrow.type.proxy.StructType     , arrow::matlab::type::proxy::StructType);
    REGISTER_PROXY(arrow.type.proxy.ListType       , arrow::matlab::type::proxy::ListType);
    REGISTER_PROXY(arrow.io.feather.proxy.Writer   , arrow::matlab::io::feather::proxy::Writer);
    REGISTER_PROXY(arrow.io.feather.proxy.Reader   , arrow::matlab::io::feather::proxy::Reader);
    REGISTER_PROXY(arrow.io.csv.proxy.TableWriter  , arrow::matlab::io::csv::proxy::TableWriter);
    REGISTER_PROXY(arrow.io.csv.proxy.TableReader  , arrow::matlab::io::csv::proxy::TableReader);

    return libmexclass::error::Error{error::UNKNOWN_PROXY_ERROR_ID, "Did not find matching C++ proxy for " + class_name};
};

}
