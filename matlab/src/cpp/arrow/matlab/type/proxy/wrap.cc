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

#include "arrow/matlab/type/proxy/wrap.h"

#include "arrow/matlab/type/proxy/primitive_ctype.h"
#include "arrow/matlab/type/proxy/timestamp_type.h"
#include "arrow/matlab/type/proxy/time32_type.h"
#include "arrow/matlab/type/proxy/time64_type.h"
#include "arrow/matlab/type/proxy/date32_type.h"
#include "arrow/matlab/type/proxy/date64_type.h"
#include "arrow/matlab/type/proxy/string_type.h"
#include "arrow/matlab/type/proxy/list_type.h"
#include "arrow/matlab/type/proxy/struct_type.h"

namespace arrow::matlab::type::proxy {

    arrow::Result<std::shared_ptr<type::proxy::Type>> wrap(const std::shared_ptr<arrow::DataType>& type) {
        using ID = arrow::Type::type;
        switch (type->id()) {
            case ID::BOOL:
                return std::make_shared<PrimitiveCType<bool>>(std::static_pointer_cast<arrow::BooleanType>(type));
            case ID::UINT8:
                return std::make_shared<PrimitiveCType<uint8_t>>(std::static_pointer_cast<arrow::UInt8Type>(type));
            case ID::UINT16:
                return std::make_shared<PrimitiveCType<uint16_t>>(std::static_pointer_cast<arrow::UInt16Type>(type));
            case ID::UINT32:
                return std::make_shared<PrimitiveCType<uint32_t>>(std::static_pointer_cast<arrow::UInt32Type>(type));
            case ID::UINT64:
                return std::make_shared<PrimitiveCType<uint64_t>>(std::static_pointer_cast<arrow::UInt64Type>(type));
            case ID::INT8:
                return std::make_shared<PrimitiveCType<int8_t>>(std::static_pointer_cast<arrow::Int8Type>(type));
            case ID::INT16:
                return std::make_shared<PrimitiveCType<int16_t>>(std::static_pointer_cast<arrow::Int16Type>(type));
            case ID::INT32:
                return std::make_shared<PrimitiveCType<int32_t>>(std::static_pointer_cast<arrow::Int32Type>(type));
            case ID::INT64:
                return std::make_shared<PrimitiveCType<int64_t>>(std::static_pointer_cast<arrow::Int64Type>(type));
            case ID::FLOAT:
                return std::make_shared<PrimitiveCType<float>>(std::static_pointer_cast<arrow::FloatType>(type));
            case ID::DOUBLE:
                return std::make_shared<PrimitiveCType<double>>(std::static_pointer_cast<arrow::DoubleType>(type));
            case ID::TIMESTAMP:
                return std::make_shared<TimestampType>(std::static_pointer_cast<arrow::TimestampType>(type));
            case ID::TIME32:
                return std::make_shared<Time32Type>(std::static_pointer_cast<arrow::Time32Type>(type));
            case ID::TIME64:
                return std::make_shared<Time64Type>(std::static_pointer_cast<arrow::Time64Type>(type));
            case ID::DATE32:
                return std::make_shared<Date32Type>(std::static_pointer_cast<arrow::Date32Type>(type));
            case ID::DATE64:
                return std::make_shared<Date64Type>(std::static_pointer_cast<arrow::Date64Type>(type));
            case ID::STRING:
                return std::make_shared<StringType>(std::static_pointer_cast<arrow::StringType>(type));
            case ID::LIST:
                return std::make_shared<ListType>(std::static_pointer_cast<arrow::ListType>(type));
            case ID::STRUCT:
                return std::make_shared<StructType>(std::static_pointer_cast<arrow::StructType>(type));
            default:
                return arrow::Status::NotImplemented("Unsupported DataType: " + type->ToString());
        }
    }
}
