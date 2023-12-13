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


#include "arrow/matlab/array/proxy/wrap.h"
#include "arrow/matlab/array/proxy/array.h"
#include "arrow/matlab/array/proxy/boolean_array.h"
#include "arrow/matlab/array/proxy/numeric_array.h"
#include "arrow/matlab/array/proxy/string_array.h"
#include "arrow/matlab/array/proxy/list_array.h"
#include "arrow/matlab/array/proxy/struct_array.h"

namespace arrow::matlab::array::proxy {

    arrow::Result<std::shared_ptr<proxy::Array>> wrap(const std::shared_ptr<arrow::Array>& array) {
        using ID = arrow::Type::type;
        switch (array->type_id()) {
            case ID::BOOL:
                return std::make_shared<proxy::BooleanArray>(std::static_pointer_cast<arrow::BooleanArray>(array));
            case ID::UINT8:
                return std::make_shared<proxy::NumericArray<arrow::UInt8Type>>(std::static_pointer_cast<arrow::UInt8Array>(array));
            case ID::UINT16:
                return std::make_shared<proxy::NumericArray<arrow::UInt16Type>>(std::static_pointer_cast<arrow::UInt16Array>(array));
            case ID::UINT32:
                return std::make_shared<proxy::NumericArray<arrow::UInt32Type>>(std::static_pointer_cast<arrow::UInt32Array>(array));
            case ID::UINT64:
                return std::make_shared<proxy::NumericArray<arrow::UInt64Type>>(std::static_pointer_cast<arrow::UInt64Array>(array));
            case ID::INT8:
                return std::make_shared<proxy::NumericArray<arrow::Int8Type>>(std::static_pointer_cast<arrow::Int8Array>(array));
            case ID::INT16:
                return std::make_shared<proxy::NumericArray<arrow::Int16Type>>(std::static_pointer_cast<arrow::Int16Array>(array));
            case ID::INT32:
                return std::make_shared<proxy::NumericArray<arrow::Int32Type>>(std::static_pointer_cast<arrow::Int32Array>(array));
            case ID::INT64:
                return std::make_shared<proxy::NumericArray<arrow::Int64Type>>(std::static_pointer_cast<arrow::Int64Array>(array));
            case ID::FLOAT:
                return std::make_shared<proxy::NumericArray<arrow::FloatType>>(std::static_pointer_cast<arrow::FloatArray>(array));
            case ID::DOUBLE:
                return std::make_shared<proxy::NumericArray<arrow::DoubleType>>(std::static_pointer_cast<arrow::DoubleArray>(array));
            case ID::TIMESTAMP:
                return std::make_shared<proxy::NumericArray<arrow::TimestampType>>(std::static_pointer_cast<arrow::TimestampArray>(array));
            case ID::TIME32:
                return std::make_shared<proxy::NumericArray<arrow::Time32Type>>(std::static_pointer_cast<arrow::Time32Array>(array));
            case ID::TIME64:
                return std::make_shared<proxy::NumericArray<arrow::Time64Type>>(std::static_pointer_cast<arrow::Time64Array>(array));
            case ID::DATE32:
                return std::make_shared<proxy::NumericArray<arrow::Date32Type>>(std::static_pointer_cast<arrow::Date32Array>(array));
            case ID::DATE64:
                return std::make_shared<proxy::NumericArray<arrow::Date64Type>>(std::static_pointer_cast<arrow::Date64Array>(array));
            case ID::STRING:
                return std::make_shared<proxy::StringArray>(std::static_pointer_cast<arrow::StringArray>(array));
            case ID::LIST:
                return std::make_shared<proxy::ListArray>(std::static_pointer_cast<arrow::ListArray>(array));
            case ID::STRUCT:
                return std::make_shared<proxy::StructArray>(std::static_pointer_cast<arrow::StructArray>(array));
            default:
                return arrow::Status::NotImplemented("Unsupported DataType: " + array->type()->ToString());
        }
    }
}
