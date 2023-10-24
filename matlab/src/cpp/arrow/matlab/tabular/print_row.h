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

#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/scalar.h"

#include <sstream>

namespace arrow::matlab::tabular {

    namespace {
        arrow::Result<std::string> scalar_to_string(const std::shared_ptr<arrow::Scalar>& scalar) {
            using ID = arrow::Type::type;

            const auto type_id = scalar->type->id();
            if (arrow::is_primitive(type_id) || arrow::is_string(type_id)) {
                return scalar->ToString();
            } else if (type_id == ID::STRUCT) {
                return "<Struct>";
            } else if (type_id == ID::LIST) {
                return "<List>";
            } else {
                return arrow::Status::NotImplemented("Unsupported DataType: " + scalar->type->ToString());
            }
        }
    }  // namespace

    template <typename TabularType>
    arrow::Result<std::string> print_row(const std::shared_ptr<TabularType>& tabular_object, const int64_t matlab_row_index) {
        std::stringstream ss;
        const int64_t row_index = matlab_row_index - 1;
        
        if (row_index >= tabular_object->num_rows() || row_index < 0) {
            ss << "Invalid Row Index: " << matlab_row_index;
            return arrow::Status::Invalid(ss.str());
        }

        const auto num_columns = tabular_object->num_columns();
        const auto& columns = tabular_object->columns();

        for (int32_t i = 0; i < num_columns; ++i) {
            const auto& column = columns[i];
            ARROW_ASSIGN_OR_RAISE(auto scalar, column->GetScalar(row_index));
            ARROW_ASSIGN_OR_RAISE(auto str, scalar_to_string(scalar));
            ss << str;
            if (i + 1 < num_columns) {
                ss << " | ";
            }
        }
        return ss.str();
    }
}