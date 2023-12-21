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

#include "arrow/matlab/index/validate.h"

#include <sstream>

namespace arrow::matlab::index {

    namespace {
        std::string makeEmptyContainerErrorMessage() {
            return "Numeric indexing using the field method is not supported for objects with zero fields.";
        }

        std::string makeIndexOutOfRangeErrorMessage(const int32_t matlab_index, const int32_t num_fields) {
            std::stringstream error_message_stream;
            error_message_stream << "Invalid field index: ";
            // matlab uses 1-based indexing
            error_message_stream << matlab_index;
            error_message_stream << ". Field index must be between 1 and the number of fields (";
            error_message_stream << num_fields;
            error_message_stream << ").";
            return error_message_stream.str();
        }
    } // anonymous namespace 

    arrow::Status validateNonEmptyContainer(const int32_t num_fields) {
        if (num_fields == 0) {
            const auto msg = makeEmptyContainerErrorMessage();
            return arrow::Status::Invalid(std::move(msg));
        }
        return arrow::Status::OK();
    }

    arrow::Status validateInRange(const int32_t matlab_index, const int32_t num_fields) {
        if (matlab_index < 1 || matlab_index > num_fields) {
            const auto msg = makeIndexOutOfRangeErrorMessage(matlab_index, num_fields);
            return arrow::Status::Invalid(std::move(msg));
        }
        return arrow::Status::OK();
    }

    arrow::Status validateSliceOffset(const int64_t matlab_offset) {
        if (matlab_offset < 1) {
            const std::string msg = "Slice offset must be positive";
            return arrow::Status::Invalid(std::move(msg));
        }
        return arrow::Status::OK();
    }

        arrow::Status validateSliceLength(const int64_t length) {
        if (length < 0) {
            const std::string msg = "Slice length must be nonnegative";
            return arrow::Status::Invalid(std::move(msg));
        }
        return arrow::Status::OK();
    }
}