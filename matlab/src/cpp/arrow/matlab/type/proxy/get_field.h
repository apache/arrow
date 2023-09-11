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

// arrow includes
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/type/proxy/field.h"

// libmexclass includes
#include "libmexclass/proxy/ProxyManager.h"

// STL includes
#include <sstream>
#include <variant>

namespace arrow::matlab::type::proxy {

        namespace {
    
            libmexclass::error::Error makeUnknownFieldNameError(const std::string& name) {
                using namespace libmexclass::error;
                std::stringstream error_message_stream;
                error_message_stream << "Unknown field name: '";
                error_message_stream << name;
                error_message_stream << "'.";
                return Error{error::ARROW_UNKNOWN_FIELD_NAME, error_message_stream.str()};
            }
    
            libmexclass::error::Error makeZeroFieldsError() {
                using namespace libmexclass::error;
                return Error{error::ARROW_ZERO_FIELDS_NUMERIC_INDEX,
                             "Numeric indexing using the field method is not supported when the number of fields is 0"};
            }

            libmexclass::error::Error makeInvalidNumericIndexError(const int32_t matlab_index, const int32_t num_fields) {
                using namespace libmexclass::error;
                const std::string& error_message_id = std::string{error::ARROW_INVALID_NUMERIC_FIELD_INDEX};
                std::stringstream error_message_stream;
                error_message_stream << "Invalid field index: ";
                error_message_stream << matlab_index;
                error_message_stream << ". Field index must be between 1 and the number of fields (";
                error_message_stream << num_fields;
                error_message_stream << ").";
                const std::string& error_message = error_message_stream.str();
               return Error{error_message_id, error_message}; 
            }
    }


    using FieldProxyIDOrError = std::variant<uint64_t, libmexclass::error::Error>;

    template <typename T>
    FieldProxyIDOrError getFieldByIndex(const std::shared_ptr<T> obj, const int32_t matlab_index) {
        using namespace libmexclass::proxy;

        // Note: MATLAB uses 1-based indexing, so subtract 1.
        // arrow::Schema::field does not do any bounds checking.
        const int32_t index = matlab_index - 1;
        const auto num_fields = obj->num_fields();

        if (num_fields == 0) {
            return makeZeroFieldsError();
        }

        if (matlab_index < 1 || matlab_index > num_fields) {
            return makeInvalidNumericIndexError(matlab_index, num_fields);
        }
        auto field = obj->field(index);
        auto field_proxy = std::make_shared<proxy::Field>(std::move(field));

        auto field_proxy_id = ProxyManager::manageProxy(field_proxy);
        return field_proxy_id;
    }
}