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

#include "arrow/matlab/array/proxy/string_array.h"
#include "arrow/matlab/type/proxy/string_type.h"

#include "arrow/array/builder_binary.h"

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/bit/pack.h"
#include "arrow/matlab/bit/unpack.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::array::proxy {

        StringArray::StringArray(const std::shared_ptr<arrow::StringArray> string_array) 
            : arrow::matlab::array::proxy::Array(std::move(string_array)) {
                REGISTER_METHOD(StringArray, toMATLAB);
            }

        libmexclass::proxy::MakeResult StringArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            namespace mda = ::matlab::data;

            mda::StructArray opts = constructor_arguments[0];
            const mda::StringArray array_mda = opts[0]["MatlabArray"];
            const mda::TypedArray<bool> unpacked_validity_bitmap_mda = opts[0]["Valid"];

            // Convert UTF-16 encoded MATLAB string values to UTF-8 encoded Arrow string values.
            const auto array_length = array_mda.getNumberOfElements();
            std::vector<std::string> strings;
            strings.reserve(array_length);
            for (const auto& str : array_mda) {
                if (!str) {
                    // Substitute MATLAB string(missing) values with the empty string value ("")
                    strings.emplace_back("");
                } else {
                    MATLAB_ASSIGN_OR_ERROR(auto str_utf8, arrow::util::UTF16StringToUTF8(*str), error::UNICODE_CONVERSION_ERROR_ID);
                    strings.push_back(std::move(str_utf8));
                }
            }

            auto unpacked_validity_bitmap_ptr = bit::extract_ptr(unpacked_validity_bitmap_mda);

            // Build up an Arrow StringArray from a vector of UTF-8 encoded strings.
            arrow::StringBuilder builder;
            MATLAB_ERROR_IF_NOT_OK(builder.AppendValues(strings, unpacked_validity_bitmap_ptr), error::STRING_BUILDER_APPEND_FAILED);
            MATLAB_ASSIGN_OR_ERROR(auto array, builder.Finish(), error::STRING_BUILDER_FINISH_FAILED);
            auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
            return std::make_shared<arrow::matlab::array::proxy::StringArray>(std::move(typed_array));
        }

        void StringArray::toMATLAB(libmexclass::proxy::method::Context& context) {
            namespace mda = ::matlab::data;

            // Convert UTF-8 encoded Arrow string values to UTF-16 encoded MATLAB string values.
            auto array_length = static_cast<size_t>(array->length());
            std::vector<mda::MATLABString> strings;
            strings.reserve(array_length);
            for (size_t i = 0; i < array_length; ++i) {
                auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                auto str_utf8 = string_array->GetView(i);
                MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
                const mda::MATLABString matlab_string = mda::MATLABString(std::move(str_utf16));
                strings.push_back(matlab_string);
            }

            // Create a MATLAB String array from a vector of UTF-16 encoded strings.
            mda::ArrayFactory factory;
            auto array_mda = factory.createArray({array_length, 1}, strings.begin(), strings.end());
            context.outputs[0] = array_mda;
        }
}
