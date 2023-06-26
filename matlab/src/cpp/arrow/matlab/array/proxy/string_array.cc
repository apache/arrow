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

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/bit/pack.h"
#include "arrow/matlab/bit/unpack.h"

namespace arrow::matlab::array::proxy {

        libmexclass::proxy::MakeResult StringArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            namespace mda = ::matlab::data;

            mda::StructArray opts = constructor_arguments[0];
            const mda::StringArray array_mda = opts[0]["MatlabArray"];
            const mda::TypedArray<bool> unpacked_validity_bitmap_mda = opts[0]["Valid"];

            const auto array_length = array_mda.getNumberOfElements();

            std::vector<std::string> strings;

            // Convert UTF-16 encoded MATLAB string values to UTF-8 encoded Arrow string values.
            strings.reserve(array_length);
            for (const str_utf16 : array_mda) {
                const auto str_utf8 = arrow::util::UTF16StringToUTF8(str_utf16);
                strings.push_back(str_utf8);
            }

            auto unpacked_validity_bitmap_ptr = bit::unpacked_as_ptr(unpacked_validity_bitmap_mda);

            // Build up an Arrow StringArray from a vector of UTF-8 encoded strings.
            arrow::StringBuilder builder;
            MATLAB_ERROR_IF_NOT_OK(builder.AppendValues(strings, unpacked_validity_bitmap_ptr), error::STRING_BUILDER_APPEND_FAILED);
            MATLAB_ASSIGN_OR_ERROR(auto array, builder.Finish(), error::STRING_BUILDER_FINISH_FAILED);

            // Note: the "type constructor function" for String is "utf8" and not "string".
            auto array_data = arrow::ArrayData::Make(data_type, array_length, {validity_bitmap_buffer, data_buffer});
            return std::make_shared<arrow::matlab::array::proxy::StringArray>(arrow::MakeArray(array_data));
        }

        void StringArray::toMATLAB(libmexclass::proxy::method::Context& context) {
            auto array_length = array->length();
            // TODO: Figure out how to get the raw data from an arrow::StringArray
            auto data_buffer = std::static_pointer_cast<arrow::BooleanArray>(array)->values();
            auto array_mda = // TODO: Make an MDA String Array from the string data buffer. 
            // TODO: Transcode from UTF-8 Arrow String to UTF-16 MATLAB String.
            context.outputs[0] = array_mda;
        }

}
