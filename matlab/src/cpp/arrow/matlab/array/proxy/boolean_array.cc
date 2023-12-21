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
#include "arrow/matlab/type/proxy/primitive_ctype.h"

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/bit/pack.h"
#include "arrow/matlab/bit/unpack.h"

namespace arrow::matlab::array::proxy {

        BooleanArray::BooleanArray(std::shared_ptr<arrow::BooleanArray> array) 
            : arrow::matlab::array::proxy::Array{std::move(array)} {
                REGISTER_METHOD(BooleanArray, toMATLAB);
            }

        libmexclass::proxy::MakeResult BooleanArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            ::matlab::data::StructArray opts = constructor_arguments[0];

            // Get the mxArray from constructor arguments
            const ::matlab::data::TypedArray<bool> logical_mda = opts[0]["MatlabArray"];
            const ::matlab::data::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];

            // Pack the logical data values.
            MATLAB_ASSIGN_OR_ERROR(auto data_buffer, bit::pack(logical_mda), error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

            // Pack the validity bitmap values.
            MATLAB_ASSIGN_OR_ERROR(const auto validity_bitmap_buffer, bit::packValid(validity_bitmap_mda), error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

            const auto data_type = arrow::boolean();
            const auto array_length = logical_mda.getNumberOfElements();

            auto array_data = arrow::ArrayData::Make(data_type, array_length, {validity_bitmap_buffer, data_buffer});
            auto arrow_array = std::static_pointer_cast<arrow::BooleanArray>(arrow::MakeArray(array_data));
            return std::make_shared<arrow::matlab::array::proxy::BooleanArray>(std::move(arrow_array));
        }

        void BooleanArray::toMATLAB(libmexclass::proxy::method::Context& context) {
            auto array_length = array->length();
            auto packed_logical_data_buffer = std::static_pointer_cast<arrow::BooleanArray>(array)->values();
            auto logical_array_mda = bit::unpack(packed_logical_data_buffer, array_length, array->offset());
            context.outputs[0] = logical_array_mda;
        }
}
