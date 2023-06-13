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

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/bit/bit_pack_matlab_logical_array.h"
#include "arrow/matlab/bit/bit_unpack_arrow_buffer.h"

namespace arrow::matlab::array::proxy {

        libmexclass::proxy::MakeResult BooleanArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            // Get the mxArray from constructor arguments
            const ::matlab::data::TypedArray<bool> logical_mda = constructor_arguments[0];
            const ::matlab::data::TypedArray<bool> validity_bitmap_mda = constructor_arguments[1];

            // Pack the logical data values.
            auto maybe_packed_logical_buffer = arrow::matlab::bit::bitPackMatlabLogicalArray(logical_mda);
            MATLAB_ERROR_IF_NOT_OK(maybe_packed_logical_buffer.status(), error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

            // Pack the validity bitmap values.
            auto maybe_validity_bitmap_buffer = arrow::matlab::bit::bitPackMatlabLogicalArray(validity_bitmap_mda);
            MATLAB_ERROR_IF_NOT_OK(maybe_validity_bitmap_buffer.status(), error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

            const auto data_type = arrow::boolean();
            const auto array_length = logical_mda.getNumberOfElements();
            const auto validity_bitmap_buffer = *maybe_validity_bitmap_buffer;
            const auto data_buffer = *maybe_packed_logical_buffer;

            auto array_data = arrow::ArrayData::Make(data_type, array_length, {validity_bitmap_buffer, data_buffer});
            return std::make_shared<arrow::matlab::array::proxy::BooleanArray>(arrow::MakeArray(array_data));
        }

        void BooleanArray::toMATLAB(libmexclass::proxy::method::Context& context) {
            auto array_length = array->length();
            auto packed_logical_data_buffer = std::static_pointer_cast<arrow::BooleanArray>(array)->values();
            auto logical_array_mda = arrow::matlab::bit::bitUnpackArrowBuffer(packed_logical_data_buffer, array_length);
            context.outputs[0] = logical_array_mda;
        }

}
