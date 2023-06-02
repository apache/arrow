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

#include "arrow/matlab/array/proxy/array.h"

#include "arrow/matlab/bit/bit_unpack_arrow_buffer.h"

namespace arrow::matlab::array::proxy {

    Array::Array(const libmexclass::proxy::FunctionArguments& constructor_arguments) {

        // Register Proxy methods.
        REGISTER_METHOD(Array, toString);
        REGISTER_METHOD(Array, toMATLAB);
        REGISTER_METHOD(Array, length);
        REGISTER_METHOD(Array, valid);
    }

    void Array::toString(libmexclass::proxy::method::Context& context) {
        ::matlab::data::ArrayFactory factory;

        // TODO: handle non-ascii characters
        auto str_mda = factory.createScalar(array->ToString());
        context.outputs[0] = str_mda;
    }

    void Array::length(libmexclass::proxy::method::Context& context) {
        ::matlab::data::ArrayFactory factory;
        auto length_mda = factory.createScalar(array->length());
        context.outputs[0] = length_mda;
    }

    void Array::valid(libmexclass::proxy::method::Context& context) {
        auto array_length = static_cast<size_t>(array->length());
        
        // If the Arrow array has no null values, then return a MATLAB
        // logical array that is all "true" for the validity bitmap.
        if (array->null_count() == 0) {
            ::matlab::data::ArrayFactory factory;
            auto validity_buffer = factory.createBuffer<bool>(array_length);
            auto validity_buffer_ptr = validity_buffer.get();
            std::fill(validity_buffer_ptr, validity_buffer_ptr + array_length, true);
            auto valid_elements_mda = factory.createArrayFromBuffer<bool>({array_length, 1}, std::move(validity_buffer));
            context.outputs[0] = valid_elements_mda;
            return;
        }

        auto validity_bitmap = array->null_bitmap();
        auto valid_elements_mda = arrow::matlab::bit::bitUnpackArrowBuffer(validity_bitmap, array_length);
        context.outputs[0] = valid_elements_mda;
    }

}
