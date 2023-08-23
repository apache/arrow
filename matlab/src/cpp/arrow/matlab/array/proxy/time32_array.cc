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

#include "arrow/matlab/array/proxy/time32_array.h"

#include "arrow/matlab/type/time_unit.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::array::proxy {

    // Specialization of NumericArray::Make for arrow::Time32Type
    template <>
    libmexclass::proxy::MakeResult Time32Array::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
       namespace mda = ::matlab::data;
       using MatlabBuffer = arrow::matlab::buffer::MatlabBuffer;
       using Time32Array = arrow::Time32Array;
       using Time32ArrayProxy = arrow::matlab::array::proxy::NumericArray<arrow::Time32Type>;

       mda::StructArray opts = constructor_arguments[0];

       // Get the mxArray from constructor arguments
       const mda::TypedArray<int32_t> time32_mda = opts[0]["MatlabArray"];
       const mda::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];
       
       const mda::TypedArray<mda::MATLABString> units_mda = opts[0]["TimeUnit"];

       // extract the time unit
       const std::u16string& u16_timeunit = units_mda[0];
       MATLAB_ASSIGN_OR_ERROR(const auto time_unit,
                              arrow::matlab::type::timeUnitFromString(u16_timeunit),
                              error::UKNOWN_TIME_UNIT_ERROR_ID)

       // create the Time32Type
       auto data_type = arrow::time32(time_unit);
       auto array_length = static_cast<int32_t>(time32_mda.getNumberOfElements()); // cast size_t to int32_t

       auto data_buffer = std::make_shared<MatlabBuffer>(time32_mda);

       // Pack the validity bitmap values.
       MATLAB_ASSIGN_OR_ERROR(auto packed_validity_bitmap,
                              bit::packValid(validity_bitmap_mda),
                              error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

       auto array_data = arrow::ArrayData::Make(data_type, array_length, {packed_validity_bitmap, data_buffer});
       auto time32_array = std::static_pointer_cast<Time32Array>(arrow::MakeArray(array_data));
       return std::make_shared<Time32ArrayProxy>(std::move(time32_array));
    }
}
