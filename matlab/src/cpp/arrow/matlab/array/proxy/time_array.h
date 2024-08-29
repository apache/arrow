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

#include "arrow/matlab/array/proxy/numeric_array.h"

#include "arrow/matlab/type/time_unit.h"
#include "arrow/util/utf8.h"
#include "arrow/type_traits.h"

namespace arrow::matlab::array::proxy {

    template <typename ArrowType>
    using is_time = arrow::is_time_type<ArrowType>;

    template <typename ArrowType>
    using enable_if_time = std::enable_if_t<is_time<ArrowType>::value, bool>; 

    template <typename ArrowType, enable_if_time<ArrowType> = true>
    libmexclass::proxy::MakeResult make_time_array(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
       namespace mda = ::matlab::data;
       using namespace arrow::matlab::type;
       using MatlabBuffer = arrow::matlab::buffer::MatlabBuffer;
       using TimeArray =  arrow::NumericArray<ArrowType>; 
       using TimeArrayProxy = proxy::NumericArray<ArrowType>;
       using CType = typename arrow::TypeTraits<ArrowType>::CType;

       mda::StructArray opts = constructor_arguments[0];

       const mda::TypedArray<CType> time_mda = opts[0]["MatlabArray"];
       const mda::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];
       const mda::TypedArray<mda::MATLABString> time_unit_mda = opts[0]["TimeUnit"];

       // extract the time unit
       const std::u16string& time_unit_utf16 = time_unit_mda[0];
       MATLAB_ASSIGN_OR_ERROR(const auto time_unit,
                              timeUnitFromString(time_unit_utf16),
                              error::UNKNOWN_TIME_UNIT_ERROR_ID);

       MATLAB_ERROR_IF_NOT_OK(validateTimeUnit<ArrowType>(time_unit),
                              error::INVALID_TIME_UNIT);

       // create the ArrowType
       const auto data_type = std::make_shared<ArrowType>(time_unit);

       auto array_length = static_cast<size_t>(time_mda.getNumberOfElements()); 
       auto data_buffer = std::make_shared<MatlabBuffer>(time_mda);

       // Pack the validity bitmap values.
       MATLAB_ASSIGN_OR_ERROR(auto packed_validity_bitmap,
                              bit::packValid(validity_bitmap_mda),
                              error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

       auto array_data = arrow::ArrayData::Make(data_type, array_length, {packed_validity_bitmap, data_buffer});
       auto time_array = std::static_pointer_cast<TimeArray>(arrow::MakeArray(array_data));
       return std::make_shared<TimeArrayProxy>(std::move(time_array));

    }
}