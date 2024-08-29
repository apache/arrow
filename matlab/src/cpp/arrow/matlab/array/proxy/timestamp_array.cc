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

#include "arrow/matlab/array/proxy/timestamp_array.h"

#include "arrow/matlab/type/time_unit.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::array::proxy {

    // Specialization of NumericArray::Make for arrow::TimestampType.
    template <>
    libmexclass::proxy::MakeResult TimestampArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
       namespace mda = ::matlab::data;
       using MatlabBuffer = arrow::matlab::buffer::MatlabBuffer;
       using TimestampArray = arrow::TimestampArray;
       using TimestampArrayProxy = arrow::matlab::array::proxy::NumericArray<arrow::TimestampType>;

       mda::StructArray opts = constructor_arguments[0];

       // Get the mxArray from constructor arguments
       const mda::TypedArray<int64_t> timestamp_mda = opts[0]["MatlabArray"];
       const mda::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];
       
       const mda::TypedArray<mda::MATLABString> timezone_mda = opts[0]["TimeZone"];
       const mda::TypedArray<mda::MATLABString> units_mda = opts[0]["TimeUnit"];

       // extract the time zone string
       const std::u16string& u16_timezone = timezone_mda[0];
       MATLAB_ASSIGN_OR_ERROR(const auto timezone,
                              arrow::util::UTF16StringToUTF8(u16_timezone),
                              error::UNICODE_CONVERSION_ERROR_ID);

       // extract the time unit
       const std::u16string& u16_timeunit = units_mda[0];
       MATLAB_ASSIGN_OR_ERROR(const auto time_unit,
                              arrow::matlab::type::timeUnitFromString(u16_timeunit),
                              error::UNKNOWN_TIME_UNIT_ERROR_ID)

       // create the timestamp_type
       auto data_type = arrow::timestamp(time_unit, timezone);
       auto array_length = static_cast<int64_t>(timestamp_mda.getNumberOfElements()); // cast size_t to int64_t

       auto data_buffer = std::make_shared<MatlabBuffer>(timestamp_mda);

       // Pack the validity bitmap values.
       MATLAB_ASSIGN_OR_ERROR(auto packed_validity_bitmap,
                              bit::packValid(validity_bitmap_mda),
                              error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

       auto array_data = arrow::ArrayData::Make(data_type, array_length, {packed_validity_bitmap, data_buffer});
       auto timestamp_array = std::static_pointer_cast<TimestampArray>(arrow::MakeArray(array_data));
       return std::make_shared<TimestampArrayProxy>(std::move(timestamp_array));
    }
}
