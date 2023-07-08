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

#include "arrow/matlab/error/error.h"
#include "arrow/matlab/bit/pack.h"
#include "arrow/matlab/bit/unpack.h"

#include "arrow/matlab/type/time_unit.h"
#include "arrow/util/utf8.h"
#include "arrow/type.h"
#include "arrow/builder.h"


namespace arrow::matlab::array::proxy {

    namespace {
        const uint8_t* getUnpackedValidityBitmap(const ::matlab::data::TypedArray<bool>& valid_elements) {
            const auto valid_elements_iterator(valid_elements.cbegin());
            return reinterpret_cast<const uint8_t*>(valid_elements_iterator.operator->());
        }
    } // anonymous namespace

    libmexclass::proxy::MakeResult TimestampArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;

        mda::StructArray opts = constructor_arguments[0];

        // Get the mxArray from constructor arguments
        const mda::TypedArray<int64_t> timestamp_mda = opts[0]["MatlabArray"];
        const mda::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];
        
        const mda::TypedArray<mda::MATLABString> timezone_mda = opts[0]["TimeZone"];
        const mda::TypedArray<mda::MATLABString> units_mda = opts[0]["TimeUnit"];

        // extract the time zone string
        const std::u16string& u16_timezone = timezone_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto timezone, arrow::util::UTF16StringToUTF8(u16_timezone),
                               error::UNICODE_CONVERSION_ERROR_ID);

        // extract the time unit
        MATLAB_ASSIGN_OR_ERROR(const auto time_unit, arrow::matlab::type::timeUnitFromString(units_mda[0]),
                               error::UKNOWN_TIME_UNIT_ERROR_ID)

        // create the timestamp_type
        auto data_type = arrow::timestamp(time_unit, timezone);
        arrow::TimestampBuilder builder(data_type, arrow::default_memory_pool());

        // Get raw pointer of mxArray
        auto it(timestamp_mda.cbegin());
        auto dt = it.operator->();

        // Pack the validity bitmap values.
        const uint8_t* valid_mask = getUnpackedValidityBitmap(validity_bitmap_mda);
        const auto num_elements = timestamp_mda.getNumberOfElements();
        
        // Append values
        MATLAB_ERROR_IF_NOT_OK(builder.AppendValues(dt, num_elements, valid_mask), error::APPEND_VALUES_ERROR_ID);
        MATLAB_ASSIGN_OR_ERROR(auto timestamp_array, builder.Finish(), error::BUILD_ARRAY_ERROR_ID);

        return std::make_shared<arrow::matlab::array::proxy::TimestampArray>(timestamp_array);
    }

    void TimestampArray::toMATLAB(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        const auto num_elements = static_cast<size_t>(array->length());
        const auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(array);
        const int64_t* const data_begin = timestamp_array->raw_values();
        const int64_t* const data_end = data_begin + num_elements;

        mda::ArrayFactory factory;

        // Constructs a TypedArray from the raw values. Makes a copy.
        mda::TypedArray<int64_t> result = factory.createArray({num_elements, 1}, data_begin, data_end);
        context.outputs[0] = result;
    }
}
