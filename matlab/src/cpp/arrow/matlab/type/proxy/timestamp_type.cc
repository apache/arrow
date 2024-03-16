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

#include "arrow/matlab/type/proxy/timestamp_type.h"
#include "arrow/matlab/type/time_unit.h"
#include "arrow/matlab/error/error.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::type::proxy {

    TimestampType::TimestampType(std::shared_ptr<arrow::TimestampType> timestamp_type) : FixedWidthType(std::move(timestamp_type)) {
        REGISTER_METHOD(TimestampType, getTimeUnit);
        REGISTER_METHOD(TimestampType, getTimeZone);
    }

    libmexclass::proxy::MakeResult TimestampType::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        
        using TimestampTypeProxy = arrow::matlab::type::proxy::TimestampType;

        mda::StructArray opts = constructor_arguments[0];

        // Get the mxArray from constructor arguments
        const mda::StringArray timezone_mda = opts[0]["TimeZone"];
        const mda::StringArray timeunit_mda = opts[0]["TimeUnit"];

        // extract the time zone
        const std::u16string& utf16_timezone = timezone_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto timezone,
                               arrow::util::UTF16StringToUTF8(utf16_timezone),
                               error::UNICODE_CONVERSION_ERROR_ID);

        // extract the time unit
        const std::u16string& utf16_timeunit = timeunit_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto timeunit,
                               arrow::matlab::type::timeUnitFromString(utf16_timeunit),
                               error::UNKNOWN_TIME_UNIT_ERROR_ID);

        auto type = arrow::timestamp(timeunit, timezone);
        auto time_type = std::static_pointer_cast<arrow::TimestampType>(type);
        return std::make_shared<TimestampTypeProxy>(std::move(time_type));
    }

    void TimestampType::getTimeZone(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(data_type);
        const auto timezone_utf8 = timestamp_type->timezone();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto timezone_utf16, 
                                            arrow::util::UTF8StringToUTF16(timezone_utf8),
                                            context, error::UNICODE_CONVERSION_ERROR_ID);
        auto timezone_mda = factory.createScalar(timezone_utf16);
        context.outputs[0] = timezone_mda;
    }

    void TimestampType::getTimeUnit(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(data_type);
        const auto timeunit = timestamp_type->unit();
        auto timeunit_mda = factory.createScalar(static_cast<int16_t>(timeunit)); 
        context.outputs[0] = timeunit_mda;
    }
}
