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

#include "arrow/matlab/type/proxy/time_type.h"
#include "arrow/matlab/type/proxy/traits.h"
#include "arrow/matlab/type/time_unit.h"
#include "arrow/matlab/error/error.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::type::proxy {

    TimeType::TimeType(std::shared_ptr<arrow::TimeType> time_type) : FixedWidthType(std::move(time_type)) {
        REGISTER_METHOD(TimeType, getTimeUnit);
    }

    void TimeType::getTimeUnit(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        auto time_type = std::static_pointer_cast<arrow::TimeType>(data_type);
        const auto time_unit = time_type->unit();
        // Cast to uint8_t since there are only four supported TimeUnit enumeration values:
        // Nanosecond, Microsecond, Millisecond, Second
        auto timeunit_mda = factory.createScalar(static_cast<uint8_t>(time_unit));
        context.outputs[0] = timeunit_mda;
    }

    template <typename ArrowType> 
    libmexclass::proxy::MakeResult make_time_type(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using namespace arrow::matlab::type;
        using TimeTypeProxy = typename proxy::Traits<ArrowType>::TypeProxy; 

        mda::StructArray opts = constructor_arguments[0];

        const mda::StringArray time_unit_mda = opts[0]["TimeUnit"];

        // extract the time unit
        const std::u16string& time_unit_utf16 = time_unit_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto timeunit,
                               timeUnitFromString(time_unit_utf16),
                               error::UNKNOWN_TIME_UNIT_ERROR_ID);

        // validate timeunit 
        MATLAB_ERROR_IF_NOT_OK(validateTimeUnit<ArrowType>(timeunit),
                               error::INVALID_TIME_UNIT);

        auto type = std::make_shared<ArrowType>(timeunit);
        auto time_type = std::static_pointer_cast<ArrowType>(type);
        return std::make_shared<TimeTypeProxy>(std::move(time_type));
    }

    // Trigger code generation for the allowed template specializations using explicit instantiation.
    template
    libmexclass::proxy::MakeResult make_time_type<arrow::Time32Type>(const libmexclass::proxy::FunctionArguments& constructor_arguments);

    template
    libmexclass::proxy::MakeResult make_time_type<arrow::Time64Type>(const libmexclass::proxy::FunctionArguments& constructor_arguments);

}
