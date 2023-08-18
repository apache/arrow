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

#include "arrow/matlab/type/proxy/time32_type.h"
#include "arrow/matlab/type/time_unit.h"
#include "arrow/matlab/error/error.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::type::proxy {

    Time32Type::Time32Type(std::shared_ptr<arrow::Time32Type> time32_type) : FixedWidthType(std::move(time32_type)) {
        REGISTER_METHOD(Time32Type, getTimeUnit);
    }

    libmexclass::proxy::MakeResult Time32Type::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;

        using Time32TypeProxy = arrow::matlab::type::proxy::Time32Type;

        mda::StructArray opts = constructor_arguments[0];

        const mda::StringArray timeunit_mda = opts[0]["TimeUnit"];

        // extract the time unit
        const std::u16string& utf16_timeunit = timeunit_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto timeunit,
                               arrow::matlab::type::timeUnitFromString(utf16_timeunit),
                               error::UKNOWN_TIME_UNIT_ERROR_ID);

        auto type = arrow::time32(timeunit);
        auto time_type = std::static_pointer_cast<arrow::Time32Type>(type);
        return std::make_shared<Time32TypeProxy>(std::move(time_type));
    }

    void Time32Type::getTimeUnit(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        auto time32_type = std::static_pointer_cast<arrow::Time32Type>(data_type);
        const auto timeunit = time32_type->unit();
        // Cast to uint8_t since there are only four supported TimeUnit enumeration values:
        // Nanosecond, Microsecond, Millisecond, Second
        auto timeunit_mda = factory.createScalar(static_cast<uint8_t>(timeunit));
        context.outputs[0] = timeunit_mda;
    }
}
