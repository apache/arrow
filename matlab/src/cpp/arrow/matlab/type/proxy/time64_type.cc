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

#include "arrow/matlab/type/proxy/time64_type.h"
#include "arrow/matlab/type/time_unit.h"
#include "arrow/matlab/error/error.h"
#include "arrow/util/utf8.h"

namespace arrow::matlab::type::proxy {

    Time64Type::Time64Type(std::shared_ptr<arrow::Time64Type> time64_type) : TimeType(std::move(time64_type)) {}

    libmexclass::proxy::MakeResult Time64Type::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;

        using Time64TypeProxy = arrow::matlab::type::proxy::Time64Type;

        mda::StructArray opts = constructor_arguments[0];

        const mda::StringArray timeunit_mda = opts[0]["TimeUnit"];

        // extract the time unit
        const std::u16string& utf16_timeunit = timeunit_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto timeunit,
                               arrow::matlab::type::timeUnitFromString(utf16_timeunit),
                               error::UKNOWN_TIME_UNIT_ERROR_ID);

        auto type = arrow::time64(timeunit);
        auto time_type = std::static_pointer_cast<arrow::Time64Type>(type);
        return std::make_shared<Time64TypeProxy>(std::move(time_type));
    }
}
