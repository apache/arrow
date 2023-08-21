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

namespace arrow::matlab::type::proxy {

    TimeType::TimeType(std::shared_ptr<arrow::TimeType> time_type) : FixedWidthType(std::move(time_type)) {
        REGISTER_METHOD(TimeType, getTimeUnit);
    }

    void TimeType::getTimeUnit(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        auto time_type = std::static_pointer_cast<arrow::TimeType>(data_type);
        const auto timeunit = time_type->unit();
        // Cast to uint8_t since there are only four supported TimeUnit enumeration values:
        // Nanosecond, Microsecond, Millisecond, Second
        auto timeunit_mda = factory.createScalar(static_cast<uint8_t>(timeunit));
        context.outputs[0] = timeunit_mda;
    }
}
