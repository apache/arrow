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

#include "arrow/matlab/type/proxy/date_type.h"

namespace arrow::matlab::type::proxy {

    DateType::DateType(std::shared_ptr<arrow::DateType> date_type) : FixedWidthType(std::move(date_type)) {
        REGISTER_METHOD(DateType, getDateUnit);
    }

    void DateType::getDateUnit(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        auto date_type = std::static_pointer_cast<arrow::DateType>(data_type);
        const auto date_unit = date_type->unit();
        // Cast to uint8_t since there are only two supported DateUnit enumeration values:
        // Day and Millisecond
        auto date_unit_mda = factory.createScalar(static_cast<uint8_t>(date_unit));
        context.outputs[0] = date_unit_mda;
    }
}
