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

#pragma once

#include "arrow/matlab/type/proxy/fixed_width_type.h"
#include "arrow/type_traits.h"

namespace arrow::matlab::type::proxy {

class TimestampType : public arrow::matlab::type::proxy::FixedWidthType {
        
    public:
        TimestampType(std::shared_ptr<arrow::TimestampType> timestamp_type);

        ~TimestampType() {}

        static libmexclass::proxy::MakeResult make(const libmexclass::proxy::FunctionArguments& constructor_arguments);

    protected:

        void getTimeZone(libmexclass::proxy::method::Context& context);

        void getTimeUnit(libmexclass::proxy::method::Context& context);
};

}

