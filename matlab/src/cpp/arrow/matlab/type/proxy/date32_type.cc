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

#include "arrow/matlab/type/proxy/date32_type.h"

namespace arrow::matlab::type::proxy {

    Date32Type::Date32Type(std::shared_ptr<arrow::Date32Type> date32_type) : DateType(std::move(date32_type)) {}

    libmexclass::proxy::MakeResult Date32Type::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        using Date32TypeProxy = arrow::matlab::type::proxy::Date32Type;

        const auto type = arrow::date32();
        const auto date32_type = std::static_pointer_cast<arrow::Date32Type>(type);
        return std::make_shared<Date32TypeProxy>(std::move(date32_type));
    }
}
