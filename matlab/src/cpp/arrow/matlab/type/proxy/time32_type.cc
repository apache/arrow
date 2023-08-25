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

namespace arrow::matlab::type::proxy {

    Time32Type::Time32Type(std::shared_ptr<arrow::Time32Type> time32_type) : TimeType(std::move(time32_type)) {}

    libmexclass::proxy::MakeResult Time32Type::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        return make_time_type<arrow::Time32Type>(constructor_arguments);
    }
}
