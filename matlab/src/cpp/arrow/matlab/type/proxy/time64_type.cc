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

namespace arrow::matlab::type::proxy {

    Time64Type::Time64Type(std::shared_ptr<arrow::Time64Type> time64_type) : TimeType(std::move(time64_type)) {}

    libmexclass::proxy::MakeResult Time64Type::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        return make_time_type<arrow::Time64Type>(constructor_arguments);
    }
}
