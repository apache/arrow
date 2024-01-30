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

#include "arrow/matlab/type/proxy/string_type.h"

namespace arrow::matlab::type::proxy {

    StringType::StringType(std::shared_ptr<arrow::StringType> string_type) : Type(std::move(string_type)) {}

    libmexclass::proxy::MakeResult StringType::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        auto string_type = std::static_pointer_cast<arrow::StringType>(arrow::utf8());
        return std::make_shared<StringType>(std::move(string_type));
    }
}
