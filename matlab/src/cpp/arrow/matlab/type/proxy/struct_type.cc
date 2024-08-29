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

#include "arrow/matlab/type/proxy/struct_type.h"
#include "arrow/matlab/type/proxy/field.h"
#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::type::proxy {

    StructType::StructType(std::shared_ptr<arrow::StructType> struct_type) : Type(std::move(struct_type)) {}

    libmexclass::proxy::MakeResult StructType::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using StructTypeProxy = arrow::matlab::type::proxy::StructType;

        mda::StructArray args = constructor_arguments[0];
        const mda::TypedArray<uint64_t> field_proxy_ids_mda = args[0]["FieldProxyIDs"];

        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(field_proxy_ids_mda.getNumberOfElements());
        for (const auto proxy_id : field_proxy_ids_mda) {
            using namespace libmexclass::proxy;
            auto proxy = std::static_pointer_cast<proxy::Field>(ProxyManager::getProxy(proxy_id));
            auto field = proxy->unwrap();
            fields.push_back(field);
        }

        auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow::struct_(fields));
        return std::make_shared<StructTypeProxy>(std::move(struct_type));
    }
}