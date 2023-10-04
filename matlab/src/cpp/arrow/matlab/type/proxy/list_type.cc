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

#include "arrow/matlab/type/proxy/list_type.h"
#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::type::proxy {

    ListType::ListType(std::shared_ptr<arrow::ListType> list_type) : Type(std::move(list_type)) {}

    libmexclass::proxy::MakeResult ListType::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using ListTypeProxy = arrow::matlab::type::proxy::ListType;

        mda::StructArray args = constructor_arguments[0];
        const mda::TypedArray<uint64_t> type_proxy_id_mda = args[0]["TypeProxyID"];
        const auto proxy_id = type_proxy_id_mda[0];
        const auto proxy = ProxyManager::getProxy(proxy_id);
        const auto type_proxy = std::static_pointer_cast<type::proxy:Type>(proxy);
        const auto type = type_proxy->unwrap();
        const auto list_type = arrow::list(type);
        return std::make_shared<ListTypeProxy>(std::move(list_type));
    }
}
