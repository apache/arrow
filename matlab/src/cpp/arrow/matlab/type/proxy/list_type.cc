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
#include "arrow/matlab/type/proxy/wrap.h"
#include "libmexclass/proxy/ProxyManager.h"
#include "arrow/matlab/error/error.h"

namespace arrow::matlab::type::proxy {

    ListType::ListType(std::shared_ptr<arrow::ListType> list_type) : Type(std::move(list_type)) {
        REGISTER_METHOD(ListType, getType);
    }

    void ListType::getType(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto list_type = std::static_pointer_cast<arrow::ListType>(data_type);
        const auto type = list_type->value_type();
        const auto type_id = static_cast<int32_t>(type->id());

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto type_proxy,
                                    type::proxy::wrap(type),
                                    context,
                                    error::ARRAY_FAILED_TO_CREATE_TYPE_PROXY);
        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(type_proxy);

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar(type_id);

        context.outputs[0] = output;
    }

    libmexclass::proxy::MakeResult ListType::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        using ListTypeProxy = arrow::matlab::type::proxy::ListType;

        mda::StructArray args = constructor_arguments[0];
        const mda::TypedArray<uint64_t> type_proxy_id_mda = args[0]["TypeProxyID"];
        const auto proxy_id = type_proxy_id_mda[0];
        const auto proxy = ProxyManager::getProxy(proxy_id);
        const auto type_proxy = std::static_pointer_cast<type::proxy::Type>(proxy);
        const auto type = type_proxy->unwrap();
        const auto list_type = std::static_pointer_cast<arrow::ListType>(arrow::list(type));
        return std::make_shared<ListTypeProxy>(std::move(list_type));
    }
}
