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


#include "arrow/matlab/array/proxy/list_array.h"
#include "arrow/matlab/array/proxy/wrap.h"
#include "arrow/matlab/error/error.h"
#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::array::proxy {

    ListArray::ListArray(std::shared_ptr<arrow::ListArray> array) : array{std::move(array)} {
        REGISTER_METHOD(ListArray, getValues);
        REGISTER_METHOD(ListArray, getOffsets);
    }


    void ListArray::getValues(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
        auto value_array = list_array->values();

        // Wrap the array within a proxy object if possible.
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto value_array_proxy,
                                            proxy::wrap(value_array),
                                            context, error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);
        const auto value_array_proxy_id = ProxyManager::manageProxy(value_array_proxy);
        const auto type_id = value_array->type_id();

        // Return a struct with two fields: ProxyID and TypeID. The MATLAB
        // layer will use these values to construct the appropriate MATLAB
        // arrow.array.Array subclass.
        mda::ArrayFactory factory;
        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(field_array_proxy_id);
        output[0]["TypeID"] = factory.createScalar(static_cast<int32_t>(type_id));
        context.outputs[0] = output;
    }

    void ListArray::getOffsets(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        // STUB METHOD
    }
}
