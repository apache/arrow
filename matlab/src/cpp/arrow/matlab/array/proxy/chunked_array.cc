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

#include "arrow/util/utf8.h"

#include "arrow/matlab/array/proxy/chunked_array.h"
#include "arrow/matlab/array/proxy/array.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/type/proxy/wrap.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::array::proxy {

    ChunkedArray::ChunkedArray(std::shared_ptr<arrow::ChunkedArray> chunked_array) : chunked_array{std::move(chunked_array)} {

        // Register Proxy methods.
        REGISTER_METHOD(ChunkedArray, getLength);
        REGISTER_METHOD(ChunkedArray, getNumChunks);
        REGISTER_METHOD(ChunkedArray, getType);
    }


    libmexclass::proxy::MakeResult ChunkedArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;

        mda::StructArray opts = constructor_arguments[0];
        const mda::TypedArray<uint64_t> array_proxy_ids = opts[0]["ArrayProxyIDs"];

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        // Retrieve all of the Array Proxy instances from the libmexclass ProxyManager.
        for (const auto& array_proxy_id : array_proxy_ids) {
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(array_proxy_id);
            auto array_proxy = std::static_pointer_cast<proxy::Array>(proxy);
            auto array = array_proxy->unwrap();
            arrays.push_back(array);
        }

        MATLAB_ASSIGN_OR_ERROR(auto chunked_array, 
                               arrow::ChunkedArray::Make(arrays),
                               error::CHUNKED_ARRAY_MAKE_FAILED);

        return std::make_unique<proxy::ChunkedArray>(std::move(chunked_array));
    }

    std::shared_ptr<arrow::ChunkedArray> ChunkedArray::unwrap() {
        return chunked_array;
    }

    void ChunkedArray::getLength(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        auto length_mda = factory.createScalar(chunked_array->length());
        context.outputs[0] = length_mda;
    }

    void ChunkedArray::getNumChunks(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        auto length_mda = factory.createScalar(chunked_array->num_chunks());
        context.outputs[0] = length_mda;
    }

    void ChunkedArray::getType(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        mda::ArrayFactory factory;

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto type_proxy,
                                            type::proxy::wrap(chunked_array->type()),
                                            context,
                                            error::ARRAY_FAILED_TO_CREATE_TYPE_PROXY);

        auto type_id = type_proxy->unwrap()->id();
        auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(type_proxy);

        context.outputs[0] = factory.createScalar(proxy_id);
        context.outputs[1] = factory.createScalar(static_cast<int64_t>(type_id));
    }
}
