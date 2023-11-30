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
#include "arrow/matlab/array/proxy/wrap.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::array::proxy {

    namespace {
        libmexclass::error::Error makeEmptyChunkedArrayError() {
            const std::string error_msg =  "Numeric indexing using the chunk method is not supported for chunked arrays with zero chunks.";
            return libmexclass::error::Error{error::CHUNKED_ARRAY_NUMERIC_INDEX_WITH_EMPTY_CHUNKED_ARRAY, error_msg};
        }

        libmexclass::error::Error makeInvalidNumericIndexError(const int32_t matlab_index, const int32_t num_chunks) {
            std::stringstream error_message_stream;
            error_message_stream << "Invalid chunk index: ";
            error_message_stream << matlab_index;
            error_message_stream << ". Chunk index must be between 1 and the number of chunks (";
            error_message_stream << num_chunks;
            error_message_stream << ").";
            return libmexclass::error::Error{error::CHUNKED_ARRAY_INVALID_NUMERIC_CHUNK_INDEX, error_message_stream.str()};
        }
    }

    ChunkedArray::ChunkedArray(std::shared_ptr<arrow::ChunkedArray> chunked_array) : chunked_array{std::move(chunked_array)} {

        // Register Proxy methods.
        REGISTER_METHOD(ChunkedArray, getNumElements);
        REGISTER_METHOD(ChunkedArray, getNumChunks);
        REGISTER_METHOD(ChunkedArray, getChunk);
        REGISTER_METHOD(ChunkedArray, getType);
        REGISTER_METHOD(ChunkedArray, isEqual);
    }


    libmexclass::proxy::MakeResult ChunkedArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;

        mda::StructArray opts = constructor_arguments[0];
        const mda::TypedArray<uint64_t> array_proxy_ids = opts[0]["ArrayProxyIDs"];
        const mda::TypedArray<uint64_t> type_proxy_id = opts[0]["TypeProxyID"];

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        // Retrieve all of the Array Proxy instances from the libmexclass ProxyManager.
        for (const auto& array_proxy_id : array_proxy_ids) {
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(array_proxy_id);
            auto array_proxy = std::static_pointer_cast<proxy::Array>(proxy);
            auto array = array_proxy->unwrap();
            arrays.push_back(array);
        }

        auto proxy = libmexclass::proxy::ProxyManager::getProxy(type_proxy_id[0]);
        auto type_proxy = std::static_pointer_cast<type::proxy::Type>(proxy);
        auto type = type_proxy->unwrap();

        MATLAB_ASSIGN_OR_ERROR(auto chunked_array, 
                               arrow::ChunkedArray::Make(arrays, type),
                               error::CHUNKED_ARRAY_MAKE_FAILED);

        return std::make_unique<proxy::ChunkedArray>(std::move(chunked_array));
    }

    std::shared_ptr<arrow::ChunkedArray> ChunkedArray::unwrap() {
        return chunked_array;
    }

    void ChunkedArray::getNumElements(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        auto num_elements_mda = factory.createScalar(chunked_array->length());
        context.outputs[0] = num_elements_mda;
    }

    void ChunkedArray::getNumChunks(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        auto length_mda = factory.createScalar(chunked_array->num_chunks());
        context.outputs[0] = length_mda;
    }

    void ChunkedArray::getChunk(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        
        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int32_t> index_mda = args[0]["Index"];
        const auto matlab_index = int32_t(index_mda[0]);
        
        // Note: MATLAB uses 1-based indexing, so subtract 1.
        // arrow::Schema::field does not do any bounds checking.
        const int32_t index = matlab_index - 1;
        const auto num_chunks = chunked_array->num_chunks();
        
        if (num_chunks == 0) {
            context.error = makeEmptyChunkedArrayError();
            return;
        }
        
        if (matlab_index < 1 || matlab_index > num_chunks) {
            context.error = makeInvalidNumericIndexError(matlab_index, num_chunks);
            return;
        }

        const auto array = chunked_array->chunk(index);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto array_proxy,
                                            arrow::matlab::array::proxy::wrap(array),
                                            context,
                                            error::UNKNOWN_PROXY_FOR_ARRAY_TYPE);
        
        
        const auto array_proxy_id = libmexclass::proxy::ProxyManager::manageProxy(array_proxy);
        const auto type_id = static_cast<int64_t>(array->type_id());

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(array_proxy_id);
        output[0]["TypeID"] = factory.createScalar(type_id);
        context.outputs[0] = output;
    }


    void ChunkedArray::getType(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        mda::ArrayFactory factory;

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto type_proxy,
                                            type::proxy::wrap(chunked_array->type()),
                                            context,
                                            error::ARRAY_FAILED_TO_CREATE_TYPE_PROXY);


        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(type_proxy);
        const auto type_id = static_cast<int32_t>(type_proxy->unwrap()->id());

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar(type_id);
        context.outputs[0] = output;
    }

    void ChunkedArray::isEqual(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        const mda::TypedArray<uint64_t> chunked_array_proxy_ids = context.inputs[0];

        bool is_equal = true;
        for (const auto& chunked_array_proxy_id : chunked_array_proxy_ids) {
            // Retrieve the ChunkedArray proxy from the ProxyManager
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(chunked_array_proxy_id);
            auto chunked_array_proxy = std::static_pointer_cast<proxy::ChunkedArray>(proxy);
            auto chunked_array_to_compare = chunked_array_proxy->unwrap();

            // Use the ChunkedArray::Equals(const ChunkedArray& other) overload instead
            // of ChunkedArray::Equals(const std::shared_ptr<ChunkedArray> other&) to 
            // ensure we don't assume chunked arrays with the same memory address are
            // equal. This ensures we treat NaNs as not equal by default.
            if (!chunked_array->Equals(*chunked_array_to_compare)) {
                is_equal = false;
                break;
            }
        }
        mda::ArrayFactory factory;
        context.outputs[0] = factory.createScalar(is_equal);
    }
}
