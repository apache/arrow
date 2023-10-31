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

#include "arrow/matlab/array/validation_mode.h"
#include "arrow/matlab/array/proxy/list_array.h"
#include "arrow/matlab/array/proxy/numeric_array.h"
#include "arrow/matlab/array/proxy/wrap.h"
#include "arrow/matlab/error/error.h"
#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::array::proxy {

    ListArray::ListArray(std::shared_ptr<arrow::ListArray> list_array) : proxy::Array{std::move(list_array)} {
        REGISTER_METHOD(ListArray, getValues);
        REGISTER_METHOD(ListArray, getOffsets);
        REGISTER_METHOD(ListArray, validate);
    }

    libmexclass::proxy::MakeResult ListArray::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using libmexclass::proxy::ProxyManager;
        using Int32ArrayProxy = arrow::matlab::array::proxy::NumericArray<arrow::Int32Type>;
        using ListArrayProxy = arrow::matlab::array::proxy::ListArray;
        using ArrayProxy = arrow::matlab::array::proxy::Array;

        mda::StructArray opts = constructor_arguments[0];
        const mda::TypedArray<uint64_t> offsets_proxy_id_mda = opts[0]["OffsetsProxyID"];
        const mda::TypedArray<uint64_t> values_proxy_id_mda = opts[0]["ValuesProxyID"];
        const mda::TypedArray<bool> validity_bitmap_mda = opts[0]["Valid"];

        const auto offsets_proxy_id = offsets_proxy_id_mda[0];
        const auto values_proxy_id = values_proxy_id_mda[0];

        const auto offsets_proxy = std::static_pointer_cast<Int32ArrayProxy>(ProxyManager::getProxy(offsets_proxy_id));
        const auto values_proxy = std::static_pointer_cast<ArrayProxy>(ProxyManager::getProxy(values_proxy_id));

        const auto offsets = offsets_proxy->unwrap();
        const auto values = values_proxy->unwrap();

        // Pack the validity bitmap values.
        MATLAB_ASSIGN_OR_ERROR(auto validity_bitmap_buffer,
                               bit::packValid(validity_bitmap_mda),
                               error::BITPACK_VALIDITY_BITMAP_ERROR_ID);

        // Create a ListArray from values and offsets.
        MATLAB_ASSIGN_OR_ERROR(auto array,
                               arrow::ListArray::FromArrays(*offsets, *values, arrow::default_memory_pool(), validity_bitmap_buffer),
                               error::LIST_ARRAY_FROM_ARRAYS_FAILED);

        // Return a ListArray Proxy.
        auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
        return std::make_shared<ListArrayProxy>(std::move(list_array));
    }

    void ListArray::getValues(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using libmexclass::proxy::ProxyManager;

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
        output[0]["ProxyID"] = factory.createScalar(value_array_proxy_id);
        output[0]["TypeID"] = factory.createScalar(static_cast<int32_t>(type_id));
        context.outputs[0] = output;
    }

    void ListArray::getOffsets(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using libmexclass::proxy::ProxyManager;
        using Int32ArrayProxy = arrow::matlab::array::proxy::NumericArray<arrow::Int32Type>;
        auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
        auto offsets_array = list_array->offsets();
        auto offsets_int32_array = std::static_pointer_cast<arrow::Int32Array>(offsets_array);
        auto offsets_int32_array_proxy = std::make_shared<Int32ArrayProxy>(offsets_int32_array);
        const auto offsets_int32_array_proxy_id = ProxyManager::manageProxy(offsets_int32_array_proxy);
        mda::ArrayFactory factory;
        context.outputs[0] = factory.createScalar(offsets_int32_array_proxy_id);
    }

    void ListArray::validate(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<std::uint8_t> validation_mode_mda = args[0]["ValidationMode"];
        const auto validation_mode_integer = uint8_t(validation_mode_mda[0]);
        // Convert integer representation to ValidationMode enum.
        const auto validation_mode = static_cast<ValidationMode>(validation_mode_integer);
        switch (validation_mode) {
            case ValidationMode::None: {
                // Do nothing.
                break;
            }
            case ValidationMode::Minimal: {
                MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(array->Validate(),
                        context,
                        error::ARRAY_VALIDATE_MINIMAL_FAILED);
                break;
            }
            case ValidationMode::Full: {
                MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(array->ValidateFull(),
                        context,
                        error::ARRAY_VALIDATE_FULL_FAILED);
                break;
            }
            default: {
                // Throw an error if an unsupported enumeration value is provided.
                const auto msg = "Unsupported ValidationMode enumeration value: " + std::to_string(validation_mode_integer);
                context.error = libmexclass::error::Error{error::ARRAY_VALIDATE_UNSUPPORTED_ENUM, msg};
                return;
            }
        }
    }

}
