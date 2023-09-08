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

#include "arrow/matlab/array/proxy/array.h"
#include "arrow/matlab/bit/unpack.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/type/proxy/wrap.h"
#include "arrow/type_traits.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::array::proxy {

    Array::Array(std::shared_ptr<arrow::Array> array) : array{std::move(array)} {

        // Register Proxy methods.
        REGISTER_METHOD(Array, toString);
        REGISTER_METHOD(Array, toMATLAB);
        REGISTER_METHOD(Array, getLength);
        REGISTER_METHOD(Array, getValid);
        REGISTER_METHOD(Array, getType);
        REGISTER_METHOD(Array, isEqual);

    }

    std::shared_ptr<arrow::Array> Array::unwrap() {
        return array;
    }

    void Array::toString(libmexclass::proxy::method::Context& context) {
        ::matlab::data::ArrayFactory factory;
        const auto str_utf8 = array->ToString();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto str_mda = factory.createScalar(str_utf16);
        context.outputs[0] = str_mda;
    }

    void Array::getLength(libmexclass::proxy::method::Context& context) {
        ::matlab::data::ArrayFactory factory;
        auto length_mda = factory.createScalar(array->length());
        context.outputs[0] = length_mda;
    }

    void Array::getValid(libmexclass::proxy::method::Context& context) {
        auto array_length = static_cast<size_t>(array->length());

        // If the Arrow array has no null values, then return a MATLAB
        // logical array that is all "true" for the validity bitmap.
        if (array->null_count() == 0) {
            ::matlab::data::ArrayFactory factory;
            auto validity_buffer = factory.createBuffer<bool>(array_length);
            auto validity_buffer_ptr = validity_buffer.get();
            std::fill(validity_buffer_ptr, validity_buffer_ptr + array_length, true);
            auto valid_elements_mda = factory.createArrayFromBuffer<bool>({array_length, 1}, std::move(validity_buffer));
            context.outputs[0] = valid_elements_mda;
            return;
        }

        auto validity_bitmap = array->null_bitmap();
        auto valid_elements_mda = bit::unpack(validity_bitmap, array_length);
        context.outputs[0] = valid_elements_mda;
    }

    void Array::getType(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        mda::ArrayFactory factory;

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto type_proxy,
                                            type::proxy::wrap(array->type()),
                                            context,
                                            error::ARRAY_FAILED_TO_CREATE_TYPE_PROXY);

        const auto type_id = static_cast<int32_t>(type_proxy->unwrap()->id());
        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(type_proxy);

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar(type_id);
        context.outputs[0] = output;
    }

    void Array::isEqual(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        const mda::TypedArray<uint64_t> array_proxy_ids = context.inputs[0];

        bool is_equal = true;
        const auto equals_options = arrow::EqualOptions::Defaults();
        for (const auto& array_proxy_id : array_proxy_ids) {
           // Retrieve the Array proxy from the ProxyManager
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(array_proxy_id);
            auto array_proxy = std::static_pointer_cast<proxy::Array>(proxy);
            auto array_to_compare = array_proxy->unwrap();

            if (!array->Equals(array_to_compare, equals_options)) {
                is_equal = false;
                break;
            }
        }
        mda::ArrayFactory factory;
        context.outputs[0] = factory.createScalar(is_equal);
    }
}
