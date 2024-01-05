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
#include "arrow/matlab/array/proxy/wrap.h"
#include "arrow/matlab/bit/unpack.h"
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/index/validate.h"
#include "arrow/matlab/type/proxy/wrap.h"
#include "arrow/pretty_print.h"
#include "arrow/type_traits.h"

#include "libmexclass/proxy/ProxyManager.h"

#include <sstream>

namespace arrow::matlab::array::proxy {

    Array::Array(std::shared_ptr<arrow::Array> array) : array{std::move(array)} {

        // Register Proxy methods.
        REGISTER_METHOD(Array, toString);
        REGISTER_METHOD(Array, getNumElements);
        REGISTER_METHOD(Array, getValid);
        REGISTER_METHOD(Array, getType);
        REGISTER_METHOD(Array, isEqual);
        REGISTER_METHOD(Array, slice);
    }

    std::shared_ptr<arrow::Array> Array::unwrap() {
        return array;
    }

    void Array::toString(libmexclass::proxy::method::Context& context) {
        ::matlab::data::ArrayFactory factory;
        
        auto opts = arrow::PrettyPrintOptions::Defaults();
        opts.window = 3;
        opts.indent = 4;
        opts.indent_size = 4;

        const auto type_id = array->type()->id();
        if (arrow::is_primitive(type_id) || arrow::is_string(type_id)) {
            /* 
             * Display primitive and string types horizontally without 
             * opening and closing delimiters. Use " | " as the delimiter
             * between elements. Below is an example Int32Array display:
             *
             *    1 | 2 | 3 | ... | 6 | 7 | 8
             */
            opts.skip_new_lines = true;
            opts.array_delimiters.open = "";
            opts.array_delimiters.close = "";
            opts.array_delimiters.element = " | ";
        }

        std::stringstream ss;
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(arrow::PrettyPrint(*array, opts, &ss), context, error::ARRAY_PRETTY_PRINT_FAILED);
        
        const auto str_utf8 = opts.skip_new_lines ? "    " + ss.str() : ss.str();

        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto str_mda = factory.createScalar(str_utf16);
        context.outputs[0] = str_mda;
    }

    void Array::getNumElements(libmexclass::proxy::method::Context& context) {
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
        auto valid_elements_mda = bit::unpack(validity_bitmap, array_length, array->offset());
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

    void Array::slice(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        mda::StructArray opts = context.inputs[0];
        const mda::TypedArray<int64_t> offset_mda = opts[0]["Offset"];
        const mda::TypedArray<int64_t> length_mda = opts[0]["Length"];

        const auto matlab_offset = int64_t(offset_mda[0]);        
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(arrow::matlab::index::validateSliceOffset(matlab_offset),
                                            context, error::ARRAY_SLICE_NON_POSITIVE_OFFSET);

        // Note: MATLAB uses 1-based indexing, so subtract 1.
        const int64_t offset = matlab_offset - 1;
        const int64_t length = int64_t(length_mda[0]);
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(arrow::matlab::index::validateSliceLength(length),
                                            context, error::ARRAY_SLICE_NEGATIVE_LENGTH);

        auto sliced_array = array->Slice(offset, length);
        const auto type_id = static_cast<int32_t>(sliced_array->type_id());
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto sliced_array_proxy,
                                            array::proxy::wrap(sliced_array),
                                            context, error::ARRAY_SLICE_FAILED_TO_CREATE_ARRAY_PROXY);

        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(sliced_array_proxy);

        mda::ArrayFactory factory;
        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar(type_id);
        context.outputs[0] = output;
    }
}
