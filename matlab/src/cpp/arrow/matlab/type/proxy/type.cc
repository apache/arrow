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


#include "arrow/matlab/error/error.h"
#include "arrow/matlab/index/validate.h"
#include "arrow/matlab/type/proxy/type.h"
#include "arrow/matlab/type/proxy/field.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::type::proxy {

    Type::Type(std::shared_ptr<arrow::DataType> type) : data_type{std::move(type)} {
        REGISTER_METHOD(Type, getTypeID);
        REGISTER_METHOD(Type, getNumFields);
        REGISTER_METHOD(Type, getFieldByIndex);
        REGISTER_METHOD(Type, isEqual);
    }

    std::shared_ptr<arrow::DataType> Type::unwrap() {
        return data_type;
    }

    void Type::getTypeID(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        
        auto type_number_mda = factory.createScalar(static_cast<int64_t>(data_type->id()));
        context.outputs[0] = type_number_mda;
    }

    void Type::getNumFields(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        
        auto num_fields_mda = factory.createScalar(data_type->num_fields());
        context.outputs[0] = num_fields_mda;
    }

    void Type::getFieldByIndex(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int32_t> index_mda = args[0]["Index"];
        const auto matlab_index = int32_t(index_mda[0]);

        // Validate there is at least 1 field
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(
            index::validateNonEmptyContainer(data_type->num_fields()),
            context,
            error::INDEX_EMPTY_CONTAINER);

        // Validate the matlab index provided is within the range [1, num_fields]
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(
            index::validateInRange(matlab_index, data_type->num_fields()),
            context,
            error::INDEX_OUT_OF_RANGE);

        // Note: MATLAB uses 1-based indexing, so subtract 1.
        // arrow::DataType::field does not do any bounds checking.
        const int32_t index = matlab_index - 1;

        auto field = data_type->field(index);
        auto field_proxy = std::make_shared<proxy::Field>(std::move(field));
        auto field_proxy_id  = libmexclass::proxy::ProxyManager::manageProxy(field_proxy);
        context.outputs[0] = factory.createScalar(field_proxy_id);
    }

    void Type::isEqual(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        const mda::TypedArray<uint64_t> type_proxy_ids = context.inputs[0];

        bool is_equal = true;
        const auto check_metadata = false;
        for (const auto& type_proxy_id : type_proxy_ids) {
            // Retrieve the Type proxy from the ProxyManager
            auto proxy = libmexclass::proxy::ProxyManager::getProxy(type_proxy_id);
            auto type_proxy = std::static_pointer_cast<proxy::Type>(proxy);
            auto type_to_compare = type_proxy->unwrap();

            if (!data_type->Equals(type_to_compare, check_metadata)) {
                is_equal = false;
                break;
            }
        }
        mda::ArrayFactory factory;
        context.outputs[0] = factory.createScalar(is_equal);
    }
}

