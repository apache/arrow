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
#include "arrow/matlab/tabular/proxy/schema.h"
#include "arrow/matlab/type/proxy/field.h"

#include "libmexclass/proxy/ProxyManager.h"

#include "arrow/util/utf8.h"

namespace arrow::matlab::tabular::proxy {

    Schema::Schema(std::shared_ptr<arrow::Schema> schema) : schema{std::move(schema)} {
        REGISTER_METHOD(Schema, getFieldByIndex);
        REGISTER_METHOD(Schema, toString);
    }

    libmexclass::proxy::MakeResult Schema::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using SchemaProxy = arrow::matlab::tabular::proxy::Schema;

        mda::StructArray args = constructor_arguments[0];
        const mda::TypedArray<uint64_t> field_proxy_ids_mda = args[0]["FieldProxyIDs"];

        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto proxy_id : field_proxy_ids_mda) {
            using namespace libmexclass::proxy;
            auto proxy = std::static_pointer_cast<arrow::matlab::type::proxy::Field>(ProxyManager::getProxy(proxy_id));
            auto field = proxy->unwrap();
            fields.push_back(field);
        }
        auto schema = arrow::schema(fields);
        return std::make_shared<SchemaProxy>(std::move(schema));
    }

    std::shared_ptr<arrow::Schema> Schema::unwrap() {
        return schema;
    }

    void Schema::getFieldByIndex(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        using FieldProxy = arrow::matlab::type::proxy::Field;
        mda::ArrayFactory factory;

        const auto index = int32_t(context.inputs[0]);
        if (index > schema->num_fields()) {
            // TODO: Error if invalid index.
        }

        const auto& field = schema->field(index);
        auto field_proxy = std::make_shared<FieldProxy>(field);
        const auto field_proxy_id = ProxyManager::manageProxy(field_proxy);
        const auto field_proxy_id_mda = factory.createScalar(field_proxy_id);

        context.outputs[0] = field_proxy_id_mda;
    }

    void Schema::toString(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto str_utf8 = schema->ToString();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto str_mda = factory.createScalar(str_utf16);
        context.outputs[0] = str_mda;
    }

}
