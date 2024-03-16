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
#include "arrow/matlab/index/validate.h"

#include "libmexclass/proxy/ProxyManager.h"
#include "libmexclass/error/Error.h"

#include "arrow/util/utf8.h"

#include <sstream>

namespace arrow::matlab::tabular::proxy {

    Schema::Schema(std::shared_ptr<arrow::Schema> schema) : schema{std::move(schema)} {
        REGISTER_METHOD(Schema, getFieldByIndex);
        REGISTER_METHOD(Schema, getFieldByName);
        REGISTER_METHOD(Schema, getNumFields);
        REGISTER_METHOD(Schema, getFieldNames);
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

        mda::StructArray args = context.inputs[0];
        const mda::TypedArray<int32_t> index_mda = args[0]["Index"];
        const auto matlab_index = int32_t(index_mda[0]);

        // Validate there is at least 1 field
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(
            index::validateNonEmptyContainer(schema->num_fields()),
            context,
            error::INDEX_EMPTY_CONTAINER);

        // Validate the matlab index provided is within the range [1, num_fields]
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(
            index::validateInRange(matlab_index, schema->num_fields()),
            context,
            error::INDEX_OUT_OF_RANGE);

        // Note: MATLAB uses 1-based indexing, so subtract 1.
        // arrow::Schema::field does not do any bounds checking.
        const int32_t index = matlab_index - 1;

        auto field = schema->field(index);
        auto field_proxy = std::make_shared<FieldProxy>(std::move(field));
        auto field_proxy_id  = ProxyManager::manageProxy(field_proxy);
        context.outputs[0] = factory.createScalar(field_proxy_id);
    }

    void Schema::getFieldByName(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        using namespace libmexclass::proxy;
        using FieldProxy = arrow::matlab::type::proxy::Field;
        mda::ArrayFactory factory;

        mda::StructArray args = context.inputs[0];
        const mda::StringArray name_mda = args[0]["Name"];
        const auto name_utf16 = std::u16string(name_mda[0]);
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto name, arrow::util::UTF16StringToUTF8(name_utf16), context, error::UNICODE_CONVERSION_ERROR_ID);
        const std::vector<std::string> names = {name};
        MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(schema->CanReferenceFieldsByNames(names), context, error::ARROW_TABULAR_SCHEMA_AMBIGUOUS_FIELD_NAME);

        const auto field = schema->GetFieldByName(name);
        auto field_proxy = std::make_shared<FieldProxy>(field);
        const auto field_proxy_id = ProxyManager::manageProxy(field_proxy);
        context.outputs[0] = factory.createScalar(field_proxy_id);
    }

    void Schema::getNumFields(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto num_fields = schema->num_fields();
        const auto num_fields_mda = factory.createScalar(num_fields);

        context.outputs[0] = num_fields_mda;
    }

    void Schema::getFieldNames(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto field_names_utf8 = schema->field_names();
        const auto num_fields = static_cast<size_t>(schema->num_fields());

        std::vector<std::u16string> field_names_utf16;
        field_names_utf16.reserve(num_fields);

        // Convert the field names from UTF-8 to UTF-16.
        for (const auto& field_name_utf8 : field_names_utf8) {
            MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto field_name_utf16, arrow::util::UTF8StringToUTF16(field_name_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
            field_names_utf16.push_back(field_name_utf16);
        }

        const auto field_names_mda = factory.createArray({1, num_fields}, field_names_utf16.cbegin(), field_names_utf16.cend());

        context.outputs[0] = field_names_mda;
    }

}
