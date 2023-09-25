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

#include "arrow/matlab/type/proxy/field.h"
#include "arrow/matlab/error/error.h"

#include "arrow/matlab/type/proxy/primitive_ctype.h"
#include "arrow/matlab/type/proxy/timestamp_type.h"
#include "arrow/matlab/type/proxy/string_type.h"
#include "arrow/matlab/type/proxy/wrap.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::type::proxy {

    Field::Field(std::shared_ptr<arrow::Field> field) : field{std::move(field)} {
        REGISTER_METHOD(Field, getName);
        REGISTER_METHOD(Field, getType);
    }

    std::shared_ptr<arrow::Field> Field::unwrap() {
        return field;
    }

    void Field::getName(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto& str_utf8 = field->name();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto str_mda = factory.createScalar(str_utf16);
        context.outputs[0] = str_mda;
    }

    void Field::getType(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        const auto& datatype = field->type();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto proxy, type::proxy::wrap(datatype), context, error::FIELD_FAILED_TO_CREATE_TYPE_PROXY); 
        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(proxy);
        const auto type_id = static_cast<int32_t>(datatype->id());

        mda::ArrayFactory factory;
        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar(type_id);
        context.outputs[0] = output;
    }

    libmexclass::proxy::MakeResult Field::make(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
        namespace mda = ::matlab::data;
        using FieldProxy = arrow::matlab::type::proxy::Field;

        mda::StructArray opts = constructor_arguments[0];
        const mda::StringArray name_mda = opts[0]["Name"];
        const mda::TypedArray<uint64_t> type_proxy_id_mda = opts[0]["TypeProxyID"];

        const std::u16string& name_utf16 = name_mda[0];
        MATLAB_ASSIGN_OR_ERROR(const auto name,
                arrow::util::UTF16StringToUTF8(name_utf16),
                error::UNICODE_CONVERSION_ERROR_ID);

        auto proxy = std::static_pointer_cast<type::proxy::Type>(libmexclass::proxy::ProxyManager::getProxy(type_proxy_id_mda[0]));
        auto type = proxy->unwrap();
        auto field = arrow::field(name, type);
        return std::make_shared<FieldProxy>(std::move(field));
    }

}

