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

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::type::proxy {

    Field::Field(std::shared_ptr<arrow::Field> field) : field{std::move(field)} {
        REGISTER_METHOD(Field, name);
        REGISTER_METHOD(Field, type);
        REGISTER_METHOD(Field, toString);
    }

    std::shared_ptr<arrow::Field> Field::unwrap() {
        return field;
    }

    void Field::name(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto& str_utf8 = field->name();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto str_mda = factory.createScalar(str_utf16);
        context.outputs[0] = str_mda;
    }

    arrow::Result<std::shared_ptr<libmexclass::proxy::Proxy>> makeTypeProxy(const std::shared_ptr<arrow::DataType>& datatype) {
        using arrow_type = arrow::Type::type;
        namespace type_proxy = arrow::matlab::type::proxy;
        switch (datatype->id()) {
            case arrow_type::UINT8:
                return std::make_shared<type_proxy::PrimitiveCType<uint8_t>>(std::static_pointer_cast<arrow::UInt8Type>(datatype));
            case arrow_type::UINT16:
                return std::make_shared<type_proxy::PrimitiveCType<uint16_t>>(std::static_pointer_cast<arrow::UInt16Type>(datatype));
            case arrow_type::UINT32:
                return std::make_shared<type_proxy::PrimitiveCType<uint32_t>>(std::static_pointer_cast<arrow::UInt32Type>(datatype));
            case arrow_type::UINT64:
                return std::make_shared<type_proxy::PrimitiveCType<uint64_t>>(std::static_pointer_cast<arrow::UInt64Type>(datatype));
            case arrow_type::INT8:
                return std::make_shared<type_proxy::PrimitiveCType<int8_t>>(std::static_pointer_cast<arrow::Int8Type>(datatype));
            case arrow_type::INT16:
                return std::make_shared<type_proxy::PrimitiveCType<int16_t>>(std::static_pointer_cast<arrow::Int16Type>(datatype));
            case arrow_type::INT32:
                return std::make_shared<type_proxy::PrimitiveCType<int32_t>>(std::static_pointer_cast<arrow::Int32Type>(datatype));
            case arrow_type::INT64:
                return std::make_shared<type_proxy::PrimitiveCType<int64_t>>(std::static_pointer_cast<arrow::Int64Type>(datatype));
            case arrow_type::FLOAT:
                return std::make_shared<type_proxy::PrimitiveCType<float>>(std::static_pointer_cast<arrow::FloatType>(datatype));
            case arrow_type::DOUBLE:
                return std::make_shared<type_proxy::PrimitiveCType<double>>(std::static_pointer_cast<arrow::DoubleType>(datatype));
            case arrow_type::BOOL:
                return std::make_shared<type_proxy::PrimitiveCType<bool>>(std::static_pointer_cast<arrow::BooleanType>(datatype));
            case arrow_type::STRING:
                return std::make_shared<type_proxy::StringType>(std::static_pointer_cast<arrow::StringType>(datatype));
            case arrow_type::TIMESTAMP:
                return std::make_shared<type_proxy::TimestampType>(std::static_pointer_cast<arrow::TimestampType>(datatype));
            default:
                return arrow::Status::NotImplemented("Unsupported DataType: " + datatype->ToString());
        }
    }


    void Field::type(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;

        auto datatype = field->type();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto proxy, makeTypeProxy(datatype), context, "arrow:field:FailedToCreateTypeProxy");
        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(proxy);

        mda::ArrayFactory factory;
        context.outputs[0] = factory.createScalar(proxy_id);
        context.outputs[1] = factory.createScalar(static_cast<uint64_t>(datatype->id()));
    }

    void Field::toString(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        const auto str_utf8 = field->ToString();
        MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(const auto str_utf16, arrow::util::UTF8StringToUTF16(str_utf8), context, error::UNICODE_CONVERSION_ERROR_ID);
        auto str_mda = factory.createScalar(str_utf16);
        context.outputs[0] = str_mda;
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

