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

#include "arrow/matlab/array/proxy/boolean_array.h"
#include "arrow/matlab/array/proxy/list_array.h"
#include "arrow/matlab/array/proxy/numeric_array.h"
#include "arrow/matlab/array/proxy/string_array.h"
#include "arrow/matlab/array/proxy/struct_array.h"
#include "arrow/matlab/proxy/wrap.h"
#include "arrow/matlab/type/proxy/primitive_ctype.h"
#include "arrow/matlab/type/proxy/string_type.h"
#include "arrow/matlab/type/proxy/date32_type.h"
#include "arrow/matlab/type/proxy/date64_type.h"
#include "arrow/matlab/type/proxy/time32_type.h"
#include "arrow/matlab/type/proxy/time64_type.h"
#include "arrow/matlab/type/proxy/string_type.h"
#include "arrow/matlab/type/proxy/list_type.h"
#include "arrow/matlab/type/proxy/struct_type.h"

#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::proxy {

    namespace {
        template <typename T>
        struct TypeProxyTraits {};

        template <>
        struct TypeProxyTraits<arrow::BooleanType> {
            using ArrayProxyType = arrow::matlab::array::proxy::BooleanArray;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<bool>;
        };

        template <>
        struct TypeProxyTraits<arrow::Int8Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Int8Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<int8_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::Int16Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Int16Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<int16_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::Int32Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Int32Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<int32_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::Int64Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Int64Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<int64_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::UInt8Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::UInt8Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<uint8_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::UInt16Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::UInt16Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<uint16_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::UInt32Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::UInt32Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<uint32_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::UInt64Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::UInt64Type>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<uint64_t>;
        };

        template <>
        struct TypeProxyTraits<arrow::FloatType> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::FloatType>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<float>;
        };

        template <>
        struct TypeProxyTraits<arrow::DoubleType> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::DoubleType>;
            using TypeProxyType = arrow::matlab::type::proxy::PrimitiveCType<double>;
        };

        template <>
        struct TypeProxyTraits<arrow::Time32Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Time32Type>;
            using TypeProxyType = arrow::matlab::type::proxy::Time32Type;
        };

        template <>
        struct TypeProxyTraits<arrow::Time64Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Time64Type>;
            using TypeProxyType = arrow::matlab::type::proxy::Time64Type;
        };

        template <>
        struct TypeProxyTraits<arrow::Date32Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Date32Type>;
            using TypeProxyType = arrow::matlab::type::proxy::Date32Type;
        };

        template <>
        struct TypeProxyTraits<arrow::Date64Type> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::Date64Type>;
            using TypeProxyType = arrow::matlab::type::proxy::Date64Type;
        };

        template <>
        struct TypeProxyTraits<arrow::TimestampType> {
            using ArrayProxyType = arrow::matlab::array::proxy::NumericArray<arrow::TimestampType>;
            using TypeProxyType = arrow::matlab::type::proxy::TimestampType;
        };

        template <>
        struct TypeProxyTraits<arrow::StringType> {
            using ArrayProxyType = arrow::matlab::array::proxy::StringArray;
            using TypeProxyType = arrow::matlab::type::proxy::StringType;
        };

        template <>
        struct TypeProxyTraits<arrow::ListType> {
            using ArrayProxyType = arrow::matlab::array::proxy::ListArray;
            using TypeProxyType = arrow::matlab::type::proxy::ListType;
        };

        template <>
        struct TypeProxyTraits<arrow::StructType> {
            using ArrayProxyType = arrow::matlab::array::proxy::StructArray;
            using TypeProxyType = arrow::matlab::type::proxy::StructType;
        };

        template <typename ArrowType> 
        std::shared_ptr<typename TypeProxyTraits<ArrowType>::ArrayProxyType> make_proxy(const std::shared_ptr<arrow::Array>& array) {
            using ArrowArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
            using ArrayProxyType = typename TypeProxyTraits<ArrowType>::ArrayProxyType;
            return std::make_shared<ArrayProxyType>(std::static_pointer_cast<ArrowArrayType>(array));
        }

        template <typename ArrowType> 
        std::shared_ptr<typename TypeProxyTraits<ArrowType>::TypeProxyType> make_proxy(const std::shared_ptr<arrow::DataType>& datatype) {
            using TypeProxyType = typename TypeProxyTraits<ArrowType>::TypeProxyType;
            return std::make_shared<TypeProxyType>(std::static_pointer_cast<ArrowType>(datatype));
        }

        arrow::Type::type get_type_id(const std::shared_ptr<arrow::Array>& array) {
            return array->type_id();
        }

        arrow::Type::type get_type_id(const std::shared_ptr<arrow::DataType>& datatype) {
            return datatype->id();
        }

        std::string get_type_string(const std::shared_ptr<arrow::Array>& array) {
            return array->type()->ToString();
        }

        std::string get_type_string(const std::shared_ptr<arrow::DataType>& datatype) {
            return datatype->ToString();
        }

        struct WrapArrayFunctor {
            using InputType = arrow::Array;
            using OutputType = arrow::matlab::array::proxy::Array;

            template <typename ArrowType>
            std::shared_ptr<OutputType> wrap(const std::shared_ptr<InputType>& input) const {
                return make_proxy<ArrowType>(input);
            }
        };


        struct WrapTypeFunctor {
            using InputType = arrow::DataType;
            using OutputType = arrow::matlab::type::proxy::Type;

            template <typename ArrowType>
            std::shared_ptr<OutputType> wrap(const std::shared_ptr<InputType>& input) const {
                return make_proxy<ArrowType>(input);
            }

        };

        template <typename Function>
        arrow::Result<std::shared_ptr<typename Function::OutputType>> do_wrap(const std::shared_ptr<typename Function::InputType>& input, const Function& func) {
            using ID = arrow::Type::type;
            switch (get_type_id(input)) {
                case ID::BOOL:
                    return func.template wrap<arrow::BooleanType>(input);
                case ID::UINT8:
                    return func.template wrap<arrow::UInt8Type>(input);
                case ID::UINT16:
                    return func.template wrap<arrow::UInt16Type>(input);
                case ID::UINT32:
                    return func.template wrap<arrow::UInt32Type>(input);
                case ID::UINT64:
                    return func.template wrap<arrow::UInt64Type>(input);
                case ID::INT8:
                    return func.template wrap<arrow::Int8Type>(input);
                case ID::INT16:
                    return func.template wrap<arrow::Int16Type>(input);
                case ID::INT32:
                    return func.template wrap<arrow::Int32Type>(input);
                case ID::INT64:
                    return func.template wrap<arrow::Int64Type>(input);
                case ID::FLOAT:
                    return func.template wrap<arrow::FloatType>(input);
                case ID::DOUBLE:
                    return func.template wrap<arrow::DoubleType>(input);
                case ID::TIMESTAMP:
                    return func.template wrap<arrow::TimestampType>(input);
                case ID::TIME32:
                    return func.template wrap<arrow::Time32Type>(input);
                case ID::TIME64:
                    return func.template wrap<arrow::Time64Type>(input);
                case ID::DATE32:
                    return func.template wrap<arrow::Date32Type>(input);
                case ID::DATE64:
                    return func.template wrap<arrow::Date64Type>(input);
                case ID::STRING:
                    return func.template wrap<arrow::StringType>(input);
                case ID::LIST:
                    return func.template wrap<arrow::ListType>(input);
                case ID::STRUCT:
                    return func.template wrap<arrow::StructType>(input);
                default:
                return arrow::Status::NotImplemented("Unsupported DataType: " + get_type_string(input));                                      
            }
        }

    } // anonymous namespace

    arrow::Result<std::shared_ptr<arrow::matlab::array::proxy::Array>> wrap(const std::shared_ptr<arrow::Array>& array) {
        WrapArrayFunctor functor;
        return do_wrap(array, functor);
    }

    arrow::Result<::matlab::data::StructArray> wrap_and_manage(const std::shared_ptr<arrow::Array>& array) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        ARROW_ASSIGN_OR_RAISE(auto proxy, wrap(array));
        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(proxy);

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar( static_cast<int32_t>(array->type_id()));
        return output;
    }

    arrow::Result<std::shared_ptr<arrow::matlab::type::proxy::Type>> wrap(const std::shared_ptr<arrow::DataType>& datatype) {
        WrapTypeFunctor functor;
        return do_wrap(datatype, functor);
    }
}
