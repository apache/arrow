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
        struct ProxyTraits {};

        #define MATLAB_PROXY_TRAITS_DEF(ArrowType_, ArrayProxyName_, TypeProxyName_) \
            template <>                                                              \
            struct ProxyTraits<ArrowType_> {                                         \
                using ArrayProxy = arrow::matlab::array::proxy::ArrayProxyName_;     \
                using TypeProxy = arrow::matlab::type::proxy::TypeProxyName_;        \
            };
        
        MATLAB_PROXY_TRAITS_DEF(arrow::BooleanType, BooleanArray, PrimitiveCType<bool>)
        MATLAB_PROXY_TRAITS_DEF(arrow::Int8Type, NumericArray<arrow::Int8Type>, PrimitiveCType<int8_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::Int16Type, NumericArray<arrow::Int16Type>, PrimitiveCType<int16_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::Int32Type, NumericArray<arrow::Int32Type>, PrimitiveCType<int32_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::Int64Type, NumericArray<arrow::Int64Type>, PrimitiveCType<int64_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::UInt8Type, NumericArray<arrow::UInt8Type>, PrimitiveCType<uint8_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::UInt16Type, NumericArray<arrow::UInt16Type>, PrimitiveCType<uint16_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::UInt32Type, NumericArray<arrow::UInt32Type>, PrimitiveCType<uint32_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::UInt64Type, NumericArray<arrow::UInt64Type>, PrimitiveCType<uint64_t>)
        MATLAB_PROXY_TRAITS_DEF(arrow::FloatType, NumericArray<arrow::FloatType>, PrimitiveCType<float>)
        MATLAB_PROXY_TRAITS_DEF(arrow::DoubleType, NumericArray<arrow::DoubleType>, PrimitiveCType<double>)
        MATLAB_PROXY_TRAITS_DEF(arrow::Time32Type, NumericArray<arrow::Time32Type>, Time32Type)
        MATLAB_PROXY_TRAITS_DEF(arrow::Time64Type, NumericArray<arrow::Time64Type>, Time64Type)
        MATLAB_PROXY_TRAITS_DEF(arrow::Date32Type, NumericArray<arrow::Date32Type>, Date32Type)
        MATLAB_PROXY_TRAITS_DEF(arrow::Date64Type, NumericArray<arrow::Date64Type>, Date64Type)
        MATLAB_PROXY_TRAITS_DEF(arrow::TimestampType, NumericArray<arrow::TimestampType>, TimestampType)
        MATLAB_PROXY_TRAITS_DEF(arrow::StringType, StringArray, StringType)
        MATLAB_PROXY_TRAITS_DEF(arrow::ListType, ListArray, ListType)
        MATLAB_PROXY_TRAITS_DEF(arrow::StructType, StructArray, StructType)

        #undef MATLAB_PROXY_TRAITS_DEF

        template <typename ArrowType> 
        std::shared_ptr<typename ProxyTraits<ArrowType>::ArrayProxy> make_proxy(const std::shared_ptr<arrow::Array>& array) {
            using ArrowArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
            using ArrayProxy = typename ProxyTraits<ArrowType>::ArrayProxy;
            return std::make_shared<ArrayProxy>(std::static_pointer_cast<ArrowArrayType>(array));
        }

        template <typename ArrowType> 
        std::shared_ptr<typename ProxyTraits<ArrowType>::TypeProxy> make_proxy(const std::shared_ptr<arrow::DataType>& datatype) {
            using TypeProxy = typename ProxyTraits<ArrowType>::TypeProxy;
            return std::make_shared<TypeProxy>(std::static_pointer_cast<ArrowType>(datatype));
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
        arrow::Result<std::shared_ptr<typename Function::OutputType>> wrap(const std::shared_ptr<typename Function::InputType>& input, const Function& func) {
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
        return wrap(array, functor);
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
        return wrap(datatype, functor);
    }

    arrow::Result<::matlab::data::StructArray> wrap_and_manage(const std::shared_ptr<arrow::DataType>& datatype) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        ARROW_ASSIGN_OR_RAISE(auto proxy, wrap(datatype));
        const auto proxy_id = libmexclass::proxy::ProxyManager::manageProxy(proxy);

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar( static_cast<int32_t>(datatype->id()));
        return output;
    }
}
