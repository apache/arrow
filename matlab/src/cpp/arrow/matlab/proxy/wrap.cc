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
#include "arrow/matlab/error/error.h"
#include "arrow/matlab/proxy/wrap.h"
#include "arrow/visitor.h"
#include "libmexclass/proxy/ProxyManager.h"

namespace arrow::matlab::proxy {

    namespace {
        // using namespace arrow::matlab::array;

        struct ArrayProxyWrapperVisitor : public arrow::TypeVisitor {

            std::shared_ptr<arrow::Array> array_in_;
            std::shared_ptr<arrow::matlab::array::proxy::Array> array_proxy_out_;

            ArrayProxyWrapperVisitor(const std::shared_ptr<arrow::Array>& array) 
                : array_in_{array}
                , array_proxy_out_{nullptr}  {}

            virtual arrow::Status Visit(const arrow::BooleanType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::BooleanArray>(
                    std::static_pointer_cast<arrow::BooleanArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Int8Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Int8Type>>(
                    std::static_pointer_cast<arrow::Int8Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Int16Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Int16Type>>(
                    std::static_pointer_cast<arrow::Int16Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Int32Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Int32Type>>(
                    std::static_pointer_cast<arrow::Int32Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Int64Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Int64Type>>(
                    std::static_pointer_cast<arrow::Int64Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::UInt8Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::UInt8Type>>(
                    std::static_pointer_cast<arrow::UInt8Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::UInt16Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::UInt16Type>>(
                    std::static_pointer_cast<arrow::UInt16Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::UInt32Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::UInt32Type>>(
                    std::static_pointer_cast<arrow::UInt32Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::UInt64Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::UInt64Type>>(
                    std::static_pointer_cast<arrow::UInt64Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::FloatType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::FloatType>>(
                    std::static_pointer_cast<arrow::FloatArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::DoubleType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::DoubleType>>(
                    std::static_pointer_cast<arrow::DoubleArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::TimestampType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::TimestampType>>(
                    std::static_pointer_cast<arrow::TimestampArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Time32Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Time32Type>>(
                    std::static_pointer_cast<arrow::Time32Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Time64Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Time64Type>>(
                    std::static_pointer_cast<arrow::Time64Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Date32Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Date32Type>>(
                    std::static_pointer_cast<arrow::Date32Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::Date64Type&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::NumericArray<arrow::Date64Type>>(
                    std::static_pointer_cast<arrow::Date64Array>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::StringType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::StringArray>(
                    std::static_pointer_cast<arrow::StringArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::ListType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::ListArray>(
                    std::static_pointer_cast<arrow::ListArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            virtual arrow::Status Visit(const arrow::StructType&) override {
                array_proxy_out_ = std::make_shared<arrow::matlab::array::proxy::StructArray>(
                    std::static_pointer_cast<arrow::StructArray>(array_in_)
                );
                return arrow::Status::OK();
            }

            arrow::Result<std::shared_ptr<arrow::matlab::array::proxy::Array>> wrap() {
                auto status = array_in_->type()->Accept(this);
                if (status.ok()) {
                    return array_proxy_out_;
                } else {
                    return arrow::Status::NotImplemented("Unsupported Datatype: " + status.message());
                }
            }
        };

    } // anonymous namespace



    arrow::Result<std::shared_ptr<arrow::matlab::array::proxy::Array>> wrap(const std::shared_ptr<arrow::Array>& array) {
        ArrayProxyWrapperVisitor visitor{array};
        return visitor.wrap();
    }

    arrow::Result<std::shared_ptr<arrow::matlab::array::proxy::Array>> wrap(const std::shared_ptr<arrow::Array>& array) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;

        ArrayProxyWrapperVisitor visitor{array};
        ARROW_ASSIGN_OR_RAISE(auto proxy, visitor.wrap());
        const auto proxy_id = ProxyManager::manageProxy(proxy);

        mda::StructArray output = factory.createStructArray({1, 1}, {"ProxyID", "TypeID"});
        output[0]["ProxyID"] = factory.createScalar(proxy_id);
        output[0]["TypeID"] = factory.createScalar(array->type()->type_id());

        return output;
    }
}
