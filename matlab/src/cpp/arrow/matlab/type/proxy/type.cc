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

#include "arrow/matlab/type/proxy/type.h"

namespace arrow::matlab::type::proxy {

    Type::Type(std::shared_ptr<arrow::DataType> type) : data_type{std::move(type)} {
        REGISTER_METHOD(Type, typeID);
        REGISTER_METHOD(Type, numFields);
    }

    std::shared_ptr<arrow::DataType> Type::unwrap() {
        return data_type;
    }

    void Type::typeID(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        
        auto type_number_mda = factory.createScalar(static_cast<int64_t>(data_type->id()));
        context.outputs[0] = type_number_mda;
    }

    void Type::numFields(libmexclass::proxy::method::Context& context) {
        namespace mda = ::matlab::data;
        mda::ArrayFactory factory;
        
        auto num_fields_mda = factory.createScalar(data_type->num_fields());
        context.outputs[0] = num_fields_mda;
    }

}

