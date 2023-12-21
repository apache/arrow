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

#pragma once

#include "arrow/type.h"

#include "libmexclass/proxy/Proxy.h"

namespace arrow::matlab::type::proxy {

class Type : public libmexclass::proxy::Proxy {
    public:
        Type(std::shared_ptr<arrow::DataType> type);
    
        virtual ~Type() {}

        std::shared_ptr<arrow::DataType> unwrap();

    protected:

        void getTypeID(libmexclass::proxy::method::Context& context);

        void getNumFields(libmexclass::proxy::method::Context& context);

        void getFieldByIndex(libmexclass::proxy::method::Context& context);

        void isEqual(libmexclass::proxy::method::Context& context);

        std::shared_ptr<arrow::DataType> data_type;
};

}
