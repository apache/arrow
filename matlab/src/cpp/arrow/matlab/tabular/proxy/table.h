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

#include "arrow/table.h"

#include "libmexclass/proxy/Proxy.h"

namespace arrow::matlab::tabular::proxy {

    class Table : public libmexclass::proxy::Proxy {
        public:
            Table(std::shared_ptr<arrow::Table> table);

            virtual ~Table() {}

            std::shared_ptr<arrow::Table> unwrap();

            static libmexclass::proxy::MakeResult make(const libmexclass::proxy::FunctionArguments& constructor_arguments);

        protected:
            void toString(libmexclass::proxy::method::Context& context);
            void getNumRows(libmexclass::proxy::method::Context& context);
            void getNumColumns(libmexclass::proxy::method::Context& context);
            void getColumnNames(libmexclass::proxy::method::Context& context);
            void getSchema(libmexclass::proxy::method::Context& context);
            void getColumnByIndex(libmexclass::proxy::method::Context& context);
            void getColumnByName(libmexclass::proxy::method::Context& context);
            void getRowAsString(libmexclass::proxy::method::Context& context);

            std::shared_ptr<arrow::Table> table;
    };

}
