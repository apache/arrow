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

#include "libmexclass/proxy/Proxy.h"

#include "arrow/array.h"
#include "arrow/builder.h"

using namespace libmexclass::proxy;

class DoubleArrayProxy : public libmexclass::proxy::Proxy {
    public:
        DoubleArrayProxy(const FunctionArguments& constructor_arguments) {
            // TODO: Implement initialization of Array from the MATLAB mxArray passed to the constructor.
            arrow::DoubleBuilder builder;

            auto status = builder.Append(1.0);
            status = builder.Append(2.0);
            status = builder.Append(3.0);

            auto maybe_array = builder.Finish();
            if (!maybe_array.ok()) {
                // TODO: Handle possible errors.
            }

            array = *maybe_array;

            // Register Proxy methods.
            registerMethod(DoubleArrayProxy, Print);
        }
    private:
        void Print(method::Context& context);

        // "Raw" arrow::Array
        std::shared_ptr<arrow::Array> array;
};
