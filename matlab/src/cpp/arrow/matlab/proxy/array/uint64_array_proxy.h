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

#include <_types/_uint64_t.h>
#include "libmexclass/proxy/Proxy.h"

#include "arrow/array.h"
#include "arrow/builder.h"

namespace proxy::array {
class UInt64ArrayProxy : public libmexclass::proxy::Proxy {
    public:
        UInt64ArrayProxy(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            // TODO: Implement initialization of Array from the MATLAB mxArray passed to the constructor.
            std::cout << "UInt64ArrayProxy constructor: " << std::endl;

            // Get the mxArray from constructor arguments
            matlab::data::TypedArray<uint64_t> uint64_mda = constructor_arguments[0];

            std::cout << "1. Get raw pointer of mxArray" << std::endl;
            // Get raw pointer of mxArray
            matlab::data::TypedIterator<uint64_t> it(uint64_mda.begin());
            auto dt = it.operator->();

            std::cout << "2. Pass raw pointer to construct array" << std::endl;
            // Pass pointer to Arrow array constructor that takes a buffer
            // Do not make a copy when creating arrow::Buffer
            std::shared_ptr<arrow::Buffer> buffer(
                  new arrow::Buffer(reinterpret_cast<const uint8_t*>(dt), 
                                    sizeof(uint64_t) * uint64_mda.getNumberOfElements()));
            
            // Construct arrow::NumericArray specialization using arrow::Buffer.
            // pass in nulls information...we could compute and provide the number of nulls here too
            std::shared_ptr<arrow::Array> array_wrapper(
                new arrow::NumericArray<arrow::UInt64Type>(uint64_mda.getNumberOfElements(), buffer,
                                                       nullptr, // TODO: fill validity bitmap with data
                                                       -1));

            std::cout << "3. Return array to be stored in LifetimeManager" << std::endl;
            array = array_wrapper;

            // Register Proxy methods.
            registerMethod(UInt64ArrayProxy, Print);
        }
    private:
        void Print(libmexclass::proxy::method::Context& context);

        // "Raw" arrow::Array
        std::shared_ptr<arrow::Array> array;
};
} // namespace proxy::array