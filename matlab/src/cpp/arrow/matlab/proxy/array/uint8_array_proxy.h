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

namespace proxy::array {
class UInt8ArrayProxy : public libmexclass::proxy::Proxy {
    private:
        // const matlab::data::TypedArray<uint8_t> uint8_mda;

    public:
        UInt8ArrayProxy(const libmexclass::proxy::FunctionArguments& constructor_arguments) {
            // Fewer copy version

            // Get the mxArray from constructor arguments
            const matlab::data::TypedArray<uint8_t> uint8_mda = constructor_arguments[0];

            for (auto& elem : uint8_mda) {
                std::cout << elem << std::endl;
            }

            // Create a shared data copy of uint8_mda and store it as a property of proxy class 
            // Get raw pointer of mxArray
            auto it = uint8_mda.cbegin();
            auto dt = it.operator->();

            // Pass pointer to Arrow array constructor that takes a buffer
            // Do not make a copy when creating arrow::Buffer
            std::shared_ptr<arrow::Buffer> buffer(
                  new arrow::Buffer(reinterpret_cast<const uint8_t*>(dt), 
                                    sizeof(uint8_t) * uint8_mda.getNumberOfElements()));
            
            // Construct arrow::NumericArray specialization using arrow::Buffer.
            // pass in nulls information...we could compute and provide the number of nulls here too
            std::shared_ptr<arrow::Array> array_wrapper(
                new arrow::NumericArray<arrow::UInt8Type>(uint8_mda.getNumberOfElements(), buffer,
                                                       nullptr, // TODO: fill validity bitmap with data
                                                       -1));

            array = array_wrapper;
            
            // Register Proxy methods.
            registerMethod(UInt8ArrayProxy, Print);
        }
    private:
        void Print(libmexclass::proxy::method::Context& context);

        // "Raw" arrow::Array
        std::shared_ptr<arrow::Array> array;
};
} // namespace proxy::array