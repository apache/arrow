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

#include "arrow/matlab/proxy/array/double_array_proxy.h"

#include "arrow_proxy_factory.h"

#include <iostream>

std::shared_ptr<Proxy> ArrowProxyFactory::make_proxy(const ClassName& class_name, const FunctionArguments& constructor_arguments) {

    // Register MATLAB Proxy classes with corresponding C++ Proxy classes.
    REGISTER_PROXY(arrow.proxy.array.DoubleArrayProxy, arrow::matlab::proxy::array::DoubleArrayProxy);

    // TODO: Decide what to do in the case that there isn't a Proxy match.
    std::cout << "Did not find a matching C++ proxy for: " + class_name << std::endl;
    return nullptr;
};
