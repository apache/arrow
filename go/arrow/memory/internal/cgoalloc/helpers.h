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

#include <cstdint>
#include <memory>

template <typename T>
uintptr_t create_ref(std::shared_ptr<T> t) {
    std::shared_ptr<T>* retained_ptr = new std::shared_ptr<T>(t);
    return reinterpret_cast<uintptr_t>(retained_ptr);
}

template <typename T>
uintptr_t create_ref(std::shared_ptr<const T> t) {
    std::shared_ptr<const T>* retained_ptr = new std::shared_ptr<const T>(t);
    return reinterpret_cast<uintptr_t>(retained_ptr);
}

template <typename T>
std::shared_ptr<T> retrieve_instance(uintptr_t ref) {
    std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
    return *retrieved_ptr;
}

template <typename T>
void release_ref(uintptr_t ref) {
    std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
    delete retrieved_ptr;
}
