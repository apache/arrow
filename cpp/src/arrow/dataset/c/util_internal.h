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

#include <memory>

namespace arrow {
namespace dataset {
namespace internal {

/// \brief Create a new shared_ptr on heap from shared_ptr t to maintain
/// a reference to the object after returning the value to C code.
///
/// \param[in] t shared_ptr to object to get an address for
/// \return address of the newly created shared_ptr
template <typename T>
uintptr_t create_native_ref(std::shared_ptr<T> t) {
    std::shared_ptr<T>* retained_ptr = new std::shared_ptr<T>(t);
    return reinterpret_cast<uintptr_t>(retained_ptr);
}

/// \brief Get the shared_ptr that was derived via create_native_ref.
///
/// \param[in] ref address of the shared_ptr
/// \return the shared_ptr object
template <typename T>
std::shared_ptr<T> retrieve_native_instance(uintptr_t ref) {
    std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
    return *retrieved_ptr;
}

/// \brief Destroy the shared_ptr using its memory address.
///
/// \param[in] ref address of the shared_ptr
template <typename T>
void release_native_ref(uintptr_t ref) {
    std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
    delete retrieved_ptr;
}

}
}
}
