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

#include "arrow/c/abi.h"

#include "arrow/matlab/c/proxy/array.h"

#include "libmexclass/proxy/Proxy.h"

#include <memory.h>
#include <type_traits>

namespace arrow::matlab::c::proxy {

  struct ArrowArrayDeleter {
    void operator()(ArrowArray* array) const {
      if (array) {
        free(array);
      }
    }
  };

  Array::Array() : arrowArray{std::shared_ptr<ArrowArrayPtr>(new ArrowArray(), ArrowArrayDeleter())} {}

  Array::~Array() {
    if (arrowArray && arrowArray->released != nullptr) {
      arrowArray->release(arrowArray.get());
    }
  }

} // namespace arrow::matlab::c::proxy