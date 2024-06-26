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

#include "arrow/array.h"
#include "arrow/matlab/type/proxy/type.h"

#include "libmexclass/proxy/Proxy.h"

namespace arrow::matlab::array::proxy {

class Array : public libmexclass::proxy::Proxy {
 public:
  Array(std::shared_ptr<arrow::Array> array);

  virtual ~Array() {}

  std::shared_ptr<arrow::Array> unwrap();

 protected:
  void toString(libmexclass::proxy::method::Context& context);

  void getNumElements(libmexclass::proxy::method::Context& context);

  void getValid(libmexclass::proxy::method::Context& context);

  void getType(libmexclass::proxy::method::Context& context);

  void isEqual(libmexclass::proxy::method::Context& context);

  void slice(libmexclass::proxy::method::Context& context);

  void exportToC(libmexclass::proxy::method::Context& context);

  std::shared_ptr<arrow::Array> array;
};

}  // namespace arrow::matlab::array::proxy
