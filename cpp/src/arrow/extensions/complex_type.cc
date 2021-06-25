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

// Complex Number Extension Type

#include <mutex>
#include <thread>
#include <sstream>

#include "arrow/extensions/complex_type.h"

namespace arrow {

bool ComplexFloatType::ExtensionEquals(const ExtensionType& other) const {
  const auto& other_ext = static_cast<const ExtensionType&>(other);
  return other_ext.extension_name() == this->extension_name();
}

bool ComplexDoubleType::ExtensionEquals(const ExtensionType& other) const {
  const auto& other_ext = static_cast<const ExtensionType&>(other);
  return other_ext.extension_name() == this->extension_name();
}


std::shared_ptr<DataType> complex64() {
  return std::make_shared<ComplexFloatType>();
}

std::shared_ptr<DataType> complex128() {
  return std::make_shared<ComplexDoubleType>();
}

/// NOTE(sjperkins)
// Suggestions on how to improve this welcome!
std::once_flag complex_float_registered;
std::once_flag complex_double_registered;

Status register_complex_types()
{
  std::call_once(complex_float_registered,
                 RegisterExtensionType,
                 std::make_shared<ComplexFloatType>());

  std::call_once(complex_double_registered,
                 RegisterExtensionType,
                 std::make_shared<ComplexDoubleType>());
  
  return Status::OK();
}

static Status complex_types_registered = register_complex_types();

};  // namespace arrow
