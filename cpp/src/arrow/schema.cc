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

#include "arrow/schema.h"

#include <memory>
#include <string>
#include <sstream>
#include <vector>

#include "arrow/type.h"

namespace arrow {

Schema::Schema(const std::vector<std::shared_ptr<Field>>& fields) :
    fields_(fields) {}

bool Schema::Equals(const Schema& other) const {
  if (this == &other) return true;
  if (num_fields() != other.num_fields()) {
    return false;
  }
  for (int i = 0; i < num_fields(); ++i) {
    if (!field(i)->Equals(*other.field(i).get())) {
      return false;
    }
  }
  return true;
}

bool Schema::Equals(const std::shared_ptr<Schema>& other) const {
  return Equals(*other.get());
}

std::string Schema::ToString() const {
  std::stringstream buffer;

  int i = 0;
  for (auto field : fields_) {
    if (i > 0) {
      buffer << std::endl;
    }
    buffer << field->ToString();
    ++i;
  }
  return buffer.str();
}

} // namespace arrow
