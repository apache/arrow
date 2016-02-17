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

#ifndef ARROW_FIELD_H
#define ARROW_FIELD_H

#include <string>

#include "arrow/type.h"

namespace arrow {

// A field is a piece of metadata that includes (for now) a name and a data
// type

struct Field {
  // Field name
  std::string name;

  // The field's data type
  TypePtr type;

  Field(const std::string& name, const TypePtr& type) :
      name(name), type(type) {}

  bool Equals(const Field& other) const {
    return (this == &other) || (this->name == other.name &&
        this->type->Equals(other.type.get()));
  }
};

} // namespace arrow

#endif  // ARROW_FIELD_H
