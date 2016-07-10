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

#ifndef ARROW_SCHEMA_H
#define ARROW_SCHEMA_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

struct Field;

class ARROW_EXPORT Schema {
 public:
  explicit Schema(const std::vector<std::shared_ptr<Field>>& fields);

  // Returns true if all of the schema fields are equal
  bool Equals(const Schema& other) const;
  bool Equals(const std::shared_ptr<Schema>& other) const;

  // Return the ith schema element. Does not boundscheck
  const std::shared_ptr<Field>& field(int i) const { return fields_[i]; }

  // Render a string representation of the schema suitable for debugging
  std::string ToString() const;

  int num_fields() const { return fields_.size(); }

 private:
  std::vector<std::shared_ptr<Field>> fields_;
};

}  // namespace arrow

#endif  // ARROW_FIELD_H
