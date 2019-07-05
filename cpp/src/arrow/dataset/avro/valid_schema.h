/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "arrow/dataset/avro/node.h"

namespace arrow {
namespace avro {

class Schema;

/// A ValidSchema is basically a immutable Schema that has passed some
/// minumum of sanity checks.  Once valididated, any Schema that is part of
/// this ValidSchema is considered locked, and cannot be modified (an attempt
/// to modify a locked Schema will throw).  Also, as it is validated, any
/// recursive duplications of schemas are replaced with symbolic links to the
/// original.
///
/// Once a Schema is converted to a valid schema it can be used in validating
/// parsers/serializers, converted to a json schema, etc.
///

class ValidSchema {
 public:
  explicit ValidSchema(const NodePtr& root);
  explicit ValidSchema(const Schema& schema);
  ValidSchema();

  void SetSchema(const Schema& schema);

  const std::shared_ptr<Node>& root() const { return root_; }

  void ToJson(std::ostream& os) const;
  std::string ToJson(bool prettyPrint = true) const;

  void ToFlatList(std::ostream& os) const;

 protected:
  std::shared_ptr<Node> root_;

 private:
  static std::string CompactSchema(const std::string& schema);
};

}  // namespace avro
}

#endif
