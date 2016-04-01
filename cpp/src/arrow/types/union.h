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

#ifndef ARROW_TYPES_UNION_H
#define ARROW_TYPES_UNION_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/types/collection.h"

namespace arrow {

class Buffer;

struct DenseUnionType : public CollectionType<Type::DENSE_UNION> {
  typedef CollectionType<Type::DENSE_UNION> Base;

  explicit DenseUnionType(const std::vector<TypePtr>& child_types) : Base() {
    child_types_ = child_types;
  }

  virtual std::string ToString() const;
};

struct SparseUnionType : public CollectionType<Type::SPARSE_UNION> {
  typedef CollectionType<Type::SPARSE_UNION> Base;

  explicit SparseUnionType(const std::vector<TypePtr>& child_types) : Base() {
    child_types_ = child_types;
  }

  virtual std::string ToString() const;
};

class UnionArray : public Array {
 protected:
  // The data are types encoded as int16
  Buffer* types_;
  std::vector<std::shared_ptr<Array>> children_;
};

class DenseUnionArray : public UnionArray {
 protected:
  Buffer* offset_buf_;
};

class SparseUnionArray : public UnionArray {};

}  // namespace arrow

#endif  // ARROW_TYPES_UNION_H
