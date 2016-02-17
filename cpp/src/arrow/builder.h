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

#ifndef ARROW_BUILDER_H
#define ARROW_BUILDER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/buffer.h"
#include "arrow/util/macros.h"
#include "arrow/util/status.h"

namespace arrow {

class Array;

static constexpr int64_t MIN_BUILDER_CAPACITY = 1 << 8;

// Base class for all data array builders
class ArrayBuilder {
 public:
  explicit ArrayBuilder(const TypePtr& type)
      : type_(type),
        nullable_(type_->nullable),
        nulls_(nullptr), null_bits_(nullptr),
        length_(0),
        capacity_(0) {}

  virtual ~ArrayBuilder() {}

  // For nested types. Since the objects are owned by this class instance, we
  // skip shared pointers and just return a raw pointer
  ArrayBuilder* child(int i) {
    return children_[i].get();
  }

  int num_children() const {
    return children_.size();
  }

  int64_t length() const { return length_;}
  int64_t capacity() const { return capacity_;}
  bool nullable() const { return nullable_;}

  // Allocates requires memory at this level, but children need to be
  // initialized independently
  Status Init(int64_t capacity);

  // Resizes the nulls array (if nullable)
  Status Resize(int64_t new_bits);

  // For cases where raw data was memcpy'd into the internal buffers, allows us
  // to advance the length of the builder. It is your responsibility to use
  // this function responsibly.
  Status Advance(int64_t elements);

  const std::shared_ptr<OwnedMutableBuffer>& nulls() const { return nulls_;}

  // Creates new array object to hold the contents of the builder and transfers
  // ownership of the data
  virtual Status ToArray(Array** out) = 0;

 protected:
  TypePtr type_;
  bool nullable_;

  // If the type is not nullable, then null_ is nullptr after initialization
  std::shared_ptr<OwnedMutableBuffer> nulls_;
  uint8_t* null_bits_;

  // Array length, so far. Also, the index of the next element to be added
  int64_t length_;
  int64_t capacity_;

  // Child value array builders. These are owned by this class
  std::vector<std::unique_ptr<ArrayBuilder> > children_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ArrayBuilder);
};

} // namespace arrow

#endif // ARROW_BUILDER_H_
