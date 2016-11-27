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
#include "arrow/types/primitive.h"

namespace arrow {

class Buffer;

class ARROW_EXPORT UnionArray : public Array {
 public:
  UnionArray(const TypePtr& type, int32_t length, std::vector<ArrayPtr>& children,
       std::shared_ptr<Buffer> types, std::shared_ptr<Buffer> offset_buf,
       int32_t null_count = 0, std::shared_ptr<Buffer> null_bitmap = nullptr)
       : Array(type, length, null_count, null_bitmap), types_(types) {
    type_ = type;
    children_ = children;
    offset_buf_ = offset_buf;
  }

  const std::shared_ptr<Buffer>& types() const { return types_; }

  const std::vector<ArrayPtr>& children() const { return children_; }

  const std::shared_ptr<Buffer>& offset_buf() const { return offset_buf_; }

  Status Validate() const override;
  
  Status Accept(ArrayVisitor* visitor) const override;

  bool Equals(const std::shared_ptr<Array>& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const std::shared_ptr<Array>& arr) const override;

  ArrayPtr  child(int32_t index) const { return children_[index]; }
 protected:
  // The data are types encoded as int16
  std::shared_ptr<Buffer> types_;
  std::vector<std::shared_ptr<Array>> children_;
  std::shared_ptr<Buffer> offset_buf_;
};

}  // namespace arrow

#endif  // ARROW_TYPES_UNION_H
