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

#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/buffer-builder.h"

namespace arrow {

class ARROW_EXPORT UnionBuilder : public ArrayBuilder {
 public:
  UnionBuilder(MemoryPool* pool,
               const std::vector<std::shared_ptr<ArrayBuilder>>& children);

  Status AppendNull() {
    ARROW_RETURN_NOT_OK(types_builder_.Append(0));
    ARROW_RETURN_NOT_OK(offsets_builder_.Append(0));
    return AppendToBitmap(false);
  }

  Status Append(int8_t type, int32_t offset) {
    ARROW_RETURN_NOT_OK(types_builder_.Append(type));
    ARROW_RETURN_NOT_OK(offsets_builder_.Append(offset));
    return AppendToBitmap(true);
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  void SetChild(int32_t i, const std::shared_ptr<ArrayBuilder>& child) {
    children_[i] = child;
  }

 private:
  TypedBufferBuilder<int8_t> types_builder_;
  TypedBufferBuilder<int32_t> offsets_builder_;
};

}  // namespace arrow
