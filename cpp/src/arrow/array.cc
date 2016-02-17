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

#include "arrow/array.h"

#include "arrow/util/buffer.h"

namespace arrow {

// ----------------------------------------------------------------------
// Base array class

Array::Array(const TypePtr& type, int64_t length,
    const std::shared_ptr<Buffer>& nulls) {
  Init(type, length, nulls);
}

void Array::Init(const TypePtr& type, int64_t length,
    const std::shared_ptr<Buffer>& nulls) {
  type_ = type;
  length_ = length;
  nulls_ = nulls;

  nullable_ = type->nullable;
  if (nulls_) {
    null_bits_ = nulls_->data();
  }
}

} // namespace arrow
