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

#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/visit_type_inline.h"

namespace arrow::util {

namespace detail {

struct ValueVisitor {
  template <typename ValueType, typename Function, typename... Args>
  enable_if_number<ValueType, Status> Visit(const ValueType& value_type,
                                            Function&& function, Args&&... args) {
    return function(value_type, std::forward<Args>(args)...);
  }

  template <typename... Args>
  Status Visit(const DataType& value_type, Args&&... args) {
    return Status::TypeError("Invalid value type: ", value_type.name(),
                             ". Expected a number.");
  }
};

struct IndexVisitor {
  template <typename IndexType, typename Function, typename... Args>
  enable_if_integer<IndexType, Status> Visit(const IndexType& index_type,
                                             Function&& function,
                                             const DataType& value_type, Args&&... args) {
    ValueVisitor visitor;
    return VisitTypeInline(value_type, &visitor, std::forward<Function>(function),
                           index_type, std::forward<Args>(args)...);
  }

  template <typename... Args>
  Status Visit(const DataType& index_type, Args&&...) {
    return Status::TypeError("Invalid index pointer type: ", index_type.name(),
                             ". Expected integer.");
  }
};

struct IndexPointerVisitor {
  template <typename IndexPointerType, typename Function>
  enable_if_integer<IndexPointerType, Status> Visit(
      const IndexPointerType& index_pointer_type, Function&& function,
      const DataType& index_type, const DataType& value_type) {
    IndexVisitor visitor;
    return VisitTypeInline(index_type, &visitor, std::forward<Function>(function),
                           value_type, index_pointer_type);
  }

  template <typename... Args>
  Status Visit(const DataType& index_pointer_type, Args&&...) {
    return Status::TypeError("Invalid index pointer type: ", index_pointer_type.name(),
                             ". Expected integer.");
  }
};

}  // namespace detail

template <typename Function>
inline Status VisitCSXType(const DataType& value_type, const DataType& index_type,
                           const DataType& indptr_type, Function&& function) {
  detail::IndexPointerVisitor visitor;
  return VisitTypeInline(indptr_type, &visitor, std::forward<Function>(function),
                         index_type, value_type);
}

template <typename Function>
inline Status VisitCOOTensorType(const DataType& value_type, const DataType& index_type,
                                 Function&& function) {
  detail::IndexVisitor visitor;
  return VisitTypeInline(index_type, &visitor, std::forward<Function>(function),
                         value_type);
}

}  // namespace arrow::util
