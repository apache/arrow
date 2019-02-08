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

// Object model for scalar (non-Array) values. Not intended for use with large
// amounts of data

#pragma once

#include "arrow/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \brief Base class for scalar values, representing a single value occupying
/// an array "slot"
class ARROW_EXPORT Scalar {
 public:
  bool is_valid() const { return is_valid_; }
  std::shared_ptr<DataType> type() const { return type_; }

 protected:
  Scalar(bool is_valid, const std::shared_ptr<DataType>& type)
    : is_valid_(is_valid), type_(type) {}
  bool is_valid_;
  std::shared_ptr<DataType> type_;
};

template <typename Type>
class CTypeScalar : public Scalar {
 public:
  using T = typename Type::c_type;

  T value() const { return value_; }

 private:
  CTypeScalar(T value, bool is_valid = true)
      : Scalar(is_valid, TypeTraits<Type>::type_singleton()),
        value_(value)

  T value_;
};

class BinaryScalar : public Scalar {
 protected:
  std::shared_ptr<Buffer> value_;
};

class ListScalar : public Scalar {
 protected:
  std::shared_ptr<Array> value_;
};

}  // namespace arrow
