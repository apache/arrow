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
#include <type_traits>
#include <utility>

#include "arrow/util/macros.h"

namespace arrow {
namespace util {

/// CRTP helper for declaring equality comparison. Defines operator== and operator!=
template <typename T>
class EqualityComparable {
 public:
  ~EqualityComparable() {
    static_assert(
        std::is_same<decltype(std::declval<const T>().Equals(std::declval<const T>())),
                     bool>::value,
        "EqualityComparable depends on the method T::Equals(const T&) const");
  }

  template <typename... Extra>
  bool Equals(const std::shared_ptr<T>& other, Extra&&... extra) const {
    if (other == NULLPTR) {
      return false;
    }
    return cast().Equals(*other, std::forward<Extra>(extra)...);
  }

  struct PtrsEqual {
    bool operator()(const std::shared_ptr<T>& l, const std::shared_ptr<T>& r) const {
      return l->Equals(r);
    }
  };

  bool operator==(const T& other) const { return cast().Equals(other); }
  bool operator!=(const T& other) const { return !(cast() == other); }

 private:
  const T& cast() const { return static_cast<const T&>(*this); }
};

/// CRTP helper for declaring order comparison. Defines the operators < > <= >=
template <typename T>
class OrderComparable {
 public:
  using P = std::shared_ptr<T>;

  ~OrderComparable() {
    static_assert(
        std::is_same<decltype(std::declval<const T>().LessThan(std::declval<const T>())),
                     bool>::value,
        "OrderComparable depends on the method T::LessThan(const T&) const");
    static_assert(
        std::is_same<decltype(std::declval<const T>().IsAtMost(std::declval<const T>())),
                     bool>::value,
        "OrderComparable depends on the method T::IsAtMost(const T&) const");
  }

  template <typename... Extra>
  bool LessThan(const P& other, Extra&&... extra) const {
    if (other == NULLPTR) {
      return false;
    }
    return cast().LessThan(*other, std::forward<Extra>(extra)...);
  }

  template <typename... Extra>
  bool IsAtMost(const P& other, Extra&&... extra) const {
    if (other == NULLPTR) {
      return false;
    }
    return cast().IsAtMost(*other, std::forward<Extra>(extra)...);
  }

  template <typename... Extra>
  bool MoreThan(const P& other, Extra&&... extra) const {
    if (other == NULLPTR) {
      return false;
    }
    return !cast().IsAtMost(*other, std::forward<Extra>(extra)...);
  }

  template <typename... Extra>
  bool IsAtLeast(const P& other, Extra&&... extra) const {
    if (other == NULLPTR) {
      return false;
    }
    return !cast().LessThan(*other, std::forward<Extra>(extra)...);
  }

  struct PtrsLessThan {
    bool operator()(const P& l, const P& r) const { return l->LessThan(*r); }
  };

  struct PtrsIsAtMost {
    bool operator()(const P& l, const P& r) const { return l->IsAtMost(*r); }
  };

  struct PtrsMoreThan {
    bool operator()(const P& l, const P& r) const { return l->MoreThan(*r); }
  };

  struct PtrsIsAtLeast {
    bool operator()(const P& l, const P& r) const { return l->IsAtLeast(*r); }
  };

  bool MoreThan(const T& other) const { return !cast().IsAtMost(other); }
  bool IsAtLeast(const T& other) const { return !cast().LessThan(other); }

  bool operator<(const T& other) const { return cast().LessThan(other); }
  bool operator<=(const T& other) const { return cast().IsAtMost(other); }
  bool operator>(const T& other) const { return cast().MoreThan(other); }
  bool operator>=(const T& other) const { return cast().IsAtLeast(other); }

  bool operator<(const P& other) const { return LessThan(other); }
  bool operator<=(const P& other) const { return IsAtMost(other); }
  bool operator>(const P& other) const { return MoreThan(other); }
  bool operator>=(const P& other) const { return IsAtLeast(other); }

 private:
  const T& cast() const { return static_cast<const T&>(*this); }
};

}  // namespace util
}  // namespace arrow
