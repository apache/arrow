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

#include <cassert>
#include <cstdint>
#include "arrow/array/data.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/macros.h"

namespace arrow::internal {

struct BitmapTag {
  enum Value {
    kMustCheck,  // bitmap pointer must be checked at runtime
    kChecked,    // bitmap pointer is known to be non-null
    kEmpty,      // bitmap pointer is known to be null
  };

  // kChecked <~ kMustCheck
  // kEmpty <~ kMustCheck
  template <Value Lhs, Value Rhs>
  struct PartialOrd {
    static constexpr bool lte = Lhs == Rhs || Rhs == kMustCheck;
    static constexpr bool gte = Lhs == Rhs || Lhs == kMustCheck;
  };
};

inline namespace validity_internal {

/// \brief Non-owning and immutable validity bitmap view.
template <BitmapTag::Value Tag = BitmapTag::kMustCheck>
class OptionalValidity {
 private:
  template <typename From>
  static constexpr bool CanAssign = BitmapTag::PartialOrd<Tag, From::kBitmapTag>::gte;

  template <BitmapTag::Value Override, BitmapTag::Value Original>
  static constexpr bool IsProfitableOverride =
      BitmapTag::PartialOrd<Override, Original>::lte;

 public:
  static constexpr BitmapTag::Value kBitmapTag = Tag;

  // Commonly used tag predicates
  static constexpr bool kMustCheck = Tag == BitmapTag::kMustCheck;
  static constexpr bool kEitherEmptyOrChecked = !kMustCheck;
  static constexpr bool kMayHaveBitmap = Tag != BitmapTag::kEmpty;

  int64_t length;              // the length of the array this bitmap refers to
  const uint8_t* bitmap;       // can be nullptr if Tag is kEmpty or kMustCheck
  mutable int64_t null_count;  // kUnknownNullCount if not precomputed
  int64_t offset;              // offset into the bitmap (in bits)

  explicit OptionalValidity(const ArrayData& data) noexcept
      : OptionalValidity(data.length,
                         data.MayHaveNulls() ? data.buffers[0]->data() : nullptr,
                         data.null_count, data.offset) {}

  explicit OptionalValidity(const ArraySpan& array) noexcept
      : OptionalValidity(array.length, array.buffers[0].data, array.null_count,
                         array.offset) {}

  OptionalValidity(int64_t length, const uint8_t* bitmap,
                   int64_t null_count = kUnknownNullCount, int64_t offset = 0) noexcept
      : length(length), bitmap(bitmap), offset(offset) {
    if constexpr (Tag == BitmapTag::kChecked) {
      assert(bitmap != nullptr);
      this->null_count = length == 0 ? 0 : null_count;
    } else if constexpr (Tag == BitmapTag::kEmpty) {
      assert(bitmap == nullptr || length == 0 || null_count == 0);
      if (length == 0 || null_count == 0) {
        this->bitmap = nullptr;
      }
      this->null_count = 0;
    } else {  // kMustCheck
      this->null_count = (bitmap == nullptr || length == 0) ? 0 : null_count;
    }
  }

  template <class Other>
  OptionalValidity(  // NOLINT implicit
      const std::enable_if_t<CanAssign<Other>, Other>& other) noexcept
      : length(other.length),
        bitmap(other.bitmap),
        null_count(other.null_count),
        offset(other.offset) {}

  template <class Other>
  OptionalValidity& operator=(
      const std::enable_if_t<CanAssign<Other>, Other>& other) noexcept {
    length = other.length;
    bitmap = other.bitmap;
    null_count = other.null_count;
    offset = other.offset;
    return *this;
  }

  template <BitmapTag::Value To>
  inline bool CanCast() const {
    if constexpr (BitmapTag::PartialOrd<Tag, To>::lte) {
      return true;
    } else if constexpr (To == BitmapTag::kEmpty) {
      return bitmap == nullptr || length == 0 || null_count == 0;
    } else if constexpr (To == BitmapTag::kChecked) {
      return bitmap != nullptr;
    }
    return false;
  }

  template <BitmapTag::Value To>
  inline OptionalValidity<To>* TryCast() && {
    if constexpr (BitmapTag::PartialOrd<Tag, To>::lte) {
      return reinterpret_cast<OptionalValidity<To>*>(this);
    } else if constexpr (To == BitmapTag::kEmpty) {
      if (bitmap == nullptr || length == 0 || null_count == 0) {
        bitmap = nullptr;
        return reinterpret_cast<OptionalValidity<To>*>(this);
      }
    } else if constexpr (To == BitmapTag::kChecked) {
      if (bitmap != nullptr) {
        return reinterpret_cast<OptionalValidity<To>*>(this);
      }
    }
    return nullptr;
  }

  inline bool HasBitmap() const {
    if constexpr (Tag == BitmapTag::kEmpty) {
      return false;
    } else if constexpr (Tag == BitmapTag::kChecked) {
      return true;
    } else {
      return bitmap != nullptr;
    }
  }

  int64_t GetNullCount() const {
    if constexpr (Tag == BitmapTag::kEmpty) {
      return 0;
    }
    int64_t precomputed = this->null_count;
    if (ARROW_PREDICT_FALSE(precomputed == kUnknownNullCount)) {
      if (Tag == BitmapTag::kChecked || bitmap != nullptr) {
        precomputed = this->length - CountSetBits(bitmap, this->offset, this->length);
      } else {
        precomputed = 0;
      }
    }
    this->null_count = precomputed;
    return precomputed;
  }

  template <BitmapTag::Value UnsafeTagOverride = Tag>
  inline std::enable_if_t<IsProfitableOverride<UnsafeTagOverride, Tag>, bool> IsValid(
      int64_t i) const {
    if constexpr (UnsafeTagOverride == BitmapTag::kEmpty) {
      return true;
    } else if constexpr (UnsafeTagOverride == BitmapTag::kMustCheck) {
      if (bitmap == nullptr) {
        return true;
      }
      return bit_util::GetBit(bitmap, offset + i);
    } else {
      return bit_util::GetBit(bitmap, offset + i);
    }
  }

  template <BitmapTag::Value UnsafeTagOverride = Tag>
  inline bool IsNull(int64_t i) const {
    return !IsValid<UnsafeTagOverride>(i);
  }
};

}  // namespace validity_internal
}  // namespace arrow::internal
