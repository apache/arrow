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

// Private header, not to be exported

#pragma once

#include "arrow/array.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/util/bitmap_inline.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string_view.h"
#include "arrow/visit_array_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

// Visit an array's data values, in order, without overhead.
//
// The Visit function's `visitor` argument should define two public methods:
// - Status VisitNull()
// - Status VisitValue(<scalar>)
//
// The scalar value's type depends on the array data type:
// - the type's `c_type`, if any
// - for boolean arrays, a `bool`
// - for binary, string and fixed-size binary arrays, a `util::string_view`

template <typename T, typename Enable = void>
struct ArrayDataVisitor {};

template <>
struct ArrayDataVisitor<BooleanType> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          ARROW_RETURN_NOT_OK(visitor->VisitValue(value_reader.IsSet()));
        }
        valid_reader.Next();
        value_reader.Next();
      }
    } else {
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        ARROW_RETURN_NOT_OK(visitor->VisitValue(value_reader.IsSet()));
        value_reader.Next();
      }
    }
    return Status::OK();
  }
};

template <typename T>
struct ArrayDataVisitor<T, enable_if_has_c_type<T>> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    using c_type = typename T::c_type;
    const c_type* data = arr.GetValues<c_type>(1);

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          ARROW_RETURN_NOT_OK(visitor->VisitValue(data[i]));
        }
        valid_reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        ARROW_RETURN_NOT_OK(visitor->VisitValue(data[i]));
      }
    }
    return Status::OK();
  }
};

template <typename T>
struct ArrayDataVisitor<T, enable_if_base_binary<T>> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    using offset_type = typename T::offset_type;
    constexpr uint8_t empty_value = 0;

    const offset_type* offsets = arr.GetValues<offset_type>(1);
    const uint8_t* data;
    if (!arr.buffers[2]) {
      data = &empty_value;
    } else {
      // Do not use array offset here, as the sliced offsets array refers
      // to the non-sliced values array.
      data = arr.GetValues<uint8_t>(2, /*absolute_offset=*/0);
    }

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                         offsets[i + 1] - offsets[i]);
          ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
        }
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                       offsets[i + 1] - offsets[i]);
        ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
      }
    }
    return Status::OK();
  }
};

template <typename T>
struct ArrayDataVisitor<T, enable_if_fixed_size_binary<T>> {
  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    const auto& fw_type = internal::checked_cast<const FixedSizeBinaryType&>(*arr.type);

    const int32_t byte_width = fw_type.byte_width();
    const uint8_t* data =
        arr.GetValues<uint8_t>(1,
                               /*absolute_offset=*/arr.offset * byte_width);

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          ARROW_RETURN_NOT_OK(visitor->VisitNull());
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
          ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
        }
        data += byte_width;
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
        ARROW_RETURN_NOT_OK(visitor->VisitValue(value));
        data += byte_width;
      }
    }
    return Status::OK();
  }
};

}  // namespace arrow
