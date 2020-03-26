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

#include <utility>

#include "arrow/array.h"
#include "arrow/extension_type.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/functional.h"
#include "arrow/util/optional.h"
#include "arrow/util/string_view.h"

namespace arrow {

#define ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(ACTION) \
  ACTION(Int8);                                      \
  ACTION(UInt8);                                     \
  ACTION(Int16);                                     \
  ACTION(UInt16);                                    \
  ACTION(Int32);                                     \
  ACTION(UInt32);                                    \
  ACTION(Int64);                                     \
  ACTION(UInt64)

#define ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(ACTION) \
  ARROW_GENERATE_FOR_ALL_INTEGER_TYPES(ACTION);      \
  ACTION(HalfFloat);                                 \
  ACTION(Float);                                     \
  ACTION(Double)

#define ARROW_GENERATE_FOR_ALL_TYPES(ACTION)    \
  ACTION(Null);                                 \
  ACTION(Boolean);                              \
  ARROW_GENERATE_FOR_ALL_NUMERIC_TYPES(ACTION); \
  ACTION(String);                               \
  ACTION(Binary);                               \
  ACTION(LargeString);                          \
  ACTION(LargeBinary);                          \
  ACTION(FixedSizeBinary);                      \
  ACTION(Duration);                             \
  ACTION(Date32);                               \
  ACTION(Date64);                               \
  ACTION(Timestamp);                            \
  ACTION(Time32);                               \
  ACTION(Time64);                               \
  ACTION(Decimal128);                           \
  ACTION(List);                                 \
  ACTION(LargeList);                            \
  ACTION(Map);                                  \
  ACTION(FixedSizeList);                        \
  ACTION(Struct);                               \
  ACTION(Union);                                \
  ACTION(Dictionary);                           \
  ACTION(Extension)

#define TYPE_VISIT_INLINE(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:     \
    return visitor->Visit(internal::checked_cast<const TYPE_CLASS##Type&>(type));

template <typename VISITOR>
inline Status VisitTypeInline(const DataType& type, VISITOR* visitor) {
  switch (type.id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(TYPE_VISIT_INLINE);
    case Type::INTERVAL: {
      const auto& interval_type = dynamic_cast<const IntervalType&>(type);
      if (interval_type.interval_type() == IntervalType::MONTHS) {
        return visitor->Visit(internal::checked_cast<const MonthIntervalType&>(type));
      }
      if (interval_type.interval_type() == IntervalType::DAY_TIME) {
        return visitor->Visit(internal::checked_cast<const DayTimeIntervalType&>(type));
      }
      break;
    }
    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

#undef TYPE_VISIT_INLINE

#define ARRAY_VISIT_INLINE(TYPE_CLASS)                                                   \
  case TYPE_CLASS##Type::type_id:                                                        \
    return visitor->Visit(                                                               \
        internal::checked_cast<const typename TypeTraits<TYPE_CLASS##Type>::ArrayType&>( \
            array));

template <typename VISITOR>
inline Status VisitArrayInline(const Array& array, VISITOR* visitor) {
  switch (array.type_id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(ARRAY_VISIT_INLINE);
    case Type::INTERVAL: {
      const auto& interval_type = dynamic_cast<const IntervalType&>(*array.type());
      if (interval_type.interval_type() == IntervalType::MONTHS) {
        return visitor->Visit(internal::checked_cast<const MonthIntervalArray&>(array));
      }
      if (interval_type.interval_type() == IntervalType::DAY_TIME) {
        return visitor->Visit(internal::checked_cast<const DayTimeIntervalArray&>(array));
      }
      break;
    }

    default:
      break;
  }
  return Status::NotImplemented("Type not implemented");
}

namespace internal {

template <typename T, typename Enable = void>
struct ArrayDataInlineVisitor {};

// Numeric and primitive C-compatible types
template <typename T>
struct ArrayDataInlineVisitor<T, enable_if_has_c_type<T>> {
  using c_type = typename T::c_type;

  template <typename VisitFunc>
  static Status VisitStatus(const ArrayData& arr, VisitFunc&& func) {
    const c_type* data = arr.GetValues<c_type>(1);

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          ARROW_RETURN_NOT_OK(func(util::optional<c_type>()));
        } else {
          ARROW_RETURN_NOT_OK(func(util::optional<c_type>(data[i])));
        }
        valid_reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        ARROW_RETURN_NOT_OK(func(util::optional<c_type>(data[i])));
      }
    }
    return Status::OK();
  }

  template <typename VisitFunc>
  static void VisitVoid(const ArrayData& arr, VisitFunc&& func) {
    using c_type = typename T::c_type;
    const c_type* data = arr.GetValues<c_type>(1);

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          func(util::optional<c_type>());
        } else {
          func(util::optional<c_type>(data[i]));
        }
        valid_reader.Next();
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        func(util::optional<c_type>(data[i]));
      }
    }
  }
};

// Boolean
template <>
struct ArrayDataInlineVisitor<BooleanType> {
  using c_type = bool;

  template <typename VisitFunc>
  static Status VisitStatus(const ArrayData& arr, VisitFunc&& func) {
    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          ARROW_RETURN_NOT_OK(func(util::optional<bool>()));
        } else {
          ARROW_RETURN_NOT_OK(func(util::optional<bool>(value_reader.IsSet())));
        }
        valid_reader.Next();
        value_reader.Next();
      }
    } else {
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        ARROW_RETURN_NOT_OK(func(util::optional<bool>(value_reader.IsSet())));
        value_reader.Next();
      }
    }
    return Status::OK();
  }

  template <typename VisitFunc>
  static void VisitVoid(const ArrayData& arr, VisitFunc&& func) {
    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        if (is_null) {
          func(util::optional<bool>());
        } else {
          func(util::optional<bool>(value_reader.IsSet()));
        }
        valid_reader.Next();
        value_reader.Next();
      }
    } else {
      internal::BitmapReader value_reader(arr.buffers[1]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        func(util::optional<bool>(value_reader.IsSet()));
        value_reader.Next();
      }
    }
  }
};

// Binary, String...
template <typename T>
struct ArrayDataInlineVisitor<T, enable_if_base_binary<T>> {
  using c_type = util::string_view;

  template <typename VisitFunc>
  static Status VisitStatus(const ArrayData& arr, VisitFunc&& func) {
    using offset_type = typename T::offset_type;
    constexpr uint8_t empty_value = 0;

    const offset_type* offsets = arr.GetValues<offset_type>(1);
    const uint8_t* data;
    if (!arr.buffers[2]) {
      data = &empty_value;
    } else {
      // Do not apply the array offset to the values array; the value_offsets
      // index the non-sliced values array.
      data = arr.GetValues<uint8_t>(2, /*absolute_offset=*/0);
    }

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          ARROW_RETURN_NOT_OK(func(util::optional<util::string_view>()));
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                         offsets[i + 1] - offsets[i]);
          ARROW_RETURN_NOT_OK(func(util::optional<util::string_view>(value)));
        }
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                       offsets[i + 1] - offsets[i]);
        ARROW_RETURN_NOT_OK(func(util::optional<util::string_view>(value)));
      }
    }
    return Status::OK();
  }

  template <typename VisitFunc>
  static void VisitVoid(const ArrayData& arr, VisitFunc&& func) {
    using offset_type = typename T::offset_type;
    constexpr uint8_t empty_value = 0;

    const offset_type* offsets = arr.GetValues<offset_type>(1);
    const uint8_t* data;
    if (!arr.buffers[2]) {
      data = &empty_value;
    } else {
      // Do not apply the array offset to the values array; the value_offsets
      // index the non-sliced values array.
      data = arr.GetValues<uint8_t>(2, /*absolute_offset=*/0);
    }

    if (arr.null_count != 0) {
      internal::BitmapReader valid_reader(arr.buffers[0]->data(), arr.offset, arr.length);
      for (int64_t i = 0; i < arr.length; ++i) {
        const bool is_null = valid_reader.IsNotSet();
        valid_reader.Next();
        if (is_null) {
          func(util::optional<util::string_view>());
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                         offsets[i + 1] - offsets[i]);
          func(util::optional<util::string_view>(value));
        }
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data + offsets[i]),
                                       offsets[i + 1] - offsets[i]);
        func(util::optional<util::string_view>(value));
      }
    }
  }
};

// FixedSizeBinary, Decimal128
template <typename T>
struct ArrayDataInlineVisitor<T, enable_if_fixed_size_binary<T>> {
  using c_type = util::string_view;

  template <typename VisitFunc>
  static Status VisitStatus(const ArrayData& arr, VisitFunc&& func) {
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
          ARROW_RETURN_NOT_OK(func(util::optional<util::string_view>()));
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
          ARROW_RETURN_NOT_OK(func(util::optional<util::string_view>(value)));
        }
        data += byte_width;
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
        ARROW_RETURN_NOT_OK(func(util::optional<util::string_view>(value)));
        data += byte_width;
      }
    }
    return Status::OK();
  }

  template <typename VisitFunc>
  static void VisitVoid(const ArrayData& arr, VisitFunc&& func) {
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
          func(util::optional<util::string_view>());
        } else {
          auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
          func(util::optional<util::string_view>(value));
        }
        data += byte_width;
      }
    } else {
      for (int64_t i = 0; i < arr.length; ++i) {
        auto value = util::string_view(reinterpret_cast<const char*>(data), byte_width);
        func(util::optional<util::string_view>(value));
        data += byte_width;
      }
    }
  }
};

}  // namespace internal

// Visit an array's data values, in order, without overhead.
//
// The given `VisitFunc` should be a callable with either of these signatures:
// - void(util::optional<scalar_type>)
// - Status(util::optional<scalar_type>)
//
// ... where `scalar_type` depends on the array data type:
// - the type's `c_type`, if any
// - for boolean arrays, a `bool`
// - for binary, string and fixed-size binary arrays, a `util::string_view`

template <typename T, typename VisitFunc>
typename internal::call_traits::enable_if_return<VisitFunc, Status>::type
VisitArrayDataInline(const ArrayData& arr, VisitFunc&& func) {
  return internal::ArrayDataInlineVisitor<T>::VisitStatus(arr,
                                                          std::forward<VisitFunc>(func));
}

template <typename T, typename VisitFunc>
typename internal::call_traits::enable_if_return<VisitFunc, void>::type
VisitArrayDataInline(const ArrayData& arr, VisitFunc&& func) {
  return internal::ArrayDataInlineVisitor<T>::VisitVoid(arr,
                                                        std::forward<VisitFunc>(func));
}

// Visit an array's data values, in order, without overhead.
//
// The Visit method's `visitor` argument should be an object with two public methods:
// - Status VisitNull()
// - Status VisitValue(<scalar>)
//
// The scalar value's type depends on the array data type:
// - the type's `c_type`, if any
// - for boolean arrays, a `bool`
// - for binary, string and fixed-size binary arrays, a `util::string_view`

template <typename T>
struct ArrayDataVisitor {
  using InlineVisitorType = internal::ArrayDataInlineVisitor<T>;
  using c_type = typename InlineVisitorType::c_type;

  template <typename Visitor>
  static Status Visit(const ArrayData& arr, Visitor* visitor) {
    auto func = [visitor](util::optional<c_type> v) {
      if (v.has_value()) {
        return visitor->VisitValue(*v);
      } else {
        return visitor->VisitNull();
      }
    };
    return InlineVisitorType::VisitStatus(arr, std::move(func));
  }
};

#define SCALAR_VISIT_INLINE(TYPE_CLASS) \
  case TYPE_CLASS##Type::type_id:       \
    return visitor->Visit(internal::checked_cast<const TYPE_CLASS##Scalar&>(scalar));

template <typename VISITOR>
inline Status VisitScalarInline(const Scalar& scalar, VISITOR* visitor) {
  switch (scalar.type->id()) {
    ARROW_GENERATE_FOR_ALL_TYPES(SCALAR_VISIT_INLINE);
    case Type::INTERVAL: {
      const auto& interval_type =
          internal::checked_cast<const IntervalType&>(*scalar.type);
      if (interval_type.interval_type() == IntervalType::MONTHS) {
        return visitor->Visit(internal::checked_cast<const MonthIntervalScalar&>(scalar));
      }
      if (interval_type.interval_type() == IntervalType::DAY_TIME) {
        return visitor->Visit(
            internal::checked_cast<const DayTimeIntervalScalar&>(scalar));
      }
    }
    default:
      break;
  }
  return Status::NotImplemented("Scalar visitor for type not implemented ",
                                scalar.type->ToString());
}

#undef TYPE_VISIT_INLINE

// Visit a null bitmap, in order, without overhead.
//
// The given `VisitFunc` should be a callable with either of these signatures:
// - void(bool is_valid)
// - Status(bool is_valid)

template <typename VisitFunc>
typename internal::call_traits::enable_if_return<VisitFunc, Status>::type
VisitNullBitmapInline(const uint8_t* valid_bits, int64_t valid_bits_offset,
                      int64_t num_values, int64_t null_count, VisitFunc&& func) {
  if (null_count != 0) {
    internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
    for (int i = 0; i < num_values; ++i) {
      RETURN_NOT_OK(func(bit_reader.IsSet()));
      bit_reader.Next();
    }
  } else {
    for (int i = 0; i < num_values; ++i) {
      RETURN_NOT_OK(func(true));
    }
  }
  return Status::OK();
}

template <typename VisitFunc>
typename internal::call_traits::enable_if_return<VisitFunc, void>::type
VisitNullBitmapInline(const uint8_t* valid_bits, int64_t valid_bits_offset,
                      int64_t num_values, int64_t null_count, VisitFunc&& func) {
  if (null_count != 0) {
    internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
    for (int64_t i = 0; i < num_values; ++i) {
      func(bit_reader.IsSet());
      bit_reader.Next();
    }
  } else {
    for (int64_t i = 0; i < num_values; ++i) {
      func(true);
    }
  }
}

}  // namespace arrow
