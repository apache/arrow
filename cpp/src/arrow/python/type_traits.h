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

// Internal header

#include "arrow/python/platform.h"

#include <cstdint>
#include <limits>

#include "arrow/python/numpy_interop.h"

#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {

template <int TYPE>
struct npy_traits {};

template <>
struct npy_traits<NPY_BOOL> {
  typedef uint8_t value_type;
  using TypeClass = BooleanType;
  using BuilderClass = BooleanBuilder;

  static constexpr bool supports_nulls = false;
  static inline bool isnull(uint8_t v) { return false; }
};

#define NPY_INT_DECL(TYPE, CapType, T)               \
  template <>                                        \
  struct npy_traits<NPY_##TYPE> {                    \
    typedef T value_type;                            \
    using TypeClass = CapType##Type;                 \
    using BuilderClass = CapType##Builder;           \
                                                     \
    static constexpr bool supports_nulls = false;    \
    static inline bool isnull(T v) { return false; } \
  };

NPY_INT_DECL(INT8, Int8, int8_t);
NPY_INT_DECL(INT16, Int16, int16_t);
NPY_INT_DECL(INT32, Int32, int32_t);
NPY_INT_DECL(INT64, Int64, int64_t);

NPY_INT_DECL(UINT8, UInt8, uint8_t);
NPY_INT_DECL(UINT16, UInt16, uint16_t);
NPY_INT_DECL(UINT32, UInt32, uint32_t);
NPY_INT_DECL(UINT64, UInt64, uint64_t);

#if NPY_INT64 != NPY_LONGLONG
NPY_INT_DECL(LONGLONG, Int64, int64_t);
NPY_INT_DECL(ULONGLONG, UInt64, uint64_t);
#endif

template <>
struct npy_traits<NPY_FLOAT32> {
  typedef float value_type;
  using TypeClass = FloatType;
  using BuilderClass = FloatBuilder;

  static constexpr bool supports_nulls = true;

  static inline bool isnull(float v) { return v != v; }
};

template <>
struct npy_traits<NPY_FLOAT64> {
  typedef double value_type;
  using TypeClass = DoubleType;
  using BuilderClass = DoubleBuilder;

  static constexpr bool supports_nulls = true;

  static inline bool isnull(double v) { return v != v; }
};

template <>
struct npy_traits<NPY_DATETIME> {
  typedef int64_t value_type;
  using TypeClass = TimestampType;
  using BuilderClass = TimestampBuilder;

  static constexpr bool supports_nulls = true;

  static inline bool isnull(int64_t v) {
    // NaT = -2**63
    // = -0x8000000000000000
    // = -9223372036854775808;
    // = std::numeric_limits<int64_t>::min()
    return v == std::numeric_limits<int64_t>::min();
  }
};

template <>
struct npy_traits<NPY_OBJECT> {
  typedef PyObject* value_type;
  static constexpr bool supports_nulls = true;
};

template <int TYPE>
struct arrow_traits {};

template <>
struct arrow_traits<Type::BOOL> {
  static constexpr int npy_type = NPY_BOOL;
  static constexpr bool supports_nulls = false;
};

#define INT_DECL(TYPE)                                     \
  template <>                                              \
  struct arrow_traits<Type::TYPE> {                        \
    static constexpr int npy_type = NPY_##TYPE;            \
    static constexpr bool supports_nulls = false;          \
    static constexpr double na_value = NAN;                \
    typedef typename npy_traits<NPY_##TYPE>::value_type T; \
  };

INT_DECL(INT8);
INT_DECL(INT16);
INT_DECL(INT32);
INT_DECL(INT64);
INT_DECL(UINT8);
INT_DECL(UINT16);
INT_DECL(UINT32);
INT_DECL(UINT64);

template <>
struct arrow_traits<Type::FLOAT> {
  static constexpr int npy_type = NPY_FLOAT32;
  static constexpr bool supports_nulls = true;
  static constexpr float na_value = NAN;
  typedef typename npy_traits<NPY_FLOAT32>::value_type T;
};

template <>
struct arrow_traits<Type::DOUBLE> {
  static constexpr int npy_type = NPY_FLOAT64;
  static constexpr bool supports_nulls = true;
  static constexpr double na_value = NAN;
  typedef typename npy_traits<NPY_FLOAT64>::value_type T;
};

static constexpr int64_t kPandasTimestampNull = std::numeric_limits<int64_t>::min();

constexpr int64_t kNanosecondsInDay = 86400000000000LL;

template <>
struct arrow_traits<Type::TIMESTAMP> {
  static constexpr int npy_type = NPY_DATETIME;
  static constexpr int64_t npy_shift = 1;

  static constexpr bool supports_nulls = true;
  static constexpr int64_t na_value = kPandasTimestampNull;
  typedef typename npy_traits<NPY_DATETIME>::value_type T;
};

template <>
struct arrow_traits<Type::DATE32> {
  // Data stores as FR_D day unit
  static constexpr int npy_type = NPY_DATETIME;
  static constexpr int64_t npy_shift = 1;

  static constexpr bool supports_nulls = true;
  typedef typename npy_traits<NPY_DATETIME>::value_type T;

  static constexpr int64_t na_value = kPandasTimestampNull;
  static inline bool isnull(int64_t v) { return npy_traits<NPY_DATETIME>::isnull(v); }
};

template <>
struct arrow_traits<Type::DATE64> {
  // Data stores as FR_D day unit
  static constexpr int npy_type = NPY_DATETIME;

  // There are 1000 * 60 * 60 * 24 = 86400000ms in a day
  static constexpr int64_t npy_shift = 86400000;

  static constexpr bool supports_nulls = true;
  typedef typename npy_traits<NPY_DATETIME>::value_type T;

  static constexpr int64_t na_value = kPandasTimestampNull;
  static inline bool isnull(int64_t v) { return npy_traits<NPY_DATETIME>::isnull(v); }
};

template <>
struct arrow_traits<Type::TIME32> {
  static constexpr int npy_type = NPY_OBJECT;
  static constexpr bool supports_nulls = true;
  static constexpr int64_t na_value = kPandasTimestampNull;
  typedef typename npy_traits<NPY_DATETIME>::value_type T;
};

template <>
struct arrow_traits<Type::TIME64> {
  static constexpr int npy_type = NPY_OBJECT;
  static constexpr bool supports_nulls = true;
  typedef typename npy_traits<NPY_DATETIME>::value_type T;
};

template <>
struct arrow_traits<Type::STRING> {
  static constexpr int npy_type = NPY_OBJECT;
  static constexpr bool supports_nulls = true;
};

template <>
struct arrow_traits<Type::BINARY> {
  static constexpr int npy_type = NPY_OBJECT;
  static constexpr bool supports_nulls = true;
};

static inline int numpy_type_size(int npy_type) {
  switch (npy_type) {
    case NPY_BOOL:
      return 1;
    case NPY_INT8:
      return 1;
    case NPY_INT16:
      return 2;
    case NPY_INT32:
      return 4;
    case NPY_INT64:
      return 8;
#if (NPY_INT64 != NPY_LONGLONG)
    case NPY_LONGLONG:
      return 8;
#endif
    case NPY_UINT8:
      return 1;
    case NPY_UINT16:
      return 2;
    case NPY_UINT32:
      return 4;
    case NPY_UINT64:
      return 8;
#if (NPY_UINT64 != NPY_ULONGLONG)
    case NPY_ULONGLONG:
      return 8;
#endif
    case NPY_FLOAT16:
      return 2;
    case NPY_FLOAT32:
      return 4;
    case NPY_FLOAT64:
      return 8;
    case NPY_DATETIME:
      return 8;
    case NPY_OBJECT:
      return sizeof(void*);
    default:
      DCHECK(false) << "unhandled numpy type";
      break;
  }
  return -1;
}

}  // namespace py
}  // namespace arrow
