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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

template <typename T>
class Result;

}  // namespace arrow

// NOTE: failing must be inline in the macros below, to get correct file / line number
// reporting on test failures.

#define ASSERT_RAISES(ENUM, expr)                                                     \
  do {                                                                                \
    auto _res = (expr);                                                               \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);                   \
    if (!_st.Is##ENUM()) {                                                            \
      FAIL() << "Expected '" ARROW_STRINGIFY(expr) "' to fail with " ARROW_STRINGIFY( \
                    ENUM) ", but got "                                                \
             << _st.ToString();                                                       \
    }                                                                                 \
  } while (false)

#define ASSERT_RAISES_WITH_MESSAGE(ENUM, message, expr)                               \
  do {                                                                                \
    auto _res = (expr);                                                               \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);                   \
    if (!_st.Is##ENUM()) {                                                            \
      FAIL() << "Expected '" ARROW_STRINGIFY(expr) "' to fail with " ARROW_STRINGIFY( \
                    ENUM) ", but got "                                                \
             << _st.ToString();                                                       \
    }                                                                                 \
    ASSERT_EQ((message), _st.ToString());                                             \
  } while (false)

#define EXPECT_RAISES_WITH_MESSAGE_THAT(ENUM, matcher, expr)                          \
  do {                                                                                \
    auto _res = (expr);                                                               \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);                   \
    if (!_st.Is##ENUM()) {                                                            \
      FAIL() << "Expected '" ARROW_STRINGIFY(expr) "' to fail with " ARROW_STRINGIFY( \
                    ENUM) ", but got "                                                \
             << _st.ToString();                                                       \
    }                                                                                 \
    EXPECT_THAT(_st.ToString(), (matcher));                                           \
  } while (false)

#define ASSERT_OK(expr)                                                       \
  do {                                                                        \
    auto _res = (expr);                                                       \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res);           \
    if (!_st.ok()) {                                                          \
      FAIL() << "'" ARROW_STRINGIFY(expr) "' failed with " << _st.ToString(); \
    }                                                                         \
  } while (false)

#define ASSERT_OK_NO_THROW(expr) ASSERT_NO_THROW(ASSERT_OK(expr))

#define ARROW_EXPECT_OK(expr)                                       \
  do {                                                              \
    auto _res = (expr);                                             \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res); \
    EXPECT_TRUE(_st.ok());                                          \
  } while (false)

#define ABORT_NOT_OK(expr)                                          \
  do {                                                              \
    auto _res = (expr);                                             \
    ::arrow::Status _st = ::arrow::internal::GenericToStatus(_res); \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {                           \
      _st.Abort();                                                  \
    }                                                               \
  } while (false);

#define ASSIGN_OR_HANDLE_ERROR_IMPL(handle_error, status_name, lhs, rexpr) \
  auto status_name = (rexpr);                                              \
  handle_error(status_name.status());                                      \
  lhs = std::move(status_name).ValueOrDie();

#define ASSERT_OK_AND_ASSIGN(lhs, rexpr) \
  ASSIGN_OR_HANDLE_ERROR_IMPL(           \
      ASSERT_OK, ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

#define ASSIGN_OR_ABORT(lhs, rexpr)                                                     \
  ASSIGN_OR_HANDLE_ERROR_IMPL(ABORT_NOT_OK,                                             \
                              ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                              lhs, rexpr);

#define EXPECT_OK_AND_ASSIGN(lhs, rexpr)                                                \
  ASSIGN_OR_HANDLE_ERROR_IMPL(ARROW_EXPECT_OK,                                          \
                              ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), \
                              lhs, rexpr);

#define ASSERT_OK_AND_EQ(expected, expr)        \
  do {                                          \
    ASSERT_OK_AND_ASSIGN(auto _actual, (expr)); \
    ASSERT_EQ(expected, _actual);               \
  } while (0)

namespace arrow {

// ----------------------------------------------------------------------
// Useful testing::Types declarations

typedef ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type,
                         Int16Type, Int32Type, Int64Type, FloatType, DoubleType>
    NumericArrowTypes;

typedef ::testing::Types<FloatType, DoubleType> RealArrowTypes;

typedef testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                       Int32Type, Int64Type>
    IntegralArrowTypes;

class Array;
class ChunkedArray;
class RecordBatch;
class Table;

namespace compute {
struct Datum;
}

using Datum = compute::Datum;

using ArrayVector = std::vector<std::shared_ptr<Array>>;

#define ASSERT_ARRAYS_EQUAL(lhs, rhs) AssertArraysEqual((lhs), (rhs))
#define ASSERT_BATCHES_EQUAL(lhs, rhs) AssertBatchesEqual((lhs), (rhs))
#define ASSERT_TABLES_EQUAL(lhs, rhs) AssertTablesEqual((lhs), (rhs))

// If verbose is true, then the arrays will be pretty printed
ARROW_EXPORT void AssertArraysEqual(const Array& expected, const Array& actual,
                                    bool verbose = false);
ARROW_EXPORT void AssertBatchesEqual(const RecordBatch& expected,
                                     const RecordBatch& actual);
ARROW_EXPORT void AssertChunkedEqual(const ChunkedArray& expected,
                                     const ChunkedArray& actual);
ARROW_EXPORT void AssertChunkedEqual(const ChunkedArray& actual,
                                     const ArrayVector& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer,
                                    const std::vector<uint8_t>& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer, const std::string& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer, const Buffer& expected);

ARROW_EXPORT void AssertTypeEqual(const DataType& lhs, const DataType& rhs,
                                  bool check_metadata = true);
ARROW_EXPORT void AssertTypeEqual(const std::shared_ptr<DataType>& lhs,
                                  const std::shared_ptr<DataType>& rhs,
                                  bool check_metadata = true);
ARROW_EXPORT void AssertFieldEqual(const Field& lhs, const Field& rhs,
                                   bool check_metadata = true);
ARROW_EXPORT void AssertFieldEqual(const std::shared_ptr<Field>& lhs,
                                   const std::shared_ptr<Field>& rhs,
                                   bool check_metadata = true);
ARROW_EXPORT void AssertSchemaEqual(const Schema& lhs, const Schema& rhs,
                                    bool check_metadata = true);
ARROW_EXPORT void AssertSchemaEqual(const std::shared_ptr<Schema>& lhs,
                                    const std::shared_ptr<Schema>& rhs,
                                    bool check_metadata = true);

ARROW_EXPORT void AssertTypeNotEqual(const DataType& lhs, const DataType& rhs,
                                     bool check_metadata = true);
ARROW_EXPORT void AssertTypeNotEqual(const std::shared_ptr<DataType>& lhs,
                                     const std::shared_ptr<DataType>& rhs,
                                     bool check_metadata = true);
ARROW_EXPORT void AssertFieldNotEqual(const Field& lhs, const Field& rhs,
                                      bool check_metadata = true);
ARROW_EXPORT void AssertFieldNotEqual(const std::shared_ptr<Field>& lhs,
                                      const std::shared_ptr<Field>& rhs,
                                      bool check_metadata = true);
ARROW_EXPORT void AssertSchemaNotEqual(const Schema& lhs, const Schema& rhs,
                                       bool check_metadata = true);
ARROW_EXPORT void AssertSchemaNotEqual(const std::shared_ptr<Schema>& lhs,
                                       const std::shared_ptr<Schema>& rhs,
                                       bool check_metadata = true);

ARROW_EXPORT void AssertTablesEqual(const Table& expected, const Table& actual,
                                    bool same_chunk_layout = true, bool flatten = false);

ARROW_EXPORT void AssertDatumsEqual(const Datum& expected, const Datum& actual);

template <typename C_TYPE>
void AssertNumericDataEqual(const C_TYPE* raw_data,
                            const std::vector<C_TYPE>& expected_values) {
  for (auto expected : expected_values) {
    ASSERT_EQ(expected, *raw_data);
    ++raw_data;
  }
}

ARROW_EXPORT void CompareBatch(const RecordBatch& left, const RecordBatch& right,
                               bool compare_metadata = true);

// Check if the padding of the buffers of the array is zero.
// Also cause valgrind warnings if the padding bytes are uninitialized.
ARROW_EXPORT void AssertZeroPadded(const Array& array);

// Check if the valid buffer bytes are initialized
// and cause valgrind warnings otherwise.
ARROW_EXPORT void TestInitialized(const Array& array);

ARROW_EXPORT void FinishAndCheckPadding(ArrayBuilder* builder,
                                        std::shared_ptr<Array>* out);

#define DECL_T() typedef typename TestFixture::T T;

#define DECL_TYPE() typedef typename TestFixture::Type Type;

// ArrayFromJSON: construct an Array from a simple JSON representation

ARROW_EXPORT
std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>&,
                                     util::string_view json);

ARROW_EXPORT std::shared_ptr<RecordBatch> RecordBatchFromJSON(
    const std::shared_ptr<Schema>&, util::string_view);

ARROW_EXPORT
std::shared_ptr<ChunkedArray> ChunkedArrayFromJSON(const std::shared_ptr<DataType>&,
                                                   const std::vector<std::string>& json);

ARROW_EXPORT
std::shared_ptr<Table> TableFromJSON(const std::shared_ptr<Schema>&,
                                     const std::vector<std::string>& json);

ARROW_EXPORT
Status GetBitmapFromVector(const std::vector<bool>& is_valid,
                           std::shared_ptr<Buffer>* result);

ARROW_EXPORT
Status GetBitmapFromVector(const std::vector<uint8_t>& is_valid,
                           std::shared_ptr<Buffer>* result);

ARROW_EXPORT
Status GetBitmapFromVector(const std::vector<int32_t>& is_valid,
                           std::shared_ptr<Buffer>* result);

ARROW_EXPORT
Status GetBitmapFromVector(const std::vector<int64_t>& is_valid,
                           std::shared_ptr<Buffer>* result);

template <typename T>
void BitmapFromVector(const std::vector<T>& is_valid, std::shared_ptr<Buffer>* out) {
  ASSERT_OK(GetBitmapFromVector(is_valid, out));
}

template <typename T>
void AssertSortedEquals(std::vector<T> u, std::vector<T> v) {
  std::sort(u.begin(), u.end());
  std::sort(v.begin(), v.end());
  ASSERT_EQ(u, v);
}

// A RAII-style object that switches to a new locale, and switches back
// to the old locale when going out of scope.  Doesn't do anything if the
// new locale doesn't exist on the local machine.
// ATTENTION: may crash with an assertion failure on Windows debug builds.
// See ARROW-6108, also https://gerrit.libreoffice.org/#/c/54110/
class ARROW_EXPORT LocaleGuard {
 public:
  explicit LocaleGuard(const char* new_locale);
  ~LocaleGuard();

 protected:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

#ifndef ARROW_LARGE_MEMORY_TESTS
#define LARGE_MEMORY_TEST(name) DISABLED_##name
#else
#define LARGE_MEMORY_TEST(name) name
#endif

}  // namespace arrow
