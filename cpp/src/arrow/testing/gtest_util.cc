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

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/extension_type.h"

#ifndef _WIN32
#include <sys/stat.h>  // IWYU pragma: keep
#include <sys/wait.h>  // IWYU pragma: keep
#include <unistd.h>    // IWYU pragma: keep
#endif

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <locale>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/datum.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

std::vector<Type::type> AllTypeIds() {
  return {Type::NA,
          Type::BOOL,
          Type::INT8,
          Type::INT16,
          Type::INT32,
          Type::INT64,
          Type::UINT8,
          Type::UINT16,
          Type::UINT32,
          Type::UINT64,
          Type::HALF_FLOAT,
          Type::FLOAT,
          Type::DOUBLE,
          Type::DECIMAL,
          Type::DATE32,
          Type::DATE64,
          Type::TIME32,
          Type::TIME64,
          Type::TIMESTAMP,
          Type::INTERVAL_DAY_TIME,
          Type::INTERVAL_MONTHS,
          Type::DURATION,
          Type::STRING,
          Type::BINARY,
          Type::LARGE_STRING,
          Type::LARGE_BINARY,
          Type::FIXED_SIZE_BINARY,
          Type::STRUCT,
          Type::LIST,
          Type::LARGE_LIST,
          Type::FIXED_SIZE_LIST,
          Type::MAP,
          Type::DENSE_UNION,
          Type::SPARSE_UNION,
          Type::DICTIONARY,
          Type::EXTENSION};
}

template <typename T, typename CompareFunctor>
void AssertTsSame(const T& expected, const T& actual, CompareFunctor&& compare) {
  if (!compare(actual, expected)) {
    std::stringstream pp_expected;
    std::stringstream pp_actual;
    ::arrow::PrettyPrintOptions options(/*indent=*/2);
    options.window = 50;
    ARROW_EXPECT_OK(PrettyPrint(expected, options, &pp_expected));
    ARROW_EXPECT_OK(PrettyPrint(actual, options, &pp_actual));
    FAIL() << "Got: \n" << pp_actual.str() << "\nExpected: \n" << pp_expected.str();
  }
}

template <typename CompareFunctor>
void AssertArraysEqualWith(const Array& expected, const Array& actual, bool verbose,
                           CompareFunctor&& compare) {
  std::stringstream diff;
  if (!compare(expected, actual, &diff)) {
    if (expected.data()->null_count != actual.data()->null_count) {
      diff << "Null counts differ. Expected " << expected.data()->null_count
           << " but was " << actual.data()->null_count << "\n";
    }
    if (verbose) {
      ::arrow::PrettyPrintOptions options(/*indent=*/2);
      options.window = 50;
      diff << "Expected:\n";
      ARROW_EXPECT_OK(PrettyPrint(expected, options, &diff));
      diff << "\nActual:\n";
      ARROW_EXPECT_OK(PrettyPrint(actual, options, &diff));
    }
    FAIL() << diff.str();
  }
}

void AssertArraysEqual(const Array& expected, const Array& actual, bool verbose) {
  return AssertArraysEqualWith(
      expected, actual, verbose,
      [](const Array& expected, const Array& actual, std::stringstream* diff) {
        return expected.Equals(actual, EqualOptions().diff_sink(diff));
      });
}

void AssertArraysApproxEqual(const Array& expected, const Array& actual, bool verbose) {
  return AssertArraysEqualWith(
      expected, actual, verbose,
      [](const Array& expected, const Array& actual, std::stringstream* diff) {
        return expected.ApproxEquals(actual, EqualOptions().diff_sink(diff));
      });
}

void AssertScalarsEqual(const Scalar& expected, const Scalar& actual, bool verbose) {
  std::stringstream diff;
  // ARROW-8956, ScalarEquals returns false when both are null
  if (!expected.is_valid && !actual.is_valid) {
    // We consider both being null to be equal in this function
    return;
  }
  if (!expected.Equals(actual)) {
    if (verbose) {
      diff << "Expected:\n" << expected.ToString();
      diff << "\nActual:\n" << actual.ToString();
    }
    FAIL() << diff.str();
  }
}

void AssertBatchesEqual(const RecordBatch& expected, const RecordBatch& actual,
                        bool check_metadata) {
  AssertTsSame(expected, actual,
               [&](const RecordBatch& expected, const RecordBatch& actual) {
                 return expected.Equals(actual, check_metadata);
               });
}

void AssertBatchesApproxEqual(const RecordBatch& expected, const RecordBatch& actual) {
  AssertTsSame(expected, actual,
               [&](const RecordBatch& expected, const RecordBatch& actual) {
                 return expected.ApproxEquals(actual);
               });
}

void AssertChunkedEqual(const ChunkedArray& expected, const ChunkedArray& actual) {
  ASSERT_EQ(expected.num_chunks(), actual.num_chunks()) << "# chunks unequal";
  if (!actual.Equals(expected)) {
    std::stringstream diff;
    for (int i = 0; i < actual.num_chunks(); ++i) {
      auto c1 = actual.chunk(i);
      auto c2 = expected.chunk(i);
      diff << "# chunk " << i << std::endl;
      ARROW_IGNORE_EXPR(c1->Equals(c2, EqualOptions().diff_sink(&diff)));
    }
    FAIL() << diff.str();
  }
}

void AssertChunkedEqual(const ChunkedArray& actual, const ArrayVector& expected) {
  AssertChunkedEqual(ChunkedArray(expected, actual.type()), actual);
}

void AssertChunkedEquivalent(const ChunkedArray& expected, const ChunkedArray& actual) {
  // XXX: AssertChunkedEqual in gtest_util.h does not permit the chunk layouts
  // to be different
  if (!actual.Equals(expected)) {
    std::stringstream pp_expected;
    std::stringstream pp_actual;
    ::arrow::PrettyPrintOptions options(/*indent=*/2);
    options.window = 50;
    ARROW_EXPECT_OK(PrettyPrint(expected, options, &pp_expected));
    ARROW_EXPECT_OK(PrettyPrint(actual, options, &pp_actual));
    FAIL() << "Got: \n" << pp_actual.str() << "\nExpected: \n" << pp_expected.str();
  }
}

void AssertBufferEqual(const Buffer& buffer, const std::vector<uint8_t>& expected) {
  ASSERT_EQ(static_cast<size_t>(buffer.size()), expected.size())
      << "Mismatching buffer size";
  const uint8_t* buffer_data = buffer.data();
  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(buffer_data[i], expected[i]);
  }
}

void AssertBufferEqual(const Buffer& buffer, const std::string& expected) {
  ASSERT_EQ(static_cast<size_t>(buffer.size()), expected.length())
      << "Mismatching buffer size";
  const uint8_t* buffer_data = buffer.data();
  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(buffer_data[i], expected[i]);
  }
}

void AssertBufferEqual(const Buffer& buffer, const Buffer& expected) {
  ASSERT_EQ(buffer.size(), expected.size()) << "Mismatching buffer size";
  ASSERT_TRUE(buffer.Equals(expected));
}

template <typename T>
void AssertFingerprintablesEqual(const T& left, const T& right, bool check_metadata,
                                 const char* types_plural) {
  ASSERT_TRUE(left.Equals(right, check_metadata))
      << types_plural << " '" << left.ToString() << "' and '" << right.ToString()
      << "' should have compared equal";
  auto lfp = left.fingerprint();
  auto rfp = right.fingerprint();
  // Note: all types tested in this file should implement fingerprinting,
  // except extension types.
  if (check_metadata) {
    lfp += left.metadata_fingerprint();
    rfp += right.metadata_fingerprint();
  }
  ASSERT_EQ(lfp, rfp) << "Fingerprints for " << types_plural << " '" << left.ToString()
                      << "' and '" << right.ToString() << "' should have compared equal";
}

template <typename T>
void AssertFingerprintablesEqual(const std::shared_ptr<T>& left,
                                 const std::shared_ptr<T>& right, bool check_metadata,
                                 const char* types_plural) {
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  AssertFingerprintablesEqual(*left, *right, check_metadata, types_plural);
}

template <typename T>
void AssertFingerprintablesNotEqual(const T& left, const T& right, bool check_metadata,
                                    const char* types_plural) {
  ASSERT_FALSE(left.Equals(right, check_metadata))
      << types_plural << " '" << left.ToString() << "' and '" << right.ToString()
      << "' should have compared unequal";
  auto lfp = left.fingerprint();
  auto rfp = right.fingerprint();
  // Note: all types tested in this file should implement fingerprinting,
  // except extension types.
  if (lfp != "" && rfp != "") {
    if (check_metadata) {
      lfp += left.metadata_fingerprint();
      rfp += right.metadata_fingerprint();
    }
    ASSERT_NE(lfp, rfp) << "Fingerprints for " << types_plural << " '" << left.ToString()
                        << "' and '" << right.ToString()
                        << "' should have compared unequal";
  }
}

template <typename T>
void AssertFingerprintablesNotEqual(const std::shared_ptr<T>& left,
                                    const std::shared_ptr<T>& right, bool check_metadata,
                                    const char* types_plural) {
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  AssertFingerprintablesNotEqual(*left, *right, check_metadata, types_plural);
}

#define ASSERT_EQUAL_IMPL(NAME, TYPE, PLURAL)                                            \
  void Assert##NAME##Equal(const TYPE& left, const TYPE& right, bool check_metadata) {   \
    AssertFingerprintablesEqual(left, right, check_metadata, PLURAL);                    \
  }                                                                                      \
                                                                                         \
  void Assert##NAME##Equal(const std::shared_ptr<TYPE>& left,                            \
                           const std::shared_ptr<TYPE>& right, bool check_metadata) {    \
    AssertFingerprintablesEqual(left, right, check_metadata, PLURAL);                    \
  }                                                                                      \
                                                                                         \
  void Assert##NAME##NotEqual(const TYPE& left, const TYPE& right,                       \
                              bool check_metadata) {                                     \
    AssertFingerprintablesNotEqual(left, right, check_metadata, PLURAL);                 \
  }                                                                                      \
  void Assert##NAME##NotEqual(const std::shared_ptr<TYPE>& left,                         \
                              const std::shared_ptr<TYPE>& right, bool check_metadata) { \
    AssertFingerprintablesNotEqual(left, right, check_metadata, PLURAL);                 \
  }

ASSERT_EQUAL_IMPL(Type, DataType, "types")
ASSERT_EQUAL_IMPL(Field, Field, "fields")
ASSERT_EQUAL_IMPL(Schema, Schema, "schemas")
#undef ASSERT_EQUAL_IMPL

void AssertDatumsEqual(const Datum& expected, const Datum& actual, bool verbose) {
  ASSERT_EQ(expected.kind(), actual.kind())
      << "expected:" << expected.ToString() << " got:" << actual.ToString();

  switch (expected.kind()) {
    case Datum::SCALAR:
      AssertScalarsEqual(*expected.scalar(), *actual.scalar(), verbose);
      break;
    case Datum::ARRAY: {
      auto expected_array = expected.make_array();
      auto actual_array = actual.make_array();
      AssertArraysEqual(*expected_array, *actual_array, verbose);
    } break;
    case Datum::CHUNKED_ARRAY:
      AssertChunkedEquivalent(*expected.chunked_array(), *actual.chunked_array());
      break;
    default:
      // TODO: Implement better print
      ASSERT_TRUE(actual.Equals(expected));
      break;
  }
}

std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                     util::string_view json) {
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(ipc::internal::json::ArrayFromJSON(type, json, &out));
  return out;
}

std::shared_ptr<Array> DictArrayFromJSON(const std::shared_ptr<DataType>& type,
                                         util::string_view indices_json,
                                         util::string_view dictionary_json) {
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(
      ipc::internal::json::DictArrayFromJSON(type, indices_json, dictionary_json, &out));
  return out;
}

std::shared_ptr<ChunkedArray> ChunkedArrayFromJSON(const std::shared_ptr<DataType>& type,
                                                   const std::vector<std::string>& json) {
  ArrayVector out_chunks;
  for (const std::string& chunk_json : json) {
    out_chunks.push_back(ArrayFromJSON(type, chunk_json));
  }
  return std::make_shared<ChunkedArray>(std::move(out_chunks), type);
}

std::shared_ptr<RecordBatch> RecordBatchFromJSON(const std::shared_ptr<Schema>& schema,
                                                 util::string_view json) {
  // Parse as a StructArray
  auto struct_type = struct_(schema->fields());
  std::shared_ptr<Array> struct_array = ArrayFromJSON(struct_type, json);

  // Convert StructArray to RecordBatch
  return *RecordBatch::FromStructArray(struct_array);
}

std::shared_ptr<Table> TableFromJSON(const std::shared_ptr<Schema>& schema,
                                     const std::vector<std::string>& json) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  for (const std::string& batch_json : json) {
    batches.push_back(RecordBatchFromJSON(schema, batch_json));
  }
  return *Table::FromRecordBatches(schema, std::move(batches));
}

void AssertTablesEqual(const Table& expected, const Table& actual, bool same_chunk_layout,
                       bool combine_chunks) {
  ASSERT_EQ(expected.num_columns(), actual.num_columns());

  if (combine_chunks) {
    auto pool = default_memory_pool();
    ASSERT_OK_AND_ASSIGN(auto new_expected, expected.CombineChunks(pool));
    ASSERT_OK_AND_ASSIGN(auto new_actual, actual.CombineChunks(pool));

    AssertTablesEqual(*new_expected, *new_actual, false, false);
    return;
  }

  if (same_chunk_layout) {
    for (int i = 0; i < actual.num_columns(); ++i) {
      AssertChunkedEqual(*expected.column(i), *actual.column(i));
    }
  } else {
    std::stringstream ss;
    for (int i = 0; i < actual.num_columns(); ++i) {
      auto actual_col = actual.column(i);
      auto expected_col = expected.column(i);

      PrettyPrintOptions options(/*indent=*/2);
      options.window = 50;

      if (!actual_col->Equals(*expected_col)) {
        ASSERT_OK(internal::ApplyBinaryChunked(
            *actual_col, *expected_col,
            [&](const Array& left_piece, const Array& right_piece, int64_t position) {
              std::stringstream diff;
              if (!left_piece.Equals(right_piece, EqualOptions().diff_sink(&diff))) {
                ss << "Unequal at absolute position " << position << "\n" << diff.str();
                ss << "Expected:\n";
                ARROW_EXPECT_OK(PrettyPrint(right_piece, options, &ss));
                ss << "\nActual:\n";
                ARROW_EXPECT_OK(PrettyPrint(left_piece, options, &ss));
              }
              return Status::OK();
            }));
        FAIL() << ss.str();
      }
    }
  }
}

template <typename CompareFunctor>
void CompareBatchWith(const RecordBatch& left, const RecordBatch& right,
                      bool compare_metadata, CompareFunctor&& compare) {
  if (!left.schema()->Equals(*right.schema(), compare_metadata)) {
    FAIL() << "Left schema: " << left.schema()->ToString(compare_metadata)
           << "\nRight schema: " << right.schema()->ToString(compare_metadata);
  }
  ASSERT_EQ(left.num_columns(), right.num_columns())
      << left.schema()->ToString() << " result: " << right.schema()->ToString();
  ASSERT_EQ(left.num_rows(), right.num_rows());
  for (int i = 0; i < left.num_columns(); ++i) {
    if (!compare(*left.column(i), *right.column(i))) {
      std::stringstream ss;
      ss << "Idx: " << i << " Name: " << left.column_name(i);
      ss << std::endl << "Left: ";
      ASSERT_OK(PrettyPrint(*left.column(i), 0, &ss));
      ss << std::endl << "Right: ";
      ASSERT_OK(PrettyPrint(*right.column(i), 0, &ss));
      FAIL() << ss.str();
    }
  }
}

void CompareBatch(const RecordBatch& left, const RecordBatch& right,
                  bool compare_metadata) {
  return CompareBatchWith(
      left, right, compare_metadata,
      [](const Array& left, const Array& right) { return left.Equals(right); });
}

void ApproxCompareBatch(const RecordBatch& left, const RecordBatch& right,
                        bool compare_metadata) {
  return CompareBatchWith(
      left, right, compare_metadata,
      [](const Array& left, const Array& right) { return left.ApproxEquals(right); });
}

class LocaleGuard::Impl {
 public:
  explicit Impl(const char* new_locale) : global_locale_(std::locale()) {
    try {
      std::locale::global(std::locale(new_locale));
    } catch (std::runtime_error&) {
      ARROW_LOG(WARNING) << "Locale unavailable (ignored): '" << new_locale << "'";
    }
  }

  ~Impl() { std::locale::global(global_locale_); }

 protected:
  std::locale global_locale_;
};

LocaleGuard::LocaleGuard(const char* new_locale) : impl_(new Impl(new_locale)) {}

LocaleGuard::~LocaleGuard() {}

namespace {

// Used to prevent compiler optimizing away side-effect-less statements
volatile int throw_away = 0;

}  // namespace

void AssertZeroPadded(const Array& array) {
  for (const auto& buffer : array.data()->buffers) {
    if (buffer) {
      const int64_t padding = buffer->capacity() - buffer->size();
      if (padding > 0) {
        std::vector<uint8_t> zeros(padding);
        ASSERT_EQ(0, memcmp(buffer->data() + buffer->size(), zeros.data(), padding));
      }
    }
  }
}

void TestInitialized(const Array& array) {
  for (const auto& buffer : array.data()->buffers) {
    if (buffer && buffer->capacity() > 0) {
      int total = 0;
      auto data = buffer->data();
      for (int64_t i = 0; i < buffer->size(); ++i) {
        total ^= data[i];
      }
      throw_away = total;
    }
  }
}

void SleepFor(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

///////////////////////////////////////////////////////////////////////////
// Extension types

bool UuidType::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> UuidType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("uuid", static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<UuidArray>(data);
}

Result<std::shared_ptr<DataType>> UuidType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (serialized != "uuid-serialized") {
    return Status::Invalid("Type identifier did not match: '", serialized, "'");
  }
  if (!storage_type->Equals(*fixed_size_binary(16))) {
    return Status::Invalid("Invalid storage type for UuidType: ",
                           storage_type->ToString());
  }
  return std::make_shared<UuidType>();
}

bool SmallintType::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> SmallintType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("smallint", static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<SmallintArray>(data);
}

Result<std::shared_ptr<DataType>> SmallintType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (serialized != "smallint") {
    return Status::Invalid("Type identifier did not match: '", serialized, "'");
  }
  if (!storage_type->Equals(*int16())) {
    return Status::Invalid("Invalid storage type for SmallintType: ",
                           storage_type->ToString());
  }
  return std::make_shared<SmallintType>();
}

bool DictExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> DictExtensionType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK(ExtensionEquals(checked_cast<const ExtensionType&>(*data->type)));
  // No need for a specific ExtensionArray derived class
  return std::make_shared<ExtensionArray>(data);
}

Result<std::shared_ptr<DataType>> DictExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (serialized != "dict-extension-serialized") {
    return Status::Invalid("Type identifier did not match: '", serialized, "'");
  }
  if (!storage_type->Equals(*storage_type_)) {
    return Status::Invalid("Invalid storage type for DictExtensionType: ",
                           storage_type->ToString());
  }
  return std::make_shared<DictExtensionType>();
}

std::shared_ptr<DataType> uuid() { return std::make_shared<UuidType>(); }

std::shared_ptr<DataType> smallint() { return std::make_shared<SmallintType>(); }

std::shared_ptr<DataType> dict_extension_type() {
  return std::make_shared<DictExtensionType>();
}

std::shared_ptr<Array> ExampleUuid() {
  auto storage_type = fixed_size_binary(16);
  auto ext_type = uuid();

  auto arr = ArrayFromJSON(
      storage_type,
      "[null, \"abcdefghijklmno0\", \"abcdefghijklmno1\", \"abcdefghijklmno2\"]");

  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

std::shared_ptr<Array> ExampleSmallint() {
  auto storage_type = int16();
  auto ext_type = smallint();
  auto arr = ArrayFromJSON(storage_type, "[-32768, null, 1, 2, 3, 4, 32767]");
  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

ExtensionTypeGuard::ExtensionTypeGuard(const std::shared_ptr<DataType>& type) {
  ARROW_CHECK_EQ(type->id(), Type::EXTENSION);
  auto ext_type = checked_pointer_cast<ExtensionType>(type);

  ARROW_CHECK_OK(RegisterExtensionType(ext_type));
  extension_name_ = ext_type->extension_name();
  DCHECK(!extension_name_.empty());
}

ExtensionTypeGuard::~ExtensionTypeGuard() {
  if (!extension_name_.empty()) {
    ARROW_CHECK_OK(UnregisterExtensionType(extension_name_));
  }
}

}  // namespace arrow
