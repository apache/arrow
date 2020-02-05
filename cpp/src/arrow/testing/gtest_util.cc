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
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <locale>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/kernel.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {

static void PrintChunkedArray(const ChunkedArray& carr, std::stringstream* ss) {
  for (int i = 0; i < carr.num_chunks(); ++i) {
    auto c1 = carr.chunk(i);
    *ss << "Chunk " << i << std::endl;
    ::arrow::PrettyPrintOptions options(/*indent=*/2);
    ARROW_EXPECT_OK(::arrow::PrettyPrint(*c1, options, ss));
    *ss << std::endl;
  }
}

template <typename T>
void AssertTsEqual(const T& expected, const T& actual) {
  if (!expected.Equals(actual)) {
    std::stringstream pp_expected;
    std::stringstream pp_actual;
    ::arrow::PrettyPrintOptions options(/*indent=*/2);
    options.window = 50;
    ARROW_EXPECT_OK(PrettyPrint(expected, options, &pp_expected));
    ARROW_EXPECT_OK(PrettyPrint(actual, options, &pp_actual));
    FAIL() << "Got: \n" << pp_actual.str() << "\nExpected: \n" << pp_expected.str();
  }
}

void AssertArraysEqual(const Array& expected, const Array& actual, bool verbose) {
  std::stringstream diff;
  if (!expected.Equals(actual, EqualOptions().diff_sink(&diff))) {
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

void AssertBatchesEqual(const RecordBatch& expected, const RecordBatch& actual) {
  AssertTsEqual(expected, actual);
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
  // All types tested in this file should implement fingerprinting
  ASSERT_NE(lfp, "") << "fingerprint for '" << left.ToString() << "' should not be empty";
  ASSERT_NE(rfp, "") << "fingerprint for '" << right.ToString()
                     << "' should not be empty";
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
  // All types tested in this file should implement fingerprinting
  ASSERT_NE(lfp, "") << "fingerprint for '" << left.ToString() << "' should not be empty";
  ASSERT_NE(rfp, "") << "fingerprint for '" << right.ToString()
                     << "' should not be empty";
  if (check_metadata) {
    lfp += left.metadata_fingerprint();
    rfp += right.metadata_fingerprint();
  }
  ASSERT_NE(lfp, rfp) << "Fingerprints for " << types_plural << " '" << left.ToString()
                      << "' and '" << right.ToString()
                      << "' should have compared unequal";
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

void AssertDatumsEqual(const Datum& expected, const Datum& actual) {
  // TODO: Implements better print.
  ASSERT_TRUE(actual.Equals(expected));
}

std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                     util::string_view json) {
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(ipc::internal::json::ArrayFromJSON(type, json, &out));
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
  // Parses as a StructArray
  auto struct_type = struct_(schema->fields());
  std::shared_ptr<Array> struct_array = ArrayFromJSON(struct_type, json);

  // Converts StructArray to RecordBatch
  std::shared_ptr<RecordBatch> record_batch;
  ABORT_NOT_OK(RecordBatch::FromStructArray(struct_array, &record_batch));

  return record_batch;
}

std::shared_ptr<Table> TableFromJSON(const std::shared_ptr<Schema>& schema,
                                     const std::vector<std::string>& json) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  for (const std::string& batch_json : json) {
    batches.push_back(RecordBatchFromJSON(schema, batch_json));
  }
  std::shared_ptr<Table> table;
  ABORT_NOT_OK(Table::FromRecordBatches(schema, batches, &table));

  return table;
}

void AssertTablesEqual(const Table& expected, const Table& actual, bool same_chunk_layout,
                       bool combine_chunks) {
  ASSERT_EQ(expected.num_columns(), actual.num_columns());

  if (combine_chunks) {
    auto pool = default_memory_pool();
    std::shared_ptr<Table> new_expected, new_actual;
    ASSERT_OK(expected.CombineChunks(pool, &new_expected));
    ASSERT_OK(actual.CombineChunks(pool, &new_actual));

    AssertTablesEqual(*new_expected, *new_actual, false, false);
    return;
  }

  if (same_chunk_layout) {
    for (int i = 0; i < actual.num_columns(); ++i) {
      AssertChunkedEqual(*expected.column(i), *actual.column(i));
    }
  } else {
    std::stringstream ss;
    if (!actual.Equals(expected)) {
      for (int i = 0; i < expected.num_columns(); ++i) {
        ss << "Actual column " << i << std::endl;
        PrintChunkedArray(*actual.column(i), &ss);

        ss << "Expected column " << i << std::endl;
        PrintChunkedArray(*expected.column(i), &ss);
      }
      FAIL() << ss.str();
    }
  }
}

void CompareBatch(const RecordBatch& left, const RecordBatch& right,
                  bool compare_metadata) {
  if (!left.schema()->Equals(*right.schema(), compare_metadata)) {
    FAIL() << "Left schema: " << left.schema()->ToString()
           << "\nRight schema: " << right.schema()->ToString();
  }
  ASSERT_EQ(left.num_columns(), right.num_columns())
      << left.schema()->ToString() << " result: " << right.schema()->ToString();
  ASSERT_EQ(left.num_rows(), right.num_rows());
  for (int i = 0; i < left.num_columns(); ++i) {
    if (!left.column(i)->Equals(right.column(i))) {
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

///////////////////////////////////////////////////////////////////////////
// Extension types

bool UUIDType::ExtensionEquals(const ExtensionType& other) const {
  const auto& other_ext = static_cast<const ExtensionType&>(other);
  if (other_ext.extension_name() != this->extension_name()) {
    return false;
  }
  return true;
}

std::shared_ptr<Array> UUIDType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("uuid", static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<UUIDArray>(data);
}

Status UUIDType::Deserialize(std::shared_ptr<DataType> storage_type,
                             const std::string& serialized,
                             std::shared_ptr<DataType>* out) const {
  if (serialized != "uuid-type-unique-code") {
    return Status::Invalid("Type identifier did not match");
  }
  if (!storage_type->Equals(*fixed_size_binary(16))) {
    return Status::Invalid("Invalid storage type for UUIDType");
  }
  *out = std::make_shared<UUIDType>();
  return Status::OK();
}

std::shared_ptr<DataType> uuid() { return std::make_shared<UUIDType>(); }

std::shared_ptr<Array> ExampleUUID() {
  auto storage_type = fixed_size_binary(16);
  auto ext_type = uuid();

  auto arr = ArrayFromJSON(
      storage_type,
      "[null, \"abcdefghijklmno0\", \"abcdefghijklmno1\", \"abcdefghijklmno2\"]");

  auto ext_data = arr->data()->Copy();
  ext_data->type = ext_type;
  return MakeArray(ext_data);
}

}  // namespace arrow
