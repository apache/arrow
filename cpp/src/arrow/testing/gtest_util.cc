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

#ifdef _WIN32
#include <crtdbg.h>
#include <io.h>
#else
#include <fcntl.h>     // IWYU pragma: keep
#include <sys/stat.h>  // IWYU pragma: keep
#include <sys/wait.h>  // IWYU pragma: keep
#include <unistd.h>    // IWYU pragma: keep
#endif

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <locale>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/api_vector.h"
#include "arrow/datum.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/windows_compatibility.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::ThreadPool;

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

void AssertArraysEqual(const Array& expected, const Array& actual, bool verbose,
                       const EqualOptions& options) {
  return AssertArraysEqualWith(
      expected, actual, verbose,
      [&](const Array& expected, const Array& actual, std::stringstream* diff) {
        return expected.Equals(actual, options.diff_sink(diff));
      });
}

void AssertArraysApproxEqual(const Array& expected, const Array& actual, bool verbose,
                             const EqualOptions& options) {
  return AssertArraysEqualWith(
      expected, actual, verbose,
      [&](const Array& expected, const Array& actual, std::stringstream* diff) {
        return expected.ApproxEquals(actual, options.diff_sink(diff));
      });
}

void AssertScalarsEqual(const Scalar& expected, const Scalar& actual, bool verbose,
                        const EqualOptions& options) {
  if (!expected.Equals(actual, options)) {
    std::stringstream diff;
    if (verbose) {
      diff << "Expected:\n" << expected.ToString();
      diff << "\nActual:\n" << actual.ToString();
    }
    FAIL() << diff.str();
  }
}

void AssertScalarsApproxEqual(const Scalar& expected, const Scalar& actual, bool verbose,
                              const EqualOptions& options) {
  if (!expected.ApproxEquals(actual, options)) {
    std::stringstream diff;
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

void AssertChunkedApproxEquivalent(const ChunkedArray& expected,
                                   const ChunkedArray& actual,
                                   const EqualOptions& equal_options) {
  if (!actual.ApproxEquals(expected, equal_options)) {
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
std::string ToStringWithMetadata(const T& t, bool show_metadata) {
  return t.ToString(show_metadata);
}

std::string ToStringWithMetadata(const DataType& t, bool show_metadata) {
  return t.ToString();
}

template <typename T>
void AssertFingerprintablesEqual(const T& left, const T& right, bool check_metadata,
                                 const char* types_plural) {
  ASSERT_TRUE(left.Equals(right, check_metadata))
      << types_plural << " '" << ToStringWithMetadata(left, check_metadata) << "' and '"
      << ToStringWithMetadata(right, check_metadata) << "' should have compared equal";
  auto lfp = left.fingerprint();
  auto rfp = right.fingerprint();
  // Note: all types tested in this file should implement fingerprinting,
  // except extension types.
  if (check_metadata) {
    lfp += left.metadata_fingerprint();
    rfp += right.metadata_fingerprint();
  }
  ASSERT_EQ(lfp, rfp) << "Fingerprints for " << types_plural << " '"
                      << ToStringWithMetadata(left, check_metadata) << "' and '"
                      << ToStringWithMetadata(right, check_metadata)
                      << "' should have compared equal";
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
      << types_plural << " '" << ToStringWithMetadata(left, check_metadata) << "' and '"
      << ToStringWithMetadata(right, check_metadata) << "' should have compared unequal";
  auto lfp = left.fingerprint();
  auto rfp = right.fingerprint();
  // Note: all types tested in this file should implement fingerprinting,
  // except extension types.
  if (lfp != "" && rfp != "") {
    if (check_metadata) {
      lfp += left.metadata_fingerprint();
      rfp += right.metadata_fingerprint();
    }
    ASSERT_NE(lfp, rfp) << "Fingerprints for " << types_plural << " '"
                        << ToStringWithMetadata(left, check_metadata) << "' and '"
                        << ToStringWithMetadata(right, check_metadata)
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

void AssertDatumsApproxEqual(const Datum& expected, const Datum& actual, bool verbose,
                             const EqualOptions& options) {
  ASSERT_EQ(expected.kind(), actual.kind())
      << "expected:" << expected.ToString() << " got:" << actual.ToString();

  switch (expected.kind()) {
    case Datum::SCALAR:
      AssertScalarsApproxEqual(*expected.scalar(), *actual.scalar(), verbose, options);
      break;
    case Datum::ARRAY: {
      auto expected_array = expected.make_array();
      auto actual_array = actual.make_array();
      AssertArraysApproxEqual(*expected_array, *actual_array, verbose, options);
      break;
    }
    case Datum::CHUNKED_ARRAY: {
      auto expected_array = expected.chunked_array();
      auto actual_array = actual.chunked_array();
      AssertChunkedApproxEquivalent(*expected_array, *actual_array, options);
      break;
    }
    default:
      // TODO: Implement better print
      ASSERT_TRUE(actual.Equals(expected));
      break;
  }
}

std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                     std::string_view json) {
  EXPECT_OK_AND_ASSIGN(auto out, ipc::internal::json::ArrayFromJSON(type, json));
  return out;
}

std::shared_ptr<Array> DictArrayFromJSON(const std::shared_ptr<DataType>& type,
                                         std::string_view indices_json,
                                         std::string_view dictionary_json) {
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(
      ipc::internal::json::DictArrayFromJSON(type, indices_json, dictionary_json, &out));
  return out;
}

std::shared_ptr<ChunkedArray> ChunkedArrayFromJSON(const std::shared_ptr<DataType>& type,
                                                   const std::vector<std::string>& json) {
  std::shared_ptr<ChunkedArray> out;
  ABORT_NOT_OK(ipc::internal::json::ChunkedArrayFromJSON(type, json, &out));
  return out;
}

std::shared_ptr<RecordBatch> RecordBatchFromJSON(const std::shared_ptr<Schema>& schema,
                                                 std::string_view json) {
  // Parse as a StructArray
  auto struct_type = struct_(schema->fields());
  std::shared_ptr<Array> struct_array = ArrayFromJSON(struct_type, json);

  // Convert StructArray to RecordBatch
  return *RecordBatch::FromStructArray(struct_array);
}

std::shared_ptr<Scalar> ScalarFromJSON(const std::shared_ptr<DataType>& type,
                                       std::string_view json) {
  std::shared_ptr<Scalar> out;
  ABORT_NOT_OK(ipc::internal::json::ScalarFromJSON(type, json, &out));
  return out;
}

std::shared_ptr<Scalar> DictScalarFromJSON(const std::shared_ptr<DataType>& type,
                                           std::string_view index_json,
                                           std::string_view dictionary_json) {
  std::shared_ptr<Scalar> out;
  ABORT_NOT_OK(
      ipc::internal::json::DictScalarFromJSON(type, index_json, dictionary_json, &out));
  return out;
}

std::shared_ptr<Table> TableFromJSON(const std::shared_ptr<Schema>& schema,
                                     const std::vector<std::string>& json) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  for (const std::string& batch_json : json) {
    batches.push_back(RecordBatchFromJSON(schema, batch_json));
  }
  return *Table::FromRecordBatches(schema, std::move(batches));
}

Result<std::shared_ptr<Table>> RunEndEncodeTableColumns(
    const Table& table, const std::vector<int>& column_indices) {
  const int num_columns = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> encoded_columns;
  encoded_columns.reserve(num_columns);
  for (int i = 0; i < num_columns; i++) {
    if (std::find(column_indices.begin(), column_indices.end(), i) !=
        column_indices.end()) {
      ARROW_ASSIGN_OR_RAISE(auto run_end_encoded, compute::RunEndEncode(table.column(i)));
      DCHECK_EQ(run_end_encoded.kind(), Datum::CHUNKED_ARRAY);
      encoded_columns.push_back(run_end_encoded.chunked_array());
    } else {
      encoded_columns.push_back(table.column(i));
    }
  }
  return Table::Make(table.schema(), std::move(encoded_columns));
}

Result<std::optional<std::string>> PrintArrayDiff(const ChunkedArray& expected,
                                                  const ChunkedArray& actual) {
  if (actual.Equals(expected)) {
    return std::nullopt;
  }

  std::stringstream ss;
  if (expected.length() != actual.length()) {
    ss << "Expected length " << expected.length() << " but was actually "
       << actual.length();
    return ss.str();
  }

  PrettyPrintOptions options(/*indent=*/2);
  options.window = 50;
  RETURN_NOT_OK(internal::ApplyBinaryChunked(
      actual, expected,
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
  return ss.str();
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

      ASSERT_OK_AND_ASSIGN(auto diff, PrintArrayDiff(*expected_col, *actual_col));
      if (diff.has_value()) {
        FAIL() << *diff;
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

std::shared_ptr<Array> TweakValidityBit(const std::shared_ptr<Array>& array,
                                        int64_t index, bool validity) {
  auto data = array->data()->Copy();
  if (data->buffers[0] == nullptr) {
    data->buffers[0] = *AllocateBitmap(data->length);
    bit_util::SetBitsTo(data->buffers[0]->mutable_data(), 0, data->length, true);
  }
  bit_util::SetBitTo(data->buffers[0]->mutable_data(), index, validity);
  data->null_count = kUnknownNullCount;
  // Need to return a new array, because Array caches the null bitmap pointer
  return MakeArray(data);
}

// XXX create a testing/io.{h,cc}?

#if defined(_WIN32)
static void InvalidParamHandler(const wchar_t* expr, const wchar_t* func,
                                const wchar_t* source_file, unsigned int source_line,
                                uintptr_t reserved) {
  wprintf(L"Invalid parameter in function '%s'. Source: '%s' line %d expression '%s'\n",
          func, source_file, source_line, expr);
}
#endif

bool FileIsClosed(int fd) {
#if defined(_WIN32)
  // Disables default behavior on wrong params which causes the application to crash
  // https://msdn.microsoft.com/en-us/library/ksazx244.aspx
  _set_invalid_parameter_handler(InvalidParamHandler);

  // Disables possible assertion alert box on invalid input arguments
  _CrtSetReportMode(_CRT_ASSERT, 0);

  int new_fd = _dup(fd);
  if (new_fd == -1) {
    return errno == EBADF;
  }
  _close(new_fd);
  return false;
#else
  if (-1 != fcntl(fd, F_GETFD)) {
    return false;
  }
  return errno == EBADF;
#endif
}

#if !defined(_WIN32)
void AssertChildExit(int child_pid, int expected_exit_status) {
  ASSERT_GT(child_pid, 0);
  int child_status;
  int got_pid = waitpid(child_pid, &child_status, 0);
  ASSERT_EQ(got_pid, child_pid);
  if (WIFSIGNALED(child_status)) {
    FAIL() << "Child terminated by signal " << WTERMSIG(child_status);
  }
  if (!WIFEXITED(child_status)) {
    FAIL() << "Child didn't terminate normally?? Child status = " << child_status;
  }
  ASSERT_EQ(WEXITSTATUS(child_status), expected_exit_status);
}
#endif

bool LocaleExists(const char* locale) {
  try {
    std::locale loc(locale);
    return true;
  } catch (std::runtime_error&) {
    return false;
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

EnvVarGuard::EnvVarGuard(const std::string& name, const std::string& value)
    : name_(name) {
  auto maybe_value = arrow::internal::GetEnvVar(name);
  if (maybe_value.ok()) {
    was_set_ = true;
    old_value_ = *std::move(maybe_value);
  } else {
    was_set_ = false;
  }
  ARROW_CHECK_OK(arrow::internal::SetEnvVar(name, value));
}

EnvVarGuard::~EnvVarGuard() {
  if (was_set_) {
    ARROW_CHECK_OK(arrow::internal::SetEnvVar(name_, old_value_));
  } else {
    ARROW_CHECK_OK(arrow::internal::DelEnvVar(name_));
  }
}

struct SignalHandlerGuard::Impl {
  int signum_;
  internal::SignalHandler old_handler_;

  Impl(int signum, const internal::SignalHandler& handler)
      : signum_(signum), old_handler_(*internal::SetSignalHandler(signum, handler)) {}

  ~Impl() { ARROW_EXPECT_OK(internal::SetSignalHandler(signum_, old_handler_)); }
};

SignalHandlerGuard::SignalHandlerGuard(int signum, Callback cb)
    : SignalHandlerGuard(signum, internal::SignalHandler(cb)) {}

SignalHandlerGuard::SignalHandlerGuard(int signum, const internal::SignalHandler& handler)
    : impl_(new Impl{signum, handler}) {}

SignalHandlerGuard::~SignalHandlerGuard() = default;

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

void TestInitialized(const Array& array) { TestInitialized(*array.data()); }

void TestInitialized(const ArrayData& array) {
  uint8_t total = 0;
  for (const auto& buffer : array.buffers) {
    if (buffer && buffer->capacity() > 0) {
      auto data = buffer->data();
      for (int64_t i = 0; i < buffer->size(); ++i) {
        total ^= data[i];
      }
    }
  }
  uint8_t total_bit = 0;
  for (uint32_t mask = 1; mask < 256; mask <<= 1) {
    total_bit ^= (total & mask) != 0;
  }
  // This is a dummy condition on all the bits of `total` (which depend on the
  // entire buffer data).  If not all bits are well-defined, Valgrind will
  // error with "Conditional jump or move depends on uninitialised value(s)".
  if (total_bit == 0) {
    throw_away = throw_away + 1;
  }
  for (const auto& child : array.child_data) {
    TestInitialized(*child);
  }
  if (array.dictionary) {
    TestInitialized(*array.dictionary);
  }
}

void SleepFor(double seconds) {
  std::this_thread::sleep_for(
      std::chrono::nanoseconds(static_cast<int64_t>(seconds * 1e9)));
}

#ifdef _WIN32
void SleepABit() {
  LARGE_INTEGER freq, start, now;
  QueryPerformanceFrequency(&freq);
  // 1 ms
  auto desired = freq.QuadPart / 1000;
  if (desired <= 0) {
    // Fallback to STL sleep if high resolution clock not available, tests may fail,
    // shouldn't really happen
    SleepFor(1e-3);
    return;
  }
  QueryPerformanceCounter(&start);
  while (true) {
    std::this_thread::yield();
    QueryPerformanceCounter(&now);
    auto elapsed = now.QuadPart - start.QuadPart;
    if (elapsed > desired) {
      break;
    }
  }
}
#else
// std::this_thread::sleep_for should be high enough resolution on non-Windows systems
void SleepABit() { SleepFor(1e-3); }
#endif

void BusyWait(double seconds, std::function<bool()> predicate) {
  const double period = 0.001;
  for (int i = 0; !predicate() && i * period < seconds; ++i) {
    SleepFor(period);
  }
}

namespace {

// These threads will spend most of their time sleeping so there
// is no need to base this on the # of cores.  Instead it should be
// high enough to ensure good concurrency when there is concurrent hardware.
//
// Note using a thread pool prevents potentially hitting thread count limits
// in stress tests (ARROW-17927).
constexpr int kNumSleepThreads = 32;

std::shared_ptr<ThreadPool> CreateSleepThreadPool() {
  Result<std::shared_ptr<ThreadPool>> thread_pool =
      ThreadPool::MakeEternal(kNumSleepThreads);
  return thread_pool.ValueOrDie();
}

}  // namespace

Future<> SleepABitAsync() {
  static std::shared_ptr<ThreadPool> sleep_tp = CreateSleepThreadPool();
  return DeferNotOk(sleep_tp->Submit([] { SleepABit(); }));
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

bool TinyintType::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> TinyintType::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("tinyint", static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<TinyintArray>(data);
}

Result<std::shared_ptr<DataType>> TinyintType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (serialized != "tinyint") {
    return Status::Invalid("Type identifier did not match: '", serialized, "'");
  }
  if (!storage_type->Equals(*int16())) {
    return Status::Invalid("Invalid storage type for TinyintType: ",
                           storage_type->ToString());
  }
  return std::make_shared<TinyintType>();
}

bool ListExtensionType::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> ListExtensionType::MakeArray(
    std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK_EQ("list-ext", static_cast<const ExtensionType&>(*data->type).extension_name());
  return std::make_shared<ListExtensionArray>(data);
}

Result<std::shared_ptr<DataType>> ListExtensionType::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (serialized != "list-ext") {
    return Status::Invalid("Type identifier did not match: '", serialized, "'");
  }
  if (!storage_type->Equals(*list(int32()))) {
    return Status::Invalid("Invalid storage type for ListExtensionType: ",
                           storage_type->ToString());
  }
  return std::make_shared<ListExtensionType>();
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

bool Complex128Type::ExtensionEquals(const ExtensionType& other) const {
  return (other.extension_name() == this->extension_name());
}

std::shared_ptr<Array> Complex128Type::MakeArray(std::shared_ptr<ArrayData> data) const {
  DCHECK_EQ(data->type->id(), Type::EXTENSION);
  DCHECK(ExtensionEquals(checked_cast<const ExtensionType&>(*data->type)));
  return std::make_shared<Complex128Array>(data);
}

Result<std::shared_ptr<DataType>> Complex128Type::Deserialize(
    std::shared_ptr<DataType> storage_type, const std::string& serialized) const {
  if (serialized != "complex128-serialized") {
    return Status::Invalid("Type identifier did not match: '", serialized, "'");
  }
  if (!storage_type->Equals(*storage_type_)) {
    return Status::Invalid("Invalid storage type for Complex128Type: ",
                           storage_type->ToString());
  }
  return std::make_shared<Complex128Type>();
}

std::shared_ptr<DataType> uuid() { return std::make_shared<UuidType>(); }

std::shared_ptr<DataType> smallint() { return std::make_shared<SmallintType>(); }

std::shared_ptr<DataType> tinyint() { return std::make_shared<TinyintType>(); }

std::shared_ptr<DataType> list_extension_type() {
  return std::make_shared<ListExtensionType>();
}

std::shared_ptr<DataType> dict_extension_type() {
  return std::make_shared<DictExtensionType>();
}

std::shared_ptr<DataType> complex128() { return std::make_shared<Complex128Type>(); }

std::shared_ptr<Array> MakeComplex128(const std::shared_ptr<Array>& real,
                                      const std::shared_ptr<Array>& imag) {
  auto type = complex128();
  std::shared_ptr<Array> storage(
      new StructArray(checked_cast<const ExtensionType&>(*type).storage_type(),
                      real->length(), {real, imag}));
  return ExtensionType::WrapArray(type, storage);
}

std::shared_ptr<Array> ExampleUuid() {
  auto arr = ArrayFromJSON(
      fixed_size_binary(16),
      "[null, \"abcdefghijklmno0\", \"abcdefghijklmno1\", \"abcdefghijklmno2\"]");
  return ExtensionType::WrapArray(uuid(), arr);
}

std::shared_ptr<Array> ExampleSmallint() {
  auto arr = ArrayFromJSON(int16(), "[-32768, null, 1, 2, 3, 4, 32767]");
  return ExtensionType::WrapArray(smallint(), arr);
}

std::shared_ptr<Array> ExampleTinyint() {
  auto arr = ArrayFromJSON(int8(), "[-128, null, 1, 2, 3, 4, 127]");
  return ExtensionType::WrapArray(tinyint(), arr);
}

std::shared_ptr<Array> ExampleDictExtension() {
  auto arr = DictArrayFromJSON(dictionary(int8(), utf8()), "[0, 1, null, 1]",
                               R"(["foo", "bar"])");
  return ExtensionType::WrapArray(dict_extension_type(), arr);
}

std::shared_ptr<Array> ExampleComplex128() {
  auto arr = ArrayFromJSON(struct_({field("", float64()), field("", float64())}),
                           "[[1.0, -2.5], null, [3.0, -4.5]]");
  return ExtensionType::WrapArray(complex128(), arr);
}

ExtensionTypeGuard::ExtensionTypeGuard(const std::shared_ptr<DataType>& type)
    : ExtensionTypeGuard(DataTypeVector{type}) {}

ExtensionTypeGuard::ExtensionTypeGuard(const DataTypeVector& types) {
  for (const auto& type : types) {
    ARROW_CHECK_EQ(type->id(), Type::EXTENSION);
    auto ext_type = checked_pointer_cast<ExtensionType>(type);

    ARROW_CHECK_OK(RegisterExtensionType(ext_type));
    extension_names_.push_back(ext_type->extension_name());
    DCHECK(!extension_names_.back().empty());
  }
}

ExtensionTypeGuard::~ExtensionTypeGuard() {
  for (const auto& name : extension_names_) {
    ARROW_CHECK_OK(UnregisterExtensionType(name));
  }
}

class GatingTask::Impl : public std::enable_shared_from_this<GatingTask::Impl> {
 public:
  explicit Impl(double timeout_seconds)
      : timeout_seconds_(timeout_seconds), status_(), unlocked_(false) {
    unlocked_future_ = Future<>::Make();
  }

  ~Impl() {
    if (num_running_ != num_launched_) {
      ADD_FAILURE()
          << "A GatingTask instance was destroyed but some underlying tasks did not "
             "start running"
          << std::endl;
    } else if (num_finished_ != num_launched_) {
      ADD_FAILURE()
          << "A GatingTask instance was destroyed but some underlying tasks did not "
             "finish running"
          << std::endl;
    }
  }

  std::function<void()> Task() {
    num_launched_++;
    auto self = shared_from_this();
    return [self] { self->RunTask(); };
  }

  Future<> AsyncTask() {
    std::lock_guard<std::mutex> lk(mx_);
    num_launched_++;
    num_running_++;
    running_cv_.notify_all();
    /// TODO(ARROW-13004) Could maybe implement this check with future chains
    /// if we check to see if the future has been "consumed" or not
    num_finished_++;
    return unlocked_future_;
  }

  void RunTask() {
    std::unique_lock<std::mutex> lk(mx_);
    num_running_++;
    running_cv_.notify_all();
    if (!unlocked_cv_.wait_for(
            lk, std::chrono::nanoseconds(static_cast<int64_t>(timeout_seconds_ * 1e9)),
            [this] { return unlocked_; })) {
      status_ &= Status::Invalid("Timed out (" + std::to_string(timeout_seconds_) + "," +
                                 std::to_string(unlocked_) +
                                 " seconds) waiting for the gating task to be unlocked");
    }
    num_finished_++;
  }

  Status WaitForRunning(int count) {
    std::unique_lock<std::mutex> lk(mx_);
    if (running_cv_.wait_for(
            lk, std::chrono::nanoseconds(static_cast<int64_t>(timeout_seconds_ * 1e9)),
            [this, count] { return num_running_ >= count; })) {
      return Status::OK();
    }
    return Status::Invalid("Timed out waiting for tasks to launch");
  }

  Status Unlock() {
    {
      std::lock_guard<std::mutex> lk(mx_);
      unlocked_ = true;
      unlocked_cv_.notify_all();
    }
    unlocked_future_.MarkFinished();
    return status_;
  }

 private:
  double timeout_seconds_;
  Status status_;
  bool unlocked_;
  std::atomic<int> num_launched_{0};
  int num_running_ = 0;
  int num_finished_ = 0;
  std::mutex mx_;
  std::condition_variable running_cv_;
  std::condition_variable unlocked_cv_;
  Future<> unlocked_future_;
};

GatingTask::GatingTask(double timeout_seconds) : impl_(new Impl(timeout_seconds)) {}

GatingTask::~GatingTask() {}

std::function<void()> GatingTask::Task() { return impl_->Task(); }

Future<> GatingTask::AsyncTask() { return impl_->AsyncTask(); }

Status GatingTask::Unlock() { return impl_->Unlock(); }

Status GatingTask::WaitForRunning(int count) { return impl_->WaitForRunning(count); }

std::shared_ptr<GatingTask> GatingTask::Make(double timeout_seconds) {
  return std::make_shared<GatingTask>(timeout_seconds);
}

std::shared_ptr<ArrayData> UnalignBuffers(const ArrayData& array) {
  std::vector<std::shared_ptr<Buffer>> new_buffers;
  new_buffers.reserve(array.buffers.size());

  for (const auto& buffer : array.buffers) {
    if (!buffer) {
      new_buffers.emplace_back();
      continue;
    }
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<Buffer> padded,
                         AllocateBuffer(buffer->size() + 1, default_memory_pool()));
    memcpy(padded->mutable_data() + 1, buffer->data(), buffer->size());
    std::shared_ptr<Buffer> unaligned = SliceBuffer(padded, 1);
    new_buffers.push_back(std::move(unaligned));
  }

  std::shared_ptr<ArrayData> array_data = std::make_shared<ArrayData>(array);
  array_data->buffers = std::move(new_buffers);
  return array_data;
}

std::shared_ptr<Array> UnalignBuffers(const Array& array) {
  std::shared_ptr<ArrayData> array_data = UnalignBuffers(*array.data());
  return MakeArray(array_data);
}

}  // namespace arrow
