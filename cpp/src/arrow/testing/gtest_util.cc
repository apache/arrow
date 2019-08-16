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
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/kernel.h"
#include "arrow/ipc/json-simple.h"
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

void AssertSchemaEqual(const Schema& lhs, const Schema& rhs) {
  if (!lhs.Equals(rhs)) {
    std::stringstream ss;
    ss << "left schema: " << lhs.ToString() << std::endl
       << "right schema: " << rhs.ToString() << std::endl;
    FAIL() << ss.str();
  }
}

void AssertDatumsEqual(const Datum& expected, const Datum& actual) {
  // TODO: Implements better print.
  ASSERT_TRUE(actual.Equals(expected));
}

std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                     const std::string& json) {
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(ipc::internal::json::ArrayFromJSON(type, json, &out));
  return out;
}

void AssertTablesEqual(const Table& expected, const Table& actual,
                       bool same_chunk_layout) {
  ASSERT_EQ(expected.num_columns(), actual.num_columns());

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

}  // namespace arrow
