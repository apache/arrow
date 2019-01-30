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

#include "arrow/test-util.h"

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
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/ipc/json-simple.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {

std::shared_ptr<Array> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                     const std::string& json) {
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(ipc::internal::json::ArrayFromJSON(type, json, &out));
  return out;
}

void random_null_bytes(int64_t n, double pct_null, uint8_t* null_bytes) {
  const int random_seed = 0;
  std::default_random_engine gen(random_seed);
  std::uniform_real_distribution<double> d(0.0, 1.0);
  std::generate(null_bytes, null_bytes + n,
                [&d, &gen, &pct_null] { return d(gen) > pct_null; });
}

void random_is_valid(int64_t n, double pct_null, std::vector<bool>* is_valid) {
  const int random_seed = 0;
  std::default_random_engine gen(random_seed);
  std::uniform_real_distribution<double> d(0.0, 1.0);
  is_valid->resize(n, false);
  std::generate(is_valid->begin(), is_valid->end(),
                [&d, &gen, &pct_null] { return d(gen) > pct_null; });
}

void random_bytes(int64_t n, uint32_t seed, uint8_t* out) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  std::generate(out, out + n, [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
}

int32_t DecimalSize(int32_t precision) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;
  DCHECK_LE(precision, 38) << "decimal precision must be less than or equal to 38, got "
                           << precision;

  switch (precision) {
    case 1:
    case 2:
      return 1;  // 127
    case 3:
    case 4:
      return 2;  // 32,767
    case 5:
    case 6:
      return 3;  // 8,388,607
    case 7:
    case 8:
    case 9:
      return 4;  // 2,147,483,427
    case 10:
    case 11:
      return 5;  // 549,755,813,887
    case 12:
    case 13:
    case 14:
      return 6;  // 140,737,488,355,327
    case 15:
    case 16:
      return 7;  // 36,028,797,018,963,967
    case 17:
    case 18:
      return 8;  // 9,223,372,036,854,775,807
    case 19:
    case 20:
    case 21:
      return 9;  // 2,361,183,241,434,822,606,847
    case 22:
    case 23:
      return 10;  // 604,462,909,807,314,587,353,087
    case 24:
    case 25:
    case 26:
      return 11;  // 154,742,504,910,672,534,362,390,527
    case 27:
    case 28:
      return 12;  // 39,614,081,257,132,168,796,771,975,167
    case 29:
    case 30:
    case 31:
      return 13;  // 10,141,204,801,825,835,211,973,625,643,007
    case 32:
    case 33:
      return 14;  // 2,596,148,429,267,413,814,265,248,164,610,047
    case 34:
    case 35:
      return 15;  // 664,613,997,892,457,936,451,903,530,140,172,287
    case 36:
    case 37:
    case 38:
      return 16;  // 170,141,183,460,469,231,731,687,303,715,884,105,727
    default:
      DCHECK(false);
      break;
  }
  return -1;
}

void random_decimals(int64_t n, uint32_t seed, int32_t precision, uint8_t* out) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  const int32_t required_bytes = DecimalSize(precision);
  constexpr int32_t byte_width = 16;
  std::fill(out, out + byte_width * n, '\0');

  for (int64_t i = 0; i < n; ++i, out += byte_width) {
    std::generate(out, out + required_bytes,
                  [&d, &gen] { return static_cast<uint8_t>(d(gen)); });

    // sign extend if the sign bit is set for the last byte generated
    // 0b10000000 == 0x80 == 128
    if ((out[required_bytes - 1] & '\x80') != 0) {
      std::fill(out + required_bytes, out + byte_width, '\xFF');
    }
  }
}

void random_ascii(int64_t n, uint32_t seed, uint8_t* out) {
  rand_uniform_int(n, seed, static_cast<int32_t>('A'), static_cast<int32_t>('z'), out);
}

int64_t CountNulls(const std::vector<uint8_t>& valid_bytes) {
  return static_cast<int64_t>(std::count(valid_bytes.cbegin(), valid_bytes.cend(), '\0'));
}

Status MakeRandomByteBuffer(int64_t length, MemoryPool* pool,
                            std::shared_ptr<ResizableBuffer>* out, uint32_t seed) {
  std::shared_ptr<ResizableBuffer> result;
  RETURN_NOT_OK(AllocateResizableBuffer(pool, length, &result));
  random_bytes(length, seed, result->mutable_data());
  *out = result;
  return Status::OK();
}

void AssertArraysEqual(const Array& expected, const Array& actual) {
  ASSERT_ARRAYS_EQUAL(expected, actual);
}

void AssertChunkedEqual(const ChunkedArray& expected, const ChunkedArray& actual) {
  ASSERT_EQ(expected.num_chunks(), actual.num_chunks()) << "# chunks unequal";
  if (!actual.Equals(expected)) {
    std::stringstream pp_result;
    std::stringstream pp_expected;

    for (int i = 0; i < actual.num_chunks(); ++i) {
      auto c1 = actual.chunk(i);
      auto c2 = expected.chunk(i);
      if (!c1->Equals(*c2)) {
        EXPECT_OK(::arrow::PrettyPrint(*c1, 0, &pp_result));
        EXPECT_OK(::arrow::PrettyPrint(*c2, 0, &pp_expected));
        FAIL() << "Chunk " << i << " Got: " << pp_result.str()
               << "\nExpected: " << pp_expected.str();
      }
    }
  }
}

void AssertChunkedEqual(const ChunkedArray& actual, const ArrayVector& expected) {
  AssertChunkedEqual(ChunkedArray(expected, actual.type()), actual);
}

void AssertBufferEqual(const Buffer& buffer, const std::vector<uint8_t>& expected) {
  ASSERT_EQ(buffer.size(), expected.size()) << "Mismatching buffer size";
  const uint8_t* buffer_data = buffer.data();
  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(buffer_data[i], expected[i]);
  }
}

void AssertBufferEqual(const Buffer& buffer, const std::string& expected) {
  ASSERT_EQ(buffer.size(), expected.length()) << "Mismatching buffer size";
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

void PrintColumn(const Column& col, std::stringstream* ss) {
  const ChunkedArray& carr = *col.data();
  for (int i = 0; i < carr.num_chunks(); ++i) {
    auto c1 = carr.chunk(i);
    *ss << "Chunk " << i << std::endl;
    EXPECT_OK(::arrow::PrettyPrint(*c1, 0, ss));
    *ss << std::endl;
  }
}

void AssertTablesEqual(const Table& expected, const Table& actual,
                       bool same_chunk_layout) {
  ASSERT_EQ(expected.num_columns(), actual.num_columns());

  if (same_chunk_layout) {
    for (int i = 0; i < actual.num_columns(); ++i) {
      AssertChunkedEqual(*expected.column(i)->data(), *actual.column(i)->data());
    }
  } else {
    std::stringstream ss;
    if (!actual.Equals(expected)) {
      for (int i = 0; i < expected.num_columns(); ++i) {
        ss << "Actual column " << i << std::endl;
        PrintColumn(*actual.column(i), &ss);

        ss << "Expected column " << i << std::endl;
        PrintColumn(*expected.column(i), &ss);
      }
      FAIL() << ss.str();
    }
  }
}

void CompareBatch(const RecordBatch& left, const RecordBatch& right) {
  if (!left.schema()->Equals(*right.schema())) {
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
