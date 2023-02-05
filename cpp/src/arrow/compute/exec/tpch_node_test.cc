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

#include <gmock/gmock-matchers.h>

#include <cctype>
#include <memory>
#include <regex>
#include <string>
#include <unordered_set>

#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/tpch_node.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::StartsWith;

namespace compute {
namespace internal {

static constexpr uint32_t kStartDate =
    8035;  // January 1, 1992 is 8035 days after January 1, 1970
static constexpr uint32_t kEndDate =
    10591;  // December 12, 1998 is 10591 days after January 1, 1970

using TableNodeFn = Result<ExecNode*> (TpchGen::*)(std::vector<std::string>);

constexpr double kDefaultScaleFactor = 0.1;

Status AddTableAndSinkToPlan(ExecPlan& plan, TpchGen& gen,
                             AsyncGenerator<std::optional<ExecBatch>>& sink_gen,
                             TableNodeFn table) {
  ARROW_ASSIGN_OR_RAISE(ExecNode * table_node, ((gen.*table)({})));
  Declaration sink("sink", {Declaration::Input(table_node)}, SinkNodeOptions{&sink_gen});
  ARROW_RETURN_NOT_OK(sink.AddToPlan(&plan));
  return Status::OK();
}

Result<std::vector<ExecBatch>> GenerateTable(TableNodeFn table,
                                             double scale_factor = kDefaultScaleFactor) {
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ExecPlan> plan, ExecPlan::Make(ctx));
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<TpchGen> gen,
                        TpchGen::Make(plan.get(), scale_factor));
  AsyncGenerator<std::optional<ExecBatch>> sink_gen;
  ARROW_RETURN_NOT_OK(AddTableAndSinkToPlan(*plan, *gen, sink_gen, table));
  auto fut = StartAndCollect(plan.get(), sink_gen);
  return fut.MoveResult();
}

// Verifies that the data is valid Arrow and ensures it's not null.
void ValidateBatch(const ExecBatch& batch) {
  for (const Datum& d : batch.values) {
    ASSERT_EQ(d.kind(), Datum::ARRAY);
    const auto array = d.make_array();
    ASSERT_OK(array->ValidateFull());
    TestInitialized(*array);
    ASSERT_EQ(array->data()->buffers[0].get(), nullptr);
  }
}

// Verifies that each element is seen exactly once, and that it's between min and max
// inclusive
void VerifyUniqueKey(std::unordered_set<int32_t>* seen, const Datum& d, int32_t min,
                     int32_t max) {
  const int32_t* keys = reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  int64_t num_keys = d.length();
  for (int64_t i = 0; i < num_keys; i++) {
    ASSERT_TRUE(seen->insert(keys[i]).second);
    ASSERT_LE(keys[i], max);
    ASSERT_GE(keys[i], min);
  }
}

void VerifyStringAndNumber_Single(std::string_view row, std::string_view prefix,
                                  const int64_t i, const int32_t* nums,
                                  bool verify_padding) {
  ASSERT_TRUE(StartsWith(row, prefix)) << row << ", prefix=" << prefix << ", i=" << i;
  const char* num_str = row.data() + prefix.size();
  const char* num_str_end = row.data() + row.size();
  int64_t num = 0;
  // Parse the number out; note that it can be padded with NUL chars at the end
  for (; num_str < num_str_end && *num_str; num_str++) {
    num *= 10;
    ASSERT_TRUE(std::isdigit(*num_str)) << row << ", prefix=" << prefix << ", i=" << i;
    num += *num_str - '0';
  }
  // If nums is not null, ensure it matches the parsed number
  if (nums) {
    ASSERT_EQ(num, nums[i]);
  }
  // TPC-H requires only ever requires padding up to 9 digits, so we ensure that
  // the total length of the string was at least 9 (could be more for bigger numbers).
  if (verify_padding) {
    const auto num_chars = num_str - (row.data() + prefix.size());
    ASSERT_GE(num_chars, 9);
  }
}

// Verifies that each row is the string "prefix" followed by a number. If numbers is not
// EMPTY, it also checks that the number following the prefix is equal to the
// corresponding row in numbers. Some TPC-H data is padded to 9 zeros, which this function
// can optionally verify as well. This string function verifies fixed width columns.
void VerifyStringAndNumber_FixedWidth(const Datum& strings, const Datum& numbers,
                                      int byte_width, std::string_view prefix,
                                      bool verify_padding = true) {
  int64_t length = strings.length();
  const char* str = reinterpret_cast<const char*>(strings.array()->buffers[1]->data());

  const int32_t* nums = nullptr;
  if (numbers.kind() != Datum::NONE) {
    ASSERT_EQ(length, numbers.length());
    nums = reinterpret_cast<const int32_t*>(numbers.array()->buffers[1]->data());
  }

  for (int64_t i = 0; i < length; i++) {
    const char* row = str + i * byte_width;
    std::string_view view(row, byte_width);
    VerifyStringAndNumber_Single(view, prefix, i, nums, verify_padding);
  }
}

// Same as above but for variable length columns
void VerifyStringAndNumber_Varlen(const Datum& strings, const Datum& numbers,
                                  std::string_view prefix, bool verify_padding = true) {
  int64_t length = strings.length();
  const int32_t* offsets =
      reinterpret_cast<const int32_t*>(strings.array()->buffers[1]->data());
  const char* str = reinterpret_cast<const char*>(strings.array()->buffers[2]->data());

  const int32_t* nums = nullptr;
  if (numbers.kind() != Datum::NONE) {
    ASSERT_EQ(length, numbers.length());
    nums = reinterpret_cast<const int32_t*>(numbers.array()->buffers[1]->data());
  }

  for (int64_t i = 0; i < length; i++) {
    int32_t start = offsets[i];
    int32_t str_len = offsets[i + 1] - offsets[i];
    std::string_view view(str + start, str_len);
    VerifyStringAndNumber_Single(view, prefix, i, nums, verify_padding);
  }
}

// Verifies that each row is a V-string, which is defined in the spec to be
// a string of random length between min_length and max_length, that is composed
// of alphanumeric characters, commas, or spaces.
void VerifyVString(const Datum& d, int min_length, int max_length) {
  int64_t length = d.length();
  const int32_t* off = reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  const char* str = reinterpret_cast<const char*>(d.array()->buffers[2]->data());
  for (int64_t i = 0; i < length; i++) {
    int32_t start = off[i];
    int32_t end = off[i + 1];
    int32_t str_len = end - start;
    ASSERT_LE(str_len, max_length);
    ASSERT_GE(str_len, min_length);
    for (int32_t i = start; i < end; i++) {
      bool is_valid =
          std::isdigit(str[i]) || std::isalpha(str[i]) || str[i] == ',' || str[i] == ' ';
      ASSERT_TRUE(is_valid) << "Character " << str[i]
                            << " is not a digit, a letter, a comma, or a space";
    }
  }
}

// Verifies that each 32-bit element modulo "mod" is between min and max.
void VerifyModuloBetween(const Datum& d, int32_t min, int32_t max, int32_t mod) {
  int64_t length = d.length();
  const int32_t* n = reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  for (int64_t i = 0; i < length; i++) {
    int32_t m = n[i] % mod;
    ASSERT_GE(m, min) << "Value must be between " << min << " and " << max << " mod "
                      << mod << ", " << n[i] << " % " << mod << " = " << m;
    ASSERT_LE(m, max) << "Value must be between " << min << " and " << max << " mod "
                      << mod << ", " << n[i] << " % " << mod << " = " << m;
  }
}

// Verifies that each 32-bit element is between min and max.
void VerifyAllBetween(const Datum& d, int32_t min, int32_t max) {
  int64_t length = d.length();
  const int32_t* n = reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  for (int64_t i = 0; i < length; i++) {
    ASSERT_GE(n[i], min) << "Value must be between " << min << " and " << max << ", got "
                         << n[i];
    ASSERT_LE(n[i], max) << "Value must be between " << min << " and " << max << ", got "
                         << n[i];
  }
}

void VerifyNationKey(const Datum& d) { VerifyAllBetween(d, 0, 24); }

// Verifies that each row satisfies the phone number spec.
void VerifyPhone(const Datum& d) {
  int64_t length = d.length();
  const char* phones = reinterpret_cast<const char*>(d.array()->buffers[1]->data());
  constexpr int kByteWidth = 15;  // This is common for all PHONE columns
  std::regex exp("\\d{2}-\\d{3}-\\d{3}-\\d{4}");
  for (int64_t i = 0; i < length; i++) {
    const char* row = phones + i * kByteWidth;
    ASSERT_TRUE(std::regex_match(row, row + kByteWidth, exp));
  }
}

// Verifies that each decimal is between min and max
void VerifyDecimalsBetween(const Datum& d, int64_t min, int64_t max) {
  int64_t length = d.length();
  const Decimal128* decs =
      reinterpret_cast<const Decimal128*>(d.array()->buffers[1]->data());
  for (int64_t i = 0; i < length; i++) {
    int64_t val = static_cast<int64_t>(decs[i]);
    ASSERT_LE(val, max);
    ASSERT_GE(val, min);
  }
}

// Verifies that each variable-length row is a series of words separated by
// spaces. Number of words is determined by the number of spaces.
void VerifyCorrectNumberOfWords_Varlen(const Datum& d, int num_words) {
  int expected_num_spaces = num_words - 1;
  int64_t length = d.length();
  const int32_t* offsets =
      reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  const char* str = reinterpret_cast<const char*>(d.array()->buffers[2]->data());

  for (int64_t i = 0; i < length; i++) {
    int actual_num_spaces = 0;

    int32_t start = offsets[i];
    int32_t end = offsets[i + 1];
    int32_t str_len = end - start;
    std::string_view view(str + start, str_len);
    bool is_only_alphas_or_spaces = true;
    for (const char& c : view) {
      bool is_space = c == ' ';
      actual_num_spaces += is_space;
      is_only_alphas_or_spaces &= (is_space || std::isalpha(c));
    }
    ASSERT_TRUE(is_only_alphas_or_spaces)
        << "Words must be composed only of letters, got " << view;
    ASSERT_EQ(actual_num_spaces, expected_num_spaces)
        << "Wrong number of spaces in " << view;
  }
}

// Same as above but for fixed width columns.
void VerifyCorrectNumberOfWords_FixedWidth(const Datum& d, int num_words,
                                           int byte_width) {
  int expected_num_spaces = num_words - 1;
  int64_t length = d.length();
  const char* str = reinterpret_cast<const char*>(d.array()->buffers[1]->data());

  for (int64_t i = 0; i < length; i++) {
    int actual_num_spaces = 0;
    const char* row = str + i * byte_width;
    bool is_only_alphas_or_spaces = true;
    for (int32_t j = 0; j < byte_width && row[j]; j++) {
      bool is_space = row[j] == ' ';
      actual_num_spaces += is_space;
      is_only_alphas_or_spaces &= (is_space || std::isalpha(row[j]));
    }
    ASSERT_TRUE(is_only_alphas_or_spaces)
        << "Words must be composed only of letters, got " << row;
    ASSERT_EQ(actual_num_spaces, expected_num_spaces)
        << "Wrong number of spaces in " << row;
  }
}

// Verifies that each row of the single-byte-wide column is one of the possibilities.
void VerifyOneOf(const Datum& d, const std::unordered_set<char>& possibilities) {
  int64_t length = d.length();
  const char* col = reinterpret_cast<const char*>(d.array()->buffers[1]->data());
  for (int64_t i = 0; i < length; i++)
    ASSERT_TRUE(possibilities.find(col[i]) != possibilities.end());
}

// Verifies that each fixed-width row is one of the possibilities
void VerifyOneOf(const Datum& d, int32_t byte_width,
                 const std::unordered_set<std::string_view>& possibilities) {
  int64_t length = d.length();
  const char* col = reinterpret_cast<const char*>(d.array()->buffers[1]->data());
  for (int64_t i = 0; i < length; i++) {
    const char* row = col + i * byte_width;
    int32_t row_len = 0;
    while (row_len < byte_width && row[row_len]) row_len++;
    std::string_view view(row, row_len);
    ASSERT_TRUE(possibilities.find(view) != possibilities.end())
        << view << " is not a valid string.";
  }
}

// Counts the number of instances of each integer
void CountInstances(std::unordered_map<int32_t, int32_t>* counts, const Datum& d) {
  int64_t length = d.length();
  const int32_t* nums = reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  for (int64_t i = 0; i < length; i++) (*counts)[nums[i]]++;
}

// For the S_COMMENT column, some of the columns must be modified to contain
// "Customer...Complaints" or "Customer...Recommends". This function counts the number of
// good and bad comments.
void CountModifiedComments(const Datum& d, int* good_count, int* bad_count) {
  int64_t length = d.length();
  const int32_t* offsets =
      reinterpret_cast<const int32_t*>(d.array()->buffers[1]->data());
  const char* str = reinterpret_cast<const char*>(d.array()->buffers[2]->data());
  for (int64_t i = 0; i < length; i++) {
    const char* row = str + offsets[i];
    int32_t row_length = offsets[i + 1] - offsets[i];
    std::string_view view(row, row_length);
    bool customer = view.find("Customer") != std::string_view::npos;
    bool recommends = view.find("Recommends") != std::string_view::npos;
    bool complaints = view.find("Complaints") != std::string_view::npos;
    if (customer) {
      ASSERT_TRUE(recommends ^ complaints);
      if (recommends) *good_count += 1;
      if (complaints) *bad_count += 1;
    }
  }
}

void VerifySupplier(const std::vector<ExecBatch>& batches,
                    double scale_factor = kDefaultScaleFactor) {
  int64_t kExpectedRows = static_cast<int64_t>(10000 * scale_factor);
  int64_t num_rows = 0;

  std::unordered_set<int32_t> seen_suppkey;
  int good_count = 0;
  int bad_count = 0;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    VerifyUniqueKey(&seen_suppkey, batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(kExpectedRows));
    VerifyStringAndNumber_FixedWidth(batch[1], batch[0], /*byte_width=*/25, "Supplie#r");
    VerifyVString(batch[2], /*min_length=*/10, /*max_length=*/40);
    VerifyNationKey(batch[3]);
    VerifyPhone(batch[4]);
    VerifyDecimalsBetween(batch[5], -99999, 999999);
    CountModifiedComments(batch[6], &good_count, &bad_count);
    num_rows += batch.length;
  }
  ASSERT_EQ(seen_suppkey.size(), kExpectedRows);
  ASSERT_EQ(num_rows, kExpectedRows);
  ASSERT_EQ(good_count, static_cast<int64_t>(5 * scale_factor));
  ASSERT_EQ(bad_count, static_cast<int64_t>(5 * scale_factor));
}

TEST(TpchNode, Supplier) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Supplier));
  VerifySupplier(res);
}

void VerifyPart(const std::vector<ExecBatch>& batches,
                double scale_factor = kDefaultScaleFactor) {
  int64_t kExpectedRows = static_cast<int64_t>(200000 * scale_factor);
  int64_t num_rows = 0;

  std::unordered_set<int32_t> seen_partkey;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    VerifyUniqueKey(&seen_partkey, batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(kExpectedRows));
    VerifyCorrectNumberOfWords_Varlen(batch[1],
                                      /*num_words*=*/5);
    VerifyStringAndNumber_FixedWidth(batch[2], Datum(),
                                     /*byte_width=*/25, "Manufacturer#",
                                     /*verify_padding=*/false);
    VerifyStringAndNumber_FixedWidth(batch[3], Datum(),
                                     /*byte_width=*/10, "Brand#",
                                     /*verify_padding=*/false);
    VerifyCorrectNumberOfWords_Varlen(batch[4],
                                      /*num_words=*/3);
    VerifyAllBetween(batch[5], /*min=*/1, /*max=*/50);
    VerifyCorrectNumberOfWords_FixedWidth(batch[6],
                                          /*num_words=*/2,
                                          /*byte_width=*/10);
    num_rows += batch.length;
  }
  ASSERT_EQ(seen_partkey.size(), kExpectedRows);
  ASSERT_EQ(num_rows, kExpectedRows);
}

TEST(TpchNode, Part) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Part));
  VerifyPart(res);
}

void VerifyPartSupp(const std::vector<ExecBatch>& batches,
                    double scale_factor = kDefaultScaleFactor) {
  const int64_t kExpectedRows = static_cast<int64_t>(800000 * scale_factor);
  int64_t num_rows = 0;

  std::unordered_map<int32_t, int32_t> counts;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    CountInstances(&counts, batch[0]);
    VerifyAllBetween(batch[2], 1, 9999);
    VerifyDecimalsBetween(batch[3], 100, 100000);
    num_rows += batch.length;
  }
  for (auto& partkey : counts)
    ASSERT_EQ(partkey.second, 4)
        << "Key " << partkey.first << " has count " << partkey.second;
  ASSERT_EQ(counts.size(), kExpectedRows / 4);

  ASSERT_EQ(num_rows, kExpectedRows);
}

TEST(TpchNode, PartSupp) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::PartSupp));
  VerifyPartSupp(res);
}

void VerifyCustomer(const std::vector<ExecBatch>& batches,
                    double scale_factor = kDefaultScaleFactor) {
  const int64_t kExpectedRows = static_cast<int64_t>(150000 * scale_factor);
  int64_t num_rows = 0;

  std::unordered_set<int32_t> seen_custkey;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    VerifyUniqueKey(&seen_custkey, batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(kExpectedRows));
    VerifyStringAndNumber_Varlen(batch[1], batch[0], "Customer#");
    VerifyVString(batch[2], /*min=*/10, /*max=*/40);
    VerifyNationKey(batch[3]);
    VerifyPhone(batch[4]);
    VerifyDecimalsBetween(batch[5], -99999, 999999);
    VerifyCorrectNumberOfWords_FixedWidth(batch[6],
                                          /*num_words=*/1,
                                          /*byte_width=*/10);
    num_rows += batch.length;
  }
  ASSERT_EQ(seen_custkey.size(), kExpectedRows);
  ASSERT_EQ(num_rows, kExpectedRows);
}

TEST(TpchNode, Customer) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Customer));
  VerifyCustomer(res);
}

void VerifyOrders(const std::vector<ExecBatch>& batches,
                  double scale_factor = kDefaultScaleFactor) {
  const int64_t kExpectedRows = static_cast<int64_t>(1500000 * scale_factor);
  int64_t num_rows = 0;

  std::unordered_set<int32_t> seen_orderkey;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    VerifyUniqueKey(&seen_orderkey, batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(4 * kExpectedRows));
    VerifyAllBetween(batch[1], /*min=*/1, /*max=*/static_cast<int32_t>(kExpectedRows));
    VerifyModuloBetween(batch[1], /*min=*/1, /*max=*/2, /*mod=*/3);
    VerifyOneOf(batch[2], {'F', 'O', 'P'});
    VerifyAllBetween(batch[4], kStartDate, kEndDate - 151);
    VerifyOneOf(batch[5],
                /*byte_width=*/15,
                {
                    "1-URGENT",
                    "2-HIGH",
                    "3-MEDIUM",
                    "4-NOT SPECIFIED",
                    "5-LOW",
                });
    VerifyStringAndNumber_FixedWidth(batch[6], Datum(),
                                     /*byte_width=*/15, "Clerk#",
                                     /*verify_padding=*/true);
    VerifyAllBetween(batch[7], /*min=*/0, /*max=*/0);
    num_rows += batch.length;
  }
  ASSERT_EQ(seen_orderkey.size(), kExpectedRows);
  ASSERT_EQ(num_rows, kExpectedRows);
}

TEST(TpchNode, Orders) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Orders));
  VerifyOrders(res);
}

void VerifyLineitem(const std::vector<ExecBatch>& batches,
                    double scale_factor = kDefaultScaleFactor) {
  std::unordered_map<int32_t, int32_t> counts;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    CountInstances(&counts, batch[0]);
    VerifyAllBetween(batch[1], /*min=*/1,
                     /*max=*/static_cast<int32_t>(200000 * scale_factor));
    VerifyAllBetween(batch[3], /*min=*/1, /*max=*/7);
    VerifyDecimalsBetween(batch[4], /*min=*/100, /*max=*/5000);
    VerifyDecimalsBetween(batch[6], /*min=*/0, /*max=*/10);
    VerifyDecimalsBetween(batch[7], /*min=*/0, /*max=*/8);
    VerifyOneOf(batch[8], {'R', 'A', 'N'});
    VerifyOneOf(batch[9], {'O', 'F'});
    VerifyAllBetween(batch[10], kStartDate + 1, kEndDate - 151 + 121);
    VerifyAllBetween(batch[11], kStartDate + 30, kEndDate - 151 + 90);
    VerifyAllBetween(batch[12], kStartDate + 2, kEndDate - 151 + 121 + 30);
    VerifyOneOf(batch[13],
                /*byte_width=*/25,
                {
                    "DELIVER IN PERSON",
                    "COLLECT COD",
                    "NONE",
                    "TAKE BACK RETURN",
                });
    VerifyOneOf(batch[14],
                /*byte_width=*/10,
                {
                    "REG AIR",
                    "AIR",
                    "RAIL",
                    "SHIP",
                    "TRUCK",
                    "MAIL",
                    "FOB",
                });
  }
  for (auto& count : counts) {
    ASSERT_GE(count.second, 1);
    ASSERT_LE(count.second, 7);
  }
}

TEST(TpchNode, Lineitem) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Lineitem));
  VerifyLineitem(res);
}

void VerifyNation(const std::vector<ExecBatch>& batches,
                  double scale_factor = kDefaultScaleFactor) {
  constexpr int64_t kExpectedRows = 25;
  int64_t num_rows = 0;

  std::unordered_set<int32_t> seen_nationkey;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    VerifyUniqueKey(&seen_nationkey, batch[0], 0, kExpectedRows - 1);
    VerifyOneOf(
        batch[1],
        /*byte_width=*/25,
        {"ALGERIA",      "ARGENTINA",  "BRAZIL",  "CANADA",         "EGYPT",
         "ETHIOPIA",     "FRANCE",     "GERMANY", "INDIA",          "INDONESIA",
         "IRAN",         "IRAQ",       "JAPAN",   "JORDAN",         "KENYA",
         "MOROCCO",      "MOZAMBIQUE", "PERU",    "CHINA",          "ROMANIA",
         "SAUDI ARABIA", "VIETNAM",    "RUSSIA",  "UNITED KINGDOM", "UNITED STATES"});
    VerifyAllBetween(batch[2], 0, 4);
    num_rows += batch.length;
  }
  ASSERT_EQ(num_rows, kExpectedRows);
}

TEST(TpchNode, Nation) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Nation));
  VerifyNation(res);
}

void VerifyRegion(const std::vector<ExecBatch>& batches,
                  double scale_factor = kDefaultScaleFactor) {
  constexpr int64_t kExpectedRows = 5;
  int64_t num_rows = 0;

  std::unordered_set<int32_t> seen_regionkey;
  for (auto& batch : batches) {
    ValidateBatch(batch);
    VerifyUniqueKey(&seen_regionkey, batch[0], 0, kExpectedRows - 1);
    VerifyOneOf(batch[1],
                /*byte_width=*/25,
                {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"});

    num_rows += batch.length;
  }
  ASSERT_EQ(num_rows, 5);
}

TEST(TpchNode, Region) {
  ASSERT_OK_AND_ASSIGN(auto res, GenerateTable(&TpchGen::Region));
  VerifyRegion(res);
}

TEST(TpchNode, AllTables) {
  constexpr double kScaleFactor = 0.05;
  constexpr int kNumTables = 8;
  std::array<TableNodeFn, kNumTables> tables = {
      &TpchGen::Supplier, &TpchGen::Part,     &TpchGen::PartSupp, &TpchGen::Customer,
      &TpchGen::Orders,   &TpchGen::Lineitem, &TpchGen::Nation,   &TpchGen::Region,
  };
  using VerifyFn = void(const std::vector<ExecBatch>&, double);
  std::array<VerifyFn*, kNumTables> verify_fns = {
      &VerifySupplier, &VerifyPart,     &VerifyPartSupp, &VerifyCustomer,
      &VerifyOrders,   &VerifyLineitem, &VerifyNation,   &VerifyRegion,
  };

  std::array<AsyncGenerator<std::optional<ExecBatch>>, kNumTables> gens;
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ExecPlan> plan, ExecPlan::Make(ctx));
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<TpchGen> gen,
                       TpchGen::Make(plan.get(), kScaleFactor));
  for (int i = 0; i < kNumTables; i++) {
    ASSERT_OK(AddTableAndSinkToPlan(*plan, *gen, gens[i], tables[i]));
  }

  ASSERT_OK(plan->Validate());
  plan->StartProducing();
  ASSERT_OK(plan->finished().status());
  for (int i = 0; i < kNumTables; i++) {
    auto fut = CollectAsyncGenerator(gens[i]);
    ASSERT_OK_AND_ASSIGN(auto maybe_batches, fut.MoveResult());
    std::vector<ExecBatch> batches;
    for (auto& maybe_batch : maybe_batches) batches.emplace_back(std::move(*maybe_batch));
    verify_fns[i](batches, kScaleFactor);
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
