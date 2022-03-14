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

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/exec/tpch_node.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/thread_pool.h"
#include "arrow/array/validate.h"

#include <cctype>
#include <unordered_set>
#include <string>

namespace arrow
{
    namespace compute
    {
        static constexpr uint32_t kStartDate = 8035; // January 1, 1992 is 8035 days after January 1, 1970
        static constexpr uint32_t kEndDate = 10591; // December 12, 1998 is 10591 days after January 1, 1970

        void ValidateBatch(const ExecBatch &batch)
        {
            for(const Datum &d : batch.values)
                ASSERT_OK(arrow::internal::ValidateArray(*d.array()));
        }

        void VerifyUniqueKey(
            std::unordered_set<int32_t> &seen,
            const Datum &d,
            int32_t min,
            int32_t max)
        {
            const int32_t *keys = reinterpret_cast<const int32_t *>(d.array()->buffers[1]->data());
            int64_t num_keys = d.length();
            for(int64_t i = 0; i < num_keys; i++)
            {
                ASSERT_TRUE(seen.insert(keys[i]).second);
                ASSERT_LE(keys[i], max);
                ASSERT_GE(keys[i], min);
            }
        }

        void VerifyStringAndNumber_Single(
            const char *row,
            const char *prefix,
            const int64_t i,
            const int32_t *nums,
            int byte_width,
            bool verify_padding)
        {
            int num_offset = static_cast<int>(std::strlen(prefix));
            ASSERT_EQ(std::memcmp(row, prefix, num_offset), 0) << row << ", prefix=" << prefix << ", i=" << i;
            const char *num_str = row + num_offset;
            int64_t num = 0;
            int ibyte = static_cast<int>(num_offset);
            for(; *num_str && ibyte < byte_width; ibyte++)
            {
                num *= 10;
                ASSERT_TRUE(std::isdigit(*num_str));
                num += *num_str++ - '0';
            }
            if(nums)
            {
                ASSERT_EQ(static_cast<int32_t>(num), nums[i]);
            }
            if(verify_padding)
            {
                int num_chars = ibyte - num_offset;
                ASSERT_GE(num_chars, 9);
            }
        }

        void VerifyStringAndNumber_FixedWidth(
            const Datum &strings,
            const Datum &numbers,
            int byte_width,
            const char *prefix,
            bool verify_padding = true)
        {
            int64_t length = strings.length();
            const char *str = reinterpret_cast<const char *>(
                strings.array()->buffers[1]->data());

            const int32_t *nums = nullptr;
            if(numbers.kind() != Datum::NONE)
            {
                ASSERT_EQ(length, numbers.length());
                nums = reinterpret_cast<const int32_t *>(
                    numbers.array()->buffers[1]->data());
            }

            for(int64_t i = 0; i < length; i++)
            {
                const char *row = str + i * byte_width;
                VerifyStringAndNumber_Single(row, prefix, i, nums, byte_width, verify_padding);
            }
        }

        void VerifyStringAndNumber_Varlen(
            const Datum &strings,
            const Datum &numbers,
            const char *prefix,
            bool verify_padding = true)
        {
            int64_t length = strings.length();
            const int32_t *offsets = reinterpret_cast<const int32_t *>(
                strings.array()->buffers[1]->data());
            const char *str = reinterpret_cast<const char *>(
                strings.array()->buffers[2]->data());

            const int32_t *nums = nullptr;
            if(numbers.kind() != Datum::NONE)
            {
                ASSERT_EQ(length, numbers.length());
                nums = reinterpret_cast<const int32_t *>(
                    numbers.array()->buffers[1]->data());
            }

            for(int64_t i = 0; i < length; i++)
            {
                char tmp_str[256] = {};
                int32_t start = offsets[i];
                int32_t str_len = offsets[i + 1] - offsets[i];
                std::memcpy(tmp_str, str + start, str_len);
                VerifyStringAndNumber_Single(
                    tmp_str,
                    prefix,
                    i,
                    nums,
                    sizeof(tmp_str),
                    verify_padding);
            }
        }

        void VerifyVString(const Datum &d, int min_length, int max_length)
        {
            int64_t length = d.length();
            const int32_t *off = reinterpret_cast<const int32_t *>(
                d.array()->buffers[1]->data());
            const char *str = reinterpret_cast<const char *>(
                d.array()->buffers[2]->data());
            for(int64_t i = 0; i < length; i++)
            {
                int32_t start = off[i];
                int32_t end = off[i + 1];
                int32_t str_len = end - start;
                ASSERT_LE(str_len, max_length);
                ASSERT_GE(str_len, min_length);
                for(int32_t i = start; i < end; i++)
                {
                    bool is_valid = std::isdigit(str[i]) || std::isalpha(str[i]) || str[i] == ',' || str[i] == ' ';
                    ASSERT_TRUE(is_valid) << "Character " << str[i] << " is not a digit, a letter, a comma, or a space";
                }
            }
        }

        void VerifyModuloBetween(const Datum &d, int32_t min, int32_t max, int32_t mod)
        {
            int64_t length = d.length();
            const int32_t *n = reinterpret_cast<const int32_t *>(d.array()->buffers[1]->data());
            for(int64_t i = 0; i < length; i++)
            {
                int32_t m = n[i] % mod;
                ASSERT_GE(m, min) << "Value must be between " << min << " and " << max << " mod " << mod << ", " << n[i] << " % " << mod << " = " << m;
                ASSERT_LE(m, max) << "Value must be between " << min << " and " << max << " mod " << mod << ", " << n[i] << " % " << mod << " = " << m;
            }
        }

        void VerifyAllBetween(const Datum &d, int32_t min, int32_t max)
        {
            int64_t length = d.length();
            const int32_t *n = reinterpret_cast<const int32_t *>(d.array()->buffers[1]->data());
            for(int64_t i = 0; i < length; i++)
            {
                ASSERT_GE(n[i], min) << "Value must be between " << min << " and " << max << ", got " << n[i];
                ASSERT_LE(n[i], max) << "Value must be between " << min << " and " << max << ", got " << n[i];
            }
        }

        void VerifyNationKey(const Datum &d)
        {
            VerifyAllBetween(d, 0, 24);
        }

        void VerifyPhone(const Datum &d)
        {
            int64_t length = d.length();
            const char *phones = reinterpret_cast<const char *>(d.array()->buffers[1]->data());
            constexpr int kByteWidth = 15; // This is common for all PHONE columns
            for(int64_t i = 0; i < length; i++)
            {
                const char *row = phones + i * kByteWidth;
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_EQ(*row++, '-');
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_EQ(*row++, '-');
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_EQ(*row++, '-');
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
                ASSERT_TRUE(std::isdigit(*row++));
            }
        }

        void VerifyDecimalsBetween(const Datum &d, int64_t min, int64_t max)
        {
            int64_t length = d.length();
            const Decimal128 *decs = reinterpret_cast<const Decimal128 *>(
                d.array()->buffers[1]->data());
            for(int64_t i = 0; i < length; i++)
            {
                int64_t val = static_cast<int64_t>(decs[i]);
                ASSERT_LE(val, max);
                ASSERT_GE(val, min);
            }
        }
        
        void VerifyCorrectNumberOfWords_Varlen(const Datum &d, int num_words)
        {
            int expected_num_spaces = num_words - 1;
            int64_t length = d.length();
            const int32_t *offsets = reinterpret_cast<const int32_t *>(
                d.array()->buffers[1]->data());
            const char *str = reinterpret_cast<const char *>(
                d.array()->buffers[2]->data());

            for(int64_t i = 0; i < length; i++)
            {
                int actual_num_spaces = 0;

                int32_t start = offsets[i];
                int32_t end = offsets[i + 1];
                int32_t str_len = end - start;
                char tmp_str[256] = {};
                std::memcpy(tmp_str, str + start, str_len);
                bool is_only_alphas_or_spaces = true;
                for(int32_t j = offsets[i]; j < offsets[i + 1]; j++)
                {
                    bool is_space = str[j] == ' ';
                    actual_num_spaces += is_space;
                    is_only_alphas_or_spaces &= (is_space || std::isalpha(str[j]));
                }
                ASSERT_TRUE(is_only_alphas_or_spaces) << "Words must be composed only of letters, got " << tmp_str;
                ASSERT_EQ(actual_num_spaces, expected_num_spaces) << "Wrong number of spaces in " << tmp_str;
            }
        }

        void VerifyCorrectNumberOfWords_FixedWidth(
            const Datum &d,
            int num_words,
            int byte_width)
        {
            int expected_num_spaces = num_words - 1;
            int64_t length = d.length();
            const char *str = reinterpret_cast<const char *>(
                d.array()->buffers[1]->data());

            for(int64_t i = 0; i < length; i++)
            {
                int actual_num_spaces = 0;
                const char *row = str + i * byte_width;
                bool is_only_alphas_or_spaces = true;
                for(int32_t j = 0; j < byte_width && row[j]; j++)
                {
                    bool is_space = row[j] == ' ';
                    actual_num_spaces += is_space;
                    is_only_alphas_or_spaces &= (is_space || std::isalpha(row[j]));
                }
                ASSERT_TRUE(is_only_alphas_or_spaces) << "Words must be composed only of letters, got " << row;
                ASSERT_EQ(actual_num_spaces, expected_num_spaces) << "Wrong number of spaces in " << row;
            }
        }

        void VerifyOneOf(const Datum &d, const std::unordered_set<char> &possibilities)
        {
            int64_t length = d.length();
            const char *col = reinterpret_cast<const char *>(
                d.array()->buffers[1]->data());
            for(int64_t i = 0; i < length; i++)
                ASSERT_TRUE(possibilities.find(col[i]) != possibilities.end());
        }
        
        void VerifyOneOf(
            const Datum &d,
            int32_t byte_width,
            const std::unordered_set<std::string> &possibilities)
        {
            int64_t length = d.length();
            const char *col = reinterpret_cast<const char *>(
                d.array()->buffers[1]->data());
            for(int64_t i = 0; i < length; i++)
            {
                const char *row = col + i * byte_width;
                char tmp_str[256] = {};
                std::memcpy(tmp_str, row, byte_width);
                ASSERT_TRUE(possibilities.find(tmp_str) != possibilities.end()) << tmp_str << " is not a valid string.";
            }
        }

        void CountInstances(std::unordered_map<int32_t, int32_t> &counts, const Datum &d)
        {
            int64_t length = d.length();
            const int32_t *nums = reinterpret_cast<const int32_t *>(
                d.array()->buffers[1]->data());
            for(int64_t i = 0; i < length; i++)
                counts[nums[i]]++;
        }

        void CountModifiedComments(const Datum &d, int &good_count, int &bad_count)
        {
            int64_t length = d.length();
            const int32_t *offsets = reinterpret_cast<const int32_t *>(
                d.array()->buffers[1]->data());
            const char *str = reinterpret_cast<const char *>(
                d.array()->buffers[2]->data());
            // Length of S_COMMENT is at most 100
            char tmp_string[101];
            for(int64_t i = 0; i < length; i++)
            {
                const char *row = str + offsets[i];
                int32_t row_length = offsets[i + 1] - offsets[i];
                std::memset(tmp_string, 0, sizeof(tmp_string));
                std::memcpy(tmp_string, row, row_length);
                char *customer = std::strstr(tmp_string, "Customer");
                char *recommends = std::strstr(tmp_string, "Recommends");
                char *complaints = std::strstr(tmp_string, "Complaints");
                if(customer)
                {
                    ASSERT_TRUE((recommends != nullptr) ^ (complaints != nullptr));
                    if(recommends)
                        good_count++;
                    if(complaints)
                        bad_count++;
                }
            }
        }

        TEST(TpchNode, ScaleFactor)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get(), 0.25f);
            ExecNode *table = *gen.Supplier();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            int64_t kExpectedRows = 2500;
            int64_t num_rows = 0;
            for(auto &batch : res)
                num_rows += batch.length;
            ASSERT_EQ(num_rows, kExpectedRows);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Supplier)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Supplier();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            int64_t kExpectedRows = 10000;
            int64_t num_rows = 0;

            std::unordered_set<int32_t> seen_suppkey;
            int good_count = 0;
            int bad_count = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                VerifyUniqueKey(
                    seen_suppkey,
                    batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(kExpectedRows));
                VerifyStringAndNumber_FixedWidth(batch[1], batch[0], /*byte_width=*/25, "Supplie#r");
                VerifyVString(batch[2], /*min_length=*/10, /*max_length=*/40);
                VerifyNationKey(batch[3]);
                VerifyPhone(batch[4]);
                VerifyDecimalsBetween(batch[5], -99999, 999999);
                CountModifiedComments(batch[6], good_count, bad_count);
                num_rows += batch.length;
            }
            ASSERT_EQ(seen_suppkey.size(), kExpectedRows);
            ASSERT_EQ(num_rows, kExpectedRows);
            ASSERT_EQ(good_count, 5);
            ASSERT_EQ(bad_count, 5);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Part)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Part();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            int64_t kExpectedRows = 200000;
            int64_t num_rows = 0;

            std::unordered_set<int32_t> seen_partkey;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                VerifyUniqueKey(
                    seen_partkey,
                    batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(kExpectedRows));
                VerifyCorrectNumberOfWords_Varlen(
                    batch[1],
                    /*num_words*=*/5);
                VerifyStringAndNumber_FixedWidth(
                    batch[2],
                    Datum(),
                    /*byte_width=*/25,
                    "Manufacturer#",
                    /*verify_padding=*/false);
                VerifyStringAndNumber_FixedWidth(
                    batch[3],
                    Datum(),
                    /*byte_width=*/10,
                    "Brand#",
                    /*verify_padding=*/false);
                VerifyCorrectNumberOfWords_Varlen(
                    batch[4],
                    /*num_words=*/3);
                VerifyAllBetween(batch[5], /*min=*/1, /*max=*/50);
                VerifyCorrectNumberOfWords_FixedWidth(
                    batch[6],
                    /*num_words=*/2,
                    /*byte_width=*/10);
                num_rows += batch.length;
            }
            ASSERT_EQ(seen_partkey.size(), kExpectedRows);
            ASSERT_EQ(num_rows, kExpectedRows);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, PartSupp)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.PartSupp();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            constexpr int64_t kExpectedRows = 800000;
            int64_t num_rows = 0;

            std::unordered_map<int32_t, int32_t> counts;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                CountInstances(counts, batch[0]);
                VerifyAllBetween(batch[2], 1, 9999);
                VerifyDecimalsBetween(batch[3], 100, 100000);
                num_rows += batch.length;
            }
            for(auto &partkey : counts)
                ASSERT_EQ(partkey.second, 4) << "Key " << partkey.first << " has count " << partkey.second;
            ASSERT_EQ(counts.size(), kExpectedRows / 4);

            ASSERT_EQ(num_rows, kExpectedRows);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Customer)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Customer();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            const int64_t kExpectedRows = 150000;
            int64_t num_rows = 0;

            std::unordered_set<int32_t> seen_custkey;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                VerifyUniqueKey(
                    seen_custkey,
                    batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(kExpectedRows));
                VerifyStringAndNumber_Varlen(
                    batch[1],
                    batch[0],
                    "Customer#");
                VerifyVString(batch[2], /*min=*/10, /*max=*/40);
                VerifyNationKey(batch[3]);
                VerifyPhone(batch[4]);
                VerifyDecimalsBetween(batch[5], -99999, 999999);
                VerifyCorrectNumberOfWords_FixedWidth(
                    batch[6],
                    /*num_words=*/1,
                    /*byte_width=*/10);
                num_rows += batch.length;
            }
            ASSERT_EQ(seen_custkey.size(), kExpectedRows);
            ASSERT_EQ(num_rows, kExpectedRows);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Orders)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Orders();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            constexpr int64_t kExpectedRows = 1500000;
            int64_t num_rows = 0;

            std::unordered_set<int32_t> seen_orderkey;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                VerifyUniqueKey(
                    seen_orderkey,
                    batch[0],
                    /*min=*/1,
                    /*max=*/static_cast<int32_t>(4 * kExpectedRows));
                VerifyAllBetween(batch[1], /*min=*/1, /*max=*/static_cast<int32_t>(kExpectedRows));
                VerifyModuloBetween(batch[1], /*min=*/1, /*max=*/2, /*mod=*/3);
                VerifyOneOf(batch[2], { 'F', 'O', 'P' });
                VerifyAllBetween(batch[4], kStartDate, kEndDate - 151);
                VerifyOneOf(batch[5],
                            /*byte_width=*/15,
                            {
                                "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW",
                            });
                VerifyStringAndNumber_FixedWidth(
                    batch[6],
                    Datum(),
                    /*byte_width=*/15,
                    "Clerk#",
                    /*verify_padding=*/true);
                VerifyAllBetween(batch[7], /*min=*/0, /*max=*/0);
                num_rows += batch.length;
            }
            ASSERT_EQ(seen_orderkey.size(), kExpectedRows);
            ASSERT_EQ(num_rows, kExpectedRows);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Lineitem)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Lineitem();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            std::unordered_map<int32_t, int32_t> counts;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                CountInstances(counts, batch[0]);
                VerifyAllBetween(batch[1], /*min=*/1, /*max=*/200000);
                VerifyAllBetween(batch[3], /*min=*/1, /*max=*/7);
                VerifyDecimalsBetween(batch[4], /*min=*/100, /*max=*/5000);
                VerifyDecimalsBetween(batch[6], /*min=*/0, /*max=*/10);
                VerifyDecimalsBetween(batch[7], /*min=*/0, /*max=*/8);
                VerifyOneOf(batch[8], { 'R', 'A', 'N' });
                VerifyOneOf(batch[9], { 'O', 'F' });
                VerifyAllBetween(batch[10], kStartDate + 1, kEndDate - 151 + 121);
                VerifyAllBetween(batch[11], kStartDate + 30, kEndDate - 151 + 90);
                VerifyAllBetween(batch[12], kStartDate + 2, kEndDate - 151 + 121 + 30);
                VerifyOneOf(
                    batch[13],
                    /*byte_width=*/25,
                    {
                        "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN",
                    });
                VerifyOneOf(
                    batch[14],
                    /*byte_width=*/10,
                    {
                        "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB",
                    });
            }
            for(auto &count : counts)
            {
                ASSERT_GE(count.second, 1);
                ASSERT_LE(count.second, 7);
            }
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Nation)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Nation();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            constexpr int64_t kExpectedRows = 25;
            int64_t num_rows = 0;

            std::unordered_set<int32_t> seen_nationkey;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                VerifyUniqueKey(seen_nationkey, batch[0], 0, kExpectedRows - 1);
                VerifyOneOf(
                    batch[1],
                    /*byte_width=*/25,
                    {
                        "ALGERIA", "ARGENTINA", "BRAZIL",
                        "CANADA", "EGYPT", "ETHIOPIA",
                        "FRANCE", "GERMANY", "INDIA",
                        "INDONESIA", "IRAN", "IRAQ",
                        "JAPAN", "JORDAN", "KENYA",
                        "MOROCCO", "MOZAMBIQUE", "PERU",
                        "CHINA", "ROMANIA", "SAUDI ARABIA",
                        "VIETNAM", "RUSSIA", "UNITED KINGDOM",
                        "UNITED STATES"
                    });
                VerifyAllBetween(batch[2], 0, 4);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, kExpectedRows);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }

        TEST(TpchNode, Region)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Region();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();

            constexpr int64_t kExpectedRows = 5;
            int64_t num_rows = 0;

            std::unordered_set<int32_t> seen_regionkey;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                VerifyUniqueKey(seen_regionkey, batch[0], 0, kExpectedRows - 1);
                VerifyOneOf(
                    batch[1],
                    /*byte_width=*/25,
                    {
                        "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"
                    });

                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 5);
            arrow::internal::GetCpuThreadPool()->WaitForIdle();
        }
    }
}
