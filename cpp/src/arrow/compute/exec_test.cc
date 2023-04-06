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

#include <cstring>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/function.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/ordering.h"
#include "arrow/compute/registry.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace detail {

using ::arrow::internal::BitmapEquals;
using ::arrow::internal::CopyBitmap;
using ::arrow::internal::CountSetBits;

TEST(ExecBatch, SliceBasics) {
  int64_t length = 4, cut_length = 2, left_length = length - cut_length;
  ExecBatch batch{{Int32Scalar(0), ArrayFromJSON(utf8(), R"(["a", "b", "c", "d"])"),
                   ChunkedArrayFromJSON(float64(), {"[1.1]", "[2.2]", "[3.3]", "[4.4]"})},
                  length};
  std::vector<ExecBatch> expected_sliced{
      {{Int32Scalar(0), ArrayFromJSON(utf8(), R"(["a", "b"])"),
        ChunkedArrayFromJSON(float64(), {"[1.1]", "[2.2]"})},
       cut_length},
      {{Int32Scalar(0), ArrayFromJSON(utf8(), R"(["c", "d"])"),
        ChunkedArrayFromJSON(float64(), {"[3.3]", "[4.4]"})},
       left_length}};
  std::vector<ExecBatch> actual_sliced = {batch.Slice(0, cut_length),
                                          batch.Slice(cut_length, left_length)};
  for (size_t i = 0; i < expected_sliced.size(); i++) {
    ASSERT_EQ(expected_sliced[i].length, actual_sliced[i].length);
    ASSERT_EQ(expected_sliced[i].values.size(), actual_sliced[i].values.size());
    for (size_t j = 0; j < expected_sliced[i].values.size(); j++) {
      AssertDatumsEqual(expected_sliced[i].values[j], actual_sliced[i].values[j]);
    }
    ASSERT_EQ(expected_sliced[i].ToString(), actual_sliced[i].ToString());
  }
}

TEST(ExecBatch, ToRecordBatch) {
  auto i32_array = ArrayFromJSON(int32(), "[0, 1, 2]");
  auto utf8_array = ArrayFromJSON(utf8(), R"(["a", "b", "c"])");
  ExecBatch exec_batch({Datum(i32_array), Datum(utf8_array)}, 3);

  auto right_schema = schema({field("a", int32()), field("b", utf8())});
  ASSERT_OK_AND_ASSIGN(auto right_record_batch, exec_batch.ToRecordBatch(right_schema));
  ASSERT_OK(right_record_batch->ValidateFull());
  auto expected_batch = RecordBatchFromJSON(right_schema, R"([
      {"a": 0, "b": "a"},
      {"a": 1, "b": "b"},
      {"a": 2, "b": "c"}
      ])");
  AssertBatchesEqual(*right_record_batch, *expected_batch);

  // With a scalar column
  auto utf8_scalar = ScalarFromJSON(utf8(), R"("z")");
  exec_batch = ExecBatch({Datum(i32_array), Datum(utf8_scalar)}, 3);
  ASSERT_OK_AND_ASSIGN(right_record_batch, exec_batch.ToRecordBatch(right_schema));
  ASSERT_OK(right_record_batch->ValidateFull());
  expected_batch = RecordBatchFromJSON(right_schema, R"([
      {"a": 0, "b": "z"},
      {"a": 1, "b": "z"},
      {"a": 2, "b": "z"}
      ])");
  AssertBatchesEqual(*right_record_batch, *expected_batch);

  // Wrong number of fields in schema
  auto reject_schema =
      schema({field("a", int32()), field("b", utf8()), field("c", float64())});
  ASSERT_RAISES(Invalid, exec_batch.ToRecordBatch(reject_schema));

  // Wrong-kind exec batch (not really valid, but test it here anyway)
  ExecBatch miskinded_batch({Datum()}, 0);
  auto null_schema = schema({field("a", null())});
  ASSERT_RAISES(TypeError, miskinded_batch.ToRecordBatch(null_schema));
}

TEST(ExecContext, BasicWorkings) {
  {
    ExecContext ctx;
    ASSERT_EQ(GetFunctionRegistry(), ctx.func_registry());
    ASSERT_EQ(default_memory_pool(), ctx.memory_pool());
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), ctx.exec_chunksize());

    ASSERT_TRUE(ctx.use_threads());
    ASSERT_EQ(arrow::internal::CpuInfo::GetInstance(), ctx.cpu_info());
  }

  // Now, let's customize all the things
  LoggingMemoryPool my_pool(default_memory_pool());
  std::unique_ptr<FunctionRegistry> custom_reg = FunctionRegistry::Make();
  ExecContext ctx(&my_pool, /*executor=*/nullptr, custom_reg.get());

  ASSERT_EQ(custom_reg.get(), ctx.func_registry());
  ASSERT_EQ(&my_pool, ctx.memory_pool());

  ctx.set_exec_chunksize(1 << 20);
  ASSERT_EQ(1 << 20, ctx.exec_chunksize());

  ctx.set_use_threads(false);
  ASSERT_FALSE(ctx.use_threads());
}

TEST(SelectionVector, Basics) {
  auto indices = ArrayFromJSON(int32(), "[0, 3]");
  auto sel_vector = std::make_shared<SelectionVector>(*indices);

  ASSERT_EQ(indices->length(), sel_vector->length());
  ASSERT_EQ(3, sel_vector->indices()[1]);
}

void AssertValidityZeroExtraBits(const uint8_t* data, int64_t length, int64_t offset) {
  const int64_t bit_extent = ((offset + length + 7) / 8) * 8;
  for (int64_t i = offset + length; i < bit_extent; ++i) {
    EXPECT_FALSE(bit_util::GetBit(data, i)) << i;
  }
}

void AssertValidityZeroExtraBits(const ArraySpan& arr) {
  return AssertValidityZeroExtraBits(arr.buffers[0].data, arr.length, arr.offset);
}

void AssertValidityZeroExtraBits(const ArrayData& arr) {
  const Buffer& buf = *arr.buffers[0];
  return AssertValidityZeroExtraBits(buf.data(), arr.length, arr.offset);
}

class TestComputeInternals : public ::testing::Test {
 public:
  void SetUp() {
    rng_.reset(new random::RandomArrayGenerator(/*seed=*/0));
    ResetContexts();
  }

  void ResetContexts() {
    exec_ctx_.reset(new ExecContext(default_memory_pool()));
    ctx_.reset(new KernelContext(exec_ctx_.get()));
  }

  std::shared_ptr<Array> GetUInt8Array(int64_t size, double null_probability = 0.1) {
    return rng_->UInt8(size, /*min=*/0, /*max=*/100, null_probability);
  }

  std::shared_ptr<Array> GetInt32Array(int64_t size, double null_probability = 0.1) {
    return rng_->Int32(size, /*min=*/0, /*max=*/1000, null_probability);
  }

  std::shared_ptr<Array> GetFloat64Array(int64_t size, double null_probability = 0.1) {
    return rng_->Float64(size, /*min=*/0, /*max=*/1000, null_probability);
  }

  std::shared_ptr<ChunkedArray> GetInt32Chunked(const std::vector<int>& sizes) {
    std::vector<std::shared_ptr<Array>> chunks;
    for (auto size : sizes) {
      chunks.push_back(GetInt32Array(size));
    }
    return std::make_shared<ChunkedArray>(std::move(chunks));
  }

 protected:
  std::unique_ptr<ExecContext> exec_ctx_;
  std::unique_ptr<KernelContext> ctx_;
  std::unique_ptr<random::RandomArrayGenerator> rng_;
};

// ----------------------------------------------------------------------
// Test PropagateNulls

class TestPropagateNulls : public TestComputeInternals {};

TEST_F(TestPropagateNulls, UnknownNullCountWithNullsZeroCopies) {
  const int64_t length = 16;

  constexpr uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);

  ArrayData output(boolean(), length, {nullptr, nullptr});
  ArrayData input(boolean(), length, {nulls, nullptr}, kUnknownNullCount);

  ExecBatch batch({input}, length);
  ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));
  ASSERT_EQ(nulls.get(), output.buffers[0].get());
  ASSERT_EQ(kUnknownNullCount, output.null_count);
  ASSERT_EQ(9, output.GetNullCount());
}

TEST_F(TestPropagateNulls, UnknownNullCountWithoutNulls) {
  const int64_t length = 16;
  constexpr uint8_t validity_bitmap[8] = {255, 255, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);

  ArrayData output(boolean(), length, {nullptr, nullptr});
  ArrayData input(boolean(), length, {nulls, nullptr}, kUnknownNullCount);

  ExecBatch batch({input}, length);
  ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));
  EXPECT_EQ(-1, output.null_count);
  EXPECT_EQ(nulls.get(), output.buffers[0].get());
}

TEST_F(TestPropagateNulls, SetAllNulls) {
  const int64_t length = 16;

  auto CheckSetAllNull = [&](std::vector<Datum> values, bool preallocate) {
    // Make fresh bitmap with all 1's
    uint8_t bitmap_data[2] = {255, 255};
    auto preallocated_mem = std::make_shared<MutableBuffer>(bitmap_data, 2);

    std::vector<std::shared_ptr<Buffer>> buffers(2);
    if (preallocate) {
      buffers[0] = preallocated_mem;
    }

    ArrayData output(boolean(), length, buffers);

    ExecBatch batch(values, length);
    ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));

    if (preallocate) {
      // Ensure that buffer object the same when we pass in preallocated memory
      ASSERT_EQ(preallocated_mem.get(), output.buffers[0].get());
    }
    ASSERT_NE(nullptr, output.buffers[0]);
    uint8_t expected[2] = {0, 0};
    const Buffer& out_buf = *output.buffers[0];
    ASSERT_EQ(0, std::memcmp(out_buf.data(), expected, out_buf.size()));
  };

  // There is a null scalar
  std::shared_ptr<Scalar> i32_val = std::make_shared<Int32Scalar>(3);
  std::vector<Datum> vals = {i32_val, MakeNullScalar(boolean())};
  CheckSetAllNull(vals, true);
  CheckSetAllNull(vals, false);

  const double true_prob = 0.5;

  vals[0] = rng_->Boolean(length, true_prob);
  CheckSetAllNull(vals, true);
  CheckSetAllNull(vals, false);

  auto arr_all_nulls = rng_->Boolean(length, true_prob, /*null_probability=*/1);

  // One value is all null
  vals = {rng_->Boolean(length, true_prob, /*null_probability=*/0.5), arr_all_nulls};
  CheckSetAllNull(vals, true);
  CheckSetAllNull(vals, false);

  // A value is NullType
  std::shared_ptr<Array> null_arr = std::make_shared<NullArray>(length);
  vals = {rng_->Boolean(length, true_prob), null_arr};
  CheckSetAllNull(vals, true);
  CheckSetAllNull(vals, false);

  // Other nitty-gritty scenarios
  {
    // An all-null bitmap is zero-copied over, even though there is a
    // null-scalar earlier in the batch
    ArrayData output(boolean(), length, {nullptr, nullptr});
    ExecBatch batch({MakeNullScalar(boolean()), arr_all_nulls}, length);
    ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));
    ASSERT_EQ(arr_all_nulls->data()->buffers[0].get(), output.buffers[0].get());
  }
}

TEST_F(TestPropagateNulls, SingleValueWithNulls) {
  // Input offset is non-zero (0 mod 8 and nonzero mod 8 cases)
  const int64_t length = 100;
  auto arr = rng_->Boolean(length, 0.5, /*null_probability=*/0.5);

  auto CheckSliced = [&](int64_t offset, bool preallocate = false,
                         int64_t out_offset = 0) {
    // Unaligned bitmap, zero copy not possible
    auto sliced = arr->Slice(offset);
    std::vector<Datum> vals = {sliced};

    ArrayData output(boolean(), vals[0].length(), {nullptr, nullptr});
    output.offset = out_offset;

    ExecBatch batch(vals, vals[0].length());

    std::shared_ptr<Buffer> preallocated_bitmap;
    if (preallocate) {
      ASSERT_OK_AND_ASSIGN(
          preallocated_bitmap,
          AllocateBuffer(bit_util::BytesForBits(sliced->length() + out_offset)));
      std::memset(preallocated_bitmap->mutable_data(), 0, preallocated_bitmap->size());
      output.buffers[0] = preallocated_bitmap;
    } else {
      ASSERT_EQ(0, output.offset);
    }

    ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));

    if (!preallocate) {
      const Buffer* parent_buf = arr->data()->buffers[0].get();
      if (offset == 0) {
        // Validity bitmap same, no slice
        ASSERT_EQ(parent_buf, output.buffers[0].get());
      } else if (offset % 8 == 0) {
        // Validity bitmap sliced
        ASSERT_NE(parent_buf, output.buffers[0].get());
        ASSERT_EQ(parent_buf, output.buffers[0]->parent().get());
      } else {
        // New memory for offset not 0 mod 8
        ASSERT_NE(parent_buf, output.buffers[0].get());
        ASSERT_EQ(nullptr, output.buffers[0]->parent());
      }
    } else {
      // preallocated, so check that the validity bitmap is unbothered
      ASSERT_EQ(preallocated_bitmap.get(), output.buffers[0].get());
    }

    ASSERT_EQ(arr->Slice(offset)->null_count(), output.GetNullCount());

    ASSERT_TRUE(BitmapEquals(output.buffers[0]->data(), output.offset,
                             sliced->null_bitmap_data(), sliced->offset(),
                             output.length));
    AssertValidityZeroExtraBits(output);
  };

  CheckSliced(8);
  CheckSliced(7);
  CheckSliced(8, /*preallocated=*/true);
  CheckSliced(7, true);
  CheckSliced(8, true, /*offset=*/4);
  CheckSliced(7, true, 4);
}

TEST_F(TestPropagateNulls, ZeroCopyWhenZeroNullsOnOneInput) {
  const int64_t length = 16;

  constexpr uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);

  ArrayData some_nulls(boolean(), 16, {nulls, nullptr}, /*null_count=*/9);
  ArrayData no_nulls(boolean(), length, {nullptr, nullptr}, /*null_count=*/0);

  ArrayData output(boolean(), length, {nullptr, nullptr});
  ExecBatch batch({some_nulls, no_nulls}, length);
  ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));
  ASSERT_EQ(nulls.get(), output.buffers[0].get());
  ASSERT_EQ(9, output.null_count);

  // Flip order of args
  output = ArrayData(boolean(), length, {nullptr, nullptr});
  batch.values = {no_nulls, no_nulls, some_nulls};
  ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));
  ASSERT_EQ(nulls.get(), output.buffers[0].get());
  ASSERT_EQ(9, output.null_count);

  // Check that preallocated memory is not clobbered
  uint8_t bitmap_data[2] = {0, 0};
  auto preallocated_mem = std::make_shared<MutableBuffer>(bitmap_data, 2);
  output.null_count = kUnknownNullCount;
  output.buffers[0] = preallocated_mem;
  ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));

  ASSERT_EQ(preallocated_mem.get(), output.buffers[0].get());
  ASSERT_EQ(9, output.null_count);
  ASSERT_EQ(254, bitmap_data[0]);
  ASSERT_EQ(0, bitmap_data[1]);
}

TEST_F(TestPropagateNulls, IntersectsNulls) {
  const int64_t length = 16;

  // 0b01111111 0b11001111
  constexpr uint8_t bitmap1[8] = {127, 207, 0, 0, 0, 0, 0, 0};

  // 0b11111110 0b01111111
  constexpr uint8_t bitmap2[8] = {254, 127, 0, 0, 0, 0, 0, 0};

  // 0b11101111 0b11111110
  constexpr uint8_t bitmap3[8] = {239, 254, 0, 0, 0, 0, 0, 0};

  ArrayData arr1(boolean(), length, {std::make_shared<Buffer>(bitmap1, 8), nullptr});
  ArrayData arr2(boolean(), length, {std::make_shared<Buffer>(bitmap2, 8), nullptr});
  ArrayData arr3(boolean(), length, {std::make_shared<Buffer>(bitmap3, 8), nullptr});

  auto CheckCase = [&](std::vector<Datum> values, int64_t ex_null_count,
                       const uint8_t* ex_bitmap, bool preallocate = false,
                       int64_t output_offset = 0) {
    ExecBatch batch(values, length);

    std::shared_ptr<Buffer> nulls;
    if (preallocate) {
      // Make the buffer one byte bigger so we can have non-zero offsets
      ASSERT_OK_AND_ASSIGN(nulls, AllocateBuffer(3));
      std::memset(nulls->mutable_data(), 0, nulls->size());
    } else {
      // non-zero output offset not permitted unless the output memory is
      // preallocated
      ASSERT_EQ(0, output_offset);
    }
    ArrayData output(boolean(), length, {nulls, nullptr});
    output.offset = output_offset;

    ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));

    // Preallocated memory used
    if (preallocate) {
      ASSERT_EQ(nulls.get(), output.buffers[0].get());
    }

    EXPECT_EQ(kUnknownNullCount, output.null_count);
    EXPECT_EQ(ex_null_count, output.GetNullCount());

    const auto& out_buffer = *output.buffers[0];

    ASSERT_TRUE(BitmapEquals(out_buffer.data(), output_offset, ex_bitmap,
                             /*ex_offset=*/0, length));

    // Now check that the rest of the bits in out_buffer are still 0
    AssertValidityZeroExtraBits(output);
  };

  // 0b01101110 0b01001110
  uint8_t expected1[2] = {110, 78};
  CheckCase({arr1, arr2, arr3}, 7, expected1);
  CheckCase({arr1, arr2, arr3}, 7, expected1, /*preallocate=*/true);
  CheckCase({arr1, arr2, arr3}, 7, expected1, /*preallocate=*/true,
            /*output_offset=*/4);

  // 0b01111110 0b01001111
  uint8_t expected2[2] = {126, 79};
  CheckCase({arr1, arr2}, 5, expected2);
  CheckCase({arr1, arr2}, 5, expected2, /*preallocate=*/true,
            /*output_offset=*/4);
}

TEST_F(TestPropagateNulls, NullOutputTypeNoop) {
  // Ensure we leave the buffers alone when the output type is null()
  const int64_t length = 100;
  ExecBatch batch({rng_->Boolean(100, 0.5, 0.5)}, length);

  ArrayData output(null(), length, {nullptr});
  ASSERT_OK(PropagateNulls(ctx_.get(), ExecSpan(batch), &output));
  ASSERT_EQ(nullptr, output.buffers[0]);
}

// ----------------------------------------------------------------------
// Test PropagateNullsSpans (new span-based implementation). Some of
// the tests above had to be rewritten because the span-based
// implementation does not deal with zero-copy optimizations right now

class TestPropagateNullsSpans : public TestComputeInternals {};

TEST_F(TestPropagateNullsSpans, UnknownNullCountWithNullsZeroCopies) {
  const int64_t length = 16;

  const uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);
  auto ty = boolean();
  ArrayData input(ty, length, {nulls, nullptr}, kUnknownNullCount);

  uint8_t validity_bitmap2[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  auto nulls2 = std::make_shared<Buffer>(validity_bitmap2, 8);
  ArraySpan output(ty.get(), length);
  output.buffers[0].data = validity_bitmap2;
  output.buffers[0].size = 0;

  ExecSpan span(ExecBatch({input}, length));
  PropagateNullsSpans(span, &output);
  ASSERT_EQ(kUnknownNullCount, output.null_count);
  ASSERT_EQ(9, output.GetNullCount());
}

TEST_F(TestPropagateNullsSpans, UnknownNullCountWithoutNulls) {
  const int64_t length = 16;
  constexpr uint8_t validity_bitmap[8] = {255, 255, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);

  auto ty = boolean();
  ArrayData input(ty, length, {nulls, nullptr}, kUnknownNullCount);

  uint8_t validity_bitmap2[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  auto nulls2 = std::make_shared<Buffer>(validity_bitmap2, 8);
  ArraySpan output(ty.get(), length);
  output.buffers[0].data = validity_bitmap2;
  output.buffers[0].size = 0;

  ExecSpan span(ExecBatch({input}, length));
  PropagateNullsSpans(span, &output);
  ASSERT_EQ(kUnknownNullCount, output.null_count);
  ASSERT_EQ(0, output.GetNullCount());
}

TEST_F(TestPropagateNullsSpans, SetAllNulls) {
  const int64_t length = 16;

  auto CheckSetAllNull = [&](std::vector<Datum> values) {
    // Make fresh bitmap with all 1's
    uint8_t bitmap_data[2] = {255, 255};
    auto buf = std::make_shared<MutableBuffer>(bitmap_data, 2);

    auto ty = boolean();
    ArraySpan output(ty.get(), length);
    output.SetBuffer(0, buf);

    ExecSpan span(ExecBatch(values, length));
    PropagateNullsSpans(span, &output);

    uint8_t expected[2] = {0, 0};
    ASSERT_EQ(0, std::memcmp(output.buffers[0].data, expected, output.buffers[0].size));
  };

  // There is a null scalar
  std::shared_ptr<Scalar> i32_val = std::make_shared<Int32Scalar>(3);
  std::vector<Datum> vals = {i32_val, MakeNullScalar(boolean())};
  CheckSetAllNull(vals);

  const double true_prob = 0.5;
  vals[0] = rng_->Boolean(length, true_prob);
  CheckSetAllNull(vals);

  auto arr_all_nulls = rng_->Boolean(length, true_prob, /*null_probability=*/1);

  // One value is all null
  vals = {rng_->Boolean(length, true_prob, /*null_probability=*/0.5), arr_all_nulls};
  CheckSetAllNull(vals);

  // A value is NullType
  std::shared_ptr<Array> null_arr = std::make_shared<NullArray>(length);
  vals = {rng_->Boolean(length, true_prob), null_arr};
  CheckSetAllNull(vals);
}

TEST_F(TestPropagateNullsSpans, SingleValueWithNulls) {
  // Input offset is non-zero (0 mod 8 and nonzero mod 8 cases)
  const int64_t length = 100;
  auto arr = rng_->Boolean(length, 0.5, /*null_probability=*/0.5);

  auto CheckSliced = [&](int64_t offset, int64_t out_offset = 0) {
    // Unaligned bitmap, zero copy not possible
    auto sliced = arr->Slice(offset);
    std::vector<Datum> vals = {sliced};

    auto ty = boolean();
    ArraySpan output(ty.get(), vals[0].length());
    output.offset = out_offset;

    std::shared_ptr<Buffer> preallocated_bitmap;
    ASSERT_OK_AND_ASSIGN(
        preallocated_bitmap,
        AllocateBuffer(bit_util::BytesForBits(sliced->length() + out_offset)));
    std::memset(preallocated_bitmap->mutable_data(), 0, preallocated_bitmap->size());
    output.SetBuffer(0, preallocated_bitmap);

    ExecBatch batch(vals, vals[0].length());
    PropagateNullsSpans(ExecSpan(batch), &output);
    ASSERT_EQ(arr->Slice(offset)->null_count(), output.GetNullCount());
    ASSERT_TRUE(BitmapEquals(output.buffers[0].data, output.offset,
                             sliced->null_bitmap_data(), sliced->offset(),
                             output.length));
    AssertValidityZeroExtraBits(output);
  };

  CheckSliced(8);
  CheckSliced(7);
  CheckSliced(8, /*offset=*/4);
  CheckSliced(7, /*offset=*/4);
}

TEST_F(TestPropagateNullsSpans, CasesThatUsedToBeZeroCopy) {
  // ARROW-16576: testing behaviors that used to be zero copy but are
  // not anymore
  const int64_t length = 16;

  auto ty = boolean();
  constexpr uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);

  ArraySpan some_nulls(ty.get(), length);
  some_nulls.SetBuffer(0, nulls);
  some_nulls.null_count = 9;

  ArraySpan no_nulls(ty.get(), length);
  no_nulls.null_count = 0;

  {
    uint8_t bitmap_data[2] = {0, 0};
    auto preallocated_mem = std::make_shared<Buffer>(bitmap_data, 2);

    ArraySpan output(ty.get(), length);
    output.SetBuffer(0, preallocated_mem);
    PropagateNullsSpans(ExecSpan({some_nulls, no_nulls}, length), &output);
    ASSERT_EQ(
        0, std::memcmp(output.buffers[0].data, validity_bitmap, output.buffers[0].size));
    ASSERT_EQ(output.buffers[0].owner, &preallocated_mem);
    ASSERT_EQ(9, output.GetNullCount());
  }

  // Flip order of args
  {
    uint8_t bitmap_data[2] = {0, 0};
    auto preallocated_mem = std::make_shared<Buffer>(bitmap_data, 2);

    ArraySpan output(ty.get(), length);
    output.SetBuffer(0, preallocated_mem);
    PropagateNullsSpans(ExecSpan({no_nulls, no_nulls, some_nulls}, length), &output);
    ASSERT_EQ(
        0, std::memcmp(output.buffers[0].data, validity_bitmap, output.buffers[0].size));
    ASSERT_EQ(output.buffers[0].owner, &preallocated_mem);
    ASSERT_EQ(9, output.GetNullCount());
  }
}

TEST_F(TestPropagateNullsSpans, IntersectsNulls) {
  const int64_t length = 16;

  // 0b01111111 0b11001111
  constexpr uint8_t bitmap1[8] = {127, 207, 0, 0, 0, 0, 0, 0};
  auto buffer1 = std::make_shared<Buffer>(bitmap1, 8);

  // 0b11111110 0b01111111
  constexpr uint8_t bitmap2[8] = {254, 127, 0, 0, 0, 0, 0, 0};
  auto buffer2 = std::make_shared<Buffer>(bitmap2, 8);

  // 0b11101111 0b11111110
  constexpr uint8_t bitmap3[8] = {239, 254, 0, 0, 0, 0, 0, 0};
  auto buffer3 = std::make_shared<Buffer>(bitmap3, 8);

  auto ty = boolean();

  ArraySpan arr1(ty.get(), length);
  arr1.SetBuffer(0, buffer1);

  ArraySpan arr2(ty.get(), length);
  arr2.SetBuffer(0, buffer2);

  ArraySpan arr3(ty.get(), length);
  arr3.SetBuffer(0, buffer3);

  auto CheckCase = [&](std::vector<ExecValue> values, int64_t ex_null_count,
                       const uint8_t* ex_bitmap, int64_t output_offset = 0) {
    ExecSpan batch(values, length);

    std::shared_ptr<Buffer> nulls;
    // Make the buffer one byte bigger so we can have non-zero offsets
    ASSERT_OK_AND_ASSIGN(nulls, AllocateBuffer(3));
    std::memset(nulls->mutable_data(), 0, nulls->size());

    ArraySpan output(ty.get(), length);
    output.SetBuffer(0, nulls);
    output.offset = output_offset;

    PropagateNullsSpans(batch, &output);
    ASSERT_EQ(&nulls, output.buffers[0].owner);
    EXPECT_EQ(kUnknownNullCount, output.null_count);
    EXPECT_EQ(ex_null_count, output.GetNullCount());
    ASSERT_TRUE(BitmapEquals(output.buffers[0].data, output_offset, ex_bitmap,
                             /*ex_offset=*/0, length));

    // Now check that the rest of the bits in out_buffer are still 0
    AssertValidityZeroExtraBits(output);
  };

  // 0b01101110 0b01001110
  uint8_t expected1[2] = {110, 78};
  CheckCase({arr1, arr2, arr3}, 7, expected1);
  CheckCase({arr1, arr2, arr3}, 7, expected1, /*output_offset=*/4);

  // 0b01111110 0b01001111
  uint8_t expected2[2] = {126, 79};
  CheckCase({arr1, arr2}, 5, expected2, /*output_offset=*/4);
}

TEST_F(TestPropagateNullsSpans, NullOutputTypeNoop) {
  // Ensure we leave the buffers alone when the output type is null()
  // TODO(wesm): is this test useful? Can probably delete
  const int64_t length = 100;
  ExecBatch batch({rng_->Boolean(100, 0.5, 0.5)}, length);

  auto ty = null();
  ArraySpan result(ty.get(), length);
  PropagateNullsSpans(ExecSpan(batch), &result);
  ASSERT_EQ(nullptr, result.buffers[0].data);
}

// ----------------------------------------------------------------------
// ExecSpanIterator tests

class TestExecSpanIterator : public TestComputeInternals {
 public:
  void SetupIterator(const ExecBatch& batch,
                     int64_t max_chunksize = kDefaultMaxChunksize) {
    ASSERT_OK(iterator_.Init(batch, max_chunksize));
  }
  void CheckIteration(const ExecBatch& input, int chunksize,
                      const std::vector<int>& ex_batch_sizes) {
    SetupIterator(input, chunksize);
    ExecSpan batch;
    int64_t position = 0;
    for (size_t i = 0; i < ex_batch_sizes.size(); ++i) {
      ASSERT_EQ(position, iterator_.position());
      ASSERT_TRUE(iterator_.Next(&batch));
      ASSERT_EQ(ex_batch_sizes[i], batch.length);

      for (size_t j = 0; j < input.values.size(); ++j) {
        switch (input[j].kind()) {
          case Datum::SCALAR:
            ASSERT_TRUE(input[j].scalar()->Equals(*batch[j].scalar));
            break;
          case Datum::ARRAY:
            AssertArraysEqual(*input[j].make_array()->Slice(position, batch.length),
                              *batch[j].array.ToArray());
            break;
          case Datum::CHUNKED_ARRAY: {
            const ChunkedArray& carr = *input[j].chunked_array();
            if (batch.length == 0) {
              ASSERT_EQ(0, carr.length());
            } else {
              auto arg_slice = carr.Slice(position, batch.length);
              // The sliced ChunkedArrays should only ever be 1 chunk
              ASSERT_EQ(1, arg_slice->num_chunks());
              AssertArraysEqual(*arg_slice->chunk(0), *batch[j].array.ToArray());
            }
          } break;
          default:
            break;
        }
      }
      position += ex_batch_sizes[i];
    }
    // Ensure that the iterator is exhausted
    ASSERT_FALSE(iterator_.Next(&batch));

    ASSERT_EQ(iterator_.length(), iterator_.position());
  }

 protected:
  ExecSpanIterator iterator_;
};

TEST_F(TestExecSpanIterator, Basics) {
  const int64_t length = 100;

  ExecBatch input;
  input.length = 100;

  // Simple case with a single chunk
  input.values = {Datum(GetInt32Array(length)), Datum(GetFloat64Array(length)),
                  Datum(std::make_shared<Int32Scalar>(3))};
  SetupIterator(input);

  ExecSpan batch;
  ASSERT_TRUE(iterator_.Next(&batch));
  ASSERT_EQ(3, batch.values.size());
  ASSERT_EQ(3, batch.num_values());
  ASSERT_EQ(length, batch.length);

  AssertArraysEqual(*input[0].make_array(), *batch[0].array.ToArray());
  AssertArraysEqual(*input[1].make_array(), *batch[1].array.ToArray());
  ASSERT_TRUE(input[2].scalar()->Equals(*batch[2].scalar));

  ASSERT_EQ(length, iterator_.position());
  ASSERT_FALSE(iterator_.Next(&batch));

  // Split into chunks of size 16
  CheckIteration(input, /*chunksize=*/16, {16, 16, 16, 16, 16, 16, 4});
}

TEST_F(TestExecSpanIterator, InputValidation) {
  ExecSpanIterator iterator;

  ExecBatch batch({Datum(GetInt32Array(10)), Datum(GetInt32Array(9))}, 10);
  ASSERT_RAISES(Invalid, iterator.Init(batch));

  batch.values = {Datum(GetInt32Array(9)), Datum(GetInt32Array(10))};
  ASSERT_RAISES(Invalid, iterator.Init(batch));

  batch.values = {Datum(GetInt32Array(10))};
  ASSERT_OK(iterator.Init(batch));
}

TEST_F(TestExecSpanIterator, ChunkedArrays) {
  ExecBatch batch({Datum(GetInt32Chunked({0, 20, 10})), Datum(GetInt32Chunked({15, 15})),
                   Datum(GetInt32Array(30)), Datum(std::make_shared<Int32Scalar>(5)),
                   Datum(MakeNullScalar(boolean()))},
                  30);

  CheckIteration(batch, /*chunksize=*/10, {10, 5, 5, 10});
  CheckIteration(batch, /*chunksize=*/20, {15, 5, 10});
  CheckIteration(batch, /*chunksize=*/30, {15, 5, 10});
}

TEST_F(TestExecSpanIterator, ZeroLengthInputs) {
  auto carr = std::make_shared<ChunkedArray>(ArrayVector{}, int32());
  auto dict_arr =
      std::make_shared<ChunkedArray>(ArrayVector{}, dictionary(int32(), utf8()));
  auto nested_arr = std::make_shared<ChunkedArray>(
      ArrayVector{}, struct_({field("x", int32()), field("y", int64())}));

  auto CheckArgs = [&](const ExecBatch& batch) {
    ExecSpanIterator iterator;
    ASSERT_OK(iterator.Init(batch));
    ExecSpan iter_span;
    ASSERT_TRUE(iterator.Next(&iter_span));
    ASSERT_EQ(0, iter_span.length);
    for (int col_idx = 0; col_idx < iter_span.num_values(); col_idx++) {
      const ExecValue& val = iter_span.values[col_idx];
      ASSERT_TRUE(val.is_array());
      const ArraySpan& span = val.array;
      if (span.type->id() == Type::DICTIONARY) {
        ASSERT_EQ(1, span.child_data.size());
        ASSERT_EQ(0, span.dictionary().length);
      } else {
        for (const auto& child : span.child_data) {
          ASSERT_EQ(0, child.length);
        }
      }
    }
    ASSERT_FALSE(iterator.Next(&iter_span));
  };

  ExecBatch input;
  input.length = 0;

  // Zero-length ChunkedArray with zero chunks
  input.values = {Datum(carr)};
  CheckArgs(input);

  // Zero-length ChunkedArray with zero chunks, dictionary
  input.values = {Datum(dict_arr)};
  CheckArgs(input);

  // Zero-length ChunkedArray with zero chunks, nested
  input.values = {Datum(nested_arr)};
  CheckArgs(input);

  // Zero-length array
  input.values = {Datum(GetInt32Array(0))};
  CheckArgs(input);

  // ChunkedArray with single empty chunk
  input.values = {Datum(GetInt32Chunked({0}))};
  CheckArgs(input);
}

// ----------------------------------------------------------------------
// Scalar function execution

Status ExecCopyArrayData(KernelContext*, const ExecSpan& batch, ExecResult* out) {
  DCHECK_EQ(1, batch.num_values());
  int value_size = batch[0].type()->byte_width();

  const ArraySpan& arg0 = batch[0].array;
  ArrayData* out_arr = out->array_data().get();
  uint8_t* dst = out_arr->buffers[1]->mutable_data() + out_arr->offset * value_size;
  const uint8_t* src = arg0.buffers[1].data + arg0.offset * value_size;
  std::memcpy(dst, src, batch.length * value_size);
  return Status::OK();
}

Status ExecCopyArraySpan(KernelContext*, const ExecSpan& batch, ExecResult* out) {
  DCHECK_EQ(1, batch.num_values());
  int value_size = batch[0].type()->byte_width();
  const ArraySpan& arg0 = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  uint8_t* dst = out_arr->buffers[1].data + out_arr->offset * value_size;
  const uint8_t* src = arg0.buffers[1].data + arg0.offset * value_size;
  std::memcpy(dst, src, batch.length * value_size);
  return Status::OK();
}

Status ExecComputedBitmap(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // Propagate nulls not used. Check that the out bitmap isn't the same already
  // as the input bitmap
  const ArraySpan& arg0 = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  if (CountSetBits(arg0.buffers[0].data, arg0.offset, batch.length) > 0) {
    // Check that the bitmap has not been already copied over
    DCHECK(!BitmapEquals(arg0.buffers[0].data, arg0.offset, out_arr->buffers[0].data,
                         out_arr->offset, batch.length));
  }

  CopyBitmap(arg0.buffers[0].data, arg0.offset, batch.length, out_arr->buffers[0].data,
             out_arr->offset);
  return ExecCopyArraySpan(ctx, batch, out);
}

Status ExecNoPreallocatedData(KernelContext* ctx, const ExecSpan& batch,
                              ExecResult* out) {
  // Validity preallocated, but not the data
  ArrayData* out_arr = out->array_data().get();
  DCHECK_EQ(0, out_arr->offset);
  int value_size = batch[0].type()->byte_width();
  Status s = (ctx->Allocate(out_arr->length * value_size).Value(&out_arr->buffers[1]));
  DCHECK_OK(s);
  return ExecCopyArrayData(ctx, batch, out);
}

Status ExecNoPreallocatedAnything(KernelContext* ctx, const ExecSpan& batch,
                                  ExecResult* out) {
  // Neither validity nor data preallocated
  ArrayData* out_arr = out->array_data().get();
  DCHECK_EQ(0, out_arr->offset);
  Status s = (ctx->AllocateBitmap(out_arr->length).Value(&out_arr->buffers[0]));
  DCHECK_OK(s);
  const ArraySpan& arg0 = batch[0].array;
  CopyBitmap(arg0.buffers[0].data, arg0.offset, batch.length,
             out_arr->buffers[0]->mutable_data(), /*offset=*/0);

  // Reuse the kernel that allocates the data
  return ExecNoPreallocatedData(ctx, batch, out);
}

class ExampleOptions : public FunctionOptions {
 public:
  explicit ExampleOptions(std::shared_ptr<Scalar> value);
  std::shared_ptr<Scalar> value;
};

class ExampleOptionsType : public FunctionOptionsType {
 public:
  static const FunctionOptionsType* GetInstance() {
    static std::unique_ptr<FunctionOptionsType> instance(new ExampleOptionsType());
    return instance.get();
  }
  const char* type_name() const override { return "example"; }
  std::string Stringify(const FunctionOptions& options) const override {
    return type_name();
  }
  bool Compare(const FunctionOptions& options,
               const FunctionOptions& other) const override {
    return true;
  }
  std::unique_ptr<FunctionOptions> Copy(const FunctionOptions& options) const override {
    const auto& opts = static_cast<const ExampleOptions&>(options);
    return std::make_unique<ExampleOptions>(opts.value);
  }
};
ExampleOptions::ExampleOptions(std::shared_ptr<Scalar> value)
    : FunctionOptions(ExampleOptionsType::GetInstance()), value(std::move(value)) {}

struct ExampleState : public KernelState {
  std::shared_ptr<Scalar> value;
  explicit ExampleState(std::shared_ptr<Scalar> value) : value(std::move(value)) {}
};

Result<std::unique_ptr<KernelState>> InitStateful(KernelContext*,
                                                  const KernelInitArgs& args) {
  auto func_options = static_cast<const ExampleOptions*>(args.options);
  return std::make_unique<ExampleState>(func_options ? func_options->value : nullptr);
}

Status ExecStateful(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // We take the value from the state and multiply the data in batch[0] with it
  ExampleState* state = static_cast<ExampleState*>(ctx->state());
  int32_t multiplier = checked_cast<const Int32Scalar&>(*state->value).value;

  const ArraySpan& arg0 = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  const int32_t* arg0_data = arg0.GetValues<int32_t>(1);
  int32_t* dst = out_arr->GetValues<int32_t>(1);
  for (int64_t i = 0; i < arg0.length; ++i) {
    dst[i] = arg0_data[i] * multiplier;
  }
  return Status::OK();
}

Status ExecAddInt32(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const int32_t* left_data = batch[0].array.GetValues<int32_t>(1);
  const int32_t* right_data = batch[1].array.GetValues<int32_t>(1);
  int32_t* out_data = out->array_span_mutable()->GetValues<int32_t>(1);
  for (int64_t i = 0; i < batch.length; ++i) {
    *out_data++ = *left_data++ + *right_data++;
  }
  return Status::OK();
}

class TestCallScalarFunction : public TestComputeInternals {
 protected:
  static bool initialized_;

  void SetUp() {
    TestComputeInternals::SetUp();

    if (!initialized_) {
      initialized_ = true;
      AddCopyFunctions();
      AddNoPreallocateFunctions();
      AddStatefulFunction();
      AddScalarFunction();
    }
  }

  void AddCopyFunctions() {
    auto registry = GetFunctionRegistry();

    // This function simply copies memory from the input argument into the
    // (preallocated) output
    auto func = std::make_shared<ScalarFunction>("test_copy", Arity::Unary(),
                                                 /*doc=*/FunctionDoc::Empty());

    // Add a few kernels. Our implementation only accepts arrays
    ASSERT_OK(func->AddKernel({uint8()}, uint8(), ExecCopyArraySpan));
    ASSERT_OK(func->AddKernel({int32()}, int32(), ExecCopyArraySpan));
    ASSERT_OK(func->AddKernel({float64()}, float64(), ExecCopyArraySpan));
    ASSERT_OK(registry->AddFunction(func));

    // A version which doesn't want the executor to call PropagateNulls
    auto func2 = std::make_shared<ScalarFunction>(
        "test_copy_computed_bitmap", Arity::Unary(), /*doc=*/FunctionDoc::Empty());
    ScalarKernel kernel({uint8()}, uint8(), ExecComputedBitmap);
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    ASSERT_OK(func2->AddKernel(kernel));
    ASSERT_OK(registry->AddFunction(func2));
  }

  void AddNoPreallocateFunctions() {
    auto registry = GetFunctionRegistry();

    // A function that allocates its own output memory. We have cases for both
    // non-preallocated data and non-preallocated validity bitmap
    auto f1 = std::make_shared<ScalarFunction>("test_nopre_data", Arity::Unary(),
                                               /*doc=*/FunctionDoc::Empty());
    auto f2 = std::make_shared<ScalarFunction>(
        "test_nopre_validity_or_data", Arity::Unary(), /*doc=*/FunctionDoc::Empty());

    ScalarKernel kernel({uint8()}, uint8(), ExecNoPreallocatedData);
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    ASSERT_OK(f1->AddKernel(kernel));

    kernel.exec = ExecNoPreallocatedAnything;
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    ASSERT_OK(f2->AddKernel(kernel));

    ASSERT_OK(registry->AddFunction(f1));
    ASSERT_OK(registry->AddFunction(f2));
  }

  void AddStatefulFunction() {
    auto registry = GetFunctionRegistry();

    // This function's behavior depends on a static parameter that is made
    // available to the kernel's execution function through its Options object
    auto func = std::make_shared<ScalarFunction>("test_stateful", Arity::Unary(),
                                                 /*doc=*/FunctionDoc::Empty());

    ScalarKernel kernel({int32()}, int32(), ExecStateful, InitStateful);
    ASSERT_OK(func->AddKernel(kernel));
    ASSERT_OK(registry->AddFunction(func));
  }

  void AddScalarFunction() {
    auto registry = GetFunctionRegistry();

    auto func = std::make_shared<ScalarFunction>("test_scalar_add_int32", Arity::Binary(),
                                                 /*doc=*/FunctionDoc::Empty());
    ASSERT_OK(func->AddKernel({int32(), int32()}, int32(), ExecAddInt32));
    ASSERT_OK(registry->AddFunction(func));
  }
};

bool TestCallScalarFunction::initialized_ = false;

class FunctionCaller {
 public:
  virtual ~FunctionCaller() = default;

  virtual Result<Datum> Call(const std::vector<Datum>& args,
                             const FunctionOptions* options,
                             ExecContext* ctx = NULLPTR) = 0;
  virtual Result<Datum> Call(const std::vector<Datum>& args,
                             ExecContext* ctx = NULLPTR) = 0;
};

using FunctionCallerMaker = std::function<Result<std::shared_ptr<FunctionCaller>>(
    const std::string& func_name, std::vector<TypeHolder> in_types)>;

class SimpleFunctionCaller : public FunctionCaller {
 public:
  explicit SimpleFunctionCaller(const std::string& func_name) : func_name(func_name) {}

  static Result<std::shared_ptr<FunctionCaller>> Make(const std::string& func_name) {
    return std::make_shared<SimpleFunctionCaller>(func_name);
  }

  static Result<std::shared_ptr<FunctionCaller>> Maker(const std::string& func_name,
                                                       std::vector<TypeHolder> in_types) {
    return Make(func_name);
  }

  Result<Datum> Call(const std::vector<Datum>& args, const FunctionOptions* options,
                     ExecContext* ctx) override {
    return CallFunction(func_name, args, options, ctx);
  }
  Result<Datum> Call(const std::vector<Datum>& args, ExecContext* ctx) override {
    return CallFunction(func_name, args, ctx);
  }

  std::string func_name;
};

class ExecFunctionCaller : public FunctionCaller {
 public:
  explicit ExecFunctionCaller(std::shared_ptr<FunctionExecutor> func_exec)
      : func_exec(std::move(func_exec)) {}

  static Result<std::shared_ptr<FunctionCaller>> Make(
      const std::string& func_name, const std::vector<Datum>& args,
      const FunctionOptions* options = nullptr,
      FunctionRegistry* func_registry = nullptr) {
    ARROW_ASSIGN_OR_RAISE(auto func_exec,
                          GetFunctionExecutor(func_name, args, options, func_registry));
    return std::make_shared<ExecFunctionCaller>(std::move(func_exec));
  }

  static Result<std::shared_ptr<FunctionCaller>> Make(
      const std::string& func_name, std::vector<TypeHolder> in_types,
      const FunctionOptions* options = nullptr,
      FunctionRegistry* func_registry = nullptr) {
    ARROW_ASSIGN_OR_RAISE(
        auto func_exec, GetFunctionExecutor(func_name, in_types, options, func_registry));
    return std::make_shared<ExecFunctionCaller>(std::move(func_exec));
  }

  static Result<std::shared_ptr<FunctionCaller>> Maker(const std::string& func_name,
                                                       std::vector<TypeHolder> in_types) {
    return Make(func_name, std::move(in_types));
  }

  Result<Datum> Call(const std::vector<Datum>& args, const FunctionOptions* options,
                     ExecContext* ctx) override {
    ARROW_RETURN_NOT_OK(func_exec->Init(options, ctx));
    return func_exec->Execute(args);
  }
  Result<Datum> Call(const std::vector<Datum>& args, ExecContext* ctx) override {
    return Call(args, nullptr, ctx);
  }

  std::shared_ptr<FunctionExecutor> func_exec;
};

class TestCallScalarFunctionArgumentValidation : public TestCallScalarFunction {
 protected:
  void DoTest(FunctionCallerMaker caller_maker);
};

void TestCallScalarFunctionArgumentValidation::DoTest(FunctionCallerMaker caller_maker) {
  ASSERT_OK_AND_ASSIGN(auto test_copy, caller_maker("test_copy", {int32()}));

  // Copy accepts only a single array argument
  Datum d1(GetInt32Array(10));

  // Too many args
  std::vector<Datum> args = {d1, d1};
  ASSERT_RAISES(Invalid, test_copy->Call(args));

  // Too few
  args = {};
  ASSERT_RAISES(Invalid, test_copy->Call(args));

  // Cannot do scalar
  Datum d1_scalar(std::make_shared<Int32Scalar>(5));
  ASSERT_OK_AND_ASSIGN(auto result, test_copy->Call({d1}));
  ASSERT_OK_AND_ASSIGN(result, test_copy->Call({d1_scalar}));
}

TEST_F(TestCallScalarFunctionArgumentValidation, SimpleCall) {
  TestCallScalarFunctionArgumentValidation::DoTest(SimpleFunctionCaller::Maker);
}

TEST_F(TestCallScalarFunctionArgumentValidation, ExecCall) {
  TestCallScalarFunctionArgumentValidation::DoTest(ExecFunctionCaller::Maker);
}

class TestCallScalarFunctionPreallocationCases : public TestCallScalarFunction {
 protected:
  void DoTest(FunctionCallerMaker caller_maker);
};

void TestCallScalarFunctionPreallocationCases::DoTest(FunctionCallerMaker caller_maker) {
  double null_prob = 0.2;

  auto arr = GetUInt8Array(100, null_prob);

  auto CheckFunction = [&](std::shared_ptr<FunctionCaller> test_copy) {
    ResetContexts();

    // The default should be a single array output
    {
      std::vector<Datum> args = {Datum(arr)};
      ASSERT_OK_AND_ASSIGN(Datum result, test_copy->Call(args));
      ASSERT_EQ(Datum::ARRAY, result.kind());
      AssertArraysEqual(*arr, *result.make_array());
    }

    // Set the exec_chunksize to be smaller, so now we have several invocations
    // of the kernel, but still the output is onee array
    {
      std::vector<Datum> args = {Datum(arr)};
      exec_ctx_->set_exec_chunksize(80);
      ASSERT_OK_AND_ASSIGN(Datum result, test_copy->Call(args, exec_ctx_.get()));
      AssertArraysEqual(*arr, *result.make_array());
    }

    {
      // Chunksize not multiple of 8
      std::vector<Datum> args = {Datum(arr)};
      exec_ctx_->set_exec_chunksize(11);
      ASSERT_OK_AND_ASSIGN(Datum result, test_copy->Call(args, exec_ctx_.get()));
      AssertArraysEqual(*arr, *result.make_array());
    }

    // Input is chunked, output has one big chunk
    {
      auto carr =
          std::make_shared<ChunkedArray>(ArrayVector{arr->Slice(0, 10), arr->Slice(10)});
      std::vector<Datum> args = {Datum(carr)};
      ASSERT_OK_AND_ASSIGN(Datum result, test_copy->Call(args, exec_ctx_.get()));
      std::shared_ptr<ChunkedArray> actual = result.chunked_array();
      ASSERT_EQ(1, actual->num_chunks());
      AssertChunkedEquivalent(*carr, *actual);
    }

    // Preallocate independently for each batch
    {
      std::vector<Datum> args = {Datum(arr)};
      exec_ctx_->set_preallocate_contiguous(false);
      exec_ctx_->set_exec_chunksize(40);
      ASSERT_OK_AND_ASSIGN(Datum result, test_copy->Call(args, exec_ctx_.get()));
      ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
      const ChunkedArray& carr = *result.chunked_array();
      ASSERT_EQ(3, carr.num_chunks());
      AssertArraysEqual(*arr->Slice(0, 40), *carr.chunk(0));
      AssertArraysEqual(*arr->Slice(40, 40), *carr.chunk(1));
      AssertArraysEqual(*arr->Slice(80), *carr.chunk(2));
    }
  };

  ASSERT_OK_AND_ASSIGN(auto test_copy, caller_maker("test_copy", {uint8()}));
  CheckFunction(test_copy);
  ASSERT_OK_AND_ASSIGN(auto test_copy_computed_bitmap,
                       caller_maker("test_copy_computed_bitmap", {uint8()}));
  CheckFunction(test_copy_computed_bitmap);
}

TEST_F(TestCallScalarFunctionPreallocationCases, SimpleCaller) {
  TestCallScalarFunctionPreallocationCases::DoTest(SimpleFunctionCaller::Maker);
}

TEST_F(TestCallScalarFunctionPreallocationCases, ExecCaller) {
  TestCallScalarFunctionPreallocationCases::DoTest(ExecFunctionCaller::Maker);
}

class TestCallScalarFunctionBasicNonStandardCases : public TestCallScalarFunction {
 protected:
  void DoTest(FunctionCallerMaker caller_maker);
};

void TestCallScalarFunctionBasicNonStandardCases::DoTest(
    FunctionCallerMaker caller_maker) {
  // Test a handful of cases
  //
  // * Validity bitmap computed by kernel rather than using PropagateNulls
  // * Data not pre-allocated
  // * Validity bitmap not pre-allocated

  double null_prob = 0.2;

  auto arr = GetUInt8Array(1000, null_prob);
  std::vector<Datum> args = {Datum(arr)};

  auto CheckFunction = [&](std::shared_ptr<FunctionCaller> test_nopre) {
    ResetContexts();

    // The default should be a single array output
    {
      ASSERT_OK_AND_ASSIGN(Datum result, test_nopre->Call(args));
      AssertArraysEqual(*arr, *result.make_array(), true);
    }

    // Split execution into 3 chunks
    {
      exec_ctx_->set_exec_chunksize(400);
      ASSERT_OK_AND_ASSIGN(Datum result, test_nopre->Call(args, exec_ctx_.get()));
      ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
      const ChunkedArray& carr = *result.chunked_array();
      ASSERT_EQ(3, carr.num_chunks());
      AssertArraysEqual(*arr->Slice(0, 400), *carr.chunk(0));
      AssertArraysEqual(*arr->Slice(400, 400), *carr.chunk(1));
      AssertArraysEqual(*arr->Slice(800), *carr.chunk(2));
    }
  };

  ASSERT_OK_AND_ASSIGN(auto test_nopre_data, caller_maker("test_nopre_data", {uint8()}));
  CheckFunction(test_nopre_data);
  ASSERT_OK_AND_ASSIGN(auto test_nopre_validity_or_data,
                       caller_maker("test_nopre_validity_or_data", {uint8()}));
  CheckFunction(test_nopre_validity_or_data);
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, SimpleCall) {
  TestCallScalarFunctionBasicNonStandardCases::DoTest(SimpleFunctionCaller::Maker);
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, ExecCall) {
  TestCallScalarFunctionBasicNonStandardCases::DoTest(ExecFunctionCaller::Maker);
}

class TestCallScalarFunctionStatefulKernel : public TestCallScalarFunction {
 protected:
  void DoTest(FunctionCallerMaker caller_maker);
};

void TestCallScalarFunctionStatefulKernel::DoTest(FunctionCallerMaker caller_maker) {
  ASSERT_OK_AND_ASSIGN(auto test_stateful, caller_maker("test_stateful", {int32()}));

  auto input = ArrayFromJSON(int32(), "[1, 2, 3, null, 5]");
  auto multiplier = std::make_shared<Int32Scalar>(2);
  auto expected = ArrayFromJSON(int32(), "[2, 4, 6, null, 10]");

  ExampleOptions options(multiplier);
  std::vector<Datum> args = {Datum(input)};
  ASSERT_OK_AND_ASSIGN(Datum result, test_stateful->Call(args, &options));
  AssertArraysEqual(*expected, *result.make_array());
}

TEST_F(TestCallScalarFunctionStatefulKernel, Simplecall) {
  TestCallScalarFunctionStatefulKernel::DoTest(SimpleFunctionCaller::Maker);
}

TEST_F(TestCallScalarFunctionStatefulKernel, ExecCall) {
  TestCallScalarFunctionStatefulKernel::DoTest(ExecFunctionCaller::Maker);
}

class TestCallScalarFunctionScalarFunction : public TestCallScalarFunction {
 protected:
  void DoTest(FunctionCallerMaker caller_maker);
};

void TestCallScalarFunctionScalarFunction::DoTest(FunctionCallerMaker caller_maker) {
  ASSERT_OK_AND_ASSIGN(auto test_scalar_add_int32,
                       caller_maker("test_scalar_add_int32", {int32(), int32()}));

  std::vector<Datum> args = {Datum(std::make_shared<Int32Scalar>(5)),
                             Datum(std::make_shared<Int32Scalar>(7))};
  ASSERT_OK_AND_ASSIGN(Datum result, test_scalar_add_int32->Call(args));
  ASSERT_EQ(Datum::SCALAR, result.kind());

  auto expected = std::make_shared<Int32Scalar>(12);
  ASSERT_TRUE(expected->Equals(*result.scalar()));
}

TEST_F(TestCallScalarFunctionScalarFunction, SimpleCall) {
  TestCallScalarFunctionScalarFunction::DoTest(SimpleFunctionCaller::Maker);
}

TEST_F(TestCallScalarFunctionScalarFunction, ExecCall) {
  TestCallScalarFunctionScalarFunction::DoTest(ExecFunctionCaller::Maker);
}

TEST(Ordering, IsSuborderOf) {
  Ordering a{{SortKey{3}, SortKey{1}, SortKey{7}}};
  Ordering b{{SortKey{3}, SortKey{1}}};
  Ordering c{{SortKey{1}, SortKey{7}}};
  Ordering d{{SortKey{1}, SortKey{7}}, NullPlacement::AtEnd};
  Ordering imp = Ordering::Implicit();
  Ordering unordered = Ordering::Unordered();

  std::vector<Ordering> orderings = {a, b, c, d, imp, unordered};

  auto CheckOrdering = [&](const Ordering& ordering, std::vector<bool> expected) {
    for (std::size_t other_idx = 0; other_idx < orderings.size(); other_idx++) {
      const auto& other = orderings[other_idx];
      if (expected[other_idx]) {
        ASSERT_TRUE(ordering.IsSuborderOf(other));
      } else {
        ASSERT_FALSE(ordering.IsSuborderOf(other));
      }
    }
  };

  CheckOrdering(a, {true, false, false, false, false, false});
  CheckOrdering(b, {true, true, false, false, false, false});
  CheckOrdering(c, {false, false, true, false, false, false});
  CheckOrdering(d, {false, false, false, true, false, false});
  CheckOrdering(imp, {false, false, false, false, false, false});
  CheckOrdering(unordered, {true, true, true, true, true, true});
}

}  // namespace detail
}  // namespace compute
}  // namespace arrow
