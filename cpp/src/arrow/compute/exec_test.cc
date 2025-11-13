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
#include "arrow/compute/expression.h"
#include "arrow/compute/function.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/ordering.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/test_util_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging_internal.h"

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
  auto sel_vector = SelectionVectorFromJSON("[0, 42]");

  ASSERT_EQ(sel_vector->length(), 2);
  ASSERT_EQ(sel_vector->indices()[0], 0);
  ASSERT_EQ(sel_vector->indices()[1], 42);
}

TEST(SelectionVector, Validate) {
  {
    auto sel_vector = SelectionVectorFromJSON("[]");
    ASSERT_OK(sel_vector->Validate());
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[0, null, 42]");
    ASSERT_RAISES(Invalid, sel_vector->Validate());
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[42, 0]");
    ASSERT_RAISES(Invalid, sel_vector->Validate());
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[-42, 0]");
    ASSERT_RAISES(Invalid, sel_vector->Validate());
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[]");
    ASSERT_OK(sel_vector->Validate(/*values_length=*/0));
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[0]");
    ASSERT_RAISES(Invalid, sel_vector->Validate(/*values_length=*/0));
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[0, 41]");
    ASSERT_OK(sel_vector->Validate(/*values_length=*/42));
  }
  {
    auto sel_vector = SelectionVectorFromJSON("[0, 42]");
    ASSERT_RAISES(Invalid, sel_vector->Validate(/*values_length=*/42));
  }
}

TEST(SelectionVectorSpan, Basics) {
  auto indices = ArrayFromJSON(int32(), "[0, 3, 7]");
  SelectionVectorSpan sel_span(indices->data()->GetValues<int32_t>(1),
                               indices->length() - 1,
                               /*offset=*/1, /*index_back_shift=*/1);
  ASSERT_EQ(sel_span[0], 2);
  ASSERT_EQ(sel_span[1], 6);

  sel_span.SetSlice(/*offset=*/1, /*length=*/2, /*index_back_shift=*/0);
  ASSERT_EQ(sel_span[0], 3);
  ASSERT_EQ(sel_span[1], 7);

  sel_span.SetSlice(/*offset=*/0, /*length=*/3);
  ASSERT_EQ(sel_span[0], 0);
  ASSERT_EQ(sel_span[1], 3);
  ASSERT_EQ(sel_span[2], 7);
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
    ASSERT_EQ(input.selection_vector, nullptr);
    std::vector<int> ex_selection_sizes(ex_batch_sizes.size(), 0);
    return CheckIteration(input, chunksize, ex_batch_sizes, ex_selection_sizes);
  }
  void CheckIteration(const ExecBatch& input, int chunksize,
                      const std::vector<int>& ex_batch_sizes,
                      const std::vector<int>& ex_selection_sizes) {
    SetupIterator(input, chunksize);
    ExecSpan batch;
    SelectionVectorSpan selection;
    int64_t position = 0, selection_position = 0;
    for (size_t i = 0; i < ex_batch_sizes.size(); ++i) {
      ASSERT_EQ(position, iterator_.position());
      ASSERT_EQ(selection_position, iterator_.selection_position());
      ASSERT_TRUE(iterator_.Next(&batch, &selection));
      ASSERT_EQ(ex_batch_sizes[i], batch.length);
      ASSERT_EQ(ex_selection_sizes[i], selection.length());

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
      if (iterator_.have_selection_vector()) {
        for (int64_t j = 0; j < selection.length(); ++j) {
          ASSERT_EQ(input.selection_vector->indices()[selection_position + j] - position,
                    selection[j]);
          ASSERT_GE(selection[j], 0);
          ASSERT_LT(selection[j], batch.length);
        }
      }
      position += ex_batch_sizes[i];
      selection_position += ex_selection_sizes[i];
    }
    // Ensure that the iterator is exhausted
    ASSERT_FALSE(iterator_.Next(&batch, &selection));

    ASSERT_EQ(iterator_.length(), iterator_.position());
    ASSERT_EQ(iterator_.selection_length(), iterator_.selection_position());
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

TEST_F(TestExecSpanIterator, SelectionSpanBasic) {
  ExecBatch batch(
      {Datum(GetInt32Array(30)), Datum(GetInt32Array(30)),
       Datum(std::make_shared<Int32Scalar>(5)), Datum(MakeNullScalar(boolean()))},
      30, SelectionVectorFromJSON("[1, 2, 7, 29]"));

  CheckIteration(batch, /*chunksize=*/7, {7, 7, 7, 7, 2}, {2, 1, 0, 0, 1});
  CheckIteration(batch, /*chunksize=*/10, {10, 10, 10}, {3, 0, 1});
  CheckIteration(batch, /*chunksize=*/20, {20, 10}, {3, 1});
  CheckIteration(batch, /*chunksize=*/30, {30}, {4});
}

TEST_F(TestExecSpanIterator, SelectionSpanChunked) {
  ExecBatch batch({Datum(GetInt32Chunked({0, 20, 10})), Datum(GetInt32Chunked({15, 15})),
                   Datum(GetInt32Array(30)), Datum(std::make_shared<Int32Scalar>(5)),
                   Datum(MakeNullScalar(boolean()))},
                  30, SelectionVectorFromJSON("[1, 2, 7, 29]"));

  CheckIteration(batch, /*chunksize=*/7, {7, 7, 1, 5, 7, 3}, {2, 1, 0, 0, 0, 1});
  CheckIteration(batch, /*chunksize=*/10, {10, 5, 5, 10}, {3, 0, 0, 1});
  CheckIteration(batch, /*chunksize=*/20, {15, 5, 10}, {3, 0, 1});
  CheckIteration(batch, /*chunksize=*/30, {15, 5, 10}, {3, 0, 1});
}

// ----------------------------------------------------------------------
// Scalar function execution

template <typename OnSelectedFn, typename OnNonSelectedFn>
void VisitIndicesWithSelection(int64_t length, const SelectionVectorSpan& selection,
                               OnSelectedFn&& on_selected,
                               OnNonSelectedFn&& on_non_selected) {
  int64_t selected = 0;
  for (int64_t i = 0; i < length; ++i) {
    if (selected < selection.length() && i == selection[selected]) {
      on_selected(i);
      ++selected;
    } else {
      on_non_selected(i);
    }
  }
}

constexpr uint8_t kNonSelectedByte = 0xFE;

void AssertArraysEqualSparseWithSelection(const Array& src,
                                          const SelectionVectorSpan& selection,
                                          const Array& dst) {
  ASSERT_EQ(src.length(), dst.length());
  ASSERT_EQ(src.type()->id(), dst.type()->id());

  int value_size = src.type()->byte_width();
  const uint8_t* src_validity = src.data()->buffers[0]->data();
  const uint8_t* dst_validity = dst.data()->buffers[0]->data();
  const uint8_t* src_data = src.data()->buffers[1]->data();
  const uint8_t* dst_data = dst.data()->buffers[1]->data();
  int64_t src_offset = src.data()->offset;
  int64_t dst_offset = dst.data()->offset;

  VisitIndicesWithSelection(
      src.length(), selection,
      [&](int64_t i) {
        // Selected values should match
        ASSERT_EQ(bit_util::GetBit(src_validity, src_offset + i),
                  bit_util::GetBit(dst_validity, dst_offset + i));
        if (bit_util::GetBit(src_validity, src_offset + i)) {
          ASSERT_EQ(memcmp(src_data + (src_offset + i) * value_size,
                           dst_data + (dst_offset + i) * value_size, value_size),
                    0);
        }
      },
      [&](int64_t i) {
        // Non-selected values should be the valid special value in the output
        ASSERT_TRUE(bit_util::GetBit(dst_validity, dst_offset + i));
        for (int j = 0; j < value_size; ++j) {
          ASSERT_EQ(dst_data[(dst_offset + i) * value_size + j], kNonSelectedByte);
        }
      });
}

void AssertArraysEqualDenseWithSelection(const Array& src,
                                         const SelectionVectorSpan& selection,
                                         const Array& dst) {
  ASSERT_EQ(src.length(), dst.length());
  ASSERT_EQ(src.type()->id(), dst.type()->id());

  int value_size = src.type()->byte_width();
  const uint8_t* src_validity = src.data()->buffers[0]->data();
  const uint8_t* dst_validity = dst.data()->buffers[0]->data();
  const uint8_t* src_data = src.data()->buffers[1]->data();
  const uint8_t* dst_data = dst.data()->buffers[1]->data();
  int64_t src_offset = src.data()->offset;
  int64_t dst_offset = dst.data()->offset;

  VisitIndicesWithSelection(
      src.length(), selection,
      [&](int64_t i) {
        // Selected values should match
        ASSERT_EQ(bit_util::GetBit(src_validity, src_offset + i),
                  bit_util::GetBit(dst_validity, dst_offset + i));
        if (bit_util::GetBit(src_validity, src_offset + i)) {
          ASSERT_EQ(memcmp(src_data + (src_offset + i) * value_size,
                           dst_data + (dst_offset + i) * value_size, value_size),
                    0);
        }
      },
      [&](int64_t i) {
        // Non-selected values should be invalid in the output
        ASSERT_FALSE(bit_util::GetBit(dst_validity, dst_offset + i));
      });
}

void AssertChunkedExecResultsEqualSparseWithSelection(int64_t exec_chunksize,
                                                      const Array& input,
                                                      const SelectionVector* selection,
                                                      const Datum& result) {
  ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
  const ChunkedArray& carr = *result.chunked_array();
  SelectionVectorSpan selection_span(selection->indices(), selection->length());
  ASSERT_EQ(bit_util::CeilDiv(input.length(), exec_chunksize), carr.num_chunks());
  int64_t selection_idx = 0;
  for (int i = 0; i < carr.num_chunks(); ++i) {
    auto next_selection_idx = selection_idx;
    while (next_selection_idx < selection->length() &&
           selection->indices()[next_selection_idx] < exec_chunksize * (i + 1)) {
      ++next_selection_idx;
    }
    selection_span.SetSlice(selection_idx, next_selection_idx - selection_idx,
                            static_cast<int32_t>(exec_chunksize * i));
    selection_idx = next_selection_idx;
    AssertArraysEqualSparseWithSelection(
        *input.Slice(exec_chunksize * i,
                     std::min(exec_chunksize, input.length() - exec_chunksize * i)),
        selection_span, *carr.chunk(i));
  }
}

void AssertChunkedExecResultsEqualDenseWithSelection(int64_t exec_chunksize,
                                                     const Array& input,
                                                     const SelectionVector* selection,
                                                     const Datum& result) {
  SelectionVectorSpan selection_span(selection->indices(), selection->length());
  if (selection_span.length() <= exec_chunksize) {
    ASSERT_EQ(Datum::ARRAY, result.kind());
    AssertArraysEqualDenseWithSelection(input, selection_span, *result.make_array());
  } else {
    ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
    const ChunkedArray& carr = *result.chunked_array();
    ASSERT_EQ(1, carr.num_chunks());
    AssertArraysEqualDenseWithSelection(input, selection_span, *carr.chunk(0));
  }
}

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

Status SelectiveExecCopyArrayData(KernelContext* ctx, const ExecSpan& batch,
                                  const SelectionVectorSpan& selection, ExecResult* out) {
  DCHECK_EQ(1, batch.num_values());
  int value_size = batch[0].type()->byte_width();

  const ArraySpan& arg0 = batch[0].array;
  ArrayData* out_arr = out->array_data().get();
  uint8_t* dst_validity = out_arr->buffers[0]->mutable_data();
  int64_t dst_validity_offset = out_arr->offset;
  uint8_t* dst = out_arr->buffers[1]->mutable_data() + out_arr->offset * value_size;
  const uint8_t* src = arg0.buffers[1].data + arg0.offset * value_size;
  VisitIndicesWithSelection(
      batch.length, selection,
      [&](int64_t i) {
        // Copy the selected value
        std::memcpy(dst + i * value_size, src + i * value_size, value_size);
      },
      [&](int64_t i) {
        // Set the non-selected as valid (regardless of its precomputed validity) and set
        // its values with a special value
        bit_util::SetBit(dst_validity, dst_validity_offset + i);
        std::memset(dst + i * value_size, kNonSelectedByte, value_size);
      });
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

Status SelectiveExecCopyArraySpan(KernelContext* ctx, const ExecSpan& batch,
                                  const SelectionVectorSpan& selection, ExecResult* out) {
  DCHECK_EQ(1, batch.num_values());
  int value_size = batch[0].type()->byte_width();
  const ArraySpan& arg0 = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  uint8_t* dst_validity = out_arr->buffers[0].data;
  int64_t dst_validity_offset = out_arr->offset;
  uint8_t* dst = out_arr->buffers[1].data + out_arr->offset * value_size;
  const uint8_t* src = arg0.buffers[1].data + arg0.offset * value_size;
  VisitIndicesWithSelection(
      batch.length, selection,
      [&](int64_t i) {
        // Copy the selected value
        std::memcpy(dst + i * value_size, src + i * value_size, value_size);
      },
      [&](int64_t i) {
        // Set the non-selected as valid (regardless of its precomputed validity) and set
        // its values with a special value
        bit_util::SetBit(dst_validity, dst_validity_offset + i);
        std::memset(dst + i * value_size, kNonSelectedByte, value_size);
      });
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

Status SelectiveExecComputedBitmap(KernelContext* ctx, const ExecSpan& batch,
                                   const SelectionVectorSpan& selection,
                                   ExecResult* out) {
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
  return SelectiveExecCopyArraySpan(ctx, batch, selection, out);
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

Status SelectiveExecNoPreallocatedData(KernelContext* ctx, const ExecSpan& batch,
                                       const SelectionVectorSpan& selection,
                                       ExecResult* out) {
  // Validity preallocated, but not the data
  ArrayData* out_arr = out->array_data().get();
  DCHECK_EQ(0, out_arr->offset);
  int value_size = batch[0].type()->byte_width();
  Status s = (ctx->Allocate(out_arr->length * value_size).Value(&out_arr->buffers[1]));
  DCHECK_OK(s);
  return SelectiveExecCopyArrayData(ctx, batch, selection, out);
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

Status SelectiveExecNoPreallocatedAnything(KernelContext* ctx, const ExecSpan& batch,
                                           const SelectionVectorSpan& selection,
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
  return SelectiveExecNoPreallocatedData(ctx, batch, selection, out);
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

Status SelectiveExecStateful(KernelContext* ctx, const ExecSpan& batch,
                             const SelectionVectorSpan& selection, ExecResult* out) {
  // We take the value from the state and multiply the data in batch[0] with it
  ExampleState* state = static_cast<ExampleState*>(ctx->state());
  int32_t multiplier = checked_cast<const Int32Scalar&>(*state->value).value;

  const ArraySpan& arg0 = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  const int32_t* arg0_data = arg0.GetValues<int32_t>(1);
  uint8_t* dst_validity = out_arr->buffers[0].data;
  int64_t dst_validity_offset = out_arr->offset;
  int32_t* dst = out_arr->GetValues<int32_t>(1);
  VisitIndicesWithSelection(
      batch.length, selection,
      [&](int64_t i) {
        // Copy the selected value
        dst[i] = arg0_data[i] * multiplier;
      },
      [&](int64_t i) {
        // Set the non-selected as valid (regardless of its precomputed validity) and set
        // its values with a special value
        bit_util::SetBit(dst_validity, dst_validity_offset + i);
        memset(dst + i, kNonSelectedByte, sizeof(int32_t));
      });
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

Status SelectiveExecAddInt32(KernelContext* ctx, const ExecSpan& batch,
                             const SelectionVectorSpan& selection, ExecResult* out) {
  const int32_t* left_data = batch[0].array.GetValues<int32_t>(1);
  const int32_t* right_data = batch[1].array.GetValues<int32_t>(1);
  ArraySpan* out_arr = out->array_span_mutable();
  uint8_t* dst_validity = out_arr->buffers[0].data;
  int64_t dst_validity_offset = out_arr->offset;
  int32_t* out_data = out_arr->GetValues<int32_t>(1);
  VisitIndicesWithSelection(
      batch.length, selection,
      [&](int64_t i) {
        // Copy the selected value
        out_data[i] = left_data[i] + right_data[i];
      },
      [&](int64_t i) {
        // Set the non-selected as valid (regardless of its precomputed validity) and set
        // its values with a special value
        bit_util::SetBit(dst_validity, dst_validity_offset + i);
        memset(out_data + i, kNonSelectedByte, sizeof(int32_t));
      });
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
      AddSelectiveCopyFunctions();
      AddNoPreallocateFunctions();
      AddSelectiveNoPreallocateFunctions();
      AddStatefulFunction();
      AddSelectiveStatefulFunction();
      AddScalarFunction();
      AddSelectiveScalarFunction();
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

  void AddSelectiveCopyFunctions() {
    auto registry = GetFunctionRegistry();

    // This function simply copies memory from the input argument into the
    // (preallocated) output
    auto func = std::make_shared<ScalarFunction>("test_copy_selective", Arity::Unary(),
                                                 /*doc=*/FunctionDoc::Empty());

    // Add a few kernels. Our implementation only accepts arrays
    ASSERT_OK(func->AddKernel({uint8()}, uint8(), ExecCopyArraySpan,
                              SelectiveExecCopyArraySpan));
    ASSERT_OK(func->AddKernel({int32()}, int32(), ExecCopyArraySpan,
                              SelectiveExecCopyArraySpan));
    ASSERT_OK(func->AddKernel({float64()}, float64(), ExecCopyArraySpan,
                              SelectiveExecCopyArraySpan));
    ASSERT_OK(registry->AddFunction(func));

    // A version which doesn't want the executor to call PropagateNulls
    auto func2 =
        std::make_shared<ScalarFunction>("test_copy_computed_bitmap_selective",
                                         Arity::Unary(), /*doc=*/FunctionDoc::Empty());
    ScalarKernel kernel({uint8()}, uint8(), ExecComputedBitmap,
                        SelectiveExecComputedBitmap);
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

  void AddSelectiveNoPreallocateFunctions() {
    auto registry = GetFunctionRegistry();

    // A function that allocates its own output memory. We have cases for both
    // non-preallocated data and non-preallocated validity bitmap
    auto f1 =
        std::make_shared<ScalarFunction>("test_nopre_data_selective", Arity::Unary(),
                                         /*doc=*/FunctionDoc::Empty());
    auto f2 =
        std::make_shared<ScalarFunction>("test_nopre_validity_or_data_selective",
                                         Arity::Unary(), /*doc=*/FunctionDoc::Empty());

    ScalarKernel kernel({uint8()}, uint8(), ExecNoPreallocatedData,
                        SelectiveExecNoPreallocatedData);
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    ASSERT_OK(f1->AddKernel(kernel));

    kernel.exec = ExecNoPreallocatedAnything;
    kernel.selective_exec = SelectiveExecNoPreallocatedAnything;
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

  void AddSelectiveStatefulFunction() {
    auto registry = GetFunctionRegistry();

    // This function's behavior depends on a static parameter that is made
    // available to the kernel's execution function through its Options object
    auto func =
        std::make_shared<ScalarFunction>("test_stateful_selective", Arity::Unary(),
                                         /*doc=*/FunctionDoc::Empty());

    ScalarKernel kernel({int32()}, int32(), ExecStateful, SelectiveExecStateful,
                        InitStateful);
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

  void AddSelectiveScalarFunction() {
    auto registry = GetFunctionRegistry();

    auto func = std::make_shared<ScalarFunction>("test_scalar_add_int32_selective",
                                                 Arity::Binary(),
                                                 /*doc=*/FunctionDoc::Empty());
    ASSERT_OK(func->AddKernel({int32(), int32()}, int32(), ExecAddInt32,
                              SelectiveExecAddInt32));
    ASSERT_OK(registry->AddFunction(func));
  }
};

bool TestCallScalarFunction::initialized_ = false;

class FunctionCaller {
 public:
  virtual ~FunctionCaller() = default;

  virtual std::string name() const = 0;

  virtual Result<Datum> Call(const std::vector<Datum>& args,
                             std::shared_ptr<SelectionVector> selection,
                             const FunctionOptions* options = NULLPTR,
                             ExecContext* ctx = NULLPTR) const = 0;

  virtual Result<Datum> Call(const std::vector<Datum>& args,
                             const FunctionOptions* options,
                             ExecContext* ctx = NULLPTR) const {
    return Call(args, nullptr, options, ctx);
  }

  virtual Result<Datum> Call(const std::vector<Datum>& args,
                             ExecContext* ctx = NULLPTR) const {
    return Call(args, /*options=*/nullptr, ctx);
  }
};

using FunctionCallerMaker = std::function<Result<std::shared_ptr<FunctionCaller>>(
    const std::string& func_name, std::vector<TypeHolder> in_types)>;

class SimpleFunctionCaller : public FunctionCaller {
 public:
  explicit SimpleFunctionCaller(const std::string& func_name) : func_name(func_name) {}

  std::string name() const override { return "simple_caller"; }

  static Result<std::shared_ptr<FunctionCaller>> Make(const std::string& func_name) {
    return std::make_shared<SimpleFunctionCaller>(func_name);
  }

  static Result<std::shared_ptr<FunctionCaller>> Maker(const std::string& func_name,
                                                       std::vector<TypeHolder> in_types) {
    return Make(func_name);
  }

  Result<Datum> Call(const std::vector<Datum>& args,
                     std::shared_ptr<SelectionVector> selection,
                     const FunctionOptions* options, ExecContext* ctx) const override {
    ARROW_RETURN_IF(selection != nullptr,
                    Status::Invalid("Selection vector not supported"));
    return CallFunction(func_name, args, options, ctx);
  }

  std::string func_name;
};

class ExecFunctionCaller : public FunctionCaller {
 public:
  explicit ExecFunctionCaller(std::shared_ptr<FunctionExecutor> func_exec)
      : func_exec(std::move(func_exec)) {}

  std::string name() const override { return "exec_caller"; }

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

  Result<Datum> Call(const std::vector<Datum>& args,
                     std::shared_ptr<SelectionVector> selection,
                     const FunctionOptions* options, ExecContext* ctx) const override {
    ARROW_RETURN_IF(selection != nullptr,
                    Status::Invalid("Selection vector not supported"));
    ARROW_RETURN_NOT_OK(func_exec->Init(options, ctx));
    return func_exec->Execute(args);
  }

  std::shared_ptr<FunctionExecutor> func_exec;
};

// Call the function via expression with an optional selection vector.
class ExpressionFunctionCaller : public FunctionCaller {
 public:
  ExpressionFunctionCaller(std::string func_name, const std::vector<TypeHolder>& in_types)
      : func_name_(std::move(func_name)) {
    std::vector<std::shared_ptr<Field>> fields(in_types.size());
    for (size_t i = 0; i < in_types.size(); ++i) {
      fields[i] = field("arg" + std::to_string(i), in_types[i].GetSharedPtr());
    }
    schema_ = schema(std::move(fields));
  }

  std::string name() const override { return "expression_caller"; }

  static Result<std::shared_ptr<FunctionCaller>> Make(std::string func_name,
                                                      std::vector<TypeHolder> in_types) {
    return std::make_shared<ExpressionFunctionCaller>(std::move(func_name),
                                                      std::move(in_types));
  }

  Result<Datum> Call(const std::vector<Datum>& args,
                     std::shared_ptr<SelectionVector> selection,
                     const FunctionOptions* options, ExecContext* ctx) const override {
    bool all_same = false;
    auto length = InferBatchLength(args, &all_same);
    ExecBatch batch(args, length, std::move(selection));
    std::vector<Expression> expr_args(args.size());
    for (int i = 0; i < static_cast<int>(args.size()); ++i) {
      expr_args[i] = field_ref(i);
    }
    Expression expr =
        call(func_name_, std::move(expr_args), options ? options->Copy() : nullptr);
    ARROW_ASSIGN_OR_RAISE(auto bound, expr.Bind(*schema_, ctx));
    return ExecuteScalarExpression(bound, batch, ctx);
  }

  Result<Datum> Call(const std::vector<Datum>& args, const FunctionOptions* options,
                     ExecContext* ctx) const override {
    return Call(args, /*selection=*/nullptr, options, ctx);
  }

  static Result<std::shared_ptr<FunctionCaller>> Maker(const std::string& func_name,
                                                       std::vector<TypeHolder> in_types) {
    return Make(func_name, std::move(in_types));
  }

 private:
  std::string func_name_;
  std::shared_ptr<Schema> schema_;
};

class TestCallScalarFunctionArgumentValidation : public TestCallScalarFunction {};

TEST_F(TestCallScalarFunctionArgumentValidation, Basic) {
  for (const auto& caller_maker : {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
                                   ExpressionFunctionCaller::Maker}) {
    ASSERT_OK_AND_ASSIGN(auto test_copy, caller_maker("test_copy", {int32()}));
    ARROW_SCOPED_TRACE(test_copy->name());
    ResetContexts();

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
}

class TestCallScalarFunctionPreallocationCases : public TestCallScalarFunction {
 protected:
  std::shared_ptr<Array> GetTestArray() { return GetUInt8Array(100, 0.2); }

  std::vector<std::shared_ptr<SelectionVector>> GetTestSelectionVectors() {
    return {SelectionVectorFromJSON("[]"),
            SelectionVectorFromJSON("[0]"),
            SelectionVectorFromJSON("[42]"),
            SelectionVectorFromJSON("[99]"),
            SelectionVectorFromJSON("[0, 1, 2, 3, 4]"),
            SelectionVectorFromJSON("[0, 42, 99]"),
            MakeSelectionVectorTo(40),
            MakeSelectionVectorTo(41),
            MakeSelectionVectorTo(99),
            MakeSelectionVectorTo(100)};
  }

  template <typename CheckFunc>
  void DoTestBasic(const FunctionCaller* caller, const Array& input,
                   std::shared_ptr<SelectionVector> selection, CheckFunc&& check_func) {
    // The default should be a single array output
    {
      std::vector<Datum> args = {Datum(input)};
      ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, selection));
      check_func(result);
    }

    // Set the exec_chunksize to be smaller, so now we have several invocations
    // of the kernel, but still the output is one array
    {
      std::vector<Datum> args = {Datum(input)};
      exec_ctx_->set_exec_chunksize(80);
      ASSERT_OK_AND_ASSIGN(
          Datum result,
          caller->Call(args, selection, /*options=*/nullptr, exec_ctx_.get()));
      check_func(result);
    }

    {
      // Chunksize not multiple of 8
      std::vector<Datum> args = {Datum(input)};
      exec_ctx_->set_exec_chunksize(11);
      ASSERT_OK_AND_ASSIGN(
          Datum result,
          caller->Call(args, selection, /*options=*/nullptr, exec_ctx_.get()));
      check_func(result);
    }
  }

  template <typename CheckFunc>
  void DoTestChunked(const FunctionCaller* caller, const ChunkedArray& input,
                     std::shared_ptr<SelectionVector> selection, CheckFunc&& check_func) {
    // Input is chunked, output has one big chunk
    std::vector<Datum> args = {Datum(input)};
    ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, selection, /*options=*/nullptr,
                                                    exec_ctx_.get()));
    check_func(result);
  }

  template <typename CheckFunc>
  void DoTestIndependentPreallocate(const FunctionCaller* caller, int64_t exec_chunksize,
                                    const Array& input,
                                    std::shared_ptr<SelectionVector> selection,
                                    CheckFunc&& check_func) {
    // Preallocate independently for each batch
    std::vector<Datum> args = {Datum(input)};
    exec_ctx_->set_preallocate_contiguous(false);
    exec_ctx_->set_exec_chunksize(exec_chunksize);
    ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, selection, /*options=*/nullptr,
                                                    exec_ctx_.get()));
    check_func(result);
  }
};

TEST_F(TestCallScalarFunctionPreallocationCases, Basic) {
  auto arr = GetTestArray();
  for (const auto& name : {"test_copy", "test_copy_computed_bitmap"}) {
    ARROW_SCOPED_TRACE(name);
    for (const auto& caller_maker :
         {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
          ExpressionFunctionCaller::Maker}) {
      ASSERT_OK_AND_ASSIGN(auto test_copy, caller_maker(name, {uint8()}));
      ARROW_SCOPED_TRACE(test_copy->name());
      ResetContexts();

      DoTestBasic(test_copy.get(), *arr, /*selection=*/nullptr, [&](const Datum& result) {
        ASSERT_EQ(Datum::ARRAY, result.kind());
        AssertArraysEqual(*arr, *result.make_array());
      });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, BasicSelectiveSparse) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name :
       {"test_copy_selective", "test_copy_computed_bitmap_selective"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_copy,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      SelectionVectorSpan selection_span(selection->indices(), selection->length());
      ResetContexts();

      DoTestBasic(test_copy.get(), *arr, selection, [&](const Datum& result) {
        ASSERT_EQ(Datum::ARRAY, result.kind());
        AssertArraysEqualSparseWithSelection(*arr, selection_span, *result.make_array());
      });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, BasicSelectiveDense) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name : {"test_copy", "test_copy_computed_bitmap"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_copy,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      SelectionVectorSpan selection_span(selection->indices(), selection->length());
      ResetContexts();

      DoTestBasic(test_copy.get(), *arr, selection, [&](const Datum& result) {
        ASSERT_EQ(Datum::ARRAY, result.kind());
        AssertArraysEqualDenseWithSelection(*arr, selection_span, *result.make_array());
      });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, Chunked) {
  auto arr = GetTestArray();
  auto carr =
      std::make_shared<ChunkedArray>(ArrayVector{arr->Slice(0, 10), arr->Slice(10)});
  for (const auto& name : {"test_copy", "test_copy_computed_bitmap"}) {
    ARROW_SCOPED_TRACE(name);
    for (const auto& caller_maker :
         {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
          ExpressionFunctionCaller::Maker}) {
      ASSERT_OK_AND_ASSIGN(auto test_copy, caller_maker(name, {uint8()}));
      ARROW_SCOPED_TRACE(test_copy->name());
      ResetContexts();

      DoTestChunked(test_copy.get(), *carr, /*selection=*/nullptr,
                    [&](const Datum& result) {
                      ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
                      std::shared_ptr<ChunkedArray> actual = result.chunked_array();
                      ASSERT_EQ(1, actual->num_chunks());
                      AssertChunkedEquivalent(*carr, *actual);
                    });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, ChunkedSelectiveSparse) {
  auto arr = GetTestArray();
  auto carr =
      std::make_shared<ChunkedArray>(ArrayVector{arr->Slice(0, 10), arr->Slice(10)});
  auto selections = GetTestSelectionVectors();
  for (const auto& name :
       {"test_copy_selective", "test_copy_computed_bitmap_selective"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_copy,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      SelectionVectorSpan selection_span(selection->indices(), selection->length());
      ResetContexts();

      DoTestChunked(test_copy.get(), *carr, selection, [&](const Datum& result) {
        ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
        std::shared_ptr<ChunkedArray> actual = result.chunked_array();
        ASSERT_EQ(1, actual->num_chunks());
        AssertArraysEqualSparseWithSelection(*arr, selection_span, *actual->chunk(0));
      });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, ChunkedSelectiveDense) {
  auto arr = GetTestArray();
  auto carr =
      std::make_shared<ChunkedArray>(ArrayVector{arr->Slice(0, 10), arr->Slice(10)});
  auto selections = GetTestSelectionVectors();
  for (const auto& name : {"test_copy", "test_copy_computed_bitmap"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_copy,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      SelectionVectorSpan selection_span(selection->indices(), selection->length());
      ResetContexts();

      DoTestChunked(test_copy.get(), *carr, selection, [&](const Datum& result) {
        ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
        std::shared_ptr<ChunkedArray> actual = result.chunked_array();
        ASSERT_EQ(1, actual->num_chunks());
        AssertArraysEqualDenseWithSelection(*arr, selection_span, *actual->chunk(0));
      });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, IndependentPreallocate) {
  auto arr = GetTestArray();
  for (const auto& name : {"test_copy", "test_copy_computed_bitmap"}) {
    ARROW_SCOPED_TRACE(name);
    for (const auto& caller_maker :
         {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
          ExpressionFunctionCaller::Maker}) {
      ASSERT_OK_AND_ASSIGN(auto test_copy, caller_maker(name, {uint8()}));
      ARROW_SCOPED_TRACE(test_copy->name());
      ResetContexts();

      DoTestIndependentPreallocate(
          test_copy.get(), /*exec_chunksize=*/40, *arr, /*selection=*/nullptr,
          [&](const Datum& result) {
            ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
            const ChunkedArray& carr = *result.chunked_array();
            ASSERT_EQ(3, carr.num_chunks());
            AssertArraysEqual(*arr->Slice(0, 40), *carr.chunk(0));
            AssertArraysEqual(*arr->Slice(40, 40), *carr.chunk(1));
            AssertArraysEqual(*arr->Slice(80), *carr.chunk(2));
          });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, IndependentPreallocateSelectiveSparse) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name :
       {"test_copy_selective", "test_copy_computed_bitmap_selective"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_copy,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      const int64_t exec_chunksize = 40;
      ResetContexts();

      DoTestIndependentPreallocate(test_copy.get(), exec_chunksize, *arr, selection,
                                   [&](const Datum& result) {
                                     AssertChunkedExecResultsEqualSparseWithSelection(
                                         exec_chunksize, *arr, selection.get(), result);
                                   });
    }
  }
}

TEST_F(TestCallScalarFunctionPreallocationCases, IndependentPreallocateSelectiveDense) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name : {"test_copy", "test_copy_computed_bitmap"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_copy,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      const int64_t exec_chunksize = 40;
      ResetContexts();

      DoTestIndependentPreallocate(test_copy.get(), exec_chunksize, *arr, selection,
                                   [&](const Datum& result) {
                                     AssertChunkedExecResultsEqualDenseWithSelection(
                                         exec_chunksize, *arr, selection.get(), result);
                                   });
    }
  }
}

// Test a handful of cases
//
// * Validity bitmap computed by kernel rather than using PropagateNulls
// * Data not pre-allocated
// * Validity bitmap not pre-allocated
class TestCallScalarFunctionBasicNonStandardCases : public TestCallScalarFunction {
 protected:
  std::shared_ptr<Array> GetTestArray() { return GetUInt8Array(1000, 0.2); }

  std::vector<std::shared_ptr<SelectionVector>> GetTestSelectionVectors() {
    return {SelectionVectorFromJSON("[]"),    SelectionVectorFromJSON("[0]"),
            SelectionVectorFromJSON("[999]"), MakeSelectionVectorTo(400),
            MakeSelectionVectorTo(401),       MakeSelectionVectorTo(1000)};
  }

  template <typename CheckFunc>
  void DoTestBasic(const FunctionCaller* caller, const Array& input,
                   std::shared_ptr<SelectionVector> selection, CheckFunc&& check_func) {
    // The default should be a single array output
    std::vector<Datum> args = {Datum(input)};
    ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, selection));
    check_func(result);
  }

  template <typename CheckFunc>
  void DoTestSplitExecution(const FunctionCaller* caller, int64_t exec_chunksize,
                            const Array& input,
                            std::shared_ptr<SelectionVector> selection,
                            CheckFunc&& check_func) {
    // Split execution into several chunks
    std::vector<Datum> args = {Datum(input)};
    exec_ctx_->set_exec_chunksize(exec_chunksize);
    ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, selection, /*options=*/nullptr,
                                                    exec_ctx_.get()));
    check_func(result);
  }
};

TEST_F(TestCallScalarFunctionBasicNonStandardCases, Basic) {
  auto arr = GetTestArray();
  for (const auto& name : {"test_nopre_data", "test_nopre_validity_or_data"}) {
    ARROW_SCOPED_TRACE(name);
    for (const auto& caller_maker :
         {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
          ExpressionFunctionCaller::Maker}) {
      ASSERT_OK_AND_ASSIGN(auto test_nopre, caller_maker(name, {uint8()}));
      ARROW_SCOPED_TRACE(test_nopre->name());
      ResetContexts();

      DoTestBasic(test_nopre.get(), *arr, /*selection=*/nullptr,
                  [&](const Datum& result) {
                    ASSERT_EQ(Datum::ARRAY, result.kind());
                    AssertArraysEqual(*arr, *result.make_array(), /*verbose=*/true);
                  });
    }
  }
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, BasicSelectiveSparse) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name :
       {"test_nopre_data_selective", "test_nopre_validity_or_data_selective"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_nopre,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      SelectionVectorSpan selection_span(selection->indices(), selection->length());
      ResetContexts();

      DoTestBasic(test_nopre.get(), *arr, selection, [&](const Datum& result) {
        ASSERT_EQ(Datum::ARRAY, result.kind());
        AssertArraysEqualSparseWithSelection(*arr, selection_span, *result.make_array());
      });
    }
  }
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, BasicSelectiveDense) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name : {"test_nopre_data", "test_nopre_validity_or_data"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_nopre,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      SelectionVectorSpan selection_span(selection->indices(), selection->length());
      ResetContexts();

      DoTestBasic(test_nopre.get(), *arr, selection, [&](const Datum& result) {
        ASSERT_EQ(Datum::ARRAY, result.kind());
        AssertArraysEqualDenseWithSelection(*arr, selection_span, *result.make_array());
      });
    }
  }
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, SplitExecution) {
  auto arr = GetTestArray();
  for (const auto& name : {"test_nopre_data", "test_nopre_validity_or_data"}) {
    ARROW_SCOPED_TRACE(name);
    for (const auto& caller_maker :
         {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
          ExpressionFunctionCaller::Maker}) {
      ASSERT_OK_AND_ASSIGN(auto test_nopre, caller_maker(name, {uint8()}));
      ARROW_SCOPED_TRACE(test_nopre->name());
      ResetContexts();

      DoTestSplitExecution(test_nopre.get(), /*exec_chunksize=*/400, *arr,
                           /*selection=*/nullptr, [&](const Datum& result) {
                             ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
                             const ChunkedArray& carr = *result.chunked_array();
                             ASSERT_EQ(3, carr.num_chunks());
                             AssertArraysEqual(*arr->Slice(0, 400), *carr.chunk(0));
                             AssertArraysEqual(*arr->Slice(400, 400), *carr.chunk(1));
                             AssertArraysEqual(*arr->Slice(800), *carr.chunk(2));
                           });
    }
  }
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, SplitExecutionSelectiveSparse) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name :
       {"test_nopre_data_selective", "test_nopre_validity_or_data_selective"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_nopre,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      const int64_t exec_chunksize = 400;
      ResetContexts();

      DoTestSplitExecution(test_nopre.get(), exec_chunksize, *arr, selection,
                           [&](const Datum& result) {
                             AssertChunkedExecResultsEqualSparseWithSelection(
                                 exec_chunksize, *arr, selection.get(), result);
                           });
    }
  }
}

TEST_F(TestCallScalarFunctionBasicNonStandardCases, SplitExecutionSelectiveDense) {
  auto arr = GetTestArray();
  auto selections = GetTestSelectionVectors();
  for (const auto& name : {"test_nopre_data", "test_nopre_validity_or_data"}) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_OK_AND_ASSIGN(auto test_nopre,
                         ExpressionFunctionCaller::Maker(name, {uint8()}));
    for (const auto& selection : selections) {
      const int64_t exec_chunksize = 400;
      ResetContexts();

      DoTestSplitExecution(test_nopre.get(), exec_chunksize, *arr, selection,
                           [&](const Datum& result) {
                             AssertChunkedExecResultsEqualDenseWithSelection(
                                 exec_chunksize, *arr, selection.get(), result);
                           });
    }
  }
}

class TestCallScalarFunctionStatefulKernel : public TestCallScalarFunction {
 protected:
  std::shared_ptr<Array> GetTestArray() {
    return ArrayFromJSON(int32(), "[1, 2, 3, null, 5]");
  }

  static constexpr int32_t kMultiplier = 2;

  std::shared_ptr<Array> GetExpected() {
    return ArrayFromJSON(int32(), "[2, 4, 6, null, 10]");
  }

  std::vector<std::shared_ptr<SelectionVector>> GetTestSelectionVectors() {
    return {SelectionVectorFromJSON("[]"), SelectionVectorFromJSON("[0]"),
            SelectionVectorFromJSON("[4]"), MakeSelectionVectorTo(2),
            MakeSelectionVectorTo(5)};
  }

  template <typename CheckFunc>
  void DoTestBasic(const FunctionCaller* caller, const Array& input,
                   std::shared_ptr<Scalar> multiplier,
                   std::shared_ptr<SelectionVector> selection, CheckFunc&& check_func) {
    ExampleOptions options(multiplier);
    std::vector<Datum> args = {Datum(input)};
    ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, selection, &options));
    check_func(result);
  }
};

TEST_F(TestCallScalarFunctionStatefulKernel, Basic) {
  auto input = GetTestArray();
  auto multiplier = std::make_shared<Int32Scalar>(kMultiplier);
  auto expected = GetExpected();
  for (const auto& caller_maker : {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
                                   ExpressionFunctionCaller::Maker}) {
    ASSERT_OK_AND_ASSIGN(auto test_stateful, caller_maker("test_stateful", {int32()}));
    ARROW_SCOPED_TRACE(test_stateful->name());
    ResetContexts();

    DoTestBasic(
        test_stateful.get(), *input, multiplier, /*selection=*/nullptr,
        [&](const Datum& result) { AssertArraysEqual(*expected, *result.make_array()); });
  }
}

TEST_F(TestCallScalarFunctionStatefulKernel, BasicSelectiveSparse) {
  auto input = GetTestArray();
  auto multiplier = std::make_shared<Int32Scalar>(kMultiplier);
  auto selections = GetTestSelectionVectors();
  auto expected = GetExpected();
  ASSERT_OK_AND_ASSIGN(
      auto caller, ExpressionFunctionCaller::Maker("test_stateful_selective", {int32()}));
  for (const auto& selection : selections) {
    SelectionVectorSpan selection_span(selection->indices(), selection->length());
    ResetContexts();

    DoTestBasic(caller.get(), *input, multiplier, selection, [&](const Datum& result) {
      ASSERT_EQ(Datum::ARRAY, result.kind());
      AssertArraysEqualSparseWithSelection(*expected, selection_span,
                                           *result.make_array());
    });
  }
}

TEST_F(TestCallScalarFunctionStatefulKernel, BasicSelectiveDense) {
  auto input = GetTestArray();
  auto multiplier = std::make_shared<Int32Scalar>(kMultiplier);
  auto selections = GetTestSelectionVectors();
  auto expected = GetExpected();
  ASSERT_OK_AND_ASSIGN(auto caller,
                       ExpressionFunctionCaller::Maker("test_stateful", {int32()}));
  for (const auto& selection : selections) {
    SelectionVectorSpan selection_span(selection->indices(), selection->length());
    ResetContexts();

    DoTestBasic(caller.get(), *input, multiplier, selection, [&](const Datum& result) {
      ASSERT_EQ(Datum::ARRAY, result.kind());
      AssertArraysEqualDenseWithSelection(*expected, selection_span,
                                          *result.make_array());
    });
  }
}

class TestCallScalarFunctionScalarFunction : public TestCallScalarFunction {
 protected:
  std::vector<Datum> GetTestArgs() {
    return {Datum(std::make_shared<Int32Scalar>(5)),
            Datum(std::make_shared<Int32Scalar>(7))};
  }

  static constexpr int32_t kExpectedResult = 12;

  std::vector<std::shared_ptr<SelectionVector>> GetTestSelectionVectors() {
    return {SelectionVectorFromJSON("[]"), SelectionVectorFromJSON("[0]"),
            SelectionVectorFromJSON("[0, 42]")};
  }

  // Despite the existence of a selection vector, the result is always a scalar when all
  // arguments are scalars.
  void DoTestBasic(const FunctionCaller* caller, const std::vector<Datum>& args,
                   std::shared_ptr<SelectionVector> selection) {
    ASSERT_OK_AND_ASSIGN(Datum result, caller->Call(args, std::move(selection)));
    ASSERT_EQ(Datum::SCALAR, result.kind());

    auto expected = std::make_shared<Int32Scalar>(kExpectedResult);
    ASSERT_TRUE(expected->Equals(*result.scalar()));
  }
};

TEST_F(TestCallScalarFunctionScalarFunction, Basic) {
  auto args = GetTestArgs();
  for (const auto& caller_maker : {SimpleFunctionCaller::Maker, ExecFunctionCaller::Maker,
                                   ExpressionFunctionCaller::Maker}) {
    ASSERT_OK_AND_ASSIGN(auto test_scalar_add_int32,
                         caller_maker("test_scalar_add_int32", {int32(), int32()}));
    ARROW_SCOPED_TRACE(test_scalar_add_int32->name());
    ResetContexts();

    DoTestBasic(test_scalar_add_int32.get(), args, /*selection=*/nullptr);
  }
}

TEST_F(TestCallScalarFunctionScalarFunction, BasicSelectiveSparse) {
  auto args = GetTestArgs();
  auto selections = GetTestSelectionVectors();
  ASSERT_OK_AND_ASSIGN(auto test_scalar_add_int32,
                       ExpressionFunctionCaller::Maker("test_scalar_add_int32_selective",
                                                       {int32(), int32()}));
  for (const auto& selection : selections) {
    ResetContexts();

    DoTestBasic(test_scalar_add_int32.get(), GetTestArgs(), selection);
  }
}

TEST_F(TestCallScalarFunctionScalarFunction, BasicSelectiveDense) {
  auto args = GetTestArgs();
  auto selections = GetTestSelectionVectors();
  ASSERT_OK_AND_ASSIGN(
      auto test_scalar_add_int32,
      ExpressionFunctionCaller::Maker("test_scalar_add_int32", {int32(), int32()}));
  for (const auto& selection : selections) {
    ResetContexts();

    DoTestBasic(test_scalar_add_int32.get(), GetTestArgs(), selection);
  }
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
