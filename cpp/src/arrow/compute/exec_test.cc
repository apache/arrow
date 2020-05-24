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

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/registry.h"
#include "arrow/memory_pool.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace detail {

TEST(ExecContext, BasicWorkings) {
  {
    ExecContext ctx;
    ASSERT_EQ(GetFunctionRegistry(), ctx.func_registry());
    ASSERT_EQ(default_memory_pool(), ctx.memory_pool());
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), ctx.exec_chunksize());

    ASSERT_TRUE(ctx.use_threads());
    ASSERT_EQ(internal::CpuInfo::GetInstance(), ctx.cpu_info());
  }

  // Now, let's customize all the things
  LoggingMemoryPool my_pool(default_memory_pool());
  std::unique_ptr<FunctionRegistry> custom_reg = FunctionRegistry::Make();
  ExecContext ctx(&my_pool, custom_reg.get());

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

void AssertValidityZeroExtraBits(const ArrayData& arr) {
  const Buffer& buf = *arr.buffers[0];

  const int64_t bit_extent = ((arr.offset + arr.length + 7) / 8) * 8;
  for (int64_t i = arr.offset + arr.length; i < bit_extent; ++i) {
    EXPECT_FALSE(BitUtil::GetBit(buf.data(), i)) << i;
  }
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

class TestPropagateNulls : public TestComputeInternals {};

TEST_F(TestPropagateNulls, UnknownNullCountWithNullsZeroCopies) {
  const int64_t length = 16;

  constexpr uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  auto nulls = std::make_shared<Buffer>(validity_bitmap, 8);

  ArrayData output(boolean(), length, {nullptr, nullptr});
  ArrayData input(boolean(), length, {nulls, nullptr}, kUnknownNullCount);

  ExecBatch batch({input}, length);
  ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));
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
  ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));
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
    ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));

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
    ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));
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
          AllocateBuffer(BitUtil::BytesForBits(sliced->length() + out_offset)));
      std::memset(preallocated_bitmap->mutable_data(), 0, preallocated_bitmap->size());
      output.buffers[0] = preallocated_bitmap;
    } else {
      ASSERT_EQ(0, output.offset);
    }

    ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));

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

    ASSERT_TRUE(internal::BitmapEquals(output.buffers[0]->data(), output.offset,
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
  ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));
  ASSERT_EQ(nulls.get(), output.buffers[0].get());
  ASSERT_EQ(9, output.null_count);

  // Flip order of args
  output = ArrayData(boolean(), length, {nullptr, nullptr});
  batch.values = {no_nulls, no_nulls, some_nulls};
  ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));
  ASSERT_EQ(nulls.get(), output.buffers[0].get());
  ASSERT_EQ(9, output.null_count);

  // Check that preallocated memory is not clobbered
  uint8_t bitmap_data[2] = {0, 0};
  auto preallocated_mem = std::make_shared<MutableBuffer>(bitmap_data, 2);
  output.null_count = kUnknownNullCount;
  output.buffers[0] = preallocated_mem;
  ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));

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

    ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));

    // Preallocated memory used
    if (preallocate) {
      ASSERT_EQ(nulls.get(), output.buffers[0].get());
    }

    EXPECT_EQ(kUnknownNullCount, output.null_count);
    EXPECT_EQ(ex_null_count, output.GetNullCount());

    const auto& out_buffer = *output.buffers[0];

    ASSERT_TRUE(internal::BitmapEquals(out_buffer.data(), output_offset, ex_bitmap,
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
  ASSERT_OK(PropagateNulls(ctx_.get(), batch, &output));
  ASSERT_EQ(nullptr, output.buffers[0]);
}

// ----------------------------------------------------------------------
// ExecBatchIterator

class TestExecBatchIterator : public TestComputeInternals {
 public:
  void SetupIterator(std::vector<Datum> args,
                     int64_t max_chunksize = kDefaultMaxChunksize) {
    ASSERT_OK_AND_ASSIGN(iterator_,
                         ExecBatchIterator::Make(std::move(args), max_chunksize));
  }
  void CheckIteration(const std::vector<Datum>& args, int chunksize,
                      const std::vector<int>& ex_batch_sizes) {
    SetupIterator(args, chunksize);
    ExecBatch batch;
    int64_t position = 0;
    for (size_t i = 0; i < ex_batch_sizes.size(); ++i) {
      ASSERT_EQ(position, iterator_->position());
      ASSERT_TRUE(iterator_->Next(&batch));
      ASSERT_EQ(ex_batch_sizes[i], batch.length);

      for (size_t j = 0; j < args.size(); ++j) {
        switch (args[j].kind()) {
          case Datum::SCALAR:
            ASSERT_TRUE(args[j].scalar()->Equals(batch[j].scalar()));
            break;
          case Datum::ARRAY:
            AssertArraysEqual(*args[j].make_array()->Slice(position, batch.length),
                              *batch[j].make_array());
            break;
          case Datum::CHUNKED_ARRAY: {
            const ChunkedArray& carr = *args[j].chunked_array();
            if (batch.length == 0) {
              ASSERT_EQ(0, carr.length());
            } else {
              auto arg_slice = carr.Slice(position, batch.length);
              // The sliced ChunkedArrays should only ever be 1 chunk
              ASSERT_EQ(1, arg_slice->num_chunks());
              AssertArraysEqual(*arg_slice->chunk(0), *batch[j].make_array());
            }
          } break;
          default:
            break;
        }
      }
      position += ex_batch_sizes[i];
    }
    // Ensure that the iterator is exhausted
    ASSERT_FALSE(iterator_->Next(&batch));

    ASSERT_EQ(iterator_->length(), iterator_->position());
  }

 protected:
  std::unique_ptr<ExecBatchIterator> iterator_;
};

TEST_F(TestExecBatchIterator, Basics) {
  const int64_t length = 100;

  // Simple case with a single chunk
  std::vector<Datum> args = {Datum(GetInt32Array(length)), Datum(GetFloat64Array(length)),
                             Datum(std::make_shared<Int32Scalar>(3))};
  SetupIterator(args);

  ExecBatch batch;
  ASSERT_TRUE(iterator_->Next(&batch));
  ASSERT_EQ(3, batch.values.size());
  ASSERT_EQ(3, batch.num_values());
  ASSERT_EQ(length, batch.length);

  std::vector<ValueDescr> descrs = batch.GetDescriptors();
  ASSERT_EQ(ValueDescr::Array(int32()), descrs[0]);
  ASSERT_EQ(ValueDescr::Array(float64()), descrs[1]);
  ASSERT_EQ(ValueDescr::Scalar(int32()), descrs[2]);

  AssertArraysEqual(*args[0].make_array(), *batch[0].make_array());
  AssertArraysEqual(*args[1].make_array(), *batch[1].make_array());
  ASSERT_TRUE(args[2].scalar()->Equals(batch[2].scalar()));

  ASSERT_EQ(length, iterator_->position());
  ASSERT_FALSE(iterator_->Next(&batch));

  // Split into chunks of size 16
  CheckIteration(args, /*chunksize=*/16, {16, 16, 16, 16, 16, 16, 4});
}

TEST_F(TestExecBatchIterator, InputValidation) {
  std::vector<Datum> args = {Datum(GetInt32Array(10)), Datum(GetInt32Array(9))};
  ASSERT_RAISES(Invalid, ExecBatchIterator::Make(args));

  args = {Datum(GetInt32Array(9)), Datum(GetInt32Array(10))};
  ASSERT_RAISES(Invalid, ExecBatchIterator::Make(args));

  args = {Datum(GetInt32Array(10))};
  ASSERT_OK_AND_ASSIGN(auto iterator, ExecBatchIterator::Make(args));
  ASSERT_EQ(10, iterator->max_chunksize());
}

TEST_F(TestExecBatchIterator, ChunkedArrays) {
  std::vector<Datum> args = {Datum(GetInt32Chunked({0, 20, 10})),
                             Datum(GetInt32Chunked({15, 15})), Datum(GetInt32Array(30)),
                             Datum(std::make_shared<Int32Scalar>(5)),
                             Datum(MakeNullScalar(boolean()))};

  CheckIteration(args, /*chunksize=*/10, {10, 5, 5, 10});
  CheckIteration(args, /*chunksize=*/20, {15, 5, 10});
  CheckIteration(args, /*chunksize=*/30, {15, 5, 10});
}

TEST_F(TestExecBatchIterator, ZeroLengthInputs) {
  auto carr = std::shared_ptr<ChunkedArray>(new ChunkedArray({}, int32()));

  auto CheckArgs = [&](const std::vector<Datum>& args) {
    auto iterator = ExecBatchIterator::Make(args).ValueOrDie();
    ExecBatch batch;
    ASSERT_FALSE(iterator->Next(&batch));
  };

  // Zero-length ChunkedArray with zero chunks
  std::vector<Datum> args = {Datum(carr)};
  CheckArgs(args);

  // Zero-length array
  args = {Datum(GetInt32Array(0))};
  CheckArgs(args);

  // ChunkedArray with single empty chunk
  args = {Datum(GetInt32Chunked({0}))};
  CheckArgs(args);
}

// ----------------------------------------------------------------------
// Scalar function execution

void ExecCopy(KernelContext*, const ExecBatch& batch, Datum* out) {
  DCHECK_EQ(1, batch.num_values());
  const auto& type = checked_cast<const FixedWidthType&>(*batch[0].type());
  int value_size = type.bit_width() / 8;

  const ArrayData& arg0 = *batch[0].array();
  ArrayData* out_arr = out->mutable_array();
  uint8_t* dst = out_arr->buffers[1]->mutable_data() + out_arr->offset * value_size;
  const uint8_t* src = arg0.buffers[1]->data() + arg0.offset * value_size;
  std::memcpy(dst, src, batch.length * value_size);
}

void ExecComputedBitmap(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // Propagate nulls not used. Check that the out bitmap isn't the same already
  // as the input bitmap
  const ArrayData& arg0 = *batch[0].array();
  ArrayData* out_arr = out->mutable_array();

  if (internal::CountSetBits(arg0.buffers[0]->data(), arg0.offset, batch.length) > 0) {
    // Check that the bitmap has not been already copied over
    DCHECK(!internal::BitmapEquals(arg0.buffers[0]->data(), arg0.offset,
                                   out_arr->buffers[0]->data(), out_arr->offset,
                                   batch.length));
  }
  internal::CopyBitmap(arg0.buffers[0]->data(), arg0.offset, batch.length,
                       out_arr->buffers[0]->mutable_data(), out_arr->offset);
  ExecCopy(ctx, batch, out);
}

void ExecNoPreallocatedData(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // Validity preallocated, but not the data
  ArrayData* out_arr = out->mutable_array();
  DCHECK_EQ(0, out_arr->offset);
  const auto& type = checked_cast<const FixedWidthType&>(*batch[0].type());
  int value_size = type.bit_width() / 8;
  Status s = (ctx->Allocate(out_arr->length * value_size).Value(&out_arr->buffers[1]));
  DCHECK_OK(s);
  ExecCopy(ctx, batch, out);
}

void ExecNoPreallocatedAnything(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // Neither validity nor data preallocated
  ArrayData* out_arr = out->mutable_array();
  DCHECK_EQ(0, out_arr->offset);
  Status s = (ctx->AllocateBitmap(out_arr->length).Value(&out_arr->buffers[0]));
  DCHECK_OK(s);
  const ArrayData& arg0 = *batch[0].array();
  internal::CopyBitmap(arg0.buffers[0]->data(), arg0.offset, batch.length,
                       out_arr->buffers[0]->mutable_data(), /*offset=*/0);

  // Reuse the kernel that allocates the data
  ExecNoPreallocatedData(ctx, batch, out);
}

struct ExampleOptions : public FunctionOptions {
  std::shared_ptr<Scalar> value;
  explicit ExampleOptions(std::shared_ptr<Scalar> value) : value(std::move(value)) {}
};

struct ExampleState : public KernelState {
  std::shared_ptr<Scalar> value;
  explicit ExampleState(std::shared_ptr<Scalar> value) : value(std::move(value)) {}
};

std::unique_ptr<KernelState> InitStateful(KernelContext*, const KernelInitArgs& args) {
  auto func_options = static_cast<const ExampleOptions*>(args.options);
  return std::unique_ptr<KernelState>(new ExampleState{func_options->value});
}

void ExecStateful(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // We take the value from the state and multiply the data in batch[0] with it
  ExampleState* state = static_cast<ExampleState*>(ctx->state());
  int32_t multiplier = checked_cast<const Int32Scalar&>(*state->value).value;

  const ArrayData& arg0 = *batch[0].array();
  ArrayData* out_arr = out->mutable_array();
  const int32_t* arg0_data = arg0.GetValues<int32_t>(1);
  int32_t* dst = out_arr->GetMutableValues<int32_t>(1);
  for (int64_t i = 0; i < arg0.length; ++i) {
    dst[i] = arg0_data[i] * multiplier;
  }
}

void ExecAddInt32(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const Int32Scalar& arg0 = batch[0].scalar_as<Int32Scalar>();
  const Int32Scalar& arg1 = batch[1].scalar_as<Int32Scalar>();
  out->value = std::make_shared<Int32Scalar>(arg0.value + arg1.value);
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
    auto func = std::make_shared<ScalarFunction>("test_copy", 1);

    // Add a few kernels. Our implementation only accepts arrays
    ASSERT_OK(func->AddKernel({InputType::Array(uint8())}, uint8(), ExecCopy));
    ASSERT_OK(func->AddKernel({InputType::Array(int32())}, int32(), ExecCopy));
    ASSERT_OK(func->AddKernel({InputType::Array(float64())}, float64(), ExecCopy));
    ASSERT_OK(registry->AddFunction(func));

    // A version which doesn't want the executor to call PropagateNulls
    auto func2 = std::make_shared<ScalarFunction>("test_copy_computed_bitmap", 1);
    ScalarKernel kernel({InputType::Array(uint8())}, uint8(), ExecComputedBitmap);
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    ASSERT_OK(func2->AddKernel(kernel));
    ASSERT_OK(registry->AddFunction(func2));
  }

  void AddNoPreallocateFunctions() {
    auto registry = GetFunctionRegistry();

    // A function that allocates its own output memory. We have cases for both
    // non-preallocated data and non-preallocated validity bitmap
    auto f1 = std::make_shared<ScalarFunction>("test_nopre_data", 1);
    auto f2 = std::make_shared<ScalarFunction>("test_nopre_validity_or_data", 1);

    ScalarKernel kernel({InputType::Array(uint8())}, uint8(), ExecNoPreallocatedData);
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
    auto func = std::make_shared<ScalarFunction>("test_stateful", 1);

    ScalarKernel kernel({InputType::Array(int32())}, int32(), ExecStateful, InitStateful);
    ASSERT_OK(func->AddKernel(kernel));
    ASSERT_OK(registry->AddFunction(func));
  }

  void AddScalarFunction() {
    auto registry = GetFunctionRegistry();

    auto func = std::make_shared<ScalarFunction>("test_scalar_add_int32", 2);
    ASSERT_OK(func->AddKernel({InputType::Scalar(int32()), InputType::Scalar(int32())},
                              int32(), ExecAddInt32));
    ASSERT_OK(registry->AddFunction(func));
  }
};

bool TestCallScalarFunction::initialized_ = false;

TEST_F(TestCallScalarFunction, ArgumentValidation) {
  // Copy accepts only a single array argument
  Datum d1(GetInt32Array(10));

  // Too many args
  std::vector<Datum> args = {d1, d1};
  ASSERT_RAISES(Invalid, CallFunction("test_copy", args));

  // Too few
  args = {};
  ASSERT_RAISES(Invalid, CallFunction("test_copy", args));

  // Cannot do scalar
  args = {Datum(std::make_shared<Int32Scalar>(5))};
  ASSERT_RAISES(NotImplemented, CallFunction("test_copy", args));
}

TEST_F(TestCallScalarFunction, PreallocationCases) {
  double null_prob = 0.2;

  auto arr = GetUInt8Array(1000, null_prob);

  auto CheckFunction = [&](std::string func_name) {
    ResetContexts();

    // The default should be a single array output
    {
      std::vector<Datum> args = {Datum(arr)};
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args));
      ASSERT_EQ(Datum::ARRAY, result.kind());
      AssertArraysEqual(*arr, *result.make_array());
    }

    // Set the exec_chunksize to be smaller, so now we have several invocations
    // of the kernel, but still the output is onee array
    {
      std::vector<Datum> args = {Datum(arr)};
      exec_ctx_->set_exec_chunksize(80);
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args, exec_ctx_.get()));
      AssertArraysEqual(*arr, *result.make_array());
    }

    exec_ctx_->set_exec_chunksize(12);

    // Chunksize not multiple of 8
    {
      std::vector<Datum> args = {Datum(arr)};
      exec_ctx_->set_exec_chunksize(111);
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args, exec_ctx_.get()));
      AssertArraysEqual(*arr, *result.make_array());
    }

    // Input is chunked, output has one big chunk
    {
      auto carr = std::shared_ptr<ChunkedArray>(
          new ChunkedArray({arr->Slice(0, 100), arr->Slice(100)}));
      std::vector<Datum> args = {Datum(carr)};
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args, exec_ctx_.get()));
      std::shared_ptr<ChunkedArray> actual = result.chunked_array();
      ASSERT_EQ(1, actual->num_chunks());
      AssertChunkedEquivalent(*carr, *actual);
    }

    // Preallocate independently for each batch
    {
      std::vector<Datum> args = {Datum(arr)};
      exec_ctx_->set_preallocate_contiguous(false);
      exec_ctx_->set_exec_chunksize(400);
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args, exec_ctx_.get()));
      ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
      const ChunkedArray& carr = *result.chunked_array();
      ASSERT_EQ(3, carr.num_chunks());
      AssertArraysEqual(*arr->Slice(0, 400), *carr.chunk(0));
      AssertArraysEqual(*arr->Slice(400, 400), *carr.chunk(1));
      AssertArraysEqual(*arr->Slice(800), *carr.chunk(2));
    }
  };

  CheckFunction("test_copy");
  CheckFunction("test_copy_computed_bitmap");
}

TEST_F(TestCallScalarFunction, BasicNonStandardCases) {
  // Test a handful of cases
  //
  // * Validity bitmap computed by kernel rather than using PropagateNulls
  // * Data not pre-allocated
  // * Validity bitmap not pre-allocated

  double null_prob = 0.2;

  auto arr = GetUInt8Array(1000, null_prob);
  std::vector<Datum> args = {Datum(arr)};

  auto CheckFunction = [&](std::string func_name) {
    ResetContexts();

    // The default should be a single array output
    {
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args));
      AssertArraysEqual(*arr, *result.make_array(), true);
    }

    // Split execution into 3 chunks
    {
      exec_ctx_->set_exec_chunksize(400);
      ASSERT_OK_AND_ASSIGN(Datum result, CallFunction(func_name, args, exec_ctx_.get()));
      ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
      const ChunkedArray& carr = *result.chunked_array();
      ASSERT_EQ(3, carr.num_chunks());
      AssertArraysEqual(*arr->Slice(0, 400), *carr.chunk(0));
      AssertArraysEqual(*arr->Slice(400, 400), *carr.chunk(1));
      AssertArraysEqual(*arr->Slice(800), *carr.chunk(2));
    }
  };

  CheckFunction("test_nopre_data");
  CheckFunction("test_nopre_validity_or_data");
}

TEST_F(TestCallScalarFunction, StatefulKernel) {
  auto input = ArrayFromJSON(int32(), "[1, 2, 3, null, 5]");
  auto multiplier = std::make_shared<Int32Scalar>(2);
  auto expected = ArrayFromJSON(int32(), "[2, 4, 6, null, 10]");

  ExampleOptions options(multiplier);
  std::vector<Datum> args = {Datum(input)};
  ASSERT_OK_AND_ASSIGN(Datum result, CallFunction("test_stateful", args, &options));
  AssertArraysEqual(*expected, *result.make_array());
}

TEST_F(TestCallScalarFunction, ScalarFunction) {
  std::vector<Datum> args = {Datum(std::make_shared<Int32Scalar>(5)),
                             Datum(std::make_shared<Int32Scalar>(7))};
  ASSERT_OK_AND_ASSIGN(Datum result, CallFunction("test_scalar_add_int32", args));
  ASSERT_EQ(Datum::SCALAR, result.kind());

  auto expected = std::make_shared<Int32Scalar>(12);
  ASSERT_TRUE(expected->Equals(*result.scalar()));
}

}  // namespace detail
}  // namespace compute
}  // namespace arrow
