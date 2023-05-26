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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/compute/kernels/vector_selection_filter_internal.h"
#include "arrow/compute/kernels/vector_selection_take_internal.h"
#include "arrow/extension_type.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/int_util.h"

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CheckIndexBounds;
using internal::CopyBitmap;
using internal::CountSetBits;
using internal::OptionalBitBlockCounter;
using internal::OptionalBitIndexer;

namespace compute {
namespace internal {

namespace {

using FilterState = OptionsWrapper<FilterOptions>;
using TakeState = OptionsWrapper<TakeOptions>;

// ----------------------------------------------------------------------
// DropNull Implementation

Result<std::shared_ptr<arrow::BooleanArray>> GetDropNullFilter(const Array& values,
                                                               MemoryPool* memory_pool) {
  auto bitmap_buffer = values.null_bitmap();
  std::shared_ptr<arrow::BooleanArray> out_array = std::make_shared<BooleanArray>(
      values.length(), bitmap_buffer, nullptr, 0, values.offset());
  return out_array;
}

Result<Datum> DropNullArray(const std::shared_ptr<Array>& values, ExecContext* ctx) {
  if (values->null_count() == 0) {
    return values;
  }
  if (values->null_count() == values->length()) {
    return MakeEmptyArray(values->type(), ctx->memory_pool());
  }
  if (values->type()->id() == Type::type::NA) {
    return std::make_shared<NullArray>(0);
  }
  ARROW_ASSIGN_OR_RAISE(auto drop_null_filter,
                        GetDropNullFilter(*values, ctx->memory_pool()));
  return Filter(values, drop_null_filter, FilterOptions::Defaults(), ctx);
}

Result<Datum> DropNullChunkedArray(const std::shared_ptr<ChunkedArray>& values,
                                   ExecContext* ctx) {
  if (values->null_count() == 0) {
    return values;
  }
  if (values->null_count() == values->length()) {
    return ChunkedArray::MakeEmpty(values->type(), ctx->memory_pool());
  }
  std::vector<std::shared_ptr<Array>> new_chunks;
  for (const auto& chunk : values->chunks()) {
    ARROW_ASSIGN_OR_RAISE(auto new_chunk, DropNullArray(chunk, ctx));
    if (new_chunk.length() > 0) {
      new_chunks.push_back(new_chunk.make_array());
    }
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<Datum> DropNullRecordBatch(const std::shared_ptr<RecordBatch>& batch,
                                  ExecContext* ctx) {
  // Compute an upper bound of the final null count
  int64_t null_count = 0;
  for (const auto& column : batch->columns()) {
    null_count += column->null_count();
  }
  if (null_count == 0) {
    return batch;
  }
  ARROW_ASSIGN_OR_RAISE(auto dst,
                        AllocateEmptyBitmap(batch->num_rows(), ctx->memory_pool()));
  bit_util::SetBitsTo(dst->mutable_data(), 0, batch->num_rows(), true);
  for (const auto& column : batch->columns()) {
    if (column->type()->id() == Type::type::NA) {
      bit_util::SetBitsTo(dst->mutable_data(), 0, batch->num_rows(), false);
      break;
    }
    if (column->null_bitmap_data()) {
      ::arrow::internal::BitmapAnd(column->null_bitmap_data(), column->offset(),
                                   dst->data(), 0, column->length(), 0,
                                   dst->mutable_data());
    }
  }
  auto drop_null_filter = std::make_shared<BooleanArray>(batch->num_rows(), dst);
  if (drop_null_filter->true_count() == 0) {
    return RecordBatch::MakeEmpty(batch->schema(), ctx->memory_pool());
  }
  return Filter(Datum(batch), Datum(drop_null_filter), FilterOptions::Defaults(), ctx);
}

Result<Datum> DropNullTable(const std::shared_ptr<Table>& table, ExecContext* ctx) {
  if (table->num_rows() == 0) {
    return table;
  }
  // Compute an upper bound of the final null count
  int64_t null_count = 0;
  for (const auto& col : table->columns()) {
    for (const auto& column_chunk : col->chunks()) {
      null_count += column_chunk->null_count();
    }
  }
  if (null_count == 0) {
    return table;
  }

  arrow::RecordBatchVector filtered_batches;
  TableBatchReader batch_iter(*table);
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_iter.Next());
    if (batch == nullptr) {
      break;
    }
    ARROW_ASSIGN_OR_RAISE(auto filtered_datum, DropNullRecordBatch(batch, ctx))
    if (filtered_datum.length() > 0) {
      filtered_batches.push_back(filtered_datum.record_batch());
    }
  }

  return Table::FromRecordBatches(table->schema(), filtered_batches);
}
const FunctionDoc drop_null_doc(
    "Drop nulls from the input",
    ("The output is populated with values from the input (Array, ChunkedArray,\n"
     "RecordBatch, or Table) without the null values.\n"
     "For the RecordBatch and Table cases, `drop_null` drops the full row if\n"
     "there is any null."),
    {"input"});

class DropNullMetaFunction : public MetaFunction {
 public:
  DropNullMetaFunction() : MetaFunction("drop_null", Arity::Unary(), drop_null_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    switch (args[0].kind()) {
      case Datum::ARRAY: {
        return DropNullArray(args[0].make_array(), ctx);
      } break;
      case Datum::CHUNKED_ARRAY: {
        return DropNullChunkedArray(args[0].chunked_array(), ctx);
      } break;
      case Datum::RECORD_BATCH: {
        return DropNullRecordBatch(args[0].record_batch(), ctx);
      } break;
      case Datum::TABLE: {
        return DropNullTable(args[0].table(), ctx);
      } break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for drop_null operation: "
        "values=",
        args[0].ToString());
  }
};

// ----------------------------------------------------------------------

const FunctionDoc array_filter_doc(
    "Filter with a boolean selection filter",
    ("The output is populated with values from the input `array` at positions\n"
     "where the selection filter is non-zero.  Nulls in the selection filter\n"
     "are handled based on FilterOptions."),
    {"array", "selection_filter"}, "FilterOptions");

const FunctionDoc array_take_doc(
    "Select values from an array based on indices from another array",
    ("The output is populated with values from the input array at positions\n"
     "given by `indices`.  Nulls in `indices` emit null in the output."),
    {"array", "indices"}, "TakeOptions");

const FunctionDoc indices_nonzero_doc(
    "Return the indices of the values in the array that are non-zero",
    ("For each input value, check if it's zero, false or null. Emit the index\n"
     "of the value in the array if it's none of the those."),
    {"values"});

struct NonZeroVisitor {
  UInt64Builder* builder;
  const std::vector<ArraySpan>& arrays;

  NonZeroVisitor(UInt64Builder* builder, const std::vector<ArraySpan>& arrays)
      : builder(builder), arrays(arrays) {}

  Status Visit(const DataType& type) { return Status::NotImplemented(type.ToString()); }

  template <typename Type>
  enable_if_t<is_decimal_type<Type>::value || is_primitive_ctype<Type>::value ||
                  is_boolean_type<Type>::value,
              Status>
  Visit(const Type&) {
    using T = typename GetOutputType<Type>::T;
    const T zero{};
    uint64_t index = 0;

    for (const ArraySpan& current_array : arrays) {
      VisitArrayValuesInline<Type>(
          current_array,
          [&](T v) {
            if (v != zero) {
              this->builder->UnsafeAppend(index++);
            } else {
              ++index;
            }
          },
          [&]() { ++index; });
    }
    return Status::OK();
  }
};

Status DoNonZero(const std::vector<ArraySpan>& arrays, int64_t total_length,
                 std::shared_ptr<ArrayData>* out) {
  UInt64Builder builder;
  RETURN_NOT_OK(builder.Reserve(total_length));

  NonZeroVisitor visitor(&builder, arrays);
  RETURN_NOT_OK(VisitTypeInline(*arrays[0].type, &visitor));
  return builder.FinishInternal(out);
}

Status IndicesNonZeroExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  std::shared_ptr<ArrayData> result;
  RETURN_NOT_OK(DoNonZero({batch[0].array}, batch.length, &result));
  out->value = std::move(result);
  return Status::OK();
}

Status IndicesNonZeroExecChunked(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const ChunkedArray& arr = *batch[0].chunked_array();
  std::vector<ArraySpan> arrays;
  for (int i = 0; i < arr.num_chunks(); ++i) {
    arrays.push_back(ArraySpan(*arr.chunk(i)->data()));
  }
  std::shared_ptr<ArrayData> result;
  RETURN_NOT_OK(DoNonZero(arrays, arr.length(), &result));
  out->value = std::move(result);
  return Status::OK();
}

std::shared_ptr<VectorFunction> MakeIndicesNonZeroFunction(std::string name,
                                                           FunctionDoc doc) {
  auto func = std::make_shared<VectorFunction>(name, Arity::Unary(), std::move(doc));

  VectorKernel kernel;
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  kernel.output_chunked = false;
  kernel.exec = IndicesNonZeroExec;
  kernel.exec_chunked = IndicesNonZeroExecChunked;
  kernel.can_execute_chunkwise = false;

  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      kernel.signature = KernelSignature::Make({ty}, uint64());
      DCHECK_OK(func->AddKernel(kernel));
    }
  };

  AddKernels(NumericTypes());
  AddKernels({boolean()});

  for (const auto& ty : {Type::DECIMAL128, Type::DECIMAL256}) {
    kernel.signature = KernelSignature::Make({ty}, uint64());
    DCHECK_OK(func->AddKernel(kernel));
  }

  return func;
}

}  // namespace

void RegisterVectorSelection(FunctionRegistry* registry) {
  // Filter kernels
  std::vector<SelectionKernelData> filter_kernels;
  PopulateFilterKernels(&filter_kernels);

  VectorKernel filter_base;
  filter_base.init = FilterState::Init;
  RegisterSelectionFunction("array_filter", array_filter_doc, filter_base,
                            /*selection_type=*/boolean(), filter_kernels,
                            GetDefaultFilterOptions(), registry);

  DCHECK_OK(registry->AddFunction(MakeFilterMetaFunction()));

  // Take kernels
  std::vector<SelectionKernelData> take_kernels;
  PopulateTakeKernels(&take_kernels);

  VectorKernel take_base;
  take_base.init = TakeState::Init;
  take_base.can_execute_chunkwise = false;
  RegisterSelectionFunction("array_take", array_take_doc, take_base,
                            /*selection_type=*/match::Integer(), take_kernels,
                            GetDefaultTakeOptions(), registry);

  DCHECK_OK(registry->AddFunction(MakeTakeMetaFunction()));

  // DropNull kernel
  DCHECK_OK(registry->AddFunction(std::make_shared<DropNullMetaFunction>()));

  DCHECK_OK(registry->AddFunction(
      MakeIndicesNonZeroFunction("indices_nonzero", indices_nonzero_doc)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
