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

#include "arrow/array/array_base.h"
#include "arrow/array/concatenate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {

struct TakeState : public KernelState {
  explicit TakeState(const TakeOptions& options) : options(options) {}
  TakeOptions options;
};

std::unique_ptr<KernelState> InitTake(KernelContext*, const KernelInitArgs& args) {
  // NOTE: TakeOptions are currently unused, but we pass it through anyway
  auto take_options = static_cast<const TakeOptions*>(args.options);
  return std::unique_ptr<KernelState>(new TakeState{*take_options});
}

template <typename ValueType, typename IndexType>
struct TakeFunctor {
  using ValueArrayType = typename TypeTraits<ValueType>::ArrayType;
  using IndexArrayType = typename TypeTraits<IndexType>::ArrayType;
  using IS = ArrayIndexSequence<IndexType>;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ValueArrayType values(batch[0].array());
    IndexArrayType indices(batch[1].array());
    std::shared_ptr<Array> result;
    KERNEL_RETURN_IF_ERROR(ctx, Select(ctx, values, IS(indices), &result));
    out->value = result->data();
  }
};

struct TakeKernelVisitor {
  TakeKernelVisitor(const DataType& value_type, const DataType& index_type)
      : value_type(value_type), index_type(index_type) {}

  template <typename Type>
  Status Visit(const Type&) {
    this->result = codegen::Integer<TakeFunctor, Type>(index_type.id());
    return Status::OK();
  }

  Status Create() { return VisitTypeInline(value_type, this); }

  const DataType& value_type;
  const DataType& index_type;
  ArrayKernelExec result;
};

Status GetTakeKernel(const DataType& value_type, const DataType& index_type,
                     ArrayKernelExec* exec) {
  TakeKernelVisitor visitor(value_type, index_type);
  RETURN_NOT_OK(visitor.Create());
  *exec = visitor.result;
  return Status::OK();
}

// Shorthand naming of these functions
// A -> Array
// C -> ChunkedArray
// R -> RecordBatch
// T -> Table

Result<std::shared_ptr<Array>> TakeAA(const Array& values, const Array& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result,
                        CallFunction("array_take", {values, indices}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<ChunkedArray>> TakeCA(const ChunkedArray& values,
                                             const Array& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto num_chunks = values.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(1);  // Hard-coded 1 for now
  std::shared_ptr<Array> current_chunk;

  // Case 1: `values` has a single chunk, so just use it
  if (num_chunks == 1) {
    current_chunk = values.chunk(0);
  } else {
    // TODO Case 2: See if all `indices` fall in the same chunk and call Array Take on it
    // See
    // https://github.com/apache/arrow/blob/6f2c9041137001f7a9212f244b51bc004efc29af/r/src/compute.cpp#L123-L151
    // TODO Case 3: If indices are sorted, can slice them and call Array Take

    // Case 4: Else, concatenate chunks and call Array Take
    RETURN_NOT_OK(Concatenate(values.chunks(), default_memory_pool(), &current_chunk));
  }
  // Call Array Take on our single chunk
  ARROW_ASSIGN_OR_RAISE(new_chunks[0], TakeAA(*current_chunk, indices, options, ctx));
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<ChunkedArray>> TakeCC(const ChunkedArray& values,
                                             const ChunkedArray& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    // Note that as currently implemented, this is inefficient because `values`
    // will get concatenated on every iteration of this loop
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ChunkedArray> current_chunk,
                          TakeCA(values, *indices.chunk(i), options, ctx));
    // Concatenate the result to make a single array for this chunk
    RETURN_NOT_OK(
        Concatenate(current_chunk->chunks(), default_memory_pool(), &new_chunks[i]));
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<ChunkedArray>> TakeAC(const Array& values,
                                             const ChunkedArray& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    ARROW_ASSIGN_OR_RAISE(new_chunks[i], TakeAA(values, *indices.chunk(i), options, ctx));
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<RecordBatch>> TakeRA(const RecordBatch& batch,
                                            const Array& indices,
                                            const TakeOptions& options,
                                            ExecContext* ctx) {
  auto ncols = batch.num_columns();
  auto nrows = indices.length();
  std::vector<std::shared_ptr<Array>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeAA(*batch.column(j), indices, options, ctx));
  }
  return RecordBatch::Make(batch.schema(), nrows, columns);
}

Result<std::shared_ptr<Table>> TakeTA(const Table& table, const Array& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCA(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), columns);
}

Result<std::shared_ptr<Table>> TakeTC(const Table& table, const ChunkedArray& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCC(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), columns);
}

// Metafunction for dispatching to different Take implementations other than
// Array-Array.
//
// TODO: Revamp approach to executing Take operations. In addition to being
// overly complex dispatching, there is no parallelization.
class TakeMetaFunction : public MetaFunction {
 public:
  TakeMetaFunction() : MetaFunction("take", Arity::Binary()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    Datum::Kind index_kind = args[1].kind();
    const TakeOptions& take_opts = static_cast<const TakeOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeAA(*args[0].make_array(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeAC(*args[0].make_array(), *args[1].chunked_array(), take_opts, ctx);
        }
        break;
      case Datum::CHUNKED_ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeCA(*args[0].chunked_array(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeCC(*args[0].chunked_array(), *args[1].chunked_array(), take_opts,
                        ctx);
        }
        break;
      case Datum::RECORD_BATCH:
        if (index_kind == Datum::ARRAY) {
          return TakeRA(*args[0].record_batch(), *args[1].make_array(), take_opts, ctx);
        }
        break;
      case Datum::TABLE:
        if (index_kind == Datum::ARRAY) {
          return TakeTA(*args[0].table(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeTC(*args[0].table(), *args[1].chunked_array(), take_opts, ctx);
        }
        break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for take operation: "
        "values=",
        args[0].ToString(), "indices=", args[1].ToString());
  }
};

void RegisterVectorTake(FunctionRegistry* registry) {
  VectorKernel base;
  base.init = InitTake;
  base.can_execute_chunkwise = false;

  auto array_take = std::make_shared<VectorFunction>("array_take", Arity::Binary());
  OutputType out_ty(FirstType);

  auto AddKernel = [&](InputType value_ty, const DataType& example_value_ty,
                       const std::shared_ptr<DataType>& index_ty) {
    base.signature =
        KernelSignature::Make({value_ty, InputType::Array(index_ty)}, out_ty);
    DCHECK_OK(GetTakeKernel(example_value_ty, *index_ty, &base.exec));
    DCHECK_OK(array_take->AddKernel(base));
  };

  for (const auto& value_ty : PrimitiveTypes()) {
    for (const auto& index_ty : IntTypes()) {
      AddKernel(InputType::Array(value_ty), *value_ty, index_ty);
    }
  }
  for (const auto& value_ty : ExampleParametricTypes()) {
    for (const auto& index_ty : IntTypes()) {
      AddKernel(InputType::Array(value_ty->id()), *value_ty, index_ty);
    }
  }
  DCHECK_OK(registry->AddFunction(std::move(array_take)));

  // Add take metafunction
  DCHECK_OK(registry->AddFunction(std::make_shared<TakeMetaFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
