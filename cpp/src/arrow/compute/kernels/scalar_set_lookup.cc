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
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/hashing.h"
#include "arrow/util/optional.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::HashTraits;

namespace compute {
namespace internal {
namespace {

template <typename Type>
struct SetLookupState : public KernelState {
  explicit SetLookupState(MemoryPool* pool)
      : lookup_table(pool, 0), lookup_null_count(0) {}

  Status Init(const SetLookupOptions& options) {
    using T = typename GetViewType<Type>::T;
    auto visit_valid = [&](T v) {
      int32_t unused_memo_index;
      return lookup_table.GetOrInsert(v, &unused_memo_index);
    };
    auto visit_null = [&]() {
      if (!options.skip_nulls) {
        lookup_table.GetOrInsertNull();
      }
      return Status::OK();
    };
    if (options.value_set.kind() == Datum::ARRAY) {
      const std::shared_ptr<ArrayData>& value_set = options.value_set.array();
      this->lookup_null_count += value_set->GetNullCount();
      return VisitArrayDataInline<Type>(*value_set, std::move(visit_valid),
                                        std::move(visit_null));
    } else {
      const ChunkedArray& value_set = *options.value_set.chunked_array();
      for (const std::shared_ptr<Array>& chunk : value_set.chunks()) {
        this->lookup_null_count += chunk->null_count();
        RETURN_NOT_OK(VisitArrayDataInline<Type>(*chunk->data(), std::move(visit_valid),
                                                 std::move(visit_null)));
      }
      return Status::OK();
    }
  }

  using MemoTable = typename HashTraits<Type>::MemoTableType;
  MemoTable lookup_table;
  int64_t lookup_null_count;
  int64_t null_index = -1;
};

template <>
struct SetLookupState<NullType> : public KernelState {
  explicit SetLookupState(MemoryPool*) {}

  Status Init(const SetLookupOptions& options) {
    this->lookup_null_count = options.value_set.null_count();
    return Status::OK();
  }

  int64_t lookup_null_count;
};

// TODO: Put this concept somewhere reusable
template <int width>
struct UnsignedIntType;

template <>
struct UnsignedIntType<1> {
  using Type = UInt8Type;
};

template <>
struct UnsignedIntType<2> {
  using Type = UInt16Type;
};

template <>
struct UnsignedIntType<4> {
  using Type = UInt32Type;
};

template <>
struct UnsignedIntType<8> {
  using Type = UInt64Type;
};

// Constructing the type requires a type parameter
struct InitStateVisitor {
  KernelContext* ctx;
  const SetLookupOptions* options;
  std::unique_ptr<KernelState> result;

  InitStateVisitor(KernelContext* ctx, const SetLookupOptions* options)
      : ctx(ctx), options(options) {}

  template <typename Type>
  Status Init() {
    if (options == nullptr) {
      return Status::Invalid(
          "Attempted to call a set lookup function without SetLookupOptions");
    }
    using StateType = SetLookupState<Type>;
    result.reset(new StateType(ctx->exec_context()->memory_pool()));
    return static_cast<StateType*>(result.get())->Init(*options);
  }

  Status Visit(const DataType&) { return Init<NullType>(); }

  template <typename Type>
  enable_if_boolean<Type, Status> Visit(const Type&) {
    return Init<BooleanType>();
  }

  template <typename Type>
  enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value, Status> Visit(
      const Type&) {
    return Init<typename UnsignedIntType<sizeof(typename Type::c_type)>::Type>();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    return Init<typename Type::PhysicalType>();
  }

  // Handle Decimal128Type, FixedSizeBinaryType
  Status Visit(const FixedSizeBinaryType& type) { return Init<FixedSizeBinaryType>(); }

  Status GetResult(std::unique_ptr<KernelState>* out) {
    RETURN_NOT_OK(VisitTypeInline(*options->value_set.type(), this));
    *out = std::move(result);
    return Status::OK();
  }
};

std::unique_ptr<KernelState> InitSetLookup(KernelContext* ctx,
                                           const KernelInitArgs& args) {
  InitStateVisitor visitor{ctx, static_cast<const SetLookupOptions*>(args.options)};
  std::unique_ptr<KernelState> result;
  ctx->SetStatus(visitor.GetResult(&result));
  return result;
}

struct IndexInVisitor {
  KernelContext* ctx;
  const ArrayData& data;
  Datum* out;
  Int32Builder builder;

  IndexInVisitor(KernelContext* ctx, const ArrayData& data, Datum* out)
      : ctx(ctx), data(data), out(out), builder(ctx->exec_context()->memory_pool()) {}

  Status Visit(const DataType&) {
    const auto& state = checked_cast<const SetLookupState<NullType>&>(*ctx->state());
    if (data.length != 0) {
      if (state.lookup_null_count == 0) {
        RETURN_NOT_OK(this->builder.AppendNulls(data.length));
      } else {
        RETURN_NOT_OK(this->builder.Reserve(data.length));
        for (int64_t i = 0; i < data.length; ++i) {
          this->builder.UnsafeAppend(0);
        }
      }
    }
    return Status::OK();
  }

  template <typename Type>
  Status ProcessIndexIn() {
    using T = typename GetViewType<Type>::T;

    const auto& state = checked_cast<const SetLookupState<Type>&>(*ctx->state());

    int32_t null_index = state.lookup_table.GetNull();
    RETURN_NOT_OK(this->builder.Reserve(data.length));
    VisitArrayDataInline<Type>(
        data,
        [&](T v) {
          int32_t index = state.lookup_table.Get(v);
          if (index != -1) {
            // matching needle; output index from value_set
            this->builder.UnsafeAppend(index);
          } else {
            // no matching needle; output null
            this->builder.UnsafeAppendNull();
          }
        },
        [&]() {
          if (null_index != -1) {
            // value_set included null
            this->builder.UnsafeAppend(null_index);
          } else {
            // value_set does not include null; output null
            this->builder.UnsafeAppendNull();
          }
        });
    return Status::OK();
  }

  template <typename Type>
  enable_if_boolean<Type, Status> Visit(const Type&) {
    return ProcessIndexIn<BooleanType>();
  }

  template <typename Type>
  enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value, Status> Visit(
      const Type&) {
    return ProcessIndexIn<
        typename UnsignedIntType<sizeof(typename Type::c_type)>::Type>();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    return ProcessIndexIn<typename Type::PhysicalType>();
  }

  // Handle Decimal128Type, FixedSizeBinaryType
  Status Visit(const FixedSizeBinaryType& type) {
    return ProcessIndexIn<FixedSizeBinaryType>();
  }

  Status Execute() {
    Status s = VisitTypeInline(*data.type, this);
    if (!s.ok()) {
      return s;
    }
    std::shared_ptr<ArrayData> out_data;
    RETURN_NOT_OK(this->builder.FinishInternal(&out_data));
    out->value = std::move(out_data);
    return Status::OK();
  }
};

void ExecArrayOrScalar(KernelContext* ctx, const Datum& in, Datum* out,
                       std::function<Status(const ArrayData&)> array_impl) {
  if (in.is_array()) {
    KERNEL_RETURN_IF_ERROR(ctx, array_impl(*in.array()));
    return;
  }

  std::shared_ptr<Array> in_array;
  std::shared_ptr<Scalar> out_scalar;
  KERNEL_RETURN_IF_ERROR(ctx, MakeArrayFromScalar(*in.scalar(), 1).Value(&in_array));
  KERNEL_RETURN_IF_ERROR(ctx, array_impl(*in_array->data()));
  KERNEL_RETURN_IF_ERROR(ctx, out->make_array()->GetScalar(0).Value(&out_scalar));
  *out = std::move(out_scalar);
}

void ExecIndexIn(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ExecArrayOrScalar(ctx, batch[0], out, [&](const ArrayData& in) {
    return IndexInVisitor(ctx, in, out).Execute();
  });
}

// ----------------------------------------------------------------------

// IsIn writes the results into a preallocated binary data bitmap
struct IsInVisitor {
  KernelContext* ctx;
  const ArrayData& data;
  Datum* out;

  IsInVisitor(KernelContext* ctx, const ArrayData& data, Datum* out)
      : ctx(ctx), data(data), out(out) {}

  Status Visit(const DataType&) {
    const auto& state = checked_cast<const SetLookupState<NullType>&>(*ctx->state());
    ArrayData* output = out->mutable_array();
    if (state.lookup_null_count > 0) {
      BitUtil::SetBitsTo(output->buffers[0]->mutable_data(), output->offset,
                         output->length, true);
      BitUtil::SetBitsTo(output->buffers[1]->mutable_data(), output->offset,
                         output->length, true);
    } else {
      BitUtil::SetBitsTo(output->buffers[1]->mutable_data(), output->offset,
                         output->length, false);
    }
    return Status::OK();
  }

  template <typename Type>
  Status ProcessIsIn() {
    using T = typename GetViewType<Type>::T;
    const auto& state = checked_cast<const SetLookupState<Type>&>(*ctx->state());
    ArrayData* output = out->mutable_array();

    if (this->data.GetNullCount() > 0 && state.lookup_null_count > 0) {
      // If there were nulls in the value set, set the whole validity bitmap to
      // true
      output->null_count = 0;
      BitUtil::SetBitsTo(output->buffers[0]->mutable_data(), output->offset,
                         output->length, true);
    }
    FirstTimeBitmapWriter writer(output->buffers[1]->mutable_data(), output->offset,
                                 output->length);
    VisitArrayDataInline<Type>(
        this->data,
        [&](T v) {
          if (state.lookup_table.Get(v) != -1) {
            writer.Set();
          } else {
            writer.Clear();
          }
          writer.Next();
        },
        [&]() {
          writer.Set();
          writer.Next();
        });
    writer.Finish();
    return Status::OK();
  }

  template <typename Type>
  enable_if_boolean<Type, Status> Visit(const Type&) {
    return ProcessIsIn<BooleanType>();
  }

  template <typename Type>
  enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value, Status> Visit(
      const Type&) {
    return ProcessIsIn<typename UnsignedIntType<sizeof(typename Type::c_type)>::Type>();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    return ProcessIsIn<typename Type::PhysicalType>();
  }

  // Handle Decimal128Type, FixedSizeBinaryType
  Status Visit(const FixedSizeBinaryType& type) {
    return ProcessIsIn<FixedSizeBinaryType>();
  }

  Status Execute() { return VisitTypeInline(*data.type, this); }
};

void ExecIsIn(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ExecArrayOrScalar(ctx, batch[0], out, [&](const ArrayData& in) {
    return IsInVisitor(ctx, in, out).Execute();
  });
}

// Unary set lookup kernels available for the following input types
//
// * Null type
// * Boolean
// * Numeric
// * Simple temporal types (date, time, timestamp)
// * Base binary types
// * Decimal

void AddBasicSetLookupKernels(ScalarKernel kernel,
                              const std::shared_ptr<DataType>& out_ty,
                              ScalarFunction* func) {
  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      kernel.signature = KernelSignature::Make({ty}, out_ty);
      DCHECK_OK(func->AddKernel(kernel));
    }
  };

  AddKernels(BaseBinaryTypes());
  AddKernels(NumericTypes());
  AddKernels(TemporalTypes());

  std::vector<Type::type> other_types = {Type::BOOL, Type::DECIMAL,
                                         Type::FIXED_SIZE_BINARY};
  for (auto ty : other_types) {
    kernel.signature = KernelSignature::Make({InputType::Array(ty)}, out_ty);
    DCHECK_OK(func->AddKernel(kernel));
  }
}

// Enables calling is_in with CallFunction as though it were binary.
class IsInMetaBinary : public MetaFunction {
 public:
  IsInMetaBinary() : MetaFunction("is_in_meta_binary", Arity::Binary()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    DCHECK_EQ(options, nullptr);
    return IsIn(args[0], args[1], ctx);
  }
};

// Enables calling index_in with CallFunction as though it were binary.
class IndexInMetaBinary : public MetaFunction {
 public:
  IndexInMetaBinary() : MetaFunction("index_in_meta_binary", Arity::Binary()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    DCHECK_EQ(options, nullptr);
    return IndexIn(args[0], args[1], ctx);
  }
};

}  // namespace

void RegisterScalarSetLookup(FunctionRegistry* registry) {
  // IsIn always writes into preallocated memory
  {
    ScalarKernel isin_base;
    isin_base.init = InitSetLookup;
    isin_base.exec = ExecIsIn;
    auto is_in = std::make_shared<ScalarFunction>("is_in", Arity::Unary());

    AddBasicSetLookupKernels(isin_base, /*output_type=*/boolean(), is_in.get());

    isin_base.signature = KernelSignature::Make({null()}, boolean());
    isin_base.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    DCHECK_OK(is_in->AddKernel(isin_base));
    DCHECK_OK(registry->AddFunction(is_in));

    DCHECK_OK(registry->AddFunction(std::make_shared<IsInMetaBinary>()));
  }

  // IndexIn uses Int32Builder and so is responsible for all its own allocation
  {
    ScalarKernel match_base;
    match_base.init = InitSetLookup;
    match_base.exec = ExecIndexIn;
    match_base.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    match_base.mem_allocation = MemAllocation::NO_PREALLOCATE;
    auto match = std::make_shared<ScalarFunction>("index_in", Arity::Unary());
    AddBasicSetLookupKernels(match_base, /*output_type=*/int32(), match.get());

    match_base.signature = KernelSignature::Make({null()}, int32());
    DCHECK_OK(match->AddKernel(match_base));
    DCHECK_OK(registry->AddFunction(match));

    DCHECK_OK(registry->AddFunction(std::make_shared<IndexInMetaBinary>()));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
