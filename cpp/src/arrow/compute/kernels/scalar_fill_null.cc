
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
#include "arrow/visitor_inline.h"

namespace arrow {

namespace compute {
namespace internal {
namespace {

template <typename T, typename R = void>
using enable_if_supports_fill_null = enable_if_t<has_c_type<T>::value, R>;

template <typename Type>
struct FillNullState : public KernelState {
  explicit FillNullState(MemoryPool* pool) {}

  Status Init(const FillNullOptions& options) {
    fill_value = options.fill_value.scalar();
    return Status::OK();
  }

  std::shared_ptr<Scalar> fill_value;
};

template <>
struct FillNullState<NullType> : public KernelState {
  explicit FillNullState(MemoryPool*) {}

  Status Init(const FillNullOptions& options) { return Status::OK(); }

  std::shared_ptr<Scalar> fill_value;
};

struct InitFillNullStateVisitor {
  KernelContext* ctx;
  const FillNullOptions* options;
  std::unique_ptr<KernelState> result;

  InitFillNullStateVisitor(KernelContext* ctx, const FillNullOptions* options)
      : ctx(ctx), options(options) {}

  template <typename Type>
  Status Init() {
    using StateType = FillNullState<Type>;
    result.reset(new StateType(ctx->exec_context()->memory_pool()));
    return static_cast<StateType*>(result.get())->Init(*options);
  }

  Status Visit(const DataType&) { return Init<NullType>(); }

  template <typename Type>
  enable_if_supports_fill_null<Type, Status> Visit(const Type&) {
    return Init<Type>();
  }

  Status GetResult(std::unique_ptr<KernelState>* out) {
    RETURN_NOT_OK(VisitTypeInline(*options->fill_value.type(), this));
    *out = std::move(result);
    return Status::OK();
  }
};

std::unique_ptr<KernelState> InitFillNull(KernelContext* ctx,
                                          const KernelInitArgs& args) {
  InitFillNullStateVisitor visitor{ctx,
                                   static_cast<const FillNullOptions*>(args.options)};
  std::unique_ptr<KernelState> result;
  ctx->SetStatus(visitor.GetResult(&result));
  return result;
}

struct ScalarFillVisitor {
  KernelContext* ctx;
  const ArrayData& data;
  Datum* out;

  ScalarFillVisitor(KernelContext* ctx, const ArrayData& data, Datum* out)
      : ctx(ctx), data(data), out(out) {}

  Status Visit(const DataType&) {
    ArrayData* out_arr = out->mutable_array();
    *out_arr = data;
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    const auto& state = checked_cast<const FillNullState<BooleanType>&>(*ctx->state());
    bool value = UnboxScalar<BooleanType>::Unbox(*state.fill_value);
    ArrayData* out_arr = out->mutable_array();
    FirstTimeBitmapWriter bit_writer(out_arr->buffers[1]->mutable_data(), out_arr->offset,
                                     out_arr->length);
    FirstTimeBitmapWriter bit_writer_validity(out_arr->buffers[0]->mutable_data(),
                                              out_arr->offset, out_arr->length);
    if (data.null_count != 0) {
      BitmapReader bit_reader(data.buffers[1]->data(), data.offset, data.length);
      BitmapReader bit_reader_validity(data.buffers[0]->data(), data.offset, data.length);
      for (int64_t i = 0; i < data.length; i++) {
        if (bit_reader_validity.IsNotSet()) {
          if (value == true) {
            bit_writer.Set();
          } else {
            bit_writer.Clear();
          }
          bit_writer_validity.Set();
        } else {
          if (bit_reader.IsSet()) {
            bit_writer.Set();
          } else {
            bit_writer.Clear();
          }
          bit_writer_validity.Set();
        }
        bit_reader.Next();
        bit_writer.Next();
        bit_reader_validity.Next();
        bit_writer_validity.Next();
      }
      bit_writer_validity.Finish();
      bit_writer.Finish();
    } else {
      *out_arr = data;
    }
    return Status::OK();
  }

  template <typename Type>
  enable_if_supports_fill_null<Type, Status> Visit(const Type&) {
    using T = typename GetViewType<Type>::T;
    const auto& state = checked_cast<const FillNullState<Type>&>(*ctx->state());
    T value = UnboxScalar<Type>::Unbox(*state.fill_value);
    const T* in_data = data.GetValues<T>(1);
    ArrayData* out_arr = out->mutable_array();
    auto out_data = out_arr->GetMutableValues<T>(1);

    if (data.null_count != 0) {
      BitmapReader bit_reader(data.buffers[0]->data(), data.offset, data.length);
      for (int64_t i = 0; i < data.length; i++) {
        if (bit_reader.IsNotSet()) {
          out_data[i] = value;
        } else {
          out_data[i] = static_cast<T>(in_data[i]);
        }
        bit_reader.Next();
      }
      BitUtil::SetBitsTo(out_arr->buffers[0]->mutable_data(), out_arr->offset,
                         out_arr->length, true);
    } else {
      *out_arr = data;
    }
    return Status::OK();
  }

  Status Execute() { return VisitTypeInline(*data.type, this); }
};

void ExecFillNull(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ScalarFillVisitor dispatch(ctx, *batch[0].array(), out);
  ctx->SetStatus(dispatch.Execute());
}

void AddBasicFillNullKernels(ScalarKernel kernel, ScalarFunction* func) {
  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      kernel.signature = KernelSignature::Make({InputType::Array(ty)}, ty);
      DCHECK_OK(func->AddKernel(kernel));
    }
  };

  AddKernels(NumericTypes());
  AddKernels(TemporalTypes());

  std::vector<std::shared_ptr<DataType>> other_types = {boolean()};

  for (auto ty : other_types) {
    kernel.signature = KernelSignature::Make({InputType::Array(ty)}, ty);
    DCHECK_OK(func->AddKernel(kernel));
  }
}

}  // namespace

void RegisterScalarFillNull(FunctionRegistry* registry) {
  // Fill Null always writes into preallocated memory
  {
    ScalarKernel fill_null_base;
    fill_null_base.init = InitFillNull;
    fill_null_base.exec = ExecFillNull;
    auto fill_null = std::make_shared<ScalarFunction>("fill_null", Arity::Unary());

    AddBasicFillNullKernels(fill_null_base, fill_null.get());
    fill_null_base.signature = KernelSignature::Make({InputType::Array(null())}, null());
    fill_null_base.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    DCHECK_OK(fill_null->AddKernel(fill_null_base));
    DCHECK_OK(registry->AddFunction(fill_null));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
