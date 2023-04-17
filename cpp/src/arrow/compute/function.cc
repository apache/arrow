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

#include "arrow/compute/function.h"

#include <cstddef>
#include <memory>
#include <sstream>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
Result<std::shared_ptr<Buffer>> FunctionOptionsType::Serialize(
    const FunctionOptions&) const {
  return Status::NotImplemented("Serialize for ", type_name());
}

Result<std::unique_ptr<FunctionOptions>> FunctionOptionsType::Deserialize(
    const Buffer& buffer) const {
  return Status::NotImplemented("Deserialize for ", type_name());
}

std::string FunctionOptions::ToString() const { return options_type()->Stringify(*this); }

bool FunctionOptions::Equals(const FunctionOptions& other) const {
  if (this == &other) return true;
  if (options_type() != other.options_type()) return false;
  return options_type()->Compare(*this, other);
}

std::unique_ptr<FunctionOptions> FunctionOptions::Copy() const {
  return options_type()->Copy(*this);
}

Result<std::shared_ptr<Buffer>> FunctionOptions::Serialize() const {
  return options_type()->Serialize(*this);
}

Result<std::unique_ptr<FunctionOptions>> FunctionOptions::Deserialize(
    const std::string& type_name, const Buffer& buffer) {
  ARROW_ASSIGN_OR_RAISE(auto options,
                        GetFunctionRegistry()->GetFunctionOptionsType(type_name));
  return options->Deserialize(buffer);
}

void PrintTo(const FunctionOptions& options, std::ostream* os) {
  *os << options.ToString();
}

static const FunctionDoc kEmptyFunctionDoc{};

const FunctionDoc& FunctionDoc::Empty() { return kEmptyFunctionDoc; }

static Status CheckArityImpl(const Function& func, int num_args) {
  if (func.arity().is_varargs && num_args < func.arity().num_args) {
    return Status::Invalid("VarArgs function '", func.name(), "' needs at least ",
                           func.arity().num_args, " arguments but only ", num_args,
                           " passed");
  }

  if (!func.arity().is_varargs && num_args != func.arity().num_args) {
    return Status::Invalid("Function '", func.name(), "' accepts ", func.arity().num_args,
                           " arguments but ", num_args, " passed");
  }
  return Status::OK();
}

Status Function::CheckArity(size_t num_args) const {
  return CheckArityImpl(*this, static_cast<int>(num_args));
}

namespace {

Status CheckOptions(const Function& function, const FunctionOptions* options) {
  if (options == nullptr && function.doc().options_required) {
    return Status::Invalid("Function '", function.name(),
                           "' cannot be called without options");
  }
  return Status::OK();
}

}  // namespace

namespace detail {

Status NoMatchingKernel(const Function* func, const std::vector<TypeHolder>& types) {
  return Status::NotImplemented("Function '", func->name(),
                                "' has no kernel matching input types ",
                                TypeHolder::ToString(types));
}

template <typename KernelType>
const KernelType* DispatchExactImpl(const std::vector<KernelType*>& kernels,
                                    const std::vector<TypeHolder>& values) {
  const KernelType* kernel_matches[SimdLevel::MAX] = {nullptr};

  // Validate arity
  for (const auto& kernel : kernels) {
    if (kernel->signature->MatchesInputs(values)) {
      kernel_matches[kernel->simd_level] = kernel;
    }
  }

  // Dispatch as the CPU feature
#if defined(ARROW_HAVE_RUNTIME_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX2)
  auto cpu_info = arrow::internal::CpuInfo::GetInstance();
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX512)) {
    if (kernel_matches[SimdLevel::AVX512]) {
      return kernel_matches[SimdLevel::AVX512];
    }
  }
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX2)
  if (cpu_info->IsSupported(arrow::internal::CpuInfo::AVX2)) {
    if (kernel_matches[SimdLevel::AVX2]) {
      return kernel_matches[SimdLevel::AVX2];
    }
  }
#endif
  if (kernel_matches[SimdLevel::NONE]) {
    return kernel_matches[SimdLevel::NONE];
  }

  return nullptr;
}

const Kernel* DispatchExactImpl(const Function* func,
                                const std::vector<TypeHolder>& values) {
  if (func->kind() == Function::SCALAR) {
    return DispatchExactImpl(checked_cast<const ScalarFunction*>(func)->kernels(),
                             values);
  }

  if (func->kind() == Function::VECTOR) {
    return DispatchExactImpl(checked_cast<const VectorFunction*>(func)->kernels(),
                             values);
  }

  if (func->kind() == Function::SCALAR_AGGREGATE) {
    return DispatchExactImpl(
        checked_cast<const ScalarAggregateFunction*>(func)->kernels(), values);
  }

  if (func->kind() == Function::HASH_AGGREGATE) {
    return DispatchExactImpl(checked_cast<const HashAggregateFunction*>(func)->kernels(),
                             values);
  }

  return nullptr;
}

struct FunctionExecutorImpl : public FunctionExecutor {
  FunctionExecutorImpl(std::vector<TypeHolder> in_types, const Kernel* kernel,
                       std::unique_ptr<detail::KernelExecutor> executor,
                       const Function& func)
      : in_types(std::move(in_types)),
        kernel(kernel),
        kernel_ctx(default_exec_context(), kernel),
        executor(std::move(executor)),
        func(func),
        state(),
        options(NULLPTR),
        inited(false) {}
  virtual ~FunctionExecutorImpl() {}

  Status KernelInit(const FunctionOptions* options) {
    RETURN_NOT_OK(CheckOptions(func, options));
    if (options == NULLPTR) {
      options = func.default_options();
    }
    if (kernel->init) {
      ARROW_ASSIGN_OR_RAISE(state,
                            kernel->init(&kernel_ctx, {kernel, in_types, options}));
      kernel_ctx.SetState(state.get());
    }

    RETURN_NOT_OK(executor->Init(&kernel_ctx, {kernel, in_types, options}));
    this->options = options;
    inited = true;
    return Status::OK();
  }

  Status Init(const FunctionOptions* options, ExecContext* exec_ctx) override {
    if (exec_ctx == NULLPTR) {
      exec_ctx = default_exec_context();
    }
    kernel_ctx = KernelContext{exec_ctx, kernel};
    return KernelInit(options);
  }

  Result<Datum> Execute(const std::vector<Datum>& args, int64_t passed_length) override {
    util::tracing::Span span;

    auto func_kind = func.kind();
    const auto& func_name = func.name();
    START_COMPUTE_SPAN(span, func_name,
                       {{"function.name", func_name},
                        {"function.options", options ? options->ToString() : "<NULLPTR>"},
                        {"function.kind", func_kind}});

    if (in_types.size() != args.size()) {
      return Status::Invalid("Execution of '", func_name, "' expected ", in_types.size(),
                             " arguments but got ", args.size());
    }
    if (!inited) {
      ARROW_RETURN_NOT_OK(Init(NULLPTR, default_exec_context()));
    }
    ExecContext* ctx = kernel_ctx.exec_context();
    // Cast arguments if necessary
    std::vector<Datum> args_with_cast(args.size());
    for (size_t i = 0; i != args.size(); ++i) {
      const auto& in_type = in_types[i];
      auto arg = args[i];
      if (in_type != args[i].type()) {
        ARROW_ASSIGN_OR_RAISE(arg, Cast(args[i], CastOptions::Safe(in_type), ctx));
      }
      args_with_cast[i] = std::move(arg);
    }

    detail::DatumAccumulator listener;

    ExecBatch input(std::move(args_with_cast), /*length=*/0);
    if (input.num_values() == 0) {
      if (passed_length != -1) {
        input.length = passed_length;
      }
    } else {
      bool all_same_length = false;
      int64_t inferred_length = detail::InferBatchLength(input.values, &all_same_length);
      input.length = inferred_length;
      if (func_kind == Function::SCALAR) {
        if (passed_length != -1 && passed_length != inferred_length) {
          return Status::Invalid(
              "Passed batch length for execution did not match actual"
              " length of values for execution of scalar function '",
              func_name, "'");
        }
      } else if (func_kind == Function::VECTOR) {
        auto vkernel = static_cast<const VectorKernel*>(kernel);
        if (!all_same_length && vkernel->can_execute_chunkwise) {
          return Status::Invalid("Arguments for execution of vector kernel function '",
                                 func_name, "' must all be the same length");
        }
      }
    }
    RETURN_NOT_OK(executor->Execute(input, &listener));
    const auto out = executor->WrapResults(input.values, listener.values());
#ifndef NDEBUG
    DCHECK_OK(executor->CheckResultType(out, func_name.c_str()));
#endif
    return out;
  }

  std::vector<TypeHolder> in_types;
  const Kernel* kernel;
  KernelContext kernel_ctx;
  std::unique_ptr<detail::KernelExecutor> executor;
  const Function& func;
  std::unique_ptr<KernelState> state;
  const FunctionOptions* options;
  bool inited;
};

}  // namespace detail

Result<const Kernel*> Function::DispatchExact(
    const std::vector<TypeHolder>& values) const {
  if (kind_ == Function::META) {
    return Status::NotImplemented("Dispatch for a MetaFunction's Kernels");
  }
  RETURN_NOT_OK(CheckArity(values.size()));

  if (auto kernel = detail::DispatchExactImpl(this, values)) {
    return kernel;
  }
  return detail::NoMatchingKernel(this, values);
}

Result<const Kernel*> Function::DispatchBest(std::vector<TypeHolder>* values) const {
  // TODO(ARROW-11508) permit generic conversions here
  return DispatchExact(*values);
}

Result<std::shared_ptr<FunctionExecutor>> Function::GetBestExecutor(
    std::vector<TypeHolder> inputs) const {
  std::unique_ptr<detail::KernelExecutor> executor;
  if (kind() == Function::SCALAR) {
    executor = detail::KernelExecutor::MakeScalar();
  } else if (kind() == Function::VECTOR) {
    executor = detail::KernelExecutor::MakeVector();
  } else if (kind() == Function::SCALAR_AGGREGATE) {
    executor = detail::KernelExecutor::MakeScalarAggregate();
  } else {
    return Status::NotImplemented("Direct execution of HASH_AGGREGATE functions");
  }

  ARROW_ASSIGN_OR_RAISE(const Kernel* kernel, DispatchBest(&inputs));

  return std::make_shared<detail::FunctionExecutorImpl>(std::move(inputs), kernel,
                                                        std::move(executor), *this);
}

namespace {

Result<Datum> ExecuteInternal(const Function& func, std::vector<Datum> args,
                              int64_t passed_length, const FunctionOptions* options,
                              ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(auto inputs, internal::GetFunctionArgumentTypes(args));
  ARROW_ASSIGN_OR_RAISE(auto func_exec, func.GetBestExecutor(inputs));
  ARROW_RETURN_NOT_OK(func_exec->Init(options, ctx));
  return func_exec->Execute(args, passed_length);
}

}  // namespace

Result<Datum> Function::Execute(const std::vector<Datum>& args,
                                const FunctionOptions* options, ExecContext* ctx) const {
  return ExecuteInternal(*this, args, /*passed_length=*/-1, options, ctx);
}

Result<Datum> Function::Execute(const ExecBatch& batch, const FunctionOptions* options,
                                ExecContext* ctx) const {
  return ExecuteInternal(*this, batch.values, batch.length, options, ctx);
}

namespace {

Status ValidateFunctionSummary(const std::string& s) {
  if (s.find('\n') != s.npos) {
    return Status::Invalid("summary contains a newline");
  }
  if (s.back() == '.') {
    return Status::Invalid("summary ends with a point");
  }
  return Status::OK();
}

Status ValidateFunctionDescription(const std::string& s) {
  if (!s.empty() && s.back() == '\n') {
    return Status::Invalid("description ends with a newline");
  }
  constexpr int kMaxLineSize = 78;
  int cur_line_size = 0;
  for (const auto c : s) {
    cur_line_size = (c == '\n') ? 0 : cur_line_size + 1;
    if (cur_line_size > kMaxLineSize) {
      return Status::Invalid("description line length exceeds ", kMaxLineSize,
                             " characters");
    }
  }
  return Status::OK();
}

}  // namespace

Status Function::Validate() const {
  if (!doc_.summary.empty()) {
    // Documentation given, check its contents
    int arg_count = static_cast<int>(doc_.arg_names.size());
    // Some varargs functions allow 0 vararg, others expect at least 1,
    // hence the two possible values below.
    bool arg_count_match = (arg_count == arity_.num_args) ||
                           (arity_.is_varargs && arg_count == arity_.num_args + 1);
    if (!arg_count_match) {
      return Status::Invalid(
          "In function '", name_,
          "': ", "number of argument names for function documentation != function arity");
    }
    Status st = ValidateFunctionSummary(doc_.summary);
    if (st.ok()) {
      st &= ValidateFunctionDescription(doc_.description);
    }
    if (!st.ok()) {
      return st.WithMessage("In function '", name_, "': ", st.message());
    }
  }
  return Status::OK();
}

Status ScalarFunction::AddKernel(std::vector<InputType> in_types, OutputType out_type,
                                 ArrayKernelExec exec, KernelInit init) {
  RETURN_NOT_OK(CheckArity(in_types.size()));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("VarArgs signatures must have exactly one input type");
  }
  auto sig =
      KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();
}

Status ScalarFunction::AddKernel(ScalarKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types().size()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Status VectorFunction::AddKernel(std::vector<InputType> in_types, OutputType out_type,
                                 ArrayKernelExec exec, KernelInit init) {
  RETURN_NOT_OK(CheckArity(in_types.size()));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("VarArgs signatures must have exactly one input type");
  }
  auto sig =
      KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();
}

Status VectorFunction::AddKernel(VectorKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types().size()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Status ScalarAggregateFunction::AddKernel(ScalarAggregateKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types().size()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Status HashAggregateFunction::AddKernel(HashAggregateKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types().size()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Result<Datum> MetaFunction::Execute(const std::vector<Datum>& args,
                                    const FunctionOptions* options,
                                    ExecContext* ctx) const {
  RETURN_NOT_OK(CheckArityImpl(*this, static_cast<int>(args.size())));
  RETURN_NOT_OK(CheckOptions(*this, options));

  if (options == nullptr) {
    options = default_options();
  }
  return ExecuteImpl(args, options, ctx);
}

Result<Datum> MetaFunction::Execute(const ExecBatch& batch,
                                    const FunctionOptions* options,
                                    ExecContext* ctx) const {
  return Execute(batch.values, options, ctx);
}

}  // namespace compute
}  // namespace arrow
