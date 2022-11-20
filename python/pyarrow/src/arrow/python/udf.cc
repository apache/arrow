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

#include "arrow/python/udf.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/python/common.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using compute::ExecResult;
using compute::ExecSpan;
using compute::Function;
using compute::OutputType;
using compute::ScalarFunction;
using compute::ScalarKernel;
using internal::checked_cast;

namespace py {

namespace {

struct PythonScalarUdfKernelState : public compute::KernelState {
  explicit PythonScalarUdfKernelState(std::shared_ptr<OwnedRefNoGIL> function)
      : function(function) {}

  std::shared_ptr<OwnedRefNoGIL> function;
};

struct PythonScalarUdfKernelInit {
  explicit PythonScalarUdfKernelInit(std::shared_ptr<OwnedRefNoGIL> function)
      : function(function) {}

  // function needs to be destroyed at process exit
  // and Python may no longer be initialized.
  ~PythonScalarUdfKernelInit() {
    if (_Py_IsFinalizing()) {
      function->detach();
    }
  }

  Result<std::unique_ptr<compute::KernelState>> operator()(
      compute::KernelContext*, const compute::KernelInitArgs&) {
    return std::make_unique<PythonScalarUdfKernelState>(function);
  }

  std::shared_ptr<OwnedRefNoGIL> function;
};

struct PythonTableUdfKernelInit {
  PythonTableUdfKernelInit(std::shared_ptr<OwnedRefNoGIL> function_maker,
                           ScalarUdfWrapperCallback cb)
      : function_maker(function_maker), cb(cb) {
    Py_INCREF(function_maker->obj());
  }

  Result<std::unique_ptr<compute::KernelState>> operator()(
      compute::KernelContext* ctx, const compute::KernelInitArgs&) {
    ScalarUdfContext udf_context{ctx->memory_pool(), /*batch_length=*/0};
    std::unique_ptr<OwnedRefNoGIL> function;
    RETURN_NOT_OK(SafeCallIntoPython([this, &udf_context, &function] {
      OwnedRef empty_tuple(PyTuple_New(0));
      function = std::make_unique<OwnedRefNoGIL>(
          cb(function_maker->obj(), udf_context, empty_tuple.obj()));
      RETURN_NOT_OK(CheckPyError());
      return Status::OK();
    }));
    if (!PyCallable_Check(function->obj())) {
      return Status::TypeError("Expected a callable Python object.");
    }
    return std::make_unique<PythonScalarUdfKernelState>(
        std::move(function));
  }

  std::shared_ptr<OwnedRefNoGIL> function_maker;
  ScalarUdfWrapperCallback cb;
};

struct PythonUdf : public PythonScalarUdfKernelState {
  PythonUdf(std::shared_ptr<OwnedRefNoGIL> function, ScalarUdfWrapperCallback cb,
            compute::OutputType output_type)
      : PythonScalarUdfKernelState(function), cb(cb), output_type(output_type) {}

  ScalarUdfWrapperCallback cb;
  compute::OutputType output_type;

  Status Exec(compute::KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto state =
        ::arrow::internal::checked_cast<PythonScalarUdfKernelState*>(ctx->state());
    std::shared_ptr<OwnedRefNoGIL>& function = state->function;
    const int num_args = batch.num_values();
    ScalarUdfContext udf_context{ctx->memory_pool(), batch.length};

    OwnedRef arg_tuple(PyTuple_New(num_args));
    RETURN_NOT_OK(CheckPyError());
    for (int arg_id = 0; arg_id < num_args; arg_id++) {
      if (batch[arg_id].is_scalar()) {
        std::shared_ptr<Scalar> c_data = batch[arg_id].scalar->GetSharedPtr();
        PyObject* data = wrap_scalar(c_data);
        PyTuple_SetItem(arg_tuple.obj(), arg_id, data);
      } else {
        std::shared_ptr<Array> c_data = batch[arg_id].array.ToArray();
        PyObject* data = wrap_array(c_data);
        PyTuple_SetItem(arg_tuple.obj(), arg_id, data);
      }
    }

    OwnedRef result(cb(function->obj(), udf_context, arg_tuple.obj()));
    RETURN_NOT_OK(CheckPyError());
    // unwrapping the output for expected output type
    if (is_array(result.obj())) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> val, unwrap_array(result.obj()));
      ARROW_ASSIGN_OR_RAISE(TypeHolder type, output_type.Resolve(ctx, batch.GetTypes()));
      if (type.type == NULLPTR) {
        return Status::TypeError("expected output datatype is null");
      }
      if (*type.type != *val->type()) {
        return Status::TypeError("Expected output datatype ", type.type->ToString(),
                                 ", but function returned datatype ",
                                 val->type()->ToString());
      }
      out->value = std::move(val->data());
      return Status::OK();
    } else {
      return Status::TypeError("Unexpected output type: ", Py_TYPE(result.obj())->tp_name,
                               " (expected Array)");
    }
    return Status::OK();
  }
};

Status PythonUdfExec(compute::KernelContext* ctx, const ExecSpan& batch,
                     ExecResult* out) {
  auto udf = static_cast<PythonUdf*>(ctx->kernel()->data.get());
  return SafeCallIntoPython([&]() -> Status { return udf->Exec(ctx, batch, out); });
}

Status RegisterScalarLikeFunction(PyObject* user_function,
                                  compute::KernelInit kernel_init,
                                  ScalarUdfWrapperCallback wrapper,
                                  const ScalarUdfOptions& options,
                                  compute::FunctionRegistry* registry) {
  if (!PyCallable_Check(user_function)) {
    return Status::TypeError("Expected a callable Python object.");
  }
  auto scalar_func = std::make_shared<compute::ScalarFunction>(
      options.func_name, options.arity, options.func_doc);
  Py_INCREF(user_function);
  std::vector<compute::InputType> input_types;
  for (const auto& in_dtype : options.input_types) {
    input_types.emplace_back(in_dtype);
  }
  compute::OutputType output_type(options.output_type);
  auto udf_data = std::make_shared<PythonUdf>(
      std::make_shared<OwnedRefNoGIL>(user_function), wrapper, options.output_type);
  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(std::move(input_types), std::move(output_type),
                                     options.arity.is_varargs),
      PythonUdfExec, kernel_init);
  kernel.data = std::move(udf_data);

  kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  RETURN_NOT_OK(scalar_func->AddKernel(std::move(kernel)));
  if (registry == NULLPTR) {
    registry = compute::GetFunctionRegistry();
  }
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func)));
  return Status::OK();
}

}  // namespace

Status RegisterScalarFunction(PyObject* user_function, ScalarUdfWrapperCallback wrapper,
                              const ScalarUdfOptions& options,
                              compute::FunctionRegistry* registry) {
  return RegisterScalarLikeFunction(
      user_function,
      PythonScalarUdfKernelInit{std::make_shared<OwnedRefNoGIL>(user_function)}, wrapper,
      options, registry);
}

Status RegisterTabularFunction(PyObject* user_function, ScalarUdfWrapperCallback wrapper,
                               const ScalarUdfOptions& options,
                               compute::FunctionRegistry* registry) {
  if (options.arity.num_args != 0 || options.arity.is_varargs) {
    return Status::Invalid("tabular function must have no arguments");
  }
  return RegisterScalarLikeFunction(
      user_function,
      PythonTableUdfKernelInit{std::make_shared<OwnedRefNoGIL>(user_function), wrapper},
      wrapper, options, registry);
}

namespace  {

Result<std::shared_ptr<RecordBatch>> RecordBatchFromArray(
    std::shared_ptr<Schema> schema, std::shared_ptr<Array> array) {
  auto& data = const_cast<std::shared_ptr<ArrayData>&>(array->data());
  if (data->child_data.size() != static_cast<size_t>(schema->num_fields())) {
    return Status::Invalid("UDF result with shape not conforming to schema");
  }
  return RecordBatch::Make(std::move(schema), data->length, std::move(data->child_data));
}

}  // namespace

Result<RecordBatchIterator> GetRecordBatchesFromTabularFunction(
    const std::string& func_name, compute::FunctionRegistry* registry) {
  if (registry == NULLPTR) {
    registry = compute::GetFunctionRegistry();
  }
  ARROW_ASSIGN_OR_RAISE(auto func, registry->GetFunction(func_name));
  if (func->kind() != Function::SCALAR) {
    return Status::Invalid("tabular function of non-scalar kind");
  }
  auto arity = func->arity();
  if (arity.num_args != 0 || arity.is_varargs) {
    return Status::Invalid("tabular function of non-null arity");
  }
  auto kernels = ::arrow::internal::checked_pointer_cast<ScalarFunction>(func)->kernels();
  if (kernels.size() != 1) {
    return Status::Invalid("tabular function with non-single kernel");
  }
  const ScalarKernel* kernel = kernels[0];
  auto out_type = kernel->signature->out_type();
  if (out_type.kind() != OutputType::FIXED) {
    return Status::Invalid("tabular kernel of non-fixed kind");
  }
  auto datatype = out_type.type();
  if (datatype->id() != Type::type::STRUCT) {
    return Status::Invalid("tabular kernel with non-struct output");
  }
  auto fields = checked_cast<const StructType*>(datatype.get())->fields();
  auto schema = ::arrow::schema(fields);
  std::vector<TypeHolder> in_types;
  ARROW_ASSIGN_OR_RAISE(auto func_exec,
                        GetFunctionExecutor(func_name, in_types, NULLPTR, registry));
  auto next_func =
      [schema = std::move(schema),
       func_exec = std::move(func_exec)]() -> Result<std::shared_ptr<RecordBatch>> {
    std::vector<Datum> args;
    // passed_length of -1 or 0 with args.size() of 0 leads to an empty ExecSpanIterator
    // in exec.cc and to never invoking the source function, so 1 is passed instead
    ARROW_ASSIGN_OR_RAISE(auto datum, func_exec->Execute(args, /*passed_length=*/1));
    if (!datum.is_array()) {
      return Status::Invalid("UDF result of non-array kind");
    }
    std::shared_ptr<Array> array = datum.make_array();
    if (array->length() == 0) {
      return IterationTraits<std::shared_ptr<RecordBatch>>::End();
    }
    return RecordBatchFromArray(std::move(schema), std::move(array));
  };
  return MakeFunctionIterator(std::move(next_func));
}

}  // namespace py

}  // namespace arrow
