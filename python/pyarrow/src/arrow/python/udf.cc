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
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/function.h"
#include "arrow/python/common.h"

// TODO REMOVE
#include <iostream>

namespace arrow {

using compute::ExecResult;
using compute::ExecSpan;

namespace py {

namespace {

void SetUpPythonArgs(int num_args, const ExecSpan& batch, OwnedRef& arg_tuple) {
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
}

struct PythonUdf : public compute::KernelState {
  ScalarUdfWrapperCallback cb;
  std::shared_ptr<OwnedRefNoGIL> function;
  std::shared_ptr<DataType> output_type;

  PythonUdf(ScalarUdfWrapperCallback cb, std::shared_ptr<OwnedRefNoGIL> function,
            const std::shared_ptr<DataType>& output_type)
      : cb(cb), function(function), output_type(output_type) {}

  // function needs to be destroyed at process exit
  // and Python may no longer be initialized.
  ~PythonUdf() {
    if (_Py_IsFinalizing()) {
      function->detach();
    }
  }

  Status Exec(compute::KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const int num_args = batch.num_values();
    ScalarUdfContext udf_context{ctx->memory_pool(), batch.length};

    OwnedRef arg_tuple(PyTuple_New(num_args));
    RETURN_NOT_OK(CheckPyError());
    SetUpPythonArgs(num_args, batch, arg_tuple);
    OwnedRef result(cb(function->obj(), udf_context, arg_tuple.obj()));
    RETURN_NOT_OK(CheckPyError());
    // unwrapping the output for expected output type
    if (is_array(result.obj())) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> val, unwrap_array(result.obj()));
      if (!output_type->Equals(*val->type())) {
        return Status::TypeError("Expected output datatype ", output_type->ToString(),
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

}  // namespace

Status RegisterScalarFunction(PyObject* user_function, ScalarUdfWrapperCallback wrapper,
                              const ScalarUdfOptions& options) {
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
      wrapper, std::make_shared<OwnedRefNoGIL>(user_function), options.output_type);
  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(std::move(input_types), std::move(output_type),
                                     options.arity.is_varargs),
      PythonUdfExec);
  kernel.data = std::move(udf_data);

  kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  RETURN_NOT_OK(scalar_func->AddKernel(std::move(kernel)));
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func)));
  return Status::OK();
}

// Scalar Aggregate Functions

struct ScalarUdfAggregator : public compute::KernelState {
  virtual Status Consume(compute::KernelContext* ctx, const compute::ExecSpan& batch) = 0;
  virtual Status MergeFrom(compute::KernelContext* ctx, compute::KernelState&& src) = 0;
  virtual Status Finalize(compute::KernelContext* ctx, Datum* out) = 0;
};

arrow::Status AggregateUdfConsume(compute::KernelContext* ctx, const compute::ExecSpan& batch) {
  return arrow::internal::checked_cast<ScalarUdfAggregator*>(ctx->state())
      ->Consume(ctx, batch);
}

arrow::Status AggregateUdfMerge(compute::KernelContext* ctx, compute::KernelState&& src,
                                compute::KernelState* dst) {
  return arrow::internal::checked_cast<ScalarUdfAggregator*>(dst)->MergeFrom(
      ctx, std::move(src));
}

arrow::Status AggregateUdfFinalize(compute::KernelContext* ctx, arrow::Datum* out) {
  return arrow::internal::checked_cast<ScalarUdfAggregator*>(ctx->state())
      ->Finalize(ctx, out);
}

ScalarAggregateUdfContext::~ScalarAggregateUdfContext() {
  if (_Py_IsFinalizing()) {
      Py_DECREF(this->state);
    }
}

struct PythonScalarUdfAggregatorImpl : public ScalarUdfAggregator {

  ScalarAggregateInitUdfWrapperCallback init_cb;
  ScalarAggregateConsumeUdfWrapperCallback consume_cb;
  ScalarAggregateMergeUdfWrapperCallback merge_cb;
  ScalarAggregateFinalizeUdfWrapperCallback finalize_cb;
  std::shared_ptr<OwnedRefNoGIL> init_function;
  std::shared_ptr<OwnedRefNoGIL> consume_function;
  std::shared_ptr<OwnedRefNoGIL> merge_function;
  std::shared_ptr<OwnedRefNoGIL> finalize_function;
  std::shared_ptr<DataType> output_type;


  PythonScalarUdfAggregatorImpl(ScalarAggregateInitUdfWrapperCallback init_cb,
   ScalarAggregateConsumeUdfWrapperCallback consume_cb,
   ScalarAggregateMergeUdfWrapperCallback merge_cb,
   ScalarAggregateFinalizeUdfWrapperCallback finalize_cb,
   std::shared_ptr<OwnedRefNoGIL> init_function,
   std::shared_ptr<OwnedRefNoGIL> consume_function,
   std::shared_ptr<OwnedRefNoGIL> merge_function,
   std::shared_ptr<OwnedRefNoGIL> finalize_function,
            const std::shared_ptr<DataType>& output_type) : init_cb(init_cb),
            consume_cb(consume_cb),
            merge_cb(merge_cb),
            finalize_cb(finalize_cb),
            init_function(init_function),
            consume_function(consume_function),
            merge_function(merge_function),
            finalize_function(finalize_function),
            output_type(output_type) {
              Init(init_cb, init_function);
            }

  ~PythonScalarUdfAggregatorImpl() {
    if (_Py_IsFinalizing()) {
      init_function->detach();
      consume_function->detach();
      merge_function->detach();
      finalize_function->detach();
    }
  }

  void Init(ScalarAggregateInitUdfWrapperCallback& init_cb , std::shared_ptr<OwnedRefNoGIL>& init_function) {
    auto st =  SafeCallIntoPython([&]() -> Status { 
      OwnedRef result(init_cb(init_function->obj()));
      PyObject* init_res = result.obj();
      Py_INCREF(init_res);
      this->udf_context_ = ScalarAggregateUdfContext{default_memory_pool(), std::move(init_res)};
      this->owned_state_.reset(result.obj());
      RETURN_NOT_OK(CheckPyError());
      return Status::OK();
    });
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
  }

  Status ConsumeBatch(compute::KernelContext* ctx, const compute::ExecSpan& batch) {
    const int num_args = batch.num_values();
    Py_INCREF(this->udf_context_.state);
    this->udf_context_.state = this->owned_state_.obj();
    // TODO: think about guaranteeing DRY (following logic already used in ScalarUDFs)
    OwnedRef arg_tuple(PyTuple_New(num_args));
    RETURN_NOT_OK(CheckPyError());
    SetUpPythonArgs(num_args, batch, arg_tuple);
    OwnedRef result(consume_cb(consume_function->obj(), this->udf_context_, arg_tuple.obj()));
    RETURN_NOT_OK(CheckPyError());
    PyObject* consume_res = result.obj();
    Py_INCREF(consume_res);
    this->owned_state_.reset(consume_res);
    Py_INCREF(this->udf_context_.state);
    this->udf_context_.state = this->owned_state_.obj();
    RETURN_NOT_OK(CheckPyError());
    return Status::OK();
  }
  
  Status Consume(compute::KernelContext* ctx, const compute::ExecSpan& batch) override {
    RETURN_NOT_OK(SafeCallIntoPython([&]() -> Status { 
      RETURN_NOT_OK(ConsumeBatch(ctx, batch));
      return Status::OK();
    }));
    return Status::OK();
  }

  Status MergeFrom(compute::KernelContext* ctx, compute::KernelState&& src) override {
    const auto& other_state = arrow::internal::checked_cast<const PythonScalarUdfAggregatorImpl&>(src);
    return SafeCallIntoPython([&]() -> Status {
      OwnedRef result(merge_cb(merge_function->obj(), 
        this->udf_context_, other_state.owned_state_.obj()));
      RETURN_NOT_OK(CheckPyError());
      PyObject* merge_res = result.obj();
      Py_INCREF(merge_res);
      this->owned_state_.reset(merge_res);
      Py_INCREF(this->udf_context_.state);
      this->udf_context_.state = this->owned_state_.obj();
      return Status::OK();
    });  
  }

  Status Finalize(compute::KernelContext* ctx, arrow::Datum* out) override {
    return SafeCallIntoPython([&]() -> Status { 
        OwnedRef result(finalize_cb(finalize_function->obj(), this->udf_context_));
        RETURN_NOT_OK(CheckPyError());
        // unwrapping the output for expected output type
        if (is_array(result.obj())) {
          ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> val, unwrap_array(result.obj()));
          if (!output_type->Equals(*val->type())) {
            return Status::TypeError("Expected output datatype ", output_type->ToString(),
                                    ", but function returned datatype ",
                                    val->type()->ToString());
          }
          *out = Datum(std::move(val));
          return Status::OK();
        } else {
          return Status::TypeError("Unexpected output type: ", Py_TYPE(result.obj())->tp_name,
                                  " (expected Array)");
        }
      });
  };

private:
  OwnedRefNoGIL owned_state_;
  ScalarAggregateUdfContext udf_context_;
};

Status AddAggKernel(std::shared_ptr<compute::KernelSignature> sig, compute::KernelInit init,
                           compute::ScalarAggregateFunction* func) {
  compute::ScalarAggregateKernel kernel(std::move(sig), std::move(init), AggregateUdfConsume,
                                   AggregateUdfMerge, AggregateUdfFinalize);
  RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
  return Status::OK();
}

Status RegisterScalarAggregateFunction(PyObject* consume_function,
                                                  ScalarAggregateConsumeUdfWrapperCallback consume_wrapper,
                                                  PyObject* merge_function,
                                                  ScalarAggregateMergeUdfWrapperCallback merge_wrapper,
                                                  PyObject* finalize_function,
                                                  ScalarAggregateFinalizeUdfWrapperCallback finalize_wrapper,
                                                  PyObject* init_function,
                                                  ScalarAggregateInitUdfWrapperCallback init_wrapper,
                                                  const ScalarUdfOptions& options) {
  if (!PyCallable_Check(consume_function) || !PyCallable_Check(merge_function) || !PyCallable_Check(finalize_function)) {
    return Status::TypeError("Expected a callable Python object.");
  }
  static auto default_scalar_aggregate_options = compute::ScalarAggregateOptions::Defaults();
  auto aggregate_func = std::make_shared<compute::ScalarAggregateFunction>(
      options.func_name, options.arity, options.func_doc, &default_scalar_aggregate_options);
  
  Py_INCREF(consume_function);
  Py_INCREF(merge_function);
  Py_INCREF(finalize_function);

  std::vector<compute::InputType> input_types;
  for (const auto& in_dtype : options.input_types) {
    input_types.emplace_back(in_dtype);
  }
  compute::OutputType output_type(options.output_type);

  auto init = [init_wrapper, consume_wrapper, merge_wrapper, finalize_wrapper,
    init_function, consume_function, merge_function, finalize_function, options](
                  compute::KernelContext* ctx,
                  const compute::KernelInitArgs& args) -> Result<std::unique_ptr<compute::KernelState>> {
    return std::make_unique<PythonScalarUdfAggregatorImpl>(
      init_wrapper,
      consume_wrapper,
      merge_wrapper,
      finalize_wrapper, 
      std::make_shared<OwnedRefNoGIL>(init_function), 
      std::make_shared<OwnedRefNoGIL>(consume_function),
      std::make_shared<OwnedRefNoGIL>(merge_function),
      std::make_shared<OwnedRefNoGIL>(finalize_function), 
      options.output_type);
  };

  RETURN_NOT_OK(
      AddAggKernel(compute::KernelSignature::Make(input_types, output_type),
                   init, aggregate_func.get()));
  
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(aggregate_func)));
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
