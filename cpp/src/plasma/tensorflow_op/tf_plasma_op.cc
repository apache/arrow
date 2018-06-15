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

#include "tensorflow/core/framework/device_base.h"
#include "tensorflow/core/framework/op.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/shape_inference.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/platform/mutex.h"
#include "tensorflow/stream_executor/device_memory.h"
#include "tensorflow/stream_executor/event.h"
#include "tensorflow/stream_executor/stream.h"

#ifdef GOOGLE_CUDA
#include "tensorflow/core/common_runtime/gpu/gpu_event_mgr.h"
#include "tensorflow/core/platform/stream_executor.h"
#endif

#include "arrow/python/tensor_util.h"
#include "plasma/client.h"

using namespace tensorflow;  // NOLINT

using ArrowStatus = arrow::Status;
using CPUDevice = Eigen::ThreadPoolDevice;
using GPUDevice = Eigen::GpuDevice;

static plasma::PlasmaClient client_;
static bool connected_ = false;
static mutex mu_;

using Event = perftools::gputools::Event;
using Stream = perftools::gputools::Stream;

// NOTE(zongheng): for some reason using unique_ptr or shared_ptr results in
// CUDA_ERROR_DEINITIALIZED on program exit.  I suspect this is because the
// static object's dtor gets called *after* TensorFlow's own CUDA cleanup.
// Instead, we use a raw pointer here and manually clean up in the Ops' dtors.
static Stream* d2h_stream = nullptr;
static mutex d2h_stream_mu;

// TODO(zongheng): CPU kernels' std::memcpy might be able to be sped up by
// parallelization.

arrow::Status TfDtypeToArrow(DataType dtype, std::shared_ptr<arrow::DataType>* out) {
  switch (dtype) {
    case DT_BOOL:
      *out = arrow::boolean();
      break;
    case DT_FLOAT:
      *out = arrow::float32();
      break;
    case DT_DOUBLE:
      *out = arrow::float64();
      break;
    case DT_HALF:
      *out = arrow::float16();
      break;
    case DT_INT8:
      *out = arrow::int8();
      break;
    case DT_INT16:
      *out = arrow::int16();
      break;
    case DT_INT32:
      *out = arrow::int32();
      break;
    case DT_INT64:
      *out = arrow::int64();
      break;
    case DT_UINT8:
      *out = arrow::uint8();
      break;
    case DT_UINT16:
      *out = arrow::uint16();
      break;
    case DT_UINT32:
      *out = arrow::uint32();
      break;
    case DT_UINT64:
      *out = arrow::uint64();
      break;
    case DT_BFLOAT16:
    case DT_COMPLEX64:
    case DT_COMPLEX128:
    case DT_INVALID:
    case DT_QINT8:
    case DT_QINT16:
    case DT_QINT32:
    case DT_QUINT8:
    case DT_QUINT16:
    case DT_RESOURCE:
    case DT_STRING:
    case DT_VARIANT:
    default:
      return arrow::Status(arrow::StatusCode::TypeError,
                           "Tensorflow data type is not supported");
  }
  return arrow::Status::OK();
}

arrow::Status ArrowDtypeToTf(std::shared_ptr<arrow::DataType> dtype, DataType* out) {
  switch (dtype->id()) {
    case arrow::Type::BOOL:
      *out = DT_BOOL;
      break;
    case arrow::Type::UINT8:
      *out = DT_UINT8;
      break;
    case arrow::Type::INT8:
      *out = DT_INT8;
      break;
    case arrow::Type::UINT16:
      *out = DT_UINT16;
      break;
    case arrow::Type::INT16:
      *out = DT_INT16;
      break;
    case arrow::Type::UINT32:
      *out = DT_UINT32;
      break;
    case arrow::Type::INT32:
      *out = DT_INT32;
      break;
    case arrow::Type::UINT64:
      *out = DT_UINT64;
      break;
    case arrow::Type::INT64:
      *out = DT_INT64;
      break;
    case arrow::Type::HALF_FLOAT:
      *out = DT_HALF;
      break;
    case arrow::Type::FLOAT:
      *out = DT_FLOAT;
      break;
    case arrow::Type::DOUBLE:
      *out = DT_DOUBLE;
      break;
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
    case arrow::Type::TIMESTAMP:
    case arrow::Type::TIME32:
    case arrow::Type::TIME64:
    case arrow::Type::INTERVAL:
    case arrow::Type::DECIMAL:
    case arrow::Type::LIST:
    case arrow::Type::STRUCT:
    case arrow::Type::UNION:
    case arrow::Type::DICTIONARY:
    case arrow::Type::MAP:
    default:
    return arrow::Status(arrow::StatusCode::TypeError,
                         "Arrow data type is not supported");
  }
  return arrow::Status::OK();
}

int64_t get_byte_width(std::shared_ptr<arrow::DataType> dtype) {
  return arrow::checked_cast<const arrow::FixedWidthType&>(*dtype).bit_width() / CHAR_BIT;
}

// Put:  tf.Tensor -> plasma.
template <typename Device>
class TensorToPlasmaOp : public AsyncOpKernel {
 public:
  explicit TensorToPlasmaOp(OpKernelConstruction* context) : AsyncOpKernel(context) {
    OP_REQUIRES_OK(context, context->GetAttr("plasma_store_socket_name",
                                             &plasma_store_socket_name_));
    OP_REQUIRES_OK(context, context->GetAttr("plasma_manager_socket_name",
                                             &plasma_manager_socket_name_));
    mutex_lock lock(mu_);
    if (!connected_) {
      VLOG(1) << "Connecting to Plasma...";
      ARROW_CHECK_OK(client_.Connect(plasma_store_socket_name_,
                                     plasma_manager_socket_name_,
                                     plasma::kPlasmaDefaultReleaseDelay));
      VLOG(1) << "Connected!";
      connected_ = true;
    }
  }

  ~TensorToPlasmaOp() override {
    {
      mutex_lock lock(mu_);
      ARROW_CHECK_OK(client_.Disconnect());
    }
    {
      mutex_lock lock(d2h_stream_mu);
      if (d2h_stream != nullptr) {
        delete d2h_stream;
      }
    }
  }

  void ComputeAsync(OpKernelContext* context, DoneCallback done) override {
    const int num_inputs = context->num_inputs();
    OP_REQUIRES_ASYNC(
        context, num_inputs >= 2,
        errors::InvalidArgument("Input should have at least 1 tensor and 1 object_id"),
        done);
    const int num_tensors = num_inputs - 1;

    // Check that all tensors have the same dtype
    DataType tf_dtype = context->input(0).dtype();
    for (int i = 1; i < num_inputs - 1; i++) {
      if (tf_dtype != context->input(i).dtype()) {
        ARROW_CHECK_OK(arrow::Status(arrow::StatusCode::TypeError,
                                     "All input tensors must have the same data type"));
      }
    }

    std::shared_ptr<arrow::DataType> arrow_dtype;
    ARROW_CHECK_OK(TfDtypeToArrow(tf_dtype, &arrow_dtype));
    int64_t byte_width = get_byte_width(arrow_dtype);

    std::vector<size_t> offsets;
    offsets.reserve(num_tensors + 1);
    offsets.push_back(0);
    int64_t total_bytes = 0;
    for (int i = 0; i < num_tensors; ++i) {
      const size_t s = context->input(i).TotalBytes();
      CHECK_EQ(s, context->input(i).NumElements() * byte_width);
      CHECK_GT(s, 0);
      total_bytes += s;
      offsets.push_back(total_bytes);
    }

    const Tensor& plasma_object_id = context->input(num_inputs - 1);
    CHECK_EQ(plasma_object_id.NumElements(), 1);
    const string& plasma_object_id_str = plasma_object_id.flat<std::string>()(0);
    VLOG(1) << "plasma_object_id_str: '" << plasma_object_id_str << "'";
    const plasma::ObjectID object_id =
        plasma::ObjectID::from_binary(plasma_object_id_str);

    std::vector<int64_t> shape = {total_bytes / byte_width};

    int64_t header_size;
    ARROW_CHECK_OK(
        arrow::py::TensorFlowTensorGetHeaderSize(arrow_dtype, shape, &header_size));

    std::shared_ptr<Buffer> data_buffer;
    {
      mutex_lock lock(mu_);
      ARROW_CHECK_OK(client_.Create(object_id,
                                    header_size + total_bytes,
                                    /*metadata=*/nullptr, 0, &data_buffer));
    }

    int64_t offset;
    ARROW_CHECK_OK(
        arrow::py::TensorFlowTensorWrite(arrow_dtype, shape, total_bytes, data_buffer, &offset));

    float* data = reinterpret_cast<float*>(data_buffer->mutable_data() + offset);

    auto wrapped_callback = [this, context, done, data_buffer, object_id]() {
      {
        mutex_lock lock(mu_);
        ARROW_CHECK_OK(client_.Seal(object_id));
      }
      context->SetStatus(tensorflow::Status::OK());
      done();
    };

    if (std::is_same<Device, CPUDevice>::value) {
      for (int i = 0; i < num_tensors; ++i) {
        const auto& input_tensor = context->input(i);
        std::memcpy(static_cast<void*>(data + offsets[i] / byte_width),
                    input_tensor.flat<float>().data(),
                    static_cast<uint64>(offsets[i + 1] - offsets[i]));
      }
      wrapped_callback();
    } else {
#ifdef GOOGLE_CUDA
      auto orig_stream = context->op_device_context()->stream();
      OP_REQUIRES_ASYNC(context, orig_stream != nullptr,
                        errors::Internal("No GPU stream available."), done);
      auto stream_executor = orig_stream->parent();

      // NOTE(zongheng): this is critical of getting good performance out of D2H
      // async memcpy.  Under the hood it performs cuMemHostRegister(), see:
      // http://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__MEM.html#group__CUDA__MEM_1gf0a9fe11544326dabd743b7aa6b54223
      CHECK(stream_executor->HostMemoryRegister(static_cast<void*>(data),
                                                static_cast<uint64>(total_bytes)));

      {
        mutex_lock l(d2h_stream_mu);
        if (d2h_stream == nullptr) {
          d2h_stream = new Stream(stream_executor);
          CHECK(d2h_stream->Init().ok());
        }
      }

      // Needed to make sure the input buffers have been computed.
      // NOTE(ekl): this is unnecessary when the op is behind a NCCL allreduce already
      CHECK(d2h_stream->ThenWaitFor(orig_stream).ok());

      for (int i = 0; i < num_tensors; ++i) {
        const auto& input_tensor = context->input(i);
        float* input_buffer = const_cast<float*>(input_tensor.flat<float>().data());
        perftools::gputools::DeviceMemoryBase wrapped_src(
            static_cast<void*>(input_buffer));
        const bool success =
            d2h_stream
                ->ThenMemcpy(static_cast<void*>(data + offsets[i] / sizeof(float)),
                             wrapped_src,
                             static_cast<uint64>(offsets[i + 1] - offsets[i]))
                .ok();
        OP_REQUIRES_ASYNC(context, success,
                          errors::Internal("D2H memcpy failed to be enqueued."), done);
      }
      context->device()->tensorflow_gpu_device_info()->event_mgr->ThenExecute(
          d2h_stream, std::move(wrapped_callback));
#endif
    }
  }

 private:
  std::string plasma_store_socket_name_;
  std::string plasma_manager_socket_name_;

  mutex mu_;
  bool connected_ = false;
  plasma::PlasmaClient client_ GUARDED_BY(mu_);
};

static Stream* h2d_stream = nullptr;
static mutex h2d_stream_mu;

// Get:  plasma -> tf.Tensor.
template <typename Device>
class PlasmaToTensorOp : public AsyncOpKernel {
 public:
  explicit PlasmaToTensorOp(OpKernelConstruction* context) : AsyncOpKernel(context) {
    OP_REQUIRES_OK(context, context->GetAttr("plasma_store_socket_name",
                                             &plasma_store_socket_name_));
    OP_REQUIRES_OK(context, context->GetAttr("plasma_manager_socket_name",
                                             &plasma_manager_socket_name_));
    mutex_lock lock(mu_);
    if (!connected_) {
      VLOG(1) << "Connecting to Plasma...";
      ARROW_CHECK_OK(client_.Connect(plasma_store_socket_name_,
                                     plasma_manager_socket_name_,
                                     plasma::kPlasmaDefaultReleaseDelay));
      VLOG(1) << "Connected!";
      connected_ = true;
    }
  }

  ~PlasmaToTensorOp() override {
    {
      mutex_lock lock(mu_);
      ARROW_CHECK_OK(client_.Disconnect());
    }
    {
      mutex_lock lock(h2d_stream_mu);
      if (h2d_stream != nullptr) {
        delete h2d_stream;
      }
    }
  }

  void ComputeAsync(OpKernelContext* context, DoneCallback done) override {
    const Tensor& plasma_object_id = context->input(0);
    CHECK_EQ(plasma_object_id.NumElements(), 1);
    const string& plasma_object_id_str = plasma_object_id.flat<std::string>()(0);

    VLOG(1) << "plasma_object_id_str: '" << plasma_object_id_str << "'";
    const plasma::ObjectID object_id =
        plasma::ObjectID::from_binary(plasma_object_id_str);

    plasma::ObjectBuffer object_buffer;
    {
      mutex_lock lock(mu_);
      // NOTE(zongheng): this is a blocking call.  We might want to (1) make
      // Plasma asynchronous, (2) launch a thread / event here ourselves, or
      // something like that...
      ARROW_CHECK_OK(client_.Get(&object_id, /*num_objects=*/1,
                                 /*timeout_ms=*/-1, &object_buffer));
    }

    std::shared_ptr<arrow::Tensor> tensor;
    ARROW_CHECK_OK(arrow::py::ReadTensor(object_buffer.data, &tensor));

    int64_t byte_width = get_byte_width(tensor->type());
    const int64_t size_in_bytes = tensor->data()->size();

    TensorShape shape({static_cast<int64_t>(size_in_bytes / byte_width)});

    const float* plasma_data = reinterpret_cast<const float*>(tensor->raw_data());

    Tensor* output_tensor = nullptr;
    OP_REQUIRES_OK_ASYNC(context, context->allocate_output(0, shape, &output_tensor),
                         done);

    if (std::is_same<Device, CPUDevice>::value) {
      std::memcpy(output_tensor->flat<float>().data(),
                  plasma_data,
                  size_in_bytes);
      done();
    } else {
#ifdef GOOGLE_CUDA
      auto orig_stream = context->op_device_context()->stream();
      OP_REQUIRES_ASYNC(context, orig_stream != nullptr,
                        errors::Internal("No GPU stream available."), done);
      auto stream_executor = orig_stream->parent();

      {
        mutex_lock l(h2d_stream_mu);
        if (h2d_stream == nullptr) {
          h2d_stream = new Stream(stream_executor);
          CHECK(h2d_stream->Init().ok());
        }
      }

      // Important.  See note in T2P op.
      // We don't check the return status since the host memory might've been
      // already registered (e.g., the TensorToPlasmaOp might've been run).
      stream_executor->HostMemoryRegister(
          const_cast<void*>(static_cast<const void*>(plasma_data)),
          static_cast<uint64>(size_in_bytes));

      perftools::gputools::DeviceMemoryBase wrapped_dst(
          static_cast<void*>(output_tensor->flat<float>().data()));
      const bool success =
          h2d_stream
              ->ThenMemcpy(&wrapped_dst, static_cast<const void*>(plasma_data),
                           static_cast<uint64>(size_in_bytes))
              .ok();
      OP_REQUIRES_ASYNC(context, success,
                        errors::Internal("H2D memcpy failed to be enqueued."), done);

      // Without this sync the main compute stream might proceed to use the
      // Tensor buffer, but its contents might still be in-flight from our
      // h2d_stream.
      CHECK(orig_stream->ThenWaitFor(h2d_stream).ok());

      context->device()->tensorflow_gpu_device_info()->event_mgr->ThenExecute(
          h2d_stream, std::move(done));
#endif
    }
  }

 private:
  std::string plasma_store_socket_name_;
  std::string plasma_manager_socket_name_;
};

REGISTER_OP("TensorToPlasma")
    .Input("input_tensor: dtypes")
    .Input("plasma_object_id: string")
    .Attr("dtypes: list(type)")
    .Attr("plasma_store_socket_name: string")
    .Attr("plasma_manager_socket_name: string");

REGISTER_KERNEL_BUILDER(Name("TensorToPlasma").Device(DEVICE_CPU),
                        TensorToPlasmaOp<CPUDevice>);
#ifdef GOOGLE_CUDA
REGISTER_KERNEL_BUILDER(Name("TensorToPlasma").Device(DEVICE_GPU),
                        TensorToPlasmaOp<GPUDevice>);
#endif

REGISTER_OP("PlasmaToTensor")
    .Input("plasma_object_id: string")
    .Output("tensor: float")
    .Attr("plasma_store_socket_name: string")
    .Attr("plasma_manager_socket_name: string");

REGISTER_KERNEL_BUILDER(Name("PlasmaToTensor").Device(DEVICE_CPU),
                        PlasmaToTensorOp<CPUDevice>);
#ifdef GOOGLE_CUDA
REGISTER_KERNEL_BUILDER(Name("PlasmaToTensor").Device(DEVICE_GPU),
                        PlasmaToTensorOp<GPUDevice>);
#endif
