#include "tensorflow/core/framework/op.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/shape_inference.h"

#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/tensor.h"
#include "plasma/client.h"

using namespace tensorflow;

REGISTER_OP("PlasmaData")
    .Input("object_id: string")
    .Output("output: float32")
    .Attr("socket: string");

// TODO(pcm): Make this zero-copy if possible

class PlasmaDataOp : public OpKernel {
 public:
  explicit PlasmaDataOp(OpKernelConstruction* context) : OpKernel(context) {
    std::cout << "called constructor" << std::endl;
    std::string socket;
    OP_REQUIRES_OK(context, context->GetAttr("socket", &socket));
    // Connect to plasma
    ARROW_CHECK_OK(client_.Connect(socket, "", PLASMA_DEFAULT_RELEASE_DELAY));
    std::cout << "constructor finished" << std::endl;
  }

  void Compute(OpKernelContext* context) override {
    // Grab the input tensor
    const Tensor& input_tensor = context->input(0);
    auto input = input_tensor.flat<string>();

    // Get the object
    plasma::ObjectID object_id = plasma::ObjectID::from_binary(input(0));
    plasma::ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client_.Get(&object_id, 1, -1, &object_buffer));

    // Get the tensor
    std::shared_ptr<arrow::Tensor> result;
    arrow::io::BufferReader reader(object_buffer.data, object_buffer.data_size);
    int64_t offset;
    ARROW_CHECK_OK(reader.Tell(&offset));
    ARROW_CHECK_OK(arrow::ipc::ReadTensor(0, &reader, &result));

    std::cout << "shape is" << result->shape()[0] << " , " << result->shape()[1]
              << std::endl;

    // Create an output tensor
    TensorShape shape(result->shape());
    Tensor* output_tensor = NULL;
    OP_REQUIRES_OK(context, context->allocate_output(0, shape, &output_tensor));
    auto output_flat = output_tensor->flat<float>();

    // Set all but the first element of the output tensor to 0.
    const int64_t N = result->size();
    std::cout << "size is " << N << std::endl;
    const float* data = reinterpret_cast<const float*>(result->data()->data());
    for (int i = 0; i < N; i++) {
      output_flat(i) = data[i];
    }
  }
  ~PlasmaDataOp() { ARROW_CHECK_OK(client_.Disconnect()); }

 private:
  plasma::PlasmaClient client_;
};

REGISTER_KERNEL_BUILDER(Name("PlasmaData").Device(DEVICE_CPU), PlasmaDataOp);
