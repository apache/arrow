#include <torch/torch.h>
#include <cuda.h>
#include <cuda_runtime.h>

using namespace at;
using namespace std;

Tensor load(uintptr_t data, std::vector<int64_t> data_size) {
  auto f = torch::CUDA(kFloat).tensorFromBlob(reinterpret_cast<void*>(data), data_size);
  cout << f << endl;
  return f;
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
  m.def("load", &load, "load data");
}
