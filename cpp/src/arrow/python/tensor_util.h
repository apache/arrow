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

#include <memory>
#include <vector>

#include "arrow/tensor.h"

namespace arrow {

namespace py {

Status TensorFlowTensorGetHeaderSize(std::shared_ptr<arrow::DataType> dtype,
                                     const std::vector<int64_t>& shape,
                                     int64_t* header_size);

Status TensorFlowTensorWrite(std::shared_ptr<arrow::DataType> dtype,
                             const std::vector<int64_t>& shape, int64_t tensor_num_bytes,
                             std::shared_ptr<Buffer> buffer, int64_t* offset);

ARROW_EXPORT
Status ReadTensor(std::shared_ptr<Buffer> src, std::shared_ptr<Tensor>* out);

}  // namespace py

}  // namespace arrow
