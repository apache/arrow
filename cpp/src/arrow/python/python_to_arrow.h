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

#ifndef ARROW_PYTHON_PYTHON_TO_ARROW_H
#define ARROW_PYTHON_PYTHON_TO_ARROW_H

#include "arrow/python/platform.h"

#include <memory>
#include <vector>

#include "arrow/status.h"

namespace arrow {

class RecordBatch;
class Tensor;

namespace io {

class OutputStream;

}  // namespace io

namespace py {

void set_serialization_callbacks(PyObject* serialize_callback,
                                 PyObject* deserialize_callback);

// This acquires the GIL
Status SerializePythonSequence(PyObject* sequence,
                               std::shared_ptr<RecordBatch>* batch_out,
                               std::vector<std::shared_ptr<Tensor>>* tensors_out);

Status WriteSerializedPythonSequence(std::shared_ptr<RecordBatch> batch,
                                     std::vector<std::shared_ptr<Tensor>> tensors,
                                     io::OutputStream* dst);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_PYTHON_TO_ARROW_H
