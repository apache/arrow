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

#pragma once

#include <memory>
#include <vector>

#include "arrow/ipc/options.h"
#include "arrow/python/visibility.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"

// Forward declaring PyObject, see
// https://mail.python.org/pipermail/python-dev/2003-August/037601.html
#ifndef PyObject_HEAD
struct _object;
typedef _object PyObject;
#endif

namespace arrow {

class Buffer;
class DataType;
class MemoryPool;
class RecordBatch;
class Tensor;

namespace io {

class OutputStream;

}  // namespace io

namespace py {

struct ARROW_PYTHON_EXPORT SerializedPyObject {
  std::shared_ptr<RecordBatch> batch;
  std::vector<std::shared_ptr<Tensor>> tensors;
  std::vector<std::shared_ptr<SparseTensor>> sparse_tensors;
  std::vector<std::shared_ptr<Tensor>> ndarrays;
  std::vector<std::shared_ptr<Buffer>> buffers;
  ipc::IpcWriteOptions ipc_options;

  SerializedPyObject();

  /// \brief Write serialized Python object to OutputStream
  /// \param[in,out] dst an OutputStream
  /// \return Status
  Status WriteTo(io::OutputStream* dst);

  /// \brief Convert SerializedPyObject to a dict containing the message
  /// components as Buffer instances with minimal memory allocation
  ///
  /// {
  ///   'num_tensors': M,
  ///   'num_sparse_tensors': N,
  ///   'num_buffers': K,
  ///   'data': [Buffer]
  /// }
  ///
  /// Each tensor is written as two buffers, one for the metadata and one for
  /// the body. Therefore, the number of buffers in 'data' is 2 * M + 2 * N + K + 1,
  /// with the first buffer containing the serialized record batch containing
  /// the UnionArray that describes the whole object
  Status GetComponents(MemoryPool* pool, PyObject** out);
};

struct PythonType {
  enum type {
    NONE,
    BOOL,
    INT,
    PY2INT,  // Kept for compatibility
    BYTES,
    STRING,
    HALF_FLOAT,
    FLOAT,
    DOUBLE,
    DATE64,
    LIST,
    DICT,
    TUPLE,
    SET,
    TENSOR,
    NDARRAY,
    BUFFER,
    SPARSECOOTENSOR,
    SPARSECSRMATRIX,
    SPARSECSCMATRIX,
    SPARSECSFTENSOR,
    NUM_PYTHON_TYPES
  };
};

}  // namespace py

}  // namespace arrow
