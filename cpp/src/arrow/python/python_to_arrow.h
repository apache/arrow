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

#include "arrow/python/common.h"
#include "arrow/python/pyarrow.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class RecordBatch;
class Tensor;

namespace io {

class OutputStream;

}  // namespace io

namespace py {

struct ARROW_EXPORT SerializedPyObject {
  std::shared_ptr<RecordBatch> batch;
  std::vector<std::shared_ptr<Tensor>> tensors;
  std::vector<std::shared_ptr<Buffer>> buffers;

  /// \brief Write serialized Python object to OutputStream
  /// \param[in,out] dst an OutputStream
  /// \return Status
  Status WriteTo(io::OutputStream* dst);

  /// \brief Convert SerializedPyObject to a dict containing the message
  /// components as Buffer instances with minimal memory allocation
  ///
  /// {
  ///   'num_tensors': N,
  ///   'num_buffers': K,
  ///   'data': [Buffer]
  /// }
  ///
  /// Each tensor is written as two buffers, one for the metadata and one for
  /// the body. Therefore, the number of buffers in 'data' is 2 * N + K + 1,
  /// with the first buffer containing the serialized record batch containing
  /// the UnionArray that describes the whole object
  Status GetComponents(MemoryPool* pool, PyObject** out);
};

/// \brief Serialize Python sequence as a RecordBatch plus
/// \param[in] context Serialization context which contains custom serialization
/// and deserialization callbacks. Can be any Python object with a
/// _serialize_callback method for serialization and a _deserialize_callback
/// method for deserialization. If context is None, no custom serialization
/// will be attempted.
/// \param[in] sequence a Python sequence object to serialize to Arrow data
/// structures
/// \param[out] out the serialized representation
/// \return Status
///
/// Release GIL before calling
ARROW_EXPORT
Status SerializeObject(PyObject* context, PyObject* sequence, SerializedPyObject* out);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_PYTHON_TO_ARROW_H
