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

// Functions for converting between CPython built-in data structures and Arrow
// data structures

#ifndef ARROW_PYTHON_ADAPTERS_BUILTIN_H
#define ARROW_PYTHON_ADAPTERS_BUILTIN_H

#include "arrow/python/platform.h"

#include <cstdint>
#include <memory>

#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#include "arrow/python/common.h"

namespace arrow {

class Array;
class Status;

namespace py {

struct PyConversionOptions {
  PyConversionOptions() : type(NULLPTR), size(-1), pool(NULLPTR), from_pandas(false) {}

  PyConversionOptions(const std::shared_ptr<DataType>& type, int64_t size,
                      MemoryPool* pool, bool from_pandas)
      : type(type), size(size), pool(default_memory_pool()), from_pandas(from_pandas) {}

  // Set to null if to be inferred
  std::shared_ptr<DataType> type;

  // Default is -1: infer from data
  int64_t size;

  // Memory pool to use for allocations
  MemoryPool* pool;

  // Default false
  bool from_pandas;
};

/// \brief Convert sequence (list, generator, NumPy array with dtype object) of
/// Python objects.
/// \param[in] obj the sequence to convert
/// \param[in] mask a NumPy array of true/false values to indicate whether
/// values in the sequence are null (true) or not null (false). This parameter
/// may be null
/// \param[in] options various conversion options
/// \param[out] out a ChunkedArray containing one or more chunks
/// \return Status
ARROW_EXPORT
Status ConvertPySequence(PyObject* obj, PyObject* mask,
                         const PyConversionOptions& options,
                         std::shared_ptr<ChunkedArray>* out);

ARROW_EXPORT
Status ConvertPySequence(PyObject* obj, const PyConversionOptions& options,
                         std::shared_ptr<ChunkedArray>* out);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_ADAPTERS_BUILTIN_H
