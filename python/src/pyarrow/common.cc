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

#include "pyarrow/common.h"

#include <cstdlib>
#include <mutex>
#include <sstream>

#include "arrow/memory_pool.h"
#include "arrow/status.h"

using arrow::Status;

namespace pyarrow {

class PyArrowMemoryPool : public arrow::MemoryPool {
 public:
  PyArrowMemoryPool() : bytes_allocated_(0) {}
  virtual ~PyArrowMemoryPool() {}

  Status Allocate(int64_t size, uint8_t** out) override {
    std::lock_guard<std::mutex> guard(pool_lock_);
    *out = static_cast<uint8_t*>(std::malloc(size));
    if (*out == nullptr) {
      std::stringstream ss;
      ss << "malloc of size " << size << " failed";
      return Status::OutOfMemory(ss.str());
    }

    bytes_allocated_ += size;

    return Status::OK();
  }

  int64_t bytes_allocated() const override {
    std::lock_guard<std::mutex> guard(pool_lock_);
    return bytes_allocated_;
  }

  void Free(uint8_t* buffer, int64_t size) override {
    std::lock_guard<std::mutex> guard(pool_lock_);
    std::free(buffer);
    bytes_allocated_ -= size;
  }

 private:
  mutable std::mutex pool_lock_;
  int64_t bytes_allocated_;
};

arrow::MemoryPool* get_memory_pool() {
  static PyArrowMemoryPool memory_pool;
  return &memory_pool;
}

// ----------------------------------------------------------------------
// PyBytesBuffer

PyBytesBuffer::PyBytesBuffer(PyObject* obj)
    : Buffer(reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(obj)),
          PyBytes_GET_SIZE(obj)),
      obj_(obj) {
  Py_INCREF(obj_);
}

PyBytesBuffer::~PyBytesBuffer() {
  PyGILGuard lock;
  Py_DECREF(obj_);
}

}  // namespace pyarrow
