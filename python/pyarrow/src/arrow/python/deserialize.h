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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/python/serialize.h"
#include "arrow/python/visibility.h"
#include "arrow/status.h"

namespace arrow {

class RecordBatch;
class Tensor;

namespace io {

class RandomAccessFile;

}  // namespace io

namespace py {

struct ARROW_PYTHON_EXPORT SparseTensorCounts {
  int coo;
  int csr;
  int csc;
  int csf;
  int ndim_csf;

  int num_total_tensors() const { return coo + csr + csc + csf; }
  int num_total_buffers() const {
    return coo * 3 + csr * 4 + csc * 4 + 2 * ndim_csf + csf;
  }
};

}  // namespace py
}  // namespace arrow
