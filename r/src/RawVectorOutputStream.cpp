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

#include "RawVectorOutputStream.h"
#include <Rcpp.h>

using namespace arrow;
using namespace Rcpp;

Status RawVectorOutputStream::Close() { return Status::OK(); }

Status RawVectorOutputStream::Tell(int64_t* position) const {
  *position = extent_bytes_written_;
  return Status::OK();
}

Status RawVectorOutputStream::Write(const void* data, int64_t nbytes) {
  if (extent_bytes_written_ + nbytes > buffer_.size()) {
    return Status::CapacityError("Not enough memory allocated for fixed size buffer.");
  }

  std::copy(reinterpret_cast<const uint8_t*>(data),
            reinterpret_cast<const uint8_t*>(data) + nbytes,
            buffer_.begin() + extent_bytes_written_);

  extent_bytes_written_ += nbytes;

  return Status::OK();
}
