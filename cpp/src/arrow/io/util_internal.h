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

#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {
namespace internal {

ARROW_EXPORT void CloseFromDestructor(FileInterface* file);

// Validate a (offset, size) region (as given to ReadAt) against
// the file size.  Return the actual read size.
ARROW_EXPORT Result<int64_t> ValidateReadRegion(int64_t offset, int64_t size,
                                                int64_t file_size);
// Validate a (offset, size) region (as given to WriteAt) against
// the file size.  Short writes are not allowed.
ARROW_EXPORT Status ValidateWriteRegion(int64_t offset, int64_t size, int64_t file_size);

// Validate a (offset, size) region (as given to ReadAt or WriteAt), without
// knowing the file size.
ARROW_EXPORT Status ValidateRegion(int64_t offset, int64_t size);

}  // namespace internal
}  // namespace io
}  // namespace arrow
