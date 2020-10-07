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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/util/key_value_metadata.h>

inline std::shared_ptr<arrow::KeyValueMetadata> KeyValueMetadata__Make(
    const cpp11::strings& metadata) {
  // TODO(bkietz): Make this a custom conversion after
  // https://github.com/r-lib/cpp11/pull/104
  return std::make_shared<arrow::KeyValueMetadata>(
      cpp11::as_cpp<std::vector<std::string>>(metadata.names()),
      cpp11::as_cpp<std::vector<std::string>>(metadata));
}

inline cpp11::writable::strings KeyValueMetadata__as_vector(
    const std::shared_ptr<const arrow::KeyValueMetadata>& metadata) {
  cpp11::writable::strings metadata_vector(metadata->values());
  metadata_vector.names() = cpp11::writable::strings(metadata->keys());
  return metadata_vector;
}
#endif
