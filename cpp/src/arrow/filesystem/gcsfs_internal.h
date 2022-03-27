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

#include <google/cloud/status.h>
#include <google/cloud/storage/object_metadata.h>
#include <google/cloud/storage/well_known_headers.h>
#include <google/cloud/storage/well_known_parameters.h>

#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/filesystem.h"

namespace arrow {
namespace fs {
namespace internal {

Status ToArrowStatus(const google::cloud::Status& s);

int ErrnoFromStatus(const google::cloud::Status& s);

Result<google::cloud::storage::EncryptionKey> ToEncryptionKey(
    const std::shared_ptr<const KeyValueMetadata>& metadata);

Result<google::cloud::storage::PredefinedAcl> ToPredefinedAcl(
    const std::shared_ptr<const KeyValueMetadata>& metadata);

Result<google::cloud::storage::KmsKeyName> ToKmsKeyName(
    const std::shared_ptr<const KeyValueMetadata>& metadata);

Result<google::cloud::storage::WithObjectMetadata> ToObjectMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata);

Result<std::shared_ptr<const KeyValueMetadata>> FromObjectMetadata(
    google::cloud::storage::ObjectMetadata const& m);

std::int64_t Depth(arrow::util::string_view path);

}  // namespace internal
}  // namespace fs
}  // namespace arrow
