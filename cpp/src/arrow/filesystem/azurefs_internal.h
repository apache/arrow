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

#include "arrow/result.h"

namespace Azure::Storage::Files::DataLake {
class DataLakeFileSystemClient;
class DataLakeServiceClient;
}  // namespace Azure::Storage::Files::DataLake

namespace arrow::fs {

struct AzureOptions;

namespace internal {

enum class HierarchicalNamespaceSupport {
  kUnknown = 0,
  kContainerNotFound = 1,
  kDisabled = 2,
  kEnabled = 3,
};

/// \brief Performs a request to check if the storage account has Hierarchical
/// Namespace support enabled.
///
/// This check requires a DataLakeFileSystemClient for any container of the
/// storage account. If the container doesn't exist yet, we just forward that
/// error to the caller (kContainerNotFound) since that's a proper error to the operation
/// on that container anyways -- no need to try again with or without the knowledge of
/// Hierarchical Namespace support.
///
/// Hierarchical Namespace support can't easily be changed after the storage account is
/// created and the feature is shared by all containers in the storage account.
/// This means the result of this check can (and should!) be cached as soon as
/// it returns a successful result on any container of the storage account (see
/// AzureFileSystem::Impl).
///
/// The check consists of a call to DataLakeFileSystemClient::GetAccessControlList()
/// on the root directory of the container. An approach taken by the Hadoop Azure
/// project [1]. A more obvious approach would be to call
/// BlobServiceClient::GetAccountInfo(), but that endpoint requires elevated
/// permissions [2] that we can't generally rely on.
///
/// [1]:
/// https://github.com/apache/hadoop/blob/7c6af6a5f626d18d68b656d085cc23e4c1f7a1ef/hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azurebfs/AzureBlobFileSystemStore.java#L356.
/// [2]:
/// https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties?tabs=azure-ad#authorization
///
/// IMPORTANT: If the result is kEnabled or kDisabled, it doesn't necessarily mean that
/// the container exists.
///
/// \param adlfs_client A DataLakeFileSystemClient for a container of the storage
/// account.
/// \return kEnabled/kDisabled/kContainerNotFound (kUnknown is never
/// returned).
ARROW_EXPORT Result<HierarchicalNamespaceSupport> CheckIfHierarchicalNamespaceIsEnabled(
    const Azure::Storage::Files::DataLake::DataLakeFileSystemClient& adlfs_client,
    const arrow::fs::AzureOptions& options);

}  // namespace internal
}  // namespace arrow::fs
