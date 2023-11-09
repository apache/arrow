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

#include "arrow/filesystem/azurefs_internal.h"

#include <azure/storage/files/datalake.hpp>

#include "arrow/result.h"

namespace arrow::fs::internal {

Status ExceptionToStatus(const std::string& prefix,
                         const Azure::Storage::StorageException& exception) {
  return Status::IOError(prefix, " Azure Error: ", exception.what());
}

Status HierarchicalNamespaceDetector::Init(
    Azure::Storage::Files::DataLake::DataLakeServiceClient* datalake_service_client) {
  datalake_service_client_ = datalake_service_client;
  return Status::OK();
}

Result<bool> HierarchicalNamespaceDetector::Enabled(const std::string& container_name) {
  // Hierarchical namespace can't easily be changed after the storage account is created
  // and its common across all containers in the storage account. Do nothing until we've
  // checked for a cached result.
  if (enabled_.has_value()) {
    return enabled_.value();
  }

  // This approach is inspired by hadoop-azure
  // https://github.com/apache/hadoop/blob/7c6af6a5f626d18d68b656d085cc23e4c1f7a1ef/hadoop-tools/hadoop-azure/src/main/java/org/apache/hadoop/fs/azurebfs/AzureBlobFileSystemStore.java#L356.
  // Unfortunately `blob_service_client->GetAccountInfo()` requires significantly
  // elevated permissions.
  // https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties?tabs=azure-ad#authorization
  auto filesystem_client = datalake_service_client_->GetFileSystemClient(container_name);
  auto directory_client = filesystem_client.GetDirectoryClient("/");
  try {
    directory_client.GetAccessControlList();
    enabled_ = true;
  } catch (const Azure::Storage::StorageException& exception) {
    // GetAccessControlList will fail on storage accounts without hierarchical
    // namespace enabled.

    if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::BadRequest ||
        exception.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict) {
      // Flat namespace storage accounts with soft delete enabled return
      // Conflict - This endpoint does not support BlobStorageEvents or SoftDelete
      // otherwise it returns: BadRequest - This operation is only supported on a
      // hierarchical namespace account.
      enabled_ = false;
    } else if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
      // Azurite returns NotFound.
      try {
        filesystem_client.GetProperties();
        enabled_ = false;
      } catch (const Azure::Storage::StorageException& exception) {
        return ExceptionToStatus("Failed to confirm '" + filesystem_client.GetUrl() +
                                     "' is an accessible container. Therefore the "
                                     "hierarchical namespace check was invalid.",
                                 exception);
      }
    } else {
      return ExceptionToStatus(
          "GetAccessControlList for '" + directory_client.GetUrl() +
              "' failed with an unexpected Azure error, while checking "
              "whether the storage account has hierarchical namespace enabled.",
          exception);
    }
  }
  return enabled_.value();
}

}  // namespace arrow::fs::internal
