#include "arrow/filesystem/azurefs_internal.h"

#include <azure/storage/files/datalake.hpp>

#include "arrow/result.h"

namespace arrow {
namespace fs {
namespace internal {

Status ErrorToStatus(const std::string& prefix,
                     const Azure::Storage::StorageException& exception) {
  return Status::IOError(prefix, " Azure Error: ", exception.what());
}

Status HierarchicalNamespaceDetector::Init(
    std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
        datalake_service_client) {
  datalake_service_client_ = datalake_service_client;
  return Status::OK();
};

Result<bool> HierarchicalNamespaceDetector::Enabled(const std::string& container_name) {
  // Hierarchical namespace can't easily be changed after the storage account is created
  // and its common across all containers in the storage account. Do nothing until we've
  // checked for a cached result.
  if (is_hierarchical_namespace_enabled_.has_value()) {
    return is_hierarchical_namespace_enabled_.value();
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
    is_hierarchical_namespace_enabled_ = true;
  } catch (const Azure::Storage::StorageException& exception) {
    // GetAccessControlList will fail on storage accounts without hierarchical
    // namespace enabled.

    if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::BadRequest ||
        exception.StatusCode == Azure::Core::Http::HttpStatusCode::Conflict) {
      // Flat namespace storage accounts with soft delete enabled return
      // Conflict - This endpoint does not support BlobStorageEvents or SoftDelete
      // otherwise it returns: BadRequest - This operation is only supported on a
      // hierarchical namespace account.
      is_hierarchical_namespace_enabled_ = false;
    } else if (exception.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
      // Azurite returns NotFound.
      try {
        // Ensure sure that the directory exists by checking its properties. If it
        // doesn't then the GetAccessControlList check was invalid and we can't tell
        // if the storage account has hierachical namespace.
        filesystem_client.GetProperties();
        is_hierarchical_namespace_enabled_ = false;
      } catch (const Azure::Storage::StorageException& exception) {
        return ErrorToStatus(
            "When getting properties '" + filesystem_client.GetUrl() + "': ", exception);
      }
    } else {
      // Unexpected error so we can't tell if the storage account has hierachical
      // namespace.
      return ErrorToStatus(
          "When getting access control list '" + directory_client.GetUrl() + "': ",
          exception);
    }
  }
  return is_hierarchical_namespace_enabled_.value();
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
