
#include <azure/storage/files/datalake.hpp>

#include "arrow/result.h"

namespace arrow {
namespace fs {
namespace internal {

Status ErrorToStatus(const std::string& prefix,
                     const Azure::Storage::StorageException& exception);

class HierarchicalNamespaceDetector {
 public:
  Status Init(std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
                  datalake_service_client);
  Result<bool> Enabled(const std::string& container_name);

 private:
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient>
      datalake_service_client_;
  std::optional<bool> is_hierarchical_namespace_enabled_;
};

}  // namespace internal
}  // namespace fs
}  // namespace arrow
