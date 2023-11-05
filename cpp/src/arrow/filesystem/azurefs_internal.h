
#include <azure/storage/files/datalake.hpp>

#include "arrow/result.h"

namespace arrow {
namespace fs {
namespace internal {

Status ErrorToStatus(const std::string& prefix,
                                  const Azure::Storage::StorageException& exception);

class HierachicalNamespaceDetecter {
 public:
  //  TODO: Switch back to holding a reference to the datalake service client. That way we
  //  can avoid instantiating a filesystem client if the result is cached.
  Result<bool> Enabled(
      Azure::Storage::Files::DataLake::DataLakeFileSystemClient filesystem_client);

 private:
  std::optional<bool> is_hierachical_namespace_enabled_;
};

}  // namespace internal
}  // namespace fs
}  // namespace arrow
