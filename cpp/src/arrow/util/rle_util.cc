#include "arrow/util/rle_util.h"
#include <algorithm>

namespace arrow {
namespace rle_util {

int64_t FindPhysicalOffset(const int64_t* accumulated_run_lengths,
                           int64_t physical_length, int64_t logical_offset) {
  auto it = std::upper_bound(accumulated_run_lengths,
                             accumulated_run_lengths + physical_length, logical_offset);
  return std::distance(accumulated_run_lengths, it);
}

}  // namespace rle_util
}  // namespace arrow
