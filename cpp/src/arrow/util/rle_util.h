namespace arrow {
namespace rle_util {

#include "arrow/array/data.h"

int64_t FindPhysicalOffset(const int64_t* accumulated_run_lengths, int64_t /*physical_length*/, int64_t logical_offset) {
  // TODO
  int64_t physical_offset = 0;
  while (accumulated_run_lengths[physical_offset] <= logical_offset) {
    physical_offset++;
  }
  return physical_offset;
}
}
}
