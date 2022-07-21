#pragma once

#include <cstdint>

namespace arrow {
namespace rle_util {

int64_t FindPhysicalOffset(const int32_t* accumulated_run_lengths,
                           int64_t physical_length, int64_t logical_offset);

}  // namespace rle_util
}  // namespace arrow
