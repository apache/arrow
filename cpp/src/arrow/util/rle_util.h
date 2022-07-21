#pragma once

#include <cstdint>

#include "arrow/array/data.h"

#include "arrow/util/logging.h"

namespace arrow {
namespace rle_util {

struct Metadata {
  int64_t physical_offset;
  int64_t physical_length;
};

int64_t FindPhysicalOffset(const int32_t* accumulated_run_lengths,
                           int64_t physical_length, int64_t logical_offset);

int64_t FindPhysicalIndex(const int32_t* accumulated_run_lengths,
                          int64_t physical_length, int64_t physical_offset, int64_t logical_offset, int64_t logical_index);

// TODO: this may fit better into some testing header
void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset);

}  // namespace rle_util
}  // namespace arrow
