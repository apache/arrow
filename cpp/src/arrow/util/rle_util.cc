#include "arrow/util/rle_util.h"
#include <algorithm>
#include "arrow/builder.h"

namespace arrow {
namespace rle_util {

int64_t FindPhysicalOffset(const int64_t* accumulated_run_lengths,
                           int64_t physical_length, int64_t logical_offset) {
  auto it = std::upper_bound(accumulated_run_lengths,
                             accumulated_run_lengths + physical_length, logical_offset);
  return std::distance(accumulated_run_lengths, it);
}

void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset) {
  auto& child = array->child_data[0];
  auto builder = MakeBuilder(child->type).ValueOrDie();
  ARROW_CHECK_OK(builder->AppendNulls(offset));
  ARROW_CHECK_OK(builder->AppendArraySlice(ArraySpan(*child), 0, child->length));
  array->child_data[0] = builder->Finish().ValueOrDie()->Slice(offset)->data();
}

}  // namespace rle_util
}  // namespace arrow
