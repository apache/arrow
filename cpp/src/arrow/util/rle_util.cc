#include "arrow/util/rle_util.h"
#include <algorithm>
#include "arrow/builder.h"

namespace arrow {
namespace rle_util {

template <typename RunEndsType>
int64_t FindPhysicalOffset(const RunEndsType* run_ends, int64_t num_run_ends,
                           int64_t logical_offset) {
  auto it = std::upper_bound(run_ends, run_ends + num_run_ends, logical_offset);
  int64_t result = std::distance(run_ends, it);
  ARROW_DCHECK_LE(result, num_run_ends);
  return result;
}

void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset) {
  auto& child = array->child_data[1];
  auto builder = MakeBuilder(child->type).ValueOrDie();
  ARROW_CHECK_OK(builder->AppendNulls(offset));
  ARROW_CHECK_OK(builder->AppendArraySlice(ArraySpan(*child), 0, child->length));
  array->child_data[1] = builder->Finish().ValueOrDie()->Slice(offset)->data();
}

int64_t GetPhysicalOffset(const ArraySpan& span) {
  // TODO: caching
  if (span.type->id() == Type::INT16) {
    return FindPhysicalOffset(RunEnds<int16_t>(span), RunEndsArray(span).length,
                              span.offset);
  } else if (span.type->id() == Type::INT32) {
    return FindPhysicalOffset(RunEnds<int32_t>(span), RunEndsArray(span).length,
                              span.offset);
  } else {
    ARROW_CHECK(span.type->id() == Type::INT64);
    return FindPhysicalOffset(RunEnds<int64_t>(span), RunEndsArray(span).length,
                              span.offset);
  }
}

template <typename RunEndsType>
static int64_t GetPhysicalLengthInternal(const ArraySpan& span) {
  // find the offset of the last element and add 1
  int64_t physical_offset = GetPhysicalOffset(span);
  return FindPhysicalOffset(RunEnds<RunEndsType>(span) + physical_offset,
                            RunEndsArray(span).length - physical_offset,
                            span.offset + span.length - 1) +
         1;
}

int64_t GetPhysicalLength(const ArraySpan& span) {
  // TODO: caching
  if (span.length == 0) {
    return 0;
  } else {
    if (span.type->id() == Type::INT16) {
      return GetPhysicalLengthInternal<int16_t>(span);
    } else if (span.type->id() == Type::INT32) {
      return GetPhysicalLengthInternal<int32_t>(span);
    } else {
      ARROW_CHECK(span.type->id() == Type::INT64);
      return GetPhysicalLengthInternal<int64_t>(span);
    }
  }
}

}  // namespace rle_util
}  // namespace arrow
