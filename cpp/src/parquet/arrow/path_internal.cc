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

// Overview.
//
// The strategy used for this code for repetition/definition
// is to dissect the top level array into a list of paths
// from the top level array to the final primitive (possibly
// dictionary encoded array). It then evaluates each one of
// those paths to produce results for the callback iteratively.
//
// This approach was taken to reduce the aggregate memory required if we were
// to build all def/rep levels in parallel as apart of a tree traversal.  It
// also allows for straightforward parallelization at the path level if that is
// desired in the future.
//
// The main downside to this approach is it duplicates effort for nodes
// that share common ancestors. This can be mitigated to some degree
// by adding in optimizations that detect leaf arrays that share
// the same common list ancestor and reuse the repetition levels
// from the first leaf encountered (only definition levels greater
// the list ancestor need to be re-evaluated. This is left for future
// work.
//
// Algorithm.
//
// As mentioned above this code dissects arrays into constituent parts:
// nullability data, and list offset data. It tries to optimize for
// some special cases, where it is known ahead of time that a step
// can be skipped (e.g. a nullable array happens to have all of its
// values) or batch filled (a nullable array has all null values).
// One further optimization that is not implemented but could be done
// in the future is special handling for nested list arrays that
// have some intermediate data which indicates the final array contains only
// nulls.
//
// In general, the algorithm attempts to batch work at each node as much
// as possible.  For nullability nodes this means finding runs of null
// values and batch filling those interspersed with finding runs of non-null values
// to process in batch at the next column.
//
// Similarly, list runs of empty lists are all processed in one batch
// followed by either:
//    - A single list entry for non-terminal lists (i.e. the upper part of a nested list)
//    - Runs of non-empty lists for the terminal list (i.e. the lowest part of a nested
//    list).
//
// This makes use of the following observations.
// 1.  Null values at any node on the path are terminal (repetition and definition
//     level can be set directly when a Null value is encountered).
// 2.  Empty lists share this eager termination property with Null values.
// 3.  In order to keep repetition/definition level populated the algorithm is lazy
//     in assigning repetition levels. The algorithm tracks whether it is currently
//     in the middle of a list by comparing the lengths of repetition/definition levels.
//     If it is currently in the middle of a list the the number of repetition levels
//     populated will be greater than definition levels (the start of a List requires
//     adding the first element). If there are equal numbers of definition and repetition
//     levels populated this indicates a list is waiting to be started and the next list
//     encountered will have its repetition level signify the beginning of the list.
//
//     Other implementation notes.
//
//     This code hasn't been benchmarked (or assembly analyzed) but did the following
//     as optimizations (yes premature optimization is the root of all evil).
//     - This code does not use recursion, instead it constructs its own stack and manages
//       updating elements accordingly.
//     - It tries to avoid using Status for common return states.
//     - Avoids virtual dispatch in favor of if/else statements on a set of well known
//     classes.

#include "parquet/arrow/path_internal.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/extension_type.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_visit.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/visit_array_inline.h"

#include "parquet/properties.h"

namespace parquet {
namespace arrow {

namespace {

using ::arrow::Array;
using ::arrow::Status;
using ::arrow::TypedBufferBuilder;

constexpr static int16_t kLevelNotSet = -1;

/// \brief Simple result of a iterating over a column to determine values.
enum IterationResult {
  /// Processing is done at this node. Move back up the path
  /// to continue processing.
  kDone = -1,
  /// Move down towards the leaf for processing.
  kNext = 1,
  /// An error occurred while processing.
  kError = 2
};

#define RETURN_IF_ERROR(iteration_result)                  \
  do {                                                     \
    if (ARROW_PREDICT_FALSE(iteration_result == kError)) { \
      return iteration_result;                             \
    }                                                      \
  } while (false)

int64_t LazyNullCount(const Array& array) { return array.data()->null_count.load(); }

bool LazyNoNulls(const Array& array) {
  int64_t null_count = LazyNullCount(array);
  return null_count == 0 ||
         // kUnkownNullCount comparison is needed to account
         // for null arrays.
         (null_count == ::arrow::kUnknownNullCount &&
          array.null_bitmap_data() == nullptr);
}

struct PathWriteContext {
  PathWriteContext(::arrow::MemoryPool* pool,
                   std::shared_ptr<::arrow::ResizableBuffer> def_levels_buffer)
      : rep_levels(pool), def_levels(std::move(def_levels_buffer), pool) {}
  IterationResult ReserveDefLevels(int64_t elements) {
    last_status = def_levels.Reserve(elements);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  IterationResult AppendDefLevel(int16_t def_level) {
    last_status = def_levels.Append(def_level);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  IterationResult AppendDefLevels(int64_t count, int16_t def_level) {
    last_status = def_levels.Append(count, def_level);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  void UnsafeAppendDefLevel(int16_t def_level) { def_levels.UnsafeAppend(def_level); }

  IterationResult AppendRepLevel(int16_t rep_level) {
    last_status = rep_levels.Append(rep_level);

    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  IterationResult AppendRepLevels(int64_t count, int16_t rep_level) {
    last_status = rep_levels.Append(count, rep_level);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  bool EqualRepDefLevelsLengths() const {
    return rep_levels.length() == def_levels.length();
  }

  // Incorporates |range| into visited elements. If the |range| is contiguous
  // with the last range, extend the last range, otherwise add |range| separately
  // tot he list.
  void RecordPostListVisit(const ElementRange& range) {
    if (!visited_elements.empty() && range.start == visited_elements.back().end) {
      visited_elements.back().end = range.end;
      return;
    }
    visited_elements.push_back(range);
  }

  Status last_status;
  TypedBufferBuilder<int16_t> rep_levels;
  TypedBufferBuilder<int16_t> def_levels;
  std::vector<ElementRange> visited_elements;
};

IterationResult FillRepLevels(int64_t count, int16_t rep_level,
                              PathWriteContext* context) {
  if (rep_level == kLevelNotSet) {
    return kDone;
  }
  int64_t fill_count = count;
  // This condition occurs (rep and dep levels equals), in one of
  // in a few cases:
  // 1.  Before any list is encountered.
  // 2.  After rep-level has been filled in due to null/empty
  //     values above it.
  // 3.  After finishing a list.
  if (!context->EqualRepDefLevelsLengths()) {
    fill_count--;
  }
  return context->AppendRepLevels(fill_count, rep_level);
}

// A node for handling an array that is discovered to have all
// null elements. It is referred to as a TerminalNode because
// traversal of nodes will not continue it when generating
// rep/def levels. However, there could be many nested children
// elements beyond it in the Array that is being processed.
class AllNullsTerminalNode {
 public:
  explicit AllNullsTerminalNode(int16_t def_level, int16_t rep_level = kLevelNotSet)
      : def_level_(def_level), rep_level_(rep_level) {}
  void SetRepLevelIfNull(int16_t rep_level) { rep_level_ = rep_level; }
  IterationResult Run(const ElementRange& range, PathWriteContext* context) {
    int64_t size = range.Size();
    RETURN_IF_ERROR(FillRepLevels(size, rep_level_, context));
    return context->AppendDefLevels(size, def_level_);
  }

 private:
  int16_t def_level_;
  int16_t rep_level_;
};

// Handles the case where all remaining arrays until the leaf have no nulls
// (and are not interrupted by lists). Unlike AllNullsTerminalNode this is
// always the last node in a path. We don't need an analogue to the AllNullsTerminalNode
// because if all values are present at an intermediate array no node is added for it
// (the def-level for the next nullable node is incremented).
struct AllPresentTerminalNode {
  IterationResult Run(const ElementRange& range, PathWriteContext* context) {
    return context->AppendDefLevels(range.end - range.start, def_level);
    // No need to worry about rep levels, because this state should
    // only be applicable for after all list/repeated values
    // have been evaluated in the path.
  }
  int16_t def_level;
};

/// Node for handling the case when the leaf-array is nullable
/// and contains null elements.
struct NullableTerminalNode {
  NullableTerminalNode() = default;

  NullableTerminalNode(const uint8_t* bitmap, int64_t element_offset,
                       int16_t def_level_if_present)
      : bitmap_(bitmap),
        element_offset_(element_offset),
        def_level_if_present_(def_level_if_present),
        def_level_if_null_(def_level_if_present - 1) {}

  IterationResult Run(const ElementRange& range, PathWriteContext* context) {
    int64_t elements = range.Size();
    RETURN_IF_ERROR(context->ReserveDefLevels(elements));

    DCHECK_GT(elements, 0);

    auto bit_visitor = [&](bool is_set) {
      context->UnsafeAppendDefLevel(is_set ? def_level_if_present_ : def_level_if_null_);
    };

    if (elements > 16) {  // 16 guarantees at least one unrolled loop.
      ::arrow::internal::VisitBitsUnrolled(bitmap_, range.start + element_offset_,
                                           elements, bit_visitor);
    } else {
      ::arrow::internal::VisitBits(bitmap_, range.start + element_offset_, elements,
                                   bit_visitor);
    }
    return kDone;
  }
  const uint8_t* bitmap_;
  int64_t element_offset_;
  int16_t def_level_if_present_;
  int16_t def_level_if_null_;
};

// List nodes handle populating rep_level for Arrow Lists and def-level for empty lists.
// Nullability (both list and children) is handled by other Nodes. By
// construction all list nodes will be intermediate nodes (they will always be followed by
// at least one other node).
//
// Type parameters:
//    |RangeSelector| - A strategy for determine the the range of the child node to
//    process.
//       this varies depending on the type of list (int32_t* offsets, int64_t* offsets of
//       fixed.
template <typename RangeSelector>
class ListPathNode {
 public:
  ListPathNode(RangeSelector selector, int16_t rep_lev, int16_t def_level_if_empty)
      : selector_(std::move(selector)),
        prev_rep_level_(rep_lev - 1),
        rep_level_(rep_lev),
        def_level_if_empty_(def_level_if_empty) {}

  int16_t rep_level() const { return rep_level_; }

  IterationResult Run(ElementRange* range, ElementRange* child_range,
                      PathWriteContext* context) {
    if (range->Empty()) {
      return kDone;
    }
    // Find the first non-empty list (skipping a run of empties).
    int64_t empty_elements = 0;
    do {
      // Retrieve the range of elements that this list contains.
      *child_range = selector_.GetRange(range->start);
      if (!child_range->Empty()) {
        break;
      }
      ++empty_elements;
      ++range->start;
    } while (!range->Empty());

    // Post condition:
    //   * range is either empty (we are done processing at this node)
    //     or start corresponds a non-empty list.
    //   * If range is non-empty child_range contains
    //     the bounds of non-empty list.

    // Handle any skipped over empty lists.
    if (empty_elements > 0) {
      RETURN_IF_ERROR(FillRepLevels(empty_elements, prev_rep_level_, context));
      RETURN_IF_ERROR(context->AppendDefLevels(empty_elements, def_level_if_empty_));
    }
    // Start of a new list. Note that for nested lists adding the element
    // here effectively suppresses this code until we either encounter null
    // elements or empty lists between here and the innermost list (since
    // we make the rep levels repetition and definition levels unequal).
    // Similarly when we are backtracking up the stack the repetition and
    // definition levels are again equal so if we encounter an intermediate list
    // with more elements this will detect it as a new list.
    if (context->EqualRepDefLevelsLengths() && !range->Empty()) {
      RETURN_IF_ERROR(context->AppendRepLevel(prev_rep_level_));
    }

    if (range->Empty()) {
      return kDone;
    }

    ++range->start;
    if (is_last_) {
      // If this is the last repeated node, we can extend try
      // to extend the child range as wide as possible before
      // continuing to the next node.
      return FillForLast(range, child_range, context);
    }
    return kNext;
  }

  void SetLast() { is_last_ = true; }

 private:
  IterationResult FillForLast(ElementRange* range, ElementRange* child_range,
                              PathWriteContext* context) {
    // First fill int the remainder of the list.
    RETURN_IF_ERROR(FillRepLevels(child_range->Size(), rep_level_, context));
    // Once we've reached this point the following preconditions should hold:
    // 1.  There are no more repeated path nodes to deal with.
    // 2.  All elements in |range| represent contiguous elements in the
    //     child array (Null values would have shortened the range to ensure
    //     all remaining list elements are present (though they may be empty lists)).
    // 3.  No element of range spans a parent list (intermediate
    //     list nodes only handle one list entry at a time).
    //
    // Given these preconditions it should be safe to fill runs on non-empty
    // lists here and expand the range in the child node accordingly.

    while (!range->Empty()) {
      ElementRange size_check = selector_.GetRange(range->start);
      if (size_check.Empty()) {
        // The empty range will need to be handled after we pass down the accumulated
        // range because it affects def_level placement and we need to get the children
        // def_levels entered first.
        break;
      }
      // This is the start of a new list. We can be sure it only applies
      // to the previous list (and doesn't jump to the start of any list
      // further up in nesting due to the constraints mentioned at the start
      // of the function).
      RETURN_IF_ERROR(context->AppendRepLevel(prev_rep_level_));
      RETURN_IF_ERROR(context->AppendRepLevels(size_check.Size() - 1, rep_level_));
      DCHECK_EQ(size_check.start, child_range->end)
          << size_check.start << " != " << child_range->end;
      child_range->end = size_check.end;
      ++range->start;
    }

    // Do book-keeping to track the elements of the arrays that are actually visited
    // beyond this point.  This is necessary to identify "gaps" in values that should
    // not be processed (written out to parquet).
    context->RecordPostListVisit(*child_range);
    return kNext;
  }

  RangeSelector selector_;
  int16_t prev_rep_level_;
  int16_t rep_level_;
  int16_t def_level_if_empty_;
  bool is_last_ = false;
};

template <typename OffsetType>
struct VarRangeSelector {
  ElementRange GetRange(int64_t index) const {
    return ElementRange{offsets[index], offsets[index + 1]};
  }

  // Either int32_t* or int64_t*.
  const OffsetType* offsets;
};

struct FixedSizedRangeSelector {
  ElementRange GetRange(int64_t index) const {
    int64_t start = index * list_size;
    return ElementRange{start, start + list_size};
  }
  int list_size;
};

// An intermediate node that handles null values.
class NullableNode {
 public:
  NullableNode(const uint8_t* null_bitmap, int64_t entry_offset,
               int16_t def_level_if_null, int16_t rep_level_if_null = kLevelNotSet)
      : null_bitmap_(null_bitmap),
        entry_offset_(entry_offset),
        valid_bits_reader_(MakeReader(ElementRange{0, 0})),
        def_level_if_null_(def_level_if_null),
        rep_level_if_null_(rep_level_if_null),
        new_range_(true) {}

  void SetRepLevelIfNull(int16_t rep_level) { rep_level_if_null_ = rep_level; }

  ::arrow::internal::BitRunReader MakeReader(const ElementRange& range) {
    return ::arrow::internal::BitRunReader(null_bitmap_, entry_offset_ + range.start,
                                           range.Size());
  }

  IterationResult Run(ElementRange* range, ElementRange* child_range,
                      PathWriteContext* context) {
    if (new_range_) {
      // Reset the reader each time we are starting fresh on a range.
      // We can't rely on continuity because nulls above can
      // cause discontinuities.
      valid_bits_reader_ = MakeReader(*range);
    }
    child_range->start = range->start;
    ::arrow::internal::BitRun run = valid_bits_reader_.NextRun();
    if (!run.set) {
      range->start += run.length;
      RETURN_IF_ERROR(FillRepLevels(run.length, rep_level_if_null_, context));
      RETURN_IF_ERROR(context->AppendDefLevels(run.length, def_level_if_null_));
      run = valid_bits_reader_.NextRun();
    }
    if (range->Empty()) {
      new_range_ = true;
      return kDone;
    }
    child_range->end = child_range->start = range->start;
    child_range->end += run.length;

    DCHECK(!child_range->Empty());
    range->start += child_range->Size();
    new_range_ = false;
    return kNext;
  }

  const uint8_t* null_bitmap_;
  int64_t entry_offset_;
  ::arrow::internal::BitRunReader valid_bits_reader_;
  int16_t def_level_if_null_;
  int16_t rep_level_if_null_;

  // Whether the next invocation will be a new range.
  bool new_range_ = true;
};

using ListNode = ListPathNode<VarRangeSelector<int32_t>>;
using LargeListNode = ListPathNode<VarRangeSelector<int64_t>>;
using FixedSizeListNode = ListPathNode<FixedSizedRangeSelector>;

// Contains static information derived from traversing the schema.
struct PathInfo {
  // The vectors are expected to the same length info.

  // Note index order matters here.
  using Node =
      std::variant<NullableTerminalNode, ListNode, LargeListNode, FixedSizeListNode,
                   NullableNode, AllPresentTerminalNode, AllNullsTerminalNode>;

  std::vector<Node> path;
  std::shared_ptr<Array> primitive_array;
  int16_t max_def_level = 0;
  int16_t max_rep_level = 0;
  bool has_dictionary = false;
  bool leaf_is_nullable = false;
};

/// Contains logic for writing a single leaf node to parquet.
/// This tracks the path from root to leaf.
///
/// |writer| will be called after all of the definition/repetition
/// values have been calculated for root_range with the calculated
/// values. It is intended to abstract the complexity of writing
/// the levels and values to parquet.
Status WritePath(ElementRange root_range, PathInfo* path_info,
                 ArrowWriteContext* arrow_context,
                 MultipathLevelBuilder::CallbackFunction writer) {
  std::vector<ElementRange> stack(path_info->path.size());
  MultipathLevelBuilderResult builder_result;
  builder_result.leaf_array = path_info->primitive_array;
  builder_result.leaf_is_nullable = path_info->leaf_is_nullable;

  if (path_info->max_def_level == 0) {
    // This case only occurs when there are no nullable or repeated
    // columns in the path from the root to leaf.
    int64_t leaf_length = builder_result.leaf_array->length();
    builder_result.def_rep_level_count = leaf_length;
    builder_result.post_list_visited_elements.push_back({0, leaf_length});
    return writer(builder_result);
  }
  stack[0] = root_range;
  RETURN_NOT_OK(
      arrow_context->def_levels_buffer->Resize(/*new_size=*/0, /*shrink_to_fit*/ false));
  PathWriteContext context(arrow_context->memory_pool, arrow_context->def_levels_buffer);
  // We should need at least this many entries so reserve the space ahead of time.
  RETURN_NOT_OK(context.def_levels.Reserve(root_range.Size()));
  if (path_info->max_rep_level > 0) {
    RETURN_NOT_OK(context.rep_levels.Reserve(root_range.Size()));
  }

  auto stack_base = &stack[0];
  auto stack_position = stack_base;
  // This is the main loop for calculated rep/def levels. The nodes
  // in the path implement a chain-of-responsibility like pattern
  // where each node can add some number of repetition/definition
  // levels to PathWriteContext and also delegate to the next node
  // in the path to add values. The values are added through each Run(...)
  // call and the choice to delegate to the next node (or return to the
  // previous node) is communicated by the return value of Run(...).
  // The loop terminates after the first node indicates all values in
  // |root_range| are processed.
  while (stack_position >= stack_base) {
    PathInfo::Node& node = path_info->path[stack_position - stack_base];
    struct {
      IterationResult operator()(NullableNode& node) {
        return node.Run(stack_position, stack_position + 1, context);
      }
      IterationResult operator()(ListNode& node) {
        return node.Run(stack_position, stack_position + 1, context);
      }
      IterationResult operator()(NullableTerminalNode& node) {
        return node.Run(*stack_position, context);
      }
      IterationResult operator()(FixedSizeListNode& node) {
        return node.Run(stack_position, stack_position + 1, context);
      }
      IterationResult operator()(AllPresentTerminalNode& node) {
        return node.Run(*stack_position, context);
      }
      IterationResult operator()(AllNullsTerminalNode& node) {
        return node.Run(*stack_position, context);
      }
      IterationResult operator()(LargeListNode& node) {
        return node.Run(stack_position, stack_position + 1, context);
      }
      ElementRange* stack_position;
      PathWriteContext* context;
    } visitor = {stack_position, &context};

    IterationResult result = std::visit(visitor, node);

    if (ARROW_PREDICT_FALSE(result == kError)) {
      DCHECK(!context.last_status.ok());
      return context.last_status;
    }
    stack_position += static_cast<int>(result);
  }
  RETURN_NOT_OK(context.last_status);
  builder_result.def_rep_level_count = context.def_levels.length();

  if (context.rep_levels.length() > 0) {
    // This case only occurs when there was a repeated element that needs to be
    // processed.
    builder_result.rep_levels = context.rep_levels.data();
    std::swap(builder_result.post_list_visited_elements, context.visited_elements);
    // If it is possible when processing lists that all lists where empty. In this
    // case no elements would have been added to post_list_visited_elements. By
    // added an empty element we avoid special casing in downstream consumers.
    if (builder_result.post_list_visited_elements.empty()) {
      builder_result.post_list_visited_elements.push_back({0, 0});
    }
  } else {
    builder_result.post_list_visited_elements.push_back(
        {0, builder_result.leaf_array->length()});
    builder_result.rep_levels = nullptr;
  }

  builder_result.def_levels = context.def_levels.data();
  return writer(builder_result);
}

struct FixupVisitor {
  int max_rep_level = -1;
  int16_t rep_level_if_null = kLevelNotSet;

  template <typename T>
  void HandleListNode(T& arg) {
    if (arg.rep_level() == max_rep_level) {
      arg.SetLast();
      // after the last list node we don't need to fill
      // rep levels on null.
      rep_level_if_null = kLevelNotSet;
    } else {
      rep_level_if_null = arg.rep_level();
    }
  }
  void operator()(ListNode& node) { HandleListNode(node); }
  void operator()(LargeListNode& node) { HandleListNode(node); }
  void operator()(FixedSizeListNode& node) { HandleListNode(node); }

  // For non-list intermediate nodes.
  template <typename T>
  void HandleIntermediateNode(T& arg) {
    if (rep_level_if_null != kLevelNotSet) {
      arg.SetRepLevelIfNull(rep_level_if_null);
    }
  }

  void operator()(NullableNode& arg) { HandleIntermediateNode(arg); }

  void operator()(AllNullsTerminalNode& arg) {
    // Even though no processing happens past this point we
    // still need to adjust it if a list occurred after an
    // all null array.
    HandleIntermediateNode(arg);
  }

  void operator()(NullableTerminalNode&) {}
  void operator()(AllPresentTerminalNode&) {}
};

PathInfo Fixup(PathInfo info) {
  // We only need to fixup the path if there were repeated
  // elements on it.
  if (info.max_rep_level == 0) {
    return info;
  }
  FixupVisitor visitor;
  visitor.max_rep_level = info.max_rep_level;
  if (visitor.max_rep_level > 0) {
    visitor.rep_level_if_null = 0;
  }
  for (size_t x = 0; x < info.path.size(); x++) {
    std::visit(visitor, info.path[x]);
  }
  return info;
}

class PathBuilder {
 public:
  explicit PathBuilder(bool start_nullable) : nullable_in_parent_(start_nullable) {}
  template <typename T>
  void AddTerminalInfo(const T& array) {
    info_.leaf_is_nullable = nullable_in_parent_;
    if (nullable_in_parent_) {
      info_.max_def_level++;
    }
    // We don't use null_count() because if the null_count isn't known
    // and the array does in fact contain nulls, we will end up
    // traversing the null bitmap twice (once here and once when calculating
    // rep/def levels).
    if (LazyNoNulls(array)) {
      info_.path.emplace_back(AllPresentTerminalNode{info_.max_def_level});
    } else if (LazyNullCount(array) == array.length()) {
      info_.path.emplace_back(AllNullsTerminalNode(info_.max_def_level - 1));
    } else {
      info_.path.emplace_back(NullableTerminalNode(array.null_bitmap_data(),
                                                   array.offset(), info_.max_def_level));
    }
    info_.primitive_array = std::make_shared<T>(array.data());
    paths_.push_back(Fixup(info_));
  }

  template <typename T>
  ::arrow::enable_if_t<std::is_base_of<::arrow::FlatArray, T>::value, Status> Visit(
      const T& array) {
    AddTerminalInfo(array);
    return Status::OK();
  }

  template <typename T>
  ::arrow::enable_if_t<std::is_same<::arrow::ListArray, T>::value ||
                           std::is_same<::arrow::LargeListArray, T>::value,
                       Status>
  Visit(const T& array) {
    MaybeAddNullable(array);
    // Increment necessary due to empty lists.
    info_.max_def_level++;
    info_.max_rep_level++;
    // raw_value_offsets() accounts for any slice offset.
    ListPathNode<VarRangeSelector<typename T::offset_type>> node(
        VarRangeSelector<typename T::offset_type>{array.raw_value_offsets()},
        info_.max_rep_level, info_.max_def_level - 1);
    info_.path.emplace_back(std::move(node));
    nullable_in_parent_ = array.list_type()->value_field()->nullable();
    return VisitInline(*array.values());
  }

  Status Visit(const ::arrow::DictionaryArray& array) {
    // Only currently handle DictionaryArray where the dictionary is a
    // primitive type
    if (array.dict_type()->value_type()->num_fields() > 0) {
      return Status::NotImplemented(
          "Writing DictionaryArray with nested dictionary "
          "type not yet supported");
    }
    if (array.dictionary()->null_count() > 0) {
      return Status::NotImplemented(
          "Writing DictionaryArray with null encoded in dictionary "
          "type not yet supported");
    }
    AddTerminalInfo(array);
    return Status::OK();
  }

  void MaybeAddNullable(const Array& array) {
    if (!nullable_in_parent_) {
      return;
    }
    info_.max_def_level++;
    // We don't use null_count() because if the null_count isn't known
    // and the array does in fact contain nulls, we will end up
    // traversing the null bitmap twice (once here and once when calculating
    // rep/def levels). Because this isn't terminal this might not be
    // the right decision for structs that share the same nullable
    // parents.
    if (LazyNoNulls(array)) {
      // Don't add anything because there won't be any point checking
      // null values for the array.  There will always be at least
      // one more array to handle nullability.
      return;
    }
    if (LazyNullCount(array) == array.length()) {
      info_.path.emplace_back(AllNullsTerminalNode(info_.max_def_level - 1));
      return;
    }
    info_.path.emplace_back(
        NullableNode(array.null_bitmap_data(), array.offset(),
                     /* def_level_if_null = */ info_.max_def_level - 1));
  }

  Status VisitInline(const Array& array);

  Status Visit(const ::arrow::MapArray& array) {
    return Visit(static_cast<const ::arrow::ListArray&>(array));
  }

  Status Visit(const ::arrow::StructArray& array) {
    MaybeAddNullable(array);
    PathInfo info_backup = info_;
    for (int x = 0; x < array.num_fields(); x++) {
      nullable_in_parent_ = array.type()->field(x)->nullable();
      RETURN_NOT_OK(VisitInline(*array.field(x)));
      info_ = info_backup;
    }
    return Status::OK();
  }

  Status Visit(const ::arrow::RunLengthEncodedArray& array) {
    return Status::NotImplemented("arrow rle array in Parquet");
  }

  Status Visit(const ::arrow::FixedSizeListArray& array) {
    MaybeAddNullable(array);
    int32_t list_size = array.list_type()->list_size();
    // Technically we could encode fixed size lists with two level encodings
    // but since we always use 3 level encoding we increment def levels as
    // well.
    info_.max_def_level++;
    info_.max_rep_level++;
    info_.path.emplace_back(FixedSizeListNode(FixedSizedRangeSelector{list_size},
                                              info_.max_rep_level, info_.max_def_level));
    nullable_in_parent_ = array.list_type()->value_field()->nullable();
    if (array.offset() > 0) {
      return VisitInline(*array.values()->Slice(array.value_offset(0)));
    }
    return VisitInline(*array.values());
  }

  Status Visit(const ::arrow::ExtensionArray& array) {
    return VisitInline(*array.storage());
  }

#define NOT_IMPLEMENTED_VISIT(ArrowTypePrefix)                             \
  Status Visit(const ::arrow::ArrowTypePrefix##Array& array) {             \
    return Status::NotImplemented("Level generation for " #ArrowTypePrefix \
                                  " not supported yet");                   \
  }

  // Union types aren't supported in Parquet.
  NOT_IMPLEMENTED_VISIT(Union)

#undef NOT_IMPLEMENTED_VISIT
  std::vector<PathInfo>& paths() { return paths_; }

 private:
  PathInfo info_;
  std::vector<PathInfo> paths_;
  bool nullable_in_parent_;
};

Status PathBuilder::VisitInline(const Array& array) {
  return ::arrow::VisitArrayInline(array, this);
}

#undef RETURN_IF_ERROR
}  // namespace

class MultipathLevelBuilderImpl : public MultipathLevelBuilder {
 public:
  MultipathLevelBuilderImpl(std::shared_ptr<::arrow::ArrayData> data,
                            std::unique_ptr<PathBuilder> path_builder)
      : root_range_{0, data->length},
        data_(std::move(data)),
        path_builder_(std::move(path_builder)) {}

  int GetLeafCount() const override {
    return static_cast<int>(path_builder_->paths().size());
  }

  ::arrow::Status Write(int leaf_index, ArrowWriteContext* context,
                        CallbackFunction write_leaf_callback) override {
    DCHECK_GE(leaf_index, 0);
    DCHECK_LT(leaf_index, GetLeafCount());
    return WritePath(root_range_, &path_builder_->paths()[leaf_index], context,
                     std::move(write_leaf_callback));
  }

 private:
  ElementRange root_range_;
  // Reference holder to ensure the data stays valid.
  std::shared_ptr<::arrow::ArrayData> data_;
  std::unique_ptr<PathBuilder> path_builder_;
};

// static
::arrow::Result<std::unique_ptr<MultipathLevelBuilder>> MultipathLevelBuilder::Make(
    const ::arrow::Array& array, bool array_field_nullable) {
  auto constructor = std::make_unique<PathBuilder>(array_field_nullable);
  RETURN_NOT_OK(VisitArrayInline(array, constructor.get()));
  return std::make_unique<MultipathLevelBuilderImpl>(array.data(),
                                                     std::move(constructor));
}

// static
Status MultipathLevelBuilder::Write(const Array& array, bool array_field_nullable,
                                    ArrowWriteContext* context,
                                    MultipathLevelBuilder::CallbackFunction callback) {
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<MultipathLevelBuilder> builder,
                        MultipathLevelBuilder::Make(array, array_field_nullable));
  PathBuilder constructor(array_field_nullable);
  RETURN_NOT_OK(VisitArrayInline(array, &constructor));
  for (int leaf_idx = 0; leaf_idx < builder->GetLeafCount(); leaf_idx++) {
    RETURN_NOT_OK(builder->Write(leaf_idx, context, callback));
  }
  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
