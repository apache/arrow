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
// The strategy used for this code for repeition/defition
// is to dissect the top level array into a list of paths
// from the top level array to the final primitive (possibly
// dictionary encoded array). It then evaluates each one of
// those paths to produce results for the callback iteratively.
//
// This approach was taken to reduce the aggregate memory required
// if we were to build all def/rep levels in parallel as apart of
// a tree traversal.  It also allows for straighforward parellization
// at the path level if that is desired in the future.
//
// The main downside to this approach is it duplicates effort for nodes
// that share common ancestors. This can be mitigated to some degree
// by adding in optimizations that detect leaf arrays that share
// the same common list ancestor and reuse the repeteition levels
// from the first leaf encountered (only definition levels greater
// the list ancestor need to be re-evaluated. This is left for future
// work.
//
// Algorithm.
//
// As mentioned above this code dissects arrays into constiutent parts:
// nullability data, and list offset data. It tries to optimize for
// some special cases, were is known ahead of time that a step
// can be skipped (e.g. a nullable array happens to have all of its
// values) or batch filled (a nullable array has all null values).
// One futher optimization that is not implemented but could be done
// in the future is special handling for nested list arrays that
// have a intermediate data that indicates the final array contains all
// nulls.
//
// In general, the algorithm attempts to batch work at each node as much
// as possible.  For nullability nodes this means finding runs of null
// values and batch filling those before finding runs fo non-null values
// to process in batch at the next column.
//
// Similarly for lists runs, of empty lists are all processed in one batch
// followed by either:
//    - A single list entry to non-terminal lists (i.e. the upper part of a nested list)
//    - Runs of non-mepty lists for the terminal list (i.e. the lowest nested list).
//
// This makes use of the following observations.
// 1.  Null values at any node on the path are terminal (repetition and definition
//     level can be set directly at that point).
// 2.  Empty lists share the same property as Null values.
// 3.  In order to keep repetition/defition level populated the algorithm is lazy
//     in assigning repetition levels. The algorithm tracks whether it is currently
//     in the middle of a list by comparing the lengths of repetition/definition levels.
//     If it is currently in the middle of a list the the number of repetition levels
//     populated will be greater then definition levels (list starts require adding
//     the first element). If there are equal number of definition and repetition levels
//     populated this indicates a list is waiting to be started and the next list
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

#include "arrow/buffer_builder.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/variant.h"
#include "arrow/visitor_inline.h"

namespace parquet {
namespace arrow {

namespace {

using ::arrow::Array;
using ::arrow::Status;
using ::arrow::util::holds_alternative;

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

struct PathWriteContext {
  PathWriteContext(::arrow::MemoryPool* pool,
                   std::shared_ptr<::arrow::ResizableBuffer> def_levels_buffer)
      : rep_levels(pool), def_levels(std::move(def_levels_buffer), pool) {}
  IterationResult ReserveDefLevels(int16_t elements) {
    last_status = def_levels.Reserve(elements);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  IterationResult AppendDefLevel(int16_t def_level) {
    last_status = def_levels.Append(def_level);
    last_appended_def_level = def_level;
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  IterationResult AppendDefLevels(int64_t count, int16_t def_level) {
    if (count > 0) {
      last_appended_def_level = def_level;
    }
    last_status = def_levels.Append(count, def_level);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  void UnsafeAppendDefLevel(int16_t def_level) {
    last_appended_def_level = def_level;
    def_levels.UnsafeAppend(def_level);
  }

  IterationResult AppendRepLevel(int16_t rep_level) {
    last_status = rep_levels.Append(rep_level);

    last_appended_rep_level = rep_level;
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  IterationResult AppendRepLevels(int64_t count, int16_t rep_level) {
    if (count > 0) {
      last_appended_rep_level = rep_level;
    }
    last_status = rep_levels.Append(count, rep_level);
    if (ARROW_PREDICT_TRUE(last_status.ok())) {
      return kDone;
    }
    return kError;
  }

  bool EqualRepDefLevelsLengths() const {
    return rep_levels.length() == def_levels.length();
  }

  void RecordPostListVisit(const ElementRange& range) {
    if (!visited_elements.empty() && range.start == visited_elements.back().end) {
      visited_elements.back().end = range.end;
      return;
    }
    visited_elements.push_back(range);
  }

  Status last_status;
  ::arrow::TypedBufferBuilder<int16_t> rep_levels;
  ::arrow::TypedBufferBuilder<int16_t> def_levels;
  int16_t last_appended_rep_level = 0;
  int16_t last_appended_def_level = 0;
  std::vector<ElementRange> visited_elements;
};

IterationResult FillRepLevels(int64_t count, int16_t rep_level,
                              PathWriteContext* context) {
  if (rep_level == kLevelNotSet) {
    return kDone;
  }
  int64_t fill_count = count;
  if (!context->EqualRepDefLevelsLengths()) {
    fill_count--;
  }
  return context->AppendRepLevels(fill_count, rep_level);
}

class AllNullsTerminalNode {
 public:
  explicit AllNullsTerminalNode(int16_t def_level, int16_t rep_level = kLevelNotSet)
      : def_level_(def_level), rep_level_(rep_level) {}
  void SetRepLevelIfNull(int16_t rep_level) { rep_level_ = rep_level; }
  IterationResult Run(const ElementRange& range, PathWriteContext* context) {
    int64_t size = range.Size();
    RETURN_IF_ERROR(FillRepLevels(size, rep_level_, context));
    // pass through an empty range to force not setting the rep-level values.
    return context->AppendDefLevels(size, def_level_);
  }

 private:
  int16_t def_level_;
  int16_t rep_level_;
};

struct AllPresentTerminalNode {
  IterationResult Run(const ElementRange& range, PathWriteContext* context) {
    return context->AppendDefLevels(range.end - range.start, def_level);
    // No need to worry about rep levels, because this state should
    // only be applicable for after all list/repeated values
    // have been evaluated in the path.
  }
  int16_t def_level;
};

struct NullableTerminalNode {
  NullableTerminalNode();
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

// List nodes handle populating rep_level for Arrow Lists and dep-level for empty lists.
// Nullability (both list and children) is handled by other Nodes. This class should not
// be used directly instead one of its CRTP extensions should be used below. By
// construction all list nodes will be intermediate nodes (they will always be followed by
// at least one other node).
//
// Type parameters:
//    |RangeSelector| - A strategy for determine the the range of the child node to
//    process.
//       this varies depending on the type of list (int32_t* offsets, int64_t* offsets of
//       fixed.
//    |PositionType| - For non-empty lists a strategy to fill in rep_levels.  The strategy
//       varies dependong if the node is the last list node for the column. This used for
//       CRTP.
//
// TODO(micahk): This logic currently doesn't detect lists with a null value that has
// non-zero size but depends on this relationship for correctness.
template <typename RangeSelector>
class ListPathNode {
 public:
  ListPathNode(RangeSelector selector, int16_t rep_lev, int16_t def_level_if_empty)
      : selector_(std::move(selector)),
        prev_rep_level_(rep_lev - 1),
        rep_level_(rep_lev),
        def_level_if_empty_(def_level_if_empty) {}

  int16_t rep_level() const { return rep_level_; }

  IterationResult Run(ElementRange* range, ElementRange* next_range,
                      PathWriteContext* context) {
    if (range->Empty()) {
      return kDone;
    }

    // Find runs of empty lists.
    int64_t start = range->start;
    *next_range = selector_.GetRange(range->start);
    while (next_range->Empty() && !range->Empty()) {
      ++range->start;
      *next_range = selector_.GetRange(range->start);
    }

    int64_t empty_elements = range->start - start;
    if (empty_elements > 0) {
      RETURN_IF_ERROR(FillRepLevels(empty_elements, prev_rep_level_, context));
      RETURN_IF_ERROR(context->AppendDefLevels(empty_elements, def_level_if_empty_));
    }
    if (context->EqualRepDefLevelsLengths() && !range->Empty()) {
      RETURN_IF_ERROR(context->AppendRepLevel(prev_rep_level_));
    }

    if (range->Empty()) {
      return kDone;
    }

    ++range->start;
    if (is_last_) {
      return FillForLast(range, next_range, context);
    }
    return kNext;
  }

  void SetLast() { is_last_ = true; }

 private:
  IterationResult FillForLast(ElementRange* range, ElementRange* next_range,
                              PathWriteContext* context) {
    // First fill int the remainder of the list.
    RETURN_IF_ERROR(FillRepLevels(next_range->Size(), rep_level_, context));
    // Once we've reached this point the following preconditions should hold:
    // 1.  There are no more repeated path nodes to deal with.
    // 2.  All elements in |range| represent contiguous elements in the
    //     child array (Null values would have shortened the range to ensure
    //     all list elements are in present (but possibly empty)).
    // 3.  All elements of range do not span parent lists (intermediate
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
      RETURN_IF_ERROR(context->AppendRepLevel(prev_rep_level_));
      RETURN_IF_ERROR(context->AppendRepLevels(size_check.Size() - 1, rep_level_));
      DCHECK_EQ(size_check.start, next_range->end);
      next_range->end = size_check.end;
      ++range->start;
    }

    context->RecordPostListVisit(*next_range);
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
  };

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

// An intermediate node that nhandles null values.
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

  ::arrow::internal::BitmapReader MakeReader(const ElementRange& range) {
    return ::arrow::internal::BitmapReader(null_bitmap_, entry_offset_ + range.start,
                                           range.Size());
  }

  IterationResult Run(ElementRange* range, ElementRange* next_range,
                      PathWriteContext* context) {
    if (new_range_) {
      // Reset the reader each time we are starting fresh on a range.
      // We can't rely on continuity because nulls above can
      // cause discontinuties.
      valid_bits_reader_ = MakeReader(*range);
    }
    next_range->start = range->start;
    while (!range->Empty() && !valid_bits_reader_.IsSet()) {
      ++range->start;
      valid_bits_reader_.Next();
    }
    int64_t null_count = range->start - next_range->start;
    if (null_count > 0) {
      RETURN_IF_ERROR(FillRepLevels(null_count, rep_level_if_null_, context));
      RETURN_IF_ERROR(context->AppendDefLevels(null_count, def_level_if_null_));
    }
    if (range->Empty()) {
      new_range_ = true;
      return kDone;
    }
    next_range->end = next_range->start = range->start;

    while (next_range->end != range->end && valid_bits_reader_.IsSet()) {
      ++next_range->end;
      valid_bits_reader_.Next();
    }
    DCHECK(!next_range->Empty());
    range->start += next_range->Size();
    new_range_ = false;
    return kNext;
  };

  const uint8_t* null_bitmap_;
  int64_t entry_offset_;
  ::arrow::internal::BitmapReader valid_bits_reader_;
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
  using Node = ::arrow::util::variant<NullableTerminalNode, ListNode, LargeListNode,
                                      FixedSizeListNode, NullableNode,
                                      AllPresentTerminalNode, AllNullsTerminalNode>;
  std::vector<Node> path;
  std::shared_ptr<Array> primitive_array;
  int16_t max_def_level = 0;
  int16_t max_rep_level = 0;
  bool has_dictionary;
};

/// \brief Contains logic for writing a single leaf node to parquet.
/// This tracks the path from root to leaf.
Status WritePath(ElementRange start_range, PathInfo* path_info,
                 ArrowWriteContext* arrow_context,
                 MultipathLevelBuilder::CallbackFunction writer) {
  std::vector<ElementRange> stack(path_info->path.size());
  MultipathLevelBuilderResult builder_result;
  builder_result.leaf_array = path_info->primitive_array;

  if (path_info->max_def_level == 0) {
    int64_t leaf_length = builder_result.leaf_array->length();
    builder_result.def_rep_level_count = leaf_length;
    builder_result.post_list_visited_elements.push_back({0, leaf_length});
    return writer(builder_result);
  }
  stack[0] = start_range;
  RETURN_NOT_OK(
      arrow_context->def_levels_buffer->Resize(/*new_size=*/0, /*shrink_to_fit*/ false));
  PathWriteContext context(arrow_context->memory_pool, arrow_context->def_levels_buffer);

  auto stack_base = &stack[0];
  auto stack_position = stack_base;
  IterationResult result;
  while (stack_position >= stack_base) {
    PathInfo::Node& node = path_info->path[stack_position - stack_base];
    // Blocks ordered roughly in likely path usage.
    if (holds_alternative<NullableNode>(node)) {
      result = ::arrow::util::get<NullableNode>(node).Run(stack_position,
                                                          stack_position + 1, &context);
    } else if (holds_alternative<ListNode>(node)) {
      result = ::arrow::util::get<ListNode>(node).Run(stack_position, stack_position + 1,
                                                      &context);
    } else if (holds_alternative<NullableTerminalNode>(node)) {
      result =
          ::arrow::util::get<NullableTerminalNode>(node).Run(*stack_position, &context);
    } else if (holds_alternative<FixedSizeListNode>(node)) {
      result = ::arrow::util::get<FixedSizeListNode>(node).Run(
          stack_position, stack_position + 1, &context);
    } else if (holds_alternative<AllPresentTerminalNode>(node)) {
      result =
          ::arrow::util::get<AllPresentTerminalNode>(node).Run(*stack_position, &context);
    } else if (holds_alternative<AllNullsTerminalNode>(node)) {
      result =
          ::arrow::util::get<AllNullsTerminalNode>(node).Run(*stack_position, &context);
    } else if (holds_alternative<LargeListNode>(node)) {
      result = ::arrow::util::get<LargeListNode>(node).Run(stack_position,
                                                           stack_position + 1, &context);
    } else {
      return Status::UnknownError("should never get here.", node.index());
    }
    if (ARROW_PREDICT_FALSE(result == kError)) {
      DCHECK(!context.last_status.ok());
      return context.last_status;
    }
    stack_position += static_cast<int>(result);
  }
  RETURN_NOT_OK(context.last_status);
  builder_result.def_rep_level_count = context.def_levels.length();

  if (context.rep_levels.length() > 0) {
    builder_result.rep_levels = context.rep_levels.data();
    std::swap(builder_result.post_list_visited_elements, context.visited_elements);
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
  std::vector<PathInfo::Node> cleaned_up_nodes;
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

  void operator()(NullableTerminalNode& arg) {}
  void operator()(AllPresentTerminalNode& arg) {}
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
  visitor.cleaned_up_nodes.reserve(info.path.size());
  for (size_t x = 0; x < info.path.size(); x++) {
    ::arrow::util::visit(visitor, info.path[x]);
  }
  return info;
}

class PathBuilder {
 public:
  explicit PathBuilder(bool start_nullable) : last_nullable_(start_nullable) {}
  template <typename T>
  void AddTerminalInfo(const T& array) {
    if (last_nullable_) {
      info_.max_def_level++;
    }
    if (array.null_count() == 0) {
      info_.path.push_back(AllPresentTerminalNode{info_.max_def_level});
    } else if (array.null_count() == array.length()) {
      info_.path.push_back(AllNullsTerminalNode(info_.max_def_level - 1));
    } else {
      info_.path.push_back(NullableTerminalNode(array.null_bitmap_data(), array.offset(),
                                                info_.max_def_level));
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
    ListPathNode<VarRangeSelector<typename T::offset_type>> node(
        VarRangeSelector<typename T::offset_type>{array.raw_value_offsets() +
                                                  array.data()->offset},
        info_.max_rep_level, info_.max_def_level - 1);
    info_.path.push_back(node);
    last_nullable_ = array.list_type()->value_field()->nullable();
    return VisitInline(*array.values());
  }

  Status Visit(const ::arrow::DictionaryArray& array) {
    // Only currently handle DictionaryArray where the dictionary is a
    // primitive type
    if (array.dict_type()->value_type()->num_children() > 0) {
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
    if (!last_nullable_) {
      return;
    }
    info_.max_def_level++;
    if (array.null_count() == 0) {
      // Don't add anything because there won't be any point checking
      // null values for the array.  There will always be at least
      // one more array to handle nullability.
      return;
    }
    if (array.null_count() == array.length()) {
      info_.path.push_back(AllNullsTerminalNode(info_.max_def_level - 1));
      return;
    }
    info_.path.push_back(NullableNode(array.null_bitmap_data(), array.offset(),
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
      last_nullable_ = array.type()->child(x)->nullable();
      RETURN_NOT_OK(VisitInline(*array.field(x)));
      info_ = info_backup;
    }
    return Status::OK();
  }

  Status Visit(const ::arrow::FixedSizeListArray& array) {
    MaybeAddNullable(array);
    int32_t list_size = array.list_type()->list_size();
    if (list_size == 0) {
      info_.max_def_level++;
    }
    info_.max_rep_level++;
    info_.path.push_back(FixedSizeListNode(FixedSizedRangeSelector{list_size},
                                           info_.max_rep_level, info_.max_def_level));
    last_nullable_ = array.list_type()->value_field()->nullable();
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
  bool last_nullable_;
};

Status PathBuilder::VisitInline(const Array& array) {
  return ::arrow::VisitArrayInline(array, this);
}

#undef RETURN_IF_ERROR
}  // namespace

Status MultipathLevelBuilder::Write(const Array& array, bool array_nullable,
                                    ArrowWriteContext* context,
                                    MultipathLevelBuilder::CallbackFunction callback) {
  PathBuilder constructor(array_nullable);
  RETURN_NOT_OK(VisitArrayInline(array, &constructor));
  ElementRange start_range{0, array.length()};
  for (auto& write_path_info : constructor.paths()) {
    RETURN_NOT_OK(WritePath(start_range, &write_path_info, context, callback));
  }
  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
