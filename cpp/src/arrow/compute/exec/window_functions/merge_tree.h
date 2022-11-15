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

#pragma once

#include <cstdint>
#include <functional>
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec/window_functions/bit_vector_navigator.h"
#include "arrow/compute/exec/window_functions/window_frame.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace compute {

// Represents a fixed set of 2D points with attributes X and Y.
// Values of each attribute across points are unique integers in the range
// [0, N - 1] for N points.
// Supports two kinds of queries:
// a) Nth element
// b) Box count / box filter
//
// Nth element query: filter points using range predicate on Y, return the nth
// smallest X within the remaining points.
//
// Box count query: filter points using range predicate on X and less than
// predicate on Y, count and return the number of remaining points.
//
class MergeTree {
 public:
  // Constant used in description of boundaries of the ranges of node elements
  // to indicate an empty range.
  //
  static constexpr int64_t kEmptyRange = -1;

  // Constant returned from nth element query when the result is outside of the
  // input range of elements.
  //
  static constexpr int64_t kOutOfBounds = -1;

  int num_levels() const { return bit_util::Log2(length_) + 1; }

  Status Build(int64_t length, int level_begin, int64_t* permutation_of_X,
               ParallelForStream& parallel_fors);

  // Internal state of a single box count / box filter query preserved between
  // visiting different levels of the merge tree.
  //
  struct BoxQueryState {
    // End positions for ranges of elements sorted on Y belonging to up
    // to two nodes from a single level that are active for this box query.
    //
    // There may be between 0 and 2 nodes represented in this state.
    // If it is less than 2 we mark the remaining elements in the ends array
    // with the kEmptyRange constant.
    //
    int64_t ends[2];
  };

  // Input and mutable state for a series of box queries
  //
  struct BoxQueryRequest {
    // Callback for reporting partial query results for a batch of queries and a
    // single level.
    //
    // The arguments are:
    // - tree level,
    // - range of query indices (begin and end),
    // - two arrays with one element per query in a batch containing two
    // cursors. Each cursor represents a prefix of elements (sorted on Y) inside
    // a single node from the specified level that satisfy the query. Each
    // cursor can be set to kEmptyRange constant, which indicates empty result
    // set.
    //
    using BoxQueryCallback = std::function<void(int, int64_t, int64_t, const int64_t*,
                                                const int64_t*, ThreadContext&)>;
    BoxQueryCallback report_results_callback_;
    // Number of queries
    //
    int64_t num_queries;
    // The predicate on X can represent a union of multiple ranges,
    // but all queries need to use exactly the same number of ranges.
    //
    int num_x_ranges;
    // Range predicates on X.
    //
    // Since every query can use multiple ranges it is an array of arrays.
    //
    // Beginnings and ends of corresponding ranges are stored in separate arrays
    // of arrays.
    //
    const int64_t* xbegins[WindowFrames::kMaxRangesInFrame];
    const int64_t* xends[WindowFrames::kMaxRangesInFrame];
    // Range of tree levels to traverse.
    //
    // If the range does not represent the entire tree, then only part of
    // the tree will be processed, starting from the query states provided in
    // the array below. The array of query states will be updated afterwards,
    // allowing subsequent call to continue processing for the remaining tree
    // levels.
    //
    int level_begin;
    int level_end;
    // Query state is a pair of cursors pointing to two locations in two nodes
    // in a single (level_begin) level of the tree. A cursor can be seen as a
    // prefix of elements (sorted on Y) that belongs to a single node. The
    // number of cursors may be less than 2, in which case one or two cursors
    // are set to the kEmptyRange constant.
    //
    // Initially the first cursor should be set to exclusive upper bound on Y
    // (kEmptyRange if 0) and the second cursor to kEmptyRange.
    //
    // If we split query processing into multiple steps (level_end > 0), then
    // the state will be updated.
    //
    BoxQueryState* states;
  };

  void BoxQuery(const BoxQueryRequest& queries, ThreadContext& thread_ctx);

  void BoxCountQuery(int64_t num_queries, int num_x_ranges_per_query,
                     const int64_t** x_begins, const int64_t** x_ends,
                     const int64_t* y_ends, int64_t* results,
                     ThreadContext& thread_context);

  // Internal state of a single nth element query preserved between visiting
  // different levels of the merge tree.
  struct NthQueryState {
    // Position within a single node from a single level that encodes:
    // - the node from which the search will continue,
    // - the relative position of the output X within the sorted sequence of X
    // of points associated with this node.
    int64_t pos;
  };

  // Input and mutable state for a series of nth element queries
  //
  struct NthQueryRequest {
    int64_t num_queries;
    // Range predicates on Y.
    //
    // Since every query can use multiple ranges it is an array of arrays.
    //
    // Beginnings and ends of corresponding ranges are stored in separate arrays
    // of arrays.
    //
    int num_y_ranges;
    const int64_t** ybegins;
    const int64_t** yends;
    // State encodes a node (all states will point to nodes from the same level)
    // and the N for the Nth element we are looking for.
    //
    // When the query starts it is set directly to N in the query (N part is the
    // input and node part is zero).
    //
    // When the query finishes it is set to the query result - a value of X that
    // is Nth in the given range of Y (node part is the result and N part is
    // zero).
    //
    NthQueryState* states;
  };

  void NthQuery(const NthQueryRequest& queries, ThreadContext& thread_ctx);

 private:
  // Return true if the given array of N elements contains a permutation of
  // integers from [0, N - 1] range.
  //
  bool IsPermutation(int64_t length, const int64_t* values);

  // Find the beginning (index in the split bit vector) of the merge tree node
  // for a given position within the range of bits for that node.
  //
  inline int64_t NodeBegin(int level, int64_t pos) const;

  // Find the end (index one after the last) of the merge tree node given a
  // position within its range.
  //
  // All nodes of the level have (1 << level) elements except for the last that
  // can be truncated.
  //
  inline int64_t NodeEnd(int level, int64_t pos) const;

  // Use split bit vector and bit vector navigator to map beginning of a
  // range of Y from a parent node to both child nodes.
  //
  // If the child range is empty return kEmptyRange for it.
  //
  inline void CascadeBegin(int from_level, int64_t begin, int64_t* lbegin,
                           int64_t* rbegin) const;

  // Same as CascadeBegin but for the end (one after the last element) of the
  // range.
  //
  // The difference is that end offset within the node can have values in
  // [1; S] range, where S is the size of the node, while the beginning offset
  // is in [0; S - 1].
  //
  inline void CascadeEnd(int from_level, int64_t end, int64_t* lend, int64_t* rend) const;

  // Fractional cascading for a single element of a parent node.
  //
  inline int64_t CascadePos(int from_level, int64_t pos) const;

  enum class NodeSubsetType { EMPTY, PARTIAL, FULL };

  // Check whether the intersection with respect to X axis of the range
  // represented by the node and a given range is: a) empty, b) full node, c)
  // partial node.
  //
  inline NodeSubsetType NodeIntersect(int level, int64_t pos, int64_t begin, int64_t end);

  // Split a subset of elements from the source level.
  //
  // When MULTIPLE_SOURCE_NODES == false,
  // then the subset must be contained in a single source node (it can also
  // represent the entire source node).
  //
  template <typename T, bool MULTIPLE_SOURCE_NODES>
  void SplitSubsetImp(const BitWeaverNavigator& split_bits, int source_level,
                      const T* source_level_vector, T* target_level_vector,
                      int64_t read_begin, int64_t read_end, int64_t write_begin_bit0,
                      int64_t write_begin_bit1, ThreadContext& thread_ctx);

  // Split a subset of elements from the source level.
  //
  template <typename T>
  void SplitSubset(int source_level, const T* source_level_vector, T* target_level_vector,
                   int64_t read_begin, int64_t read_end, ThreadContext& thread_ctx);

  void SetMorselLoglen(int morsel_loglen);

  // Load up to 64 bits from interleaved bit vector starting at an arbitrary bit
  // index.
  //
  inline uint64_t GetWordUnaligned(const BitWeaverNavigator& source, int64_t bit_index,
                                   int num_bits = 64);

  // Set a subsequence of bits within a single word inside an interleaved bit
  // vector.
  //
  inline void UpdateWord(BitWeaverNavigator& target, int64_t bit_index, int num_bits,
                         uint64_t bits);

  // Copy bits while reading and writing aligned 64-bit words only.
  //
  // Input and output bit vectors may be logical bit vectors inside a
  // collection of interleaved bit vectors of the same length (accessed
  // using BitWeaverNavigator).
  //
  void BitMemcpy(const BitWeaverNavigator& source, BitWeaverNavigator& target,
                 int64_t source_begin, int64_t source_end, int64_t target_begin);

  void GetChildrenBoundaries(const BitWeaverNavigator& split_bits,
                             int64_t num_source_nodes, int64_t* source_node_begins,
                             int64_t* target_node_begins);

  void BuildUpperSliceMorsel(int level_begin, int64_t* permutation_of_X,
                             int64_t* temp_permutation_of_X, int64_t morsel_index,
                             ThreadContext& thread_ctx);

  void CombineUpperSlicesMorsel(int level_begin, int64_t output_morsel,
                                int64_t* input_permutation_of_X,
                                int64_t* output_permutation_of_X,
                                ThreadContext& thread_ctx);

  void BuildLower(int level_begin, int64_t morsel_index, int64_t* begin_permutation_of_X,
                  int64_t* temp_permutation_of_X, ThreadContext& thread_ctx);

  bool NOutOfBounds(const NthQueryRequest& queries, int64_t query_index);

  void DebugPrintToFile(const char* filename) const;

  static constexpr int kBitMatrixBandSize = 4;
  static constexpr int kMinMorselLoglen = BitVectorWithCounts::kLogBitsPerBlock;

  int morsel_loglen_;
  int64_t length_;

  BitMatrixWithCounts bit_matrix_;
  BitMatrixWithCounts bit_matrix_upper_slices_;

  // Temp buffer used while building the tree for double buffering of the
  // permutation of X (buffer for upper level is used to generate buffer for
  // lower level, then we traverse down and swap the buffers).
  // The other buffer is provided by the caller of the build method.
  //
  std::vector<int64_t> temp_permutation_of_X_;
};

}  // namespace compute
}  // namespace arrow
