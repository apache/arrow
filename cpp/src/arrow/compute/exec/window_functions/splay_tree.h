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
#include <map>
#include <string>
#include <vector>
#include "arrow/compute/exec/util.h"  // for ARROW_DCHECK

namespace arrow {
namespace compute {

class SplayTree {
 public:
  using index_t = int32_t;

  SplayTree();

  void Insert(int64_t value);

  void Remove(int64_t value);

  void Clear();

  // Value does not need to be present
  int64_t Rank(bool ties_low, int64_t value);

  // Value does not need to be present
  int64_t DenseRank(int64_t value);

 private:
  static constexpr index_t kNilId = 0;
  static constexpr int kCountStar = 0;
  static constexpr int kCountDistinctValue = 1;

  struct NodeType {
    int64_t value;
    index_t value_count;

    index_t subtree_count[2];

    index_t parent_id;
    index_t child_id[2];
  };

  std::vector<NodeType> nodes_;
  index_t root_id_;
  std::vector<index_t> empty_slots_;

  index_t AllocateNode();

  void DeallocateNode(index_t node_id);

  void SwitchParent(index_t old_parent_id, int old_child_side, index_t new_parent_id,
                    int new_child_side);

  //     parent         node                |
  //    /      \       /    \               |
  //   node     y --> x      parent         |
  //  /    \                /      \        |
  // x      mid            mid      y       |
  void Zig(index_t node_id, index_t parent_id, int parent_side);

  //          grandparent         node                          |
  //         /           \       /    \                         |
  //        parent        y     x      parent                   |
  //       /      \         -->       /       \                 |
  //      node     mid1              mid0      grandparent      |
  //     /    \                               /           \     |
  //    x      mid0                          mid1          y    |
  void ZigZig(index_t node_id, index_t parent_id, index_t grandparent_id,
              int parent_side);

  //         grandparent                    node                |
  //        /           \                  /     \              |
  //       parent        y                parent  grandparent   |
  //      /      \          -->          /\      /    \         |
  //     x        node                  x  mid0 mid1   y        |
  //             /    \                                         |
  //            mid0   mid1                                     |
  void ZigZag(index_t node_id, index_t parent_id, index_t grandparent_id, int parent_side,
              int grandparent_side);

  void Splay(index_t node_id);

  // Find the node with the given value if exists.
  // Otherwise find the place in the tree where the new value would be
  // inserted (its parent and parent's child index).
  //
  void Find(int64_t value, int counter_id, index_t* parent_id, int* parent_side,
            index_t* node_id, index_t* count_less) const;

  void ValidateVisit(index_t node_id, index_t* count, index_t* count_distinct);

  void ValidateTree();

  template <typename T>
  static int Print_StrLen(const T& value);

  struct PrintBox {
    int x, y, w, h;
    int root_x;
  };

  std::string Print_Label(index_t node_id) const;

  void Print_BoxWH(index_t node_id, std::map<index_t, PrintBox>& boxes);

  void Print_BoxXY(int x, int y, index_t node_id, std::map<index_t, PrintBox>& boxes);

  void Print_PutChar(std::vector<std::vector<char>>& canvas, int x, int y, char c);

  void Print_PutString(std::vector<std::vector<char>>& canvas, int x, int y,
                       std::string str);

  void Print_Node(index_t node_id, std::map<index_t, PrintBox>& boxes,
                  std::vector<std::vector<char>>& canvas);

  void Print();
};

}  // namespace compute
}  // namespace arrow
