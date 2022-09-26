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

#include "arrow/compute/exec/window_functions/splay_tree.h"

namespace arrow {
namespace compute {

SplayTree::SplayTree() { Clear(); }

void SplayTree::Insert(int64_t value) {
  index_t rank;
  index_t parent_id;
  int parent_side;
  index_t node_id;
  Find(value, kCountStar, &parent_id, &parent_side, &node_id, &rank);

  if (node_id != kNilId) {
    ++nodes_[node_id].value_count;
    ++nodes_[node_id].subtree_count[kCountStar];
    while (parent_id != kNilId) {
      NodeType& node = nodes_[parent_id];
      ++node.subtree_count[kCountStar];
      parent_id = node.parent_id;
    }

#ifndef NDEBUG
    ValidateTree();
#endif

    return;
  }

  index_t new_node_id = AllocateNode();
  NodeType& new_node = nodes_[new_node_id];
  new_node.value = value;
  new_node.value_count = 0;
  new_node.subtree_count[0] = new_node.subtree_count[1] = 0;
  new_node.parent_id = parent_id;
  new_node.child_id[0] = new_node.child_id[1] = kNilId;
  if (parent_id == kNilId) {
    root_id_ = new_node_id;
  } else {
    nodes_[parent_id].child_id[parent_side] = new_node_id;
    Splay(new_node_id);
  }
  nodes_[root_id_].value_count = 1;
  for (int i = 0; i < 2; ++i) {
    ++nodes_[root_id_].subtree_count[i];
  }

#ifndef NDEBUG
  ValidateTree();
#endif
}

void SplayTree::Remove(int64_t value) {
  index_t rank;
  index_t parent_id;
  int parent_side;
  index_t node_id;
  Find(value, kCountStar, &parent_id, &parent_side, &node_id, &rank);

  // Noop if value is not present
  if (node_id == kNilId) {
    return;
  }

  NodeType* node = &nodes_[node_id];

  // Decrease subtree_count for all ancestors of the node.
  //
  for (index_t x = parent_id; x != kNilId; x = nodes_[x].parent_id) {
    nodes_[x].subtree_count[kCountStar] -= 1;
  }
  --node->value_count;
  --node->subtree_count[kCountStar];

  if (node->value_count > 0) {
#ifndef NDEBUG
    ValidateTree();
#endif

    return;
  }

  for (index_t x = parent_id; x != kNilId; x = nodes_[x].parent_id) {
    nodes_[x].subtree_count[kCountDistinctValue] -= 1;
  }
  --node->subtree_count[kCountDistinctValue];

  if (node->child_id[0] != kNilId && node->child_id[1] != kNilId) {
    index_t prev_node_id = node->child_id[0];
    while (nodes_[prev_node_id].child_id[1] != kNilId) {
      prev_node_id = nodes_[prev_node_id].child_id[1];
    }
    NodeType& prev_node = nodes_[prev_node_id];
    for (index_t x = nodes_[prev_node_id].parent_id; x != node_id;
         x = nodes_[x].parent_id) {
      nodes_[x].subtree_count[kCountStar] -= prev_node.value_count;
      nodes_[x].subtree_count[kCountDistinctValue] -= 1;
    }
    index_t prev_node_parent_id = nodes_[prev_node_id].parent_id;
    if (nodes_[prev_node_parent_id].child_id[0] == prev_node_id) {
      nodes_[prev_node_parent_id].child_id[0] = nodes_[prev_node_id].child_id[0];
    } else {
      nodes_[prev_node_parent_id].child_id[1] = nodes_[prev_node_id].child_id[0];
    }
    if (nodes_[prev_node_id].child_id[0] != kNilId) {
      nodes_[nodes_[prev_node_id].child_id[0]].parent_id = prev_node_parent_id;
    }
    nodes_[prev_node_id].parent_id = kNilId;

    node->value = prev_node.value;
    node->value_count = prev_node.value_count;

    DeallocateNode(prev_node_id);

#ifndef NDEBUG
    ValidateTree();
#endif

    return;
  }

  for (int side = 0; side < 2; ++side) {
    if (node->child_id[side] == kNilId) {
      nodes_[parent_id].child_id[parent_side] = node->child_id[1 - side];
      nodes_[node->child_id[1 - side]].parent_id = parent_id;
      if (parent_id == kNilId) {
        root_id_ = node->child_id[1 - side];
      } else {
        Splay(parent_id);
      }
      DeallocateNode(node_id);

#ifndef NDEBUG
      ValidateTree();
#endif

      return;
    }
  }
}

void SplayTree::Clear() {
  nodes_.clear();
  empty_slots_.clear();
  root_id_ = kNilId;
  nodes_.push_back(NodeType());
  nodes_[kNilId].value_count = 0;
  for (int i = 0; i < 2; ++i) {
    nodes_[kNilId].subtree_count[i] = 0;
  }
}

// Value does not need to be present
int64_t SplayTree::Rank(bool ties_low, int64_t value) {
  index_t rank;
  index_t parent_id;
  int parent_side;
  index_t node_id;
  Find(value, kCountStar, &parent_id, &parent_side, &node_id, &rank);
  if (ties_low) {
    return rank + 1;
  }
  return rank + nodes_[node_id].value_count;
}

// Value does not need to be present
int64_t SplayTree::DenseRank(int64_t value) {
  index_t rank;
  index_t parent_id;
  int parent_side;
  index_t node_id;
  Find(value, kCountDistinctValue, &parent_id, &parent_side, &node_id, &rank);
  return rank + 1;
}

SplayTree::index_t SplayTree::AllocateNode() {
  index_t new_node_id;
  if (empty_slots_.empty()) {
    new_node_id = static_cast<index_t>(nodes_.size());
    nodes_.push_back(NodeType());
  } else {
    new_node_id = empty_slots_.back();
    empty_slots_.pop_back();
  }
  return new_node_id;
}

void SplayTree::DeallocateNode(index_t node_id) { empty_slots_.push_back(node_id); }

void SplayTree::SwitchParent(index_t old_parent_id, int old_child_side,
                             index_t new_parent_id, int new_child_side) {
  NodeType& old_parent = nodes_[old_parent_id];
  NodeType& new_parent = nodes_[new_parent_id];
  index_t child_id = old_parent.child_id[old_child_side];
  NodeType& child = nodes_[child_id];
  index_t replaced_child_id = new_parent.child_id[new_child_side];
  NodeType& replaced_child = nodes_[replaced_child_id];

  // New parent cannot be a child of old parent.
  ARROW_DCHECK(new_parent.parent_id != old_parent_id);

  child.parent_id = new_parent_id;
  replaced_child.parent_id = kNilId;
  new_parent.child_id[new_child_side] = child_id;
  old_parent.child_id[old_child_side] = kNilId;

  for (int i = 0; i < 2; ++i) {
    new_parent.subtree_count[i] +=
        child.subtree_count[i] - replaced_child.subtree_count[i];
    old_parent.subtree_count[i] -= child.subtree_count[i];
  }
}

//     parent         node            |
//    /      \       /    \           |
//   node     y --> x      parent     |
//  /    \                /      \    |
// x      mid            mid      y   |
void SplayTree::Zig(index_t node_id, index_t parent_id, int parent_side) {
  NodeType& node = nodes_[node_id];
  NodeType& parent = nodes_[parent_id];

  // zig is only called when parent is the root of the tree
  //
  ARROW_DCHECK(parent.parent_id == kNilId);

  // Rearrange tree nodes
  //
  SwitchParent(node_id, 1 - parent_side, parent_id, parent_side);

  // At this point we have:
  //
  //     nil          nil       |
  //      |            |        |
  //    node     +   parent     |
  //   /    \       /      \    |
  //  x      nil   mid      y   |
  //

  // Connect parent to node
  //
  node.child_id[1 - parent_side] = parent_id;
  parent.parent_id = node_id;
  for (int i = 0; i < 2; ++i) {
    node.subtree_count[i] += parent.subtree_count[i];
  }
  root_id_ = node_id;
}

//          grandparent         node                          |
//         /           \       /    \                         |
//        parent        y     x      parent                   |
//       /      \         -->       /       \                 |
//      node     mid1              mid0      grandparent      |
//     /    \                               /           \     |
//    x      mid0                          mid1          y    |
void SplayTree::ZigZig(index_t node_id, index_t parent_id, index_t grandparent_id,
                       int parent_side) {
  NodeType& node = nodes_[node_id];
  NodeType& parent = nodes_[parent_id];
  NodeType& grandparent = nodes_[grandparent_id];

  // Rearrange tree nodes.
  // The order of the calls below is important.
  //
  SwitchParent(parent_id, 1 - parent_side, grandparent_id, parent_side);
  SwitchParent(node_id, 1 - parent_side, parent_id, parent_side);

  // At this point we have:
  //
  //     nil          nil                z          |
  //      |            |                 |          |
  //    node     +   parent     +   grandparent     |
  //   /    \       /      \       /           \    |
  //  x      nil   mid0     nil   mid1          y   |
  //

  node.parent_id = grandparent.parent_id;
  if (node.parent_id != kNilId) {
    int side = (nodes_[node.parent_id].child_id[0] == grandparent_id) ? 0 : 1;
    nodes_[node.parent_id].child_id[side] = node_id;
  }

  // Connect grandparent to parent
  //
  parent.child_id[1 - parent_side] = grandparent_id;
  grandparent.parent_id = parent_id;
  for (int i = 0; i < 2; ++i) {
    parent.subtree_count[i] += grandparent.subtree_count[i];
  }

  // Connect parent to node
  //
  node.child_id[1 - parent_side] = parent_id;
  parent.parent_id = node_id;
  for (int i = 0; i < 2; ++i) {
    node.subtree_count[i] += parent.subtree_count[i];
  }
  if (root_id_ == grandparent_id) {
    root_id_ = node_id;
  }
}

//         grandparent                    node                |
//        /           \                  /     \              |
//       parent        y                parent  grandparent   |
//      /      \          -->          /\      /    \         |
//     x        node                  x  mid0 mid1   y        |
//             /    \                                         |
//            mid0   mid1                                     |
void SplayTree::ZigZag(index_t node_id, index_t parent_id, index_t grandparent_id,
                       int parent_side, int grandparent_side) {
  NodeType& node = nodes_[node_id];
  NodeType& parent = nodes_[parent_id];
  NodeType& grandparent = nodes_[grandparent_id];

  // Rearrange tree nodes.
  // The order of the calls below is important.
  //
  SwitchParent(node_id, parent_side, grandparent_id, 1 - parent_side);
  if (grandparent.child_id[1 - parent_side] != kNilId) {
    for (int i = 0; i < 2; ++i) {
      parent.subtree_count[i] -=
          nodes_[grandparent.child_id[1 - parent_side]].subtree_count[i];
    }
  }
  SwitchParent(node_id, 1 - parent_side, parent_id, parent_side);

  // At this point we have:
  //
  //     nil          nil                 z           |
  //      |            |                  |           |
  //    node     +   parent      +    grandparent     |
  //   /    \       /      \         /           \    |
  //  nil    nil   x        mid0    mid1          y   |
  //

  node.parent_id = grandparent.parent_id;
  if (node.parent_id != kNilId) {
    int side = (nodes_[node.parent_id].child_id[0] == grandparent_id) ? 0 : 1;
    nodes_[node.parent_id].child_id[side] = node_id;
  }

  // Connect parent and grandparent to node
  //
  node.child_id[1 - parent_side] = parent_id;
  node.child_id[parent_side] = grandparent_id;
  parent.parent_id = node_id;
  grandparent.parent_id = node_id;
  for (int i = 0; i < 2; ++i) {
    node.subtree_count[i] += parent.subtree_count[i] + grandparent.subtree_count[i];
  }
  if (root_id_ == grandparent_id) {
    root_id_ = node_id;
  }
}

void SplayTree::Splay(index_t node_id) {
  for (;;) {
    NodeType& node = nodes_[node_id];
    index_t parent_id = node.parent_id;
    if (parent_id == kNilId) {
      break;
    }
    NodeType& parent = nodes_[parent_id];
    int parent_side = (parent.child_id[0] == node_id ? 0 : 1);
    index_t grandparent_id = parent.parent_id;
    if (grandparent_id == kNilId) {
      Zig(node_id, parent_id, parent_side);
      continue;
    }
    NodeType& grandparent = nodes_[grandparent_id];
    int grandparent_side = (grandparent.child_id[0] == parent_id ? 0 : 1);
    if (parent_side == grandparent_side) {
      ZigZig(node_id, parent_id, grandparent_id, parent_side);
    } else {
      ZigZag(node_id, parent_id, grandparent_id, parent_side, grandparent_side);
    }
#ifndef NDEBUG
    ValidateTree();
#endif
  }
}

// Find the node with the given value if exists.
// Otherwise find the place in the tree where the new value would be
// inserted (its parent and parent's child index).
//
void SplayTree::Find(int64_t value, int counter_id, index_t* parent_id, int* parent_side,
                     index_t* node_id, index_t* count_less) const {
  *parent_id = kNilId;
  *parent_side = 0;
  *count_less = 0;

  *node_id = root_id_;
  for (;;) {
    if (*node_id == kNilId) {
      return;
    }
    const NodeType& node = nodes_[*node_id];
    const NodeType& left_child = nodes_[node.child_id[0]];
    if (value == node.value) {
      *count_less += left_child.subtree_count[counter_id];
      return;
    }
    int direction = value < node.value ? 0 : 1;
    if (direction == 1) {
      *count_less += left_child.subtree_count[counter_id] +
                     (counter_id == kCountStar ? node.value_count : 1);
    }
    *parent_id = *node_id;
    *parent_side = direction;
    *node_id = node.child_id[direction];
  }
}

void SplayTree::ValidateVisit(index_t node_id, index_t* count, index_t* count_distinct) {
  ARROW_DCHECK(node_id != kNilId);
  ARROW_DCHECK(nodes_[node_id].parent_id == kNilId ||
               nodes_[nodes_[node_id].parent_id].child_id[0] == node_id ||
               nodes_[nodes_[node_id].parent_id].child_id[1] == node_id);
  *count = nodes_[node_id].value_count;
  *count_distinct = nodes_[node_id].value_count > 0 ? 1 : 0;
  for (int side = 0; side < 2; ++side) {
    if (nodes_[node_id].child_id[side] != kNilId) {
      index_t count_child, count_distinct_child;
      ARROW_DCHECK(nodes_[nodes_[node_id].child_id[side]].parent_id == node_id);
      ValidateVisit(nodes_[node_id].child_id[side], &count_child, &count_distinct_child);
      *count += count_child;
      *count_distinct += count_distinct_child;
    }
  }
  bool count_correct = (*count == nodes_[node_id].subtree_count[kCountStar]);
  bool count_distinct_correct =
      (*count_distinct == nodes_[node_id].subtree_count[kCountDistinctValue]);
  if (!count_correct || !count_distinct_correct) {
    Print();
  }
  ARROW_DCHECK(count_correct);
  ARROW_DCHECK(count_distinct_correct);
}

void SplayTree::ValidateTree() {
  index_t count = 0;
  index_t count_distinct = 0;
  if (root_id_ != kNilId) {
    ValidateVisit(root_id_, &count, &count_distinct);
  }
  ARROW_DCHECK(nodes_.size() <= empty_slots_.size() + count_distinct +
                                    /*extra one for kNilId*/ 1 + 1);
}

template <typename T>
int SplayTree::Print_StrLen(const T& value) {
  std::string s = std::to_string(value);
  return static_cast<int>(s.length());
}

std::string SplayTree::Print_Label(index_t node_id) const {
  const NodeType& node = nodes_[node_id];
  return std::string("(") + std::to_string(node.value) + "," +
         std::to_string(node.value_count) + "," + std::to_string(node.subtree_count[0]) +
         "," + std::to_string(node.subtree_count[1]) + ")";
}

void SplayTree::Print_BoxWH(index_t node_id, std::map<index_t, PrintBox>& boxes) {
  // Recursively compute box size for left and right child if they exist
  //
  bool has_child[2];
  for (int ichild = 0; ichild < 2; ++ichild) {
    has_child[ichild] = (nodes_[node_id].child_id[ichild] != kNilId);
    if (has_child[ichild]) {
      Print_BoxWH(nodes_[node_id].child_id[ichild], boxes);
    }
  }

  PrintBox box;
  box.x = box.y = 0;
  int label_size = static_cast<int>(Print_Label(node_id).length());

  if (!has_child[0] && !has_child[1]) {
    box.root_x = 0;
    box.w = label_size;
    box.h = 1;
  } else if (has_child[0] && has_child[1]) {
    // Both children
    PrintBox left_box = boxes.find(nodes_[node_id].child_id[0])->second;
    PrintBox right_box = boxes.find(nodes_[node_id].child_id[1])->second;
    box.w = left_box.w + right_box.w + 1;
    box.h = std::max(left_box.h, right_box.h) + 4;
    int mid = (left_box.w + right_box.w + 1) / 2;
    box.root_x =
        std::min(std::max(mid, left_box.root_x), left_box.w + 1 + right_box.root_x);
    box.w = std::max(box.w, box.root_x + label_size);
  } else {
    // One child
    int ichild = (has_child[0] ? 0 : 1);
    PrintBox child_box = boxes.find(nodes_[node_id].child_id[ichild])->second;
    box.h = child_box.h + 4;
    box.w = child_box.w;
    box.root_x = box.w / 2;
    box.w = std::max(box.w, box.root_x + label_size);
  }

  boxes.insert(std::make_pair(node_id, box));
}

void SplayTree::Print_BoxXY(int x, int y, index_t node_id,
                            std::map<index_t, PrintBox>& boxes) {
  PrintBox& box = boxes.find(node_id)->second;
  box.root_x += x;
  box.x += x;
  box.y += y;
  bool has_child[2];
  for (int ichild = 0; ichild < 2; ++ichild) {
    has_child[ichild] = (nodes_[node_id].child_id[ichild] != kNilId);
  }
  if (has_child[0] && has_child[1]) {
    Print_BoxXY(x, y + 4, nodes_[node_id].child_id[0], boxes);
    Print_BoxXY(x + boxes.find(nodes_[node_id].child_id[0])->second.w + 1, y + 4,
                nodes_[node_id].child_id[1], boxes);
  } else if (has_child[0] || has_child[1]) {
    Print_BoxXY(x, y + 4, nodes_[node_id].child_id[has_child[0] ? 0 : 1], boxes);
  }
}

void SplayTree::Print_PutChar(std::vector<std::vector<char>>& canvas, int x, int y,
                              char c) {
  if (y >= static_cast<int>(canvas.size())) {
    canvas.resize(y + 1);
  }
  if (x >= static_cast<int>(canvas[y].size())) {
    canvas[y].resize(x + 1);
  }
  canvas[y][x] = c;
}

void SplayTree::Print_PutString(std::vector<std::vector<char>>& canvas, int x, int y,
                                std::string str) {
  for (int i = 0; i < static_cast<int>(str.length()); ++i) {
    Print_PutChar(canvas, x + i, y, str[i]);
  }
}

void SplayTree::Print_Node(index_t node_id, std::map<index_t, PrintBox>& boxes,
                           std::vector<std::vector<char>>& canvas) {
  PrintBox box = boxes.find(node_id)->second;
  Print_PutString(canvas, box.root_x, box.y, Print_Label(node_id));
  for (int ichild = 0; ichild < 2; ++ichild) {
    if (nodes_[node_id].child_id[ichild] != kNilId) {
      PrintBox child_box = boxes.find(nodes_[node_id].child_id[ichild])->second;
      int top_x = child_box.root_x;
      int bottom_x = box.root_x + ichild;
      Print_PutChar(canvas, top_x, box.y + 3, '|');
      for (int x = std::min(bottom_x, top_x); x <= std::max(bottom_x, top_x); ++x) {
        Print_PutChar(canvas, x, box.y + 2, '-');
      }
      Print_PutChar(canvas, bottom_x, box.y + 1, '|');
      Print_Node(nodes_[node_id].child_id[ichild], boxes, canvas);
    }
  }
}

void SplayTree::Print() {
  if (root_id_ == kNilId) {
    return;
  }
  std::map<index_t, PrintBox> boxes;
  Print_BoxWH(root_id_, boxes);
  Print_BoxXY(0, 0, root_id_, boxes);
  std::vector<std::vector<char>> canvas;
  Print_Node(root_id_, boxes, canvas);

  const char* filename = "splay_tree_output.txt";
  FILE* fout;
#if defined(_MSC_VER) && _MSC_VER >= 1400
  fopen_s(&fout, filename, "wt");
#else
  fout = fopen(filename, "wt");
#endif

  for (size_t y = 0; y < canvas.size(); ++y) {
    for (size_t x = 0; x < canvas[y].size(); ++x) {
      fprintf(fout, "%c", canvas[y][x]);
    }
    fprintf(fout, "\n");
  }
  fclose(fout);
}

}  // namespace compute
}  // namespace arrow
