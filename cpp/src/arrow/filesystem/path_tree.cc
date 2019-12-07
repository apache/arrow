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
//

#include "arrow/filesystem/path_tree.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/filesystem/path_util.h"
#include "arrow/status.h"

namespace arrow {
namespace fs {

using PathTreeByPathMap = std::unordered_map<std::string, std::shared_ptr<PathTree>>;

std::shared_ptr<PathTree> FindAncestor(const PathTreeByPathMap& directories,
                                       std::string path) {
  while (path != "") {
    auto parent = internal::GetAbstractPathParent(path).first;
    auto found = directories.find(parent);
    if (found != directories.end()) {
      return found->second;
    }

    path = std::move(parent);
  }

  return nullptr;
}

Status PathTree::Make(std::vector<FileStats> stats, PathForest* out) {
  PathTreeByPathMap directories;
  PathForest forest;

  auto link_parent_or_insert_root = [&directories, &forest](const FileStats& s) {
    if (s.path() == "") {
      return;
    }

    auto ancestor = FindAncestor(directories, s.path());
    auto node = std::make_shared<PathTree>(s);
    if (ancestor) {
      ancestor->AddChild(node);
    } else {
      forest.push_back(node);
    }

    if (s.type() == FileType::Directory) {
      directories[s.path()] = node;
    }
  };

  // Insert nodes by ascending path length, ensuring that nodes are always
  // inserted after their ancestors. Note that this strategy does not account
  // for special directories like '..'. It is expected that path are absolute.
  auto cmp = [](const FileStats& lhs, const FileStats& rhs) {
    return lhs.path().size() < rhs.path().size();
  };
  std::stable_sort(stats.begin(), stats.end(), cmp);
  std::for_each(stats.cbegin(), stats.cend(), link_parent_or_insert_root);

  *out = std::move(forest);
  return Status::OK();
}

Status PathTree::Make(std::vector<FileStats> stats, std::shared_ptr<PathTree>* out) {
  PathForest forest;
  RETURN_NOT_OK(Make(stats, &forest));

  auto size = forest.size();
  if (size > 1) {
    return Status::Invalid("Requested PathTree has ", size, " roots, but expected 1.");
  } else if (size == 1) {
    *out = forest[0];
  }

  return Status::OK();
}

std::ostream& operator<<(std::ostream& os, const PathTree& tree) {
  os << "PathTree(" << tree.stats();

  const auto& subtrees = tree.subtrees();
  if (subtrees.size()) {
    os << ", [";
    for (size_t i = 0; i < subtrees.size(); i++) {
      if (i != 0) os << ", ";
      os << *subtrees[i];
    }
    os << "]";
  }
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const std::shared_ptr<PathTree>& tree) {
  if (tree != nullptr) {
    return os << *tree.get();
  }

  return os;
}

bool operator==(const std::shared_ptr<PathTree>& lhs,
                const std::shared_ptr<PathTree>& rhs) {
  if (lhs == NULLPTR && rhs == NULLPTR) {
    return true;
  } else if (lhs != NULLPTR && rhs != NULLPTR) {
    return *lhs == *rhs;
  }

  return false;
}

}  // namespace fs
}  // namespace arrow
