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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "arrow/filesystem/path_util.h"

namespace arrow {
namespace fs {

static int PathDepth(const std::string& path) {
  return std::count(path.begin(), path.end(), internal::kSep);
}

using PathTreeByPathMap = std::unordered_map<std::string, std::shared_ptr<PathTree>>;

template <FileType Type>
bool IsType(const std ::shared_ptr<PathTree>& tree) {
  return tree->stats().type() == Type;
}

std::shared_ptr<PathTree> FindAncestor(PathTreeByPathMap* directories, std::string path) {
  while (path != "") {
    auto parent = internal::GetAbstractPathParent(path).first;
    auto found = directories->find(parent);
    if (found != directories->end()) {
      return found->second;
    }

    path = parent;
  }

  return nullptr;
}

static void LinkToParentOrInsertNewRoot(FileStats stats, PathTreeByPathMap* directories,
                                        PathTreeForest* forest) {
  auto node = std::make_shared<PathTree>(stats);
  if (stats.path() == "") {
    forest->push_back(node);
    return;
  }

  auto ancestor = FindAncestor(directories, stats.path());
  if (ancestor) {
    ancestor->AddChild(node);
  } else {
    forest->push_back(node);
  }

  if (IsType<FileType::Directory>(node)) {
    directories->insert({stats.path(), node});
  }
}

using DirectoryByDepthMap = std::unordered_map<int, std::vector<FileStats>>;

std::vector<int> OrderedDepths(const DirectoryByDepthMap& directories_by_depth) {
  std::vector<int> depths;
  for (auto k_v : directories_by_depth) {
    depths.push_back(k_v.first);
  }

  // In practice, this is going to be O(lg(n)lg(lg(n))), i.e. constant.
  std::sort(depths.begin(), depths.end());
  return depths;
}

Status PathTree::Make(std::vector<FileStats> stats, PathTreeForest* out) {
  PathTreeByPathMap directories;
  PathTreeForest forest;

  // Partition the stats vector into (directories, others)
  auto is_directory = [](const FileStats& stats) { return stats.IsDirectory(); };
  std::stable_partition(stats.begin(), stats.end(), is_directory);
  auto mid = std::partition_point(stats.begin(), stats.end(), is_directory);

  // First, partition directories by path depth.
  DirectoryByDepthMap directories_by_depth;
  std::for_each(stats.begin(), mid, [&directories_by_depth](FileStats s) {
    directories_by_depth[PathDepth(s.path())].push_back(s);
  });

  // Insert directories by ascending depth, ensuring that children directories
  // are always inserted after ancestors.
  for (int d : OrderedDepths(directories_by_depth)) {
    auto dir = directories_by_depth.at(d);
    std::for_each(dir.begin(), dir.end(), [&directories, &forest](FileStats s) {
      LinkToParentOrInsertNewRoot(s, &directories, &forest);
    });
  }

  // Second, ingest files. By the same logic, directories are added before
  // files, hence the lookup for ancestors is valid.
  std::for_each(mid, stats.end(), [&directories, &forest](FileStats s) {
    LinkToParentOrInsertNewRoot(s, &directories, &forest);
  });

  *out = std::move(forest);
  return Status::OK();
}

Status PathTree::Make(std::vector<FileStats> stats, std::shared_ptr<PathTree>* out) {
  PathTreeForest forest;
  RETURN_NOT_OK(Make(stats, &forest));

  auto size = forest.size();
  if (size > 1) {
    return Status::Invalid("Requested PathTree has ", size, " roots, but expected 1.");
  } else if (size == 1) {
    *out = forest[0];
  }

  return Status::OK();
}

}  // namespace fs
}  // namespace arrow
