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

#include "arrow/filesystem/path-util.h"

namespace arrow {
namespace fs {
namespace internal {

// TODO Add unit tests for these functions.

std::vector<std::string> SplitAbstractPath(const std::string& s) {
  std::vector<std::string> parts;
  if (s.length() == 0) {
    return parts;
  }

  auto append_part = [&parts, &s](size_t start, size_t end) {
    parts.push_back(s.substr(start, end - start));
  };
  // XXX should strip leading and trailing slashes?

  size_t start = 0;
  while (true) {
    size_t end = s.find_first_of('/', start);
    append_part(start, end);
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return parts;
}

std::pair<std::string, std::string> GetAbstractPathParent(const std::string& s) {
  // XXX should strip trailing slash?

  auto pos = s.find_last_of('/');
  if (pos == std::string::npos) {
    // Empty parent
    return {{}, s};
  }
  return {s.substr(0, pos - 1), s.substr(pos + 1)};
}

Status ValidateAbstractPathParts(const std::vector<std::string>& parts) {
  for (const auto& part : parts) {
    if (part.length() == 0) {
      return Status::Invalid("Empty path component");
    }
  }
  return Status::OK();
}

std::string ConcatAbstractPath(const std::string& base, const std::string& stem) {
  return base.empty() ? stem : base + "/" + stem;
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
