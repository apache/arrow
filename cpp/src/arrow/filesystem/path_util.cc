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

#include <algorithm>
#include <regex>

#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::StartsWith;

namespace fs {
namespace internal {

// XXX How does this encode Windows UNC paths?

std::vector<std::string> SplitAbstractPath(const std::string& path, char sep) {
  std::vector<std::string> parts;
  auto v = std::string_view(path);
  // Strip trailing separator
  if (v.length() > 0 && v.back() == sep) {
    v = v.substr(0, v.length() - 1);
  }
  // Strip leading separator
  if (v.length() > 0 && v.front() == sep) {
    v = v.substr(1);
  }
  if (v.length() == 0) {
    return parts;
  }

  auto append_part = [&parts, &v](size_t start, size_t end) {
    parts.push_back(std::string(v.substr(start, end - start)));
  };

  size_t start = 0;
  while (true) {
    size_t end = v.find_first_of(sep, start);
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

  auto pos = s.find_last_of(kSep);
  if (pos == std::string::npos) {
    // Empty parent
    return {{}, s};
  }
  return {s.substr(0, pos), s.substr(pos + 1)};
}

std::string GetAbstractPathExtension(const std::string& s) {
  std::string_view basename(s);
  auto offset = basename.find_last_of(kSep);
  if (offset != std::string::npos) {
    basename = basename.substr(offset);
  }
  auto dot = basename.find_last_of('.');
  if (dot == std::string_view::npos) {
    // Empty extension
    return "";
  }
  return std::string(basename.substr(dot + 1));
}

Status ValidateAbstractPathParts(const std::vector<std::string>& parts) {
  for (const auto& part : parts) {
    if (part.length() == 0) {
      return Status::Invalid("Empty path component");
    }
    if (part.find_first_of(kSep) != std::string::npos) {
      return Status::Invalid("Separator in component '", part, "'");
    }
  }
  return Status::OK();
}

std::string ConcatAbstractPath(const std::string& base, const std::string& stem) {
  DCHECK(!stem.empty());
  if (base.empty()) {
    return stem;
  }
  return EnsureTrailingSlash(base) + std::string(RemoveLeadingSlash(stem));
}

std::string EnsureTrailingSlash(std::string_view v) {
  if (v.length() > 0 && v.back() != kSep) {
    // XXX How about "C:" on Windows?  We probably don't want to turn it into "C:/"...
    // Unless the local filesystem always uses absolute paths
    return std::string(v) + kSep;
  } else {
    return std::string(v);
  }
}

std::string EnsureLeadingSlash(std::string_view v) {
  if (v.length() == 0 || v.front() != kSep) {
    // XXX How about "C:" on Windows?  We probably don't want to turn it into "/C:"...
    return kSep + std::string(v);
  } else {
    return std::string(v);
  }
}
std::string_view RemoveTrailingSlash(std::string_view key) {
  while (!key.empty() && key.back() == kSep) {
    key.remove_suffix(1);
  }
  return key;
}

std::string_view RemoveLeadingSlash(std::string_view key) {
  while (!key.empty() && key.front() == kSep) {
    key.remove_prefix(1);
  }
  return key;
}

Status AssertNoTrailingSlash(std::string_view key) {
  if (key.back() == '/') {
    return NotAFile(key);
  }
  return Status::OK();
}

bool HasLeadingSlash(std::string_view key) {
  if (key.front() != '/') {
    return false;
  }
  return true;
}

Result<std::string> MakeAbstractPathRelative(const std::string& base,
                                             const std::string& path) {
  if (base.empty() || base.front() != kSep) {
    return Status::Invalid("MakeAbstractPathRelative called with non-absolute base '",
                           base, "'");
  }
  auto b = EnsureLeadingSlash(RemoveTrailingSlash(base));
  auto p = std::string_view(path);
  if (p.substr(0, b.size()) != std::string_view(b)) {
    return Status::Invalid("Path '", path, "' is not relative to '", base, "'");
  }
  p = p.substr(b.size());
  if (!p.empty() && p.front() != kSep && b.back() != kSep) {
    return Status::Invalid("Path '", path, "' is not relative to '", base, "'");
  }
  return std::string(RemoveLeadingSlash(p));
}

bool IsAncestorOf(std::string_view ancestor, std::string_view descendant) {
  ancestor = RemoveTrailingSlash(ancestor);
  if (ancestor == "") {
    // everything is a descendant of the root directory
    return true;
  }

  descendant = RemoveTrailingSlash(descendant);
  if (!StartsWith(descendant, ancestor)) {
    // an ancestor path is a prefix of descendant paths
    return false;
  }

  descendant.remove_prefix(ancestor.size());

  if (descendant.empty()) {
    // "/hello" is an ancestor of "/hello"
    return true;
  }

  // "/hello/w" is not an ancestor of "/hello/world"
  return StartsWith(descendant, std::string{kSep});
}

std::optional<std::string_view> RemoveAncestor(std::string_view ancestor,
                                               std::string_view descendant) {
  if (!IsAncestorOf(ancestor, descendant)) {
    return std::nullopt;
  }

  auto relative_to_ancestor = descendant.substr(ancestor.size());
  return RemoveLeadingSlash(relative_to_ancestor);
}

std::vector<std::string> AncestorsFromBasePath(std::string_view base_path,
                                               std::string_view descendant) {
  std::vector<std::string> ancestry;
  if (auto relative = RemoveAncestor(base_path, descendant)) {
    auto relative_segments = fs::internal::SplitAbstractPath(std::string(*relative));

    // the last segment indicates descendant
    relative_segments.pop_back();

    if (relative_segments.empty()) {
      // no missing parent
      return {};
    }

    for (auto&& relative_segment : relative_segments) {
      ancestry.push_back(JoinAbstractPath(
          std::vector<std::string>{std::string(base_path), std::move(relative_segment)}));
      base_path = ancestry.back();
    }
  }
  return ancestry;
}

std::vector<std::string> MinimalCreateDirSet(std::vector<std::string> dirs) {
  std::sort(dirs.begin(), dirs.end());

  for (auto ancestor = dirs.begin(); ancestor != dirs.end(); ++ancestor) {
    auto descendant = ancestor;
    auto descendants_end = descendant + 1;

    while (descendants_end != dirs.end() && IsAncestorOf(*descendant, *descendants_end)) {
      ++descendant;
      ++descendants_end;
    }

    ancestor = dirs.erase(ancestor, descendants_end - 1);
  }

  // the root directory need not be created
  if (dirs.size() == 1 && IsAncestorOf(dirs[0], "")) {
    return {};
  }

  return dirs;
}

std::string ToBackslashes(std::string_view v) {
  std::string s(v);
  for (auto& c : s) {
    if (c == '/') {
      c = '\\';
    }
  }
  return s;
}

std::string ToSlashes(std::string_view v) {
  std::string s(v);
#ifdef _WIN32
  for (auto& c : s) {
    if (c == '\\') {
      c = '/';
    }
  }
#endif
  return s;
}

bool IsEmptyPath(std::string_view v) {
  for (const auto c : v) {
    if (c != '/') {
      return false;
    }
  }
  return true;
}

bool IsLikelyUri(std::string_view v) {
  if (v.empty() || v[0] == '/') {
    return false;
  }
  const auto pos = v.find_first_of(':');
  if (pos == v.npos) {
    return false;
  }
  if (pos < 2) {
    // One-letter URI schemes don't officially exist, perhaps a Windows drive letter?
    return false;
  }
  if (pos > 36) {
    // The largest IANA-registered URI scheme is "microsoft.windows.camera.multipicker"
    // with 36 characters.
    return false;
  }
  return ::arrow::internal::IsValidUriScheme(v.substr(0, pos));
}

struct Globber::Impl {
  std::regex pattern_;

  explicit Impl(const std::string& p) : pattern_(std::regex(PatternToRegex(p))) {}

  static std::string PatternToRegex(const std::string& p) {
    std::string special_chars = "()[]{}+-|^$\\.&~# \t\n\r\v\f";
    std::string transformed;
    auto it = p.begin();
    while (it != p.end()) {
      if (*it == '\\') {
        transformed += '\\';
        if (++it != p.end()) {
          transformed += *it;
        }
      } else if (*it == '*') {
        transformed += "[^/]*";
      } else if (*it == '?') {
        transformed += "[^/]";
      } else if (special_chars.find(*it) != std::string::npos) {
        transformed += "\\";
        transformed += *it;
      } else {
        transformed += *it;
      }
      it++;
    }
    return transformed;
  }
};

Globber::Globber(std::string pattern) : impl_(new Impl(pattern)) {}

Globber::~Globber() {}

bool Globber::Matches(const std::string& path) {
  return regex_match(path, impl_->pattern_);
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
