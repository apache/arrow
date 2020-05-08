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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

/// \brief Semantic versioning
struct ARROW_EXPORT SemVer {
 public:
  SemVer(){};
  SemVer(int major_version, int minor_version, int patch_version,
         const std::string& unknown = "", const std::string& pre_release = "",
         const std::string& build_info = "")
      : unknown(unknown), pre_release(pre_release), build_info(build_info) {
    // using major minor in the initializer list expands unwanted macros on gcc 4.8
    // https://bugzilla.redhat.com/show_bug.cgi?id=130601
    major = major_version;
    minor = minor_version;
    patch = patch_version;
  };

  ~SemVer(){};

  bool operator==(const SemVer& other) const;

  bool operator!=(const SemVer& other) const;

  bool operator<(const SemVer& other) const;

  bool operator>(const SemVer& other) const;

  bool operator<=(const SemVer& other) const;

  bool operator>=(const SemVer& other) const;

  /// Get the string representation of this version.
  std::string ToString() const;

  /// Factory function to parse a version string.
  static Result<SemVer> Parse(const std::string& version_string);

  unsigned int major, minor, patch;
  std::string unknown, pre_release, build_info;
};

}  // namespace internal
}  // namespace arrow
