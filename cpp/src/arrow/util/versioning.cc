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

#include "arrow/util/versioning.h"

#include <cstdint>
#include <memory>
#include <string>
#include <sstream>
#include <utility>
#include <vector>
#include <cctype>

#include "arrow/type_fwd.h"
#include "arrow/result.h"

namespace arrow {
namespace internal {

// ? optionally support `v` prefix?

enum VersionPart {
  MAJOR,
  MINOR,
  PATCH,
  UNKNOWN,
  PRE_RELEASE,
  BUILD_INFO
};


Result<SemVer> SemVer::Parse(const std::string& version_string) {
  std::string major = "0", minor = "0", patch = "0";
  std::string unknown, pre_release, build_info;

  auto part = VersionPart::MAJOR;
  for (char const &c: version_string) {
    switch (part) {
      case VersionPart::MAJOR:
        if (std::isdigit(c)) {
          major += c;
        } else if (c == '.') {
          part = VersionPart::MINOR;
        } else if (c == '-') {
          part = VersionPart::PRE_RELEASE;
        } else if (c == '+') {
          part = VersionPart::BUILD_INFO;
        } else if (std::isalpha(c)) {
          unknown += c;
          part = VersionPart::UNKNOWN;
        } else {
          return Status::Invalid("");
        }
        break;
      case VersionPart::MINOR:
        if (std::isdigit(c)) {
          minor += c;
        } else if (c == '.') {
          part = VersionPart::PATCH;
        } else if (c == '-') {
          part = VersionPart::PRE_RELEASE;
        } else if (c == '+') {
          part = VersionPart::BUILD_INFO;
        } else if (std::isalpha(c)) {
          unknown += c;
          part = VersionPart::UNKNOWN;
        } else {
          return Status::Invalid("");
        }
        break;
      case VersionPart::PATCH:
        if (std::isdigit(c)) {
          patch += c;
        } else if (c == '-') {
          part = VersionPart::PRE_RELEASE;
        } else if (c == '+') {
          part = VersionPart::BUILD_INFO;
        } else if (std::isalpha(c)) {
          unknown += c;
          part = VersionPart::UNKNOWN;
        } else {
          return Status::Invalid("");
        }
        break;
      case VersionPart::UNKNOWN:
        if (std::isalnum(c)) {
          unknown += c;
        } else if (c == '-') {
          part = VersionPart::PRE_RELEASE;
        } else if (c == '+') {
          part = VersionPart::BUILD_INFO;
        } else {
          return Status::Invalid("");
        }
        break;
      case VersionPart::PRE_RELEASE:
        if (c == '.' || std::isalnum(c)) {
          pre_release += c;
        } else if (c == '+') {
          part = VersionPart::BUILD_INFO;
        } else {
          return Status::Invalid("");
        }
        break;
      case VersionPart::BUILD_INFO:
        if (std::isalnum(c)) {
          build_info += c;
        } else {
          return Status::Invalid("");
        }
        break;
    }
  }
  try {
    return SemVer(std::stoi(major), std::stoi(minor), std::stoi(patch), unknown,
                  pre_release, build_info);
  } catch (std::invalid_argument& e) {
    return Status::Invalid("Unable to convert version");
  } catch (std::out_of_range& e) {
    return Status::Invalid("Unable to convert version");
  }
}

std::string SemVer::ToString() const {
  std::stringstream ss;
  ss << major << '.' << minor << '.' << patch << unknown;
  if (pre_release.size()) {
    ss << '-' << pre_release;
  }
  if (build_info.size()) {
    ss << '+' << build_info;
  }
  return ss.str();
}

static int compare(const SemVer &lhs, const SemVer &rhs) {
  // -1 if lhs < rhs, zero if lhs == rhs, +1 if lhs > rhs
  if (lhs.major < rhs.major) {
    return -1;
  } else if (lhs.major > rhs.major) {
    return 1;
  }

  if (lhs.minor < rhs.minor) {
    return -1;
  } else if (lhs.minor > rhs.minor) {
    return 1;
  }

  if (lhs.patch < rhs.patch) {
    return -1;
  } else if (lhs.patch > rhs.patch) {
    return 1;
  }

  return 0;
}

bool SemVer::operator==(const SemVer &other) const {
  return compare(*this, other) == 0;
}

bool SemVer::operator!=(const SemVer &other) const {
  return compare(*this, other) != 0;
}

bool SemVer::operator<(const SemVer &other) const {
  return compare(*this, other) < 0;
}

bool SemVer::operator>(const SemVer &other) const {
  return compare(*this, other) > 0;
}

bool SemVer::operator<=(const SemVer &other) const {
  return compare(other, *this) > 0;
}

bool SemVer::operator>=(const SemVer &other) const {
  return compare(other, *this) < 0;
}

}  // namespace internal
}  // namespace arrow



