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

#include "arrow/util/uri.h"

#include <algorithm>
#include <codecvt>
#include <cstring>
#include <iostream>
#include <locale>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/util/value_parsing.h"
#include "arrow/vendored/uriparser/Uri.h"

namespace arrow {
namespace internal {

namespace {

std::wstring_view TextRangeToView(const UriTextRangeStructW& range) {
  if (range.first == nullptr) {
    return L"";
  } else {
    return {range.first, static_cast<size_t>(range.afterLast - range.first)};
  }
}

std::string TextRangeToString(const UriTextRangeStructW& range) {
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  return converter.to_bytes(std::wstring(TextRangeToView(range)));
}

// There can be a difference between an absent field and an empty field.
// For example, in "unix:/tmp/foo", the host is absent, while in
// "unix:///tmp/foo", the host is empty but present.
// This function helps distinguish.
bool IsTextRangeSet(const UriTextRangeStructW& range) { return range.first != nullptr; }

#ifdef _WIN32
bool IsDriveSpec(const std::string_view s) {
  return (s.length() >= 2 && s[1] == ':' &&
          ((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z')));
}
#endif

}  // namespace

std::string UriEscape(const std::string& s) {
  if (s.empty()) {
    // Avoid passing null pointer to uriEscapeExA
    return s;
  }
  std::string escaped;
  escaped.resize(3 * s.length());

  auto end = uriEscapeExA(s.data(), s.data() + s.length(), &escaped[0],
                          /*spaceToPlus=*/URI_FALSE, /*normalizeBreaks=*/URI_FALSE);
  escaped.resize(end - &escaped[0]);
  return escaped;
}

std::string UriUnescape(const std::string_view s) {
  std::string result(s);
  if (!result.empty()) {
    auto end = uriUnescapeInPlaceA(&result[0]);
    result.resize(end - &result[0]);
  }
  return result;
}

std::string UriEncodeHost(const std::string& host) {
  // Fairly naive check: if it contains a ':', it's IPv6 and needs
  // brackets, else it's OK
  if (host.find(":") != std::string::npos) {
    std::string result = "[";
    result += host;
    result += ']';
    return result;
  } else {
    return host;
  }
}

bool IsValidUriScheme(const std::string_view s) {
  auto is_alpha = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); };
  auto is_scheme_char = [&](char c) {
    return is_alpha(c) || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.';
  };

  if (s.empty()) {
    return false;
  }
  if (!is_alpha(s[0])) {
    return false;
  }
  return std::all_of(s.begin() + 1, s.end(), is_scheme_char);
}

struct Uri::Impl {
  Impl() : string_rep_(""), port_(-1) { memset(&uri_, 0, sizeof(uri_)); }

  ~Impl() { uriFreeUriMembersW(&uri_); }

  void Reset() {
    uriFreeUriMembersW(&uri_);
    memset(&uri_, 0, sizeof(uri_));
    data_.clear();
    string_rep_.clear();
    path_segments_.clear();
    port_ = -1;
  }

  const std::wstring& KeepString(const std::wstring& s) {
    data_.push_back(s);
    return data_.back();
  }

  UriUriW uri_;
  // Keep alive strings that uriparser stores pointers to
  std::vector<std::wstring> data_;
  std::string string_rep_;
  int32_t port_;
  std::vector<std::wstring_view> path_segments_;
  bool is_file_uri_;
  bool is_absolute_path_;
};

Uri::Uri() : impl_(new Impl) {}

Uri::~Uri() {}

Uri::Uri(Uri&& u) : impl_(std::move(u.impl_)) {}

Uri& Uri::operator=(Uri&& u) {
  impl_ = std::move(u.impl_);
  return *this;
}

std::string Uri::scheme() const { return TextRangeToString(impl_->uri_.scheme); }

bool Uri::is_file_scheme() const { return impl_->is_file_uri_; }

std::string Uri::host() const { return TextRangeToString(impl_->uri_.hostText); }

bool Uri::has_host() const { return IsTextRangeSet(impl_->uri_.hostText); }

std::string Uri::port_text() const { return TextRangeToString(impl_->uri_.portText); }

int32_t Uri::port() const { return impl_->port_; }

std::string Uri::username() const {
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  auto userpass = converter.to_bytes(std::wstring(TextRangeToView(impl_->uri_.userInfo)));
  auto sep_pos = userpass.find_first_of(':');
  if (sep_pos == std::string_view::npos) {
    return UriUnescape(userpass);
  } else {
    return UriUnescape(userpass.substr(0, sep_pos));
  }
}

std::string Uri::password() const {
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  auto userpass = converter.to_bytes(std::wstring(TextRangeToView(impl_->uri_.userInfo)));
  auto sep_pos = userpass.find_first_of(':');

  if (sep_pos == std::string_view::npos) {
    return std::string();
  } else {
    return UriUnescape(userpass.substr(sep_pos + 1));
  }
}

std::string Uri::path() const {
  const auto& segments = impl_->path_segments_;
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;

  bool must_prepend_slash = impl_->is_absolute_path_;
#ifdef _WIN32
  // On Windows, "file:///C:/foo" should have path "C:/foo", not "/C:/foo",
  // despite it being absolute.
  // (see https://tools.ietf.org/html/rfc8089#page-13)
  if (impl_->is_absolute_path_ && impl_->is_file_uri_ && segments.size() > 0 &&
      IsDriveSpec(segments[0])) {
    must_prepend_slash = false;
  }
#endif

  std::wstringstream ss;
  if (must_prepend_slash) {
    ss << "/";
  }
  bool first = true;
  for (const auto& seg : segments) {
    if (!first) {
      ss << L"/";
    }
    first = false;
    ss << seg;
  }
  return converter.to_bytes(std::move(ss).str());
}

std::string Uri::query_string() const { return TextRangeToString(impl_->uri_.query); }

Result<std::vector<std::pair<std::string, std::string>>> Uri::query_items() const {
  const auto& query = impl_->uri_.query;
  UriQueryListW* query_list;
  int item_count;
  std::vector<std::pair<std::string, std::string>> items;
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;

  if (query.first == nullptr) {
    return items;
  }
  if (uriDissectQueryMallocW(&query_list, &item_count, query.first, query.afterLast) !=
      URI_SUCCESS) {
    return Status::Invalid("Cannot parse query string: '", query_string(), "'");
  }
  std::unique_ptr<UriQueryListW, decltype(&uriFreeQueryListW)> query_guard(
      query_list, uriFreeQueryListW);

  items.reserve(item_count);
  while (query_list != nullptr) {
    if (query_list->value != nullptr) {
      items.emplace_back(converter.to_bytes(query_list->key),
                         converter.to_bytes(query_list->value));
    } else {
      items.emplace_back(converter.to_bytes(query_list->key), "");
    }
    query_list = query_list->next;
  }
  return items;
}

const std::string& Uri::ToString() const { return impl_->string_rep_; }

Status Uri::Parse(const std::string& uri_string) {
  impl_->Reset();

  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  std::wstring wide_uri = converter.from_bytes(uri_string);
  std::wcout << "Wide URI: " << wide_uri << std::endl;

  const auto& s = impl_->KeepString(wide_uri);
  impl_->string_rep_ = converter.to_bytes(s);

  const wchar_t* error_pos;
  if (uriParseSingleUriExW(&impl_->uri_, s.data(), s.data() + s.size(), &error_pos) !=
      URI_SUCCESS) {
    return Status::Invalid("Cannot parse URI: '", uri_string, "' at: '",
                           converter.to_bytes(error_pos), "'");
  }

  const auto scheme = TextRangeToView(impl_->uri_.scheme);
  if (scheme.empty()) {
    return Status::Invalid("URI has empty scheme: '", uri_string, "'");
  }
  impl_->is_file_uri_ = (scheme == L"file");

  // Gather path segments
  auto path_seg = impl_->uri_.pathHead;
  while (path_seg != nullptr) {
    impl_->path_segments_.push_back(TextRangeToView(path_seg->text));
    path_seg = path_seg->next;
  }

  // Decide whether URI path is absolute
  impl_->is_absolute_path_ = false;
  if (impl_->uri_.absolutePath == URI_TRUE) {
    impl_->is_absolute_path_ = true;
  } else if (has_host() && impl_->path_segments_.size() > 0) {
    // When there's a host (even empty), uriparser considers the path relative.
    // Several URI parsers for Python all consider it absolute, though.
    // For example, the path for "file:///tmp/foo" is "/tmp/foo", not "tmp/foo".
    // Similarly, the path for "file://localhost/" is "/".
    // However, the path for "file://localhost" is "".
    impl_->is_absolute_path_ = true;
  }
#ifdef _WIN32
  // There's an exception on Windows: "file:/C:foo/bar" is relative.
  if (impl_->is_file_uri_ && impl_->path_segments_.size() > 0) {
    const auto& first_seg = impl_->path_segments_[0];
    if (IsDriveSpec(first_seg) && (first_seg.length() >= 3 && first_seg[2] != '/')) {
      impl_->is_absolute_path_ = false;
    }
  }
#endif

  if (impl_->is_file_uri_ && !impl_->is_absolute_path_) {
    return Status::Invalid("File URI cannot be relative: '", uri_string, "'");
  }

  // Parse port number
  auto port_text =
      converter.to_bytes(std::wstring(TextRangeToView(impl_->uri_.portText)));
  if (port_text.size()) {
    uint16_t port_num;
    if (!ParseValue<UInt16Type>(port_text.data(), port_text.size(), &port_num)) {
      return Status::Invalid("Invalid port number '", port_text, "' in URI '", uri_string,
                             "'");
    }
    impl_->port_ = port_num;
  }

  return Status::OK();
}

std::string UriFromAbsolutePath(const std::string& path) {
#ifdef _WIN32
  // Path is supposed to start with "X:/..."
  return "file:///" + path;
#else
  // Path is supposed to start with "/..."
  return "file://" + path;
#endif
}

}  // namespace internal
}  // namespace arrow
