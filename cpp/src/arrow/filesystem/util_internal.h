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
#include <string_view>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/util/uri.h"
#include "arrow/util/visibility.h"

namespace arrow {
using internal::Uri;
namespace fs {
namespace internal {

ARROW_EXPORT
TimePoint CurrentTimePoint();

ARROW_EXPORT
Status CopyStream(const std::shared_ptr<io::InputStream>& src,
                  const std::shared_ptr<io::OutputStream>& dest, int64_t chunk_size,
                  const io::IOContext& io_context);

ARROW_EXPORT
Status PathNotFound(std::string_view path);

ARROW_EXPORT
Status NotADir(std::string_view path);

ARROW_EXPORT
Status NotAFile(std::string_view path);

ARROW_EXPORT
Status InvalidDeleteDirContents(std::string_view path);

/// \brief Parse the string as a URI
/// \param uri_string the string to parse
///
/// This is the same as Uri::Parse except it tolerates Windows
/// file URIs that contain backslash instead of /
Result<Uri> ParseFileSystemUri(const std::string& uri_string);

/// \brief check if the string is a local absolute path
ARROW_EXPORT
bool DetectAbsolutePath(const std::string& s);

/// \brief describes how to handle the authority (host) component of the URI
enum class AuthorityHandlingBehavior {
  // Return an invalid status if the authority is non-empty
  kDisallow = 0,
  // Prepend the authority to the path (e.g. authority/some/path)
  kPrepend = 1,
  // Convert to a Windows style network path (e.g. //authority/some/path)
  kWindows = 2,
  // Ignore the authority and just use the path
  kIgnore = 3
};

/// \brief check to see if uri_string matches one of the supported schemes and return the
/// path component
/// \param uri_string a uri or local path to test and convert
/// \param supported_schemes the set of URI schemes that should be accepted
/// \param accept_local_paths if true, allow an absolute path
/// \return the path portion of the URI
Result<std::string> PathFromUriHelper(const std::string& uri_string,
                                      std::vector<std::string> supported_schemes,
                                      bool accept_local_paths,
                                      AuthorityHandlingBehavior authority_handling);

/// \brief Return files matching the glob pattern on the filesystem
///
/// Globbing starts from the root of the filesystem.
ARROW_EXPORT
Result<FileInfoVector> GlobFiles(const std::shared_ptr<FileSystem>& filesystem,
                                 const std::string& glob);

extern FileSystemGlobalOptions global_options;

}  // namespace internal
}  // namespace fs
}  // namespace arrow
