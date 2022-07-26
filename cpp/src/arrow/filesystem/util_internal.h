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

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace fs {
namespace internal {

ARROW_EXPORT
TimePoint CurrentTimePoint();

ARROW_EXPORT
Status CopyStream(const std::shared_ptr<io::InputStream>& src,
                  const std::shared_ptr<io::OutputStream>& dest, int64_t chunk_size,
                  const io::IOContext& io_context);

ARROW_EXPORT
Status PathNotFound(util::string_view path);

ARROW_EXPORT
Status NotADir(util::string_view path);

ARROW_EXPORT
Status NotAFile(util::string_view path);

ARROW_EXPORT
Status InvalidDeleteDirContents(util::string_view path);

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
