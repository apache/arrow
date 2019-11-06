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

#include <memory>
#include <string>
#include "arrow/filesystem/filesystem.h"

namespace arrow {

namespace fs {

enum class FileSystemType { HDFS, LOCAL, S3, UNKNOWN };

namespace factory {
/// \brief Creates a new FileSystem by path
///
/// \param[in] full_path a URI-based path, ex: hdfs:///some/path?replication=3
/// \param[out] fs FileSystemFactory instance.
/// \return Status
ARROW_EXPORT Status MakeFileSystem(const std::string& full_path,
                                   std::shared_ptr<FileSystem>* fs);
}  // namespace factory
}  // namespace fs
}  // namespace arrow
