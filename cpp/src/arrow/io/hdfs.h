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
#include <unordered_map>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/hdfs.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace io {

using fs::FileStats;
using fs::FileSystem;
using fs::FileType;
using fs::Selector;
using fs::TimePoint;

using fs::ObjectType;

using HdfsPathInfo = fs::HadoopPathInfo;
using HdfsDriver = fs::HadoopDriver;
using HdfsConnectionConfig = fs::HadoopOptions;

class ARROW_EXPORT HadoopFileSystem : public fs::HadoopFileSystem {
 public:
  ~HadoopFileSystem() override;

  /// Connect to an HDFS cluster given a configuration
  ///
  /// \param config (in): configuration for connecting
  /// \param fs (out): the created client
  /// \returns Status
  static Status Connect(const HdfsConnectionConfig* options,
                        std::shared_ptr<HadoopFileSystem>* fs) {
    // FIXME(bkietz) delete this soon
    std::shared_ptr<fs::HadoopFileSystem> fs_fs;
    RETURN_NOT_OK(fs::HadoopFileSystem::Connect(*options, &fs_fs));
    *fs = std::static_pointer_cast<HadoopFileSystem>(fs_fs);
    return Status::OK();
  }
};

}  // namespace io
}  // namespace arrow
