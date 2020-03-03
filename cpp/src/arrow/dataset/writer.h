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
#include <utility>
#include <vector>

#include "arrow/dataset/file_base.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/optional.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace dataset {

// XXX does this merit its own header? Maybe this should just be folded into file_base.h

/// \brief Write a fragment to a single OutputStream.
class ARROW_DS_EXPORT WriteTask {
 public:
  virtual Result<std::shared_ptr<FileFragment>> Execute() = 0;

  virtual ~WriteTask() = default;

  const FileSource& destination() const;
  const std::shared_ptr<FileFormat>& format() const { return format_; }

 protected:
  WriteTask(FileSource destination, std::shared_ptr<FileFormat> format)
      : destination_(std::move(destination)), format_(std::move(format)) {}

  Status CreateDestinationParentDir() const;

  FileSource destination_;
  std::shared_ptr<FileFormat> format_;
};

/// \brief A declarative plan for writing fragments to a partitioned directory structure.
class ARROW_DS_EXPORT WritePlan {
 public:
  /// The partitioning with which paths were generated
  std::shared_ptr<Partitioning> partitioning;

  /// The schema of the Dataset which will be written
  std::shared_ptr<Schema> schema;

  /// The format into which fragments will be written
  std::shared_ptr<FileFormat> format;

  /// The FileSystem and base directory for partitioned writing
  std::shared_ptr<fs::FileSystem> filesystem;
  std::string partition_base_dir;

  using FragmentOrPartitionExpression =
      util::variant<std::shared_ptr<Expression>, std::shared_ptr<Fragment>>;

  /// If fragment_or_partition_expressions[i] is a Fragment, that Fragment will be
  /// written to paths[i]. If it is an Expression, a directory representing that partition
  /// expression will be created at paths[i] instead.
  std::vector<FragmentOrPartitionExpression> fragment_or_partition_expressions;
  std::vector<std::string> paths;

  /// An optional directory into which Fragments with non representable partition
  /// expressions will be written.
  util::optional<std::string> unpartitioned_base_dir;
};

}  // namespace dataset
}  // namespace arrow
