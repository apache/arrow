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

// This API is EXPERIMENTAL.

#pragma once

#include <memory>
#include <vector>

#include "arrow/dataset/visibility.h"
#include "arrow/filesystem/type_fwd.h"  // IWYU pragma: export
#include "arrow/type_fwd.h"             // IWYU pragma: export

namespace arrow {
namespace compute {

class ExecContext;

}  // namespace compute

namespace dataset {

class Dataset;
using DatasetVector = std::vector<std::shared_ptr<Dataset>>;

class UnionDataset;
class DatasetFactory;

class Fragment;
using FragmentIterator = Iterator<std::shared_ptr<Fragment>>;
using FragmentVector = std::vector<std::shared_ptr<Fragment>>;

class FileSource;
class FileFormat;
class FileFragment;
class FileSystemDataset;

class CsvFileFormat;

class IpcFileFormat;

class ParquetFileFormat;
class ParquetFileFragment;

class Expression;
using ExpressionVector = std::vector<std::shared_ptr<Expression>>;
class ExpressionEvaluator;

/// forward declared to facilitate scalar(true) as a default for Expression parameters
ARROW_DS_EXPORT
std::shared_ptr<Expression> scalar(bool);

class Partitioning;
class PartitioningFactory;
class PartitioningOrFactory;

struct ScanContext;

class ScanOptions;

class Scanner;

class ScannerBuilder;

class ScanTask;
using ScanTaskVector = std::vector<std::shared_ptr<ScanTask>>;
using ScanTaskIterator = Iterator<std::shared_ptr<ScanTask>>;

class RecordBatchProjector;

class WriteTask;
class WritePlan;

}  // namespace dataset
}  // namespace arrow
