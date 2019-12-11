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
#include <vector>

#include "arrow/dataset/visibility.h"
#include "arrow/type_fwd.h"  // IWYU pragma: export

namespace arrow {

namespace compute {

class FunctionContext;

}  // namespace compute

namespace fs {

class FileSystem;
using FileSystemPtr = std::shared_ptr<FileSystem>;

struct FileStats;
using FileStatsVector = std::vector<FileStats>;

}  // namespace fs

namespace dataset {

class Dataset;
using DatasetPtr = std::shared_ptr<Dataset>;

class DataFragment;
using DataFragmentPtr = std::shared_ptr<DataFragment>;
using DataFragmentIterator = Iterator<DataFragmentPtr>;
using DataFragmentVector = std::vector<DataFragmentPtr>;

class DataSource;
using DataSourcePtr = std::shared_ptr<DataSource>;
using DataSourceVector = std::vector<DataSourcePtr>;

struct DiscoveryOptions;
class DataSourceDiscovery;
using DataSourceDiscoveryPtr = std::shared_ptr<DataSourceDiscovery>;

class FileFormat;
using FileFormatPtr = std::shared_ptr<FileFormat>;

class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;
using ExpressionVector = std::vector<ExpressionPtr>;

class ComparisonExpression;
class InExpression;
class IsValidExpression;
class AndExpression;
class OrExpression;
class NotExpression;
class CastExpression;
class ScalarExpression;
class FieldReferenceExpression;
class ExpressionEvaluator;

class PartitionScheme;
using PartitionSchemePtr = std::shared_ptr<PartitionScheme>;

class PartitionSchemeDiscovery;
using PartitionSchemeDiscoveryPtr = std::shared_ptr<PartitionSchemeDiscovery>;

struct ScanContext;
using ScanContextPtr = std::shared_ptr<ScanContext>;

class ScanOptions;
using ScanOptionsPtr = std::shared_ptr<ScanOptions>;

class Scanner;
using ScannerPtr = std::shared_ptr<Scanner>;

class ScannerBuilder;
using ScannerBuilderPtr = std::shared_ptr<ScannerBuilder>;

class ScanTask;
using ScanTaskPtr = std::shared_ptr<ScanTask>;
using ScanTaskVector = std::vector<ScanTaskPtr>;
using ScanTaskIterator = Iterator<ScanTaskPtr>;

class RecordBatchProjector;

class DatasetWriter;
class WriteContext;
class WriteOptions;

}  // namespace dataset
}  // namespace arrow
