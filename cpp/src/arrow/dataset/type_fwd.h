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

struct FileStats;
using FileStatsVector = std::vector<FileStats>;

}  // namespace fs

namespace dataset {

class Dataset;

class DataFragment;
using DataFragmentIterator = Iterator<std::shared_ptr<DataFragment>>;
using DataFragmentVector = std::vector<std::shared_ptr<DataFragment>>;

class DataSource;
using DataSourceVector = std::vector<std::shared_ptr<DataSource>>;

struct DiscoveryOptions;
class DataSourceDiscovery;

class FileFormat;

class Expression;
using ExpressionVector = std::vector<std::shared_ptr<Expression>>;

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

class PartitionSchemeDiscovery;

class PartitionSchemeOrDiscovery;

struct ScanContext;

class ScanOptions;

class Scanner;

class ScannerBuilder;

class ScanTask;
using ScanTaskVector = std::vector<std::shared_ptr<ScanTask>>;
using ScanTaskIterator = Iterator<std::shared_ptr<ScanTask>>;

class RecordBatchProjector;

class DatasetWriter;
class WriteContext;
class WriteOptions;

}  // namespace dataset
}  // namespace arrow
