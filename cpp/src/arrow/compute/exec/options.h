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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/util/optional.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class ARROW_EXPORT ExecNodeOptions {
 public:
  virtual ~ExecNodeOptions() = default;
};

/// \brief Adapt an AsyncGenerator<ExecBatch> as a source node
///
/// plan->exec_context()->executor() will be used to parallelize pushing to
/// outputs, if provided.
class ARROW_EXPORT SourceNodeOptions : public ExecNodeOptions {
 public:
  SourceNodeOptions(std::shared_ptr<Schema> output_schema,
                    std::function<Future<util::optional<ExecBatch>>()> generator)
      : output_schema(std::move(output_schema)), generator(std::move(generator)) {}

  std::shared_ptr<Schema> output_schema;
  std::function<Future<util::optional<ExecBatch>>()> generator;
};

/// \brief Make a node which excludes some rows from batches passed through it
///
/// filter_expression will be evaluated against each batch which is pushed to
/// this node. Any rows for which filter_expression does not evaluate to `true` will be
/// excluded in the batch emitted by this node.
class ARROW_EXPORT FilterNodeOptions : public ExecNodeOptions {
 public:
  explicit FilterNodeOptions(Expression filter_expression)
      : filter_expression(std::move(filter_expression)) {}

  Expression filter_expression;
};

/// \brief Make a node which executes expressions on input batches, producing new batches.
///
/// Each expression will be evaluated against each batch which is pushed to
/// this node to produce a corresponding output column.
///
/// If names are not provided, the string representations of exprs will be used.
class ARROW_EXPORT ProjectNodeOptions : public ExecNodeOptions {
 public:
  explicit ProjectNodeOptions(std::vector<Expression> expressions,
                              std::vector<std::string> names = {})
      : expressions(std::move(expressions)), names(std::move(names)) {}

  std::vector<Expression> expressions;
  std::vector<std::string> names;
};

/// \brief Make a node which aggregates input batches, optionally grouped by keys.
class ARROW_EXPORT AggregateNodeOptions : public ExecNodeOptions {
 public:
  AggregateNodeOptions(std::vector<internal::Aggregate> aggregates,
                       std::vector<FieldRef> targets, std::vector<std::string> names,
                       std::vector<FieldRef> keys = {})
      : aggregates(std::move(aggregates)),
        targets(std::move(targets)),
        names(std::move(names)),
        keys(std::move(keys)) {}

  // aggregations which will be applied to the targetted fields
  std::vector<internal::Aggregate> aggregates;
  // fields to which aggregations will be applied
  std::vector<FieldRef> targets;
  // output field names for aggregations
  std::vector<std::string> names;
  // keys by which aggregations will be grouped
  std::vector<FieldRef> keys;
};

/// \brief Add a sink node which forwards to an AsyncGenerator<ExecBatch>
///
/// Emitted batches will not be ordered.
class ARROW_EXPORT SinkNodeOptions : public ExecNodeOptions {
 public:
  explicit SinkNodeOptions(std::function<Future<util::optional<ExecBatch>>()>* generator)
      : generator(generator) {}

  std::function<Future<util::optional<ExecBatch>>()>* generator;
};

/// \brief Make a node which sorts rows passed through it
///
/// All batches pushed to this node will be accumulated, then sorted, by the given
/// fields. Then sorted batches will be forwarded to the generator in sorted order.
class ARROW_EXPORT OrderBySinkNodeOptions : public SinkNodeOptions {
 public:
  explicit OrderBySinkNodeOptions(
      SortOptions sort_options,
      std::function<Future<util::optional<ExecBatch>>()>* generator)
      : SinkNodeOptions(generator), sort_options(std::move(sort_options)) {}

  SortOptions sort_options;
};

}  // namespace compute
}  // namespace arrow
