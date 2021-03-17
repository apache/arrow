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
#include <vector>

#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace engine {

class ExecPlan;
class QueryContext;
class QueryNode;

class ARROW_EXPORT QueryPlan {
 public:
  using NodeVector = std::vector<QueryNode*>;

  virtual ~QueryPlan() = default;

  /// Make an empty query plan
  static Result<std::shared_ptr<QueryPlan>> Make(QueryContext*);

  /// The initial inputs
  const NodeVector& sources() const;

  /// The final outputs
  const NodeVector& sinks() const;

  QueryContext* context();
  const QueryContext* context() const;

  /// Make a concrete execution plan from this query plan
  Result<std::unique_ptr<ExecPlan>> MakeExecPlan(compute::ExecContext*);

 protected:
  QueryPlan() = default;
};

class ARROW_EXPORT QueryNode {
 public:
  using NodeVector = std::vector<QueryNode*>;

  virtual ~QueryNode();

  virtual const char* kind_name() = 0;

  /// This node's predecessors in the query plan
  const NodeVector& inputs() const { return inputs_; }

  /// This node's successors in the query plan
  const NodeVector& outputs() const { return outputs_; }

  int num_inputs() const { return static_cast<int>(inputs_.size()); }

  int num_outputs() const { return static_cast<int>(outputs_.size()); }

  /// This node's query plan
  QueryPlan* plan() { return plan_; }

  /// \brief An optional label, for display and debugging
  ///
  /// There is no guarantee that this value is non-empty or unique.
  const std::string& label() const { return label_; }

 protected:
  friend class QueryPlan;

  QueryNode() = default;

  std::string label_;
  QueryPlan* plan_;
  NodeVector inputs_;
  NodeVector outputs_;
};

}  // namespace engine
}  // namespace arrow
