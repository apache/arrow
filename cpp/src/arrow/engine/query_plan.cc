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

#include "arrow/engine/query_plan.h"

#include "arrow/engine/exec_plan.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace engine {

namespace {

struct QueryPlanImpl : public QueryPlan {
  explicit QueryPlanImpl(QueryContext* ctx) : context_(ctx) {}

  ~QueryPlanImpl() {}

  QueryContext* context_;
  std::vector<std::unique_ptr<QueryNode>> nodes_;
  NodeVector sources_;
  NodeVector sinks_;
};

QueryPlanImpl* ToDerived(QueryPlan* ptr) { return checked_cast<QueryPlanImpl*>(ptr); }

const QueryPlanImpl* ToDerived(const QueryPlan* ptr) {
  return checked_cast<const QueryPlanImpl*>(ptr);
}

}  // namespace

Result<std::shared_ptr<QueryPlan>> QueryPlan::Make(QueryContext* ctx) {
  return std::make_shared<QueryPlanImpl>(ctx);
}

const QueryPlan::NodeVector& QueryPlan::sources() const {
  return ToDerived(this)->sources_;
}

const QueryPlan::NodeVector& QueryPlan::sinks() const { return ToDerived(this)->sinks_; }

QueryContext* QueryPlan::context() { return ToDerived(this)->context_; }

const QueryContext* QueryPlan::context() const { return ToDerived(this)->context_; }

}  // namespace engine
}  // namespace arrow
