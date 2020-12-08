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

//! This module defines the interface for logical nodes
use super::{Expr, LogicalPlan};
use crate::logical_plan::DFSchemaRef;
use std::{any::Any, collections::HashSet, fmt, sync::Arc};

/// This defines the interface for `LogicalPlan` nodes that can be
/// used to extend DataFusion with custom relational operators.
///
/// See the example in
/// [user_defined_plan.rs](../../tests/user_defined_plan.rs) for an
/// example of how to use this extension API
pub trait UserDefinedLogicalNode: fmt::Debug {
    /// Return a reference to self as Any, to support dynamic downcasting
    fn as_any(&self) -> &dyn Any;

    /// Return the logical plan's inputs
    fn inputs(&self) -> Vec<&LogicalPlan>;

    /// Return the output schema of this logical plan node
    fn schema(&self) -> &DFSchemaRef;

    /// returns all expressions in the current logical plan node. This
    /// should not include expressions of any inputs (aka
    /// non-recursively) These expressions are used for optimizer
    /// passes and rewrites.
    fn expressions(&self) -> Vec<Expr>;

    /// A list of output columns (e.g. the names of columns in
    /// self.schema()) for which predicates can not be pushed below
    /// this node without changing the output.
    ///
    /// By default, this returns all columns and thus prevents any
    /// predicates from being pushed below this node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // default (safe) is all columns in the schema.
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Write a single line, human readable string to `f` for use in explain plan
    ///
    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result;

    /// Create a new `ExtensionPlanNode` with the specified children
    /// and expressions. This function is used during optimization
    /// when the plan is being rewritten and a new instance of the
    /// `ExtensionPlanNode` must be created.
    ///
    /// Note that exprs and inputs are in the same order as the result
    /// of self.inputs and self.exprs.
    ///
    /// So, `self.from_template(exprs, ..).expressions() == exprs
    fn from_template(
        &self,
        exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync>;
}
