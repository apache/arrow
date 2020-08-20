//! Prototype LogicalPlanNode interface for defining extensions for LogicalPlan nodes

use crate::error::Result;
use crate::{
    execution::{context::ExecutionContextState, physical_plan::ExecutionPlan},
    logicalplan::{Expr, LogicalPlan},
};
use arrow::datatypes::Schema;
use core::fmt;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

/// This defines the interface for (currently user defined)
/// LogicalPLan nodes that can be used to extend the logic
pub trait LogicalPlanNode {
    /// Return  a reference to the logical plan's inputs
    fn inputs(&self) -> Vec<&LogicalPlan>;

    /// Get a reference to the logical plan's schema
    fn schema(&self) -> &Schema;

    /// returns all expressions (non-recursively) in the current logical plan node.
    fn expressions(&self) -> Vec<Expr>;

    /// A list of output columns (column names in self.schema()) for
    /// which predicates can not be pushed below this node without
    /// changing the output
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // default (safe) is all columns in the schema.
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Write a single line human readable string to `f` for use in explain plan
    fn format_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result;

    /// Create a clone of this node.
    ///
    /// Note std::Clone needs a Sized type, so we must implement a
    /// clone that creates a node with a known Size (i.e. Box)
    //
    fn dyn_clone(&self) -> Box<dyn LogicalPlanNode>;

    /// Create a clone of this LogicalPlanNode with inputs and expressions replaced.
    ///
    /// Note that exprs and inputs are in the same order as the result
    /// of self.inputs and self.exprs.
    ///
    /// So, clone_from_template(exprs).exprs() == exprs
    fn clone_from_template(
        &self,
        exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Box<dyn LogicalPlanNode>;

    /// Create the corresponding physical scheplan for this node
    fn create_physical_plan(
        &self,
        input_physical_plans: Vec<Arc<dyn ExecutionPlan>>,
        ctx_state: Arc<Mutex<ExecutionContextState>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Adaptor LogicalPlanNode that can be Clone'd (needed in the current
/// formulation of LogicalPlan)
pub struct CloneableLogicalPlanNode {
    /// The actual LogicalPlaNode
    pub node: Box<dyn LogicalPlanNode>,
}

impl CloneableLogicalPlanNode {
    /// Create a new CloneableLogicalPlanNode
    pub fn new(node: Box<dyn LogicalPlanNode>) -> Self {
        CloneableLogicalPlanNode { node }
    }
}

impl Clone for CloneableLogicalPlanNode {
    fn clone(&self) -> Self {
        CloneableLogicalPlanNode {
            node: self.node.dyn_clone(),
        }
    }
}
