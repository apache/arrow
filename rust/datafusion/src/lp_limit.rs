//! Example of how a "User Defined logical plan node would work. Use
//! the LogicalPlanNode trait for LimitNode

use crate::error::Result;
use crate::{
    execution::{
        context::ExecutionContextState,
        physical_plan::{limit::GlobalLimitExec, ExecutionPlan},
    },
    logicalplan::{Expr, LogicalPlan},
    lp::LogicalPlanNode,
};
use arrow::datatypes::Schema;
use core::fmt;
use std::sync::{Arc, Mutex};

/// Produces the first `n` tuples from its input and discards the rest.
#[derive(Clone)]
pub struct LimitNode {
    /// The limit
    n: usize,
    /// The input plan -- always exactly a single entry, but stored in
    /// a Vector to implement `LogicalPlanNode::inputs`
    inputs: Vec<Arc<LogicalPlan>>,
}

impl LimitNode {
    pub fn new(n: usize, input: LogicalPlan) -> Self {
        LimitNode {
            n,
            inputs: vec![Arc::new(input)],
        }
    }

    // a limit node has a single input
    pub fn input(&self) -> Arc<LogicalPlan> {
        self.inputs[0].clone()
    }
}

impl LogicalPlanNode for LimitNode {
    // returns a reference to the inputs of this node
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().map(|arc| arc.as_ref()).collect()
    }

    fn schema(&self) -> &Schema {
        self.inputs[0].schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    /// Write a single line human readable string to `f` for use in explain plan
    fn format_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Limit: {}", self.n)
    }

    fn dyn_clone(&self) -> Box<dyn LogicalPlanNode> {
        Box::new(self.clone())
    }

    fn clone_from_template(
        &self,
        _exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Box<dyn LogicalPlanNode> {
        let inputs = inputs.iter().map(|lp| Arc::new(lp.clone())).collect();

        Box::new(LimitNode { n: self.n, inputs })
    }

    fn create_physical_plan(
        &self,
        input_physical_plans: Vec<Arc<dyn ExecutionPlan>>,
        ctx_state: Arc<Mutex<ExecutionContextState>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = self.input().as_ref().schema().clone();
        let input_schema = Arc::new(input_schema);

        assert_eq!(
            input_physical_plans.len(),
            1,
            "Limit can only have a single input plan"
        );

        Ok(Arc::new(GlobalLimitExec::new(
            input_schema,
            input_physical_plans[0].partitions()?,
            self.n,
            ctx_state
                .lock()
                .expect("failed to lock mutex")
                .config
                .concurrency,
        )))
    }
}
