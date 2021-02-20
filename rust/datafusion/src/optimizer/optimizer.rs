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

//! Query optimizer traits

use crate::error::Result;
use crate::logical_plan::LogicalPlan;

/// `OptimizerRule` transforms one ['LogicalPlan'] into another which
/// computes the same results, but in a potentially more efficient
/// way.
pub trait OptimizerRule {
    /// Rewrite `plan` to an optimized form
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;
}
