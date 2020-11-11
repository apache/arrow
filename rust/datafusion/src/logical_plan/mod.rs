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

//! This module provides a logical query plan enum that can describe queries. Logical query
//! plans can be created from a SQL statement or built programmatically via the Table API.
//!
//! Logical query plans can then be optimized and executed directly, or translated into
//! physical query plans and executed.

mod builder;
mod display;
mod expr;
mod extension;
mod operators;
mod plan;
mod registry;

pub use builder::LogicalPlanBuilder;
pub use display::display_schema;
pub use expr::{
    and, array, avg, binary_expr, col, concat, count, create_udaf, create_udf,
    exprlist_to_fields, length, lit, max, min, sum, Expr, Literal,
};
pub use extension::UserDefinedLogicalNode;
pub use operators::Operator;
pub use plan::{LogicalPlan, PlanType, PlanVisitor, StringifiedPlan, TableSource};
pub use registry::FunctionRegistry;
