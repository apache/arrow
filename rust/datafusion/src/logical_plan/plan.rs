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
//! This module contains the  `LogicalPlan` enum that describes queries
//! via a logical query plan.

use std::{
    fmt::{self, Display},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::datasource::TableProvider;
use crate::sql::parser::FileType;

use super::display::{GraphvizVisitor, IndentVisitor};
use super::expr::Expr;
use super::extension::UserDefinedLogicalNode;
use crate::logical_plan::dfschema::DFSchemaRef;

/// Describes the source of the table, either registered on the context or by reference
#[derive(Clone)]
pub enum TableSource {
    /// The source provider is registered in the context with the corresponding name
    FromContext(String),
    /// The source provider is passed directly by reference
    FromProvider(Arc<dyn TableProvider + Send + Sync>),
}

/// Join type
#[derive(Debug, Clone)]
pub enum JoinType {
    /// Inner join
    Inner,
    /// Left join
    Left,
    /// Right join
    Right,
}

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner and the DataFrame API.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// The schema description of the output
        schema: DFSchemaRef,
    },
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter {
        /// The predicate expression, which must have Boolean type.
        predicate: Expr,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate {
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description of the aggregate output
        schema: DFSchemaRef,
    },
    /// Sorts its input according to a list of sort expressions.
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
    /// Join two logical plans on one or more join columns
    Join {
        /// Left input
        left: Arc<LogicalPlan>,
        /// Right input
        right: Arc<LogicalPlan>,
        /// Equijoin clause expressed as pairs of (left, right) join columns
        on: Vec<(String, String)>,
        /// Join type
        join_type: JoinType,
        /// The output schema, containing fields from the left and right inputs
        schema: DFSchemaRef,
    },
    /// Produces rows from a table provider by reference or from the context
    TableScan {
        /// The name of the schema
        schema_name: String,
        /// The source of the table
        source: TableSource,
        /// The schema of the source data
        table_schema: SchemaRef,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: DFSchemaRef,
    },
    /// Produces rows that come from a `Vec` of in memory `RecordBatch`es
    InMemoryScan {
        /// Record batch partitions
        data: Vec<Vec<RecordBatch>>,
        /// The schema of the record batches
        schema: SchemaRef,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: DFSchemaRef,
    },
    /// Produces rows by scanning Parquet file(s)
    ParquetScan {
        /// The path to the files
        path: String,
        /// The schema of the Parquet file(s)
        schema: SchemaRef,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: DFSchemaRef,
    },
    /// Produces rows by scanning a CSV file(s)
    CsvScan {
        /// The path to the files
        path: String,
        /// The underlying table schema
        schema: SchemaRef,
        /// Whether the CSV file(s) have a header containing column names
        has_header: bool,
        /// An optional column delimiter. Defaults to `b','`
        delimiter: Option<u8>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: DFSchemaRef,
    },
    /// Produces no rows: An empty relation with an empty schema
    EmptyRelation {
        /// Whether to produce a placeholder row
        produce_one_row: bool,
        /// The schema description of the output
        schema: DFSchemaRef,
    },
    /// Produces the first `n` tuples from its input and discards the rest.
    Limit {
        /// The limit
        n: usize,
        /// The logical plan
        input: Arc<LogicalPlan>,
    },
    /// Creates an external table.
    CreateExternalTable {
        /// The table schema
        schema: DFSchemaRef,
        /// The table name
        name: String,
        /// The physical location
        location: String,
        /// The file type of physical file
        file_type: FileType,
        /// Whether the CSV file contains a header
        has_header: bool,
    },
    /// Produces a relation with string representations of
    /// various parts of the plan
    Explain {
        /// Should extra (detailed, intermediate plans) be included?
        verbose: bool,
        /// The logical plan that is being EXPLAIN'd
        plan: Arc<LogicalPlan>,
        /// Represent the various stages plans have gone through
        stringified_plans: Vec<StringifiedPlan>,
        /// The output schema of the explain (2 columns of text)
        schema: DFSchemaRef,
    },
    /// Extension operator defined outside of DataFusion
    Extension {
        /// The runtime extension operator
        node: Arc<dyn UserDefinedLogicalNode + Send + Sync>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            LogicalPlan::EmptyRelation { schema, .. } => &schema,
            LogicalPlan::InMemoryScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::CsvScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::ParquetScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::TableScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Join { schema, .. } => &schema,
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::CreateExternalTable { schema, .. } => &schema,
            LogicalPlan::Explain { schema, .. } => &schema,
            LogicalPlan::Extension { node } => &node.schema(),
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
    }
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `LogicalPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
////
/// To use, define a struct that implements this trait and then invoke
/// "LogicalPlan::accept".
///
/// For example, for a logical plan like:
///
/// Projection: #id
///    Filter: #state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
///
/// The sequence of visit operations would be:
/// ```text
/// visitor.pre_visit(Projection)
/// visitor.pre_visit(Filter)
/// visitor.pre_visit(CsvScan)
/// visitor.post_visit(CsvScan)
/// visitor.post_visit(Filter)
/// visitor.post_visit(Projection)
/// ```
pub trait PlanVisitor {
    /// The type of error returned by this visitor
    type Error;

    /// Invoked on a logical plan before any of its child inputs have been
    /// visited. If Ok(true) is returned, the recursion continues. If
    /// Err(..) or Ok(false) are returned, the recursion stops
    /// immedately and the error, if any, is returned to `accept`
    fn pre_visit(&mut self, plan: &LogicalPlan)
        -> std::result::Result<bool, Self::Error>;

    /// Invoked on a logical plan after all of its child inputs have
    /// been visited. The return value is handled the same as the
    /// return value of `pre_visit`. The provided default implementation
    /// returns `Ok(true)`.
    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

impl LogicalPlan {
    /// returns all inputs in the logical plan. Returns Ok(true) if
    /// all nodes were visited, and Ok(false) if any call to
    /// `pre_visit` or `post_visit` returned Ok(false) and may have
    /// cut short the recursion
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            LogicalPlan::Projection { input, .. } => input.accept(visitor)?,
            LogicalPlan::Filter { input, .. } => input.accept(visitor)?,
            LogicalPlan::Aggregate { input, .. } => input.accept(visitor)?,
            LogicalPlan::Sort { input, .. } => input.accept(visitor)?,
            LogicalPlan::Join { left, right, .. } => {
                left.accept(visitor)? && right.accept(visitor)?
            }
            LogicalPlan::Limit { input, .. } => input.accept(visitor)?,
            LogicalPlan::Extension { node } => {
                for input in node.inputs() {
                    if !input.accept(visitor)? {
                        return Ok(false);
                    }
                }
                true
            }
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::InMemoryScan { .. }
            | LogicalPlan::ParquetScan { .. }
            | LogicalPlan::CsvScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Explain { .. } => true,
        };
        if !recurse {
            return Ok(false);
        }

        if !visitor.post_visit(self)? {
            return Ok(false);
        }

        Ok(true)
    }
}

// Various implementations for printing out LogicalPlans
impl LogicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    ///
    /// ```text
    /// Projection: #id
    ///    Filter: #state Eq Utf8(\"CO\")\
    ///       CsvScan: employee.csv projection=Some([0, 3])
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent
    /// let display_string = format!("{}", plan.display_indent());
    ///
    /// assert_eq!("Filter: #id Eq Int32(5)\
    ///              \n  TableScan: foo.csv projection=None",
    ///             display_string);
    /// ```
    pub fn display_indent<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor::new(f, with_schema);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces a single line
    /// per node that includes the output schema. For example:
    ///
    /// ```text
    /// Projection: #id [id:Int32]\
    ///    Filter: #state Eq Utf8(\"CO\") [id:Int32, state:Utf8]\
    ///      TableScan: employee.csv projection=Some([0, 3]) [id:Int32, state:Utf8]";
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent_schema
    /// let display_string = format!("{}", plan.display_indent_schema());
    ///
    /// assert_eq!("Filter: #id Eq Int32(5) [id:Int32]\
    ///             \n  TableScan: foo.csv projection=None [id:Int32]",
    ///             display_string);
    /// ```
    pub fn display_indent_schema<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = true;
                let mut visitor = IndentVisitor::new(f, with_schema);
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces lines meant for
    /// graphical display using the `DOT` language. This format can be
    /// visualized using software from
    /// [`graphviz`](https://graphviz.org/)
    ///
    /// This currently produces two graphs -- one with the basic
    /// structure, and one with additional details such as schema.
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_graphviz
    /// let graphviz_string = format!("{}", plan.display_graphviz());
    /// ```
    ///
    /// If graphviz string is saved to a file such as `/tmp/example.dot`, the following
    /// commands can be used to render it as a pdf:
    ///
    /// ```bash
    ///   dot -Tpdf < /tmp/example.dot  > /tmp/example.pdf
    /// ```
    ///
    pub fn display_graphviz<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFusion GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;

                let mut visitor = GraphvizVisitor::new(f);

                visitor.pre_visit_plan("LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                writeln!(f, "}}")?;
                writeln!(f, "// End DataFusion GraphViz Plan")?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children. For example:
    ///
    /// ```text
    /// Projection: #id
    /// ```
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display
    /// let display_string = format!("{}", plan.display());
    ///
    /// assert_eq!("TableScan: foo.csv projection=None", display_string);
    /// ```
    pub fn display<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match *self.0 {
                    LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
                    LogicalPlan::TableScan {
                        ref source,
                        ref projection,
                        ..
                    } => match source {
                        TableSource::FromContext(table_name) => write!(
                            f,
                            "TableScan: {} projection={:?}",
                            table_name, projection
                        ),
                        TableSource::FromProvider(_) => {
                            write!(f, "TableScan: projection={:?}", projection)
                        }
                    },
                    LogicalPlan::InMemoryScan { ref projection, .. } => {
                        write!(f, "InMemoryScan: projection={:?}", projection)
                    }
                    LogicalPlan::CsvScan {
                        ref path,
                        ref projection,
                        ..
                    } => write!(f, "CsvScan: {} projection={:?}", path, projection),
                    LogicalPlan::ParquetScan {
                        ref path,
                        ref projection,
                        ..
                    } => write!(f, "ParquetScan: {} projection={:?}", path, projection),
                    LogicalPlan::Projection { ref expr, .. } => {
                        write!(f, "Projection: ")?;
                        for i in 0..expr.len() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr[i])?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter {
                        predicate: ref expr,
                        ..
                    } => write!(f, "Filter: {:?}", expr),
                    LogicalPlan::Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    } => write!(
                        f,
                        "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                        group_expr, aggr_expr
                    ),
                    LogicalPlan::Sort { ref expr, .. } => {
                        write!(f, "Sort: ")?;
                        for i in 0..expr.len() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr[i])?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Join { on: ref keys, .. } => {
                        let join_expr: Vec<String> =
                            keys.iter().map(|(l, r)| format!("{} = {}", l, r)).collect();
                        write!(f, "Join: {}", join_expr.join(", "))
                    }
                    LogicalPlan::Limit { ref n, .. } => write!(f, "Limit: {}", n),
                    LogicalPlan::CreateExternalTable { ref name, .. } => {
                        write!(f, "CreateExternalTable: {:?}", name)
                    }
                    LogicalPlan::Explain { .. } => write!(f, "Explain"),
                    LogicalPlan::Extension { ref node } => node.fmt_for_explain(f),
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

/// Represents which type of plan
#[derive(Debug, Clone, PartialEq)]
pub enum PlanType {
    /// The initial LogicalPlan provided to DataFusion
    LogicalPlan,
    /// The LogicalPlan which results from applying an optimizer pass
    OptimizedLogicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The physical plan, prepared for execution
    PhysicalPlan,
}

impl From<&PlanType> for String {
    fn from(t: &PlanType) -> Self {
        match t {
            PlanType::LogicalPlan => "logical_plan".into(),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                format!("logical_plan after {}", optimizer_name)
            }
            PlanType::PhysicalPlan => "physical_plan".into(),
        }
    }
}

/// Represents some sort of execution plan, in String form
#[derive(Debug, Clone, PartialEq)]
pub struct StringifiedPlan {
    /// An identifier of what type of plan this string represents
    pub plan_type: PlanType,
    /// The string representation of the plan
    pub plan: Arc<String>,
}

impl StringifiedPlan {
    /// Create a new Stringified plan of `plan_type` with string
    /// representation `plan`
    pub fn new(plan_type: PlanType, plan: impl Into<String>) -> Self {
        StringifiedPlan {
            plan_type,
            plan: Arc::new(plan.into()),
        }
    }

    /// returns true if this plan should be displayed. Generally
    /// `verbose_mode = true` will display all available plans
    pub fn should_display(&self, verbose_mode: bool) -> bool {
        self.plan_type == PlanType::LogicalPlan || verbose_mode
    }
}

#[cfg(test)]
mod tests {
    use super::super::{col, lit, LogicalPlanBuilder};
    use super::*;

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    fn display_plan() -> LogicalPlan {
        LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )
        .unwrap()
        .filter(col("state").eq(lit("CO")))
        .unwrap()
        .project(vec![col("id")])
        .unwrap()
        .build()
        .unwrap()
    }

    #[test]
    fn test_display_indent() {
        let plan = display_plan();

        let expected = "Projection: #id\
        \n  Filter: #state Eq Utf8(\"CO\")\
        \n    TableScan: employee.csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{}", plan.display_indent()));
    }

    #[test]
    fn test_display_indent_schema() {
        let plan = display_plan();

        let expected = "Projection: #id [id:Int32]\
                        \n  Filter: #state Eq Utf8(\"CO\") [id:Int32, state:Utf8]\
                        \n    TableScan: employee.csv projection=Some([0, 3]) [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
    }

    #[test]
    fn test_display_graphviz() {
        let plan = display_plan();

        // just test for a few key lines in the output rather than the
        // whole thing to make test mainteance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert!(
            graphviz.contains(
                r#"// Begin DataFusion GraphViz Plan (see https://graphviz.org)"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(
            graphviz.contains(
                r#"[shape=box label="TableScan: employee.csv projection=Some([0, 3])"]"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(graphviz.contains(r#"[shape=box label="TableScan: employee.csv projection=Some([0, 3])\nSchema: [id:Int32, state:Utf8]"]"#),
                "\n{}", plan.display_graphviz());
        assert!(
            graphviz.contains(r#"// End DataFusion GraphViz Plan"#),
            "\n{}",
            plan.display_graphviz()
        );
    }

    /// Tests for the Visitor trait and walking logical plan nodes

    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }
    impl PlanVisitor for OkVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
                "post_visit Filter",
                "post_visit Projection"
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Counter than counts to zero and returns true when it gets there
    struct OptionalCounter {
        val: Option<usize>,
    }
    impl OptionalCounter {
        fn new(val: usize) -> Self {
            Self { val: Some(val) }
        }
        // Decrements the counter by 1, if any, returning true if it hits zero
        fn dec(&mut self) -> bool {
            if Some(0) == self.val {
                true
            } else {
                self.val = self.val.take().map(|i| i - 1);
                false
            }
        }
    }

    #[derive(Debug, Default)]
    /// Visitor that returns false after some number of visits
    struct StoppingVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_false_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_false_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for StoppingVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_pre_in.dec() {
                return Ok(false);
            }
            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_post_in.dec() {
                return Ok(false);
            }

            self.inner.post_visit(plan)
        }
    }

    /// test earliy stopping in pre-visit
    #[test]
    fn early_stoping_pre_visit() {
        let mut visitor = StoppingVisitor::default();
        visitor.return_false_from_pre_in = OptionalCounter::new(2);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter",]
        );
    }

    #[test]
    fn early_stoping_post_visit() {
        let mut visitor = StoppingVisitor::default();
        visitor.return_false_from_post_in = OptionalCounter::new(1);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Visitor that returns an error after some number of visits
    struct ErrorVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_error_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_error_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for ErrorVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_pre_in.dec() {
                return Err("Error in pre_visit".into());
            }

            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_post_in.dec() {
                return Err("Error in post_visit".into());
            }

            self.inner.post_visit(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor::default();
        visitor.return_error_from_pre_in = OptionalCounter::new(2);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);

        if let Err(e) = res {
            assert_eq!("Error in pre_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter",]
        );
    }

    #[test]
    fn error_post_visit() {
        let mut visitor = ErrorVisitor::default();
        visitor.return_error_from_post_in = OptionalCounter::new(1);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        if let Err(e) = res {
            assert_eq!("Error in post_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        LogicalPlanBuilder::scan("default", "employee.csv", &schema, Some(vec![0]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }
}
