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
//! This module provides logic for displaying LogicalPlans in various styles

use super::{LogicalPlan, PlanVisitor};
use crate::logical_plan::DFSchema;
use std::fmt;

/// Formats plans with a single line per node. For example:
///
/// Projection: #id
///    Filter: #state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
pub struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// If true, includes summarized schema information
    with_schema: bool,
    indent: u32,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    /// Create a visitor that will write a formatted LogicalPlan to f. If `with_schema` is
    /// true, includes schema information on each line.
    pub fn new(f: &'a mut fmt::Formatter<'b>, with_schema: bool) -> Self {
        Self {
            f,
            with_schema,
            indent: 0,
        }
    }

    fn write_indent(&mut self) -> fmt::Result {
        for _ in 0..self.indent {
            write!(self.f, "  ")?;
        }
        Ok(())
    }
}

impl<'a, 'b> PlanVisitor for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, fmt::Error> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        self.write_indent()?;

        write!(self.f, "{}", plan.display())?;
        if self.with_schema {
            write!(self.f, " {}", display_schema(plan.schema()))?;
        }

        self.indent += 1;
        Ok(true)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, fmt::Error> {
        self.indent -= 1;
        Ok(true)
    }
}

/// Print the schema in a compact representation to `buf`
///
/// For example: `foo:Utf8` if `foo` can not be null, and
/// `foo:Utf8;N` if `foo` is nullable.
///
/// ```
/// use arrow::datatypes::{Field, Schema, DataType};
/// use datafusion::logical_plan::DFSchema;
/// # use datafusion::error::Result;
/// # use datafusion::logical_plan::display_schema;
/// # fn main() -> Result<()> {
/// let schema = DFSchema::from(&Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("first_name", DataType::Utf8, true),
///  ]))?;
///
///  assert_eq!(
///      "[id:Int32, first_name:Utf8;N]",
///      format!("{}", display_schema(&schema))
///  );
/// # Ok(())
/// # }
/// ```
pub fn display_schema<'a>(schema: &'a DFSchema) -> impl fmt::Display + 'a {
    struct Wrapper<'a>(&'a DFSchema);

    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "[")?;
            for (idx, field) in self.0.fields().iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                let nullable_str = if field.is_nullable() { ";N" } else { "" };
                write!(
                    f,
                    "{}:{:?}{}",
                    field.qualified_name(),
                    field.data_type(),
                    nullable_str
                )?;
            }
            write!(f, "]")
        }
    }
    Wrapper(schema)
}

/// Logic related to creating DOT language graphs.
#[derive(Default)]
struct GraphvizBuilder {
    id_gen: usize,
}

impl GraphvizBuilder {
    fn next_id(&mut self) -> usize {
        self.id_gen += 1;
        self.id_gen
    }

    // write out the start of the subgraph cluster
    fn start_cluster(&mut self, f: &mut fmt::Formatter, title: &str) -> fmt::Result {
        writeln!(f, "  subgraph cluster_{}", self.next_id())?;
        writeln!(f, "  {{")?;
        writeln!(f, "    graph[label={}]", Self::quoted(title))
    }

    // write out the end of the subgraph cluster
    fn end_cluster(&mut self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "  }}")
    }

    /// makes a quoted string suitable for inclusion in a graphviz chart
    fn quoted(label: &str) -> String {
        let label = label.replace('"', "_");
        format!("\"{}\"", label)
    }
}

/// Formats plans for graphical display using the `DOT` language. This
/// format can be visualized using software from
/// [`graphviz`](https://graphviz.org/)
pub struct GraphvizVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    graphviz_builder: GraphvizBuilder,
    /// If true, includes summarized schema information
    with_schema: bool,

    /// Holds the ids (as generated from `graphviz_builder` of all
    /// parent nodes
    parent_ids: Vec<usize>,
}

impl<'a, 'b> GraphvizVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self {
            f,
            graphviz_builder: GraphvizBuilder::default(),
            with_schema: false,
            parent_ids: Vec::new(),
        }
    }

    /// Sets a flag which controls if the output schema is displayed
    pub fn set_with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }

    pub fn pre_visit_plan(&mut self, label: &str) -> fmt::Result {
        self.graphviz_builder.start_cluster(self.f, label)
    }

    pub fn post_visit_plan(&mut self) -> fmt::Result {
        self.graphviz_builder.end_cluster(self.f)
    }
}

impl<'a, 'b> PlanVisitor for GraphvizVisitor<'a, 'b> {
    type Error = fmt::Error;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, fmt::Error> {
        let id = self.graphviz_builder.next_id();

        // Create a new graph node for `plan` such as
        // id [label="foo"]
        let label = if self.with_schema {
            format!(
                "{}\\nSchema: {}",
                plan.display(),
                display_schema(plan.schema())
            )
        } else {
            format!("{}", plan.display())
        };

        writeln!(
            self.f,
            "    {}[shape=box label={}]",
            id,
            GraphvizBuilder::quoted(&label)
        )?;

        // Create an edge to our parent node, if any
        //  parent_id -> id
        if let Some(parent_id) = self.parent_ids.last() {
            writeln!(
                self.f,
                "    {} -> {} [arrowhead=none, arrowtail=normal, dir=back]",
                parent_id, id
            )?;
        }

        self.parent_ids.push(id);
        Ok(true)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, fmt::Error> {
        // always be non-empty as pre_visit always pushes
        self.parent_ids.pop().unwrap();
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::logical_plan::DFField;
    use arrow::datatypes::DataType;

    #[test]
    fn test_display_empty_schema() -> Result<()> {
        let schema = DFSchema::new(vec![])?;
        assert_eq!("[]", format!("{}", display_schema(&schema)));
        Ok(())
    }

    #[test]
    fn test_display_schema() -> Result<()> {
        let schema = DFSchema::new(vec![
            DFField::new(None, "id", DataType::Int32, false),
            DFField::new(None, "first_name", DataType::Utf8, true),
        ])?;

        assert_eq!(
            "[id:Int32, first_name:Utf8;N]",
            format!("{}", display_schema(&schema))
        );
        Ok(())
    }

    #[test]
    fn test_display_qualified_schema() -> Result<()> {
        let schema = DFSchema::new(vec![
            DFField::new(Some("t1"), "id", DataType::Int32, false),
            DFField::new(None, "first_name", DataType::Utf8, true),
        ])?;

        assert_eq!(
            "[t1.id:Int32, first_name:Utf8;N]",
            format!("{}", display_schema(&schema))
        );
        Ok(())
    }
}
