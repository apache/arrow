// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod from_proto;
pub mod to_proto;

#[cfg(test)]
mod roundtrip_tests {
    use datafusion::physical_plan::hash_utils::JoinType;
    use std::{convert::TryInto, sync::Arc};

    use arrow::datatypes::{DataType, Schema};
    use datafusion::physical_plan::ColumnarValue;
    use datafusion::physical_plan::{
        empty::EmptyExec,
        expressions::{Avg, Column, PhysicalSortExpr},
        hash_aggregate::{AggregateMode, HashAggregateExec},
        hash_join::HashJoinExec,
        limit::{GlobalLimitExec, LocalLimitExec},
        sort::SortExec,
        ExecutionPlan,
    };
    use datafusion::physical_plan::{AggregateExpr, Distribution, Partitioning, PhysicalExpr};

    use super::super::super::error::Result;
    use super::super::protobuf;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let proto: protobuf::PhysicalPlanNode = exec_plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> = (&proto).try_into()?;
        assert_eq!(
            format!("{:?}", exec_plan),
            format!("{:?}", result_exec_plan)
        );
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_local_limit() -> Result<()> {
        roundtrip_test(Arc::new(LocalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_global_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_hash_join() -> Result<()> {
        use arrow::datatypes::{DataType, Field, Schema};
        let field_a = Field::new("col", DataType::Int64, false);
        let schema_left = Schema::new(vec![field_a.clone()]);
        let schema_right = Schema::new(vec![field_a]);

        roundtrip_test(Arc::new(HashJoinExec::try_new(
            Arc::new(EmptyExec::new(false, Arc::new(schema_left))),
            Arc::new(EmptyExec::new(false, Arc::new(schema_right))),
            &[("col".to_string(), "col".to_string())],
            &JoinType::Inner,
        )?))
    }

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name))
    }

    #[test]
    fn rountrip_hash_aggregate() -> Result<()> {
        use arrow::datatypes::{DataType, Field, Schema};
        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![(col("a"), "unused".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b"),
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        roundtrip_test(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            groups.clone(),
            aggregates.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            schema,
        )?))
    }

    #[test]
    fn roundtrip_filter_with_not_and_in_list() -> Result<()> {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::logical_plan::Operator;
        use datafusion::physical_plan::{
            expressions::{binary, lit, InListExpr, NotExpr},
            filter::FilterExec,
        };
        use datafusion::scalar::ScalarValue;
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let field_c = Field::new("c", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
        let not = Arc::new(NotExpr::new(col("a")));
        let in_list = Arc::new(InListExpr::new(
            col("b"),
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            false,
        ));
        let and = binary(not, Operator::And, in_list, &schema)?;
        roundtrip_test(Arc::new(FilterExec::try_new(
            and,
            Arc::new(EmptyExec::new(false, schema.clone())),
        )?))
    }

    #[test]
    fn roundtrip_sort() -> Result<()> {
        use arrow::compute::kernels::sort::SortOptions;
        use arrow::datatypes::{DataType, Field, Schema};
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: col("a"),
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("b"),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
        ];
        roundtrip_test(Arc::new(SortExec::try_new(
            sort_exprs,
            Arc::new(EmptyExec::new(false, schema)),
        )?))
    }
}
