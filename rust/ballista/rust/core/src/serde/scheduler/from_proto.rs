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

use std::{collections::HashMap, convert::TryInto};

use crate::error::BallistaError;
use crate::serde::protobuf;
use crate::serde::protobuf::action::ActionType;
use crate::serde::scheduler::{
    Action, ExecutePartition, PartitionId, PartitionLocation, PartitionStats,
};

use datafusion::logical_plan::LogicalPlan;
use uuid::Uuid;

impl TryInto<Action> for protobuf::Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<Action, Self::Error> {
        match self.action_type {
            Some(ActionType::ExecutePartition(partition)) => {
                Ok(Action::ExecutePartition(ExecutePartition::new(
                    partition.job_id,
                    partition.stage_id as usize,
                    partition.partition_id.iter().map(|n| *n as usize).collect(),
                    partition
                        .plan
                        .as_ref()
                        .ok_or_else(|| {
                            BallistaError::General(
                                "PhysicalPlanNode in ExecutePartition is missing".to_owned(),
                            )
                        })?
                        .try_into()?,
                    HashMap::new(),
                )))
            }
            Some(ActionType::FetchPartition(partition)) => {
                Ok(Action::FetchPartition(partition.try_into()?))
            }
            _ => Err(BallistaError::General(
                "scheduler::from_proto(Action) invalid or missing action".to_owned(),
            )),
        }
    }
}

impl TryInto<PartitionId> for protobuf::PartitionId {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionId, Self::Error> {
        Ok(PartitionId::new(
            &self.job_id,
            self.stage_id as usize,
            self.partition_id as usize,
        ))
    }
}

impl Into<PartitionStats> for protobuf::PartitionStats {
    fn into(self) -> PartitionStats {
        PartitionStats::new(
            foo(self.num_rows),
            foo(self.num_batches),
            foo(self.num_bytes),
        )
    }
}

fn foo(n: i64) -> Option<u64> {
    if n < 0 {
        None
    } else {
        Some(n as u64)
    }
}

impl TryInto<PartitionLocation> for protobuf::PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<PartitionLocation, Self::Error> {
        Ok(PartitionLocation {
            partition_id: self
                .partition_id
                .ok_or_else(|| {
                    BallistaError::General(
                        "partition_id in PartitionLocation is missing.".to_owned(),
                    )
                })?
                .try_into()?,
            executor_meta: self
                .executor_meta
                .ok_or_else(|| {
                    BallistaError::General(
                        "executor_meta in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
            partition_stats: self
                .partition_stats
                .ok_or_else(|| {
                    BallistaError::General(
                        "partition_stats in PartitionLocation is missing".to_owned(),
                    )
                })?
                .into(),
        })
    }
}
