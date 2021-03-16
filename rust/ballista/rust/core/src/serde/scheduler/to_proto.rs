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

use std::convert::TryInto;

use crate::error::BallistaError;
use crate::serde::protobuf;
use crate::serde::protobuf::action::ActionType;
use crate::serde::scheduler::{
    Action, ExecutePartition, PartitionId, PartitionLocation, PartitionStats,
};

impl TryInto<protobuf::Action> for Action {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::Action, Self::Error> {
        match self {
            Action::ExecutePartition(partition) => Ok(protobuf::Action {
                action_type: Some(ActionType::ExecutePartition(partition.try_into()?)),
                settings: vec![],
            }),
            Action::FetchPartition(partition_id) => Ok(protobuf::Action {
                action_type: Some(ActionType::FetchPartition(partition_id.into())),
                settings: vec![],
            }),
        }
    }
}

impl TryInto<protobuf::ExecutePartition> for ExecutePartition {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::ExecutePartition, Self::Error> {
        Ok(protobuf::ExecutePartition {
            job_id: self.job_id,
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id.iter().map(|n| *n as u32).collect(),
            plan: Some(self.plan.try_into()?),
            partition_location: vec![],
        })
    }
}

impl Into<protobuf::PartitionId> for PartitionId {
    fn into(self) -> protobuf::PartitionId {
        protobuf::PartitionId {
            job_id: self.job_id,
            stage_id: self.stage_id as u32,
            partition_id: self.partition_id as u32,
        }
    }
}

impl TryInto<protobuf::PartitionLocation> for PartitionLocation {
    type Error = BallistaError;

    fn try_into(self) -> Result<protobuf::PartitionLocation, Self::Error> {
        Ok(protobuf::PartitionLocation {
            partition_id: Some(self.partition_id.into()),
            executor_meta: Some(self.executor_meta.into()),
            partition_stats: Some(self.partition_stats.into()),
        })
    }
}

impl Into<protobuf::PartitionStats> for PartitionStats {
    fn into(self) -> protobuf::PartitionStats {
        let none_value = -1_i64;
        protobuf::PartitionStats {
            num_rows: self.num_rows.map(|n| n as i64).unwrap_or(none_value),
            num_batches: self.num_batches.map(|n| n as i64).unwrap_or(none_value),
            num_bytes: self.num_bytes.map(|n| n as i64).unwrap_or(none_value),
            column_stats: vec![],
        }
    }
}
