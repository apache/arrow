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

//! Functionality used both on logical and physical plans

use crate::error::{ExecutionError, Result};
use arrow::datatypes::{Field, Schema};
use std::collections::HashSet;

/// All valid types of joins.
#[derive(Clone, Debug)]
pub enum JoinHow {
    /// Inner join
    Inner,
}

/// Checks whether the schemas "left" and "right" and columns "on" represent a valid join.
/// They are valid whenever their columns' intersection equals the set `on`
pub fn check_join_is_valid(
    left: &Schema,
    right: &Schema,
    on: &HashSet<String>,
) -> Result<()> {
    let left: HashSet<String> = left.fields().iter().map(|f| f.name().clone()).collect();
    let right: HashSet<String> =
        right.fields().iter().map(|f| f.name().clone()).collect();

    check_join_set_is_valid(&left, &right, &on)?;
    Ok(())
}

/// Checks whether the sets left, right and on compose a valid join.
/// They are valid whenever their intersection equals the set `on`
fn check_join_set_is_valid(
    left: &HashSet<String>,
    right: &HashSet<String>,
    on: &HashSet<String>,
) -> Result<()> {
    if on.len() == 0 {
        return Err(ExecutionError::General(
            "The 'on' clause of a join cannot be empty".to_string(),
        ));
    }

    let on_columns = on.iter().map(|s| s).collect::<HashSet<_>>();
    let common_columns = left.intersection(&right).collect::<HashSet<_>>();
    let missing = on_columns
        .difference(&common_columns)
        .collect::<HashSet<_>>();
    if missing.len() > 0 {
        return Err(ExecutionError::General(format!(
                "The left or right side of the join does not have columns {:?} columns on \"on\": \nLeft: {:?}\nRight: {:?}\nOn: {:?}",
                missing,
                left,
                right,
                on,
            ).to_string()));
    };
    Ok(())
}

/// Creates a schema for a join operation.
/// The fields "on" from the left side are always first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    on: &HashSet<String>,
    how: &JoinHow,
) -> Result<Schema> {
    let fields: Vec<Field> = match how {
        JoinHow::Inner => {
            // inner: all fields are there

            let on_fields = left.fields().iter().filter(|f| on.contains(f.name()));

            let left_fields = left.fields().iter().filter(|f| !on.contains(f.name()));

            let right_fields = right.fields().iter().filter(|f| !on.contains(f.name()));

            // "on" are first by construction, then left, then right
            on_fields
                .chain(left_fields)
                .chain(right_fields)
                .map(|f| f.clone())
                .collect()
        }
    };
    Ok(Schema::new(fields))
}

#[cfg(test)]
mod tests {

    use super::*;

    fn check(left: &[&str], right: &[&str], on: &[&str]) -> Result<()> {
        let left = left.iter().map(|x| x.to_string()).collect::<HashSet<_>>();
        let right = right.iter().map(|x| x.to_string()).collect::<HashSet<_>>();
        let on = on.iter().map(|x| x.to_string()).collect::<HashSet<_>>();

        check_join_set_is_valid(&left, &right, &on)
    }

    #[test]
    fn check_valid() -> Result<()> {
        let left = vec!["a", "b1"];
        let right = vec!["a", "b2"];
        let on = vec!["a"];

        check(&left, &right, &on)?;
        Ok(())
    }

    #[test]
    fn check_not_in_right() {
        let left = vec!["a", "b"];
        let right = vec!["b"];
        let on = vec!["a"];

        assert!(check(&left, &right, &on).is_err());
    }

    #[test]
    fn check_not_in_left() {
        let left = vec!["b"];
        let right = vec!["a"];
        let on = vec!["a"];

        assert!(check(&left, &right, &on).is_err());
    }
}
