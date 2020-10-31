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

//! Kernel to compute the indices necessary to sort-merge two arrays

use std::cmp::Ordering;

use crate::array::{build_compare, ArrayRef};
use crate::{
    compute::SortOptions,
    error::{ArrowError, Result},
};

/// Given two sets of _ordered_ arrays, returns a bool vector denoting which of the items of the lhs and rhs are to pick from so that
/// if we were to sort-merge the lhs and rhs arrays together, they would all be sorted according to the `options`.
/// # Example
/// ```
/// use std::sync::Arc;
/// use arrow::array::UInt32Array;
/// use arrow::compute::SortOptions;
/// use arrow::compute::merge_indices;
/// # fn main() -> arrow::error::Result<()> {
/// let a = Arc::new(UInt32Array::from(vec![None, Some(1), Some(3)]));
/// let b = Arc::new(UInt32Array::from(vec![None, Some(2), Some(4), Some(5)]));
/// let c = merge_indices(&[a], &[b], &[SortOptions::default()])?;
/// // [0] false: when equal (None = None), rhs is picked
/// // [1] true: None < 2
/// // [2] true: 1 < 2
/// // [3] false: 2 < 3
/// // [4] true: 3 < 4
/// // [5,6] false: lhs has finished => pick rhs
/// assert_eq!(c, vec![false, true, true, false, true, false, false]);
/// // I.e. taking `[rhs[0], lhs[0], lhs[1], rhs[1], lhs[2], rhs[2], rhs[3]]`
/// // leads to an ordered array.
/// # Ok(())
/// # }
/// ```
/// # Errors
/// This function errors when:
/// * `lhs.len() != rhs.len()`
/// * `lhs.len() == 0`
/// * `lhs.len() != options.len()`
/// * Arrays on `lhs` and `rhs` have no order relationship
pub fn merge_indices(
    lhs: &[ArrayRef],
    rhs: &[ArrayRef],
    options: &[SortOptions],
) -> Result<Vec<bool>> {
    if lhs.len() != rhs.len() {
        return Err(ArrowError::InvalidArgumentError(format!("Merge requires lhs and rhs to have the same number of arrays. lhs has {}, rhs has {}.", lhs.len(), rhs.len())));
    };
    if lhs.len() == 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Merge requires lhs to have at least 1 entry."
        )));
    };
    if lhs.len() != options.len() {
        return Err(ArrowError::InvalidArgumentError(format!("Merge requires the number of sort options to equal number of columns. lhs has {} entries, options has {} entries", lhs.len(), options.len())));
    };

    // prepare the comparison function between lhs and rhs arrays
    let cmp = lhs
        .iter()
        .zip(rhs.iter())
        .map(|(l, r)| build_compare(l.as_ref(), r.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    // prepare a comparison function taking into account nulls and sort options
    let cmp = |left, right| {
        for c in 0..lhs.len() {
            let descending = options[c].descending;
            let null_first = options[c].nulls_first;
            let mut result = match (lhs[c].is_valid(left), rhs[c].is_valid(right)) {
                (true, true) => (cmp[c])(left, right),
                (false, true) => {
                    if null_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (true, false) => {
                    if null_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => Ordering::Equal,
            };
            if descending {
                result = result.reverse();
            };
            if result != Ordering::Equal {
                // we found a relevant comparison => short-circuit and return it
                return result;
            }
        }
        Ordering::Equal
    };

    // the actual merge-sort code is from this point onwards
    let mut left = 0; // Head of left pile.
    let mut right = 0; // Head of right pile.
    let max_left = lhs[0].len();
    let max_right = rhs[0].len();

    let mut result = Vec::with_capacity(lhs.len() + rhs.len());
    while left < max_left || right < max_right {
        let order = match (left >= max_left, right >= max_right) {
            (true, true) => break,
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => (cmp)(left, right),
        };
        let value = if order == Ordering::Less {
            left += 1;
            true
        } else {
            right += 1;
            false
        };
        result.push(value)
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::UInt32Array;
    use crate::error::Result;

    #[test]
    fn test_indices_other() -> Result<()> {
        let a = Arc::new(UInt32Array::from(vec![None, Some(1), Some(2), Some(4)]));
        let b = Arc::new(UInt32Array::from(vec![None, Some(3)]));
        let c = merge_indices(&[a], &[b], &[SortOptions::default()])?;

        // [0] false: when equal (None = None), rhs is picked
        // [1] true: None < 3
        // [2] true: 1 < 3
        // [3] true: 2 < 3
        // [4] false: 3 < 4
        // [5] true: rhs has finished => pick lhs
        assert_eq!(c, vec![false, true, true, true, false, true]);
        Ok(())
    }

    #[test]
    fn test_indices_many() -> Result<()> {
        let a1 = Arc::new(UInt32Array::from(vec![None, Some(1), Some(3)]));
        let b1 = Arc::new(UInt32Array::from(vec![None, Some(2), Some(3), Some(5)]));
        let option1 = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let a2 = Arc::new(UInt32Array::from(vec![Some(2), Some(3), Some(5)]));
        let b2 = Arc::new(UInt32Array::from(vec![Some(1), Some(4), Some(6), Some(6)]));
        let option2 = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let c = merge_indices(&[a1, a2], &[b1, b2], &[option1, option2])?;

        // [0] true: (N = N, 2 > 1)
        // [1] false: (1 < N, irrelevant)
        // [2] true: (1 < 2, irrelevant)
        // [3] false: (2 < 3, irrelevant)
        // [4] false: (3 = 3, 5 < 6)
        // [5] true: (3 < 5, irrelevant)
        // [6] false: lhs finished
        assert_eq!(c, vec![true, false, true, false, false, true, false]);
        Ok(())
    }
}
