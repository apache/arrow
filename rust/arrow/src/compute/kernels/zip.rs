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

use crate::array::*;
use crate::compute::SlicesIterator;
use crate::error::{ArrowError, Result};

/// Zip two arrays by some boolean mask. Where the mask evaluates `true` values of `truthy`
/// are taken, where the mask evaluates `false` values of `falsy` are taken.
///
/// # Arguments
/// * `mask` - Boolean values used to determine from which array to take the values.
/// * `truthy` - Values of this array are taken if mask evaluates `true`
/// * `falsy` - Values of this array are taken if mask evaluates `false`
pub fn zip(
    mask: &BooleanArray,
    truthy: &dyn Array,
    falsy: &dyn Array,
) -> Result<ArrayRef> {
    if truthy.data_type() != falsy.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "arguments need to have the same data type".into(),
        ));
    }
    if truthy.len() != falsy.len() || falsy.len() != mask.len() {
        return Err(ArrowError::InvalidArgumentError(
            "all arrays should have the same length".into(),
        ));
    }
    let falsy = falsy.data();
    let truthy = truthy.data();

    let mut mutable = MutableArrayData::new(vec![&*truthy, &*falsy], false, truthy.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;

    SlicesIterator::new(mask).for_each(|(start, end)| {
        // the gap needs to be filled with falsy values
        if start > filled {
            mutable.extend(1, filled, start);
        }
        // fill with truthy values
        mutable.extend(0, start, end);
        filled = end;
    });
    // the remaining part is falsy
    if filled < truthy.len() {
        mutable.extend(1, filled, truthy.len());
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zip_kernel() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(3), Some(6), Some(7), Some(3)]);
        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &a, &b).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(6), Some(7), Some(1)]);
        assert_eq!(actual, &expected);
    }
}
