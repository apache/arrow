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

//! Defines concat kernel for `ArrayRef`
//!
//! Example:
//!
//! ```
//! use arrow::array::{ArrayRef, StringArray};
//! use arrow::compute::concat;
//!
//! let arr = concat(&[
//!     &StringArray::from(vec!["hello", "world"]),
//!     &StringArray::from(vec!["!"]),
//! ]).unwrap();
//! assert_eq!(arr.len(), 3);
//! ```

use std::sync::Arc;

use crate::array::*;
use crate::error::{ArrowError, Result};

/// Concatenate multiple [Array] of the same type into a single [ArrayRef].
pub fn concat(arrays: &[&Array]) -> Result<ArrayRef> {
    if arrays.is_empty() {
        return Err(ArrowError::ComputeError(
            "concat requires input of at least one array".to_string(),
        ));
    }

    if arrays
        .iter()
        .any(|array| array.data_type() != arrays[0].data_type())
    {
        return Err(ArrowError::InvalidArgumentError(
            "It is not possible to concatenate arrays of different data types."
                .to_string(),
        ));
    }

    let lengths = arrays.iter().map(|array| array.len()).collect::<Vec<_>>();
    let capacity = lengths.iter().sum();

    let arrays = arrays
        .iter()
        .map(|a| a.data_ref().as_ref())
        .collect::<Vec<_>>();

    let mut mutable = MutableArrayData::new(arrays, false, capacity);

    for (i, len) in lengths.iter().enumerate() {
        mutable.extend(i, 0, *len)
    }

    Ok(make_array(Arc::new(mutable.freeze())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::*;
    use std::sync::Arc;

    #[test]
    fn test_concat_empty_vec() -> Result<()> {
        let re = concat(&[]);
        assert!(re.is_err());
        Ok(())
    }

    #[test]
    fn test_concat_incompatible_datatypes() -> Result<()> {
        let re = concat(&[
            &PrimitiveArray::<Int64Type>::from(vec![Some(-1), Some(2), None]),
            &StringArray::from(vec![Some("hello"), Some("bar"), Some("world")]),
        ]);
        assert!(re.is_err());
        Ok(())
    }

    #[test]
    fn test_concat_string_arrays() -> Result<()> {
        let arr = concat(&[
            &StringArray::from(vec!["hello", "world"]),
            &StringArray::from(vec!["2", "3", "4"]),
            &StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]),
        ])?;

        let expected_output = Arc::new(StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("2"),
            Some("3"),
            Some("4"),
            Some("foo"),
            Some("bar"),
            None,
            Some("baz"),
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);

        Ok(())
    }

    #[test]
    fn test_concat_primitive_arrays() -> Result<()> {
        let arr = concat(&[
            &PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ]),
            &PrimitiveArray::<Int64Type>::from(vec![
                Some(101),
                Some(102),
                Some(103),
                None,
            ]),
            &PrimitiveArray::<Int64Type>::from(vec![Some(256), Some(512), Some(1024)]),
        ])?;

        let expected_output = Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(-1),
            Some(2),
            None,
            None,
            Some(101),
            Some(102),
            Some(103),
            None,
            Some(256),
            Some(512),
            Some(1024),
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);

        Ok(())
    }

    #[test]
    fn test_concat_boolean_primitive_arrays() -> Result<()> {
        let arr = concat(&[
            &BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                None,
                None,
                Some(false),
            ]),
            &BooleanArray::from(vec![None, Some(false), Some(true), Some(false)]),
        ])?;

        let expected_output = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(false),
        ])) as ArrayRef;

        assert_eq!(&arr, &expected_output);

        Ok(())
    }

    #[test]
    fn test_concat_primitive_list_arrays() -> Result<()> {
        fn populate_list1(
            b: &mut ListBuilder<PrimitiveBuilder<Int64Type>>,
        ) -> Result<()> {
            b.values().append_value(-1)?;
            b.values().append_value(-1)?;
            b.values().append_value(2)?;
            b.values().append_null()?;
            b.values().append_null()?;
            b.append(true)?;
            b.append(true)?;
            b.append(false)?;
            b.values().append_value(10)?;
            b.append(true)?;
            Ok(())
        }

        fn populate_list2(
            b: &mut ListBuilder<PrimitiveBuilder<Int64Type>>,
        ) -> Result<()> {
            b.append(false)?;
            b.values().append_value(100)?;
            b.values().append_null()?;
            b.values().append_value(101)?;
            b.append(true)?;
            b.values().append_value(102)?;
            b.append(true)?;
            Ok(())
        }

        fn populate_list3(
            b: &mut ListBuilder<PrimitiveBuilder<Int64Type>>,
        ) -> Result<()> {
            b.values().append_value(1000)?;
            b.values().append_value(1001)?;
            b.append(true)?;
            Ok(())
        }

        let mut builder_in1 = ListBuilder::new(PrimitiveArray::<Int64Type>::builder(0));
        let mut builder_in2 = ListBuilder::new(PrimitiveArray::<Int64Type>::builder(0));
        let mut builder_in3 = ListBuilder::new(PrimitiveArray::<Int64Type>::builder(0));
        populate_list1(&mut builder_in1)?;
        populate_list2(&mut builder_in2)?;
        populate_list3(&mut builder_in3)?;

        let mut builder_expected =
            ListBuilder::new(PrimitiveArray::<Int64Type>::builder(0));
        populate_list1(&mut builder_expected)?;
        populate_list2(&mut builder_expected)?;
        populate_list3(&mut builder_expected)?;

        let array_result = concat(&[
            &builder_in1.finish(),
            &builder_in2.finish(),
            &builder_in3.finish(),
        ])?;

        let array_expected = Arc::new(builder_expected.finish()) as ArrayRef;

        assert_eq!(&array_result, &array_expected);

        Ok(())
    }
}
