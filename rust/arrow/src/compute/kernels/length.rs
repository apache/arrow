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

//! Defines kernel for length of a string array

use crate::array::*;
use crate::{
    datatypes::DataType,
    error::{ArrowError, Result},
};
use std::sync::Arc;

/// Returns an array of UInt32 denoting the number of characters of the array.
///
/// * This only accepts StringArray
/// * Lenght of null is null.
pub fn length(array: &Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            // TODO: is it possible to compute the length directly from the data instead, of value by value?
            let b = array.as_any().downcast_ref::<StringArray>().unwrap();
            let mut builder = UInt32Builder::new(b.len());
            for i in 0..b.len() {
                if b.is_null(i) {
                    builder.append_null()?
                } else {
                    builder.append_value(b.value(i).len() as u32)?
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(ArrowError::ComputeError(format!(
            "length not supported for {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length() -> Result<()> {
        let a = StringArray::from(vec!["hello", " ", "world", "!"]);
        let b = length(&a)?;
        let c = b.as_ref().as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(4, c.len());
        assert_eq!(5, c.value(0));
        assert_eq!(1, c.value(1));
        assert_eq!(5, c.value(2));
        assert_eq!(1, c.value(3));
        Ok(())
    }

    #[test]
    fn test_null() -> Result<()> {
        let mut builder: StringBuilder = StringBuilder::new(4);
        builder.append_value("one")?;
        builder.append_null()?;
        builder.append_value("three")?;
        builder.append_value("four")?;
        let array = builder.finish();

        let a = length(&array)?;
        assert_eq!(a.len(), array.len());

        let mut expected = UInt32Builder::new(4);
        expected.append_value(3)?;
        expected.append_null()?;
        expected.append_value(5)?;
        expected.append_value(4)?;
        let expected = expected.finish();

        assert_eq!(expected.data(), a.data());
        Ok(())
    }

    #[test]
    fn test_wrong_type() -> Result<()> {
        let mut builder = UInt64Builder::new(1);
        builder.append_value(1)?;
        let array = builder.finish();

        assert!(length(&array).is_err());
        Ok(())
    }
}
