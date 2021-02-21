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

use super::*;
use crate::datatypes::*;
use array::Array;
use hex::FromHex;
use serde_json::value::Value::{Null as JNull, Object, String as JString};
use serde_json::Value;

/// Trait for comparing arrow array with json array
pub trait JsonEqual {
    /// Checks whether arrow array equals to json array.
    fn equals_json(&self, json: &[&Value]) -> bool;

    /// Checks whether arrow array equals to json array.
    fn equals_json_values(&self, json: &[Value]) -> bool {
        let refs = json.iter().collect::<Vec<&Value>>();

        self.equals_json(&refs)
    }
}

/// Implement array equals for numeric type
impl<T: ArrowPrimitiveType> JsonEqual for PrimitiveArray<T> {
    fn equals_json(&self, json: &[&Value]) -> bool {
        self.len() == json.len()
            && (0..self.len()).all(|i| match json[i] {
                Value::Null => self.is_null(i),
                v => {
                    self.is_valid(i)
                        && Some(v) == self.value(i).into_json_value().as_ref()
                }
            })
    }
}

/// Implement array equals for numeric type
impl JsonEqual for BooleanArray {
    fn equals_json(&self, json: &[&Value]) -> bool {
        self.len() == json.len()
            && (0..self.len()).all(|i| match json[i] {
                Value::Null => self.is_null(i),
                v => {
                    self.is_valid(i)
                        && Some(v) == self.value(i).into_json_value().as_ref()
                }
            })
    }
}

impl<T: ArrowPrimitiveType> PartialEq<Value> for PrimitiveArray<T> {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(array) => self.equals_json_values(&array),
            _ => false,
        }
    }
}

impl<T: ArrowPrimitiveType> PartialEq<PrimitiveArray<T>> for Value {
    fn eq(&self, arrow: &PrimitiveArray<T>) -> bool {
        match self {
            Value::Array(array) => arrow.equals_json_values(&array),
            _ => false,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> JsonEqual for GenericListArray<OffsetSize> {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        (0..self.len()).all(|i| match json[i] {
            Value::Array(v) => self.is_valid(i) && self.value(i).equals_json_values(v),
            Value::Null => self.is_null(i) || self.value_length(i).is_zero(),
            _ => false,
        })
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq<Value> for GenericListArray<OffsetSize> {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(json_array),
            _ => false,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq<GenericListArray<OffsetSize>> for Value {
    fn eq(&self, arrow: &GenericListArray<OffsetSize>) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(json_array),
            _ => false,
        }
    }
}

impl<T: ArrowPrimitiveType> JsonEqual for DictionaryArray<T> {
    fn equals_json(&self, json: &[&Value]) -> bool {
        // todo: this is wrong: we must test the values also
        self.keys().equals_json(json)
    }
}

impl<T: ArrowPrimitiveType> PartialEq<Value> for DictionaryArray<T> {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(json_array),
            _ => false,
        }
    }
}

impl<T: ArrowPrimitiveType> PartialEq<DictionaryArray<T>> for Value {
    fn eq(&self, arrow: &DictionaryArray<T>) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(json_array),
            _ => false,
        }
    }
}

impl JsonEqual for FixedSizeListArray {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        (0..self.len()).all(|i| match json[i] {
            Value::Array(v) => self.is_valid(i) && self.value(i).equals_json_values(v),
            Value::Null => self.is_null(i) || self.value_length() == 0,
            _ => false,
        })
    }
}

impl PartialEq<Value> for FixedSizeListArray {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(json_array),
            _ => false,
        }
    }
}

impl PartialEq<FixedSizeListArray> for Value {
    fn eq(&self, arrow: &FixedSizeListArray) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(json_array),
            _ => false,
        }
    }
}

impl JsonEqual for StructArray {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        let all_object = json.iter().all(|v| matches!(v, Object(_) | JNull));

        if !all_object {
            return false;
        }

        for column_name in self.column_names() {
            let json_values = json
                .iter()
                .map(|obj| obj.get(column_name).unwrap_or(&Value::Null))
                .collect::<Vec<&Value>>();

            if !self
                .column_by_name(column_name)
                .map(|arr| arr.equals_json(&json_values))
                .unwrap_or(false)
            {
                return false;
            }
        }

        true
    }
}

impl PartialEq<Value> for StructArray {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl PartialEq<StructArray> for Value {
    fn eq(&self, arrow: &StructArray) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> JsonEqual for GenericBinaryArray<OffsetSize> {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        (0..self.len()).all(|i| match json[i] {
            JString(s) => {
                // binary data is sometimes hex encoded, this checks if bytes are equal,
                // and if not converting to hex is attempted
                self.is_valid(i)
                    && (s.as_str().as_bytes() == self.value(i)
                        || Vec::from_hex(s.as_str()) == Ok(self.value(i).to_vec()))
            }
            JNull => self.is_null(i),
            _ => false,
        })
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> PartialEq<Value>
    for GenericBinaryArray<OffsetSize>
{
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> PartialEq<GenericBinaryArray<OffsetSize>>
    for Value
{
    fn eq(&self, arrow: &GenericBinaryArray<OffsetSize>) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl<OffsetSize: StringOffsetSizeTrait> JsonEqual for GenericStringArray<OffsetSize> {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        (0..self.len()).all(|i| match json[i] {
            JString(s) => self.is_valid(i) && s.as_str() == self.value(i),
            JNull => self.is_null(i),
            _ => false,
        })
    }
}

impl<OffsetSize: StringOffsetSizeTrait> PartialEq<Value>
    for GenericStringArray<OffsetSize>
{
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl<OffsetSize: StringOffsetSizeTrait> PartialEq<GenericStringArray<OffsetSize>>
    for Value
{
    fn eq(&self, arrow: &GenericStringArray<OffsetSize>) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl JsonEqual for FixedSizeBinaryArray {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        (0..self.len()).all(|i| match json[i] {
            JString(s) => {
                // binary data is sometimes hex encoded, this checks if bytes are equal,
                // and if not converting to hex is attempted
                self.is_valid(i)
                    && (s.as_str().as_bytes() == self.value(i)
                        || Vec::from_hex(s.as_str()) == Ok(self.value(i).to_vec()))
            }
            JNull => self.is_null(i),
            _ => false,
        })
    }
}

impl PartialEq<Value> for FixedSizeBinaryArray {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl PartialEq<FixedSizeBinaryArray> for Value {
    fn eq(&self, arrow: &FixedSizeBinaryArray) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl JsonEqual for DecimalArray {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        (0..self.len()).all(|i| match json[i] {
            JString(s) => {
                self.is_valid(i)
                    && (s
                        .parse::<i128>()
                        .map_or_else(|_| false, |v| v == self.value(i)))
            }
            JNull => self.is_null(i),
            _ => false,
        })
    }
}

impl PartialEq<Value> for DecimalArray {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl PartialEq<DecimalArray> for Value {
    fn eq(&self, arrow: &DecimalArray) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl JsonEqual for UnionArray {
    fn equals_json(&self, _json: &[&Value]) -> bool {
        unimplemented!(
            "Added to allow UnionArray to implement the Array trait: see ARROW-8547"
        )
    }
}

impl JsonEqual for NullArray {
    fn equals_json(&self, json: &[&Value]) -> bool {
        if self.len() != json.len() {
            return false;
        }

        // all JSON values must be nulls
        json.iter().all(|&v| v == &JNull)
    }
}

impl PartialEq<NullArray> for Value {
    fn eq(&self, arrow: &NullArray) -> bool {
        match self {
            Value::Array(json_array) => arrow.equals_json_values(&json_array),
            _ => false,
        }
    }
}

impl PartialEq<Value> for NullArray {
    fn eq(&self, json: &Value) -> bool {
        match json {
            Value::Array(json_array) => self.equals_json_values(&json_array),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Result;
    use std::{convert::TryFrom, sync::Arc};

    fn create_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        builder: &mut ListBuilder<Int32Builder>,
        data: T,
    ) -> Result<ListArray> {
        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref())?;
                builder.append(true)?
            } else {
                builder.append(false)?
            }
        }
        Ok(builder.finish())
    }

    /// Create a fixed size list of 2 value lengths
    fn create_fixed_size_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        builder: &mut FixedSizeListBuilder<Int32Builder>,
        data: T,
    ) -> Result<FixedSizeListArray> {
        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref())?;
                builder.append(true)?
            } else {
                for _ in 0..builder.value_length() {
                    builder.values().append_null()?;
                }
                builder.append(false)?
            }
        }
        Ok(builder.finish())
    }

    #[test]
    fn test_primitive_json_equal() {
        // Test equaled array
        let arrow_array = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                1, null, 2, 3
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequaled array
        let arrow_array = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                1, 1, 2, 3
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test unequal length case
        let arrow_array = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                1, 1
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test not json array type case
        let arrow_array = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let json_array: Value = serde_json::from_str(
            r#"
            {
               "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_list_json_equal() {
        // Test equal case
        let arrow_array = create_list_array(
            &mut ListBuilder::new(Int32Builder::new(10)),
            &[Some(&[1, 2, 3]), None, Some(&[4, 5, 6])],
        )
        .unwrap();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                [1, 2, 3],
                null,
                [4, 5, 6]
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal case
        let arrow_array = create_list_array(
            &mut ListBuilder::new(Int32Builder::new(10)),
            &[Some(&[1, 2, 3]), None, Some(&[4, 5, 6])],
        )
        .unwrap();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                [1, 2, 3],
                [7, 8],
                [4, 5, 6]
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let arrow_array = create_list_array(
            &mut ListBuilder::new(Int32Builder::new(10)),
            &[Some(&[1, 2, 3]), None, Some(&[4, 5, 6])],
        )
        .unwrap();
        let json_array: Value = serde_json::from_str(
            r#"
            {
               "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_fixed_size_list_json_equal() {
        // Test equal case
        let arrow_array = create_fixed_size_list_array(
            &mut FixedSizeListBuilder::new(Int32Builder::new(10), 3),
            &[Some(&[1, 2, 3]), None, Some(&[4, 5, 6])],
        )
        .unwrap();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                [1, 2, 3],
                null,
                [4, 5, 6]
            ]
        "#,
        )
        .unwrap();
        println!("{:?}", arrow_array);
        println!("{:?}", json_array);
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal case
        let arrow_array = create_fixed_size_list_array(
            &mut FixedSizeListBuilder::new(Int32Builder::new(10), 3),
            &[Some(&[1, 2, 3]), None, Some(&[4, 5, 6])],
        )
        .unwrap();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                [1, 2, 3],
                [7, 8, 9],
                [4, 5, 6]
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let arrow_array = create_fixed_size_list_array(
            &mut FixedSizeListBuilder::new(Int32Builder::new(10), 3),
            &[Some(&[1, 2, 3]), None, Some(&[4, 5, 6])],
        )
        .unwrap();
        let json_array: Value = serde_json::from_str(
            r#"
            {
               "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_string_json_equal() {
        // Test the equal case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None, None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "world",
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None, None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "arrow",
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test unequal length case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "arrow",
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None]);
        let json_array: Value = serde_json::from_str(
            r#"
            {
                "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect value type case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                1,
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_binary_json_equal() {
        // Test the equal case
        let mut builder = BinaryBuilder::new(6);
        builder.append_value(b"hello").unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"world").unwrap();
        builder.append_null().unwrap();
        builder.append_null().unwrap();
        let arrow_array = builder.finish();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "world",
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None, None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "arrow",
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test unequal length case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "arrow",
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None]);
        let json_array: Value = serde_json::from_str(
            r#"
            {
                "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect value type case
        let arrow_array =
            StringArray::from(vec![Some("hello"), None, None, Some("world"), None]);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                1,
                null,
                null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_fixed_size_binary_json_equal() {
        // Test the equal case
        let mut builder = FixedSizeBinaryBuilder::new(15, 5);
        builder.append_value(b"hello").unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"world").unwrap();
        let arrow_array: FixedSizeBinaryArray = builder.finish();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                "world"
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal case
        builder.append_value(b"hello").unwrap();
        builder.append_null().unwrap();
        builder.append_value(b"world").unwrap();
        let arrow_array: FixedSizeBinaryArray = builder.finish();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                "arrow"
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test unequal length case
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                null,
                "world"
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let json_array: Value = serde_json::from_str(
            r#"
            {
                "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect value type case
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                1
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_decimal_json_equal() {
        // Test the equal case
        let mut builder = DecimalBuilder::new(30, 23, 6);
        builder.append_value(1_000).unwrap();
        builder.append_null().unwrap();
        builder.append_value(-250).unwrap();
        let arrow_array: DecimalArray = builder.finish();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "1000",
                null,
                "-250"
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal case
        builder.append_value(1_000).unwrap();
        builder.append_null().unwrap();
        builder.append_value(55).unwrap();
        let arrow_array: DecimalArray = builder.finish();
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "1000",
                null,
                "-250"
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test unequal length case
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "1000",
                null,
                null,
                "55"
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let json_array: Value = serde_json::from_str(
            r#"
            {
                "a": 1
            }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect value type case
        let json_array: Value = serde_json::from_str(
            r#"
            [
                "hello",
                null,
                1
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_struct_json_equal() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let arrow_array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();

        let json_array: Value = serde_json::from_str(
            r#"
            [
              {
                "f1": "joe",
                "f2": 1
              },
              {
                "f2": 2
              },
              null,
              {
                "f1": "mark",
                "f2": 4
              },
              {
                "f1": "doe",
                "f2": 5
              }
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequal length case
        let json_array: Value = serde_json::from_str(
            r#"
            [
              {
                "f1": "joe",
                "f2": 1
              },
              {
                "f2": 2
              },
              null,
              {
                "f1": "mark",
                "f2": 4
              }
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test incorrect type case
        let json_array: Value = serde_json::from_str(
            r#"
              {
                "f1": "joe",
                "f2": 1
              }
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));

        // Test not all object case
        let json_array: Value = serde_json::from_str(
            r#"
            [
              {
                "f1": "joe",
                "f2": 1
              },
              2,
              null,
              {
                "f1": "mark",
                "f2": 4
              }
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }

    #[test]
    fn test_null_json_equal() {
        // Test equaled array
        let arrow_array = NullArray::new(4);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                null, null, null, null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.eq(&json_array));
        assert!(json_array.eq(&arrow_array));

        // Test unequaled array
        let arrow_array = NullArray::new(2);
        let json_array: Value = serde_json::from_str(
            r#"
            [
                null, null, null
            ]
        "#,
        )
        .unwrap();
        assert!(arrow_array.ne(&json_array));
        assert!(json_array.ne(&arrow_array));
    }
}
