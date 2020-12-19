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

use super::{Array, BinaryArray, StructArray};
use crate::error::ArrowError;
use crate::json::reader::{infer_json_schema_from_iterator, Decoder};
use serde_json::Value;

#[derive(Debug)]
/// Interpret `BinaryArray` values as serialized JSON.
pub struct JSONArray(BinaryArray);

impl From<BinaryArray> for JSONArray {
    fn from(bin: BinaryArray) -> JSONArray {
        JSONArray(bin)
    }
}

impl core::ops::Deref for JSONArray {
    type Target = BinaryArray;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::convert::TryFrom<JSONArray> for StructArray {
    type Error = ArrowError;

    fn try_from(json: JSONArray) -> Result<StructArray, Self::Error> {
        // Parse Records
        let mut records: Vec<Value> = Vec::with_capacity(json.len());
        for i in 0..json.len() {
            let buf = json.0.value(i);
            // TODO: Handle nulls?
            let value = serde_json::from_slice(buf)
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
            records.push(value);
        }

        // Infer Schema
        // TODO: This should not require a `clone()`.
        let schema_records = records.clone().into_iter().map(|record| Ok(record));
        let inferred_schema = infer_json_schema_from_iterator(schema_records)?;

        // Construct StructArray
        let decoder = Decoder::new(inferred_schema, json.len(), None);
        // TODO: Why does this return an Option?
        // TODO: Can this handle nulls?
        let mut batch_records = records.into_iter().map(|record| Ok(record));
        let batch = decoder.next_batch(&mut batch_records)?.unwrap();
        Ok(batch.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{
        Array, BinaryBuilder, Float64Builder, Int64Builder, JSONArray, StringBuilder,
        StructArray,
    };
    use crate::datatypes::{DataType, Field};
    use std::convert::TryInto;
    use std::sync::Arc;

    #[test]
    fn test_basic_json() {
        // Create test input
        let mut builder = BinaryBuilder::new(3);
        builder
            .append_value(br#"{"string":"foo","int":0,"float":0.0}"#)
            .unwrap();
        builder
            .append_value(br#"{"string":"bar","int":1,"float":1.0}"#)
            .unwrap();
        builder
            .append_value(br#"{"string":"baz","int":2,"float":2.0}"#)
            .unwrap();
        let binary_array = builder.finish();

        // Create validation array
        let mut test_string_builder = StringBuilder::new(3);
        test_string_builder.append_value("foo").unwrap();
        test_string_builder.append_value("bar").unwrap();
        test_string_builder.append_value("baz").unwrap();
        let test_string_array = test_string_builder.finish();

        let mut test_int_builder = Int64Builder::new(3);
        test_int_builder.append_value(0).unwrap();
        test_int_builder.append_value(1).unwrap();
        test_int_builder.append_value(2).unwrap();
        let test_int_array = test_int_builder.finish();

        let mut test_float_builder = Float64Builder::new(3);
        test_float_builder.append_value(0.0).unwrap();
        test_float_builder.append_value(1.0).unwrap();
        test_float_builder.append_value(2.0).unwrap();
        let test_float_array = test_float_builder.finish();

        let fields: Vec<(Field, Arc<dyn Array + 'static>)> = vec![
            (
                Field::new("string", DataType::Utf8, true),
                Arc::new(test_string_array),
            ),
            (
                Field::new("int", DataType::Int64, true),
                Arc::new(test_int_array),
            ),
            (
                Field::new("float", DataType::Float64, true),
                Arc::new(test_float_array),
            ),
        ];
        let test_struct_array: StructArray = fields.into();

        // Parsing
        let json_array: JSONArray = binary_array.into();
        let struct_array: StructArray = json_array.try_into().unwrap();

        // Validate parsing
        assert_eq!(struct_array, test_struct_array);
    }
}
