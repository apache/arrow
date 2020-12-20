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

use std::sync::Arc;

use serde_json::Value;

use super::{Array, BinaryArray, StructArray, StructBuilder};
use crate::datatypes::SchemaRef;
use crate::error::ArrowError;
use crate::json::reader::{infer_json_schema_from_iterator, Decoder};

#[derive(Debug)]
/// Interpret `BinaryArray` values as serialized JSON.
pub struct JSONArray {
    inner: Arc<BinaryArray>,
    schema: Option<SchemaRef>,
}

impl From<Arc<BinaryArray>> for JSONArray {
    fn from(inner: Arc<BinaryArray>) -> JSONArray {
        JSONArray {
            inner,
            schema: None,
        }
    }
}

impl From<BinaryArray> for JSONArray {
    fn from(inner: BinaryArray) -> JSONArray {
        Arc::new(inner).into()
    }
}

impl JSONArray {
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        JSONArray {
            inner: self.inner,
            schema: Some(schema),
        }
    }

    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.as_ref().map(|schema| Arc::clone(schema))
    }
}

impl core::ops::Deref for JSONArray {
    type Target = BinaryArray;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::convert::TryFrom<JSONArray> for StructArray {
    type Error = ArrowError;

    fn try_from(json: JSONArray) -> Result<StructArray, Self::Error> {
        let mut records: Vec<Value> = Vec::with_capacity(json.len());

        // Parse values. Return error on null. Could alternatively interpret as empty object.
        for i in 0..json.len() {
            if json.is_valid(i) {
                let buf = json.value(i);
                let value = serde_json::from_slice(buf)
                    .map_err(|e| ArrowError::JsonError(format!("{:?}", e)))?;
                records.push(value);
            } else {
                return Err(ArrowError::JsonError("Encountered null value.".to_string()));
            }
        }

        let schema = match json.schema() {
            Some(schema) => schema,
            None => {
                // Infer schema from records.
                let schema_records = records.iter().map(|value| Ok(value.clone()));
                infer_json_schema_from_iterator(schema_records)?
            }
        };
        let fields = schema.fields().to_vec();

        // Decode as `StructArray`.
        let decoder = Decoder::new(schema, json.len(), None);
        let mut batch_records = records.into_iter().map(Ok);
        Ok(decoder
            .next_batch(&mut batch_records)?
            .map(|batch| batch.into())
            .unwrap_or_else(|| StructBuilder::from_fields(fields, 0).finish()))
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{
        Array, BinaryArray, BinaryBuilder, Float64Builder, Int64Builder, JSONArray,
        StringBuilder, StructArray,
    };
    use crate::datatypes::{DataType, Field, Schema};
    use std::convert::TryInto;
    use std::sync::Arc;

    fn test_input_binary() -> BinaryArray {
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
        builder.finish()
    }

    fn validation_data() -> StructArray {
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
        fields.into()
    }

    #[test]
    fn test_binary() {
        let binary_array = test_input_binary();
        let test_struct_array = validation_data();

        // Parsing
        let json_array: JSONArray = binary_array.into();
        let struct_array: StructArray = json_array.try_into().unwrap();

        assert_eq!(struct_array, test_struct_array);
    }

    #[test]
    fn test_with_schema() {
        let binary_array = test_input_binary();
        let test_struct_array = validation_data();

        let schema = Arc::new(Schema::new(vec![
            Field::new("string", DataType::Utf8, true),
            Field::new("int", DataType::Int64, true),
            Field::new("float", DataType::Float64, true),
        ]));

        // Parsing with schema
        let inferred_json_array: JSONArray = Arc::new(binary_array).into();
        let json_array = inferred_json_array.with_schema(schema);
        let struct_array: StructArray = json_array.try_into().unwrap();

        assert_eq!(struct_array, test_struct_array);
    }
}
