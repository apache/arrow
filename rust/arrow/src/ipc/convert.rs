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

//! Utilities for converting between IPC types and native Arrow types

use crate::datatypes::DataType::*;
use crate::datatypes::Schema;
use crate::ipc;

use flatbuffers::FlatBufferBuilder;

/// Serialize a schema in IPC format
fn schema_to_fb(schema: &Schema) -> FlatBufferBuilder {
    let mut fbb = FlatBufferBuilder::new();

    let mut fields = vec![];
    for field in schema.fields() {
        let fb_field_name = fbb.create_string(field.name().as_str());
        let mut field_builder = ipc::FieldBuilder::new(&mut fbb);
        field_builder.add_name(fb_field_name);
        let ipc_type = match field.data_type() {
            Boolean => ipc::Type::Bool,
            UInt8 | UInt16 | UInt32 | UInt64 => ipc::Type::Int,
            Int8 | Int16 | Int32 | Int64 => ipc::Type::Int,
            Float32 | Float64 => ipc::Type::FloatingPoint,
            Utf8 => ipc::Type::Utf8,
            Date32(_) | Date64(_) => ipc::Type::Date,
            Time32(_) | Time64(_) => ipc::Type::Time,
            Timestamp(_) => ipc::Type::Timestamp,
            _ => ipc::Type::NONE,
        };
        field_builder.add_type_type(ipc_type);
        field_builder.add_nullable(field.is_nullable());
        fields.push(field_builder.finish());
    }

    let fb_field_list = fbb.create_vector(&fields);

    let root = {
        let mut builder = ipc::SchemaBuilder::new(&mut fbb);
        builder.add_fields(fb_field_list);
        builder.finish()
    };

    fbb.finish(root, None);

    fbb
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::{DataType, Field, Schema};

    #[test]
    fn convert_schema() {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let ipc = schema_to_fb(&schema);
        assert_eq!(60, ipc.finished_data().len());
    }
}
