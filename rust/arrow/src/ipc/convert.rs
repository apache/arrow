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

use crate::datatypes::DataType;
use crate::datatypes::Field;
use crate::datatypes::Schema;
use crate::ipc;

use flatbuffers::FlatBufferBuilder;

/// Serialize a schema in IPC format
fn schema_to_fb(schema: &Schema) -> FlatBufferBuilder {
    use DataType::*;
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

/// Deserialize a Schema table from IPC format to Schema data type
pub fn fb_to_schema(fb: ipc::Schema) -> Schema {
    let mut fields: Vec<Field> = vec![];
    let c_fields = fb.fields().unwrap();
    let len = c_fields.len();
    for i in 0..len {
        let c_field: ipc::Field = c_fields.get(i);
        let field = Field::new(
            c_field.name().unwrap(),
            get_data_type(c_field),
            c_field.nullable(),
        );
        fields.push(field);
    }
    Schema::new(fields)
}

fn get_fbs_type(dtype: DataType) -> ipc::Type {
    use ipc::Type::*;
    use DataType::*;

    match dtype {
        Boolean => Bool,
        Int8 | Int16 | Int32 | Int64 => Int,
        UInt8 | UInt16 | UInt32 | UInt64 => Int,
        Float16 => unimplemented!("Float16 type not supported in Rust Arrow"),
        Float32 | Float64 => FloatingPoint,
        DataType::Timestamp(_) => ipc::Type::Timestamp,
        Date32(_) | Date64(_) => Date,
        Time32(_) | Time64(_) => Time,
        DataType::Interval(_) => unimplemented!("Interval type not supported"),
        DataType::Utf8 => ipc::Type::Utf8,
        DataType::List(_) => ipc::Type::List,
        Struct(_) => Struct_,
        _ => unimplemented!("Type not supported in Rust Arrow"),
    }
}

fn get_data_type(field: ipc::Field) -> DataType {
    match field.type_type() {
        ipc::Type::Bool => DataType::Boolean,
        ipc::Type::Int => {
            let int = field.type__as_int().unwrap();
            match (int.bitWidth(), int.is_signed()) {
                (8, true) => DataType::Int8,
                (8, false) => DataType::UInt8,
                (16, true) => DataType::Int16,
                (16, false) => DataType::UInt16,
                (32, true) => DataType::Int32,
                (32, false) => DataType::UInt32,
                (64, true) => DataType::Int64,
                (64, false) => DataType::UInt64,
                _ => panic!("Unexpected bitwidth and signed"),
            }
        }
        ipc::Type::Utf8 => DataType::Utf8,
        ipc::Type::FloatingPoint => DataType::Float64,
        t @ _ => unimplemented!("Type {:?} not supported", t),
        // ipc::Type::BINARY => DataType::Utf8,
        // ipc::Type::CATEGORY => unimplemented!("Reading CATEGORY type columns not implemented"),
        // ipc::Type::TIMESTAMP | fbs::Type::DATE | fbs::Type::TIME => {
        //     unimplemented!("Reading date and time fields not implemented")
        // }
    }
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
