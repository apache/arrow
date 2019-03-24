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

use crate::datatypes::{DataType, Field, Schema};
use crate::ipc;

use flatbuffers::FlatBufferBuilder;

fn schema_to_fb(schema: &Schema) -> FlatBufferBuilder {
    let mut fbb = FlatBufferBuilder::new();

    let mut fields = vec![];
    for field in schema.fields() {
        let fb_field_name = fbb.create_string(field.name().as_str());
        let mut field_builder = ipc::FieldBuilder::new(&mut fbb);
        field_builder.add_name(fb_field_name);
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

    #[test]
    fn convert_schema() {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt32, false)]);

        let ipc = schema_to_fb(&schema);
        assert_eq!(52, ipc.finished_data().len());
    }
}
