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

use serde_json::Value;

#[derive(Debug, Clone)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Struct(Vec<Field>),
}

impl DataType {
    pub fn to_json(&self) -> Value {
        match self {
            &DataType::Boolean => json!({"name": "bool"}),
            &DataType::Int8 => json!({"name": "int", "bitWidth": "8", "isSigned": "true"}),
            &DataType::Int16 => json!({"name": "int", "bitWidth": "16", "isSigned": "true"}),
            &DataType::Int32 => json!({"name": "int", "bitWidth": "32", "isSigned": "true"}),
            &DataType::Int64 => json!({"name": "int", "bitWidth": "64", "isSigned": "true"}),
            &DataType::UInt8 => json!({"name": "int", "bitWidth": "8", "isSigned": "false"}),
            &DataType::UInt16 => json!({"name": "int", "bitWidth": "16", "isSigned": "false"}),
            &DataType::UInt32 => json!({"name": "int", "bitWidth": "32", "isSigned": "false"}),
            &DataType::UInt64 => json!({"name": "int", "bitWidth": "64", "isSigned": "false"}),
            &DataType::Float32 => json!({"name": "floatingpoint", "precision": "SINGLE"}),
            &DataType::Float64 => json!({"name": "floatingpoint", "precision": "DOUBLE"}),
            &DataType::Utf8 => json!({"name": "utf8"}),
            &DataType::Struct(ref fields) => {
                let field_json_array =
                    Value::Array(fields.iter().map(|f| f.to_json()).collect::<Vec<Value>>());
                json!({ "fields": field_json_array })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.to_string(),
            data_type: data_type,
            nullable: nullable,
        }
    }

    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "nullable": self.nullable,
            "type": self.data_type.to_json(),
        })

        //        let mut map = Map::new();
        //        map.insert("name".to_string(), Value::String(self.name.clone()));
        //        Value::from(map)
    }

    pub fn to_string(&self) -> String {
        format!("{}: {:?}", self.name, self.data_type)
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Field>,
}

impl Schema {
    /// create an empty schema
    pub fn empty() -> Self {
        Schema { columns: vec![] }
    }

    pub fn new(columns: Vec<Field>) -> Self {
        Schema { columns: columns }
    }

    /// look up a column by name and return a reference to the column along with it's index
    pub fn column(&self, name: &str) -> Option<(usize, &Field)> {
        self.columns
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name == name)
    }

    pub fn to_string(&self) -> String {
        let s: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        s.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_struct_type() {
        let _person = DataType::Struct(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);
    }

    #[test]
    fn struct_field_to_json() {
        let f = Field::new(
            "address",
            DataType::Struct(vec![
                Field::new("street", DataType::Utf8, false),
                Field::new("zip", DataType::UInt16, false),
            ]),
            false,
        );
        assert_eq!(
            "{\"name\":\"address\",\"nullable\":false,\"type\":{\"\
             fields\":[\
             {\"name\":\"street\",\"nullable\":false,\"type\":{\"name\":\"utf8\"}},\
             {\"name\":\"zip\",\"nullable\":false,\"type\":\
             {\"bitWidth\":\"16\",\"isSigned\":\"false\",\"name\":\"int\"}}\
             ]}}",
            f.to_json().to_string()
        );
    }

    #[test]
    fn primitive_field_to_json() {
        let f = Field::new("first_name", DataType::Utf8, false);
        assert_eq!(
            "{\"name\":\"first_name\",\"nullable\":false,\"type\":{\"name\":\"utf8\"}}",
            f.to_json().to_string()
        );
    }
}
