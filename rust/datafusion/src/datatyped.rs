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

//! This module contains a public trait to annotate objects that know their data type.
// The pattern in this module follows https://stackoverflow.com/a/28664881/931303

use crate::error::Result;
use arrow::datatypes::{DataType, Schema};

/// Any object that knows how to infer its resulting data type from an underlying schema
pub trait DataTyped: AsDataTyped {
    fn get_type(&self, input_schema: &Schema) -> Result<DataType>;
}

/// Trait that allows DataTyped objects to be upcasted to DataTyped.
pub trait AsDataTyped {
    fn as_datatyped(&self) -> &dyn DataTyped;
}

impl<T: DataTyped> AsDataTyped for T {
    fn as_datatyped(&self) -> &dyn DataTyped {
        self
    }
}
