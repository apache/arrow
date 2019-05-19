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

use arrow::bitmap::Bitmap;
use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::record_batch::RecordBatch;
use evaluator::{Filter, Projector};
use expression::Expr;

/// Projector implementation using functions in ```gandiva_ffi```
struct GandivaProjector {}

impl GandivaProjector {
    /// schema is record batch's schema, it's used by gandiva to generate code.
    /// exprs is expression to evaluate, for example ```a+b```
    pub fn new(_schema: Schema, _exprs: Vec<Expr>) -> Result<Self> {
        unimplemented!()
    }
}

impl Projector for GandivaProjector {
    fn eval(&self, _record_batch: RecordBatch) -> Result<RecordBatch> {
        unimplemented!()
    }
}

/// Filter implementation using functions in ```gandiva_ffi```
struct GandivaFilter {}

impl GandivaFilter {
    /// schema is record batch's schema, it's used by gandiva to generate code.
    /// exprs is expression to evaluate, for example ```a+b```
    pub fn new(_schema: Schema, _exprs: Vec<Expr>) -> Result<Self> {
        unimplemented!()
    }
}

impl Filter for GandivaFilter {
    fn eval(&self, _record_batch: RecordBatch) -> Result<Bitmap> {
        unimplemented!()
    }
}
