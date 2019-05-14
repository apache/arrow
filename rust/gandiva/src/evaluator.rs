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
use arrow::error::Result;
use arrow::record_batch::RecordBatch;

/// Evaluate a record batch against its expressions.
pub trait Projector {
    fn eval(&self, record_batch: RecordBatch) -> Result<RecordBatch>;
}

/// Filter a record batch against its condition and return a bitmap.
pub trait Filter {
    fn eval(&self, record_batch: RecordBatch) -> Result<Bitmap>;
}
