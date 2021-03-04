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

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::scalar::ScalarValue;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("to_array_of_size 100000", |b| {
        let scalar = ScalarValue::Int32(Some(100));

        b.iter(|| assert_eq!(scalar.to_array_of_size(100000).null_count(), 0))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
