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

#include <arrow/util/parallel.h>
#include "arrow_types.h"

//' Get the capacity of the global thread pool
//'
//' @return the number of worker threads in the thread pool to which
//' Arrow dispatches various CPU-bound tasks. This is an ideal number,
//' not necessarily the exact number of threads at a given point in time.
//'
//' You can change this number using [SetCpuThreadPoolCapacity()].
//'
//' @export
// [[Rcpp::export]]
int GetCpuThreadPoolCapacity() { return arrow::GetCpuThreadPoolCapacity(); }

//' Set the capacity of the global thread pool
//'
//' @param threads the number of worker threads int the thread pool to which
//' Arrow dispatches various CPU-bound tasks.
//'
//' The current number is returned by [GetCpuThreadPoolCapacity()]
//'
//' @export
// [[Rcpp::export]]
void SetCpuThreadPoolCapacity(int threads) {
  STOP_IF_NOT_OK(arrow::SetCpuThreadPoolCapacity(threads));
}
