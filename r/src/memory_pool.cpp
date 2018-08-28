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

<<<<<<< HEAD:r/src/hello.cpp
#include "r_arrow_types.h"
=======
#include "arrow_types.h"
>>>>>>> Initial work for type metadata, with tests.:r/src/memory_pool.cpp

using namespace Rcpp;

// [[Rcpp::export]]
static_ptr<arrow::MemoryPool> MemoryPool_default(){
  return arrow::default_memory_pool();
}

// [[Rcpp::export]]
int MemoryPool_bytes_allocated(static_ptr<arrow::MemoryPool> pool){
  return pool->bytes_allocated();
}

// [[Rcpp::export]]
int MemoryPool_max_memory(static_ptr<arrow::MemoryPool> pool){
  return pool->max_memory();
}
