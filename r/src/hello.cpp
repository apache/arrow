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

#include <Rcpp.h>
#include "arrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp;
using namespace arrow;

// [[Rcpp::export]]
IntegerVector bla() {
  arrow::NumericBuilder<Int32Type> builder;

  builder.Append(1);
  builder.Append(2);
  builder.AppendNull();
  builder.Append(4);
  builder.Append(5);
  builder.Append(6);
  builder.Append(7);

  std::shared_ptr<Array> array;
  builder.Finish(&array);

  // Cast the Array to its actual type to access its data
  std::shared_ptr<Int32Array> int32_array = std::static_pointer_cast<Int32Array>(array);

  // Get the pointer to the actual data
  const int32_t* data = int32_array->raw_values();

  // Get the pointer to the null bitmap.
  const uint8_t* null_bitmap = int32_array->null_bitmap_data();

  int n = array->length();
  IntegerVector out = no_init(array->length());

  for (int i = 0; i < n; i++) {
    out[i] = BitUtil::BitNotSet(null_bitmap, i) ? NA_INTEGER : data[i];
  }

  return out;
}
