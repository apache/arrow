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

#include "arrow/dataset/dataset.h"

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/test_util.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/stl.h"
#include "arrow/testing/generator.h"
#include "arrow/util/optional.h"


namespace arrow {
namespace dataset {

class TestRadosDataset : public DatasetFixtureMixin {};

TEST_F(TestRadosDataset, GetFragments) {
  constexpr int32_t start_object = 0;
  constexpr int32_t num_objects = 15;

  SetSchema({field("i32", int32()), field("f64", float64())});
  auto dataset = std::make_shared<RadosDataset>(
      schema_, "/path/to/ceph.conf", "testpool");

  FragmentIterator it = dataset.GetFragments();
}

}
}
