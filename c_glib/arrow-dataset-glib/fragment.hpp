/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow/dataset/api.h>

#include <arrow-dataset-glib/fragment.h>

std::shared_ptr<arrow::dataset::Fragment>
gadataset_fragment_get_raw(GADatasetFragment *fragment);

GADatasetFragment*
gadataset_fragment_new_raw(
  std::shared_ptr<arrow::dataset::Fragment> *arrow_fragment);

GADatasetInMemoryFragment*
gadataset_in_memory_fragment_new_raw(
  std::shared_ptr<arrow::dataset::InMemoryFragment> *arrow_fragment);
