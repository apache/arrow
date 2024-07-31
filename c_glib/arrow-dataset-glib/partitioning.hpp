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

#include <arrow-dataset-glib/partitioning.h>

arrow::dataset::PartitioningFactoryOptions
gadataset_partitioning_factory_options_get_raw(
  GADatasetPartitioningFactoryOptions *options);

arrow::dataset::KeyValuePartitioningOptions
gadataset_key_value_partitioning_options_get_raw(
  GADatasetKeyValuePartitioningOptions *options);

arrow::dataset::HivePartitioningOptions
gadataset_hive_partitioning_options_get_raw(GADatasetHivePartitioningOptions *options);

GADatasetPartitioning *
gadataset_partitioning_new_raw(
  std::shared_ptr<arrow::dataset::Partitioning> *arrow_partitioning);

std::shared_ptr<arrow::dataset::Partitioning>
gadataset_partitioning_get_raw(GADatasetPartitioning *partitioning);
