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

#include <arrow-dataset-glib/dataset.h>


GADatasetDataset *
gadataset_dataset_new_raw(
  std::shared_ptr<arrow::dataset::Dataset> *arrow_dataset);
GADatasetDataset *
gadataset_dataset_new_raw(
  std::shared_ptr<arrow::dataset::Dataset> *arrow_dataset,
  const gchar *first_property_name,
  ...);
GADatasetDataset *
gadataset_dataset_new_raw_valist(
  std::shared_ptr<arrow::dataset::Dataset> *arrow_dataset,
  const gchar *first_property_name,
  va_list arg);
std::shared_ptr<arrow::dataset::Dataset>
gadataset_dataset_get_raw(GADatasetDataset *dataset);


arrow::dataset::FileSystemDatasetWriteOptions *
gadataset_file_system_dataset_write_options_get_raw(
  GADatasetFileSystemDatasetWriteOptions *options);
