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

#include <arrow-dataset-glib/file-format.h>

GADatasetFileWriteOptions *
gadataset_file_write_options_new_raw(
  std::shared_ptr<arrow::dataset::FileWriteOptions> *arrow_options);
std::shared_ptr<arrow::dataset::FileWriteOptions>
gadataset_file_write_options_get_raw(GADatasetFileWriteOptions *options);


GADatasetFileWriter *
gadataset_file_writer_new_raw(
  std::shared_ptr<arrow::dataset::FileWriter> *arrow_writer);
std::shared_ptr<arrow::dataset::FileWriter>
gadataset_file_writer_get_raw(GADatasetFileWriter *writer);


GADatasetFileFormat *
gadataset_file_format_new_raw(
  std::shared_ptr<arrow::dataset::FileFormat> *arrow_format);
std::shared_ptr<arrow::dataset::FileFormat>
gadataset_file_format_get_raw(GADatasetFileFormat *format);
