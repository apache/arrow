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

#include <arrow-dataset-glib/scanner.h>

GADScanContext *
gad_scan_context_new_raw(std::shared_ptr<arrow::dataset::ScanContext> *arrow_scan_context);
std::shared_ptr<arrow::dataset::ScanContext>
gad_scan_context_get_raw(GADScanContext *scan_context);

GADScanOptions *
gad_scan_options_new_raw(std::shared_ptr<arrow::dataset::ScanOptions> *arrow_scan_options);
std::shared_ptr<arrow::dataset::ScanOptions>
gad_scan_options_get_raw(GADScanOptions *scan_options);

GADInMemoryScanTask *
gad_in_memory_scan_task_new_raw(std::shared_ptr<arrow::dataset::InMemoryScanTask> *arrow_in_memory_scan_task,
                                GADScanOptions *scan_options,
                                GADScanContext *scan_context);
