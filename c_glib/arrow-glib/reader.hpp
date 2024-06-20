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

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/feather.h>
#include <arrow/json/api.h>

#include <arrow-glib/reader.h>

GARROW_EXTERN
GArrowRecordBatchReader *
garrow_record_batch_reader_new_raw(
  std::shared_ptr<arrow::ipc::RecordBatchReader> *arrow_reader, GList *sources);

GARROW_EXTERN
std::shared_ptr<arrow::ipc::RecordBatchReader>
garrow_record_batch_reader_get_raw(GArrowRecordBatchReader *reader);

GARROW_EXTERN
GArrowTableBatchReader *
garrow_table_batch_reader_new_raw(std::shared_ptr<arrow::TableBatchReader> *arrow_reader,
                                  GArrowTable *table);

GARROW_EXTERN
std::shared_ptr<arrow::TableBatchReader>
garrow_table_batch_reader_get_raw(GArrowTableBatchReader *reader);

GARROW_EXTERN
GArrowRecordBatchStreamReader *
garrow_record_batch_stream_reader_new_raw(
  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> *arrow_reader);

GARROW_EXTERN
GArrowRecordBatchFileReader *
garrow_record_batch_file_reader_new_raw(
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> *arrow_reader);

GARROW_EXTERN
std::shared_ptr<arrow::ipc::RecordBatchFileReader>
garrow_record_batch_file_reader_get_raw(GArrowRecordBatchFileReader *reader);

GARROW_EXTERN
GArrowFeatherFileReader *
garrow_feather_file_reader_new_raw(
  std::shared_ptr<arrow::ipc::feather::Reader> *arrow_reader);

GARROW_EXTERN
std::shared_ptr<arrow::ipc::feather::Reader>
garrow_feather_file_reader_get_raw(GArrowFeatherFileReader *reader);

GARROW_EXTERN
GArrowCSVReader *
garrow_csv_reader_new_raw(std::shared_ptr<arrow::csv::TableReader> *arrow_reader,
                          GArrowInputStream *input);

GARROW_EXTERN
std::shared_ptr<arrow::csv::TableReader>
garrow_csv_reader_get_raw(GArrowCSVReader *reader);

GARROW_EXTERN
GArrowJSONReader *
garrow_json_reader_new_raw(std::shared_ptr<arrow::json::TableReader> *arrow_reader,
                           GArrowInputStream *input);

GARROW_EXTERN
std::shared_ptr<arrow::json::TableReader>
garrow_json_reader_get_raw(GArrowJSONReader *reader);
