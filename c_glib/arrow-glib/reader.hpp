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
#include <arrow/ipc/api.h>
#include <arrow/ipc/feather.h>

#include <arrow-glib/reader.h>

GArrowRecordBatchReader *garrow_record_batch_reader_new_raw(std::shared_ptr<arrow::ipc::RecordBatchReader> *arrow_reader);
std::shared_ptr<arrow::ipc::RecordBatchReader> garrow_record_batch_reader_get_raw(GArrowRecordBatchReader *reader);

GArrowRecordBatchStreamReader *garrow_record_batch_stream_reader_new_raw(std::shared_ptr<arrow::ipc::RecordBatchStreamReader> *arrow_reader);

GArrowRecordBatchFileReader *garrow_record_batch_file_reader_new_raw(std::shared_ptr<arrow::ipc::RecordBatchFileReader> *arrow_reader);
std::shared_ptr<arrow::ipc::RecordBatchFileReader> garrow_record_batch_file_reader_get_raw(GArrowRecordBatchFileReader *reader);

GArrowFeatherFileReader *garrow_feather_file_reader_new_raw(arrow::ipc::feather::TableReader *arrow_reader);
arrow::ipc::feather::TableReader *garrow_feather_file_reader_get_raw(GArrowFeatherFileReader *reader);
