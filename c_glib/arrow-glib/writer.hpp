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

#include <arrow-glib/writer.h>

GArrowRecordBatchWriter *
garrow_record_batch_writer_new_raw(
  std::shared_ptr<arrow::ipc::RecordBatchWriter> *arrow_writer);
std::shared_ptr<arrow::ipc::RecordBatchWriter>
garrow_record_batch_writer_get_raw(GArrowRecordBatchWriter *writer);

GArrowRecordBatchStreamWriter *
garrow_record_batch_stream_writer_new_raw(
  std::shared_ptr<arrow::ipc::RecordBatchWriter> *arrow_writer);

GArrowRecordBatchFileWriter *
garrow_record_batch_file_writer_new_raw(
  std::shared_ptr<arrow::ipc::RecordBatchWriter> *arrow_writer);
