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

#include <arrow/io/compressed.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>

#include <arrow-glib/input-stream.h>

GARROW_EXTERN
GArrowInputStream *
garrow_input_stream_new_raw(std::shared_ptr<arrow::io::InputStream> *arrow_input_stream);

GARROW_EXTERN
std::shared_ptr<arrow::io::InputStream>
garrow_input_stream_get_raw(GArrowInputStream *input_stream);

GARROW_EXTERN
GArrowSeekableInputStream *
garrow_seekable_input_stream_new_raw(
  std::shared_ptr<arrow::io::RandomAccessFile> *arrow_random_access_file);

GARROW_EXTERN
std::shared_ptr<arrow::io::RandomAccessFile>
garrow_seekable_input_stream_get_raw(GArrowSeekableInputStream *input_stream);

GARROW_EXTERN
GArrowBufferInputStream *
garrow_buffer_input_stream_new_raw(
  std::shared_ptr<arrow::io::BufferReader> *arrow_buffer_reader, GArrowBuffer *buffer);

GARROW_EXTERN
std::shared_ptr<arrow::io::BufferReader>
garrow_buffer_input_stream_get_raw(GArrowBufferInputStream *input_stream);

GARROW_EXTERN
GArrowFileInputStream *
garrow_file_input_stream_new_raw(std::shared_ptr<arrow::io::ReadableFile> *arrow_stream);

GARROW_EXTERN
GArrowMemoryMappedInputStream *
garrow_memory_mapped_input_stream_new_raw(
  std::shared_ptr<arrow::io::MemoryMappedFile> *arrow_stream);

GARROW_EXTERN
GArrowCompressedInputStream *
garrow_compressed_input_stream_new_raw(
  std::shared_ptr<arrow::io::CompressedInputStream> *arrow_raw,
  GArrowCodec *codec,
  GArrowInputStream *raw);

GARROW_EXTERN
std::shared_ptr<arrow::io::InputStream>
garrow_compressed_input_stream_get_raw(GArrowCompressedInputStream *stream);
