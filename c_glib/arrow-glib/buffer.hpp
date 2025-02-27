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

#include <arrow-glib/buffer.h>

GARROW_EXTERN
GArrowBuffer *
garrow_buffer_new_raw(std::shared_ptr<arrow::Buffer> *arrow_buffer);

GARROW_EXTERN
GArrowBuffer *
garrow_buffer_new_raw_bytes(std::shared_ptr<arrow::Buffer> *arrow_buffer, GBytes *data);

GARROW_EXTERN
GArrowBuffer *
garrow_buffer_new_raw_parent(std::shared_ptr<arrow::Buffer> *arrow_buffer,
                             GArrowBuffer *parent);

GARROW_EXTERN
std::shared_ptr<arrow::Buffer>
garrow_buffer_get_raw(GArrowBuffer *buffer);

GARROW_EXTERN
GArrowMutableBuffer *
garrow_mutable_buffer_new_raw(std::shared_ptr<arrow::MutableBuffer> *arrow_buffer);

GARROW_EXTERN
GArrowMutableBuffer *
garrow_mutable_buffer_new_raw_bytes(std::shared_ptr<arrow::MutableBuffer> *arrow_buffer,
                                    GBytes *data);

GARROW_EXTERN
GArrowResizableBuffer *
garrow_resizable_buffer_new_raw(std::shared_ptr<arrow::ResizableBuffer> *arrow_buffer);
