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

#include <arrow-glib/decoder.h>

GARROW_EXTERN
std::shared_ptr<arrow::ipc::Listener>
garrow_stream_listener_get_raw(GArrowStreamListener *listener);

GARROW_EXTERN
GArrowStreamDecoder *
garrow_stream_decoder_new_raw(std::shared_ptr<arrow::ipc::StreamDecoder> *arrow_decoder,
                              GArrowStreamListener *listener);

GARROW_EXTERN
std::shared_ptr<arrow::ipc::StreamDecoder>
garrow_stream_decoder_get_raw(GArrowStreamDecoder *decoder);
