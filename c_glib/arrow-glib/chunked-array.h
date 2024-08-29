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

#include <arrow-glib/array.h>
#include <arrow-glib/chunked-array-definition.h>

G_BEGIN_DECLS

GArrowChunkedArray *
garrow_chunked_array_new(GList *chunks,
                         GError **error);
GARROW_AVAILABLE_IN_11_0
GArrowChunkedArray *
garrow_chunked_array_new_empty(GArrowDataType *data_type,
                               GError **error);

gboolean garrow_chunked_array_equal(GArrowChunkedArray *chunked_array,
                                    GArrowChunkedArray *other_chunked_array);

GArrowDataType *
garrow_chunked_array_get_value_data_type(GArrowChunkedArray *chunked_array);
GArrowType
garrow_chunked_array_get_value_type(GArrowChunkedArray *chunked_array);

GARROW_DEPRECATED_IN_0_15_FOR(garrow_chunked_array_get_n_rows)
guint64 garrow_chunked_array_get_length (GArrowChunkedArray *chunked_array);
GARROW_AVAILABLE_IN_0_15
guint64 garrow_chunked_array_get_n_rows (GArrowChunkedArray *chunked_array);
guint64 garrow_chunked_array_get_n_nulls(GArrowChunkedArray *chunked_array);
guint   garrow_chunked_array_get_n_chunks (GArrowChunkedArray *chunked_array);

GArrowArray *garrow_chunked_array_get_chunk(GArrowChunkedArray *chunked_array,
                                            guint i);
GList *garrow_chunked_array_get_chunks(GArrowChunkedArray *chunked_array);
GArrowChunkedArray *garrow_chunked_array_slice(GArrowChunkedArray *chunked_array,
                                               guint64 offset,
                                               guint64 length);
gchar *garrow_chunked_array_to_string(GArrowChunkedArray *chunked_array,
                                      GError **error);
GARROW_AVAILABLE_IN_4_0
GArrowArray *garrow_chunked_array_combine(GArrowChunkedArray *chunked_array,
                                          GError **error);

G_END_DECLS
