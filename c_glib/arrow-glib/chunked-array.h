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

G_BEGIN_DECLS

#define GARROW_TYPE_CHUNKED_ARRAY               \
  (garrow_chunked_array_get_type())
#define GARROW_CHUNKED_ARRAY(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_CHUNKED_ARRAY,        \
                              GArrowChunkedArray))
#define GARROW_CHUNKED_ARRAY_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_CHUNKED_ARRAY,   \
                           GArrowChunkedArrayClass))
#define GARROW_IS_CHUNKED_ARRAY(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_CHUNKED_ARRAY))
#define GARROW_IS_CHUNKED_ARRAY_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_CHUNKED_ARRAY))
#define GARROW_CHUNKED_ARRAY_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_CHUNKED_ARRAY, \
                             GArrowChunkedArrayClass))

typedef struct _GArrowChunkedArray         GArrowChunkedArray;
typedef struct _GArrowChunkedArrayClass    GArrowChunkedArrayClass;

/**
 * GArrowChunkedArray:
 *
 * It wraps `arrow::ChunkedArray`.
 */
struct _GArrowChunkedArray
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowChunkedArrayClass
{
  GObjectClass parent_class;
};

GType garrow_chunked_array_get_type(void) G_GNUC_CONST;

GArrowChunkedArray *garrow_chunked_array_new(GList *chunks);

guint64 garrow_chunked_array_get_length (GArrowChunkedArray *chunked_array);
guint64 garrow_chunked_array_get_n_nulls(GArrowChunkedArray *chunked_array);
guint   garrow_chunked_array_get_n_chunks (GArrowChunkedArray *chunked_array);

GArrowArray *garrow_chunked_array_get_chunk(GArrowChunkedArray *chunked_array,
                                            guint i);
GList *garrow_chunked_array_get_chunks(GArrowChunkedArray *chunked_array);

G_END_DECLS
