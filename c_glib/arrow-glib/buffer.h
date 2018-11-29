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

#include <arrow-glib/gobject-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_BUFFER (garrow_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBuffer,
                         garrow_buffer,
                         GARROW,
                         BUFFER,
                         GObject)
struct _GArrowBufferClass
{
  GObjectClass parent_class;
};

GArrowBuffer  *garrow_buffer_new          (const guint8 *data,
                                           gint64 size);
GArrowBuffer  *garrow_buffer_new_bytes    (GBytes *data);
gboolean       garrow_buffer_equal        (GArrowBuffer *buffer,
                                           GArrowBuffer *other_buffer);
gboolean       garrow_buffer_equal_n_bytes(GArrowBuffer *buffer,
                                           GArrowBuffer *other_buffer,
                                           gint64 n_bytes);
gboolean       garrow_buffer_is_mutable   (GArrowBuffer *buffer);
gint64         garrow_buffer_get_capacity (GArrowBuffer *buffer);
GBytes        *garrow_buffer_get_data     (GArrowBuffer *buffer);
GBytes        *garrow_buffer_get_mutable_data(GArrowBuffer *buffer);
gint64         garrow_buffer_get_size     (GArrowBuffer *buffer);
GArrowBuffer  *garrow_buffer_get_parent   (GArrowBuffer *buffer);

GArrowBuffer  *garrow_buffer_copy         (GArrowBuffer *buffer,
                                           gint64 start,
                                           gint64 size,
                                           GError **error);
GArrowBuffer  *garrow_buffer_slice        (GArrowBuffer *buffer,
                                           gint64 offset,
                                           gint64 size);


#define GARROW_TYPE_MUTABLE_BUFFER (garrow_mutable_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMutableBuffer,
                         garrow_mutable_buffer,
                         GARROW,
                         MUTABLE_BUFFER,
                         GArrowBuffer)
struct _GArrowMutableBufferClass
{
  GArrowBufferClass parent_class;
};

GArrowMutableBuffer *garrow_mutable_buffer_new  (guint8 *data,
                                                 gint64 size);
GArrowMutableBuffer *garrow_mutable_buffer_new_bytes(GBytes *data);
GArrowMutableBuffer *garrow_mutable_buffer_slice(GArrowMutableBuffer *buffer,
                                                 gint64 offset,
                                                 gint64 size);
gboolean garrow_mutable_buffer_set_data(GArrowMutableBuffer *buffer,
                                        gint64 offset,
                                        const guint8 *data,
                                        gint64 size,
                                        GError **error);


#define GARROW_TYPE_RESIZABLE_BUFFER (garrow_resizable_buffer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowResizableBuffer,
                         garrow_resizable_buffer,
                         GARROW,
                         RESIZABLE_BUFFER,
                         GArrowMutableBuffer)
struct _GArrowResizableBufferClass
{
  GArrowMutableBufferClass parent_class;
};


GArrowResizableBuffer *garrow_resizable_buffer_new(gint64 initial_size,
                                                   GError **error);
gboolean garrow_resizable_buffer_resize(GArrowResizableBuffer *buffer,
                                        gint64 new_size,
                                        GError **error);
gboolean garrow_resizable_buffer_reserve(GArrowResizableBuffer *buffer,
                                         gint64 new_capacity,
                                         GError **error);

G_END_DECLS
