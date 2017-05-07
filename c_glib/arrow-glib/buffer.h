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

#include <glib-object.h>

G_BEGIN_DECLS

#define GARROW_TYPE_BUFFER \
  (garrow_buffer_get_type())
#define GARROW_BUFFER(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj), GARROW_TYPE_BUFFER, GArrowBuffer))
#define GARROW_BUFFER_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass), GARROW_TYPE_BUFFER, GArrowBufferClass))
#define GARROW_IS_BUFFER(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_BUFFER))
#define GARROW_IS_BUFFER_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_BUFFER))
#define GARROW_BUFFER_GET_CLASS(obj) \
  (G_TYPE_INSTANCE_GET_CLASS((obj), GARROW_TYPE_BUFFER, GArrowBufferClass))

typedef struct _GArrowBuffer         GArrowBuffer;
typedef struct _GArrowBufferClass    GArrowBufferClass;

/**
 * GArrowBuffer:
 *
 * It wraps `arrow::Buffer`.
 */
struct _GArrowBuffer
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowBufferClass
{
  GObjectClass parent_class;
};

GType          garrow_buffer_get_type     (void) G_GNUC_CONST;

GArrowBuffer  *garrow_buffer_new          (const guint8 *data,
                                           gint64 size);
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


#define GARROW_TYPE_MUTABLE_BUFFER              \
  (garrow_mutable_buffer_get_type())
#define GARROW_MUTABLE_BUFFER(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_MUTABLE_BUFFER,       \
                              GArrowMutableBuffer))
#define GARROW_MUTABLE_BUFFER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_MUTABLE_BUFFER,  \
                           GArrowMutableBufferClass))
#define GARROW_IS_MUTABLE_BUFFER(obj)                                   \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_MUTABLE_BUFFER))
#define GARROW_IS_MUTABLE_BUFFER_CLASS(klass)                           \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_MUTABLE_BUFFER))
#define GARROW_MUTABLE_BUFFER_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_MUTABLE_BUFFER,        \
                             GArrowMutableBufferClass))

typedef struct _GArrowMutableBuffer         GArrowMutableBuffer;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowMutableBufferClass    GArrowMutableBufferClass;
#endif

/**
 * GArrowMutableBuffer:
 *
 * It wraps `arrow::MutableBuffer`.
 */
struct _GArrowMutableBuffer
{
  /*< private >*/
  GArrowBuffer parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowMutableBufferClass
{
  GArrowBufferClass parent_class;
};
#endif

GType garrow_mutable_buffer_get_type(void) G_GNUC_CONST;

GArrowMutableBuffer *garrow_mutable_buffer_new  (guint8 *data,
                                                 gint64 size);
GArrowMutableBuffer *garrow_mutable_buffer_slice(GArrowMutableBuffer *buffer,
                                                 gint64 offset,
                                                 gint64 size);


#define GARROW_TYPE_RESIZABLE_BUFFER            \
  (garrow_resizable_buffer_get_type())
#define GARROW_RESIZABLE_BUFFER(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_RESIZABLE_BUFFER,     \
                              GArrowResizableBuffer))
#define GARROW_RESIZABLE_BUFFER_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_RESIZABLE_BUFFER,        \
                           GArrowResizableBufferClass))
#define GARROW_IS_RESIZABLE_BUFFER(obj)                                 \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_RESIZABLE_BUFFER))
#define GARROW_IS_RESIZABLE_BUFFER_CLASS(klass)                         \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_RESIZABLE_BUFFER))
#define GARROW_RESIZABLE_BUFFER_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_RESIZABLE_BUFFER,      \
                             GArrowResizableBufferClass))

typedef struct _GArrowResizableBuffer         GArrowResizableBuffer;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowResizableBufferClass    GArrowResizableBufferClass;
#endif

/**
 * GArrowResizableBuffer:
 *
 * It wraps `arrow::ResizableBuffer`.
 */
struct _GArrowResizableBuffer
{
  /*< private >*/
  GArrowMutableBuffer parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowResizableBufferClass
{
  GArrowMutableBufferClass parent_class;
};
#endif

GType garrow_resizable_buffer_get_type(void) G_GNUC_CONST;

gboolean garrow_resizable_buffer_resize(GArrowResizableBuffer *buffer,
                                        gint64 new_size,
                                        GError **error);
gboolean garrow_resizable_buffer_reserve(GArrowResizableBuffer *buffer,
                                         gint64 new_capacity,
                                         GError **error);


#define GARROW_TYPE_POOL_BUFFER                 \
  (garrow_pool_buffer_get_type())
#define GARROW_POOL_BUFFER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_POOL_BUFFER,  \
                              GArrowPoolBuffer))
#define GARROW_POOL_BUFFER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_POOL_BUFFER,     \
                           GArrowPoolBufferClass))
#define GARROW_IS_POOL_BUFFER(obj)                              \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_POOL_BUFFER))
#define GARROW_IS_POOL_BUFFER_CLASS(klass)                      \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_POOL_BUFFER))
#define GARROW_POOL_BUFFER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_POOL_BUFFER,   \
                             GArrowPoolBufferClass))

typedef struct _GArrowPoolBuffer         GArrowPoolBuffer;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowPoolBufferClass    GArrowPoolBufferClass;
#endif

/**
 * GArrowPoolBuffer:
 *
 * It wraps `arrow::PoolBuffer`.
 */
struct _GArrowPoolBuffer
{
  /*< private >*/
  GArrowResizableBuffer parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowPoolBufferClass
{
  GArrowResizableBufferClass parent_class;
};
#endif

GType garrow_pool_buffer_get_type(void) G_GNUC_CONST;

GArrowPoolBuffer *garrow_pool_buffer_new(void);

G_END_DECLS
