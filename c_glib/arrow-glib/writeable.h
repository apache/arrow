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

#define GARROW_TYPE_WRITEABLE                \
  (garrow_writeable_get_type())
#define GARROW_WRITEABLE(obj)                                \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_WRITEABLE,         \
                              GArrowWriteable))
#define GARROW_IS_WRITEABLE(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_WRITEABLE))
#define GARROW_WRITEABLE_GET_IFACE(obj)                      \
  (G_TYPE_INSTANCE_GET_INTERFACE((obj),                         \
                                 GARROW_TYPE_WRITEABLE,      \
                                 GArrowWriteableInterface))

typedef struct _GArrowWriteable          GArrowWriteable;
typedef struct _GArrowWriteableInterface GArrowWriteableInterface;

GType garrow_writeable_get_type(void) G_GNUC_CONST;

gboolean garrow_writeable_write(GArrowWriteable *writeable,
                                   const guint8 *data,
                                   gint64 n_bytes,
                                   GError **error);
gboolean garrow_writeable_flush(GArrowWriteable *writeable,
                                   GError **error);

G_END_DECLS
