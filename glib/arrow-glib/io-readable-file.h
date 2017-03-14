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

#define GARROW_IO_TYPE_READABLE_FILE                     \
  (garrow_io_readable_file_get_type())
#define GARROW_IO_READABLE_FILE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_IO_TYPE_READABLE_FILE,     \
                              GArrowIOReadableFileInterface))
#define GARROW_IO_IS_READABLE_FILE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_IO_TYPE_READABLE_FILE))
#define GARROW_IO_READABLE_FILE_GET_IFACE(obj)                          \
  (G_TYPE_INSTANCE_GET_INTERFACE((obj),                                 \
                                 GARROW_IO_TYPE_READABLE_FILE,          \
                                 GArrowIOReadableFileInterface))

typedef struct _GArrowIOReadableFile          GArrowIOReadableFile;
typedef struct _GArrowIOReadableFileInterface GArrowIOReadableFileInterface;

GType garrow_io_readable_file_get_type(void) G_GNUC_CONST;

guint64 garrow_io_readable_file_get_size(GArrowIOReadableFile *file,
                                         GError **error);
gboolean garrow_io_readable_file_get_support_zero_copy(GArrowIOReadableFile *file);
gboolean garrow_io_readable_file_read_at(GArrowIOReadableFile *file,
                                         gint64 position,
                                         gint64 n_bytes,
                                         gint64 *n_read_bytes,
                                         guint8 *buffer,
                                         GError **error);

G_END_DECLS
