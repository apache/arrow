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

#include <arrow-glib/io-file-mode.h>

G_BEGIN_DECLS

#define GARROW_IO_TYPE_MEMORY_MAPPED_FILE       \
  (garrow_io_memory_mapped_file_get_type())
#define GARROW_IO_MEMORY_MAPPED_FILE(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_IO_TYPE_MEMORY_MAPPED_FILE,        \
                              GArrowIOMemoryMappedFile))
#define GARROW_IO_MEMORY_MAPPED_FILE_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_IO_TYPE_MEMORY_MAPPED_FILE,   \
                           GArrowIOMemoryMappedFileClass))
#define GARROW_IO_IS_MEMORY_MAPPED_FILE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_IO_TYPE_MEMORY_MAPPED_FILE))
#define GARROW_IO_IS_MEMORY_MAPPED_FILE_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_IO_TYPE_MEMORY_MAPPED_FILE))
#define GARROW_IO_MEMORY_MAPPED_FILE_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_IO_TYPE_MEMORY_MAPPED_FILE, \
                             GArrowIOMemoryMappedFileClass))

typedef struct _GArrowIOMemoryMappedFile         GArrowIOMemoryMappedFile;
typedef struct _GArrowIOMemoryMappedFileClass    GArrowIOMemoryMappedFileClass;

/**
 * GArrowIOMemoryMappedFile:
 *
 * It wraps `arrow::io::MemoryMappedFile`.
 */
struct _GArrowIOMemoryMappedFile
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowIOMemoryMappedFileClass
{
  GObjectClass parent_class;
};

GType garrow_io_memory_mapped_file_get_type(void) G_GNUC_CONST;

GArrowIOMemoryMappedFile *garrow_io_memory_mapped_file_open(const gchar *path,
                                                            GArrowIOFileMode mode,
                                                            GError **error);

G_END_DECLS
