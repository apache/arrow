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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS

#define GPARQUET_TYPE_ARROW_FILE_WRITER (gparquet_arrow_file_writer_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetArrowFileWriter,
                         gparquet_arrow_file_writer,
                         GPARQUET,
                         ARROW_FILE_WRITER,
                         GObject)
struct _GParquetArrowFileWriterClass
{
  GObjectClass parent_class;
};

GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_arrow(GArrowSchema *schema,
                                     GArrowOutputStream *sink,
                                     GError **error);
GParquetArrowFileWriter *
gparquet_arrow_file_writer_new_path(GArrowSchema *schema,
                                    const gchar *path,
                                    GError **error);

gboolean
gparquet_arrow_file_writer_write_table(GParquetArrowFileWriter *writer,
                                       GArrowTable *table,
                                       guint64 chunk_size,
                                       GError **error);

gboolean
gparquet_arrow_file_writer_close(GParquetArrowFileWriter *writer,
                                 GError **error);

G_END_DECLS
