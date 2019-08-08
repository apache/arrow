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

#define GPARQUET_TYPE_ARROW_FILE_READER (gparquet_arrow_file_reader_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetArrowFileReader,
                         gparquet_arrow_file_reader,
                         GPARQUET,
                         ARROW_FILE_READER,
                         GObject)
struct _GParquetArrowFileReaderClass
{
  GObjectClass parent_class;
};

GParquetArrowFileReader *
gparquet_arrow_file_reader_new_arrow(GArrowSeekableInputStream *source,
                                     GError **error);
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_path(const gchar *path,
                                    GError **error);

GArrowTable *
gparquet_arrow_file_reader_read_table(GParquetArrowFileReader *reader,
                                      GError **error);

GArrowSchema *
gparquet_arrow_file_reader_get_schema(GParquetArrowFileReader *reader,
                                      GError **error);

GARROW_AVAILABLE_IN_1_0
GArrowChunkedArray *
gparquet_arrow_file_reader_read_column_data(GParquetArrowFileReader *reader,
                                            gint i,
                                            GError **error);

gint
gparquet_arrow_file_reader_get_n_row_groups(GParquetArrowFileReader *reader);

void
gparquet_arrow_file_reader_set_use_threads(GParquetArrowFileReader *reader,
                                           gboolean use_threads);

G_END_DECLS
