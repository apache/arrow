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

#include <parquet-glib/metadata.h>

G_BEGIN_DECLS

#define GPARQUET_TYPE_READER_PROPERTIES (gparquet_reader_properties_get_type())
GPARQUET_AVAILABLE_IN_26_0
G_DECLARE_DERIVABLE_TYPE(GParquetReaderProperties,
                         gparquet_reader_properties,
                         GPARQUET,
                         READER_PROPERTIES,
                         GObject)
struct _GParquetReaderPropertiesClass
{
  GObjectClass parent_class;
};

GPARQUET_AVAILABLE_IN_26_0
GParquetReaderProperties *
gparquet_reader_properties_new(void);

GPARQUET_AVAILABLE_IN_26_0
void
gparquet_reader_properties_enable_buffered_stream(GParquetReaderProperties *properties);
GPARQUET_AVAILABLE_IN_26_0
void
gparquet_reader_properties_disable_buffered_stream(GParquetReaderProperties *properties);
GPARQUET_AVAILABLE_IN_26_0
gboolean
gparquet_reader_properties_is_buffered_stream_enabled(
  GParquetReaderProperties *properties);

GPARQUET_AVAILABLE_IN_26_0
void
gparquet_reader_properties_set_buffer_size(GParquetReaderProperties *properties,
                                           gint64 buffer_size);
GPARQUET_AVAILABLE_IN_26_0
gint64
gparquet_reader_properties_get_buffer_size(GParquetReaderProperties *properties);

#define GPARQUET_TYPE_ARROW_FILE_READER (gparquet_arrow_file_reader_get_type())
GPARQUET_AVAILABLE_IN_0_11
G_DECLARE_DERIVABLE_TYPE(GParquetArrowFileReader,
                         gparquet_arrow_file_reader,
                         GPARQUET,
                         ARROW_FILE_READER,
                         GObject)
struct _GParquetArrowFileReaderClass
{
  GObjectClass parent_class;
};

GPARQUET_AVAILABLE_IN_0_11
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_arrow(GArrowSeekableInputStream *source, GError **error);

GPARQUET_AVAILABLE_IN_0_11
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_path(const gchar *path, GError **error);

GPARQUET_AVAILABLE_IN_26_0
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_arrow_with_properties(GArrowSeekableInputStream *source,
                                                     GParquetReaderProperties *properties,
                                                     GError **error);

GPARQUET_AVAILABLE_IN_26_0
GParquetArrowFileReader *
gparquet_arrow_file_reader_new_path_with_properties(const gchar *path,
                                                    GParquetReaderProperties *properties,
                                                    GError **error);

GPARQUET_AVAILABLE_IN_23_0
void
gparquet_arrow_file_reader_close(GParquetArrowFileReader *reader);

GPARQUET_AVAILABLE_IN_0_11
GArrowTable *
gparquet_arrow_file_reader_read_table(GParquetArrowFileReader *reader, GError **error);

GPARQUET_AVAILABLE_IN_1_0
GArrowTable *
gparquet_arrow_file_reader_read_row_group(GParquetArrowFileReader *reader,
                                          gint row_group_index,
                                          gint *column_indices,
                                          gsize n_column_indices,
                                          GError **error);

GPARQUET_AVAILABLE_IN_0_12
GArrowSchema *
gparquet_arrow_file_reader_get_schema(GParquetArrowFileReader *reader, GError **error);

GPARQUET_AVAILABLE_IN_0_15
GArrowChunkedArray *
gparquet_arrow_file_reader_read_column_data(GParquetArrowFileReader *reader,
                                            gint i,
                                            GError **error);

GPARQUET_AVAILABLE_IN_0_11
gint
gparquet_arrow_file_reader_get_n_row_groups(GParquetArrowFileReader *reader);

GPARQUET_AVAILABLE_IN_6_0
gint64
gparquet_arrow_file_reader_get_n_rows(GParquetArrowFileReader *reader);

GPARQUET_AVAILABLE_IN_0_11
void
gparquet_arrow_file_reader_set_use_threads(GParquetArrowFileReader *reader,
                                           gboolean use_threads);

GPARQUET_AVAILABLE_IN_8_0
GParquetFileMetadata *
gparquet_arrow_file_reader_get_metadata(GParquetArrowFileReader *reader);

G_END_DECLS
