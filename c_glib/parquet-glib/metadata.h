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

#define GPARQUET_TYPE_FILE_METADATA (gparquet_file_metadata_get_type())
G_DECLARE_DERIVABLE_TYPE(GParquetFileMetadata,
                         gparquet_file_metadata,
                         GPARQUET,
                         FILE_METADATA,
                         GObject)
struct _GParquetFileMetadataClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_file_metadata_equal(GParquetFileMetadata *metadata,
                             GParquetFileMetadata *other_metadata);
GARROW_AVAILABLE_IN_8_0
gint
gparquet_file_metadata_get_n_columns(GParquetFileMetadata *metadata);
GARROW_AVAILABLE_IN_8_0
gint
gparquet_file_metadata_get_n_schema_elements(GParquetFileMetadata *metadata);
GARROW_AVAILABLE_IN_8_0
gint64
gparquet_file_metadata_get_n_rows(GParquetFileMetadata *metadata);
GARROW_AVAILABLE_IN_8_0
gint
gparquet_file_metadata_get_n_row_groups(GParquetFileMetadata *metadata);
GARROW_AVAILABLE_IN_8_0
const gchar *
gparquet_file_metadata_get_created_by(GParquetFileMetadata *metadata);
GARROW_AVAILABLE_IN_8_0
guint32
gparquet_file_metadata_get_size(GParquetFileMetadata *metadata);
GARROW_AVAILABLE_IN_8_0
gboolean
gparquet_file_metadata_can_decompress(GParquetFileMetadata *metadata);

G_END_DECLS
