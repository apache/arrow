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

#include <arrow-glib/reader.h>

G_BEGIN_DECLS

#define GARROW_TYPE_ORC_FILE_READER (garrow_orc_file_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowORCFileReader, garrow_orc_file_reader, GARROW, ORC_FILE_READER, GObject)
struct _GArrowORCFileReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowORCFileReader *
garrow_orc_file_reader_new(GArrowSeekableInputStream *file, GError **error);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
GARROW_DEPRECATED_IN_0_12_FOR(garrow_orc_file_reader_set_field_indices)
void
garrow_orc_file_reader_set_field_indexes(GArrowORCFileReader *reader,
                                         const gint *field_indexes,
                                         guint n_field_indexes);
#endif
GARROW_AVAILABLE_IN_0_12
void
garrow_orc_file_reader_set_field_indices(GArrowORCFileReader *reader,
                                         const gint *field_indices,
                                         guint n_field_indices);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
GARROW_DEPRECATED_IN_0_12_FOR(garrow_orc_file_reader_get_field_indices)
const gint *
garrow_orc_file_reader_get_field_indexes(GArrowORCFileReader *reader,
                                         guint *n_field_indexes);
#endif
GARROW_AVAILABLE_IN_0_12
const gint *
garrow_orc_file_reader_get_field_indices(GArrowORCFileReader *reader,
                                         guint *n_field_indices);

GARROW_AVAILABLE_IN_ALL
GArrowSchema *
garrow_orc_file_reader_read_type(GArrowORCFileReader *reader, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowTable *
garrow_orc_file_reader_read_stripes(GArrowORCFileReader *reader, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_orc_file_reader_read_stripe(GArrowORCFileReader *reader, gint64 i, GError **error);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_orc_file_reader_get_n_stripes(GArrowORCFileReader *reader);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_orc_file_reader_get_n_rows(GArrowORCFileReader *reader);

G_END_DECLS
