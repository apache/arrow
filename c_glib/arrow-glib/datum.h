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

#include <arrow-glib/array.h>
#include <arrow-glib/chunked-array.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/scalar.h>
#include <arrow-glib/table.h>

G_BEGIN_DECLS

#define GARROW_TYPE_DATUM (garrow_datum_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDatum,
                         garrow_datum,
                         GARROW,
                         DATUM,
                         GObject)
struct _GArrowDatumClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
gboolean garrow_datum_is_array(GArrowDatum *datum);
GARROW_AVAILABLE_IN_1_0
gboolean garrow_datum_is_array_like(GArrowDatum *datum);
GARROW_AVAILABLE_IN_5_0
gboolean garrow_datum_is_scalar(GArrowDatum *datum);
GARROW_AVAILABLE_IN_5_0
gboolean garrow_datum_is_value(GArrowDatum *datum);
/*
GARROW_AVAILABLE_IN_5_0
gboolean garrow_datum_is_collection(GArrowDatum *datum);
*/
GARROW_AVAILABLE_IN_1_0
gboolean garrow_datum_equal(GArrowDatum *datum,
                            GArrowDatum *other_datum);
GARROW_AVAILABLE_IN_1_0
gchar *garrow_datum_to_string(GArrowDatum *datum);

/* GARROW_TYPE_NONE_DATUM */

#define GARROW_TYPE_SCALAR_DATUM (garrow_scalar_datum_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowScalarDatum,
                         garrow_scalar_datum,
                         GARROW,
                         SCALAR_DATUM,
                         GArrowDatum)
struct _GArrowScalarDatumClass
{
  GArrowDatumClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowScalarDatum *garrow_scalar_datum_new(GArrowScalar *value);

#define GARROW_TYPE_ARRAY_DATUM (garrow_array_datum_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowArrayDatum,
                         garrow_array_datum,
                         GARROW,
                         ARRAY_DATUM,
                         GArrowDatum)
struct _GArrowArrayDatumClass
{
  GArrowDatumClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowArrayDatum *garrow_array_datum_new(GArrowArray *value);

#define GARROW_TYPE_CHUNKED_ARRAY_DATUM (garrow_chunked_array_datum_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowChunkedArrayDatum,
                         garrow_chunked_array_datum,
                         GARROW,
                         CHUNKED_ARRAY_DATUM,
                         GArrowDatum)
struct _GArrowChunkedArrayDatumClass
{
  GArrowDatumClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowChunkedArrayDatum *
garrow_chunked_array_datum_new(GArrowChunkedArray *value);

#define GARROW_TYPE_RECORD_BATCH_DATUM (garrow_record_batch_datum_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchDatum,
                         garrow_record_batch_datum,
                         GARROW,
                         RECORD_BATCH_DATUM,
                         GArrowDatum)
struct _GArrowRecordBatchDatumClass
{
  GArrowDatumClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowRecordBatchDatum *
garrow_record_batch_datum_new(GArrowRecordBatch *value);

#define GARROW_TYPE_TABLE_DATUM (garrow_table_datum_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTableDatum,
                         garrow_table_datum,
                         GARROW,
                         TABLE_DATUM,
                         GArrowDatum)
struct _GArrowTableDatumClass
{
  GArrowDatumClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowTableDatum *garrow_table_datum_new(GArrowTable *value);

/* GARROW_TYPE_COLLECTION_DATUM */

G_END_DECLS
