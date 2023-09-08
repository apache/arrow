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
#include <arrow-glib/decimal.h>
#include <arrow-glib/interval.h>

G_BEGIN_DECLS

#define GARROW_TYPE_ARRAY_BUILDER (garrow_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowArrayBuilder,
                         garrow_array_builder,
                         GARROW,
                         ARRAY_BUILDER,
                         GObject)
struct _GArrowArrayBuilderClass
{
  GObjectClass parent_class;
};

GArrowDataType *
garrow_array_builder_get_value_data_type(GArrowArrayBuilder *builder);
GArrowType garrow_array_builder_get_value_type(GArrowArrayBuilder *builder);

GArrowArray *garrow_array_builder_finish(GArrowArrayBuilder *builder,
                                         GError **error);

GARROW_AVAILABLE_IN_2_0
void garrow_array_builder_reset(GArrowArrayBuilder *builder);

GARROW_AVAILABLE_IN_2_0
gint64 garrow_array_builder_get_capacity(GArrowArrayBuilder *builder);
GARROW_AVAILABLE_IN_2_0
gint64 garrow_array_builder_get_length(GArrowArrayBuilder *builder);
GARROW_AVAILABLE_IN_2_0
gint64 garrow_array_builder_get_n_nulls(GArrowArrayBuilder *builder);
GARROW_AVAILABLE_IN_12_0
GArrowArrayBuilder *
garrow_array_builder_get_child(GArrowArrayBuilder *builder,
                               gint i);
GARROW_AVAILABLE_IN_12_0
GList *
garrow_array_builder_get_children(GArrowArrayBuilder *builder);

GARROW_AVAILABLE_IN_2_0
gboolean garrow_array_builder_resize(GArrowArrayBuilder *builder,
                                     gint64 capacity,
                                     GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean garrow_array_builder_reserve(GArrowArrayBuilder *builder,
                                      gint64 additional_capacity,
                                      GError **error);

GARROW_AVAILABLE_IN_3_0
gboolean garrow_array_builder_append_null(GArrowArrayBuilder *builder,
                                          GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_array_builder_append_nulls(GArrowArrayBuilder *builder,
                                           gint64 n,
                                           GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_array_builder_append_empty_value(GArrowArrayBuilder *builder,
                                                 GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_array_builder_append_empty_values(GArrowArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);

#define GARROW_TYPE_NULL_ARRAY_BUILDER (garrow_null_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowNullArrayBuilder,
                         garrow_null_array_builder,
                         GARROW,
                         NULL_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowNullArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_0_13
GArrowNullArrayBuilder *garrow_null_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_0_13
gboolean garrow_null_array_builder_append_null(GArrowNullArrayBuilder *builder,
                                               GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
GARROW_AVAILABLE_IN_0_13
gboolean garrow_null_array_builder_append_nulls(GArrowNullArrayBuilder *builder,
                                                gint64 n,
                                                GError **error);
#endif


#define GARROW_TYPE_BOOLEAN_ARRAY_BUILDER       \
  (garrow_boolean_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBooleanArrayBuilder,
                         garrow_boolean_array_builder,
                         GARROW,
                         BOOLEAN_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowBooleanArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowBooleanArrayBuilder *garrow_boolean_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_boolean_array_builder_append_value)
gboolean garrow_boolean_array_builder_append(GArrowBooleanArrayBuilder *builder,
                                             gboolean value,
                                             GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_boolean_array_builder_append_value(GArrowBooleanArrayBuilder *builder,
                                                   gboolean value,
                                                   GError **error);
gboolean garrow_boolean_array_builder_append_values(GArrowBooleanArrayBuilder *builder,
                                                    const gboolean *values,
                                                    gint64 values_length,
                                                    const gboolean *is_valids,
                                                    gint64 is_valids_length,
                                                    GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_boolean_array_builder_append_null(GArrowBooleanArrayBuilder *builder,
                                                  GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_boolean_array_builder_append_nulls(GArrowBooleanArrayBuilder *builder,
                                                   gint64 n,
                                                   GError **error);
#endif


#define GARROW_TYPE_INT_ARRAY_BUILDER (garrow_int_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowIntArrayBuilder,
                         garrow_int_array_builder,
                         GARROW,
                         INT_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowIntArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowIntArrayBuilder *garrow_int_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_int_array_builder_append_value)
gboolean garrow_int_array_builder_append(GArrowIntArrayBuilder *builder,
                                         gint64 value,
                                         GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_int_array_builder_append_value(GArrowIntArrayBuilder *builder,
                                               gint64 value,
                                               GError **error);
gboolean garrow_int_array_builder_append_values(GArrowIntArrayBuilder *builder,
                                                const gint64 *values,
                                                gint64 values_length,
                                                const gboolean *is_valids,
                                                gint64 is_valids_length,
                                                GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_int_array_builder_append_null(GArrowIntArrayBuilder *builder,
                                              GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_int_array_builder_append_nulls(GArrowIntArrayBuilder *builder,
                                               gint64 n,
                                               GError **error);
#endif


#define GARROW_TYPE_UINT_ARRAY_BUILDER (garrow_uint_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUIntArrayBuilder,
                         garrow_uint_array_builder,
                         GARROW,
                         UINT_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowUIntArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowUIntArrayBuilder *garrow_uint_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_uint_array_builder_append_value)
gboolean garrow_uint_array_builder_append(GArrowUIntArrayBuilder *builder,
                                          guint64 value,
                                          GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_uint_array_builder_append_value(GArrowUIntArrayBuilder *builder,
                                                guint64 value,
                                                GError **error);
gboolean garrow_uint_array_builder_append_values(GArrowUIntArrayBuilder *builder,
                                                 const guint64 *values,
                                                 gint64 values_length,
                                                 const gboolean *is_valids,
                                                 gint64 is_valids_length,
                                                 GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_uint_array_builder_append_null(GArrowUIntArrayBuilder *builder,
                                               GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_uint_array_builder_append_nulls(GArrowUIntArrayBuilder *builder,
                                                gint64 n,
                                                GError **error);
#endif


#define GARROW_TYPE_INT8_ARRAY_BUILDER (garrow_int8_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt8ArrayBuilder,
                         garrow_int8_array_builder,
                         GARROW,
                         INT8_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowInt8ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowInt8ArrayBuilder *garrow_int8_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_int8_array_builder_append_value)
gboolean garrow_int8_array_builder_append(GArrowInt8ArrayBuilder *builder,
                                          gint8 value,
                                          GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_int8_array_builder_append_value(GArrowInt8ArrayBuilder *builder,
                                                gint8 value,
                                                GError **error);
gboolean garrow_int8_array_builder_append_values(GArrowInt8ArrayBuilder *builder,
                                                 const gint8 *values,
                                                 gint64 values_length,
                                                 const gboolean *is_valids,
                                                 gint64 is_valids_length,
                                                 GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_int8_array_builder_append_null(GArrowInt8ArrayBuilder *builder,
                                               GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_int8_array_builder_append_nulls(GArrowInt8ArrayBuilder *builder,
                                                gint64 n,
                                                GError **error);
#endif


#define GARROW_TYPE_UINT8_ARRAY_BUILDER (garrow_uint8_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt8ArrayBuilder,
                         garrow_uint8_array_builder,
                         GARROW,
                         UINT8_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowUInt8ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowUInt8ArrayBuilder *garrow_uint8_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_uint8_array_builder_append_value)
gboolean garrow_uint8_array_builder_append(GArrowUInt8ArrayBuilder *builder,
                                           guint8 value,
                                           GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_uint8_array_builder_append_value(GArrowUInt8ArrayBuilder *builder,
                                                 guint8 value,
                                                 GError **error);
gboolean garrow_uint8_array_builder_append_values(GArrowUInt8ArrayBuilder *builder,
                                                  const guint8 *values,
                                                  gint64 values_length,
                                                  const gboolean *is_valids,
                                                  gint64 is_valids_length,
                                                  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_uint8_array_builder_append_null(GArrowUInt8ArrayBuilder *builder,
                                                GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_uint8_array_builder_append_nulls(GArrowUInt8ArrayBuilder *builder,
                                                 gint64 n,
                                                 GError **error);
#endif


#define GARROW_TYPE_INT16_ARRAY_BUILDER (garrow_int16_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt16ArrayBuilder,
                         garrow_int16_array_builder,
                         GARROW,
                         INT16_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowInt16ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowInt16ArrayBuilder *garrow_int16_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_int16_array_builder_append_value)
gboolean garrow_int16_array_builder_append(GArrowInt16ArrayBuilder *builder,
                                           gint16 value,
                                           GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_int16_array_builder_append_value(GArrowInt16ArrayBuilder *builder,
                                                 gint16 value,
                                                 GError **error);
gboolean garrow_int16_array_builder_append_values(GArrowInt16ArrayBuilder *builder,
                                                  const gint16 *values,
                                                  gint64 values_length,
                                                  const gboolean *is_valids,
                                                  gint64 is_valids_length,
                                                  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_int16_array_builder_append_null(GArrowInt16ArrayBuilder *builder,
                                                GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_int16_array_builder_append_nulls(GArrowInt16ArrayBuilder *builder,
                                                 gint64 n,
                                                 GError **error);
#endif


#define GARROW_TYPE_UINT16_ARRAY_BUILDER        \
  (garrow_uint16_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt16ArrayBuilder,
                         garrow_uint16_array_builder,
                         GARROW,
                         UINT16_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowUInt16ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowUInt16ArrayBuilder *garrow_uint16_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_uint16_array_builder_append_value)
gboolean garrow_uint16_array_builder_append(GArrowUInt16ArrayBuilder *builder,
                                            guint16 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_uint16_array_builder_append_value(GArrowUInt16ArrayBuilder *builder,
                                                  guint16 value,
                                                  GError **error);
gboolean garrow_uint16_array_builder_append_values(GArrowUInt16ArrayBuilder *builder,
                                                   const guint16 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_uint16_array_builder_append_null(GArrowUInt16ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_uint16_array_builder_append_nulls(GArrowUInt16ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_INT32_ARRAY_BUILDER (garrow_int32_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt32ArrayBuilder,
                         garrow_int32_array_builder,
                         GARROW,
                         INT32_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowInt32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowInt32ArrayBuilder *garrow_int32_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_int32_array_builder_append_value)
gboolean garrow_int32_array_builder_append(GArrowInt32ArrayBuilder *builder,
                                           gint32 value,
                                           GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_int32_array_builder_append_value(GArrowInt32ArrayBuilder *builder,
                                                 gint32 value,
                                                 GError **error);
gboolean garrow_int32_array_builder_append_values(GArrowInt32ArrayBuilder *builder,
                                                  const gint32 *values,
                                                  gint64 values_length,
                                                  const gboolean *is_valids,
                                                  gint64 is_valids_length,
                                                  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_int32_array_builder_append_null(GArrowInt32ArrayBuilder *builder,
                                                GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_int32_array_builder_append_nulls(GArrowInt32ArrayBuilder *builder,
                                                 gint64 n,
                                                 GError **error);
#endif


#define GARROW_TYPE_UINT32_ARRAY_BUILDER        \
  (garrow_uint32_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt32ArrayBuilder,
                         garrow_uint32_array_builder,
                         GARROW,
                         UINT32_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowUInt32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowUInt32ArrayBuilder *garrow_uint32_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_uint32_array_builder_append_value)
gboolean garrow_uint32_array_builder_append(GArrowUInt32ArrayBuilder *builder,
                                            guint32 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_uint32_array_builder_append_value(GArrowUInt32ArrayBuilder *builder,
                                                  guint32 value,
                                                  GError **error);
gboolean garrow_uint32_array_builder_append_values(GArrowUInt32ArrayBuilder *builder,
                                                   const guint32 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_uint32_array_builder_append_null(GArrowUInt32ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_uint32_array_builder_append_nulls(GArrowUInt32ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_INT64_ARRAY_BUILDER (garrow_int64_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt64ArrayBuilder,
                         garrow_int64_array_builder,
                         GARROW,
                         INT64_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowInt64ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowInt64ArrayBuilder *garrow_int64_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_int64_array_builder_append_value)
gboolean garrow_int64_array_builder_append(GArrowInt64ArrayBuilder *builder,
                                           gint64 value,
                                           GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_int64_array_builder_append_value(GArrowInt64ArrayBuilder *builder,
                                                 gint64 value,
                                                 GError **error);
gboolean garrow_int64_array_builder_append_values(GArrowInt64ArrayBuilder *builder,
                                                  const gint64 *values,
                                                  gint64 values_length,
                                                  const gboolean *is_valids,
                                                  gint64 is_valids_length,
                                                  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_int64_array_builder_append_null(GArrowInt64ArrayBuilder *builder,
                                                GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_int64_array_builder_append_nulls(GArrowInt64ArrayBuilder *builder,
                                                 gint64 n,
                                                 GError **error);
#endif


#define GARROW_TYPE_UINT64_ARRAY_BUILDER        \
  (garrow_uint64_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt64ArrayBuilder,
                         garrow_uint64_array_builder,
                         GARROW,
                         UINT64_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowUInt64ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowUInt64ArrayBuilder *garrow_uint64_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_uint64_array_builder_append_value)
gboolean garrow_uint64_array_builder_append(GArrowUInt64ArrayBuilder *builder,
                                            guint64 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_uint64_array_builder_append_value(GArrowUInt64ArrayBuilder *builder,
                                                  guint64 value,
                                                  GError **error);
gboolean garrow_uint64_array_builder_append_values(GArrowUInt64ArrayBuilder *builder,
                                                   const guint64 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_uint64_array_builder_append_null(GArrowUInt64ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_uint64_array_builder_append_nulls(GArrowUInt64ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_HALF_FLOAT_ARRAY_BUILDER    \
  (garrow_half_float_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowHalfFloatArrayBuilder,
                         garrow_half_float_array_builder,
                         GARROW,
                         HALF_FLOAT_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowHalfFloatArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GArrowHalfFloatArrayBuilder *
garrow_half_float_array_builder_new(void);

GARROW_AVAILABLE_IN_11_0
gboolean
garrow_half_float_array_builder_append_value(
  GArrowHalfFloatArrayBuilder *builder,
  guint16 value,
  GError **error);
GARROW_AVAILABLE_IN_11_0
gboolean garrow_half_float_array_builder_append_values(
  GArrowHalfFloatArrayBuilder *builder,
  const guint16 *values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);


#define GARROW_TYPE_FLOAT_ARRAY_BUILDER (garrow_float_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFloatArrayBuilder,
                         garrow_float_array_builder,
                         GARROW,
                         FLOAT_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowFloatArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowFloatArrayBuilder *garrow_float_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_float_array_builder_append_value)
gboolean garrow_float_array_builder_append(GArrowFloatArrayBuilder *builder,
                                           gfloat value,
                                           GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_float_array_builder_append_value(GArrowFloatArrayBuilder *builder,
                                                 gfloat value,
                                                 GError **error);
gboolean garrow_float_array_builder_append_values(GArrowFloatArrayBuilder *builder,
                                                  const gfloat *values,
                                                  gint64 values_length,
                                                  const gboolean *is_valids,
                                                  gint64 is_valids_length,
                                                  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_float_array_builder_append_null(GArrowFloatArrayBuilder *builder,
                                                GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_float_array_builder_append_nulls(GArrowFloatArrayBuilder *builder,
                                                 gint64 n,
                                                 GError **error);
#endif


#define GARROW_TYPE_DOUBLE_ARRAY_BUILDER        \
  (garrow_double_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDoubleArrayBuilder,
                         garrow_double_array_builder,
                         GARROW,
                         DOUBLE_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowDoubleArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowDoubleArrayBuilder *garrow_double_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_double_array_builder_append_value)
gboolean garrow_double_array_builder_append(GArrowDoubleArrayBuilder *builder,
                                            gdouble value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_double_array_builder_append_value(GArrowDoubleArrayBuilder *builder,
                                                  gdouble value,
                                                  GError **error);
gboolean garrow_double_array_builder_append_values(GArrowDoubleArrayBuilder *builder,
                                                   const gdouble *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_double_array_builder_append_null(GArrowDoubleArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_double_array_builder_append_nulls(GArrowDoubleArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_BINARY_ARRAY_BUILDER        \
  (garrow_binary_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBinaryArrayBuilder,
                         garrow_binary_array_builder,
                         GARROW,
                         BINARY_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowBinaryArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowBinaryArrayBuilder *garrow_binary_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_binary_array_builder_append_value)
gboolean garrow_binary_array_builder_append(GArrowBinaryArrayBuilder *builder,
                                            const guint8 *value,
                                            gint32 length,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_binary_array_builder_append_value(GArrowBinaryArrayBuilder *builder,
                                                  const guint8 *value,
                                                  gint32 length,
                                                  GError **error);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_binary_array_builder_append_value_bytes(GArrowBinaryArrayBuilder *builder,
                                                        GBytes *value,
                                                        GError **error);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_binary_array_builder_append_values(GArrowBinaryArrayBuilder *builder,
                                                   GBytes **values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_binary_array_builder_append_null(GArrowBinaryArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
GARROW_AVAILABLE_IN_0_16
gboolean garrow_binary_array_builder_append_nulls(GArrowBinaryArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_LARGE_BINARY_ARRAY_BUILDER        \
  (garrow_large_binary_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeBinaryArrayBuilder,
                         garrow_large_binary_array_builder,
                         GARROW,
                         LARGE_BINARY_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowLargeBinaryArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeBinaryArrayBuilder *garrow_large_binary_array_builder_new(void);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_binary_array_builder_append_value(GArrowLargeBinaryArrayBuilder *builder,
                                                        const guint8 *value,
                                                        gint64 length,
                                                        GError **error);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_binary_array_builder_append_value_bytes(GArrowLargeBinaryArrayBuilder *builder,
                                                              GBytes *value,
                                                              GError **error);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_binary_array_builder_append_values(GArrowLargeBinaryArrayBuilder *builder,
                                                         GBytes **values,
                                                         gint64 values_length,
                                                         const gboolean *is_valids,
                                                         gint64 is_valids_length,
                                                         GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_binary_array_builder_append_null(GArrowLargeBinaryArrayBuilder *builder,
                                                       GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_binary_array_builder_append_nulls(GArrowLargeBinaryArrayBuilder *builder,
                                                        gint64 n,
                                                        GError **error);
#endif


#define GARROW_TYPE_STRING_ARRAY_BUILDER        \
  (garrow_string_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStringArrayBuilder,
                         garrow_string_array_builder,
                         GARROW,
                         STRING_ARRAY_BUILDER,
                         GArrowBinaryArrayBuilder)
struct _GArrowStringArrayBuilderClass
{
  GArrowBinaryArrayBuilderClass parent_class;
};

GArrowStringArrayBuilder *garrow_string_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_string_array_builder_append_value)
gboolean garrow_string_array_builder_append(GArrowStringArrayBuilder *builder,
                                            const gchar *value,
                                            GError **error);
#endif
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_16_FOR(garrow_string_array_builder_append_string)
GARROW_AVAILABLE_IN_0_12
gboolean garrow_string_array_builder_append_value(GArrowStringArrayBuilder *builder,
                                                  const gchar *value,
                                                  GError **error);
#endif
GARROW_AVAILABLE_IN_0_16
gboolean garrow_string_array_builder_append_string(GArrowStringArrayBuilder *builder,
                                                   const gchar *value,
                                                   GError **error);

GARROW_AVAILABLE_IN_8_0
gboolean
garrow_string_array_builder_append_string_len(GArrowStringArrayBuilder *builder,
                                              const gchar *value,
                                              gint32 length,
                                              GError **error);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_16_FOR(garrow_string_array_builder_append_strings)
gboolean garrow_string_array_builder_append_values(GArrowStringArrayBuilder *builder,
                                                   const gchar **values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#endif
GARROW_AVAILABLE_IN_0_16
gboolean garrow_string_array_builder_append_strings(GArrowStringArrayBuilder *builder,
                                                    const gchar **values,
                                                    gint64 values_length,
                                                    const gboolean *is_valids,
                                                    gint64 is_valids_length,
                                                    GError **error);


#define GARROW_TYPE_LARGE_STRING_ARRAY_BUILDER        \
  (garrow_large_string_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeStringArrayBuilder,
                         garrow_large_string_array_builder,
                         GARROW,
                         LARGE_STRING_ARRAY_BUILDER,
                         GArrowLargeBinaryArrayBuilder)
struct _GArrowLargeStringArrayBuilderClass
{
  GArrowLargeBinaryArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeStringArrayBuilder *garrow_large_string_array_builder_new(void);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_string_array_builder_append_string(GArrowLargeStringArrayBuilder *builder,
                                                         const gchar *value,
                                                         GError **error);
GARROW_AVAILABLE_IN_8_0
gboolean garrow_large_string_array_builder_append_string_len(
  GArrowLargeStringArrayBuilder *builder,
  const gchar *value,
  gint64 length,
  GError **error);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_string_array_builder_append_strings(GArrowLargeStringArrayBuilder *builder,
                                                          const gchar **values,
                                                          gint64 values_length,
                                                          const gboolean *is_valids,
                                                          gint64 is_valids_length,
                                                          GError **error);


#define GARROW_TYPE_FIXED_SIZE_BINARY_ARRAY_BUILDER      \
  (garrow_fixed_size_binary_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFixedSizeBinaryArrayBuilder,
                         garrow_fixed_size_binary_array_builder,
                         GARROW,
                         FIXED_SIZE_BINARY_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowFixedSizeBinaryArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowFixedSizeBinaryArrayBuilder *
garrow_fixed_size_binary_array_builder_new(
  GArrowFixedSizeBinaryDataType *data_type);

GARROW_AVAILABLE_IN_3_0
gboolean
garrow_fixed_size_binary_array_builder_append_value(
  GArrowFixedSizeBinaryArrayBuilder *builder,
  const guint8 *value,
  gint32 length,
  GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_fixed_size_binary_array_builder_append_value_bytes(
  GArrowFixedSizeBinaryArrayBuilder *builder,
  GBytes *value,
  GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_fixed_size_binary_array_builder_append_values(
  GArrowFixedSizeBinaryArrayBuilder *builder,
  GBytes **values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_fixed_size_binary_array_builder_append_values_packed(
  GArrowFixedSizeBinaryArrayBuilder *builder,
  GBytes *values,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);

#define GARROW_TYPE_DATE32_ARRAY_BUILDER        \
  (garrow_date32_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDate32ArrayBuilder,
                         garrow_date32_array_builder,
                         GARROW,
                         DATE32_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowDate32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowDate32ArrayBuilder *garrow_date32_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_date32_array_builder_append_value)
gboolean garrow_date32_array_builder_append(GArrowDate32ArrayBuilder *builder,
                                            gint32 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_date32_array_builder_append_value(GArrowDate32ArrayBuilder *builder,
                                                  gint32 value,
                                                  GError **error);
gboolean garrow_date32_array_builder_append_values(GArrowDate32ArrayBuilder *builder,
                                                   const gint32 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_date32_array_builder_append_null(GArrowDate32ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_date32_array_builder_append_nulls(GArrowDate32ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_DATE64_ARRAY_BUILDER        \
  (garrow_date64_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDate64ArrayBuilder,
                         garrow_date64_array_builder,
                         GARROW,
                         DATE64_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowDate64ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowDate64ArrayBuilder *garrow_date64_array_builder_new(void);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_date64_array_builder_append_value)
gboolean garrow_date64_array_builder_append(GArrowDate64ArrayBuilder *builder,
                                            gint64 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_date64_array_builder_append_value(GArrowDate64ArrayBuilder *builder,
                                                  gint64 value,
                                                  GError **error);
gboolean garrow_date64_array_builder_append_values(GArrowDate64ArrayBuilder *builder,
                                                   const gint64 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_date64_array_builder_append_null(GArrowDate64ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_date64_array_builder_append_nulls(GArrowDate64ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_TIMESTAMP_ARRAY_BUILDER     \
  (garrow_timestamp_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTimestampArrayBuilder,
                         garrow_timestamp_array_builder,
                         GARROW,
                         TIMESTAMP_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowTimestampArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowTimestampArrayBuilder *
garrow_timestamp_array_builder_new(GArrowTimestampDataType *data_type);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_timestamp_array_builder_append_value)
gboolean garrow_timestamp_array_builder_append(GArrowTimestampArrayBuilder *builder,
                                               gint64 value,
                                               GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_timestamp_array_builder_append_value(GArrowTimestampArrayBuilder *builder,
                                                     gint64 value,
                                                     GError **error);
gboolean garrow_timestamp_array_builder_append_values(GArrowTimestampArrayBuilder *builder,
                                                      const gint64 *values,
                                                      gint64 values_length,
                                                      const gboolean *is_valids,
                                                      gint64 is_valids_length,
                                                      GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_timestamp_array_builder_append_null(GArrowTimestampArrayBuilder *builder,
                                                    GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_timestamp_array_builder_append_nulls(GArrowTimestampArrayBuilder *builder,
                                                     gint64 n,
                                                     GError **error);
#endif


#define GARROW_TYPE_TIME32_ARRAY_BUILDER        \
  (garrow_time32_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTime32ArrayBuilder,
                         garrow_time32_array_builder,
                         GARROW,
                         TIME32_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowTime32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowTime32ArrayBuilder *garrow_time32_array_builder_new(GArrowTime32DataType *data_type);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_time32_array_builder_append_value)
gboolean garrow_time32_array_builder_append(GArrowTime32ArrayBuilder *builder,
                                            gint32 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_time32_array_builder_append_value(GArrowTime32ArrayBuilder *builder,
                                                  gint32 value,
                                                  GError **error);
gboolean garrow_time32_array_builder_append_values(GArrowTime32ArrayBuilder *builder,
                                                   const gint32 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_time32_array_builder_append_null(GArrowTime32ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_time32_array_builder_append_nulls(GArrowTime32ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_TIME64_ARRAY_BUILDER        \
  (garrow_time64_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTime64ArrayBuilder,
                         garrow_time64_array_builder,
                         GARROW,
                         TIME64_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowTime64ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowTime64ArrayBuilder *garrow_time64_array_builder_new(GArrowTime64DataType *data_type);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_time64_array_builder_append_value)
gboolean garrow_time64_array_builder_append(GArrowTime64ArrayBuilder *builder,
                                            gint64 value,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_time64_array_builder_append_value(GArrowTime64ArrayBuilder *builder,
                                                  gint64 value,
                                                  GError **error);
gboolean garrow_time64_array_builder_append_values(GArrowTime64ArrayBuilder *builder,
                                                   const gint64 *values,
                                                   gint64 values_length,
                                                   const gboolean *is_valids,
                                                   gint64 is_valids_length,
                                                   GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_time64_array_builder_append_null(GArrowTime64ArrayBuilder *builder,
                                                 GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
gboolean garrow_time64_array_builder_append_nulls(GArrowTime64ArrayBuilder *builder,
                                                  gint64 n,
                                                  GError **error);
#endif


#define GARROW_TYPE_MONTH_INTERVAL_ARRAY_BUILDER        \
  (garrow_month_interval_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthIntervalArrayBuilder,
                         garrow_month_interval_array_builder,
                         GARROW,
                         MONTH_INTERVAL_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowMonthIntervalArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthIntervalArrayBuilder *
garrow_month_interval_array_builder_new(void);

GARROW_AVAILABLE_IN_8_0
gboolean
garrow_month_interval_array_builder_append_value(
  GArrowMonthIntervalArrayBuilder *builder,
  gint32 value,
  GError **error);
GARROW_AVAILABLE_IN_8_0
gboolean
garrow_month_interval_array_builder_append_values(
  GArrowMonthIntervalArrayBuilder *builder,
  const gint32 *values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);


#define GARROW_TYPE_DAY_TIME_INTERVAL_ARRAY_BUILDER     \
  (garrow_day_time_interval_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDayTimeIntervalArrayBuilder,
                         garrow_day_time_interval_array_builder,
                         GARROW,
                         DAY_TIME_INTERVAL_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowDayTimeIntervalArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowDayTimeIntervalArrayBuilder *
garrow_day_time_interval_array_builder_new(void);

GARROW_AVAILABLE_IN_8_0
gboolean
garrow_day_time_interval_array_builder_append_value(
  GArrowDayTimeIntervalArrayBuilder *builder,
  GArrowDayMillisecond *value,
  GError **error);
GARROW_AVAILABLE_IN_8_0
gboolean
garrow_day_time_interval_array_builder_append_values(
  GArrowDayTimeIntervalArrayBuilder *builder,
  const GArrowDayMillisecond **values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);


#define GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_ARRAY_BUILDER       \
  (garrow_month_day_nano_interval_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthDayNanoIntervalArrayBuilder,
                         garrow_month_day_nano_interval_array_builder,
                         GARROW,
                         MONTH_DAY_NANO_INTERVAL_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowMonthDayNanoIntervalArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthDayNanoIntervalArrayBuilder *
garrow_month_day_nano_interval_array_builder_new(void);

GARROW_AVAILABLE_IN_8_0
gboolean
garrow_month_day_nano_interval_array_builder_append_value(
  GArrowMonthDayNanoIntervalArrayBuilder *builder,
  GArrowMonthDayNano *value,
  GError **error);
GARROW_AVAILABLE_IN_8_0
gboolean
garrow_month_day_nano_interval_array_builder_append_values(
  GArrowMonthDayNanoIntervalArrayBuilder *builder,
  const GArrowMonthDayNano **values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);


#define GARROW_TYPE_BINARY_DICTIONARY_ARRAY_BUILDER (garrow_binary_dictionary_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBinaryDictionaryArrayBuilder,
                         garrow_binary_dictionary_array_builder,
                         GARROW,
                         BINARY_DICTIONARY_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowBinaryDictionaryArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_2_0
GArrowBinaryDictionaryArrayBuilder *
garrow_binary_dictionary_array_builder_new(void);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_append_null(GArrowBinaryDictionaryArrayBuilder *builder,
                                                   GError **error);
#endif
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_append_value(GArrowBinaryDictionaryArrayBuilder *builder,
                                                    const guint8 *value,
                                                    gint32 length,
                                                    GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_append_value_bytes(GArrowBinaryDictionaryArrayBuilder *builder,
                                                          GBytes *value,
                                                          GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_append_array(GArrowBinaryDictionaryArrayBuilder *builder,
                                                    GArrowBinaryArray *array,
                                                    GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_append_indices(GArrowBinaryDictionaryArrayBuilder *builder,
                                                      const gint64 *values,
                                                      gint64 values_length,
                                                      const gboolean *is_valids,
                                                      gint64 is_valids_length,
                                                      GError **error);
GARROW_AVAILABLE_IN_2_0
gint64
garrow_binary_dictionary_array_builder_get_dictionary_length(GArrowBinaryDictionaryArrayBuilder *builder);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_finish_delta(GArrowBinaryDictionaryArrayBuilder* builder,
                                                    GArrowArray **out_indices,
                                                    GArrowArray **out_delta,
                                                    GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_binary_dictionary_array_builder_insert_memo_values(GArrowBinaryDictionaryArrayBuilder *builder,
                                                          GArrowBinaryArray *values,
                                                          GError **error);
GARROW_AVAILABLE_IN_2_0
void
garrow_binary_dictionary_array_builder_reset_full(GArrowBinaryDictionaryArrayBuilder *builder);


#define GARROW_TYPE_STRING_DICTIONARY_ARRAY_BUILDER (garrow_string_dictionary_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStringDictionaryArrayBuilder,
                         garrow_string_dictionary_array_builder,
                         GARROW,
                         STRING_DICTIONARY_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowStringDictionaryArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_2_0
GArrowStringDictionaryArrayBuilder *
garrow_string_dictionary_array_builder_new(void);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_string_dictionary_array_builder_append_null(GArrowStringDictionaryArrayBuilder *builder,
                                                   GError **error);
#endif
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_string_dictionary_array_builder_append_string(GArrowStringDictionaryArrayBuilder *builder,
                                                     const gchar *value,
                                                     GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_string_dictionary_array_builder_append_array(GArrowStringDictionaryArrayBuilder *builder,
                                                    GArrowStringArray *array,
                                                    GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_string_dictionary_array_builder_append_indices(GArrowStringDictionaryArrayBuilder *builder,
                                                      const gint64 *values,
                                                      gint64 values_length,
                                                      const gboolean *is_valids,
                                                      gint64 is_valids_length,
                                                      GError **error);
GARROW_AVAILABLE_IN_2_0
gint64
garrow_string_dictionary_array_builder_get_dictionary_length(GArrowStringDictionaryArrayBuilder *builder);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_string_dictionary_array_builder_finish_delta(GArrowStringDictionaryArrayBuilder* builder,
                                                    GArrowArray **out_indices,
                                                    GArrowArray **out_delta,
                                                    GError **error);
GARROW_AVAILABLE_IN_2_0
gboolean
garrow_string_dictionary_array_builder_insert_memo_values(GArrowStringDictionaryArrayBuilder *builder,
                                                          GArrowStringArray *values,
                                                          GError **error);
GARROW_AVAILABLE_IN_2_0
void
garrow_string_dictionary_array_builder_reset_full(GArrowStringDictionaryArrayBuilder *builder);


#define GARROW_TYPE_LIST_ARRAY_BUILDER (garrow_list_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowListArrayBuilder,
                         garrow_list_array_builder,
                         GARROW,
                         LIST_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowListArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowListArrayBuilder *garrow_list_array_builder_new(GArrowListDataType *data_type,
                                                      GError **error);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_list_array_builder_append_value)
gboolean garrow_list_array_builder_append(GArrowListArrayBuilder *builder,
                                          GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_list_array_builder_append_value(GArrowListArrayBuilder *builder,
                                                GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_list_array_builder_append_null(GArrowListArrayBuilder *builder,
                                               GError **error);
#endif

GArrowArrayBuilder *garrow_list_array_builder_get_value_builder(GArrowListArrayBuilder *builder);


#define GARROW_TYPE_LARGE_LIST_ARRAY_BUILDER (garrow_large_list_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeListArrayBuilder,
                         garrow_large_list_array_builder,
                         GARROW,
                         LARGE_LIST_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowLargeListArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeListArrayBuilder *garrow_large_list_array_builder_new(GArrowLargeListDataType *data_type,
                                                                 GError **error);
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_list_array_builder_append_value(GArrowLargeListArrayBuilder *builder,
                                                      GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_0_16
gboolean garrow_large_list_array_builder_append_null(GArrowLargeListArrayBuilder *builder,
                                                     GError **error);
#endif
GARROW_AVAILABLE_IN_0_16
GArrowArrayBuilder *garrow_large_list_array_builder_get_value_builder(GArrowLargeListArrayBuilder *builder);


#define GARROW_TYPE_STRUCT_ARRAY_BUILDER        \
  (garrow_struct_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStructArrayBuilder,
                         garrow_struct_array_builder,
                         GARROW,
                         STRUCT_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowStructArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GArrowStructArrayBuilder *garrow_struct_array_builder_new(GArrowStructDataType *data_type,
                                                          GError **error);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_struct_array_builder_append_value)
gboolean garrow_struct_array_builder_append(GArrowStructArrayBuilder *builder,
                                            GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_struct_array_builder_append_value(GArrowStructArrayBuilder *builder,
                                                  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
gboolean garrow_struct_array_builder_append_null(GArrowStructArrayBuilder *builder,
                                                 GError **error);
#endif

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_12_0_FOR(garrow_array_builder_get_child)
GArrowArrayBuilder *
garrow_struct_array_builder_get_field_builder(GArrowStructArrayBuilder *builder,
                                              gint i);
GARROW_DEPRECATED_IN_12_0_FOR(garrow_array_builder_get_children)
GList *
garrow_struct_array_builder_get_field_builders(GArrowStructArrayBuilder *builder);
#endif


#define GARROW_TYPE_MAP_ARRAY_BUILDER        \
  (garrow_map_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMapArrayBuilder,
                         garrow_map_array_builder,
                         GARROW,
                         MAP_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowMapArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowMapArrayBuilder *garrow_map_array_builder_new(GArrowMapDataType *data_type,
                                                    GError **error);
GARROW_AVAILABLE_IN_0_17
gboolean
garrow_map_array_builder_append_value(GArrowMapArrayBuilder *builder,
                                      GError **error);
GARROW_AVAILABLE_IN_0_17
gboolean
garrow_map_array_builder_append_values(GArrowMapArrayBuilder *builder,
                                       const gint32 *offsets,
                                       gint64 offsets_length,
                                       const gboolean *is_valids,
                                       gint64 is_valids_length,
                                       GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_0_17
gboolean
garrow_map_array_builder_append_null(GArrowMapArrayBuilder *builder,
                                     GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_nulls)
GARROW_AVAILABLE_IN_0_17
gboolean
garrow_map_array_builder_append_nulls(GArrowMapArrayBuilder *builder,
                                      gint64 n,
                                      GError **error);
#endif
GARROW_AVAILABLE_IN_0_17
GArrowArrayBuilder *
garrow_map_array_builder_get_key_builder(GArrowMapArrayBuilder *builder);
GARROW_AVAILABLE_IN_0_17
GArrowArrayBuilder *
garrow_map_array_builder_get_item_builder(GArrowMapArrayBuilder *builder);
GARROW_AVAILABLE_IN_0_17
GArrowArrayBuilder *
garrow_map_array_builder_get_value_builder(GArrowMapArrayBuilder *builder);


#define GARROW_TYPE_DECIMAL128_ARRAY_BUILDER (garrow_decimal128_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128ArrayBuilder,
                         garrow_decimal128_array_builder,
                         GARROW,
                         DECIMAL128_ARRAY_BUILDER,
                         GArrowFixedSizeBinaryArrayBuilder)
struct _GArrowDecimal128ArrayBuilderClass
{
  GArrowFixedSizeBinaryArrayBuilderClass parent_class;
};

GArrowDecimal128ArrayBuilder *garrow_decimal128_array_builder_new(GArrowDecimal128DataType *data_type);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_12_FOR(garrow_decimal128_array_builder_append_value)
gboolean garrow_decimal128_array_builder_append(GArrowDecimal128ArrayBuilder *builder,
                                                GArrowDecimal128 *value,
                                                GError **error);
#endif
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_array_builder_append_value(GArrowDecimal128ArrayBuilder *builder,
                                                      GArrowDecimal128 *value,
                                                      GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal128_array_builder_append_values(
  GArrowDecimal128ArrayBuilder *builder,
  GArrowDecimal128 **values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_builder_append_null)
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_array_builder_append_null(GArrowDecimal128ArrayBuilder *builder,
                                                     GError **error);
#endif


#define GARROW_TYPE_DECIMAL256_ARRAY_BUILDER (garrow_decimal256_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal256ArrayBuilder,
                         garrow_decimal256_array_builder,
                         GARROW,
                         DECIMAL256_ARRAY_BUILDER,
                         GArrowFixedSizeBinaryArrayBuilder)
struct _GArrowDecimal256ArrayBuilderClass
{
  GArrowFixedSizeBinaryArrayBuilderClass parent_class;
};

GArrowDecimal256ArrayBuilder *garrow_decimal256_array_builder_new(GArrowDecimal256DataType *data_type);

GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_array_builder_append_value(GArrowDecimal256ArrayBuilder *builder,
                                                      GArrowDecimal256 *value,
                                                      GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_array_builder_append_values(
  GArrowDecimal256ArrayBuilder *builder,
  GArrowDecimal256 **values,
  gint64 values_length,
  const gboolean *is_valids,
  gint64 is_valids_length,
  GError **error);


#define GARROW_TYPE_UNION_ARRAY_BUILDER         \
  (garrow_union_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUnionArrayBuilder,
                         garrow_union_array_builder,
                         GARROW,
                         UNION_ARRAY_BUILDER,
                         GArrowArrayBuilder)
struct _GArrowUnionArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
gint8
garrow_union_array_builder_append_child(GArrowUnionArrayBuilder *builder,
                                        GArrowArrayBuilder *child,
                                        const gchar *filed_name);

GARROW_AVAILABLE_IN_12_0
gboolean
garrow_union_array_builder_append_value(GArrowUnionArrayBuilder *builder,
                                        gint8 value,
                                        GError **error);


#define GARROW_TYPE_DENSE_UNION_ARRAY_BUILDER   \
  (garrow_dense_union_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDenseUnionArrayBuilder,
                         garrow_dense_union_array_builder,
                         GARROW,
                         DENSE_UNION_ARRAY_BUILDER,
                         GArrowUnionArrayBuilder)
struct _GArrowDenseUnionArrayBuilderClass
{
  GArrowUnionArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
GArrowDenseUnionArrayBuilder *
garrow_dense_union_array_builder_new(GArrowDenseUnionDataType *data_type,
                                     GError **error);


#define GARROW_TYPE_SPARSE_UNION_ARRAY_BUILDER   \
  (garrow_sparse_union_array_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSparseUnionArrayBuilder,
                         garrow_sparse_union_array_builder,
                         GARROW,
                         SPARSE_UNION_ARRAY_BUILDER,
                         GArrowUnionArrayBuilder)
struct _GArrowSparseUnionArrayBuilderClass
{
  GArrowUnionArrayBuilderClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
GArrowSparseUnionArrayBuilder *
garrow_sparse_union_array_builder_new(GArrowSparseUnionDataType *data_type,
                                      GError **error);


G_END_DECLS
