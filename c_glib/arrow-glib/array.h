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

#include <arrow-glib/buffer.h>
#include <arrow-glib/data-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_ARRAY \
  (garrow_array_get_type())
#define GARROW_ARRAY(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj), GARROW_TYPE_ARRAY, GArrowArray))
#define GARROW_ARRAY_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass), GARROW_TYPE_ARRAY, GArrowArrayClass))
#define GARROW_IS_ARRAY(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj), GARROW_TYPE_ARRAY))
#define GARROW_IS_ARRAY_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass), GARROW_TYPE_ARRAY))
#define GARROW_ARRAY_GET_CLASS(obj) \
  (G_TYPE_INSTANCE_GET_CLASS((obj), GARROW_TYPE_ARRAY, GArrowArrayClass))

typedef struct _GArrowArray         GArrowArray;
typedef struct _GArrowArrayClass    GArrowArrayClass;

/**
 * GArrowArray:
 *
 * It wraps `arrow::Array`.
 */
struct _GArrowArray
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowArrayClass
{
  GObjectClass parent_class;
};

GType          garrow_array_get_type    (void) G_GNUC_CONST;

gboolean       garrow_array_equal       (GArrowArray *array,
                                         GArrowArray *other_array);
gboolean       garrow_array_equal_approx(GArrowArray *array,
                                         GArrowArray *other_array);
gboolean       garrow_array_equal_range (GArrowArray *array,
                                         gint64 start_index,
                                         GArrowArray *other_array,
                                         gint64 other_start_index,
                                         gint64 end_index);

gboolean       garrow_array_is_null     (GArrowArray *array,
                                         gint64 i);
gint64         garrow_array_get_length  (GArrowArray *array);
gint64         garrow_array_get_offset  (GArrowArray *array);
gint64         garrow_array_get_n_nulls (GArrowArray *array);
GArrowBuffer  *garrow_array_get_null_bitmap(GArrowArray *array);
GArrowDataType *garrow_array_get_value_data_type(GArrowArray *array);
GArrowType     garrow_array_get_value_type(GArrowArray *array);
GArrowArray   *garrow_array_slice       (GArrowArray *array,
                                         gint64 offset,
                                         gint64 length);
gchar         *garrow_array_to_string   (GArrowArray *array,
                                         GError **error);

#define GARROW_TYPE_NULL_ARRAY                  \
  (garrow_null_array_get_type())
#define GARROW_NULL_ARRAY(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_NULL_ARRAY,   \
                              GArrowNullArray))
#define GARROW_NULL_ARRAY_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_NULL_ARRAY,      \
                           GArrowNullArrayClass))
#define GARROW_IS_NULL_ARRAY(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_NULL_ARRAY))
#define GARROW_IS_NULL_ARRAY_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_NULL_ARRAY))
#define GARROW_NULL_ARRAY_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_NULL_ARRAY,    \
                             GArrowNullArrayClass))

typedef struct _GArrowNullArray         GArrowNullArray;
typedef struct _GArrowNullArrayClass    GArrowNullArrayClass;

/**
 * GArrowNullArray:
 *
 * It wraps `arrow::NullArray`.
 */
struct _GArrowNullArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowNullArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_null_array_get_type(void) G_GNUC_CONST;

GArrowNullArray *garrow_null_array_new(gint64 length);


#define GARROW_TYPE_PRIMITIVE_ARRAY             \
  (garrow_primitive_array_get_type())
#define GARROW_PRIMITIVE_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_PRIMITIVE_ARRAY,      \
                              GArrowPrimitiveArray))
#define GARROW_PRIMITIVE_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_PRIMITIVE_ARRAY, \
                           GArrowPrimitiveArrayClass))
#define GARROW_IS_PRIMITIVE_ARRAY(obj)                          \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_PRIMITIVE_ARRAY))
#define GARROW_IS_PRIMITIVE_ARRAY_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_PRIMITIVE_ARRAY))
#define GARROW_PRIMITIVE_ARRAY_GET_CLASS(obj)                   \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_PRIMITIVE_ARRAY,       \
                             GArrowPrimitiveArrayClass))

typedef struct _GArrowPrimitiveArray         GArrowPrimitiveArray;
typedef struct _GArrowPrimitiveArrayClass    GArrowPrimitiveArrayClass;

/**
 * GArrowPrimitiveArray:
 *
 * It wraps `arrow::PrimitiveArray`.
 */
struct _GArrowPrimitiveArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowPrimitiveArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_primitive_array_get_type(void) G_GNUC_CONST;

GArrowBuffer *garrow_primitive_array_get_buffer(GArrowPrimitiveArray *array);


#define GARROW_TYPE_BOOLEAN_ARRAY               \
  (garrow_boolean_array_get_type())
#define GARROW_BOOLEAN_ARRAY(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BOOLEAN_ARRAY,        \
                              GArrowBooleanArray))
#define GARROW_BOOLEAN_ARRAY_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_BOOLEAN_ARRAY,   \
                           GArrowBooleanArrayClass))
#define GARROW_IS_BOOLEAN_ARRAY(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BOOLEAN_ARRAY))
#define GARROW_IS_BOOLEAN_ARRAY_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_BOOLEAN_ARRAY))
#define GARROW_BOOLEAN_ARRAY_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_BOOLEAN_ARRAY, \
                             GArrowBooleanArrayClass))

typedef struct _GArrowBooleanArray         GArrowBooleanArray;
typedef struct _GArrowBooleanArrayClass    GArrowBooleanArrayClass;

/**
 * GArrowBooleanArray:
 *
 * It wraps `arrow::BooleanArray`.
 */
struct _GArrowBooleanArray
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowBooleanArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType          garrow_boolean_array_get_type  (void) G_GNUC_CONST;

GArrowBooleanArray *garrow_boolean_array_new(gint64 length,
                                             GArrowBuffer *data,
                                             GArrowBuffer *null_bitmap,
                                             gint64 n_nulls);

gboolean       garrow_boolean_array_get_value (GArrowBooleanArray *array,
                                               gint64 i);
gboolean      *garrow_boolean_array_get_values(GArrowBooleanArray *array,
                                               gint64 *length);


#define GARROW_TYPE_INT8_ARRAY                  \
  (garrow_int8_array_get_type())
#define GARROW_INT8_ARRAY(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_INT8_ARRAY,   \
                              GArrowInt8Array))
#define GARROW_INT8_ARRAY_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT8_ARRAY,      \
                           GArrowInt8ArrayClass))
#define GARROW_IS_INT8_ARRAY(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_INT8_ARRAY))
#define GARROW_IS_INT8_ARRAY_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_INT8_ARRAY))
#define GARROW_INT8_ARRAY_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_INT8_ARRAY,    \
                             GArrowInt8ArrayClass))

typedef struct _GArrowInt8Array         GArrowInt8Array;
typedef struct _GArrowInt8ArrayClass    GArrowInt8ArrayClass;

/**
 * GArrowInt8Array:
 *
 * It wraps `arrow::Int8Array`.
 */
struct _GArrowInt8Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowInt8ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_int8_array_get_type(void) G_GNUC_CONST;

GArrowInt8Array *garrow_int8_array_new(gint64 length,
                                       GArrowBuffer *data,
                                       GArrowBuffer *null_bitmap,
                                       gint64 n_nulls);

gint8 garrow_int8_array_get_value(GArrowInt8Array *array,
                                  gint64 i);
const gint8 *garrow_int8_array_get_values(GArrowInt8Array *array,
                                          gint64 *length);


#define GARROW_TYPE_UINT8_ARRAY                 \
  (garrow_uint8_array_get_type())
#define GARROW_UINT8_ARRAY(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_UINT8_ARRAY,  \
                              GArrowUInt8Array))
#define GARROW_UINT8_ARRAY_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_UINT8_ARRAY,     \
                           GArrowUInt8ArrayClass))
#define GARROW_IS_UINT8_ARRAY(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_UINT8_ARRAY))
#define GARROW_IS_UINT8_ARRAY_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_UINT8_ARRAY))
#define GARROW_UINT8_ARRAY_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_UINT8_ARRAY,   \
                             GArrowUInt8ArrayClass))

typedef struct _GArrowUInt8Array         GArrowUInt8Array;
typedef struct _GArrowUInt8ArrayClass    GArrowUInt8ArrayClass;

/**
 * GArrowUInt8Array:
 *
 * It wraps `arrow::UInt8Array`.
 */
struct _GArrowUInt8Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowUInt8ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_uint8_array_get_type(void) G_GNUC_CONST;

GArrowUInt8Array *garrow_uint8_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls);

guint8 garrow_uint8_array_get_value(GArrowUInt8Array *array,
                                    gint64 i);
const guint8 *garrow_uint8_array_get_values(GArrowUInt8Array *array,
                                            gint64 *length);


#define GARROW_TYPE_INT16_ARRAY                  \
  (garrow_int16_array_get_type())
#define GARROW_INT16_ARRAY(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_INT16_ARRAY,  \
                              GArrowInt16Array))
#define GARROW_INT16_ARRAY_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT16_ARRAY,     \
                           GArrowInt16ArrayClass))
#define GARROW_IS_INT16_ARRAY(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_INT16_ARRAY))
#define GARROW_IS_INT16_ARRAY_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_INT16_ARRAY))
#define GARROW_INT16_ARRAY_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_INT16_ARRAY,   \
                             GArrowInt16ArrayClass))

typedef struct _GArrowInt16Array         GArrowInt16Array;
typedef struct _GArrowInt16ArrayClass    GArrowInt16ArrayClass;

/**
 * GArrowInt16Array:
 *
 * It wraps `arrow::Int16Array`.
 */
struct _GArrowInt16Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowInt16ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_int16_array_get_type(void) G_GNUC_CONST;

GArrowInt16Array *garrow_int16_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls);

gint16 garrow_int16_array_get_value(GArrowInt16Array *array,
                                    gint64 i);
const gint16 *garrow_int16_array_get_values(GArrowInt16Array *array,
                                            gint64 *length);


#define GARROW_TYPE_UINT16_ARRAY                 \
  (garrow_uint16_array_get_type())
#define GARROW_UINT16_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_UINT16_ARRAY, \
                              GArrowUInt16Array))
#define GARROW_UINT16_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_UINT16_ARRAY,    \
                           GArrowUInt16ArrayClass))
#define GARROW_IS_UINT16_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT16_ARRAY))
#define GARROW_IS_UINT16_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_UINT16_ARRAY))
#define GARROW_UINT16_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_UINT16_ARRAY,  \
                             GArrowUInt16ArrayClass))

typedef struct _GArrowUInt16Array         GArrowUInt16Array;
typedef struct _GArrowUInt16ArrayClass    GArrowUInt16ArrayClass;

/**
 * GArrowUInt16Array:
 *
 * It wraps `arrow::UInt16Array`.
 */
struct _GArrowUInt16Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowUInt16ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_uint16_array_get_type(void) G_GNUC_CONST;

GArrowUInt16Array *garrow_uint16_array_new(gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

guint16 garrow_uint16_array_get_value(GArrowUInt16Array *array,
                                      gint64 i);
const guint16 *garrow_uint16_array_get_values(GArrowUInt16Array *array,
                                              gint64 *length);


#define GARROW_TYPE_INT32_ARRAY                 \
  (garrow_int32_array_get_type())
#define GARROW_INT32_ARRAY(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_INT32_ARRAY,  \
                              GArrowInt32Array))
#define GARROW_INT32_ARRAY_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT32_ARRAY,     \
                           GArrowInt32ArrayClass))
#define GARROW_IS_INT32_ARRAY(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_INT32_ARRAY))
#define GARROW_IS_INT32_ARRAY_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_INT32_ARRAY))
#define GARROW_INT32_ARRAY_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_INT32_ARRAY,   \
                             GArrowInt32ArrayClass))

typedef struct _GArrowInt32Array         GArrowInt32Array;
typedef struct _GArrowInt32ArrayClass    GArrowInt32ArrayClass;

/**
 * GArrowInt32Array:
 *
 * It wraps `arrow::Int32Array`.
 */
struct _GArrowInt32Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowInt32ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_int32_array_get_type(void) G_GNUC_CONST;

GArrowInt32Array *garrow_int32_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls);

gint32 garrow_int32_array_get_value(GArrowInt32Array *array,
                                    gint64 i);
const gint32 *garrow_int32_array_get_values(GArrowInt32Array *array,
                                            gint64 *length);


#define GARROW_TYPE_UINT32_ARRAY                \
  (garrow_uint32_array_get_type())
#define GARROW_UINT32_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_UINT32_ARRAY, \
                              GArrowUInt32Array))
#define GARROW_UINT32_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_UINT32_ARRAY,    \
                           GArrowUInt32ArrayClass))
#define GARROW_IS_UINT32_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT32_ARRAY))
#define GARROW_IS_UINT32_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_UINT32_ARRAY))
#define GARROW_UINT32_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_UINT32_ARRAY,  \
                             GArrowUInt32ArrayClass))

typedef struct _GArrowUInt32Array         GArrowUInt32Array;
typedef struct _GArrowUInt32ArrayClass    GArrowUInt32ArrayClass;

/**
 * GArrowUInt32Array:
 *
 * It wraps `arrow::UInt32Array`.
 */
struct _GArrowUInt32Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowUInt32ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_uint32_array_get_type(void) G_GNUC_CONST;

GArrowUInt32Array *garrow_uint32_array_new(gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

guint32 garrow_uint32_array_get_value(GArrowUInt32Array *array,
                                      gint64 i);
const guint32 *garrow_uint32_array_get_values(GArrowUInt32Array *array,
                                              gint64 *length);


#define GARROW_TYPE_INT64_ARRAY                 \
  (garrow_int64_array_get_type())
#define GARROW_INT64_ARRAY(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_INT64_ARRAY,  \
                              GArrowInt64Array))
#define GARROW_INT64_ARRAY_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_INT64_ARRAY,     \
                           GArrowInt64ArrayClass))
#define GARROW_IS_INT64_ARRAY(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_INT64_ARRAY))
#define GARROW_IS_INT64_ARRAY_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_INT64_ARRAY))
#define GARROW_INT64_ARRAY_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_INT64_ARRAY,   \
                             GArrowInt64ArrayClass))

typedef struct _GArrowInt64Array         GArrowInt64Array;
typedef struct _GArrowInt64ArrayClass    GArrowInt64ArrayClass;

/**
 * GArrowInt64Array:
 *
 * It wraps `arrow::Int64Array`.
 */
struct _GArrowInt64Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowInt64ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_int64_array_get_type(void) G_GNUC_CONST;

GArrowInt64Array *garrow_int64_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls);

gint64 garrow_int64_array_get_value(GArrowInt64Array *array,
                                    gint64 i);
const gint64 *garrow_int64_array_get_values(GArrowInt64Array *array,
                                            gint64 *length);


#define GARROW_TYPE_UINT64_ARRAY                \
  (garrow_uint64_array_get_type())
#define GARROW_UINT64_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_UINT64_ARRAY, \
                              GArrowUInt64Array))
#define GARROW_UINT64_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_UINT64_ARRAY,    \
                           GArrowUInt64ArrayClass))
#define GARROW_IS_UINT64_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT64_ARRAY))
#define GARROW_IS_UINT64_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_UINT64_ARRAY))
#define GARROW_UINT64_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_UINT64_ARRAY,  \
                             GArrowUInt64ArrayClass))

typedef struct _GArrowUInt64Array         GArrowUInt64Array;
typedef struct _GArrowUInt64ArrayClass    GArrowUInt64ArrayClass;

/**
 * GArrowUInt64Array:
 *
 * It wraps `arrow::UInt64Array`.
 */
struct _GArrowUInt64Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowUInt64ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_uint64_array_get_type(void) G_GNUC_CONST;

GArrowUInt64Array *garrow_uint64_array_new(gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

guint64 garrow_uint64_array_get_value(GArrowUInt64Array *array,
                                      gint64 i);
const guint64 *garrow_uint64_array_get_values(GArrowUInt64Array *array,
                                              gint64 *length);


#define GARROW_TYPE_FLOAT_ARRAY                 \
  (garrow_float_array_get_type())
#define GARROW_FLOAT_ARRAY(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_FLOAT_ARRAY,  \
                              GArrowFloatArray))
#define GARROW_FLOAT_ARRAY_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_FLOAT_ARRAY,     \
                           GArrowFloatArrayClass))
#define GARROW_IS_FLOAT_ARRAY(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_FLOAT_ARRAY))
#define GARROW_IS_FLOAT_ARRAY_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_FLOAT_ARRAY))
#define GARROW_FLOAT_ARRAY_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_FLOAT_ARRAY,   \
                             GArrowFloatArrayClass))

typedef struct _GArrowFloatArray         GArrowFloatArray;
typedef struct _GArrowFloatArrayClass    GArrowFloatArrayClass;

/**
 * GArrowFloatArray:
 *
 * It wraps `arrow::FloatArray`.
 */
struct _GArrowFloatArray
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowFloatArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_float_array_get_type(void) G_GNUC_CONST;

GArrowFloatArray *garrow_float_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls);

gfloat garrow_float_array_get_value(GArrowFloatArray *array,
                                    gint64 i);
const gfloat *garrow_float_array_get_values(GArrowFloatArray *array,
                                            gint64 *length);


#define GARROW_TYPE_DOUBLE_ARRAY                \
  (garrow_double_array_get_type())
#define GARROW_DOUBLE_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_DOUBLE_ARRAY, \
                              GArrowDoubleArray))
#define GARROW_DOUBLE_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_DOUBLE_ARRAY,    \
                           GArrowDoubleArrayClass))
#define GARROW_IS_DOUBLE_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DOUBLE_ARRAY))
#define GARROW_IS_DOUBLE_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_DOUBLE_ARRAY))
#define GARROW_DOUBLE_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_DOUBLE_ARRAY,  \
                             GArrowDoubleArrayClass))

typedef struct _GArrowDoubleArray         GArrowDoubleArray;
typedef struct _GArrowDoubleArrayClass    GArrowDoubleArrayClass;

/**
 * GArrowDoubleArray:
 *
 * It wraps `arrow::DoubleArray`.
 */
struct _GArrowDoubleArray
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowDoubleArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_double_array_get_type(void) G_GNUC_CONST;

GArrowDoubleArray *garrow_double_array_new(gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

gdouble garrow_double_array_get_value(GArrowDoubleArray *array,
                                      gint64 i);
const gdouble *garrow_double_array_get_values(GArrowDoubleArray *array,
                                              gint64 *length);


#define GARROW_TYPE_BINARY_ARRAY                \
  (garrow_binary_array_get_type())
#define GARROW_BINARY_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_BINARY_ARRAY, \
                              GArrowBinaryArray))
#define GARROW_BINARY_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_BINARY_ARRAY,    \
                           GArrowBinaryArrayClass))
#define GARROW_IS_BINARY_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BINARY_ARRAY))
#define GARROW_IS_BINARY_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_BINARY_ARRAY))
#define GARROW_BINARY_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_BINARY_ARRAY,  \
                             GArrowBinaryArrayClass))

typedef struct _GArrowBinaryArray         GArrowBinaryArray;
typedef struct _GArrowBinaryArrayClass    GArrowBinaryArrayClass;

/**
 * GArrowBinaryArray:
 *
 * It wraps `arrow::BinaryArray`.
 */
struct _GArrowBinaryArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowBinaryArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_binary_array_get_type(void) G_GNUC_CONST;

GArrowBinaryArray *garrow_binary_array_new(gint64 length,
                                           GArrowBuffer *value_offsets,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

GBytes *garrow_binary_array_get_value(GArrowBinaryArray *array,
                                      gint64 i);
GArrowBuffer *garrow_binary_array_get_buffer(GArrowBinaryArray *array);
GArrowBuffer *garrow_binary_array_get_offsets_buffer(GArrowBinaryArray *array);

#define GARROW_TYPE_STRING_ARRAY                \
  (garrow_string_array_get_type())
#define GARROW_STRING_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_STRING_ARRAY, \
                              GArrowStringArray))
#define GARROW_STRING_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_STRING_ARRAY,    \
                           GArrowStringArrayClass))
#define GARROW_IS_STRING_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_STRING_ARRAY))
#define GARROW_IS_STRING_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_STRING_ARRAY))
#define GARROW_STRING_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_STRING_ARRAY,  \
                             GArrowStringArrayClass))

typedef struct _GArrowStringArray         GArrowStringArray;
typedef struct _GArrowStringArrayClass    GArrowStringArrayClass;

/**
 * GArrowStringArray:
 *
 * It wraps `arrow::StringArray`.
 */
struct _GArrowStringArray
{
  /*< private >*/
  GArrowBinaryArray parent_instance;
};

struct _GArrowStringArrayClass
{
  GArrowBinaryArrayClass parent_class;
};

GType garrow_string_array_get_type(void) G_GNUC_CONST;

GArrowStringArray *garrow_string_array_new(gint64 length,
                                           GArrowBuffer *value_offsets,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

gchar *garrow_string_array_get_string(GArrowStringArray *array,
                                      gint64 i);


#define GARROW_TYPE_DATE32_ARRAY                \
  (garrow_date32_array_get_type())
#define GARROW_DATE32_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_DATE32_ARRAY, \
                              GArrowDate32Array))
#define GARROW_DATE32_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_DATE32_ARRAY,    \
                           GArrowDate32ArrayClass))
#define GARROW_IS_DATE32_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DATE32_ARRAY))
#define GARROW_IS_DATE32_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_DATE32_ARRAY))
#define GARROW_DATE32_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_DATE32_ARRAY,  \
                             GArrowDate32ArrayClass))

typedef struct _GArrowDate32Array         GArrowDate32Array;
typedef struct _GArrowDate32ArrayClass    GArrowDate32ArrayClass;

/**
 * GArrowDate32Array:
 *
 * It wraps `arrow::Date32Array`.
 */
struct _GArrowDate32Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowDate32ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_date32_array_get_type(void) G_GNUC_CONST;

GArrowDate32Array *garrow_date32_array_new(gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

gint32 garrow_date32_array_get_value(GArrowDate32Array *array,
                                     gint64 i);
const gint32 *garrow_date32_array_get_values(GArrowDate32Array *array,
                                             gint64 *length);


#define GARROW_TYPE_DATE64_ARRAY                \
  (garrow_date64_array_get_type())
#define GARROW_DATE64_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_DATE64_ARRAY, \
                              GArrowDate64Array))
#define GARROW_DATE64_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_DATE64_ARRAY,    \
                           GArrowDate64ArrayClass))
#define GARROW_IS_DATE64_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DATE64_ARRAY))
#define GARROW_IS_DATE64_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_DATE64_ARRAY))
#define GARROW_DATE64_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_DATE64_ARRAY,  \
                             GArrowDate64ArrayClass))

typedef struct _GArrowDate64Array         GArrowDate64Array;
typedef struct _GArrowDate64ArrayClass    GArrowDate64ArrayClass;

/**
 * GArrowDate64Array:
 *
 * It wraps `arrow::Date64Array`.
 */
struct _GArrowDate64Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowDate64ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_date64_array_get_type(void) G_GNUC_CONST;

GArrowDate64Array *garrow_date64_array_new(gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

gint64 garrow_date64_array_get_value(GArrowDate64Array *array,
                                     gint64 i);
const gint64 *garrow_date64_array_get_values(GArrowDate64Array *array,
                                             gint64 *length);


#define GARROW_TYPE_TIMESTAMP_ARRAY             \
  (garrow_timestamp_array_get_type())
#define GARROW_TIMESTAMP_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIMESTAMP_ARRAY,      \
                              GArrowTimestampArray))
#define GARROW_TIMESTAMP_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_TIMESTAMP_ARRAY, \
                           GArrowTimestampArrayClass))
#define GARROW_IS_TIMESTAMP_ARRAY(obj)                          \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIMESTAMP_ARRAY))
#define GARROW_IS_TIMESTAMP_ARRAY_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIMESTAMP_ARRAY))
#define GARROW_TIMESTAMP_ARRAY_GET_CLASS(obj)                   \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIMESTAMP_ARRAY,       \
                             GArrowTimestampArrayClass))

typedef struct _GArrowTimestampArray         GArrowTimestampArray;
typedef struct _GArrowTimestampArrayClass    GArrowTimestampArrayClass;

/**
 * GArrowTimestampArray:
 *
 * It wraps `arrow::TimestampArray`.
 */
struct _GArrowTimestampArray
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowTimestampArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_timestamp_array_get_type(void) G_GNUC_CONST;

GArrowTimestampArray *garrow_timestamp_array_new(GArrowTimestampDataType *data_type,
                                                 gint64 length,
                                                 GArrowBuffer *data,
                                                 GArrowBuffer *null_bitmap,
                                                 gint64 n_nulls);

gint64 garrow_timestamp_array_get_value(GArrowTimestampArray *array,
                                        gint64 i);
const gint64 *garrow_timestamp_array_get_values(GArrowTimestampArray *array,
                                                gint64 *length);


#define GARROW_TYPE_TIME32_ARRAY                \
  (garrow_time32_array_get_type())
#define GARROW_TIME32_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_TIME32_ARRAY, \
                              GArrowTime32Array))
#define GARROW_TIME32_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_TIME32_ARRAY,    \
                           GArrowTime32ArrayClass))
#define GARROW_IS_TIME32_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME32_ARRAY))
#define GARROW_IS_TIME32_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_TIME32_ARRAY))
#define GARROW_TIME32_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_TIME32_ARRAY,  \
                             GArrowTime32ArrayClass))

typedef struct _GArrowTime32Array         GArrowTime32Array;
typedef struct _GArrowTime32ArrayClass    GArrowTime32ArrayClass;

/**
 * GArrowTime32Array:
 *
 * It wraps `arrow::Time32Array`.
 */
struct _GArrowTime32Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowTime32ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_time32_array_get_type(void) G_GNUC_CONST;

GArrowTime32Array *garrow_time32_array_new(GArrowTime32DataType *data_type,
                                           gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

gint32 garrow_time32_array_get_value(GArrowTime32Array *array,
                                     gint64 i);
const gint32 *garrow_time32_array_get_values(GArrowTime32Array *array,
                                             gint64 *length);


#define GARROW_TYPE_TIME64_ARRAY                \
  (garrow_time64_array_get_type())
#define GARROW_TIME64_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_TIME64_ARRAY, \
                              GArrowTime64Array))
#define GARROW_TIME64_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_TIME64_ARRAY,    \
                           GArrowTime64ArrayClass))
#define GARROW_IS_TIME64_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME64_ARRAY))
#define GARROW_IS_TIME64_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_TIME64_ARRAY))
#define GARROW_TIME64_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_TIME64_ARRAY,  \
                             GArrowTime64ArrayClass))

typedef struct _GArrowTime64Array         GArrowTime64Array;
typedef struct _GArrowTime64ArrayClass    GArrowTime64ArrayClass;

/**
 * GArrowTime64Array:
 *
 * It wraps `arrow::Time64Array`.
 */
struct _GArrowTime64Array
{
  /*< private >*/
  GArrowPrimitiveArray parent_instance;
};

struct _GArrowTime64ArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GType garrow_time64_array_get_type(void) G_GNUC_CONST;

GArrowTime64Array *garrow_time64_array_new(GArrowTime64DataType *data_type,
                                           gint64 length,
                                           GArrowBuffer *data,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

gint64 garrow_time64_array_get_value(GArrowTime64Array *array,
                                     gint64 i);
const gint64 *garrow_time64_array_get_values(GArrowTime64Array *array,
                                             gint64 *length);


#define GARROW_TYPE_LIST_ARRAY                  \
  (garrow_list_array_get_type())
#define GARROW_LIST_ARRAY(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_LIST_ARRAY,   \
                              GArrowListArray))
#define GARROW_LIST_ARRAY_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_LIST_ARRAY,      \
                           GArrowListArrayClass))
#define GARROW_IS_LIST_ARRAY(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                    \
                              GARROW_TYPE_LIST_ARRAY))
#define GARROW_IS_LIST_ARRAY_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_LIST_ARRAY))
#define GARROW_LIST_ARRAY_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_LIST_ARRAY,    \
                             GArrowListArrayClass))

typedef struct _GArrowListArray         GArrowListArray;
typedef struct _GArrowListArrayClass    GArrowListArrayClass;

/**
 * GArrowListArray:
 *
 * It wraps `arrow::ListArray`.
 */
struct _GArrowListArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowListArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_list_array_get_type(void) G_GNUC_CONST;

GArrowListArray *garrow_list_array_new(gint64 length,
                                       GArrowBuffer *value_offsets,
                                       GArrowArray *values,
                                       GArrowBuffer *null_bitmap,
                                       gint64 n_nulls);

GArrowDataType *garrow_list_array_get_value_type(GArrowListArray *array);
GArrowArray *garrow_list_array_get_value(GArrowListArray *array,
                                         gint64 i);


#define GARROW_TYPE_STRUCT_ARRAY                \
  (garrow_struct_array_get_type())
#define GARROW_STRUCT_ARRAY(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_STRUCT_ARRAY, \
                              GArrowStructArray))
#define GARROW_STRUCT_ARRAY_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_STRUCT_ARRAY,    \
                           GArrowStructArrayClass))
#define GARROW_IS_STRUCT_ARRAY(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_STRUCT_ARRAY))
#define GARROW_IS_STRUCT_ARRAY_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_STRUCT_ARRAY))
#define GARROW_STRUCT_ARRAY_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_STRUCT_ARRAY,  \
                             GArrowStructArrayClass))

typedef struct _GArrowStructArray         GArrowStructArray;
typedef struct _GArrowStructArrayClass    GArrowStructArrayClass;

/**
 * GArrowStructArray:
 *
 * It wraps `arrow::StructArray`.
 */
struct _GArrowStructArray
{
  /*< private >*/
  GArrowArray parent_instance;
};

struct _GArrowStructArrayClass
{
  GArrowArrayClass parent_class;
};

GType garrow_struct_array_get_type(void) G_GNUC_CONST;

GArrowStructArray *garrow_struct_array_new(GArrowDataType *data_type,
                                           gint64 length,
                                           GList *children,
                                           GArrowBuffer *null_bitmap,
                                           gint64 n_nulls);

GArrowArray *garrow_struct_array_get_field(GArrowStructArray *array,
                                           gint i);
GList *garrow_struct_array_get_fields(GArrowStructArray *array);

G_END_DECLS
