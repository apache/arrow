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

G_BEGIN_DECLS

#define GARROW_TYPE_ARRAY_BUILDER               \
  (garrow_array_builder_get_type())
#define GARROW_ARRAY_BUILDER(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_ARRAY_BUILDER,        \
                              GArrowArrayBuilder))
#define GARROW_ARRAY_BUILDER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_ARRAY_BUILDER,   \
                           GArrowArrayBuilderClass))
#define GARROW_IS_ARRAY_BUILDER(obj)            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),            \
                              GARROW_TYPE_ARRAY_BUILDER))
#define GARROW_IS_ARRAY_BUILDER_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_ARRAY_BUILDER))
#define GARROW_ARRAY_BUILDER_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_ARRAY_BUILDER, \
                             GArrowArrayBuilderClass))

typedef struct _GArrowArrayBuilder         GArrowArrayBuilder;
typedef struct _GArrowArrayBuilderClass    GArrowArrayBuilderClass;

/**
 * GArrowArrayBuilder:
 *
 * It wraps `arrow::ArrayBuilder`.
 */
struct _GArrowArrayBuilder
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowArrayBuilderClass
{
  GObjectClass parent_class;
};

GType               garrow_array_builder_get_type (void) G_GNUC_CONST;

GArrowArray        *garrow_array_builder_finish   (GArrowArrayBuilder *builder);


#define GARROW_TYPE_BOOLEAN_ARRAY_BUILDER       \
  (garrow_boolean_array_builder_get_type())
#define GARROW_BOOLEAN_ARRAY_BUILDER(obj)                               \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_BOOLEAN_ARRAY_BUILDER,        \
                              GArrowBooleanArrayBuilder))
#define GARROW_BOOLEAN_ARRAY_BUILDER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BOOLEAN_ARRAY_BUILDER,   \
                           GArrowBooleanArrayBuilderClass))
#define GARROW_IS_BOOLEAN_ARRAY_BUILDER(obj)                            \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_BOOLEAN_ARRAY_BUILDER))
#define GARROW_IS_BOOLEAN_ARRAY_BUILDER_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BOOLEAN_ARRAY_BUILDER))
#define GARROW_BOOLEAN_ARRAY_BUILDER_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BOOLEAN_ARRAY_BUILDER, \
                             GArrowBooleanArrayBuilderClass))

typedef struct _GArrowBooleanArrayBuilder         GArrowBooleanArrayBuilder;
typedef struct _GArrowBooleanArrayBuilderClass    GArrowBooleanArrayBuilderClass;

/**
 * GArrowBooleanArrayBuilder:
 *
 * It wraps `arrow::BooleanBuilder`.
 */
struct _GArrowBooleanArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowBooleanArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_boolean_array_builder_get_type(void) G_GNUC_CONST;

GArrowBooleanArrayBuilder *garrow_boolean_array_builder_new(void);

gboolean garrow_boolean_array_builder_append(GArrowBooleanArrayBuilder *builder,
                                             gboolean value,
                                             GError **error);
gboolean garrow_boolean_array_builder_append_null(GArrowBooleanArrayBuilder *builder,
                                                  GError **error);


#define GARROW_TYPE_INT_ARRAY_BUILDER           \
  (garrow_int_array_builder_get_type())
#define GARROW_INT_ARRAY_BUILDER(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT_ARRAY_BUILDER,    \
                              GArrowIntArrayBuilder))
#define GARROW_INT_ARRAY_BUILDER_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_INT_ARRAY_BUILDER,       \
                           GArrowIntArrayBuilderClass))
#define GARROW_IS_INT_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT_ARRAY_BUILDER))
#define GARROW_IS_INT_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT_ARRAY_BUILDER))
#define GARROW_INT_ARRAY_BUILDER_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT_ARRAY_BUILDER,     \
                             GArrowIntArrayBuilderClass))

typedef struct _GArrowIntArrayBuilder         GArrowIntArrayBuilder;
typedef struct _GArrowIntArrayBuilderClass    GArrowIntArrayBuilderClass;

/**
 * GArrowIntArrayBuilder:
 *
 * It wraps `arrow::AdaptiveIntBuilder`.
 */
struct _GArrowIntArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowIntArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_int_array_builder_get_type(void) G_GNUC_CONST;

GArrowIntArrayBuilder *garrow_int_array_builder_new(void);

gboolean garrow_int_array_builder_append(GArrowIntArrayBuilder *builder,
                                         gint64 value,
                                         GError **error);
gboolean garrow_int_array_builder_append_null(GArrowIntArrayBuilder *builder,
                                              GError **error);


#define GARROW_TYPE_INT8_ARRAY_BUILDER          \
  (garrow_int8_array_builder_get_type())
#define GARROW_INT8_ARRAY_BUILDER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT8_ARRAY_BUILDER,   \
                              GArrowInt8ArrayBuilder))
#define GARROW_INT8_ARRAY_BUILDER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_INT8_ARRAY_BUILDER,      \
                           GArrowInt8ArrayBuilderClass))
#define GARROW_IS_INT8_ARRAY_BUILDER(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT8_ARRAY_BUILDER))
#define GARROW_IS_INT8_ARRAY_BUILDER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT8_ARRAY_BUILDER))
#define GARROW_INT8_ARRAY_BUILDER_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT8_ARRAY_BUILDER,    \
                             GArrowInt8ArrayBuilderClass))

typedef struct _GArrowInt8ArrayBuilder         GArrowInt8ArrayBuilder;
typedef struct _GArrowInt8ArrayBuilderClass    GArrowInt8ArrayBuilderClass;

/**
 * GArrowInt8ArrayBuilder:
 *
 * It wraps `arrow::Int8Builder`.
 */
struct _GArrowInt8ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowInt8ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_int8_array_builder_get_type(void) G_GNUC_CONST;

GArrowInt8ArrayBuilder *garrow_int8_array_builder_new(void);

gboolean garrow_int8_array_builder_append(GArrowInt8ArrayBuilder *builder,
                                          gint8 value,
                                          GError **error);
gboolean garrow_int8_array_builder_append_null(GArrowInt8ArrayBuilder *builder,
                                               GError **error);


#define GARROW_TYPE_UINT8_ARRAY_BUILDER         \
  (garrow_uint8_array_builder_get_type())
#define GARROW_UINT8_ARRAY_BUILDER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT8_ARRAY_BUILDER,  \
                              GArrowUInt8ArrayBuilder))
#define GARROW_UINT8_ARRAY_BUILDER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT8_ARRAY_BUILDER,     \
                           GArrowUInt8ArrayBuilderClass))
#define GARROW_IS_UINT8_ARRAY_BUILDER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_UINT8_ARRAY_BUILDER))
#define GARROW_IS_UINT8_ARRAY_BUILDER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT8_ARRAY_BUILDER))
#define GARROW_UINT8_ARRAY_BUILDER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT8_ARRAY_BUILDER,   \
                             GArrowUInt8ArrayBuilderClass))

typedef struct _GArrowUInt8ArrayBuilder         GArrowUInt8ArrayBuilder;
typedef struct _GArrowUInt8ArrayBuilderClass    GArrowUInt8ArrayBuilderClass;

/**
 * GArrowUInt8ArrayBuilder:
 *
 * It wraps `arrow::UInt8Builder`.
 */
struct _GArrowUInt8ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowUInt8ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_uint8_array_builder_get_type(void) G_GNUC_CONST;

GArrowUInt8ArrayBuilder *garrow_uint8_array_builder_new(void);

gboolean garrow_uint8_array_builder_append(GArrowUInt8ArrayBuilder *builder,
                                           guint8 value,
                                           GError **error);
gboolean garrow_uint8_array_builder_append_null(GArrowUInt8ArrayBuilder *builder,
                                                GError **error);


#define GARROW_TYPE_INT16_ARRAY_BUILDER         \
  (garrow_int16_array_builder_get_type())
#define GARROW_INT16_ARRAY_BUILDER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT16_ARRAY_BUILDER,  \
                              GArrowInt16ArrayBuilder))
#define GARROW_INT16_ARRAY_BUILDER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_INT16_ARRAY_BUILDER,     \
                           GArrowInt16ArrayBuilderClass))
#define GARROW_IS_INT16_ARRAY_BUILDER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT16_ARRAY_BUILDER))
#define GARROW_IS_INT16_ARRAY_BUILDER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT16_ARRAY_BUILDER))
#define GARROW_INT16_ARRAY_BUILDER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT16_ARRAY_BUILDER,   \
                             GArrowInt16ArrayBuilderClass))

typedef struct _GArrowInt16ArrayBuilder         GArrowInt16ArrayBuilder;
typedef struct _GArrowInt16ArrayBuilderClass    GArrowInt16ArrayBuilderClass;

/**
 * GArrowInt16ArrayBuilder:
 *
 * It wraps `arrow::Int16Builder`.
 */
struct _GArrowInt16ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowInt16ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_int16_array_builder_get_type(void) G_GNUC_CONST;

GArrowInt16ArrayBuilder *garrow_int16_array_builder_new(void);

gboolean garrow_int16_array_builder_append(GArrowInt16ArrayBuilder *builder,
                                           gint16 value,
                                           GError **error);
gboolean garrow_int16_array_builder_append_null(GArrowInt16ArrayBuilder *builder,
                                                GError **error);


#define GARROW_TYPE_UINT16_ARRAY_BUILDER        \
  (garrow_uint16_array_builder_get_type())
#define GARROW_UINT16_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT16_ARRAY_BUILDER, \
                              GArrowUInt16ArrayBuilder))
#define GARROW_UINT16_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT16_ARRAY_BUILDER,    \
                           GArrowUInt16ArrayBuilderClass))
#define GARROW_IS_UINT16_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_UINT16_ARRAY_BUILDER))
#define GARROW_IS_UINT16_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT16_ARRAY_BUILDER))
#define GARROW_UINT16_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT16_ARRAY_BUILDER,  \
                             GArrowUInt16ArrayBuilderClass))

typedef struct _GArrowUInt16ArrayBuilder         GArrowUInt16ArrayBuilder;
typedef struct _GArrowUInt16ArrayBuilderClass    GArrowUInt16ArrayBuilderClass;

/**
 * GArrowUInt16ArrayBuilder:
 *
 * It wraps `arrow::UInt16Builder`.
 */
struct _GArrowUInt16ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowUInt16ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_uint16_array_builder_get_type(void) G_GNUC_CONST;

GArrowUInt16ArrayBuilder *garrow_uint16_array_builder_new(void);

gboolean garrow_uint16_array_builder_append(GArrowUInt16ArrayBuilder *builder,
                                            guint16 value,
                                            GError **error);
gboolean garrow_uint16_array_builder_append_null(GArrowUInt16ArrayBuilder *builder,
                                                 GError **error);


#define GARROW_TYPE_INT32_ARRAY_BUILDER         \
  (garrow_int32_array_builder_get_type())
#define GARROW_INT32_ARRAY_BUILDER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT32_ARRAY_BUILDER,  \
                              GArrowInt32ArrayBuilder))
#define GARROW_INT32_ARRAY_BUILDER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_INT32_ARRAY_BUILDER,     \
                           GArrowInt32ArrayBuilderClass))
#define GARROW_IS_INT32_ARRAY_BUILDER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT32_ARRAY_BUILDER))
#define GARROW_IS_INT32_ARRAY_BUILDER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT32_ARRAY_BUILDER))
#define GARROW_INT32_ARRAY_BUILDER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT32_ARRAY_BUILDER,   \
                             GArrowInt32ArrayBuilderClass))

typedef struct _GArrowInt32ArrayBuilder         GArrowInt32ArrayBuilder;
typedef struct _GArrowInt32ArrayBuilderClass    GArrowInt32ArrayBuilderClass;

/**
 * GArrowInt32ArrayBuilder:
 *
 * It wraps `arrow::Int32Builder`.
 */
struct _GArrowInt32ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowInt32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_int32_array_builder_get_type(void) G_GNUC_CONST;

GArrowInt32ArrayBuilder *garrow_int32_array_builder_new(void);

gboolean garrow_int32_array_builder_append(GArrowInt32ArrayBuilder *builder,
                                           gint32 value,
                                           GError **error);
gboolean garrow_int32_array_builder_append_null(GArrowInt32ArrayBuilder *builder,
                                                GError **error);


#define GARROW_TYPE_UINT32_ARRAY_BUILDER        \
  (garrow_uint32_array_builder_get_type())
#define GARROW_UINT32_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT32_ARRAY_BUILDER, \
                              GArrowUInt32ArrayBuilder))
#define GARROW_UINT32_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT32_ARRAY_BUILDER,    \
                           GArrowUInt32ArrayBuilderClass))
#define GARROW_IS_UINT32_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_UINT32_ARRAY_BUILDER))
#define GARROW_IS_UINT32_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT32_ARRAY_BUILDER))
#define GARROW_UINT32_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT32_ARRAY_BUILDER,  \
                             GArrowUInt32ArrayBuilderClass))

typedef struct _GArrowUInt32ArrayBuilder         GArrowUInt32ArrayBuilder;
typedef struct _GArrowUInt32ArrayBuilderClass    GArrowUInt32ArrayBuilderClass;

/**
 * GArrowUInt32ArrayBuilder:
 *
 * It wraps `arrow::UInt32Builder`.
 */
struct _GArrowUInt32ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowUInt32ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_uint32_array_builder_get_type(void) G_GNUC_CONST;

GArrowUInt32ArrayBuilder *garrow_uint32_array_builder_new(void);

gboolean garrow_uint32_array_builder_append(GArrowUInt32ArrayBuilder *builder,
                                            guint32 value,
                                            GError **error);
gboolean garrow_uint32_array_builder_append_null(GArrowUInt32ArrayBuilder *builder,
                                                 GError **error);


#define GARROW_TYPE_INT64_ARRAY_BUILDER         \
  (garrow_int64_array_builder_get_type())
#define GARROW_INT64_ARRAY_BUILDER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_INT64_ARRAY_BUILDER,  \
                              GArrowInt64ArrayBuilder))
#define GARROW_INT64_ARRAY_BUILDER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_INT64_ARRAY_BUILDER,     \
                           GArrowInt64ArrayBuilderClass))
#define GARROW_IS_INT64_ARRAY_BUILDER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_INT64_ARRAY_BUILDER))
#define GARROW_IS_INT64_ARRAY_BUILDER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_INT64_ARRAY_BUILDER))
#define GARROW_INT64_ARRAY_BUILDER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_INT64_ARRAY_BUILDER,   \
                             GArrowInt64ArrayBuilderClass))

typedef struct _GArrowInt64ArrayBuilder         GArrowInt64ArrayBuilder;
typedef struct _GArrowInt64ArrayBuilderClass    GArrowInt64ArrayBuilderClass;

/**
 * GArrowInt64ArrayBuilder:
 *
 * It wraps `arrow::Int64Builder`.
 */
struct _GArrowInt64ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowInt64ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_int64_array_builder_get_type(void) G_GNUC_CONST;

GArrowInt64ArrayBuilder *garrow_int64_array_builder_new(void);

gboolean garrow_int64_array_builder_append(GArrowInt64ArrayBuilder *builder,
                                           gint64 value,
                                           GError **error);
gboolean garrow_int64_array_builder_append_null(GArrowInt64ArrayBuilder *builder,
                                                GError **error);


#define GARROW_TYPE_UINT64_ARRAY_BUILDER        \
  (garrow_uint64_array_builder_get_type())
#define GARROW_UINT64_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_UINT64_ARRAY_BUILDER, \
                              GArrowUInt64ArrayBuilder))
#define GARROW_UINT64_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_UINT64_ARRAY_BUILDER,    \
                           GArrowUInt64ArrayBuilderClass))
#define GARROW_IS_UINT64_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_UINT64_ARRAY_BUILDER))
#define GARROW_IS_UINT64_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_UINT64_ARRAY_BUILDER))
#define GARROW_UINT64_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_UINT64_ARRAY_BUILDER,  \
                             GArrowUInt64ArrayBuilderClass))

typedef struct _GArrowUInt64ArrayBuilder         GArrowUInt64ArrayBuilder;
typedef struct _GArrowUInt64ArrayBuilderClass    GArrowUInt64ArrayBuilderClass;

/**
 * GArrowUInt64ArrayBuilder:
 *
 * It wraps `arrow::UInt64Builder`.
 */
struct _GArrowUInt64ArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowUInt64ArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_uint64_array_builder_get_type(void) G_GNUC_CONST;

GArrowUInt64ArrayBuilder *garrow_uint64_array_builder_new(void);

gboolean garrow_uint64_array_builder_append(GArrowUInt64ArrayBuilder *builder,
                                            guint64 value,
                                            GError **error);
gboolean garrow_uint64_array_builder_append_null(GArrowUInt64ArrayBuilder *builder,
                                                 GError **error);


#define GARROW_TYPE_FLOAT_ARRAY_BUILDER         \
  (garrow_float_array_builder_get_type())
#define GARROW_FLOAT_ARRAY_BUILDER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_FLOAT_ARRAY_BUILDER,  \
                              GArrowFloatArrayBuilder))
#define GARROW_FLOAT_ARRAY_BUILDER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_FLOAT_ARRAY_BUILDER,     \
                           GArrowFloatArrayBuilderClass))
#define GARROW_IS_FLOAT_ARRAY_BUILDER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_FLOAT_ARRAY_BUILDER))
#define GARROW_IS_FLOAT_ARRAY_BUILDER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_FLOAT_ARRAY_BUILDER))
#define GARROW_FLOAT_ARRAY_BUILDER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_FLOAT_ARRAY_BUILDER,   \
                             GArrowFloatArrayBuilderClass))

typedef struct _GArrowFloatArrayBuilder         GArrowFloatArrayBuilder;
typedef struct _GArrowFloatArrayBuilderClass    GArrowFloatArrayBuilderClass;

/**
 * GArrowFloatArrayBuilder:
 *
 * It wraps `arrow::FloatBuilder`.
 */
struct _GArrowFloatArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowFloatArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_float_array_builder_get_type(void) G_GNUC_CONST;

GArrowFloatArrayBuilder *garrow_float_array_builder_new(void);

gboolean garrow_float_array_builder_append(GArrowFloatArrayBuilder *builder,
                                           gfloat value,
                                           GError **error);
gboolean garrow_float_array_builder_append_null(GArrowFloatArrayBuilder *builder,
                                                GError **error);


#define GARROW_TYPE_DOUBLE_ARRAY_BUILDER        \
  (garrow_double_array_builder_get_type())
#define GARROW_DOUBLE_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_DOUBLE_ARRAY_BUILDER, \
                              GArrowDoubleArrayBuilder))
#define GARROW_DOUBLE_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_DOUBLE_ARRAY_BUILDER,    \
                           GArrowDoubleArrayBuilderClass))
#define GARROW_IS_DOUBLE_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_DOUBLE_ARRAY_BUILDER))
#define GARROW_IS_DOUBLE_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_DOUBLE_ARRAY_BUILDER))
#define GARROW_DOUBLE_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_DOUBLE_ARRAY_BUILDER,  \
                             GArrowDoubleArrayBuilderClass))

typedef struct _GArrowDoubleArrayBuilder         GArrowDoubleArrayBuilder;
typedef struct _GArrowDoubleArrayBuilderClass    GArrowDoubleArrayBuilderClass;

/**
 * GArrowDoubleArrayBuilder:
 *
 * It wraps `arrow::DoubleBuilder`.
 */
struct _GArrowDoubleArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowDoubleArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_double_array_builder_get_type(void) G_GNUC_CONST;

GArrowDoubleArrayBuilder *garrow_double_array_builder_new(void);

gboolean garrow_double_array_builder_append(GArrowDoubleArrayBuilder *builder,
                                            gdouble value,
                                            GError **error);
gboolean garrow_double_array_builder_append_null(GArrowDoubleArrayBuilder *builder,
                                                 GError **error);


#define GARROW_TYPE_BINARY_ARRAY_BUILDER        \
  (garrow_binary_array_builder_get_type())
#define GARROW_BINARY_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BINARY_ARRAY_BUILDER, \
                              GArrowBinaryArrayBuilder))
#define GARROW_BINARY_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BINARY_ARRAY_BUILDER,    \
                           GArrowBinaryArrayBuilderClass))
#define GARROW_IS_BINARY_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_BINARY_ARRAY_BUILDER))
#define GARROW_IS_BINARY_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BINARY_ARRAY_BUILDER))
#define GARROW_BINARY_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BINARY_ARRAY_BUILDER,  \
                             GArrowBinaryArrayBuilderClass))

typedef struct _GArrowBinaryArrayBuilder         GArrowBinaryArrayBuilder;
typedef struct _GArrowBinaryArrayBuilderClass    GArrowBinaryArrayBuilderClass;

/**
 * GArrowBinaryArrayBuilder:
 *
 * It wraps `arrow::BinaryBuilder`.
 */
struct _GArrowBinaryArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowBinaryArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_binary_array_builder_get_type(void) G_GNUC_CONST;

GArrowBinaryArrayBuilder *garrow_binary_array_builder_new(void);

gboolean garrow_binary_array_builder_append(GArrowBinaryArrayBuilder *builder,
                                            const guint8 *value,
                                            gint32 length,
                                            GError **error);
gboolean garrow_binary_array_builder_append_null(GArrowBinaryArrayBuilder *builder,
                                                 GError **error);


#define GARROW_TYPE_STRING_ARRAY_BUILDER        \
  (garrow_string_array_builder_get_type())
#define GARROW_STRING_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_STRING_ARRAY_BUILDER, \
                              GArrowStringArrayBuilder))
#define GARROW_STRING_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_STRING_ARRAY_BUILDER,    \
                           GArrowStringArrayBuilderClass))
#define GARROW_IS_STRING_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_STRING_ARRAY_BUILDER))
#define GARROW_IS_STRING_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_STRING_ARRAY_BUILDER))
#define GARROW_STRING_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_STRING_ARRAY_BUILDER,  \
                             GArrowStringArrayBuilderClass))

typedef struct _GArrowStringArrayBuilder         GArrowStringArrayBuilder;
typedef struct _GArrowStringArrayBuilderClass    GArrowStringArrayBuilderClass;

/**
 * GArrowStringArrayBuilder:
 *
 * It wraps `arrow::StringBuilder`.
 */
struct _GArrowStringArrayBuilder
{
  /*< private >*/
  GArrowBinaryArrayBuilder parent_instance;
};

struct _GArrowStringArrayBuilderClass
{
  GArrowBinaryArrayBuilderClass parent_class;
};

GType garrow_string_array_builder_get_type(void) G_GNUC_CONST;

GArrowStringArrayBuilder *garrow_string_array_builder_new(void);

gboolean garrow_string_array_builder_append(GArrowStringArrayBuilder *builder,
                                            const gchar *value,
                                            GError **error);


#define GARROW_TYPE_LIST_ARRAY_BUILDER          \
  (garrow_list_array_builder_get_type())
#define GARROW_LIST_ARRAY_BUILDER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_LIST_ARRAY_BUILDER,   \
                              GArrowListArrayBuilder))
#define GARROW_LIST_ARRAY_BUILDER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_LIST_ARRAY_BUILDER,      \
                           GArrowListArrayBuilderClass))
#define GARROW_IS_LIST_ARRAY_BUILDER(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_LIST_ARRAY_BUILDER))
#define GARROW_IS_LIST_ARRAY_BUILDER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_LIST_ARRAY_BUILDER))
#define GARROW_LIST_ARRAY_BUILDER_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_LIST_ARRAY_BUILDER,    \
                             GArrowListArrayBuilderClass))

typedef struct _GArrowListArrayBuilder         GArrowListArrayBuilder;
typedef struct _GArrowListArrayBuilderClass    GArrowListArrayBuilderClass;

/**
 * GArrowListArrayBuilder:
 *
 * It wraps `arrow::ListBuilder`.
 */
struct _GArrowListArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowListArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_list_array_builder_get_type(void) G_GNUC_CONST;

GArrowListArrayBuilder *garrow_list_array_builder_new(GArrowListDataType *data_type,
                                                      GError **error);

gboolean garrow_list_array_builder_append(GArrowListArrayBuilder *builder,
                                          GError **error);
gboolean garrow_list_array_builder_append_null(GArrowListArrayBuilder *builder,
                                               GError **error);

GArrowArrayBuilder *garrow_list_array_builder_get_value_builder(GArrowListArrayBuilder *builder);


#define GARROW_TYPE_STRUCT_ARRAY_BUILDER        \
  (garrow_struct_array_builder_get_type())
#define GARROW_STRUCT_ARRAY_BUILDER(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_STRUCT_ARRAY_BUILDER, \
                              GArrowStructArrayBuilder))
#define GARROW_STRUCT_ARRAY_BUILDER_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_STRUCT_ARRAY_BUILDER,    \
                           GArrowStructArrayBuilderClass))
#define GARROW_IS_STRUCT_ARRAY_BUILDER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_STRUCT_ARRAY_BUILDER))
#define GARROW_IS_STRUCT_ARRAY_BUILDER_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_STRUCT_ARRAY_BUILDER))
#define GARROW_STRUCT_ARRAY_BUILDER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_STRUCT_ARRAY_BUILDER,  \
                             GArrowStructArrayBuilderClass))

typedef struct _GArrowStructArrayBuilder         GArrowStructArrayBuilder;
typedef struct _GArrowStructArrayBuilderClass    GArrowStructArrayBuilderClass;

/**
 * GArrowStructArrayBuilder:
 *
 * It wraps `arrow::StructBuilder`.
 */
struct _GArrowStructArrayBuilder
{
  /*< private >*/
  GArrowArrayBuilder parent_instance;
};

struct _GArrowStructArrayBuilderClass
{
  GArrowArrayBuilderClass parent_class;
};

GType garrow_struct_array_builder_get_type(void) G_GNUC_CONST;

GArrowStructArrayBuilder *garrow_struct_array_builder_new(GArrowStructDataType *data_type,
                                                          GError **error);

gboolean garrow_struct_array_builder_append(GArrowStructArrayBuilder *builder,
                                            GError **error);
gboolean garrow_struct_array_builder_append_null(GArrowStructArrayBuilder *builder,
                                                 GError **error);

GArrowArrayBuilder *garrow_struct_array_builder_get_field_builder(GArrowStructArrayBuilder *builder,
                                                                  gint i);
GList *garrow_struct_array_builder_get_field_builders(GArrowStructArrayBuilder *builder);

G_END_DECLS
