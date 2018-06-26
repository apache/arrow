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

#include <arrow-glib/gobject-type.h>
#include <arrow-glib/type.h>
#include <arrow-glib/decimal.h>

G_BEGIN_DECLS

#define GARROW_TYPE_DATA_TYPE (garrow_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDataType,
                         garrow_data_type,
                         GARROW,
                         DATA_TYPE,
                         GObject)
struct _GArrowDataTypeClass
{
  GObjectClass parent_class;
};

gboolean   garrow_data_type_equal     (GArrowDataType *data_type,
                                       GArrowDataType *other_data_type);
gchar     *garrow_data_type_to_string (GArrowDataType *data_type);
GArrowType garrow_data_type_get_id    (GArrowDataType *data_type);


#define GARROW_TYPE_FIXED_WIDTH_DATA_TYPE (garrow_fixed_width_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFixedWidthDataType,
                         garrow_fixed_width_data_type,
                         GARROW,
                         FIXED_WIDTH_DATA_TYPE,
                         GArrowDataType)
struct _GArrowFixedWidthDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

gint garrow_fixed_width_data_type_get_bit_width(GArrowFixedWidthDataType *data_type);
/* TODO:
GList *garrow_fixed_width_data_type_get_buffer_layout(GArrowFixedWidthDataType *data_type);
*/


#define GARROW_TYPE_NULL_DATA_TYPE              \
  (garrow_null_data_type_get_type())
#define GARROW_NULL_DATA_TYPE(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_NULL_DATA_TYPE,       \
                              GArrowNullDataType))
#define GARROW_NULL_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_NULL_DATA_TYPE,  \
                           GArrowNullDataTypeClass))
#define GARROW_IS_NULL_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_NULL_DATA_TYPE))
#define GARROW_IS_NULL_DATA_TYPE_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_NULL_DATA_TYPE))
#define GARROW_NULL_DATA_TYPE_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_NULL_DATA_TYPE,        \
                             GArrowNullDataTypeClass))

typedef struct _GArrowNullDataType         GArrowNullDataType;
typedef struct _GArrowNullDataTypeClass    GArrowNullDataTypeClass;

/**
 * GArrowNullDataType:
 *
 * It wraps `arrow::NullType`.
 */
struct _GArrowNullDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowNullDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType               garrow_null_data_type_get_type (void) G_GNUC_CONST;
GArrowNullDataType *garrow_null_data_type_new      (void);


#define GARROW_TYPE_BOOLEAN_DATA_TYPE (garrow_boolean_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBooleanDataType,
                         garrow_boolean_data_type,
                         GARROW,
                         BOOLEAN_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowBooleanDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};

GArrowBooleanDataType *garrow_boolean_data_type_new      (void);


#define GARROW_TYPE_NUMERIC_DATA_TYPE (garrow_numeric_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowNumericDataType,
                         garrow_numeric_data_type,
                         GARROW,
                         NUMERIC_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowNumericDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};


#define GARROW_TYPE_INTEGER_DATA_TYPE (garrow_integer_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowIntegerDataType,
                         garrow_integer_data_type,
                         GARROW,
                         INTEGER_DATA_TYPE,
                         GArrowNumericDataType)
struct _GArrowIntegerDataTypeClass
{
  GArrowNumericDataTypeClass parent_class;
};


#define GARROW_TYPE_INT8_DATA_TYPE (garrow_int8_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt8DataType,
                         garrow_int8_data_type,
                         GARROW,
                         INT8_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt8DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt8DataType   *garrow_int8_data_type_new      (void);


#define GARROW_TYPE_UINT8_DATA_TYPE (garrow_uint8_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt8DataType,
                         garrow_uint8_data_type,
                         GARROW,
                         UINT8_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt8DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt8DataType  *garrow_uint8_data_type_new      (void);


#define GARROW_TYPE_INT16_DATA_TYPE (garrow_int16_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt16DataType,
                         garrow_int16_data_type,
                         GARROW,
                         INT16_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt16DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt16DataType  *garrow_int16_data_type_new      (void);


#define GARROW_TYPE_UINT16_DATA_TYPE (garrow_uint16_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt16DataType,
                         garrow_uint16_data_type,
                         GARROW,
                         UINT16_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt16DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt16DataType *garrow_uint16_data_type_new      (void);


#define GARROW_TYPE_INT32_DATA_TYPE (garrow_int32_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt32DataType,
                         garrow_int32_data_type,
                         GARROW,
                         INT32_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt32DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt32DataType  *garrow_int32_data_type_new      (void);


#define GARROW_TYPE_UINT32_DATA_TYPE (garrow_uint32_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt32DataType,
                         garrow_uint32_data_type,
                         GARROW,
                         UINT32_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt32DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt32DataType *garrow_uint32_data_type_new      (void);


#define GARROW_TYPE_INT64_DATA_TYPE (garrow_int64_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt64DataType,
                         garrow_int64_data_type,
                         GARROW,
                         INT64_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt64DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt64DataType  *garrow_int64_data_type_new      (void);


#define GARROW_TYPE_UINT64_DATA_TYPE (garrow_uint64_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt64DataType,
                         garrow_uint64_data_type,
                         GARROW,
                         UINT64_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt64DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt64DataType *garrow_uint64_data_type_new      (void);


#define GARROW_TYPE_FLOATING_POINT_DATA_TYPE    \
  (garrow_floating_point_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFloatingPointDataType,
                         garrow_floating_point_data_type,
                         GARROW,
                         FLOATING_POINT_DATA_TYPE,
                         GArrowNumericDataType)
struct _GArrowFloatingPointDataTypeClass
{
  GArrowNumericDataTypeClass parent_class;
};


#define GARROW_TYPE_FLOAT_DATA_TYPE (garrow_float_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFloatDataType,
                         garrow_float_data_type,
                         GARROW,
                         FLOAT_DATA_TYPE,
                         GArrowFloatingPointDataType)
struct _GArrowFloatDataTypeClass
{
  GArrowFloatingPointDataTypeClass parent_class;
};

GArrowFloatDataType *garrow_float_data_type_new      (void);


#define GARROW_TYPE_DOUBLE_DATA_TYPE (garrow_double_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDoubleDataType,
                         garrow_double_data_type,
                         GARROW,
                         DOUBLE_DATA_TYPE,
                         GArrowFloatingPointDataType)
struct _GArrowDoubleDataTypeClass
{
  GArrowFloatingPointDataTypeClass parent_class;
};

GArrowDoubleDataType *garrow_double_data_type_new      (void);


#define GARROW_TYPE_BINARY_DATA_TYPE            \
  (garrow_binary_data_type_get_type())
#define GARROW_BINARY_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_BINARY_DATA_TYPE,     \
                              GArrowBinaryDataType))
#define GARROW_BINARY_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_BINARY_DATA_TYPE,        \
                           GArrowBinaryDataTypeClass))
#define GARROW_IS_BINARY_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_BINARY_DATA_TYPE))
#define GARROW_IS_BINARY_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_BINARY_DATA_TYPE))
#define GARROW_BINARY_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_BINARY_DATA_TYPE,      \
                             GArrowBinaryDataTypeClass))

typedef struct _GArrowBinaryDataType         GArrowBinaryDataType;
typedef struct _GArrowBinaryDataTypeClass    GArrowBinaryDataTypeClass;

/**
 * GArrowBinaryDataType:
 *
 * It wraps `arrow::BinaryType`.
 */
struct _GArrowBinaryDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowBinaryDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_binary_data_type_get_type (void) G_GNUC_CONST;
GArrowBinaryDataType *garrow_binary_data_type_new      (void);


#define GARROW_TYPE_STRING_DATA_TYPE            \
  (garrow_string_data_type_get_type())
#define GARROW_STRING_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                           \
                              GARROW_TYPE_STRING_DATA_TYPE,    \
                              GArrowStringDataType))
#define GARROW_STRING_DATA_TYPE_CLASS(klass)                   \
  (G_TYPE_CHECK_CLASS_CAST((klass),                            \
                           GARROW_TYPE_STRING_DATA_TYPE,       \
                           GArrowStringDataTypeClass))
#define GARROW_IS_STRING_DATA_TYPE(obj)                        \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                           \
                              GARROW_TYPE_STRING_DATA_TYPE))
#define GARROW_IS_STRING_DATA_TYPE_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                            \
                           GARROW_TYPE_STRING_DATA_TYPE))
#define GARROW_STRING_DATA_TYPE_GET_CLASS(obj)                 \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                            \
                             GARROW_TYPE_STRING_DATA_TYPE,     \
                             GArrowStringDataTypeClass))

typedef struct _GArrowStringDataType         GArrowStringDataType;
typedef struct _GArrowStringDataTypeClass    GArrowStringDataTypeClass;

/**
 * GArrowStringDataType:
 *
 * It wraps `arrow::StringType`.
 */
struct _GArrowStringDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowStringDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_string_data_type_get_type (void) G_GNUC_CONST;
GArrowStringDataType *garrow_string_data_type_new      (void);


#define GARROW_TYPE_DATE32_DATA_TYPE           \
  (garrow_date32_data_type_get_type())
#define GARROW_DATE32_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_DATE32_DATA_TYPE,     \
                              GArrowDate32DataType))
#define GARROW_DATE32_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_DATE32_DATA_TYPE,        \
                           GArrowDate32DataTypeClass))
#define GARROW_IS_DATE32_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DATE32_DATA_TYPE))
#define GARROW_IS_DATE32_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_DATE32_DATA_TYPE))
#define GARROW_DATE32_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_DATE32_DATA_TYPE,      \
                             GArrowDate32DataTypeClass))

typedef struct _GArrowDate32DataType         GArrowDate32DataType;
typedef struct _GArrowDate32DataTypeClass    GArrowDate32DataTypeClass;

/**
 * GArrowDate32DataType:
 *
 * It wraps `arrow::Date32Type`.
 */
struct _GArrowDate32DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowDate32DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_date32_data_type_get_type (void) G_GNUC_CONST;
GArrowDate32DataType *garrow_date32_data_type_new      (void);


#define GARROW_TYPE_DATE64_DATA_TYPE           \
  (garrow_date64_data_type_get_type())
#define GARROW_DATE64_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_DATE64_DATA_TYPE,     \
                              GArrowDate64DataType))
#define GARROW_DATE64_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_DATE64_DATA_TYPE,        \
                           GArrowDate64DataTypeClass))
#define GARROW_IS_DATE64_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_DATE64_DATA_TYPE))
#define GARROW_IS_DATE64_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_DATE64_DATA_TYPE))
#define GARROW_DATE64_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_DATE64_DATA_TYPE,      \
                             GArrowDate64DataTypeClass))

typedef struct _GArrowDate64DataType         GArrowDate64DataType;
typedef struct _GArrowDate64DataTypeClass    GArrowDate64DataTypeClass;

/**
 * GArrowDate64DataType:
 *
 * It wraps `arrow::Date64Type`.
 */
struct _GArrowDate64DataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowDate64DataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_date64_data_type_get_type (void) G_GNUC_CONST;
GArrowDate64DataType *garrow_date64_data_type_new      (void);


#define GARROW_TYPE_TIMESTAMP_DATA_TYPE         \
  (garrow_timestamp_data_type_get_type())
#define GARROW_TIMESTAMP_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIMESTAMP_DATA_TYPE,  \
                              GArrowTimestampDataType))
#define GARROW_TIMESTAMP_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_TIMESTAMP_DATA_TYPE,     \
                           GArrowTimestampDataTypeClass))
#define GARROW_IS_TIMESTAMP_DATA_TYPE(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIMESTAMP_DATA_TYPE))
#define GARROW_IS_TIMESTAMP_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIMESTAMP_DATA_TYPE))
#define GARROW_TIMESTAMP_DATA_TYPE_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIMESTAMP_DATA_TYPE,   \
                             GArrowTimestampDataTypeClass))

typedef struct _GArrowTimestampDataType         GArrowTimestampDataType;
typedef struct _GArrowTimestampDataTypeClass    GArrowTimestampDataTypeClass;

/**
 * GArrowTimestampDataType:
 *
 * It wraps `arrow::TimestampType`.
 */
struct _GArrowTimestampDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowTimestampDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType                 garrow_timestamp_data_type_get_type (void) G_GNUC_CONST;
GArrowTimestampDataType *garrow_timestamp_data_type_new   (GArrowTimeUnit unit);
GArrowTimeUnit
garrow_timestamp_data_type_get_unit (GArrowTimestampDataType *timestamp_data_type);


#define GARROW_TYPE_TIME_DATA_TYPE              \
  (garrow_time_data_type_get_type())
#define GARROW_TIME_DATA_TYPE(obj)                              \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIME_DATA_TYPE,       \
                              GArrowTimeDataType))
#define GARROW_TIME_DATA_TYPE_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_TIME_DATA_TYPE,  \
                           GArrowTimeDataTypeClass))
#define GARROW_IS_TIME_DATA_TYPE(obj)                           \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME_DATA_TYPE))
#define GARROW_IS_TIME_DATA_TYPE_CLASS(klass)           \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_TIME_DATA_TYPE))
#define GARROW_TIME_DATA_TYPE_GET_CLASS(obj)                    \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIME_DATA_TYPE,        \
                             GArrowTimeDataTypeClass))

typedef struct _GArrowTimeDataType         GArrowTimeDataType;
typedef struct _GArrowTimeDataTypeClass    GArrowTimeDataTypeClass;

/**
 * GArrowTimeDataType:
 *
 * It wraps `arrow::TimeType`.
 */
struct _GArrowTimeDataType
{
  /*< private >*/
  GArrowDataType parent_instance;
};

struct _GArrowTimeDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GType          garrow_time_data_type_get_type (void) G_GNUC_CONST;
GArrowTimeUnit garrow_time_data_type_get_unit (GArrowTimeDataType *time_data_type);


#define GARROW_TYPE_TIME32_DATA_TYPE           \
  (garrow_time32_data_type_get_type())
#define GARROW_TIME32_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIME32_DATA_TYPE,     \
                              GArrowTime32DataType))
#define GARROW_TIME32_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_TIME32_DATA_TYPE,        \
                           GArrowTime32DataTypeClass))
#define GARROW_IS_TIME32_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME32_DATA_TYPE))
#define GARROW_IS_TIME32_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIME32_DATA_TYPE))
#define GARROW_TIME32_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIME32_DATA_TYPE,      \
                             GArrowTime32DataTypeClass))

typedef struct _GArrowTime32DataType         GArrowTime32DataType;
typedef struct _GArrowTime32DataTypeClass    GArrowTime32DataTypeClass;

/**
 * GArrowTime32DataType:
 *
 * It wraps `arrow::Time32Type`.
 */
struct _GArrowTime32DataType
{
  /*< private >*/
  GArrowTimeDataType parent_instance;
};

struct _GArrowTime32DataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GType                 garrow_time32_data_type_get_type (void) G_GNUC_CONST;
GArrowTime32DataType *garrow_time32_data_type_new      (GArrowTimeUnit unit,
                                                        GError **error);


#define GARROW_TYPE_TIME64_DATA_TYPE           \
  (garrow_time64_data_type_get_type())
#define GARROW_TIME64_DATA_TYPE(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_TIME64_DATA_TYPE,     \
                              GArrowTime64DataType))
#define GARROW_TIME64_DATA_TYPE_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_TIME64_DATA_TYPE,        \
                           GArrowTime64DataTypeClass))
#define GARROW_IS_TIME64_DATA_TYPE(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_TIME64_DATA_TYPE))
#define GARROW_IS_TIME64_DATA_TYPE_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_TIME64_DATA_TYPE))
#define GARROW_TIME64_DATA_TYPE_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_TIME64_DATA_TYPE,      \
                             GArrowTime64DataTypeClass))

typedef struct _GArrowTime64DataType         GArrowTime64DataType;
typedef struct _GArrowTime64DataTypeClass    GArrowTime64DataTypeClass;

/**
 * GArrowTime64DataType:
 *
 * It wraps `arrow::Time64Type`.
 */
struct _GArrowTime64DataType
{
  /*< private >*/
  GArrowTimeDataType parent_instance;
};

struct _GArrowTime64DataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GType                 garrow_time64_data_type_get_type (void) G_GNUC_CONST;
GArrowTime64DataType *garrow_time64_data_type_new      (GArrowTimeUnit unit,
                                                        GError **error);


#define GARROW_TYPE_DECIMAL_DATA_TYPE (garrow_decimal_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimalDataType,
                         garrow_decimal_data_type,
                         GARROW,
                         DECIMAL_DATA_TYPE,
                         GArrowDataType)
struct _GArrowDecimalDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GArrowDecimalDataType   *garrow_decimal_data_type_new     (gint32 precision,
                                                           gint32 scale);

G_END_DECLS
