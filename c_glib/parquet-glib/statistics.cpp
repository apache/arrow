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

#include <arrow-glib/arrow-glib.hpp>

#include <parquet-glib/statistics.hpp>

#include <parquet/schema.h>

G_BEGIN_DECLS

/**
 * SECTION: statistics
 * @title: Statistics related classes
 * @include: parquet-glib/parquet-glib.h
 *
 * #GParquetStatistics is a base class for statistics classes such as
 * #GParquetInt32Statistics.
 *
 * #GParquetBooleanStatistics is a class for boolean statistics.
 *
 * #GParquetInt32Statistics is a class for 32-bit integer statistics.
 *
 * #GParquetInt64Statistics is a class for 64-bit integer statistics.
 *
 * #GParquetFloatStatistics is a class for 32-bit floating point
 * number statistics.
 *
 * #GParquetDoubleStatistics is a class for 64-bit floating point
 * number statistics.
 *
 * #GParquetByteArrayStatistics is a class for byte array statistics.
 *
 * #GParquetFixedLengthByteArrayStatistics is a class for fixed length
 * byte array statistics.
 */

struct GParquetStatisticsPrivate {
  std::shared_ptr<parquet::Statistics> statistics;
};

enum {
  PROP_STATISTICS = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GParquetStatistics,
                                    gparquet_statistics,
                                    G_TYPE_OBJECT)

#define GPARQUET_STATISTICS_GET_PRIVATE(object)       \
  static_cast<GParquetStatisticsPrivate *>(           \
    gparquet_statistics_get_instance_private(         \
      GPARQUET_STATISTICS(object)))

static void
gparquet_statistics_finalize(GObject *object)
{
  auto priv = GPARQUET_STATISTICS_GET_PRIVATE(object);
  priv->statistics.~shared_ptr();
  G_OBJECT_CLASS(gparquet_statistics_parent_class)->finalize(object);
}

static void
gparquet_statistics_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GPARQUET_STATISTICS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STATISTICS:
    priv->statistics =
      *static_cast<std::shared_ptr<parquet::Statistics> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gparquet_statistics_init(GParquetStatistics *object)
{
  auto priv = GPARQUET_STATISTICS_GET_PRIVATE(object);
  new(&priv->statistics) std::shared_ptr<parquet::Statistics>;
}

static void
gparquet_statistics_class_init(GParquetStatisticsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gparquet_statistics_finalize;
  gobject_class->set_property = gparquet_statistics_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("statistics",
                              "Statistics",
                              "The raw std::shared_ptr<parquet::Statistics>",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STATISTICS, spec);
}

/**
 * gparquet_statistics_equal:
 * @statistics: A #GParquetStatistics.
 * @other_statistics: A #GParquetStatistics.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_statistics_equal(GParquetStatistics *statistics,
                          GParquetStatistics *other_statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  auto parquet_other_statistics = gparquet_statistics_get_raw(other_statistics);
  return parquet_statistics->Equals(*parquet_other_statistics);
}

/**
 * gparquet_statistics_has_n_nulls:
 * @statistics: A #GParquetStatistics.
 *
 * Returns: %TRUE if the number of null values is set, %FALSE otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_statistics_has_n_nulls(GParquetStatistics *statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  return parquet_statistics->HasNullCount();
}

/**
 * gparquet_statistics_get_n_nulls:
 * @statistics: A #GParquetStatistics.
 *
 * Returns: The number of null values.
 *
 * Since: 8.0.0
 */
gint64
gparquet_statistics_get_n_nulls(GParquetStatistics *statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  return parquet_statistics->null_count();
}

/**
 * gparquet_statistics_has_n_distinct_valuess:
 * @statistics: A #GParquetStatistics.
 *
 * Returns: %TRUE if the number of distinct values is set, %FALSE otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_statistics_has_n_distinct_values(GParquetStatistics *statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  return parquet_statistics->HasDistinctCount();
}

/**
 * gparquet_statistics_get_n_distinct_values:
 * @statistics: A #GParquetStatistics.
 *
 * Returns: The number of distinct values.
 *
 * Since: 8.0.0
 */
gint64
gparquet_statistics_get_n_distinct_values(GParquetStatistics *statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  return parquet_statistics->distinct_count();
}

/**
 * gparquet_statistics_get_n_values:
 * @statistics: A #GParquetStatistics.
 *
 * Returns: The number of values.
 *
 * Since: 8.0.0
 */
gint64
gparquet_statistics_get_n_values(GParquetStatistics *statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  return parquet_statistics->num_values();
}

/**
 * gparquet_statistics_has_min_max:
 * @statistics: A #GParquetStatistics.
 *
 * Returns: %TRUE if the min and max statistics are set, %FALSE otherwise.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_statistics_has_min_max(GParquetStatistics *statistics)
{
  auto parquet_statistics = gparquet_statistics_get_raw(statistics);
  return parquet_statistics->HasMinMax();
}


G_END_DECLS
namespace {
  template <typename ParquetStatisticsClass, typename GParquetStatisticsClass>
  typename std::shared_ptr<ParquetStatisticsClass>
  gparquet_typed_statistics_get_raw(GParquetStatisticsClass *statistics)
  {
    return
      std::static_pointer_cast<ParquetStatisticsClass>(
        gparquet_statistics_get_raw(GPARQUET_STATISTICS(statistics)));
  }

  template <typename ParquetStatisticsClass, typename GParquetStatisticsClass>
  const typename ParquetStatisticsClass::T &
  gparquet_typed_statistics_get_min(GParquetStatisticsClass *statistics)
  {
    auto parquet_statistics =
      gparquet_typed_statistics_get_raw<ParquetStatisticsClass>(statistics);
    return parquet_statistics->min();
  }

  template <typename ParquetStatisticsClass, typename GParquetStatisticsClass>
  const typename ParquetStatisticsClass::T &
  gparquet_typed_statistics_get_max(GParquetStatisticsClass *statistics)
  {
    auto parquet_statistics =
      gparquet_typed_statistics_get_raw<ParquetStatisticsClass>(statistics);
    return parquet_statistics->max();
  }
}
G_BEGIN_DECLS


G_DEFINE_TYPE(GParquetBooleanStatistics,
              gparquet_boolean_statistics,
              GPARQUET_TYPE_STATISTICS)

static void
gparquet_boolean_statistics_init(GParquetBooleanStatistics *object)
{
}

static void
gparquet_boolean_statistics_class_init(GParquetBooleanStatisticsClass *klass)
{
}

/**
 * gparquet_boolean_statistics_get_min:
 * @statistics: A #GParquetBooleanStatistics.
 *
 * Returns: The minimum value.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_boolean_statistics_get_min(GParquetBooleanStatistics *statistics)
{
  return gparquet_typed_statistics_get_min<parquet::BoolStatistics>(statistics);
}

/**
 * gparquet_boolean_statistics_get_max:
 * @statistics: A #GParquetBooleanStatistics.
 *
 * Returns: The maximum value.
 *
 * Since: 8.0.0
 */
gboolean
gparquet_boolean_statistics_get_max(GParquetBooleanStatistics *statistics)
{
  return gparquet_typed_statistics_get_max<parquet::BoolStatistics>(statistics);
}


G_DEFINE_TYPE(GParquetInt32Statistics,
              gparquet_int32_statistics,
              GPARQUET_TYPE_STATISTICS)

static void
gparquet_int32_statistics_init(GParquetInt32Statistics *object)
{
}

static void
gparquet_int32_statistics_class_init(GParquetInt32StatisticsClass *klass)
{
}

/**
 * gparquet_int32_statistics_get_min:
 * @statistics: A #GParquetInt32Statistics.
 *
 * Returns: The minimum value.
 *
 * Since: 8.0.0
 */
gint32
gparquet_int32_statistics_get_min(GParquetInt32Statistics *statistics)
{
  return gparquet_typed_statistics_get_min<parquet::Int32Statistics>(statistics);
}

/**
 * gparquet_int32_statistics_get_max:
 * @statistics: A #GParquetInt32Statistics.
 *
 * Returns: The maximum value.
 *
 * Since: 8.0.0
 */
gint32
gparquet_int32_statistics_get_max(GParquetInt32Statistics *statistics)
{
  return gparquet_typed_statistics_get_max<parquet::Int32Statistics>(statistics);
}


G_DEFINE_TYPE(GParquetInt64Statistics,
              gparquet_int64_statistics,
              GPARQUET_TYPE_STATISTICS)

static void
gparquet_int64_statistics_init(GParquetInt64Statistics *object)
{
}

static void
gparquet_int64_statistics_class_init(GParquetInt64StatisticsClass *klass)
{
}

/**
 * gparquet_int64_statistics_get_min:
 * @statistics: A #GParquetInt64Statistics.
 *
 * Returns: The minimum value.
 *
 * Since: 8.0.0
 */
gint64
gparquet_int64_statistics_get_min(GParquetInt64Statistics *statistics)
{
  return gparquet_typed_statistics_get_min<parquet::Int64Statistics>(statistics);
}

/**
 * gparquet_int64_statistics_get_max:
 * @statistics: A #GParquetInt64Statistics.
 *
 * Returns: The maximum value.
 *
 * Since: 8.0.0
 */
gint64
gparquet_int64_statistics_get_max(GParquetInt64Statistics *statistics)
{
  return gparquet_typed_statistics_get_max<parquet::Int64Statistics>(statistics);
}


G_DEFINE_TYPE(GParquetFloatStatistics,
              gparquet_float_statistics,
              GPARQUET_TYPE_STATISTICS)

static void
gparquet_float_statistics_init(GParquetFloatStatistics *object)
{
}

static void
gparquet_float_statistics_class_init(GParquetFloatStatisticsClass *klass)
{
}

/**
 * gparquet_float_statistics_get_min:
 * @statistics: A #GParquetFloatStatistics.
 *
 * Returns: The minimum value.
 *
 * Since: 8.0.0
 */
gfloat
gparquet_float_statistics_get_min(GParquetFloatStatistics *statistics)
{
  return gparquet_typed_statistics_get_min<parquet::FloatStatistics>(statistics);
}

/**
 * gparquet_float_statistics_get_max:
 * @statistics: A #GParquetFloatStatistics.
 *
 * Returns: The maximum value.
 *
 * Since: 8.0.0
 */
gfloat
gparquet_float_statistics_get_max(GParquetFloatStatistics *statistics)
{
  return gparquet_typed_statistics_get_max<parquet::FloatStatistics>(statistics);
}


G_DEFINE_TYPE(GParquetDoubleStatistics,
              gparquet_double_statistics,
              GPARQUET_TYPE_STATISTICS)

static void
gparquet_double_statistics_init(GParquetDoubleStatistics *object)
{
}

static void
gparquet_double_statistics_class_init(GParquetDoubleStatisticsClass *klass)
{
}

/**
 * gparquet_double_statistics_get_min:
 * @statistics: A #GParquetDoubleStatistics.
 *
 * Returns: The minimum value.
 *
 * Since: 8.0.0
 */
gdouble
gparquet_double_statistics_get_min(GParquetDoubleStatistics *statistics)
{
  return gparquet_typed_statistics_get_min<parquet::DoubleStatistics>(statistics);
}

/**
 * gparquet_double_statistics_get_max:
 * @statistics: A #GParquetDoubleStatistics.
 *
 * Returns: The maximum value.
 *
 * Since: 8.0.0
 */
gdouble
gparquet_double_statistics_get_max(GParquetDoubleStatistics *statistics)
{
  return gparquet_typed_statistics_get_max<parquet::DoubleStatistics>(statistics);
}


struct GParquetByteArrayStatisticsPrivate {
  GBytes *min;
  GBytes *max;
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetByteArrayStatistics,
                           gparquet_byte_array_statistics,
                           GPARQUET_TYPE_STATISTICS)

#define GPARQUET_BYTE_ARRAY_STATISTICS_GET_PRIVATE(object)      \
  static_cast<GParquetByteArrayStatisticsPrivate *>(            \
    gparquet_byte_array_statistics_get_instance_private(        \
      GPARQUET_BYTE_ARRAY_STATISTICS(object)))

static void
gparquet_byte_array_statistics_dispose(GObject *object)
{
  auto priv = GPARQUET_BYTE_ARRAY_STATISTICS_GET_PRIVATE(object);

  if (priv->min) {
    g_bytes_unref(priv->min);
    priv->min = nullptr;
  }

  if (priv->max) {
    g_bytes_unref(priv->max);
    priv->max = nullptr;
  }

  G_OBJECT_CLASS(gparquet_byte_array_statistics_parent_class)->dispose(object);
}

static void
gparquet_byte_array_statistics_init(GParquetByteArrayStatistics *object)
{
}

static void
gparquet_byte_array_statistics_class_init(
  GParquetByteArrayStatisticsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = gparquet_byte_array_statistics_dispose;
}

/**
 * gparquet_byte_array_statistics_get_min:
 * @statistics: A #GParquetByteArrayStatistics.
 *
 * Returns: (transfer none): The minimum value.
 *
 * Since: 8.0.0
 */
GBytes *
gparquet_byte_array_statistics_get_min(GParquetByteArrayStatistics *statistics)
{
  auto priv = GPARQUET_BYTE_ARRAY_STATISTICS_GET_PRIVATE(statistics);
  if (!priv->min) {
    const auto& parquet_min =
      gparquet_typed_statistics_get_min<
        parquet::ByteArrayStatistics>(statistics);
    priv->min = g_bytes_new_static(parquet_min.ptr, parquet_min.len);
  }
  return priv->min;
}

/**
 * gparquet_byte_array_statistics_get_max:
 * @statistics: A #GParquetByteArrayStatistics.
 *
 * Returns: (transfer none): The maximum value.
 *
 * Since: 8.0.0
 */
GBytes *
gparquet_byte_array_statistics_get_max(GParquetByteArrayStatistics *statistics)
{
  auto priv = GPARQUET_BYTE_ARRAY_STATISTICS_GET_PRIVATE(statistics);
  if (!priv->max) {
    const auto& parquet_max =
      gparquet_typed_statistics_get_max<
        parquet::ByteArrayStatistics>(statistics);
    priv->max = g_bytes_new_static(parquet_max.ptr, parquet_max.len);
  }
  return priv->max;
}


struct GParquetFixedLengthByteArrayStatisticsPrivate {
  GBytes *min;
  GBytes *max;
};

G_DEFINE_TYPE_WITH_PRIVATE(GParquetFixedLengthByteArrayStatistics,
                           gparquet_fixed_length_byte_array_statistics,
                           GPARQUET_TYPE_STATISTICS)

#define GPARQUET_FIXED_LENGTH_BYTE_ARRAY_STATISTICS_GET_PRIVATE(object) \
  static_cast<GParquetFixedLengthByteArrayStatisticsPrivate *>(         \
    gparquet_fixed_length_byte_array_statistics_get_instance_private(   \
      GPARQUET_FIXED_LENGTH_BYTE_ARRAY_STATISTICS(object)))

static void
gparquet_fixed_length_byte_array_statistics_dispose(GObject *object)
{
  auto priv = GPARQUET_FIXED_LENGTH_BYTE_ARRAY_STATISTICS_GET_PRIVATE(object);

  if (priv->min) {
    g_bytes_unref(priv->min);
    priv->min = nullptr;
  }

  if (priv->max) {
    g_bytes_unref(priv->max);
    priv->max = nullptr;
  }

  G_OBJECT_CLASS(gparquet_fixed_length_byte_array_statistics_parent_class)->
    dispose(object);
}

static void
gparquet_fixed_length_byte_array_statistics_init(
  GParquetFixedLengthByteArrayStatistics *object)
{
}

static void
gparquet_fixed_length_byte_array_statistics_class_init(
  GParquetFixedLengthByteArrayStatisticsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = gparquet_fixed_length_byte_array_statistics_dispose;
}

/**
 * gparquet_fixed_length_byte_array_statistics_get_min:
 * @statistics: A #GParquetFixedLengthByteArrayStatistics.
 *
 * Returns: (transfer none): The minimum value.
 *
 * Since: 8.0.0
 */
GBytes *
gparquet_fixed_length_byte_array_statistics_get_min(
  GParquetFixedLengthByteArrayStatistics *statistics)
{
  auto priv =
    GPARQUET_FIXED_LENGTH_BYTE_ARRAY_STATISTICS_GET_PRIVATE(statistics);
  if (!priv->min) {
    auto parquet_statistics =
      gparquet_typed_statistics_get_raw<parquet::FLBAStatistics>(statistics);
    priv->min = g_bytes_new_static(parquet_statistics->min().ptr,
                                   parquet_statistics->descr()->type_length());
  }
  return priv->min;
}

/**
 * gparquet_fixed_length_byte_array_statistics_get_max:
 * @statistics: A #GParquetFixedLengthByteArrayStatistics.
 *
 * Returns: (transfer none): The maximum value.
 *
 * Since: 8.0.0
 */
GBytes *
gparquet_fixed_length_byte_array_statistics_get_max(
  GParquetFixedLengthByteArrayStatistics *statistics)
{
  auto priv =
    GPARQUET_FIXED_LENGTH_BYTE_ARRAY_STATISTICS_GET_PRIVATE(statistics);
  if (!priv->max) {
    auto parquet_statistics =
      gparquet_typed_statistics_get_raw<parquet::FLBAStatistics>(statistics);
    priv->max = g_bytes_new_static(parquet_statistics->max().ptr,
                                   parquet_statistics->descr()->type_length());
  }
  return priv->max;
}


G_END_DECLS


GParquetStatistics *
gparquet_statistics_new_raw(
  std::shared_ptr<parquet::Statistics> *parquet_statistics)
{
  GType type = GPARQUET_TYPE_STATISTICS;
  switch ((*parquet_statistics)->physical_type()) {
    case parquet::Type::BOOLEAN:
      type = GPARQUET_TYPE_BOOLEAN_STATISTICS;
      break;
    case parquet::Type::INT32:
      type = GPARQUET_TYPE_INT32_STATISTICS;
      break;
    case parquet::Type::INT64:
      type = GPARQUET_TYPE_INT64_STATISTICS;
      break;
    case parquet::Type::FLOAT:
      type = GPARQUET_TYPE_FLOAT_STATISTICS;
      break;
    case parquet::Type::DOUBLE:
      type = GPARQUET_TYPE_DOUBLE_STATISTICS;
      break;
    case parquet::Type::BYTE_ARRAY:
      type = GPARQUET_TYPE_BYTE_ARRAY_STATISTICS;
      break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      type = GPARQUET_TYPE_FIXED_LENGTH_BYTE_ARRAY_STATISTICS;
      break;
    default:
      break;
  }
  auto statistics =
    GPARQUET_STATISTICS(g_object_new(type,
                                     "statistics", parquet_statistics,
                                     NULL));
  return statistics;
}

std::shared_ptr<parquet::Statistics>
gparquet_statistics_get_raw(GParquetStatistics *statistics)
{
  auto priv = GPARQUET_STATISTICS_GET_PRIVATE(statistics);
  return priv->statistics;
}
