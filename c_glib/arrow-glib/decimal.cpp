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

#include <arrow-glib/decimal.hpp>
#include <arrow-glib/error.hpp>

template <typename Decimal> struct DecimalConverter
{
};

template <> struct DecimalConverter<arrow::Decimal32>
{
  using ArrowType = arrow::Decimal32;
  using GArrowType = GArrowDecimal32;

  GArrowType *
  new_raw(std::shared_ptr<ArrowType> *arrow_decimal32)
  {
    return garrow_decimal32_new_raw(arrow_decimal32);
  }

  std::shared_ptr<ArrowType>
  get_raw(GArrowType *decimal32)
  {
    return garrow_decimal32_get_raw(decimal32);
  }
};

template <> struct DecimalConverter<arrow::Decimal64>
{
  using ArrowType = arrow::Decimal64;
  using GArrowType = GArrowDecimal64;

  GArrowType *
  new_raw(std::shared_ptr<ArrowType> *arrow_decimal64)
  {
    return garrow_decimal64_new_raw(arrow_decimal64);
  }

  std::shared_ptr<ArrowType>
  get_raw(GArrowType *decimal64)
  {
    return garrow_decimal64_get_raw(decimal64);
  }
};

template <> struct DecimalConverter<arrow::Decimal128>
{
  using ArrowType = arrow::Decimal128;
  using GArrowType = GArrowDecimal128;

  GArrowType *
  new_raw(std::shared_ptr<ArrowType> *arrow_decimal128)
  {
    return garrow_decimal128_new_raw(arrow_decimal128);
  }

  std::shared_ptr<ArrowType>
  get_raw(GArrowType *decimal128)
  {
    return garrow_decimal128_get_raw(decimal128);
  }
};

template <> struct DecimalConverter<arrow::Decimal256>
{
  using ArrowType = arrow::Decimal256;
  using GArrowType = GArrowDecimal256;

  GArrowType *
  new_raw(std::shared_ptr<ArrowType> *arrow_decimal256)
  {
    return garrow_decimal256_new_raw(arrow_decimal256);
  }

  std::shared_ptr<ArrowType>
  get_raw(GArrowType *decimal256)
  {
    return garrow_decimal256_get_raw(decimal256);
  }
};

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_new_string(const gchar *data, GError **error, const gchar *tag)
{
  auto arrow_decimal_result = Decimal::FromString(data);
  if (garrow::check(error, arrow_decimal_result, tag)) {
    auto arrow_decimal = std::make_shared<Decimal>(*arrow_decimal_result);
    DecimalConverter<Decimal> converter;
    return converter.new_raw(&arrow_decimal);
  } else {
    return NULL;
  }
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_new_integer(const gint64 data)
{
  auto arrow_decimal = std::make_shared<Decimal>(data);
  DecimalConverter<Decimal> converter;
  return converter.new_raw(&arrow_decimal);
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_copy(typename DecimalConverter<Decimal>::GArrowType *decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  auto arrow_copied_decimal = std::make_shared<Decimal>(*arrow_decimal);
  return converter.new_raw(&arrow_copied_decimal);
}

template <typename Decimal>
gboolean
garrow_decimal_equal(typename DecimalConverter<Decimal>::GArrowType *decimal,
                     typename DecimalConverter<Decimal>::GArrowType *other_decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto arrow_other_decimal = converter.get_raw(other_decimal);
  return *arrow_decimal == *arrow_other_decimal;
}

template <typename Decimal>
gboolean
garrow_decimal_not_equal(typename DecimalConverter<Decimal>::GArrowType *decimal,
                         typename DecimalConverter<Decimal>::GArrowType *other_decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto arrow_other_decimal = converter.get_raw(other_decimal);
  return *arrow_decimal != *arrow_other_decimal;
}

template <typename Decimal>
gboolean
garrow_decimal_less_than(typename DecimalConverter<Decimal>::GArrowType *decimal,
                         typename DecimalConverter<Decimal>::GArrowType *other_decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto arrow_other_decimal = converter.get_raw(other_decimal);
  return *arrow_decimal < *arrow_other_decimal;
}

template <typename Decimal>
gboolean
garrow_decimal_less_than_or_equal(
  typename DecimalConverter<Decimal>::GArrowType *decimal,
  typename DecimalConverter<Decimal>::GArrowType *other_decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto arrow_other_decimal = converter.get_raw(other_decimal);
  return *arrow_decimal <= *arrow_other_decimal;
}

template <typename Decimal>
gboolean
garrow_decimal_greater_than(typename DecimalConverter<Decimal>::GArrowType *decimal,
                            typename DecimalConverter<Decimal>::GArrowType *other_decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto arrow_other_decimal = converter.get_raw(other_decimal);
  return *arrow_decimal > *arrow_other_decimal;
}

template <typename Decimal>
gboolean
garrow_decimal_greater_than_or_equal(
  typename DecimalConverter<Decimal>::GArrowType *decimal,
  typename DecimalConverter<Decimal>::GArrowType *other_decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto arrow_other_decimal = converter.get_raw(other_decimal);
  return *arrow_decimal >= *arrow_other_decimal;
}

template <typename Decimal>
gchar *
garrow_decimal_to_string_scale(typename DecimalConverter<Decimal>::GArrowType *decimal,
                               gint32 scale)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto string = arrow_decimal->ToString(scale);
  return g_strdup(string.c_str());
}

template <typename Decimal>
gchar *
garrow_decimal_to_string(typename DecimalConverter<Decimal>::GArrowType *decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  const auto string = arrow_decimal->ToIntegerString();
  return g_strdup(string.c_str());
}

template <typename Decimal>
GBytes *
garrow_decimal_to_bytes(typename DecimalConverter<Decimal>::GArrowType *decimal)
{
  DecimalConverter<Decimal> converter;
  const auto arrow_decimal = converter.get_raw(decimal);
  uint8_t data[DecimalConverter<Decimal>::ArrowType::kBitWidth / 8];
  arrow_decimal->ToBytes(data);
  return g_bytes_new(data, sizeof(data));
}

template <typename Decimal>
void
garrow_decimal_abs(typename DecimalConverter<Decimal>::GArrowType *decimal)
{
  DecimalConverter<Decimal> converter;
  auto arrow_decimal = converter.get_raw(decimal);
  arrow_decimal->Abs();
}

template <typename Decimal>
void
garrow_decimal_negate(typename DecimalConverter<Decimal>::GArrowType *decimal)
{
  DecimalConverter<Decimal> converter;
  auto arrow_decimal = converter.get_raw(decimal);
  arrow_decimal->Negate();
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_plus(typename DecimalConverter<Decimal>::GArrowType *left,
                    typename DecimalConverter<Decimal>::GArrowType *right)
{
  DecimalConverter<Decimal> converter;
  auto arrow_left = converter.get_raw(left);
  auto arrow_right = converter.get_raw(right);
  auto arrow_decimal = std::make_shared<Decimal>(*arrow_left + *arrow_right);
  return converter.new_raw(&arrow_decimal);
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_minus(typename DecimalConverter<Decimal>::GArrowType *left,
                     typename DecimalConverter<Decimal>::GArrowType *right)
{
  DecimalConverter<Decimal> converter;
  auto arrow_left = converter.get_raw(left);
  auto arrow_right = converter.get_raw(right);
  auto arrow_decimal = std::make_shared<Decimal>(*arrow_left - *arrow_right);
  return converter.new_raw(&arrow_decimal);
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_multiply(typename DecimalConverter<Decimal>::GArrowType *left,
                        typename DecimalConverter<Decimal>::GArrowType *right)
{
  DecimalConverter<Decimal> converter;
  auto arrow_left = converter.get_raw(left);
  auto arrow_right = converter.get_raw(right);
  auto arrow_decimal = std::make_shared<Decimal>(*arrow_left * *arrow_right);
  return converter.new_raw(&arrow_decimal);
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_divide(typename DecimalConverter<Decimal>::GArrowType *left,
                      typename DecimalConverter<Decimal>::GArrowType *right,
                      typename DecimalConverter<Decimal>::GArrowType **remainder,
                      GError **error,
                      const gchar *tag)
{
  DecimalConverter<Decimal> converter;
  auto arrow_left = converter.get_raw(left);
  auto arrow_right = converter.get_raw(right);
  auto arrow_result = arrow_left->Divide(*arrow_right);
  if (garrow::check(error, arrow_result, tag)) {
    Decimal arrow_quotient_raw;
    Decimal arrow_remainder_raw;
    std::tie(arrow_quotient_raw, arrow_remainder_raw) = *arrow_result;
    if (remainder) {
      auto arrow_remainder = std::make_shared<Decimal>(arrow_remainder_raw);
      *remainder = converter.new_raw(&arrow_remainder);
    }
    auto arrow_quotient = std::make_shared<Decimal>(arrow_quotient_raw);
    return converter.new_raw(&arrow_quotient);
  } else {
    if (remainder) {
      *remainder = NULL;
    }
    return NULL;
  }
}

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_rescale(typename DecimalConverter<Decimal>::GArrowType *decimal,
                       gint32 original_scale,
                       gint32 new_scale,
                       GError **error,
                       const gchar *tag)
{
  DecimalConverter<Decimal> converter;
  auto arrow_decimal = converter.get_raw(decimal);
  auto arrow_result = arrow_decimal->Rescale(original_scale, new_scale);
  if (garrow::check(error, arrow_result, tag)) {
    auto arrow_rescaled_decimal = std::make_shared<Decimal>(*arrow_result);
    return converter.new_raw(&arrow_rescaled_decimal);
  } else {
    return NULL;
  }
}

G_BEGIN_DECLS

/**
 * SECTION: decimal
 * @section_id: decimal
 * @title: 32-bit, 64-bit, 128-bit and 256-bit decimal classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowDecimal32 is a 32-bit decimal class.
 *
 * #GArrowDecimal64 is a 64-bit decimal class.
 *
 * #GArrowDecimal128 is a 128-bit decimal class.
 *
 * #GArrowDecimal256 is a 256-bit decimal class.
 *
 * Since: 0.10.0
 */

typedef struct GArrowDecimal32Private_
{
  std::shared_ptr<arrow::Decimal32> decimal32;
} GArrowDecimal32Private;

enum {
  PROP_DECIMAL32 = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal32, garrow_decimal32, G_TYPE_OBJECT)

#define GARROW_DECIMAL32_GET_PRIVATE(obj)                                                \
  static_cast<GArrowDecimal32Private *>(                                                 \
    garrow_decimal32_get_instance_private(GARROW_DECIMAL32(obj)))

static void
garrow_decimal32_finalize(GObject *object)
{
  auto priv = GARROW_DECIMAL32_GET_PRIVATE(object);

  priv->decimal32.~shared_ptr();

  G_OBJECT_CLASS(garrow_decimal32_parent_class)->finalize(object);
}

static void
garrow_decimal32_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GARROW_DECIMAL32_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DECIMAL32:
    priv->decimal32 =
      *static_cast<std::shared_ptr<arrow::Decimal32> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_decimal32_init(GArrowDecimal32 *object)
{
  auto priv = GARROW_DECIMAL32_GET_PRIVATE(object);
  new (&priv->decimal32) std::shared_ptr<arrow::Decimal32>;
}

static void
garrow_decimal32_class_init(GArrowDecimal32Class *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_decimal32_finalize;
  gobject_class->set_property = garrow_decimal32_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "decimal32",
    "Decimal32",
    "The raw std::shared<arrow::Decimal32> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DECIMAL32, spec);
}

/**
 * garrow_decimal32_new_string:
 * @data: The data of the decimal.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created #GArrowDecimal32 on success, %NULL on error.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_new_string(const gchar *data, GError **error)
{
  return garrow_decimal_new_string<arrow::Decimal32>(data,
                                                     error,
                                                     "[decimal32][new][string]");
}

/**
 * garrow_decimal32_new_integer:
 * @data: The data of the decimal.
 *
 * Returns: A newly created #GArrowDecimal32.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_new_integer(const gint64 data)
{
  return garrow_decimal_new_integer<arrow::Decimal32>(data);
}

/**
 * garrow_decimal32_copy:
 * @decimal: The decimal to be copied.
 *
 * Returns: (transfer full): A copied #GArrowDecimal32.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_copy(GArrowDecimal32 *decimal)
{
  return garrow_decimal_copy<arrow::Decimal32>(decimal);
}

/**
 * garrow_decimal32_equal:
 * @decimal: A #GArrowDecimal32.
 * @other_decimal: A #GArrowDecimal32 to be compared.
 *
 * Returns: %TRUE if the decimal is equal to the other decimal, %FALSE
 *   otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal32_equal(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal)
{
  return garrow_decimal_equal<arrow::Decimal32>(decimal, other_decimal);
}

/**
 * garrow_decimal32_not_equal:
 * @decimal: A #GArrowDecimal32.
 * @other_decimal: A #GArrowDecimal32 to be compared.
 *
 * Returns: %TRUE if the decimal isn't equal to the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal32_not_equal(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal)
{
  return garrow_decimal_not_equal<arrow::Decimal32>(decimal, other_decimal);
}

/**
 * garrow_decimal32_less_than:
 * @decimal: A #GArrowDecimal32.
 * @other_decimal: A #GArrowDecimal32 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal32_less_than(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal)
{
  return garrow_decimal_less_than<arrow::Decimal32>(decimal, other_decimal);
}

/**
 * garrow_decimal32_less_than_or_equal:
 * @decimal: A #GArrowDecimal32.
 * @other_decimal: A #GArrowDecimal32 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal32_less_than_or_equal(GArrowDecimal32 *decimal,
                                    GArrowDecimal32 *other_decimal)
{
  return garrow_decimal_less_than_or_equal<arrow::Decimal32>(decimal, other_decimal);
}

/**
 * garrow_decimal32_greater_than:
 * @decimal: A #GArrowDecimal32.
 * @other_decimal: A #GArrowDecimal32 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal32_greater_than(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal)
{
  return garrow_decimal_greater_than<arrow::Decimal32>(decimal, other_decimal);
}

/**
 * garrow_decimal32_greater_than_or_equal:
 * @decimal: A #GArrowDecimal32.
 * @other_decimal: A #GArrowDecimal32 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal32_greater_than_or_equal(GArrowDecimal32 *decimal,
                                       GArrowDecimal32 *other_decimal)
{
  return garrow_decimal_greater_than_or_equal<arrow::Decimal32>(decimal, other_decimal);
}

/**
 * garrow_decimal32_to_string_scale:
 * @decimal: A #GArrowDecimal32.
 * @scale: The scale of the decimal.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 19.0.0
 */
gchar *
garrow_decimal32_to_string_scale(GArrowDecimal32 *decimal, gint32 scale)
{
  return garrow_decimal_to_string_scale<arrow::Decimal32>(decimal, scale);
}

/**
 * garrow_decimal32_to_string:
 * @decimal: A #GArrowDecimal32.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 19.0.0
 */
gchar *
garrow_decimal32_to_string(GArrowDecimal32 *decimal)
{
  return garrow_decimal_to_string<arrow::Decimal32>(decimal);
}

/**
 * garrow_decimal32_to_bytes:
 * @decimal: A #GArrowDecimal32.
 *
 * Returns: (transfer full): The binary representation of the decimal.
 *
 * Since: 19.0.0
 */
GBytes *
garrow_decimal32_to_bytes(GArrowDecimal32 *decimal)
{
  return garrow_decimal_to_bytes<arrow::Decimal32>(decimal);
}

/**
 * garrow_decimal32_abs:
 * @decimal: A #GArrowDecimal32.
 *
 * Computes the absolute value of the @decimal destructively.
 *
 * Since: 19.0.0
 */
void
garrow_decimal32_abs(GArrowDecimal32 *decimal)
{
  garrow_decimal_abs<arrow::Decimal32>(decimal);
}

/**
 * garrow_decimal32_negate:
 * @decimal: A #GArrowDecimal32.
 *
 * Negate the current value of the @decimal destructively.
 *
 * Since: 19.0.0
 */
void
garrow_decimal32_negate(GArrowDecimal32 *decimal)
{
  garrow_decimal_negate<arrow::Decimal32>(decimal);
}

/**
 * garrow_decimal32_to_integer:
 * @decimal: A #GArrowDecimal32.
 *
 * Returns: The 64-bit integer representation of the decimal.
 *
 * Since: 19.0.0
 */
gint64
garrow_decimal32_to_integer(GArrowDecimal32 *decimal)
{
  auto arrow_decimal = garrow_decimal32_get_raw(decimal);
  return static_cast<int64_t>(*arrow_decimal);
}

/**
 * garrow_decimal32_plus:
 * @left: A #GArrowDecimal32.
 * @right: A #GArrowDecimal32.
 *
 * Returns: (transfer full): The added value of these decimals.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_plus(GArrowDecimal32 *left, GArrowDecimal32 *right)
{
  return garrow_decimal_plus<arrow::Decimal32>(left, right);
}

/**
 * garrow_decimal32_minus:
 * @left: A #GArrowDecimal32.
 * @right: A #GArrowDecimal32.
 *
 * Returns: (transfer full): The subtracted value of these decimals.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_minus(GArrowDecimal32 *left, GArrowDecimal32 *right)
{
  return garrow_decimal_minus<arrow::Decimal32>(left, right);
}

/**
 * garrow_decimal32_multiply:
 * @left: A #GArrowDecimal32.
 * @right: A #GArrowDecimal32.
 *
 * Returns: (transfer full): The multiplied value of these decimals.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_multiply(GArrowDecimal32 *left, GArrowDecimal32 *right)
{
  return garrow_decimal_multiply<arrow::Decimal32>(left, right);
}

/**
 * garrow_decimal32_divide:
 * @left: A #GArrowDecimal32.
 * @right: A #GArrowDecimal32.
 * @remainder: (out) (nullable): A return location for the remainder
 *   value of these decimals. The returned #GArrowDecimal32 be
 *   unreferred with g_object_unref() when no longer needed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The divided value of
 *   these decimals or %NULL on error.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_divide(GArrowDecimal32 *left,
                        GArrowDecimal32 *right,
                        GArrowDecimal32 **remainder,
                        GError **error)
{
  return garrow_decimal_divide<arrow::Decimal32>(left,
                                                 right,
                                                 remainder,
                                                 error,
                                                 "[decimal32][divide]");
}

/**
 * garrow_decimal32_rescale:
 * @decimal: A #GArrowDecimal32.
 * @original_scale: A scale to be converted from.
 * @new_scale: A scale to be converted to.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The rescaled decimal or %NULL on error.
 *
 * Since: 19.0.0
 */
GArrowDecimal32 *
garrow_decimal32_rescale(GArrowDecimal32 *decimal,
                         gint32 original_scale,
                         gint32 new_scale,
                         GError **error)
{
  return garrow_decimal_rescale<arrow::Decimal32>(decimal,
                                                  original_scale,
                                                  new_scale,
                                                  error,
                                                  "[decimal32][rescale]");
}
typedef struct GArrowDecimal64Private_
{
  std::shared_ptr<arrow::Decimal64> decimal64;
} GArrowDecimal64Private;

enum {
  PROP_DECIMAL64 = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal64, garrow_decimal64, G_TYPE_OBJECT)

#define GARROW_DECIMAL64_GET_PRIVATE(obj)                                                \
  static_cast<GArrowDecimal64Private *>(                                                 \
    garrow_decimal64_get_instance_private(GARROW_DECIMAL64(obj)))

static void
garrow_decimal64_finalize(GObject *object)
{
  auto priv = GARROW_DECIMAL64_GET_PRIVATE(object);

  priv->decimal64.~shared_ptr();

  G_OBJECT_CLASS(garrow_decimal64_parent_class)->finalize(object);
}

static void
garrow_decimal64_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GARROW_DECIMAL64_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DECIMAL64:
    priv->decimal64 =
      *static_cast<std::shared_ptr<arrow::Decimal64> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_decimal64_init(GArrowDecimal64 *object)
{
  auto priv = GARROW_DECIMAL64_GET_PRIVATE(object);
  new (&priv->decimal64) std::shared_ptr<arrow::Decimal64>;
}

static void
garrow_decimal64_class_init(GArrowDecimal64Class *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_decimal64_finalize;
  gobject_class->set_property = garrow_decimal64_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "decimal64",
    "Decimal64",
    "The raw std::shared<arrow::Decimal64> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DECIMAL64, spec);
}

/**
 * garrow_decimal64_new_string:
 * @data: The data of the decimal.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created #GArrowDecimal64 on success, %NULL on error.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_new_string(const gchar *data, GError **error)
{
  return garrow_decimal_new_string<arrow::Decimal64>(data,
                                                     error,
                                                     "[decimal64][new][string]");
}

/**
 * garrow_decimal64_new_integer:
 * @data: The data of the decimal.
 *
 * Returns: A newly created #GArrowDecimal64.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_new_integer(const gint64 data)
{
  return garrow_decimal_new_integer<arrow::Decimal64>(data);
}

/**
 * garrow_decimal64_copy:
 * @decimal: The decimal to be copied.
 *
 * Returns: (transfer full): A copied #GArrowDecimal64.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_copy(GArrowDecimal64 *decimal)
{
  return garrow_decimal_copy<arrow::Decimal64>(decimal);
}

/**
 * garrow_decimal64_equal:
 * @decimal: A #GArrowDecimal64.
 * @other_decimal: A #GArrowDecimal64 to be compared.
 *
 * Returns: %TRUE if the decimal is equal to the other decimal, %FALSE
 *   otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal64_equal(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal)
{
  return garrow_decimal_equal<arrow::Decimal64>(decimal, other_decimal);
}

/**
 * garrow_decimal64_not_equal:
 * @decimal: A #GArrowDecimal64.
 * @other_decimal: A #GArrowDecimal64 to be compared.
 *
 * Returns: %TRUE if the decimal isn't equal to the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal64_not_equal(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal)
{
  return garrow_decimal_not_equal<arrow::Decimal64>(decimal, other_decimal);
}

/**
 * garrow_decimal64_less_than:
 * @decimal: A #GArrowDecimal64.
 * @other_decimal: A #GArrowDecimal64 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal64_less_than(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal)
{
  return garrow_decimal_less_than<arrow::Decimal64>(decimal, other_decimal);
}

/**
 * garrow_decimal64_less_than_or_equal:
 * @decimal: A #GArrowDecimal64.
 * @other_decimal: A #GArrowDecimal64 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal64_less_than_or_equal(GArrowDecimal64 *decimal,
                                    GArrowDecimal64 *other_decimal)
{
  return garrow_decimal_less_than_or_equal<arrow::Decimal64>(decimal, other_decimal);
}

/**
 * garrow_decimal64_greater_than:
 * @decimal: A #GArrowDecimal64.
 * @other_decimal: A #GArrowDecimal64 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal64_greater_than(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal)
{
  return garrow_decimal_greater_than<arrow::Decimal64>(decimal, other_decimal);
}

/**
 * garrow_decimal64_greater_than_or_equal:
 * @decimal: A #GArrowDecimal64.
 * @other_decimal: A #GArrowDecimal64 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 19.0.0
 */
gboolean
garrow_decimal64_greater_than_or_equal(GArrowDecimal64 *decimal,
                                       GArrowDecimal64 *other_decimal)
{
  return garrow_decimal_greater_than_or_equal<arrow::Decimal64>(decimal, other_decimal);
}

/**
 * garrow_decimal64_to_string_scale:
 * @decimal: A #GArrowDecimal64.
 * @scale: The scale of the decimal.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 19.0.0
 */
gchar *
garrow_decimal64_to_string_scale(GArrowDecimal64 *decimal, gint32 scale)
{
  return garrow_decimal_to_string_scale<arrow::Decimal64>(decimal, scale);
}

/**
 * garrow_decimal64_to_string:
 * @decimal: A #GArrowDecimal64.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 19.0.0
 */
gchar *
garrow_decimal64_to_string(GArrowDecimal64 *decimal)
{
  return garrow_decimal_to_string<arrow::Decimal64>(decimal);
}

/**
 * garrow_decimal64_to_bytes:
 * @decimal: A #GArrowDecimal64.
 *
 * Returns: (transfer full): The binary representation of the decimal.
 *
 * Since: 19.0.0
 */
GBytes *
garrow_decimal64_to_bytes(GArrowDecimal64 *decimal)
{
  return garrow_decimal_to_bytes<arrow::Decimal64>(decimal);
}

/**
 * garrow_decimal64_abs:
 * @decimal: A #GArrowDecimal64.
 *
 * Computes the absolute value of the @decimal destructively.
 *
 * Since: 19.0.0
 */
void
garrow_decimal64_abs(GArrowDecimal64 *decimal)
{
  garrow_decimal_abs<arrow::Decimal64>(decimal);
}

/**
 * garrow_decimal64_negate:
 * @decimal: A #GArrowDecimal64.
 *
 * Negate the current value of the @decimal destructively.
 *
 * Since: 19.0.0
 */
void
garrow_decimal64_negate(GArrowDecimal64 *decimal)
{
  garrow_decimal_negate<arrow::Decimal64>(decimal);
}

/**
 * garrow_decimal64_to_integer:
 * @decimal: A #GArrowDecimal64.
 *
 * Returns: The 64-bit integer representation of the decimal.
 *
 * Since: 19.0.0
 */
gint64
garrow_decimal64_to_integer(GArrowDecimal64 *decimal)
{
  auto arrow_decimal = garrow_decimal64_get_raw(decimal);
  return static_cast<int64_t>(*arrow_decimal);
}

/**
 * garrow_decimal64_plus:
 * @left: A #GArrowDecimal64.
 * @right: A #GArrowDecimal64.
 *
 * Returns: (transfer full): The added value of these decimals.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_plus(GArrowDecimal64 *left, GArrowDecimal64 *right)
{
  return garrow_decimal_plus<arrow::Decimal64>(left, right);
}

/**
 * garrow_decimal64_minus:
 * @left: A #GArrowDecimal64.
 * @right: A #GArrowDecimal64.
 *
 * Returns: (transfer full): The subtracted value of these decimals.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_minus(GArrowDecimal64 *left, GArrowDecimal64 *right)
{
  return garrow_decimal_minus<arrow::Decimal64>(left, right);
}

/**
 * garrow_decimal64_multiply:
 * @left: A #GArrowDecimal64.
 * @right: A #GArrowDecimal64.
 *
 * Returns: (transfer full): The multiplied value of these decimals.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_multiply(GArrowDecimal64 *left, GArrowDecimal64 *right)
{
  return garrow_decimal_multiply<arrow::Decimal64>(left, right);
}

/**
 * garrow_decimal64_divide:
 * @left: A #GArrowDecimal64.
 * @right: A #GArrowDecimal64.
 * @remainder: (out) (nullable): A return location for the remainder
 *   value of these decimals. The returned #GArrowDecimal64 be
 *   unreferred with g_object_unref() when no longer needed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The divided value of
 *   these decimals or %NULL on error.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_divide(GArrowDecimal64 *left,
                        GArrowDecimal64 *right,
                        GArrowDecimal64 **remainder,
                        GError **error)
{
  return garrow_decimal_divide<arrow::Decimal64>(left,
                                                 right,
                                                 remainder,
                                                 error,
                                                 "[decimal64][divide]");
}

/**
 * garrow_decimal64_rescale:
 * @decimal: A #GArrowDecimal64.
 * @original_scale: A scale to be converted from.
 * @new_scale: A scale to be converted to.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The rescaled decimal or %NULL on error.
 *
 * Since: 19.0.0
 */
GArrowDecimal64 *
garrow_decimal64_rescale(GArrowDecimal64 *decimal,
                         gint32 original_scale,
                         gint32 new_scale,
                         GError **error)
{
  return garrow_decimal_rescale<arrow::Decimal64>(decimal,
                                                  original_scale,
                                                  new_scale,
                                                  error,
                                                  "[decimal64][rescale]");
}

typedef struct GArrowDecimal128Private_
{
  std::shared_ptr<arrow::Decimal128> decimal128;
} GArrowDecimal128Private;

enum {
  PROP_DECIMAL128 = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal128, garrow_decimal128, G_TYPE_OBJECT)

#define GARROW_DECIMAL128_GET_PRIVATE(obj)                                               \
  static_cast<GArrowDecimal128Private *>(                                                \
    garrow_decimal128_get_instance_private(GARROW_DECIMAL128(obj)))

static void
garrow_decimal128_finalize(GObject *object)
{
  auto priv = GARROW_DECIMAL128_GET_PRIVATE(object);

  priv->decimal128.~shared_ptr();

  G_OBJECT_CLASS(garrow_decimal128_parent_class)->finalize(object);
}

static void
garrow_decimal128_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_DECIMAL128_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DECIMAL128:
    priv->decimal128 =
      *static_cast<std::shared_ptr<arrow::Decimal128> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_decimal128_init(GArrowDecimal128 *object)
{
  auto priv = GARROW_DECIMAL128_GET_PRIVATE(object);
  new (&priv->decimal128) std::shared_ptr<arrow::Decimal128>;
}

static void
garrow_decimal128_class_init(GArrowDecimal128Class *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_decimal128_finalize;
  gobject_class->set_property = garrow_decimal128_set_property;

  spec = g_param_spec_pointer(
    "decimal128",
    "Decimal128",
    "The raw std::shared<arrow::Decimal128> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DECIMAL128, spec);
}

/**
 * garrow_decimal128_new_string:
 * @data: The data of the decimal.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created #GArrowDecimal128 on success, %NULL on error.
 *
 * Since: 0.10.0
 */
GArrowDecimal128 *
garrow_decimal128_new_string(const gchar *data, GError **error)
{
  return garrow_decimal_new_string<arrow::Decimal128>(data,
                                                      error,
                                                      "[decimal128][new][string]");
}

/**
 * garrow_decimal128_new_integer:
 * @data: The data of the decimal.
 *
 * Returns: A newly created #GArrowDecimal128.
 *
 * Since: 0.10.0
 */
GArrowDecimal128 *
garrow_decimal128_new_integer(const gint64 data)
{
  return garrow_decimal_new_integer<arrow::Decimal128>(data);
}

/**
 * garrow_decimal128_copy:
 * @decimal: The decimal to be copied.
 *
 * Returns: (transfer full): A copied #GArrowDecimal128.
 *
 * Since: 3.0.0
 */
GArrowDecimal128 *
garrow_decimal128_copy(GArrowDecimal128 *decimal)
{
  return garrow_decimal_copy<arrow::Decimal128>(decimal);
}

/**
 * garrow_decimal128_equal:
 * @decimal: A #GArrowDecimal128.
 * @other_decimal: A #GArrowDecimal128 to be compared.
 *
 * Returns: %TRUE if the decimal is equal to the other decimal, %FALSE
 *   otherwise.
 *
 * Since: 0.12.0
 */
gboolean
garrow_decimal128_equal(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal)
{
  return garrow_decimal_equal<arrow::Decimal128>(decimal, other_decimal);
}

/**
 * garrow_decimal128_not_equal:
 * @decimal: A #GArrowDecimal128.
 * @other_decimal: A #GArrowDecimal128 to be compared.
 *
 * Returns: %TRUE if the decimal isn't equal to the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 0.12.0
 */
gboolean
garrow_decimal128_not_equal(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal)
{
  return garrow_decimal_not_equal<arrow::Decimal128>(decimal, other_decimal);
}

/**
 * garrow_decimal128_less_than:
 * @decimal: A #GArrowDecimal128.
 * @other_decimal: A #GArrowDecimal128 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 0.12.0
 */
gboolean
garrow_decimal128_less_than(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal)
{
  return garrow_decimal_less_than<arrow::Decimal128>(decimal, other_decimal);
}

/**
 * garrow_decimal128_less_than_or_equal:
 * @decimal: A #GArrowDecimal128.
 * @other_decimal: A #GArrowDecimal128 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 0.12.0
 */
gboolean
garrow_decimal128_less_than_or_equal(GArrowDecimal128 *decimal,
                                     GArrowDecimal128 *other_decimal)
{
  return garrow_decimal_less_than_or_equal<arrow::Decimal128>(decimal, other_decimal);
}

/**
 * garrow_decimal128_greater_than:
 * @decimal: A #GArrowDecimal128.
 * @other_decimal: A #GArrowDecimal128 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 0.12.0
 */
gboolean
garrow_decimal128_greater_than(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal)
{
  return garrow_decimal_greater_than<arrow::Decimal128>(decimal, other_decimal);
}

/**
 * garrow_decimal128_greater_than_or_equal:
 * @decimal: A #GArrowDecimal128.
 * @other_decimal: A #GArrowDecimal128 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 0.12.0
 */
gboolean
garrow_decimal128_greater_than_or_equal(GArrowDecimal128 *decimal,
                                        GArrowDecimal128 *other_decimal)
{
  return garrow_decimal_greater_than_or_equal<arrow::Decimal128>(decimal, other_decimal);
}

/**
 * garrow_decimal128_to_string_scale:
 * @decimal: A #GArrowDecimal128.
 * @scale: The scale of the decimal.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.10.0
 */
gchar *
garrow_decimal128_to_string_scale(GArrowDecimal128 *decimal, gint32 scale)
{
  return garrow_decimal_to_string_scale<arrow::Decimal128>(decimal, scale);
}

/**
 * garrow_decimal128_to_string:
 * @decimal: A #GArrowDecimal128.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.10.0
 */
gchar *
garrow_decimal128_to_string(GArrowDecimal128 *decimal)
{
  return garrow_decimal_to_string<arrow::Decimal128>(decimal);
}

/**
 * garrow_decimal128_to_bytes:
 * @decimal: A #GArrowDecimal128.
 *
 * Returns: (transfer full): The binary representation of the decimal.
 *
 * Since: 3.0.0
 */
GBytes *
garrow_decimal128_to_bytes(GArrowDecimal128 *decimal)
{
  return garrow_decimal_to_bytes<arrow::Decimal128>(decimal);
}

/**
 * garrow_decimal128_abs:
 * @decimal: A #GArrowDecimal128.
 *
 * Computes the absolute value of the @decimal destructively.
 *
 * Since: 0.10.0
 */
void
garrow_decimal128_abs(GArrowDecimal128 *decimal)
{
  garrow_decimal_abs<arrow::Decimal128>(decimal);
}

/**
 * garrow_decimal128_negate:
 * @decimal: A #GArrowDecimal128.
 *
 * Negate the current value of the @decimal destructively.
 *
 * Since: 0.10.0
 */
void
garrow_decimal128_negate(GArrowDecimal128 *decimal)
{
  garrow_decimal_negate<arrow::Decimal128>(decimal);
}

/**
 * garrow_decimal128_to_integer:
 * @decimal: A #GArrowDecimal128.
 *
 * Returns: The 64-bit integer representation of the decimal.
 *
 * Since: 0.10.0
 */
gint64
garrow_decimal128_to_integer(GArrowDecimal128 *decimal)
{
  auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  return static_cast<int64_t>(*arrow_decimal);
}

/**
 * garrow_decimal128_plus:
 * @left: A #GArrowDecimal128.
 * @right: A #GArrowDecimal128.
 *
 * Returns: (transfer full): The added value of these decimals.
 *
 * Since: 0.11.0
 */
GArrowDecimal128 *
garrow_decimal128_plus(GArrowDecimal128 *left, GArrowDecimal128 *right)
{
  return garrow_decimal_plus<arrow::Decimal128>(left, right);
}

/**
 * garrow_decimal128_minus:
 * @left: A #GArrowDecimal128.
 * @right: A #GArrowDecimal128.
 *
 * Returns: (transfer full): The subtracted value of these decimals.
 *
 * Since: 0.11.0
 */
GArrowDecimal128 *
garrow_decimal128_minus(GArrowDecimal128 *left, GArrowDecimal128 *right)
{
  return garrow_decimal_minus<arrow::Decimal128>(left, right);
}

/**
 * garrow_decimal128_multiply:
 * @left: A #GArrowDecimal128.
 * @right: A #GArrowDecimal128.
 *
 * Returns: (transfer full): The multiplied value of these decimals.
 *
 * Since: 0.11.0
 */
GArrowDecimal128 *
garrow_decimal128_multiply(GArrowDecimal128 *left, GArrowDecimal128 *right)
{
  return garrow_decimal_multiply<arrow::Decimal128>(left, right);
}

/**
 * garrow_decimal128_divide:
 * @left: A #GArrowDecimal128.
 * @right: A #GArrowDecimal128.
 * @remainder: (out) (nullable): A return location for the remainder
 *   value of these decimals. The returned #GArrowDecimal128 be
 *   unreferred with g_object_unref() when no longer needed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The divided value of
 *   these decimals or %NULL on error.
 *
 * Since: 0.11.0
 */
GArrowDecimal128 *
garrow_decimal128_divide(GArrowDecimal128 *left,
                         GArrowDecimal128 *right,
                         GArrowDecimal128 **remainder,
                         GError **error)
{
  return garrow_decimal_divide<arrow::Decimal128>(left,
                                                  right,
                                                  remainder,
                                                  error,
                                                  "[decimal128][divide]");
}

/**
 * garrow_decimal128_rescale:
 * @decimal: A #GArrowDecimal128.
 * @original_scale: A scale to be converted from.
 * @new_scale: A scale to be converted to.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The rescaled decimal or %NULL on error.
 *
 * Since: 0.15.0
 */
GArrowDecimal128 *
garrow_decimal128_rescale(GArrowDecimal128 *decimal,
                          gint32 original_scale,
                          gint32 new_scale,
                          GError **error)
{
  return garrow_decimal_rescale<arrow::Decimal128>(decimal,
                                                   original_scale,
                                                   new_scale,
                                                   error,
                                                   "[decimal128][rescale]");
}

typedef struct GArrowDecimal256Private_
{
  std::shared_ptr<arrow::Decimal256> decimal256;
} GArrowDecimal256Private;

enum {
  PROP_DECIMAL256 = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal256, garrow_decimal256, G_TYPE_OBJECT)

#define GARROW_DECIMAL256_GET_PRIVATE(obj)                                               \
  static_cast<GArrowDecimal256Private *>(                                                \
    garrow_decimal256_get_instance_private(GARROW_DECIMAL256(obj)))

static void
garrow_decimal256_finalize(GObject *object)
{
  auto priv = GARROW_DECIMAL256_GET_PRIVATE(object);

  priv->decimal256.~shared_ptr();

  G_OBJECT_CLASS(garrow_decimal256_parent_class)->finalize(object);
}

static void
garrow_decimal256_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_DECIMAL256_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DECIMAL256:
    priv->decimal256 =
      *static_cast<std::shared_ptr<arrow::Decimal256> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_decimal256_init(GArrowDecimal256 *object)
{
  auto priv = GARROW_DECIMAL256_GET_PRIVATE(object);
  new (&priv->decimal256) std::shared_ptr<arrow::Decimal256>;
}

static void
garrow_decimal256_class_init(GArrowDecimal256Class *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_decimal256_finalize;
  gobject_class->set_property = garrow_decimal256_set_property;

  spec = g_param_spec_pointer(
    "decimal256",
    "Decimal256",
    "The raw std::shared<arrow::Decimal256> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DECIMAL256, spec);
}

/**
 * garrow_decimal256_new_string:
 * @data: The data of the decimal.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created #GArrowDecimal256 on success, %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_new_string(const gchar *data, GError **error)
{
  return garrow_decimal_new_string<arrow::Decimal256>(data,
                                                      error,
                                                      "[decimal256][new][string]");
}

/**
 * garrow_decimal256_new_integer:
 * @data: The data of the decimal.
 *
 * Returns: A newly created #GArrowDecimal256.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_new_integer(const gint64 data)
{
  return garrow_decimal_new_integer<arrow::Decimal256>(data);
}

/**
 * garrow_decimal256_copy:
 * @decimal: The decimal to be copied.
 *
 * Returns: (transfer full): A copied #GArrowDecimal256.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_copy(GArrowDecimal256 *decimal)
{
  return garrow_decimal_copy<arrow::Decimal256>(decimal);
}

/**
 * garrow_decimal256_equal:
 * @decimal: A #GArrowDecimal256.
 * @other_decimal: A #GArrowDecimal256 to be compared.
 *
 * Returns: %TRUE if the decimal is equal to the other decimal, %FALSE
 *   otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_decimal256_equal(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal)
{
  return garrow_decimal_equal<arrow::Decimal256>(decimal, other_decimal);
}

/**
 * garrow_decimal256_not_equal:
 * @decimal: A #GArrowDecimal256.
 * @other_decimal: A #GArrowDecimal256 to be compared.
 *
 * Returns: %TRUE if the decimal isn't equal to the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_decimal256_not_equal(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal)
{
  return garrow_decimal_not_equal<arrow::Decimal256>(decimal, other_decimal);
}

/**
 * garrow_decimal256_less_than:
 * @decimal: A #GArrowDecimal256.
 * @other_decimal: A #GArrowDecimal256 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_decimal256_less_than(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal)
{
  return garrow_decimal_less_than<arrow::Decimal256>(decimal, other_decimal);
}

/**
 * garrow_decimal256_less_than_or_equal:
 * @decimal: A #GArrowDecimal256.
 * @other_decimal: A #GArrowDecimal256 to be compared.
 *
 * Returns: %TRUE if the decimal is less than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_decimal256_less_than_or_equal(GArrowDecimal256 *decimal,
                                     GArrowDecimal256 *other_decimal)
{
  return garrow_decimal_less_than_or_equal<arrow::Decimal256>(decimal, other_decimal);
}

/**
 * garrow_decimal256_greater_than:
 * @decimal: A #GArrowDecimal256.
 * @other_decimal: A #GArrowDecimal256 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal,
 *   %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_decimal256_greater_than(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal)
{
  return garrow_decimal_greater_than<arrow::Decimal256>(decimal, other_decimal);
}

/**
 * garrow_decimal256_greater_than_or_equal:
 * @decimal: A #GArrowDecimal256.
 * @other_decimal: A #GArrowDecimal256 to be compared.
 *
 * Returns: %TRUE if the decimal is greater than the other decimal
 *   or equal to the other decimal, %FALSE otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_decimal256_greater_than_or_equal(GArrowDecimal256 *decimal,
                                        GArrowDecimal256 *other_decimal)
{
  return garrow_decimal_greater_than_or_equal<arrow::Decimal256>(decimal, other_decimal);
}

/**
 * garrow_decimal256_to_string_scale:
 * @decimal: A #GArrowDecimal256.
 * @scale: The scale of the decimal.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
garrow_decimal256_to_string_scale(GArrowDecimal256 *decimal, gint32 scale)
{
  return garrow_decimal_to_string_scale<arrow::Decimal256>(decimal, scale);
}

/**
 * garrow_decimal256_to_string:
 * @decimal: A #GArrowDecimal256.
 *
 * Returns: The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
garrow_decimal256_to_string(GArrowDecimal256 *decimal)
{
  return garrow_decimal_to_string<arrow::Decimal256>(decimal);
}

/**
 * garrow_decimal256_to_bytes:
 * @decimal: A #GArrowDecimal256.
 *
 * Returns: (transfer full): The binary representation of the decimal.
 *
 * Since: 3.0.0
 */
GBytes *
garrow_decimal256_to_bytes(GArrowDecimal256 *decimal)
{
  return garrow_decimal_to_bytes<arrow::Decimal256>(decimal);
}

/**
 * garrow_decimal256_abs:
 * @decimal: A #GArrowDecimal256.
 *
 * Computes the absolute value of the @decimal destructively.
 *
 * Since: 3.0.0
 */
void
garrow_decimal256_abs(GArrowDecimal256 *decimal)
{
  garrow_decimal_abs<arrow::Decimal256>(decimal);
}

/**
 * garrow_decimal256_negate:
 * @decimal: A #GArrowDecimal256.
 *
 * Negate the current value of the @decimal destructively.
 *
 * Since: 3.0.0
 */
void
garrow_decimal256_negate(GArrowDecimal256 *decimal)
{
  garrow_decimal_negate<arrow::Decimal256>(decimal);
}

/**
 * garrow_decimal256_plus:
 * @left: A #GArrowDecimal256.
 * @right: A #GArrowDecimal256.
 *
 * Returns: (transfer full): The added value of these decimals.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_plus(GArrowDecimal256 *left, GArrowDecimal256 *right)
{
  return garrow_decimal_plus<arrow::Decimal256>(left, right);
}

/**
 * garrow_decimal256_multiply:
 * @left: A #GArrowDecimal256.
 * @right: A #GArrowDecimal256.
 *
 * Returns: (transfer full): The multiplied value of these decimals.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_multiply(GArrowDecimal256 *left, GArrowDecimal256 *right)
{
  return garrow_decimal_multiply<arrow::Decimal256>(left, right);
}

/**
 * garrow_decimal256_divide:
 * @left: A #GArrowDecimal256.
 * @right: A #GArrowDecimal256.
 * @remainder: (out) (nullable): A return location for the remainder
 *   value of these decimals. The returned #GArrowDecimal256 be
 *   unreferred with g_object_unref() when no longer needed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The divided value of
 *   these decimals or %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_divide(GArrowDecimal256 *left,
                         GArrowDecimal256 *right,
                         GArrowDecimal256 **remainder,
                         GError **error)
{
  return garrow_decimal_divide<arrow::Decimal256>(left,
                                                  right,
                                                  remainder,
                                                  error,
                                                  "[decimal256][divide]");
}

/**
 * garrow_decimal256_rescale:
 * @decimal: A #GArrowDecimal256.
 * @original_scale: A scale to be converted from.
 * @new_scale: A scale to be converted to.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The rescaled decimal or %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_rescale(GArrowDecimal256 *decimal,
                          gint32 original_scale,
                          gint32 new_scale,
                          GError **error)
{
  return garrow_decimal_rescale<arrow::Decimal256>(decimal,
                                                   original_scale,
                                                   new_scale,
                                                   error,
                                                   "[decimal256][rescale]");
}

G_END_DECLS

GArrowDecimal32 *
garrow_decimal32_new_raw(std::shared_ptr<arrow::Decimal32> *arrow_decimal32)
{
  auto decimal32 =
    g_object_new(garrow_decimal32_get_type(), "decimal32", arrow_decimal32, NULL);
  return GARROW_DECIMAL32(decimal32);
}

std::shared_ptr<arrow::Decimal32>
garrow_decimal32_get_raw(GArrowDecimal32 *decimal32)
{
  auto priv = GARROW_DECIMAL32_GET_PRIVATE(decimal32);
  return priv->decimal32;
}

GArrowDecimal64 *
garrow_decimal64_new_raw(std::shared_ptr<arrow::Decimal64> *arrow_decimal64)
{
  auto decimal64 =
    g_object_new(garrow_decimal64_get_type(), "decimal64", arrow_decimal64, NULL);
  return GARROW_DECIMAL64(decimal64);
}

std::shared_ptr<arrow::Decimal64>
garrow_decimal64_get_raw(GArrowDecimal64 *decimal64)
{
  auto priv = GARROW_DECIMAL64_GET_PRIVATE(decimal64);
  return priv->decimal64;
}

GArrowDecimal128 *
garrow_decimal128_new_raw(std::shared_ptr<arrow::Decimal128> *arrow_decimal128)
{
  auto decimal128 =
    g_object_new(garrow_decimal128_get_type(), "decimal128", arrow_decimal128, NULL);
  return GARROW_DECIMAL128(decimal128);
}

std::shared_ptr<arrow::Decimal128>
garrow_decimal128_get_raw(GArrowDecimal128 *decimal128)
{
  auto priv = GARROW_DECIMAL128_GET_PRIVATE(decimal128);
  return priv->decimal128;
}

GArrowDecimal256 *
garrow_decimal256_new_raw(std::shared_ptr<arrow::Decimal256> *arrow_decimal256)
{
  auto decimal256 =
    g_object_new(garrow_decimal256_get_type(), "decimal256", arrow_decimal256, NULL);
  return GARROW_DECIMAL256(decimal256);
}

std::shared_ptr<arrow::Decimal256>
garrow_decimal256_get_raw(GArrowDecimal256 *decimal256)
{
  auto priv = GARROW_DECIMAL256_GET_PRIVATE(decimal256);
  return priv->decimal256;
}
