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

template <typename Decimal>
struct DecimalConverter {
};

template <>
struct DecimalConverter<arrow::Decimal128> {
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

template <>
struct DecimalConverter<arrow::Decimal256> {
  using ArrowType = arrow::Decimal256;
  using GArrowType = GArrowDecimal256;

  GArrowType *
  new_raw(std::shared_ptr<ArrowType> *arrow_decimal256) {
    return garrow_decimal256_new_raw(arrow_decimal256);
  }

  std::shared_ptr<ArrowType>
  get_raw(GArrowType *decimal256) {
    return garrow_decimal256_get_raw(decimal256);
  }
};

template <typename Decimal>
typename DecimalConverter<Decimal>::GArrowType *
garrow_decimal_new_string(const gchar *data,
                          GError **error,
                          const gchar *tag)
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
garrow_decimal_less_than_or_equal(typename DecimalConverter<Decimal>::GArrowType *decimal,
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
garrow_decimal_greater_than_or_equal(typename DecimalConverter<Decimal>::GArrowType *decimal,
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
 * @title: 128-bit and 256-bit decimal classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowDecimal128 is a 128-bit decimal class.
 *
 * #GArrowDecimal256 is a 256-bit decimal class.
 *
 * Since: 0.10.0
 */

typedef struct GArrowDecimal128Private_ {
  std::shared_ptr<arrow::Decimal128> decimal128;
} GArrowDecimal128Private;

enum {
  PROP_DECIMAL128 = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal128,
                           garrow_decimal128,
                           G_TYPE_OBJECT)

#define GARROW_DECIMAL128_GET_PRIVATE(obj)         \
  static_cast<GArrowDecimal128Private *>(          \
     garrow_decimal128_get_instance_private(       \
       GARROW_DECIMAL128(obj)))

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
  new(&priv->decimal128) std::shared_ptr<arrow::Decimal128>;
}

static void
garrow_decimal128_class_init(GArrowDecimal128Class *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_decimal128_finalize;
  gobject_class->set_property = garrow_decimal128_set_property;

  spec = g_param_spec_pointer("decimal128",
                              "Decimal128",
                              "The raw std::shared<arrow::Decimal128> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
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
  return garrow_decimal_new_string<arrow::Decimal128>(
    data, error, "[decimal128][new][string]");
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
garrow_decimal128_equal(GArrowDecimal128 *decimal,
                        GArrowDecimal128 *other_decimal)
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
garrow_decimal128_not_equal(GArrowDecimal128 *decimal,
                            GArrowDecimal128 *other_decimal)
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
garrow_decimal128_less_than(GArrowDecimal128 *decimal,
                            GArrowDecimal128 *other_decimal)
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
garrow_decimal128_greater_than(GArrowDecimal128 *decimal,
                               GArrowDecimal128 *other_decimal)
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
garrow_decimal128_plus(GArrowDecimal128 *left,
                       GArrowDecimal128 *right)
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
garrow_decimal128_minus(GArrowDecimal128 *left,
                        GArrowDecimal128 *right)
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
garrow_decimal128_multiply(GArrowDecimal128 *left,
                           GArrowDecimal128 *right)
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


typedef struct GArrowDecimal256Private_ {
  std::shared_ptr<arrow::Decimal256> decimal256;
} GArrowDecimal256Private;

enum {
  PROP_DECIMAL256 = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal256,
                           garrow_decimal256,
                           G_TYPE_OBJECT)

#define GARROW_DECIMAL256_GET_PRIVATE(obj)         \
  static_cast<GArrowDecimal256Private *>(          \
     garrow_decimal256_get_instance_private(       \
       GARROW_DECIMAL256(obj)))

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
  new(&priv->decimal256) std::shared_ptr<arrow::Decimal256>;
}

static void
garrow_decimal256_class_init(GArrowDecimal256Class *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_decimal256_finalize;
  gobject_class->set_property = garrow_decimal256_set_property;

  spec = g_param_spec_pointer("decimal256",
                              "Decimal256",
                              "The raw std::shared<arrow::Decimal256> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
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
  return garrow_decimal_new_string<arrow::Decimal256>(
    data, error, "[decimal256][new][string]");
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
garrow_decimal256_equal(GArrowDecimal256 *decimal,
                        GArrowDecimal256 *other_decimal)
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
garrow_decimal256_not_equal(GArrowDecimal256 *decimal,
                            GArrowDecimal256 *other_decimal)
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
garrow_decimal256_less_than(GArrowDecimal256 *decimal,
                            GArrowDecimal256 *other_decimal)
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
garrow_decimal256_greater_than(GArrowDecimal256 *decimal,
                               GArrowDecimal256 *other_decimal)
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
garrow_decimal256_plus(GArrowDecimal256 *left,
                       GArrowDecimal256 *right)
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
garrow_decimal256_multiply(GArrowDecimal256 *left,
                           GArrowDecimal256 *right)
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

GArrowDecimal128 *
garrow_decimal128_new_raw(std::shared_ptr<arrow::Decimal128> *arrow_decimal128)
{
  auto decimal128 = g_object_new(garrow_decimal128_get_type(),
                                 "decimal128", arrow_decimal128,
                                 NULL);
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
  auto decimal256 = g_object_new(garrow_decimal256_get_type(),
                                 "decimal256", arrow_decimal256,
                                 NULL);
  return GARROW_DECIMAL256(decimal256);
}

std::shared_ptr<arrow::Decimal256>
garrow_decimal256_get_raw(GArrowDecimal256 *decimal256)
{
  auto priv = GARROW_DECIMAL256_GET_PRIVATE(decimal256);
  return priv->decimal256;
}
