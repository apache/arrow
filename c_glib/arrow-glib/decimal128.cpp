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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/decimal128.hpp>
#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: decimal
 * @title: Decimal classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowDecimal128 is a 128-bit decimal class.
 *
 * Since: 0.10.0
 */

typedef struct GArrowDecimal128Private_ {
  std::shared_ptr<arrow::Decimal128> decimal128;
} GArrowDecimal128Private;

enum {
  PROP_0,
  PROP_DECIMAL128
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

  priv->decimal128 = nullptr;

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
 *
 * Returns: A newly created #GArrowDecimal128.
 *
 * Since: 0.10.0
 */
GArrowDecimal128 *
garrow_decimal128_new_string(const gchar *data)
{
  auto arrow_decimal = std::make_shared<arrow::Decimal128>(data);
  return garrow_decimal128_new_raw(&arrow_decimal);
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
  auto arrow_decimal = std::make_shared<arrow::Decimal128>(data);
  return garrow_decimal128_new_raw(&arrow_decimal);
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
  const auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  const auto arrow_other_decimal = garrow_decimal128_get_raw(other_decimal);
  return *arrow_decimal == *arrow_other_decimal;
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
  const auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  const auto arrow_other_decimal = garrow_decimal128_get_raw(other_decimal);
  return *arrow_decimal != *arrow_other_decimal;
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
  const auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  const auto arrow_other_decimal = garrow_decimal128_get_raw(other_decimal);
  return *arrow_decimal < *arrow_other_decimal;
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
  const auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  const auto arrow_other_decimal = garrow_decimal128_get_raw(other_decimal);
  return *arrow_decimal <= *arrow_other_decimal;
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
  const auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  const auto arrow_other_decimal = garrow_decimal128_get_raw(other_decimal);
  return *arrow_decimal > *arrow_other_decimal;
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
  const auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  const auto arrow_other_decimal = garrow_decimal128_get_raw(other_decimal);
  return *arrow_decimal >= *arrow_other_decimal;
}

/**
 * garrow_decimal128_to_string_scale:
 * @decimal: A #GArrowDecimal128.
 * @scale: The scale of the decimal.
 *
 * Returns: (transfer full): The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.10.0
 */
gchar *
garrow_decimal128_to_string_scale(GArrowDecimal128 *decimal, gint32 scale)
{
  auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  auto string =  arrow_decimal->ToString(scale);
  return g_strndup(string.data(), string.size());
}

/**
 * garrow_decimal128_to_string:
 * @decimal: A #GArrowDecimal128.
 *
 * Returns: (transfer full): The string representation of the decimal.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.10.0
 */
gchar *
garrow_decimal128_to_string(GArrowDecimal128 *decimal)
{
  auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  auto string =  arrow_decimal->ToIntegerString();
  return g_strndup(string.data(), string.size());
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
  auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  arrow_decimal->Abs();
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
  auto arrow_decimal = garrow_decimal128_get_raw(decimal);
  arrow_decimal->Negate();
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
  auto arrow_decimal_left = garrow_decimal128_get_raw(left);
  auto arrow_decimal_right = garrow_decimal128_get_raw(right);
  auto arrow_decimal =
    std::make_shared<arrow::Decimal128>(*arrow_decimal_left + *arrow_decimal_right);
  return garrow_decimal128_new_raw(&arrow_decimal);
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
  auto arrow_decimal_left = garrow_decimal128_get_raw(left);
  auto arrow_decimal_right = garrow_decimal128_get_raw(right);
  auto arrow_decimal =
    std::make_shared<arrow::Decimal128>(*arrow_decimal_left - *arrow_decimal_right);
  return garrow_decimal128_new_raw(&arrow_decimal);
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
  auto arrow_decimal_left = garrow_decimal128_get_raw(left);
  auto arrow_decimal_right = garrow_decimal128_get_raw(right);
  auto arrow_decimal =
    std::make_shared<arrow::Decimal128>(*arrow_decimal_left * *arrow_decimal_right);
  return garrow_decimal128_new_raw(&arrow_decimal);
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
  auto arrow_decimal_left = garrow_decimal128_get_raw(left);
  auto arrow_decimal_right = garrow_decimal128_get_raw(right);
  arrow::Decimal128 arrow_result_raw;
  arrow::Decimal128 arrow_remainder_raw;
  auto status =
    arrow_decimal_left->Divide(*arrow_decimal_right,
                               &arrow_result_raw,
                               &arrow_remainder_raw);
  if (garrow_error_check(error, status, "[decimal][divide]")) {
    if (remainder) {
      auto arrow_remainder =
        std::make_shared<arrow::Decimal128>(arrow_remainder_raw);
      *remainder = garrow_decimal128_new_raw(&arrow_remainder);
    }
    auto arrow_result = std::make_shared<arrow::Decimal128>(arrow_result_raw);
    return garrow_decimal128_new_raw(&arrow_result);
  } else {
    if (remainder) {
      *remainder = NULL;
    }
    return NULL;
  }
}

G_END_DECLS

GArrowDecimal128 *
garrow_decimal128_new_raw(std::shared_ptr<arrow::Decimal128> *arrow_decimal128)
{
  auto decimal = g_object_new(GARROW_TYPE_DECIMAL128,
                              "decimal128", arrow_decimal128,
                              NULL);
  return GARROW_DECIMAL128(decimal);
}

std::shared_ptr<arrow::Decimal128>
garrow_decimal128_get_raw(GArrowDecimal128 *decimal)
{
  auto priv = GARROW_DECIMAL128_GET_PRIVATE(decimal);
  return priv->decimal128;
}
