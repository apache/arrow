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

#include <sstream>

#include <arrow-glib/array.hpp>
#include <arrow-glib/compute.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/datum.hpp>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/table.hpp>

template <typename ArrowType, typename GArrowArrayType>
typename ArrowType::c_type
garrow_numeric_array_sum(GArrowArrayType array,
                         GError **error,
                         const gchar *tag,
                         typename ArrowType::c_type default_value)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_sum_datum = arrow::compute::Sum(arrow_array);
  if (garrow::check(error, arrow_sum_datum, tag)) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
    auto arrow_numeric_scalar =
      std::dynamic_pointer_cast<ScalarType>((*arrow_sum_datum).scalar());
    if (arrow_numeric_scalar->is_valid) {
      return arrow_numeric_scalar->value;
    } else {
      return default_value;
    }
  } else {
    return default_value;
  }
}

template <typename GArrowArrayType, typename VALUE>
GArrowBooleanArray *
garrow_numeric_array_compare(GArrowArrayType array,
                             VALUE value,
                             GArrowCompareOptions *options,
                             GError **error,
                             const gchar *tag)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_options = garrow_compare_options_get_raw(options);
  auto arrow_compared_datum = arrow::compute::Compare(arrow_array,
                                                      arrow::Datum(value),
                                                      *arrow_options);
  if (garrow::check(error, arrow_compared_datum, tag)) {
    auto arrow_compared_array = (*arrow_compared_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_compared_array));
  } else {
    return NULL;
  }
}

template <typename GArrowTypeNewRaw>
auto
garrow_take(arrow::Datum arrow_values,
            arrow::Datum arrow_indices,
            GArrowTakeOptions *options,
            GArrowTypeNewRaw garrow_type_new_raw,
            GError **error,
            const gchar *tag) -> decltype(garrow_type_new_raw(arrow::Datum()))
{
  arrow::Result<arrow::Datum> arrow_taken_datum;
  if (options) {
    auto arrow_options = garrow_take_options_get_raw(options);
    arrow_taken_datum = arrow::compute::Take(arrow_values,
                                             arrow_indices,
                                             *arrow_options);
  } else {
    arrow_taken_datum = arrow::compute::Take(arrow_values,
                                             arrow_indices);
  }
  if (garrow::check(error, arrow_taken_datum, tag)) {
    return garrow_type_new_raw(*arrow_taken_datum);
  } else {
    return NULL;
  }
}

G_BEGIN_DECLS

/**
 * SECTION: compute
 * @section_id: compute
 * @title: Computation on array
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowExecuteContext is a class to customize how to execute a
 * function.
 *
 * #GArrowFunctionOptions is an interface for function options. All
 * function options such as #GArrowCastOptions must implement this
 * interface.
 *
 * #GArrowFunction is a class to process data.
 *
 * #GArrowCastOptions is a class to customize the `cast` function and
 * garrow_array_cast().
 *
 * #GArrowCountOptions is a class to customize the `count` function and
 * garrow_array_count().
 *
 * #GArrowFilterOptions is a class to customize the `filter` function and
 * garrow_array_filter() family.
 *
 * #GArrowTakeOptions is a class to customize the `take` function and
 * garrow_array_take() family.
 *
 * #GArrowCompareOptions is a class to customize the `equal` function
 * family and garrow_int8_array_compare() family.
 *
 * There are many functions to compute data on an array.
 */

typedef struct GArrowExecuteContextPrivate_ {
  arrow::compute::ExecContext context;
} GArrowExecuteContextPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExecuteContext,
                           garrow_execute_context,
                           G_TYPE_OBJECT)

#define GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object) \
  static_cast<GArrowExecuteContextPrivate *>(      \
    garrow_execute_context_get_instance_private(   \
      GARROW_EXECUTE_CONTEXT(object)))

static void
garrow_execute_context_finalize(GObject *object)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);
  priv->context.~ExecContext();
  G_OBJECT_CLASS(garrow_execute_context_parent_class)->finalize(object);
}

static void
garrow_execute_context_init(GArrowExecuteContext *object)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);
  new(&priv->context) arrow::compute::ExecContext(arrow::default_memory_pool(),
                                                  nullptr);
}

static void
garrow_execute_context_class_init(GArrowExecuteContextClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_execute_context_finalize;
}

/**
 * garrow_execute_context_new:
 *
 * Returns: A newly created #GArrowExecuteContext.
 *
 * Since: 1.0.0
 */
GArrowExecuteContext *
garrow_execute_context_new(void)
{
  auto execute_context = g_object_new(GARROW_TYPE_EXECUTE_CONTEXT, NULL);
  return GARROW_EXECUTE_CONTEXT(execute_context);
}


G_DEFINE_INTERFACE(GArrowFunctionOptions,
                   garrow_function_options,
                   G_TYPE_INVALID)

static void
garrow_function_options_default_init(GArrowFunctionOptionsInterface *iface)
{
}


typedef struct GArrowFunctionPrivate_ {
  std::shared_ptr<arrow::compute::Function> function;
} GArrowFunctionPrivate;

enum {
  PROP_FUNCTION = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFunction,
                           garrow_function,
                           G_TYPE_OBJECT)

#define GARROW_FUNCTION_GET_PRIVATE(object)        \
  static_cast<GArrowFunctionPrivate *>(            \
    garrow_function_get_instance_private(          \
      GARROW_FUNCTION(object)))

static void
garrow_function_finalize(GObject *object)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(object);
  priv->function.~shared_ptr();
  G_OBJECT_CLASS(garrow_function_parent_class)->finalize(object);
}

static void
garrow_function_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FUNCTION:
    priv->function =
      *static_cast<std::shared_ptr<arrow::compute::Function> *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_function_init(GArrowFunction *object)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(object);
  new(&priv->function) std::shared_ptr<arrow::compute::Function>;
}

static void
garrow_function_class_init(GArrowFunctionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_function_finalize;
  gobject_class->set_property = garrow_function_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("function",
                              "Function",
                              "The raw std::shared<arrow::compute::Function> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FUNCTION, spec);
}

/**
 * garrow_function_find:
 * @name: A function name to be found.
 *
 * Returns: (transfer full):
 *   The found #GArrowFunction or %NULL on not found.
 *
 * Since: 1.0.0
 */
GArrowFunction *
garrow_function_find(const gchar *name)
{
  auto arrow_function_registry = arrow::compute::GetFunctionRegistry();
  auto arrow_function_result = arrow_function_registry->GetFunction(name);
  if (!arrow_function_result.ok()) {
    return NULL;
  }
  auto arrow_function = *arrow_function_result;
  return garrow_function_new_raw(&arrow_function);
}

/**
 * garrow_function_execute:
 * @function: A #GArrowFunction.
 * @args: (element-type GArrowDatum): A list of #GArrowDatum.
 * @options: (nullable): Options for the execution as an object that
 *   implements  #GArrowFunctionOptions.
 * @context: (nullable): A #GArrowExecuteContext for the execution.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A return value of the execution as #GArrowData on success, %NULL on error.
 *
 * Since: 1.0.0
 */
GArrowDatum *
garrow_function_execute(GArrowFunction *function,
                        GList *args,
                        GArrowFunctionOptions *options,
                        GArrowExecuteContext *context,
                        GError **error)
{
  auto arrow_function = garrow_function_get_raw(function);
  std::vector<arrow::Datum> arrow_args;
  for (GList *node = args; node; node = node->next) {
    GArrowDatum *datum = GARROW_DATUM(node->data);
    arrow_args.push_back(garrow_datum_get_raw(datum));
  }
  const arrow::compute::FunctionOptions *arrow_options;
  if (options) {
    arrow_options = garrow_function_options_get_raw(options);
  } else {
    arrow_options = arrow_function->default_options();
  }
  arrow::Result<arrow::Datum> arrow_result_result;
  if (context) {
    auto arrow_context = garrow_execute_context_get_raw(context);
    arrow_result_result = arrow_function->Execute(arrow_args,
                                                  arrow_options,
                                                  arrow_context);
  } else {
    arrow::compute::ExecContext arrow_context;
    arrow_result_result = arrow_function->Execute(arrow_args,
                                                  arrow_options,
                                                  &arrow_context);
  }
  if (garrow::check(error, arrow_result_result, "[function][execute]")) {
    auto arrow_result = *arrow_result_result;
    return garrow_datum_new_raw(&arrow_result);
  } else {
    return NULL;
  }
}


typedef struct GArrowCastOptionsPrivate_ {
  GArrowDataType *to_data_type;
  arrow::compute::CastOptions options;
} GArrowCastOptionsPrivate;

enum {
  PROP_TO_DATA_TYPE = 1,
  PROP_ALLOW_INT_OVERFLOW,
  PROP_ALLOW_TIME_TRUNCATE,
  PROP_ALLOW_TIME_OVERFLOW,
  PROP_ALLOW_DECIMAL_TRUNCATE,
  PROP_ALLOW_FLOAT_TRUNCATE,
  PROP_ALLOW_INVALID_UTF8,
};

static arrow::compute::FunctionOptions *
garrow_cast_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_cast_options_get_raw(GARROW_CAST_OPTIONS(options));
}

static void
garrow_cast_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_cast_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowCastOptions,
                        garrow_cast_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowCastOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_cast_options_function_options_interface_init))

#define GARROW_CAST_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowCastOptionsPrivate *>(      \
    garrow_cast_options_get_instance_private(   \
      GARROW_CAST_OPTIONS(object)))

static void
garrow_cast_options_dispose(GObject *object)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  if (priv->to_data_type) {
    g_object_unref(priv->to_data_type);
    priv->to_data_type = NULL;
  }

  G_OBJECT_CLASS(garrow_cast_options_parent_class)->dispose(object);
}

static void
garrow_cast_options_finalize(GObject *object)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);
  priv->options.~CastOptions();
  G_OBJECT_CLASS(garrow_cast_options_parent_class)->finalize(object);
}

static void
garrow_cast_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TO_DATA_TYPE:
    {
      auto to_data_type = g_value_dup_object(value);
      if (priv->to_data_type) {
        g_object_unref(priv->to_data_type);
      }
      if (to_data_type) {
        priv->to_data_type = GARROW_DATA_TYPE(to_data_type);
        priv->options.to_type = garrow_data_type_get_raw(priv->to_data_type);
      } else {
        priv->to_data_type = NULL;
        priv->options.to_type = nullptr;
      }
      break;
    }
  case PROP_ALLOW_INT_OVERFLOW:
    priv->options.allow_int_overflow = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_TIME_TRUNCATE:
    priv->options.allow_time_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_TIME_OVERFLOW:
    priv->options.allow_time_overflow = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_DECIMAL_TRUNCATE:
    priv->options.allow_decimal_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_FLOAT_TRUNCATE:
    priv->options.allow_float_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_INVALID_UTF8:
    priv->options.allow_invalid_utf8 = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TO_DATA_TYPE:
    g_value_set_object(value, priv->to_data_type);
    break;
  case PROP_ALLOW_INT_OVERFLOW:
    g_value_set_boolean(value, priv->options.allow_int_overflow);
    break;
  case PROP_ALLOW_TIME_TRUNCATE:
    g_value_set_boolean(value, priv->options.allow_time_truncate);
    break;
  case PROP_ALLOW_TIME_OVERFLOW:
    g_value_set_boolean(value, priv->options.allow_time_overflow);
    break;
  case PROP_ALLOW_DECIMAL_TRUNCATE:
    g_value_set_boolean(value, priv->options.allow_decimal_truncate);
    break;
  case PROP_ALLOW_FLOAT_TRUNCATE:
    g_value_set_boolean(value, priv->options.allow_float_truncate);
    break;
  case PROP_ALLOW_INVALID_UTF8:
    g_value_set_boolean(value, priv->options.allow_invalid_utf8);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_init(GArrowCastOptions *object)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::CastOptions;
}

static void
garrow_cast_options_class_init(GArrowCastOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_cast_options_dispose;
  gobject_class->finalize     = garrow_cast_options_finalize;
  gobject_class->set_property = garrow_cast_options_set_property;
  gobject_class->get_property = garrow_cast_options_get_property;

  GParamSpec *spec;

  /**
   * GArrowCastOptions:to-data-type:
   *
   * The GArrowDataType being casted to.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_object("to-data-type",
                             "To data type",
                             "The GArrowDataType being casted to",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_TO_DATA_TYPE, spec);

  /**
   * GArrowCastOptions:allow-int-overflow:
   *
   * Whether integer overflow is allowed or not.
   *
   * Since: 0.7.0
   */
  spec = g_param_spec_boolean("allow-int-overflow",
                              "Allow int overflow",
                              "Whether integer overflow is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_INT_OVERFLOW, spec);

  /**
   * GArrowCastOptions:allow-time-truncate:
   *
   * Whether truncating time value is allowed or not.
   *
   * Since: 0.8.0
   */
  spec = g_param_spec_boolean("allow-time-truncate",
                              "Allow time truncate",
                              "Whether truncating time value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_TIME_TRUNCATE, spec);

  /**
   * GArrowCastOptions:allow-time-overflow:
   *
   * Whether time overflow is allowed or not.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("allow-time-overflow",
                              "Allow time overflow",
                              "Whether time overflow is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_TIME_OVERFLOW, spec);

  /**
   * GArrowCastOptions:allow-decimal-truncate:
   *
   * Whether truncating decimal value is allowed or not.
   *
   * Since: 1.0.0
   */
  spec = g_param_spec_boolean("allow-decimal-truncate",
                              "Allow decimal truncate",
                              "Whether truncating decimal value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_DECIMAL_TRUNCATE, spec);

  /**
   * GArrowCastOptions:allow-float-truncate:
   *
   * Whether truncating float value is allowed or not.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("allow-float-truncate",
                              "Allow float truncate",
                              "Whether truncating float value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_FLOAT_TRUNCATE, spec);

  /**
   * GArrowCastOptions:allow-invalid-utf8:
   *
   * Whether invalid UTF-8 string value is allowed or not.
   *
   * Since: 0.13.0
   */
  spec = g_param_spec_boolean("allow-invalid-utf8",
                              "Allow invalid UTF-8",
                              "Whether invalid UTF-8 string value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_INVALID_UTF8, spec);
}

/**
 * garrow_cast_options_new:
 *
 * Returns: A newly created #GArrowCastOptions.
 *
 * Since: 0.7.0
 */
GArrowCastOptions *
garrow_cast_options_new(void)
{
  auto cast_options = g_object_new(GARROW_TYPE_CAST_OPTIONS, NULL);
  return GARROW_CAST_OPTIONS(cast_options);
}


typedef struct GArrowCountOptionsPrivate_ {
  arrow::compute::CountOptions options;
} GArrowCountOptionsPrivate;

enum {
  PROP_MODE = 1,
};

static arrow::compute::FunctionOptions *
garrow_count_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_count_options_get_raw(GARROW_COUNT_OPTIONS(options));
}

static void
garrow_count_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_count_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowCountOptions,
                        garrow_count_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowCountOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_count_options_function_options_interface_init))

#define GARROW_COUNT_OPTIONS_GET_PRIVATE(object)        \
  static_cast<GArrowCountOptionsPrivate *>(             \
    garrow_count_options_get_instance_private(          \
      GARROW_COUNT_OPTIONS(object)))

static void
garrow_count_options_finalize(GObject *object)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(object);
  priv->options.~CountOptions();
  G_OBJECT_CLASS(garrow_count_options_parent_class)->finalize(object);
}

static void
garrow_count_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_MODE:
    priv->options.count_mode =
      static_cast<arrow::compute::CountOptions::Mode>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_count_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_MODE:
    g_value_set_enum(value, priv->options.count_mode);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_count_options_init(GArrowCountOptions *object)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::CountOptions(
    arrow::compute::CountOptions::COUNT_NON_NULL);
}

static void
garrow_count_options_class_init(GArrowCountOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_count_options_finalize;
  gobject_class->set_property = garrow_count_options_set_property;
  gobject_class->get_property = garrow_count_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowCountOptions:mode:
   *
   * How to count values.
   *
   * Since: 0.13.0
   */
  spec = g_param_spec_enum("mode",
                           "Mode",
                           "How to count values",
                           GARROW_TYPE_COUNT_MODE,
                           GARROW_COUNT_ALL,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MODE, spec);
}

/**
 * garrow_count_options_new:
 *
 * Returns: A newly created #GArrowCountOptions.
 *
 * Since: 0.13.0
 */
GArrowCountOptions *
garrow_count_options_new(void)
{
  auto count_options = g_object_new(GARROW_TYPE_COUNT_OPTIONS, NULL);
  return GARROW_COUNT_OPTIONS(count_options);
}


typedef struct GArrowFilterOptionsPrivate_ {
  arrow::compute::FilterOptions options;
} GArrowFilterOptionsPrivate;

enum {
  PROP_NULL_SELECTION_BEHAVIOR = 1,
};

static arrow::compute::FunctionOptions *
garrow_filter_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_filter_options_get_raw(GARROW_FILTER_OPTIONS(options));
}

static void
garrow_filter_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_filter_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowFilterOptions,
                        garrow_filter_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowFilterOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_filter_options_function_options_interface_init))

#define GARROW_FILTER_OPTIONS_GET_PRIVATE(object)        \
  static_cast<GArrowFilterOptionsPrivate *>(             \
    garrow_filter_options_get_instance_private(          \
      GARROW_FILTER_OPTIONS(object)))

static void
garrow_filter_options_finalize(GObject *object)
{
  auto priv = GARROW_FILTER_OPTIONS_GET_PRIVATE(object);
  priv->options.~FilterOptions();
  G_OBJECT_CLASS(garrow_filter_options_parent_class)->finalize(object);
}

static void
garrow_filter_options_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GARROW_FILTER_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_NULL_SELECTION_BEHAVIOR:
    priv->options.null_selection_behavior =
      static_cast<arrow::compute::FilterOptions::NullSelectionBehavior>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_filter_options_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_FILTER_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_NULL_SELECTION_BEHAVIOR:
    g_value_set_enum(value, priv->options.null_selection_behavior);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_filter_options_init(GArrowFilterOptions *object)
{
  auto priv = GARROW_FILTER_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::FilterOptions;
}

static void
garrow_filter_options_class_init(GArrowFilterOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_filter_options_finalize;
  gobject_class->set_property = garrow_filter_options_set_property;
  gobject_class->get_property = garrow_filter_options_get_property;

  arrow::compute::FilterOptions default_options;

  GParamSpec *spec;
  /**
   * GArrowFilterOptions:null_selection_behavior:
   *
   * How to handle filtered values.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_enum("null_selection_behavior",
                           "Null selection behavior",
                           "How to handle filtered values",
                           GARROW_TYPE_FILTER_NULL_SELECTION_BEHAVIOR,
                           static_cast<GArrowFilterNullSelectionBehavior>(
                             default_options.null_selection_behavior),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_NULL_SELECTION_BEHAVIOR, spec);
}

/**
 * garrow_filter_options_new:
 *
 * Returns: A newly created #GArrowFilterOptions.
 *
 * Since: 0.17.0
 */
GArrowFilterOptions *
garrow_filter_options_new(void)
{
  auto filter_options = g_object_new(GARROW_TYPE_FILTER_OPTIONS, NULL);
  return GARROW_FILTER_OPTIONS(filter_options);
}


typedef struct GArrowTakeOptionsPrivate_ {
  arrow::compute::TakeOptions options;
} GArrowTakeOptionsPrivate;

static arrow::compute::FunctionOptions *
garrow_take_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_take_options_get_raw(GARROW_TAKE_OPTIONS(options));
}

static void
garrow_take_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_take_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowTakeOptions,
                        garrow_take_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowTakeOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_take_options_function_options_interface_init))

#define GARROW_TAKE_OPTIONS_GET_PRIVATE(object)        \
  static_cast<GArrowTakeOptionsPrivate *>(             \
    garrow_take_options_get_instance_private(          \
      GARROW_TAKE_OPTIONS(object)))

static void
garrow_take_options_finalize(GObject *object)
{
  auto priv = GARROW_TAKE_OPTIONS_GET_PRIVATE(object);
  priv->options.~TakeOptions();
  G_OBJECT_CLASS(garrow_take_options_parent_class)->finalize(object);
}

static void
garrow_take_options_init(GArrowTakeOptions *object)
{
  auto priv = GARROW_TAKE_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::TakeOptions;
}

static void
garrow_take_options_class_init(GArrowTakeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_take_options_finalize;
}

/**
 * garrow_take_options_new:
 *
 * Returns: A newly created #GArrowTakeOptions.
 *
 * Since: 0.14.0
 */
GArrowTakeOptions *
garrow_take_options_new(void)
{
  auto take_options = g_object_new(GARROW_TYPE_TAKE_OPTIONS, NULL);
  return GARROW_TAKE_OPTIONS(take_options);
}


typedef struct GArrowCompareOptionsPrivate_ {
  arrow::compute::CompareOptions options;
} GArrowCompareOptionsPrivate;

enum {
  PROP_OPERATOR = 1,
};

static arrow::compute::FunctionOptions *
garrow_compare_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_compare_options_get_raw(GARROW_COMPARE_OPTIONS(options));
}

static void
garrow_compare_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_compare_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowCompareOptions,
                        garrow_compare_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowCompareOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_compare_options_function_options_interface_init))

#define GARROW_COMPARE_OPTIONS_GET_PRIVATE(object)        \
  static_cast<GArrowCompareOptionsPrivate *>(             \
    garrow_compare_options_get_instance_private(          \
      GARROW_COMPARE_OPTIONS(object)))

static void
garrow_compare_options_finalize(GObject *object)
{
  auto priv = GARROW_COMPARE_OPTIONS_GET_PRIVATE(object);
  priv->options.~CompareOptions();
  G_OBJECT_CLASS(garrow_compare_options_parent_class)->finalize(object);
}

static void
garrow_compare_options_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_COMPARE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OPERATOR:
    priv->options.op =
      static_cast<arrow::compute::CompareOperator>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_compare_options_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_COMPARE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OPERATOR:
    g_value_set_enum(value, priv->options.op);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_compare_options_init(GArrowCompareOptions *object)
{
  auto priv = GARROW_COMPARE_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::CompareOptions(arrow::compute::EQUAL);
}

static void
garrow_compare_options_class_init(GArrowCompareOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_compare_options_finalize;
  gobject_class->set_property = garrow_compare_options_set_property;
  gobject_class->get_property = garrow_compare_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowCompareOptions:operator:
   *
   * How to compare the value.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_enum("operator",
                           "Operator",
                           "How to compare the value",
                           GARROW_TYPE_COMPARE_OPERATOR,
                           0,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_OPERATOR, spec);
}

/**
 * garrow_compare_options_new:
 *
 * Returns: A newly created #GArrowCompareOptions.
 *
 * Since: 0.14.0
 */
GArrowCompareOptions *
garrow_compare_options_new(void)
{
  auto compare_options = g_object_new(GARROW_TYPE_COMPARE_OPTIONS, NULL);
  return GARROW_COMPARE_OPTIONS(compare_options);
}


/**
 * garrow_array_cast:
 * @array: A #GArrowArray.
 * @target_data_type: A #GArrowDataType of cast target data.
 * @options: (nullable): A #GArrowCastOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created casted array on success, %NULL on error.
 *
 * Since: 0.7.0
 */
GArrowArray *
garrow_array_cast(GArrowArray *array,
                  GArrowDataType *target_data_type,
                  GArrowCastOptions *options,
                  GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  auto arrow_target_data_type = garrow_data_type_get_raw(target_data_type);
  arrow::Result<std::shared_ptr<arrow::Array>> arrow_casted_array;
  if (options) {
    auto arrow_options = garrow_cast_options_get_raw(options);
    arrow_casted_array = arrow::compute::Cast(*arrow_array_raw,
                                              arrow_target_data_type,
                                              *arrow_options);
  } else {
    arrow_casted_array = arrow::compute::Cast(*arrow_array_raw,
                                              arrow_target_data_type);
  }
  if (garrow::check(error,
                    arrow_casted_array,
                    [&]() {
                      std::stringstream message;
                      message << "[array][cast] <";
                      message << arrow_array->type()->ToString();
                      message << "> -> <";
                      message << arrow_target_data_type->ToString();
                      message << ">";
                      return message.str();
                    })) {
    return garrow_array_new_raw(&(*arrow_casted_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_array_unique:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created unique elements array on success, %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowArray *
garrow_array_unique(GArrowArray *array,
                    GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_unique_array = arrow::compute::Unique(arrow_array);
  if (garrow::check(error,
                    arrow_unique_array,
                    [&]() {
                      std::stringstream message;
                      message << "[array][unique] <";
                      message << arrow_array->type()->ToString();
                      message << ">";
                      return message.str();
                    })) {
    return garrow_array_new_raw(&(*arrow_unique_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_array_dictionary_encode:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created #GArrowDictionaryArray for the @array on success,
 *   %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowDictionaryArray *
garrow_array_dictionary_encode(GArrowArray *array,
                               GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_dictionary_encoded_datum =
    arrow::compute::DictionaryEncode(arrow_array);
  if (garrow::check(error,
                    arrow_dictionary_encoded_datum,
                    [&]() {
                      std::stringstream message;
                      message << "[array][dictionary-encode] <";
                      message << arrow_array->type()->ToString();
                      message << ">";
                      return message.str();
                    })) {
    auto arrow_dictionary_encoded_array =
      (*arrow_dictionary_encoded_datum).make_array();
    auto dictionary_encoded_array =
      garrow_array_new_raw(&arrow_dictionary_encoded_array);
    return GARROW_DICTIONARY_ARRAY(dictionary_encoded_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_array_count:
 * @array: A #GArrowArray.
 * @options: (nullable): A #GArrowCountOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The number of target values on success. If an error is occurred,
 *   the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_array_count(GArrowArray *array,
                   GArrowCountOptions *options,
                   GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  arrow::Result<arrow::Datum> arrow_counted_datum;
  if (options) {
    auto arrow_options = garrow_count_options_get_raw(options);
    arrow_counted_datum =
      arrow::compute::Count(*arrow_array_raw, *arrow_options);
  } else {
    arrow_counted_datum = arrow::compute::Count(*arrow_array_raw);
  }
  if (garrow::check(error, arrow_counted_datum, "[array][count]")) {
    using ScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
    auto arrow_counted_scalar =
      std::dynamic_pointer_cast<ScalarType>((*arrow_counted_datum).scalar());
    return arrow_counted_scalar->value;
  } else {
    return 0;
  }
}

/**
 * garrow_array_count_values:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A #GArrowStructArray of `{input type "values", int64_t "counts"}`
 *   on success, %NULL on error.
 *
 * Since: 0.13.0
 */
GArrowStructArray *
garrow_array_count_values(GArrowArray *array,
                          GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_counted_values = arrow::compute::ValueCounts(arrow_array);
  if (garrow::check(error, arrow_counted_values, "[array][count-values]")) {
    return GARROW_STRUCT_ARRAY(garrow_array_new_raw(&(*arrow_counted_values)));
  } else {
    return NULL;
  }
}


/**
 * garrow_boolean_array_invert:
 * @array: A #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise inverted boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_invert(GArrowBooleanArray *array,
                            GError **error)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_inverted_datum = arrow::compute::Invert(arrow_array);
  if (garrow::check(error, arrow_inverted_datum, "[boolean-array][invert]")) {
    auto arrow_inverted_array = (*arrow_inverted_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_inverted_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_boolean_array_and:
 * @left: A left hand side #GArrowBooleanArray.
 * @right: A right hand side #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise AND operated boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_and(GArrowBooleanArray *left,
                         GArrowBooleanArray *right,
                         GError **error)
{
  auto arrow_left = garrow_array_get_raw(GARROW_ARRAY(left));
  auto arrow_right = garrow_array_get_raw(GARROW_ARRAY(right));
  auto arrow_operated_datum = arrow::compute::And(arrow_left, arrow_right);
  if (garrow::check(error, arrow_operated_datum, "[boolean-array][and]")) {
    auto arrow_operated_array = (*arrow_operated_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_operated_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_boolean_array_or:
 * @left: A left hand side #GArrowBooleanArray.
 * @right: A right hand side #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise OR operated boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_or(GArrowBooleanArray *left,
                        GArrowBooleanArray *right,
                        GError **error)
{
  auto arrow_left = garrow_array_get_raw(GARROW_ARRAY(left));
  auto arrow_right = garrow_array_get_raw(GARROW_ARRAY(right));
  auto arrow_operated_datum = arrow::compute::Or(arrow_left, arrow_right);
  if (garrow::check(error, arrow_operated_datum, "[boolean-array][or]")) {
    auto arrow_operated_array = (*arrow_operated_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_operated_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_boolean_array_xor:
 * @left: A left hand side #GArrowBooleanArray.
 * @right: A right hand side #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise XOR operated boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_xor(GArrowBooleanArray *left,
                         GArrowBooleanArray *right,
                         GError **error)
{
  auto arrow_left = garrow_array_get_raw(GARROW_ARRAY(left));
  auto arrow_right = garrow_array_get_raw(GARROW_ARRAY(right));
  auto arrow_operated_datum = arrow::compute::Xor(arrow_left, arrow_right);
  if (garrow::check(error, arrow_operated_datum, "[boolean-array][xor]")) {
    auto arrow_operated_array = (*arrow_operated_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_operated_array));
  } else {
    return NULL;
  }
}


/**
 * garrow_numeric_array_mean:
 * @array: A #GArrowNumericArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed mean.
 *
 * Since: 0.13.0
 */
gdouble
garrow_numeric_array_mean(GArrowNumericArray *array,
                          GError **error)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_mean_datum = arrow::compute::Mean(arrow_array);
  if (garrow::check(error, arrow_mean_datum, "[numeric-array][mean]")) {
    using ScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    auto arrow_numeric_scalar =
      std::dynamic_pointer_cast<ScalarType>((*arrow_mean_datum).scalar());
    if (arrow_numeric_scalar->is_valid) {
      return arrow_numeric_scalar->value;
    } else {
      return 0.0;
    }
  } else {
    return 0.0;
  }
}


/**
 * garrow_int8_array_sum:
 * @array: A #GArrowInt8Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int8_array_sum(GArrowInt8Array *array,
                      GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int8-array][sum]",
                                                    0);
}

/**
 * garrow_uint8_array_sum:
 * @array: A #GArrowUInt8Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint8_array_sum(GArrowUInt8Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                     error,
                                                     "[uint8-array][sum]",
                                                     0);
}

/**
 * garrow_int16_array_sum:
 * @array: A #GArrowInt16Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int16_array_sum(GArrowInt16Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int16-array][sum]",
                                                    0);
}

/**
 * garrow_uint16_array_sum:
 * @array: A #GArrowUInt16Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint16_array_sum(GArrowUInt16Array *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                     error,
                                                     "[uint16-array][sum]",
                                                     0);
}

/**
 * garrow_int32_array_sum:
 * @array: A #GArrowInt32Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int32_array_sum(GArrowInt32Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int32-array][sum]",
                                                    0);
}

/**
 * garrow_uint32_array_sum:
 * @array: A #GArrowUInt32Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint32_array_sum(GArrowUInt32Array *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                    error,
                                                    "[uint32-array][sum]",
                                                    0);
}

/**
 * garrow_int64_array_sum:
 * @array: A #GArrowInt64Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int64_array_sum(GArrowInt64Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int64-array][sum]",
                                                    0);
}

/**
 * garrow_uint64_array_sum:
 * @array: A #GArrowUInt64Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint64_array_sum(GArrowUInt64Array *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                    error,
                                                    "[uint64-array][sum]",
                                                    0);
}

/**
 * garrow_float_array_sum:
 * @array: A #GArrowFloatArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gdouble
garrow_float_array_sum(GArrowFloatArray *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::DoubleType>(array,
                                                     error,
                                                     "[float-array][sum]",
                                                     0);
}

/**
 * garrow_double_array_sum:
 * @array: A #GArrowDoubleArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gdouble
garrow_double_array_sum(GArrowDoubleArray *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::DoubleType>(array,
                                                     error,
                                                     "[double-array][sum]",
                                                     0);
}

/**
 * garrow_array_take:
 * @array: A #GArrowArray.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowArray taken from
 *   an array of values at indices in input array or %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowArray *
garrow_array_take(GArrowArray *array,
                  GArrowArray *indices,
                  GArrowTakeOptions *options,
                  GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_indices = garrow_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_array),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_array = arrow_datum.make_array();
      return garrow_array_new_raw(&arrow_taken_array);
    },
    error,
    "[array][take][array]");
}

/**
 * garrow_array_take_chunked_array:
 * @array: A #GArrowArray.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray taken from
 *   an array of values at indices in chunked array or %NULL on error.
 *
 * Since: 0.16.0
 */
GArrowChunkedArray *
garrow_array_take_chunked_array(GArrowArray *array,
                                GArrowChunkedArray *indices,
                                GArrowTakeOptions *options,
                                GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_indices = garrow_chunked_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_array),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_chunked_array = arrow_datum.chunked_array();
      return garrow_chunked_array_new_raw(&arrow_taken_chunked_array);
    },
    error,
    "[array][take][chunked-array]");
}

/**
 * garrow_table_take:
 * @table: A #GArrowTable.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowTable taken from
 *   an array of values at indices in input array or %NULL on error.
 *
 * Since: 0.16.0
 */
GArrowTable *
garrow_table_take(GArrowTable *table,
                  GArrowArray *indices,
                  GArrowTakeOptions *options,
                  GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_indices = garrow_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_table),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_table = arrow_datum.table();
      return garrow_table_new_raw(&arrow_taken_table);
    },
    error,
    "[table][take]");
}

/**
 * garrow_table_take_chunked_array:
 * @table: A #GArrowTable.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowTable taken from
 *   an array of values at indices in chunked array or %NULL on error.
 *
 * Since: 0.16.0
 */
GArrowTable *
garrow_table_take_chunked_array(GArrowTable *table,
                                GArrowChunkedArray *indices,
                                GArrowTakeOptions *options,
                                GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_indices = garrow_chunked_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_table),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_table = arrow_datum.table();
      return garrow_table_new_raw(&arrow_taken_table);
    },
    error,
    "[table][take][chunked-array]");
}

/**
 * garrow_chunked_array_take:
 * @chunked_array: A #GArrowChunkedArray.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray taken from
 *   an array of values at indices in input array or %NULL on error.
 *
 * Since: 0.16.0
 */
GArrowChunkedArray *
garrow_chunked_array_take(GArrowChunkedArray *chunked_array,
                          GArrowArray *indices,
                          GArrowTakeOptions *options,
                          GError **error)
{
  auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_indices = garrow_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_chunked_array),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_chunked_array = arrow_datum.chunked_array();
      return garrow_chunked_array_new_raw(&arrow_taken_chunked_array);
    },
    error,
    "[chunked-array][take]");
}

/**
 * garrow_chunked_array_take_chunked_array:
 * @chunked_array: A #GArrowChunkedArray.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray taken from
 *   an array of values at indices in chunked array or %NULL on error.
 *
 * Since: 0.16.0
 */
GArrowChunkedArray *
garrow_chunked_array_take_chunked_array(GArrowChunkedArray *chunked_array,
                                        GArrowChunkedArray *indices,
                                        GArrowTakeOptions *options,
                                        GError **error)
{
  auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_indices = garrow_chunked_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_chunked_array),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_chunked_array = arrow_datum.chunked_array();
      return garrow_chunked_array_new_raw(&arrow_taken_chunked_array);
    },
    error,
    "[chunked-array][take][chunked-array]");
}

/**
 * garrow_record_batch_take:
 * @record_batch: A #GArrowRecordBatch.
 * @indices: The indices of values to take.
 * @options: (nullable): A #GArrowTakeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray taken from
 *   an array of values at indices in input array or %NULL on error.
 *
 * Since: 0.16.0
 */
GArrowRecordBatch *
garrow_record_batch_take(GArrowRecordBatch *record_batch,
                         GArrowArray *indices,
                         GArrowTakeOptions *options,
                         GError **error)
{
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_indices = garrow_array_get_raw(indices);
  return garrow_take(
    arrow::Datum(arrow_record_batch),
    arrow::Datum(arrow_indices),
    options,
    [](arrow::Datum arrow_datum) {
      auto arrow_taken_record_batch = arrow_datum.record_batch();
      return garrow_record_batch_new_raw(&arrow_taken_record_batch);
    },
    error,
    "[record-batch][take]");
}


/**
 * garrow_int8_array_compare:
 * @array: A #GArrowInt8Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_int8_array_compare(GArrowInt8Array *array,
                          gint8 value,
                          GArrowCompareOptions *options,
                          GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[int8-array][compare]");
}

/**
 * garrow_uint8_array_compare:
 * @array: A #GArrowUInt8Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_uint8_array_compare(GArrowUInt8Array *array,
                           guint8 value,
                           GArrowCompareOptions *options,
                           GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[uint8-array][compare]");
}

/**
 * garrow_int16_array_compare:
 * @array: A #GArrowInt16Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_int16_array_compare(GArrowInt16Array *array,
                           gint16 value,
                           GArrowCompareOptions *options,
                           GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[int16-array][compare]");
}

/**
 * garrow_uint16_array_compare:
 * @array: A #GArrowUInt16Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_uint16_array_compare(GArrowUInt16Array *array,
                            guint16 value,
                            GArrowCompareOptions *options,
                            GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[uint16-array][compare]");
}

/**
 * garrow_int32_array_compare:
 * @array: A #GArrowUInt32Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_int32_array_compare(GArrowInt32Array *array,
                           gint32 value,
                           GArrowCompareOptions *options,
                           GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[int32-array][compare]");
}

/**
 * garrow_uint32_array_compare:
 * @array: A #GArrowUInt32Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_uint32_array_compare(GArrowUInt32Array *array,
                            guint32 value,
                            GArrowCompareOptions *options,
                            GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[uint32-array][compare]");
}

/**
 * garrow_int64_array_compare:
 * @array: A #GArrowInt64Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_int64_array_compare(GArrowInt64Array *array,
                           gint64 value,
                           GArrowCompareOptions *options,
                           GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[int64-array][compare]");
}

/**
 * garrow_uint64_array_compare:
 * @array: A #GArrowUInt64Array.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_uint64_array_compare(GArrowUInt64Array *array,
                            guint64 value,
                            GArrowCompareOptions *options,
                            GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[uint64-array][compare]");
}

/**
 * garrow_float_array_compare:
 * @array: A #GArrowFloatArray.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_float_array_compare(GArrowFloatArray *array,
                           gfloat value,
                           GArrowCompareOptions *options,
                           GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[float-array][compare]");
}

/**
 * garrow_double_array_compare:
 * @array: A #GArrowDoubleArray.
 * @value: The value to compare.
 * @options: A #GArrowCompareOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray as
 *   the result compared a numeric array with a scalar on success,
 *   %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowBooleanArray *
garrow_double_array_compare(GArrowDoubleArray *array,
                            gdouble value,
                            GArrowCompareOptions *options,
                            GError **error)
{
  return garrow_numeric_array_compare(array,
                                      value,
                                      options,
                                      error,
                                      "[double-array][compare]");
}

/**
 * garrow_array_filter:
 * @array: A #GArrowArray.
 * @filter: The values indicates which values should be filtered out.
 * @options: (nullable): A #GArrowFilterOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowArray filterd
 *   with a boolean selection filter. Nulls in the filter will
 *   result in nulls in the output.
 *
 * Since: 0.15.0
 */
GArrowArray *
garrow_array_filter(GArrowArray *array,
                    GArrowBooleanArray *filter,
                    GArrowFilterOptions *options,
                    GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_filter = garrow_array_get_raw(GARROW_ARRAY(filter));
  arrow::Result<arrow::Datum> arrow_filtered_datum;
  if (options) {
    auto arrow_options = garrow_filter_options_get_raw(options);
    arrow_filtered_datum = arrow::compute::Filter(arrow_array,
                                                  arrow_filter,
                                                  *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_array,
                                                  arrow_filter);
  }
  if (garrow::check(error, arrow_filtered_datum, "[array][filter]")) {
    auto arrow_filtered_array = (*arrow_filtered_datum).make_array();
    return garrow_array_new_raw(&arrow_filtered_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_array_is_in:
 * @left: A left hand side #GArrowArray.
 * @right: A right hand side #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray
 *   showing whether each element in the left array is contained
 *   in right array.
 *
 * Since: 0.15.0
 */
GArrowBooleanArray *
garrow_array_is_in(GArrowArray *left,
                   GArrowArray *right,
                   GError **error)
{
  auto arrow_left = garrow_array_get_raw(left);
  auto arrow_right = garrow_array_get_raw(right);
  auto arrow_is_in_datum = arrow::compute::IsIn(arrow_left, arrow_right);
  if (garrow::check(error, arrow_is_in_datum, "[array][is-in]")) {
    auto arrow_is_in_array = (*arrow_is_in_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_is_in_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_array_is_in_chunked_array:
 * @left: A left hand side #GArrowArray.
 * @right: A right hand side #GArrowChunkedArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowBooleanArray
 *   showing whether each element in the left array is contained
 *   in right chunked array.
 *
 * Since: 0.15.0
 */
GArrowBooleanArray *
garrow_array_is_in_chunked_array(GArrowArray *left,
                                 GArrowChunkedArray *right,
                                 GError **error)
{
  auto arrow_left = garrow_array_get_raw(left);
  auto arrow_right = garrow_chunked_array_get_raw(right);
  auto arrow_is_in_datum = arrow::compute::IsIn(arrow_left, arrow_right);
  if (garrow::check(error,
                    arrow_is_in_datum,
                    "[array][is-in][chunked-array]")) {
    auto arrow_is_in_array = (*arrow_is_in_datum).make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_is_in_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_array_sort_to_indices:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The indices that would sort
 *   an array on success, %NULL on error.
 *
 * Since: 0.15.0
 */
GArrowUInt64Array *
garrow_array_sort_to_indices(GArrowArray *array,
                             GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  auto arrow_indices_array = arrow::compute::SortToIndices(*arrow_array_raw);
  if (garrow::check(error, arrow_indices_array, "[array][sort-to-indices]")) {
    return GARROW_UINT64_ARRAY(garrow_array_new_raw(&(*arrow_indices_array)));
  } else {
    return NULL;
  }
}

/**
 * garrow_table_filter:
 * @table: A #GArrowTable.
 * @filter: The values indicates which values should be filtered out.
 * @options: (nullable): A #GArrowFilterOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowTable filterd
 *   with a boolean selection filter. Nulls in the filter will
 *   result in nulls in the output.
 *
 * Since: 0.15.0
 */
GArrowTable *
garrow_table_filter(GArrowTable *table,
                    GArrowBooleanArray *filter,
                    GArrowFilterOptions *options,
                    GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_filter = garrow_array_get_raw(GARROW_ARRAY(filter));
  arrow::Result<arrow::Datum> arrow_filtered_datum;
  if (options) {
    auto arrow_options = garrow_filter_options_get_raw(options);
    arrow_filtered_datum = arrow::compute::Filter(arrow_table,
                                                  arrow_filter,
                                                  *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_table,
                                                  arrow_filter);
  }
  if (garrow::check(error, arrow_filtered_datum, "[table][filter]")) {
    auto arrow_filtered_table = (*arrow_filtered_datum).table();
    return garrow_table_new_raw(&arrow_filtered_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_table_filter_chunked_array:
 * @table: A #GArrowTable.
 * @filter: The values indicates which values should be filtered out.
 * @options: (nullable): A #GArrowFilterOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowTable filterd
 *   with a chunked array filter. Nulls in the filter will
 *   result in nulls in the output.
 *
 * Since: 0.15.0
 */
GArrowTable *
garrow_table_filter_chunked_array(GArrowTable *table,
                                  GArrowChunkedArray *filter,
                                  GArrowFilterOptions *options,
                                  GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_filter = garrow_chunked_array_get_raw(filter);
  arrow::Result<arrow::Datum> arrow_filtered_datum;
  if (options) {
    auto arrow_options = garrow_filter_options_get_raw(options);
    arrow_filtered_datum = arrow::compute::Filter(arrow_table,
                                                  arrow_filter,
                                                  *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_table,
                                                  arrow_filter);
  }
  if (garrow::check(error,
                    arrow_filtered_datum,
                    "[table][filter][chunked-array]")) {
    auto arrow_filtered_table = (*arrow_filtered_datum).table();
    return garrow_table_new_raw(&arrow_filtered_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_chunked_array_filter:
 * @chunked_array: A #GArrowChunkedArray.
 * @filter: The values indicates which values should be filtered out.
 * @options: (nullable): A #GArrowFilterOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray filterd
 *   with a boolean selection filter. Nulls in the filter will
 *   result in nulls in the output.
 *
 * Since: 0.15.0
 */
GArrowChunkedArray *
garrow_chunked_array_filter(GArrowChunkedArray *chunked_array,
                            GArrowBooleanArray *filter,
                            GArrowFilterOptions *options,
                            GError **error)
{
  auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_filter = garrow_array_get_raw(GARROW_ARRAY(filter));
  arrow::Result<arrow::Datum> arrow_filtered_datum;
  if (options) {
    auto arrow_options = garrow_filter_options_get_raw(options);
    arrow_filtered_datum = arrow::compute::Filter(arrow_chunked_array,
                                                  arrow_filter,
                                                  *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_chunked_array,
                                                  arrow_filter);
  }
  if (garrow::check(error, arrow_filtered_datum, "[chunked-array][filter]")) {
    auto arrow_filtered_chunked_array = (*arrow_filtered_datum).chunked_array();
    return garrow_chunked_array_new_raw(&arrow_filtered_chunked_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_chunked_array_filter_chunked_array:
 * @chunked_array: A #GArrowChunkedArray.
 * @filter: The values indicates which values should be filtered out.
 * @options: (nullable): A #GArrowFilterOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray filterd
 *   with a chunked array filter. Nulls in the filter will
 *   result in nulls in the output.
 *
 * Since: 0.15.0
 */
GArrowChunkedArray *
garrow_chunked_array_filter_chunked_array(GArrowChunkedArray *chunked_array,
                                          GArrowChunkedArray *filter,
                                          GArrowFilterOptions *options,
                                          GError **error)
{
  auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_filter = garrow_chunked_array_get_raw(filter);
  arrow::Result<arrow::Datum> arrow_filtered_datum;
  if (options) {
    auto arrow_options = garrow_filter_options_get_raw(options);
    arrow_filtered_datum = arrow::compute::Filter(arrow_chunked_array,
                                                  arrow_filter,
                                                  *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_chunked_array,
                                                  arrow_filter);
  }
  if (garrow::check(error,
                    arrow_filtered_datum,
                    "[chunked-array][filter][chunked-array]")) {
    auto arrow_filtered_chunked_array = (*arrow_filtered_datum).chunked_array();
    return garrow_chunked_array_new_raw(&arrow_filtered_chunked_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_filter:
 * @record_batch: A #GArrowRecordBatch.
 * @filter: The values indicates which values should be filtered out.
 * @options: (nullable): A #GArrowFilterOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GArrowRecordBatch filterd
 *   with a boolean selection filter. Nulls in the filter will
 *   result in nulls in the output.
 *
 * Since: 0.15.0
 */
GArrowRecordBatch *
garrow_record_batch_filter(GArrowRecordBatch *record_batch,
                           GArrowBooleanArray *filter,
                           GArrowFilterOptions *options,
                           GError **error)
{
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_filter = garrow_array_get_raw(GARROW_ARRAY(filter));
  arrow::Result<arrow::Datum> arrow_filtered_datum;
  if (options) {
    auto arrow_options = garrow_filter_options_get_raw(options);
    arrow_filtered_datum = arrow::compute::Filter(arrow_record_batch,
                                                  arrow_filter,
                                                  *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_record_batch,
                                                  arrow_filter);
  }
  if (garrow::check(error, arrow_filtered_datum, "[record-batch][filter]")) {
    auto arrow_filtered_record_batch = (*arrow_filtered_datum).record_batch();
    return garrow_record_batch_new_raw(&arrow_filtered_record_batch);
  } else {
    return NULL;
  }
}

G_END_DECLS

arrow::compute::ExecContext *
garrow_execute_context_get_raw(GArrowExecuteContext *context)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(context);
  return &priv->context;
}

arrow::compute::FunctionOptions *
garrow_function_options_get_raw(GArrowFunctionOptions *options)
{
  auto iface = GARROW_FUNCTION_OPTIONS_GET_IFACE(options);
  return iface->get_raw(options);
}

GArrowFunction *
garrow_function_new_raw(std::shared_ptr<arrow::compute::Function> *arrow_function)
{
  return GARROW_FUNCTION(g_object_new(GARROW_TYPE_FUNCTION,
                                      "function", arrow_function,
                                      NULL));
}

std::shared_ptr<arrow::compute::Function>
garrow_function_get_raw(GArrowFunction *function)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(function);
  return priv->function;
}

GArrowCastOptions *
garrow_cast_options_new_raw(arrow::compute::CastOptions *arrow_cast_options)
{
  GArrowDataType *to_data_type = NULL;
  if (arrow_cast_options->to_type) {
    to_data_type = garrow_data_type_new_raw(&(arrow_cast_options->to_type));
  }
  auto cast_options =
    g_object_new(GARROW_TYPE_CAST_OPTIONS,
                 "to-data-type", to_data_type,
                 "allow-int-overflow", arrow_cast_options->allow_int_overflow,
                 "allow-time-truncate", arrow_cast_options->allow_time_truncate,
                 "allow-time-overflow", arrow_cast_options->allow_time_overflow,
                 "allow-decimal-truncate", arrow_cast_options->allow_decimal_truncate,
                 "allow-float-truncate", arrow_cast_options->allow_float_truncate,
                 "allow-invalid-utf8", arrow_cast_options->allow_invalid_utf8,
                 NULL);
  return GARROW_CAST_OPTIONS(cast_options);
}

arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *cast_options)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(cast_options);
  return &(priv->options);
}

GArrowCountOptions *
garrow_count_options_new_raw(arrow::compute::CountOptions *arrow_count_options)
{
  auto count_options =
    g_object_new(GARROW_TYPE_COUNT_OPTIONS,
                 "mode", arrow_count_options->count_mode,
                 NULL);
  return GARROW_COUNT_OPTIONS(count_options);
}

arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *count_options)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(count_options);
  return &(priv->options);
}

arrow::compute::FilterOptions *
garrow_filter_options_get_raw(GArrowFilterOptions *filter_options)
{
  auto priv = GARROW_FILTER_OPTIONS_GET_PRIVATE(filter_options);
  return &(priv->options);
}

arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *take_options)
{
  auto priv = GARROW_TAKE_OPTIONS_GET_PRIVATE(take_options);
  return &(priv->options);
}

arrow::compute::CompareOptions *
garrow_compare_options_get_raw(GArrowCompareOptions *compare_options)
{
  auto priv = GARROW_COMPARE_OPTIONS_GET_PRIVATE(compare_options);
  return &(priv->options);
}
