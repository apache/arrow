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

namespace {
  bool
  garrow_sort_key_equal_raw(const arrow::compute::SortKey &sort_key,
                            const arrow::compute::SortKey &other_sort_key) {
    return
      (sort_key.name == other_sort_key.name) &&
      (sort_key.order == other_sort_key.order);

  }
}

G_BEGIN_DECLS

/**
 * SECTION: compute
 * @section_id: compute
 * @title: Computation on data
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
 * #GArrowScalarAggregateOptions is a class to customize the scalar
 * aggregate functions such as `count` function and convenient
 * functions of them such as garrow_array_count().
 *
 * #GArrowFilterOptions is a class to customize the `filter` function and
 * garrow_array_filter() family.
 *
 * #GArrowTakeOptions is a class to customize the `take` function and
 * garrow_array_take() family.
 *
 * #GArrowArraySortOptions is a class to customize the
 * `array_sort_indices` function.
 *
 * #GArrowSortOptions is a class to customize the `sort_indices`
 * function.
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
 *   A return value of the execution as #GArrowDatum on success, %NULL on error.
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
   * The #GArrowDataType being casted to.
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


typedef struct GArrowScalarAggregateOptionsPrivate_ {
  arrow::compute::ScalarAggregateOptions options;
} GArrowScalarAggregateOptionsPrivate;

enum {
  PROP_SKIP_NULLS = 1,
  PROP_MIN_COUNT,
};

static arrow::compute::FunctionOptions *
garrow_scalar_aggregate_options_get_raw_function_options(
  GArrowFunctionOptions *options)
{
  return garrow_scalar_aggregate_options_get_raw(
    GARROW_SCALAR_AGGREGATE_OPTIONS(options));
}

static void
garrow_scalar_aggregate_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_scalar_aggregate_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowScalarAggregateOptions,
                        garrow_scalar_aggregate_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowScalarAggregateOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_scalar_aggregate_options_function_options_interface_init))

#define GARROW_SCALAR_AGGREGATE_OPTIONS_GET_PRIVATE(object)        \
  static_cast<GArrowScalarAggregateOptionsPrivate *>(              \
    garrow_scalar_aggregate_options_get_instance_private(          \
      GARROW_SCALAR_AGGREGATE_OPTIONS(object)))

static void
garrow_scalar_aggregate_options_finalize(GObject *object)
{
  auto priv = GARROW_SCALAR_AGGREGATE_OPTIONS_GET_PRIVATE(object);
  priv->options.~ScalarAggregateOptions();
  G_OBJECT_CLASS(garrow_scalar_aggregate_options_parent_class)->finalize(object);
}

static void
garrow_scalar_aggregate_options_set_property(GObject *object,
                                             guint prop_id,
                                             const GValue *value,
                                             GParamSpec *pspec)
{
  auto priv = GARROW_SCALAR_AGGREGATE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SKIP_NULLS:
    priv->options.skip_nulls = g_value_get_boolean(value);
    break;
  case PROP_MIN_COUNT:
    priv->options.min_count = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_scalar_aggregate_options_get_property(GObject *object,
                                             guint prop_id,
                                             GValue *value,
                                             GParamSpec *pspec)
{
  auto priv = GARROW_SCALAR_AGGREGATE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SKIP_NULLS:
    g_value_set_boolean(value, priv->options.skip_nulls);
    break;
  case PROP_MIN_COUNT:
    g_value_set_uint(value, priv->options.min_count);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_scalar_aggregate_options_init(GArrowScalarAggregateOptions *object)
{
  auto priv = GARROW_SCALAR_AGGREGATE_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::ScalarAggregateOptions();
}

static void
garrow_scalar_aggregate_options_class_init(
  GArrowScalarAggregateOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_scalar_aggregate_options_finalize;
  gobject_class->set_property = garrow_scalar_aggregate_options_set_property;
  gobject_class->get_property = garrow_scalar_aggregate_options_get_property;

  auto options = arrow::compute::ScalarAggregateOptions::Defaults();

  GParamSpec *spec;
  /**
   * GArrowScalarAggregateOptions:skip-nulls:
   *
   * Whether NULLs are skipped or not.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_boolean("skip-nulls",
                              "Skip NULLs",
                              "Whether NULLs are skipped or not",
                              options.skip_nulls,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_SKIP_NULLS, spec);

  /**
   * GArrowScalarAggregateOptions:min-count:
   *
   * The minimum required number of values.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_uint("min-count",
                           "Min count",
                           "The minimum required number of values",
                           0,
                           G_MAXUINT,
                           options.min_count,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MIN_COUNT, spec);
}

/**
 * garrow_scalar_aggregate_options_new:
 *
 * Returns: A newly created #GArrowScalarAggregateOptions.
 *
 * Since: 5.0.0
 */
GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new(void)
{
  auto scalar_aggregate_options =
    g_object_new(GARROW_TYPE_SCALAR_AGGREGATE_OPTIONS, NULL);
  return GARROW_SCALAR_AGGREGATE_OPTIONS(scalar_aggregate_options);
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
   * GArrowFilterOptions:null-selection-behavior:
   *
   * How to handle filtered values.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_enum("null-selection-behavior",
                           "NULL selection behavior",
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


typedef struct GArrowArraySortOptionsPrivate_ {
  arrow::compute::ArraySortOptions options;
} GArrowArraySortOptionsPrivate;

enum {
  PROP_ARRAY_SORT_OPTIONS_ORDER = 1,
};

static arrow::compute::FunctionOptions *
garrow_array_sort_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_array_sort_options_get_raw(GARROW_ARRAY_SORT_OPTIONS(options));
}

static void
garrow_array_sort_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_array_sort_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowArraySortOptions,
                        garrow_array_sort_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowArraySortOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_array_sort_options_function_options_interface_init))

#define GARROW_ARRAY_SORT_OPTIONS_GET_PRIVATE(object)   \
  static_cast<GArrowArraySortOptionsPrivate *>(         \
    garrow_array_sort_options_get_instance_private(     \
      GARROW_ARRAY_SORT_OPTIONS(object)))

static void
garrow_array_sort_options_finalize(GObject *object)
{
  auto priv = GARROW_ARRAY_SORT_OPTIONS_GET_PRIVATE(object);
  priv->options.~ArraySortOptions();
  G_OBJECT_CLASS(garrow_array_sort_options_parent_class)->finalize(object);
}

static void
garrow_array_sort_options_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_SORT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARRAY_SORT_OPTIONS_ORDER:
    priv->options.order =
      static_cast<arrow::compute::SortOrder>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_sort_options_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_SORT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARRAY_SORT_OPTIONS_ORDER:
    g_value_set_enum(value, static_cast<GArrowSortOrder>(priv->options.order));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_sort_options_init(GArrowArraySortOptions *object)
{
  auto priv = GARROW_ARRAY_SORT_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::ArraySortOptions();
}

static void
garrow_array_sort_options_class_init(GArrowArraySortOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_array_sort_options_finalize;
  gobject_class->set_property = garrow_array_sort_options_set_property;
  gobject_class->get_property = garrow_array_sort_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowArraySortOptions:order:
   *
   * How to order values.
   *
   * Since: 3.0.0
   */
  spec = g_param_spec_enum("order",
                           "Order",
                           "How to order values",
                           GARROW_TYPE_SORT_ORDER,
                           0,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ARRAY_SORT_OPTIONS_ORDER,
                                  spec);
}

/**
 * garrow_array_sort_options_new:
 * @order: How to order by values.
 *
 * Returns: A newly created #GArrowArraySortOptions.
 *
 * Since: 3.0.0
 */
GArrowArraySortOptions *
garrow_array_sort_options_new(GArrowSortOrder order)
{
  auto array_sort_options =
    g_object_new(GARROW_TYPE_ARRAY_SORT_OPTIONS,
                 "order", order,
                 NULL);
  return GARROW_ARRAY_SORT_OPTIONS(array_sort_options);
}

/**
 * garrow_array_sort_options_equal:
 * @options: A #GArrowArraySortOptions.
 * @other_options: A #GArrowArraySortOptions to be compared.
 *
 * Returns: %TRUE if both of them have the same order, %FALSE
 *   otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_array_sort_options_equal(GArrowArraySortOptions *options,
                                GArrowArraySortOptions *other_options)
{
  auto arrow_options = garrow_array_sort_options_get_raw(options);
  auto arrow_other_options = garrow_array_sort_options_get_raw(other_options);
  return arrow_options->order == arrow_other_options->order;
}


typedef struct GArrowSortKeyPrivate_ {
  arrow::compute::SortKey sort_key;
} GArrowSortKeyPrivate;

enum {
  PROP_SORT_KEY_NAME = 1,
  PROP_SORT_KEY_ORDER,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSortKey,
                           garrow_sort_key,
                           G_TYPE_OBJECT)

#define GARROW_SORT_KEY_GET_PRIVATE(object)     \
  static_cast<GArrowSortKeyPrivate *>(          \
    garrow_sort_key_get_instance_private(       \
      GARROW_SORT_KEY(object)))

static void
garrow_sort_key_finalize(GObject *object)
{
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(object);
  priv->sort_key.~SortKey();
  G_OBJECT_CLASS(garrow_sort_key_parent_class)->finalize(object);
}

static void
garrow_sort_key_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SORT_KEY_NAME:
    priv->sort_key.name = g_value_get_string(value);
    break;
  case PROP_SORT_KEY_ORDER:
    priv->sort_key.order =
      static_cast<arrow::compute::SortOrder>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_sort_key_get_property(GObject *object,
                             guint prop_id,
                             GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SORT_KEY_NAME:
    g_value_set_string(value, priv->sort_key.name.c_str());
    break;
  case PROP_SORT_KEY_ORDER:
    g_value_set_enum(value, static_cast<GArrowSortOrder>(priv->sort_key.order));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_sort_key_init(GArrowSortKey *object)
{
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(object);
  new(&priv->sort_key) arrow::compute::SortKey("");
}

static void
garrow_sort_key_class_init(GArrowSortKeyClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_sort_key_finalize;
  gobject_class->set_property = garrow_sort_key_set_property;
  gobject_class->get_property = garrow_sort_key_get_property;

  GParamSpec *spec;
  /**
   * GArrowSortKey:name:
   *
   * The column name to be used.
   *
   * Since: 3.0.0
   */
  spec = g_param_spec_string("name",
                             "Name",
                             "The column name to be used",
                             NULL,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_SORT_KEY_NAME, spec);

  /**
   * GArrowSortKey:order:
   *
   * How to order values.
   *
   * Since: 3.0.0
   */
  spec = g_param_spec_enum("order",
                           "Order",
                           "How to order values",
                           GARROW_TYPE_SORT_ORDER,
                           0,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_SORT_KEY_ORDER, spec);
}

/**
 * garrow_sort_key_new:
 * @name: A column name to be used.
 * @order: How to order by this sort key.
 *
 * Returns: A newly created #GArrowSortKey.
 *
 * Since: 3.0.0
 */
GArrowSortKey *
garrow_sort_key_new(const gchar *name, GArrowSortOrder order)
{
  auto sort_key = g_object_new(GARROW_TYPE_SORT_KEY,
                               "name", name,
                               "order", order,
                               NULL);
  return GARROW_SORT_KEY(sort_key);
}

/**
 * garrow_sort_key_equal:
 * @sort_key: A #GArrowSortKey.
 * @other_sort_key: A #GArrowSortKey to be compared.
 *
 * Returns: %TRUE if both of them have the same name and order, %FALSE
 *   otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_sort_key_equal(GArrowSortKey *sort_key,
                      GArrowSortKey *other_sort_key)
{
  auto arrow_sort_key = garrow_sort_key_get_raw(sort_key);
  auto arrow_other_sort_key = garrow_sort_key_get_raw(other_sort_key);
  return garrow_sort_key_equal_raw(*arrow_sort_key,
                                   *arrow_other_sort_key);
}


typedef struct GArrowSortOptionsPrivate_ {
  arrow::compute::SortOptions options;
} GArrowSortOptionsPrivate;

static arrow::compute::FunctionOptions *
garrow_sort_options_get_raw_function_options(GArrowFunctionOptions *options)
{
  return garrow_sort_options_get_raw(GARROW_SORT_OPTIONS(options));
}

static void
garrow_sort_options_function_options_interface_init(
  GArrowFunctionOptionsInterface *iface)
{
  iface->get_raw = garrow_sort_options_get_raw_function_options;
}

G_DEFINE_TYPE_WITH_CODE(GArrowSortOptions,
                        garrow_sort_options,
                        G_TYPE_OBJECT,
                        G_ADD_PRIVATE(GArrowSortOptions)
                        G_IMPLEMENT_INTERFACE(
                          GARROW_TYPE_FUNCTION_OPTIONS,
                          garrow_sort_options_function_options_interface_init))

#define GARROW_SORT_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowSortOptionsPrivate *>(      \
    garrow_sort_options_get_instance_private(   \
      GARROW_SORT_OPTIONS(object)))

static void
garrow_sort_options_finalize(GObject *object)
{
  auto priv = GARROW_SORT_OPTIONS_GET_PRIVATE(object);
  priv->options.~SortOptions();
  G_OBJECT_CLASS(garrow_sort_options_parent_class)->finalize(object);
}

static void
garrow_sort_options_init(GArrowSortOptions *object)
{
  auto priv = GARROW_SORT_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::compute::SortOptions();
}

static void
garrow_sort_options_class_init(GArrowSortOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_sort_options_finalize;
}

/**
 * garrow_sort_options_new:
 * @sort_keys: (nullable) (element-type GArrowSortKey): The sort keys to be used.
 *
 * Returns: A newly created #GArrowSortOptions.
 *
 * Since: 3.0.0
 */
GArrowSortOptions *
garrow_sort_options_new(GList *sort_keys)
{
  auto sort_options =
    GARROW_SORT_OPTIONS(g_object_new(GARROW_TYPE_SORT_OPTIONS, NULL));
  if (sort_keys) {
    garrow_sort_options_set_sort_keys(sort_options, sort_keys);
  }
  return sort_options;
}

/**
 * garrow_sort_options_equal:
 * @options: A #GArrowSortOptions.
 * @other_options: A #GArrowSortOptions to be compared.
 *
 * Returns: %TRUE if both of them have the same sort keys, %FALSE
 *   otherwise.
 *
 * Since: 3.0.0
 */
gboolean
garrow_sort_options_equal(GArrowSortOptions *options,
                          GArrowSortOptions *other_options)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  auto arrow_other_options = garrow_sort_options_get_raw(other_options);
  if (arrow_options->sort_keys.size() !=
      arrow_other_options->sort_keys.size()) {
    return FALSE;
  }
  const auto n_sort_keys = arrow_options->sort_keys.size();
  for (size_t i = 0; i < n_sort_keys; ++i) {
    if (!garrow_sort_key_equal_raw(arrow_options->sort_keys[i],
                                   arrow_other_options->sort_keys[i])) {
      return FALSE;
    }
  }
  return TRUE;
}

/**
 * garrow_sort_options_get_sort_keys:
 * @options: A #GArrowSortOptions.
 *
 * Returns: (transfer full) (element-type GArrowSortKey):
 *   The sort keys to be used.
 *
 * Since: 3.0.0
 */
GList *
garrow_sort_options_get_sort_keys(GArrowSortOptions *options)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  GList *sort_keys = NULL;
  for (const auto &arrow_sort_key : arrow_options->sort_keys) {
    auto sort_key =
      garrow_sort_key_new(arrow_sort_key.name.c_str(),
                          static_cast<GArrowSortOrder>(arrow_sort_key.order));
    sort_keys = g_list_prepend(sort_keys, sort_key);
  }
  return g_list_reverse(sort_keys);
}

/**
 * garrow_sort_options_add_sort_key:
 * @options: A #GArrowSortOptions.
 * @sort_key: The sort key to be added.
 *
 * Add a sort key to be used.
 *
 * Since: 3.0.0
 */
void
garrow_sort_options_add_sort_key(GArrowSortOptions *options,
                                 GArrowSortKey *sort_key)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  auto arrow_sort_key = garrow_sort_key_get_raw(sort_key);
  arrow_options->sort_keys.push_back(*arrow_sort_key);
}

/**
 * garrow_sort_options_set_sort_keys:
 * @options: A #GArrowSortOptions.
 * @sort_keys: (element-type GArrowSortKey): The sort keys to be used.
 *
 * Set sort keys to be used.
 *
 * Since: 3.0.0
 */
void
garrow_sort_options_set_sort_keys(GArrowSortOptions *options,
                                  GList *sort_keys)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  arrow_options->sort_keys.clear();
  for (auto node = sort_keys; node; node = node->next) {
    auto sort_key = GARROW_SORT_KEY(node->data);
    auto arrow_sort_key = garrow_sort_key_get_raw(sort_key);
    arrow_options->sort_keys.push_back(*arrow_sort_key);
  }
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
 * @options: (nullable): A #GArrowScalarAggregateOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The number of target values on success. If an error is occurred,
 *   the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_array_count(GArrowArray *array,
                   GArrowScalarAggregateOptions *options,
                   GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  arrow::Result<arrow::Datum> arrow_counted_datum;
  if (options) {
    auto arrow_options = garrow_scalar_aggregate_options_get_raw(options);
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
    std::shared_ptr<arrow::Array> arrow_counted_values_array = *arrow_counted_values;
    return GARROW_STRUCT_ARRAY(garrow_array_new_raw(&arrow_counted_values_array));
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
 * garrow_array_sort_indices:
 * @array: A #GArrowArray.
 * @order: The order for sort.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The indices that would sort
 *   an array in the specified order on success, %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowUInt64Array *
garrow_array_sort_indices(GArrowArray *array,
                          GArrowSortOrder order,
                          GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  auto arrow_order = static_cast<arrow::compute::SortOrder>(order);
  auto arrow_indices_array =
    arrow::compute::SortIndices(*arrow_array_raw, arrow_order);
  if (garrow::check(error, arrow_indices_array, "[array][sort-indices]")) {
    return GARROW_UINT64_ARRAY(garrow_array_new_raw(&(*arrow_indices_array)));
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
 *   an array in ascending order on success, %NULL on error.
 *
 * Since: 0.15.0
 *
 * Deprecated: 3.0.0: Use garrow_array_sort_indices() instead.
 */
GArrowUInt64Array *
garrow_array_sort_to_indices(GArrowArray *array,
                             GError **error)
{
  return garrow_array_sort_indices(array, GARROW_SORT_ORDER_ASCENDING, error);
}

/**
 * garrow_chunked_array_sort_indices:
 * @chunked_array: A #GArrowChunkedArray.
 * @order: The order for sort.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The indices that would sort
 *   a chunked array in the specified order on success, %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowUInt64Array *
garrow_chunked_array_sort_indices(GArrowChunkedArray *chunked_array,
                                  GArrowSortOrder order,
                                  GError **error)
{
  auto arrow_chunked_array = garrow_chunked_array_get_raw(chunked_array);
  auto arrow_chunked_array_raw = arrow_chunked_array.get();
  auto arrow_order = static_cast<arrow::compute::SortOrder>(order);
  auto arrow_indices_array =
    arrow::compute::SortIndices(*arrow_chunked_array_raw, arrow_order);
  if (garrow::check(error,
                    arrow_indices_array,
                    "[chunked-array][sort-indices]")) {
    return GARROW_UINT64_ARRAY(garrow_array_new_raw(&(*arrow_indices_array)));
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_sort_indices:
 * @record_batch: A #GArrowRecordBatch.
 * @options: The options to be used.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The indices that would sort
 *   a record batch with the specified options on success, %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowUInt64Array *
garrow_record_batch_sort_indices(GArrowRecordBatch *record_batch,
                                 GArrowSortOptions *options,
                                 GError **error)
{
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_record_batch_raw = arrow_record_batch.get();
  auto arrow_options = garrow_sort_options_get_raw(options);
  auto arrow_indices_array =
    arrow::compute::SortIndices(::arrow::Datum(*arrow_record_batch_raw),
                                *arrow_options);
  if (garrow::check(error,
                    arrow_indices_array,
                    "[record-batch][sort-indices]")) {
    return GARROW_UINT64_ARRAY(garrow_array_new_raw(&(*arrow_indices_array)));
  } else {
    return NULL;
  }
}

/**
 * garrow_table_sort_indices:
 * @table: A #GArrowTable.
 * @options: The options to be used.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The indices that would sort
 *   a table with the specified options on success, %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowUInt64Array *
garrow_table_sort_indices(GArrowTable *table,
                          GArrowSortOptions *options,
                          GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_table_raw = arrow_table.get();
  auto arrow_options = garrow_sort_options_get_raw(options);
  auto arrow_indices_array =
    arrow::compute::SortIndices(::arrow::Datum(*arrow_table_raw),
                                *arrow_options);
  if (garrow::check(error,
                    arrow_indices_array,
                    "[table][sort-indices]")) {
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

GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new_raw(
  arrow::compute::ScalarAggregateOptions *arrow_scalar_aggregate_options)
{
  auto scalar_aggregate_options =
    g_object_new(GARROW_TYPE_SCALAR_AGGREGATE_OPTIONS,
                 "skip-nulls", arrow_scalar_aggregate_options->skip_nulls,
                 "min-count", arrow_scalar_aggregate_options->min_count,
                 NULL);
  return GARROW_SCALAR_AGGREGATE_OPTIONS(scalar_aggregate_options);
}

arrow::compute::ScalarAggregateOptions *
garrow_scalar_aggregate_options_get_raw(
  GArrowScalarAggregateOptions *scalar_aggregate_options)
{
  auto priv = GARROW_SCALAR_AGGREGATE_OPTIONS_GET_PRIVATE(scalar_aggregate_options);
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

arrow::compute::ArraySortOptions *
garrow_array_sort_options_get_raw(GArrowArraySortOptions *array_sort_options)
{
  auto priv = GARROW_ARRAY_SORT_OPTIONS_GET_PRIVATE(array_sort_options);
  return &(priv->options);
}

arrow::compute::SortKey *
garrow_sort_key_get_raw(GArrowSortKey *sort_key)
{
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(sort_key);
  return &(priv->sort_key);
}

arrow::compute::SortOptions *
garrow_sort_options_get_raw(GArrowSortOptions *sort_options)
{
  auto priv = GARROW_SORT_OPTIONS_GET_PRIVATE(sort_options);
  return &(priv->options);
}
