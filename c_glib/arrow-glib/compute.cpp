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
#include <arrow-glib/executor.hpp>
#include <arrow-glib/expression.hpp>
#include <arrow-glib/reader.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/scalar.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table.hpp>
#include <arrow-glib/type.hpp>

#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/api.h>

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
    arrow_taken_datum = arrow::compute::Take(arrow_values, arrow_indices, *arrow_options);
  } else {
    arrow_taken_datum = arrow::compute::Take(arrow_values, arrow_indices);
  }
  if (garrow::check(error, arrow_taken_datum, tag)) {
    return garrow_type_new_raw(*arrow_taken_datum);
  } else {
    return NULL;
  }
}

namespace {
  gboolean
  garrow_field_refs_add(std::vector<arrow::FieldRef> &arrow_field_refs,
                        const gchar *string,
                        GError **error,
                        const gchar *tag)
  {
    if (string[0] == '.' || string[0] == '[') {
      auto arrow_field_ref_result = arrow::FieldRef::FromDotPath(string);
      if (!garrow::check(error, arrow_field_ref_result, tag)) {
        return false;
      }
      arrow_field_refs.push_back(std::move(*arrow_field_ref_result));
    } else {
      arrow_field_refs.emplace_back(string);
    }
    return true;
  }

  GList *
  garrow_sort_keys_new_raw(std::vector<arrow::compute::SortKey> &arrow_sort_keys)
  {
    GList *sort_keys = NULL;
    for (const auto &arrow_sort_key : arrow_sort_keys) {
      auto sort_key = garrow_sort_key_new_raw(arrow_sort_key);
      sort_keys = g_list_prepend(sort_keys, sort_key);
    }
    return g_list_reverse(sort_keys);
  }

  gboolean
  garrow_raw_sort_keys_equal(std::vector<arrow::compute::SortKey> &arrow_sort_keys,
                             std::vector<arrow::compute::SortKey> &arrow_other_sort_keys)
  {
    if (arrow_sort_keys.size() != arrow_other_sort_keys.size()) {
      return FALSE;
    }
    const auto n_sort_keys = arrow_sort_keys.size();
    for (size_t i = 0; i < n_sort_keys; ++i) {
      auto arrow_sort_key = &(arrow_sort_keys[i]);
      auto arrow_other_sort_key = &(arrow_other_sort_keys[i]);
      if (!arrow_sort_key->Equals(*arrow_other_sort_key)) {
        return FALSE;
      }
    }
    return TRUE;
  }

  void
  garrow_raw_sort_keys_set(std::vector<arrow::compute::SortKey> &arrow_sort_keys,
                           GList *sort_keys)
  {
    arrow_sort_keys.clear();
    for (auto node = sort_keys; node; node = node->next) {
      auto sort_key = GARROW_SORT_KEY(node->data);
      auto arrow_sort_key = garrow_sort_key_get_raw(sort_key);
      arrow_sort_keys.push_back(*arrow_sort_key);
    }
  }

  void
  garrow_raw_sort_keys_add(std::vector<arrow::compute::SortKey> &arrow_sort_keys,
                           GArrowSortKey *sort_key)
  {
    auto arrow_sort_key = garrow_sort_key_get_raw(sort_key);
    arrow_sort_keys.push_back(*arrow_sort_key);
  }
} // namespace

G_BEGIN_DECLS

/**
 * SECTION: compute
 * @section_id: compute
 * @title: Computation on data
 * @include: arrow-glib/arrow-glib.h
 *
 * You must call garrow_compute_initialize() explicitly before you use
 * computation related features.
 *
 * #GArrowExecuteContext is a class to customize how to execute a
 * function.
 *
 * #GArrowFunctionOptions is a base class for all function options
 * classes such as #GArrowCastOptions.
 *
 * #GArrowFunctionDoc is a class for function document.
 *
 * #GArrowFunction is a class to process data.
 *
 * #GArrowExecuteNodeOptions is a base class for all execute node
 * options classes such as #GArrowSourceNodeOptions.
 *
 * #GArrowSourceNodeOptions is a class to customize a source node.
 *
 * #GArrowFilterNodeOptions is a class to customize a filter node.
 *
 * #GArrowProjectNodeOptions is a class to customize a project node.
 *
 * #GArrowAggregation is a class to specify how to aggregate.
 *
 * #GArrowAggregateNodeOptions is a class to customize an aggregate node.
 *
 * #GArrowSinkNodeOptions is a class to customize a sink node.
 *
 * #GArrowHashJoinNodeOptions is a class to customize a hash join node.
 *
 * #GArrowExecuteNode is a class to execute an operation.
 *
 * #GArrowExecutePlan is a class to execute operations.
 *
 * #GArrowCastOptions is a class to customize the `cast` function and
 * garrow_array_cast().
 *
 * #GArrowScalarAggregateOptions is a class to customize the scalar
 * aggregate functions such as `count` function and convenient
 * functions of them such as garrow_array_count().
 *
 * #GArrowCountOptions is a class to customize the `count` function and
 * garrow_array_count() family.
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
 * #GArrowSetLookupOptions is a class to customize the `is_in` function
 * and `index_in` function.
 *
 * #GArrowVarianceOptions is a class to customize the `stddev` function
 * and `variance` function.
 *
 * #GArrowRoundOptions is a class to customize the `round` function.
 *
 * #GArrowRoundToMultipleOptions is a class to customize the
 * `round_to_multiple` function.
 *
 * #GArrowUTF8NormalizeOptions is a class to customize the
 * `utf8_normalize` function.
 *
 * #GArrowQuantileOptions is a class to customize the `quantile`
 * function.
 *
 * #GArrowIndexOptions is a class to customize the `index` function.
 *
 * #GArrowRankOptions is a class to customize the `rank` function.
 *
 * #GArrowRunEndEncodeOptions is a class to customize the
 * `run_end_encode` function.
 *
 * #GArrowStrptimeOptions is a class to customize the `strptime` function.
 *
 * #GArrowStrftimeOptions is a class to customize the `strftime` function.
 *
 * #GArrowSplitPatternOptions is a class to customize the `split_pattern` and
 * `split_pattern_regex` functions.
 *
 * #GArrowStructFieldOptions is a class to customize the `struct_field`
 * function.
 *
 * #GArrowAssumeTimezoneOptions is a class to customize the `assume_timezone`
 * function.
 *
 * #GArrowCumulativeOptions is a class to customize the cumulative functions
 * such as `cumulative_sum`, `cumulative_prod`, `cumulative_max`, and
 * `cumulative_min`.
 *
 * #GArrowDictionaryEncodeOptions is a class to customize the `dictionary_encode`
 * function.
 *
 * #GArrowElementWiseAggregateOptions is a class to customize element-wise
 * aggregate functions such as `min_element_wise` and `max_element_wise`.
 *
 * #GArrowDayOfWeekOptions is a class to customize the `day_of_week` function.
 *
 * #GArrowExtractRegexOptions is a class to customize the `extract_regex`
 * function.
 *
 * #GArrowExtractRegexSpanOptions is a class to customize the `extract_regex_span`
 * function.
 *
 * #GArrowJoinOptions is a class to customize the `binary_join_element_wise`
 * function.
 *
 * #GArrowListFlattenOptions is a class to customize the `list_flatten`
 * function.
 *
 * #GArrowMapLookupOptions is a class to customize the `map_lookup`
 * function.
 *
 * #GArrowListSliceOptions is a class to customize the `list_slice`
 * function.
 *
 * #GArrowModeOptions is a class to customize the `mode` function.
 *
 * #GArrowNullOptions is a class to customize the `is_null` function.
 *
 * #GArrowPadOptions is a class to customize the padding functions such as
 * `utf8_lpad`, `utf8_rpad`, `utf8_center`, `ascii_lpad`, `ascii_rpad`, and
 * `ascii_center`.
 *
 * #GArrowPairwiseOptions is a class to customize the pairwise
 * functions such as `pairwise_diff` and `pairwise_diff_checked`.
 *
 * #GArrowReplaceSliceOptions is a class to customize the
 * `utf8_replace_slice` and `binary_replace_slice` functions.
 *
 *
 * #GArrowPartitionNthOptions is a class to customize the
 * `partition_nth_indices` function.
 *
 * #GArrowPivotWiderOptions is a class to customize the `pivot_wider` and
 * `hash_pivot_wider` functions.
 *
 * #GArrowRankQuantileOptions is a class to customize the `rank_quantile` and
 * `rank_normal` functions.
 *
 * There are many functions to compute data on an array.
 */

/**
 * garrow_compute_initialize:
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * You must call this explicitly before you use computation related
 * features.
 *
 * Returns: %TRUE if initializing the compute module completed successfully,
 *   %FALSE otherwise.
 *
 * Since: 21.0.0
 */
gboolean
garrow_compute_initialize(GError **error)
{
  auto status = arrow::compute::Initialize();
  return garrow::check(error, status, "[compute][initialize]");
}

struct GArrowExecuteContextPrivate
{
  std::shared_ptr<arrow::compute::ExecContext> context;
  GArrowExecutor *executor;
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExecuteContext, garrow_execute_context, G_TYPE_OBJECT)

#define GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object)                                       \
  static_cast<GArrowExecuteContextPrivate *>(                                            \
    garrow_execute_context_get_instance_private(GARROW_EXECUTE_CONTEXT(object)))

enum {
  PROP_EXECUTOR = 1,
};

static void
garrow_execute_context_dispose(GObject *object)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);

  if (priv->executor) {
    g_object_unref(priv->executor);
    priv->executor = nullptr;
  }

  G_OBJECT_CLASS(garrow_execute_context_parent_class)->dispose(object);
}

static void
garrow_execute_context_finalize(GObject *object)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);
  priv->context.~shared_ptr();
  G_OBJECT_CLASS(garrow_execute_context_parent_class)->finalize(object);
}

static void
garrow_execute_context_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_EXECUTOR:
    {
      priv->executor = GARROW_EXECUTOR(g_value_dup_object(value));
      auto arrow_executor = garrow_executor_get_raw(priv->executor);
      priv->context =
        std::make_shared<arrow::compute::ExecContext>(arrow::default_memory_pool(),
                                                      arrow_executor.get());
      break;
    }
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_execute_context_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_EXECUTOR:
    g_value_set_object(value, priv->executor);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_execute_context_init(GArrowExecuteContext *object)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(object);
  new (&priv->context) std::shared_ptr<arrow::compute::ExecContext>;
}

static void
garrow_execute_context_class_init(GArrowExecuteContextClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_execute_context_dispose;
  gobject_class->finalize = garrow_execute_context_finalize;
  gobject_class->set_property = garrow_execute_context_set_property;
  gobject_class->get_property = garrow_execute_context_get_property;

  GParamSpec *spec;
  /**
   * GArrowExecuteContext:executor:
   *
   * The executor for execution.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_object(
    "executor",
    "Executor",
    "The executor for execution",
    GARROW_TYPE_EXECUTOR,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXECUTOR, spec);
}

/**
 * garrow_execute_context_new:
 * @executor: (nullable): A #GArrowExecutor or %NULL.
 *
 * Returns: A newly created #GArrowExecuteContext.
 *
 * Since: 1.0.0
 */
GArrowExecuteContext *
garrow_execute_context_new(GArrowExecutor *executor)
{
  return GARROW_EXECUTE_CONTEXT(
    g_object_new(GARROW_TYPE_EXECUTE_CONTEXT, "executor", executor, nullptr));
}

typedef struct GArrowFunctionOptionsPrivate_
{
  arrow::compute::FunctionOptions *options;
} GArrowFunctionOptionsPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFunctionOptions, garrow_function_options, G_TYPE_OBJECT)

#define GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object)                                      \
  static_cast<GArrowFunctionOptionsPrivate *>(                                           \
    garrow_function_options_get_instance_private(GARROW_FUNCTION_OPTIONS(object)))

static void
garrow_function_options_finalize(GObject *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  delete priv->options;
  G_OBJECT_CLASS(garrow_function_options_parent_class)->finalize(object);
}

static void
garrow_function_options_init(GArrowFunctionOptions *object)
{
}

static void
garrow_function_options_class_init(GArrowFunctionOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = garrow_function_options_finalize;
}

/**
 * garrow_function_options_equal:
 * @options: A #GArrowFunctionOptions.
 * @other_options: (nullable): A #GArrowFunctionOptions to be compared.
 *
 * Returns: %TRUE if both of them have the same values, %FALSE
 *   otherwise.
 *
 * Since: 7.0.0
 */
gboolean
garrow_function_options_equal(GArrowFunctionOptions *options,
                              GArrowFunctionOptions *other_options)
{
  if (!other_options) {
    return FALSE;
  }
  auto arrow_options = garrow_function_options_get_raw(options);
  auto arrow_other_options = garrow_function_options_get_raw(other_options);
  return arrow_options->Equals(*arrow_other_options);
}

/**
 * garrow_function_options_to_string:
 * @options: A #GArrowFunctionOptions.
 *
 * Returns: The formatted options.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 7.0.0
 */
gchar *
garrow_function_options_to_string(GArrowFunctionOptions *options)
{
  auto arrow_options = garrow_function_options_get_raw(options);
  auto string = arrow_options->ToString();
  return g_strndup(string.data(), string.size());
}

typedef struct GArrowFunctionDocPrivate_
{
  arrow::compute::FunctionDoc *doc;
} GArrowFunctionDocPrivate;

enum {
  PROP_DOC = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFunctionDoc, garrow_function_doc, G_TYPE_OBJECT)

#define GARROW_FUNCTION_DOC_GET_PRIVATE(object)                                          \
  static_cast<GArrowFunctionDocPrivate *>(                                               \
    garrow_function_doc_get_instance_private(GARROW_FUNCTION_DOC(object)))

static void
garrow_function_doc_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_FUNCTION_DOC_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DOC:
    priv->doc = static_cast<arrow::compute::FunctionDoc *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_function_doc_init(GArrowFunctionDoc *object)
{
}

static void
garrow_function_doc_class_init(GArrowFunctionDocClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->set_property = garrow_function_doc_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "doc",
    "Doc",
    "The raw arrow::compute::FunctionDoc *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DOC, spec);
}

/**
 * garrow_function_doc_get_summary:
 * @doc: A #GArrowFunctionDoc.
 *
 * Returns: A one-line summary of the function, using a verb.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gchar *
garrow_function_doc_get_summary(GArrowFunctionDoc *doc)
{
  auto arrow_doc = garrow_function_doc_get_raw(doc);
  return g_strndup(arrow_doc->summary.data(), arrow_doc->summary.size());
}

/**
 * garrow_function_doc_get_description:
 * @doc: A #GArrowFunctionDoc.
 *
 * Returns: A detailed description of the function, meant to follow
 *   the summary.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gchar *
garrow_function_doc_get_description(GArrowFunctionDoc *doc)
{
  auto arrow_doc = garrow_function_doc_get_raw(doc);
  return g_strndup(arrow_doc->description.data(), arrow_doc->description.size());
}

/**
 * garrow_function_doc_get_arg_names:
 * @doc: A #GArrowFunctionDoc.
 *
 * Returns: (array zero-terminated=1) (element-type utf8) (transfer full):
 *   Symbolic names (identifiers) for the function arguments.
 *
 *   It's a %NULL-terminated string array. It must be freed with
 *   g_strfreev() when no longer needed.
 *
 * Since: 6.0.0
 */
gchar **
garrow_function_doc_get_arg_names(GArrowFunctionDoc *doc)
{
  auto arrow_doc = garrow_function_doc_get_raw(doc);
  const auto &arrow_arg_names = arrow_doc->arg_names;
  auto n = arrow_arg_names.size();
  auto arg_names = g_new(gchar *, n + 1);
  for (size_t i = 0; i < n; ++i) {
    arg_names[i] = g_strndup(arrow_arg_names[i].data(), arrow_arg_names[i].size());
  }
  arg_names[n] = NULL;
  return arg_names;
}

/**
 * garrow_function_doc_get_options_class_name:
 * @doc: A #GArrowFunctionDoc.
 *
 * Returns: Name of the options class, if any.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gchar *
garrow_function_doc_get_options_class_name(GArrowFunctionDoc *doc)
{
  auto arrow_doc = garrow_function_doc_get_raw(doc);
  return g_strndup(arrow_doc->options_class.data(), arrow_doc->options_class.size());
}

typedef struct GArrowFunctionPrivate_
{
  std::shared_ptr<arrow::compute::Function> function;
  gchar *name;
} GArrowFunctionPrivate;

enum {
  PROP_FUNCTION = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFunction, garrow_function, G_TYPE_OBJECT)

#define GARROW_FUNCTION_GET_PRIVATE(object)                                              \
  static_cast<GArrowFunctionPrivate *>(                                                  \
    garrow_function_get_instance_private(GARROW_FUNCTION(object)))

static void
garrow_function_finalize(GObject *object)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(object);
  priv->function.~shared_ptr();
  if (priv->name) {
    g_free(priv->name);
  }
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
    priv->function = *static_cast<std::shared_ptr<arrow::compute::Function> *>(
      g_value_get_pointer(value));
    {
      const auto &arrow_name = priv->function->name();
      priv->name = g_strndup(arrow_name.data(), arrow_name.length());
    }
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
  new (&priv->function) std::shared_ptr<arrow::compute::Function>;
}

static void
garrow_function_class_init(GArrowFunctionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_function_finalize;
  gobject_class->set_property = garrow_function_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "function",
    "Function",
    "The raw std::shared<arrow::compute::Function> *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
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
 * garrow_function_all:
 *
 * Returns: (element-type GArrowFunction) (transfer full):
 *   The all available functions.
 *
 *   It should be freed with g_list_free_full() and g_object_unref()
 *   when no longer needed.
 *
 * Since: 7.0.0
 */
GList *
garrow_function_all(void)
{
  auto arrow_function_registry = arrow::compute::GetFunctionRegistry();
  GList *functions = NULL;
  for (const auto &arrow_name : arrow_function_registry->GetFunctionNames()) {
    auto arrow_function_result = arrow_function_registry->GetFunction(arrow_name);
    if (!arrow_function_result.ok()) {
      continue;
    }
    auto arrow_function = *arrow_function_result;
    functions = g_list_prepend(functions, garrow_function_new_raw(&arrow_function));
  }
  return g_list_reverse(functions);
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
    arrow_result_result =
      arrow_function->Execute(arrow_args, arrow_options, arrow_context);
  } else {
    arrow::compute::ExecContext arrow_context;
    arrow_result_result =
      arrow_function->Execute(arrow_args, arrow_options, &arrow_context);
  }
  if (garrow::check(error, arrow_result_result, "[function][execute]")) {
    auto arrow_result = *arrow_result_result;
    return garrow_datum_new_raw(&arrow_result);
  } else {
    return NULL;
  }
}

/**
 * garrow_function_get_name:
 * @function: A #GArrowFunction.
 *
 * Returns: The function name.
 *
 * Since: 7.0.0
 */
const gchar *
garrow_function_get_name(GArrowFunction *function)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(function);
  return priv->name;
}

/**
 * garrow_function_get_doc:
 * @function: A #GArrowFunction.
 *
 * Returns: (transfer full): The function documentation.
 *
 * Since: 6.0.0
 */
GArrowFunctionDoc *
garrow_function_get_doc(GArrowFunction *function)
{
  auto arrow_function = garrow_function_get_raw(function);
  const auto &arrow_doc = arrow_function->doc();
  return garrow_function_doc_new_raw(&arrow_doc);
}

/**
 * garrow_function_get_default_options:
 * @function: A #GArrowFunction.
 *
 * Returns: (transfer full) (nullable): The default options of this
 *   function if exists, %NULL otherwise.
 *
 * Since: 7.0.0
 */
GArrowFunctionOptions *
garrow_function_get_default_options(GArrowFunction *function)
{
  auto arrow_function = garrow_function_get_raw(function);
  auto arrow_function_options = arrow_function->default_options();
  if (!arrow_function_options) {
    return NULL;
  }
  return garrow_function_options_new_raw(arrow_function_options);
}

/**
 * garrow_function_get_options_type:
 * @function: A #GArrowFunction.
 *
 * Returns: %G_TYPE_NONE if this function doesn't have options, the
 *   #GType of options of this function if it exists and Apache Arrow
 *   GLib bindings of it also exist, %G_TYPE_INVALID if options of this
 *   function exists but Apache Arrow GLib bindings of it don't exist.
 *
 * Since: 7.0.0
 */
GType
garrow_function_get_options_type(GArrowFunction *function)
{
  auto arrow_function = garrow_function_get_raw(function);
  const auto &arrow_doc = arrow_function->doc();
  if (arrow_doc.options_class.empty()) {
    return G_TYPE_NONE;
  }
  auto options_type_name = std::string("GArrow") + arrow_doc.options_class;
  return g_type_from_name(options_type_name.c_str());
}

/**
 * garrow_function_equal:
 * @function: A #GArrowFunction.
 * @other_function: A #GArrowFunction to be compared.
 *
 * Returns: %TRUE if both of them have the same name, %FALSE
 *   otherwise.
 *
 * Since: 7.0.0
 */
gboolean
garrow_function_equal(GArrowFunction *function, GArrowFunction *other_function)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(function);
  auto other_priv = GARROW_FUNCTION_GET_PRIVATE(other_function);
  return g_strcmp0(priv->name, other_priv->name) == 0;
}

/**
 * garrow_function_to_string:
 * @function: A #GArrowFunction.
 *
 * Returns: The formatted function.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 7.0.0
 */
gchar *
garrow_function_to_string(GArrowFunction *function)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(function);
  auto arrow_function = garrow_function_get_raw(function);
  const auto &arrow_doc = arrow_function->doc();
  const auto arrow_default_options = arrow_function->default_options();
  auto string = g_string_new(NULL);
  g_string_append_printf(string, "%s(", priv->name);
  int i = 0;
  for (const auto &arrow_arg_name : arrow_doc.arg_names) {
    if (i > 0) {
      g_string_append(string, ", ");
    }
    g_string_append(string, arrow_arg_name.c_str());
    ++i;
  }
  if (arrow_default_options) {
    if (i > 0) {
      g_string_append(string, ", ");
    }
    const auto options_string = arrow_default_options->ToString();
    g_string_append(string, options_string.c_str());
  }
  g_string_append_printf(string, "): %s", arrow_doc.summary.c_str());
  return g_string_free(string, FALSE);
}

typedef struct GArrowExecuteNodeOptionsPrivate_
{
  arrow::acero::ExecNodeOptions *options;
} GArrowExecuteNodeOptionsPrivate;

enum {
  PROP_EXECUTE_NODE_OPTIONS = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExecuteNodeOptions,
                           garrow_execute_node_options,
                           G_TYPE_OBJECT)

#define GARROW_EXECUTE_NODE_OPTIONS_GET_PRIVATE(object)                                  \
  static_cast<GArrowExecuteNodeOptionsPrivate *>(                                        \
    garrow_execute_node_options_get_instance_private(                                    \
      GARROW_EXECUTE_NODE_OPTIONS(object)))

static void
garrow_execute_node_options_finalize(GObject *object)
{
  auto priv = GARROW_EXECUTE_NODE_OPTIONS_GET_PRIVATE(object);
  delete priv->options;
  G_OBJECT_CLASS(garrow_execute_node_options_parent_class)->finalize(object);
}

static void
garrow_execute_node_options_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTE_NODE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FUNCTION:
    priv->options =
      static_cast<arrow::acero::ExecNodeOptions *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_execute_node_options_init(GArrowExecuteNodeOptions *object)
{
  auto priv = GARROW_EXECUTE_NODE_OPTIONS_GET_PRIVATE(object);
  priv->options = nullptr;
}

static void
garrow_execute_node_options_class_init(GArrowExecuteNodeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = garrow_execute_node_options_finalize;
  gobject_class->set_property = garrow_execute_node_options_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "options",
    "Options",
    "The raw arrow::acero::ExecNodeOptions *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXECUTE_NODE_OPTIONS, spec);
}

typedef struct GArrowSourceNodeOptionsPrivate_
{
  GArrowRecordBatchReader *reader;
  GArrowRecordBatch *record_batch;
} GArrowSourceNodeOptionsPrivate;

enum {
  PROP_READER = 1,
  PROP_RECORD_BATCH,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSourceNodeOptions,
                           garrow_source_node_options,
                           GARROW_TYPE_EXECUTE_NODE_OPTIONS)

#define GARROW_SOURCE_NODE_OPTIONS_GET_PRIVATE(object)                                   \
  static_cast<GArrowSourceNodeOptionsPrivate *>(                                         \
    garrow_source_node_options_get_instance_private(GARROW_SOURCE_NODE_OPTIONS(object)))

static void
garrow_source_node_options_dispose(GObject *object)
{
  auto priv = GARROW_SOURCE_NODE_OPTIONS_GET_PRIVATE(object);

  if (priv->reader) {
    g_object_unref(priv->reader);
    priv->reader = nullptr;
  }

  if (priv->record_batch) {
    g_object_unref(priv->record_batch);
    priv->record_batch = nullptr;
  }

  G_OBJECT_CLASS(garrow_source_node_options_parent_class)->dispose(object);
}

static void
garrow_source_node_options_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_SOURCE_NODE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READER:
    priv->reader = GARROW_RECORD_BATCH_READER(g_value_dup_object(value));
    break;
  case PROP_RECORD_BATCH:
    priv->record_batch = GARROW_RECORD_BATCH(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_source_node_options_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_SOURCE_NODE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READER:
    g_value_set_object(value, priv->reader);
    break;
  case PROP_RECORD_BATCH:
    g_value_set_object(value, priv->record_batch);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_source_node_options_init(GArrowSourceNodeOptions *object)
{
}

static void
garrow_source_node_options_class_init(GArrowSourceNodeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_source_node_options_dispose;
  gobject_class->set_property = garrow_source_node_options_set_property;
  gobject_class->get_property = garrow_source_node_options_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object(
    "reader",
    "Reader",
    "The GArrowRecordBatchReader that produces "
    "record batches",
    GARROW_TYPE_RECORD_BATCH_READER,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_READER, spec);

  spec = g_param_spec_object(
    "record-batch",
    "Record batch",
    "The GArrowRecordBatch to be produced",
    GARROW_TYPE_RECORD_BATCH,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RECORD_BATCH, spec);
}

/**
 * garrow_source_node_options_new_record_batch_reader:
 * @reader: A #GArrowRecordBatchReader.
 *
 * Returns: A newly created #GArrowSourceNodeOptions.
 *
 * Since: 6.0.0
 */
GArrowSourceNodeOptions *
garrow_source_node_options_new_record_batch_reader(GArrowRecordBatchReader *reader)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  auto arrow_options =
    new arrow::acero::SourceNodeOptions(arrow_reader->schema(), [arrow_reader]() {
      using ExecBatch = arrow::compute::ExecBatch;
      using ExecBatchOptional = std::optional<ExecBatch>;
      auto arrow_record_batch_result = arrow_reader->Next();
      if (!arrow_record_batch_result.ok()) {
        return arrow::AsyncGeneratorEnd<ExecBatchOptional>();
      }
      auto arrow_record_batch = std::move(*arrow_record_batch_result);
      if (!arrow_record_batch) {
        return arrow::AsyncGeneratorEnd<ExecBatchOptional>();
      }
      return arrow::Future<ExecBatchOptional>::MakeFinished(
        ExecBatch(*arrow_record_batch));
    });
  auto options = g_object_new(GARROW_TYPE_SOURCE_NODE_OPTIONS,
                              "options",
                              arrow_options,
                              "reader",
                              reader,
                              NULL);
  return GARROW_SOURCE_NODE_OPTIONS(options);
}

/**
 * garrow_source_node_options_new_record_batch:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: A newly created #GArrowSourceNodeOptions.
 *
 * Since: 6.0.0
 */
GArrowSourceNodeOptions *
garrow_source_node_options_new_record_batch(GArrowRecordBatch *record_batch)
{
  struct State
  {
    std::shared_ptr<arrow::RecordBatch> record_batch;
    bool generated;
  };
  auto state = std::make_shared<State>();
  state->record_batch = garrow_record_batch_get_raw(record_batch);
  state->generated = false;
  auto arrow_options =
    new arrow::acero::SourceNodeOptions(state->record_batch->schema(), [state]() {
      using ExecBatch = arrow::compute::ExecBatch;
      using ExecBatchOptional = std::optional<ExecBatch>;
      if (!state->generated) {
        state->generated = true;
        return arrow::Future<ExecBatchOptional>::MakeFinished(
          ExecBatch(*(state->record_batch)));
      } else {
        return arrow::AsyncGeneratorEnd<ExecBatchOptional>();
      }
    });
  auto options = g_object_new(GARROW_TYPE_SOURCE_NODE_OPTIONS,
                              "options",
                              arrow_options,
                              "record-batch",
                              record_batch,
                              NULL);
  return GARROW_SOURCE_NODE_OPTIONS(options);
}

/**
 * garrow_source_node_options_new_table:
 * @table: A #GArrowTable.
 *
 * Returns: A newly created #GArrowSourceNodeOptions.
 *
 * Since: 6.0.0
 */
GArrowSourceNodeOptions *
garrow_source_node_options_new_table(GArrowTable *table)
{
  auto reader = garrow_table_batch_reader_new(table);
  auto options = garrow_source_node_options_new_record_batch_reader(
    GARROW_RECORD_BATCH_READER(reader));
  g_object_unref(reader);
  return options;
}

G_DEFINE_TYPE(GArrowFilterNodeOptions,
              garrow_filter_node_options,
              GARROW_TYPE_EXECUTE_NODE_OPTIONS)

static void
garrow_filter_node_options_init(GArrowFilterNodeOptions *object)
{
}

static void
garrow_filter_node_options_class_init(GArrowFilterNodeOptionsClass *klass)
{
}

/**
 * garrow_filter_node_options_new:
 * @expression: A #GArrowExpression to be used for filter.
 *
 * Returns: A newly created #GArrowFilterNodeOptions.
 *
 * Since: 12.0.0
 */
GArrowFilterNodeOptions *
garrow_filter_node_options_new(GArrowExpression *expression)
{
  auto arrow_expression = garrow_expression_get_raw(expression);
  auto arrow_options = new arrow::acero::FilterNodeOptions(*arrow_expression);
  auto options =
    g_object_new(GARROW_TYPE_FILTER_NODE_OPTIONS, "options", arrow_options, NULL);
  return GARROW_FILTER_NODE_OPTIONS(options);
}

G_DEFINE_TYPE(GArrowProjectNodeOptions,
              garrow_project_node_options,
              GARROW_TYPE_EXECUTE_NODE_OPTIONS)

static void
garrow_project_node_options_init(GArrowProjectNodeOptions *object)
{
}

static void
garrow_project_node_options_class_init(GArrowProjectNodeOptionsClass *klass)
{
}

/**
 * garrow_project_node_options_new:
 * @expressions: (element-type GArrowExpression):
 *   A list of #GArrowExpression to be executed.
 * @names: (nullable) (array length=n_names):
 *   A list of output column names of @expressions. If @names is %NULL,
 *   the string representations of @expressions will be used.
 * @n_names: The number of @names.
 *
 * Returns: A newly created #GArrowProjectNodeOptions.
 *
 * Since: 11.0.0
 */
GArrowProjectNodeOptions *
garrow_project_node_options_new(GList *expressions, gchar **names, gsize n_names)
{
  std::vector<arrow::compute::Expression> arrow_expressions;
  std::vector<std::string> arrow_names;
  for (auto node = expressions; node; node = g_list_next(node)) {
    auto expression = GARROW_EXPRESSION(node->data);
    arrow_expressions.push_back(*garrow_expression_get_raw(expression));
  }
  for (gsize i = 0; i < n_names; ++i) {
    arrow_names.emplace_back(names[i]);
  }
  if (!arrow_names.empty()) {
    for (size_t i = arrow_names.size(); i < arrow_expressions.size(); ++i) {
      arrow_names.push_back(arrow_expressions[i].ToString());
    }
  }
  auto arrow_options =
    new arrow::acero::ProjectNodeOptions(arrow_expressions, arrow_names);
  auto options =
    g_object_new(GARROW_TYPE_PROJECT_NODE_OPTIONS, "options", arrow_options, NULL);
  return GARROW_PROJECT_NODE_OPTIONS(options);
}

typedef struct GArrowAggregationPrivate_
{
  gchar *function;
  GArrowFunctionOptions *options;
  gchar *input;
  gchar *output;
} GArrowAggregationPrivate;

enum {
  PROP_AGGREGATION_FUNCTION = 1,
  PROP_AGGREGATION_OPTIONS,
  PROP_AGGREGATION_INPUT,
  PROP_AGGREGATION_OUTPUT,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowAggregation, garrow_aggregation, G_TYPE_OBJECT)

#define GARROW_AGGREGATION_GET_PRIVATE(object)                                           \
  static_cast<GArrowAggregationPrivate *>(                                               \
    garrow_aggregation_get_instance_private(GARROW_AGGREGATION(object)))

static void
garrow_aggregation_dispose(GObject *object)
{
  auto priv = GARROW_AGGREGATION_GET_PRIVATE(object);
  if (priv->options) {
    g_object_unref(priv->options);
    priv->options = nullptr;
  }
  G_OBJECT_CLASS(garrow_aggregation_parent_class)->dispose(object);
}

static void
garrow_aggregation_finalize(GObject *object)
{
  auto priv = GARROW_AGGREGATION_GET_PRIVATE(object);
  g_free(priv->function);
  g_free(priv->input);
  g_free(priv->output);
  G_OBJECT_CLASS(garrow_aggregation_parent_class)->finalize(object);
}

static void
garrow_aggregation_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_AGGREGATION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_AGGREGATION_FUNCTION:
    priv->function = g_value_dup_string(value);
    break;
  case PROP_AGGREGATION_OPTIONS:
    priv->options = GARROW_FUNCTION_OPTIONS(g_value_dup_object(value));
    break;
  case PROP_AGGREGATION_INPUT:
    priv->input = g_value_dup_string(value);
    break;
  case PROP_AGGREGATION_OUTPUT:
    priv->output = g_value_dup_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_aggregation_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_AGGREGATION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_AGGREGATION_FUNCTION:
    g_value_set_string(value, priv->function);
    break;
  case PROP_AGGREGATION_OPTIONS:
    g_value_set_object(value, priv->options);
    break;
  case PROP_AGGREGATION_INPUT:
    g_value_set_string(value, priv->input);
    break;
  case PROP_AGGREGATION_OUTPUT:
    g_value_set_string(value, priv->output);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_aggregation_init(GArrowAggregation *object)
{
}

static void
garrow_aggregation_class_init(GArrowAggregationClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_aggregation_dispose;
  gobject_class->finalize = garrow_aggregation_finalize;
  gobject_class->set_property = garrow_aggregation_set_property;
  gobject_class->get_property = garrow_aggregation_get_property;

  GParamSpec *spec;
  /**
   * GArrowAggregation:function:
   *
   * The function name to aggregate.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_string(
    "function",
    "Function",
    "The function name to aggregate",
    NULL,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_AGGREGATION_FUNCTION, spec);

  /**
   * GArrowAggregation:options:
   *
   * The options of aggregate function.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object(
    "options",
    "Options",
    "The options of aggregate function",
    GARROW_TYPE_FUNCTION_OPTIONS,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_AGGREGATION_OPTIONS, spec);

  /**
   * GArrowAggregation:input:
   *
   * The input field name of aggregate function.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_string(
    "input",
    "Input",
    "The input field name of aggregate function",
    NULL,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_AGGREGATION_INPUT, spec);

  /**
   * GArrowAggregation:output:
   *
   * The output field name of aggregate function.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_string(
    "output",
    "Output",
    "The output field name of aggregate function",
    NULL,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_AGGREGATION_OUTPUT, spec);
}

/**
 * garrow_aggregation_new:
 * @function: A name of aggregate function.
 * @options: (nullable): A #GArrowFunctionOptions of aggregate function.
 * @input: An input field name of aggregate function.
 * @output: An output field name of aggregate function.
 *
 * Returns: A newly created #GArrowAggregation.
 *
 * Since: 6.0.0
 */
GArrowAggregation *
garrow_aggregation_new(const gchar *function,
                       GArrowFunctionOptions *options,
                       const gchar *input,
                       const gchar *output)
{
  return GARROW_AGGREGATION(g_object_new(GARROW_TYPE_AGGREGATION,
                                         "function",
                                         function,
                                         "options",
                                         options,
                                         "input",
                                         input,
                                         "output",
                                         output,
                                         NULL));
}

G_DEFINE_TYPE(GArrowAggregateNodeOptions,
              garrow_aggregate_node_options,
              GARROW_TYPE_EXECUTE_NODE_OPTIONS)

static void
garrow_aggregate_node_options_init(GArrowAggregateNodeOptions *object)
{
}

static void
garrow_aggregate_node_options_class_init(GArrowAggregateNodeOptionsClass *klass)
{
}

/**
 * garrow_aggregate_node_options_new:
 * @aggregations: (element-type GArrowAggregation): A list of #GArrowAggregation.
 * @keys: (nullable) (array length=n_keys): Group keys.
 * @n_keys: The number of @keys.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowAggregateNodeOptions on success,
 *   %NULL otherwise.
 *
 * Since: 6.0.0
 */
GArrowAggregateNodeOptions *
garrow_aggregate_node_options_new(GList *aggregations,
                                  const gchar **keys,
                                  gsize n_keys,
                                  GError **error)
{
  std::vector<arrow::compute::Aggregate> arrow_aggregates;
  std::vector<arrow::FieldRef> arrow_keys;
  for (auto node = aggregations; node; node = node->next) {
    auto aggregation_priv = GARROW_AGGREGATION_GET_PRIVATE(node->data);
    arrow::compute::FunctionOptions *function_options = nullptr;
    if (aggregation_priv->options) {
      function_options = garrow_function_options_get_raw(aggregation_priv->options);
    };
    std::vector<arrow::FieldRef> arrow_targets;
    if (!garrow_field_refs_add(arrow_targets,
                               aggregation_priv->input,
                               error,
                               "[aggregate-node-options][new][input]")) {
      return NULL;
    }
    arrow_aggregates.push_back({
      aggregation_priv->function,
      function_options ? function_options->Copy() : nullptr,
      arrow_targets[0],
      aggregation_priv->output,
    });
  }
  for (gsize i = 0; i < n_keys; ++i) {
    if (!garrow_field_refs_add(arrow_keys,
                               keys[i],
                               error,
                               "[aggregate-node-options][new][key]")) {
      return NULL;
    }
  }
  auto arrow_options = new arrow::acero::AggregateNodeOptions(std::move(arrow_aggregates),
                                                              std::move(arrow_keys));
  auto options =
    g_object_new(GARROW_TYPE_AGGREGATE_NODE_OPTIONS, "options", arrow_options, NULL);
  return GARROW_AGGREGATE_NODE_OPTIONS(options);
}

typedef struct GArrowSinkNodeOptionsPrivate_
{
  arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>> generator;
  GArrowRecordBatchReader *reader;
} GArrowSinkNodeOptionsPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSinkNodeOptions,
                           garrow_sink_node_options,
                           GARROW_TYPE_EXECUTE_NODE_OPTIONS)

#define GARROW_SINK_NODE_OPTIONS_GET_PRIVATE(object)                                     \
  static_cast<GArrowSinkNodeOptionsPrivate *>(                                           \
    garrow_sink_node_options_get_instance_private(GARROW_SINK_NODE_OPTIONS(object)))

static void
garrow_sink_node_options_dispose(GObject *object)
{
  auto priv = GARROW_SINK_NODE_OPTIONS_GET_PRIVATE(object);
  if (priv->reader) {
    g_object_unref(priv->reader);
    priv->reader = nullptr;
  }
  G_OBJECT_CLASS(garrow_sink_node_options_parent_class)->dispose(object);
}

static void
garrow_sink_node_options_finalize(GObject *object)
{
  auto priv = GARROW_SINK_NODE_OPTIONS_GET_PRIVATE(object);
  priv->generator.~function();
  G_OBJECT_CLASS(garrow_sink_node_options_parent_class)->finalize(object);
}

static void
garrow_sink_node_options_init(GArrowSinkNodeOptions *object)
{
  auto priv = GARROW_SINK_NODE_OPTIONS_GET_PRIVATE(object);
  new (&(priv->generator))
    arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>>();
}

static void
garrow_sink_node_options_class_init(GArrowSinkNodeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_sink_node_options_dispose;
  gobject_class->finalize = garrow_sink_node_options_finalize;
}

/**
 * garrow_sink_node_options_new:
 *
 * Returns: A newly created #GArrowSinkNodeOptions.
 *
 * Since: 6.0.0
 */
GArrowSinkNodeOptions *
garrow_sink_node_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_SINK_NODE_OPTIONS, NULL);
  auto priv = GARROW_SINK_NODE_OPTIONS_GET_PRIVATE(options);
  auto arrow_options = new arrow::acero::SinkNodeOptions(&(priv->generator));
  auto execute_node_options_priv = GARROW_EXECUTE_NODE_OPTIONS_GET_PRIVATE(options);
  execute_node_options_priv->options = arrow_options;
  return GARROW_SINK_NODE_OPTIONS(options);
}

/**
 * garrow_sink_node_options_get_reader:
 * @options: A #GArrowSinkNodeOptions.
 * @schema: A #GArrowSchema.
 *
 * Returns: (transfer full): A #GArrowRecordBatchReader to read generated record batches.
 *
 * Since: 6.0.0
 */
GArrowRecordBatchReader *
garrow_sink_node_options_get_reader(GArrowSinkNodeOptions *options, GArrowSchema *schema)
{
  auto arrow_schema = garrow_schema_get_raw(schema);
  auto priv = GARROW_SINK_NODE_OPTIONS_GET_PRIVATE(options);
  if (!priv->reader) {
    auto arrow_reader = arrow::acero::MakeGeneratorReader(arrow_schema,
                                                          std::move(priv->generator),
                                                          arrow::default_memory_pool());
    priv->reader = garrow_record_batch_reader_new_raw(&arrow_reader, nullptr);
  }
  g_object_ref(priv->reader);
  return priv->reader;
}

G_DEFINE_TYPE(GArrowHashJoinNodeOptions,
              garrow_hash_join_node_options,
              GARROW_TYPE_EXECUTE_NODE_OPTIONS)

static void
garrow_hash_join_node_options_init(GArrowHashJoinNodeOptions *object)
{
}

static void
garrow_hash_join_node_options_class_init(GArrowHashJoinNodeOptionsClass *klass)
{
  /* TODO: Add left_output_prefix and right_output_prefix properties */
}

/**
 * garrow_hash_join_node_options_new:
 * @type: A #GArrowJoinType to be used.
 * @left_keys: (array length=n_left_keys): Left join keys.
 * @n_left_keys: The number of @left_keys.
 * @right_keys: (array length=n_right_keys): Right join keys.
 * @n_right_keys: The number of @right_keys.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowHashJoinNodeOptions on success,
 *   %NULL otherwise.
 *
 * Since: 7.0.0
 */
GArrowHashJoinNodeOptions *
garrow_hash_join_node_options_new(GArrowJoinType type,
                                  const gchar **left_keys,
                                  gsize n_left_keys,
                                  const gchar **right_keys,
                                  gsize n_right_keys,
                                  GError **error)
{
  auto arrow_type = static_cast<arrow::acero::JoinType>(type);
  std::vector<arrow::FieldRef> arrow_left_keys;
  for (gsize i = 0; i < n_left_keys; ++i) {
    if (!garrow_field_refs_add(arrow_left_keys,
                               left_keys[i],
                               error,
                               "[hash-join-node-options][new][left-key]")) {
      return NULL;
    }
  }
  std::vector<arrow::FieldRef> arrow_right_keys;
  for (gsize i = 0; i < n_right_keys; ++i) {
    if (!garrow_field_refs_add(arrow_right_keys,
                               right_keys[i],
                               error,
                               "[hash-join-node-options][new][right-key]")) {
      return NULL;
    }
  }
  auto arrow_options = new arrow::acero::HashJoinNodeOptions(arrow_type,
                                                             std::move(arrow_left_keys),
                                                             std::move(arrow_right_keys));
  auto options =
    g_object_new(GARROW_TYPE_HASH_JOIN_NODE_OPTIONS, "options", arrow_options, NULL);
  return GARROW_HASH_JOIN_NODE_OPTIONS(options);
}

/**
 * garrow_hash_join_node_options_set_left_outputs:
 * @options: A #GArrowHashJoinNodeOptions.
 * @outputs: (array length=n_outputs): Output fields.
 * @n_outputs: The number of @outputs.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 7.0.0
 */
gboolean
garrow_hash_join_node_options_set_left_outputs(GArrowHashJoinNodeOptions *options,
                                               const gchar **outputs,
                                               gsize n_outputs,
                                               GError **error)
{
  auto arrow_options = static_cast<arrow::acero::HashJoinNodeOptions *>(
    garrow_execute_node_options_get_raw(GARROW_EXECUTE_NODE_OPTIONS(options)));
  arrow_options->output_all = false;
  arrow_options->left_output.clear();
  for (gsize i = 0; i < n_outputs; ++i) {
    if (!garrow_field_refs_add(arrow_options->left_output,
                               outputs[i],
                               error,
                               "[hash-join-node-options][set-left-outputs]")) {
      return FALSE;
    }
  }
  return TRUE;
}

/**
 * garrow_hash_join_node_options_set_right_outputs:
 * @options: A #GArrowHashJoinNodeOptions.
 * @outputs: (array length=n_outputs): Output fields.
 * @n_outputs: The number of @outputs.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 7.0.0
 */
gboolean
garrow_hash_join_node_options_set_right_outputs(GArrowHashJoinNodeOptions *options,
                                                const gchar **outputs,
                                                gsize n_outputs,
                                                GError **error)
{
  auto arrow_options = static_cast<arrow::acero::HashJoinNodeOptions *>(
    garrow_execute_node_options_get_raw(GARROW_EXECUTE_NODE_OPTIONS(options)));
  arrow_options->output_all = false;
  arrow_options->right_output.clear();
  for (gsize i = 0; i < n_outputs; ++i) {
    if (!garrow_field_refs_add(arrow_options->right_output,
                               outputs[i],
                               error,
                               "[hash-join-node-options][set-right-outputs]")) {
      return FALSE;
    }
  }
  return TRUE;
}

struct GArrowExecuteNodePrivate
{
  arrow::acero::ExecNode *node;
  GArrowExecuteNodeOptions *options;
};

enum {
  PROP_NODE = 1,
  PROP_OPTIONS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExecuteNode, garrow_execute_node, G_TYPE_OBJECT)

#define GARROW_EXECUTE_NODE_GET_PRIVATE(object)                                          \
  static_cast<GArrowExecuteNodePrivate *>(                                               \
    garrow_execute_node_get_instance_private(GARROW_EXECUTE_NODE(object)))

static void
garrow_execute_node_dispose(GObject *object)
{
  auto priv = GARROW_EXECUTE_NODE_GET_PRIVATE(object);
  if (priv->options) {
    g_object_unref(priv->options);
    priv->options = nullptr;
  }
  G_OBJECT_CLASS(garrow_execute_node_parent_class)->dispose(object);
}

static void
garrow_execute_node_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTE_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_NODE:
    priv->node = static_cast<arrow::acero::ExecNode *>(g_value_get_pointer(value));
    break;
  case PROP_OPTIONS:
    priv->options = static_cast<GArrowExecuteNodeOptions *>(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_execute_node_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTE_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OPTIONS:
    g_value_set_object(value, priv->options);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_execute_node_init(GArrowExecuteNode *object)
{
}

static void
garrow_execute_node_class_init(GArrowExecuteNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_execute_node_dispose;
  gobject_class->set_property = garrow_execute_node_set_property;
  gobject_class->get_property = garrow_execute_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "node",
    "Node",
    "The raw arrow::acero::ExecNode *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_NODE, spec);

  spec = g_param_spec_object(
    "options",
    "Options",
    "The options of this node",
    GARROW_TYPE_EXECUTE_NODE_OPTIONS,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OPTIONS, spec);
}

/**
 * garrow_execute_node_get_kind_name:
 * @node: A #GArrowExecuteNode.
 *
 * Returns: The kind name of the node.
 *
 * Since: 6.0.0
 */
const gchar *
garrow_execute_node_get_kind_name(GArrowExecuteNode *node)
{
  auto arrow_node = garrow_execute_node_get_raw(node);
  return arrow_node->kind_name();
}

/**
 * garrow_execute_node_get_output_schema:
 * @node: A #GArrowExecuteNode.
 *
 * Returns: (transfer full): The output schema of the node.
 *
 * Since: 6.0.0
 */
GArrowSchema *
garrow_execute_node_get_output_schema(GArrowExecuteNode *node)
{
  auto arrow_node = garrow_execute_node_get_raw(node);
  std::shared_ptr<arrow::Schema> arrow_schema = arrow_node->output_schema();
  return garrow_schema_new_raw(&arrow_schema);
}

struct GArrowExecutePlanPrivate
{
  std::shared_ptr<arrow::acero::ExecPlan> plan;
  GList *nodes;
};

enum {
  PROP_PLAN = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExecutePlan, garrow_execute_plan, G_TYPE_OBJECT)

#define GARROW_EXECUTE_PLAN_GET_PRIVATE(object)                                          \
  static_cast<GArrowExecutePlanPrivate *>(                                               \
    garrow_execute_plan_get_instance_private(GARROW_EXECUTE_PLAN(object)))

static void
garrow_execute_plan_finalize(GObject *object)
{
  auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(object);
  priv->plan.~shared_ptr();
  G_OBJECT_CLASS(garrow_execute_plan_parent_class)->finalize(object);
}

static void
garrow_execute_plan_dispose(GObject *object)
{
  auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(object);
  g_list_free_full(priv->nodes, g_object_unref);
  priv->nodes = nullptr;
  G_OBJECT_CLASS(garrow_execute_plan_parent_class)->dispose(object);
}

static void
garrow_execute_plan_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_PLAN:
    priv->plan =
      *static_cast<std::shared_ptr<arrow::acero::ExecPlan> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_execute_plan_init(GArrowExecutePlan *object)
{
  auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(object);
  new (&(priv->plan)) std::shared_ptr<arrow::acero::ExecPlan>;
}

static void
garrow_execute_plan_class_init(GArrowExecutePlanClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = garrow_execute_plan_finalize;
  gobject_class->dispose = garrow_execute_plan_dispose;
  gobject_class->set_property = garrow_execute_plan_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "plan",
    "Plan",
    "The raw std::shared_ptr<arrow::acero::ExecPlan>",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PLAN, spec);
}

/**
 * garrow_execute_plan_new:
 * @context: (nullable): A #GArrowExecuteContext or %NULL.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowExecutePlan on success,
 *   %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowExecutePlan *
garrow_execute_plan_new(GArrowExecuteContext *context, GError **error)
{
  arrow::Result<std::shared_ptr<arrow::acero::ExecPlan>> arrow_plan_result;
  if (context) {
    auto arrow_context = garrow_execute_context_get_raw(context);
    arrow_plan_result = arrow::acero::ExecPlan::Make(*arrow_context);
  } else {
    arrow_plan_result = arrow::acero::ExecPlan::Make();
  }
  if (garrow::check(error, arrow_plan_result, "[execute-plan][new]")) {
    return GARROW_EXECUTE_PLAN(
      g_object_new(GARROW_TYPE_EXECUTE_PLAN, "plan", &(*arrow_plan_result), NULL));
  } else {
    return NULL;
  }
}

/**
 * garrow_execute_plan_build_node:
 * @plan: A #GArrowExecutePlan.
 * @factory_name: A factory name to build a #GArrowExecuteNode.
 * @inputs: (element-type GArrowExecuteNode): An inputs to execute new node.
 * @options: A #GArrowExecuteNodeOptions for new node.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_node(GArrowExecutePlan *plan,
                               const gchar *factory_name,
                               GList *inputs,
                               GArrowExecuteNodeOptions *options,
                               GError **error)
{
  auto arrow_plan = garrow_execute_plan_get_raw(plan);
  std::vector<arrow::acero::ExecNode *> arrow_inputs;
  for (auto node = inputs; node; node = node->next) {
    auto arrow_node = garrow_execute_node_get_raw(GARROW_EXECUTE_NODE(node->data));
    arrow_inputs.push_back(arrow_node);
  }
  auto arrow_options = garrow_execute_node_options_get_raw(options);
  auto arrow_node_result = arrow::acero::MakeExecNode(factory_name,
                                                      arrow_plan.get(),
                                                      arrow_inputs,
                                                      *arrow_options);
  if (garrow::check(error, arrow_node_result, "[execute-plan][build-node]")) {
    auto arrow_node = *arrow_node_result;
    arrow_node->SetLabel(factory_name);
    auto node = garrow_execute_node_new_raw(arrow_node, options);
    g_object_ref(node);
    auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(plan);
    priv->nodes = g_list_prepend(priv->nodes, node);
    return node;
  } else {
    return NULL;
  }
}

/**
 * garrow_execute_plan_build_source_node:
 * @plan: A #GArrowExecutePlan.
 * @options: A #GArrowSourceNodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a shortcut of garrow_execute_plan_build_node() for source
 * node.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   for source on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_source_node(GArrowExecutePlan *plan,
                                      GArrowSourceNodeOptions *options,
                                      GError **error)
{
  return garrow_execute_plan_build_node(plan,
                                        "source",
                                        NULL,
                                        GARROW_EXECUTE_NODE_OPTIONS(options),
                                        error);
}

/**
 * garrow_execute_plan_build_filter_node:
 * @plan: A #GArrowExecutePlan.
 * @input: A #GArrowExecuteNode.
 * @options: A #GArrowFilterNodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a shortcut of garrow_execute_plan_build_node() for filter
 * node.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   for filter on success, %NULL on error.
 *
 * Since: 12.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_filter_node(GArrowExecutePlan *plan,
                                      GArrowExecuteNode *input,
                                      GArrowFilterNodeOptions *options,
                                      GError **error)
{
  GList *inputs = nullptr;
  inputs = g_list_prepend(inputs, input);
  auto node = garrow_execute_plan_build_node(plan,
                                             "filter",
                                             inputs,
                                             GARROW_EXECUTE_NODE_OPTIONS(options),
                                             error);
  g_list_free(inputs);
  return node;
}

/**
 * garrow_execute_plan_build_project_node:
 * @plan: A #GArrowExecutePlan.
 * @input: A #GArrowExecuteNode.
 * @options: A #GArrowProjectNodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a shortcut of garrow_execute_plan_build_node() for project
 * node.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   for project on success, %NULL on error.
 *
 * Since: 11.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_project_node(GArrowExecutePlan *plan,
                                       GArrowExecuteNode *input,
                                       GArrowProjectNodeOptions *options,
                                       GError **error)
{
  GList *inputs = nullptr;
  inputs = g_list_prepend(inputs, input);
  auto node = garrow_execute_plan_build_node(plan,
                                             "project",
                                             inputs,
                                             GARROW_EXECUTE_NODE_OPTIONS(options),
                                             error);
  g_list_free(inputs);
  return node;
}

/**
 * garrow_execute_plan_build_aggregate_node:
 * @plan: A #GArrowExecutePlan.
 * @input: A #GArrowExecuteNode.
 * @options: A #GArrowAggregateNodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a shortcut of garrow_execute_plan_build_node() for aggregate
 * node.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   for aggregation on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_aggregate_node(GArrowExecutePlan *plan,
                                         GArrowExecuteNode *input,
                                         GArrowAggregateNodeOptions *options,
                                         GError **error)
{
  GList *inputs = NULL;
  inputs = g_list_prepend(inputs, input);
  auto node = garrow_execute_plan_build_node(plan,
                                             "aggregate",
                                             inputs,
                                             GARROW_EXECUTE_NODE_OPTIONS(options),
                                             error);
  g_list_free(inputs);
  return node;
}

/**
 * garrow_execute_plan_build_sink_node:
 * @plan: A #GArrowExecutePlan.
 * @input: A #GArrowExecuteNode.
 * @options: A #GArrowSinkNodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a shortcut of garrow_execute_plan_build_node() for sink
 * node.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   for sink on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_sink_node(GArrowExecutePlan *plan,
                                    GArrowExecuteNode *input,
                                    GArrowSinkNodeOptions *options,
                                    GError **error)
{
  GList *inputs = NULL;
  inputs = g_list_prepend(inputs, input);
  auto node = garrow_execute_plan_build_node(plan,
                                             "sink",
                                             inputs,
                                             GARROW_EXECUTE_NODE_OPTIONS(options),
                                             error);
  g_list_free(inputs);
  return node;
}

/**
 * garrow_execute_plan_build_hash_join_node:
 * @plan: A #GArrowExecutePlan.
 * @left: A left #GArrowExecuteNode.
 * @right: A right #GArrowExecuteNode.
 * @options: A #GArrowHashJoinNodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * This is a shortcut of garrow_execute_plan_build_node() for hash
 * join node.
 *
 * Returns: (transfer full): A newly built and added #GArrowExecuteNode
 *   for hash join on success, %NULL on error.
 *
 * Since: 7.0.0
 */
GArrowExecuteNode *
garrow_execute_plan_build_hash_join_node(GArrowExecutePlan *plan,
                                         GArrowExecuteNode *left,
                                         GArrowExecuteNode *right,
                                         GArrowHashJoinNodeOptions *options,
                                         GError **error)
{
  GList *inputs = NULL;
  inputs = g_list_append(inputs, left);
  inputs = g_list_append(inputs, right);
  auto node = garrow_execute_plan_build_node(plan,
                                             "hashjoin",
                                             inputs,
                                             GARROW_EXECUTE_NODE_OPTIONS(options),
                                             error);
  g_list_free(inputs);
  return node;
}

/**
 * garrow_execute_plan_get_nodes:
 * @plan: A #GArrowExecutePlan.
 *
 * Returns: (transfer none) (element-type GArrowExecuteNode): A list of
 *   #GArrowExecuteNode of this plan.
 *
 * Since: 13.0.0
 */
GList *
garrow_execute_plan_get_nodes(GArrowExecutePlan *plan)
{
  auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(plan);
  return priv->nodes;
}

/**
 * garrow_execute_plan_validate:
 * @plan: A #GArrowExecutePlan.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
garrow_execute_plan_validate(GArrowExecutePlan *plan, GError **error)
{
  auto arrow_plan = garrow_execute_plan_get_raw(plan);
  return garrow::check(error, arrow_plan->Validate(), "[execute-plan][validate]");
}

/**
 * garrow_execute_plan_start:
 * @plan: A #GArrowExecutePlan.
 *
 * Starts this plan.
 *
 * Since: 6.0.0
 */
void
garrow_execute_plan_start(GArrowExecutePlan *plan)
{
  auto arrow_plan = garrow_execute_plan_get_raw(plan);
  arrow_plan->StartProducing();
}

/**
 * garrow_execute_plan_stop:
 * @plan: A #GArrowExecutePlan.
 *
 * Stops this plan.
 *
 * Since: 6.0.0
 */
void
garrow_execute_plan_stop(GArrowExecutePlan *plan)
{
  auto arrow_plan = garrow_execute_plan_get_raw(plan);
  arrow_plan->StopProducing();
}

/**
 * garrow_execute_plan_wait:
 * @plan: A #GArrowExecutePlan.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Waits for finishing this plan.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
garrow_execute_plan_wait(GArrowExecutePlan *plan, GError **error)
{
  auto arrow_plan = garrow_execute_plan_get_raw(plan);
  arrow_plan->finished().Wait();
  return garrow::check(error, arrow_plan->finished().status(), "[execute-plan][wait]");
}

typedef struct GArrowCastOptionsPrivate_
{
  GArrowDataType *to_data_type;
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

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCastOptions,
                           garrow_cast_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_CAST_OPTIONS_GET_PRIVATE(object)                                          \
  static_cast<GArrowCastOptionsPrivate *>(                                               \
    garrow_cast_options_get_instance_private(GARROW_CAST_OPTIONS(object)))

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
garrow_cast_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_cast_options_get_raw(GARROW_CAST_OPTIONS(object));

  switch (prop_id) {
  case PROP_TO_DATA_TYPE:
    {
      auto to_data_type = g_value_dup_object(value);
      if (priv->to_data_type) {
        g_object_unref(priv->to_data_type);
      }
      if (to_data_type) {
        priv->to_data_type = GARROW_DATA_TYPE(to_data_type);
        options->to_type = garrow_data_type_get_raw(priv->to_data_type);
      } else {
        priv->to_data_type = NULL;
        options->to_type = nullptr;
      }
      break;
    }
  case PROP_ALLOW_INT_OVERFLOW:
    options->allow_int_overflow = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_TIME_TRUNCATE:
    options->allow_time_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_TIME_OVERFLOW:
    options->allow_time_overflow = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_DECIMAL_TRUNCATE:
    options->allow_decimal_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_FLOAT_TRUNCATE:
    options->allow_float_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_INVALID_UTF8:
    options->allow_invalid_utf8 = g_value_get_boolean(value);
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
  auto options = garrow_cast_options_get_raw(GARROW_CAST_OPTIONS(object));

  switch (prop_id) {
  case PROP_TO_DATA_TYPE:
    g_value_set_object(value, priv->to_data_type);
    break;
  case PROP_ALLOW_INT_OVERFLOW:
    g_value_set_boolean(value, options->allow_int_overflow);
    break;
  case PROP_ALLOW_TIME_TRUNCATE:
    g_value_set_boolean(value, options->allow_time_truncate);
    break;
  case PROP_ALLOW_TIME_OVERFLOW:
    g_value_set_boolean(value, options->allow_time_overflow);
    break;
  case PROP_ALLOW_DECIMAL_TRUNCATE:
    g_value_set_boolean(value, options->allow_decimal_truncate);
    break;
  case PROP_ALLOW_FLOAT_TRUNCATE:
    g_value_set_boolean(value, options->allow_float_truncate);
    break;
  case PROP_ALLOW_INVALID_UTF8:
    g_value_set_boolean(value, options->allow_invalid_utf8);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_init(GArrowCastOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::CastOptions());
}

static void
garrow_cast_options_class_init(GArrowCastOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_cast_options_dispose;
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

enum {
  PROP_SKIP_NULLS = 1,
  PROP_MIN_COUNT,
};

G_DEFINE_TYPE(GArrowScalarAggregateOptions,
              garrow_scalar_aggregate_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_scalar_aggregate_options_set_property(GObject *object,
                                             guint prop_id,
                                             const GValue *value,
                                             GParamSpec *pspec)
{
  auto options =
    garrow_scalar_aggregate_options_get_raw(GARROW_SCALAR_AGGREGATE_OPTIONS(object));

  switch (prop_id) {
  case PROP_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  case PROP_MIN_COUNT:
    options->min_count = g_value_get_uint(value);
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
  auto options =
    garrow_scalar_aggregate_options_get_raw(GARROW_SCALAR_AGGREGATE_OPTIONS(object));

  switch (prop_id) {
  case PROP_SKIP_NULLS:
    g_value_set_boolean(value, options->skip_nulls);
    break;
  case PROP_MIN_COUNT:
    g_value_set_uint(value, options->min_count);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_scalar_aggregate_options_init(GArrowScalarAggregateOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ScalarAggregateOptions());
}

static void
garrow_scalar_aggregate_options_class_init(GArrowScalarAggregateOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

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

enum {
  PROP_MODE = 1,
};

G_DEFINE_TYPE(GArrowCountOptions, garrow_count_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_count_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto options = garrow_count_options_get_raw(GARROW_COUNT_OPTIONS(object));

  switch (prop_id) {
  case PROP_MODE:
    options->mode =
      static_cast<arrow::compute::CountOptions::CountMode>(g_value_get_enum(value));
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
  auto options = garrow_count_options_get_raw(GARROW_COUNT_OPTIONS(object));

  switch (prop_id) {
  case PROP_MODE:
    g_value_set_enum(value, options->mode);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_count_options_init(GArrowCountOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::CountOptions());
}

static void
garrow_count_options_class_init(GArrowCountOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_count_options_set_property;
  gobject_class->get_property = garrow_count_options_get_property;

  auto options = arrow::compute::CountOptions::Defaults();

  GParamSpec *spec;
  /**
   * GArrowCountOptions:null-selection-behavior:
   *
   * How to handle counted values.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_enum("mode",
                           "Count mode",
                           "Which values to count",
                           GARROW_TYPE_COUNT_MODE,
                           static_cast<GArrowCountMode>(options.mode),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MODE, spec);
}

/**
 * garrow_count_options_new:
 *
 * Returns: A newly created #GArrowCountOptions.
 *
 * Since: 6.0.0
 */
GArrowCountOptions *
garrow_count_options_new(void)
{
  auto count_options = g_object_new(GARROW_TYPE_COUNT_OPTIONS, NULL);
  return GARROW_COUNT_OPTIONS(count_options);
}

enum {
  PROP_NULL_SELECTION_BEHAVIOR = 1,
};

G_DEFINE_TYPE(GArrowFilterOptions, garrow_filter_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_filter_options_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto options = garrow_filter_options_get_raw(GARROW_FILTER_OPTIONS(object));

  switch (prop_id) {
  case PROP_NULL_SELECTION_BEHAVIOR:
    options->null_selection_behavior =
      static_cast<arrow::compute::FilterOptions::NullSelectionBehavior>(
        g_value_get_enum(value));
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
  auto options = garrow_filter_options_get_raw(GARROW_FILTER_OPTIONS(object));

  switch (prop_id) {
  case PROP_NULL_SELECTION_BEHAVIOR:
    g_value_set_enum(value, options->null_selection_behavior);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_filter_options_init(GArrowFilterOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::FilterOptions());
}

static void
garrow_filter_options_class_init(GArrowFilterOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_filter_options_set_property;
  gobject_class->get_property = garrow_filter_options_get_property;

  auto options = arrow::compute::FilterOptions::Defaults();

  GParamSpec *spec;
  /**
   * GArrowFilterOptions:null-selection-behavior:
   *
   * How to handle filtered values.
   *
   * Since: 0.17.0
   */
  spec = g_param_spec_enum(
    "null-selection-behavior",
    "NULL selection behavior",
    "How to handle filtered values",
    GARROW_TYPE_FILTER_NULL_SELECTION_BEHAVIOR,
    static_cast<GArrowFilterNullSelectionBehavior>(options.null_selection_behavior),
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

G_DEFINE_TYPE(GArrowTakeOptions, garrow_take_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_take_options_init(GArrowTakeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::TakeOptions());
}

static void
garrow_take_options_class_init(GArrowTakeOptionsClass *klass)
{
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

enum {
  PROP_ARRAY_SORT_OPTIONS_ORDER = 1,
};

G_DEFINE_TYPE(GArrowArraySortOptions,
              garrow_array_sort_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_array_sort_options_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto options = garrow_array_sort_options_get_raw(GARROW_ARRAY_SORT_OPTIONS(object));

  switch (prop_id) {
  case PROP_ARRAY_SORT_OPTIONS_ORDER:
    options->order = static_cast<arrow::compute::SortOrder>(g_value_get_enum(value));
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
  auto options = garrow_array_sort_options_get_raw(GARROW_ARRAY_SORT_OPTIONS(object));

  switch (prop_id) {
  case PROP_ARRAY_SORT_OPTIONS_ORDER:
    g_value_set_enum(value, static_cast<GArrowSortOrder>(options->order));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_sort_options_init(GArrowArraySortOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ArraySortOptions());
}

static void
garrow_array_sort_options_class_init(GArrowArraySortOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_array_sort_options_set_property;
  gobject_class->get_property = garrow_array_sort_options_get_property;

  auto options = arrow::compute::ArraySortOptions::Defaults();

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
                           static_cast<GArrowSortOrder>(options.order),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ARRAY_SORT_OPTIONS_ORDER, spec);
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
    g_object_new(GARROW_TYPE_ARRAY_SORT_OPTIONS, "order", order, NULL);
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

typedef struct GArrowSortKeyPrivate_
{
  arrow::compute::SortKey sort_key;
} GArrowSortKeyPrivate;

enum {
  PROP_SORT_KEY_TARGET = 1,
  PROP_SORT_KEY_ORDER,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSortKey, garrow_sort_key, G_TYPE_OBJECT)

#define GARROW_SORT_KEY_GET_PRIVATE(object)                                              \
  static_cast<GArrowSortKeyPrivate *>(                                                   \
    garrow_sort_key_get_instance_private(GARROW_SORT_KEY(object)))

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
  case PROP_SORT_KEY_TARGET:
    {
      auto name = priv->sort_key.target.name();
      if (name) {
        g_value_set_string(value, name->c_str());
      } else {
        g_value_set_string(value, priv->sort_key.target.ToDotPath().c_str());
      }
    }
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
  new (&priv->sort_key) arrow::compute::SortKey("");
}

static void
garrow_sort_key_class_init(GArrowSortKeyClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_sort_key_finalize;
  gobject_class->set_property = garrow_sort_key_set_property;
  gobject_class->get_property = garrow_sort_key_get_property;

  GParamSpec *spec;
  /**
   * GArrowSortKey:target:
   *
   * A name or dot path for the sort target.
   *
   *     dot_path = '.' name
   *              | '[' digit+ ']'
   *              | dot_path+
   *
   * Since: 7.0.0
   */
  spec = g_param_spec_string("target",
                             "Target",
                             "The sort target",
                             NULL,
                             static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class, PROP_SORT_KEY_TARGET, spec);

  /**
   * GArrowSortKey:order:
   *
   * How to order values.
   *
   * Since: 3.0.0
   */
  spec = g_param_spec_enum(
    "order",
    "Order",
    "How to order values",
    GARROW_TYPE_SORT_ORDER,
    0,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SORT_KEY_ORDER, spec);
}

/**
 * garrow_sort_key_new:
 * @target: A name or dot path for sort target.
 * @order: How to order by this sort key.
 *
 * Returns: A newly created #GArrowSortKey.
 *
 * Since: 3.0.0
 */
GArrowSortKey *
garrow_sort_key_new(const gchar *target, GArrowSortOrder order, GError **error)
{
  auto arrow_reference_result = garrow_field_reference_resolve_raw(target);
  if (!garrow::check(error, arrow_reference_result, "[sort-key][new]")) {
    return NULL;
  }
  auto sort_key = g_object_new(GARROW_TYPE_SORT_KEY, "order", order, NULL);
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(sort_key);
  priv->sort_key.target = *arrow_reference_result;
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
garrow_sort_key_equal(GArrowSortKey *sort_key, GArrowSortKey *other_sort_key)
{
  auto arrow_sort_key = garrow_sort_key_get_raw(sort_key);
  auto arrow_other_sort_key = garrow_sort_key_get_raw(other_sort_key);
  return arrow_sort_key->Equals(*arrow_other_sort_key);
}

G_DEFINE_TYPE(GArrowSortOptions, garrow_sort_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_sort_options_init(GArrowSortOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::SortOptions());
}

static void
garrow_sort_options_class_init(GArrowSortOptionsClass *klass)
{
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
  auto sort_options = GARROW_SORT_OPTIONS(g_object_new(GARROW_TYPE_SORT_OPTIONS, NULL));
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
garrow_sort_options_equal(GArrowSortOptions *options, GArrowSortOptions *other_options)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  auto arrow_other_options = garrow_sort_options_get_raw(other_options);
  if (!garrow_raw_sort_keys_equal(arrow_options->sort_keys,
                                  arrow_other_options->sort_keys)) {
    return FALSE;
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
  return garrow_sort_keys_new_raw(arrow_options->sort_keys);
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
garrow_sort_options_set_sort_keys(GArrowSortOptions *options, GList *sort_keys)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  garrow_raw_sort_keys_set(arrow_options->sort_keys, sort_keys);
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
garrow_sort_options_add_sort_key(GArrowSortOptions *options, GArrowSortKey *sort_key)
{
  auto arrow_options = garrow_sort_options_get_raw(options);
  garrow_raw_sort_keys_add(arrow_options->sort_keys, sort_key);
}

typedef struct GArrowSetLookupOptionsPrivate_
{
  GArrowDatum *value_set;
} GArrowSetLookupOptionsPrivate;

enum {
  PROP_SET_LOOKUP_OPTIONS_VALUE_SET = 1,
  PROP_SET_LOOKUP_OPTIONS_SKIP_NULLS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowSetLookupOptions,
                           garrow_set_lookup_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_SET_LOOKUP_OPTIONS_GET_PRIVATE(object)                                    \
  static_cast<GArrowSetLookupOptionsPrivate *>(                                          \
    garrow_set_lookup_options_get_instance_private(GARROW_SET_LOOKUP_OPTIONS(object)))

static void
garrow_set_lookup_options_dispose(GObject *object)
{
  auto priv = GARROW_SET_LOOKUP_OPTIONS_GET_PRIVATE(object);

  if (priv->value_set) {
    g_object_unref(priv->value_set);
    priv->value_set = NULL;
  }

  G_OBJECT_CLASS(garrow_set_lookup_options_parent_class)->dispose(object);
}

static void
garrow_set_lookup_options_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_SET_LOOKUP_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_set_lookup_options_get_raw(GARROW_SET_LOOKUP_OPTIONS(object));

  switch (prop_id) {
  case PROP_SET_LOOKUP_OPTIONS_VALUE_SET:
    {
      auto value_set = g_value_get_object(value);
      if (priv->value_set != value_set) {
        if (priv->value_set) {
          g_object_unref(priv->value_set);
        }
        priv->value_set = GARROW_DATUM(value_set);
        g_object_ref(priv->value_set);
        if (priv->value_set) {
          options->value_set = garrow_datum_get_raw(priv->value_set);
        } else {
          options->value_set = {};
        }
      }
    }
    break;
  case PROP_SET_LOOKUP_OPTIONS_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_set_lookup_options_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_SET_LOOKUP_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_set_lookup_options_get_raw(GARROW_SET_LOOKUP_OPTIONS(object));

  switch (prop_id) {
  case PROP_SET_LOOKUP_OPTIONS_VALUE_SET:
    g_value_set_object(value, priv->value_set);
    break;
  case PROP_SET_LOOKUP_OPTIONS_SKIP_NULLS:
    g_value_set_boolean(value,
                        options->skip_nulls.has_value() && options->skip_nulls.value());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_set_lookup_options_init(GArrowSetLookupOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::SetLookupOptions());
}

static void
garrow_set_lookup_options_class_init(GArrowSetLookupOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_set_lookup_options_dispose;
  gobject_class->set_property = garrow_set_lookup_options_set_property;
  gobject_class->get_property = garrow_set_lookup_options_get_property;

  arrow::compute::SetLookupOptions options;

  GParamSpec *spec;
  /**
   * GArrowSetLookupOptions:value-set:
   *
   * The set of values to look up input values into.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("value-set",
                             "Value set",
                             "The set of values to look up input values into",
                             GARROW_TYPE_DATUM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_SET_LOOKUP_OPTIONS_VALUE_SET, spec);

  /**
   * GArrowSetLookupOptions:skip-nulls:
   *
   * Whether NULLs are skipped or not.
   *
   * Since: 6.0.0
   */
  auto skip_nulls = (options.skip_nulls.has_value() && options.skip_nulls.value());
  spec = g_param_spec_boolean("skip-nulls",
                              "Skip NULLs",
                              "Whether NULLs are skipped or not",
                              skip_nulls,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_SET_LOOKUP_OPTIONS_SKIP_NULLS,
                                  spec);
}

/**
 * garrow_set_lookup_options_new:
 * @value_set: (nullable): A #GArrowArrayDatum or
 *   #GArrowChunkedArrayDatum to be looked up.
 *
 * Returns: A newly created #GArrowSetLookupOptions.
 *
 * Since: 6.0.0
 */
GArrowSetLookupOptions *
garrow_set_lookup_options_new(GArrowDatum *value_set)
{
  return GARROW_SET_LOOKUP_OPTIONS(
    g_object_new(GARROW_TYPE_SET_LOOKUP_OPTIONS, "value-set", value_set, NULL));
}

enum {
  PROP_VARIANCE_OPTIONS_DDOF = 1,
  PROP_VARIANCE_OPTIONS_SKIP_NULLS,
  PROP_VARIANCE_OPTIONS_MIN_COUNT,
};

G_DEFINE_TYPE(GArrowVarianceOptions,
              garrow_variance_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_VARIANCE_OPTIONS_GET_PRIVATE(object)                                      \
  static_cast<GArrowVarianceOptionsPrivate *>(                                           \
    garrow_variance_options_get_instance_private(GARROW_VARIANCE_OPTIONS(object)))

static void
garrow_variance_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_variance_options_get_raw(GARROW_VARIANCE_OPTIONS(object));

  switch (prop_id) {
  case PROP_VARIANCE_OPTIONS_DDOF:
    options->ddof = g_value_get_int(value);
    break;
  case PROP_VARIANCE_OPTIONS_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  case PROP_VARIANCE_OPTIONS_MIN_COUNT:
    options->min_count = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_variance_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_variance_options_get_raw(GARROW_VARIANCE_OPTIONS(object));

  switch (prop_id) {
  case PROP_VARIANCE_OPTIONS_DDOF:
    g_value_set_int(value, options->ddof);
    break;
  case PROP_VARIANCE_OPTIONS_SKIP_NULLS:
    g_value_set_boolean(value, options->skip_nulls);
    break;
  case PROP_VARIANCE_OPTIONS_MIN_COUNT:
    g_value_set_uint(value, options->min_count);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_variance_options_init(GArrowVarianceOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::VarianceOptions());
}

static void
garrow_variance_options_class_init(GArrowVarianceOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_variance_options_set_property;
  gobject_class->get_property = garrow_variance_options_get_property;

  arrow::compute::VarianceOptions options;

  GParamSpec *spec;
  /**
   * GArrowVarianceOptions:ddof:
   *
   * The Delta Degrees of Freedom (ddof) to be used.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_int("ddof",
                          "Delta Degrees of Freedom",
                          "The Delta Degrees of Freedom (ddof) to be used",
                          G_MININT,
                          G_MAXINT,
                          options.ddof,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_VARIANCE_OPTIONS_DDOF, spec);

  /**
   * GArrowVarianceOptions:skip-nulls:
   *
   * Whether NULLs are skipped or not.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_boolean("skip-nulls",
                              "Skip NULLs",
                              "Whether NULLs are skipped or not",
                              options.skip_nulls,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_VARIANCE_OPTIONS_SKIP_NULLS, spec);

  /**
   * GArrowVarianceOptions:min-count:
   *
   * If less than this many non-null values are observed, emit null.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_uint("min-count",
                           "Min count",
                           "If less than this many non-null values "
                           "are observed, emit null",
                           0,
                           G_MAXUINT,
                           options.min_count,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_VARIANCE_OPTIONS_MIN_COUNT, spec);
}

/**
 * garrow_variance_options_new:
 *
 * Returns: A newly created #GArrowVarianceOptions.
 *
 * Since: 6.0.0
 */
GArrowVarianceOptions *
garrow_variance_options_new(void)
{
  return GARROW_VARIANCE_OPTIONS(g_object_new(GARROW_TYPE_VARIANCE_OPTIONS, NULL));
}

enum {
  PROP_ROUND_OPTIONS_N_DIGITS = 1,
  PROP_ROUND_OPTIONS_MODE,
};

G_DEFINE_TYPE(GArrowRoundOptions, garrow_round_options, GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_ROUND_OPTIONS_GET_PRIVATE(object)                                         \
  static_cast<GArrowRoundOptionsPrivate *>(                                              \
    garrow_round_options_get_instance_private(GARROW_ROUND_OPTIONS(object)))

static void
garrow_round_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto options = garrow_round_options_get_raw(GARROW_ROUND_OPTIONS(object));

  switch (prop_id) {
  case PROP_ROUND_OPTIONS_N_DIGITS:
    options->ndigits = g_value_get_int64(value);
    break;
  case PROP_ROUND_OPTIONS_MODE:
    options->round_mode = static_cast<arrow::compute::RoundMode>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_round_options_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  auto options = garrow_round_options_get_raw(GARROW_ROUND_OPTIONS(object));

  switch (prop_id) {
  case PROP_ROUND_OPTIONS_N_DIGITS:
    g_value_set_int64(value, options->ndigits);
    break;
  case PROP_ROUND_OPTIONS_MODE:
    g_value_set_enum(value, static_cast<GArrowRoundMode>(options->round_mode));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_round_options_init(GArrowRoundOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::RoundOptions());
}

static void
garrow_round_options_class_init(GArrowRoundOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_round_options_set_property;
  gobject_class->get_property = garrow_round_options_get_property;

  arrow::compute::RoundOptions options;

  GParamSpec *spec;
  /**
   * GArrowRoundOptions:n-digits:
   *
   * The rounding precision (number of digits to round to).
   *
   * Since: 7.0.0
   */
  spec = g_param_spec_int64("n-digits",
                            "Number of digits to round to",
                            "The round precision",
                            G_MININT64,
                            G_MAXINT64,
                            options.ndigits,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ROUND_OPTIONS_N_DIGITS, spec);

  /**
   * GArrowRoundOptions:mode:
   *
   * The rounding and tie-breaking mode.
   *
   * Since: 7.0.0
   */
  spec = g_param_spec_enum("mode",
                           "Mode",
                           "The rounding and tie-breaking mode",
                           GARROW_TYPE_ROUND_MODE,
                           static_cast<GArrowRoundMode>(options.round_mode),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ROUND_OPTIONS_MODE, spec);
}

/**
 * garrow_round_options_new:
 *
 * Returns: A newly created #GArrowRoundOptions.
 *
 * Since: 7.0.0
 */
GArrowRoundOptions *
garrow_round_options_new(void)
{
  return GARROW_ROUND_OPTIONS(g_object_new(GARROW_TYPE_ROUND_OPTIONS, NULL));
}

typedef struct GArrowRoundToMultipleOptionsPrivate_
{
  GArrowScalar *multiple;
} GArrowRoundToMultipleOptionsPrivate;

enum {
  PROP_ROUND_TO_MULTIPLE_OPTIONS_MULTIPLE = 1,
  PROP_ROUND_TO_MULTIPLE_OPTIONS_MODE,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRoundToMultipleOptions,
                           garrow_round_to_multiple_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_ROUND_TO_MULTIPLE_OPTIONS_GET_PRIVATE(object)                             \
  static_cast<GArrowRoundToMultipleOptionsPrivate *>(                                    \
    garrow_round_to_multiple_options_get_instance_private(                               \
      GARROW_ROUND_TO_MULTIPLE_OPTIONS(object)))

static void
garrow_round_to_multiple_options_dispose(GObject *object)
{
  auto priv = GARROW_ROUND_TO_MULTIPLE_OPTIONS_GET_PRIVATE(object);

  if (priv->multiple) {
    g_object_unref(priv->multiple);
    priv->multiple = NULL;
  }

  G_OBJECT_CLASS(garrow_round_to_multiple_options_parent_class)->dispose(object);
}

static void
garrow_round_to_multiple_options_set_property(GObject *object,
                                              guint prop_id,
                                              const GValue *value,
                                              GParamSpec *pspec)
{
  auto priv = GARROW_ROUND_TO_MULTIPLE_OPTIONS_GET_PRIVATE(object);
  auto options =
    garrow_round_to_multiple_options_get_raw(GARROW_ROUND_TO_MULTIPLE_OPTIONS(object));

  switch (prop_id) {
  case PROP_ROUND_TO_MULTIPLE_OPTIONS_MULTIPLE:
    {
      auto multiple = g_value_get_object(value);
      if (priv->multiple != multiple) {
        if (priv->multiple) {
          g_object_unref(priv->multiple);
        }
        if (multiple) {
          priv->multiple = GARROW_SCALAR(multiple);
          g_object_ref(priv->multiple);
          options->multiple = garrow_scalar_get_raw(priv->multiple);
        } else {
          priv->multiple = NULL;
          options->multiple = std::make_shared<arrow::NullScalar>();
        }
      }
    }
    break;
  case PROP_ROUND_TO_MULTIPLE_OPTIONS_MODE:
    options->round_mode = static_cast<arrow::compute::RoundMode>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_round_to_multiple_options_get_property(GObject *object,
                                              guint prop_id,
                                              GValue *value,
                                              GParamSpec *pspec)
{
  auto priv = GARROW_ROUND_TO_MULTIPLE_OPTIONS_GET_PRIVATE(object);
  auto options =
    garrow_round_to_multiple_options_get_raw(GARROW_ROUND_TO_MULTIPLE_OPTIONS(object));

  switch (prop_id) {
  case PROP_ROUND_TO_MULTIPLE_OPTIONS_MULTIPLE:
    g_value_set_object(value, priv->multiple);
    break;
  case PROP_ROUND_TO_MULTIPLE_OPTIONS_MODE:
    g_value_set_enum(value, static_cast<GArrowRoundMode>(options->round_mode));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_round_to_multiple_options_init(GArrowRoundToMultipleOptions *object)
{
  auto function_options_priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  auto options = new arrow::compute::RoundToMultipleOptions();
  function_options_priv->options =
    static_cast<arrow::compute::FunctionOptions *>(options);
  auto priv = GARROW_ROUND_TO_MULTIPLE_OPTIONS_GET_PRIVATE(object);
  priv->multiple = garrow_scalar_new_raw(&(options->multiple));
}

static void
garrow_round_to_multiple_options_class_init(GArrowRoundToMultipleOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_round_to_multiple_options_dispose;
  gobject_class->set_property = garrow_round_to_multiple_options_set_property;
  gobject_class->get_property = garrow_round_to_multiple_options_get_property;

  arrow::compute::RoundToMultipleOptions options;

  GParamSpec *spec;
  /**
   * GArrowRoundToMultipleOptions:multiple:
   *
   * The rounding scale (multiple to round to).
   *
   * Should be a scalar of a type compatible with the argument to be rounded.
   * For example, rounding a decimal value means a decimal multiple is
   * required. Rounding a floating point or integer value means a floating
   * point scalar is required.
   *
   * Since: 7.0.0
   */
  spec = g_param_spec_object("multiple",
                             "Multiple to round to",
                             "The round scale",
                             GARROW_TYPE_SCALAR,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ROUND_TO_MULTIPLE_OPTIONS_MULTIPLE,
                                  spec);

  /**
   * GArrowRoundToMultipleOptions:mode:
   *
   * The rounding and tie-breaking mode.
   *
   * Since: 7.0.0
   */
  spec = g_param_spec_enum("mode",
                           "Mode",
                           "The rounding and tie-breaking mode",
                           GARROW_TYPE_ROUND_MODE,
                           static_cast<GArrowRoundMode>(options.round_mode),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ROUND_TO_MULTIPLE_OPTIONS_MODE,
                                  spec);
}

/**
 * garrow_round_to_multiple_options_new:
 *
 * Returns: A newly created #GArrowRoundToMultipleOptions.
 *
 * Since: 7.0.0
 */
GArrowRoundToMultipleOptions *
garrow_round_to_multiple_options_new(void)
{
  return GARROW_ROUND_TO_MULTIPLE_OPTIONS(
    g_object_new(GARROW_TYPE_ROUND_TO_MULTIPLE_OPTIONS, NULL));
}

enum {
  PROP_MATCH_SUBSTRING_OPTIONS_PATTERN = 1,
  PROP_MATCH_SUBSTRING_OPTIONS_IGNORE_CASE,
};

G_DEFINE_TYPE(GArrowMatchSubstringOptions,
              garrow_match_substring_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_match_substring_options_set_property(GObject *object,
                                            guint prop_id,
                                            const GValue *value,
                                            GParamSpec *pspec)
{
  auto options =
    garrow_match_substring_options_get_raw(GARROW_MATCH_SUBSTRING_OPTIONS(object));

  switch (prop_id) {
  case PROP_MATCH_SUBSTRING_OPTIONS_PATTERN:
    options->pattern = g_value_get_string(value);
    break;
  case PROP_MATCH_SUBSTRING_OPTIONS_IGNORE_CASE:
    options->ignore_case = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_match_substring_options_get_property(GObject *object,
                                            guint prop_id,
                                            GValue *value,
                                            GParamSpec *pspec)
{
  auto options =
    garrow_match_substring_options_get_raw(GARROW_MATCH_SUBSTRING_OPTIONS(object));

  switch (prop_id) {
  case PROP_MATCH_SUBSTRING_OPTIONS_PATTERN:
    g_value_set_string(value, options->pattern.c_str());
    break;
  case PROP_MATCH_SUBSTRING_OPTIONS_IGNORE_CASE:
    g_value_set_boolean(value, options->ignore_case);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_match_substring_options_init(GArrowMatchSubstringOptions *object)
{
  auto function_options_priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  function_options_priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::MatchSubstringOptions());
}

static void
garrow_match_substring_options_class_init(GArrowMatchSubstringOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->set_property = garrow_match_substring_options_set_property;
  gobject_class->get_property = garrow_match_substring_options_get_property;

  arrow::compute::MatchSubstringOptions options;

  GParamSpec *spec;
  /**
   * GArrowMatchSubstringOptions:pattern:
   *
   * The exact substring (or regex, depending on kernel) to look for
   * inside input values.
   *
   * Since: 12.0.0
   */
  spec = g_param_spec_string("pattern",
                             "Pattern",
                             "The pattern to be looked for",
                             NULL,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_MATCH_SUBSTRING_OPTIONS_PATTERN,
                                  spec);

  /**
   * GArrowMatchSubstringOptions:ignore-case:
   *
   * Whether to perform a case-insensitive match.
   *
   * Since: 12.0.0
   */
  spec = g_param_spec_boolean("ignore-case",
                              "Ignore case",
                              "Whether to perform a case-insensitive match",
                              options.ignore_case,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_MATCH_SUBSTRING_OPTIONS_IGNORE_CASE,
                                  spec);
}

/**
 * garrow_match_substring_options_new:
 *
 * Returns: A newly created #GArrowMatchSubstringOptions.
 *
 * Since: 12.0.0
 */
GArrowMatchSubstringOptions *
garrow_match_substring_options_new(void)
{
  return GARROW_MATCH_SUBSTRING_OPTIONS(
    g_object_new(GARROW_TYPE_MATCH_SUBSTRING_OPTIONS, NULL));
}

enum {
  PROP_UTF8_NORMALIZE_OPTIONS_FORM = 1,
};

G_DEFINE_TYPE(GArrowUTF8NormalizeOptions,
              garrow_utf8_normalize_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_UTF8_NORMARIZE_OPTIONS_GET_PRIVATE(object)                                \
  static_cast<GArrowUTF8NormalizeOptionsPrivate *>(                                      \
    garrow_utf8_normalize_options_get_instance_private(                                  \
      GARROW_UTF8_NORMALIZE_OPTIONS(object)))

static void
garrow_utf8_normalize_options_set_property(GObject *object,
                                           guint prop_id,
                                           const GValue *value,
                                           GParamSpec *pspec)
{
  auto options =
    garrow_utf8_normalize_options_get_raw(GARROW_UTF8_NORMALIZE_OPTIONS(object));

  switch (prop_id) {
  case PROP_UTF8_NORMALIZE_OPTIONS_FORM:
    options->form =
      static_cast<arrow::compute::Utf8NormalizeOptions::Form>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_utf8_normalize_options_get_property(GObject *object,
                                           guint prop_id,
                                           GValue *value,
                                           GParamSpec *pspec)
{
  auto options =
    garrow_utf8_normalize_options_get_raw(GARROW_UTF8_NORMALIZE_OPTIONS(object));

  switch (prop_id) {
  case PROP_UTF8_NORMALIZE_OPTIONS_FORM:
    g_value_set_enum(value, static_cast<GArrowUTF8NormalizeForm>(options->form));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_utf8_normalize_options_init(GArrowUTF8NormalizeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::Utf8NormalizeOptions());
}

static void
garrow_utf8_normalize_options_class_init(GArrowUTF8NormalizeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_utf8_normalize_options_set_property;
  gobject_class->get_property = garrow_utf8_normalize_options_get_property;

  arrow::compute::Utf8NormalizeOptions options;

  GParamSpec *spec;
  /**
   * GArrowUTF8NormalizeOptions:form:
   *
   * The Unicode normalization form to apply.
   *
   * Since: 8.0.0
   */
  spec = g_param_spec_enum("form",
                           "Form",
                           "The Unicode normalization form to apply",
                           GARROW_TYPE_UTF8_NORMALIZE_FORM,
                           static_cast<GArrowUTF8NormalizeForm>(options.form),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_UTF8_NORMALIZE_OPTIONS_FORM, spec);
}

/**
 * garrow_utf8_normalize_options_new:
 *
 * Returns: A newly created #GArrowUTF8NormalizeOptions.
 *
 * Since: 8.0.0
 */
GArrowUTF8NormalizeOptions *
garrow_utf8_normalize_options_new(void)
{
  return GARROW_UTF8_NORMALIZE_OPTIONS(
    g_object_new(GARROW_TYPE_UTF8_NORMALIZE_OPTIONS, NULL));
}

enum {
  PROP_QUANTILE_OPTIONS_INTERPOLATION = 1,
  PROP_QUANTILE_OPTIONS_SKIP_NULLS,
  PROP_QUANTILE_OPTIONS_MIN_COUNT,
};

G_DEFINE_TYPE(GArrowQuantileOptions,
              garrow_quantile_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_quantile_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_quantile_options_get_raw(GARROW_QUANTILE_OPTIONS(object));

  switch (prop_id) {
  case PROP_QUANTILE_OPTIONS_INTERPOLATION:
    options->interpolation = static_cast<arrow::compute::QuantileOptions::Interpolation>(
      g_value_get_enum(value));
    break;
  case PROP_QUANTILE_OPTIONS_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  case PROP_QUANTILE_OPTIONS_MIN_COUNT:
    options->min_count = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_quantile_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_quantile_options_get_raw(GARROW_QUANTILE_OPTIONS(object));

  switch (prop_id) {
  case PROP_QUANTILE_OPTIONS_INTERPOLATION:
    g_value_set_enum(value,
                     static_cast<GArrowQuantileInterpolation>(options->interpolation));
    break;
  case PROP_QUANTILE_OPTIONS_SKIP_NULLS:
    g_value_set_boolean(value, options->skip_nulls);
    break;
  case PROP_QUANTILE_OPTIONS_MIN_COUNT:
    g_value_set_uint(value, options->min_count);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_quantile_options_init(GArrowQuantileOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::QuantileOptions());
}

static void
garrow_quantile_options_class_init(GArrowQuantileOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_quantile_options_set_property;
  gobject_class->get_property = garrow_quantile_options_get_property;

  auto options = arrow::compute::QuantileOptions::Defaults();

  GParamSpec *spec;
  /**
   * GArrowQuantileOptions:interpolation:
   *
   * Interpolation method to use when quantile lies between two data
   * points.
   *
   * Since: 9.0.0
   */
  spec =
    g_param_spec_enum("interpolation",
                      "Interpolation",
                      "Interpolation method to use when "
                      "quantile lies between two data points.",
                      GARROW_TYPE_QUANTILE_INTERPOLATION,
                      static_cast<GArrowQuantileInterpolation>(options.interpolation),
                      static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_QUANTILE_OPTIONS_INTERPOLATION,
                                  spec);

  /**
   * GArrowQuantileOptions:skip-nulls:
   *
   * If true (the default), null values are ignored. Otherwise, if any
   * value is null, emit null.
   *
   * Since: 9.0.0
   */
  spec = g_param_spec_boolean("skip-nulls",
                              "Skip nulls",
                              "If true (the default), "
                              "null values are ignored. "
                              "Otherwise, if any value is null, "
                              "emit null.",
                              options.skip_nulls,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_QUANTILE_OPTIONS_SKIP_NULLS, spec);

  /**
   * GArrowQuantileOptions:min-count:
   *
   * If less than this many non-null values are observed, emit null.
   *
   * Since: 9.0.0
   */
  spec = g_param_spec_uint("min-count",
                           "Min count",
                           "If less than this many non-null values are "
                           "observed, emit null.",
                           0,
                           G_MAXUINT32,
                           options.min_count,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_QUANTILE_OPTIONS_MIN_COUNT, spec);
}

/**
 * garrow_quantile_options_new:
 *
 * Returns: A newly created #GArrowQuantileOptions.
 *
 * Since: 9.0.0
 */
GArrowQuantileOptions *
garrow_quantile_options_new(void)
{
  return GARROW_QUANTILE_OPTIONS(g_object_new(GARROW_TYPE_QUANTILE_OPTIONS, NULL));
}

/**
 * garrow_quantile_options_get_qs:
 * @options: A #GArrowQuantileOptions.
 * @n: (out): The number of `q`s.
 *
 * Returns: (array length=n) (transfer none): The `q`s to be used.
 *
 * Since: 9.0.0
 */
const gdouble *
garrow_quantile_options_get_qs(GArrowQuantileOptions *options, gsize *n)
{
  auto priv = garrow_quantile_options_get_raw(options);
  if (n) {
    *n = priv->q.size();
  }
  return priv->q.data();
}

/**
 * garrow_quantile_options_set_q:
 * @options: A #GArrowQuantileOptions.
 * @q: A `q` to be used.
 *
 * Since: 9.0.0
 */
void
garrow_quantile_options_set_q(GArrowQuantileOptions *options, gdouble q)
{
  auto priv = garrow_quantile_options_get_raw(options);
  priv->q.clear();
  priv->q.push_back(q);
}

/**
 * garrow_quantile_options_set_qs:
 * @options: A #GArrowQuantileOptions.
 * @qs: (array length=n): `q`s to be used.
 * @n: The number of @qs.
 *
 * Since: 9.0.0
 */
void
garrow_quantile_options_set_qs(GArrowQuantileOptions *options, const gdouble *qs, gsize n)
{
  auto priv = garrow_quantile_options_get_raw(options);
  priv->q.clear();
  for (gsize i = 0; i < n; i++) {
    priv->q.push_back(qs[i]);
  }
}

struct GArrowIndexOptionsPrivate
{
  GArrowScalar *value;
};

enum {
  PROP_INDEX_OPTIONS_VALUE = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowIndexOptions,
                           garrow_index_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_INDEX_OPTIONS_GET_PRIVATE(object)                                         \
  static_cast<GArrowIndexOptionsPrivate *>(                                              \
    garrow_index_options_get_instance_private(GARROW_INDEX_OPTIONS(object)))

static void
garrow_index_options_dispose(GObject *object)
{
  auto priv = GARROW_INDEX_OPTIONS_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = nullptr;
  }

  G_OBJECT_CLASS(garrow_index_options_parent_class)->dispose(object);
}

static void
garrow_index_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_INDEX_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_index_options_get_raw(GARROW_INDEX_OPTIONS(object));

  switch (prop_id) {
  case PROP_INDEX_OPTIONS_VALUE:
    {
      auto scalar = GARROW_SCALAR(g_value_get_object(value));
      if (priv->value == scalar) {
        return;
      }
      if (priv->value) {
        g_object_unref(priv->value);
      }
      priv->value = scalar;
      if (priv->value) {
        g_object_ref(priv->value);
        options->value = garrow_scalar_get_raw(scalar);
      } else {
        options->value = nullptr;
      }
      break;
    }
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_index_options_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_INDEX_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INDEX_OPTIONS_VALUE:
    g_value_set_object(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_index_options_init(GArrowIndexOptions *object)
{
  auto function_options_priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  auto options = new arrow::compute::IndexOptions();
  function_options_priv->options =
    static_cast<arrow::compute::FunctionOptions *>(options);
  auto priv = GARROW_INDEX_OPTIONS_GET_PRIVATE(object);
  priv->value = nullptr;
}

static void
garrow_index_options_class_init(GArrowIndexOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_index_options_dispose;
  gobject_class->set_property = garrow_index_options_set_property;
  gobject_class->get_property = garrow_index_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowIndexOptions:value:
   *
   * The value to be compared.
   *
   * Since: 12.0.0
   */
  spec = g_param_spec_object("value",
                             "Value",
                             "The value to be compared.",
                             GARROW_TYPE_SCALAR,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_INDEX_OPTIONS_VALUE, spec);
}

/**
 * garrow_index_options_new:
 *
 * Returns: A newly created #GArrowIndexOptions.
 *
 * Since: 12.0.0
 */
GArrowIndexOptions *
garrow_index_options_new(void)
{
  return GARROW_INDEX_OPTIONS(g_object_new(GARROW_TYPE_INDEX_OPTIONS, NULL));
}

enum {
  PROP_RANK_OPTIONS_NULL_PLACEMENT = 1,
  PROP_RANK_OPTIONS_TIEBREAKER,
};

G_DEFINE_TYPE(GArrowRankOptions, garrow_rank_options, GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_RANK_OPTIONS_GET_PRIVATE(object)                                          \
  static_cast<GArrowRankOptionsPrivate *>(                                               \
    garrow_rank_options_get_instance_private(GARROW_RANK_OPTIONS(object)))

static void
garrow_rank_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_rank_options_get_raw(GARROW_RANK_OPTIONS(object));

  switch (prop_id) {
  case PROP_RANK_OPTIONS_NULL_PLACEMENT:
    options->null_placement =
      static_cast<arrow::compute::NullPlacement>(g_value_get_enum(value));
    break;
  case PROP_RANK_OPTIONS_TIEBREAKER:
    options->tiebreaker =
      static_cast<arrow::compute::RankOptions::Tiebreaker>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_rank_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_rank_options_get_raw(GARROW_RANK_OPTIONS(object));

  switch (prop_id) {
  case PROP_RANK_OPTIONS_NULL_PLACEMENT:
    g_value_set_enum(value, static_cast<GArrowNullPlacement>(options->null_placement));
    break;
  case PROP_RANK_OPTIONS_TIEBREAKER:
    g_value_set_enum(value, static_cast<GArrowRankTiebreaker>(options->tiebreaker));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_rank_options_init(GArrowRankOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::RankOptions());
}

static void
garrow_rank_options_class_init(GArrowRankOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_rank_options_set_property;
  gobject_class->get_property = garrow_rank_options_get_property;

  auto options = arrow::compute::RankOptions::Defaults();

  GParamSpec *spec;
  /**
   * GArrowRankOptions:null-placement:
   *
   * Whether nulls and NaNs are placed at the start or at the end.
   *
   * Since: 12.0.0
   */
  spec = g_param_spec_enum("null-placement",
                           "Null placement",
                           "Whether nulls and NaNs are placed "
                           "at the start or at the end.",
                           GARROW_TYPE_NULL_PLACEMENT,
                           static_cast<GArrowNullPlacement>(options.null_placement),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_RANK_OPTIONS_NULL_PLACEMENT, spec);

  /**
   * GArrowRankOptions:tiebreaker:
   *
   * Tiebreaker for dealing with equal values in ranks.
   *
   * Since: 12.0.0
   */
  spec = g_param_spec_enum("tiebreaker",
                           "Tiebreaker",
                           "Tiebreaker for dealing with equal values in ranks.",
                           GARROW_TYPE_RANK_TIEBREAKER,
                           static_cast<GArrowRankTiebreaker>(options.tiebreaker),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_RANK_OPTIONS_TIEBREAKER, spec);
}

/**
 * garrow_rank_options_new:
 *
 * Returns: A newly created #GArrowRankOptions.
 *
 * Since: 12.0.0
 */
GArrowRankOptions *
garrow_rank_options_new(void)
{
  return GARROW_RANK_OPTIONS(g_object_new(GARROW_TYPE_RANK_OPTIONS, nullptr));
}

/**
 * garrow_rank_options_equal:
 * @options: A #GArrowRankOptions.
 * @other_options: A #GArrowRankOptions to be compared.
 *
 * Returns: %TRUE if both of them have the same option values, %FALSE
 *   otherwise.
 *
 * Since: 12.0.0
 */
gboolean
garrow_rank_options_equal(GArrowRankOptions *options, GArrowRankOptions *other_options)
{
  auto arrow_options = garrow_rank_options_get_raw(options);
  auto arrow_other_options = garrow_rank_options_get_raw(other_options);
  if (!garrow_raw_sort_keys_equal(arrow_options->sort_keys,
                                  arrow_other_options->sort_keys)) {
    return FALSE;
  }
  if (arrow_options->null_placement != arrow_other_options->null_placement) {
    return FALSE;
  }
  if (arrow_options->tiebreaker != arrow_other_options->tiebreaker) {
    return FALSE;
  }
  return TRUE;
}

/**
 * garrow_rank_options_get_sort_keys:
 * @options: A #GArrowSortOptions.
 *
 * Returns: (transfer full) (element-type GArrowSortKey):
 *   The sort keys to be used.
 *
 * Since: 12.0.0
 */
GList *
garrow_rank_options_get_sort_keys(GArrowRankOptions *options)
{
  auto arrow_options = garrow_rank_options_get_raw(options);
  return garrow_sort_keys_new_raw(arrow_options->sort_keys);
}

/**
 * garrow_rank_options_set_sort_keys:
 * @options: A #GArrowRankOptions.
 * @sort_keys: (element-type GArrowSortKey): The sort keys to be used.
 *
 * Set sort keys to be used.
 *
 * Since: 12.0.0
 */
void
garrow_rank_options_set_sort_keys(GArrowRankOptions *options, GList *sort_keys)
{
  auto arrow_options = garrow_rank_options_get_raw(options);
  garrow_raw_sort_keys_set(arrow_options->sort_keys, sort_keys);
}

/**
 * garrow_rank_options_add_sort_key:
 * @options: A #GArrowRankOptions.
 * @sort_key: The sort key to be added.
 *
 * Add a sort key to be used.
 *
 * Since: 12.0.0
 */
void
garrow_rank_options_add_sort_key(GArrowRankOptions *options, GArrowSortKey *sort_key)
{
  auto arrow_options = garrow_rank_options_get_raw(options);
  garrow_raw_sort_keys_add(arrow_options->sort_keys, sort_key);
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
    arrow_casted_array =
      arrow::compute::Cast(*arrow_array_raw, arrow_target_data_type, *arrow_options);
  } else {
    arrow_casted_array = arrow::compute::Cast(*arrow_array_raw, arrow_target_data_type);
  }
  if (garrow::check(error, arrow_casted_array, [&]() {
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
garrow_array_unique(GArrowArray *array, GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_unique_array = arrow::compute::Unique(arrow_array);
  if (garrow::check(error, arrow_unique_array, [&]() {
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
garrow_array_dictionary_encode(GArrowArray *array, GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_dictionary_encoded_datum = arrow::compute::DictionaryEncode(arrow_array);
  if (garrow::check(error, arrow_dictionary_encoded_datum, [&]() {
        std::stringstream message;
        message << "[array][dictionary-encode] <";
        message << arrow_array->type()->ToString();
        message << ">";
        return message.str();
      })) {
    auto arrow_dictionary_encoded_array = (*arrow_dictionary_encoded_datum).make_array();
    auto dictionary_encoded_array = garrow_array_new_raw(&arrow_dictionary_encoded_array);
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
garrow_array_count(GArrowArray *array, GArrowCountOptions *options, GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  arrow::Result<arrow::Datum> arrow_counted_datum;
  if (options) {
    auto arrow_options = garrow_count_options_get_raw(options);
    arrow_counted_datum = arrow::compute::Count(*arrow_array_raw, *arrow_options);
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
garrow_array_count_values(GArrowArray *array, GError **error)
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
garrow_boolean_array_invert(GArrowBooleanArray *array, GError **error)
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
garrow_numeric_array_mean(GArrowNumericArray *array, GError **error)
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
garrow_int8_array_sum(GArrowInt8Array *array, GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array, error, "[int8-array][sum]", 0);
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
garrow_uint8_array_sum(GArrowUInt8Array *array, GError **error)
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
garrow_int16_array_sum(GArrowInt16Array *array, GError **error)
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
garrow_uint16_array_sum(GArrowUInt16Array *array, GError **error)
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
garrow_int32_array_sum(GArrowInt32Array *array, GError **error)
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
garrow_uint32_array_sum(GArrowUInt32Array *array, GError **error)
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
garrow_int64_array_sum(GArrowInt64Array *array, GError **error)
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
garrow_uint64_array_sum(GArrowUInt64Array *array, GError **error)
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
garrow_float_array_sum(GArrowFloatArray *array, GError **error)
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
garrow_double_array_sum(GArrowDoubleArray *array, GError **error)
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
 * Returns: (nullable) (transfer full): The #GArrowArray filtered
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
    arrow_filtered_datum =
      arrow::compute::Filter(arrow_array, arrow_filter, *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_array, arrow_filter);
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
garrow_array_is_in(GArrowArray *left, GArrowArray *right, GError **error)
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
  if (garrow::check(error, arrow_is_in_datum, "[array][is-in][chunked-array]")) {
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
garrow_array_sort_indices(GArrowArray *array, GArrowSortOrder order, GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  auto arrow_order = static_cast<arrow::compute::SortOrder>(order);
  auto arrow_indices_array = arrow::compute::SortIndices(*arrow_array_raw, arrow_order);
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
garrow_array_sort_to_indices(GArrowArray *array, GError **error)
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
  if (garrow::check(error, arrow_indices_array, "[chunked-array][sort-indices]")) {
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
    arrow::compute::SortIndices(::arrow::Datum(*arrow_record_batch_raw), *arrow_options);
  if (garrow::check(error, arrow_indices_array, "[record-batch][sort-indices]")) {
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
garrow_table_sort_indices(GArrowTable *table, GArrowSortOptions *options, GError **error)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_table_raw = arrow_table.get();
  auto arrow_options = garrow_sort_options_get_raw(options);
  auto arrow_indices_array =
    arrow::compute::SortIndices(::arrow::Datum(*arrow_table_raw), *arrow_options);
  if (garrow::check(error, arrow_indices_array, "[table][sort-indices]")) {
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
 * Returns: (nullable) (transfer full): The #GArrowTable filtered
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
    arrow_filtered_datum =
      arrow::compute::Filter(arrow_table, arrow_filter, *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_table, arrow_filter);
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
 * Returns: (nullable) (transfer full): The #GArrowTable filtered
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
    arrow_filtered_datum =
      arrow::compute::Filter(arrow_table, arrow_filter, *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_table, arrow_filter);
  }
  if (garrow::check(error, arrow_filtered_datum, "[table][filter][chunked-array]")) {
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
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray filtered
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
    arrow_filtered_datum =
      arrow::compute::Filter(arrow_chunked_array, arrow_filter, *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_chunked_array, arrow_filter);
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
 * Returns: (nullable) (transfer full): The #GArrowChunkedArray filtered
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
    arrow_filtered_datum =
      arrow::compute::Filter(arrow_chunked_array, arrow_filter, *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_chunked_array, arrow_filter);
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
 * Returns: (nullable) (transfer full): The #GArrowRecordBatch filtered
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
    arrow_filtered_datum =
      arrow::compute::Filter(arrow_record_batch, arrow_filter, *arrow_options);
  } else {
    arrow_filtered_datum = arrow::compute::Filter(arrow_record_batch, arrow_filter);
  }
  if (garrow::check(error, arrow_filtered_datum, "[record-batch][filter]")) {
    auto arrow_filtered_record_batch = (*arrow_filtered_datum).record_batch();
    return garrow_record_batch_new_raw(&arrow_filtered_record_batch);
  } else {
    return NULL;
  }
}

struct GArrowRunEndEncodeOptionsPrivate
{
  GArrowDataType *run_end_data_type;
};

enum {
  PROP_RUN_END_ENCODE_OPTIONS_RUN_END_DATA_TYPE = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRunEndEncodeOptions,
                           garrow_run_end_encode_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_RUN_END_ENCODE_OPTIONS_GET_PRIVATE(object)                                \
  static_cast<GArrowRunEndEncodeOptionsPrivate *>(                                       \
    garrow_run_end_encode_options_get_instance_private(                                  \
      GARROW_RUN_END_ENCODE_OPTIONS(object)))

static void
garrow_run_end_encode_options_dispose(GObject *object)
{
  auto priv = GARROW_RUN_END_ENCODE_OPTIONS_GET_PRIVATE(object);

  if (priv->run_end_data_type) {
    g_object_unref(priv->run_end_data_type);
    priv->run_end_data_type = NULL;
  }

  G_OBJECT_CLASS(garrow_run_end_encode_options_parent_class)->dispose(object);
}

static void
garrow_run_end_encode_options_set_property(GObject *object,
                                           guint prop_id,
                                           const GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GARROW_RUN_END_ENCODE_OPTIONS_GET_PRIVATE(object);
  auto options =
    garrow_run_end_encode_options_get_raw(GARROW_RUN_END_ENCODE_OPTIONS(object));

  switch (prop_id) {
  case PROP_RUN_END_ENCODE_OPTIONS_RUN_END_DATA_TYPE:
    {
      auto run_end_data_type = g_value_dup_object(value);
      if (priv->run_end_data_type) {
        g_object_unref(priv->run_end_data_type);
      }
      if (run_end_data_type) {
        priv->run_end_data_type = GARROW_DATA_TYPE(run_end_data_type);
        options->run_end_type = garrow_data_type_get_raw(priv->run_end_data_type);
      } else {
        priv->run_end_data_type = NULL;
        options->run_end_type = nullptr;
      }
      break;
    }
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_run_end_encode_options_get_property(GObject *object,
                                           guint prop_id,
                                           GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GARROW_RUN_END_ENCODE_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RUN_END_ENCODE_OPTIONS_RUN_END_DATA_TYPE:
    g_value_set_object(value, priv->run_end_data_type);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_run_end_encode_options_init(GArrowRunEndEncodeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::RunEndEncodeOptions());
}

static void
garrow_run_end_encode_options_class_init(GArrowRunEndEncodeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_run_end_encode_options_dispose;
  gobject_class->set_property = garrow_run_end_encode_options_set_property;
  gobject_class->get_property = garrow_run_end_encode_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowRunEndEncodeOptions:run-end-data-type:
   *
   * The data type for run-end.
   *
   * Since: 13.0.0
   */
  spec = g_param_spec_object("run-end-data-type",
                             "run-end data type",
                             "The data type for run-end.",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_RUN_END_ENCODE_OPTIONS_RUN_END_DATA_TYPE,
                                  spec);
}

/**
 * garrow_run_end_encode_options_new:
 * @run_end_data_type: (nullable): A #GArrowDataType for run-end. If this
 *   is %NULL, garrow_int32_data_type_new() is used.
 *
 * Returns: A newly created #GArrowRunEndEncodeOptions.
 *
 * Since: 13.0.0
 */
GArrowRunEndEncodeOptions *
garrow_run_end_encode_options_new(GArrowDataType *run_end_data_type)
{
  bool need_run_end_data_type_unref = false;
  if (!run_end_data_type) {
    run_end_data_type = GARROW_DATA_TYPE(garrow_int32_data_type_new());
    need_run_end_data_type_unref = true;
  }
  auto options = g_object_new(GARROW_TYPE_RUN_END_ENCODE_OPTIONS,
                              "run-end-data-type",
                              run_end_data_type,
                              NULL);
  if (need_run_end_data_type_unref) {
    g_object_unref(run_end_data_type);
  }
  return GARROW_RUN_END_ENCODE_OPTIONS(options);
}

/**
 * garrow_array_run_end_encode:
 * @array: A #GArrowArray.
 * @options: (nullable): A #GArrowRunEndEncodeOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created #GArrowRunEndEncodeArray for the @array on success,
 *   %NULL on error.
 *
 * Since: 13.0.0
 */
GArrowRunEndEncodedArray *
garrow_array_run_end_encode(GArrowArray *array,
                            GArrowRunEndEncodeOptions *options,
                            GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  arrow::Result<arrow::Datum> arrow_run_end_encoded_datum_result;
  if (options) {
    auto arrow_options = garrow_run_end_encode_options_get_raw(options);
    arrow_run_end_encoded_datum_result =
      arrow::compute::RunEndEncode(arrow_array, *arrow_options);
  } else {
    arrow_run_end_encoded_datum_result = arrow::compute::RunEndEncode(arrow_array);
  }
  if (garrow::check(error, arrow_run_end_encoded_datum_result, [&]() {
        std::stringstream message;
        message << "[array][run-end-encode] <";
        message << arrow_array->type()->ToString();
        message << ">";
        return message.str();
      })) {
    auto arrow_run_end_encoded_array = (*arrow_run_end_encoded_datum_result).make_array();
    auto run_end_encoded_array = garrow_array_new_raw(&arrow_run_end_encoded_array);
    return GARROW_RUN_END_ENCODED_ARRAY(run_end_encoded_array);
  } else {
    return nullptr;
  }
}

/**
 * garrow_run_end_encoded_array_decode:
 * @array: A #GArrowRunEndEncodeArray to be decoded.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly decoded #GArrowArray for the @array on success,
 *   %NULL on error.
 *
 * Since: 13.0.0
 */
GArrowArray *
garrow_run_end_encoded_array_decode(GArrowRunEndEncodedArray *array, GError **error)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_decoded_datum_result = arrow::compute::RunEndDecode(arrow_array);
  if (garrow::check(error, arrow_decoded_datum_result, [&]() {
        std::stringstream message;
        message << "[run-end-encoded-array][decode] <";
        message << arrow_array->type()->ToString();
        message << ">";
        return message.str();
      })) {
    auto arrow_decoded_array = (*arrow_decoded_datum_result).make_array();
    return garrow_array_new_raw(&arrow_decoded_array);
  } else {
    return nullptr;
  }
}

enum {
  PROP_STRPTIME_OPTIONS_FORMAT = 1,
  PROP_STRPTIME_OPTIONS_UNIT,
  PROP_STRPTIME_OPTIONS_ERROR_IS_NULL,
};

G_DEFINE_TYPE(GArrowStrptimeOptions,
              garrow_strptime_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_strptime_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_strptime_options_get_raw(GARROW_STRPTIME_OPTIONS(object));

  switch (prop_id) {
  case PROP_STRPTIME_OPTIONS_FORMAT:
    options->format = g_value_get_string(value);
    break;
  case PROP_STRPTIME_OPTIONS_UNIT:
    options->unit =
      garrow_time_unit_to_raw(static_cast<GArrowTimeUnit>(g_value_get_enum(value)));
    break;
  case PROP_STRPTIME_OPTIONS_ERROR_IS_NULL:
    options->error_is_null = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_strptime_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_strptime_options_get_raw(GARROW_STRPTIME_OPTIONS(object));

  switch (prop_id) {
  case PROP_STRPTIME_OPTIONS_FORMAT:
    g_value_set_string(value, options->format.c_str());
    break;
  case PROP_STRPTIME_OPTIONS_UNIT:
    g_value_set_enum(value, garrow_time_unit_from_raw(options->unit));
    break;
  case PROP_STRPTIME_OPTIONS_ERROR_IS_NULL:
    g_value_set_boolean(value, options->error_is_null);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_strptime_options_init(GArrowStrptimeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::StrptimeOptions());
}

static void
garrow_strptime_options_class_init(GArrowStrptimeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_strptime_options_set_property;
  gobject_class->get_property = garrow_strptime_options_get_property;

  arrow::compute::StrptimeOptions options;

  GParamSpec *spec;
  /**
   * GArrowStrptimeOptions:format:
   *
   * The desired format string.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_string("format",
                             "Format",
                             "The desired format string",
                             options.format.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_STRPTIME_OPTIONS_FORMAT, spec);

  /**
   * GArrowStrptimeOptions:unit:
   *
   * The desired time resolution.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_enum("unit",
                           "Unit",
                           "The desired time resolution",
                           GARROW_TYPE_TIME_UNIT,
                           garrow_time_unit_from_raw(options.unit),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_STRPTIME_OPTIONS_UNIT, spec);

  /**
   * GArrowStrptimeOptions:error-is-null:
   *
   * Return null on parsing errors if true or raise if false.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_boolean("error-is-null",
                              "Error is null",
                              "Return null on parsing errors if true or raise if false",
                              options.error_is_null,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_STRPTIME_OPTIONS_ERROR_IS_NULL,
                                  spec);
}

/**
 * garrow_strptime_options_new:
 *
 * Returns: A newly created #GArrowStrptimeOptions.
 *
 * Since: 16.0.0
 */
GArrowStrptimeOptions *
garrow_strptime_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_STRPTIME_OPTIONS, NULL);
  return GARROW_STRPTIME_OPTIONS(options);
}

enum {
  PROP_STRFTIME_OPTIONS_FORMAT = 1,
  PROP_STRFTIME_OPTIONS_LOCALE,
};

G_DEFINE_TYPE(GArrowStrftimeOptions,
              garrow_strftime_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_strftime_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_strftime_options_get_raw(GARROW_STRFTIME_OPTIONS(object));

  switch (prop_id) {
  case PROP_STRFTIME_OPTIONS_FORMAT:
    options->format = g_value_get_string(value);
    break;
  case PROP_STRFTIME_OPTIONS_LOCALE:
    options->locale = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_strftime_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_strftime_options_get_raw(GARROW_STRFTIME_OPTIONS(object));

  switch (prop_id) {
  case PROP_STRFTIME_OPTIONS_FORMAT:
    g_value_set_string(value, options->format.c_str());
    break;
  case PROP_STRFTIME_OPTIONS_LOCALE:
    g_value_set_string(value, options->locale.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_strftime_options_init(GArrowStrftimeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::StrftimeOptions());
}

static void
garrow_strftime_options_class_init(GArrowStrftimeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_strftime_options_set_property;
  gobject_class->get_property = garrow_strftime_options_get_property;

  arrow::compute::StrftimeOptions options;

  GParamSpec *spec;
  /**
   * GArrowStrftimeOptions:format:
   *
   * The desired format string.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_string("format",
                             "Format",
                             "The desired format string",
                             options.format.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_STRFTIME_OPTIONS_FORMAT, spec);

  /**
   * GArrowStrftimeOptions:locale:
   *
   * The desired output locale string.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_string("locale",
                             "locale",
                             "The desired output locale string",
                             options.locale.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_STRFTIME_OPTIONS_LOCALE, spec);
}

/**
 * garrow_strftime_options_new:
 *
 * Returns: A newly created #GArrowStrftimeOptions.
 *
 * Since: 16.0.0
 */
GArrowStrftimeOptions *
garrow_strftime_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_STRFTIME_OPTIONS, NULL);
  return GARROW_STRFTIME_OPTIONS(options);
}

enum {
  PROP_SPLIT_PATTERN_OPTIONS_PATTERN = 1,
  PROP_SPLIT_PATTERN_OPTIONS_MAX_SPLITS,
  PROP_SPLIT_PATTERN_OPTIONS_REVERSE,
};

G_DEFINE_TYPE(GArrowSplitPatternOptions,
              garrow_split_pattern_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_split_pattern_options_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_split_pattern_options_get_raw(GARROW_SPLIT_PATTERN_OPTIONS(object));

  switch (prop_id) {
  case PROP_SPLIT_PATTERN_OPTIONS_PATTERN:
    options->pattern = g_value_get_string(value);
    break;
  case PROP_SPLIT_PATTERN_OPTIONS_MAX_SPLITS:
    options->max_splits = g_value_get_int64(value);
    break;
  case PROP_SPLIT_PATTERN_OPTIONS_REVERSE:
    options->reverse = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_split_pattern_options_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_split_pattern_options_get_raw(GARROW_SPLIT_PATTERN_OPTIONS(object));

  switch (prop_id) {
  case PROP_SPLIT_PATTERN_OPTIONS_PATTERN:
    g_value_set_string(value, options->pattern.c_str());
    break;
  case PROP_SPLIT_PATTERN_OPTIONS_MAX_SPLITS:
    g_value_set_int64(value, options->max_splits);
    break;
  case PROP_SPLIT_PATTERN_OPTIONS_REVERSE:
    g_value_set_boolean(value, options->reverse);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_split_pattern_options_init(GArrowSplitPatternOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::SplitPatternOptions());
}

static void
garrow_split_pattern_options_class_init(GArrowSplitPatternOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_split_pattern_options_set_property;
  gobject_class->get_property = garrow_split_pattern_options_get_property;

  arrow::compute::SplitPatternOptions options;

  GParamSpec *spec;
  /**
   * GArrowSplitPatternOptions:pattern:
   *
   * The exact substring to split on.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_string("pattern",
                             "Pattern",
                             "The exact substring to split on",
                             options.pattern.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_SPLIT_PATTERN_OPTIONS_PATTERN,
                                  spec);

  /**
   * GArrowSplitPatternOptions:max_splits:
   *
   * Maximum number of splits allowed, or unlimited when -1.
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_int64("max_splits",
                            "Max splits",
                            "Maximum number of splits allowed, or unlimited when -1",
                            G_MININT64,
                            G_MAXINT64,
                            options.max_splits,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_SPLIT_PATTERN_OPTIONS_MAX_SPLITS,
                                  spec);

  /**
   * GArrowSplitPatternOptions:reverse:
   *
   * Start splitting from the end of the string (only relevant when
   * max_splits != -1)
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_boolean(
    "reverse",
    "Reverse",
    "Start splitting from the end of the string (only relevant when max_splits != -1)",
    options.reverse,
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_SPLIT_PATTERN_OPTIONS_REVERSE,
                                  spec);
}

/**
 * garrow_split_pattern_options_new:
 *
 * Returns: A newly created #GArrowSplitPatternOptions.
 *
 * Since: 16.0.0
 */
GArrowSplitPatternOptions *
garrow_split_pattern_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_SPLIT_PATTERN_OPTIONS, NULL);
  return GARROW_SPLIT_PATTERN_OPTIONS(options);
}

enum {
  PROP_STRUCT_FIELD_OPTIONS_FIELD_REF = 1,
};

G_DEFINE_TYPE(GArrowStructFieldOptions,
              garrow_struct_field_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

/**
 * garrow_struct_field_options_set_field_ref:
 * @field_ref: The name or dot path specifying what to extract from struct or
 *  union.
 *
 * Since: 16.0.0
 */
void
garrow_struct_field_options_set_field_ref(GArrowStructFieldOptions *options,
                                          const gchar *field_ref,
                                          GError **error)
{
  auto arrow_options =
    garrow_struct_field_options_get_raw(GARROW_STRUCT_FIELD_OPTIONS(options));

  auto arrow_reference_result = garrow_field_reference_resolve_raw(field_ref);
  if (!garrow::check(error,
                     arrow_reference_result,
                     "[struct-field-options][set-field-ref]")) {
    return;
  }
  arrow_options->field_ref = *arrow_reference_result;
}

static void
garrow_struct_field_options_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto options = garrow_struct_field_options_get_raw(GARROW_STRUCT_FIELD_OPTIONS(object));

  switch (prop_id) {
  case PROP_STRUCT_FIELD_OPTIONS_FIELD_REF:
    {
      auto name = options->field_ref.name();
      if (name) {
        g_value_set_string(value, name->c_str());
      } else {
        g_value_set_string(value, options->field_ref.ToDotPath().c_str());
      }
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_struct_field_options_init(GArrowStructFieldOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::StructFieldOptions());
}

static void
garrow_struct_field_options_class_init(GArrowStructFieldOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->get_property = garrow_struct_field_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowStructFieldOptions:field_ref:
   *
   * The name or dot path specifying what to extract from struct or union.
   *
   *     dot_path = '.' name
   *              | '[' digit+ ']'
   *              | dot_path+
   *
   * Since: 16.0.0
   */
  spec = g_param_spec_string(
    "field_ref",
    "Field ref",
    "The name or dot path specifying what to extract from struct or union.",
    "",
    static_cast<GParamFlags>(G_PARAM_READABLE));
  g_object_class_install_property(gobject_class,
                                  PROP_STRUCT_FIELD_OPTIONS_FIELD_REF,
                                  spec);
}

/**
 * garrow_struct_field_options_new:
 *
 * Returns: A newly created #GArrowStructFieldOptions.
 *
 * Since: 16.0.0
 */
GArrowStructFieldOptions *
garrow_struct_field_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_STRUCT_FIELD_OPTIONS, NULL);
  return GARROW_STRUCT_FIELD_OPTIONS(options);
}

enum {
  PROP_ASSUME_TIMEZONE_OPTIONS_TIMEZONE = 1,
  PROP_ASSUME_TIMEZONE_OPTIONS_AMBIGUOUS,
  PROP_ASSUME_TIMEZONE_OPTIONS_NONEXISTENT,
};

G_DEFINE_TYPE(GArrowAssumeTimezoneOptions,
              garrow_assume_timezone_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_assume_timezone_options_set_property(GObject *object,
                                            guint prop_id,
                                            const GValue *value,
                                            GParamSpec *pspec)
{
  auto options =
    garrow_assume_timezone_options_get_raw(GARROW_ASSUME_TIMEZONE_OPTIONS(object));

  switch (prop_id) {
  case PROP_ASSUME_TIMEZONE_OPTIONS_TIMEZONE:
    options->timezone = g_value_get_string(value);
    break;
  case PROP_ASSUME_TIMEZONE_OPTIONS_AMBIGUOUS:
    options->ambiguous = static_cast<arrow::compute::AssumeTimezoneOptions::Ambiguous>(
      g_value_get_enum(value));
    break;
  case PROP_ASSUME_TIMEZONE_OPTIONS_NONEXISTENT:
    options->nonexistent =
      static_cast<arrow::compute::AssumeTimezoneOptions::Nonexistent>(
        g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_assume_timezone_options_get_property(GObject *object,
                                            guint prop_id,
                                            GValue *value,
                                            GParamSpec *pspec)
{
  auto options =
    garrow_assume_timezone_options_get_raw(GARROW_ASSUME_TIMEZONE_OPTIONS(object));

  switch (prop_id) {
  case PROP_ASSUME_TIMEZONE_OPTIONS_TIMEZONE:
    g_value_set_string(value, options->timezone.c_str());
    break;
  case PROP_ASSUME_TIMEZONE_OPTIONS_AMBIGUOUS:
    g_value_set_enum(value,
                     static_cast<GArrowAssumeTimezoneAmbiguous>(options->ambiguous));
    break;
  case PROP_ASSUME_TIMEZONE_OPTIONS_NONEXISTENT:
    g_value_set_enum(value,
                     static_cast<GArrowAssumeTimezoneNonexistent>(options->nonexistent));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_assume_timezone_options_init(GArrowAssumeTimezoneOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::AssumeTimezoneOptions());
}

static void
garrow_assume_timezone_options_class_init(GArrowAssumeTimezoneOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_assume_timezone_options_set_property;
  gobject_class->get_property = garrow_assume_timezone_options_get_property;

  arrow::compute::AssumeTimezoneOptions options;

  GParamSpec *spec;
  /**
   * GArrowAssumeTimezoneOptions:timezone:
   *
   * Timezone to convert timestamps from.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_string("timezone",
                             "Timezone",
                             "Timezone to convert timestamps from",
                             options.timezone.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ASSUME_TIMEZONE_OPTIONS_TIMEZONE,
                                  spec);

  /**
   * GArrowAssumeTimezoneOptions:ambiguous:
   *
   * How to interpret ambiguous local times (due to DST shifts).
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_enum("ambiguous",
                           "Ambiguous",
                           "How to interpret ambiguous local times (due to DST shifts)",
                           GARROW_TYPE_ASSUME_TIMEZONE_AMBIGUOUS,
                           static_cast<GArrowAssumeTimezoneAmbiguous>(options.ambiguous),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ASSUME_TIMEZONE_OPTIONS_AMBIGUOUS,
                                  spec);

  /**
   * GArrowAssumeTimezoneOptions:nonexistent:
   *
   * How to interpret nonexistent local times (due to DST shifts).
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_enum("nonexistent",
                      "Nonexistent",
                      "How to interpret nonexistent local times (due to DST shifts)",
                      GARROW_TYPE_ASSUME_TIMEZONE_NONEXISTENT,
                      static_cast<GArrowAssumeTimezoneNonexistent>(options.nonexistent),
                      static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ASSUME_TIMEZONE_OPTIONS_NONEXISTENT,
                                  spec);
}

/**
 * garrow_assume_timezone_options_new:
 *
 * Returns: A newly created #GArrowAssumeTimezoneOptions.
 *
 * Since: 23.0.0
 */
GArrowAssumeTimezoneOptions *
garrow_assume_timezone_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_ASSUME_TIMEZONE_OPTIONS, NULL);
  return GARROW_ASSUME_TIMEZONE_OPTIONS(options);
}

typedef struct GArrowCumulativeOptionsPrivate_
{
  GArrowScalar *start;
} GArrowCumulativeOptionsPrivate;

enum {
  PROP_CUMULATIVE_OPTIONS_START = 1,
  PROP_CUMULATIVE_OPTIONS_SKIP_NULLS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCumulativeOptions,
                           garrow_cumulative_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_CUMULATIVE_OPTIONS_GET_PRIVATE(object)                                    \
  static_cast<GArrowCumulativeOptionsPrivate *>(                                         \
    garrow_cumulative_options_get_instance_private(GARROW_CUMULATIVE_OPTIONS(object)))

static void
garrow_cumulative_options_dispose(GObject *object)
{
  auto priv = GARROW_CUMULATIVE_OPTIONS_GET_PRIVATE(object);

  if (priv->start) {
    g_object_unref(priv->start);
    priv->start = nullptr;
  }

  G_OBJECT_CLASS(garrow_cumulative_options_parent_class)->dispose(object);
}

static void
garrow_cumulative_options_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_CUMULATIVE_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_cumulative_options_get_raw(GARROW_CUMULATIVE_OPTIONS(object));

  switch (prop_id) {
  case PROP_CUMULATIVE_OPTIONS_START:
    {
      auto scalar = GARROW_SCALAR(g_value_get_object(value));
      if (priv->start == scalar) {
        return;
      }
      if (priv->start) {
        g_object_unref(priv->start);
      }
      priv->start = scalar;
      if (priv->start) {
        g_object_ref(priv->start);
        options->start = garrow_scalar_get_raw(scalar);
      } else {
        options->start = std::nullopt;
      }
      break;
    }
  case PROP_CUMULATIVE_OPTIONS_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cumulative_options_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_CUMULATIVE_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_cumulative_options_get_raw(GARROW_CUMULATIVE_OPTIONS(object));

  switch (prop_id) {
  case PROP_CUMULATIVE_OPTIONS_START:
    {
      if (priv->start) {
        g_value_set_object(value, G_OBJECT(priv->start));
      } else {
        g_value_set_object(value, NULL);
      }
      break;
    }
  case PROP_CUMULATIVE_OPTIONS_SKIP_NULLS:
    g_value_set_boolean(value, options->skip_nulls);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cumulative_options_init(GArrowCumulativeOptions *object)
{
  auto priv = GARROW_CUMULATIVE_OPTIONS_GET_PRIVATE(object);
  priv->start = nullptr;
  auto function_options_priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  function_options_priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::CumulativeOptions());
}

static void
garrow_cumulative_options_class_init(GArrowCumulativeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_cumulative_options_dispose;
  gobject_class->set_property = garrow_cumulative_options_set_property;
  gobject_class->get_property = garrow_cumulative_options_get_property;

  arrow::compute::CumulativeOptions options;

  GParamSpec *spec;
  /**
   * GArrowCumulativeOptions:start:
   *
   * Optional starting value for cumulative operation computation.
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_object("start",
                        "Start",
                        "Optional starting value for cumulative operation computation",
                        GARROW_TYPE_SCALAR,
                        static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_CUMULATIVE_OPTIONS_START, spec);

  /**
   * GArrowCumulativeOptions:skip-nulls:
   *
   * If true, nulls in the input are ignored and produce a corresponding null output.
   * When false, the first null encountered is propagated through the remaining output.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boolean(
    "skip-nulls",
    "Skip nulls",
    "If true, nulls in the input are ignored and produce a corresponding null output. "
    "When false, the first null encountered is propagated through the remaining output",
    options.skip_nulls,
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_CUMULATIVE_OPTIONS_SKIP_NULLS,
                                  spec);
}

/**
 * garrow_cumulative_options_new:
 *
 * Returns: A newly created #GArrowCumulativeOptions.
 *
 * Since: 23.0.0
 */
GArrowCumulativeOptions *
garrow_cumulative_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_CUMULATIVE_OPTIONS, NULL);
  return GARROW_CUMULATIVE_OPTIONS(options);
}

enum {
  PROP_DICTIONARY_ENCODE_OPTIONS_NULL_ENCODING_BEHAVIOR = 1,
};

G_DEFINE_TYPE(GArrowDictionaryEncodeOptions,
              garrow_dictionary_encode_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_dictionary_encode_options_set_property(GObject *object,
                                              guint prop_id,
                                              const GValue *value,
                                              GParamSpec *pspec)
{
  auto options =
    garrow_dictionary_encode_options_get_raw(GARROW_DICTIONARY_ENCODE_OPTIONS(object));

  switch (prop_id) {
  case PROP_DICTIONARY_ENCODE_OPTIONS_NULL_ENCODING_BEHAVIOR:
    options->null_encoding_behavior =
      static_cast<arrow::compute::DictionaryEncodeOptions::NullEncodingBehavior>(
        g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_dictionary_encode_options_get_property(GObject *object,
                                              guint prop_id,
                                              GValue *value,
                                              GParamSpec *pspec)
{
  auto options =
    garrow_dictionary_encode_options_get_raw(GARROW_DICTIONARY_ENCODE_OPTIONS(object));

  switch (prop_id) {
  case PROP_DICTIONARY_ENCODE_OPTIONS_NULL_ENCODING_BEHAVIOR:
    g_value_set_enum(value,
                     static_cast<GArrowDictionaryEncodeNullEncodingBehavior>(
                       options->null_encoding_behavior));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_dictionary_encode_options_init(GArrowDictionaryEncodeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::DictionaryEncodeOptions());
}

static void
garrow_dictionary_encode_options_class_init(GArrowDictionaryEncodeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_dictionary_encode_options_set_property;
  gobject_class->get_property = garrow_dictionary_encode_options_get_property;

  arrow::compute::DictionaryEncodeOptions options;

  GParamSpec *spec;
  /**
   * GArrowDictionaryEncodeOptions:null-encoding-behavior:
   *
   * How null values will be encoded.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_enum("null-encoding-behavior",
                           "Null encoding behavior",
                           "How null values will be encoded",
                           GARROW_TYPE_DICTIONARY_ENCODE_NULL_ENCODING_BEHAVIOR,
                           static_cast<GArrowDictionaryEncodeNullEncodingBehavior>(
                             options.null_encoding_behavior),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_DICTIONARY_ENCODE_OPTIONS_NULL_ENCODING_BEHAVIOR,
                                  spec);
}

/**
 * garrow_dictionary_encode_options_new:
 *
 * Returns: A newly created #GArrowDictionaryEncodeOptions.
 *
 * Since: 23.0.0
 */
GArrowDictionaryEncodeOptions *
garrow_dictionary_encode_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_DICTIONARY_ENCODE_OPTIONS, NULL);
  return GARROW_DICTIONARY_ENCODE_OPTIONS(options);
}

enum {
  PROP_ELEMENT_WISE_AGGREGATE_OPTIONS_SKIP_NULLS = 1,
};

G_DEFINE_TYPE(GArrowElementWiseAggregateOptions,
              garrow_element_wise_aggregate_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_element_wise_aggregate_options_set_property(GObject *object,
                                                   guint prop_id,
                                                   const GValue *value,
                                                   GParamSpec *pspec)
{
  auto options = garrow_element_wise_aggregate_options_get_raw(
    GARROW_ELEMENT_WISE_AGGREGATE_OPTIONS(object));

  switch (prop_id) {
  case PROP_ELEMENT_WISE_AGGREGATE_OPTIONS_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_element_wise_aggregate_options_get_property(GObject *object,
                                                   guint prop_id,
                                                   GValue *value,
                                                   GParamSpec *pspec)
{
  auto options = garrow_element_wise_aggregate_options_get_raw(
    GARROW_ELEMENT_WISE_AGGREGATE_OPTIONS(object));

  switch (prop_id) {
  case PROP_ELEMENT_WISE_AGGREGATE_OPTIONS_SKIP_NULLS:
    g_value_set_boolean(value, options->skip_nulls);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_element_wise_aggregate_options_init(GArrowElementWiseAggregateOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ElementWiseAggregateOptions());
}

static void
garrow_element_wise_aggregate_options_class_init(
  GArrowElementWiseAggregateOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_element_wise_aggregate_options_set_property;
  gobject_class->get_property = garrow_element_wise_aggregate_options_get_property;

  arrow::compute::ElementWiseAggregateOptions options;

  GParamSpec *spec;
  /**
   * GArrowElementWiseAggregateOptions:skip-nulls:
   *
   * Whether to skip (ignore) nulls in the input.
   * If false, any null in the input forces the output to null.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boolean("skip-nulls",
                              "Skip nulls",
                              "Whether to skip (ignore) nulls in the input. If false, "
                              "any null in the input forces the output to null",
                              options.skip_nulls,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ELEMENT_WISE_AGGREGATE_OPTIONS_SKIP_NULLS,
                                  spec);
}

/**
 * garrow_element_wise_aggregate_options_new:
 *
 * Returns: A newly created #GArrowElementWiseAggregateOptions.
 *
 * Since: 23.0.0
 */
GArrowElementWiseAggregateOptions *
garrow_element_wise_aggregate_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_ELEMENT_WISE_AGGREGATE_OPTIONS, NULL);
  return GARROW_ELEMENT_WISE_AGGREGATE_OPTIONS(options);
}

enum {
  PROP_DAY_OF_WEEK_OPTIONS_COUNT_FROM_ZERO = 1,
  PROP_DAY_OF_WEEK_OPTIONS_WEEK_START,
};

G_DEFINE_TYPE(GArrowDayOfWeekOptions,
              garrow_day_of_week_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_day_of_week_options_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto options = garrow_day_of_week_options_get_raw(GARROW_DAY_OF_WEEK_OPTIONS(object));

  switch (prop_id) {
  case PROP_DAY_OF_WEEK_OPTIONS_COUNT_FROM_ZERO:
    options->count_from_zero = g_value_get_boolean(value);
    break;
  case PROP_DAY_OF_WEEK_OPTIONS_WEEK_START:
    options->week_start = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_day_of_week_options_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto options = garrow_day_of_week_options_get_raw(GARROW_DAY_OF_WEEK_OPTIONS(object));

  switch (prop_id) {
  case PROP_DAY_OF_WEEK_OPTIONS_COUNT_FROM_ZERO:
    g_value_set_boolean(value, options->count_from_zero);
    break;
  case PROP_DAY_OF_WEEK_OPTIONS_WEEK_START:
    g_value_set_uint(value, options->week_start);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_day_of_week_options_init(GArrowDayOfWeekOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::DayOfWeekOptions());
}

static void
garrow_day_of_week_options_class_init(GArrowDayOfWeekOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_day_of_week_options_set_property;
  gobject_class->get_property = garrow_day_of_week_options_get_property;

  arrow::compute::DayOfWeekOptions options;

  GParamSpec *spec;
  /**
   * GArrowDayOfWeekOptions:count-from-zero:
   *
   * Number days from 0 if true and from 1 if false.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boolean("count-from-zero",
                              "Count from zero",
                              "Number days from 0 if true and from 1 if false",
                              options.count_from_zero,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_DAY_OF_WEEK_OPTIONS_COUNT_FROM_ZERO,
                                  spec);

  /**
   * GArrowDayOfWeekOptions:week-start:
   *
   * What day does the week start with (Monday=1, Sunday=7).
   * The numbering is unaffected by the count_from_zero parameter.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_uint("week-start",
                           "Week start",
                           "What day does the week start with (Monday=1, Sunday=7). The "
                           "numbering is unaffected by the count_from_zero parameter",
                           1,
                           7,
                           options.week_start,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_DAY_OF_WEEK_OPTIONS_WEEK_START,
                                  spec);
}

/**
 * garrow_day_of_week_options_new:
 *
 * Returns: A newly created #GArrowDayOfWeekOptions.
 *
 * Since: 23.0.0
 */
GArrowDayOfWeekOptions *
garrow_day_of_week_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_DAY_OF_WEEK_OPTIONS, NULL);
  return GARROW_DAY_OF_WEEK_OPTIONS(options);
}

enum {
  PROP_EXTRACT_REGEX_OPTIONS_PATTERN = 1,
};

G_DEFINE_TYPE(GArrowExtractRegexOptions,
              garrow_extract_regex_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_extract_regex_options_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_extract_regex_options_get_raw(GARROW_EXTRACT_REGEX_OPTIONS(object));

  switch (prop_id) {
  case PROP_EXTRACT_REGEX_OPTIONS_PATTERN:
    options->pattern = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extract_regex_options_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_extract_regex_options_get_raw(GARROW_EXTRACT_REGEX_OPTIONS(object));

  switch (prop_id) {
  case PROP_EXTRACT_REGEX_OPTIONS_PATTERN:
    g_value_set_string(value, options->pattern.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extract_regex_options_init(GArrowExtractRegexOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ExtractRegexOptions());
}

static void
garrow_extract_regex_options_class_init(GArrowExtractRegexOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_extract_regex_options_set_property;
  gobject_class->get_property = garrow_extract_regex_options_get_property;

  arrow::compute::ExtractRegexOptions options;

  GParamSpec *spec;
  /**
   * GArrowExtractRegexOptions:pattern:
   *
   * Regular expression with named capture fields.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_string("pattern",
                             "Pattern",
                             "Regular expression with named capture fields",
                             options.pattern.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_EXTRACT_REGEX_OPTIONS_PATTERN,
                                  spec);
}

/**
 * garrow_extract_regex_options_new:
 *
 * Returns: A newly created #GArrowExtractRegexOptions.
 *
 * Since: 23.0.0
 */
GArrowExtractRegexOptions *
garrow_extract_regex_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_EXTRACT_REGEX_OPTIONS, NULL);
  return GARROW_EXTRACT_REGEX_OPTIONS(options);
}

enum {
  PROP_EXTRACT_REGEX_SPAN_OPTIONS_PATTERN = 1,
};

G_DEFINE_TYPE(GArrowExtractRegexSpanOptions,
              garrow_extract_regex_span_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_extract_regex_span_options_set_property(GObject *object,
                                               guint prop_id,
                                               const GValue *value,
                                               GParamSpec *pspec)
{
  auto options =
    garrow_extract_regex_span_options_get_raw(GARROW_EXTRACT_REGEX_SPAN_OPTIONS(object));

  switch (prop_id) {
  case PROP_EXTRACT_REGEX_SPAN_OPTIONS_PATTERN:
    options->pattern = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extract_regex_span_options_get_property(GObject *object,
                                               guint prop_id,
                                               GValue *value,
                                               GParamSpec *pspec)
{
  auto options =
    garrow_extract_regex_span_options_get_raw(GARROW_EXTRACT_REGEX_SPAN_OPTIONS(object));

  switch (prop_id) {
  case PROP_EXTRACT_REGEX_SPAN_OPTIONS_PATTERN:
    g_value_set_string(value, options->pattern.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extract_regex_span_options_init(GArrowExtractRegexSpanOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ExtractRegexSpanOptions());
}

static void
garrow_extract_regex_span_options_class_init(GArrowExtractRegexSpanOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_extract_regex_span_options_set_property;
  gobject_class->get_property = garrow_extract_regex_span_options_get_property;

  arrow::compute::ExtractRegexSpanOptions options;

  GParamSpec *spec;
  /**
   * GArrowExtractRegexSpanOptions:pattern:
   *
   * Regular expression with named capture fields.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_string("pattern",
                             "Pattern",
                             "Regular expression with named capture fields",
                             options.pattern.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_EXTRACT_REGEX_SPAN_OPTIONS_PATTERN,
                                  spec);
}

/**
 * garrow_extract_regex_span_options_new:
 *
 * Returns: A newly created #GArrowExtractRegexSpanOptions.
 *
 * Since: 23.0.0
 */
GArrowExtractRegexSpanOptions *
garrow_extract_regex_span_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_EXTRACT_REGEX_SPAN_OPTIONS, NULL);
  return GARROW_EXTRACT_REGEX_SPAN_OPTIONS(options);
}

enum {
  PROP_JOIN_OPTIONS_NULL_HANDLING = 1,
  PROP_JOIN_OPTIONS_NULL_REPLACEMENT,
};

G_DEFINE_TYPE(GArrowJoinOptions, garrow_join_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_join_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_join_options_get_raw(GARROW_JOIN_OPTIONS(object));

  switch (prop_id) {
  case PROP_JOIN_OPTIONS_NULL_HANDLING:
    options->null_handling =
      static_cast<arrow::compute::JoinOptions::NullHandlingBehavior>(
        g_value_get_enum(value));
    break;
  case PROP_JOIN_OPTIONS_NULL_REPLACEMENT:
    options->null_replacement = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_join_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_join_options_get_raw(GARROW_JOIN_OPTIONS(object));

  switch (prop_id) {
  case PROP_JOIN_OPTIONS_NULL_HANDLING:
    g_value_set_enum(value,
                     static_cast<GArrowJoinNullHandlingBehavior>(options->null_handling));
    break;
  case PROP_JOIN_OPTIONS_NULL_REPLACEMENT:
    g_value_set_string(value, options->null_replacement.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_join_options_init(GArrowJoinOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::JoinOptions());
}

static void
garrow_join_options_class_init(GArrowJoinOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_join_options_set_property;
  gobject_class->get_property = garrow_join_options_get_property;

  arrow::compute::JoinOptions options;

  GParamSpec *spec;
  /**
   * GArrowJoinOptions:null-handling:
   *
   * How to handle null values. (A null separator always results in a null output.)
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_enum("null-handling",
                      "Null handling",
                      "How to handle null values",
                      GARROW_TYPE_JOIN_NULL_HANDLING_BEHAVIOR,
                      static_cast<GArrowJoinNullHandlingBehavior>(options.null_handling),
                      static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_JOIN_OPTIONS_NULL_HANDLING, spec);

  /**
   * GArrowJoinOptions:null-replacement:
   *
   * Replacement string for null values when null-handling is REPLACE.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_string(
    "null-replacement",
    "Null replacement",
    "Replacement string for null values when null-handling is REPLACE",
    options.null_replacement.c_str(),
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_JOIN_OPTIONS_NULL_REPLACEMENT,
                                  spec);
}

/**
 * garrow_join_options_new:
 *
 * Returns: A newly created #GArrowJoinOptions.
 *
 * Since: 23.0.0
 */
GArrowJoinOptions *
garrow_join_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_JOIN_OPTIONS, nullptr);
  return GARROW_JOIN_OPTIONS(options);
}

enum {
  PROP_LIST_FLATTEN_OPTIONS_RECURSIVE = 1,
};

G_DEFINE_TYPE(GArrowListFlattenOptions,
              garrow_list_flatten_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_list_flatten_options_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto options = garrow_list_flatten_options_get_raw(GARROW_LIST_FLATTEN_OPTIONS(object));

  switch (prop_id) {
  case PROP_LIST_FLATTEN_OPTIONS_RECURSIVE:
    options->recursive = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_list_flatten_options_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto options = garrow_list_flatten_options_get_raw(GARROW_LIST_FLATTEN_OPTIONS(object));

  switch (prop_id) {
  case PROP_LIST_FLATTEN_OPTIONS_RECURSIVE:
    g_value_set_boolean(value, options->recursive);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_list_flatten_options_init(GArrowListFlattenOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ListFlattenOptions());
}

static void
garrow_list_flatten_options_class_init(GArrowListFlattenOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_list_flatten_options_set_property;
  gobject_class->get_property = garrow_list_flatten_options_get_property;

  arrow::compute::ListFlattenOptions options;

  GParamSpec *spec;
  /**
   * GArrowListFlattenOptions:recursive:
   *
   * If true, the list is flattened recursively until a non-list array is formed.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boolean(
    "recursive",
    "Recursive",
    "If true, the list is flattened recursively until a non-list array is formed",
    options.recursive,
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_LIST_FLATTEN_OPTIONS_RECURSIVE,
                                  spec);
}

/**
 * garrow_list_flatten_options_new:
 *
 * Returns: A newly created #GArrowListFlattenOptions.
 *
 * Since: 23.0.0
 */
GArrowListFlattenOptions *
garrow_list_flatten_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_LIST_FLATTEN_OPTIONS, NULL);
  return GARROW_LIST_FLATTEN_OPTIONS(options);
}

typedef struct GArrowMapLookupOptionsPrivate_
{
  GArrowScalar *query_key;
} GArrowMapLookupOptionsPrivate;

enum {
  PROP_MAP_LOOKUP_OPTIONS_QUERY_KEY = 1,
  PROP_MAP_LOOKUP_OPTIONS_OCCURRENCE,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowMapLookupOptions,
                           garrow_map_lookup_options,
                           GARROW_TYPE_FUNCTION_OPTIONS)

#define GARROW_MAP_LOOKUP_OPTIONS_GET_PRIVATE(object)                                    \
  static_cast<GArrowMapLookupOptionsPrivate *>(                                          \
    garrow_map_lookup_options_get_instance_private(GARROW_MAP_LOOKUP_OPTIONS(object)))

static void
garrow_map_lookup_options_dispose(GObject *object)
{
  auto priv = GARROW_MAP_LOOKUP_OPTIONS_GET_PRIVATE(object);

  if (priv->query_key) {
    g_object_unref(priv->query_key);
    priv->query_key = NULL;
  }

  G_OBJECT_CLASS(garrow_map_lookup_options_parent_class)->dispose(object);
}

static void
garrow_map_lookup_options_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_MAP_LOOKUP_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_map_lookup_options_get_raw(GARROW_MAP_LOOKUP_OPTIONS(object));

  switch (prop_id) {
  case PROP_MAP_LOOKUP_OPTIONS_QUERY_KEY:
    {
      auto query_key = g_value_get_object(value);
      if (priv->query_key != query_key) {
        if (priv->query_key) {
          g_object_unref(priv->query_key);
        }
        priv->query_key = GARROW_SCALAR(query_key);
        if (priv->query_key) {
          g_object_ref(priv->query_key);
          options->query_key = garrow_scalar_get_raw(priv->query_key);
        } else {
          options->query_key = nullptr;
        }
      }
    }
    break;
  case PROP_MAP_LOOKUP_OPTIONS_OCCURRENCE:
    options->occurrence =
      static_cast<arrow::compute::MapLookupOptions::Occurrence>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_map_lookup_options_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_MAP_LOOKUP_OPTIONS_GET_PRIVATE(object);
  auto options = garrow_map_lookup_options_get_raw(GARROW_MAP_LOOKUP_OPTIONS(object));

  switch (prop_id) {
  case PROP_MAP_LOOKUP_OPTIONS_QUERY_KEY:
    g_value_set_object(value, priv->query_key);
    break;
  case PROP_MAP_LOOKUP_OPTIONS_OCCURRENCE:
    g_value_set_enum(value, static_cast<GArrowMapLookupOccurrence>(options->occurrence));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_map_lookup_options_init(GArrowMapLookupOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::MapLookupOptions());
}

static void
garrow_map_lookup_options_class_init(GArrowMapLookupOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_map_lookup_options_dispose;
  gobject_class->set_property = garrow_map_lookup_options_set_property;
  gobject_class->get_property = garrow_map_lookup_options_get_property;

  arrow::compute::MapLookupOptions options;

  GParamSpec *spec;
  /**
   * GArrowMapLookupOptions:query-key:
   *
   * The key to lookup in the map.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_object("query-key",
                             "Query key",
                             "The key to lookup in the map",
                             GARROW_TYPE_SCALAR,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MAP_LOOKUP_OPTIONS_QUERY_KEY, spec);

  /**
   * GArrowMapLookupOptions:occurrence:
   *
   * Whether to return the first, last, or all matching values.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_enum("occurrence",
                           "Occurrence",
                           "Whether to return the first, last, or all matching values",
                           GARROW_TYPE_MAP_LOOKUP_OCCURRENCE,
                           static_cast<GArrowMapLookupOccurrence>(options.occurrence),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_MAP_LOOKUP_OPTIONS_OCCURRENCE,
                                  spec);
}

/**
 * garrow_map_lookup_options_new:
 * @query_key: (nullable): A #GArrowScalar to be looked up.
 * @occurrence: A #GArrowMapLookupOccurrence.
 *
 * Returns: A newly created #GArrowMapLookupOptions.
 *
 * Since: 23.0.0
 */
GArrowMapLookupOptions *
garrow_map_lookup_options_new(GArrowScalar *query_key,
                              GArrowMapLookupOccurrence occurrence)
{
  return GARROW_MAP_LOOKUP_OPTIONS(g_object_new(GARROW_TYPE_MAP_LOOKUP_OPTIONS,
                                                "query-key",
                                                query_key,
                                                "occurrence",
                                                occurrence,
                                                NULL));
}

enum {
  PROP_LIST_SLICE_OPTIONS_START = 1,
  PROP_LIST_SLICE_OPTIONS_STOP,
  PROP_LIST_SLICE_OPTIONS_STEP,
  PROP_LIST_SLICE_OPTIONS_RETURN_FIXED_SIZE_LIST,
};

G_DEFINE_TYPE(GArrowListSliceOptions,
              garrow_list_slice_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_list_slice_options_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto options = garrow_list_slice_options_get_raw(GARROW_LIST_SLICE_OPTIONS(object));

  switch (prop_id) {
  case PROP_LIST_SLICE_OPTIONS_START:
    options->start = g_value_get_int64(value);
    break;
  case PROP_LIST_SLICE_OPTIONS_STOP:
    {
      auto stop_value = g_value_get_int64(value);
      if (stop_value == GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED) {
        options->stop = std::nullopt;
      } else {
        options->stop = stop_value;
      }
    }
    break;
  case PROP_LIST_SLICE_OPTIONS_STEP:
    options->step = g_value_get_int64(value);
    break;
  case PROP_LIST_SLICE_OPTIONS_RETURN_FIXED_SIZE_LIST:
    {
      auto return_fixed_size_list_value =
        static_cast<GArrowListSliceReturnFixedSizeList>(g_value_get_enum(value));
      switch (return_fixed_size_list_value) {
      case GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_AUTO:
        options->return_fixed_size_list = std::nullopt;
        break;
      case GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_FALSE:
        options->return_fixed_size_list = false;
        break;
      case GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_TRUE:
        options->return_fixed_size_list = true;
        break;
      default:
        options->return_fixed_size_list = std::nullopt;
        break;
      }
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_list_slice_options_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto options = garrow_list_slice_options_get_raw(GARROW_LIST_SLICE_OPTIONS(object));

  switch (prop_id) {
  case PROP_LIST_SLICE_OPTIONS_START:
    g_value_set_int64(value, options->start);
    break;
  case PROP_LIST_SLICE_OPTIONS_STOP:
    if (options->stop.has_value()) {
      g_value_set_int64(value, options->stop.value());
    } else {
      g_value_set_int64(value, GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED);
    }
    break;
  case PROP_LIST_SLICE_OPTIONS_STEP:
    g_value_set_int64(value, options->step);
    break;
  case PROP_LIST_SLICE_OPTIONS_RETURN_FIXED_SIZE_LIST:
    if (options->return_fixed_size_list.has_value()) {
      if (options->return_fixed_size_list.value()) {
        g_value_set_enum(value, GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_TRUE);
      } else {
        g_value_set_enum(value, GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_FALSE);
      }
    } else {
      // When not set (nullopt), return AUTO (default)
      g_value_set_enum(value, GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_AUTO);
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_list_slice_options_init(GArrowListSliceOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ListSliceOptions());
}

static void
garrow_list_slice_options_class_init(GArrowListSliceOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_list_slice_options_set_property;
  gobject_class->get_property = garrow_list_slice_options_get_property;

  arrow::compute::ListSliceOptions options;

  GParamSpec *spec;
  /**
   * GArrowListSliceOptions:start:
   *
   * The start of list slicing.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64("start",
                            "Start",
                            "The start of list slicing",
                            G_MININT64,
                            G_MAXINT64,
                            options.start,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_LIST_SLICE_OPTIONS_START, spec);

  /**
   * GArrowListSliceOptions:stop:
   *
   * Optional stop of list slicing. If not set (value is
   * %GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED), then slice to end.
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_int64("stop",
                       "Stop",
                       "Optional stop of list slicing. If not set (value is "
                       "GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED), then slice to end",
                       GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED,
                       G_MAXINT64,
                       GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED,
                       static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_LIST_SLICE_OPTIONS_STOP, spec);

  /**
   * GArrowListSliceOptions:step:
   *
   * Slicing step.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64("step",
                            "Step",
                            "Slicing step",
                            G_MININT64,
                            G_MAXINT64,
                            options.step,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_LIST_SLICE_OPTIONS_STEP, spec);

  /**
   * GArrowListSliceOptions:return-fixed-size-list:
   *
   * Whether to return a FixedSizeListArray. If
   * #GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_TRUE and stop is after a list element's
   * length, nulls will be appended to create the requested slice size. If
   * #GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_AUTO (default), will return whatever type
   * it got in.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_enum(
    "return-fixed-size-list",
    "Return fixed size list",
    "Whether to return a FixedSizeListArray. If TRUE and stop is after a list element's "
    "length, nulls will be appended to create the requested slice size. If AUTO "
    "(default), will return whatever type it got in",
    GARROW_TYPE_LIST_SLICE_RETURN_FIXED_SIZE_LIST,
    GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_AUTO,
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_LIST_SLICE_OPTIONS_RETURN_FIXED_SIZE_LIST,
                                  spec);
}

/**
 * garrow_list_slice_options_new:
 *
 * Returns: A newly created #GArrowListSliceOptions.
 *
 * Since: 23.0.0
 */
GArrowListSliceOptions *
garrow_list_slice_options_new(void)
{
  auto options = g_object_new(GARROW_TYPE_LIST_SLICE_OPTIONS, nullptr);
  return GARROW_LIST_SLICE_OPTIONS(options);
}

enum {
  PROP_MODE_OPTIONS_N = 1,
  PROP_MODE_OPTIONS_SKIP_NULLS,
  PROP_MODE_OPTIONS_MIN_COUNT,
};

G_DEFINE_TYPE(GArrowModeOptions, garrow_mode_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_mode_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_mode_options_get_raw(GARROW_MODE_OPTIONS(object));

  switch (prop_id) {
  case PROP_MODE_OPTIONS_N:
    options->n = g_value_get_int64(value);
    break;
  case PROP_MODE_OPTIONS_SKIP_NULLS:
    options->skip_nulls = g_value_get_boolean(value);
    break;
  case PROP_MODE_OPTIONS_MIN_COUNT:
    options->min_count = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_mode_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_mode_options_get_raw(GARROW_MODE_OPTIONS(object));

  switch (prop_id) {
  case PROP_MODE_OPTIONS_N:
    g_value_set_int64(value, options->n);
    break;
  case PROP_MODE_OPTIONS_SKIP_NULLS:
    g_value_set_boolean(value, options->skip_nulls);
    break;
  case PROP_MODE_OPTIONS_MIN_COUNT:
    g_value_set_uint(value, options->min_count);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_mode_options_init(GArrowModeOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::ModeOptions());
}

static void
garrow_mode_options_class_init(GArrowModeOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_mode_options_set_property;
  gobject_class->get_property = garrow_mode_options_get_property;

  arrow::compute::ModeOptions options;

  GParamSpec *spec;
  /**
   * GArrowModeOptions:n:
   *
   * Number of distinct most-common values to return.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64("n",
                            "N",
                            "Number of distinct most-common values to return",
                            1,
                            G_MAXINT64,
                            options.n,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MODE_OPTIONS_N, spec);

  /**
   * GArrowModeOptions:skip-nulls:
   *
   * Whether NULLs are skipped or not.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boolean("skip-nulls",
                              "Skip NULLs",
                              "Whether NULLs are skipped or not",
                              options.skip_nulls,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MODE_OPTIONS_SKIP_NULLS, spec);

  /**
   * GArrowModeOptions:min-count:
   *
   * If less than this many non-null values are observed, emit null.
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_uint("min-count",
                      "Min count",
                      "If less than this many non-null values are observed, emit null",
                      0,
                      G_MAXUINT,
                      options.min_count,
                      static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MODE_OPTIONS_MIN_COUNT, spec);
}

/**
 * garrow_mode_options_new:
 *
 * Returns: A newly created #GArrowModeOptions.
 *
 * Since: 23.0.0
 */
GArrowModeOptions *
garrow_mode_options_new(void)
{
  return GARROW_MODE_OPTIONS(g_object_new(GARROW_TYPE_MODE_OPTIONS, NULL));
}

enum {
  PROP_NULL_OPTIONS_NAN_IS_NULL = 1,
};

G_DEFINE_TYPE(GArrowNullOptions, garrow_null_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_null_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_null_options_get_raw(GARROW_NULL_OPTIONS(object));

  switch (prop_id) {
  case PROP_NULL_OPTIONS_NAN_IS_NULL:
    options->nan_is_null = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_null_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto options = garrow_null_options_get_raw(GARROW_NULL_OPTIONS(object));

  switch (prop_id) {
  case PROP_NULL_OPTIONS_NAN_IS_NULL:
    g_value_set_boolean(value, options->nan_is_null);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_null_options_init(GArrowNullOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::NullOptions());
}

static void
garrow_null_options_class_init(GArrowNullOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_null_options_set_property;
  gobject_class->get_property = garrow_null_options_get_property;

  arrow::compute::NullOptions options;

  GParamSpec *spec;
  /**
   * GArrowNullOptions:nan-is-null:
   *
   * Whether floating-point NaN values are considered null.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boolean("nan-is-null",
                              "NaN is null",
                              "Whether floating-point NaN values are considered null",
                              options.nan_is_null,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_NULL_OPTIONS_NAN_IS_NULL, spec);
}

/**
 * garrow_null_options_new:
 *
 * Returns: A newly created #GArrowNullOptions.
 *
 * Since: 23.0.0
 */
GArrowNullOptions *
garrow_null_options_new(void)
{
  return GARROW_NULL_OPTIONS(g_object_new(GARROW_TYPE_NULL_OPTIONS, NULL));
}

enum {
  PROP_PAD_OPTIONS_WIDTH = 1,
  PROP_PAD_OPTIONS_PADDING,
  PROP_PAD_OPTIONS_LEAN_LEFT_ON_ODD_PADDING,
};

G_DEFINE_TYPE(GArrowPadOptions, garrow_pad_options, GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_pad_options_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto options = garrow_pad_options_get_raw(GARROW_PAD_OPTIONS(object));

  switch (prop_id) {
  case PROP_PAD_OPTIONS_WIDTH:
    options->width = g_value_get_int64(value);
    break;
  case PROP_PAD_OPTIONS_PADDING:
    options->padding = g_value_get_string(value);
    break;
  case PROP_PAD_OPTIONS_LEAN_LEFT_ON_ODD_PADDING:
    options->lean_left_on_odd_padding = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_pad_options_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto options = garrow_pad_options_get_raw(GARROW_PAD_OPTIONS(object));

  switch (prop_id) {
  case PROP_PAD_OPTIONS_WIDTH:
    g_value_set_int64(value, options->width);
    break;
  case PROP_PAD_OPTIONS_PADDING:
    g_value_set_string(value, options->padding.c_str());
    break;
  case PROP_PAD_OPTIONS_LEAN_LEFT_ON_ODD_PADDING:
    g_value_set_boolean(value, options->lean_left_on_odd_padding);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_pad_options_init(GArrowPadOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::PadOptions());
}

static void
garrow_pad_options_class_init(GArrowPadOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_pad_options_set_property;
  gobject_class->get_property = garrow_pad_options_get_property;

  arrow::compute::PadOptions options;

  GParamSpec *spec;
  /**
   * GArrowPadOptions:width:
   *
   * The desired string length.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64("width",
                            "Width",
                            "The desired string length",
                            0,
                            G_MAXINT64,
                            options.width,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PAD_OPTIONS_WIDTH, spec);

  /**
   * GArrowPadOptions:padding:
   *
   * What to pad the string with. Should be one codepoint (Unicode)/byte (ASCII).
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_string(
    "padding",
    "Padding",
    "What to pad the string with. Should be one codepoint (Unicode)/byte (ASCII)",
    options.padding.c_str(),
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PAD_OPTIONS_PADDING, spec);

  /**
   * GArrowPadOptions:lean-left-on-odd-padding:
   *
   * What to do if there is an odd number of padding characters (in case of centered
   * padding). Defaults to aligning on the left (i.e. adding the extra padding character
   * on the right).
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_boolean("lean-left-on-odd-padding",
                         "Lean left on odd padding",
                         "What to do if there is an odd number of padding characters (in "
                         "case of centered padding). Defaults to aligning on the left "
                         "(i.e. adding the extra padding character on the right)",
                         options.lean_left_on_odd_padding,
                         static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_PAD_OPTIONS_LEAN_LEFT_ON_ODD_PADDING,
                                  spec);
}

/**
 * garrow_pad_options_new:
 *
 * Returns: A newly created #GArrowPadOptions.
 *
 * Since: 23.0.0
 */
GArrowPadOptions *
garrow_pad_options_new(void)
{
  return GARROW_PAD_OPTIONS(g_object_new(GARROW_TYPE_PAD_OPTIONS, NULL));
}

enum {
  PROP_PAIRWISE_OPTIONS_PERIODS = 1,
};

G_DEFINE_TYPE(GArrowPairwiseOptions,
              garrow_pairwise_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_pairwise_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_pairwise_options_get_raw(GARROW_PAIRWISE_OPTIONS(object));

  switch (prop_id) {
  case PROP_PAIRWISE_OPTIONS_PERIODS:
    options->periods = g_value_get_int64(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_pairwise_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto options = garrow_pairwise_options_get_raw(GARROW_PAIRWISE_OPTIONS(object));

  switch (prop_id) {
  case PROP_PAIRWISE_OPTIONS_PERIODS:
    g_value_set_int64(value, options->periods);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_pairwise_options_init(GArrowPairwiseOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options =
    static_cast<arrow::compute::FunctionOptions *>(new arrow::compute::PairwiseOptions());
}

static void
garrow_pairwise_options_class_init(GArrowPairwiseOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_pairwise_options_set_property;
  gobject_class->get_property = garrow_pairwise_options_get_property;

  arrow::compute::PairwiseOptions options;

  GParamSpec *spec;
  /**
   * GArrowPairwiseOptions:periods:
   *
   * Periods to shift for applying the binary operation, accepts negative values.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64(
    "periods",
    "Periods",
    "Periods to shift for applying the binary operation, accepts negative values",
    G_MININT64,
    G_MAXINT64,
    options.periods,
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PAIRWISE_OPTIONS_PERIODS, spec);
}

/**
 * garrow_pairwise_options_new:
 *
 * Returns: A newly created #GArrowPairwiseOptions.
 *
 * Since: 23.0.0
 */
GArrowPairwiseOptions *
garrow_pairwise_options_new(void)
{
  return GARROW_PAIRWISE_OPTIONS(g_object_new(GARROW_TYPE_PAIRWISE_OPTIONS, NULL));
}

enum {
  PROP_REPLACE_SLICE_OPTIONS_START = 1,
  PROP_REPLACE_SLICE_OPTIONS_STOP,
  PROP_REPLACE_SLICE_OPTIONS_REPLACEMENT,
};

G_DEFINE_TYPE(GArrowReplaceSliceOptions,
              garrow_replace_slice_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_replace_slice_options_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_replace_slice_options_get_raw(GARROW_REPLACE_SLICE_OPTIONS(object));

  switch (prop_id) {
  case PROP_REPLACE_SLICE_OPTIONS_START:
    options->start = g_value_get_int64(value);
    break;
  case PROP_REPLACE_SLICE_OPTIONS_STOP:
    options->stop = g_value_get_int64(value);
    break;
  case PROP_REPLACE_SLICE_OPTIONS_REPLACEMENT:
    options->replacement = g_value_get_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_replace_slice_options_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_replace_slice_options_get_raw(GARROW_REPLACE_SLICE_OPTIONS(object));

  switch (prop_id) {
  case PROP_REPLACE_SLICE_OPTIONS_START:
    g_value_set_int64(value, options->start);
    break;
  case PROP_REPLACE_SLICE_OPTIONS_STOP:
    g_value_set_int64(value, options->stop);
    break;
  case PROP_REPLACE_SLICE_OPTIONS_REPLACEMENT:
    g_value_set_string(value, options->replacement.c_str());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_replace_slice_options_init(GArrowReplaceSliceOptions *object)
{
  auto arrow_priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  arrow_priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::ReplaceSliceOptions());
}

static void
garrow_replace_slice_options_class_init(GArrowReplaceSliceOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_replace_slice_options_set_property;
  gobject_class->get_property = garrow_replace_slice_options_get_property;

  arrow::compute::ReplaceSliceOptions options;

  GParamSpec *spec;
  /**
   * GArrowReplaceSliceOptions:start:
   *
   * Index to start slicing at.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64("start",
                            "Start",
                            "Index to start slicing at",
                            G_MININT64,
                            G_MAXINT64,
                            options.start,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_REPLACE_SLICE_OPTIONS_START, spec);

  /**
   * GArrowReplaceSliceOptions:stop:
   *
   * Index to stop slicing at.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64("stop",
                            "Stop",
                            "Index to stop slicing at",
                            G_MININT64,
                            G_MAXINT64,
                            options.stop,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_REPLACE_SLICE_OPTIONS_STOP, spec);

  /**
   * GArrowReplaceSliceOptions:replacement:
   *
   * String to replace the slice with.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_string("replacement",
                             "Replacement",
                             "String to replace the slice with",
                             options.replacement.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_REPLACE_SLICE_OPTIONS_REPLACEMENT,
                                  spec);
}

/**
 * garrow_replace_slice_options_new:
 *
 * Returns: A newly created #GArrowReplaceSliceOptions.
 *
 * Since: 23.0.0
 */
GArrowReplaceSliceOptions *
garrow_replace_slice_options_new(void)
{
  return GARROW_REPLACE_SLICE_OPTIONS(
    g_object_new(GARROW_TYPE_REPLACE_SLICE_OPTIONS, nullptr));
}

enum {
  PROP_PARTITION_NTH_OPTIONS_PIVOT = 1,
  PROP_PARTITION_NTH_OPTIONS_NULL_PLACEMENT,
};

G_DEFINE_TYPE(GArrowPartitionNthOptions,
              garrow_partition_nth_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_partition_nth_options_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_partition_nth_options_get_raw(GARROW_PARTITION_NTH_OPTIONS(object));

  switch (prop_id) {
  case PROP_PARTITION_NTH_OPTIONS_PIVOT:
    options->pivot = g_value_get_int64(value);
    break;
  case PROP_PARTITION_NTH_OPTIONS_NULL_PLACEMENT:
    options->null_placement =
      static_cast<arrow::compute::NullPlacement>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_partition_nth_options_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_partition_nth_options_get_raw(GARROW_PARTITION_NTH_OPTIONS(object));

  switch (prop_id) {
  case PROP_PARTITION_NTH_OPTIONS_PIVOT:
    g_value_set_int64(value, options->pivot);
    break;
  case PROP_PARTITION_NTH_OPTIONS_NULL_PLACEMENT:
    g_value_set_enum(value, static_cast<GArrowNullPlacement>(options->null_placement));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_partition_nth_options_init(GArrowPartitionNthOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::PartitionNthOptions());
}

static void
garrow_partition_nth_options_class_init(GArrowPartitionNthOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_partition_nth_options_set_property;
  gobject_class->get_property = garrow_partition_nth_options_get_property;

  arrow::compute::PartitionNthOptions options;

  GParamSpec *spec;
  /**
   * GArrowPartitionNthOptions:pivot:
   *
   * The index into the equivalent sorted array of the partition pivot element.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_int64(
    "pivot",
    "Pivot",
    "The index into the equivalent sorted array of the partition pivot element",
    0,
    G_MAXINT64,
    options.pivot,
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_PARTITION_NTH_OPTIONS_PIVOT, spec);

  /**
   * GArrowPartitionNthOptions:null-placement:
   *
   * Whether nulls and NaNs are partitioned at the start or at the end.
   *
   * Since: 23.0.0
   */
  spec =
    g_param_spec_enum("null-placement",
                      "Null placement",
                      "Whether nulls and NaNs are partitioned at the start or at the end",
                      GARROW_TYPE_NULL_PLACEMENT,
                      static_cast<GArrowNullPlacement>(options.null_placement),
                      static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_PARTITION_NTH_OPTIONS_NULL_PLACEMENT,
                                  spec);
}

/**
 * garrow_partition_nth_options_new:
 *
 * Returns: A newly created #GArrowPartitionNthOptions.
 *
 * Since: 23.0.0
 */
GArrowPartitionNthOptions *
garrow_partition_nth_options_new(void)
{
  return GARROW_PARTITION_NTH_OPTIONS(
    g_object_new(GARROW_TYPE_PARTITION_NTH_OPTIONS, nullptr));
}

enum {
  PROP_PIVOT_WIDER_OPTIONS_KEY_NAMES = 1,
  PROP_PIVOT_WIDER_OPTIONS_UNEXPECTED_KEY_BEHAVIOR,
};

G_DEFINE_TYPE(GArrowPivotWiderOptions,
              garrow_pivot_wider_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_pivot_wider_options_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto options = garrow_pivot_wider_options_get_raw(GARROW_PIVOT_WIDER_OPTIONS(object));

  switch (prop_id) {
  case PROP_PIVOT_WIDER_OPTIONS_KEY_NAMES:
    {
      auto strv = static_cast<gchar **>(g_value_get_boxed(value));
      options->key_names.clear();
      if (strv) {
        for (gchar **p = strv; *p; ++p) {
          options->key_names.emplace_back(*p);
        }
      }
    }
    break;
  case PROP_PIVOT_WIDER_OPTIONS_UNEXPECTED_KEY_BEHAVIOR:
    options->unexpected_key_behavior =
      static_cast<arrow::compute::PivotWiderOptions::UnexpectedKeyBehavior>(
        g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_pivot_wider_options_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto options = garrow_pivot_wider_options_get_raw(GARROW_PIVOT_WIDER_OPTIONS(object));

  switch (prop_id) {
  case PROP_PIVOT_WIDER_OPTIONS_KEY_NAMES:
    {
      const auto &names = options->key_names;
      auto strv = static_cast<gchar **>(g_new0(gchar *, names.size() + 1));
      for (gsize i = 0; i < names.size(); ++i) {
        strv[i] = g_strdup(names[i].c_str());
      }
      g_value_take_boxed(value, strv);
    }
    break;
  case PROP_PIVOT_WIDER_OPTIONS_UNEXPECTED_KEY_BEHAVIOR:
    g_value_set_enum(value,
                     static_cast<GArrowPivotWiderUnexpectedKeyBehavior>(
                       options->unexpected_key_behavior));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_pivot_wider_options_init(GArrowPivotWiderOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::PivotWiderOptions());
}

static void
garrow_pivot_wider_options_class_init(GArrowPivotWiderOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_pivot_wider_options_set_property;
  gobject_class->get_property = garrow_pivot_wider_options_get_property;

  arrow::compute::PivotWiderOptions options;

  GParamSpec *spec;
  /**
   * GArrowPivotWiderOptions:key-names:
   *
   * The values expected in the pivot key column.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_boxed("key-names",
                            "Key names",
                            "The values expected in the pivot key column",
                            G_TYPE_STRV,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_PIVOT_WIDER_OPTIONS_KEY_NAMES,
                                  spec);

  /**
   * GArrowPivotWiderOptions:unexpected-key-behavior:
   *
   * The behavior when pivot keys not in key_names are encountered.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_enum(
    "unexpected-key-behavior",
    "Unexpected key behavior",
    "The behavior when pivot keys not in key_names are encountered",
    GARROW_TYPE_PIVOT_WIDER_UNEXPECTED_KEY_BEHAVIOR,
    static_cast<GArrowPivotWiderUnexpectedKeyBehavior>(options.unexpected_key_behavior),
    static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_PIVOT_WIDER_OPTIONS_UNEXPECTED_KEY_BEHAVIOR,
                                  spec);
}

/**
 * garrow_pivot_wider_options_new:
 *
 * Returns: A newly created #GArrowPivotWiderOptions.
 *
 * Since: 23.0.0
 */
GArrowPivotWiderOptions *
garrow_pivot_wider_options_new(void)
{
  return GARROW_PIVOT_WIDER_OPTIONS(
    g_object_new(GARROW_TYPE_PIVOT_WIDER_OPTIONS, nullptr));
}

enum {
  PROP_RANK_QUANTILE_OPTIONS_NULL_PLACEMENT = 1,
};

G_DEFINE_TYPE(GArrowRankQuantileOptions,
              garrow_rank_quantile_options,
              GARROW_TYPE_FUNCTION_OPTIONS)

static void
garrow_rank_quantile_options_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_rank_quantile_options_get_raw(GARROW_RANK_QUANTILE_OPTIONS(object));

  switch (prop_id) {
  case PROP_RANK_QUANTILE_OPTIONS_NULL_PLACEMENT:
    options->null_placement =
      static_cast<arrow::compute::NullPlacement>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_rank_quantile_options_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto options =
    garrow_rank_quantile_options_get_raw(GARROW_RANK_QUANTILE_OPTIONS(object));

  switch (prop_id) {
  case PROP_RANK_QUANTILE_OPTIONS_NULL_PLACEMENT:
    g_value_set_enum(value, static_cast<GArrowNullPlacement>(options->null_placement));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_rank_quantile_options_init(GArrowRankQuantileOptions *object)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(object);
  priv->options = static_cast<arrow::compute::FunctionOptions *>(
    new arrow::compute::RankQuantileOptions());
}

static void
garrow_rank_quantile_options_class_init(GArrowRankQuantileOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_rank_quantile_options_set_property;
  gobject_class->get_property = garrow_rank_quantile_options_get_property;

  auto options = arrow::compute::RankQuantileOptions::Defaults();

  GParamSpec *spec;
  /**
   * GArrowRankQuantileOptions:null-placement:
   *
   * Whether nulls and NaNs are placed at the start or at the end.
   *
   * Since: 23.0.0
   */
  spec = g_param_spec_enum("null-placement",
                           "Null placement",
                           "Whether nulls and NaNs are placed "
                           "at the start or at the end.",
                           GARROW_TYPE_NULL_PLACEMENT,
                           static_cast<GArrowNullPlacement>(options.null_placement),
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_RANK_QUANTILE_OPTIONS_NULL_PLACEMENT,
                                  spec);
}

/**
 * garrow_rank_quantile_options_new:
 *
 * Returns: A newly created #GArrowRankQuantileOptions.
 *
 * Since: 23.0.0
 */
GArrowRankQuantileOptions *
garrow_rank_quantile_options_new(void)
{
  return GARROW_RANK_QUANTILE_OPTIONS(
    g_object_new(GARROW_TYPE_RANK_QUANTILE_OPTIONS, nullptr));
}

/**
 * garrow_rank_quantile_options_get_sort_keys:
 * @options: A #GArrowRankQuantileOptions.
 *
 * Returns: (transfer full) (element-type GArrowSortKey):
 *   The sort keys to be used.
 *
 * Since: 23.0.0
 */
GList *
garrow_rank_quantile_options_get_sort_keys(GArrowRankQuantileOptions *options)
{
  auto arrow_options = garrow_rank_quantile_options_get_raw(options);
  return garrow_sort_keys_new_raw(arrow_options->sort_keys);
}

/**
 * garrow_rank_quantile_options_set_sort_keys:
 * @options: A #GArrowRankQuantileOptions.
 * @sort_keys: (element-type GArrowSortKey): The sort keys to be used.
 *
 * Set sort keys to be used.
 *
 * Since: 23.0.0
 */
void
garrow_rank_quantile_options_set_sort_keys(GArrowRankQuantileOptions *options,
                                           GList *sort_keys)
{
  auto arrow_options = garrow_rank_quantile_options_get_raw(options);
  garrow_raw_sort_keys_set(arrow_options->sort_keys, sort_keys);
}

/**
 * garrow_rank_quantile_options_add_sort_key:
 * @options: A #GArrowRankQuantileOptions.
 * @sort_key: The sort key to be added.
 *
 * Add a sort key to be used.
 *
 * Since: 23.0.0
 */
void
garrow_rank_quantile_options_add_sort_key(GArrowRankQuantileOptions *options,
                                          GArrowSortKey *sort_key)
{
  auto arrow_options = garrow_rank_quantile_options_get_raw(options);
  garrow_raw_sort_keys_add(arrow_options->sort_keys, sort_key);
}

G_END_DECLS

arrow::Result<arrow::FieldRef>
garrow_field_reference_resolve_raw(const gchar *reference)
{
  if (reference && (reference[0] == '.' || reference[0] == '[')) {
    return arrow::FieldRef::FromDotPath(reference);
  } else {
    arrow::FieldRef arrow_reference(reference);
    return arrow_reference;
  }
}

arrow::compute::ExecContext *
garrow_execute_context_get_raw(GArrowExecuteContext *context)
{
  auto priv = GARROW_EXECUTE_CONTEXT_GET_PRIVATE(context);
  return priv->context.get();
}

GArrowFunctionOptions *
garrow_function_options_new_raw(const arrow::compute::FunctionOptions *arrow_options)
{
  std::string_view arrow_type_name(arrow_options->type_name());
  if (arrow_type_name == "CastOptions") {
    auto arrow_cast_options =
      static_cast<const arrow::compute::CastOptions *>(arrow_options);
    auto options = garrow_cast_options_new_raw(arrow_cast_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ScalarAggregateOptions") {
    const auto arrow_scalar_aggregate_options =
      static_cast<const arrow::compute::ScalarAggregateOptions *>(arrow_options);
    auto options =
      garrow_scalar_aggregate_options_new_raw(arrow_scalar_aggregate_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "CountOptions") {
    const auto arrow_count_options =
      static_cast<const arrow::compute::CountOptions *>(arrow_options);
    auto options = garrow_count_options_new_raw(arrow_count_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "FilterOptions") {
    const auto arrow_filter_options =
      static_cast<const arrow::compute::FilterOptions *>(arrow_options);
    auto options = garrow_filter_options_new_raw(arrow_filter_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "TakeOptions") {
    const auto arrow_take_options =
      static_cast<const arrow::compute::TakeOptions *>(arrow_options);
    auto options = garrow_take_options_new_raw(arrow_take_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ArraySortOptions") {
    const auto arrow_array_sort_options =
      static_cast<const arrow::compute::ArraySortOptions *>(arrow_options);
    auto options = garrow_array_sort_options_new_raw(arrow_array_sort_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "SortOptions") {
    const auto arrow_sort_options =
      static_cast<const arrow::compute::SortOptions *>(arrow_options);
    auto options = garrow_sort_options_new_raw(arrow_sort_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "SetLookupOptions") {
    const auto arrow_set_lookup_options =
      static_cast<const arrow::compute::SetLookupOptions *>(arrow_options);
    auto options = garrow_set_lookup_options_new_raw(arrow_set_lookup_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "VarianceOptions") {
    const auto arrow_variance_options =
      static_cast<const arrow::compute::VarianceOptions *>(arrow_options);
    auto options = garrow_variance_options_new_raw(arrow_variance_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "RoundOptions") {
    const auto arrow_round_options =
      static_cast<const arrow::compute::RoundOptions *>(arrow_options);
    auto options = garrow_round_options_new_raw(arrow_round_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "RoundToMultipleOptions") {
    const auto arrow_round_to_multiple_options =
      static_cast<const arrow::compute::RoundToMultipleOptions *>(arrow_options);
    auto options =
      garrow_round_to_multiple_options_new_raw(arrow_round_to_multiple_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "MatchSubstringOptions") {
    const auto arrow_match_substring_options =
      static_cast<const arrow::compute::MatchSubstringOptions *>(arrow_options);
    auto options = garrow_match_substring_options_new_raw(arrow_match_substring_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "UTF8NormalizedOptions") {
    const auto arrow_utf8_normalize_options =
      static_cast<const arrow::compute::Utf8NormalizeOptions *>(arrow_options);
    auto options = garrow_utf8_normalize_options_new_raw(arrow_utf8_normalize_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "QuantileOptions") {
    const auto arrow_quantile_options =
      static_cast<const arrow::compute::QuantileOptions *>(arrow_options);
    auto options = garrow_quantile_options_new_raw(arrow_quantile_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "IndexOptions") {
    const auto arrow_index_options =
      static_cast<const arrow::compute::IndexOptions *>(arrow_options);
    auto options = garrow_index_options_new_raw(arrow_index_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "RankOptions") {
    const auto arrow_rank_options =
      static_cast<const arrow::compute::RankOptions *>(arrow_options);
    auto options = garrow_rank_options_new_raw(arrow_rank_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "RunEndEncodeOptions") {
    const auto arrow_run_end_encode_options =
      static_cast<const arrow::compute::RunEndEncodeOptions *>(arrow_options);
    auto options = garrow_run_end_encode_options_new_raw(arrow_run_end_encode_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "StrptimeOptions") {
    const auto arrow_strptime_options =
      static_cast<const arrow::compute::StrptimeOptions *>(arrow_options);
    auto options = garrow_strptime_options_new_raw(arrow_strptime_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "StrftimeOptions") {
    const auto arrow_strftime_options =
      static_cast<const arrow::compute::StrftimeOptions *>(arrow_options);
    auto options = garrow_strftime_options_new_raw(arrow_strftime_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "SplitPatternOptions") {
    const auto arrow_split_pattern_options =
      static_cast<const arrow::compute::SplitPatternOptions *>(arrow_options);
    auto options = garrow_split_pattern_options_new_raw(arrow_split_pattern_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "StructFieldOptions") {
    const auto arrow_struct_field_options =
      static_cast<const arrow::compute::StructFieldOptions *>(arrow_options);
    auto options = garrow_struct_field_options_new_raw(arrow_struct_field_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "AssumeTimezoneOptions") {
    const auto arrow_assume_timezone_options =
      static_cast<const arrow::compute::AssumeTimezoneOptions *>(arrow_options);
    auto options = garrow_assume_timezone_options_new_raw(arrow_assume_timezone_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "CumulativeOptions") {
    const auto arrow_cumulative_options =
      static_cast<const arrow::compute::CumulativeOptions *>(arrow_options);
    auto options = garrow_cumulative_options_new_raw(arrow_cumulative_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "DictionaryEncodeOptions") {
    const auto arrow_dictionary_encode_options =
      static_cast<const arrow::compute::DictionaryEncodeOptions *>(arrow_options);
    auto options =
      garrow_dictionary_encode_options_new_raw(arrow_dictionary_encode_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ElementWiseAggregateOptions") {
    const auto arrow_element_wise_aggregate_options =
      static_cast<const arrow::compute::ElementWiseAggregateOptions *>(arrow_options);
    auto options =
      garrow_element_wise_aggregate_options_new_raw(arrow_element_wise_aggregate_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "DayOfWeekOptions") {
    const auto arrow_day_of_week_options =
      static_cast<const arrow::compute::DayOfWeekOptions *>(arrow_options);
    auto options = garrow_day_of_week_options_new_raw(arrow_day_of_week_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ExtractRegexOptions") {
    const auto arrow_extract_regex_options =
      static_cast<const arrow::compute::ExtractRegexOptions *>(arrow_options);
    auto options = garrow_extract_regex_options_new_raw(arrow_extract_regex_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ExtractRegexSpanOptions") {
    const auto arrow_extract_regex_span_options =
      static_cast<const arrow::compute::ExtractRegexSpanOptions *>(arrow_options);
    auto options =
      garrow_extract_regex_span_options_new_raw(arrow_extract_regex_span_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "JoinOptions") {
    const auto arrow_join_options =
      static_cast<const arrow::compute::JoinOptions *>(arrow_options);
    auto options = garrow_join_options_new_raw(arrow_join_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ListFlattenOptions") {
    const auto arrow_list_flatten_options =
      static_cast<const arrow::compute::ListFlattenOptions *>(arrow_options);
    auto options = garrow_list_flatten_options_new_raw(arrow_list_flatten_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "MapLookupOptions") {
    const auto arrow_map_lookup_options =
      static_cast<const arrow::compute::MapLookupOptions *>(arrow_options);
    auto options = garrow_map_lookup_options_new_raw(arrow_map_lookup_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ModeOptions") {
    const auto arrow_mode_options =
      static_cast<const arrow::compute::ModeOptions *>(arrow_options);
    auto options = garrow_mode_options_new_raw(arrow_mode_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "NullOptions") {
    const auto arrow_null_options =
      static_cast<const arrow::compute::NullOptions *>(arrow_options);
    auto options = garrow_null_options_new_raw(arrow_null_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "PadOptions") {
    const auto arrow_pad_options =
      static_cast<const arrow::compute::PadOptions *>(arrow_options);
    auto options = garrow_pad_options_new_raw(arrow_pad_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "PairwiseOptions") {
    const auto arrow_pairwise_options =
      static_cast<const arrow::compute::PairwiseOptions *>(arrow_options);
    auto options = garrow_pairwise_options_new_raw(arrow_pairwise_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "ReplaceSliceOptions") {
    const auto arrow_replace_slice_options =
      static_cast<const arrow::compute::ReplaceSliceOptions *>(arrow_options);
    auto options = garrow_replace_slice_options_new_raw(arrow_replace_slice_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "PartitionNthOptions") {
    const auto arrow_partition_nth_options =
      static_cast<const arrow::compute::PartitionNthOptions *>(arrow_options);
    auto options = garrow_partition_nth_options_new_raw(arrow_partition_nth_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "PivotWiderOptions") {
    const auto arrow_pivot_wider_options =
      static_cast<const arrow::compute::PivotWiderOptions *>(arrow_options);
    auto options = garrow_pivot_wider_options_new_raw(arrow_pivot_wider_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else if (arrow_type_name == "RankQuantileOptions") {
    const auto arrow_rank_quantile_options =
      static_cast<const arrow::compute::RankQuantileOptions *>(arrow_options);
    auto options = garrow_rank_quantile_options_new_raw(arrow_rank_quantile_options);
    return GARROW_FUNCTION_OPTIONS(options);
  } else {
    auto options = g_object_new(GARROW_TYPE_FUNCTION_OPTIONS, NULL);
    return GARROW_FUNCTION_OPTIONS(options);
  }
}

arrow::compute::FunctionOptions *
garrow_function_options_get_raw(GArrowFunctionOptions *options)
{
  auto priv = GARROW_FUNCTION_OPTIONS_GET_PRIVATE(options);
  return priv->options;
}

GArrowFunctionDoc *
garrow_function_doc_new_raw(const arrow::compute::FunctionDoc *arrow_doc)
{
  return GARROW_FUNCTION_DOC(
    g_object_new(GARROW_TYPE_FUNCTION_DOC, "doc", arrow_doc, NULL));
}

arrow::compute::FunctionDoc *
garrow_function_doc_get_raw(GArrowFunctionDoc *doc)
{
  auto priv = GARROW_FUNCTION_DOC_GET_PRIVATE(doc);
  return priv->doc;
}

GArrowFunction *
garrow_function_new_raw(std::shared_ptr<arrow::compute::Function> *arrow_function)
{
  return GARROW_FUNCTION(
    g_object_new(GARROW_TYPE_FUNCTION, "function", arrow_function, NULL));
}

std::shared_ptr<arrow::compute::Function>
garrow_function_get_raw(GArrowFunction *function)
{
  auto priv = GARROW_FUNCTION_GET_PRIVATE(function);
  return priv->function;
}

GArrowExecuteNodeOptions *
garrow_execute_node_options_new_raw(arrow::acero::ExecNodeOptions *arrow_options)
{
  return GARROW_EXECUTE_NODE_OPTIONS(
    g_object_new(GARROW_TYPE_EXECUTE_NODE_OPTIONS, "options", arrow_options, NULL));
}

arrow::acero::ExecNodeOptions *
garrow_execute_node_options_get_raw(GArrowExecuteNodeOptions *options)
{
  auto priv = GARROW_EXECUTE_NODE_OPTIONS_GET_PRIVATE(options);
  return priv->options;
}

GArrowExecuteNode *
garrow_execute_node_new_raw(arrow::acero::ExecNode *arrow_node,
                            GArrowExecuteNodeOptions *options)
{
  return GARROW_EXECUTE_NODE(
    g_object_new(GARROW_TYPE_EXECUTE_NODE, "node", arrow_node, "options", options, NULL));
}

arrow::acero::ExecNode *
garrow_execute_node_get_raw(GArrowExecuteNode *node)
{
  auto priv = GARROW_EXECUTE_NODE_GET_PRIVATE(node);
  return priv->node;
}

std::shared_ptr<arrow::acero::ExecPlan>
garrow_execute_plan_get_raw(GArrowExecutePlan *plan)
{
  auto priv = GARROW_EXECUTE_PLAN_GET_PRIVATE(plan);
  return priv->plan;
}

GArrowCastOptions *
garrow_cast_options_new_raw(const arrow::compute::CastOptions *arrow_options)
{
  GArrowDataType *to_data_type = NULL;
  if (arrow_options->to_type.type) {
    auto arrow_to_data_type = arrow_options->to_type.GetSharedPtr();
    to_data_type = garrow_data_type_new_raw(&arrow_to_data_type);
  }
  auto options = g_object_new(GARROW_TYPE_CAST_OPTIONS,
                              "to-data-type",
                              to_data_type,
                              "allow-int-overflow",
                              arrow_options->allow_int_overflow,
                              "allow-time-truncate",
                              arrow_options->allow_time_truncate,
                              "allow-time-overflow",
                              arrow_options->allow_time_overflow,
                              "allow-decimal-truncate",
                              arrow_options->allow_decimal_truncate,
                              "allow-float-truncate",
                              arrow_options->allow_float_truncate,
                              "allow-invalid-utf8",
                              arrow_options->allow_invalid_utf8,
                              NULL);
  return GARROW_CAST_OPTIONS(options);
}

arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *options)
{
  return static_cast<arrow::compute::CastOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new_raw(
  const arrow::compute::ScalarAggregateOptions *arrow_options)
{
  auto options = g_object_new(GARROW_TYPE_SCALAR_AGGREGATE_OPTIONS,
                              "skip-nulls",
                              arrow_options->skip_nulls,
                              "min-count",
                              arrow_options->min_count,
                              NULL);
  return GARROW_SCALAR_AGGREGATE_OPTIONS(options);
}

arrow::compute::ScalarAggregateOptions *
garrow_scalar_aggregate_options_get_raw(GArrowScalarAggregateOptions *options)
{
  return static_cast<arrow::compute::ScalarAggregateOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowCountOptions *
garrow_count_options_new_raw(const arrow::compute::CountOptions *arrow_options)
{
  auto options =
    g_object_new(GARROW_TYPE_COUNT_OPTIONS, "mode", arrow_options->mode, NULL);
  return GARROW_COUNT_OPTIONS(options);
}

arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *options)
{
  return static_cast<arrow::compute::CountOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowFilterOptions *
garrow_filter_options_new_raw(const arrow::compute::FilterOptions *arrow_options)
{
  auto options = g_object_new(GARROW_TYPE_FILTER_OPTIONS,
                              "null-selection-behavior",
                              arrow_options->null_selection_behavior,
                              NULL);
  return GARROW_FILTER_OPTIONS(options);
}

arrow::compute::FilterOptions *
garrow_filter_options_get_raw(GArrowFilterOptions *options)
{
  return static_cast<arrow::compute::FilterOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowTakeOptions *
garrow_take_options_new_raw(const arrow::compute::TakeOptions *arrow_options)
{
  auto options = GARROW_TAKE_OPTIONS(g_object_new(GARROW_TYPE_TAKE_OPTIONS, NULL));
  /* TODO: Use property when we add support for boundscheck. */
  auto arrow_new_options = garrow_take_options_get_raw(options);
  arrow_new_options->boundscheck = arrow_options->boundscheck;
  return options;
}

arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *options)
{
  return static_cast<arrow::compute::TakeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowArraySortOptions *
garrow_array_sort_options_new_raw(const arrow::compute::ArraySortOptions *arrow_options)
{
  auto options =
    g_object_new(GARROW_TYPE_ARRAY_SORT_OPTIONS, "order", arrow_options->order, NULL);
  return GARROW_ARRAY_SORT_OPTIONS(options);
}

arrow::compute::ArraySortOptions *
garrow_array_sort_options_get_raw(GArrowArraySortOptions *options)
{
  return static_cast<arrow::compute::ArraySortOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowSortKey *
garrow_sort_key_new_raw(const arrow::compute::SortKey &arrow_sort_key)
{
  auto sort_key = g_object_new(GARROW_TYPE_SORT_KEY, NULL);
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(sort_key);
  priv->sort_key = arrow_sort_key;
  return GARROW_SORT_KEY(sort_key);
}

arrow::compute::SortKey *
garrow_sort_key_get_raw(GArrowSortKey *sort_key)
{
  auto priv = GARROW_SORT_KEY_GET_PRIVATE(sort_key);
  return &(priv->sort_key);
}

GArrowSortOptions *
garrow_sort_options_new_raw(const arrow::compute::SortOptions *arrow_options)
{
  auto options = GARROW_SORT_OPTIONS(g_object_new(GARROW_TYPE_SORT_OPTIONS, NULL));
  auto arrow_new_options = garrow_sort_options_get_raw(options);
  arrow_new_options->sort_keys = arrow_options->sort_keys;
  /* TODO: Use property when we add support for null_placement. */
  arrow_new_options->null_placement = arrow_options->null_placement;
  return options;
}

arrow::compute::SortOptions *
garrow_sort_options_get_raw(GArrowSortOptions *options)
{
  return static_cast<arrow::compute::SortOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowSetLookupOptions *
garrow_set_lookup_options_new_raw(const arrow::compute::SetLookupOptions *arrow_options)
{
  auto arrow_copied_options = arrow_options->Copy();
  auto arrow_copied_set_lookup_options =
    static_cast<arrow::compute::SetLookupOptions *>(arrow_copied_options.get());
  auto value_set = garrow_datum_new_raw(&(arrow_copied_set_lookup_options->value_set));
  auto skip_nulls =
    (arrow_options->skip_nulls.has_value() && arrow_options->skip_nulls.value());
  auto options = g_object_new(GARROW_TYPE_SET_LOOKUP_OPTIONS,
                              "value-set",
                              value_set,
                              "skip-nulls",
                              skip_nulls,
                              NULL);
  return GARROW_SET_LOOKUP_OPTIONS(options);
}

arrow::compute::SetLookupOptions *
garrow_set_lookup_options_get_raw(GArrowSetLookupOptions *options)
{
  return static_cast<arrow::compute::SetLookupOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowVarianceOptions *
garrow_variance_options_new_raw(const arrow::compute::VarianceOptions *arrow_options)
{
  auto options = g_object_new(GARROW_TYPE_VARIANCE_OPTIONS,
                              "ddof",
                              arrow_options->ddof,
                              "skip-nulls",
                              arrow_options->skip_nulls,
                              "min-count",
                              arrow_options->min_count,
                              NULL);
  return GARROW_VARIANCE_OPTIONS(options);
}

arrow::compute::VarianceOptions *
garrow_variance_options_get_raw(GArrowVarianceOptions *options)
{
  return static_cast<arrow::compute::VarianceOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowRoundOptions *
garrow_round_options_new_raw(const arrow::compute::RoundOptions *arrow_options)
{
  auto options = g_object_new(GARROW_TYPE_ROUND_OPTIONS,
                              "n-digits",
                              arrow_options->ndigits,
                              "mode",
                              static_cast<GArrowRoundMode>(arrow_options->round_mode),
                              NULL);
  return GARROW_ROUND_OPTIONS(options);
}

arrow::compute::RoundOptions *
garrow_round_options_get_raw(GArrowRoundOptions *options)
{
  return static_cast<arrow::compute::RoundOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowRoundToMultipleOptions *
garrow_round_to_multiple_options_new_raw(
  const arrow::compute::RoundToMultipleOptions *arrow_options)
{
  auto arrow_copied_options = arrow_options->Copy();
  auto arrow_copied_round_to_multiple_options =
    static_cast<arrow::compute::RoundToMultipleOptions *>(arrow_copied_options.get());
  auto multiple =
    garrow_scalar_new_raw(&(arrow_copied_round_to_multiple_options->multiple));
  auto options = g_object_new(GARROW_TYPE_ROUND_TO_MULTIPLE_OPTIONS,
                              "multiple",
                              multiple,
                              "mode",
                              static_cast<GArrowRoundMode>(arrow_options->round_mode),
                              NULL);
  g_object_unref(multiple);
  return GARROW_ROUND_TO_MULTIPLE_OPTIONS(options);
}

arrow::compute::RoundToMultipleOptions *
garrow_round_to_multiple_options_get_raw(GArrowRoundToMultipleOptions *options)
{
  return static_cast<arrow::compute::RoundToMultipleOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowMatchSubstringOptions *
garrow_match_substring_options_new_raw(
  const arrow::compute::MatchSubstringOptions *arrow_options)
{
  return GARROW_MATCH_SUBSTRING_OPTIONS(g_object_new(GARROW_TYPE_MATCH_SUBSTRING_OPTIONS,
                                                     "pattern",
                                                     arrow_options->pattern.c_str(),
                                                     "ignore-case",
                                                     arrow_options->ignore_case,
                                                     NULL));
}

arrow::compute::MatchSubstringOptions *
garrow_match_substring_options_get_raw(GArrowMatchSubstringOptions *options)
{
  return static_cast<arrow::compute::MatchSubstringOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowUTF8NormalizeOptions *
garrow_utf8_normalize_options_new_raw(
  const arrow::compute::Utf8NormalizeOptions *arrow_options)
{
  auto options =
    g_object_new(GARROW_TYPE_UTF8_NORMALIZE_FORM, "form", arrow_options->form, NULL);
  return GARROW_UTF8_NORMALIZE_OPTIONS(options);
}

arrow::compute::Utf8NormalizeOptions *
garrow_utf8_normalize_options_get_raw(GArrowUTF8NormalizeOptions *options)
{
  return static_cast<arrow::compute::Utf8NormalizeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowQuantileOptions *
garrow_quantile_options_new_raw(const arrow::compute::QuantileOptions *arrow_options)
{
  auto options = GARROW_QUANTILE_OPTIONS(g_object_new(GARROW_TYPE_QUANTILE_OPTIONS,
                                                      "interpolation",
                                                      arrow_options->interpolation,
                                                      "skip-nulls",
                                                      arrow_options->skip_nulls,
                                                      "min-count",
                                                      arrow_options->min_count,
                                                      NULL));
  garrow_quantile_options_set_qs(options,
                                 arrow_options->q.data(),
                                 arrow_options->q.size());
  return options;
}

arrow::compute::QuantileOptions *
garrow_quantile_options_get_raw(GArrowQuantileOptions *options)
{
  return static_cast<arrow::compute::QuantileOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowIndexOptions *
garrow_index_options_new_raw(const arrow::compute::IndexOptions *arrow_options)
{
  auto arrow_value = arrow_options->value->GetSharedPtr();
  GArrowScalar *value = nullptr;
  if (arrow_value) {
    value = garrow_scalar_new_raw(&arrow_value);
  }
  auto options =
    GARROW_INDEX_OPTIONS(g_object_new(GARROW_TYPE_INDEX_OPTIONS, "value", value, NULL));
  if (value) {
    g_object_unref(value);
  }
  return options;
}

arrow::compute::IndexOptions *
garrow_index_options_get_raw(GArrowIndexOptions *options)
{
  return static_cast<arrow::compute::IndexOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowRankOptions *
garrow_rank_options_new_raw(const arrow::compute::RankOptions *arrow_options)
{
  auto options = GARROW_RANK_OPTIONS(g_object_new(GARROW_TYPE_RANK_OPTIONS,
                                                  "null-placement",
                                                  arrow_options->null_placement,
                                                  "tiebreaker",
                                                  arrow_options->tiebreaker,
                                                  nullptr));
  auto arrow_new_options = garrow_rank_options_get_raw(options);
  arrow_new_options->sort_keys = arrow_options->sort_keys;
  return options;
}

arrow::compute::RankOptions *
garrow_rank_options_get_raw(GArrowRankOptions *options)
{
  return static_cast<arrow::compute::RankOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowRunEndEncodeOptions *
garrow_run_end_encode_options_new_raw(
  const arrow::compute::RunEndEncodeOptions *arrow_options)
{
  GArrowDataType *run_end_data_type = nullptr;
  if (arrow_options->run_end_type) {
    auto arrow_run_end_data_type = arrow_options->run_end_type;
    run_end_data_type = garrow_data_type_new_raw(&arrow_run_end_data_type);
  }
  return garrow_run_end_encode_options_new(run_end_data_type);
}

arrow::compute::RunEndEncodeOptions *
garrow_run_end_encode_options_get_raw(GArrowRunEndEncodeOptions *options)
{
  return static_cast<arrow::compute::RunEndEncodeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowStrptimeOptions *
garrow_strptime_options_new_raw(const arrow::compute::StrptimeOptions *arrow_options)
{
  return GARROW_STRPTIME_OPTIONS(g_object_new(GARROW_TYPE_STRPTIME_OPTIONS,
                                              "format",
                                              arrow_options->format.c_str(),
                                              "unit",
                                              arrow_options->unit,
                                              "error_is_null",
                                              arrow_options->error_is_null,
                                              NULL));
}

arrow::compute::StrptimeOptions *
garrow_strptime_options_get_raw(GArrowStrptimeOptions *options)
{
  return static_cast<arrow::compute::StrptimeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowStrftimeOptions *
garrow_strftime_options_new_raw(const arrow::compute::StrftimeOptions *arrow_options)
{
  return GARROW_STRFTIME_OPTIONS(g_object_new(GARROW_TYPE_STRFTIME_OPTIONS,
                                              "format",
                                              arrow_options->format.c_str(),
                                              "locale",
                                              arrow_options->locale.c_str(),
                                              NULL));
}

arrow::compute::StrftimeOptions *
garrow_strftime_options_get_raw(GArrowStrftimeOptions *options)
{
  return static_cast<arrow::compute::StrftimeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowSplitPatternOptions *
garrow_split_pattern_options_new_raw(
  const arrow::compute::SplitPatternOptions *arrow_options)
{
  return GARROW_SPLIT_PATTERN_OPTIONS(g_object_new(GARROW_TYPE_SPLIT_PATTERN_OPTIONS,
                                                   "pattern",
                                                   arrow_options->pattern.c_str(),
                                                   "max_splits",
                                                   arrow_options->max_splits,
                                                   "reverse",
                                                   arrow_options->reverse,
                                                   NULL));
}

arrow::compute::SplitPatternOptions *
garrow_split_pattern_options_get_raw(GArrowSplitPatternOptions *options)
{
  return static_cast<arrow::compute::SplitPatternOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowStructFieldOptions *
garrow_struct_field_options_new_raw(
  const arrow::compute::StructFieldOptions *arrow_options)
{
  auto options =
    GARROW_STRUCT_FIELD_OPTIONS(g_object_new(GARROW_TYPE_STRUCT_FIELD_OPTIONS, NULL));
  auto arrow_new_options = garrow_struct_field_options_get_raw(options);
  arrow_new_options->field_ref = arrow_options->field_ref;
  return options;
}

arrow::compute::StructFieldOptions *
garrow_struct_field_options_get_raw(GArrowStructFieldOptions *options)
{
  return static_cast<arrow::compute::StructFieldOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowAssumeTimezoneOptions *
garrow_assume_timezone_options_new_raw(
  const arrow::compute::AssumeTimezoneOptions *arrow_options)
{
  return GARROW_ASSUME_TIMEZONE_OPTIONS(
    g_object_new(GARROW_TYPE_ASSUME_TIMEZONE_OPTIONS,
                 "timezone",
                 arrow_options->timezone.c_str(),
                 "ambiguous",
                 static_cast<GArrowAssumeTimezoneAmbiguous>(arrow_options->ambiguous),
                 "nonexistent",
                 static_cast<GArrowAssumeTimezoneNonexistent>(arrow_options->nonexistent),
                 NULL));
}

arrow::compute::AssumeTimezoneOptions *
garrow_assume_timezone_options_get_raw(GArrowAssumeTimezoneOptions *options)
{
  return static_cast<arrow::compute::AssumeTimezoneOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowCumulativeOptions *
garrow_cumulative_options_new_raw(const arrow::compute::CumulativeOptions *arrow_options)
{
  GArrowScalar *start = nullptr;
  if (arrow_options->start.has_value()) {
    std::shared_ptr<arrow::Scalar> arrow_start = arrow_options->start.value();
    start = garrow_scalar_new_raw(&arrow_start);
  }
  auto options = GARROW_CUMULATIVE_OPTIONS(g_object_new(GARROW_TYPE_CUMULATIVE_OPTIONS,
                                                        "start",
                                                        start,
                                                        "skip-nulls",
                                                        arrow_options->skip_nulls,
                                                        NULL));
  if (start) {
    g_object_unref(start);
  }
  return options;
}

arrow::compute::CumulativeOptions *
garrow_cumulative_options_get_raw(GArrowCumulativeOptions *options)
{
  return static_cast<arrow::compute::CumulativeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowDictionaryEncodeOptions *
garrow_dictionary_encode_options_new_raw(
  const arrow::compute::DictionaryEncodeOptions *arrow_options)
{
  return GARROW_DICTIONARY_ENCODE_OPTIONS(
    g_object_new(GARROW_TYPE_DICTIONARY_ENCODE_OPTIONS,
                 "null-encoding-behavior",
                 static_cast<GArrowDictionaryEncodeNullEncodingBehavior>(
                   arrow_options->null_encoding_behavior),
                 NULL));
}

arrow::compute::DictionaryEncodeOptions *
garrow_dictionary_encode_options_get_raw(GArrowDictionaryEncodeOptions *options)
{
  return static_cast<arrow::compute::DictionaryEncodeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowElementWiseAggregateOptions *
garrow_element_wise_aggregate_options_new_raw(
  const arrow::compute::ElementWiseAggregateOptions *arrow_options)
{
  return GARROW_ELEMENT_WISE_AGGREGATE_OPTIONS(
    g_object_new(GARROW_TYPE_ELEMENT_WISE_AGGREGATE_OPTIONS,
                 "skip-nulls",
                 arrow_options->skip_nulls,
                 NULL));
}

arrow::compute::ElementWiseAggregateOptions *
garrow_element_wise_aggregate_options_get_raw(GArrowElementWiseAggregateOptions *options)
{
  return static_cast<arrow::compute::ElementWiseAggregateOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowDayOfWeekOptions *
garrow_day_of_week_options_new_raw(const arrow::compute::DayOfWeekOptions *arrow_options)
{
  return GARROW_DAY_OF_WEEK_OPTIONS(g_object_new(GARROW_TYPE_DAY_OF_WEEK_OPTIONS,
                                                 "count-from-zero",
                                                 arrow_options->count_from_zero,
                                                 "week-start",
                                                 arrow_options->week_start,
                                                 NULL));
}

arrow::compute::DayOfWeekOptions *
garrow_day_of_week_options_get_raw(GArrowDayOfWeekOptions *options)
{
  return static_cast<arrow::compute::DayOfWeekOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowExtractRegexOptions *
garrow_extract_regex_options_new_raw(
  const arrow::compute::ExtractRegexOptions *arrow_options)
{
  return GARROW_EXTRACT_REGEX_OPTIONS(g_object_new(GARROW_TYPE_EXTRACT_REGEX_OPTIONS,
                                                   "pattern",
                                                   arrow_options->pattern.c_str(),
                                                   NULL));
}

arrow::compute::ExtractRegexOptions *
garrow_extract_regex_options_get_raw(GArrowExtractRegexOptions *options)
{
  return static_cast<arrow::compute::ExtractRegexOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowExtractRegexSpanOptions *
garrow_extract_regex_span_options_new_raw(
  const arrow::compute::ExtractRegexSpanOptions *arrow_options)
{
  return GARROW_EXTRACT_REGEX_SPAN_OPTIONS(
    g_object_new(GARROW_TYPE_EXTRACT_REGEX_SPAN_OPTIONS,
                 "pattern",
                 arrow_options->pattern.c_str(),
                 NULL));
}

arrow::compute::ExtractRegexSpanOptions *
garrow_extract_regex_span_options_get_raw(GArrowExtractRegexSpanOptions *options)
{
  return static_cast<arrow::compute::ExtractRegexSpanOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowJoinOptions *
garrow_join_options_new_raw(const arrow::compute::JoinOptions *arrow_options)
{
  return GARROW_JOIN_OPTIONS(g_object_new(
    GARROW_TYPE_JOIN_OPTIONS,
    "null-handling",
    static_cast<GArrowJoinNullHandlingBehavior>(arrow_options->null_handling),
    "null-replacement",
    arrow_options->null_replacement.c_str(),
    nullptr));
}

arrow::compute::JoinOptions *
garrow_join_options_get_raw(GArrowJoinOptions *options)
{
  return static_cast<arrow::compute::JoinOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowListFlattenOptions *
garrow_list_flatten_options_new_raw(
  const arrow::compute::ListFlattenOptions *arrow_options)
{
  return GARROW_LIST_FLATTEN_OPTIONS(g_object_new(GARROW_TYPE_LIST_FLATTEN_OPTIONS,
                                                  "recursive",
                                                  arrow_options->recursive,
                                                  NULL));
}

arrow::compute::ListFlattenOptions *
garrow_list_flatten_options_get_raw(GArrowListFlattenOptions *options)
{
  return static_cast<arrow::compute::ListFlattenOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowMapLookupOptions *
garrow_map_lookup_options_new_raw(const arrow::compute::MapLookupOptions *arrow_options)
{
  GArrowScalar *query_key = nullptr;
  if (arrow_options->query_key) {
    auto arrow_query_key = arrow_options->query_key;
    query_key = garrow_scalar_new_raw(&arrow_query_key);
  }
  GArrowMapLookupOccurrence occurrence =
    static_cast<GArrowMapLookupOccurrence>(arrow_options->occurrence);
  return GARROW_MAP_LOOKUP_OPTIONS(g_object_new(GARROW_TYPE_MAP_LOOKUP_OPTIONS,
                                                "query-key",
                                                query_key,
                                                "occurrence",
                                                occurrence,
                                                NULL));
}

arrow::compute::MapLookupOptions *
garrow_map_lookup_options_get_raw(GArrowMapLookupOptions *options)
{
  return static_cast<arrow::compute::MapLookupOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowListSliceOptions *
garrow_list_slice_options_new_raw(const arrow::compute::ListSliceOptions *arrow_options)
{
  gint64 stop_value = GARROW_LIST_SLICE_OPTIONS_STOP_UNSPECIFIED;
  if (arrow_options->stop.has_value()) {
    stop_value = arrow_options->stop.value();
  }
  GArrowListSliceReturnFixedSizeList return_fixed_size_list_value =
    GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_AUTO;
  if (arrow_options->return_fixed_size_list.has_value()) {
    if (arrow_options->return_fixed_size_list.value()) {
      return_fixed_size_list_value = GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_TRUE;
    } else {
      return_fixed_size_list_value = GARROW_LIST_SLICE_RETURN_FIXED_SIZE_LIST_FALSE;
    }
  }
  return GARROW_LIST_SLICE_OPTIONS(g_object_new(GARROW_TYPE_LIST_SLICE_OPTIONS,
                                                "start",
                                                arrow_options->start,
                                                "stop",
                                                stop_value,
                                                "step",
                                                arrow_options->step,
                                                "return-fixed-size-list",
                                                return_fixed_size_list_value,
                                                NULL));
}

arrow::compute::ListSliceOptions *
garrow_list_slice_options_get_raw(GArrowListSliceOptions *options)
{
  return static_cast<arrow::compute::ListSliceOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowModeOptions *
garrow_mode_options_new_raw(const arrow::compute::ModeOptions *arrow_options)
{
  auto options = g_object_new(GARROW_TYPE_MODE_OPTIONS,
                              "n",
                              arrow_options->n,
                              "skip-nulls",
                              arrow_options->skip_nulls,
                              "min-count",
                              arrow_options->min_count,
                              NULL);
  return GARROW_MODE_OPTIONS(options);
}

arrow::compute::ModeOptions *
garrow_mode_options_get_raw(GArrowModeOptions *options)
{
  return static_cast<arrow::compute::ModeOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowNullOptions *
garrow_null_options_new_raw(const arrow::compute::NullOptions *arrow_options)
{
  return GARROW_NULL_OPTIONS(g_object_new(GARROW_TYPE_NULL_OPTIONS,
                                          "nan-is-null",
                                          arrow_options->nan_is_null,
                                          NULL));
}

arrow::compute::NullOptions *
garrow_null_options_get_raw(GArrowNullOptions *options)
{
  return static_cast<arrow::compute::NullOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowPadOptions *
garrow_pad_options_new_raw(const arrow::compute::PadOptions *arrow_options)
{
  return GARROW_PAD_OPTIONS(g_object_new(GARROW_TYPE_PAD_OPTIONS,
                                         "width",
                                         arrow_options->width,
                                         "padding",
                                         arrow_options->padding.c_str(),
                                         "lean-left-on-odd-padding",
                                         arrow_options->lean_left_on_odd_padding,
                                         NULL));
}

arrow::compute::PadOptions *
garrow_pad_options_get_raw(GArrowPadOptions *options)
{
  return static_cast<arrow::compute::PadOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowPairwiseOptions *
garrow_pairwise_options_new_raw(const arrow::compute::PairwiseOptions *arrow_options)
{
  return GARROW_PAIRWISE_OPTIONS(
    g_object_new(GARROW_TYPE_PAIRWISE_OPTIONS, "periods", arrow_options->periods, NULL));
}

arrow::compute::PairwiseOptions *
garrow_pairwise_options_get_raw(GArrowPairwiseOptions *options)
{
  return static_cast<arrow::compute::PairwiseOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowReplaceSliceOptions *
garrow_replace_slice_options_new_raw(
  const arrow::compute::ReplaceSliceOptions *arrow_options)
{
  return GARROW_REPLACE_SLICE_OPTIONS(g_object_new(GARROW_TYPE_REPLACE_SLICE_OPTIONS,
                                                   "start",
                                                   arrow_options->start,
                                                   "stop",
                                                   arrow_options->stop,
                                                   "replacement",
                                                   arrow_options->replacement.c_str(),
                                                   nullptr));
}

arrow::compute::ReplaceSliceOptions *
garrow_replace_slice_options_get_raw(GArrowReplaceSliceOptions *options)
{
  return static_cast<arrow::compute::ReplaceSliceOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowPartitionNthOptions *
garrow_partition_nth_options_new_raw(
  const arrow::compute::PartitionNthOptions *arrow_options)
{
  return GARROW_PARTITION_NTH_OPTIONS(
    g_object_new(GARROW_TYPE_PARTITION_NTH_OPTIONS,
                 "pivot",
                 arrow_options->pivot,
                 "null-placement",
                 static_cast<GArrowNullPlacement>(arrow_options->null_placement),
                 nullptr));
}

arrow::compute::PartitionNthOptions *
garrow_partition_nth_options_get_raw(GArrowPartitionNthOptions *options)
{
  return static_cast<arrow::compute::PartitionNthOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowPivotWiderOptions *
garrow_pivot_wider_options_new_raw(const arrow::compute::PivotWiderOptions *arrow_options)
{
  auto strv = static_cast<gchar **>(g_new0(gchar *, arrow_options->key_names.size() + 1));
  for (gsize i = 0; i < arrow_options->key_names.size(); ++i) {
    strv[i] = g_strdup(arrow_options->key_names[i].c_str());
  }
  return GARROW_PIVOT_WIDER_OPTIONS(
    g_object_new(GARROW_TYPE_PIVOT_WIDER_OPTIONS,
                 "key-names",
                 strv,
                 "unexpected-key-behavior",
                 static_cast<GArrowPivotWiderUnexpectedKeyBehavior>(
                   arrow_options->unexpected_key_behavior),
                 nullptr));
}

arrow::compute::PivotWiderOptions *
garrow_pivot_wider_options_get_raw(GArrowPivotWiderOptions *options)
{
  return static_cast<arrow::compute::PivotWiderOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}

GArrowRankQuantileOptions *
garrow_rank_quantile_options_new_raw(
  const arrow::compute::RankQuantileOptions *arrow_options)
{
  auto options =
    GARROW_RANK_QUANTILE_OPTIONS(g_object_new(GARROW_TYPE_RANK_QUANTILE_OPTIONS,
                                              "null-placement",
                                              arrow_options->null_placement,
                                              nullptr));
  auto arrow_new_options = garrow_rank_quantile_options_get_raw(options);
  arrow_new_options->sort_keys = arrow_options->sort_keys;
  return options;
}

arrow::compute::RankQuantileOptions *
garrow_rank_quantile_options_get_raw(GArrowRankQuantileOptions *options)
{
  return static_cast<arrow::compute::RankQuantileOptions *>(
    garrow_function_options_get_raw(GARROW_FUNCTION_OPTIONS(options)));
}
