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

#include <arrow-glib/compute-definition.h>
#include <arrow-glib/datum.h>
#include <arrow-glib/reader.h>

G_BEGIN_DECLS

#define GARROW_TYPE_EXECUTE_CONTEXT (garrow_execute_context_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExecuteContext,
                         garrow_execute_context,
                         GARROW,
                         EXECUTE_CONTEXT,
                         GObject)
struct _GArrowExecuteContextClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowExecuteContext *garrow_execute_context_new(void);


GARROW_AVAILABLE_IN_7_0
gboolean
garrow_function_options_equal(GArrowFunctionOptions *options,
                              GArrowFunctionOptions *other_options);
GARROW_AVAILABLE_IN_7_0
gchar *
garrow_function_options_to_string(GArrowFunctionOptions *options);


#define GARROW_TYPE_FUNCTION_DOC (garrow_function_doc_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFunctionDoc,
                         garrow_function_doc,
                         GARROW,
                         FUNCTION_DOC,
                         GObject)
struct _GArrowFunctionDocClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
gchar *
garrow_function_doc_get_summary(GArrowFunctionDoc *doc);
GARROW_AVAILABLE_IN_6_0
gchar *
garrow_function_doc_get_description(GArrowFunctionDoc *doc);
GARROW_AVAILABLE_IN_6_0
gchar **
garrow_function_doc_get_arg_names(GArrowFunctionDoc *doc);
GARROW_AVAILABLE_IN_6_0
gchar *
garrow_function_doc_get_options_class_name(GArrowFunctionDoc *doc);


#define GARROW_TYPE_FUNCTION (garrow_function_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFunction,
                         garrow_function,
                         GARROW,
                         FUNCTION,
                         GObject)
struct _GArrowFunctionClass
{
  GObjectClass parent_class;
};


GARROW_AVAILABLE_IN_1_0
GArrowFunction *garrow_function_find(const gchar *name);

GARROW_AVAILABLE_IN_7_0
GList *garrow_function_all(void);

GARROW_AVAILABLE_IN_1_0
GArrowDatum *garrow_function_execute(GArrowFunction *function,
                                     GList *args,
                                     GArrowFunctionOptions *options,
                                     GArrowExecuteContext *context,
                                     GError **error);

GARROW_AVAILABLE_IN_7_0
const gchar *
garrow_function_get_name(GArrowFunction *function);
GARROW_AVAILABLE_IN_6_0
GArrowFunctionDoc *
garrow_function_get_doc(GArrowFunction *function);
GARROW_AVAILABLE_IN_7_0
GArrowFunctionOptions *
garrow_function_get_default_options(GArrowFunction *function);
GARROW_AVAILABLE_IN_7_0
GType
garrow_function_get_options_type(GArrowFunction *function);

GARROW_AVAILABLE_IN_7_0
gboolean
garrow_function_equal(GArrowFunction *function,
                      GArrowFunction *other_function);
GARROW_AVAILABLE_IN_7_0
gchar *
garrow_function_to_string(GArrowFunction *function);


#define GARROW_TYPE_EXECUTE_NODE_OPTIONS (garrow_execute_node_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExecuteNodeOptions,
                         garrow_execute_node_options,
                         GARROW,
                         EXECUTE_NODE_OPTIONS,
                         GObject)
struct _GArrowExecuteNodeOptionsClass
{
  GObjectClass parent_class;
};


#define GARROW_TYPE_SOURCE_NODE_OPTIONS (garrow_source_node_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSourceNodeOptions,
                         garrow_source_node_options,
                         GARROW,
                         SOURCE_NODE_OPTIONS,
                         GArrowExecuteNodeOptions)
struct _GArrowSourceNodeOptionsClass
{
  GArrowExecuteNodeOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowSourceNodeOptions *
garrow_source_node_options_new_record_batch_reader(
  GArrowRecordBatchReader *reader);
GARROW_AVAILABLE_IN_6_0
GArrowSourceNodeOptions *
garrow_source_node_options_new_record_batch(GArrowRecordBatch *record_batch);
GARROW_AVAILABLE_IN_6_0
GArrowSourceNodeOptions *
garrow_source_node_options_new_table(GArrowTable *table);


#define GARROW_TYPE_PROJECT_NODE_OPTIONS (garrow_project_node_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowProjectNodeOptions,
                         garrow_project_node_options,
                         GARROW,
                         PROJECT_NODE_OPTIONS,
                         GArrowExecuteNodeOptions)
struct _GArrowProjectNodeOptionsClass
{
  GArrowExecuteNodeOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GArrowProjectNodeOptions *
garrow_project_node_options_new(GList *expressions,
                                gchar **names,
                                gsize n_names);


#define GARROW_TYPE_AGGREGATION (garrow_aggregation_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowAggregation,
                         garrow_aggregation,
                         GARROW,
                         AGGREGATION,
                         GObject)
struct _GArrowAggregationClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowAggregation *
garrow_aggregation_new(const gchar *function,
                       GArrowFunctionOptions *options,
                       const gchar *input,
                       const gchar *output);

#define GARROW_TYPE_AGGREGATE_NODE_OPTIONS      \
  (garrow_aggregate_node_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowAggregateNodeOptions,
                         garrow_aggregate_node_options,
                         GARROW,
                         AGGREGATE_NODE_OPTIONS,
                         GArrowExecuteNodeOptions)
struct _GArrowAggregateNodeOptionsClass
{
  GArrowExecuteNodeOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowAggregateNodeOptions *
garrow_aggregate_node_options_new(GList *aggregations,
                                  const gchar **keys,
                                  gsize n_keys,
                                  GError **error);


#define GARROW_TYPE_SINK_NODE_OPTIONS (garrow_sink_node_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSinkNodeOptions,
                         garrow_sink_node_options,
                         GARROW,
                         SINK_NODE_OPTIONS,
                         GArrowExecuteNodeOptions)
struct _GArrowSinkNodeOptionsClass
{
  GArrowExecuteNodeOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowSinkNodeOptions *
garrow_sink_node_options_new(void);
GARROW_AVAILABLE_IN_6_0
GArrowRecordBatchReader *
garrow_sink_node_options_get_reader(GArrowSinkNodeOptions *options,
                                    GArrowSchema *schema);


/**
 * GArrowJoinType:
 * @GARROW_JOIN_TYPE_LEFT_SEMI:
 * @GARROW_JOIN_TYPE_RIGHT_SEMI:
 * @GARROW_JOIN_TYPE_LEFT_ANTI:
 * @GARROW_JOIN_TYPE_RIGHT_ANTI:
 * @GARROW_JOIN_TYPE_INNER:
 * @GARROW_JOIN_TYPE_LEFT_OUTER:
 * @GARROW_JOIN_TYPE_RIGHT_OUTER:
 * @GARROW_JOIN_TYPE_FULL_OUTER:
 *
 * They correspond to the values of `arrow::compute::JoinType`.
 *
 * Since: 7.0.0
 */
typedef enum {
  GARROW_JOIN_TYPE_LEFT_SEMI,
  GARROW_JOIN_TYPE_RIGHT_SEMI,
  GARROW_JOIN_TYPE_LEFT_ANTI,
  GARROW_JOIN_TYPE_RIGHT_ANTI,
  GARROW_JOIN_TYPE_INNER,
  GARROW_JOIN_TYPE_LEFT_OUTER,
  GARROW_JOIN_TYPE_RIGHT_OUTER,
  GARROW_JOIN_TYPE_FULL_OUTER,
} GArrowJoinType;

#define GARROW_TYPE_HASH_JOIN_NODE_OPTIONS      \
  (garrow_hash_join_node_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowHashJoinNodeOptions,
                         garrow_hash_join_node_options,
                         GARROW,
                         HASH_JOIN_NODE_OPTIONS,
                         GArrowExecuteNodeOptions)
struct _GArrowHashJoinNodeOptionsClass
{
  GArrowExecuteNodeOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowHashJoinNodeOptions *
garrow_hash_join_node_options_new(GArrowJoinType type,
                                  const gchar **left_keys,
                                  gsize n_left_keys,
                                  const gchar **right_keys,
                                  gsize n_right_keys,
                                  GError **error);
GARROW_AVAILABLE_IN_7_0
gboolean
garrow_hash_join_node_options_set_left_outputs(
  GArrowHashJoinNodeOptions *options,
  const gchar **outputs,
  gsize n_outputs,
  GError **error);
GARROW_AVAILABLE_IN_7_0
gboolean
garrow_hash_join_node_options_set_right_outputs(
  GArrowHashJoinNodeOptions *options,
  const gchar **outputs,
  gsize n_outputs,
  GError **error);


#define GARROW_TYPE_EXECUTE_NODE (garrow_execute_node_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExecuteNode,
                         garrow_execute_node,
                         GARROW,
                         EXECUTE_NODE,
                         GObject)
struct _GArrowExecuteNodeClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
const gchar *
garrow_execute_node_get_kind_name(GArrowExecuteNode *node);
GARROW_AVAILABLE_IN_6_0
GArrowSchema *
garrow_execute_node_get_output_schema(GArrowExecuteNode *node);


#define GARROW_TYPE_EXECUTE_PLAN (garrow_execute_plan_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExecutePlan,
                         garrow_execute_plan,
                         GARROW,
                         EXECUTE_PLAN,
                         GObject)
struct _GArrowExecutePlanClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowExecutePlan *
garrow_execute_plan_new(GError **error);
GARROW_AVAILABLE_IN_6_0
GArrowExecuteNode *
garrow_execute_plan_build_node(GArrowExecutePlan *plan,
                               const gchar *factory_name,
                               GList *inputs,
                               GArrowExecuteNodeOptions *options,
                               GError **error);
GARROW_AVAILABLE_IN_6_0
GArrowExecuteNode *
garrow_execute_plan_build_source_node(GArrowExecutePlan *plan,
                                      GArrowSourceNodeOptions *options,
                                      GError **error);
GARROW_AVAILABLE_IN_11_0
GArrowExecuteNode *
garrow_execute_plan_build_project_node(GArrowExecutePlan *plan,
                                       GArrowExecuteNode *input,
                                       GArrowProjectNodeOptions *options,
                                       GError **error);
GARROW_AVAILABLE_IN_6_0
GArrowExecuteNode *
garrow_execute_plan_build_aggregate_node(GArrowExecutePlan *plan,
                                         GArrowExecuteNode *input,
                                         GArrowAggregateNodeOptions *options,
                                         GError **error);
GARROW_AVAILABLE_IN_6_0
GArrowExecuteNode *
garrow_execute_plan_build_sink_node(GArrowExecutePlan *plan,
                                    GArrowExecuteNode *input,
                                    GArrowSinkNodeOptions *options,
                                    GError **error);
GARROW_AVAILABLE_IN_7_0
GArrowExecuteNode *
garrow_execute_plan_build_hash_join_node(GArrowExecutePlan *plan,
                                         GArrowExecuteNode *left,
                                         GArrowExecuteNode *right,
                                         GArrowHashJoinNodeOptions *options,
                                         GError **error);
GARROW_AVAILABLE_IN_6_0
gboolean
garrow_execute_plan_validate(GArrowExecutePlan *plan,
                             GError **error);
GARROW_AVAILABLE_IN_6_0
void
garrow_execute_plan_start(GArrowExecutePlan *plan);
GARROW_AVAILABLE_IN_6_0
void
garrow_execute_plan_stop(GArrowExecutePlan *plan);
GARROW_AVAILABLE_IN_6_0
gboolean
garrow_execute_plan_wait(GArrowExecutePlan *plan,
                         GError **error);


GArrowCastOptions *garrow_cast_options_new(void);


#define GARROW_TYPE_SCALAR_AGGREGATE_OPTIONS    \
  (garrow_scalar_aggregate_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowScalarAggregateOptions,
                         garrow_scalar_aggregate_options,
                         GARROW,
                         SCALAR_AGGREGATE_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowScalarAggregateOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new(void);

/**
 * GArrowCountMode:
 * @GARROW_COUNT_MODE_ONLY_VALID:
 *   Only non-null values will be counted.
 * @GARROW_COUNT_MODE_ONLY_NULL:
 *   Only null values will be counted.
 * @GARROW_COUNT_MODE_ALL:
 *   All will be counted.
 *
 * They correspond to the values of `arrow::compute::CountOptions::CountMode`.
 */
typedef enum {
  GARROW_COUNT_MODE_ONLY_VALID,
  GARROW_COUNT_MODE_ONLY_NULL,
  GARROW_COUNT_MODE_ALL,
} GArrowCountMode;

#define GARROW_TYPE_COUNT_OPTIONS (garrow_count_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCountOptions,
                         garrow_count_options,
                         GARROW,
                         COUNT_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowCountOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowCountOptions *
garrow_count_options_new(void);


/**
 * GArrowFilterNullSelectionBehavior:
 * @GARROW_FILTER_NULL_SELECTION_DROP:
 *   Filtered value will be removed in the output.
 * @GARROW_FILTER_NULL_SELECTION_EMIT_NULL:
 *   Filtered value will be null in the output.
 *
 * They are corresponding to
 * `arrow::compute::FilterOptions::NullSelectionBehavior` values.
 */
typedef enum {
  GARROW_FILTER_NULL_SELECTION_DROP,
  GARROW_FILTER_NULL_SELECTION_EMIT_NULL,
} GArrowFilterNullSelectionBehavior;

#define GARROW_TYPE_FILTER_OPTIONS (garrow_filter_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFilterOptions,
                         garrow_filter_options,
                         GARROW,
                         FILTER_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowFilterOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowFilterOptions *
garrow_filter_options_new(void);


#define GARROW_TYPE_TAKE_OPTIONS (garrow_take_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTakeOptions,
                         garrow_take_options,
                         GARROW,
                         TAKE_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowTakeOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_0_14
GArrowTakeOptions *
garrow_take_options_new(void);


/**
 * GArrowSortOrder:
 * @GARROW_SORT_ORDER_ASCENDING: Sort in ascending order.
 * @GARROW_SORT_ORDER_DESCENDING: Sort in descending order.
 *
 * They are corresponding to `arrow::compute::SortOrder` values.
 *
 * Since: 3.0.0
 */
typedef enum {
  GARROW_SORT_ORDER_ASCENDING,
  GARROW_SORT_ORDER_DESCENDING,
} GArrowSortOrder;

/**
 * GArrowNullPlacement:
 * @GARROW_NULL_PLACEMENT_AT_START:
 *   Place nulls and NaNs before any non-null values.
 *   NaNs will come after nulls.
 * @GARROW_NULL_PLACEMENT_AT_END:
 *   Place nulls and NaNs after any non-null values.
 *   NaNs will come before nulls.
 *
 * They are corresponding to `arrow::compute::NullPlacement` values.
 *
 * Since: 12.0.0
 */
typedef enum /*<prefix=GARROW_NULL_PLACEMENT_>*/ {
  GARROW_NULL_PLACEMENT_AT_START,
  GARROW_NULL_PLACEMENT_AT_END,
} GArrowNullPlacement;

#define GARROW_TYPE_ARRAY_SORT_OPTIONS (garrow_array_sort_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowArraySortOptions,
                         garrow_array_sort_options,
                         GARROW,
                         ARRAY_SORT_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowArraySortOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowArraySortOptions *
garrow_array_sort_options_new(GArrowSortOrder order);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_array_sort_options_equal(GArrowArraySortOptions *options,
                                GArrowArraySortOptions *other_options);


#define GARROW_TYPE_SORT_KEY (garrow_sort_key_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSortKey,
                         garrow_sort_key,
                         GARROW,
                         SORT_KEY,
                         GObject)
struct _GArrowSortKeyClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowSortKey *
garrow_sort_key_new(const gchar *target,
                    GArrowSortOrder order,
                    GError **error);

GARROW_AVAILABLE_IN_3_0
gboolean
garrow_sort_key_equal(GArrowSortKey *sort_key,
                      GArrowSortKey *other_sort_key);


#define GARROW_TYPE_SORT_OPTIONS (garrow_sort_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSortOptions,
                         garrow_sort_options,
                         GARROW,
                         SORT_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowSortOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowSortOptions *
garrow_sort_options_new(GList *sort_keys);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_sort_options_equal(GArrowSortOptions *options,
                          GArrowSortOptions *other_options);
GARROW_AVAILABLE_IN_3_0
GList *
garrow_sort_options_get_sort_keys(GArrowSortOptions *options);
GARROW_AVAILABLE_IN_3_0
void
garrow_sort_options_set_sort_keys(GArrowSortOptions *options,
                                  GList *sort_keys);
GARROW_AVAILABLE_IN_3_0
void
garrow_sort_options_add_sort_key(GArrowSortOptions *options,
                                 GArrowSortKey *sort_key);


#define GARROW_TYPE_SET_LOOKUP_OPTIONS (garrow_set_lookup_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSetLookupOptions,
                         garrow_set_lookup_options,
                         GARROW,
                         SET_LOOKUP_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowSetLookupOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowSetLookupOptions *
garrow_set_lookup_options_new(GArrowDatum *value_set);


#define GARROW_TYPE_VARIANCE_OPTIONS (garrow_variance_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowVarianceOptions,
                         garrow_variance_options,
                         GARROW,
                         VARIANCE_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowVarianceOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowVarianceOptions *
garrow_variance_options_new(void);


/**
 * GArrowRoundMode:
 * @GARROW_ROUND_MODE_DOWN:
 *   Round to nearest integer less than or equal in magnitude (aka "floor").
 * @GARROW_ROUND_MODE_UP:
 *   Round to nearest integer greater than or equal in magnitude (aka "ceil").
 * @GARROW_ROUND_TOWARDS_ZERO:
 *   Get the integral part without fractional digits (aka "trunc")
 * @GARROW_ROUND_TOWARDS_INFINITY,
 *   Round negative values with @GARROW_ROUND_MODE_DOWN rule
 *   and positive values with UP rule (aka "away from zero")
 * @GARROW_ROUND_HALF_DOWN,
 *   Round ties with @GARROW_ROUND_MODE_DOWN rule
 *   (also called "round half towards negative infinity")
 * @GARROW_ROUND_HALF_UP,
 *   Round ties with @GARROW_ROUND_MODE_UP rule
 *   (also called "round half towards positive infinity")
 * @GARROW_ROUND_HALF_TOWARDS_ZERO,
 *   Round ties with GARROW_ROUND_MODE_TOWARDS_ZERO rule
 *   (also called "round half away from infinity")
 * @GARROW_ROUND_HALF_TOWARDS_INFINITY,
 *   Round ties with GARROW_ROUND_MODE_TOWARDS_INFINITY rule
 *   (also called "round half away from zero")
 * @GARROW_ROUND_HALF_TO_EVEN,
 *   Round ties to nearest even integer
 * @GARROW_ROUND_HALF_TO_ODD,
 *   Round ties to nearest odd integer
 *
 * They correspond to the values of `arrow::compute::RoundMode`.
 *
 * Since: 7.0.0
 */
typedef enum {
  GARROW_ROUND_DOWN,
  GARROW_ROUND_UP,
  GARROW_ROUND_TOWARDS_ZERO,
  GARROW_ROUND_TOWARDS_INFINITY,
  GARROW_ROUND_HALF_DOWN,
  GARROW_ROUND_HALF_UP,
  GARROW_ROUND_HALF_TOWARDS_ZERO,
  GARROW_ROUND_HALF_TOWARDS_INFINITY,
  GARROW_ROUND_HALF_TO_EVEN,
  GARROW_ROUND_HALF_TO_ODD,
} GArrowRoundMode;


#define GARROW_TYPE_ROUND_OPTIONS (garrow_round_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRoundOptions,
                         garrow_round_options,
                         GARROW,
                         ROUND_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowRoundOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowRoundOptions *
garrow_round_options_new(void);


#define GARROW_TYPE_ROUND_TO_MULTIPLE_OPTIONS   \
  (garrow_round_to_multiple_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRoundToMultipleOptions,
                         garrow_round_to_multiple_options,
                         GARROW,
                         ROUND_TO_MULTIPLE_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowRoundToMultipleOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowRoundToMultipleOptions *
garrow_round_to_multiple_options_new(void);


/**
 * GArrowUTF8NormalizeForm:
 * @GARROW_UTF8_NORMALIZE_FORM_NFC: Normalization Form Canonical Composition.
 * @GARROW_UTF8_NORMALIZE_FORM_NFKC: Normalization Form Compatibility
 *   Composition.
 * @GARROW_UTF8_NORMALIZE_FORM_NFD: Normalization Form Canonical Decomposition.
 * @GARROW_UTF8_NORMALIZE_FORM_NFKD: Normalization Form Compatibility
 *   Decomposition.
 *
 * They correspond to the values of `arrow::compute::Utf8NormalizeOptions::Form`.
 *
 * Since: 8.0.0
 */
typedef enum /*< underscore_name=garrow_utf8_normalize_form >*/ {
  GARROW_UTF8_NORMALIZE_FORM_NFC,
  GARROW_UTF8_NORMALIZE_FORM_NFKC,
  GARROW_UTF8_NORMALIZE_FORM_NFD,
  GARROW_UTF8_NORMALIZE_FORM_NFKD,
} GArrowUTF8NormalizeForm;

#define GARROW_TYPE_UTF8_NORMALIZE_OPTIONS      \
  (garrow_utf8_normalize_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUTF8NormalizeOptions,
                         garrow_utf8_normalize_options,
                         GARROW,
                         UTF8_NORMALIZE_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowUTF8NormalizeOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowUTF8NormalizeOptions *
garrow_utf8_normalize_options_new(void);


/**
 * GArrowQuantileInterpolation:
 * @GARROW_QUANTILE_INTERPOLATION_LINEAR: Linear.
 * @GARROW_QUANTILE_INTERPOLATION_LOWER: Lower.
 * @GARROW_QUANTILE_INTERPOLATION_HIGHER: Higher.
 * @GARROW_QUANTILE_INTERPOLATION_NEAREST: Nearest.
 * @GARROW_QUANTILE_INTERPOLATION_MIDPOINT: Midpoint.
 *
 * They correspond to the values of
 * `arrow::compute::QuantileOptions::Interpolation`.
 *
 * Since: 9.0.0
 */
typedef enum {
  GARROW_QUANTILE_INTERPOLATION_LINEAR,
  GARROW_QUANTILE_INTERPOLATION_LOWER,
  GARROW_QUANTILE_INTERPOLATION_HIGHER,
  GARROW_QUANTILE_INTERPOLATION_NEAREST,
  GARROW_QUANTILE_INTERPOLATION_MIDPOINT,
} GArrowQuantileInterpolation;

#define GARROW_TYPE_QUANTILE_OPTIONS            \
  (garrow_quantile_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowQuantileOptions,
                         garrow_quantile_options,
                         GARROW,
                         QUANTILE_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowQuantileOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_9_0
GArrowQuantileOptions *
garrow_quantile_options_new(void);
GARROW_AVAILABLE_IN_9_0
const gdouble *
garrow_quantile_options_get_qs(GArrowQuantileOptions *options,
                               gsize *n);
GARROW_AVAILABLE_IN_9_0
void
garrow_quantile_options_set_q(GArrowQuantileOptions *options,
                              gdouble q);
GARROW_AVAILABLE_IN_9_0
void
garrow_quantile_options_set_qs(GArrowQuantileOptions *options,
                               const gdouble *qs,
                               gsize n);


/**
 * GArrowRankTiebreader:
 * @GARROW_RANK_TIEBREAKER_MIN:
 *   Ties get the smallest possible rank in sorted order.
 * @GARROW_RANK_TIEBREAKER_MAX:
 *   Ties get the largest possible rank in sorted order.
 * @GARROW_RANK_TIEBREAKER_FIRST:
 *   Ranks are assigned in order of when ties appear in the input.
 *   This ensures the ranks are a stable permutation of the input.
 * @GARROW_RANK_TIEBREAKER_DENSE:
 *   The ranks span a dense [1, M] interval where M is the number
 *   of distinct values in the input.
 *
 * They correspond to the values of
 * `arrow::compute::RankOptions::Tiebreaker`.
 *
 * Since: 12.0.0
 */
typedef enum {
  GARROW_RANK_TIEBREAKER_MIN,
  GARROW_RANK_TIEBREAKER_MAX,
  GARROW_RANK_TIEBREAKER_FIRST,
  GARROW_RANK_TIEBREAKER_DENSE,
} GArrowRankTiebreaker;

#define GARROW_TYPE_RANK_OPTIONS                \
  (garrow_rank_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRankOptions,
                         garrow_rank_options,
                         GARROW,
                         RANK_OPTIONS,
                         GArrowFunctionOptions)
struct _GArrowRankOptionsClass
{
  GArrowFunctionOptionsClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
GArrowRankOptions *
garrow_rank_options_new(void);
GARROW_AVAILABLE_IN_12_0
gboolean
garrow_rank_options_equal(GArrowRankOptions *options,
                          GArrowRankOptions *other_options);
GARROW_AVAILABLE_IN_12_0
GList *
garrow_rank_options_get_sort_keys(GArrowRankOptions *options);
GARROW_AVAILABLE_IN_12_0
void
garrow_rank_options_set_sort_keys(GArrowRankOptions *options,
                                  GList *sort_keys);
GARROW_AVAILABLE_IN_12_0
void
garrow_rank_options_add_sort_key(GArrowRankOptions *options,
                                 GArrowSortKey *sort_key);


GArrowArray *garrow_array_cast(GArrowArray *array,
                               GArrowDataType *target_data_type,
                               GArrowCastOptions *options,
                               GError **error);
GArrowArray *garrow_array_unique(GArrowArray *array,
                                 GError **error);
GArrowDictionaryArray *garrow_array_dictionary_encode(GArrowArray *array,
                                                      GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_array_count(GArrowArray *array,
                          GArrowCountOptions *options,
                          GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowStructArray *garrow_array_count_values(GArrowArray *array,
                                             GError **error);

GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_invert(GArrowBooleanArray *array,
                                                GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_and(GArrowBooleanArray *left,
                                             GArrowBooleanArray *right,
                                             GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_or(GArrowBooleanArray *left,
                                            GArrowBooleanArray *right,
                                            GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_xor(GArrowBooleanArray *left,
                                             GArrowBooleanArray *right,
                                             GError **error);

GARROW_AVAILABLE_IN_0_13
gdouble garrow_numeric_array_mean(GArrowNumericArray *array,
                                  GError **error);

GARROW_AVAILABLE_IN_0_13
gint64 garrow_int8_array_sum(GArrowInt8Array *array,
                             GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint8_array_sum(GArrowUInt8Array *array,
                               GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_int16_array_sum(GArrowInt16Array *array,
                              GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint16_array_sum(GArrowUInt16Array *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_int32_array_sum(GArrowInt32Array *array,
                              GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint32_array_sum(GArrowUInt32Array *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_int64_array_sum(GArrowInt64Array *array,
                              GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint64_array_sum(GArrowUInt64Array *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_13
gdouble garrow_float_array_sum(GArrowFloatArray *array,
                               GError **error);
GARROW_AVAILABLE_IN_0_13
gdouble garrow_double_array_sum(GArrowDoubleArray *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowArray *garrow_array_take(GArrowArray *array,
                               GArrowArray *indices,
                               GArrowTakeOptions *options,
                               GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_array_take_chunked_array(GArrowArray *array,
                                GArrowChunkedArray *indices,
                                GArrowTakeOptions *options,
                                GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_take(GArrowTable *table,
                  GArrowArray *indices,
                  GArrowTakeOptions *options,
                  GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_take_chunked_array(GArrowTable *table,
                                GArrowChunkedArray *indices,
                                GArrowTakeOptions *options,
                                GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_take(GArrowChunkedArray *chunked_array,
                          GArrowArray *indices,
                          GArrowTakeOptions *options,
                          GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_take_chunked_array(GArrowChunkedArray *chunked_array,
                                        GArrowChunkedArray *indices,
                                        GArrowTakeOptions *options,
                                        GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowRecordBatch *
garrow_record_batch_take(GArrowRecordBatch *record_batch,
                         GArrowArray *indices,
                         GArrowTakeOptions *options,
                         GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowArray *
garrow_array_filter(GArrowArray *array,
                    GArrowBooleanArray *filter,
                    GArrowFilterOptions *options,
                    GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowBooleanArray *
garrow_array_is_in(GArrowArray *left,
                   GArrowArray *right,
                   GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowBooleanArray *
garrow_array_is_in_chunked_array(GArrowArray *left,
                                 GArrowChunkedArray *right,
                                 GError **error);


GARROW_AVAILABLE_IN_3_0
GArrowUInt64Array *
garrow_array_sort_indices(GArrowArray *array,
                          GArrowSortOrder order,
                          GError **error);
GARROW_DEPRECATED_IN_3_0_FOR(garrow_array_sort_indices)
GARROW_AVAILABLE_IN_0_15
GArrowUInt64Array *
garrow_array_sort_to_indices(GArrowArray *array,
                             GError **error);

GARROW_AVAILABLE_IN_3_0
GArrowUInt64Array *
garrow_chunked_array_sort_indices(GArrowChunkedArray *chunked_array,
                                  GArrowSortOrder order,
                                  GError **error);


GARROW_AVAILABLE_IN_3_0
GArrowUInt64Array *
garrow_record_batch_sort_indices(GArrowRecordBatch *record_batch,
                                 GArrowSortOptions *options,
                                 GError **error);

GARROW_AVAILABLE_IN_3_0
GArrowUInt64Array *
garrow_table_sort_indices(GArrowTable *table,
                          GArrowSortOptions *options,
                          GError **error);


GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_filter(GArrowTable *table,
                    GArrowBooleanArray *filter,
                    GArrowFilterOptions *options,
                    GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_filter_chunked_array(GArrowTable *table,
                                  GArrowChunkedArray *filter,
                                  GArrowFilterOptions *options,
                                  GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_filter(GArrowChunkedArray *chunked_array,
                            GArrowBooleanArray *filter,
                            GArrowFilterOptions *options,
                            GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_filter_chunked_array(GArrowChunkedArray *chunked_array,
                                          GArrowChunkedArray *filter,
                                          GArrowFilterOptions *options,
                                          GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowRecordBatch *
garrow_record_batch_filter(GArrowRecordBatch *record_batch,
                           GArrowBooleanArray *filter,
                           GArrowFilterOptions *options,
                           GError **error);

G_END_DECLS
