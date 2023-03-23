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

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>

#include <arrow-glib/compute.h>


arrow::Result<arrow::FieldRef>
garrow_field_reference_resolve_raw(const gchar *reference);


arrow::compute::ExecContext *
garrow_execute_context_get_raw(GArrowExecuteContext *context);


GArrowFunctionOptions *
garrow_function_options_new_raw(
  const arrow::compute::FunctionOptions *arrow_options);
arrow::compute::FunctionOptions *
garrow_function_options_get_raw(GArrowFunctionOptions *options);


GArrowFunctionDoc *
garrow_function_doc_new_raw(const arrow::compute::FunctionDoc *arrow_doc);
arrow::compute::FunctionDoc *
garrow_function_doc_get_raw(GArrowFunctionDoc *doc);

GArrowFunction *
garrow_function_new_raw(std::shared_ptr<arrow::compute::Function> *arrow_function);
std::shared_ptr<arrow::compute::Function>
garrow_function_get_raw(GArrowFunction *function);


GArrowExecuteNodeOptions *
garrow_execute_node_options_new_raw(
  arrow::compute::ExecNodeOptions *arrow_options);
arrow::compute::ExecNodeOptions *
garrow_execute_node_options_get_raw(GArrowExecuteNodeOptions *options);


GArrowExecuteNode *
garrow_execute_node_new_raw(arrow::compute::ExecNode *arrow_node);
arrow::compute::ExecNode *
garrow_execute_node_get_raw(GArrowExecuteNode *node);


std::shared_ptr<arrow::compute::ExecPlan>
garrow_execute_plan_get_raw(GArrowExecutePlan *plan);


GArrowCastOptions *
garrow_cast_options_new_raw(const arrow::compute::CastOptions *arrow_options);
arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *options);


GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new_raw(
  const arrow::compute::ScalarAggregateOptions *arrow_options);
arrow::compute::ScalarAggregateOptions *
garrow_scalar_aggregate_options_get_raw(
  GArrowScalarAggregateOptions *options);


GArrowCountOptions *
garrow_count_options_new_raw(
  const arrow::compute::CountOptions *arrow_options);
arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *options);


GArrowFilterOptions *
garrow_filter_options_new_raw(
  const arrow::compute::FilterOptions *arrow_options);
arrow::compute::FilterOptions *
garrow_filter_options_get_raw(GArrowFilterOptions *options);


GArrowTakeOptions *
garrow_take_options_new_raw(
  const arrow::compute::TakeOptions *arrow_options);
arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *options);


GArrowArraySortOptions *
garrow_array_sort_options_new_raw(
  const arrow::compute::ArraySortOptions *arrow_options);
arrow::compute::ArraySortOptions *
garrow_array_sort_options_get_raw(GArrowArraySortOptions *options);


GArrowSortKey *
garrow_sort_key_new_raw(const arrow::compute::SortKey &arrow_sort_key);
arrow::compute::SortKey *
garrow_sort_key_get_raw(GArrowSortKey *sort_key);


GArrowSortOptions *
garrow_sort_options_new_raw(
  const arrow::compute::SortOptions *arrow_options);
arrow::compute::SortOptions *
garrow_sort_options_get_raw(GArrowSortOptions *options);


GArrowSetLookupOptions *
garrow_set_lookup_options_new_raw(
  const arrow::compute::SetLookupOptions *arrow_options);
arrow::compute::SetLookupOptions *
garrow_set_lookup_options_get_raw(GArrowSetLookupOptions *options);


GArrowVarianceOptions *
garrow_variance_options_new_raw(
  const arrow::compute::VarianceOptions *arrow_options);
arrow::compute::VarianceOptions *
garrow_variance_options_get_raw(GArrowVarianceOptions *options);


GArrowRoundOptions *
garrow_round_options_new_raw(
  const arrow::compute::RoundOptions *arrow_options);
arrow::compute::RoundOptions *
garrow_round_options_get_raw(GArrowRoundOptions *options);


GArrowRoundToMultipleOptions *
garrow_round_to_multiple_options_new_raw(
  const arrow::compute::RoundToMultipleOptions *arrow_options);
arrow::compute::RoundToMultipleOptions *
garrow_round_to_multiple_options_get_raw(GArrowRoundToMultipleOptions *options);


GArrowUTF8NormalizeOptions *
garrow_utf8_normalize_options_new_raw(
  const arrow::compute::Utf8NormalizeOptions *arrow_options);
arrow::compute::Utf8NormalizeOptions *
garrow_utf8_normalize_options_get_raw(GArrowUTF8NormalizeOptions *options);


GArrowQuantileOptions *
garrow_quantile_options_new_raw(
  const arrow::compute::QuantileOptions *arrow_options);
arrow::compute::QuantileOptions *
garrow_quantile_options_get_raw(GArrowQuantileOptions *options);


GArrowRankOptions *
garrow_rank_options_new_raw(const arrow::compute::RankOptions *arrow_options);
arrow::compute::RankOptions *
garrow_rank_options_get_raw(GArrowRankOptions *options);
