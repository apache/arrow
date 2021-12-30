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

#include <arrow-glib/compute.h>


arrow::Result<arrow::FieldRef>
garrow_field_reference_resolve_raw(const gchar *reference);


arrow::compute::ExecContext *
garrow_execute_context_get_raw(GArrowExecuteContext *context);

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
garrow_cast_options_new_raw(arrow::compute::CastOptions *arrow_options);
arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *options);


GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new_raw(
  arrow::compute::ScalarAggregateOptions *arrow_options);
arrow::compute::ScalarAggregateOptions *
garrow_scalar_aggregate_options_get_raw(
  GArrowScalarAggregateOptions *options);


arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *options);


arrow::compute::FilterOptions *
garrow_filter_options_get_raw(GArrowFilterOptions *options);


arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *options);


arrow::compute::ArraySortOptions *
garrow_array_sort_options_get_raw(GArrowArraySortOptions *options);


GArrowSortKey *
garrow_sort_key_new_raw(const arrow::compute::SortKey &arrow_sort_key);
arrow::compute::SortKey *
garrow_sort_key_get_raw(GArrowSortKey *sort_key);


arrow::compute::SortOptions *
garrow_sort_options_get_raw(GArrowSortOptions *options);


arrow::compute::SetLookupOptions *
garrow_set_lookup_options_get_raw(GArrowSetLookupOptions *options);


arrow::compute::VarianceOptions *
garrow_variance_options_get_raw(GArrowVarianceOptions *options);


arrow::compute::RoundOptions *
garrow_round_options_get_raw(GArrowRoundOptions *options);
