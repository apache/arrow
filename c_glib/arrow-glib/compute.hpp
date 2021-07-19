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


struct _GArrowFunctionOptionsInterface
{
  GTypeInterface parent_iface;

  arrow::compute::FunctionOptions *(*get_raw)(GArrowFunctionOptions *options);
};


arrow::compute::ExecContext *
garrow_execute_context_get_raw(GArrowExecuteContext *context);

arrow::compute::FunctionOptions *
garrow_function_options_get_raw(GArrowFunctionOptions *options);

GArrowFunction *
garrow_function_new_raw(std::shared_ptr<arrow::compute::Function> *arrow_function);
std::shared_ptr<arrow::compute::Function>
garrow_function_get_raw(GArrowFunction *function);

GArrowCastOptions *garrow_cast_options_new_raw(arrow::compute::CastOptions *arrow_cast_options);
arrow::compute::CastOptions *garrow_cast_options_get_raw(GArrowCastOptions *cast_options);

GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new_raw(
  arrow::compute::ScalarAggregateOptions *arrow_scalar_aggregate_options);
arrow::compute::ScalarAggregateOptions *
garrow_scalar_aggregate_options_get_raw(
  GArrowScalarAggregateOptions *scalar_aggregate_options);

arrow::compute::FilterOptions *
garrow_filter_options_get_raw(GArrowFilterOptions *filter_options);

arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *take_options);

arrow::compute::ArraySortOptions *
garrow_array_sort_options_get_raw(GArrowArraySortOptions *array_sort_options);

arrow::compute::SortKey *
garrow_sort_key_get_raw(GArrowSortKey *sort_key);

arrow::compute::SortOptions *
garrow_sort_options_get_raw(GArrowSortOptions *sort_options);
