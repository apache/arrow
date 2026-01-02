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
#include <arrow/acero/exec_plan.h>

#include <arrow-glib/compute.h>

arrow::Result<arrow::FieldRef>
garrow_field_reference_resolve_raw(const gchar *reference);

arrow::compute::ExecContext *
garrow_execute_context_get_raw(GArrowExecuteContext *context);

GArrowFunctionOptions *
garrow_function_options_new_raw(const arrow::compute::FunctionOptions *arrow_options);
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
garrow_execute_node_options_new_raw(arrow::acero::ExecNodeOptions *arrow_options);
arrow::acero::ExecNodeOptions *
garrow_execute_node_options_get_raw(GArrowExecuteNodeOptions *options);

GArrowExecuteNode *
garrow_execute_node_new_raw(arrow::acero::ExecNode *arrow_node,
                            GArrowExecuteNodeOptions *options);
arrow::acero::ExecNode *
garrow_execute_node_get_raw(GArrowExecuteNode *node);

std::shared_ptr<arrow::acero::ExecPlan>
garrow_execute_plan_get_raw(GArrowExecutePlan *plan);

GArrowCastOptions *
garrow_cast_options_new_raw(const arrow::compute::CastOptions *arrow_options);
arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *options);

GArrowScalarAggregateOptions *
garrow_scalar_aggregate_options_new_raw(
  const arrow::compute::ScalarAggregateOptions *arrow_options);
arrow::compute::ScalarAggregateOptions *
garrow_scalar_aggregate_options_get_raw(GArrowScalarAggregateOptions *options);

GArrowCountOptions *
garrow_count_options_new_raw(const arrow::compute::CountOptions *arrow_options);
arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *options);

GArrowFilterOptions *
garrow_filter_options_new_raw(const arrow::compute::FilterOptions *arrow_options);
arrow::compute::FilterOptions *
garrow_filter_options_get_raw(GArrowFilterOptions *options);

GArrowTakeOptions *
garrow_take_options_new_raw(const arrow::compute::TakeOptions *arrow_options);
arrow::compute::TakeOptions *
garrow_take_options_get_raw(GArrowTakeOptions *options);

GArrowArraySortOptions *
garrow_array_sort_options_new_raw(const arrow::compute::ArraySortOptions *arrow_options);
arrow::compute::ArraySortOptions *
garrow_array_sort_options_get_raw(GArrowArraySortOptions *options);

GArrowSortKey *
garrow_sort_key_new_raw(const arrow::compute::SortKey &arrow_sort_key);
arrow::compute::SortKey *
garrow_sort_key_get_raw(GArrowSortKey *sort_key);

GArrowSortOptions *
garrow_sort_options_new_raw(const arrow::compute::SortOptions *arrow_options);
arrow::compute::SortOptions *
garrow_sort_options_get_raw(GArrowSortOptions *options);

GArrowSetLookupOptions *
garrow_set_lookup_options_new_raw(const arrow::compute::SetLookupOptions *arrow_options);
arrow::compute::SetLookupOptions *
garrow_set_lookup_options_get_raw(GArrowSetLookupOptions *options);

GArrowVarianceOptions *
garrow_variance_options_new_raw(const arrow::compute::VarianceOptions *arrow_options);
arrow::compute::VarianceOptions *
garrow_variance_options_get_raw(GArrowVarianceOptions *options);

GArrowRoundOptions *
garrow_round_options_new_raw(const arrow::compute::RoundOptions *arrow_options);
arrow::compute::RoundOptions *
garrow_round_options_get_raw(GArrowRoundOptions *options);

GArrowRoundToMultipleOptions *
garrow_round_to_multiple_options_new_raw(
  const arrow::compute::RoundToMultipleOptions *arrow_options);
arrow::compute::RoundToMultipleOptions *
garrow_round_to_multiple_options_get_raw(GArrowRoundToMultipleOptions *options);

GArrowMatchSubstringOptions *
garrow_match_substring_options_new_raw(
  const arrow::compute::MatchSubstringOptions *arrow_options);
arrow::compute::MatchSubstringOptions *
garrow_match_substring_options_get_raw(GArrowMatchSubstringOptions *options);

GArrowUTF8NormalizeOptions *
garrow_utf8_normalize_options_new_raw(
  const arrow::compute::Utf8NormalizeOptions *arrow_options);
arrow::compute::Utf8NormalizeOptions *
garrow_utf8_normalize_options_get_raw(GArrowUTF8NormalizeOptions *options);

GArrowQuantileOptions *
garrow_quantile_options_new_raw(const arrow::compute::QuantileOptions *arrow_options);
arrow::compute::QuantileOptions *
garrow_quantile_options_get_raw(GArrowQuantileOptions *options);

GArrowIndexOptions *
garrow_index_options_new_raw(const arrow::compute::IndexOptions *arrow_options);
arrow::compute::IndexOptions *
garrow_index_options_get_raw(GArrowIndexOptions *options);

GArrowRankOptions *
garrow_rank_options_new_raw(const arrow::compute::RankOptions *arrow_options);
arrow::compute::RankOptions *
garrow_rank_options_get_raw(GArrowRankOptions *options);

GArrowRunEndEncodeOptions *
garrow_run_end_encode_options_new_raw(
  const arrow::compute::RunEndEncodeOptions *arrow_options);
arrow::compute::RunEndEncodeOptions *
garrow_run_end_encode_options_get_raw(GArrowRunEndEncodeOptions *options);

GArrowStrptimeOptions *
garrow_strptime_options_new_raw(const arrow::compute::StrptimeOptions *arrow_options);
arrow::compute::StrptimeOptions *
garrow_strptime_options_get_raw(GArrowStrptimeOptions *options);

GArrowStrftimeOptions *
garrow_strftime_options_new_raw(const arrow::compute::StrftimeOptions *arrow_options);
arrow::compute::StrftimeOptions *
garrow_strftime_options_get_raw(GArrowStrftimeOptions *options);

GArrowSplitPatternOptions *
garrow_split_pattern_options_new_raw(
  const arrow::compute::SplitPatternOptions *arrow_options);
arrow::compute::SplitPatternOptions *
garrow_split_pattern_options_get_raw(GArrowSplitPatternOptions *options);

GArrowStructFieldOptions *
garrow_struct_field_options_new_raw(
  const arrow::compute::StructFieldOptions *arrow_options);
arrow::compute::StructFieldOptions *
garrow_struct_field_options_get_raw(GArrowStructFieldOptions *options);

GArrowAssumeTimezoneOptions *
garrow_assume_timezone_options_new_raw(
  const arrow::compute::AssumeTimezoneOptions *arrow_options);
arrow::compute::AssumeTimezoneOptions *
garrow_assume_timezone_options_get_raw(GArrowAssumeTimezoneOptions *options);

GArrowCumulativeOptions *
garrow_cumulative_options_new_raw(const arrow::compute::CumulativeOptions *arrow_options);
arrow::compute::CumulativeOptions *
garrow_cumulative_options_get_raw(GArrowCumulativeOptions *options);

GArrowDictionaryEncodeOptions *
garrow_dictionary_encode_options_new_raw(
  const arrow::compute::DictionaryEncodeOptions *arrow_options);
arrow::compute::DictionaryEncodeOptions *
garrow_dictionary_encode_options_get_raw(GArrowDictionaryEncodeOptions *options);

GArrowElementWiseAggregateOptions *
garrow_element_wise_aggregate_options_new_raw(
  const arrow::compute::ElementWiseAggregateOptions *arrow_options);
arrow::compute::ElementWiseAggregateOptions *
garrow_element_wise_aggregate_options_get_raw(GArrowElementWiseAggregateOptions *options);

GArrowDayOfWeekOptions *
garrow_day_of_week_options_new_raw(const arrow::compute::DayOfWeekOptions *arrow_options);
arrow::compute::DayOfWeekOptions *
garrow_day_of_week_options_get_raw(GArrowDayOfWeekOptions *options);

GArrowExtractRegexOptions *
garrow_extract_regex_options_new_raw(
  const arrow::compute::ExtractRegexOptions *arrow_options);
arrow::compute::ExtractRegexOptions *
garrow_extract_regex_options_get_raw(GArrowExtractRegexOptions *options);

GArrowExtractRegexSpanOptions *
garrow_extract_regex_span_options_new_raw(
  const arrow::compute::ExtractRegexSpanOptions *arrow_options);
arrow::compute::ExtractRegexSpanOptions *
garrow_extract_regex_span_options_get_raw(GArrowExtractRegexSpanOptions *options);

GArrowJoinOptions *
garrow_join_options_new_raw(const arrow::compute::JoinOptions *arrow_options);
arrow::compute::JoinOptions *
garrow_join_options_get_raw(GArrowJoinOptions *options);

GArrowListFlattenOptions *
garrow_list_flatten_options_new_raw(
  const arrow::compute::ListFlattenOptions *arrow_options);
arrow::compute::ListFlattenOptions *
garrow_list_flatten_options_get_raw(GArrowListFlattenOptions *options);

GArrowMapLookupOptions *
garrow_map_lookup_options_new_raw(const arrow::compute::MapLookupOptions *arrow_options);
arrow::compute::MapLookupOptions *
garrow_map_lookup_options_get_raw(GArrowMapLookupOptions *options);

GArrowListSliceOptions *
garrow_list_slice_options_new_raw(const arrow::compute::ListSliceOptions *arrow_options);
arrow::compute::ListSliceOptions *
garrow_list_slice_options_get_raw(GArrowListSliceOptions *options);

GArrowModeOptions *
garrow_mode_options_new_raw(const arrow::compute::ModeOptions *arrow_options);
arrow::compute::ModeOptions *
garrow_mode_options_get_raw(GArrowModeOptions *options);

GArrowNullOptions *
garrow_null_options_new_raw(const arrow::compute::NullOptions *arrow_options);
arrow::compute::NullOptions *
garrow_null_options_get_raw(GArrowNullOptions *options);

GArrowPadOptions *
garrow_pad_options_new_raw(const arrow::compute::PadOptions *arrow_options);
arrow::compute::PadOptions *
garrow_pad_options_get_raw(GArrowPadOptions *options);

GArrowPairwiseOptions *
garrow_pairwise_options_new_raw(const arrow::compute::PairwiseOptions *arrow_options);
arrow::compute::PairwiseOptions *
garrow_pairwise_options_get_raw(GArrowPairwiseOptions *options);

GArrowReplaceSliceOptions *
garrow_replace_slice_options_new_raw(
  const arrow::compute::ReplaceSliceOptions *arrow_options);
arrow::compute::ReplaceSliceOptions *
garrow_replace_slice_options_get_raw(GArrowReplaceSliceOptions *options);

GArrowPartitionNthOptions *
garrow_partition_nth_options_new_raw(
  const arrow::compute::PartitionNthOptions *arrow_options);
arrow::compute::PartitionNthOptions *
garrow_partition_nth_options_get_raw(GArrowPartitionNthOptions *options);

GArrowPivotWiderOptions *
garrow_pivot_wider_options_new_raw(
  const arrow::compute::PivotWiderOptions *arrow_options);
arrow::compute::PivotWiderOptions *
garrow_pivot_wider_options_get_raw(GArrowPivotWiderOptions *options);

GArrowRankQuantileOptions *
garrow_rank_quantile_options_new_raw(
  const arrow::compute::RankQuantileOptions *arrow_options);
arrow::compute::RankQuantileOptions *
garrow_rank_quantile_options_get_raw(GArrowRankQuantileOptions *options);

GArrowReplaceSubstringOptions *
garrow_replace_substring_options_new_raw(
  const arrow::compute::ReplaceSubstringOptions *arrow_options);
arrow::compute::ReplaceSubstringOptions *
garrow_replace_substring_options_get_raw(GArrowReplaceSubstringOptions *options);

GArrowRoundBinaryOptions *
garrow_round_binary_options_new_raw(
  const arrow::compute::RoundBinaryOptions *arrow_options);
arrow::compute::RoundBinaryOptions *
garrow_round_binary_options_get_raw(GArrowRoundBinaryOptions *options);

GArrowRoundTemporalOptions *
garrow_round_temporal_options_new_raw(
  const arrow::compute::RoundTemporalOptions *arrow_options);
arrow::compute::RoundTemporalOptions *
garrow_round_temporal_options_get_raw(GArrowRoundTemporalOptions *options);

GArrowSelectKOptions *
garrow_select_k_options_new_raw(const arrow::compute::SelectKOptions *arrow_options);
arrow::compute::SelectKOptions *
garrow_select_k_options_get_raw(GArrowSelectKOptions *options);

GArrowSkewOptions *
garrow_skew_options_new_raw(const arrow::compute::SkewOptions *arrow_options);
arrow::compute::SkewOptions *
garrow_skew_options_get_raw(GArrowSkewOptions *options);

GArrowSliceOptions *
garrow_slice_options_new_raw(const arrow::compute::SliceOptions *arrow_options);
arrow::compute::SliceOptions *
garrow_slice_options_get_raw(GArrowSliceOptions *options);

GArrowTDigestOptions *
garrow_tdigest_options_new_raw(const arrow::compute::TDigestOptions *arrow_options);
arrow::compute::TDigestOptions *
garrow_tdigest_options_get_raw(GArrowTDigestOptions *options);

GArrowTrimOptions *
garrow_trim_options_new_raw(const arrow::compute::TrimOptions *arrow_options);
arrow::compute::TrimOptions *
garrow_trim_options_get_raw(GArrowTrimOptions *options);

GArrowWeekOptions *
garrow_week_options_new_raw(const arrow::compute::WeekOptions *arrow_options);
arrow::compute::WeekOptions *
garrow_week_options_get_raw(GArrowWeekOptions *options);

GArrowWinsorizeOptions *
garrow_winsorize_options_new_raw(const arrow::compute::WinsorizeOptions *arrow_options);
arrow::compute::WinsorizeOptions *
garrow_winsorize_options_get_raw(GArrowWinsorizeOptions *options);

GArrowZeroFillOptions *
garrow_zero_fill_options_new_raw(const arrow::compute::ZeroFillOptions *arrow_options);
arrow::compute::ZeroFillOptions *
garrow_zero_fill_options_get_raw(GArrowZeroFillOptions *options);

GArrowSplitOptions *
garrow_split_options_new_raw(const arrow::compute::SplitOptions *arrow_options);
arrow::compute::SplitOptions *
garrow_split_options_get_raw(GArrowSplitOptions *options);
