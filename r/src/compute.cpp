// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "./arrow_types.h"
#include "./safe-call-into-r.h"

#include <arrow/array/util.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>

std::shared_ptr<arrow::compute::CastOptions> make_cast_options(cpp11::list options);

arrow::compute::ExecContext* gc_context() {
  static arrow::compute::ExecContext context(gc_memory_pool());
  return &context;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> RecordBatch__cast(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& schema, cpp11::list options) {
  auto opts = make_cast_options(options);
  auto nc = batch->num_columns();

  arrow::ArrayVector columns(nc);
  for (int i = 0; i < nc; i++) {
    columns[i] = ValueOrStop(
        arrow::compute::Cast(*batch->column(i), schema->field(i)->type(), *opts));
  }

  return arrow::RecordBatch::Make(schema, batch->num_rows(), std::move(columns));
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__cast(const std::shared_ptr<arrow::Table>& table,
                                          const std::shared_ptr<arrow::Schema>& schema,
                                          cpp11::list options) {
  auto opts = make_cast_options(options);
  auto nc = table->num_columns();

  using ColumnVector = std::vector<std::shared_ptr<arrow::ChunkedArray>>;
  ColumnVector columns(nc);
  for (int i = 0; i < nc; i++) {
    arrow::Datum value(table->column(i));
    arrow::Datum out =
        ValueOrStop(arrow::compute::Cast(value, schema->field(i)->type(), *opts));
    columns[i] = out.chunked_array();
  }
  return arrow::Table::Make(schema, std::move(columns), table->num_rows());
}

template <typename T>
std::shared_ptr<T> MaybeUnbox(const char* class_name, SEXP x) {
  if (Rf_inherits(x, "ArrowObject") && Rf_inherits(x, class_name)) {
    return cpp11::as_cpp<std::shared_ptr<T>>(x);
  }
  return nullptr;
}

namespace cpp11 {

template <>
arrow::Datum as_cpp<arrow::Datum>(SEXP x) {
  if (auto array = MaybeUnbox<arrow::Array>("Array", x)) {
    return array;
  }

  if (auto chunked_array = MaybeUnbox<arrow::ChunkedArray>("ChunkedArray", x)) {
    return chunked_array;
  }

  if (auto batch = MaybeUnbox<arrow::RecordBatch>("RecordBatch", x)) {
    return batch;
  }

  if (auto table = MaybeUnbox<arrow::Table>("Table", x)) {
    return table;
  }

  if (auto scalar = MaybeUnbox<arrow::Scalar>("Scalar", x)) {
    return scalar;
  }

  // This assumes that R objects have already been converted to Arrow objects;
  // that seems right but should we do the wrapping here too/instead?
  cpp11::stop("to_datum: Not implemented for type %s", Rf_type2char(TYPEOF(x)));
}
}  // namespace cpp11

SEXP from_datum(arrow::Datum datum) {
  switch (datum.kind()) {
    case arrow::Datum::SCALAR:
      return cpp11::to_r6(datum.scalar());

    case arrow::Datum::ARRAY:
      return cpp11::to_r6(datum.make_array());

    case arrow::Datum::CHUNKED_ARRAY:
      return cpp11::to_r6(datum.chunked_array());

    case arrow::Datum::RECORD_BATCH:
      return cpp11::to_r6(datum.record_batch());

    case arrow::Datum::TABLE:
      return cpp11::to_r6(datum.table());

    default:
      break;
  }

  cpp11::stop("from_datum: Not implemented for Datum %s", datum.ToString().c_str());
}

std::shared_ptr<arrow::compute::FunctionOptions> make_compute_options(
    std::string func_name, cpp11::list options) {
  if (func_name == "filter") {
    using Options = arrow::compute::FilterOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    SEXP keep_na = options["keep_na"];
    if (!Rf_isNull(keep_na) && cpp11::as_cpp<bool>(keep_na)) {
      out->null_selection_behavior = Options::EMIT_NULL;
    }
    return out;
  }

  if (func_name == "take") {
    using Options = arrow::compute::TakeOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    return out;
  }

  if (func_name == "array_sort_indices") {
    using Order = arrow::compute::SortOrder;
    using Options = arrow::compute::ArraySortOptions;
    // false means descending, true means ascending
    auto order = cpp11::as_cpp<bool>(options["order"]);
    auto out =
        std::make_shared<Options>(Options(order ? Order::Descending : Order::Ascending));
    return out;
  }

  if (func_name == "sort_indices") {
    using Key = arrow::compute::SortKey;
    using Order = arrow::compute::SortOrder;
    using Options = arrow::compute::SortOptions;
    auto names = cpp11::as_cpp<std::vector<std::string>>(options["names"]);
    // false means descending, true means ascending
    // cpp11 does not support bool here so use int
    auto orders = cpp11::as_cpp<std::vector<int>>(options["orders"]);
    std::vector<Key> keys;
    for (size_t i = 0; i < names.size(); i++) {
      keys.push_back(
          Key(names[i], (orders[i] > 0) ? Order::Descending : Order::Ascending));
    }
    auto out = std::make_shared<Options>(Options(keys));
    return out;
  }

  if (func_name == "all" || func_name == "hash_all" || func_name == "any" ||
      func_name == "hash_any" || func_name == "approximate_median" ||
      func_name == "hash_approximate_median" || func_name == "mean" ||
      func_name == "hash_mean" || func_name == "min_max" || func_name == "hash_min_max" ||
      func_name == "min" || func_name == "hash_min" || func_name == "max" ||
      func_name == "hash_max" || func_name == "sum" || func_name == "hash_sum") {
    using Options = arrow::compute::ScalarAggregateOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["min_count"])) {
      out->min_count = cpp11::as_cpp<int>(options["min_count"]);
    }
    if (!Rf_isNull(options["skip_nulls"])) {
      out->skip_nulls = cpp11::as_cpp<bool>(options["skip_nulls"]);
    }
    return out;
  }

  if (func_name == "tdigest" || func_name == "hash_tdigest") {
    using Options = arrow::compute::TDigestOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["q"])) {
      out->q = cpp11::as_cpp<std::vector<double>>(options["q"]);
    }
    if (!Rf_isNull(options["skip_nulls"])) {
      out->skip_nulls = cpp11::as_cpp<bool>(options["skip_nulls"]);
    }
    return out;
  }

  if (func_name == "count") {
    using Options = arrow::compute::CountOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    out->mode =
        cpp11::as_cpp<bool>(options["na.rm"]) ? Options::ONLY_VALID : Options::ONLY_NULL;
    return out;
  }

  if (func_name == "count_distinct" || func_name == "hash_count_distinct") {
    using Options = arrow::compute::CountOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    out->mode =
        cpp11::as_cpp<bool>(options["na.rm"]) ? Options::ONLY_VALID : Options::ALL;
    return out;
  }

  if (func_name == "min_element_wise" || func_name == "max_element_wise") {
    using Options = arrow::compute::ElementWiseAggregateOptions;
    bool skip_nulls = true;
    if (!Rf_isNull(options["skip_nulls"])) {
      skip_nulls = cpp11::as_cpp<bool>(options["skip_nulls"]);
    }
    return std::make_shared<Options>(skip_nulls);
  }

  if (func_name == "quantile") {
    using Options = arrow::compute::QuantileOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    SEXP q = options["q"];
    if (!Rf_isNull(q) && TYPEOF(q) == REALSXP) {
      out->q = cpp11::as_cpp<std::vector<double>>(q);
    }
    SEXP interpolation = options["interpolation"];
    if (!Rf_isNull(interpolation) && TYPEOF(interpolation) == INTSXP &&
        XLENGTH(interpolation) == 1) {
      out->interpolation =
          cpp11::as_cpp<enum arrow::compute::QuantileOptions::Interpolation>(
              interpolation);
    }
    if (!Rf_isNull(options["min_count"])) {
      out->min_count = cpp11::as_cpp<int64_t>(options["min_count"]);
    }
    if (!Rf_isNull(options["skip_nulls"])) {
      out->skip_nulls = cpp11::as_cpp<int64_t>(options["skip_nulls"]);
    }
    return out;
  }

  if (func_name == "is_in" || func_name == "index_in") {
    using Options = arrow::compute::SetLookupOptions;
    return std::make_shared<Options>(cpp11::as_cpp<arrow::Datum>(options["value_set"]),
                                     cpp11::as_cpp<bool>(options["skip_nulls"]));
  }

  if (func_name == "index") {
    using Options = arrow::compute::IndexOptions;
    return std::make_shared<Options>(
        cpp11::as_cpp<std::shared_ptr<arrow::Scalar>>(options["value"]));
  }

  if (func_name == "is_null") {
    using Options = arrow::compute::NullOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["nan_is_null"])) {
      out->nan_is_null = cpp11::as_cpp<bool>(options["nan_is_null"]);
    }
    return out;
  }

  if (func_name == "dictionary_encode") {
    using Options = arrow::compute::DictionaryEncodeOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["null_encoding_behavior"])) {
      out->null_encoding_behavior = cpp11::as_cpp<
          enum arrow::compute::DictionaryEncodeOptions::NullEncodingBehavior>(
          options["null_encoding_behavior"]);
    }
    return out;
  }

  if (func_name == "cast") {
    return make_cast_options(options);
  }

  if (func_name == "binary_join_element_wise") {
    using Options = arrow::compute::JoinOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["null_handling"])) {
      out->null_handling =
          cpp11::as_cpp<enum arrow::compute::JoinOptions::NullHandlingBehavior>(
              options["null_handling"]);
    }
    if (!Rf_isNull(options["null_replacement"])) {
      out->null_replacement = cpp11::as_cpp<std::string>(options["null_replacement"]);
    }
    return out;
  }

  if (func_name == "make_struct") {
    using Options = arrow::compute::MakeStructOptions;
    // TODO (ARROW-13371): accept `field_nullability` and `field_metadata` options
    return std::make_shared<Options>(
        cpp11::as_cpp<std::vector<std::string>>(options["field_names"]));
  }

  if (func_name == "match_substring" || func_name == "match_substring_regex" ||
      func_name == "find_substring" || func_name == "find_substring_regex" ||
      func_name == "match_like" || func_name == "starts_with" ||
      func_name == "ends_with" || func_name == "count_substring" ||
      func_name == "count_substring_regex") {
    using Options = arrow::compute::MatchSubstringOptions;
    bool ignore_case = false;
    if (!Rf_isNull(options["ignore_case"])) {
      ignore_case = cpp11::as_cpp<bool>(options["ignore_case"]);
    }
    return std::make_shared<Options>(cpp11::as_cpp<std::string>(options["pattern"]),
                                     ignore_case);
  }

  if (func_name == "replace_substring" || func_name == "replace_substring_regex") {
    using Options = arrow::compute::ReplaceSubstringOptions;
    int64_t max_replacements = -1;
    if (!Rf_isNull(options["max_replacements"])) {
      max_replacements = cpp11::as_cpp<int64_t>(options["max_replacements"]);
    }
    return std::make_shared<Options>(cpp11::as_cpp<std::string>(options["pattern"]),
                                     cpp11::as_cpp<std::string>(options["replacement"]),
                                     max_replacements);
  }

  if (func_name == "extract_regex") {
    using Options = arrow::compute::ExtractRegexOptions;
    return std::make_shared<Options>(cpp11::as_cpp<std::string>(options["pattern"]));
  }

  if (func_name == "day_of_week") {
    using Options = arrow::compute::DayOfWeekOptions;
    bool count_from_zero = false;
    if (!Rf_isNull(options["count_from_zero"])) {
      count_from_zero = cpp11::as_cpp<bool>(options["count_from_zero"]);
    }
    return std::make_shared<Options>(count_from_zero,
                                     cpp11::as_cpp<uint32_t>(options["week_start"]));
  }

  if (func_name == "iso_week") {
    return std::make_shared<arrow::compute::WeekOptions>(
        arrow::compute::WeekOptions::ISODefaults());
  }

  if (func_name == "us_week") {
    return std::make_shared<arrow::compute::WeekOptions>(
        arrow::compute::WeekOptions::USDefaults());
  }

  if (func_name == "week") {
    using Options = arrow::compute::WeekOptions;
    bool week_starts_monday = true;
    bool count_from_zero = false;
    bool first_week_is_fully_in_year = false;
    if (!Rf_isNull(options["week_starts_monday"])) {
      week_starts_monday = cpp11::as_cpp<bool>(options["week_starts_monday"]);
    }
    if (!Rf_isNull(options["count_from_zero"])) {
      count_from_zero = cpp11::as_cpp<bool>(options["count_from_zero"]);
    }
    if (!Rf_isNull(options["first_week_is_fully_in_year"])) {
      count_from_zero = cpp11::as_cpp<bool>(options["first_week_is_fully_in_year"]);
    }
    return std::make_shared<Options>(week_starts_monday, count_from_zero,
                                     first_week_is_fully_in_year);
  }

  if (func_name == "strptime") {
    using Options = arrow::compute::StrptimeOptions;
    bool error_is_null = false;
    if (!Rf_isNull(options["error_is_null"])) {
      error_is_null = cpp11::as_cpp<bool>(options["error_is_null"]);
    }
    return std::make_shared<Options>(
        cpp11::as_cpp<std::string>(options["format"]),
        cpp11::as_cpp<arrow::TimeUnit::type>(options["unit"]), error_is_null);
  }

  if (func_name == "strftime") {
    using Options = arrow::compute::StrftimeOptions;
    return std::make_shared<Options>(
        Options(cpp11::as_cpp<std::string>(options["format"]),
                cpp11::as_cpp<std::string>(options["locale"])));
  }

  if (func_name == "assume_timezone") {
    using Options = arrow::compute::AssumeTimezoneOptions;
    enum Options::Ambiguous ambiguous = Options::AMBIGUOUS_RAISE;
    enum Options::Nonexistent nonexistent = Options::NONEXISTENT_RAISE;

    if (!Rf_isNull(options["ambiguous"])) {
      ambiguous = cpp11::as_cpp<enum Options::Ambiguous>(options["ambiguous"]);
    }
    if (!Rf_isNull(options["nonexistent"])) {
      nonexistent = cpp11::as_cpp<enum Options::Nonexistent>(options["nonexistent"]);
    }

    return std::make_shared<Options>(cpp11::as_cpp<std::string>(options["timezone"]),
                                     ambiguous, nonexistent);
  }

  if (func_name == "split_pattern" || func_name == "split_pattern_regex") {
    using Options = arrow::compute::SplitPatternOptions;
    int64_t max_splits = -1;
    if (!Rf_isNull(options["max_splits"])) {
      max_splits = cpp11::as_cpp<int64_t>(options["max_splits"]);
    }
    bool reverse = false;
    if (!Rf_isNull(options["reverse"])) {
      reverse = cpp11::as_cpp<bool>(options["reverse"]);
    }
    return std::make_shared<Options>(cpp11::as_cpp<std::string>(options["pattern"]),
                                     max_splits, reverse);
  }

  if (func_name == "utf8_lpad" || func_name == "utf8_rpad" ||
      func_name == "utf8_center" || func_name == "ascii_lpad" ||
      func_name == "ascii_rpad" || func_name == "ascii_center") {
    using Options = arrow::compute::PadOptions;
    return std::make_shared<Options>(cpp11::as_cpp<int64_t>(options["width"]),
                                     cpp11::as_cpp<std::string>(options["padding"]));
  }

  if (func_name == "utf8_split_whitespace" || func_name == "ascii_split_whitespace") {
    using Options = arrow::compute::SplitOptions;
    int64_t max_splits = -1;
    if (!Rf_isNull(options["max_splits"])) {
      max_splits = cpp11::as_cpp<int64_t>(options["max_splits"]);
    }
    bool reverse = false;
    if (!Rf_isNull(options["reverse"])) {
      reverse = cpp11::as_cpp<bool>(options["reverse"]);
    }
    return std::make_shared<Options>(max_splits, reverse);
  }

  if (func_name == "utf8_trim" || func_name == "utf8_ltrim" ||
      func_name == "utf8_rtrim" || func_name == "ascii_trim" ||
      func_name == "ascii_ltrim" || func_name == "ascii_rtrim") {
    using Options = arrow::compute::TrimOptions;
    return std::make_shared<Options>(cpp11::as_cpp<std::string>(options["characters"]));
  }

  if (func_name == "utf8_slice_codeunits" || func_name == "binary_slice") {
    using Options = arrow::compute::SliceOptions;

    int64_t step = 1;
    if (!Rf_isNull(options["step"])) {
      step = cpp11::as_cpp<int64_t>(options["step"]);
    }

    int64_t stop = std::numeric_limits<int32_t>::max();
    if (!Rf_isNull(options["stop"])) {
      stop = cpp11::as_cpp<int64_t>(options["stop"]);
    }

    return std::make_shared<Options>(cpp11::as_cpp<int64_t>(options["start"]), stop,
                                     step);
  }

  if (func_name == "utf8_replace_slice" || func_name == "binary_replace_slice") {
    using Options = arrow::compute::ReplaceSliceOptions;

    return std::make_shared<Options>(cpp11::as_cpp<int64_t>(options["start"]),
                                     cpp11::as_cpp<int64_t>(options["stop"]),
                                     cpp11::as_cpp<std::string>(options["replacement"]));
  }

  if (func_name == "variance" || func_name == "stddev" || func_name == "hash_variance" ||
      func_name == "hash_stddev") {
    using Options = arrow::compute::VarianceOptions;
    auto out = std::make_shared<Options>();
    out->ddof = cpp11::as_cpp<int64_t>(options["ddof"]);
    if (!Rf_isNull(options["min_count"])) {
      out->min_count = cpp11::as_cpp<int64_t>(options["min_count"]);
    }
    if (!Rf_isNull(options["skip_nulls"])) {
      out->skip_nulls = cpp11::as_cpp<bool>(options["skip_nulls"]);
    }
    return out;
  }

  if (func_name == "mode") {
    using Options = arrow::compute::ModeOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["n"])) {
      out->n = cpp11::as_cpp<int64_t>(options["n"]);
    }
    if (!Rf_isNull(options["min_count"])) {
      out->min_count = cpp11::as_cpp<uint32_t>(options["min_count"]);
    }
    if (!Rf_isNull(options["skip_nulls"])) {
      out->skip_nulls = cpp11::as_cpp<bool>(options["skip_nulls"]);
    }
    return out;
  }

  if (func_name == "partition_nth_indices") {
    using Options = arrow::compute::PartitionNthOptions;
    return std::make_shared<Options>(cpp11::as_cpp<int64_t>(options["pivot"]));
  }

  if (func_name == "round") {
    using Options = arrow::compute::RoundOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["ndigits"])) {
      out->ndigits = cpp11::as_cpp<int64_t>(options["ndigits"]);
    }
    SEXP round_mode = options["round_mode"];
    if (!Rf_isNull(round_mode)) {
      out->round_mode = cpp11::as_cpp<enum arrow::compute::RoundMode>(round_mode);
    }
    return out;
  }

  if (func_name == "round_temporal" || func_name == "floor_temporal" ||
      func_name == "ceil_temporal") {
    using Options = arrow::compute::RoundTemporalOptions;

    int64_t multiple = 1;
    enum arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY;
    bool week_starts_monday = true;
    bool ceil_is_strictly_greater = true;
    bool calendar_based_origin = true;

    if (!Rf_isNull(options["multiple"])) {
      multiple = cpp11::as_cpp<int64_t>(options["multiple"]);
    }
    if (!Rf_isNull(options["unit"])) {
      unit = cpp11::as_cpp<enum arrow::compute::CalendarUnit>(options["unit"]);
    }
    if (!Rf_isNull(options["week_starts_monday"])) {
      week_starts_monday = cpp11::as_cpp<bool>(options["week_starts_monday"]);
    }
    if (!Rf_isNull(options["ceil_is_strictly_greater"])) {
      ceil_is_strictly_greater = cpp11::as_cpp<bool>(options["ceil_is_strictly_greater"]);
    }
    if (!Rf_isNull(options["calendar_based_origin"])) {
      calendar_based_origin = cpp11::as_cpp<bool>(options["calendar_based_origin"]);
    }
    return std::make_shared<Options>(multiple, unit, week_starts_monday,
                                     ceil_is_strictly_greater, calendar_based_origin);
  }

  if (func_name == "round_to_multiple") {
    using Options = arrow::compute::RoundToMultipleOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    if (!Rf_isNull(options["multiple"])) {
      out->multiple = std::make_shared<arrow::DoubleScalar>(
          cpp11::as_cpp<double>(options["multiple"]));
    }
    SEXP round_mode = options["round_mode"];
    if (!Rf_isNull(round_mode)) {
      out->round_mode = cpp11::as_cpp<enum arrow::compute::RoundMode>(round_mode);
    }
    return out;
  }

  if (func_name == "struct_field") {
    using Options = arrow::compute::StructFieldOptions;
    if (!Rf_isNull(options["indices"])) {
      return std::make_shared<Options>(
          cpp11::as_cpp<std::vector<int>>(options["indices"]));
    } else {
      // field_ref
      return std::make_shared<Options>(
          *cpp11::as_cpp<std::shared_ptr<arrow::compute::Expression>>(
               options["field_ref"])
               ->field_ref());
    }
  }

  return nullptr;
}

std::shared_ptr<arrow::compute::CastOptions> make_cast_options(cpp11::list options) {
  using Options = arrow::compute::CastOptions;
  auto out = std::make_shared<Options>(true);
  SEXP to_type = options["to_type"];
  if (!Rf_isNull(to_type) && cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(to_type)) {
    out->to_type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(to_type);
  }

  SEXP allow_float_truncate = options["allow_float_truncate"];
  if (!Rf_isNull(allow_float_truncate) && cpp11::as_cpp<bool>(allow_float_truncate)) {
    out->allow_float_truncate = cpp11::as_cpp<bool>(allow_float_truncate);
  }

  SEXP allow_time_truncate = options["allow_time_truncate"];
  if (!Rf_isNull(allow_time_truncate) && cpp11::as_cpp<bool>(allow_time_truncate)) {
    out->allow_time_truncate = cpp11::as_cpp<bool>(allow_time_truncate);
  }

  SEXP allow_int_overflow = options["allow_int_overflow"];
  if (!Rf_isNull(allow_int_overflow) && cpp11::as_cpp<bool>(allow_int_overflow)) {
    out->allow_int_overflow = cpp11::as_cpp<bool>(allow_int_overflow);
  }
  return out;
}

// [[arrow::export]]
SEXP compute__CallFunction(std::string func_name, cpp11::list args, cpp11::list options) {
  auto opts = make_compute_options(func_name, options);
  auto datum_args = arrow::r::from_r_list<arrow::Datum>(args);
  auto out = ValueOrStop(
      arrow::compute::CallFunction(func_name, datum_args, opts.get(), gc_context()));
  return from_datum(std::move(out));
}

// [[arrow::export]]
std::vector<std::string> compute__GetFunctionNames() {
  return arrow::compute::GetFunctionRegistry()->GetFunctionNames();
}

class RScalarUDFKernelState : public arrow::compute::KernelState {
 public:
  RScalarUDFKernelState(cpp11::sexp exec_func, cpp11::sexp resolver)
      : exec_func_(exec_func), resolver_(resolver) {}

  cpp11::sexp exec_func_;
  cpp11::sexp resolver_;
};

arrow::Result<arrow::TypeHolder> ResolveScalarUDFOutputType(
    arrow::compute::KernelContext* context,
    const std::vector<arrow::TypeHolder>& input_types) {
  return SafeCallIntoR<arrow::TypeHolder>(
      [&]() -> arrow::TypeHolder {
        auto kernel =
            reinterpret_cast<const arrow::compute::ScalarKernel*>(context->kernel());
        auto state = std::dynamic_pointer_cast<RScalarUDFKernelState>(kernel->data);

        cpp11::writable::list input_types_sexp(input_types.size());
        for (size_t i = 0; i < input_types.size(); i++) {
          input_types_sexp[i] =
              cpp11::to_r6<arrow::DataType>(input_types[i].GetSharedPtr());
        }

        cpp11::sexp output_type_sexp =
            cpp11::function(state->resolver_)(input_types_sexp);
        if (!Rf_inherits(output_type_sexp, "DataType")) {
          cpp11::stop(
              "Function specified as arrow_scalar_function() out_type argument must "
              "return a DataType");
        }

        return arrow::TypeHolder(
            cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(output_type_sexp));
      },
      "resolve scalar user-defined function output data type");
}

arrow::Status CallRScalarUDF(arrow::compute::KernelContext* context,
                             const arrow::compute::ExecSpan& span,
                             arrow::compute::ExecResult* result) {
  if (result->is_array_span()) {
    return arrow::Status::NotImplemented("ArraySpan result from R scalar UDF");
  }

  return SafeCallIntoRVoid(
      [&]() {
        auto kernel =
            reinterpret_cast<const arrow::compute::ScalarKernel*>(context->kernel());
        auto state = std::dynamic_pointer_cast<RScalarUDFKernelState>(kernel->data);

        cpp11::writable::list args_sexp(span.num_values());

        for (int i = 0; i < span.num_values(); i++) {
          const arrow::compute::ExecValue& exec_val = span[i];
          if (exec_val.is_array()) {
            args_sexp[i] = cpp11::to_r6<arrow::Array>(exec_val.array.ToArray());
          } else if (exec_val.is_scalar()) {
            args_sexp[i] = cpp11::to_r6<arrow::Scalar>(exec_val.scalar->GetSharedPtr());
          }
        }

        cpp11::sexp batch_length_sexp = cpp11::as_sexp(span.length);

        std::shared_ptr<arrow::DataType> output_type = result->type()->GetSharedPtr();
        cpp11::sexp output_type_sexp = cpp11::to_r6<arrow::DataType>(output_type);
        cpp11::writable::list udf_context = {batch_length_sexp, output_type_sexp};
        udf_context.names() = {"batch_length", "output_type"};

        cpp11::sexp func_result_sexp =
            cpp11::function(state->exec_func_)(udf_context, args_sexp);

        if (Rf_inherits(func_result_sexp, "Array")) {
          auto array = cpp11::as_cpp<std::shared_ptr<arrow::Array>>(func_result_sexp);

          // Error for an Array result of the wrong type
          if (!result->type()->Equals(array->type())) {
            return cpp11::stop(
                "Expected return Array or Scalar with type '%s' from user-defined "
                "function but got Array with type '%s'",
                result->type()->ToString().c_str(), array->type()->ToString().c_str());
          }

          result->value = std::move(array->data());
        } else if (Rf_inherits(func_result_sexp, "Scalar")) {
          auto scalar = cpp11::as_cpp<std::shared_ptr<arrow::Scalar>>(func_result_sexp);

          // handle a Scalar result of the wrong type
          if (!result->type()->Equals(scalar->type)) {
            return cpp11::stop(
                "Expected return Array or Scalar with type '%s' from user-defined "
                "function but got Scalar with type '%s'",
                result->type()->ToString().c_str(), scalar->type->ToString().c_str());
          }

          auto array = ValueOrStop(
              arrow::MakeArrayFromScalar(*scalar, span.length, context->memory_pool()));
          result->value = std::move(array->data());
        } else {
          cpp11::stop("arrow_scalar_function must return an Array or Scalar");
        }
      },
      "execute scalar user-defined function");
}

// [[arrow::export]]
void RegisterScalarUDF(std::string name, cpp11::list func_sexp) {
  cpp11::list in_type_r(func_sexp["in_type"]);
  cpp11::list out_type_r(func_sexp["out_type"]);
  R_xlen_t n_kernels = in_type_r.size();

  if (n_kernels == 0) {
    cpp11::stop("Can't register user-defined function with zero kernels");
  }

  // Compute the Arity from the list of input kernels. We don't currently handle
  // variable numbers of arguments in a user-defined function.
  int64_t n_args =
      cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(in_type_r[0])->num_fields();
  for (R_xlen_t i = 1; i < n_kernels; i++) {
    auto in_types = cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(in_type_r[i]);
    if (in_types->num_fields() != n_args) {
      cpp11::stop(
          "Kernels for user-defined function must accept the same number of arguments");
    }
  }

  arrow::compute::Arity arity(n_args, false);

  // The function documentation isn't currently accessible from R but is required
  // for the C++ function constructor.
  std::vector<std::string> dummy_argument_names(n_args);
  for (int64_t i = 0; i < n_args; i++) {
    dummy_argument_names[i] = "arg";
  }
  const arrow::compute::FunctionDoc dummy_function_doc{
      "A user-defined R function", "returns something", std::move(dummy_argument_names)};

  auto func =
      std::make_shared<arrow::compute::ScalarFunction>(name, arity, dummy_function_doc);

  for (R_xlen_t i = 0; i < n_kernels; i++) {
    auto in_types = cpp11::as_cpp<std::shared_ptr<arrow::Schema>>(in_type_r[i]);
    cpp11::sexp out_type_func = out_type_r[i];

    std::vector<arrow::compute::InputType> compute_in_types(in_types->num_fields());
    for (int64_t j = 0; j < in_types->num_fields(); j++) {
      compute_in_types[j] = arrow::compute::InputType(in_types->field(j)->type());
    }

    arrow::compute::OutputType out_type((&ResolveScalarUDFOutputType));

    auto signature = std::make_shared<arrow::compute::KernelSignature>(
        std::move(compute_in_types), std::move(out_type), true);
    arrow::compute::ScalarKernel kernel(signature, &CallRScalarUDF);
    kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    kernel.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.data =
        std::make_shared<RScalarUDFKernelState>(func_sexp["wrapper_fun"], out_type_func);

    StopIfNotOk(func->AddKernel(std::move(kernel)));
  }

  auto registry = arrow::compute::GetFunctionRegistry();
  StopIfNotOk(registry->AddFunction(std::move(func), true));
}
