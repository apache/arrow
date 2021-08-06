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

#if defined(ARROW_R_WITH_ARROW)

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

  if (func_name == "min_max" || func_name == "sum" || func_name == "mean" ||
      func_name == "count" || func_name == "any" || func_name == "all") {
    using Options = arrow::compute::ScalarAggregateOptions;
    auto out = std::make_shared<Options>(Options::Defaults());
    out->min_count = cpp11::as_cpp<int>(options["na.min_count"]);
    out->skip_nulls = cpp11::as_cpp<bool>(options["na.rm"]);
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
    return out;
  }

  if (func_name == "is_in" || func_name == "index_in") {
    using Options = arrow::compute::SetLookupOptions;
    return std::make_shared<Options>(cpp11::as_cpp<arrow::Datum>(options["value_set"]),
                                     cpp11::as_cpp<bool>(options["skip_nulls"]));
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
      func_name == "match_like") {
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

  if (func_name == "day_of_week") {
    using Options = arrow::compute::DayOfWeekOptions;
    bool one_based_numbering = true;
    if (!Rf_isNull(options["one_based_numbering"])) {
      one_based_numbering = cpp11::as_cpp<bool>(options["one_based_numbering"]);
    }
    return std::make_shared<Options>(one_based_numbering,
                                     cpp11::as_cpp<uint32_t>(options["week_start"]));
  }

  if (func_name == "strptime") {
    using Options = arrow::compute::StrptimeOptions;
    return std::make_shared<Options>(
        cpp11::as_cpp<std::string>(options["format"]),
        cpp11::as_cpp<arrow::TimeUnit::type>(options["unit"]));
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

  if (func_name == "utf8_slice_codeunits") {
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

  if (func_name == "variance" || func_name == "stddev") {
    using Options = arrow::compute::VarianceOptions;
    return std::make_shared<Options>(cpp11::as_cpp<int64_t>(options["ddof"]));
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
SEXP compute__GroupBy(cpp11::list arguments, cpp11::list keys, cpp11::list options) {
  // options is a list of pairs: string function name, list of options

  std::vector<std::shared_ptr<arrow::compute::FunctionOptions>> keep_alives;
  std::vector<arrow::compute::internal::Aggregate> aggregates;

  for (cpp11::list name_opts : options) {
    auto name = cpp11::as_cpp<std::string>(name_opts[0]);
    auto opts = make_compute_options(name, name_opts[1]);

    aggregates.push_back(
        arrow::compute::internal::Aggregate{std::move(name), opts.get()});
    keep_alives.push_back(std::move(opts));
  }

  auto datum_arguments = arrow::r::from_r_list<arrow::Datum>(arguments);
  auto datum_keys = arrow::r::from_r_list<arrow::Datum>(keys);
  auto out = ValueOrStop(arrow::compute::internal::GroupBy(datum_arguments, datum_keys,
                                                           aggregates, gc_context()));
  return from_datum(std::move(out));
}

// [[arrow::export]]
std::vector<std::string> compute__GetFunctionNames() {
  return arrow::compute::GetFunctionRegistry()->GetFunctionNames();
}

#endif
