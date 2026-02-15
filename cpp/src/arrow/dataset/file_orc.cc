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

#include "arrow/dataset/file_orc.h"

#include <memory>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/compute/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"
#include "arrow/io/file.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/thread_pool.h"
#include <limits>
#include <numeric>

namespace arrow {

using internal::checked_pointer_cast;

namespace dataset {

namespace {

Result<std::unique_ptr<arrow::adapters::orc::ORCFileReader>> OpenORCReader(
    const FileSource& source,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr) {
  ARROW_ASSIGN_OR_RAISE(auto input, source.Open());

  arrow::MemoryPool* pool;
  if (scan_options) {
    pool = scan_options->pool;
  } else {
    pool = default_memory_pool();
  }

  auto reader = arrow::adapters::orc::ORCFileReader::Open(std::move(input), pool);
  auto status = reader.status();
  if (!status.ok()) {
    return status.WithMessage("Could not open ORC input source '", source.path(),
                              "': ", status.message());
  }
  return reader;
}

struct ResolvedFieldRef {
  FieldRef field_ref;
  std::shared_ptr<Field> field;
  uint32_t orc_column_id;
};

compute::Expression DeriveFieldGuarantee(
    const ResolvedFieldRef& resolved_field,
    const ::orc::ColumnStatistics* col_stats) {
  if (!col_stats) {
    return compute::literal(true);
  }

  auto field_expr = compute::field_ref(resolved_field.field_ref);
  const bool has_null = col_stats->hasNull();
  const bool is_all_null = has_null && col_stats->getNumberOfValues() == 0;
  if (is_all_null) {
    return compute::is_null(field_expr);
  }

  const auto* int_stats = dynamic_cast<const ::orc::IntegerColumnStatistics*>(col_stats);
  if (!int_stats || !int_stats->hasMinimum() || !int_stats->hasMaximum()) {
    return compute::literal(true);
  }

  int64_t min_value = int_stats->getMinimum();
  int64_t max_value = int_stats->getMaximum();
  if (min_value > max_value) {
    return compute::literal(true);
  }

  std::shared_ptr<Scalar> min_scalar;
  std::shared_ptr<Scalar> max_scalar;
  if (resolved_field.field->type()->id() == Type::INT32) {
    if (min_value < std::numeric_limits<int32_t>::min() ||
        max_value > std::numeric_limits<int32_t>::max()) {
      // Keep evaluation conservative when ORC integer stats exceed INT32 bounds.
      return compute::literal(true);
    }
    min_scalar = std::make_shared<Int32Scalar>(static_cast<int32_t>(min_value));
    max_scalar = std::make_shared<Int32Scalar>(static_cast<int32_t>(max_value));
  } else {
    min_scalar = std::make_shared<Int64Scalar>(min_value);
    max_scalar = std::make_shared<Int64Scalar>(max_value);
  }

  auto min_expr = compute::greater_equal(field_expr, compute::literal(*min_scalar));
  auto max_expr = compute::less_equal(field_expr, compute::literal(*max_scalar));
  auto range_expr = compute::and_(std::move(min_expr), std::move(max_expr));

  if (has_null) {
    return compute::or_(std::move(range_expr), compute::is_null(field_expr));
  }
  return range_expr;
}

/// \brief A ScanTask backed by an ORC file.
class OrcScanTask {
 public:
  OrcScanTask(std::shared_ptr<FileFragment> fragment,
              std::shared_ptr<ScanOptions> options)
      : fragment_(std::move(fragment)), options_(std::move(options)) {}

  Result<RecordBatchIterator> Execute() {
    struct Impl {
      static Result<RecordBatchIterator> Make(const FileSource& source,
                                              const FileFormat& format,
                                              const ScanOptions& scan_options,
                                              const std::shared_ptr<FileFragment>& fragment) {
        ARROW_UNUSED(format);
        ARROW_ASSIGN_OR_RAISE(
            auto reader,
            OpenORCReader(source, std::make_shared<ScanOptions>(scan_options)));

        auto materialized_fields = scan_options.MaterializedFields();
        // filter out virtual columns
        std::vector<std::string> included_fields;
        ARROW_ASSIGN_OR_RAISE(auto schema, reader->ReadSchema());
        for (const auto& ref : materialized_fields) {
          ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOneOrNone(*schema));
          if (match.indices().empty()) continue;

          included_fields.push_back(schema->field(match.indices()[0])->name());
        }

        // NEW: Apply stripe filtering if OrcFileFragment and filter present
        std::vector<int> stripe_indices;
        int num_stripes = reader->NumberOfStripes();

        auto orc_fragment = std::dynamic_pointer_cast<OrcFileFragment>(fragment);
        if (orc_fragment && scan_options.filter != compute::literal(true)) {
          // Use predicate pushdown
          ARROW_ASSIGN_OR_RAISE(stripe_indices,
                               orc_fragment->FilterStripes(scan_options.filter));

          int skipped = num_stripes - static_cast<int>(stripe_indices.size());
          if (skipped > 0) {
            ARROW_LOG(DEBUG) << "ORC predicate pushdown: skipped " << skipped
                            << " of " << num_stripes << " stripes";
          }
        } else {
          // No filtering - read all stripes
          stripe_indices.resize(num_stripes);
          std::iota(stripe_indices.begin(), stripe_indices.end(), 0);
        }

        if (stripe_indices.empty()) {
          return MakeEmptyIterator<std::shared_ptr<RecordBatch>>();
        }

        return RecordBatchIterator(Impl{std::move(reader), std::move(included_fields),
                                        std::move(stripe_indices), scan_options.batch_size,
                                        0, nullptr});
      }

      Result<std::shared_ptr<RecordBatch>> Next() {
        while (true) {
          if (current_stripe_reader_) {
            std::shared_ptr<RecordBatch> batch;
            RETURN_NOT_OK(current_stripe_reader_->ReadNext(&batch));
            if (batch) {
              return batch;
            }
            current_stripe_reader_.reset();
            ++next_stripe_index_;
            continue;
          }

          if (next_stripe_index_ >= stripe_indices_.size()) {
            return IterationEnd<std::shared_ptr<RecordBatch>>();
          }

          const auto stripe = stripe_indices_[next_stripe_index_];
          const auto stripe_info = reader_->GetStripeInformation(stripe);
          RETURN_NOT_OK(reader_->Seek(stripe_info.first_row_id));
          ARROW_ASSIGN_OR_RAISE(
              current_stripe_reader_,
              reader_->NextStripeReader(batch_size_, included_fields_));
        }
      }

      std::unique_ptr<adapters::orc::ORCFileReader> reader_;
      std::vector<std::string> included_fields_;
      std::vector<int> stripe_indices_;
      int64_t batch_size_;
      size_t next_stripe_index_;
      std::shared_ptr<RecordBatchReader> current_stripe_reader_;
    };

    return Impl::Make(fragment_->source(),
                      *checked_pointer_cast<FileFragment>(fragment_)->format(),
                      *options_,
                      fragment_);
  }

 private:
  std::shared_ptr<FileFragment> fragment_;
  std::shared_ptr<ScanOptions> options_;
};

class OrcScanTaskIterator {
 public:
  static Result<Iterator<std::shared_ptr<OrcScanTask>>> Make(
      std::shared_ptr<ScanOptions> options, std::shared_ptr<FileFragment> fragment) {
    return Iterator<std::shared_ptr<OrcScanTask>>(
        OrcScanTaskIterator(std::move(options), std::move(fragment)));
  }

  Result<std::shared_ptr<OrcScanTask>> Next() {
    if (once_) {
      // Iteration is done.
      return nullptr;
    }

    once_ = true;
    return std::make_shared<OrcScanTask>(fragment_, options_);
  }

 private:
  OrcScanTaskIterator(std::shared_ptr<ScanOptions> options,
                      std::shared_ptr<FileFragment> fragment)
      : options_(std::move(options)), fragment_(std::move(fragment)) {}

  bool once_ = false;
  std::shared_ptr<ScanOptions> options_;
  std::shared_ptr<FileFragment> fragment_;
};

}  // namespace

OrcFileFormat::OrcFileFormat() : FileFormat(/*default_fragment_scan_options=*/nullptr) {}

Result<bool> OrcFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenORCReader(source).ok();
}

Result<std::shared_ptr<Schema>> OrcFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenORCReader(source));
  return reader->ReadSchema();
}

Result<std::shared_ptr<FileFragment>> OrcFileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(new OrcFileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression),
      std::move(physical_schema)));
}

Result<RecordBatchGenerator> OrcFileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& options,
    const std::shared_ptr<FileFragment>& file) const {
  // TODO investigate "true" async version
  // (https://issues.apache.org/jira/browse/ARROW-13795)
  ARROW_ASSIGN_OR_RAISE(auto task_iter, OrcScanTaskIterator::Make(options, file));
  struct IterState {
    Iterator<std::shared_ptr<OrcScanTask>> iter;
    RecordBatchIterator curr_iter;
    bool first;
    ::arrow::internal::Executor* io_executor;
  };
  struct {
    Future<std::shared_ptr<RecordBatch>> operator()() {
      auto state = state_;
      return ::arrow::DeferNotOk(
          state->io_executor->Submit([state]() -> Result<std::shared_ptr<RecordBatch>> {
            if (state->first) {
              ARROW_ASSIGN_OR_RAISE(auto task, state->iter.Next());
              ARROW_ASSIGN_OR_RAISE(state->curr_iter, task->Execute());
              state->first = false;
            }
            while (!IsIterationEnd(state->curr_iter)) {
              ARROW_ASSIGN_OR_RAISE(auto next_batch, state->curr_iter.Next());
              if (IsIterationEnd(next_batch)) {
                ARROW_ASSIGN_OR_RAISE(auto task, state->iter.Next());
                if (IsIterationEnd(task)) {
                  state->curr_iter = IterationEnd<RecordBatchIterator>();
                } else {
                  ARROW_ASSIGN_OR_RAISE(state->curr_iter, task->Execute());
                }
              } else {
                return next_batch;
              }
            }
            return IterationEnd<std::shared_ptr<RecordBatch>>();
          }));
    }
    std::shared_ptr<IterState> state_;
  } iter_to_gen{std::shared_ptr<IterState>(
      new IterState{std::move(task_iter), {}, true, options->io_context.executor()})};
  return iter_to_gen;
}

Future<std::optional<int64_t>> OrcFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
  }
  auto self = checked_pointer_cast<OrcFileFormat>(shared_from_this());
  return DeferNotOk(options->io_context.executor()->Submit(
      [self, file]() -> Result<std::optional<int64_t>> {
        ARROW_ASSIGN_OR_RAISE(auto reader, OpenORCReader(file->source()));
        return reader->NumberOfRows();
      }));
}

// //
// // OrcFileFragment
// //

OrcFileFragment::OrcFileFragment(FileSource source,
                                 std::shared_ptr<FileFormat> format,
                                 compute::Expression partition_expression,
                                 std::shared_ptr<Schema> physical_schema)
    : FileFragment(std::move(source), std::move(format),
                   std::move(partition_expression), std::move(physical_schema)) {}

Status OrcFileFragment::EnsureMetadataCached() {
  auto lock = metadata_mutex_.Lock();

  if (metadata_cached_) {
    return Status::OK();
  }

  // Open reader to get schema and stripe information
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenORCReader(source()));
  ARROW_ASSIGN_OR_RAISE(cached_schema_, reader->ReadSchema());

  // Get number of stripes and cache stripe info
  int num_stripes = reader->NumberOfStripes();

  // Cache stripe row counts for later use
  stripe_num_rows_.resize(num_stripes);
  for (int i = 0; i < num_stripes; i++) {
    ARROW_ASSIGN_OR_RAISE(auto stripe_metadata, reader->GetStripeMetadata(i));
    stripe_num_rows_[i] = stripe_metadata->num_rows;
  }

  metadata_cached_ = true;
  return Status::OK();
}

Result<std::vector<compute::Expression>> OrcFileFragment::TestStripes(
    const compute::Expression& predicate) {

  // Ensure metadata is loaded
  RETURN_NOT_OK(EnsureMetadataCached());

  // Resolve and de-duplicate top-level INT32/INT64 fields referenced in predicate.
  std::vector<ResolvedFieldRef> resolved_fields;
  std::vector<bool> field_seen(cached_schema_->num_fields(), false);
  for (const FieldRef& field_ref : compute::FieldsInExpression(predicate)) {
    ARROW_ASSIGN_OR_RAISE(auto match, field_ref.FindOne(*cached_schema_));
    if (!match.has_value()) {
      continue;
    }
    const auto& [field_indices, field] = *match;
    if (field_indices.size() != 1) {
      continue;
    }
    int field_index = field_indices[0];
    if (field_seen[field_index]) {
      continue;
    }
    if (field->type()->id() != Type::INT32 && field->type()->id() != Type::INT64) {
      continue;
    }
    field_seen[field_index] = true;
    resolved_fields.push_back(
        ResolvedFieldRef{field_ref, field, static_cast<uint32_t>(field_index + 1)});
  }

  // Open reader if not already cached
  if (!cached_reader_) {
    auto lock = metadata_mutex_.Lock();
    if (!cached_reader_) {
      ARROW_ASSIGN_OR_RAISE(auto input, source().Open());
      ARROW_ASSIGN_OR_RAISE(
          cached_reader_,
          adapters::orc::ORCFileReader::Open(std::move(input), arrow::default_memory_pool()));
    }
  }

  // Build a stripe-level guarantee expression and simplify predicate for each stripe.
  std::vector<compute::Expression> simplified_expressions;
  simplified_expressions.reserve(stripe_num_rows_.size());
  for (size_t stripe_idx = 0; stripe_idx < stripe_num_rows_.size(); stripe_idx++) {
    ARROW_ASSIGN_OR_RAISE(auto stripe_stats, cached_reader_->GetStripeStatistics(stripe_idx));
    compute::Expression stripe_guarantee = compute::literal(true);
    for (const auto& resolved_field : resolved_fields) {
      const auto* col_stats =
          stripe_stats->getColumnStatistics(resolved_field.orc_column_id);
      auto field_guarantee = DeriveFieldGuarantee(resolved_field, col_stats);
      if (field_guarantee.Equals(compute::literal(true))) {
        continue;
      }
      if (stripe_guarantee.Equals(compute::literal(true))) {
        stripe_guarantee = std::move(field_guarantee);
      } else {
        stripe_guarantee =
            compute::and_(std::move(stripe_guarantee), std::move(field_guarantee));
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto simplified,
                          compute::SimplifyWithGuarantee(predicate, stripe_guarantee));
    simplified_expressions.push_back(std::move(simplified));
  }

  return simplified_expressions;
}

Result<std::vector<int>> OrcFileFragment::FilterStripes(
    const compute::Expression& predicate) {
  RETURN_NOT_OK(EnsureMetadataCached());

  // Feature flag for disabling predicate pushdown
  if (auto env_var = arrow::internal::GetEnvVar("ARROW_ORC_DISABLE_PREDICATE_PUSHDOWN")) {
    if (env_var.ok() && *env_var == "1") {
      // Return all stripe indices
      std::vector<int> all_stripes(stripe_num_rows_.size());
      std::iota(all_stripes.begin(), all_stripes.end(), 0);
      return all_stripes;
    }
  }

  // Test each stripe
  ARROW_ASSIGN_OR_RAISE(auto tested_expressions, TestStripes(predicate));

  // Select stripes where predicate is satisfiable
  std::vector<int> selected_stripes;
  selected_stripes.reserve(stripe_num_rows_.size());

  for (size_t i = 0; i < tested_expressions.size(); i++) {
    if (compute::IsSatisfiable(tested_expressions[i])) {
      selected_stripes.push_back(static_cast<int>(i));
    }
  }

  return selected_stripes;
}

// //
// // OrcFileWriter, OrcFileWriteOptions
// //

std::shared_ptr<FileWriteOptions> OrcFileFormat::DefaultWriteOptions() {
  // TODO (https://issues.apache.org/jira/browse/ARROW-13796)
  return nullptr;
}

Result<std::shared_ptr<FileWriter>> OrcFileFormat::MakeWriter(
    std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileWriteOptions> options,
    fs::FileLocator destination_locator) const {
  // TODO (https://issues.apache.org/jira/browse/ARROW-13796)
  return Status::NotImplemented("ORC writer not yet implemented.");
}

}  // namespace dataset
}  // namespace arrow
