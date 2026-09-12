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
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

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
                                              const std::vector<int>& stripe_ids) {
        ARROW_ASSIGN_OR_RAISE(
            auto reader,
            OpenORCReader(source, std::make_shared<ScanOptions>(scan_options)));

        auto materialized_fields = scan_options.MaterializedFields();
        std::vector<std::string> included_fields;
        ARROW_ASSIGN_OR_RAISE(auto schema, reader->ReadSchema());
        for (const auto& ref : materialized_fields) {
          ARROW_ASSIGN_OR_RAISE(auto match, ref.FindOneOrNone(*schema));
          if (match.indices().empty()) continue;

          included_fields.push_back(schema->field(match.indices()[0])->name());
        }

        if (stripe_ids.empty()) {
          std::shared_ptr<RecordBatchReader> record_batch_reader;
          ARROW_ASSIGN_OR_RAISE(
              record_batch_reader,
              reader->GetRecordBatchReader(scan_options.batch_size, included_fields));

          return RecordBatchIterator(Impl{std::move(record_batch_reader)});
        }

        std::vector<int> included_indices;
        for (const auto& field_name : included_fields) {
          int idx = schema->GetFieldIndex(field_name);
          if (idx >= 0) {
            included_indices.push_back(idx);
          }
        }

        return RecordBatchIterator(
            Impl{std::move(reader), stripe_ids, std::move(included_indices),
                 scan_options.batch_size});
      }

      explicit Impl(std::shared_ptr<RecordBatchReader> reader)
          : record_batch_reader_(std::move(reader)), stripe_mode_(false) {}

      Impl(std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader,
           std::vector<int> stripe_ids, std::vector<int> included_indices,
           int64_t batch_size)
          : orc_reader_(std::move(reader)),
            stripe_ids_(std::move(stripe_ids)),
            included_indices_(std::move(included_indices)),
            batch_size_(batch_size),
            current_stripe_idx_(0),
            stripe_mode_(true) {}

      Result<std::shared_ptr<RecordBatch>> Next() {
        if (!stripe_mode_) {
          std::shared_ptr<RecordBatch> batch;
          RETURN_NOT_OK(record_batch_reader_->ReadNext(&batch));
          return batch;
        }

        while (true) {
          if (record_batch_reader_) {
            std::shared_ptr<RecordBatch> batch;
            RETURN_NOT_OK(record_batch_reader_->ReadNext(&batch));
            if (batch) {
              return batch;
            }
            record_batch_reader_.reset();
            current_stripe_idx_++;
          }

          if (current_stripe_idx_ >= stripe_ids_.size()) {
            return nullptr;
          }

          int64_t stripe_id = stripe_ids_[current_stripe_idx_];
          auto stripe_info = orc_reader_->GetStripeInformation(stripe_id);
          RETURN_NOT_OK(orc_reader_->Seek(stripe_info.first_row_id));
          ARROW_ASSIGN_OR_RAISE(
              record_batch_reader_,
              orc_reader_->NextStripeReader(batch_size_, included_indices_));
        }
      }

      std::shared_ptr<RecordBatchReader> record_batch_reader_;
      std::unique_ptr<arrow::adapters::orc::ORCFileReader> orc_reader_;
      std::vector<int> stripe_ids_;
      std::vector<int> included_indices_;
      int64_t batch_size_;
      size_t current_stripe_idx_;
      bool stripe_mode_;
    };

    std::vector<int> stripe_ids;
    if (auto* orc_fragment = dynamic_cast<OrcFileFragment*>(fragment_.get())) {
      stripe_ids = orc_fragment->stripe_ids();
    }

    return Impl::Make(fragment_->source(),
                      *checked_pointer_cast<FileFragment>(fragment_)->format(),
                      *options_, stripe_ids);
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
        auto* orc_fragment = dynamic_cast<OrcFileFragment*>(file.get());
        if (orc_fragment && !orc_fragment->stripe_ids().empty()) {
          int64_t count = 0;
          for (int stripe_id : orc_fragment->stripe_ids()) {
            auto stripe_info = reader->GetStripeInformation(stripe_id);
            count += stripe_info.num_rows;
          }
          return count;
        }
        return reader->NumberOfRows();
      }));
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

// OrcFileFragment

OrcFileFragment::OrcFileFragment(FileSource source, std::shared_ptr<FileFormat> format,
                                 compute::Expression partition_expression,
                                 std::shared_ptr<Schema> physical_schema,
                                 std::optional<std::vector<int>> stripe_ids)
    : FileFragment(std::move(source), std::move(format),
                   std::move(partition_expression), std::move(physical_schema)),
      stripe_ids_(std::move(stripe_ids)) {}

Result<std::shared_ptr<Fragment>> OrcFileFragment::Subset(std::vector<int> stripe_ids) {
  return std::shared_ptr<Fragment>(
      new OrcFileFragment(source_, format_,
                          partition_expression(), physical_schema_,
                          std::move(stripe_ids)));
}

Result<std::shared_ptr<FileFragment>> OrcFileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(
      new OrcFileFragment(std::move(source), shared_from_this(),
                          std::move(partition_expression),
                          std::move(physical_schema), std::nullopt));
}

Result<std::shared_ptr<OrcFileFragment>> OrcFileFormat::MakeFragment(
    FileSource source, compute::Expression partition_expression,
    std::shared_ptr<Schema> physical_schema, std::vector<int> stripe_ids) {
  if (!stripe_ids.empty()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, OpenORCReader(source));
    int64_t num_stripes = reader->NumberOfStripes();
    for (int stripe_id : stripe_ids) {
      if (stripe_id < 0 || stripe_id >= num_stripes) {
        return Status::IndexError("Stripe ID ", stripe_id,
                                  " is out of range. File has ", num_stripes,
                                  " stripe(s), valid range is [0, ",
                                  num_stripes - 1, "]");
      }
    }
  }
  return std::shared_ptr<OrcFileFragment>(
      new OrcFileFragment(std::move(source), shared_from_this(),
                          std::move(partition_expression),
                          std::move(physical_schema), std::move(stripe_ids)));
}

}  // namespace dataset
}  // namespace arrow
