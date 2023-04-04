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

#include "arrow/dataset/scanner.h"

#include <algorithm>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <sstream>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/query_context.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/dataset/dataset.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/plan.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/config.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::Executor;
using internal::SerialExecutor;
using internal::TaskGroup;

using internal::checked_cast;

namespace dataset {

using FragmentGenerator = std::function<Future<std::shared_ptr<Fragment>>()>;

std::vector<FieldRef> ScanOptions::MaterializedFields() const {
  std::vector<FieldRef> fields;

  for (const compute::Expression* expr : {&filter, &projection}) {
    auto refs = FieldsInExpression(*expr);
    fields.insert(fields.end(), std::make_move_iterator(refs.begin()),
                  std::make_move_iterator(refs.end()));
  }

  return fields;
}

std::vector<FieldPath> ScanV2Options::AllColumns(const Schema& dataset_schema) {
  std::vector<FieldPath> selection(dataset_schema.num_fields());
  for (int i = 0; i < dataset_schema.num_fields(); i++) {
    selection[i] = {i};
  }
  return selection;
}

Status ScanV2Options::AddFieldsNeededForFilter(ScanV2Options* options) {
  std::vector<FieldRef> fields_referenced = FieldsInExpression(options->filter);
  for (const auto& field : fields_referenced) {
    // Note: this will fail if the field reference is ambiguous or the field doesn't
    // exist in the dataset schema
    ARROW_ASSIGN_OR_RAISE(auto field_path, field.FindOne(*options->dataset->schema()));
    if (std::find(options->columns.begin(), options->columns.end(), field_path) ==
        options->columns.end()) {
      options->columns.push_back(std::move(field_path));
    }
  }
  return Status::OK();
}

namespace {
class ScannerRecordBatchReader : public RecordBatchReader {
 public:
  explicit ScannerRecordBatchReader(std::shared_ptr<Schema> schema,
                                    TaggedRecordBatchIterator delegate)
      : schema_(std::move(schema)), delegate_(std::move(delegate)) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }
  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    ARROW_ASSIGN_OR_RAISE(auto next, delegate_.Next());
    if (IsIterationEnd(next)) {
      *batch = nullptr;
    } else {
      *batch = std::move(next.record_batch);
    }
    return Status::OK();
  }

  Status Close() override {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(ReadNext(&batch));
    while (batch != nullptr) {
      RETURN_NOT_OK(ReadNext(&batch));
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;
  TaggedRecordBatchIterator delegate_;
};

const FieldVector kAugmentedFields{
    field("__fragment_index", int32()),
    field("__batch_index", int32()),
    field("__last_in_fragment", boolean()),
    field("__filename", utf8()),
};

Result<std::shared_ptr<Schema>> GetProjectedSchemaFromExpression(
    const compute::Expression& projection,
    const std::shared_ptr<Schema>& dataset_schema) {
  // process resultant dataset_schema after projection
  FieldVector project_fields;
  std::set<std::string> field_names;
  if (auto call = projection.call()) {
    if (call->function_name != "make_struct") {
      return Status::Invalid("Top level projection expression call must be make_struct");
    }
    for (auto field_ref : compute::FieldsInExpression(projection)) {
      if (field_ref.IsName()) {
        field_names.emplace(*field_ref.name());
      } else if (field_ref.IsNested()) {
        // We keep the top-level field name.
        auto nested_field_refs = *field_ref.nested_refs();
        field_names.emplace(*nested_field_refs[0].name());
      } else {
        return Status::Invalid(
            "No projected schema was supplied and we could not infer the projected "
            "schema from the projection expression.");
      }
    }
  }
  for (auto f : field_names) {
    auto field = dataset_schema->GetFieldByName(f);
    if (field) {
      // if the field is not present in the schema we ignore it.
      // the case is if kAugmentedFields are present in the expression
      // and if they are not present in the provided schema, we ignore them.
      project_fields.push_back(std::move(field));
    }
  }
  return schema(project_fields);
}

// Scan options has a number of options that we can infer from the dataset
// schema if they are not specified.
Status NormalizeScanOptions(const std::shared_ptr<ScanOptions>& scan_options,
                            const std::shared_ptr<Schema>& dataset_schema) {
  if (scan_options->dataset_schema == nullptr) {
    scan_options->dataset_schema = dataset_schema;
  }

  if (!scan_options->filter.IsBound()) {
    ARROW_ASSIGN_OR_RAISE(scan_options->filter,
                          scan_options->filter.Bind(*dataset_schema));
  }

  if (!scan_options->projected_schema) {
    // If the user specifies a projection expression we can maybe infer from
    // that expression
    if (scan_options->projection.IsBound()) {
      ARROW_ASSIGN_OR_RAISE(
          auto project_schema,
          GetProjectedSchemaFromExpression(scan_options->projection, dataset_schema));
      if (project_schema->num_fields() > 0) {
        scan_options->projected_schema = std::move(project_schema);
      }
      // If the projection isn't a call we assume it's literal(true) or some
      // invalid expression and just ignore it.  It will be replaced below
    }

    // If we couldn't infer it from the projection expression then just grab all
    // fields from the dataset
    if (!scan_options->projected_schema) {
      // Until now, we assume the project expression is bound, but if it is not
      // bound, we have to check the expressions and make sure bind them
      // and create the projected schema based on the field_refs (which guarantees
      // IsName() to be true).

      // process resultant dataset_schema after projection
      ARROW_ASSIGN_OR_RAISE(
          auto projected_schema,
          GetProjectedSchemaFromExpression(scan_options->projection, dataset_schema));

      if (projected_schema->num_fields() > 0) {
        // create the projected schema only if the provided expressions
        // produces valid set of fields.
        ARROW_ASSIGN_OR_RAISE(auto projection_descr,
                              ProjectionDescr::Default(*projected_schema));
        scan_options->projected_schema = std::move(projection_descr.schema);
        scan_options->projection = projection_descr.expression;
        ARROW_ASSIGN_OR_RAISE(scan_options->projection,
                              scan_options->projection.Bind(*projected_schema));
      } else {
        // if projected_fields are not found, we default to creating the projected_schema
        // and projection from the dataset_schema.
        ARROW_ASSIGN_OR_RAISE(auto projection_descr,
                              ProjectionDescr::Default(*dataset_schema));
        scan_options->projected_schema = std::move(projection_descr.schema);
        scan_options->projection = projection_descr.expression;
      }
    }
  }

  if (scan_options->projection == compute::literal(true)) {
    ARROW_ASSIGN_OR_RAISE(
        auto projection_descr,
        ProjectionDescr::FromNames(scan_options->projected_schema->field_names(),
                                   *dataset_schema));
    scan_options->projection = projection_descr.expression;
  }

  if (!scan_options->projection.IsBound()) {
    auto fields = dataset_schema->fields();
    for (const auto& aug_field : kAugmentedFields) {
      fields.push_back(aug_field);
    }

    ARROW_ASSIGN_OR_RAISE(scan_options->projection,
                          scan_options->projection.Bind(Schema(std::move(fields))));
  }

  return Status::OK();
}

}  // namespace

namespace {

class AsyncScanner : public Scanner, public std::enable_shared_from_this<AsyncScanner> {
 public:
  AsyncScanner(std::shared_ptr<Dataset> dataset,
               std::shared_ptr<ScanOptions> scan_options)
      : Scanner(std::move(scan_options)), dataset_(std::move(dataset)) {
    internal::Initialize();
  }

  Status Scan(std::function<Status(TaggedRecordBatch)> visitor) override;
  Result<TaggedRecordBatchIterator> ScanBatches() override;
  Result<TaggedRecordBatchGenerator> ScanBatchesAsync() override;
  Result<TaggedRecordBatchGenerator> ScanBatchesAsync(Executor* executor) override;
  Result<EnumeratedRecordBatchIterator> ScanBatchesUnordered() override;
  Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync() override;
  Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync(
      Executor* executor) override;
  Result<std::shared_ptr<Table>> TakeRows(const Array& indices) override;
  Result<std::shared_ptr<Table>> Head(int64_t num_rows) override;
  Result<std::shared_ptr<Table>> ToTable() override;
  Result<int64_t> CountRows() override;
  Future<int64_t> CountRowsAsync() override;
  Result<std::shared_ptr<RecordBatchReader>> ToRecordBatchReader() override;
  const std::shared_ptr<Dataset>& dataset() const override;

 private:
  Future<> VisitBatchesAsync(std::function<Status(TaggedRecordBatch)> visitor,
                             Executor* executor);
  Result<EnumeratedRecordBatchGenerator> ScanBatchesUnorderedAsync(
      Executor* executor, bool sequence_fragments, bool use_legacy_batching = false);
  Future<std::shared_ptr<Table>> ToTableAsync(Executor* executor);
  Future<int64_t> CountRowsAsync(Executor* executor);

  Result<FragmentGenerator> GetFragments() const;

  std::shared_ptr<Dataset> dataset_;
};

Result<EnumeratedRecordBatchGenerator> FragmentToBatches(
    const Enumerated<std::shared_ptr<Fragment>>& fragment,
    const std::shared_ptr<ScanOptions>& options) {
#ifdef ARROW_WITH_OPENTELEMETRY
  util::tracing::Span span;
  START_SPAN(span, "Scanner::FragmentToBatches",
             {
                 {"arrow.dataset.fragment", fragment.value->ToString()},
                 {"arrow.dataset.fragment.index", fragment.index},
                 {"arrow.dataset.fragment.last", fragment.last},
                 {"arrow.dataset.fragment.type_name", fragment.value->type_name()},
             });
#endif
  ARROW_ASSIGN_OR_RAISE(auto batch_gen, fragment.value->ScanBatchesAsync(options));
  ArrayVector columns;
  for (const auto& field : options->dataset_schema->fields()) {
    // TODO(ARROW-7051): use helper to make empty batch
    ARROW_ASSIGN_OR_RAISE(auto array,
                          MakeArrayOfNull(field->type(), /*length=*/0, options->pool));
    columns.push_back(std::move(array));
  }
  WRAP_ASYNC_GENERATOR(batch_gen);
  batch_gen = MakeDefaultIfEmptyGenerator(
      std::move(batch_gen),
      RecordBatch::Make(options->dataset_schema, /*num_rows=*/0, std::move(columns)));
  auto enumerated_batch_gen = MakeEnumeratedGenerator(std::move(batch_gen));

  auto combine_fn =
      [fragment](const Enumerated<std::shared_ptr<RecordBatch>>& record_batch) {
        return EnumeratedRecordBatch{record_batch, fragment};
      };

  return MakeMappedGenerator(enumerated_batch_gen, std::move(combine_fn));
}

Result<AsyncGenerator<EnumeratedRecordBatchGenerator>> FragmentsToBatches(
    FragmentGenerator fragment_gen, const std::shared_ptr<ScanOptions>& options) {
  auto enumerated_fragment_gen = MakeEnumeratedGenerator(std::move(fragment_gen));
  auto batch_gen_gen =
      MakeMappedGenerator(std::move(enumerated_fragment_gen),
                          [=](const Enumerated<std::shared_ptr<Fragment>>& fragment) {
                            return FragmentToBatches(fragment, options);
                          });
  PROPAGATE_SPAN_TO_GENERATOR(std::move(batch_gen_gen));
  return batch_gen_gen;
}

class OneShotFragment : public Fragment {
 public:
  OneShotFragment(std::shared_ptr<Schema> schema, RecordBatchIterator batch_it)
      : Fragment(compute::literal(true), std::move(schema)),
        batch_it_(std::move(batch_it)) {
    DCHECK_NE(physical_schema_, nullptr);
  }
  Status CheckConsumed() {
    if (!batch_it_) return Status::Invalid("OneShotFragment was already scanned");
    return Status::OK();
  }
  Result<RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<ScanOptions>& options) override {
    RETURN_NOT_OK(CheckConsumed());
    ARROW_ASSIGN_OR_RAISE(
        auto background_gen,
        MakeBackgroundGenerator(std::move(batch_it_), options->io_context.executor()));
    return MakeTransferredGenerator(std::move(background_gen),
                                    ::arrow::internal::GetCpuThreadPool());
  }
  std::string type_name() const override { return "one-shot"; }

 protected:
  Result<std::shared_ptr<Schema>> ReadPhysicalSchemaImpl() override {
    return physical_schema_;
  }

  RecordBatchIterator batch_it_;
};

Result<FragmentGenerator> AsyncScanner::GetFragments() const {
  // TODO(ARROW-8163): Async fragment scanning will return AsyncGenerator<Fragment>
  // here. Current iterator based versions are all fast & sync so we will just ToVector
  // it
  ARROW_ASSIGN_OR_RAISE(auto fragments_it, dataset_->GetFragments(scan_options_->filter));
  ARROW_ASSIGN_OR_RAISE(auto fragments_vec, fragments_it.ToVector());
  return MakeVectorGenerator(std::move(fragments_vec));
}

Result<TaggedRecordBatchIterator> AsyncScanner::ScanBatches() {
  return ::arrow::internal::IterateSynchronously<TaggedRecordBatch>(
      [this](::arrow::internal::Executor* executor) {
        return ScanBatchesAsync(executor);
      },
      scan_options_->use_threads);
}

Result<EnumeratedRecordBatchIterator> AsyncScanner::ScanBatchesUnordered() {
  return ::arrow::internal::IterateSynchronously<EnumeratedRecordBatch>(
      [this](::arrow::internal::Executor* executor) {
        return ScanBatchesUnorderedAsync(executor);
      },
      scan_options_->use_threads);
}

Result<std::shared_ptr<Table>> AsyncScanner::ToTable() {
  return ::arrow::internal::RunSynchronously<Future<std::shared_ptr<Table>>>(
      [this](::arrow::internal::Executor* executor) { return ToTableAsync(executor); },
      scan_options_->use_threads);
}

Result<EnumeratedRecordBatchGenerator> AsyncScanner::ScanBatchesUnorderedAsync() {
  return ScanBatchesUnorderedAsync(::arrow::internal::GetCpuThreadPool(),
                                   /*sequence_fragments=*/false);
}

Result<EnumeratedRecordBatchGenerator> AsyncScanner::ScanBatchesUnorderedAsync(
    ::arrow::internal::Executor* cpu_thread_pool) {
  return ScanBatchesUnorderedAsync(cpu_thread_pool, /*sequence_fragments=*/false);
}

Result<EnumeratedRecordBatch> ToEnumeratedRecordBatch(
    const std::optional<compute::ExecBatch>& batch, const ScanOptions& options,
    const FragmentVector& fragments) {
  int num_fields = options.projected_schema->num_fields();

  EnumeratedRecordBatch out;
  out.fragment.index = batch->values[num_fields].scalar_as<Int32Scalar>().value;
  out.fragment.last = false;  // ignored during reordering
  out.fragment.value = fragments[out.fragment.index];

  out.record_batch.index = batch->values[num_fields + 1].scalar_as<Int32Scalar>().value;
  out.record_batch.last = batch->values[num_fields + 2].scalar_as<BooleanScalar>().value;
  ARROW_ASSIGN_OR_RAISE(out.record_batch.value,
                        batch->ToRecordBatch(options.projected_schema, options.pool));
  return out;
}

Result<EnumeratedRecordBatchGenerator> AsyncScanner::ScanBatchesUnorderedAsync(
    Executor* cpu_executor, bool sequence_fragments, bool use_legacy_batching) {
  RETURN_NOT_OK(NormalizeScanOptions(scan_options_, dataset_->schema()));

  auto exec_context =
      std::make_shared<compute::ExecContext>(scan_options_->pool, cpu_executor);

  acero::QueryOptions query_options;
  query_options.use_legacy_batching = use_legacy_batching;

  ARROW_ASSIGN_OR_RAISE(auto plan,
                        acero::ExecPlan::Make(query_options, *exec_context.get()));
  AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen;

  auto exprs = scan_options_->projection.call()->arguments;
  auto names = checked_cast<const compute::MakeStructOptions*>(
                   scan_options_->projection.call()->options.get())
                   ->field_names;

  RETURN_NOT_OK(
      acero::Declaration::Sequence(
          {
              {"scan", ScanNodeOptions{dataset_, scan_options_, sequence_fragments}},
              {"filter", acero::FilterNodeOptions{scan_options_->filter}},
              {"augmented_project",
               acero::ProjectNodeOptions{std::move(exprs), std::move(names)}},
              {"sink", acero::SinkNodeOptions{&sink_gen, /*schema=*/nullptr,
                                              scan_options_->backpressure}},
          })
          .AddToPlan(plan.get()));

  plan->StartProducing();

  auto options = scan_options_;
  ARROW_ASSIGN_OR_RAISE(auto fragments_it, dataset_->GetFragments(scan_options_->filter));
  ARROW_ASSIGN_OR_RAISE(auto fragments, fragments_it.ToVector());
  auto shared_fragments = std::make_shared<FragmentVector>(std::move(fragments));

  // If the generator is destroyed before being completely drained, inform plan
  std::shared_ptr<void> stop_producing{
      nullptr, [plan, exec_context](...) {
        bool not_finished_yet = plan->finished().TryAddCallback(
            [&plan, &exec_context] { return [plan, exec_context](const Status&) {}; });

        if (not_finished_yet) {
          plan->StopProducing();
        }
      }};

  EnumeratedRecordBatchGenerator mapped_gen = MakeMappedGenerator(
      std::move(sink_gen),
      [sink_gen, options,
       shared_fragments](const std::optional<compute::ExecBatch>& batch)
          -> Future<EnumeratedRecordBatch> {
        return ToEnumeratedRecordBatch(batch, *options, *shared_fragments);
      });

  return [mapped_gen = std::move(mapped_gen), plan = std::move(plan),
          stop_producing = std::move(stop_producing)] {
    auto next = mapped_gen();
    return next.Then([plan](const EnumeratedRecordBatch& value) {
      if (IsIterationEnd(value)) {
        return plan->finished().Then([value] { return value; });
      }
      return Future<EnumeratedRecordBatch>::MakeFinished(value);
    });
  };
}

Result<std::shared_ptr<Table>> AsyncScanner::TakeRows(const Array& indices) {
  if (indices.null_count() != 0) {
    return Status::NotImplemented("null take indices");
  }

  compute::ExecContext ctx(scan_options_->pool);

  const Array* original_indices;
  // If we have to cast, this is the backing reference
  std::shared_ptr<Array> original_indices_ptr;
  if (indices.type_id() != Type::INT64) {
    ARROW_ASSIGN_OR_RAISE(
        original_indices_ptr,
        compute::Cast(indices, int64(), compute::CastOptions::Safe(), &ctx));
    original_indices = original_indices_ptr.get();
  } else {
    original_indices = &indices;
  }

  std::shared_ptr<Array> unsort_indices;
  {
    ARROW_ASSIGN_OR_RAISE(
        auto sort_indices,
        compute::SortIndices(*original_indices, compute::SortOrder::Ascending, &ctx));
    ARROW_ASSIGN_OR_RAISE(original_indices_ptr,
                          compute::Take(*original_indices, *sort_indices,
                                        compute::TakeOptions::Defaults(), &ctx));
    original_indices = original_indices_ptr.get();
    ARROW_ASSIGN_OR_RAISE(
        unsort_indices,
        compute::SortIndices(*sort_indices, compute::SortOrder::Ascending, &ctx));
  }

  RecordBatchVector out_batches;

  auto raw_indices = static_cast<const Int64Array&>(*original_indices).raw_values();
  int64_t offset = 0, row_begin = 0;

  ARROW_ASSIGN_OR_RAISE(auto batch_it, ScanBatches());
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_it.Next());
    if (IsIterationEnd(batch)) break;
    if (offset == original_indices->length()) break;
    DCHECK_LT(offset, original_indices->length());

    int64_t length = 0;
    while (offset + length < original_indices->length()) {
      auto rel_index = raw_indices[offset + length] - row_begin;
      if (rel_index >= batch.record_batch->num_rows()) break;
      ++length;
    }
    DCHECK_LE(offset + length, original_indices->length());
    if (length == 0) {
      row_begin += batch.record_batch->num_rows();
      continue;
    }

    Datum rel_indices = original_indices->Slice(offset, length);
    ARROW_ASSIGN_OR_RAISE(rel_indices,
                          compute::Subtract(rel_indices, Datum(row_begin),
                                            compute::ArithmeticOptions(), &ctx));

    ARROW_ASSIGN_OR_RAISE(Datum out_batch,
                          compute::Take(batch.record_batch, rel_indices,
                                        compute::TakeOptions::Defaults(), &ctx));
    out_batches.push_back(out_batch.record_batch());

    offset += length;
    row_begin += batch.record_batch->num_rows();
  }

  if (offset < original_indices->length()) {
    std::stringstream error;
    const int64_t max_values_shown = 3;
    const int64_t num_remaining = original_indices->length() - offset;
    for (int64_t i = 0; i < std::min<int64_t>(max_values_shown, num_remaining); i++) {
      if (i > 0) error << ", ";
      error << static_cast<const Int64Array*>(original_indices)->Value(offset + i);
    }
    if (num_remaining > max_values_shown) error << ", ...";
    return Status::IndexError("Some indices were out of bounds: ", error.str());
  }
  ARROW_ASSIGN_OR_RAISE(Datum out, Table::FromRecordBatches(options()->projected_schema,
                                                            std::move(out_batches)));
  ARROW_ASSIGN_OR_RAISE(
      out, compute::Take(out, unsort_indices, compute::TakeOptions::Defaults(), &ctx));
  return out.table();
}

Result<std::shared_ptr<Table>> AsyncScanner::Head(int64_t num_rows) {
  if (num_rows == 0) {
    return Table::FromRecordBatches(options()->projected_schema, {});
  }
  ARROW_ASSIGN_OR_RAISE(auto batch_iterator, ScanBatches());
  RecordBatchVector batches;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_iterator.Next());
    if (IsIterationEnd(batch)) break;
    batches.push_back(batch.record_batch->Slice(0, num_rows));
    num_rows -= batch.record_batch->num_rows();
    if (num_rows <= 0) break;
  }
  return Table::FromRecordBatches(options()->projected_schema, batches);
}

Result<TaggedRecordBatchGenerator> AsyncScanner::ScanBatchesAsync() {
  return ScanBatchesAsync(::arrow::internal::GetCpuThreadPool());
}

Result<TaggedRecordBatchGenerator> AsyncScanner::ScanBatchesAsync(
    Executor* cpu_executor) {
  ARROW_ASSIGN_OR_RAISE(
      auto unordered, ScanBatchesUnorderedAsync(cpu_executor, /*sequence_fragments=*/true,
                                                /*use_legacy_batching=*/true));
  // We need an initial value sentinel, so we use one with fragment.index < 0
  auto is_before_any = [](const EnumeratedRecordBatch& batch) {
    return batch.fragment.index < 0;
  };
  auto left_after_right = [&is_before_any](const EnumeratedRecordBatch& left,
                                           const EnumeratedRecordBatch& right) {
    // Before any comes first
    if (is_before_any(left)) {
      return false;
    }
    if (is_before_any(right)) {
      return true;
    }
    // Compare batches if fragment is the same
    if (left.fragment.index == right.fragment.index) {
      return left.record_batch.index > right.record_batch.index;
    }
    // Otherwise compare fragment
    return left.fragment.index > right.fragment.index;
  };
  auto is_next = [is_before_any](const EnumeratedRecordBatch& prev,
                                 const EnumeratedRecordBatch& next) {
    // Only true if next is the first batch
    if (is_before_any(prev)) {
      return next.fragment.index == 0 && next.record_batch.index == 0;
    }
    // If same fragment, compare batch index
    if (prev.fragment.index == next.fragment.index) {
      return next.record_batch.index == prev.record_batch.index + 1;
    }
    // Else only if next first batch of next fragment and prev is last batch of previous
    return next.fragment.index == prev.fragment.index + 1 && prev.record_batch.last &&
           next.record_batch.index == 0;
  };
  auto before_any = EnumeratedRecordBatch{{nullptr, -1, false}, {nullptr, -1, false}};
  auto sequenced = MakeSequencingGenerator(std::move(unordered), left_after_right,
                                           is_next, before_any);

  auto unenumerate_fn = [](const EnumeratedRecordBatch& enumerated_batch) {
    return TaggedRecordBatch{enumerated_batch.record_batch.value,
                             enumerated_batch.fragment.value};
  };
  return MakeMappedGenerator(std::move(sequenced), unenumerate_fn);
}

struct AsyncTableAssemblyState {
  /// Protecting mutating accesses to batches
  std::mutex mutex{};
  std::vector<RecordBatchVector> batches{};

  void Emplace(const EnumeratedRecordBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex);
    auto fragment_index = batch.fragment.index;
    auto batch_index = batch.record_batch.index;
    if (static_cast<int>(batches.size()) <= fragment_index) {
      batches.resize(fragment_index + 1);
    }
    if (static_cast<int>(batches[fragment_index].size()) <= batch_index) {
      batches[fragment_index].resize(batch_index + 1);
    }
    batches[fragment_index][batch_index] = batch.record_batch.value;
  }

  RecordBatchVector Finish() {
    RecordBatchVector all_batches;
    for (auto& fragment_batches : batches) {
      auto end = std::make_move_iterator(fragment_batches.end());
      for (auto it = std::make_move_iterator(fragment_batches.begin()); it != end; it++) {
        all_batches.push_back(*it);
      }
    }
    return all_batches;
  }
};

Status AsyncScanner::Scan(std::function<Status(TaggedRecordBatch)> visitor) {
  auto top_level_task = [this, &visitor](Executor* executor) {
    return VisitBatchesAsync(visitor, executor);
  };
  return ::arrow::internal::RunSynchronously<Future<>>(top_level_task,
                                                       scan_options_->use_threads);
}

Future<> AsyncScanner::VisitBatchesAsync(std::function<Status(TaggedRecordBatch)> visitor,
                                         Executor* executor) {
  ARROW_ASSIGN_OR_RAISE(auto batches_gen, ScanBatchesAsync(executor));
  return VisitAsyncGenerator(std::move(batches_gen), visitor);
}

Future<std::shared_ptr<Table>> AsyncScanner::ToTableAsync(Executor* cpu_executor) {
  auto scan_options = scan_options_;
  ARROW_ASSIGN_OR_RAISE(
      auto positioned_batch_gen,
      ScanBatchesUnorderedAsync(cpu_executor, /*sequence_fragments=*/false,
                                /*use_legacy_batching=*/true));
  /// Wraps the state in a shared_ptr to ensure that failing ScanTasks don't
  /// invalidate concurrently running tasks when Finish() early returns
  /// and the mutex/batches fail out of scope.
  auto state = std::make_shared<AsyncTableAssemblyState>();

  auto table_building_task = [state](const EnumeratedRecordBatch& batch) {
    state->Emplace(batch);
    return batch;
  };

  auto table_building_gen =
      MakeMappedGenerator(positioned_batch_gen, table_building_task);

  return DiscardAllFromAsyncGenerator(table_building_gen).Then([state, scan_options]() {
    return Table::FromRecordBatches(scan_options->projected_schema, state->Finish());
  });
}

Future<int64_t> AsyncScanner::CountRowsAsync(Executor* executor) {
  ARROW_ASSIGN_OR_RAISE(auto fragment_gen, GetFragments());

  compute::ExecContext exec_context(scan_options_->pool, executor);

  ARROW_ASSIGN_OR_RAISE(auto plan, acero::ExecPlan::Make(exec_context));
  // Drop projection since we only need to count rows
  const auto options = std::make_shared<ScanOptions>(*scan_options_);
  ARROW_ASSIGN_OR_RAISE(auto empty_projection,
                        ProjectionDescr::FromNames(std::vector<std::string>(),
                                                   *scan_options_->dataset_schema));
  SetProjection(options.get(), empty_projection);

  auto total = std::make_shared<std::atomic<int64_t>>(0);

  fragment_gen = MakeMappedGenerator(
      std::move(fragment_gen),
      [options, total](const std::shared_ptr<Fragment>& fragment) {
        return fragment->CountRows(options->filter, options)
            .Then([options, total, fragment](std::optional<int64_t> fast_count) mutable
                  -> std::shared_ptr<Fragment> {
              if (fast_count) {
                // fast path: got row count directly; skip scanning this fragment
                (*total) += *fast_count;
                return std::make_shared<InMemoryFragment>(options->dataset_schema,
                                                          RecordBatchVector{});
              }

              // slow path: actually filter this fragment's batches
              return std::move(fragment);
            });
      });

  acero::Declaration count_plan = acero::Declaration::Sequence(
      {{"scan",
        ScanNodeOptions{std::make_shared<FragmentDataset>(scan_options_->dataset_schema,
                                                          std::move(fragment_gen)),
                        options}},
       {"project", acero::ProjectNodeOptions{{options->filter}, {"mask"}}},
       {"aggregate", acero::AggregateNodeOptions{{compute::Aggregate{
                         "sum", nullptr, "mask", "selected_count"}}}}});

  return acero::DeclarationToBatchesAsync(std::move(count_plan), exec_context)
      .Then([total](const RecordBatchVector& batches) -> Result<int64_t> {
        DCHECK_EQ(1, batches.size());
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> count_scalar,
                              batches[0]->column(0)->GetScalar(0));
        return total->load() +
               static_cast<int64_t>(
                   ::arrow::internal::checked_pointer_cast<UInt64Scalar>(count_scalar)
                       ->value);
      });
}

Future<int64_t> AsyncScanner::CountRowsAsync() {
  return CountRowsAsync(::arrow::internal::GetCpuThreadPool());
}

Result<int64_t> AsyncScanner::CountRows() {
  return ::arrow::internal::RunSynchronously<Future<int64_t>>(
      [this](Executor* executor) { return CountRowsAsync(executor); },
      scan_options_->use_threads);
}

Result<std::shared_ptr<RecordBatchReader>> AsyncScanner::ToRecordBatchReader() {
  ARROW_ASSIGN_OR_RAISE(auto it, ScanBatches());
  return std::make_shared<ScannerRecordBatchReader>(options()->projected_schema,
                                                    std::move(it));
}

const std::shared_ptr<Dataset>& AsyncScanner::dataset() const { return dataset_; }
}  // namespace

Result<ProjectionDescr> ProjectionDescr::FromStructExpression(
    const compute::Expression& projection, const Schema& dataset_schema) {
  ARROW_ASSIGN_OR_RAISE(compute::Expression bound_expression,
                        projection.Bind(dataset_schema));

  if (bound_expression.type()->id() != Type::STRUCT) {
    return Status::Invalid("Projection ", projection.ToString(),
                           " cannot yield record batches");
  }
  std::shared_ptr<Schema> projection_schema =
      ::arrow::schema(checked_cast<const StructType&>(*bound_expression.type()).fields(),
                      dataset_schema.metadata());

  return ProjectionDescr{std::move(bound_expression), std::move(projection_schema)};
}

Result<ProjectionDescr> ProjectionDescr::FromExpressions(
    std::vector<compute::Expression> exprs, std::vector<std::string> names,
    const Schema& dataset_schema) {
  compute::MakeStructOptions project_options{std::move(names)};

  for (size_t i = 0; i < exprs.size(); ++i) {
    if (auto ref = exprs[i].field_ref()) {
      // set metadata and nullability for plain field references
      ARROW_ASSIGN_OR_RAISE(auto field, ref->GetOne(dataset_schema));
      project_options.field_nullability[i] = field->nullable();
      project_options.field_metadata[i] = field->metadata();
    }
  }

  return ProjectionDescr::FromStructExpression(
      call("make_struct", std::move(exprs), std::move(project_options)), dataset_schema);
}

Result<ProjectionDescr> ProjectionDescr::FromNames(std::vector<std::string> names,
                                                   const Schema& dataset_schema) {
  std::vector<compute::Expression> exprs(names.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    // If name isn't in schema, try finding it by dotted path.
    if (dataset_schema.GetFieldByName(names[i]) == nullptr) {
      auto name = names[i];
      if (name.rfind(".", 0) != 0) {
        name = "." + name;
      }
      ARROW_ASSIGN_OR_RAISE(auto field_ref, FieldRef::FromDotPath(name));
      // safe as we know there is at least 1 dot.
      names[i] = name.substr(name.rfind(".") + 1);
      exprs[i] = compute::field_ref(field_ref);
    } else {
      exprs[i] = compute::field_ref(names[i]);
    }
  }
  auto fields = dataset_schema.fields();
  for (const auto& aug_field : kAugmentedFields) {
    fields.push_back(aug_field);
  }
  return ProjectionDescr::FromExpressions(std::move(exprs), std::move(names),
                                          Schema(fields, dataset_schema.metadata()));
}

Result<ProjectionDescr> ProjectionDescr::Default(const Schema& dataset_schema) {
  return ProjectionDescr::FromNames(dataset_schema.field_names(), dataset_schema);
}

void SetProjection(ScanOptions* options, ProjectionDescr projection) {
  options->projection = std::move(projection.expression);
  options->projected_schema = std::move(projection.schema);
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset)
    : ScannerBuilder(std::move(dataset), std::make_shared<ScanOptions>()) {}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset,
                               std::shared_ptr<ScanOptions> scan_options)
    : dataset_(std::move(dataset)), scan_options_(std::move(scan_options)) {
  scan_options_->dataset_schema = dataset_->schema();
  DCHECK_OK(Filter(scan_options_->filter));
}

ScannerBuilder::ScannerBuilder(std::shared_ptr<Schema> schema,
                               std::shared_ptr<Fragment> fragment,
                               std::shared_ptr<ScanOptions> scan_options)
    : ScannerBuilder(std::make_shared<FragmentDataset>(
                         std::move(schema), FragmentVector{std::move(fragment)}),
                     std::move(scan_options)) {}

std::shared_ptr<ScannerBuilder> ScannerBuilder::FromRecordBatchReader(
    std::shared_ptr<RecordBatchReader> reader) {
  auto batch_it = MakeIteratorFromReader(reader);
  auto fragment =
      std::make_shared<OneShotFragment>(reader->schema(), std::move(batch_it));
  return std::make_shared<ScannerBuilder>(reader->schema(), std::move(fragment),
                                          std::make_shared<ScanOptions>());
}

const std::shared_ptr<Schema>& ScannerBuilder::schema() const {
  return scan_options_->dataset_schema;
}

const std::shared_ptr<Schema>& ScannerBuilder::projected_schema() const {
  return scan_options_->projected_schema;
}

Status ScannerBuilder::Project(std::vector<std::string> columns) {
  ARROW_ASSIGN_OR_RAISE(
      auto projection,
      ProjectionDescr::FromNames(std::move(columns), *scan_options_->dataset_schema));
  SetProjection(scan_options_.get(), std::move(projection));
  return Status::OK();
}

Status ScannerBuilder::Project(std::vector<compute::Expression> exprs,
                               std::vector<std::string> names) {
  ARROW_ASSIGN_OR_RAISE(auto projection, ProjectionDescr::FromExpressions(
                                             std::move(exprs), std::move(names),
                                             *scan_options_->dataset_schema));
  SetProjection(scan_options_.get(), std::move(projection));
  return Status::OK();
}

Status ScannerBuilder::Filter(const compute::Expression& filter) {
  for (const auto& ref : FieldsInExpression(filter)) {
    RETURN_NOT_OK(ref.FindOne(*scan_options_->dataset_schema));
  }
  ARROW_ASSIGN_OR_RAISE(scan_options_->filter,
                        filter.Bind(*scan_options_->dataset_schema));
  return Status::OK();
}

Status ScannerBuilder::UseThreads(bool use_threads) {
  scan_options_->use_threads = use_threads;
  return Status::OK();
}

Status ScannerBuilder::BatchSize(int64_t batch_size) {
  if (batch_size <= 0) {
    return Status::Invalid("BatchSize must be greater than 0, got ", batch_size);
  }
  scan_options_->batch_size = batch_size;
  return Status::OK();
}

Status ScannerBuilder::BatchReadahead(int32_t batch_readahead) {
  if (batch_readahead < 0) {
    return Status::Invalid("BatchReadahead must be greater than or equal 0, got ",
                           batch_readahead);
  }
  scan_options_->batch_readahead = batch_readahead;
  return Status::OK();
}

Status ScannerBuilder::FragmentReadahead(int32_t fragment_readahead) {
  if (fragment_readahead < 0) {
    return Status::Invalid("FragmentReadahead must be greater than or equal 0, got ",
                           fragment_readahead);
  }
  scan_options_->fragment_readahead = fragment_readahead;
  return Status::OK();
}

Status ScannerBuilder::Pool(MemoryPool* pool) {
  scan_options_->pool = pool;
  return Status::OK();
}

Status ScannerBuilder::FragmentScanOptions(
    std::shared_ptr<dataset::FragmentScanOptions> fragment_scan_options) {
  scan_options_->fragment_scan_options = std::move(fragment_scan_options);
  return Status::OK();
}

Status ScannerBuilder::Backpressure(acero::BackpressureOptions backpressure) {
  scan_options_->backpressure = backpressure;
  return Status::OK();
}

Result<std::shared_ptr<ScanOptions>> ScannerBuilder::GetScanOptions() {
  if (!scan_options_->projection.IsBound()) {
    RETURN_NOT_OK(Project(scan_options_->dataset_schema->field_names()));
  }

  return scan_options_;
}

Result<std::shared_ptr<Scanner>> ScannerBuilder::Finish() {
  ARROW_ASSIGN_OR_RAISE(auto scan_options, GetScanOptions());
  return std::make_shared<AsyncScanner>(dataset_, scan_options);
}

namespace {

Result<acero::ExecNode*> MakeScanNode(acero::ExecPlan* plan,
                                      std::vector<acero::ExecNode*> inputs,
                                      const acero::ExecNodeOptions& options) {
  const auto& scan_node_options = checked_cast<const ScanNodeOptions&>(options);
  auto scan_options = scan_node_options.scan_options;
  auto dataset = scan_node_options.dataset;
  bool require_sequenced_output = scan_node_options.require_sequenced_output;

  RETURN_NOT_OK(NormalizeScanOptions(scan_options, dataset->schema()));

  // using a generator for speculative forward compatibility with async fragment discovery
  ARROW_ASSIGN_OR_RAISE(auto fragments_it, dataset->GetFragments(scan_options->filter));
  ARROW_ASSIGN_OR_RAISE(auto fragments_vec, fragments_it.ToVector());
  auto fragment_gen = MakeVectorGenerator(std::move(fragments_vec));

  ARROW_ASSIGN_OR_RAISE(auto batch_gen_gen,
                        FragmentsToBatches(std::move(fragment_gen), scan_options));

  AsyncGenerator<EnumeratedRecordBatch> merged_batch_gen;
  if (require_sequenced_output) {
    if (scan_options->fragment_readahead > 1) {
      ARROW_ASSIGN_OR_RAISE(merged_batch_gen, MakeSequencedMergedGenerator(
                                                  std::move(batch_gen_gen),
                                                  scan_options->fragment_readahead));
    } else {
      merged_batch_gen = MakeConcatenatedGenerator(std::move(batch_gen_gen));
    }
  } else {
    merged_batch_gen =
        MakeMergedGenerator(std::move(batch_gen_gen), scan_options->fragment_readahead);
  }

  AsyncGenerator<EnumeratedRecordBatch> batch_gen;
  if (scan_options->fragment_readahead > 1) {
    batch_gen = MakeReadaheadGenerator(std::move(merged_batch_gen),
                                       scan_options->fragment_readahead);
  } else {
    batch_gen = std::move(merged_batch_gen);
  }

  auto gen = MakeMappedGenerator(
      std::move(batch_gen),
      [scan_options](const EnumeratedRecordBatch& partial)
          -> Result<std::optional<compute::ExecBatch>> {
        // TODO(ARROW-13263) fragments may be able to attach more guarantees to batches
        // than this, for example parquet's row group stats. Failing to do this leaves
        // perf on the table because row group stats could be used to skip kernel execs in
        // FilterNode.
        //
        // Additionally, if a fragment failed to perform projection pushdown there may be
        // unnecessarily materialized columns in batch. We could drop them now instead of
        // letting them coast through the rest of the plan.
        auto guarantee = partial.fragment.value->partition_expression();

        ARROW_ASSIGN_OR_RAISE(
            std::optional<compute::ExecBatch> batch,
            compute::MakeExecBatch(*scan_options->dataset_schema,
                                   partial.record_batch.value, guarantee));

        // tag rows with fragment- and batch-of-origin
        batch->values.emplace_back(partial.fragment.index);
        batch->values.emplace_back(partial.record_batch.index);
        batch->values.emplace_back(partial.record_batch.last);
        batch->values.emplace_back(partial.fragment.value->ToString());
        return batch;
      });

  auto fields = scan_options->dataset_schema->fields();
  for (const auto& aug_field : kAugmentedFields) {
    fields.push_back(aug_field);
  }

  return acero::MakeExecNode(
      "source", plan, {},
      acero::SourceNodeOptions{schema(std::move(fields)), std::move(gen)});
}

Result<acero::ExecNode*> MakeAugmentedProjectNode(acero::ExecPlan* plan,
                                                  std::vector<acero::ExecNode*> inputs,
                                                  const acero::ExecNodeOptions& options) {
  const auto& project_options = checked_cast<const acero::ProjectNodeOptions&>(options);
  auto exprs = project_options.expressions;
  auto names = project_options.names;

  if (names.size() == 0) {
    names.resize(exprs.size());
    for (size_t i = 0; i < exprs.size(); ++i) {
      names[i] = exprs[i].ToString();
    }
  }

  for (const auto& aug_field : kAugmentedFields) {
    exprs.push_back(compute::field_ref(aug_field->name()));
    names.push_back(aug_field->name());
  }
  return acero::MakeExecNode(
      "project", plan, std::move(inputs),
      acero::ProjectNodeOptions{std::move(exprs), std::move(names)});
}

Result<acero::ExecNode*> MakeOrderedSinkNode(acero::ExecPlan* plan,
                                             std::vector<acero::ExecNode*> inputs,
                                             const acero::ExecNodeOptions& options) {
  if (inputs.size() != 1) {
    return Status::Invalid("Ordered SinkNode requires exactly 1 input, got ",
                           inputs.size());
  }
  auto input = inputs[0];

  AsyncGenerator<std::optional<compute::ExecBatch>> unordered;
  ARROW_ASSIGN_OR_RAISE(auto node,
                        acero::MakeExecNode("sink", plan, std::move(inputs),
                                            acero::SinkNodeOptions{&unordered}));

  const Schema& schema = *input->output_schema();
  ARROW_ASSIGN_OR_RAISE(FieldPath match, FieldRef("__fragment_index").FindOne(schema));
  int i = match[0];
  auto fragment_index = [i](const compute::ExecBatch& batch) {
    return batch.values[i].scalar_as<Int32Scalar>().value;
  };
  compute::ExecBatch before_any{{}, 0};
  before_any.values.resize(i + 1);
  before_any.values.back() = Datum(-1);

  ARROW_ASSIGN_OR_RAISE(match, FieldRef("__batch_index").FindOne(schema));
  i = match[0];
  auto batch_index = [i](const compute::ExecBatch& batch) {
    return batch.values[i].scalar_as<Int32Scalar>().value;
  };

  ARROW_ASSIGN_OR_RAISE(match, FieldRef("__last_in_fragment").FindOne(schema));
  i = match[0];
  auto last_in_fragment = [i](const compute::ExecBatch& batch) {
    return batch.values[i].scalar_as<BooleanScalar>().value;
  };

  auto is_before_any = [=](const compute::ExecBatch& batch) {
    return fragment_index(batch) < 0;
  };

  auto left_after_right = [=](const std::optional<compute::ExecBatch>& left,
                              const std::optional<compute::ExecBatch>& right) {
    // Before any comes first
    if (is_before_any(*left)) {
      return false;
    }
    if (is_before_any(*right)) {
      return true;
    }
    // Compare batches if fragment is the same
    if (fragment_index(*left) == fragment_index(*right)) {
      return batch_index(*left) > batch_index(*right);
    }
    // Otherwise compare fragment
    return fragment_index(*left) > fragment_index(*right);
  };

  auto is_next = [=](const std::optional<compute::ExecBatch>& prev,
                     const std::optional<compute::ExecBatch>& next) {
    // Only true if next is the first batch
    if (is_before_any(*prev)) {
      return fragment_index(*next) == 0 && batch_index(*next) == 0;
    }
    // If same fragment, compare batch index
    if (fragment_index(*next) == fragment_index(*prev)) {
      return batch_index(*next) == batch_index(*prev) + 1;
    }
    // Else only if next first batch of next fragment and prev is last batch of previous
    return fragment_index(*next) == fragment_index(*prev) + 1 &&
           last_in_fragment(*prev) && batch_index(*next) == 0;
  };

  const auto& sink_options = checked_cast<const acero::SinkNodeOptions&>(options);
  *sink_options.generator =
      MakeSequencingGenerator(std::move(unordered), left_after_right, is_next,
                              std::make_optional(std::move(before_any)));

  return node;
}

}  // namespace

namespace internal {
void InitializeScanner(arrow::acero::ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("scan", MakeScanNode));
  DCHECK_OK(registry->AddFactory("ordered_sink", MakeOrderedSinkNode));
  DCHECK_OK(registry->AddFactory("augmented_project", MakeAugmentedProjectNode));
}
}  // namespace internal

}  // namespace dataset
}  // namespace arrow
