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

#include "arrow/dataset/file_json.h"

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/type_fwd.h"
#include "arrow/io/buffered.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/type_fwd.h"
#include "arrow/json/chunker.h"
#include "arrow/json/parser.h"
#include "arrow/json/reader.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/delimiting.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::Executor;

namespace dataset {

namespace {

using ReaderPtr = std::shared_ptr<json::StreamingReader>;

struct JsonInspectedFragment : public InspectedFragment {
  JsonInspectedFragment() : InspectedFragment({}) {}
  JsonInspectedFragment(std::vector<std::string> column_names,
                        std::shared_ptr<io::InputStream> stream, int64_t num_bytes)
      : InspectedFragment(std::move(column_names)),
        stream(std::move(stream)),
        num_bytes(num_bytes) {}
  std::shared_ptr<io::InputStream> stream;
  int64_t num_bytes;
};

class JsonFragmentScanner : public FragmentScanner {
 public:
  JsonFragmentScanner(ReaderPtr reader, int num_batches, int64_t block_size)
      : reader_(std::move(reader)), block_size_(block_size), num_batches_(num_batches) {}

  int NumBatches() override { return num_batches_; }

  Future<std::shared_ptr<RecordBatch>> ScanBatch(int index) override {
    DCHECK_EQ(num_scanned_++, index);
    return reader_->ReadNextAsync();
  }

  int64_t EstimatedDataBytes(int) override { return block_size_; }

  static Result<std::shared_ptr<Schema>> GetSchema(
      const FragmentScanRequest& scan_request, const JsonInspectedFragment& inspected) {
    FieldVector fields;
    fields.reserve(inspected.column_names.size());
    std::unordered_set<int> indices;
    indices.reserve(inspected.column_names.size());

    for (const auto& scan_column : scan_request.fragment_selection->columns()) {
      const auto index = scan_column.path[0];

      if (!indices.emplace(index).second) continue;

      const auto& name = inspected.column_names.at(index);
      auto type = scan_column.requested_type->GetSharedPtr();
      fields.push_back(field((name), std::move(type)));
    }

    return schema(std::move(fields));
  }

  static Future<std::shared_ptr<FragmentScanner>> Make(
      const FragmentScanRequest& scan_request,
      const JsonFragmentScanOptions& format_options,
      const JsonInspectedFragment& inspected, Executor* cpu_executor) {
    auto parse_options = format_options.parse_options;
    ARROW_ASSIGN_OR_RAISE(parse_options.explicit_schema,
                          GetSchema(scan_request, inspected));
    parse_options.unexpected_field_behavior = json::UnexpectedFieldBehavior::Ignore;

    int64_t block_size = format_options.read_options.block_size;
    auto num_batches =
        static_cast<int>(bit_util::CeilDiv(inspected.num_bytes, block_size));

    auto future = json::StreamingReader::MakeAsync(
        inspected.stream, format_options.read_options, parse_options,
        io::default_io_context(), cpu_executor);
    return future.Then([num_batches, block_size](const ReaderPtr& reader)
                           -> Result<std::shared_ptr<FragmentScanner>> {
      return std::make_shared<JsonFragmentScanner>(reader, num_batches, block_size);
    });
  }

 private:
  ReaderPtr reader_;
  int64_t block_size_;
  int num_batches_;
  int num_scanned_ = 0;
};

// Return the same parse options, but disable any options that could interfere with
// fragment inspection
json::ParseOptions GetInitialParseOptions(json::ParseOptions options) {
  options.explicit_schema = nullptr;
  options.unexpected_field_behavior = json::UnexpectedFieldBehavior::InferType;
  return options;
}

Result<std::shared_ptr<StructType>> ParseToStructType(
    std::string_view data, const json::ParseOptions& parse_options, MemoryPool* pool) {
  auto full_buffer = std::make_shared<Buffer>(data);
  std::shared_ptr<Buffer> buffer, partial;
  auto chunker = json::MakeChunker(parse_options);
  RETURN_NOT_OK(chunker->Process(full_buffer, &buffer, &partial));

  std::unique_ptr<json::BlockParser> parser;
  RETURN_NOT_OK(json::BlockParser::Make(pool, parse_options, &parser));
  RETURN_NOT_OK(parser->Parse(buffer));
  std::shared_ptr<Array> parsed;
  RETURN_NOT_OK(parser->Finish(&parsed));

  return checked_pointer_cast<StructType>(parsed->type());
}

Result<std::shared_ptr<Schema>> ParseToSchema(std::string_view data,
                                              const json::ParseOptions& parse_options,
                                              MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto type, ParseToStructType(data, parse_options, pool));
  return schema(type->fields());
}

// Converts a FieldPath to a FieldRef consisting exclusively of field names.
//
// The resulting FieldRef can be used to lookup the corresponding field in any schema
// regardless of missing/unordered fields. The input path is assumed to be valid for the
// given schema.
FieldRef ToUniversalRef(const FieldPath& path, const Schema& schema) {
  std::vector<FieldRef> refs;
  refs.reserve(path.indices().size());

  const FieldVector* fields = &schema.fields();
  for (auto it = path.begin(); it != path.end(); ++it) {
    DCHECK_LT(*it, static_cast<int>(fields->size()));
    const auto& child_field = *(*fields)[*it];
    refs.emplace_back(child_field.name());
    if (it + 1 != path.end()) {
      auto&& child_type = checked_cast<const StructType&>(*child_field.type());
      fields = &child_type.fields();
    }
  }

  return refs.empty()       ? FieldRef()
         : refs.size() == 1 ? refs[0]
                            : FieldRef(std::move(refs));
}

int TopLevelIndex(const FieldRef& ref, const Schema& schema) {
  if (const auto* name = ref.name()) {
    return schema.GetFieldIndex(*name);
  } else if (const auto* path = ref.field_path()) {
    DCHECK(!path->empty());
    return (*path)[0];
  }
  const auto* nested_refs = ref.nested_refs();
  DCHECK(nested_refs && !nested_refs->empty());
  return TopLevelIndex((*nested_refs)[0], schema);
}

// Make a new schema consisting only of the top-level fields in the dataset schema that:
//  (a) Have children that require materialization
//  (b) Have children present in `physical_schema`
//
// The resulting schema can be used in reader instantiation to ignore unused fields. Note
// that `physical_schema` is only of structural importance and its data types are ignored
// when constructing the final schema.
Result<std::shared_ptr<Schema>> GetPartialSchema(const ScanOptions& scan_options,
                                                 const Schema& physical_schema) {
  auto dataset_schema = scan_options.dataset_schema;
  DCHECK_NE(dataset_schema, nullptr);
  const auto max_num_fields = static_cast<size_t>(dataset_schema->num_fields());

  std::vector<bool> selected(max_num_fields, false);
  std::vector<int> toplevel_indices;
  toplevel_indices.reserve(max_num_fields);

  for (const auto& ref : scan_options.MaterializedFields()) {
    auto index = TopLevelIndex(ref, *dataset_schema);
    DCHECK_GE(index, 0);
    if (selected[index]) continue;

    // Determine if the field exists in the physical schema before selecting it
    bool found;
    if (!ref.IsNested()) {
      const auto& name = dataset_schema->field(index)->name();
      found = physical_schema.GetFieldIndex(name) != -1;
    } else {
      // Check if the nested field is present in the physical schema. If so, we load its
      // entire top-level field
      ARROW_ASSIGN_OR_RAISE(auto path, ref.FindOne(*dataset_schema));
      auto universal_ref = ToUniversalRef(path, *dataset_schema);
      ARROW_ASSIGN_OR_RAISE(auto match, universal_ref.FindOneOrNone(physical_schema));
      found = !match.empty();
    }

    if (!found) continue;

    toplevel_indices.push_back(index);
    selected[index] = true;
    // All fields in the dataset schema require materialization, so return early
    if (toplevel_indices.size() == max_num_fields) {
      return dataset_schema;
    }
  }

  FieldVector fields;
  fields.reserve(toplevel_indices.size());
  std::sort(toplevel_indices.begin(), toplevel_indices.end());
  for (auto index : toplevel_indices) {
    fields.push_back(dataset_schema->field(index));
  }

  return schema(std::move(fields));
}

Result<std::shared_ptr<JsonFragmentScanOptions>> GetJsonFormatOptions(
    const JsonFileFormat& format, const ScanOptions* scan_options) {
  return GetFragmentScanOptions<JsonFragmentScanOptions>(
      kJsonTypeName, scan_options, format.default_fragment_scan_options);
}

Result<Future<ReaderPtr>> DoOpenReader(
    const FileSource& source, const JsonFileFormat& format,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr) {
  ARROW_ASSIGN_OR_RAISE(auto json_options,
                        GetJsonFormatOptions(format, scan_options.get()));

  struct State {
    State(const JsonFragmentScanOptions& json_options,
          const std::shared_ptr<ScanOptions>& scan_options)
        : parse_options(GetInitialParseOptions(json_options.parse_options)),
          read_options(json_options.read_options),
          scan_options(scan_options) {}
    json::ParseOptions parse_options;
    json::ReadOptions read_options;
    std::shared_ptr<const ScanOptions> scan_options;
    std::shared_ptr<io::InputStream> stream;
  };

  auto state = std::make_shared<State>(*json_options, scan_options);
  ARROW_ASSIGN_OR_RAISE(state->stream, source.OpenCompressed());
  ARROW_ASSIGN_OR_RAISE(
      state->stream,
      io::BufferedInputStream::Create(state->read_options.block_size,
                                      default_memory_pool(), std::move(state->stream)));

  auto maybe_future = state->stream->io_context().executor()->Submit(
      [state = std::move(state)]() -> Future<ReaderPtr> {
        if (state->scan_options && state->scan_options->dataset_schema) {
          // Inspect the first block before anything else, so we can derive an explicit
          // schema for the reader based on the dataset schema.
          ARROW_ASSIGN_OR_RAISE(auto first_block,
                                state->stream->Peek(state->read_options.block_size));
          ARROW_ASSIGN_OR_RAISE(auto physical_schema,
                                ParseToSchema(first_block, state->parse_options,
                                              state->scan_options->pool));
          ARROW_ASSIGN_OR_RAISE(state->parse_options.explicit_schema,
                                GetPartialSchema(*state->scan_options, *physical_schema));
          state->parse_options.unexpected_field_behavior =
              json::UnexpectedFieldBehavior::Ignore;
        }
        return json::StreamingReader::MakeAsync(
            std::move(state->stream), state->read_options, state->parse_options);
      });
  ARROW_ASSIGN_OR_RAISE(auto future, maybe_future);
  return future.Then([](const ReaderPtr& reader) -> Result<ReaderPtr> { return reader; },
                     [path = source.path()](const Status& error) -> Result<ReaderPtr> {
                       return error.WithMessage("Could not open JSON input source '",
                                                path, "': ", error);
                     });
}

Future<ReaderPtr> OpenReaderAsync(
    const FileSource& source, const JsonFileFormat& format,
    const std::shared_ptr<ScanOptions>& scan_options = nullptr) {
  return DeferNotOk(DoOpenReader(source, format, scan_options));
}

Result<ReaderPtr> OpenReader(const FileSource& source, const JsonFileFormat& format,
                             const std::shared_ptr<ScanOptions>& scan_options = nullptr) {
  return OpenReaderAsync(source, format, scan_options).result();
}

Result<RecordBatchGenerator> MakeBatchGenerator(
    const JsonFileFormat& format, const std::shared_ptr<ScanOptions>& scan_options,
    const std::shared_ptr<FileFragment>& file) {
  ARROW_ASSIGN_OR_RAISE(auto future, DoOpenReader(file->source(), format, scan_options));
  auto maybe_reader = future.result();
  // Defer errors that occured during reader instantiation since they're likely related to
  // batch-processing.
  if (!maybe_reader.ok()) {
    return MakeFailingGenerator<std::shared_ptr<RecordBatch>>(maybe_reader.status());
  }
  return [reader = *std::move(maybe_reader)] { return reader->ReadNextAsync(); };
}

Result<std::shared_ptr<InspectedFragment>> DoInspectFragment(
    const FileSource& source, const JsonFragmentScanOptions& format_options,
    MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto stream, source.OpenCompressed());
  ARROW_ASSIGN_OR_RAISE(
      stream, io::BufferedInputStream::Create(format_options.read_options.block_size,
                                              default_memory_pool(), std::move(stream)));

  ARROW_ASSIGN_OR_RAISE(auto first_block,
                        stream->Peek(format_options.read_options.block_size));

  auto parse_options = GetInitialParseOptions(format_options.parse_options);
  ARROW_ASSIGN_OR_RAISE(auto struct_type,
                        ParseToStructType(first_block, parse_options, pool));

  std::vector<std::string> column_names;
  column_names.reserve(struct_type->num_fields());
  for (const auto& f : struct_type->fields()) {
    column_names.push_back(f->name());
  }

  return std::make_shared<JsonInspectedFragment>(std::move(column_names),
                                                 std::move(stream), source.Size());
}

}  // namespace

JsonFileFormat::JsonFileFormat()
    : FileFormat(std::make_shared<JsonFragmentScanOptions>()) {}

bool JsonFileFormat::Equals(const FileFormat& other) const {
  return type_name() == other.type_name();
}

Result<bool> JsonFileFormat::IsSupported(const FileSource& source) const {
  RETURN_NOT_OK(source.Open().status());
  return OpenReader(source, *this, nullptr).ok();
}

Result<std::shared_ptr<Schema>> JsonFileFormat::Inspect(const FileSource& source) const {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenReader(source, *this));
  return reader->schema();
}

Result<RecordBatchGenerator> JsonFileFormat::ScanBatchesAsync(
    const std::shared_ptr<ScanOptions>& scan_options,
    const std::shared_ptr<FileFragment>& file) const {
  ARROW_ASSIGN_OR_RAISE(auto gen, MakeBatchGenerator(*this, scan_options, file));
  return MakeChunkedBatchGenerator(std::move(gen), scan_options->batch_size);
}

Future<std::optional<int64_t>> JsonFileFormat::CountRows(
    const std::shared_ptr<FileFragment>& file, compute::Expression predicate,
    const std::shared_ptr<ScanOptions>& scan_options) {
  if (ExpressionHasFieldRefs(predicate)) {
    return Future<std::optional<int64_t>>::MakeFinished(std::nullopt);
  }
  ARROW_ASSIGN_OR_RAISE(auto gen, MakeBatchGenerator(*this, scan_options, file));
  auto count = std::make_shared<int64_t>(0);
  return VisitAsyncGenerator(std::move(gen),
                             [count](const std::shared_ptr<RecordBatch>& batch) {
                               *count += batch->num_rows();
                               return Status::OK();
                             })
      .Then([count]() -> std::optional<int64_t> { return *count; });
}

Future<std::shared_ptr<InspectedFragment>> JsonFileFormat::InspectFragment(
    const FileSource& source, const FragmentScanOptions* format_options,
    compute::ExecContext* exec_context) const {
  auto json_options = static_cast<const JsonFragmentScanOptions*>(format_options);
  auto* executor = source.filesystem() ? source.filesystem()->io_context().executor()
                                       : exec_context->executor();
  return DeferNotOk(
      executor->Submit([source, json_options, pool = exec_context->memory_pool()] {
        return DoInspectFragment(source, *json_options, pool);
      }));
}

Future<std::shared_ptr<FragmentScanner>> JsonFileFormat::BeginScan(
    const FragmentScanRequest& scan_request, const InspectedFragment& inspected,
    const FragmentScanOptions* format_options, compute::ExecContext* exec_context) const {
  return JsonFragmentScanner::Make(
      scan_request, static_cast<const JsonFragmentScanOptions&>(*format_options),
      static_cast<const JsonInspectedFragment&>(inspected), exec_context->executor());
}

}  // namespace dataset
}  // namespace arrow
