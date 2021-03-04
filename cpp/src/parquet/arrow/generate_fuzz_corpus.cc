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

// A command line executable that generates a bunch of valid Parquet files
// containing example record batches.  Those are used as fuzzing seeds
// to make fuzzing more efficient.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/io/file.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/random.h"
#include "arrow/util/compression.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"
#include "parquet/arrow/writer.h"

namespace arrow {

using ::arrow::internal::CreateDir;
using ::arrow::internal::PlatformFilename;
using ::parquet::WriterProperties;

static constexpr int32_t kBatchSize = 1000;
static constexpr int32_t kChunkSize = kBatchSize * 3 / 8;

std::shared_ptr<WriterProperties> GetWriterProperties() {
  WriterProperties::Builder builder{};
  builder.disable_dictionary("no_dict");
  builder.compression("compressed", Compression::BROTLI);
  return builder.build();
}

Result<std::shared_ptr<RecordBatch>> ExampleBatch1() {
  constexpr double kNullProbability = 0.2;

  random::RandomArrayGenerator gen(42);
  std::shared_ptr<Array> a, b, c, d, e, f, g, h, no_dict, compressed;
  std::shared_ptr<Field> f_a, f_b, f_c, f_d, f_e, f_f, f_g, f_h, f_no_dict, f_compressed;

  a = gen.Int16(kBatchSize, -10000, 10000, kNullProbability);
  f_a = field("a", a->type());

  b = gen.Float64(kBatchSize, -1e10, 1e10, /*null_probability=*/0.0);
  f_b = field("b", b->type());

  // A column of tiny strings that will hopefully trigger dict encoding
  c = gen.String(kBatchSize, 0, 3, kNullProbability);
  f_c = field("c", c->type());

  // A column of lists
  {
    auto values = gen.Int64(kBatchSize * 10, -10000, 10000, kNullProbability);
    auto offsets = gen.Offsets(kBatchSize + 1, 0, static_cast<int32_t>(values->length()));
    ARROW_ASSIGN_OR_RAISE(d, ListArray::FromArrays(*offsets, *values));
  }
  f_d = field("d", d->type());

  // A column of a repeated constant that will hopefully trigger RLE encoding
  ARROW_ASSIGN_OR_RAISE(e, MakeArrayFromScalar(Int16Scalar(42), kBatchSize));
  f_e = field("e", e->type());

  // A column of lists of lists
  {
    auto inner_values = gen.Int64(kBatchSize * 9, -10000, 10000, kNullProbability);
    auto inner_offsets =
        gen.Offsets(kBatchSize * 3 + 1, 0, static_cast<int32_t>(inner_values->length()),
                    kNullProbability);
    ARROW_ASSIGN_OR_RAISE(auto inner_lists,
                          ListArray::FromArrays(*inner_offsets, *inner_values));
    auto offsets = gen.Offsets(
        kBatchSize + 1, 0, static_cast<int32_t>(inner_lists->length()), kNullProbability);
    ARROW_ASSIGN_OR_RAISE(f, ListArray::FromArrays(*offsets, *inner_lists));
  }
  f_f = field("f", f->type());

  // A column of nested non-nullable structs
  {
    ARROW_ASSIGN_OR_RAISE(
        auto inner_a,
        StructArray::Make({a, b}, std::vector<std::string>{"inner1_aa", "inner1_ab"}));
    ARROW_ASSIGN_OR_RAISE(
        g, StructArray::Make({inner_a, c},
                             {field("inner1_a", inner_a->type(), /*nullable=*/false),
                              field("inner1_c", c->type())}));
  }
  f_g = field("g", g->type(), /*nullable=*/false);

  // A column of nested nullable structs
  {
    auto null_bitmap = gen.NullBitmap(kBatchSize, kNullProbability);
    ARROW_ASSIGN_OR_RAISE(
        auto inner_a,
        StructArray::Make({a, b}, std::vector<std::string>{"inner2_aa", "inner2_ab"},
                          std::move(null_bitmap)));
    null_bitmap = gen.NullBitmap(kBatchSize, kNullProbability);
    ARROW_ASSIGN_OR_RAISE(
        h,
        StructArray::Make({inner_a, c}, std::vector<std::string>{"inner2_a", "inner2_c"},
                          std::move(null_bitmap)));
  }
  f_h = field("h", h->type());

  // A non-dict-encoded column (see GetWriterProperties)
  no_dict = gen.String(kBatchSize, 0, 30, kNullProbability);
  f_no_dict = field("no_dict", no_dict->type());

  // A non-dict-encoded column (see GetWriterProperties)
  compressed = gen.Int64(kBatchSize, -10, 10, kNullProbability);
  f_compressed = field("compressed", compressed->type());

  auto schema =
      ::arrow::schema({f_a, f_b, f_c, f_d, f_e, f_f, f_g, f_h, f_compressed, f_no_dict});
  auto md = key_value_metadata({"key1", "key2"}, {"value1", ""});
  schema = schema->WithMetadata(md);
  return RecordBatch::Make(schema, kBatchSize,
                           {a, b, c, d, e, f, g, h, compressed, no_dict});
}

Result<std::vector<std::shared_ptr<RecordBatch>>> Batches() {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  ARROW_ASSIGN_OR_RAISE(auto batch, ExampleBatch1());
  batches.push_back(batch);
  return batches;
}

Status DoMain(const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(out_dir));
  RETURN_NOT_OK(CreateDir(dir_fn));

  int sample_num = 1;
  auto sample_name = [&]() -> std::string {
    return "pq-table-" + std::to_string(sample_num++);
  };

  ARROW_ASSIGN_OR_RAISE(auto batches, Batches());

  auto writer_properties = GetWriterProperties();

  for (const auto& batch : batches) {
    RETURN_NOT_OK(batch->ValidateFull());
    ARROW_ASSIGN_OR_RAISE(auto table, Table::FromRecordBatches({batch}));

    ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_name()));
    std::cerr << sample_fn.ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
    RETURN_NOT_OK(::parquet::arrow::WriteTable(*table, default_memory_pool(), file,
                                               kChunkSize, writer_properties));
    RETURN_NOT_OK(file->Close());
  }
  return Status::OK();
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: parquet-arrow-generate-fuzz-corpus "
            << "<output directory>" << std::endl;
  std::exit(2);
}

int Main(int argc, char** argv) {
  if (argc != 2) {
    Usage();
  }
  auto out_dir = std::string(argv[1]);

  Status st = DoMain(out_dir);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}

}  // namespace arrow

int main(int argc, char** argv) { return ::arrow::Main(argc, argv); }
