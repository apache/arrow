#include "benchmark/benchmark.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

namespace parquet {

using parquet::Repetition;
using parquet::test::MakePages;
using schema::NodePtr;

namespace benchmark {

static void BM_Skip(::benchmark::State& state) {
  internal::LevelInfo level_info;
  level_info.def_level = state.range(0);
  level_info.rep_level = state.range(1);
  // If true, tests skip, otherwise tests read.
  const int skip = state.range(2);
  const int batch_size = state.range(3);

  Repetition::type repetition = Repetition::REQUIRED;
  if (level_info.def_level > 0) {
    repetition = Repetition::OPTIONAL;
  }
  if (level_info.rep_level > 0) {
    repetition = Repetition::REPEATED;
  }
  NodePtr type = schema::Int32("b", repetition);
  const ColumnDescriptor descr(type, level_info.def_level, level_info.rep_level);

  const int num_pages = 5;
  const int levels_per_page = 100000;
  // Vectors filled with random rep/defs and values to make pages.
  std::vector<int32_t> values;
  std::vector<int16_t> def_levels;
  std::vector<int16_t> rep_levels;
  std::vector<uint8_t> data_buffer;
  std::vector<std::shared_ptr<Page>> pages;
  MakePages<Int32Type>(&descr, num_pages, levels_per_page, def_levels, rep_levels, values,
                       data_buffer, pages, Encoding::PLAIN);

  // Vectors to read the values into.
  std::vector<int32_t> read_values(batch_size, -1);
  std::vector<int16_t> read_defs(batch_size, -1);
  std::vector<int16_t> read_reps(batch_size, -1);

  while (state.KeepRunning()) {
    state.PauseTiming();
    std::unique_ptr<PageReader> pager;
    pager.reset(new test::MockPageReader(pages));
    std::shared_ptr<ColumnReader> column_reader =
        ColumnReader::Make(&descr, std::move(pager));
    Int32Reader* reader = static_cast<Int32Reader*>(column_reader.get());
    int values_count = -1;
    state.ResumeTiming();
    while (values_count != 0) {
      if (skip == 1) {
        values_count = reader->Skip(batch_size);
      } else {
        int64_t values_read = 0;
        values_count = reader->ReadBatch(batch_size, read_defs.data(), read_reps.data(),
                                         read_values.data(), &values_read);
      }
    }
  }
}

BENCHMARK(BM_Skip)
    ->Iterations(1000)
    ->Args({0, 0, 0, 1})
    ->Args({0, 0, 0, 1000})
    ->Args({0, 0, 0, 10000})
    ->Args({0, 0, 0, 100000})
    ->Args({0, 0, 1, 1})
    ->Args({0, 0, 1, 1000})
    ->Args({0, 0, 1, 10000})
    ->Args({0, 0, 1, 100000})
    ->Args({1, 0, 0, 1})
    ->Args({1, 0, 0, 1000})
    ->Args({1, 0, 0, 10000})
    ->Args({1, 0, 0, 100000})
    ->Args({1, 0, 1, 1})
    ->Args({1, 0, 1, 1000})
    ->Args({1, 0, 1, 10000})
    ->Args({1, 0, 1, 100000})
    ->Args({1, 1, 0, 1})
    ->Args({1, 1, 0, 1000})
    ->Args({1, 1, 0, 10000})
    ->Args({1, 1, 0, 100000})
    ->Args({1, 1, 1, 1})
    ->Args({1, 1, 1, 1000})
    ->Args({1, 1, 1, 10000})
    ->Args({1, 1, 1, 100000});

}  // namespace benchmark
}  // namespace parquet
