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

#ifdef _MSC_VER
#  pragma warning(push)
// Disable forcing value to bool warnings
#  pragma warning(disable : 4800)
#endif

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <cstdint>
#include <functional>
#include <set>
#include <sstream>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/extension/json.h"
#include "arrow/io/api.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/range.h"

#ifdef ARROW_CSV
#  include "arrow/csv/api.h"
#endif

#include "parquet/api/reader.h"

#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test_util.h"
#include "parquet/bloom_filter.h"
#include "parquet/bloom_filter_reader.h"
#include "parquet/test_util.h"

using arrow::Array;
using arrow::ArrayData;
using arrow::ArrayFromJSON;
using arrow::ArrayVector;
using arrow::ArrayVisitor;
using arrow::Buffer;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Datum;
using arrow::DecimalType;
using arrow::default_memory_pool;
using arrow::DictionaryArray;
using arrow::ListArray;
using arrow::PrimitiveArray;
using arrow::ResizableBuffer;
using arrow::Result;
using arrow::Scalar;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;
using arrow::compute::DictionaryEncode;
using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;
using arrow::internal::Iota;
using arrow::io::BufferReader;

using arrow::randint;
using arrow::random_is_valid;

using ArrowId = ::arrow::Type;
using ParquetType = parquet::Type;

using parquet::arrow::FromParquetSchema;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

namespace arrow::parquet {

namespace {

struct ColumnIndexObject {
  std::vector<bool> null_pages;
  std::vector<std::string> min_values;
  std::vector<std::string> max_values;
  BoundaryOrder::type boundary_order = BoundaryOrder::Unordered;
  std::vector<int64_t> null_counts;

  ColumnIndexObject() = default;

  ColumnIndexObject(const std::vector<bool>& null_pages,
                    const std::vector<std::string>& min_values,
                    const std::vector<std::string>& max_values,
                    BoundaryOrder::type boundary_order,
                    const std::vector<int64_t>& null_counts)
      : null_pages(null_pages),
        min_values(min_values),
        max_values(max_values),
        boundary_order(boundary_order),
        null_counts(null_counts) {}

  explicit ColumnIndexObject(const ColumnIndex* column_index) {
    if (column_index == nullptr) {
      return;
    }
    null_pages = column_index->null_pages();
    min_values = column_index->encoded_min_values();
    max_values = column_index->encoded_max_values();
    boundary_order = column_index->boundary_order();
    if (column_index->has_null_counts()) {
      null_counts = column_index->null_counts();
    }
  }

  bool operator==(const ColumnIndexObject& b) const {
    return null_pages == b.null_pages && min_values == b.min_values &&
           max_values == b.max_values && boundary_order == b.boundary_order &&
           null_counts == b.null_counts;
  }
};

auto encode_int64 = [](int64_t value) {
  return std::string(reinterpret_cast<const char*>(&value), sizeof(int64_t));
};

auto encode_double = [](double value) {
  return std::string(reinterpret_cast<const char*>(&value), sizeof(double));
};

}  // namespace

class TestingWithPageIndex {
 public:
  void WriteFile(const std::shared_ptr<WriterProperties>& writer_properties,
                 const std::shared_ptr<::arrow::Table>& table) {
    // Get schema from table.
    auto schema = table->schema();
    std::shared_ptr<SchemaDescriptor> parquet_schema;
    auto arrow_writer_properties = default_arrow_writer_properties();
    ASSERT_OK_NO_THROW(ToParquetSchema(schema.get(), *writer_properties,
                                       *arrow_writer_properties, &parquet_schema));
    auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

    // Write table to buffer.
    auto sink = CreateOutputStream();
    auto pool = ::arrow::default_memory_pool();
    auto writer = ParquetFileWriter::Open(sink, schema_node, writer_properties);
    std::unique_ptr<FileWriter> arrow_writer;
    ASSERT_OK(FileWriter::Make(pool, std::move(writer), schema, arrow_writer_properties,
                               &arrow_writer));
    ASSERT_OK_NO_THROW(arrow_writer->WriteTable(*table));
    ASSERT_OK_NO_THROW(arrow_writer->Close());
    ASSERT_OK_AND_ASSIGN(buffer_, sink->Finish());
  }

 protected:
  std::shared_ptr<Buffer> buffer_;
};

class ParquetPageIndexRoundTripTest : public ::testing::Test,
                                      public TestingWithPageIndex {
 public:
  void ReadPageIndexes(int expect_num_row_groups, int expect_num_pages,
                       const std::set<int>& expect_columns_without_index = {}) {
    auto read_properties = default_arrow_reader_properties();
    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));

    auto metadata = reader->metadata();
    ASSERT_EQ(expect_num_row_groups, metadata->num_row_groups());

    auto page_index_reader = reader->GetPageIndexReader();
    ASSERT_NE(page_index_reader, nullptr);

    int64_t offset_lower_bound = 0;
    for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
      auto row_group_index_reader = page_index_reader->RowGroup(rg);
      ASSERT_NE(row_group_index_reader, nullptr);

      auto row_group_reader = reader->RowGroup(rg);
      ASSERT_NE(row_group_reader, nullptr);

      for (int col = 0; col < metadata->num_columns(); ++col) {
        auto column_index = row_group_index_reader->GetColumnIndex(col);
        column_indexes_.emplace_back(column_index.get());

        bool expect_no_page_index =
            expect_columns_without_index.find(col) != expect_columns_without_index.cend();

        auto offset_index = row_group_index_reader->GetOffsetIndex(col);
        if (expect_no_page_index) {
          ASSERT_EQ(offset_index, nullptr);
        } else {
          CheckOffsetIndex(offset_index.get(), expect_num_pages, &offset_lower_bound);
        }

        // Verify page stats are not written to page header if page index is enabled.
        auto page_reader = row_group_reader->GetColumnPageReader(col);
        ASSERT_NE(page_reader, nullptr);
        std::shared_ptr<Page> page = nullptr;
        while ((page = page_reader->NextPage()) != nullptr) {
          if (page->type() == PageType::DATA_PAGE ||
              page->type() == PageType::DATA_PAGE_V2) {
            ASSERT_EQ(std::static_pointer_cast<DataPage>(page)->statistics().is_set(),
                      expect_no_page_index);
          }
        }
      }
    }
  }

 private:
  void CheckOffsetIndex(const OffsetIndex* offset_index, int expect_num_pages,
                        int64_t* offset_lower_bound_in_out) {
    ASSERT_NE(offset_index, nullptr);
    const auto& locations = offset_index->page_locations();
    ASSERT_EQ(static_cast<size_t>(expect_num_pages), locations.size());
    int64_t prev_first_row_index = -1;
    for (const auto& location : locations) {
      // Make sure first_row_index is in the ascending order within a row group.
      ASSERT_GT(location.first_row_index, prev_first_row_index);
      // Make sure page offset is in the ascending order across the file.
      ASSERT_GE(location.offset, *offset_lower_bound_in_out);
      // Make sure page size is positive.
      ASSERT_GT(location.compressed_page_size, 0);
      prev_first_row_index = location.first_row_index;
      *offset_lower_bound_in_out = location.offset + location.compressed_page_size;
    }
  }

 protected:
  std::vector<ColumnIndexObject> column_indexes_;
};

TEST_F(ParquetPageIndexRoundTripTest, SimpleRoundTrip) {
  auto writer_properties = WriterProperties::Builder()
                               .enable_write_page_index()
                               ->max_row_group_length(4)
                               ->build();
  auto schema = ::arrow::schema({::arrow::field("c0", ::arrow::int64()),
                                 ::arrow::field("c1", ::arrow::utf8()),
                                 ::arrow::field("c2", ::arrow::list(::arrow::int64()))});
  WriteFile(writer_properties, ::arrow::TableFromJSON(schema, {R"([
      [1,     "a",  [1]      ],
      [2,     "b",  [1, 2]   ],
      [3,     "c",  [null]   ],
      [null,  "d",  []       ],
      [5,     null, [3, 3, 3]],
      [6,     "f",  null     ]
    ])"}));

  ReadPageIndexes(/*expect_num_row_groups=*/2, /*expect_num_pages=*/1);

  EXPECT_THAT(
      column_indexes_,
      ::testing::ElementsAre(
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(1)},
                            /*max_values=*/{encode_int64(3)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{1}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{"a"},
                            /*max_values=*/{"d"}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(1)},
                            /*max_values=*/{encode_int64(2)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{2}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(5)},
                            /*max_values=*/{encode_int64(6)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{"f"},
                            /*max_values=*/{"f"}, BoundaryOrder::Ascending,
                            /*null_counts=*/{1}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(3)},
                            /*max_values=*/{encode_int64(3)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{1}}));
}

TEST_F(ParquetPageIndexRoundTripTest, SimpleRoundTripWithStatsDisabled) {
  auto writer_properties = WriterProperties::Builder()
                               .enable_write_page_index()
                               ->disable_statistics()
                               ->build();
  auto schema = ::arrow::schema({::arrow::field("c0", ::arrow::int64()),
                                 ::arrow::field("c1", ::arrow::utf8()),
                                 ::arrow::field("c2", ::arrow::list(::arrow::int64()))});
  WriteFile(writer_properties, ::arrow::TableFromJSON(schema, {R"([
      [1,     "a",  [1]      ],
      [2,     "b",  [1, 2]   ],
      [3,     "c",  [null]   ],
      [null,  "d",  []       ],
      [5,     null, [3, 3, 3]],
      [6,     "f",  null     ]
    ])"}));

  ReadPageIndexes(/*expect_num_row_groups=*/1, /*expect_num_pages=*/1);
  for (auto& column_index : column_indexes_) {
    // Means page index is empty.
    EXPECT_EQ(ColumnIndexObject{}, column_index);
  }
}

TEST_F(ParquetPageIndexRoundTripTest, SimpleRoundTripWithColumnStatsDisabled) {
  auto writer_properties = WriterProperties::Builder()
                               .enable_write_page_index()
                               ->disable_statistics("c0")
                               ->max_row_group_length(4)
                               ->build();
  auto schema = ::arrow::schema({::arrow::field("c0", ::arrow::int64()),
                                 ::arrow::field("c1", ::arrow::utf8()),
                                 ::arrow::field("c2", ::arrow::list(::arrow::int64()))});
  WriteFile(writer_properties, ::arrow::TableFromJSON(schema, {R"([
      [1,     "a",  [1]      ],
      [2,     "b",  [1, 2]   ],
      [3,     "c",  [null]   ],
      [null,  "d",  []       ],
      [5,     null, [3, 3, 3]],
      [6,     "f",  null     ]
    ])"}));

  ReadPageIndexes(/*expect_num_row_groups=*/2, /*expect_num_pages=*/1);

  ColumnIndexObject empty_column_index{};
  EXPECT_THAT(
      column_indexes_,
      ::testing::ElementsAre(
          empty_column_index,
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{"a"},
                            /*max_values=*/{"d"}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(1)},
                            /*max_values=*/{encode_int64(2)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{2}},
          empty_column_index,
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{"f"},
                            /*max_values=*/{"f"}, BoundaryOrder::Ascending,
                            /*null_counts=*/{1}},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(3)},
                            /*max_values=*/{encode_int64(3)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{1}}));
}

TEST_F(ParquetPageIndexRoundTripTest, DropLargeStats) {
  auto writer_properties = WriterProperties::Builder()
                               .enable_write_page_index()
                               ->max_row_group_length(1) /* write single-row row group */
                               ->max_statistics_size(20) /* drop stats larger than it */
                               ->build();
  auto schema = ::arrow::schema({::arrow::field("c0", ::arrow::utf8())});
  WriteFile(writer_properties, ::arrow::TableFromJSON(schema, {R"([
      ["short_string"],
      ["very_large_string_to_drop_stats"]
    ])"}));

  ReadPageIndexes(/*expect_num_row_groups=*/2, /*expect_num_pages=*/1);

  EXPECT_THAT(
      column_indexes_,
      ::testing::ElementsAre(
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{"short_string"},
                            /*max_values=*/{"short_string"}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{}));
}

TEST_F(ParquetPageIndexRoundTripTest, MultiplePages) {
  auto writer_properties = WriterProperties::Builder()
                               .enable_write_page_index()
                               ->data_pagesize(1) /* write multiple pages */
                               ->build();
  auto schema = ::arrow::schema(
      {::arrow::field("c0", ::arrow::int64()), ::arrow::field("c1", ::arrow::utf8())});
  WriteFile(
      writer_properties,
      ::arrow::TableFromJSON(
          schema, {R"([[1, "a"], [2, "b"]])", R"([[3, "c"], [4, "d"]])",
                   R"([[null, null], [6, "f"]])", R"([[null, null], [null, null]])"}));

  ReadPageIndexes(/*expect_num_row_groups=*/1, /*expect_num_pages=*/4);

  EXPECT_THAT(
      column_indexes_,
      ::testing::ElementsAre(
          ColumnIndexObject{
              /*null_pages=*/{false, false, false, true},
              /*min_values=*/{encode_int64(1), encode_int64(3), encode_int64(6), ""},
              /*max_values=*/{encode_int64(2), encode_int64(4), encode_int64(6), ""},
              BoundaryOrder::Ascending,
              /*null_counts=*/{0, 0, 1, 2}},
          ColumnIndexObject{/*null_pages=*/{false, false, false, true},
                            /*min_values=*/{"a", "c", "f", ""},
                            /*max_values=*/{"b", "d", "f", ""}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0, 0, 1, 2}}));
}

TEST_F(ParquetPageIndexRoundTripTest, DoubleWithNaNs) {
  auto writer_properties = WriterProperties::Builder()
                               .enable_write_page_index()
                               ->max_row_group_length(3) /* 3 rows per row group */
                               ->build();

  // Create table to write with NaNs.
  auto vectors = std::vector<std::shared_ptr<Array>>(4);
  // NaN will be ignored in min/max stats.
  ::arrow::ArrayFromVector<::arrow::DoubleType>({1.0, NAN, 0.1}, &vectors[0]);
  // Lower bound will use -0.0.
  ::arrow::ArrayFromVector<::arrow::DoubleType>({+0.0, NAN, +0.0}, &vectors[1]);
  // Upper bound will use -0.0.
  ::arrow::ArrayFromVector<::arrow::DoubleType>({-0.0, NAN, -0.0}, &vectors[2]);
  // Pages with all NaNs will not build column index.
  ::arrow::ArrayFromVector<::arrow::DoubleType>({NAN, NAN, NAN}, &vectors[3]);
  ASSERT_OK_AND_ASSIGN(auto chunked_array,
                       arrow::ChunkedArray::Make(vectors, ::arrow::float64()));

  auto schema = ::arrow::schema({::arrow::field("c0", ::arrow::float64())});
  auto table = Table::Make(schema, {chunked_array});
  WriteFile(writer_properties, table);

  ReadPageIndexes(/*expect_num_row_groups=*/4, /*expect_num_pages=*/1);

  EXPECT_THAT(
      column_indexes_,
      ::testing::ElementsAre(
          ColumnIndexObject{/*null_pages=*/{false},
                            /*min_values=*/{encode_double(0.1)},
                            /*max_values=*/{encode_double(1.0)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{/*null_pages=*/{false},
                            /*min_values=*/{encode_double(-0.0)},
                            /*max_values=*/{encode_double(+0.0)},
                            BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{/*null_pages=*/{false},
                            /*min_values=*/{encode_double(-0.0)},
                            /*max_values=*/{encode_double(+0.0)},
                            BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{
              /* Page with only NaN values does not have column index built */}));
}

TEST_F(ParquetPageIndexRoundTripTest, EnablePerColumn) {
  auto schema = ::arrow::schema({::arrow::field("c0", ::arrow::int64()),
                                 ::arrow::field("c1", ::arrow::int64()),
                                 ::arrow::field("c2", ::arrow::int64())});
  auto writer_properties =
      WriterProperties::Builder()
          .enable_write_page_index()       /* enable by default */
          ->enable_write_page_index("c0")  /* enable c0 explicitly */
          ->disable_write_page_index("c1") /* disable c1 explicitly */
          ->build();
  WriteFile(writer_properties, ::arrow::TableFromJSON(schema, {R"([[0,  1,  2]])"}));

  ReadPageIndexes(/*expect_num_row_groups=*/1, /*expect_num_pages=*/1,
                  /*expect_columns_without_index=*/{1});

  EXPECT_THAT(
      column_indexes_,
      ::testing::ElementsAre(
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(0)},
                            /*max_values=*/{encode_int64(0)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}},
          ColumnIndexObject{/* page index of c1 is disabled */},
          ColumnIndexObject{/*null_pages=*/{false}, /*min_values=*/{encode_int64(2)},
                            /*max_values=*/{encode_int64(2)}, BoundaryOrder::Ascending,
                            /*null_counts=*/{0}}));
}

class ParquetBloomFilterRoundTripTest : public ::testing::Test,
                                        public TestingWithPageIndex {
 public:
  void ReadBloomFilters(int expect_num_row_groups,
                        const std::set<int>& expect_columns_without_filter = {}) {
    auto reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer_));

    auto metadata = reader->metadata();
    ASSERT_EQ(expect_num_row_groups, metadata->num_row_groups());

    auto& bloom_filter_reader = reader->GetBloomFilterReader();

    for (int rg = 0; rg < metadata->num_row_groups(); ++rg) {
      auto row_group_reader = bloom_filter_reader.RowGroup(rg);
      ASSERT_NE(row_group_reader, nullptr);

      for (int col = 0; col < metadata->num_columns(); ++col) {
        bool expect_no_bloom_filter = expect_columns_without_filter.find(col) !=
                                      expect_columns_without_filter.cend();

        auto bloom_filter = row_group_reader->GetColumnBloomFilter(col);
        if (expect_no_bloom_filter) {
          ASSERT_EQ(nullptr, bloom_filter);
        } else {
          ASSERT_NE(nullptr, bloom_filter);
          bloom_filters_.push_back(std::move(bloom_filter));
        }
      }
    }
  }

  template <typename ArrowType>
  void VerifyBloomFilterContains(const BloomFilter* bloom_filter,
                                 const ::arrow::ChunkedArray& chunked_array) {
    for (auto value : ::arrow::stl::Iterate<ArrowType>(chunked_array)) {
      if (value == std::nullopt) {
        continue;
      }
      EXPECT_TRUE(bloom_filter->FindHash(bloom_filter->Hash(value.value())));
    }
  }

  template <typename ArrowType>
  void VerifyBloomFilterNotContains(const BloomFilter* bloom_filter,
                                    const ::arrow::ChunkedArray& chunked_array) {
    for (auto value : ::arrow::stl::Iterate<ArrowType>(chunked_array)) {
      if (value == std::nullopt) {
        continue;
      }
      EXPECT_FALSE(bloom_filter->FindHash(bloom_filter->Hash(value.value())));
    }
  }

 protected:
  std::vector<std::unique_ptr<BloomFilter>> bloom_filters_;
};

TEST_F(ParquetBloomFilterRoundTripTest, SimpleRoundTrip) {
  auto schema = ::arrow::schema(
      {::arrow::field("c0", ::arrow::int64()), ::arrow::field("c1", ::arrow::utf8())});
  BloomFilterOptions options;
  options.ndv = 10;
  auto writer_properties = WriterProperties::Builder()
                               .enable_bloom_filter_options(options, "c0")
                               ->enable_bloom_filter_options(options, "c1")
                               ->max_row_group_length(4)
                               ->build();
  auto table = ::arrow::TableFromJSON(schema, {R"([
        [1,     "a"],
        [2,     "b"],
        [3,     "c"],
        [null,  "d"],
        [5,     null],
        [6,     "f"]
  ])"});
  WriteFile(writer_properties, table);

  ReadBloomFilters(/*expect_num_row_groups=*/2);
  ASSERT_EQ(4, bloom_filters_.size());
  std::vector<int64_t> row_group_row_count{4, 2};
  int64_t current_row = 0;
  int64_t bloom_filter_idx = 0;  // current index in `bloom_filters_`
  for (int64_t row_group_id = 0; row_group_id < 2; ++row_group_id) {
    {
      // The bloom filter for same column in another row-group.
      int64_t bloom_filter_idx_another_rg =
          row_group_id == 0 ? bloom_filter_idx + 2 : bloom_filter_idx - 2;
      ASSERT_NE(nullptr, bloom_filters_[bloom_filter_idx]);
      auto col = table->column(0)->Slice(current_row, row_group_row_count[row_group_id]);
      VerifyBloomFilterContains<::arrow::Int64Type>(
          bloom_filters_[bloom_filter_idx].get(), *col);
      VerifyBloomFilterNotContains<::arrow::Int64Type>(
          bloom_filters_[bloom_filter_idx_another_rg].get(), *col);
      ++bloom_filter_idx;
    }
    {
      int64_t bloom_filter_idx_another_rg =
          row_group_id == 0 ? bloom_filter_idx + 2 : bloom_filter_idx - 2;
      ASSERT_NE(nullptr, bloom_filters_[bloom_filter_idx]);
      auto col = table->column(1)->Slice(current_row, row_group_row_count[row_group_id]);
      VerifyBloomFilterContains<::arrow::StringType>(
          bloom_filters_[bloom_filter_idx].get(), *col);
      VerifyBloomFilterNotContains<::arrow::StringType>(
          bloom_filters_[bloom_filter_idx_another_rg].get(), *col);
      ++bloom_filter_idx;
    }
    current_row += row_group_row_count[row_group_id];
  }
}

TEST_F(ParquetBloomFilterRoundTripTest, SimpleRoundTripDictionary) {
  for (const auto& arrow_utf8_type :
       {::arrow::utf8(), ::arrow::large_utf8(), ::arrow::utf8_view()}) {
    auto origin_schema = ::arrow::schema(
        {::arrow::field("c0", ::arrow::int64()), ::arrow::field("c1", arrow_utf8_type)});
    auto schema = ::arrow::schema(
        {::arrow::field("c0", ::arrow::dictionary(::arrow::int64(), ::arrow::int64())),
         ::arrow::field("c1", ::arrow::dictionary(::arrow::int64(), ::arrow::utf8()))});
    bloom_filters_.clear();
    BloomFilterOptions options;
    options.ndv = 10;
    auto writer_properties = WriterProperties::Builder()
                                 .enable_bloom_filter_options(options, "c0")
                                 ->enable_bloom_filter_options(options, "c1")
                                 ->max_row_group_length(4)
                                 ->build();
    std::vector<std::string> contents = {R"([
        [1,     "a"],
        [2,     "b"],
        [3,     "c"],
        [null,  "d"],
        [5,     null],
        [6,     "f"]
  ])"};
    auto dict_encoded_table = ::arrow::TableFromJSON(schema, contents);
    // using non_dict_table to adapt some interface which doesn't support dictionary.
    auto table = ::arrow::TableFromJSON(origin_schema, contents);
    WriteFile(writer_properties, dict_encoded_table);

    ReadBloomFilters(/*expect_num_row_groups=*/2);
    ASSERT_EQ(4, bloom_filters_.size());
    std::vector<int64_t> row_group_row_count{4, 2};
    int64_t current_row = 0;
    int64_t bloom_filter_idx = 0;  // current index in `bloom_filters_`
    for (int64_t row_group_id = 0; row_group_id < 2; ++row_group_id) {
      {
        // The bloom filter for same column in another row-group.
        int64_t bloom_filter_idx_another_rg =
            row_group_id == 0 ? bloom_filter_idx + 2 : bloom_filter_idx - 2;
        ASSERT_NE(nullptr, bloom_filters_[bloom_filter_idx]);
        auto col =
            table->column(0)->Slice(current_row, row_group_row_count[row_group_id]);
        VerifyBloomFilterContains<::arrow::Int64Type>(
            bloom_filters_[bloom_filter_idx].get(), *col);
        VerifyBloomFilterNotContains<::arrow::Int64Type>(
            bloom_filters_[bloom_filter_idx_another_rg].get(), *col);
        ++bloom_filter_idx;
      }
      {
        int64_t bloom_filter_idx_another_rg =
            row_group_id == 0 ? bloom_filter_idx + 2 : bloom_filter_idx - 2;
        ASSERT_NE(nullptr, bloom_filters_[bloom_filter_idx]);
        auto col =
            table->column(1)->Slice(current_row, row_group_row_count[row_group_id]);
        if (arrow_utf8_type->id() == ::arrow::Type::STRING) {
          // For STRING, we can use the same function.
          VerifyBloomFilterContains<::arrow::StringType>(
              bloom_filters_[bloom_filter_idx].get(), *col);
          VerifyBloomFilterNotContains<::arrow::StringType>(
              bloom_filters_[bloom_filter_idx_another_rg].get(), *col);
        } else if (arrow_utf8_type->id() == ::arrow::Type::LARGE_STRING) {
          // For LARGE_STRING, we can use the same function.
          VerifyBloomFilterContains<::arrow::LargeStringType>(
              bloom_filters_[bloom_filter_idx].get(), *col);
          VerifyBloomFilterNotContains<::arrow::LargeStringType>(
              bloom_filters_[bloom_filter_idx_another_rg].get(), *col);
        } else if (arrow_utf8_type->id() == ::arrow::Type::STRING_VIEW) {
          // For STRING_VIEW, we can use the same function.
          VerifyBloomFilterContains<::arrow::StringViewType>(
              bloom_filters_[bloom_filter_idx].get(), *col);
          VerifyBloomFilterNotContains<::arrow::StringViewType>(
              bloom_filters_[bloom_filter_idx_another_rg].get(), *col);
        }
        ++bloom_filter_idx;
      }
      current_row += row_group_row_count[row_group_id];
    }
  }
}

TEST_F(ParquetBloomFilterRoundTripTest, SimpleRoundTripWithOneFilter) {
  auto schema = ::arrow::schema(
      {::arrow::field("c0", ::arrow::int64()), ::arrow::field("c1", ::arrow::utf8())});
  BloomFilterOptions options;
  options.ndv = 10;
  auto writer_properties = WriterProperties::Builder()
                               .enable_bloom_filter_options(options, "c0")
                               ->disable_bloom_filter("c1")
                               ->max_row_group_length(4)
                               ->build();
  auto table = ::arrow::TableFromJSON(schema, {R"([
        [1,     "a"],
        [2,     "b"],
        [3,     "c"],
        [null,  "d"],
        [5,     null],
        [6,     "f"]
  ])"});
  WriteFile(writer_properties, table);

  ReadBloomFilters(/*expect_num_row_groups=*/2, /*expect_columns_without_filter=*/{1});
  ASSERT_EQ(2, bloom_filters_.size());
  std::vector<int64_t> row_group_row_count{4, 2};
  int64_t current_row = 0;
  int64_t bloom_filter_idx = 0;  // current index in `bloom_filters_`
  for (int64_t row_group_id = 0; row_group_id < 2; ++row_group_id) {
    {
      ASSERT_NE(nullptr, bloom_filters_[bloom_filter_idx]);
      auto col = table->column(0)->Slice(current_row, row_group_row_count[row_group_id]);
      VerifyBloomFilterContains<::arrow::Int64Type>(
          bloom_filters_[bloom_filter_idx].get(), *col);
      ++bloom_filter_idx;
    }
    current_row += row_group_row_count[row_group_id];
  }
}

TEST_F(ParquetBloomFilterRoundTripTest, ThrowForBoolean) {
  auto schema = ::arrow::schema({::arrow::field("boolean_col", ::arrow::boolean())});
  BloomFilterOptions options;
  options.ndv = 10;
  auto writer_properties = WriterProperties::Builder()
                               .enable_bloom_filter_options(options, "boolean_col")
                               ->max_row_group_length(4)
                               ->build();
  auto table = ::arrow::TableFromJSON(schema, {R"([
        [true],
        [null],
        [false]
  ])"});
  std::shared_ptr<SchemaDescriptor> parquet_schema;
  auto arrow_writer_properties = default_arrow_writer_properties();
  ASSERT_OK_NO_THROW(ToParquetSchema(schema.get(), *writer_properties,
                                     *arrow_writer_properties, &parquet_schema));
  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

  // Write table to buffer.
  auto sink = CreateOutputStream();
  auto pool = ::arrow::default_memory_pool();
  auto writer = ParquetFileWriter::Open(sink, schema_node, writer_properties);
  std::unique_ptr<FileWriter> arrow_writer;
  ASSERT_OK(FileWriter::Make(pool, std::move(writer), schema, arrow_writer_properties,
                             &arrow_writer));
  auto s = arrow_writer->WriteTable(*table);
  EXPECT_TRUE(s.IsIOError());
  EXPECT_THAT(s.message(),
              ::testing::HasSubstr("BloomFilterBuilder does not support boolean type"));
}

}  // namespace arrow::parquet