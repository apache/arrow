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

#include "parquet/file_rewriter.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <unordered_set>
#include <utility>

#include "arrow/util/compression.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "parquet/bloom_filter.h"  // IWYU pragma: keep
#include "parquet/bloom_filter_reader.h"
#include "parquet/bloom_filter_writer.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/index_location.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

namespace {
void CopyStream(std::shared_ptr<ArrowInputStream> from,
                std::shared_ptr<ArrowOutputStream> to, int64_t size,
                ::arrow::MemoryPool* pool) {
  int64_t bytes_copied = 0;
  if (from->supports_zero_copy()) {
    while (bytes_copied < size) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, from->Read(size - bytes_copied));
      if (buffer->size() == 0) {
        throw ParquetException("Unexpected end of stream at ", bytes_copied);
      }
      PARQUET_THROW_NOT_OK(to->Write(buffer->data(), buffer->size()));
      bytes_copied += buffer->size();
    }
    return;
  }

  std::shared_ptr<ResizableBuffer> buffer =
      AllocateBuffer(pool, kDefaultOutputStreamSize);
  while (bytes_copied < size) {
    PARQUET_ASSIGN_OR_THROW(auto read_size,
                            from->Read(std::min(size - bytes_copied, buffer->size()),
                                       buffer->mutable_data()));
    if (read_size == 0) {
      throw ParquetException("Unexpected end of stream at ", bytes_copied);
    }
    PARQUET_THROW_NOT_OK(to->Write(buffer->data(), read_size));
    bytes_copied += read_size;
  }
}
}  // namespace

const std::shared_ptr<RewriterProperties>& default_rewriter_properties() {
  static std::shared_ptr<RewriterProperties> default_rewriter_properties =
      RewriterProperties::Builder().build();
  return default_rewriter_properties;
}

class PagesRewriter {
 public:
  PagesRewriter(const RewriterProperties& props, std::unique_ptr<PageReader> page_reader,
                std::unique_ptr<PageWriter> page_writer,
                std::shared_ptr<OffsetIndex> original_offset_index)
      : props_(props),
        page_reader_(std::move(page_reader)),
        page_writer_(std::move(page_writer)),
        original_offset_index_(std::move(original_offset_index)) {}

  void WritePages() {
    bool has_dictionary = false;
    bool fallback = false;
    std::shared_ptr<Page> page;
    size_t page_no = 0;
    while ((page = page_reader_->NextPage()) != nullptr) {
      switch (page->type()) {
        case parquet::PageType::DICTIONARY_PAGE: {
          WriteDictionaryPage(*static_cast<const DictionaryPage*>(page.get()));
          has_dictionary = true;
          break;
        }
        case parquet::PageType::DATA_PAGE: {
          auto& data_page = *static_cast<const DataPageV1*>(page.get());
          if (data_page.encoding() != Encoding::PLAIN_DICTIONARY) {
            fallback = true;
          }
          WriteDataPageV1(data_page, page_no);
          page_no++;
          break;
        }
        case parquet::PageType::DATA_PAGE_V2: {
          auto& data_page = *static_cast<const DataPageV2*>(page.get());
          if (data_page.encoding() != Encoding::PLAIN_DICTIONARY) {
            fallback = true;
          }
          WriteDataPageV2(data_page, page_no);
          page_no++;
          break;
        }
        default: {
          ARROW_LOG(DEBUG) << "Unsupported page type: " << static_cast<int>(page->type());
          break;
        }
      }
    }
    page_writer_->Close(has_dictionary, has_dictionary && fallback);
  }

  int64_t total_uncompressed_size() const { return total_uncompressed_size_; }

 private:
  void WriteDictionaryPage(const DictionaryPage& dict_page) {
    total_uncompressed_size_ += page_writer_->WriteDictionaryPage(dict_page);
  }

  void WriteDataPageV1(const DataPageV1& data_page, const size_t page_no) {
    std::shared_ptr<Buffer> compressed_data;
    if (page_writer_->has_compressor()) {
      auto buffer = std::static_pointer_cast<ResizableBuffer>(
          AllocateBuffer(props_.memory_pool(), data_page.size()));
      page_writer_->Compress(*data_page.buffer(), buffer.get());
      compressed_data = std::move(buffer);
    } else {
      compressed_data = data_page.buffer();
    }
    auto first_row_index =
        original_offset_index_
            ? std::optional{original_offset_index_->page_locations()[page_no]
                                .first_row_index}
            : std::nullopt;
    SizeStatistics size_statistics;
    size_statistics.unencoded_byte_array_data_bytes =
        original_offset_index_ &&
                !original_offset_index_->unencoded_byte_array_data_bytes().empty()
            ? std::optional{original_offset_index_
                                ->unencoded_byte_array_data_bytes()[page_no]}
            : std::nullopt;
    DataPageV1 new_page(compressed_data, data_page.num_values(), data_page.encoding(),
                        data_page.definition_level_encoding(),
                        data_page.repetition_level_encoding(),
                        data_page.uncompressed_size(), data_page.statistics(),
                        first_row_index, size_statistics);
    total_uncompressed_size_ += page_writer_->WriteDataPage(new_page);
  }

  void WriteDataPageV2(const DataPageV2& data_page, const size_t page_no) {
    int32_t levels_byte_len = data_page.repetition_levels_byte_length() +
                              data_page.definition_levels_byte_length();
    bool page_is_compressed = false;
    std::shared_ptr<Buffer> output_buffer;
    if (page_writer_->has_compressor() && data_page.size() > levels_byte_len) {
      auto values_buffer = SliceBuffer(data_page.buffer(), levels_byte_len);
      auto compressed_values = std::static_pointer_cast<ResizableBuffer>(
          AllocateBuffer(props_.memory_pool(), values_buffer->size()));
      page_writer_->Compress(*values_buffer, compressed_values.get());
      if (compressed_values->size() < values_buffer->size()) {
        page_is_compressed = true;
        int64_t combined_size = levels_byte_len + compressed_values->size();
        auto combined = AllocateBuffer(props_.memory_pool(), combined_size);
        if (levels_byte_len > 0) {
          std::memcpy(combined->mutable_data(), data_page.data(), levels_byte_len);
        }
        std::memcpy(combined->mutable_data() + levels_byte_len, compressed_values->data(),
                    compressed_values->size());
        output_buffer = std::move(combined);
      }
    }
    if (!page_is_compressed) {
      output_buffer = data_page.buffer();
    }

    auto first_row_index =
        original_offset_index_
            ? std::optional{original_offset_index_->page_locations()[page_no]
                                .first_row_index}
            : std::nullopt;
    SizeStatistics size_statistics;
    size_statistics.unencoded_byte_array_data_bytes =
        original_offset_index_ &&
                !original_offset_index_->unencoded_byte_array_data_bytes().empty()
            ? std::optional{original_offset_index_
                                ->unencoded_byte_array_data_bytes()[page_no]}
            : std::nullopt;
    DataPageV2 new_page(output_buffer, data_page.num_values(), data_page.num_nulls(),
                        data_page.num_rows(), data_page.encoding(),
                        data_page.definition_levels_byte_length(),
                        data_page.repetition_levels_byte_length(),
                        data_page.uncompressed_size(), page_is_compressed,
                        data_page.statistics(), first_row_index, size_statistics);
    total_uncompressed_size_ += page_writer_->WriteDataPage(new_page);
  }

  const RewriterProperties& props_;
  std::unique_ptr<PageReader> page_reader_;
  std::unique_ptr<PageWriter> page_writer_;
  std::shared_ptr<OffsetIndex> original_offset_index_;
  int64_t total_uncompressed_size_{0};
};

class ColumnChunkRewriter {
 public:
  ColumnChunkRewriter(std::shared_ptr<ArrowInputFile> source,
                      std::shared_ptr<ArrowOutputStream> sink,
                      const RewriterProperties& props,
                      std::unique_ptr<ColumnChunkMetaData> metadata,
                      std::shared_ptr<RowGroupPageIndexReader> page_index_reader,
                      std::shared_ptr<RowGroupBloomFilterReader> bloom_filter_reader,
                      int32_t row_group_ordinal, int32_t column_ordinal, bool fast_copy)
      : source_(std::move(source)),
        sink_(std::move(sink)),
        props_(props),
        metadata_(std::move(metadata)),
        page_index_reader_(std::move(page_index_reader)),
        bloom_filter_reader_(std::move(bloom_filter_reader)),
        row_group_ordinal_(row_group_ordinal),
        column_ordinal_(column_ordinal),
        fast_copy_(fast_copy) {
    if (metadata_ == nullptr) {
      throw ParquetException("ColumnChunkMetaData should not be nullptr");
    }
    if (metadata_->is_encrypted()) {
      ParquetException::NYI("Rewriter does not support reading encrypted column chunks");
    }
  }

  static bool CanFastCopy(const RewriterProperties& props,
                          const ColumnChunkMetaData& metadata) {
    Compression::type original_codec = metadata.compression();
    auto column_path = metadata.path_in_schema();
    Compression::type new_codec = props.writer_properties()->compression(column_path);
    return (original_codec == new_codec);
  }

  void WriteColumnChunkData(RowGroupMetaDataBuilder& rg_metadata_builder,
                            PageIndexBuilder* page_index_builder,
                            BloomFilterBuilder* bloom_filter_builder,
                            int64_t& total_bytes_written) {
    auto& reader_props = props_.reader_properties();
    auto& writer_props = *props_.writer_properties();
    auto stream = reader_props.GetStream(source_, metadata_->start_offset(),
                                         metadata_->total_compressed_size());

    if (fast_copy_) {
      PARQUET_ASSIGN_OR_THROW(int64_t sink_offset, sink_->Tell());
      int64_t shift = sink_offset - metadata_->start_offset();

      CopyStream(stream, sink_, metadata_->total_compressed_size(), props_.memory_pool());
      PARQUET_THROW_NOT_OK(stream->Close());

      rg_metadata_builder.NextColumnChunk(std::move(metadata_), shift);

      if (page_index_reader_ != nullptr && page_index_builder != nullptr) {
        auto offset_index = page_index_reader_->GetOffsetIndex(column_ordinal_);
        if (offset_index != nullptr) {
          page_index_builder->SetOffsetIndex(column_ordinal_, offset_index, shift);
        }
      }

      total_bytes_written += metadata_->total_uncompressed_size();
    } else {
      auto column_path = metadata_->path_in_schema();
      auto new_codec = writer_props.compression(column_path);
      auto codec_options = writer_props.codec_options(column_path);

      auto* cc_metadata_builder = rg_metadata_builder.NextColumnChunk();

      OffsetIndexBuilder* offset_index_builder = nullptr;
      std::shared_ptr<OffsetIndex> original_offset_index = nullptr;
      if (page_index_reader_ != nullptr && page_index_builder != nullptr) {
        offset_index_builder = page_index_builder->GetOffsetIndexBuilder(column_ordinal_);
        original_offset_index = page_index_reader_->GetOffsetIndex(column_ordinal_);
      }

      auto page_reader = PageReader::Open(std::move(stream), metadata_->num_values(),
                                          metadata_->compression(), reader_props);
      auto page_writer = PageWriter::Open(
          sink_, new_codec, cc_metadata_builder, static_cast<int16_t>(row_group_ordinal_),
          static_cast<int16_t>(column_ordinal_), props_.memory_pool(),
          /*buffered_row_group=*/false,
          /*header_encryptor=*/nullptr, /*data_encryptor=*/nullptr,
          writer_props.page_checksum_enabled(),
          /*column_index_builder=*/nullptr, offset_index_builder,
          codec_options ? *codec_options : CodecOptions{});

      PagesRewriter pages_rewriter(props_, std::move(page_reader), std::move(page_writer),
                                   std::move(original_offset_index));
      pages_rewriter.WritePages();

      total_bytes_written += pages_rewriter.total_uncompressed_size();
    }
    if (page_index_reader_ != nullptr && page_index_builder != nullptr) {
      auto column_index = page_index_reader_->GetColumnIndex(column_ordinal_);
      if (column_index != nullptr) {
        page_index_builder->SetColumnIndex(column_ordinal_, column_index);
      }
    }
    if (bloom_filter_reader_ != nullptr && bloom_filter_builder != nullptr) {
      auto bloom_filter = bloom_filter_reader_->GetColumnBloomFilter(column_ordinal_);
      if (bloom_filter != nullptr) {
        bloom_filter_builder->InsertBloomFilter(column_ordinal_, std::move(bloom_filter));
      }
    }
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  std::shared_ptr<ArrowOutputStream> sink_;
  const RewriterProperties& props_;
  std::unique_ptr<ColumnChunkMetaData> metadata_;
  std::shared_ptr<RowGroupPageIndexReader> page_index_reader_;
  std::shared_ptr<RowGroupBloomFilterReader> bloom_filter_reader_;
  const int32_t row_group_ordinal_;
  const int32_t column_ordinal_;
  const bool fast_copy_;
};

class RowGroupRewriter {
 public:
  RowGroupRewriter(std::shared_ptr<ArrowInputFile> source,
                   std::shared_ptr<ArrowOutputStream> sink,
                   const RewriterProperties& props,
                   std::shared_ptr<RowGroupReader> row_group_reader,
                   std::shared_ptr<RowGroupPageIndexReader> page_index_reader,
                   std::shared_ptr<RowGroupBloomFilterReader> bloom_filter_reader)
      : source_(std::move(source)),
        sink_(std::move(sink)),
        props_(props),
        row_group_reader_(std::move(row_group_reader)),
        page_index_reader_(std::move(page_index_reader)),
        bloom_filter_reader_(std::move(bloom_filter_reader)),
        metadata_(row_group_reader_->metadata()) {
    if (metadata_ == nullptr) {
      throw ParquetException("RowGroupMetaData should not be nullptr");
    }
  }

  void WriteRowGroupData(int32_t row_group_ordinal,
                         RowGroupMetaDataBuilder& rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         BloomFilterBuilder* bloom_filter_builder,
                         int64_t& total_bytes_written) {
    rg_metadata_builder.set_num_rows(metadata_->num_rows());

    std::vector<bool> can_column_chunk_fast_copy(metadata_->num_columns());
    for (int i = 0; i < metadata_->num_columns(); ++i) {
      auto cc_metadata = metadata_->ColumnChunk(i);
      can_column_chunk_fast_copy[i] =
          ColumnChunkRewriter::CanFastCopy(props_, *cc_metadata);
    }
    bool fast_copy = std::ranges::all_of(can_column_chunk_fast_copy, std::identity{});
    if (fast_copy) {
      fast_copy = metadata_->file_offset() != 0;
    }
    if (fast_copy) {
      PARQUET_ASSIGN_OR_THROW(int64_t sink_offset, sink_->Tell());
      int64_t shift = sink_offset - metadata_->file_offset();

      auto stream = props_.reader_properties().GetStream(
          source_, metadata_->file_offset(), metadata_->total_compressed_size());
      CopyStream(stream, sink_, metadata_->total_compressed_size(), props_.memory_pool());
      PARQUET_THROW_NOT_OK(stream->Close());

      for (int i = 0; i < metadata_->num_columns(); ++i) {
        auto cc_metadata = metadata_->ColumnChunk(i);
        rg_metadata_builder.NextColumnChunk(std::move(cc_metadata), shift);

        auto column_index =
            page_index_reader_ ? page_index_reader_->GetColumnIndex(i) : nullptr;
        auto offset_index =
            page_index_reader_ ? page_index_reader_->GetOffsetIndex(i) : nullptr;
        auto bloom_filter = bloom_filter_reader_
                                ? bloom_filter_reader_->GetColumnBloomFilter(i)
                                : nullptr;

        if (column_index != nullptr && page_index_builder != nullptr) {
          page_index_builder->SetColumnIndex(i, column_index);
        }
        if (offset_index != nullptr && page_index_builder != nullptr) {
          page_index_builder->SetOffsetIndex(i, offset_index, shift);
        }
        if (bloom_filter != nullptr && bloom_filter_builder != nullptr) {
          bloom_filter_builder->InsertBloomFilter(i, std::move(bloom_filter));
        }
      }

      total_bytes_written += metadata_->total_byte_size();
    } else {
      for (int i = 0; i < metadata_->num_columns(); ++i) {
        auto cc_metadata = metadata_->ColumnChunk(i);
        ColumnChunkRewriter rewriter(source_, sink_, props_, std::move(cc_metadata),
                                     page_index_reader_, bloom_filter_reader_,
                                     row_group_ordinal, i, can_column_chunk_fast_copy[i]);
        rewriter.WriteColumnChunkData(rg_metadata_builder, page_index_builder,
                                      bloom_filter_builder, total_bytes_written);
      }
    }
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  std::shared_ptr<ArrowOutputStream> sink_;
  const RewriterProperties& props_;
  std::shared_ptr<RowGroupReader> row_group_reader_;
  std::shared_ptr<RowGroupPageIndexReader> page_index_reader_;
  std::shared_ptr<RowGroupBloomFilterReader> bloom_filter_reader_;
  const RowGroupMetaData* metadata_;
};

class SingleFileRewriter {
 public:
  SingleFileRewriter(std::shared_ptr<ArrowInputFile> source,
                     std::shared_ptr<ArrowOutputStream> sink,
                     std::shared_ptr<FileMetaData> source_metadata,
                     const RewriterProperties& props)
      : source_(source),
        sink_(std::move(sink)),
        props_(props),
        parquet_file_reader_(ParquetFileReader::Open(
            std::move(source), props_.reader_properties(), std::move(source_metadata))),
        page_index_reader_(parquet_file_reader_->GetPageIndexReader()),
        bloom_filter_reader_(parquet_file_reader_->GetBloomFilterReader()),
        metadata_(parquet_file_reader_->metadata()) {
    if (metadata_ == nullptr) {
      throw ParquetException("FileMetaData should not be nullptr");
    }

    std::vector<int32_t> row_group_indices(metadata_->num_row_groups());
    std::iota(row_group_indices.begin(), row_group_indices.end(), 0);
    std::vector<int32_t> column_indices(metadata_->num_columns());
    std::iota(column_indices.begin(), column_indices.end(), 0);
    if (page_index_reader_) {
      page_index_reader_->WillNeed(row_group_indices, column_indices,
                                   {/*column_index=*/true, /*offset_index=*/true});
    }
  }

  void WriteRowGroupData(int32_t row_group_ordinal,
                         RowGroupMetaDataBuilder& rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         BloomFilterBuilder* bloom_filter_builder,
                         int64_t& total_bytes_written) {
    if (current_row_group_index_ >= metadata_->num_row_groups()) {
      throw ParquetException("Trying to read row group ", current_row_group_index_,
                             " but file only has ", metadata_->num_row_groups(),
                             " row groups");
    }
    auto row_group_reader = parquet_file_reader_->RowGroup(current_row_group_index_);
    auto page_index_reader = page_index_reader_
                                 ? page_index_reader_->RowGroup(current_row_group_index_)
                                 : nullptr;
    auto bloom_filter_reader = bloom_filter_reader_.RowGroup(current_row_group_index_);
    RowGroupRewriter rewriter(source_, sink_, props_, std::move(row_group_reader),
                              std::move(page_index_reader),
                              std::move(bloom_filter_reader));
    rewriter.WriteRowGroupData(row_group_ordinal, rg_metadata_builder, page_index_builder,
                               bloom_filter_builder, total_bytes_written);
    ++current_row_group_index_;
  }

  bool HasMoreRowGroup() {
    return current_row_group_index_ < metadata_->num_row_groups();
  }

  void Close() { parquet_file_reader_->Close(); }

  const SchemaDescriptor& schema() const { return *metadata_->schema(); }

  std::vector<int64_t> row_group_row_counts() const {
    int num_row_groups = metadata_->num_row_groups();
    std::vector<int64_t> row_counts;
    row_counts.reserve(num_row_groups);
    for (int i = 0; i < num_row_groups; ++i) {
      row_counts.emplace_back(metadata_->RowGroup(i)->num_rows());
    }
    return row_counts;
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  std::shared_ptr<ArrowOutputStream> sink_;
  const RewriterProperties& props_;
  std::unique_ptr<ParquetFileReader> parquet_file_reader_;
  std::shared_ptr<PageIndexReader> page_index_reader_;
  BloomFilterReader& bloom_filter_reader_;
  std::shared_ptr<FileMetaData> metadata_;
  int current_row_group_index_{};
};

class ConcatRewriter {
 public:
  explicit ConcatRewriter(std::vector<std::unique_ptr<SingleFileRewriter>> rewriters)
      : file_rewriters_(std::move(rewriters)) {
    if (file_rewriters_.empty()) {
      throw ParquetException("At least one SingleFileRewriter is required");
    }
    auto& schema = file_rewriters_[0]->schema();
    if (std::ranges::any_of(
            file_rewriters_ | std::views::drop(1),
            [&schema](auto& rewriter) { return !schema.Equals(rewriter->schema()); })) {
      throw ParquetException("Input files have different schemas.");
    }
  }

  void WriteRowGroupData(int32_t row_group_ordinal,
                         RowGroupMetaDataBuilder& rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         BloomFilterBuilder* bloom_filter_builder,
                         int64_t& total_bytes_written) {
    file_rewriters_[current_rewriter_index_]->WriteRowGroupData(
        row_group_ordinal, rg_metadata_builder, page_index_builder, bloom_filter_builder,
        total_bytes_written);
  }

  bool HasMoreRowGroup() {
    while (current_rewriter_index_ < file_rewriters_.size() &&
           !file_rewriters_[current_rewriter_index_]->HasMoreRowGroup()) {
      file_rewriters_[current_rewriter_index_]->Close();
      ARROW_LOG(DEBUG) << "Finished rewriting file index " << current_rewriter_index_;
      ++current_rewriter_index_;
    }
    return current_rewriter_index_ < file_rewriters_.size();
  }

  void Close() { std::ranges::for_each(file_rewriters_, &SingleFileRewriter::Close); }

  const SchemaDescriptor& schema() const { return file_rewriters_[0]->schema(); }

  std::vector<int64_t> row_group_row_counts() const {
    std::vector<int64_t> row_counts;
    for (auto& rewriter : file_rewriters_) {
      auto count = rewriter->row_group_row_counts();
      row_counts.insert(row_counts.end(), count.begin(), count.end());
    }
    return row_counts;
  }

 private:
  std::vector<std::unique_ptr<SingleFileRewriter>> file_rewriters_;
  size_t current_rewriter_index_{};
};

class JoinRewriter {
 public:
  explicit JoinRewriter(std::vector<std::unique_ptr<ConcatRewriter>> rewriters)
      : rewriters_(std::move(rewriters)) {
    if (rewriters_.empty()) {
      throw ParquetException("At least one ConcatRewriter is required");
    }
    auto row_counts = rewriters_[0]->row_group_row_counts();
    for (size_t i = 1; i < rewriters_.size(); ++i) {
      if (auto current_row_counts = rewriters_[i]->row_group_row_counts();
          row_counts != current_row_counts) {
        // TODO(anyone): use `std::format("{}", row_counts)` instead when C++23 available
        auto vecToString = [](const std::vector<int64_t>& v) -> std::string {
          if (v.empty()) {
            return "[]";
          }

          std::string result = "[" + ::arrow::internal::ToChars(v[0]);
          for (const auto& val : v | std::views::drop(1)) {
            result += ", " + ::arrow::internal::ToChars(val);
          }
          result += "]";
          return result;
        };
        throw ParquetException(
            "The number of rows in each block must match! No.0 blocks row counts: ",
            vecToString(row_counts), ", No.", i,
            " blocks row counts: ", vecToString(current_row_counts));
      }
    }

    std::unordered_set<std::string> column_paths;
    schema::NodeVector fields;

    for (auto& rewriter : rewriters_) {
      const SchemaDescriptor& schema_desc = rewriter->schema();

      for (int i = 0; i < schema_desc.num_columns(); ++i) {
        auto path = schema_desc.Column(i)->path()->ToDotString();
        if (auto [_, inserted] = column_paths.emplace(path); !inserted) {
          // TODO(HuaHuaY): support choose one column from columns with same path
          ParquetException::NYI("files have the same column path: " + path);
        }
      }

      const auto& group_node = schema_desc.group_node();
      for (int i = 0; i < group_node->field_count(); ++i) {
        fields.push_back(group_node->field(i));
      }
    }

    auto new_root = schema::GroupNode::Make(rewriters_[0]->schema().name(),
                                            Repetition::REQUIRED, fields);
    schema_.Init(new_root);
  }

  void WriteRowGroupData(int32_t row_group_ordinal,
                         RowGroupMetaDataBuilder& rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         BloomFilterBuilder* bloom_filter_builder,
                         int64_t& total_bytes_written) {
    for (auto& rewriter : rewriters_) {
      rewriter->WriteRowGroupData(row_group_ordinal, rg_metadata_builder,
                                  page_index_builder, bloom_filter_builder,
                                  total_bytes_written);
    }
  }

  bool HasMoreRowGroup() {
    return std::ranges::all_of(rewriters_, &ConcatRewriter::HasMoreRowGroup);
  }

  void Close() { std::ranges::for_each(rewriters_, &ConcatRewriter::Close); }

  const SchemaDescriptor& schema() const { return schema_; }

 private:
  std::vector<std::unique_ptr<ConcatRewriter>> rewriters_;
  SchemaDescriptor schema_;
};

// ----------------------------------------------------------------------
// GeneratedFile

class GeneratedFile : public ParquetFileRewriter::Contents {
 public:
  static std::unique_ptr<ParquetFileRewriter::Contents> Open(
      std::vector<std::vector<std::shared_ptr<ArrowInputFile>>> sources,
      std::shared_ptr<ArrowOutputStream> sink,
      std::vector<std::vector<std::shared_ptr<FileMetaData>>> sources_metadata,
      std::shared_ptr<const KeyValueMetadata> sink_metadata,
      std::shared_ptr<RewriterProperties> props) {
    if (sources.size() != sources_metadata.size() ||
        // TODO(anyone): use std::views::zip when C++23 available
        std::ranges::any_of(std::views::iota(0u, sources.size()), [&](size_t i) {
          return sources[i].size() != sources_metadata[i].size();
        })) {
      throw ParquetException(
          "The number of sources and sources_metadata must be the same");
    }
    std::unique_ptr<ParquetFileRewriter::Contents> result(new GeneratedFile(
        std::move(sources), std::move(sink), std::move(sources_metadata),
        std::move(sink_metadata), std::move(props)));
    return result;
  }

  void Close() override {
    if (rewriter_) {
      rewriter_->Close();
      rewriter_.reset();
    }
  }

  void Rewrite() override {
    int32_t row_group_ordinal = 0;
    while (rewriter_->HasMoreRowGroup()) {
      auto& rg_metadata_builder = *metadata_builder_->AppendRowGroup();
      if (page_index_builder_) {
        page_index_builder_->AppendRowGroup();
      }
      if (bloom_filter_builder_) {
        bloom_filter_builder_->AppendRowGroup();
      }
      int64_t total_bytes_written = 0;
      rewriter_->WriteRowGroupData(row_group_ordinal, rg_metadata_builder,
                                   page_index_builder_.get(), bloom_filter_builder_.get(),
                                   total_bytes_written);
      rg_metadata_builder.Finish(total_bytes_written);
      row_group_ordinal++;
    }
    if (page_index_builder_) {
      page_index_builder_->Finish();
      auto [column_index_locations, offset_index_locations] =
          page_index_builder_->WriteTo(sink_.get());
      metadata_builder_->SetIndexLocations(IndexKind::kColumnIndex,
                                           column_index_locations);
      metadata_builder_->SetIndexLocations(IndexKind::kOffsetIndex,
                                           offset_index_locations);
    }
    if (bloom_filter_builder_) {
      auto bloom_filter_locations = bloom_filter_builder_->WriteTo(sink_.get());
      metadata_builder_->SetIndexLocations(IndexKind::kBloomFilter,
                                           bloom_filter_locations);
    }

    auto file_metadata = metadata_builder_->Finish(sink_metadata_);
    WriteFileMetaData(*file_metadata, sink_.get());
  }

 private:
  GeneratedFile(std::vector<std::vector<std::shared_ptr<ArrowInputFile>>> sources,
                std::shared_ptr<ArrowOutputStream> sink,
                std::vector<std::vector<std::shared_ptr<FileMetaData>>> sources_metadata,
                std::shared_ptr<const KeyValueMetadata> sink_metadata,
                std::shared_ptr<RewriterProperties> props)
      : sink_(std::move(sink)),
        props_(std::move(props)),
        sink_metadata_(std::move(sink_metadata)) {
    std::vector<std::unique_ptr<ConcatRewriter>> rewriters;
    rewriters.reserve(sources.size());
    for (size_t i = 0; i < sources.size(); ++i) {
      std::vector<std::unique_ptr<SingleFileRewriter>> concat_rewriters;
      concat_rewriters.reserve(sources[i].size());
      for (size_t j = 0; j < sources[i].size(); ++j) {
        concat_rewriters.emplace_back(std::make_unique<SingleFileRewriter>(
            std::move(sources[i][j]), sink_, std::move(sources_metadata[i][j]), *props_));
      }
      rewriters.emplace_back(
          std::make_unique<ConcatRewriter>(std::move(concat_rewriters)));
    }
    rewriter_ = std::make_unique<JoinRewriter>(std::move(rewriters));

    if (props_->writer_properties()->file_encryption_properties() == nullptr) {
      // Unencrypted parquet files always start with PAR1
      PARQUET_THROW_NOT_OK(sink_->Write(kParquetMagic, 4));
    } else {
      ParquetException::NYI("Rewriter does not support writing encrypted files.");
    }

    auto new_schema = rewriter_->schema().schema_root();
    new_schema_.Init(new_schema);
    metadata_builder_ =
        FileMetaDataBuilder::Make(&new_schema_, props_->writer_properties());
    if (props_->writer_properties()->page_index_enabled()) {
      page_index_builder_ = PageIndexBuilder::Make(&new_schema_, nullptr);
    }
    if (props_->writer_properties()->bloom_filter_enabled()) {
      bloom_filter_builder_ =
          BloomFilterBuilder::Make(&new_schema_, props_->writer_properties().get());
    }
  }

  std::shared_ptr<ArrowOutputStream> sink_;
  std::shared_ptr<RewriterProperties> props_;
  std::shared_ptr<const KeyValueMetadata> sink_metadata_;
  std::unique_ptr<JoinRewriter> rewriter_;

  SchemaDescriptor new_schema_;
  std::unique_ptr<FileMetaDataBuilder> metadata_builder_;
  std::unique_ptr<PageIndexBuilder> page_index_builder_;
  std::unique_ptr<BloomFilterBuilder> bloom_filter_builder_;
};

// ----------------------------------------------------------------------
// ParquetFileRewriter public API

ParquetFileRewriter::ParquetFileRewriter() = default;

ParquetFileRewriter::~ParquetFileRewriter() {
  try {
    Close();
  } catch (...) {
  }
}

std::unique_ptr<ParquetFileRewriter> ParquetFileRewriter::Open(
    std::vector<std::vector<std::shared_ptr<ArrowInputFile>>> sources,
    std::shared_ptr<ArrowOutputStream> sink,
    std::vector<std::vector<std::shared_ptr<FileMetaData>>> sources_metadata,
    std::shared_ptr<const KeyValueMetadata> sink_metadata,
    std::shared_ptr<RewriterProperties> props) {
  auto contents = GeneratedFile::Open(std::move(sources), std::move(sink),
                                      std::move(sources_metadata),
                                      std::move(sink_metadata), std::move(props));
  std::unique_ptr<ParquetFileRewriter> result(new ParquetFileRewriter());
  result->Open(std::move(contents));
  return result;
}

void ParquetFileRewriter::Open(std::unique_ptr<ParquetFileRewriter::Contents> contents) {
  contents_ = std::move(contents);
}

void ParquetFileRewriter::Close() {
  if (contents_) {
    contents_->Close();
    contents_.reset();
  }
}

void ParquetFileRewriter::Rewrite() { contents_->Rewrite(); }

}  // namespace parquet
