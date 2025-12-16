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

#include <memory>
#include <numeric>
#include <sstream>
#include <utility>

#include "arrow/util/logging.h"
#include "parquet/bloom_filter.h"  // IWYU pragma: keep
#include "parquet/bloom_filter_reader.h"
#include "parquet/column_reader.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/index_location.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/platform.h"
#include "parquet/properties.h"

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
    PARQUET_ASSIGN_OR_THROW(auto read_size, from->Read(size - bytes_copied, &buffer));
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

class RowGroupRewriter {
 public:
  RowGroupRewriter(std::shared_ptr<ArrowInputFile> source,
                   std::shared_ptr<ArrowOutputStream> sink,
                   const RewriterProperties* props,
                   std::shared_ptr<RowGroupReader> row_group_reader,
                   std::shared_ptr<RowGroupPageIndexReader> page_index_reader,
                   std::shared_ptr<RowGroupBloomFilterReader> bloom_filter_reader)
      : source_(std::move(source)),
        sink_(std::move(sink)),
        props_(props),
        row_group_reader_(std::move(row_group_reader)),
        page_index_reader_(std::move(page_index_reader)),
        bloom_filter_reader_(std::move(bloom_filter_reader)),
        metadata_(row_group_reader_->metadata()) {}

  void WriteRowGroupData(RowGroupMetaDataBuilder* rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         int64_t& total_bytes_written) {
    rg_metadata_builder->set_num_rows(metadata_->num_rows());

    bool fast_copy = metadata_->file_offset() != 0;
    if (fast_copy) {
      PARQUET_ASSIGN_OR_THROW(int64_t sink_offset, sink_->Tell());
      int64_t shift = sink_offset - metadata_->file_offset();

      auto stream = props_->reader_properties().GetStream(
          source_, metadata_->file_offset(), metadata_->total_compressed_size());
      CopyStream(stream, sink_, metadata_->total_compressed_size(),
                 props_->memory_pool());
      PARQUET_THROW_NOT_OK(stream->Close());

      for (int i = 0; i < metadata_->num_columns(); ++i) {
        auto cc_metadata = metadata_->ColumnChunk(i);
        rg_metadata_builder->NextColumnChunk(std::move(cc_metadata), shift);

        // TODO(HuaHuaY): copy column index and bloom filter instead of reading and
        // writing it.
        auto column_index = page_index_reader_->GetColumnIndex(i);
        auto offset_index = page_index_reader_->GetOffsetIndex(i);
        // TODO(HuaHuaY): support bloom filter writer
        [[maybe_unused]] auto bloom_filter =
            bloom_filter_reader_->GetColumnBloomFilter(i);

        if (column_index != nullptr) {
          page_index_builder->SetColumnIndex(i, column_index);
        }
        if (offset_index != nullptr) {
          page_index_builder->SetOffsetIndex(i, offset_index, shift);
        }
      }

      total_bytes_written += metadata_->total_byte_size();
    } else {
      for (int i = 0; i < metadata_->num_columns(); ++i) {
        auto cc_metadata = metadata_->ColumnChunk(i);

        PARQUET_ASSIGN_OR_THROW(int64_t sink_offset, sink_->Tell());
        int64_t shift = sink_offset - cc_metadata->start_offset();

        // TODO(HuaHuaY): add else branch to rewrite column chunk with new encoding,
        // compression, etc.
        if (true) {
          // TODO(HuaHuaY): copy column index and bloom filter instead of reading and
          // writing it.
          auto column_index = page_index_reader_->GetColumnIndex(i);
          auto offset_index = page_index_reader_->GetOffsetIndex(i);
          // TODO(HuaHuaY): support bloom filter writer
          [[maybe_unused]] auto bloom_filter =
              bloom_filter_reader_->GetColumnBloomFilter(i);

          auto stream = props_->reader_properties().GetStream(
              source_, cc_metadata->start_offset(), cc_metadata->total_compressed_size());
          CopyStream(stream, sink_, cc_metadata->total_compressed_size(),
                     props_->memory_pool());
          PARQUET_THROW_NOT_OK(stream->Close());

          rg_metadata_builder->NextColumnChunk(std::move(cc_metadata), shift);

          page_index_builder->SetColumnIndex(i, column_index);
          page_index_builder->SetOffsetIndex(i, offset_index, shift);

          total_bytes_written += cc_metadata->total_uncompressed_size();
        }
      }
    }
  }

 private:
  std::shared_ptr<ArrowInputFile> source_;
  std::shared_ptr<ArrowOutputStream> sink_;
  const RewriterProperties* props_;
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
                     const RewriterProperties* props)
      : source_(source),
        sink_(std::move(sink)),
        props_(props),
        parquet_file_reader_(ParquetFileReader::Open(
            std::move(source), props->reader_properties(), std::move(source_metadata))),
        page_index_reader_(parquet_file_reader_->GetPageIndexReader()),
        bloom_filter_reader_(parquet_file_reader_->GetBloomFilterReader()),
        metadata_(parquet_file_reader_->metadata()) {
    std::vector<int32_t> row_group_indices(metadata_->num_row_groups());
    std::iota(row_group_indices.begin(), row_group_indices.end(), 0);
    std::vector<int32_t> column_indices(metadata_->num_columns());
    std::iota(column_indices.begin(), column_indices.end(), 0);
    page_index_reader_->WillNeed(row_group_indices, column_indices,
                                 {/*column_index=*/true, /*offset_index=*/true});
  }

  void WriteRowGroupData(RowGroupMetaDataBuilder* rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         int64_t& total_bytes_written) {
    if (current_row_group_index_ >= metadata_->num_row_groups()) {
      std::stringstream ss;
      ss << "Trying to read row group " << current_row_group_index_
         << " but file only has " << metadata_->num_row_groups() << " row groups";
      throw ParquetException(ss.str());
    }
    auto row_group_metadata = metadata_->RowGroup(current_row_group_index_);
    auto row_group_reader = parquet_file_reader_->RowGroup(current_row_group_index_);
    auto page_index_reader = page_index_reader_->RowGroup(current_row_group_index_);
    auto bloom_filter_reader = bloom_filter_reader_.RowGroup(current_row_group_index_);
    RowGroupRewriter rewriter(source_, sink_, props_, std::move(row_group_reader),
                              std::move(page_index_reader),
                              std::move(bloom_filter_reader));
    rewriter.WriteRowGroupData(rg_metadata_builder, page_index_builder,
                               total_bytes_written);
    ++current_row_group_index_;
  }

  bool HasMoreRowGroup() {
    return current_row_group_index_ < metadata_->num_row_groups();
  }

  void Close() { parquet_file_reader_->Close(); }

  const SchemaDescriptor* schema() const { return metadata_->schema(); }

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
  const RewriterProperties* props_;
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
    auto* schema = file_rewriters_[0]->schema();
    for (size_t i = 1; i < file_rewriters_.size(); ++i) {
      if (!schema->Equals(*file_rewriters_[i]->schema())) {
        throw ParquetException("Input files have different schemas, current index: ", i,
                               ", schema:", file_rewriters_[i]->schema()->ToString());
      }
    }
  }

  void WriteRowGroupData(RowGroupMetaDataBuilder* rg_metadata_builder,
                         PageIndexBuilder* page_index_builder,
                         int64_t& total_bytes_written) {
    file_rewriters_[current_rewriter_index_]->WriteRowGroupData(
        rg_metadata_builder, page_index_builder, total_bytes_written);
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

  void Close() {
    for (size_t i = current_rewriter_index_; i < file_rewriters_.size(); ++i) {
      file_rewriters_[i]->Close();
    }
  }

  const SchemaDescriptor* schema() const { return file_rewriters_[0]->schema(); }

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

// ----------------------------------------------------------------------
// GeneratedFile

class GeneratedFile : public ParquetFileRewriter::Contents {
 public:
  static std::unique_ptr<ParquetFileRewriter::Contents> Open(
      std::vector<std::shared_ptr<ArrowInputFile>> sources,
      std::shared_ptr<ArrowOutputStream> sink,
      std::vector<std::shared_ptr<FileMetaData>> sources_metadata,
      std::shared_ptr<const KeyValueMetadata> sink_metadata,
      std::shared_ptr<RewriterProperties> props) {
    if (sources.size() != sources_metadata.size()) {
      throw ParquetException(
          "The number of sources and sources_metadata must be the same.");
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
    while (rewriter_->HasMoreRowGroup()) {
      auto* rg_metadata_builder = metadata_builder_->AppendRowGroup();
      page_index_builder_->AppendRowGroup();
      int64_t total_bytes_written = 0;
      rewriter_->WriteRowGroupData(rg_metadata_builder, page_index_builder_.get(),
                                   total_bytes_written);
      rg_metadata_builder->Finish(total_bytes_written);
    }
    page_index_builder_->Finish();

    auto [column_index_locations, offset_index_locations] =
        page_index_builder_->WriteTo(sink_.get());
    metadata_builder_->SetIndexLocations(IndexKind::kColumnIndex, column_index_locations);
    metadata_builder_->SetIndexLocations(IndexKind::kOffsetIndex, offset_index_locations);

    auto file_metadata = metadata_builder_->Finish(sink_metadata_);
    WriteFileMetaData(*file_metadata, sink_.get());
  }

 private:
  GeneratedFile(std::vector<std::shared_ptr<ArrowInputFile>> sources,
                std::shared_ptr<ArrowOutputStream> sink,
                std::vector<std::shared_ptr<FileMetaData>> sources_metadata,
                std::shared_ptr<const KeyValueMetadata> sink_metadata,
                std::shared_ptr<RewriterProperties> props)
      : sink_(std::move(sink)),
        props_(std::move(props)),
        sink_metadata_(std::move(sink_metadata)) {
    std::vector<std::unique_ptr<SingleFileRewriter>> rewriters;
    rewriters.reserve(sources.size());
    for (size_t i = 0; i < sources.size(); ++i) {
      rewriters.emplace_back(std::make_unique<SingleFileRewriter>(
          std::move(sources[i]), sink_, std::move(sources_metadata[i]), props_.get()));
    }
    rewriter_ = std::make_unique<ConcatRewriter>(std::move(rewriters));

    if (props_->writer_properties()->file_encryption_properties() == nullptr) {
      // Unencrypted parquet files always start with PAR1
      PARQUET_THROW_NOT_OK(sink_->Write(kParquetMagic, 4));
    } else {
      throw ParquetException(
          "NotImplemented, rewriter does not support to write encrypted files.");
    }

    auto new_schema = rewriter_->schema()->schema_root();
    new_schema_.Init(new_schema);
    metadata_builder_ =
        FileMetaDataBuilder::Make(&new_schema_, props_->writer_properties());
    if (props_->writer_properties()->page_index_enabled()) {
      page_index_builder_ = PageIndexBuilder::Make(&new_schema_, nullptr);
    }
  }

  std::shared_ptr<ArrowOutputStream> sink_;
  std::shared_ptr<RewriterProperties> props_;
  std::shared_ptr<const KeyValueMetadata> sink_metadata_;
  std::unique_ptr<ConcatRewriter> rewriter_;

  SchemaDescriptor new_schema_;
  std::unique_ptr<FileMetaDataBuilder> metadata_builder_;
  std::unique_ptr<PageIndexBuilder> page_index_builder_;
};

// ----------------------------------------------------------------------
// ParquetFilesRewriter public API

ParquetFileRewriter::ParquetFileRewriter() = default;

ParquetFileRewriter::~ParquetFileRewriter() {
  try {
    Close();
  } catch (...) {
  }
}

std::unique_ptr<ParquetFileRewriter> ParquetFileRewriter::Open(
    std::vector<std::shared_ptr<ArrowInputFile>> sources,
    std::shared_ptr<ArrowOutputStream> sink,
    std::vector<std::shared_ptr<FileMetaData>> sources_metadata,
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
