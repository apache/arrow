#include "bloom_filter_writer.h"

#include "arrow/io/interfaces.h"

#include "parquet/encryption/encryption.h"
#include "parquet/encryption/internal_file_encryptor.h"
#include "parquet/exception.h"
#include "parquet/properties.h"

namespace parquet {

void RowGroupBloomFilterReference::AppendRowGroup() { references_.emplace_back(); }

void RowGroupBloomFilterReference::AddBloomFilter(int32_t column_id, int64_t offset,
                                                  int32_t length) {
  DCHECK(!references_.empty());
  references_.back().emplace(column_id, Reference{offset, length});
}

bool RowGroupBloomFilterReference::GetBloomFilterOffsets(
    size_t row_group_ordinal, const std::map<int32_t, Reference>** out) const {
  if (row_group_ordinal < references_.size()) {
    *out = &references_.at(row_group_ordinal);
    return true;
  }
  *out = nullptr;
  return false;
}

void BloomFilterWriter::DropRowGroupBloomFilter(
    const std::shared_ptr<schema::ColumnPath>& col_path) {
  if (properties_.bloom_filter_enabled(col_path)) {
    row_group_bloom_filters_.back().erase(col_path->ToDotString());
  }
}

void BloomFilterWriter::AppendRowGroup() { row_group_bloom_filters_.emplace_back(); }

void BloomFilterWriter::WriteTo(::arrow::io::OutputStream* sink,
                                const SchemaDescriptor* schema,
                                InternalFileEncryptor* encryptor) {
  if (row_group_bloom_filters_.empty()) {
    // Return quickly if there is no bloom filter
    return;
  }

  size_t total_serialized_count = 0;
  for (const auto& row_group_bloom_filters : row_group_bloom_filters_) {
    reference_.AppendRowGroup();

    // the whole row group has no bloom filter
    if (row_group_bloom_filters.empty()) {
      continue;
    }

    // serialize bloom filter by ascending order of column id
    for (int32_t column_id = 0; column_id < schema->num_columns(); ++column_id) {
      const auto column_path = schema->Column(column_id)->path()->ToDotString();
      auto meta_encryptor =
          encryptor ? encryptor->GetColumnMetaEncryptor(column_path) : nullptr;
      auto data_encryptor =
          encryptor ? encryptor->GetColumnDataEncryptor(column_path) : nullptr;
      if (meta_encryptor || data_encryptor) {
        ParquetException::NYI("Bloom filter encryption is not implemented");
      }

      auto iter = row_group_bloom_filters.find(column_path);
      if (iter != row_group_bloom_filters.cend()) {
        PARQUET_ASSIGN_OR_THROW(int64_t offset, sink->Tell());
        iter->second->WriteTo(sink);
        PARQUET_ASSIGN_OR_THROW(int64_t pos, sink->Tell());
        reference_.AddBloomFilter(column_id, offset, static_cast<int32_t>(pos - offset));
        total_serialized_count++;
      }
    }
  }

  // release memory actively
  row_group_bloom_filters_.clear();
}

const RowGroupBloomFilterReference& BloomFilterWriter::reference() const {
  return reference_;
}

BloomFilter* BloomFilterWriter::GetOrCreateBloomFilter(
    const std::shared_ptr<schema::ColumnPath>& col_path,
    const std::function<std::unique_ptr<BloomFilter>()>& bf_creator) {
  if (properties_.bloom_filter_enabled(col_path)) {
    DCHECK(!row_group_bloom_filters_.empty());
    auto it =
        row_group_bloom_filters_.back().emplace(col_path->ToDotString(), bf_creator());
    return it.first->second.get();
  }
  return nullptr;
}

}  // namespace parquet
