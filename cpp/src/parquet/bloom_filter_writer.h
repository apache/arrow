#pragma once

#include "bloom_filter.h"

#include <map>
#include <vector>

namespace parquet {

class PARQUET_EXPORT RowGroupBloomFilterReference {
 public:
  struct Reference {
    int64_t offset;
    int32_t length;
  };

  /// Append a new row group to host all incoming bloom filters.
  void AppendRowGroup();

  /// Add reference to the serialized bloom filter.
  void AddBloomFilter(int32_t column_id, int64_t offset, int32_t length);

  /// Get bloom filter offsets of a specific row group.
  bool GetBloomFilterOffsets(size_t row_group_ordinal,
                             const std::map<int32_t, Reference>** out) const;

  bool empty() const { return references_.empty(); }

 private:
  std::vector<std::map<int32_t, Reference>> references_;
};

class InternalFileEncryptor;

namespace schema {
class ColumnPath;
}

class PARQUET_EXPORT BloomFilterWriter {
 public:
  explicit BloomFilterWriter(const WriterProperties& properties)
      : properties_(properties) {}
  /// Append a new row group to host all incoming bloom filters.
  void AppendRowGroup();

  /// Return a BloomFilter defined by `col_path`.
  ///
  /// * If the col_path has a bloom filter, create a BloomFilter in
  ///  `row_group_bloom_filters_` and return.
  /// * Otherwise, return nullptr.
  BloomFilter* GetOrCreateBloomFilter(
      const std::shared_ptr<schema::ColumnPath>& col_path,
      const std::function<std::unique_ptr<BloomFilter>()>& bf_creator);

  /// Serialize all bloom filters with header and bitset in the order of row group and
  /// column id. Column encryption is not implemented yet. The side effect is that it
  /// deletes all bloom filters after they have been flushed.
  void WriteTo(::arrow::io::OutputStream* sink, const SchemaDescriptor* schema,
               InternalFileEncryptor* encryptor = nullptr);

  /// Return reference to serialized bloom filters. It is undefined behavior until
  /// WriteTo() has been called.
  const RowGroupBloomFilterReference& reference() const;

 private:
  const WriterProperties& properties_;

  std::vector<std::map<std::string, std::unique_ptr<BloomFilter>>>
      row_group_bloom_filters_;

  RowGroupBloomFilterReference reference_;
};

}  // namespace parquet
