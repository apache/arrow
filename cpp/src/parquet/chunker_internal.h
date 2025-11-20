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

#pragma once

#include <cstdint>
#include <vector>

#include "arrow/array.h"
#include "parquet/level_conversion.h"

namespace parquet::internal {

// Represents a chunk of data with level offsets and value offsets due to the
// record shredding for nested data.
struct Chunk {
  // The start offset of this chunk inside the given levels
  int64_t level_offset;
  // The start offset of this chunk inside the given values array
  int64_t value_offset;
  // The length of the chunk in levels
  int64_t levels_to_write;
};

/// CDC (Content-Defined Chunking) is a technique that divides data into variable-sized
/// chunks based on the content of the data itself, rather than using fixed-size
/// boundaries.
///
/// For example, given this sequence of values in a column:
///
/// File1:    [1,2,3,   4,5,6,   7,8,9]
///            chunk1   chunk2   chunk3
///
/// Assume there is an inserted value between 3 and 4:
///
/// File2:     [1,2,3,0,  4,5,6,   7,8,9]
///            new-chunk  chunk2   chunk3
///
/// The chunking process will adjust to maintain stable boundaries across data
/// modifications. Each chunk defines a new parquet data page which is contiguously
/// written out to the file. Since each page compressed independently, the files' contents
/// would look like the following with unique page identifiers:
///
/// File1:     [Page1][Page2][Page3]...
/// File2:     [Page4][Page2][Page3]...
///
/// Then the parquet file is being uploaded to a content addressable storage (CAS) system
/// which splits the bytes stream into content defined blobs. The CAS system will
/// calculate a unique identifier for each blob, then store the blob in a key-value store.
/// If the same blob is encountered again, the system can refer to the hash instead of
/// physically storing the blob again. In the example above, the CAS system would store
/// Page1, Page2, Page3, and Page4 only once and the required metadata to reassemble the
/// files.
/// While the deduplication is performed by the CAS system, the parquet chunker makes it
/// possible to efficiently deduplicate the data by consistently dividing the data into
/// chunks.
///
/// Implementation details:
///
/// Only the parquet writer must be aware of the content defined chunking, the reader
/// doesn't need to know about it. Each parquet column writer holds a
/// ContentDefinedChunker instance depending on the writer's properties. The chunker's
/// state is maintained across the entire column without being reset between pages and row
/// groups.
///
/// The chunker receives the record shredded column data (def_levels, rep_levels, values)
/// and goes over the (def_level, rep_level, value) triplets one by one while adjusting
/// the column-global rolling hash based on the triplet. Whenever the rolling hash matches
/// a predefined mask, the chunker creates a new chunk. The chunker returns a vector of
/// Chunk objects that represent the boundaries of the chunks.
/// Note that the boundaries are deterministically calculated exclusively based on the
/// data itself, so the same data will always produce the same chunks - given the same
/// chunker configuration.
///
/// References:
/// - FastCDC: a Fast and Efficient Content-Defined Chunking Approach for Data
///   Deduplication
///   https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
/// - Git is for Data (chunk size normalization used here is described in section 6.2.1):
///   https://www.cidrdb.org/cidr2023/papers/p43-low.pdf
class PARQUET_EXPORT ContentDefinedChunker {
 public:
  /// Create a new ContentDefinedChunker instance
  ///
  /// @param level_info Information about definition and repetition levels
  /// @param min_chunk_size Minimum chunk size in bytes
  ///   The rolling hash will not be updated until this size is reached for each chunk.
  ///   Note that all data sent through the hash function is counted towards the chunk
  ///   size, including definition and repetition levels if present.
  /// @param max_chunk_size Maximum chunk size in bytes
  ///   The chunker creates a new chunk whenever the chunk size exceeds this value. The
  ///   chunk size distribution approximates a normal distribution between min_chunk_size
  ///   and max_chunk_size. Note that the parquet writer has a related `data_pagesize`
  //    property that controls the maximum size of a parquet data page after encoding.
  ///   While setting `data_pagesize` to a smaller value than `max_chunk_size` doesn't
  ///   affect the chunking effectiveness, it results in more small parquet data pages.
  /// @param norm_level Normalization level to center the chunk size around the average
  ///   size more aggressively, default 0.
  ///   Increasing the normalization level increases the probability of finding a chunk
  ///   boundary, improving the deduplication ratio, but also increases the number of
  ///   small chunks resulting in many small parquet data pages. The default value
  ///   provides a good balance between deduplication ratio and fragmentation.
  ///   Use norm_level=1 or norm_level=2 to reach a higher deduplication ratio at the
  ///   expense of fragmentation.
  ContentDefinedChunker(const LevelInfo& level_info, int64_t min_chunk_size,
                        int64_t max_chunk_size, int norm_level = 0);
  ~ContentDefinedChunker();

  /// Get the chunk boundaries for the given column data
  ///
  /// @param def_levels Definition levels
  /// @param rep_levels Repetition levels
  /// @param num_levels Number of levels
  /// @param values Column values as an Arrow array
  /// @return Vector of Chunk objects representing the chunk boundaries
  std::vector<Chunk> GetChunks(const int16_t* def_levels, const int16_t* rep_levels,
                               int64_t num_levels, const ::arrow::Array& values);

 private:
  /// @brief Get the rolling hash mask used to determine chunk boundaries, used for
  /// testing the mask calculation.
  uint64_t GetRollingHashMask() const;

  class Impl;
  std::unique_ptr<Impl> impl_;

  friend class TestCDC;
};

}  // namespace parquet::internal
