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

#include <cmath>
#include <string>
#include <vector>
#include "arrow/array.h"
#include "parquet/level_conversion.h"

namespace parquet {

namespace internal {

// Represents a chunk of data with level offsets and value offsets due to the
// record shredding for nested data.
struct Chunk {
  int64_t level_offset;
  int64_t value_offset;
  int64_t levels_to_write;

  Chunk(int64_t level_offset, int64_t value_offset, int64_t levels_to_write)
      : level_offset(level_offset),
        value_offset(value_offset),
        levels_to_write(levels_to_write) {}
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
/// modifications. Each chunk defines a new parquet data page which are contiguously
/// written out to the file. Since each page compressed independently, the files' contents
/// would look like the following with unique page identifiers:
///
/// File1:     [Page1][Page2][Page3]...
/// File2:     [Page4][Page2][Page3]...
///
/// Then the parquet file is being uploaded to a content addressable storage systems (CAS)
/// which split the bytes stream into content defined blobs. The CAS system will calculate
/// a unique identifier for each blob, then store the blob in a key-value store. If the
/// same blob is encountered again, the system can refer to the hash instead of physically
/// storing the blob again. In the example above, the CAS system would phiysically store
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
/// Chunk objects that represent the boundaries of the chunks///
/// Note that the boundaries are deterministically calculated exclusively based on the
/// data itself, so the same data will always produce the same chunks - given the same
/// chunker configuration.
///
/// References:
/// - FastCDC paper: "FastCDC: a Fast and Efficient Content-Defined Chunking Approach for
/// Data Deduplication"
///   https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
class ContentDefinedChunker {
 public:
  /// Create a new ContentDefinedChunker instance
  ///
  /// @param level_info Information about definition and repetition levels
  /// @param size_range Min/max chunk size as pair<min_size, max_size>, the chunker will
  ///                   attempt to uniformly distribute the chunks between these extremes.
  /// @param norm_factor Normalization factor to center the chunk size around the average
  ///                    size more aggressively. By increasing the normalization factor,
  ///                    probability of finding a chunk boundary increases.
  ContentDefinedChunker(const LevelInfo& level_info,
                        std::pair<uint64_t, uint64_t> size_range,
                        uint8_t norm_factor = 0);

  /// Get the chunk boundaries for the given column data
  ///
  /// @param def_levels Definition levels
  /// @param rep_levels Repetition levels
  /// @param num_levels Number of levels
  /// @param values Column values as an Arrow array
  /// @return Vector of Chunk objects representing the chunk boundaries
  const ::arrow::Result<std::vector<Chunk>> GetBoundaries(const int16_t* def_levels,
                                                          const int16_t* rep_levels,
                                                          int64_t num_levels,
                                                          const ::arrow::Array& values);

 private:
  template <typename T>
  void Roll(const T value);
  void Roll(std::string_view value);
  inline bool NeedNewChunk();

  template <typename T>
  const std::vector<Chunk> Calculate(const int16_t* def_levels, const int16_t* rep_levels,
                                     int64_t num_levels, const T& leaf_array);

  // Reference to the column's level information
  const internal::LevelInfo& level_info_;
  // Minimum chunk size in bytes, the rolling hash will not be updated until this size is
  // reached for each chunk. Note that all data sent through the hash function is counted
  // towards the chunk size, including definition and repetition levels.
  const uint64_t min_size_;
  const uint64_t max_size_;
  // The mask to match the rolling hash against to determine if a new chunk should be
  // created. The mask is calculated based on min/max chunk size and the normalization
  // factor.
  const uint64_t hash_mask_;

  bool has_matched_ = false;
  uint64_t nth_run_ = 0;
  uint64_t chunk_size_ = 0;
  uint64_t rolling_hash_ = 0;
};

}  // namespace internal
}  // namespace parquet
