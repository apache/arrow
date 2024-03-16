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

#include <optional>

#include "parquet/platform.h"

namespace parquet {

struct ByteArray;
class ColumnDescriptor;

/// \brief SizeStatistics is a proxy around format::SizeStatistics.
///
/// A structure for capturing metadata for estimating the unencoded,
/// uncompressed size of data written. This is useful for readers to estimate
/// how much memory is needed to reconstruct data in their memory model and for
/// fine-grained filter push down on nested structures (the histograms contained
/// in this structure can help determine the number of nulls at a particular
/// nesting level and maximum length of lists).
class PARQUET_EXPORT SizeStatistics {
 public:
  /// \brief API convenience to get a SizeStatistics accessor
  static std::unique_ptr<SizeStatistics> Make(const void* size_statistics,
                                              const ColumnDescriptor* descr);

  ~SizeStatistics();

  /// When present, there is expected to be one element corresponding to each
  /// repetition (i.e. size=max repetition_level+1) where each element
  /// represents the number of times the repetition level was observed in the
  /// data.
  ///
  /// This field may be omitted if max_repetition_level is 0 without loss
  /// of information.
  ///
  /// \returns repetition level histogram of all levels if not empty.
  const std::vector<int64_t>& repetition_level_histogram() const;

  /// Same as repetition_level_histogram except for definition levels.
  ///
  /// This field may be omitted if max_definition_level is 0 or 1 without
  /// loss of information.
  ///
  /// \returns definition level histogram of all levels if not empty.
  const std::vector<int64_t>& definition_level_histogram() const;

  /// The number of physical bytes stored for BYTE_ARRAY data values assuming
  /// no encoding. This is exclusive of the bytes needed to store the length of
  /// each byte array. In other words, this field is equivalent to the `(size
  /// of PLAIN-ENCODING the byte array values) - (4 bytes * number of values
  /// written)`. To determine unencoded sizes of other types readers can use
  /// schema information multiplied by the number of non-null and null values.
  /// The number of null/non-null values can be inferred from the histograms
  /// below.
  ///
  /// For example, if a column chunk is dictionary-encoded with dictionary
  /// ["a", "bc", "cde"], and a data page contains the indices [0, 0, 1, 2],
  /// then this value for that data page should be 7 (1 + 1 + 2 + 3).
  ///
  /// This field should only be set for types that use BYTE_ARRAY as their
  /// physical type.
  ///
  /// \returns unencoded and uncompressed byte size of the BYTE_ARRAY column,
  /// or std::nullopt for other types.
  std::optional<int64_t> unencoded_byte_array_data_bytes() const;

  /// \brief Merge two SizeStatistics of the same column.
  ///
  /// It is used to merge size statistics from all pages of the same column chunk.
  void Merge(const SizeStatistics& other);

 private:
  friend class SizeStatisticsBuilder;
  SizeStatistics(const void* size_statistics, const ColumnDescriptor* descr);

  // PIMPL Idiom
  SizeStatistics();
  class SizeStatisticsImpl;
  std::unique_ptr<SizeStatisticsImpl> impl_;
};

/// \brief Builder to create a SizeStatistics.
class PARQUET_EXPORT SizeStatisticsBuilder {
 public:
  /// \brief API convenience to get a SizeStatisticsBuilder.
  static std::unique_ptr<SizeStatisticsBuilder> Make(const ColumnDescriptor* descr);

  ~SizeStatisticsBuilder();

  /// \brief Add repetition levels to the histogram.
  /// \param num_levels number of repetition levels to add.
  /// \param rep_levels repetition levels to add.
  void WriteRepetitionLevels(int64_t num_levels, const int16_t* rep_levels);

  /// \brief Add definition levels to the histogram.
  /// \param num_levels number of definition levels to add.
  /// \param def_levels definition levels to add.
  void WriteDefinitionLevels(int64_t num_levels, const int16_t* def_levels);

  /// \brief Add repeated repetition level to the histogram.
  /// \param num_levels number of repetition levels to add.
  /// \param rep_level repeated repetition level value.
  void WriteRepetitionLevel(int64_t num_levels, int16_t rep_level);

  /// \brief Add repeated definition level to the histogram.
  /// \param num_levels number of definition levels to add.
  /// \param def_level repeated definition level value.
  void WriteDefinitionLevel(int64_t num_levels, int16_t def_level);

  /// \brief Add spaced BYTE_ARRAY values.
  /// \param[in] values pointer to values of BYTE_ARRAY type.
  /// \param[in] valid_bits pointer to bitmap representing if values are non-null.
  /// \param[in] valid_bits_offset offset into valid_bits where the slice of data begins.
  /// \param[in] num_spaced_values length of values in values/valid_bits to inspect.
  void WriteValuesSpaced(const ByteArray* values, const uint8_t* valid_bits,
                         int64_t valid_bits_offset, int64_t num_spaced_values);

  /// \brief Add dense BYTE_ARRAY values.
  /// \param values pointer to values of BYTE_ARRAY type.
  /// \param num_values  length of values.
  void WriteValues(const ByteArray* values, int64_t num_values);

  /// \brief Add BYTE_ARRAY values in the arrow array.
  void WriteValues(const ::arrow::Array& values);

  /// \brief Build a SizeStatistics from collected data.
  std::unique_ptr<SizeStatistics> Build();

  /// \brief Reset all collected data for reuse.
  void Reset();

 private:
  // PIMPL Idiom
  SizeStatisticsBuilder(const ColumnDescriptor* descr);
  class SizeStatisticsBuilderImpl;
  std::unique_ptr<SizeStatisticsBuilderImpl> impl_;
};

}  // namespace parquet
