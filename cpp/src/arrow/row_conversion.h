// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/iterator.h"

namespace arrow {

/// \brief Base class for Arrow to row converter.
///
/// To use, specialize the conversion for a particular row type.
///
/// \tparam RowType type that contains a single row
/// \tparam ContainerType optional alternative vector implementation
///
/// The derived conversion functions take a batch_size parameter that determines
/// how many rows convert at once. This allows you to do the conversion in smaller
/// batches.
template <typename RowType, template <typename...> class ContainerType = std::vector>
class ToRowConverter {
 public:
  /// \brief Convert a record batch into a vector of rows.
  ///
  /// Implement this method in specialized classes to derive all other methods.
  ///
  /// \param batch Record batch to convert
  /// \return A vector of rows containing all data in batch
  virtual Result<ContainerType<RowType>> ConvertToVector(
      std::shared_ptr<RecordBatch> batch) = 0;

  /// \brief Convert a record batch into a vector of rows.
  ///
  /// \param batch Record batch to convert
  /// \param batch_size Number of rows to convert at once
  /// \return A vector of rows containing all data in batch
  Result<ContainerType<RowType>> ConvertToVector(std::shared_ptr<RecordBatch> batch,
                                                 size_t batch_size) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                          Table::FromRecordBatches({batch}));
    return ConvertToVector(table, batch_size);
  }

  /// \brief Convert an Arrow table to a vector of rows.
  ///
  /// \param table Table to convert
  /// \param batch_size Number of rows to convert at once
  /// \return A vector of rows containing all data in table
  Result<ContainerType<RowType>> ConvertToVector(std::shared_ptr<Table> table,
                                                 size_t batch_size) {
    ContainerType<RowType> out;

    TableBatchReader batch_reader(*table);
    batch_reader.set_chunksize(batch_size);
    for (auto batch : batch_reader) {
      ARROW_ASSIGN_OR_RAISE(auto converted, ConvertToVector(batch));
      out.extend(converted);
    }

    return out;
  }

  /// \brief Convert a record batch stream to a iterator of rows.
  ///
  /// \param reader a record batch reader
  /// \return A iterator of rows yielding each row produced by reader
  Iterator<RowType> ConvertToIterator(std::shared_ptr<RecordBatchReader> reader) {
    auto read_batch =
        [this](const std::shared_ptr<RecordBatch>& batch) -> Result<Iterator<RowType>> {
      ARROW_ASSIGN_OR_RAISE(auto rows, ConvertToVector(batch));
      return MakeVectorIterator(std::move(rows));
    };

    auto nested_iter =
        MakeMaybeMapIterator(read_batch, MakeIteratorFromReader(std::move(reader)));

    return MakeFlattenIterator(std::move(nested_iter));
  }

  /// \brief Convert an Arrow table to a iterator of rows.
  ///
  /// \param table Table to convert
  /// \param batch_size Number of rows to convert at once
  /// \return A iterator of rows yielding each row in table
  Iterator<RowType> ConvertToIterator(std::shared_ptr<Table> table, size_t batch_size) {
    auto batch_reader = std::make_shared<TableBatchReader>(*table);
    batch_reader->set_chunksize(batch_size);

    return ConvertToIterator(batch_reader);
  }

  /// \brief Convert an Arrow record batch to an iterator of rows.
  ///
  /// \param batch Record batch to convert
  /// \param batch_size Number of rows to convert at once
  /// \return A iterator of rows yielding each row in batch
  Iterator<RowType> ConvertToIterator(std::shared_ptr<RecordBatch> batch,
                                      size_t batch_size) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                          Table::FromRecordBatches({batch}));
    return ConvertToIterator(table, batch_size);
  }
};

/// \brief Base class for Row to Arrow conversions.
///
/// To use, specialize the conversion for a particular row type.
///
/// \tparam RowType type that contains a single row
/// \tparam ContainerType optional alternative vector implementation
template <typename RowType, template <typename...> class ContainerType = std::vector>
class FromRowConverter {
 public:
  /// \brief Convert a vector of rows to a record batch.
  ///
  /// Implement this method to enable all other methods.
  ///
  /// \param rows Vector of rows to convert
  /// \param schema Schema of record batch to convert to
  /// \return Arrow result with record batch containing all rows
  virtual Result<std::shared_ptr<RecordBatch>> ConvertToRecordBatch(
      const ContainerType<RowType>& rows, std::shared_ptr<Schema> schema) = 0;

  /// \brief Convert a vector of rows to an Arrow table
  ///
  /// \param rows Vector of rows to convert
  /// \param schema Schema of record batch to convert to
  /// \return Arrow result with table containing all rows
  Result<std::shared_ptr<Table>> ConvertToTable(const ContainerType<RowType>& rows,
                                                std::shared_ptr<Schema> schema) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> batch,
                          ConvertToRecordBatch(rows, schema));
    return Table::FromRecordBatches({batch});
  }

  /// \brief Convert a vector of rows to a record batch reader.
  ///
  /// \param row_iter Iterator of rows
  /// \param schema Schema of record batches to convert to
  /// \param batch_size Max size of record batches to produce in reader
  /// \return Arrow result with record batch reader containing all rows
  Result<std::shared_ptr<RecordBatchReader>> ConvertToReader(
      Iterator<RowType> row_iter, std::shared_ptr<Schema> schema, size_t batch_size) {
    auto get_next_batch = [row_iter, batch_size, schema,
                           this]() -> Result<std::shared_ptr<RecordBatch>> {
      ContainerType<RowType> rows;
      RowType row;
      rows.reserve(batch_size);

      while (true) {
        auto row_res = row_iter->Next();
        if (row_res.ok()) {
          row = std::move(row).MoveValueUnsafe();
        }
        if (row == NULLPTR) {
          break;
        }
        rows.emplace_back(row);

        if (rows.size() == batch_size) {
          break;
        }
      }

      if (rows.size() > 0) {
        return ConvertToRecordBatch(rows, schema);
      } else {
        return IterationTraits<RowType>::End();
      }
    };

    auto batch_iter = MakeFunctionIterator(get_next_batch);

    return SimpleRecordBatchReader(batch_iter, schema);
  }
};

}  // namespace arrow
