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
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "arrow/array/array_binary.h"
#include "arrow/flight/client.h"
#include "arrow/flight/client_tracing_middleware.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/visibility.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_builder.h"

// Override the built-in visibility defines with Arrow's
#define ADBC_EXPORT ARROW_FLIGHT_SQL_EXPORT
#include "arrow/c/adbc_internal.h"  // IWYU pragma: export

namespace arrow::flight::sql {

// Internal utilities for the Flight SQL driver.

#define ADBC_RETURN_NOT_OK(EXPR)         \
  do {                                   \
    auto _s = (EXPR);                    \
    if (_s != ADBC_STATUS_OK) return _s; \
  } while (false)

#ifdef ARROW_EXTRA_ERROR_CONTEXT
#define ADBC_ARROW_RETURN_NOT_OK(ERROR, EXPR)                       \
  do {                                                              \
    if (Status _s = (EXPR); !_s.ok()) {                             \
      _s.AddContextLine(__FILE__, __LINE__, ARROW_STRINGIFY(EXPR)); \
      SetError(error, _s);                                          \
      return ::arrow::flight::sql::ArrowToAdbcStatusCode(_s);       \
    }                                                               \
  } while (false)
#else
#define ADBC_ARROW_RETURN_NOT_OK(ERROR, EXPR)                 \
  do {                                                        \
    if (Status _s = (EXPR); !_s.ok()) {                       \
      SetError(error, _s);                                    \
      return ::arrow::flight::sql::ArrowToAdbcStatusCode(_s); \
    }                                                         \
  } while (false)
#endif

ARROW_FLIGHT_SQL_EXPORT
AdbcStatusCode ArrowToAdbcStatusCode(const Status& status);
ARROW_FLIGHT_SQL_EXPORT
void ReleaseError(struct AdbcError* error);
ARROW_FLIGHT_SQL_EXPORT
FlightClientOptions DefaultClientOptions();

/// Helper to populate an AdbcError
template <typename... Args>
void SetError(struct AdbcError* error, Args&&... args) {
  if (!error) return;
  std::string message = util::StringBuilder("[Flight SQL] ", std::forward<Args>(args)...);
  if (error->message) {
    message.reserve(message.size() + 1 + std::strlen(error->message));
    message.append(1, '\n');
    message.append(error->message);
    delete[] error->message;
  }
  error->message = new char[message.size() + 1];
  message.copy(error->message, message.size());
  error->message[message.size()] = '\0';
  error->release = ReleaseError;
}

template <typename ArrowType,
          typename ArrayType = typename TypeTraits<ArrowType>::ArrayType>
auto GetNotNull(const RecordBatch& batch, int col_index, int64_t row_index) {
  return arrow::internal::checked_cast<const ArrayType&>(*batch.column(col_index))
      .GetView(row_index);
}

template <typename ArrowType,
          typename ArrayType = typename TypeTraits<ArrowType>::ArrayType>
std::optional<std::invoke_result_t<decltype(&ArrayType::GetView), ArrayType, int64_t>>
GetNullable(const RecordBatch& batch, int col_index, int64_t row_index) {
  // compiler unfortunately can't infer the return type here
  const auto& arr =
      arrow::internal::checked_cast<const ArrayType&>(*batch.column(col_index));
  if (arr.IsNull(row_index)) return std::nullopt;
  return arr.GetView(row_index);
}

/// \brief Helper to iterate over a RecordBatchReader in a rowwise
/// manner
class ARROW_FLIGHT_SQL_EXPORT ReaderIterator {
 public:
  explicit ReaderIterator(const Schema& schema, RecordBatchReader* reader)
      : schema_(schema), reader_(reader) {}

  Status Init() {
    if (!schema_.Equals(*reader_->schema())) {
      return Status::Invalid("Server sent the wrong schema.\nExpected:", schema_,
                             "\nActual:", *reader_->schema());
    }
    return reader_->Next().Value(&current_);
  }

  std::shared_ptr<RecordBatch> NewBatch() const {
    return current_row_ == 0 ? current_ : nullptr;
  }

  arrow::Result<bool> Next() {
    if (done_) return false;

    current_row_++;
    while (current_ && current_row_ >= current_->num_rows()) {
      ARROW_ASSIGN_OR_RAISE(current_, reader_->Next());
      if (!current_) {
        done_ = true;
        return false;
      }
      current_row_ = 0;
    }
    return current_ != nullptr;
  }

  template <typename ArrowType,
            typename ArrayType = typename TypeTraits<ArrowType>::ArrayType>
  std::invoke_result_t<decltype(&ArrayType::GetView), ArrayType, int64_t> GetNotNull(
      int col_index) const {
    return sql::GetNotNull<ArrowType>(*current_, col_index, current_row_);
  }

  template <typename ArrowType,
            typename ArrayType = typename TypeTraits<ArrowType>::ArrayType>
  std::optional<std::invoke_result_t<decltype(&ArrayType::GetView), ArrayType, int64_t>>
  GetNullable(int col_index) const {
    return sql::GetNullable<ArrowType>(*current_, col_index, current_row_);
  }

 private:
  const Schema& schema_;
  RecordBatchReader* reader_;
  std::shared_ptr<RecordBatch> current_;
  int64_t current_row_ = -1;
  bool done_ = false;
};

// {[catalog name]: {[db schema name]: (inclusive_lower_bound, exclusive_upper_bound)}}
using TableIndex = std::unordered_map<
    std::optional<std::string>,
    std::unordered_map<std::optional<std::string>, std::pair<int64_t, int64_t>>>;

/// Build up an index of database metadata in-memory to help implement GetObjects.
ARROW_FLIGHT_SQL_EXPORT
Status IndexDbSchemas(RecordBatchReader* reader, TableIndex* table_index);

/// Build up an index of database metadata in-memory to help implement GetObjects.
ARROW_FLIGHT_SQL_EXPORT
Status IndexTables(RecordBatchReader* reader, TableIndex* table_index,
                   std::shared_ptr<RecordBatch>* table_data);

/// Export batches of data as an ArrayStream.
ARROW_FLIGHT_SQL_EXPORT
Status ExportRecordBatches(std::shared_ptr<Schema> schema, RecordBatchVector batches,
                           struct ArrowArrayStream* stream);

/// Check if a name is (probably) a valid SQL identifier. We don't
/// know the exact SQL syntax (though we could try to take advantage
/// of the info in GetSqlInfo), so this function is conservative about
/// allowed identifiers.
ARROW_FLIGHT_SQL_EXPORT
bool IsApproximatelyValidIdentifier(std::string_view name);

}  // namespace arrow::flight::sql
