//
// Created by jose on 9/24/21.
//

#ifndef ARROW_SQLITE_TABLES_SCHEMA_BATCH_READER_H
#define ARROW_SQLITE_TABLES_SCHEMA_BATCH_READER_H

#include <arrow/record_batch.h>
#include <sqlite3.h>
#include "arrow/flight/flight-sql/example/sqlite_statement.h"
#include "arrow/flight/flight-sql/example/sqlite_statement_batch_reader.h"

namespace arrow {
namespace flight {
namespace sql {

class SqliteTablesWithSchemaBatchReader : public RecordBatchReader {
 public:
  std::shared_ptr<example::SqliteStatementBatchReader> reader_;
  sqlite3* db_;

  SqliteTablesWithSchemaBatchReader(std::shared_ptr<example::SqliteStatementBatchReader> reader,
                                    sqlite3* db_)
      : reader_(reader), db_(db_) {}

  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override;

  std::shared_ptr<DataType> GetArrowType(const std::string& sqlite_type);
};
}  // namespace sql
}  // namespace flight
}  // namespace arrow

#endif  // ARROW_SQLITE_TABLES_SCHEMA_BATCH_READER_H
