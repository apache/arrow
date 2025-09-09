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

#include <optional>
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/sql/odbc/flight_sql/record_batch_transformer.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"
#include "arrow/status.h"

namespace driver {
namespace flight_sql {

using arrow::Int16Builder;
using arrow::Int32Builder;
using arrow::Result;
using arrow::Status;
using arrow::StringBuilder;

using odbcabstraction::MetadataSettings;
using std::optional;

class GetColumns_RecordBatchBuilder {
 private:
  odbcabstraction::OdbcVersion odbc_version_;

  StringBuilder TABLE_CAT_Builder_;
  StringBuilder TABLE_SCHEM_Builder_;
  StringBuilder TABLE_NAME_Builder_;
  StringBuilder COLUMN_NAME_Builder_;
  Int16Builder DATA_TYPE_Builder_;
  StringBuilder TYPE_NAME_Builder_;
  Int32Builder COLUMN_SIZE_Builder_;
  Int32Builder BUFFER_LENGTH_Builder_;
  Int16Builder DECIMAL_DIGITS_Builder_;
  Int16Builder NUM_PREC_RADIX_Builder_;
  Int16Builder NULLABLE_Builder_;
  StringBuilder REMARKS_Builder_;
  StringBuilder COLUMN_DEF_Builder_;
  Int16Builder SQL_DATA_TYPE_Builder_;
  Int16Builder SQL_DATETIME_SUB_Builder_;
  Int32Builder CHAR_OCTET_LENGTH_Builder_;
  Int32Builder ORDINAL_POSITION_Builder_;
  StringBuilder IS_NULLABLE_Builder_;
  int64_t num_rows_{0};

 public:
  struct Data {
    optional<std::string> table_cat;
    optional<std::string> table_schem;
    std::string table_name;
    std::string column_name;
    std::string type_name;
    optional<int32_t> column_size;
    optional<int32_t> buffer_length;
    optional<int16_t> decimal_digits;
    optional<int16_t> num_prec_radix;
    optional<std::string> remarks;
    optional<std::string> column_def;
    int16_t sql_data_type{};
    optional<int16_t> sql_datetime_sub;
    optional<int32_t> char_octet_length;
    optional<std::string> is_nullable;
    int16_t data_type;
    int16_t nullable;
    int32_t ordinal_position;
  };

  explicit GetColumns_RecordBatchBuilder(odbcabstraction::OdbcVersion odbc_version);

  Result<std::shared_ptr<RecordBatch>> Build();

  Status Append(const Data& data);
};

class GetColumns_Transformer : public RecordBatchTransformer {
 private:
  const MetadataSettings& metadata_settings_;
  odbcabstraction::OdbcVersion odbc_version_;
  optional<std::string> column_name_pattern_;

 public:
  explicit GetColumns_Transformer(const MetadataSettings& metadata_settings,
                                  odbcabstraction::OdbcVersion odbc_version,
                                  const std::string* column_name_pattern);

  std::shared_ptr<RecordBatch> Transform(
      const std::shared_ptr<RecordBatch>& original) override;

  std::shared_ptr<Schema> GetTransformedSchema() override;
};

}  // namespace flight_sql
}  // namespace driver
