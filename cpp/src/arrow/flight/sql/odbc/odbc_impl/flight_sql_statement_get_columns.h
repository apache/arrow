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
#include "arrow/flight/sql/odbc/odbc_impl/record_batch_transformer.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"
#include "arrow/status.h"

namespace arrow::flight::sql::odbc {

class GetColumns_RecordBatchBuilder {
 private:
  OdbcVersion odbc_version_;

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
    std::optional<std::string> table_cat;
    std::optional<std::string> table_schem;
    std::string table_name;
    std::string column_name;
    std::string type_name;
    std::optional<int32_t> column_size;
    std::optional<int32_t> buffer_length;
    std::optional<int16_t> decimal_digits;
    std::optional<int16_t> num_prec_radix;
    std::optional<std::string> remarks;
    std::optional<std::string> column_def;
    int16_t sql_data_type{};
    std::optional<int16_t> sql_datetime_sub;
    std::optional<int32_t> char_octet_length;
    std::optional<std::string> is_nullable;
    int16_t data_type;
    int16_t nullable;
    int32_t ordinal_position;
  };

  explicit GetColumns_RecordBatchBuilder(OdbcVersion odbc_version);

  arrow::Result<std::shared_ptr<RecordBatch>> Build();

  Status Append(const Data& data);
};

class GetColumns_Transformer : public RecordBatchTransformer {
 private:
  const MetadataSettings& metadata_settings_;
  OdbcVersion odbc_version_;
  std::optional<std::string> column_name_pattern_;

 public:
  explicit GetColumns_Transformer(const MetadataSettings& metadata_settings,
                                  OdbcVersion odbc_version,
                                  const std::string* column_name_pattern);

  std::shared_ptr<RecordBatch> Transform(
      const std::shared_ptr<RecordBatch>& original) override;

  std::shared_ptr<Schema> GetTransformedSchema() override;
};

}  // namespace arrow::flight::sql::odbc
