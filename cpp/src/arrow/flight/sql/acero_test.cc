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

/// Integration test using the Acero backend

#include <memory>
#include <sstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/flight/server.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/example/acero_server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/types.h"
#include "arrow/scalar.h"
#include "arrow/stl_iterator.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace flight {
namespace sql {

using arrow::internal::checked_cast;

class TestAcero : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 0));
    flight::FlightServerOptions options(location);

    ASSERT_OK_AND_ASSIGN(server_, acero_example::MakeAceroServer());
    ASSERT_OK(server_->Init(options));

    ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(server_->location()));
    client_.reset(new FlightSqlClient(std::move(client)));
  }

  void TearDown() override {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightSqlClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

arrow::Result<std::shared_ptr<Buffer>> MakeSubstraitPlan() {
  ARROW_ASSIGN_OR_RAISE(std::string dir_string,
                        arrow::internal::GetEnvVar("PARQUET_TEST_DATA"));
  ARROW_ASSIGN_OR_RAISE(auto dir,
                        arrow::internal::PlatformFilename::FromString(dir_string));
  ARROW_ASSIGN_OR_RAISE(auto filename, dir.Join("binary.parquet"));
  std::string uri = std::string("file://") + filename.ToString();

  // TODO(ARROW-17229): we should use a RootRel here
  std::string json_plan = R"({
    "relations": [
      {
        "rel": {
          "read": {
            "base_schema": {
              "struct": {
                "types": [
                  {"binary": {}}
                ]
              },
              "names": [
                "foo"
              ]
            },
            "local_files": {
              "items": [
                {
                  "uri_file": "URI_PLACEHOLDER",
                  "parquet": {}
                }
              ]
            }
          }
        }
      }
    ]
})";
  std::string uri_placeholder = "URI_PLACEHOLDER";
  json_plan.replace(json_plan.find(uri_placeholder), uri_placeholder.size(), uri);
  return engine::SerializeJsonPlan(json_plan);
}

TEST_F(TestAcero, GetSqlInfo) {
  FlightCallOptions call_options;
  std::vector<int> sql_info_codes = {
      SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT,
      SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
  };
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       client_->GetSqlInfo(call_options, sql_info_codes));
  ASSERT_OK_AND_ASSIGN(auto reader,
                       client_->DoGet(call_options, flight_info->endpoints()[0].ticket));
  ASSERT_OK_AND_ASSIGN(auto results, reader->ToTable());
  ASSERT_OK_AND_ASSIGN(auto batch, results->CombineChunksToBatch());
  ASSERT_EQ(2, results->num_rows());
  std::vector<std::pair<uint32_t, SqlInfoResult>> info;
  const auto& ids = checked_cast<const UInt32Array&>(*batch->column(0));
  const auto& values = checked_cast<const DenseUnionArray&>(*batch->column(1));
  for (int64_t i = 0; i < batch->num_rows(); i++) {
    ASSERT_OK_AND_ASSIGN(auto scalar, values.GetScalar(i));
    if (ids.Value(i) == SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT) {
      ASSERT_EQ(*checked_cast<const DenseUnionScalar&>(*scalar).value,
                BooleanScalar(true));
    } else if (ids.Value(i) == SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION) {
      ASSERT_EQ(
          *checked_cast<const DenseUnionScalar&>(*scalar).value,
          Int32Scalar(
              SqlInfoOptions::SqlSupportedTransaction::SQL_SUPPORTED_TRANSACTION_NONE));
    } else {
      FAIL() << "Unexpected info value: " << ids.Value(i);
    }
  }
}

TEST_F(TestAcero, Scan) {
  FlightCallOptions call_options;
  ASSERT_OK_AND_ASSIGN(auto serialized_plan, MakeSubstraitPlan());

  SubstraitPlan plan{serialized_plan->ToString(), /*version=*/"0.6.0"};
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FlightInfo> info,
                       client_->ExecuteSubstrait(call_options, plan));
  ipc::DictionaryMemo memo;
  ASSERT_OK_AND_ASSIGN(auto schema, info->GetSchema(&memo));
  // TODO(ARROW-17229): the scanner "special" fields are still included, strip them
  // manually
  auto fixed_schema = arrow::schema({schema->fields()[0]});
  ASSERT_NO_FATAL_FAILURE(
      AssertSchemaEqual(fixed_schema, arrow::schema({field("foo", binary())})));

  ASSERT_EQ(1, info->endpoints().size());
  ASSERT_EQ(0, info->endpoints()[0].locations.size());
  ASSERT_OK_AND_ASSIGN(auto reader,
                       client_->DoGet(call_options, info->endpoints()[0].ticket));
  ASSERT_OK_AND_ASSIGN(auto reader_schema, reader->GetSchema());
  ASSERT_NO_FATAL_FAILURE(AssertSchemaEqual(schema, reader_schema));
  ASSERT_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_GT(table->num_rows(), 0);
}

TEST_F(TestAcero, Update) {
  FlightCallOptions call_options;
  ASSERT_OK_AND_ASSIGN(auto serialized_plan, MakeSubstraitPlan());
  SubstraitPlan plan{serialized_plan->ToString(), /*version=*/"0.6.0"};
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("Updates are unsupported"),
                                  client_->ExecuteSubstraitUpdate(call_options, plan));
}

TEST_F(TestAcero, Prepare) {
  FlightCallOptions call_options;
  ASSERT_OK_AND_ASSIGN(auto serialized_plan, MakeSubstraitPlan());
  SubstraitPlan plan{serialized_plan->ToString(), /*version=*/"0.6.0"};
  ASSERT_OK_AND_ASSIGN(auto prepared_statement,
                       client_->PrepareSubstrait(call_options, plan));
  ASSERT_NE(prepared_statement->dataset_schema(), nullptr);
  ASSERT_EQ(prepared_statement->parameter_schema(), nullptr);

  auto fixed_schema = arrow::schema({prepared_statement->dataset_schema()->fields()[0]});
  ASSERT_NO_FATAL_FAILURE(
      AssertSchemaEqual(fixed_schema, arrow::schema({field("foo", binary())})));

  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("Updates are unsupported"),
                                  prepared_statement->ExecuteUpdate());

  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FlightInfo> info, prepared_statement->Execute());
  ASSERT_EQ(1, info->endpoints().size());
  ASSERT_EQ(0, info->endpoints()[0].locations.size());
  ASSERT_OK_AND_ASSIGN(auto reader,
                       client_->DoGet(call_options, info->endpoints()[0].ticket));
  ASSERT_OK_AND_ASSIGN(auto reader_schema, reader->GetSchema());
  ASSERT_NO_FATAL_FAILURE(
      AssertSchemaEqual(prepared_statement->dataset_schema(), reader_schema));
  ASSERT_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_GT(table->num_rows(), 0);

  ASSERT_OK(prepared_statement->Close());
}

TEST_F(TestAcero, Transactions) {
  FlightCallOptions call_options;
  ASSERT_OK_AND_ASSIGN(auto serialized_plan, MakeSubstraitPlan());
  Transaction handle("fake-id");
  SubstraitPlan plan{serialized_plan->ToString(), /*version=*/"0.6.0"};

  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("Transactions are unsupported"),
                                  client_->ExecuteSubstrait(call_options, plan, handle));
  EXPECT_RAISES_WITH_MESSAGE_THAT(NotImplemented,
                                  ::testing::HasSubstr("Transactions are unsupported"),
                                  client_->PrepareSubstrait(call_options, plan, handle));
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
