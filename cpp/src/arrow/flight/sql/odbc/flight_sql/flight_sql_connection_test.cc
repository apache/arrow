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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/types.h"
#include "gtest/gtest.h"

namespace driver {
namespace flight_sql {

using arrow::flight::Location;
using arrow::flight::TimeoutDuration;
using odbcabstraction::Connection;

TEST(AttributeTests, SetAndGetAttribute) {
  FlightSqlConnection connection(odbcabstraction::V_3);
  connection.SetClosed(false);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, static_cast<uint32_t>(200));
  const boost::optional<Connection::Attribute> firstValue =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(firstValue);

  EXPECT_EQ(boost::get<uint32_t>(*firstValue), static_cast<uint32_t>(200));

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, static_cast<uint32_t>(300));

  const boost::optional<Connection::Attribute> changeValue =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);

  EXPECT_TRUE(changeValue);
  EXPECT_EQ(boost::get<uint32_t>(*changeValue), static_cast<uint32_t>(300));

  connection.Close();
}

TEST(AttributeTests, GetAttributeWithoutSetting) {
  FlightSqlConnection connection(odbcabstraction::V_3);

  const boost::optional<Connection::Attribute> optional =
      connection.GetAttribute(Connection::CONNECTION_TIMEOUT);
  connection.SetClosed(false);

  EXPECT_EQ(0, boost::get<uint32_t>(*optional));

  connection.Close();
}

TEST(MetadataSettingsTest, StringColumnLengthTest) {
  FlightSqlConnection connection(odbcabstraction::V_3);
  connection.SetClosed(false);

  const int32_t expected_string_column_length = 100000;

  const Connection::ConnPropertyMap properties = {
      {FlightSqlConnection::HOST, std::string("localhost")},        // expect not used
      {FlightSqlConnection::PORT, std::string("32010")},            // expect not used
      {FlightSqlConnection::USE_ENCRYPTION, std::string("false")},  // expect not used
      {FlightSqlConnection::STRING_COLUMN_LENGTH,
       std::to_string(expected_string_column_length)},
  };

  const boost::optional<int32_t> actual_string_column_length =
      connection.GetStringColumnLength(properties);

  EXPECT_TRUE(actual_string_column_length);
  EXPECT_EQ(expected_string_column_length, *actual_string_column_length);

  connection.Close();
}

TEST(MetadataSettingsTest, UseWideCharTest) {
  FlightSqlConnection connection(odbcabstraction::V_3);
  connection.SetClosed(false);

  const Connection::ConnPropertyMap properties1 = {
      {FlightSqlConnection::USE_WIDE_CHAR, std::string("true")},
  };
  const Connection::ConnPropertyMap properties2 = {
      {FlightSqlConnection::USE_WIDE_CHAR, std::string("false")},
  };

  EXPECT_EQ(true, connection.GetUseWideChar(properties1));
  EXPECT_EQ(false, connection.GetUseWideChar(properties2));

  connection.Close();
}

TEST(BuildLocationTests, ForTcp) {
  std::vector<std::string_view> missing_attr;
  Connection::ConnPropertyMap properties = {
      {FlightSqlConnection::HOST, std::string("localhost")},
      {FlightSqlConnection::PORT, std::string("32010")},
      {FlightSqlConnection::USE_ENCRYPTION, std::string("false")},
  };

  const std::shared_ptr<FlightSqlSslConfig>& ssl_config =
      LoadFlightSslConfigs(properties);

  const Location& actual_location1 =
      FlightSqlConnection::BuildLocation(properties, missing_attr, ssl_config);
  const Location& actual_location2 = FlightSqlConnection::BuildLocation(
      {
          {FlightSqlConnection::HOST, std::string("localhost")},
          {FlightSqlConnection::PORT, std::string("32011")},
      },
      missing_attr, ssl_config);

  Location expected_location;
  ASSERT_TRUE(Location::ForGrpcTcp("localhost", 32010).Value(&expected_location).ok());
  ASSERT_EQ(expected_location, actual_location1);
  ASSERT_NE(expected_location, actual_location2);
}

TEST(BuildLocationTests, ForTls) {
  std::vector<std::string_view> missing_attr;
  Connection::ConnPropertyMap properties = {
      {FlightSqlConnection::HOST, std::string("localhost")},
      {FlightSqlConnection::PORT, std::string("32010")},
      {FlightSqlConnection::USE_ENCRYPTION, std::string("1")},
  };

  const std::shared_ptr<FlightSqlSslConfig>& ssl_config =
      LoadFlightSslConfigs(properties);

  const Location& actual_location1 =
      FlightSqlConnection::BuildLocation(properties, missing_attr, ssl_config);

  Connection::ConnPropertyMap second_properties = {
      {FlightSqlConnection::HOST, std::string("localhost")},
      {FlightSqlConnection::PORT, std::string("32011")},
      {FlightSqlConnection::USE_ENCRYPTION, std::string("1")},
  };

  const std::shared_ptr<FlightSqlSslConfig>& second_ssl_config =
      LoadFlightSslConfigs(properties);

  const Location& actual_location2 =
      FlightSqlConnection::BuildLocation(second_properties, missing_attr, ssl_config);

  Location expected_location;
  ASSERT_TRUE(Location::ForGrpcTls("localhost", 32010).Value(&expected_location).ok());
  ASSERT_EQ(expected_location, actual_location1);
  ASSERT_NE(expected_location, actual_location2);
}

TEST(PopulateCallOptionsTest, ConnectionTimeout) {
  FlightSqlConnection connection(odbcabstraction::V_3);
  connection.SetClosed(false);

  // Expect default timeout to be -1
  ASSERT_EQ(TimeoutDuration{-1.0},
            connection.PopulateCallOptions(Connection::ConnPropertyMap()).timeout);

  connection.SetAttribute(Connection::CONNECTION_TIMEOUT, static_cast<uint32_t>(10));
  ASSERT_EQ(TimeoutDuration{10.0},
            connection.PopulateCallOptions(Connection::ConnPropertyMap()).timeout);
}

TEST(PopulateCallOptionsTest, GenericOption) {
  FlightSqlConnection connection(odbcabstraction::V_3);
  connection.SetClosed(false);

  Connection::ConnPropertyMap properties;
  properties["Foo"] = "Bar";
  auto options = connection.PopulateCallOptions(properties);
  auto headers = options.headers;
  ASSERT_EQ(1, headers.size());

  // Header name must be lower-case because gRPC will crash if it is not lower-case.
  ASSERT_EQ("foo", headers[0].first);

  // Header value should preserve case.
  ASSERT_EQ("Bar", headers[0].second);
}

TEST(PopulateCallOptionsTest, GenericOptionWithSpaces) {
  FlightSqlConnection connection(odbcabstraction::V_3);
  connection.SetClosed(false);

  Connection::ConnPropertyMap properties;
  properties["Persist Security Info"] = "False";
  auto options = connection.PopulateCallOptions(properties);
  auto headers = options.headers;
  // Header names with spaces must be omitted or gRPC will crash.
  ASSERT_TRUE(headers.empty());
}

}  // namespace flight_sql
}  // namespace driver

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
