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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_auth_method.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"

#include "arrow/flight/client.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include <utility>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;
using arrow::flight::TimeoutDuration;
using driver::odbcabstraction::AuthenticationException;
using driver::odbcabstraction::CommunicationException;
using driver::odbcabstraction::Connection;

namespace {
class NoOpAuthMethod : public FlightSqlAuthMethod {
 public:
  void Authenticate(FlightSqlConnection& connection,
                    FlightCallOptions& call_options) override {
    // Do nothing
  }
};

class NoOpClientAuthHandler : public arrow::flight::ClientAuthHandler {
 public:
  NoOpClientAuthHandler() {}

  arrow::Status Authenticate(arrow::flight::ClientAuthSender* outgoing,
                             arrow::flight::ClientAuthReader* incoming) override {
    // Write a blank string. The server should ignore this and just accept any Handshake
    // request.
    return outgoing->Write(std::string());
  }

  arrow::Status GetToken(std::string* token) override {
    *token = std::string();
    return arrow::Status::OK();
  }
};

class UserPasswordAuthMethod : public FlightSqlAuthMethod {
 public:
  UserPasswordAuthMethod(FlightClient& client, std::string user, std::string password)
      : client_(client), user_(std::move(user)), password_(std::move(password)) {}

  void Authenticate(FlightSqlConnection& connection,
                    FlightCallOptions& call_options) override {
    FlightCallOptions auth_call_options;
    const boost::optional<Connection::Attribute>& login_timeout =
        connection.GetAttribute(Connection::LOGIN_TIMEOUT);
    if (login_timeout && boost::get<uint32_t>(*login_timeout) > 0) {
      // ODBC's LOGIN_TIMEOUT attribute and FlightCallOptions.timeout use
      // seconds as time unit.
      double timeout_seconds = static_cast<double>(boost::get<uint32_t>(*login_timeout));
      if (timeout_seconds > 0) {
        auth_call_options.timeout = TimeoutDuration{timeout_seconds};
      }
    }

    Result<std::pair<std::string, std::string>> bearer_result =
        client_.AuthenticateBasicToken(auth_call_options, user_, password_);

    if (!bearer_result.ok()) {
      const auto& flightStatus =
          arrow::flight::FlightStatusDetail::UnwrapStatus(bearer_result.status());
      if (flightStatus != nullptr) {
        if (flightStatus->code() == arrow::flight::FlightStatusCode::Unauthenticated) {
          throw AuthenticationException(
              "Failed to authenticate with user and password: " +
              bearer_result.status().ToString());
        } else if (flightStatus->code() == arrow::flight::FlightStatusCode::Unavailable) {
          throw CommunicationException(bearer_result.status().message());
        }
      }

      throw odbcabstraction::DriverException(bearer_result.status().message());
    }

    call_options.headers.push_back(bearer_result.ValueOrDie());
  }

  std::string GetUser() override { return user_; }

 private:
  FlightClient& client_;
  std::string user_;
  std::string password_;
};

class TokenAuthMethod : public FlightSqlAuthMethod {
 private:
  FlightClient& client_;
  std::string token_;  // this is the token the user provides

 public:
  TokenAuthMethod(FlightClient& client, std::string token)
      : client_{client}, token_{std::move(token)} {}

  void Authenticate(FlightSqlConnection& connection,
                    FlightCallOptions& call_options) override {
    // add the token to the headers
    const std::pair<std::string, std::string> token_header("authorization",
                                                           "Bearer " + token_);
    call_options.headers.push_back(token_header);

    const arrow::Status status = client_.Authenticate(
        call_options,
        std::unique_ptr<arrow::flight::ClientAuthHandler>(new NoOpClientAuthHandler()));
    if (!status.ok()) {
      const auto& flightStatus = arrow::flight::FlightStatusDetail::UnwrapStatus(status);
      if (flightStatus != nullptr) {
        if (flightStatus->code() == arrow::flight::FlightStatusCode::Unauthenticated) {
          throw AuthenticationException("Failed to authenticate with token: " + token_ +
                                        " Message: " + status.message());
        } else if (flightStatus->code() == arrow::flight::FlightStatusCode::Unavailable) {
          throw CommunicationException(status.message());
        }
      }
      throw odbcabstraction::DriverException(status.message());
    }
  }
};
}  // namespace

std::unique_ptr<FlightSqlAuthMethod> FlightSqlAuthMethod::FromProperties(
    const std::unique_ptr<FlightClient>& client,
    const Connection::ConnPropertyMap& properties) {
  // Check if should use user-password authentication
  auto it_user = properties.find(FlightSqlConnection::USER);
  if (it_user == properties.end()) {
    // The Microsoft OLE DB to ODBC bridge provider (MSDASQL) will write
    // "User ID" and "Password" properties instead of mapping
    // to ODBC compliant UID/PWD keys.
    it_user = properties.find(FlightSqlConnection::USER_ID);
  }

  auto it_password = properties.find(FlightSqlConnection::PASSWORD);
  auto it_token = properties.find(FlightSqlConnection::TOKEN);

  if (it_user == properties.end() || it_password == properties.end()) {
    // Accept UID/PWD as aliases for User/Password. These are suggested as
    // standard properties in the documentation for SQLDriverConnect.
    it_user = properties.find(FlightSqlConnection::UID);
    it_password = properties.find(FlightSqlConnection::PWD);
  }
  if (it_user != properties.end() || it_password != properties.end()) {
    const std::string& user = it_user != properties.end() ? it_user->second : "";
    const std::string& password =
        it_password != properties.end() ? it_password->second : "";

    return std::unique_ptr<FlightSqlAuthMethod>(
        new UserPasswordAuthMethod(*client, user, password));
  } else if (it_token != properties.end()) {
    const auto& token = it_token->second;
    return std::unique_ptr<FlightSqlAuthMethod>(new TokenAuthMethod(*client, token));
  }

  return std::unique_ptr<FlightSqlAuthMethod>(new NoOpAuthMethod);
}

}  // namespace flight_sql
}  // namespace driver
