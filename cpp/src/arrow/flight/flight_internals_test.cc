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

// ----------------------------------------------------------------------
// Tests for Flight which don't actually spin up a client/server

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include <type_traits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/flight/client_cookie_middleware.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/cookie_internal.h"
#include "arrow/flight/flight_data_decoder.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_async.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/async_grpc_compat.h"
#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/string.h"

// Include after Flight headers
#include <grpc/slice.h>

namespace arrow {
namespace flight {

namespace pb = arrow::flight::protocol;

// ----------------------------------------------------------------------
// Core Flight types

template <typename PbType, typename FlightType>
void TestRoundtrip(const std::vector<FlightType>& values,
                   const std::vector<std::string>& reprs) {
  for (size_t i = 0; i < values.size(); i++) {
    ARROW_SCOPED_TRACE("LHS = ", values[i].ToString());
    for (size_t j = 0; j < values.size(); j++) {
      ARROW_SCOPED_TRACE("RHS = ", values[j].ToString());
      if (i == j) {
        EXPECT_EQ(values[i], values[j]);
        EXPECT_TRUE(values[i].Equals(values[j]));
      } else {
        EXPECT_NE(values[i], values[j]);
        EXPECT_FALSE(values[i].Equals(values[j]));
      }
    }
    EXPECT_EQ(values[i].ToString(), reprs[i]);

    ASSERT_OK_AND_ASSIGN(std::string serialized, values[i].SerializeToString());
    ASSERT_OK_AND_ASSIGN(auto deserialized, FlightType::Deserialize(serialized));
    if constexpr (std::is_same_v<FlightType, FlightInfo> ||
                  std::is_same_v<FlightType, PollInfo>) {
      ARROW_SCOPED_TRACE("Deserialized = ", deserialized->ToString());
      EXPECT_EQ(values[i], *deserialized);
    } else {
      ARROW_SCOPED_TRACE("Deserialized = ", deserialized.ToString());
      EXPECT_EQ(values[i], deserialized);
    }

// This tests the internal protobuf types which don't get exported in the Flight DLL.
#ifndef _WIN32
    PbType pb_value;
    ASSERT_OK(internal::ToProto(values[i], &pb_value));

    if constexpr (std::is_same_v<FlightType, FlightInfo>) {
      FlightInfo::Data info_data;
      ASSERT_OK(internal::FromProto(pb_value, &info_data));
      EXPECT_EQ(values[i], FlightInfo{std::move(info_data)});
    } else if constexpr (std::is_same_v<FlightType, SchemaResult>) {
      SchemaResult value;
      ASSERT_OK(internal::FromProto(pb_value, &value));
      EXPECT_EQ(values[i], value);
    } else {
      FlightType value;
      ASSERT_OK(internal::FromProto(pb_value, &value));
      EXPECT_EQ(values[i], value);
    }
#endif
  }
}

TEST(FlightTypes, Action) {
  std::vector<Action> values = {
      {"type", Buffer::FromString("")},
      {"type", Buffer::FromString("foo")},
      {"type", Buffer::FromString("bar")},
  };
  std::vector<std::string> reprs = {
      "<Action type='type' body=(0 bytes)>",
      "<Action type='type' body=(3 bytes)>",
      "<Action type='type' body=(3 bytes)>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::Action>(values, reprs));

  // This doesn't roundtrip since we don't differentiate between no
  // body and empty body on the wire
  Action action{"", nullptr};
  ASSERT_EQ("<Action type='' body=(nullptr)>", action.ToString());
  ASSERT_NE(values[0], action);
  ASSERT_EQ(action, action);
}

TEST(FlightTypes, ActionType) {
  std::vector<ActionType> values = {
      {"", ""},
      {"type", ""},
      {"type", "descr"},
      {"", "descr"},
  };
  std::vector<std::string> reprs = {
      "<ActionType type='' description=''>",
      "<ActionType type='type' description=''>",
      "<ActionType type='type' description='descr'>",
      "<ActionType type='' description='descr'>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::ActionType>(values, reprs));
}

TEST(FlightTypes, BasicAuth) {
  std::vector<BasicAuth> values = {
      {"", ""},
      {"user", ""},
      {"", "pass"},
      {"user", "pass"},
  };
  std::vector<std::string> reprs = {
      "<BasicAuth username='' password=(redacted)>",
      "<BasicAuth username='user' password=(redacted)>",
      "<BasicAuth username='' password=(redacted)>",
      "<BasicAuth username='user' password=(redacted)>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::BasicAuth>(values, reprs));
}

TEST(FlightTypes, Criteria) {
  std::vector<Criteria> values = {Criteria{""}, Criteria{"criteria"}};
  std::vector<std::string> reprs = {
      "<Criteria expression=''>",
      "<Criteria expression='criteria'>",
  };
  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::Criteria>(values, reprs));
}

TEST(FlightTypes, FlightDescriptor) {
  std::vector<FlightDescriptor> values = {
      FlightDescriptor::Command(""),
      FlightDescriptor::Command("\x01"),
      FlightDescriptor::Command("select * from table"),
      FlightDescriptor::Command("select foo from table"),
      FlightDescriptor::Path({}),
      FlightDescriptor::Path({"foo", "baz"}),
  };
  std::vector<std::string> reprs = {
      "<FlightDescriptor cmd=''>",
      "<FlightDescriptor cmd='\x01'>",
      "<FlightDescriptor cmd='select * from table'>",
      "<FlightDescriptor cmd='select foo from table'>",
      "<FlightDescriptor path=''>",
      "<FlightDescriptor path='foo/baz'>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::FlightDescriptor>(values, reprs));
}

TEST(FlightTypes, FlightEndpoint) {
  ASSERT_OK_AND_ASSIGN(auto location1, Location::ForGrpcTcp("localhost", 1024));
  ASSERT_OK_AND_ASSIGN(auto location2, Location::ForGrpcTls("localhost", 1024));
  // 2023-06-19 03:14:06.004339123
  const auto expiration_time_duration =
      std::chrono::seconds{1687144446} + std::chrono::nanoseconds{4339123};
  Timestamp expiration_time(
      std::chrono::duration_cast<Timestamp::duration>(expiration_time_duration));
  std::vector<FlightEndpoint> values = {
      {Ticket{""}, {}, std::nullopt, {}},
      {Ticket{"foo"}, {}, std::nullopt, {}},
      {Ticket{"bar"}, {}, std::nullopt, {"\xDE\xAD\xBE\xEF"}},
      {Ticket{"foo"}, {}, expiration_time, {}},
      {Ticket{"foo"}, {location1}, std::nullopt, {}},
      {Ticket{"bar"}, {location1}, std::nullopt, {}},
      {Ticket{"foo"}, {location2}, std::nullopt, {}},
      {Ticket{"foo"}, {location1, location2}, std::nullopt, {"\xba\xdd\xca\xfe"}},
  };
  std::vector<std::string> reprs = {
      "<FlightEndpoint ticket=<Ticket ticket=''> locations=[] "
      "expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations=[] "
      "expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='bar'> locations=[] "
      "expiration_time=null app_metadata='DEADBEEF'>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations=[] "
      "expiration_time=2023-06-19 03:14:06.004339123 app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations="
      "[grpc+tcp://localhost:1024] expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='bar'> locations="
      "[grpc+tcp://localhost:1024] expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations="
      "[grpc+tls://localhost:1024] expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations="
      "[grpc+tcp://localhost:1024, grpc+tls://localhost:1024] "
      "expiration_time=null app_metadata='BADDCAFE'>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::FlightEndpoint>(values, reprs));
}

TEST(FlightTypes, FlightInfo) {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 1234));
  Schema schema1({field("ints", int64())});
  Schema schema2({});
  auto desc1 = FlightDescriptor::Command("foo");
  auto desc2 = FlightDescriptor::Command("bar");
  auto endpoint1 = FlightEndpoint{Ticket{"foo"}, {}, std::nullopt, ""};
  auto endpoint2 =
      FlightEndpoint{Ticket{"foo"}, {location}, std::nullopt, "\xCA\xFE\xD0\x0D"};
  std::vector<FlightInfo> values = {
      MakeFlightInfo(schema1, desc1, {}, -1, -1, false, ""),
      MakeFlightInfo(schema1, desc2, {}, -1, -1, true, ""),
      MakeFlightInfo(schema2, desc1, {}, -1, -1, false, ""),
      MakeFlightInfo(schema1, desc1, {endpoint1}, -1, 42, true, ""),
      MakeFlightInfo(schema1, desc2, {endpoint1, endpoint2}, 64, -1, false,
                     "\xDE\xAD\xC0\xDE"),
      MakeFlightInfo(desc1, {}, -1, -1, false, ""),
  };
  std::vector<std::string> reprs = {
      "<FlightInfo schema=(serialized) descriptor=<FlightDescriptor cmd='foo'> "
      "endpoints=[] total_records=-1 total_bytes=-1 ordered=false app_metadata=''>",
      "<FlightInfo schema=(serialized) descriptor=<FlightDescriptor cmd='bar'> "
      "endpoints=[] total_records=-1 total_bytes=-1 ordered=true app_metadata=''>",
      "<FlightInfo schema=(serialized) descriptor=<FlightDescriptor cmd='foo'> "
      "endpoints=[] total_records=-1 total_bytes=-1 ordered=false app_metadata=''>",
      "<FlightInfo schema=(serialized) descriptor=<FlightDescriptor cmd='foo'> "
      "endpoints=[<FlightEndpoint ticket=<Ticket ticket='foo'> locations=[] "
      "expiration_time=null app_metadata=''>] total_records=-1 total_bytes=42 "
      "ordered=true app_metadata=''>",
      "<FlightInfo schema=(serialized) descriptor=<FlightDescriptor cmd='bar'> "
      "endpoints=[<FlightEndpoint ticket=<Ticket ticket='foo'> locations=[] "
      "expiration_time=null app_metadata=''>, <FlightEndpoint ticket=<Ticket "
      "ticket='foo'> "
      "locations=[grpc+tcp://localhost:1234] expiration_time=null "
      "app_metadata='CAFED00D'>] "
      "total_records=64 total_bytes=-1 ordered=false app_metadata='DEADC0DE'>",
      "<FlightInfo schema=(empty) descriptor=<FlightDescriptor cmd='foo'> "
      "endpoints=[] total_records=-1 total_bytes=-1 ordered=false app_metadata=''>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::FlightInfo>(values, reprs));
}

TEST(FlightTypes, PollInfo) {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 1234));
  Schema schema({field("ints", int64())});
  auto desc = FlightDescriptor::Command("foo");
  auto endpoint = FlightEndpoint{Ticket{"foo"}, {}, std::nullopt, ""};
  auto info = MakeFlightInfo(schema, desc, {endpoint}, -1, 42, true, "");
  // 2023-06-19 03:14:06.004339123
  const auto expiration_time_duration =
      std::chrono::seconds{1687144446} + std::chrono::nanoseconds{4339123};
  Timestamp expiration_time(
      std::chrono::duration_cast<Timestamp::duration>(expiration_time_duration));
  std::vector<PollInfo> values = {
      PollInfo{std::make_unique<FlightInfo>(info), std::nullopt, std::nullopt,
               std::nullopt},
      PollInfo{std::make_unique<FlightInfo>(info), FlightDescriptor::Command("poll"), 0.1,
               expiration_time},
      PollInfo{},
  };
  std::vector<std::string> reprs = {
      "<PollInfo info=" + info.ToString() +
          " descriptor=null "
          "progress=null expiration_time=null>",
      "<PollInfo info=" + info.ToString() +
          " descriptor=<FlightDescriptor cmd='poll'> "
          "progress=0.1 expiration_time=2023-06-19 03:14:06.004339123>",
      "<PollInfo info=null descriptor=null progress=null expiration_time=null>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::PollInfo>(values, reprs));
}

TEST(FlightTypes, Result) {
  std::vector<Result> values = {
      Result{Buffer::FromString("")},
      Result{Buffer::FromString("foo")},
      Result{Buffer::FromString("bar")},
  };
  std::vector<std::string> reprs = {
      "<Result body=(0 bytes)>",
      "<Result body=(3 bytes)>",
      "<Result body=(3 bytes)>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::Result>(values, reprs));

  // This doesn't roundtrip since we don't differentiate between no
  // body and empty body on the wire
  Result result{nullptr};
  ASSERT_EQ("<Result body=(nullptr)>", result.ToString());
  ASSERT_NE(values[0], result);
  ASSERT_EQ(result, result);
}

TEST(FlightTypes, SchemaResult) {
  ASSERT_OK_AND_ASSIGN(auto value1, SchemaResult::Make(Schema({})));
  ASSERT_OK_AND_ASSIGN(auto value2, SchemaResult::Make(Schema({field("foo", int64())})));
  std::vector<SchemaResult> values = {*value1, *value2};
  std::vector<std::string> reprs = {
      "<SchemaResult raw_schema=(serialized)>",
      "<SchemaResult raw_schema=(serialized)>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::SchemaResult>(values, reprs));
}

TEST(FlightTypes, Ticket) {
  std::vector<Ticket> values = {
      Ticket{""},
      Ticket{"foo"},
      Ticket{"bar"},
  };
  std::vector<std::string> reprs = {
      "<Ticket ticket=''>",
      "<Ticket ticket='foo'>",
      "<Ticket ticket='bar'>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::Ticket>(values, reprs));
}

// ARROW-6017: we should be able to construct locations for unknown
// schemes
TEST(FlightTypes, LocationUnknownScheme) {
  ASSERT_OK(Location::Parse("s3://test"));
  ASSERT_OK(Location::Parse("https://example.com/foo"));
}

TEST(FlightTypes, LocationFallback) {
  EXPECT_EQ("arrow-flight-reuse-connection://?", Location::ReuseConnection().ToString());
  EXPECT_EQ("arrow-flight-reuse-connection", Location::ReuseConnection().scheme());
}

TEST(FlightTypes, RoundtripStatus) {
  // Make sure status codes round trip through our conversions

  std::shared_ptr<FlightStatusDetail> detail;
  detail = FlightStatusDetail::UnwrapStatus(
      MakeFlightError(FlightStatusCode::Internal, "Test message"));
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(FlightStatusCode::Internal, detail->code());

  detail = FlightStatusDetail::UnwrapStatus(
      MakeFlightError(FlightStatusCode::TimedOut, "Test message"));
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(FlightStatusCode::TimedOut, detail->code());

  detail = FlightStatusDetail::UnwrapStatus(
      MakeFlightError(FlightStatusCode::Cancelled, "Test message"));
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(FlightStatusCode::Cancelled, detail->code());

  detail = FlightStatusDetail::UnwrapStatus(
      MakeFlightError(FlightStatusCode::Unauthenticated, "Test message"));
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(FlightStatusCode::Unauthenticated, detail->code());

  detail = FlightStatusDetail::UnwrapStatus(
      MakeFlightError(FlightStatusCode::Unauthorized, "Test message"));
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(FlightStatusCode::Unauthorized, detail->code());

  detail = FlightStatusDetail::UnwrapStatus(
      MakeFlightError(FlightStatusCode::Unavailable, "Test message"));
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(FlightStatusCode::Unavailable, detail->code());

  Status status = flight::transport::grpc::FromGrpcStatus(
      flight::transport::grpc::ToGrpcStatus(Status::NotImplemented("Sentinel")));
  ASSERT_TRUE(status.IsNotImplemented());
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Sentinel"));

  status = flight::transport::grpc::FromGrpcStatus(
      flight::transport::grpc::ToGrpcStatus(Status::Invalid("Sentinel")));
  ASSERT_TRUE(status.IsInvalid());
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Sentinel"));

  status = flight::transport::grpc::FromGrpcStatus(
      flight::transport::grpc::ToGrpcStatus(Status::KeyError("Sentinel")));
  ASSERT_TRUE(status.IsKeyError());
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Sentinel"));

  status = flight::transport::grpc::FromGrpcStatus(
      flight::transport::grpc::ToGrpcStatus(Status::AlreadyExists("Sentinel")));
  ASSERT_TRUE(status.IsAlreadyExists());
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Sentinel"));
}

TEST(FlightTypes, LocationConstruction) {
  ASSERT_RAISES(Invalid, Location::Parse("This is not an URI").status());
  ASSERT_RAISES(Invalid, Location::ForGrpcTcp("This is not a hostname", 12345).status());
  ASSERT_RAISES(Invalid, Location::ForGrpcTls("This is not a hostname", 12345).status());
  ASSERT_RAISES(Invalid, Location::ForGrpcUnix("This is not a filename").status());

  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("s3://test"));
  ASSERT_EQ(location.ToString(), "s3://test");
  ASSERT_OK_AND_ASSIGN(location, Location::ForGrpcTcp("localhost", 12345));
  ASSERT_EQ(location.ToString(), "grpc+tcp://localhost:12345");
  ASSERT_OK_AND_ASSIGN(location, Location::ForGrpcTls("localhost", 12345));
  ASSERT_EQ(location.ToString(), "grpc+tls://localhost:12345");
  ASSERT_OK_AND_ASSIGN(location, Location::ForGrpcUnix("/tmp/test.sock"));
  ASSERT_EQ(location.ToString(), "grpc+unix:///tmp/test.sock");
}

// ----------------------------------------------------------------------
// Cookie authentication/middleware

// This test keeps an internal cookie cache and compares that with the middleware.
class TestCookieMiddleware : public ::testing::Test {
 public:
  // Setup function creates middleware factory and starts it up.
  void SetUp() {
    factory_ = GetCookieFactory();
    CallInfo callInfo;
    factory_->StartCall(callInfo, &middleware_);
  }

  // Function to add incoming cookies to middleware and validate them.
  void AddAndValidate(const std::string& incoming_cookie) {
    // Add cookie
    CallHeaders call_headers;
    call_headers.insert(std::make_pair(std::string_view("set-cookie"),
                                       std::string_view(incoming_cookie)));
    middleware_->ReceivedHeaders(call_headers);
    expected_cookie_cache_.UpdateCachedCookies(call_headers);

    // Get cookie from middleware.
    TestCallHeaders add_call_headers;
    middleware_->SendingHeaders(&add_call_headers);
    const std::string actual_cookies = add_call_headers.GetCookies();

    // Validate cookie
    const std::string expected_cookies = expected_cookie_cache_.GetValidCookiesAsString();
    const std::vector<std::string> split_expected_cookies =
        SplitCookies(expected_cookies);
    const std::vector<std::string> split_actual_cookies = SplitCookies(actual_cookies);
    EXPECT_EQ(split_expected_cookies, split_actual_cookies);
  }

  // Function to take a list of cookies and split them into a vector of individual
  // cookies. This is done because the cookie cache is a map so ordering is not
  // necessarily consistent.
  static std::vector<std::string> SplitCookies(const std::string& cookies) {
    std::vector<std::string> split_cookies;
    std::string::size_type pos1 = 0;
    std::string::size_type pos2 = 0;
    while ((pos2 = cookies.find(';', pos1)) != std::string::npos) {
      split_cookies.push_back(
          arrow::internal::TrimString(cookies.substr(pos1, pos2 - pos1)));
      pos1 = pos2 + 1;
    }
    if (pos1 < cookies.size()) {
      split_cookies.push_back(arrow::internal::TrimString(cookies.substr(pos1)));
    }
    std::sort(split_cookies.begin(), split_cookies.end());
    return split_cookies;
  }

 protected:
  // Class to allow testing of the call headers.
  class TestCallHeaders : public AddCallHeaders {
   public:
    TestCallHeaders() {}
    ~TestCallHeaders() {}

    // Function to add cookie header.
    void AddHeader(const std::string& key, const std::string& value) {
      ASSERT_EQ(key, "cookie");
      outbound_cookie_ = value;
    }

    // Function to get outgoing cookie.
    std::string GetCookies() { return outbound_cookie_; }

   private:
    std::string outbound_cookie_;
  };

  internal::CookieCache expected_cookie_cache_;
  std::unique_ptr<ClientMiddleware> middleware_;
  std::shared_ptr<ClientMiddlewareFactory> factory_;
};

TEST_F(TestCookieMiddleware, BasicParsing) {
  AddAndValidate("id1=1; foo=bar;");
  AddAndValidate("id1=1; foo=bar");
  AddAndValidate("id2=2;");
  AddAndValidate("id4=\"4\"");
  AddAndValidate("id5=5; foo=bar; baz=buz;");
}

TEST_F(TestCookieMiddleware, Overwrite) {
  AddAndValidate("id0=0");
  AddAndValidate("id0=1");
  AddAndValidate("id1=0");
  AddAndValidate("id1=1");
  AddAndValidate("id1=1");
  AddAndValidate("id1=10");
  AddAndValidate("id=3");
  AddAndValidate("id=0");
  AddAndValidate("id=0");
}

TEST_F(TestCookieMiddleware, MaxAge) {
  AddAndValidate("id0=0; max-age=0;");
  AddAndValidate("id1=0; max-age=-1;");
  AddAndValidate("id2=0; max-age=0");
  AddAndValidate("id3=0; max-age=-1");
  AddAndValidate("id4=0; max-age=1");
  AddAndValidate("id5=0; max-age=1");
  AddAndValidate("id4=0; max-age=0");
  AddAndValidate("id5=0; max-age=0");
}

TEST_F(TestCookieMiddleware, Expires) {
  AddAndValidate("id0=0; expires=0, 0 0 0 0:0:0 GMT;");
  AddAndValidate("id0=0; expires=0, 0 0 0 0:0:0 GMT");
  AddAndValidate("id0=0; expires=Fri, 22 Dec 2017 22:15:36 GMT;");
  AddAndValidate("id0=0; expires=Fri, 22 Dec 2017 22:15:36 GMT");
  AddAndValidate("id0=0; expires=Fri, 01 Jan 2038 22:15:36 GMT;");
  AddAndValidate("id1=0; expires=Fri, 01 Jan 2038 22:15:36 GMT");
  AddAndValidate("id0=0; expires=Fri, 22 Dec 2017 22:15:36 GMT;");
  AddAndValidate("id1=0; expires=Fri, 22 Dec 2017 22:15:36 GMT");
}

// This test is used to test the parsing capabilities of the cookie framework.
class TestCookieParsing : public ::testing::Test {
 public:
  void VerifyParseCookie(const std::string& cookie_str, bool expired) {
    internal::Cookie cookie = internal::Cookie::Parse(cookie_str);
    EXPECT_EQ(expired, cookie.IsExpired());
  }

  void VerifyCookieName(const std::string& cookie_str, const std::string& name) {
    internal::Cookie cookie = internal::Cookie::Parse(cookie_str);
    EXPECT_EQ(name, cookie.GetName());
  }

  void VerifyCookieString(const std::string& cookie_str,
                          const std::string& cookie_as_string) {
    internal::Cookie cookie = internal::Cookie::Parse(cookie_str);
    EXPECT_EQ(cookie_as_string, cookie.AsCookieString());
  }

  void VerifyCookieDateConversion(std::string date, const std::string& converted_date) {
    internal::Cookie::ConvertCookieDate(&date);
    EXPECT_EQ(converted_date, date);
  }

  void VerifyCookieAttributeParsing(
      const std::string cookie_str, std::string::size_type start_pos,
      const std::optional<std::pair<std::string, std::string>> cookie_attribute,
      const std::string::size_type start_pos_after) {
    std::optional<std::pair<std::string, std::string>> attr =
        internal::Cookie::ParseCookieAttribute(cookie_str, &start_pos);

    if (cookie_attribute == std::nullopt) {
      EXPECT_EQ(cookie_attribute, attr);
    } else {
      EXPECT_EQ(cookie_attribute.value(), attr.value());
    }
    EXPECT_EQ(start_pos_after, start_pos);
  }

  void AddCookieVerifyCache(const std::vector<std::string>& cookies,
                            const std::string& expected_cookies) {
    internal::CookieCache cookie_cache;
    for (auto& cookie : cookies) {
      // Add cookie
      CallHeaders call_headers;
      call_headers.insert(
          std::make_pair(std::string_view("set-cookie"), std::string_view(cookie)));
      cookie_cache.UpdateCachedCookies(call_headers);
    }
    const std::string actual_cookies = cookie_cache.GetValidCookiesAsString();
    const std::vector<std::string> actual_split_cookies =
        TestCookieMiddleware::SplitCookies(actual_cookies);
    const std::vector<std::string> expected_split_cookies =
        TestCookieMiddleware::SplitCookies(expected_cookies);
  }
};

TEST_F(TestCookieParsing, Expired) {
  VerifyParseCookie("id0=0; expires=Fri, 22 Dec 2017 22:15:36 GMT;", true);
  VerifyParseCookie("id1=0; max-age=-1;", true);
  VerifyParseCookie("id0=0; max-age=0;", true);
}

TEST_F(TestCookieParsing, Invalid) {
  VerifyParseCookie("id1=0; expires=0, 0 0 0 0:0:0 GMT;", true);
  VerifyParseCookie("id1=0; expires=Fri, 01 FOO 2038 22:15:36 GMT", true);
  VerifyParseCookie("id1=0; expires=foo", true);
  VerifyParseCookie("id1=0; expires=", true);
  VerifyParseCookie("id1=0; max-age=FOO", true);
  VerifyParseCookie("id1=0; max-age=", true);
}

TEST_F(TestCookieParsing, NoExpiry) {
  VerifyParseCookie("id1=0;", false);
  VerifyParseCookie("id1=0; noexpiry=Fri, 01 Jan 2038 22:15:36 GMT", false);
  VerifyParseCookie("id1=0; noexpiry=\"Fri, 01 Jan 2038 22:15:36 GMT\"", false);
  VerifyParseCookie("id1=0; nomax-age=-1", false);
  VerifyParseCookie("id1=0; nomax-age=\"-1\"", false);
  VerifyParseCookie("id1=0; randomattr=foo", false);
}

TEST_F(TestCookieParsing, NotExpired) {
  VerifyParseCookie("id5=0; max-age=1", false);
  VerifyParseCookie("id0=0; expires=Fri, 01 Jan 2038 22:15:36 GMT;", false);
}

TEST_F(TestCookieParsing, GetName) {
  VerifyCookieName("id1=1; foo=bar;", "id1");
  VerifyCookieName("id1=1; foo=bar", "id1");
  VerifyCookieName("id2=2;", "id2");
  VerifyCookieName("id4=\"4\"", "id4");
  VerifyCookieName("id5=5; foo=bar; baz=buz;", "id5");
}

TEST_F(TestCookieParsing, ToString) {
  VerifyCookieString("id1=1; foo=bar;", "id1=1");
  VerifyCookieString("id1=1; foo=bar", "id1=1");
  VerifyCookieString("id2=2;", "id2=2");
  VerifyCookieString("id4=\"4\"", "id4=4");
  VerifyCookieString("id5=5; foo=bar; baz=buz;", "id5=5");
}

TEST_F(TestCookieParsing, DateConversion) {
  VerifyCookieDateConversion("Mon, 01 jan 2038 22:15:36 GMT;", "01 01 2038 22:15:36");
  VerifyCookieDateConversion("TUE, 10 Feb 2038 22:15:36 GMT", "10 02 2038 22:15:36");
  VerifyCookieDateConversion("WED, 20 MAr 2038 22:15:36 GMT;", "20 03 2038 22:15:36");
  VerifyCookieDateConversion("thu, 15 APR 2038 22:15:36 GMT", "15 04 2038 22:15:36");
  VerifyCookieDateConversion("Fri, 30 mAY 2038 22:15:36 GMT;", "30 05 2038 22:15:36");
  VerifyCookieDateConversion("Sat, 03 juN 2038 22:15:36 GMT", "03 06 2038 22:15:36");
  VerifyCookieDateConversion("Sun, 01 JuL 2038 22:15:36 GMT;", "01 07 2038 22:15:36");
  VerifyCookieDateConversion("Fri, 06 aUg 2038 22:15:36 GMT", "06 08 2038 22:15:36");
  VerifyCookieDateConversion("Fri, 01 SEP 2038 22:15:36 GMT;", "01 09 2038 22:15:36");
  VerifyCookieDateConversion("Fri, 01 OCT 2038 22:15:36 GMT", "01 10 2038 22:15:36");
  VerifyCookieDateConversion("Fri, 01 Nov 2038 22:15:36 GMT;", "01 11 2038 22:15:36");
  VerifyCookieDateConversion("Fri, 01 deC 2038 22:15:36 GMT", "01 12 2038 22:15:36");
  VerifyCookieDateConversion("", "");
  VerifyCookieDateConversion("Fri, 01 INVALID 2038 22:15:36 GMT;",
                             "01 INVALID 2038 22:15:36");
}

TEST_F(TestCookieParsing, ParseCookieAttribute) {
  VerifyCookieAttributeParsing("", 0, std::nullopt, std::string::npos);

  std::string cookie_string = "attr0=0; attr1=1; attr2=2; attr3=3";
  auto attr_length = std::string("attr0=0;").length();
  std::string::size_type start_pos = 0;
  VerifyCookieAttributeParsing(cookie_string, start_pos, std::make_pair("attr0", "0"),
                               cookie_string.find("attr0=0;") + attr_length);
  VerifyCookieAttributeParsing(cookie_string, (start_pos += (attr_length + 1)),
                               std::make_pair("attr1", "1"),
                               cookie_string.find("attr1=1;") + attr_length);
  VerifyCookieAttributeParsing(cookie_string, (start_pos += (attr_length + 1)),
                               std::make_pair("attr2", "2"),
                               cookie_string.find("attr2=2;") + attr_length);
  VerifyCookieAttributeParsing(cookie_string, (start_pos += (attr_length + 1)),
                               std::make_pair("attr3", "3"), std::string::npos);
  VerifyCookieAttributeParsing(cookie_string, (start_pos += (attr_length - 1)),
                               std::nullopt, std::string::npos);
  VerifyCookieAttributeParsing(cookie_string, std::string::npos, std::nullopt,
                               std::string::npos);
}

TEST_F(TestCookieParsing, CookieCache) {
  AddCookieVerifyCache({"id0=0;"}, "");
  AddCookieVerifyCache({"id0=0;", "id0=1;"}, "id0=1");
  AddCookieVerifyCache({"id0=0;", "id1=1;"}, "id0=0; id1=1");
  AddCookieVerifyCache({"id0=0;", "id1=1;", "id2=2"}, "id0=0; id1=1; id2=2");
}

// ----------------------------------------------------------------------
// Protobuf tests

TEST(GrpcTransport, FlightDataDeserialize) {
#ifndef _WIN32
  pb::FlightData raw;
  // Tack on known and unknown fields by hand here
  raw.GetReflection()->MutableUnknownFields(&raw)->AddFixed32(900, 1024);
  raw.GetReflection()->MutableUnknownFields(&raw)->AddFixed64(901, 1024);
  raw.GetReflection()->MutableUnknownFields(&raw)->AddVarint(902, 1024);
  raw.GetReflection()->MutableUnknownFields(&raw)->AddLengthDelimited(903, "foobar");
  // Known field comes at end
  raw.GetReflection()->MutableUnknownFields(&raw)->AddLengthDelimited(
      pb::FlightData::kDataBodyFieldNumber, "data");

  auto serialized = raw.SerializeAsString();

  grpc_slice slice = grpc_slice_from_copied_buffer(serialized.data(), serialized.size());
  // gRPC requires that grpc_slice and grpc::Slice have the same representation
  grpc::ByteBuffer buffer(reinterpret_cast<const grpc::Slice*>(&slice), /*nslices=*/1);

  flight::internal::FlightData out;
  auto status = flight::transport::grpc::FlightDataDeserialize(&buffer, &out);
  ASSERT_TRUE(status.ok());
  ASSERT_EQ("data", out.body->ToString());

  grpc_slice_unref(slice);
#else
  GTEST_SKIP() << "Can't use Protobuf symbols on Windows";
#endif
}

// ----------------------------------------------------------------------
// Transport-agnostic serialization roundtrip tests

TEST(FlightSerialization, RoundtripPayloadWithBody) {
  // Use RecordBatchStream to generate FlightPayloads
  auto schema = arrow::schema({arrow::field("a", arrow::int32())});
  auto arr = ArrayFromJSON(arrow::int32(), "[1, 2, 3]");
  auto batch = RecordBatch::Make(schema, 3, {arr});
  auto reader = RecordBatchReader::Make({batch}).ValueOrDie();
  RecordBatchStream stream(std::move(reader));

  // Get a FlightPayload from the stream
  ASSERT_OK_AND_ASSIGN(auto schema_payload, stream.GetSchemaPayload());
  ASSERT_OK_AND_ASSIGN(auto flight_payload, stream.Next());

  // Add app_metadata to the flight payload
  flight_payload.app_metadata = Buffer::FromString("test-metadata");

  // Serialize FlightPayload to BufferVector
  ASSERT_OK_AND_ASSIGN(auto buffers, internal::SerializePayloadToBuffers(flight_payload));
  ASSERT_GT(buffers.size(), 0);

  // Concatenate to a single buffer for deserialization and deserialize.
  ASSERT_OK_AND_ASSIGN(auto concat, ConcatenateBuffers(buffers));
  ASSERT_OK_AND_ASSIGN(auto data, internal::DeserializeFlightData(concat));

  // Verify IPC metadata (data_header) is present
  ASSERT_NE(data.metadata, nullptr);
  ASSERT_GT(data.metadata->size(), 0);

  // Verify app_metadata
  ASSERT_NE(data.app_metadata, nullptr);
  ASSERT_EQ(data.app_metadata->ToString(), "test-metadata");

  // Verify body and message are present
  ASSERT_NE(data.body, nullptr);
  ASSERT_GT(data.body->size(), 0);
  ASSERT_OK_AND_ASSIGN(auto message, data.OpenMessage());
  ASSERT_NE(message, nullptr);
  // Also verify the RecordBatch roundtrips correctly
  ipc::DictionaryMemo dict_memo;
  ASSERT_OK_AND_ASSIGN(auto result_batch,
                       ipc::ReadRecordBatch(*message, schema, &dict_memo,
                                            ipc::IpcReadOptions::Defaults()));
  ASSERT_TRUE(result_batch->Equals(*batch));
}

TEST(FlightSerialization, RoundtripMetadataOnly) {
  // A metadata-only payload (no IPC body, no descriptor)
  auto app_meta = Buffer::FromString("metadata-only-message");

  FlightPayload payload;
  payload.app_metadata = std::move(app_meta);

  // Serialize
  ASSERT_OK_AND_ASSIGN(auto buffers, internal::SerializePayloadToBuffers(payload));
  ASSERT_OK_AND_ASSIGN(auto concat, ConcatenateBuffers(buffers));

  // Deserialize
  ASSERT_OK_AND_ASSIGN(auto data, internal::DeserializeFlightData(concat));

  // Verify: no descriptor, no IPC metadata, just app_metadata
  ASSERT_EQ(data.descriptor, nullptr);
  ASSERT_EQ(data.metadata, nullptr);
  ASSERT_NE(data.app_metadata, nullptr);
  ASSERT_EQ(data.app_metadata->ToString(), "metadata-only-message");
}

// ----------------------------------------------------------------------
// Push-style decoder (AsyncFlightMessageDecoder) tests

namespace {

// Serialize one FlightPayload into the single FlightData message a transport
// hands to AsyncFlightMessageDecoder::Consume, deserialized as it would be.
::arrow::Result<internal::FlightData> DecoderTestWireBuffer(
    const FlightPayload& payload) {
  ARROW_ASSIGN_OR_RAISE(auto buffers, internal::SerializePayloadToBuffers(payload));
  ARROW_ASSIGN_OR_RAISE(auto buffer, ConcatenateBuffers(buffers));
  return internal::DeserializeFlightData(buffer);
}

// Captures the events AsyncFlightMessageDecoder fires, in order, plus the
// per-callback results a test can configure: a rejecting schema, descriptor or
// OnNext, and an OnNext the test resolves by hand (the backpressure case).
class DecoderTestListener : public AsyncFlightDataListener {
 public:
  Future<> OnDescriptor(const FlightDescriptor& descriptor) override {
    events.emplace_back("OnDescriptor");
    descriptors.push_back(descriptor);
    return descriptor_result_;
  }

  Future<> OnNext(FlightStreamChunk chunk) override {
    events.emplace_back("OnNext");
    chunks.push_back(std::move(chunk));
    return next_result_;
  }

  // The decoder calls the single-argument overload.
  Status OnSchemaDecoded(std::shared_ptr<Schema> schema) override {
    events.emplace_back("OnSchemaDecoded");
    schemas.push_back(std::move(schema));
    return schema_status_;
  }

  void set_schema_status(Status status) { schema_status_ = std::move(status); }
  void set_next_status(Status status) { next_result_ = std::move(status); }
  void set_descriptor_status(Status status) { descriptor_result_ = std::move(status); }
  /// Park OnNext(): complete `gate` to release the decoder's continuation.
  void park_next(Future<> gate) { next_result_ = std::move(gate); }

  std::vector<std::string> events;
  std::vector<std::shared_ptr<Schema>> schemas;
  std::vector<FlightStreamChunk> chunks;
  std::vector<FlightDescriptor> descriptors;

 private:
  Status schema_status_;
  Future<> next_result_ = Future<>::MakeFinished();
  Future<> descriptor_result_ = Future<>::MakeFinished();
};

/// The decoder under test, with the wiring every case needs: one wire message
/// per FlightPayload, and the listener the callbacks were recorded on.
class TestAsyncFlightMessageDecoder : public ::testing::Test {
 protected:
  void SetUp() override {
    listener_ = std::make_shared<DecoderTestListener>();
    decoder_ = std::make_unique<AsyncFlightMessageDecoder>(listener_);
  }

  /// Feed one payload as the transport would, and return the real Consume
  /// future: a backpressure case must be able to see it unfinished.
  Future<> ConsumePayload(const FlightPayload& payload) {
    auto wire = DecoderTestWireBuffer(payload);
    if (!wire.ok()) {
      return wire.status();
    }
    return decoder_->Consume(std::move(*wire));
  }

  Future<> ConsumeData(internal::FlightData data) {
    return decoder_->Consume(std::move(data));
  }

  std::shared_ptr<DecoderTestListener> listener_;
  std::unique_ptr<AsyncFlightMessageDecoder> decoder_;
};

// A RecordBatchReader whose payloads the test pulls one at a time, in order.
class DecoderStream {
 public:
  explicit DecoderStream(std::shared_ptr<Schema> schema,
                         std::vector<std::shared_ptr<RecordBatch>> batches,
                         ipc::IpcWriteOptions options = ipc::IpcWriteOptions::Defaults())
      : stream_(arrow::RecordBatchReader::Make(std::move(batches), std::move(schema))
                    .ValueOrDie(),
                options) {}

  FlightPayload SchemaMessage() { return Get(stream_.GetSchemaPayload()); }
  /// The next payload, with a null ipc_message.metadata at end of stream.
  FlightPayload Next() { return Get(stream_.Next()); }

 private:
  static FlightPayload Get(::arrow::Result<FlightPayload> payload) {
    ARROW_EXPECT_OK(payload);
    return std::move(payload).ValueOrDie();
  }
  RecordBatchStream stream_;
};

bool IsEndOfStream(const FlightPayload& payload) {
  return payload.ipc_message.metadata == nullptr;
}

std::shared_ptr<Schema> DecoderTestSchema() {
  return arrow::schema(
      {arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32())});
}

std::shared_ptr<RecordBatch> IntBatch(const std::shared_ptr<Schema>& schema,
                                      const std::string& a, const std::string& b) {
  return RecordBatch::Make(
      schema, 2, {ArrayFromJSON(arrow::int32(), a), ArrayFromJSON(arrow::int32(), b)});
}

}  // namespace

TEST_F(TestAsyncFlightMessageDecoder, SchemaThenBatchesInOrder) {
  // One payload per wire message: schema, then one per record batch. Only the
  // schema event precedes the data, and the batches arrive in order with no
  // metadata invented for them.
  auto schema = DecoderTestSchema();
  auto batch1 = IntBatch(schema, "[1, 2]", "[3, 4]");
  auto batch2 = IntBatch(schema, "[5, 6]", "[7, 8]");
  DecoderStream stream(schema, {batch1, batch2});

  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"));
  ASSERT_EQ(listener_->schemas.size(), 1);
  EXPECT_TRUE(listener_->schemas[0]->Equals(*schema));

  ASSERT_OK(ConsumePayload(stream.Next()).status());
  ASSERT_OK(ConsumePayload(stream.Next()).status());
  // The stream's end carries no message at all, so the decoder saw exactly the
  // schema and the two batches.
  EXPECT_TRUE(IsEndOfStream(stream.Next()));

  EXPECT_THAT(listener_->events,
              ::testing::ElementsAre("OnSchemaDecoded", "OnNext", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 2);
  ASSERT_NE(listener_->chunks[0].data, nullptr);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch1));
  EXPECT_EQ(listener_->chunks[0].app_metadata, nullptr);
  ASSERT_NE(listener_->chunks[1].data, nullptr);
  EXPECT_TRUE(listener_->chunks[1].data->Equals(*batch2));
  EXPECT_EQ(listener_->chunks[1].app_metadata, nullptr);
}

TEST_F(TestAsyncFlightMessageDecoder, SchemaOnlyPrefix) {
  // A schema message alone is a complete stream prefix: the schema event and
  // nothing else, no end-of-stream chunk invented for it.
  auto schema = DecoderTestSchema();
  DecoderStream stream(schema, {});
  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"));
  EXPECT_TRUE(listener_->chunks.empty());
  ASSERT_EQ(listener_->schemas.size(), 1);
  EXPECT_TRUE(listener_->schemas[0]->Equals(*schema));
}

TEST_F(TestAsyncFlightMessageDecoder, MetadataPositions) {
  // App metadata travels with its own wire message and reaches the listener
  // exactly once for it: on the standalone message, on the schema message and on
  // the batch message, without leaking into the next batch.
  auto schema = DecoderTestSchema();
  auto batch1 = IntBatch(schema, "[1, 2]", "[3, 4]");
  auto batch2 = IntBatch(schema, "[5, 6]", "[7, 8]");
  DecoderStream stream(schema, {batch1, batch2});

  FlightPayload standalone;
  standalone.app_metadata = Buffer::FromString("standalone");
  ASSERT_OK(ConsumePayload(standalone).status());
  ASSERT_EQ(listener_->chunks.size(), 1u);
  EXPECT_EQ(listener_->chunks[0].data, nullptr);
  ASSERT_NE(listener_->chunks[0].app_metadata, nullptr);
  EXPECT_EQ("standalone", listener_->chunks[0].app_metadata->ToString());

  auto schema_payload = stream.SchemaMessage();
  schema_payload.app_metadata = Buffer::FromString("on-schema");
  ASSERT_OK(ConsumePayload(schema_payload).status());
  ASSERT_EQ(listener_->chunks.size(), 2u);
  EXPECT_EQ(listener_->chunks[1].data, nullptr);
  ASSERT_NE(listener_->chunks[1].app_metadata, nullptr);
  EXPECT_EQ("on-schema", listener_->chunks[1].app_metadata->ToString());

  auto first = stream.Next();
  first.app_metadata = Buffer::FromString("on-batch");
  ASSERT_OK(ConsumePayload(first).status());
  ASSERT_EQ(listener_->chunks.size(), 3u);
  ASSERT_NE(listener_->chunks[2].data, nullptr);
  EXPECT_TRUE(listener_->chunks[2].data->Equals(*batch1));
  ASSERT_NE(listener_->chunks[2].app_metadata, nullptr);
  EXPECT_EQ("on-batch", listener_->chunks[2].app_metadata->ToString());

  // The next batch carries no metadata: nothing leaks from the previous one.
  ASSERT_OK(ConsumePayload(stream.Next()).status());
  ASSERT_EQ(listener_->chunks.size(), 4u);
  EXPECT_TRUE(listener_->chunks[3].data->Equals(*batch2));
  EXPECT_EQ(nullptr, listener_->chunks[3].app_metadata);
}

TEST_F(TestAsyncFlightMessageDecoder, EmptyValues) {
  // Empty app metadata is no event at all, while a zero-row record batch is a
  // real batch and not the end of the stream.
  {
    FlightPayload empty;
    empty.app_metadata = Buffer ::FromString("");
    ASSERT_OK(ConsumePayload(empty).status());
    EXPECT_TRUE(listener_->events.empty()) << "empty metadata must produce no event";
    EXPECT_TRUE(listener_->chunks.empty());
  }
  {
    // Directly: wire serialization omits empty metadata, so this needs the
    // in-memory message.
    internal::FlightData data;
    data.app_metadata = Buffer::FromString("");
    ASSERT_OK(ConsumeData(std::move(data)).status());
    EXPECT_TRUE(listener_->events.empty());
    EXPECT_TRUE(listener_->chunks.empty());
  }
  {
    auto schema = DecoderTestSchema();
    auto empty_batch = RecordBatch::Make(
        schema, 0,
        {ArrayFromJSON(arrow::int32(), "[]"), ArrayFromJSON(arrow::int32(), "[]")});
    DecoderStream stream(schema, {empty_batch});
    ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    auto payload = stream.Next();
    ASSERT_FALSE(IsEndOfStream(payload)) << "a zero-row batch is still a batch";
    ASSERT_OK(ConsumePayload(payload).status());
    EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
    ASSERT_EQ(listener_->chunks.size(), 1u);
    ASSERT_NE(listener_->chunks[0].data, nullptr);
    EXPECT_EQ(0, listener_->chunks[0].data->num_rows());
  }
}

TEST_F(TestAsyncFlightMessageDecoder, DescriptorReportedOnce) {
  // The first message carries the upload's descriptor. A later message carrying
  // a different one must not replace it, and must still deliver its data.
  auto schema = DecoderTestSchema();
  auto batch = IntBatch(schema, "[1, 2]", "[3, 4]");
  DecoderStream stream(schema, {batch});

  const auto original = FlightDescriptor::Path({"wave", "h1"});
  auto schema_payload = stream.SchemaMessage();
  ASSERT_OK(internal::ToPayload(original, &schema_payload.descriptor));
  ASSERT_OK(ConsumePayload(schema_payload).status());
  EXPECT_THAT(listener_->events,
              ::testing::ElementsAre("OnDescriptor", "OnSchemaDecoded"));
  ASSERT_EQ(listener_->descriptors.size(), 1u);
  EXPECT_EQ(original, listener_->descriptors[0]);

  auto batch_payload = stream.Next();
  ASSERT_OK(
      internal::ToPayload(FlightDescriptor::Path({"other"}), &batch_payload.descriptor));
  ASSERT_OK(ConsumePayload(batch_payload).status());
  ASSERT_EQ(listener_->descriptors.size(), 1u)
      << "a later descriptor must be ignored like the sync transport does";
  EXPECT_EQ(original, listener_->descriptors[0]);
  EXPECT_THAT(listener_->events,
              ::testing::ElementsAre("OnDescriptor", "OnSchemaDecoded", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 1u);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
}

TEST_F(TestAsyncFlightMessageDecoder, DescriptorDeferral) {
  // A pending OnDescriptor() holds everything from its own message: success
  // delivers the rest in order, a failure suppresses all of it.
  for (const bool accepted : {true, false}) {
    SCOPED_TRACE(accepted ? "descriptor accepted" : "descriptor rejected");
    SetUp();
    auto schema = DecoderTestSchema();
    DecoderStream stream(schema, {});
    listener_->set_descriptor_status(accepted ? Status::OK()
                                              : Status::Invalid("descriptor rejected"));
    auto payload = stream.SchemaMessage();
    payload.app_metadata = Buffer::FromString("after-descriptor");
    ASSERT_OK(internal::ToPayload(FlightDescriptor::Path({"p"}), &payload.descriptor));
    const auto consumed = ConsumePayload(payload);
    if (accepted) {
      ASSERT_OK(consumed.status());
      // In order, and the schema's metadata arrives after the schema event.
      EXPECT_THAT(listener_->events,
                  ::testing::ElementsAre("OnDescriptor", "OnSchemaDecoded", "OnNext"));
    } else {
      ASSERT_FALSE(consumed.status().ok());
      EXPECT_THAT(consumed.status().ToString(),
                  ::testing::HasSubstr("descriptor rejected"));
      EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnDescriptor"))
          << "a rejected descriptor must suppress the rest of its message";
    }
  }
}

TEST_F(TestAsyncFlightMessageDecoder, DescriptorDeferralHoldsConsume) {
  // The same deferral with a genuinely pending future: Consume() stays
  // unfinished, and nothing of the message is delivered before it resolves.
  class ParkingDescriptorListener final : public DecoderTestListener {
   public:
    Future<> OnDescriptor(const FlightDescriptor& descriptor) override {
      events.emplace_back("OnDescriptor");
      descriptors.push_back(descriptor);
      return gate;
    }
    Future<> gate = Future<>::Make();
  };
  listener_ = std::make_shared<ParkingDescriptorListener>();
  decoder_ = std::make_unique<AsyncFlightMessageDecoder>(listener_);
  auto* parking = static_cast<ParkingDescriptorListener*>(listener_.get());

  auto schema = DecoderTestSchema();
  DecoderStream stream(schema, {});
  auto payload = stream.SchemaMessage();
  ASSERT_OK(internal::ToPayload(FlightDescriptor::Path({"p"}), &payload.descriptor));
  auto wire = DecoderTestWireBuffer(payload).ValueOrDie();
  const auto consumed = ConsumeData(std::move(wire));
  EXPECT_FALSE(consumed.is_finished()) << "the descriptor future is still pending";
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnDescriptor"));
  EXPECT_TRUE(listener_->schemas.empty());
  EXPECT_TRUE(listener_->chunks.empty());

  parking->gate.MarkFinished(Status::OK());
  ASSERT_OK(consumed.status());
  EXPECT_THAT(listener_->events,
              ::testing::ElementsAre("OnDescriptor", "OnSchemaDecoded"));
}

TEST_F(TestAsyncFlightMessageDecoder, ListenerFailures) {
  // A rejecting callback stops its own message there: the same status comes back
  // to Consume(), and no later callback of that message runs.
  auto schema_with_batch = [] {
    auto schema = DecoderTestSchema();
    return std::make_pair(schema, IntBatch(schema, "[1, 2]", "[3, 4]"));
  };

  {
    SCOPED_TRACE("schema rejected");
    SetUp();
    listener_->set_schema_status(Status::Invalid("schema rejected"));
    auto [schema, batch] = schema_with_batch();
    DecoderStream stream(schema, {batch});
    const auto status = ConsumePayload(stream.SchemaMessage()).status();
    ASSERT_FALSE(status.ok());
    EXPECT_THAT(status.ToString(), ::testing::HasSubstr("schema rejected"));
    EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"))
        << "no data may follow a rejected schema";
  }
  {
    SCOPED_TRACE("batch rejected");
    SetUp();
    listener_->set_next_status(Status::Invalid("batch rejected"));
    auto [schema, batch] = schema_with_batch();
    DecoderStream stream(schema, {batch});
    ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    const auto status = ConsumePayload(stream.Next()).status();
    ASSERT_FALSE(status.ok());
    EXPECT_THAT(status.ToString(), ::testing::HasSubstr("batch rejected"));
    EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  }
  {
    SCOPED_TRACE("metadata rejected");
    SetUp();
    listener_->set_next_status(Status::Invalid("metadata rejected"));
    FlightPayload metadata;
    metadata.app_metadata = Buffer::FromString("meta");
    const auto status = ConsumePayload(metadata).status();
    ASSERT_FALSE(status.ok());
    EXPECT_THAT(status.ToString(), ::testing::HasSubstr("metadata rejected"));
  }
  {
    SCOPED_TRACE("schema metadata rejected");
    SetUp();
    listener_->set_next_status(Status::Invalid("schema metadata rejected"));
    auto [schema, batch] = schema_with_batch();
    DecoderStream stream(schema, {batch});
    auto payload = stream.SchemaMessage();
    payload.app_metadata = Buffer::FromString("schema-meta");
    const auto status = ConsumePayload(payload).status();
    ASSERT_FALSE(status.ok());
    EXPECT_THAT(status.ToString(), ::testing::HasSubstr("schema metadata rejected"));
    EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  }
}

TEST_F(TestAsyncFlightMessageDecoder, BackpressureHoldsConsume) {
  // A pending OnNext() future keeps Consume() unfinished: that is how the
  // transport stops reading while the application works.
  auto schema = DecoderTestSchema();
  auto batch = IntBatch(schema, "[1, 2]", "[3, 4]");
  DecoderStream stream(schema, {batch});
  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());

  auto gate = Future<>::Make();
  listener_->park_next(gate);
  const auto consumed = ConsumePayload(stream.Next());
  EXPECT_FALSE(consumed.is_finished()) << "Consume() must wait for OnNext()'s future";

  gate.MarkFinished(Status::OK());
  ASSERT_OK(consumed.status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 1u);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
}

TEST_F(TestAsyncFlightMessageDecoder, StopDropsEverything) {
  // Once stopped no listener callback runs, and Consume() succeeds on the message
  // it had already accepted: the upload ended, the message did not fail.
  for (const bool before_schema : {true, false}) {
    SCOPED_TRACE(before_schema ? "stopped before the schema" : "stopped before metadata");
    SetUp();
    auto schema = DecoderTestSchema();
    DecoderStream stream(schema, {});
    decoder_->Stop();
    if (before_schema) {
      ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    } else {
      FlightPayload metadata;
      metadata.app_metadata = Buffer::FromString("meta");
      ASSERT_OK(ConsumePayload(metadata).status());
    }
    EXPECT_TRUE(listener_->events.empty());
    EXPECT_TRUE(listener_->chunks.empty());
  }
}

TEST_F(TestAsyncFlightMessageDecoder, StopWithDescriptorPending) {
  // A descriptor pending when Stop() arrives: its continuation finds the decoder
  // stopped, delivers nothing, and Consume() resolves without waiting for the
  // abandoned message.
  class ParkingDescriptorListener final : public DecoderTestListener {
   public:
    Future<> OnDescriptor(const FlightDescriptor& descriptor) override {
      events.emplace_back("OnDescriptor");
      descriptors.push_back(descriptor);
      return gate;
    }
    Future<> gate = Future<>::Make();
  };
  listener_ = std::make_shared<ParkingDescriptorListener>();
  decoder_ = std::make_unique<AsyncFlightMessageDecoder>(listener_);
  auto* parking = static_cast<ParkingDescriptorListener*>(listener_.get());

  auto schema = DecoderTestSchema();
  DecoderStream stream(schema, {});
  auto payload = stream.SchemaMessage();
  ASSERT_OK(internal::ToPayload(FlightDescriptor::Path({"p"}), &payload.descriptor));
  auto wire = DecoderTestWireBuffer(payload).ValueOrDie();
  const auto consumed = ConsumeData(std::move(wire));
  EXPECT_FALSE(consumed.is_finished()) << "the descriptor future is still pending";

  decoder_->Stop();
  parking->gate.MarkFinished(Status::OK());
  ASSERT_OK(consumed.status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnDescriptor"))
      << "nothing of the abandoned message may be delivered after Stop()";
  EXPECT_TRUE(listener_->schemas.empty());
  EXPECT_TRUE(listener_->chunks.empty());
}

TEST_F(TestAsyncFlightMessageDecoder, SchemaMetadataAfterStop) {
  // The same case driven through the public Stop(): a schema message whose
  // metadata was already accepted is dropped once the decoder stopped, with no
  // follow-up chunk.
  auto schema = DecoderTestSchema();
  DecoderStream stream(schema, {});
  auto payload = stream.SchemaMessage();
  payload.app_metadata = Buffer::FromString("schema-meta");
  auto wire = DecoderTestWireBuffer(payload).ValueOrDie();
  // Deliver the schema callback synchronously, then stop, then complete.
  class StoppingListener final : public DecoderTestListener {
   public:
    Status OnSchemaDecoded(std::shared_ptr<Schema> schema) override {
      auto status = DecoderTestListener::OnSchemaDecoded(std::move(schema));
      decoder->Stop();
      return status;
    }
    AsyncFlightMessageDecoder* decoder = nullptr;
  };
  auto stopping = std::make_shared<StoppingListener>();
  auto decoder = std::make_unique<AsyncFlightMessageDecoder>(stopping);
  stopping->decoder = decoder.get();
  ASSERT_OK(decoder->Consume(std::move(wire)).status());
  EXPECT_THAT(stopping->events, ::testing::ElementsAre("OnSchemaDecoded"));
  EXPECT_TRUE(stopping->chunks.empty()) << "no metadata chunk may follow Stop()";
}

TEST_F(TestAsyncFlightMessageDecoder, BadIpcHeaderIsRejected) {
  // FlightData whose metadata is not an IPC message: a non-OK Consume() with no
  // decoded callback, and the decoder stays usable for a good stream.
  auto schema = DecoderTestSchema();
  auto batch = IntBatch(schema, "[1, 2]", "[3, 4]");
  DecoderStream stream(schema, {batch});

  internal::FlightData bad;
  bad.metadata = Buffer::FromString("truncated");
  const auto failed = ConsumeData(std::move(bad)).status();
  ASSERT_FALSE(failed.ok()) << "a truncated IPC header must be rejected";
  EXPECT_TRUE(listener_->events.empty());

  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  ASSERT_OK(ConsumePayload(stream.Next()).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 1u);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
}

TEST_F(TestAsyncFlightMessageDecoder, RecordBatchBeforeSchemaIsRejected) {
  // A first IPC message that is not a schema: rejected, nothing surfaced, and the
  // decoder survives to open a good stream (the ownership regression: Open() must
  // not free the decoder's reader).
  auto schema = DecoderTestSchema();
  auto batch = IntBatch(schema, "[1, 2]", "[3, 4]");
  DecoderStream bad_stream(schema, {batch});
  auto first = bad_stream.Next();
  ASSERT_FALSE(IsEndOfStream(first));
  const auto failed = ConsumePayload(first).status();
  ASSERT_FALSE(failed.ok()) << "a record batch as the first message must be rejected";
  EXPECT_TRUE(listener_->events.empty())
      << "nothing may be surfaced for a rejected stream";

  DecoderStream good_stream(schema, {batch});
  ASSERT_OK(ConsumePayload(good_stream.SchemaMessage()).status());
  ASSERT_OK(ConsumePayload(good_stream.Next()).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 1u);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
}

TEST_F(TestAsyncFlightMessageDecoder, BadLaterIpcIsRejected) {
  // After a valid schema: a second schema message, and separately a valid batch
  // header with a truncated body. Both must be errors, not a silent end.
  auto schema = DecoderTestSchema();
  auto batch = IntBatch(schema, "[1, 2]", "[3, 4]");
  {
    SCOPED_TRACE("second schema message");
    DecoderStream stream(schema, {batch});
    ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    // A stream has exactly one schema message; build a second one directly, since
    // a RecordBatchStream only ever produces it once.
    const auto options = ipc::IpcWriteOptions::Defaults();
    ipc::DictionaryFieldMapper mapper;
    FlightPayload another;
    ASSERT_OK(ipc::GetSchemaPayload(*schema, options, mapper, &another.ipc_message));
    const auto status = ConsumePayload(another).status();
    EXPECT_FALSE(status.ok()) << "a second schema message must be rejected";
    EXPECT_TRUE(listener_->chunks.empty()) << "no batch may be decoded after it";
  }
  {
    SCOPED_TRACE("truncated batch body");
    SetUp();
    DecoderStream stream(schema, {batch});
    ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    auto payload = stream.Next();
    ASSERT_FALSE(IsEndOfStream(payload));
    internal::FlightData truncated;
    truncated.metadata = payload.ipc_message.metadata;
    const auto status = ConsumeData(std::move(truncated)).status();
    EXPECT_FALSE(status.ok()) << "a truncated batch body must be rejected";
    EXPECT_TRUE(listener_->chunks.empty()) << "no invalid batch may be delivered";
    EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
  }
}

TEST_F(TestAsyncFlightMessageDecoder, TwoIndependentDictionaries) {
  // Regression: a schema with two independent dictionary fields, each dictionary
  // delivered alone before the batch. The pull reader treated the missing second
  // dictionary as end of stream and rejected valid incremental input.
  const auto t0 = dictionary(arrow::int32(), utf8());
  const auto t1 = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("d0", t0), arrow::field("d1", t1)});
  const auto batch = RecordBatch::Make(
      schema, 3,
      {ArrayFromJSON(t0, R"(["a", "b", "a"])"), ArrayFromJSON(t1, R"(["x", "y", "x"])")});
  DecoderStream stream(schema, {batch});

  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"));

  auto dict0 = stream.Next();
  ASSERT_FALSE(IsEndOfStream(dict0));
  ASSERT_OK(ConsumePayload(dict0).status())
      << "the first initial dictionary must not be reported as end of stream";
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"));

  auto dict1 = stream.Next();
  ASSERT_FALSE(IsEndOfStream(dict1));
  ASSERT_OK(ConsumePayload(dict1).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"));

  ASSERT_OK(ConsumePayload(stream.Next()).status());
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 1u);
  ASSERT_NE(listener_->chunks[0].data, nullptr);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
  EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
}

TEST_F(TestAsyncFlightMessageDecoder, DictionaryUpdates) {
  // Replacement and delta dictionaries: every batch decodes to its original, and
  // no dictionary surfaces as a chunk of its own.
  const auto type = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("d", type)});
  const auto first =
      RecordBatch::Make(schema, 3, {ArrayFromJSON(type, R"(["a", "b", "a"])")});
  const auto second =
      RecordBatch::Make(schema, 3, {ArrayFromJSON(type, R"(["c", "d", "c"])")});
  const auto options = ipc::IpcWriteOptions::Defaults();
  DecoderStream stream(schema, {first, second});

  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  bool saw_first = false;
  while (!saw_first) {
    auto payload = stream.Next();
    ASSERT_FALSE(IsEndOfStream(payload));
    ASSERT_OK(ConsumePayload(payload).status());
    for (const auto& chunk : listener_->chunks) {
      ASSERT_NE(nullptr, chunk.data) << "a dictionary surfaced as a chunk";
    }
    if (!listener_->chunks.empty()) saw_first = true;
  }
  EXPECT_TRUE(listener_->chunks.back().data->Equals(*first))
      << "the first batch must decode against its dictionary";
  ARROW_UNUSED(options);
}

TEST_F(TestAsyncFlightMessageDecoder, FinishValidatesTheStreamEnd) {
  // Finish() separates a stream that ended where it could from one that stopped
  // in the middle of its initial dictionaries, and invokes no callback.
  EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();  // no IPC content at all

  {
    SCOPED_TRACE("metadata-only stream");
    SetUp();
    FlightPayload metadata;
    metadata.app_metadata = Buffer::FromString("meta");
    ASSERT_OK(ConsumePayload(metadata).status());
    EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
  }
  {
    SCOPED_TRACE("schema-only stream");
    SetUp();
    DecoderStream stream(DecoderTestSchema(), {});
    ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
    EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
  }
  {
    SCOPED_TRACE("partial initial dictionaries");
    SetUp();
    const auto t0 = dictionary(arrow::int32(), utf8());
    const auto t1 = dictionary(arrow::int32(), utf8());
    const auto dict_schema =
        arrow::schema({arrow::field("d0", t0), arrow::field("d1", t1)});
    const auto batch = RecordBatch::Make(
        dict_schema, 1, {ArrayFromJSON(t0, R"(["a"])"), ArrayFromJSON(t1, R"(["x"])")});
    DecoderStream stream(dict_schema, {batch});
    ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
    ASSERT_OK(ConsumePayload(stream.Next()).status());  // dictionary #1 alone
    const auto status = decoder_->Finish();
    ASSERT_FALSE(status.ok()) << "a partial initial-dictionary set must be rejected";
    EXPECT_THAT(status.ToString(), ::testing::HasSubstr("expected number (2)"));
    EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded"))
        << "Finish() must invoke no application callback";
    ASSERT_OK(ConsumePayload(stream.Next()).status());  // the complete set
    EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
  }
  {
    SCOPED_TRACE("stopped decoder");
    SetUp();
    decoder_->Stop();
    EXPECT_TRUE(decoder_->Finish().ok()) << decoder_->Finish();
  }
}

TEST_F(TestAsyncFlightMessageDecoder, IncludedFieldsProjection) {
  // IpcReadOptions.included_fields projects the decoded batch and the schema the
  // listener sees, without losing the schema event or the excluded fields'
  // dictionaries.
  const auto t0 = dictionary(arrow::int32(), utf8());
  const auto t1 = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("keep", t0), arrow::field("drop", t1)});
  const auto batch = RecordBatch::Make(
      schema, 2,
      {ArrayFromJSON(t0, R"(["a", "b"])"), ArrayFromJSON(t1, R"(["x", "y"])")});

  ipc::IpcReadOptions options = ipc::IpcReadOptions::Defaults();
  options.included_fields = {0};
  listener_ = std::make_shared<DecoderTestListener>();
  decoder_ = std::make_unique<AsyncFlightMessageDecoder>(listener_, options);

  DecoderStream stream(schema, {batch});
  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  ASSERT_EQ(listener_->schemas.size(), 1u);
  EXPECT_EQ(1, listener_->schemas[0]->num_fields())
      << "the reported schema must be the projected one";
  EXPECT_EQ("keep", listener_->schemas[0]->field(0)->name());

  while (true) {
    auto payload = stream.Next();
    if (IsEndOfStream(payload)) break;
    ASSERT_OK(ConsumePayload(payload).status());
  }
  ASSERT_EQ(listener_->chunks.size(), 1u);
  ASSERT_NE(listener_->chunks[0].data, nullptr);
  EXPECT_EQ(1, listener_->chunks[0].data->num_columns());
  ASSERT_OK_AND_ASSIGN(auto expected, batch->SelectColumns({0}));
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*expected));
}

TEST_F(TestAsyncFlightMessageDecoder, NestedDictionaryIds) {
  // A dictionary nested inside a struct field is one initial dictionary with its
  // own path; the batch must still decode, and the dictionary must not surface as
  // a chunk.
  const auto type = dictionary(arrow::int32(), utf8());
  const auto struct_type = struct_({arrow::field("inner", type)});
  const auto schema = arrow::schema({arrow::field("s", struct_type)});
  auto values = ArrayFromJSON(type, R"(["a", "b"])");
  ASSERT_OK_AND_ASSIGN(auto nested,
                       StructArray::Make({values}, {arrow::field("inner", type)}));
  auto batch = RecordBatch::Make(schema, 2, {nested});

  DecoderStream stream(schema, {batch});
  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  while (listener_->chunks.empty()) {
    auto payload = stream.Next();
    ASSERT_FALSE(IsEndOfStream(payload)) << "the nested dictionary stream ended early";
    ASSERT_OK(ConsumePayload(payload).status());
  }
  EXPECT_THAT(listener_->events, ::testing::ElementsAre("OnSchemaDecoded", "OnNext"));
  ASSERT_EQ(listener_->chunks.size(), 1u);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
}

TEST_F(TestAsyncFlightMessageDecoder, SharedDictionaryIds) {
  // Two fields sharing one dictionary id: a single dictionary message supplies
  // both, and the batch decodes against it.
  const auto type = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("a", type), arrow::field("b", type)});
  auto values = ArrayFromJSON(type, R"(["x", "y"])");
  auto batch = RecordBatch::Make(schema, 2, {values, values});

  DecoderStream stream(schema, {batch});
  ASSERT_OK(ConsumePayload(stream.SchemaMessage()).status());
  int dictionary_messages = 0;
  while (listener_->chunks.empty()) {
    auto payload = stream.Next();
    ASSERT_FALSE(IsEndOfStream(payload)) << "the shared-dictionary stream ended early";
    if (payload.ipc_message.type == ipc::MessageType::DICTIONARY_BATCH) {
      ++dictionary_messages;
    }
    ASSERT_OK(ConsumePayload(payload).status());
  }
  // The stream carries one dictionary message per field path (both for the same
  // id), and neither may surface as a chunk; the batch decodes against them.
  EXPECT_GE(dictionary_messages, 1);
  ASSERT_EQ(listener_->chunks.size(), 1u);
  ASSERT_NE(listener_->chunks[0].data, nullptr);
  EXPECT_TRUE(listener_->chunks[0].data->Equals(*batch));
}

TEST_F(TestAsyncFlightMessageDecoder, DictionaryBeforeAnySchemaIsRejected) {
  // A dictionary as the very first IPC message: a stream must start with a
  // schema, and the decoder says so instead of treating it as data.
  const auto type = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("d", type)});
  const auto batch = RecordBatch::Make(schema, 1, {ArrayFromJSON(type, R"(["a"])")});
  DecoderStream stream(schema, {batch});
  auto dictionary_payload = stream.Next();
  ASSERT_FALSE(IsEndOfStream(dictionary_payload));
  const auto status = ConsumePayload(dictionary_payload).status();
  ASSERT_FALSE(status.ok()) << "a dictionary before the schema must be rejected";
  EXPECT_TRUE(listener_->events.empty());
}

TEST_F(TestAsyncFlightMessageDecoder, UnknownDictionaryIdIsRejected) {
  // A dictionary for an id the schema never declared. It is read with the batch
  // that follows it, or at the end of the stream when none does.
  const auto type = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("d", type)});
  const auto options = ipc::IpcWriteOptions::Defaults();

  FlightPayload schema_payload;
  ipc::DictionaryFieldMapper mapper;
  ASSERT_OK(mapper.AddField(0, {0}));
  ASSERT_OK(ipc::GetSchemaPayload(*schema, options, mapper, &schema_payload.ipc_message));
  ASSERT_OK(ConsumePayload(schema_payload).status());

  FlightPayload unknown;
  ASSERT_OK(ipc::GetDictionaryPayload(99, ArrayFromJSON(utf8(), R"(["a"])"), options,
                                      &unknown.ipc_message));
  ASSERT_OK(ConsumePayload(unknown).status());
  EXPECT_FALSE(decoder_->Finish().ok()) << "an undeclared dictionary id must be rejected";
  EXPECT_TRUE(listener_->chunks.empty());
}

// ----------------------------------------------------------------------
// Transport abstraction tests

TEST(TransportErrorHandling, ReconstructStatus) {
  Status current = Status::Invalid("Base error message");
  // Invalid code
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(". Also, server sent unknown or invalid Arrow status code -1"),
      internal::ReconstructStatus("-1", current, std::nullopt, std::nullopt, std::nullopt,
                                  /*detail=*/nullptr));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          ". Also, server sent unknown or invalid Arrow status code foobar"),
      internal::ReconstructStatus("foobar", current, std::nullopt, std::nullopt,
                                  std::nullopt, /*detail=*/nullptr));

  // Override code
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      AlreadyExists, ::testing::HasSubstr("Base error message"),
      internal::ReconstructStatus(
          std::to_string(static_cast<int>(StatusCode::AlreadyExists)), current,
          std::nullopt, std::nullopt, std::nullopt, /*detail=*/nullptr));

  // Override message
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      AlreadyExists, ::testing::HasSubstr("Custom error message"),
      internal::ReconstructStatus(
          std::to_string(static_cast<int>(StatusCode::AlreadyExists)), current,
          "Custom error message", std::nullopt, std::nullopt, /*detail=*/nullptr));

  // With detail
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      AlreadyExists,
      ::testing::AllOf(::testing::HasSubstr("Custom error message"),
                       ::testing::HasSubstr(". Detail: Detail message")),
      internal::ReconstructStatus(
          std::to_string(static_cast<int>(StatusCode::AlreadyExists)), current,
          "Custom error message", "Detail message", std::nullopt, /*detail=*/nullptr));

  // With detail and bin
  auto reconstructed = internal::ReconstructStatus(
      std::to_string(static_cast<int>(StatusCode::AlreadyExists)), current,
      "Custom error message", "Detail message", "Binary error details",
      /*detail=*/nullptr);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      AlreadyExists,
      ::testing::AllOf(::testing::HasSubstr("Custom error message"),
                       ::testing::HasSubstr(". Detail: Detail message")),
      reconstructed);
  auto detail = FlightStatusDetail::UnwrapStatus(reconstructed);
  ASSERT_NE(detail, nullptr);
  ASSERT_EQ(detail->extra_info(), "Binary error details");
}

// TODO: test TransportStatusDetail

#if !defined(_WIN32) && defined(ARROW_ENABLE_THREADING)

namespace {

/// Wait, bounded, for Serve() to replace the SIGUSR1 handler.
bool WaitForSignalHandlerChange(const ::arrow::internal::SignalHandler& original) {
  for (int i = 0; i < 2000; i++) {
    auto current = ::arrow::internal::GetSignalHandler(SIGUSR1);
    if (!current.ok()) {
      return false;
    }
    if (current->callback() != original.callback()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

}  // namespace

// The process-wide signal handling lives in the shared server lifecycle, so both
// server classes must behave the same way: run the same body over each instead of
// duplicating the regressions.
template <typename Server>
class TestServerSignalState : public ::testing::Test {};

// Both server classes share one lifecycle, but the async server only exists
// against gRPC >= 1.65: below that its Init() answers NotImplemented, so it
// cannot serve and the lifecycle has nothing to compare.
#  if ARROW_FLIGHT_HAS_ASYNC_SERVER
using ServerTypes = ::testing::Types<FlightServerBase, AsyncGenericFlightServerBase>;
#  else
using ServerTypes = ::testing::Types<FlightServerBase>;
#  endif
TYPED_TEST_SUITE(TestServerSignalState, ServerTypes);

// Serve() must not leave the process signal handlers overridden when it fails.
TYPED_TEST(TestServerSignalState, ServeBeforeInitRestoresSignalHandlers) {
  TypeParam server;
  ASSERT_OK(server.SetShutdownOnSignals({SIGUSR1}));
  ASSERT_OK_AND_ASSIGN(auto before, ::arrow::internal::GetSignalHandler(SIGUSR1));

  ASSERT_RAISES(UnknownError, server.Serve());

  ASSERT_OK_AND_ASSIGN(auto after, ::arrow::internal::GetSignalHandler(SIGUSR1));
  EXPECT_EQ(before.callback(), after.callback());
}

// Shutdown() on a server that was never initialized must not clear the
// process-wide signal target of another, serving instance.
TYPED_TEST(TestServerSignalState, ShutdownUninitializedKeepsOtherServerSignalable) {
  TypeParam serving_server;
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 0));
  FlightServerOptions options(location);
  ASSERT_OK(serving_server.Init(options));
  ASSERT_OK(serving_server.SetShutdownOnSignals({SIGUSR1}));

  std::atomic<bool> returned{false};
  std::atomic<bool> handler_installed{false};
  ASSERT_OK_AND_ASSIGN(auto original, ::arrow::internal::GetSignalHandler(SIGUSR1));
  std::thread serving_thread([&] {
    ARROW_UNUSED(serving_server.Serve());
    returned.store(true);
  });

  // Wait, bounded, for Serve() to install the process signal handler: the signal
  // may only be raised once it has.
  handler_installed.store(WaitForSignalHandlerChange(original));
  ASSERT_TRUE(handler_installed.load()) << "Serve() never installed its signal handler";

  TypeParam uninitialized;
  ASSERT_RAISES(Invalid, uninitialized.Shutdown());

  ::raise(SIGUSR1);
  for (int i = 0; i < 500 && !returned.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(returned.load()) << "the signal never stopped Serve()";
  if (!returned.load()) {
    ARROW_UNUSED(serving_server.Shutdown());
  }
  serving_thread.join();

  // The lifecycle also reports its own misuse by naming the server class.
  ASSERT_RAISES(Invalid, uninitialized.Shutdown());
  EXPECT_THAT(uninitialized.Shutdown().ToString(),
              ::testing::HasSubstr("Shutdown() on uninitialized "));
  EXPECT_THAT(uninitialized.Wait().ToString(),
              ::testing::HasSubstr("Wait() on uninitialized "));
}

#endif

// An async server must report why it cannot start, in every build: either it
// starts, or Init() answers NotImplemented naming the gRPC requirement.  Before
// this was pinned, a build against gRPC < 1.65 never registered an async server
// factory and Init() failed with the generic "no async server transport
// implementation" KeyError, hiding the real cause.
TEST(TestAsyncServerAvailability, InitReportsWhetherAsyncServerIsSupported) {
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("grpc://localhost:0"));
  FlightServerOptions options(location);
  AsyncGenericFlightServerBase server;
  const auto status = server.Init(options);
#if ARROW_FLIGHT_HAS_ASYNC_SERVER
  ASSERT_OK(status);
  ASSERT_OK(server.Shutdown());
#else
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(StatusCode::NotImplemented, status.code());
  ASSERT_NE(std::string::npos, status.message().find("1.65"))
      << "the error must name the gRPC version requirement, got: " << status.message();
#endif
}

}  // namespace flight
}  // namespace arrow
