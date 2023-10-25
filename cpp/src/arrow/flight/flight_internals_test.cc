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

#include <type_traits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/flight/client_cookie_middleware.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/cookie_internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/flight/types.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
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
      ASSERT_OK_AND_ASSIGN(FlightInfo value, internal::FromProto(pb_value));
      EXPECT_EQ(values[i], value);
    } else if constexpr (std::is_same_v<FlightType, SchemaResult>) {
      std::string data;
      ASSERT_OK(internal::FromProto(pb_value, &data));
      SchemaResult value(std::move(data));
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
  std::vector<Criteria> values = {{""}, {"criteria"}};
  std::vector<std::string> reprs = {"<Criteria expression=''>",
                                    "<Criteria expression='criteria'>"};
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
  // 2023-06-19 03:14:06.004330100
  // We must use microsecond resolution here for portability.
  // std::chrono::system_clock::time_point may not provide nanosecond
  // resolution on some platforms such as Windows.
  const auto expiration_time_duration =
      std::chrono::seconds{1687144446} + std::chrono::nanoseconds{4339000};
  Timestamp expiration_time(
      std::chrono::duration_cast<Timestamp::duration>(expiration_time_duration));
  std::vector<FlightEndpoint> values = {
      {{""}, {}, std::nullopt, {}},
      {{"foo"}, {}, std::nullopt, {}},
      {{"bar"}, {}, std::nullopt, {"\xDE\xAD\xBE\xEF"}},
      {{"foo"}, {}, expiration_time, {}},
      {{"foo"}, {location1}, std::nullopt, {}},
      {{"bar"}, {location1}, std::nullopt, {}},
      {{"foo"}, {location2}, std::nullopt, {}},
      {{"foo"}, {location1, location2}, std::nullopt, {"\xba\xdd\xca\xfe"}},
  };
  std::vector<std::string> reprs = {
      "<FlightEndpoint ticket=<Ticket ticket=''> locations=[] "
      "expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations=[] "
      "expiration_time=null app_metadata=''>",
      "<FlightEndpoint ticket=<Ticket ticket='bar'> locations=[] "
      "expiration_time=null app_metadata='DEADBEEF'>",
      "<FlightEndpoint ticket=<Ticket ticket='foo'> locations=[] "
      "expiration_time=2023-06-19 03:14:06.004339000 app_metadata=''>",
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
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::FlightInfo>(values, reprs));
}

TEST(FlightTypes, PollInfo) {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 1234));
  Schema schema({field("ints", int64())});
  auto desc = FlightDescriptor::Command("foo");
  auto endpoint = FlightEndpoint{Ticket{"foo"}, {}, std::nullopt, ""};
  auto info = MakeFlightInfo(schema, desc, {endpoint}, -1, 42, true, "");
  // 2023-06-19 03:14:06.004330100
  // We must use microsecond resolution here for portability.
  // std::chrono::system_clock::time_point may not provide nanosecond
  // resolution on some platforms such as Windows.
  const auto expiration_time_duration =
      std::chrono::seconds{1687144446} + std::chrono::nanoseconds{4339000};
  Timestamp expiration_time(
      std::chrono::duration_cast<Timestamp::duration>(expiration_time_duration));
  std::vector<PollInfo> values = {
      PollInfo{std::make_unique<FlightInfo>(info), std::nullopt, std::nullopt,
               std::nullopt},
      PollInfo{std::make_unique<FlightInfo>(info), FlightDescriptor::Command("poll"), 0.1,
               expiration_time},
  };
  std::vector<std::string> reprs = {
      "<PollInfo info=" + info.ToString() +
          " descriptor=null "
          "progress=null expiration_time=null>",
      "<PollInfo info=" + info.ToString() +
          " descriptor=<FlightDescriptor cmd='poll'> "
          "progress=0.1 expiration_time=2023-06-19 03:14:06.004339000>",
  };

  ASSERT_NO_FATAL_FAILURE(TestRoundtrip<pb::PollInfo>(values, reprs));
}

TEST(FlightTypes, Result) {
  std::vector<Result> values = {
      {Buffer::FromString("")},
      {Buffer::FromString("foo")},
      {Buffer::FromString("bar")},
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
      {""},
      {"foo"},
      {"bar"},
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

  void VerifyCookieDateConverson(std::string date, const std::string& converted_date) {
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
  VerifyCookieDateConverson("Mon, 01 jan 2038 22:15:36 GMT;", "01 01 2038 22:15:36");
  VerifyCookieDateConverson("TUE, 10 Feb 2038 22:15:36 GMT", "10 02 2038 22:15:36");
  VerifyCookieDateConverson("WED, 20 MAr 2038 22:15:36 GMT;", "20 03 2038 22:15:36");
  VerifyCookieDateConverson("thu, 15 APR 2038 22:15:36 GMT", "15 04 2038 22:15:36");
  VerifyCookieDateConverson("Fri, 30 mAY 2038 22:15:36 GMT;", "30 05 2038 22:15:36");
  VerifyCookieDateConverson("Sat, 03 juN 2038 22:15:36 GMT", "03 06 2038 22:15:36");
  VerifyCookieDateConverson("Sun, 01 JuL 2038 22:15:36 GMT;", "01 07 2038 22:15:36");
  VerifyCookieDateConverson("Fri, 06 aUg 2038 22:15:36 GMT", "06 08 2038 22:15:36");
  VerifyCookieDateConverson("Fri, 01 SEP 2038 22:15:36 GMT;", "01 09 2038 22:15:36");
  VerifyCookieDateConverson("Fri, 01 OCT 2038 22:15:36 GMT", "01 10 2038 22:15:36");
  VerifyCookieDateConverson("Fri, 01 Nov 2038 22:15:36 GMT;", "01 11 2038 22:15:36");
  VerifyCookieDateConverson("Fri, 01 deC 2038 22:15:36 GMT", "01 12 2038 22:15:36");
  VerifyCookieDateConverson("", "");
  VerifyCookieDateConverson("Fri, 01 INVALID 2038 22:15:36 GMT;",
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

}  // namespace flight
}  // namespace arrow
