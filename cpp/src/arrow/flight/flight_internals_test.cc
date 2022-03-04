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

namespace arrow {
namespace flight {

namespace pb = arrow::flight::protocol;

// ----------------------------------------------------------------------
// Core Flight types

TEST(FlightTypes, FlightDescriptor) {
  auto a = FlightDescriptor::Command("select * from table");
  auto b = FlightDescriptor::Command("select * from table");
  auto c = FlightDescriptor::Command("select foo from table");
  auto d = FlightDescriptor::Path({"foo", "bar"});
  auto e = FlightDescriptor::Path({"foo", "baz"});
  auto f = FlightDescriptor::Path({"foo", "baz"});

  ASSERT_EQ(a.ToString(), "FlightDescriptor<cmd = 'select * from table'>");
  ASSERT_EQ(d.ToString(), "FlightDescriptor<path = 'foo/bar'>");
  ASSERT_TRUE(a.Equals(b));
  ASSERT_FALSE(a.Equals(c));
  ASSERT_FALSE(a.Equals(d));
  ASSERT_FALSE(d.Equals(e));
  ASSERT_TRUE(e.Equals(f));
}

// This tests the internal protobuf types which don't get exported in the Flight DLL.
#ifndef _WIN32
TEST(FlightTypes, FlightDescriptorToFromProto) {
  FlightDescriptor descr_test;
  pb::FlightDescriptor pb_descr;

  FlightDescriptor descr1{FlightDescriptor::PATH, "", {"foo", "bar"}};
  ASSERT_OK(internal::ToProto(descr1, &pb_descr));
  ASSERT_OK(internal::FromProto(pb_descr, &descr_test));
  ASSERT_EQ(descr1, descr_test);

  FlightDescriptor descr2{FlightDescriptor::CMD, "command", {}};
  ASSERT_OK(internal::ToProto(descr2, &pb_descr));
  ASSERT_OK(internal::FromProto(pb_descr, &descr_test));
  ASSERT_EQ(descr2, descr_test);
}
#endif

// ARROW-6017: we should be able to construct locations for unknown
// schemes
TEST(FlightTypes, LocationUnknownScheme) {
  Location location;
  ASSERT_OK(Location::Parse("s3://test", &location));
  ASSERT_OK(Location::Parse("https://example.com/foo", &location));
}

TEST(FlightTypes, RoundTripTypes) {
  Ticket ticket{"foo"};
  ASSERT_OK_AND_ASSIGN(std::string ticket_serialized, ticket.SerializeToString());
  ASSERT_OK_AND_ASSIGN(Ticket ticket_deserialized,
                       Ticket::Deserialize(ticket_serialized));
  ASSERT_EQ(ticket.ticket, ticket_deserialized.ticket);

  FlightDescriptor desc = FlightDescriptor::Command("select * from foo;");
  ASSERT_OK_AND_ASSIGN(std::string desc_serialized, desc.SerializeToString());
  ASSERT_OK_AND_ASSIGN(FlightDescriptor desc_deserialized,
                       FlightDescriptor::Deserialize(desc_serialized));
  ASSERT_TRUE(desc.Equals(desc_deserialized));

  desc = FlightDescriptor::Path({"a", "b", "test.arrow"});
  ASSERT_OK_AND_ASSIGN(desc_serialized, desc.SerializeToString());
  ASSERT_OK_AND_ASSIGN(desc_deserialized, FlightDescriptor::Deserialize(desc_serialized));
  ASSERT_TRUE(desc.Equals(desc_deserialized));

  FlightInfo::Data data;
  std::shared_ptr<Schema> schema =
      arrow::schema({field("a", int64()), field("b", int64()), field("c", int64()),
                     field("d", int64())});
  Location location1, location2, location3;
  ASSERT_OK(Location::ForGrpcTcp("localhost", 10010, &location1));
  ASSERT_OK(Location::ForGrpcTls("localhost", 10010, &location2));
  ASSERT_OK(Location::ForGrpcUnix("/tmp/test.sock", &location3));
  std::vector<FlightEndpoint> endpoints{FlightEndpoint{ticket, {location1, location2}},
                                        FlightEndpoint{ticket, {location3}}};
  ASSERT_OK(MakeFlightInfo(*schema, desc, endpoints, -1, -1, &data));
  std::unique_ptr<FlightInfo> info = std::unique_ptr<FlightInfo>(new FlightInfo(data));
  ASSERT_OK_AND_ASSIGN(std::string info_serialized, info->SerializeToString());
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FlightInfo> info_deserialized,
                       FlightInfo::Deserialize(info_serialized));
  ASSERT_TRUE(info->descriptor().Equals(info_deserialized->descriptor()));
  ASSERT_EQ(info->endpoints(), info_deserialized->endpoints());
  ASSERT_EQ(info->total_records(), info_deserialized->total_records());
  ASSERT_EQ(info->total_bytes(), info_deserialized->total_bytes());
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
    call_headers.insert(std::make_pair(arrow::util::string_view("set-cookie"),
                                       arrow::util::string_view(incoming_cookie)));
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
      const util::optional<std::pair<std::string, std::string>> cookie_attribute,
      const std::string::size_type start_pos_after) {
    util::optional<std::pair<std::string, std::string>> attr =
        internal::Cookie::ParseCookieAttribute(cookie_str, &start_pos);

    if (cookie_attribute == util::nullopt) {
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
      call_headers.insert(std::make_pair(arrow::util::string_view("set-cookie"),
                                         arrow::util::string_view(cookie)));
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
  VerifyCookieAttributeParsing("", 0, util::nullopt, std::string::npos);

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
                               util::nullopt, std::string::npos);
  VerifyCookieAttributeParsing(cookie_string, std::string::npos, util::nullopt,
                               std::string::npos);
}

TEST_F(TestCookieParsing, CookieCache) {
  AddCookieVerifyCache({"id0=0;"}, "");
  AddCookieVerifyCache({"id0=0;", "id0=1;"}, "id0=1");
  AddCookieVerifyCache({"id0=0;", "id1=1;"}, "id0=0; id1=1");
  AddCookieVerifyCache({"id0=0;", "id1=1;", "id2=2"}, "id0=0; id1=1; id2=2");
}

}  // namespace flight
}  // namespace arrow
