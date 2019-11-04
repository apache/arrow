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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace internal {

TEST(Uri, Empty) {
  Uri uri;
  ASSERT_EQ(uri.scheme(), "");
}

TEST(Uri, ParseSimple) {
  Uri uri;
  {
    // An ephemeral string object shouldn't invalidate results
    std::string s = "https://arrow.apache.org";
    ASSERT_OK(uri.Parse(s));
    s.replace(0, s.size(), s.size(), 'X');  // replace contents
  }
  ASSERT_EQ(uri.scheme(), "https");
  ASSERT_EQ(uri.host(), "arrow.apache.org");
  ASSERT_EQ(uri.port_text(), "");
}

TEST(Uri, ParsePath) {
  // The various edge cases below (leading and trailing slashes) have been
  // checked against several Python URI parsing modules: `uri`, `rfc3986`, `rfc3987`

  Uri uri;

  auto check_case = [&](std::string uri_string, std::string scheme, bool has_host,
                        std::string host, std::string path) -> void {
    ASSERT_OK(uri.Parse(uri_string));
    ASSERT_EQ(uri.scheme(), scheme);
    ASSERT_EQ(uri.has_host(), has_host);
    ASSERT_EQ(uri.host(), host);
    ASSERT_EQ(uri.path(), path);
  };

  // Relative path
  check_case("unix:tmp/flight.sock", "unix", false, "", "tmp/flight.sock");

  // Absolute path
  check_case("unix:/tmp/flight.sock", "unix", false, "", "/tmp/flight.sock");
  check_case("unix://localhost/tmp/flight.sock", "unix", true, "localhost",
             "/tmp/flight.sock");
  check_case("unix:///tmp/flight.sock", "unix", true, "", "/tmp/flight.sock");

  // Empty path
  check_case("unix:", "unix", false, "", "");
  check_case("unix://localhost", "unix", true, "localhost", "");

  // With trailing slash
  check_case("unix:/", "unix", false, "", "/");
  check_case("unix:tmp/", "unix", false, "", "tmp/");
  check_case("unix://localhost/", "unix", true, "localhost", "/");
  check_case("unix:/tmp/flight/", "unix", false, "", "/tmp/flight/");
  check_case("unix://localhost/tmp/flight/", "unix", true, "localhost", "/tmp/flight/");
  check_case("unix:///tmp/flight/", "unix", true, "", "/tmp/flight/");

  // With query string
  check_case("unix:?", "unix", false, "", "");
  check_case("unix:?foo", "unix", false, "", "");
  check_case("unix:?foo=bar", "unix", false, "", "");
  check_case("unix:/?", "unix", false, "", "/");
  check_case("unix:/?foo", "unix", false, "", "/");
  check_case("unix:/?foo=bar", "unix", false, "", "/");
  check_case("unix://localhost/tmp?", "unix", true, "localhost", "/tmp");
  check_case("unix://localhost/tmp?foo", "unix", true, "localhost", "/tmp");
  check_case("unix://localhost/tmp?foo=bar", "unix", true, "localhost", "/tmp");
}

TEST(Uri, ParseQuery) {
  Uri uri;

  auto check_case = [&](std::string uri_string, std::string query_string,
                        std::vector<std::pair<std::string, std::string>> items) -> void {
    ASSERT_OK(uri.Parse(uri_string));
    ASSERT_EQ(uri.query_string(), query_string);
    auto result = uri.query_items();
    ASSERT_OK(result);
    ASSERT_EQ(*result, items);
  };

  check_case("unix://localhost/tmp", "", {});
  check_case("unix://localhost/tmp?", "", {});
  check_case("unix://localhost/tmp?foo=bar", "foo=bar", {{"foo", "bar"}});
  check_case("unix:?foo=bar", "foo=bar", {{"foo", "bar"}});
  check_case("unix:?a=b&c=d", "a=b&c=d", {{"a", "b"}, {"c", "d"}});

  // With escaped values
  check_case("unix:?a=some+value&b=c", "a=some+value&b=c",
             {{"a", "some value"}, {"b", "c"}});
  check_case("unix:?a=some%20value%2Fanother&b=c", "a=some%20value%2Fanother&b=c",
             {{"a", "some value/another"}, {"b", "c"}});
}

TEST(Uri, ParseHostPort) {
  Uri uri;

  ASSERT_OK(uri.Parse("http://localhost:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.port_text(), "80");
  ASSERT_EQ(uri.port(), 80);

  ASSERT_OK(uri.Parse("http://1.2.3.4"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "1.2.3.4");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);

  ASSERT_OK(uri.Parse("http://1.2.3.4:"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "1.2.3.4");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);

  ASSERT_OK(uri.Parse("http://1.2.3.4:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "1.2.3.4");
  ASSERT_EQ(uri.port_text(), "80");
  ASSERT_EQ(uri.port(), 80);

  ASSERT_OK(uri.Parse("http://[::1]"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "::1");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);

  ASSERT_OK(uri.Parse("http://[::1]:"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "::1");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);

  ASSERT_OK(uri.Parse("http://[::1]:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "::1");
  ASSERT_EQ(uri.port_text(), "80");
  ASSERT_EQ(uri.port(), 80);
}

TEST(Uri, ParseError) {
  Uri uri;

  ASSERT_RAISES(Invalid, uri.Parse("http://a:b:c:d"));
  ASSERT_RAISES(Invalid, uri.Parse("http://localhost:z"));
  ASSERT_RAISES(Invalid, uri.Parse("http://localhost:-1"));
  ASSERT_RAISES(Invalid, uri.Parse("http://localhost:99999"));
}

}  // namespace internal
}  // namespace arrow
