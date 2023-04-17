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

TEST(UriEscape, Basics) {
  ASSERT_EQ(UriEscape(""), "");
  ASSERT_EQ(UriEscape("foo123"), "foo123");
  ASSERT_EQ(UriEscape("/El Niño/"), "%2FEl%20Ni%C3%B1o%2F");
  ASSERT_EQ(UriEscape("arrow.apache.org"), "arrow.apache.org");
  ASSERT_EQ(UriEscape("192.168.1.1"), "192.168.1.1");
}

TEST(UriEncodeHost, Basics) {
  ASSERT_EQ(UriEncodeHost("::1"), "[::1]");
  ASSERT_EQ(UriEncodeHost("arrow.apache.org"), "arrow.apache.org");
  ASSERT_EQ(UriEncodeHost("192.168.1.1"), "192.168.1.1");
}

TEST(IsValidUriScheme, Basics) {
  ASSERT_FALSE(IsValidUriScheme(""));
  ASSERT_FALSE(IsValidUriScheme(":"));
  ASSERT_FALSE(IsValidUriScheme("."));
  ASSERT_TRUE(IsValidUriScheme("a"));
  ASSERT_TRUE(IsValidUriScheme("file"));
  ASSERT_TRUE(IsValidUriScheme("local-file"));
  ASSERT_TRUE(IsValidUriScheme("s3"));
  ASSERT_TRUE(IsValidUriScheme("grpc+https"));
  ASSERT_TRUE(IsValidUriScheme("file.local"));
  ASSERT_FALSE(IsValidUriScheme("3s"));
  ASSERT_FALSE(IsValidUriScheme("-file"));
  ASSERT_FALSE(IsValidUriScheme("local/file"));
  ASSERT_FALSE(IsValidUriScheme("filé"));
}

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

  // With escaped path characters
  check_case("unix://localhost/tmp/some%20path/100%25%20%C3%A9l%C3%A9phant", "unix", true,
             "localhost", "/tmp/some path/100% éléphant");
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
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://1.2.3.4"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "1.2.3.4");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://1.2.3.4:"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "1.2.3.4");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://1.2.3.4:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "1.2.3.4");
  ASSERT_EQ(uri.port_text(), "80");
  ASSERT_EQ(uri.port(), 80);
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://[::1]"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "::1");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://[::1]:"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "::1");
  ASSERT_EQ(uri.port_text(), "");
  ASSERT_EQ(uri.port(), -1);
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://[::1]:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "::1");
  ASSERT_EQ(uri.port_text(), "80");
  ASSERT_EQ(uri.port(), 80);
  ASSERT_EQ(uri.username(), "");
  ASSERT_EQ(uri.password(), "");
}

TEST(Uri, ParseUserPass) {
  Uri uri;

  ASSERT_OK(uri.Parse("http://someuser@localhost:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.username(), "someuser");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://someuser:@localhost:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.username(), "someuser");
  ASSERT_EQ(uri.password(), "");

  ASSERT_OK(uri.Parse("http://someuser:somepass@localhost:80"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.username(), "someuser");
  ASSERT_EQ(uri.password(), "somepass");

  ASSERT_OK(uri.Parse("http://someuser:somepass@localhost"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.username(), "someuser");
  ASSERT_EQ(uri.password(), "somepass");

  // With %-encoding
  ASSERT_OK(uri.Parse("http://some%20user%2Fname:somepass@localhost"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.username(), "some user/name");
  ASSERT_EQ(uri.password(), "somepass");

  ASSERT_OK(uri.Parse("http://some%20user%2Fname:some%20pass%2Fword@localhost"));
  ASSERT_EQ(uri.scheme(), "http");
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.username(), "some user/name");
  ASSERT_EQ(uri.password(), "some pass/word");
}

TEST(Uri, FileScheme) {
  // "file" scheme URIs
  // https://en.wikipedia.org/wiki/File_URI_scheme
  // https://tools.ietf.org/html/rfc8089
  Uri uri;

  auto check_file_no_host = [&](std::string uri_string, std::string path) -> void {
    ASSERT_OK(uri.Parse(uri_string));
    ASSERT_TRUE(uri.is_file_scheme());
    ASSERT_EQ(uri.scheme(), "file");
    ASSERT_EQ(uri.host(), "");
    ASSERT_EQ(uri.path(), path);
    ASSERT_EQ(uri.username(), "");
    ASSERT_EQ(uri.password(), "");
  };

  auto check_notfile_no_host = [&](std::string uri_string, std::string path) -> void {
    ASSERT_OK(uri.Parse(uri_string));
    ASSERT_FALSE(uri.is_file_scheme());
    ASSERT_NE(uri.scheme(), "file");
    ASSERT_EQ(uri.host(), "");
    ASSERT_EQ(uri.path(), path);
    ASSERT_EQ(uri.username(), "");
    ASSERT_EQ(uri.password(), "");
  };

  auto check_file_with_host = [&](std::string uri_string, std::string host,
                                  std::string path) -> void {
    ASSERT_OK(uri.Parse(uri_string));
    ASSERT_TRUE(uri.is_file_scheme());
    ASSERT_EQ(uri.scheme(), "file");
    ASSERT_EQ(uri.host(), host);
    ASSERT_EQ(uri.path(), path);
    ASSERT_EQ(uri.username(), "");
    ASSERT_EQ(uri.password(), "");
  };

  // Relative paths are not accepted in "file" URIs.
  ASSERT_RAISES(Invalid, uri.Parse("file:"));
  ASSERT_RAISES(Invalid, uri.Parse("file:foo/bar"));

  // Absolute paths
  // (no authority)
  check_file_no_host("file:/", "/");
  check_file_no_host("file:/foo1/bar", "/foo1/bar");
  // (empty authority)
  check_file_no_host("file:///", "/");
  check_file_no_host("file:///foo2/bar", "/foo2/bar");
  // (not file scheme)
  check_notfile_no_host("s3:/", "/");
  check_notfile_no_host("s3:///foo3/bar", "/foo3/bar");
  // (non-empty authority)
  check_file_with_host("file://localhost/", "localhost", "/");
  check_file_with_host("file://localhost/foo/bar", "localhost", "/foo/bar");
  check_file_with_host("file://hostname.com/", "hostname.com", "/");
  check_file_with_host("file://hostname.com/foo/bar", "hostname.com", "/foo/bar");
  // (authority with special chars, not 100% sure this is the right behavior)
  check_file_with_host("file://some%20host/foo/bar", "some host", "/foo/bar");

#ifdef _WIN32
  // Relative paths
  ASSERT_RAISES(Invalid, uri.Parse("file:/C:foo/bar"));
  // (NOTE: "file:/C:" is currently parsed as an absolute URI pointing to "C:/")

  // Absolute paths
  // (no authority)
  check_file_no_host("file:/C:/", "C:/");
  check_file_no_host("file:/C:/foo/bar", "C:/foo/bar");
  // (empty authority)
  check_file_no_host("file:///D:/", "D:/");
  check_file_no_host("file:///D:/foo/bar", "D:/foo/bar");
  // (not file scheme; so slash is prepended)
  check_notfile_no_host("hive:///E:/", "/E:/");
  check_notfile_no_host("hive:/E:/foo/bar", "/E:/foo/bar");
  // (non-empty authority)
  check_file_with_host("file://server/share/", "server", "/share/");
  check_file_with_host("file://server/share/foo/bar", "server", "/share/foo/bar");
#endif
}

TEST(Uri, ParseError) {
  Uri uri;

  ASSERT_RAISES(Invalid, uri.Parse("http://a:b:c:d"));
  ASSERT_RAISES(Invalid, uri.Parse("http://localhost:z"));
  ASSERT_RAISES(Invalid, uri.Parse("http://localhost:-1"));
  ASSERT_RAISES(Invalid, uri.Parse("http://localhost:99999"));

  // Scheme-less URIs (forbidden by RFC 3986, and ambiguous to parse)
  ASSERT_RAISES(Invalid, uri.Parse("localhost"));
  ASSERT_RAISES(Invalid, uri.Parse("/foo/bar"));
  ASSERT_RAISES(Invalid, uri.Parse("foo/bar"));
  ASSERT_RAISES(Invalid, uri.Parse(""));
}

TEST(UriFromAbsolutePath, Basics) {
#ifdef _WIN32
  ASSERT_OK_AND_EQ("file:///C:/foo/bar", UriFromAbsolutePath("C:\\foo\\bar"));
  ASSERT_OK_AND_EQ("file:///C:/foo/bar", UriFromAbsolutePath("C:/foo/bar"));
  ASSERT_OK_AND_EQ("file:///C:/some%20path/100%25%20%C3%A9l%C3%A9phant",
                   UriFromAbsolutePath("C:/some path/100% éléphant"));

  ASSERT_OK_AND_EQ("file://some/share/foo/bar",
                   UriFromAbsolutePath("\\\\some\\share\\foo\\bar"));
  ASSERT_OK_AND_EQ("file://some/share/foo/bar",
                   UriFromAbsolutePath("//some/share/foo/bar"));
  ASSERT_OK_AND_EQ("file://some%20share/some%20path/100%25%20%C3%A9l%C3%A9phant",
                   UriFromAbsolutePath("//some share/some path/100% éléphant"));
#else
  ASSERT_OK_AND_EQ("file:///", UriFromAbsolutePath("/"));
  ASSERT_OK_AND_EQ("file:///tmp/foo/bar", UriFromAbsolutePath("/tmp/foo/bar"));
  ASSERT_OK_AND_EQ("file:///some%20path/100%25%20%C3%A9l%C3%A9phant",
                   UriFromAbsolutePath("/some path/100% éléphant"));
#endif
}

}  // namespace internal
}  // namespace arrow
