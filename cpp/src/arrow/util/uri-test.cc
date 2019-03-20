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

  // Relative path
  ASSERT_OK(uri.Parse("unix:tmp/flight.sock"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_FALSE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "tmp/flight.sock");

  // Absolute path
  ASSERT_OK(uri.Parse("unix:/tmp/flight.sock"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_FALSE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "/tmp/flight.sock");

  ASSERT_OK(uri.Parse("unix://localhost/tmp/flight.sock"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_TRUE(uri.has_host());
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.path(), "/tmp/flight.sock");

  ASSERT_OK(uri.Parse("unix:///tmp/flight.sock"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_TRUE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "/tmp/flight.sock");

  // Empty path
  ASSERT_OK(uri.Parse("unix:"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_FALSE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "");

  ASSERT_OK(uri.Parse("unix://localhost"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_TRUE(uri.has_host());
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.path(), "");

  // With trailing slash
  ASSERT_OK(uri.Parse("unix:/"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_FALSE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "/");

  ASSERT_OK(uri.Parse("unix:tmp/"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_FALSE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "tmp/");

  ASSERT_OK(uri.Parse("unix://localhost/"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_TRUE(uri.has_host());
  ASSERT_EQ(uri.host(), "localhost");
  ASSERT_EQ(uri.path(), "/");

  ASSERT_OK(uri.Parse("unix:/tmp/flight/"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_FALSE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "/tmp/flight/");

  ASSERT_OK(uri.Parse("unix:///tmp/flight/"));
  ASSERT_EQ(uri.scheme(), "unix");
  ASSERT_TRUE(uri.has_host());
  ASSERT_EQ(uri.host(), "");
  ASSERT_EQ(uri.path(), "/tmp/flight/");
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
