/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.driver.jdbc.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

class UrlParserTest {
  @Test
  void parse() {
    final Map<String, String> parsed = UrlParser.parse("foo=bar&123=456", "&");
    assertEquals(parsed.get("foo"), "bar");
    assertEquals(parsed.get("123"), "456");
  }

  @Test
  void parseEscaped() {
    final Map<String, String> parsed = UrlParser.parse("foo=bar%26&%26123=456", "&");
    assertEquals(parsed.get("foo"), "bar&");
    assertEquals(parsed.get("&123"), "456");
  }

  @Test
  void parseEmpty() {
    final Map<String, String> parsed = UrlParser.parse("a=&b&foo=bar&123=456", "&");
    assertEquals(parsed.get("a"), "");
    assertNull(parsed.get("b"));
    assertEquals(parsed.get("foo"), "bar");
    assertEquals(parsed.get("123"), "456");
  }
}
