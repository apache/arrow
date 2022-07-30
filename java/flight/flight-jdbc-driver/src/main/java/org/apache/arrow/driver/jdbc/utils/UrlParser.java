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

import java.util.HashMap;
import java.util.Map;

/**
 * URL Parser for extracting key values from a connection string.
 */
public class UrlParser {
  /**
   * Parse a url key value parameters.
   *
   * @param url {@link String}
   * @return {@link Map}
   */
  public static Map<String, String> parse(String url, String separator) {
    Map<String, String> resultMap = new HashMap<>();
    String[] keyValues = url.split(separator);

    for (String keyValue : keyValues) {
      int separatorKey = keyValue.indexOf("="); // Find the first equal sign to split key and value.
      if (separatorKey != -1) { // Avoid crashes when not finding an equal sign in the property value.
        String key = keyValue.substring(0, separatorKey);
        String value = keyValue.substring(separatorKey + 1);
        resultMap.put(key, value);
      }
    }
    return resultMap;
  }
}
