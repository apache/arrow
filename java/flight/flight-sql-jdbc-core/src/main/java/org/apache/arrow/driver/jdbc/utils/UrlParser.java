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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * URL Parser for extracting key values from a connection string.
 */
public final class UrlParser {
  private UrlParser() {
  }

  /**
   * Parse URL key value parameters.
   *
   * <p>URL-decodes keys and values.
   *
   * @param url {@link String}
   * @return {@link Map}
   */
  public static Map<String, String> parse(String url, String separator) {
    Map<String, String> resultMap = new HashMap<>();
    if (url != null) {
      String[] keyValues = url.split(separator);

      for (String keyValue : keyValues) {
        try {
          int separatorKey = keyValue.indexOf("="); // Find the first equal sign to split key and value.
          if (separatorKey != -1) { // Avoid crashes when not finding an equal sign in the property value.
            String key = keyValue.substring(0, separatorKey);
            key = URLDecoder.decode(key, "UTF-8");
            String value = "";
            if (!keyValue.endsWith("=")) { // Avoid crashes for empty values.
              value = keyValue.substring(separatorKey + 1);
            }
            value = URLDecoder.decode(value, "UTF-8");
            resultMap.put(key, value);
          }
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return resultMap;
  }
}
