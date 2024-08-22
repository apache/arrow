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
package org.apache.arrow.dataset.utils;

import java.util.Map;

/** The utility class for Map. */
public class MapUtil {
  private MapUtil() {}

  /**
   * Convert the map to string array as JNI bridge.
   *
   * @param config config map
   * @return string array for serialization
   */
  public static String[] convertMapToStringArray(Map<String, String> config) {
    if (config.isEmpty()) {
      return null;
    }
    String[] configs = new String[config.size() * 2];
    int i = 0;
    for (Map.Entry<String, String> entry : config.entrySet()) {
      configs[i++] = entry.getKey();
      configs[i++] = entry.getValue();
    }
    return configs;
  }
}
