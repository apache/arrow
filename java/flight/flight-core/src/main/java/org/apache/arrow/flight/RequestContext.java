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

package org.apache.arrow.flight;

import java.util.Set;

/**
 * Tracks variables about the current request.
 */
public interface RequestContext {
  /**
   * Register a variable and a value.
   * @param key the variable name.
   * @param value the value.
   */
  void put(String key, String value);

  /**
   * Retrieve a registered variable.
   * @param key the variable name.
   * @return the value, or null if not found.
   */
  String get(String key);

  /**
   * Retrieves the keys that have been registered to this context.
   * @return the keys used in this context.
   */
  Set<String> keySet();

  /**
   * Deletes a registered variable.
   * @return the value associated with the deleted variable, or null if the key doesn't exist.
   */
  String remove(String key);
}
