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
 * A set of headers for a call (request or response).
 *
 * <p>Only text (ASCII) headers are supported.
 */
public interface CallHeaders {

  /**
   * Get the value of a header. If multiple values are present, then get the last one.
   */
  String get(String key);

  /**
   * Get all values present for the given header.
   */
  Iterable<String> getAll(String key);

  /**
   * Insert a header with the given value.
   *
   * <p>Duplicate headers are permitted.
   */
  void insert(String key, String value);

  /** Get a set of all the headers. */
  Set<String> keys();

  /** Check whether the given header is present. */
  boolean containsKey(String key);
}
