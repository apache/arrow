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
 * A set of metadata key value pairs for a call (request or response).
 */
public interface CallHeaders {
  /**
   * Get the value of a metadata key. If multiple values are present, then get the last one.
   */
  String get(String key);

  /**
   * Get the value of a metadata key. If multiple values are present, then get the last one.
   */
  byte[] getByte(String key);

  /**
   * Get all values present for the given metadata key.
   */
  Iterable<String> getAll(String key);

  /**
   * Get all values present for the given metadata key.
   */
  Iterable<byte[]> getAllByte(String key);

  /**
   * Insert a metadata pair with the given value.
   *
   * <p>Duplicate metadata are permitted.
   */
  void insert(String key, String value);

  /**
   * Insert a metadata pair with the given value.
   *
   * <p>Duplicate metadata are permitted.
   */
  void insert(String key, byte[] value);

  /** Get a set of all the metadata keys. */
  Set<String> keys();

  /** Check whether the given metadata key is present. */
  boolean containsKey(String key);
}
