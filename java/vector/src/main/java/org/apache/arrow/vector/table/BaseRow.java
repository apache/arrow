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

package org.apache.arrow.vector.table;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Provides row based access to the data held by a {@link Table}.
 */
public abstract class BaseRow {

  /** The table we're enumerating. */
  protected final BaseTable table;

  /** the current row number. */
  protected int rowNumber = -1;

  /**
   * Returns the standard character set to use for decoding strings. Can be overridden for
   * individual columns by providing the {@link Charset} as an argument in the getter.
   */
  private Charset defaultCharacterSet = StandardCharsets.UTF_8;

  /**
   * Constructs a new BaseRow backed by the given table.
   *
   * @param table the table that this MutableCursor object represents
   */
  public BaseRow(BaseTable table) {
    this.table = table;
  }

  /**
   * Constructs a new BaseRow backed by the given table.
   *
   * @param table the table that this cursor represents
   * @param charset the standard charset for decoding bytes into strings. Note: This can be
   *     overridden for individual columns.
   */
  public BaseRow(BaseTable table, Charset charset) {
    this.table = table;
    this.defaultCharacterSet = charset;
  }

  /**
   * Resets the row index to -1 and returns this object.
   */
  BaseRow resetPosition() {
    rowNumber = -1;
    return this;
  }

  /**
   * Returns the default character set for use with character vectors.
   */
  public Charset getDefaultCharacterSet() {
    return defaultCharacterSet;
  }
}
