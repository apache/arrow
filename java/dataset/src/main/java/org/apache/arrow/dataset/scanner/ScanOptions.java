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

package org.apache.arrow.dataset.scanner;

import java.nio.ByteBuffer;
import java.util.Optional;

import org.apache.arrow.util.Preconditions;

/**
 * Options used during scanning.
 */
public class ScanOptions {
  private final Optional<String[]> columns;
  private final long batchSize;
  private ByteBuffer substraitExtendedExpression;

  /**
   * Constructor.
   * @param columns Projected columns. Empty for scanning all columns.
   * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}
   *
   * @deprecated Deprecated. Use {@link #ScanOptions(long, Optional)} instead.
   */
  @Deprecated
  public ScanOptions(String[] columns, long batchSize) {
    this(batchSize, Optional.of(columns).map(present -> {
      if (present.length == 0) {
        // Backwards compatibility: See ARROW-13257, in the new constructor, we now use null to scan for all columns.
        return null;
      }
      return present;
    }));
  }

  /**
   * Constructor.
   * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}
   * @param columns (Optional) Projected columns. {@link Optional#empty()} for scanning all columns. Otherwise,
   *                Only columns present in the Array will be scanned.
   */
  public ScanOptions(long batchSize, Optional<String[]> columns) {
    Preconditions.checkNotNull(columns);
    this.batchSize = batchSize;
    this.columns = columns;
  }

  /**
   * Constructor.
   * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}
   * @param substraitExtendedExpression Extended expression to evaluate for project new columns or apply filter.
   */
  public ScanOptions(long batchSize, ByteBuffer substraitExtendedExpression) {
    Preconditions.checkNotNull(substraitExtendedExpression);
    this.batchSize = batchSize;
    this.columns = Optional.empty();
    this.substraitExtendedExpression = substraitExtendedExpression;
  }

  public ScanOptions(long batchSize) {
    this(batchSize, Optional.empty());
  }

  public Optional<String[]> getColumns() {
    return columns;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public ByteBuffer getSubstraitExtendedExpression() {
    return substraitExtendedExpression;
  }
}
