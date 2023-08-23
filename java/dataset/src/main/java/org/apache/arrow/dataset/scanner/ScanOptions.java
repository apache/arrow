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
  private final Optional<String[]> columnsSubset;
  private final long batchSize;
  private Optional<ByteBuffer> columnsProduceOrFilter;

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
   * @param columnsSubset (Optional) Projected columns. {@link Optional#empty()} for scanning all columns. Otherwise,
   *                Only columns present in the Array will be scanned.
   */
  public ScanOptions(long batchSize, Optional<String[]> columnsSubset) {
    Preconditions.checkNotNull(columnsSubset);
    this.batchSize = batchSize;
    this.columnsSubset = columnsSubset;
    this.columnsProduceOrFilter = Optional.empty();
  }

  public ScanOptions(long batchSize) {
    this(batchSize, Optional.empty());
  }

  public Optional<String[]> getColumnsSubset() {
    return columnsSubset;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public Optional<ByteBuffer> getColumnsProduceOrFilter() {
    return columnsProduceOrFilter;
  }

  /**
   * Builder for Options used during scanning.
   */
  public static class Builder {
    private final long batchSize;
    private final Optional<String[]> columnsSubset;
    private Optional<ByteBuffer> columnsProduceOrFilter = Optional.empty();

    /**
     * Constructor.
     * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}
     * @param columnsSubset (Optional) Projected columns. {@link Optional#empty()} for scanning all columns. Otherwise,
     *                Only columns present in the Array will be scanned.
     */
    public Builder(long batchSize, Optional<String[]> columnsSubset) {
      Preconditions.checkNotNull(columnsSubset);
      this.batchSize = batchSize;
      this.columnsSubset = columnsSubset;
    }

    /**
     * Set the Substrait extended expression.
     *
     * <p>Can be used to filter data and/or project new columns.
     *
     * @param columnsProduceOrFilter (Optional) Expressions to evaluate to projects new columns or applies filter.
     * @return the ScanOptions configured.
     */
    public Builder columnsProduceOrFilter(Optional<ByteBuffer> columnsProduceOrFilter) {
      Preconditions.checkNotNull(columnsProduceOrFilter);
      this.columnsProduceOrFilter = columnsProduceOrFilter;
      return this;
    }

    public ScanOptions build() {
      return new ScanOptions(this);
    }
  }

  private ScanOptions(Builder builder) {
    columnsSubset = builder.columnsSubset;
    batchSize = builder.batchSize;
    columnsProduceOrFilter = builder.columnsProduceOrFilter;
  }
}
