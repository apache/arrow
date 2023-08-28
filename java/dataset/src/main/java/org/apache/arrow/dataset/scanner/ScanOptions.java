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
  private ByteBuffer projection;
  private ByteBuffer filter;
  private ByteBuffer projectionAndFilter;

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

  public ScanOptions(long batchSize) {
    this(batchSize, Optional.empty());
  }

  public Optional<String[]> getColumns() {
    return columns;
  }

  public long getBatchSize() {
    return batchSize;
  }

  private ByteBuffer getProjection() {
    return projection;
  }

  private ByteBuffer getFilter() {
    return filter;
  }

  private ByteBuffer getProjectionAndFilter() {
    return projectionAndFilter;
  }

  /**
   * To evaluate what option was used to define Substrait Extended Expression (Project/Filter).
   *
   * @return Substrait Extended Expression configured for project new columns and/or apply filter
   */
  public ByteBuffer getSubstraitExtendedExpression() {
    if (getProjection() != null) {
      return getProjection();
    } else if (getFilter() != null) {
      return getFilter();
    } else if (getProjectionAndFilter() != null) {
      return getProjectionAndFilter();
    } else {
      return null;
    }
  }

  /**
   * Builder for Options used during scanning.
   */
  public static class Builder {
    private final long batchSize;
    private final Optional<String[]> columns;
    private ByteBuffer projection;
    private ByteBuffer filter;
    private ByteBuffer projectionAndFilter;

    /**
     * Constructor.
     * @param batchSize Maximum row number of each returned {@link org.apache.arrow.vector.ipc.message.ArrowRecordBatch}
     * @param columns (Optional) Projected columns. {@link Optional#empty()} for scanning all columns. Otherwise,
     *                Only columns present in the Array will be scanned.
     */
    public Builder(long batchSize, Optional<String[]> columns) {
      Preconditions.checkNotNull(columns);
      this.batchSize = batchSize;
      this.columns = columns;
    }

    /**
     * Set the Substrait extended expression.
     *
     * <p>Can be used to filter data and/or project new columns.
     *
     * @param substraitExtendedExpression (Optional) Expressions to evaluate to projects new columns or applies filter.
     * @return the ScanOptions configured.
     */
    public Builder projectionAndFilter(ByteBuffer substraitExtendedExpression) {
      Preconditions.checkNotNull(substraitExtendedExpression);
      this.projectionAndFilter = substraitExtendedExpression;
      return this;
    }

    /**
     * Set the Substrait extended expression for Projection new columns.
     *
     * @param substraitExtendedExpression (Optional) Expressions to evaluate for Project new columns.
     * @return the ScanOptions configured.
     */
    public Builder projection(ByteBuffer substraitExtendedExpression) {
      Preconditions.checkNotNull(substraitExtendedExpression);
      this.projection = substraitExtendedExpression;
      return this;
    }

    /**
     * Set the Substrait extended expression for Filter.
     *
     * @param substraitExtendedExpression (Optional) Expressions to evaluate for applies Filter.
     * @return the ScanOptions configured.
     */
    public Builder filter(ByteBuffer substraitExtendedExpression) {
      Preconditions.checkNotNull(substraitExtendedExpression);
      this.filter = substraitExtendedExpression;
      return this;
    }

    public ScanOptions build() {
      return new ScanOptions(this);
    }
  }

  private ScanOptions(Builder builder) {
    columns = builder.columns;
    batchSize = builder.batchSize;
    projection = builder.projection;
    filter = builder.filter;
    projectionAndFilter = builder.projectionAndFilter;
  }
}
