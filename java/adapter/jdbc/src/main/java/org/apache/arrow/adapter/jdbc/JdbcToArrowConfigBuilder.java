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
package org.apache.arrow.adapter.jdbc;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowConfig.DEFAULT_TARGET_BATCH_SIZE;

import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** This class builds {@link JdbcToArrowConfig}s. */
public class JdbcToArrowConfigBuilder {
  private Calendar calendar;
  private BufferAllocator allocator;
  private boolean includeMetadata;
  private boolean reuseVectorSchemaRoot;
  private Map<Integer, JdbcFieldInfo> arraySubTypesByColumnIndex;
  private Map<String, JdbcFieldInfo> arraySubTypesByColumnName;
  private Map<Integer, JdbcFieldInfo> explicitTypesByColumnIndex;
  private Map<String, JdbcFieldInfo> explicitTypesByColumnName;
  private Map<String, String> schemaMetadata;
  private Map<Integer, Map<String, String>> columnMetadataByColumnIndex;
  private int targetBatchSize;
  private Function<JdbcFieldInfo, ArrowType> jdbcToArrowTypeConverter;
  private JdbcToArrowConfig.JdbcConsumerFactory jdbcConsumerGetter;
  private RoundingMode bigDecimalRoundingMode;

  /**
   * Default constructor for the <code>JdbcToArrowConfigBuilder}</code>. Use the setter methods for
   * the allocator and calendar; the allocator must be set. Otherwise, {@link #build()} will throw a
   * {@link NullPointerException}.
   */
  public JdbcToArrowConfigBuilder() {
    this.allocator = null;
    this.calendar = null;
    this.includeMetadata = false;
    this.reuseVectorSchemaRoot = false;
    this.arraySubTypesByColumnIndex = null;
    this.arraySubTypesByColumnName = null;
    this.explicitTypesByColumnIndex = null;
    this.explicitTypesByColumnName = null;
    this.schemaMetadata = null;
    this.columnMetadataByColumnIndex = null;
    this.bigDecimalRoundingMode = null;
  }

  /**
   * Constructor for the <code>JdbcToArrowConfigBuilder</code>. The allocator is required, and a
   * {@link NullPointerException} will be thrown if it is <code>null</code>.
   *
   * <p>The allocator is used to construct Arrow vectors from the JDBC ResultSet. The calendar is
   * used to determine the time zone of {@link java.sql.Timestamp} fields and convert {@link
   * java.sql.Date}, {@link java.sql.Time}, and {@link java.sql.Timestamp} fields to a single,
   * common time zone when reading from the result set.
   *
   * @param allocator The Arrow Vector memory allocator.
   * @param calendar The calendar to use when constructing timestamp fields.
   */
  public JdbcToArrowConfigBuilder(BufferAllocator allocator, Calendar calendar) {
    this();

    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");

    this.allocator = allocator;
    this.calendar = calendar;
    this.includeMetadata = false;
    this.reuseVectorSchemaRoot = false;
    this.targetBatchSize = DEFAULT_TARGET_BATCH_SIZE;
  }

  /**
   * Constructor for the <code>JdbcToArrowConfigBuilder</code>. Both the allocator and calendar are
   * required. A {@link NullPointerException} will be thrown if either of those arguments is <code>
   * null</code>.
   *
   * <p>The allocator is used to construct Arrow vectors from the JDBC ResultSet. The calendar is
   * used to determine the time zone of {@link java.sql.Timestamp} fields and convert {@link
   * java.sql.Date}, {@link java.sql.Time}, and {@link java.sql.Timestamp} fields to a single,
   * common time zone when reading from the result set.
   *
   * <p>The <code>includeMetadata</code> argument, if <code>true</code> will cause various
   * information about each database field to be added to the Vector Schema's field metadata.
   *
   * @param allocator The Arrow Vector memory allocator.
   * @param calendar The calendar to use when constructing timestamp fields.
   */
  public JdbcToArrowConfigBuilder(
      BufferAllocator allocator, Calendar calendar, boolean includeMetadata) {
    this(allocator, calendar);
    this.includeMetadata = includeMetadata;
  }

  /**
   * Sets the memory allocator to use when constructing the Arrow vectors from the ResultSet.
   *
   * @param allocator the allocator to set.
   * @exception NullPointerException if <code>allocator</code> is null.
   */
  public JdbcToArrowConfigBuilder setAllocator(BufferAllocator allocator) {
    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");
    this.allocator = allocator;
    return this;
  }

  /**
   * Sets the {@link Calendar} to use when constructing timestamp fields in the Arrow schema, and
   * reading time-based fields from the JDBC <code>ResultSet</code>.
   *
   * @param calendar the calendar to set.
   */
  public JdbcToArrowConfigBuilder setCalendar(Calendar calendar) {
    this.calendar = calendar;
    return this;
  }

  /**
   * Sets whether to include JDBC ResultSet field metadata in the Arrow Schema field metadata.
   *
   * @param includeMetadata Whether to include or exclude JDBC metadata in the Arrow Schema field
   *     metadata.
   * @return This instance of the <code>JdbcToArrowConfig</code>, for chaining.
   */
  public JdbcToArrowConfigBuilder setIncludeMetadata(boolean includeMetadata) {
    this.includeMetadata = includeMetadata;
    return this;
  }

  /**
   * Sets the mapping of column-index-to-{@link JdbcFieldInfo} used for columns of type {@link
   * java.sql.Types#ARRAY}. The column index is 1-based, to match the JDBC column index.
   *
   * @param map The mapping.
   * @return This instance of the <code>JdbcToArrowConfig</code>, for chaining.
   */
  public JdbcToArrowConfigBuilder setArraySubTypeByColumnIndexMap(Map<Integer, JdbcFieldInfo> map) {
    this.arraySubTypesByColumnIndex = map;
    return this;
  }

  /**
   * Sets the mapping of column-name-to-{@link JdbcFieldInfo} used for columns of type {@link
   * java.sql.Types#ARRAY}.
   *
   * @param map The mapping.
   * @return This instance of the <code>JdbcToArrowConfig</code>, for chaining.
   */
  public JdbcToArrowConfigBuilder setArraySubTypeByColumnNameMap(Map<String, JdbcFieldInfo> map) {
    this.arraySubTypesByColumnName = map;
    return this;
  }

  /**
   * Sets the mapping of column-index-to-{@link JdbcFieldInfo} used for column types.
   *
   * <p>This can be useful to override type information from JDBC drivers that provide incomplete
   * type info, e.g. DECIMAL with precision = scale = 0.
   *
   * <p>The column index is 1-based, to match the JDBC column index.
   *
   * @param map The mapping.
   */
  public JdbcToArrowConfigBuilder setExplicitTypesByColumnIndex(Map<Integer, JdbcFieldInfo> map) {
    this.explicitTypesByColumnIndex = map;
    return this;
  }

  /**
   * Sets the mapping of column-name-to-{@link JdbcFieldInfo} used for column types.
   *
   * <p>This can be useful to override type information from JDBC drivers that provide incomplete
   * type info, e.g. DECIMAL with precision = scale = 0.
   *
   * @param map The mapping.
   */
  public JdbcToArrowConfigBuilder setExplicitTypesByColumnName(Map<String, JdbcFieldInfo> map) {
    this.explicitTypesByColumnName = map;
    return this;
  }

  /**
   * Set the target number of rows to convert at once.
   *
   * <p>Use {@link JdbcToArrowConfig#NO_LIMIT_BATCH_SIZE} to read all rows at once.
   */
  public JdbcToArrowConfigBuilder setTargetBatchSize(int targetBatchSize) {
    this.targetBatchSize = targetBatchSize;
    return this;
  }

  /**
   * Set the function used to convert JDBC types to Arrow types.
   *
   * <p>Defaults to wrapping {@link JdbcToArrowUtils#getArrowTypeFromJdbcType(JdbcFieldInfo,
   * Calendar)}.
   */
  public JdbcToArrowConfigBuilder setJdbcToArrowTypeConverter(
      Function<JdbcFieldInfo, ArrowType> jdbcToArrowTypeConverter) {
    this.jdbcToArrowTypeConverter = jdbcToArrowTypeConverter;
    return this;
  }

  /**
   * Set the function used to get a JDBC consumer for a given type.
   *
   * <p>Defaults to wrapping {@link JdbcToArrowUtils#getConsumer(ArrowType, Integer, Boolean,
   * FieldVector, JdbcToArrowConfig)}.
   */
  public JdbcToArrowConfigBuilder setJdbcConsumerGetter(
      JdbcToArrowConfig.JdbcConsumerFactory jdbcConsumerGetter) {
    this.jdbcConsumerGetter = jdbcConsumerGetter;
    return this;
  }

  /**
   * Set whether to use the same {@link org.apache.arrow.vector.VectorSchemaRoot} instance on each
   * iteration, or to allocate a new one.
   */
  public JdbcToArrowConfigBuilder setReuseVectorSchemaRoot(boolean reuseVectorSchemaRoot) {
    this.reuseVectorSchemaRoot = reuseVectorSchemaRoot;
    return this;
  }

  /** Set metadata for schema. */
  public JdbcToArrowConfigBuilder setSchemaMetadata(Map<String, String> schemaMetadata) {
    this.schemaMetadata = schemaMetadata;
    return this;
  }

  /** Set metadata from columnIndex->meta map on per field basis. */
  public JdbcToArrowConfigBuilder setColumnMetadataByColumnIndex(
      Map<Integer, Map<String, String>> columnMetadataByColumnIndex) {
    this.columnMetadataByColumnIndex = columnMetadataByColumnIndex;
    return this;
  }

  /**
   * Set the rounding mode used when the scale of the actual value does not match the declared
   * scale.
   *
   * <p>By default, an error is raised in such cases.
   */
  public JdbcToArrowConfigBuilder setBigDecimalRoundingMode(RoundingMode bigDecimalRoundingMode) {
    this.bigDecimalRoundingMode = bigDecimalRoundingMode;
    return this;
  }

  /**
   * This builds the {@link JdbcToArrowConfig} from the provided {@link BufferAllocator} and {@link
   * Calendar}.
   *
   * @return The built {@link JdbcToArrowConfig}
   * @throws NullPointerException if either the allocator or calendar was not set.
   */
  public JdbcToArrowConfig build() {
    return new JdbcToArrowConfig(
        allocator,
        calendar,
        includeMetadata,
        reuseVectorSchemaRoot,
        arraySubTypesByColumnIndex,
        arraySubTypesByColumnName,
        targetBatchSize,
        jdbcToArrowTypeConverter,
        jdbcConsumerGetter,
        explicitTypesByColumnIndex,
        explicitTypesByColumnName,
        schemaMetadata,
        columnMetadataByColumnIndex,
        bigDecimalRoundingMode);
  }
}
