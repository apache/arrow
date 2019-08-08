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

import java.util.Calendar;
import java.util.Map;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * This class configures the JDBC-to-Arrow conversion process.
 * <p>
 * The allocator is used to construct the {@link org.apache.arrow.vector.VectorSchemaRoot},
 * and the calendar is used to define the time zone of any
 * {@link org.apache.arrow.vector.types.pojo.ArrowType.Timestamp}
 * fields that are created during the conversion.  Neither field may be <code>null</code>.
 * </p>
 * <p>
 * If the <code>includeMetadata</code> flag is set, the Arrow field metadata will contain information
 * from the corresponding {@link java.sql.ResultSetMetaData} that was used to create the
 * {@link org.apache.arrow.vector.types.pojo.FieldType} of the corresponding
 * {@link org.apache.arrow.vector.FieldVector}.
 * </p>
 * <p>
 * If there are any {@link java.sql.Types#ARRAY} fields in the {@link java.sql.ResultSet}, the corresponding
 * {@link JdbcFieldInfo} for the array's contents must be defined here.  Unfortunately, the sub-type
 * information cannot be retrieved from all JDBC implementations (H2 for example, returns
 * {@link java.sql.Types#NULL} for the array sub-type), so it must be configured here.  The column index
 * or name can be used to map to a {@link JdbcFieldInfo}, and that will be used for the conversion.
 * </p>
 */
public final class JdbcToArrowConfig {

  private Calendar calendar;
  private BaseAllocator allocator;
  private boolean includeMetadata;
  private Map<Integer, JdbcFieldInfo> arraySubTypesByColumnIndex;
  private Map<String, JdbcFieldInfo> arraySubTypesByColumnName;

  /**
   * Constructs a new configuration from the provided allocator and calendar.  The <code>allocator</code>
   * is used when constructing the Arrow vectors from the ResultSet, and the calendar is used to define
   * Arrow Timestamp fields, and to read time-based fields from the JDBC <code>ResultSet</code>. 
   *
   * @param allocator       The memory allocator to construct the Arrow vectors with.
   * @param calendar        The calendar to use when constructing Timestamp fields and reading time-based results.
   */
  JdbcToArrowConfig(BaseAllocator allocator, Calendar calendar) {
    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");

    this.allocator = allocator;
    this.calendar = calendar;
    this.includeMetadata = false;
    this.arraySubTypesByColumnIndex = null;
    this.arraySubTypesByColumnName = null;
  }

  /**
   * Constructs a new configuration from the provided allocator and calendar.  The <code>allocator</code>
   * is used when constructing the Arrow vectors from the ResultSet, and the calendar is used to define
   * Arrow Timestamp fields, and to read time-based fields from the JDBC <code>ResultSet</code>. 
   *
   * @param allocator       The memory allocator to construct the Arrow vectors with.
   * @param calendar        The calendar to use when constructing Timestamp fields and reading time-based results.
   * @param includeMetadata Whether to include JDBC field metadata in the Arrow Schema Field metadata.
   * @param arraySubTypesByColumnIndex The type of the JDBC array at the column index (1-based).
   * @param arraySubTypesByColumnName  The type of the JDBC array at the column name.
   */
  JdbcToArrowConfig(
      BaseAllocator allocator,
      Calendar calendar,
      boolean includeMetadata,
      Map<Integer, JdbcFieldInfo> arraySubTypesByColumnIndex,
      Map<String, JdbcFieldInfo> arraySubTypesByColumnName) {

    this(allocator, calendar);

    this.includeMetadata = includeMetadata;
    this.arraySubTypesByColumnIndex = arraySubTypesByColumnIndex;
    this.arraySubTypesByColumnName = arraySubTypesByColumnName;
  }

  /**
   * The calendar to use when defining Arrow Timestamp fields
   * and retrieving {@link java.sql.Date}, {@link java.sql.Time}, or {@link java.sql.Timestamp}
   * data types from the {@link java.sql.ResultSet}, or <code>null</code> if not converting.
   *
   * @return the calendar.
   */
  public Calendar getCalendar() {
    return calendar;
  }

  /**
   * The Arrow memory allocator.
   * @return the allocator.
   */
  public BaseAllocator getAllocator() {
    return allocator;
  }

  /**
   * Whether to include JDBC ResultSet field metadata in the Arrow Schema field metadata.
   *
   * @return <code>true</code> to include field metadata, <code>false</code> to exclude it.
   */
  public boolean shouldIncludeMetadata() {
    return includeMetadata;
  }

  /**
   * Returns the array sub-type {@link JdbcFieldInfo} defined for the provided column index.
   *
   * @param index The {@link java.sql.ResultSetMetaData} column index of an {@link java.sql.Types#ARRAY} type.
   * @return The {@link JdbcFieldInfo} for that array's sub-type, or <code>null</code> if not defined.
   */
  public JdbcFieldInfo getArraySubTypeByColumnIndex(int index) {
    if (arraySubTypesByColumnIndex == null) {
      return null;
    } else {
      return arraySubTypesByColumnIndex.get(index);
    }
  }

  /**
   * Returns the array sub-type {@link JdbcFieldInfo} defined for the provided column name.
   *
   * @param name The {@link java.sql.ResultSetMetaData} column name of an {@link java.sql.Types#ARRAY} type.
   * @return The {@link JdbcFieldInfo} for that array's sub-type, or <code>null</code> if not defined.
   */
  public JdbcFieldInfo getArraySubTypeByColumnName(String name) {
    if (arraySubTypesByColumnName == null) {
      return null;
    } else {
      return arraySubTypesByColumnName.get(name);
    }
  }
}
