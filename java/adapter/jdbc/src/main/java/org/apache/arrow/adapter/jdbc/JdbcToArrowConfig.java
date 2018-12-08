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

import org.apache.arrow.memory.BaseAllocator;

import com.google.common.base.Preconditions;

/**
 * This class configures the JDBC-to-Arrow conversion process.
 * <p>
 * The allocator is used to construct the {@link org.apache.arrow.vector.VectorSchemaRoot},
 * and the calendar is used to define the time zone of any {@link org.apahe.arrow.vector.pojo.ArrowType.Timestamp}
 * fields that are created during the conversion.
 * </p>
 * <p>
 * Neither field may be <code>null</code>.
 * </p>
 */
public final class JdbcToArrowConfig {
  private Calendar calendar;
  private BaseAllocator allocator;
  private boolean includeMetadata;

  /**
   * Constructs a new configuration from the provided allocator and calendar.  The <code>allocator</code>
   * is used when constructing the Arrow vectors from the ResultSet, and the calendar is used to define
   * Arrow Timestamp fields, and to read time-based fields from the JDBC <code>ResultSet</code>. 
   *
   * @param allocator The memory allocator to construct the Arrow vectors with.
   * @param calendar The calendar to use when constructing Timestamp fields and reading time-based results.
   */
  public JdbcToArrowConfig(BaseAllocator allocator, Calendar calendar) {
    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    this.allocator = allocator;
    this.calendar = calendar;
    this.includeMetadata = false;
  }

  public JdbcToArrowConfig(BaseAllocator allocator, Calendar calendar, boolean includeMetadata) {
    this(allocator, calendar);
    this.includeMetadata = includeMetadata;
  }

  /**
   * The calendar to use when defining Arrow Timestamp fields
   * and retrieving time-based fields from the database.
   * @return the calendar.
   */
  public Calendar getCalendar() {
    return calendar;
  }

  /**
   * Sets the {@link Calendar} to use when constructing timestamp fields in the
   * Arrow schema, and reading time-based fields from the JDBC <code>ResultSet</code>.
   *
   * @param calendar the calendar to set.
   * @exception NullPointerExeption if <code>calendar</code> is <code>null</code>.
   */
  public JdbcToArrowConfig setCalendar(Calendar calendar) {
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");
    this.calendar = calendar;
    return this;
  }

  /**
   * The Arrow memory allocator.
   * @return the allocator.
   */
  public BaseAllocator getAllocator() {
    return allocator;
  }

  /**
   * Sets the memory allocator to use when construting the Arrow vectors from the ResultSet.
   *
   * @param allocator the allocator to set.
   * @exception NullPointerException if <code>allocator</code> is null.
   */
  public JdbcToArrowConfig setAllocator(BaseAllocator allocator) {
    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");
    this.allocator = allocator;
    return this;
  }

  public boolean includeMetadata() {
    return includeMetadata;
  }

  /**
   * Whether this configuration is valid.  The configuration is valid when:
   * <ul>
   *   <li>A memory allocator is provided.</li>
   *   <li>A calendar is provided.</li>
   * </ul>
   *
   * @return Whether this configuration is valid.
   */
  public boolean isValid() {
    return (calendar != null) && (allocator != null);
  }
}
