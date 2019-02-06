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
 * This class builds {@link JdbcToArrowConfig}s.
 */
public class JdbcToArrowConfigBuilder {
  private Calendar calendar;
  private BaseAllocator allocator;
  private boolean includeMetadata;

  /**
   * Default constructor for the <code>JdbcToArrowConfigBuilder}</code>.
   * Use the setter methods for the allocator and calendar; the allocator must be
   * set.  Otherwise, {@link #build()} will throw a {@link NullPointerException}.
   */
  public JdbcToArrowConfigBuilder() {
    this.allocator = null;
    this.calendar = null;
    this.includeMetadata = false;
  }

  /**
   * Constructor for the <code>JdbcToArrowConfigBuilder</code>.  The
   * allocator is required, and a {@link NullPointerException}
   * will be thrown if it is <code>null</code>.
   * <p>
   * The allocator is used to construct Arrow vectors from the JDBC ResultSet.
   * The calendar is used to determine the time zone of {@link java.sql.Timestamp}
   * fields and convert {@link java.sql.Date}, {@link java.sql.Time}, and
   * {@link java.sql.Timestamp} fields to a single, common time zone when reading
   * from the result set.
   * </p>
   *
   * @param allocator The Arrow Vector memory allocator.
   * @param calendar The calendar to use when constructing timestamp fields.
   */
  public JdbcToArrowConfigBuilder(BaseAllocator allocator, Calendar calendar) {
    this();

    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");

    this.allocator = allocator;
    this.calendar = calendar;
    this.includeMetadata = false;
  }

  /**
   * Constructor for the <code>JdbcToArrowConfigBuilder</code>.  Both the
   * allocator and calendar are required.  A {@link NullPointerException}
   * will be thrown if either of those arguments is <code>null</code>.
   * <p>
   * The allocator is used to construct Arrow vectors from the JDBC ResultSet.
   * The calendar is used to determine the time zone of {@link java.sql.Timestamp}
   * fields and convert {@link java.sql.Date}, {@link java.sql.Time}, and
   * {@link java.sql.Timestamp} fields to a single, common time zone when reading
   * from the result set.
   * </p>
   * <p>
   * The <code>includeMetadata</code> argument, if <code>true</code> will cause
   * various information about each database field to be added to the Vector
   * Schema's field metadata.
   * </p>
   *
   * @param allocator The Arrow Vector memory allocator.
   * @param calendar The calendar to use when constructing timestamp fields.
   */
  public JdbcToArrowConfigBuilder(BaseAllocator allocator, Calendar calendar, boolean includeMetadata) {
    this(allocator, calendar);
    this.includeMetadata = includeMetadata;
  }

  /**
   * Sets the memory allocator to use when constructing the Arrow vectors from the ResultSet.
   *
   * @param allocator the allocator to set.
   * @exception NullPointerException if <code>allocator</code> is null.
   */
  public JdbcToArrowConfigBuilder setAllocator(BaseAllocator allocator) {
    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");
    this.allocator = allocator;
    return this;
  }

  /**
   * Sets the {@link Calendar} to use when constructing timestamp fields in the
   * Arrow schema, and reading time-based fields from the JDBC <code>ResultSet</code>.
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
   * @param includeMetadata Whether to include or exclude JDBC metadata in the Arrow Schema field metadata.
   * @return This instance of the <code>JdbcToArrowConfig</code>, for chaining.
   */
  public JdbcToArrowConfigBuilder setIncludeMetadata(boolean includeMetadata) {
    this.includeMetadata = includeMetadata;
    return this;
  }

  /**
   * This builds the {@link JdbcToArrowConfig} from the provided
   * {@link BaseAllocator} and {@link Calendar}.
   *
   * @return The built {@link JdbcToArrowConfig}
   * @throws NullPointerException if either the allocator or calendar was not set.
   */
  public JdbcToArrowConfig build() {
    return new JdbcToArrowConfig(allocator, calendar, includeMetadata);
  }
}
