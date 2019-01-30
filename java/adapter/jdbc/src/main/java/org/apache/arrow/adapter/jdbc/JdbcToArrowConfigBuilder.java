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

public class JdbcToArrowConfigBuilder {
  private Calendar calendar;
  private BaseAllocator allocator;

  public JdbcToArrowConfigBuilder() {
  }

  public JdbcToArrowConfigBuilder(BaseAllocator allocator, Calendar calendar) {
    this();

    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    this.allocator = allocator;
    this.calendar = calendar;
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
   * @exception NullPointerExeption if <code>calendar</code> is <code>null</code>.
   */
  public JdbcToArrowConfigBuilder setCalendar(Calendar calendar) {
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");
    this.calendar = calendar;
    return this;
  }

  public JdbcToArrowConfig build() {
    return new JdbcToArrowConfig(allocator, calendar);
  }
}
