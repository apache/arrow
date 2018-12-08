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
 */
public final class JdbcToArrowConfig {
  private Calendar calendar;
  private BaseAllocator allocator;

  public JdbcToArrowConfig(BaseAllocator allocator, Calendar calendar) {
    Preconditions.checkNotNull(allocator, "Memory allocator cannot be null");
    Preconditions.checkNotNull(calendar, "Calendar object can not be null");

    this.allocator = allocator;
    this.calendar = calendar;
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
   * @param calendar the calendar to set.
   */
  public void setCalendar(Calendar calendar) {
    this.calendar = calendar;
  }

  /**
   * The Arrow memory allocator.
   * @return the allocator.
   */
  public BaseAllocator getAllocator() {
    return allocator;
  }

  /**
   * @param allocator the allocator to set.
   */
  public void setAllocator(BaseAllocator allocator) {
    this.allocator = allocator;
  }

  public boolean isValid() {
    return (calendar != null) || (allocator != null);
  }
}
