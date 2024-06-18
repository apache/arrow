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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

public class JdbcToArrowConfigTest {

  private static final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
  private static final Calendar calendar =
      Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

  @Test
  public void testConfigNullArguments() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new JdbcToArrowConfig(null, null);
        });
  }

  @Test
  public void testBuilderNullArguments() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new JdbcToArrowConfigBuilder(null, null);
        });
  }

  @Test
  public void testConfigNullCalendar() {
    JdbcToArrowConfig config = new JdbcToArrowConfig(allocator, null);
    assertNull(config.getCalendar());
  }

  @Test
  public void testBuilderNullCalendar() {
    JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder(allocator, null);
    JdbcToArrowConfig config = builder.build();
    assertNull(config.getCalendar());
  }

  @Test
  public void testConfigNullAllocator() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new JdbcToArrowConfig(null, calendar);
        });
  }

  @Test
  public void testBuilderNullAllocator() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new JdbcToArrowConfigBuilder(null, calendar);
        });
  }

  @Test
  public void testSetNullAllocator() {
    assertThrows(
        NullPointerException.class,
        () -> {
          JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder(allocator, calendar);
          builder.setAllocator(null);
        });
  }

  @Test
  public void testSetNullCalendar() {
    JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder(allocator, calendar);
    JdbcToArrowConfig config = builder.setCalendar(null).build();
    assertNull(config.getCalendar());
  }

  @Test
  public void testConfig() {
    JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder(allocator, calendar);
    JdbcToArrowConfig config = builder.build();

    assertEquals(allocator, config.getAllocator());
    assertEquals(calendar, config.getCalendar());

    Calendar newCalendar = Calendar.getInstance();
    BufferAllocator newAllocator = new RootAllocator(Integer.SIZE);

    builder.setAllocator(newAllocator).setCalendar(newCalendar);
    config = builder.build();

    assertEquals(newAllocator, config.getAllocator());
    assertEquals(newCalendar, config.getCalendar());
  }

  @Test
  public void testIncludeMetadata() {
    JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder(allocator, calendar, false);

    JdbcToArrowConfig config = builder.build();
    assertFalse(config.shouldIncludeMetadata());

    builder.setIncludeMetadata(true);
    config = builder.build();
    assertTrue(config.shouldIncludeMetadata());

    config = new JdbcToArrowConfigBuilder(allocator, calendar, true).build();
    assertTrue(config.shouldIncludeMetadata());

    config =
        new JdbcToArrowConfig(
            allocator,
            calendar, /* include metadata */
            true,
            /* reuse vector schema root */ true,
            null,
            null,
            JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE,
            null);
    assertTrue(config.shouldIncludeMetadata());
    assertTrue(config.isReuseVectorSchemaRoot());

    config =
        new JdbcToArrowConfig(
            allocator,
            calendar, /* include metadata */
            false,
            /* reuse vector schema root */ false,
            null,
            null,
            JdbcToArrowConfig.NO_LIMIT_BATCH_SIZE,
            null);
    assertFalse(config.shouldIncludeMetadata());
    assertFalse(config.isReuseVectorSchemaRoot());
  }

  @Test
  public void testArraySubTypes() {
    JdbcToArrowConfigBuilder builder = new JdbcToArrowConfigBuilder(allocator, calendar, false);
    JdbcToArrowConfig config = builder.build();

    final int columnIndex = 1;
    final String columnName = "COLUMN";

    assertNull(config.getArraySubTypeByColumnIndex(columnIndex));
    assertNull(config.getArraySubTypeByColumnName(columnName));

    final HashMap<Integer, JdbcFieldInfo> indexMapping = new HashMap<Integer, JdbcFieldInfo>();
    indexMapping.put(2, new JdbcFieldInfo(Types.BIGINT));

    final HashMap<String, JdbcFieldInfo> fieldMapping = new HashMap<String, JdbcFieldInfo>();
    fieldMapping.put("NEW_COLUMN", new JdbcFieldInfo(Types.BINARY));

    builder.setArraySubTypeByColumnIndexMap(indexMapping);
    builder.setArraySubTypeByColumnNameMap(fieldMapping);
    config = builder.build();

    assertNull(config.getArraySubTypeByColumnIndex(columnIndex));
    assertNull(config.getArraySubTypeByColumnName(columnName));

    indexMapping.put(columnIndex, new JdbcFieldInfo(Types.BIT));
    fieldMapping.put(columnName, new JdbcFieldInfo(Types.BLOB));

    assertNotNull(config.getArraySubTypeByColumnIndex(columnIndex));
    assertEquals(Types.BIT, config.getArraySubTypeByColumnIndex(columnIndex).getJdbcType());
    assertEquals(Types.BLOB, config.getArraySubTypeByColumnName(columnName).getJdbcType());
  }
}
