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

package org.apache.arrow.driver.jdbc.accessor.impl.calendar;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.time.Duration;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcDurationVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private DurationVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcDurationVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> new ArrowFlightJdbcDurationVectorAccessor((DurationVector) vector,
              getCurrentRow, (boolean wasNull) -> {
          });

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcDurationVectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Before
  public void setup() {
    FieldType fieldType = new FieldType(true, new ArrowType.Duration(TimeUnit.MILLISECOND), null);
    this.vector = new DurationVector("", fieldType, rootAllocatorTestRule.getRootAllocator());

    int valueCount = 10;
    this.vector.setValueCount(valueCount);
    for (int i = 0; i < valueCount; i++) {
      this.vector.set(i, java.util.concurrent.TimeUnit.DAYS.toMillis(i + 1));
    }
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void getObject() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDurationVectorAccessor::getObject,
        (accessor, currentRow) -> is(Duration.ofDays(currentRow + 1)));
  }

  @Test
  public void getObjectForNull() throws Exception {
    int valueCount = vector.getValueCount();
    for (int i = 0; i < valueCount; i++) {
      vector.setNull(i);
    }

    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDurationVectorAccessor::getObject,
        (accessor, currentRow) -> equalTo(null));
  }

  @Test
  public void getString() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcAccessor::getString,
        (accessor, currentRow) -> is(Duration.ofDays(currentRow + 1).toString()));
  }

  @Test
  public void getStringForNull() throws Exception {
    int valueCount = vector.getValueCount();
    for (int i = 0; i < valueCount; i++) {
      vector.setNull(i);
    }

    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcAccessor::getString,
        (accessor, currentRow) -> equalTo(null));
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(Duration.class));
  }
}
