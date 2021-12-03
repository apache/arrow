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
import java.time.Period;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.ValueVector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArrowFlightJdbcIntervalVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private final Supplier<ValueVector> vectorSupplier;
  private ValueVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcIntervalVectorAccessor>
      accessorSupplier = (vector, getCurrentRow) -> {
        ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer = (boolean wasNull) -> {
        };
        if (vector instanceof IntervalDayVector) {
          return new ArrowFlightJdbcIntervalVectorAccessor((IntervalDayVector) vector,
              getCurrentRow, noOpWasNullConsumer);
        } else if (vector instanceof IntervalYearVector) {
          return new ArrowFlightJdbcIntervalVectorAccessor((IntervalYearVector) vector,
              getCurrentRow, noOpWasNullConsumer);
        }
        return null;
      };

  final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcIntervalVectorAccessor> accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<ValueVector>) () -> {
          IntervalDayVector vector =
              new IntervalDayVector("", rootAllocatorTestRule.getRootAllocator());

          int valueCount = 10;
          vector.setValueCount(valueCount);
          for (int i = 0; i < valueCount; i++) {
            vector.set(i, i + 1, (i + 1) * 1000);
          }
          return vector;
        }, "IntervalDayVector"},
        {(Supplier<ValueVector>) () -> {
          IntervalYearVector vector =
              new IntervalYearVector("", rootAllocatorTestRule.getRootAllocator());

          int valueCount = 10;
          vector.setValueCount(valueCount);
          for (int i = 0; i < valueCount; i++) {
            vector.set(i, i + 1);
          }
          return vector;
        }, "IntervalYearVector"},
    });
  }

  public ArrowFlightJdbcIntervalVectorAccessorTest(Supplier<ValueVector> vectorSupplier,
                                                   String vectorType) {
    this.vectorSupplier = vectorSupplier;
  }

  @Before
  public void setup() {
    this.vector = vectorSupplier.get();
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void testShouldGetObjectReturnValidObject() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcIntervalVectorAccessor::getObject,
        (accessor, currentRow) -> is(getExpectedObject(vector, currentRow)));
  }

  @Test
  public void testShouldGetObjectPassingObjectClassAsParameterReturnValidObject() throws Exception {
    Class<?> objectClass = getExpectedObjectClassForVector(vector);
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(objectClass),
        (accessor, currentRow) -> is(getExpectedObject(vector, currentRow)));
  }

  @Test
  public void testShouldGetObjectReturnNull() throws Exception {
    setAllNullOnVector(vector);
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcIntervalVectorAccessor::getObject,
        (accessor, currentRow) -> equalTo(null));
  }

  private String getStringOnVector(ValueVector vector, int index) {
    if (vector instanceof IntervalDayVector) {
      return ((IntervalDayVector) vector).getAsStringBuilder(index).toString();
    } else if (vector instanceof IntervalYearVector) {
      return ((IntervalYearVector) vector).getAsStringBuilder(index).toString();
    }
    return null;
  }

  @Test
  public void testShouldGetStringReturnCorrectString() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcIntervalVectorAccessor::getString,
        (accessor, currentRow) -> is(getStringOnVector(vector, currentRow)));
  }

  @Test
  public void testShouldGetStringReturnNull() throws Exception {
    setAllNullOnVector(vector);
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcIntervalVectorAccessor::getString,
        (accessor, currentRow) -> equalTo(null));
  }

  @Test
  public void testShouldGetObjectClassReturnCorrectClass() throws Exception {
    Class<?> expectedObjectClass = getExpectedObjectClassForVector(vector);
    accessorIterator.assertAccessorGetter(vector,
        ArrowFlightJdbcIntervalVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(expectedObjectClass));
  }

  private Class<?> getExpectedObjectClassForVector(ValueVector vector) {
    if (vector instanceof IntervalDayVector) {
      return Duration.class;
    } else if (vector instanceof IntervalYearVector) {
      return Period.class;
    }
    return null;
  }

  private void setAllNullOnVector(ValueVector vector) {
    int valueCount = vector.getValueCount();
    if (vector instanceof IntervalDayVector) {
      for (int i = 0; i < valueCount; i++) {
        ((IntervalDayVector) vector).setNull(i);
      }
    } else if (vector instanceof IntervalYearVector) {
      for (int i = 0; i < valueCount; i++) {
        ((IntervalYearVector) vector).setNull(i);
      }
    }
  }

  private Object getExpectedObject(ValueVector vector, int currentRow) {
    if (vector instanceof IntervalDayVector) {
      return Duration.ofDays(currentRow + 1).plusMillis((currentRow + 1) * 1000L);
    } else if (vector instanceof IntervalYearVector) {
      return Period.ofMonths(currentRow + 1);
    }
    return null;
  }
}
