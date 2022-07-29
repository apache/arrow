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

package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import static org.hamcrest.CoreMatchers.equalTo;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AbstractArrowFlightJdbcListAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private final Supplier<ValueVector> vectorSupplier;
  private ValueVector vector;

  private final AccessorTestUtils.AccessorSupplier<AbstractArrowFlightJdbcListVectorAccessor>
      accessorSupplier = (vector, getCurrentRow) -> {
        ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer = (boolean wasNull) -> {
        };
        if (vector instanceof ListVector) {
          return new ArrowFlightJdbcListVectorAccessor((ListVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof LargeListVector) {
          return new ArrowFlightJdbcLargeListVectorAccessor((LargeListVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof FixedSizeListVector) {
          return new ArrowFlightJdbcFixedSizeListVectorAccessor((FixedSizeListVector) vector,
              getCurrentRow, noOpWasNullConsumer);
        }
        return null;
      };

  final AccessorTestUtils.AccessorIterator<AbstractArrowFlightJdbcListVectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createListVector(), "ListVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createLargeListVector(),
            "LargeListVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createFixedSizeListVector(),
            "FixedSizeListVector"},
    });
  }

  public AbstractArrowFlightJdbcListAccessorTest(Supplier<ValueVector> vectorSupplier,
                                                 String vectorType) {
    this.vectorSupplier = vectorSupplier;
  }

  @Before
  public void setup() {
    this.vector = this.vectorSupplier.get();
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void testShouldGetObjectClassReturnCorrectClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector,
        AbstractArrowFlightJdbcListVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(List.class));
  }

  @Test
  public void testShouldGetObjectReturnValidList() throws Exception {
    accessorIterator.assertAccessorGetter(vector,
        AbstractArrowFlightJdbcListVectorAccessor::getObject,
        (accessor, currentRow) -> equalTo(
            Arrays.asList(0, (currentRow), (currentRow) * 2, (currentRow) * 3, (currentRow) * 4)));
  }

  @Test
  public void testShouldGetObjectReturnNull() throws Exception {
    vector.clear();
    vector.allocateNewSafe();
    vector.setValueCount(5);

    accessorIterator.assertAccessorGetter(vector,
        AbstractArrowFlightJdbcListVectorAccessor::getObject,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetArrayReturnValidArray() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      Array array = accessor.getArray();
      assert array != null;

      Object[] arrayObject = (Object[]) array.getArray();

      collector.checkThat(arrayObject, equalTo(
          new Object[] {0, currentRow, (currentRow) * 2, (currentRow) * 3, (currentRow) * 4}));
    });
  }

  @Test
  public void testShouldGetArrayReturnNull() throws Exception {
    vector.clear();
    vector.allocateNewSafe();
    vector.setValueCount(5);

    accessorIterator.assertAccessorGetter(vector,
        AbstractArrowFlightJdbcListVectorAccessor::getArray,
        CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetArrayReturnValidArrayPassingOffsets() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      Array array = accessor.getArray();
      assert array != null;

      Object[] arrayObject = (Object[]) array.getArray(1, 3);

      collector.checkThat(arrayObject, equalTo(
          new Object[] {currentRow, (currentRow) * 2, (currentRow) * 3}));
    });
  }

  @Test
  public void testShouldGetArrayGetResultSetReturnValidResultSet() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      Array array = accessor.getArray();
      assert array != null;

      try (ResultSet rs = array.getResultSet()) {
        int count = 0;
        while (rs.next()) {
          final int value = rs.getInt(1);
          collector.checkThat(value, equalTo(currentRow * count));
          count++;
        }
        collector.checkThat(count, equalTo(5));
      }
    });
  }
}
