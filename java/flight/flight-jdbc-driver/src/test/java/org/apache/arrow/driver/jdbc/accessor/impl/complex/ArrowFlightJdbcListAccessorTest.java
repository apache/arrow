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

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArrowFlightJdbcListAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private final Supplier<ValueVector> vectorSupplier;
  private ValueVector vector;

  private final AccessorTestUtils.AccessorSupplier<AbstractArrowFlightJdbcListVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> {
        if (vector instanceof ListVector) {
          return new ArrowFlightJdbcListVectorAccessor((ListVector) vector, getCurrentRow);
        } else if (vector instanceof LargeListVector) {
          return new ArrowFlightJdbcLargeListVectorAccessor((LargeListVector) vector, getCurrentRow);
        } else if (vector instanceof FixedSizeListVector) {
          return new ArrowFlightJdbcFixedSizeListVectorAccessor((FixedSizeListVector) vector, getCurrentRow);
        }
        return null;
      };

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createListVector(), "ListVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createLargeListVector(), "LargeListVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createFixedSizeListVector(), "FixedSizeListVector"},
    });
  }

  public ArrowFlightJdbcListAccessorTest(Supplier<ValueVector> vectorSupplier, String vectorType) {
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
  public void test() throws Exception {
    iterateOnAccessor(vector, accessorSupplier, (
        (accessor, currentRow) -> {
          final Object array = accessor.getObject();
          System.out.println(array.toString());
        })
    );
  }


  @Test
  public void testArray() throws Exception {
    iterateOnAccessor(vector, accessorSupplier, (
        (accessor, currentRow) -> {
          Array array = accessor.getArray();
          final Object[] array2 = (Object[]) array.getArray(1, 4);
          System.out.println(Arrays.asList(array2));
        })
    );
  }

  @Test
  public void test2() throws Exception {
    iterateOnAccessor(vector, accessorSupplier, (
        (accessor, currentRow) -> {
          Array array = accessor.getArray();
          try (ResultSet rs = array.getResultSet()) {
            System.out.println("start list " + currentRow);
            while (rs.next()) {
              final int value = rs.getInt(1);
              System.out.print(value);
              System.out.print(", ");
            }
            System.out.println("\nend list " + currentRow);
            System.out.println(array.toString());
          }
        })
    );
  }
}
