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

package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class ArrowFlightJdbcBaseIntVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private BaseIntVector vector;
  private final Supplier<BaseIntVector> vectorSupplier;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBaseIntVectorAccessor>
      accessorSupplier = (vector, getCurrentRow) -> {
        ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer = (boolean wasNull) -> {
        };
        if (vector instanceof UInt1Vector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((UInt1Vector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof UInt2Vector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((UInt2Vector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else {
          if (vector instanceof UInt4Vector) {
            return new ArrowFlightJdbcBaseIntVectorAccessor((UInt4Vector) vector, getCurrentRow,
                noOpWasNullConsumer);
          } else if (vector instanceof UInt8Vector) {
            return new ArrowFlightJdbcBaseIntVectorAccessor((UInt8Vector) vector, getCurrentRow,
                noOpWasNullConsumer);
          } else if (vector instanceof TinyIntVector) {
            return new ArrowFlightJdbcBaseIntVectorAccessor((TinyIntVector) vector, getCurrentRow,
                noOpWasNullConsumer);
          } else if (vector instanceof SmallIntVector) {
            return new ArrowFlightJdbcBaseIntVectorAccessor((SmallIntVector) vector, getCurrentRow,
                noOpWasNullConsumer);
          } else if (vector instanceof IntVector) {
            return new ArrowFlightJdbcBaseIntVectorAccessor((IntVector) vector, getCurrentRow,
                noOpWasNullConsumer);
          } else if (vector instanceof BigIntVector) {
            return new ArrowFlightJdbcBaseIntVectorAccessor((BigIntVector) vector, getCurrentRow,
                noOpWasNullConsumer);
          }
        }
        throw new UnsupportedOperationException();
      };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcBaseIntVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createIntVector(), "IntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createSmallIntVector(),
            "SmallIntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createTinyIntVector(),
            "TinyIntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createBigIntVector(),
            "BigIntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt1Vector(), "UInt1Vector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt2Vector(), "UInt2Vector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt4Vector(), "UInt4Vector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt8Vector(), "UInt8Vector"}
    });
  }

  public ArrowFlightJdbcBaseIntVectorAccessorTest(Supplier<BaseIntVector> vectorSupplier,
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
  public void testShouldConvertToByteMethodFromBaseIntVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBaseIntVectorAccessor::getByte,
        (accessor, currentRow) -> equalTo((byte) accessor.getLong()));
  }

  @Test
  public void testShouldConvertToShortMethodFromBaseIntVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBaseIntVectorAccessor::getShort,
        (accessor, currentRow) -> equalTo((short) accessor.getLong()));
  }

  @Test
  public void testShouldConvertToIntegerMethodFromBaseIntVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBaseIntVectorAccessor::getInt,
        (accessor, currentRow) -> equalTo((int) accessor.getLong()));
  }

  @Test
  public void testShouldConvertToFloatMethodFromBaseIntVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBaseIntVectorAccessor::getFloat,
        (accessor, currentRow) -> equalTo((float) accessor.getLong()));
  }

  @Test
  public void testShouldConvertToDoubleMethodFromBaseIntVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBaseIntVectorAccessor::getDouble,
        (accessor, currentRow) -> equalTo((double) accessor.getLong()));
  }

  @Test
  public void testShouldConvertToBooleanMethodFromBaseIntVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBaseIntVectorAccessor::getBoolean,
        (accessor, currentRow) -> equalTo(accessor.getLong() != 0L));
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getObjectClass,
        equalTo(Long.class));
  }
}
