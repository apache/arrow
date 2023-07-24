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

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArrowFlightJdbcBaseIntVectorAccessorUnitTest {

  @ClassRule
  public static RootAllocatorTestRule rule = new RootAllocatorTestRule();
  private static UInt4Vector int4Vector;
  private static UInt8Vector int8Vector;
  private static IntVector intVectorWithNull;
  private static TinyIntVector tinyIntVector;
  private static SmallIntVector smallIntVector;
  private static IntVector intVector;
  private static BigIntVector bigIntVector;
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

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
        } else if (vector instanceof UInt4Vector) {
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
        return null;
      };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcBaseIntVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @BeforeClass
  public static void setup() {
    int4Vector = new UInt4Vector("ID", rule.getRootAllocator());
    int4Vector.setSafe(0, 0x80000001);
    int4Vector.setValueCount(1);

    int8Vector = new UInt8Vector("ID", rule.getRootAllocator());
    int8Vector.setSafe(0, 0xFFFFFFFFFFFFFFFFL);
    int8Vector.setValueCount(1);

    intVectorWithNull = new IntVector("ID", rule.getRootAllocator());
    intVectorWithNull.setNull(0);
    intVectorWithNull.setValueCount(1);

    tinyIntVector = new TinyIntVector("ID", rule.getRootAllocator());
    tinyIntVector.setSafe(0, 0xAA);
    tinyIntVector.setValueCount(1);

    smallIntVector = new SmallIntVector("ID", rule.getRootAllocator());
    smallIntVector.setSafe(0, 0xAABB);
    smallIntVector.setValueCount(1);

    intVector = new IntVector("ID", rule.getRootAllocator());
    intVector.setSafe(0, 0xAABBCCDD);
    intVector.setValueCount(1);

    bigIntVector = new BigIntVector("ID", rule.getRootAllocator());
    bigIntVector.setSafe(0, 0xAABBCCDDEEFFAABBL);
    bigIntVector.setValueCount(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(bigIntVector, intVector, smallIntVector, tinyIntVector, int4Vector,
        int8Vector, intVectorWithNull, rule);
  }

  @Test
  public void testShouldGetStringFromUnsignedValue() throws Exception {
    accessorIterator.assertAccessorGetter(int8Vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getString, equalTo("18446744073709551615"));
  }

  @Test
  public void testShouldGetBytesFromIntVectorThrowsSqlException() throws Exception {
    accessorIterator.assertAccessorGetterThrowingException(intVector, ArrowFlightJdbcBaseIntVectorAccessor::getBytes);
  }

  @Test
  public void testShouldGetStringFromIntVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(intVectorWithNull,
        ArrowFlightJdbcBaseIntVectorAccessor::getString, CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetObjectFromInt() throws Exception {
    accessorIterator.assertAccessorGetter(intVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getObject, equalTo(0xAABBCCDD));
  }

  @Test
  public void testShouldGetObjectFromTinyInt() throws Exception {
    accessorIterator.assertAccessorGetter(tinyIntVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getObject, equalTo((byte) 0xAA));
  }

  @Test
  public void testShouldGetObjectFromSmallInt() throws Exception {
    accessorIterator.assertAccessorGetter(smallIntVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getObject, equalTo((short) 0xAABB));
  }

  @Test
  public void testShouldGetObjectFromBigInt() throws Exception {
    accessorIterator.assertAccessorGetter(bigIntVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getObject, equalTo(0xAABBCCDDEEFFAABBL));
  }

  @Test
  public void testShouldGetObjectFromUnsignedInt() throws Exception {
    accessorIterator.assertAccessorGetter(int4Vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getObject, equalTo(0x80000001));
  }

  @Test
  public void testShouldGetObjectFromIntVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(intVectorWithNull,
        ArrowFlightJdbcBaseIntVectorAccessor::getObject, CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetBigDecimalFromIntVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(intVectorWithNull,
        ArrowFlightJdbcBaseIntVectorAccessor::getBigDecimal, CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetBigDecimalWithScaleFromIntVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(intVectorWithNull, accessor -> accessor.getBigDecimal(2),
        CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetBytesFromSmallVectorThrowsSqlException() throws Exception {
    accessorIterator.assertAccessorGetterThrowingException(smallIntVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getBytes);
  }

  @Test
  public void testShouldGetBytesFromTinyIntVectorThrowsSqlException() throws Exception {
    accessorIterator.assertAccessorGetterThrowingException(tinyIntVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getBytes);
  }

  @Test
  public void testShouldGetBytesFromBigIntVectorThrowsSqlException() throws Exception {
    accessorIterator.assertAccessorGetterThrowingException(bigIntVector,
        ArrowFlightJdbcBaseIntVectorAccessor::getBytes);
  }
}
