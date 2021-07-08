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

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
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
public class ArrowFlightJdbcBaseIntVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private BaseIntVector vector;
  private final Supplier<BaseIntVector> vectorSupplier;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBaseIntVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> {
        if (vector instanceof UInt1Vector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((UInt1Vector) vector, getCurrentRow);
        } else if (vector instanceof UInt2Vector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((UInt2Vector) vector, getCurrentRow);
        } else if (vector instanceof UInt4Vector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((UInt4Vector) vector, getCurrentRow);
        } else if (vector instanceof UInt8Vector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((UInt8Vector) vector, getCurrentRow);
        } else if (vector instanceof TinyIntVector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((TinyIntVector) vector, getCurrentRow);
        } else if (vector instanceof SmallIntVector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((SmallIntVector) vector, getCurrentRow);
        } else if (vector instanceof IntVector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((IntVector) vector, getCurrentRow);
        } else if (vector instanceof BigIntVector) {
          return new ArrowFlightJdbcBaseIntVectorAccessor((BigIntVector) vector, getCurrentRow);
        }
        throw new UnsupportedOperationException();
      };

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createIntVector(), "IntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createSmallIntVector(), "SmallIntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createTinyIntVector(), "TinyIntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createBigIntVector(), "BigIntVector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt1Vector(), "UInt1Vector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt2VectorVector(), "UInt2Vector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt4Vector(), "UInt4Vector"},
        {(Supplier<BaseIntVector>) () -> rootAllocatorTestRule.createUInt8Vector(), "UInt8Vector"}
    });
  }

  public ArrowFlightJdbcBaseIntVectorAccessorTest(Supplier<BaseIntVector> vectorSupplier, String vectorType) {
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
  public void testShouldGetLongMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final long result = accessor.getLong();

          collector.checkThat(result, instanceOf(long.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetIntMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final int result = accessor.getInt();

          collector.checkThat(result, instanceOf(int.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetShortMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final short result = accessor.getShort();

          collector.checkThat(result, instanceOf(short.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetByteMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final byte result = accessor.getByte();

          collector.checkThat(result, instanceOf(byte.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetStringMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final String result = accessor.getString();

          collector.checkThat(result, instanceOf(String.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final Object result = accessor.getObject();

          collector.checkThat(result, instanceOf(Object.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetBytesMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final byte[] result = accessor.getBytes();

          collector.checkThat(result, instanceOf(byte[].class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetFloatMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float result = accessor.getFloat();

          collector.checkThat(result, instanceOf(float.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetDoubleMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double result = accessor.getDouble();

          collector.checkThat(result, instanceOf(double.class));
          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToByteMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final long result = accessor.getLong();
          final byte secondResult = accessor.getByte();

          collector.checkThat(result, instanceOf(long.class));
          collector.checkThat(secondResult, equalTo((byte) result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToShortMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final long result = accessor.getLong();
          final short secondResult = accessor.getShort();

          collector.checkThat(result, instanceOf(long.class));
          collector.checkThat(secondResult, equalTo((short) result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToIntegerMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final long result = accessor.getLong();
          final int secondResult = accessor.getInt();

          collector.checkThat(result, instanceOf(long.class));
          collector.checkThat(secondResult, equalTo((int) result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }
}
