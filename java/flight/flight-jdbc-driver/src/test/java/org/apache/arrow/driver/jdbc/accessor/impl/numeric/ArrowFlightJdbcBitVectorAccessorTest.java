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
import static org.hamcrest.CoreMatchers.*;

import java.math.BigDecimal;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.BitVector;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcBitVectorAccessorTest {
  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private BitVector vector;
  private BitVector vectorWithNull;
  private boolean[] arrayToAssert;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBitVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcBitVectorAccessor((BitVector) vector, getCurrentRow);

  @Before
  public void setup() {

    this.arrayToAssert = new boolean[]{false, true};
    this.vector = rootAllocatorTestRule.createBitVector();
    this.vectorWithNull = rootAllocatorTestRule.createBitVectorForNullTests();
  }

  @After
  public void tearDown() {
    this.vector.close();
    this.vectorWithNull.close();
  }

  @Test
  public void testShouldGetBooleanMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final boolean value = accessor.getBoolean();
          collector.checkThat(value, is(arrayToAssert[currentRow]));
        })
    );
  }

  @Test
  public void testShouldGetByteMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final byte value = accessor.getByte();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? (byte) 1 : (byte) 0));
        })
    );
  }

  @Test
  public void testShouldGetShortMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final short value = accessor.getShort();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? (short) 1 : (short) 0));
        })
    );
  }

  @Test
  public void testShouldGetIntMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final int value = accessor.getInt();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? 1 : 0));
        })
    );
  }

  @Test
  public void testShouldGetLongMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final long value = accessor.getLong();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? (long) 1 : (long) 0));
        })
    );
  }

  @Test
  public void testShouldGetFloatMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final float value = accessor.getFloat();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? (float) 1 : (float) 0));
        })
    );
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final double value = accessor.getDouble();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? (double) 1 : (double) 0));
        })
    );
  }

  @Test
  public void testShouldGetBytesMethodFromFloat4Vector() throws Exception {
    byte[][] bytes = new byte[][] {{0x0}, {0x1}};

    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final byte[] value = accessor.getBytes();

          collector.checkThat(value, CoreMatchers.is(bytes[currentRow]));
        })
    );
  }

  @Test
  public void testShouldGetBytesMethodFromFloat4VectorFromNll() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        ((accessor, currentRow) -> {
          final byte[] value = accessor.getBytes();

          collector.checkThat(value, nullValue());
        })
    );
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final BigDecimal value = accessor.getBigDecimal();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? BigDecimal.ONE : BigDecimal.ZERO));
        })
    );
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4VectorFromNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        ((accessor, currentRow) -> {
          final BigDecimal value = accessor.getBigDecimal();

          collector.checkThat(value, nullValue());
        })
    );
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final Object value = accessor.getObject();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? 1L : 0L));
        })
    );
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4VectorFromNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        ((accessor, currentRow) -> {
          final Object value = accessor.getObject();

          collector.checkThat(value, nullValue());
        })
    );
  }

  @Test
  public void testShouldGetStringMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final String value = accessor.getString();

          collector.checkThat(value, is(arrayToAssert[currentRow] ? "true" : "false"));
        })
    );
  }

  @Test
  public void testShouldGetStringMethodFromFloat4VectorFromNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        ((accessor, currentRow) -> {
          final String value = accessor.getString();

          collector.checkThat(value, nullValue());
        })
    );
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {

          collector.checkThat(accessor.getObjectClass(), equalTo(Long.class));
        });
  }
}
