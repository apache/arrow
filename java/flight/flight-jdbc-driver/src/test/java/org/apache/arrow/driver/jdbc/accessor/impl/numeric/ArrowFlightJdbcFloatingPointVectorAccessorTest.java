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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.FloatingPointVector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArrowFlightJdbcFloatingPointVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private FloatingPointVector vector;
  private final Supplier<FloatingPointVector> vectorSupplier;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcFloatingPointVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcFloatingPointVectorAccessor((FloatingPointVector) vector,
          getCurrentRow);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {
            (Supplier<FloatingPointVector>) () -> rootAllocatorTestRule.createFloat8Vector(), "Float8Vector"},
        {
            (Supplier<FloatingPointVector>) () -> rootAllocatorTestRule.createFloat4Vector(), "Float4Vector"},
    });
  }

  public ArrowFlightJdbcFloatingPointVectorAccessorTest(Supplier<FloatingPointVector> vectorSupplier,
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
  public void getDouble() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          double doubleValue = accessor.getDouble();

          collector.checkThat(doubleValue, is(vector.getValueAsDouble(currentRow)));
        });
  }


  @Test
  public void getObject() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          double doubleValue = accessor.getDouble();
          Object object = accessor.getObject();

          collector.checkThat(object, instanceOf(Double.class));
          collector.checkThat(object, is(doubleValue));
        });
  }


  @Test
  public void getString() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getString(), is(Double.toString(accessor.getDouble())));
        });
  }


  @Test
  public void getBoolean() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBoolean(), is(accessor.getDouble() != 0.0));
        });
  }


  @Test
  public void getByte() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getByte(), is((byte) accessor.getDouble()));
        });
  }


  @Test
  public void getShort() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getShort(), is((short) accessor.getDouble()));
        });
  }


  @Test
  public void getInt() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getInt(), is((int) accessor.getDouble()));
        });
  }


  @Test
  public void getLong() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getLong(), is((long) accessor.getDouble()));
        });
  }


  @Test
  public void getFloat() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getFloat(), is((float) accessor.getDouble()));
        });
  }


  @Test
  public void getBigDecimal() throws SQLException {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          double value = accessor.getDouble();
          if (Double.isInfinite(value)) {
            // BigDecimal does not support Infinities
            return;
          }
          collector.checkThat(accessor.getBigDecimal(), is(BigDecimal.valueOf(value)));
        });
  }
}
