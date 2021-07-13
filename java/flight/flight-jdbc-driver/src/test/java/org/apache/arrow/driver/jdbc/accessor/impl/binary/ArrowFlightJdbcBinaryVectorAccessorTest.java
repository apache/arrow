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

package org.apache.arrow.driver.jdbc.accessor.impl.binary;

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.is;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.commons.io.IOUtils;
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
public class ArrowFlightJdbcBinaryVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private ValueVector vector;
  private final Supplier<ValueVector> vectorSupplier;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBinaryVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> {
        if (vector instanceof VarBinaryVector) {
          return new ArrowFlightJdbcBinaryVectorAccessor(((VarBinaryVector) vector), getCurrentRow);
        } else if (vector instanceof LargeVarBinaryVector) {
          return new ArrowFlightJdbcBinaryVectorAccessor(((LargeVarBinaryVector) vector), getCurrentRow);
        } else if (vector instanceof FixedSizeBinaryVector) {
          return new ArrowFlightJdbcBinaryVectorAccessor(((FixedSizeBinaryVector) vector), getCurrentRow);
        }
        return null;
      };

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createVarBinaryVector(), "VarBinaryVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createLargeVarBinaryVector(), "LargeVarBinaryVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createFixedSizeBinaryVector(), "FixedSizeBinaryVector"},
    });
  }

  public ArrowFlightJdbcBinaryVectorAccessorTest(Supplier<ValueVector> vectorSupplier, String vectorType) {
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
  public void getString() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          String expected = new String(accessor.getBytes(), StandardCharsets.UTF_8);
          collector.checkThat(accessor.wasNull(), is(false));
          collector.checkThat(accessor.getString(), is(expected));
        });
  }

  @Test
  public void getStringForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getString(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getBytes() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          if (vector instanceof VarBinaryVector) {
            collector.checkThat(accessor.getBytes(), is(((VarBinaryVector) vector).get(currentRow)));
          } else if (vector instanceof LargeVarBinaryVector) {
            collector.checkThat(accessor.getBytes(), is(((LargeVarBinaryVector) vector).get(currentRow)));
          }
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getBytesForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getBytes(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getObject() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getObject(), is(accessor.getBytes()));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getObjectForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getObject(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getAsciiStream() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          InputStream inputStream = accessor.getAsciiStream();
          String actualString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
          collector.checkThat(accessor.wasNull(), is(false));
          collector.checkThat(actualString, is(accessor.getString()));
        });
  }

  @Test
  public void getAsciiStreamForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getAsciiStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getBinaryStream() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          InputStream inputStream = accessor.getBinaryStream();
          String actualString = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
          collector.checkThat(accessor.wasNull(), is(false));
          collector.checkThat(actualString, is(accessor.getString()));
        });
  }

  @Test
  public void getBinaryStreamForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getBinaryStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getCharacterStream() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          Reader characterStream = accessor.getCharacterStream();
          String actualString = IOUtils.toString(characterStream);
          collector.checkThat(accessor.wasNull(), is(false));
          collector.checkThat(actualString, is(accessor.getString()));
        });
  }

  @Test
  public void getCharacterStreamForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getCharacterStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }
}
