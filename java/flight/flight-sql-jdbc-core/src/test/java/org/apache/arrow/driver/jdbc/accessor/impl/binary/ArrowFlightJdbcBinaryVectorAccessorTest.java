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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;

import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
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

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBinaryVectorAccessor>
      accessorSupplier = (vector, getCurrentRow) -> {
        ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer = (boolean wasNull) -> {
        };
        if (vector instanceof VarBinaryVector) {
          return new ArrowFlightJdbcBinaryVectorAccessor(((VarBinaryVector) vector), getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof LargeVarBinaryVector) {
          return new ArrowFlightJdbcBinaryVectorAccessor(((LargeVarBinaryVector) vector),
              getCurrentRow, noOpWasNullConsumer);
        } else if (vector instanceof FixedSizeBinaryVector) {
          return new ArrowFlightJdbcBinaryVectorAccessor(((FixedSizeBinaryVector) vector),
              getCurrentRow, noOpWasNullConsumer);
        }
        return null;
      };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcBinaryVectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createVarBinaryVector(),
            "VarBinaryVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createLargeVarBinaryVector(),
            "LargeVarBinaryVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createFixedSizeBinaryVector(),
            "FixedSizeBinaryVector"},
    });
  }

  public ArrowFlightJdbcBinaryVectorAccessorTest(Supplier<ValueVector> vectorSupplier,
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
  public void testShouldGetStringReturnExpectedString() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBinaryVectorAccessor::getString,
        (accessor) -> is(new String(accessor.getBytes(), UTF_8)));
  }

  @Test
  public void testShouldGetStringReturnNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    accessorIterator
        .assertAccessorGetter(vector, ArrowFlightJdbcBinaryVectorAccessor::getString,
            CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetBytesReturnExpectedByteArray() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBinaryVectorAccessor::getBytes,
        (accessor, currentRow) -> {
          if (vector instanceof VarBinaryVector) {
            return is(((VarBinaryVector) vector).get(currentRow));
          } else if (vector instanceof LargeVarBinaryVector) {
            return is(((LargeVarBinaryVector) vector).get(currentRow));
          } else if (vector instanceof FixedSizeBinaryVector) {
            return is(((FixedSizeBinaryVector) vector).get(currentRow));
          }
          return null;
        });
  }

  @Test
  public void testShouldGetBytesReturnNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getBytes(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testShouldGetObjectReturnAsGetBytes() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcBinaryVectorAccessor::getObject,
        (accessor) -> is(accessor.getBytes()));
  }

  @Test
  public void testShouldGetObjectReturnNull() {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getObject(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testShouldGetUnicodeStreamReturnCorrectInputStream() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      InputStream inputStream = accessor.getUnicodeStream();
      String actualString = IOUtils.toString(inputStream, UTF_8);
      collector.checkThat(accessor.wasNull(), is(false));
      collector.checkThat(actualString, is(accessor.getString()));
    });
  }

  @Test
  public void testShouldGetUnicodeStreamReturnNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getUnicodeStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testShouldGetAsciiStreamReturnCorrectInputStream() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      InputStream inputStream = accessor.getAsciiStream();
      String actualString = IOUtils.toString(inputStream, US_ASCII);
      collector.checkThat(accessor.wasNull(), is(false));
      collector.checkThat(actualString, is(accessor.getString()));
    });
  }

  @Test
  public void testShouldGetAsciiStreamReturnNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getAsciiStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testShouldGetBinaryStreamReturnCurrentInputStream() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      InputStream inputStream = accessor.getBinaryStream();
      String actualString = IOUtils.toString(inputStream, UTF_8);
      collector.checkThat(accessor.wasNull(), is(false));
      collector.checkThat(actualString, is(accessor.getString()));
    });
  }

  @Test
  public void testShouldGetBinaryStreamReturnNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getBinaryStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testShouldGetCharacterStreamReturnCorrectReader() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      Reader characterStream = accessor.getCharacterStream();
      String actualString = IOUtils.toString(characterStream);
      collector.checkThat(accessor.wasNull(), is(false));
      collector.checkThat(actualString, is(accessor.getString()));
    });
  }

  @Test
  public void testShouldGetCharacterStreamReturnNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getCharacterStream(), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }
}
