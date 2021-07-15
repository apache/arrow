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

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.accessorToObjectList;
import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.ArrowFlightJdbcNullVectorAccessor;
import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcUnionVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private UnionVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcUnionVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcUnionVectorAccessor((UnionVector) vector, getCurrentRow);

  @Before
  public void setup() {
    this.vector = UnionVector.empty("", rootAllocatorTestRule.getRootAllocator());
    this.vector.allocateNew();

    NullableBigIntHolder nullableBigIntHolder = new NullableBigIntHolder();
    nullableBigIntHolder.isSet = 1;
    nullableBigIntHolder.value = Long.MAX_VALUE;
    this.vector.setType(0, Types.MinorType.BIGINT);
    this.vector.setSafe(0, nullableBigIntHolder);

    NullableFloat8Holder nullableFloat4Holder = new NullableFloat8Holder();
    nullableFloat4Holder.isSet = 1;
    nullableFloat4Holder.value = Math.PI;
    this.vector.setType(1, Types.MinorType.FLOAT8);
    this.vector.setSafe(1, nullableFloat4Holder);

    NullableTimeStampMilliHolder nullableTimeStampMilliHolder = new NullableTimeStampMilliHolder();
    nullableTimeStampMilliHolder.isSet = 1;
    nullableTimeStampMilliHolder.value = 1625702400000L;
    this.vector.setType(2, Types.MinorType.TIMESTAMPMILLI);
    this.vector.setSafe(2, nullableTimeStampMilliHolder);

//    NullableIntervalDayHolder nullableIntervalDayHolder = new NullableIntervalDayHolder();
//    nullableIntervalDayHolder.isSet = 1;
//    nullableIntervalDayHolder.days = 7;
//    nullableIntervalDayHolder.milliseconds = 100;
//    this.vector.setType(3, Types.MinorType.INTERVALDAY);
//    this.vector.setSafe(3, nullableIntervalDayHolder);

    nullableBigIntHolder.isSet = 0;
    this.vector.setType(4, Types.MinorType.BIGINT);
    this.vector.setSafe(4, nullableBigIntHolder);

    this.vector.setValueCount(6);
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void getObject() throws Exception {
    List<Object> result = accessorToObjectList(vector, accessorSupplier);
    List<Object> expected = Arrays.asList(
        Long.MAX_VALUE,
        Math.PI,
        new Timestamp(1625702400000L),
//        Duration.ofDays(7).plusMillis(100),
        null,
        null,
        null);

    collector.checkThat(result, is(expected));
  }

  @Test
  public void getObjectForNull() throws Exception {
    vector.reset();
    vector.setValueCount(5);

    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getObject(), equalTo(null));
          collector.checkThat(accessor.wasNull(), is(true));
        });
  }

  @Test
  public void testGetNCharacterStreamUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getNCharacterStream();
    verify(innerAccessor).getNCharacterStream();
  }

  @Test
  public void testGetNStringUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getNString();
    verify(innerAccessor).getNString();
  }

  @Test
  public void testGetSQLXMLUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getSQLXML();
    verify(innerAccessor).getSQLXML();
  }

  @Test
  public void testGetNClobUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getNClob();
    verify(innerAccessor).getNClob();
  }

  @Test
  public void testGetURLUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getURL();
    verify(innerAccessor).getURL();
  }

  @Test
  public void testGetStructUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getStruct();
    verify(innerAccessor).getStruct();
  }

  @Test
  public void testGetArrayUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getArray();
    verify(innerAccessor).getArray();
  }

  @Test
  public void testGetClobUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getClob();
    verify(innerAccessor).getClob();
  }

  @Test
  public void testGetBlobUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getBlob();
    verify(innerAccessor).getBlob();
  }

  @Test
  public void testGetRefUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getRef();
    verify(innerAccessor).getRef();
  }

  @Test
  public void testGetCharacterStreamUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getCharacterStream();
    verify(innerAccessor).getCharacterStream();
  }

  @Test
  public void testGetBinaryStreamUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getBinaryStream();
    verify(innerAccessor).getBinaryStream();
  }

  @Test
  public void testGetUnicodeStreamUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getUnicodeStream();
    verify(innerAccessor).getUnicodeStream();
  }

  @Test
  public void testGetAsciiStreamUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getAsciiStream();
    verify(innerAccessor).getAsciiStream();
  }

  @Test
  public void testGetBytesUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getBytes();
    verify(innerAccessor).getBytes();
  }

  @Test
  public void testGetBigDecimalUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getBigDecimal();
    verify(innerAccessor).getBigDecimal();
  }

  @Test
  public void testGetDoubleUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getDouble();
    verify(innerAccessor).getDouble();
  }

  @Test
  public void testGetFloatUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getFloat();
    verify(innerAccessor).getFloat();
  }

  @Test
  public void testGetLongUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getLong();
    verify(innerAccessor).getLong();
  }

  @Test
  public void testGetIntUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getInt();
    verify(innerAccessor).getInt();
  }

  @Test
  public void testGetShortUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getShort();
    verify(innerAccessor).getShort();
  }

  @Test
  public void testGetByteUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getByte();
    verify(innerAccessor).getByte();
  }

  @Test
  public void testGetBooleanUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getBoolean();
    verify(innerAccessor).getBoolean();
  }

  @Test
  public void testGetStringUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getString();
    verify(innerAccessor).getString();
  }

  @Test
  public void testGetObjectClassUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getObjectClass();
    verify(innerAccessor).getObjectClass();
  }

  @Test
  public void testGetObjectWithClassUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getObject(Object.class);
    verify(innerAccessor).getObject(Object.class);
  }

  @Test
  public void testGetTimestampUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    Calendar calendar = Calendar.getInstance();
    accessor.getTimestamp(calendar);
    verify(innerAccessor).getTimestamp(calendar);
  }

  @Test
  public void testGetTimeUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    Calendar calendar = Calendar.getInstance();
    accessor.getTime(calendar);
    verify(innerAccessor).getTime(calendar);
  }

  @Test
  public void testGetDateUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    Calendar calendar = Calendar.getInstance();
    accessor.getDate(calendar);
    verify(innerAccessor).getDate(calendar);
  }

  @Test
  public void testGetObjectUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    Map<String, Class<?>> map = mock(Map.class);
    accessor.getObject(map);
    verify(innerAccessor).getObject(map);
  }

  @Test
  public void testGetBigDecimalWithScaleUsesSpecificAccessor() {
    ArrowFlightJdbcAccessor innerAccessor = mock(ArrowFlightJdbcNullVectorAccessor.class);
    ArrowFlightJdbcUnionVectorAccessor accessor = spy(new ArrowFlightJdbcUnionVectorAccessor(vector, () -> 0));
    when(accessor.getAccessor()).thenReturn(innerAccessor);

    accessor.getBigDecimal(2);
    verify(innerAccessor).getBigDecimal(2);
  }
}
