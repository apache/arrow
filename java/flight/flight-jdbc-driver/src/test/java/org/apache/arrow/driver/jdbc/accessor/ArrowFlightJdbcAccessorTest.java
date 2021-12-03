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

package org.apache.arrow.driver.jdbc.accessor;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArrowFlightJdbcAccessorTest {

  static class MockedArrowFlightJdbcAccessor extends ArrowFlightJdbcAccessor {

    protected MockedArrowFlightJdbcAccessor() {
      super(() -> 0, (boolean wasNull) -> {
      });
    }

    @Override
    public Class<?> getObjectClass() {
      return Long.class;
    }
  }

  @Mock
  MockedArrowFlightJdbcAccessor accessor;

  @Test
  public void testShouldGetObjectWithByteClassReturnGetByte() {
    byte expected = Byte.MAX_VALUE;
    when(accessor.getByte()).thenReturn(expected);

    when(accessor.getObject(Byte.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Byte.class), (Object) expected);
    verify(accessor).getByte();
  }

  @Test
  public void testShouldGetObjectWithShortClassReturnGetShort() {
    short expected = Short.MAX_VALUE;
    when(accessor.getShort()).thenReturn(expected);

    when(accessor.getObject(Short.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Short.class), (Object) expected);
    verify(accessor).getShort();
  }

  @Test
  public void testShouldGetObjectWithIntegerClassReturnGetInt() {
    int expected = Integer.MAX_VALUE;
    when(accessor.getInt()).thenReturn(expected);

    when(accessor.getObject(Integer.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Integer.class), (Object) expected);
    verify(accessor).getInt();
  }

  @Test
  public void testShouldGetObjectWithLongClassReturnGetLong() {
    long expected = Long.MAX_VALUE;
    when(accessor.getLong()).thenReturn(expected);

    when(accessor.getObject(Long.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Long.class), (Object) expected);
    verify(accessor).getLong();
  }

  @Test
  public void testShouldGetObjectWithFloatClassReturnGetFloat() {
    float expected = Float.MAX_VALUE;
    when(accessor.getFloat()).thenReturn(expected);

    when(accessor.getObject(Float.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Float.class), (Object) expected);
    verify(accessor).getFloat();
  }

  @Test
  public void testShouldGetObjectWithDoubleClassReturnGetDouble() {
    double expected = Double.MAX_VALUE;
    when(accessor.getDouble()).thenReturn(expected);

    when(accessor.getObject(Double.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Double.class), (Object) expected);
    verify(accessor).getDouble();
  }

  @Test
  public void testShouldGetObjectWithBooleanClassReturnGetBoolean() {
    when(accessor.getBoolean()).thenReturn(true);

    when(accessor.getObject(Boolean.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Boolean.class), true);
    verify(accessor).getBoolean();
  }

  @Test
  public void testShouldGetObjectWithBigDecimalClassReturnGetBigDecimal() {
    BigDecimal expected = BigDecimal.TEN;
    when(accessor.getBigDecimal()).thenReturn(expected);

    when(accessor.getObject(BigDecimal.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(BigDecimal.class), expected);
    verify(accessor).getBigDecimal();
  }

  @Test
  public void testShouldGetObjectWithStringClassReturnGetString() {
    String expected = "STRING_VALUE";
    when(accessor.getString()).thenReturn(expected);

    when(accessor.getObject(String.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(String.class), expected);
    verify(accessor).getString();
  }

  @Test
  public void testShouldGetObjectWithByteArrayClassReturnGetBytes() {
    byte[] expected = "STRING_VALUE".getBytes(StandardCharsets.UTF_8);
    when(accessor.getBytes()).thenReturn(expected);

    when(accessor.getObject(byte[].class)).thenCallRealMethod();

    Assert.assertArrayEquals(accessor.getObject(byte[].class), expected);
    verify(accessor).getBytes();
  }

  @Test
  public void testShouldGetObjectWithObjectClassReturnGetObject() {
    Object expected = new Object();
    when(accessor.getObject()).thenReturn(expected);

    when(accessor.getObject(Object.class)).thenCallRealMethod();

    Assert.assertEquals(accessor.getObject(Object.class), expected);
    verify(accessor).getObject();
  }

  @Test
  public void testShouldGetObjectWithAccessorsObjectClassReturnGetObject() {
    Class<Long> objectClass = Long.class;

    when(accessor.getObject(objectClass)).thenCallRealMethod();

    accessor.getObject(objectClass);
    verify(accessor).getObject(objectClass);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetBoolean() {
    when(accessor.getBoolean()).thenCallRealMethod();
    accessor.getBoolean();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetByte() {
    when(accessor.getByte()).thenCallRealMethod();
    accessor.getByte();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetShort() {
    when(accessor.getShort()).thenCallRealMethod();
    accessor.getShort();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetInt() {
    when(accessor.getInt()).thenCallRealMethod();
    accessor.getInt();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetLong() {
    when(accessor.getLong()).thenCallRealMethod();
    accessor.getLong();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetFloat() {
    when(accessor.getFloat()).thenCallRealMethod();
    accessor.getFloat();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetDouble() {
    when(accessor.getDouble()).thenCallRealMethod();
    accessor.getDouble();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetBigDecimal() {
    when(accessor.getBigDecimal()).thenCallRealMethod();
    accessor.getBigDecimal();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetBytes() {
    when(accessor.getBytes()).thenCallRealMethod();
    accessor.getBytes();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetAsciiStream() {
    when(accessor.getAsciiStream()).thenCallRealMethod();
    accessor.getAsciiStream();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetUnicodeStream() {
    when(accessor.getUnicodeStream()).thenCallRealMethod();
    accessor.getUnicodeStream();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetBinaryStream() {
    when(accessor.getBinaryStream()).thenCallRealMethod();
    accessor.getBinaryStream();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetObject() {
    when(accessor.getObject()).thenCallRealMethod();
    accessor.getObject();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetObjectMap() {
    Map<String, Class<?>> map = new HashMap<>();
    when(accessor.getObject(map)).thenCallRealMethod();
    accessor.getObject(map);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetCharacterStream() {
    when(accessor.getCharacterStream()).thenCallRealMethod();
    accessor.getCharacterStream();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetRef() {
    when(accessor.getRef()).thenCallRealMethod();
    accessor.getRef();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetBlob() {
    when(accessor.getBlob()).thenCallRealMethod();
    accessor.getBlob();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetClob() {
    when(accessor.getClob()).thenCallRealMethod();
    accessor.getClob();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetArray() {
    when(accessor.getArray()).thenCallRealMethod();
    accessor.getArray();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetStruct() {
    when(accessor.getStruct()).thenCallRealMethod();
    accessor.getStruct();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetURL() {
    when(accessor.getURL()).thenCallRealMethod();
    accessor.getURL();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetNClob() {
    when(accessor.getNClob()).thenCallRealMethod();
    accessor.getNClob();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetSQLXML() {
    when(accessor.getSQLXML()).thenCallRealMethod();
    accessor.getSQLXML();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetNString() {
    when(accessor.getNString()).thenCallRealMethod();
    accessor.getNString();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetNCharacterStream() {
    when(accessor.getNCharacterStream()).thenCallRealMethod();
    accessor.getNCharacterStream();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetDate() {
    when(accessor.getDate(null)).thenCallRealMethod();
    accessor.getDate(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetTime() {
    when(accessor.getTime(null)).thenCallRealMethod();
    accessor.getTime(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetTimestamp() {
    when(accessor.getTimestamp(null)).thenCallRealMethod();
    accessor.getTimestamp(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testShouldFailToGetBigDecimalWithValue() {
    when(accessor.getBigDecimal(0)).thenCallRealMethod();
    accessor.getBigDecimal(0);
  }
}
