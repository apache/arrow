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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ArrowFlightJdbcAccessorTest {

  static class MockedArrowFlightJdbcAccessor extends ArrowFlightJdbcAccessor {

    protected MockedArrowFlightJdbcAccessor() {
      super(() -> 0, (boolean wasNull) -> {});
    }

    @Override
    public Class<?> getObjectClass() {
      return Long.class;
    }
  }

  @Mock MockedArrowFlightJdbcAccessor accessor;

  @Test
  public void testShouldGetObjectWithByteClassReturnGetByte() throws SQLException {
    byte expected = Byte.MAX_VALUE;
    when(accessor.getByte()).thenReturn(expected);

    when(accessor.getObject(Byte.class)).thenCallRealMethod();

    assertEquals(accessor.getObject(Byte.class), (Object) expected);
    verify(accessor).getByte();
  }

  @Test
  public void testShouldGetObjectWithShortClassReturnGetShort() throws SQLException {
    short expected = Short.MAX_VALUE;
    when(accessor.getShort()).thenReturn(expected);

    when(accessor.getObject(Short.class)).thenCallRealMethod();

    assertEquals(accessor.getObject(Short.class), (Object) expected);
    verify(accessor).getShort();
  }

  @Test
  public void testShouldGetObjectWithIntegerClassReturnGetInt() throws SQLException {
    int expected = Integer.MAX_VALUE;
    when(accessor.getInt()).thenReturn(expected);

    when(accessor.getObject(Integer.class)).thenCallRealMethod();

    assertEquals(accessor.getObject(Integer.class), (Object) expected);
    verify(accessor).getInt();
  }

  @Test
  public void testShouldGetObjectWithLongClassReturnGetLong() throws SQLException {
    long expected = Long.MAX_VALUE;
    when(accessor.getLong()).thenReturn(expected);

    when(accessor.getObject(Long.class)).thenCallRealMethod();

    assertEquals(accessor.getObject(Long.class), (Object) expected);
    verify(accessor).getLong();
  }

  @Test
  public void testShouldGetObjectWithFloatClassReturnGetFloat() throws SQLException {
    float expected = Float.MAX_VALUE;
    when(accessor.getFloat()).thenReturn(expected);

    when(accessor.getObject(Float.class)).thenCallRealMethod();

    assertEquals(accessor.getObject(Float.class), (Object) expected);
    verify(accessor).getFloat();
  }

  @Test
  public void testShouldGetObjectWithDoubleClassReturnGetDouble() throws SQLException {
    double expected = Double.MAX_VALUE;
    when(accessor.getDouble()).thenReturn(expected);

    when(accessor.getObject(Double.class)).thenCallRealMethod();

    assertEquals(accessor.getObject(Double.class), (Object) expected);
    verify(accessor).getDouble();
  }

  @Test
  public void testShouldGetObjectWithBooleanClassReturnGetBoolean() throws SQLException {
    when(accessor.getBoolean()).thenReturn(true);

    when(accessor.getObject(Boolean.class)).thenCallRealMethod();

    assertEquals(true, accessor.getObject(Boolean.class));
    verify(accessor).getBoolean();
  }

  @Test
  public void testShouldGetObjectWithBigDecimalClassReturnGetBigDecimal() throws SQLException {
    BigDecimal expected = BigDecimal.TEN;
    when(accessor.getBigDecimal()).thenReturn(expected);

    when(accessor.getObject(BigDecimal.class)).thenCallRealMethod();

    assertEquals(expected, accessor.getObject(BigDecimal.class));
    verify(accessor).getBigDecimal();
  }

  @Test
  public void testShouldGetObjectWithStringClassReturnGetString() throws SQLException {
    String expected = "STRING_VALUE";
    when(accessor.getString()).thenReturn(expected);

    when(accessor.getObject(String.class)).thenCallRealMethod();

    assertEquals(expected, accessor.getObject(String.class));
    verify(accessor).getString();
  }

  @Test
  public void testShouldGetObjectWithByteArrayClassReturnGetBytes() throws SQLException {
    byte[] expected = "STRING_VALUE".getBytes(StandardCharsets.UTF_8);
    when(accessor.getBytes()).thenReturn(expected);

    when(accessor.getObject(byte[].class)).thenCallRealMethod();

    assertArrayEquals(accessor.getObject(byte[].class), expected);
    verify(accessor).getBytes();
  }

  @Test
  public void testShouldGetObjectWithObjectClassReturnGetObject() throws SQLException {
    Object expected = new Object();
    when(accessor.getObject()).thenReturn(expected);

    when(accessor.getObject(Object.class)).thenCallRealMethod();

    assertEquals(expected, accessor.getObject(Object.class));
    verify(accessor).getObject();
  }

  @Test
  public void testShouldGetObjectWithAccessorsObjectClassReturnGetObject() throws SQLException {
    Class<Long> objectClass = Long.class;

    when(accessor.getObject(objectClass)).thenCallRealMethod();

    accessor.getObject(objectClass);
    verify(accessor).getObject(objectClass);
  }

  @Test
  public void testShouldFailToGetBoolean() throws SQLException {
    when(accessor.getBoolean()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getBoolean());
  }

  @Test
  public void testShouldFailToGetByte() throws SQLException {
    when(accessor.getByte()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getByte());
  }

  @Test
  public void testShouldFailToGetShort() throws SQLException {
    when(accessor.getShort()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getShort());
  }

  @Test
  public void testShouldFailToGetInt() throws SQLException {
    when(accessor.getInt()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getInt());
  }

  @Test
  public void testShouldFailToGetLong() throws SQLException {
    when(accessor.getLong()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getLong());
  }

  @Test
  public void testShouldFailToGetFloat() throws SQLException {
    when(accessor.getFloat()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getFloat());
  }

  @Test
  public void testShouldFailToGetDouble() throws SQLException {
    when(accessor.getDouble()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getDouble());
  }

  @Test
  public void testShouldFailToGetBigDecimal() throws SQLException {
    when(accessor.getBigDecimal()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getBigDecimal());
  }

  @Test
  public void testShouldFailToGetBytes() throws SQLException {
    when(accessor.getBytes()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getBytes());
  }

  @Test
  public void testShouldFailToGetAsciiStream() throws SQLException {
    when(accessor.getAsciiStream()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getAsciiStream());
  }

  @Test
  public void testShouldFailToGetUnicodeStream() throws SQLException {
    when(accessor.getUnicodeStream()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getUnicodeStream());
  }

  @Test
  public void testShouldFailToGetBinaryStream() throws SQLException {
    when(accessor.getBinaryStream()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getBinaryStream());
  }

  @Test
  public void testShouldFailToGetObject() throws SQLException {
    when(accessor.getObject()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getObject());
  }

  @Test
  public void testShouldFailToGetObjectMap() throws SQLException {
    Map<String, Class<?>> map = new HashMap<>();
    when(accessor.getObject(map)).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getObject(map));
  }

  @Test
  public void testShouldFailToGetCharacterStream() throws SQLException {
    when(accessor.getCharacterStream()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getCharacterStream());
  }

  @Test
  public void testShouldFailToGetRef() throws SQLException {
    when(accessor.getRef()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getRef());
  }

  @Test
  public void testShouldFailToGetBlob() throws SQLException {
    when(accessor.getBlob()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getBlob());
  }

  @Test
  public void testShouldFailToGetClob() throws SQLException {
    when(accessor.getClob()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getClob());
  }

  @Test
  public void testShouldFailToGetArray() throws SQLException {
    when(accessor.getArray()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getArray());
  }

  @Test
  public void testShouldFailToGetStruct() throws SQLException {
    when(accessor.getStruct()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getStruct());
  }

  @Test
  public void testShouldFailToGetURL() throws SQLException {
    when(accessor.getURL()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getURL());
  }

  @Test
  public void testShouldFailToGetNClob() throws SQLException {
    when(accessor.getNClob()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getNClob());
  }

  @Test
  public void testShouldFailToGetSQLXML() throws SQLException {
    when(accessor.getSQLXML()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getSQLXML());
  }

  @Test
  public void testShouldFailToGetNString() throws SQLException {
    when(accessor.getNString()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getNString());
  }

  @Test
  public void testShouldFailToGetNCharacterStream() throws SQLException {
    when(accessor.getNCharacterStream()).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getNCharacterStream());
  }

  @Test
  public void testShouldFailToGetDate() throws SQLException {
    when(accessor.getDate(null)).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getDate(null));
  }

  @Test
  public void testShouldFailToGetTime() throws SQLException {
    when(accessor.getTime(null)).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getTime(null));
  }

  @Test
  public void testShouldFailToGetTimestamp() throws SQLException {
    when(accessor.getTimestamp(null)).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getTimestamp(null));
  }

  @Test
  public void testShouldFailToGetBigDecimalWithValue() throws SQLException {
    when(accessor.getBigDecimal(0)).thenCallRealMethod();
    assertThrows(SQLException.class, () -> accessor.getBigDecimal(0));
  }
}
