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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Map;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.impl.ArrowFlightJdbcNullVectorAccessor;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbstractArrowFlightJdbcUnionVectorAccessorTest {

  private static class AbstractArrowFlightJdbcUnionVectorAccessorMock
      extends AbstractArrowFlightJdbcUnionVectorAccessor {
    protected AbstractArrowFlightJdbcUnionVectorAccessorMock() {
      super(() -> 0, (boolean wasNull) -> {
      });
    }

    @Override
    protected ArrowFlightJdbcAccessor createAccessorForVector(ValueVector vector) {
      return new ArrowFlightJdbcNullVectorAccessor((boolean wasNull) -> {
      });
    }

    @Override
    protected byte getCurrentTypeId() {
      return 0;
    }

    @Override
    protected ValueVector getVectorByTypeId(byte typeId) {
      return new NullVector();
    }
  }

  @Mock
  ArrowFlightJdbcAccessor innerAccessor;

  @Spy
  AbstractArrowFlightJdbcUnionVectorAccessorMock accessor;

  @Before
  public void setup() {
    when(accessor.getAccessor()).thenReturn(innerAccessor);
  }

  @Test
  public void testGetNCharacterStreamUsesSpecificAccessor() {
    accessor.getNCharacterStream();
    verify(innerAccessor).getNCharacterStream();
  }

  @Test
  public void testGetNStringUsesSpecificAccessor() {
    accessor.getNString();
    verify(innerAccessor).getNString();
  }

  @Test
  public void testGetSQLXMLUsesSpecificAccessor() {
    accessor.getSQLXML();
    verify(innerAccessor).getSQLXML();
  }

  @Test
  public void testGetNClobUsesSpecificAccessor() {
    accessor.getNClob();
    verify(innerAccessor).getNClob();
  }

  @Test
  public void testGetURLUsesSpecificAccessor() {
    accessor.getURL();
    verify(innerAccessor).getURL();
  }

  @Test
  public void testGetStructUsesSpecificAccessor() {
    accessor.getStruct();
    verify(innerAccessor).getStruct();
  }

  @Test
  public void testGetArrayUsesSpecificAccessor() {
    accessor.getArray();
    verify(innerAccessor).getArray();
  }

  @Test
  public void testGetClobUsesSpecificAccessor() {
    accessor.getClob();
    verify(innerAccessor).getClob();
  }

  @Test
  public void testGetBlobUsesSpecificAccessor() {
    accessor.getBlob();
    verify(innerAccessor).getBlob();
  }

  @Test
  public void testGetRefUsesSpecificAccessor() {
    accessor.getRef();
    verify(innerAccessor).getRef();
  }

  @Test
  public void testGetCharacterStreamUsesSpecificAccessor() {
    accessor.getCharacterStream();
    verify(innerAccessor).getCharacterStream();
  }

  @Test
  public void testGetBinaryStreamUsesSpecificAccessor() {
    accessor.getBinaryStream();
    verify(innerAccessor).getBinaryStream();
  }

  @Test
  public void testGetUnicodeStreamUsesSpecificAccessor() {
    accessor.getUnicodeStream();
    verify(innerAccessor).getUnicodeStream();
  }

  @Test
  public void testGetAsciiStreamUsesSpecificAccessor() {
    accessor.getAsciiStream();
    verify(innerAccessor).getAsciiStream();
  }

  @Test
  public void testGetBytesUsesSpecificAccessor() {
    accessor.getBytes();
    verify(innerAccessor).getBytes();
  }

  @Test
  public void testGetBigDecimalUsesSpecificAccessor() throws SQLException {
    accessor.getBigDecimal();
    verify(innerAccessor).getBigDecimal();
  }

  @Test
  public void testGetDoubleUsesSpecificAccessor() throws SQLException {
    accessor.getDouble();
    verify(innerAccessor).getDouble();
  }

  @Test
  public void testGetFloatUsesSpecificAccessor() throws SQLException {
    accessor.getFloat();
    verify(innerAccessor).getFloat();
  }

  @Test
  public void testGetLongUsesSpecificAccessor() throws SQLException {
    accessor.getLong();
    verify(innerAccessor).getLong();
  }

  @Test
  public void testGetIntUsesSpecificAccessor() throws SQLException {
    accessor.getInt();
    verify(innerAccessor).getInt();
  }

  @Test
  public void testGetShortUsesSpecificAccessor() throws SQLException {
    accessor.getShort();
    verify(innerAccessor).getShort();
  }

  @Test
  public void testGetByteUsesSpecificAccessor() throws SQLException {
    accessor.getByte();
    verify(innerAccessor).getByte();
  }

  @Test
  public void testGetBooleanUsesSpecificAccessor() {
    accessor.getBoolean();
    verify(innerAccessor).getBoolean();
  }

  @Test
  public void testGetStringUsesSpecificAccessor() {
    accessor.getString();
    verify(innerAccessor).getString();
  }

  @Test
  public void testGetObjectClassUsesSpecificAccessor() {
    accessor.getObjectClass();
    verify(innerAccessor).getObjectClass();
  }

  @Test
  public void testGetObjectWithClassUsesSpecificAccessor() throws SQLException {
    try {
      accessor.getObject(Object.class);
      verify(innerAccessor).getObject(Object.class);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Test
  public void testGetTimestampUsesSpecificAccessor() throws SQLException {
    Calendar calendar = Calendar.getInstance();
    accessor.getTimestamp(calendar);
    verify(innerAccessor).getTimestamp(calendar);
  }

  @Test
  public void testGetTimeUsesSpecificAccessor() throws SQLException {
    Calendar calendar = Calendar.getInstance();
    accessor.getTime(calendar);
    verify(innerAccessor).getTime(calendar);
  }

  @Test
  public void testGetDateUsesSpecificAccessor() throws SQLException {
    Calendar calendar = Calendar.getInstance();
    accessor.getDate(calendar);
    verify(innerAccessor).getDate(calendar);
  }

  @Test
  public void testGetObjectUsesSpecificAccessor() throws SQLException {
    Map<String, Class<?>> map = mock(Map.class);
    accessor.getObject(map);
    verify(innerAccessor).getObject(map);
  }

  @Test
  public void testGetBigDecimalWithScaleUsesSpecificAccessor() throws SQLException {
    accessor.getBigDecimal(2);
    verify(innerAccessor).getBigDecimal(2);
  }
}
