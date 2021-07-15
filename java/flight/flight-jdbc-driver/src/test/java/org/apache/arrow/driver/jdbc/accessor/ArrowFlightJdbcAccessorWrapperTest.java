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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Calendar;
import java.util.Map;

import org.apache.arrow.driver.jdbc.accessor.impl.ArrowFlightJdbcNullVectorAccessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArrowFlightJdbcAccessorWrapperTest {

  private static class ArrowFlightJdbcAccessorWrapperMock extends ArrowFlightJdbcAccessorWrapper {
    protected ArrowFlightJdbcAccessorWrapperMock() {
      super(() -> 0);
    }

    @Override
    protected ArrowFlightJdbcAccessor getAccessor() {
      return new ArrowFlightJdbcNullVectorAccessor();
    }
  }

  @Mock
  ArrowFlightJdbcAccessor innerAccessor;

  @Spy
  ArrowFlightJdbcAccessorWrapperMock accessor;

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
  public void testGetBigDecimalUsesSpecificAccessor() {
    accessor.getBigDecimal();
    verify(innerAccessor).getBigDecimal();
  }

  @Test
  public void testGetDoubleUsesSpecificAccessor() {
    accessor.getDouble();
    verify(innerAccessor).getDouble();
  }

  @Test
  public void testGetFloatUsesSpecificAccessor() {
    accessor.getFloat();
    verify(innerAccessor).getFloat();
  }

  @Test
  public void testGetLongUsesSpecificAccessor() {
    accessor.getLong();
    verify(innerAccessor).getLong();
  }

  @Test
  public void testGetIntUsesSpecificAccessor() {
    accessor.getInt();
    verify(innerAccessor).getInt();
  }

  @Test
  public void testGetShortUsesSpecificAccessor() {
    accessor.getShort();
    verify(innerAccessor).getShort();
  }

  @Test
  public void testGetByteUsesSpecificAccessor() {
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
  public void testGetObjectWithClassUsesSpecificAccessor() {
    accessor.getObject(Object.class);
    verify(innerAccessor).getObject(Object.class);
  }

  @Test
  public void testGetTimestampUsesSpecificAccessor() {
    Calendar calendar = Calendar.getInstance();
    accessor.getTimestamp(calendar);
    verify(innerAccessor).getTimestamp(calendar);
  }

  @Test
  public void testGetTimeUsesSpecificAccessor() {
    Calendar calendar = Calendar.getInstance();
    accessor.getTime(calendar);
    verify(innerAccessor).getTime(calendar);
  }

  @Test
  public void testGetDateUsesSpecificAccessor() {
    Calendar calendar = Calendar.getInstance();
    accessor.getDate(calendar);
    verify(innerAccessor).getDate(calendar);
  }

  @Test
  public void testGetObjectUsesSpecificAccessor() {
    Map<String, Class<?>> map = mock(Map.class);
    accessor.getObject(map);
    verify(innerAccessor).getObject(map);
  }

  @Test
  public void testGetBigDecimalWithScaleUsesSpecificAccessor() {
    accessor.getBigDecimal(2);
    verify(innerAccessor).getBigDecimal(2);
  }

}