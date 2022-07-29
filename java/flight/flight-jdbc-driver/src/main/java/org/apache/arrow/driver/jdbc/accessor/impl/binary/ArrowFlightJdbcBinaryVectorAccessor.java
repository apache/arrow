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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.VarBinaryVector;

/**
 * Accessor for the Arrow types: {@link FixedSizeBinaryVector}, {@link VarBinaryVector}
 * and {@link LargeVarBinaryVector}.
 */
public class ArrowFlightJdbcBinaryVectorAccessor extends ArrowFlightJdbcAccessor {

  private interface ByteArrayGetter {
    byte[] get(int index);
  }

  private final ByteArrayGetter getter;

  public ArrowFlightJdbcBinaryVectorAccessor(FixedSizeBinaryVector vector,
                                             IntSupplier currentRowSupplier,
                                             ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::get, currentRowSupplier, setCursorWasNull);
  }

  public ArrowFlightJdbcBinaryVectorAccessor(VarBinaryVector vector, IntSupplier currentRowSupplier,
                                             ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::get, currentRowSupplier, setCursorWasNull);
  }

  public ArrowFlightJdbcBinaryVectorAccessor(LargeVarBinaryVector vector,
                                             IntSupplier currentRowSupplier,
                                             ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::get, currentRowSupplier, setCursorWasNull);
  }

  private ArrowFlightJdbcBinaryVectorAccessor(ByteArrayGetter getter,
                                              IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.getter = getter;
  }

  @Override
  public byte[] getBytes() {
    byte[] bytes = getter.get(getCurrentRow());
    this.wasNull = bytes == null;
    this.wasNullConsumer.setWasNull(this.wasNull);

    return bytes;
  }

  @Override
  public Object getObject() {
    return this.getBytes();
  }

  @Override
  public Class<?> getObjectClass() {
    return byte[].class;
  }

  @Override
  public String getString() {
    byte[] bytes = this.getBytes();
    if (bytes == null) {
      return null;
    }

    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public InputStream getAsciiStream() {
    byte[] bytes = getBytes();
    if (bytes == null) {
      return null;
    }

    return new ByteArrayInputStream(bytes);
  }

  @Override
  public InputStream getUnicodeStream() {
    byte[] bytes = getBytes();
    if (bytes == null) {
      return null;
    }

    return new ByteArrayInputStream(bytes);
  }

  @Override
  public InputStream getBinaryStream() {
    byte[] bytes = getBytes();
    if (bytes == null) {
      return null;
    }

    return new ByteArrayInputStream(bytes);
  }

  @Override
  public Reader getCharacterStream() {
    String string = getString();
    if (string == null) {
      return null;
    }

    return new CharArrayReader(string.toCharArray());
  }
}
