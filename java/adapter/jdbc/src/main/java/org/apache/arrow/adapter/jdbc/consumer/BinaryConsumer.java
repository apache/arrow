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

package org.apache.arrow.adapter.jdbc.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.vector.VarBinaryVector;

/**
 * Consumer which consume binary type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarBinaryVector}.
 */
public class BinaryConsumer implements JdbcConsumer<VarBinaryVector> {

  private static final int BUFFER_SIZE = 1024;

  private VarBinaryVector vector;
  private final int columnIndexInResultSet;

  private int currentIndex;

  /**
   * Instantiate a BinaryConsumer.
   */
  public BinaryConsumer(VarBinaryVector vector, int index) {
    this.vector = vector;
    this.columnIndexInResultSet = index;
  }

  /**
   * consume a InputStream.
   */
  public void consume(InputStream is) throws IOException {
    if (is != null) {
      int length = is.available();
      byte[] bytes = new byte[length];

      int readSize = length < BUFFER_SIZE ? length : BUFFER_SIZE;
      int totalBytes = 0;
      while (totalBytes < length) {
        is.read(bytes, totalBytes, readSize);
        totalBytes += readSize;
      }
      vector.setSafe(currentIndex, bytes, 0, totalBytes);
    }
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException, IOException {
    InputStream is = resultSet.getBinaryStream(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      consume(is);
    }
    currentIndex++;
  }

  public void moveWriterPosition() {
    currentIndex++;
  }

  @Override
  public void close() throws Exception {
    this.vector.close();
  }

  @Override
  public void resetValueVector(VarBinaryVector vector) {
    this.vector = vector;
    this.currentIndex = 0;
  }
}
