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

import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.VarCharWriter;

import io.netty.buffer.ArrowBuf;

/**
 * Consumer which consume clob type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarCharVector}.
 */
public class ClobConsumer implements JdbcConsumer<VarCharVector> {

  private static final int BUFFER_SIZE = 256;

  private VarCharWriter writer;
  private final int columnIndexInResultSet;
  private BufferAllocator allocator;

  private ArrowBuf reuse;

  /**
   * Instantiate a ClobConsumer.
   */
  public ClobConsumer(VarCharVector vector, int index) {
    this.writer = new VarCharWriterImpl(vector);
    this.columnIndexInResultSet = index;

    this.allocator = vector.getAllocator();
    reuse = allocator.buffer(1024);
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    Clob clob = resultSet.getClob(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      if (clob != null) {
        long length = clob.length();
        if (length > reuse.capacity()) {
          reuse.close();
          reuse = allocator.buffer((int) length);
        }

        int read = 1;
        int readSize = length < BUFFER_SIZE ? (int) length : BUFFER_SIZE;
        int totalBytes = 0;
        while (read <= length) {
          String str = clob.getSubString(read, readSize);
          byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
          reuse.setBytes(totalBytes, bytes, 0, bytes.length);
          totalBytes += bytes.length;
          read += readSize;
        }
        writer.writeVarChar(0, totalBytes, reuse);
      }
    }
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void close() {
    if (reuse != null) {
      reuse.close();
    }
  }

  @Override
  public void resetValueVector(VarCharVector vector) {
    this.writer = new VarCharWriterImpl(vector);
  }
}
