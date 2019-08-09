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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;

import io.netty.buffer.ArrowBuf;

/**
 * Consumer which consume binary type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarBinaryVector}.
 */
public class BinaryConsumer implements JdbcConsumer {

  private static final int BUFFER_SIZE = 1024;

  private final VarBinaryWriter writer;
  private final int columnIndexInResultSet;
  private BufferAllocator allocator;

  private ArrowBuf reuse;
  private byte[] reuseBytes;

  /**
   * Instantiate a BinaryConsumer.
   */
  public BinaryConsumer(VarBinaryVector vector, int index) {
    this.writer = new VarBinaryWriterImpl(vector);
    this.columnIndexInResultSet = index;

    this.allocator = vector.getAllocator();
    reuse = allocator.buffer(BUFFER_SIZE);
    reuseBytes = new byte[BUFFER_SIZE];
  }

  /**
   * consume a InputStream.
   */
  public void consume(InputStream is) throws IOException {
    if (is != null) {
      int length = is.available();
      if (length > reuse.capacity()) {
        reuse.close();
        reuse = allocator.buffer(length);
      }

      int total = 0;
      int read;
      while ((read = is.read(reuseBytes, 0, reuseBytes.length)) != -1) {
        reuse.setBytes(total, reuseBytes, 0, read);
        total += read;
      }
      writer.writeVarBinary(0, total, reuse);
    }
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException, IOException {
    InputStream is = resultSet.getBinaryStream(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      consume(is);
    }
    writer.setPosition(writer.getPosition() + 1);
  }

  public void moveWriterPosition() {
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void close() {
    if (reuse != null) {
      reuse.close();
    }
  }
}
