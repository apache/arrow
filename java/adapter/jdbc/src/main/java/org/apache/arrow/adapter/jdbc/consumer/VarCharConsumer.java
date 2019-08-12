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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.VarCharWriter;

import io.netty.buffer.ArrowBuf;

/**
 * Consumer which consume varchar type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarCharVector}.
 */
public class VarCharConsumer implements JdbcConsumer {

  private final VarCharWriter writer;
  private final int columnIndexInResultSet;
  private BufferAllocator allocator;

  private ArrowBuf reuse;

  /**
   * Instantiate a VarCharConsumer.
   */
  public VarCharConsumer(VarCharVector vector, int index) {
    this.writer = new VarCharWriterImpl(vector);
    this.columnIndexInResultSet = index;
    this.allocator = vector.getAllocator();
  }

  @Override
  public void consume(ResultSet resultSet) throws SQLException {
    String value = resultSet.getString(columnIndexInResultSet);
    if (!resultSet.wasNull()) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (reuse == null) {
        reuse = allocator.buffer(bytes.length);
      }
      if (bytes.length > reuse.capacity()) {
        reuse.close();
        reuse = allocator.buffer(bytes.length);
      }
      reuse.setBytes(0, bytes, 0, bytes.length);
      writer.writeVarChar(0, bytes.length, reuse);
    }
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void close() {
    if (reuse != null) {
      reuse.close();
    }
  }
}
