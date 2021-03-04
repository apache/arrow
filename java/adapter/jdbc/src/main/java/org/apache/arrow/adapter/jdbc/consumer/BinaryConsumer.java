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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarBinaryVector;

import io.netty.util.internal.PlatformDependent;

/**
 * Consumer which consume binary type values from {@link ResultSet}.
 * Write the data to {@link org.apache.arrow.vector.VarBinaryVector}.
 */
public abstract class BinaryConsumer extends BaseConsumer<VarBinaryVector> {

  /**
   * Creates a consumer for {@link VarBinaryVector}.
   */
  public static BinaryConsumer createConsumer(VarBinaryVector vector, int index, boolean nullable) {
    if (nullable) {
      return new NullableBinaryConsumer(vector, index);
    } else {
      return new NonNullableBinaryConsumer(vector, index);
    }
  }

  private static final int BUFFER_SIZE = 1024;

  /**
   * Instantiate a BinaryConsumer.
   */
  public BinaryConsumer(VarBinaryVector vector, int index) {
    super(vector, index);
    if (vector != null) {
      vector.allocateNewSafe();
    }
  }

  /**
   * consume a InputStream.
   */
  public void consume(InputStream is) throws IOException {
    if (is != null) {

      int read;
      byte[] bytes = new byte[BUFFER_SIZE];
      int totalBytes = 0;

      ArrowBuf dataBuffer = vector.getDataBuffer();
      ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      int startIndex = offsetBuffer.getInt(currentIndex * 4);
      while ((read = is.read(bytes)) != -1) {
        while ((dataBuffer.writerIndex() + read) > dataBuffer.capacity()) {
          vector.reallocDataBuffer();
        }
        PlatformDependent.copyMemory(bytes, 0,
                dataBuffer.memoryAddress() + startIndex + totalBytes, read);
        totalBytes += read;
      }
      offsetBuffer.setInt((currentIndex + 1) * 4, startIndex + totalBytes);
      BitVectorHelper.setBit(vector.getValidityBuffer(), currentIndex);
      vector.setLastSet(currentIndex);
    }
  }

  public void moveWriterPosition() {
    currentIndex++;
  }

  @Override
  public void resetValueVector(VarBinaryVector vector) {
    this.vector = vector;
    this.vector.allocateNewSafe();
    this.currentIndex = 0;
  }

  /**
   * Consumer for nullable binary data.
   */
  static class NullableBinaryConsumer extends BinaryConsumer {
    
    /**
     * Instantiate a BinaryConsumer.
     */
    public NullableBinaryConsumer(VarBinaryVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException, IOException {
      InputStream is = resultSet.getBinaryStream(columnIndexInResultSet);
      if (!resultSet.wasNull()) {
        consume(is);
      }
      currentIndex++;
    }
  }

  /**
   * Consumer for non-nullable binary data.
   */
  static class NonNullableBinaryConsumer extends BinaryConsumer {

    /**
     * Instantiate a BinaryConsumer.
     */
    public NonNullableBinaryConsumer(VarBinaryVector vector, int index) {
      super(vector, index);
    }

    @Override
    public void consume(ResultSet resultSet) throws SQLException, IOException {
      InputStream is = resultSet.getBinaryStream(columnIndexInResultSet);
      consume(is);
      currentIndex++;
    }
  }
}
