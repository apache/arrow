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

  private final byte[] reuseBytes = new byte[1024];

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
      while (currentIndex >= vector.getValueCapacity()) {
        vector.reallocValidityAndOffsetBuffers();
      }
      final int startOffset = vector.getStartOffset(currentIndex);
      final ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      int dataLength = 0;
      int read;
      while ((read = is.read(reuseBytes)) != -1) {
        while (vector.getDataBuffer().capacity() < (startOffset + dataLength + read)) {
          vector.reallocDataBuffer();
        }
        vector.getDataBuffer().setBytes(startOffset + dataLength, reuseBytes, 0, read);
        dataLength += read;
      }
      offsetBuffer.setInt((currentIndex + 1) * VarBinaryVector.OFFSET_WIDTH, startOffset + dataLength);
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
      moveWriterPosition();
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
      moveWriterPosition();
    }
  }
}
