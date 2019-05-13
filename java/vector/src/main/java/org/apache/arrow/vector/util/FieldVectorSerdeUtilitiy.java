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

package org.apache.arrow.vector.util;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import java.io.IOException;

/**
 * Utility class for serializing/deserializing FieldVector(only support BaseFixedWidthVector and BaseVariableWidthVector).
 * The serialization format is:
 * <pre>
 * | 4 byte for valueCount
 * | 4 byte for ArrowBuf capacity | serialized first ArrowBuf
 * ...
 * | 4 byte for ArrowBuf capacity | serialized nth ArrowBuf
 * </pre>
 * Note: This isn't part of the arrow standard format.
 */
public class FieldVectorSerdeUtilitiy {

  private FieldVectorSerdeUtilitiy(){}

  /**
   * write the vector to a channel.
   * @param out the channel to write data
   * @param vector the vector to write
   * @throws IOException
   */
  public static void serializeFieldVector(WriteChannel out, FieldVector vector) throws IOException {
    int valueCount = vector.getValueCount();
    byte[] sizeBytes = new byte[4];
    MessageSerializer.intToBytes(valueCount, sizeBytes);
    out.write(sizeBytes);
    ArrowBuf dataBuffer = vector.getDataBuffer();
    ArrowBuf validityBuffer = vector.getValidityBuffer();

    int validityBufferCapacity = BitVectorHelper.getValidityBufferSize(valueCount);

    if (vector instanceof BaseFixedWidthVector) {

      int dataBufferCapacity = vector.getBufferSizeFor(valueCount) - validityBufferCapacity;

      MessageSerializer.intToBytes(dataBufferCapacity, sizeBytes);
      out.write(sizeBytes);
      MessageSerializer.intToBytes(validityBufferCapacity, sizeBytes);
      out.write(sizeBytes);

      out.write(dataBuffer.slice(0, dataBufferCapacity));
      out.write(validityBuffer.slice(0, validityBufferCapacity));

    } else if (vector instanceof BaseVariableWidthVector) {
      ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      int offsetBufferCapacity = (valueCount + 1) * BaseVariableWidthVector.OFFSET_WIDTH;
      int dataBufferCapacity = offsetBuffer.getInt(valueCount * 4);

      MessageSerializer.intToBytes(dataBufferCapacity, sizeBytes);
      out.write(sizeBytes);
      MessageSerializer.intToBytes(validityBufferCapacity, sizeBytes);
      out.write(sizeBytes);
      MessageSerializer.intToBytes(offsetBufferCapacity, sizeBytes);
      out.write(sizeBytes);

      out.write(dataBuffer.slice(0, dataBufferCapacity));
      out.write(validityBuffer.slice(0, validityBufferCapacity));
      out.write(offsetBuffer.slice(0, offsetBufferCapacity));
    } else {
      throw new UnsupportedOperationException("Only BaseFixedWidthVector and BaseVariableWidthVector are supported.");
    }
  }

  /**
   * read a vector from channel
   * @param in the channel to read data
   * @param vector the vector to read data in
   * @throws IOException
   */
  public static void deserializeFieldVector(ReadChannel in, FieldVector vector) throws IOException {

    byte[] sizeBytes = new byte[4];
    in.read(sizeBytes);
    int valueCount = MessageSerializer.bytesToInt(sizeBytes);
    vector.setValueCount(valueCount);

    if (vector instanceof BaseFixedWidthVector) {
      in.read(sizeBytes);
      int dataLength = MessageSerializer.bytesToInt(sizeBytes);
      in.read(sizeBytes);
      int validityLength = MessageSerializer.bytesToInt(sizeBytes);

      ArrowBuf dataBuffer = vector.getDataBuffer();
      ArrowBuf validityBuffer = vector.getValidityBuffer();

      in.readFully(dataBuffer.nioBuffer(0, dataLength));
      in.readFully(validityBuffer.nioBuffer(0, validityLength));

    } else if (vector instanceof BaseVariableWidthVector) {
      in.read(sizeBytes);
      int dataLength = MessageSerializer.bytesToInt(sizeBytes);
      in.read(sizeBytes);
      int validityLength = MessageSerializer.bytesToInt(sizeBytes);
      in.read(sizeBytes);
      int offsetLength = MessageSerializer.bytesToInt(sizeBytes);

      //resize dataBuffer if needed.
      while (vector.getDataBuffer().capacity() < dataLength) {
        ((BaseVariableWidthVector) vector).reallocDataBuffer();
      }

      ArrowBuf dataBuffer = vector.getDataBuffer();
      ArrowBuf validityBuffer = vector.getValidityBuffer();
      ArrowBuf offsetBuffer = vector.getOffsetBuffer();

      in.readFully(dataBuffer.nioBuffer(0, dataLength));
      in.readFully(validityBuffer.nioBuffer(0, validityLength));
      in.readFully(offsetBuffer.nioBuffer(0, offsetLength));
    } else {
      throw new UnsupportedOperationException("Only BaseFixedWidthVector and BaseVariableWidthVector are supported.");
    }
  }
}
