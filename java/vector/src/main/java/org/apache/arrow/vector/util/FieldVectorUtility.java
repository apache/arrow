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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import java.io.IOException;

/**
 * Utility class for FieldVector.
 */
public class FieldVectorUtility {

  public static void serializeFieldVector(WriteChannel out, FieldVector vector) throws IOException {
    int valueCount = vector.getValueCount();
    byte[] reuseIntBytes = new byte[4];
    MessageSerializer.intToBytes(valueCount, reuseIntBytes);
    out.write(reuseIntBytes);
    ArrowBuf dataBuffer = vector.getDataBuffer();
    ArrowBuf validityBuffer = vector.getValidityBuffer();

    int validityBufferCapacity = (valueCount + 7) >> 3;

    if (vector instanceof BaseFixedWidthVector) {

      int dataBufferCapacity = vector.getBufferSizeFor(valueCount) - validityBufferCapacity;

      MessageSerializer.intToBytes(dataBufferCapacity, reuseIntBytes);
      out.write(reuseIntBytes);
      MessageSerializer.intToBytes(validityBufferCapacity, reuseIntBytes);
      out.write(reuseIntBytes);

      out.write(dataBuffer.slice(0, dataBufferCapacity));
      out.write(validityBuffer.slice(0, validityBufferCapacity));

    } else if (vector instanceof BaseVariableWidthVector) {
      ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      int offsetBufferCapacity = (valueCount + 1) * BaseVariableWidthVector.OFFSET_WIDTH;
      int dataBufferCapacity = offsetBuffer.getInt(valueCount * 4);

      MessageSerializer.intToBytes(dataBufferCapacity, reuseIntBytes);
      out.write(reuseIntBytes);
      MessageSerializer.intToBytes(validityBufferCapacity, reuseIntBytes);
      out.write(reuseIntBytes);
      MessageSerializer.intToBytes(offsetBufferCapacity, reuseIntBytes);
      out.write(reuseIntBytes);

      out.write(dataBuffer.slice(0, dataBufferCapacity));
      out.write(validityBuffer.slice(0, validityBufferCapacity));
      out.write(offsetBuffer.slice(0, offsetBufferCapacity));
    } else {
      //TODO support other type
      throw new UnsupportedOperationException("Unsupported FieldVector:" + vector.getField().getName());
    }
  }

  public static void deserializeFieldVector(ReadChannel in, FieldVector vector) throws IOException {

    byte[] reuseIntBytes = new byte[4];
    in.read(reuseIntBytes);
    int valueCount = MessageSerializer.bytesToInt(reuseIntBytes);
    vector.setValueCount(valueCount);

    if (vector instanceof BaseFixedWidthVector) {
      in.read(reuseIntBytes);
      int dataLength = MessageSerializer.bytesToInt(reuseIntBytes);
      in.read(reuseIntBytes);
      int validityLength = MessageSerializer.bytesToInt(reuseIntBytes);

      ArrowBuf dataBuffer = vector.getDataBuffer();
      ArrowBuf validityBuffer = vector.getValidityBuffer();

      in.readFully(dataBuffer.nioBuffer(0, dataLength));
      in.readFully(validityBuffer.nioBuffer(0, validityLength));

    } else if (vector instanceof BaseVariableWidthVector) {
      in.read(reuseIntBytes);
      int dataLength = MessageSerializer.bytesToInt(reuseIntBytes);
      in.read(reuseIntBytes);
      int validityLength = MessageSerializer.bytesToInt(reuseIntBytes);
      in.read(reuseIntBytes);
      int offsetLength = MessageSerializer.bytesToInt(reuseIntBytes);

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
      throw new UnsupportedOperationException("Unsupported FieldVector:" + vector.getField().getName());
    }
  }
}
