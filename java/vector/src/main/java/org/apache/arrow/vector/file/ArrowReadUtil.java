/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.file;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

public class ArrowReadUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowReadUtil.class);

  public static final byte[] MAGIC = "ARROW1".getBytes();

  public static int bytesToInt(byte[] bytes) {
    return ((bytes[3] & 255) << 24) +
           ((bytes[2] & 255) << 16) +
           ((bytes[1] & 255) <<  8) +
           ((bytes[0] & 255) <<  0);
  }

  /**
   * Reconstructs an ArrowRecordBatch from a serialized ArrowBuf.
   */
  public static ArrowRecordBatch constructRecordBatch(final ArrowBuf buffer,
      int metadataLength, int bodyLength) {
    LOGGER.debug(String.format(
        "Reconstructing batch with metadataLength: %d bodyLength: %d",
        metadataLength, bodyLength));
    // Record batch flatbuffer is prefixed by its size as int32le
    final ArrowBuf metadata = buffer.slice(4, metadataLength - 4);
    RecordBatch recordBatchFB =
        RecordBatch.getRootAsRecordBatch(metadata.nioBuffer().asReadOnlyBuffer());

    int nodesLength = recordBatchFB.nodesLength();
    final ArrowBuf body = buffer.slice(metadataLength, bodyLength);
    List<ArrowFieldNode> nodes = new ArrayList<>();
    for (int i = 0; i < nodesLength; ++i) {
      FieldNode node = recordBatchFB.nodes(i);
      nodes.add(new ArrowFieldNode(node.length(), node.nullCount()));
    }
    List<ArrowBuf> buffers = new ArrayList<>();
    for (int i = 0; i < recordBatchFB.buffersLength(); ++i) {
      Buffer bufferFB = recordBatchFB.buffers(i);
      LOGGER.debug(String.format(
          "Buffer in RecordBatch at %d, length: %d",
          bufferFB.offset(), bufferFB.length()));
      ArrowBuf vectorBuffer = body.slice((int)bufferFB.offset(), (int)bufferFB.length());
      buffers.add(vectorBuffer);
    }
    ArrowRecordBatch arrowRecordBatch =
        new ArrowRecordBatch(recordBatchFB.length(), nodes, buffers);
    LOGGER.debug("released buffer " + buffer);
    buffer.release();
    return arrowRecordBatch;
  }
}
