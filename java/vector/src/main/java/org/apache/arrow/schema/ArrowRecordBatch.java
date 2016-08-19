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
package org.apache.arrow.schema;

import static org.apache.arrow.schema.FBSerializables.writeAllStructsToVector;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flatbuf.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;

public class ArrowRecordBatch implements FBSerializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowRecordBatch.class);

  /** number of records */
  private final int length;

  /** Nodes correspond to the pre-ordered flattened logical schema */
  private final List<ArrowFieldNode> nodes;

  private final List<ArrowBuf> buffers;

  public ArrowRecordBatch(int length, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    super();
    this.length = length;
    this.nodes = nodes;
    this.buffers = buffers;
  }

  public int getLength() {
    return length;
  }

  public List<ArrowFieldNode> getNodes() {
    return nodes;
  }

  public List<ArrowBuf> getBuffers() {
    return buffers;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    RecordBatch.startNodesVector(builder, nodes.size());
    int nodesOffset = writeAllStructsToVector(builder, nodes);
    List<ArrowBuffer> arrowBuffers = new ArrayList<>();
    long offset = 0;
    for (ArrowBuf buffer : buffers) {;
      long size = buffer.readableBytes();
      arrowBuffers.add(new ArrowBuffer(0, offset, size));
      LOGGER.debug(String.format("Buffer in RecordBatch at %d, length: %d", offset, size));
      offset += size;
    }
    RecordBatch.startBuffersVector(builder, buffers.size());
    int buffersOffset = writeAllStructsToVector(builder, arrowBuffers);
    RecordBatch.startRecordBatch(builder);
    RecordBatch.addLength(builder, length);
    RecordBatch.addNodes(builder, nodesOffset);
    RecordBatch.addBuffers(builder, buffersOffset);
    return RecordBatch.endRecordBatch(builder);
  }

}
