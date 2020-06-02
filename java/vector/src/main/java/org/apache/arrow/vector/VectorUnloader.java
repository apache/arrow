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

package org.apache.arrow.vector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtility;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 * Helper class that handles converting a {@link VectorSchemaRoot}
 * to a {@link ArrowRecordBatch}.
 */
public class VectorUnloader {

  private final VectorSchemaRoot root;
  private final boolean includeNullCount;
  private final CompressionCodec codec;
  private final boolean alignBuffers;

  /**
   * Constructs a new instance of the given set of vectors.
   */
  public VectorUnloader(VectorSchemaRoot root) {
    this(root, true, null, true);
  }

  /**
   * Constructs a new instance.
   *
   * @param root  The set of vectors to serialize to an {@link ArrowRecordBatch}.
   * @param includeNullCount Controls whether null count is copied to the {@link ArrowRecordBatch}
   * @param alignBuffers Controls if buffers get aligned to 8-byte boundaries.
   */
  public VectorUnloader(
      VectorSchemaRoot root, boolean includeNullCount, boolean alignBuffers) {
    this(root, includeNullCount, null, alignBuffers);
  }

  /**
   * Constructs a new instance.
   *
   * @param root  The set of vectors to serialize to an {@link ArrowRecordBatch}.
   * @param includeNullCount Controls whether null count is copied to the {@link ArrowRecordBatch}
   * @param codec the codec for compressing data. If it is null, then no compression is needed.
   * @param alignBuffers Controls if buffers get aligned to 8-byte boundaries.
   */
  public VectorUnloader(
      VectorSchemaRoot root, boolean includeNullCount, CompressionCodec codec, boolean alignBuffers) {
    this.root = root;
    this.includeNullCount = includeNullCount;
    this.codec = codec;
    this.alignBuffers = alignBuffers;
  }

  /**
   * Performs the depth first traversal of the Vectors to create an {@link ArrowRecordBatch} suitable
   * for serialization.
   */
  public ArrowRecordBatch getRecordBatch() {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    for (FieldVector vector : root.getFieldVectors()) {
      appendNodes(vector, nodes, buffers);
    }
    return new ArrowRecordBatch(
        root.getRowCount(), nodes, buffers, CompressionUtility.createBodyCompression(codec), alignBuffers);
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    nodes.add(new ArrowFieldNode(vector.getValueCount(), includeNullCount ? vector.getNullCount() : -1));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    int expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
    if (fieldBuffers.size() != expectedBufferCount) {
      throw new IllegalArgumentException(String.format(
          "wrong number of buffers for field %s in vector %s. found: %s",
          vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
    }
    if (codec != null) {
      fieldBuffers = fieldBuffers.stream().map(buf -> {
        long estimatedSize = codec.estimateCompressedSize(buf);
        return codec.compress(buf, estimatedSize);
      }).collect(Collectors.toList());
    }
    buffers.addAll(fieldBuffers);
    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }

}
