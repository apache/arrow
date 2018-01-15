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

package org.apache.arrow.vector;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BufferLayout.BufferType;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public class VectorUnloader {

  private final VectorSchemaRoot root;
  private final boolean includeNullCount;
  private final boolean alignBuffers;

  public VectorUnloader(VectorSchemaRoot root) {
    this(root, true, true);
  }

  public VectorUnloader(VectorSchemaRoot root, boolean includeNullCount, boolean alignBuffers) {
    this.root = root;
    this.includeNullCount = includeNullCount;
    this.alignBuffers = alignBuffers;
  }

  public ArrowRecordBatch getRecordBatch() {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    for (FieldVector vector : root.getFieldVectors()) {
      appendNodes(vector, nodes, buffers);
    }
    return new ArrowRecordBatch(root.getRowCount(), nodes, buffers, alignBuffers);
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    nodes.add(new ArrowFieldNode(vector.getValueCount(), includeNullCount ? vector.getNullCount() : -1));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    List<BufferType> expectedBuffers = TypeLayout.getTypeLayout(vector.getField().getType()).getBufferTypes();
    if (fieldBuffers.size() != expectedBuffers.size()) {
      throw new IllegalArgumentException(String.format(
          "wrong number of buffers for field %s in vector %s. found: %s",
          vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
    }
    buffers.addAll(fieldBuffers);
    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }

}