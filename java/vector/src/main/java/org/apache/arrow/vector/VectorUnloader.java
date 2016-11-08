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

import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

public class VectorUnloader {

  private final Schema schema;
  private final int valueCount;
  private final List<FieldVector> vectors;

  public VectorUnloader(Schema schema, int valueCount, List<FieldVector> vectors) {
    super();
    this.schema = schema;
    this.valueCount = valueCount;
    this.vectors = vectors;
  }

  public VectorUnloader(VectorSchemaRoot root) {
    this(root.getSchema(), root.getRowCount(), root.getFieldVectors());
  }

  public Schema getSchema() {
    return schema;
  }

  public ArrowRecordBatch getRecordBatch() {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    for (FieldVector vector : vectors) {
      appendNodes(vector, nodes, buffers);
    }
    return new ArrowRecordBatch(valueCount, nodes, buffers);
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    Accessor accessor = vector.getAccessor();
    int nullCount = 0;
    // TODO: should not have to do that
    // we can do that a lot more efficiently (for example with Long.bitCount(i))
    for (int i = 0; i < accessor.getValueCount(); i++) {
      if (accessor.isNull(i)) {
        nullCount ++;
      }
    }
    nodes.add(new ArrowFieldNode(accessor.getValueCount(), nullCount));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    List<ArrowVectorType> expectedBuffers = vector.getField().getTypeLayout().getVectorTypes();
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
