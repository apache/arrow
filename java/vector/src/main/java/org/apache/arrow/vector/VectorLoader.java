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

import static org.apache.arrow.util.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Loads buffers into vectors.
 */
public class VectorLoader {

  private final VectorSchemaRoot root;

  private final CompressionCodec.Factory factory;

  /**
   * A flag indicating if decompression is needed.
   * This will affect the behavior of releasing buffers.
   */
  private boolean decompressionNeeded;

  /**
   * Construct with a root to load and will create children in root based on schema.
   *
   * @param root the root to add vectors to based on schema
   */
  public VectorLoader(VectorSchemaRoot root) {
    this(root, NoCompressionCodec.Factory.INSTANCE);
  }

  /**
   * Construct with a root to load and will create children in root based on schema.
   *
   * @param root the root to add vectors to based on schema.
   * @param factory the factory to create codec.
   */
  public VectorLoader(VectorSchemaRoot root, CompressionCodec.Factory factory) {
    this.root = root;
    this.factory = factory;
  }

  /**
   * Loads the record batch in the vectors.
   * will not close the record batch
   *
   * @param recordBatch the batch to load
   */
  public void load(ArrowRecordBatch recordBatch) {
    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    CompressionUtil.CodecType codecType =
        CompressionUtil.CodecType.fromCompressionType(recordBatch.getBodyCompression().getCodec());
    decompressionNeeded = codecType != CompressionUtil.CodecType.NO_COMPRESSION;
    CompressionCodec codec = decompressionNeeded ? factory.createCodec(codecType) : NoCompressionCodec.INSTANCE;
    Iterator<Long> variadicBufferCounts = null;
    if (recordBatch.getVariadicBufferCounts() != null && !recordBatch.getVariadicBufferCounts().isEmpty()) {
      variadicBufferCounts = recordBatch.getVariadicBufferCounts().iterator();
    }

    for (FieldVector fieldVector : root.getFieldVectors()) {
      loadBuffers(fieldVector, fieldVector.getField(), buffers, nodes, codec, variadicBufferCounts);
    }
    root.setRowCount(recordBatch.getLength());
    if (nodes.hasNext() || buffers.hasNext()) {
      throw new IllegalArgumentException("not all nodes and buffers were consumed. nodes: " +
          Collections2.toString(nodes) + " buffers: " + Collections2.toString(buffers));
    }
  }

  private void loadBuffers(
      FieldVector vector,
      Field field,
      Iterator<ArrowBuf> buffers,
      Iterator<ArrowFieldNode> nodes,
      CompressionCodec codec,
      Iterator<Long> variadicBufferCounts) {
    checkArgument(nodes.hasNext(), "no more field nodes for field %s and vector %s", field, vector);
    ArrowFieldNode fieldNode = nodes.next();
    // variadicBufferLayoutCount will be 0 for vectors of type except BaseVariableWidthViewVector
    long variadicBufferLayoutCount = 0;
    if (variadicBufferCounts != null) {
      variadicBufferLayoutCount = variadicBufferCounts.next();
    }
    int bufferLayoutCount = (int) (variadicBufferLayoutCount + TypeLayout.getTypeBufferCount(field.getType()));
    List<ArrowBuf> ownBuffers = new ArrayList<>(bufferLayoutCount);
    for (int j = 0; j < bufferLayoutCount; j++) {
      ArrowBuf nextBuf = buffers.next();
      // for vectors without nulls, the buffer is empty, so there is no need to decompress it.
      ArrowBuf bufferToAdd = nextBuf.writerIndex() > 0 ? codec.decompress(vector.getAllocator(), nextBuf) : nextBuf;
      ownBuffers.add(bufferToAdd);
      if (decompressionNeeded) {
        // decompression performed
        nextBuf.getReferenceManager().retain();
      }
    }
    try {
      vector.loadFieldBuffers(fieldNode, ownBuffers);
      if (decompressionNeeded) {
        for (ArrowBuf buf : ownBuffers) {
          buf.close();
        }
      }
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not load buffers for field " +
          field + ". error message: " + e.getMessage(), e);
    }
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      checkArgument(children.size() == childrenFromFields.size(),
          "should have as many children as in the schema: found %s expected %s",
          childrenFromFields.size(), children.size());
      for (int i = 0; i < childrenFromFields.size(); i++) {
        Field child = children.get(i);
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes, codec, variadicBufferCounts);
      }
    }
  }
}
