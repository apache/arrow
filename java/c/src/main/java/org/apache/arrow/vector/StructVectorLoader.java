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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Loads buffers into {@link StructVector}.
 */
public class StructVectorLoader {

  private final Schema schema;
  private final CompressionCodec.Factory factory;

  /**
   * A flag indicating if decompression is needed. This will affect the behavior
   * of releasing buffers.
   */
  private boolean decompressionNeeded;

  /**
   * Construct with a schema.
   *
   * @param schema buffers are added based on schema.
   */
  public StructVectorLoader(Schema schema) {
    this(schema, NoCompressionCodec.Factory.INSTANCE);
  }

  /**
   * Construct with a schema and a compression codec factory.
   *
   * @param schema  buffers are added based on schema.
   * @param factory the factory to create codec.
   */
  public StructVectorLoader(Schema schema, CompressionCodec.Factory factory) {
    this.schema = schema;
    this.factory = factory;
  }

  /**
   * Loads the record batch into the struct vector.
   * 
   * <p>
   * This will not close the record batch.
   *
   * @param recordBatch the batch to load
   */
  public StructVector load(BufferAllocator allocator, ArrowRecordBatch recordBatch) {
    StructVector result = StructVector.emptyWithDuplicates("", allocator);
    result.initializeChildrenFromFields(this.schema.getFields());

    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    CompressionUtil.CodecType codecType = CompressionUtil.CodecType
        .fromCompressionType(recordBatch.getBodyCompression().getCodec());
    decompressionNeeded = codecType != CompressionUtil.CodecType.NO_COMPRESSION;
    CompressionCodec codec = decompressionNeeded ? factory.createCodec(codecType) : NoCompressionCodec.INSTANCE;
    for (FieldVector fieldVector : result.getChildrenFromFields()) {
      loadBuffers(fieldVector, fieldVector.getField(), buffers, nodes, codec);
    }
    result.loadFieldBuffers(new ArrowFieldNode(recordBatch.getLength(), 0), Collections.singletonList(null));
    if (nodes.hasNext() || buffers.hasNext()) {
      throw new IllegalArgumentException("not all nodes and buffers were consumed. nodes: " + 
        Collections2.toList(nodes).toString() + " buffers: " + Collections2.toList(buffers).toString());
    }
    return result;
  }

  private void loadBuffers(FieldVector vector, Field field, Iterator<ArrowBuf> buffers, Iterator<ArrowFieldNode> nodes,
      CompressionCodec codec) {
    checkArgument(nodes.hasNext(), "no more field nodes for for field %s and vector %s", field, vector);
    ArrowFieldNode fieldNode = nodes.next();
    int bufferLayoutCount = TypeLayout.getTypeBufferCount(field.getType());
    List<ArrowBuf> ownBuffers = new ArrayList<>(bufferLayoutCount);
    for (int j = 0; j < bufferLayoutCount; j++) {
      ArrowBuf nextBuf = buffers.next();
      // for vectors without nulls, the buffer is empty, so there is no need to
      // decompress it.
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
      throw new IllegalArgumentException(
          "Could not load buffers for field " + field + ". error message: " + e.getMessage(), e);
    }
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      checkArgument(children.size() == childrenFromFields.size(),
          "should have as many children as in the schema: found %s expected %s", childrenFromFields.size(),
          children.size());
      for (int i = 0; i < childrenFromFields.size(); i++) {
        Field child = children.get(i);
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes, codec);
      }
    }
  }
}
