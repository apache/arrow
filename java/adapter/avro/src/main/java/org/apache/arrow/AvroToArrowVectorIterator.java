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

package org.apache.arrow;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.consumers.CompositeAvroConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

/**
 * VectorSchemaRoot iterator for partially converting avro data.
 */
public class AvroToArrowVectorIterator implements Iterator<VectorSchemaRoot> {

  public static final int NO_LIMIT_BATCH_SIZE = -1;
  public static final int DEFAULT_BATCH_SIZE = 1024;

  private final Decoder decoder;
  private final Schema schema;

  private final BufferAllocator allocator;

  private CompositeAvroConsumer compositeConsumer;

  private org.apache.arrow.vector.types.pojo.Schema rootSchema;

  private VectorSchemaRoot nextBatch;

  private final int targetBatchSize;

  /**
   * Construct an instance.
   */
  private AvroToArrowVectorIterator(
      Decoder decoder,
      Schema schema,
      BufferAllocator allocator,
      int targetBatchSize) {

    this.decoder = decoder;
    this.schema = schema;
    this.allocator = allocator;
    this.targetBatchSize = targetBatchSize;

  }

  /**
   * Create a ArrowVectorIterator to partially convert data.
   */
  public static AvroToArrowVectorIterator create(
      Decoder decoder,
      Schema schema,
      BufferAllocator allocator,
      int targetBatchSize) {

    AvroToArrowVectorIterator iterator = new AvroToArrowVectorIterator(decoder, schema, allocator, targetBatchSize);
    try {
      iterator.initialize();
      return iterator;
    } catch (Exception e) {
      iterator.close();
      throw new RuntimeException("Error occurs while creating iterator.", e);
    }
  }

  private void initialize() {
    // create consumers
    compositeConsumer = AvroToArrowUtils.createCompositeConsumer(schema, allocator);
    List<FieldVector> vectors = new ArrayList<>();
    compositeConsumer.getConsumers().forEach(c -> vectors.add(c.getVector()));
    List<Field> fields = vectors.stream().map(t -> t.getField()).collect(Collectors.toList());
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, 0);
    rootSchema = root.getSchema();

    load(root);
  }

  private void consumeData(VectorSchemaRoot root) {
    int readRowCount = 0;
    try {
      while ((targetBatchSize == NO_LIMIT_BATCH_SIZE || readRowCount < targetBatchSize)) {
        compositeConsumer.consume(decoder, root);
        readRowCount++;
      }
      root.setRowCount(readRowCount);
    } catch (EOFException eof) {
      // reach the end of encoder stream.
      root.setRowCount(readRowCount);
    } catch (Exception e) {
      compositeConsumer.close();
      throw new RuntimeException("Error occurs while consuming data.", e);
    }
  }

  // Loads the next schema root or null if no more rows are available.
  private void load(VectorSchemaRoot root) {

    Preconditions.checkArgument(root.getFieldVectors().size() == compositeConsumer.getConsumers().size(),
        "Schema root vectors size not equals to consumers size.");

    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      compositeConsumer.getConsumers().get(i).resetValueVector(root.getFieldVectors().get(i));
    }

    // consume data
    consumeData(root);

    if (root.getRowCount() == 0) {
      root.close();
      nextBatch = null;
    } else {
      nextBatch = root;
    }
  }

  @Override
  public boolean hasNext() {
    return nextBatch != null;
  }

  /**
   * Gets the next vector. The user is responsible for freeing its resources.
   */
  public VectorSchemaRoot next() {
    Preconditions.checkArgument(hasNext());
    VectorSchemaRoot returned = nextBatch;
    try {
      load(VectorSchemaRoot.create(rootSchema, allocator));
    } catch (Exception e) {
      close();
      throw new RuntimeException("Error occurs while getting next schema root.", e);
    }
    return returned;
  }

  /**
   * Clean up resources.
   */
  public void close() {
    if (nextBatch != null) {
      nextBatch.close();
    }
    compositeConsumer.close();
  }
}
