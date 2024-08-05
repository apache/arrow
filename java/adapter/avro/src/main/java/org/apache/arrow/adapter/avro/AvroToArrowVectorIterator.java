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
package org.apache.arrow.adapter.avro;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.adapter.avro.consumers.CompositeAvroConsumer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

/** VectorSchemaRoot iterator for partially converting avro data. */
public class AvroToArrowVectorIterator implements Iterator<VectorSchemaRoot>, AutoCloseable {

  public static final int NO_LIMIT_BATCH_SIZE = -1;
  public static final int DEFAULT_BATCH_SIZE = 1024;

  private final Decoder decoder;
  private final Schema schema;

  private final AvroToArrowConfig config;

  private CompositeAvroConsumer compositeConsumer;

  private org.apache.arrow.vector.types.pojo.Schema rootSchema;

  private VectorSchemaRoot nextBatch;

  private final int targetBatchSize;

  /** Construct an instance. */
  private AvroToArrowVectorIterator(Decoder decoder, Schema schema, AvroToArrowConfig config) {

    this.decoder = decoder;
    this.schema = schema;
    this.config = config;
    this.targetBatchSize = config.getTargetBatchSize();
  }

  /** Create a ArrowVectorIterator to partially convert data. */
  public static AvroToArrowVectorIterator create(
      Decoder decoder, Schema schema, AvroToArrowConfig config) {

    AvroToArrowVectorIterator iterator = new AvroToArrowVectorIterator(decoder, schema, config);
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
    compositeConsumer = AvroToArrowUtils.createCompositeConsumer(schema, config);
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
        compositeConsumer.consume(decoder);
        readRowCount++;
      }

      if (targetBatchSize == NO_LIMIT_BATCH_SIZE) {
        while (true) {
          ValueVectorUtility.ensureCapacity(root, readRowCount + 1);
          compositeConsumer.consume(decoder);
          readRowCount++;
        }
      } else {
        while (readRowCount < targetBatchSize) {
          compositeConsumer.consume(decoder);
          readRowCount++;
        }
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
    final int targetBatchSize = config.getTargetBatchSize();
    if (targetBatchSize != NO_LIMIT_BATCH_SIZE) {
      ValueVectorUtility.preAllocate(root, targetBatchSize);
    }

    long validConsumerCount =
        compositeConsumer.getConsumers().stream().filter(c -> !c.skippable()).count();
    Preconditions.checkArgument(
        root.getFieldVectors().size() == validConsumerCount,
        "Schema root vectors size not equals to consumers size.");

    compositeConsumer.resetConsumerVectors(root);

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

  /** Gets the next vector. The user is responsible for freeing its resources. */
  @Override
  public VectorSchemaRoot next() {
    Preconditions.checkArgument(hasNext());
    VectorSchemaRoot returned = nextBatch;
    try {
      load(VectorSchemaRoot.create(rootSchema, config.getAllocator()));
    } catch (Exception e) {
      returned.close();
      throw new RuntimeException("Error occurs while getting next schema root.", e);
    }
    return returned;
  }

  /** Clean up resources. */
  @Override
  public void close() {
    if (nextBatch != null) {
      nextBatch.close();
    }
    compositeConsumer.close();
  }
}
