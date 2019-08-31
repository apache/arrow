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

package org.apache.arrow.consumers;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.io.Decoder;

/**
 * Composite consumer which hold all consumers.
 * It manages the consume and cleanup process.
 */
public class CompositeAvroConsumer implements AutoCloseable {

  private final List<Consumer> consumers;

  public List<Consumer> getConsumers() {
    return consumers;
  }

  public CompositeAvroConsumer(List<Consumer> consumers) {
    this.consumers = consumers;
  }

  /**
   * Consume decoder data and write into {@link VectorSchemaRoot}.
   */
  public void consume(Decoder decoder, VectorSchemaRoot root) throws IOException {
    for (Consumer consumer : consumers) {
      consumer.consume(decoder);
    }
  }

  /**
   * Reset vector of consumers with the given {@link VectorSchemaRoot}.
   */
  public void resetConsumerVectors(VectorSchemaRoot root) {
    for (int i = 0; i < root.getFieldVectors().size(); i++) {
      consumers.get(i).resetValueVector(root.getFieldVectors().get(i));
    }
  }

  @Override
  public void close() {
    // clean up
    try {
      AutoCloseables.close(consumers);
    } catch (Exception e) {
      throw new RuntimeException("Error occurs in close.", e);
    }
  }
}
