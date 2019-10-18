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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume map type values from avro decoder.
 * Write the data to {@link MapVector}.
 */
public class AvroMapConsumer implements Consumer<MapVector> {

  private MapVector vector;
  private final Consumer delegate;

  private int currentIndex;

  /**
   * Instantiate a AvroMapConsumer.
   */
  public AvroMapConsumer(MapVector vector, Consumer delegate) {
    this.vector = vector;
    this.delegate = delegate;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {

    vector.startNewValue(currentIndex);
    long totalCount = 0;
    for (long count = decoder.readMapStart(); count != 0; count = decoder.mapNext()) {
      totalCount += count;
      for (int element = 0; element < count; element++) {
        delegate.consume(decoder);
      }
    }
    vector.endValue(currentIndex, (int) totalCount);
    currentIndex++;
  }

  @Override
  public void addNull() {
    vector.setValueCount(vector.getValueCount() + 1);
  }

  @Override
  public void setPosition(int index) {
    this.currentIndex = index;
  }

  @Override
  public FieldVector getVector() {
    return this.vector;
  }

  @Override
  public void close() throws Exception {
    vector.close();
    delegate.close();
  }

  @Override
  public void resetValueVector(MapVector vector) {
    this.vector = vector;
    this.delegate.resetValueVector(vector.getDataVector());
    this.currentIndex = 0;
  }
}
