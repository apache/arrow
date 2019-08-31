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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume array type values from avro decoder.
 * Write the data to {@link ListVector}.
 */
public class AvroArraysConsumer implements Consumer<ListVector> {

  private ListVector vector;
  private final Consumer delegate;

  private int currentIndex = 0;

  /**
   * Instantiate a ArrayConsumer.
   */
  public AvroArraysConsumer(ListVector vector, Consumer delegate) {
    this.vector = vector;
    this.delegate = delegate;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {

    vector.startNewValue(currentIndex);
    long totalCount = 0;
    for (long count = decoder.readArrayStart(); count != 0; count = decoder.arrayNext()) {
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
    currentIndex++;
  }

  @Override
  public void setPosition(int index) {
    currentIndex = index;
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
  public void resetValueVector(ListVector vector) {
    this.currentIndex = 0;
    this.vector = vector;
    this.delegate.resetValueVector(vector.getDataVector());
  }
}
