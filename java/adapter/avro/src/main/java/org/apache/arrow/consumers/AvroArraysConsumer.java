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

import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume int type values from avro decoder.
 * Write the data to {@link ListVector}.
 */
public class AvroArraysConsumer implements Consumer {

  private final ListVector vector;

  private final Consumer delegate;

  /**
   * Indicated whether has read the first block of this array.
   */
  private boolean firstRead;

  /**
   * Instantiate a ArrayConsumer.
   */
  public AvroArraysConsumer(ListVector vector, Consumer delegete) {
    this.vector = vector;
    this.delegate = delegete;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {

    long count;
    if (!firstRead) {
      count = decoder.readArrayStart();
      firstRead = true;
    } else {
      do {
        count = decoder.arrayNext();
      } while (count == 0);
    }

    int idx = vector.getValueCount();
    vector.startNewValue(idx);
    for (int i = 0; i < count; i++) {
      delegate.consume(decoder);
    }

    int end = (int) (vector.getOffsetBuffer().getInt(idx * 4) + count);
    vector.getOffsetBuffer().setInt((idx + 1) * 4, end);
    BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), vector.getValueCount());

    vector.setValueCount(idx + 1);
  }

  @Override
  public void addNull() {
    vector.setValueCount(vector.getValueCount() + 1);
  }

  @Override
  public void setPosition(int index) {
    vector.startNewValue(index);
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
}
