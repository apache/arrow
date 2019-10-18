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
import org.apache.arrow.vector.ValueVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer holds a primitive consumer, could consume nullable values from avro decoder.
 * Write data via writer of delegate consumer.
 */
public class NullableTypeConsumer implements Consumer {


  private final Consumer delegate;

  /**
   * Null field index in avro schema.
   */
  protected int nullIndex;

  public NullableTypeConsumer(Consumer delegate, int nullIndex) {
    this.delegate = delegate;
    this.nullIndex = nullIndex;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    if (nullIndex != decoder.readInt()) {
      delegate.consume(decoder);
    } else {
      addNull();
    }
  }

  @Override
  public void addNull() {
    delegate.addNull();
  }

  @Override
  public void setPosition(int index) {
    delegate.setPosition(index);
  }

  @Override
  public FieldVector getVector() {
    return delegate.getVector();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public void resetValueVector(ValueVector vector) {
    this.delegate.resetValueVector(vector);
  }

}
