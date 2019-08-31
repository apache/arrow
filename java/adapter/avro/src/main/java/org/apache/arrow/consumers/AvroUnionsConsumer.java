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

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume unions type values from avro decoder.
 * Write the data to {@link org.apache.arrow.vector.complex.UnionVector}.
 */
public class AvroUnionsConsumer implements Consumer<UnionVector> {

  private Consumer[] delegates;
  private Types.MinorType[] types;

  private UnionVector vector;
  private int currentIndex;

  /**
   * Instantiate an AvroUnionConsumer.
   */
  public AvroUnionsConsumer(UnionVector vector, Consumer[] delegates, Types.MinorType[] types) {

    this.vector = vector;
    this.delegates = delegates;
    this.types = types;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    int fieldIndex = decoder.readInt();

    Consumer delegate = delegates[fieldIndex];

    vector.setType(currentIndex, types[fieldIndex]);
    // In UnionVector we need to set sub vector writer position before consume a value
    // because in the previous iterations we might not have written to the specific union sub vector.
    delegate.setPosition(currentIndex);
    delegate.consume(decoder);

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
    vector.setValueCount(currentIndex);
    return this.vector;
  }

  @Override
  public void close() throws Exception {
    vector.close();
    AutoCloseables.close(delegates);
  }

  @Override
  public void resetValueVector(UnionVector vector) {
    this.vector = vector;
    for (int i = 0; i < delegates.length; i++) {
      delegates[i].resetValueVector(vector.getChildrenFromFields().get(i));
    }
    this.currentIndex = 0;
  }
}
