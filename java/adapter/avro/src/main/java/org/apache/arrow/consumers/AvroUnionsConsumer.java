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
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume unions type values from avro decoder.
 * Write the data to {@link org.apache.arrow.vector.complex.UnionVector}.
 */
public class AvroUnionsConsumer implements Consumer {

  private Consumer[] indexDelegates;
  private Types.MinorType[] types;

  private UnionWriter writer;
  private UnionVector vector;

  /**
   * Instantiate a AvroUnionConsumer.
   */
  public AvroUnionsConsumer(UnionVector vector, Consumer[] indexDelegates, Types.MinorType[] types) {

    this.writer = new UnionWriter(vector);
    this.vector = vector;
    this.indexDelegates = indexDelegates;
    this.types = types;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    int fieldIndex = decoder.readInt();
    int position = writer.getPosition();

    Consumer delegate = indexDelegates[fieldIndex];

    vector.setType(position, types[fieldIndex]);
    // In UnionVector we need to set sub vector writer position before consume a value
    // because in the previous iterations we might not have written to the specific union sub vector.
    delegate.setPosition(position);
    delegate.consume(decoder);

    writer.setPosition(position + 1);

  }

  @Override
  public void addNull() {
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void setPosition(int index) {
    writer.setPosition(index);
  }

  @Override
  public FieldVector getVector() {
    vector.setValueCount(writer.getPosition());
    return this.vector;
  }
}
