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
import java.util.Map;

import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume unions type values from avro decoder.
 * Write the data to {@link org.apache.arrow.vector.complex.UnionVector}.
 */
public class AvroUnionsConsumer implements Consumer {

  private Map<Integer, Consumer> indexDelegates;
  private Map<Integer, Types.MinorType> types;

  private UnionWriter writer;
  private UnionVector vector;

  /**
   * Instantiate a AvroUnionConsumer.
   */
  public AvroUnionsConsumer(UnionVector vector, Map<Integer, Consumer> indexDelegates,
      Map<Integer, Types.MinorType> types) {

    this.writer = new UnionWriter(vector);
    this.vector = vector;
    this.indexDelegates = indexDelegates;
    this.types = types;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    int fieldIndex = decoder.readInt();
    int position = writer.getPosition();

    Consumer delegate = indexDelegates.get(fieldIndex);
    // null value
    if (delegate == null) {
      // do nothing
    } else {
      vector.setType(position, types.get(fieldIndex));
      delegate.consume(decoder, position);
    }

    writer.setPosition(position + 1);

  }

  @Override
  public void consume(Decoder decoder, int index) throws IOException {
    int fieldIndex = decoder.readInt();
    writer.setPosition(index + 1);

    Consumer delegate = indexDelegates.get(fieldIndex);
    if (delegate == null) {
      // do nothing
    } else {
      vector.setType(index, types.get(fieldIndex));
      delegate.consume(decoder, index);
    }
    writer.setPosition(index + 1);
  }

  @Override
  public void addNull() {
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void addNull(int index) {
    writer.setPosition(index + 1);
  }
}
