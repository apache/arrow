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
package org.apache.arrow.adapter.avro.consumers;

import java.io.IOException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.Types;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume unions type values from avro decoder. Write the data to {@link
 * org.apache.arrow.vector.complex.UnionVector}.
 */
public class AvroUnionsConsumer extends BaseAvroConsumer<UnionVector> {

  private Consumer[] delegates;
  private Types.MinorType[] types;

  /** Instantiate an AvroUnionConsumer. */
  public AvroUnionsConsumer(UnionVector vector, Consumer[] delegates, Types.MinorType[] types) {

    super(vector);
    this.delegates = delegates;
    this.types = types;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    int fieldIndex = decoder.readInt();

    ensureInnerVectorCapacity(currentIndex + 1, fieldIndex);
    Consumer delegate = delegates[fieldIndex];

    vector.setType(currentIndex, types[fieldIndex]);
    // In UnionVector we need to set sub vector writer position before consume a value
    // because in the previous iterations we might not have written to the specific union sub
    // vector.
    delegate.setPosition(currentIndex);
    delegate.consume(decoder);

    currentIndex++;
  }

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(delegates);
  }

  @Override
  public boolean resetValueVector(UnionVector vector) {
    for (int i = 0; i < delegates.length; i++) {
      delegates[i].resetValueVector(vector.getChildrenFromFields().get(i));
    }
    return super.resetValueVector(vector);
  }

  void ensureInnerVectorCapacity(long targetCapacity, int fieldIndex) {
    ValueVector fieldVector = vector.getChildrenFromFields().get(fieldIndex);
    if (fieldVector.getMinorType() == Types.MinorType.NULL) {
      return;
    }
    while (fieldVector.getValueCapacity() < targetCapacity) {
      fieldVector.reAlloc();
    }
  }
}
