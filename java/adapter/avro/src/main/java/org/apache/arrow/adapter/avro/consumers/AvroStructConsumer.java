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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume nested record type values from avro decoder.
 * Write the data to {@link org.apache.arrow.vector.complex.StructVector}.
 */
public class AvroStructConsumer extends BaseAvroConsumer<StructVector> {

  private final Consumer[] delegates;

  /**
   * Instantiate a AvroStructConsumer.
   */
  public AvroStructConsumer(StructVector vector, Consumer[] delegates) {
    super(vector);
    this.delegates = delegates;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {

    ensureInnerVectorCapacity(currentIndex + 1);
    for (int i = 0; i < delegates.length; i++) {
      delegates[i].consume(decoder);
    }
    vector.setIndexDefined(currentIndex);
    currentIndex++;

  }

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(delegates);
  }

  @Override
  public boolean resetValueVector(StructVector vector) {
    for (int i = 0; i < delegates.length; i++) {
      delegates[i].resetValueVector(vector.getChildrenFromFields().get(i));
    }
    return super.resetValueVector(vector);
  }

  void ensureInnerVectorCapacity(long targetCapacity) {
    for (FieldVector v : vector.getChildrenFromFields()) {
      while (v.getValueCapacity() < targetCapacity) {
        v.reAlloc();
      }
    }
  }
}
