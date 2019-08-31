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
import org.apache.arrow.vector.ZeroVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume null type values from avro decoder.
 * Corresponding to {@link org.apache.arrow.vector.ZeroVector}.
 */
public class AvroNullConsumer implements Consumer<ZeroVector> {

  private ZeroVector vector;

  public AvroNullConsumer(ZeroVector vector) {
    this.vector = vector;
  }

  @Override
  public void consume(Decoder decoder) throws IOException {}

  @Override
  public void addNull() {}

  @Override
  public void setPosition(int index) {}

  @Override
  public FieldVector getVector() {
    return this.vector;
  }

  @Override
  public void close() {
    vector.close();
  }

  @Override
  public void resetValueVector(ZeroVector vector) {
    this.vector = vector;
  }
}
