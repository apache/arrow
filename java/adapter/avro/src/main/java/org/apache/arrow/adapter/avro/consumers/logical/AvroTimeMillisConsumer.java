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
package org.apache.arrow.adapter.avro.consumers.logical;

import java.io.IOException;
import org.apache.arrow.adapter.avro.consumers.BaseAvroConsumer;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume date time-millis values from avro decoder. Write the data to {@link
 * TimeMilliVector}.
 */
public class AvroTimeMillisConsumer extends BaseAvroConsumer<TimeMilliVector> {

  /** Instantiate a AvroTimeMilliConsumer. */
  public AvroTimeMillisConsumer(TimeMilliVector vector) {
    super(vector);
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    vector.set(currentIndex++, decoder.readInt());
  }
}
