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
import java.nio.ByteBuffer;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume string type values from avro decoder.
 * Write the data to {@link VarCharVector}.
 */
public class AvroStringConsumer implements Consumer {

  private final VarCharVector vector;
  private final VarCharWriter writer;
  private ByteBuffer cacheBuffer;

  public AvroStringConsumer(VarCharVector vector) {
    this.vector = vector;
    this.writer = new VarCharWriterImpl(vector);
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    VarCharHolder holder = new VarCharHolder();

    // cacheBuffer is initialized null and create in the first consume,
    // if its capacity < size to read, decoder will create a new one with new capacity.
    cacheBuffer = decoder.readBytes(cacheBuffer);

    holder.start = 0;
    holder.end = cacheBuffer.limit();
    holder.buffer = vector.getAllocator().buffer(cacheBuffer.limit());
    holder.buffer.setBytes(0, cacheBuffer, 0, cacheBuffer.limit());

    writer.write(holder);
    writer.setPosition(writer.getPosition() + 1);
  }
}
