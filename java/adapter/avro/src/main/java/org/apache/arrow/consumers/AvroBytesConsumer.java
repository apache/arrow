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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.impl.VarBinaryWriterImpl;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume bytes type values from avro decoder.
 * Write the data to {@link VarBinaryVector}.
 */
public class AvroBytesConsumer implements Consumer {

  private final VarBinaryWriter writer;
  private final VarBinaryVector vector;
  private ByteBuffer cacheBuffer;

  /**
   * Instantiate a AvroBytesConsumer.
   */
  public AvroBytesConsumer(VarBinaryVector vector) {
    this.vector = vector;
    this.writer = new VarBinaryWriterImpl(vector);
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    writeValue(decoder);
    writer.setPosition(writer.getPosition() + 1);
  }

  @Override
  public void addNull() {
    writer.setPosition(writer.getPosition() + 1);
  }

  private void writeValue(Decoder decoder) throws IOException {
    VarBinaryHolder holder = new VarBinaryHolder();

    // cacheBuffer is initialized null and create in the first consume,
    // if its capacity < size to read, decoder will create a new one with new capacity.
    cacheBuffer = decoder.readBytes(cacheBuffer);

    holder.start = 0;
    holder.end = cacheBuffer.limit();
    holder.buffer = vector.getAllocator().buffer(cacheBuffer.limit());
    holder.buffer.setBytes(0, cacheBuffer, 0,  cacheBuffer.limit());
    writer.write(holder);
  }

  @Override
  public void setPosition(int index) {
    writer.setPosition(index);
  }

  @Override
  public FieldVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
