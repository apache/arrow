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

  public AvroBytesConsumer(VarBinaryVector vector) {
    this.vector = vector;
    this.writer = new VarBinaryWriterImpl(vector);
  }

  @Override
  public void consume(Decoder decoder) throws IOException {
    VarBinaryHolder holder = new VarBinaryHolder();
    ByteBuffer byteBuffer = decoder.readBytes(null);

    holder.start = 0;
    holder.end = byteBuffer.capacity();
    holder.buffer = vector.getAllocator().buffer(byteBuffer.capacity());
    holder.buffer.setBytes(0, byteBuffer);

    writer.write(holder);
    writer.setPosition(writer.getPosition() + 1);
  }
}
