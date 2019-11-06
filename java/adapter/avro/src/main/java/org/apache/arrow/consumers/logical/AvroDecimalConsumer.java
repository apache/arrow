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

package org.apache.arrow.consumers.logical;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.arrow.consumers.BaseAvroConsumer;
import org.apache.arrow.vector.DecimalVector;
import org.apache.avro.io.Decoder;

/**
 * Consumer which consume decimal type values from avro decoder.
 * Write the data to {@link DecimalVector}.
 */
public abstract class AvroDecimalConsumer extends BaseAvroConsumer<DecimalVector> {

  protected int scale;

  /**
   * Instantiate a AvroDecimalConsumer.
   */
  public AvroDecimalConsumer(DecimalVector vector) {
    super(vector);
    scale = vector.getScale();
  }

  /**
   * Consumer for decimal logical type with original bytes type.
   */
  public static class BytesDecimalConsumer extends AvroDecimalConsumer {

    private ByteBuffer cacheBuffer;

    public BytesDecimalConsumer(DecimalVector vector) {
      super(vector);
    }

    @Override
    public void consume(Decoder decoder) throws IOException {
      cacheBuffer = decoder.readBytes(cacheBuffer);
      byte[] bytes = new byte[cacheBuffer.limit()];
      cacheBuffer.get(bytes);
      BigDecimal decimal = new BigDecimal(new BigInteger(bytes), scale);
      vector.setSafe(currentIndex++, decimal);
    }

  }

  /**
   * Consumer for decimal logical type with original fixed type.
   */
  public static class FixedDecimalConsumer extends AvroDecimalConsumer {

    private byte[] reuseBytes;

    public FixedDecimalConsumer(DecimalVector vector, int size) {
      super(vector);
      reuseBytes = new byte[size];
    }

    @Override
    public void consume(Decoder decoder) throws IOException {
      decoder.readFixed(reuseBytes);
      BigDecimal decimal = new BigDecimal(new BigInteger(reuseBytes), scale);
      vector.setSafe(currentIndex++, decimal);
    }
  }
}
