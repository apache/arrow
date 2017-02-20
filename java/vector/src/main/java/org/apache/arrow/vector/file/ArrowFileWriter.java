/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.file;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class ArrowFileWriter extends ArrowWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  public ArrowFileWriter(Schema schema, WritableByteChannel out, BufferAllocator allocator) {
    super(schema, out, allocator);
  }

  public ArrowFileWriter(List<Field> fields, List<FieldVector> vectors, WritableByteChannel out) {
    super(fields, vectors, out, false);
  }

  @Override
  protected void startInternal(WriteChannel out) throws IOException {
    writeMagic(out);
  }

  @Override
  protected void endInternal(WriteChannel out, List<ArrowBlock> dictionaries, List<ArrowBlock> records) throws IOException {
    long footerStart = out.getCurrentPosition();
    out.write(new ArrowFooter(getSchema(), dictionaries, records), false);
    int footerLength = (int)(out.getCurrentPosition() - footerStart);
    if (footerLength <= 0) {
      throw new InvalidArrowFileException("invalid footer");
    }
    out.writeIntLittleEndian(footerLength);
    LOGGER.debug(String.format("Footer starts at %d, length: %d", footerStart, footerLength));
    writeMagic(out);
  }

  private void writeMagic(WriteChannel out) throws IOException {
    out.write(ArrowFileReader.MAGIC);
    LOGGER.debug(String.format("magic written, now at %d", out.getCurrentPosition()));
  }
}
