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

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowWriter implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  private final WriteChannel out;

  private final Schema schema;

  private final List<ArrowBlock> recordBatches = new ArrayList<>();
  private boolean started = false;

  public ArrowWriter(WritableByteChannel out, Schema schema) {
    this.out = new WriteChannel(out);
    this.schema = schema;
  }

  private void start() throws IOException {
    writeMagic();
    MessageSerializer.serialize(out, schema);
  }

  // TODO: write dictionaries

  public void writeRecordBatch(ArrowRecordBatch recordBatch) throws IOException {
    checkStarted();
    ArrowBlock batchDesc = MessageSerializer.serialize(out, recordBatch);
    LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d",
        batchDesc.getOffset(), batchDesc.getMetadataLength(), batchDesc.getBodyLength()));

    // add metadata to footer
    recordBatches.add(batchDesc);
  }

  private void checkStarted() throws IOException {
    if (!started) {
      started = true;
      start();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      long footerStart = out.getCurrentPosition();
      writeFooter();
      int footerLength = (int)(out.getCurrentPosition() - footerStart);
      if (footerLength <= 0 ) {
        throw new InvalidArrowFileException("invalid footer");
      }
      out.writeIntLittleEndian(footerLength);
      LOGGER.debug(String.format("Footer starts at %d, length: %d", footerStart, footerLength));
      writeMagic();
    } finally {
      out.close();
    }
  }

  private void writeMagic() throws IOException {
    out.write(ArrowReader.MAGIC);
    LOGGER.debug(String.format("magic written, now at %d", out.getCurrentPosition()));
  }

  private void writeFooter() throws IOException {
    // TODO: dictionaries
    out.write(new ArrowFooter(schema, Collections.<ArrowBlock>emptyList(), recordBatches), false);
  }
}
