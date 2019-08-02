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

package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ArrowWriter} that writes out a Arrow files (https://arrow.apache.org/docs/format/IPC.html#file-format).
 */
public class ArrowFileWriter extends ArrowWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFileWriter.class);

  // All ArrowBlocks written are saved in these lists to be passed to ArrowFooter in endInternal.
  private final List<ArrowBlock> dictionaryBlocks = new ArrayList<>();
  private final List<ArrowBlock> recordBlocks = new ArrayList<>();

  public ArrowFileWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    super(root, provider, out);
  }

  @Override
  protected void startInternal(WriteChannel out) throws IOException {
    ArrowMagic.writeMagic(out, true);
  }

  @Override
  protected ArrowBlock writeDictionaryBatch(ArrowDictionaryBatch batch) throws IOException {
    ArrowBlock block = super.writeDictionaryBatch(batch);
    dictionaryBlocks.add(block);
    return block;
  }

  @Override
  protected ArrowBlock writeRecordBatch(ArrowRecordBatch batch) throws IOException {
    ArrowBlock block = super.writeRecordBatch(batch);
    recordBlocks.add(block);
    return block;
  }

  @Override
  protected void endInternal(WriteChannel out) throws IOException {
    out.writeIntLittleEndian(0);
    long footerStart = out.getCurrentPosition();
    out.write(new ArrowFooter(schema, dictionaryBlocks, recordBlocks), false);
    int footerLength = (int) (out.getCurrentPosition() - footerStart);
    if (footerLength <= 0) {
      throw new InvalidArrowFileException("invalid footer");
    }
    out.writeIntLittleEndian(footerLength);
    LOGGER.debug("Footer starts at {}, length: {}", footerStart, footerLength);
    ArrowMagic.writeMagic(out, false);
    LOGGER.debug("magic written, now at {}", out.getCurrentPosition());
  }

  @VisibleForTesting
  public List<ArrowBlock> getRecordBlocks() {
    return recordBlocks;
  }

  @VisibleForTesting
  public List<ArrowBlock> getDictionaryBlocks() {
    return dictionaryBlocks;
  }
}
