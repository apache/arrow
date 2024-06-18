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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ArrowWriter} that writes out a Arrow files
 * (https://arrow.apache.org/docs/format/IPC.html#file-format).
 */
public class ArrowFileWriter extends ArrowWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFileWriter.class);

  // All ArrowBlocks written are saved in these lists to be passed to ArrowFooter in endInternal.
  private final List<ArrowBlock> dictionaryBlocks = new ArrayList<>();
  private final List<ArrowBlock> recordBlocks = new ArrayList<>();

  private Map<String, String> metaData;
  private boolean dictionariesWritten = false;

  public ArrowFileWriter(
      VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    super(root, provider, out);
  }

  public ArrowFileWriter(
      VectorSchemaRoot root,
      DictionaryProvider provider,
      WritableByteChannel out,
      Map<String, String> metaData) {
    super(root, provider, out);
    this.metaData = metaData;
  }

  public ArrowFileWriter(
      VectorSchemaRoot root,
      DictionaryProvider provider,
      WritableByteChannel out,
      IpcOption option) {
    super(root, provider, out, option);
  }

  public ArrowFileWriter(
      VectorSchemaRoot root,
      DictionaryProvider provider,
      WritableByteChannel out,
      Map<String, String> metaData,
      IpcOption option) {
    super(root, provider, out, option);
    this.metaData = metaData;
  }

  public ArrowFileWriter(
      VectorSchemaRoot root,
      DictionaryProvider provider,
      WritableByteChannel out,
      Map<String, String> metaData,
      IpcOption option,
      CompressionCodec.Factory compressionFactory,
      CompressionUtil.CodecType codecType) {
    this(root, provider, out, metaData, option, compressionFactory, codecType, Optional.empty());
  }

  public ArrowFileWriter(
      VectorSchemaRoot root,
      DictionaryProvider provider,
      WritableByteChannel out,
      Map<String, String> metaData,
      IpcOption option,
      CompressionCodec.Factory compressionFactory,
      CompressionUtil.CodecType codecType,
      Optional<Integer> compressionLevel) {
    super(root, provider, out, option, compressionFactory, codecType, compressionLevel);
    this.metaData = metaData;
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
    if (!option.write_legacy_ipc_format) {
      out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
    }
    out.writeIntLittleEndian(0);

    long footerStart = out.getCurrentPosition();
    out.write(
        new ArrowFooter(schema, dictionaryBlocks, recordBlocks, metaData, option.metadataVersion),
        false);
    int footerLength = (int) (out.getCurrentPosition() - footerStart);
    if (footerLength <= 0) {
      throw new InvalidArrowFileException("invalid footer");
    }
    out.writeIntLittleEndian(footerLength);
    LOGGER.debug("Footer starts at {}, length: {}", footerStart, footerLength);
    ArrowMagic.writeMagic(out, false);
    LOGGER.debug("magic written, now at {}", out.getCurrentPosition());
  }

  @Override
  protected void ensureDictionariesWritten(DictionaryProvider provider, Set<Long> dictionaryIdsUsed)
      throws IOException {
    if (dictionariesWritten) {
      return;
    }
    dictionariesWritten = true;
    // Write out all dictionaries required.
    // Replacement dictionaries are not supported in the IPC file format.
    for (long id : dictionaryIdsUsed) {
      Dictionary dictionary = provider.lookup(id);
      writeDictionaryBatch(dictionary);
    }
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
