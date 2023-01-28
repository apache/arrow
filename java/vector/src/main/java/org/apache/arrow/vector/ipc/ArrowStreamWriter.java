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
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

/**
 * Writer for the Arrow stream format to send ArrowRecordBatches over a WriteChannel.
 */
public class ArrowStreamWriter extends ArrowWriter {

  /**
   * Construct an ArrowStreamWriter with an optional DictionaryProvider for the OutputStream.
   *
   * @param root Existing VectorSchemaRoot with vectors to be written.
   * @param provider DictionaryProvider for any vectors that are dictionary encoded.
   *                 (Optional, can be null)
   * @param out OutputStream for writing.
   */
  public ArrowStreamWriter(VectorSchemaRoot root, DictionaryProvider provider, OutputStream out) {
    this(root, provider, Channels.newChannel(out));
  }

  /**
   * Construct an ArrowStreamWriter with an optional DictionaryProvider for the WritableByteChannel.
   */
  public ArrowStreamWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    this(root, provider, out, IpcOption.DEFAULT);
  }

  /**
   * Construct an ArrowStreamWriter with an optional DictionaryProvider for the WritableByteChannel.
   *
   * @param root Existing VectorSchemaRoot with vectors to be written.
   * @param provider DictionaryProvider for any vectors that are dictionary encoded.
   *                 (Optional, can be null)
   * @param option IPC write options
   * @param out WritableByteChannel for writing.
   */
  public ArrowStreamWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out,
      IpcOption option) {
    super(root, provider, out, option);
  }

  /**
   * Construct an ArrowStreamWriter with compression enabled.
   *
   * @param root Existing VectorSchemaRoot with vectors to be written.
   * @param provider DictionaryProvider for any vectors that are dictionary encoded.
   *                 (Optional, can be null)
   * @param option IPC write options
   * @param compressionFactory Compression codec factory
   * @param codecType Codec type
   * @param out WritableByteChannel for writing.
   */
  public ArrowStreamWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out,
                           IpcOption option, CompressionCodec.Factory compressionFactory,
                           CompressionUtil.CodecType codecType) {
    super(root, provider, out, option, compressionFactory, codecType);
  }

  /**
   * Write an EOS identifier to the WriteChannel.
   *
   * @param out Open WriteChannel with an active Arrow stream.
   * @param option IPC write option
   * @throws IOException on error
   */
  public static void writeEndOfStream(WriteChannel out, IpcOption option) throws IOException {
    if (!option.write_legacy_ipc_format) {
      out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
    }
    out.writeIntLittleEndian(0);
  }

  @Override
  protected void endInternal(WriteChannel out) throws IOException {
    writeEndOfStream(out, option);
  }
}
