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

package org.apache.arrow.adapter.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;

import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

/** Wrapper for Parquet Writer native API. */
public class ParquetWriter implements AutoCloseable {

  /** reference to native reader instance. */
  private long nativeInstanceId;

  private ParquetWriterJniWrapper jniWrapper;

  /**
   * Open native ParquetWriter Instance.
   *
   * @param path Parquet File Path to write.
   * @param schema arrow schema to initialize Parquet file.
   * @throws IOException throws io exception in case of native failure.
   */
  public ParquetWriter(String path, Schema schema)
      throws IOException {
    this.jniWrapper = new ParquetWriterJniWrapper();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    byte[] schemaBytes = out.toByteArray();
    this.nativeInstanceId = jniWrapper.nativeOpenParquetWriter(path, schemaBytes);
  }

  /**
   * Write Next ArrowRecordBatch to ParquetWriter.
   *
   * @param recordBatch next ArrowRecordBatch to write.
   * @throws IOException throws exception in case of io issues.
   */
  public void writeNext(ArrowRecordBatch recordBatch) throws IOException {
    // convert ArrowRecordBatch to buffer List
    int numRows = recordBatch.getLength();
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }
    jniWrapper.nativeWriteNext(nativeInstanceId, numRows, bufAddrs, bufSizes);
  }

  @Override
  public void close() throws IOException {
    jniWrapper.nativeCloseParquetWriter(nativeInstanceId);
  }
}
