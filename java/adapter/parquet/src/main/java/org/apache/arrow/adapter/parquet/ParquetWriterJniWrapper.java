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

/**
 * Wrapper for Parquet Writer native API.
 */
public class ParquetWriterJniWrapper {
  private native long nativeOpenParquetWriter(String path, byte[] schemaBytes);

  private native void nativeInitParquetWriter(long nativeHandler, boolean useHdfs3, int rep);

  private native void nativeCloseParquetWriter(long nativeHandler);

  private native void nativeWriteNext(
      long nativeHandler, int numRows, long[] bufAddrs, long[] bufSizes);

  /**
   * Create an instance for ParquetWriterJniWrapper.
   * @throws IOException throws exception in case failed to open library.
   */
  public ParquetWriterJniWrapper() throws IOException {
    ParquetJniUtils.getInstance();
  }

  /**
   * Open native ParquetWriter Instance.
   * @param path Parquet File Path to write.
   * @param schema arrow schema to initialize Parquet file.
   * @param useHdfs3 A flag to tell if ArrowParquetReader should use hdfs3.
   * @param replication Replication num for Hdfs Write.
   * @return native instance handler.
   * @throws IOException throws exception in case of io issues.
   */
  long openParquetFile(String path, Schema schema, boolean useHdfs3, int replication)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema);
    byte[] schemaBytes = out.toByteArray(); 
    long nativeHandler = nativeOpenParquetWriter(path, schemaBytes);
    nativeInitParquetWriter(nativeHandler, useHdfs3, replication);
    return nativeHandler;
  }

  /**
   * close native ParquetWriter Instance.
   * @param nativeHandler native ParquetWriter Handler.
   */
  void closeParquetFile(long nativeHandler) throws IOException {
    nativeCloseParquetWriter(nativeHandler);
  }

  /**
   * Write Next ArrowRecordBatch to ParquetWriter.
   * @param nativeHandler native ParquetWriter Handler.
   * @param recordBatch next ArrowRecordBatch to write.
   */
  void writeNext(long nativeHandler, ArrowRecordBatch recordBatch) throws IOException {
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
    nativeWriteNext(nativeHandler, numRows, bufAddrs, bufSizes);
  }
}
