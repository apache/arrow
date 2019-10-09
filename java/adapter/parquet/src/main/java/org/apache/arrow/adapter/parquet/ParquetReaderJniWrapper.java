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

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

/**
 * Wrapper for Parquet Reader native API.
 */
public class ParquetReaderJniWrapper {
  private native long nativeOpenParquetReader(String path);

  private native void nativeInitParquetReader(long nativeHandler, int[] columnIndices,
                                              int[] rowGroupIndices, long batchSize,
                                              boolean useHdfs3);

  private native void nativeInitParquetReader2(long nativeHandler, int[] columnIndices,
                                               long startPos, long endPos,
                                               long batchSize, boolean useHdfs3);

  private native void nativeCloseParquetReader(long nativeHandler);

  private native ArrowRecordBatchBuilder nativeReadNext(long nativeHandler);

  private native byte[] nativeGetSchema(long nativeHandler);

  private BufferAllocator allocator;

  /**
   * Create an instance for ParquetReaderJniWrapper.
   * @param allocator An pre-created BufferAllocator.
   */
  public ParquetReaderJniWrapper(BufferAllocator allocator)
      throws IOException {
    ParquetJniUtils.getInstance();
    this.allocator = allocator;
  }

  /**
   * Open Parquet File as ParquetReader.
   * @param path Parquet Reader File Path.
   * @param rowGroupIndices An array to indicate which rowGroup to read.
   * @param columnIndices An array to indicate which column to read.
   * @param batchSize how many rows will be read in one batch.
   * @param useHdfs3 A flag to tell if ArrowParquetReader should use hdfs3.
   * @return native ParquetReader handler.
   * @throws IOException throws exception in case of io issues.
   */
  long openParquetFile(String path, int[] rowGroupIndices, int[] columnIndices, long batchSize,
                       boolean useHdfs3) throws IOException {
    long nativeHandler = nativeOpenParquetReader(path);
    nativeInitParquetReader(nativeHandler, columnIndices, rowGroupIndices, batchSize, useHdfs3);
    return nativeHandler;
  }

  /**
   * Open Parquet File as ParquetReader.
   * @param path Parquet Reader File Path.
   * @param columnIndices An array to indicate which column to read.
   * @param startPos A start position to indicate rowGroup.
   * @param endPos A end position to indicate rowGroup.
   * @param batchSize how many rows will be read in one batch.
   * @param useHdfs3 A flag to tell if ArrowParquetReader should use hdfs3.
   * @return native ParquetReader handler.
   * @throws IOException throws exception in case of io issues.
   */
  long openParquetFile(String path, int[] columnIndices, long startPos, long endPos,
                       long batchSize, boolean useHdfs3) throws IOException {
    long nativeHandler = nativeOpenParquetReader(path);
    nativeInitParquetReader2(nativeHandler, columnIndices, startPos, endPos, batchSize, useHdfs3);
    return nativeHandler;
  }

  /**
   * close native ParquetReader Instance.
   * @param nativeHandler native ParquetReader handler.
   */
  void closeParquetFile(long nativeHandler) {
    nativeCloseParquetReader(nativeHandler);
  }

  /**
   * Get Arrow Schema from ParquetReader.
   * @param nativeHandler native ParquetReader handler.
   * @return Arrow Schema.
   * @throws IOException throws exception in case of io issues.
   */
  Schema getSchema(long nativeHandler) throws IOException {
    byte[] schemaBytes = nativeGetSchema(nativeHandler);

    try (MessageChannelReader schemaReader =
           new MessageChannelReader(
                new ReadChannel(
                new ByteArrayReadableSeekableByteChannel(schemaBytes)), allocator)) {

      MessageResult result = schemaReader.readNext();
      if (result == null) {
        throw new IOException("Unexpected end of input. Missing schema.");
      }

      return MessageSerializer.deserializeSchema(result.getMessage());
    }
  }

  /**
   * Read Next ArrowRecordBatch from ParquetReader.
   * @param nativeHandler native ParquetReader handler.
   * @return readed ArrowRecordBatch.
   * @throws IOException throws exception in case of io issues.
   */
  ArrowRecordBatch readNext(long nativeHandler) throws IOException {
    ArrowRecordBatchBuilder recordBatchBuilder = nativeReadNext(nativeHandler);
    ArrowRecordBatchBuilderImpl recordBatchBuilderImpl =
        new ArrowRecordBatchBuilderImpl(recordBatchBuilder);
    if (recordBatchBuilder == null) {
      return null;
    }
    return recordBatchBuilderImpl.build();
  }
}
