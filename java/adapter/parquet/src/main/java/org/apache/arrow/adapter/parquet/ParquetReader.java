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
import java.util.List;

import org.apache.arrow.adapter.common.ArrowRecordBatchBuilder;
import org.apache.arrow.adapter.common.ArrowRecordBatchBuilderImpl;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

/** Parquet Reader Class. */
public class ParquetReader implements AutoCloseable {

  /** reference to native reader instance. */
  private long nativeInstanceId;

  /** last readed length of a record batch. */
  private long lastReadLength;

  private BufferAllocator allocator;
  private ParquetReaderJniWrapper jniWrapper;

  /**
   * Create an instance for ParquetReader.
   *
   * @param path Parquet Reader File Path.
   * @param rowGroupIndices An array to indicate which rowGroup to read.
   * @param columnIndices An array to indicate which columns to read.
   * @param batchSize number of rows expected to be read in one batch.
   * @param allocator A BufferAllocator reference.
   * @throws IOException throws io exception in case of native failure.
   */
  public ParquetReader(
      String path,
      int[] rowGroupIndices,
      int[] columnIndices,
      long batchSize,
      BufferAllocator allocator)
      throws IOException {
    this.jniWrapper = new ParquetReaderJniWrapper();
    this.allocator = allocator;
    this.nativeInstanceId = jniWrapper.nativeOpenParquetReader(path, batchSize);
    jniWrapper.nativeInitParquetReader(nativeInstanceId, columnIndices, rowGroupIndices);
  }

  /**
   * Create an instance for ParquetReader.
   *
   * @param path Parquet Reader File Path.
   * @param startPos A start pos to indicate which rowGroup to read.
   * @param endPos An end pos indicate which rowGroup to read.
   * @param columnIndices An array to indicate which columns to read.
   * @param batchSize number of rows expected to be read in one batch.
   * @param allocator A BufferAllocator reference.
   * @throws IOException throws io exception in case of native failure.
   */
  public ParquetReader(
      String path,
      long startPos,
      long endPos,
      int[] columnIndices,
      long batchSize,
      BufferAllocator allocator)
      throws IOException {
    this.jniWrapper = new ParquetReaderJniWrapper();
    this.allocator = allocator;
    this.nativeInstanceId = jniWrapper.nativeOpenParquetReader(path, batchSize);
    jniWrapper.nativeInitParquetReader2(nativeInstanceId, columnIndices, startPos, endPos);
  }

  /**
   * Get Arrow Schema from ParquetReader.
   *
   * @return Schema of parquet file
   * @throws IOException throws io exception in case of native failure
   */
  Schema getSchema() throws IOException {
    byte[] schemaBytes = jniWrapper.nativeGetSchema(nativeInstanceId);

    try (MessageChannelReader schemaReader =
        new MessageChannelReader(
            new ReadChannel(new ByteArrayReadableSeekableByteChannel(schemaBytes)), allocator)) {

      MessageResult result = schemaReader.readNext();
      if (result == null) {
        throw new IOException("Unexpected end of input. Missing schema.");
      }

      return MessageSerializer.deserializeSchema(result.getMessage());
    }
  }

  /**
   * Read Next ArrowRecordBatch from ParquetReader.
   *
   * @return One ArrowRecordBatch readed from parquet file reader
   * @throws IOException throws io exception in case of native failure
   */
  ArrowRecordBatch readNext() throws IOException {
    ArrowRecordBatchBuilder recordBatchBuilder = jniWrapper.nativeReadNext(nativeInstanceId);
    if (recordBatchBuilder == null) {
      return null;
    }
    ArrowRecordBatchBuilderImpl recordBatchBuilderImpl =
        new ArrowRecordBatchBuilderImpl(recordBatchBuilder);
    ArrowRecordBatch batch = recordBatchBuilderImpl.build();
    this.lastReadLength = batch.getLength();
    return batch;
  }

  /**
   * Read Next ValueVectorList from ParquetReader.
   *
   * @return Next ValueVectorList readed from parquet file.
   * @throws IOException throws io exception in case of native failure
   */
  public List<FieldVector> readNextVectors(VectorSchemaRoot root) throws IOException {
    ArrowRecordBatch batch = readNext();
    if (batch == null) {
      return null;
    }
    VectorLoader loader = new VectorLoader(root);
    loader.load(batch);
    batch.close();
    return root.getFieldVectors();
  }

  /**
   * Get last readed ArrowRecordBatch Length.
   *
   * @return lastReadLength.
   */
  public long lastReadLength() {
    return lastReadLength;
  }

  @Override
  public void close() {
    jniWrapper.nativeCloseParquetReader(nativeInstanceId);
  }
}
