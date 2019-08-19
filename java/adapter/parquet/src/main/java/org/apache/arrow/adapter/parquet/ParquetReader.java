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
import java.lang.Exception;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Wrapper for Parquet Reader native API.
 */
public class ParquetReader {
  private ParquetReaderJniWrapper wrapper;
  private long parquetReaderHandler;
  private long lastReadLength;

  /**
   * Create an instance for ParquetReader.
   * @param wrapper An holder of ParquetReaderJniWrapper.
   * @param path Parquet Reader File Path.
   * @param rowGroupIndices An array to indicate which rowGroup to read.
   * @param columnIndices An array to indicate which column to read.
   * @param batchSize how many rows will be read in one batch.
   * @param useHdfs3 A flag to tell if ArrowParquetReader should use hdfs3.
   */
  public ParquetReader(ParquetReaderJniWrapper wrapper, String path, int[] rowGroupIndices,
                       int[] columnIndices, long batchSize, boolean useHdfs3) {
    this.wrapper = wrapper;
    parquetReaderHandler = wrapper.openParquetFile(path, rowGroupIndices, columnIndices,
                                                   batchSize, useHdfs3);
  }

  /**
   * Create an instance for ParquetReader.
   * @param wrapper An holder of ParquetReaderJniWrapper.
   * @param path Parquet Reader File Path.
   * @param rowGroupIndices An array to indicate which rowGroup to read.
   * @param columnIndices An array to indicate which column to read.
   * @param batchSize how many rows will be read in one batch.
   */
  public ParquetReader(ParquetReaderJniWrapper wrapper, String path, int[] rowGroupIndices,
                       int[] columnIndices, long batchSize) {
    this.wrapper = wrapper;
    parquetReaderHandler = wrapper.openParquetFile(path, rowGroupIndices, columnIndices,
                                                   batchSize, true);
  }

  /**
   * Create an instance for ParquetReader.
   * @param wrapper An holder of ParquetReaderJniWrapper.
   * @param path Parquet Reader File Path.
   * @param columnIndices An array to indicate which column to read.
   * @param startPos A start position to indicate rowGroup.
   * @param endPos A end position to indicate rowGroup.
   * @param batchSize how many rows will be read in one batch.
   * @param useHdfs3 A flag to tell if ArrowParquetReader should use hdfs3.
   */
  public ParquetReader(ParquetReaderJniWrapper wrapper, String path, int[] columnIndices,
                       long startPos, long endPos, long batchSize, boolean useHdfs3) {
    this.wrapper = wrapper;
    parquetReaderHandler = wrapper.openParquetFile(path, columnIndices, startPos, endPos,
                                                   batchSize, useHdfs3);
  }

  /**
   * Create an instance for ParquetReader.
   * @param wrapper An holder of ParquetReaderJniWrapper.
   * @param path Parquet Reader File Path.
   * @param columnIndices An array to indicate which column to read.
   * @param startPos A start position to indicate rowGroup.
   * @param endPos A end position to indicate rowGroup.
   * @param batchSize how many rows will be read in one batch.
   */
  public ParquetReader(ParquetReaderJniWrapper wrapper, String path, int[] columnIndices,
                       long startPos, long endPos, long batchSize) {
    this.wrapper = wrapper;
    parquetReaderHandler = wrapper.openParquetFile(path, columnIndices, startPos, endPos,
                                                   batchSize, true);
  }

  /**
   * close native ParquetReader Instance.
   */
  public void close() {
    wrapper.closeParquetFile(parquetReaderHandler);
  }

  /**
   * Read Next ArrowRecordBatch from ParquetReader.
   * @return readed ArrowRecordBatch.
   * @throws Exception throws exception in case of io issues.
   */
  public ArrowRecordBatch readNext() throws Exception {
    ArrowRecordBatch batch = wrapper.readNext(parquetReaderHandler);
    if (batch == null) {
      return null;
    }
    lastReadLength = batch.getLength();
    return batch;
  }

  /**
   * Get Arrow Schema from ParquetReader.
   * @return Arrow Schema.
   * @throws IOException throws exception in case of io issues.
   */
  public Schema getSchema() throws IOException {
    return wrapper.getSchema(parquetReaderHandler);
  }

  /**
   * Read Next ValueVectorList from ParquetReader.
   * @return readed ValueVectorList.
   * @throws Exception throws exception in case of io issues.
   */
  public List<FieldVector> readNextVectors(VectorSchemaRoot root) throws Exception {
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
   * @return lastReadLength.
   */
  public long lastReadLength() {
    return lastReadLength;
  }
}
