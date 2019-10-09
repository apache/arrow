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

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Wrapper for Parquet Writer native API.
 */
public class ParquetWriter {

  private long parquetWriterHandler;
  private ParquetWriterJniWrapper wrapper;

  /**
   * Open native ParquetWriter Instance.
   * @param wrapper ParquetWriterJniWrapper instance.
   * @param path Parquet File Path to write.
   * @param schema arrow schema to initialize Parquet file.
   * @param useHdfs3 A flag to tell if ArrowParquetReader should use hdfs3.
   * @param rep Replication num for Hdfs Write.
   * @throws IOException throws exception in case of io issues.
   */
  public ParquetWriter(ParquetWriterJniWrapper wrapper, String path, Schema schema,
                       boolean useHdfs3, int rep) throws IOException {
    this.wrapper = wrapper;
    parquetWriterHandler = wrapper.openParquetFile(path, schema, useHdfs3, rep);
  }

  /**
   * Open native ParquetWriter Instance.
   * @param wrapper ParquetWriterJniWrapper instance.
   * @param path Parquet File Path to write.
   * @param schema arrow schema to initialize Parquet file.
   * @throws IOException throws exception in case of io issues.
   */
  public ParquetWriter(ParquetWriterJniWrapper wrapper, String path, Schema schema)
      throws IOException {
    this.wrapper = wrapper;
    parquetWriterHandler = wrapper.openParquetFile(path, schema, true, 1);
  }

  /**
   * close native ParquetWriter Instance.
   * @throws IOException throws exception in case of io issues.
   */
  public void close() throws IOException {
    wrapper.closeParquetFile(parquetWriterHandler);
  }

  /**
   * Write Next ArrowRecordBatch to ParquetWriter.
   * @param recordBatch next ArrowRecordBatch to write.
   * @throws IOException throws exception in case of io issues.
   */
  public void writeNext(ArrowRecordBatch recordBatch) throws IOException {
    wrapper.writeNext(parquetWriterHandler, recordBatch);
  }
}
