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

/** Wrapper for Parquet Writer native API. */
public class ParquetWriterJniWrapper {

  /** Construct a Jni Instance. */
  public ParquetWriterJniWrapper() throws IOException {
    ParquetJniUtils.getInstance();
  }

  /**
   * Construct a parquet file reader over the target file name.
   *
   * @param path absolute file path of target file
   * @param schemaBytes a byte array of Schema serialized output
   * @return long id of the parquet writer instance
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native long nativeOpenParquetWriter(String path, byte[] schemaBytes);

  /**
   * Close a parquet file writer.
   *
   * @param id parquet writer instance number
   */
  public native void nativeCloseParquetWriter(long id);

  /**
   * Write next record batch to parquet file writer.
   *
   * @param id parquet writer instance number
   * @param numRows number of Rows in this batch
   * @param bufAddrs a array of buffers address of this batch
   * @param bufSizes a array of buffers size of this batch
   * @throws IOException throws exception in case of any io exception in native codes
   */
  public native void nativeWriteNext(long id, int numRows, long[] bufAddrs, long[] bufSizes);
}
