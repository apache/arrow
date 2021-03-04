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

package org.apache.arrow.dataset.jni;

import java.util.Arrays;
import java.util.List;

/**
 * Hold pointers to a Arrow C++ RecordBatch.
 */
public class NativeRecordBatchHandle {

  private final long numRows;
  private final List<Field> fields;
  private final List<Buffer> buffers;

  /**
   * Constructor.
   *
   * @param numRows Total row number of the associated RecordBatch
   * @param fields Metadata of fields
   * @param buffers Retained Arrow buffers
   */
  public NativeRecordBatchHandle(long numRows, Field[] fields, Buffer[] buffers) {
    this.numRows = numRows;
    this.fields = Arrays.asList(fields);
    this.buffers = Arrays.asList(buffers);
  }

  /**
   * Returns the total row number of the associated RecordBatch.
   * @return Total row number of the associated RecordBatch.
   */
  public long getNumRows() {
    return numRows;
  }

  /**
   * Returns Metadata of fields.
   * @return Metadata of fields.
   */
  public List<Field> getFields() {
    return fields;
  }

  /**
   * Returns the buffers.
   * @return Retained Arrow buffers.
   */
  public List<Buffer> getBuffers() {
    return buffers;
  }

  /**
   * Field metadata.
   */
  public static class Field {
    public final long length;
    public final long nullCount;

    public Field(long length, long nullCount) {
      this.length = length;
      this.nullCount = nullCount;
    }
  }

  /**
   * Pointers and metadata of the targeted Arrow buffer.
   */
  public static class Buffer {
    public final long nativeInstanceId;
    public final long memoryAddress;
    public final long size;
    public final long capacity;

    /**
     * Constructor.
     *
     * @param nativeInstanceId Native instance's id
     * @param memoryAddress Memory address of the first byte
     * @param size Size (in bytes)
     * @param capacity Capacity (in bytes)
     */
    public Buffer(long nativeInstanceId, long memoryAddress, long size, long capacity) {
      this.nativeInstanceId = nativeInstanceId;
      this.memoryAddress = memoryAddress;
      this.size = size;
      this.capacity = capacity;
    }
  }
}
