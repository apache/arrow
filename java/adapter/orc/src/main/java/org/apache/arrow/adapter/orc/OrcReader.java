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

package org.apache.arrow.adapter.orc;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 *  Orc Reader that allow accessing orc stripes in Orc file.
 *  This orc reader basically acts like an ArrowReader iterator that
 *  iterate over orc stripes. Each stripe will be accessed via an
 *  ArrowReader.
 */
public class OrcReader implements AutoCloseable {
  private final OrcReaderJniWrapper jniWrapper;
  private BufferAllocator allocator;

  /**
   * reference to native reader instance.
   */
  private final long nativeInstanceId;

  /**
   * Create an OrcReader that iterate over orc stripes.
   * @param filePath file path to target file, currently only support local file.
   * @param allocator allocator provided to ArrowReader.
   * @throws IOException throws exception in case of file not found
   */
  public OrcReader(String filePath, BufferAllocator allocator) throws IOException, IllegalAccessException {
    this.allocator = allocator;
    this.jniWrapper = OrcReaderJniWrapper.getInstance();
    this.nativeInstanceId = jniWrapper.open(filePath);
  }

  /**
   * Seek to designated row. Invoke NextStripeReader() after seek
   * will return stripe reader starting from designated row.
   * @param rowNumber the rows number to seek
   * @return true if seek operation is succeeded
   */
  public boolean seek(int rowNumber) throws IllegalArgumentException  {
    return jniWrapper.seek(nativeInstanceId, rowNumber);
  }

  /**
   * Get a stripe level ArrowReader with specified batchSize in each record batch.
   *
   * @param batchSize the number of rows loaded on each iteration
   * @return ArrowReader that iterate over current stripes
   */
  public ArrowReader nextStripeReader(long batchSize) throws IllegalArgumentException {
    long stripeReaderId = jniWrapper.nextStripeReader(nativeInstanceId, batchSize);
    if (stripeReaderId < 0) {
      return null;
    }

    return new OrcStripeReader(stripeReaderId, allocator);
  }

  /**
   * The number of stripes in the file.
   *
   * @return number of stripes
   */
  public int getNumberOfStripes() throws IllegalArgumentException  {
    return jniWrapper.getNumberOfStripes(nativeInstanceId);
  }

  @Override
  public void close() throws Exception {
    jniWrapper.close(nativeInstanceId);
  }
}
