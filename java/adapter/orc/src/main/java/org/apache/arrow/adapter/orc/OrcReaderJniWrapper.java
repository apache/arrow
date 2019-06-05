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

/**
 * JNI wrapper for Orc reader.
 */
class OrcReaderJniWrapper {

  private static volatile OrcReaderJniWrapper INSTANCE;

  static OrcReaderJniWrapper getInstance() throws IOException, IllegalAccessException {
    if (INSTANCE == null) {
      synchronized (OrcReaderJniWrapper.class) {
        if (INSTANCE == null) {
          OrcJniUtils.loadOrcAdapterLibraryFromJar();
          INSTANCE = new OrcReaderJniWrapper();
        }
      }
    }

    return INSTANCE;
  }

  /**
   * Construct a orc file reader over the target file.
   * @param fileName absolute file path of target file
   * @return id of the orc reader instance if file opened successfully,
   *     otherwise return error code * -1.
   */
  native long open(String fileName);

  /**
   * Release resources associated with designated reader instance.
   * @param readerId id of the reader instance.
   */
  native void close(long readerId);

  /**
   *  Seek to designated row. Invoke nextStripeReader() after seek
   *  will return id of stripe reader starting from designated row.
   * @param readerId id of the reader instance
   * @param rowNumber the rows number to seek
   * @return true if seek operation is succeeded
   */
  native boolean seek(long readerId, int rowNumber);

  /**
   * The number of stripes in the file.
   * @param readerId id of the reader instance
   * @return number of stripes
   */
  native int getNumberOfStripes(long readerId);

  /**
   * Get a stripe level ArrowReader with specified batchSize in each record batch.
   * @param readerId id of the reader instance
   * @param batchSize the number of rows loaded on each iteration
   * @return id of the stripe reader instance.
   */
  native long nextStripeReader(long readerId, long batchSize);
}
