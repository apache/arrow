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

/**
 * JNI wrapper for orc stripe reader.
 */
class OrcStripeReaderJniWrapper {

  /**
   * Get the schema of current stripe.
   * @param readerId id of the stripe reader instance.
   * @return serialized schema.
   */
  static native byte[] getSchema(long readerId);

  /**
   * Load next record batch.
   * @param readerId id of the stripe reader instance.
   * @return loaded record batch, return null when reached
   *     the end of current stripe.
   */
  static native OrcRecordBatch next(long readerId);

  /**
   * Release resources of underlying reader.
   * @param readerId id of the stripe reader instance.
   */
  static native void close(long readerId);
}
