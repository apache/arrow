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

package org.apache.arrow.c;

/**
 * Base interface for C Data Interface structures.
 */
public interface BaseStruct extends AutoCloseable {
  /**
   * Get memory address.
   * 
   * @return Memory address
   */
  long memoryAddress();

  /**
   * Call the release callback of an ArrowArray.
   * <p>
   * This function must not be called for child arrays.
   */
  void release();

  /**
   * Close to release the main buffer.
   */
  @Override
  void close();
}
