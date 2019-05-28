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

package org.apache.arrow.vector.ipc.message;

/**
 * Interface for Arrow IPC messages (https://arrow.apache.org/docs/format/IPC.html).
 */
public interface ArrowMessage extends FBSerializable, AutoCloseable {

  int computeBodyLength();

  <T> T accepts(ArrowMessageVisitor<T> visitor);

  /**
   * Visitor interface for implementations of {@link ArrowMessage}.
   *
   * @param <T> The type of value to return after visiting.
   */
  static interface ArrowMessageVisitor<T> {
    T visit(ArrowDictionaryBatch message);

    T visit(ArrowRecordBatch message);
  }
}
