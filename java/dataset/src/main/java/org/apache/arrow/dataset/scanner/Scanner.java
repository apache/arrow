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

package org.apache.arrow.dataset.scanner;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A high level interface for scanning data over dataset.
 */
public interface Scanner extends AutoCloseable {

  /**
   * Read the dataset as a stream of record batches.
   *
   * @return a {@link ArrowReader}.
   */
  ArrowReader scanBatches();

  /**
   * Perform the scan operation.
   *
   * @return a iterable set of {@link ScanTask}s. Each task is considered independent and it is allowed
   *     to execute the tasks concurrently to gain better performance.
   * @deprecated use {@link #scanBatches()} instead.
   */
  @Deprecated
  Iterable<? extends ScanTask> scan();

  /**
   * Get the schema of this Scanner.
   *
   * @return the schema instance
   */
  Schema schema();
}
