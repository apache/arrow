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

package org.apache.arrow.dataset.substrait;

import java.util.Map;

import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Java binding of the C++ Substrait API.
 */
public interface Substrait {
  /**
   * Execute Substrait Plan to read Schema and ArrowRecordBatches for Local Files.
   *
   * @param plan the JSON Substrait plan.
   * @return the ArrowReader to iterate for record batches.
   */
  ArrowReader runQueryLocalFiles(String plan);

  /**
   * Execute JSON Substrait Plan to read Schema and ArrowRecordBatches for Named Tables.
   *
   * @param plan                  the JSON Substrait plan.
   * @param mapTableToArrowReader the mapping name of Tables Name and theirs memory addres representation.
   * @return the ArrowReader to iterate for record batches.
   */
  ArrowReader runQueryNamedTables(String plan, Map<String, ArrowReader> mapTableToArrowReader);


  /**
   * Execute binary Substrait Plan to read Schema and ArrowRecordBatches for Named Tables.
   *
   * @param plan                  the binary Substrait plan.
   * @param mapTableToArrowReader the mapping name of Tables Name and theirs memory addres representation.
   * @return the ArrowReader to iterate for record batches.
   */
  ArrowReader runQueryNamedTables(Object plan, Map<String, ArrowReader> mapTableToArrowReader);
}
