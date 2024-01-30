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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Class to expose Java Substrait API for end users, currently operations supported are only to Consume Substrait Plan
 * in Plan format (JSON) or Binary format (ByteBuffer).
 */
public final class AceroSubstraitConsumer {
  private final BufferAllocator allocator;

  public AceroSubstraitConsumer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Run Substrait plan.
   *
   * @param plan The JSON Substrait plan.
   * @return the ArrowReader to iterate for record batches.
   */
  public ArrowReader runQuery(String plan) throws Exception {
    return runQuery(plan, Collections.emptyMap());
  }

  /**
   * Run Substrait plan.
   *
   * @param plan The JSON Substrait plan.
   * @param namedTables A mapping of named tables referenced by the plan to an ArrowReader providing the data
   *                    for the table. Contains the Table Name to Query as a Key and ArrowReader as a Value.
   * <pre>{@code ArrowReader nationReader = scanner.scanBatches();
   * Map<String, ArrowReader> namedTables = new HashMap<>();
   * namedTables.put("NATION", nationReader);}</pre>
   * @return the ArrowReader to iterate for record batches.
   */
  public ArrowReader runQuery(String plan, Map<String, ArrowReader> namedTables) throws Exception {
    return execute(plan, namedTables);
  }

  /**
   * Run Substrait plan.
   *
   * @param plan                  the binary Substrait plan.
   * @return the ArrowReader to iterate for record batches.
   */
  public ArrowReader runQuery(ByteBuffer plan) throws Exception {
    return runQuery(plan, Collections.emptyMap());
  }

  /**
   * Read binary Substrait plan, execute and return an ArrowReader to read Schema and ArrowRecordBatches.
   *
   * @param plan                  the binary Substrait plan.
   * @param namedTables A mapping of named tables referenced by the plan to an ArrowReader providing the data
   *                              for the table. Contains the Table Name to Query as a Key and ArrowReader as a Value.
   * <pre>{@code ArrowReader nationReader = scanner.scanBatches();
   * Map<String, ArrowReader> namedTables = new HashMap<>();
   * namedTables.put("NATION", nationReader);}</pre>
   * @return the ArrowReader to iterate for record batches.
   */
  public ArrowReader runQuery(ByteBuffer plan, Map<String, ArrowReader> namedTables) throws Exception {
    return execute(plan, namedTables);
  }

  private ArrowReader execute(String plan, Map<String, ArrowReader> namedTables) throws Exception {
    List<ArrowArrayStream> arrowArrayStream = new ArrayList<>();
    try (
        ArrowArrayStream streamOutput = ArrowArrayStream.allocateNew(this.allocator)
    ) {
      String[] mapTableToMemoryAddress = getMapTableToMemoryAddress(namedTables, arrowArrayStream);
      JniWrapper.get().executeSerializedPlan(
          plan,
          mapTableToMemoryAddress,
          streamOutput.memoryAddress()
      );
      return Data.importArrayStream(this.allocator, streamOutput);
    } finally {
      AutoCloseables.close(arrowArrayStream);
    }
  }

  private ArrowReader execute(ByteBuffer plan, Map<String, ArrowReader> namedTables) throws Exception {
    List<ArrowArrayStream> arrowArrayStream = new ArrayList<>();
    try (
        ArrowArrayStream streamOutput = ArrowArrayStream.allocateNew(this.allocator)
    ) {
      String[] mapTableToMemoryAddress = getMapTableToMemoryAddress(namedTables, arrowArrayStream);
      JniWrapper.get().executeSerializedPlan(
          plan,
          mapTableToMemoryAddress,
          streamOutput.memoryAddress()
      );
      return Data.importArrayStream(this.allocator, streamOutput);
    } finally {
      AutoCloseables.close(arrowArrayStream);
    }
  }

  private String[] getMapTableToMemoryAddress(Map<String, ArrowReader> mapTableToArrowReader,
                                              List<ArrowArrayStream> listStreamInput) {
    String[] mapTableToMemoryAddress = new String[mapTableToArrowReader.size() * 2];
    ArrowArrayStream streamInput;
    int pos = 0;
    for (Map.Entry<String, ArrowReader> entries : mapTableToArrowReader.entrySet()) {
      streamInput = ArrowArrayStream.allocateNew(this.allocator);
      listStreamInput.add(streamInput);
      Data.exportArrayStream(this.allocator, entries.getValue(), streamInput);
      mapTableToMemoryAddress[pos] = entries.getKey();
      mapTableToMemoryAddress[pos + 1] = String.valueOf(streamInput.memoryAddress());
      pos += 2;
    }
    return mapTableToMemoryAddress;
  }
}
