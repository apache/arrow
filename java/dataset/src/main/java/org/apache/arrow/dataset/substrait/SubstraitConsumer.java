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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Java binding of the C++ ExecuteSerializedPlan.
 */
public class SubstraitConsumer implements Substrait {
  private final BufferAllocator allocator;

  public SubstraitConsumer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public ArrowReader runQueryLocalFiles(String plan) {
    try (ArrowArrayStream arrowArrayStream = ArrowArrayStream.allocateNew(this.allocator)) {
      JniWrapper.get().executeSerializedPlanLocalFiles(plan, arrowArrayStream.memoryAddress());
      return Data.importArrayStream(this.allocator, arrowArrayStream);
    }
  }

  @Override
  public ArrowReader runQueryNamedTables(String plan, Map<String, ArrowReader> mapTableToArrowReader) {
    List<ArrowArrayStream> listStreamInput = new ArrayList<>();
    try (
        ArrowArrayStream streamOutput = ArrowArrayStream.allocateNew(this.allocator)
    ) {
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
      JniWrapper.get().executeSerializedPlanNamedTables(
          plan,
          mapTableToMemoryAddress,
          streamOutput.memoryAddress()
      );
      return Data.importArrayStream(this.allocator, streamOutput);
    } finally {
      for (ArrowArrayStream stream : listStreamInput) {
        stream.close();
      }
    }
  }
}
