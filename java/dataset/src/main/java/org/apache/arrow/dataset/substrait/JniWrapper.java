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

import org.apache.arrow.dataset.jni.JniLoader;

/**
 * Class that contains Native methods to call Acero C++ Substrait API. It internally depends on C++ function
 * arrow::engine::ExecuteSerializedPlan. Currently supported input parameters supported are:
 * <pre>
 * - arrow::Buffer: Susbtrait Plan (JSON or Binary format).
 * - arrow::engine::ConversionOptions: Mapping for arrow::engine::NamedTableProvider.
 * </pre>
 */
final class JniWrapper {
  private static final JniWrapper INSTANCE = new JniWrapper();

  private JniWrapper() {
  }

  public static JniWrapper get() {
    JniLoader.get().ensureLoaded();
    return INSTANCE;
  }

  /**
   * Consume the JSON Substrait Plan that contains Named Tables and export the RecordBatchReader into
   * C-Data Interface ArrowArrayStream.
   *
   * @param planInput the JSON Substrait plan.
   * @param mapTableToMemoryAddressInput the mapping name of Tables Name on position `i` and theirs Memory Address
   *                                     representation on `i+1` position linearly.
   * <pre>{@code String[] mapTableToMemoryAddress = new String[2];
   * mapTableToMemoryAddress[0]="NATION";
   * mapTableToMemoryAddress[1]="140650250895360";}</pre>
   * @param memoryAddressOutput the memory address where RecordBatchReader is exported.
   *
   */
  public native void executeSerializedPlan(String planInput, String[] mapTableToMemoryAddressInput,
                                                      long memoryAddressOutput);

  /**
   * Consume the binary Substrait Plan that contains Named Tables and export the RecordBatchReader into
   * C-Data Interface ArrowArrayStream.
   *
   * @param planInput the binary Substrait plan.
   * @param mapTableToMemoryAddressInput the mapping name of Tables Name on position `i` and theirs Memory Address
   *                                     representation on `i+1` position linearly.
   * <pre>{@code String[] mapTableToMemoryAddress = new String[2];
   * mapTableToMemoryAddress[0]="NATION";
   * mapTableToMemoryAddress[1]="140650250895360";}</pre>
   * @param memoryAddressOutput the memory address where RecordBatchReader is exported.
   */
  public native void executeSerializedPlan(ByteBuffer planInput, String[] mapTableToMemoryAddressInput,
                                                      long memoryAddressOutput);
}
