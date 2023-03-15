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

import org.apache.arrow.dataset.jni.JniLoader;

/**
 * JniWrapper to Consume Substrait Plans.
 */
public class JniWrapper {
  private static final JniWrapper INSTANCE = new JniWrapper();

  private JniWrapper() {
  }

  public static JniWrapper get() {
    JniLoader.get().ensureLoaded();
    return INSTANCE;
  }

  /**
   * Consume the JSON Substrait Plan that contains Local Files and export the RecordBatchReader into
   * C-Data Interface ArrowArrayStream.
   *
   * @param planInput the JSON Substrait plan.
   * @param memoryAddressOutput the memory address where RecordBatchReader is exported.
   */
  public native void executeSerializedPlanLocalFiles(String planInput, long memoryAddressOutput);

  /**
   * Consume the JSON Substrait Plan that contains Named Tables and export the RecordBatchReader into
   * C-Data Interface ArrowArrayStream.
   *
   * @param planInput the JSON Substrait plan.
   * @param mapTableToMemoryAddressInput the mapping name of Tables Name and theirs memory addres representation.
   * @param memoryAddressOutput the memory address where RecordBatchReader is exported.
   */
  public native void executeSerializedPlanNamedTables(String planInput, String[] mapTableToMemoryAddressInput,
                                                      long memoryAddressOutput);

  /**
   * Consume the binary Substrait Plan that contains Named Tables and export the RecordBatchReader into
   * C-Data Interface ArrowArrayStream.
   *
   * @param planInput the binary Substrait plan.
   * @param mapTableToMemoryAddressInput the mapping name of Tables Name and theirs memory addres representation.
   * @param memoryAddressOutput the memory address where RecordBatchReader is exported.
   */
  public native void executeSerializedPlanNamedTables(Object planInput, String[] mapTableToMemoryAddressInput,
                                                      long memoryAddressOutput);
}
