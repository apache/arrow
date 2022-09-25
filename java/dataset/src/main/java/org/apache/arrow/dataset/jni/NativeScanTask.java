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

package org.apache.arrow.dataset.jni;

import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Native implementation of {@link ScanTask}. Currently RecordBatches are iterated directly by the scanner
 * id via {@link JniWrapper}, thus we allow only one-time execution of method {@link #execute()}. If a re-scan
 * operation is expected, call {@link NativeDataset#newScan} to create a new scanner instance.
 */
@Deprecated
public class NativeScanTask implements ScanTask {
  private final NativeScanner scanner;

  /**
   * Constructor.
   */
  public NativeScanTask(NativeScanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public ArrowReader execute() {
    return scanner.execute();
  }

  @Override
  public void close() {
    scanner.close();
  }
}
