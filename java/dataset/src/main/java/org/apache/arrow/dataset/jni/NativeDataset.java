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

import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.source.Dataset;

/**
 * Native implementation of {@link Dataset}.
 */
public class NativeDataset implements Dataset {

  private final NativeContext context;
  private final long datasetId;

  private boolean closed = false;

  public NativeDataset(NativeContext context, long datasetId) {
    this.context = context;
    this.datasetId = datasetId;
  }

  @Override
  public synchronized NativeScanner newScan(ScanOptions options) {
    if (closed) {
      throw new NativeInstanceReleasedException();
    }
    long scannerId = JniWrapper.get().createScanner(datasetId, options.getColumns().orElse(null),
        options.getBatchSize(), context.getMemoryPool().getNativeInstanceId());
    return new NativeScanner(context, scannerId);
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    JniWrapper.get().closeDataset(datasetId);
  }
}
