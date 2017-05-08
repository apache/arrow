/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector;

import java.util.Iterator;

import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.RecordBatch;

import com.google.common.base.Preconditions;

public class BuffersIterator {
  private final RecordBatch recordBatch;
  private final Buffer buffer = new Buffer();
  private int index = -1;

  public BuffersIterator(RecordBatch recordBatch) {
    this.recordBatch = recordBatch;
  }

  public boolean next() {
    return ++index < recordBatch.buffersLength();
  }

  public long offset() {
    return recordBatch.buffers(buffer, index).offset();
  }

  public long length() {
    return recordBatch.buffers(buffer, index).length();
  }
}
