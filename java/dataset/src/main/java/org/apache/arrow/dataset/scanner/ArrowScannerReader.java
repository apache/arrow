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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.Iterator;

public class ArrowScannerReader extends ArrowReader {
  private final Scanner scanner;

  private Iterator<? extends ScanTask> taskIterator;

  private ScanTask currentTask = null;
  private ArrowReader currentReader = null;

  public ArrowScannerReader(Scanner scanner, BufferAllocator allocator) {
    super(allocator);
    this.scanner = scanner;
    this.taskIterator = scanner.scan().iterator();
    if (taskIterator.hasNext()) {
      currentTask = taskIterator.next();
      currentReader = currentTask.execute();
    } else {
      currentReader = null;
    }
  }

  @Override
  protected void loadRecordBatch(ArrowRecordBatch batch) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void loadDictionary(ArrowDictionaryBatch dictionaryBatch) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (currentReader == null) return false;
    Boolean result = currentReader.loadNextBatch();

    if (!result) {
      try {
        currentTask.close();
        currentReader.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      while (!result) {
        if (!taskIterator.hasNext()) {
          return false;
        } else {
          currentTask = taskIterator.next();
          currentReader = currentTask.execute();
          result = currentReader.loadNextBatch();
        }
      }
    }

    // Load the currentReader#VectorSchemaRoot to ArrowArray
    VectorSchemaRoot vsr = currentReader.getVectorSchemaRoot();
    ArrowArray array = ArrowArray.allocateNew(allocator);
    Data.exportVectorSchemaRoot(allocator, vsr, currentReader, array);

    // Load the ArrowArray into ArrowScannerReader#VectorSchemaRoot
    CDataDictionaryProvider provider = new CDataDictionaryProvider();
    Data.importIntoVectorSchemaRoot(allocator,
        array, this.getVectorSchemaRoot(), provider);
    array.close();
    provider.close();
    return true;
  }

  @Override
  public long bytesRead() {
    return 0L;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close(true);
      currentTask.close();
      currentReader.close();
      scanner.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void closeReadSource() throws IOException {
    // no-op
  }

  @Override
  protected Schema readSchema() throws IOException {
    return scanner.schema();
  }
}
