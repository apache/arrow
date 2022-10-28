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

import java.io.IOException;
import java.util.Iterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An implementation of {@link ArrowReader} that reads
 * the dataset from {@link Scanner}.
 */
public class ArrowScannerReader extends ArrowReader {
  private final Scanner scanner;

  private Iterator<? extends ScanTask> taskIterator;

  private ScanTask currentTask = null;
  private ArrowReader currentReader = null;

  /**
   * Constructs a scanner reader using a Scanner.
   *
   * @param scanner scanning data over dataset
   * @param allocator to allocate new buffers
   */
  public ArrowScannerReader(Scanner scanner, BufferAllocator allocator) {
    super(allocator);
    this.scanner = scanner;
    this.taskIterator = scanner.scan().iterator();
    if (taskIterator.hasNext()) {
      currentTask = taskIterator.next();
      currentReader = currentTask.execute();
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
    if (currentReader == null) {
      return false;
    }
    boolean result = currentReader.loadNextBatch();

    if (!result) {
      try {
        currentTask.close();
        currentReader.close();
      } catch (Exception e) {
        throw new IOException(e);
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

    VectorLoader loader = new VectorLoader(this.getVectorSchemaRoot());
    VectorUnloader unloader =
        new VectorUnloader(currentReader.getVectorSchemaRoot());
    try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
      loader.load(recordBatch);
    }
    return true;
  }

  @Override
  public long bytesRead() {
    return 0L;
  }

  @Override
  protected void closeReadSource() throws IOException {
    try {
      currentTask.close();
      currentReader.close();
      scanner.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected Schema readSchema() throws IOException {
    return scanner.schema();
  }
}
