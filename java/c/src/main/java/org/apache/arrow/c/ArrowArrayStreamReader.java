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

package org.apache.arrow.c;

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.util.Preconditions.checkState;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An implementation of an {@link ArrowReader} backed by an ArrowArrayStream.
 */
final class ArrowArrayStreamReader extends ArrowReader {
  private final ArrowArrayStream ownedStream;
  private final CDataDictionaryProvider provider;

  ArrowArrayStreamReader(BufferAllocator allocator, ArrowArrayStream stream) {
    super(allocator);
    this.provider = new CDataDictionaryProvider();

    ArrowArrayStream.Snapshot snapshot = stream.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowArrayStream");

    // Move imported stream
    this.ownedStream = ArrowArrayStream.allocateNew(allocator);
    this.ownedStream.save(snapshot);
    stream.markReleased();
    stream.close();
  }

  @Override
  public Map<Long, Dictionary> getDictionaryVectors() {
    return provider.getDictionaryIds().stream().collect(Collectors.toMap(Function.identity(), provider::lookup));
  }

  @Override
  public Dictionary lookup(long id) {
    return provider.lookup(id);
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    try (ArrowArray array = ArrowArray.allocateNew(allocator)) {
      ownedStream.getNext(array);
      if (array.snapshot().release == NULL) {
        return false;
      }
      Data.importIntoVectorSchemaRoot(allocator, array, getVectorSchemaRoot(), provider);
      return true;
    }
  }

  @Override
  public long bytesRead() {
    return 0;
  }

  @Override
  protected void closeReadSource() {
    ownedStream.release();
    ownedStream.close();
    provider.close();
  }

  @Override
  protected Schema readSchema() throws IOException {
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      ownedStream.getSchema(schema);
      return Data.importSchema(allocator, schema, provider);
    }
  }
}
