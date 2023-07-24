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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.c.jni.PrivateData;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Utility to export an {@link ArrowReader} as an ArrowArrayStream.
 */
final class ArrayStreamExporter {
  private final BufferAllocator allocator;

  ArrayStreamExporter(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Java-side state for the exported stream.
   */
  static class ExportedArrayStreamPrivateData implements PrivateData {
    final BufferAllocator allocator;
    final ArrowReader reader;
    // Read by the JNI side for get_last_error
    byte[] lastError;

    ExportedArrayStreamPrivateData(BufferAllocator allocator, ArrowReader reader) {
      this.allocator = allocator;
      this.reader = reader;
    }

    private int setLastError(Throwable err) {
      // Do not let exceptions propagate up to JNI
      try {
        StringWriter buf = new StringWriter();
        PrintWriter writer = new PrintWriter(buf);
        err.printStackTrace(writer);
        lastError = buf.toString().getBytes(StandardCharsets.UTF_8);
      } catch (Throwable e) {
        // Bail out of setting the error message - we'll still return an error code
        lastError = null;
      }
      return 5; // = EIO
    }

    @SuppressWarnings("unused") // Used by JNI
    int getNext(long arrayAddress) {
      try (ArrowArray out = ArrowArray.wrap(arrayAddress)) {
        if (reader.loadNextBatch()) {
          Data.exportVectorSchemaRoot(allocator, reader.getVectorSchemaRoot(), reader, out);
        } else {
          out.markReleased();
        }
        return 0;
      } catch (Throwable e) {
        return setLastError(e);
      }
    }

    @SuppressWarnings("unused") // Used by JNI
    int getSchema(long schemaAddress) {
      try (ArrowSchema out = ArrowSchema.wrap(schemaAddress)) {
        final Schema schema = reader.getVectorSchemaRoot().getSchema();
        Data.exportSchema(allocator, schema, reader, out);
        return 0;
      } catch (Throwable e) {
        return setLastError(e);
      }
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        // XXX: C Data Interface gives us no way to signal errors to the caller,
        // but the JNI side will catch this and log an error.
        throw new RuntimeException(e);
      }
    }
  }

  void export(ArrowArrayStream stream, ArrowReader reader) {
    ExportedArrayStreamPrivateData data = new ExportedArrayStreamPrivateData(allocator, reader);
    try {
      JniWrapper.get().exportArrayStream(stream.memoryAddress(), data);
    } catch (Exception e) {
      data.close();
      throw e;
    }
  }
}
