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

package org.apache.arrow.flight.grpc;

import java.io.InputStream;
import java.lang.reflect.Field;

import com.google.common.base.Throwables;

import io.grpc.internal.ReadableBuffer;

/**
 * Enable access to ReadableBuffer directly to copy data from an BufferInputStream into a target
 * ByteBuffer/ByteBuf.
 *
 * <p>This could be solved by BufferInputStream exposing Drainable.
 */
public class GetReadableBuffer {

  private static final Field READABLE_BUFFER;
  private static final Class<?> BUFFER_INPUT_STREAM;

  static {
    Field tmpField = null;
    Class<?> tmpClazz = null;
    try {
      Class<?> clazz = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream");

      Field f = clazz.getDeclaredField("buffer");
      f.setAccessible(true);
      // don't set until we've gotten past all exception cases.
      tmpField = f;
      tmpClazz = clazz;
    } catch (Exception e) {
      e.printStackTrace();
    }
    READABLE_BUFFER = tmpField;
    BUFFER_INPUT_STREAM = tmpClazz;
  }

  /**
   * Extracts the ReadableBuffer for the given input stream.
   *
   * @param is Must be an instance of io.grpc.internal.ReadableBuffers$BufferInputStream or
   *     null will be returned.
   */
  public static ReadableBuffer getReadableBuffer(InputStream is) {

    if (BUFFER_INPUT_STREAM == null || !is.getClass().equals(BUFFER_INPUT_STREAM)) {
      return null;
    }

    try {
      return (ReadableBuffer) READABLE_BUFFER.get(is);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

}
