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

package org.apache.arrow.flight;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;

/**
 * An interface for writing data to a peer, client or server.
 */
public interface OutboundStreamListener {

  /**
   * A hint indicating whether the client is ready to receive data without excessive buffering.
   *
   * <p>Writers should poll this flag before sending data to respect backpressure from the client and
   * avoid sending data faster than the client can handle. Ignoring this flag may mean that the server
   * will start consuming excessive amounts of memory, as it may buffer messages in memory.
   */
  boolean isReady();

  /**
   * Set a callback for when the listener is ready for new calls to putNext(), i.e. {@link #isReady()}
   * has become true.
   *
   * <p>Note that this callback may only be called some time after {@link #isReady()} becomes true, and may never
   * be called if all executor threads on the server are busy, or the RPC method body is implemented in a blocking
   * fashion. Note that isReady() must still be checked after the callback is run as it may have been run
   * spuriously.
   */
  default void setOnReadyHandler(Runnable handler) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Start sending data, using the schema of the given {@link VectorSchemaRoot}.
   *
   * <p>This method must be called before all others, except {@link #putMetadata(ArrowBuf)}.
   */
  default void start(VectorSchemaRoot root) {
    start(root, null, IpcOption.DEFAULT);
  }

  /**
   * Start sending data, using the schema of the given {@link VectorSchemaRoot}.
   *
   * <p>This method must be called before all others, except {@link #putMetadata(ArrowBuf)}.
   */
  default void start(VectorSchemaRoot root, DictionaryProvider dictionaries) {
    start(root, dictionaries, IpcOption.DEFAULT);
  }

  /**
   * Start sending data, using the schema of the given {@link VectorSchemaRoot}.
   *
   * <p>This method must be called before all others, except {@link #putMetadata(ArrowBuf)}.
   */
  void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option);

  /**
   * Send the current contents of the associated {@link VectorSchemaRoot}.
   *
   * <p>This will not necessarily block until the message is actually sent; it may buffer messages
   * in memory. Use {@link #isReady()} to check if there is backpressure and avoid excessive buffering.
   */
  void putNext();

  /**
   * Send the current contents of the associated {@link VectorSchemaRoot} alongside application-defined metadata.
   * @param metadata The metadata to send. Ownership of the buffer is transferred to the Flight implementation.
   */
  void putNext(ArrowBuf metadata);

  /**
   * Send a pure metadata message without any associated data.
   *
   * <p>This may be called without starting the stream.
   */
  void putMetadata(ArrowBuf metadata);

  /**
   * Indicate an error to the client. Terminates the stream; do not call {@link #completed()} afterwards.
   */
  void error(Throwable ex);

  /**
   * Indicate that transmission is finished.
   */
  void completed();

  /**
   * Toggle whether to use the zero-copy write optimization.
   *
   * <p>By default or when disabled, Arrow may copy data into a buffer for the underlying implementation to
   * send. When enabled, Arrow will instead try to directly enqueue the Arrow buffer for sending. Not all
   * implementations support this optimization, so even if enabled, you may not see a difference.
   *
   * <p>In this mode, buffers must not be reused after they are written with {@link #putNext()}. For example,
   * you would have to call {@link VectorSchemaRoot#allocateNew()} after every call to {@link #putNext()}.
   * Hence, this is not enabled by default.
   *
   * <p>The default value can be toggled globally by setting the JVM property arrow.flight.enable_zero_copy_write
   * or the environment variable ARROW_FLIGHT_ENABLE_ZERO_COPY_WRITE.
   */
  default void setUseZeroCopy(boolean enabled) {
  }
}
