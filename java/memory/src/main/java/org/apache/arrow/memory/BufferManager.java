/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.apache.arrow.memory;

import io.netty.buffer.ArrowBuf;

/**
 * Manages a list of {@link ArrowBuf}s that can be reallocated as needed. Upon
 * re-allocation the old buffer will be freed. Managing a list of these buffers
 * prevents some parts of the system from needing to define a correct location
 * to place the final call to free them.
 */
public interface BufferManager extends AutoCloseable {

  /**
   * Replace an old buffer with a new version at least of the provided size. Does not copy data.
   *
   * @param old     Old Buffer that the user is no longer going to use.
   * @param newSize Size of new replacement buffer.
   * @return A new version of the buffer.
   */
  public ArrowBuf replace(ArrowBuf old, int newSize);

  /**
   * Get a managed buffer of indeterminate size.
   *
   * @return A buffer.
   */
  public ArrowBuf getManagedBuffer();

  /**
   * Get a managed buffer of at least a certain size.
   *
   * @param size The desired size
   * @return A buffer
   */
  public ArrowBuf getManagedBuffer(int size);

  public void close();
}
