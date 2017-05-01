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

package org.apache.arrow.memory;

import io.netty.buffer.ArrowBuf;

/**
 * Supports cumulative allocation reservation. Clients may increase the size of the reservation
 * repeatedly until they
 * call for an allocation of the current total size. The reservation can only be used once, and
 * will throw an exception
 * if it is used more than once.
 * <p>
 * For the purposes of airtight memory accounting, the reservation must be close()d whether it is
 * used or not.
 * This is not threadsafe.
 * </p>
 */
public interface AllocationReservation extends AutoCloseable {

  /**
   * Add to the current reservation.
   *
   * <p>Adding may fail if the allocator is not allowed to consume any more space.</p>
   *
   * @param nBytes the number of bytes to add
   * @return true if the addition is possible, false otherwise
   * @throws IllegalStateException if called after buffer() is used to allocate the reservation
   */
  boolean add(final int nBytes);

  /**
   * Requests a reservation of additional space.
   *
   * <p>The implementation of the allocator's inner class provides this.</p>
   *
   * @param nBytes the amount to reserve
   * @return true if the reservation can be satisfied, false otherwise
   */
  boolean reserve(int nBytes);

  /**
   * Allocate a buffer whose size is the total of all the add()s made.
   *
   * <p>The allocation request can still fail, even if the amount of space
   * requested is available, if the allocation cannot be made contiguously.</p>
   *
   * @return the buffer, or null, if the request cannot be satisfied
   * @throws IllegalStateException if called called more than once
   */
  ArrowBuf allocateBuffer();

  /**
   * Get the current size of the reservation (the sum of all the add()s).
   *
   * @return size of the current reservation
   */
  int getSize();

  /**
   * Return whether or not the reservation has been used.
   *
   * @return whether or not the reservation has been used
   */
  public boolean isUsed();

  /**
   * Return whether or not the reservation has been closed.
   *
   * @return whether or not the reservation has been closed
   */
  public boolean isClosed();

  public void close();
}
