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

package org.apache.arrow.memory;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.arrow.util.Preconditions;

/**
 * Provides a concurrent way to manage account for memory usage without locking. Used as basis
 * for Allocators. All
 * operations are threadsafe (except for close).
 */
@ThreadSafe
class Accountant implements AutoCloseable {

  /**
   * The parent allocator.
   */
  protected final Accountant parent;

  private final String name;

  /**
   * The amount of memory reserved for this allocator. Releases below this amount of memory will
   * not be returned to the
   * parent Accountant until this Accountant is closed.
   */
  protected final long reservation;

  private final AtomicLong peakAllocation = new AtomicLong();

  /**
   * Maximum local memory that can be held. This can be externally updated. Changing it won't
   * cause past memory to
   * change but will change responses to future allocation efforts
   */
  private final AtomicLong allocationLimit = new AtomicLong();

  /**
   * Currently allocated amount of memory.
   */
  private final AtomicLong locallyHeldMemory = new AtomicLong();

  public Accountant(Accountant parent, String name, long reservation, long maxAllocation) {
    Preconditions.checkNotNull(name, "name must not be null");
    Preconditions.checkArgument(reservation >= 0, "The initial reservation size must be " +
        "non-negative.");
    Preconditions.checkArgument(maxAllocation >= 0, "The maximum allocation limit must be " +
        "non-negative.");
    Preconditions.checkArgument(reservation <= maxAllocation,
        "The initial reservation size must be <= the maximum allocation.");
    Preconditions.checkArgument(reservation == 0 || parent != null, "The root accountant can't " +
        "reserve memory.");

    this.parent = parent;
    this.name = name;
    this.reservation = reservation;
    this.allocationLimit.set(maxAllocation);

    if (reservation != 0) {
      // we will allocate a reservation from our parent.
      final AllocationOutcome outcome = parent.allocateBytes(reservation);
      if (!outcome.isOk()) {
        throw new OutOfMemoryException(String.format(
            "Failure trying to allocate initial reservation for Allocator. " +
                "Attempted to allocate %d bytes.", reservation,
            outcome.getStatus().name()), outcome.getDetails());
      }
    }
  }

  /**
   * Attempt to allocate the requested amount of memory. Either completely succeeds or completely
   * fails. If it fails, no changes are made to accounting.
   *
   * @param size The amount of memory to reserve in bytes.
   * @return the status and details of allocation at each allocator in the chain.
   */
  AllocationOutcome allocateBytes(long size) {
    AllocationOutcome.Status status = allocateBytesInternal(size);
    if (status.isOk()) {
      return AllocationOutcome.SUCCESS_INSTANCE;
    } else {
      // Try again, but with details this time.
      // Populating details only on failures avoids performance overhead in the common case (success case).
      AllocationOutcomeDetails details = new AllocationOutcomeDetails();
      status = allocateBytesInternal(size, details);
      return new AllocationOutcome(status, details);
    }
  }

  private AllocationOutcome.Status allocateBytesInternal(long size, AllocationOutcomeDetails details) {
    final AllocationOutcome.Status status = allocate(size,
        true /*incomingUpdatePeek*/, false /*forceAllocation*/, details);
    if (!status.isOk()) {
      releaseBytes(size);
    }
    return status;
  }

  private AllocationOutcome.Status allocateBytesInternal(long size) {
    return allocateBytesInternal(size, null /*details*/);
  }

  private void updatePeak() {
    final long currentMemory = locallyHeldMemory.get();
    while (true) {

      final long previousPeak = peakAllocation.get();
      if (currentMemory > previousPeak) {
        if (!peakAllocation.compareAndSet(previousPeak, currentMemory)) {
          // peak allocation changed underneath us. try again.
          continue;
        }
      }

      // we either succeeded to set peak allocation or we weren't above the previous peak, exit.
      return;
    }
  }


  /**
   * Increase the accounting. Returns whether the allocation fit within limits.
   *
   * @param size to increase
   * @return Whether the allocation fit within limits.
   */
  boolean forceAllocate(long size) {
    final AllocationOutcome.Status outcome = allocate(size, true, true, null);
    return outcome.isOk();
  }

  /**
   * Internal method for allocation. This takes a forced approach to allocation to ensure that we
   * manage reservation
   * boundary issues consistently. Allocation is always done through the entire tree. The two
   * options that we influence
   * are whether the allocation should be forced and whether or not the peak memory allocation
   * should be updated. If at
   * some point during allocation escalation we determine that the allocation is no longer
   * possible, we will continue to
   * do a complete and consistent allocation but we will stop updating the peak allocation. We do
   * this because we know
   * that we will be directly unwinding this allocation (and thus never actually making the
   * allocation). If force
   * allocation is passed, then we continue to update the peak limits since we now know that this
   * allocation will occur
   * despite our moving past one or more limits.
   *
   * @param size               The size of the allocation.
   * @param incomingUpdatePeak Whether we should update the local peak for this allocation.
   * @param forceAllocation    Whether we should force the allocation.
   * @return The outcome of the allocation.
   */
  private AllocationOutcome.Status allocate(final long size, final boolean incomingUpdatePeak,
      final boolean forceAllocation, AllocationOutcomeDetails details) {
    final long newLocal = locallyHeldMemory.addAndGet(size);
    final long beyondReservation = newLocal - reservation;
    final boolean beyondLimit = newLocal > allocationLimit.get();
    final boolean updatePeak = forceAllocation || (incomingUpdatePeak && !beyondLimit);

    if (details != null) {
      // Add details if required (used in exceptions and debugging).
      boolean allocationFailed = true;
      long allocatedLocal = 0;
      if (!beyondLimit) {
        allocatedLocal = size - Math.min(beyondReservation, size);
        allocationFailed = false;
      }
      details.pushEntry(this, newLocal - size, size, allocatedLocal, allocationFailed);
    }

    AllocationOutcome.Status parentOutcome = AllocationOutcome.Status.SUCCESS;
    if (beyondReservation > 0 && parent != null) {
      // we need to get memory from our parent.
      final long parentRequest = Math.min(beyondReservation, size);
      parentOutcome = parent.allocate(parentRequest, updatePeak, forceAllocation, details);
    }

    final AllocationOutcome.Status finalOutcome;
    if (beyondLimit) {
      finalOutcome = AllocationOutcome.Status.FAILED_LOCAL;
    } else {
      finalOutcome = parentOutcome.isOk() ? AllocationOutcome.Status.SUCCESS
          : AllocationOutcome.Status.FAILED_PARENT;
    }

    if (updatePeak) {
      updatePeak();
    }

    return finalOutcome;
  }

  public void releaseBytes(long size) {
    // reduce local memory. all memory released above reservation should be released up the tree.
    final long newSize = locallyHeldMemory.addAndGet(-size);

    Preconditions.checkArgument(newSize >= 0, "Accounted size went negative.");

    final long originalSize = newSize + size;
    if (originalSize > reservation && parent != null) {
      // we deallocated memory that we should release to our parent.
      final long possibleAmountToReleaseToParent = originalSize - reservation;
      final long actualToReleaseToParent = Math.min(size, possibleAmountToReleaseToParent);
      parent.releaseBytes(actualToReleaseToParent);
    }

  }

  public boolean isOverLimit() {
    return getAllocatedMemory() > getLimit() || (parent != null && parent.isOverLimit());
  }

  /**
   * Close this Accountant. This will release any reservation bytes back to a parent Accountant.
   */
  @Override
  public void close() {
    // return memory reservation to parent allocator.
    if (parent != null) {
      parent.releaseBytes(reservation);
    }
  }

  /**
   * Return the name of the accountant.
   *
   * @return name of accountant
   */
  public String getName() {
    return name;
  }

  /**
   * Return the current limit of this Accountant.
   *
   * @return Limit in bytes.
   */
  public long getLimit() {
    return allocationLimit.get();
  }

  /**
   * Return the initial reservation.
   *
   * @return reservation in bytes.
   */
  public long getInitReservation() {
    return reservation;
  }

  /**
   * Set the maximum amount of memory that can be allocated in the this Accountant before failing
   * an allocation.
   *
   * @param newLimit The limit in bytes.
   */
  public void setLimit(long newLimit) {
    allocationLimit.set(newLimit);
  }

  /**
   * Return the current amount of allocated memory that this Accountant is managing accounting
   * for. Note this does not
   * include reservation memory that hasn't been allocated.
   *
   * @return Currently allocate memory in bytes.
   */
  public long getAllocatedMemory() {
    return locallyHeldMemory.get();
  }

  /**
   * The peak memory allocated by this Accountant.
   *
   * @return The peak allocated memory in bytes.
   */
  public long getPeakMemoryAllocation() {
    return peakAllocation.get();
  }

  public long getHeadroom() {
    long localHeadroom = allocationLimit.get() - locallyHeldMemory.get();
    if (parent == null) {
      return localHeadroom;
    }

    // Amount of reserved memory left on top of what parent has
    long reservedHeadroom = Math.max(0, reservation - locallyHeldMemory.get());
    return Math.min(localHeadroom, parent.getHeadroom() + reservedHeadroom);
  }

}
