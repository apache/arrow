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

import org.apache.arrow.util.Preconditions;

/**
 * An AllocationManager is the implementation of a physical memory allocation.
 *
 * <p>Manages the relationship between the allocators and a particular memory allocation. Ensures that
 * one allocator owns the memory that multiple allocators may be referencing. Manages a BufferLedger between
 * each of its associated allocators. It does not track the reference count; that is the role of {@link BufferLedger}
 * (aka {@link ReferenceManager}).
 *
 * <p>This is a public interface implemented by concrete allocator implementations (e.g. Netty or Unsafe).
 *
 * <p>Threading: AllocationManager manages thread-safety internally. Operations within the context
 * of a single BufferLedger are lockless in nature and can be leveraged by multiple threads. Operations that cross the
 * context of two ledgers will acquire a lock on the AllocationManager instance. Important note, there is one
 * AllocationManager per physical buffer allocation. As such, there will be thousands of these in a
 * typical query. The contention of acquiring a lock on AllocationManager should be very low.
 */
public abstract class AllocationManager {
  // The RootAllocator we are associated with. An allocation can only ever be associated with a single RootAllocator.
  private final BufferAllocator root;
  // An allocation can be tracked by multiple allocators. (This is because an allocator is more like a ledger.)
  // All such allocators track reference counts individually, via BufferLedger instances. When an individual
  // reference count reaches zero, the allocator will be dissociated from this allocation. If that was via the
  // owningLedger, then no more allocators should be tracking this allocation, and the allocation will be freed.
  // ARROW-1627: Trying to minimize memory overhead caused by previously used IdentityHashMap
  private final LowCostIdentityHashMap<BufferAllocator, BufferLedger> map = new LowCostIdentityHashMap<>();
  // The primary BufferLedger (i.e. reference count) tracking this allocation.
  // This is mostly a semantic constraint on the API user: if the reference count reaches 0 in the owningLedger, then
  // there are not supposed to be any references through other allocators. In practice, this doesn't do anything
  // as the implementation just forces ownership to be transferred to one of the other extant references.
  private volatile BufferLedger owningLedger;

  protected AllocationManager(BufferAllocator accountingAllocator) {
    Preconditions.checkNotNull(accountingAllocator);
    accountingAllocator.assertOpen();

    this.root = accountingAllocator.getRoot();

    // we do a no retain association since our creator will want to retrieve the newly created
    // ledger and will create a reference count at that point
    this.owningLedger = associate(accountingAllocator, false);
  }

  BufferLedger getOwningLedger() {
    return owningLedger;
  }

  void setOwningLedger(final BufferLedger ledger) {
    this.owningLedger = ledger;
  }

  /**
   * Associate the existing underlying buffer with a new allocator. This will increase the
   * reference count on the corresponding buffer ledger by 1.
   *
   * @param allocator The target allocator to associate this buffer with.
   * @return The reference manager (new or existing) that associates the underlying
   *         buffer to this new ledger.
   */
  BufferLedger associate(final BufferAllocator allocator) {
    return associate(allocator, true);
  }

  private BufferLedger associate(final BufferAllocator allocator, final boolean retain) {
    allocator.assertOpen();
    Preconditions.checkState(root == allocator.getRoot(),
          "A buffer can only be associated between two allocators that share the same root");

    synchronized (this) {
      BufferLedger ledger = map.get(allocator);
      if (ledger != null) {
        // We were already being tracked by the given allocator, just return it
        if (retain) {
          // bump the ref count for the ledger
          ledger.increment();
        }
        return ledger;
      }

      // We weren't previously being tracked by the given allocator; create a new ledger
      ledger = new BufferLedger(allocator, this);

      if (retain) {
        // the new reference manager will have a ref count of 1
        ledger.increment();
      }

      // store the mapping for <allocator, reference manager>
      BufferLedger oldLedger = map.put(ledger);
      Preconditions.checkState(oldLedger == null,
          "Detected inconsistent state: A reference manager already exists for this allocator");

      if (allocator instanceof BaseAllocator) {
        // needed for debugging only: keep a pointer to reference manager inside allocator
        // to dump state, verify allocator state etc
        ((BaseAllocator) allocator).associateLedger(ledger);
      }
      return ledger;
    }
  }

  /**
   * The way that a particular ReferenceManager (BufferLedger) communicates back to the
   * AllocationManager that it no longer needs to hold a reference to a particular
   * piece of memory. Reference manager needs to hold a lock to invoke this method
   * It is called when the shared refcount of all the ArrowBufs managed by the
   * calling ReferenceManager drops to 0.
   */
  void release(final BufferLedger ledger) {
    final BufferAllocator allocator = ledger.getAllocator();
    allocator.assertOpen();

    // remove the <BaseAllocator, BufferLedger> mapping for the allocator
    // of calling BufferLedger
    Preconditions.checkState(map.containsKey(allocator),
        "Expecting a mapping for allocator and reference manager");
    final BufferLedger oldLedger = map.remove(allocator);

    BufferAllocator oldAllocator = oldLedger.getAllocator();
    if (oldAllocator instanceof BaseAllocator) {
      // needed for debug only: tell the allocator that AllocationManager is removing a
      // reference manager associated with this particular allocator
      ((BaseAllocator) oldAllocator).dissociateLedger(oldLedger);
    }

    if (oldLedger == owningLedger) {
      // the release call was made by the owning reference manager
      if (map.isEmpty()) {
        // the only <allocator, reference manager> mapping was for the owner
        // which now has been removed, it implies we can safely destroy the
        // underlying memory chunk as it is no longer being referenced
        oldAllocator.releaseBytes(getSize());
        // free the memory chunk associated with the allocation manager
        release0();
        oldAllocator.getListener().onRelease(getSize());
        owningLedger = null;
      } else {
        // since the refcount dropped to 0 for the owning reference manager and allocation
        // manager will no longer keep a mapping for it, we need to change the owning
        // reference manager to whatever the next available <allocator, reference manager>
        // mapping exists.
        BufferLedger newOwningLedger = map.getNextValue();
        // we'll forcefully transfer the ownership and not worry about whether we
        // exceeded the limit since this consumer can't do anything with this.
        oldLedger.transferBalance(newOwningLedger);
      }
    } else {
      // the release call was made by a non-owning reference manager, so after remove there have
      // to be 1 or more <allocator, reference manager> mappings
      Preconditions.checkState(map.size() > 0,
          "The final removal of reference manager should be connected to owning reference manager");
    }
  }

  /**
   * Return the size of underlying chunk of memory managed by this Allocation Manager.
   *
   * <p>The underlying memory chunk managed can be different from the original requested size.
   *
   * @return size of underlying memory chunk
   */
  public abstract long getSize();

  /**
   * Return the absolute memory address pointing to the fist byte of underlying memory chunk.
   */
  protected abstract long memoryAddress();

  /**
   * Release the underlying memory chunk.
   */
  protected abstract void release0();

  /**
   * A factory interface for creating {@link AllocationManager}.
   * One may extend this interface to use a user-defined AllocationManager implementation.
   */
  public interface Factory {

    /**
     * Create an {@link AllocationManager}.
     *
     * @param accountingAllocator The allocator that are expected to be associated with newly created AllocationManager.
     *                            Currently it is always equivalent to "this"
     * @param size Size (in bytes) of memory managed by the AllocationManager
     * @return The created AllocationManager used by this allocator
     */
    AllocationManager create(BufferAllocator accountingAllocator, long size);

    ArrowBuf empty();
  }
}
