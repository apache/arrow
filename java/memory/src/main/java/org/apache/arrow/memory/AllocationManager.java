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

import org.apache.arrow.util.Preconditions;

import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

/**
 * Manages the relationship between one or more allocators and a particular UDLE. Ensures that
 * one allocator owns the
 * memory that multiple allocators may be referencing. Manages a BufferLedger between each of its
 * associated allocators.
 * This class is also responsible for managing when memory is allocated and returned to the
 * Netty-based
 * PooledByteBufAllocatorL.
 *
 * <p>The only reason that this isn't package private is we're forced to put ArrowBuf in Netty's
 * package which need access
 * to these objects or methods.
 *
 * <p>Threading: AllocationManager manages thread-safety internally. Operations within the context
 * of a single BufferLedger
 * are lockless in nature and can be leveraged by multiple threads. Operations that cross the
 * context of two ledgers
 * will acquire a lock on the AllocationManager instance. Important note, there is one
 * AllocationManager per
 * UnsafeDirectLittleEndian buffer allocation. As such, there will be thousands of these in a
 * typical query. The
 * contention of acquiring a lock on AllocationManager should be very low.
 */
public class AllocationManager {

  private static final AtomicLong MANAGER_ID_GENERATOR = new AtomicLong(0);
  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL();

  static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;
  static final long CHUNK_SIZE = INNER_ALLOCATOR.getChunkSize();

  private final RootAllocator root;
  private final long allocatorManagerId = MANAGER_ID_GENERATOR.incrementAndGet();
  private final int size;
  private final UnsafeDirectLittleEndian memoryChunk;
  // ARROW-1627 Trying to minimize memory overhead caused by previously used IdentityHashMap
  // see JIRA for details
  private final LowCostIdentityHashMap<BaseAllocator, BufferLedger> map = new LowCostIdentityHashMap<>();
  private final long amCreationTime = System.nanoTime();

  // The ReferenceManager created at the time of creation of this AllocationManager
  // is treated as the owning reference manager for the underlying chunk of memory
  // managed by this allocation manager
  private volatile BufferLedger owningLedger;
  private volatile long amDestructionTime = 0;

  AllocationManager(BaseAllocator accountingAllocator, int size) {
    Preconditions.checkNotNull(accountingAllocator);
    accountingAllocator.assertOpen();

    this.root = accountingAllocator.root;
    this.memoryChunk = INNER_ALLOCATOR.allocate(size);

    // we do a no retain association since our creator will want to retrieve the newly created
    // ledger and will create a reference count at that point
    this.owningLedger = associate(accountingAllocator, false);
    this.size = memoryChunk.capacity();
  }

  BufferLedger getOwningLedger() {
    return owningLedger;
  }

  void setOwningLedger(final BufferLedger ledger) {
    this.owningLedger = ledger;
  }

  /**
   * Get the underlying memory chunk managed by this AllocationManager.
   * @return buffer
   */
  UnsafeDirectLittleEndian getMemoryChunk() {
    return memoryChunk;
  }

  /**
   * Associate the existing underlying buffer with a new allocator. This will increase the
   * reference count on the corresponding buffer ledger by 1
   *
   * @param allocator The target allocator to associate this buffer with.
   * @return The reference manager (new or existing) that associates the underlying
   *         buffer to this new ledger.
   */
  BufferLedger associate(final BaseAllocator allocator) {
    return associate(allocator, true);
  }

  private BufferLedger associate(final BaseAllocator allocator, final boolean retain) {
    allocator.assertOpen();
    Preconditions.checkState(root == allocator.root,
          "A buffer can only be associated between two allocators that share the same root");

    synchronized (this) {
      BufferLedger ledger = map.get(allocator);
      if (ledger != null) {
        if (retain) {
          // bump the ref count for the ledger
          ledger.increment();
        }
        return ledger;
      }

      ledger = new BufferLedger(allocator, this);

      if (retain) {
        // the new reference manager will have a ref count of 1
        ledger.increment();
      }

      // store the mapping for <allocator, reference manager>
      BufferLedger oldLedger = map.put(ledger);
      Preconditions.checkState(oldLedger == null,
          "Detected inconsistent state: A reference manager already exists for this allocator");

      // needed for debugging only: keep a pointer to reference manager inside allocator
      // to dump state, verify allocator state etc
      allocator.associateLedger(ledger);
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
    final BaseAllocator allocator = (BaseAllocator)ledger.getAllocator();
    allocator.assertOpen();

    // remove the <BaseAllocator, BufferLedger> mapping for the allocator
    // of calling BufferLedger
    Preconditions.checkState(map.containsKey(allocator),
        "Expecting a mapping for allocator and reference manager");
    final BufferLedger oldLedger = map.remove(allocator);

    // needed for debug only: tell the allocator that AllocationManager is removing a
    // reference manager associated with this particular allocator
    ((BaseAllocator)oldLedger.getAllocator()).dissociateLedger(oldLedger);

    if (oldLedger == owningLedger) {
      // the release call was made by the owning reference manager
      if (map.isEmpty()) {
        // the only <allocator, reference manager> mapping was for the owner
        // which now has been removed, it implies we can safely destroy the
        // underlying memory chunk as it is no longer being referenced
        ((BaseAllocator)oldLedger.getAllocator()).releaseBytes(size);
        // free the memory chunk associated with the allocation manager
        memoryChunk.release();
        ((BaseAllocator)oldLedger.getAllocator()).getListener().onRelease(size);
        amDestructionTime = System.nanoTime();
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
   * @return size of memory chunk
   */
  public int getSize() {
    return size;
  }
}
