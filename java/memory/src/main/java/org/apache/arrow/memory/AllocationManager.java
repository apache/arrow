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

import static org.apache.arrow.memory.BaseAllocator.indent;

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.memory.BaseAllocator.Verbosity;
import org.apache.arrow.memory.util.AutoCloseableLock;
import org.apache.arrow.memory.util.HistoricalLog;

import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;
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
 * <p>
 * The only reason that this isn't package private is we're forced to put ArrowBuf in Netty's
 * package which need access
 * to these objects or methods.
 * <p>
 * Threading: AllocationManager manages thread-safety internally. Operations within the context
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
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger
  // (AllocationManager.class);

  private static final AtomicLong MANAGER_ID_GENERATOR = new AtomicLong(0);
  private static final AtomicLong LEDGER_ID_GENERATOR = new AtomicLong(0);
  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL();

  static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;
  static final long CHUNK_SIZE = INNER_ALLOCATOR.getChunkSize();

  private final RootAllocator root;
  private final long allocatorManagerId = MANAGER_ID_GENERATOR.incrementAndGet();
  private final int size;
  private final UnsafeDirectLittleEndian underlying;
  private final IdentityHashMap<BufferAllocator, BufferLedger> map = new IdentityHashMap<>();
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final AutoCloseableLock readLock = new AutoCloseableLock(lock.readLock());
  private final AutoCloseableLock writeLock = new AutoCloseableLock(lock.writeLock());
  private final long amCreationTime = System.nanoTime();

  private volatile BufferLedger owningLedger;
  private volatile long amDestructionTime = 0;

  AllocationManager(BaseAllocator accountingAllocator, int size) {
    Preconditions.checkNotNull(accountingAllocator);
    accountingAllocator.assertOpen();

    this.root = accountingAllocator.root;
    this.underlying = INNER_ALLOCATOR.allocate(size);

    // we do a no retain association since our creator will want to retrieve the newly created
    // ledger and will create a
    // reference count at that point
    this.owningLedger = associate(accountingAllocator, false);
    this.size = underlying.capacity();
  }

  /**
   * Associate the existing underlying buffer with a new allocator. This will increase the
   * reference count to the
   * provided ledger by 1.
   *
   * @param allocator The target allocator to associate this buffer with.
   * @return The Ledger (new or existing) that associates the underlying buffer to this new ledger.
   */
  BufferLedger associate(final BaseAllocator allocator) {
    return associate(allocator, true);
  }

  private BufferLedger associate(final BaseAllocator allocator, final boolean retain) {
    allocator.assertOpen();

    if (root != allocator.root) {
      throw new IllegalStateException(
          "A buffer can only be associated between two allocators that share the same root.");
    }

    try (AutoCloseableLock read = readLock.open()) {

      final BufferLedger ledger = map.get(allocator);
      if (ledger != null) {
        if (retain) {
          ledger.inc();
        }
        return ledger;
      }

    }
    try (AutoCloseableLock write = writeLock.open()) {
      // we have to recheck existing ledger since a second reader => writer could be competing
      // with us.

      final BufferLedger existingLedger = map.get(allocator);
      if (existingLedger != null) {
        if (retain) {
          existingLedger.inc();
        }
        return existingLedger;
      }

      final BufferLedger ledger = new BufferLedger(allocator, new ReleaseListener(allocator));
      if (retain) {
        ledger.inc();
      }
      BufferLedger oldLedger = map.put(allocator, ledger);
      Preconditions.checkArgument(oldLedger == null);
      allocator.associateLedger(ledger);
      return ledger;
    }
  }


  /**
   * The way that a particular BufferLedger communicates back to the AllocationManager that it
   * now longer needs to hold
   * a reference to particular piece of memory.
   */
  private class ReleaseListener {

    private final BufferAllocator allocator;

    public ReleaseListener(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    /**
     * Can only be called when you already hold the writeLock.
     */
    public void release() {
      allocator.assertOpen();

      final BufferLedger oldLedger = map.remove(allocator);
      oldLedger.allocator.dissociateLedger(oldLedger);

      if (oldLedger == owningLedger) {
        if (map.isEmpty()) {
          // no one else owns, lets release.
          oldLedger.allocator.releaseBytes(size);
          underlying.release();
          amDestructionTime = System.nanoTime();
          owningLedger = null;
        } else {
          // we need to change the owning allocator. we've been removed so we'll get whatever is
          // top of list
          BufferLedger newLedger = map.values().iterator().next();

          // we'll forcefully transfer the ownership and not worry about whether we exceeded the
          // limit
          // since this consumer can't do anything with this.
          oldLedger.transferBalance(newLedger);
        }
      } else {
        if (map.isEmpty()) {
          throw new IllegalStateException("The final removal of a ledger should be connected to " +
              "the owning ledger.");
        }
      }


    }
  }

  /**
   * The reference manager that binds an allocator manager to a particular BaseAllocator. Also
   * responsible for creating
   * a set of ArrowBufs that share a common fate and set of reference counts.
   * As with AllocationManager, the only reason this is public is due to ArrowBuf being in io
   * .netty.buffer package.
   */
  public class BufferLedger {

    private final IdentityHashMap<ArrowBuf, Object> buffers =
        BaseAllocator.DEBUG ? new IdentityHashMap<ArrowBuf, Object>() : null;

    private final long ledgerId = LEDGER_ID_GENERATOR.incrementAndGet(); // unique ID assigned to
    // each ledger
    private final AtomicInteger bufRefCnt = new AtomicInteger(0); // start at zero so we can
    // manage request for retain
    // correctly
    private final long lCreationTime = System.nanoTime();
    private final BaseAllocator allocator;
    private final ReleaseListener listener;
    private final HistoricalLog historicalLog = BaseAllocator.DEBUG ? new HistoricalLog
        (BaseAllocator.DEBUG_LOG_LENGTH,
            "BufferLedger[%d]", 1)
        : null;
    private volatile long lDestructionTime = 0;

    private BufferLedger(BaseAllocator allocator, ReleaseListener listener) {
      this.allocator = allocator;
      this.listener = listener;
    }

    /**
     * Transfer any balance the current ledger has to the target ledger. In the case that the
     * current ledger holds no
     * memory, no transfer is made to the new ledger.
     *
     * @param target The ledger to transfer ownership account to.
     * @return Whether transfer fit within target ledgers limits.
     */
    public boolean transferBalance(final BufferLedger target) {
      Preconditions.checkNotNull(target);
      Preconditions.checkArgument(allocator.root == target.allocator.root,
          "You can only transfer between two allocators that share the same root.");
      allocator.assertOpen();

      target.allocator.assertOpen();
      // if we're transferring to ourself, just return.
      if (target == this) {
        return true;
      }

      // since two balance transfers out from the allocator manager could cause incorrect
      // accounting, we need to ensure
      // that this won't happen by synchronizing on the allocator manager instance.
      try (AutoCloseableLock write = writeLock.open()) {
        if (owningLedger != this) {
          return true;
        }

        if (BaseAllocator.DEBUG) {
          this.historicalLog.recordEvent("transferBalance(%s)", target.allocator.name);
          target.historicalLog.recordEvent("incoming(from %s)", owningLedger.allocator.name);
        }

        boolean overlimit = target.allocator.forceAllocate(size);
        allocator.releaseBytes(size);
        owningLedger = target;
        return overlimit;
      }

    }

    /**
     * Print the current ledger state to a the provided StringBuilder.
     *
     * @param sb        The StringBuilder to populate.
     * @param indent    The level of indentation to position the data.
     * @param verbosity The level of verbosity to print.
     */
    public void print(StringBuilder sb, int indent, Verbosity verbosity) {
      indent(sb, indent)
          .append("ledger[")
          .append(ledgerId)
          .append("] allocator: ")
          .append(allocator.name)
          .append("), isOwning: ")
          .append(owningLedger == this)
          .append(", size: ")
          .append(size)
          .append(", references: ")
          .append(bufRefCnt.get())
          .append(", life: ")
          .append(lCreationTime)
          .append("..")
          .append(lDestructionTime)
          .append(", allocatorManager: [")
          .append(AllocationManager.this.allocatorManagerId)
          .append(", life: ")
          .append(amCreationTime)
          .append("..")
          .append(amDestructionTime);

      if (!BaseAllocator.DEBUG) {
        sb.append("]\n");
      } else {
        synchronized (buffers) {
          sb.append("] holds ")
              .append(buffers.size())
              .append(" buffers. \n");
          for (ArrowBuf buf : buffers.keySet()) {
            buf.print(sb, indent + 2, verbosity);
            sb.append('\n');
          }
        }
      }

    }

    private void inc() {
      bufRefCnt.incrementAndGet();
    }

    /**
     * Decrement the ledger's reference count. If the ledger is decremented to zero, this ledger
     * should release its
     * ownership back to the AllocationManager
     *
     * @param decrement amout to decrease the reference count by
     * @return the new reference count
     */
    public int decrement(int decrement) {
      allocator.assertOpen();

      final int outcome;
      try (AutoCloseableLock write = writeLock.open()) {
        outcome = bufRefCnt.addAndGet(-decrement);
        if (outcome == 0) {
          lDestructionTime = System.nanoTime();
          listener.release();
        }
      }

      return outcome;
    }

    /**
     * Returns the ledger associated with a particular BufferAllocator. If the BufferAllocator
     * doesn't currently have a
     * ledger associated with this AllocationManager, a new one is created. This is placed on
     * BufferLedger rather than
     * AllocationManager directly because ArrowBufs don't have access to AllocationManager and
     * they are the ones
     * responsible for exposing the ability to associate multiple allocators with a particular
     * piece of underlying
     * memory. Note that this will increment the reference count of this ledger by one to ensure
     * the ledger isn't
     * destroyed before use.
     *
     * @param allocator A BufferAllocator.
     * @return The ledger associated with the BufferAllocator.
     */
    public BufferLedger getLedgerForAllocator(BufferAllocator allocator) {
      return associate((BaseAllocator) allocator);
    }

    /**
     * Create a new ArrowBuf associated with this AllocationManager and memory. Does not impact
     * reference count.
     * Typically used for slicing.
     *
     * @param offset The offset in bytes to start this new ArrowBuf.
     * @param length The length in bytes that this ArrowBuf will provide access to.
     * @return A new ArrowBuf that shares references with all ArrowBufs associated with this
     * BufferLedger
     */
    public ArrowBuf newArrowBuf(int offset, int length) {
      allocator.assertOpen();
      return newArrowBuf(offset, length, null);
    }

    /**
     * Create a new ArrowBuf associated with this AllocationManager and memory.
     *
     * @param offset  The offset in bytes to start this new ArrowBuf.
     * @param length  The length in bytes that this ArrowBuf will provide access to.
     * @param manager An optional BufferManager argument that can be used to manage expansion of
     *                this ArrowBuf
     * @return A new ArrowBuf that shares references with all ArrowBufs associated with this
     * BufferLedger
     */
    public ArrowBuf newArrowBuf(int offset, int length, BufferManager manager) {
      allocator.assertOpen();

      final ArrowBuf buf = new ArrowBuf(
          bufRefCnt,
          this,
          underlying,
          manager,
          allocator.getAsByteBufAllocator(),
          offset,
          length,
          false);

      if (BaseAllocator.DEBUG) {
        historicalLog.recordEvent(
            "ArrowBuf(BufferLedger, BufferAllocator[%s], " +
                "UnsafeDirectLittleEndian[identityHashCode == "
                + "%d](%s)) => ledger hc == %d",
            allocator.name, System.identityHashCode(buf), buf.toString(),
            System.identityHashCode(this));

        synchronized (buffers) {
          buffers.put(buf, null);
        }
      }

      return buf;

    }

    /**
     * What is the total size (in bytes) of memory underlying this ledger.
     *
     * @return Size in bytes
     */
    public int getSize() {
      return size;
    }

    /**
     * How much memory is accounted for by this ledger. This is either getSize() if this is the
     * owning ledger for the
     * memory or zero in the case that this is not the owning ledger associated with this memory.
     *
     * @return Amount of accounted(owned) memory associated with this ledger.
     */
    public int getAccountedSize() {
      try (AutoCloseableLock read = readLock.open()) {
        if (owningLedger == this) {
          return size;
        } else {
          return 0;
        }
      }
    }

    /**
     * Package visible for debugging/verification only.
     */
    UnsafeDirectLittleEndian getUnderlying() {
      return underlying;
    }

    /**
     * Package visible for debugging/verification only.
     */
    boolean isOwningLedger() {
      return this == owningLedger;
    }

  }

}