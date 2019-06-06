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

import static org.apache.arrow.memory.BaseAllocator.indent;

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.UnsafeDirectLittleEndian;

/**
 * The reference manager that binds an {@link AllocationManager} to
 * {@link BufferAllocator} and a set of {@link ArrowBuf}. The set of
 * ArrowBufs managed by this reference manager share a common
 * fate (same reference count).
 */
public class BufferLedger implements ValueWithKeyIncluded<BaseAllocator>, ReferenceManager  {
  private final IdentityHashMap<ArrowBuf, Object> buffers =
          BaseAllocator.DEBUG ? new IdentityHashMap<>() : null;
  private static final AtomicLong LEDGER_ID_GENERATOR = new AtomicLong(0);
  // unique ID assigned to each ledger
  private final long ledgerId = LEDGER_ID_GENERATOR.incrementAndGet();
  private final AtomicInteger bufRefCnt = new AtomicInteger(0); // start at zero so we can
  // manage request for retain
  // correctly
  private final long lCreationTime = System.nanoTime();
  private final BaseAllocator allocator;
  private final AllocationManager allocationManager;
  private final HistoricalLog historicalLog =
      BaseAllocator.DEBUG ? new HistoricalLog(BaseAllocator.DEBUG_LOG_LENGTH,
        "BufferLedger[%d]", 1) : null;
  private volatile long lDestructionTime = 0;

  BufferLedger(final BaseAllocator allocator, final AllocationManager allocationManager) {
    this.allocator = allocator;
    this.allocationManager = allocationManager;
  }

  boolean isOwningLedger() {
    return this == allocationManager.getOwningLedger();
  }

  public BaseAllocator getKey() {
    return allocator;
  }

  /**
   * Get the buffer allocator associated with this reference manager.
   * @return buffer allocator
   */
  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Get this ledger's reference count.
   * @return reference count
   */
  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }

  /**
   * Increment the ledger's reference count for the associated
   * underlying memory chunk. All ArrowBufs managed by this ledger
   * will share the ref count.
   */
  void increment() {
    bufRefCnt.incrementAndGet();
  }

  /**
   * Decrement the ledger's reference count by 1 for the associated underlying
   * memory chunk. If the reference count drops to 0, it implies that
   * no ArrowBufs managed by this reference manager need access to the memory
   * chunk. In that case, the ledger should inform the allocation manager
   * about releasing its ownership for the chunk. Whether or not the memory
   * chunk will be released is something that {@link AllocationManager} will
   * decide since tracks the usage of memory chunk across multiple reference
   * managers and allocators.
   * @return true if the new ref count has dropped to 0, false otherwise
   */
  @Override
  public boolean release() {
    return release(1);
  }

  /**
   * Decrement the ledger's reference count for the associated underlying
   * memory chunk. If the reference count drops to 0, it implies that
   * no ArrowBufs managed by this reference manager need access to the memory
   * chunk. In that case, the ledger should inform the allocation manager
   * about releasing its ownership for the chunk. Whether or not the memory
   * chunk will be released is something that {@link AllocationManager} will
   * decide since tracks the usage of memory chunk across multiple reference
   * managers and allocators.
   * @param decrement amount to decrease the reference count by
   * @return true if the new ref count has dropped to 0, false otherwise
   */
  @Override
  public boolean release(int decrement) {
    Preconditions.checkState(decrement >= 1,
          "ref count decrement should be greater than or equal to 1");
    // decrement the ref count
    final int refCnt = decrement(decrement);
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent("release(%d). original value: %d",
          decrement, refCnt + decrement);
    }
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "RefCnt has gone negative");
    return refCnt == 0;
  }

  /**
   * Decrement the ledger's reference count for the associated underlying
   * memory chunk. If the reference count drops to 0, it implies that
   * no ArrowBufs managed by this reference manager need access to the memory
   * chunk. In that case, the ledger should inform the allocation manager
   * about releasing its ownership for the chunk. Whether or not the memory
   * chunk will be released is something that {@link AllocationManager} will
   * decide since tracks the usage of memory chunk across multiple reference
   * managers and allocators.
   *
   * @param decrement amount to decrease the reference count by
   * @return the new reference count
   */
  private int decrement(int decrement) {
    allocator.assertOpen();
    final int outcome;
    synchronized (allocationManager) {
      outcome = bufRefCnt.addAndGet(-decrement);
      if (outcome == 0) {
        lDestructionTime = System.nanoTime();
        // refcount of this reference manager has dropped to 0
        // inform the allocation manager that this reference manager
        // no longer holds references to underlying memory
        allocationManager.release(this);
      }
    }
    return outcome;
  }

  /**
   * Increment the ledger's reference count for associated
   * underlying memory chunk by 1.
   */
  @Override
  public void retain() {
    retain(1);
  }

  /**
   * Increment the ledger's reference count for associated
   * underlying memory chunk by the given amount.
   *
   * @param increment amount to increase the reference count by
   */
  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%d) argument is not positive", increment);
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent("retain(%d)", increment);
    }
    final int originalReferenceCount = bufRefCnt.getAndAdd(increment);
    Preconditions.checkArgument(originalReferenceCount > 0);
  }

  /**
   * Derive a new ArrowBuf from a given source ArrowBuf. The new derived
   * ArrowBuf will share the same reference count as rest of the ArrowBufs
   * associated with this ledger. This operation is typically used for
   * slicing -- creating new ArrowBufs from a compound ArrowBuf starting at
   * a particular index in the underlying memory and having access to a
   * particular length (in bytes) of data in memory chunk.
   * <p>
   * This method is also used as a helper for transferring ownership and retain to target
   * allocator.
   * </p>
   * @param sourceBuffer source ArrowBuf
   * @param index index (relative to source ArrowBuf) new ArrowBuf should be
   *              derived from
   * @param length length (bytes) of data in underlying memory that derived buffer will
   *               have access to in underlying memory
   * @return derived buffer
   */
  @Override
  public ArrowBuf deriveBuffer(final ArrowBuf sourceBuffer, int index, int length) {
    /*
     * Usage type 1 for deriveBuffer():
     * Used for slicing where index represents a relative index in the source ArrowBuf
     * as the slice start point. This is why we need to add the source buffer offset
     * to compute the start virtual address of derived buffer within the
     * underlying chunk.
     *
     * Usage type 2 for deriveBuffer():
     * Used for retain(target allocator) and transferOwnership(target allocator)
     * where index is 0 since these operations simply create a new ArrowBuf associated
     * with another combination of allocator buffer ledger for the same underlying memory
     */

    // the memory address stored inside ArrowBuf is its starting virtual
    // address in the underlying memory chunk from the point it has
    // access. so it is already accounting for the offset of the source buffer
    // we can simply add the index to get the starting address of new buffer.
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;

    // create new ArrowBuf
    final ArrowBuf derivedBuf = new ArrowBuf(
            this,
            null,
            length, // length (in bytes) in the underlying memory chunk for this new ArrowBuf
            derivedBufferAddress, // starting byte address in the underlying memory for this new ArrowBuf,
            false);

    // logging
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent(
              "ArrowBuf(BufferLedger, BufferAllocator[%s], " +
                      "UnsafeDirectLittleEndian[identityHashCode == " +
                      "%d](%s)) => ledger hc == %d",
              allocator.name, System.identityHashCode(derivedBuf), derivedBuf.toString(),
              System.identityHashCode(this));

      synchronized (buffers) {
        buffers.put(derivedBuf, null);
      }
    }

    return derivedBuf;
  }

  /**
   * Used by an allocator to create a new ArrowBuf. This is provided
   * as a helper method for the allocator when it allocates a new memory chunk
   * using a new instance of allocation manager and creates a new reference manager
   * too.
   *
   * @param length  The length in bytes that this ArrowBuf will provide access to.
   * @param manager An optional BufferManager argument that can be used to manage expansion of
   *                this ArrowBuf
   * @return A new ArrowBuf that shares references with all ArrowBufs associated
   *         with this BufferLedger
   */
  ArrowBuf newArrowBuf(final int length, final BufferManager manager) {
    allocator.assertOpen();

    // the start virtual address of the ArrowBuf will be same as address of memory chunk
    final long startAddress = allocationManager.getMemoryChunk().memoryAddress();

    // create ArrowBuf
    final ArrowBuf buf = new ArrowBuf(this, manager, length, startAddress, false);

    // logging
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent(
          "ArrowBuf(BufferLedger, BufferAllocator[%s], " +
          "UnsafeDirectLittleEndian[identityHashCode == " + "%d](%s)) => ledger hc == %d",
          allocator.name, System.identityHashCode(buf), buf.toString(),
          System.identityHashCode(this));

      synchronized (buffers) {
        buffers.put(buf, null);
      }
    }

    return buf;
  }

  /**
   * Create a new ArrowBuf that is associated with an alternative allocator for the purposes of
   * memory ownership and accounting. This has no impact on the reference counting for the current
   * ArrowBuf except in the situation where the passed in Allocator is the same as the current buffer.
   * <p>
   * This operation has no impact on the reference count of this ArrowBuf. The newly created
   * ArrowBuf with either have a reference count of 1 (in the case that this is the first time this
   * memory is being associated with the target allocator or in other words allocation manager currently
   * doesn't hold a mapping for the target allocator) or the current value of the reference count for
   * the target allocator-reference manager combination + 1 in the case that the provided allocator
   * already had an association to this underlying memory.
   * </p>
   *
   * @param srcBuffer source ArrowBuf
   * @param target The target allocator to create an association with.
   * @return A new ArrowBuf which shares the same underlying memory as the provided ArrowBuf.
   */
  @Override
  public ArrowBuf retain(final ArrowBuf srcBuffer, BufferAllocator target) {
    if (srcBuffer.isEmpty()) {
      return srcBuffer;
    }

    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent("retain(%s)", target.getName());
    }

    // the call to associate will return the corresponding reference manager (buffer ledger) for
    // the target allocator. if the allocation manager didn't already have a mapping
    // for the target allocator, it will create one and return the new reference manager with a
    // reference count of 1. Thus the newly created buffer in this case will have a ref count of 1.
    // alternatively, if there was already a mapping for <buffer allocator, ref manager> in
    // allocation manager, the ref count of the new buffer will be targetrefmanager.refcount() + 1
    // and this will be true for all the existing buffers currently managed by targetrefmanager
    final BufferLedger targetRefManager = allocationManager.associate((BaseAllocator)target);
    // create a new ArrowBuf to associate with new allocator and target ref manager
    final int targetBufLength = srcBuffer.capacity();
    ArrowBuf targetArrowBuf = targetRefManager.deriveBuffer(srcBuffer, 0, targetBufLength);
    targetArrowBuf.readerIndex(srcBuffer.readerIndex());
    targetArrowBuf.writerIndex(srcBuffer.writerIndex());
    return targetArrowBuf;
  }

  /**
   * Transfer any balance the current ledger has to the target ledger. In the case
   * that the current ledger holds no memory, no transfer is made to the new ledger.
   *
   * @param targetReferenceManager The ledger to transfer ownership account to.
   * @return Whether transfer fit within target ledgers limits.
   */
  boolean transferBalance(final ReferenceManager targetReferenceManager) {
    Preconditions.checkArgument(targetReferenceManager != null,
        "Expecting valid target reference manager");
    final BaseAllocator targetAllocator = (BaseAllocator)targetReferenceManager.getAllocator();
    Preconditions.checkArgument(allocator.root == targetAllocator.root,
        "You can only transfer between two allocators that share the same root.");

    allocator.assertOpen();
    targetReferenceManager.getAllocator().assertOpen();

    // if we're transferring to ourself, just return.
    if (targetReferenceManager == this) {
      return true;
    }

    // since two balance transfers out from the allocation manager could cause incorrect
    // accounting, we need to ensure
    // that this won't happen by synchronizing on the allocation manager instance.
    synchronized (allocationManager) {
      if (allocationManager.getOwningLedger() != this) {
        // since the calling reference manager is not the owning
        // reference manager for the underlying memory, transfer is
        // a NO-OP
        return true;
      }

      if (BaseAllocator.DEBUG) {
        this.historicalLog.recordEvent("transferBalance(%s)",
            targetReferenceManager.getAllocator().getName());
      }

      boolean overlimit = targetAllocator.forceAllocate(allocationManager.getSize());
      allocator.releaseBytes(allocationManager.getSize());
      // since the transfer can only happen from the owning reference manager,
      // we need to set the target ref manager as the new owning ref manager
      // for the chunk of memory in allocation manager
      allocationManager.setOwningLedger((BufferLedger)targetReferenceManager);
      return overlimit;
    }
  }

  /**
   * Transfer the memory accounting ownership of this ArrowBuf to another allocator.
   * This will generate a new ArrowBuf that carries an association with the underlying memory
   * of this ArrowBuf. If this ArrowBuf is connected to the owning BufferLedger of this memory,
   * that memory ownership/accounting will be transferred to the target allocator. If this
   * ArrowBuf does not currently own the memory underlying it (and is only associated with it),
   * this does not transfer any ownership to the newly created ArrowBuf.
   * <p>
   * This operation has no impact on the reference count of this ArrowBuf. The newly created
   * ArrowBuf with either have a reference count of 1 (in the case that this is the first time
   * this memory is being associated with the new allocator) or the current value of the reference
   * count for the other AllocationManager/BufferLedger combination + 1 in the case that the provided
   * allocator already had an association to this underlying memory.
   * </p>
   * <p>
   * Transfers will always succeed, even if that puts the other allocator into an overlimit
   * situation. This is possible due to the fact that the original owning allocator may have
   * allocated this memory out of a local reservation whereas the target allocator may need to
   * allocate new memory from a parent or RootAllocator. This operation is done n a mostly-lockless
   * but consistent manner. As such, the overlimit==true situation could occur slightly prematurely
   * to an actual overlimit==true condition. This is simply conservative behavior which means we may
   * return overlimit slightly sooner than is necessary.
   * </p>
   *
   * @param target The allocator to transfer ownership to.
   * @return A new transfer result with the impact of the transfer (whether it was overlimit) as
   *         well as the newly created ArrowBuf.
   */
  @Override
  public TransferResult transferOwnership(final ArrowBuf srcBuffer, final BufferAllocator target) {
    if (srcBuffer.isEmpty()) {
      return new TransferResult(true, srcBuffer);
    }
    // the call to associate will return the corresponding reference manager (buffer ledger) for
    // the target allocator. if the allocation manager didn't already have a mapping
    // for the target allocator, it will create one and return the new reference manager with a
    // reference count of 1. Thus the newly created buffer in this case will have a ref count of 1.
    // alternatively, if there was already a mapping for <buffer allocator, ref manager> in
    // allocation manager, the ref count of the new buffer will be targetrefmanager.refcount() + 1
    // and this will be true for all the existing buffers currently managed by targetrefmanager
    final BufferLedger targetRefManager = allocationManager.associate((BaseAllocator)target);
    // create a new ArrowBuf to associate with new allocator and target ref manager
    final int targetBufLength = srcBuffer.capacity();
    final ArrowBuf targetArrowBuf = targetRefManager.deriveBuffer(srcBuffer, 0, targetBufLength);
    targetArrowBuf.readerIndex(srcBuffer.readerIndex());
    targetArrowBuf.writerIndex(srcBuffer.writerIndex());
    final boolean allocationFit = transferBalance(targetRefManager);
    return new TransferResult(allocationFit, targetArrowBuf);
  }

  /**
   * The outcome of a Transfer.
   */
  public class TransferResult implements OwnershipTransferResult {

    // Whether this transfer fit within the target allocator's capacity.
    final boolean allocationFit;

    // The newly created buffer associated with the target allocator
    public final ArrowBuf buffer;

    private TransferResult(boolean allocationFit, ArrowBuf buffer) {
      this.allocationFit = allocationFit;
      this.buffer = buffer;
    }

    @Override
    public ArrowBuf getTransferredBuffer() {
      return buffer;
    }

    @Override
    public boolean getAllocationFit() {
      return allocationFit;
    }
  }

  /**
   * Total size (in bytes) of memory underlying this reference manager.
   * @return Size (in bytes) of the memory chunk
   */
  @Override
  public int getSize() {
    return allocationManager.getSize();
  }

  /**
   * How much memory is accounted for by this ledger. This is either getSize()
   * if this is the owning ledger for the memory or zero in the case that this
   * is not the owning ledger associated with this memory.
   * @return Amount of accounted(owned) memory associated with this ledger.
   */
  @Override
  public int getAccountedSize() {
    synchronized (allocationManager) {
      if (allocationManager.getOwningLedger() == this) {
        return allocationManager.getSize();
      } else {
        return 0;
      }
    }
  }

  /**
   * Print the current ledger state to the provided StringBuilder.
   *
   * @param sb        The StringBuilder to populate.
   * @param indent    The level of indentation to position the data.
   * @param verbosity The level of verbosity to print.
   */
  void print(StringBuilder sb, int indent, BaseAllocator.Verbosity verbosity) {
    indent(sb, indent)
      .append("ledger[")
      .append(ledgerId)
      .append("] allocator: ")
      .append(allocator.name)
      .append("), isOwning: ")
      .append(", size: ")
      .append(", references: ")
      .append(bufRefCnt.get())
      .append(", life: ")
      .append(lCreationTime)
      .append("..")
      .append(lDestructionTime)
      .append(", allocatorManager: [")
      .append(", life: ");

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

  public UnsafeDirectLittleEndian getUnderlying() {
    return allocationManager.getMemoryChunk();
  }
}
