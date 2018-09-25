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

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.AllocationManager.BufferLedger;
import org.apache.arrow.memory.util.AssertionUtil;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.UnsafeDirectLittleEndian;
import io.netty.util.internal.OutOfDirectMemoryError;

public abstract class BaseAllocator extends Accountant implements BufferAllocator {

  public static final String DEBUG_ALLOCATOR = "arrow.memory.debug.allocator";
  public static final int DEBUG_LOG_LENGTH = 6;
  public static final boolean DEBUG = AssertionUtil.isAssertionsEnabled() ||
      Boolean.parseBoolean(System.getProperty(DEBUG_ALLOCATOR, "false"));
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseAllocator
      .class);
  // Package exposed for sharing between AllocatorManger and BaseAllocator objects
  final String name;
  final RootAllocator root;
  private final Object DEBUG_LOCK = DEBUG ? new Object() : null;
  private final AllocationListener listener;
  private final BaseAllocator parentAllocator;
  private final ArrowByteBufAllocator thisAsByteBufAllocator;
  private final IdentityHashMap<BaseAllocator, Object> childAllocators;
  private final ArrowBuf empty;
  // members used purely for debugging
  private final IdentityHashMap<BufferLedger, Object> childLedgers;
  private final IdentityHashMap<Reservation, Object> reservations;
  private final HistoricalLog historicalLog;
  private volatile boolean isClosed = false; // the allocator has been closed

  /**
   * Initialize an allocator
   * @param parentAllocator   parent allocator. null if defining a root allocator
   * @param listener          listener callback. Must be non-null -- use
   *                          {@link AllocationListener#NOOP} if no listener desired
   * @param name              name of this allocator
   * @param initReservation   initial reservation. Cannot be modified after construction
   * @param maxAllocation     limit. Allocations past the limit fail. Can be modified after
   *                          construction
   */
  protected BaseAllocator(
          final BaseAllocator parentAllocator,
          final AllocationListener listener,
          final String name,
          final long initReservation,
          final long maxAllocation) throws OutOfMemoryException {
    super(parentAllocator, initReservation, maxAllocation);

    this.listener = listener;

    if (parentAllocator != null) {
      this.root = parentAllocator.root;
      empty = parentAllocator.empty;
    } else if (this instanceof RootAllocator) {
      this.root = (RootAllocator) this;
      empty = createEmpty();
    } else {
      throw new IllegalStateException("An parent allocator must either carry a root or be the " +
          "root.");
    }

    this.parentAllocator = parentAllocator;
    this.name = name;

    this.thisAsByteBufAllocator = new ArrowByteBufAllocator(this);

    if (DEBUG) {
      childAllocators = new IdentityHashMap<>();
      reservations = new IdentityHashMap<>();
      childLedgers = new IdentityHashMap<>();
      historicalLog = new HistoricalLog(DEBUG_LOG_LENGTH, "allocator[%s]", name);
      hist("created by \"%s\", owned = %d", name, this.getAllocatedMemory());
    } else {
      childAllocators = null;
      reservations = null;
      historicalLog = null;
      childLedgers = null;
    }

  }

  private static String createErrorMsg(final BufferAllocator allocator, final int rounded, final int requested) {
    if (rounded != requested) {
      return String.format(
        "Unable to allocate buffer of size %d (rounded from %d) due to memory limit. Current " +
          "allocation: %d", rounded, requested, allocator.getAllocatedMemory());
    } else {
      return String.format(
        "Unable to allocate buffer of size %d due to memory limit. Current " +
           "allocation: %d", rounded, allocator.getAllocatedMemory());
    }
  }

  /**
   * Rounds up the provided value to the nearest power of two.
   *
   * @param val An integer value.
   * @return The closest power of two of that value.
   */
  public static int nextPowerOfTwo(int val) {
    if (val == 0 || val == 1) {
      return val + 1;
    }
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  /**
   * Rounds up the provided value to the nearest power of two.
   *
   * @param val A long value.
   * @return The closest power of two of that value.
   */
  public static long nextPowerOfTwo(long val) {
    if (val == 0 || val == 1) {
      return val + 1;
    }
    long highestBit = Long.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  public static StringBuilder indent(StringBuilder sb, int indent) {
    final char[] indentation = new char[indent * 2];
    Arrays.fill(indentation, ' ');
    sb.append(indentation);
    return sb;
  }

  public static boolean isDebug() {
    return DEBUG;
  }

  @Override
  public void assertOpen() {
    if (AssertionUtil.ASSERT_ENABLED) {
      if (isClosed) {
        throw new IllegalStateException("Attempting operation on allocator when allocator is closed.\n" +
          toVerboseString());
      }
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public ArrowBuf getEmpty() {
    return empty;
  }

  /**
   * For debug/verification purposes only. Allows an AllocationManager to tell the allocator that
   * we have a new ledger
   * associated with this allocator.
   */
  void associateLedger(BufferLedger ledger) {
    assertOpen();
    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        childLedgers.put(ledger, null);
      }
    }
  }

  /**
   * For debug/verification purposes only. Allows an AllocationManager to tell the allocator that
   * we are removing a
   * ledger associated with this allocator
   */
  void dissociateLedger(BufferLedger ledger) {
    assertOpen();
    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        if (!childLedgers.containsKey(ledger)) {
          throw new IllegalStateException("Trying to remove a child ledger that doesn't exist.");
        }
        childLedgers.remove(ledger);
      }
    }
  }

  /**
   * Track when a ChildAllocator of this BaseAllocator is closed. Used for debugging purposes.
   *
   * @param childAllocator The child allocator that has been closed.
   */
  private void childClosed(final BaseAllocator childAllocator) {
    assertOpen();

    if (DEBUG) {
      Preconditions.checkArgument(childAllocator != null, "child allocator can't be null");

      synchronized (DEBUG_LOCK) {
        final Object object = childAllocators.remove(childAllocator);
        if (object == null) {
          childAllocator.historicalLog.logHistory(logger);
          throw new IllegalStateException("Child allocator[" + childAllocator.name +
            "] not found in parent allocator[" + name + "]'s childAllocators");
        }
      }
    }
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize) {
    assertOpen();

    return buffer(initialRequestSize, null);
  }

  private ArrowBuf createEmpty() {
    return new ArrowBuf(new AtomicInteger(), null, AllocationManager.EMPTY, null, null, 0, 0, true);
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize, BufferManager manager) {
    assertOpen();

    Preconditions.checkArgument(initialRequestSize >= 0, "the requested size must be non-negative");

    if (initialRequestSize == 0) {
      return empty;
    }

    // round to next largest power of two if we're within a chunk since that is how our allocator
    // operates
    final int actualRequestSize = initialRequestSize < AllocationManager.CHUNK_SIZE ?
        nextPowerOfTwo(initialRequestSize)
        : initialRequestSize;
    AllocationOutcome outcome = this.allocateBytes(actualRequestSize);
    if (!outcome.isOk()) {
      if (listener.onFailedAllocation(actualRequestSize, outcome)) {
        // Second try, in case the listener can do something about it
        outcome = this.allocateBytes(actualRequestSize);
      }
      if (!outcome.isOk()) {
        throw new OutOfMemoryException(createErrorMsg(this, actualRequestSize, initialRequestSize));
      }
    }

    boolean success = false;
    try {
      ArrowBuf buffer = bufferWithoutReservation(actualRequestSize, manager);
      success = true;
      listener.onAllocation(actualRequestSize);
      return buffer;
    } catch (OutOfMemoryError e) {
      /*
       * OutOfDirectMemoryError is thrown by Netty when we exceed the direct memory limit defined by
       * -XX:MaxDirectMemorySize. OutOfMemoryError with "Direct buffer memory" message is thrown by
       * java.nio.Bits when we exceed the direct memory limit. This should never be hit in practice
       * as Netty is expected to throw an OutOfDirectMemoryError first.
       */
      if (e instanceof OutOfDirectMemoryError || "Direct buffer memory".equals(e.getMessage())) {
        throw new OutOfMemoryException(e);
      }
      throw e;
    } finally {
      if (!success) {
        releaseBytes(actualRequestSize);
      }
    }

  }

  /**
   * Used by usual allocation as well as for allocating a pre-reserved buffer. Skips the typical
   * accounting associated
   * with creating a new buffer.
   */
  private ArrowBuf bufferWithoutReservation(final int size, BufferManager bufferManager) throws
      OutOfMemoryException {
    assertOpen();

    final AllocationManager manager = new AllocationManager(this, size);
    final BufferLedger ledger = manager.associate(this); // +1 ref cnt (required)
    final ArrowBuf buffer = ledger.newArrowBuf(0, size, bufferManager);

    // make sure that our allocation is equal to what we expected.
    Preconditions.checkArgument(buffer.capacity() == size,
        "Allocated capacity %d was not equal to requested capacity %d.", buffer.capacity(), size);

    return buffer;
  }

  @Override
  public ArrowByteBufAllocator getAsByteBufAllocator() {
    return thisAsByteBufAllocator;
  }

  @Override
  public BufferAllocator newChildAllocator(
      final String name,
      final long initReservation,
      final long maxAllocation) {
    return newChildAllocator(name, this.listener, initReservation, maxAllocation);
  }

  @Override
  public BufferAllocator newChildAllocator(
      final String name,
      final AllocationListener listener,
      final long initReservation,
      final long maxAllocation) {
    assertOpen();

    final ChildAllocator childAllocator = new ChildAllocator(listener, this, name, initReservation,
        maxAllocation);

    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        childAllocators.put(childAllocator, childAllocator);
        historicalLog.recordEvent("allocator[%s] created new child allocator[%s]", name,
            childAllocator.name);
      }
    }

    return childAllocator;
  }

  @Override
  public AllocationReservation newReservation() {
    assertOpen();

    return new Reservation();
  }

  @Override
  public synchronized void close() {
    /*
     * Some owners may close more than once because of complex cleanup and shutdown
     * procedures.
     */
    if (isClosed) {
      return;
    }

    isClosed = true;

    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        verifyAllocator();

        // are there outstanding child allocators?
        if (!childAllocators.isEmpty()) {
          for (final BaseAllocator childAllocator : childAllocators.keySet()) {
            if (childAllocator.isClosed) {
              logger.warn(String.format(
                  "Closed child allocator[%s] on parent allocator[%s]'s child list.\n%s",
                  childAllocator.name, name, toString()));
            }
          }

          throw new IllegalStateException(
              String.format("Allocator[%s] closed with outstanding child allocators.\n%s", name,
                  toString()));
        }

        // are there outstanding buffers?
        final int allocatedCount = childLedgers.size();
        if (allocatedCount > 0) {
          throw new IllegalStateException(
              String.format("Allocator[%s] closed with outstanding buffers allocated (%d).\n%s",
                  name, allocatedCount, toString()));
        }

        if (reservations.size() != 0) {
          throw new IllegalStateException(
              String.format("Allocator[%s] closed with outstanding reservations (%d).\n%s", name,
                  reservations.size(),
                  toString()));
        }

      }
    }

    // Is there unaccounted-for outstanding allocation?
    final long allocated = getAllocatedMemory();
    if (allocated > 0) {
      throw new IllegalStateException(
          String.format("Memory was leaked by query. Memory leaked: (%d)\n%s", allocated,
              toString()));
    }

    // we need to release our memory to our parent before we tell it we've closed.
    super.close();

    // Inform our parent allocator that we've closed
    if (parentAllocator != null) {
      parentAllocator.childClosed(this);
    }

    if (DEBUG) {
      historicalLog.recordEvent("closed");
      logger.debug(String.format(
          "closed allocator[%s].",
          name));
    }


  }

  @Override
  public String toString() {
    final Verbosity verbosity = logger.isTraceEnabled() ? Verbosity.LOG_WITH_STACKTRACE
        : Verbosity.BASIC;
    final StringBuilder sb = new StringBuilder();
    print(sb, 0, verbosity);
    return sb.toString();
  }

  /**
   * Provide a verbose string of the current allocator state. Includes the state of all child
   * allocators, along with
   * historical logs of each object and including stacktraces.
   *
   * @return A Verbose string of current allocator state.
   */
  @Override
  public String toVerboseString() {
    final StringBuilder sb = new StringBuilder();
    print(sb, 0, Verbosity.LOG_WITH_STACKTRACE);
    return sb.toString();
  }

  private void hist(String noteFormat, Object... args) {
    historicalLog.recordEvent(noteFormat, args);
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * @throws IllegalStateException when any problems are found
   */
  void verifyAllocator() {
    final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen = new
        IdentityHashMap<>();
    verifyAllocator(buffersSeen);
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   * <p>
   * <p>
   * This overload is used for recursive calls, allowing for checking that ArrowBufs are unique
   * across all allocators
   * that are checked.
   * </p>
   *
   * @param buffersSeen a map of buffers that have already been seen when walking a tree of
   *                    allocators
   * @throws IllegalStateException when any problems are found
   */
  private void verifyAllocator(final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator>
                                   buffersSeen) {
    // The remaining tests can only be performed if we're in debug mode.
    if (!DEBUG) {
      return;
    }

    synchronized (DEBUG_LOCK) {
      final long allocated = getAllocatedMemory();

      // verify my direct descendants
      final Set<BaseAllocator> childSet = childAllocators.keySet();
      for (final BaseAllocator childAllocator : childSet) {
        childAllocator.verifyAllocator(buffersSeen);
      }

      /*
       * Verify my relationships with my descendants.
       *
       * The sum of direct child allocators' owned memory must be <= my allocated memory; my
       * allocated memory also
       * includes ArrowBuf's directly allocated by me.
       */
      long childTotal = 0;
      for (final BaseAllocator childAllocator : childSet) {
        childTotal += Math.max(childAllocator.getAllocatedMemory(), childAllocator.reservation);
      }
      if (childTotal > getAllocatedMemory()) {
        historicalLog.logHistory(logger);
        logger.debug("allocator[" + name + "] child event logs BEGIN");
        for (final BaseAllocator childAllocator : childSet) {
          childAllocator.historicalLog.logHistory(logger);
        }
        logger.debug("allocator[" + name + "] child event logs END");
        throw new IllegalStateException(
            "Child allocators own more memory (" + childTotal + ") than their parent (name = " +
                name + " ) has allocated (" + getAllocatedMemory() + ')');
      }

      // Furthermore, the amount I've allocated should be that plus buffers I've allocated.
      long bufferTotal = 0;

      final Set<BufferLedger> ledgerSet = childLedgers.keySet();
      for (final BufferLedger ledger : ledgerSet) {
        if (!ledger.isOwningLedger()) {
          continue;
        }

        final UnsafeDirectLittleEndian udle = ledger.getUnderlying();
        /*
         * Even when shared, ArrowBufs are rewrapped, so we should never see the same instance
         * twice.
         */
        final BaseAllocator otherOwner = buffersSeen.get(udle);
        if (otherOwner != null) {
          throw new IllegalStateException("This allocator's ArrowBuf already owned by another " +
              "allocator");
        }
        buffersSeen.put(udle, this);

        bufferTotal += udle.capacity();
      }

      // Preallocated space has to be accounted for
      final Set<Reservation> reservationSet = reservations.keySet();
      long reservedTotal = 0;
      for (final Reservation reservation : reservationSet) {
        if (!reservation.isUsed()) {
          reservedTotal += reservation.getSize();
        }
      }

      if (bufferTotal + reservedTotal + childTotal != getAllocatedMemory()) {
        final StringBuilder sb = new StringBuilder();
        sb.append("allocator[");
        sb.append(name);
        sb.append("]\nallocated: ");
        sb.append(Long.toString(allocated));
        sb.append(" allocated - (bufferTotal + reservedTotal + childTotal): ");
        sb.append(Long.toString(allocated - (bufferTotal + reservedTotal + childTotal)));
        sb.append('\n');

        if (bufferTotal != 0) {
          sb.append("buffer total: ");
          sb.append(Long.toString(bufferTotal));
          sb.append('\n');
          dumpBuffers(sb, ledgerSet);
        }

        if (childTotal != 0) {
          sb.append("child total: ");
          sb.append(Long.toString(childTotal));
          sb.append('\n');

          for (final BaseAllocator childAllocator : childSet) {
            sb.append("child allocator[");
            sb.append(childAllocator.name);
            sb.append("] owned ");
            sb.append(Long.toString(childAllocator.getAllocatedMemory()));
            sb.append('\n');
          }
        }

        if (reservedTotal != 0) {
          sb.append(String.format("reserved total : %d bytes.", reservedTotal));
          for (final Reservation reservation : reservationSet) {
            reservation.historicalLog.buildHistory(sb, 0, true);
            sb.append('\n');
          }
        }

        logger.debug(sb.toString());

        final long allocated2 = getAllocatedMemory();

        if (allocated2 != allocated) {
          throw new IllegalStateException(String.format(
              "allocator[%s]: allocated t1 (%d) + allocated t2 (%d). Someone released memory while in verification.",
              name, allocated, allocated2));

        }
        throw new IllegalStateException(String.format(
            "allocator[%s]: buffer space (%d) + prealloc space (%d) + child space (%d) != allocated (%d)",
            name, bufferTotal, reservedTotal, childTotal, allocated));
      }
    }
  }

  void print(StringBuilder sb, int level, Verbosity verbosity) {

    indent(sb, level)
        .append("Allocator(")
        .append(name)
        .append(") ")
        .append(reservation)
        .append('/')
        .append(getAllocatedMemory())
        .append('/')
        .append(getPeakMemoryAllocation())
        .append('/')
        .append(getLimit())
        .append(" (res/actual/peak/limit)")
        .append('\n');

    if (DEBUG) {
      indent(sb, level + 1).append(String.format("child allocators: %d\n", childAllocators.size()));
      for (BaseAllocator child : childAllocators.keySet()) {
        child.print(sb, level + 2, verbosity);
      }

      indent(sb, level + 1).append(String.format("ledgers: %d\n", childLedgers.size()));
      for (BufferLedger ledger : childLedgers.keySet()) {
        ledger.print(sb, level + 2, verbosity);
      }

      final Set<Reservation> reservations = this.reservations.keySet();
      indent(sb, level + 1).append(String.format("reservations: %d\n", reservations.size()));
      for (final Reservation reservation : reservations) {
        if (verbosity.includeHistoricalLog) {
          reservation.historicalLog.buildHistory(sb, level + 3, true);
        }
      }

    }

  }

  private void dumpBuffers(final StringBuilder sb, final Set<BufferLedger> ledgerSet) {
    for (final BufferLedger ledger : ledgerSet) {
      if (!ledger.isOwningLedger()) {
        continue;
      }
      final UnsafeDirectLittleEndian udle = ledger.getUnderlying();
      sb.append("UnsafeDirectLittleEndian[identityHashCode == ");
      sb.append(Integer.toString(System.identityHashCode(udle)));
      sb.append("] size ");
      sb.append(Integer.toString(udle.capacity()));
      sb.append('\n');
    }
  }

  public static enum Verbosity {
    BASIC(false, false), // only include basic information
    LOG(true, false), // include basic
    LOG_WITH_STACKTRACE(true, true) //
    ;

    public final boolean includeHistoricalLog;
    public final boolean includeStackTraces;

    Verbosity(boolean includeHistoricalLog, boolean includeStackTraces) {
      this.includeHistoricalLog = includeHistoricalLog;
      this.includeStackTraces = includeStackTraces;
    }
  }

  public class Reservation implements AllocationReservation {

    private final HistoricalLog historicalLog;
    private int nBytes = 0;
    private boolean used = false;
    private boolean closed = false;

    public Reservation() {
      if (DEBUG) {
        historicalLog = new HistoricalLog("Reservation[allocator[%s], %d]", name, System
            .identityHashCode(this));
        historicalLog.recordEvent("created");
        synchronized (DEBUG_LOCK) {
          reservations.put(this, this);
        }
      } else {
        historicalLog = null;
      }
    }

    @Override
    public boolean add(final int nBytes) {
      assertOpen();

      Preconditions.checkArgument(nBytes >= 0, "nBytes(%d) < 0", nBytes);
      Preconditions.checkState(!closed, "Attempt to increase reservation after reservation has been closed");
      Preconditions.checkState(!used, "Attempt to increase reservation after reservation has been used");

      // we round up to next power of two since all reservations are done in powers of two. This
      // may overestimate the
      // preallocation since someone may perceive additions to be power of two. If this becomes a
      // problem, we can look
      // at
      // modifying this behavior so that we maintain what we reserve and what the user asked for
      // and make sure to only
      // round to power of two as necessary.
      final int nBytesTwo = BaseAllocator.nextPowerOfTwo(nBytes);
      if (!reserve(nBytesTwo)) {
        return false;
      }

      this.nBytes += nBytesTwo;
      return true;
    }

    @Override
    public ArrowBuf allocateBuffer() {
      assertOpen();

      Preconditions.checkState(!closed, "Attempt to allocate after closed");
      Preconditions.checkState(!used, "Attempt to allocate more than once");

      final ArrowBuf arrowBuf = allocate(nBytes);
      used = true;
      return arrowBuf;
    }

    @Override
    public int getSize() {
      return nBytes;
    }

    @Override
    public boolean isUsed() {
      return used;
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public void close() {
      assertOpen();

      if (closed) {
        return;
      }

      if (DEBUG) {
        if (!isClosed()) {
          final Object object;
          synchronized (DEBUG_LOCK) {
            object = reservations.remove(this);
          }
          if (object == null) {
            final StringBuilder sb = new StringBuilder();
            print(sb, 0, Verbosity.LOG_WITH_STACKTRACE);
            logger.debug(sb.toString());
            throw new IllegalStateException(
                String.format("Didn't find closing reservation[%d]", System.identityHashCode
                    (this)));
          }

          historicalLog.recordEvent("closed");
        }
      }

      if (!used) {
        releaseReservation(nBytes);
      }

      closed = true;
    }

    @Override
    public boolean reserve(int nBytes) {
      assertOpen();

      final AllocationOutcome outcome = BaseAllocator.this.allocateBytes(nBytes);

      if (DEBUG) {
        historicalLog.recordEvent("reserve(%d) => %s", nBytes, Boolean.toString(outcome.isOk()));
      }

      return outcome.isOk();
    }

    /**
     * Allocate a buffer of the requested size.
     * <p>
     * <p>
     * The implementation of the allocator's inner class provides this.
     *
     * @param nBytes the size of the buffer requested
     * @return the buffer, or null, if the request cannot be satisfied
     */
    private ArrowBuf allocate(int nBytes) {
      assertOpen();

      boolean success = false;

      /*
       * The reservation already added the requested bytes to the allocators owned and allocated
       * bytes via reserve().
       * This ensures that they can't go away. But when we ask for the buffer here, that will add
        * to the allocated bytes
       * as well, so we need to return the same number back to avoid double-counting them.
       */
      try {
        final ArrowBuf arrowBuf = BaseAllocator.this.bufferWithoutReservation(nBytes, null);

        listener.onAllocation(nBytes);
        if (DEBUG) {
          historicalLog.recordEvent("allocate() => %s", String.format("ArrowBuf[%d]", arrowBuf
              .getId()));
        }
        success = true;
        return arrowBuf;
      } finally {
        if (!success) {
          releaseBytes(nBytes);
        }
      }
    }

    /**
     * Return the reservation back to the allocator without having used it.
     *
     * @param nBytes the size of the reservation
     */
    private void releaseReservation(int nBytes) {
      assertOpen();

      releaseBytes(nBytes);

      if (DEBUG) {
        historicalLog.recordEvent("releaseReservation(%d)", nBytes);
      }
    }

  }
}
