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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.memory.util.AssertionUtil;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

/**
 * A base-class that implements all functionality of {@linkplain BufferAllocator}s.
 *
 * <p>The class is abstract to enforce usage of {@linkplain RootAllocator}/{@linkplain ChildAllocator}
 * facades.
 */
abstract class BaseAllocator extends Accountant implements BufferAllocator {

  public static final String DEBUG_ALLOCATOR = "arrow.memory.debug.allocator";
  public static final int DEBUG_LOG_LENGTH = 6;
  public static final boolean DEBUG;
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseAllocator.class);

  // Initialize this before DEFAULT_CONFIG as DEFAULT_CONFIG will eventually initialize the allocation manager,
  // which in turn allocates an ArrowBuf, which requires DEBUG to have been properly initialized
  static {
    // the system property takes precedence.
    String propValue = System.getProperty(DEBUG_ALLOCATOR);
    if (propValue != null) {
      DEBUG = Boolean.parseBoolean(propValue);
    } else {
      DEBUG = false;
    }
    logger.info("Debug mode " + (DEBUG ? "enabled."
        : "disabled. Enable with the VM option -Darrow.memory.debug.allocator=true."));
  }

  public static final Config DEFAULT_CONFIG = ImmutableConfig.builder().build();

  // Package exposed for sharing between AllocatorManger and BaseAllocator objects
  private final String name;
  private final RootAllocator root;
  private final Object DEBUG_LOCK = new Object();
  private final AllocationListener listener;
  private final @Nullable BaseAllocator parentAllocator;
  private final Map<BaseAllocator, Object> childAllocators;
  private final ArrowBuf empty;
  // members used purely for debugging
  private final @Nullable IdentityHashMap<BufferLedger, @Nullable Object> childLedgers;
  private final @Nullable IdentityHashMap<Reservation, Object> reservations;
  private final @Nullable HistoricalLog historicalLog;
  private final RoundingPolicy roundingPolicy;
  private final AllocationManager.@NonNull Factory allocationManagerFactory;

  private volatile boolean isClosed = false; // the allocator has been closed

  /**
   * Initialize an allocator.
   *
   * @param parentAllocator   parent allocator. null if defining a root allocator
   * @param name              name of this allocator
   * @param config            configuration including other options of this allocator
   *
   * @see Config
   */
  @SuppressWarnings({"nullness:method.invocation", "nullness:cast.unsafe"})
  //{"call to hist(,...) not allowed on the given receiver.", "cast cannot be statically verified"}
  protected BaseAllocator(
      final @Nullable BaseAllocator parentAllocator,
      final String name,
      final Config config) throws OutOfMemoryException {
    super(parentAllocator, name, config.getInitReservation(), config.getMaxAllocation());

    this.listener = config.getListener();
    this.allocationManagerFactory = config.getAllocationManagerFactory();

    if (parentAllocator != null) {
      this.root = parentAllocator.root;
      empty = parentAllocator.empty;
    } else if (this instanceof RootAllocator) {
      this.root = (@Initialized RootAllocator) this;
      empty = createEmpty();
    } else {
      throw new IllegalStateException("An parent allocator must either carry a root or be the " +
        "root.");
    }

    this.parentAllocator = parentAllocator;
    this.name = name;

    this.childAllocators = Collections.synchronizedMap(new IdentityHashMap<>());

    if (DEBUG) {
      reservations = new IdentityHashMap<>();
      childLedgers = new IdentityHashMap<>();
      historicalLog = new HistoricalLog(DEBUG_LOG_LENGTH, "allocator[%s]", name);
      hist("created by \"%s\", owned = %d", name, this.getAllocatedMemory());
    } else {
      reservations = null;
      historicalLog = null;
      childLedgers = null;
    }
    this.roundingPolicy = config.getRoundingPolicy();
  }

  @Override
  public AllocationListener getListener() {
    return listener;
  }

  @Override
  public @Nullable BaseAllocator getParentAllocator() {
    return parentAllocator;
  }

  @Override
  public Collection<BufferAllocator> getChildAllocators() {
    synchronized (childAllocators) {
      return new HashSet<>(childAllocators.keySet());
    }
  }

  private static String createErrorMsg(final BufferAllocator allocator, final long rounded, final long requested) {
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
        if (childLedgers != null) {
          childLedgers.put(ledger, null);
        }
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
        Preconditions.checkState(childLedgers != null, "childLedgers must not be null");
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
          if (childAllocator.historicalLog != null) {
            childAllocator.historicalLog.logHistory(logger);
          }
          throw new IllegalStateException("Child allocator[" + childAllocator.name +
            "] not found in parent allocator[" + name + "]'s childAllocators");
        }
      }
    } else {
      childAllocators.remove(childAllocator);
    }
    listener.onChildRemoved(this, childAllocator);
  }

  @Override
  public ArrowBuf wrapForeignAllocation(ForeignAllocation allocation) {
    assertOpen();
    final long size = allocation.getSize();
    listener.onPreAllocation(size);
    AllocationOutcome outcome = this.allocateBytes(size);
    if (!outcome.isOk()) {
      if (listener.onFailedAllocation(size, outcome)) {
        // Second try, in case the listener can do something about it
        outcome = this.allocateBytes(size);
      }
      if (!outcome.isOk()) {
        throw new OutOfMemoryException(createErrorMsg(this, size,
            size), outcome.getDetails());
      }
    }
    try {
      final AllocationManager manager = new ForeignAllocationManager(this, allocation);
      final BufferLedger ledger = manager.associate(this);
      final ArrowBuf buf =
          new ArrowBuf(ledger, /*bufferManager=*/null, size, allocation.memoryAddress());
      buf.writerIndex(size);
      listener.onAllocation(size);
      return buf;
    } catch (Throwable t) {
      try {
        releaseBytes(size);
      } catch (Throwable e) {
        t.addSuppressed(e);
      }
      try {
        allocation.release0();
      } catch (Throwable e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  @Override
  public ArrowBuf buffer(final long initialRequestSize) {
    assertOpen();

    return buffer(initialRequestSize, null);
  }

  @SuppressWarnings("nullness:dereference.of.nullable")//dereference of possibly-null reference allocationManagerFactory
  private ArrowBuf createEmpty() {
    return allocationManagerFactory.empty();
  }

  @Override
  public ArrowBuf buffer(final long initialRequestSize, @Nullable BufferManager manager) {
    assertOpen();

    Preconditions.checkArgument(initialRequestSize >= 0, "the requested size must be non-negative");

    if (initialRequestSize == 0) {
      return getEmpty();
    }

    // round the request size according to the rounding policy
    final long actualRequestSize = roundingPolicy.getRoundedSize(initialRequestSize);

    listener.onPreAllocation(actualRequestSize);

    AllocationOutcome outcome = this.allocateBytes(actualRequestSize);
    if (!outcome.isOk()) {
      if (listener.onFailedAllocation(actualRequestSize, outcome)) {
        // Second try, in case the listener can do something about it
        outcome = this.allocateBytes(actualRequestSize);
      }
      if (!outcome.isOk()) {
        throw new OutOfMemoryException(createErrorMsg(this, actualRequestSize,
            initialRequestSize), outcome.getDetails());
      }
    }

    boolean success = false;
    try {
      ArrowBuf buffer = bufferWithoutReservation(actualRequestSize, manager);
      success = true;
      listener.onAllocation(actualRequestSize);
      return buffer;
    } catch (OutOfMemoryError e) {
      throw e;
    } finally {
      if (!success) {
        releaseBytes(actualRequestSize);
      }
    }
  }

  /**
   * Used by usual allocation as well as for allocating a pre-reserved buffer.
   * Skips the typical accounting associated with creating a new buffer.
   */
  private ArrowBuf bufferWithoutReservation(
      final long size,
      @Nullable BufferManager bufferManager) throws OutOfMemoryException {
    assertOpen();

    final AllocationManager manager = newAllocationManager(size);
    final BufferLedger ledger = manager.associate(this); // +1 ref cnt (required)
    final ArrowBuf buffer = ledger.newArrowBuf(size, bufferManager);

    // make sure that our allocation is equal to what we expected.
    Preconditions.checkArgument(buffer.capacity() == size,
        "Allocated capacity %d was not equal to requested capacity %d.", buffer.capacity(), size);

    return buffer;
  }

  private AllocationManager newAllocationManager(long size) {
    return newAllocationManager(this, size);
  }


  private AllocationManager newAllocationManager(BaseAllocator accountingAllocator, long size) {
    return allocationManagerFactory.create(accountingAllocator, size);
  }

  @Override
  public BufferAllocator getRoot() {
    return root;
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

    final ChildAllocator childAllocator =
        new ChildAllocator(this, name, configBuilder()
            .listener(listener)
            .initReservation(initReservation)
            .maxAllocation(maxAllocation)
            .roundingPolicy(roundingPolicy)
            .allocationManagerFactory(allocationManagerFactory)
            .build());

    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        childAllocators.put(childAllocator, childAllocator);
        if (historicalLog != null) {
          historicalLog.recordEvent("allocator[%s] created new child allocator[%s]", name,
              childAllocator.getName());
        }
      }
    } else {
      childAllocators.put(childAllocator, childAllocator);
    }
    this.listener.onChildAdded(this, childAllocator);

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

    StringBuilder outstandingChildAllocators = new StringBuilder();
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
        final int allocatedCount = childLedgers != null ? childLedgers.size() : 0;
        if (allocatedCount > 0) {
          throw new IllegalStateException(
            String.format("Allocator[%s] closed with outstanding buffers allocated (%d).\n%s",
              name, allocatedCount, toString()));
        }

        if (reservations != null && reservations.size() != 0) {
          throw new IllegalStateException(
            String.format("Allocator[%s] closed with outstanding reservations (%d).\n%s", name,
              reservations.size(),
              toString()));
        }

      }
    } else {
      if (!childAllocators.isEmpty()) {
        outstandingChildAllocators.append("Outstanding child allocators : \n");
        synchronized (childAllocators) {
          for (final BaseAllocator childAllocator : childAllocators.keySet()) {
            outstandingChildAllocators.append(String.format("  %s", childAllocator.toString()));
          }
        }
      }
    }

    // Is there unaccounted-for outstanding allocation?
    final long allocated = getAllocatedMemory();
    if (allocated > 0) {
      if (parent != null && reservation > allocated) {
        parent.releaseBytes(reservation - allocated);
      }
      String msg = String.format("Memory was leaked by query. Memory leaked: (%d)\n%s%s", allocated,
          outstandingChildAllocators.toString(), toString());
      logger.error(msg);
      throw new IllegalStateException(msg);
    }

    // we need to release our memory to our parent before we tell it we've closed.
    super.close();

    // Inform our parent allocator that we've closed
    if (parentAllocator != null) {
      parentAllocator.childClosed(this);
    }

    if (DEBUG) {
      if (historicalLog != null) {
        historicalLog.recordEvent("closed");
      }
      logger.debug(String.format("closed allocator[%s].", name));
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

  /* Remove @SuppressWarnings after fixing https://github.com/apache/arrow/issues/41951 */
  @SuppressWarnings("FormatStringAnnotation")
  private void hist(String noteFormat, Object... args) {
    if (historicalLog != null) {
      historicalLog.recordEvent(noteFormat, args);
    }
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * @throws IllegalStateException when any problems are found
   */
  void verifyAllocator() {
    final IdentityHashMap<AllocationManager, BaseAllocator> seen = new IdentityHashMap<>();
    verifyAllocator(seen);
  }

  /**
   * Verifies the accounting state of the allocator (Only works for DEBUG)
   * This overload is used for recursive calls, allowing for checking
   * that ArrowBufs are unique across all allocators that are checked.
   *
   * @param buffersSeen a map of buffers that have already been seen when walking a tree of
   *                    allocators
   * @throws IllegalStateException when any problems are found
   */
  private void verifyAllocator(
      final IdentityHashMap<AllocationManager, BaseAllocator> buffersSeen) {
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
        if (historicalLog != null) {
          historicalLog.logHistory(logger);
        }
        logger.debug("allocator[" + name + "] child event logs BEGIN");
        for (final BaseAllocator childAllocator : childSet) {
          if (childAllocator.historicalLog != null) {
            childAllocator.historicalLog.logHistory(logger);
          }
        }
        logger.debug("allocator[" + name + "] child event logs END");
        throw new IllegalStateException(
          "Child allocators own more memory (" + childTotal + ") than their parent (name = " +
            name + " ) has allocated (" + getAllocatedMemory() + ')');
      }

      // Furthermore, the amount I've allocated should be that plus buffers I've allocated.
      long bufferTotal = 0;

      final Set<@KeyFor("this.childLedgers") BufferLedger> ledgerSet = childLedgers != null ?
              childLedgers.keySet() : null;
      if (ledgerSet != null) {
        for (final BufferLedger ledger : ledgerSet) {
          if (!ledger.isOwningLedger()) {
            continue;
          }

          final AllocationManager am = ledger.getAllocationManager();
          /*
           * Even when shared, ArrowBufs are rewrapped, so we should never see the same instance
           * twice.
           */
          final BaseAllocator otherOwner = buffersSeen.get(am);
          if (otherOwner != null) {
            throw new IllegalStateException("This allocator's ArrowBuf already owned by another " +
              "allocator");
          }
          buffersSeen.put(am, this);

          bufferTotal += am.getSize();
        }
      }

      // Preallocated space has to be accounted for
      final Set<@KeyFor("this.reservations") Reservation> reservationSet = reservations != null ?
              reservations.keySet() : null;
      long reservedTotal = 0;
      if (reservationSet != null) {
        for (final Reservation reservation : reservationSet) {
          if (!reservation.isUsed()) {
            reservedTotal += reservation.getSize();
          }
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
          if (reservationSet != null) {
            for (final Reservation reservation : reservationSet) {
              if (reservation.historicalLog != null) {
                reservation.historicalLog.buildHistory(sb, 0, true);
              }
              sb.append('\n');
            }
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

    CommonUtil.indent(sb, level)
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
      CommonUtil.indent(sb, level + 1).append(String.format("child allocators: %d\n", childAllocators.size()));
      for (BaseAllocator child : childAllocators.keySet()) {
        child.print(sb, level + 2, verbosity);
      }

      CommonUtil.indent(sb, level + 1).append(String.format("ledgers: %d\n", childLedgers != null ?
              childLedgers.size() : 0));
      if (childLedgers != null) {
        for (BufferLedger ledger : childLedgers.keySet()) {
          ledger.print(sb, level + 2, verbosity);
        }
      }

      final Set<@KeyFor("this.reservations") Reservation> reservations = this.reservations != null ?
              this.reservations.keySet() : null;
      CommonUtil.indent(sb, level + 1).append(String.format("reservations: %d\n",
              reservations != null ? reservations.size() : 0));
      if (reservations != null) {
        for (final Reservation reservation : reservations) {
          if (verbosity.includeHistoricalLog) {
            if (reservation.historicalLog != null) {
              reservation.historicalLog.buildHistory(sb, level + 3, true);
            }
          }
        }
      }

    }

  }

  private void dumpBuffers(final StringBuilder sb,
                           final @Nullable Set<@KeyFor("this.childLedgers") BufferLedger> ledgerSet) {
    if (ledgerSet != null) {
      for (final BufferLedger ledger : ledgerSet) {
        if (!ledger.isOwningLedger()) {
          continue;
        }
        final AllocationManager am = ledger.getAllocationManager();
        sb.append("UnsafeDirectLittleEndian[identityHashCode == ");
        sb.append(Integer.toString(System.identityHashCode(am)));
        sb.append("] size ");
        sb.append(Long.toString(am.getSize()));
        sb.append('\n');
      }
    }
  }

  /**
   * Enum for logging verbosity.
   */
  public enum Verbosity {
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

  /**
   * Returns a default {@link Config} instance.
   *
   * @see ImmutableConfig.Builder
   */
  public static Config defaultConfig() {
    return DEFAULT_CONFIG;

  }

  /**
   * Returns a builder class for configuring BaseAllocator's options.
   */
  public static ImmutableConfig.Builder configBuilder() {
    return ImmutableConfig.builder();
  }

  @Override
  public RoundingPolicy getRoundingPolicy() {
    return roundingPolicy;
  }

  /**
   * Config class of {@link BaseAllocator}.
   */
  @Value.Immutable
  abstract static class Config {
    /**
     * Factory for creating {@link AllocationManager} instances.
     */
    @Value.Default
    AllocationManager.Factory getAllocationManagerFactory() {
      return DefaultAllocationManagerOption.getDefaultAllocationManagerFactory();
    }

    /**
     * Listener callback. Must be non-null.
     */
    @Value.Default
    AllocationListener getListener() {
      return AllocationListener.NOOP;
    }

    /**
     * Initial reservation size (in bytes) for this allocator.
     */
    @Value.Default
    long getInitReservation() {
      return 0;
    }

    /**
     * Max allocation size (in bytes) for this allocator, allocations past this limit fail.
     * Can be modified after construction.
     */
    @Value.Default
    long getMaxAllocation() {
      return Long.MAX_VALUE;
    }

    /**
     * The policy for rounding the buffer size.
     */
    @Value.Default
    RoundingPolicy getRoundingPolicy() {
      return DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY;
    }
  }

  /**
   * Implementation of {@link AllocationReservation} that supports
   * history tracking under {@linkplain #DEBUG} is true.
   */
  public class Reservation implements AllocationReservation {

    private final @Nullable HistoricalLog historicalLog;
    private int nBytes = 0;
    private boolean used = false;
    private boolean closed = false;

    /**
     * Creates a new reservation.
     *
     * <p>If {@linkplain #DEBUG} is true this will capture a historical
     * log of events relevant to this Reservation.
     */
    @SuppressWarnings("nullness:argument")//to handle null assignment on third party dependency: System.identityHashCode
    public Reservation() {
      if (DEBUG) {
        historicalLog = new HistoricalLog("Reservation[allocator[%s], %d]", name, System
          .identityHashCode(this));
        historicalLog.recordEvent("created");
        synchronized (DEBUG_LOCK) {
          if (reservations != null) {
            reservations.put(this, this);
          }
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
      final int nBytesTwo = CommonUtil.nextPowerOfTwo(nBytes);
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
            object = reservations != null ? reservations.remove(this) : null;
          }
          if (object == null) {
            final StringBuilder sb = new StringBuilder();
            print(sb, 0, Verbosity.LOG_WITH_STACKTRACE);
            logger.debug(sb.toString());
            throw new IllegalStateException(String.format("Didn't find closing reservation[%d]",
              System.identityHashCode(this)));
          }

          if (historicalLog != null) {
            historicalLog.recordEvent("closed");
          }
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

      if (historicalLog != null) {
        historicalLog.recordEvent("reserve(%d) => %s", nBytes, Boolean.toString(outcome.isOk()));
      }

      return outcome.isOk();
    }

    /**
     * Allocate a buffer of the requested size.
     *
     * <p>The implementation of the allocator's inner class provides this.
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
        if (historicalLog != null) {
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

      if (historicalLog != null) {
        historicalLog.recordEvent("releaseReservation(%d)", nBytes);
      }
    }

  }
}
