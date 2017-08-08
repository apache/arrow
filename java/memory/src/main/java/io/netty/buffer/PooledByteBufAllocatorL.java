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

package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.SystemPropertyUtil;

import org.apache.arrow.memory.OutOfMemoryException;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.arrow.memory.util.AssertionUtil.ASSERT_ENABLED;

/**
 * The base allocator that we use for all of Arrow's memory management. Returns
 * UnsafeDirectLittleEndian buffers.
 */
public class PooledByteBufAllocatorL {

  private static final org.slf4j.Logger memoryLogger = org.slf4j.LoggerFactory.getLogger("arrow" +
      ".allocator");

  private static final int DEFAULT_NUM_HEAP_ARENA;
  private static final int DEFAULT_NUM_DIRECT_ARENA;

  private static final int DEFAULT_PAGE_SIZE;
  private static final int DEFAULT_MAX_ORDER; // 8192 << 11 = 16 MiB per chunk
  private static final int DEFAULT_TINY_CACHE_SIZE;
  private static final int DEFAULT_SMALL_CACHE_SIZE;
  private static final int DEFAULT_NORMAL_CACHE_SIZE;
  private static final int DEFAULT_MAX_CACHED_BUFFER_CAPACITY;
  private static final int DEFAULT_CACHE_TRIM_INTERVAL;
  private static final boolean DEFAULT_USE_CACHE_FOR_ALL_THREADS;
  private static final int DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT = 64;

  private static final int MIN_PAGE_SIZE = 4096;
  private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

  static {
      int defaultPageSize = SystemPropertyUtil.getInt("io.netty.allocator.pageSize", 8192);
      Throwable pageSizeFallbackCause = null;
      try {
          validateAndCalculatePageShifts(defaultPageSize);
      } catch (Throwable t) {
          pageSizeFallbackCause = t;
          defaultPageSize = 8192;
      }
      DEFAULT_PAGE_SIZE = defaultPageSize;

      int defaultMaxOrder = SystemPropertyUtil.getInt("io.netty.allocator.maxOrder", 11);
      Throwable maxOrderFallbackCause = null;
      try {
          validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
      } catch (Throwable t) {
          maxOrderFallbackCause = t;
          defaultMaxOrder = 11;
      }
      DEFAULT_MAX_ORDER = defaultMaxOrder;

      // Determine reasonable default for nHeapArena and nDirectArena.
      // Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
      final Runtime runtime = Runtime.getRuntime();

      // Use 2 * cores by default to reduce condition as we use 2 * cores for the number of EventLoops
      // in NIO and EPOLL as well. If we choose a smaller number we will run into hotspots as allocation and
      // deallocation needs to be synchronized on the PoolArena.
      // See https://github.com/netty/netty/issues/3888
      final int defaultMinNumArena = runtime.availableProcessors() * 2;
      final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
      DEFAULT_NUM_HEAP_ARENA = Math.max(0,
              SystemPropertyUtil.getInt(
                      "io.netty.allocator.numHeapArenas",
                      (int) Math.min(
                              defaultMinNumArena,
                              runtime.maxMemory() / defaultChunkSize / 2 / 3)));
      DEFAULT_NUM_DIRECT_ARENA = Math.max(0,
              SystemPropertyUtil.getInt(
                      "io.netty.allocator.numDirectArenas",
                      (int) Math.min(
                              defaultMinNumArena,
                              PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)));

      // cache sizes
      DEFAULT_TINY_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.tinyCacheSize", 512);
      DEFAULT_SMALL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.smallCacheSize", 256);
      DEFAULT_NORMAL_CACHE_SIZE = SystemPropertyUtil.getInt("io.netty.allocator.normalCacheSize", 64);

      // 32 kb is the default maximum capacity of the cached buffer. Similar to what is explained in
      // 'Scalable memory allocation using jemalloc'
      DEFAULT_MAX_CACHED_BUFFER_CAPACITY = SystemPropertyUtil.getInt(
              "io.netty.allocator.maxCachedBufferCapacity", 32 * 1024);

      // the number of threshold of allocations when cached entries will be freed up if not frequently used
      DEFAULT_CACHE_TRIM_INTERVAL = SystemPropertyUtil.getInt(
              "io.netty.allocator.cacheTrimInterval", 8192);

      DEFAULT_USE_CACHE_FOR_ALL_THREADS = SystemPropertyUtil.getBoolean(
              "io.netty.allocator.useCacheForAllThreads", true);


      if (memoryLogger.isDebugEnabled()) {
          memoryLogger.debug("-Dio.netty.allocator.numHeapArenas: {}", DEFAULT_NUM_HEAP_ARENA);
          memoryLogger.debug("-Dio.netty.allocator.numDirectArenas: {}", DEFAULT_NUM_DIRECT_ARENA);
          if (pageSizeFallbackCause == null) {
              memoryLogger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE);
          } else {
              memoryLogger.debug("-Dio.netty.allocator.pageSize: {}", DEFAULT_PAGE_SIZE, pageSizeFallbackCause);
          }
          if (maxOrderFallbackCause == null) {
              memoryLogger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER);
          } else {
              memoryLogger.debug("-Dio.netty.allocator.maxOrder: {}", DEFAULT_MAX_ORDER, maxOrderFallbackCause);
          }
          memoryLogger.debug("-Dio.netty.allocator.chunkSize: {}", DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER);
          memoryLogger.debug("-Dio.netty.allocator.tinyCacheSize: {}", DEFAULT_TINY_CACHE_SIZE);
          memoryLogger.debug("-Dio.netty.allocator.smallCacheSize: {}", DEFAULT_SMALL_CACHE_SIZE);
          memoryLogger.debug("-Dio.netty.allocator.normalCacheSize: {}", DEFAULT_NORMAL_CACHE_SIZE);
          memoryLogger.debug("-Dio.netty.allocator.maxCachedBufferCapacity: {}", DEFAULT_MAX_CACHED_BUFFER_CAPACITY);
          memoryLogger.debug("-Dio.netty.allocator.cacheTrimInterval: {}", DEFAULT_CACHE_TRIM_INTERVAL);
          memoryLogger.debug("-Dio.netty.allocator.useCacheForAllThreads: {}", DEFAULT_USE_CACHE_FOR_ALL_THREADS);
      }
  }

  private static final int MEMORY_LOGGER_FREQUENCY_SECONDS = 60;

  public final UnsafeDirectLittleEndian empty;
  private final AtomicLong hugeBufferSize = new AtomicLong(0);
  private final AtomicLong hugeBufferCount = new AtomicLong(0);
  private final AtomicLong normalBufferSize = new AtomicLong(0);
  private final AtomicLong normalBufferCount = new AtomicLong(0);
  private final InnerAllocator allocator;

  public PooledByteBufAllocatorL() {
    allocator = new InnerAllocator();
    empty = new UnsafeDirectLittleEndian(new DuplicatedByteBuf(Unpooled.EMPTY_BUFFER));
  }

  public UnsafeDirectLittleEndian allocate(int size) {
    try {
      return allocator.directBuffer(size, Integer.MAX_VALUE);
    } catch (OutOfMemoryError e) {
      throw new OutOfMemoryException("Failure allocating buffer.", e);
    }

  }

  public int getChunkSize() {
    return allocator.chunkSize;
  }

  public long getHugeBufferSize() {
    return hugeBufferSize.get();
  }

  public long getHugeBufferCount() {
    return hugeBufferCount.get();
  }

  public long getNormalBufferSize() {
    return normalBufferSize.get();
  }

  public long getNormalBufferCount() {
    return normalBufferSize.get();
  }

  private static int validateAndCalculatePageShifts(int pageSize) {
    if (pageSize < MIN_PAGE_SIZE) {
      throw new IllegalArgumentException(
          "pageSize: " + pageSize + " (expected: " + MIN_PAGE_SIZE + ")");
    }

    if ((pageSize & pageSize - 1) != 0) {
      throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2)");
    }

    // Logarithm base 2. At this point we know that pageSize is a power of two.
    return Integer.SIZE - 1 - Integer.numberOfLeadingZeros(pageSize);
  }

  private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
    if (maxOrder > 14) {
      throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
    }

    // Ensure the resulting chunkSize does not overflow.
    int chunkSize = pageSize;
    for (int i = maxOrder; i > 0; i--) {
      if (chunkSize > MAX_CHUNK_SIZE / 2) {
        throw new IllegalArgumentException(
            String.format("pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder,
                MAX_CHUNK_SIZE));
      }
      chunkSize <<= 1;
    }
    return chunkSize;
  }

  private static class AccountedUnsafeDirectLittleEndian extends UnsafeDirectLittleEndian {

    private final long initialCapacity;
    private final AtomicLong count;
    private final AtomicLong size;

    private AccountedUnsafeDirectLittleEndian(LargeBuffer buf, AtomicLong count, AtomicLong size) {
      super(buf);
      this.initialCapacity = buf.capacity();
      this.count = count;
      this.size = size;
    }

    private AccountedUnsafeDirectLittleEndian(PooledUnsafeDirectByteBuf buf, AtomicLong count,
                                              AtomicLong size) {
      super(buf);
      this.initialCapacity = buf.capacity();
      this.count = count;
      this.size = size;
    }

    @Override
    public ByteBuf copy() {
      throw new UnsupportedOperationException("copy method is not supported");
    }

    @Override
    public ByteBuf copy(int index, int length) {
      throw new UnsupportedOperationException("copy method is not supported");
    }

    @Override
    public boolean release(int decrement) {
      boolean released = super.release(decrement);
      if (released) {
        count.decrementAndGet();
        size.addAndGet(-initialCapacity);
      }
      return released;
    }

  }

  private class InnerAllocator extends PooledByteBufAllocator {

    private final PoolArena<ByteBuffer>[] directArenas;
    private final MemoryStatusThread statusThread;
    private final int chunkSize;

    public InnerAllocator() {
      super(true, DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER, DEFAULT_TINY_CACHE_SIZE, DEFAULT_SMALL_CACHE_SIZE, DEFAULT_NORMAL_CACHE_SIZE, DEFAULT_USE_CACHE_FOR_ALL_THREADS, DEFAULT_DIRECT_MEMORY_CACHE_ALIGNMENT);

      try {
        Field f = PooledByteBufAllocator.class.getDeclaredField("directArenas");
        f.setAccessible(true);
        this.directArenas = (PoolArena<ByteBuffer>[]) f.get(this);
      } catch (Exception e) {
        throw new RuntimeException("Failure while initializing allocator.  Unable to retrieve " +
            "direct arenas field.", e);
      }

      this.chunkSize = directArenas[0].chunkSize;

      if (memoryLogger.isTraceEnabled()) {
        statusThread = new MemoryStatusThread();
        statusThread.start();
      } else {
        statusThread = null;
      }
    }

    private UnsafeDirectLittleEndian newDirectBufferL(int initialCapacity, int maxCapacity) {
      PoolThreadCache cache = threadCache();
      PoolArena<ByteBuffer> directArena = cache.directArena;

      if (directArena != null) {

        if (initialCapacity > directArena.chunkSize) {
          // This is beyond chunk size so we'll allocate separately.
          ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity, maxCapacity);

          hugeBufferSize.addAndGet(buf.capacity());
          hugeBufferCount.incrementAndGet();

          // logger.debug("Allocating huge buffer of size {}", initialCapacity, new Exception());
          return new AccountedUnsafeDirectLittleEndian(new LargeBuffer(buf), hugeBufferCount,
              hugeBufferSize);
        } else {
          // within chunk, use arena.
          ByteBuf buf = directArena.allocate(cache, initialCapacity, maxCapacity);
          if (!(buf instanceof PooledUnsafeDirectByteBuf)) {
            fail();
          }

          if (!ASSERT_ENABLED) {
            return new UnsafeDirectLittleEndian((PooledUnsafeDirectByteBuf) buf);
          }

          normalBufferSize.addAndGet(buf.capacity());
          normalBufferCount.incrementAndGet();

          return new AccountedUnsafeDirectLittleEndian((PooledUnsafeDirectByteBuf) buf,
              normalBufferCount, normalBufferSize);
        }

      } else {
        throw fail();
      }
    }

    private UnsupportedOperationException fail() {
      return new UnsupportedOperationException(
          "Arrow requires that the JVM used supports access sun.misc.Unsafe.  This platform " +
              "didn't provide that functionality.");
    }

    @Override
    public UnsafeDirectLittleEndian directBuffer(int initialCapacity, int maxCapacity) {
      if (initialCapacity == 0 && maxCapacity == 0) {
        newDirectBuffer(initialCapacity, maxCapacity);
      }
      validate(initialCapacity, maxCapacity);
      return newDirectBufferL(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
      throw new UnsupportedOperationException("Arrow doesn't support using heap buffers.");
    }


    private void validate(int initialCapacity, int maxCapacity) {
      if (initialCapacity < 0) {
        throw new IllegalArgumentException("initialCapacity: " + initialCapacity + " (expectd: " +
            "0+)");
      }
      if (initialCapacity > maxCapacity) {
        throw new IllegalArgumentException(String.format(
            "initialCapacity: %d (expected: not greater than maxCapacity(%d)",
            initialCapacity, maxCapacity));
      }
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append(directArenas.length);
      buf.append(" direct arena(s):");
      buf.append(StringUtil.NEWLINE);
      for (PoolArena<ByteBuffer> a : directArenas) {
        buf.append(a);
      }

      buf.append("Large buffers outstanding: ");
      buf.append(hugeBufferCount.get());
      buf.append(" totaling ");
      buf.append(hugeBufferSize.get());
      buf.append(" bytes.");
      buf.append('\n');
      buf.append("Normal buffers outstanding: ");
      buf.append(normalBufferCount.get());
      buf.append(" totaling ");
      buf.append(normalBufferSize.get());
      buf.append(" bytes.");
      return buf.toString();
    }

    private class MemoryStatusThread extends Thread {

      public MemoryStatusThread() {
        super("allocation.logger");
        this.setDaemon(true);
      }

      @Override
      public void run() {
        while (true) {
          memoryLogger.trace("Memory Usage: \n{}", PooledByteBufAllocatorL.this.toString());
          try {
            Thread.sleep(MEMORY_LOGGER_FREQUENCY_SECONDS * 1000);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }


  }
}
