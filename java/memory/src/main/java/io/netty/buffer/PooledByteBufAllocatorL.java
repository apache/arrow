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

import io.netty.util.internal.StringUtil;

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
      super(true);

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
