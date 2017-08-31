/*
 */
package io.netty.buffer;

import com.google.common.io.ByteStreams;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ArrowBufTest {

  private final static int MAX_ALLOCATION = 8 * 1024;
  private static RootAllocator allocator;
  
  @BeforeClass
  public static void beforeClass() {
    allocator = new RootAllocator(MAX_ALLOCATION);
  }
  
  @AfterClass
  public static void afterClass() {
    if (allocator != null) {
      allocator.close();
    }
  }
  
  @Test
  public void testSetBytesSliced() {
    int arrLength = 64;
    byte[] expecteds = new byte[arrLength];
    for (int i = 0; i < expecteds.length; i++) {
      expecteds[i] = (byte) i;
    }
    ByteBuffer data = ByteBuffer.wrap(expecteds);
    try (ArrowBuf buf = allocator.buffer(expecteds.length)) {
      buf.setBytes(0, data, 0, data.capacity());
      
      byte[] actuals = new byte[expecteds.length];
      buf.getBytes(0, actuals);
      assertArrayEquals(expecteds, actuals);
    }
  }
  
  @Test
  public void testSetBytesUnsliced() {
    int arrLength = 64;
    byte[] arr = new byte[arrLength];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (byte) i;
    }
    ByteBuffer data = ByteBuffer.wrap(arr);
    
    int from = 10;
    int to = arrLength;
    byte[] expecteds = Arrays.copyOfRange(arr, from, to);
    try (ArrowBuf buf = allocator.buffer(expecteds.length)) {
      buf.setBytes(0, data, from, to - from);
      
      byte[] actuals = new byte[expecteds.length];
      buf.getBytes(0, actuals);
      assertArrayEquals(expecteds, actuals);
    }
  }
  
}
