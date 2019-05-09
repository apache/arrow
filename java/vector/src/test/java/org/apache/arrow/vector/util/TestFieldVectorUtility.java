package org.apache.arrow.vector.util;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;

public class TestFieldVectorUtility {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBaseFixedWidthVector() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WriteChannel out = new WriteChannel(Channels.newChannel(outputStream));

    final int valueCount = 1000;

    IntVector vector = new IntVector("f0", allocator);
    vector.setValueCount(valueCount);
    for (int i = 0; i < valueCount; i++) {
      vector.set(i, i);
    }
    FieldVectorUtility.serializeFieldVector(out, vector);


    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    ReadChannel in = new ReadChannel(Channels.newChannel(inputStream));

    IntVector deserialized = new IntVector("f0", allocator);
    FieldVectorUtility.deserializeFieldVector(in, deserialized);

    assertEquals(deserialized.getValueCount(), valueCount);
    assertEquals(deserialized.get(10), 10);

    vector.clear();
    deserialized.clear();

  }

  @Test
  public void testBaseVariableWidthVector() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    WriteChannel out = new WriteChannel(Channels.newChannel(outputStream));

    final int valueCount = 1000;

    VarCharVector vector = new VarCharVector("f0", allocator);
    vector.setInitialCapacity(valueCount, 8);
    vector.allocateNew();
    vector.setValueCount(valueCount);
    for (int i = 0; i < valueCount; i++) {
      vector.set(i, new Text("test" + i));
    }
    FieldVectorUtility.serializeFieldVector(out, vector);


    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    ReadChannel in = new ReadChannel(Channels.newChannel(inputStream));

    VarCharVector deserialized = new VarCharVector("f0", allocator);
    FieldVectorUtility.deserializeFieldVector(in, deserialized);

    assertEquals(valueCount, deserialized.getValueCount());
    assertEquals("test10", deserialized.getObject(10).toString());

    vector.clear();
    deserialized.clear();

  }
}
