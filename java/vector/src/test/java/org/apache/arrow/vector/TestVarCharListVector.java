package org.apache.arrow.vector;

import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestVarCharListVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testVarCharListWithNulls() {
    byte[] bytes = "a".getBytes();
    try (ListVector vector = new ListVector("VarList", allocator, FieldType.nullable(Types
            .MinorType.VARCHAR.getType()),null);
         ArrowBuf tempBuf = allocator.buffer(bytes.length)) {
      UnionListWriter writer = vector.getWriter();
      writer.allocate();

      // populate input vector with the following records
      // ["a"]
      // null
      // ["b"]
      writer.setPosition(0); // optional
      writer.startList();
      tempBuf.setBytes(0, bytes);
      writer.writeVarChar(0, bytes.length, tempBuf);
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      bytes = "b".getBytes();
      tempBuf.setBytes(0, bytes);
      writer.writeVarChar(0, bytes.length, tempBuf);
      writer.endList();

      writer.setValueCount(2);

      Assert.assertTrue(vector.getValueCount() == 2);
      Assert.assertTrue(vector.getDataVector().getValueCount() == 2);
    }
  }
}
