/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.file;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.schema.ArrowFieldNode;
import org.apache.arrow.schema.ArrowRecordBatch;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestArrowFile {
  static final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  @Test
  public void test() throws IOException {
    File file = new File("target/mytest.arrow");
    int count = 10000;

    {
      MapVector parent = new MapVector("parent", allocator, null);
      ComplexWriter writer = new ComplexWriterImpl("root", parent);
      MapWriter rootWriter = writer.rootAsMap();
      IntWriter intWriter = rootWriter.integer("int");
      BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
      for (int i = 0; i < count; i++) {
        intWriter.setPosition(i);
        intWriter.writeInt(i);
        bigIntWriter.setPosition(i);
        bigIntWriter.writeBigInt(i);
      }
      writer.setValueCount(count);

      write(parent, file);
      parent.close();
    }

    {
      try (
          FileOutputStream fileOutputStream = new FileOutputStream(file);
          ArrowReader arrowReader = new ArrowReader(fileOutputStream.getChannel(), allocator)
          ) {
        ArrowFooter footer = arrowReader.readFooter();
        org.apache.arrow.vector.types.pojo.Schema schema = footer.getSchema();
        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock);
          Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
          Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
          MapVector parent = new MapVector("parent", allocator, null);
          List<Field> fields = schema.getFields();
          for (Field field : fields) {
            VectorLoader.addChild(parent, field, nodes, buffers);
          }

          MapReader rootReader = new SingleMapReaderImpl(parent).reader("root");
          for (int i = 0; i < count; i++) {
            rootReader.setPosition(i);
            Assert.assertEquals(i, rootReader.reader("int").readInteger().intValue());
            Assert.assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
          }

          parent.close();
        }
      }

    }
  }

  private void write(MapVector parent, File file) throws FileNotFoundException, IOException {
    Field rootField = parent.getField();
    Schema schema = new Schema(rootField.getChildren());
    try (
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema)
            ) {
      List<ArrowFieldNode> nodes = new ArrayList<>();
      for (ValueVector vector : parent) {
        appendNodes(vector, nodes);
      }
      List<ArrowBuf> buffers = new ArrayList<>(asList(parent.getBuffers(false)));
      arrowWriter.writeRecordBatch(new ArrowRecordBatch(parent.getAccessor().getValueCount(), nodes, buffers));
    }
  }

  private void appendNodes(ValueVector vector, List<ArrowFieldNode> nodes) {
    Accessor accessor = vector.getAccessor();
    int nullCount = 0;
    // TODO: should not have to do that
    // we can do that a lot more efficiently (for example with Long.bitCount(i))
    for (int i = 0; i < accessor.getValueCount(); i++) {
      if (accessor.isNull(i)) {
        nullCount ++;
      }
    }
    nodes.add(new ArrowFieldNode(accessor.getValueCount(), nullCount));
    for (ValueVector child : vector) {
      appendNodes(child, nodes);
    }
  }



}
