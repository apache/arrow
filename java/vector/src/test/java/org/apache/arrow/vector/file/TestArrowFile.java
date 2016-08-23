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
package org.apache.arrow.vector.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

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

      write((MapVector)parent.getChild("root"), file);
      parent.close();
    }

    {
      try (
          FileInputStream fileInputStream = new FileInputStream(file);
          ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), allocator)
          ) {
        ArrowFooter footer = arrowReader.readFooter();
        Schema schema = footer.getSchema();
        System.out.println("reading schema: " + schema);

        // initialize vectors
        MapVector parent = new MapVector("parent", allocator, null);
        ComplexWriter writer = new ComplexWriterImpl("root", parent);
        MapWriter rootWriter = writer.rootAsMap();
        MapVector root = (MapVector)parent.getChild("root");

        VectorLoader vectorLoader = new VectorLoader(schema, root);

        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock);

          vectorLoader.load(recordBatch);

          MapReader rootReader = new SingleMapReaderImpl(parent).reader("root");
          for (int i = 0; i < count; i++) {
            rootReader.setPosition(i);
            Assert.assertEquals(i, rootReader.reader("int").readInteger().intValue());
            Assert.assertEquals(i, rootReader.reader("bigInt").readLong().longValue());
          }

        }
        parent.close();
      }

    }
  }

//  private void validateLayout(List<Field> fields, Iterable<ValueVector> childVectors) {
//    int i = 0;
//    for (ValueVector valueVector : childVectors) {
//      Field field = fields.get(i);
//      TypeLayout typeLayout = field.getTypeLayout();
//      TypeLayout expectedTypeLayout = valueVector.getTypeLayout();
//      if (!expectedTypeLayout.equals(typeLayout)) {
//        throw new InvalidArrowFileException("The type layout does not match the expected layout: expected " + expectedTypeLayout + " found " + typeLayout);
//      }
//      if (field.getChildren().size() > 0) {
//        validateLayout(field.getChildren(), valueVector);
//      }
//      ++i;
//    }
//    Preconditions.checkArgument(i == fields.size(), "should have as many children as in the schema: found " + i + " expected " + fields.size());
//  }

  private void write(MapVector parent, File file) throws FileNotFoundException, IOException {
    VectorUnloader vectorUnloader = new VectorUnloader(parent);
    Schema schema = vectorUnloader.getSchema();
    System.out.println("writing schema: " + schema);
    try (
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema)
            ) {
      arrowWriter.writeRecordBatch(vectorUnloader.getRecordBatch());
    }
  }


}
