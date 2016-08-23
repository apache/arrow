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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleMapReaderImpl;
import org.apache.arrow.vector.complex.reader.BaseReader.MapReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.VectorLayout;
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

      write((MapVector)parent.getChild("root"), file);
      parent.close();
    }

    System.out.println(file.length());
    {
      try (
          FileInputStream fileInputStream = new FileInputStream(file);
          ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), allocator)
          ) {
        ArrowFooter footer = arrowReader.readFooter();
        org.apache.arrow.vector.types.pojo.Schema schema = footer.getSchema();
        System.out.println("reading schema: " + schema);

        // initialize vectors
        MapVector parent = new MapVector("parent", allocator, null);
        ComplexWriter writer = new ComplexWriterImpl("root", parent);
        MapWriter rootWriter = writer.rootAsMap();
        MapVector root = (MapVector)parent.getChild("root");

        List<Field> fields = schema.getFields();
        root.initializeChildren(fields);
        List<FieldVector> fieldVectors = root.getFieldVectors();
        if (fieldVectors.size() != fields.size()) {
          throw new IllegalArgumentException(); //TODO
        }

//        validateLayout(fields, parent);
        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock);

          Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
          Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
          System.out.println(recordBatch.getNodes().size() + " nodes");
          System.out.println(recordBatch.getBuffers().size() + " buffers");
          for (int i = 0; i < fields.size(); ++i) {
            Field field = fields.get(i);
            FieldVector fieldVector = fieldVectors.get(i);
            loadBuffers(fieldVector, field, buffers, nodes);
          }

//          public void load(List<Field> fields, int length, Iterator<ArrowFieldNode> nodes, Iterator<ArrowBuf> buffers) {
//
//            for (Field field : fields) {
//              MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
//              ValueVector vector = this.add(field.getName(), minorType);
//
//
//              vector.loadBuffers(typeLayout, ownBuffers);
//              List<Field> children = field.getChildren();
//              for (Field child : children) {
//                addChild((NestedVector)vector, child, nodes, buffers);
//                vector.loadChild()
//              }
//            }
//          }
//          parent.load(fields, recordBatch.getLength(), nodes, buffers);

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

  private void loadBuffers(FieldVector vector, Field field, Iterator<ArrowBuf> buffers, Iterator<ArrowFieldNode> nodes) {
    ArrowFieldNode fieldNode = nodes.next();
    List<VectorLayout> typeLayout = field.getTypeLayout().getVectors();
    List<ArrowBuf> ownBuffers = new ArrayList<>(typeLayout.size());
    for (int j = 0; j < typeLayout.size(); j++) {
      ownBuffers.add(buffers.next());
    }
    vector.loadFieldBuffers(fieldNode, ownBuffers);
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      int i = 0;
      checkArgument(children.size() == childrenFromFields.size(), "should have as many children as in the schema: found " + childrenFromFields.size() + " expected " + children.size());
      for (Field child : children) {
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes);
        ++i;
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
    Field rootField = parent.getField();
    Schema schema = new Schema(rootField.getChildren());
    System.out.println("writing schema: " + schema);
    try (
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema)
            ) {
      List<ArrowFieldNode> nodes = new ArrayList<>();
      List<ArrowBuf> buffers = new ArrayList<>();
      for (FieldVector vector : parent.getFieldVectors()) {
        appendNodes(vector, nodes, buffers);
      }
      arrowWriter.writeRecordBatch(new ArrowRecordBatch(parent.getAccessor().getValueCount(), nodes, buffers));
    }
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
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
    // TODO: validate buffer count
    buffers.addAll(vector.getFieldBuffers());
    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }



}
