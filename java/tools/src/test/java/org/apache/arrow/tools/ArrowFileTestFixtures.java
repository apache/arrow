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
package org.apache.arrow.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.ViewVarBinaryWriter;
import org.apache.arrow.vector.complex.writer.ViewVarCharWriter;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.util.Text;

public class ArrowFileTestFixtures {
  static final int COUNT = 10;

  static void writeData(int count, NonNullableStructVector parent) {
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    for (int i = 0; i < count; i++) {
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
    }
    writer.setValueCount(count);
  }

  private static String generateString(int length) {
    StringBuilder stringBuilder = new StringBuilder(length);

    for (int i = 0; i < length; i++) {
      stringBuilder.append(i);
    }

    return stringBuilder.toString();
  }

  private static byte[] generateBytes(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) i;
    }
    return bytes;
  }

  static void writeVariableWidthViewData(int count, NonNullableStructVector parent) {
    ComplexWriter writer = new ComplexWriterImpl("root", parent);
    StructWriter rootWriter = writer.rootAsStruct();
    ViewVarCharWriter viewVarCharWriter = rootWriter.viewVarChar("viewVarChar");
    ViewVarBinaryWriter viewVarBinaryWriter = rootWriter.viewVarBinary("viewVarBinary");
    IntWriter intWriter = rootWriter.integer("int");
    BigIntWriter bigIntWriter = rootWriter.bigInt("bigInt");
    for (int i = 0; i < count; i++) {
      viewVarCharWriter.setPosition(i);
      viewVarCharWriter.writeViewVarChar(generateString(i));
      viewVarBinaryWriter.setPosition(i);
      viewVarBinaryWriter.writeViewVarBinary(generateBytes(i));
      intWriter.setPosition(i);
      intWriter.writeInt(i);
      bigIntWriter.setPosition(i);
      bigIntWriter.writeBigInt(i);
    }
    writer.setValueCount(count);
  }

  static void validateOutput(File testOutFile, BufferAllocator allocator) throws Exception {
    // read
    try (BufferAllocator readerAllocator =
            allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(testOutFile);
        ArrowFileReader arrowReader =
            new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        if (!arrowReader.loadRecordBatch(rbBlock)) {
          throw new IOException("Expected to read record batch");
        }
        validateContent(COUNT, root);
      }
    }
  }

  static void validateVariadicOutput(File testOutFile, BufferAllocator allocator, int count)
      throws Exception {
    // read
    try (BufferAllocator readerAllocator =
            allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(testOutFile);
        ArrowFileReader arrowReader =
            new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        if (!arrowReader.loadRecordBatch(rbBlock)) {
          throw new IOException("Expected to read record batch");
        }
        validateVariadicContent(count, root);
      }
    }
  }

  static void validateContent(int count, VectorSchemaRoot root) {
    assertEquals(count, root.getRowCount());
    for (int i = 0; i < count; i++) {
      assertEquals(i, root.getVector("int").getObject(i));
      assertEquals(Long.valueOf(i), root.getVector("bigInt").getObject(i));
    }
  }

  static void validateVariadicContent(int count, VectorSchemaRoot root) {
    assertEquals(count, root.getRowCount());
    ViewVarCharVector viewVarCharVector = (ViewVarCharVector) root.getVector("viewVarChar");
    ViewVarBinaryVector viewVarBinaryVector = (ViewVarBinaryVector) root.getVector("viewVarBinary");
    IntVector intVector = (IntVector) root.getVector("int");
    BigIntVector bigIntVector = (BigIntVector) root.getVector("bigInt");
    for (int i = 0; i < count; i++) {
      assertEquals(new Text(generateString(i)), viewVarCharVector.getObject(i));
      assertArrayEquals(generateBytes(i), viewVarBinaryVector.get(i));
      assertEquals(i, intVector.getObject(i));
      assertEquals(Long.valueOf(i), bigIntVector.getObject(i));
    }
  }

  static void write(FieldVector parent, File file) throws IOException {
    VectorSchemaRoot root = new VectorSchemaRoot(parent);
    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowFileWriter arrowWriter =
            new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
      arrowWriter.writeBatch();
    }
  }

  static void writeInput(File testInFile, BufferAllocator allocator) throws IOException {
    try (BufferAllocator vectorAllocator =
            allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", vectorAllocator)) {
      writeData(COUNT, parent);
      write(parent.getChild("root"), testInFile);
    }
  }

  static void writeVariableWidthViewInput(File testInFile, BufferAllocator allocator, int count)
      throws IOException {
    try (BufferAllocator vectorAllocator =
            allocator.newChildAllocator("original view vectors", 0, Integer.MAX_VALUE);
        NonNullableStructVector parent = NonNullableStructVector.empty("parent", vectorAllocator)) {
      writeVariableWidthViewData(count, parent);
      write(parent.getChild("root"), testInFile);
    }
  }
}
