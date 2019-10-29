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

package org.apache.arrow.adapter.parquet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.buffer.ArrowBuf;

public class ParquetReadWriteTest {

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void testParquetReadWrite() throws Exception {

    File testFile = testFolder.newFile("_tmpfile_ParquetWriterReaderTest");
    String path = testFile.getAbsolutePath();

    int numColumns = 10;
    int[] rowGroupIndices = {0};
    int[] columnIndices = new int[numColumns];
    ;
    for (int i = 0; i < numColumns; i++) {
      columnIndices[i] = i;
    }

    Schema schema =
        new Schema(
            asList(
                field("a", new Int(32, true)),
                field("b", new Int(32, true)),
                field("c", new Int(32, true)),
                field("d", new Int(32, true)),
                field("e", new Int(32, true)),
                field("f", new Int(32, true)),
                field("g", new Int(32, true)),
                field("h", new Int(32, true)),
                field("i", new Int(32, true)),
                field("j", new Int(32, true))));

    VectorSchemaRoot expectedSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    for (FieldVector vector : expectedSchemaRoot.getFieldVectors()) {
      vector.allocateNew();
      IntVector intVector = (IntVector) vector;
      for (int i = 0; i < 16; i++) {
        intVector.set(i, i);
      }
    }

    ArrowRecordBatch batch = createArrowRecordBatch(expectedSchemaRoot);
    ParquetWriter writer = new ParquetWriter(path, schema);
    writer.writeNext(batch);
    releaseArrowRecordBatch(batch);
    writer.close();

    ParquetReader reader = new ParquetReader(path, rowGroupIndices, columnIndices, 16, allocator);

    Schema readedSchema = reader.getSchema();
    assertEquals(schema.toJson(), readedSchema.toJson());

    VectorSchemaRoot actualSchemaRoot = VectorSchemaRoot.create(readedSchema, allocator);
    reader.readNextVectors(actualSchemaRoot);

    assertEquals(actualSchemaRoot.getRowCount(), expectedSchemaRoot.getRowCount());
    assertEquals(actualSchemaRoot.contentToTSVString(), expectedSchemaRoot.contentToTSVString());

    actualSchemaRoot.close();
    expectedSchemaRoot.close();
    reader.close();
    testFile.delete();
  }

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null, null), asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  private ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  private ArrowRecordBatch createArrowRecordBatch(VectorSchemaRoot root) {
    List<ArrowFieldNode> fieldNodes = new ArrayList<ArrowFieldNode>();
    List<ArrowBuf> inputData = new ArrayList<ArrowBuf>();
    int numRowsInBatch = root.getRowCount();
    for (FieldVector inputVector : root.getFieldVectors()) {
      fieldNodes.add(new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount()));
      inputData.add(inputVector.getValidityBuffer());
      inputData.add(inputVector.getDataBuffer());
    }
    return new ArrowRecordBatch(numRowsInBatch, fieldNodes, inputData);
  }

  private void releaseArrowRecordBatch(ArrowRecordBatch recordBatch) {
    recordBatch.close();
  }
}
