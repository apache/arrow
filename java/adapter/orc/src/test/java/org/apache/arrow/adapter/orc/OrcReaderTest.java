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

package org.apache.arrow.adapter.orc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;


import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;



public class OrcReaderTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private static final int MAX_ALLOCATION = 8 * 1024;
  private static RootAllocator allocator;

  @BeforeClass
  public static void beforeClass() {
    allocator = new RootAllocator(MAX_ALLOCATION);
  }

  @Test
  public void testOrcJniReader() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");
    File testFile = new File(testFolder.getRoot(), "test-orc");

    Writer writer = OrcFile.createWriter(new Path(testFile.getAbsolutePath()),
                                    OrcFile.writerOptions(new Configuration()).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector longColumnVector = (LongColumnVector) batch.cols[0];
    BytesColumnVector bytesColumnVector = (BytesColumnVector) batch.cols[1];
    for (int r = 0; r < 1024; ++r) {
      int row = batch.size++;
      longColumnVector.vector[row] = r;
      byte[] buffer = ("Last-" + (r * 3)).getBytes(StandardCharsets.UTF_8);
      bytesColumnVector.setRef(row, buffer, 0, buffer.length);
    }
    writer.addRowBatch(batch);
    writer.close();

    OrcReader reader = new OrcReader(testFile.getAbsolutePath(), allocator);
    assertEquals(1, reader.getNumberOfStripes());

    ArrowReader stripeReader = reader.nextStripeReader(1024);
    VectorSchemaRoot schemaRoot = stripeReader.getVectorSchemaRoot();
    stripeReader.loadNextBatch();

    List<FieldVector> fields = schemaRoot.getFieldVectors();
    assertEquals(2, fields.size());

    IntVector intVector = (IntVector)fields.get(0);
    VarCharVector varCharVector = (VarCharVector)fields.get(1);
    for (int i = 0; i < 1024; ++i) {
      assertEquals(i, intVector.get(i));
      assertEquals("Last-" + (i * 3), new String(varCharVector.get(i), StandardCharsets.UTF_8));
    }

    assertFalse(stripeReader.loadNextBatch());
    assertNull(reader.nextStripeReader(1024));

    stripeReader.close();
    reader.close();
  }
}
