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
package org.apache.arrow.adapter.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestWriteReadAvroRecord {

  @TempDir public static File TMP;

  @Test
  public void testWriteAndRead() throws Exception {
    File dataFile = new File(TMP, "test.avro");
    Schema schema = AvroTestBase.getSchema("test.avsc");

    // write data to disk
    GenericRecord user1 = new GenericData.Record(schema);
    user1.put("name", "Alyssa");
    user1.put("favorite_number", 256);

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<GenericRecord>(datumWriter)) {
      dataFileWriter.create(schema, dataFile);
      dataFileWriter.append(user1);
      dataFileWriter.append(user2);
    }

    // read data from disk
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    List<GenericRecord> result = new ArrayList<>();
    try (DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<GenericRecord>(dataFile, datumReader)) {
      while (dataFileReader.hasNext()) {
        GenericRecord user = dataFileReader.next();
        result.add(user);
      }
    }

    assertEquals(2, result.size());
    GenericRecord deUser1 = result.get(0);
    assertEquals("Alyssa", deUser1.get("name").toString());
    assertEquals(256, deUser1.get("favorite_number"));
    assertEquals(null, deUser1.get("favorite_color"));

    GenericRecord deUser2 = result.get(1);
    assertEquals("Ben", deUser2.get("name").toString());
    assertEquals(7, deUser2.get("favorite_number"));
    assertEquals("red", deUser2.get("favorite_color").toString());
  }
}
