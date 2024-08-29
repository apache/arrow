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

package org.apache.arrow.dataset;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.arrow.util.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * Utility class for writing Parquet files using Avro based tools.
 */
public class ParquetWriteSupport implements AutoCloseable {

  private final String path;
  private final String uri;
  private final ParquetWriter<GenericRecord> writer;
  private final Schema avroSchema;
  private final List<GenericRecord> writtenRecords = new ArrayList<>();
  private final GenericRecordListBuilder recordListBuilder = new GenericRecordListBuilder();
  private final Random random = new Random();


  public ParquetWriteSupport(String schemaName, File outputFolder) throws Exception {
    avroSchema = getSchema(schemaName);
    path = outputFolder.getPath() + "/" + "generated-" + random.nextLong() + ".parquet";
    uri = "file://" + path;
    writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
        .withSchema(avroSchema)
        .build();
  }

  public static Schema getSchema(String schemaName) throws Exception {
    try {
      // Attempt to use JDK 9 behavior of getting the module then the resource stream from the module.
      // Note that this code is caller-sensitive.
      Method getModuleMethod = Class.class.getMethod("getModule");
      Object module = getModuleMethod.invoke(ParquetWriteSupport.class);
      Method getResourceAsStreamFromModule = module.getClass().getMethod("getResourceAsStream", String.class);
      try (InputStream is = (InputStream) getResourceAsStreamFromModule.invoke(module, "/avroschema/" + schemaName)) {
        return new Schema.Parser()
            .parse(is);
      }
    } catch (NoSuchMethodException ex) {
      // Use JDK8 behavior.
      try (InputStream is = ParquetWriteSupport.class.getResourceAsStream("/avroschema/" + schemaName)) {
        return new Schema.Parser().parse(is);
      }
    }
  }

  public static ParquetWriteSupport writeTempFile(String schemaName, File outputFolder,
      Object... values) throws Exception {
    try (final ParquetWriteSupport writeSupport = new ParquetWriteSupport(schemaName, outputFolder)) {
      writeSupport.writeRecords(values);
      return writeSupport;
    }
  }

  public void writeRecords(Object... values) throws Exception {
    final List<GenericRecord> valueList = getRecordListBuilder().createRecordList(values);
    writeRecords(valueList);
  }

  public void writeRecords(List<GenericRecord> records) throws Exception {
    for (GenericRecord record : records) {
      writeRecord(record);
    }
  }

  public void writeRecord(GenericRecord record) throws Exception {
    writtenRecords.add(record);
    writer.write(record);
  }

  public String getOutputURI() {
    return uri;
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }

  public GenericRecordListBuilder getRecordListBuilder() {
    return recordListBuilder;
  }

  public List<GenericRecord> getWrittenRecords() {
    return Collections.unmodifiableList(writtenRecords);
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }

  public class GenericRecordListBuilder {
    public final List<GenericRecord> createRecordList(Object... values) {
      final int fieldCount = avroSchema.getFields().size();
      Preconditions.checkArgument(values.length % fieldCount == 0,
          "arg count of values should be divide by field number");
      final List<GenericRecord> recordList = new ArrayList<>();
      for (int i = 0; i < values.length / fieldCount; i++) {
        final GenericRecord record = new GenericData.Record(avroSchema);
        for (int j = 0; j < fieldCount; j++) {
          record.put(j, values[i * fieldCount + j]);
        }
        recordList.add(record);
      }
      return Collections.unmodifiableList(recordList);
    }
  }
}
