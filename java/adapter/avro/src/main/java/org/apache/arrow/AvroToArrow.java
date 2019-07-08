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

package org.apache.arrow;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * Utility class to convert Avro objects to columnar Arrow format objects.
 */
public class AvroToArrow {

  /**
   * Fetch the data from {@link DataFileReader} and convert it to Arrow objects.
   * @param reader avro data file reader.
   * @param allocator Memory allocator to use.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   */
  public static VectorSchemaRoot readToArrow(DataFileReader<GenericRecord> reader, BaseAllocator allocator) {
    Preconditions.checkNotNull(reader, "Avro DataFileReader object can not be null");

    VectorSchemaRoot root = VectorSchemaRoot.create(
        AvroToArrowUtils.avroToArrowSchema(reader.getSchema()), allocator);
    AvroToArrowUtils.avroToArrowVectors(reader, root);
    return root;
  }

  /**
   * Fetch the data with given avro schema file and dataFile, convert it to Arrow objects.
   * @param schemaFile avro schema file.
   * @param dataFile avro data file.
   * @param allocator Memory allocator to use.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   */
  public static VectorSchemaRoot readToArrow(File schemaFile, File dataFile, BaseAllocator allocator)
      throws IOException {

    Schema schema = new Schema.Parser().parse(schemaFile);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    return readToArrow(dataFileReader, allocator);
  }

}


