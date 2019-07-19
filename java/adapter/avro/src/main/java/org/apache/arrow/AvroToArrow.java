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

import java.io.IOException;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

/**
 * Utility class to convert Avro objects to columnar Arrow format objects.
 */
public class AvroToArrow {

  /**
   * Fetch the data from {@link GenericDatumReader} and convert it to Arrow objects.
   * @param schema avro schema.
   * @param allocator Memory allocator to use.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   */
  public static VectorSchemaRoot avroToArrow(Schema schema, Decoder decoder, BaseAllocator allocator)
      throws IOException {
    Preconditions.checkNotNull(schema, "Avro schema object can not be null");

    VectorSchemaRoot root = VectorSchemaRoot.create(
        AvroToArrowUtils.avroToArrowSchema(schema), allocator);
    AvroToArrowUtils.avroToArrowVectors(decoder, root);
    return root;
  }
}
