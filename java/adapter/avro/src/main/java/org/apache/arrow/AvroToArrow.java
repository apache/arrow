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

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

/**
 * Utility class to convert Avro objects to columnar Arrow format objects.
 */
public class AvroToArrow {

  /**
   * Fetch the data from {@link Decoder} and convert it to Arrow objects.
   * Only for testing purpose.
   * @param schema avro schema.
   * @param decoder avro decoder
   * @param config configuration of the conversion.
   * @return Arrow Data Objects {@link VectorSchemaRoot}
   */
  static VectorSchemaRoot avroToArrow(Schema schema, Decoder decoder, AvroToArrowConfig config)
      throws IOException {
    Preconditions.checkNotNull(schema, "Avro schema object can not be null");
    Preconditions.checkNotNull(decoder, "Avro decoder object can not be null");
    Preconditions.checkNotNull(config, "config can not be null");

    return AvroToArrowUtils.avroToArrowVectors(schema, decoder, config);
  }

  /**
   * Fetch the data from {@link Decoder} and iteratively convert it to Arrow objects.
   * @param schema avro schema
   * @param decoder avro decoder
   * @param config configuration of the conversion.
   * @throws IOException on error
   */
  public static AvroToArrowVectorIterator avroToArrowIterator(
      Schema schema,
      Decoder decoder,
      AvroToArrowConfig config) throws IOException {

    Preconditions.checkNotNull(schema, "Avro schema object can not be null");
    Preconditions.checkNotNull(decoder, "Avro decoder object can not be null");
    Preconditions.checkNotNull(config, "config can not be null");

    return AvroToArrowVectorIterator.create(decoder, schema, config);
  }
}
