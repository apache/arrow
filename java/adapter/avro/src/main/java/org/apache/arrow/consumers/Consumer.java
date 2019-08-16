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

package org.apache.arrow.consumers;

import java.io.IOException;

import org.apache.arrow.vector.FieldVector;
import org.apache.avro.io.Decoder;

/**
 * Interface that is used to consume values from avro decoder.
 */
public interface Consumer extends AutoCloseable {

  /**
   * Consume a specific type value from avro decoder and write it to vector.
   * @param decoder avro decoder to read data
   * @throws IOException on error
   */
  void consume(Decoder decoder) throws IOException;

  /**
   * Add null value to vector by making writer position + 1.
   */
  void addNull();

  /**
   * Set the position to write value into vector.
   */
  void setPosition(int index);

  /**
   * Get the vector within the consumer.
   */
  FieldVector getVector();

  /**
   * Close this consumer when occurs exception to avoid potential leak.
   */
  void close() throws Exception;
}
