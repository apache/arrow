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

package org.apache.arrow.dataset.source;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * DatasetFactory provides a way to inspect a Dataset potential
 * schema before materializing it. Thus, the user can peek the schema for
 * data sources and decide on a unified schema.
 */
public interface DatasetFactory extends AutoCloseable {

  /**
   * Get unified schema for the resulting Dataset.
   *
   * @return the schema object inspected
   */
  Schema inspect();

  /**
   * Create a Dataset with auto-inferred schema. Which means, the schema of the resulting Dataset will be
   * the same with calling {@link #inspect()} manually.
   *
   * @return the Dataset instance
   */
  Dataset finish();

  /**
   * Create a Dataset with predefined schema. Schema inference will not be performed.
   *
   * @param schema a predefined schema
   * @return the Dataset instance
   */
  Dataset finish(Schema schema);
}
