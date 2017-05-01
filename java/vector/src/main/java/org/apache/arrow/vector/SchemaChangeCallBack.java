/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import org.apache.arrow.vector.util.CallBack;


public class SchemaChangeCallBack implements CallBack {
  private boolean schemaChanged = false;

  /**
   * Constructs a schema-change callback with the schema-changed state set to
   * {@code false}.
   */
  public SchemaChangeCallBack() {
  }

  /**
   * Sets the schema-changed state to {@code true}.
   */
  @Override
  public void doWork() {
    schemaChanged = true;
  }

  /**
   * Returns the value of schema-changed state, <strong>resetting</strong> the
   * schema-changed state to {@code false}.
   * @return the previous schema-changed state
   */
  public boolean getSchemaChangedAndReset() {
    final boolean current = schemaChanged;
    schemaChanged = false;
    return current;
  }
}

