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

package org.apache.arrow.gandiva.evaluator;

import io.netty.buffer.ArrowBuf;

import org.apache.arrow.gandiva.exceptions.EvaluatorClosedException;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ArrowTypeHelper;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class provides a mechanism to filter a RecordBatch by evaluating a condition expression.
 * Follow these steps to use this class:
 * 1) Use the static method make() to create an instance of this class that evaluates a
 * condition.
 * 2) Invoke the method evaluate() to evaluate the filter against a RecordBatch
 * 3) Invoke close() to release resources
 */
public class Filter {
  private static final Logger logger = LoggerFactory.getLogger(Filter.class);

  private final long moduleId;
  private final Schema schema;
  private boolean closed;

  private Filter(long moduleId, Schema schema) {
    this.moduleId = moduleId;
    this.schema = schema;
    this.closed = false;
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the condition expression.
   * Invoke Filter::Evaluate() against a RecordBatch to evaluate the filter on
   * this record batch
   *
   * @param schema    Table schema. The field names in the schema should match the fields used
   *                  to create the TreeNodes
   * @param condition condition to be evaluated against data
   * @return A native filter object that can be used to invoke on a RecordBatch
   */
  public static Filter make(Schema schema, Condition condition) throws GandivaException {
    return make(schema, condition, ConfigurationBuilder.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the condition expression.
   * Invoke Filter::Evaluate() against a RecordBatch to evaluate the filter on
   * this record batch
   *
   * @param schema          Table schema. The field names in the schema should match the fields used
   *                        to create the TreeNodes
   * @param condition       condition to be evaluated against data
   * @param configurationId Custom configuration created through config builder.
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Filter make(Schema schema, Condition condition, long configurationId)
      throws GandivaException {
    // Invoke the JNI layer to create the LLVM module representing the filter.
    GandivaTypes.Condition conditionBuf = condition.toProtobuf();
    GandivaTypes.Schema schemaBuf = ArrowTypeHelper.arrowSchemaToProtobuf(schema);
    JniWrapper gandivaBridge = JniWrapper.getInstance();
    long moduleId = gandivaBridge.buildFilter(schemaBuf.toByteArray(),
        conditionBuf.toByteArray(), configurationId);
    logger.info("Created module for the projector with id {}", moduleId);
    return new Filter(moduleId, schema);
  }

  /**
   * Invoke this function to evaluate a set of expressions against a recordBatch.
   *
   * @param recordBatch Record batch including the data
   * @param selectionVector  Result of applying the filter on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch, SelectionVector selectionVector)
      throws GandivaException {

    if (this.closed) {
      throw new EvaluatorClosedException();
    }
    int numRows = recordBatch.getLength();
    if (selectionVector.getMaxRecords() < numRows) {
      logger.error("selectionVector has capacity for " + numRows
          + " rows, minimum required " + recordBatch.getLength());
      throw new GandivaException("SelectionVector too small");
    }

    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();

    long[] bufAddrs = new long[buffers.size()];
    long[] bufSizes = new long[buffers.size()];

    int idx = 0;
    for (ArrowBuf buf : buffers) {
      bufAddrs[idx++] = buf.memoryAddress();
    }

    idx = 0;
    for (ArrowBuffer bufLayout : buffersLayout) {
      bufSizes[idx++] = bufLayout.getSize();
    }

    int numRecords = JniWrapper.getInstance().evaluateFilter(this.moduleId, numRows,
        bufAddrs, bufSizes,
        selectionVector.getType().getNumber(),
        selectionVector.getBuffer().memoryAddress(), selectionVector.getBuffer().capacity());
    if (numRecords >= 0) {
      selectionVector.setRecordCount(numRecords);
    }
  }

  /**
   * Closes the LLVM module representing this filter.
   */
  public void close() throws GandivaException {
    if (this.closed) {
      return;
    }

    JniWrapper.getInstance().closeFilter(this.moduleId);
    this.closed = true;
  }
}
