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

package org.apache.arrow.gandiva.evaluator;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.gandiva.exceptions.EvaluatorClosedException;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.exceptions.UnsupportedTypeException;
import org.apache.arrow.gandiva.expression.ArrowTypeHelper;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

/**
 * This class provides a mechanism to evaluate a set of expressions against a RecordBatch.
 * Follow these steps to use this class:
 * 1) Use the static method make() to create an instance of this class that evaluates a
 *    set of expressions
 * 2) Invoke the method evaluate() to evaluate these expressions against a RecordBatch
 * 3) Invoke close() to release resources
 */
public class Projector {
  private static final org.slf4j.Logger logger =
          org.slf4j.LoggerFactory.getLogger(Projector.class);

  private final long moduleId;
  private final Schema schema;
  private final int numExprs;
  private boolean closed;

  private Projector(long moduleId, Schema schema, int numExprs) {
    this.moduleId = moduleId;
    this.schema = schema;
    this.numExprs = numExprs;
    this.closed = false;
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evalute() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs)
          throws GandivaException {
    return make(schema, exprs, ConfigurationBuilder.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evalute() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param configurationId Custom configuration created through config builder.
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs, long
          configurationId) throws GandivaException {
    // serialize the schema and the list of expressions as a protobuf
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }

    // Invoke the JNI layer to create the LLVM module representing the expressions
    GandivaTypes.Schema schemaBuf = ArrowTypeHelper.arrowSchemaToProtobuf(schema);
    JniWrapper gandivaBridge = JniWrapper.getInstance();
    long moduleId = gandivaBridge.buildProjector(schemaBuf.toByteArray(), builder.build()
            .toByteArray(), configurationId);
    logger.info("Created module for the projector with id {}", moduleId);
    return new Projector(moduleId, schema, exprs.size());
  }

  /**
   * Invoke this function to evaluate a set of expressions against a recordBatch.
   *
   * @param recordBatch Record batch including the data
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch, List<ValueVector> outColumns)
          throws GandivaException {
    evaluate(recordBatch.getLength(), recordBatch.getBuffers(), recordBatch.getBuffersLayout(),
        outColumns);
  }

  /**
   * Invoke this function to evaluate a set of expressions against a set of arrow buffers.
   * (this is an optimised version that skips taking references).
   *
   * @param numRows number of rows.
   * @param buffers List of input arrow buffers
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(int numRows, List<ArrowBuf> buffers,
                       List<ValueVector> outColumns) throws GandivaException {
    List<ArrowBuffer> buffersLayout = new ArrayList<>();
    long offset = 0;
    for (ArrowBuf arrowBuf : buffers) {
      long size = arrowBuf.readableBytes();
      buffersLayout.add(new ArrowBuffer(offset, size));
      offset += size;
    }
    evaluate(numRows, buffers, buffersLayout, outColumns);
  }

  private void evaluate(int numRows, List<ArrowBuf> buffers, List<ArrowBuffer> buffersLayout,
                       List<ValueVector> outColumns) throws GandivaException {
    if (this.closed) {
      throw new EvaluatorClosedException();
    }

    if (numExprs != outColumns.size()) {
      logger.info("Expected " + numExprs + " columns, got " + outColumns.size());
      throw new GandivaException("Incorrect number of columns for the output vector");
    }

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

    long[] outAddrs = new long[2 * outColumns.size()];
    long[] outSizes = new long[2 * outColumns.size()];
    idx = 0;
    for (ValueVector valueVector : outColumns) {
      if (!(valueVector instanceof FixedWidthVector)) {
        throw new UnsupportedTypeException("Unsupported value vector type");
      }

      outAddrs[idx] = valueVector.getValidityBuffer().memoryAddress();
      outSizes[idx++] = valueVector.getValidityBuffer().capacity();
      outAddrs[idx] = valueVector.getDataBuffer().memoryAddress();
      outSizes[idx++] = valueVector.getDataBuffer().capacity();

      valueVector.setValueCount(numRows);
    }

    JniWrapper.getInstance().evaluateProjector(this.moduleId, numRows,
            bufAddrs, bufSizes,
            outAddrs, outSizes);
  }

  /**
   * Closes the LLVM module representing this evaluator.
   */
  public void close() throws GandivaException {
    if (this.closed) {
      return;
    }

    JniWrapper.getInstance().closeProjector(this.moduleId);
    this.closed = true;
  }
}
