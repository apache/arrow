/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.arrow.gandiva.exceptions.UnsupportedTypeException;
import org.apache.arrow.gandiva.expression.ArrowTypeHelper;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * This class provides a mechanism to evaluate a set of expressions against a RecordBatch.
 * Follow these steps to use this class:
 * 1) Use the static method makeProjector() to create an instance of this class that evaluates a
 *    set of expressions
 * 2) Invoke the method evaluate() to evaluate these expressions against a RecordBatch
 * 3) Invoke close() to release resources
 */
public class NativeEvaluator {
  private static final org.slf4j.Logger logger =
          org.slf4j.LoggerFactory.getLogger(NativeEvaluator.class);

  private final long moduleId;
  private final Schema schema;
  private final int numExprs;
  private boolean closed;

  private NativeEvaluator(long moduleId, Schema schema, int numExprs) {
    this.moduleId = moduleId;
    this.schema = schema;
    this.numExprs = numExprs;
    this.closed = false;
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke NativeEvaluator::Evalute() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static NativeEvaluator makeProjector(Schema schema, List<ExpressionTree> exprs)
          throws GandivaException {
    return makeProjector(schema, exprs, ConfigurationBuilder.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke NativeEvaluator::Evalute() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param configurationId Custom configuration created through config builder.
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static NativeEvaluator makeProjector(Schema schema, List<ExpressionTree> exprs, long
          configurationId) throws GandivaException {
    // serialize the schema and the list of expressions as a protobuf
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }

    // Invoke the JNI layer to create the LLVM module representing the expressions
    GandivaTypes.Schema schemaBuf = ArrowTypeHelper.arrowSchemaToProtobuf(schema);
    NativeBuilder gandivaBridge = NativeBuilder.getInstance();
    long moduleId = gandivaBridge.buildNativeCode(schemaBuf.toByteArray(), builder.build()
            .toByteArray(), configurationId);
    return new NativeEvaluator(moduleId, schema, exprs.size());
  }

  /**
   * Invoke this function to evaluate a set of expressions against a recordBatch.
   *
   * @param recordBatch Record batch including the data
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch, List<ValueVector> outColumns)
          throws GandivaException {
    if (this.closed) {
      throw new EvaluatorClosedException();
    }

    if (numExprs != outColumns.size()) {
      logger.info("Expected " + numExprs + " columns, got " + outColumns.size());
      throw new GandivaException("Incorrect number of columns for the output vector");
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

    int numRows = recordBatch.getLength();
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

    NativeBuilder.getInstance().evaluate(this.moduleId, numRows,
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

    NativeBuilder.getInstance().close(this.moduleId);
    this.closed = true;
  }
}
