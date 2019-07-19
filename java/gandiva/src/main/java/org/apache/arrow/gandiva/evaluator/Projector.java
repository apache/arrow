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
import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthVector;
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

  private JniWrapper wrapper;
  private final long moduleId;
  private final Schema schema;
  private final int numExprs;
  private boolean closed;

  private Projector(JniWrapper wrapper, long moduleId, Schema schema, int numExprs) {
    this.wrapper = wrapper;
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
    return make(schema, exprs, SelectionVectorType.SV_NONE, JniLoader.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evalute() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
      SelectionVectorType selectionVectorType)
          throws GandivaException {
    return make(schema, exprs, selectionVectorType, JniLoader.getDefaultConfiguration());
  }

  /**
   * Invoke this function to generate LLVM code to evaluate the list of project expressions.
   * Invoke Projector::Evalute() against a RecordBatch to evaluate the record batch
   * against these projections.
   *
   * @param schema Table schema. The field names in the schema should match the fields used
   *               to create the TreeNodes
   * @param exprs  List of expressions to be evaluated against data
   * @param selectionVectorType type of selection vector
   * @param configurationId Custom configuration created through config builder.
   *
   * @return A native evaluator object that can be used to invoke these projections on a RecordBatch
   */
  public static Projector make(Schema schema, List<ExpressionTree> exprs,
      SelectionVectorType selectionVectorType,
      long configurationId) throws GandivaException {
    // serialize the schema and the list of expressions as a protobuf
    GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
    for (ExpressionTree expr : exprs) {
      builder.addExprs(expr.toProtobuf());
    }

    // Invoke the JNI layer to create the LLVM module representing the expressions
    GandivaTypes.Schema schemaBuf = ArrowTypeHelper.arrowSchemaToProtobuf(schema);
    JniWrapper wrapper = JniLoader.getInstance().getWrapper();
    long moduleId = wrapper.buildProjector(schemaBuf.toByteArray(),
        builder.build().toByteArray(), selectionVectorType.getNumber(), configurationId);
    logger.debug("Created module for the projector with id {}", moduleId);
    return new Projector(wrapper, moduleId, schema, exprs.size());
  }

  /**
   * Invoke this function to evaluate a set of expressions against a recordBatch.
   *
   * @param recordBatch Record batch including the data
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch, List<ValueVector> outColumns)
          throws GandivaException {
    evaluate(recordBatch.getLength(), recordBatch.getBuffers(),
             recordBatch.getBuffersLayout(),
             SelectionVectorType.SV_NONE.getNumber(), recordBatch.getLength(),
             0, 0, outColumns);
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
    evaluate(numRows, buffers, buffersLayout,
             SelectionVectorType.SV_NONE.getNumber(),
             numRows, 0, 0, outColumns);
  }

  /**
   * Invoke this function to evaluate a set of expressions against a {@link ArrowRecordBatch}.
   *
   * @param recordBatch The data to evaluate against.
   * @param selectionVector Selection vector which stores the selected rows.
   * @param outColumns Result of applying the project on the data
   */
  public void evaluate(ArrowRecordBatch recordBatch,
                     SelectionVector selectionVector, List<ValueVector> outColumns)
        throws GandivaException {
    evaluate(recordBatch.getLength(), recordBatch.getBuffers(),
        recordBatch.getBuffersLayout(),
        selectionVector.getType().getNumber(),
        selectionVector.getRecordCount(),
        selectionVector.getBuffer().memoryAddress(),
        selectionVector.getBuffer().capacity(),
        outColumns);
  }

  /**
 * Invoke this function to evaluate a set of expressions against a set of arrow buffers
 * on the selected positions.
 * (this is an optimised version that skips taking references).
 *
 * @param numRows number of rows.
 * @param buffers List of input arrow buffers
 * @param selectionVector Selection vector which stores the selected rows.
 * @param outColumns Result of applying the project on the data
 */
  public void evaluate(int numRows, List<ArrowBuf> buffers,
                     SelectionVector selectionVector,
                     List<ValueVector> outColumns) throws GandivaException {
    List<ArrowBuffer> buffersLayout = new ArrayList<>();
    long offset = 0;
    for (ArrowBuf arrowBuf : buffers) {
      long size = arrowBuf.readableBytes();
      buffersLayout.add(new ArrowBuffer(offset, size));
      offset += size;
    }
    evaluate(numRows, buffers, buffersLayout,
        selectionVector.getType().getNumber(),
        selectionVector.getRecordCount(),
        selectionVector.getBuffer().memoryAddress(),
        selectionVector.getBuffer().capacity(),
        outColumns);
  }

  private void evaluate(int numRows, List<ArrowBuf> buffers, List<ArrowBuffer> buffersLayout,
                       int selectionVectorType, int selectionVectorRecordCount,
                       long selectionVectorAddr, long selectionVectorSize,
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

    boolean hasVariableWidthColumns = false;
    BaseVariableWidthVector[] resizableVectors = new BaseVariableWidthVector[outColumns.size()];
    long[] outAddrs = new long[3 * outColumns.size()];
    long[] outSizes = new long[3 * outColumns.size()];
    idx = 0;
    int outColumnIdx = 0;
    for (ValueVector valueVector : outColumns) {
      boolean isFixedWith = valueVector instanceof FixedWidthVector;
      boolean isVarWidth = valueVector instanceof VariableWidthVector;
      if (!isFixedWith && !isVarWidth) {
        throw new UnsupportedTypeException(
            "Unsupported value vector type " + valueVector.getField().getFieldType());
      }

      outAddrs[idx] = valueVector.getValidityBuffer().memoryAddress();
      outSizes[idx++] = valueVector.getValidityBuffer().capacity();
      if (isVarWidth) {
        outAddrs[idx] = valueVector.getOffsetBuffer().memoryAddress();
        outSizes[idx++] = valueVector.getOffsetBuffer().capacity();
        hasVariableWidthColumns = true;

        // save vector to allow for resizing.
        resizableVectors[outColumnIdx] = (BaseVariableWidthVector)valueVector;
      }
      outAddrs[idx] = valueVector.getDataBuffer().memoryAddress();
      outSizes[idx++] = valueVector.getDataBuffer().capacity();

      valueVector.setValueCount(selectionVectorRecordCount);
      outColumnIdx++;
    }

    wrapper.evaluateProjector(
        hasVariableWidthColumns ? new VectorExpander(resizableVectors) : null,
        this.moduleId, numRows, bufAddrs, bufSizes,
        selectionVectorType, selectionVectorRecordCount,
        selectionVectorAddr, selectionVectorSize,
        outAddrs, outSizes);
  }

  /**
   * Closes the LLVM module representing this evaluator.
   */
  public void close() throws GandivaException {
    if (this.closed) {
      return;
    }

    wrapper.closeProjector(this.moduleId);
    this.closed = true;
  }
}
