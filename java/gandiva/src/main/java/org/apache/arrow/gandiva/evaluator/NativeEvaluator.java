/*
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

import org.apache.arrow.gandiva.expression.ArrowTypeHelper;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class NativeEvaluator {

    /**
     * Invoke this function to generate LLVM code to evaluate the list of expressions
     * Invoke NativeEvaluator::Evalute() against a RecordBatch to evaluate the record batch
     * against these expressions
     *
     * @param schema Table schema. The field names in the schema should match the fields used
     *               to create the TreeNodes
     * @param exprs List of expressions to be evaluated against data
     * @return A native evaluator object that can be used to invoke these expressions on a RecordBatch
     */
    public static NativeEvaluator MakeProjector(Schema schema, List<ExpressionTree> exprs) throws Exception
    {
        // serialize the schema and the list of expressions as a protobuf
        GandivaTypes.ExpressionList.Builder builder = GandivaTypes.ExpressionList.newBuilder();
        for(ExpressionTree expr : exprs) {
            builder.addExprs(expr.toProtobuf());
        }

        // Invoke the JNI layer to create the LLVM module representing the expressions
        GandivaTypes.Schema schemaBuf = ArrowTypeHelper.ArrowSchemaToProtobuf(schema);
        long moduleID = NativeBuilder.BuildNativeCode(schemaBuf.toByteArray(), builder.build().toByteArray());
        return new NativeEvaluator(moduleID, schema);
    }

    private NativeEvaluator(long moduleID, Schema schema) {
        this.moduleID = moduleID;
        this.schema = schema;
    }

    public void Evaluate(ArrowRecordBatch recordBatch, List<ValueVector> out_columns) throws Exception {
    }

    private long moduleID;
    private Schema schema;
}
