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

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Opaque class representing C++ expression
 */
public class ExpressionTree {
    ExpressionTree(TreeNode root, Field result_field) {
        this.root = root;
        this.result_field = result_field;
    }

    public GandivaTypes.ExpressionRoot toProtobuf() throws Exception {
        GandivaTypes.ExpressionRoot.Builder builder = GandivaTypes.ExpressionRoot.newBuilder();
        builder.setRoot(root.toProtobuf());
        builder.setResultType(ArrowTypeHelper.ArrowFieldToProtobuf(result_field));
        return builder.build();
    }

    private TreeNode root;
    private Field result_field;
}
