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

import java.util.Set;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ExpressionRegistryTest {

  @Test
  public void testTypes() throws GandivaException {
    Set<ArrowType> types = ExpressionRegistry.getInstance().getSupportedTypes();
    ArrowType.Int uint8 = new ArrowType.Int(8, false);
    Assert.assertTrue(types.contains(uint8));
  }

  @Test
  public void testFunctions() throws GandivaException {
    ArrowType.Int uint8 = new ArrowType.Int(8, false);
    FunctionSignature signature =
        new FunctionSignature("add", uint8, Lists.newArrayList(uint8, uint8));
    Set<FunctionSignature> functions = ExpressionRegistry.getInstance().getSupportedFunctions();
    Assert.assertTrue(functions.contains(signature));
  }
}
