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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

/**
 * POJO to define a function signature.
 */
public class FunctionSignature {
  private final String name;
  private final ArrowType returnType;
  private final List<ArrowType> paramTypes;

  public ArrowType getReturnType() {
    return returnType;
  }

  public List<ArrowType> getParamTypes() {
    return paramTypes;
  }

  public String getName() {
    return name;
  }

  /**
   * Ctor.
   * @param name - name of the function.
   * @param returnType - data type of return
   * @param paramTypes - data type of input args.
   */
  public FunctionSignature(String name, ArrowType returnType, List<ArrowType> paramTypes) {
    this.name = name;
    this.returnType = returnType;
    this.paramTypes = paramTypes;
  }

  /**
   * Override equals.
   * @param signature - signature to compare
   * @return true if equal and false if not.
   */
  public boolean equals(Object signature) {
    if (signature == null) {
      return false;
    }
    if (getClass() != signature.getClass()) {
      return false;
    }
    final FunctionSignature other = (FunctionSignature) signature;
    return Objects.equal(this.name, other.name)
            && Objects.equal(this.returnType, other.returnType)
            && Objects.equal(this.paramTypes, other.paramTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.name, this.returnType, this.paramTypes);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("name ", name)
            .add("return type ", returnType)
            .add("param types ", paramTypes)
            .toString();

  }


}
