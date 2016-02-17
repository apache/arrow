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

import java.util.Collection;

import com.google.common.base.Preconditions;

import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types.MajorType;

public class VectorDescriptor {
  private static final String DEFAULT_NAME = "NONE";

  private final MaterializedField field;

  public VectorDescriptor(final MajorType type) {
    this(DEFAULT_NAME, type);
  }

  public VectorDescriptor(final String name, final MajorType type) {
    this(MaterializedField.create(name, type));
  }

  public VectorDescriptor(final MaterializedField field) {
    this.field = Preconditions.checkNotNull(field, "field cannot be null");
  }

  public MaterializedField getField() {
    return field;
  }

  public MajorType getType() {
    return field.getType();
  }

  public String getName() {
    return field.getLastName();
  }

  public Collection<MaterializedField> getChildren() {
    return field.getChildren();
  }

  public boolean hasName() {
    return getName() != DEFAULT_NAME;
  }

  public VectorDescriptor withName(final String name) {
    return new VectorDescriptor(field.withPath(name));
  }

  public VectorDescriptor withType(final MajorType type) {
    return new VectorDescriptor(field.withType(type));
  }

  public static VectorDescriptor create(final String name, final MajorType type) {
    return new VectorDescriptor(name, type);
  }

  public static VectorDescriptor create(final MajorType type) {
    return new VectorDescriptor(type);
  }

  public static VectorDescriptor create(final MaterializedField field) {
    return new VectorDescriptor(field);
  }
}
