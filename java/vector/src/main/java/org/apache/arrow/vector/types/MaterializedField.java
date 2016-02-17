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
package org.apache.arrow.vector.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;

import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.util.BasicTypeHelper;


public class MaterializedField {
  private final String name;
  private final MajorType type;
  // use an ordered set as existing code relies on order (e,g. parquet writer)
  private final LinkedHashSet<MaterializedField> children;

  MaterializedField(String name, MajorType type, LinkedHashSet<MaterializedField> children) {
    this.name = name;
    this.type = type;
    this.children = children;
  }

  public Collection<MaterializedField> getChildren() {
    return new ArrayList<>(children);
  }

  public MaterializedField newWithChild(MaterializedField child) {
    MaterializedField newField = clone();
    newField.addChild(child);
    return newField;
  }

  public void addChild(MaterializedField field){
    children.add(field);
  }

  public MaterializedField clone() {
    return withPathAndType(name, getType());
  }

  public MaterializedField withType(MajorType type) {
    return withPathAndType(name, type);
  }

  public MaterializedField withPath(String name) {
    return withPathAndType(name, getType());
  }

  public MaterializedField withPathAndType(String name, final MajorType type) {
    final LinkedHashSet<MaterializedField> newChildren = new LinkedHashSet<>(children.size());
    for (final MaterializedField child:children) {
      newChildren.add(child.clone());
    }
    return new MaterializedField(name, type, newChildren);
  }

//  public String getLastName(){
//    PathSegment seg = key.path.getRootSegment();
//    while (seg.getChild() != null) {
//      seg = seg.getChild();
//    }
//    return seg.getNameSegment().getPath();
//  }


  // TODO: rewrite without as direct match rather than conversion then match.
//  public boolean matches(SerializedField booleanfield){
//    MaterializedField f = create(field);
//    return f.equals(this);
//  }

  public static MaterializedField create(String name, MajorType type){
    return new MaterializedField(name, type, new LinkedHashSet<MaterializedField>());
  }

//  public String getName(){
//    StringBuilder sb = new StringBuilder();
//    boolean first = true;
//    for(NamePart np : def.getNameList()){
//      if(np.getType() == Type.ARRAY){
//        sb.append("[]");
//      }else{
//        if(first){
//          first = false;
//        }else{
//          sb.append(".");
//        }
//        sb.append('`');
//        sb.append(np.getName());
//        sb.append('`');
//
//      }
//    }
//    return sb.toString();
//  }

  public String getPath() {
    return getName();
  }

  public String getLastName() {
    return getName();
  }

  public String getName() {
    return name;
  }

//  public int getWidth() {
//    return type.getWidth();
//  }

  public MajorType getType() {
    return type;
  }

  public int getScale() {
      return type.getScale();
  }
  public int getPrecision() {
      return type.getPrecision();
  }
  public boolean isNullable() {
    return type.getMode() == DataMode.OPTIONAL;
  }

  public DataMode getDataMode() {
    return type.getMode();
  }

  public MaterializedField getOtherNullableVersion(){
    MajorType mt = type;
    DataMode newDataMode = null;
    switch (mt.getMode()){
    case OPTIONAL:
      newDataMode = DataMode.REQUIRED;
      break;
    case REQUIRED:
      newDataMode = DataMode.OPTIONAL;
      break;
    default:
      throw new UnsupportedOperationException();
    }
    return new MaterializedField(name, new MajorType(mt.getMinorType(), newDataMode, mt.getPrecision(), mt.getScale(), mt.getTimezone(), mt.getSubTypes()), children);
  }

  public Class<?> getValueClass() {
    return BasicTypeHelper.getValueVectorClass(getType().getMinorType(), getDataMode());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.type, this.children);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MaterializedField other = (MaterializedField) obj;
    // DRILL-1872: Compute equals only on key. See also the comment
    // in MapVector$MapTransferPair

    return this.name.equalsIgnoreCase(other.name) &&
            Objects.equals(this.type, other.type);
  }


  @Override
  public String toString() {
    final int maxLen = 10;
    String childStr = children != null && !children.isEmpty() ? toString(children, maxLen) : "";
    return name + "(" + type.getMinorType().name() + ":" + type.getMode().name() + ")" + childStr;
  }


  private String toString(Collection<?> collection, int maxLen) {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    int i = 0;
    for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
      if (i > 0){
        builder.append(", ");
      }
      builder.append(iterator.next());
    }
    builder.append("]");
    return builder.toString();
  }
}
