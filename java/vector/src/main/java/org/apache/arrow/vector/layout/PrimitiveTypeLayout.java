package org.apache.arrow.vector.layout;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType;


public class PrimitiveTypeLayout extends TypeLayout {

  public PrimitiveTypeLayout(ArrowType type, List<VectorLayout> vectors) {
    super(type, vectors, Collections.<TypeLayout>emptyList());
  }

  public PrimitiveTypeLayout(ArrowType type, VectorLayout... vectors) {
    this(type, asList(vectors));
  }

}
