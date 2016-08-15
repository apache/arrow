package org.apache.arrow.vector.layout;

import static java.util.Arrays.asList;
import static org.apache.arrow.flatbuf.Precision.DOUBLE;
import static org.apache.arrow.flatbuf.Precision.SINGLE;
import static org.apache.arrow.vector.layout.VectorLayout.newBooleanVectorLayout;
import static org.apache.arrow.vector.layout.VectorLayout.newByteVectorLayout;
import static org.apache.arrow.vector.layout.VectorLayout.newIntVectorLayout;
import static org.apache.arrow.vector.layout.VectorLayout.newOffsetVectorLayout;
import static org.apache.arrow.vector.layout.VectorLayout.newValidityVectorLayout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flatbuf.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeVisitor;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.IntervalDay;
import org.apache.arrow.vector.types.pojo.ArrowType.IntervalYear;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Tuple;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * The layout of vectors for a given type
 * It defines its own vectors followed by the vectors for the children
 * if it is a nested type (Tuple, List, Union)
 */
public class TypeLayout {

  public static TypeLayout newTypeLayout(Field field) {
    final org.apache.arrow.vector.types.pojo.ArrowType arrowType = field.getType();
    final List<Field> children = field.getChildren();
    TypeLayout layout = arrowType.accept(new ArrowTypeVisitor<TypeLayout>() {

      @Override public TypeLayout visit(Int type) {
        return new FixedWidthTypeLayout(
            arrowType,
            newIntVectorLayout(type.getBitWidth()));
      }

      @Override public TypeLayout visit(Union type) {
        List<TypeLayout> childLayouts = childrenLayout(children);
        List<VectorLayout> vectors;
        switch (type.getMode()) {
          case UnionMode.Dense:
            vectors = asList(
                // TODO: validate this
                newValidityVectorLayout(),
                newIntVectorLayout(8), // type vector
                newOffsetVectorLayout() // offset to find the vector
                );
            break;
          case UnionMode.Sparse:
            vectors = asList(
                newValidityVectorLayout(),
                newIntVectorLayout(8) // type vector
                );
            break;
          default:
            throw new UnsupportedOperationException("Unsupported Union Mode: " + type.getMode());
        }
        return new TypeLayout(arrowType, vectors, childLayouts);
      }

      @Override public TypeLayout visit(Tuple type) {
        List<TypeLayout> childLayouts = childrenLayout(children);
        List<VectorLayout> vectors = asList(
            newValidityVectorLayout()
            );
        return new TypeLayout(arrowType, vectors, childLayouts);
      }

      @Override public TypeLayout visit(Timestamp type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override public TypeLayout visit(org.apache.arrow.vector.types.pojo.ArrowType.List type) {
        if (children.size() != 1) {
          throw new IllegalArgumentException("Lists should have exactly one child. Found " + children);
        }
        List<TypeLayout> childLayouts = childrenLayout(children);
        List<VectorLayout> vectors = asList(
            newValidityVectorLayout()
            );
        return new TypeLayout(arrowType, vectors, childLayouts);
      }

      @Override public TypeLayout visit(FloatingPoint type) {
        int bitWidth;
        switch (type.getPrecision()) {
        case SINGLE:
          bitWidth = 32;
          break;
        case DOUBLE:
          bitWidth = 64;
          break;
        default:
          throw new UnsupportedOperationException("Unsupported Precision: " + type.getPrecision());
        }
        return new FixedWidthTypeLayout(
            arrowType,
            newIntVectorLayout(bitWidth));
      }

      @Override public TypeLayout visit(Decimal type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override public TypeLayout visit(Bool type) {
        return new FixedWidthTypeLayout(
            arrowType,
            newBooleanVectorLayout());
      }

      @Override public TypeLayout visit(Binary type) {
        return new VariableWidthTypeLayout(
            arrowType,
            newByteVectorLayout());
      }

      @Override public TypeLayout visit(Utf8 type) {
        return new VariableWidthTypeLayout(
            arrowType,
            newByteVectorLayout());
      }

      @Override
      public TypeLayout visit(Null type) {
        return new TypeLayout(type, Collections.<VectorLayout>emptyList(), Collections.<TypeLayout>emptyList());
      }

      @Override
      public TypeLayout visit(Date type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public TypeLayout visit(Time type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public TypeLayout visit(IntervalDay type) {
        throw new UnsupportedOperationException("NYI");
      }

      @Override
      public TypeLayout visit(IntervalYear type) {
        throw new UnsupportedOperationException("NYI");
      }

      private List<TypeLayout> childrenLayout(final List<Field> children) {
        List<TypeLayout> childLayouts = new ArrayList<TypeLayout>();
        for (Field child : children) {
          childLayouts.add(newTypeLayout(child));
        }
        return childLayouts;
      }
    });
    return layout;
  }

  private final ArrowType type;
  private final List<VectorLayout> vectors;
  private final List<TypeLayout> children;

  public TypeLayout(ArrowType type, List<VectorLayout> vectors, List<TypeLayout> children) {
    super();
    this.type = type;
    this.vectors = vectors;
    this.children = children;
  }

  public ArrowType getType() {
    return type;
  }

  public List<VectorLayout> getVectors() {
    return vectors;
  }

  public List<TypeLayout> getChildren() {
    return children;
  }
}
