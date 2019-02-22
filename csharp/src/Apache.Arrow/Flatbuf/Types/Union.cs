// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::FlatBuffers;

/// A union is a complex type with children in Field
/// By default ids in the type vector refer to the offsets in the children
/// optionally typeIds provides an indirection between the child offset and the type id
/// for each child typeIds[offset] is the id used in the type vector
internal struct Union : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static Union GetRootAsUnion(ByteBuffer _bb) { return GetRootAsUnion(_bb, new Union()); }
  public static Union GetRootAsUnion(ByteBuffer _bb, Union obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public Union __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public UnionMode Mode { get { int o = __p.__offset(4); return o != 0 ? (UnionMode)__p.bb.GetShort(o + __p.bb_pos) : UnionMode.Sparse; } }
  public int TypeIds(int j) { int o = __p.__offset(6); return o != 0 ? __p.bb.GetInt(__p.__vector(o) + j * 4) : (int)0; }
  public int TypeIdsLength { get { int o = __p.__offset(6); return o != 0 ? __p.__vector_len(o) : 0; } }
#if ENABLE_SPAN_T
  public Span<byte> GetTypeIdsBytes() { return __p.__vector_as_span(6); }
#else
  public ArraySegment<byte>? GetTypeIdsBytes() { return __p.__vector_as_arraysegment(6); }
#endif
  public int[] GetTypeIdsArray() { return __p.__vector_as_array<int>(6); }

  public static Offset<Union> CreateUnion(FlatBufferBuilder builder,
      UnionMode mode = UnionMode.Sparse,
      VectorOffset typeIdsOffset = default(VectorOffset)) {
    builder.StartObject(2);
    Union.AddTypeIds(builder, typeIdsOffset);
    Union.AddMode(builder, mode);
    return Union.EndUnion(builder);
  }

  public static void StartUnion(FlatBufferBuilder builder) { builder.StartObject(2); }
  public static void AddMode(FlatBufferBuilder builder, UnionMode mode) { builder.AddShort(0, (short)mode, 0); }
  public static void AddTypeIds(FlatBufferBuilder builder, VectorOffset typeIdsOffset) { builder.AddOffset(1, typeIdsOffset.Value, 0); }
  public static VectorOffset CreateTypeIdsVector(FlatBufferBuilder builder, int[] data) { builder.StartVector(4, data.Length, 4); for (int i = data.Length - 1; i >= 0; i--) builder.AddInt(data[i]); return builder.EndVector(); }
  public static VectorOffset CreateTypeIdsVectorBlock(FlatBufferBuilder builder, int[] data) { builder.StartVector(4, data.Length, 4); builder.Add(data); return builder.EndVector(); }
  public static void StartTypeIdsVector(FlatBufferBuilder builder, int numElems) { builder.StartVector(4, numElems, 4); }
  public static Offset<Union> EndUnion(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<Union>(o);
  }
};


}
