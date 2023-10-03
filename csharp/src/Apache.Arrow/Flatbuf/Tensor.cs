// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

internal struct Tensor : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Tensor GetRootAsTensor(ByteBuffer _bb) { return GetRootAsTensor(_bb, new Tensor()); }
  public static Tensor GetRootAsTensor(ByteBuffer _bb, Tensor obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public static bool VerifyTensor(ByteBuffer _bb) {Google.FlatBuffers.Verifier verifier = new Google.FlatBuffers.Verifier(_bb); return verifier.VerifyBuffer("", false, TensorVerify.Verify); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Tensor __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public Type TypeType { get { int o = __p.__offset(4); return o != 0 ? (Type)__p.bb.Get(o + __p.bb_pos) : Apache.Arrow.Flatbuf.Type.NONE; } }
  /// The type of data contained in a value cell. Currently only fixed-width
  /// value types are supported, no strings or nested types
  public TTable? Type<TTable>() where TTable : struct, IFlatbufferObject { int o = __p.__offset(6); return o != 0 ? (TTable?)__p.__union<TTable>(o + __p.bb_pos) : null; }
  public Null TypeAsNull() { return Type<Null>().Value; }
  public Int TypeAsInt() { return Type<Int>().Value; }
  public FloatingPoint TypeAsFloatingPoint() { return Type<FloatingPoint>().Value; }
  public Binary TypeAsBinary() { return Type<Binary>().Value; }
  public Utf8 TypeAsUtf8() { return Type<Utf8>().Value; }
  public Bool TypeAsBool() { return Type<Bool>().Value; }
  public Decimal TypeAsDecimal() { return Type<Decimal>().Value; }
  public Date TypeAsDate() { return Type<Date>().Value; }
  public Time TypeAsTime() { return Type<Time>().Value; }
  public Timestamp TypeAsTimestamp() { return Type<Timestamp>().Value; }
  public Interval TypeAsInterval() { return Type<Interval>().Value; }
  public List TypeAsList() { return Type<List>().Value; }
  public Struct_ TypeAsStruct_() { return Type<Struct_>().Value; }
  public Union TypeAsUnion() { return Type<Union>().Value; }
  public FixedSizeBinary TypeAsFixedSizeBinary() { return Type<FixedSizeBinary>().Value; }
  public FixedSizeList TypeAsFixedSizeList() { return Type<FixedSizeList>().Value; }
  public Map TypeAsMap() { return Type<Map>().Value; }
  public Duration TypeAsDuration() { return Type<Duration>().Value; }
  public LargeBinary TypeAsLargeBinary() { return Type<LargeBinary>().Value; }
  public LargeUtf8 TypeAsLargeUtf8() { return Type<LargeUtf8>().Value; }
  public LargeList TypeAsLargeList() { return Type<LargeList>().Value; }
  public RunEndEncoded TypeAsRunEndEncoded() { return Type<RunEndEncoded>().Value; }
  /// The dimensions of the tensor, optionally named
  public TensorDim? Shape(int j) { int o = __p.__offset(8); return o != 0 ? (TensorDim?)(new TensorDim()).__assign(__p.__indirect(__p.__vector(o) + j * 4), __p.bb) : null; }
  public int ShapeLength { get { int o = __p.__offset(8); return o != 0 ? __p.__vector_len(o) : 0; } }
  /// Non-negative byte offsets to advance one value cell along each dimension
  /// If omitted, default to row-major order (C-like).
  public long Strides(int j) { int o = __p.__offset(10); return o != 0 ? __p.bb.GetLong(__p.__vector(o) + j * 8) : (long)0; }
  public int StridesLength { get { int o = __p.__offset(10); return o != 0 ? __p.__vector_len(o) : 0; } }
#if ENABLE_SPAN_T
  public Span<long> GetStridesBytes() { return __p.__vector_as_span<long>(10, 8); }
#else
  public ArraySegment<byte>? GetStridesBytes() { return __p.__vector_as_arraysegment(10); }
#endif
  public long[] GetStridesArray() { return __p.__vector_as_array<long>(10); }
  /// The location and size of the tensor's data
  public Buffer? Data { get { int o = __p.__offset(12); return o != 0 ? (Buffer?)(new Buffer()).__assign(o + __p.bb_pos, __p.bb) : null; } }

  public static void StartTensor(FlatBufferBuilder builder) { builder.StartTable(5); }
  public static void AddTypeType(FlatBufferBuilder builder, Type typeType) { builder.AddByte(0, (byte)typeType, 0); }
  public static void AddType(FlatBufferBuilder builder, int typeOffset) { builder.AddOffset(1, typeOffset, 0); }
  public static void AddShape(FlatBufferBuilder builder, VectorOffset shapeOffset) { builder.AddOffset(2, shapeOffset.Value, 0); }
  public static VectorOffset CreateShapeVector(FlatBufferBuilder builder, Offset<TensorDim>[] data) { builder.StartVector(4, data.Length, 4); for (int i = data.Length - 1; i >= 0; i--) builder.AddOffset(data[i].Value); return builder.EndVector(); }
  public static VectorOffset CreateShapeVectorBlock(FlatBufferBuilder builder, Offset<TensorDim>[] data) { builder.StartVector(4, data.Length, 4); builder.Add(data); return builder.EndVector(); }
  public static VectorOffset CreateShapeVectorBlock(FlatBufferBuilder builder, ArraySegment<Offset<TensorDim>> data) { builder.StartVector(4, data.Count, 4); builder.Add(data); return builder.EndVector(); }
  public static VectorOffset CreateShapeVectorBlock(FlatBufferBuilder builder, IntPtr dataPtr, int sizeInBytes) { builder.StartVector(1, sizeInBytes, 1); builder.Add<Offset<TensorDim>>(dataPtr, sizeInBytes); return builder.EndVector(); }
  public static void StartShapeVector(FlatBufferBuilder builder, int numElems) { builder.StartVector(4, numElems, 4); }
  public static void AddStrides(FlatBufferBuilder builder, VectorOffset stridesOffset) { builder.AddOffset(3, stridesOffset.Value, 0); }
  public static VectorOffset CreateStridesVector(FlatBufferBuilder builder, long[] data) { builder.StartVector(8, data.Length, 8); for (int i = data.Length - 1; i >= 0; i--) builder.AddLong(data[i]); return builder.EndVector(); }
  public static VectorOffset CreateStridesVectorBlock(FlatBufferBuilder builder, long[] data) { builder.StartVector(8, data.Length, 8); builder.Add(data); return builder.EndVector(); }
  public static VectorOffset CreateStridesVectorBlock(FlatBufferBuilder builder, ArraySegment<long> data) { builder.StartVector(8, data.Count, 8); builder.Add(data); return builder.EndVector(); }
  public static VectorOffset CreateStridesVectorBlock(FlatBufferBuilder builder, IntPtr dataPtr, int sizeInBytes) { builder.StartVector(1, sizeInBytes, 1); builder.Add<long>(dataPtr, sizeInBytes); return builder.EndVector(); }
  public static void StartStridesVector(FlatBufferBuilder builder, int numElems) { builder.StartVector(8, numElems, 8); }
  public static void AddData(FlatBufferBuilder builder, Offset<Buffer> dataOffset) { builder.AddStruct(4, dataOffset.Value, 0); }
  public static Offset<Tensor> EndTensor(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    builder.Required(o, 6);  // type
    builder.Required(o, 8);  // shape
    builder.Required(o, 12);  // data
    return new Offset<Tensor>(o);
  }
  public static void FinishTensorBuffer(FlatBufferBuilder builder, Offset<Tensor> offset) { builder.Finish(offset.Value); }
  public static void FinishSizePrefixedTensorBuffer(FlatBufferBuilder builder, Offset<Tensor> offset) { builder.FinishSizePrefixed(offset.Value); }
}


static internal class TensorVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyField(tablePos, 4 /*TypeType*/, 1 /*Type*/, 1, false)
      && verifier.VerifyUnion(tablePos, 4, 6 /*Type*/, TypeVerify.Verify, true)
      && verifier.VerifyVectorOfTables(tablePos, 8 /*Shape*/, TensorDimVerify.Verify, true)
      && verifier.VerifyVectorOfData(tablePos, 10 /*Strides*/, 8 /*long*/, false)
      && verifier.VerifyField(tablePos, 12 /*Data*/, 16 /*Buffer*/, 8, true)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
