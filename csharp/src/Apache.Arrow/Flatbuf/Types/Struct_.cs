// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

/// A Struct_ in the flatbuffer metadata is the same as an Arrow Struct
/// (according to the physical memory layout). We used Struct_ here as
/// Struct is a reserved word in Flatbuffers
internal struct Struct_ : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Struct_ GetRootAsStruct_(ByteBuffer _bb) { return GetRootAsStruct_(_bb, new Struct_()); }
  public static Struct_ GetRootAsStruct_(ByteBuffer _bb, Struct_ obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Struct_ __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }


  public static void StartStruct_(FlatBufferBuilder builder) { builder.StartTable(0); }
  public static Offset<Struct_> EndStruct_(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<Struct_>(o);
  }
}


static internal class Struct_Verify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
