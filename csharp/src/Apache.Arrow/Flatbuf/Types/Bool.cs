// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

internal struct Bool : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Bool GetRootAsBool(ByteBuffer _bb) { return GetRootAsBool(_bb, new Bool()); }
  public static Bool GetRootAsBool(ByteBuffer _bb, Bool obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Bool __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }


  public static void StartBool(FlatBufferBuilder builder) { builder.StartTable(0); }
  public static Offset<Bool> EndBool(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<Bool>(o);
  }
}


static internal class BoolVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
