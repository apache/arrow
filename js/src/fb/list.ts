// automatically generated by the FlatBuffers compiler, do not modify

import * as flatbuffers from 'flatbuffers';

export class List {
  bb: flatbuffers.ByteBuffer|null = null;
  bb_pos = 0;
__init(i:number, bb:flatbuffers.ByteBuffer):List {
  this.bb_pos = i;
  this.bb = bb;
  return this;
}

static getRootAsList(bb:flatbuffers.ByteBuffer, obj?:List):List {
  return (obj || new List()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static getSizePrefixedRootAsList(bb:flatbuffers.ByteBuffer, obj?:List):List {
  bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
  return (obj || new List()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
}

static startList(builder:flatbuffers.Builder) {
  builder.startObject(0);
}

static endList(builder:flatbuffers.Builder):flatbuffers.Offset {
  const offset = builder.endObject();
  return offset;
}

static createList(builder:flatbuffers.Builder):flatbuffers.Offset {
  List.startList(builder);
  return List.endList(builder);
}
}
