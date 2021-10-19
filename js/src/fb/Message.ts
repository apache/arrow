// automatically generated by the FlatBuffers compiler, do not modify

import { flatbuffers } from 'flatbuffers';
import * as NS2587435351494372344 from './Schema';
/**
 * @enum {number}
 */
export enum CompressionType {
    LZ4_FRAME = 0,
    ZSTD = 1
}

/**
 * Provided for forward compatibility in case we need to support different
 * strategies for compressing the IPC message body (like whole-body
 * compression rather than buffer-level) in the future
 *
 * @enum {number}
 */
export enum BodyCompressionMethod {
    /**
     * Each constituent buffer is first compressed with the indicated
     * compressor, and then written with the uncompressed length in the first 8
     * bytes as a 64-bit little-endian signed integer followed by the compressed
     * buffer bytes (and then padding as required by the protocol). The
     * uncompressed length may be set to -1 to indicate that the data that
     * follows is not compressed, which can be useful for cases where
     * compression does not yield appreciable savings.
     */
    BUFFER = 0
}

/**
 * ----------------------------------------------------------------------
 * The root Message type
 * This union enables us to easily send different message types without
 * redundant storage, and in the future we can easily add new message types.
 *
 * Arrow implementations do not need to implement all of the message types,
 * which may include experimental metadata types. For maximum compatibility,
 * it is best to send data using RecordBatch
 *
 * @enum {number}
 */
export enum MessageHeader {
    NONE = 0,
    Schema = 1,
    DictionaryBatch = 2,
    RecordBatch = 3,
    Tensor = 4,
    SparseTensor = 5
}

/**
 * ----------------------------------------------------------------------
 * Data structures for describing a table row batch (a collection of
 * equal-length Arrow arrays)
 * Metadata about a field at some level of a nested type tree (but not
 * its children).
 *
 * For example, a List<Int16> with values `[[1, 2, 3], null, [4], [5, 6], null]`
 * would have {length: 5, null_count: 2} for its List node, and {length: 6,
 * null_count: 0} for its Int16 node, as separate FieldNode structs
 *
 * @constructor
 */
export class FieldNode {
    bb: flatbuffers.ByteBuffer | null = null;

    bb_pos: number = 0;
    /**
     * @param number i
     * @param flatbuffers.ByteBuffer bb
     * @returns FieldNode
     */
    __init(i: number, bb: flatbuffers.ByteBuffer): FieldNode {
        this.bb_pos = i;
        this.bb = bb;
        return this;
    }

    /**
     * The number of value slots in the Arrow array at this level of a nested
     * tree
     *
     * @returns flatbuffers.Long
     */
    length(): flatbuffers.Long {
        return this.bb!.readInt64(this.bb_pos);
    }

    /**
     * The number of observed nulls. Fields with null_count == 0 may choose not
     * to write their physical validity bitmap out as a materialized buffer,
     * instead setting the length of the bitmap buffer to 0.
     *
     * @returns flatbuffers.Long
     */
    nullCount(): flatbuffers.Long {
        return this.bb!.readInt64(this.bb_pos + 8);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Long length
     * @param flatbuffers.Long null_count
     * @returns flatbuffers.Offset
     */
    static createFieldNode(builder: flatbuffers.Builder, length: flatbuffers.Long, null_count: flatbuffers.Long): flatbuffers.Offset {
        builder.prep(8, 16);
        builder.writeInt64(null_count);
        builder.writeInt64(length);
        return builder.offset();
    }

}
/**
 * Optional compression for the memory buffers constituting IPC message
 * bodies. Intended for use with RecordBatch but could be used for other
 * message types
 *
 * @constructor
 */
export class BodyCompression {
    bb: flatbuffers.ByteBuffer | null = null;

    bb_pos: number = 0;
    /**
     * @param number i
     * @param flatbuffers.ByteBuffer bb
     * @returns BodyCompression
     */
    __init(i: number, bb: flatbuffers.ByteBuffer): BodyCompression {
        this.bb_pos = i;
        this.bb = bb;
        return this;
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param BodyCompression= obj
     * @returns BodyCompression
     */
    static getRootAsBodyCompression(bb: flatbuffers.ByteBuffer, obj?: BodyCompression): BodyCompression {
        return (obj || new BodyCompression()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param BodyCompression= obj
     * @returns BodyCompression
     */
    static getSizePrefixedRootAsBodyCompression(bb: flatbuffers.ByteBuffer, obj?: BodyCompression): BodyCompression {
        bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
        return (obj || new BodyCompression()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * Compressor library
     *
     * @returns CompressionType
     */
    codec(): CompressionType {
        const offset = this.bb!.__offset(this.bb_pos, 4);
        return offset ? /**  */ (this.bb!.readInt8(this.bb_pos + offset)) : CompressionType.LZ4_FRAME;
    }

    /**
     * Indicates the way the record batch body was compressed
     *
     * @returns BodyCompressionMethod
     */
    method(): BodyCompressionMethod {
        const offset = this.bb!.__offset(this.bb_pos, 6);
        return offset ? /**  */ (this.bb!.readInt8(this.bb_pos + offset)) : BodyCompressionMethod.BUFFER;
    }

    /**
     * @param flatbuffers.Builder builder
     */
    static startBodyCompression(builder: flatbuffers.Builder) {
        builder.startObject(2);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param CompressionType codec
     */
    static addCodec(builder: flatbuffers.Builder, codec: CompressionType) {
        builder.addFieldInt8(0, codec, CompressionType.LZ4_FRAME);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param BodyCompressionMethod method
     */
    static addMethod(builder: flatbuffers.Builder, method: BodyCompressionMethod) {
        builder.addFieldInt8(1, method, BodyCompressionMethod.BUFFER);
    }

    /**
     * @param flatbuffers.Builder builder
     * @returns flatbuffers.Offset
     */
    static endBodyCompression(builder: flatbuffers.Builder): flatbuffers.Offset {
        const offset = builder.endObject();
        return offset;
    }

    static createBodyCompression(builder: flatbuffers.Builder, codec: CompressionType, method: BodyCompressionMethod): flatbuffers.Offset {
        BodyCompression.startBodyCompression(builder);
        BodyCompression.addCodec(builder, codec);
        BodyCompression.addMethod(builder, method);
        return BodyCompression.endBodyCompression(builder);
    }
}
/**
 * A data header describing the shared memory layout of a "record" or "row"
 * batch. Some systems call this a "row batch" internally and others a "record
 * batch".
 *
 * @constructor
 */
export class RecordBatch {
    bb: flatbuffers.ByteBuffer | null = null;

    bb_pos: number = 0;
    /**
     * @param number i
     * @param flatbuffers.ByteBuffer bb
     * @returns RecordBatch
     */
    __init(i: number, bb: flatbuffers.ByteBuffer): RecordBatch {
        this.bb_pos = i;
        this.bb = bb;
        return this;
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param RecordBatch= obj
     * @returns RecordBatch
     */
    static getRootAsRecordBatch(bb: flatbuffers.ByteBuffer, obj?: RecordBatch): RecordBatch {
        return (obj || new RecordBatch()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param RecordBatch= obj
     * @returns RecordBatch
     */
    static getSizePrefixedRootAsRecordBatch(bb: flatbuffers.ByteBuffer, obj?: RecordBatch): RecordBatch {
        bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
        return (obj || new RecordBatch()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * number of records / rows. The arrays in the batch should all have this
     * length
     *
     * @returns flatbuffers.Long
     */
    length(): flatbuffers.Long {
        const offset = this.bb!.__offset(this.bb_pos, 4);
        return offset ? this.bb!.readInt64(this.bb_pos + offset) : this.bb!.createLong(0, 0);
    }

    /**
     * Nodes correspond to the pre-ordered flattened logical schema
     *
     * @param number index
     * @param FieldNode= obj
     * @returns FieldNode
     */
    nodes(index: number, obj?: FieldNode): FieldNode | null {
        const offset = this.bb!.__offset(this.bb_pos, 6);
        return offset ? (obj || new FieldNode()).__init(this.bb!.__vector(this.bb_pos + offset) + index * 16, this.bb!) : null;
    }

    /**
     * @returns number
     */
    nodesLength(): number {
        const offset = this.bb!.__offset(this.bb_pos, 6);
        return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
    }

    /**
     * Buffers correspond to the pre-ordered flattened buffer tree
     *
     * The number of buffers appended to this list depends on the schema. For
     * example, most primitive arrays will have 2 buffers, 1 for the validity
     * bitmap and 1 for the values. For struct arrays, there will only be a
     * single buffer for the validity (nulls) bitmap
     *
     * @param number index
     * @param Buffer= obj
     * @returns Buffer
     */
    buffers(index: number, obj?: NS2587435351494372344.Buffer): NS2587435351494372344.Buffer | null {
        const offset = this.bb!.__offset(this.bb_pos, 8);
        return offset ? (obj || new NS2587435351494372344.Buffer()).__init(this.bb!.__vector(this.bb_pos + offset) + index * 16, this.bb!) : null;
    }

    /**
     * @returns number
     */
    buffersLength(): number {
        const offset = this.bb!.__offset(this.bb_pos, 8);
        return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
    }

    /**
     * Optional compression of the message body
     *
     * @param BodyCompression= obj
     * @returns BodyCompression|null
     */
    compression(obj?: BodyCompression): BodyCompression | null {
        const offset = this.bb!.__offset(this.bb_pos, 10);
        return offset ? (obj || new BodyCompression()).__init(this.bb!.__indirect(this.bb_pos + offset), this.bb!) : null;
    }

    /**
     * @param flatbuffers.Builder builder
     */
    static startRecordBatch(builder: flatbuffers.Builder) {
        builder.startObject(4);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Long length
     */
    static addLength(builder: flatbuffers.Builder, length: flatbuffers.Long) {
        builder.addFieldInt64(0, length, builder.createLong(0, 0));
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset nodesOffset
     */
    static addNodes(builder: flatbuffers.Builder, nodesOffset: flatbuffers.Offset) {
        builder.addFieldOffset(1, nodesOffset, 0);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param number numElems
     */
    static startNodesVector(builder: flatbuffers.Builder, numElems: number) {
        builder.startVector(16, numElems, 8);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset buffersOffset
     */
    static addBuffers(builder: flatbuffers.Builder, buffersOffset: flatbuffers.Offset) {
        builder.addFieldOffset(2, buffersOffset, 0);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param number numElems
     */
    static startBuffersVector(builder: flatbuffers.Builder, numElems: number) {
        builder.startVector(16, numElems, 8);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset compressionOffset
     */
    static addCompression(builder: flatbuffers.Builder, compressionOffset: flatbuffers.Offset) {
        builder.addFieldOffset(3, compressionOffset, 0);
    }

    /**
     * @param flatbuffers.Builder builder
     * @returns flatbuffers.Offset
     */
    static endRecordBatch(builder: flatbuffers.Builder): flatbuffers.Offset {
        const offset = builder.endObject();
        return offset;
    }

    static createRecordBatch(builder: flatbuffers.Builder, length: flatbuffers.Long, nodesOffset: flatbuffers.Offset, buffersOffset: flatbuffers.Offset, compressionOffset: flatbuffers.Offset): flatbuffers.Offset {
        RecordBatch.startRecordBatch(builder);
        RecordBatch.addLength(builder, length);
        RecordBatch.addNodes(builder, nodesOffset);
        RecordBatch.addBuffers(builder, buffersOffset);
        RecordBatch.addCompression(builder, compressionOffset);
        return RecordBatch.endRecordBatch(builder);
    }
}
/**
 * For sending dictionary encoding information. Any Field can be
 * dictionary-encoded, but in this case none of its children may be
 * dictionary-encoded.
 * There is one vector / column per dictionary, but that vector / column
 * may be spread across multiple dictionary batches by using the isDelta
 * flag
 *
 * @constructor
 */
export class DictionaryBatch {
    bb: flatbuffers.ByteBuffer | null = null;

    bb_pos: number = 0;
    /**
     * @param number i
     * @param flatbuffers.ByteBuffer bb
     * @returns DictionaryBatch
     */
    __init(i: number, bb: flatbuffers.ByteBuffer): DictionaryBatch {
        this.bb_pos = i;
        this.bb = bb;
        return this;
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param DictionaryBatch= obj
     * @returns DictionaryBatch
     */
    static getRootAsDictionaryBatch(bb: flatbuffers.ByteBuffer, obj?: DictionaryBatch): DictionaryBatch {
        return (obj || new DictionaryBatch()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param DictionaryBatch= obj
     * @returns DictionaryBatch
     */
    static getSizePrefixedRootAsDictionaryBatch(bb: flatbuffers.ByteBuffer, obj?: DictionaryBatch): DictionaryBatch {
        bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
        return (obj || new DictionaryBatch()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * @returns flatbuffers.Long
     */
    id(): flatbuffers.Long {
        const offset = this.bb!.__offset(this.bb_pos, 4);
        return offset ? this.bb!.readInt64(this.bb_pos + offset) : this.bb!.createLong(0, 0);
    }

    /**
     * @param RecordBatch= obj
     * @returns RecordBatch|null
     */
    data(obj?: RecordBatch): RecordBatch | null {
        const offset = this.bb!.__offset(this.bb_pos, 6);
        return offset ? (obj || new RecordBatch()).__init(this.bb!.__indirect(this.bb_pos + offset), this.bb!) : null;
    }

    /**
     * If isDelta is true the values in the dictionary are to be appended to a
     * dictionary with the indicated id. If isDelta is false this dictionary
     * should replace the existing dictionary.
     *
     * @returns boolean
     */
    isDelta(): boolean {
        const offset = this.bb!.__offset(this.bb_pos, 8);
        return offset ? !!this.bb!.readInt8(this.bb_pos + offset) : false;
    }

    /**
     * @param flatbuffers.Builder builder
     */
    static startDictionaryBatch(builder: flatbuffers.Builder) {
        builder.startObject(3);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Long id
     */
    static addId(builder: flatbuffers.Builder, id: flatbuffers.Long) {
        builder.addFieldInt64(0, id, builder.createLong(0, 0));
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset dataOffset
     */
    static addData(builder: flatbuffers.Builder, dataOffset: flatbuffers.Offset) {
        builder.addFieldOffset(1, dataOffset, 0);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param boolean isDelta
     */
    static addIsDelta(builder: flatbuffers.Builder, isDelta: boolean) {
        builder.addFieldInt8(2, +isDelta, +false);
    }

    /**
     * @param flatbuffers.Builder builder
     * @returns flatbuffers.Offset
     */
    static endDictionaryBatch(builder: flatbuffers.Builder): flatbuffers.Offset {
        const offset = builder.endObject();
        return offset;
    }

    static createDictionaryBatch(builder: flatbuffers.Builder, id: flatbuffers.Long, dataOffset: flatbuffers.Offset, isDelta: boolean): flatbuffers.Offset {
        DictionaryBatch.startDictionaryBatch(builder);
        DictionaryBatch.addId(builder, id);
        DictionaryBatch.addData(builder, dataOffset);
        DictionaryBatch.addIsDelta(builder, isDelta);
        return DictionaryBatch.endDictionaryBatch(builder);
    }
}
/**
 * @constructor
 */
export class Message {
    bb: flatbuffers.ByteBuffer | null = null;

    bb_pos: number = 0;
    /**
     * @param number i
     * @param flatbuffers.ByteBuffer bb
     * @returns Message
     */
    __init(i: number, bb: flatbuffers.ByteBuffer): Message {
        this.bb_pos = i;
        this.bb = bb;
        return this;
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param Message= obj
     * @returns Message
     */
    static getRootAsMessage(bb: flatbuffers.ByteBuffer, obj?: Message): Message {
        return (obj || new Message()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * @param flatbuffers.ByteBuffer bb
     * @param Message= obj
     * @returns Message
     */
    static getSizePrefixedRootAsMessage(bb: flatbuffers.ByteBuffer, obj?: Message): Message {
        bb.setPosition(bb.position() + flatbuffers.SIZE_PREFIX_LENGTH);
        return (obj || new Message()).__init(bb.readInt32(bb.position()) + bb.position(), bb);
    }

    /**
     * @returns MetadataVersion
     */
    version(): NS2587435351494372344.MetadataVersion {
        const offset = this.bb!.__offset(this.bb_pos, 4);
        return offset ? /**  */ (this.bb!.readInt16(this.bb_pos + offset)) : NS2587435351494372344.MetadataVersion.V1;
    }

    /**
     * @returns MessageHeader
     */
    headerType(): MessageHeader {
        const offset = this.bb!.__offset(this.bb_pos, 6);
        return offset ? /**  */ (this.bb!.readUint8(this.bb_pos + offset)) : MessageHeader.NONE;
    }

    /**
     * @param flatbuffers.Table obj
     * @returns ?flatbuffers.Table
     */
    header<T extends flatbuffers.Table>(obj: T): T | null {
        const offset = this.bb!.__offset(this.bb_pos, 8);
        return offset ? this.bb!.__union(obj, this.bb_pos + offset) : null;
    }

    /**
     * @returns flatbuffers.Long
     */
    bodyLength(): flatbuffers.Long {
        const offset = this.bb!.__offset(this.bb_pos, 10);
        return offset ? this.bb!.readInt64(this.bb_pos + offset) : this.bb!.createLong(0, 0);
    }

    /**
     * @param number index
     * @param KeyValue= obj
     * @returns KeyValue
     */
    customMetadata(index: number, obj?: NS2587435351494372344.KeyValue): NS2587435351494372344.KeyValue | null {
        const offset = this.bb!.__offset(this.bb_pos, 12);
        return offset ? (obj || new NS2587435351494372344.KeyValue()).__init(this.bb!.__indirect(this.bb!.__vector(this.bb_pos + offset) + index * 4), this.bb!) : null;
    }

    /**
     * @returns number
     */
    customMetadataLength(): number {
        const offset = this.bb!.__offset(this.bb_pos, 12);
        return offset ? this.bb!.__vector_len(this.bb_pos + offset) : 0;
    }

    /**
     * @param flatbuffers.Builder builder
     */
    static startMessage(builder: flatbuffers.Builder) {
        builder.startObject(5);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param MetadataVersion version
     */
    static addVersion(builder: flatbuffers.Builder, version: NS2587435351494372344.MetadataVersion) {
        builder.addFieldInt16(0, version, NS2587435351494372344.MetadataVersion.V1);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param MessageHeader headerType
     */
    static addHeaderType(builder: flatbuffers.Builder, headerType: MessageHeader) {
        builder.addFieldInt8(1, headerType, MessageHeader.NONE);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset headerOffset
     */
    static addHeader(builder: flatbuffers.Builder, headerOffset: flatbuffers.Offset) {
        builder.addFieldOffset(2, headerOffset, 0);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Long bodyLength
     */
    static addBodyLength(builder: flatbuffers.Builder, bodyLength: flatbuffers.Long) {
        builder.addFieldInt64(3, bodyLength, builder.createLong(0, 0));
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset customMetadataOffset
     */
    static addCustomMetadata(builder: flatbuffers.Builder, customMetadataOffset: flatbuffers.Offset) {
        builder.addFieldOffset(4, customMetadataOffset, 0);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param Array.<flatbuffers.Offset> data
     * @returns flatbuffers.Offset
     */
    static createCustomMetadataVector(builder: flatbuffers.Builder, data: flatbuffers.Offset[]): flatbuffers.Offset {
        builder.startVector(4, data.length, 4);
        for (let i = data.length - 1; i >= 0; i--) {
            builder.addOffset(data[i]);
        }
        return builder.endVector();
    }

    /**
     * @param flatbuffers.Builder builder
     * @param number numElems
     */
    static startCustomMetadataVector(builder: flatbuffers.Builder, numElems: number) {
        builder.startVector(4, numElems, 4);
    }

    /**
     * @param flatbuffers.Builder builder
     * @returns flatbuffers.Offset
     */
    static endMessage(builder: flatbuffers.Builder): flatbuffers.Offset {
        const offset = builder.endObject();
        return offset;
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset offset
     */
    static finishMessageBuffer(builder: flatbuffers.Builder, offset: flatbuffers.Offset) {
        builder.finish(offset);
    }

    /**
     * @param flatbuffers.Builder builder
     * @param flatbuffers.Offset offset
     */
    static finishSizePrefixedMessageBuffer(builder: flatbuffers.Builder, offset: flatbuffers.Offset) {
        builder.finish(offset, undefined, true);
    }

    static createMessage(builder: flatbuffers.Builder, version: NS2587435351494372344.MetadataVersion, headerType: MessageHeader, headerOffset: flatbuffers.Offset, bodyLength: flatbuffers.Long, customMetadataOffset: flatbuffers.Offset): flatbuffers.Offset {
        Message.startMessage(builder);
        Message.addVersion(builder, version);
        Message.addHeaderType(builder, headerType);
        Message.addHeader(builder, headerOffset);
        Message.addBodyLength(builder, bodyLength);
        Message.addCustomMetadata(builder, customMetadataOffset);
        return Message.endMessage(builder);
    }
}
