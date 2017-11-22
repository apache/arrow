import { flatbuffers } from 'flatbuffers';

import * as Schema_ from './fb/Schema';
import * as Message_ from './fb/Message';

import Type = Schema_.org.apache.arrow.flatbuf.Type;
import Field = Schema_.org.apache.arrow.flatbuf.Field;
import FieldNode = Message_.org.apache.arrow.flatbuf.FieldNode;

export class Metadatum {
    constructor(private key_: string, private value_: any) {
    }

    key(): string {
        return this.key_;
    }

    value(): any {
        return this.value_;
    }
}

export class FieldBuilder {
    constructor(private name_: string, private typeType_: Type, private nullable_: boolean, private metadata_: Metadatum[]) {}
    name(): string {
        return this.name_;
    }
    typeType(): number {
        return this.typeType_;
    }
    nullable(): boolean {
        return this.nullable_;
    }
    customMetadataLength(): number {
        return this.metadata_.length;
    }
    customMetadata(i: number): Metadatum {
        return this.metadata_[i];
    }
    write(builder: flatbuffers.Builder): flatbuffers.Offset {
        Field.startField(builder);
        // TODO..
        return Field.endField(builder);
    }
}

export class FieldNodeBuilder {
    constructor(private length_: flatbuffers.Long, private nullCount_: flatbuffers.Long) {}
    length(): flatbuffers.Long {
        return this.length_;
    }
    nullCount(): flatbuffers.Long {
        return this.nullCount_;
    }
    write(builder: flatbuffers.Builder): flatbuffers.Offset {
        return FieldNode.createFieldNode(builder, this.length(), this.nullCount());
    }
}
