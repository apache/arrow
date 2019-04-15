import { Field } from '../schema';
import { Builder, NestedBuilder, DataBuilderOptions } from './base';
import { Union, SparseUnion, DenseUnion } from '../type';

export class UnionBuilder<T extends Union, TNull = any> extends NestedBuilder<T, TNull> {
    constructor(options: DataBuilderOptions<T, TNull>) {
        super(options);
        this.typeIds = new Int8Array(0);
    }
    public get bytesReserved() {
        return this.children.reduce(
            (acc, { bytesReserved }) => acc + bytesReserved,
            this.typeIds.byteLength + this.nullBitmap.byteLength
        );
    }
    public write(value: any | TNull, childTypeId: number) {
        const offset = this.length;
        if (this.writeValid(this.isValid(value), offset)) {
            this.writeValue(value, offset, childTypeId);
        }
        this.length = offset + 1;
        return this;
    }
    public appendChild(child: Builder, name = `${this.children.length}`): number {
        const childIndex = this.children.push(child);
        const { type: { children, mode, typeIds } } = this;
        const fields = [...children, new Field(name, child.type)];
        this._type = new Union(mode, [...typeIds, childIndex], fields) as T;
        return childIndex;
    }
    public writeValue(value: any, offset: number, typeId: number) {
        this._getTypeIds(offset)[offset] = typeId;
        return super.writeValue(value, offset);
    }
    protected _updateBytesUsed(offset: number, length: number) {
        this._bytesUsed += 1;
        return super._updateBytesUsed(offset, length);
    }
}

export class SparseUnionBuilder<T extends SparseUnion, TNull = any> extends UnionBuilder<T, TNull> {}

export class DenseUnionBuilder<T extends DenseUnion, TNull = any> extends UnionBuilder<T, TNull> {
    constructor(options: DataBuilderOptions<T, TNull>) {
        super(options);
        this.valueOffsets = new Int32Array(0);
    }
    public writeValue(value: any, offset: number, childTypeId: number) {
        const valueOffsets = this._getValueOffsets(offset);
        valueOffsets[offset] = this.getChildAt(childTypeId)!.length;
        return super.writeValue(value, offset, childTypeId);
    }
    protected _updateBytesUsed(offset: number, length: number) {
        this._bytesUsed += 4;
        return super._updateBytesUsed(offset, length);
    }
}
