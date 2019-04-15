import { DataType, Struct } from '../type';
import { NestedBuilder } from './base';

export class StructBuilder<T extends { [key: string]: DataType } = any, TNull = any> extends NestedBuilder<Struct<T>, TNull> {}
