// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { SparseUnion, DenseUnion, Union } from '../type';
import { MakeDataVisitor, visitUnion, SparseUnionDataProps, DenseUnionDataProps, UnionDataProps } from '../visitor/data';

MakeDataVisitor.prototype.visitUnion = visitUnion;

declare module '../data' {
    // https://github.com/microsoft/TypeScript/issues/21566#issuecomment-362462824
    // @ts-ignore
    export function makeData<T extends SparseUnion>(props: SparseUnionDataProps<T>): Data<T>;
    // @ts-ignore
    export function makeData<T extends DenseUnion>(props: DenseUnionDataProps<T>): Data<T>;
    // @ts-ignore
    export function makeData<T extends Union>(props: UnionDataProps<T>): Data<T>;
}
