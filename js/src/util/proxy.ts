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

type GetString<T> = (inst: T, key: string) => any;
type GetNumber<T> = (inst: T, key: number) => any;
type SetString<T> = (inst: T, key: string, val: any) => any;
type SetNumber<T> = (inst: T, key: number, val: any) => any;

/** @ignore */
export class IndexingProxyHandlerMixin<T extends object = any> implements ProxyHandler<T> {
    constructor(
        protected getNumberProp: GetNumber<T>,
        protected setNumberProp: SetNumber<T>,
        protected getStringProp: GetString<T>,
        protected setStringProp: SetString<T>
    ) { }

    get(target: any, key: any, instance: any) {
        if (typeof key === 'string') {
            let num = +key;
            if (num === num) {
                return this.getNumberProp(instance || target, num);
            }
            return this.getStringProp(instance || target, key);
        }
        return Reflect.get(target, key, instance);
    }
    set(target: any, key: any, value: any, instance: any) {
        if (typeof key === 'string') {
            let num = +key;
            if (num === num) {
                return (this.setNumberProp(instance || target, num, value), true);
            }
            return (this.setStringProp(instance || target, key, value), true);
        }
        return Reflect.set(target, key, value, instance);
    }
}

export class NumericIndexingProxyHandlerMixin<T extends object = any> implements ProxyHandler<T> {
    constructor(protected getNumberProp: GetNumber<T>, protected setNumberProp: SetNumber<T>) { }
    get(target: any, key: any, instance: any) {
        if (typeof key === 'string') {
            let num = +key;
            if (num === num) {
                return this.getNumberProp(instance || target, num);
            }
        }
        return Reflect.get(target, key, instance);
    }
    set(target: any, key: any, value: any, instance: any) {
        if (typeof key === 'string') {
            let num = +key;
            if (num === num) {
                return (this.setNumberProp(instance || target, num, value), true);
            }
        }
        return Reflect.set(target, key, value, instance);
    }
}
