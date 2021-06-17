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

/** @ignore */
export class IndexingProxyHandlerMixin implements ProxyHandler<any> {
    get(target: any, key: any, instance: any) {
        let p = key;
        switch (typeof key) {
            // @ts-ignore
            // fall through if key can be cast to a number
            case 'string': if ((p = +key) !== p) { break; }
            // eslint-disable-next-line no-fallthrough
            case 'number': return instance.get(p);
        }
        return Reflect.get(target, key, instance);
    }
    set(target: any, key: any, value: any, instance: any) {
        let p = key;
        switch (typeof key) {
            // @ts-ignore
            // fall through if key can be cast to a number
            case 'string': if ((p = +key) !== p) { break; }
            // eslint-disable-next-line no-fallthrough
            case 'number': return (instance.set(p, value), true);
        }
        return Reflect.set(target, key, value, instance);
    }
}
