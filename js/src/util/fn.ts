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
export function partial0<T>(visit: (node: T) => any) {
    return function(this: T) { return visit(this); };
}

/** @ignore */
export function partial1<T>(visit: (node: T, a: any) => any) {
    return function(this: T, a: any) { return visit(this, a); };
}

/** @ignore */
export function partial2<T>(visit: (node: T, a: any, b: any) => any) {
    return function(this: T, a: any, b: any) { return visit(this, a, b); };
}
