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

import { tableFromIPC } from 'apache-arrow';

const bytes = Uint8Array.from([65, 82, 82, 79, 87, 49, 0, 0, 255, 255, 255, 255, 120, 0, 0, 0, 16, 0, 0, 0, 0, 0, 10, 0, 12, 0, 6, 0, 5, 0, 8, 0, 10, 0, 0, 0, 0, 1, 4, 0, 12, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 16, 0, 20, 0, 8, 0, 0, 0, 7, 0, 12, 0, 0, 0, 16, 0, 16, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 28, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 49, 0, 0, 0, 8, 0, 12, 0, 8, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 1, 32, 0, 0, 0, 255, 255, 255, 255, 136, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 12, 0, 22, 0, 6, 0, 5, 0, 8, 0, 12, 0, 12, 0, 0, 0, 0, 3, 4, 0, 24, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 24, 0, 12, 0, 4, 0, 8, 0, 10, 0, 0, 0, 60, 0, 0, 0, 16, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 0, 16, 0, 0, 0, 12, 0, 20, 0, 6, 0, 8, 0, 12, 0, 16, 0, 12, 0, 0, 0, 0, 0, 4, 0, 60, 0, 0, 0, 40, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 136, 0, 0, 0, 0, 0, 0, 0, 144, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 8, 0, 0, 0, 4, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 16, 0, 20, 0, 8, 0, 0, 0, 7, 0, 12, 0, 0, 0, 16, 0, 16, 0, 0, 0, 0, 0, 0, 2, 16, 0, 0, 0, 28, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 49, 0, 0, 0, 8, 0, 12, 0, 8, 0, 7, 0, 8, 0, 0, 0, 0, 0, 0, 1, 32, 0, 0, 0, 168, 0, 0, 0, 65, 82, 82, 79, 87, 49]);

const table = tableFromIPC(bytes);

console.log("table", table);
console.log("table", table.toArray());
