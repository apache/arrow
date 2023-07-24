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

int main (string[] args) {
    var builder = new GArrow.Int32ArrayBuilder();
    try {
        builder.append_value(29);
        builder.append_value(2929);
        builder.append_value(292929);
    } catch (Error error) {
        stderr.printf("failed to append: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    GArrow.Array array;
    try {
        array = builder.finish();
    } catch (Error error) {
        stderr.printf("failed to finish: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    var int32_array = array as GArrow.Int32Array;
    var n = array.get_length();
    stdout.printf("length: %" + int64.FORMAT + "\n", n);
    for (int64 i = 0; i < n; i++) {
        var value = int32_array.get_value(i);
        stdout.printf("array[%" + int64.FORMAT + "] = %d\n",
                      i, value);
    }

    return Posix.EXIT_SUCCESS;
}
