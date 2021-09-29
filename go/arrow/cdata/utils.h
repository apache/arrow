// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build cgo
// +build test

/* 
   Function check_for_endianness() returns 1, if architecture 
   is little endian, 0 in case of big endian.
*/
inline int is_little_endian()
{
  unsigned int x = 1;
  char *c = (char*) &x;
  return (int)*c;
}

// metadata keys 1: {"key1", "key2"}
// metadata values 1: {"", "bar"}
static const char kEncodedMeta1LE[] = {
    2, 0, 0, 0,
    4, 0, 0, 0, 'k', 'e', 'y', '1', 0, 0, 0, 0,
    4, 0, 0, 0, 'k', 'e', 'y', '2', 3, 0, 0, 0, 'b', 'a', 'r'};

static const char kEncodedMeta1BE[] = {
    0, 0, 0, 2,
    0, 0, 0, 4, 'k', 'e', 'y', '1', 0, 0, 0, 0,
    0, 0, 0, 4, 'k', 'e', 'y', '2', 0, 0, 0, 3, 'b', 'a', 'r'};

static const char* kMetadataKeys2[] = {"key"};
static const char* kMetadataValues2[] = {"abcde"};

// metadata keys 2: {"key"}
// metadata values 2: {"abcde"}
static const char kEncodedMeta2LE[] = {
    1, 0, 0, 0,
    3, 0, 0, 0, 'k', 'e', 'y', 5, 0, 0, 0, 'a', 'b', 'c', 'd', 'e'};

static const char kEncodedMeta2BE[] = {
    0, 0, 0, 1,
    0, 0, 0, 3, 'k', 'e', 'y', 0, 0, 0, 5, 'a', 'b', 'c', 'd', 'e'};


