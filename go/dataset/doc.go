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

// Package dataset provides a cgo wrapper around the Apache Arrow Dataset API
// from the C++ arrow-dataset package.
//
// Install
//
// Using this library requires the installation of the headers and dependencies
// for the arrow-dataset library. See https://arrow.apache.org/install for
// details on how to install libarrow-dataset-dev and its dependencies.
//
// After the dependencies are installed and accessible via pkg-config to report
// the location of the headers and libraries, then this module can be safely
// retrieved via:
// 		go get -u github.com/apache/arrow/go/dataset
//
// API
//
// The Dataset API is experimental and in development and as such a stable API
// is not yet guaranteed.
//
// Currently only a subset of the API is implemented.
package dataset
