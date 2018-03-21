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

/*
Package math provides optimized mathematical functions for processing Arrow arrays.
*/
package math

//go:generate go run ../_tools/tmpl/main.go -i -data=float64.tmpldata type.go.tmpl=float64.go type_amd64.go.tmpl=float64_amd64.go type_test.go.tmpl=float64_test.go
//go:generate go run ../_tools/tmpl/main.go -i -data=float64.tmpldata -d arch=avx2 type_simd_amd64.go.tmpl=float64_avx2_amd64.go
//go:generate go run ../_tools/tmpl/main.go -i -data=float64.tmpldata -d arch=sse4 type_simd_amd64.go.tmpl=float64_sse4_amd64.go
//go:generate go run ../_tools/tmpl/main.go -i -data=int64.tmpldata type.go.tmpl=int64.go type_amd64.go.tmpl=int64_amd64.go type_test.go.tmpl=int64_test.go
//go:generate go run ../_tools/tmpl/main.go -i -data=int64.tmpldata -d arch=avx2 type_simd_amd64.go.tmpl=int64_avx2_amd64.go
//go:generate go run ../_tools/tmpl/main.go -i -data=int64.tmpldata -d arch=sse4 type_simd_amd64.go.tmpl=int64_sse4_amd64.go
//go:generate go run ../_tools/tmpl/main.go -i -data=uint64.tmpldata type.go.tmpl=uint64.go type_amd64.go.tmpl=uint64_amd64.go type_test.go.tmpl=uint64_test.go
//go:generate go run ../_tools/tmpl/main.go -i -data=uint64.tmpldata -d arch=avx2 type_simd_amd64.go.tmpl=uint64_avx2_amd64.go
//go:generate go run ../_tools/tmpl/main.go -i -data=uint64.tmpldata -d arch=sse4 type_simd_amd64.go.tmpl=uint64_sse4_amd64.go
