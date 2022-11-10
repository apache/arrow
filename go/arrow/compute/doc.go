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

// Package compute is a native-go implementation of an Acero-like
// arrow compute engine.
//
// For now in order to properly use the compute library you'll need to run:
//
//     go mod edit -replace github.com/apache/arrow/go/v${major_version}/arrow/compute=github.com/apache/arrow/go/arrow/compute
//
// In order to import "github.com/apache/arrow/go/v${major_version}/arrow/compute"
// in your package. This is due to it being a separate module so that it can
// utilize go1.18. After the release of go1.20, the Arrow modules will be bumped to
// a minimum go.mod of go1.18 and the compute package will be integrated
// into the arrow module proper, rather than being a separate module. At that
// point, the replace directive will no longer be needed.
//
// While consumers of Arrow that are able to use CGO could utilize the
// C Data API (using the cdata package) and could link against the
// acero library directly, there are consumers who cannot use CGO. This
// is an attempt to provide for those users, and in general create a
// native-go arrow compute engine.
//
// Everything in this package should be considered Experimental for now.
package compute

//go:generate stringer -type=FuncKind -linecomment
