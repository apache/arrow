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

package scalar

import "github.com/apache/arrow/go/v9/arrow"

//TODO(zeroshade): approxequals
// tracked in https://issues.apache.org/jira/browse/ARROW-13980

// Equals returns true if two scalars are equal, which means they have the same
// datatype, validity and value.
func Equals(left, right Scalar) bool {
	if left == right {
		return true
	}

	if !arrow.TypeEqual(left.DataType(), right.DataType()) {
		return false
	}

	if left.IsValid() != right.IsValid() {
		return false
	}

	if !left.IsValid() {
		return true
	}

	return left.equals(right)
}
