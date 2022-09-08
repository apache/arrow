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

// +build !noasm

package utils

import (
	"io"
	"os"
	"strings"
)
import "golang.org/x/sys/cpu"

var unpack32 func(io.Reader, []uint32, int) int = unpack32Default

func init() {
        cpu.ARM64.HasASIMD = false
        cpu.ARM64.HasAES = false
        cpu.ARM64.HasPMULL = false
        // Added ability to enable extension via environment:
	if ext, ok := os.LookupEnv("ARM_ENABLE_EXT"); ok {
		exts := strings.Split(ext, ",")

		for _, x := range exts {
			switch x {
			case "NEON":
				cpu.ARM64.HasASIMD = true
			case "AES":
				cpu.ARM64.HasAES = true
			case "PMULL":
				cpu.ARM64.HasPMULL = true
			default:
			}
		}
	}
	if cpu.ARM64.HasASIMD {
		unpack32 = unpack32NEON
	} else { // default to the pure go implementation if no avx2 available
		unpack32 = unpack32Default
	}
}

