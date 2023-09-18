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

//go:build !noasm
// +build !noasm

package bmi

import (
	"os"
	"strings"

	"github.com/klauspost/cpuid/v2"
)

func init() {
	// Added ability to enable extension via environment:
	// ARM_ENABLE_EXT=NEON go test
	if ext, ok := os.LookupEnv("ARM_ENABLE_EXT"); ok {
		exts := strings.Split(ext, ",")

		for _, x := range exts {
			switch x {
			case "NEON":
				cpuid.CPU.Enable(cpuid.ASIMD)
			case "AES":
				cpuid.CPU.Enable(cpuid.AESARM)
			case "PMULL":
				cpuid.CPU.Enable(cpuid.PMULL)
			default:
				cpuid.CPU.Disable(cpuid.ASIMD, cpuid.AESARM, cpuid.PMULL)
			}
		}
	}
	if cpuid.CPU.Has(cpuid.ASIMD) {
		funclist.extractBits = extractBitsNEON
		funclist.gtbitmap = greaterThanBitmapNEON
	} else {
		funclist.extractBits = extractBitsGo
		funclist.gtbitmap = greaterThanBitmapGo
	}
}
