// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build arm64

package cpu

import (
	"os"
	"strings"
)

const CacheLineSize = 64

func init() {
	// NOTE(sgc): added ability to enable extension via environment
	if ext, ok := os.LookupEnv("ARM_ENABLE_EXT"); ok {
		exts := strings.Split(ext, ",")

		for _, x := range exts {
			switch x {
			case "NEON":
				ARM64.HasASIMD = true
			case "AES":
				ARM64.HasAES = true
			case "PMULL":
				ARM64.HasPMULL = true
			default:
				ARM64.HasASIMD = false
				ARM64.HasAES = false
				ARM64.HasPMULL = false
			}
		}
	}
}

