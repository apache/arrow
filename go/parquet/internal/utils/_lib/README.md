<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# SIMD Bit Packing Implementation

Go doesn't have any SIMD intrinsics so for some low-level optimizations we can 
leverage auto-vectorization by C++ compilers and the fact that Go lets you specify the body of a
function in assembly to benefit from SIMD.

In here we have implementations using SIMD intrinsics for AVX (amd64) and NEON (arm64).

## Generating the Go assembly

c2goasm and asm2plan9s are two projects which can be used in conjunction to generate
compatible Go assembly from C assembly.

First the tools need to be installed:

```bash
go install github.com/klauspost/asmfmt/cmd/asmfmt@latest
go install github.com/minio/asm2plan9s@latest
go install github.com/minio/c2goasm@latest
```

### Generating for amd64

The Makefile in the directory above will work for amd64. `make assembly` will compile
the c sources and then call `c2goasm` to generate the Go assembly for amd64 
architectures.

### Generating for arm64

Unfortunately there are some caveats for arm64. c2goasm / asm2plan9s doesn't fully
support arm64 correctly. However, proper assembly can be created with some slight
manipulation of the result.

The Makefile has the NEON flags for compiling the assembly by using 
`make _lib/bit_packing_neon.s` and `make _lib/unpack_bool_neon.s` to generate the
raw assembly sources. 

Before calling `c2goasm` there's a few things that need to be modified in the assembly:

* x86-64 assembly uses `#` for comments while arm64 assembly uses `//` for comments.
  `c2goasm` assumes `#` for comments and splits lines based on them. For most lines
  this isn't an issue, but for any constants this is important and will need to have
  the comment character converted from `//` to `#`.
* A `word` for x86-64 is 16 bits, a `double` word is 32 bits, and a `quad` is 64 bits.
  For arm64, a `word` is 32 bits. This means that constants in the assembly need to be
  modified. `c2goasm` and `asm2plan9s` expect the x86-64 meaning for the sizes, so
  usage of `.word ######` needs to be converted to `.long #####` before running
  `c2goasm`. In addition, `.xword` is an 8-byte value and as such should be changed to
  `.quad` before running `c2goasm`.
* Because of this change in bits, `MOVQ` instructions will also be converted to 
  `MOVD` instructions.

After running `c2goasm` there will still need to be modifications made to the 
resulting assembly.

* Most of the ARM instructions will be converted to using the Go assembly construction
  of `WORD $0x########` to provide an instruction directly to the processor rather than
  going through the Go assembler. Some of the instructions, however, aren't recognized
  by `c2goasm` and will need to added. If you look at the assembly, you'll see these
  as assembly that is commented out without any `WORD` instruction. For example:
  ```asm
  // stp x29, x30, [sp, #-48]!
  WORD $0x11007c48 // add  w8, w2, #31
  ```
  The `stp` instruction needs to be added. This can be done in one of two ways:
  1. Many instructions are properly handled by the Go assembler correctly. You can
     find the arm-specific caveats to Go's assembly [here](https://pkg.go.dev/cmd/internal/obj/arm64). In this case, the instruction would be `STP.W (R29, R30), -48(RSP)`.
  2. Assuming that the GNU assembler is installed, you can use it to generate the
     correct byte sequence. Create a file named `neon.asm` with a single line 
     (the instruction) and call `as -o neon.o neon.asm`. Then you can run
     `objdump -S neon.o` to get the value to use. The output should look something 
     like:
     ```
     Disassembly of section .text:

     0000000000000000 <.text>:
     0:   11 00 7c 48    add  w8, w2, #31
     ```
     And then update the assembly as `WORD $0x11007c48 // add w8, w2, #31`
* Labels used in instructions won't work when using the `WORD $0x#########` syntax.
  They need to be the actual instructions for the labels. So all lines that have a
  label will need to be converted. This is two-fold:
  1. Any lines for branching such as those which end with `// b.le LBB0_10` are updated
     to be `BLE LBB0_10`. The same is true for `b.gt`, `b.ge`, `b.ne`, and `b.eq`. `b` 
     instructions are instead converted to `JMP` calls.
  2. References to constants need to be updated, for example `LCPI0_192`. By default,
     these will get converted to global data instructions like 
     `DATA LCDATA1<>+0xc68(SB)/8, $0x0000000000000000`. Unfortunately, these seem to 
     have issues with being referenced by the assembler. The pattern to look for in 
     the assembly is an `adrp x9, .LCPI0_192` instruction that is later followed by 
     an instruction that looks like `str d4, [x9, 0:lo12:.LCPI0_192]`. These will
     need to be converted to a macro and a `VMOV` instruction. 
     * In the original assembly, you'll see blocks like:
       ```asm
       .LCPI0_0
          .word 1           // 0x00000001
          .word 2           // 0x00000002
       .LCPI0_1
          .word 4294967265  // 0xffffffe1
          .word 4294967266  // 0xffffffe2
       ```
       which were converted to the `DATA LCDATA1`.... lines. Instead they should get
       converted to a macro and a vector instruction:
       ```asm
       #define LCPI0_0 $0x0000000200000001
       #define LCPI0_1 $0xffffffe2ffffffe1
       ```
       Notice the lower/higher bits!
       Then replace the `str`/`ldr`/`mov` instruction as `VMOVD LCPI0_0, v4`. Because
       the original instruction storing the value in `d4`, we use `VMOVD` and `V4`. 
       Alternately we might find a prefix of `q` instead of `d`, in which case it we
       need to use `VMOVQ` and pass the lower bytes followed by the higher bytes.
       ```asm
       #define LCPI0_48L $0x0000000d00000008
       #define LCPI0_48H $0x0000001700000012
       ...
       VMOVQ LCPI0_48L, LCPI0_48H, V4
       ```
       After replacing the instructions, both the `adrp` and the `str`/`ldr`/`mov` 
       instructions should be removed/commented out.
       There might also be a `LEAQ LCDATA1<>(SB), BP` instruction at the top of the
       function. That should be removed/commented out as we are replacing the constants
       with macros.
* Finally, if the function has a return value, make sure that at the end of the 
  function, ends with something akin to `MOVD R0, num+32(FP)`. Where `num` is the
  local variable name of the return value, and `32` is the byte size of the arguments.

To faciliate some automation, a `script.sed` file is provided in this directory which
can be run against the generated assembly from `c2goasm` as 
`sed -f _lib/script.sed -i bit_packing_neon_arm64.s` which will perform several of 
these steps on the generated assembly such as convering `b.le`/etc calls with labels
to proper `BLE LBB0_....` lines, and converting `adrp`/`ldr` pairs to `VMOVD` and 
`VMOVQ` instructions.

This should be sufficient to ensuring the assembly is generated and works properly!