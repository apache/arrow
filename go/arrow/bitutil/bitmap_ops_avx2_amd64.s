//+build !noasm !appengine
// AUTO-GENERATED BY C2GOASM -- DO NOT EDIT

TEXT ·_bitmap_aligned_and_avx2(SB), $0-32

	MOVQ left+0(FP), DI
	MOVQ right+8(FP), SI
	MOVQ out+16(FP), DX
	MOVQ length+24(FP), CX

	WORD $0x8548; BYTE $0xc9 // test    rcx, rcx
	JLE  LBB0_12
	LONG $0x7ff98348         // cmp    rcx, 127
	JA   LBB0_7
	WORD $0x3145; BYTE $0xd2 // xor    r10d, r10d
	JMP  LBB0_3

LBB0_7:
	LONG $0x0a0c8d4c         // lea    r9, [rdx + rcx]
	LONG $0x0f048d48         // lea    rax, [rdi + rcx]
	WORD $0x3948; BYTE $0xd0 // cmp    rax, rdx
	LONG $0xd3970f41         // seta    r11b
	LONG $0x0e048d48         // lea    rax, [rsi + rcx]
	WORD $0x3949; BYTE $0xf9 // cmp    r9, rdi
	WORD $0x970f; BYTE $0xd3 // seta    bl
	WORD $0x3948; BYTE $0xd0 // cmp    rax, rdx
	LONG $0xd0970f41         // seta    r8b
	WORD $0x3949; BYTE $0xf1 // cmp    r9, rsi
	LONG $0xd1970f41         // seta    r9b
	WORD $0x3145; BYTE $0xd2 // xor    r10d, r10d
	WORD $0x8441; BYTE $0xdb // test    r11b, bl
	JNE  LBB0_3
	WORD $0x2045; BYTE $0xc8 // and    r8b, r9b
	JNE  LBB0_3
	WORD $0x8949; BYTE $0xca // mov    r10, rcx
	LONG $0x80e28349         // and    r10, -128
	WORD $0x3145; BYTE $0xc0 // xor    r8d, r8d

LBB0_10:
	LONG $0x107ca1c4; WORD $0x0604             // vmovups    ymm0, yword [rsi + r8]
	LONG $0x107ca1c4; WORD $0x064c; BYTE $0x20 // vmovups    ymm1, yword [rsi + r8 + 32]
	LONG $0x107ca1c4; WORD $0x0654; BYTE $0x40 // vmovups    ymm2, yword [rsi + r8 + 64]
	LONG $0x107ca1c4; WORD $0x065c; BYTE $0x60 // vmovups    ymm3, yword [rsi + r8 + 96]
	LONG $0x547ca1c4; WORD $0x0704             // vandps    ymm0, ymm0, yword [rdi + r8]
	LONG $0x5474a1c4; WORD $0x074c; BYTE $0x20 // vandps    ymm1, ymm1, yword [rdi + r8 + 32]
	LONG $0x546ca1c4; WORD $0x0754; BYTE $0x40 // vandps    ymm2, ymm2, yword [rdi + r8 + 64]
	LONG $0x5464a1c4; WORD $0x075c; BYTE $0x60 // vandps    ymm3, ymm3, yword [rdi + r8 + 96]
	LONG $0x117ca1c4; WORD $0x0204             // vmovups    yword [rdx + r8], ymm0
	LONG $0x117ca1c4; WORD $0x024c; BYTE $0x20 // vmovups    yword [rdx + r8 + 32], ymm1
	LONG $0x117ca1c4; WORD $0x0254; BYTE $0x40 // vmovups    yword [rdx + r8 + 64], ymm2
	LONG $0x117ca1c4; WORD $0x025c; BYTE $0x60 // vmovups    yword [rdx + r8 + 96], ymm3
	LONG $0x80e88349                           // sub    r8, -128
	WORD $0x394d; BYTE $0xc2                   // cmp    r10, r8
	JNE  LBB0_10
	WORD $0x3949; BYTE $0xca                   // cmp    r10, rcx
	JE   LBB0_12

LBB0_3:
	WORD $0x894d; BYTE $0xd0 // mov    r8, r10
	WORD $0xf749; BYTE $0xd0 // not    r8
	WORD $0x0149; BYTE $0xc8 // add    r8, rcx
	WORD $0x8949; BYTE $0xc9 // mov    r9, rcx
	LONG $0x03e18349         // and    r9, 3
	JE   LBB0_5

LBB0_4:
	LONG $0x04b60f42; BYTE $0x16 // movzx    eax, byte [rsi + r10]
	LONG $0x17042242             // and    al, byte [rdi + r10]
	LONG $0x12048842             // mov    byte [rdx + r10], al
	LONG $0x01c28349             // add    r10, 1
	LONG $0xffc18349             // add    r9, -1
	JNE  LBB0_4

LBB0_5:
	LONG $0x03f88349 // cmp    r8, 3
	JB   LBB0_12

LBB0_6:
	LONG $0x04b60f42; BYTE $0x16   // movzx    eax, byte [rsi + r10]
	LONG $0x17042242               // and    al, byte [rdi + r10]
	LONG $0x12048842               // mov    byte [rdx + r10], al
	LONG $0x44b60f42; WORD $0x0116 // movzx    eax, byte [rsi + r10 + 1]
	LONG $0x17442242; BYTE $0x01   // and    al, byte [rdi + r10 + 1]
	LONG $0x12448842; BYTE $0x01   // mov    byte [rdx + r10 + 1], al
	LONG $0x44b60f42; WORD $0x0216 // movzx    eax, byte [rsi + r10 + 2]
	LONG $0x17442242; BYTE $0x02   // and    al, byte [rdi + r10 + 2]
	LONG $0x12448842; BYTE $0x02   // mov    byte [rdx + r10 + 2], al
	LONG $0x44b60f42; WORD $0x0316 // movzx    eax, byte [rsi + r10 + 3]
	LONG $0x17442242; BYTE $0x03   // and    al, byte [rdi + r10 + 3]
	LONG $0x12448842; BYTE $0x03   // mov    byte [rdx + r10 + 3], al
	LONG $0x04c28349               // add    r10, 4
	WORD $0x394c; BYTE $0xd1       // cmp    rcx, r10
	JNE  LBB0_6

LBB0_12:
	VZEROUPPER
	RET

TEXT ·_bitmap_aligned_or_avx2(SB), $0-32

	MOVQ left+0(FP), DI
	MOVQ right+8(FP), SI
	MOVQ out+16(FP), DX
	MOVQ length+24(FP), CX

	WORD $0x8548; BYTE $0xc9 // test    rcx, rcx
	JLE  LBB1_12
	LONG $0x7ff98348         // cmp    rcx, 127
	JA   LBB1_7
	WORD $0x3145; BYTE $0xd2 // xor    r10d, r10d
	JMP  LBB1_3

LBB1_7:
	LONG $0x0a0c8d4c         // lea    r9, [rdx + rcx]
	LONG $0x0f048d48         // lea    rax, [rdi + rcx]
	WORD $0x3948; BYTE $0xd0 // cmp    rax, rdx
	LONG $0xd3970f41         // seta    r11b
	LONG $0x0e048d48         // lea    rax, [rsi + rcx]
	WORD $0x3949; BYTE $0xf9 // cmp    r9, rdi
	WORD $0x970f; BYTE $0xd3 // seta    bl
	WORD $0x3948; BYTE $0xd0 // cmp    rax, rdx
	LONG $0xd0970f41         // seta    r8b
	WORD $0x3949; BYTE $0xf1 // cmp    r9, rsi
	LONG $0xd1970f41         // seta    r9b
	WORD $0x3145; BYTE $0xd2 // xor    r10d, r10d
	WORD $0x8441; BYTE $0xdb // test    r11b, bl
	JNE  LBB1_3
	WORD $0x2045; BYTE $0xc8 // and    r8b, r9b
	JNE  LBB1_3
	WORD $0x8949; BYTE $0xca // mov    r10, rcx
	LONG $0x80e28349         // and    r10, -128
	WORD $0x3145; BYTE $0xc0 // xor    r8d, r8d

LBB1_10:
	LONG $0x107ca1c4; WORD $0x0604             // vmovups    ymm0, yword [rsi + r8]
	LONG $0x107ca1c4; WORD $0x064c; BYTE $0x20 // vmovups    ymm1, yword [rsi + r8 + 32]
	LONG $0x107ca1c4; WORD $0x0654; BYTE $0x40 // vmovups    ymm2, yword [rsi + r8 + 64]
	LONG $0x107ca1c4; WORD $0x065c; BYTE $0x60 // vmovups    ymm3, yword [rsi + r8 + 96]
	LONG $0x567ca1c4; WORD $0x0704             // vorps    ymm0, ymm0, yword [rdi + r8]
	LONG $0x5674a1c4; WORD $0x074c; BYTE $0x20 // vorps    ymm1, ymm1, yword [rdi + r8 + 32]
	LONG $0x566ca1c4; WORD $0x0754; BYTE $0x40 // vorps    ymm2, ymm2, yword [rdi + r8 + 64]
	LONG $0x5664a1c4; WORD $0x075c; BYTE $0x60 // vorps    ymm3, ymm3, yword [rdi + r8 + 96]
	LONG $0x117ca1c4; WORD $0x0204             // vmovups    yword [rdx + r8], ymm0
	LONG $0x117ca1c4; WORD $0x024c; BYTE $0x20 // vmovups    yword [rdx + r8 + 32], ymm1
	LONG $0x117ca1c4; WORD $0x0254; BYTE $0x40 // vmovups    yword [rdx + r8 + 64], ymm2
	LONG $0x117ca1c4; WORD $0x025c; BYTE $0x60 // vmovups    yword [rdx + r8 + 96], ymm3
	LONG $0x80e88349                           // sub    r8, -128
	WORD $0x394d; BYTE $0xc2                   // cmp    r10, r8
	JNE  LBB1_10
	WORD $0x3949; BYTE $0xca                   // cmp    r10, rcx
	JE   LBB1_12

LBB1_3:
	WORD $0x894d; BYTE $0xd0 // mov    r8, r10
	WORD $0xf749; BYTE $0xd0 // not    r8
	WORD $0x0149; BYTE $0xc8 // add    r8, rcx
	WORD $0x8949; BYTE $0xc9 // mov    r9, rcx
	LONG $0x03e18349         // and    r9, 3
	JE   LBB1_5

LBB1_4:
	LONG $0x04b60f42; BYTE $0x16 // movzx    eax, byte [rsi + r10]
	LONG $0x17040a42             // or    al, byte [rdi + r10]
	LONG $0x12048842             // mov    byte [rdx + r10], al
	LONG $0x01c28349             // add    r10, 1
	LONG $0xffc18349             // add    r9, -1
	JNE  LBB1_4

LBB1_5:
	LONG $0x03f88349 // cmp    r8, 3
	JB   LBB1_12

LBB1_6:
	LONG $0x04b60f42; BYTE $0x16   // movzx    eax, byte [rsi + r10]
	LONG $0x17040a42               // or    al, byte [rdi + r10]
	LONG $0x12048842               // mov    byte [rdx + r10], al
	LONG $0x44b60f42; WORD $0x0116 // movzx    eax, byte [rsi + r10 + 1]
	LONG $0x17440a42; BYTE $0x01   // or    al, byte [rdi + r10 + 1]
	LONG $0x12448842; BYTE $0x01   // mov    byte [rdx + r10 + 1], al
	LONG $0x44b60f42; WORD $0x0216 // movzx    eax, byte [rsi + r10 + 2]
	LONG $0x17440a42; BYTE $0x02   // or    al, byte [rdi + r10 + 2]
	LONG $0x12448842; BYTE $0x02   // mov    byte [rdx + r10 + 2], al
	LONG $0x44b60f42; WORD $0x0316 // movzx    eax, byte [rsi + r10 + 3]
	LONG $0x17440a42; BYTE $0x03   // or    al, byte [rdi + r10 + 3]
	LONG $0x12448842; BYTE $0x03   // mov    byte [rdx + r10 + 3], al
	LONG $0x04c28349               // add    r10, 4
	WORD $0x394c; BYTE $0xd1       // cmp    rcx, r10
	JNE  LBB1_6

LBB1_12:
	VZEROUPPER
	RET
