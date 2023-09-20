#include "textflag.h"

// Adapted for ARM64 Go Plan9 from 
// https://elixir.bootlin.com/linux/v6.5.4/source/arch/arm64/lib/memcpy.S

// void *memcpy(void *dst, const void *src, size_t n)
TEXT clib·_memcpy(SB), NOSPLIT|NOFRAME, $0
	MOVD R0, R6
	
	CMP $16, R2
	BLO Ltiny15

	NEG R1, R4
	ANDS $15, R4, R4
	BEQ LSrcAligned
	SUB R4, R2, R2

	TBZ $0, R4, L01
	MOVBU.P 1(R1), R3
	MOVB.P  R3, 1(R6)
L01:	
	TBZ $1, R4, L02
	MOVHU.P 2(R1), R3
	MOVH.P  R3, 2(R6)
L02:
	TBZ $2, R4, L03
	MOVWU.P 4(R1), R3
	MOVW.P  R3, 4(R6)
L03:
	TBZ $3, R4, LSrcAligned
	MOVD.P  8(R1), R3
	MOVD.P  R3, 8(R6)

LSrcAligned:
	CMP $64, R2
	BGE Lcpy_over64

Ltail63:
	ANDS $0x30, R2, R3
	BEQ Ltiny15
	CMPW $0x20, R3
	BEQ L11
	BLT L12

	LDP.P 16(R1), (R7, R8)
	STP.P (R7, R8), 16(R6)
L11:
	LDP.P 16(R1), (R7, R8)
	STP.P (R7, R8), 16(R6)
L12:
	LDP.P 16(R1), (R7, R8)
	STP.P (R7, R8), 16(R6)

Ltiny15:
	TBZ $3, R2, Ltiny1
	MOVD.P 8(R1), R3
	MOVD.P R3, 8(R6)
Ltiny1:
	TBZ $2, R2, Ltiny2
	MOVWU.P 4(R1), R3
	MOVW.P R3, 4(R6)
Ltiny2:
	TBZ $1, R2, Ltiny3
	MOVHU.P 2(R1), R3
	MOVH.P  R3, 2(R6)
Ltiny3:
	TBZ $0, R2, Lexitfunc
	MOVBU.P (R1), R3
	MOVB.P  R3, (R6)
		
Lexitfunc:
	RET

Lcpy_over64:
	SUBS $128, R2, R2
	BGE Lcpy_body_large

	LDP.P 16(R1), (R7, R8)
	STP.P (R7, R8), 16(R6)
	LDP.P 16(R1), (R9, R10)
	LDP.P 16(R1), (R11, R12)
	STP.P (R9, R10), 16(R6)
	STP.P (R11, R12), 16(R6)
	LDP.P 16(R1), (R13, R14)
	STP.P (R13, R14), 16(R6)

	TST $0x3f, R2
	BNE Ltail63
	RET

	PCALIGN $64
Lcpy_body_large:
	LDP.P 16(R1), (R7, R8)
	LDP.P 16(R1), (R9, R10)
	LDP.P 16(R1), (R11, R12)
	LDP.P 16(R1), (R13, R14)
Lcpy1:
	STP.P (R7, R8), 16(R6)
	LDP.P 16(R1), (R7, R8)
	STP.P (R9, R10), 16(R6)
	LDP.P 16(R1), (R9, R10)
	STP.P (R11, R12), 16(R6)
	LDP.P 16(R1), (R11, R12)
	STP.P (R13, R14), 16(R6)
	LDP.P 16(R1), (R13, R14)
	SUBS $64, R2, R2
	BGE Lcpy1
	STP.P (R7, R8), 16(R6)
	STP.P (R9, R10), 16(R6)
	STP.P (R11, R12), 16(R6)
	STP.P (R13, R14), 16(R6)

	TST $0x3f, R2
	BNE Ltail63
	RET

// func _ClibMemcpy(dst, src unsafe.Pointer, n uint) unsafe.Pointer
TEXT ·_ClibMemcpy(SB), NOSPLIT, $16-24
	MOVD arg1+0(FP), R0
	MOVD arg2+8(FP), R1
	MOVD arg3+16(FP), R2
	CALL clib·_memcpy(SB)
	MOVD R0, ret+24(FP)
	RET

// Adapted for ARM64 Go Plan9 from
// https://elixir.bootlin.com/linux/v6.5.4/source/arch/arm64/lib/memset.S

// void *memset(void *str, int c, size_t n)
TEXT clib·_memset(SB), NOSPLIT|NOFRAME, $0-0
	MOVD R0, R8
	ANDW $255, R1, R7
	ORRW R7<<8, R7, R7
	ORRW R7<<16, R7, R7
	ORR  R7<<32, R7, R7

	CMP $15, R2
	BHI Lover16_proc
	TBZ $3, R2, L01
	MOVD.P R7, 8(R8)
L01:
	TBZ $2, R2, L02
	MOVW.P R7, 4(R8)
L02:
	TBZ $1, R2, L03
	MOVH.P R7, 2(R8)
L03:
	TBZ $0, R2, L04
	MOVB R7, (R8)
L04:
	RET

Lover16_proc:
	NEG R6, R4
	ANDS $15, R4, R4
	BEQ Laligned

	STP (R7, R7), (R8)
	SUB R4, R2, R2
	ADD R2, R8, R8

Laligned:
	CBZ R7, Lzero_mem

Ltail_maybe_long:
	CMP $64, R2
	BGE Lnot_short

Ltail63:
	ANDS $0x30, R2, R3
	BEQ Ltail3
	CMPW $0x20, R3
	BEQ Ltail1
	BLT Ltail2
	STP.P (R7, R7), 16(R8)
Ltail1:
	STP.P (R7, R7), 16(R8)
Ltail2:
	STP.P (R7, R7), 16(R8)
Ltail3:
	ANDS $15, R2, R2
	CBZ R2, Ltail4
	ADD R2, R8, R8
	STP (R7, R7), -16(R8)
Ltail4:	
	RET
	PCALIGN $64
Lnot_short:
	SUB $16, R8, R8
	SUB $64, R2, R2

Lnot_short01:	
	STP (R7, R7), 16(R8)
	STP (R7, R7), 32(R8)
	STP (R7, R7), 48(R8)
	STP.W (R7, R7), 64(R8)
	SUBS $64, R2, R2
	BGE Lnot_short01
	TST $0x3f, R2
	ADD $16, R8, R8
	BNE Ltail63	
Lexitfunc:
	RET
	
Lzero_mem:
	CMP $63, R2
	BLE Ltail63

	CMP $128, R2
	BLT Lnot_short

	MRS DCZID_EL0, R3
	TBNZ $4, R3, Lnot_short

	MOVW $4, R9
	ANDW $15, R3, R5
	LSLW R5, R9, R5
	MOVW R5, block_size<>(SB)
	
	ANDS $63, R5, R9

	BNE Lnot_short

Lzero_by_line:
	CMP R5, R1
	BLT Lnot_short

	SUB $1, R5, R6
	NEG R8, R4
	ANDS R6, R4, R4
	BEQ Lzero_aligned

	SUB R4, R2, R3

	CMP $64, R3
	CCMP GE, R3, R5, $8
	BLT Lnot_short

	MOVD R3, R1

Lloop_zva_prolog:
	STP (R7, R7), (R8)
	STP (R7, R7), 16(R8)
	STP (R7, R7), 32(R8)
	SUBS $64, R4, R4
	STP (R7, R7), 48(R8)
	BGE Lloop_zva_prolog

	ADD R4, R8, R8
Lzero_aligned:
	SUB R2, R2, R5

Lloop_zva:
	WORD $0xd50b7428 // DC ZVA, R8
	ADD R5, R8, R8
	SUBS R5, R2, R2
	BGE Lloop_zva
	ANDS R6, R1, R1
	BNE Ltail_maybe_long
	RET
	
// func _ClibMemset(dst unsafe.Pointer, c int, n uint) unsafe.Pointer
TEXT ·_ClibMemset(SB), NOSPLIT, $16-24
	MOVD arg1+0(FP), R0
	MOVD arg2+8(FP), R1
	MOVD arg3+16(FP), R2
	CALL clib·_memset(SB)
	MOVD R0, ret+24(FP)
	RET

GLOBL block_size<>(SB), NOPTR, $8
