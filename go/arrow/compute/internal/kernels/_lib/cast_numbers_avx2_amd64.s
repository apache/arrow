	.text
	.intel_syntax noprefix
	.file	"cast_numbers.c"
	.globl	cast_numeric_uint8_uint8_avx2   # -- Begin function cast_numeric_uint8_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint8_avx2,@function
cast_numeric_uint8_uint8_avx2:          # @cast_numeric_uint8_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB0_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 127
	jbe	.LBB0_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB0_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB0_9
.LBB0_2:
	xor	ecx, ecx
.LBB0_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB0_5
	.p2align	4, 0x90
.LBB0_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB0_4
.LBB0_5:
	cmp	r8, 3
	jb	.LBB0_16
	.p2align	4, 0x90
.LBB0_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB0_6
	jmp	.LBB0_16
.LBB0_9:
	mov	ecx, r9d
	and	ecx, -128
	lea	rax, [rcx - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB0_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 224]
	vmovups	ymmword ptr [rsi + rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + rdx + 224], ymm3
	add	rdx, 256
	add	rax, 2
	jne	.LBB0_12
# %bb.13:
	test	r8b, 1
	je	.LBB0_15
.LBB0_14:
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
.LBB0_15:
	cmp	rcx, r9
	jne	.LBB0_3
.LBB0_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB0_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB0_14
	jmp	.LBB0_15
.Lfunc_end0:
	.size	cast_numeric_uint8_uint8_avx2, .Lfunc_end0-cast_numeric_uint8_uint8_avx2
                                        # -- End function
	.globl	cast_numeric_int8_uint8_avx2    # -- Begin function cast_numeric_int8_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint8_avx2,@function
cast_numeric_int8_uint8_avx2:           # @cast_numeric_int8_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB1_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 127
	jbe	.LBB1_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB1_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB1_9
.LBB1_2:
	xor	ecx, ecx
.LBB1_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB1_5
	.p2align	4, 0x90
.LBB1_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB1_4
.LBB1_5:
	cmp	r8, 3
	jb	.LBB1_16
	.p2align	4, 0x90
.LBB1_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB1_6
	jmp	.LBB1_16
.LBB1_9:
	mov	ecx, r9d
	and	ecx, -128
	lea	rax, [rcx - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB1_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB1_12:                               # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 224]
	vmovups	ymmword ptr [rsi + rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + rdx + 224], ymm3
	add	rdx, 256
	add	rax, 2
	jne	.LBB1_12
# %bb.13:
	test	r8b, 1
	je	.LBB1_15
.LBB1_14:
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
.LBB1_15:
	cmp	rcx, r9
	jne	.LBB1_3
.LBB1_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB1_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB1_14
	jmp	.LBB1_15
.Lfunc_end1:
	.size	cast_numeric_int8_uint8_avx2, .Lfunc_end1-cast_numeric_int8_uint8_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_uint16_uint8_avx2
.LCPI2_0:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	cast_numeric_uint16_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint8_avx2,@function
cast_numeric_uint16_uint8_avx2:         # @cast_numeric_uint16_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB2_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB2_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB2_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB2_9
.LBB2_2:
	xor	ecx, ecx
.LBB2_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB2_5
	.p2align	4, 0x90
.LBB2_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB2_4
.LBB2_5:
	cmp	r8, 3
	jb	.LBB2_16
	.p2align	4, 0x90
.LBB2_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 2*rcx + 2]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 2*rcx + 4]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 2*rcx + 6]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB2_6
	jmp	.LBB2_16
.LBB2_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB2_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	.p2align	4, 0x90
.LBB2_12:                               # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 112], xmm4
	sub	rdx, -128
	add	rax, 2
	jne	.LBB2_12
# %bb.13:
	test	r8b, 1
	je	.LBB2_15
.LBB2_14:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm0
.LBB2_15:
	cmp	rcx, r9
	jne	.LBB2_3
.LBB2_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB2_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB2_14
	jmp	.LBB2_15
.Lfunc_end2:
	.size	cast_numeric_uint16_uint8_avx2, .Lfunc_end2-cast_numeric_uint16_uint8_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_int16_uint8_avx2
.LCPI3_0:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	cast_numeric_int16_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint8_avx2,@function
cast_numeric_int16_uint8_avx2:          # @cast_numeric_int16_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB3_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB3_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB3_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB3_9
.LBB3_2:
	xor	ecx, ecx
.LBB3_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB3_5
	.p2align	4, 0x90
.LBB3_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB3_4
.LBB3_5:
	cmp	r8, 3
	jb	.LBB3_16
	.p2align	4, 0x90
.LBB3_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 2*rcx + 2]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 2*rcx + 4]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 2*rcx + 6]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB3_6
	jmp	.LBB3_16
.LBB3_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB3_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI3_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	.p2align	4, 0x90
.LBB3_12:                               # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 112], xmm4
	sub	rdx, -128
	add	rax, 2
	jne	.LBB3_12
# %bb.13:
	test	r8b, 1
	je	.LBB3_15
.LBB3_14:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI3_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm0
.LBB3_15:
	cmp	rcx, r9
	jne	.LBB3_3
.LBB3_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB3_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB3_14
	jmp	.LBB3_15
.Lfunc_end3:
	.size	cast_numeric_int16_uint8_avx2, .Lfunc_end3-cast_numeric_int16_uint8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_uint8_avx2
.LCPI4_0:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
	.byte	8                               # 0x8
	.byte	12                              # 0xc
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_uint32_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint8_avx2,@function
cast_numeric_uint32_uint8_avx2:         # @cast_numeric_uint32_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB4_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB4_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB4_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB4_9
.LBB4_2:
	xor	ecx, ecx
.LBB4_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB4_5
	.p2align	4, 0x90
.LBB4_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB4_4
.LBB4_5:
	cmp	r8, 3
	jb	.LBB4_16
	.p2align	4, 0x90
.LBB4_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB4_6
	jmp	.LBB4_16
.LBB4_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB4_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI4_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB4_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx + 32], ymm1
	add	rdx, 64
	add	rax, 2
	jne	.LBB4_12
# %bb.13:
	test	r8b, 1
	je	.LBB4_15
.LBB4_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI4_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
.LBB4_15:
	cmp	rcx, r9
	jne	.LBB4_3
.LBB4_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB4_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB4_14
	jmp	.LBB4_15
.Lfunc_end4:
	.size	cast_numeric_uint32_uint8_avx2, .Lfunc_end4-cast_numeric_uint32_uint8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int32_uint8_avx2
.LCPI5_0:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
	.byte	8                               # 0x8
	.byte	12                              # 0xc
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_int32_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint8_avx2,@function
cast_numeric_int32_uint8_avx2:          # @cast_numeric_int32_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB5_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB5_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB5_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB5_9
.LBB5_2:
	xor	ecx, ecx
.LBB5_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB5_5
	.p2align	4, 0x90
.LBB5_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB5_4
.LBB5_5:
	cmp	r8, 3
	jb	.LBB5_16
	.p2align	4, 0x90
.LBB5_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB5_6
	jmp	.LBB5_16
.LBB5_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB5_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI5_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB5_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx + 32], ymm1
	add	rdx, 64
	add	rax, 2
	jne	.LBB5_12
# %bb.13:
	test	r8b, 1
	je	.LBB5_15
.LBB5_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI5_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
.LBB5_15:
	cmp	rcx, r9
	jne	.LBB5_3
.LBB5_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB5_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB5_14
	jmp	.LBB5_15
.Lfunc_end5:
	.size	cast_numeric_int32_uint8_avx2, .Lfunc_end5-cast_numeric_int32_uint8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint64_uint8_avx2
.LCPI6_0:
	.byte	0                               # 0x0
	.byte	8                               # 0x8
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_uint64_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint8_avx2,@function
cast_numeric_uint64_uint8_avx2:         # @cast_numeric_uint64_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB6_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB6_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB6_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB6_9
.LBB6_2:
	xor	ecx, ecx
.LBB6_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB6_5
	.p2align	4, 0x90
.LBB6_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB6_4
.LBB6_5:
	cmp	r8, 3
	jb	.LBB6_16
	.p2align	4, 0x90
.LBB6_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB6_6
	jmp	.LBB6_16
.LBB6_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB6_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI6_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB6_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB6_12
# %bb.13:
	test	r8b, 1
	je	.LBB6_15
.LBB6_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI6_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
.LBB6_15:
	cmp	rcx, r9
	jne	.LBB6_3
.LBB6_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB6_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB6_14
	jmp	.LBB6_15
.Lfunc_end6:
	.size	cast_numeric_uint64_uint8_avx2, .Lfunc_end6-cast_numeric_uint64_uint8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int64_uint8_avx2
.LCPI7_0:
	.byte	0                               # 0x0
	.byte	8                               # 0x8
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_int64_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint8_avx2,@function
cast_numeric_int64_uint8_avx2:          # @cast_numeric_int64_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB7_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB7_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB7_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB7_9
.LBB7_2:
	xor	ecx, ecx
.LBB7_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB7_5
	.p2align	4, 0x90
.LBB7_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB7_4
.LBB7_5:
	cmp	r8, 3
	jb	.LBB7_16
	.p2align	4, 0x90
.LBB7_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB7_6
	jmp	.LBB7_16
.LBB7_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB7_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI7_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB7_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB7_12
# %bb.13:
	test	r8b, 1
	je	.LBB7_15
.LBB7_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI7_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
.LBB7_15:
	cmp	rcx, r9
	jne	.LBB7_3
.LBB7_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB7_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB7_14
	jmp	.LBB7_15
.Lfunc_end7:
	.size	cast_numeric_int64_uint8_avx2, .Lfunc_end7-cast_numeric_int64_uint8_avx2
                                        # -- End function
	.globl	cast_numeric_float32_uint8_avx2 # -- Begin function cast_numeric_float32_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint8_avx2,@function
cast_numeric_float32_uint8_avx2:        # @cast_numeric_float32_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB8_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB8_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB8_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB8_9
.LBB8_2:
	xor	ecx, ecx
.LBB8_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB8_5
	.p2align	4, 0x90
.LBB8_4:                                # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB8_4
.LBB8_5:
	cmp	r8, 3
	jb	.LBB8_16
	.p2align	4, 0x90
.LBB8_6:                                # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	vcvttss2si	eax, dword ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	vcvttss2si	eax, dword ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	vcvttss2si	eax, dword ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB8_6
	jmp	.LBB8_16
.LBB8_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB8_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB8_12:                               # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 32]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 64]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpackuswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpackuswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 160]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 192]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdi + 4*rdx + 224]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpackuswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpackuswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx + 32], ymm0
	add	rdx, 64
	add	rax, 2
	jne	.LBB8_12
# %bb.13:
	test	r8b, 1
	je	.LBB8_15
.LBB8_14:
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vinserti128	ymm2, ymm2, xmm3, 1
	vpackuswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpackuswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
.LBB8_15:
	cmp	rcx, r9
	jne	.LBB8_3
.LBB8_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB8_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB8_14
	jmp	.LBB8_15
.Lfunc_end8:
	.size	cast_numeric_float32_uint8_avx2, .Lfunc_end8-cast_numeric_float32_uint8_avx2
                                        # -- End function
	.globl	cast_numeric_float64_uint8_avx2 # -- Begin function cast_numeric_float64_uint8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint8_avx2,@function
cast_numeric_float64_uint8_avx2:        # @cast_numeric_float64_uint8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB9_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB9_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB9_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB9_9
.LBB9_2:
	xor	ecx, ecx
.LBB9_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB9_5
	.p2align	4, 0x90
.LBB9_4:                                # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB9_4
.LBB9_5:
	cmp	r8, 3
	jb	.LBB9_16
	.p2align	4, 0x90
.LBB9_6:                                # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB9_6
	jmp	.LBB9_16
.LBB9_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB9_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB9_12:                               # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vpackusdw	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vpackuswb	xmm0, xmm0, xmm0
	vpackusdw	xmm1, xmm1, xmm1
	vpackuswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 64]
	vpackusdw	xmm1, xmm1, xmm1
	vpackuswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 96]
	vpackusdw	xmm2, xmm2, xmm2
	vpackuswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 160]
	vpackusdw	xmm0, xmm0, xmm0
	vpackuswb	xmm0, xmm0, xmm0
	vpackusdw	xmm1, xmm1, xmm1
	vpackuswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 192]
	vpackusdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 224]
	vpackuswb	xmm1, xmm1, xmm1
	vpackusdw	xmm2, xmm2, xmm2
	vpackuswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm0
	add	rdx, 32
	add	rax, 2
	jne	.LBB9_12
# %bb.13:
	test	r8b, 1
	je	.LBB9_15
.LBB9_14:
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vpackusdw	xmm0, xmm0, xmm0
	vpackuswb	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vpackusdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vpackuswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vpackusdw	xmm1, xmm2, xmm2
	vpackuswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 96]
	vpackusdw	xmm2, xmm2, xmm2
	vpackuswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
.LBB9_15:
	cmp	rcx, r9
	jne	.LBB9_3
.LBB9_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB9_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB9_14
	jmp	.LBB9_15
.Lfunc_end9:
	.size	cast_numeric_float64_uint8_avx2, .Lfunc_end9-cast_numeric_float64_uint8_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_int8_avx2    # -- Begin function cast_numeric_uint8_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int8_avx2,@function
cast_numeric_uint8_int8_avx2:           # @cast_numeric_uint8_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB10_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 127
	jbe	.LBB10_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB10_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB10_9
.LBB10_2:
	xor	ecx, ecx
.LBB10_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB10_5
	.p2align	4, 0x90
.LBB10_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB10_4
.LBB10_5:
	cmp	r8, 3
	jb	.LBB10_16
	.p2align	4, 0x90
.LBB10_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB10_6
	jmp	.LBB10_16
.LBB10_9:
	mov	ecx, r9d
	and	ecx, -128
	lea	rax, [rcx - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB10_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB10_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 224]
	vmovups	ymmword ptr [rsi + rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + rdx + 224], ymm3
	add	rdx, 256
	add	rax, 2
	jne	.LBB10_12
# %bb.13:
	test	r8b, 1
	je	.LBB10_15
.LBB10_14:
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
.LBB10_15:
	cmp	rcx, r9
	jne	.LBB10_3
.LBB10_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB10_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB10_14
	jmp	.LBB10_15
.Lfunc_end10:
	.size	cast_numeric_uint8_int8_avx2, .Lfunc_end10-cast_numeric_uint8_int8_avx2
                                        # -- End function
	.globl	cast_numeric_int8_int8_avx2     # -- Begin function cast_numeric_int8_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_int8_avx2,@function
cast_numeric_int8_int8_avx2:            # @cast_numeric_int8_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB11_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 127
	jbe	.LBB11_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB11_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB11_9
.LBB11_2:
	xor	ecx, ecx
.LBB11_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB11_5
	.p2align	4, 0x90
.LBB11_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB11_4
.LBB11_5:
	cmp	r8, 3
	jb	.LBB11_16
	.p2align	4, 0x90
.LBB11_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB11_6
	jmp	.LBB11_16
.LBB11_9:
	mov	ecx, r9d
	and	ecx, -128
	lea	rax, [rcx - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB11_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB11_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 224]
	vmovups	ymmword ptr [rsi + rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + rdx + 224], ymm3
	add	rdx, 256
	add	rax, 2
	jne	.LBB11_12
# %bb.13:
	test	r8b, 1
	je	.LBB11_15
.LBB11_14:
	vmovups	ymm0, ymmword ptr [rdi + rdx]
	vmovups	ymm1, ymmword ptr [rdi + rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + rdx + 96]
	vmovups	ymmword ptr [rsi + rdx], ymm0
	vmovups	ymmword ptr [rsi + rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + rdx + 96], ymm3
.LBB11_15:
	cmp	rcx, r9
	jne	.LBB11_3
.LBB11_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB11_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB11_14
	jmp	.LBB11_15
.Lfunc_end11:
	.size	cast_numeric_int8_int8_avx2, .Lfunc_end11-cast_numeric_int8_int8_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_uint16_int8_avx2
.LCPI12_0:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	cast_numeric_uint16_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int8_avx2,@function
cast_numeric_uint16_int8_avx2:          # @cast_numeric_uint16_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB12_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB12_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB12_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB12_9
.LBB12_2:
	xor	ecx, ecx
.LBB12_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB12_5
	.p2align	4, 0x90
.LBB12_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB12_4
.LBB12_5:
	cmp	r8, 3
	jb	.LBB12_16
	.p2align	4, 0x90
.LBB12_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 2*rcx + 2]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 2*rcx + 4]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 2*rcx + 6]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB12_6
	jmp	.LBB12_16
.LBB12_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB12_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI12_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	.p2align	4, 0x90
.LBB12_12:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 112], xmm4
	sub	rdx, -128
	add	rax, 2
	jne	.LBB12_12
# %bb.13:
	test	r8b, 1
	je	.LBB12_15
.LBB12_14:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI12_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm0
.LBB12_15:
	cmp	rcx, r9
	jne	.LBB12_3
.LBB12_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB12_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB12_14
	jmp	.LBB12_15
.Lfunc_end12:
	.size	cast_numeric_uint16_int8_avx2, .Lfunc_end12-cast_numeric_uint16_int8_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_int16_int8_avx2
.LCPI13_0:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	cast_numeric_int16_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_int8_avx2,@function
cast_numeric_int16_int8_avx2:           # @cast_numeric_int16_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB13_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB13_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB13_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB13_9
.LBB13_2:
	xor	ecx, ecx
.LBB13_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB13_5
	.p2align	4, 0x90
.LBB13_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB13_4
.LBB13_5:
	cmp	r8, 3
	jb	.LBB13_16
	.p2align	4, 0x90
.LBB13_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 2*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 2*rcx + 2]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 2*rcx + 4]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 2*rcx + 6]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB13_6
	jmp	.LBB13_16
.LBB13_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB13_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI13_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	.p2align	4, 0x90
.LBB13_12:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdi + 2*rdx + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rsi + rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 112], xmm4
	sub	rdx, -128
	add	rax, 2
	jne	.LBB13_12
# %bb.13:
	test	r8b, 1
	je	.LBB13_15
.LBB13_14:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI13_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdi + 2*rdx]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdi + 2*rdx + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdi + 2*rdx + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdi + 2*rdx + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + rdx + 48], xmm0
.LBB13_15:
	cmp	rcx, r9
	jne	.LBB13_3
.LBB13_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB13_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB13_14
	jmp	.LBB13_15
.Lfunc_end13:
	.size	cast_numeric_int16_int8_avx2, .Lfunc_end13-cast_numeric_int16_int8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_int8_avx2
.LCPI14_0:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
	.byte	8                               # 0x8
	.byte	12                              # 0xc
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_uint32_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int8_avx2,@function
cast_numeric_uint32_int8_avx2:          # @cast_numeric_uint32_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB14_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB14_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB14_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB14_9
.LBB14_2:
	xor	ecx, ecx
.LBB14_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB14_5
	.p2align	4, 0x90
.LBB14_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB14_4
.LBB14_5:
	cmp	r8, 3
	jb	.LBB14_16
	.p2align	4, 0x90
.LBB14_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB14_6
	jmp	.LBB14_16
.LBB14_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB14_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI14_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB14_12:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx + 32], ymm1
	add	rdx, 64
	add	rax, 2
	jne	.LBB14_12
# %bb.13:
	test	r8b, 1
	je	.LBB14_15
.LBB14_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI14_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
.LBB14_15:
	cmp	rcx, r9
	jne	.LBB14_3
.LBB14_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB14_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB14_14
	jmp	.LBB14_15
.Lfunc_end14:
	.size	cast_numeric_uint32_int8_avx2, .Lfunc_end14-cast_numeric_uint32_int8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int32_int8_avx2
.LCPI15_0:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
	.byte	8                               # 0x8
	.byte	12                              # 0xc
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_int32_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_int8_avx2,@function
cast_numeric_int32_int8_avx2:           # @cast_numeric_int32_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB15_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB15_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB15_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB15_9
.LBB15_2:
	xor	ecx, ecx
.LBB15_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB15_5
	.p2align	4, 0x90
.LBB15_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB15_4
.LBB15_5:
	cmp	r8, 3
	jb	.LBB15_16
	.p2align	4, 0x90
.LBB15_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB15_6
	jmp	.LBB15_16
.LBB15_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB15_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI15_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB15_12:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm1
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx + 32], ymm1
	add	rdx, 64
	add	rax, 2
	jne	.LBB15_12
# %bb.13:
	test	r8b, 1
	je	.LBB15_15
.LBB15_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI15_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdi + 4*rdx + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdi + 4*rdx + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
.LBB15_15:
	cmp	rcx, r9
	jne	.LBB15_3
.LBB15_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB15_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB15_14
	jmp	.LBB15_15
.Lfunc_end15:
	.size	cast_numeric_int32_int8_avx2, .Lfunc_end15-cast_numeric_int32_int8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint64_int8_avx2
.LCPI16_0:
	.byte	0                               # 0x0
	.byte	8                               # 0x8
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_uint64_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int8_avx2,@function
cast_numeric_uint64_int8_avx2:          # @cast_numeric_uint64_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB16_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB16_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB16_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB16_9
.LBB16_2:
	xor	ecx, ecx
.LBB16_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB16_5
	.p2align	4, 0x90
.LBB16_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB16_4
.LBB16_5:
	cmp	r8, 3
	jb	.LBB16_16
	.p2align	4, 0x90
.LBB16_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB16_6
	jmp	.LBB16_16
.LBB16_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB16_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI16_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB16_12:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB16_12
# %bb.13:
	test	r8b, 1
	je	.LBB16_15
.LBB16_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI16_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
.LBB16_15:
	cmp	rcx, r9
	jne	.LBB16_3
.LBB16_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB16_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB16_14
	jmp	.LBB16_15
.Lfunc_end16:
	.size	cast_numeric_uint64_int8_avx2, .Lfunc_end16-cast_numeric_uint64_int8_avx2
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int64_int8_avx2
.LCPI17_0:
	.byte	0                               # 0x0
	.byte	8                               # 0x8
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_int64_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_int8_avx2,@function
cast_numeric_int64_int8_avx2:           # @cast_numeric_int64_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB17_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB17_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB17_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB17_9
.LBB17_2:
	xor	ecx, ecx
.LBB17_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB17_5
	.p2align	4, 0x90
.LBB17_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB17_4
.LBB17_5:
	cmp	r8, 3
	jb	.LBB17_16
	.p2align	4, 0x90
.LBB17_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	movzx	eax, byte ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	movzx	eax, byte ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	movzx	eax, byte ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB17_6
	jmp	.LBB17_16
.LBB17_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB17_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI17_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB17_12:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 128]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 144]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 160]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB17_12
# %bb.13:
	test	r8b, 1
	je	.LBB17_15
.LBB17_14:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI17_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
.LBB17_15:
	cmp	rcx, r9
	jne	.LBB17_3
.LBB17_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB17_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB17_14
	jmp	.LBB17_15
.Lfunc_end17:
	.size	cast_numeric_int64_int8_avx2, .Lfunc_end17-cast_numeric_int64_int8_avx2
                                        # -- End function
	.globl	cast_numeric_float32_int8_avx2  # -- Begin function cast_numeric_float32_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_int8_avx2,@function
cast_numeric_float32_int8_avx2:         # @cast_numeric_float32_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB18_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB18_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB18_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB18_9
.LBB18_2:
	xor	ecx, ecx
.LBB18_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB18_5
	.p2align	4, 0x90
.LBB18_4:                               # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB18_4
.LBB18_5:
	cmp	r8, 3
	jb	.LBB18_16
	.p2align	4, 0x90
.LBB18_6:                               # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	vcvttss2si	eax, dword ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	vcvttss2si	eax, dword ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	vcvttss2si	eax, dword ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB18_6
	jmp	.LBB18_16
.LBB18_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB18_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB18_12:                              # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 32]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 64]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpacksswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpacksswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 160]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 192]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdi + 4*rdx + 224]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpacksswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpacksswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx + 32], ymm0
	add	rdx, 64
	add	rax, 2
	jne	.LBB18_12
# %bb.13:
	test	r8b, 1
	je	.LBB18_15
.LBB18_14:
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vinserti128	ymm2, ymm2, xmm3, 1
	vpacksswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpacksswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + rdx], ymm0
.LBB18_15:
	cmp	rcx, r9
	jne	.LBB18_3
.LBB18_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB18_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB18_14
	jmp	.LBB18_15
.Lfunc_end18:
	.size	cast_numeric_float32_int8_avx2, .Lfunc_end18-cast_numeric_float32_int8_avx2
                                        # -- End function
	.globl	cast_numeric_float64_int8_avx2  # -- Begin function cast_numeric_float64_int8_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_int8_avx2,@function
cast_numeric_float64_int8_avx2:         # @cast_numeric_float64_int8_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB19_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB19_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB19_9
# %bb.8:
	lea	rax, [rsi + r9]
	cmp	rax, rdi
	jbe	.LBB19_9
.LBB19_2:
	xor	ecx, ecx
.LBB19_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB19_5
	.p2align	4, 0x90
.LBB19_4:                               # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB19_4
.LBB19_5:
	cmp	r8, 3
	jb	.LBB19_16
	.p2align	4, 0x90
.LBB19_6:                               # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB19_6
	jmp	.LBB19_16
.LBB19_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB19_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB19_12:                              # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vpackssdw	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vpacksswb	xmm0, xmm0, xmm0
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 64]
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 96]
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 160]
	vpackssdw	xmm0, xmm0, xmm0
	vpacksswb	xmm0, xmm0, xmm0
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 192]
	vpackssdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 224]
	vpacksswb	xmm1, xmm1, xmm1
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rsi + rdx + 16], xmm0
	add	rdx, 32
	add	rax, 2
	jne	.LBB19_12
# %bb.13:
	test	r8b, 1
	je	.LBB19_15
.LBB19_14:
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vpackssdw	xmm0, xmm0, xmm0
	vpacksswb	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vpackssdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vpacksswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vpackssdw	xmm1, xmm2, xmm2
	vpacksswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 96]
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rsi + rdx], xmm0
.LBB19_15:
	cmp	rcx, r9
	jne	.LBB19_3
.LBB19_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB19_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB19_14
	jmp	.LBB19_15
.Lfunc_end19:
	.size	cast_numeric_float64_int8_avx2, .Lfunc_end19-cast_numeric_float64_int8_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_uint16_avx2  # -- Begin function cast_numeric_uint8_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint16_avx2,@function
cast_numeric_uint8_uint16_avx2:         # @cast_numeric_uint8_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB20_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB20_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB20_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB20_9
.LBB20_2:
	xor	ecx, ecx
.LBB20_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB20_5
	.p2align	4, 0x90
.LBB20_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB20_4
.LBB20_5:
	cmp	r8, 3
	jb	.LBB20_16
	.p2align	4, 0x90
.LBB20_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB20_6
	jmp	.LBB20_16
.LBB20_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB20_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB20_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbw	ymm0, xmmword ptr [rdi + rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdi + rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdi + rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdi + rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vpmovzxbw	ymm0, xmmword ptr [rdi + rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdi + rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdi + rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdi + rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB20_12
# %bb.13:
	test	r8b, 1
	je	.LBB20_15
.LBB20_14:
	vpmovzxbw	ymm0, xmmword ptr [rdi + rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdi + rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdi + rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdi + rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB20_15:
	cmp	rcx, r9
	jne	.LBB20_3
.LBB20_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB20_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB20_14
	jmp	.LBB20_15
.Lfunc_end20:
	.size	cast_numeric_uint8_uint16_avx2, .Lfunc_end20-cast_numeric_uint8_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_int8_uint16_avx2   # -- Begin function cast_numeric_int8_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint16_avx2,@function
cast_numeric_int8_uint16_avx2:          # @cast_numeric_int8_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB21_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB21_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB21_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB21_9
.LBB21_2:
	xor	ecx, ecx
.LBB21_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB21_5
	.p2align	4, 0x90
.LBB21_4:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB21_4
.LBB21_5:
	cmp	r8, 3
	jb	.LBB21_16
	.p2align	4, 0x90
.LBB21_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movsx	eax, byte ptr [rdi + rcx + 1]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movsx	eax, byte ptr [rdi + rcx + 2]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movsx	eax, byte ptr [rdi + rcx + 3]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB21_6
	jmp	.LBB21_16
.LBB21_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB21_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB21_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbw	ymm0, xmmword ptr [rdi + rdx]
	vpmovsxbw	ymm1, xmmword ptr [rdi + rdx + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdi + rdx + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdi + rdx + 48]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vpmovsxbw	ymm0, xmmword ptr [rdi + rdx + 64]
	vpmovsxbw	ymm1, xmmword ptr [rdi + rdx + 80]
	vpmovsxbw	ymm2, xmmword ptr [rdi + rdx + 96]
	vpmovsxbw	ymm3, xmmword ptr [rdi + rdx + 112]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB21_12
# %bb.13:
	test	r8b, 1
	je	.LBB21_15
.LBB21_14:
	vpmovsxbw	ymm0, xmmword ptr [rdi + rdx]
	vpmovsxbw	ymm1, xmmword ptr [rdi + rdx + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdi + rdx + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdi + rdx + 48]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB21_15:
	cmp	rcx, r9
	jne	.LBB21_3
.LBB21_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB21_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB21_14
	jmp	.LBB21_15
.Lfunc_end21:
	.size	cast_numeric_int8_uint16_avx2, .Lfunc_end21-cast_numeric_int8_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_uint16_avx2 # -- Begin function cast_numeric_uint16_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint16_avx2,@function
cast_numeric_uint16_uint16_avx2:        # @cast_numeric_uint16_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB22_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB22_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB22_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB22_9
.LBB22_2:
	xor	ecx, ecx
.LBB22_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB22_5
	.p2align	4, 0x90
.LBB22_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB22_4
.LBB22_5:
	cmp	r8, 3
	jb	.LBB22_16
	.p2align	4, 0x90
.LBB22_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movzx	eax, word ptr [rdi + 2*rcx + 2]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movzx	eax, word ptr [rdi + 2*rcx + 4]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movzx	eax, word ptr [rdi + 2*rcx + 6]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB22_6
	jmp	.LBB22_16
.LBB22_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB22_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB22_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 224]
	vmovups	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB22_12
# %bb.13:
	test	r8b, 1
	je	.LBB22_15
.LBB22_14:
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB22_15:
	cmp	rcx, r9
	jne	.LBB22_3
.LBB22_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB22_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB22_14
	jmp	.LBB22_15
.Lfunc_end22:
	.size	cast_numeric_uint16_uint16_avx2, .Lfunc_end22-cast_numeric_uint16_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_int16_uint16_avx2  # -- Begin function cast_numeric_int16_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint16_avx2,@function
cast_numeric_int16_uint16_avx2:         # @cast_numeric_int16_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB23_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB23_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB23_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB23_9
.LBB23_2:
	xor	ecx, ecx
.LBB23_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB23_5
	.p2align	4, 0x90
.LBB23_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB23_4
.LBB23_5:
	cmp	r8, 3
	jb	.LBB23_16
	.p2align	4, 0x90
.LBB23_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movzx	eax, word ptr [rdi + 2*rcx + 2]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movzx	eax, word ptr [rdi + 2*rcx + 4]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movzx	eax, word ptr [rdi + 2*rcx + 6]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB23_6
	jmp	.LBB23_16
.LBB23_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB23_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB23_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 224]
	vmovups	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB23_12
# %bb.13:
	test	r8b, 1
	je	.LBB23_15
.LBB23_14:
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB23_15:
	cmp	rcx, r9
	jne	.LBB23_3
.LBB23_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB23_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB23_14
	jmp	.LBB23_15
.Lfunc_end23:
	.size	cast_numeric_int16_uint16_avx2, .Lfunc_end23-cast_numeric_int16_uint16_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_uint32_uint16_avx2
.LCPI24_0:
	.byte	0                               # 0x0
	.byte	1                               # 0x1
	.byte	4                               # 0x4
	.byte	5                               # 0x5
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	14                              # 0xe
	.byte	15                              # 0xf
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
	.text
	.globl	cast_numeric_uint32_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint16_avx2,@function
cast_numeric_uint32_uint16_avx2:        # @cast_numeric_uint32_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB24_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB24_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB24_10
.LBB24_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB24_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI24_0] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	.p2align	4, 0x90
.LBB24_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 128]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 160]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 192]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 112], xmm4
	add	rdx, 64
	add	rax, 2
	jne	.LBB24_6
# %bb.7:
	test	r8b, 1
	je	.LBB24_9
.LBB24_8:
	vmovdqu	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI24_0] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
.LBB24_9:
	cmp	rcx, r9
	je	.LBB24_11
	.p2align	4, 0x90
.LBB24_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB24_10
.LBB24_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB24_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB24_8
	jmp	.LBB24_9
.Lfunc_end24:
	.size	cast_numeric_uint32_uint16_avx2, .Lfunc_end24-cast_numeric_uint32_uint16_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_int32_uint16_avx2
.LCPI25_0:
	.byte	0                               # 0x0
	.byte	1                               # 0x1
	.byte	4                               # 0x4
	.byte	5                               # 0x5
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	14                              # 0xe
	.byte	15                              # 0xf
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
	.text
	.globl	cast_numeric_int32_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint16_avx2,@function
cast_numeric_int32_uint16_avx2:         # @cast_numeric_int32_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB25_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB25_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB25_10
.LBB25_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB25_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI25_0] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	.p2align	4, 0x90
.LBB25_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 128]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 160]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 192]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 112], xmm4
	add	rdx, 64
	add	rax, 2
	jne	.LBB25_6
# %bb.7:
	test	r8b, 1
	je	.LBB25_9
.LBB25_8:
	vmovdqu	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI25_0] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
.LBB25_9:
	cmp	rcx, r9
	je	.LBB25_11
	.p2align	4, 0x90
.LBB25_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB25_10
.LBB25_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB25_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB25_8
	jmp	.LBB25_9
.Lfunc_end25:
	.size	cast_numeric_int32_uint16_avx2, .Lfunc_end25-cast_numeric_int32_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_uint64_uint16_avx2 # -- Begin function cast_numeric_uint64_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint16_avx2,@function
cast_numeric_uint64_uint16_avx2:        # @cast_numeric_uint64_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB26_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB26_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB26_10
.LBB26_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB26_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vpxor	xmm0, xmm0, xmm0
	.p2align	4, 0x90
.LBB26_6:                               # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB26_6
# %bb.7:
	test	r8b, 1
	je	.LBB26_9
.LBB26_8:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
.LBB26_9:
	cmp	rcx, r9
	je	.LBB26_11
	.p2align	4, 0x90
.LBB26_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB26_10
.LBB26_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB26_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB26_8
	jmp	.LBB26_9
.Lfunc_end26:
	.size	cast_numeric_uint64_uint16_avx2, .Lfunc_end26-cast_numeric_uint64_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_int64_uint16_avx2  # -- Begin function cast_numeric_int64_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint16_avx2,@function
cast_numeric_int64_uint16_avx2:         # @cast_numeric_int64_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB27_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB27_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB27_10
.LBB27_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB27_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vpxor	xmm0, xmm0, xmm0
	.p2align	4, 0x90
.LBB27_6:                               # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB27_6
# %bb.7:
	test	r8b, 1
	je	.LBB27_9
.LBB27_8:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
.LBB27_9:
	cmp	rcx, r9
	je	.LBB27_11
	.p2align	4, 0x90
.LBB27_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB27_10
.LBB27_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB27_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB27_8
	jmp	.LBB27_9
.Lfunc_end27:
	.size	cast_numeric_int64_uint16_avx2, .Lfunc_end27-cast_numeric_int64_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_float32_uint16_avx2 # -- Begin function cast_numeric_float32_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint16_avx2,@function
cast_numeric_float32_uint16_avx2:       # @cast_numeric_float32_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB28_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB28_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB28_10
.LBB28_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB28_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB28_6:                               # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vpackusdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vextracti128	xmm2, ymm1, 1
	vpackusdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vextracti128	xmm3, ymm2, 1
	vpackusdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vextracti128	xmm4, ymm3, 1
	vpackusdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vextracti128	xmm1, ymm0, 1
	vpackusdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vextracti128	xmm2, ymm1, 1
	vpackusdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vextracti128	xmm3, ymm2, 1
	vpackusdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vextracti128	xmm4, ymm3, 1
	vpackusdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rsi + 2*rdx + 64], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 80], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 96], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 112], xmm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB28_6
# %bb.7:
	test	r8b, 1
	je	.LBB28_9
.LBB28_8:
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vpackusdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vextracti128	xmm2, ymm1, 1
	vpackusdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vextracti128	xmm3, ymm2, 1
	vpackusdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vextracti128	xmm4, ymm3, 1
	vpackusdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
.LBB28_9:
	cmp	rcx, r9
	je	.LBB28_11
	.p2align	4, 0x90
.LBB28_10:                              # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB28_10
.LBB28_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB28_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB28_8
	jmp	.LBB28_9
.Lfunc_end28:
	.size	cast_numeric_float32_uint16_avx2, .Lfunc_end28-cast_numeric_float32_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_float64_uint16_avx2 # -- Begin function cast_numeric_float64_uint16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint16_avx2,@function
cast_numeric_float64_uint16_avx2:       # @cast_numeric_float64_uint16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB29_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB29_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB29_10
.LBB29_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB29_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB29_6:                               # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackusdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackusdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 160]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 192]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 224]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackusdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackusdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm0
	add	rdx, 32
	add	rax, 2
	jne	.LBB29_6
# %bb.7:
	test	r8b, 1
	je	.LBB29_9
.LBB29_8:
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackusdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackusdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
.LBB29_9:
	cmp	rcx, r9
	je	.LBB29_11
	.p2align	4, 0x90
.LBB29_10:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB29_10
.LBB29_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB29_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB29_8
	jmp	.LBB29_9
.Lfunc_end29:
	.size	cast_numeric_float64_uint16_avx2, .Lfunc_end29-cast_numeric_float64_uint16_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_int16_avx2   # -- Begin function cast_numeric_uint8_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int16_avx2,@function
cast_numeric_uint8_int16_avx2:          # @cast_numeric_uint8_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB30_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB30_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB30_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB30_9
.LBB30_2:
	xor	ecx, ecx
.LBB30_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB30_5
	.p2align	4, 0x90
.LBB30_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB30_4
.LBB30_5:
	cmp	r8, 3
	jb	.LBB30_16
	.p2align	4, 0x90
.LBB30_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB30_6
	jmp	.LBB30_16
.LBB30_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB30_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB30_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbw	ymm0, xmmword ptr [rdi + rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdi + rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdi + rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdi + rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vpmovzxbw	ymm0, xmmword ptr [rdi + rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdi + rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdi + rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdi + rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB30_12
# %bb.13:
	test	r8b, 1
	je	.LBB30_15
.LBB30_14:
	vpmovzxbw	ymm0, xmmword ptr [rdi + rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdi + rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdi + rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdi + rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB30_15:
	cmp	rcx, r9
	jne	.LBB30_3
.LBB30_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB30_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB30_14
	jmp	.LBB30_15
.Lfunc_end30:
	.size	cast_numeric_uint8_int16_avx2, .Lfunc_end30-cast_numeric_uint8_int16_avx2
                                        # -- End function
	.globl	cast_numeric_int8_int16_avx2    # -- Begin function cast_numeric_int8_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_int16_avx2,@function
cast_numeric_int8_int16_avx2:           # @cast_numeric_int8_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB31_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB31_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB31_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB31_9
.LBB31_2:
	xor	ecx, ecx
.LBB31_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB31_5
	.p2align	4, 0x90
.LBB31_4:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB31_4
.LBB31_5:
	cmp	r8, 3
	jb	.LBB31_16
	.p2align	4, 0x90
.LBB31_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movsx	eax, byte ptr [rdi + rcx + 1]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movsx	eax, byte ptr [rdi + rcx + 2]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movsx	eax, byte ptr [rdi + rcx + 3]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB31_6
	jmp	.LBB31_16
.LBB31_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB31_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB31_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbw	ymm0, xmmword ptr [rdi + rdx]
	vpmovsxbw	ymm1, xmmword ptr [rdi + rdx + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdi + rdx + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdi + rdx + 48]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vpmovsxbw	ymm0, xmmword ptr [rdi + rdx + 64]
	vpmovsxbw	ymm1, xmmword ptr [rdi + rdx + 80]
	vpmovsxbw	ymm2, xmmword ptr [rdi + rdx + 96]
	vpmovsxbw	ymm3, xmmword ptr [rdi + rdx + 112]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB31_12
# %bb.13:
	test	r8b, 1
	je	.LBB31_15
.LBB31_14:
	vpmovsxbw	ymm0, xmmword ptr [rdi + rdx]
	vpmovsxbw	ymm1, xmmword ptr [rdi + rdx + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdi + rdx + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdi + rdx + 48]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB31_15:
	cmp	rcx, r9
	jne	.LBB31_3
.LBB31_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB31_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB31_14
	jmp	.LBB31_15
.Lfunc_end31:
	.size	cast_numeric_int8_int16_avx2, .Lfunc_end31-cast_numeric_int8_int16_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_int16_avx2  # -- Begin function cast_numeric_uint16_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int16_avx2,@function
cast_numeric_uint16_int16_avx2:         # @cast_numeric_uint16_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB32_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB32_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB32_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB32_9
.LBB32_2:
	xor	ecx, ecx
.LBB32_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB32_5
	.p2align	4, 0x90
.LBB32_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB32_4
.LBB32_5:
	cmp	r8, 3
	jb	.LBB32_16
	.p2align	4, 0x90
.LBB32_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movzx	eax, word ptr [rdi + 2*rcx + 2]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movzx	eax, word ptr [rdi + 2*rcx + 4]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movzx	eax, word ptr [rdi + 2*rcx + 6]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB32_6
	jmp	.LBB32_16
.LBB32_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB32_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB32_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 224]
	vmovups	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB32_12
# %bb.13:
	test	r8b, 1
	je	.LBB32_15
.LBB32_14:
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB32_15:
	cmp	rcx, r9
	jne	.LBB32_3
.LBB32_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB32_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB32_14
	jmp	.LBB32_15
.Lfunc_end32:
	.size	cast_numeric_uint16_int16_avx2, .Lfunc_end32-cast_numeric_uint16_int16_avx2
                                        # -- End function
	.globl	cast_numeric_int16_int16_avx2   # -- Begin function cast_numeric_int16_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_int16_avx2,@function
cast_numeric_int16_int16_avx2:          # @cast_numeric_int16_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB33_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 63
	jbe	.LBB33_2
# %bb.7:
	lea	rax, [rdi + 2*r9]
	cmp	rax, rsi
	jbe	.LBB33_9
# %bb.8:
	lea	rax, [rsi + 2*r9]
	cmp	rax, rdi
	jbe	.LBB33_9
.LBB33_2:
	xor	ecx, ecx
.LBB33_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB33_5
	.p2align	4, 0x90
.LBB33_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB33_4
.LBB33_5:
	cmp	r8, 3
	jb	.LBB33_16
	.p2align	4, 0x90
.LBB33_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	movzx	eax, word ptr [rdi + 2*rcx + 2]
	mov	word ptr [rsi + 2*rcx + 2], ax
	movzx	eax, word ptr [rdi + 2*rcx + 4]
	mov	word ptr [rsi + 2*rcx + 4], ax
	movzx	eax, word ptr [rdi + 2*rcx + 6]
	mov	word ptr [rsi + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB33_6
	jmp	.LBB33_16
.LBB33_9:
	mov	ecx, r9d
	and	ecx, -64
	lea	rax, [rcx - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB33_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB33_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 224]
	vmovups	ymmword ptr [rsi + 2*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 224], ymm3
	sub	rdx, -128
	add	rax, 2
	jne	.LBB33_12
# %bb.13:
	test	r8b, 1
	je	.LBB33_15
.LBB33_14:
	vmovups	ymm0, ymmword ptr [rdi + 2*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 2*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 2*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 2*rdx + 96]
	vmovups	ymmword ptr [rsi + 2*rdx], ymm0
	vmovups	ymmword ptr [rsi + 2*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 2*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 2*rdx + 96], ymm3
.LBB33_15:
	cmp	rcx, r9
	jne	.LBB33_3
.LBB33_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB33_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB33_14
	jmp	.LBB33_15
.Lfunc_end33:
	.size	cast_numeric_int16_int16_avx2, .Lfunc_end33-cast_numeric_int16_int16_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_uint32_int16_avx2
.LCPI34_0:
	.byte	0                               # 0x0
	.byte	1                               # 0x1
	.byte	4                               # 0x4
	.byte	5                               # 0x5
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	14                              # 0xe
	.byte	15                              # 0xf
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
	.text
	.globl	cast_numeric_uint32_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int16_avx2,@function
cast_numeric_uint32_int16_avx2:         # @cast_numeric_uint32_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB34_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB34_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB34_10
.LBB34_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB34_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI34_0] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	.p2align	4, 0x90
.LBB34_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 128]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 160]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 192]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 112], xmm4
	add	rdx, 64
	add	rax, 2
	jne	.LBB34_6
# %bb.7:
	test	r8b, 1
	je	.LBB34_9
.LBB34_8:
	vmovdqu	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI34_0] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
.LBB34_9:
	cmp	rcx, r9
	je	.LBB34_11
	.p2align	4, 0x90
.LBB34_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB34_10
.LBB34_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB34_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB34_8
	jmp	.LBB34_9
.Lfunc_end34:
	.size	cast_numeric_uint32_int16_avx2, .Lfunc_end34-cast_numeric_uint32_int16_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function cast_numeric_int32_int16_avx2
.LCPI35_0:
	.byte	0                               # 0x0
	.byte	1                               # 0x1
	.byte	4                               # 0x4
	.byte	5                               # 0x5
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	8                               # 0x8
	.byte	9                               # 0x9
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	12                              # 0xc
	.byte	13                              # 0xd
	.byte	14                              # 0xe
	.byte	15                              # 0xf
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
	.text
	.globl	cast_numeric_int32_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_int16_avx2,@function
cast_numeric_int32_int16_avx2:          # @cast_numeric_int32_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB35_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB35_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB35_10
.LBB35_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB35_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI35_0] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	.p2align	4, 0x90
.LBB35_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 128]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 160]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 192]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx + 64], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 80], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 96], xmm3
	vmovdqu	xmmword ptr [rsi + 2*rdx + 112], xmm4
	add	rdx, 64
	add	rax, 2
	jne	.LBB35_6
# %bb.7:
	test	r8b, 1
	je	.LBB35_9
.LBB35_8:
	vmovdqu	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI35_0] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
.LBB35_9:
	cmp	rcx, r9
	je	.LBB35_11
	.p2align	4, 0x90
.LBB35_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB35_10
.LBB35_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB35_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB35_8
	jmp	.LBB35_9
.Lfunc_end35:
	.size	cast_numeric_int32_int16_avx2, .Lfunc_end35-cast_numeric_int32_int16_avx2
                                        # -- End function
	.globl	cast_numeric_uint64_int16_avx2  # -- Begin function cast_numeric_uint64_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int16_avx2,@function
cast_numeric_uint64_int16_avx2:         # @cast_numeric_uint64_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB36_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB36_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB36_10
.LBB36_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB36_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vpxor	xmm0, xmm0, xmm0
	.p2align	4, 0x90
.LBB36_6:                               # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB36_6
# %bb.7:
	test	r8b, 1
	je	.LBB36_9
.LBB36_8:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
.LBB36_9:
	cmp	rcx, r9
	je	.LBB36_11
	.p2align	4, 0x90
.LBB36_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB36_10
.LBB36_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB36_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB36_8
	jmp	.LBB36_9
.Lfunc_end36:
	.size	cast_numeric_uint64_int16_avx2, .Lfunc_end36-cast_numeric_uint64_int16_avx2
                                        # -- End function
	.globl	cast_numeric_int64_int16_avx2   # -- Begin function cast_numeric_int64_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_int16_avx2,@function
cast_numeric_int64_int16_avx2:          # @cast_numeric_int64_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB37_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB37_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB37_10
.LBB37_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB37_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vpxor	xmm0, xmm0, xmm0
	.p2align	4, 0x90
.LBB37_6:                               # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdi + 8*rdx + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB37_6
# %bb.7:
	test	r8b, 1
	je	.LBB37_9
.LBB37_8:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdi + 8*rdx], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdi + 8*rdx + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdi + 8*rdx + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdi + 8*rdx + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdi + 8*rdx + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdi + 8*rdx + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdi + 8*rdx + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
.LBB37_9:
	cmp	rcx, r9
	je	.LBB37_11
	.p2align	4, 0x90
.LBB37_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB37_10
.LBB37_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB37_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB37_8
	jmp	.LBB37_9
.Lfunc_end37:
	.size	cast_numeric_int64_int16_avx2, .Lfunc_end37-cast_numeric_int64_int16_avx2
                                        # -- End function
	.globl	cast_numeric_float32_int16_avx2 # -- Begin function cast_numeric_float32_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_int16_avx2,@function
cast_numeric_float32_int16_avx2:        # @cast_numeric_float32_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB38_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB38_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB38_10
.LBB38_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB38_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB38_6:                               # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rsi + 2*rdx + 64], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 80], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 96], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 112], xmm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB38_6
# %bb.7:
	test	r8b, 1
	je	.LBB38_9
.LBB38_8:
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rsi + 2*rdx], xmm0
	vmovdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 2*rdx + 32], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rdx + 48], xmm3
.LBB38_9:
	cmp	rcx, r9
	je	.LBB38_11
	.p2align	4, 0x90
.LBB38_10:                              # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB38_10
.LBB38_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB38_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB38_8
	jmp	.LBB38_9
.Lfunc_end38:
	.size	cast_numeric_float32_int16_avx2, .Lfunc_end38-cast_numeric_float32_int16_avx2
                                        # -- End function
	.globl	cast_numeric_float64_int16_avx2 # -- Begin function cast_numeric_float64_int16_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_int16_avx2,@function
cast_numeric_float64_int16_avx2:        # @cast_numeric_float64_int16_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB39_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB39_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB39_10
.LBB39_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB39_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB39_6:                               # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackssdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackssdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 160]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 192]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 224]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackssdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackssdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx + 32], ymm0
	add	rdx, 32
	add	rax, 2
	jne	.LBB39_6
# %bb.7:
	test	r8b, 1
	je	.LBB39_9
.LBB39_8:
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackssdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackssdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rsi + 2*rdx], ymm0
.LBB39_9:
	cmp	rcx, r9
	je	.LBB39_11
	.p2align	4, 0x90
.LBB39_10:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB39_10
.LBB39_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB39_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB39_8
	jmp	.LBB39_9
.Lfunc_end39:
	.size	cast_numeric_float64_int16_avx2, .Lfunc_end39-cast_numeric_float64_int16_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_uint32_avx2  # -- Begin function cast_numeric_uint8_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint32_avx2,@function
cast_numeric_uint8_uint32_avx2:         # @cast_numeric_uint8_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB40_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB40_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB40_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB40_9
.LBB40_2:
	xor	ecx, ecx
.LBB40_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB40_5
	.p2align	4, 0x90
.LBB40_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB40_4
.LBB40_5:
	cmp	r8, 3
	jb	.LBB40_16
	.p2align	4, 0x90
.LBB40_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB40_6
	jmp	.LBB40_16
.LBB40_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB40_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB40_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	ymm0, qword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovzxbd	ymm0, qword ptr [rdi + rdx + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB40_12
# %bb.13:
	test	r8b, 1
	je	.LBB40_15
.LBB40_14:
	vpmovzxbd	ymm0, qword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB40_15:
	cmp	rcx, r9
	jne	.LBB40_3
.LBB40_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB40_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB40_14
	jmp	.LBB40_15
.Lfunc_end40:
	.size	cast_numeric_uint8_uint32_avx2, .Lfunc_end40-cast_numeric_uint8_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_int8_uint32_avx2   # -- Begin function cast_numeric_int8_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint32_avx2,@function
cast_numeric_int8_uint32_avx2:          # @cast_numeric_int8_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB41_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB41_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB41_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB41_9
.LBB41_2:
	xor	ecx, ecx
.LBB41_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB41_5
	.p2align	4, 0x90
.LBB41_4:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB41_4
.LBB41_5:
	cmp	r8, 3
	jb	.LBB41_16
	.p2align	4, 0x90
.LBB41_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	movsx	eax, byte ptr [rdi + rcx + 1]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	movsx	eax, byte ptr [rdi + rcx + 2]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	movsx	eax, byte ptr [rdi + rcx + 3]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB41_6
	jmp	.LBB41_16
.LBB41_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB41_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB41_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm0, qword ptr [rdi + rdx]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 8]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 16]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 24]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovsxbd	ymm0, qword ptr [rdi + rdx + 32]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 40]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 48]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 56]
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB41_12
# %bb.13:
	test	r8b, 1
	je	.LBB41_15
.LBB41_14:
	vpmovsxbd	ymm0, qword ptr [rdi + rdx]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 8]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 16]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 24]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB41_15:
	cmp	rcx, r9
	jne	.LBB41_3
.LBB41_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB41_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB41_14
	jmp	.LBB41_15
.Lfunc_end41:
	.size	cast_numeric_int8_uint32_avx2, .Lfunc_end41-cast_numeric_int8_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_uint32_avx2 # -- Begin function cast_numeric_uint16_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint32_avx2,@function
cast_numeric_uint16_uint32_avx2:        # @cast_numeric_uint16_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB42_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB42_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB42_10
.LBB42_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB42_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB42_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB42_6
# %bb.7:
	test	r8b, 1
	je	.LBB42_9
.LBB42_8:
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB42_9:
	cmp	rcx, r9
	je	.LBB42_11
	.p2align	4, 0x90
.LBB42_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB42_10
.LBB42_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB42_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB42_8
	jmp	.LBB42_9
.Lfunc_end42:
	.size	cast_numeric_uint16_uint32_avx2, .Lfunc_end42-cast_numeric_uint16_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_int16_uint32_avx2  # -- Begin function cast_numeric_int16_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint32_avx2,@function
cast_numeric_int16_uint32_avx2:         # @cast_numeric_int16_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB43_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB43_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB43_10
.LBB43_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB43_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB43_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx + 64]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 80]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 96]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 112]
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB43_6
# %bb.7:
	test	r8b, 1
	je	.LBB43_9
.LBB43_8:
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB43_9:
	cmp	rcx, r9
	je	.LBB43_11
	.p2align	4, 0x90
.LBB43_10:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdi + 2*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB43_10
.LBB43_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB43_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB43_8
	jmp	.LBB43_9
.Lfunc_end43:
	.size	cast_numeric_int16_uint32_avx2, .Lfunc_end43-cast_numeric_int16_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_uint32_uint32_avx2 # -- Begin function cast_numeric_uint32_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint32_avx2,@function
cast_numeric_uint32_uint32_avx2:        # @cast_numeric_uint32_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB44_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB44_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB44_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB44_9
.LBB44_2:
	xor	ecx, ecx
.LBB44_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB44_5
	.p2align	4, 0x90
.LBB44_4:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB44_4
.LBB44_5:
	cmp	r8, 3
	jb	.LBB44_16
	.p2align	4, 0x90
.LBB44_6:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	mov	eax, dword ptr [rdi + 4*rcx + 4]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	mov	eax, dword ptr [rdi + 4*rcx + 8]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	mov	eax, dword ptr [rdi + 4*rcx + 12]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB44_6
	jmp	.LBB44_16
.LBB44_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB44_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB44_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB44_12
# %bb.13:
	test	r8b, 1
	je	.LBB44_15
.LBB44_14:
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB44_15:
	cmp	rcx, r9
	jne	.LBB44_3
.LBB44_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB44_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB44_14
	jmp	.LBB44_15
.Lfunc_end44:
	.size	cast_numeric_uint32_uint32_avx2, .Lfunc_end44-cast_numeric_uint32_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_int32_uint32_avx2  # -- Begin function cast_numeric_int32_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint32_avx2,@function
cast_numeric_int32_uint32_avx2:         # @cast_numeric_int32_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB45_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB45_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB45_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB45_9
.LBB45_2:
	xor	ecx, ecx
.LBB45_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB45_5
	.p2align	4, 0x90
.LBB45_4:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB45_4
.LBB45_5:
	cmp	r8, 3
	jb	.LBB45_16
	.p2align	4, 0x90
.LBB45_6:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	mov	eax, dword ptr [rdi + 4*rcx + 4]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	mov	eax, dword ptr [rdi + 4*rcx + 8]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	mov	eax, dword ptr [rdi + 4*rcx + 12]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB45_6
	jmp	.LBB45_16
.LBB45_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB45_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB45_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB45_12
# %bb.13:
	test	r8b, 1
	je	.LBB45_15
.LBB45_14:
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB45_15:
	cmp	rcx, r9
	jne	.LBB45_3
.LBB45_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB45_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB45_14
	jmp	.LBB45_15
.Lfunc_end45:
	.size	cast_numeric_int32_uint32_avx2, .Lfunc_end45-cast_numeric_int32_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_uint64_uint32_avx2 # -- Begin function cast_numeric_uint64_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint32_avx2,@function
cast_numeric_uint64_uint32_avx2:        # @cast_numeric_uint64_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB46_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB46_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB46_10
.LBB46_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB46_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB46_6:                               # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx + 128]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 160]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 192]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 80], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 96], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB46_6
# %bb.7:
	test	r8b, 1
	je	.LBB46_9
.LBB46_8:
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB46_9:
	cmp	rcx, r9
	je	.LBB46_11
	.p2align	4, 0x90
.LBB46_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 8*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB46_10
.LBB46_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB46_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB46_8
	jmp	.LBB46_9
.Lfunc_end46:
	.size	cast_numeric_uint64_uint32_avx2, .Lfunc_end46-cast_numeric_uint64_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_int64_uint32_avx2  # -- Begin function cast_numeric_int64_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint32_avx2,@function
cast_numeric_int64_uint32_avx2:         # @cast_numeric_int64_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB47_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB47_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB47_10
.LBB47_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB47_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB47_6:                               # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx + 128]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 160]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 192]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 80], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 96], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB47_6
# %bb.7:
	test	r8b, 1
	je	.LBB47_9
.LBB47_8:
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB47_9:
	cmp	rcx, r9
	je	.LBB47_11
	.p2align	4, 0x90
.LBB47_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 8*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB47_10
.LBB47_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB47_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB47_8
	jmp	.LBB47_9
.Lfunc_end47:
	.size	cast_numeric_int64_uint32_avx2, .Lfunc_end47-cast_numeric_int64_uint32_avx2
                                        # -- End function
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2                               # -- Begin function cast_numeric_float32_uint32_avx2
.LCPI48_0:
	.long	0x4f000000                      # float 2.14748365E+9
.LCPI48_1:
	.long	2147483648                      # 0x80000000
	.text
	.globl	cast_numeric_float32_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint32_avx2,@function
cast_numeric_float32_uint32_avx2:       # @cast_numeric_float32_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB48_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB48_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB48_10
.LBB48_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB48_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vbroadcastss	xmm0, dword ptr [rip + .LCPI48_0] # xmm0 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	vbroadcastss	xmm1, dword ptr [rip + .LCPI48_1] # xmm1 = [2147483648,2147483648,2147483648,2147483648]
	.p2align	4, 0x90
.LBB48_6:                               # =>This Inner Loop Header: Depth=1
	vmovups	xmm2, xmmword ptr [rdi + 4*rdx]
	vmovups	xmm3, xmmword ptr [rdi + 4*rdx + 16]
	vmovups	xmm4, xmmword ptr [rdi + 4*rdx + 32]
	vcmpltps	xmm5, xmm2, xmm0
	vsubps	xmm6, xmm2, xmm0
	vcvttps2dq	xmm6, xmm6
	vxorps	xmm6, xmm6, xmm1
	vcvttps2dq	xmm2, xmm2
	vblendvps	xmm2, xmm6, xmm2, xmm5
	vmovups	xmm5, xmmword ptr [rdi + 4*rdx + 48]
	vcmpltps	xmm6, xmm3, xmm0
	vsubps	xmm7, xmm3, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm3, xmm3
	vblendvps	xmm3, xmm7, xmm3, xmm6
	vcmpltps	xmm6, xmm4, xmm0
	vsubps	xmm7, xmm4, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm4, xmm4
	vblendvps	xmm4, xmm7, xmm4, xmm6
	vcmpltps	xmm6, xmm5, xmm0
	vsubps	xmm7, xmm5, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm5, xmm5
	vblendvps	xmm5, xmm7, xmm5, xmm6
	vmovups	xmmword ptr [rsi + 4*rdx], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm3
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm4
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm5
	vmovups	xmm2, xmmword ptr [rdi + 4*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 4*rdx + 80]
	vmovups	xmm4, xmmword ptr [rdi + 4*rdx + 96]
	vcmpltps	xmm5, xmm2, xmm0
	vsubps	xmm6, xmm2, xmm0
	vcvttps2dq	xmm6, xmm6
	vxorps	xmm6, xmm6, xmm1
	vcvttps2dq	xmm2, xmm2
	vblendvps	xmm2, xmm6, xmm2, xmm5
	vmovups	xmm5, xmmword ptr [rdi + 4*rdx + 112]
	vcmpltps	xmm6, xmm3, xmm0
	vsubps	xmm7, xmm3, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm3, xmm3
	vblendvps	xmm3, xmm7, xmm3, xmm6
	vcmpltps	xmm6, xmm4, xmm0
	vsubps	xmm7, xmm4, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm4, xmm4
	vblendvps	xmm4, xmm7, xmm4, xmm6
	vcmpltps	xmm6, xmm5, xmm0
	vsubps	xmm7, xmm5, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm5, xmm5
	vblendvps	xmm5, xmm7, xmm5, xmm6
	vmovups	xmmword ptr [rsi + 4*rdx + 64], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 80], xmm3
	vmovups	xmmword ptr [rsi + 4*rdx + 96], xmm4
	vmovups	xmmword ptr [rsi + 4*rdx + 112], xmm5
	add	rdx, 32
	add	rax, 2
	jne	.LBB48_6
# %bb.7:
	test	r8b, 1
	je	.LBB48_9
.LBB48_8:
	vmovups	xmm0, xmmword ptr [rdi + 4*rdx]
	vbroadcastss	xmm1, dword ptr [rip + .LCPI48_0] # xmm1 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	vcmpltps	xmm2, xmm0, xmm1
	vsubps	xmm3, xmm0, xmm1
	vcvttps2dq	xmm3, xmm3
	vbroadcastss	xmm4, dword ptr [rip + .LCPI48_1] # xmm4 = [2147483648,2147483648,2147483648,2147483648]
	vxorps	xmm3, xmm3, xmm4
	vcvttps2dq	xmm0, xmm0
	vblendvps	xmm0, xmm3, xmm0, xmm2
	vmovups	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	vcmpltps	xmm3, xmm2, xmm1
	vsubps	xmm5, xmm2, xmm1
	vcvttps2dq	xmm5, xmm5
	vxorps	xmm5, xmm5, xmm4
	vcvttps2dq	xmm2, xmm2
	vblendvps	xmm2, xmm5, xmm2, xmm3
	vmovups	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	vcmpltps	xmm5, xmm3, xmm1
	vsubps	xmm6, xmm3, xmm1
	vcvttps2dq	xmm6, xmm6
	vxorps	xmm6, xmm6, xmm4
	vcvttps2dq	xmm3, xmm3
	vblendvps	xmm3, xmm6, xmm3, xmm5
	vmovups	xmm5, xmmword ptr [rdi + 4*rdx + 48]
	vcmpltps	xmm6, xmm5, xmm1
	vsubps	xmm1, xmm5, xmm1
	vcvttps2dq	xmm1, xmm1
	vxorps	xmm1, xmm1, xmm4
	vcvttps2dq	xmm4, xmm5
	vblendvps	xmm1, xmm1, xmm4, xmm6
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm3
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm1
.LBB48_9:
	cmp	rcx, r9
	je	.LBB48_11
	.p2align	4, 0x90
.LBB48_10:                              # =>This Inner Loop Header: Depth=1
	vcvttss2si	rax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB48_10
.LBB48_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB48_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB48_8
	jmp	.LBB48_9
.Lfunc_end48:
	.size	cast_numeric_float32_uint32_avx2, .Lfunc_end48-cast_numeric_float32_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_float64_uint32_avx2 # -- Begin function cast_numeric_float64_uint32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint32_avx2,@function
cast_numeric_float64_uint32_avx2:       # @cast_numeric_float64_uint32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB49_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB49_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB49_3
.LBB49_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB49_8:                               # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx]
	mov	dword ptr [rsi + 4*rdx], eax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 8]
	mov	dword ptr [rsi + 4*rdx + 4], eax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 16]
	mov	dword ptr [rsi + 4*rdx + 8], eax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 24]
	mov	dword ptr [rsi + 4*rdx + 12], eax
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB49_8
.LBB49_3:
	test	r8, r8
	je	.LBB49_6
# %bb.4:
	lea	rcx, [rsi + 4*rdx]
	lea	rdx, [rdi + 8*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB49_5:                               # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rdi, qword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB49_5
.LBB49_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end49:
	.size	cast_numeric_float64_uint32_avx2, .Lfunc_end49-cast_numeric_float64_uint32_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_int32_avx2   # -- Begin function cast_numeric_uint8_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int32_avx2,@function
cast_numeric_uint8_int32_avx2:          # @cast_numeric_uint8_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB50_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB50_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB50_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB50_9
.LBB50_2:
	xor	ecx, ecx
.LBB50_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB50_5
	.p2align	4, 0x90
.LBB50_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB50_4
.LBB50_5:
	cmp	r8, 3
	jb	.LBB50_16
	.p2align	4, 0x90
.LBB50_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB50_6
	jmp	.LBB50_16
.LBB50_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB50_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB50_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	ymm0, qword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovzxbd	ymm0, qword ptr [rdi + rdx + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB50_12
# %bb.13:
	test	r8b, 1
	je	.LBB50_15
.LBB50_14:
	vpmovzxbd	ymm0, qword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB50_15:
	cmp	rcx, r9
	jne	.LBB50_3
.LBB50_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB50_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB50_14
	jmp	.LBB50_15
.Lfunc_end50:
	.size	cast_numeric_uint8_int32_avx2, .Lfunc_end50-cast_numeric_uint8_int32_avx2
                                        # -- End function
	.globl	cast_numeric_int8_int32_avx2    # -- Begin function cast_numeric_int8_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_int32_avx2,@function
cast_numeric_int8_int32_avx2:           # @cast_numeric_int8_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB51_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB51_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB51_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB51_9
.LBB51_2:
	xor	ecx, ecx
.LBB51_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB51_5
	.p2align	4, 0x90
.LBB51_4:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB51_4
.LBB51_5:
	cmp	r8, 3
	jb	.LBB51_16
	.p2align	4, 0x90
.LBB51_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	movsx	eax, byte ptr [rdi + rcx + 1]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	movsx	eax, byte ptr [rdi + rcx + 2]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	movsx	eax, byte ptr [rdi + rcx + 3]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB51_6
	jmp	.LBB51_16
.LBB51_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB51_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB51_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm0, qword ptr [rdi + rdx]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 8]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 16]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 24]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovsxbd	ymm0, qword ptr [rdi + rdx + 32]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 40]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 48]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 56]
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB51_12
# %bb.13:
	test	r8b, 1
	je	.LBB51_15
.LBB51_14:
	vpmovsxbd	ymm0, qword ptr [rdi + rdx]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 8]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 16]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 24]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB51_15:
	cmp	rcx, r9
	jne	.LBB51_3
.LBB51_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB51_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB51_14
	jmp	.LBB51_15
.Lfunc_end51:
	.size	cast_numeric_int8_int32_avx2, .Lfunc_end51-cast_numeric_int8_int32_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_int32_avx2  # -- Begin function cast_numeric_uint16_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int32_avx2,@function
cast_numeric_uint16_int32_avx2:         # @cast_numeric_uint16_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB52_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB52_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB52_10
.LBB52_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB52_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB52_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB52_6
# %bb.7:
	test	r8b, 1
	je	.LBB52_9
.LBB52_8:
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB52_9:
	cmp	rcx, r9
	je	.LBB52_11
	.p2align	4, 0x90
.LBB52_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB52_10
.LBB52_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB52_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB52_8
	jmp	.LBB52_9
.Lfunc_end52:
	.size	cast_numeric_uint16_int32_avx2, .Lfunc_end52-cast_numeric_uint16_int32_avx2
                                        # -- End function
	.globl	cast_numeric_int16_int32_avx2   # -- Begin function cast_numeric_int16_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_int32_avx2,@function
cast_numeric_int16_int32_avx2:          # @cast_numeric_int16_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB53_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB53_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB53_10
.LBB53_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB53_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB53_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx + 64]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 80]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 96]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 112]
	vmovdqu	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB53_6
# %bb.7:
	test	r8b, 1
	je	.LBB53_9
.LBB53_8:
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 4*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB53_9:
	cmp	rcx, r9
	je	.LBB53_11
	.p2align	4, 0x90
.LBB53_10:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdi + 2*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB53_10
.LBB53_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB53_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB53_8
	jmp	.LBB53_9
.Lfunc_end53:
	.size	cast_numeric_int16_int32_avx2, .Lfunc_end53-cast_numeric_int16_int32_avx2
                                        # -- End function
	.globl	cast_numeric_uint32_int32_avx2  # -- Begin function cast_numeric_uint32_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int32_avx2,@function
cast_numeric_uint32_int32_avx2:         # @cast_numeric_uint32_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB54_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB54_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB54_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB54_9
.LBB54_2:
	xor	ecx, ecx
.LBB54_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB54_5
	.p2align	4, 0x90
.LBB54_4:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB54_4
.LBB54_5:
	cmp	r8, 3
	jb	.LBB54_16
	.p2align	4, 0x90
.LBB54_6:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	mov	eax, dword ptr [rdi + 4*rcx + 4]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	mov	eax, dword ptr [rdi + 4*rcx + 8]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	mov	eax, dword ptr [rdi + 4*rcx + 12]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB54_6
	jmp	.LBB54_16
.LBB54_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB54_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB54_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB54_12
# %bb.13:
	test	r8b, 1
	je	.LBB54_15
.LBB54_14:
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB54_15:
	cmp	rcx, r9
	jne	.LBB54_3
.LBB54_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB54_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB54_14
	jmp	.LBB54_15
.Lfunc_end54:
	.size	cast_numeric_uint32_int32_avx2, .Lfunc_end54-cast_numeric_uint32_int32_avx2
                                        # -- End function
	.globl	cast_numeric_int32_int32_avx2   # -- Begin function cast_numeric_int32_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_int32_avx2,@function
cast_numeric_int32_int32_avx2:          # @cast_numeric_int32_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB55_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB55_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB55_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB55_9
.LBB55_2:
	xor	ecx, ecx
.LBB55_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB55_5
	.p2align	4, 0x90
.LBB55_4:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB55_4
.LBB55_5:
	cmp	r8, 3
	jb	.LBB55_16
	.p2align	4, 0x90
.LBB55_6:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	mov	eax, dword ptr [rdi + 4*rcx + 4]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	mov	eax, dword ptr [rdi + 4*rcx + 8]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	mov	eax, dword ptr [rdi + 4*rcx + 12]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB55_6
	jmp	.LBB55_16
.LBB55_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB55_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB55_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB55_12
# %bb.13:
	test	r8b, 1
	je	.LBB55_15
.LBB55_14:
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB55_15:
	cmp	rcx, r9
	jne	.LBB55_3
.LBB55_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB55_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB55_14
	jmp	.LBB55_15
.Lfunc_end55:
	.size	cast_numeric_int32_int32_avx2, .Lfunc_end55-cast_numeric_int32_int32_avx2
                                        # -- End function
	.globl	cast_numeric_uint64_int32_avx2  # -- Begin function cast_numeric_uint64_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int32_avx2,@function
cast_numeric_uint64_int32_avx2:         # @cast_numeric_uint64_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB56_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB56_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB56_10
.LBB56_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB56_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB56_6:                               # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx + 128]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 160]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 192]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 80], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 96], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB56_6
# %bb.7:
	test	r8b, 1
	je	.LBB56_9
.LBB56_8:
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB56_9:
	cmp	rcx, r9
	je	.LBB56_11
	.p2align	4, 0x90
.LBB56_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 8*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB56_10
.LBB56_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB56_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB56_8
	jmp	.LBB56_9
.Lfunc_end56:
	.size	cast_numeric_uint64_int32_avx2, .Lfunc_end56-cast_numeric_uint64_int32_avx2
                                        # -- End function
	.globl	cast_numeric_int64_int32_avx2   # -- Begin function cast_numeric_int64_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_int32_avx2,@function
cast_numeric_int64_int32_avx2:          # @cast_numeric_int64_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB57_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB57_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB57_10
.LBB57_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB57_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB57_6:                               # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx + 128]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 160]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 192]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 80], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 96], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB57_6
# %bb.7:
	test	r8b, 1
	je	.LBB57_9
.LBB57_8:
	vmovups	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovups	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	vmovups	xmm2, xmmword ptr [rdi + 8*rdx + 64]
	vmovups	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdi + 8*rdx + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdi + 8*rdx + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rdx + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rdx + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB57_9:
	cmp	rcx, r9
	je	.LBB57_11
	.p2align	4, 0x90
.LBB57_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 8*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB57_10
.LBB57_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB57_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB57_8
	jmp	.LBB57_9
.Lfunc_end57:
	.size	cast_numeric_int64_int32_avx2, .Lfunc_end57-cast_numeric_int64_int32_avx2
                                        # -- End function
	.globl	cast_numeric_float32_int32_avx2 # -- Begin function cast_numeric_float32_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_int32_avx2,@function
cast_numeric_float32_int32_avx2:        # @cast_numeric_float32_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB58_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB58_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB58_10
.LBB58_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB58_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB58_6:                               # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB58_6
# %bb.7:
	test	r8b, 1
	je	.LBB58_9
.LBB58_8:
	vcvttps2dq	ymm0, ymmword ptr [rdi + 4*rdx]
	vcvttps2dq	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vcvttps2dq	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vcvttps2dq	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB58_9:
	cmp	rcx, r9
	je	.LBB58_11
	.p2align	4, 0x90
.LBB58_10:                              # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB58_10
.LBB58_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB58_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB58_8
	jmp	.LBB58_9
.Lfunc_end58:
	.size	cast_numeric_float32_int32_avx2, .Lfunc_end58-cast_numeric_float32_int32_avx2
                                        # -- End function
	.globl	cast_numeric_float64_int32_avx2 # -- Begin function cast_numeric_float64_int32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_int32_avx2,@function
cast_numeric_float64_int32_avx2:        # @cast_numeric_float64_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB59_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB59_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB59_10
.LBB59_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB59_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB59_6:                               # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovupd	xmmword ptr [rsi + 4*rdx], xmm0
	vmovupd	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovupd	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovupd	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 160]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 192]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovupd	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovupd	xmmword ptr [rsi + 4*rdx + 80], xmm1
	vmovupd	xmmword ptr [rsi + 4*rdx + 96], xmm2
	vmovupd	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB59_6
# %bb.7:
	test	r8b, 1
	je	.LBB59_9
.LBB59_8:
	vcvttpd2dq	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvttpd2dq	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovupd	xmmword ptr [rsi + 4*rdx], xmm0
	vmovupd	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovupd	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovupd	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB59_9:
	cmp	rcx, r9
	je	.LBB59_11
	.p2align	4, 0x90
.LBB59_10:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB59_10
.LBB59_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB59_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB59_8
	jmp	.LBB59_9
.Lfunc_end59:
	.size	cast_numeric_float64_int32_avx2, .Lfunc_end59-cast_numeric_float64_int32_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_uint64_avx2  # -- Begin function cast_numeric_uint8_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint64_avx2,@function
cast_numeric_uint8_uint64_avx2:         # @cast_numeric_uint8_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB60_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB60_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB60_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB60_9
.LBB60_2:
	xor	ecx, ecx
.LBB60_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB60_5
	.p2align	4, 0x90
.LBB60_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB60_4
.LBB60_5:
	cmp	r8, 3
	jb	.LBB60_16
	.p2align	4, 0x90
.LBB60_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB60_6
	jmp	.LBB60_16
.LBB60_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB60_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB60_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbq	ymm0, dword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdi + rdx + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdi + rdx + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdi + rdx + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxbq	ymm0, dword ptr [rdi + rdx + 16] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdi + rdx + 20] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdi + rdx + 24] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdi + rdx + 28] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB60_12
# %bb.13:
	test	r8b, 1
	je	.LBB60_15
.LBB60_14:
	vpmovzxbq	ymm0, dword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdi + rdx + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdi + rdx + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdi + rdx + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB60_15:
	cmp	rcx, r9
	jne	.LBB60_3
.LBB60_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB60_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB60_14
	jmp	.LBB60_15
.Lfunc_end60:
	.size	cast_numeric_uint8_uint64_avx2, .Lfunc_end60-cast_numeric_uint8_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_int8_uint64_avx2   # -- Begin function cast_numeric_int8_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint64_avx2,@function
cast_numeric_int8_uint64_avx2:          # @cast_numeric_int8_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB61_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB61_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB61_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB61_9
.LBB61_2:
	xor	ecx, ecx
.LBB61_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB61_5
	.p2align	4, 0x90
.LBB61_4:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB61_4
.LBB61_5:
	cmp	r8, 3
	jb	.LBB61_16
	.p2align	4, 0x90
.LBB61_6:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	movsx	rax, byte ptr [rdi + rcx + 1]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	movsx	rax, byte ptr [rdi + rcx + 2]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	movsx	rax, byte ptr [rdi + rcx + 3]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB61_6
	jmp	.LBB61_16
.LBB61_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB61_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB61_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbq	ymm0, dword ptr [rdi + rdx]
	vpmovsxbq	ymm1, dword ptr [rdi + rdx + 4]
	vpmovsxbq	ymm2, dword ptr [rdi + rdx + 8]
	vpmovsxbq	ymm3, dword ptr [rdi + rdx + 12]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxbq	ymm0, dword ptr [rdi + rdx + 16]
	vpmovsxbq	ymm1, dword ptr [rdi + rdx + 20]
	vpmovsxbq	ymm2, dword ptr [rdi + rdx + 24]
	vpmovsxbq	ymm3, dword ptr [rdi + rdx + 28]
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB61_12
# %bb.13:
	test	r8b, 1
	je	.LBB61_15
.LBB61_14:
	vpmovsxbq	ymm0, dword ptr [rdi + rdx]
	vpmovsxbq	ymm1, dword ptr [rdi + rdx + 4]
	vpmovsxbq	ymm2, dword ptr [rdi + rdx + 8]
	vpmovsxbq	ymm3, dword ptr [rdi + rdx + 12]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB61_15:
	cmp	rcx, r9
	jne	.LBB61_3
.LBB61_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB61_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB61_14
	jmp	.LBB61_15
.Lfunc_end61:
	.size	cast_numeric_int8_uint64_avx2, .Lfunc_end61-cast_numeric_int8_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_uint64_avx2 # -- Begin function cast_numeric_uint16_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint64_avx2,@function
cast_numeric_uint16_uint64_avx2:        # @cast_numeric_uint16_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB62_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB62_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB62_10
.LBB62_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB62_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB62_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxwq	ymm0, qword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdi + 2*rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdi + 2*rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdi + 2*rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxwq	ymm0, qword ptr [rdi + 2*rdx + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdi + 2*rdx + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdi + 2*rdx + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdi + 2*rdx + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB62_6
# %bb.7:
	test	r8b, 1
	je	.LBB62_9
.LBB62_8:
	vpmovzxwq	ymm0, qword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdi + 2*rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdi + 2*rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdi + 2*rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB62_9:
	cmp	rcx, r9
	je	.LBB62_11
	.p2align	4, 0x90
.LBB62_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB62_10
.LBB62_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB62_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB62_8
	jmp	.LBB62_9
.Lfunc_end62:
	.size	cast_numeric_uint16_uint64_avx2, .Lfunc_end62-cast_numeric_uint16_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_int16_uint64_avx2  # -- Begin function cast_numeric_int16_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint64_avx2,@function
cast_numeric_int16_uint64_avx2:         # @cast_numeric_int16_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB63_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB63_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB63_10
.LBB63_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB63_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB63_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxwq	ymm0, qword ptr [rdi + 2*rdx]
	vpmovsxwq	ymm1, qword ptr [rdi + 2*rdx + 8]
	vpmovsxwq	ymm2, qword ptr [rdi + 2*rdx + 16]
	vpmovsxwq	ymm3, qword ptr [rdi + 2*rdx + 24]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxwq	ymm0, qword ptr [rdi + 2*rdx + 32]
	vpmovsxwq	ymm1, qword ptr [rdi + 2*rdx + 40]
	vpmovsxwq	ymm2, qword ptr [rdi + 2*rdx + 48]
	vpmovsxwq	ymm3, qword ptr [rdi + 2*rdx + 56]
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB63_6
# %bb.7:
	test	r8b, 1
	je	.LBB63_9
.LBB63_8:
	vpmovsxwq	ymm0, qword ptr [rdi + 2*rdx]
	vpmovsxwq	ymm1, qword ptr [rdi + 2*rdx + 8]
	vpmovsxwq	ymm2, qword ptr [rdi + 2*rdx + 16]
	vpmovsxwq	ymm3, qword ptr [rdi + 2*rdx + 24]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB63_9:
	cmp	rcx, r9
	je	.LBB63_11
	.p2align	4, 0x90
.LBB63_10:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB63_10
.LBB63_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB63_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB63_8
	jmp	.LBB63_9
.Lfunc_end63:
	.size	cast_numeric_int16_uint64_avx2, .Lfunc_end63-cast_numeric_int16_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_uint32_uint64_avx2 # -- Begin function cast_numeric_uint32_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint64_avx2,@function
cast_numeric_uint32_uint64_avx2:        # @cast_numeric_uint32_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB64_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB64_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB64_10
.LBB64_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB64_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB64_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB64_6
# %bb.7:
	test	r8b, 1
	je	.LBB64_9
.LBB64_8:
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB64_9:
	cmp	rcx, r9
	je	.LBB64_11
	.p2align	4, 0x90
.LBB64_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB64_10
.LBB64_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB64_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB64_8
	jmp	.LBB64_9
.Lfunc_end64:
	.size	cast_numeric_uint32_uint64_avx2, .Lfunc_end64-cast_numeric_uint32_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_int32_uint64_avx2  # -- Begin function cast_numeric_int32_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint64_avx2,@function
cast_numeric_int32_uint64_avx2:         # @cast_numeric_int32_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB65_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB65_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB65_10
.LBB65_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB65_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB65_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxdq	ymm0, xmmword ptr [rdi + 4*rdx]
	vpmovsxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxdq	ymm0, xmmword ptr [rdi + 4*rdx + 64]
	vpmovsxdq	ymm1, xmmword ptr [rdi + 4*rdx + 80]
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rdx + 96]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rdx + 112]
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB65_6
# %bb.7:
	test	r8b, 1
	je	.LBB65_9
.LBB65_8:
	vpmovsxdq	ymm0, xmmword ptr [rdi + 4*rdx]
	vpmovsxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB65_9:
	cmp	rcx, r9
	je	.LBB65_11
	.p2align	4, 0x90
.LBB65_10:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB65_10
.LBB65_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB65_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB65_8
	jmp	.LBB65_9
.Lfunc_end65:
	.size	cast_numeric_int32_uint64_avx2, .Lfunc_end65-cast_numeric_int32_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_uint64_uint64_avx2 # -- Begin function cast_numeric_uint64_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint64_avx2,@function
cast_numeric_uint64_uint64_avx2:        # @cast_numeric_uint64_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB66_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB66_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB66_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB66_9
.LBB66_2:
	xor	ecx, ecx
.LBB66_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB66_5
	.p2align	4, 0x90
.LBB66_4:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB66_4
.LBB66_5:
	cmp	r8, 3
	jb	.LBB66_16
	.p2align	4, 0x90
.LBB66_6:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	mov	rax, qword ptr [rdi + 8*rcx + 8]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	mov	rax, qword ptr [rdi + 8*rcx + 16]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	mov	rax, qword ptr [rdi + 8*rcx + 24]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB66_6
	jmp	.LBB66_16
.LBB66_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB66_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB66_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovups	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB66_12
# %bb.13:
	test	r8b, 1
	je	.LBB66_15
.LBB66_14:
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB66_15:
	cmp	rcx, r9
	jne	.LBB66_3
.LBB66_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB66_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB66_14
	jmp	.LBB66_15
.Lfunc_end66:
	.size	cast_numeric_uint64_uint64_avx2, .Lfunc_end66-cast_numeric_uint64_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_int64_uint64_avx2  # -- Begin function cast_numeric_int64_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint64_avx2,@function
cast_numeric_int64_uint64_avx2:         # @cast_numeric_int64_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB67_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB67_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB67_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB67_9
.LBB67_2:
	xor	ecx, ecx
.LBB67_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB67_5
	.p2align	4, 0x90
.LBB67_4:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB67_4
.LBB67_5:
	cmp	r8, 3
	jb	.LBB67_16
	.p2align	4, 0x90
.LBB67_6:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	mov	rax, qword ptr [rdi + 8*rcx + 8]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	mov	rax, qword ptr [rdi + 8*rcx + 16]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	mov	rax, qword ptr [rdi + 8*rcx + 24]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB67_6
	jmp	.LBB67_16
.LBB67_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB67_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB67_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovups	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB67_12
# %bb.13:
	test	r8b, 1
	je	.LBB67_15
.LBB67_14:
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB67_15:
	cmp	rcx, r9
	jne	.LBB67_3
.LBB67_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB67_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB67_14
	jmp	.LBB67_15
.Lfunc_end67:
	.size	cast_numeric_int64_uint64_avx2, .Lfunc_end67-cast_numeric_int64_uint64_avx2
                                        # -- End function
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2                               # -- Begin function cast_numeric_float32_uint64_avx2
.LCPI68_0:
	.long	0x5f000000                      # float 9.22337203E+18
	.text
	.globl	cast_numeric_float32_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint64_avx2,@function
cast_numeric_float32_uint64_avx2:       # @cast_numeric_float32_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r14
	push	rbx
	and	rsp, -8
	test	edx, edx
	jle	.LBB68_13
# %bb.1:
	mov	r8d, edx
	movabs	r11, -9223372036854775808
	cmp	edx, 3
	ja	.LBB68_5
# %bb.2:
	xor	r14d, r14d
	jmp	.LBB68_3
.LBB68_5:
	mov	r14d, r8d
	and	r14d, -4
	lea	rax, [r14 - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB68_7
# %bb.6:
	xor	eax, eax
	jmp	.LBB68_9
.LBB68_7:
	and	r10, -4
	neg	r10
	xor	eax, eax
	vmovss	xmm0, dword ptr [rip + .LCPI68_0] # xmm0 = mem[0],zero,zero,zero
	.p2align	4, 0x90
.LBB68_8:                               # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdi + 4*rax + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	vmovss	xmm2, dword ptr [rdi + 4*rax]   # xmm2 = mem[0],zero,zero,zero
	xor	rcx, r11
	vcvttss2si	rbx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rbx, rcx
	vsubss	xmm1, xmm2, xmm0
	vcvttss2si	rcx, xmm1
	xor	rcx, r11
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm0
	vmovq	xmm1, rbx
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vmovss	xmm3, dword ptr [rdi + 4*rax + 12] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rcx, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rcx, r11
	vcvttss2si	rdx, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdx, rcx
	vmovss	xmm2, dword ptr [rdi + 4*rax + 8] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rcx, xmm3
	vmovq	xmm3, rdx
	xor	rcx, r11
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax], xmm1
	vmovss	xmm1, dword ptr [rdi + 4*rax + 20] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	vmovss	xmm2, dword ptr [rdi + 4*rax + 16] # xmm2 = mem[0],zero,zero,zero
	xor	rcx, r11
	vcvttss2si	rdx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdx, rcx
	vsubss	xmm1, xmm2, xmm0
	vcvttss2si	rcx, xmm1
	xor	rcx, r11
	vcvttss2si	rbx, xmm2
	vucomiss	xmm2, xmm0
	vmovq	xmm1, rdx
	cmovae	rbx, rcx
	vmovq	xmm2, rbx
	vmovss	xmm3, dword ptr [rdi + 4*rax + 28] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rcx, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rcx, r11
	vcvttss2si	rdx, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdx, rcx
	vmovss	xmm2, dword ptr [rdi + 4*rax + 24] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rcx, xmm3
	vmovq	xmm3, rdx
	xor	rcx, r11
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 48], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax + 32], xmm1
	vmovss	xmm1, dword ptr [rdi + 4*rax + 36] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	vmovss	xmm2, dword ptr [rdi + 4*rax + 32] # xmm2 = mem[0],zero,zero,zero
	xor	rcx, r11
	vcvttss2si	rdx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdx, rcx
	vsubss	xmm1, xmm2, xmm0
	vcvttss2si	rcx, xmm1
	xor	rcx, r11
	vcvttss2si	rbx, xmm2
	vucomiss	xmm2, xmm0
	vmovq	xmm1, rdx
	cmovae	rbx, rcx
	vmovq	xmm2, rbx
	vmovss	xmm3, dword ptr [rdi + 4*rax + 44] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rcx, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rcx, r11
	vcvttss2si	rdx, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdx, rcx
	vmovss	xmm2, dword ptr [rdi + 4*rax + 40] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rcx, xmm3
	vmovq	xmm3, rdx
	xor	rcx, r11
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 80], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax + 64], xmm1
	vmovss	xmm1, dword ptr [rdi + 4*rax + 52] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	xor	rcx, r11
	vcvttss2si	rdx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdx, rcx
	vmovss	xmm1, dword ptr [rdi + 4*rax + 48] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	xor	rcx, r11
	vcvttss2si	rbx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rbx, rcx
	vmovq	xmm1, rdx
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovss	xmm2, dword ptr [rdi + 4*rax + 60] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rcx, xmm3
	xor	rcx, r11
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vmovss	xmm3, dword ptr [rdi + 4*rax + 56] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rcx, xmm4
	xor	rcx, r11
	vcvttss2si	rdx, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdx, rcx
	vmovq	xmm3, rdx
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 112], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax + 96], xmm1
	add	rax, 16
	add	r10, 4
	jne	.LBB68_8
.LBB68_9:
	test	r9, r9
	je	.LBB68_12
# %bb.10:
	shl	rax, 2
	neg	r9
	vmovss	xmm0, dword ptr [rip + .LCPI68_0] # xmm0 = mem[0],zero,zero,zero
	.p2align	4, 0x90
.LBB68_11:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdi + rax + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	xor	rcx, r11
	vcvttss2si	rdx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdx, rcx
	vmovss	xmm1, dword ptr [rdi + rax]     # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rcx, xmm2
	xor	rcx, r11
	vcvttss2si	rbx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rbx, rcx
	vmovq	xmm1, rdx
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovss	xmm2, dword ptr [rdi + rax + 12] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rcx, xmm3
	xor	rcx, r11
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vmovss	xmm3, dword ptr [rdi + rax + 8] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rcx, xmm4
	xor	rcx, r11
	vcvttss2si	rdx, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdx, rcx
	vmovq	xmm3, rdx
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + 2*rax + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 2*rax], xmm1
	add	rax, 16
	inc	r9
	jne	.LBB68_11
.LBB68_12:
	cmp	r14, r8
	je	.LBB68_13
.LBB68_3:
	vmovss	xmm0, dword ptr [rip + .LCPI68_0] # xmm0 = mem[0],zero,zero,zero
	.p2align	4, 0x90
.LBB68_4:                               # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdi + 4*r14]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rax, xmm2
	xor	rax, r11
	vcvttss2si	rcx, xmm1
	vucomiss	xmm0, xmm1
	cmovbe	rcx, rax
	mov	qword ptr [rsi + 8*r14], rcx
	add	r14, 1
	cmp	r8, r14
	jne	.LBB68_4
.LBB68_13:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	ret
.Lfunc_end68:
	.size	cast_numeric_float32_uint64_avx2, .Lfunc_end68-cast_numeric_float32_uint64_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_numeric_float64_uint64_avx2
.LCPI69_0:
	.quad	0x43e0000000000000              # double 9.2233720368547758E+18
	.text
	.globl	cast_numeric_float64_uint64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint64_avx2,@function
cast_numeric_float64_uint64_avx2:       # @cast_numeric_float64_uint64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r14
	push	rbx
	and	rsp, -8
	test	edx, edx
	jle	.LBB69_13
# %bb.1:
	mov	r8d, edx
	movabs	r11, -9223372036854775808
	cmp	edx, 3
	ja	.LBB69_5
# %bb.2:
	xor	r14d, r14d
	jmp	.LBB69_3
.LBB69_5:
	mov	r14d, r8d
	and	r14d, -4
	lea	rax, [r14 - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB69_7
# %bb.6:
	xor	eax, eax
	jmp	.LBB69_9
.LBB69_7:
	and	r10, -4
	neg	r10
	xor	eax, eax
	vmovsd	xmm0, qword ptr [rip + .LCPI69_0] # xmm0 = mem[0],zero
	.p2align	4, 0x90
.LBB69_8:                               # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdi + 8*rax + 8] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	vmovsd	xmm2, qword ptr [rdi + 8*rax]   # xmm2 = mem[0],zero
	xor	rcx, r11
	vcvttsd2si	rbx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rbx, rcx
	vsubsd	xmm1, xmm2, xmm0
	vcvttsd2si	rcx, xmm1
	xor	rcx, r11
	vcvttsd2si	rdx, xmm2
	vucomisd	xmm2, xmm0
	vmovq	xmm1, rbx
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vmovsd	xmm3, qword ptr [rdi + 8*rax + 24] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rcx, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rcx, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdx, rcx
	vmovsd	xmm2, qword ptr [rdi + 8*rax + 16] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rcx, xmm3
	vmovq	xmm3, rdx
	xor	rcx, r11
	vcvttsd2si	rdx, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 16], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax], xmm1
	vmovsd	xmm1, qword ptr [rdi + 8*rax + 40] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	vmovsd	xmm2, qword ptr [rdi + 8*rax + 32] # xmm2 = mem[0],zero
	xor	rcx, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdx, rcx
	vsubsd	xmm1, xmm2, xmm0
	vcvttsd2si	rcx, xmm1
	xor	rcx, r11
	vcvttsd2si	rbx, xmm2
	vucomisd	xmm2, xmm0
	vmovq	xmm1, rdx
	cmovae	rbx, rcx
	vmovq	xmm2, rbx
	vmovsd	xmm3, qword ptr [rdi + 8*rax + 56] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rcx, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rcx, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdx, rcx
	vmovsd	xmm2, qword ptr [rdi + 8*rax + 48] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rcx, xmm3
	vmovq	xmm3, rdx
	xor	rcx, r11
	vcvttsd2si	rdx, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 48], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax + 32], xmm1
	vmovsd	xmm1, qword ptr [rdi + 8*rax + 72] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	vmovsd	xmm2, qword ptr [rdi + 8*rax + 64] # xmm2 = mem[0],zero
	xor	rcx, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdx, rcx
	vsubsd	xmm1, xmm2, xmm0
	vcvttsd2si	rcx, xmm1
	xor	rcx, r11
	vcvttsd2si	rbx, xmm2
	vucomisd	xmm2, xmm0
	vmovq	xmm1, rdx
	cmovae	rbx, rcx
	vmovq	xmm2, rbx
	vmovsd	xmm3, qword ptr [rdi + 8*rax + 88] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rcx, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rcx, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdx, rcx
	vmovsd	xmm2, qword ptr [rdi + 8*rax + 80] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rcx, xmm3
	vmovq	xmm3, rdx
	xor	rcx, r11
	vcvttsd2si	rdx, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 80], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax + 64], xmm1
	vmovsd	xmm1, qword ptr [rdi + 8*rax + 104] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	xor	rcx, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdx, rcx
	vmovsd	xmm1, qword ptr [rdi + 8*rax + 96] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	xor	rcx, r11
	vcvttsd2si	rbx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rbx, rcx
	vmovq	xmm1, rdx
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovsd	xmm2, qword ptr [rdi + 8*rax + 120] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rcx, xmm3
	xor	rcx, r11
	vcvttsd2si	rdx, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vmovsd	xmm3, qword ptr [rdi + 8*rax + 112] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rcx, xmm4
	xor	rcx, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdx, rcx
	vmovq	xmm3, rdx
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + 8*rax + 112], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rax + 96], xmm1
	add	rax, 16
	add	r10, 4
	jne	.LBB69_8
.LBB69_9:
	test	r9, r9
	je	.LBB69_12
# %bb.10:
	shl	rax, 3
	neg	r9
	vmovsd	xmm0, qword ptr [rip + .LCPI69_0] # xmm0 = mem[0],zero
	.p2align	4, 0x90
.LBB69_11:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdi + rax + 8] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	xor	rcx, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdx, rcx
	vmovsd	xmm1, qword ptr [rdi + rax]     # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rcx, xmm2
	xor	rcx, r11
	vcvttsd2si	rbx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rbx, rcx
	vmovq	xmm1, rdx
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovsd	xmm2, qword ptr [rdi + rax + 24] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rcx, xmm3
	xor	rcx, r11
	vcvttsd2si	rdx, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdx, rcx
	vmovq	xmm2, rdx
	vmovsd	xmm3, qword ptr [rdi + rax + 16] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rcx, xmm4
	xor	rcx, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdx, rcx
	vmovq	xmm3, rdx
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rsi + rax + 16], xmm2
	vmovdqu	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB69_11
.LBB69_12:
	cmp	r14, r8
	je	.LBB69_13
.LBB69_3:
	vmovsd	xmm0, qword ptr [rip + .LCPI69_0] # xmm0 = mem[0],zero
	.p2align	4, 0x90
.LBB69_4:                               # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdi + 8*r14]   # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rax, xmm2
	xor	rax, r11
	vcvttsd2si	rcx, xmm1
	vucomisd	xmm0, xmm1
	cmovbe	rcx, rax
	mov	qword ptr [rsi + 8*r14], rcx
	add	r14, 1
	cmp	r8, r14
	jne	.LBB69_4
.LBB69_13:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	ret
.Lfunc_end69:
	.size	cast_numeric_float64_uint64_avx2, .Lfunc_end69-cast_numeric_float64_uint64_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_int64_avx2   # -- Begin function cast_numeric_uint8_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int64_avx2,@function
cast_numeric_uint8_int64_avx2:          # @cast_numeric_uint8_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB70_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB70_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB70_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB70_9
.LBB70_2:
	xor	ecx, ecx
.LBB70_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB70_5
	.p2align	4, 0x90
.LBB70_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB70_4
.LBB70_5:
	cmp	r8, 3
	jb	.LBB70_16
	.p2align	4, 0x90
.LBB70_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	movzx	eax, byte ptr [rdi + rcx + 1]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	movzx	eax, byte ptr [rdi + rcx + 2]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	movzx	eax, byte ptr [rdi + rcx + 3]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB70_6
	jmp	.LBB70_16
.LBB70_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB70_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB70_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbq	ymm0, dword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdi + rdx + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdi + rdx + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdi + rdx + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxbq	ymm0, dword ptr [rdi + rdx + 16] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdi + rdx + 20] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdi + rdx + 24] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdi + rdx + 28] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB70_12
# %bb.13:
	test	r8b, 1
	je	.LBB70_15
.LBB70_14:
	vpmovzxbq	ymm0, dword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdi + rdx + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdi + rdx + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdi + rdx + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB70_15:
	cmp	rcx, r9
	jne	.LBB70_3
.LBB70_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB70_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB70_14
	jmp	.LBB70_15
.Lfunc_end70:
	.size	cast_numeric_uint8_int64_avx2, .Lfunc_end70-cast_numeric_uint8_int64_avx2
                                        # -- End function
	.globl	cast_numeric_int8_int64_avx2    # -- Begin function cast_numeric_int8_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_int64_avx2,@function
cast_numeric_int8_int64_avx2:           # @cast_numeric_int8_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB71_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB71_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB71_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB71_9
.LBB71_2:
	xor	ecx, ecx
.LBB71_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB71_5
	.p2align	4, 0x90
.LBB71_4:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB71_4
.LBB71_5:
	cmp	r8, 3
	jb	.LBB71_16
	.p2align	4, 0x90
.LBB71_6:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	movsx	rax, byte ptr [rdi + rcx + 1]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	movsx	rax, byte ptr [rdi + rcx + 2]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	movsx	rax, byte ptr [rdi + rcx + 3]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB71_6
	jmp	.LBB71_16
.LBB71_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB71_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB71_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbq	ymm0, dword ptr [rdi + rdx]
	vpmovsxbq	ymm1, dword ptr [rdi + rdx + 4]
	vpmovsxbq	ymm2, dword ptr [rdi + rdx + 8]
	vpmovsxbq	ymm3, dword ptr [rdi + rdx + 12]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxbq	ymm0, dword ptr [rdi + rdx + 16]
	vpmovsxbq	ymm1, dword ptr [rdi + rdx + 20]
	vpmovsxbq	ymm2, dword ptr [rdi + rdx + 24]
	vpmovsxbq	ymm3, dword ptr [rdi + rdx + 28]
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB71_12
# %bb.13:
	test	r8b, 1
	je	.LBB71_15
.LBB71_14:
	vpmovsxbq	ymm0, dword ptr [rdi + rdx]
	vpmovsxbq	ymm1, dword ptr [rdi + rdx + 4]
	vpmovsxbq	ymm2, dword ptr [rdi + rdx + 8]
	vpmovsxbq	ymm3, dword ptr [rdi + rdx + 12]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB71_15:
	cmp	rcx, r9
	jne	.LBB71_3
.LBB71_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB71_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB71_14
	jmp	.LBB71_15
.Lfunc_end71:
	.size	cast_numeric_int8_int64_avx2, .Lfunc_end71-cast_numeric_int8_int64_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_int64_avx2  # -- Begin function cast_numeric_uint16_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int64_avx2,@function
cast_numeric_uint16_int64_avx2:         # @cast_numeric_uint16_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB72_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB72_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB72_10
.LBB72_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB72_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB72_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxwq	ymm0, qword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdi + 2*rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdi + 2*rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdi + 2*rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxwq	ymm0, qword ptr [rdi + 2*rdx + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdi + 2*rdx + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdi + 2*rdx + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdi + 2*rdx + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB72_6
# %bb.7:
	test	r8b, 1
	je	.LBB72_9
.LBB72_8:
	vpmovzxwq	ymm0, qword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdi + 2*rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdi + 2*rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdi + 2*rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB72_9:
	cmp	rcx, r9
	je	.LBB72_11
	.p2align	4, 0x90
.LBB72_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB72_10
.LBB72_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB72_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB72_8
	jmp	.LBB72_9
.Lfunc_end72:
	.size	cast_numeric_uint16_int64_avx2, .Lfunc_end72-cast_numeric_uint16_int64_avx2
                                        # -- End function
	.globl	cast_numeric_int16_int64_avx2   # -- Begin function cast_numeric_int16_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_int64_avx2,@function
cast_numeric_int16_int64_avx2:          # @cast_numeric_int16_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB73_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB73_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB73_10
.LBB73_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB73_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB73_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxwq	ymm0, qword ptr [rdi + 2*rdx]
	vpmovsxwq	ymm1, qword ptr [rdi + 2*rdx + 8]
	vpmovsxwq	ymm2, qword ptr [rdi + 2*rdx + 16]
	vpmovsxwq	ymm3, qword ptr [rdi + 2*rdx + 24]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxwq	ymm0, qword ptr [rdi + 2*rdx + 32]
	vpmovsxwq	ymm1, qword ptr [rdi + 2*rdx + 40]
	vpmovsxwq	ymm2, qword ptr [rdi + 2*rdx + 48]
	vpmovsxwq	ymm3, qword ptr [rdi + 2*rdx + 56]
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB73_6
# %bb.7:
	test	r8b, 1
	je	.LBB73_9
.LBB73_8:
	vpmovsxwq	ymm0, qword ptr [rdi + 2*rdx]
	vpmovsxwq	ymm1, qword ptr [rdi + 2*rdx + 8]
	vpmovsxwq	ymm2, qword ptr [rdi + 2*rdx + 16]
	vpmovsxwq	ymm3, qword ptr [rdi + 2*rdx + 24]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB73_9:
	cmp	rcx, r9
	je	.LBB73_11
	.p2align	4, 0x90
.LBB73_10:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB73_10
.LBB73_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB73_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB73_8
	jmp	.LBB73_9
.Lfunc_end73:
	.size	cast_numeric_int16_int64_avx2, .Lfunc_end73-cast_numeric_int16_int64_avx2
                                        # -- End function
	.globl	cast_numeric_uint32_int64_avx2  # -- Begin function cast_numeric_uint32_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int64_avx2,@function
cast_numeric_uint32_int64_avx2:         # @cast_numeric_uint32_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB74_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB74_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB74_10
.LBB74_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB74_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB74_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB74_6
# %bb.7:
	test	r8b, 1
	je	.LBB74_9
.LBB74_8:
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB74_9:
	cmp	rcx, r9
	je	.LBB74_11
	.p2align	4, 0x90
.LBB74_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB74_10
.LBB74_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB74_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB74_8
	jmp	.LBB74_9
.Lfunc_end74:
	.size	cast_numeric_uint32_int64_avx2, .Lfunc_end74-cast_numeric_uint32_int64_avx2
                                        # -- End function
	.globl	cast_numeric_int32_int64_avx2   # -- Begin function cast_numeric_int32_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_int64_avx2,@function
cast_numeric_int32_int64_avx2:          # @cast_numeric_int32_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB75_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB75_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB75_10
.LBB75_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB75_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB75_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxdq	ymm0, xmmword ptr [rdi + 4*rdx]
	vpmovsxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxdq	ymm0, xmmword ptr [rdi + 4*rdx + 64]
	vpmovsxdq	ymm1, xmmword ptr [rdi + 4*rdx + 80]
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rdx + 96]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rdx + 112]
	vmovdqu	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB75_6
# %bb.7:
	test	r8b, 1
	je	.LBB75_9
.LBB75_8:
	vpmovsxdq	ymm0, xmmword ptr [rdi + 4*rdx]
	vpmovsxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovdqu	ymmword ptr [rsi + 8*rdx], ymm0
	vmovdqu	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovdqu	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB75_9:
	cmp	rcx, r9
	je	.LBB75_11
	.p2align	4, 0x90
.LBB75_10:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB75_10
.LBB75_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB75_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB75_8
	jmp	.LBB75_9
.Lfunc_end75:
	.size	cast_numeric_int32_int64_avx2, .Lfunc_end75-cast_numeric_int32_int64_avx2
                                        # -- End function
	.globl	cast_numeric_uint64_int64_avx2  # -- Begin function cast_numeric_uint64_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int64_avx2,@function
cast_numeric_uint64_int64_avx2:         # @cast_numeric_uint64_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB76_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB76_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB76_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB76_9
.LBB76_2:
	xor	ecx, ecx
.LBB76_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB76_5
	.p2align	4, 0x90
.LBB76_4:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB76_4
.LBB76_5:
	cmp	r8, 3
	jb	.LBB76_16
	.p2align	4, 0x90
.LBB76_6:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	mov	rax, qword ptr [rdi + 8*rcx + 8]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	mov	rax, qword ptr [rdi + 8*rcx + 16]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	mov	rax, qword ptr [rdi + 8*rcx + 24]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB76_6
	jmp	.LBB76_16
.LBB76_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB76_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB76_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovups	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB76_12
# %bb.13:
	test	r8b, 1
	je	.LBB76_15
.LBB76_14:
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB76_15:
	cmp	rcx, r9
	jne	.LBB76_3
.LBB76_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB76_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB76_14
	jmp	.LBB76_15
.Lfunc_end76:
	.size	cast_numeric_uint64_int64_avx2, .Lfunc_end76-cast_numeric_uint64_int64_avx2
                                        # -- End function
	.globl	cast_numeric_int64_int64_avx2   # -- Begin function cast_numeric_int64_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_int64_avx2,@function
cast_numeric_int64_int64_avx2:          # @cast_numeric_int64_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB77_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB77_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB77_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB77_9
.LBB77_2:
	xor	ecx, ecx
.LBB77_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB77_5
	.p2align	4, 0x90
.LBB77_4:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB77_4
.LBB77_5:
	cmp	r8, 3
	jb	.LBB77_16
	.p2align	4, 0x90
.LBB77_6:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	mov	rax, qword ptr [rdi + 8*rcx + 8]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	mov	rax, qword ptr [rdi + 8*rcx + 16]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	mov	rax, qword ptr [rdi + 8*rcx + 24]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB77_6
	jmp	.LBB77_16
.LBB77_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB77_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB77_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovups	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB77_12
# %bb.13:
	test	r8b, 1
	je	.LBB77_15
.LBB77_14:
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB77_15:
	cmp	rcx, r9
	jne	.LBB77_3
.LBB77_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB77_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB77_14
	jmp	.LBB77_15
.Lfunc_end77:
	.size	cast_numeric_int64_int64_avx2, .Lfunc_end77-cast_numeric_int64_int64_avx2
                                        # -- End function
	.globl	cast_numeric_float32_int64_avx2 # -- Begin function cast_numeric_float32_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_int64_avx2,@function
cast_numeric_float32_int64_avx2:        # @cast_numeric_float32_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	edx, edx
	jle	.LBB78_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB78_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB78_10
.LBB78_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB78_4
# %bb.5:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edx, edx
	.p2align	4, 0x90
.LBB78_6:                               # =>This Inner Loop Header: Depth=1
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 4]
	vmovq	xmm0, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx]
	vmovq	xmm1, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 12]
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttss2si	r11, dword ptr [rdi + 4*rdx + 8]
	vmovq	xmm1, rax
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 28]
	vmovq	xmm2, r11
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 24]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovq	xmm2, rbx
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 20]
	vmovq	xmm3, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 16]
	vmovq	xmm4, rax
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 44]
	vmovq	xmm4, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 40]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 36]
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 32]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 60]
	vmovq	xmm6, rbx
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 56]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vmovq	xmm6, rax
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 52]
	vmovq	xmm7, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 48]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rsi + 8*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 8*rdx], xmm8
	vmovdqu	xmmword ptr [rsi + 8*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 8*rdx + 48], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rdx + 64], xmm5
	vmovdqu	xmmword ptr [rsi + 8*rdx + 80], xmm4
	vmovdqu	xmmword ptr [rsi + 8*rdx + 96], xmm0
	vmovdqu	xmmword ptr [rsi + 8*rdx + 112], xmm6
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 68]
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 64]
	vmovq	xmm0, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 76]
	vmovq	xmm1, rbx
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 72]
	vmovq	xmm2, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vmovq	xmm1, rbx
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 92]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 88]
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 84]
	vmovq	xmm3, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 80]
	vmovq	xmm4, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 108]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 104]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 100]
	vmovq	xmm7, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 96]
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vpunpcklqdq	xmm4, xmm6, xmm5        # xmm4 = xmm6[0],xmm5[0]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm5, xmm5, xmm7        # xmm5 = xmm5[0],xmm7[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 124]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 120]
	vmovq	xmm7, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 116]
	vmovq	xmm7, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 112]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rsi + 8*rdx + 144], xmm1
	vmovdqu	xmmword ptr [rsi + 8*rdx + 128], xmm8
	vmovdqu	xmmword ptr [rsi + 8*rdx + 160], xmm3
	vmovdqu	xmmword ptr [rsi + 8*rdx + 176], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rdx + 192], xmm5
	vmovdqu	xmmword ptr [rsi + 8*rdx + 208], xmm4
	vmovdqu	xmmword ptr [rsi + 8*rdx + 224], xmm0
	vmovdqu	xmmword ptr [rsi + 8*rdx + 240], xmm6
	add	rdx, 32
	add	r10, 2
	jne	.LBB78_6
# %bb.7:
	test	r8b, 1
	je	.LBB78_9
.LBB78_8:
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 4]
	vmovq	xmm0, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx]
	vmovq	xmm1, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 12]
	vmovq	xmm1, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 8]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 28]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 24]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 20]
	vmovq	xmm3, rbx
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 16]
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovq	xmm3, rax
	vmovq	xmm4, rbx
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 44]
	vmovq	xmm4, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 40]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 36]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 32]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 60]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 56]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdi + 4*rdx + 52]
	vmovq	xmm7, rbx
	vcvttss2si	rbx, dword ptr [rdi + 4*rdx + 48]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vmovdqu	xmmword ptr [rsi + 8*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 8*rdx], xmm8
	vmovdqu	xmmword ptr [rsi + 8*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 8*rdx + 48], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rdx + 64], xmm5
	vmovdqu	xmmword ptr [rsi + 8*rdx + 80], xmm4
	vmovdqu	xmmword ptr [rsi + 8*rdx + 96], xmm0
	vmovdqu	xmmword ptr [rsi + 8*rdx + 112], xmm6
.LBB78_9:
	cmp	rcx, r9
	je	.LBB78_11
	.p2align	4, 0x90
.LBB78_10:                              # =>This Inner Loop Header: Depth=1
	vcvttss2si	rax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB78_10
.LBB78_11:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	ret
.LBB78_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB78_8
	jmp	.LBB78_9
.Lfunc_end78:
	.size	cast_numeric_float32_int64_avx2, .Lfunc_end78-cast_numeric_float32_int64_avx2
                                        # -- End function
	.globl	cast_numeric_float64_int64_avx2 # -- Begin function cast_numeric_float64_int64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_int64_avx2,@function
cast_numeric_float64_int64_avx2:        # @cast_numeric_float64_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	edx, edx
	jle	.LBB79_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB79_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB79_10
.LBB79_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB79_4
# %bb.5:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edx, edx
	.p2align	4, 0x90
.LBB79_6:                               # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 8]
	vmovq	xmm0, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx]
	vmovq	xmm1, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 24]
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttsd2si	r11, qword ptr [rdi + 8*rdx + 16]
	vmovq	xmm1, rax
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 56]
	vmovq	xmm2, r11
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 48]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovq	xmm2, rbx
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 40]
	vmovq	xmm3, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 32]
	vmovq	xmm4, rax
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 88]
	vmovq	xmm4, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 80]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 72]
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 64]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 120]
	vmovq	xmm6, rbx
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 112]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vmovq	xmm6, rax
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 104]
	vmovq	xmm7, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 96]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rsi + 8*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 8*rdx], xmm8
	vmovdqu	xmmword ptr [rsi + 8*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 8*rdx + 48], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rdx + 64], xmm5
	vmovdqu	xmmword ptr [rsi + 8*rdx + 80], xmm4
	vmovdqu	xmmword ptr [rsi + 8*rdx + 96], xmm0
	vmovdqu	xmmword ptr [rsi + 8*rdx + 112], xmm6
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 136]
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 128]
	vmovq	xmm0, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 152]
	vmovq	xmm1, rbx
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 144]
	vmovq	xmm2, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vmovq	xmm1, rbx
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 184]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 176]
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 168]
	vmovq	xmm3, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 160]
	vmovq	xmm4, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 216]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 208]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 200]
	vmovq	xmm7, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 192]
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vpunpcklqdq	xmm4, xmm6, xmm5        # xmm4 = xmm6[0],xmm5[0]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm5, xmm5, xmm7        # xmm5 = xmm5[0],xmm7[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 248]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 240]
	vmovq	xmm7, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 232]
	vmovq	xmm7, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 224]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rsi + 8*rdx + 144], xmm1
	vmovdqu	xmmword ptr [rsi + 8*rdx + 128], xmm8
	vmovdqu	xmmword ptr [rsi + 8*rdx + 160], xmm3
	vmovdqu	xmmword ptr [rsi + 8*rdx + 176], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rdx + 192], xmm5
	vmovdqu	xmmword ptr [rsi + 8*rdx + 208], xmm4
	vmovdqu	xmmword ptr [rsi + 8*rdx + 224], xmm0
	vmovdqu	xmmword ptr [rsi + 8*rdx + 240], xmm6
	add	rdx, 32
	add	r10, 2
	jne	.LBB79_6
# %bb.7:
	test	r8b, 1
	je	.LBB79_9
.LBB79_8:
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 8]
	vmovq	xmm0, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx]
	vmovq	xmm1, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 24]
	vmovq	xmm1, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 16]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 56]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 48]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 40]
	vmovq	xmm3, rbx
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 32]
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovq	xmm3, rax
	vmovq	xmm4, rbx
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 88]
	vmovq	xmm4, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 80]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 72]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 64]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 120]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 112]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdi + 8*rdx + 104]
	vmovq	xmm7, rbx
	vcvttsd2si	rbx, qword ptr [rdi + 8*rdx + 96]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vmovdqu	xmmword ptr [rsi + 8*rdx + 16], xmm1
	vmovdqu	xmmword ptr [rsi + 8*rdx], xmm8
	vmovdqu	xmmword ptr [rsi + 8*rdx + 32], xmm3
	vmovdqu	xmmword ptr [rsi + 8*rdx + 48], xmm2
	vmovdqu	xmmword ptr [rsi + 8*rdx + 64], xmm5
	vmovdqu	xmmword ptr [rsi + 8*rdx + 80], xmm4
	vmovdqu	xmmword ptr [rsi + 8*rdx + 96], xmm0
	vmovdqu	xmmword ptr [rsi + 8*rdx + 112], xmm6
.LBB79_9:
	cmp	rcx, r9
	je	.LBB79_11
	.p2align	4, 0x90
.LBB79_10:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB79_10
.LBB79_11:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	ret
.LBB79_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB79_8
	jmp	.LBB79_9
.Lfunc_end79:
	.size	cast_numeric_float64_int64_avx2, .Lfunc_end79-cast_numeric_float64_int64_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_float32_avx2 # -- Begin function cast_numeric_uint8_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_float32_avx2,@function
cast_numeric_uint8_float32_avx2:        # @cast_numeric_uint8_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB80_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB80_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB80_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB80_9
.LBB80_2:
	xor	ecx, ecx
.LBB80_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB80_5
	.p2align	4, 0x90
.LBB80_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	add	rdx, -1
	jne	.LBB80_4
.LBB80_5:
	cmp	r8, 3
	jb	.LBB80_16
	.p2align	4, 0x90
.LBB80_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	movzx	eax, byte ptr [rdi + rcx + 1]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx + 4], xmm0
	movzx	eax, byte ptr [rdi + rcx + 2]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx + 8], xmm0
	movzx	eax, byte ptr [rdi + rcx + 3]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx + 12], xmm0
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB80_6
	jmp	.LBB80_16
.LBB80_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB80_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB80_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	ymm0, qword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovzxbd	ymm0, qword ptr [rdi + rdx + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB80_12
# %bb.13:
	test	r8b, 1
	je	.LBB80_15
.LBB80_14:
	vpmovzxbd	ymm0, qword ptr [rdi + rdx] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdi + rdx + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdi + rdx + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdi + rdx + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB80_15:
	cmp	rcx, r9
	jne	.LBB80_3
.LBB80_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB80_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB80_14
	jmp	.LBB80_15
.Lfunc_end80:
	.size	cast_numeric_uint8_float32_avx2, .Lfunc_end80-cast_numeric_uint8_float32_avx2
                                        # -- End function
	.globl	cast_numeric_int8_float32_avx2  # -- Begin function cast_numeric_int8_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_float32_avx2,@function
cast_numeric_int8_float32_avx2:         # @cast_numeric_int8_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB81_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB81_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB81_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB81_9
.LBB81_2:
	xor	ecx, ecx
.LBB81_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB81_5
	.p2align	4, 0x90
.LBB81_4:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	add	rdx, -1
	jne	.LBB81_4
.LBB81_5:
	cmp	r8, 3
	jb	.LBB81_16
	.p2align	4, 0x90
.LBB81_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	movsx	eax, byte ptr [rdi + rcx + 1]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx + 4], xmm0
	movsx	eax, byte ptr [rdi + rcx + 2]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx + 8], xmm0
	movsx	eax, byte ptr [rdi + rcx + 3]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx + 12], xmm0
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB81_6
	jmp	.LBB81_16
.LBB81_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB81_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB81_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm0, qword ptr [rdi + rdx]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 8]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 16]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 24]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovsxbd	ymm0, qword ptr [rdi + rdx + 32]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 40]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 48]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 56]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB81_12
# %bb.13:
	test	r8b, 1
	je	.LBB81_15
.LBB81_14:
	vpmovsxbd	ymm0, qword ptr [rdi + rdx]
	vpmovsxbd	ymm1, qword ptr [rdi + rdx + 8]
	vpmovsxbd	ymm2, qword ptr [rdi + rdx + 16]
	vpmovsxbd	ymm3, qword ptr [rdi + rdx + 24]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB81_15:
	cmp	rcx, r9
	jne	.LBB81_3
.LBB81_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB81_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB81_14
	jmp	.LBB81_15
.Lfunc_end81:
	.size	cast_numeric_int8_float32_avx2, .Lfunc_end81-cast_numeric_int8_float32_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_float32_avx2 # -- Begin function cast_numeric_uint16_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_float32_avx2,@function
cast_numeric_uint16_float32_avx2:       # @cast_numeric_uint16_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB82_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB82_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB82_10
.LBB82_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB82_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB82_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB82_6
# %bb.7:
	test	r8b, 1
	je	.LBB82_9
.LBB82_8:
	vpmovzxwd	ymm0, xmmword ptr [rdi + 2*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB82_9:
	cmp	rcx, r9
	je	.LBB82_11
	.p2align	4, 0x90
.LBB82_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB82_10
.LBB82_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB82_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB82_8
	jmp	.LBB82_9
.Lfunc_end82:
	.size	cast_numeric_uint16_float32_avx2, .Lfunc_end82-cast_numeric_uint16_float32_avx2
                                        # -- End function
	.globl	cast_numeric_int16_float32_avx2 # -- Begin function cast_numeric_int16_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_float32_avx2,@function
cast_numeric_int16_float32_avx2:        # @cast_numeric_int16_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB83_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB83_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB83_10
.LBB83_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB83_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB83_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx + 64]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 80]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 96]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 112]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB83_6
# %bb.7:
	test	r8b, 1
	je	.LBB83_9
.LBB83_8:
	vpmovsxwd	ymm0, xmmword ptr [rdi + 2*rdx]
	vpmovsxwd	ymm1, xmmword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdi + 2*rdx + 48]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB83_9:
	cmp	rcx, r9
	je	.LBB83_11
	.p2align	4, 0x90
.LBB83_10:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdi + 2*rcx]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB83_10
.LBB83_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB83_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB83_8
	jmp	.LBB83_9
.Lfunc_end83:
	.size	cast_numeric_int16_float32_avx2, .Lfunc_end83-cast_numeric_int16_float32_avx2
                                        # -- End function
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2                               # -- Begin function cast_numeric_uint32_float32_avx2
.LCPI84_0:
	.long	1258291200                      # 0x4b000000
.LCPI84_1:
	.long	1392508928                      # 0x53000000
.LCPI84_2:
	.long	0x53000080                      # float 5.49764202E+11
	.text
	.globl	cast_numeric_uint32_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_float32_avx2,@function
cast_numeric_uint32_float32_avx2:       # @cast_numeric_uint32_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB84_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB84_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB84_10
.LBB84_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB84_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI84_0] # ymm0 = [1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200]
	xor	edx, edx
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI84_1] # ymm1 = [1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928]
	vbroadcastss	ymm2, dword ptr [rip + .LCPI84_2] # ymm2 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
	.p2align	4, 0x90
.LBB84_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm5, ymmword ptr [rdi + 4*rdx + 64]
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rdx + 96]
	vpblendw	ymm7, ymm3, ymm0, 170           # ymm7 = ymm3[0],ymm0[1],ymm3[2],ymm0[3],ymm3[4],ymm0[5],ymm3[6],ymm0[7],ymm3[8],ymm0[9],ymm3[10],ymm0[11],ymm3[12],ymm0[13],ymm3[14],ymm0[15]
	vpsrld	ymm3, ymm3, 16
	vpblendw	ymm3, ymm3, ymm1, 170           # ymm3 = ymm3[0],ymm1[1],ymm3[2],ymm1[3],ymm3[4],ymm1[5],ymm3[6],ymm1[7],ymm3[8],ymm1[9],ymm3[10],ymm1[11],ymm3[12],ymm1[13],ymm3[14],ymm1[15]
	vsubps	ymm3, ymm3, ymm2
	vaddps	ymm3, ymm7, ymm3
	vpblendw	ymm7, ymm4, ymm0, 170           # ymm7 = ymm4[0],ymm0[1],ymm4[2],ymm0[3],ymm4[4],ymm0[5],ymm4[6],ymm0[7],ymm4[8],ymm0[9],ymm4[10],ymm0[11],ymm4[12],ymm0[13],ymm4[14],ymm0[15]
	vpsrld	ymm4, ymm4, 16
	vpblendw	ymm4, ymm4, ymm1, 170           # ymm4 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7],ymm4[8],ymm1[9],ymm4[10],ymm1[11],ymm4[12],ymm1[13],ymm4[14],ymm1[15]
	vsubps	ymm4, ymm4, ymm2
	vaddps	ymm4, ymm7, ymm4
	vpblendw	ymm7, ymm5, ymm0, 170           # ymm7 = ymm5[0],ymm0[1],ymm5[2],ymm0[3],ymm5[4],ymm0[5],ymm5[6],ymm0[7],ymm5[8],ymm0[9],ymm5[10],ymm0[11],ymm5[12],ymm0[13],ymm5[14],ymm0[15]
	vpsrld	ymm5, ymm5, 16
	vpblendw	ymm5, ymm5, ymm1, 170           # ymm5 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7],ymm5[8],ymm1[9],ymm5[10],ymm1[11],ymm5[12],ymm1[13],ymm5[14],ymm1[15]
	vsubps	ymm5, ymm5, ymm2
	vaddps	ymm5, ymm7, ymm5
	vpblendw	ymm7, ymm6, ymm0, 170           # ymm7 = ymm6[0],ymm0[1],ymm6[2],ymm0[3],ymm6[4],ymm0[5],ymm6[6],ymm0[7],ymm6[8],ymm0[9],ymm6[10],ymm0[11],ymm6[12],ymm0[13],ymm6[14],ymm0[15]
	vpsrld	ymm6, ymm6, 16
	vpblendw	ymm6, ymm6, ymm1, 170           # ymm6 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7],ymm6[8],ymm1[9],ymm6[10],ymm1[11],ymm6[12],ymm1[13],ymm6[14],ymm1[15]
	vsubps	ymm6, ymm6, ymm2
	vaddps	ymm6, ymm7, ymm6
	vmovups	ymmword ptr [rsi + 4*rdx], ymm3
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm4
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm5
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm6
	vmovdqu	ymm3, ymmword ptr [rdi + 4*rdx + 128]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 160]
	vmovdqu	ymm5, ymmword ptr [rdi + 4*rdx + 192]
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rdx + 224]
	vpblendw	ymm7, ymm3, ymm0, 170           # ymm7 = ymm3[0],ymm0[1],ymm3[2],ymm0[3],ymm3[4],ymm0[5],ymm3[6],ymm0[7],ymm3[8],ymm0[9],ymm3[10],ymm0[11],ymm3[12],ymm0[13],ymm3[14],ymm0[15]
	vpsrld	ymm3, ymm3, 16
	vpblendw	ymm3, ymm3, ymm1, 170           # ymm3 = ymm3[0],ymm1[1],ymm3[2],ymm1[3],ymm3[4],ymm1[5],ymm3[6],ymm1[7],ymm3[8],ymm1[9],ymm3[10],ymm1[11],ymm3[12],ymm1[13],ymm3[14],ymm1[15]
	vsubps	ymm3, ymm3, ymm2
	vaddps	ymm3, ymm7, ymm3
	vpblendw	ymm7, ymm4, ymm0, 170           # ymm7 = ymm4[0],ymm0[1],ymm4[2],ymm0[3],ymm4[4],ymm0[5],ymm4[6],ymm0[7],ymm4[8],ymm0[9],ymm4[10],ymm0[11],ymm4[12],ymm0[13],ymm4[14],ymm0[15]
	vpsrld	ymm4, ymm4, 16
	vpblendw	ymm4, ymm4, ymm1, 170           # ymm4 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7],ymm4[8],ymm1[9],ymm4[10],ymm1[11],ymm4[12],ymm1[13],ymm4[14],ymm1[15]
	vsubps	ymm4, ymm4, ymm2
	vaddps	ymm4, ymm7, ymm4
	vpblendw	ymm7, ymm5, ymm0, 170           # ymm7 = ymm5[0],ymm0[1],ymm5[2],ymm0[3],ymm5[4],ymm0[5],ymm5[6],ymm0[7],ymm5[8],ymm0[9],ymm5[10],ymm0[11],ymm5[12],ymm0[13],ymm5[14],ymm0[15]
	vpsrld	ymm5, ymm5, 16
	vpblendw	ymm5, ymm5, ymm1, 170           # ymm5 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7],ymm5[8],ymm1[9],ymm5[10],ymm1[11],ymm5[12],ymm1[13],ymm5[14],ymm1[15]
	vsubps	ymm5, ymm5, ymm2
	vaddps	ymm5, ymm7, ymm5
	vpblendw	ymm7, ymm6, ymm0, 170           # ymm7 = ymm6[0],ymm0[1],ymm6[2],ymm0[3],ymm6[4],ymm0[5],ymm6[6],ymm0[7],ymm6[8],ymm0[9],ymm6[10],ymm0[11],ymm6[12],ymm0[13],ymm6[14],ymm0[15]
	vpsrld	ymm6, ymm6, 16
	vpblendw	ymm6, ymm6, ymm1, 170           # ymm6 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7],ymm6[8],ymm1[9],ymm6[10],ymm1[11],ymm6[12],ymm1[13],ymm6[14],ymm1[15]
	vsubps	ymm6, ymm6, ymm2
	vaddps	ymm6, ymm7, ymm6
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm3
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm4
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm5
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm6
	add	rdx, 64
	add	rax, 2
	jne	.LBB84_6
# %bb.7:
	test	r8b, 1
	je	.LBB84_9
.LBB84_8:
	vmovdqu	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovdqu	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovdqu	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vpbroadcastd	ymm3, dword ptr [rip + .LCPI84_0] # ymm3 = [1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200]
	vmovdqu	ymm4, ymmword ptr [rdi + 4*rdx + 96]
	vpblendw	ymm5, ymm0, ymm3, 170           # ymm5 = ymm0[0],ymm3[1],ymm0[2],ymm3[3],ymm0[4],ymm3[5],ymm0[6],ymm3[7],ymm0[8],ymm3[9],ymm0[10],ymm3[11],ymm0[12],ymm3[13],ymm0[14],ymm3[15]
	vpbroadcastd	ymm6, dword ptr [rip + .LCPI84_1] # ymm6 = [1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928]
	vpsrld	ymm0, ymm0, 16
	vpblendw	ymm0, ymm0, ymm6, 170           # ymm0 = ymm0[0],ymm6[1],ymm0[2],ymm6[3],ymm0[4],ymm6[5],ymm0[6],ymm6[7],ymm0[8],ymm6[9],ymm0[10],ymm6[11],ymm0[12],ymm6[13],ymm0[14],ymm6[15]
	vbroadcastss	ymm7, dword ptr [rip + .LCPI84_2] # ymm7 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
	vsubps	ymm0, ymm0, ymm7
	vaddps	ymm0, ymm5, ymm0
	vpblendw	ymm5, ymm1, ymm3, 170           # ymm5 = ymm1[0],ymm3[1],ymm1[2],ymm3[3],ymm1[4],ymm3[5],ymm1[6],ymm3[7],ymm1[8],ymm3[9],ymm1[10],ymm3[11],ymm1[12],ymm3[13],ymm1[14],ymm3[15]
	vpsrld	ymm1, ymm1, 16
	vpblendw	ymm1, ymm1, ymm6, 170           # ymm1 = ymm1[0],ymm6[1],ymm1[2],ymm6[3],ymm1[4],ymm6[5],ymm1[6],ymm6[7],ymm1[8],ymm6[9],ymm1[10],ymm6[11],ymm1[12],ymm6[13],ymm1[14],ymm6[15]
	vsubps	ymm1, ymm1, ymm7
	vaddps	ymm1, ymm5, ymm1
	vpblendw	ymm5, ymm2, ymm3, 170           # ymm5 = ymm2[0],ymm3[1],ymm2[2],ymm3[3],ymm2[4],ymm3[5],ymm2[6],ymm3[7],ymm2[8],ymm3[9],ymm2[10],ymm3[11],ymm2[12],ymm3[13],ymm2[14],ymm3[15]
	vpsrld	ymm2, ymm2, 16
	vpblendw	ymm2, ymm2, ymm6, 170           # ymm2 = ymm2[0],ymm6[1],ymm2[2],ymm6[3],ymm2[4],ymm6[5],ymm2[6],ymm6[7],ymm2[8],ymm6[9],ymm2[10],ymm6[11],ymm2[12],ymm6[13],ymm2[14],ymm6[15]
	vsubps	ymm2, ymm2, ymm7
	vaddps	ymm2, ymm5, ymm2
	vpblendw	ymm3, ymm4, ymm3, 170           # ymm3 = ymm4[0],ymm3[1],ymm4[2],ymm3[3],ymm4[4],ymm3[5],ymm4[6],ymm3[7],ymm4[8],ymm3[9],ymm4[10],ymm3[11],ymm4[12],ymm3[13],ymm4[14],ymm3[15]
	vpsrld	ymm4, ymm4, 16
	vpblendw	ymm4, ymm4, ymm6, 170           # ymm4 = ymm4[0],ymm6[1],ymm4[2],ymm6[3],ymm4[4],ymm6[5],ymm4[6],ymm6[7],ymm4[8],ymm6[9],ymm4[10],ymm6[11],ymm4[12],ymm6[13],ymm4[14],ymm6[15]
	vsubps	ymm4, ymm4, ymm7
	vaddps	ymm3, ymm3, ymm4
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB84_9:
	cmp	rcx, r9
	je	.LBB84_11
	.p2align	4, 0x90
.LBB84_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	vcvtsi2ss	xmm0, xmm8, rax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB84_10
.LBB84_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB84_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB84_8
	jmp	.LBB84_9
.Lfunc_end84:
	.size	cast_numeric_uint32_float32_avx2, .Lfunc_end84-cast_numeric_uint32_float32_avx2
                                        # -- End function
	.globl	cast_numeric_int32_float32_avx2 # -- Begin function cast_numeric_int32_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_float32_avx2,@function
cast_numeric_int32_float32_avx2:        # @cast_numeric_int32_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB85_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	ja	.LBB85_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB85_10
.LBB85_3:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB85_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB85_6:                               # =>This Inner Loop Header: Depth=1
	vcvtdq2ps	ymm0, ymmword ptr [rdi + 4*rdx]
	vcvtdq2ps	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vcvtdq2ps	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vcvtdq2ps	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vcvtdq2ps	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vcvtdq2ps	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vcvtdq2ps	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vcvtdq2ps	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB85_6
# %bb.7:
	test	r8b, 1
	je	.LBB85_9
.LBB85_8:
	vcvtdq2ps	ymm0, ymmword ptr [rdi + 4*rdx]
	vcvtdq2ps	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vcvtdq2ps	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vcvtdq2ps	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB85_9:
	cmp	rcx, r9
	je	.LBB85_11
	.p2align	4, 0x90
.LBB85_10:                              # =>This Inner Loop Header: Depth=1
	vcvtsi2ss	xmm0, xmm4, dword ptr [rdi + 4*rcx]
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB85_10
.LBB85_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB85_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB85_8
	jmp	.LBB85_9
.Lfunc_end85:
	.size	cast_numeric_int32_float32_avx2, .Lfunc_end85-cast_numeric_int32_float32_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_numeric_uint64_float32_avx2
.LCPI86_0:
	.quad	1                               # 0x1
	.text
	.globl	cast_numeric_uint64_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_float32_avx2,@function
cast_numeric_uint64_float32_avx2:       # @cast_numeric_uint64_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB86_15
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	ja	.LBB86_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB86_11
.LBB86_3:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB86_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB86_7
.LBB86_5:
	and	r10, -4
	neg	r10
	xor	eax, eax
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI86_0] # ymm0 = [1,1,1,1]
	.p2align	4, 0x90
.LBB86_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdx, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdx
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm3, xmm5, rdx
	vextracti128	xmm1, ymm1, 1
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm4, xmm5, rdx
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vpextrq	rdx, xmm1, 1
	vinsertps	xmm1, xmm2, xmm4, 32    # xmm1 = xmm2[0,1],xmm4[0],xmm2[3]
	vcvtsi2ss	xmm2, xmm5, rdx
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rax]
	vpackssdw	xmm2, xmm2, xmmword ptr [rdi + 8*rax + 16]
	vaddps	xmm3, xmm1, xmm1
	vblendvps	xmm1, xmm1, xmm3, xmm2
	vmovups	xmmword ptr [rsi + 4*rax], xmm1
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 32]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdx, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdx
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm3, xmm5, rdx
	vextracti128	xmm1, ymm1, 1
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm4, xmm5, rdx
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vpextrq	rdx, xmm1, 1
	vinsertps	xmm1, xmm2, xmm4, 32    # xmm1 = xmm2[0,1],xmm4[0],xmm2[3]
	vcvtsi2ss	xmm2, xmm5, rdx
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rax + 32]
	vpackssdw	xmm2, xmm2, xmmword ptr [rdi + 8*rax + 48]
	vaddps	xmm3, xmm1, xmm1
	vblendvps	xmm1, xmm1, xmm3, xmm2
	vmovups	xmmword ptr [rsi + 4*rax + 16], xmm1
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 64]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdx, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdx
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm3, xmm5, rdx
	vextracti128	xmm1, ymm1, 1
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm4, xmm5, rdx
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vpextrq	rdx, xmm1, 1
	vinsertps	xmm1, xmm2, xmm4, 32    # xmm1 = xmm2[0,1],xmm4[0],xmm2[3]
	vcvtsi2ss	xmm2, xmm5, rdx
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rax + 64]
	vpackssdw	xmm2, xmm2, xmmword ptr [rdi + 8*rax + 80]
	vaddps	xmm3, xmm1, xmm1
	vblendvps	xmm1, xmm1, xmm3, xmm2
	vmovups	xmmword ptr [rsi + 4*rax + 32], xmm1
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 96]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdx, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdx
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm3, xmm5, rdx
	vextracti128	xmm1, ymm1, 1
	vpextrq	r11, xmm1, 1
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm1, xmm5, rdx
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vcvtsi2ss	xmm3, xmm5, r11
	vinsertps	xmm1, xmm2, xmm1, 32    # xmm1 = xmm2[0,1],xmm1[0],xmm2[3]
	vinsertps	xmm1, xmm1, xmm3, 48    # xmm1 = xmm1[0,1,2],xmm3[0]
	vaddps	xmm2, xmm1, xmm1
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rax + 96]
	vpackssdw	xmm3, xmm3, xmmword ptr [rdi + 8*rax + 112]
	vblendvps	xmm1, xmm1, xmm2, xmm3
	vmovups	xmmword ptr [rsi + 4*rax + 48], xmm1
	add	rax, 16
	add	r10, 4
	jne	.LBB86_6
.LBB86_7:
	test	r9, r9
	je	.LBB86_10
# %bb.8:
	shl	rax, 2
	neg	r9
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI86_0] # ymm0 = [1,1,1,1]
	.p2align	4, 0x90
.LBB86_9:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 2*rax]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdx, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdx
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm3, xmm5, rdx
	vextracti128	xmm1, ymm1, 1
	vpextrq	r10, xmm1, 1
	vmovq	rdx, xmm1
	vcvtsi2ss	xmm1, xmm5, rdx
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vcvtsi2ss	xmm3, xmm5, r10
	vinsertps	xmm1, xmm2, xmm1, 32    # xmm1 = xmm2[0,1],xmm1[0],xmm2[3]
	vinsertps	xmm1, xmm1, xmm3, 48    # xmm1 = xmm1[0,1,2],xmm3[0]
	vaddps	xmm2, xmm1, xmm1
	vmovdqu	xmm3, xmmword ptr [rdi + 2*rax]
	vpackssdw	xmm3, xmm3, xmmword ptr [rdi + 2*rax + 16]
	vblendvps	xmm1, xmm1, xmm2, xmm3
	vmovups	xmmword ptr [rsi + rax], xmm1
	add	rax, 16
	inc	r9
	jne	.LBB86_9
.LBB86_10:
	cmp	rcx, r8
	jne	.LBB86_11
	jmp	.LBB86_15
	.p2align	4, 0x90
.LBB86_13:                              #   in Loop: Header=BB86_11 Depth=1
	vcvtsi2ss	xmm0, xmm5, rax
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r8, rcx
	je	.LBB86_15
.LBB86_11:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	test	rax, rax
	jns	.LBB86_13
# %bb.12:                               #   in Loop: Header=BB86_11 Depth=1
	mov	rdx, rax
	shr	rdx
	and	eax, 1
	or	rax, rdx
	vcvtsi2ss	xmm0, xmm5, rax
	vaddss	xmm0, xmm0, xmm0
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r8, rcx
	jne	.LBB86_11
.LBB86_15:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end86:
	.size	cast_numeric_uint64_float32_avx2, .Lfunc_end86-cast_numeric_uint64_float32_avx2
                                        # -- End function
	.globl	cast_numeric_int64_float32_avx2 # -- Begin function cast_numeric_int64_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_float32_avx2,@function
cast_numeric_int64_float32_avx2:        # @cast_numeric_int64_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB87_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB87_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB87_10
.LBB87_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB87_4
# %bb.5:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edx, edx
	.p2align	4, 0x90
.LBB87_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	vpextrq	rax, xmm0, 1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm0
	vcvtsi2ss	xmm0, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm3, xmm8, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2ss	xmm1, xmm8, rax
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 32]
	vpextrq	rax, xmm4, 1
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 48]
	vcvtsi2ss	xmm6, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm5
	vcvtsi2ss	xmm7, xmm8, rax
	vinsertps	xmm0, xmm0, xmm2, 16    # xmm0 = xmm0[0],xmm2[0],xmm0[2,3]
	vinsertps	xmm0, xmm0, xmm3, 32    # xmm0 = xmm0[0,1],xmm3[0],xmm0[3]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm0, xmm0, xmm1, 48    # xmm0 = xmm0[0,1,2],xmm1[0]
	vcvtsi2ss	xmm1, xmm8, rax
	vinsertps	xmm2, xmm4, xmm6, 16    # xmm2 = xmm4[0],xmm6[0],xmm4[2,3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpextrq	rax, xmm3, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 80]
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm2, xmm2, xmm7, 32    # xmm2 = xmm2[0,1],xmm7[0],xmm2[3]
	vinsertps	xmm1, xmm2, xmm1, 48    # xmm1 = xmm2[0,1,2],xmm1[0]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm2, xmm3, xmm4, 16    # xmm2 = xmm3[0],xmm4[0],xmm3[2,3]
	vcvtsi2ss	xmm3, xmm8, rax
	vinsertps	xmm2, xmm2, xmm6, 32    # xmm2 = xmm2[0,1],xmm6[0],xmm2[3]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 96]
	vpextrq	rax, xmm4, 1
	vcvtsi2ss	xmm5, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vmovdqu	xmm6, xmmword ptr [rdi + 8*rdx + 112]
	vmovq	rax, xmm6
	vcvtsi2ss	xmm7, xmm8, rax
	vinsertps	xmm2, xmm2, xmm3, 48    # xmm2 = xmm2[0,1,2],xmm3[0]
	vinsertps	xmm3, xmm4, xmm5, 16    # xmm3 = xmm4[0],xmm5[0],xmm4[2,3]
	vpextrq	rax, xmm6, 1
	vinsertps	xmm3, xmm3, xmm7, 32    # xmm3 = xmm3[0,1],xmm7[0],xmm3[3]
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm3, xmm3, xmm4, 48    # xmm3 = xmm3[0,1,2],xmm4[0]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vmovdqu	xmm0, xmmword ptr [rdi + 8*rdx + 128]
	vpextrq	rax, xmm0, 1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 144]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm0
	vcvtsi2ss	xmm0, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm3, xmm8, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2ss	xmm1, xmm8, rax
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 160]
	vpextrq	rax, xmm4, 1
	vcvtsi2ss	xmm5, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm0, xmm0, xmm2, 16    # xmm0 = xmm0[0],xmm2[0],xmm0[2,3]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 176]
	vpextrq	r11, xmm2, 1
	vmovq	rax, xmm2
	vcvtsi2ss	xmm2, xmm8, rax
	vinsertps	xmm0, xmm0, xmm3, 32    # xmm0 = xmm0[0,1],xmm3[0],xmm0[3]
	vcvtsi2ss	xmm3, xmm8, r11
	vinsertps	xmm0, xmm0, xmm1, 48    # xmm0 = xmm0[0,1,2],xmm1[0]
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 192]
	vpextrq	rax, xmm1, 1
	vinsertps	xmm4, xmm4, xmm5, 16    # xmm4 = xmm4[0],xmm5[0],xmm4[2,3]
	vcvtsi2ss	xmm5, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm1, xmm8, rax
	vinsertps	xmm2, xmm4, xmm2, 32    # xmm2 = xmm4[0,1],xmm2[0],xmm4[3]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 208]
	vpextrq	r11, xmm4, 1
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm2, xmm2, xmm3, 48    # xmm2 = xmm2[0,1,2],xmm3[0]
	vcvtsi2ss	xmm3, xmm8, r11
	vinsertps	xmm1, xmm1, xmm5, 16    # xmm1 = xmm1[0],xmm5[0],xmm1[2,3]
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 224]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm1, xmm1, xmm4, 32    # xmm1 = xmm1[0,1],xmm4[0],xmm1[3]
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm5
	vcvtsi2ss	xmm5, xmm8, rax
	vinsertps	xmm1, xmm1, xmm3, 48    # xmm1 = xmm1[0,1,2],xmm3[0]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 240]
	vpextrq	r11, xmm3, 1
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vinsertps	xmm4, xmm5, xmm4, 16    # xmm4 = xmm5[0],xmm4[0],xmm5[2,3]
	vcvtsi2ss	xmm5, xmm8, r11
	vinsertps	xmm3, xmm4, xmm3, 32    # xmm3 = xmm4[0,1],xmm3[0],xmm4[3]
	vinsertps	xmm3, xmm3, xmm5, 48    # xmm3 = xmm3[0,1,2],xmm5[0]
	vmovups	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 80], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 96], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	r10, 2
	jne	.LBB87_6
# %bb.7:
	test	r8b, 1
	je	.LBB87_9
.LBB87_8:
	vmovdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	vpextrq	rax, xmm0, 1
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm0
	vcvtsi2ss	xmm0, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm3, xmm8, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2ss	xmm1, xmm8, rax
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 48]
	vpextrq	rax, xmm4, 1
	vinsertps	xmm0, xmm0, xmm2, 16    # xmm0 = xmm0[0],xmm2[0],xmm0[2,3]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm0, xmm0, xmm3, 32    # xmm0 = xmm0[0,1],xmm3[0],xmm0[3]
	vinsertps	xmm0, xmm0, xmm1, 48    # xmm0 = xmm0[0,1,2],xmm1[0]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm1, xmm4, xmm2, 16    # xmm1 = xmm4[0],xmm2[0],xmm4[2,3]
	vcvtsi2ss	xmm2, xmm8, rax
	vinsertps	xmm1, xmm1, xmm6, 32    # xmm1 = xmm1[0,1],xmm6[0],xmm1[3]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 64]
	vpextrq	rax, xmm3, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 80]
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vinsertps	xmm2, xmm3, xmm4, 16    # xmm2 = xmm3[0],xmm4[0],xmm3[2,3]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm2, xmm2, xmm6, 32    # xmm2 = xmm2[0,1],xmm6[0],xmm2[3]
	vcvtsi2ss	xmm3, xmm8, rax
	vinsertps	xmm2, xmm2, xmm3, 48    # xmm2 = xmm2[0,1,2],xmm3[0]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 96]
	vpextrq	rax, xmm3, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 112]
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm3, xmm3, xmm4, 16    # xmm3 = xmm3[0],xmm4[0],xmm3[2,3]
	vinsertps	xmm3, xmm3, xmm6, 32    # xmm3 = xmm3[0,1],xmm6[0],xmm3[3]
	vpextrq	rax, xmm5, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm3, xmm3, xmm4, 48    # xmm3 = xmm3[0,1,2],xmm4[0]
	vmovups	xmmword ptr [rsi + 4*rdx], xmm0
	vmovups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovups	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovups	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB87_9:
	cmp	rcx, r9
	je	.LBB87_11
	.p2align	4, 0x90
.LBB87_10:                              # =>This Inner Loop Header: Depth=1
	vcvtsi2ss	xmm0, xmm8, qword ptr [rdi + 8*rcx]
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB87_10
.LBB87_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB87_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB87_8
	jmp	.LBB87_9
.Lfunc_end87:
	.size	cast_numeric_int64_float32_avx2, .Lfunc_end87-cast_numeric_int64_float32_avx2
                                        # -- End function
	.globl	cast_numeric_float32_float32_avx2 # -- Begin function cast_numeric_float32_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_float32_avx2,@function
cast_numeric_float32_float32_avx2:      # @cast_numeric_float32_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB88_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB88_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB88_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB88_9
.LBB88_2:
	xor	ecx, ecx
.LBB88_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 7
	je	.LBB88_5
	.p2align	4, 0x90
.LBB88_4:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB88_4
.LBB88_5:
	cmp	r8, 7
	jb	.LBB88_16
	.p2align	4, 0x90
.LBB88_6:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	mov	eax, dword ptr [rdi + 4*rcx + 4]
	mov	dword ptr [rsi + 4*rcx + 4], eax
	mov	eax, dword ptr [rdi + 4*rcx + 8]
	mov	dword ptr [rsi + 4*rcx + 8], eax
	mov	eax, dword ptr [rdi + 4*rcx + 12]
	mov	dword ptr [rsi + 4*rcx + 12], eax
	mov	eax, dword ptr [rdi + 4*rcx + 16]
	mov	dword ptr [rsi + 4*rcx + 16], eax
	mov	eax, dword ptr [rdi + 4*rcx + 20]
	mov	dword ptr [rsi + 4*rcx + 20], eax
	mov	eax, dword ptr [rdi + 4*rcx + 24]
	mov	dword ptr [rsi + 4*rcx + 24], eax
	mov	eax, dword ptr [rdi + 4*rcx + 28]
	mov	dword ptr [rsi + 4*rcx + 28], eax
	add	rcx, 8
	cmp	r9, rcx
	jne	.LBB88_6
	jmp	.LBB88_16
.LBB88_9:
	mov	ecx, r9d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB88_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB88_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 224]
	vmovups	ymmword ptr [rsi + 4*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 224], ymm3
	add	rdx, 64
	add	rax, 2
	jne	.LBB88_12
# %bb.13:
	test	r8b, 1
	je	.LBB88_15
.LBB88_14:
	vmovups	ymm0, ymmword ptr [rdi + 4*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 4*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 4*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 4*rdx + 96]
	vmovups	ymmword ptr [rsi + 4*rdx], ymm0
	vmovups	ymmword ptr [rsi + 4*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 4*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 4*rdx + 96], ymm3
.LBB88_15:
	cmp	rcx, r9
	jne	.LBB88_3
.LBB88_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB88_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB88_14
	jmp	.LBB88_15
.Lfunc_end88:
	.size	cast_numeric_float32_float32_avx2, .Lfunc_end88-cast_numeric_float32_float32_avx2
                                        # -- End function
	.globl	cast_numeric_float64_float32_avx2 # -- Begin function cast_numeric_float64_float32_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_float32_avx2,@function
cast_numeric_float64_float32_avx2:      # @cast_numeric_float64_float32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB89_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB89_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB89_10
.LBB89_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB89_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB89_6:                               # =>This Inner Loop Header: Depth=1
	vcvtpd2ps	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvtpd2ps	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvtpd2ps	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvtpd2ps	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovupd	xmmword ptr [rsi + 4*rdx], xmm0
	vmovupd	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovupd	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovupd	xmmword ptr [rsi + 4*rdx + 48], xmm3
	vcvtpd2ps	xmm0, ymmword ptr [rdi + 8*rdx + 128]
	vcvtpd2ps	xmm1, ymmword ptr [rdi + 8*rdx + 160]
	vcvtpd2ps	xmm2, ymmword ptr [rdi + 8*rdx + 192]
	vcvtpd2ps	xmm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovupd	xmmword ptr [rsi + 4*rdx + 64], xmm0
	vmovupd	xmmword ptr [rsi + 4*rdx + 80], xmm1
	vmovupd	xmmword ptr [rsi + 4*rdx + 96], xmm2
	vmovupd	xmmword ptr [rsi + 4*rdx + 112], xmm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB89_6
# %bb.7:
	test	r8b, 1
	je	.LBB89_9
.LBB89_8:
	vcvtpd2ps	xmm0, ymmword ptr [rdi + 8*rdx]
	vcvtpd2ps	xmm1, ymmword ptr [rdi + 8*rdx + 32]
	vcvtpd2ps	xmm2, ymmword ptr [rdi + 8*rdx + 64]
	vcvtpd2ps	xmm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovupd	xmmword ptr [rsi + 4*rdx], xmm0
	vmovupd	xmmword ptr [rsi + 4*rdx + 16], xmm1
	vmovupd	xmmword ptr [rsi + 4*rdx + 32], xmm2
	vmovupd	xmmword ptr [rsi + 4*rdx + 48], xmm3
.LBB89_9:
	cmp	rcx, r9
	je	.LBB89_11
	.p2align	4, 0x90
.LBB89_10:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdi + 8*rcx]   # xmm0 = mem[0],zero
	vcvtsd2ss	xmm0, xmm0, xmm0
	vmovss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB89_10
.LBB89_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB89_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB89_8
	jmp	.LBB89_9
.Lfunc_end89:
	.size	cast_numeric_float64_float32_avx2, .Lfunc_end89-cast_numeric_float64_float32_avx2
                                        # -- End function
	.globl	cast_numeric_uint8_float64_avx2 # -- Begin function cast_numeric_uint8_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint8_float64_avx2,@function
cast_numeric_uint8_float64_avx2:        # @cast_numeric_uint8_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB90_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB90_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB90_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB90_9
.LBB90_2:
	xor	ecx, ecx
.LBB90_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB90_5
	.p2align	4, 0x90
.LBB90_4:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	add	rdx, -1
	jne	.LBB90_4
.LBB90_5:
	cmp	r8, 3
	jb	.LBB90_16
	.p2align	4, 0x90
.LBB90_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	movzx	eax, byte ptr [rdi + rcx + 1]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx + 8], xmm0
	movzx	eax, byte ptr [rdi + rcx + 2]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx + 16], xmm0
	movzx	eax, byte ptr [rdi + rcx + 3]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx + 24], xmm0
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB90_6
	jmp	.LBB90_16
.LBB90_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB90_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB90_12:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	xmm0, dword ptr [rdi + rdx] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm2, dword ptr [rdi + rdx + 8] # xmm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm3, dword ptr [rdi + rdx + 12] # xmm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxbd	xmm0, dword ptr [rdi + rdx + 16] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm1, dword ptr [rdi + rdx + 20] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm2, dword ptr [rdi + rdx + 24] # xmm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm3, dword ptr [rdi + rdx + 28] # xmm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB90_12
# %bb.13:
	test	r8b, 1
	je	.LBB90_15
.LBB90_14:
	vpmovzxbd	xmm0, dword ptr [rdi + rdx] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm2, dword ptr [rdi + rdx + 8] # xmm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm3, dword ptr [rdi + rdx + 12] # xmm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB90_15:
	cmp	rcx, r9
	jne	.LBB90_3
.LBB90_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB90_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB90_14
	jmp	.LBB90_15
.Lfunc_end90:
	.size	cast_numeric_uint8_float64_avx2, .Lfunc_end90-cast_numeric_uint8_float64_avx2
                                        # -- End function
	.globl	cast_numeric_int8_float64_avx2  # -- Begin function cast_numeric_int8_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int8_float64_avx2,@function
cast_numeric_int8_float64_avx2:         # @cast_numeric_int8_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB91_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB91_2
# %bb.7:
	lea	rax, [rdi + r9]
	cmp	rax, rsi
	jbe	.LBB91_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB91_9
.LBB91_2:
	xor	ecx, ecx
.LBB91_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 3
	je	.LBB91_5
	.p2align	4, 0x90
.LBB91_4:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	add	rdx, -1
	jne	.LBB91_4
.LBB91_5:
	cmp	r8, 3
	jb	.LBB91_16
	.p2align	4, 0x90
.LBB91_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	movsx	eax, byte ptr [rdi + rcx + 1]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx + 8], xmm0
	movsx	eax, byte ptr [rdi + rcx + 2]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx + 16], xmm0
	movsx	eax, byte ptr [rdi + rcx + 3]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx + 24], xmm0
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB91_6
	jmp	.LBB91_16
.LBB91_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB91_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB91_12:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	xmm0, dword ptr [rdi + rdx]
	vpmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	vpmovsxbd	xmm2, dword ptr [rdi + rdx + 8]
	vpmovsxbd	xmm3, dword ptr [rdi + rdx + 12]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxbd	xmm0, dword ptr [rdi + rdx + 16]
	vpmovsxbd	xmm1, dword ptr [rdi + rdx + 20]
	vpmovsxbd	xmm2, dword ptr [rdi + rdx + 24]
	vpmovsxbd	xmm3, dword ptr [rdi + rdx + 28]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB91_12
# %bb.13:
	test	r8b, 1
	je	.LBB91_15
.LBB91_14:
	vpmovsxbd	xmm0, dword ptr [rdi + rdx]
	vpmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	vpmovsxbd	xmm2, dword ptr [rdi + rdx + 8]
	vpmovsxbd	xmm3, dword ptr [rdi + rdx + 12]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB91_15:
	cmp	rcx, r9
	jne	.LBB91_3
.LBB91_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB91_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB91_14
	jmp	.LBB91_15
.Lfunc_end91:
	.size	cast_numeric_int8_float64_avx2, .Lfunc_end91-cast_numeric_int8_float64_avx2
                                        # -- End function
	.globl	cast_numeric_uint16_float64_avx2 # -- Begin function cast_numeric_uint16_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint16_float64_avx2,@function
cast_numeric_uint16_float64_avx2:       # @cast_numeric_uint16_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB92_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB92_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB92_10
.LBB92_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB92_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB92_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxwd	xmm0, qword ptr [rdi + 2*rdx] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm2, qword ptr [rdi + 2*rdx + 16] # xmm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm3, qword ptr [rdi + 2*rdx + 24] # xmm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovzxwd	xmm0, qword ptr [rdi + 2*rdx + 32] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 40] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm2, qword ptr [rdi + 2*rdx + 48] # xmm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm3, qword ptr [rdi + 2*rdx + 56] # xmm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB92_6
# %bb.7:
	test	r8b, 1
	je	.LBB92_9
.LBB92_8:
	vpmovzxwd	xmm0, qword ptr [rdi + 2*rdx] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm2, qword ptr [rdi + 2*rdx + 16] # xmm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm3, qword ptr [rdi + 2*rdx + 24] # xmm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB92_9:
	cmp	rcx, r9
	je	.LBB92_11
	.p2align	4, 0x90
.LBB92_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB92_10
.LBB92_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB92_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB92_8
	jmp	.LBB92_9
.Lfunc_end92:
	.size	cast_numeric_uint16_float64_avx2, .Lfunc_end92-cast_numeric_uint16_float64_avx2
                                        # -- End function
	.globl	cast_numeric_int16_float64_avx2 # -- Begin function cast_numeric_int16_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int16_float64_avx2,@function
cast_numeric_int16_float64_avx2:        # @cast_numeric_int16_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB93_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB93_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB93_10
.LBB93_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB93_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB93_6:                               # =>This Inner Loop Header: Depth=1
	vpmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	vpmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	vpmovsxwd	xmm2, qword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	xmm3, qword ptr [rdi + 2*rdx + 24]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vpmovsxwd	xmm0, qword ptr [rdi + 2*rdx + 32]
	vpmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 40]
	vpmovsxwd	xmm2, qword ptr [rdi + 2*rdx + 48]
	vpmovsxwd	xmm3, qword ptr [rdi + 2*rdx + 56]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB93_6
# %bb.7:
	test	r8b, 1
	je	.LBB93_9
.LBB93_8:
	vpmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	vpmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	vpmovsxwd	xmm2, qword ptr [rdi + 2*rdx + 16]
	vpmovsxwd	xmm3, qword ptr [rdi + 2*rdx + 24]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB93_9:
	cmp	rcx, r9
	je	.LBB93_11
	.p2align	4, 0x90
.LBB93_10:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdi + 2*rcx]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB93_10
.LBB93_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB93_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB93_8
	jmp	.LBB93_9
.Lfunc_end93:
	.size	cast_numeric_int16_float64_avx2, .Lfunc_end93-cast_numeric_int16_float64_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_numeric_uint32_float64_avx2
.LCPI94_0:
	.quad	0x4330000000000000              # double 4503599627370496
	.text
	.globl	cast_numeric_uint32_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint32_float64_avx2,@function
cast_numeric_uint32_float64_avx2:       # @cast_numeric_uint32_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB94_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB94_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB94_10
.LBB94_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB94_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI94_0] # ymm0 = [4.503599627370496E+15,4.503599627370496E+15,4.503599627370496E+15,4.503599627370496E+15]
	.p2align	4, 0x90
.LBB94_6:                               # =>This Inner Loop Header: Depth=1
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 16] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 32] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm4, xmmword ptr [rdi + 4*rdx + 48] # ymm4 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpor	ymm1, ymm1, ymm0
	vsubpd	ymm1, ymm1, ymm0
	vpor	ymm2, ymm2, ymm0
	vsubpd	ymm2, ymm2, ymm0
	vpor	ymm3, ymm3, ymm0
	vsubpd	ymm3, ymm3, ymm0
	vpor	ymm4, ymm4, ymm0
	vsubpd	ymm4, ymm4, ymm0
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm1
	vmovupd	ymmword ptr [rsi + 8*rdx + 32], ymm2
	vmovupd	ymmword ptr [rsi + 8*rdx + 64], ymm3
	vmovupd	ymmword ptr [rsi + 8*rdx + 96], ymm4
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 64] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 80] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 96] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm4, xmmword ptr [rdi + 4*rdx + 112] # ymm4 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpor	ymm1, ymm1, ymm0
	vsubpd	ymm1, ymm1, ymm0
	vpor	ymm2, ymm2, ymm0
	vsubpd	ymm2, ymm2, ymm0
	vpor	ymm3, ymm3, ymm0
	vsubpd	ymm3, ymm3, ymm0
	vpor	ymm4, ymm4, ymm0
	vsubpd	ymm4, ymm4, ymm0
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm1
	vmovupd	ymmword ptr [rsi + 8*rdx + 160], ymm2
	vmovupd	ymmword ptr [rsi + 8*rdx + 192], ymm3
	vmovupd	ymmword ptr [rsi + 8*rdx + 224], ymm4
	add	rdx, 32
	add	rax, 2
	jne	.LBB94_6
# %bb.7:
	test	r8b, 1
	je	.LBB94_9
.LBB94_8:
	vpmovzxdq	ymm0, xmmword ptr [rdi + 4*rdx] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdi + 4*rdx + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdi + 4*rdx + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdi + 4*rdx + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpbroadcastq	ymm4, qword ptr [rip + .LCPI94_0] # ymm4 = [4.503599627370496E+15,4.503599627370496E+15,4.503599627370496E+15,4.503599627370496E+15]
	vpor	ymm0, ymm0, ymm4
	vsubpd	ymm0, ymm0, ymm4
	vpor	ymm1, ymm1, ymm4
	vsubpd	ymm1, ymm1, ymm4
	vpor	ymm2, ymm2, ymm4
	vsubpd	ymm2, ymm2, ymm4
	vpor	ymm3, ymm3, ymm4
	vsubpd	ymm3, ymm3, ymm4
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovupd	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovupd	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovupd	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB94_9:
	cmp	rcx, r9
	je	.LBB94_11
	.p2align	4, 0x90
.LBB94_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	vcvtsi2sd	xmm0, xmm5, rax
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB94_10
.LBB94_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB94_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB94_8
	jmp	.LBB94_9
.Lfunc_end94:
	.size	cast_numeric_uint32_float64_avx2, .Lfunc_end94-cast_numeric_uint32_float64_avx2
                                        # -- End function
	.globl	cast_numeric_int32_float64_avx2 # -- Begin function cast_numeric_int32_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int32_float64_avx2,@function
cast_numeric_int32_float64_avx2:        # @cast_numeric_int32_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB95_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB95_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB95_10
.LBB95_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB95_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB95_6:                               # =>This Inner Loop Header: Depth=1
	vcvtdq2pd	ymm0, xmmword ptr [rdi + 4*rdx]
	vcvtdq2pd	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vcvtdq2pd	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vcvtdq2pd	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vcvtdq2pd	ymm0, xmmword ptr [rdi + 4*rdx + 64]
	vcvtdq2pd	ymm1, xmmword ptr [rdi + 4*rdx + 80]
	vcvtdq2pd	ymm2, xmmword ptr [rdi + 4*rdx + 96]
	vcvtdq2pd	ymm3, xmmword ptr [rdi + 4*rdx + 112]
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB95_6
# %bb.7:
	test	r8b, 1
	je	.LBB95_9
.LBB95_8:
	vcvtdq2pd	ymm0, xmmword ptr [rdi + 4*rdx]
	vcvtdq2pd	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vcvtdq2pd	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vcvtdq2pd	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB95_9:
	cmp	rcx, r9
	je	.LBB95_11
	.p2align	4, 0x90
.LBB95_10:                              # =>This Inner Loop Header: Depth=1
	vcvtsi2sd	xmm0, xmm4, dword ptr [rdi + 4*rcx]
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB95_10
.LBB95_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB95_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB95_8
	jmp	.LBB95_9
.Lfunc_end95:
	.size	cast_numeric_int32_float64_avx2, .Lfunc_end95-cast_numeric_int32_float64_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_numeric_uint64_float64_avx2
.LCPI96_0:
	.quad	4841369599423283200             # 0x4330000000000000
.LCPI96_1:
	.quad	4985484787499139072             # 0x4530000000000000
.LCPI96_2:
	.quad	0x4530000000100000              # double 1.9342813118337666E+25
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI96_3:
	.long	1127219200                      # 0x43300000
	.long	1160773632                      # 0x45300000
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI96_4:
	.quad	0x4330000000000000              # double 4503599627370496
	.quad	0x4530000000000000              # double 1.9342813113834067E+25
	.text
	.globl	cast_numeric_uint64_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_uint64_float64_avx2,@function
cast_numeric_uint64_float64_avx2:       # @cast_numeric_uint64_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB96_12
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB96_5
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB96_3
.LBB96_5:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB96_6
# %bb.7:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI96_0] # ymm0 = [4841369599423283200,4841369599423283200,4841369599423283200,4841369599423283200]
	vpxor	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI96_1] # ymm2 = [4985484787499139072,4985484787499139072,4985484787499139072,4985484787499139072]
	vbroadcastsd	ymm3, qword ptr [rip + .LCPI96_2] # ymm3 = [1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25]
	.p2align	4, 0x90
.LBB96_8:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rdx]
	vmovdqu	ymm5, ymmword ptr [rdi + 8*rdx + 32]
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rdx + 64]
	vmovdqu	ymm7, ymmword ptr [rdi + 8*rdx + 96]
	vpblendd	ymm8, ymm4, ymm1, 170           # ymm8 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm4, ymm4, 32
	vpor	ymm4, ymm4, ymm2
	vsubpd	ymm4, ymm4, ymm3
	vaddpd	ymm4, ymm8, ymm4
	vpblendd	ymm8, ymm5, ymm1, 170           # ymm8 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm5, ymm5, 32
	vpor	ymm5, ymm5, ymm2
	vsubpd	ymm5, ymm5, ymm3
	vaddpd	ymm5, ymm8, ymm5
	vpblendd	ymm8, ymm6, ymm1, 170           # ymm8 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm6, ymm6, 32
	vpor	ymm6, ymm6, ymm2
	vsubpd	ymm6, ymm6, ymm3
	vaddpd	ymm6, ymm8, ymm6
	vpblendd	ymm8, ymm7, ymm1, 170           # ymm8 = ymm7[0],ymm1[1],ymm7[2],ymm1[3],ymm7[4],ymm1[5],ymm7[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm7, ymm7, 32
	vpor	ymm7, ymm7, ymm2
	vsubpd	ymm7, ymm7, ymm3
	vaddpd	ymm7, ymm8, ymm7
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm4
	vmovupd	ymmword ptr [rsi + 8*rdx + 32], ymm5
	vmovupd	ymmword ptr [rsi + 8*rdx + 64], ymm6
	vmovupd	ymmword ptr [rsi + 8*rdx + 96], ymm7
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rdx + 128]
	vmovdqu	ymm5, ymmword ptr [rdi + 8*rdx + 160]
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rdx + 192]
	vmovdqu	ymm7, ymmword ptr [rdi + 8*rdx + 224]
	vpblendd	ymm8, ymm4, ymm1, 170           # ymm8 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm4, ymm4, 32
	vpor	ymm4, ymm4, ymm2
	vsubpd	ymm4, ymm4, ymm3
	vaddpd	ymm4, ymm8, ymm4
	vpblendd	ymm8, ymm5, ymm1, 170           # ymm8 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm5, ymm5, 32
	vpor	ymm5, ymm5, ymm2
	vsubpd	ymm5, ymm5, ymm3
	vaddpd	ymm5, ymm8, ymm5
	vpblendd	ymm8, ymm6, ymm1, 170           # ymm8 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm6, ymm6, 32
	vpor	ymm6, ymm6, ymm2
	vsubpd	ymm6, ymm6, ymm3
	vaddpd	ymm6, ymm8, ymm6
	vpblendd	ymm8, ymm7, ymm1, 170           # ymm8 = ymm7[0],ymm1[1],ymm7[2],ymm1[3],ymm7[4],ymm1[5],ymm7[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm7, ymm7, 32
	vpor	ymm7, ymm7, ymm2
	vsubpd	ymm7, ymm7, ymm3
	vaddpd	ymm7, ymm8, ymm7
	vmovupd	ymmword ptr [rsi + 8*rdx + 128], ymm4
	vmovupd	ymmword ptr [rsi + 8*rdx + 160], ymm5
	vmovupd	ymmword ptr [rsi + 8*rdx + 192], ymm6
	vmovupd	ymmword ptr [rsi + 8*rdx + 224], ymm7
	add	rdx, 32
	add	rax, 2
	jne	.LBB96_8
# %bb.9:
	test	r8b, 1
	je	.LBB96_11
.LBB96_10:
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovdqu	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vxorpd	xmm4, xmm4, xmm4
	vpblendd	ymm5, ymm0, ymm4, 170           # ymm5 = ymm0[0],ymm4[1],ymm0[2],ymm4[3],ymm0[4],ymm4[5],ymm0[6],ymm4[7]
	vpbroadcastq	ymm6, qword ptr [rip + .LCPI96_0] # ymm6 = [4841369599423283200,4841369599423283200,4841369599423283200,4841369599423283200]
	vpor	ymm5, ymm5, ymm6
	vpsrlq	ymm0, ymm0, 32
	vpbroadcastq	ymm7, qword ptr [rip + .LCPI96_1] # ymm7 = [4985484787499139072,4985484787499139072,4985484787499139072,4985484787499139072]
	vpor	ymm0, ymm0, ymm7
	vbroadcastsd	ymm8, qword ptr [rip + .LCPI96_2] # ymm8 = [1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25]
	vsubpd	ymm0, ymm0, ymm8
	vaddpd	ymm0, ymm5, ymm0
	vpblendd	ymm5, ymm1, ymm4, 170           # ymm5 = ymm1[0],ymm4[1],ymm1[2],ymm4[3],ymm1[4],ymm4[5],ymm1[6],ymm4[7]
	vpor	ymm5, ymm5, ymm6
	vpsrlq	ymm1, ymm1, 32
	vpor	ymm1, ymm1, ymm7
	vsubpd	ymm1, ymm1, ymm8
	vaddpd	ymm1, ymm5, ymm1
	vpblendd	ymm5, ymm2, ymm4, 170           # ymm5 = ymm2[0],ymm4[1],ymm2[2],ymm4[3],ymm2[4],ymm4[5],ymm2[6],ymm4[7]
	vpor	ymm5, ymm5, ymm6
	vpsrlq	ymm2, ymm2, 32
	vpor	ymm2, ymm2, ymm7
	vsubpd	ymm2, ymm2, ymm8
	vaddpd	ymm2, ymm5, ymm2
	vpblendd	ymm4, ymm3, ymm4, 170           # ymm4 = ymm3[0],ymm4[1],ymm3[2],ymm4[3],ymm3[4],ymm4[5],ymm3[6],ymm4[7]
	vpor	ymm4, ymm4, ymm6
	vpsrlq	ymm3, ymm3, 32
	vpor	ymm3, ymm3, ymm7
	vsubpd	ymm3, ymm3, ymm8
	vaddpd	ymm3, ymm4, ymm3
	vmovupd	ymmword ptr [rsi + 8*rdx], ymm0
	vmovupd	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovupd	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovupd	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB96_11:
	cmp	rcx, r9
	je	.LBB96_12
.LBB96_3:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI96_3] # xmm0 = [1127219200,1160773632,0,0]
	vmovapd	xmm1, xmmword ptr [rip + .LCPI96_4] # xmm1 = [4.503599627370496E+15,1.9342813113834067E+25]
	.p2align	4, 0x90
.LBB96_4:                               # =>This Inner Loop Header: Depth=1
	vmovsd	xmm2, qword ptr [rdi + 8*rcx]   # xmm2 = mem[0],zero
	vunpcklps	xmm2, xmm2, xmm0        # xmm2 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vsubpd	xmm2, xmm2, xmm1
	vpermilpd	xmm3, xmm2, 1           # xmm3 = xmm2[1,0]
	vaddsd	xmm2, xmm3, xmm2
	vmovsd	qword ptr [rsi + 8*rcx], xmm2
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB96_4
.LBB96_12:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB96_6:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB96_10
	jmp	.LBB96_11
.Lfunc_end96:
	.size	cast_numeric_uint64_float64_avx2, .Lfunc_end96-cast_numeric_uint64_float64_avx2
                                        # -- End function
	.globl	cast_numeric_int64_float64_avx2 # -- Begin function cast_numeric_int64_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_int64_float64_avx2,@function
cast_numeric_int64_float64_avx2:        # @cast_numeric_int64_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB97_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB97_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB97_10
.LBB97_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB97_4
# %bb.5:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edx, edx
	.p2align	4, 0x90
.LBB97_6:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	vpextrq	rax, xmm0, 1
	vcvtsi2sd	xmm2, xmm11, rax
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovq	rax, xmm0
	vcvtsi2sd	xmm0, xmm11, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 48]
	vmovq	rax, xmm1
	vcvtsi2sd	xmm1, xmm11, rax
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm8, xmm0, xmm2        # xmm8 = xmm0[0],xmm2[0]
	vmovq	rax, xmm5
	vcvtsi2sd	xmm2, xmm11, rax
	vpextrq	rax, xmm3, 1
	vcvtsi2sd	xmm5, xmm11, rax
	vunpcklpd	xmm10, xmm1, xmm4       # xmm10 = xmm1[0],xmm4[0]
	vmovq	rax, xmm3
	vcvtsi2sd	xmm3, xmm11, rax
	vunpcklpd	xmm9, xmm2, xmm6        # xmm9 = xmm2[0],xmm6[0]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 80]
	vpextrq	rax, xmm4, 1
	vunpcklpd	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0]
	vcvtsi2sd	xmm5, xmm11, rax
	vmovq	rax, xmm4
	vcvtsi2sd	xmm4, xmm11, rax
	vunpcklpd	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0]
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 64]
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vmovq	rax, xmm5
	vcvtsi2sd	xmm5, xmm11, rax
	vmovdqu	xmm7, xmmword ptr [rdi + 8*rdx + 112]
	vpextrq	rax, xmm7, 1
	vcvtsi2sd	xmm0, xmm11, rax
	vmovq	rax, xmm7
	vcvtsi2sd	xmm7, xmm11, rax
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 96]
	vpextrq	rax, xmm2, 1
	vcvtsi2sd	xmm1, xmm11, rax
	vunpcklpd	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vmovq	rax, xmm2
	vcvtsi2sd	xmm2, xmm11, rax
	vunpcklpd	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vunpcklpd	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovupd	xmmword ptr [rsi + 8*rdx + 16], xmm10
	vmovupd	xmmword ptr [rsi + 8*rdx], xmm8
	vmovupd	xmmword ptr [rsi + 8*rdx + 32], xmm3
	vmovupd	xmmword ptr [rsi + 8*rdx + 48], xmm9
	vmovupd	xmmword ptr [rsi + 8*rdx + 64], xmm5
	vmovupd	xmmword ptr [rsi + 8*rdx + 80], xmm4
	vmovupd	xmmword ptr [rsi + 8*rdx + 96], xmm1
	vmovupd	xmmword ptr [rsi + 8*rdx + 112], xmm0
	vmovdqu	xmm0, xmmword ptr [rdi + 8*rdx + 128]
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 144]
	vpextrq	rax, xmm0, 1
	vcvtsi2sd	xmm2, xmm11, rax
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 160]
	vmovq	rax, xmm0
	vcvtsi2sd	xmm0, xmm11, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 176]
	vmovq	rax, xmm1
	vcvtsi2sd	xmm1, xmm11, rax
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm8, xmm0, xmm2        # xmm8 = xmm0[0],xmm2[0]
	vmovq	rax, xmm5
	vcvtsi2sd	xmm2, xmm11, rax
	vpextrq	rax, xmm3, 1
	vcvtsi2sd	xmm5, xmm11, rax
	vunpcklpd	xmm10, xmm1, xmm4       # xmm10 = xmm1[0],xmm4[0]
	vmovq	rax, xmm3
	vcvtsi2sd	xmm3, xmm11, rax
	vunpcklpd	xmm9, xmm2, xmm6        # xmm9 = xmm2[0],xmm6[0]
	vmovdqu	xmm4, xmmword ptr [rdi + 8*rdx + 208]
	vpextrq	rax, xmm4, 1
	vunpcklpd	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0]
	vcvtsi2sd	xmm5, xmm11, rax
	vmovq	rax, xmm4
	vcvtsi2sd	xmm4, xmm11, rax
	vunpcklpd	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0]
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 192]
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vmovq	rax, xmm5
	vcvtsi2sd	xmm5, xmm11, rax
	vmovdqu	xmm7, xmmword ptr [rdi + 8*rdx + 240]
	vpextrq	rax, xmm7, 1
	vcvtsi2sd	xmm0, xmm11, rax
	vmovq	rax, xmm7
	vcvtsi2sd	xmm7, xmm11, rax
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 224]
	vpextrq	rax, xmm2, 1
	vcvtsi2sd	xmm1, xmm11, rax
	vunpcklpd	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vmovq	rax, xmm2
	vcvtsi2sd	xmm2, xmm11, rax
	vunpcklpd	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vunpcklpd	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovupd	xmmword ptr [rsi + 8*rdx + 144], xmm10
	vmovupd	xmmword ptr [rsi + 8*rdx + 128], xmm8
	vmovupd	xmmword ptr [rsi + 8*rdx + 160], xmm3
	vmovupd	xmmword ptr [rsi + 8*rdx + 176], xmm9
	vmovupd	xmmword ptr [rsi + 8*rdx + 192], xmm5
	vmovupd	xmmword ptr [rsi + 8*rdx + 208], xmm4
	vmovupd	xmmword ptr [rsi + 8*rdx + 224], xmm1
	vmovupd	xmmword ptr [rsi + 8*rdx + 240], xmm0
	add	rdx, 32
	add	r10, 2
	jne	.LBB97_6
# %bb.7:
	test	r8b, 1
	je	.LBB97_9
.LBB97_8:
	vmovdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	vmovdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	vmovdqu	xmm3, xmmword ptr [rdi + 8*rdx + 32]
	vmovdqu	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	vpextrq	rax, xmm0, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm0
	vcvtsi2sd	xmm0, xmm11, rax
	vunpcklpd	xmm8, xmm0, xmm4        # xmm8 = xmm0[0],xmm4[0]
	vpextrq	rax, xmm1, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm1
	vcvtsi2sd	xmm1, xmm11, rax
	vunpcklpd	xmm1, xmm1, xmm4        # xmm1 = xmm1[0],xmm4[0]
	vpextrq	rax, xmm2, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm2
	vcvtsi2sd	xmm2, xmm11, rax
	vunpcklpd	xmm2, xmm2, xmm4        # xmm2 = xmm2[0],xmm4[0]
	vpextrq	rax, xmm3, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm3
	vcvtsi2sd	xmm3, xmm11, rax
	vmovdqu	xmm5, xmmword ptr [rdi + 8*rdx + 80]
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vmovq	rax, xmm5
	vcvtsi2sd	xmm5, xmm11, rax
	vmovdqu	xmm7, xmmword ptr [rdi + 8*rdx + 64]
	vpextrq	rax, xmm7, 1
	vcvtsi2sd	xmm0, xmm11, rax
	vunpcklpd	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0]
	vmovq	rax, xmm7
	vcvtsi2sd	xmm4, xmm11, rax
	vunpcklpd	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vmovdqu	xmm6, xmmword ptr [rdi + 8*rdx + 112]
	vpextrq	rax, xmm6, 1
	vunpcklpd	xmm0, xmm4, xmm0        # xmm0 = xmm4[0],xmm0[0]
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm6
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm4, xmm6, xmm4        # xmm4 = xmm6[0],xmm4[0]
	vmovdqu	xmm6, xmmword ptr [rdi + 8*rdx + 96]
	vpextrq	rax, xmm6, 1
	vcvtsi2sd	xmm7, xmm11, rax
	vmovq	rax, xmm6
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm6, xmm6, xmm7        # xmm6 = xmm6[0],xmm7[0]
	vmovupd	xmmword ptr [rsi + 8*rdx + 16], xmm1
	vmovupd	xmmword ptr [rsi + 8*rdx], xmm8
	vmovupd	xmmword ptr [rsi + 8*rdx + 32], xmm3
	vmovupd	xmmword ptr [rsi + 8*rdx + 48], xmm2
	vmovupd	xmmword ptr [rsi + 8*rdx + 64], xmm0
	vmovupd	xmmword ptr [rsi + 8*rdx + 80], xmm5
	vmovupd	xmmword ptr [rsi + 8*rdx + 96], xmm6
	vmovupd	xmmword ptr [rsi + 8*rdx + 112], xmm4
.LBB97_9:
	cmp	rcx, r9
	je	.LBB97_11
	.p2align	4, 0x90
.LBB97_10:                              # =>This Inner Loop Header: Depth=1
	vcvtsi2sd	xmm0, xmm11, qword ptr [rdi + 8*rcx]
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB97_10
.LBB97_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB97_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB97_8
	jmp	.LBB97_9
.Lfunc_end97:
	.size	cast_numeric_int64_float64_avx2, .Lfunc_end97-cast_numeric_int64_float64_avx2
                                        # -- End function
	.globl	cast_numeric_float32_float64_avx2 # -- Begin function cast_numeric_float32_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float32_float64_avx2,@function
cast_numeric_float32_float64_avx2:      # @cast_numeric_float32_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB98_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	ja	.LBB98_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB98_10
.LBB98_3:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB98_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB98_6:                               # =>This Inner Loop Header: Depth=1
	vcvtps2pd	ymm0, xmmword ptr [rdi + 4*rdx]
	vcvtps2pd	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vcvtps2pd	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vcvtps2pd	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vcvtps2pd	ymm0, xmmword ptr [rdi + 4*rdx + 64]
	vcvtps2pd	ymm1, xmmword ptr [rdi + 4*rdx + 80]
	vcvtps2pd	ymm2, xmmword ptr [rdi + 4*rdx + 96]
	vcvtps2pd	ymm3, xmmword ptr [rdi + 4*rdx + 112]
	vmovups	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB98_6
# %bb.7:
	test	r8b, 1
	je	.LBB98_9
.LBB98_8:
	vcvtps2pd	ymm0, xmmword ptr [rdi + 4*rdx]
	vcvtps2pd	ymm1, xmmword ptr [rdi + 4*rdx + 16]
	vcvtps2pd	ymm2, xmmword ptr [rdi + 4*rdx + 32]
	vcvtps2pd	ymm3, xmmword ptr [rdi + 4*rdx + 48]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB98_9:
	cmp	rcx, r9
	je	.LBB98_11
	.p2align	4, 0x90
.LBB98_10:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdi + 4*rcx]   # xmm0 = mem[0],zero,zero,zero
	vcvtss2sd	xmm0, xmm0, xmm0
	vmovsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB98_10
.LBB98_11:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB98_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB98_8
	jmp	.LBB98_9
.Lfunc_end98:
	.size	cast_numeric_float32_float64_avx2, .Lfunc_end98-cast_numeric_float32_float64_avx2
                                        # -- End function
	.globl	cast_numeric_float64_float64_avx2 # -- Begin function cast_numeric_float64_float64_avx2
	.p2align	4, 0x90
	.type	cast_numeric_float64_float64_avx2,@function
cast_numeric_float64_float64_avx2:      # @cast_numeric_float64_float64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB99_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
	jbe	.LBB99_2
# %bb.7:
	lea	rax, [rdi + 8*r9]
	cmp	rax, rsi
	jbe	.LBB99_9
# %bb.8:
	lea	rax, [rsi + 8*r9]
	cmp	rax, rdi
	jbe	.LBB99_9
.LBB99_2:
	xor	ecx, ecx
.LBB99_3:
	mov	r8, rcx
	not	r8
	add	r8, r9
	mov	rdx, r9
	and	rdx, 7
	je	.LBB99_5
	.p2align	4, 0x90
.LBB99_4:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	add	rdx, -1
	jne	.LBB99_4
.LBB99_5:
	cmp	r8, 7
	jb	.LBB99_16
	.p2align	4, 0x90
.LBB99_6:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	mov	rax, qword ptr [rdi + 8*rcx + 8]
	mov	qword ptr [rsi + 8*rcx + 8], rax
	mov	rax, qword ptr [rdi + 8*rcx + 16]
	mov	qword ptr [rsi + 8*rcx + 16], rax
	mov	rax, qword ptr [rdi + 8*rcx + 24]
	mov	qword ptr [rsi + 8*rcx + 24], rax
	mov	rax, qword ptr [rdi + 8*rcx + 32]
	mov	qword ptr [rsi + 8*rcx + 32], rax
	mov	rax, qword ptr [rdi + 8*rcx + 40]
	mov	qword ptr [rsi + 8*rcx + 40], rax
	mov	rax, qword ptr [rdi + 8*rcx + 48]
	mov	qword ptr [rsi + 8*rcx + 48], rax
	mov	rax, qword ptr [rdi + 8*rcx + 56]
	mov	qword ptr [rsi + 8*rcx + 56], rax
	add	rcx, 8
	cmp	r9, rcx
	jne	.LBB99_6
	jmp	.LBB99_16
.LBB99_9:
	mov	ecx, r9d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB99_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB99_12:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx + 128]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 160]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 192]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 224]
	vmovups	ymmword ptr [rsi + 8*rdx + 128], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 160], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 192], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 224], ymm3
	add	rdx, 32
	add	rax, 2
	jne	.LBB99_12
# %bb.13:
	test	r8b, 1
	je	.LBB99_15
.LBB99_14:
	vmovups	ymm0, ymmword ptr [rdi + 8*rdx]
	vmovups	ymm1, ymmword ptr [rdi + 8*rdx + 32]
	vmovups	ymm2, ymmword ptr [rdi + 8*rdx + 64]
	vmovups	ymm3, ymmword ptr [rdi + 8*rdx + 96]
	vmovups	ymmword ptr [rsi + 8*rdx], ymm0
	vmovups	ymmword ptr [rsi + 8*rdx + 32], ymm1
	vmovups	ymmword ptr [rsi + 8*rdx + 64], ymm2
	vmovups	ymmword ptr [rsi + 8*rdx + 96], ymm3
.LBB99_15:
	cmp	rcx, r9
	jne	.LBB99_3
.LBB99_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB99_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB99_14
	jmp	.LBB99_15
.Lfunc_end99:
	.size	cast_numeric_float64_float64_avx2, .Lfunc_end99-cast_numeric_float64_float64_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
