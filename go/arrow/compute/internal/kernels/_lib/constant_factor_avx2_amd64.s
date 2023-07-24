	.text
	.intel_syntax noprefix
	.file	"constant_factor.c"
	.globl	multiply_constant_int32_int32_avx2 # -- Begin function multiply_constant_int32_int32_avx2
	.p2align	4, 0x90
	.type	multiply_constant_int32_int32_avx2,@function
multiply_constant_int32_int32_avx2:     # @multiply_constant_int32_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB0_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 31
	jbe	.LBB0_2
# %bb.7:
	lea	rax, [rdi + 4*r9]
	cmp	rax, rsi
	jbe	.LBB0_9
# %bb.8:
	lea	rax, [rsi + 4*r9]
	cmp	rax, rdi
	jbe	.LBB0_9
.LBB0_2:
	xor	r11d, r11d
.LBB0_3:
	mov	r8, r11
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_5
	.p2align	4, 0x90
.LBB0_4:                                # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rdi + 4*r11]
	imul	edx, ecx
	mov	dword ptr [rsi + 4*r11], edx
	add	r11, 1
	add	rax, -1
	jne	.LBB0_4
.LBB0_5:
	cmp	r8, 3
	jb	.LBB0_16
	.p2align	4, 0x90
.LBB0_6:                                # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*r11]
	imul	eax, ecx
	mov	dword ptr [rsi + 4*r11], eax
	mov	eax, dword ptr [rdi + 4*r11 + 4]
	imul	eax, ecx
	mov	dword ptr [rsi + 4*r11 + 4], eax
	mov	eax, dword ptr [rdi + 4*r11 + 8]
	imul	eax, ecx
	mov	dword ptr [rsi + 4*r11 + 8], eax
	mov	eax, dword ptr [rdi + 4*r11 + 12]
	imul	eax, ecx
	mov	dword ptr [rsi + 4*r11 + 12], eax
	add	r11, 4
	cmp	r9, r11
	jne	.LBB0_6
	jmp	.LBB0_16
.LBB0_9:
	mov	r11d, r9d
	and	r11d, -32
	vmovd	xmm0, ecx
	vpbroadcastd	ymm0, xmm0
	lea	rax, [r11 - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_10
# %bb.11:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	eax, eax
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rdi + 4*rax]
	vpmulld	ymm2, ymm0, ymmword ptr [rdi + 4*rax + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdi + 4*rax + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rdi + 4*rax + 96]
	vmovdqu	ymmword ptr [rsi + 4*rax], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rax + 32], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rax + 64], ymm3
	vmovdqu	ymmword ptr [rsi + 4*rax + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rdi + 4*rax + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rdi + 4*rax + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rdi + 4*rax + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rdi + 4*rax + 224]
	vmovdqu	ymmword ptr [rsi + 4*rax + 128], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rax + 160], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rax + 192], ymm3
	vmovdqu	ymmword ptr [rsi + 4*rax + 224], ymm4
	add	rax, 64
	add	r10, 2
	jne	.LBB0_12
# %bb.13:
	test	r8b, 1
	je	.LBB0_15
.LBB0_14:
	vpmulld	ymm1, ymm0, ymmword ptr [rdi + 4*rax]
	vpmulld	ymm2, ymm0, ymmword ptr [rdi + 4*rax + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdi + 4*rax + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdi + 4*rax + 96]
	vmovdqu	ymmword ptr [rsi + 4*rax], ymm1
	vmovdqu	ymmword ptr [rsi + 4*rax + 32], ymm2
	vmovdqu	ymmword ptr [rsi + 4*rax + 64], ymm3
	vmovdqu	ymmword ptr [rsi + 4*rax + 96], ymm0
.LBB0_15:
	cmp	r11, r9
	jne	.LBB0_3
.LBB0_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB0_10:
	xor	eax, eax
	test	r8b, 1
	jne	.LBB0_14
	jmp	.LBB0_15
.Lfunc_end0:
	.size	multiply_constant_int32_int32_avx2, .Lfunc_end0-multiply_constant_int32_int32_avx2
                                        # -- End function
	.globl	divide_constant_int32_int32_avx2 # -- Begin function divide_constant_int32_int32_avx2
	.p2align	4, 0x90
	.type	divide_constant_int32_int32_avx2,@function
divide_constant_int32_int32_avx2:       # @divide_constant_int32_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB1_8
# %bb.1:
	mov	r9d, edx
	cmp	edx, 1
	jne	.LBB1_9
# %bb.2:
	xor	r8d, r8d
.LBB1_3:
	test	r9b, 1
	je	.LBB1_8
# %bb.4:
	movsxd	rax, dword ptr [rdi + 4*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB1_5
# %bb.6:
	cqo
	idiv	rcx
	jmp	.LBB1_7
.LBB1_9:
	mov	r10d, r9d
	and	r10d, -2
	xor	r8d, r8d
	jmp	.LBB1_10
	.p2align	4, 0x90
.LBB1_15:                               #   in Loop: Header=BB1_10 Depth=1
	cqo
	idiv	rcx
.LBB1_16:                               #   in Loop: Header=BB1_10 Depth=1
	mov	dword ptr [rsi + 4*r8 + 4], eax
	add	r8, 2
	cmp	r10, r8
	je	.LBB1_3
.LBB1_10:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB1_11
# %bb.12:                               #   in Loop: Header=BB1_10 Depth=1
	cqo
	idiv	rcx
	jmp	.LBB1_13
	.p2align	4, 0x90
.LBB1_11:                               #   in Loop: Header=BB1_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB1_13:                               #   in Loop: Header=BB1_10 Depth=1
	mov	dword ptr [rsi + 4*r8], eax
	movsxd	rax, dword ptr [rdi + 4*r8 + 4]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	jne	.LBB1_15
# %bb.14:                               #   in Loop: Header=BB1_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
	jmp	.LBB1_16
.LBB1_5:
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB1_7:
	mov	dword ptr [rsi + 4*r8], eax
.LBB1_8:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end1:
	.size	divide_constant_int32_int32_avx2, .Lfunc_end1-divide_constant_int32_int32_avx2
                                        # -- End function
	.globl	multiply_constant_int32_int64_avx2 # -- Begin function multiply_constant_int32_int64_avx2
	.p2align	4, 0x90
	.type	multiply_constant_int32_int64_avx2,@function
multiply_constant_int32_int64_avx2:     # @multiply_constant_int32_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB2_7
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	ja	.LBB2_3
# %bb.2:
	xor	edx, edx
	jmp	.LBB2_6
.LBB2_3:
	mov	edx, r8d
	and	edx, -16
	vmovq	xmm0, rcx
	vpbroadcastq	ymm0, xmm0
	xor	eax, eax
	vpsrlq	ymm1, ymm0, 32
	.p2align	4, 0x90
.LBB2_4:                                # =>This Inner Loop Header: Depth=1
	vpmovsxdq	ymm2, xmmword ptr [rdi + 4*rax]
	vpmovsxdq	ymm3, xmmword ptr [rdi + 4*rax + 16]
	vpmovsxdq	ymm4, xmmword ptr [rdi + 4*rax + 32]
	vpmovsxdq	ymm5, xmmword ptr [rdi + 4*rax + 48]
	vpmuludq	ymm6, ymm1, ymm2
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm0, ymm7
	vpaddq	ymm6, ymm7, ymm6
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm0, ymm2
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm1, ymm3
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm0, ymm7
	vpaddq	ymm6, ymm7, ymm6
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm0, ymm3
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm1, ymm4
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm0, ymm7
	vpaddq	ymm6, ymm7, ymm6
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm0, ymm4
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm1, ymm5
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm0, ymm7
	vpaddq	ymm6, ymm7, ymm6
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm0, ymm5
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [rsi + 8*rax], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rax + 32], ymm3
	vmovdqu	ymmword ptr [rsi + 8*rax + 64], ymm4
	vmovdqu	ymmword ptr [rsi + 8*rax + 96], ymm5
	add	rax, 16
	cmp	rdx, rax
	jne	.LBB2_4
# %bb.5:
	cmp	rdx, r8
	je	.LBB2_7
	.p2align	4, 0x90
.LBB2_6:                                # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*rdx]
	imul	rax, rcx
	mov	qword ptr [rsi + 8*rdx], rax
	add	rdx, 1
	cmp	r8, rdx
	jne	.LBB2_6
.LBB2_7:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end2:
	.size	multiply_constant_int32_int64_avx2, .Lfunc_end2-multiply_constant_int32_int64_avx2
                                        # -- End function
	.globl	divide_constant_int32_int64_avx2 # -- Begin function divide_constant_int32_int64_avx2
	.p2align	4, 0x90
	.type	divide_constant_int32_int64_avx2,@function
divide_constant_int32_int64_avx2:       # @divide_constant_int32_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB3_8
# %bb.1:
	mov	r9d, edx
	cmp	edx, 1
	jne	.LBB3_9
# %bb.2:
	xor	r8d, r8d
.LBB3_3:
	test	r9b, 1
	je	.LBB3_8
# %bb.4:
	movsxd	rax, dword ptr [rdi + 4*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB3_5
# %bb.6:
	cqo
	idiv	rcx
	jmp	.LBB3_7
.LBB3_9:
	mov	r10d, r9d
	and	r10d, -2
	xor	r8d, r8d
	jmp	.LBB3_10
	.p2align	4, 0x90
.LBB3_15:                               #   in Loop: Header=BB3_10 Depth=1
	cqo
	idiv	rcx
.LBB3_16:                               #   in Loop: Header=BB3_10 Depth=1
	mov	qword ptr [rsi + 8*r8 + 8], rax
	add	r8, 2
	cmp	r10, r8
	je	.LBB3_3
.LBB3_10:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB3_11
# %bb.12:                               #   in Loop: Header=BB3_10 Depth=1
	cqo
	idiv	rcx
	jmp	.LBB3_13
	.p2align	4, 0x90
.LBB3_11:                               #   in Loop: Header=BB3_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB3_13:                               #   in Loop: Header=BB3_10 Depth=1
	mov	qword ptr [rsi + 8*r8], rax
	movsxd	rax, dword ptr [rdi + 4*r8 + 4]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	jne	.LBB3_15
# %bb.14:                               #   in Loop: Header=BB3_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
	jmp	.LBB3_16
.LBB3_5:
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB3_7:
	mov	qword ptr [rsi + 8*r8], rax
.LBB3_8:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end3:
	.size	divide_constant_int32_int64_avx2, .Lfunc_end3-divide_constant_int32_int64_avx2
                                        # -- End function
	.globl	multiply_constant_int64_int32_avx2 # -- Begin function multiply_constant_int64_int32_avx2
	.p2align	4, 0x90
	.type	multiply_constant_int64_int32_avx2,@function
multiply_constant_int64_int32_avx2:     # @multiply_constant_int64_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB4_7
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	ja	.LBB4_3
# %bb.2:
	xor	edx, edx
	jmp	.LBB4_6
.LBB4_3:
	mov	edx, r8d
	and	edx, -16
	vmovq	xmm0, rcx
	vpbroadcastq	ymm0, xmm0
	xor	eax, eax
	vextracti128	xmm1, ymm0, 1
	.p2align	4, 0x90
.LBB4_4:                                # =>This Inner Loop Header: Depth=1
	vmovups	xmm2, xmmword ptr [rdi + 8*rax]
	vmovups	xmm3, xmmword ptr [rdi + 8*rax + 32]
	vmovups	xmm4, xmmword ptr [rdi + 8*rax + 64]
	vmovups	xmm5, xmmword ptr [rdi + 8*rax + 96]
	vshufps	xmm2, xmm2, xmmword ptr [rdi + 8*rax + 16], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm6, xmm0, xmm1, 136           # xmm6 = xmm0[0,2],xmm1[0,2]
	vpmulld	xmm2, xmm2, xmm6
	vshufps	xmm3, xmm3, xmmword ptr [rdi + 8*rax + 48], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vshufps	xmm6, xmm0, xmm1, 136           # xmm6 = xmm0[0,2],xmm1[0,2]
	vpmulld	xmm3, xmm3, xmm6
	vshufps	xmm4, xmm4, xmmword ptr [rdi + 8*rax + 80], 136 # xmm4 = xmm4[0,2],mem[0,2]
	vshufps	xmm6, xmm0, xmm1, 136           # xmm6 = xmm0[0,2],xmm1[0,2]
	vpmulld	xmm4, xmm4, xmm6
	vshufps	xmm5, xmm5, xmmword ptr [rdi + 8*rax + 112], 136 # xmm5 = xmm5[0,2],mem[0,2]
	vshufps	xmm6, xmm0, xmm1, 136           # xmm6 = xmm0[0,2],xmm1[0,2]
	vpmulld	xmm5, xmm5, xmm6
	vmovdqu	xmmword ptr [rsi + 4*rax], xmm2
	vmovdqu	xmmword ptr [rsi + 4*rax + 16], xmm3
	vmovdqu	xmmword ptr [rsi + 4*rax + 32], xmm4
	vmovdqu	xmmword ptr [rsi + 4*rax + 48], xmm5
	add	rax, 16
	cmp	rdx, rax
	jne	.LBB4_4
# %bb.5:
	cmp	rdx, r8
	je	.LBB4_7
	.p2align	4, 0x90
.LBB4_6:                                # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 8*rdx]
	imul	eax, ecx
	mov	dword ptr [rsi + 4*rdx], eax
	add	rdx, 1
	cmp	r8, rdx
	jne	.LBB4_6
.LBB4_7:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end4:
	.size	multiply_constant_int64_int32_avx2, .Lfunc_end4-multiply_constant_int64_int32_avx2
                                        # -- End function
	.globl	divide_constant_int64_int32_avx2 # -- Begin function divide_constant_int64_int32_avx2
	.p2align	4, 0x90
	.type	divide_constant_int64_int32_avx2,@function
divide_constant_int64_int32_avx2:       # @divide_constant_int64_int32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB5_8
# %bb.1:
	mov	r9d, edx
	cmp	edx, 1
	jne	.LBB5_9
# %bb.2:
	xor	r8d, r8d
.LBB5_3:
	test	r9b, 1
	je	.LBB5_8
# %bb.4:
	mov	rax, qword ptr [rdi + 8*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB5_5
# %bb.6:
	cqo
	idiv	rcx
	jmp	.LBB5_7
.LBB5_9:
	mov	r10d, r9d
	and	r10d, -2
	xor	r8d, r8d
	jmp	.LBB5_10
	.p2align	4, 0x90
.LBB5_15:                               #   in Loop: Header=BB5_10 Depth=1
	cqo
	idiv	rcx
.LBB5_16:                               #   in Loop: Header=BB5_10 Depth=1
	mov	dword ptr [rsi + 4*r8 + 4], eax
	add	r8, 2
	cmp	r10, r8
	je	.LBB5_3
.LBB5_10:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB5_11
# %bb.12:                               #   in Loop: Header=BB5_10 Depth=1
	cqo
	idiv	rcx
	jmp	.LBB5_13
	.p2align	4, 0x90
.LBB5_11:                               #   in Loop: Header=BB5_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB5_13:                               #   in Loop: Header=BB5_10 Depth=1
	mov	dword ptr [rsi + 4*r8], eax
	mov	rax, qword ptr [rdi + 8*r8 + 8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	jne	.LBB5_15
# %bb.14:                               #   in Loop: Header=BB5_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
	jmp	.LBB5_16
.LBB5_5:
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB5_7:
	mov	dword ptr [rsi + 4*r8], eax
.LBB5_8:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end5:
	.size	divide_constant_int64_int32_avx2, .Lfunc_end5-divide_constant_int64_int32_avx2
                                        # -- End function
	.globl	multiply_constant_int64_int64_avx2 # -- Begin function multiply_constant_int64_int64_avx2
	.p2align	4, 0x90
	.type	multiply_constant_int64_int64_avx2,@function
multiply_constant_int64_int64_avx2:     # @multiply_constant_int64_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB6_16
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	jbe	.LBB6_2
# %bb.7:
	lea	rax, [rdi + 8*r8]
	cmp	rax, rsi
	jbe	.LBB6_9
# %bb.8:
	lea	rax, [rsi + 8*r8]
	cmp	rax, rdi
	jbe	.LBB6_9
.LBB6_2:
	xor	r11d, r11d
.LBB6_3:
	mov	r9, r11
	not	r9
	add	r9, r8
	mov	rax, r8
	and	rax, 3
	je	.LBB6_5
	.p2align	4, 0x90
.LBB6_4:                                # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rdi + 8*r11]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*r11], rdx
	add	r11, 1
	add	rax, -1
	jne	.LBB6_4
.LBB6_5:
	cmp	r9, 3
	jb	.LBB6_16
	.p2align	4, 0x90
.LBB6_6:                                # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r11]
	imul	rax, rcx
	mov	qword ptr [rsi + 8*r11], rax
	mov	rax, qword ptr [rdi + 8*r11 + 8]
	imul	rax, rcx
	mov	qword ptr [rsi + 8*r11 + 8], rax
	mov	rax, qword ptr [rdi + 8*r11 + 16]
	imul	rax, rcx
	mov	qword ptr [rsi + 8*r11 + 16], rax
	mov	rax, qword ptr [rdi + 8*r11 + 24]
	imul	rax, rcx
	mov	qword ptr [rsi + 8*r11 + 24], rax
	add	r11, 4
	cmp	r8, r11
	jne	.LBB6_6
	jmp	.LBB6_16
.LBB6_9:
	mov	r11d, r8d
	and	r11d, -16
	vmovq	xmm0, rcx
	vpbroadcastq	ymm0, xmm0
	lea	rax, [r11 - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rax, rax
	je	.LBB6_10
# %bb.11:
	mov	r10, r9
	and	r10, -2
	neg	r10
	xor	eax, eax
	.p2align	4, 0x90
.LBB6_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax]
	vmovdqu	ymm3, ymmword ptr [rdi + 8*rax + 32]
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 64]
	vmovdqu	ymm5, ymmword ptr [rdi + 8*rax + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [rsi + 8*rax], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rax + 32], ymm3
	vmovdqu	ymmword ptr [rsi + 8*rax + 64], ymm4
	vmovdqu	ymmword ptr [rsi + 8*rax + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 128]
	vmovdqu	ymm3, ymmword ptr [rdi + 8*rax + 160]
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 192]
	vmovdqu	ymm5, ymmword ptr [rdi + 8*rax + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [rsi + 8*rax + 128], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rax + 160], ymm3
	vmovdqu	ymmword ptr [rsi + 8*rax + 192], ymm4
	vmovdqu	ymmword ptr [rsi + 8*rax + 224], ymm5
	add	rax, 32
	add	r10, 2
	jne	.LBB6_12
# %bb.13:
	test	r9b, 1
	je	.LBB6_15
.LBB6_14:
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax]
	vmovdqu	ymm3, ymmword ptr [rdi + 8*rax + 32]
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 64]
	vmovdqu	ymm5, ymmword ptr [rdi + 8*rax + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [rsi + 8*rax], ymm2
	vmovdqu	ymmword ptr [rsi + 8*rax + 32], ymm3
	vmovdqu	ymmword ptr [rsi + 8*rax + 64], ymm4
	vmovdqu	ymmword ptr [rsi + 8*rax + 96], ymm0
.LBB6_15:
	cmp	r11, r8
	jne	.LBB6_3
.LBB6_16:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB6_10:
	xor	eax, eax
	test	r9b, 1
	jne	.LBB6_14
	jmp	.LBB6_15
.Lfunc_end6:
	.size	multiply_constant_int64_int64_avx2, .Lfunc_end6-multiply_constant_int64_int64_avx2
                                        # -- End function
	.globl	divide_constant_int64_int64_avx2 # -- Begin function divide_constant_int64_int64_avx2
	.p2align	4, 0x90
	.type	divide_constant_int64_int64_avx2,@function
divide_constant_int64_int64_avx2:       # @divide_constant_int64_int64_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB7_8
# %bb.1:
	mov	r9d, edx
	cmp	edx, 1
	jne	.LBB7_9
# %bb.2:
	xor	r8d, r8d
.LBB7_3:
	test	r9b, 1
	je	.LBB7_8
# %bb.4:
	mov	rax, qword ptr [rdi + 8*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB7_5
# %bb.6:
	cqo
	idiv	rcx
	jmp	.LBB7_7
.LBB7_9:
	mov	r10d, r9d
	and	r10d, -2
	xor	r8d, r8d
	jmp	.LBB7_10
	.p2align	4, 0x90
.LBB7_15:                               #   in Loop: Header=BB7_10 Depth=1
	cqo
	idiv	rcx
.LBB7_16:                               #   in Loop: Header=BB7_10 Depth=1
	mov	qword ptr [rsi + 8*r8 + 8], rax
	add	r8, 2
	cmp	r10, r8
	je	.LBB7_3
.LBB7_10:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	je	.LBB7_11
# %bb.12:                               #   in Loop: Header=BB7_10 Depth=1
	cqo
	idiv	rcx
	jmp	.LBB7_13
	.p2align	4, 0x90
.LBB7_11:                               #   in Loop: Header=BB7_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB7_13:                               #   in Loop: Header=BB7_10 Depth=1
	mov	qword ptr [rsi + 8*r8], rax
	mov	rax, qword ptr [rdi + 8*r8 + 8]
	mov	rdx, rax
	or	rdx, rcx
	shr	rdx, 32
	jne	.LBB7_15
# %bb.14:                               #   in Loop: Header=BB7_10 Depth=1
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
	jmp	.LBB7_16
.LBB7_5:
                                        # kill: def $eax killed $eax killed $rax
	xor	edx, edx
	div	ecx
                                        # kill: def $eax killed $eax def $rax
.LBB7_7:
	mov	qword ptr [rsi + 8*r8], rax
.LBB7_8:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end7:
	.size	divide_constant_int64_int64_avx2, .Lfunc_end7-divide_constant_int64_int64_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
