	.text
	.intel_syntax noprefix
	.file	"constant_factor.c"
	.globl	multiply_constant_int32_int32_sse4 # -- Begin function multiply_constant_int32_int32_sse4
	.p2align	4, 0x90
	.type	multiply_constant_int32_int32_sse4,@function
multiply_constant_int32_int32_sse4:     # @multiply_constant_int32_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB0_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	and	r11d, -8
	movd	xmm0, ecx
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [r11 - 8]
	mov	r8, rax
	shr	r8, 3
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
	movdqu	xmm1, xmmword ptr [rdi + 4*rax]
	movdqu	xmm2, xmmword ptr [rdi + 4*rax + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [rsi + 4*rax], xmm1
	movdqu	xmmword ptr [rsi + 4*rax + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdi + 4*rax + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rax + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [rsi + 4*rax + 32], xmm1
	movdqu	xmmword ptr [rsi + 4*rax + 48], xmm2
	add	rax, 16
	add	r10, 2
	jne	.LBB0_12
# %bb.13:
	test	r8b, 1
	je	.LBB0_15
.LBB0_14:
	movdqu	xmm1, xmmword ptr [rdi + 4*rax]
	movdqu	xmm2, xmmword ptr [rdi + 4*rax + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [rsi + 4*rax], xmm1
	movdqu	xmmword ptr [rsi + 4*rax + 16], xmm2
.LBB0_15:
	cmp	r11, r9
	jne	.LBB0_3
.LBB0_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_10:
	xor	eax, eax
	test	r8b, 1
	jne	.LBB0_14
	jmp	.LBB0_15
.Lfunc_end0:
	.size	multiply_constant_int32_int32_sse4, .Lfunc_end0-multiply_constant_int32_int32_sse4
                                        # -- End function
	.globl	divide_constant_int32_int32_sse4 # -- Begin function divide_constant_int32_int32_sse4
	.p2align	4, 0x90
	.type	divide_constant_int32_int32_sse4,@function
divide_constant_int32_int32_sse4:       # @divide_constant_int32_int32_sse4
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
	.size	divide_constant_int32_int32_sse4, .Lfunc_end1-divide_constant_int32_int32_sse4
                                        # -- End function
	.globl	multiply_constant_int32_int64_sse4 # -- Begin function multiply_constant_int32_int64_sse4
	.p2align	4, 0x90
	.type	multiply_constant_int32_int64_sse4,@function
multiply_constant_int32_int64_sse4:     # @multiply_constant_int32_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB2_6
# %bb.1:
	mov	r9d, edx
	lea	rax, [r9 - 1]
	mov	r8d, r9d
	and	r8d, 3
	cmp	rax, 3
	jae	.LBB2_7
# %bb.2:
	xor	eax, eax
	jmp	.LBB2_3
.LBB2_7:
	and	r9d, -4
	xor	eax, eax
	.p2align	4, 0x90
.LBB2_8:                                # =>This Inner Loop Header: Depth=1
	movsxd	rdx, dword ptr [rdi + 4*rax]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax], rdx
	movsxd	rdx, dword ptr [rdi + 4*rax + 4]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 8], rdx
	movsxd	rdx, dword ptr [rdi + 4*rax + 8]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 16], rdx
	movsxd	rdx, dword ptr [rdi + 4*rax + 12]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 24], rdx
	add	rax, 4
	cmp	r9, rax
	jne	.LBB2_8
.LBB2_3:
	test	r8, r8
	je	.LBB2_6
# %bb.4:
	lea	rdx, [rsi + 8*rax]
	lea	rax, [rdi + 4*rax]
	xor	esi, esi
	.p2align	4, 0x90
.LBB2_5:                                # =>This Inner Loop Header: Depth=1
	movsxd	rdi, dword ptr [rax + 4*rsi]
	imul	rdi, rcx
	mov	qword ptr [rdx + 8*rsi], rdi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB2_5
.LBB2_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end2:
	.size	multiply_constant_int32_int64_sse4, .Lfunc_end2-multiply_constant_int32_int64_sse4
                                        # -- End function
	.globl	divide_constant_int32_int64_sse4 # -- Begin function divide_constant_int32_int64_sse4
	.p2align	4, 0x90
	.type	divide_constant_int32_int64_sse4,@function
divide_constant_int32_int64_sse4:       # @divide_constant_int32_int64_sse4
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
	.size	divide_constant_int32_int64_sse4, .Lfunc_end3-divide_constant_int32_int64_sse4
                                        # -- End function
	.globl	multiply_constant_int64_int32_sse4 # -- Begin function multiply_constant_int64_int32_sse4
	.p2align	4, 0x90
	.type	multiply_constant_int64_int32_sse4,@function
multiply_constant_int64_int32_sse4:     # @multiply_constant_int64_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB4_6
# %bb.1:
	mov	r9d, edx
	lea	rax, [r9 - 1]
	mov	r8d, r9d
	and	r8d, 3
	cmp	rax, 3
	jae	.LBB4_7
# %bb.2:
	xor	eax, eax
	jmp	.LBB4_3
.LBB4_7:
	and	r9d, -4
	xor	eax, eax
	.p2align	4, 0x90
.LBB4_8:                                # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rdi + 8*rax]
	imul	edx, ecx
	mov	dword ptr [rsi + 4*rax], edx
	mov	edx, dword ptr [rdi + 8*rax + 8]
	imul	edx, ecx
	mov	dword ptr [rsi + 4*rax + 4], edx
	mov	edx, dword ptr [rdi + 8*rax + 16]
	imul	edx, ecx
	mov	dword ptr [rsi + 4*rax + 8], edx
	mov	edx, dword ptr [rdi + 8*rax + 24]
	imul	edx, ecx
	mov	dword ptr [rsi + 4*rax + 12], edx
	add	rax, 4
	cmp	r9, rax
	jne	.LBB4_8
.LBB4_3:
	test	r8, r8
	je	.LBB4_6
# %bb.4:
	lea	rdx, [rsi + 4*rax]
	lea	rax, [rdi + 8*rax]
	xor	esi, esi
	.p2align	4, 0x90
.LBB4_5:                                # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rax + 8*rsi]
	imul	edi, ecx
	mov	dword ptr [rdx + 4*rsi], edi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB4_5
.LBB4_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end4:
	.size	multiply_constant_int64_int32_sse4, .Lfunc_end4-multiply_constant_int64_int32_sse4
                                        # -- End function
	.globl	divide_constant_int64_int32_sse4 # -- Begin function divide_constant_int64_int32_sse4
	.p2align	4, 0x90
	.type	divide_constant_int64_int32_sse4,@function
divide_constant_int64_int32_sse4:       # @divide_constant_int64_int32_sse4
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
	.size	divide_constant_int64_int32_sse4, .Lfunc_end5-divide_constant_int64_int32_sse4
                                        # -- End function
	.globl	multiply_constant_int64_int64_sse4 # -- Begin function multiply_constant_int64_int64_sse4
	.p2align	4, 0x90
	.type	multiply_constant_int64_int64_sse4,@function
multiply_constant_int64_int64_sse4:     # @multiply_constant_int64_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB6_6
# %bb.1:
	mov	r9d, edx
	lea	rax, [r9 - 1]
	mov	r8d, r9d
	and	r8d, 3
	cmp	rax, 3
	jae	.LBB6_7
# %bb.2:
	xor	eax, eax
	jmp	.LBB6_3
.LBB6_7:
	and	r9d, -4
	xor	eax, eax
	.p2align	4, 0x90
.LBB6_8:                                # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rdi + 8*rax]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax], rdx
	mov	rdx, qword ptr [rdi + 8*rax + 8]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 8], rdx
	mov	rdx, qword ptr [rdi + 8*rax + 16]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 16], rdx
	mov	rdx, qword ptr [rdi + 8*rax + 24]
	imul	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 24], rdx
	add	rax, 4
	cmp	r9, rax
	jne	.LBB6_8
.LBB6_3:
	test	r8, r8
	je	.LBB6_6
# %bb.4:
	lea	rdx, [rsi + 8*rax]
	lea	rax, [rdi + 8*rax]
	xor	esi, esi
	.p2align	4, 0x90
.LBB6_5:                                # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rax + 8*rsi]
	imul	rdi, rcx
	mov	qword ptr [rdx + 8*rsi], rdi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB6_5
.LBB6_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end6:
	.size	multiply_constant_int64_int64_sse4, .Lfunc_end6-multiply_constant_int64_int64_sse4
                                        # -- End function
	.globl	divide_constant_int64_int64_sse4 # -- Begin function divide_constant_int64_int64_sse4
	.p2align	4, 0x90
	.type	divide_constant_int64_int64_sse4,@function
divide_constant_int64_int64_sse4:       # @divide_constant_int64_int64_sse4
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
	.size	divide_constant_int64_int64_sse4, .Lfunc_end7-divide_constant_int64_int64_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
