	.text
	.intel_syntax noprefix
	.file	"_lib/float64.c"
	.globl	sum_float64_sse4
	.p2align	4, 0x90
	.type	sum_float64_sse4,@function
sum_float64_sse4:                       # @sum_float64_sse4
# BB#0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	xorpd	xmm0, xmm0
	test	rsi, rsi
	je	.LBB0_14
# BB#1:
	cmp	rsi, 3
	jbe	.LBB0_2
# BB#5:
	mov	r9, rsi
	and	r9, -4
	je	.LBB0_2
# BB#6:
	lea	r8, [r9 - 4]
	mov	eax, r8d
	shr	eax, 2
	inc	eax
	and	rax, 3
	je	.LBB0_7
# BB#8:
	neg	rax
	xorpd	xmm0, xmm0
	xor	ecx, ecx
	xorpd	xmm1, xmm1
	.p2align	4, 0x90
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdi + 8*rcx]
	movupd	xmm3, xmmword ptr [rdi + 8*rcx + 16]
	addpd	xmm0, xmm2
	addpd	xmm1, xmm3
	add	rcx, 4
	inc	rax
	jne	.LBB0_9
	jmp	.LBB0_10
.LBB0_2:
	xor	r9d, r9d
.LBB0_3:
	lea	rax, [rdi + 8*r9]
	sub	rsi, r9
	.p2align	4, 0x90
.LBB0_4:                                # =>This Inner Loop Header: Depth=1
	addsd	xmm0, qword ptr [rax]
	add	rax, 8
	dec	rsi
	jne	.LBB0_4
.LBB0_14:
	movsd	qword ptr [rdx], xmm0
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_7:
	xor	ecx, ecx
	xorpd	xmm0, xmm0
	xorpd	xmm1, xmm1
.LBB0_10:
	cmp	r8, 12
	jb	.LBB0_13
# BB#11:
	mov	rax, r9
	sub	rax, rcx
	lea	rcx, [rdi + 8*rcx + 112]
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx - 112]
	movupd	xmm3, xmmword ptr [rcx - 96]
	movupd	xmm4, xmmword ptr [rcx - 80]
	movupd	xmm5, xmmword ptr [rcx - 64]
	addpd	xmm2, xmm0
	addpd	xmm3, xmm1
	movupd	xmm6, xmmword ptr [rcx - 48]
	movupd	xmm7, xmmword ptr [rcx - 32]
	addpd	xmm6, xmm4
	addpd	xmm6, xmm2
	addpd	xmm7, xmm5
	addpd	xmm7, xmm3
	movupd	xmm0, xmmword ptr [rcx - 16]
	movupd	xmm1, xmmword ptr [rcx]
	addpd	xmm0, xmm6
	addpd	xmm1, xmm7
	sub	rcx, -128
	add	rax, -16
	jne	.LBB0_12
.LBB0_13:
	addpd	xmm0, xmm1
	haddpd	xmm0, xmm0
	cmp	r9, rsi
	jne	.LBB0_3
	jmp	.LBB0_14
.Lfunc_end0:
	.size	sum_float64_sse4, .Lfunc_end0-sum_float64_sse4


	.ident	"Apple LLVM version 9.0.0 (clang-900.0.39.2)"
	.section	".note.GNU-stack","",@progbits
