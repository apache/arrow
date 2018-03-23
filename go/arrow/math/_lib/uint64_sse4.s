	.text
	.intel_syntax noprefix
	.file	"_lib/uint64.c"
	.globl	sum_uint64_sse4
	.p2align	4, 0x90
	.type	sum_uint64_sse4,@function
sum_uint64_sse4:                        # @sum_uint64_sse4
# BB#0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	rsi, rsi
	je	.LBB0_1
# BB#2:
	cmp	rsi, 3
	jbe	.LBB0_3
# BB#6:
	mov	r9, rsi
	and	r9, -4
	je	.LBB0_3
# BB#7:
	lea	r8, [r9 - 4]
	mov	eax, r8d
	shr	eax, 2
	inc	eax
	and	rax, 3
	je	.LBB0_8
# BB#9:
	neg	rax
	pxor	xmm0, xmm0
	xor	ecx, ecx
	pxor	xmm1, xmm1
	.p2align	4, 0x90
.LBB0_10:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rdi + 8*rcx]
	movdqu	xmm3, xmmword ptr [rdi + 8*rcx + 16]
	paddq	xmm0, xmm2
	paddq	xmm1, xmm3
	add	rcx, 4
	inc	rax
	jne	.LBB0_10
	jmp	.LBB0_11
.LBB0_3:
	xor	r9d, r9d
	xor	eax, eax
.LBB0_4:
	lea	rcx, [rdi + 8*r9]
	sub	rsi, r9
	.p2align	4, 0x90
.LBB0_5:                                # =>This Inner Loop Header: Depth=1
	add	rax, qword ptr [rcx]
	add	rcx, 8
	dec	rsi
	jne	.LBB0_5
	jmp	.LBB0_15
.LBB0_1:
	xor	eax, eax
.LBB0_15:
	mov	qword ptr [rdx], rax
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_8:
	xor	ecx, ecx
	pxor	xmm0, xmm0
	pxor	xmm1, xmm1
.LBB0_11:
	cmp	r8, 12
	jb	.LBB0_14
# BB#12:
	mov	rax, r9
	sub	rax, rcx
	lea	rcx, [rdi + 8*rcx + 112]
	.p2align	4, 0x90
.LBB0_13:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx - 112]
	movdqu	xmm3, xmmword ptr [rcx - 96]
	movdqu	xmm4, xmmword ptr [rcx - 80]
	movdqu	xmm5, xmmword ptr [rcx - 64]
	paddq	xmm2, xmm0
	paddq	xmm3, xmm1
	movdqu	xmm6, xmmword ptr [rcx - 48]
	movdqu	xmm7, xmmword ptr [rcx - 32]
	paddq	xmm6, xmm4
	paddq	xmm6, xmm2
	paddq	xmm7, xmm5
	paddq	xmm7, xmm3
	movdqu	xmm0, xmmword ptr [rcx - 16]
	movdqu	xmm1, xmmword ptr [rcx]
	paddq	xmm0, xmm6
	paddq	xmm1, xmm7
	sub	rcx, -128
	add	rax, -16
	jne	.LBB0_13
.LBB0_14:
	paddq	xmm0, xmm1
	pshufd	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	paddq	xmm1, xmm0
	movq	rax, xmm1
	cmp	r9, rsi
	jne	.LBB0_4
	jmp	.LBB0_15
.Lfunc_end0:
	.size	sum_uint64_sse4, .Lfunc_end0-sum_uint64_sse4


	.ident	"Apple LLVM version 9.0.0 (clang-900.0.39.2)"
	.section	".note.GNU-stack","",@progbits
