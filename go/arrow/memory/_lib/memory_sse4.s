	.text
	.intel_syntax noprefix
	.file	"_lib/memory.c"
	.globl	memset_sse4
	.p2align	4, 0x90
	.type	memset_sse4,@function
memset_sse4:                            # @memset_sse4
# BB#0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	lea	r11, [rdi + rsi]
	cmp	r11, rdi
	jbe	.LBB0_13
# BB#1:
	cmp	rsi, 32
	jb	.LBB0_12
# BB#2:
	mov	r8, rsi
	and	r8, -32
	mov	r10, rsi
	and	r10, -32
	je	.LBB0_12
# BB#3:
	movzx	eax, dl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	r9, [r10 - 32]
	mov	ecx, r9d
	shr	ecx, 5
	inc	ecx
	and	rcx, 7
	je	.LBB0_4
# BB#5:
	neg	rcx
	xor	eax, eax
	.p2align	4, 0x90
.LBB0_6:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rdi + rax], xmm0
	movdqu	xmmword ptr [rdi + rax + 16], xmm0
	add	rax, 32
	inc	rcx
	jne	.LBB0_6
	jmp	.LBB0_7
.LBB0_4:
	xor	eax, eax
.LBB0_7:
	cmp	r9, 224
	jb	.LBB0_10
# BB#8:
	mov	rcx, r10
	sub	rcx, rax
	lea	rax, [rdi + rax + 240]
	.p2align	4, 0x90
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rax - 240], xmm0
	movdqu	xmmword ptr [rax - 224], xmm0
	movdqu	xmmword ptr [rax - 208], xmm0
	movdqu	xmmword ptr [rax - 192], xmm0
	movdqu	xmmword ptr [rax - 176], xmm0
	movdqu	xmmword ptr [rax - 160], xmm0
	movdqu	xmmword ptr [rax - 144], xmm0
	movdqu	xmmword ptr [rax - 128], xmm0
	movdqu	xmmword ptr [rax - 112], xmm0
	movdqu	xmmword ptr [rax - 96], xmm0
	movdqu	xmmword ptr [rax - 80], xmm0
	movdqu	xmmword ptr [rax - 64], xmm0
	movdqu	xmmword ptr [rax - 48], xmm0
	movdqu	xmmword ptr [rax - 32], xmm0
	movdqu	xmmword ptr [rax - 16], xmm0
	movdqu	xmmword ptr [rax], xmm0
	add	rax, 256
	add	rcx, -256
	jne	.LBB0_9
.LBB0_10:
	cmp	r10, rsi
	je	.LBB0_13
# BB#11:
	add	rdi, r8
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	mov	byte ptr [rdi], dl
	inc	rdi
	cmp	r11, rdi
	jne	.LBB0_12
.LBB0_13:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	memset_sse4, .Lfunc_end0-memset_sse4


	.ident	"Apple LLVM version 9.0.0 (clang-900.0.39.2)"
	.section	".note.GNU-stack","",@progbits
