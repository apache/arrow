	.text
	.intel_syntax noprefix
	.file	"_lib/memory.c"
	.globl	memset_avx2
	.p2align	4, 0x90
	.type	memset_avx2,@function
memset_avx2:                            # @memset_avx2
# BB#0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	lea	r11, [rdi + rsi]
	cmp	r11, rdi
	jbe	.LBB0_13
# BB#1:
	cmp	rsi, 128
	jb	.LBB0_12
# BB#2:
	mov	r8, rsi
	and	r8, -128
	mov	r10, rsi
	and	r10, -128
	je	.LBB0_12
# BB#3:
	vmovd	xmm0, edx
	vpbroadcastb	ymm0, xmm0
	lea	r9, [r10 - 128]
	mov	eax, r9d
	shr	eax, 7
	inc	eax
	and	rax, 3
	je	.LBB0_4
# BB#5:
	neg	rax
	xor	ecx, ecx
	.p2align	4, 0x90
.LBB0_6:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rdi + rcx], ymm0
	vmovdqu	ymmword ptr [rdi + rcx + 32], ymm0
	vmovdqu	ymmword ptr [rdi + rcx + 64], ymm0
	vmovdqu	ymmword ptr [rdi + rcx + 96], ymm0
	sub	rcx, -128
	inc	rax
	jne	.LBB0_6
	jmp	.LBB0_7
.LBB0_4:
	xor	ecx, ecx
.LBB0_7:
	cmp	r9, 384
	jb	.LBB0_10
# BB#8:
	mov	rax, r10
	sub	rax, rcx
	lea	rcx, [rdi + rcx + 480]
	.p2align	4, 0x90
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rcx - 480], ymm0
	vmovdqu	ymmword ptr [rcx - 448], ymm0
	vmovdqu	ymmword ptr [rcx - 416], ymm0
	vmovdqu	ymmword ptr [rcx - 384], ymm0
	vmovdqu	ymmword ptr [rcx - 352], ymm0
	vmovdqu	ymmword ptr [rcx - 320], ymm0
	vmovdqu	ymmword ptr [rcx - 288], ymm0
	vmovdqu	ymmword ptr [rcx - 256], ymm0
	vmovdqu	ymmword ptr [rcx - 224], ymm0
	vmovdqu	ymmword ptr [rcx - 192], ymm0
	vmovdqu	ymmword ptr [rcx - 160], ymm0
	vmovdqu	ymmword ptr [rcx - 128], ymm0
	vmovdqu	ymmword ptr [rcx - 96], ymm0
	vmovdqu	ymmword ptr [rcx - 64], ymm0
	vmovdqu	ymmword ptr [rcx - 32], ymm0
	vmovdqu	ymmword ptr [rcx], ymm0
	add	rcx, 512
	add	rax, -512
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
	vzeroupper
	ret
.Lfunc_end0:
	.size	memset_avx2, .Lfunc_end0-memset_avx2


	.ident	"Apple LLVM version 9.0.0 (clang-900.0.39.2)"
	.section	".note.GNU-stack","",@progbits
