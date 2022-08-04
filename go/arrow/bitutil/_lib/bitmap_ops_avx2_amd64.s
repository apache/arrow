	.text
	.intel_syntax noprefix
	.file	"bitmap_ops.c"
	.globl	bitmap_aligned_and_avx2         # -- Begin function bitmap_aligned_and_avx2
	.p2align	4, 0x90
	.type	bitmap_aligned_and_avx2,@function
bitmap_aligned_and_avx2:                # @bitmap_aligned_and_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	rcx, rcx
	jle	.LBB0_12
# %bb.1:
	cmp	rcx, 127
	ja	.LBB0_7
# %bb.2:
	xor	r10d, r10d
	jmp	.LBB0_3
.LBB0_7:
	lea	r9, [rdx + rcx]
	lea	rax, [rdi + rcx]
	cmp	rax, rdx
	seta	r11b
	lea	rax, [rsi + rcx]
	cmp	r9, rdi
	seta	bl
	cmp	rax, rdx
	seta	r8b
	cmp	r9, rsi
	seta	r9b
	xor	r10d, r10d
	test	r11b, bl
	jne	.LBB0_3
# %bb.8:
	and	r8b, r9b
	jne	.LBB0_3
# %bb.9:
	mov	r10, rcx
	and	r10, -128
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB0_10:                               # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rsi + r8]
	vmovups	ymm1, ymmword ptr [rsi + r8 + 32]
	vmovups	ymm2, ymmword ptr [rsi + r8 + 64]
	vmovups	ymm3, ymmword ptr [rsi + r8 + 96]
	vandps	ymm0, ymm0, ymmword ptr [rdi + r8]
	vandps	ymm1, ymm1, ymmword ptr [rdi + r8 + 32]
	vandps	ymm2, ymm2, ymmword ptr [rdi + r8 + 64]
	vandps	ymm3, ymm3, ymmword ptr [rdi + r8 + 96]
	vmovups	ymmword ptr [rdx + r8], ymm0
	vmovups	ymmword ptr [rdx + r8 + 32], ymm1
	vmovups	ymmword ptr [rdx + r8 + 64], ymm2
	vmovups	ymmword ptr [rdx + r8 + 96], ymm3
	sub	r8, -128
	cmp	r10, r8
	jne	.LBB0_10
# %bb.11:
	cmp	r10, rcx
	je	.LBB0_12
.LBB0_3:
	mov	r8, r10
	not	r8
	add	r8, rcx
	mov	r9, rcx
	and	r9, 3
	je	.LBB0_5
	.p2align	4, 0x90
.LBB0_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r10]
	and	al, byte ptr [rdi + r10]
	mov	byte ptr [rdx + r10], al
	add	r10, 1
	add	r9, -1
	jne	.LBB0_4
.LBB0_5:
	cmp	r8, 3
	jb	.LBB0_12
	.p2align	4, 0x90
.LBB0_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r10]
	and	al, byte ptr [rdi + r10]
	mov	byte ptr [rdx + r10], al
	movzx	eax, byte ptr [rsi + r10 + 1]
	and	al, byte ptr [rdi + r10 + 1]
	mov	byte ptr [rdx + r10 + 1], al
	movzx	eax, byte ptr [rsi + r10 + 2]
	and	al, byte ptr [rdi + r10 + 2]
	mov	byte ptr [rdx + r10 + 2], al
	movzx	eax, byte ptr [rsi + r10 + 3]
	and	al, byte ptr [rdi + r10 + 3]
	mov	byte ptr [rdx + r10 + 3], al
	add	r10, 4
	cmp	rcx, r10
	jne	.LBB0_6
.LBB0_12:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	vzeroupper
	ret
.Lfunc_end0:
	.size	bitmap_aligned_and_avx2, .Lfunc_end0-bitmap_aligned_and_avx2
                                        # -- End function
	.globl	bitmap_aligned_or_avx2          # -- Begin function bitmap_aligned_or_avx2
	.p2align	4, 0x90
	.type	bitmap_aligned_or_avx2,@function
bitmap_aligned_or_avx2:                 # @bitmap_aligned_or_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	rcx, rcx
	jle	.LBB1_12
# %bb.1:
	cmp	rcx, 127
	ja	.LBB1_7
# %bb.2:
	xor	r10d, r10d
	jmp	.LBB1_3
.LBB1_7:
	lea	r9, [rdx + rcx]
	lea	rax, [rdi + rcx]
	cmp	rax, rdx
	seta	r11b
	lea	rax, [rsi + rcx]
	cmp	r9, rdi
	seta	bl
	cmp	rax, rdx
	seta	r8b
	cmp	r9, rsi
	seta	r9b
	xor	r10d, r10d
	test	r11b, bl
	jne	.LBB1_3
# %bb.8:
	and	r8b, r9b
	jne	.LBB1_3
# %bb.9:
	mov	r10, rcx
	and	r10, -128
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB1_10:                               # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rsi + r8]
	vmovups	ymm1, ymmword ptr [rsi + r8 + 32]
	vmovups	ymm2, ymmword ptr [rsi + r8 + 64]
	vmovups	ymm3, ymmword ptr [rsi + r8 + 96]
	vorps	ymm0, ymm0, ymmword ptr [rdi + r8]
	vorps	ymm1, ymm1, ymmword ptr [rdi + r8 + 32]
	vorps	ymm2, ymm2, ymmword ptr [rdi + r8 + 64]
	vorps	ymm3, ymm3, ymmword ptr [rdi + r8 + 96]
	vmovups	ymmword ptr [rdx + r8], ymm0
	vmovups	ymmword ptr [rdx + r8 + 32], ymm1
	vmovups	ymmword ptr [rdx + r8 + 64], ymm2
	vmovups	ymmword ptr [rdx + r8 + 96], ymm3
	sub	r8, -128
	cmp	r10, r8
	jne	.LBB1_10
# %bb.11:
	cmp	r10, rcx
	je	.LBB1_12
.LBB1_3:
	mov	r8, r10
	not	r8
	add	r8, rcx
	mov	r9, rcx
	and	r9, 3
	je	.LBB1_5
	.p2align	4, 0x90
.LBB1_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r10]
	or	al, byte ptr [rdi + r10]
	mov	byte ptr [rdx + r10], al
	add	r10, 1
	add	r9, -1
	jne	.LBB1_4
.LBB1_5:
	cmp	r8, 3
	jb	.LBB1_12
	.p2align	4, 0x90
.LBB1_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r10]
	or	al, byte ptr [rdi + r10]
	mov	byte ptr [rdx + r10], al
	movzx	eax, byte ptr [rsi + r10 + 1]
	or	al, byte ptr [rdi + r10 + 1]
	mov	byte ptr [rdx + r10 + 1], al
	movzx	eax, byte ptr [rsi + r10 + 2]
	or	al, byte ptr [rdi + r10 + 2]
	mov	byte ptr [rdx + r10 + 2], al
	movzx	eax, byte ptr [rsi + r10 + 3]
	or	al, byte ptr [rdi + r10 + 3]
	mov	byte ptr [rdx + r10 + 3], al
	add	r10, 4
	cmp	rcx, r10
	jne	.LBB1_6
.LBB1_12:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	vzeroupper
	ret
.Lfunc_end1:
	.size	bitmap_aligned_or_avx2, .Lfunc_end1-bitmap_aligned_or_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
