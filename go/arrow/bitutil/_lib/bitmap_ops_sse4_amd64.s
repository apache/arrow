	.text
	.intel_syntax noprefix
	.file	"bitmap_ops.c"
	.globl	bitmap_aligned_and_sse4         # -- Begin function bitmap_aligned_and_sse4
	.p2align	4, 0x90
	.type	bitmap_aligned_and_sse4,@function
bitmap_aligned_and_sse4:                # @bitmap_aligned_and_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	rcx, rcx
	jle	.LBB0_16
# %bb.1:
	cmp	rcx, 31
	ja	.LBB0_7
# %bb.2:
	xor	r11d, r11d
.LBB0_3:
	mov	r8, r11
	not	r8
	add	r8, rcx
	mov	r9, rcx
	and	r9, 3
	je	.LBB0_5
	.p2align	4, 0x90
.LBB0_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	and	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	add	r11, 1
	add	r9, -1
	jne	.LBB0_4
.LBB0_5:
	cmp	r8, 3
	jb	.LBB0_16
	.p2align	4, 0x90
.LBB0_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	and	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	movzx	eax, byte ptr [rsi + r11 + 1]
	and	al, byte ptr [rdi + r11 + 1]
	mov	byte ptr [rdx + r11 + 1], al
	movzx	eax, byte ptr [rsi + r11 + 2]
	and	al, byte ptr [rdi + r11 + 2]
	mov	byte ptr [rdx + r11 + 2], al
	movzx	eax, byte ptr [rsi + r11 + 3]
	and	al, byte ptr [rdi + r11 + 3]
	mov	byte ptr [rdx + r11 + 3], al
	add	r11, 4
	cmp	rcx, r11
	jne	.LBB0_6
	jmp	.LBB0_16
.LBB0_7:
	lea	r9, [rdx + rcx]
	lea	rax, [rdi + rcx]
	cmp	rax, rdx
	seta	r10b
	lea	rax, [rsi + rcx]
	cmp	r9, rdi
	seta	bl
	cmp	rax, rdx
	seta	r8b
	cmp	r9, rsi
	seta	r9b
	xor	r11d, r11d
	test	r10b, bl
	jne	.LBB0_3
# %bb.8:
	and	r8b, r9b
	jne	.LBB0_3
# %bb.9:
	mov	r11, rcx
	and	r11, -32
	lea	rax, [r11 - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_10
# %bb.11:
	mov	r10, r9
	and	r10, -2
	neg	r10
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	andps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	andps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
	movups	xmm0, xmmword ptr [rdi + r8 + 32]
	movups	xmm1, xmmword ptr [rdi + r8 + 48]
	movups	xmm2, xmmword ptr [rsi + r8 + 32]
	andps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 48]
	andps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8 + 32], xmm2
	movups	xmmword ptr [rdx + r8 + 48], xmm0
	add	r8, 64
	add	r10, 2
	jne	.LBB0_12
# %bb.13:
	test	r9b, 1
	je	.LBB0_15
.LBB0_14:
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	andps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	andps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
.LBB0_15:
	cmp	r11, rcx
	jne	.LBB0_3
.LBB0_16:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	ret
.LBB0_10:
	xor	r8d, r8d
	test	r9b, 1
	jne	.LBB0_14
	jmp	.LBB0_15
.Lfunc_end0:
	.size	bitmap_aligned_and_sse4, .Lfunc_end0-bitmap_aligned_and_sse4
                                        # -- End function
	.globl	bitmap_aligned_or_sse4          # -- Begin function bitmap_aligned_or_sse4
	.p2align	4, 0x90
	.type	bitmap_aligned_or_sse4,@function
bitmap_aligned_or_sse4:                 # @bitmap_aligned_or_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	rcx, rcx
	jle	.LBB1_16
# %bb.1:
	cmp	rcx, 31
	ja	.LBB1_7
# %bb.2:
	xor	r11d, r11d
.LBB1_3:
	mov	r8, r11
	not	r8
	add	r8, rcx
	mov	r9, rcx
	and	r9, 3
	je	.LBB1_5
	.p2align	4, 0x90
.LBB1_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	or	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	add	r11, 1
	add	r9, -1
	jne	.LBB1_4
.LBB1_5:
	cmp	r8, 3
	jb	.LBB1_16
	.p2align	4, 0x90
.LBB1_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	or	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	movzx	eax, byte ptr [rsi + r11 + 1]
	or	al, byte ptr [rdi + r11 + 1]
	mov	byte ptr [rdx + r11 + 1], al
	movzx	eax, byte ptr [rsi + r11 + 2]
	or	al, byte ptr [rdi + r11 + 2]
	mov	byte ptr [rdx + r11 + 2], al
	movzx	eax, byte ptr [rsi + r11 + 3]
	or	al, byte ptr [rdi + r11 + 3]
	mov	byte ptr [rdx + r11 + 3], al
	add	r11, 4
	cmp	rcx, r11
	jne	.LBB1_6
	jmp	.LBB1_16
.LBB1_7:
	lea	r9, [rdx + rcx]
	lea	rax, [rdi + rcx]
	cmp	rax, rdx
	seta	r10b
	lea	rax, [rsi + rcx]
	cmp	r9, rdi
	seta	bl
	cmp	rax, rdx
	seta	r8b
	cmp	r9, rsi
	seta	r9b
	xor	r11d, r11d
	test	r10b, bl
	jne	.LBB1_3
# %bb.8:
	and	r8b, r9b
	jne	.LBB1_3
# %bb.9:
	mov	r11, rcx
	and	r11, -32
	lea	rax, [r11 - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB1_10
# %bb.11:
	mov	r10, r9
	and	r10, -2
	neg	r10
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB1_12:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	orps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	orps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
	movups	xmm0, xmmword ptr [rdi + r8 + 32]
	movups	xmm1, xmmword ptr [rdi + r8 + 48]
	movups	xmm2, xmmword ptr [rsi + r8 + 32]
	orps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 48]
	orps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8 + 32], xmm2
	movups	xmmword ptr [rdx + r8 + 48], xmm0
	add	r8, 64
	add	r10, 2
	jne	.LBB1_12
# %bb.13:
	test	r9b, 1
	je	.LBB1_15
.LBB1_14:
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	orps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	orps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
.LBB1_15:
	cmp	r11, rcx
	jne	.LBB1_3
.LBB1_16:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	ret
.LBB1_10:
	xor	r8d, r8d
	test	r9b, 1
	jne	.LBB1_14
	jmp	.LBB1_15
.Lfunc_end1:
	.size	bitmap_aligned_or_sse4, .Lfunc_end1-bitmap_aligned_or_sse4
                                        # -- End function
	.globl	bitmap_aligned_and_not_sse4     # -- Begin function bitmap_aligned_and_not_sse4
	.p2align	4, 0x90
	.type	bitmap_aligned_and_not_sse4,@function
bitmap_aligned_and_not_sse4:            # @bitmap_aligned_and_not_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	rcx, rcx
	jle	.LBB2_16
# %bb.1:
	cmp	rcx, 31
	ja	.LBB2_7
# %bb.2:
	xor	r11d, r11d
.LBB2_3:
	mov	r8, r11
	not	r8
	test	cl, 1
	je	.LBB2_5
# %bb.4:
	mov	al, byte ptr [rsi + r11]
	not	al
	and	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	or	r11, 1
.LBB2_5:
	add	r8, rcx
	je	.LBB2_16
	.p2align	4, 0x90
.LBB2_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	not	al
	and	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	movzx	eax, byte ptr [rsi + r11 + 1]
	not	al
	and	al, byte ptr [rdi + r11 + 1]
	mov	byte ptr [rdx + r11 + 1], al
	add	r11, 2
	cmp	rcx, r11
	jne	.LBB2_6
	jmp	.LBB2_16
.LBB2_7:
	lea	r9, [rdx + rcx]
	lea	rax, [rdi + rcx]
	cmp	rax, rdx
	seta	r10b
	lea	rax, [rsi + rcx]
	cmp	r9, rdi
	seta	bl
	cmp	rax, rdx
	seta	r8b
	cmp	r9, rsi
	seta	r9b
	xor	r11d, r11d
	test	r10b, bl
	jne	.LBB2_3
# %bb.8:
	and	r8b, r9b
	jne	.LBB2_3
# %bb.9:
	mov	r11, rcx
	and	r11, -32
	lea	rax, [r11 - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB2_10
# %bb.11:
	mov	r10, r9
	and	r10, -2
	neg	r10
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB2_12:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	andnps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	andnps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
	movups	xmm0, xmmword ptr [rdi + r8 + 32]
	movups	xmm1, xmmword ptr [rdi + r8 + 48]
	movups	xmm2, xmmword ptr [rsi + r8 + 32]
	andnps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 48]
	andnps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8 + 32], xmm2
	movups	xmmword ptr [rdx + r8 + 48], xmm0
	add	r8, 64
	add	r10, 2
	jne	.LBB2_12
# %bb.13:
	test	r9b, 1
	je	.LBB2_15
.LBB2_14:
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	andnps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	andnps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
.LBB2_15:
	cmp	r11, rcx
	jne	.LBB2_3
.LBB2_16:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	ret
.LBB2_10:
	xor	r8d, r8d
	test	r9b, 1
	jne	.LBB2_14
	jmp	.LBB2_15
.Lfunc_end2:
	.size	bitmap_aligned_and_not_sse4, .Lfunc_end2-bitmap_aligned_and_not_sse4
                                        # -- End function
	.globl	bitmap_aligned_xor_sse4         # -- Begin function bitmap_aligned_xor_sse4
	.p2align	4, 0x90
	.type	bitmap_aligned_xor_sse4,@function
bitmap_aligned_xor_sse4:                # @bitmap_aligned_xor_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	rbx
	and	rsp, -8
	test	rcx, rcx
	jle	.LBB3_16
# %bb.1:
	cmp	rcx, 31
	ja	.LBB3_7
# %bb.2:
	xor	r11d, r11d
.LBB3_3:
	mov	r8, r11
	not	r8
	add	r8, rcx
	mov	r9, rcx
	and	r9, 3
	je	.LBB3_5
	.p2align	4, 0x90
.LBB3_4:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	xor	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	add	r11, 1
	add	r9, -1
	jne	.LBB3_4
.LBB3_5:
	cmp	r8, 3
	jb	.LBB3_16
	.p2align	4, 0x90
.LBB3_6:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rsi + r11]
	xor	al, byte ptr [rdi + r11]
	mov	byte ptr [rdx + r11], al
	movzx	eax, byte ptr [rsi + r11 + 1]
	xor	al, byte ptr [rdi + r11 + 1]
	mov	byte ptr [rdx + r11 + 1], al
	movzx	eax, byte ptr [rsi + r11 + 2]
	xor	al, byte ptr [rdi + r11 + 2]
	mov	byte ptr [rdx + r11 + 2], al
	movzx	eax, byte ptr [rsi + r11 + 3]
	xor	al, byte ptr [rdi + r11 + 3]
	mov	byte ptr [rdx + r11 + 3], al
	add	r11, 4
	cmp	rcx, r11
	jne	.LBB3_6
	jmp	.LBB3_16
.LBB3_7:
	lea	r9, [rdx + rcx]
	lea	rax, [rdi + rcx]
	cmp	rax, rdx
	seta	r10b
	lea	rax, [rsi + rcx]
	cmp	r9, rdi
	seta	bl
	cmp	rax, rdx
	seta	r8b
	cmp	r9, rsi
	seta	r9b
	xor	r11d, r11d
	test	r10b, bl
	jne	.LBB3_3
# %bb.8:
	and	r8b, r9b
	jne	.LBB3_3
# %bb.9:
	mov	r11, rcx
	and	r11, -32
	lea	rax, [r11 - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB3_10
# %bb.11:
	mov	r10, r9
	and	r10, -2
	neg	r10
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB3_12:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	xorps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	xorps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
	movups	xmm0, xmmword ptr [rdi + r8 + 32]
	movups	xmm1, xmmword ptr [rdi + r8 + 48]
	movups	xmm2, xmmword ptr [rsi + r8 + 32]
	xorps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 48]
	xorps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8 + 32], xmm2
	movups	xmmword ptr [rdx + r8 + 48], xmm0
	add	r8, 64
	add	r10, 2
	jne	.LBB3_12
# %bb.13:
	test	r9b, 1
	je	.LBB3_15
.LBB3_14:
	movups	xmm0, xmmword ptr [rdi + r8]
	movups	xmm1, xmmword ptr [rdi + r8 + 16]
	movups	xmm2, xmmword ptr [rsi + r8]
	xorps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rsi + r8 + 16]
	xorps	xmm0, xmm1
	movups	xmmword ptr [rdx + r8], xmm2
	movups	xmmword ptr [rdx + r8 + 16], xmm0
.LBB3_15:
	cmp	r11, rcx
	jne	.LBB3_3
.LBB3_16:
	lea	rsp, [rbp - 8]
	pop	rbx
	pop	rbp
	ret
.LBB3_10:
	xor	r8d, r8d
	test	r9b, 1
	jne	.LBB3_14
	jmp	.LBB3_15
.Lfunc_end3:
	.size	bitmap_aligned_xor_sse4, .Lfunc_end3-bitmap_aligned_xor_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
