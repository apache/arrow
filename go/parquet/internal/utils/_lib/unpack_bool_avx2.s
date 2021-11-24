	.text
	.intel_syntax noprefix
	.file	"unpack_bool.c"
	.globl	bytes_to_bools_avx2             # -- Begin function bytes_to_bools_avx2
	.p2align	4, 0x90
	.type	bytes_to_bools_avx2,@function
bytes_to_bools_avx2:                    # @bytes_to_bools_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB0_5
# %bb.1:
	mov	r8d, esi
	shl	r8, 3
	xor	r10d, r10d
	jmp	.LBB0_2
	.p2align	4, 0x90
.LBB0_4:                                #   in Loop: Header=BB0_2 Depth=1
	add	r10, 8
	add	rdi, 1
	cmp	r8, r10
	je	.LBB0_5
.LBB0_2:                                # =>This Inner Loop Header: Depth=1
	cmp	r10d, ecx
	jge	.LBB0_4
# %bb.3:                                #   in Loop: Header=BB0_2 Depth=1
	mov	r9d, r10d
	movzx	eax, byte ptr [rdi]
	and	al, 1
	mov	byte ptr [rdx + r9], al
	mov	rsi, r9
	or	rsi, 1
	cmp	esi, ecx
	jge	.LBB0_4
# %bb.6:                                #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al
	and	al, 1
	mov	byte ptr [rdx + rsi], al
	mov	rsi, r9
	or	rsi, 2
	cmp	esi, ecx
	jge	.LBB0_4
# %bb.7:                                #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al, 2
	and	al, 1
	mov	byte ptr [rdx + rsi], al
	mov	rsi, r9
	or	rsi, 3
	cmp	esi, ecx
	jge	.LBB0_4
# %bb.8:                                #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al, 3
	and	al, 1
	mov	byte ptr [rdx + rsi], al
	mov	rsi, r9
	or	rsi, 4
	cmp	esi, ecx
	jge	.LBB0_4
# %bb.9:                                #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al, 4
	and	al, 1
	mov	byte ptr [rdx + rsi], al
	mov	rsi, r9
	or	rsi, 5
	cmp	esi, ecx
	jge	.LBB0_4
# %bb.10:                               #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al, 5
	and	al, 1
	mov	byte ptr [rdx + rsi], al
	mov	rsi, r9
	or	rsi, 6
	cmp	esi, ecx
	jge	.LBB0_4
# %bb.11:                               #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al, 6
	and	al, 1
	mov	byte ptr [rdx + rsi], al
	or	r9, 7
	cmp	r9d, ecx
	jge	.LBB0_4
# %bb.12:                               #   in Loop: Header=BB0_2 Depth=1
	movzx	eax, byte ptr [rdi]
	shr	al, 7
	mov	byte ptr [rdx + r9], al
	jmp	.LBB0_4
.LBB0_5:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	bytes_to_bools_avx2, .Lfunc_end0-bytes_to_bools_avx2
                                        # -- End function
	.ident	"Debian clang version 11.1.0-++20210428103820+1fdec59bffc1-1~exp1~20210428204437.162"
	.section	".note.GNU-stack","",@progbits
	.addrsig
