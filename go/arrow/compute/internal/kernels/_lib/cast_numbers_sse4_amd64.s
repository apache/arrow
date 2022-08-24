	.text
	.intel_syntax noprefix
	.file	"cast_numbers.c"
	.globl	cast_numeric_uint8_uint8_sse4   # -- Begin function cast_numeric_uint8_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint8_sse4,@function
cast_numeric_uint8_uint8_sse4:          # @cast_numeric_uint8_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB0_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 31
	jbe	.LBB0_2
# %bb.7:
	lea	rax, [rdi + r8]
	cmp	rax, rsi
	jbe	.LBB0_9
# %bb.8:
	lea	rax, [rsi + r8]
	cmp	rax, rdi
	jbe	.LBB0_9
.LBB0_2:
	xor	ecx, ecx
.LBB0_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB0_17
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
	cmp	r8, rcx
	jne	.LBB0_6
	jmp	.LBB0_17
.LBB0_9:
	mov	ecx, r8d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdx, rax
	shr	rdx, 5
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB0_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB0_13
.LBB0_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax]
	movups	xmm1, xmmword ptr [rdi + rax + 16]
	movups	xmmword ptr [rsi + rax], xmm0
	movups	xmmword ptr [rsi + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 32]
	movups	xmm1, xmmword ptr [rdi + rax + 48]
	movups	xmmword ptr [rsi + rax + 32], xmm0
	movups	xmmword ptr [rsi + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 64]
	movups	xmm1, xmmword ptr [rdi + rax + 80]
	movups	xmmword ptr [rsi + rax + 64], xmm0
	movups	xmmword ptr [rsi + rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 96]
	movups	xmm1, xmmword ptr [rdi + rax + 112]
	movups	xmmword ptr [rsi + rax + 96], xmm0
	movups	xmmword ptr [rsi + rax + 112], xmm1
	sub	rax, -128
	add	rdx, 4
	jne	.LBB0_12
.LBB0_13:
	test	r9, r9
	je	.LBB0_16
# %bb.14:
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB0_15:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB0_15
.LBB0_16:
	cmp	rcx, r8
	jne	.LBB0_3
.LBB0_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	cast_numeric_uint8_uint8_sse4, .Lfunc_end0-cast_numeric_uint8_uint8_sse4
                                        # -- End function
	.globl	cast_numeric_int8_uint8_sse4    # -- Begin function cast_numeric_int8_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint8_sse4,@function
cast_numeric_int8_uint8_sse4:           # @cast_numeric_int8_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB1_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 31
	jbe	.LBB1_2
# %bb.7:
	lea	rax, [rdi + r8]
	cmp	rax, rsi
	jbe	.LBB1_9
# %bb.8:
	lea	rax, [rsi + r8]
	cmp	rax, rdi
	jbe	.LBB1_9
.LBB1_2:
	xor	ecx, ecx
.LBB1_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB1_17
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
	cmp	r8, rcx
	jne	.LBB1_6
	jmp	.LBB1_17
.LBB1_9:
	mov	ecx, r8d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdx, rax
	shr	rdx, 5
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB1_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB1_13
.LBB1_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB1_12:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax]
	movups	xmm1, xmmword ptr [rdi + rax + 16]
	movups	xmmword ptr [rsi + rax], xmm0
	movups	xmmword ptr [rsi + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 32]
	movups	xmm1, xmmword ptr [rdi + rax + 48]
	movups	xmmword ptr [rsi + rax + 32], xmm0
	movups	xmmword ptr [rsi + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 64]
	movups	xmm1, xmmword ptr [rdi + rax + 80]
	movups	xmmword ptr [rsi + rax + 64], xmm0
	movups	xmmword ptr [rsi + rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 96]
	movups	xmm1, xmmword ptr [rdi + rax + 112]
	movups	xmmword ptr [rsi + rax + 96], xmm0
	movups	xmmword ptr [rsi + rax + 112], xmm1
	sub	rax, -128
	add	rdx, 4
	jne	.LBB1_12
.LBB1_13:
	test	r9, r9
	je	.LBB1_16
# %bb.14:
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB1_15:                               # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB1_15
.LBB1_16:
	cmp	rcx, r8
	jne	.LBB1_3
.LBB1_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end1:
	.size	cast_numeric_int8_uint8_sse4, .Lfunc_end1-cast_numeric_int8_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint16_uint8_sse4
.LCPI2_0:
	.byte	0                               # 0x0
	.byte	2                               # 0x2
	.byte	4                               # 0x4
	.byte	6                               # 0x6
	.byte	8                               # 0x8
	.byte	10                              # 0xa
	.byte	12                              # 0xc
	.byte	14                              # 0xe
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_uint16_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint8_sse4,@function
cast_numeric_uint16_uint8_sse4:         # @cast_numeric_uint16_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB2_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB2_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB2_12:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB2_12
# %bb.13:
	test	r8b, 1
	je	.LBB2_15
.LBB2_14:
	movdqu	xmm0, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_0] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + rdx], xmm0
.LBB2_15:
	cmp	rcx, r9
	jne	.LBB2_3
.LBB2_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB2_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB2_14
	jmp	.LBB2_15
.Lfunc_end2:
	.size	cast_numeric_uint16_uint8_sse4, .Lfunc_end2-cast_numeric_uint16_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int16_uint8_sse4
.LCPI3_0:
	.byte	0                               # 0x0
	.byte	2                               # 0x2
	.byte	4                               # 0x4
	.byte	6                               # 0x6
	.byte	8                               # 0x8
	.byte	10                              # 0xa
	.byte	12                              # 0xc
	.byte	14                              # 0xe
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_int16_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint8_sse4,@function
cast_numeric_int16_uint8_sse4:          # @cast_numeric_int16_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB3_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB3_12:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB3_12
# %bb.13:
	test	r8b, 1
	je	.LBB3_15
.LBB3_14:
	movdqu	xmm0, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI3_0] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + rdx], xmm0
.LBB3_15:
	cmp	rcx, r9
	jne	.LBB3_3
.LBB3_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB3_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB3_14
	jmp	.LBB3_15
.Lfunc_end3:
	.size	cast_numeric_int16_uint8_sse4, .Lfunc_end3-cast_numeric_int16_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_uint8_sse4
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
	.globl	cast_numeric_uint32_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint8_sse4,@function
cast_numeric_uint32_uint8_sse4:         # @cast_numeric_uint32_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB4_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB4_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI4_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB4_12:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx], xmm1
	movd	dword ptr [rsi + rdx + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx + 8], xmm1
	movd	dword ptr [rsi + rdx + 12], xmm2
	add	rdx, 16
	add	rax, 2
	jne	.LBB4_12
# %bb.13:
	test	r8b, 1
	je	.LBB4_15
.LBB4_14:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_0] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
.LBB4_15:
	cmp	rcx, r9
	jne	.LBB4_3
.LBB4_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB4_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB4_14
	jmp	.LBB4_15
.Lfunc_end4:
	.size	cast_numeric_uint32_uint8_sse4, .Lfunc_end4-cast_numeric_uint32_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int32_uint8_sse4
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
	.globl	cast_numeric_int32_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint8_sse4,@function
cast_numeric_int32_uint8_sse4:          # @cast_numeric_int32_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB5_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB5_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI5_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB5_12:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx], xmm1
	movd	dword ptr [rsi + rdx + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx + 8], xmm1
	movd	dword ptr [rsi + rdx + 12], xmm2
	add	rdx, 16
	add	rax, 2
	jne	.LBB5_12
# %bb.13:
	test	r8b, 1
	je	.LBB5_15
.LBB5_14:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI5_0] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
.LBB5_15:
	cmp	rcx, r9
	jne	.LBB5_3
.LBB5_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB5_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB5_14
	jmp	.LBB5_15
.Lfunc_end5:
	.size	cast_numeric_int32_uint8_sse4, .Lfunc_end5-cast_numeric_int32_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint64_uint8_sse4
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
	.globl	cast_numeric_uint64_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint8_sse4,@function
cast_numeric_uint64_uint8_sse4:         # @cast_numeric_uint64_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB6_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB6_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI6_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB6_12:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 6], xmm2, 0
	add	rdx, 8
	add	rax, 2
	jne	.LBB6_12
# %bb.13:
	test	r8b, 1
	je	.LBB6_15
.LBB6_14:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI6_0] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rsi + rdx], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rsi + rdx + 2], xmm1, 0
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
	.size	cast_numeric_uint64_uint8_sse4, .Lfunc_end6-cast_numeric_uint64_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int64_uint8_sse4
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
	.globl	cast_numeric_int64_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint8_sse4,@function
cast_numeric_int64_uint8_sse4:          # @cast_numeric_int64_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB7_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB7_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI7_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB7_12:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 6], xmm2, 0
	add	rdx, 8
	add	rax, 2
	jne	.LBB7_12
# %bb.13:
	test	r8b, 1
	je	.LBB7_15
.LBB7_14:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI7_0] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rsi + rdx], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rsi + rdx + 2], xmm1, 0
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
	.size	cast_numeric_int64_uint8_sse4, .Lfunc_end7-cast_numeric_int64_uint8_sse4
                                        # -- End function
	.globl	cast_numeric_float32_uint8_sse4 # -- Begin function cast_numeric_float32_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint8_sse4,@function
cast_numeric_float32_uint8_sse4:        # @cast_numeric_float32_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB8_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB8_4
.LBB8_5:
	cmp	r8, 3
	jb	.LBB8_16
	.p2align	4, 0x90
.LBB8_6:                                # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	cvttss2si	eax, dword ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	cvttss2si	eax, dword ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	cvttss2si	eax, dword ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB8_6
	jmp	.LBB8_16
.LBB8_9:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	packusdw	xmm0, xmm0
	packuswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm1, xmm1
	packuswb	xmm1, xmm1
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rdx + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 48]
	cvttps2dq	xmm0, xmm0
	packusdw	xmm0, xmm0
	packuswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm1, xmm1
	packuswb	xmm1, xmm1
	movd	dword ptr [rsi + rdx + 8], xmm0
	movd	dword ptr [rsi + rdx + 12], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB8_12
# %bb.13:
	test	r8b, 1
	je	.LBB8_15
.LBB8_14:
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	packusdw	xmm0, xmm0
	packuswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm1, xmm1
	packuswb	xmm1, xmm1
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
.LBB8_15:
	cmp	rcx, r9
	jne	.LBB8_3
.LBB8_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB8_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB8_14
	jmp	.LBB8_15
.Lfunc_end8:
	.size	cast_numeric_float32_uint8_sse4, .Lfunc_end8-cast_numeric_float32_uint8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_float64_uint8_sse4
.LCPI9_0:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
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
	.globl	cast_numeric_float64_uint8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint8_sse4,@function
cast_numeric_float64_uint8_sse4:        # @cast_numeric_float64_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB9_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB9_4
.LBB9_5:
	cmp	r8, 3
	jb	.LBB9_16
	.p2align	4, 0x90
.LBB9_6:                                # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	cvttsd2si	eax, qword ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	cvttsd2si	eax, qword ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	cvttsd2si	eax, qword ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB9_6
	jmp	.LBB9_16
.LBB9_9:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB9_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI9_0] # xmm0 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB9_12:                               # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdi + 8*rdx]
	movupd	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 2], xmm2, 0
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	movupd	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 6], xmm2, 0
	add	rdx, 8
	add	rax, 2
	jne	.LBB9_12
# %bb.13:
	test	r8b, 1
	je	.LBB9_15
.LBB9_14:
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	cvttpd2dq	xmm0, xmm0
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI9_0] # xmm2 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	cvttpd2dq	xmm1, xmm1
	pshufb	xmm0, xmm2
	pextrw	word ptr [rsi + rdx], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rsi + rdx + 2], xmm1, 0
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
	.size	cast_numeric_float64_uint8_sse4, .Lfunc_end9-cast_numeric_float64_uint8_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_int8_sse4    # -- Begin function cast_numeric_uint8_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int8_sse4,@function
cast_numeric_uint8_int8_sse4:           # @cast_numeric_uint8_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB10_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 31
	jbe	.LBB10_2
# %bb.7:
	lea	rax, [rdi + r8]
	cmp	rax, rsi
	jbe	.LBB10_9
# %bb.8:
	lea	rax, [rsi + r8]
	cmp	rax, rdi
	jbe	.LBB10_9
.LBB10_2:
	xor	ecx, ecx
.LBB10_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB10_17
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
	cmp	r8, rcx
	jne	.LBB10_6
	jmp	.LBB10_17
.LBB10_9:
	mov	ecx, r8d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdx, rax
	shr	rdx, 5
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB10_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB10_13
.LBB10_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB10_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax]
	movups	xmm1, xmmword ptr [rdi + rax + 16]
	movups	xmmword ptr [rsi + rax], xmm0
	movups	xmmword ptr [rsi + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 32]
	movups	xmm1, xmmword ptr [rdi + rax + 48]
	movups	xmmword ptr [rsi + rax + 32], xmm0
	movups	xmmword ptr [rsi + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 64]
	movups	xmm1, xmmword ptr [rdi + rax + 80]
	movups	xmmword ptr [rsi + rax + 64], xmm0
	movups	xmmword ptr [rsi + rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 96]
	movups	xmm1, xmmword ptr [rdi + rax + 112]
	movups	xmmword ptr [rsi + rax + 96], xmm0
	movups	xmmword ptr [rsi + rax + 112], xmm1
	sub	rax, -128
	add	rdx, 4
	jne	.LBB10_12
.LBB10_13:
	test	r9, r9
	je	.LBB10_16
# %bb.14:
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB10_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB10_15
.LBB10_16:
	cmp	rcx, r8
	jne	.LBB10_3
.LBB10_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end10:
	.size	cast_numeric_uint8_int8_sse4, .Lfunc_end10-cast_numeric_uint8_int8_sse4
                                        # -- End function
	.globl	cast_numeric_int8_int8_sse4     # -- Begin function cast_numeric_int8_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_int8_sse4,@function
cast_numeric_int8_int8_sse4:            # @cast_numeric_int8_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB11_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 31
	jbe	.LBB11_2
# %bb.7:
	lea	rax, [rdi + r8]
	cmp	rax, rsi
	jbe	.LBB11_9
# %bb.8:
	lea	rax, [rsi + r8]
	cmp	rax, rdi
	jbe	.LBB11_9
.LBB11_2:
	xor	ecx, ecx
.LBB11_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB11_17
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
	cmp	r8, rcx
	jne	.LBB11_6
	jmp	.LBB11_17
.LBB11_9:
	mov	ecx, r8d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdx, rax
	shr	rdx, 5
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB11_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB11_13
.LBB11_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB11_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax]
	movups	xmm1, xmmword ptr [rdi + rax + 16]
	movups	xmmword ptr [rsi + rax], xmm0
	movups	xmmword ptr [rsi + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 32]
	movups	xmm1, xmmword ptr [rdi + rax + 48]
	movups	xmmword ptr [rsi + rax + 32], xmm0
	movups	xmmword ptr [rsi + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 64]
	movups	xmm1, xmmword ptr [rdi + rax + 80]
	movups	xmmword ptr [rsi + rax + 64], xmm0
	movups	xmmword ptr [rsi + rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + rax + 96]
	movups	xmm1, xmmword ptr [rdi + rax + 112]
	movups	xmmword ptr [rsi + rax + 96], xmm0
	movups	xmmword ptr [rsi + rax + 112], xmm1
	sub	rax, -128
	add	rdx, 4
	jne	.LBB11_12
.LBB11_13:
	test	r9, r9
	je	.LBB11_16
# %bb.14:
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB11_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB11_15
.LBB11_16:
	cmp	rcx, r8
	jne	.LBB11_3
.LBB11_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end11:
	.size	cast_numeric_int8_int8_sse4, .Lfunc_end11-cast_numeric_int8_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint16_int8_sse4
.LCPI12_0:
	.byte	0                               # 0x0
	.byte	2                               # 0x2
	.byte	4                               # 0x4
	.byte	6                               # 0x6
	.byte	8                               # 0x8
	.byte	10                              # 0xa
	.byte	12                              # 0xc
	.byte	14                              # 0xe
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_uint16_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int8_sse4,@function
cast_numeric_uint16_int8_sse4:          # @cast_numeric_uint16_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB12_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB12_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI12_0] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB12_12:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB12_12
# %bb.13:
	test	r8b, 1
	je	.LBB12_15
.LBB12_14:
	movdqu	xmm0, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI12_0] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + rdx], xmm0
.LBB12_15:
	cmp	rcx, r9
	jne	.LBB12_3
.LBB12_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB12_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB12_14
	jmp	.LBB12_15
.Lfunc_end12:
	.size	cast_numeric_uint16_int8_sse4, .Lfunc_end12-cast_numeric_uint16_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int16_int8_sse4
.LCPI13_0:
	.byte	0                               # 0x0
	.byte	2                               # 0x2
	.byte	4                               # 0x4
	.byte	6                               # 0x6
	.byte	8                               # 0x8
	.byte	10                              # 0xa
	.byte	12                              # 0xc
	.byte	14                              # 0xe
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.text
	.globl	cast_numeric_int16_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_int8_sse4,@function
cast_numeric_int16_int8_sse4:           # @cast_numeric_int16_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB13_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB13_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI13_0] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB13_12:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 2*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + rdx + 16], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB13_12
# %bb.13:
	test	r8b, 1
	je	.LBB13_15
.LBB13_14:
	movdqu	xmm0, xmmword ptr [rdi + 2*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 2*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI13_0] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + rdx], xmm0
.LBB13_15:
	cmp	rcx, r9
	jne	.LBB13_3
.LBB13_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB13_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB13_14
	jmp	.LBB13_15
.Lfunc_end13:
	.size	cast_numeric_int16_int8_sse4, .Lfunc_end13-cast_numeric_int16_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_int8_sse4
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
	.globl	cast_numeric_uint32_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int8_sse4,@function
cast_numeric_uint32_int8_sse4:          # @cast_numeric_uint32_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB14_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB14_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI14_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB14_12:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx], xmm1
	movd	dword ptr [rsi + rdx + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx + 8], xmm1
	movd	dword ptr [rsi + rdx + 12], xmm2
	add	rdx, 16
	add	rax, 2
	jne	.LBB14_12
# %bb.13:
	test	r8b, 1
	je	.LBB14_15
.LBB14_14:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI14_0] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
.LBB14_15:
	cmp	rcx, r9
	jne	.LBB14_3
.LBB14_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB14_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB14_14
	jmp	.LBB14_15
.Lfunc_end14:
	.size	cast_numeric_uint32_int8_sse4, .Lfunc_end14-cast_numeric_uint32_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int32_int8_sse4
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
	.globl	cast_numeric_int32_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_int8_sse4,@function
cast_numeric_int32_int8_sse4:           # @cast_numeric_int32_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB15_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB15_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI15_0] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB15_12:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx], xmm1
	movd	dword ptr [rsi + rdx + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rsi + rdx + 8], xmm1
	movd	dword ptr [rsi + rdx + 12], xmm2
	add	rdx, 16
	add	rax, 2
	jne	.LBB15_12
# %bb.13:
	test	r8b, 1
	je	.LBB15_15
.LBB15_14:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI15_0] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
.LBB15_15:
	cmp	rcx, r9
	jne	.LBB15_3
.LBB15_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB15_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB15_14
	jmp	.LBB15_15
.Lfunc_end15:
	.size	cast_numeric_int32_int8_sse4, .Lfunc_end15-cast_numeric_int32_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint64_int8_sse4
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
	.globl	cast_numeric_uint64_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int8_sse4,@function
cast_numeric_uint64_int8_sse4:          # @cast_numeric_uint64_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB16_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB16_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI16_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB16_12:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 6], xmm2, 0
	add	rdx, 8
	add	rax, 2
	jne	.LBB16_12
# %bb.13:
	test	r8b, 1
	je	.LBB16_15
.LBB16_14:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI16_0] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rsi + rdx], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rsi + rdx + 2], xmm1, 0
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
	.size	cast_numeric_uint64_int8_sse4, .Lfunc_end16-cast_numeric_uint64_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int64_int8_sse4
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
	.globl	cast_numeric_int64_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_int8_sse4,@function
cast_numeric_int64_int8_sse4:           # @cast_numeric_int64_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB17_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB17_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI17_0] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB17_12:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 6], xmm2, 0
	add	rdx, 8
	add	rax, 2
	jne	.LBB17_12
# %bb.13:
	test	r8b, 1
	je	.LBB17_15
.LBB17_14:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI17_0] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rsi + rdx], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rsi + rdx + 2], xmm1, 0
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
	.size	cast_numeric_int64_int8_sse4, .Lfunc_end17-cast_numeric_int64_int8_sse4
                                        # -- End function
	.globl	cast_numeric_float32_int8_sse4  # -- Begin function cast_numeric_float32_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_int8_sse4,@function
cast_numeric_float32_int8_sse4:         # @cast_numeric_float32_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB18_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB18_4
.LBB18_5:
	cmp	r8, 3
	jb	.LBB18_16
	.p2align	4, 0x90
.LBB18_6:                               # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	byte ptr [rsi + rcx], al
	cvttss2si	eax, dword ptr [rdi + 4*rcx + 4]
	mov	byte ptr [rsi + rcx + 1], al
	cvttss2si	eax, dword ptr [rdi + 4*rcx + 8]
	mov	byte ptr [rsi + rcx + 2], al
	cvttss2si	eax, dword ptr [rdi + 4*rcx + 12]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB18_6
	jmp	.LBB18_16
.LBB18_9:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rdx + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 48]
	cvttps2dq	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movd	dword ptr [rsi + rdx + 8], xmm0
	movd	dword ptr [rsi + rdx + 12], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB18_12
# %bb.13:
	test	r8b, 1
	je	.LBB18_15
.LBB18_14:
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movd	dword ptr [rsi + rdx], xmm0
	movd	dword ptr [rsi + rdx + 4], xmm1
.LBB18_15:
	cmp	rcx, r9
	jne	.LBB18_3
.LBB18_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB18_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB18_14
	jmp	.LBB18_15
.Lfunc_end18:
	.size	cast_numeric_float32_int8_sse4, .Lfunc_end18-cast_numeric_float32_int8_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_float64_int8_sse4
.LCPI19_0:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
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
	.globl	cast_numeric_float64_int8_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_int8_sse4,@function
cast_numeric_float64_int8_sse4:         # @cast_numeric_float64_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB19_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	add	rcx, 1
	add	rdx, -1
	jne	.LBB19_4
.LBB19_5:
	cmp	r8, 3
	jb	.LBB19_16
	.p2align	4, 0x90
.LBB19_6:                               # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	byte ptr [rsi + rcx], al
	cvttsd2si	eax, qword ptr [rdi + 8*rcx + 8]
	mov	byte ptr [rsi + rcx + 1], al
	cvttsd2si	eax, qword ptr [rdi + 8*rcx + 16]
	mov	byte ptr [rsi + rcx + 2], al
	cvttsd2si	eax, qword ptr [rdi + 8*rcx + 24]
	mov	byte ptr [rsi + rcx + 3], al
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB19_6
	jmp	.LBB19_16
.LBB19_9:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB19_10
# %bb.11:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI19_0] # xmm0 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	.p2align	4, 0x90
.LBB19_12:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdi + 8*rdx]
	movupd	xmm2, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 2], xmm2, 0
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 32]
	movupd	xmm2, xmmword ptr [rdi + 8*rdx + 48]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rsi + rdx + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rsi + rdx + 6], xmm2, 0
	add	rdx, 8
	add	rax, 2
	jne	.LBB19_12
# %bb.13:
	test	r8b, 1
	je	.LBB19_15
.LBB19_14:
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	cvttpd2dq	xmm0, xmm0
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI19_0] # xmm2 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	cvttpd2dq	xmm1, xmm1
	pshufb	xmm0, xmm2
	pextrw	word ptr [rsi + rdx], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rsi + rdx + 2], xmm1, 0
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
	.size	cast_numeric_float64_int8_sse4, .Lfunc_end19-cast_numeric_float64_int8_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_uint16_sse4  # -- Begin function cast_numeric_uint8_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint16_sse4,@function
cast_numeric_uint8_uint16_sse4:         # @cast_numeric_uint8_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB20_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	mov	rax, r9
	and	rax, 3
	je	.LBB20_5
	.p2align	4, 0x90
.LBB20_4:                               # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], dx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
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
	pmovzxbw	xmm0, qword ptr [rdi + rdx]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdi + rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	pmovzxbw	xmm0, qword ptr [rdi + rdx + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdi + rdx + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rsi + 2*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 48], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB20_12
# %bb.13:
	test	r8b, 1
	je	.LBB20_15
.LBB20_14:
	pmovzxbw	xmm0, qword ptr [rdi + rdx]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdi + rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
.LBB20_15:
	cmp	rcx, r9
	jne	.LBB20_3
.LBB20_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB20_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB20_14
	jmp	.LBB20_15
.Lfunc_end20:
	.size	cast_numeric_uint8_uint16_sse4, .Lfunc_end20-cast_numeric_uint8_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_int8_uint16_sse4   # -- Begin function cast_numeric_int8_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint16_sse4,@function
cast_numeric_int8_uint16_sse4:          # @cast_numeric_int8_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB21_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	mov	rax, r9
	and	rax, 3
	je	.LBB21_5
	.p2align	4, 0x90
.LBB21_4:                               # =>This Inner Loop Header: Depth=1
	movsx	edx, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], dx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
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
	pmovsxbw	xmm0, qword ptr [rdi + rdx]
	pmovsxbw	xmm1, qword ptr [rdi + rdx + 8]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	pmovsxbw	xmm0, qword ptr [rdi + rdx + 16]
	pmovsxbw	xmm1, qword ptr [rdi + rdx + 24]
	movdqu	xmmword ptr [rsi + 2*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 48], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB21_12
# %bb.13:
	test	r8b, 1
	je	.LBB21_15
.LBB21_14:
	pmovsxbw	xmm0, qword ptr [rdi + rdx]
	pmovsxbw	xmm1, qword ptr [rdi + rdx + 8]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
.LBB21_15:
	cmp	rcx, r9
	jne	.LBB21_3
.LBB21_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB21_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB21_14
	jmp	.LBB21_15
.Lfunc_end21:
	.size	cast_numeric_int8_uint16_sse4, .Lfunc_end21-cast_numeric_int8_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_uint16_sse4 # -- Begin function cast_numeric_uint16_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint16_sse4,@function
cast_numeric_uint16_uint16_sse4:        # @cast_numeric_uint16_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB22_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	jbe	.LBB22_2
# %bb.7:
	lea	rax, [rdi + 2*r8]
	cmp	rax, rsi
	jbe	.LBB22_9
# %bb.8:
	lea	rax, [rsi + 2*r8]
	cmp	rax, rdi
	jbe	.LBB22_9
.LBB22_2:
	xor	ecx, ecx
.LBB22_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB22_17
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
	cmp	r8, rcx
	jne	.LBB22_6
	jmp	.LBB22_17
.LBB22_9:
	mov	ecx, r8d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdx, rax
	shr	rdx, 4
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 48
	jae	.LBB22_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB22_13
.LBB22_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB22_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 2*rax]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 16]
	movups	xmmword ptr [rsi + 2*rax], xmm0
	movups	xmmword ptr [rsi + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 48]
	movups	xmmword ptr [rsi + 2*rax + 32], xmm0
	movups	xmmword ptr [rsi + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 80]
	movups	xmmword ptr [rsi + 2*rax + 64], xmm0
	movups	xmmword ptr [rsi + 2*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 112]
	movups	xmmword ptr [rsi + 2*rax + 96], xmm0
	movups	xmmword ptr [rsi + 2*rax + 112], xmm1
	add	rax, 64
	add	rdx, 4
	jne	.LBB22_12
.LBB22_13:
	test	r9, r9
	je	.LBB22_16
# %bb.14:
	add	rax, rax
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB22_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB22_15
.LBB22_16:
	cmp	rcx, r8
	jne	.LBB22_3
.LBB22_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end22:
	.size	cast_numeric_uint16_uint16_sse4, .Lfunc_end22-cast_numeric_uint16_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_int16_uint16_sse4  # -- Begin function cast_numeric_int16_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint16_sse4,@function
cast_numeric_int16_uint16_sse4:         # @cast_numeric_int16_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB23_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	jbe	.LBB23_2
# %bb.7:
	lea	rax, [rdi + 2*r8]
	cmp	rax, rsi
	jbe	.LBB23_9
# %bb.8:
	lea	rax, [rsi + 2*r8]
	cmp	rax, rdi
	jbe	.LBB23_9
.LBB23_2:
	xor	ecx, ecx
.LBB23_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB23_17
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
	cmp	r8, rcx
	jne	.LBB23_6
	jmp	.LBB23_17
.LBB23_9:
	mov	ecx, r8d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdx, rax
	shr	rdx, 4
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 48
	jae	.LBB23_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB23_13
.LBB23_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB23_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 2*rax]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 16]
	movups	xmmword ptr [rsi + 2*rax], xmm0
	movups	xmmword ptr [rsi + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 48]
	movups	xmmword ptr [rsi + 2*rax + 32], xmm0
	movups	xmmword ptr [rsi + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 80]
	movups	xmmword ptr [rsi + 2*rax + 64], xmm0
	movups	xmmword ptr [rsi + 2*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 112]
	movups	xmmword ptr [rsi + 2*rax + 96], xmm0
	movups	xmmword ptr [rsi + 2*rax + 112], xmm1
	add	rax, 64
	add	rdx, 4
	jne	.LBB23_12
.LBB23_13:
	test	r9, r9
	je	.LBB23_16
# %bb.14:
	add	rax, rax
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB23_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB23_15
.LBB23_16:
	cmp	rcx, r8
	jne	.LBB23_3
.LBB23_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end23:
	.size	cast_numeric_int16_uint16_sse4, .Lfunc_end23-cast_numeric_int16_uint16_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_uint16_sse4
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
	.text
	.globl	cast_numeric_uint32_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint16_sse4,@function
cast_numeric_uint32_uint16_sse4:        # @cast_numeric_uint32_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB24_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB24_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB24_10
.LBB24_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB24_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI24_0] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	.p2align	4, 0x90
.LBB24_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB24_6
# %bb.7:
	test	r8b, 1
	je	.LBB24_9
.LBB24_8:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI24_0] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
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
	ret
.LBB24_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB24_8
	jmp	.LBB24_9
.Lfunc_end24:
	.size	cast_numeric_uint32_uint16_sse4, .Lfunc_end24-cast_numeric_uint32_uint16_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int32_uint16_sse4
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
	.text
	.globl	cast_numeric_int32_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint16_sse4,@function
cast_numeric_int32_uint16_sse4:         # @cast_numeric_int32_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB25_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB25_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB25_10
.LBB25_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB25_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI25_0] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	.p2align	4, 0x90
.LBB25_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB25_6
# %bb.7:
	test	r8b, 1
	je	.LBB25_9
.LBB25_8:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI25_0] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
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
	ret
.LBB25_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB25_8
	jmp	.LBB25_9
.Lfunc_end25:
	.size	cast_numeric_int32_uint16_sse4, .Lfunc_end25-cast_numeric_int32_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_uint64_uint16_sse4 # -- Begin function cast_numeric_uint64_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint16_sse4,@function
cast_numeric_uint64_uint16_sse4:        # @cast_numeric_uint64_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB26_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB26_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB26_10
.LBB26_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB26_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB26_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx + 8], xmm0
	movd	dword ptr [rsi + 2*rdx + 12], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB26_6
# %bb.7:
	test	r8b, 1
	je	.LBB26_9
.LBB26_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
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
	ret
.LBB26_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB26_8
	jmp	.LBB26_9
.Lfunc_end26:
	.size	cast_numeric_uint64_uint16_sse4, .Lfunc_end26-cast_numeric_uint64_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_int64_uint16_sse4  # -- Begin function cast_numeric_int64_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint16_sse4,@function
cast_numeric_int64_uint16_sse4:         # @cast_numeric_int64_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB27_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB27_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB27_10
.LBB27_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB27_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB27_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx + 8], xmm0
	movd	dword ptr [rsi + 2*rdx + 12], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB27_6
# %bb.7:
	test	r8b, 1
	je	.LBB27_9
.LBB27_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
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
	ret
.LBB27_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB27_8
	jmp	.LBB27_9
.Lfunc_end27:
	.size	cast_numeric_int64_uint16_sse4, .Lfunc_end27-cast_numeric_int64_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_float32_uint16_sse4 # -- Begin function cast_numeric_float32_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint16_sse4,@function
cast_numeric_float32_uint16_sse4:       # @cast_numeric_float32_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB28_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB28_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB28_10
.LBB28_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm0, xmm1
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movups	xmm0, xmmword ptr [rdi + 4*rdx + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 48]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm0, xmm1
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm0
	add	rdx, 16
	add	rax, 2
	jne	.LBB28_6
# %bb.7:
	test	r8b, 1
	je	.LBB28_9
.LBB28_8:
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm0, xmm1
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
.LBB28_9:
	cmp	rcx, r9
	je	.LBB28_11
	.p2align	4, 0x90
.LBB28_10:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB28_10
.LBB28_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB28_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB28_8
	jmp	.LBB28_9
.Lfunc_end28:
	.size	cast_numeric_float32_uint16_sse4, .Lfunc_end28-cast_numeric_float32_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_float64_uint16_sse4 # -- Begin function cast_numeric_float64_uint16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint16_sse4,@function
cast_numeric_float64_uint16_sse4:       # @cast_numeric_float64_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB29_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB29_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB29_10
.LBB29_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
	movupd	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	cvttpd2dq	xmm0, xmm0
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx + 8], xmm0
	movd	dword ptr [rsi + 2*rdx + 12], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB29_6
# %bb.7:
	test	r8b, 1
	je	.LBB29_9
.LBB29_8:
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
.LBB29_9:
	cmp	rcx, r9
	je	.LBB29_11
	.p2align	4, 0x90
.LBB29_10:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB29_10
.LBB29_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB29_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB29_8
	jmp	.LBB29_9
.Lfunc_end29:
	.size	cast_numeric_float64_uint16_sse4, .Lfunc_end29-cast_numeric_float64_uint16_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_int16_sse4   # -- Begin function cast_numeric_uint8_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int16_sse4,@function
cast_numeric_uint8_int16_sse4:          # @cast_numeric_uint8_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB30_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	mov	rax, r9
	and	rax, 3
	je	.LBB30_5
	.p2align	4, 0x90
.LBB30_4:                               # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], dx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
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
	pmovzxbw	xmm0, qword ptr [rdi + rdx]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdi + rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	pmovzxbw	xmm0, qword ptr [rdi + rdx + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdi + rdx + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rsi + 2*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 48], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB30_12
# %bb.13:
	test	r8b, 1
	je	.LBB30_15
.LBB30_14:
	pmovzxbw	xmm0, qword ptr [rdi + rdx]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdi + rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
.LBB30_15:
	cmp	rcx, r9
	jne	.LBB30_3
.LBB30_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB30_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB30_14
	jmp	.LBB30_15
.Lfunc_end30:
	.size	cast_numeric_uint8_int16_sse4, .Lfunc_end30-cast_numeric_uint8_int16_sse4
                                        # -- End function
	.globl	cast_numeric_int8_int16_sse4    # -- Begin function cast_numeric_int8_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_int16_sse4,@function
cast_numeric_int8_int16_sse4:           # @cast_numeric_int8_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB31_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 15
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
	mov	rax, r9
	and	rax, 3
	je	.LBB31_5
	.p2align	4, 0x90
.LBB31_4:                               # =>This Inner Loop Header: Depth=1
	movsx	edx, byte ptr [rdi + rcx]
	mov	word ptr [rsi + 2*rcx], dx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r8, rax
	shr	r8, 4
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
	pmovsxbw	xmm0, qword ptr [rdi + rdx]
	pmovsxbw	xmm1, qword ptr [rdi + rdx + 8]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	pmovsxbw	xmm0, qword ptr [rdi + rdx + 16]
	pmovsxbw	xmm1, qword ptr [rdi + rdx + 24]
	movdqu	xmmword ptr [rsi + 2*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 48], xmm1
	add	rdx, 32
	add	rax, 2
	jne	.LBB31_12
# %bb.13:
	test	r8b, 1
	je	.LBB31_15
.LBB31_14:
	pmovsxbw	xmm0, qword ptr [rdi + rdx]
	pmovsxbw	xmm1, qword ptr [rdi + rdx + 8]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
.LBB31_15:
	cmp	rcx, r9
	jne	.LBB31_3
.LBB31_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB31_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB31_14
	jmp	.LBB31_15
.Lfunc_end31:
	.size	cast_numeric_int8_int16_sse4, .Lfunc_end31-cast_numeric_int8_int16_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_int16_sse4  # -- Begin function cast_numeric_uint16_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int16_sse4,@function
cast_numeric_uint16_int16_sse4:         # @cast_numeric_uint16_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB32_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	jbe	.LBB32_2
# %bb.7:
	lea	rax, [rdi + 2*r8]
	cmp	rax, rsi
	jbe	.LBB32_9
# %bb.8:
	lea	rax, [rsi + 2*r8]
	cmp	rax, rdi
	jbe	.LBB32_9
.LBB32_2:
	xor	ecx, ecx
.LBB32_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB32_17
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
	cmp	r8, rcx
	jne	.LBB32_6
	jmp	.LBB32_17
.LBB32_9:
	mov	ecx, r8d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdx, rax
	shr	rdx, 4
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 48
	jae	.LBB32_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB32_13
.LBB32_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB32_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 2*rax]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 16]
	movups	xmmword ptr [rsi + 2*rax], xmm0
	movups	xmmword ptr [rsi + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 48]
	movups	xmmword ptr [rsi + 2*rax + 32], xmm0
	movups	xmmword ptr [rsi + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 80]
	movups	xmmword ptr [rsi + 2*rax + 64], xmm0
	movups	xmmword ptr [rsi + 2*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 112]
	movups	xmmword ptr [rsi + 2*rax + 96], xmm0
	movups	xmmword ptr [rsi + 2*rax + 112], xmm1
	add	rax, 64
	add	rdx, 4
	jne	.LBB32_12
.LBB32_13:
	test	r9, r9
	je	.LBB32_16
# %bb.14:
	add	rax, rax
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB32_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB32_15
.LBB32_16:
	cmp	rcx, r8
	jne	.LBB32_3
.LBB32_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end32:
	.size	cast_numeric_uint16_int16_sse4, .Lfunc_end32-cast_numeric_uint16_int16_sse4
                                        # -- End function
	.globl	cast_numeric_int16_int16_sse4   # -- Begin function cast_numeric_int16_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_int16_sse4,@function
cast_numeric_int16_int16_sse4:          # @cast_numeric_int16_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB33_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 15
	jbe	.LBB33_2
# %bb.7:
	lea	rax, [rdi + 2*r8]
	cmp	rax, rsi
	jbe	.LBB33_9
# %bb.8:
	lea	rax, [rsi + 2*r8]
	cmp	rax, rdi
	jbe	.LBB33_9
.LBB33_2:
	xor	ecx, ecx
.LBB33_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB33_17
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
	cmp	r8, rcx
	jne	.LBB33_6
	jmp	.LBB33_17
.LBB33_9:
	mov	ecx, r8d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdx, rax
	shr	rdx, 4
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 48
	jae	.LBB33_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB33_13
.LBB33_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB33_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 2*rax]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 16]
	movups	xmmword ptr [rsi + 2*rax], xmm0
	movups	xmmword ptr [rsi + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 48]
	movups	xmmword ptr [rsi + 2*rax + 32], xmm0
	movups	xmmword ptr [rsi + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 80]
	movups	xmmword ptr [rsi + 2*rax + 64], xmm0
	movups	xmmword ptr [rsi + 2*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 2*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 2*rax + 112]
	movups	xmmword ptr [rsi + 2*rax + 96], xmm0
	movups	xmmword ptr [rsi + 2*rax + 112], xmm1
	add	rax, 64
	add	rdx, 4
	jne	.LBB33_12
.LBB33_13:
	test	r9, r9
	je	.LBB33_16
# %bb.14:
	add	rax, rax
	add	rax, 16
	neg	r9
	.p2align	4, 0x90
.LBB33_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB33_15
.LBB33_16:
	cmp	rcx, r8
	jne	.LBB33_3
.LBB33_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end33:
	.size	cast_numeric_int16_int16_sse4, .Lfunc_end33-cast_numeric_int16_int16_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_int16_sse4
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
	.text
	.globl	cast_numeric_uint32_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int16_sse4,@function
cast_numeric_uint32_int16_sse4:         # @cast_numeric_uint32_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB34_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB34_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB34_10
.LBB34_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB34_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI34_0] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	.p2align	4, 0x90
.LBB34_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB34_6
# %bb.7:
	test	r8b, 1
	je	.LBB34_9
.LBB34_8:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI34_0] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
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
	ret
.LBB34_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB34_8
	jmp	.LBB34_9
.Lfunc_end34:
	.size	cast_numeric_uint32_int16_sse4, .Lfunc_end34-cast_numeric_uint32_int16_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_int32_int16_sse4
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
	.text
	.globl	cast_numeric_int32_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_int16_sse4,@function
cast_numeric_int32_int16_sse4:          # @cast_numeric_int32_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB35_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB35_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB35_10
.LBB35_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB35_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI35_0] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	.p2align	4, 0x90
.LBB35_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm1
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm2, xmmword ptr [rdi + 4*rdx + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB35_6
# %bb.7:
	test	r8b, 1
	je	.LBB35_9
.LBB35_8:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI35_0] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
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
	ret
.LBB35_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB35_8
	jmp	.LBB35_9
.Lfunc_end35:
	.size	cast_numeric_int32_int16_sse4, .Lfunc_end35-cast_numeric_int32_int16_sse4
                                        # -- End function
	.globl	cast_numeric_uint64_int16_sse4  # -- Begin function cast_numeric_uint64_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int16_sse4,@function
cast_numeric_uint64_int16_sse4:         # @cast_numeric_uint64_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB36_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB36_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB36_10
.LBB36_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB36_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB36_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx + 8], xmm0
	movd	dword ptr [rsi + 2*rdx + 12], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB36_6
# %bb.7:
	test	r8b, 1
	je	.LBB36_9
.LBB36_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
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
	ret
.LBB36_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB36_8
	jmp	.LBB36_9
.Lfunc_end36:
	.size	cast_numeric_uint64_int16_sse4, .Lfunc_end36-cast_numeric_uint64_int16_sse4
                                        # -- End function
	.globl	cast_numeric_int64_int16_sse4   # -- Begin function cast_numeric_int64_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_int16_sse4,@function
cast_numeric_int64_int16_sse4:          # @cast_numeric_int64_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB37_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB37_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB37_10
.LBB37_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB37_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	.p2align	4, 0x90
.LBB37_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx + 8], xmm0
	movd	dword ptr [rsi + 2*rdx + 12], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB37_6
# %bb.7:
	test	r8b, 1
	je	.LBB37_9
.LBB37_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
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
	ret
.LBB37_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB37_8
	jmp	.LBB37_9
.Lfunc_end37:
	.size	cast_numeric_int64_int16_sse4, .Lfunc_end37-cast_numeric_int64_int16_sse4
                                        # -- End function
	.globl	cast_numeric_float32_int16_sse4 # -- Begin function cast_numeric_float32_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_int16_sse4,@function
cast_numeric_float32_int16_sse4:        # @cast_numeric_float32_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB38_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB38_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB38_10
.LBB38_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm0, xmm1
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
	movups	xmm0, xmmword ptr [rdi + 4*rdx + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 48]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm0, xmm1
	movdqu	xmmword ptr [rsi + 2*rdx + 16], xmm0
	add	rdx, 16
	add	rax, 2
	jne	.LBB38_6
# %bb.7:
	test	r8b, 1
	je	.LBB38_9
.LBB38_8:
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm0, xmm1
	movdqu	xmmword ptr [rsi + 2*rdx], xmm0
.LBB38_9:
	cmp	rcx, r9
	je	.LBB38_11
	.p2align	4, 0x90
.LBB38_10:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB38_10
.LBB38_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB38_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB38_8
	jmp	.LBB38_9
.Lfunc_end38:
	.size	cast_numeric_float32_int16_sse4, .Lfunc_end38-cast_numeric_float32_int16_sse4
                                        # -- End function
	.globl	cast_numeric_float64_int16_sse4 # -- Begin function cast_numeric_float64_int16_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_int16_sse4,@function
cast_numeric_float64_int16_sse4:        # @cast_numeric_float64_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB39_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB39_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB39_10
.LBB39_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
	movupd	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	cvttpd2dq	xmm0, xmm0
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx + 8], xmm0
	movd	dword ptr [rsi + 2*rdx + 12], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB39_6
# %bb.7:
	test	r8b, 1
	je	.LBB39_9
.LBB39_8:
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rsi + 2*rdx], xmm0
	movd	dword ptr [rsi + 2*rdx + 4], xmm1
.LBB39_9:
	cmp	rcx, r9
	je	.LBB39_11
	.p2align	4, 0x90
.LBB39_10:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
	mov	word ptr [rsi + 2*rcx], ax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB39_10
.LBB39_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB39_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB39_8
	jmp	.LBB39_9
.Lfunc_end39:
	.size	cast_numeric_float64_int16_sse4, .Lfunc_end39-cast_numeric_float64_int16_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_uint32_sse4  # -- Begin function cast_numeric_uint8_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint32_sse4,@function
cast_numeric_uint8_uint32_sse4:         # @cast_numeric_uint8_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB40_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	mov	rax, r9
	and	rax, 3
	je	.LBB40_5
	.p2align	4, 0x90
.LBB40_4:                               # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], edx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovzxbd	xmm0, dword ptr [rdi + rdx]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovzxbd	xmm0, dword ptr [rdi + rdx + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB40_12
# %bb.13:
	test	r8b, 1
	je	.LBB40_15
.LBB40_14:
	pmovzxbd	xmm0, dword ptr [rdi + rdx]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB40_15:
	cmp	rcx, r9
	jne	.LBB40_3
.LBB40_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB40_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB40_14
	jmp	.LBB40_15
.Lfunc_end40:
	.size	cast_numeric_uint8_uint32_sse4, .Lfunc_end40-cast_numeric_uint8_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_int8_uint32_sse4   # -- Begin function cast_numeric_int8_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint32_sse4,@function
cast_numeric_int8_uint32_sse4:          # @cast_numeric_int8_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB41_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	mov	rax, r9
	and	rax, 3
	je	.LBB41_5
	.p2align	4, 0x90
.LBB41_4:                               # =>This Inner Loop Header: Depth=1
	movsx	edx, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], edx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovsxbd	xmm0, dword ptr [rdi + rdx]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovsxbd	xmm0, dword ptr [rdi + rdx + 8]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 12]
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB41_12
# %bb.13:
	test	r8b, 1
	je	.LBB41_15
.LBB41_14:
	pmovsxbd	xmm0, dword ptr [rdi + rdx]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB41_15:
	cmp	rcx, r9
	jne	.LBB41_3
.LBB41_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB41_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB41_14
	jmp	.LBB41_15
.Lfunc_end41:
	.size	cast_numeric_int8_uint32_sse4, .Lfunc_end41-cast_numeric_int8_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_uint32_sse4 # -- Begin function cast_numeric_uint16_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint32_sse4,@function
cast_numeric_uint16_uint32_sse4:        # @cast_numeric_uint16_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB42_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB42_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB42_10
.LBB42_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB42_6
# %bb.7:
	test	r8b, 1
	je	.LBB42_9
.LBB42_8:
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
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
	ret
.LBB42_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB42_8
	jmp	.LBB42_9
.Lfunc_end42:
	.size	cast_numeric_uint16_uint32_sse4, .Lfunc_end42-cast_numeric_uint16_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_int16_uint32_sse4  # -- Begin function cast_numeric_int16_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint32_sse4,@function
cast_numeric_int16_uint32_sse4:         # @cast_numeric_int16_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB43_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB43_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB43_10
.LBB43_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx + 16]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 24]
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB43_6
# %bb.7:
	test	r8b, 1
	je	.LBB43_9
.LBB43_8:
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
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
	ret
.LBB43_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB43_8
	jmp	.LBB43_9
.Lfunc_end43:
	.size	cast_numeric_int16_uint32_sse4, .Lfunc_end43-cast_numeric_int16_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_uint32_uint32_sse4 # -- Begin function cast_numeric_uint32_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint32_sse4,@function
cast_numeric_uint32_uint32_sse4:        # @cast_numeric_uint32_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB44_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 7
	jbe	.LBB44_2
# %bb.7:
	lea	rax, [rdi + 4*r8]
	cmp	rax, rsi
	jbe	.LBB44_9
# %bb.8:
	lea	rax, [rsi + 4*r8]
	cmp	rax, rdi
	jbe	.LBB44_9
.LBB44_2:
	xor	ecx, ecx
.LBB44_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB44_17
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
	cmp	r8, rcx
	jne	.LBB44_6
	jmp	.LBB44_17
.LBB44_9:
	mov	ecx, r8d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdx, rax
	shr	rdx, 3
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 24
	jae	.LBB44_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB44_13
.LBB44_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB44_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 4*rax]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 16]
	movups	xmmword ptr [rsi + 4*rax], xmm0
	movups	xmmword ptr [rsi + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 48]
	movups	xmmword ptr [rsi + 4*rax + 32], xmm0
	movups	xmmword ptr [rsi + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 80]
	movups	xmmword ptr [rsi + 4*rax + 64], xmm0
	movups	xmmword ptr [rsi + 4*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 112]
	movups	xmmword ptr [rsi + 4*rax + 96], xmm0
	movups	xmmword ptr [rsi + 4*rax + 112], xmm1
	add	rax, 32
	add	rdx, 4
	jne	.LBB44_12
.LBB44_13:
	test	r9, r9
	je	.LBB44_16
# %bb.14:
	lea	rax, [4*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB44_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB44_15
.LBB44_16:
	cmp	rcx, r8
	jne	.LBB44_3
.LBB44_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end44:
	.size	cast_numeric_uint32_uint32_sse4, .Lfunc_end44-cast_numeric_uint32_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_int32_uint32_sse4  # -- Begin function cast_numeric_int32_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint32_sse4,@function
cast_numeric_int32_uint32_sse4:         # @cast_numeric_int32_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB45_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 7
	jbe	.LBB45_2
# %bb.7:
	lea	rax, [rdi + 4*r8]
	cmp	rax, rsi
	jbe	.LBB45_9
# %bb.8:
	lea	rax, [rsi + 4*r8]
	cmp	rax, rdi
	jbe	.LBB45_9
.LBB45_2:
	xor	ecx, ecx
.LBB45_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB45_17
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
	cmp	r8, rcx
	jne	.LBB45_6
	jmp	.LBB45_17
.LBB45_9:
	mov	ecx, r8d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdx, rax
	shr	rdx, 3
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 24
	jae	.LBB45_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB45_13
.LBB45_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB45_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 4*rax]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 16]
	movups	xmmword ptr [rsi + 4*rax], xmm0
	movups	xmmword ptr [rsi + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 48]
	movups	xmmword ptr [rsi + 4*rax + 32], xmm0
	movups	xmmword ptr [rsi + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 80]
	movups	xmmword ptr [rsi + 4*rax + 64], xmm0
	movups	xmmword ptr [rsi + 4*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 112]
	movups	xmmword ptr [rsi + 4*rax + 96], xmm0
	movups	xmmword ptr [rsi + 4*rax + 112], xmm1
	add	rax, 32
	add	rdx, 4
	jne	.LBB45_12
.LBB45_13:
	test	r9, r9
	je	.LBB45_16
# %bb.14:
	lea	rax, [4*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB45_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB45_15
.LBB45_16:
	cmp	rcx, r8
	jne	.LBB45_3
.LBB45_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end45:
	.size	cast_numeric_int32_uint32_sse4, .Lfunc_end45-cast_numeric_int32_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_uint64_uint32_sse4 # -- Begin function cast_numeric_uint64_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint32_sse4,@function
cast_numeric_uint64_uint32_sse4:        # @cast_numeric_uint64_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB46_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB46_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB46_10
.LBB46_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm0
	add	rdx, 8
	add	rax, 2
	jne	.LBB46_6
# %bb.7:
	test	r8b, 1
	je	.LBB46_9
.LBB46_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
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
	.size	cast_numeric_uint64_uint32_sse4, .Lfunc_end46-cast_numeric_uint64_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_int64_uint32_sse4  # -- Begin function cast_numeric_int64_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint32_sse4,@function
cast_numeric_int64_uint32_sse4:         # @cast_numeric_int64_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB47_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB47_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB47_10
.LBB47_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm0
	add	rdx, 8
	add	rax, 2
	jne	.LBB47_6
# %bb.7:
	test	r8b, 1
	je	.LBB47_9
.LBB47_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
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
	.size	cast_numeric_int64_uint32_sse4, .Lfunc_end47-cast_numeric_int64_uint32_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_float32_uint32_sse4
.LCPI48_0:
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
.LCPI48_1:
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.text
	.globl	cast_numeric_float32_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint32_sse4,@function
cast_numeric_float32_uint32_sse4:       # @cast_numeric_float32_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB48_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB48_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB48_10
.LBB48_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB48_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movaps	xmm1, xmmword ptr [rip + .LCPI48_0] # xmm1 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	movaps	xmm2, xmmword ptr [rip + .LCPI48_1] # xmm2 = [2147483648,2147483648,2147483648,2147483648]
	.p2align	4, 0x90
.LBB48_6:                               # =>This Inner Loop Header: Depth=1
	movups	xmm3, xmmword ptr [rdi + 4*rdx]
	movups	xmm4, xmmword ptr [rdi + 4*rdx + 16]
	movaps	xmm0, xmm3
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm5, xmm3
	subps	xmm3, xmm1
	cvttps2dq	xmm3, xmm3
	xorps	xmm3, xmm2
	blendvps	xmm3, xmm5, xmm0
	movaps	xmm0, xmm4
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm5, xmm4
	subps	xmm4, xmm1
	cvttps2dq	xmm4, xmm4
	xorps	xmm4, xmm2
	blendvps	xmm4, xmm5, xmm0
	movups	xmmword ptr [rsi + 4*rdx], xmm3
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm4
	movups	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	movaps	xmm0, xmm3
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm4, xmm3
	subps	xmm3, xmm1
	cvttps2dq	xmm3, xmm3
	xorps	xmm3, xmm2
	blendvps	xmm3, xmm4, xmm0
	movups	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	movaps	xmm0, xmm4
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm5, xmm4
	subps	xmm4, xmm1
	cvttps2dq	xmm4, xmm4
	xorps	xmm4, xmm2
	blendvps	xmm4, xmm5, xmm0
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm3
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm4
	add	rdx, 16
	add	rax, 2
	jne	.LBB48_6
# %bb.7:
	test	r8b, 1
	je	.LBB48_9
.LBB48_8:
	movups	xmm1, xmmword ptr [rdi + 4*rdx]
	movups	xmm2, xmmword ptr [rdi + 4*rdx + 16]
	movaps	xmm3, xmmword ptr [rip + .LCPI48_0] # xmm3 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	movaps	xmm0, xmm1
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm4, xmm1
	subps	xmm1, xmm3
	cvttps2dq	xmm1, xmm1
	movaps	xmm5, xmmword ptr [rip + .LCPI48_1] # xmm5 = [2147483648,2147483648,2147483648,2147483648]
	xorps	xmm1, xmm5
	blendvps	xmm1, xmm4, xmm0
	movaps	xmm0, xmm2
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm4, xmm2
	subps	xmm2, xmm3
	cvttps2dq	xmm2, xmm2
	xorps	xmm2, xmm5
	blendvps	xmm2, xmm4, xmm0
	movups	xmmword ptr [rsi + 4*rdx], xmm1
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm2
.LBB48_9:
	cmp	rcx, r9
	je	.LBB48_11
	.p2align	4, 0x90
.LBB48_10:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	rax, dword ptr [rdi + 4*rcx]
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
	.size	cast_numeric_float32_uint32_sse4, .Lfunc_end48-cast_numeric_float32_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_float64_uint32_sse4 # -- Begin function cast_numeric_float64_uint32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint32_sse4,@function
cast_numeric_float64_uint32_sse4:       # @cast_numeric_float64_uint32_sse4
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
	cvttsd2si	rax, qword ptr [rdi + 8*rdx]
	mov	dword ptr [rsi + 4*rdx], eax
	cvttsd2si	rax, qword ptr [rdi + 8*rdx + 8]
	mov	dword ptr [rsi + 4*rdx + 4], eax
	cvttsd2si	rax, qword ptr [rdi + 8*rdx + 16]
	mov	dword ptr [rsi + 4*rdx + 8], eax
	cvttsd2si	rax, qword ptr [rdi + 8*rdx + 24]
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
	cvttsd2si	rdi, qword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB49_5
.LBB49_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end49:
	.size	cast_numeric_float64_uint32_sse4, .Lfunc_end49-cast_numeric_float64_uint32_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_int32_sse4   # -- Begin function cast_numeric_uint8_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int32_sse4,@function
cast_numeric_uint8_int32_sse4:          # @cast_numeric_uint8_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB50_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	mov	rax, r9
	and	rax, 3
	je	.LBB50_5
	.p2align	4, 0x90
.LBB50_4:                               # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], edx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovzxbd	xmm0, dword ptr [rdi + rdx]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovzxbd	xmm0, dword ptr [rdi + rdx + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB50_12
# %bb.13:
	test	r8b, 1
	je	.LBB50_15
.LBB50_14:
	pmovzxbd	xmm0, dword ptr [rdi + rdx]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB50_15:
	cmp	rcx, r9
	jne	.LBB50_3
.LBB50_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB50_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB50_14
	jmp	.LBB50_15
.Lfunc_end50:
	.size	cast_numeric_uint8_int32_sse4, .Lfunc_end50-cast_numeric_uint8_int32_sse4
                                        # -- End function
	.globl	cast_numeric_int8_int32_sse4    # -- Begin function cast_numeric_int8_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_int32_sse4,@function
cast_numeric_int8_int32_sse4:           # @cast_numeric_int8_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB51_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	mov	rax, r9
	and	rax, 3
	je	.LBB51_5
	.p2align	4, 0x90
.LBB51_4:                               # =>This Inner Loop Header: Depth=1
	movsx	edx, byte ptr [rdi + rcx]
	mov	dword ptr [rsi + 4*rcx], edx
	add	rcx, 1
	add	rax, -1
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
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovsxbd	xmm0, dword ptr [rdi + rdx]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovsxbd	xmm0, dword ptr [rdi + rdx + 8]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 12]
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB51_12
# %bb.13:
	test	r8b, 1
	je	.LBB51_15
.LBB51_14:
	pmovsxbd	xmm0, dword ptr [rdi + rdx]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB51_15:
	cmp	rcx, r9
	jne	.LBB51_3
.LBB51_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB51_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB51_14
	jmp	.LBB51_15
.Lfunc_end51:
	.size	cast_numeric_int8_int32_sse4, .Lfunc_end51-cast_numeric_int8_int32_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_int32_sse4  # -- Begin function cast_numeric_uint16_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int32_sse4,@function
cast_numeric_uint16_int32_sse4:         # @cast_numeric_uint16_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB52_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB52_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB52_10
.LBB52_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB52_6
# %bb.7:
	test	r8b, 1
	je	.LBB52_9
.LBB52_8:
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
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
	ret
.LBB52_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB52_8
	jmp	.LBB52_9
.Lfunc_end52:
	.size	cast_numeric_uint16_int32_sse4, .Lfunc_end52-cast_numeric_uint16_int32_sse4
                                        # -- End function
	.globl	cast_numeric_int16_int32_sse4   # -- Begin function cast_numeric_int16_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_int32_sse4,@function
cast_numeric_int16_int32_sse4:          # @cast_numeric_int16_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB53_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB53_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB53_10
.LBB53_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx + 16]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 24]
	movdqu	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB53_6
# %bb.7:
	test	r8b, 1
	je	.LBB53_9
.LBB53_8:
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm1
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
	ret
.LBB53_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB53_8
	jmp	.LBB53_9
.Lfunc_end53:
	.size	cast_numeric_int16_int32_sse4, .Lfunc_end53-cast_numeric_int16_int32_sse4
                                        # -- End function
	.globl	cast_numeric_uint32_int32_sse4  # -- Begin function cast_numeric_uint32_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int32_sse4,@function
cast_numeric_uint32_int32_sse4:         # @cast_numeric_uint32_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB54_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 7
	jbe	.LBB54_2
# %bb.7:
	lea	rax, [rdi + 4*r8]
	cmp	rax, rsi
	jbe	.LBB54_9
# %bb.8:
	lea	rax, [rsi + 4*r8]
	cmp	rax, rdi
	jbe	.LBB54_9
.LBB54_2:
	xor	ecx, ecx
.LBB54_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB54_17
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
	cmp	r8, rcx
	jne	.LBB54_6
	jmp	.LBB54_17
.LBB54_9:
	mov	ecx, r8d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdx, rax
	shr	rdx, 3
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 24
	jae	.LBB54_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB54_13
.LBB54_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB54_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 4*rax]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 16]
	movups	xmmword ptr [rsi + 4*rax], xmm0
	movups	xmmword ptr [rsi + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 48]
	movups	xmmword ptr [rsi + 4*rax + 32], xmm0
	movups	xmmword ptr [rsi + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 80]
	movups	xmmword ptr [rsi + 4*rax + 64], xmm0
	movups	xmmword ptr [rsi + 4*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 112]
	movups	xmmword ptr [rsi + 4*rax + 96], xmm0
	movups	xmmword ptr [rsi + 4*rax + 112], xmm1
	add	rax, 32
	add	rdx, 4
	jne	.LBB54_12
.LBB54_13:
	test	r9, r9
	je	.LBB54_16
# %bb.14:
	lea	rax, [4*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB54_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB54_15
.LBB54_16:
	cmp	rcx, r8
	jne	.LBB54_3
.LBB54_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end54:
	.size	cast_numeric_uint32_int32_sse4, .Lfunc_end54-cast_numeric_uint32_int32_sse4
                                        # -- End function
	.globl	cast_numeric_int32_int32_sse4   # -- Begin function cast_numeric_int32_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_int32_sse4,@function
cast_numeric_int32_int32_sse4:          # @cast_numeric_int32_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB55_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 7
	jbe	.LBB55_2
# %bb.7:
	lea	rax, [rdi + 4*r8]
	cmp	rax, rsi
	jbe	.LBB55_9
# %bb.8:
	lea	rax, [rsi + 4*r8]
	cmp	rax, rdi
	jbe	.LBB55_9
.LBB55_2:
	xor	ecx, ecx
.LBB55_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB55_17
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
	cmp	r8, rcx
	jne	.LBB55_6
	jmp	.LBB55_17
.LBB55_9:
	mov	ecx, r8d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdx, rax
	shr	rdx, 3
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 24
	jae	.LBB55_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB55_13
.LBB55_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB55_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 4*rax]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 16]
	movups	xmmword ptr [rsi + 4*rax], xmm0
	movups	xmmword ptr [rsi + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 48]
	movups	xmmword ptr [rsi + 4*rax + 32], xmm0
	movups	xmmword ptr [rsi + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 80]
	movups	xmmword ptr [rsi + 4*rax + 64], xmm0
	movups	xmmword ptr [rsi + 4*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 112]
	movups	xmmword ptr [rsi + 4*rax + 96], xmm0
	movups	xmmword ptr [rsi + 4*rax + 112], xmm1
	add	rax, 32
	add	rdx, 4
	jne	.LBB55_12
.LBB55_13:
	test	r9, r9
	je	.LBB55_16
# %bb.14:
	lea	rax, [4*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB55_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB55_15
.LBB55_16:
	cmp	rcx, r8
	jne	.LBB55_3
.LBB55_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end55:
	.size	cast_numeric_int32_int32_sse4, .Lfunc_end55-cast_numeric_int32_int32_sse4
                                        # -- End function
	.globl	cast_numeric_uint64_int32_sse4  # -- Begin function cast_numeric_uint64_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int32_sse4,@function
cast_numeric_uint64_int32_sse4:         # @cast_numeric_uint64_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB56_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB56_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB56_10
.LBB56_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm0
	add	rdx, 8
	add	rax, 2
	jne	.LBB56_6
# %bb.7:
	test	r8b, 1
	je	.LBB56_9
.LBB56_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
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
	.size	cast_numeric_uint64_int32_sse4, .Lfunc_end56-cast_numeric_uint64_int32_sse4
                                        # -- End function
	.globl	cast_numeric_int64_int32_sse4   # -- Begin function cast_numeric_int64_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_int32_sse4,@function
cast_numeric_int64_int32_sse4:          # @cast_numeric_int64_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB57_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB57_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB57_10
.LBB57_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx + 16], xmm0
	add	rdx, 8
	add	rax, 2
	jne	.LBB57_6
# %bb.7:
	test	r8b, 1
	je	.LBB57_9
.LBB57_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rsi + 4*rdx], xmm0
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
	.size	cast_numeric_int64_int32_sse4, .Lfunc_end57-cast_numeric_int64_int32_sse4
                                        # -- End function
	.globl	cast_numeric_float32_int32_sse4 # -- Begin function cast_numeric_float32_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_int32_sse4,@function
cast_numeric_float32_int32_sse4:        # @cast_numeric_float32_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB58_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB58_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB58_10
.LBB58_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rdx + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 48]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB58_6
# %bb.7:
	test	r8b, 1
	je	.LBB58_9
.LBB58_8:
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB58_9:
	cmp	rcx, r9
	je	.LBB58_11
	.p2align	4, 0x90
.LBB58_10:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdi + 4*rcx]
	mov	dword ptr [rsi + 4*rcx], eax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB58_10
.LBB58_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB58_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB58_8
	jmp	.LBB58_9
.Lfunc_end58:
	.size	cast_numeric_float32_int32_sse4, .Lfunc_end58-cast_numeric_float32_int32_sse4
                                        # -- End function
	.globl	cast_numeric_float64_int32_sse4 # -- Begin function cast_numeric_float64_int32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_int32_sse4,@function
cast_numeric_float64_int32_sse4:        # @cast_numeric_float64_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB59_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB59_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB59_10
.LBB59_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rsi + 4*rdx], xmm0
	movupd	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rsi + 4*rdx + 16], xmm0
	add	rdx, 8
	add	rax, 2
	jne	.LBB59_6
# %bb.7:
	test	r8b, 1
	je	.LBB59_9
.LBB59_8:
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rsi + 4*rdx], xmm0
.LBB59_9:
	cmp	rcx, r9
	je	.LBB59_11
	.p2align	4, 0x90
.LBB59_10:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdi + 8*rcx]
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
	.size	cast_numeric_float64_int32_sse4, .Lfunc_end59-cast_numeric_float64_int32_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_uint64_sse4  # -- Begin function cast_numeric_uint8_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_uint64_sse4,@function
cast_numeric_uint8_uint64_sse4:         # @cast_numeric_uint8_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB60_17
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	jb	.LBB60_17
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
	jmp	.LBB60_17
.LBB60_9:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB60_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB60_13
.LBB60_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB60_12:                              # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [rdi + rax]      # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 2]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovzxbq	xmm0, word ptr [rdi + rax + 4]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 6]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovzxbq	xmm0, word ptr [rdi + rax + 8]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 10] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovzxbq	xmm0, word ptr [rdi + rax + 12] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 14] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB60_12
.LBB60_13:
	test	r8, r8
	je	.LBB60_16
# %bb.14:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + rax]
	add	r10, 2
	xor	eax, eax
	.p2align	4, 0x90
.LBB60_15:                              # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [r10 + 4*rax - 2] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [r10 + 4*rax]    # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB60_15
.LBB60_16:
	cmp	rcx, r9
	jne	.LBB60_3
.LBB60_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end60:
	.size	cast_numeric_uint8_uint64_sse4, .Lfunc_end60-cast_numeric_uint8_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_int8_uint64_sse4   # -- Begin function cast_numeric_int8_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_uint64_sse4,@function
cast_numeric_int8_uint64_sse4:          # @cast_numeric_int8_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB61_17
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	jb	.LBB61_17
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
	jmp	.LBB61_17
.LBB61_9:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB61_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB61_13
.LBB61_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB61_12:                              # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [rdi + rax]
	pmovsxbq	xmm1, word ptr [rdi + rax + 2]
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovsxbq	xmm0, word ptr [rdi + rax + 4]
	pmovsxbq	xmm1, word ptr [rdi + rax + 6]
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovsxbq	xmm0, word ptr [rdi + rax + 8]
	pmovsxbq	xmm1, word ptr [rdi + rax + 10]
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovsxbq	xmm0, word ptr [rdi + rax + 12]
	pmovsxbq	xmm1, word ptr [rdi + rax + 14]
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB61_12
.LBB61_13:
	test	r8, r8
	je	.LBB61_16
# %bb.14:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + rax]
	add	r10, 2
	xor	eax, eax
	.p2align	4, 0x90
.LBB61_15:                              # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [r10 + 4*rax - 2]
	pmovsxbq	xmm1, word ptr [r10 + 4*rax]
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB61_15
.LBB61_16:
	cmp	rcx, r9
	jne	.LBB61_3
.LBB61_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end61:
	.size	cast_numeric_int8_uint64_sse4, .Lfunc_end61-cast_numeric_int8_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_uint64_sse4 # -- Begin function cast_numeric_uint16_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_uint64_sse4,@function
cast_numeric_uint16_uint64_sse4:        # @cast_numeric_uint16_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB62_12
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB62_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB62_11
.LBB62_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB62_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB62_7
.LBB62_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB62_6:                               # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax]   # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax + 16] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 20] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax + 24] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 28] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB62_6
.LBB62_7:
	test	r8, r8
	je	.LBB62_10
# %bb.8:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + 2*rax]
	add	r10, 4
	xor	eax, eax
	.p2align	4, 0x90
.LBB62_9:                               # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [r10 + 8*rax - 4] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [r10 + 8*rax]   # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB62_9
.LBB62_10:
	cmp	rcx, r9
	je	.LBB62_12
	.p2align	4, 0x90
.LBB62_11:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB62_11
.LBB62_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end62:
	.size	cast_numeric_uint16_uint64_sse4, .Lfunc_end62-cast_numeric_uint16_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_int16_uint64_sse4  # -- Begin function cast_numeric_int16_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_uint64_sse4,@function
cast_numeric_int16_uint64_sse4:         # @cast_numeric_int16_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB63_12
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB63_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB63_11
.LBB63_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB63_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB63_7
.LBB63_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB63_6:                               # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 4]
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax + 8]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 12]
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax + 16]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 20]
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax + 24]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 28]
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB63_6
.LBB63_7:
	test	r8, r8
	je	.LBB63_10
# %bb.8:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + 2*rax]
	add	r10, 4
	xor	eax, eax
	.p2align	4, 0x90
.LBB63_9:                               # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [r10 + 8*rax - 4]
	pmovsxwq	xmm1, dword ptr [r10 + 8*rax]
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB63_9
.LBB63_10:
	cmp	rcx, r9
	je	.LBB63_12
	.p2align	4, 0x90
.LBB63_11:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB63_11
.LBB63_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end63:
	.size	cast_numeric_int16_uint64_sse4, .Lfunc_end63-cast_numeric_int16_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_uint32_uint64_sse4 # -- Begin function cast_numeric_uint32_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_uint64_sse4,@function
cast_numeric_uint32_uint64_sse4:        # @cast_numeric_uint32_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB64_12
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	ja	.LBB64_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB64_11
.LBB64_3:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB64_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB64_7
.LBB64_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB64_6:                               # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax]   # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 8] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax + 16] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 24] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax + 32] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 40] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax + 48] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 56] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB64_6
.LBB64_7:
	test	r9, r9
	je	.LBB64_10
# %bb.8:
	lea	rax, [4*rax + 8]
	neg	r9
	.p2align	4, 0x90
.LBB64_9:                               # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdi + rax - 8] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + rax]     # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rsi + 2*rax], xmm1
	add	rax, 16
	inc	r9
	jne	.LBB64_9
.LBB64_10:
	cmp	rcx, r8
	je	.LBB64_12
	.p2align	4, 0x90
.LBB64_11:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r8, rcx
	jne	.LBB64_11
.LBB64_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end64:
	.size	cast_numeric_uint32_uint64_sse4, .Lfunc_end64-cast_numeric_uint32_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_int32_uint64_sse4  # -- Begin function cast_numeric_int32_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_uint64_sse4,@function
cast_numeric_int32_uint64_sse4:         # @cast_numeric_int32_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB65_12
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	ja	.LBB65_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB65_11
.LBB65_3:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB65_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB65_7
.LBB65_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB65_6:                               # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 8]
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax + 16]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 24]
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax + 32]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 40]
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax + 48]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 56]
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB65_6
.LBB65_7:
	test	r9, r9
	je	.LBB65_10
# %bb.8:
	lea	rax, [4*rax + 8]
	neg	r9
	.p2align	4, 0x90
.LBB65_9:                               # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdi + rax - 8]
	pmovsxdq	xmm1, qword ptr [rdi + rax]
	movdqu	xmmword ptr [rsi + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rsi + 2*rax], xmm1
	add	rax, 16
	inc	r9
	jne	.LBB65_9
.LBB65_10:
	cmp	rcx, r8
	je	.LBB65_12
	.p2align	4, 0x90
.LBB65_11:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r8, rcx
	jne	.LBB65_11
.LBB65_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end65:
	.size	cast_numeric_int32_uint64_sse4, .Lfunc_end65-cast_numeric_int32_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_uint64_uint64_sse4 # -- Begin function cast_numeric_uint64_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_uint64_sse4,@function
cast_numeric_uint64_uint64_sse4:        # @cast_numeric_uint64_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB66_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	jbe	.LBB66_2
# %bb.7:
	lea	rax, [rdi + 8*r8]
	cmp	rax, rsi
	jbe	.LBB66_9
# %bb.8:
	lea	rax, [rsi + 8*r8]
	cmp	rax, rdi
	jbe	.LBB66_9
.LBB66_2:
	xor	ecx, ecx
.LBB66_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB66_17
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
	cmp	r8, rcx
	jne	.LBB66_6
	jmp	.LBB66_17
.LBB66_9:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB66_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB66_13
.LBB66_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB66_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 8*rax]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movups	xmmword ptr [rsi + 8*rax], xmm0
	movups	xmmword ptr [rsi + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 48]
	movups	xmmword ptr [rsi + 8*rax + 32], xmm0
	movups	xmmword ptr [rsi + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 80]
	movups	xmmword ptr [rsi + 8*rax + 64], xmm0
	movups	xmmword ptr [rsi + 8*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 112]
	movups	xmmword ptr [rsi + 8*rax + 96], xmm0
	movups	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB66_12
.LBB66_13:
	test	r9, r9
	je	.LBB66_16
# %bb.14:
	lea	rax, [8*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB66_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB66_15
.LBB66_16:
	cmp	rcx, r8
	jne	.LBB66_3
.LBB66_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end66:
	.size	cast_numeric_uint64_uint64_sse4, .Lfunc_end66-cast_numeric_uint64_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_int64_uint64_sse4  # -- Begin function cast_numeric_int64_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_uint64_sse4,@function
cast_numeric_int64_uint64_sse4:         # @cast_numeric_int64_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB67_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	jbe	.LBB67_2
# %bb.7:
	lea	rax, [rdi + 8*r8]
	cmp	rax, rsi
	jbe	.LBB67_9
# %bb.8:
	lea	rax, [rsi + 8*r8]
	cmp	rax, rdi
	jbe	.LBB67_9
.LBB67_2:
	xor	ecx, ecx
.LBB67_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB67_17
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
	cmp	r8, rcx
	jne	.LBB67_6
	jmp	.LBB67_17
.LBB67_9:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB67_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB67_13
.LBB67_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB67_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 8*rax]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movups	xmmword ptr [rsi + 8*rax], xmm0
	movups	xmmword ptr [rsi + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 48]
	movups	xmmword ptr [rsi + 8*rax + 32], xmm0
	movups	xmmword ptr [rsi + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 80]
	movups	xmmword ptr [rsi + 8*rax + 64], xmm0
	movups	xmmword ptr [rsi + 8*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 112]
	movups	xmmword ptr [rsi + 8*rax + 96], xmm0
	movups	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB67_12
.LBB67_13:
	test	r9, r9
	je	.LBB67_16
# %bb.14:
	lea	rax, [8*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB67_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB67_15
.LBB67_16:
	cmp	rcx, r8
	jne	.LBB67_3
.LBB67_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end67:
	.size	cast_numeric_int64_uint64_sse4, .Lfunc_end67-cast_numeric_int64_uint64_sse4
                                        # -- End function
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2                               # -- Begin function cast_numeric_float32_uint64_sse4
.LCPI68_0:
	.long	0x5f000000                      # float 9.22337203E+18
	.text
	.globl	cast_numeric_float32_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_uint64_sse4,@function
cast_numeric_float32_uint64_sse4:       # @cast_numeric_float32_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB68_6
# %bb.1:
	mov	r10d, edx
	lea	rax, [r10 - 1]
	mov	r8d, r10d
	and	r8d, 3
	movabs	r9, -9223372036854775808
	cmp	rax, 3
	jae	.LBB68_7
# %bb.2:
	xor	eax, eax
	jmp	.LBB68_3
.LBB68_7:
	and	r10d, -4
	xor	eax, eax
	movss	xmm0, dword ptr [rip + .LCPI68_0] # xmm0 = mem[0],zero,zero,zero
	.p2align	4, 0x90
.LBB68_8:                               # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdi + 4*rax]   # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rcx, xmm2
	xor	rcx, r9
	cvttss2si	rdx, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax], rdx
	movss	xmm1, dword ptr [rdi + 4*rax + 4] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rcx, xmm2
	xor	rcx, r9
	cvttss2si	rdx, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 8], rdx
	movss	xmm1, dword ptr [rdi + 4*rax + 8] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rcx, xmm2
	xor	rcx, r9
	cvttss2si	rdx, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 16], rdx
	movss	xmm1, dword ptr [rdi + 4*rax + 12] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rcx, xmm2
	xor	rcx, r9
	cvttss2si	rdx, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 24], rdx
	add	rax, 4
	cmp	r10, rax
	jne	.LBB68_8
.LBB68_3:
	test	r8, r8
	je	.LBB68_6
# %bb.4:
	lea	rdx, [rsi + 8*rax]
	lea	rax, [rdi + 4*rax]
	xor	esi, esi
	movss	xmm0, dword ptr [rip + .LCPI68_0] # xmm0 = mem[0],zero,zero,zero
	.p2align	4, 0x90
.LBB68_5:                               # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rax + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rdi, xmm2
	xor	rdi, r9
	cvttss2si	rcx, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rcx, rdi
	mov	qword ptr [rdx + 8*rsi], rcx
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB68_5
.LBB68_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end68:
	.size	cast_numeric_float32_uint64_sse4, .Lfunc_end68-cast_numeric_float32_uint64_sse4
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_numeric_float64_uint64_sse4
.LCPI69_0:
	.quad	0x43e0000000000000              # double 9.2233720368547758E+18
	.text
	.globl	cast_numeric_float64_uint64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_uint64_sse4,@function
cast_numeric_float64_uint64_sse4:       # @cast_numeric_float64_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB69_6
# %bb.1:
	mov	r10d, edx
	lea	rax, [r10 - 1]
	mov	r8d, r10d
	and	r8d, 3
	movabs	r9, -9223372036854775808
	cmp	rax, 3
	jae	.LBB69_7
# %bb.2:
	xor	eax, eax
	jmp	.LBB69_3
.LBB69_7:
	and	r10d, -4
	xor	eax, eax
	movsd	xmm0, qword ptr [rip + .LCPI69_0] # xmm0 = mem[0],zero
	.p2align	4, 0x90
.LBB69_8:                               # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdi + 8*rax]   # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rcx, xmm2
	xor	rcx, r9
	cvttsd2si	rdx, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax], rdx
	movsd	xmm1, qword ptr [rdi + 8*rax + 8] # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rcx, xmm2
	xor	rcx, r9
	cvttsd2si	rdx, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 8], rdx
	movsd	xmm1, qword ptr [rdi + 8*rax + 16] # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rcx, xmm2
	xor	rcx, r9
	cvttsd2si	rdx, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 16], rdx
	movsd	xmm1, qword ptr [rdi + 8*rax + 24] # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rcx, xmm2
	xor	rcx, r9
	cvttsd2si	rdx, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdx, rcx
	mov	qword ptr [rsi + 8*rax + 24], rdx
	add	rax, 4
	cmp	r10, rax
	jne	.LBB69_8
.LBB69_3:
	test	r8, r8
	je	.LBB69_6
# %bb.4:
	lea	rdx, [rsi + 8*rax]
	lea	rax, [rdi + 8*rax]
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI69_0] # xmm0 = mem[0],zero
	.p2align	4, 0x90
.LBB69_5:                               # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rax + 8*rsi]   # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rdi, xmm2
	xor	rdi, r9
	cvttsd2si	rcx, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rcx, rdi
	mov	qword ptr [rdx + 8*rsi], rcx
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB69_5
.LBB69_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end69:
	.size	cast_numeric_float64_uint64_sse4, .Lfunc_end69-cast_numeric_float64_uint64_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_int64_sse4   # -- Begin function cast_numeric_uint8_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_int64_sse4,@function
cast_numeric_uint8_int64_sse4:          # @cast_numeric_uint8_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB70_17
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	jb	.LBB70_17
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
	jmp	.LBB70_17
.LBB70_9:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB70_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB70_13
.LBB70_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB70_12:                              # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [rdi + rax]      # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 2]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovzxbq	xmm0, word ptr [rdi + rax + 4]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 6]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovzxbq	xmm0, word ptr [rdi + rax + 8]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 10] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovzxbq	xmm0, word ptr [rdi + rax + 12] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdi + rax + 14] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB70_12
.LBB70_13:
	test	r8, r8
	je	.LBB70_16
# %bb.14:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + rax]
	add	r10, 2
	xor	eax, eax
	.p2align	4, 0x90
.LBB70_15:                              # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [r10 + 4*rax - 2] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [r10 + 4*rax]    # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB70_15
.LBB70_16:
	cmp	rcx, r9
	jne	.LBB70_3
.LBB70_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end70:
	.size	cast_numeric_uint8_int64_sse4, .Lfunc_end70-cast_numeric_uint8_int64_sse4
                                        # -- End function
	.globl	cast_numeric_int8_int64_sse4    # -- Begin function cast_numeric_int8_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_int64_sse4,@function
cast_numeric_int8_int64_sse4:           # @cast_numeric_int8_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB71_17
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
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
	jb	.LBB71_17
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
	jmp	.LBB71_17
.LBB71_9:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB71_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB71_13
.LBB71_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB71_12:                              # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [rdi + rax]
	pmovsxbq	xmm1, word ptr [rdi + rax + 2]
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovsxbq	xmm0, word ptr [rdi + rax + 4]
	pmovsxbq	xmm1, word ptr [rdi + rax + 6]
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovsxbq	xmm0, word ptr [rdi + rax + 8]
	pmovsxbq	xmm1, word ptr [rdi + rax + 10]
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovsxbq	xmm0, word ptr [rdi + rax + 12]
	pmovsxbq	xmm1, word ptr [rdi + rax + 14]
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB71_12
.LBB71_13:
	test	r8, r8
	je	.LBB71_16
# %bb.14:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + rax]
	add	r10, 2
	xor	eax, eax
	.p2align	4, 0x90
.LBB71_15:                              # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [r10 + 4*rax - 2]
	pmovsxbq	xmm1, word ptr [r10 + 4*rax]
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB71_15
.LBB71_16:
	cmp	rcx, r9
	jne	.LBB71_3
.LBB71_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end71:
	.size	cast_numeric_int8_int64_sse4, .Lfunc_end71-cast_numeric_int8_int64_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_int64_sse4  # -- Begin function cast_numeric_uint16_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_int64_sse4,@function
cast_numeric_uint16_int64_sse4:         # @cast_numeric_uint16_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB72_12
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB72_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB72_11
.LBB72_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB72_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB72_7
.LBB72_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB72_6:                               # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax]   # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax + 16] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 20] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovzxwq	xmm0, dword ptr [rdi + 2*rax + 24] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdi + 2*rax + 28] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB72_6
.LBB72_7:
	test	r8, r8
	je	.LBB72_10
# %bb.8:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + 2*rax]
	add	r10, 4
	xor	eax, eax
	.p2align	4, 0x90
.LBB72_9:                               # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [r10 + 8*rax - 4] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [r10 + 8*rax]   # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB72_9
.LBB72_10:
	cmp	rcx, r9
	je	.LBB72_12
	.p2align	4, 0x90
.LBB72_11:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB72_11
.LBB72_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end72:
	.size	cast_numeric_uint16_int64_sse4, .Lfunc_end72-cast_numeric_uint16_int64_sse4
                                        # -- End function
	.globl	cast_numeric_int16_int64_sse4   # -- Begin function cast_numeric_int16_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_int64_sse4,@function
cast_numeric_int16_int64_sse4:          # @cast_numeric_int16_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB73_12
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB73_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB73_11
.LBB73_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r8d, edx
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB73_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB73_7
.LBB73_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB73_6:                               # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 4]
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax + 8]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 12]
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax + 16]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 20]
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovsxwq	xmm0, dword ptr [rdi + 2*rax + 24]
	pmovsxwq	xmm1, dword ptr [rdi + 2*rax + 28]
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB73_6
.LBB73_7:
	test	r8, r8
	je	.LBB73_10
# %bb.8:
	lea	rdx, [rsi + 8*rax]
	add	rdx, 16
	lea	r10, [rdi + 2*rax]
	add	r10, 4
	xor	eax, eax
	.p2align	4, 0x90
.LBB73_9:                               # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [r10 + 8*rax - 4]
	pmovsxwq	xmm1, dword ptr [r10 + 8*rax]
	movdqu	xmmword ptr [rdx - 16], xmm0
	movdqu	xmmword ptr [rdx], xmm1
	add	rdx, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB73_9
.LBB73_10:
	cmp	rcx, r9
	je	.LBB73_12
	.p2align	4, 0x90
.LBB73_11:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + 2*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB73_11
.LBB73_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end73:
	.size	cast_numeric_int16_int64_sse4, .Lfunc_end73-cast_numeric_int16_int64_sse4
                                        # -- End function
	.globl	cast_numeric_uint32_int64_sse4  # -- Begin function cast_numeric_uint32_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_int64_sse4,@function
cast_numeric_uint32_int64_sse4:         # @cast_numeric_uint32_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB74_12
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	ja	.LBB74_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB74_11
.LBB74_3:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB74_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB74_7
.LBB74_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB74_6:                               # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax]   # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 8] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax + 16] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 24] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax + 32] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 40] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovzxdq	xmm0, qword ptr [rdi + 4*rax + 48] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + 4*rax + 56] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB74_6
.LBB74_7:
	test	r9, r9
	je	.LBB74_10
# %bb.8:
	lea	rax, [4*rax + 8]
	neg	r9
	.p2align	4, 0x90
.LBB74_9:                               # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdi + rax - 8] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdi + rax]     # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rsi + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rsi + 2*rax], xmm1
	add	rax, 16
	inc	r9
	jne	.LBB74_9
.LBB74_10:
	cmp	rcx, r8
	je	.LBB74_12
	.p2align	4, 0x90
.LBB74_11:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r8, rcx
	jne	.LBB74_11
.LBB74_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end74:
	.size	cast_numeric_uint32_int64_sse4, .Lfunc_end74-cast_numeric_uint32_int64_sse4
                                        # -- End function
	.globl	cast_numeric_int32_int64_sse4   # -- Begin function cast_numeric_int32_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_int64_sse4,@function
cast_numeric_int32_int64_sse4:          # @cast_numeric_int32_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB75_12
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	ja	.LBB75_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB75_11
.LBB75_3:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB75_5
# %bb.4:
	xor	eax, eax
	jmp	.LBB75_7
.LBB75_5:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB75_6:                               # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 8]
	movdqu	xmmword ptr [rsi + 8*rax], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 16], xmm1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax + 16]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 24]
	movdqu	xmmword ptr [rsi + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 48], xmm1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax + 32]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 40]
	movdqu	xmmword ptr [rsi + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 80], xmm1
	pmovsxdq	xmm0, qword ptr [rdi + 4*rax + 48]
	pmovsxdq	xmm1, qword ptr [rdi + 4*rax + 56]
	movdqu	xmmword ptr [rsi + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB75_6
.LBB75_7:
	test	r9, r9
	je	.LBB75_10
# %bb.8:
	lea	rax, [4*rax + 8]
	neg	r9
	.p2align	4, 0x90
.LBB75_9:                               # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdi + rax - 8]
	pmovsxdq	xmm1, qword ptr [rdi + rax]
	movdqu	xmmword ptr [rsi + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rsi + 2*rax], xmm1
	add	rax, 16
	inc	r9
	jne	.LBB75_9
.LBB75_10:
	cmp	rcx, r8
	je	.LBB75_12
	.p2align	4, 0x90
.LBB75_11:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*rcx]
	mov	qword ptr [rsi + 8*rcx], rax
	add	rcx, 1
	cmp	r8, rcx
	jne	.LBB75_11
.LBB75_12:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end75:
	.size	cast_numeric_int32_int64_sse4, .Lfunc_end75-cast_numeric_int32_int64_sse4
                                        # -- End function
	.globl	cast_numeric_uint64_int64_sse4  # -- Begin function cast_numeric_uint64_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_int64_sse4,@function
cast_numeric_uint64_int64_sse4:         # @cast_numeric_uint64_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB76_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	jbe	.LBB76_2
# %bb.7:
	lea	rax, [rdi + 8*r8]
	cmp	rax, rsi
	jbe	.LBB76_9
# %bb.8:
	lea	rax, [rsi + 8*r8]
	cmp	rax, rdi
	jbe	.LBB76_9
.LBB76_2:
	xor	ecx, ecx
.LBB76_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB76_17
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
	cmp	r8, rcx
	jne	.LBB76_6
	jmp	.LBB76_17
.LBB76_9:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB76_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB76_13
.LBB76_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB76_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 8*rax]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movups	xmmword ptr [rsi + 8*rax], xmm0
	movups	xmmword ptr [rsi + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 48]
	movups	xmmword ptr [rsi + 8*rax + 32], xmm0
	movups	xmmword ptr [rsi + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 80]
	movups	xmmword ptr [rsi + 8*rax + 64], xmm0
	movups	xmmword ptr [rsi + 8*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 112]
	movups	xmmword ptr [rsi + 8*rax + 96], xmm0
	movups	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB76_12
.LBB76_13:
	test	r9, r9
	je	.LBB76_16
# %bb.14:
	lea	rax, [8*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB76_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB76_15
.LBB76_16:
	cmp	rcx, r8
	jne	.LBB76_3
.LBB76_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end76:
	.size	cast_numeric_uint64_int64_sse4, .Lfunc_end76-cast_numeric_uint64_int64_sse4
                                        # -- End function
	.globl	cast_numeric_int64_int64_sse4   # -- Begin function cast_numeric_int64_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_int64_sse4,@function
cast_numeric_int64_int64_sse4:          # @cast_numeric_int64_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB77_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	jbe	.LBB77_2
# %bb.7:
	lea	rax, [rdi + 8*r8]
	cmp	rax, rsi
	jbe	.LBB77_9
# %bb.8:
	lea	rax, [rsi + 8*r8]
	cmp	rax, rdi
	jbe	.LBB77_9
.LBB77_2:
	xor	ecx, ecx
.LBB77_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 3
	jb	.LBB77_17
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
	cmp	r8, rcx
	jne	.LBB77_6
	jmp	.LBB77_17
.LBB77_9:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB77_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB77_13
.LBB77_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB77_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 8*rax]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movups	xmmword ptr [rsi + 8*rax], xmm0
	movups	xmmword ptr [rsi + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 48]
	movups	xmmword ptr [rsi + 8*rax + 32], xmm0
	movups	xmmword ptr [rsi + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 80]
	movups	xmmword ptr [rsi + 8*rax + 64], xmm0
	movups	xmmword ptr [rsi + 8*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 112]
	movups	xmmword ptr [rsi + 8*rax + 96], xmm0
	movups	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB77_12
.LBB77_13:
	test	r9, r9
	je	.LBB77_16
# %bb.14:
	lea	rax, [8*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB77_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB77_15
.LBB77_16:
	cmp	rcx, r8
	jne	.LBB77_3
.LBB77_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end77:
	.size	cast_numeric_int64_int64_sse4, .Lfunc_end77-cast_numeric_int64_int64_sse4
                                        # -- End function
	.globl	cast_numeric_float32_int64_sse4 # -- Begin function cast_numeric_float32_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_int64_sse4,@function
cast_numeric_float32_int64_sse4:        # @cast_numeric_float32_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB78_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB78_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB78_3
.LBB78_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB78_8:                               # =>This Inner Loop Header: Depth=1
	cvttss2si	rax, dword ptr [rdi + 4*rdx]
	mov	qword ptr [rsi + 8*rdx], rax
	cvttss2si	rax, dword ptr [rdi + 4*rdx + 4]
	mov	qword ptr [rsi + 8*rdx + 8], rax
	cvttss2si	rax, dword ptr [rdi + 4*rdx + 8]
	mov	qword ptr [rsi + 8*rdx + 16], rax
	cvttss2si	rax, dword ptr [rdi + 4*rdx + 12]
	mov	qword ptr [rsi + 8*rdx + 24], rax
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB78_8
.LBB78_3:
	test	r8, r8
	je	.LBB78_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 4*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB78_5:                               # =>This Inner Loop Header: Depth=1
	cvttss2si	rdi, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB78_5
.LBB78_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end78:
	.size	cast_numeric_float32_int64_sse4, .Lfunc_end78-cast_numeric_float32_int64_sse4
                                        # -- End function
	.globl	cast_numeric_float64_int64_sse4 # -- Begin function cast_numeric_float64_int64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_int64_sse4,@function
cast_numeric_float64_int64_sse4:        # @cast_numeric_float64_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB79_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB79_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB79_3
.LBB79_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB79_8:                               # =>This Inner Loop Header: Depth=1
	cvttsd2si	rax, qword ptr [rdi + 8*rdx]
	mov	qword ptr [rsi + 8*rdx], rax
	cvttsd2si	rax, qword ptr [rdi + 8*rdx + 8]
	mov	qword ptr [rsi + 8*rdx + 8], rax
	cvttsd2si	rax, qword ptr [rdi + 8*rdx + 16]
	mov	qword ptr [rsi + 8*rdx + 16], rax
	cvttsd2si	rax, qword ptr [rdi + 8*rdx + 24]
	mov	qword ptr [rsi + 8*rdx + 24], rax
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB79_8
.LBB79_3:
	test	r8, r8
	je	.LBB79_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 8*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB79_5:                               # =>This Inner Loop Header: Depth=1
	cvttsd2si	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB79_5
.LBB79_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end79:
	.size	cast_numeric_float64_int64_sse4, .Lfunc_end79-cast_numeric_float64_int64_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_float32_sse4 # -- Begin function cast_numeric_uint8_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_float32_sse4,@function
cast_numeric_uint8_float32_sse4:        # @cast_numeric_uint8_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB80_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	add	rdx, -1
	jne	.LBB80_4
.LBB80_5:
	cmp	r8, 3
	jb	.LBB80_16
	.p2align	4, 0x90
.LBB80_6:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rcx]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx], xmm0
	movzx	eax, byte ptr [rdi + rcx + 1]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx + 4], xmm0
	movzx	eax, byte ptr [rdi + rcx + 2]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx + 8], xmm0
	movzx	eax, byte ptr [rdi + rcx + 3]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx + 12], xmm0
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB80_6
	jmp	.LBB80_16
.LBB80_9:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovzxbd	xmm0, dword ptr [rdi + rdx]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovzxbd	xmm0, dword ptr [rdi + rdx + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB80_12
# %bb.13:
	test	r8b, 1
	je	.LBB80_15
.LBB80_14:
	pmovzxbd	xmm0, dword ptr [rdi + rdx]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdi + rdx + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB80_15:
	cmp	rcx, r9
	jne	.LBB80_3
.LBB80_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB80_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB80_14
	jmp	.LBB80_15
.Lfunc_end80:
	.size	cast_numeric_uint8_float32_sse4, .Lfunc_end80-cast_numeric_uint8_float32_sse4
                                        # -- End function
	.globl	cast_numeric_int8_float32_sse4  # -- Begin function cast_numeric_int8_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_float32_sse4,@function
cast_numeric_int8_float32_sse4:         # @cast_numeric_int8_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB81_16
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
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
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	add	rdx, -1
	jne	.LBB81_4
.LBB81_5:
	cmp	r8, 3
	jb	.LBB81_16
	.p2align	4, 0x90
.LBB81_6:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rcx]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx], xmm0
	movsx	eax, byte ptr [rdi + rcx + 1]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx + 4], xmm0
	movsx	eax, byte ptr [rdi + rcx + 2]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx + 8], xmm0
	movsx	eax, byte ptr [rdi + rcx + 3]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx + 12], xmm0
	add	rcx, 4
	cmp	r9, rcx
	jne	.LBB81_6
	jmp	.LBB81_16
.LBB81_9:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovsxbd	xmm0, dword ptr [rdi + rdx]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovsxbd	xmm0, dword ptr [rdi + rdx + 8]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 12]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB81_12
# %bb.13:
	test	r8b, 1
	je	.LBB81_15
.LBB81_14:
	pmovsxbd	xmm0, dword ptr [rdi + rdx]
	pmovsxbd	xmm1, dword ptr [rdi + rdx + 4]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB81_15:
	cmp	rcx, r9
	jne	.LBB81_3
.LBB81_16:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB81_10:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB81_14
	jmp	.LBB81_15
.Lfunc_end81:
	.size	cast_numeric_int8_float32_sse4, .Lfunc_end81-cast_numeric_int8_float32_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_float32_sse4 # -- Begin function cast_numeric_uint16_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_float32_sse4,@function
cast_numeric_uint16_float32_sse4:       # @cast_numeric_uint16_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB82_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB82_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB82_10
.LBB82_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB82_6
# %bb.7:
	test	r8b, 1
	je	.LBB82_9
.LBB82_8:
	pmovzxwd	xmm0, qword ptr [rdi + 2*rdx]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdi + 2*rdx + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB82_9:
	cmp	rcx, r9
	je	.LBB82_11
	.p2align	4, 0x90
.LBB82_10:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rcx]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB82_10
.LBB82_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB82_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB82_8
	jmp	.LBB82_9
.Lfunc_end82:
	.size	cast_numeric_uint16_float32_sse4, .Lfunc_end82-cast_numeric_uint16_float32_sse4
                                        # -- End function
	.globl	cast_numeric_int16_float32_sse4 # -- Begin function cast_numeric_int16_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_float32_sse4,@function
cast_numeric_int16_float32_sse4:        # @cast_numeric_int16_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB83_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB83_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB83_10
.LBB83_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx + 16]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 24]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB83_6
# %bb.7:
	test	r8b, 1
	je	.LBB83_9
.LBB83_8:
	pmovsxwd	xmm0, qword ptr [rdi + 2*rdx]
	pmovsxwd	xmm1, qword ptr [rdi + 2*rdx + 8]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB83_9:
	cmp	rcx, r9
	je	.LBB83_11
	.p2align	4, 0x90
.LBB83_10:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdi + 2*rcx]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB83_10
.LBB83_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB83_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB83_8
	jmp	.LBB83_9
.Lfunc_end83:
	.size	cast_numeric_int16_float32_sse4, .Lfunc_end83-cast_numeric_int16_float32_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint32_float32_sse4
.LCPI84_0:
	.long	1258291200                      # 0x4b000000
	.long	1258291200                      # 0x4b000000
	.long	1258291200                      # 0x4b000000
	.long	1258291200                      # 0x4b000000
.LCPI84_1:
	.long	1392508928                      # 0x53000000
	.long	1392508928                      # 0x53000000
	.long	1392508928                      # 0x53000000
	.long	1392508928                      # 0x53000000
.LCPI84_2:
	.long	0x53000080                      # float 5.49764202E+11
	.long	0x53000080                      # float 5.49764202E+11
	.long	0x53000080                      # float 5.49764202E+11
	.long	0x53000080                      # float 5.49764202E+11
	.text
	.globl	cast_numeric_uint32_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_float32_sse4,@function
cast_numeric_uint32_float32_sse4:       # @cast_numeric_uint32_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB84_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB84_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB84_10
.LBB84_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB84_4
# %bb.5:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	movdqa	xmm0, xmmword ptr [rip + .LCPI84_0] # xmm0 = [1258291200,1258291200,1258291200,1258291200]
	movdqa	xmm1, xmmword ptr [rip + .LCPI84_1] # xmm1 = [1392508928,1392508928,1392508928,1392508928]
	movaps	xmm2, xmmword ptr [rip + .LCPI84_2] # xmm2 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
	.p2align	4, 0x90
.LBB84_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm4, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm5, xmm3
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm3, 16
	pblendw	xmm3, xmm1, 170                 # xmm3 = xmm3[0],xmm1[1],xmm3[2],xmm1[3],xmm3[4],xmm1[5],xmm3[6],xmm1[7]
	subps	xmm3, xmm2
	addps	xmm3, xmm5
	movdqa	xmm5, xmm4
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm4, 16
	pblendw	xmm4, xmm1, 170                 # xmm4 = xmm4[0],xmm1[1],xmm4[2],xmm1[3],xmm4[4],xmm1[5],xmm4[6],xmm1[7]
	subps	xmm4, xmm2
	addps	xmm4, xmm5
	movups	xmmword ptr [rsi + 4*rdx], xmm3
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm4
	movdqu	xmm3, xmmword ptr [rdi + 4*rdx + 32]
	movdqu	xmm4, xmmword ptr [rdi + 4*rdx + 48]
	movdqa	xmm5, xmm3
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm3, 16
	pblendw	xmm3, xmm1, 170                 # xmm3 = xmm3[0],xmm1[1],xmm3[2],xmm1[3],xmm3[4],xmm1[5],xmm3[6],xmm1[7]
	subps	xmm3, xmm2
	addps	xmm3, xmm5
	movdqa	xmm5, xmm4
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm4, 16
	pblendw	xmm4, xmm1, 170                 # xmm4 = xmm4[0],xmm1[1],xmm4[2],xmm1[3],xmm4[4],xmm1[5],xmm4[6],xmm1[7]
	subps	xmm4, xmm2
	addps	xmm4, xmm5
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm3
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm4
	add	rdx, 16
	add	rax, 2
	jne	.LBB84_6
# %bb.7:
	test	r8b, 1
	je	.LBB84_9
.LBB84_8:
	movdqu	xmm0, xmmword ptr [rdi + 4*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI84_0] # xmm2 = [1258291200,1258291200,1258291200,1258291200]
	movdqa	xmm3, xmm0
	pblendw	xmm3, xmm2, 170                 # xmm3 = xmm3[0],xmm2[1],xmm3[2],xmm2[3],xmm3[4],xmm2[5],xmm3[6],xmm2[7]
	psrld	xmm0, 16
	movdqa	xmm4, xmmword ptr [rip + .LCPI84_1] # xmm4 = [1392508928,1392508928,1392508928,1392508928]
	pblendw	xmm0, xmm4, 170                 # xmm0 = xmm0[0],xmm4[1],xmm0[2],xmm4[3],xmm0[4],xmm4[5],xmm0[6],xmm4[7]
	movaps	xmm5, xmmword ptr [rip + .LCPI84_2] # xmm5 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
	subps	xmm0, xmm5
	addps	xmm0, xmm3
	pblendw	xmm2, xmm1, 85                  # xmm2 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	psrld	xmm1, 16
	pblendw	xmm1, xmm4, 170                 # xmm1 = xmm1[0],xmm4[1],xmm1[2],xmm4[3],xmm1[4],xmm4[5],xmm1[6],xmm4[7]
	subps	xmm1, xmm5
	addps	xmm1, xmm2
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB84_9:
	cmp	rcx, r9
	je	.LBB84_11
	.p2align	4, 0x90
.LBB84_10:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rcx]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB84_10
.LBB84_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB84_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB84_8
	jmp	.LBB84_9
.Lfunc_end84:
	.size	cast_numeric_uint32_float32_sse4, .Lfunc_end84-cast_numeric_uint32_float32_sse4
                                        # -- End function
	.globl	cast_numeric_int32_float32_sse4 # -- Begin function cast_numeric_int32_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_float32_sse4,@function
cast_numeric_int32_float32_sse4:        # @cast_numeric_int32_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB85_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 7
	ja	.LBB85_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB85_10
.LBB85_3:
	mov	ecx, r9d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	r8, rax
	shr	r8, 3
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
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rdx + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 48]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 48], xmm1
	add	rdx, 16
	add	rax, 2
	jne	.LBB85_6
# %bb.7:
	test	r8b, 1
	je	.LBB85_9
.LBB85_8:
	movups	xmm0, xmmword ptr [rdi + 4*rdx]
	movups	xmm1, xmmword ptr [rdi + 4*rdx + 16]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rsi + 4*rdx], xmm0
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm1
.LBB85_9:
	cmp	rcx, r9
	je	.LBB85_11
	.p2align	4, 0x90
.LBB85_10:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, dword ptr [rdi + 4*rcx]
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB85_10
.LBB85_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB85_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB85_8
	jmp	.LBB85_9
.Lfunc_end85:
	.size	cast_numeric_int32_float32_sse4, .Lfunc_end85-cast_numeric_int32_float32_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint64_float32_sse4
.LCPI86_0:
	.quad	1                               # 0x1
	.quad	1                               # 0x1
	.text
	.globl	cast_numeric_uint64_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_float32_sse4,@function
cast_numeric_uint64_float32_sse4:       # @cast_numeric_uint64_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB86_14
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB86_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB86_10
.LBB86_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB86_4
# %bb.5:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edx, edx
	movdqa	xmm2, xmmword ptr [rip + .LCPI86_0] # xmm2 = [1,1]
	.p2align	4, 0x90
.LBB86_6:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqa	xmm1, xmm0
	pand	xmm1, xmm2
	movdqa	xmm3, xmm0
	psrlq	xmm3, 1
	por	xmm3, xmm1
	pxor	xmm4, xmm4
	pcmpgtq	xmm4, xmm0
	blendvpd	xmm0, xmm3, xmm0
	pextrq	rax, xmm0, 1
	xorps	xmm5, xmm5
	cvtsi2ss	xmm5, rax
	movq	rax, xmm0
	xorps	xmm3, xmm3
	cvtsi2ss	xmm3, rax
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	insertps	xmm3, xmm5, 28                  # xmm3 = xmm3[0],xmm5[0],zero,zero
	movaps	xmm5, xmm3
	addps	xmm5, xmm3
	pshufd	xmm0, xmm4, 237                 # xmm0 = xmm4[1,3,2,3]
	blendvps	xmm3, xmm5, xmm0
	movdqa	xmm0, xmm1
	pand	xmm0, xmm2
	movdqa	xmm4, xmm1
	psrlq	xmm4, 1
	por	xmm4, xmm0
	xorps	xmm5, xmm5
	pcmpgtq	xmm5, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	pextrq	rax, xmm1, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movq	rax, xmm1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, rax
	insertps	xmm1, xmm0, 28                  # xmm1 = xmm1[0],xmm0[0],zero,zero
	movaps	xmm4, xmm1
	addps	xmm4, xmm1
	pshufd	xmm0, xmm5, 237                 # xmm0 = xmm5[1,3,2,3]
	blendvps	xmm1, xmm4, xmm0
	movlhps	xmm3, xmm1                      # xmm3 = xmm3[0],xmm1[0]
	movups	xmmword ptr [rsi + 4*rdx], xmm3
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movdqa	xmm1, xmm0
	pand	xmm1, xmm2
	movdqa	xmm3, xmm0
	psrlq	xmm3, 1
	por	xmm3, xmm1
	xorps	xmm4, xmm4
	pcmpgtq	xmm4, xmm0
	blendvpd	xmm0, xmm3, xmm0
	pextrq	rax, xmm0, 1
	xorps	xmm5, xmm5
	cvtsi2ss	xmm5, rax
	movq	rax, xmm0
	xorps	xmm3, xmm3
	cvtsi2ss	xmm3, rax
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	insertps	xmm3, xmm5, 28                  # xmm3 = xmm3[0],xmm5[0],zero,zero
	movaps	xmm5, xmm3
	addps	xmm5, xmm3
	pshufd	xmm0, xmm4, 237                 # xmm0 = xmm4[1,3,2,3]
	blendvps	xmm3, xmm5, xmm0
	movdqa	xmm0, xmm1
	pand	xmm0, xmm2
	movdqa	xmm4, xmm1
	psrlq	xmm4, 1
	por	xmm4, xmm0
	xorps	xmm5, xmm5
	pcmpgtq	xmm5, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	pextrq	rax, xmm1, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movq	rax, xmm1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, rax
	insertps	xmm1, xmm0, 28                  # xmm1 = xmm1[0],xmm0[0],zero,zero
	movaps	xmm4, xmm1
	addps	xmm4, xmm1
	pshufd	xmm0, xmm5, 237                 # xmm0 = xmm5[1,3,2,3]
	blendvps	xmm1, xmm4, xmm0
	movlhps	xmm3, xmm1                      # xmm3 = xmm3[0],xmm1[0]
	movups	xmmword ptr [rsi + 4*rdx + 16], xmm3
	add	rdx, 8
	add	r10, 2
	jne	.LBB86_6
# %bb.7:
	test	r8b, 1
	je	.LBB86_9
.LBB86_8:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqa	xmm3, xmmword ptr [rip + .LCPI86_0] # xmm3 = [1,1]
	movdqa	xmm1, xmm0
	movdqa	xmm2, xmm0
	movdqa	xmm4, xmm0
	pand	xmm4, xmm3
	psrlq	xmm1, 1
	por	xmm1, xmm4
	blendvpd	xmm2, xmm1, xmm0
	pextrq	rax, xmm2, 1
	xorps	xmm4, xmm4
	cvtsi2ss	xmm4, rax
	movq	rax, xmm2
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, rax
	pxor	xmm5, xmm5
	pcmpgtq	xmm5, xmm0
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	insertps	xmm2, xmm4, 28                  # xmm2 = xmm2[0],xmm4[0],zero,zero
	movaps	xmm4, xmm2
	addps	xmm4, xmm2
	pxor	xmm6, xmm6
	pshufd	xmm0, xmm5, 237                 # xmm0 = xmm5[1,3,2,3]
	blendvps	xmm2, xmm4, xmm0
	pand	xmm3, xmm1
	movdqa	xmm4, xmm1
	psrlq	xmm4, 1
	por	xmm4, xmm3
	pcmpgtq	xmm6, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	pextrq	rax, xmm1, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movq	rax, xmm1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, rax
	insertps	xmm1, xmm0, 28                  # xmm1 = xmm1[0],xmm0[0],zero,zero
	movaps	xmm3, xmm1
	addps	xmm3, xmm1
	pshufd	xmm0, xmm6, 237                 # xmm0 = xmm6[1,3,2,3]
	blendvps	xmm1, xmm3, xmm0
	movlhps	xmm2, xmm1                      # xmm2 = xmm2[0],xmm1[0]
	movups	xmmword ptr [rsi + 4*rdx], xmm2
.LBB86_9:
	cmp	rcx, r9
	jne	.LBB86_10
	jmp	.LBB86_14
	.p2align	4, 0x90
.LBB86_12:                              #   in Loop: Header=BB86_10 Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	je	.LBB86_14
.LBB86_10:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*rcx]
	test	rax, rax
	jns	.LBB86_12
# %bb.11:                               #   in Loop: Header=BB86_10 Depth=1
	mov	rdx, rax
	shr	rdx
	and	eax, 1
	or	rax, rdx
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	addss	xmm0, xmm0
	movss	dword ptr [rsi + 4*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB86_10
.LBB86_14:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB86_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB86_8
	jmp	.LBB86_9
.Lfunc_end86:
	.size	cast_numeric_uint64_float32_sse4, .Lfunc_end86-cast_numeric_uint64_float32_sse4
                                        # -- End function
	.globl	cast_numeric_int64_float32_sse4 # -- Begin function cast_numeric_int64_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_float32_sse4,@function
cast_numeric_int64_float32_sse4:        # @cast_numeric_int64_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB87_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	eax, ecx
	and	eax, 3
	cmp	rdx, 3
	jae	.LBB87_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB87_3
.LBB87_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB87_8:                               # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdi + 8*rdx]
	movss	dword ptr [rsi + 4*rdx], xmm0
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdi + 8*rdx + 8]
	movss	dword ptr [rsi + 4*rdx + 4], xmm0
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdi + 8*rdx + 16]
	movss	dword ptr [rsi + 4*rdx + 8], xmm0
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdi + 8*rdx + 24]
	movss	dword ptr [rsi + 4*rdx + 12], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB87_8
.LBB87_3:
	test	rax, rax
	je	.LBB87_6
# %bb.4:
	lea	rcx, [rsi + 4*rdx]
	lea	rdx, [rdi + 8*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB87_5:                               # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdx + 8*rsi]
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB87_5
.LBB87_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end87:
	.size	cast_numeric_int64_float32_sse4, .Lfunc_end87-cast_numeric_int64_float32_sse4
                                        # -- End function
	.globl	cast_numeric_float32_float32_sse4 # -- Begin function cast_numeric_float32_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_float32_sse4,@function
cast_numeric_float32_float32_sse4:      # @cast_numeric_float32_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB88_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 7
	jbe	.LBB88_2
# %bb.7:
	lea	rax, [rdi + 4*r8]
	cmp	rax, rsi
	jbe	.LBB88_9
# %bb.8:
	lea	rax, [rsi + 4*r8]
	cmp	rax, rdi
	jbe	.LBB88_9
.LBB88_2:
	xor	ecx, ecx
.LBB88_3:
	mov	r9, rcx
	not	r9
	add	r9, r8
	mov	rdx, r8
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
	cmp	r9, 7
	jb	.LBB88_17
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
	cmp	r8, rcx
	jne	.LBB88_6
	jmp	.LBB88_17
.LBB88_9:
	mov	ecx, r8d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdx, rax
	shr	rdx, 3
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 24
	jae	.LBB88_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB88_13
.LBB88_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB88_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 4*rax]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 16]
	movups	xmmword ptr [rsi + 4*rax], xmm0
	movups	xmmword ptr [rsi + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 48]
	movups	xmmword ptr [rsi + 4*rax + 32], xmm0
	movups	xmmword ptr [rsi + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 80]
	movups	xmmword ptr [rsi + 4*rax + 64], xmm0
	movups	xmmword ptr [rsi + 4*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 4*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 4*rax + 112]
	movups	xmmword ptr [rsi + 4*rax + 96], xmm0
	movups	xmmword ptr [rsi + 4*rax + 112], xmm1
	add	rax, 32
	add	rdx, 4
	jne	.LBB88_12
.LBB88_13:
	test	r9, r9
	je	.LBB88_16
# %bb.14:
	lea	rax, [4*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB88_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB88_15
.LBB88_16:
	cmp	rcx, r8
	jne	.LBB88_3
.LBB88_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end88:
	.size	cast_numeric_float32_float32_sse4, .Lfunc_end88-cast_numeric_float32_float32_sse4
                                        # -- End function
	.globl	cast_numeric_float64_float32_sse4 # -- Begin function cast_numeric_float64_float32_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_float32_sse4,@function
cast_numeric_float64_float32_sse4:      # @cast_numeric_float64_float32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB89_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB89_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB89_10
.LBB89_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvtpd2ps	xmm0, xmm0
	cvtpd2ps	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rsi + 4*rdx], xmm0
	movupd	xmm0, xmmword ptr [rdi + 8*rdx + 32]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 48]
	cvtpd2ps	xmm0, xmm0
	cvtpd2ps	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rsi + 4*rdx + 16], xmm0
	add	rdx, 8
	add	rax, 2
	jne	.LBB89_6
# %bb.7:
	test	r8b, 1
	je	.LBB89_9
.LBB89_8:
	movupd	xmm0, xmmword ptr [rdi + 8*rdx]
	movupd	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	cvtpd2ps	xmm0, xmm0
	cvtpd2ps	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rsi + 4*rdx], xmm0
.LBB89_9:
	cmp	rcx, r9
	je	.LBB89_11
	.p2align	4, 0x90
.LBB89_10:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdi + 8*rcx]   # xmm0 = mem[0],zero
	cvtsd2ss	xmm0, xmm0
	movss	dword ptr [rsi + 4*rcx], xmm0
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
	.size	cast_numeric_float64_float32_sse4, .Lfunc_end89-cast_numeric_float64_float32_sse4
                                        # -- End function
	.globl	cast_numeric_uint8_float64_sse4 # -- Begin function cast_numeric_uint8_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint8_float64_sse4,@function
cast_numeric_uint8_float64_sse4:        # @cast_numeric_uint8_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB90_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB90_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB90_3
.LBB90_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB90_8:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx], xmm0
	movzx	eax, byte ptr [rdi + rdx + 1]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	movzx	eax, byte ptr [rdi + rdx + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	movzx	eax, byte ptr [rdi + rdx + 3]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB90_8
.LBB90_3:
	test	r8, r8
	je	.LBB90_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	add	rdi, rdx
	xor	edx, edx
	.p2align	4, 0x90
.LBB90_5:                               # =>This Inner Loop Header: Depth=1
	movzx	esi, byte ptr [rdi + rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, esi
	movsd	qword ptr [rcx + 8*rdx], xmm0
	add	rdx, 1
	cmp	r8, rdx
	jne	.LBB90_5
.LBB90_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end90:
	.size	cast_numeric_uint8_float64_sse4, .Lfunc_end90-cast_numeric_uint8_float64_sse4
                                        # -- End function
	.globl	cast_numeric_int8_float64_sse4  # -- Begin function cast_numeric_int8_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int8_float64_sse4,@function
cast_numeric_int8_float64_sse4:         # @cast_numeric_int8_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB91_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB91_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB91_3
.LBB91_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB91_8:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdi + rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx], xmm0
	movsx	eax, byte ptr [rdi + rdx + 1]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	movsx	eax, byte ptr [rdi + rdx + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	movsx	eax, byte ptr [rdi + rdx + 3]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB91_8
.LBB91_3:
	test	r8, r8
	je	.LBB91_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	add	rdi, rdx
	xor	edx, edx
	.p2align	4, 0x90
.LBB91_5:                               # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdi + rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, esi
	movsd	qword ptr [rcx + 8*rdx], xmm0
	add	rdx, 1
	cmp	r8, rdx
	jne	.LBB91_5
.LBB91_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end91:
	.size	cast_numeric_int8_float64_sse4, .Lfunc_end91-cast_numeric_int8_float64_sse4
                                        # -- End function
	.globl	cast_numeric_uint16_float64_sse4 # -- Begin function cast_numeric_uint16_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint16_float64_sse4,@function
cast_numeric_uint16_float64_sse4:       # @cast_numeric_uint16_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB92_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB92_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB92_3
.LBB92_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB92_8:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx], xmm0
	movzx	eax, word ptr [rdi + 2*rdx + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	movzx	eax, word ptr [rdi + 2*rdx + 4]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	movzx	eax, word ptr [rdi + 2*rdx + 6]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB92_8
.LBB92_3:
	test	r8, r8
	je	.LBB92_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 2*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB92_5:                               # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, edi
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB92_5
.LBB92_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end92:
	.size	cast_numeric_uint16_float64_sse4, .Lfunc_end92-cast_numeric_uint16_float64_sse4
                                        # -- End function
	.globl	cast_numeric_int16_float64_sse4 # -- Begin function cast_numeric_int16_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int16_float64_sse4,@function
cast_numeric_int16_float64_sse4:        # @cast_numeric_int16_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB93_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB93_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB93_3
.LBB93_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB93_8:                               # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdi + 2*rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx], xmm0
	movsx	eax, word ptr [rdi + 2*rdx + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	movsx	eax, word ptr [rdi + 2*rdx + 4]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	movsx	eax, word ptr [rdi + 2*rdx + 6]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB93_8
.LBB93_3:
	test	r8, r8
	je	.LBB93_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 2*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB93_5:                               # =>This Inner Loop Header: Depth=1
	movsx	edi, word ptr [rdx + 2*rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, edi
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB93_5
.LBB93_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end93:
	.size	cast_numeric_int16_float64_sse4, .Lfunc_end93-cast_numeric_int16_float64_sse4
                                        # -- End function
	.globl	cast_numeric_uint32_float64_sse4 # -- Begin function cast_numeric_uint32_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint32_float64_sse4,@function
cast_numeric_uint32_float64_sse4:       # @cast_numeric_uint32_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB94_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	r8d, ecx
	and	r8d, 3
	cmp	rdx, 3
	jae	.LBB94_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB94_3
.LBB94_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB94_8:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*rdx]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rsi + 8*rdx], xmm0
	mov	eax, dword ptr [rdi + 4*rdx + 4]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	mov	eax, dword ptr [rdi + 4*rdx + 8]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	mov	eax, dword ptr [rdi + 4*rdx + 12]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB94_8
.LBB94_3:
	test	r8, r8
	je	.LBB94_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 4*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB94_5:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rdi
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB94_5
.LBB94_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end94:
	.size	cast_numeric_uint32_float64_sse4, .Lfunc_end94-cast_numeric_uint32_float64_sse4
                                        # -- End function
	.globl	cast_numeric_int32_float64_sse4 # -- Begin function cast_numeric_int32_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int32_float64_sse4,@function
cast_numeric_int32_float64_sse4:        # @cast_numeric_int32_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB95_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	eax, ecx
	and	eax, 3
	cmp	rdx, 3
	jae	.LBB95_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB95_3
.LBB95_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB95_8:                               # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdi + 4*rdx]
	movsd	qword ptr [rsi + 8*rdx], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdi + 4*rdx + 4]
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdi + 4*rdx + 8]
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdi + 4*rdx + 12]
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB95_8
.LBB95_3:
	test	rax, rax
	je	.LBB95_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 4*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB95_5:                               # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdx + 4*rsi]
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB95_5
.LBB95_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end95:
	.size	cast_numeric_int32_float64_sse4, .Lfunc_end95-cast_numeric_int32_float64_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function cast_numeric_uint64_float64_sse4
.LCPI96_0:
	.quad	4841369599423283200             # 0x4330000000000000
	.quad	4841369599423283200             # 0x4330000000000000
.LCPI96_1:
	.quad	4985484787499139072             # 0x4530000000000000
	.quad	4985484787499139072             # 0x4530000000000000
.LCPI96_2:
	.quad	0x4530000000100000              # double 1.9342813118337666E+25
	.quad	0x4530000000100000              # double 1.9342813118337666E+25
.LCPI96_3:
	.long	1127219200                      # 0x43300000
	.long	1160773632                      # 0x45300000
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI96_4:
	.quad	0x4330000000000000              # double 4503599627370496
	.quad	0x4530000000000000              # double 1.9342813113834067E+25
	.text
	.globl	cast_numeric_uint64_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_uint64_float64_sse4,@function
cast_numeric_uint64_float64_sse4:       # @cast_numeric_uint64_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB96_12
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB96_5
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB96_3
.LBB96_5:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB96_6
# %bb.7:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edx, edx
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI96_0] # xmm1 = [4841369599423283200,4841369599423283200]
	movdqa	xmm2, xmmword ptr [rip + .LCPI96_1] # xmm2 = [4985484787499139072,4985484787499139072]
	movapd	xmm3, xmmword ptr [rip + .LCPI96_2] # xmm3 = [1.9342813118337666E+25,1.9342813118337666E+25]
	.p2align	4, 0x90
.LBB96_8:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm5, xmmword ptr [rdi + 8*rdx + 16]
	movdqa	xmm6, xmm4
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm4, 32
	por	xmm4, xmm2
	subpd	xmm4, xmm3
	addpd	xmm4, xmm6
	movdqa	xmm6, xmm5
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm5, 32
	por	xmm5, xmm2
	subpd	xmm5, xmm3
	addpd	xmm5, xmm6
	movupd	xmmword ptr [rsi + 8*rdx], xmm4
	movupd	xmmword ptr [rsi + 8*rdx + 16], xmm5
	movdqu	xmm4, xmmword ptr [rdi + 8*rdx + 32]
	movdqu	xmm5, xmmword ptr [rdi + 8*rdx + 48]
	movdqa	xmm6, xmm4
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm4, 32
	por	xmm4, xmm2
	subpd	xmm4, xmm3
	addpd	xmm4, xmm6
	movdqa	xmm6, xmm5
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm5, 32
	por	xmm5, xmm2
	subpd	xmm5, xmm3
	addpd	xmm5, xmm6
	movupd	xmmword ptr [rsi + 8*rdx + 32], xmm4
	movupd	xmmword ptr [rsi + 8*rdx + 48], xmm5
	add	rdx, 8
	add	rax, 2
	jne	.LBB96_8
# %bb.9:
	test	r8b, 1
	je	.LBB96_11
.LBB96_10:
	movdqu	xmm0, xmmword ptr [rdi + 8*rdx]
	movdqu	xmm1, xmmword ptr [rdi + 8*rdx + 16]
	pxor	xmm2, xmm2
	movdqa	xmm3, xmm0
	pblendw	xmm3, xmm2, 204                 # xmm3 = xmm3[0,1],xmm2[2,3],xmm3[4,5],xmm2[6,7]
	movdqa	xmm4, xmmword ptr [rip + .LCPI96_0] # xmm4 = [4841369599423283200,4841369599423283200]
	por	xmm3, xmm4
	psrlq	xmm0, 32
	movdqa	xmm5, xmmword ptr [rip + .LCPI96_1] # xmm5 = [4985484787499139072,4985484787499139072]
	por	xmm0, xmm5
	movapd	xmm6, xmmword ptr [rip + .LCPI96_2] # xmm6 = [1.9342813118337666E+25,1.9342813118337666E+25]
	subpd	xmm0, xmm6
	addpd	xmm0, xmm3
	pblendw	xmm2, xmm1, 51                  # xmm2 = xmm1[0,1],xmm2[2,3],xmm1[4,5],xmm2[6,7]
	por	xmm2, xmm4
	psrlq	xmm1, 32
	por	xmm1, xmm5
	subpd	xmm1, xmm6
	addpd	xmm1, xmm2
	movupd	xmmword ptr [rsi + 8*rdx], xmm0
	movupd	xmmword ptr [rsi + 8*rdx + 16], xmm1
.LBB96_11:
	cmp	rcx, r9
	je	.LBB96_12
.LBB96_3:
	movapd	xmm0, xmmword ptr [rip + .LCPI96_3] # xmm0 = [1127219200,1160773632,0,0]
	movapd	xmm1, xmmword ptr [rip + .LCPI96_4] # xmm1 = [4.503599627370496E+15,1.9342813113834067E+25]
	.p2align	4, 0x90
.LBB96_4:                               # =>This Inner Loop Header: Depth=1
	movsd	xmm2, qword ptr [rdi + 8*rcx]   # xmm2 = mem[0],zero
	unpcklps	xmm2, xmm0                      # xmm2 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	subpd	xmm2, xmm1
	movapd	xmm3, xmm2
	unpckhpd	xmm3, xmm2                      # xmm3 = xmm3[1],xmm2[1]
	addsd	xmm3, xmm2
	movsd	qword ptr [rsi + 8*rcx], xmm3
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB96_4
.LBB96_12:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB96_6:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB96_10
	jmp	.LBB96_11
.Lfunc_end96:
	.size	cast_numeric_uint64_float64_sse4, .Lfunc_end96-cast_numeric_uint64_float64_sse4
                                        # -- End function
	.globl	cast_numeric_int64_float64_sse4 # -- Begin function cast_numeric_int64_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_int64_float64_sse4,@function
cast_numeric_int64_float64_sse4:        # @cast_numeric_int64_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB97_6
# %bb.1:
	mov	ecx, edx
	lea	rdx, [rcx - 1]
	mov	eax, ecx
	and	eax, 3
	cmp	rdx, 3
	jae	.LBB97_7
# %bb.2:
	xor	edx, edx
	jmp	.LBB97_3
.LBB97_7:
	and	ecx, -4
	xor	edx, edx
	.p2align	4, 0x90
.LBB97_8:                               # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdi + 8*rdx]
	movsd	qword ptr [rsi + 8*rdx], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdi + 8*rdx + 8]
	movsd	qword ptr [rsi + 8*rdx + 8], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdi + 8*rdx + 16]
	movsd	qword ptr [rsi + 8*rdx + 16], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdi + 8*rdx + 24]
	movsd	qword ptr [rsi + 8*rdx + 24], xmm0
	add	rdx, 4
	cmp	rcx, rdx
	jne	.LBB97_8
.LBB97_3:
	test	rax, rax
	je	.LBB97_6
# %bb.4:
	lea	rcx, [rsi + 8*rdx]
	lea	rdx, [rdi + 8*rdx]
	xor	esi, esi
	.p2align	4, 0x90
.LBB97_5:                               # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB97_5
.LBB97_6:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end97:
	.size	cast_numeric_int64_float64_sse4, .Lfunc_end97-cast_numeric_int64_float64_sse4
                                        # -- End function
	.globl	cast_numeric_float32_float64_sse4 # -- Begin function cast_numeric_float32_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float32_float64_sse4,@function
cast_numeric_float32_float64_sse4:      # @cast_numeric_float32_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB98_11
# %bb.1:
	mov	r9d, edx
	cmp	edx, 3
	ja	.LBB98_3
# %bb.2:
	xor	ecx, ecx
	jmp	.LBB98_10
.LBB98_3:
	mov	ecx, r9d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r8, rax
	shr	r8, 2
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
	cvtps2pd	xmm0, qword ptr [rdi + 4*rdx]
	cvtps2pd	xmm1, qword ptr [rdi + 4*rdx + 8]
	movups	xmmword ptr [rsi + 8*rdx], xmm0
	movups	xmmword ptr [rsi + 8*rdx + 16], xmm1
	cvtps2pd	xmm0, qword ptr [rdi + 4*rdx + 16]
	cvtps2pd	xmm1, qword ptr [rdi + 4*rdx + 24]
	movups	xmmword ptr [rsi + 8*rdx + 32], xmm0
	movups	xmmword ptr [rsi + 8*rdx + 48], xmm1
	add	rdx, 8
	add	rax, 2
	jne	.LBB98_6
# %bb.7:
	test	r8b, 1
	je	.LBB98_9
.LBB98_8:
	cvtps2pd	xmm0, qword ptr [rdi + 4*rdx]
	cvtps2pd	xmm1, qword ptr [rdi + 4*rdx + 8]
	movups	xmmword ptr [rsi + 8*rdx], xmm0
	movups	xmmword ptr [rsi + 8*rdx + 16], xmm1
.LBB98_9:
	cmp	rcx, r9
	je	.LBB98_11
	.p2align	4, 0x90
.LBB98_10:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdi + 4*rcx]   # xmm0 = mem[0],zero,zero,zero
	cvtss2sd	xmm0, xmm0
	movsd	qword ptr [rsi + 8*rcx], xmm0
	add	rcx, 1
	cmp	r9, rcx
	jne	.LBB98_10
.LBB98_11:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB98_4:
	xor	edx, edx
	test	r8b, 1
	jne	.LBB98_8
	jmp	.LBB98_9
.Lfunc_end98:
	.size	cast_numeric_float32_float64_sse4, .Lfunc_end98-cast_numeric_float32_float64_sse4
                                        # -- End function
	.globl	cast_numeric_float64_float64_sse4 # -- Begin function cast_numeric_float64_float64_sse4
	.p2align	4, 0x90
	.type	cast_numeric_float64_float64_sse4,@function
cast_numeric_float64_float64_sse4:      # @cast_numeric_float64_float64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	edx, edx
	jle	.LBB99_17
# %bb.1:
	mov	r8d, edx
	cmp	edx, 3
	jbe	.LBB99_2
# %bb.7:
	lea	rax, [rdi + 8*r8]
	cmp	rax, rsi
	jbe	.LBB99_9
# %bb.8:
	lea	rax, [rsi + 8*r8]
	cmp	rax, rdi
	jbe	.LBB99_9
.LBB99_2:
	xor	ecx, ecx
.LBB99_3:
	mov	edx, r8d
	sub	edx, ecx
	mov	r9, rcx
	not	r9
	add	r9, r8
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
	cmp	r9, 7
	jb	.LBB99_17
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
	cmp	r8, rcx
	jne	.LBB99_6
	jmp	.LBB99_17
.LBB99_9:
	mov	ecx, r8d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdx, rax
	shr	rdx, 2
	add	rdx, 1
	mov	r9d, edx
	and	r9d, 3
	cmp	rax, 12
	jae	.LBB99_11
# %bb.10:
	xor	eax, eax
	jmp	.LBB99_13
.LBB99_11:
	and	rdx, -4
	neg	rdx
	xor	eax, eax
	.p2align	4, 0x90
.LBB99_12:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + 8*rax]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movups	xmmword ptr [rsi + 8*rax], xmm0
	movups	xmmword ptr [rsi + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 48]
	movups	xmmword ptr [rsi + 8*rax + 32], xmm0
	movups	xmmword ptr [rsi + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 80]
	movups	xmmword ptr [rsi + 8*rax + 64], xmm0
	movups	xmmword ptr [rsi + 8*rax + 80], xmm1
	movups	xmm0, xmmword ptr [rdi + 8*rax + 96]
	movups	xmm1, xmmword ptr [rdi + 8*rax + 112]
	movups	xmmword ptr [rsi + 8*rax + 96], xmm0
	movups	xmmword ptr [rsi + 8*rax + 112], xmm1
	add	rax, 16
	add	rdx, 4
	jne	.LBB99_12
.LBB99_13:
	test	r9, r9
	je	.LBB99_16
# %bb.14:
	lea	rax, [8*rax + 16]
	neg	r9
	.p2align	4, 0x90
.LBB99_15:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdi + rax - 16]
	movups	xmm1, xmmword ptr [rdi + rax]
	movups	xmmword ptr [rsi + rax - 16], xmm0
	movups	xmmword ptr [rsi + rax], xmm1
	add	rax, 32
	inc	r9
	jne	.LBB99_15
.LBB99_16:
	cmp	rcx, r8
	jne	.LBB99_3
.LBB99_17:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end99:
	.size	cast_numeric_float64_float64_sse4, .Lfunc_end99-cast_numeric_float64_float64_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
