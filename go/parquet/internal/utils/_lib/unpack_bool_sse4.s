	.text
	.intel_syntax noprefix
	.file	"unpack_bool.c"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4               # -- Begin function bytes_to_bools_sse4
.LCPI0_0:
	.zero	16,1
	.text
	.globl	bytes_to_bools_sse4
	.p2align	4, 0x90
	.type	bytes_to_bools_sse4,@function
bytes_to_bools_sse4:                    # @bytes_to_bools_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -16
	sub	rsp, 16
	test	esi, esi
	jle	.LBB0_11
# %bb.1:
	mov	r8d, esi
	xor	r10d, r10d
	cmp	esi, 16
	jb	.LBB0_9
# %bb.2:
	lea	rax, [r8 - 1]
	cmp	rax, 536870911
	ja	.LBB0_9
# %bb.3:
	lea	rax, [rdx + 8*r8]
	cmp	rax, rdi
	jbe	.LBB0_6
# %bb.4:
	lea	rax, [rdi + r8]
	cmp	rax, rdx
	jbe	.LBB0_6
# %bb.5:
	xor	r10d, r10d
	jmp	.LBB0_9
.LBB0_6:
	mov	r10d, r8d
	and	r10d, -16
	xor	r9d, r9d
	xor	eax, eax
	.p2align	4, 0x90
.LBB0_7:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm6, xmmword ptr [rdi + rax]
	movdqa	xmm13, xmm6
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	pand	xmm13, xmm0
	movdqa	xmm12, xmm6
	psrlw	xmm12, 1
	pand	xmm12, xmm0
	movdqa	xmm8, xmm6
	psrlw	xmm8, 2
	pand	xmm8, xmm0
	movdqa	xmm14, xmm6
	psrlw	xmm14, 3
	pand	xmm14, xmm0
	movdqa	xmm11, xmm6
	psrlw	xmm11, 4
	pand	xmm11, xmm0
	movdqa	xmm15, xmm6
	psrlw	xmm15, 5
	pand	xmm15, xmm0
	movdqa	xmm4, xmm6
	psrlw	xmm4, 6
	pand	xmm4, xmm0
	psrlw	xmm6, 7
	pand	xmm6, xmm0
	movdqa	xmm5, xmm4
	punpcklbw	xmm5, xmm6      # xmm5 = xmm5[0],xmm6[0],xmm5[1],xmm6[1],xmm5[2],xmm6[2],xmm5[3],xmm6[3],xmm5[4],xmm6[4],xmm5[5],xmm6[5],xmm5[6],xmm6[6],xmm5[7],xmm6[7]
	pshuflw	xmm0, xmm5, 96          # xmm0 = xmm5[0,0,2,1,4,5,6,7]
	pshufd	xmm0, xmm0, 96          # xmm0 = xmm0[0,0,2,1]
	movdqa	xmm3, xmm11
	punpcklbw	xmm3, xmm15     # xmm3 = xmm3[0],xmm15[0],xmm3[1],xmm15[1],xmm3[2],xmm15[2],xmm3[3],xmm15[3],xmm3[4],xmm15[4],xmm3[5],xmm15[5],xmm3[6],xmm15[6],xmm3[7],xmm15[7]
	pshuflw	xmm2, xmm3, 212         # xmm2 = xmm3[0,1,1,3,4,5,6,7]
	pshufd	xmm10, xmm2, 96         # xmm10 = xmm2[0,0,2,1]
	pblendw	xmm10, xmm0, 136        # xmm10 = xmm10[0,1,2],xmm0[3],xmm10[4,5,6],xmm0[7]
	movdqa	xmm0, xmm13
	punpcklbw	xmm0, xmm12     # xmm0 = xmm0[0],xmm12[0],xmm0[1],xmm12[1],xmm0[2],xmm12[2],xmm0[3],xmm12[3],xmm0[4],xmm12[4],xmm0[5],xmm12[5],xmm0[6],xmm12[6],xmm0[7],xmm12[7]
	pmovzxwq	xmm1, xmm0      # xmm1 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero
	movdqa	xmm2, xmm8
	punpcklbw	xmm2, xmm14     # xmm2 = xmm2[0],xmm14[0],xmm2[1],xmm14[1],xmm2[2],xmm14[2],xmm2[3],xmm14[3],xmm2[4],xmm14[4],xmm2[5],xmm14[5],xmm2[6],xmm14[6],xmm2[7],xmm14[7]
	pshuflw	xmm7, xmm2, 96          # xmm7 = xmm2[0,0,2,1,4,5,6,7]
	pshufd	xmm7, xmm7, 212         # xmm7 = xmm7[0,1,1,3]
	pblendw	xmm7, xmm1, 221         # xmm7 = xmm1[0],xmm7[1],xmm1[2,3,4],xmm7[5],xmm1[6,7]
	pblendw	xmm7, xmm10, 204        # xmm7 = xmm7[0,1],xmm10[2,3],xmm7[4,5],xmm10[6,7]
	movdqa	xmmword ptr [rsp], xmm7 # 16-byte Spill
	pshuflw	xmm7, xmm5, 232         # xmm7 = xmm5[0,2,2,3,4,5,6,7]
	pshufd	xmm10, xmm7, 96         # xmm10 = xmm7[0,0,2,1]
	pshuflw	xmm7, xmm3, 246         # xmm7 = xmm3[2,1,3,3,4,5,6,7]
	pshufd	xmm7, xmm7, 96          # xmm7 = xmm7[0,0,2,1]
	pblendw	xmm7, xmm10, 136        # xmm7 = xmm7[0,1,2],xmm10[3],xmm7[4,5,6],xmm10[7]
	pshuflw	xmm1, xmm2, 232         # xmm1 = xmm2[0,2,2,3,4,5,6,7]
	pshufd	xmm9, xmm1, 212         # xmm9 = xmm1[0,1,1,3]
	pshufd	xmm1, xmm0, 229         # xmm1 = xmm0[1,1,2,3]
	pmovzxwq	xmm10, xmm1     # xmm10 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	pblendw	xmm10, xmm9, 34         # xmm10 = xmm10[0],xmm9[1],xmm10[2,3,4],xmm9[5],xmm10[6,7]
	pblendw	xmm10, xmm7, 204        # xmm10 = xmm10[0,1],xmm7[2,3],xmm10[4,5],xmm7[6,7]
	pshufhw	xmm1, xmm5, 96          # xmm1 = xmm5[0,1,2,3,4,4,6,5]
	pshufd	xmm1, xmm1, 232         # xmm1 = xmm1[0,2,2,3]
	pshufhw	xmm7, xmm3, 212         # xmm7 = xmm3[0,1,2,3,4,5,5,7]
	pshufd	xmm7, xmm7, 232         # xmm7 = xmm7[0,2,2,3]
	pblendw	xmm7, xmm1, 136         # xmm7 = xmm7[0,1,2],xmm1[3],xmm7[4,5,6],xmm1[7]
	pshufhw	xmm1, xmm2, 96          # xmm1 = xmm2[0,1,2,3,4,4,6,5]
	pshufd	xmm9, xmm1, 246         # xmm9 = xmm1[2,1,3,3]
	pshufd	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	pmovzxwq	xmm1, xmm1      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	pblendw	xmm1, xmm9, 34          # xmm1 = xmm1[0],xmm9[1],xmm1[2,3,4],xmm9[5],xmm1[6,7]
	pblendw	xmm1, xmm7, 204         # xmm1 = xmm1[0,1],xmm7[2,3],xmm1[4,5],xmm7[6,7]
	pshufhw	xmm5, xmm5, 232         # xmm5 = xmm5[0,1,2,3,4,6,6,7]
	pshufd	xmm5, xmm5, 232         # xmm5 = xmm5[0,2,2,3]
	pshufhw	xmm3, xmm3, 246         # xmm3 = xmm3[0,1,2,3,6,5,7,7]
	pshufd	xmm3, xmm3, 232         # xmm3 = xmm3[0,2,2,3]
	pblendw	xmm3, xmm5, 136         # xmm3 = xmm3[0,1,2],xmm5[3],xmm3[4,5,6],xmm5[7]
	pshufhw	xmm2, xmm2, 232         # xmm2 = xmm2[0,1,2,3,4,6,6,7]
	pshufd	xmm2, xmm2, 246         # xmm2 = xmm2[2,1,3,3]
	pshufd	xmm0, xmm0, 231         # xmm0 = xmm0[3,1,2,3]
	pmovzxwq	xmm5, xmm0      # xmm5 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero
	pblendw	xmm5, xmm2, 34          # xmm5 = xmm5[0],xmm2[1],xmm5[2,3,4],xmm2[5],xmm5[6,7]
	pblendw	xmm5, xmm3, 204         # xmm5 = xmm5[0,1],xmm3[2,3],xmm5[4,5],xmm3[6,7]
	punpckhbw	xmm4, xmm6      # xmm4 = xmm4[8],xmm6[8],xmm4[9],xmm6[9],xmm4[10],xmm6[10],xmm4[11],xmm6[11],xmm4[12],xmm6[12],xmm4[13],xmm6[13],xmm4[14],xmm6[14],xmm4[15],xmm6[15]
	pshuflw	xmm0, xmm4, 96          # xmm0 = xmm4[0,0,2,1,4,5,6,7]
	pshufd	xmm0, xmm0, 96          # xmm0 = xmm0[0,0,2,1]
	punpckhbw	xmm11, xmm15    # xmm11 = xmm11[8],xmm15[8],xmm11[9],xmm15[9],xmm11[10],xmm15[10],xmm11[11],xmm15[11],xmm11[12],xmm15[12],xmm11[13],xmm15[13],xmm11[14],xmm15[14],xmm11[15],xmm15[15]
	pshuflw	xmm2, xmm11, 212        # xmm2 = xmm11[0,1,1,3,4,5,6,7]
	pshufd	xmm2, xmm2, 96          # xmm2 = xmm2[0,0,2,1]
	pblendw	xmm2, xmm0, 136         # xmm2 = xmm2[0,1,2],xmm0[3],xmm2[4,5,6],xmm0[7]
	punpckhbw	xmm13, xmm12    # xmm13 = xmm13[8],xmm12[8],xmm13[9],xmm12[9],xmm13[10],xmm12[10],xmm13[11],xmm12[11],xmm13[12],xmm12[12],xmm13[13],xmm12[13],xmm13[14],xmm12[14],xmm13[15],xmm12[15]
	pmovzxwq	xmm3, xmm13     # xmm3 = xmm13[0],zero,zero,zero,xmm13[1],zero,zero,zero
	punpckhbw	xmm8, xmm14     # xmm8 = xmm8[8],xmm14[8],xmm8[9],xmm14[9],xmm8[10],xmm14[10],xmm8[11],xmm14[11],xmm8[12],xmm14[12],xmm8[13],xmm14[13],xmm8[14],xmm14[14],xmm8[15],xmm14[15]
	pshuflw	xmm0, xmm8, 96          # xmm0 = xmm8[0,0,2,1,4,5,6,7]
	pshufd	xmm0, xmm0, 212         # xmm0 = xmm0[0,1,1,3]
	pblendw	xmm0, xmm3, 221         # xmm0 = xmm3[0],xmm0[1],xmm3[2,3,4],xmm0[5],xmm3[6,7]
	pblendw	xmm0, xmm2, 204         # xmm0 = xmm0[0,1],xmm2[2,3],xmm0[4,5],xmm2[6,7]
	pshuflw	xmm2, xmm4, 232         # xmm2 = xmm4[0,2,2,3,4,5,6,7]
	pshufd	xmm2, xmm2, 96          # xmm2 = xmm2[0,0,2,1]
	pshuflw	xmm3, xmm11, 246        # xmm3 = xmm11[2,1,3,3,4,5,6,7]
	pshufd	xmm3, xmm3, 96          # xmm3 = xmm3[0,0,2,1]
	pblendw	xmm3, xmm2, 136         # xmm3 = xmm3[0,1,2],xmm2[3],xmm3[4,5,6],xmm2[7]
	pshuflw	xmm2, xmm8, 232         # xmm2 = xmm8[0,2,2,3,4,5,6,7]
	pshufd	xmm6, xmm2, 212         # xmm6 = xmm2[0,1,1,3]
	pshufd	xmm2, xmm13, 229        # xmm2 = xmm13[1,1,2,3]
	pmovzxwq	xmm2, xmm2      # xmm2 = xmm2[0],zero,zero,zero,xmm2[1],zero,zero,zero
	pblendw	xmm2, xmm6, 34          # xmm2 = xmm2[0],xmm6[1],xmm2[2,3,4],xmm6[5],xmm2[6,7]
	pblendw	xmm2, xmm3, 204         # xmm2 = xmm2[0,1],xmm3[2,3],xmm2[4,5],xmm3[6,7]
	pshufhw	xmm3, xmm4, 96          # xmm3 = xmm4[0,1,2,3,4,4,6,5]
	pshufd	xmm3, xmm3, 232         # xmm3 = xmm3[0,2,2,3]
	pshufhw	xmm6, xmm11, 212        # xmm6 = xmm11[0,1,2,3,4,5,5,7]
	pshufd	xmm6, xmm6, 232         # xmm6 = xmm6[0,2,2,3]
	pblendw	xmm6, xmm3, 136         # xmm6 = xmm6[0,1,2],xmm3[3],xmm6[4,5,6],xmm3[7]
	pshufhw	xmm3, xmm8, 96          # xmm3 = xmm8[0,1,2,3,4,4,6,5]
	pshufd	xmm3, xmm3, 246         # xmm3 = xmm3[2,1,3,3]
	pshufd	xmm7, xmm13, 78         # xmm7 = xmm13[2,3,0,1]
	pmovzxwq	xmm7, xmm7      # xmm7 = xmm7[0],zero,zero,zero,xmm7[1],zero,zero,zero
	pblendw	xmm7, xmm3, 34          # xmm7 = xmm7[0],xmm3[1],xmm7[2,3,4],xmm3[5],xmm7[6,7]
	pblendw	xmm7, xmm6, 204         # xmm7 = xmm7[0,1],xmm6[2,3],xmm7[4,5],xmm6[6,7]
	pshufhw	xmm3, xmm4, 232         # xmm3 = xmm4[0,1,2,3,4,6,6,7]
	pshufd	xmm3, xmm3, 232         # xmm3 = xmm3[0,2,2,3]
	pshufhw	xmm4, xmm11, 246        # xmm4 = xmm11[0,1,2,3,6,5,7,7]
	pshufd	xmm4, xmm4, 232         # xmm4 = xmm4[0,2,2,3]
	pblendw	xmm4, xmm3, 136         # xmm4 = xmm4[0,1,2],xmm3[3],xmm4[4,5,6],xmm3[7]
	pshufhw	xmm3, xmm8, 232         # xmm3 = xmm8[0,1,2,3,4,6,6,7]
	pshufd	xmm3, xmm3, 246         # xmm3 = xmm3[2,1,3,3]
	pshufd	xmm6, xmm13, 231        # xmm6 = xmm13[3,1,2,3]
	pmovzxwq	xmm6, xmm6      # xmm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero
	pblendw	xmm6, xmm3, 34          # xmm6 = xmm6[0],xmm3[1],xmm6[2,3,4],xmm3[5],xmm6[6,7]
	pblendw	xmm6, xmm4, 204         # xmm6 = xmm6[0,1],xmm4[2,3],xmm6[4,5],xmm4[6,7]
	mov	esi, r9d
	and	esi, -128
	movdqu	xmmword ptr [rdx + rsi + 112], xmm6
	movdqu	xmmword ptr [rdx + rsi + 96], xmm7
	movdqu	xmmword ptr [rdx + rsi + 80], xmm2
	movdqu	xmmword ptr [rdx + rsi + 64], xmm0
	movdqu	xmmword ptr [rdx + rsi + 48], xmm5
	movdqu	xmmword ptr [rdx + rsi + 32], xmm1
	movdqu	xmmword ptr [rdx + rsi + 16], xmm10
	movaps	xmm0, xmmword ptr [rsp] # 16-byte Reload
	movups	xmmword ptr [rdx + rsi], xmm0
	add	rax, 16
	sub	r9, -128
	cmp	r10, rax
	jne	.LBB0_7
# %bb.8:
	cmp	r10, r8
	je	.LBB0_11
.LBB0_9:
	lea	rax, [8*r10]
	.p2align	4, 0x90
.LBB0_10:                               # =>This Inner Loop Header: Depth=1
	mov	esi, eax
	and	esi, -8
	movzx	ecx, byte ptr [rdi + r10]
	and	cl, 1
	mov	byte ptr [rdx + rsi], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl
	and	cl, 1
	mov	byte ptr [rdx + rsi + 1], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl, 2
	and	cl, 1
	mov	byte ptr [rdx + rsi + 2], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl, 3
	and	cl, 1
	mov	byte ptr [rdx + rsi + 3], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl, 4
	and	cl, 1
	mov	byte ptr [rdx + rsi + 4], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl, 5
	and	cl, 1
	mov	byte ptr [rdx + rsi + 5], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl, 6
	and	cl, 1
	mov	byte ptr [rdx + rsi + 6], cl
	movzx	ecx, byte ptr [rdi + r10]
	shr	cl, 7
	mov	byte ptr [rdx + rsi + 7], cl
	add	r10, 1
	add	rax, 8
	cmp	r8, r10
	jne	.LBB0_10
.LBB0_11:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	bytes_to_bools_sse4, .Lfunc_end0-bytes_to_bools_sse4
                                        # -- End function
	.ident	"clang version 10.0.0-4ubuntu1 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
