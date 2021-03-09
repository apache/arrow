	.text
	.intel_syntax noprefix
	.file	"unpack_bool.c"
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5               # -- Begin function bytes_to_bools_avx2
.LCPI0_0:
	.zero	32,1
	.text
	.globl	bytes_to_bools_avx2
	.p2align	4, 0x90
	.type	bytes_to_bools_avx2,@function
bytes_to_bools_avx2:                    # @bytes_to_bools_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -32
	sub	rsp, 192
	test	esi, esi
	jle	.LBB0_11
# %bb.1:
	mov	r8d, esi
	xor	r10d, r10d
	cmp	esi, 32
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
	and	r10d, -32
	xor	r9d, r9d
	xor	eax, eax
	.p2align	4, 0x90
.LBB0_7:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdi + rax]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_0] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpand	ymm13, ymm0, ymm2
	vpsrlw	ymm1, ymm0, 1
	vpand	ymm6, ymm1, ymm2
	vpsrlw	ymm1, ymm0, 2
	vpand	ymm5, ymm1, ymm2
	vpsrlw	ymm1, ymm0, 3
	vpand	ymm14, ymm1, ymm2
	vpsrlw	ymm1, ymm0, 4
	vpand	ymm8, ymm1, ymm2
	vpsrlw	ymm1, ymm0, 5
	vpand	ymm9, ymm1, ymm2
	vpsrlw	ymm1, ymm0, 6
	vpand	ymm11, ymm1, ymm2
	vpsrlw	ymm0, ymm0, 7
	vpand	ymm12, ymm0, ymm2
	vpunpcklbw	xmm0, xmm11, xmm12 # xmm0 = xmm11[0],xmm12[0],xmm11[1],xmm12[1],xmm11[2],xmm12[2],xmm11[3],xmm12[3],xmm11[4],xmm12[4],xmm11[5],xmm12[5],xmm11[6],xmm12[6],xmm11[7],xmm12[7]
	vpshuflw	xmm1, xmm0, 96  # xmm1 = xmm0[0,0,2,1,4,5,6,7]
	vpshuflw	xmm2, xmm0, 232 # xmm2 = xmm0[0,2,2,3,4,5,6,7]
	vinserti128	ymm1, ymm1, xmm2, 1
	vpshufd	ymm1, ymm1, 96          # ymm1 = ymm1[0,0,2,1,4,4,6,5]
	vpunpcklbw	xmm2, xmm8, xmm9 # xmm2 = xmm8[0],xmm9[0],xmm8[1],xmm9[1],xmm8[2],xmm9[2],xmm8[3],xmm9[3],xmm8[4],xmm9[4],xmm8[5],xmm9[5],xmm8[6],xmm9[6],xmm8[7],xmm9[7]
	vpshuflw	xmm7, xmm2, 212 # xmm7 = xmm2[0,1,1,3,4,5,6,7]
	vpshuflw	xmm3, xmm2, 246 # xmm3 = xmm2[2,1,3,3,4,5,6,7]
	vinserti128	ymm3, ymm7, xmm3, 1
	vpshufd	ymm3, ymm3, 96          # ymm3 = ymm3[0,0,2,1,4,4,6,5]
	vpblendw	ymm1, ymm3, ymm1, 136 # ymm1 = ymm3[0,1,2],ymm1[3],ymm3[4,5,6],ymm1[7],ymm3[8,9,10],ymm1[11],ymm3[12,13,14],ymm1[15]
	vpunpcklbw	xmm3, xmm13, xmm6 # xmm3 = xmm13[0],xmm6[0],xmm13[1],xmm6[1],xmm13[2],xmm6[2],xmm13[3],xmm6[3],xmm13[4],xmm6[4],xmm13[5],xmm6[5],xmm13[6],xmm6[6],xmm13[7],xmm6[7]
	vpmovzxwq	xmm7, xmm3      # xmm7 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	vpshufd	xmm4, xmm3, 229         # xmm4 = xmm3[1,1,2,3]
	vpmovzxwq	xmm4, xmm4      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero
	vinserti128	ymm4, ymm7, xmm4, 1
	vpunpcklbw	xmm7, xmm5, xmm14 # xmm7 = xmm5[0],xmm14[0],xmm5[1],xmm14[1],xmm5[2],xmm14[2],xmm5[3],xmm14[3],xmm5[4],xmm14[4],xmm5[5],xmm14[5],xmm5[6],xmm14[6],xmm5[7],xmm14[7]
	vmovdqa	ymmword ptr [rsp], ymm5 # 32-byte Spill
	vpshuflw	xmm10, xmm7, 96 # xmm10 = xmm7[0,0,2,1,4,5,6,7]
	vpshuflw	xmm15, xmm7, 232 # xmm15 = xmm7[0,2,2,3,4,5,6,7]
	vinserti128	ymm10, ymm10, xmm15, 1
	vpshufd	ymm10, ymm10, 212       # ymm10 = ymm10[0,1,1,3,4,5,5,7]
	vpblendw	ymm4, ymm4, ymm10, 34 # ymm4 = ymm4[0],ymm10[1],ymm4[2,3,4],ymm10[5],ymm4[6,7,8],ymm10[9],ymm4[10,11,12],ymm10[13],ymm4[14,15]
	vpblendd	ymm1, ymm4, ymm1, 170 # ymm1 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7]
	vmovdqa	ymmword ptr [rsp + 128], ymm1 # 32-byte Spill
	vpshufhw	xmm1, xmm0, 96  # xmm1 = xmm0[0,1,2,3,4,4,6,5]
	vpshufhw	xmm0, xmm0, 232 # xmm0 = xmm0[0,1,2,3,4,6,6,7]
	vinserti128	ymm0, ymm1, xmm0, 1
	vpshufd	ymm0, ymm0, 232         # ymm0 = ymm0[0,2,2,3,4,6,6,7]
	vpshufhw	xmm1, xmm2, 212 # xmm1 = xmm2[0,1,2,3,4,5,5,7]
	vpshufhw	xmm2, xmm2, 246 # xmm2 = xmm2[0,1,2,3,6,5,7,7]
	vinserti128	ymm1, ymm1, xmm2, 1
	vpshufd	ymm1, ymm1, 232         # ymm1 = ymm1[0,2,2,3,4,6,6,7]
	vpblendw	ymm0, ymm1, ymm0, 136 # ymm0 = ymm1[0,1,2],ymm0[3],ymm1[4,5,6],ymm0[7],ymm1[8,9,10],ymm0[11],ymm1[12,13,14],ymm0[15]
	vpshufd	xmm1, xmm3, 78          # xmm1 = xmm3[2,3,0,1]
	vpmovzxwq	xmm1, xmm1      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	vpshufd	xmm2, xmm3, 231         # xmm2 = xmm3[3,1,2,3]
	vpmovzxwq	xmm2, xmm2      # xmm2 = xmm2[0],zero,zero,zero,xmm2[1],zero,zero,zero
	vinserti128	ymm1, ymm1, xmm2, 1
	vpshufhw	xmm2, xmm7, 96  # xmm2 = xmm7[0,1,2,3,4,4,6,5]
	vpshufhw	xmm3, xmm7, 232 # xmm3 = xmm7[0,1,2,3,4,6,6,7]
	vinserti128	ymm2, ymm2, xmm3, 1
	vpshufd	ymm2, ymm2, 246         # ymm2 = ymm2[2,1,3,3,6,5,7,7]
	vpblendw	ymm1, ymm1, ymm2, 34 # ymm1 = ymm1[0],ymm2[1],ymm1[2,3,4],ymm2[5],ymm1[6,7,8],ymm2[9],ymm1[10,11,12],ymm2[13],ymm1[14,15]
	vpblendd	ymm0, ymm1, ymm0, 170 # ymm0 = ymm1[0],ymm0[1],ymm1[2],ymm0[3],ymm1[4],ymm0[5],ymm1[6],ymm0[7]
	vmovdqa	ymmword ptr [rsp + 96], ymm0 # 32-byte Spill
	vpunpckhbw	xmm0, xmm11, xmm12 # xmm0 = xmm11[8],xmm12[8],xmm11[9],xmm12[9],xmm11[10],xmm12[10],xmm11[11],xmm12[11],xmm11[12],xmm12[12],xmm11[13],xmm12[13],xmm11[14],xmm12[14],xmm11[15],xmm12[15]
	vpshuflw	xmm1, xmm0, 96  # xmm1 = xmm0[0,0,2,1,4,5,6,7]
	vpshuflw	xmm2, xmm0, 232 # xmm2 = xmm0[0,2,2,3,4,5,6,7]
	vinserti128	ymm1, ymm1, xmm2, 1
	vpshufd	ymm1, ymm1, 96          # ymm1 = ymm1[0,0,2,1,4,4,6,5]
	vpunpckhbw	xmm2, xmm8, xmm9 # xmm2 = xmm8[8],xmm9[8],xmm8[9],xmm9[9],xmm8[10],xmm9[10],xmm8[11],xmm9[11],xmm8[12],xmm9[12],xmm8[13],xmm9[13],xmm8[14],xmm9[14],xmm8[15],xmm9[15]
	vpshuflw	xmm3, xmm2, 212 # xmm3 = xmm2[0,1,1,3,4,5,6,7]
	vpshuflw	xmm4, xmm2, 246 # xmm4 = xmm2[2,1,3,3,4,5,6,7]
	vinserti128	ymm3, ymm3, xmm4, 1
	vpshufd	ymm3, ymm3, 96          # ymm3 = ymm3[0,0,2,1,4,4,6,5]
	vpblendw	ymm1, ymm3, ymm1, 136 # ymm1 = ymm3[0,1,2],ymm1[3],ymm3[4,5,6],ymm1[7],ymm3[8,9,10],ymm1[11],ymm3[12,13,14],ymm1[15]
	vpunpckhbw	xmm3, xmm13, xmm6 # xmm3 = xmm13[8],xmm6[8],xmm13[9],xmm6[9],xmm13[10],xmm6[10],xmm13[11],xmm6[11],xmm13[12],xmm6[12],xmm13[13],xmm6[13],xmm13[14],xmm6[14],xmm13[15],xmm6[15]
	vpmovzxwq	xmm4, xmm3      # xmm4 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	vpshufd	xmm7, xmm3, 229         # xmm7 = xmm3[1,1,2,3]
	vpmovzxwq	xmm7, xmm7      # xmm7 = xmm7[0],zero,zero,zero,xmm7[1],zero,zero,zero
	vinserti128	ymm4, ymm4, xmm7, 1
	vpunpckhbw	xmm7, xmm5, xmm14 # xmm7 = xmm5[8],xmm14[8],xmm5[9],xmm14[9],xmm5[10],xmm14[10],xmm5[11],xmm14[11],xmm5[12],xmm14[12],xmm5[13],xmm14[13],xmm5[14],xmm14[14],xmm5[15],xmm14[15]
	vpshuflw	xmm10, xmm7, 96 # xmm10 = xmm7[0,0,2,1,4,5,6,7]
	vpshuflw	xmm5, xmm7, 232 # xmm5 = xmm7[0,2,2,3,4,5,6,7]
	vinserti128	ymm5, ymm10, xmm5, 1
	vpshufd	ymm5, ymm5, 212         # ymm5 = ymm5[0,1,1,3,4,5,5,7]
	vpblendw	ymm4, ymm4, ymm5, 34 # ymm4 = ymm4[0],ymm5[1],ymm4[2,3,4],ymm5[5],ymm4[6,7,8],ymm5[9],ymm4[10,11,12],ymm5[13],ymm4[14,15]
	vpblendd	ymm1, ymm4, ymm1, 170 # ymm1 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7]
	vmovdqa	ymmword ptr [rsp + 64], ymm1 # 32-byte Spill
	vpshufhw	xmm1, xmm0, 96  # xmm1 = xmm0[0,1,2,3,4,4,6,5]
	vpshufhw	xmm0, xmm0, 232 # xmm0 = xmm0[0,1,2,3,4,6,6,7]
	vinserti128	ymm0, ymm1, xmm0, 1
	vpshufd	ymm0, ymm0, 232         # ymm0 = ymm0[0,2,2,3,4,6,6,7]
	vpshufhw	xmm1, xmm2, 212 # xmm1 = xmm2[0,1,2,3,4,5,5,7]
	vpshufhw	xmm2, xmm2, 246 # xmm2 = xmm2[0,1,2,3,6,5,7,7]
	vinserti128	ymm1, ymm1, xmm2, 1
	vpshufd	ymm1, ymm1, 232         # ymm1 = ymm1[0,2,2,3,4,6,6,7]
	vpblendw	ymm0, ymm1, ymm0, 136 # ymm0 = ymm1[0,1,2],ymm0[3],ymm1[4,5,6],ymm0[7],ymm1[8,9,10],ymm0[11],ymm1[12,13,14],ymm0[15]
	vpshufd	xmm1, xmm3, 78          # xmm1 = xmm3[2,3,0,1]
	vpmovzxwq	xmm1, xmm1      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	vpshufd	xmm2, xmm3, 231         # xmm2 = xmm3[3,1,2,3]
	vpmovzxwq	xmm2, xmm2      # xmm2 = xmm2[0],zero,zero,zero,xmm2[1],zero,zero,zero
	vinserti128	ymm1, ymm1, xmm2, 1
	vpshufhw	xmm2, xmm7, 96  # xmm2 = xmm7[0,1,2,3,4,4,6,5]
	vpshufhw	xmm3, xmm7, 232 # xmm3 = xmm7[0,1,2,3,4,6,6,7]
	vinserti128	ymm2, ymm2, xmm3, 1
	vpshufd	ymm2, ymm2, 246         # ymm2 = ymm2[2,1,3,3,6,5,7,7]
	vpblendw	ymm1, ymm1, ymm2, 34 # ymm1 = ymm1[0],ymm2[1],ymm1[2,3,4],ymm2[5],ymm1[6,7,8],ymm2[9],ymm1[10,11,12],ymm2[13],ymm1[14,15]
	vpblendd	ymm0, ymm1, ymm0, 170 # ymm0 = ymm1[0],ymm0[1],ymm1[2],ymm0[3],ymm1[4],ymm0[5],ymm1[6],ymm0[7]
	vmovdqa	ymmword ptr [rsp + 32], ymm0 # 32-byte Spill
	vextracti128	xmm12, ymm12, 1
	vextracti128	xmm11, ymm11, 1
	vpunpcklbw	xmm1, xmm11, xmm12 # xmm1 = xmm11[0],xmm12[0],xmm11[1],xmm12[1],xmm11[2],xmm12[2],xmm11[3],xmm12[3],xmm11[4],xmm12[4],xmm11[5],xmm12[5],xmm11[6],xmm12[6],xmm11[7],xmm12[7]
	vpshuflw	xmm0, xmm1, 96  # xmm0 = xmm1[0,0,2,1,4,5,6,7]
	vpshuflw	xmm3, xmm1, 232 # xmm3 = xmm1[0,2,2,3,4,5,6,7]
	vinserti128	ymm3, ymm0, xmm3, 1
	vextracti128	xmm15, ymm9, 1
	vextracti128	xmm0, ymm8, 1
	vpunpcklbw	xmm2, xmm0, xmm15 # xmm2 = xmm0[0],xmm15[0],xmm0[1],xmm15[1],xmm0[2],xmm15[2],xmm0[3],xmm15[3],xmm0[4],xmm15[4],xmm0[5],xmm15[5],xmm0[6],xmm15[6],xmm0[7],xmm15[7]
	vpshuflw	xmm4, xmm2, 212 # xmm4 = xmm2[0,1,1,3,4,5,6,7]
	vpshuflw	xmm5, xmm2, 246 # xmm5 = xmm2[2,1,3,3,4,5,6,7]
	vinserti128	ymm4, ymm4, xmm5, 1
	vpshufd	ymm3, ymm3, 96          # ymm3 = ymm3[0,0,2,1,4,4,6,5]
	vpshufd	ymm4, ymm4, 96          # ymm4 = ymm4[0,0,2,1,4,4,6,5]
	vpblendw	ymm8, ymm4, ymm3, 136 # ymm8 = ymm4[0,1,2],ymm3[3],ymm4[4,5,6],ymm3[7],ymm4[8,9,10],ymm3[11],ymm4[12,13,14],ymm3[15]
	vextracti128	xmm13, ymm13, 1
	vextracti128	xmm6, ymm6, 1
	vpunpcklbw	xmm7, xmm13, xmm6 # xmm7 = xmm13[0],xmm6[0],xmm13[1],xmm6[1],xmm13[2],xmm6[2],xmm13[3],xmm6[3],xmm13[4],xmm6[4],xmm13[5],xmm6[5],xmm13[6],xmm6[6],xmm13[7],xmm6[7]
	vpshufd	xmm3, xmm7, 229         # xmm3 = xmm7[1,1,2,3]
	vpmovzxwq	xmm3, xmm3      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	vpmovzxwq	xmm4, xmm7      # xmm4 = xmm7[0],zero,zero,zero,xmm7[1],zero,zero,zero
	vinserti128	ymm9, ymm4, xmm3, 1
	vextracti128	xmm4, ymm14, 1
	vmovdqa	ymm3, ymmword ptr [rsp] # 32-byte Reload
	vextracti128	xmm3, ymm3, 1
	vpunpcklbw	xmm5, xmm3, xmm4 # xmm5 = xmm3[0],xmm4[0],xmm3[1],xmm4[1],xmm3[2],xmm4[2],xmm3[3],xmm4[3],xmm3[4],xmm4[4],xmm3[5],xmm4[5],xmm3[6],xmm4[6],xmm3[7],xmm4[7]
	vpshuflw	xmm14, xmm5, 96 # xmm14 = xmm5[0,0,2,1,4,5,6,7]
	vpshuflw	xmm10, xmm5, 232 # xmm10 = xmm5[0,2,2,3,4,5,6,7]
	vinserti128	ymm10, ymm14, xmm10, 1
	vpshufd	ymm10, ymm10, 212       # ymm10 = ymm10[0,1,1,3,4,5,5,7]
	vpblendw	ymm9, ymm9, ymm10, 34 # ymm9 = ymm9[0],ymm10[1],ymm9[2,3,4],ymm10[5],ymm9[6,7,8],ymm10[9],ymm9[10,11,12],ymm10[13],ymm9[14,15]
	vpblendd	ymm9, ymm9, ymm8, 170 # ymm9 = ymm9[0],ymm8[1],ymm9[2],ymm8[3],ymm9[4],ymm8[5],ymm9[6],ymm8[7]
	vpshufhw	xmm8, xmm1, 96  # xmm8 = xmm1[0,1,2,3,4,4,6,5]
	vpshufhw	xmm1, xmm1, 232 # xmm1 = xmm1[0,1,2,3,4,6,6,7]
	vinserti128	ymm1, ymm8, xmm1, 1
	vpshufhw	xmm8, xmm2, 212 # xmm8 = xmm2[0,1,2,3,4,5,5,7]
	vpshufhw	xmm2, xmm2, 246 # xmm2 = xmm2[0,1,2,3,6,5,7,7]
	vinserti128	ymm2, ymm8, xmm2, 1
	vpshufd	ymm1, ymm1, 232         # ymm1 = ymm1[0,2,2,3,4,6,6,7]
	vpshufd	ymm2, ymm2, 232         # ymm2 = ymm2[0,2,2,3,4,6,6,7]
	vpblendw	ymm1, ymm2, ymm1, 136 # ymm1 = ymm2[0,1,2],ymm1[3],ymm2[4,5,6],ymm1[7],ymm2[8,9,10],ymm1[11],ymm2[12,13,14],ymm1[15]
	vpshufd	xmm2, xmm7, 78          # xmm2 = xmm7[2,3,0,1]
	vpmovzxwq	xmm2, xmm2      # xmm2 = xmm2[0],zero,zero,zero,xmm2[1],zero,zero,zero
	vpshufd	xmm7, xmm7, 231         # xmm7 = xmm7[3,1,2,3]
	vpmovzxwq	xmm7, xmm7      # xmm7 = xmm7[0],zero,zero,zero,xmm7[1],zero,zero,zero
	vinserti128	ymm2, ymm2, xmm7, 1
	vpshufhw	xmm7, xmm5, 96  # xmm7 = xmm5[0,1,2,3,4,4,6,5]
	vpshufhw	xmm5, xmm5, 232 # xmm5 = xmm5[0,1,2,3,4,6,6,7]
	vinserti128	ymm5, ymm7, xmm5, 1
	vpshufd	ymm5, ymm5, 246         # ymm5 = ymm5[2,1,3,3,6,5,7,7]
	vpblendw	ymm2, ymm2, ymm5, 34 # ymm2 = ymm2[0],ymm5[1],ymm2[2,3,4],ymm5[5],ymm2[6,7,8],ymm5[9],ymm2[10,11,12],ymm5[13],ymm2[14,15]
	vpblendd	ymm8, ymm2, ymm1, 170 # ymm8 = ymm2[0],ymm1[1],ymm2[2],ymm1[3],ymm2[4],ymm1[5],ymm2[6],ymm1[7]
	vpunpckhbw	xmm1, xmm11, xmm12 # xmm1 = xmm11[8],xmm12[8],xmm11[9],xmm12[9],xmm11[10],xmm12[10],xmm11[11],xmm12[11],xmm11[12],xmm12[12],xmm11[13],xmm12[13],xmm11[14],xmm12[14],xmm11[15],xmm12[15]
	vpshuflw	xmm2, xmm1, 96  # xmm2 = xmm1[0,0,2,1,4,5,6,7]
	vpshuflw	xmm5, xmm1, 232 # xmm5 = xmm1[0,2,2,3,4,5,6,7]
	vinserti128	ymm2, ymm2, xmm5, 1
	vpunpckhbw	xmm0, xmm0, xmm15 # xmm0 = xmm0[8],xmm15[8],xmm0[9],xmm15[9],xmm0[10],xmm15[10],xmm0[11],xmm15[11],xmm0[12],xmm15[12],xmm0[13],xmm15[13],xmm0[14],xmm15[14],xmm0[15],xmm15[15]
	vpshuflw	xmm5, xmm0, 212 # xmm5 = xmm0[0,1,1,3,4,5,6,7]
	vpshuflw	xmm7, xmm0, 246 # xmm7 = xmm0[2,1,3,3,4,5,6,7]
	vinserti128	ymm5, ymm5, xmm7, 1
	vpshufd	ymm2, ymm2, 96          # ymm2 = ymm2[0,0,2,1,4,4,6,5]
	vpshufd	ymm5, ymm5, 96          # ymm5 = ymm5[0,0,2,1,4,4,6,5]
	vpblendw	ymm2, ymm5, ymm2, 136 # ymm2 = ymm5[0,1,2],ymm2[3],ymm5[4,5,6],ymm2[7],ymm5[8,9,10],ymm2[11],ymm5[12,13,14],ymm2[15]
	vpunpckhbw	xmm5, xmm13, xmm6 # xmm5 = xmm13[8],xmm6[8],xmm13[9],xmm6[9],xmm13[10],xmm6[10],xmm13[11],xmm6[11],xmm13[12],xmm6[12],xmm13[13],xmm6[13],xmm13[14],xmm6[14],xmm13[15],xmm6[15]
	vpmovzxwq	xmm6, xmm5      # xmm6 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero
	vpshufd	xmm7, xmm5, 229         # xmm7 = xmm5[1,1,2,3]
	vpmovzxwq	xmm7, xmm7      # xmm7 = xmm7[0],zero,zero,zero,xmm7[1],zero,zero,zero
	vinserti128	ymm6, ymm6, xmm7, 1
	vpunpckhbw	xmm3, xmm3, xmm4 # xmm3 = xmm3[8],xmm4[8],xmm3[9],xmm4[9],xmm3[10],xmm4[10],xmm3[11],xmm4[11],xmm3[12],xmm4[12],xmm3[13],xmm4[13],xmm3[14],xmm4[14],xmm3[15],xmm4[15]
	vpshuflw	xmm4, xmm3, 96  # xmm4 = xmm3[0,0,2,1,4,5,6,7]
	vpshuflw	xmm7, xmm3, 232 # xmm7 = xmm3[0,2,2,3,4,5,6,7]
	vinserti128	ymm4, ymm4, xmm7, 1
	vpshufd	ymm4, ymm4, 212         # ymm4 = ymm4[0,1,1,3,4,5,5,7]
	vpblendw	ymm4, ymm6, ymm4, 34 # ymm4 = ymm6[0],ymm4[1],ymm6[2,3,4],ymm4[5],ymm6[6,7,8],ymm4[9],ymm6[10,11,12],ymm4[13],ymm6[14,15]
	vpblendd	ymm2, ymm4, ymm2, 170 # ymm2 = ymm4[0],ymm2[1],ymm4[2],ymm2[3],ymm4[4],ymm2[5],ymm4[6],ymm2[7]
	vpshufhw	xmm4, xmm1, 96  # xmm4 = xmm1[0,1,2,3,4,4,6,5]
	vpshufhw	xmm1, xmm1, 232 # xmm1 = xmm1[0,1,2,3,4,6,6,7]
	vinserti128	ymm1, ymm4, xmm1, 1
	vpshufhw	xmm4, xmm0, 212 # xmm4 = xmm0[0,1,2,3,4,5,5,7]
	vpshufhw	xmm0, xmm0, 246 # xmm0 = xmm0[0,1,2,3,6,5,7,7]
	vinserti128	ymm0, ymm4, xmm0, 1
	vpshufd	ymm1, ymm1, 232         # ymm1 = ymm1[0,2,2,3,4,6,6,7]
	vpshufd	ymm0, ymm0, 232         # ymm0 = ymm0[0,2,2,3,4,6,6,7]
	vpblendw	ymm0, ymm0, ymm1, 136 # ymm0 = ymm0[0,1,2],ymm1[3],ymm0[4,5,6],ymm1[7],ymm0[8,9,10],ymm1[11],ymm0[12,13,14],ymm1[15]
	vpshufd	xmm1, xmm5, 78          # xmm1 = xmm5[2,3,0,1]
	vpmovzxwq	xmm1, xmm1      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	vpshufd	xmm4, xmm5, 231         # xmm4 = xmm5[3,1,2,3]
	vpmovzxwq	xmm4, xmm4      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero
	vinserti128	ymm1, ymm1, xmm4, 1
	vpshufhw	xmm4, xmm3, 96  # xmm4 = xmm3[0,1,2,3,4,4,6,5]
	vpshufhw	xmm3, xmm3, 232 # xmm3 = xmm3[0,1,2,3,4,6,6,7]
	vinserti128	ymm3, ymm4, xmm3, 1
	vpshufd	ymm3, ymm3, 246         # ymm3 = ymm3[2,1,3,3,6,5,7,7]
	vpblendw	ymm1, ymm1, ymm3, 34 # ymm1 = ymm1[0],ymm3[1],ymm1[2,3,4],ymm3[5],ymm1[6,7,8],ymm3[9],ymm1[10,11,12],ymm3[13],ymm1[14,15]
	vpblendd	ymm0, ymm1, ymm0, 170 # ymm0 = ymm1[0],ymm0[1],ymm1[2],ymm0[3],ymm1[4],ymm0[5],ymm1[6],ymm0[7]
	mov	esi, r9d
	and	esi, -256
	vmovdqu	ymmword ptr [rdx + rsi + 224], ymm0
	vmovdqu	ymmword ptr [rdx + rsi + 192], ymm2
	vmovdqu	ymmword ptr [rdx + rsi + 160], ymm8
	vmovdqu	ymmword ptr [rdx + rsi + 128], ymm9
	vmovaps	ymm0, ymmword ptr [rsp + 32] # 32-byte Reload
	vmovups	ymmword ptr [rdx + rsi + 96], ymm0
	vmovaps	ymm0, ymmword ptr [rsp + 64] # 32-byte Reload
	vmovups	ymmword ptr [rdx + rsi + 64], ymm0
	vmovaps	ymm0, ymmword ptr [rsp + 96] # 32-byte Reload
	vmovups	ymmword ptr [rdx + rsi + 32], ymm0
	vmovaps	ymm0, ymmword ptr [rsp + 128] # 32-byte Reload
	vmovups	ymmword ptr [rdx + rsi], ymm0
	add	rax, 32
	add	r9, 256
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
	vzeroupper
	ret
.Lfunc_end0:
	.size	bytes_to_bools_avx2, .Lfunc_end0-bytes_to_bools_avx2
                                        # -- End function
	.ident	"clang version 10.0.0-4ubuntu1 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
