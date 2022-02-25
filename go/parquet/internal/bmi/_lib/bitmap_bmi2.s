	.text
	.intel_syntax noprefix
	.file	"bitmap_bmi2.c"
	.globl	extract_bits_bmi2                    # -- Begin function extract_bits_bmi2
	.p2align	4, 0x90
	.type	extract_bits_bmi2,@function
extract_bits_bmi2:                           # @extract_bits_bmi2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	pext	rax, rdi, rsi
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	extract_bits_bmi2, .Lfunc_end0-extract_bits_bmi2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function levels_to_bitmap_bmi2
.LCPI1_0:
	.quad	0                               # 0x0
	.quad	1                               # 0x1
	.quad	2                               # 0x2
	.quad	3                               # 0x3
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3
.LCPI1_1:
	.quad	4                               # 0x4
.LCPI1_2:
	.quad	8                               # 0x8
.LCPI1_3:
	.quad	12                              # 0xc
.LCPI1_4:
	.quad	1                               # 0x1
.LCPI1_5:
	.quad	16                              # 0x10
	.text
	.globl	levels_to_bitmap_bmi2
	.p2align	4, 0x90
	.type	levels_to_bitmap_bmi2,@function
levels_to_bitmap_bmi2:                       # @levels_to_bitmap_bmi2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB1_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 15
	ja	.LBB1_4
# %bb.3:
	xor	esi, esi
	xor	eax, eax
	jmp	.LBB1_7
.LBB1_1:
	xor	eax, eax
	jmp	.LBB1_8
.LBB1_4:
	mov	esi, r8d
	and	esi, -16
	vmovd	xmm0, edx
	vpbroadcastw	xmm1, xmm0
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [0,1,2,3]
	vpbroadcastq	ymm12, qword ptr [rip + .LCPI1_1] # ymm12 = [4,4,4,4]
	vpbroadcastq	ymm4, qword ptr [rip + .LCPI1_2] # ymm4 = [8,8,8,8]
	vpbroadcastq	ymm5, qword ptr [rip + .LCPI1_3] # ymm5 = [12,12,12,12]
	vpbroadcastq	ymm6, qword ptr [rip + .LCPI1_4] # ymm6 = [1,1,1,1]
	vpbroadcastq	ymm7, qword ptr [rip + .LCPI1_5] # ymm7 = [16,16,16,16]
	xor	eax, eax
	vpxor	xmm8, xmm8, xmm8
	vpxor	xmm9, xmm9, xmm9
	vpxor	xmm10, xmm10, xmm10
	.p2align	4, 0x90
.LBB1_5:                                # =>This Inner Loop Header: Depth=1
	vpaddq	ymm11, ymm12, ymm2
	vmovq	xmm3, qword ptr [rdi + 2*rax + 8] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm11
	vpaddq	ymm11, ymm2, ymm4
	vpor	ymm8, ymm8, ymm3
	vmovq	xmm3, qword ptr [rdi + 2*rax + 16] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm11
	vpaddq	ymm11, ymm2, ymm5
	vpor	ymm9, ymm9, ymm3
	vmovq	xmm3, qword ptr [rdi + 2*rax + 24] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm11
	vpor	ymm10, ymm10, ymm3
	vmovq	xmm3, qword ptr [rdi + 2*rax]   # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm2
	vpor	ymm0, ymm3, ymm0
	add	rax, 16
	vpaddq	ymm2, ymm2, ymm7
	cmp	rsi, rax
	jne	.LBB1_5
# %bb.6:
	vpor	ymm0, ymm8, ymm0
	vpor	ymm0, ymm9, ymm0
	vpor	ymm0, ymm10, ymm0
	vextracti128	xmm1, ymm0, 1
	vpor	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	vpor	xmm0, xmm0, xmm1
	vmovq	rax, xmm0
	cmp	rsi, r8
	je	.LBB1_8
	.p2align	4, 0x90
.LBB1_7:                                # =>This Inner Loop Header: Depth=1
	xor	ecx, ecx
	cmp	word ptr [rdi + 2*rsi], dx
	setg	cl
	shlx	rcx, rcx, rsi
	or	rax, rcx
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB1_7
.LBB1_8:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end1:
	.size	levels_to_bitmap_bmi2, .Lfunc_end1-levels_to_bitmap_bmi2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-++20210204121720+1fdec59bffc1-1~exp1~20210203232336.162"
	.section	".note.GNU-stack","",@progbits
	.addrsig
