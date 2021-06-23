	.text
	.intel_syntax noprefix
	.file	"min_max.c"
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2                               # -- Begin function int32_max_min_avx2
.LCPI0_0:
	.long	2147483648                      # 0x80000000
.LCPI0_1:
	.long	2147483647                      # 0x7fffffff
	.text
	.globl	int32_max_min_avx2
	.p2align	4, 0x90
	.type	int32_max_min_avx2,@function
int32_max_min_avx2:                     # @int32_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -32
	sub	rsp, 64
	test	esi, esi
	jle	.LBB0_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 31
	ja	.LBB0_6
# %bb.3:
	mov	eax, -2147483648
	mov	r9d, 2147483647
	xor	r11d, r11d
	jmp	.LBB0_4
.LBB0_1:
	mov	r9d, 2147483647
	mov	eax, -2147483648
	jmp	.LBB0_14
.LBB0_6:
	mov	r11d, r8d
	and	r11d, -32
	lea	rax, [r11 - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB0_8
# %bb.7:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_0] # ymm0 = [2147483648,2147483648,2147483648,2147483648,2147483648,2147483648,2147483648,2147483648]
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI0_1] # ymm1 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	xor	eax, eax
	vmovdqa	ymm2, ymm1
	vmovdqa	ymm4, ymm1
	vmovdqa	ymm6, ymm1
	vmovdqa	ymm3, ymm0
	vmovdqa	ymm5, ymm0
	vmovdqa	ymm7, ymm0
	jmp	.LBB0_10
.LBB0_8:
	and	r10, -4
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_0] # ymm0 = [2147483648,2147483648,2147483648,2147483648,2147483648,2147483648,2147483648,2147483648]
	neg	r10
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI0_1] # ymm1 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	xor	eax, eax
	vmovdqa	ymm2, ymm1
	vmovdqa	ymm4, ymm1
	vmovdqa	ymm6, ymm1
	vmovdqa	ymm3, ymm0
	vmovdqa	ymm5, ymm0
	vmovdqa	ymm7, ymm0
	.p2align	4, 0x90
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax]
	vmovdqu	ymm9, ymmword ptr [rdi + 4*rax + 32]
	vmovdqu	ymm10, ymmword ptr [rdi + 4*rax + 64]
	vmovdqu	ymm11, ymmword ptr [rdi + 4*rax + 96]
	vpminsd	ymm6, ymm6, ymm11
	vpminsd	ymm4, ymm4, ymm10
	vpminsd	ymm1, ymm1, ymm8
	vpminsd	ymm2, ymm2, ymm9
	vpmaxsd	ymm7, ymm7, ymm11
	vpmaxsd	ymm5, ymm5, ymm10
	vpmaxsd	ymm0, ymm0, ymm8
	vpmaxsd	ymm3, ymm3, ymm9
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax + 224]
	vmovdqu	ymm9, ymmword ptr [rdi + 4*rax + 192]
	vmovdqu	ymm10, ymmword ptr [rdi + 4*rax + 128]
	vmovdqu	ymm11, ymmword ptr [rdi + 4*rax + 160]
	vmovdqu	ymm12, ymmword ptr [rdi + 4*rax + 256]
	vmovdqu	ymm13, ymmword ptr [rdi + 4*rax + 320]
	vmovdqu	ymm14, ymmword ptr [rdi + 4*rax + 352]
	vpminsd	ymm15, ymm8, ymm14
	vpminsd	ymm6, ymm6, ymm15
	vmovdqa	ymmword ptr [rsp], ymm6         # 32-byte Spill
	vpminsd	ymm15, ymm9, ymm13
	vpminsd	ymm4, ymm4, ymm15
	vpminsd	ymm15, ymm10, ymm12
	vpminsd	ymm1, ymm1, ymm15
	vmovdqu	ymm15, ymmword ptr [rdi + 4*rax + 288]
	vpminsd	ymm6, ymm11, ymm15
	vpminsd	ymm2, ymm2, ymm6
	vpmaxsd	ymm6, ymm8, ymm14
	vpmaxsd	ymm7, ymm7, ymm6
	vpmaxsd	ymm6, ymm9, ymm13
	vpmaxsd	ymm5, ymm5, ymm6
	vpmaxsd	ymm6, ymm10, ymm12
	vpmaxsd	ymm0, ymm0, ymm6
	vpmaxsd	ymm6, ymm11, ymm15
	vpmaxsd	ymm3, ymm3, ymm6
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rax + 416]
	vpminsd	ymm2, ymm2, ymm6
	vpmaxsd	ymm3, ymm3, ymm6
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rax + 384]
	vpminsd	ymm1, ymm1, ymm6
	vpmaxsd	ymm0, ymm0, ymm6
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rax + 448]
	vpminsd	ymm4, ymm4, ymm6
	vpmaxsd	ymm5, ymm5, ymm6
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax + 480]
	vpminsd	ymm6, ymm8, ymmword ptr [rsp]   # 32-byte Folded Reload
	vpmaxsd	ymm7, ymm7, ymm8
	sub	rax, -128
	add	r10, 4
	jne	.LBB0_9
.LBB0_10:
	test	r9, r9
	je	.LBB0_13
# %bb.11:
	lea	rax, [rdi + 4*rax]
	neg	r9
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rax]
	vmovdqu	ymm9, ymmword ptr [rax + 32]
	vmovdqu	ymm10, ymmword ptr [rax + 64]
	vmovdqu	ymm11, ymmword ptr [rax + 96]
	vpminsd	ymm2, ymm2, ymm9
	vpminsd	ymm1, ymm1, ymm8
	vpminsd	ymm4, ymm4, ymm10
	vpminsd	ymm6, ymm6, ymm11
	vpmaxsd	ymm3, ymm3, ymm9
	vpmaxsd	ymm0, ymm0, ymm8
	vpmaxsd	ymm5, ymm5, ymm10
	vpmaxsd	ymm7, ymm7, ymm11
	sub	rax, -128
	inc	r9
	jne	.LBB0_12
.LBB0_13:
	vpminsd	ymm2, ymm2, ymm6
	vpminsd	ymm1, ymm1, ymm4
	vpminsd	ymm1, ymm1, ymm2
	vpmaxsd	ymm2, ymm3, ymm7
	vpmaxsd	ymm0, ymm0, ymm5
	vpmaxsd	ymm0, ymm0, ymm2
	vextracti128	xmm2, ymm0, 1
	vpmaxsd	xmm0, xmm0, xmm2
	vpshufd	xmm2, xmm0, 78                  # xmm2 = xmm0[2,3,0,1]
	vpmaxsd	xmm0, xmm0, xmm2
	vpshufd	xmm2, xmm0, 229                 # xmm2 = xmm0[1,1,2,3]
	vpmaxsd	xmm0, xmm0, xmm2
	vmovd	eax, xmm0
	vextracti128	xmm0, ymm1, 1
	vpminsd	xmm0, xmm1, xmm0
	vpshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	vpminsd	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 229                 # xmm1 = xmm0[1,1,2,3]
	vpminsd	xmm0, xmm0, xmm1
	vmovd	r9d, xmm0
	cmp	r11, r8
	je	.LBB0_14
.LBB0_4:
	mov	esi, eax
	.p2align	4, 0x90
.LBB0_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*r11]
	cmp	r9d, eax
	cmovg	r9d, eax
	cmp	esi, eax
	cmovge	eax, esi
	add	r11, 1
	mov	esi, eax
	cmp	r8, r11
	jne	.LBB0_5
.LBB0_14:
	mov	dword ptr [rcx], eax
	mov	dword ptr [rdx], r9d
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end0:
	.size	int32_max_min_avx2, .Lfunc_end0-int32_max_min_avx2
                                        # -- End function
	.globl	uint32_max_min_avx2             # -- Begin function uint32_max_min_avx2
	.p2align	4, 0x90
	.type	uint32_max_min_avx2,@function
uint32_max_min_avx2:                    # @uint32_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -32
	sub	rsp, 64
	test	esi, esi
	jle	.LBB1_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 31
	ja	.LBB1_6
# %bb.3:
	xor	r11d, r11d
	mov	r9d, -1
	xor	esi, esi
	jmp	.LBB1_4
.LBB1_1:
	mov	r9d, -1
	xor	esi, esi
	jmp	.LBB1_14
.LBB1_6:
	mov	r11d, r8d
	and	r11d, -32
	lea	rax, [r11 - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB1_8
# %bb.7:
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpcmpeqd	ymm2, ymm2, ymm2
	vpcmpeqd	ymm4, ymm4, ymm4
	vpcmpeqd	ymm6, ymm6, ymm6
	vpxor	xmm3, xmm3, xmm3
	vpxor	xmm5, xmm5, xmm5
	vpxor	xmm7, xmm7, xmm7
	jmp	.LBB1_10
.LBB1_8:
	and	r10, -4
	neg	r10
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpcmpeqd	ymm2, ymm2, ymm2
	vpcmpeqd	ymm4, ymm4, ymm4
	vpcmpeqd	ymm6, ymm6, ymm6
	vpxor	xmm3, xmm3, xmm3
	vpxor	xmm5, xmm5, xmm5
	vpxor	xmm7, xmm7, xmm7
	.p2align	4, 0x90
.LBB1_9:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax]
	vmovdqu	ymm9, ymmword ptr [rdi + 4*rax + 32]
	vmovdqu	ymm10, ymmword ptr [rdi + 4*rax + 64]
	vmovdqu	ymm11, ymmword ptr [rdi + 4*rax + 96]
	vpminud	ymm6, ymm6, ymm11
	vpminud	ymm4, ymm4, ymm10
	vpminud	ymm1, ymm1, ymm8
	vpminud	ymm2, ymm2, ymm9
	vpmaxud	ymm7, ymm7, ymm11
	vpmaxud	ymm5, ymm5, ymm10
	vpmaxud	ymm0, ymm0, ymm8
	vpmaxud	ymm3, ymm3, ymm9
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax + 224]
	vmovdqu	ymm9, ymmword ptr [rdi + 4*rax + 192]
	vmovdqu	ymm10, ymmword ptr [rdi + 4*rax + 128]
	vmovdqu	ymm11, ymmword ptr [rdi + 4*rax + 160]
	vmovdqu	ymm12, ymmword ptr [rdi + 4*rax + 256]
	vmovdqu	ymm13, ymmword ptr [rdi + 4*rax + 320]
	vmovdqu	ymm14, ymmword ptr [rdi + 4*rax + 352]
	vpminud	ymm15, ymm8, ymm14
	vpminud	ymm6, ymm6, ymm15
	vmovdqa	ymmword ptr [rsp], ymm6         # 32-byte Spill
	vpminud	ymm15, ymm9, ymm13
	vpminud	ymm4, ymm4, ymm15
	vpminud	ymm15, ymm10, ymm12
	vpminud	ymm1, ymm1, ymm15
	vmovdqu	ymm15, ymmword ptr [rdi + 4*rax + 288]
	vpminud	ymm6, ymm11, ymm15
	vpminud	ymm2, ymm2, ymm6
	vpmaxud	ymm6, ymm8, ymm14
	vpmaxud	ymm7, ymm7, ymm6
	vpmaxud	ymm6, ymm9, ymm13
	vpmaxud	ymm5, ymm5, ymm6
	vpmaxud	ymm6, ymm10, ymm12
	vpmaxud	ymm0, ymm0, ymm6
	vpmaxud	ymm6, ymm11, ymm15
	vpmaxud	ymm3, ymm3, ymm6
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rax + 416]
	vpminud	ymm2, ymm2, ymm6
	vpmaxud	ymm3, ymm3, ymm6
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rax + 384]
	vpminud	ymm1, ymm1, ymm6
	vpmaxud	ymm0, ymm0, ymm6
	vmovdqu	ymm6, ymmword ptr [rdi + 4*rax + 448]
	vpminud	ymm4, ymm4, ymm6
	vpmaxud	ymm5, ymm5, ymm6
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax + 480]
	vpminud	ymm6, ymm8, ymmword ptr [rsp]   # 32-byte Folded Reload
	vpmaxud	ymm7, ymm7, ymm8
	sub	rax, -128
	add	r10, 4
	jne	.LBB1_9
.LBB1_10:
	test	r9, r9
	je	.LBB1_13
# %bb.11:
	lea	rax, [rdi + 4*rax]
	neg	r9
	.p2align	4, 0x90
.LBB1_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rax]
	vmovdqu	ymm9, ymmword ptr [rax + 32]
	vmovdqu	ymm10, ymmword ptr [rax + 64]
	vmovdqu	ymm11, ymmword ptr [rax + 96]
	vpminud	ymm2, ymm2, ymm9
	vpminud	ymm1, ymm1, ymm8
	vpminud	ymm4, ymm4, ymm10
	vpminud	ymm6, ymm6, ymm11
	vpmaxud	ymm3, ymm3, ymm9
	vpmaxud	ymm0, ymm0, ymm8
	vpmaxud	ymm5, ymm5, ymm10
	vpmaxud	ymm7, ymm7, ymm11
	sub	rax, -128
	inc	r9
	jne	.LBB1_12
.LBB1_13:
	vpminud	ymm2, ymm2, ymm6
	vpminud	ymm1, ymm1, ymm4
	vpminud	ymm1, ymm1, ymm2
	vpmaxud	ymm2, ymm3, ymm7
	vpmaxud	ymm0, ymm0, ymm5
	vpmaxud	ymm0, ymm0, ymm2
	vextracti128	xmm2, ymm0, 1
	vpmaxud	xmm0, xmm0, xmm2
	vpshufd	xmm2, xmm0, 78                  # xmm2 = xmm0[2,3,0,1]
	vpmaxud	xmm0, xmm0, xmm2
	vpshufd	xmm2, xmm0, 229                 # xmm2 = xmm0[1,1,2,3]
	vpmaxud	xmm0, xmm0, xmm2
	vmovd	esi, xmm0
	vextracti128	xmm0, ymm1, 1
	vpminud	xmm0, xmm1, xmm0
	vpshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	vpminud	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 229                 # xmm1 = xmm0[1,1,2,3]
	vpminud	xmm0, xmm0, xmm1
	vmovd	r9d, xmm0
	cmp	r11, r8
	je	.LBB1_14
.LBB1_4:
	mov	eax, esi
	.p2align	4, 0x90
.LBB1_5:                                # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdi + 4*r11]
	cmp	r9d, esi
	cmovae	r9d, esi
	cmp	eax, esi
	cmova	esi, eax
	add	r11, 1
	mov	eax, esi
	cmp	r8, r11
	jne	.LBB1_5
.LBB1_14:
	mov	dword ptr [rcx], esi
	mov	dword ptr [rdx], r9d
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end1:
	.size	uint32_max_min_avx2, .Lfunc_end1-uint32_max_min_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function int64_max_min_avx2
.LCPI2_0:
	.quad	-9223372036854775808            # 0x8000000000000000
.LCPI2_1:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.text
	.globl	int64_max_min_avx2
	.p2align	4, 0x90
	.type	int64_max_min_avx2,@function
int64_max_min_avx2:                     # @int64_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -32
	sub	rsp, 224
	movabs	r9, 9223372036854775807
	test	esi, esi
	jle	.LBB2_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 31
	ja	.LBB2_6
# %bb.3:
	lea	rsi, [r9 + 1]
	xor	r11d, r11d
	jmp	.LBB2_4
.LBB2_1:
	lea	rsi, [r9 + 1]
	jmp	.LBB2_14
.LBB2_6:
	mov	r11d, r8d
	and	r11d, -32
	lea	rax, [r11 - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB2_8
# %bb.7:
	vpbroadcastq	ymm15, qword ptr [rip + .LCPI2_0] # ymm15 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	vpbroadcastq	ymm11, qword ptr [rip + .LCPI2_1] # ymm11 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	xor	eax, eax
	vmovdqa	ymmword ptr [rsp + 32], ymm11   # 32-byte Spill
	vmovdqa	ymm3, ymm11
	vmovdqa	ymm9, ymm11
	vmovdqa	ymm5, ymm11
	vmovdqa	ymm4, ymm11
	vmovdqa	ymm6, ymm11
	vmovdqa	ymmword ptr [rsp + 96], ymm11   # 32-byte Spill
	vmovdqa	ymmword ptr [rsp + 64], ymm15   # 32-byte Spill
	vmovdqa	ymm2, ymm15
	vmovdqa	ymm8, ymm15
	vmovdqa	ymm12, ymm15
	vmovdqa	ymm13, ymm15
	vmovdqa	ymm14, ymm15
	vmovdqa	ymmword ptr [rsp], ymm15        # 32-byte Spill
	jmp	.LBB2_10
.LBB2_8:
	and	r10, -4
	vpbroadcastq	ymm15, qword ptr [rip + .LCPI2_0] # ymm15 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	neg	r10
	vpbroadcastq	ymm11, qword ptr [rip + .LCPI2_1] # ymm11 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	xor	eax, eax
	vmovdqa	ymmword ptr [rsp + 32], ymm11   # 32-byte Spill
	vmovdqa	ymm3, ymm11
	vmovdqa	ymm9, ymm11
	vmovdqa	ymm5, ymm11
	vmovdqa	ymm4, ymm11
	vmovdqa	ymm6, ymm11
	vmovdqa	ymmword ptr [rsp + 96], ymm11   # 32-byte Spill
	vmovdqa	ymmword ptr [rsp + 64], ymm15   # 32-byte Spill
	vmovdqa	ymm2, ymm15
	vmovdqa	ymm8, ymm15
	vmovdqa	ymm12, ymm15
	vmovdqa	ymm13, ymm15
	vmovdqa	ymm14, ymm15
	vmovdqa	ymmword ptr [rsp], ymm15        # 32-byte Spill
	.p2align	4, 0x90
.LBB2_9:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 224]
	vmovdqa	ymm10, ymm8
	vmovdqa	ymm8, ymm2
	vmovdqa	ymm2, ymm3
	vmovdqa	ymm3, ymm9
	vpcmpgtq	ymm9, ymm0, ymm11
	vblendvpd	ymm1, ymm0, ymm11, ymm9
	vmovapd	ymmword ptr [rsp + 160], ymm1   # 32-byte Spill
	vpcmpgtq	ymm9, ymm15, ymm0
	vblendvpd	ymm0, ymm0, ymm15, ymm9
	vmovapd	ymmword ptr [rsp + 128], ymm0   # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 192]
	vpcmpgtq	ymm9, ymm0, ymm6
	vblendvpd	ymm7, ymm0, ymm6, ymm9
	vpcmpgtq	ymm9, ymm14, ymm0
	vblendvpd	ymm14, ymm0, ymm14, ymm9
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 160]
	vpcmpgtq	ymm9, ymm0, ymm4
	vblendvpd	ymm6, ymm0, ymm4, ymm9
	vpcmpgtq	ymm9, ymm13, ymm0
	vblendvpd	ymm13, ymm0, ymm13, ymm9
	vmovdqu	ymm9, ymmword ptr [rdi + 8*rax + 128]
	vpcmpgtq	ymm0, ymm9, ymm5
	vblendvpd	ymm1, ymm9, ymm5, ymm0
	vpcmpgtq	ymm5, ymm12, ymm9
	vblendvpd	ymm12, ymm9, ymm12, ymm5
	vmovdqu	ymm5, ymmword ptr [rdi + 8*rax + 96]
	vpcmpgtq	ymm9, ymm5, ymm3
	vblendvpd	ymm9, ymm5, ymm3, ymm9
	vpcmpgtq	ymm4, ymm10, ymm5
	vblendvpd	ymm10, ymm5, ymm10, ymm4
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 64]
	vpcmpgtq	ymm5, ymm4, ymm2
	vblendvpd	ymm5, ymm4, ymm2, ymm5
	vpcmpgtq	ymm3, ymm8, ymm4
	vblendvpd	ymm0, ymm4, ymm8, ymm3
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax]
	vmovdqa	ymm4, ymmword ptr [rsp + 96]    # 32-byte Reload
	vpcmpgtq	ymm3, ymm2, ymm4
	vblendvpd	ymm3, ymm2, ymm4, ymm3
	vmovdqa	ymm11, ymmword ptr [rsp]        # 32-byte Reload
	vpcmpgtq	ymm4, ymm11, ymm2
	vblendvpd	ymm4, ymm2, ymm11, ymm4
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 32]
	vmovdqa	ymm15, ymmword ptr [rsp + 32]   # 32-byte Reload
	vpcmpgtq	ymm11, ymm2, ymm15
	vblendvpd	ymm11, ymm2, ymm15, ymm11
	vmovdqa	ymm8, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpcmpgtq	ymm15, ymm8, ymm2
	vblendvpd	ymm2, ymm2, ymm8, ymm15
	vmovdqu	ymm8, ymmword ptr [rdi + 8*rax + 288]
	vpcmpgtq	ymm15, ymm8, ymm11
	vblendvpd	ymm11, ymm8, ymm11, ymm15
	vmovapd	ymmword ptr [rsp + 32], ymm11   # 32-byte Spill
	vpcmpgtq	ymm11, ymm2, ymm8
	vblendvpd	ymm2, ymm8, ymm2, ymm11
	vmovapd	ymmword ptr [rsp], ymm2         # 32-byte Spill
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 256]
	vpcmpgtq	ymm2, ymm11, ymm3
	vblendvpd	ymm8, ymm11, ymm3, ymm2
	vpcmpgtq	ymm3, ymm4, ymm11
	vblendvpd	ymm3, ymm11, ymm4, ymm3
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 320]
	vpcmpgtq	ymm4, ymm11, ymm5
	vblendvpd	ymm4, ymm11, ymm5, ymm4
	vpcmpgtq	ymm5, ymm0, ymm11
	vblendvpd	ymm5, ymm11, ymm0, ymm5
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 352]
	vpcmpgtq	ymm11, ymm0, ymm9
	vblendvpd	ymm9, ymm0, ymm9, ymm11
	vpcmpgtq	ymm11, ymm10, ymm0
	vblendvpd	ymm10, ymm0, ymm10, ymm11
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 384]
	vpcmpgtq	ymm0, ymm11, ymm1
	vblendvpd	ymm2, ymm11, ymm1, ymm0
	vpcmpgtq	ymm1, ymm12, ymm11
	vblendvpd	ymm12, ymm11, ymm12, ymm1
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 416]
	vpcmpgtq	ymm11, ymm1, ymm6
	vblendvpd	ymm6, ymm1, ymm6, ymm11
	vpcmpgtq	ymm11, ymm13, ymm1
	vblendvpd	ymm1, ymm1, ymm13, ymm11
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 448]
	vpcmpgtq	ymm13, ymm11, ymm7
	vblendvpd	ymm7, ymm11, ymm7, ymm13
	vpcmpgtq	ymm13, ymm14, ymm11
	vblendvpd	ymm13, ymm11, ymm14, ymm13
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 480]
	vmovdqa	ymm0, ymmword ptr [rsp + 160]   # 32-byte Reload
	vpcmpgtq	ymm14, ymm11, ymm0
	vblendvpd	ymm14, ymm11, ymm0, ymm14
	vmovdqa	ymm0, ymmword ptr [rsp + 128]   # 32-byte Reload
	vpcmpgtq	ymm15, ymm0, ymm11
	vblendvpd	ymm15, ymm11, ymm0, ymm15
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 736]
	vpcmpgtq	ymm11, ymm0, ymm14
	vblendvpd	ymm11, ymm0, ymm14, ymm11
	vmovapd	ymmword ptr [rsp + 160], ymm11  # 32-byte Spill
	vpcmpgtq	ymm14, ymm15, ymm0
	vblendvpd	ymm0, ymm0, ymm15, ymm14
	vmovapd	ymmword ptr [rsp + 128], ymm0   # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 704]
	vpcmpgtq	ymm14, ymm0, ymm7
	vblendvpd	ymm7, ymm0, ymm7, ymm14
	vpcmpgtq	ymm14, ymm13, ymm0
	vblendvpd	ymm14, ymm0, ymm13, ymm14
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 672]
	vpcmpgtq	ymm13, ymm0, ymm6
	vblendvpd	ymm6, ymm0, ymm6, ymm13
	vpcmpgtq	ymm13, ymm1, ymm0
	vblendvpd	ymm13, ymm0, ymm1, ymm13
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 640]
	vpcmpgtq	ymm0, ymm1, ymm2
	vblendvpd	ymm0, ymm1, ymm2, ymm0
	vpcmpgtq	ymm2, ymm12, ymm1
	vblendvpd	ymm12, ymm1, ymm12, ymm2
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 608]
	vpcmpgtq	ymm2, ymm1, ymm9
	vblendvpd	ymm9, ymm1, ymm9, ymm2
	vpcmpgtq	ymm2, ymm10, ymm1
	vblendvpd	ymm10, ymm1, ymm10, ymm2
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 576]
	vpcmpgtq	ymm2, ymm1, ymm4
	vblendvpd	ymm2, ymm1, ymm4, ymm2
	vpcmpgtq	ymm4, ymm5, ymm1
	vblendvpd	ymm1, ymm1, ymm5, ymm4
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 512]
	vpcmpgtq	ymm5, ymm4, ymm8
	vblendvpd	ymm5, ymm4, ymm8, ymm5
	vpcmpgtq	ymm8, ymm3, ymm4
	vblendvpd	ymm3, ymm4, ymm3, ymm8
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 544]
	vmovdqa	ymm11, ymmword ptr [rsp + 32]   # 32-byte Reload
	vpcmpgtq	ymm8, ymm4, ymm11
	vblendvpd	ymm8, ymm4, ymm11, ymm8
	vmovdqa	ymm15, ymmword ptr [rsp]        # 32-byte Reload
	vpcmpgtq	ymm11, ymm15, ymm4
	vblendvpd	ymm4, ymm4, ymm15, ymm11
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 800]
	vpcmpgtq	ymm15, ymm11, ymm8
	vblendvpd	ymm8, ymm11, ymm8, ymm15
	vmovapd	ymmword ptr [rsp + 32], ymm8    # 32-byte Spill
	vpcmpgtq	ymm8, ymm4, ymm11
	vblendvpd	ymm4, ymm11, ymm4, ymm8
	vmovapd	ymmword ptr [rsp + 64], ymm4    # 32-byte Spill
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 768]
	vpcmpgtq	ymm11, ymm4, ymm5
	vblendvpd	ymm5, ymm4, ymm5, ymm11
	vmovapd	ymmword ptr [rsp + 96], ymm5    # 32-byte Spill
	vpcmpgtq	ymm5, ymm3, ymm4
	vblendvpd	ymm3, ymm4, ymm3, ymm5
	vmovapd	ymmword ptr [rsp], ymm3         # 32-byte Spill
	vmovdqu	ymm4, ymmword ptr [rdi + 8*rax + 832]
	vpcmpgtq	ymm3, ymm4, ymm2
	vblendvpd	ymm3, ymm4, ymm2, ymm3
	vpcmpgtq	ymm2, ymm1, ymm4
	vblendvpd	ymm2, ymm4, ymm1, ymm2
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 864]
	vpcmpgtq	ymm4, ymm1, ymm9
	vblendvpd	ymm9, ymm1, ymm9, ymm4
	vpcmpgtq	ymm5, ymm10, ymm1
	vblendvpd	ymm8, ymm1, ymm10, ymm5
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 896]
	vpcmpgtq	ymm5, ymm1, ymm0
	vblendvpd	ymm5, ymm1, ymm0, ymm5
	vpcmpgtq	ymm0, ymm12, ymm1
	vblendvpd	ymm12, ymm1, ymm12, ymm0
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 928]
	vpcmpgtq	ymm1, ymm0, ymm6
	vblendvpd	ymm4, ymm0, ymm6, ymm1
	vpcmpgtq	ymm1, ymm13, ymm0
	vblendvpd	ymm13, ymm0, ymm13, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 960]
	vpcmpgtq	ymm1, ymm0, ymm7
	vblendvpd	ymm6, ymm0, ymm7, ymm1
	vpcmpgtq	ymm1, ymm14, ymm0
	vblendvpd	ymm14, ymm0, ymm14, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 992]
	vmovdqa	ymm7, ymmword ptr [rsp + 160]   # 32-byte Reload
	vpcmpgtq	ymm1, ymm0, ymm7
	vblendvpd	ymm11, ymm0, ymm7, ymm1
	vmovdqa	ymm7, ymmword ptr [rsp + 128]   # 32-byte Reload
	vpcmpgtq	ymm1, ymm7, ymm0
	vblendvpd	ymm15, ymm0, ymm7, ymm1
	sub	rax, -128
	add	r10, 4
	jne	.LBB2_9
.LBB2_10:
	test	r9, r9
	vmovdqa	ymm7, ymm5
	vmovdqa	ymm5, ymm9
	vmovdqa	ymm9, ymmword ptr [rsp + 96]    # 32-byte Reload
	vmovdqa	ymm10, ymm3
	je	.LBB2_13
# %bb.11:
	lea	rax, [rdi + 8*rax]
	neg	r9
	.p2align	4, 0x90
.LBB2_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rax + 32]
	vmovdqa	ymm3, ymmword ptr [rsp + 32]    # 32-byte Reload
	vpcmpgtq	ymm1, ymm0, ymm3
	vblendvpd	ymm3, ymm0, ymm3, ymm1
	vmovapd	ymmword ptr [rsp + 32], ymm3    # 32-byte Spill
	vmovdqa	ymm3, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpcmpgtq	ymm1, ymm3, ymm0
	vblendvpd	ymm3, ymm0, ymm3, ymm1
	vmovapd	ymmword ptr [rsp + 64], ymm3    # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rax]
	vpcmpgtq	ymm1, ymm0, ymm9
	vblendvpd	ymm9, ymm0, ymm9, ymm1
	vmovdqa	ymm3, ymmword ptr [rsp]         # 32-byte Reload
	vpcmpgtq	ymm1, ymm3, ymm0
	vblendvpd	ymm3, ymm0, ymm3, ymm1
	vmovapd	ymmword ptr [rsp], ymm3         # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rax + 64]
	vpcmpgtq	ymm1, ymm0, ymm10
	vblendvpd	ymm10, ymm0, ymm10, ymm1
	vpcmpgtq	ymm1, ymm2, ymm0
	vblendvpd	ymm2, ymm0, ymm2, ymm1
	vmovdqu	ymm0, ymmword ptr [rax + 96]
	vpcmpgtq	ymm1, ymm0, ymm5
	vblendvpd	ymm5, ymm0, ymm5, ymm1
	vpcmpgtq	ymm1, ymm8, ymm0
	vblendvpd	ymm8, ymm0, ymm8, ymm1
	vmovdqu	ymm0, ymmword ptr [rax + 128]
	vpcmpgtq	ymm1, ymm0, ymm7
	vblendvpd	ymm7, ymm0, ymm7, ymm1
	vpcmpgtq	ymm1, ymm12, ymm0
	vblendvpd	ymm12, ymm0, ymm12, ymm1
	vmovdqu	ymm0, ymmword ptr [rax + 160]
	vpcmpgtq	ymm1, ymm0, ymm4
	vblendvpd	ymm4, ymm0, ymm4, ymm1
	vpcmpgtq	ymm1, ymm13, ymm0
	vblendvpd	ymm13, ymm0, ymm13, ymm1
	vmovdqu	ymm0, ymmword ptr [rax + 192]
	vpcmpgtq	ymm1, ymm0, ymm6
	vblendvpd	ymm6, ymm0, ymm6, ymm1
	vpcmpgtq	ymm1, ymm14, ymm0
	vblendvpd	ymm14, ymm0, ymm14, ymm1
	vmovdqu	ymm0, ymmword ptr [rax + 224]
	vpcmpgtq	ymm1, ymm0, ymm11
	vblendvpd	ymm11, ymm0, ymm11, ymm1
	vpcmpgtq	ymm1, ymm15, ymm0
	vblendvpd	ymm15, ymm0, ymm15, ymm1
	add	rax, 256
	inc	r9
	jne	.LBB2_12
.LBB2_13:
	vmovdqa	ymm1, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpcmpgtq	ymm0, ymm1, ymm13
	vblendvpd	ymm0, ymm13, ymm1, ymm0
	vpcmpgtq	ymm1, ymm8, ymm15
	vblendvpd	ymm1, ymm15, ymm8, ymm1
	vmovdqa	ymm3, ymmword ptr [rsp]         # 32-byte Reload
	vpcmpgtq	ymm8, ymm3, ymm12
	vblendvpd	ymm8, ymm12, ymm3, ymm8
	vmovdqa	ymm3, ymm9
	vpcmpgtq	ymm9, ymm2, ymm14
	vblendvpd	ymm2, ymm14, ymm2, ymm9
	vpcmpgtq	ymm9, ymm8, ymm2
	vblendvpd	ymm2, ymm2, ymm8, ymm9
	vpcmpgtq	ymm8, ymm0, ymm1
	vblendvpd	ymm0, ymm1, ymm0, ymm8
	vpcmpgtq	ymm1, ymm2, ymm0
	vblendvpd	ymm0, ymm0, ymm2, ymm1
	vextractf128	xmm1, ymm0, 1
	vpcmpgtq	xmm2, xmm0, xmm1
	vblendvpd	xmm0, xmm1, xmm0, xmm2
	vpermilps	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	vpcmpgtq	xmm2, xmm0, xmm1
	vblendvpd	xmm0, xmm1, xmm0, xmm2
	vmovdqa	ymm2, ymmword ptr [rsp + 32]    # 32-byte Reload
	vpcmpgtq	ymm1, ymm4, ymm2
	vblendvpd	ymm1, ymm4, ymm2, ymm1
	vpcmpgtq	ymm2, ymm11, ymm5
	vblendvpd	ymm2, ymm11, ymm5, ymm2
	vpcmpgtq	ymm4, ymm7, ymm3
	vblendvpd	ymm4, ymm7, ymm3, ymm4
	vpcmpgtq	ymm5, ymm6, ymm10
	vblendvpd	ymm3, ymm6, ymm10, ymm5
	vpcmpgtq	ymm5, ymm3, ymm4
	vblendvpd	ymm3, ymm3, ymm4, ymm5
	vpcmpgtq	ymm4, ymm2, ymm1
	vblendvpd	ymm1, ymm2, ymm1, ymm4
	vpcmpgtq	ymm2, ymm1, ymm3
	vblendvpd	ymm1, ymm1, ymm3, ymm2
	vextractf128	xmm2, ymm1, 1
	vpcmpgtq	xmm3, xmm2, xmm1
	vblendvpd	xmm1, xmm2, xmm1, xmm3
	vpermilps	xmm2, xmm1, 78          # xmm2 = xmm1[2,3,0,1]
	vpcmpgtq	xmm3, xmm2, xmm1
	vblendvpd	xmm1, xmm2, xmm1, xmm3
	vmovq	rsi, xmm0
	vmovq	r9, xmm1
	cmp	r11, r8
	je	.LBB2_14
.LBB2_4:
	mov	rax, rsi
	.p2align	4, 0x90
.LBB2_5:                                # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdi + 8*r11]
	cmp	r9, rsi
	cmovg	r9, rsi
	cmp	rax, rsi
	cmovge	rsi, rax
	add	r11, 1
	mov	rax, rsi
	cmp	r8, r11
	jne	.LBB2_5
.LBB2_14:
	mov	qword ptr [rcx], rsi
	mov	qword ptr [rdx], r9
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end2:
	.size	int64_max_min_avx2, .Lfunc_end2-int64_max_min_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function uint64_max_min_avx2
.LCPI3_0:
	.quad	-9223372036854775808            # 0x8000000000000000
	.text
	.globl	uint64_max_min_avx2
	.p2align	4, 0x90
	.type	uint64_max_min_avx2,@function
uint64_max_min_avx2:                    # @uint64_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -32
	sub	rsp, 288
	test	esi, esi
	jle	.LBB3_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 31
	ja	.LBB3_6
# %bb.3:
	mov	r9, -1
	xor	r11d, r11d
	xor	esi, esi
	jmp	.LBB3_4
.LBB3_1:
	mov	r9, -1
	xor	esi, esi
	jmp	.LBB3_14
.LBB3_6:
	mov	r11d, r8d
	and	r11d, -32
	lea	rax, [r11 - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	mov	r9d, r10d
	and	r9d, 3
	cmp	rax, 96
	jae	.LBB3_8
# %bb.7:
	vpxor	xmm4, xmm4, xmm4
	vpcmpeqd	ymm0, ymm0, ymm0
	vmovdqa	ymmword ptr [rsp + 64], ymm0    # 32-byte Spill
	xor	eax, eax
	vpcmpeqd	ymm0, ymm0, ymm0
	vmovdqa	ymmword ptr [rsp + 96], ymm0    # 32-byte Spill
	vpcmpeqd	ymm5, ymm5, ymm5
	vpcmpeqd	ymm7, ymm7, ymm7
	vpcmpeqd	ymm12, ymm12, ymm12
	vpcmpeqd	ymm10, ymm10, ymm10
	vpcmpeqd	ymm11, ymm11, ymm11
	vpcmpeqd	ymm13, ymm13, ymm13
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymmword ptr [rsp + 32], ymm0    # 32-byte Spill
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymmword ptr [rsp], ymm0         # 32-byte Spill
	vpxor	xmm3, xmm3, xmm3
	vpxor	xmm9, xmm9, xmm9
	vpxor	xmm8, xmm8, xmm8
	vpxor	xmm15, xmm15, xmm15
	vpxor	xmm0, xmm0, xmm0
	jmp	.LBB3_10
.LBB3_8:
	and	r10, -4
	neg	r10
	vpxor	xmm4, xmm4, xmm4
	vpcmpeqd	ymm0, ymm0, ymm0
	vmovdqa	ymmword ptr [rsp + 64], ymm0    # 32-byte Spill
	xor	eax, eax
	vpbroadcastq	ymm14, qword ptr [rip + .LCPI3_0] # ymm14 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	vpcmpeqd	ymm0, ymm0, ymm0
	vmovdqa	ymmword ptr [rsp + 96], ymm0    # 32-byte Spill
	vpcmpeqd	ymm5, ymm5, ymm5
	vpcmpeqd	ymm7, ymm7, ymm7
	vpcmpeqd	ymm12, ymm12, ymm12
	vpcmpeqd	ymm10, ymm10, ymm10
	vpcmpeqd	ymm11, ymm11, ymm11
	vpcmpeqd	ymm13, ymm13, ymm13
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymmword ptr [rsp + 32], ymm0    # 32-byte Spill
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymmword ptr [rsp], ymm0         # 32-byte Spill
	vpxor	xmm3, xmm3, xmm3
	vpxor	xmm9, xmm9, xmm9
	vpxor	xmm8, xmm8, xmm8
	vpxor	xmm15, xmm15, xmm15
	vpxor	xmm0, xmm0, xmm0
	.p2align	4, 0x90
.LBB3_9:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdi + 8*rax + 224]
	vpxor	ymm2, ymm14, ymm1
	vmovdqa	ymm6, ymm3
	vpxor	ymm3, ymm13, ymm14
	vpcmpgtq	ymm3, ymm2, ymm3
	vblendvpd	ymm3, ymm1, ymm13, ymm3
	vmovapd	ymmword ptr [rsp + 128], ymm3   # 32-byte Spill
	vpxor	ymm3, ymm14, ymm0
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm0, ymm1, ymm0, ymm2
	vmovapd	ymmword ptr [rsp + 224], ymm0   # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 192]
	vpxor	ymm1, ymm14, ymm0
	vpxor	ymm2, ymm11, ymm14
	vpcmpgtq	ymm2, ymm1, ymm2
	vblendvpd	ymm2, ymm0, ymm11, ymm2
	vmovapd	ymmword ptr [rsp + 160], ymm2   # 32-byte Spill
	vpxor	ymm2, ymm15, ymm14
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm0, ymm0, ymm15, ymm1
	vmovapd	ymmword ptr [rsp + 192], ymm0   # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 160]
	vpxor	ymm1, ymm14, ymm0
	vpxor	ymm2, ymm10, ymm14
	vpcmpgtq	ymm2, ymm1, ymm2
	vmovdqa	ymm3, ymm8
	vblendvpd	ymm8, ymm0, ymm10, ymm2
	vpxor	ymm2, ymm14, ymm3
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm13, ymm0, ymm3, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 128]
	vpxor	ymm2, ymm14, ymm0
	vpxor	ymm1, ymm12, ymm14
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm1, ymm0, ymm12, ymm1
	vpxor	ymm3, ymm9, ymm14
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm12, ymm0, ymm9, ymm2
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 96]
	vpxor	ymm0, ymm14, ymm7
	vpxor	ymm3, ymm14, ymm2
	vpcmpgtq	ymm0, ymm3, ymm0
	vblendvpd	ymm0, ymm2, ymm7, ymm0
	vmovdqa	ymm15, ymm4
	vpxor	ymm4, ymm14, ymm6
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm10, ymm2, ymm6, ymm3
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 64]
	vpxor	ymm3, ymm14, ymm5
	vpxor	ymm4, ymm14, ymm2
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm5, ymm2, ymm5, ymm3
	vmovdqa	ymm6, ymmword ptr [rsp]         # 32-byte Reload
	vpxor	ymm3, ymm14, ymm6
	vpcmpgtq	ymm3, ymm3, ymm4
	vblendvpd	ymm9, ymm2, ymm6, ymm3
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax]
	vmovdqa	ymm7, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpxor	ymm3, ymm14, ymm7
	vpxor	ymm4, ymm14, ymm2
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm3, ymm2, ymm7, ymm3
	vpxor	ymm11, ymm15, ymm14
	vpcmpgtq	ymm4, ymm11, ymm4
	vblendvpd	ymm4, ymm2, ymm15, ymm4
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 32]
	vmovdqa	ymm15, ymmword ptr [rsp + 96]   # 32-byte Reload
	vpxor	ymm11, ymm15, ymm14
	vpxor	ymm7, ymm14, ymm2
	vpcmpgtq	ymm11, ymm7, ymm11
	vblendvpd	ymm11, ymm2, ymm15, ymm11
	vmovdqa	ymm6, ymmword ptr [rsp + 32]    # 32-byte Reload
	vpxor	ymm15, ymm14, ymm6
	vpcmpgtq	ymm7, ymm15, ymm7
	vblendvpd	ymm2, ymm2, ymm6, ymm7
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rax + 288]
	vxorpd	ymm7, ymm11, ymm14
	vpxor	ymm15, ymm14, ymm6
	vpcmpgtq	ymm7, ymm15, ymm7
	vblendvpd	ymm7, ymm6, ymm11, ymm7
	vmovapd	ymmword ptr [rsp + 96], ymm7    # 32-byte Spill
	vxorpd	ymm7, ymm14, ymm2
	vpcmpgtq	ymm7, ymm7, ymm15
	vblendvpd	ymm2, ymm6, ymm2, ymm7
	vmovapd	ymmword ptr [rsp + 64], ymm2    # 32-byte Spill
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rax + 256]
	vxorpd	ymm7, ymm14, ymm3
	vpxor	ymm11, ymm14, ymm6
	vpcmpgtq	ymm7, ymm11, ymm7
	vblendvpd	ymm2, ymm6, ymm3, ymm7
	vmovapd	ymmword ptr [rsp], ymm2         # 32-byte Spill
	vxorpd	ymm7, ymm14, ymm4
	vpcmpgtq	ymm7, ymm7, ymm11
	vblendvpd	ymm2, ymm6, ymm4, ymm7
	vmovapd	ymmword ptr [rsp + 32], ymm2    # 32-byte Spill
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rax + 320]
	vxorpd	ymm7, ymm14, ymm5
	vpxor	ymm11, ymm14, ymm6
	vpcmpgtq	ymm7, ymm11, ymm7
	vblendvpd	ymm5, ymm6, ymm5, ymm7
	vxorpd	ymm7, ymm9, ymm14
	vpcmpgtq	ymm7, ymm7, ymm11
	vblendvpd	ymm7, ymm6, ymm9, ymm7
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rax + 352]
	vxorpd	ymm9, ymm14, ymm0
	vpxor	ymm11, ymm14, ymm6
	vpcmpgtq	ymm9, ymm11, ymm9
	vblendvpd	ymm9, ymm6, ymm0, ymm9
	vxorpd	ymm0, ymm10, ymm14
	vpcmpgtq	ymm0, ymm0, ymm11
	vblendvpd	ymm10, ymm6, ymm10, ymm0
	vmovdqu	ymm6, ymmword ptr [rdi + 8*rax + 384]
	vxorpd	ymm0, ymm14, ymm1
	vpxor	ymm11, ymm14, ymm6
	vpcmpgtq	ymm0, ymm11, ymm0
	vblendvpd	ymm4, ymm6, ymm1, ymm0
	vxorpd	ymm1, ymm12, ymm14
	vpcmpgtq	ymm1, ymm1, ymm11
	vblendvpd	ymm3, ymm6, ymm12, ymm1
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 416]
	vxorpd	ymm6, ymm8, ymm14
	vpxor	ymm12, ymm11, ymm14
	vpcmpgtq	ymm6, ymm12, ymm6
	vblendvpd	ymm6, ymm11, ymm8, ymm6
	vxorpd	ymm8, ymm13, ymm14
	vpcmpgtq	ymm8, ymm8, ymm12
	vblendvpd	ymm12, ymm11, ymm13, ymm8
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 448]
	vmovdqa	ymm0, ymmword ptr [rsp + 160]   # 32-byte Reload
	vpxor	ymm8, ymm14, ymm0
	vpxor	ymm13, ymm11, ymm14
	vpcmpgtq	ymm8, ymm13, ymm8
	vblendvpd	ymm8, ymm11, ymm0, ymm8
	vmovdqa	ymm0, ymmword ptr [rsp + 192]   # 32-byte Reload
	vpxor	ymm15, ymm14, ymm0
	vpcmpgtq	ymm13, ymm15, ymm13
	vblendvpd	ymm13, ymm11, ymm0, ymm13
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 480]
	vmovdqa	ymm1, ymmword ptr [rsp + 128]   # 32-byte Reload
	vpxor	ymm15, ymm14, ymm1
	vpxor	ymm0, ymm11, ymm14
	vpcmpgtq	ymm15, ymm0, ymm15
	vblendvpd	ymm1, ymm11, ymm1, ymm15
	vmovdqa	ymm2, ymmword ptr [rsp + 224]   # 32-byte Reload
	vpxor	ymm15, ymm14, ymm2
	vpcmpgtq	ymm0, ymm15, ymm0
	vblendvpd	ymm15, ymm11, ymm2, ymm0
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 736]
	vxorpd	ymm11, ymm14, ymm1
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm11, ymm2, ymm11
	vblendvpd	ymm1, ymm0, ymm1, ymm11
	vmovapd	ymmword ptr [rsp + 128], ymm1   # 32-byte Spill
	vxorpd	ymm1, ymm15, ymm14
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm0, ymm0, ymm15, ymm1
	vmovapd	ymmword ptr [rsp + 224], ymm0   # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 704]
	vxorpd	ymm1, ymm8, ymm14
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm1, ymm0, ymm8, ymm1
	vmovapd	ymmword ptr [rsp + 160], ymm1   # 32-byte Spill
	vxorpd	ymm1, ymm13, ymm14
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm0, ymm0, ymm13, ymm1
	vmovapd	ymmword ptr [rsp + 192], ymm0   # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 672]
	vxorpd	ymm1, ymm14, ymm6
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm15, ymm0, ymm6, ymm1
	vxorpd	ymm1, ymm12, ymm14
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm13, ymm0, ymm12, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 640]
	vxorpd	ymm1, ymm14, ymm4
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm12, ymm0, ymm4, ymm1
	vxorpd	ymm1, ymm14, ymm3
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm8, ymm0, ymm3, ymm1
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 608]
	vxorpd	ymm1, ymm9, ymm14
	vpxor	ymm3, ymm14, ymm2
	vpcmpgtq	ymm1, ymm3, ymm1
	vblendvpd	ymm1, ymm2, ymm9, ymm1
	vxorpd	ymm4, ymm10, ymm14
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm10, ymm2, ymm10, ymm3
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 576]
	vxorpd	ymm3, ymm14, ymm5
	vpxor	ymm4, ymm14, ymm2
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm5, ymm2, ymm5, ymm3
	vxorpd	ymm3, ymm14, ymm7
	vpcmpgtq	ymm3, ymm3, ymm4
	vblendvpd	ymm9, ymm2, ymm7, ymm3
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 512]
	vmovdqa	ymm0, ymmword ptr [rsp]         # 32-byte Reload
	vpxor	ymm3, ymm14, ymm0
	vpxor	ymm4, ymm14, ymm2
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm3, ymm2, ymm0, ymm3
	vmovdqa	ymm0, ymmword ptr [rsp + 32]    # 32-byte Reload
	vpxor	ymm6, ymm14, ymm0
	vpcmpgtq	ymm4, ymm6, ymm4
	vblendvpd	ymm4, ymm2, ymm0, ymm4
	vmovdqu	ymm2, ymmword ptr [rdi + 8*rax + 544]
	vmovdqa	ymm0, ymmword ptr [rsp + 96]    # 32-byte Reload
	vpxor	ymm6, ymm14, ymm0
	vpxor	ymm7, ymm14, ymm2
	vpcmpgtq	ymm6, ymm7, ymm6
	vblendvpd	ymm6, ymm2, ymm0, ymm6
	vmovdqa	ymm0, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpxor	ymm11, ymm14, ymm0
	vpcmpgtq	ymm7, ymm11, ymm7
	vblendvpd	ymm2, ymm2, ymm0, ymm7
	vmovdqu	ymm7, ymmword ptr [rdi + 8*rax + 800]
	vxorpd	ymm11, ymm14, ymm6
	vpxor	ymm0, ymm14, ymm7
	vpcmpgtq	ymm11, ymm0, ymm11
	vblendvpd	ymm6, ymm7, ymm6, ymm11
	vmovapd	ymmword ptr [rsp + 96], ymm6    # 32-byte Spill
	vxorpd	ymm6, ymm14, ymm2
	vpcmpgtq	ymm0, ymm6, ymm0
	vblendvpd	ymm0, ymm7, ymm2, ymm0
	vmovapd	ymmword ptr [rsp + 32], ymm0    # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 768]
	vxorpd	ymm2, ymm14, ymm3
	vpxor	ymm7, ymm14, ymm0
	vpcmpgtq	ymm2, ymm7, ymm2
	vblendvpd	ymm2, ymm0, ymm3, ymm2
	vmovapd	ymmword ptr [rsp + 64], ymm2    # 32-byte Spill
	vxorpd	ymm2, ymm14, ymm4
	vpcmpgtq	ymm2, ymm2, ymm7
	vblendvpd	ymm4, ymm0, ymm4, ymm2
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 832]
	vxorpd	ymm2, ymm14, ymm5
	vpxor	ymm3, ymm14, ymm0
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm5, ymm0, ymm5, ymm2
	vxorpd	ymm2, ymm9, ymm14
	vpcmpgtq	ymm2, ymm2, ymm3
	vblendvpd	ymm0, ymm0, ymm9, ymm2
	vmovapd	ymmword ptr [rsp], ymm0         # 32-byte Spill
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 864]
	vxorpd	ymm2, ymm14, ymm1
	vpxor	ymm3, ymm14, ymm0
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm7, ymm0, ymm1, ymm2
	vxorpd	ymm1, ymm10, ymm14
	vpcmpgtq	ymm1, ymm1, ymm3
	vblendvpd	ymm3, ymm0, ymm10, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 896]
	vxorpd	ymm1, ymm12, ymm14
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm12, ymm0, ymm12, ymm1
	vxorpd	ymm1, ymm8, ymm14
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm9, ymm0, ymm8, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 928]
	vxorpd	ymm1, ymm15, ymm14
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm10, ymm0, ymm15, ymm1
	vxorpd	ymm1, ymm13, ymm14
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm8, ymm0, ymm13, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 960]
	vmovdqa	ymm6, ymmword ptr [rsp + 160]   # 32-byte Reload
	vpxor	ymm1, ymm14, ymm6
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm11, ymm0, ymm6, ymm1
	vmovdqa	ymm6, ymmword ptr [rsp + 192]   # 32-byte Reload
	vpxor	ymm1, ymm14, ymm6
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm15, ymm0, ymm6, ymm1
	vmovdqu	ymm0, ymmword ptr [rdi + 8*rax + 992]
	vmovdqa	ymm6, ymmword ptr [rsp + 128]   # 32-byte Reload
	vpxor	ymm1, ymm14, ymm6
	vpxor	ymm2, ymm14, ymm0
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm13, ymm0, ymm6, ymm1
	vmovdqa	ymm6, ymmword ptr [rsp + 224]   # 32-byte Reload
	vpxor	ymm1, ymm14, ymm6
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm0, ymm0, ymm6, ymm1
	sub	rax, -128
	add	r10, 4
	jne	.LBB3_9
.LBB3_10:
	vmovaps	ymmword ptr [rsp + 128], ymm10  # 32-byte Spill
	test	r9, r9
	vmovdqa	ymm10, ymm12
	vmovdqa	ymm12, ymm3
	je	.LBB3_13
# %bb.11:
	lea	rax, [rdi + 8*rax]
	neg	r9
	vpbroadcastq	ymm14, qword ptr [rip + .LCPI3_0] # ymm14 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	.p2align	4, 0x90
.LBB3_12:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rax + 32]
	vmovdqa	ymm6, ymm7
	vmovdqa	ymm7, ymm5
	vmovdqa	ymm5, ymm4
	vmovdqa	ymm4, ymmword ptr [rsp + 96]    # 32-byte Reload
	vpxor	ymm2, ymm14, ymm4
	vpxor	ymm3, ymm14, ymm1
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm4, ymm1, ymm4, ymm2
	vmovapd	ymmword ptr [rsp + 96], ymm4    # 32-byte Spill
	vmovdqa	ymm4, ymmword ptr [rsp + 32]    # 32-byte Reload
	vpxor	ymm2, ymm14, ymm4
	vpcmpgtq	ymm2, ymm2, ymm3
	vblendvpd	ymm4, ymm1, ymm4, ymm2
	vmovapd	ymmword ptr [rsp + 32], ymm4    # 32-byte Spill
	vmovdqu	ymm1, ymmword ptr [rax]
	vmovdqa	ymm4, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpxor	ymm2, ymm14, ymm4
	vpxor	ymm3, ymm14, ymm1
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm4, ymm1, ymm4, ymm2
	vmovapd	ymmword ptr [rsp + 64], ymm4    # 32-byte Spill
	vmovdqa	ymm4, ymm5
	vmovdqa	ymm5, ymm7
	vmovdqa	ymm7, ymm6
	vpxor	ymm2, ymm14, ymm4
	vpcmpgtq	ymm2, ymm2, ymm3
	vmovdqu	ymm3, ymmword ptr [rax + 64]
	vblendvpd	ymm4, ymm1, ymm4, ymm2
	vpxor	ymm1, ymm14, ymm3
	vpxor	ymm2, ymm14, ymm5
	vpcmpgtq	ymm2, ymm1, ymm2
	vblendvpd	ymm5, ymm3, ymm5, ymm2
	vmovdqa	ymm6, ymmword ptr [rsp]         # 32-byte Reload
	vpxor	ymm2, ymm14, ymm6
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm6, ymm3, ymm6, ymm1
	vmovapd	ymmword ptr [rsp], ymm6         # 32-byte Spill
	vmovdqu	ymm1, ymmword ptr [rax + 96]
	vpxor	ymm2, ymm14, ymm1
	vpxor	ymm3, ymm14, ymm7
	vpcmpgtq	ymm3, ymm2, ymm3
	vblendvpd	ymm7, ymm1, ymm7, ymm3
	vpxor	ymm3, ymm12, ymm14
	vpcmpgtq	ymm2, ymm3, ymm2
	vmovdqu	ymm3, ymmword ptr [rax + 128]
	vblendvpd	ymm12, ymm1, ymm12, ymm2
	vpxor	ymm1, ymm14, ymm3
	vpxor	ymm2, ymm10, ymm14
	vpcmpgtq	ymm2, ymm1, ymm2
	vblendvpd	ymm10, ymm3, ymm10, ymm2
	vpxor	ymm2, ymm9, ymm14
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm9, ymm3, ymm9, ymm1
	vmovdqu	ymm1, ymmword ptr [rax + 160]
	vpxor	ymm2, ymm14, ymm1
	vmovdqa	ymm6, ymmword ptr [rsp + 128]   # 32-byte Reload
	vpxor	ymm3, ymm14, ymm6
	vpcmpgtq	ymm3, ymm2, ymm3
	vblendvpd	ymm6, ymm1, ymm6, ymm3
	vmovapd	ymmword ptr [rsp + 128], ymm6   # 32-byte Spill
	vpxor	ymm3, ymm8, ymm14
	vpcmpgtq	ymm2, ymm3, ymm2
	vmovdqu	ymm3, ymmword ptr [rax + 192]
	vblendvpd	ymm8, ymm1, ymm8, ymm2
	vpxor	ymm1, ymm14, ymm3
	vpxor	ymm2, ymm11, ymm14
	vpcmpgtq	ymm2, ymm1, ymm2
	vblendvpd	ymm11, ymm3, ymm11, ymm2
	vpxor	ymm2, ymm15, ymm14
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm15, ymm3, ymm15, ymm1
	vmovdqu	ymm1, ymmword ptr [rax + 224]
	vpxor	ymm2, ymm14, ymm1
	vpxor	ymm3, ymm13, ymm14
	vpcmpgtq	ymm3, ymm2, ymm3
	vblendvpd	ymm13, ymm1, ymm13, ymm3
	vpxor	ymm3, ymm14, ymm0
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm0, ymm1, ymm0, ymm2
	add	rax, 256
	inc	r9
	jne	.LBB3_12
.LBB3_13:
	vpbroadcastq	ymm14, qword ptr [rip + .LCPI3_0] # ymm14 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	vmovdqa	ymm3, ymmword ptr [rsp]         # 32-byte Reload
	vpxor	ymm1, ymm14, ymm3
	vpxor	ymm2, ymm15, ymm14
	vpcmpgtq	ymm1, ymm1, ymm2
	vblendvpd	ymm1, ymm15, ymm3, ymm1
	vpxor	ymm2, ymm14, ymm4
	vpxor	ymm3, ymm9, ymm14
	vpcmpgtq	ymm2, ymm2, ymm3
	vblendvpd	ymm2, ymm9, ymm4, ymm2
	vpxor	ymm3, ymm12, ymm14
	vpxor	ymm9, ymm14, ymm0
	vpcmpgtq	ymm3, ymm3, ymm9
	vblendvpd	ymm0, ymm0, ymm12, ymm3
	vmovdqa	ymm4, ymmword ptr [rsp + 32]    # 32-byte Reload
	vpxor	ymm3, ymm14, ymm4
	vpxor	ymm9, ymm8, ymm14
	vpcmpgtq	ymm3, ymm3, ymm9
	vblendvpd	ymm3, ymm8, ymm4, ymm3
	vxorpd	ymm6, ymm14, ymm3
	vxorpd	ymm9, ymm14, ymm0
	vpcmpgtq	ymm6, ymm6, ymm9
	vblendvpd	ymm0, ymm0, ymm3, ymm6
	vxorpd	ymm3, ymm14, ymm2
	vxorpd	ymm6, ymm14, ymm1
	vpcmpgtq	ymm3, ymm3, ymm6
	vblendvpd	ymm1, ymm1, ymm2, ymm3
	vxorpd	ymm2, ymm14, ymm1
	vxorpd	ymm3, ymm14, ymm0
	vpcmpgtq	ymm2, ymm2, ymm3
	vblendvpd	ymm0, ymm0, ymm1, ymm2
	vextractf128	xmm1, ymm0, 1
	vxorpd	xmm2, xmm14, xmm1
	vxorpd	xmm3, xmm14, xmm0
	vpcmpgtq	xmm2, xmm3, xmm2
	vblendvpd	xmm0, xmm1, xmm0, xmm2
	vpermilps	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	vxorpd	xmm2, xmm14, xmm0
	vxorpd	xmm3, xmm14, xmm1
	vpcmpgtq	xmm2, xmm2, xmm3
	vblendvpd	xmm0, xmm1, xmm0, xmm2
	vpxor	ymm1, ymm14, ymm5
	vpxor	ymm2, ymm11, ymm14
	vpcmpgtq	ymm1, ymm2, ymm1
	vblendvpd	ymm1, ymm11, ymm5, ymm1
	vmovdqa	ymm4, ymmword ptr [rsp + 64]    # 32-byte Reload
	vpxor	ymm2, ymm14, ymm4
	vpxor	ymm3, ymm10, ymm14
	vpcmpgtq	ymm2, ymm3, ymm2
	vblendvpd	ymm2, ymm10, ymm4, ymm2
	vpxor	ymm3, ymm14, ymm7
	vpxor	ymm5, ymm13, ymm14
	vpcmpgtq	ymm3, ymm5, ymm3
	vblendvpd	ymm3, ymm13, ymm7, ymm3
	vmovdqa	ymm6, ymmword ptr [rsp + 96]    # 32-byte Reload
	vpxor	ymm4, ymm14, ymm6
	vmovdqa	ymm7, ymmword ptr [rsp + 128]   # 32-byte Reload
	vpxor	ymm5, ymm14, ymm7
	vpcmpgtq	ymm4, ymm5, ymm4
	vblendvpd	ymm4, ymm7, ymm6, ymm4
	vxorpd	ymm5, ymm14, ymm4
	vxorpd	ymm6, ymm14, ymm3
	vpcmpgtq	ymm5, ymm6, ymm5
	vblendvpd	ymm3, ymm3, ymm4, ymm5
	vxorpd	ymm4, ymm14, ymm2
	vxorpd	ymm5, ymm14, ymm1
	vpcmpgtq	ymm4, ymm5, ymm4
	vblendvpd	ymm1, ymm1, ymm2, ymm4
	vxorpd	ymm2, ymm14, ymm1
	vxorpd	ymm4, ymm14, ymm3
	vpcmpgtq	ymm2, ymm4, ymm2
	vblendvpd	ymm1, ymm3, ymm1, ymm2
	vextractf128	xmm2, ymm1, 1
	vxorpd	xmm3, xmm14, xmm1
	vxorpd	xmm4, xmm14, xmm2
	vpcmpgtq	xmm3, xmm4, xmm3
	vblendvpd	xmm1, xmm2, xmm1, xmm3
	vpermilps	xmm2, xmm1, 78          # xmm2 = xmm1[2,3,0,1]
	vxorpd	xmm3, xmm14, xmm1
	vxorpd	xmm4, xmm14, xmm2
	vpcmpgtq	xmm3, xmm4, xmm3
	vblendvpd	xmm1, xmm2, xmm1, xmm3
	vmovq	rsi, xmm0
	vmovq	r9, xmm1
	cmp	r11, r8
	je	.LBB3_14
.LBB3_4:
	mov	rax, rsi
	.p2align	4, 0x90
.LBB3_5:                                # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdi + 8*r11]
	cmp	r9, rsi
	cmovae	r9, rsi
	cmp	rax, rsi
	cmova	rsi, rax
	add	r11, 1
	mov	rax, rsi
	cmp	r8, r11
	jne	.LBB3_5
.LBB3_14:
	mov	qword ptr [rcx], rsi
	mov	qword ptr [rdx], r9
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end3:
	.size	uint64_max_min_avx2, .Lfunc_end3-uint64_max_min_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-++20210204121720+1fdec59bffc1-1~exp1~20210203232336.162"
	.section	".note.GNU-stack","",@progbits
	.addrsig
