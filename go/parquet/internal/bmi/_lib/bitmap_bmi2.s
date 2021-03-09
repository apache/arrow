	.text
	.intel_syntax noprefix
	.file	"bitmap_bmi2.c"
	.globl	extract_bits            # -- Begin function extract_bits
	.p2align	4, 0x90
	.type	extract_bits,@function
extract_bits:                           # @extract_bits
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	pext	rax, rdi, rsi
	mov	qword ptr [rdx], rax
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	extract_bits, .Lfunc_end0-extract_bits
                                        # -- End function
	.globl	popcount64              # -- Begin function popcount64
	.p2align	4, 0x90
	.type	popcount64,@function
popcount64:                             # @popcount64
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	popcnt	rax, rdi
	mov	qword ptr [rsi], rax
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end1:
	.size	popcount64, .Lfunc_end1-popcount64
                                        # -- End function
	.globl	popcount32              # -- Begin function popcount32
	.p2align	4, 0x90
	.type	popcount32,@function
popcount32:                             # @popcount32
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	popcnt	eax, edi
	mov	dword ptr [rsi], eax
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end2:
	.size	popcount32, .Lfunc_end2-popcount32
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5               # -- Begin function levels_to_bitmap
.LCPI3_0:
	.quad	0                       # 0x0
	.quad	1                       # 0x1
	.quad	2                       # 0x2
	.quad	3                       # 0x3
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3
.LCPI3_1:
	.quad	4                       # 0x4
.LCPI3_2:
	.quad	8                       # 0x8
.LCPI3_3:
	.quad	12                      # 0xc
.LCPI3_4:
	.quad	1                       # 0x1
.LCPI3_5:
	.quad	16                      # 0x10
	.text
	.globl	levels_to_bitmap
	.p2align	4, 0x90
	.type	levels_to_bitmap,@function
levels_to_bitmap:                       # @levels_to_bitmap
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB3_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 15
	ja	.LBB3_4
# %bb.3:
	xor	r9d, r9d
	xor	eax, eax
	jmp	.LBB3_7
.LBB3_1:
	xor	eax, eax
	jmp	.LBB3_8
.LBB3_4:
	mov	r9d, r8d
	and	r9d, -16
	vmovd	xmm0, edx
	vpbroadcastw	xmm1, xmm0
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI3_0] # ymm2 = [0,1,2,3]
	vpbroadcastq	ymm12, qword ptr [rip + .LCPI3_1] # ymm12 = [4,4,4,4]
	vpbroadcastq	ymm4, qword ptr [rip + .LCPI3_2] # ymm4 = [8,8,8,8]
	vpbroadcastq	ymm5, qword ptr [rip + .LCPI3_3] # ymm5 = [12,12,12,12]
	vpbroadcastq	ymm6, qword ptr [rip + .LCPI3_4] # ymm6 = [1,1,1,1]
	vpbroadcastq	ymm7, qword ptr [rip + .LCPI3_5] # ymm7 = [16,16,16,16]
	xor	eax, eax
	vpxor	xmm8, xmm8, xmm8
	vpxor	xmm9, xmm9, xmm9
	vpxor	xmm10, xmm10, xmm10
	.p2align	4, 0x90
.LBB3_5:                                # =>This Inner Loop Header: Depth=1
	vpaddq	ymm11, ymm12, ymm2
	vmovq	xmm3, qword ptr [rdi + 2*rax + 8] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3      # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm11
	vpaddq	ymm11, ymm2, ymm4
	vpor	ymm8, ymm8, ymm3
	vmovq	xmm3, qword ptr [rdi + 2*rax + 16] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3      # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm11
	vpaddq	ymm11, ymm2, ymm5
	vpor	ymm9, ymm9, ymm3
	vmovq	xmm3, qword ptr [rdi + 2*rax + 24] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3      # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm11
	vpor	ymm10, ymm10, ymm3
	vmovq	xmm3, qword ptr [rdi + 2*rax] # xmm3 = mem[0],zero
	vpcmpgtw	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3      # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm6
	vpsllvq	ymm3, ymm3, ymm2
	vpor	ymm0, ymm3, ymm0
	add	rax, 16
	vpaddq	ymm2, ymm2, ymm7
	cmp	r9, rax
	jne	.LBB3_5
# %bb.6:
	vpor	ymm0, ymm8, ymm0
	vpor	ymm0, ymm9, ymm0
	vpor	ymm0, ymm10, ymm0
	vextracti128	xmm1, ymm0, 1
	vpor	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	vpor	xmm0, xmm0, xmm1
	vmovq	rax, xmm0
	cmp	r9, r8
	je	.LBB3_8
	.p2align	4, 0x90
.LBB3_7:                                # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rdi + 2*r9], dx
	setg	sil
	shlx	rsi, rsi, r9
	or	rax, rsi
	add	r9, 1
	cmp	r8, r9
	jne	.LBB3_7
.LBB3_8:
	mov	qword ptr [rcx], rax
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end3:
	.size	levels_to_bitmap, .Lfunc_end3-levels_to_bitmap
                                        # -- End function
	.ident	"clang version 10.0.0-4ubuntu1 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
