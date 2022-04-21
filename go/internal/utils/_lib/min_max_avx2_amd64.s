	.text
	.intel_syntax noprefix
	.file	"min_max.c"
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function int8_max_min_avx2
.LCPI0_0:
	.zero	32,128
.LCPI0_1:
	.zero	32,127
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI0_2:
	.zero	16,127
.LCPI0_3:
	.zero	16,128
	.text
	.globl	int8_max_min_avx2
	.p2align	4, 0x90
	.type	int8_max_min_avx2,@function
int8_max_min_avx2:                      # @int8_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB0_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 63
	ja	.LBB0_4
# %bb.3:
	mov	r8b, -128
	mov	sil, 127
	xor	r10d, r10d
	jmp	.LBB0_11
.LBB0_1:
	mov	sil, 127
	mov	r8b, -128
	jmp	.LBB0_12
.LBB0_4:
	mov	r10d, r9d
	and	r10d, -64
	lea	rax, [r10 - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_5
# %bb.6:
	mov	rsi, r8
	and	rsi, -2
	neg	rsi
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_0] # ymm1 = [128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128]
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_1] # ymm0 = [127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127]
	xor	eax, eax
	vmovdqa	ymm2, ymm0
	vmovdqa	ymm3, ymm1
	.p2align	4, 0x90
.LBB0_7:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdi + rax]
	vmovdqu	ymm5, ymmword ptr [rdi + rax + 32]
	vmovdqu	ymm6, ymmword ptr [rdi + rax + 64]
	vmovdqu	ymm7, ymmword ptr [rdi + rax + 96]
	vpminsb	ymm0, ymm0, ymm4
	vpminsb	ymm2, ymm2, ymm5
	vpmaxsb	ymm1, ymm1, ymm4
	vpmaxsb	ymm3, ymm3, ymm5
	vpminsb	ymm0, ymm0, ymm6
	vpminsb	ymm2, ymm2, ymm7
	vpmaxsb	ymm1, ymm1, ymm6
	vpmaxsb	ymm3, ymm3, ymm7
	sub	rax, -128
	add	rsi, 2
	jne	.LBB0_7
# %bb.8:
	test	r8b, 1
	je	.LBB0_10
.LBB0_9:
	vmovdqu	ymm4, ymmword ptr [rdi + rax]
	vmovdqu	ymm5, ymmword ptr [rdi + rax + 32]
	vpmaxsb	ymm3, ymm3, ymm5
	vpmaxsb	ymm1, ymm1, ymm4
	vpminsb	ymm2, ymm2, ymm5
	vpminsb	ymm0, ymm0, ymm4
.LBB0_10:
	vpmaxsb	ymm1, ymm1, ymm3
	vextracti128	xmm3, ymm1, 1
	vpmaxsb	xmm1, xmm1, xmm3
	vpxor	xmm1, xmm1, xmmword ptr [rip + .LCPI0_2]
	vpminsb	ymm0, ymm0, ymm2
	vpsrlw	xmm2, xmm1, 8
	vpminub	xmm1, xmm1, xmm2
	vphminposuw	xmm1, xmm1
	vmovd	r8d, xmm1
	xor	r8b, 127
	vextracti128	xmm1, ymm0, 1
	vpminsb	xmm0, xmm0, xmm1
	vpxor	xmm0, xmm0, xmmword ptr [rip + .LCPI0_3]
	vpsrlw	xmm1, xmm0, 8
	vpminub	xmm0, xmm0, xmm1
	vphminposuw	xmm0, xmm0
	vmovd	esi, xmm0
	xor	sil, -128
	cmp	r10, r9
	je	.LBB0_12
	.p2align	4, 0x90
.LBB0_11:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r10]
	cmp	sil, al
	movzx	esi, sil
	cmovg	esi, eax
	cmp	r8b, al
	movzx	r8d, r8b
	cmovl	r8d, eax
	add	r10, 1
	cmp	r9, r10
	jne	.LBB0_11
.LBB0_12:
	mov	byte ptr [rcx], r8b
	mov	byte ptr [rdx], sil
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB0_5:
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_0] # ymm1 = [128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128]
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_1] # ymm0 = [127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127]
	xor	eax, eax
	vmovdqa	ymm2, ymm0
	vmovdqa	ymm3, ymm1
	test	r8b, 1
	jne	.LBB0_9
	jmp	.LBB0_10
.Lfunc_end0:
	.size	int8_max_min_avx2, .Lfunc_end0-int8_max_min_avx2
                                        # -- End function
	.globl	uint8_max_min_avx2              # -- Begin function uint8_max_min_avx2
	.p2align	4, 0x90
	.type	uint8_max_min_avx2,@function
uint8_max_min_avx2:                     # @uint8_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB1_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 63
	ja	.LBB1_4
# %bb.3:
	mov	sil, -1
	xor	r10d, r10d
	xor	eax, eax
	jmp	.LBB1_11
.LBB1_1:
	mov	sil, -1
	xor	eax, eax
	jmp	.LBB1_12
.LBB1_4:
	mov	r10d, r9d
	and	r10d, -64
	lea	rax, [r10 - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB1_5
# %bb.6:
	mov	rsi, r8
	and	rsi, -2
	neg	rsi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpcmpeqd	ymm2, ymm2, ymm2
	vpxor	xmm3, xmm3, xmm3
	.p2align	4, 0x90
.LBB1_7:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdi + rax]
	vmovdqu	ymm5, ymmword ptr [rdi + rax + 32]
	vmovdqu	ymm6, ymmword ptr [rdi + rax + 64]
	vmovdqu	ymm7, ymmword ptr [rdi + rax + 96]
	vpminub	ymm1, ymm1, ymm4
	vpminub	ymm2, ymm2, ymm5
	vpmaxub	ymm0, ymm0, ymm4
	vpmaxub	ymm3, ymm3, ymm5
	vpminub	ymm1, ymm1, ymm6
	vpminub	ymm2, ymm2, ymm7
	vpmaxub	ymm0, ymm0, ymm6
	vpmaxub	ymm3, ymm3, ymm7
	sub	rax, -128
	add	rsi, 2
	jne	.LBB1_7
# %bb.8:
	test	r8b, 1
	je	.LBB1_10
.LBB1_9:
	vmovdqu	ymm4, ymmword ptr [rdi + rax]
	vmovdqu	ymm5, ymmword ptr [rdi + rax + 32]
	vpmaxub	ymm3, ymm3, ymm5
	vpmaxub	ymm0, ymm0, ymm4
	vpminub	ymm2, ymm2, ymm5
	vpminub	ymm1, ymm1, ymm4
.LBB1_10:
	vpminub	ymm1, ymm1, ymm2
	vpmaxub	ymm0, ymm0, ymm3
	vextracti128	xmm2, ymm0, 1
	vpmaxub	xmm0, xmm0, xmm2
	vpcmpeqd	xmm2, xmm2, xmm2
	vpxor	xmm0, xmm0, xmm2
	vpsrlw	xmm2, xmm0, 8
	vpminub	xmm0, xmm0, xmm2
	vphminposuw	xmm0, xmm0
	vmovd	eax, xmm0
	not	al
	vextracti128	xmm0, ymm1, 1
	vpminub	xmm0, xmm1, xmm0
	vpsrlw	xmm1, xmm0, 8
	vpminub	xmm0, xmm0, xmm1
	vphminposuw	xmm0, xmm0
	vmovd	esi, xmm0
	cmp	r10, r9
	je	.LBB1_12
	.p2align	4, 0x90
.LBB1_11:                               # =>This Inner Loop Header: Depth=1
	movzx	r8d, byte ptr [rdi + r10]
	cmp	sil, r8b
	movzx	esi, sil
	cmovae	esi, r8d
	cmp	al, r8b
	movzx	eax, al
	cmovbe	eax, r8d
	add	r10, 1
	cmp	r9, r10
	jne	.LBB1_11
.LBB1_12:
	mov	byte ptr [rcx], al
	mov	byte ptr [rdx], sil
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB1_5:
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpcmpeqd	ymm2, ymm2, ymm2
	vpxor	xmm3, xmm3, xmm3
	test	r8b, 1
	jne	.LBB1_9
	jmp	.LBB1_10
.Lfunc_end1:
	.size	uint8_max_min_avx2, .Lfunc_end1-uint8_max_min_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function int16_max_min_avx2
.LCPI2_0:
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
.LCPI2_1:
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI2_2:
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
	.short	32767                           # 0x7fff
.LCPI2_3:
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.short	32768                           # 0x8000
	.text
	.globl	int16_max_min_avx2
	.p2align	4, 0x90
	.type	int16_max_min_avx2,@function
int16_max_min_avx2:                     # @int16_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB2_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 31
	ja	.LBB2_4
# %bb.3:
	mov	r8w, -32768
	mov	si, 32767
	xor	r10d, r10d
	jmp	.LBB2_11
.LBB2_1:
	mov	si, 32767
	mov	r8w, -32768
	jmp	.LBB2_12
.LBB2_4:
	mov	r10d, r9d
	and	r10d, -32
	lea	rax, [r10 - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB2_5
# %bb.6:
	mov	rsi, r8
	and	rsi, -2
	neg	rsi
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI2_0] # ymm1 = [32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768]
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_1] # ymm0 = [32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767]
	xor	eax, eax
	vmovdqa	ymm2, ymm0
	vmovdqa	ymm3, ymm1
	.p2align	4, 0x90
.LBB2_7:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdi + 2*rax]
	vmovdqu	ymm5, ymmword ptr [rdi + 2*rax + 32]
	vmovdqu	ymm6, ymmword ptr [rdi + 2*rax + 64]
	vmovdqu	ymm7, ymmword ptr [rdi + 2*rax + 96]
	vpminsw	ymm0, ymm0, ymm4
	vpminsw	ymm2, ymm2, ymm5
	vpmaxsw	ymm1, ymm1, ymm4
	vpmaxsw	ymm3, ymm3, ymm5
	vpminsw	ymm0, ymm0, ymm6
	vpminsw	ymm2, ymm2, ymm7
	vpmaxsw	ymm1, ymm1, ymm6
	vpmaxsw	ymm3, ymm3, ymm7
	add	rax, 64
	add	rsi, 2
	jne	.LBB2_7
# %bb.8:
	test	r8b, 1
	je	.LBB2_10
.LBB2_9:
	vmovdqu	ymm4, ymmword ptr [rdi + 2*rax]
	vmovdqu	ymm5, ymmword ptr [rdi + 2*rax + 32]
	vpmaxsw	ymm3, ymm3, ymm5
	vpmaxsw	ymm1, ymm1, ymm4
	vpminsw	ymm2, ymm2, ymm5
	vpminsw	ymm0, ymm0, ymm4
.LBB2_10:
	vpmaxsw	ymm1, ymm1, ymm3
	vextracti128	xmm3, ymm1, 1
	vpmaxsw	xmm1, xmm1, xmm3
	vpxor	xmm1, xmm1, xmmword ptr [rip + .LCPI2_2]
	vpminsw	ymm0, ymm0, ymm2
	vphminposuw	xmm1, xmm1
	vmovd	r8d, xmm1
	xor	r8d, 32767
	vextracti128	xmm1, ymm0, 1
	vpminsw	xmm0, xmm0, xmm1
	vpxor	xmm0, xmm0, xmmword ptr [rip + .LCPI2_3]
	vphminposuw	xmm0, xmm0
	vmovd	esi, xmm0
	xor	esi, 32768
	cmp	r10, r9
	je	.LBB2_12
	.p2align	4, 0x90
.LBB2_11:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*r10]
	cmp	si, ax
	cmovg	esi, eax
	cmp	r8w, ax
	cmovl	r8d, eax
	add	r10, 1
	cmp	r9, r10
	jne	.LBB2_11
.LBB2_12:
	mov	word ptr [rcx], r8w
	mov	word ptr [rdx], si
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB2_5:
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI2_0] # ymm1 = [32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768,32768]
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_1] # ymm0 = [32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767,32767]
	xor	eax, eax
	vmovdqa	ymm2, ymm0
	vmovdqa	ymm3, ymm1
	test	r8b, 1
	jne	.LBB2_9
	jmp	.LBB2_10
.Lfunc_end2:
	.size	int16_max_min_avx2, .Lfunc_end2-int16_max_min_avx2
                                        # -- End function
	.globl	uint16_max_min_avx2             # -- Begin function uint16_max_min_avx2
	.p2align	4, 0x90
	.type	uint16_max_min_avx2,@function
uint16_max_min_avx2:                    # @uint16_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB3_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 31
	ja	.LBB3_4
# %bb.3:
	mov	r8w, -1
	xor	r10d, r10d
	xor	esi, esi
	jmp	.LBB3_11
.LBB3_1:
	mov	r8w, -1
	xor	esi, esi
	jmp	.LBB3_12
.LBB3_4:
	mov	r10d, r9d
	and	r10d, -32
	lea	rax, [r10 - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_5
# %bb.6:
	mov	rsi, r8
	and	rsi, -2
	neg	rsi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpcmpeqd	ymm2, ymm2, ymm2
	vpxor	xmm3, xmm3, xmm3
	.p2align	4, 0x90
.LBB3_7:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdi + 2*rax]
	vmovdqu	ymm5, ymmword ptr [rdi + 2*rax + 32]
	vmovdqu	ymm6, ymmword ptr [rdi + 2*rax + 64]
	vmovdqu	ymm7, ymmword ptr [rdi + 2*rax + 96]
	vpminuw	ymm1, ymm1, ymm4
	vpminuw	ymm2, ymm2, ymm5
	vpmaxuw	ymm0, ymm0, ymm4
	vpmaxuw	ymm3, ymm3, ymm5
	vpminuw	ymm1, ymm1, ymm6
	vpminuw	ymm2, ymm2, ymm7
	vpmaxuw	ymm0, ymm0, ymm6
	vpmaxuw	ymm3, ymm3, ymm7
	add	rax, 64
	add	rsi, 2
	jne	.LBB3_7
# %bb.8:
	test	r8b, 1
	je	.LBB3_10
.LBB3_9:
	vmovdqu	ymm4, ymmword ptr [rdi + 2*rax]
	vmovdqu	ymm5, ymmword ptr [rdi + 2*rax + 32]
	vpmaxuw	ymm3, ymm3, ymm5
	vpmaxuw	ymm0, ymm0, ymm4
	vpminuw	ymm2, ymm2, ymm5
	vpminuw	ymm1, ymm1, ymm4
.LBB3_10:
	vpminuw	ymm1, ymm1, ymm2
	vpmaxuw	ymm0, ymm0, ymm3
	vextracti128	xmm2, ymm0, 1
	vpmaxuw	xmm0, xmm0, xmm2
	vpcmpeqd	xmm2, xmm2, xmm2
	vpxor	xmm0, xmm0, xmm2
	vphminposuw	xmm0, xmm0
	vmovd	esi, xmm0
	not	esi
	vextracti128	xmm0, ymm1, 1
	vpminuw	xmm0, xmm1, xmm0
	vphminposuw	xmm0, xmm0
	vmovd	r8d, xmm0
	cmp	r10, r9
	je	.LBB3_12
	.p2align	4, 0x90
.LBB3_11:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*r10]
	cmp	r8w, ax
	cmovae	r8d, eax
	cmp	si, ax
	cmovbe	esi, eax
	add	r10, 1
	cmp	r9, r10
	jne	.LBB3_11
.LBB3_12:
	mov	word ptr [rcx], si
	mov	word ptr [rdx], r8w
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB3_5:
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpcmpeqd	ymm2, ymm2, ymm2
	vpxor	xmm3, xmm3, xmm3
	test	r8b, 1
	jne	.LBB3_9
	jmp	.LBB3_10
.Lfunc_end3:
	.size	uint16_max_min_avx2, .Lfunc_end3-uint16_max_min_avx2
                                        # -- End function
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2                               # -- Begin function int32_max_min_avx2
.LCPI4_0:
	.long	2147483648                      # 0x80000000
.LCPI4_1:
	.long	2147483647                      # 0x7fffffff
	.text
	.globl	int32_max_min_avx2
	.p2align	4, 0x90
	.type	int32_max_min_avx2,@function
int32_max_min_avx2:                     # @int32_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB4_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 31
	ja	.LBB4_4
# %bb.3:
	mov	r10d, -2147483648
	mov	eax, 2147483647
	xor	r9d, r9d
	jmp	.LBB4_7
.LBB4_1:
	mov	eax, 2147483647
	mov	esi, -2147483648
	jmp	.LBB4_8
.LBB4_4:
	mov	r9d, r8d
	vpbroadcastd	ymm4, dword ptr [rip + .LCPI4_0] # ymm4 = [2147483648,2147483648,2147483648,2147483648,2147483648,2147483648,2147483648,2147483648]
	and	r9d, -32
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI4_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	xor	eax, eax
	vmovdqa	ymm1, ymm0
	vmovdqa	ymm2, ymm0
	vmovdqa	ymm3, ymm0
	vmovdqa	ymm5, ymm4
	vmovdqa	ymm6, ymm4
	vmovdqa	ymm7, ymm4
	.p2align	4, 0x90
.LBB4_5:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax]
	vmovdqu	ymm9, ymmword ptr [rdi + 4*rax + 32]
	vmovdqu	ymm10, ymmword ptr [rdi + 4*rax + 64]
	vmovdqu	ymm11, ymmword ptr [rdi + 4*rax + 96]
	vpminsd	ymm0, ymm0, ymm8
	vpminsd	ymm1, ymm1, ymm9
	vpminsd	ymm2, ymm2, ymm10
	vpminsd	ymm3, ymm3, ymm11
	vpmaxsd	ymm4, ymm4, ymm8
	vpmaxsd	ymm5, ymm5, ymm9
	vpmaxsd	ymm6, ymm6, ymm10
	vpmaxsd	ymm7, ymm7, ymm11
	add	rax, 32
	cmp	r9, rax
	jne	.LBB4_5
# %bb.6:
	vpmaxsd	ymm4, ymm4, ymm5
	vpmaxsd	ymm4, ymm4, ymm6
	vpmaxsd	ymm4, ymm4, ymm7
	vextracti128	xmm5, ymm4, 1
	vpmaxsd	xmm4, xmm4, xmm5
	vpshufd	xmm5, xmm4, 78                  # xmm5 = xmm4[2,3,0,1]
	vpmaxsd	xmm4, xmm4, xmm5
	vpshufd	xmm5, xmm4, 229                 # xmm5 = xmm4[1,1,2,3]
	vpmaxsd	xmm4, xmm4, xmm5
	vmovd	r10d, xmm4
	vpminsd	ymm0, ymm0, ymm1
	vpminsd	ymm0, ymm0, ymm2
	vpminsd	ymm0, ymm0, ymm3
	vextracti128	xmm1, ymm0, 1
	vpminsd	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	vpminsd	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 229                 # xmm1 = xmm0[1,1,2,3]
	vpminsd	xmm0, xmm0, xmm1
	vmovd	eax, xmm0
	mov	esi, r10d
	cmp	r9, r8
	je	.LBB4_8
	.p2align	4, 0x90
.LBB4_7:                                # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdi + 4*r9]
	cmp	eax, esi
	cmovg	eax, esi
	cmp	r10d, esi
	cmovge	esi, r10d
	add	r9, 1
	mov	r10d, esi
	cmp	r8, r9
	jne	.LBB4_7
.LBB4_8:
	mov	dword ptr [rcx], esi
	mov	dword ptr [rdx], eax
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end4:
	.size	int32_max_min_avx2, .Lfunc_end4-int32_max_min_avx2
                                        # -- End function
	.globl	uint32_max_min_avx2             # -- Begin function uint32_max_min_avx2
	.p2align	4, 0x90
	.type	uint32_max_min_avx2,@function
uint32_max_min_avx2:                    # @uint32_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB5_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 31
	ja	.LBB5_4
# %bb.3:
	xor	r9d, r9d
	mov	eax, -1
	xor	r10d, r10d
	jmp	.LBB5_7
.LBB5_1:
	mov	eax, -1
	xor	esi, esi
	jmp	.LBB5_8
.LBB5_4:
	mov	r9d, r8d
	and	r9d, -32
	vpxor	xmm4, xmm4, xmm4
	vpcmpeqd	ymm0, ymm0, ymm0
	xor	eax, eax
	vpcmpeqd	ymm1, ymm1, ymm1
	vpcmpeqd	ymm2, ymm2, ymm2
	vpcmpeqd	ymm3, ymm3, ymm3
	vpxor	xmm5, xmm5, xmm5
	vpxor	xmm6, xmm6, xmm6
	vpxor	xmm7, xmm7, xmm7
	.p2align	4, 0x90
.LBB5_5:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rdi + 4*rax]
	vmovdqu	ymm9, ymmword ptr [rdi + 4*rax + 32]
	vmovdqu	ymm10, ymmword ptr [rdi + 4*rax + 64]
	vmovdqu	ymm11, ymmword ptr [rdi + 4*rax + 96]
	vpminud	ymm0, ymm0, ymm8
	vpminud	ymm1, ymm1, ymm9
	vpminud	ymm2, ymm2, ymm10
	vpminud	ymm3, ymm3, ymm11
	vpmaxud	ymm4, ymm4, ymm8
	vpmaxud	ymm5, ymm5, ymm9
	vpmaxud	ymm6, ymm6, ymm10
	vpmaxud	ymm7, ymm7, ymm11
	add	rax, 32
	cmp	r9, rax
	jne	.LBB5_5
# %bb.6:
	vpmaxud	ymm4, ymm4, ymm5
	vpmaxud	ymm4, ymm4, ymm6
	vpmaxud	ymm4, ymm4, ymm7
	vextracti128	xmm5, ymm4, 1
	vpmaxud	xmm4, xmm4, xmm5
	vpshufd	xmm5, xmm4, 78                  # xmm5 = xmm4[2,3,0,1]
	vpmaxud	xmm4, xmm4, xmm5
	vpshufd	xmm5, xmm4, 229                 # xmm5 = xmm4[1,1,2,3]
	vpmaxud	xmm4, xmm4, xmm5
	vmovd	r10d, xmm4
	vpminud	ymm0, ymm0, ymm1
	vpminud	ymm0, ymm0, ymm2
	vpminud	ymm0, ymm0, ymm3
	vextracti128	xmm1, ymm0, 1
	vpminud	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	vpminud	xmm0, xmm0, xmm1
	vpshufd	xmm1, xmm0, 229                 # xmm1 = xmm0[1,1,2,3]
	vpminud	xmm0, xmm0, xmm1
	vmovd	eax, xmm0
	mov	esi, r10d
	cmp	r9, r8
	je	.LBB5_8
	.p2align	4, 0x90
.LBB5_7:                                # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdi + 4*r9]
	cmp	eax, esi
	cmovae	eax, esi
	cmp	r10d, esi
	cmova	esi, r10d
	add	r9, 1
	mov	r10d, esi
	cmp	r8, r9
	jne	.LBB5_7
.LBB5_8:
	mov	dword ptr [rcx], esi
	mov	dword ptr [rdx], eax
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end5:
	.size	uint32_max_min_avx2, .Lfunc_end5-uint32_max_min_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function int64_max_min_avx2
.LCPI6_0:
	.quad	-9223372036854775808            # 0x8000000000000000
.LCPI6_1:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.text
	.globl	int64_max_min_avx2
	.p2align	4, 0x90
	.type	int64_max_min_avx2,@function
int64_max_min_avx2:                     # @int64_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	movabs	rax, 9223372036854775807
	test	esi, esi
	jle	.LBB6_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 15
	ja	.LBB6_4
# %bb.3:
	lea	r10, [rax + 1]
	xor	r9d, r9d
	jmp	.LBB6_7
.LBB6_1:
	lea	rsi, [rax + 1]
	jmp	.LBB6_8
.LBB6_4:
	mov	r9d, r8d
	vpbroadcastq	ymm4, qword ptr [rip + .LCPI6_0] # ymm4 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	and	r9d, -16
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI6_1] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	xor	eax, eax
	vmovdqa	ymm3, ymm0
	vmovdqa	ymm2, ymm0
	vmovdqa	ymm1, ymm0
	vmovdqa	ymm7, ymm4
	vmovdqa	ymm6, ymm4
	vmovdqa	ymm5, ymm4
	.p2align	4, 0x90
.LBB6_5:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm8, ymmword ptr [rdi + 8*rax]
	vpcmpgtq	ymm9, ymm8, ymm0
	vblendvpd	ymm0, ymm8, ymm0, ymm9
	vmovdqu	ymm9, ymmword ptr [rdi + 8*rax + 32]
	vpcmpgtq	ymm10, ymm9, ymm3
	vblendvpd	ymm3, ymm9, ymm3, ymm10
	vmovdqu	ymm10, ymmword ptr [rdi + 8*rax + 64]
	vpcmpgtq	ymm11, ymm10, ymm2
	vblendvpd	ymm2, ymm10, ymm2, ymm11
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 96]
	vpcmpgtq	ymm12, ymm11, ymm1
	vblendvpd	ymm1, ymm11, ymm1, ymm12
	vpcmpgtq	ymm12, ymm4, ymm8
	vblendvpd	ymm4, ymm8, ymm4, ymm12
	vpcmpgtq	ymm8, ymm7, ymm9
	vblendvpd	ymm7, ymm9, ymm7, ymm8
	vpcmpgtq	ymm8, ymm6, ymm10
	vblendvpd	ymm6, ymm10, ymm6, ymm8
	vpcmpgtq	ymm8, ymm5, ymm11
	vblendvpd	ymm5, ymm11, ymm5, ymm8
	add	rax, 16
	cmp	r9, rax
	jne	.LBB6_5
# %bb.6:
	vpcmpgtq	ymm8, ymm4, ymm7
	vblendvpd	ymm4, ymm7, ymm4, ymm8
	vpcmpgtq	ymm7, ymm4, ymm6
	vblendvpd	ymm4, ymm6, ymm4, ymm7
	vpcmpgtq	ymm6, ymm4, ymm5
	vblendvpd	ymm4, ymm5, ymm4, ymm6
	vextractf128	xmm5, ymm4, 1
	vpcmpgtq	xmm6, xmm4, xmm5
	vblendvpd	xmm4, xmm5, xmm4, xmm6
	vpermilps	xmm5, xmm4, 78          # xmm5 = xmm4[2,3,0,1]
	vpcmpgtq	xmm6, xmm4, xmm5
	vblendvpd	xmm4, xmm5, xmm4, xmm6
	vmovq	r10, xmm4
	vpcmpgtq	ymm4, ymm3, ymm0
	vblendvpd	ymm0, ymm3, ymm0, ymm4
	vpcmpgtq	ymm3, ymm2, ymm0
	vblendvpd	ymm0, ymm2, ymm0, ymm3
	vpcmpgtq	ymm2, ymm1, ymm0
	vblendvpd	ymm0, ymm1, ymm0, ymm2
	vextractf128	xmm1, ymm0, 1
	vpcmpgtq	xmm2, xmm1, xmm0
	vblendvpd	xmm0, xmm1, xmm0, xmm2
	vpermilps	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	vpcmpgtq	xmm2, xmm1, xmm0
	vblendvpd	xmm0, xmm1, xmm0, xmm2
	vmovq	rax, xmm0
	mov	rsi, r10
	cmp	r9, r8
	je	.LBB6_8
	.p2align	4, 0x90
.LBB6_7:                                # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdi + 8*r9]
	cmp	rax, rsi
	cmovg	rax, rsi
	cmp	r10, rsi
	cmovge	rsi, r10
	add	r9, 1
	mov	r10, rsi
	cmp	r8, r9
	jne	.LBB6_7
.LBB6_8:
	mov	qword ptr [rcx], rsi
	mov	qword ptr [rdx], rax
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end6:
	.size	int64_max_min_avx2, .Lfunc_end6-int64_max_min_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function uint64_max_min_avx2
.LCPI7_0:
	.quad	-9223372036854775808            # 0x8000000000000000
	.text
	.globl	uint64_max_min_avx2
	.p2align	4, 0x90
	.type	uint64_max_min_avx2,@function
uint64_max_min_avx2:                    # @uint64_max_min_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB7_1
# %bb.2:
	mov	r8d, esi
	cmp	esi, 15
	ja	.LBB7_4
# %bb.3:
	mov	rax, -1
	xor	r9d, r9d
	xor	r10d, r10d
	jmp	.LBB7_7
.LBB7_1:
	mov	rax, -1
	xor	esi, esi
	jmp	.LBB7_8
.LBB7_4:
	mov	r9d, r8d
	and	r9d, -16
	vpxor	xmm5, xmm5, xmm5
	vpcmpeqd	ymm1, ymm1, ymm1
	xor	eax, eax
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI7_0] # ymm0 = [9223372036854775808,9223372036854775808,9223372036854775808,9223372036854775808]
	vpcmpeqd	ymm4, ymm4, ymm4
	vpcmpeqd	ymm3, ymm3, ymm3
	vpcmpeqd	ymm2, ymm2, ymm2
	vpxor	xmm8, xmm8, xmm8
	vpxor	xmm7, xmm7, xmm7
	vpxor	xmm6, xmm6, xmm6
	.p2align	4, 0x90
.LBB7_5:                                # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm9, ymmword ptr [rdi + 8*rax]
	vpxor	ymm10, ymm1, ymm0
	vpxor	ymm11, ymm9, ymm0
	vpcmpgtq	ymm10, ymm11, ymm10
	vblendvpd	ymm1, ymm9, ymm1, ymm10
	vpxor	ymm10, ymm5, ymm0
	vpcmpgtq	ymm10, ymm10, ymm11
	vblendvpd	ymm5, ymm9, ymm5, ymm10
	vmovdqu	ymm9, ymmword ptr [rdi + 8*rax + 32]
	vpxor	ymm10, ymm4, ymm0
	vpxor	ymm11, ymm9, ymm0
	vpcmpgtq	ymm10, ymm11, ymm10
	vblendvpd	ymm4, ymm9, ymm4, ymm10
	vpxor	ymm10, ymm8, ymm0
	vpcmpgtq	ymm10, ymm10, ymm11
	vmovdqu	ymm11, ymmword ptr [rdi + 8*rax + 64]
	vblendvpd	ymm8, ymm9, ymm8, ymm10
	vpxor	ymm9, ymm3, ymm0
	vpxor	ymm10, ymm11, ymm0
	vpcmpgtq	ymm9, ymm10, ymm9
	vblendvpd	ymm3, ymm11, ymm3, ymm9
	vpxor	ymm9, ymm7, ymm0
	vpcmpgtq	ymm9, ymm9, ymm10
	vblendvpd	ymm7, ymm11, ymm7, ymm9
	vmovdqu	ymm9, ymmword ptr [rdi + 8*rax + 96]
	vpxor	ymm10, ymm2, ymm0
	vpxor	ymm11, ymm9, ymm0
	vpcmpgtq	ymm10, ymm11, ymm10
	vblendvpd	ymm2, ymm9, ymm2, ymm10
	vpxor	ymm10, ymm6, ymm0
	vpcmpgtq	ymm10, ymm10, ymm11
	vblendvpd	ymm6, ymm9, ymm6, ymm10
	add	rax, 16
	cmp	r9, rax
	jne	.LBB7_5
# %bb.6:
	vpxor	ymm9, ymm8, ymm0
	vpxor	ymm10, ymm5, ymm0
	vpcmpgtq	ymm9, ymm10, ymm9
	vblendvpd	ymm5, ymm8, ymm5, ymm9
	vxorpd	ymm8, ymm5, ymm0
	vpxor	ymm9, ymm7, ymm0
	vpcmpgtq	ymm8, ymm8, ymm9
	vblendvpd	ymm5, ymm7, ymm5, ymm8
	vxorpd	ymm7, ymm5, ymm0
	vpxor	ymm8, ymm6, ymm0
	vpcmpgtq	ymm7, ymm7, ymm8
	vblendvpd	ymm5, ymm6, ymm5, ymm7
	vextractf128	xmm6, ymm5, 1
	vxorpd	xmm8, xmm6, xmm0
	vxorpd	xmm7, xmm5, xmm0
	vpcmpgtq	xmm7, xmm7, xmm8
	vblendvpd	xmm5, xmm6, xmm5, xmm7
	vpermilps	xmm6, xmm5, 78          # xmm6 = xmm5[2,3,0,1]
	vxorpd	xmm8, xmm5, xmm0
	vxorpd	xmm7, xmm6, xmm0
	vpcmpgtq	xmm7, xmm8, xmm7
	vblendvpd	xmm5, xmm6, xmm5, xmm7
	vpxor	ymm6, ymm1, ymm0
	vpxor	ymm7, ymm4, ymm0
	vpcmpgtq	ymm6, ymm7, ymm6
	vblendvpd	ymm1, ymm4, ymm1, ymm6
	vxorpd	ymm4, ymm1, ymm0
	vpxor	ymm6, ymm3, ymm0
	vpcmpgtq	ymm4, ymm6, ymm4
	vblendvpd	ymm1, ymm3, ymm1, ymm4
	vmovq	r10, xmm5
	vxorpd	ymm3, ymm1, ymm0
	vpxor	ymm4, ymm2, ymm0
	vpcmpgtq	ymm3, ymm4, ymm3
	vblendvpd	ymm1, ymm2, ymm1, ymm3
	vextractf128	xmm2, ymm1, 1
	vxorpd	xmm3, xmm1, xmm0
	vxorpd	xmm4, xmm2, xmm0
	vpcmpgtq	xmm3, xmm4, xmm3
	vblendvpd	xmm1, xmm2, xmm1, xmm3
	vpermilps	xmm2, xmm1, 78          # xmm2 = xmm1[2,3,0,1]
	vxorpd	xmm3, xmm1, xmm0
	vxorpd	xmm0, xmm2, xmm0
	vpcmpgtq	xmm0, xmm0, xmm3
	vblendvpd	xmm0, xmm2, xmm1, xmm0
	vmovq	rax, xmm0
	mov	rsi, r10
	cmp	r9, r8
	je	.LBB7_8
	.p2align	4, 0x90
.LBB7_7:                                # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdi + 8*r9]
	cmp	rax, rsi
	cmovae	rax, rsi
	cmp	r10, rsi
	cmova	rsi, r10
	add	r9, 1
	mov	r10, rsi
	cmp	r8, r9
	jne	.LBB7_7
.LBB7_8:
	mov	qword ptr [rcx], rsi
	mov	qword ptr [rdx], rax
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end7:
	.size	uint64_max_min_avx2, .Lfunc_end7-uint64_max_min_avx2
                                        # -- End function
	.ident	"Debian clang version 11.0.1-2"
	.section	".note.GNU-stack","",@progbits
	.addrsig
