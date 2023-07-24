	.text
	.intel_syntax noprefix
	.file	"min_max.c"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function int8_max_min_sse4
.LCPI0_0:
	.zero	16,128
.LCPI0_1:
	.zero	16,127
	.text
	.globl	int8_max_min_sse4
	.p2align	4, 0x90
	.type	int8_max_min_sse4,@function
int8_max_min_sse4:                      # @int8_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB0_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 31
	ja	.LBB0_4
# %bb.3:
	mov	r8b, -128
	mov	sil, 127
	xor	r11d, r11d
	jmp	.LBB0_11
.LBB0_1:
	mov	sil, 127
	mov	r8b, -128
	jmp	.LBB0_12
.LBB0_4:
	mov	r11d, r9d
	and	r11d, -32
	lea	rax, [r11 - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_5
# %bb.6:
	mov	r10, r8
	and	r10, -2
	neg	r10
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_0] # xmm1 = [128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128]
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127]
	xor	eax, eax
	movdqa	xmm2, xmm0
	movdqa	xmm3, xmm1
	.p2align	4, 0x90
.LBB0_7:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + rax]
	movdqu	xmm5, xmmword ptr [rdi + rax + 16]
	movdqu	xmm6, xmmword ptr [rdi + rax + 32]
	movdqu	xmm7, xmmword ptr [rdi + rax + 48]
	pminsb	xmm0, xmm4
	pminsb	xmm2, xmm5
	pmaxsb	xmm1, xmm4
	pmaxsb	xmm3, xmm5
	pminsb	xmm0, xmm6
	pminsb	xmm2, xmm7
	pmaxsb	xmm1, xmm6
	pmaxsb	xmm3, xmm7
	add	rax, 64
	add	r10, 2
	jne	.LBB0_7
# %bb.8:
	test	r8b, 1
	je	.LBB0_10
.LBB0_9:
	movdqu	xmm4, xmmword ptr [rdi + rax]
	movdqu	xmm5, xmmword ptr [rdi + rax + 16]
	pmaxsb	xmm3, xmm5
	pmaxsb	xmm1, xmm4
	pminsb	xmm2, xmm5
	pminsb	xmm0, xmm4
.LBB0_10:
	pminsb	xmm0, xmm2
	pmaxsb	xmm1, xmm3
	pxor	xmm1, xmmword ptr [rip + .LCPI0_1]
	movdqa	xmm2, xmm1
	psrlw	xmm2, 8
	pminub	xmm2, xmm1
	phminposuw	xmm1, xmm2
	movd	r8d, xmm1
	xor	r8b, 127
	pxor	xmm0, xmmword ptr [rip + .LCPI0_0]
	movdqa	xmm1, xmm0
	psrlw	xmm1, 8
	pminub	xmm1, xmm0
	phminposuw	xmm0, xmm1
	movd	esi, xmm0
	xor	sil, -128
	cmp	r11, r9
	je	.LBB0_12
	.p2align	4, 0x90
.LBB0_11:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r11]
	cmp	sil, al
	movzx	esi, sil
	cmovg	esi, eax
	cmp	r8b, al
	movzx	r8d, r8b
	cmovl	r8d, eax
	add	r11, 1
	cmp	r9, r11
	jne	.LBB0_11
.LBB0_12:
	mov	byte ptr [rcx], r8b
	mov	byte ptr [rdx], sil
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_5:
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_0] # xmm1 = [128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128]
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127]
	xor	eax, eax
	movdqa	xmm2, xmm0
	movdqa	xmm3, xmm1
	test	r8b, 1
	jne	.LBB0_9
	jmp	.LBB0_10
.Lfunc_end0:
	.size	int8_max_min_sse4, .Lfunc_end0-int8_max_min_sse4
                                        # -- End function
	.globl	uint8_max_min_sse4              # -- Begin function uint8_max_min_sse4
	.p2align	4, 0x90
	.type	uint8_max_min_sse4,@function
uint8_max_min_sse4:                     # @uint8_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB1_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 31
	ja	.LBB1_4
# %bb.3:
	mov	sil, -1
	xor	r11d, r11d
	xor	eax, eax
	jmp	.LBB1_11
.LBB1_1:
	mov	sil, -1
	xor	eax, eax
	jmp	.LBB1_12
.LBB1_4:
	mov	r11d, r9d
	and	r11d, -32
	lea	rax, [r11 - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB1_5
# %bb.6:
	mov	r10, r8
	and	r10, -2
	neg	r10
	pxor	xmm1, xmm1
	pcmpeqd	xmm0, xmm0
	xor	eax, eax
	pcmpeqd	xmm2, xmm2
	pxor	xmm3, xmm3
	.p2align	4, 0x90
.LBB1_7:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + rax]
	movdqu	xmm5, xmmword ptr [rdi + rax + 16]
	movdqu	xmm6, xmmword ptr [rdi + rax + 32]
	movdqu	xmm7, xmmword ptr [rdi + rax + 48]
	pminub	xmm0, xmm4
	pminub	xmm2, xmm5
	pmaxub	xmm1, xmm4
	pmaxub	xmm3, xmm5
	pminub	xmm0, xmm6
	pminub	xmm2, xmm7
	pmaxub	xmm1, xmm6
	pmaxub	xmm3, xmm7
	add	rax, 64
	add	r10, 2
	jne	.LBB1_7
# %bb.8:
	test	r8b, 1
	je	.LBB1_10
.LBB1_9:
	movdqu	xmm4, xmmword ptr [rdi + rax]
	movdqu	xmm5, xmmword ptr [rdi + rax + 16]
	pmaxub	xmm3, xmm5
	pmaxub	xmm1, xmm4
	pminub	xmm2, xmm5
	pminub	xmm0, xmm4
.LBB1_10:
	pminub	xmm0, xmm2
	pmaxub	xmm1, xmm3
	pcmpeqd	xmm2, xmm2
	pxor	xmm2, xmm1
	movdqa	xmm1, xmm2
	psrlw	xmm1, 8
	pminub	xmm1, xmm2
	phminposuw	xmm1, xmm1
	movd	eax, xmm1
	not	al
	movdqa	xmm1, xmm0
	psrlw	xmm1, 8
	pminub	xmm1, xmm0
	phminposuw	xmm0, xmm1
	movd	esi, xmm0
	cmp	r11, r9
	je	.LBB1_12
	.p2align	4, 0x90
.LBB1_11:                               # =>This Inner Loop Header: Depth=1
	movzx	r8d, byte ptr [rdi + r11]
	cmp	sil, r8b
	movzx	esi, sil
	cmovae	esi, r8d
	cmp	al, r8b
	movzx	eax, al
	cmovbe	eax, r8d
	add	r11, 1
	cmp	r9, r11
	jne	.LBB1_11
.LBB1_12:
	mov	byte ptr [rcx], al
	mov	byte ptr [rdx], sil
	mov	rsp, rbp
	pop	rbp
	ret
.LBB1_5:
	pxor	xmm1, xmm1
	pcmpeqd	xmm0, xmm0
	xor	eax, eax
	pcmpeqd	xmm2, xmm2
	pxor	xmm3, xmm3
	test	r8b, 1
	jne	.LBB1_9
	jmp	.LBB1_10
.Lfunc_end1:
	.size	uint8_max_min_sse4, .Lfunc_end1-uint8_max_min_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function int16_max_min_sse4
.LCPI2_0:
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
	.text
	.globl	int16_max_min_sse4
	.p2align	4, 0x90
	.type	int16_max_min_sse4,@function
int16_max_min_sse4:                     # @int16_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB2_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 15
	ja	.LBB2_4
# %bb.3:
	mov	r8w, -32768
	mov	si, 32767
	xor	r11d, r11d
	jmp	.LBB2_11
.LBB2_1:
	mov	si, 32767
	mov	r8w, -32768
	jmp	.LBB2_12
.LBB2_4:
	mov	r11d, r9d
	and	r11d, -16
	lea	rax, [r11 - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB2_5
# %bb.6:
	mov	r10, r8
	and	r10, -2
	neg	r10
	movdqa	xmm1, xmmword ptr [rip + .LCPI2_0] # xmm1 = [32768,32768,32768,32768,32768,32768,32768,32768]
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [32767,32767,32767,32767,32767,32767,32767,32767]
	xor	eax, eax
	movdqa	xmm2, xmm0
	movdqa	xmm3, xmm1
	.p2align	4, 0x90
.LBB2_7:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + 2*rax]
	movdqu	xmm5, xmmword ptr [rdi + 2*rax + 16]
	movdqu	xmm6, xmmword ptr [rdi + 2*rax + 32]
	movdqu	xmm7, xmmword ptr [rdi + 2*rax + 48]
	pminsw	xmm0, xmm4
	pminsw	xmm2, xmm5
	pmaxsw	xmm1, xmm4
	pmaxsw	xmm3, xmm5
	pminsw	xmm0, xmm6
	pminsw	xmm2, xmm7
	pmaxsw	xmm1, xmm6
	pmaxsw	xmm3, xmm7
	add	rax, 32
	add	r10, 2
	jne	.LBB2_7
# %bb.8:
	test	r8b, 1
	je	.LBB2_10
.LBB2_9:
	movdqu	xmm4, xmmword ptr [rdi + 2*rax]
	movdqu	xmm5, xmmword ptr [rdi + 2*rax + 16]
	pmaxsw	xmm3, xmm5
	pmaxsw	xmm1, xmm4
	pminsw	xmm2, xmm5
	pminsw	xmm0, xmm4
.LBB2_10:
	pminsw	xmm0, xmm2
	pmaxsw	xmm1, xmm3
	pxor	xmm1, xmmword ptr [rip + .LCPI2_1]
	phminposuw	xmm1, xmm1
	movd	r8d, xmm1
	xor	r8d, 32767
	pxor	xmm0, xmmword ptr [rip + .LCPI2_0]
	phminposuw	xmm0, xmm0
	movd	esi, xmm0
	xor	esi, 32768
	cmp	r11, r9
	je	.LBB2_12
	.p2align	4, 0x90
.LBB2_11:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*r11]
	cmp	si, ax
	cmovg	esi, eax
	cmp	r8w, ax
	cmovl	r8d, eax
	add	r11, 1
	cmp	r9, r11
	jne	.LBB2_11
.LBB2_12:
	mov	word ptr [rcx], r8w
	mov	word ptr [rdx], si
	mov	rsp, rbp
	pop	rbp
	ret
.LBB2_5:
	movdqa	xmm1, xmmword ptr [rip + .LCPI2_0] # xmm1 = [32768,32768,32768,32768,32768,32768,32768,32768]
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [32767,32767,32767,32767,32767,32767,32767,32767]
	xor	eax, eax
	movdqa	xmm2, xmm0
	movdqa	xmm3, xmm1
	test	r8b, 1
	jne	.LBB2_9
	jmp	.LBB2_10
.Lfunc_end2:
	.size	int16_max_min_sse4, .Lfunc_end2-int16_max_min_sse4
                                        # -- End function
	.globl	uint16_max_min_sse4             # -- Begin function uint16_max_min_sse4
	.p2align	4, 0x90
	.type	uint16_max_min_sse4,@function
uint16_max_min_sse4:                    # @uint16_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB3_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 15
	ja	.LBB3_4
# %bb.3:
	mov	r8w, -1
	xor	r11d, r11d
	xor	esi, esi
	jmp	.LBB3_11
.LBB3_1:
	mov	r8w, -1
	xor	esi, esi
	jmp	.LBB3_12
.LBB3_4:
	mov	r11d, r9d
	and	r11d, -16
	lea	rax, [r11 - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_5
# %bb.6:
	mov	r10, r8
	and	r10, -2
	neg	r10
	pxor	xmm1, xmm1
	pcmpeqd	xmm0, xmm0
	xor	eax, eax
	pcmpeqd	xmm2, xmm2
	pxor	xmm3, xmm3
	.p2align	4, 0x90
.LBB3_7:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + 2*rax]
	movdqu	xmm5, xmmword ptr [rdi + 2*rax + 16]
	movdqu	xmm6, xmmword ptr [rdi + 2*rax + 32]
	movdqu	xmm7, xmmword ptr [rdi + 2*rax + 48]
	pminuw	xmm0, xmm4
	pminuw	xmm2, xmm5
	pmaxuw	xmm1, xmm4
	pmaxuw	xmm3, xmm5
	pminuw	xmm0, xmm6
	pminuw	xmm2, xmm7
	pmaxuw	xmm1, xmm6
	pmaxuw	xmm3, xmm7
	add	rax, 32
	add	r10, 2
	jne	.LBB3_7
# %bb.8:
	test	r8b, 1
	je	.LBB3_10
.LBB3_9:
	movdqu	xmm4, xmmword ptr [rdi + 2*rax]
	movdqu	xmm5, xmmword ptr [rdi + 2*rax + 16]
	pmaxuw	xmm3, xmm5
	pmaxuw	xmm1, xmm4
	pminuw	xmm2, xmm5
	pminuw	xmm0, xmm4
.LBB3_10:
	pminuw	xmm0, xmm2
	pmaxuw	xmm1, xmm3
	pcmpeqd	xmm2, xmm2
	pxor	xmm2, xmm1
	phminposuw	xmm1, xmm2
	movd	esi, xmm1
	not	esi
	phminposuw	xmm0, xmm0
	movd	r8d, xmm0
	cmp	r11, r9
	je	.LBB3_12
	.p2align	4, 0x90
.LBB3_11:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*r11]
	cmp	r8w, ax
	cmovae	r8d, eax
	cmp	si, ax
	cmovbe	esi, eax
	add	r11, 1
	cmp	r9, r11
	jne	.LBB3_11
.LBB3_12:
	mov	word ptr [rcx], si
	mov	word ptr [rdx], r8w
	mov	rsp, rbp
	pop	rbp
	ret
.LBB3_5:
	pxor	xmm1, xmm1
	pcmpeqd	xmm0, xmm0
	xor	eax, eax
	pcmpeqd	xmm2, xmm2
	pxor	xmm3, xmm3
	test	r8b, 1
	jne	.LBB3_9
	jmp	.LBB3_10
.Lfunc_end3:
	.size	uint16_max_min_sse4, .Lfunc_end3-uint16_max_min_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function int32_max_min_sse4
.LCPI4_0:
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
.LCPI4_1:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.text
	.globl	int32_max_min_sse4
	.p2align	4, 0x90
	.type	int32_max_min_sse4,@function
int32_max_min_sse4:                     # @int32_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB4_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 7
	ja	.LBB4_6
# %bb.3:
	mov	eax, -2147483648
	mov	r8d, 2147483647
	xor	r11d, r11d
	jmp	.LBB4_4
.LBB4_1:
	mov	r8d, 2147483647
	mov	eax, -2147483648
	jmp	.LBB4_13
.LBB4_6:
	mov	r11d, r9d
	and	r11d, -8
	lea	rax, [r11 - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB4_7
# %bb.8:
	mov	r10, r8
	and	r10, -2
	neg	r10
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [2147483648,2147483648,2147483648,2147483648]
	movdqa	xmm0, xmmword ptr [rip + .LCPI4_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
	xor	eax, eax
	movdqa	xmm2, xmm0
	movdqa	xmm3, xmm1
	.p2align	4, 0x90
.LBB4_9:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + 4*rax]
	movdqu	xmm5, xmmword ptr [rdi + 4*rax + 16]
	movdqu	xmm6, xmmword ptr [rdi + 4*rax + 32]
	movdqu	xmm7, xmmword ptr [rdi + 4*rax + 48]
	pminsd	xmm0, xmm4
	pminsd	xmm2, xmm5
	pmaxsd	xmm1, xmm4
	pmaxsd	xmm3, xmm5
	pminsd	xmm0, xmm6
	pminsd	xmm2, xmm7
	pmaxsd	xmm1, xmm6
	pmaxsd	xmm3, xmm7
	add	rax, 16
	add	r10, 2
	jne	.LBB4_9
# %bb.10:
	test	r8b, 1
	je	.LBB4_12
.LBB4_11:
	movdqu	xmm4, xmmword ptr [rdi + 4*rax]
	movdqu	xmm5, xmmword ptr [rdi + 4*rax + 16]
	pmaxsd	xmm3, xmm5
	pmaxsd	xmm1, xmm4
	pminsd	xmm2, xmm5
	pminsd	xmm0, xmm4
.LBB4_12:
	pminsd	xmm0, xmm2
	pmaxsd	xmm1, xmm3
	pshufd	xmm2, xmm1, 78                  # xmm2 = xmm1[2,3,0,1]
	pmaxsd	xmm2, xmm1
	pshufd	xmm1, xmm2, 229                 # xmm1 = xmm2[1,1,2,3]
	pmaxsd	xmm1, xmm2
	movd	eax, xmm1
	pshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	pminsd	xmm1, xmm0
	pshufd	xmm0, xmm1, 229                 # xmm0 = xmm1[1,1,2,3]
	pminsd	xmm0, xmm1
	movd	r8d, xmm0
	cmp	r11, r9
	je	.LBB4_13
.LBB4_4:
	mov	esi, eax
	.p2align	4, 0x90
.LBB4_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*r11]
	cmp	r8d, eax
	cmovg	r8d, eax
	cmp	esi, eax
	cmovge	eax, esi
	add	r11, 1
	mov	esi, eax
	cmp	r9, r11
	jne	.LBB4_5
.LBB4_13:
	mov	dword ptr [rcx], eax
	mov	dword ptr [rdx], r8d
	mov	rsp, rbp
	pop	rbp
	ret
.LBB4_7:
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [2147483648,2147483648,2147483648,2147483648]
	movdqa	xmm0, xmmword ptr [rip + .LCPI4_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
	xor	eax, eax
	movdqa	xmm2, xmm0
	movdqa	xmm3, xmm1
	test	r8b, 1
	jne	.LBB4_11
	jmp	.LBB4_12
.Lfunc_end4:
	.size	int32_max_min_sse4, .Lfunc_end4-int32_max_min_sse4
                                        # -- End function
	.globl	uint32_max_min_sse4             # -- Begin function uint32_max_min_sse4
	.p2align	4, 0x90
	.type	uint32_max_min_sse4,@function
uint32_max_min_sse4:                    # @uint32_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB5_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 7
	ja	.LBB5_6
# %bb.3:
	xor	r11d, r11d
	mov	r8d, -1
	xor	esi, esi
	jmp	.LBB5_4
.LBB5_1:
	mov	r8d, -1
	xor	esi, esi
	jmp	.LBB5_13
.LBB5_6:
	mov	r11d, r9d
	and	r11d, -8
	lea	rax, [r11 - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB5_7
# %bb.8:
	mov	r10, r8
	and	r10, -2
	neg	r10
	pxor	xmm1, xmm1
	pcmpeqd	xmm0, xmm0
	xor	eax, eax
	pcmpeqd	xmm2, xmm2
	pxor	xmm3, xmm3
	.p2align	4, 0x90
.LBB5_9:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdi + 4*rax]
	movdqu	xmm5, xmmword ptr [rdi + 4*rax + 16]
	movdqu	xmm6, xmmword ptr [rdi + 4*rax + 32]
	movdqu	xmm7, xmmword ptr [rdi + 4*rax + 48]
	pminud	xmm0, xmm4
	pminud	xmm2, xmm5
	pmaxud	xmm1, xmm4
	pmaxud	xmm3, xmm5
	pminud	xmm0, xmm6
	pminud	xmm2, xmm7
	pmaxud	xmm1, xmm6
	pmaxud	xmm3, xmm7
	add	rax, 16
	add	r10, 2
	jne	.LBB5_9
# %bb.10:
	test	r8b, 1
	je	.LBB5_12
.LBB5_11:
	movdqu	xmm4, xmmword ptr [rdi + 4*rax]
	movdqu	xmm5, xmmword ptr [rdi + 4*rax + 16]
	pmaxud	xmm3, xmm5
	pmaxud	xmm1, xmm4
	pminud	xmm2, xmm5
	pminud	xmm0, xmm4
.LBB5_12:
	pminud	xmm0, xmm2
	pmaxud	xmm1, xmm3
	pshufd	xmm2, xmm1, 78                  # xmm2 = xmm1[2,3,0,1]
	pmaxud	xmm2, xmm1
	pshufd	xmm1, xmm2, 229                 # xmm1 = xmm2[1,1,2,3]
	pmaxud	xmm1, xmm2
	movd	esi, xmm1
	pshufd	xmm1, xmm0, 78                  # xmm1 = xmm0[2,3,0,1]
	pminud	xmm1, xmm0
	pshufd	xmm0, xmm1, 229                 # xmm0 = xmm1[1,1,2,3]
	pminud	xmm0, xmm1
	movd	r8d, xmm0
	cmp	r11, r9
	je	.LBB5_13
.LBB5_4:
	mov	eax, esi
	.p2align	4, 0x90
.LBB5_5:                                # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdi + 4*r11]
	cmp	r8d, esi
	cmovae	r8d, esi
	cmp	eax, esi
	cmova	esi, eax
	add	r11, 1
	mov	eax, esi
	cmp	r9, r11
	jne	.LBB5_5
.LBB5_13:
	mov	dword ptr [rcx], esi
	mov	dword ptr [rdx], r8d
	mov	rsp, rbp
	pop	rbp
	ret
.LBB5_7:
	pxor	xmm1, xmm1
	pcmpeqd	xmm0, xmm0
	xor	eax, eax
	pcmpeqd	xmm2, xmm2
	pxor	xmm3, xmm3
	test	r8b, 1
	jne	.LBB5_11
	jmp	.LBB5_12
.Lfunc_end5:
	.size	uint32_max_min_sse4, .Lfunc_end5-uint32_max_min_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function int64_max_min_sse4
.LCPI6_0:
	.quad	-9223372036854775808            # 0x8000000000000000
	.quad	-9223372036854775808            # 0x8000000000000000
.LCPI6_1:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.text
	.globl	int64_max_min_sse4
	.p2align	4, 0x90
	.type	int64_max_min_sse4,@function
int64_max_min_sse4:                     # @int64_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	movabs	r8, 9223372036854775807
	test	esi, esi
	jle	.LBB6_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 3
	ja	.LBB6_6
# %bb.3:
	lea	rsi, [r8 + 1]
	xor	r11d, r11d
	jmp	.LBB6_4
.LBB6_1:
	lea	rsi, [r8 + 1]
	jmp	.LBB6_13
.LBB6_6:
	mov	r11d, r9d
	and	r11d, -4
	lea	rax, [r11 - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB6_7
# %bb.8:
	mov	r10, r8
	and	r10, -2
	neg	r10
	movdqa	xmm9, xmmword ptr [rip + .LCPI6_0] # xmm9 = [9223372036854775808,9223372036854775808]
	movdqa	xmm8, xmmword ptr [rip + .LCPI6_1] # xmm8 = [9223372036854775807,9223372036854775807]
	xor	eax, eax
	movdqa	xmm2, xmm8
	movdqa	xmm6, xmm9
	.p2align	4, 0x90
.LBB6_9:                                # =>This Inner Loop Header: Depth=1
	movdqu	xmm7, xmmword ptr [rdi + 8*rax]
	movdqa	xmm0, xmm7
	pcmpgtq	xmm0, xmm8
	movdqa	xmm4, xmm7
	blendvpd	xmm4, xmm8, xmm0
	movdqu	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movdqa	xmm0, xmm1
	pcmpgtq	xmm0, xmm2
	movdqa	xmm5, xmm1
	blendvpd	xmm5, xmm2, xmm0
	movdqa	xmm0, xmm9
	pcmpgtq	xmm0, xmm7
	blendvpd	xmm7, xmm9, xmm0
	movdqa	xmm0, xmm6
	pcmpgtq	xmm0, xmm1
	blendvpd	xmm1, xmm6, xmm0
	movdqu	xmm3, xmmword ptr [rdi + 8*rax + 32]
	movdqa	xmm0, xmm3
	pcmpgtq	xmm0, xmm4
	movdqa	xmm8, xmm3
	blendvpd	xmm8, xmm4, xmm0
	movdqu	xmm4, xmmword ptr [rdi + 8*rax + 48]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	movdqa	xmm2, xmm4
	blendvpd	xmm2, xmm5, xmm0
	movapd	xmm0, xmm7
	pcmpgtq	xmm0, xmm3
	blendvpd	xmm3, xmm7, xmm0
	movapd	xmm0, xmm1
	pcmpgtq	xmm0, xmm4
	blendvpd	xmm4, xmm1, xmm0
	add	rax, 8
	movapd	xmm9, xmm3
	movapd	xmm6, xmm4
	add	r10, 2
	jne	.LBB6_9
# %bb.10:
	test	r8b, 1
	je	.LBB6_12
.LBB6_11:
	movdqu	xmm1, xmmword ptr [rdi + 8*rax + 16]
	movapd	xmm0, xmm4
	pcmpgtq	xmm0, xmm1
	movdqa	xmm5, xmm1
	blendvpd	xmm5, xmm4, xmm0
	movdqu	xmm4, xmmword ptr [rdi + 8*rax]
	movapd	xmm0, xmm3
	pcmpgtq	xmm0, xmm4
	movdqa	xmm6, xmm4
	blendvpd	xmm6, xmm3, xmm0
	movdqa	xmm0, xmm1
	pcmpgtq	xmm0, xmm2
	blendvpd	xmm1, xmm2, xmm0
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm8
	blendvpd	xmm4, xmm8, xmm0
	movapd	xmm8, xmm4
	movapd	xmm2, xmm1
	movapd	xmm3, xmm6
	movapd	xmm4, xmm5
.LBB6_12:
	movapd	xmm0, xmm3
	pcmpgtq	xmm0, xmm4
	blendvpd	xmm4, xmm3, xmm0
	pshufd	xmm1, xmm4, 78                  # xmm1 = xmm4[2,3,0,1]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	movq	rsi, xmm1
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm8
	blendvpd	xmm2, xmm8, xmm0
	pshufd	xmm1, xmm2, 78                  # xmm1 = xmm2[2,3,0,1]
	movdqa	xmm0, xmm1
	pcmpgtq	xmm0, xmm2
	blendvpd	xmm1, xmm2, xmm0
	movq	r8, xmm1
	cmp	r11, r9
	je	.LBB6_13
.LBB6_4:
	mov	rax, rsi
	.p2align	4, 0x90
.LBB6_5:                                # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdi + 8*r11]
	cmp	r8, rsi
	cmovg	r8, rsi
	cmp	rax, rsi
	cmovge	rsi, rax
	add	r11, 1
	mov	rax, rsi
	cmp	r9, r11
	jne	.LBB6_5
.LBB6_13:
	mov	qword ptr [rcx], rsi
	mov	qword ptr [rdx], r8
	mov	rsp, rbp
	pop	rbp
	ret
.LBB6_7:
	movapd	xmm3, xmmword ptr [rip + .LCPI6_0] # xmm3 = [9223372036854775808,9223372036854775808]
	movdqa	xmm8, xmmword ptr [rip + .LCPI6_1] # xmm8 = [9223372036854775807,9223372036854775807]
	xor	eax, eax
	movdqa	xmm2, xmm8
	movapd	xmm4, xmm3
	test	r8b, 1
	jne	.LBB6_11
	jmp	.LBB6_12
.Lfunc_end6:
	.size	int64_max_min_sse4, .Lfunc_end6-int64_max_min_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function uint64_max_min_sse4
.LCPI7_0:
	.quad	-9223372036854775808            # 0x8000000000000000
	.quad	-9223372036854775808            # 0x8000000000000000
	.text
	.globl	uint64_max_min_sse4
	.p2align	4, 0x90
	.type	uint64_max_min_sse4,@function
uint64_max_min_sse4:                    # @uint64_max_min_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	esi, esi
	jle	.LBB7_1
# %bb.2:
	mov	r9d, esi
	cmp	esi, 3
	ja	.LBB7_6
# %bb.3:
	mov	r8, -1
	xor	r11d, r11d
	xor	eax, eax
	jmp	.LBB7_4
.LBB7_1:
	mov	r8, -1
	xor	eax, eax
	jmp	.LBB7_13
.LBB7_6:
	mov	r11d, r9d
	and	r11d, -4
	lea	rax, [r11 - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB7_7
# %bb.8:
	mov	r10, r8
	and	r10, -2
	neg	r10
	pxor	xmm9, xmm9
	pcmpeqd	xmm10, xmm10
	xor	eax, eax
	movdqa	xmm8, xmmword ptr [rip + .LCPI7_0] # xmm8 = [9223372036854775808,9223372036854775808]
	pcmpeqd	xmm11, xmm11
	pxor	xmm12, xmm12
	.p2align	4, 0x90
.LBB7_9:                                # =>This Inner Loop Header: Depth=1
	movdqa	xmm2, xmm10
	pxor	xmm2, xmm8
	movdqu	xmm4, xmmword ptr [rdi + 8*rax]
	movdqu	xmm5, xmmword ptr [rdi + 8*rax + 16]
	movdqu	xmm13, xmmword ptr [rdi + 8*rax + 32]
	movdqa	xmm0, xmm4
	pxor	xmm0, xmm8
	movdqa	xmm1, xmm9
	pxor	xmm1, xmm8
	pcmpgtq	xmm1, xmm0
	pcmpgtq	xmm0, xmm2
	movdqa	xmm3, xmm4
	blendvpd	xmm3, xmm10, xmm0
	movdqu	xmm6, xmmword ptr [rdi + 8*rax + 48]
	movdqa	xmm7, xmm11
	pxor	xmm7, xmm8
	movdqa	xmm0, xmm5
	pxor	xmm0, xmm8
	movdqa	xmm2, xmm12
	pxor	xmm2, xmm8
	pcmpgtq	xmm2, xmm0
	pcmpgtq	xmm0, xmm7
	movdqa	xmm7, xmm5
	blendvpd	xmm7, xmm11, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm4, xmm9, xmm0
	movdqa	xmm0, xmm2
	blendvpd	xmm5, xmm12, xmm0
	movapd	xmm2, xmm3
	xorpd	xmm2, xmm8
	movdqa	xmm0, xmm13
	pxor	xmm0, xmm8
	movapd	xmm1, xmm4
	xorpd	xmm1, xmm8
	pcmpgtq	xmm1, xmm0
	pcmpgtq	xmm0, xmm2
	movdqa	xmm10, xmm13
	blendvpd	xmm10, xmm3, xmm0
	movapd	xmm3, xmm7
	xorpd	xmm3, xmm8
	movdqa	xmm0, xmm6
	pxor	xmm0, xmm8
	movapd	xmm2, xmm5
	xorpd	xmm2, xmm8
	pcmpgtq	xmm2, xmm0
	pcmpgtq	xmm0, xmm3
	movdqa	xmm11, xmm6
	blendvpd	xmm11, xmm7, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm13, xmm4, xmm0
	movdqa	xmm0, xmm2
	blendvpd	xmm6, xmm5, xmm0
	add	rax, 8
	movapd	xmm9, xmm13
	movapd	xmm12, xmm6
	add	r10, 2
	jne	.LBB7_9
# %bb.10:
	test	r8b, 1
	je	.LBB7_12
.LBB7_11:
	movupd	xmm4, xmmword ptr [rdi + 8*rax]
	movupd	xmm3, xmmword ptr [rdi + 8*rax + 16]
	movapd	xmm5, xmmword ptr [rip + .LCPI7_0] # xmm5 = [9223372036854775808,9223372036854775808]
	movapd	xmm0, xmm6
	xorpd	xmm0, xmm5
	movapd	xmm1, xmm3
	xorpd	xmm1, xmm5
	pcmpgtq	xmm0, xmm1
	movapd	xmm7, xmm3
	blendvpd	xmm7, xmm6, xmm0
	movapd	xmm0, xmm13
	xorpd	xmm0, xmm5
	movapd	xmm2, xmm4
	xorpd	xmm2, xmm5
	pcmpgtq	xmm0, xmm2
	movapd	xmm6, xmm4
	blendvpd	xmm6, xmm13, xmm0
	movapd	xmm0, xmm11
	xorpd	xmm0, xmm5
	pcmpgtq	xmm1, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm11, xmm0
	xorpd	xmm5, xmm10
	pcmpgtq	xmm2, xmm5
	movdqa	xmm0, xmm2
	blendvpd	xmm4, xmm10, xmm0
	movapd	xmm10, xmm4
	movapd	xmm11, xmm3
	movapd	xmm13, xmm6
	movapd	xmm6, xmm7
.LBB7_12:
	movapd	xmm1, xmmword ptr [rip + .LCPI7_0] # xmm1 = [9223372036854775808,9223372036854775808]
	movapd	xmm2, xmm6
	xorpd	xmm2, xmm1
	movapd	xmm0, xmm13
	xorpd	xmm0, xmm1
	pcmpgtq	xmm0, xmm2
	blendvpd	xmm6, xmm13, xmm0
	pshufd	xmm2, xmm6, 78                  # xmm2 = xmm6[2,3,0,1]
	movapd	xmm0, xmm6
	xorpd	xmm0, xmm1
	movdqa	xmm3, xmm2
	pxor	xmm3, xmm1
	pcmpgtq	xmm0, xmm3
	blendvpd	xmm2, xmm6, xmm0
	movq	rax, xmm2
	movdqa	xmm2, xmm10
	pxor	xmm2, xmm1
	movdqa	xmm0, xmm11
	pxor	xmm0, xmm1
	pcmpgtq	xmm0, xmm2
	blendvpd	xmm11, xmm10, xmm0
	pshufd	xmm2, xmm11, 78                 # xmm2 = xmm11[2,3,0,1]
	movdqa	xmm0, xmm11
	pxor	xmm0, xmm1
	pxor	xmm1, xmm2
	pcmpgtq	xmm1, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm2, xmm11, xmm0
	movq	r8, xmm2
	cmp	r11, r9
	je	.LBB7_13
.LBB7_4:
	mov	rsi, rax
	.p2align	4, 0x90
.LBB7_5:                                # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r11]
	cmp	r8, rax
	cmovae	r8, rax
	cmp	rsi, rax
	cmova	rax, rsi
	add	r11, 1
	mov	rsi, rax
	cmp	r9, r11
	jne	.LBB7_5
.LBB7_13:
	mov	qword ptr [rcx], rax
	mov	qword ptr [rdx], r8
	mov	rsp, rbp
	pop	rbp
	ret
.LBB7_7:
	xorpd	xmm13, xmm13
	pcmpeqd	xmm10, xmm10
	xor	eax, eax
	pcmpeqd	xmm11, xmm11
	xorpd	xmm6, xmm6
	test	r8b, 1
	jne	.LBB7_11
	jmp	.LBB7_12
.Lfunc_end7:
	.size	uint64_max_min_sse4, .Lfunc_end7-uint64_max_min_sse4
                                        # -- End function
	.ident	"Debian clang version 11.0.1-2"
	.section	".note.GNU-stack","",@progbits
	.addrsig
