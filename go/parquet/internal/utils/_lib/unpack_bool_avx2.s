	.text
	.intel_syntax noprefix
	.file	"unpack_bool.c"
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function bytes_to_bools_avx2
.LCPI0_0:
	.long	24                              # 0x18
	.long	25                              # 0x19
	.long	26                              # 0x1a
	.long	27                              # 0x1b
	.long	28                              # 0x1c
	.long	29                              # 0x1d
	.long	30                              # 0x1e
	.long	31                              # 0x1f
.LCPI0_1:
	.long	16                              # 0x10
	.long	17                              # 0x11
	.long	18                              # 0x12
	.long	19                              # 0x13
	.long	20                              # 0x14
	.long	21                              # 0x15
	.long	22                              # 0x16
	.long	23                              # 0x17
.LCPI0_2:
	.long	8                               # 0x8
	.long	9                               # 0x9
	.long	10                              # 0xa
	.long	11                              # 0xb
	.long	12                              # 0xc
	.long	13                              # 0xd
	.long	14                              # 0xe
	.long	15                              # 0xf
.LCPI0_3:
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	2                               # 0x2
	.long	3                               # 0x3
	.long	4                               # 0x4
	.long	5                               # 0x5
	.long	6                               # 0x6
	.long	7                               # 0x7
.LCPI0_4:
	.zero	32,1
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3
.LCPI0_5:
	.quad	1                               # 0x1
.LCPI0_6:
	.quad	2                               # 0x2
.LCPI0_7:
	.quad	3                               # 0x3
.LCPI0_8:
	.quad	4                               # 0x4
.LCPI0_9:
	.quad	5                               # 0x5
.LCPI0_10:
	.quad	6                               # 0x6
.LCPI0_11:
	.quad	7                               # 0x7
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI0_12:
	.long	32                              # 0x20
	.text
	.globl	bytes_to_bools_avx2
	.p2align	4, 0x90
	.type	bytes_to_bools_avx2,@function
bytes_to_bools_avx2:                    # @bytes_to_bools_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r15
	push	r14
	push	r13
	push	r12
	push	rbx
	and	rsp, -32
	sub	rsp, 960
	test	esi, esi
	jle	.LBB0_1051
# %bb.1:
	mov	r9d, ecx
	mov	r8, rdx
	mov	r10d, esi
	cmp	esi, 32
	jae	.LBB0_3
.LBB0_2:
	xor	r12d, r12d
.LBB0_1055:
	lea	ecx, [8*r12]
	jmp	.LBB0_1057
	.p2align	4, 0x90
.LBB0_1056:                             #   in Loop: Header=BB0_1057 Depth=1
	add	r12, 1
	add	ecx, 8
	cmp	r10, r12
	je	.LBB0_1051
.LBB0_1057:                             # =>This Inner Loop Header: Depth=1
	mov	edx, ecx
	mov	ecx, ecx
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1058:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	edx, byte ptr [rdi + r12]
	and	dl, 1
	mov	byte ptr [r8 + rcx], dl
	mov	rdx, rcx
	or	rdx, 1
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1059:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl
	and	bl, 1
	mov	byte ptr [r8 + rdx], bl
	mov	rdx, rcx
	or	rdx, 2
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1060:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl, 2
	and	bl, 1
	mov	byte ptr [r8 + rdx], bl
	mov	rdx, rcx
	or	rdx, 3
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1061:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl, 3
	and	bl, 1
	mov	byte ptr [r8 + rdx], bl
	mov	rdx, rcx
	or	rdx, 4
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1062:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl, 4
	and	bl, 1
	mov	byte ptr [r8 + rdx], bl
	mov	rdx, rcx
	or	rdx, 5
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1063:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl, 5
	and	bl, 1
	mov	byte ptr [r8 + rdx], bl
	mov	rdx, rcx
	or	rdx, 6
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1064:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl, 6
	and	bl, 1
	mov	byte ptr [r8 + rdx], bl
	mov	rdx, rcx
	or	rdx, 7
	cmp	edx, r9d
	jge	.LBB0_1056
# %bb.1065:                             #   in Loop: Header=BB0_1057 Depth=1
	movzx	ebx, byte ptr [rdi + r12]
	shr	bl, 7
	mov	byte ptr [r8 + rdx], bl
	jmp	.LBB0_1056
.LBB0_3:
	mov	dword ptr [rsp + 16], r9d       # 4-byte Spill
	mov	qword ptr [rsp + 48], r10       # 8-byte Spill
	lea	rsi, [r10 - 1]
	mov	ecx, 8
	mov	eax, esi
	mul	ecx
	seto	r14b
	mov	rbx, rsi
	shr	rbx, 32
	lea	rcx, [r8 + 6]
	mov	edx, 8
	mov	rax, rsi
	mul	rdx
	seto	sil
	add	rcx, rax
	setb	dl
	lea	rcx, [r8 + 7]
	add	rcx, rax
	setb	r13b
	lea	rcx, [r8 + 5]
	add	rcx, rax
	setb	r9b
	lea	rcx, [r8 + 4]
	add	rcx, rax
	setb	r15b
	lea	rcx, [r8 + 3]
	add	rcx, rax
	setb	r11b
	lea	rcx, [r8 + 2]
	add	rcx, rax
	setb	r10b
	lea	rcx, [r8 + 1]
	add	rcx, rax
	setb	cl
	add	rax, r8
	setb	al
	xor	r12d, r12d
	test	rbx, rbx
	jne	.LBB0_1052
# %bb.4:
	test	r14b, r14b
	jne	.LBB0_1052
# %bb.5:
	test	dl, dl
	jne	.LBB0_1052
# %bb.6:
	test	sil, sil
	jne	.LBB0_1052
# %bb.7:
	test	r13b, r13b
	jne	.LBB0_1052
# %bb.8:
	test	sil, sil
	jne	.LBB0_1052
# %bb.9:
	test	r9b, r9b
	jne	.LBB0_1052
# %bb.10:
	test	sil, sil
	jne	.LBB0_1052
# %bb.11:
	test	r15b, r15b
	jne	.LBB0_1052
# %bb.12:
	test	sil, sil
	jne	.LBB0_1052
# %bb.13:
	test	r11b, r11b
	jne	.LBB0_1052
# %bb.14:
	test	sil, sil
	jne	.LBB0_1052
# %bb.15:
	test	r10b, r10b
	jne	.LBB0_1052
# %bb.16:
	test	sil, sil
	mov	r10, qword ptr [rsp + 48]       # 8-byte Reload
	jne	.LBB0_1054
# %bb.17:
	test	cl, cl
	jne	.LBB0_1054
# %bb.18:
	test	sil, sil
	mov	r9d, dword ptr [rsp + 16]       # 4-byte Reload
	jne	.LBB0_1055
# %bb.19:
	test	al, al
	jne	.LBB0_1055
# %bb.20:
	test	sil, sil
	jne	.LBB0_1055
# %bb.21:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdi
	jbe	.LBB0_24
# %bb.22:
	lea	rax, [rdi + r10]
	cmp	rax, r8
	ja	.LBB0_2
.LBB0_24:
	mov	r12d, r10d
	and	r12d, -32
	vmovd	xmm0, r9d
	vpbroadcastd	ymm0, xmm0
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI0_0] # ymm9 = [24,25,26,27,28,29,30,31]
	vmovdqa	ymm8, ymmword ptr [rip + .LCPI0_1] # ymm8 = [16,17,18,19,20,21,22,23]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_2] # ymm3 = [8,9,10,11,12,13,14,15]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_3] # ymm2 = [0,1,2,3,4,5,6,7]
	xor	r11d, r11d
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_5] # ymm1 = [1,1,1,1]
	vmovaps	ymmword ptr [rsp + 768], ymm1   # 32-byte Spill
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_6] # ymm1 = [2,2,2,2]
	vmovaps	ymmword ptr [rsp + 736], ymm1   # 32-byte Spill
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_7] # ymm1 = [3,3,3,3]
	vmovaps	ymmword ptr [rsp + 704], ymm1   # 32-byte Spill
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_8] # ymm1 = [4,4,4,4]
	vmovaps	ymmword ptr [rsp + 672], ymm1   # 32-byte Spill
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_9] # ymm1 = [5,5,5,5]
	vmovaps	ymmword ptr [rsp + 640], ymm1   # 32-byte Spill
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_10] # ymm1 = [6,6,6,6]
	vmovaps	ymmword ptr [rsp + 608], ymm1   # 32-byte Spill
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI0_11] # ymm1 = [7,7,7,7]
	vmovaps	ymmword ptr [rsp + 576], ymm1   # 32-byte Spill
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI0_12] # ymm1 = [32,32,32,32,32,32,32,32]
	vmovdqa	ymmword ptr [rsp + 544], ymm1   # 32-byte Spill
	jmp	.LBB0_26
	.p2align	4, 0x90
.LBB0_25:                               #   in Loop: Header=BB0_26 Depth=1
	add	r11, 32
	vmovdqa	ymm1, ymmword ptr [rsp + 544]   # 32-byte Reload
	vpaddd	ymm2, ymm2, ymm1
	vpaddd	ymm3, ymm3, ymm1
	vpaddd	ymm8, ymm8, ymm1
	vpaddd	ymm9, ymm9, ymm1
	cmp	r11, r12
	je	.LBB0_1050
.LBB0_26:                               # =>This Inner Loop Header: Depth=1
	vmovdqa	ymmword ptr [rsp + 800], ymm2   # 32-byte Spill
	vpslld	ymm1, ymm2, 3
	vpcmpgtd	xmm2, xmm0, xmm1
	vmovd	ecx, xmm2
                                        # implicit-def: $ymm4
	test	cl, 1
	je	.LBB0_28
# %bb.27:                               #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm4, byte ptr [rdi + r11]
.LBB0_28:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r10, r11
	or	r10, 1
	vpcmpgtd	xmm2, xmm0, xmm1
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpextrb	ecx, xmm2, 1
	test	cl, 1
	je	.LBB0_30
# %bb.29:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm2, xmm4, byte ptr [rdi + r10], 1
	vpblendd	ymm4, ymm4, ymm2, 15            # ymm4 = ymm2[0,1,2,3],ymm4[4,5,6,7]
.LBB0_30:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r14, r11
	or	r14, 2
	vpcmpgtd	xmm2, xmm0, xmm1
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpextrb	ecx, xmm2, 2
	test	cl, 1
	je	.LBB0_32
# %bb.31:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm2, xmm4, byte ptr [rdi + r14], 2
	vpblendd	ymm4, ymm4, ymm2, 15            # ymm4 = ymm2[0,1,2,3],ymm4[4,5,6,7]
.LBB0_32:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm13, ymm1, 1
	mov	rdx, r11
	or	rdx, 3
	vpcmpgtd	xmm2, xmm0, xmm1
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpextrb	ecx, xmm2, 3
	test	cl, 1
	je	.LBB0_34
# %bb.33:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm2, xmm4, byte ptr [rdi + rdx], 3
	vpblendd	ymm4, ymm4, ymm2, 15            # ymm4 = ymm2[0,1,2,3],ymm4[4,5,6,7]
.LBB0_34:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, r11
	or	rcx, 4
	vextracti128	xmm7, ymm0, 1
	vpcmpgtd	xmm2, xmm7, xmm13
	vpextrb	r9d, xmm2, 0
	test	r9b, 1
	mov	qword ptr [rsp + 272], rdx      # 8-byte Spill
	mov	qword ptr [rsp + 264], rcx      # 8-byte Spill
	je	.LBB0_36
# %bb.35:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm2, xmm4, byte ptr [rdi + rcx], 4
	vpblendd	ymm4, ymm4, ymm2, 15            # ymm4 = ymm2[0,1,2,3],ymm4[4,5,6,7]
.LBB0_36:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r15, r11
	or	r15, 5
	vpcmpgtd	ymm6, ymm0, ymm1
	vpackssdw	ymm2, ymm6, ymm0
	vextracti128	xmm2, ymm2, 1
	vpbroadcastd	xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpextrb	ecx, xmm2, 5
	test	cl, 1
	je	.LBB0_38
# %bb.37:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm2, xmm4, byte ptr [rdi + r15], 5
	vpblendd	ymm4, ymm4, ymm2, 15            # ymm4 = ymm2[0,1,2,3],ymm4[4,5,6,7]
.LBB0_38:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 6
	vpackssdw	ymm2, ymm6, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpacksswb	xmm2, xmm2, xmm2
	vpextrb	ecx, xmm2, 6
	test	cl, 1
	je	.LBB0_40
# %bb.39:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm2, xmm4, byte ptr [rdi + rbx], 6
	vpblendd	ymm4, ymm4, ymm2, 15            # ymm4 = ymm2[0,1,2,3],ymm4[4,5,6,7]
.LBB0_40:                               #   in Loop: Header=BB0_26 Depth=1
	vpslld	ymm2, ymm3, 3
	mov	rax, r11
	or	rax, 7
	vpackssdw	ymm5, ymm6, ymm0
	vpermq	ymm5, ymm5, 232                 # ymm5 = ymm5[0,2,2,3]
	vpacksswb	xmm5, xmm5, xmm5
	vpextrb	ecx, xmm5, 7
	test	cl, 1
	je	.LBB0_42
# %bb.41:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm5, xmm4, byte ptr [rdi + rax], 7
	vpblendd	ymm4, ymm4, ymm5, 15            # ymm4 = ymm5[0,1,2,3],ymm4[4,5,6,7]
.LBB0_42:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, r11
	or	rsi, 8
	vpcmpgtd	xmm5, xmm0, xmm2
	vpextrb	ecx, xmm5, 0
	test	cl, 1
	je	.LBB0_44
# %bb.43:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm5, xmm4, byte ptr [rdi + rsi], 8
	vpblendd	ymm4, ymm4, ymm5, 15            # ymm4 = ymm5[0,1,2,3],ymm4[4,5,6,7]
.LBB0_44:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, r11
	or	rdx, 9
	vpcmpgtd	xmm5, xmm0, xmm2
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpextrb	ecx, xmm5, 9
	test	cl, 1
	mov	qword ptr [rsp + 224], rdx      # 8-byte Spill
	je	.LBB0_46
# %bb.45:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm5, xmm4, byte ptr [rdi + rdx], 9
	vpblendd	ymm4, ymm4, ymm5, 15            # ymm4 = ymm5[0,1,2,3],ymm4[4,5,6,7]
.LBB0_46:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, r11
	or	rdx, 10
	vpcmpgtd	xmm5, xmm0, xmm2
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpextrb	ecx, xmm5, 10
	test	cl, 1
	vmovdqa	ymmword ptr [rsp + 832], ymm3   # 32-byte Spill
	mov	qword ptr [rsp + 96], rsi       # 8-byte Spill
	je	.LBB0_48
# %bb.47:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm5, xmm4, byte ptr [rdi + rdx], 10
	vpblendd	ymm4, ymm4, ymm5, 15            # ymm4 = ymm5[0,1,2,3],ymm4[4,5,6,7]
.LBB0_48:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm5, ymm2, 1
	mov	rsi, r11
	or	rsi, 11
	vpcmpgtd	xmm3, xmm0, xmm2
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpextrb	ecx, xmm3, 11
	test	cl, 1
	mov	qword ptr [rsp + 152], r10      # 8-byte Spill
	mov	qword ptr [rsp + 296], r14      # 8-byte Spill
	mov	qword ptr [rsp + 104], r15      # 8-byte Spill
	mov	qword ptr [rsp + 288], rbx      # 8-byte Spill
	mov	qword ptr [rsp + 232], rax      # 8-byte Spill
	je	.LBB0_50
# %bb.49:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm3, xmm4, byte ptr [rdi + rsi], 11
	vpblendd	ymm4, ymm4, ymm3, 15            # ymm4 = ymm3[0,1,2,3],ymm4[4,5,6,7]
.LBB0_50:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, r11
	or	rcx, 12
	vpcmpgtd	xmm3, xmm7, xmm5
	vpextrb	r14d, xmm3, 0
	test	r14b, 1
	mov	qword ptr [rsp + 256], rsi      # 8-byte Spill
	mov	qword ptr [rsp + 248], rcx      # 8-byte Spill
	je	.LBB0_52
# %bb.51:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm3, xmm4, byte ptr [rdi + rcx], 12
	vpblendd	ymm4, ymm4, ymm3, 15            # ymm4 = ymm3[0,1,2,3],ymm4[4,5,6,7]
.LBB0_52:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rax, r11
	or	rax, 13
	vpcmpgtd	ymm7, ymm0, ymm2
	vpackssdw	ymm3, ymm7, ymm0
	vextracti128	xmm3, ymm3, 1
	vpbroadcastd	xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpextrb	ecx, xmm3, 13
	test	cl, 1
	je	.LBB0_54
# %bb.53:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm3, xmm4, byte ptr [rdi + rax], 13
	vpblendd	ymm4, ymm4, ymm3, 15            # ymm4 = ymm3[0,1,2,3],ymm4[4,5,6,7]
.LBB0_54:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 14
	vpackssdw	ymm3, ymm7, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpacksswb	xmm3, xmm3, xmm3
	vpextrb	ecx, xmm3, 14
	test	cl, 1
	mov	qword ptr [rsp + 80], rbx       # 8-byte Spill
	je	.LBB0_56
# %bb.55:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm3, xmm4, byte ptr [rdi + rbx], 14
	vpblendd	ymm4, ymm4, ymm3, 15            # ymm4 = ymm3[0,1,2,3],ymm4[4,5,6,7]
.LBB0_56:                               #   in Loop: Header=BB0_26 Depth=1
	vpslld	ymm10, ymm8, 3
	mov	rsi, r11
	or	rsi, 15
	vpackssdw	ymm3, ymm7, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpacksswb	xmm3, xmm3, xmm3
	vpextrb	ecx, xmm3, 15
	test	cl, 1
	je	.LBB0_58
# %bb.57:                               #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm3, xmm4, byte ptr [rdi + rsi], 15
	vpblendd	ymm4, ymm4, ymm3, 15            # ymm4 = ymm3[0,1,2,3],ymm4[4,5,6,7]
.LBB0_58:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r15, r11
	or	r15, 16
	vpcmpgtd	xmm3, xmm0, xmm10
	vmovd	ecx, xmm3
	test	cl, 1
	mov	qword ptr [rsp + 64], r15       # 8-byte Spill
	mov	qword ptr [rsp + 72], rsi       # 8-byte Spill
	je	.LBB0_60
# %bb.59:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + r15], 0
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_60:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, r11
	or	rsi, 17
	vpcmpgtd	xmm3, xmm0, xmm10
	vpackssdw	xmm3, xmm3, xmm3
	vpermq	ymm3, ymm3, 212                 # ymm3 = ymm3[0,1,1,3]
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 1
	test	cl, 1
	je	.LBB0_62
# %bb.61:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rsi], 1
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_62:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 18
	vpcmpgtd	xmm3, xmm0, xmm10
	vpackssdw	xmm3, xmm3, xmm3
	vpermq	ymm3, ymm3, 212                 # ymm3 = ymm3[0,1,1,3]
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 2
	test	cl, 1
	je	.LBB0_64
# %bb.63:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 2
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_64:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r15, r11
	or	r15, 19
	vpcmpgtd	xmm3, xmm0, xmm10
	vpackssdw	xmm3, xmm3, xmm3
	vpermq	ymm3, ymm3, 212                 # ymm3 = ymm3[0,1,1,3]
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 3
	test	cl, 1
	vmovdqa	ymmword ptr [rsp + 864], ymm8   # 32-byte Spill
	je	.LBB0_66
# %bb.65:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + r15], 3
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_66:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r13, r11
	or	r13, 20
	vpcmpgtd	ymm8, ymm0, ymm10
	vpackssdw	ymm3, ymm0, ymm8
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 4
	test	cl, 1
	mov	qword ptr [rsp + 56], r13       # 8-byte Spill
	je	.LBB0_68
# %bb.67:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + r13], 4
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_68:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r13, r11
	or	r13, 21
	vpackssdw	ymm3, ymm0, ymm8
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 5
	test	cl, 1
	mov	qword ptr [rsp + 128], rbx      # 8-byte Spill
	je	.LBB0_70
# %bb.69:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + r13], 5
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_70:                               #   in Loop: Header=BB0_26 Depth=1
	mov	r10, r11
	or	r10, 22
	vpackssdw	ymm3, ymm0, ymm8
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 6
	test	cl, 1
	je	.LBB0_72
# %bb.71:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + r10], 6
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_72:                               #   in Loop: Header=BB0_26 Depth=1
	vpslld	ymm11, ymm9, 3
	mov	rbx, r11
	or	rbx, 23
	vpackssdw	ymm3, ymm0, ymm8
	vpacksswb	ymm3, ymm3, ymm0
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 7
	test	cl, 1
	mov	qword ptr [rsp + 240], rbx      # 8-byte Spill
	vmovdqa	ymmword ptr [rsp + 896], ymm9   # 32-byte Spill
	je	.LBB0_74
# %bb.73:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 7
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_74:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 24
	vpcmpgtd	ymm9, ymm0, ymm11
	vpermq	ymm12, ymm9, 68                 # ymm12 = ymm9[0,1,0,1]
	vpacksswb	ymm3, ymm0, ymm12
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 8
	test	cl, 1
	mov	qword ptr [rsp + 216], rbx      # 8-byte Spill
	je	.LBB0_76
# %bb.75:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 8
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_76:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 25
	vpcmpgtd	xmm3, xmm0, xmm11
	vpackssdw	xmm3, xmm3, xmm3
	vpermq	ymm3, ymm3, 212                 # ymm3 = ymm3[0,1,1,3]
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 9
	test	cl, 1
	mov	qword ptr [rsp + 208], rbx      # 8-byte Spill
	je	.LBB0_78
# %bb.77:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 9
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_78:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 26
	vpcmpgtd	xmm3, xmm0, xmm11
	vpackssdw	xmm3, xmm3, xmm3
	vpermq	ymm3, ymm3, 212                 # ymm3 = ymm3[0,1,1,3]
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 10
	test	cl, 1
	mov	qword ptr [rsp + 200], rbx      # 8-byte Spill
	je	.LBB0_80
# %bb.79:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 10
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_80:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 27
	vpcmpgtd	xmm3, xmm0, xmm11
	vpackssdw	xmm3, xmm3, xmm3
	vpermq	ymm3, ymm3, 212                 # ymm3 = ymm3[0,1,1,3]
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 11
	test	cl, 1
	mov	qword ptr [rsp + 192], rbx      # 8-byte Spill
	mov	qword ptr [rsp + 144], rdx      # 8-byte Spill
	mov	qword ptr [rsp + 88], rax       # 8-byte Spill
	je	.LBB0_82
# %bb.81:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 11
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_82:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, r11
	or	rdx, 28
	vpackssdw	ymm3, ymm0, ymm9
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 12
	test	cl, 1
	je	.LBB0_84
# %bb.83:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rdx], 12
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_84:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 29
	vpackssdw	ymm3, ymm0, ymm9
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 13
	test	cl, 1
	mov	qword ptr [rsp + 176], rbx      # 8-byte Spill
	je	.LBB0_86
# %bb.85:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 13
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_86:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 30
	vpackssdw	ymm3, ymm0, ymm9
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 14
	test	cl, 1
	mov	qword ptr [rsp + 168], rbx      # 8-byte Spill
	je	.LBB0_88
# %bb.87:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 14
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_88:                               #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, r11
	or	rbx, 31
	vpackssdw	ymm3, ymm0, ymm9
	vpacksswb	ymm3, ymm0, ymm3
	vextracti128	xmm3, ymm3, 1
	vpextrb	ecx, xmm3, 15
	test	cl, 1
	mov	qword ptr [rsp + 160], rbx      # 8-byte Spill
	je	.LBB0_90
# %bb.89:                               #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm3, ymm4, 1
	vpinsrb	xmm3, xmm3, byte ptr [rdi + rbx], 15
	vinserti128	ymm4, ymm4, xmm3, 1
.LBB0_90:                               #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm3, xmm1              # ymm3 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero
	vmovdqa	ymmword ptr [rsp + 512], ymm3   # 32-byte Spill
	vpand	ymm15, ymm4, ymmword ptr [rip + .LCPI0_4]
	vpcmpgtd	xmm3, xmm0, xmm1
	vmovd	ecx, xmm3
	test	cl, 1
	je	.LBB0_92
# %bb.91:                               #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm3, ymmword ptr [rsp + 512]   # 32-byte Reload
	vmovq	rcx, xmm3
	vpextrb	byte ptr [r8 + rcx], xmm15, 0
.LBB0_92:                               #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm3, xmm0, xmm1
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpextrb	ecx, xmm3, 1
	test	cl, 1
	je	.LBB0_94
# %bb.93:                               #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm3, ymmword ptr [rsp + 512]   # 32-byte Reload
	vpextrq	rcx, xmm3, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 1
.LBB0_94:                               #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm3, xmm0, xmm1
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpextrb	ecx, xmm3, 2
	test	cl, 1
	je	.LBB0_96
# %bb.95:                               #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm3, ymmword ptr [rsp + 512]   # 32-byte Reload
	vextracti128	xmm3, ymm3, 1
	vmovq	rcx, xmm3
	vpextrb	byte ptr [r8 + rcx], xmm15, 2
.LBB0_96:                               #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm1
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 3
	test	cl, 1
	je	.LBB0_98
# %bb.97:                               #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 512]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 3
.LBB0_98:                               #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm1, xmm13             # ymm1 = xmm13[0],zero,xmm13[1],zero,xmm13[2],zero,xmm13[3],zero
	vmovdqa	ymmword ptr [rsp + 480], ymm1   # 32-byte Spill
	test	r9b, 1
	je	.LBB0_100
# %bb.99:                               #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 480]   # 32-byte Reload
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm15, 4
.LBB0_100:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm6, ymm0
	vextracti128	xmm1, ymm1, 1
	vpbroadcastd	xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 5
	test	cl, 1
	je	.LBB0_102
# %bb.101:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 480]   # 32-byte Reload
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 5
.LBB0_102:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm6, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 6
	test	cl, 1
	je	.LBB0_104
# %bb.103:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 480]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm15, 6
.LBB0_104:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm6, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 7
	test	cl, 1
	je	.LBB0_106
# %bb.105:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 480]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 7
.LBB0_106:                              #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm1, xmm2              # ymm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero
	vmovdqa	ymmword ptr [rsp + 448], ymm1   # 32-byte Spill
	vpcmpgtd	xmm1, xmm0, xmm2
	vpextrb	ecx, xmm1, 0
	test	cl, 1
	je	.LBB0_108
# %bb.107:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 448]   # 32-byte Reload
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm15, 8
.LBB0_108:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_110
# %bb.109:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 448]   # 32-byte Reload
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 9
.LBB0_110:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 10
	test	cl, 1
	je	.LBB0_112
# %bb.111:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 448]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm15, 10
.LBB0_112:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 11
	test	cl, 1
	je	.LBB0_114
# %bb.113:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 448]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 11
.LBB0_114:                              #   in Loop: Header=BB0_26 Depth=1
	mov	qword ptr [rsp + 136], rsi      # 8-byte Spill
	vpmovzxdq	ymm1, xmm5              # ymm1 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero
	vmovdqa	ymmword ptr [rsp + 416], ymm1   # 32-byte Spill
	test	r14b, 1
	je	.LBB0_116
# %bb.115:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 416]   # 32-byte Reload
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm15, 12
.LBB0_116:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm7, ymm0
	vextracti128	xmm1, ymm1, 1
	vpbroadcastd	xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 13
	test	cl, 1
	mov	r9, qword ptr [rsp + 152]       # 8-byte Reload
	mov	rsi, qword ptr [rsp + 296]      # 8-byte Reload
	mov	r14, qword ptr [rsp + 104]      # 8-byte Reload
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	je	.LBB0_118
# %bb.117:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 416]   # 32-byte Reload
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 13
.LBB0_118:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm7, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 14
	test	cl, 1
	je	.LBB0_120
# %bb.119:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 416]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm15, 14
.LBB0_120:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm7, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpacksswb	xmm1, xmm1, xmm1
	vpextrb	ecx, xmm1, 15
	test	cl, 1
	je	.LBB0_122
# %bb.121:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 416]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm15, 15
.LBB0_122:                              #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm1, xmm10             # ymm1 = xmm10[0],zero,xmm10[1],zero,xmm10[2],zero,xmm10[3],zero
	vmovdqa	ymmword ptr [rsp + 384], ymm1   # 32-byte Spill
	vpcmpgtd	xmm1, xmm0, xmm10
	vmovd	ecx, xmm1
	test	cl, 1
	je	.LBB0_124
# %bb.123:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 384]   # 32-byte Reload
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 0
.LBB0_124:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm10
	vpackssdw	xmm1, xmm1, xmm1
	vpermq	ymm1, ymm1, 212                 # ymm1 = ymm1[0,1,1,3]
	vpacksswb	ymm1, ymm1, ymm0
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 1
	test	cl, 1
	je	.LBB0_126
# %bb.125:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 384]   # 32-byte Reload
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 1
.LBB0_126:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm10
	vpackssdw	xmm1, xmm1, xmm1
	vpermq	ymm1, ymm1, 212                 # ymm1 = ymm1[0,1,1,3]
	vpacksswb	ymm1, ymm1, ymm0
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 2
	test	cl, 1
	je	.LBB0_128
# %bb.127:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 384]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 2
.LBB0_128:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpcmpgtd	xmm2, xmm0, xmm10
	vpackssdw	xmm2, xmm2, xmm2
	vpermq	ymm2, ymm2, 212                 # ymm2 = ymm2[0,1,1,3]
	vpacksswb	ymm2, ymm2, ymm0
	vextracti128	xmm2, ymm2, 1
	vpextrb	ecx, xmm2, 3
	test	cl, 1
	je	.LBB0_130
# %bb.129:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm2, ymmword ptr [rsp + 384]   # 32-byte Reload
	vextracti128	xmm2, ymm2, 1
	vpextrq	rcx, xmm2, 1
	vextracti128	xmm2, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm2, 3
.LBB0_130:                              #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm1, xmm1              # ymm1 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero
	vmovdqa	ymmword ptr [rsp + 352], ymm1   # 32-byte Spill
	vpackssdw	ymm1, ymm0, ymm8
	vpacksswb	ymm1, ymm1, ymm0
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 4
	test	cl, 1
	je	.LBB0_132
# %bb.131:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 352]   # 32-byte Reload
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 4
.LBB0_132:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm0, ymm8
	vpacksswb	ymm1, ymm1, ymm0
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 5
	test	cl, 1
	je	.LBB0_134
# %bb.133:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 352]   # 32-byte Reload
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 5
.LBB0_134:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm0, ymm8
	vpacksswb	ymm1, ymm1, ymm0
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 6
	test	cl, 1
	je	.LBB0_136
# %bb.135:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 352]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 6
.LBB0_136:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm0, ymm8
	vpacksswb	ymm1, ymm1, ymm0
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 7
	test	cl, 1
	je	.LBB0_138
# %bb.137:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 352]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 7
.LBB0_138:                              #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm1, xmm11             # ymm1 = xmm11[0],zero,xmm11[1],zero,xmm11[2],zero,xmm11[3],zero
	vmovdqa	ymmword ptr [rsp + 320], ymm1   # 32-byte Spill
	vpacksswb	ymm1, ymm0, ymm12
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 8
	test	cl, 1
	je	.LBB0_140
# %bb.139:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 320]   # 32-byte Reload
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 8
.LBB0_140:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm11
	vpackssdw	xmm1, xmm1, xmm1
	vpermq	ymm1, ymm1, 212                 # ymm1 = ymm1[0,1,1,3]
	vpacksswb	ymm1, ymm0, ymm1
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_142
# %bb.141:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 320]   # 32-byte Reload
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
.LBB0_142:                              #   in Loop: Header=BB0_26 Depth=1
	vpcmpgtd	xmm1, xmm0, xmm11
	vpackssdw	xmm1, xmm1, xmm1
	vpermq	ymm1, ymm1, 212                 # ymm1 = ymm1[0,1,1,3]
	vpacksswb	ymm1, ymm0, ymm1
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 10
	test	cl, 1
	je	.LBB0_144
# %bb.143:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 320]   # 32-byte Reload
	vextracti128	xmm1, ymm1, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
.LBB0_144:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpcmpgtd	xmm4, xmm0, xmm11
	vpackssdw	xmm4, xmm4, xmm4
	vpermq	ymm4, ymm4, 212                 # ymm4 = ymm4[0,1,1,3]
	vpacksswb	ymm4, ymm0, ymm4
	vextracti128	xmm4, ymm4, 1
	vpextrb	ecx, xmm4, 11
	test	cl, 1
	je	.LBB0_146
# %bb.145:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm2, ymmword ptr [rsp + 320]   # 32-byte Reload
	vextracti128	xmm4, ymm2, 1
	vpextrq	rcx, xmm4, 1
	vextracti128	xmm4, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm4, 11
.LBB0_146:                              #   in Loop: Header=BB0_26 Depth=1
	vpmovzxdq	ymm4, xmm1              # ymm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero
	vpackssdw	ymm1, ymm0, ymm9
	vpacksswb	ymm1, ymm0, ymm1
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 12
	test	cl, 1
	je	.LBB0_148
# %bb.147:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm4
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
.LBB0_148:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm0, ymm9
	vpacksswb	ymm1, ymm0, ymm1
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 13
	test	cl, 1
	je	.LBB0_150
# %bb.149:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm4, 1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
.LBB0_150:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm0, ymm9
	vpacksswb	ymm1, ymm0, ymm1
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 14
	test	cl, 1
	je	.LBB0_152
# %bb.151:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
.LBB0_152:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm0, ymm9
	vpacksswb	ymm1, ymm0, ymm1
	vextracti128	xmm1, ymm1, 1
	vpextrb	ecx, xmm1, 15
	test	cl, 1
	je	.LBB0_154
# %bb.153:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_154:                              #   in Loop: Header=BB0_26 Depth=1
	vpackssdw	ymm1, ymm6, ymm8
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vpackssdw	ymm5, ymm7, ymm9
	vpermq	ymm5, ymm5, 216                 # ymm5 = ymm5[0,2,1,3]
	vpacksswb	ymm1, ymm1, ymm5
	vmovdqa	ymm2, ymmword ptr [rsp + 768]   # 32-byte Reload
	vpor	ymm15, ymm2, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm5, ymm2, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm10, ymm2, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm9, ymm2, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm12, ymm2, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm11, ymm2, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm8, ymm2, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm7, ymm4, ymm2
	vperm2i128	ymm6, ymm8, ymm7, 49    # ymm6 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm13, ymm8, xmm7, 1
	vshufps	ymm6, ymm13, ymm6, 136          # ymm6 = ymm13[0,2],ymm6[0,2],ymm13[4,6],ymm6[4,6]
	vperm2i128	ymm13, ymm12, ymm11, 49 # ymm13 = ymm12[2,3],ymm11[2,3]
	vinserti128	ymm14, ymm12, xmm11, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vperm2i128	ymm14, ymm10, ymm9, 49  # ymm14 = ymm10[2,3],ymm9[2,3]
	vinserti128	ymm2, ymm10, xmm9, 1
	vshufps	ymm2, ymm2, ymm14, 136          # ymm2 = ymm2[0,2],ymm14[0,2],ymm2[4,6],ymm14[4,6]
	vperm2i128	ymm14, ymm15, ymm5, 49  # ymm14 = ymm15[2,3],ymm5[2,3]
	vinserti128	ymm3, ymm15, xmm5, 1
	vshufps	ymm3, ymm3, ymm14, 136          # ymm3 = ymm3[0,2],ymm14[0,2],ymm3[4,6],ymm14[4,6]
	vpcmpgtd	ymm3, ymm0, ymm3
	vpcmpgtd	ymm2, ymm0, ymm2
	vpackssdw	ymm2, ymm3, ymm2
	vpcmpgtd	ymm3, ymm0, ymm13
	vpcmpgtd	ymm6, ymm0, ymm6
	vpackssdw	ymm3, ymm3, ymm6
	vpermq	ymm2, ymm2, 216                 # ymm2 = ymm2[0,2,1,3]
	vpermq	ymm3, ymm3, 216                 # ymm3 = ymm3[0,2,1,3]
	vpacksswb	ymm2, ymm2, ymm3
	vpand	ymm6, ymm2, ymm1
	vmovd	ecx, xmm6
                                        # implicit-def: $ymm14
	test	cl, 1
	je	.LBB0_155
# %bb.660:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm14, byte ptr [rdi + r11]
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_661
.LBB0_156:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rbx, qword ptr [rsp + 224]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_157
.LBB0_662:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 2
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	jne	.LBB0_663
.LBB0_158:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_159
.LBB0_664:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 4
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_665
.LBB0_160:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 232]      # 8-byte Reload
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_161
.LBB0_666:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 6
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_667
.LBB0_162:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_163
.LBB0_668:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 96]       # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 8
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_669
.LBB0_164:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_165
.LBB0_670:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 144]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 10
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_671
.LBB0_166:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_167
.LBB0_672:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 12
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_673
.LBB0_168:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_169
.LBB0_674:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 14
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	jne	.LBB0_170
	jmp	.LBB0_171
	.p2align	4, 0x90
.LBB0_155:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_156
.LBB0_661:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r9], 1
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rbx, qword ptr [rsp + 224]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_662
.LBB0_157:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_158
.LBB0_663:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 3
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_664
.LBB0_159:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_160
.LBB0_665:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r14], 5
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rsi, qword ptr [rsp + 232]      # 8-byte Reload
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_666
.LBB0_161:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_162
.LBB0_667:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 7
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_668
.LBB0_163:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_164
.LBB0_669:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 9
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	jne	.LBB0_670
.LBB0_165:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_166
.LBB0_671:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 11
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_672
.LBB0_167:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_168
.LBB0_673:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 88]       # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 13
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	jne	.LBB0_674
.LBB0_169:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_171
.LBB0_170:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 72]       # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 15
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_171:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 208]      # 8-byte Reload
	vextracti128	xmm13, ymm6, 1
	vmovd	eax, xmm13
	mov	dword ptr [rsp + 44], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_172
# %bb.675:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rax, qword ptr [rsp + 64]       # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 0
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 1
	mov	dword ptr [rsp + 40], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_676
.LBB0_173:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 2
	mov	dword ptr [rsp + 36], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_174
.LBB0_677:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rax, qword ptr [rsp + 128]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 2
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 3
	mov	dword ptr [rsp + 32], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_678
.LBB0_175:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 4
	mov	dword ptr [rsp + 28], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_176
.LBB0_679:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rax, qword ptr [rsp + 56]       # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 4
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_680
.LBB0_177:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 6
	mov	dword ptr [rsp + 20], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_178
.LBB0_681:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + r10], 6
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 7
	mov	dword ptr [rsp + 316], eax      # 4-byte Spill
	test	al, 1
	jne	.LBB0_682
.LBB0_179:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpextrb	ebx, xmm13, 8
	test	bl, 1
	je	.LBB0_181
.LBB0_180:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 8
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_181:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm13, 9
	test	r9b, 1
	mov	qword ptr [rsp + 280], r13      # 8-byte Spill
	mov	qword ptr [rsp + 112], r10      # 8-byte Spill
	mov	qword ptr [rsp + 184], rdx      # 8-byte Spill
	je	.LBB0_183
# %bb.182:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rcx], 9
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_183:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	mov	rcx, qword ptr [rsp + 192]      # 8-byte Reload
	vpextrb	r13d, xmm13, 10
	test	r13b, 1
	je	.LBB0_184
# %bb.683:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 10
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 11
	test	al, 1
	mov	qword ptr [rsp + 120], r15      # 8-byte Spill
	jne	.LBB0_684
.LBB0_185:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r15d, xmm13, 12
	test	r15b, 1
	mov	qword ptr [rsp + 304], r11      # 8-byte Spill
	je	.LBB0_186
.LBB0_685:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rcx, qword ptr [rsp + 184]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rcx], 12
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	edx, xmm13, 13
	test	dl, 1
	jne	.LBB0_686
.LBB0_187:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm13, 14
	test	sil, 1
	je	.LBB0_188
.LBB0_687:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rcx, qword ptr [rsp + 168]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rcx], 14
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	r14d, xmm13, 15
	test	r14b, 1
	jne	.LBB0_189
	jmp	.LBB0_190
	.p2align	4, 0x90
.LBB0_172:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 1
	mov	dword ptr [rsp + 40], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_173
.LBB0_676:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rax, qword ptr [rsp + 136]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 1
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 2
	mov	dword ptr [rsp + 36], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_677
.LBB0_174:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 3
	mov	dword ptr [rsp + 32], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_175
.LBB0_678:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + r15], 3
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 4
	mov	dword ptr [rsp + 28], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_679
.LBB0_176:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_177
.LBB0_680:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + r13], 5
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	eax, xmm13, 6
	mov	dword ptr [rsp + 20], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_681
.LBB0_178:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 7
	mov	dword ptr [rsp + 316], eax      # 4-byte Spill
	test	al, 1
	je	.LBB0_179
.LBB0_682:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 7
	vinserti128	ymm14, ymm14, xmm1, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpextrb	ebx, xmm13, 8
	test	bl, 1
	jne	.LBB0_180
	jmp	.LBB0_181
	.p2align	4, 0x90
.LBB0_184:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm13, 11
	test	al, 1
	mov	qword ptr [rsp + 120], r15      # 8-byte Spill
	je	.LBB0_185
.LBB0_684:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rcx], 11
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	r15d, xmm13, 12
	test	r15b, 1
	mov	qword ptr [rsp + 304], r11      # 8-byte Spill
	jne	.LBB0_685
.LBB0_186:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	edx, xmm13, 13
	test	dl, 1
	je	.LBB0_187
.LBB0_686:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rcx, qword ptr [rsp + 176]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rcx], 13
	vinserti128	ymm14, ymm14, xmm1, 1
	vpextrb	esi, xmm13, 14
	test	sil, 1
	jne	.LBB0_687
.LBB0_188:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r14d, xmm13, 15
	test	r14b, 1
	je	.LBB0_190
.LBB0_189:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rcx, qword ptr [rsp + 160]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rcx], 15
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_190:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm14, 1
	vpand	ymm14, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r10d, xmm6
	test	r10b, 1
	je	.LBB0_191
# %bb.688:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm15
	vpextrb	byte ptr [r8 + rcx], xmm14, 0
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_689
.LBB0_192:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_193
.LBB0_690:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm14, 2
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	jne	.LBB0_691
.LBB0_194:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_195
.LBB0_692:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm5
	vpextrb	byte ptr [r8 + rcx], xmm14, 4
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_693
.LBB0_196:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_197
.LBB0_694:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm14, 6
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_695
.LBB0_198:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_199
.LBB0_696:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm12
	vpextrb	byte ptr [r8 + rcx], xmm14, 8
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_697
.LBB0_200:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_201
.LBB0_698:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm14, 10
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_699
.LBB0_202:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_203
.LBB0_700:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm11
	vpextrb	byte ptr [r8 + rcx], xmm14, 12
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_701
.LBB0_204:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_205
.LBB0_702:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rcx, xmm1
	vpextrb	byte ptr [r8 + rcx], xmm14, 14
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	jne	.LBB0_703
.LBB0_206:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	je	.LBB0_207
.LBB0_704:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm10
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 0
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	jne	.LBB0_705
.LBB0_208:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	je	.LBB0_209
.LBB0_706:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 2
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	jne	.LBB0_707
.LBB0_210:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	je	.LBB0_211
.LBB0_708:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm9
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 4
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	jne	.LBB0_709
.LBB0_212:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	je	.LBB0_213
.LBB0_710:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 6
	test	byte ptr [rsp + 316], 1         # 1-byte Folded Reload
	jne	.LBB0_711
.LBB0_214:                              #   in Loop: Header=BB0_26 Depth=1
	test	bl, 1
	je	.LBB0_215
.LBB0_712:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm8
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 8
	test	r9b, 1
	mov	r10, qword ptr [rsp + 224]      # 8-byte Reload
	mov	r11, qword ptr [rsp + 144]      # 8-byte Reload
	jne	.LBB0_713
.LBB0_216:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	je	.LBB0_217
.LBB0_714:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	mov	r9, qword ptr [rsp + 288]       # 8-byte Reload
	mov	rax, qword ptr [rsp + 232]      # 8-byte Reload
	jne	.LBB0_715
.LBB0_218:                              #   in Loop: Header=BB0_26 Depth=1
	test	r15b, 1
	je	.LBB0_219
.LBB0_716:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm7
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	dl, 1
	mov	r13, qword ptr [rsp + 136]      # 8-byte Reload
	mov	r15, qword ptr [rsp + 128]      # 8-byte Reload
	jne	.LBB0_717
.LBB0_220:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_221
.LBB0_718:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	jne	.LBB0_222
	jmp	.LBB0_223
	.p2align	4, 0x90
.LBB0_191:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_192
.LBB0_689:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm15, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 1
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_690
.LBB0_193:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_194
.LBB0_691:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 3
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_692
.LBB0_195:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_196
.LBB0_693:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm5, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 5
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_694
.LBB0_197:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_198
.LBB0_695:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 7
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_696
.LBB0_199:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_200
.LBB0_697:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm12, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 9
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	jne	.LBB0_698
.LBB0_201:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_202
.LBB0_699:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 11
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_700
.LBB0_203:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_204
.LBB0_701:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm11, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 13
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	jne	.LBB0_702
.LBB0_205:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_206
.LBB0_703:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rcx, xmm1, 1
	vpextrb	byte ptr [r8 + rcx], xmm14, 15
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	jne	.LBB0_704
.LBB0_207:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	je	.LBB0_208
.LBB0_705:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm10, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	jne	.LBB0_706
.LBB0_209:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	je	.LBB0_210
.LBB0_707:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 3
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	jne	.LBB0_708
.LBB0_211:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	je	.LBB0_212
.LBB0_709:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm9, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 5
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	jne	.LBB0_710
.LBB0_213:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 316], 1         # 1-byte Folded Reload
	je	.LBB0_214
.LBB0_711:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 7
	test	bl, 1
	jne	.LBB0_712
.LBB0_215:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	mov	r10, qword ptr [rsp + 224]      # 8-byte Reload
	mov	r11, qword ptr [rsp + 144]      # 8-byte Reload
	je	.LBB0_216
.LBB0_713:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm8, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	r13b, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	jne	.LBB0_714
.LBB0_217:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	mov	r9, qword ptr [rsp + 288]       # 8-byte Reload
	mov	rax, qword ptr [rsp + 232]      # 8-byte Reload
	je	.LBB0_218
.LBB0_715:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	r15b, 1
	jne	.LBB0_716
.LBB0_219:                              #   in Loop: Header=BB0_26 Depth=1
	test	dl, 1
	mov	r13, qword ptr [rsp + 136]      # 8-byte Reload
	mov	r15, qword ptr [rsp + 128]      # 8-byte Reload
	je	.LBB0_220
.LBB0_717:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm7, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_718
.LBB0_221:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	je	.LBB0_223
.LBB0_222:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_223:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 736]   # 32-byte Reload
	vpor	ymm15, ymm1, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm5, ymm1, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm10, ymm1, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm9, ymm1, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm12, ymm1, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm11, ymm1, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm8, ymm1, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm7, ymm4, ymm1
	vperm2i128	ymm1, ymm8, ymm7, 49    # ymm1 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm2, ymm8, xmm7, 1
	vshufps	ymm1, ymm2, ymm1, 136           # ymm1 = ymm2[0,2],ymm1[0,2],ymm2[4,6],ymm1[4,6]
	vperm2i128	ymm2, ymm12, ymm11, 49  # ymm2 = ymm12[2,3],ymm11[2,3]
	vinserti128	ymm3, ymm12, xmm11, 1
	vshufps	ymm2, ymm3, ymm2, 136           # ymm2 = ymm3[0,2],ymm2[0,2],ymm3[4,6],ymm2[4,6]
	vperm2i128	ymm3, ymm10, ymm9, 49   # ymm3 = ymm10[2,3],ymm9[2,3]
	vinserti128	ymm13, ymm10, xmm9, 1
	vshufps	ymm3, ymm13, ymm3, 136          # ymm3 = ymm13[0,2],ymm3[0,2],ymm13[4,6],ymm3[4,6]
	vperm2i128	ymm13, ymm15, ymm5, 49  # ymm13 = ymm15[2,3],ymm5[2,3]
	vinserti128	ymm14, ymm15, xmm5, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vpcmpgtd	ymm13, ymm0, ymm13
	vpcmpgtd	ymm3, ymm0, ymm3
	vpackssdw	ymm3, ymm13, ymm3
	vpcmpgtd	ymm2, ymm0, ymm2
	vpcmpgtd	ymm1, ymm0, ymm1
	vpackssdw	ymm1, ymm2, ymm1
	vpermq	ymm2, ymm3, 216                 # ymm2 = ymm3[0,2,1,3]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vpacksswb	ymm1, ymm2, ymm1
	vpand	ymm6, ymm1, ymm6
	vmovd	ecx, xmm6
                                        # implicit-def: $ymm14
	test	cl, 1
	je	.LBB0_224
# %bb.719:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm14, byte ptr [rdi + rdx]
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_720
.LBB0_225:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_227
.LBB0_226:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 2
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_227:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 96]       # 8-byte Reload
	mov	rbx, qword ptr [rsp + 72]       # 8-byte Reload
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_228
# %bb.721:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 3
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_722
.LBB0_229:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_230
.LBB0_723:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 5
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_724
.LBB0_231:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_232
.LBB0_725:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 7
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_726
.LBB0_233:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_234
.LBB0_727:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r10], 9
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	jne	.LBB0_728
.LBB0_235:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_236
.LBB0_729:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 11
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_730
.LBB0_237:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_239
.LBB0_238:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 13
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_239:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	mov	rdx, qword ptr [rsp + 64]       # 8-byte Reload
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_241
# %bb.240:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 14
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_241:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_243
# %bb.242:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 15
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_243:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm6, 1
	vmovd	eax, xmm1
	mov	dword ptr [rsp + 44], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_245
# %bb.244:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rdx], 0
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_245:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 56]       # 8-byte Reload
	vpextrb	eax, xmm1, 1
	mov	dword ptr [rsp + 40], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_247
# %bb.246:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r13], 1
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_247:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 280]      # 8-byte Reload
	mov	rsi, qword ptr [rsp + 112]      # 8-byte Reload
	vpextrb	eax, xmm1, 2
	mov	dword ptr [rsp + 36], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_249
# %bb.248:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r15], 2
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_249:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 120]      # 8-byte Reload
	vpextrb	ebx, xmm1, 3
	mov	dword ptr [rsp + 32], ebx       # 4-byte Spill
	test	bl, 1
	je	.LBB0_250
# %bb.731:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 3
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 4
	mov	dword ptr [rsp + 28], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_732
.LBB0_251:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_252
.LBB0_733:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rdx], 5
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 6
	mov	dword ptr [rsp + 20], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_734
.LBB0_253:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	je	.LBB0_254
.LBB0_735:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 7
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	jne	.LBB0_736
.LBB0_255:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_256
.LBB0_737:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 208]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 9
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	jne	.LBB0_738
.LBB0_257:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 11
	test	al, 1
	je	.LBB0_258
.LBB0_739:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 192]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 11
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	jne	.LBB0_740
.LBB0_259:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	je	.LBB0_260
.LBB0_741:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 176]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 13
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	jne	.LBB0_742
.LBB0_261:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	je	.LBB0_263
.LBB0_262:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rbx, qword ptr [rsp + 160]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 15
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_263:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm14, 2
	vpand	ymm14, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r15d, xmm6
	test	r15b, 1
	je	.LBB0_264
# %bb.743:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm15
	vpextrb	byte ptr [r8 + rbx], xmm14, 0
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	jne	.LBB0_744
.LBB0_265:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	je	.LBB0_266
.LBB0_745:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 2
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	jne	.LBB0_746
.LBB0_267:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	je	.LBB0_268
.LBB0_747:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm5
	vpextrb	byte ptr [r8 + rbx], xmm14, 4
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	jne	.LBB0_748
.LBB0_269:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	je	.LBB0_270
.LBB0_749:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 6
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	jne	.LBB0_750
.LBB0_271:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	je	.LBB0_272
.LBB0_751:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm12
	vpextrb	byte ptr [r8 + rbx], xmm14, 8
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	jne	.LBB0_752
.LBB0_273:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	je	.LBB0_274
.LBB0_753:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 10
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	jne	.LBB0_754
.LBB0_275:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	je	.LBB0_276
.LBB0_755:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm11
	vpextrb	byte ptr [r8 + rbx], xmm14, 12
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	jne	.LBB0_756
.LBB0_277:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	je	.LBB0_278
.LBB0_757:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 14
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	jne	.LBB0_758
.LBB0_279:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	je	.LBB0_280
.LBB0_759:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm10
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 0
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	jne	.LBB0_760
.LBB0_281:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	je	.LBB0_282
.LBB0_761:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 2
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	jne	.LBB0_762
.LBB0_283:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	je	.LBB0_284
.LBB0_763:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm9
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 4
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	jne	.LBB0_764
.LBB0_285:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	je	.LBB0_286
.LBB0_765:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 6
	test	r9b, 1
	jne	.LBB0_766
.LBB0_287:                              #   in Loop: Header=BB0_26 Depth=1
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	je	.LBB0_288
.LBB0_767:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rdx, xmm8
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rdx], xmm1, 8
	test	cl, 1
	jne	.LBB0_768
.LBB0_289:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_290
.LBB0_769:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	jne	.LBB0_770
.LBB0_291:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	je	.LBB0_292
.LBB0_771:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm7
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	jne	.LBB0_772
.LBB0_293:                              #   in Loop: Header=BB0_26 Depth=1
	test	r11b, 1
	je	.LBB0_294
.LBB0_773:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	jne	.LBB0_295
	jmp	.LBB0_296
	.p2align	4, 0x90
.LBB0_224:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_225
.LBB0_720:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 1
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_226
	jmp	.LBB0_227
	.p2align	4, 0x90
.LBB0_228:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_229
.LBB0_722:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 4
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_723
.LBB0_230:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_231
.LBB0_724:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r9], 6
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_725
.LBB0_232:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_233
.LBB0_726:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 8
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_727
.LBB0_234:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_235
.LBB0_728:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r11], 10
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_729
.LBB0_236:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_237
.LBB0_730:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 12
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_238
	jmp	.LBB0_239
	.p2align	4, 0x90
.LBB0_250:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 4
	mov	dword ptr [rsp + 28], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_251
.LBB0_732:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rcx], 4
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_733
.LBB0_252:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 6
	mov	dword ptr [rsp + 20], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_253
.LBB0_734:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rsi], 6
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	jne	.LBB0_735
.LBB0_254:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	je	.LBB0_255
.LBB0_736:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 8
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	jne	.LBB0_737
.LBB0_256:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	je	.LBB0_257
.LBB0_738:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 10
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 11
	test	al, 1
	jne	.LBB0_739
.LBB0_258:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	je	.LBB0_259
.LBB0_740:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 184]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 12
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	jne	.LBB0_741
.LBB0_260:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	je	.LBB0_261
.LBB0_742:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 168]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 14
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	jne	.LBB0_262
	jmp	.LBB0_263
	.p2align	4, 0x90
.LBB0_264:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	je	.LBB0_265
.LBB0_744:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm15, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	jne	.LBB0_745
.LBB0_266:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	je	.LBB0_267
.LBB0_746:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 3
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	jne	.LBB0_747
.LBB0_268:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	je	.LBB0_269
.LBB0_748:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm5, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 5
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	jne	.LBB0_749
.LBB0_270:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	je	.LBB0_271
.LBB0_750:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 7
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	jne	.LBB0_751
.LBB0_272:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	je	.LBB0_273
.LBB0_752:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm12, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 9
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	jne	.LBB0_753
.LBB0_274:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	je	.LBB0_275
.LBB0_754:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 11
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	jne	.LBB0_755
.LBB0_276:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	je	.LBB0_277
.LBB0_756:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm11, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 13
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	jne	.LBB0_757
.LBB0_278:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	je	.LBB0_279
.LBB0_758:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 15
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	jne	.LBB0_759
.LBB0_280:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	je	.LBB0_281
.LBB0_760:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm10, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	jne	.LBB0_761
.LBB0_282:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	je	.LBB0_283
.LBB0_762:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 3
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	jne	.LBB0_763
.LBB0_284:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	je	.LBB0_285
.LBB0_764:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm9, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 5
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	jne	.LBB0_765
.LBB0_286:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	je	.LBB0_287
.LBB0_766:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 7
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	jne	.LBB0_767
.LBB0_288:                              #   in Loop: Header=BB0_26 Depth=1
	test	cl, 1
	je	.LBB0_289
.LBB0_768:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm8, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_769
.LBB0_290:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	je	.LBB0_291
.LBB0_770:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	r13b, 1
	jne	.LBB0_771
.LBB0_292:                              #   in Loop: Header=BB0_26 Depth=1
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	je	.LBB0_293
.LBB0_772:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm7, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	r11b, 1
	jne	.LBB0_773
.LBB0_294:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	je	.LBB0_296
.LBB0_295:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_296:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 704]   # 32-byte Reload
	vpor	ymm15, ymm1, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm5, ymm1, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm10, ymm1, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm9, ymm1, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm12, ymm1, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm11, ymm1, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm8, ymm1, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm7, ymm4, ymm1
	vperm2i128	ymm1, ymm8, ymm7, 49    # ymm1 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm2, ymm8, xmm7, 1
	vshufps	ymm1, ymm2, ymm1, 136           # ymm1 = ymm2[0,2],ymm1[0,2],ymm2[4,6],ymm1[4,6]
	vperm2i128	ymm2, ymm12, ymm11, 49  # ymm2 = ymm12[2,3],ymm11[2,3]
	vinserti128	ymm3, ymm12, xmm11, 1
	vshufps	ymm2, ymm3, ymm2, 136           # ymm2 = ymm3[0,2],ymm2[0,2],ymm3[4,6],ymm2[4,6]
	vperm2i128	ymm3, ymm10, ymm9, 49   # ymm3 = ymm10[2,3],ymm9[2,3]
	vinserti128	ymm13, ymm10, xmm9, 1
	vshufps	ymm3, ymm13, ymm3, 136          # ymm3 = ymm13[0,2],ymm3[0,2],ymm13[4,6],ymm3[4,6]
	vperm2i128	ymm13, ymm15, ymm5, 49  # ymm13 = ymm15[2,3],ymm5[2,3]
	vinserti128	ymm14, ymm15, xmm5, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vpcmpgtd	ymm13, ymm0, ymm13
	vpcmpgtd	ymm3, ymm0, ymm3
	vpackssdw	ymm3, ymm13, ymm3
	vpcmpgtd	ymm2, ymm0, ymm2
	vpcmpgtd	ymm1, ymm0, ymm1
	vpackssdw	ymm1, ymm2, ymm1
	vpermq	ymm2, ymm3, 216                 # ymm2 = ymm3[0,2,1,3]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vpacksswb	ymm1, ymm2, ymm1
	vpand	ymm6, ymm1, ymm6
	vmovd	ecx, xmm6
                                        # implicit-def: $ymm14
	test	cl, 1
	je	.LBB0_297
# %bb.774:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm14, byte ptr [rdi + rdx]
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_775
.LBB0_298:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_300
.LBB0_299:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 2
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_300:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 96]       # 8-byte Reload
	mov	r10, qword ptr [rsp + 72]       # 8-byte Reload
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_301
# %bb.776:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 3
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_777
.LBB0_302:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_303
.LBB0_778:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 5
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_779
.LBB0_304:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_305
.LBB0_780:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r9], 7
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_781
.LBB0_306:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_308
.LBB0_307:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r15], 9
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_308:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 144]      # 8-byte Reload
	mov	rsi, qword ptr [rsp + 136]      # 8-byte Reload
	mov	rbx, qword ptr [rsp + 128]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 120]       # 8-byte Reload
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_309
# %bb.782:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 10
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_783
.LBB0_310:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_311
.LBB0_784:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 12
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_312
	jmp	.LBB0_313
	.p2align	4, 0x90
.LBB0_297:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_298
.LBB0_775:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 1
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_299
	jmp	.LBB0_300
	.p2align	4, 0x90
.LBB0_301:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_302
.LBB0_777:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 4
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_778
.LBB0_303:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_304
.LBB0_779:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 6
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_780
.LBB0_305:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_306
.LBB0_781:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 8
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_307
	jmp	.LBB0_308
	.p2align	4, 0x90
.LBB0_309:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_310
.LBB0_783:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 11
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_784
.LBB0_311:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_313
.LBB0_312:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 13
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_313:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	mov	rdx, qword ptr [rsp + 64]       # 8-byte Reload
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_315
# %bb.314:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 14
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_315:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_317
# %bb.316:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r10], 15
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_317:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm6, 1
	vmovd	eax, xmm1
	mov	dword ptr [rsp + 44], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_319
# %bb.318:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rdx], 0
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_319:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 56]       # 8-byte Reload
	vpextrb	ecx, xmm1, 1
	mov	dword ptr [rsp + 40], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_320
# %bb.785:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rsi], 1
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_786
.LBB0_321:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_322
.LBB0_787:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r9], 3
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_788
.LBB0_323:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_325
.LBB0_324:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r13], 5
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_325:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 112]      # 8-byte Reload
	vpextrb	ecx, xmm1, 6
	mov	dword ptr [rsp + 20], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_326
# %bb.789:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 6
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	jne	.LBB0_790
.LBB0_327:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	je	.LBB0_328
.LBB0_791:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 8
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	jne	.LBB0_792
.LBB0_329:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	je	.LBB0_330
.LBB0_793:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 10
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 11
	test	al, 1
	jne	.LBB0_794
.LBB0_331:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	je	.LBB0_332
.LBB0_795:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 184]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 12
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	jne	.LBB0_796
.LBB0_333:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	je	.LBB0_334
.LBB0_797:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 168]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 14
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	jne	.LBB0_335
	jmp	.LBB0_336
	.p2align	4, 0x90
.LBB0_320:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_321
.LBB0_786:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 2
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_787
.LBB0_322:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_323
.LBB0_788:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 4
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_324
	jmp	.LBB0_325
	.p2align	4, 0x90
.LBB0_326:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	je	.LBB0_327
.LBB0_790:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 7
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	jne	.LBB0_791
.LBB0_328:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_329
.LBB0_792:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 208]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 9
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	jne	.LBB0_793
.LBB0_330:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 11
	test	al, 1
	je	.LBB0_331
.LBB0_794:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 192]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 11
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	jne	.LBB0_795
.LBB0_332:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	je	.LBB0_333
.LBB0_796:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 176]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 13
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	jne	.LBB0_797
.LBB0_334:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	je	.LBB0_336
.LBB0_335:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rbx, qword ptr [rsp + 160]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 15
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_336:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm14, 3
	vpand	ymm14, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r15d, xmm6
	test	r15b, 1
	je	.LBB0_337
# %bb.798:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm15
	vpextrb	byte ptr [r8 + rbx], xmm14, 0
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	jne	.LBB0_799
.LBB0_338:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	je	.LBB0_339
.LBB0_800:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 2
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	jne	.LBB0_801
.LBB0_340:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	je	.LBB0_341
.LBB0_802:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm5
	vpextrb	byte ptr [r8 + rbx], xmm14, 4
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	jne	.LBB0_803
.LBB0_342:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	je	.LBB0_343
.LBB0_804:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 6
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	jne	.LBB0_805
.LBB0_344:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	je	.LBB0_345
.LBB0_806:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm12
	vpextrb	byte ptr [r8 + rbx], xmm14, 8
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	jne	.LBB0_807
.LBB0_346:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	je	.LBB0_347
.LBB0_808:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 10
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	jne	.LBB0_809
.LBB0_348:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	je	.LBB0_349
.LBB0_810:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm11
	vpextrb	byte ptr [r8 + rbx], xmm14, 12
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	jne	.LBB0_811
.LBB0_350:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	je	.LBB0_351
.LBB0_812:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 14
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	jne	.LBB0_813
.LBB0_352:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	je	.LBB0_353
.LBB0_814:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm10
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 0
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	jne	.LBB0_815
.LBB0_354:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	je	.LBB0_355
.LBB0_816:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 2
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	jne	.LBB0_817
.LBB0_356:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	je	.LBB0_357
.LBB0_818:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm9
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 4
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	jne	.LBB0_819
.LBB0_358:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	je	.LBB0_359
.LBB0_820:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 6
	test	r9b, 1
	jne	.LBB0_821
.LBB0_360:                              #   in Loop: Header=BB0_26 Depth=1
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	je	.LBB0_361
.LBB0_822:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rdx, xmm8
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rdx], xmm1, 8
	test	cl, 1
	jne	.LBB0_823
.LBB0_362:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_363
.LBB0_824:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	jne	.LBB0_825
.LBB0_364:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	je	.LBB0_365
.LBB0_826:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm7
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	jne	.LBB0_827
.LBB0_366:                              #   in Loop: Header=BB0_26 Depth=1
	test	r11b, 1
	je	.LBB0_367
.LBB0_828:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	jne	.LBB0_368
	jmp	.LBB0_369
	.p2align	4, 0x90
.LBB0_337:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	je	.LBB0_338
.LBB0_799:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm15, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	jne	.LBB0_800
.LBB0_339:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	je	.LBB0_340
.LBB0_801:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 3
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	jne	.LBB0_802
.LBB0_341:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	je	.LBB0_342
.LBB0_803:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm5, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 5
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	jne	.LBB0_804
.LBB0_343:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	je	.LBB0_344
.LBB0_805:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 7
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	jne	.LBB0_806
.LBB0_345:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	je	.LBB0_346
.LBB0_807:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm12, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 9
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	jne	.LBB0_808
.LBB0_347:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	je	.LBB0_348
.LBB0_809:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 11
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	jne	.LBB0_810
.LBB0_349:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	je	.LBB0_350
.LBB0_811:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm11, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 13
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	jne	.LBB0_812
.LBB0_351:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	je	.LBB0_352
.LBB0_813:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 15
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	jne	.LBB0_814
.LBB0_353:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	je	.LBB0_354
.LBB0_815:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm10, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	jne	.LBB0_816
.LBB0_355:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	je	.LBB0_356
.LBB0_817:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 3
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	jne	.LBB0_818
.LBB0_357:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	je	.LBB0_358
.LBB0_819:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm9, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 5
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	jne	.LBB0_820
.LBB0_359:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	je	.LBB0_360
.LBB0_821:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 7
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	jne	.LBB0_822
.LBB0_361:                              #   in Loop: Header=BB0_26 Depth=1
	test	cl, 1
	je	.LBB0_362
.LBB0_823:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm8, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_824
.LBB0_363:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	je	.LBB0_364
.LBB0_825:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	r13b, 1
	jne	.LBB0_826
.LBB0_365:                              #   in Loop: Header=BB0_26 Depth=1
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	je	.LBB0_366
.LBB0_827:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm7, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	r11b, 1
	jne	.LBB0_828
.LBB0_367:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	je	.LBB0_369
.LBB0_368:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_369:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 672]   # 32-byte Reload
	vpor	ymm15, ymm1, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm5, ymm1, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm10, ymm1, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm9, ymm1, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm12, ymm1, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm11, ymm1, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm8, ymm1, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm7, ymm4, ymm1
	vperm2i128	ymm1, ymm8, ymm7, 49    # ymm1 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm2, ymm8, xmm7, 1
	vshufps	ymm1, ymm2, ymm1, 136           # ymm1 = ymm2[0,2],ymm1[0,2],ymm2[4,6],ymm1[4,6]
	vperm2i128	ymm2, ymm12, ymm11, 49  # ymm2 = ymm12[2,3],ymm11[2,3]
	vinserti128	ymm3, ymm12, xmm11, 1
	vshufps	ymm2, ymm3, ymm2, 136           # ymm2 = ymm3[0,2],ymm2[0,2],ymm3[4,6],ymm2[4,6]
	vperm2i128	ymm3, ymm10, ymm9, 49   # ymm3 = ymm10[2,3],ymm9[2,3]
	vinserti128	ymm13, ymm10, xmm9, 1
	vshufps	ymm3, ymm13, ymm3, 136          # ymm3 = ymm13[0,2],ymm3[0,2],ymm13[4,6],ymm3[4,6]
	vperm2i128	ymm13, ymm15, ymm5, 49  # ymm13 = ymm15[2,3],ymm5[2,3]
	vinserti128	ymm14, ymm15, xmm5, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vpcmpgtd	ymm13, ymm0, ymm13
	vpcmpgtd	ymm3, ymm0, ymm3
	vpackssdw	ymm3, ymm13, ymm3
	vpcmpgtd	ymm2, ymm0, ymm2
	vpcmpgtd	ymm1, ymm0, ymm1
	vpackssdw	ymm1, ymm2, ymm1
	vpermq	ymm2, ymm3, 216                 # ymm2 = ymm3[0,2,1,3]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vpacksswb	ymm1, ymm2, ymm1
	vpand	ymm6, ymm1, ymm6
	vmovd	ecx, xmm6
                                        # implicit-def: $ymm14
	test	cl, 1
	je	.LBB0_370
# %bb.829:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm14, byte ptr [rdi + rdx]
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_830
.LBB0_371:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_373
.LBB0_372:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 2
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_373:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 96]       # 8-byte Reload
	mov	r10, qword ptr [rsp + 72]       # 8-byte Reload
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_374
# %bb.831:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 3
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_832
.LBB0_375:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_376
.LBB0_833:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 5
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_834
.LBB0_377:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_378
.LBB0_835:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r9], 7
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_836
.LBB0_379:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_381
.LBB0_380:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r15], 9
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_381:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 144]      # 8-byte Reload
	mov	rsi, qword ptr [rsp + 136]      # 8-byte Reload
	mov	rbx, qword ptr [rsp + 128]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 120]       # 8-byte Reload
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_382
# %bb.837:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 10
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_838
.LBB0_383:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_384
.LBB0_839:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 12
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_385
	jmp	.LBB0_386
	.p2align	4, 0x90
.LBB0_370:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_371
.LBB0_830:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 1
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_372
	jmp	.LBB0_373
	.p2align	4, 0x90
.LBB0_374:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_375
.LBB0_832:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 4
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_833
.LBB0_376:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_377
.LBB0_834:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 6
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_835
.LBB0_378:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_379
.LBB0_836:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 8
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_380
	jmp	.LBB0_381
	.p2align	4, 0x90
.LBB0_382:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_383
.LBB0_838:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 11
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_839
.LBB0_384:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_386
.LBB0_385:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 13
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_386:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	mov	rdx, qword ptr [rsp + 64]       # 8-byte Reload
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_388
# %bb.387:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 14
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_388:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_390
# %bb.389:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r10], 15
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_390:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm6, 1
	vmovd	eax, xmm1
	mov	dword ptr [rsp + 44], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_392
# %bb.391:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rdx], 0
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_392:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 56]       # 8-byte Reload
	vpextrb	ecx, xmm1, 1
	mov	dword ptr [rsp + 40], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_393
# %bb.840:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rsi], 1
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_841
.LBB0_394:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_395
.LBB0_842:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r9], 3
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_843
.LBB0_396:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_398
.LBB0_397:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r13], 5
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_398:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 112]      # 8-byte Reload
	vpextrb	ecx, xmm1, 6
	mov	dword ptr [rsp + 20], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_399
# %bb.844:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 6
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	jne	.LBB0_845
.LBB0_400:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	je	.LBB0_401
.LBB0_846:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 8
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	jne	.LBB0_847
.LBB0_402:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	je	.LBB0_403
.LBB0_848:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 10
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 11
	test	al, 1
	jne	.LBB0_849
.LBB0_404:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	je	.LBB0_405
.LBB0_850:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 184]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 12
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	jne	.LBB0_851
.LBB0_406:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	je	.LBB0_407
.LBB0_852:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 168]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 14
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	jne	.LBB0_408
	jmp	.LBB0_409
	.p2align	4, 0x90
.LBB0_393:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_394
.LBB0_841:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 2
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_842
.LBB0_395:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_396
.LBB0_843:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 4
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_397
	jmp	.LBB0_398
	.p2align	4, 0x90
.LBB0_399:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	je	.LBB0_400
.LBB0_845:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 7
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	jne	.LBB0_846
.LBB0_401:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_402
.LBB0_847:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 208]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 9
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	jne	.LBB0_848
.LBB0_403:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 11
	test	al, 1
	je	.LBB0_404
.LBB0_849:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 192]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 11
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	jne	.LBB0_850
.LBB0_405:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	je	.LBB0_406
.LBB0_851:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 176]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 13
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	jne	.LBB0_852
.LBB0_407:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	je	.LBB0_409
.LBB0_408:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rbx, qword ptr [rsp + 160]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 15
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_409:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm14, 4
	vpand	ymm14, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r15d, xmm6
	test	r15b, 1
	je	.LBB0_410
# %bb.853:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm15
	vpextrb	byte ptr [r8 + rbx], xmm14, 0
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	jne	.LBB0_854
.LBB0_411:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	je	.LBB0_412
.LBB0_855:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 2
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	jne	.LBB0_856
.LBB0_413:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	je	.LBB0_414
.LBB0_857:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm5
	vpextrb	byte ptr [r8 + rbx], xmm14, 4
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	jne	.LBB0_858
.LBB0_415:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	je	.LBB0_416
.LBB0_859:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 6
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	jne	.LBB0_860
.LBB0_417:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	je	.LBB0_418
.LBB0_861:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm12
	vpextrb	byte ptr [r8 + rbx], xmm14, 8
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	jne	.LBB0_862
.LBB0_419:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	je	.LBB0_420
.LBB0_863:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 10
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	jne	.LBB0_864
.LBB0_421:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	je	.LBB0_422
.LBB0_865:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm11
	vpextrb	byte ptr [r8 + rbx], xmm14, 12
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	jne	.LBB0_866
.LBB0_423:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	je	.LBB0_424
.LBB0_867:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 14
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	jne	.LBB0_868
.LBB0_425:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	je	.LBB0_426
.LBB0_869:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm10
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 0
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	jne	.LBB0_870
.LBB0_427:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	je	.LBB0_428
.LBB0_871:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 2
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	jne	.LBB0_872
.LBB0_429:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	je	.LBB0_430
.LBB0_873:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm9
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 4
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	jne	.LBB0_874
.LBB0_431:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	je	.LBB0_432
.LBB0_875:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 6
	test	r9b, 1
	jne	.LBB0_876
.LBB0_433:                              #   in Loop: Header=BB0_26 Depth=1
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	je	.LBB0_434
.LBB0_877:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rdx, xmm8
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rdx], xmm1, 8
	test	cl, 1
	jne	.LBB0_878
.LBB0_435:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_436
.LBB0_879:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	jne	.LBB0_880
.LBB0_437:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	je	.LBB0_438
.LBB0_881:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm7
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	jne	.LBB0_882
.LBB0_439:                              #   in Loop: Header=BB0_26 Depth=1
	test	r11b, 1
	je	.LBB0_440
.LBB0_883:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	jne	.LBB0_441
	jmp	.LBB0_442
	.p2align	4, 0x90
.LBB0_410:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	je	.LBB0_411
.LBB0_854:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm15, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	jne	.LBB0_855
.LBB0_412:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	je	.LBB0_413
.LBB0_856:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 3
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	jne	.LBB0_857
.LBB0_414:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	je	.LBB0_415
.LBB0_858:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm5, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 5
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	jne	.LBB0_859
.LBB0_416:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	je	.LBB0_417
.LBB0_860:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 7
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	jne	.LBB0_861
.LBB0_418:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	je	.LBB0_419
.LBB0_862:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm12, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 9
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	jne	.LBB0_863
.LBB0_420:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	je	.LBB0_421
.LBB0_864:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 11
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	jne	.LBB0_865
.LBB0_422:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	je	.LBB0_423
.LBB0_866:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm11, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 13
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	jne	.LBB0_867
.LBB0_424:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	je	.LBB0_425
.LBB0_868:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 15
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	jne	.LBB0_869
.LBB0_426:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	je	.LBB0_427
.LBB0_870:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm10, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	jne	.LBB0_871
.LBB0_428:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	je	.LBB0_429
.LBB0_872:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 3
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	jne	.LBB0_873
.LBB0_430:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	je	.LBB0_431
.LBB0_874:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm9, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 5
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	jne	.LBB0_875
.LBB0_432:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	je	.LBB0_433
.LBB0_876:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 7
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	jne	.LBB0_877
.LBB0_434:                              #   in Loop: Header=BB0_26 Depth=1
	test	cl, 1
	je	.LBB0_435
.LBB0_878:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm8, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_879
.LBB0_436:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	je	.LBB0_437
.LBB0_880:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	r13b, 1
	jne	.LBB0_881
.LBB0_438:                              #   in Loop: Header=BB0_26 Depth=1
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	je	.LBB0_439
.LBB0_882:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm7, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	r11b, 1
	jne	.LBB0_883
.LBB0_440:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	je	.LBB0_442
.LBB0_441:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_442:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 640]   # 32-byte Reload
	vpor	ymm15, ymm1, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm5, ymm1, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm10, ymm1, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm9, ymm1, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm12, ymm1, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm11, ymm1, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm8, ymm1, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm7, ymm4, ymm1
	vperm2i128	ymm1, ymm8, ymm7, 49    # ymm1 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm2, ymm8, xmm7, 1
	vshufps	ymm1, ymm2, ymm1, 136           # ymm1 = ymm2[0,2],ymm1[0,2],ymm2[4,6],ymm1[4,6]
	vperm2i128	ymm2, ymm12, ymm11, 49  # ymm2 = ymm12[2,3],ymm11[2,3]
	vinserti128	ymm3, ymm12, xmm11, 1
	vshufps	ymm2, ymm3, ymm2, 136           # ymm2 = ymm3[0,2],ymm2[0,2],ymm3[4,6],ymm2[4,6]
	vperm2i128	ymm3, ymm10, ymm9, 49   # ymm3 = ymm10[2,3],ymm9[2,3]
	vinserti128	ymm13, ymm10, xmm9, 1
	vshufps	ymm3, ymm13, ymm3, 136          # ymm3 = ymm13[0,2],ymm3[0,2],ymm13[4,6],ymm3[4,6]
	vperm2i128	ymm13, ymm15, ymm5, 49  # ymm13 = ymm15[2,3],ymm5[2,3]
	vinserti128	ymm14, ymm15, xmm5, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vpcmpgtd	ymm13, ymm0, ymm13
	vpcmpgtd	ymm3, ymm0, ymm3
	vpackssdw	ymm3, ymm13, ymm3
	vpcmpgtd	ymm2, ymm0, ymm2
	vpcmpgtd	ymm1, ymm0, ymm1
	vpackssdw	ymm1, ymm2, ymm1
	vpermq	ymm2, ymm3, 216                 # ymm2 = ymm3[0,2,1,3]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vpacksswb	ymm1, ymm2, ymm1
	vpand	ymm6, ymm1, ymm6
	vmovd	ecx, xmm6
                                        # implicit-def: $ymm14
	test	cl, 1
	je	.LBB0_443
# %bb.884:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm14, byte ptr [rdi + rdx]
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_885
.LBB0_444:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_446
.LBB0_445:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 2
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_446:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 96]       # 8-byte Reload
	mov	r10, qword ptr [rsp + 72]       # 8-byte Reload
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_447
# %bb.886:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 3
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_887
.LBB0_448:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_449
.LBB0_888:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 5
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_889
.LBB0_450:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_451
.LBB0_890:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r9], 7
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_891
.LBB0_452:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_454
.LBB0_453:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r15], 9
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_454:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 144]      # 8-byte Reload
	mov	rsi, qword ptr [rsp + 136]      # 8-byte Reload
	mov	rbx, qword ptr [rsp + 128]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 120]       # 8-byte Reload
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_455
# %bb.892:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 10
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_893
.LBB0_456:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_457
.LBB0_894:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 12
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_458
	jmp	.LBB0_459
	.p2align	4, 0x90
.LBB0_443:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_444
.LBB0_885:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 1
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_445
	jmp	.LBB0_446
	.p2align	4, 0x90
.LBB0_447:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_448
.LBB0_887:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 4
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_888
.LBB0_449:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_450
.LBB0_889:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 6
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_890
.LBB0_451:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_452
.LBB0_891:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 8
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_453
	jmp	.LBB0_454
	.p2align	4, 0x90
.LBB0_455:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_456
.LBB0_893:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 11
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_894
.LBB0_457:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_459
.LBB0_458:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 13
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_459:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	mov	rdx, qword ptr [rsp + 64]       # 8-byte Reload
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_461
# %bb.460:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 14
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_461:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_463
# %bb.462:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r10], 15
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_463:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm6, 1
	vmovd	eax, xmm1
	mov	dword ptr [rsp + 44], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_465
# %bb.464:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rdx], 0
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_465:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 56]       # 8-byte Reload
	vpextrb	ecx, xmm1, 1
	mov	dword ptr [rsp + 40], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_466
# %bb.895:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rsi], 1
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_896
.LBB0_467:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_468
.LBB0_897:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r9], 3
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_898
.LBB0_469:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_471
.LBB0_470:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r13], 5
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_471:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 112]      # 8-byte Reload
	vpextrb	ecx, xmm1, 6
	mov	dword ptr [rsp + 20], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_472
# %bb.899:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 6
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	jne	.LBB0_900
.LBB0_473:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	je	.LBB0_474
.LBB0_901:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 8
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	jne	.LBB0_902
.LBB0_475:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	je	.LBB0_476
.LBB0_903:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 10
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 11
	test	al, 1
	jne	.LBB0_904
.LBB0_477:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	je	.LBB0_478
.LBB0_905:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 184]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 12
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	jne	.LBB0_906
.LBB0_479:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	je	.LBB0_480
.LBB0_907:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 168]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 14
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	jne	.LBB0_481
	jmp	.LBB0_482
	.p2align	4, 0x90
.LBB0_466:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_467
.LBB0_896:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 2
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_897
.LBB0_468:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_469
.LBB0_898:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 4
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_470
	jmp	.LBB0_471
	.p2align	4, 0x90
.LBB0_472:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	je	.LBB0_473
.LBB0_900:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 7
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	jne	.LBB0_901
.LBB0_474:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_475
.LBB0_902:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 208]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 9
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	jne	.LBB0_903
.LBB0_476:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 11
	test	al, 1
	je	.LBB0_477
.LBB0_904:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 192]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 11
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	jne	.LBB0_905
.LBB0_478:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	je	.LBB0_479
.LBB0_906:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 176]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 13
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	jne	.LBB0_907
.LBB0_480:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	je	.LBB0_482
.LBB0_481:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rbx, qword ptr [rsp + 160]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 15
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_482:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm14, 5
	vpand	ymm14, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r15d, xmm6
	test	r15b, 1
	je	.LBB0_483
# %bb.908:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm15
	vpextrb	byte ptr [r8 + rbx], xmm14, 0
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	jne	.LBB0_909
.LBB0_484:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	je	.LBB0_485
.LBB0_910:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 2
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	jne	.LBB0_911
.LBB0_486:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	je	.LBB0_487
.LBB0_912:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm5
	vpextrb	byte ptr [r8 + rbx], xmm14, 4
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	jne	.LBB0_913
.LBB0_488:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	je	.LBB0_489
.LBB0_914:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 6
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	jne	.LBB0_915
.LBB0_490:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	je	.LBB0_491
.LBB0_916:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm12
	vpextrb	byte ptr [r8 + rbx], xmm14, 8
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	jne	.LBB0_917
.LBB0_492:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	je	.LBB0_493
.LBB0_918:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 10
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	jne	.LBB0_919
.LBB0_494:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	je	.LBB0_495
.LBB0_920:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm11
	vpextrb	byte ptr [r8 + rbx], xmm14, 12
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	jne	.LBB0_921
.LBB0_496:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	je	.LBB0_497
.LBB0_922:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 14
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	jne	.LBB0_923
.LBB0_498:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	je	.LBB0_499
.LBB0_924:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm10
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 0
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	jne	.LBB0_925
.LBB0_500:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	je	.LBB0_501
.LBB0_926:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 2
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	jne	.LBB0_927
.LBB0_502:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	je	.LBB0_503
.LBB0_928:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm9
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 4
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	jne	.LBB0_929
.LBB0_504:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	je	.LBB0_505
.LBB0_930:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 6
	test	r9b, 1
	jne	.LBB0_931
.LBB0_506:                              #   in Loop: Header=BB0_26 Depth=1
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	je	.LBB0_507
.LBB0_932:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rdx, xmm8
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rdx], xmm1, 8
	test	cl, 1
	jne	.LBB0_933
.LBB0_508:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_509
.LBB0_934:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	jne	.LBB0_935
.LBB0_510:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	je	.LBB0_511
.LBB0_936:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm7
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	jne	.LBB0_937
.LBB0_512:                              #   in Loop: Header=BB0_26 Depth=1
	test	r11b, 1
	je	.LBB0_513
.LBB0_938:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	jne	.LBB0_514
	jmp	.LBB0_515
	.p2align	4, 0x90
.LBB0_483:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	je	.LBB0_484
.LBB0_909:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm15, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	jne	.LBB0_910
.LBB0_485:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	je	.LBB0_486
.LBB0_911:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 3
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	jne	.LBB0_912
.LBB0_487:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	je	.LBB0_488
.LBB0_913:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm5, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 5
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	jne	.LBB0_914
.LBB0_489:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	je	.LBB0_490
.LBB0_915:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 7
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	jne	.LBB0_916
.LBB0_491:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	je	.LBB0_492
.LBB0_917:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm12, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 9
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	jne	.LBB0_918
.LBB0_493:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	je	.LBB0_494
.LBB0_919:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 11
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	jne	.LBB0_920
.LBB0_495:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	je	.LBB0_496
.LBB0_921:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm11, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 13
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	jne	.LBB0_922
.LBB0_497:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	je	.LBB0_498
.LBB0_923:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 15
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	jne	.LBB0_924
.LBB0_499:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	je	.LBB0_500
.LBB0_925:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm10, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	jne	.LBB0_926
.LBB0_501:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	je	.LBB0_502
.LBB0_927:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 3
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	jne	.LBB0_928
.LBB0_503:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	je	.LBB0_504
.LBB0_929:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm9, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 5
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	jne	.LBB0_930
.LBB0_505:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	je	.LBB0_506
.LBB0_931:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 7
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	jne	.LBB0_932
.LBB0_507:                              #   in Loop: Header=BB0_26 Depth=1
	test	cl, 1
	je	.LBB0_508
.LBB0_933:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm8, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_934
.LBB0_509:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	je	.LBB0_510
.LBB0_935:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	r13b, 1
	jne	.LBB0_936
.LBB0_511:                              #   in Loop: Header=BB0_26 Depth=1
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	je	.LBB0_512
.LBB0_937:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm7, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	r11b, 1
	jne	.LBB0_938
.LBB0_513:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	je	.LBB0_515
.LBB0_514:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_515:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 608]   # 32-byte Reload
	vpor	ymm15, ymm1, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm5, ymm1, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm10, ymm1, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm9, ymm1, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm12, ymm1, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm11, ymm1, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm8, ymm1, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm7, ymm4, ymm1
	vperm2i128	ymm1, ymm8, ymm7, 49    # ymm1 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm2, ymm8, xmm7, 1
	vshufps	ymm1, ymm2, ymm1, 136           # ymm1 = ymm2[0,2],ymm1[0,2],ymm2[4,6],ymm1[4,6]
	vperm2i128	ymm2, ymm12, ymm11, 49  # ymm2 = ymm12[2,3],ymm11[2,3]
	vinserti128	ymm3, ymm12, xmm11, 1
	vshufps	ymm2, ymm3, ymm2, 136           # ymm2 = ymm3[0,2],ymm2[0,2],ymm3[4,6],ymm2[4,6]
	vperm2i128	ymm3, ymm10, ymm9, 49   # ymm3 = ymm10[2,3],ymm9[2,3]
	vinserti128	ymm13, ymm10, xmm9, 1
	vshufps	ymm3, ymm13, ymm3, 136          # ymm3 = ymm13[0,2],ymm3[0,2],ymm13[4,6],ymm3[4,6]
	vperm2i128	ymm13, ymm15, ymm5, 49  # ymm13 = ymm15[2,3],ymm5[2,3]
	vinserti128	ymm14, ymm15, xmm5, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vpcmpgtd	ymm13, ymm0, ymm13
	vpcmpgtd	ymm3, ymm0, ymm3
	vpackssdw	ymm3, ymm13, ymm3
	vpcmpgtd	ymm2, ymm0, ymm2
	vpcmpgtd	ymm1, ymm0, ymm1
	vpackssdw	ymm1, ymm2, ymm1
	vpermq	ymm2, ymm3, 216                 # ymm2 = ymm3[0,2,1,3]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vpacksswb	ymm1, ymm2, ymm1
	vpand	ymm6, ymm1, ymm6
	vmovd	ecx, xmm6
                                        # implicit-def: $ymm14
	test	cl, 1
	je	.LBB0_516
# %bb.939:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm14, byte ptr [rdi + rdx]
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	jne	.LBB0_940
.LBB0_517:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	je	.LBB0_519
.LBB0_518:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rbx], 2
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_519:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 96]       # 8-byte Reload
	mov	r10, qword ptr [rsp + 72]       # 8-byte Reload
	vpextrb	ecx, xmm6, 3
	test	cl, 1
	je	.LBB0_520
# %bb.941:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 3
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	jne	.LBB0_942
.LBB0_521:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	je	.LBB0_522
.LBB0_943:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 5
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	jne	.LBB0_944
.LBB0_523:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	je	.LBB0_524
.LBB0_945:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r9], 7
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	jne	.LBB0_946
.LBB0_525:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_527
.LBB0_526:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r15], 9
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_527:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 144]      # 8-byte Reload
	mov	rsi, qword ptr [rsp + 136]      # 8-byte Reload
	mov	rbx, qword ptr [rsp + 128]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 120]       # 8-byte Reload
	vpextrb	ecx, xmm6, 10
	test	cl, 1
	je	.LBB0_528
# %bb.947:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 10
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	jne	.LBB0_948
.LBB0_529:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	je	.LBB0_530
.LBB0_949:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 12
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	jne	.LBB0_531
	jmp	.LBB0_532
	.p2align	4, 0x90
.LBB0_516:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 1
	test	cl, 1
	je	.LBB0_517
.LBB0_940:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 1
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm6, 2
	test	cl, 1
	jne	.LBB0_518
	jmp	.LBB0_519
	.p2align	4, 0x90
.LBB0_520:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	test	cl, 1
	je	.LBB0_521
.LBB0_942:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rcx], 4
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 5
	test	cl, 1
	jne	.LBB0_943
.LBB0_522:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 6
	test	cl, 1
	je	.LBB0_523
.LBB0_944:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 6
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 7
	test	cl, 1
	jne	.LBB0_945
.LBB0_524:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 8
	test	cl, 1
	je	.LBB0_525
.LBB0_946:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rsi], 8
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_526
	jmp	.LBB0_527
	.p2align	4, 0x90
.LBB0_528:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 11
	test	cl, 1
	je	.LBB0_529
.LBB0_948:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 11
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
	vpextrb	ecx, xmm6, 12
	test	cl, 1
	jne	.LBB0_949
.LBB0_530:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 13
	test	cl, 1
	je	.LBB0_532
.LBB0_531:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rdx], 13
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_532:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	mov	rdx, qword ptr [rsp + 64]       # 8-byte Reload
	vpextrb	ecx, xmm6, 14
	test	cl, 1
	je	.LBB0_534
# %bb.533:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + rax], 14
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_534:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 15
	test	cl, 1
	je	.LBB0_536
# %bb.535:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm1, xmm14, byte ptr [rdi + r10], 15
	vpblendd	ymm14, ymm14, ymm1, 15          # ymm14 = ymm1[0,1,2,3],ymm14[4,5,6,7]
.LBB0_536:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm6, 1
	vmovd	eax, xmm1
	mov	dword ptr [rsp + 44], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_538
# %bb.537:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rdx], 0
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_538:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 56]       # 8-byte Reload
	vpextrb	ecx, xmm1, 1
	mov	dword ptr [rsp + 40], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_539
# %bb.950:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rsi], 1
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_951
.LBB0_540:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_541
.LBB0_952:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r9], 3
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_953
.LBB0_542:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	je	.LBB0_544
.LBB0_543:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + r13], 5
	vinserti128	ymm14, ymm14, xmm2, 1
.LBB0_544:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 112]      # 8-byte Reload
	vpextrb	ecx, xmm1, 6
	mov	dword ptr [rsp + 20], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_545
# %bb.954:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 6
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	jne	.LBB0_955
.LBB0_546:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	je	.LBB0_547
.LBB0_956:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 8
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	jne	.LBB0_957
.LBB0_548:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	je	.LBB0_549
.LBB0_958:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 10
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 11
	test	al, 1
	jne	.LBB0_959
.LBB0_550:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	je	.LBB0_551
.LBB0_960:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 184]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 12
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	jne	.LBB0_961
.LBB0_552:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	je	.LBB0_553
.LBB0_962:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 168]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 14
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	jne	.LBB0_554
	jmp	.LBB0_555
	.p2align	4, 0x90
.LBB0_539:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 2
	mov	dword ptr [rsp + 36], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_540
.LBB0_951:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 2
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	ecx, xmm1, 3
	mov	dword ptr [rsp + 32], ecx       # 4-byte Spill
	test	cl, 1
	jne	.LBB0_952
.LBB0_541:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 4
	mov	dword ptr [rsp + 28], ecx       # 4-byte Spill
	test	cl, 1
	je	.LBB0_542
.LBB0_953:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 4
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	eax, xmm1, 5
	mov	dword ptr [rsp + 24], eax       # 4-byte Spill
	test	al, 1
	jne	.LBB0_543
	jmp	.LBB0_544
	.p2align	4, 0x90
.LBB0_545:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm1, 7
	test	r9b, 1
	je	.LBB0_546
.LBB0_955:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 7
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	edx, xmm1, 8
	test	dl, 1
	jne	.LBB0_956
.LBB0_547:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm1, 9
	test	cl, 1
	je	.LBB0_548
.LBB0_957:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rax, qword ptr [rsp + 208]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rax], 9
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	esi, xmm1, 10
	test	sil, 1
	jne	.LBB0_958
.LBB0_549:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm1, 11
	test	al, 1
	je	.LBB0_550
.LBB0_959:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 192]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 11
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r13d, xmm1, 12
	test	r13b, 1
	jne	.LBB0_960
.LBB0_551:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r10d, xmm1, 13
	test	r10b, 1
	je	.LBB0_552
.LBB0_961:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm2, ymm14, 1
	mov	rbx, qword ptr [rsp + 176]      # 8-byte Reload
	vpinsrb	xmm2, xmm2, byte ptr [rdi + rbx], 13
	vinserti128	ymm14, ymm14, xmm2, 1
	vpextrb	r11d, xmm1, 14
	test	r11b, 1
	jne	.LBB0_962
.LBB0_553:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r14d, xmm1, 15
	test	r14b, 1
	je	.LBB0_555
.LBB0_554:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm14, 1
	mov	rbx, qword ptr [rsp + 160]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 15
	vinserti128	ymm14, ymm14, xmm1, 1
.LBB0_555:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm14, 6
	vpand	ymm14, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r15d, xmm6
	test	r15b, 1
	je	.LBB0_556
# %bb.963:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm15
	vpextrb	byte ptr [r8 + rbx], xmm14, 0
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	jne	.LBB0_964
.LBB0_557:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	je	.LBB0_558
.LBB0_965:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 2
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	jne	.LBB0_966
.LBB0_559:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	je	.LBB0_560
.LBB0_967:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm5
	vpextrb	byte ptr [r8 + rbx], xmm14, 4
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	jne	.LBB0_968
.LBB0_561:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	je	.LBB0_562
.LBB0_969:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 6
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	jne	.LBB0_970
.LBB0_563:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	je	.LBB0_564
.LBB0_971:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm12
	vpextrb	byte ptr [r8 + rbx], xmm14, 8
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	jne	.LBB0_972
.LBB0_565:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	je	.LBB0_566
.LBB0_973:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 10
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	jne	.LBB0_974
.LBB0_567:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	je	.LBB0_568
.LBB0_975:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm11
	vpextrb	byte ptr [r8 + rbx], xmm14, 12
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	jne	.LBB0_976
.LBB0_569:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	je	.LBB0_570
.LBB0_977:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm14, 14
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	jne	.LBB0_978
.LBB0_571:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	je	.LBB0_572
.LBB0_979:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm10
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 0
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	jne	.LBB0_980
.LBB0_573:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	je	.LBB0_574
.LBB0_981:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 2
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	jne	.LBB0_982
.LBB0_575:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	je	.LBB0_576
.LBB0_983:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm9
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 4
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	jne	.LBB0_984
.LBB0_577:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	je	.LBB0_578
.LBB0_985:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 6
	test	r9b, 1
	jne	.LBB0_986
.LBB0_579:                              #   in Loop: Header=BB0_26 Depth=1
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	je	.LBB0_580
.LBB0_987:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rdx, xmm8
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rdx], xmm1, 8
	test	cl, 1
	jne	.LBB0_988
.LBB0_581:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_582
.LBB0_989:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	jne	.LBB0_990
.LBB0_583:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	je	.LBB0_584
.LBB0_991:                              #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm7
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	jne	.LBB0_992
.LBB0_585:                              #   in Loop: Header=BB0_26 Depth=1
	test	r11b, 1
	je	.LBB0_586
.LBB0_993:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	jne	.LBB0_587
	jmp	.LBB0_588
	.p2align	4, 0x90
.LBB0_556:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 1
	test	bl, 1
	je	.LBB0_557
.LBB0_964:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm15, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 1
	vpextrb	ebx, xmm6, 2
	test	bl, 1
	mov	r15, qword ptr [rsp + 224]      # 8-byte Reload
	jne	.LBB0_965
.LBB0_558:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 3
	test	bl, 1
	je	.LBB0_559
.LBB0_966:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 3
	vpextrb	ebx, xmm6, 4
	test	bl, 1
	jne	.LBB0_967
.LBB0_560:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 5
	test	bl, 1
	je	.LBB0_561
.LBB0_968:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm5, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 5
	vpextrb	ebx, xmm6, 6
	test	bl, 1
	jne	.LBB0_969
.LBB0_562:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 7
	test	bl, 1
	je	.LBB0_563
.LBB0_970:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 7
	vpextrb	ebx, xmm6, 8
	test	bl, 1
	jne	.LBB0_971
.LBB0_564:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 9
	test	bl, 1
	je	.LBB0_565
.LBB0_972:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm12, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 9
	vpextrb	ebx, xmm6, 10
	test	bl, 1
	jne	.LBB0_973
.LBB0_566:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 11
	test	bl, 1
	je	.LBB0_567
.LBB0_974:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm12, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 11
	vpextrb	ebx, xmm6, 12
	test	bl, 1
	jne	.LBB0_975
.LBB0_568:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 13
	test	bl, 1
	je	.LBB0_569
.LBB0_976:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm11, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 13
	vpextrb	ebx, xmm6, 14
	test	bl, 1
	jne	.LBB0_977
.LBB0_570:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm6, 15
	test	bl, 1
	je	.LBB0_571
.LBB0_978:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm14, 15
	test	byte ptr [rsp + 44], 1          # 1-byte Folded Reload
	jne	.LBB0_979
.LBB0_572:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 40], 1          # 1-byte Folded Reload
	je	.LBB0_573
.LBB0_980:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm10, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 1
	test	byte ptr [rsp + 36], 1          # 1-byte Folded Reload
	jne	.LBB0_981
.LBB0_574:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 32], 1          # 1-byte Folded Reload
	je	.LBB0_575
.LBB0_982:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 3
	test	byte ptr [rsp + 28], 1          # 1-byte Folded Reload
	jne	.LBB0_983
.LBB0_576:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 24], 1          # 1-byte Folded Reload
	je	.LBB0_577
.LBB0_984:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm9, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 5
	test	byte ptr [rsp + 20], 1          # 1-byte Folded Reload
	jne	.LBB0_985
.LBB0_578:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	je	.LBB0_579
.LBB0_986:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 7
	test	dl, 1
	mov	rbx, qword ptr [rsp + 296]      # 8-byte Reload
	jne	.LBB0_987
.LBB0_580:                              #   in Loop: Header=BB0_26 Depth=1
	test	cl, 1
	je	.LBB0_581
.LBB0_988:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm8, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	sil, 1
	mov	rdx, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_989
.LBB0_582:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	mov	rsi, qword ptr [rsp + 152]      # 8-byte Reload
	je	.LBB0_583
.LBB0_990:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	r13b, 1
	jne	.LBB0_991
.LBB0_584:                              #   in Loop: Header=BB0_26 Depth=1
	test	r10b, 1
	mov	r13, qword ptr [rsp + 280]      # 8-byte Reload
	je	.LBB0_585
.LBB0_992:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm7, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	r11b, 1
	jne	.LBB0_993
.LBB0_586:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	mov	rax, qword ptr [rsp + 288]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 232]       # 8-byte Reload
	je	.LBB0_588
.LBB0_587:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm14, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
.LBB0_588:                              #   in Loop: Header=BB0_26 Depth=1
	vmovdqa	ymm1, ymmword ptr [rsp + 576]   # 32-byte Reload
	vpor	ymm11, ymm1, ymmword ptr [rsp + 512] # 32-byte Folded Reload
	vpor	ymm10, ymm1, ymmword ptr [rsp + 480] # 32-byte Folded Reload
	vpor	ymm8, ymm1, ymmword ptr [rsp + 384] # 32-byte Folded Reload
	vpor	ymm7, ymm1, ymmword ptr [rsp + 352] # 32-byte Folded Reload
	vpor	ymm9, ymm1, ymmword ptr [rsp + 448] # 32-byte Folded Reload
	vpor	ymm5, ymm1, ymmword ptr [rsp + 416] # 32-byte Folded Reload
	vpor	ymm2, ymm1, ymmword ptr [rsp + 320] # 32-byte Folded Reload
	vpor	ymm15, ymm4, ymm1
	vperm2i128	ymm3, ymm2, ymm15, 49   # ymm3 = ymm2[2,3],ymm15[2,3]
	vinserti128	ymm4, ymm2, xmm15, 1
	vshufps	ymm3, ymm4, ymm3, 136           # ymm3 = ymm4[0,2],ymm3[0,2],ymm4[4,6],ymm3[4,6]
	vperm2i128	ymm4, ymm9, ymm5, 49    # ymm4 = ymm9[2,3],ymm5[2,3]
	vinserti128	ymm12, ymm9, xmm5, 1
	vshufps	ymm4, ymm12, ymm4, 136          # ymm4 = ymm12[0,2],ymm4[0,2],ymm12[4,6],ymm4[4,6]
	vperm2i128	ymm12, ymm8, ymm7, 49   # ymm12 = ymm8[2,3],ymm7[2,3]
	vinserti128	ymm13, ymm8, xmm7, 1
	vshufps	ymm12, ymm13, ymm12, 136        # ymm12 = ymm13[0,2],ymm12[0,2],ymm13[4,6],ymm12[4,6]
	vperm2i128	ymm13, ymm11, ymm10, 49 # ymm13 = ymm11[2,3],ymm10[2,3]
	vinserti128	ymm14, ymm11, xmm10, 1
	vshufps	ymm13, ymm14, ymm13, 136        # ymm13 = ymm14[0,2],ymm13[0,2],ymm14[4,6],ymm13[4,6]
	vpcmpgtd	ymm13, ymm0, ymm13
	vpcmpgtd	ymm12, ymm0, ymm12
	vpackssdw	ymm12, ymm13, ymm12
	vpermq	ymm12, ymm12, 216               # ymm12 = ymm12[0,2,1,3]
	vpcmpgtd	ymm4, ymm0, ymm4
	vpcmpgtd	ymm3, ymm0, ymm3
	vpackssdw	ymm3, ymm4, ymm3
	vpermq	ymm3, ymm3, 216                 # ymm3 = ymm3[0,2,1,3]
	vpacksswb	ymm3, ymm12, ymm3
	vpand	ymm3, ymm3, ymm6
	vmovd	ecx, xmm3
                                        # implicit-def: $ymm4
	test	cl, 1
	je	.LBB0_589
# %bb.994:                              #   in Loop: Header=BB0_26 Depth=1
	vpbroadcastb	ymm4, byte ptr [rdi + rdx]
	vpextrb	ecx, xmm3, 1
	test	cl, 1
	jne	.LBB0_995
.LBB0_590:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm3, 2
	test	cl, 1
	je	.LBB0_592
.LBB0_591:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rbx], 2
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
.LBB0_592:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rsi, qword ptr [rsp + 96]       # 8-byte Reload
	mov	r10, qword ptr [rsp + 72]       # 8-byte Reload
	vpextrb	ecx, xmm3, 3
	test	cl, 1
	je	.LBB0_593
# %bb.996:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 272]      # 8-byte Reload
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rcx], 3
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 4
	test	cl, 1
	jne	.LBB0_997
.LBB0_594:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 5
	test	cl, 1
	je	.LBB0_595
.LBB0_998:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rdx], 5
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 6
	test	cl, 1
	jne	.LBB0_999
.LBB0_596:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 7
	test	cl, 1
	je	.LBB0_597
.LBB0_1000:                             #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + r9], 7
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 8
	test	cl, 1
	jne	.LBB0_1001
.LBB0_598:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm3, 9
	test	cl, 1
	je	.LBB0_600
.LBB0_599:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + r15], 9
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
.LBB0_600:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 144]      # 8-byte Reload
	mov	rsi, qword ptr [rsp + 136]      # 8-byte Reload
	mov	rbx, qword ptr [rsp + 128]      # 8-byte Reload
	mov	r9, qword ptr [rsp + 120]       # 8-byte Reload
	vpextrb	ecx, xmm3, 10
	test	cl, 1
	je	.LBB0_601
# %bb.1002:                             #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rax], 10
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 11
	test	cl, 1
	jne	.LBB0_1003
.LBB0_602:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 12
	test	cl, 1
	je	.LBB0_603
.LBB0_1004:                             #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 248]      # 8-byte Reload
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rax], 12
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 13
	test	cl, 1
	jne	.LBB0_604
	jmp	.LBB0_605
	.p2align	4, 0x90
.LBB0_589:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 1
	test	cl, 1
	je	.LBB0_590
.LBB0_995:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rsi], 1
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	mov	rdx, qword ptr [rsp + 104]      # 8-byte Reload
	vpextrb	ecx, xmm3, 2
	test	cl, 1
	jne	.LBB0_591
	jmp	.LBB0_592
	.p2align	4, 0x90
.LBB0_593:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 4
	test	cl, 1
	je	.LBB0_594
.LBB0_997:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rcx, qword ptr [rsp + 264]      # 8-byte Reload
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rcx], 4
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 5
	test	cl, 1
	jne	.LBB0_998
.LBB0_595:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 6
	test	cl, 1
	je	.LBB0_596
.LBB0_999:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rax], 6
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 7
	test	cl, 1
	jne	.LBB0_1000
.LBB0_597:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 8
	test	cl, 1
	je	.LBB0_598
.LBB0_1001:                             #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rsi], 8
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	mov	rdx, qword ptr [rsp + 88]       # 8-byte Reload
	vpextrb	ecx, xmm3, 9
	test	cl, 1
	jne	.LBB0_599
	jmp	.LBB0_600
	.p2align	4, 0x90
.LBB0_601:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 11
	test	cl, 1
	je	.LBB0_602
.LBB0_1003:                             #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 256]      # 8-byte Reload
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rax], 11
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
	vpextrb	ecx, xmm3, 12
	test	cl, 1
	jne	.LBB0_1004
.LBB0_603:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 13
	test	cl, 1
	je	.LBB0_605
.LBB0_604:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rdx], 13
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
.LBB0_605:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 80]       # 8-byte Reload
	mov	rdx, qword ptr [rsp + 64]       # 8-byte Reload
	vpextrb	ecx, xmm3, 14
	test	cl, 1
	je	.LBB0_607
# %bb.606:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + rax], 14
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
.LBB0_607:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm3, 15
	test	cl, 1
	je	.LBB0_609
# %bb.608:                              #   in Loop: Header=BB0_26 Depth=1
	vpinsrb	xmm6, xmm4, byte ptr [rdi + r10], 15
	vpblendd	ymm4, ymm4, ymm6, 15            # ymm4 = ymm6[0,1,2,3],ymm4[4,5,6,7]
.LBB0_609:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm6, ymm3, 1
	vmovd	eax, xmm6
	mov	dword ptr [rsp + 512], eax      # 4-byte Spill
	test	al, 1
	je	.LBB0_611
# %bb.610:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rdx], 0
	vinserti128	ymm4, ymm4, xmm1, 1
.LBB0_611:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 56]       # 8-byte Reload
	vpextrb	ecx, xmm6, 1
	mov	dword ptr [rsp + 480], ecx      # 4-byte Spill
	test	cl, 1
	je	.LBB0_612
# %bb.1005:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rsi], 1
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	ecx, xmm6, 2
	mov	dword ptr [rsp + 448], ecx      # 4-byte Spill
	test	cl, 1
	jne	.LBB0_1006
.LBB0_613:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 3
	mov	dword ptr [rsp + 416], ecx      # 4-byte Spill
	test	cl, 1
	je	.LBB0_614
.LBB0_1007:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + r9], 3
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	ecx, xmm6, 4
	mov	dword ptr [rsp + 384], ecx      # 4-byte Spill
	test	cl, 1
	jne	.LBB0_1008
.LBB0_615:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm6, 5
	mov	dword ptr [rsp + 352], eax      # 4-byte Spill
	test	al, 1
	je	.LBB0_617
.LBB0_616:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + r13], 5
	vinserti128	ymm4, ymm4, xmm1, 1
.LBB0_617:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rax, qword ptr [rsp + 112]      # 8-byte Reload
	mov	rbx, qword ptr [rsp + 184]      # 8-byte Reload
	mov	rdx, qword ptr [rsp + 176]      # 8-byte Reload
	vpextrb	ecx, xmm6, 6
	mov	dword ptr [rsp + 320], ecx      # 4-byte Spill
	test	cl, 1
	je	.LBB0_618
# %bb.1009:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 6
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	eax, xmm6, 7
	mov	dword ptr [rsp + 152], eax      # 4-byte Spill
	test	al, 1
	jne	.LBB0_1010
.LBB0_619:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r9d, xmm6, 8
	test	r9b, 1
	je	.LBB0_620
.LBB0_1011:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	mov	rax, qword ptr [rsp + 216]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 8
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	jne	.LBB0_1012
.LBB0_621:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r11d, xmm6, 10
	test	r11b, 1
	je	.LBB0_622
.LBB0_1013:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	mov	rax, qword ptr [rsp + 200]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 10
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	eax, xmm6, 11
	test	al, 1
	jne	.LBB0_1014
.LBB0_623:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	esi, xmm6, 12
	test	sil, 1
	je	.LBB0_624
.LBB0_1015:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 12
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	r10d, xmm6, 13
	test	r10b, 1
	jne	.LBB0_1016
.LBB0_625:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 168]      # 8-byte Reload
	vpextrb	r13d, xmm6, 14
	test	r13b, 1
	je	.LBB0_626
.LBB0_1017:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rdx], 14
	vinserti128	ymm4, ymm4, xmm1, 1
	mov	rdx, qword ptr [rsp + 160]      # 8-byte Reload
	vpextrb	r14d, xmm6, 15
	test	r14b, 1
	jne	.LBB0_627
	jmp	.LBB0_628
	.p2align	4, 0x90
.LBB0_612:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 2
	mov	dword ptr [rsp + 448], ecx      # 4-byte Spill
	test	cl, 1
	je	.LBB0_613
.LBB0_1006:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rbx], 2
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	ecx, xmm6, 3
	mov	dword ptr [rsp + 416], ecx      # 4-byte Spill
	test	cl, 1
	jne	.LBB0_1007
.LBB0_614:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 4
	mov	dword ptr [rsp + 384], ecx      # 4-byte Spill
	test	cl, 1
	je	.LBB0_615
.LBB0_1008:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 4
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	eax, xmm6, 5
	mov	dword ptr [rsp + 352], eax      # 4-byte Spill
	test	al, 1
	jne	.LBB0_616
	jmp	.LBB0_617
	.p2align	4, 0x90
.LBB0_618:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm6, 7
	mov	dword ptr [rsp + 152], eax      # 4-byte Spill
	test	al, 1
	je	.LBB0_619
.LBB0_1010:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	mov	rax, qword ptr [rsp + 240]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 7
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	r9d, xmm6, 8
	test	r9b, 1
	jne	.LBB0_1011
.LBB0_620:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ecx, xmm6, 9
	test	cl, 1
	je	.LBB0_621
.LBB0_1012:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	mov	rax, qword ptr [rsp + 208]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rax], 9
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	r11d, xmm6, 10
	test	r11b, 1
	jne	.LBB0_1013
.LBB0_622:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	eax, xmm6, 11
	test	al, 1
	je	.LBB0_623
.LBB0_1014:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	mov	rsi, qword ptr [rsp + 192]      # 8-byte Reload
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rsi], 11
	vinserti128	ymm4, ymm4, xmm1, 1
	vpextrb	esi, xmm6, 12
	test	sil, 1
	jne	.LBB0_1015
.LBB0_624:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	r10d, xmm6, 13
	test	r10b, 1
	je	.LBB0_625
.LBB0_1016:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rdx], 13
	vinserti128	ymm4, ymm4, xmm1, 1
	mov	rdx, qword ptr [rsp + 168]      # 8-byte Reload
	vpextrb	r13d, xmm6, 14
	test	r13b, 1
	jne	.LBB0_1017
.LBB0_626:                              #   in Loop: Header=BB0_26 Depth=1
	mov	rdx, qword ptr [rsp + 160]      # 8-byte Reload
	vpextrb	r14d, xmm6, 15
	test	r14b, 1
	je	.LBB0_628
.LBB0_627:                              #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm4, 1
	vpinsrb	xmm1, xmm1, byte ptr [rdi + rdx], 15
	vinserti128	ymm4, ymm4, xmm1, 1
.LBB0_628:                              #   in Loop: Header=BB0_26 Depth=1
	vpsrlw	ymm1, ymm4, 7
	vpand	ymm4, ymm1, ymmword ptr [rip + .LCPI0_4]
	vmovd	r15d, xmm3
	test	r15b, 1
	je	.LBB0_629
# %bb.1018:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm11
	vpextrb	byte ptr [r8 + rbx], xmm4, 0
	vpextrb	ebx, xmm3, 1
	test	bl, 1
	jne	.LBB0_1019
.LBB0_630:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 2
	test	bl, 1
	je	.LBB0_631
.LBB0_1020:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm4, 2
	vpextrb	ebx, xmm3, 3
	test	bl, 1
	jne	.LBB0_1021
.LBB0_632:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 4
	test	bl, 1
	je	.LBB0_633
.LBB0_1022:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm10
	vpextrb	byte ptr [r8 + rbx], xmm4, 4
	vpextrb	ebx, xmm3, 5
	test	bl, 1
	jne	.LBB0_1023
.LBB0_634:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 6
	test	bl, 1
	je	.LBB0_635
.LBB0_1024:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm4, 6
	vpextrb	ebx, xmm3, 7
	test	bl, 1
	jne	.LBB0_1025
.LBB0_636:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 8
	test	bl, 1
	je	.LBB0_637
.LBB0_1026:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm9
	vpextrb	byte ptr [r8 + rbx], xmm4, 8
	vpextrb	ebx, xmm3, 9
	test	bl, 1
	jne	.LBB0_1027
.LBB0_638:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 10
	test	bl, 1
	je	.LBB0_639
.LBB0_1028:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm4, 10
	vpextrb	ebx, xmm3, 11
	test	bl, 1
	jne	.LBB0_1029
.LBB0_640:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 12
	test	bl, 1
	je	.LBB0_641
.LBB0_1030:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm5
	vpextrb	byte ptr [r8 + rbx], xmm4, 12
	vpextrb	ebx, xmm3, 13
	test	bl, 1
	vmovdqa	ymm9, ymmword ptr [rsp + 896]   # 32-byte Reload
	jne	.LBB0_1031
.LBB0_642:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 14
	test	bl, 1
	je	.LBB0_643
.LBB0_1032:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vmovq	rbx, xmm1
	vpextrb	byte ptr [r8 + rbx], xmm4, 14
	vpextrb	ebx, xmm3, 15
	test	bl, 1
	jne	.LBB0_1033
.LBB0_644:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 512], 1         # 1-byte Folded Reload
	vmovdqa	ymm3, ymmword ptr [rsp + 832]   # 32-byte Reload
	je	.LBB0_645
.LBB0_1034:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm8
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 0
	test	byte ptr [rsp + 480], 1         # 1-byte Folded Reload
	jne	.LBB0_1035
.LBB0_646:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 448], 1         # 1-byte Folded Reload
	je	.LBB0_647
.LBB0_1036:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 2
	test	byte ptr [rsp + 416], 1         # 1-byte Folded Reload
	jne	.LBB0_1037
.LBB0_648:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 384], 1         # 1-byte Folded Reload
	je	.LBB0_649
.LBB0_1038:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rbx, xmm7
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 4
	test	byte ptr [rsp + 352], 1         # 1-byte Folded Reload
	vmovdqa	ymm8, ymmword ptr [rsp + 864]   # 32-byte Reload
	jne	.LBB0_1039
.LBB0_650:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 320], 1         # 1-byte Folded Reload
	je	.LBB0_651
.LBB0_1040:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vmovq	rbx, xmm1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 6
	test	byte ptr [rsp + 152], 1         # 1-byte Folded Reload
	jne	.LBB0_1041
.LBB0_652:                              #   in Loop: Header=BB0_26 Depth=1
	test	r9b, 1
	mov	r9d, dword ptr [rsp + 16]       # 4-byte Reload
	je	.LBB0_653
.LBB0_1042:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rdx, xmm2
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rdx], xmm1, 8
	test	cl, 1
	jne	.LBB0_1043
.LBB0_654:                              #   in Loop: Header=BB0_26 Depth=1
	test	r11b, 1
	mov	r11, qword ptr [rsp + 304]      # 8-byte Reload
	je	.LBB0_655
.LBB0_1044:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm2, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 10
	test	al, 1
	jne	.LBB0_1045
.LBB0_656:                              #   in Loop: Header=BB0_26 Depth=1
	test	sil, 1
	je	.LBB0_657
.LBB0_1046:                             #   in Loop: Header=BB0_26 Depth=1
	vmovq	rcx, xmm15
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 12
	test	r10b, 1
	vmovdqa	ymm2, ymmword ptr [rsp + 800]   # 32-byte Reload
	jne	.LBB0_1047
.LBB0_658:                              #   in Loop: Header=BB0_26 Depth=1
	test	r13b, 1
	mov	r10, qword ptr [rsp + 48]       # 8-byte Reload
	je	.LBB0_659
.LBB0_1048:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vmovq	rcx, xmm1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 14
	test	r14b, 1
	je	.LBB0_25
	jmp	.LBB0_1049
	.p2align	4, 0x90
.LBB0_629:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 1
	test	bl, 1
	je	.LBB0_630
.LBB0_1019:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm11, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 1
	vpextrb	ebx, xmm3, 2
	test	bl, 1
	jne	.LBB0_1020
.LBB0_631:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 3
	test	bl, 1
	je	.LBB0_632
.LBB0_1021:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm11, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 3
	vpextrb	ebx, xmm3, 4
	test	bl, 1
	jne	.LBB0_1022
.LBB0_633:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 5
	test	bl, 1
	je	.LBB0_634
.LBB0_1023:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm10, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 5
	vpextrb	ebx, xmm3, 6
	test	bl, 1
	jne	.LBB0_1024
.LBB0_635:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 7
	test	bl, 1
	je	.LBB0_636
.LBB0_1025:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm10, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 7
	vpextrb	ebx, xmm3, 8
	test	bl, 1
	jne	.LBB0_1026
.LBB0_637:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 9
	test	bl, 1
	je	.LBB0_638
.LBB0_1027:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm9, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 9
	vpextrb	ebx, xmm3, 10
	test	bl, 1
	jne	.LBB0_1028
.LBB0_639:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 11
	test	bl, 1
	je	.LBB0_640
.LBB0_1029:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm9, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 11
	vpextrb	ebx, xmm3, 12
	test	bl, 1
	jne	.LBB0_1030
.LBB0_641:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 13
	test	bl, 1
	vmovdqa	ymm9, ymmword ptr [rsp + 896]   # 32-byte Reload
	je	.LBB0_642
.LBB0_1031:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm5, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 13
	vpextrb	ebx, xmm3, 14
	test	bl, 1
	jne	.LBB0_1032
.LBB0_643:                              #   in Loop: Header=BB0_26 Depth=1
	vpextrb	ebx, xmm3, 15
	test	bl, 1
	je	.LBB0_644
.LBB0_1033:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm5, 1
	vpextrq	rbx, xmm1, 1
	vpextrb	byte ptr [r8 + rbx], xmm4, 15
	test	byte ptr [rsp + 512], 1         # 1-byte Folded Reload
	vmovdqa	ymm3, ymmword ptr [rsp + 832]   # 32-byte Reload
	jne	.LBB0_1034
.LBB0_645:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 480], 1         # 1-byte Folded Reload
	je	.LBB0_646
.LBB0_1035:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm8, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 1
	test	byte ptr [rsp + 448], 1         # 1-byte Folded Reload
	jne	.LBB0_1036
.LBB0_647:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 416], 1         # 1-byte Folded Reload
	je	.LBB0_648
.LBB0_1037:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm8, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 3
	test	byte ptr [rsp + 384], 1         # 1-byte Folded Reload
	jne	.LBB0_1038
.LBB0_649:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 352], 1         # 1-byte Folded Reload
	vmovdqa	ymm8, ymmword ptr [rsp + 864]   # 32-byte Reload
	je	.LBB0_650
.LBB0_1039:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rbx, xmm7, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 5
	test	byte ptr [rsp + 320], 1         # 1-byte Folded Reload
	jne	.LBB0_1040
.LBB0_651:                              #   in Loop: Header=BB0_26 Depth=1
	test	byte ptr [rsp + 152], 1         # 1-byte Folded Reload
	je	.LBB0_652
.LBB0_1041:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm7, 1
	vpextrq	rbx, xmm1, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rbx], xmm1, 7
	test	r9b, 1
	mov	r9d, dword ptr [rsp + 16]       # 4-byte Reload
	jne	.LBB0_1042
.LBB0_653:                              #   in Loop: Header=BB0_26 Depth=1
	test	cl, 1
	je	.LBB0_654
.LBB0_1043:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm2, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 9
	test	r11b, 1
	mov	r11, qword ptr [rsp + 304]      # 8-byte Reload
	jne	.LBB0_1044
.LBB0_655:                              #   in Loop: Header=BB0_26 Depth=1
	test	al, 1
	je	.LBB0_656
.LBB0_1045:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm2, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 11
	test	sil, 1
	jne	.LBB0_1046
.LBB0_657:                              #   in Loop: Header=BB0_26 Depth=1
	test	r10b, 1
	vmovdqa	ymm2, ymmword ptr [rsp + 800]   # 32-byte Reload
	je	.LBB0_658
.LBB0_1047:                             #   in Loop: Header=BB0_26 Depth=1
	vpextrq	rcx, xmm15, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 13
	test	r13b, 1
	mov	r10, qword ptr [rsp + 48]       # 8-byte Reload
	jne	.LBB0_1048
.LBB0_659:                              #   in Loop: Header=BB0_26 Depth=1
	test	r14b, 1
	je	.LBB0_25
.LBB0_1049:                             #   in Loop: Header=BB0_26 Depth=1
	vextracti128	xmm1, ymm15, 1
	vpextrq	rcx, xmm1, 1
	vextracti128	xmm1, ymm4, 1
	vpextrb	byte ptr [r8 + rcx], xmm1, 15
	jmp	.LBB0_25
.LBB0_1050:
	cmp	r12, r10
	jne	.LBB0_1055
.LBB0_1051:
	lea	rsp, [rbp - 40]
	pop	rbx
	pop	r12
	pop	r13
	pop	r14
	pop	r15
	pop	rbp
	vzeroupper
	ret
.LBB0_1052:
	mov	r9d, dword ptr [rsp + 16]       # 4-byte Reload
	mov	r10, qword ptr [rsp + 48]       # 8-byte Reload
	jmp	.LBB0_1055
.LBB0_1054:
	mov	r9d, dword ptr [rsp + 16]       # 4-byte Reload
	jmp	.LBB0_1055
.Lfunc_end0:
	.size	bytes_to_bools_avx2, .Lfunc_end0-bytes_to_bools_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-++20210204121720+1fdec59bffc1-1~exp1~20210203232336.162"
	.section	".note.GNU-stack","",@progbits
	.addrsig
