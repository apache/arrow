	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_avx2
.LCPI0_0:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI0_1:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI0_2:
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
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
.LCPI0_4:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI0_3:
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
	.globl	arithmetic_avx2
	.p2align	4, 0x90
	.type	arithmetic_avx2,@function
arithmetic_avx2:                        # @arithmetic_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 4
	jg	.LBB0_16
# %bb.1:
	cmp	sil, 1
	jg	.LBB0_31
# %bb.2:
	test	sil, sil
	je	.LBB0_51
# %bb.3:
	cmp	sil, 1
	jne	.LBB0_1177
# %bb.4:
	cmp	edi, 6
	jg	.LBB0_99
# %bb.5:
	cmp	edi, 3
	jle	.LBB0_179
# %bb.6:
	cmp	edi, 4
	je	.LBB0_307
# %bb.7:
	cmp	edi, 5
	je	.LBB0_314
# %bb.8:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.9:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.10:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_595
# %bb.11:
	xor	esi, esi
.LBB0_12:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_14
.LBB0_13:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_13
.LBB0_14:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_15:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_15
	jmp	.LBB0_1177
.LBB0_16:
	cmp	sil, 6
	jg	.LBB0_41
# %bb.17:
	cmp	sil, 5
	je	.LBB0_63
# %bb.18:
	cmp	sil, 6
	jne	.LBB0_1177
# %bb.19:
	cmp	edi, 6
	jg	.LBB0_110
# %bb.20:
	cmp	edi, 3
	jle	.LBB0_188
# %bb.21:
	cmp	edi, 4
	je	.LBB0_321
# %bb.22:
	cmp	edi, 5
	je	.LBB0_328
# %bb.23:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.24:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.25:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_600
# %bb.26:
	xor	esi, esi
.LBB0_27:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_29
.LBB0_28:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_28
.LBB0_29:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_30:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_30
	jmp	.LBB0_1177
.LBB0_31:
	cmp	sil, 2
	je	.LBB0_75
# %bb.32:
	cmp	sil, 4
	jne	.LBB0_1177
# %bb.33:
	cmp	edi, 6
	jg	.LBB0_121
# %bb.34:
	cmp	edi, 3
	jle	.LBB0_197
# %bb.35:
	cmp	edi, 4
	je	.LBB0_335
# %bb.36:
	cmp	edi, 5
	je	.LBB0_338
# %bb.37:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.38:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.39:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_40
# %bb.605:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_951
# %bb.606:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_951
.LBB0_40:
	xor	ecx, ecx
.LBB0_1081:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1083
.LBB0_1082:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1082
.LBB0_1083:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_1084:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	mov	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	mov	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1084
	jmp	.LBB0_1177
.LBB0_41:
	cmp	sil, 7
	je	.LBB0_87
# %bb.42:
	cmp	sil, 9
	jne	.LBB0_1177
# %bb.43:
	cmp	edi, 6
	jg	.LBB0_128
# %bb.44:
	cmp	edi, 3
	jle	.LBB0_202
# %bb.45:
	cmp	edi, 4
	je	.LBB0_341
# %bb.46:
	cmp	edi, 5
	je	.LBB0_344
# %bb.47:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.48:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.49:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_50
# %bb.608:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_954
# %bb.609:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_954
.LBB0_50:
	xor	ecx, ecx
.LBB0_1089:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1091
.LBB0_1090:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1090
.LBB0_1091:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_1092:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	mov	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	mov	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1092
	jmp	.LBB0_1177
.LBB0_51:
	cmp	edi, 6
	jg	.LBB0_135
# %bb.52:
	cmp	edi, 3
	jle	.LBB0_207
# %bb.53:
	cmp	edi, 4
	je	.LBB0_347
# %bb.54:
	cmp	edi, 5
	je	.LBB0_354
# %bb.55:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.56:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.57:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_611
# %bb.58:
	xor	esi, esi
.LBB0_59:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_61
.LBB0_60:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_60
.LBB0_61:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_62:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_62
	jmp	.LBB0_1177
.LBB0_63:
	cmp	edi, 6
	jg	.LBB0_146
# %bb.64:
	cmp	edi, 3
	jle	.LBB0_216
# %bb.65:
	cmp	edi, 4
	je	.LBB0_361
# %bb.66:
	cmp	edi, 5
	je	.LBB0_368
# %bb.67:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.68:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.69:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_616
# %bb.70:
	xor	esi, esi
.LBB0_71:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_73
.LBB0_72:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_72
.LBB0_73:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_74:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_74
	jmp	.LBB0_1177
.LBB0_75:
	cmp	edi, 6
	jg	.LBB0_157
# %bb.76:
	cmp	edi, 3
	jle	.LBB0_225
# %bb.77:
	cmp	edi, 4
	je	.LBB0_375
# %bb.78:
	cmp	edi, 5
	je	.LBB0_382
# %bb.79:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.80:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.81:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_621
# %bb.82:
	xor	esi, esi
.LBB0_83:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_85
.LBB0_84:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_84
.LBB0_85:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_86:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	imul	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	imul	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	imul	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_86
	jmp	.LBB0_1177
.LBB0_87:
	cmp	edi, 6
	jg	.LBB0_168
# %bb.88:
	cmp	edi, 3
	jle	.LBB0_234
# %bb.89:
	cmp	edi, 4
	je	.LBB0_389
# %bb.90:
	cmp	edi, 5
	je	.LBB0_396
# %bb.91:
	cmp	edi, 6
	jne	.LBB0_1177
# %bb.92:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.93:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_626
# %bb.94:
	xor	esi, esi
.LBB0_95:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_97
.LBB0_96:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_96
.LBB0_97:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_98:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	imul	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	imul	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	imul	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_98
	jmp	.LBB0_1177
.LBB0_99:
	cmp	edi, 8
	jle	.LBB0_243
# %bb.100:
	cmp	edi, 9
	je	.LBB0_403
# %bb.101:
	cmp	edi, 11
	je	.LBB0_410
# %bb.102:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.103:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.104:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_631
# %bb.105:
	xor	esi, esi
.LBB0_106:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_108
.LBB0_107:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_107
.LBB0_108:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_109:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	vmovsd	xmm0, qword ptr [rdx + 8*rsi + 8] # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm0
	vmovsd	xmm0, qword ptr [rdx + 8*rsi + 16] # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm0
	vmovsd	xmm0, qword ptr [rdx + 8*rsi + 24] # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_109
	jmp	.LBB0_1177
.LBB0_110:
	cmp	edi, 8
	jle	.LBB0_252
# %bb.111:
	cmp	edi, 9
	je	.LBB0_417
# %bb.112:
	cmp	edi, 11
	je	.LBB0_424
# %bb.113:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.114:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.115:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_636
# %bb.116:
	xor	esi, esi
.LBB0_117:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_119
.LBB0_118:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_118
.LBB0_119:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_120:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	vmovsd	xmm0, qword ptr [rdx + 8*rsi + 8] # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm0
	vmovsd	xmm0, qword ptr [rdx + 8*rsi + 16] # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm0
	vmovsd	xmm0, qword ptr [rdx + 8*rsi + 24] # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_120
	jmp	.LBB0_1177
.LBB0_121:
	cmp	edi, 8
	jle	.LBB0_261
# %bb.122:
	cmp	edi, 9
	je	.LBB0_431
# %bb.123:
	cmp	edi, 11
	je	.LBB0_434
# %bb.124:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.125:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.126:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_127
# %bb.641:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_957
# %bb.642:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_957
.LBB0_127:
	xor	ecx, ecx
.LBB0_1097:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1099
.LBB0_1098:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1098
.LBB0_1099:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_1100:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1100
	jmp	.LBB0_1177
.LBB0_128:
	cmp	edi, 8
	jle	.LBB0_266
# %bb.129:
	cmp	edi, 9
	je	.LBB0_437
# %bb.130:
	cmp	edi, 11
	je	.LBB0_440
# %bb.131:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.132:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.133:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_134
# %bb.644:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_960
# %bb.645:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_960
.LBB0_134:
	xor	ecx, ecx
.LBB0_1105:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1107
.LBB0_1106:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1106
.LBB0_1107:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_1108:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1108
	jmp	.LBB0_1177
.LBB0_135:
	cmp	edi, 8
	jle	.LBB0_271
# %bb.136:
	cmp	edi, 9
	je	.LBB0_443
# %bb.137:
	cmp	edi, 11
	je	.LBB0_450
# %bb.138:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.139:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.140:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_647
# %bb.141:
	xor	esi, esi
.LBB0_142:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_144
.LBB0_143:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_143
.LBB0_144:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_145:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_145
	jmp	.LBB0_1177
.LBB0_146:
	cmp	edi, 8
	jle	.LBB0_280
# %bb.147:
	cmp	edi, 9
	je	.LBB0_457
# %bb.148:
	cmp	edi, 11
	je	.LBB0_464
# %bb.149:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.150:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.151:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_652
# %bb.152:
	xor	esi, esi
.LBB0_153:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_155
.LBB0_154:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_154
.LBB0_155:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_156:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_156
	jmp	.LBB0_1177
.LBB0_157:
	cmp	edi, 8
	jle	.LBB0_289
# %bb.158:
	cmp	edi, 9
	je	.LBB0_471
# %bb.159:
	cmp	edi, 11
	je	.LBB0_478
# %bb.160:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.161:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.162:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_657
# %bb.163:
	xor	esi, esi
.LBB0_164:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_166
.LBB0_165:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_165
.LBB0_166:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_167:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_167
	jmp	.LBB0_1177
.LBB0_168:
	cmp	edi, 8
	jle	.LBB0_298
# %bb.169:
	cmp	edi, 9
	je	.LBB0_485
# %bb.170:
	cmp	edi, 11
	je	.LBB0_492
# %bb.171:
	cmp	edi, 12
	jne	.LBB0_1177
# %bb.172:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.173:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_662
# %bb.174:
	xor	esi, esi
.LBB0_175:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_177
.LBB0_176:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_176
.LBB0_177:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_178:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm0
	vmovsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_178
	jmp	.LBB0_1177
.LBB0_179:
	cmp	edi, 2
	je	.LBB0_499
# %bb.180:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.181:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.182:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_667
# %bb.183:
	xor	esi, esi
.LBB0_184:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_186
.LBB0_185:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_185
.LBB0_186:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_187:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_187
	jmp	.LBB0_1177
.LBB0_188:
	cmp	edi, 2
	je	.LBB0_506
# %bb.189:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.190:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.191:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_672
# %bb.192:
	xor	esi, esi
.LBB0_193:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_195
.LBB0_194:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_194
.LBB0_195:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_196:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_196
	jmp	.LBB0_1177
.LBB0_197:
	cmp	edi, 2
	je	.LBB0_513
# %bb.198:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.199:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.200:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_201
# %bb.677:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB0_963
# %bb.678:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB0_963
.LBB0_201:
	xor	ecx, ecx
.LBB0_966:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_968
# %bb.967:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_968:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_969:                              # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdx + rcx]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx], sil
	movsx	esi, byte ptr [rdx + rcx + 1]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx + 1], sil
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_969
	jmp	.LBB0_1177
.LBB0_202:
	cmp	edi, 2
	je	.LBB0_516
# %bb.203:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.204:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.205:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_206
# %bb.680:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB0_970
# %bb.681:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB0_970
.LBB0_206:
	xor	ecx, ecx
.LBB0_973:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_975
# %bb.974:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_975:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_976:                              # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdx + rcx]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx], sil
	movsx	esi, byte ptr [rdx + rcx + 1]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx + 1], sil
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_976
	jmp	.LBB0_1177
.LBB0_207:
	cmp	edi, 2
	je	.LBB0_519
# %bb.208:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.209:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.210:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_683
# %bb.211:
	xor	esi, esi
.LBB0_212:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_214
.LBB0_213:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_213
.LBB0_214:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_215:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, byte ptr [rdx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_215
	jmp	.LBB0_1177
.LBB0_216:
	cmp	edi, 2
	je	.LBB0_526
# %bb.217:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.218:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.219:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_688
# %bb.220:
	xor	esi, esi
.LBB0_221:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_223
.LBB0_222:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_222
.LBB0_223:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_224:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, byte ptr [rdx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_224
	jmp	.LBB0_1177
.LBB0_225:
	cmp	edi, 2
	je	.LBB0_533
# %bb.226:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.227:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.228:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_693
# %bb.229:
	xor	edi, edi
.LBB0_230:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_232
.LBB0_231:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_231
.LBB0_232:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_233:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	byte ptr [rdx + rdi + 1]
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	byte ptr [rdx + rdi + 2]
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	byte ptr [rdx + rdi + 3]
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB0_233
	jmp	.LBB0_1177
.LBB0_234:
	cmp	edi, 2
	je	.LBB0_540
# %bb.235:
	cmp	edi, 3
	jne	.LBB0_1177
# %bb.236:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.237:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_697
# %bb.238:
	xor	edi, edi
.LBB0_239:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_241
.LBB0_240:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_240
.LBB0_241:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_242:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	byte ptr [rdx + rdi + 1]
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	byte ptr [rdx + rdi + 2]
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	byte ptr [rdx + rdi + 3]
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB0_242
	jmp	.LBB0_1177
.LBB0_243:
	cmp	edi, 7
	je	.LBB0_547
# %bb.244:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.245:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.246:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_701
# %bb.247:
	xor	esi, esi
.LBB0_248:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_250
.LBB0_249:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_249
.LBB0_250:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_251:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_251
	jmp	.LBB0_1177
.LBB0_252:
	cmp	edi, 7
	je	.LBB0_554
# %bb.253:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.254:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.255:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_706
# %bb.256:
	xor	esi, esi
.LBB0_257:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_259
.LBB0_258:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_258
.LBB0_259:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_260:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_260
	jmp	.LBB0_1177
.LBB0_261:
	cmp	edi, 7
	je	.LBB0_561
# %bb.262:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.263:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.264:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_265
# %bb.711:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_977
# %bb.712:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_977
.LBB0_265:
	xor	ecx, ecx
.LBB0_1113:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1115
.LBB0_1114:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1114
.LBB0_1115:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_1116:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1116
	jmp	.LBB0_1177
.LBB0_266:
	cmp	edi, 7
	je	.LBB0_564
# %bb.267:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.268:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.269:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_270
# %bb.714:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_980
# %bb.715:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_980
.LBB0_270:
	xor	ecx, ecx
.LBB0_1121:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1123
.LBB0_1122:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1122
.LBB0_1123:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_1124:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1124
	jmp	.LBB0_1177
.LBB0_271:
	cmp	edi, 7
	je	.LBB0_567
# %bb.272:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.273:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.274:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_717
# %bb.275:
	xor	esi, esi
.LBB0_276:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_278
.LBB0_277:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_277
.LBB0_278:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_279:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_279
	jmp	.LBB0_1177
.LBB0_280:
	cmp	edi, 7
	je	.LBB0_574
# %bb.281:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.282:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.283:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_722
# %bb.284:
	xor	esi, esi
.LBB0_285:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_287
.LBB0_286:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_286
.LBB0_287:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_288:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_288
	jmp	.LBB0_1177
.LBB0_289:
	cmp	edi, 7
	je	.LBB0_581
# %bb.290:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.291:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.292:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_727
# %bb.293:
	xor	esi, esi
.LBB0_294:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_296
.LBB0_295:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_295
.LBB0_296:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_297:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	imul	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	imul	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	imul	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	imul	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_297
	jmp	.LBB0_1177
.LBB0_298:
	cmp	edi, 7
	je	.LBB0_588
# %bb.299:
	cmp	edi, 8
	jne	.LBB0_1177
# %bb.300:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.301:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_732
# %bb.302:
	xor	esi, esi
.LBB0_303:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_305
.LBB0_304:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_304
.LBB0_305:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_306:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	imul	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	imul	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	imul	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	imul	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_306
	jmp	.LBB0_1177
.LBB0_307:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.308:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_737
# %bb.309:
	xor	esi, esi
.LBB0_310:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_312
.LBB0_311:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_311
.LBB0_312:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_313:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	sub	ax, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	sub	ax, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	sub	ax, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_313
	jmp	.LBB0_1177
.LBB0_314:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.315:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_742
# %bb.316:
	xor	esi, esi
.LBB0_317:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_319
.LBB0_318:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_318
.LBB0_319:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_320:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	sub	ax, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	sub	ax, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	sub	ax, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_320
	jmp	.LBB0_1177
.LBB0_321:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.322:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_747
# %bb.323:
	xor	esi, esi
.LBB0_324:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_326
.LBB0_325:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_325
.LBB0_326:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_327:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	sub	ax, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	sub	ax, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	sub	ax, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_327
	jmp	.LBB0_1177
.LBB0_328:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.329:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_752
# %bb.330:
	xor	esi, esi
.LBB0_331:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_333
.LBB0_332:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_332
.LBB0_333:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_334:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	sub	ax, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	sub	ax, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	sub	ax, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_334
	jmp	.LBB0_1177
.LBB0_335:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.336:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_337
# %bb.757:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_983
# %bb.758:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_983
.LBB0_337:
	xor	ecx, ecx
.LBB0_1063:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1065
.LBB0_1064:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1064
.LBB0_1065:
	cmp	rax, 3
	jb	.LBB0_1177
.LBB0_1066:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], ax
	movzx	eax, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], ax
	movzx	eax, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], ax
	movzx	eax, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1066
	jmp	.LBB0_1177
.LBB0_338:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.339:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_340
# %bb.760:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_985
# %bb.761:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_985
.LBB0_340:
	xor	ecx, ecx
.LBB0_1129:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1131
# %bb.1130:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1131:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_1132:                             # =>This Inner Loop Header: Depth=1
	movsx	esi, word ptr [rdx + 2*rcx]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx], si
	movsx	esi, word ptr [rdx + 2*rcx + 2]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx + 2], si
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_1132
	jmp	.LBB0_1177
.LBB0_341:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.342:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_343
# %bb.763:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_988
# %bb.764:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_988
.LBB0_343:
	xor	ecx, ecx
.LBB0_1073:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1075
.LBB0_1074:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1074
.LBB0_1075:
	cmp	rax, 3
	jb	.LBB0_1177
.LBB0_1076:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], ax
	movzx	eax, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], ax
	movzx	eax, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], ax
	movzx	eax, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1076
	jmp	.LBB0_1177
.LBB0_344:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.345:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_346
# %bb.766:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_990
# %bb.767:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_990
.LBB0_346:
	xor	ecx, ecx
.LBB0_1137:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1139
# %bb.1138:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1139:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_1140:                             # =>This Inner Loop Header: Depth=1
	movsx	esi, word ptr [rdx + 2*rcx]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx], si
	movsx	esi, word ptr [rdx + 2*rcx + 2]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx + 2], si
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_1140
	jmp	.LBB0_1177
.LBB0_347:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.348:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_769
# %bb.349:
	xor	esi, esi
.LBB0_350:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_352
.LBB0_351:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_351
.LBB0_352:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_353:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_353
	jmp	.LBB0_1177
.LBB0_354:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.355:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_774
# %bb.356:
	xor	esi, esi
.LBB0_357:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_359
.LBB0_358:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_358
.LBB0_359:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_360:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_360
	jmp	.LBB0_1177
.LBB0_361:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.362:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_779
# %bb.363:
	xor	esi, esi
.LBB0_364:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_366
.LBB0_365:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_365
.LBB0_366:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_367:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_367
	jmp	.LBB0_1177
.LBB0_368:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.369:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_784
# %bb.370:
	xor	esi, esi
.LBB0_371:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_373
.LBB0_372:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_372
.LBB0_373:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_374:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_374
	jmp	.LBB0_1177
.LBB0_375:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.376:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_789
# %bb.377:
	xor	esi, esi
.LBB0_378:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_380
.LBB0_379:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_379
.LBB0_380:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_381:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	imul	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	imul	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	imul	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_381
	jmp	.LBB0_1177
.LBB0_382:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.383:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_794
# %bb.384:
	xor	esi, esi
.LBB0_385:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_387
.LBB0_386:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_386
.LBB0_387:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_388:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	imul	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	imul	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	imul	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_388
	jmp	.LBB0_1177
.LBB0_389:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.390:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_799
# %bb.391:
	xor	esi, esi
.LBB0_392:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_394
.LBB0_393:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_393
.LBB0_394:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_395:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	imul	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	imul	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	imul	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_395
	jmp	.LBB0_1177
.LBB0_396:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.397:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_804
# %bb.398:
	xor	esi, esi
.LBB0_399:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_401
.LBB0_400:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_400
.LBB0_401:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_402:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	imul	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	imul	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	imul	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_402
	jmp	.LBB0_1177
.LBB0_403:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.404:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_809
# %bb.405:
	xor	esi, esi
.LBB0_406:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_408
.LBB0_407:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_407
.LBB0_408:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_409:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_409
	jmp	.LBB0_1177
.LBB0_410:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.411:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_814
# %bb.412:
	xor	esi, esi
.LBB0_413:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_415
.LBB0_414:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_414
.LBB0_415:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_416:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	vmovss	xmm0, dword ptr [rdx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm0
	vmovss	xmm0, dword ptr [rdx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm0
	vmovss	xmm0, dword ptr [rdx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_416
	jmp	.LBB0_1177
.LBB0_417:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.418:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_819
# %bb.419:
	xor	esi, esi
.LBB0_420:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_422
.LBB0_421:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_421
.LBB0_422:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_423:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_423
	jmp	.LBB0_1177
.LBB0_424:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.425:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_824
# %bb.426:
	xor	esi, esi
.LBB0_427:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_429
.LBB0_428:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_428
.LBB0_429:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_430:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	vmovss	xmm0, dword ptr [rdx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm0
	vmovss	xmm0, dword ptr [rdx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm0
	vmovss	xmm0, dword ptr [rdx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_430
	jmp	.LBB0_1177
.LBB0_431:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.432:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_433
# %bb.829:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_993
# %bb.830:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_993
.LBB0_433:
	xor	ecx, ecx
.LBB0_996:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_998
# %bb.997:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_998:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_999:                              # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rdi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_999
	jmp	.LBB0_1177
.LBB0_434:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.435:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_436
# %bb.832:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_1000
# %bb.833:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_1000
.LBB0_436:
	xor	ecx, ecx
.LBB0_1145:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1148
# %bb.1146:
	mov	esi, 2147483647
.LBB0_1147:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1147
.LBB0_1148:
	cmp	r9, 3
	jb	.LBB0_1177
# %bb.1149:
	mov	esi, 2147483647
.LBB0_1150:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1150
	jmp	.LBB0_1177
.LBB0_437:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.438:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_439
# %bb.835:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1003
# %bb.836:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1003
.LBB0_439:
	xor	ecx, ecx
.LBB0_1006:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1008
# %bb.1007:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1008:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_1009:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rdi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_1009
	jmp	.LBB0_1177
.LBB0_440:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.441:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_442
# %bb.838:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_1010
# %bb.839:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_1010
.LBB0_442:
	xor	ecx, ecx
.LBB0_1155:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1158
# %bb.1156:
	mov	esi, 2147483647
.LBB0_1157:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1157
.LBB0_1158:
	cmp	r9, 3
	jb	.LBB0_1177
# %bb.1159:
	mov	esi, 2147483647
.LBB0_1160:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1160
	jmp	.LBB0_1177
.LBB0_443:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.444:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_841
# %bb.445:
	xor	esi, esi
.LBB0_446:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_448
.LBB0_447:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_447
.LBB0_448:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_449:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_449
	jmp	.LBB0_1177
.LBB0_450:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.451:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_846
# %bb.452:
	xor	esi, esi
.LBB0_453:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_455
.LBB0_454:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_454
.LBB0_455:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_456
	jmp	.LBB0_1177
.LBB0_457:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.458:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_851
# %bb.459:
	xor	esi, esi
.LBB0_460:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_462
.LBB0_461:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_461
.LBB0_462:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_463:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_463
	jmp	.LBB0_1177
.LBB0_464:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.465:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_856
# %bb.466:
	xor	esi, esi
.LBB0_467:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_469
.LBB0_468:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_468
.LBB0_469:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_470:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_470
	jmp	.LBB0_1177
.LBB0_471:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.472:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_861
# %bb.473:
	xor	esi, esi
.LBB0_474:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_476
.LBB0_475:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_475
.LBB0_476:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_477:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	imul	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	imul	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	imul	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	imul	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_477
	jmp	.LBB0_1177
.LBB0_478:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.479:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_866
# %bb.480:
	xor	esi, esi
.LBB0_481:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_483
.LBB0_482:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_482
.LBB0_483:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_484:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_484
	jmp	.LBB0_1177
.LBB0_485:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.486:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_871
# %bb.487:
	xor	esi, esi
.LBB0_488:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_490
.LBB0_489:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_489
.LBB0_490:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_491:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	imul	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	imul	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	imul	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	imul	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_491
	jmp	.LBB0_1177
.LBB0_492:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.493:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_876
# %bb.494:
	xor	esi, esi
.LBB0_495:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_497
.LBB0_496:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_496
.LBB0_497:
	cmp	rdi, 3
	jb	.LBB0_1177
.LBB0_498:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm0
	vmovss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_498
	jmp	.LBB0_1177
.LBB0_499:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.500:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_881
# %bb.501:
	xor	esi, esi
.LBB0_502:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_504
.LBB0_503:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_503
.LBB0_504:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_505:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_505
	jmp	.LBB0_1177
.LBB0_506:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.507:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_886
# %bb.508:
	xor	esi, esi
.LBB0_509:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_511
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_510
.LBB0_511:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_512:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_512
	jmp	.LBB0_1177
.LBB0_513:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.514:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_515
# %bb.891:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1013
# %bb.892:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1013
.LBB0_515:
	xor	ecx, ecx
.LBB0_1165:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1167
.LBB0_1166:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1166
.LBB0_1167:
	cmp	rsi, 3
	jb	.LBB0_1177
.LBB0_1168:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	movzx	eax, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	movzx	eax, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	mov	byte ptr [r8 + rcx + 3], al
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1168
	jmp	.LBB0_1177
.LBB0_516:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.517:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_518
# %bb.894:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1016
# %bb.895:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1016
.LBB0_518:
	xor	ecx, ecx
.LBB0_1173:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1175
.LBB0_1174:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1174
.LBB0_1175:
	cmp	rsi, 3
	jb	.LBB0_1177
.LBB0_1176:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	movzx	eax, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	movzx	eax, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	mov	byte ptr [r8 + rcx + 3], al
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1176
	jmp	.LBB0_1177
.LBB0_519:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.520:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_897
# %bb.521:
	xor	esi, esi
.LBB0_522:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_524
.LBB0_523:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_523
.LBB0_524:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_525:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, byte ptr [rdx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_525
	jmp	.LBB0_1177
.LBB0_526:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.527:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_902
# %bb.528:
	xor	esi, esi
.LBB0_529:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_531
.LBB0_530:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_530
.LBB0_531:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_532:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, byte ptr [rdx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_532
	jmp	.LBB0_1177
.LBB0_533:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.534:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_907
# %bb.535:
	xor	edi, edi
.LBB0_536:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_538
.LBB0_537:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_537
.LBB0_538:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_539:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	byte ptr [rdx + rdi + 1]
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	byte ptr [rdx + rdi + 2]
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	byte ptr [rdx + rdi + 3]
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB0_539
	jmp	.LBB0_1177
.LBB0_540:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.541:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_911
# %bb.542:
	xor	edi, edi
.LBB0_543:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_545
.LBB0_544:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_544
.LBB0_545:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_546:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	byte ptr [rdx + rdi + 1]
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	byte ptr [rdx + rdi + 2]
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	byte ptr [rdx + rdi + 3]
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB0_546
	jmp	.LBB0_1177
.LBB0_547:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.548:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_915
# %bb.549:
	xor	esi, esi
.LBB0_550:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_552
.LBB0_551:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_551
.LBB0_552:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_553:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_553
	jmp	.LBB0_1177
.LBB0_554:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.555:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_920
# %bb.556:
	xor	esi, esi
.LBB0_557:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_559
.LBB0_558:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_558
.LBB0_559:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_560:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_560
	jmp	.LBB0_1177
.LBB0_561:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.562:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_563
# %bb.925:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1019
# %bb.926:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1019
.LBB0_563:
	xor	ecx, ecx
.LBB0_1022:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1024
# %bb.1023:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1024:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_1025:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx], edi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx + 4], edi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_1025
	jmp	.LBB0_1177
.LBB0_564:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.565:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_566
# %bb.928:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1026
# %bb.929:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1026
.LBB0_566:
	xor	ecx, ecx
.LBB0_1029:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1031
# %bb.1030:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1031:
	add	rsi, rax
	je	.LBB0_1177
.LBB0_1032:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx], edi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx + 4], edi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB0_1032
	jmp	.LBB0_1177
.LBB0_567:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.568:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_931
# %bb.569:
	xor	esi, esi
.LBB0_570:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_572
.LBB0_571:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_571
.LBB0_572:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_573:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_573
	jmp	.LBB0_1177
.LBB0_574:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.575:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_936
# %bb.576:
	xor	esi, esi
.LBB0_577:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_579
.LBB0_578:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_578
.LBB0_579:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_580:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_580
	jmp	.LBB0_1177
.LBB0_581:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.582:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_941
# %bb.583:
	xor	esi, esi
.LBB0_584:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_586
.LBB0_585:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_585
.LBB0_586:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_587:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	imul	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	imul	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	imul	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_587
	jmp	.LBB0_1177
.LBB0_588:
	test	r9d, r9d
	jle	.LBB0_1177
# %bb.589:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_946
# %bb.590:
	xor	esi, esi
.LBB0_591:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_593
.LBB0_592:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_592
.LBB0_593:
	cmp	r9, 3
	jb	.LBB0_1177
.LBB0_594:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	imul	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	imul	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	imul	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_594
	jmp	.LBB0_1177
.LBB0_595:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_12
# %bb.596:
	and	al, dil
	jne	.LBB0_12
# %bb.597:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_598:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm2, ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm3, ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_598
# %bb.599:
	cmp	rsi, r10
	jne	.LBB0_12
	jmp	.LBB0_1177
.LBB0_600:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_27
# %bb.601:
	and	al, dil
	jne	.LBB0_27
# %bb.602:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_603:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm2, ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm3, ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_603
# %bb.604:
	cmp	rsi, r10
	jne	.LBB0_27
	jmp	.LBB0_1177
.LBB0_611:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_59
# %bb.612:
	and	al, dil
	jne	.LBB0_59
# %bb.613:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_614:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_614
# %bb.615:
	cmp	rsi, r10
	jne	.LBB0_59
	jmp	.LBB0_1177
.LBB0_616:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_71
# %bb.617:
	and	al, dil
	jne	.LBB0_71
# %bb.618:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_619:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_619
# %bb.620:
	cmp	rsi, r10
	jne	.LBB0_71
	jmp	.LBB0_1177
.LBB0_621:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_83
# %bb.622:
	and	al, dil
	jne	.LBB0_83
# %bb.623:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_624:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_624
# %bb.625:
	cmp	rsi, r10
	jne	.LBB0_83
	jmp	.LBB0_1177
.LBB0_626:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_95
# %bb.627:
	and	al, dil
	jne	.LBB0_95
# %bb.628:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_629:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_629
# %bb.630:
	cmp	rsi, r10
	jne	.LBB0_95
	jmp	.LBB0_1177
.LBB0_631:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_106
# %bb.632:
	and	al, dil
	jne	.LBB0_106
# %bb.633:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_634:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vsubpd	ymm0, ymm0, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm2, ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm3, ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_634
# %bb.635:
	cmp	rsi, r10
	jne	.LBB0_106
	jmp	.LBB0_1177
.LBB0_636:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_117
# %bb.637:
	and	al, dil
	jne	.LBB0_117
# %bb.638:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_639:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vsubpd	ymm0, ymm0, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm2, ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm3, ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_639
# %bb.640:
	cmp	rsi, r10
	jne	.LBB0_117
	jmp	.LBB0_1177
.LBB0_647:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_142
# %bb.648:
	and	al, dil
	jne	.LBB0_142
# %bb.649:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_650:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vaddpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_650
# %bb.651:
	cmp	rsi, r10
	jne	.LBB0_142
	jmp	.LBB0_1177
.LBB0_652:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_153
# %bb.653:
	and	al, dil
	jne	.LBB0_153
# %bb.654:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_655:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vaddpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_655
# %bb.656:
	cmp	rsi, r10
	jne	.LBB0_153
	jmp	.LBB0_1177
.LBB0_657:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_164
# %bb.658:
	and	al, dil
	jne	.LBB0_164
# %bb.659:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_660:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmulpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_660
# %bb.661:
	cmp	rsi, r10
	jne	.LBB0_164
	jmp	.LBB0_1177
.LBB0_662:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_175
# %bb.663:
	and	al, dil
	jne	.LBB0_175
# %bb.664:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_665:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmulpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_665
# %bb.666:
	cmp	rsi, r10
	jne	.LBB0_175
	jmp	.LBB0_1177
.LBB0_667:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_184
# %bb.668:
	and	al, dil
	jne	.LBB0_184
# %bb.669:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_670:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm1, ymm1, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm2, ymm2, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm3, ymm3, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_670
# %bb.671:
	cmp	rsi, r10
	jne	.LBB0_184
	jmp	.LBB0_1177
.LBB0_672:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_193
# %bb.673:
	and	al, dil
	jne	.LBB0_193
# %bb.674:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_675:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm1, ymm1, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm2, ymm2, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm3, ymm3, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_675
# %bb.676:
	cmp	rsi, r10
	jne	.LBB0_193
	jmp	.LBB0_1177
.LBB0_683:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_212
# %bb.684:
	and	al, dil
	jne	.LBB0_212
# %bb.685:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_686:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rdi + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_686
# %bb.687:
	cmp	rsi, r10
	jne	.LBB0_212
	jmp	.LBB0_1177
.LBB0_688:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_221
# %bb.689:
	and	al, dil
	jne	.LBB0_221
# %bb.690:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_691:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rdi + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_691
# %bb.692:
	cmp	rsi, r10
	jne	.LBB0_221
	jmp	.LBB0_1177
.LBB0_693:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_230
# %bb.694:
	and	al, sil
	jne	.LBB0_230
# %bb.695:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1033
# %bb.696:
	xor	esi, esi
	jmp	.LBB0_1035
.LBB0_697:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_239
# %bb.698:
	and	al, sil
	jne	.LBB0_239
# %bb.699:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1039
# %bb.700:
	xor	esi, esi
	jmp	.LBB0_1041
.LBB0_701:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_248
# %bb.702:
	and	al, dil
	jne	.LBB0_248
# %bb.703:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_704:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm2, ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm3, ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_704
# %bb.705:
	cmp	rsi, r10
	jne	.LBB0_248
	jmp	.LBB0_1177
.LBB0_706:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_257
# %bb.707:
	and	al, dil
	jne	.LBB0_257
# %bb.708:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_709:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm2, ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm3, ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_709
# %bb.710:
	cmp	rsi, r10
	jne	.LBB0_257
	jmp	.LBB0_1177
.LBB0_717:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_276
# %bb.718:
	and	al, dil
	jne	.LBB0_276
# %bb.719:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_720:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_720
# %bb.721:
	cmp	rsi, r10
	jne	.LBB0_276
	jmp	.LBB0_1177
.LBB0_722:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_285
# %bb.723:
	and	al, dil
	jne	.LBB0_285
# %bb.724:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_725:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_725
# %bb.726:
	cmp	rsi, r10
	jne	.LBB0_285
	jmp	.LBB0_1177
.LBB0_727:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_294
# %bb.728:
	and	al, dil
	jne	.LBB0_294
# %bb.729:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_730:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rdi + 96]
	vpsrlq	ymm8, ymm4, 32
	vpmuludq	ymm8, ymm8, ymm1
	vpsrlq	ymm9, ymm1, 32
	vpmuludq	ymm9, ymm9, ymm4
	vpaddq	ymm8, ymm9, ymm8
	vpsllq	ymm8, ymm8, 32
	vpmuludq	ymm1, ymm4, ymm1
	vpaddq	ymm1, ymm8, ymm1
	vpsrlq	ymm4, ymm5, 32
	vpmuludq	ymm4, ymm4, ymm2
	vpsrlq	ymm8, ymm2, 32
	vpmuludq	ymm8, ymm8, ymm5
	vpaddq	ymm4, ymm8, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm2, ymm5, ymm2
	vpaddq	ymm2, ymm2, ymm4
	vpsrlq	ymm4, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm3
	vpsrlq	ymm5, ymm3, 32
	vpmuludq	ymm5, ymm6, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm3, ymm6, ymm3
	vpaddq	ymm3, ymm3, ymm4
	vpsrlq	ymm4, ymm7, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpsrlq	ymm5, ymm0, 32
	vpmuludq	ymm5, ymm7, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm0, ymm7, ymm0
	vpaddq	ymm0, ymm0, ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_730
# %bb.731:
	cmp	rsi, r10
	jne	.LBB0_294
	jmp	.LBB0_1177
.LBB0_732:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_303
# %bb.733:
	and	al, dil
	jne	.LBB0_303
# %bb.734:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_735:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rdi + 96]
	vpsrlq	ymm8, ymm4, 32
	vpmuludq	ymm8, ymm8, ymm1
	vpsrlq	ymm9, ymm1, 32
	vpmuludq	ymm9, ymm9, ymm4
	vpaddq	ymm8, ymm9, ymm8
	vpsllq	ymm8, ymm8, 32
	vpmuludq	ymm1, ymm4, ymm1
	vpaddq	ymm1, ymm8, ymm1
	vpsrlq	ymm4, ymm5, 32
	vpmuludq	ymm4, ymm4, ymm2
	vpsrlq	ymm8, ymm2, 32
	vpmuludq	ymm8, ymm8, ymm5
	vpaddq	ymm4, ymm8, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm2, ymm5, ymm2
	vpaddq	ymm2, ymm2, ymm4
	vpsrlq	ymm4, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm3
	vpsrlq	ymm5, ymm3, 32
	vpmuludq	ymm5, ymm6, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm3, ymm6, ymm3
	vpaddq	ymm3, ymm3, ymm4
	vpsrlq	ymm4, ymm7, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpsrlq	ymm5, ymm0, 32
	vpmuludq	ymm5, ymm7, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm0, ymm7, ymm0
	vpaddq	ymm0, ymm0, ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_735
# %bb.736:
	cmp	rsi, r10
	jne	.LBB0_303
	jmp	.LBB0_1177
.LBB0_737:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_310
# %bb.738:
	and	al, dil
	jne	.LBB0_310
# %bb.739:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_740:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm1, ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vpsubw	ymm2, ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm3, ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_740
# %bb.741:
	cmp	rsi, r10
	jne	.LBB0_310
	jmp	.LBB0_1177
.LBB0_742:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_317
# %bb.743:
	and	al, dil
	jne	.LBB0_317
# %bb.744:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_745:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm1, ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vpsubw	ymm2, ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm3, ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_745
# %bb.746:
	cmp	rsi, r10
	jne	.LBB0_317
	jmp	.LBB0_1177
.LBB0_747:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_324
# %bb.748:
	and	al, dil
	jne	.LBB0_324
# %bb.749:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_750:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm1, ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vpsubw	ymm2, ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm3, ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_750
# %bb.751:
	cmp	rsi, r10
	jne	.LBB0_324
	jmp	.LBB0_1177
.LBB0_752:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_331
# %bb.753:
	and	al, dil
	jne	.LBB0_331
# %bb.754:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_755:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm1, ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vpsubw	ymm2, ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm3, ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_755
# %bb.756:
	cmp	rsi, r10
	jne	.LBB0_331
	jmp	.LBB0_1177
.LBB0_769:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_350
# %bb.770:
	and	al, dil
	jne	.LBB0_350
# %bb.771:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_772:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_772
# %bb.773:
	cmp	rsi, r10
	jne	.LBB0_350
	jmp	.LBB0_1177
.LBB0_774:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_357
# %bb.775:
	and	al, dil
	jne	.LBB0_357
# %bb.776:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_777:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_777
# %bb.778:
	cmp	rsi, r10
	jne	.LBB0_357
	jmp	.LBB0_1177
.LBB0_779:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_364
# %bb.780:
	and	al, dil
	jne	.LBB0_364
# %bb.781:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_782:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_782
# %bb.783:
	cmp	rsi, r10
	jne	.LBB0_364
	jmp	.LBB0_1177
.LBB0_784:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_371
# %bb.785:
	and	al, dil
	jne	.LBB0_371
# %bb.786:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_787:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_787
# %bb.788:
	cmp	rsi, r10
	jne	.LBB0_371
	jmp	.LBB0_1177
.LBB0_789:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_378
# %bb.790:
	and	al, dil
	jne	.LBB0_378
# %bb.791:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_792:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpmullw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_792
# %bb.793:
	cmp	rsi, r10
	jne	.LBB0_378
	jmp	.LBB0_1177
.LBB0_794:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_385
# %bb.795:
	and	al, dil
	jne	.LBB0_385
# %bb.796:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_797:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpmullw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_797
# %bb.798:
	cmp	rsi, r10
	jne	.LBB0_385
	jmp	.LBB0_1177
.LBB0_799:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_392
# %bb.800:
	and	al, dil
	jne	.LBB0_392
# %bb.801:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_802:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpmullw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_802
# %bb.803:
	cmp	rsi, r10
	jne	.LBB0_392
	jmp	.LBB0_1177
.LBB0_804:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_399
# %bb.805:
	and	al, dil
	jne	.LBB0_399
# %bb.806:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_807:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rdi + 96]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm1, ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpmullw	ymm2, ymm2, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm3, ymm3, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	cmp	rsi, rdi
	jne	.LBB0_807
# %bb.808:
	cmp	rsi, r10
	jne	.LBB0_399
	jmp	.LBB0_1177
.LBB0_809:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_406
# %bb.810:
	and	al, dil
	jne	.LBB0_406
# %bb.811:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_812:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm2, ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm3, ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_812
# %bb.813:
	cmp	rsi, r10
	jne	.LBB0_406
	jmp	.LBB0_1177
.LBB0_814:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_413
# %bb.815:
	and	al, dil
	jne	.LBB0_413
# %bb.816:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_817:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vsubps	ymm0, ymm0, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm2, ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm3, ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_817
# %bb.818:
	cmp	rsi, r10
	jne	.LBB0_413
	jmp	.LBB0_1177
.LBB0_819:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_420
# %bb.820:
	and	al, dil
	jne	.LBB0_420
# %bb.821:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_822:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm2, ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm3, ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_822
# %bb.823:
	cmp	rsi, r10
	jne	.LBB0_420
	jmp	.LBB0_1177
.LBB0_824:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_427
# %bb.825:
	and	al, dil
	jne	.LBB0_427
# %bb.826:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_827:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vsubps	ymm0, ymm0, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm2, ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm3, ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_827
# %bb.828:
	cmp	rsi, r10
	jne	.LBB0_427
	jmp	.LBB0_1177
.LBB0_841:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_446
# %bb.842:
	and	al, dil
	jne	.LBB0_446
# %bb.843:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_844:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_844
# %bb.845:
	cmp	rsi, r10
	jne	.LBB0_446
	jmp	.LBB0_1177
.LBB0_846:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_453
# %bb.847:
	and	al, dil
	jne	.LBB0_453
# %bb.848:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_849:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vaddps	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_849
# %bb.850:
	cmp	rsi, r10
	jne	.LBB0_453
	jmp	.LBB0_1177
.LBB0_851:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_460
# %bb.852:
	and	al, dil
	jne	.LBB0_460
# %bb.853:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_854:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_854
# %bb.855:
	cmp	rsi, r10
	jne	.LBB0_460
	jmp	.LBB0_1177
.LBB0_856:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_467
# %bb.857:
	and	al, dil
	jne	.LBB0_467
# %bb.858:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_859:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vaddps	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_859
# %bb.860:
	cmp	rsi, r10
	jne	.LBB0_467
	jmp	.LBB0_1177
.LBB0_861:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_474
# %bb.862:
	and	al, dil
	jne	.LBB0_474
# %bb.863:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_864:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rdi + 96]
	vpsrlq	ymm8, ymm4, 32
	vpmuludq	ymm8, ymm8, ymm1
	vpsrlq	ymm9, ymm1, 32
	vpmuludq	ymm9, ymm9, ymm4
	vpaddq	ymm8, ymm9, ymm8
	vpsllq	ymm8, ymm8, 32
	vpmuludq	ymm1, ymm4, ymm1
	vpaddq	ymm1, ymm8, ymm1
	vpsrlq	ymm4, ymm5, 32
	vpmuludq	ymm4, ymm4, ymm2
	vpsrlq	ymm8, ymm2, 32
	vpmuludq	ymm8, ymm8, ymm5
	vpaddq	ymm4, ymm8, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm2, ymm5, ymm2
	vpaddq	ymm2, ymm2, ymm4
	vpsrlq	ymm4, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm3
	vpsrlq	ymm5, ymm3, 32
	vpmuludq	ymm5, ymm6, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm3, ymm6, ymm3
	vpaddq	ymm3, ymm3, ymm4
	vpsrlq	ymm4, ymm7, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpsrlq	ymm5, ymm0, 32
	vpmuludq	ymm5, ymm7, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm0, ymm7, ymm0
	vpaddq	ymm0, ymm0, ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_864
# %bb.865:
	cmp	rsi, r10
	jne	.LBB0_474
	jmp	.LBB0_1177
.LBB0_866:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_481
# %bb.867:
	and	al, dil
	jne	.LBB0_481
# %bb.868:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_869:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmulps	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_869
# %bb.870:
	cmp	rsi, r10
	jne	.LBB0_481
	jmp	.LBB0_1177
.LBB0_871:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_488
# %bb.872:
	and	al, dil
	jne	.LBB0_488
# %bb.873:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_874:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rdi + 96]
	vpsrlq	ymm8, ymm4, 32
	vpmuludq	ymm8, ymm8, ymm1
	vpsrlq	ymm9, ymm1, 32
	vpmuludq	ymm9, ymm9, ymm4
	vpaddq	ymm8, ymm9, ymm8
	vpsllq	ymm8, ymm8, 32
	vpmuludq	ymm1, ymm4, ymm1
	vpaddq	ymm1, ymm8, ymm1
	vpsrlq	ymm4, ymm5, 32
	vpmuludq	ymm4, ymm4, ymm2
	vpsrlq	ymm8, ymm2, 32
	vpmuludq	ymm8, ymm8, ymm5
	vpaddq	ymm4, ymm8, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm2, ymm5, ymm2
	vpaddq	ymm2, ymm2, ymm4
	vpsrlq	ymm4, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm3
	vpsrlq	ymm5, ymm3, 32
	vpmuludq	ymm5, ymm6, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm3, ymm6, ymm3
	vpaddq	ymm3, ymm3, ymm4
	vpsrlq	ymm4, ymm7, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpsrlq	ymm5, ymm0, 32
	vpmuludq	ymm5, ymm7, ymm5
	vpaddq	ymm4, ymm5, ymm4
	vpsllq	ymm4, ymm4, 32
	vpmuludq	ymm0, ymm7, ymm0
	vpaddq	ymm0, ymm0, ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB0_874
# %bb.875:
	cmp	rsi, r10
	jne	.LBB0_488
	jmp	.LBB0_1177
.LBB0_876:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_495
# %bb.877:
	and	al, dil
	jne	.LBB0_495
# %bb.878:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_879:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmulps	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_879
# %bb.880:
	cmp	rsi, r10
	jne	.LBB0_495
	jmp	.LBB0_1177
.LBB0_881:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_502
# %bb.882:
	and	al, dil
	jne	.LBB0_502
# %bb.883:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_884:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm1, ymm1, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm2, ymm2, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm3, ymm3, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_884
# %bb.885:
	cmp	rsi, r10
	jne	.LBB0_502
	jmp	.LBB0_1177
.LBB0_886:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_509
# %bb.887:
	and	al, dil
	jne	.LBB0_509
# %bb.888:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_889:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm1, ymm1, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm2, ymm2, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm3, ymm3, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_889
# %bb.890:
	cmp	rsi, r10
	jne	.LBB0_509
	jmp	.LBB0_1177
.LBB0_897:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_522
# %bb.898:
	and	al, dil
	jne	.LBB0_522
# %bb.899:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_900:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rdi + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_900
# %bb.901:
	cmp	rsi, r10
	jne	.LBB0_522
	jmp	.LBB0_1177
.LBB0_902:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_529
# %bb.903:
	and	al, dil
	jne	.LBB0_529
# %bb.904:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_905:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rdi + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB0_905
# %bb.906:
	cmp	rsi, r10
	jne	.LBB0_529
	jmp	.LBB0_1177
.LBB0_907:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_536
# %bb.908:
	and	al, sil
	jne	.LBB0_536
# %bb.909:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1045
# %bb.910:
	xor	esi, esi
	jmp	.LBB0_1047
.LBB0_911:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_543
# %bb.912:
	and	al, sil
	jne	.LBB0_543
# %bb.913:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1051
# %bb.914:
	xor	esi, esi
	jmp	.LBB0_1053
.LBB0_915:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_550
# %bb.916:
	and	al, dil
	jne	.LBB0_550
# %bb.917:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_918:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm2, ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm3, ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_918
# %bb.919:
	cmp	rsi, r10
	jne	.LBB0_550
	jmp	.LBB0_1177
.LBB0_920:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_557
# %bb.921:
	and	al, dil
	jne	.LBB0_557
# %bb.922:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_923:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm2, ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm3, ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_923
# %bb.924:
	cmp	rsi, r10
	jne	.LBB0_557
	jmp	.LBB0_1177
.LBB0_931:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_570
# %bb.932:
	and	al, dil
	jne	.LBB0_570
# %bb.933:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_934:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_934
# %bb.935:
	cmp	rsi, r10
	jne	.LBB0_570
	jmp	.LBB0_1177
.LBB0_936:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_577
# %bb.937:
	and	al, dil
	jne	.LBB0_577
# %bb.938:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_939:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_939
# %bb.940:
	cmp	rsi, r10
	jne	.LBB0_577
	jmp	.LBB0_1177
.LBB0_941:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_584
# %bb.942:
	and	al, dil
	jne	.LBB0_584
# %bb.943:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_944:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_944
# %bb.945:
	cmp	rsi, r10
	jne	.LBB0_584
	jmp	.LBB0_1177
.LBB0_946:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	r11b
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, r11b
	jne	.LBB0_591
# %bb.947:
	and	al, dil
	jne	.LBB0_591
# %bb.948:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_949:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rdi + 96]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm2, ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm3, ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB0_949
# %bb.950:
	cmp	rsi, r10
	jne	.LBB0_591
	jmp	.LBB0_1177
.LBB0_951:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1077
# %bb.952:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_953:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_953
	jmp	.LBB0_1078
.LBB0_954:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1085
# %bb.955:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_956:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_956
	jmp	.LBB0_1086
.LBB0_957:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1093
# %bb.958:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB0_959:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_959
	jmp	.LBB0_1094
.LBB0_960:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1101
# %bb.961:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB0_962:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_962
	jmp	.LBB0_1102
.LBB0_963:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_964:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rsi]
	vpmovsxbd	ymm2, qword ptr [rdx + rsi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rsi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rsi + 24]
	vpsrad	ymm5, ymm1, 7
	vpsrad	ymm6, ymm2, 7
	vpsrad	ymm7, ymm3, 7
	vpsrad	ymm8, ymm4, 7
	vpaddd	ymm1, ymm5, ymm1
	vpaddd	ymm2, ymm6, ymm2
	vpaddd	ymm3, ymm7, ymm3
	vpaddd	ymm4, ymm8, ymm4
	vpxor	ymm1, ymm1, ymm5
	vpxor	ymm2, ymm2, ymm6
	vpxor	ymm3, ymm3, ymm7
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm1, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm5        # xmm1 = xmm1[0],xmm5[0],xmm1[1],xmm5[1]
	vextracti128	xmm5, ymm2, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm2, xmm2, xmm0
	vpunpckldq	xmm2, xmm2, xmm5        # xmm2 = xmm2[0],xmm5[0],xmm2[1],xmm5[1]
	vextracti128	xmm5, ymm3, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0],xmm3[1],xmm5[1]
	vextracti128	xmm5, ymm4, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB0_964
# %bb.965:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_966
.LBB0_970:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_971:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rsi]
	vpmovsxbd	ymm2, qword ptr [rdx + rsi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rsi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rsi + 24]
	vpsrad	ymm5, ymm1, 7
	vpsrad	ymm6, ymm2, 7
	vpsrad	ymm7, ymm3, 7
	vpsrad	ymm8, ymm4, 7
	vpaddd	ymm1, ymm5, ymm1
	vpaddd	ymm2, ymm6, ymm2
	vpaddd	ymm3, ymm7, ymm3
	vpaddd	ymm4, ymm8, ymm4
	vpxor	ymm1, ymm1, ymm5
	vpxor	ymm2, ymm2, ymm6
	vpxor	ymm3, ymm3, ymm7
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm1, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm5        # xmm1 = xmm1[0],xmm5[0],xmm1[1],xmm5[1]
	vextracti128	xmm5, ymm2, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm2, xmm2, xmm0
	vpunpckldq	xmm2, xmm2, xmm5        # xmm2 = xmm2[0],xmm5[0],xmm2[1],xmm5[1]
	vextracti128	xmm5, ymm3, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0],xmm3[1],xmm5[1]
	vextracti128	xmm5, ymm4, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB0_971
# %bb.972:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_973
.LBB0_977:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1109
# %bb.978:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_979:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [r8 + 8*rdi], ymm0
	vmovups	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 8*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_979
	jmp	.LBB0_1110
.LBB0_980:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1117
# %bb.981:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_982:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [r8 + 8*rdi], ymm0
	vmovups	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 8*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_982
	jmp	.LBB0_1118
.LBB0_983:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB0_1057
# %bb.984:
	xor	eax, eax
	jmp	.LBB0_1059
.LBB0_985:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1125
# %bb.986:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_2] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_987:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 48]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm1
	add	rsi, 32
	add	rdi, 2
	jne	.LBB0_987
	jmp	.LBB0_1126
.LBB0_988:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB0_1067
# %bb.989:
	xor	eax, eax
	jmp	.LBB0_1069
.LBB0_990:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1133
# %bb.991:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_2] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_992:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 48]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm1
	add	rsi, 32
	add	rdi, 2
	jne	.LBB0_992
	jmp	.LBB0_1134
.LBB0_993:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB0_994:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rsi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rsi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rsi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rsi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm4
	add	rsi, 16
	cmp	rcx, rsi
	jne	.LBB0_994
# %bb.995:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_996
.LBB0_1000:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1141
# %bb.1001:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB0_1002:                             # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1002
	jmp	.LBB0_1142
.LBB0_1003:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1004:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rsi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rsi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rsi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rsi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm4
	add	rsi, 16
	cmp	rcx, rsi
	jne	.LBB0_1004
# %bb.1005:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1006
.LBB0_1010:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1151
# %bb.1011:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB0_1012:                             # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1012
	jmp	.LBB0_1152
.LBB0_1013:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1161
# %bb.1014:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1015:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [r8 + rdi], ymm0
	vmovups	ymmword ptr [r8 + rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm3
	add	rdi, 256
	add	rsi, 2
	jne	.LBB0_1015
	jmp	.LBB0_1162
.LBB0_1016:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1169
# %bb.1017:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1018:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [r8 + rdi], ymm0
	vmovups	ymmword ptr [r8 + rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm3
	add	rdi, 256
	add	rsi, 2
	jne	.LBB0_1018
	jmp	.LBB0_1170
.LBB0_1019:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB0_1020:                             # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rsi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rsi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rsi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rsi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm3
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB0_1020
# %bb.1021:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1022
.LBB0_1026:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB0_1027:                             # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rsi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rsi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rsi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rsi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm3
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB0_1027
# %bb.1028:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1029
.LBB0_1033:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1034:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 64]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 96]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm1
	sub	rsi, -128
	add	rax, 4
	jne	.LBB0_1034
.LBB0_1035:
	test	r9, r9
	je	.LBB0_1038
# %bb.1036:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1037:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	inc	r9
	jne	.LBB0_1037
.LBB0_1038:
	cmp	rdi, r10
	jne	.LBB0_230
	jmp	.LBB0_1177
.LBB0_1039:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1040:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 64]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 96]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm1
	sub	rsi, -128
	add	rax, 4
	jne	.LBB0_1040
.LBB0_1041:
	test	r9, r9
	je	.LBB0_1044
# %bb.1042:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1043:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	inc	r9
	jne	.LBB0_1043
.LBB0_1044:
	cmp	rdi, r10
	jne	.LBB0_239
	jmp	.LBB0_1177
.LBB0_1045:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1046:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 64]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 96]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm1
	sub	rsi, -128
	add	rax, 4
	jne	.LBB0_1046
.LBB0_1047:
	test	r9, r9
	je	.LBB0_1050
# %bb.1048:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1049:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	inc	r9
	jne	.LBB0_1049
.LBB0_1050:
	cmp	rdi, r10
	jne	.LBB0_536
	jmp	.LBB0_1177
.LBB0_1051:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1052:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 64]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi + 96]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm1
	sub	rsi, -128
	add	rax, 4
	jne	.LBB0_1052
.LBB0_1053:
	test	r9, r9
	je	.LBB0_1056
# %bb.1054:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_4] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1055:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rsi]
	vmovdqu	ymm2, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm3, ymm1, ymm1        # ymm3 = ymm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpunpckhbw	ymm4, ymm2, ymm2        # ymm4 = ymm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm3, ymm4, ymm3
	vpand	ymm3, ymm3, ymm0
	vpunpcklbw	ymm1, ymm1, ymm1        # ymm1 = ymm1[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpunpcklbw	ymm2, ymm2, ymm2        # ymm2 = ymm2[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm1, ymm2, ymm1
	vpand	ymm1, ymm1, ymm0
	vpackuswb	ymm1, ymm1, ymm3
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	inc	r9
	jne	.LBB0_1055
.LBB0_1056:
	cmp	rdi, r10
	jne	.LBB0_543
	jmp	.LBB0_1177
.LBB0_1057:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1058:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [r8 + 2*rax], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [r8 + 2*rax + 64], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [r8 + 2*rax + 128], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 160], ymm1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovdqu	ymmword ptr [r8 + 2*rax + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1058
.LBB0_1059:
	test	rsi, rsi
	je	.LBB0_1062
# %bb.1060:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB0_1061:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB0_1061
.LBB0_1062:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1063
.LBB0_1067:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1068:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [r8 + 2*rax], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [r8 + 2*rax + 64], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [r8 + 2*rax + 128], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 160], ymm1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovdqu	ymmword ptr [r8 + 2*rax + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1068
.LBB0_1069:
	test	rsi, rsi
	je	.LBB0_1072
# %bb.1070:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB0_1071:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB0_1071
.LBB0_1072:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1073
.LBB0_1077:
	xor	edi, edi
.LBB0_1078:
	test	r9b, 1
	je	.LBB0_1080
# %bb.1079:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB0_1080:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1081
.LBB0_1085:
	xor	edi, edi
.LBB0_1086:
	test	r9b, 1
	je	.LBB0_1088
# %bb.1087:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB0_1088:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1089
.LBB0_1093:
	xor	edi, edi
.LBB0_1094:
	test	r9b, 1
	je	.LBB0_1096
# %bb.1095:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1096:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1097
.LBB0_1101:
	xor	edi, edi
.LBB0_1102:
	test	r9b, 1
	je	.LBB0_1104
# %bb.1103:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1104:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1105
.LBB0_1109:
	xor	edi, edi
.LBB0_1110:
	test	r9b, 1
	je	.LBB0_1112
# %bb.1111:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB0_1112:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1113
.LBB0_1117:
	xor	edi, edi
.LBB0_1118:
	test	r9b, 1
	je	.LBB0_1120
# %bb.1119:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB0_1120:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1121
.LBB0_1125:
	xor	esi, esi
.LBB0_1126:
	test	r9b, 1
	je	.LBB0_1128
# %bb.1127:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_2] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB0_1128:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1129
.LBB0_1133:
	xor	esi, esi
.LBB0_1134:
	test	r9b, 1
	je	.LBB0_1136
# %bb.1135:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_2] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB0_1136:
	cmp	rcx, rax
	je	.LBB0_1177
	jmp	.LBB0_1137
.LBB0_1141:
	xor	edi, edi
.LBB0_1142:
	test	r9b, 1
	je	.LBB0_1144
# %bb.1143:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1144:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1145
.LBB0_1151:
	xor	edi, edi
.LBB0_1152:
	test	r9b, 1
	je	.LBB0_1154
# %bb.1153:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1154:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1155
.LBB0_1161:
	xor	edi, edi
.LBB0_1162:
	test	r9b, 1
	je	.LBB0_1164
# %bb.1163:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB0_1164:
	cmp	rcx, r10
	je	.LBB0_1177
	jmp	.LBB0_1165
.LBB0_1169:
	xor	edi, edi
.LBB0_1170:
	test	r9b, 1
	je	.LBB0_1172
# %bb.1171:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB0_1172:
	cmp	rcx, r10
	jne	.LBB0_1173
.LBB0_1177:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end0:
	.size	arithmetic_avx2, .Lfunc_end0-arithmetic_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_arr_scalar_avx2
.LCPI1_0:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI1_1:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI1_2:
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
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
.LCPI1_4:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI1_3:
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
	.globl	arithmetic_arr_scalar_avx2
	.p2align	4, 0x90
	.type	arithmetic_arr_scalar_avx2,@function
arithmetic_arr_scalar_avx2:             # @arithmetic_arr_scalar_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 4
	jg	.LBB1_12
# %bb.1:
	cmp	sil, 1
	jg	.LBB1_23
# %bb.2:
	test	sil, sil
	je	.LBB1_43
# %bb.3:
	cmp	sil, 1
	jne	.LBB1_1461
# %bb.4:
	cmp	edi, 6
	jg	.LBB1_75
# %bb.5:
	cmp	edi, 3
	jle	.LBB1_131
# %bb.6:
	cmp	edi, 4
	je	.LBB1_211
# %bb.7:
	cmp	edi, 5
	je	.LBB1_214
# %bb.8:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.9:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.10:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_11
# %bb.355:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_595
# %bb.356:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_595
.LBB1_11:
	xor	esi, esi
.LBB1_917:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_919
.LBB1_918:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_918
.LBB1_919:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_920:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_920
	jmp	.LBB1_1461
.LBB1_12:
	cmp	sil, 6
	jg	.LBB1_33
# %bb.13:
	cmp	sil, 5
	je	.LBB1_51
# %bb.14:
	cmp	sil, 6
	jne	.LBB1_1461
# %bb.15:
	cmp	edi, 6
	jg	.LBB1_82
# %bb.16:
	cmp	edi, 3
	jle	.LBB1_136
# %bb.17:
	cmp	edi, 4
	je	.LBB1_217
# %bb.18:
	cmp	edi, 5
	je	.LBB1_220
# %bb.19:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.20:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.21:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_22
# %bb.358:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_598
# %bb.359:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_598
.LBB1_22:
	xor	esi, esi
.LBB1_925:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_927
.LBB1_926:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_926
.LBB1_927:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_928:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_928
	jmp	.LBB1_1461
.LBB1_23:
	cmp	sil, 2
	je	.LBB1_59
# %bb.24:
	cmp	sil, 4
	jne	.LBB1_1461
# %bb.25:
	cmp	edi, 6
	jg	.LBB1_89
# %bb.26:
	cmp	edi, 3
	jle	.LBB1_141
# %bb.27:
	cmp	edi, 4
	je	.LBB1_223
# %bb.28:
	cmp	edi, 5
	je	.LBB1_226
# %bb.29:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.30:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.31:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_32
# %bb.361:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_601
# %bb.362:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_601
.LBB1_32:
	xor	ecx, ecx
.LBB1_933:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_935
.LBB1_934:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_934
.LBB1_935:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_936:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	mov	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	mov	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_936
	jmp	.LBB1_1461
.LBB1_33:
	cmp	sil, 7
	je	.LBB1_67
# %bb.34:
	cmp	sil, 9
	jne	.LBB1_1461
# %bb.35:
	cmp	edi, 6
	jg	.LBB1_96
# %bb.36:
	cmp	edi, 3
	jle	.LBB1_146
# %bb.37:
	cmp	edi, 4
	je	.LBB1_229
# %bb.38:
	cmp	edi, 5
	je	.LBB1_232
# %bb.39:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.40:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.41:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_42
# %bb.364:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_604
# %bb.365:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_604
.LBB1_42:
	xor	ecx, ecx
.LBB1_941:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_943
.LBB1_942:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_942
.LBB1_943:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_944:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	mov	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	mov	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_944
	jmp	.LBB1_1461
.LBB1_43:
	cmp	edi, 6
	jg	.LBB1_103
# %bb.44:
	cmp	edi, 3
	jle	.LBB1_151
# %bb.45:
	cmp	edi, 4
	je	.LBB1_235
# %bb.46:
	cmp	edi, 5
	je	.LBB1_238
# %bb.47:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.48:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.49:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_50
# %bb.367:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_607
# %bb.368:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_607
.LBB1_50:
	xor	esi, esi
.LBB1_949:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_951
.LBB1_950:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_950
.LBB1_951:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_952:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_952
	jmp	.LBB1_1461
.LBB1_51:
	cmp	edi, 6
	jg	.LBB1_110
# %bb.52:
	cmp	edi, 3
	jle	.LBB1_156
# %bb.53:
	cmp	edi, 4
	je	.LBB1_241
# %bb.54:
	cmp	edi, 5
	je	.LBB1_244
# %bb.55:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.56:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.57:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_58
# %bb.370:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_610
# %bb.371:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_610
.LBB1_58:
	xor	esi, esi
.LBB1_957:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_959
.LBB1_958:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_958
.LBB1_959:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_960:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_960
	jmp	.LBB1_1461
.LBB1_59:
	cmp	edi, 6
	jg	.LBB1_117
# %bb.60:
	cmp	edi, 3
	jle	.LBB1_161
# %bb.61:
	cmp	edi, 4
	je	.LBB1_247
# %bb.62:
	cmp	edi, 5
	je	.LBB1_250
# %bb.63:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.64:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.65:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_66
# %bb.373:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_613
# %bb.374:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_613
.LBB1_66:
	xor	esi, esi
.LBB1_965:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_967
.LBB1_966:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_966
.LBB1_967:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_968:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_968
	jmp	.LBB1_1461
.LBB1_67:
	cmp	edi, 6
	jg	.LBB1_124
# %bb.68:
	cmp	edi, 3
	jle	.LBB1_166
# %bb.69:
	cmp	edi, 4
	je	.LBB1_253
# %bb.70:
	cmp	edi, 5
	je	.LBB1_256
# %bb.71:
	cmp	edi, 6
	jne	.LBB1_1461
# %bb.72:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.73:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_74
# %bb.376:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_616
# %bb.377:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_616
.LBB1_74:
	xor	esi, esi
.LBB1_973:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_975
.LBB1_974:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_974
.LBB1_975:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_976:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_976
	jmp	.LBB1_1461
.LBB1_75:
	cmp	edi, 8
	jle	.LBB1_171
# %bb.76:
	cmp	edi, 9
	je	.LBB1_259
# %bb.77:
	cmp	edi, 11
	je	.LBB1_262
# %bb.78:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.79:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.80:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_81
# %bb.379:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_619
# %bb.380:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_619
.LBB1_81:
	xor	ecx, ecx
.LBB1_981:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_983
.LBB1_982:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_982
.LBB1_983:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_984:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_984
	jmp	.LBB1_1461
.LBB1_82:
	cmp	edi, 8
	jle	.LBB1_176
# %bb.83:
	cmp	edi, 9
	je	.LBB1_265
# %bb.84:
	cmp	edi, 11
	je	.LBB1_268
# %bb.85:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.86:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.87:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_88
# %bb.382:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_622
# %bb.383:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_622
.LBB1_88:
	xor	ecx, ecx
.LBB1_989:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_991
.LBB1_990:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_990
.LBB1_991:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_992:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_992
	jmp	.LBB1_1461
.LBB1_89:
	cmp	edi, 8
	jle	.LBB1_181
# %bb.90:
	cmp	edi, 9
	je	.LBB1_271
# %bb.91:
	cmp	edi, 11
	je	.LBB1_274
# %bb.92:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.93:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.94:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_95
# %bb.385:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_625
# %bb.386:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_625
.LBB1_95:
	xor	ecx, ecx
.LBB1_997:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_999
.LBB1_998:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_998
.LBB1_999:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1000:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1000
	jmp	.LBB1_1461
.LBB1_96:
	cmp	edi, 8
	jle	.LBB1_186
# %bb.97:
	cmp	edi, 9
	je	.LBB1_277
# %bb.98:
	cmp	edi, 11
	je	.LBB1_280
# %bb.99:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.100:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.101:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_102
# %bb.388:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_628
# %bb.389:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_628
.LBB1_102:
	xor	ecx, ecx
.LBB1_1005:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_1007
.LBB1_1006:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_1006
.LBB1_1007:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1008:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1008
	jmp	.LBB1_1461
.LBB1_103:
	cmp	edi, 8
	jle	.LBB1_191
# %bb.104:
	cmp	edi, 9
	je	.LBB1_283
# %bb.105:
	cmp	edi, 11
	je	.LBB1_286
# %bb.106:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.107:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.108:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_109
# %bb.391:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_631
# %bb.392:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_631
.LBB1_109:
	xor	ecx, ecx
.LBB1_1013:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1015
.LBB1_1014:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1014
.LBB1_1015:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1016:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 8]
	vmovsd	qword ptr [r8 + 8*rcx + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 16]
	vmovsd	qword ptr [r8 + 8*rcx + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 24]
	vmovsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1016
	jmp	.LBB1_1461
.LBB1_110:
	cmp	edi, 8
	jle	.LBB1_196
# %bb.111:
	cmp	edi, 9
	je	.LBB1_289
# %bb.112:
	cmp	edi, 11
	je	.LBB1_292
# %bb.113:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.114:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.115:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_116
# %bb.394:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_634
# %bb.395:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_634
.LBB1_116:
	xor	ecx, ecx
.LBB1_1021:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1023
.LBB1_1022:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1022
.LBB1_1023:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1024:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 8]
	vmovsd	qword ptr [r8 + 8*rcx + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 16]
	vmovsd	qword ptr [r8 + 8*rcx + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 24]
	vmovsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1024
	jmp	.LBB1_1461
.LBB1_117:
	cmp	edi, 8
	jle	.LBB1_201
# %bb.118:
	cmp	edi, 9
	je	.LBB1_295
# %bb.119:
	cmp	edi, 11
	je	.LBB1_298
# %bb.120:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.121:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.122:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_123
# %bb.397:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_637
# %bb.398:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_637
.LBB1_123:
	xor	ecx, ecx
.LBB1_1029:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1031
.LBB1_1030:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1030
.LBB1_1031:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1032:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 8]
	vmovsd	qword ptr [r8 + 8*rcx + 8], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 16]
	vmovsd	qword ptr [r8 + 8*rcx + 16], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 24]
	vmovsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1032
	jmp	.LBB1_1461
.LBB1_124:
	cmp	edi, 8
	jle	.LBB1_206
# %bb.125:
	cmp	edi, 9
	je	.LBB1_301
# %bb.126:
	cmp	edi, 11
	je	.LBB1_304
# %bb.127:
	cmp	edi, 12
	jne	.LBB1_1461
# %bb.128:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.129:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_130
# %bb.400:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_640
# %bb.401:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_640
.LBB1_130:
	xor	ecx, ecx
.LBB1_1037:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1039
.LBB1_1038:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1038
.LBB1_1039:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1040:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 8]
	vmovsd	qword ptr [r8 + 8*rcx + 8], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 16]
	vmovsd	qword ptr [r8 + 8*rcx + 16], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx + 24]
	vmovsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1040
	jmp	.LBB1_1461
.LBB1_131:
	cmp	edi, 2
	je	.LBB1_307
# %bb.132:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.133:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.134:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_135
# %bb.403:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_643
# %bb.404:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_643
.LBB1_135:
	xor	esi, esi
.LBB1_1045:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1047
.LBB1_1046:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1046
.LBB1_1047:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1048:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1048
	jmp	.LBB1_1461
.LBB1_136:
	cmp	edi, 2
	je	.LBB1_310
# %bb.137:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.138:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.139:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_140
# %bb.406:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_646
# %bb.407:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_646
.LBB1_140:
	xor	esi, esi
.LBB1_1053:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1055
.LBB1_1054:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1054
.LBB1_1055:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1056:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1056
	jmp	.LBB1_1461
.LBB1_141:
	cmp	edi, 2
	je	.LBB1_313
# %bb.142:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.143:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.144:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_145
# %bb.409:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB1_649
# %bb.410:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB1_649
.LBB1_145:
	xor	ecx, ecx
.LBB1_652:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_654
# %bb.653:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_654:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_655:                              # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdx + rcx]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx], sil
	movsx	esi, byte ptr [rdx + rcx + 1]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx + 1], sil
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_655
	jmp	.LBB1_1461
.LBB1_146:
	cmp	edi, 2
	je	.LBB1_316
# %bb.147:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.148:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.149:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_150
# %bb.412:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB1_656
# %bb.413:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB1_656
.LBB1_150:
	xor	ecx, ecx
.LBB1_659:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_661
# %bb.660:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_661:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_662:                              # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdx + rcx]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx], sil
	movsx	esi, byte ptr [rdx + rcx + 1]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx + 1], sil
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_662
	jmp	.LBB1_1461
.LBB1_151:
	cmp	edi, 2
	je	.LBB1_319
# %bb.152:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.153:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.154:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_155
# %bb.415:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_663
# %bb.416:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_663
.LBB1_155:
	xor	esi, esi
.LBB1_1061:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1063
.LBB1_1062:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1062
.LBB1_1063:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1064:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	add	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	add	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	add	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1064
	jmp	.LBB1_1461
.LBB1_156:
	cmp	edi, 2
	je	.LBB1_322
# %bb.157:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.158:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.159:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_160
# %bb.418:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_666
# %bb.419:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_666
.LBB1_160:
	xor	esi, esi
.LBB1_1069:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1071
.LBB1_1070:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1070
.LBB1_1071:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1072:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	add	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	add	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	add	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1072
	jmp	.LBB1_1461
.LBB1_161:
	cmp	edi, 2
	je	.LBB1_325
# %bb.162:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.163:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.164:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_165
# %bb.421:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_669
# %bb.422:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_669
.LBB1_165:
	xor	edi, edi
.LBB1_859:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_861
.LBB1_860:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_860
.LBB1_861:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_862:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rdx + rdi + 1]
	mul	cl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rdx + rdi + 2]
	mul	cl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rdx + rdi + 3]
	mul	cl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB1_862
	jmp	.LBB1_1461
.LBB1_166:
	cmp	edi, 2
	je	.LBB1_328
# %bb.167:
	cmp	edi, 3
	jne	.LBB1_1461
# %bb.168:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.169:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_170
# %bb.424:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_671
# %bb.425:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_671
.LBB1_170:
	xor	edi, edi
.LBB1_869:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_871
.LBB1_870:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_870
.LBB1_871:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_872:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rdx + rdi + 1]
	mul	cl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rdx + rdi + 2]
	mul	cl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rdx + rdi + 3]
	mul	cl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB1_872
	jmp	.LBB1_1461
.LBB1_171:
	cmp	edi, 7
	je	.LBB1_331
# %bb.172:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.173:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.174:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_175
# %bb.427:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_673
# %bb.428:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_673
.LBB1_175:
	xor	esi, esi
.LBB1_1077:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1079
.LBB1_1078:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1078
.LBB1_1079:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1080:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1080
	jmp	.LBB1_1461
.LBB1_176:
	cmp	edi, 7
	je	.LBB1_334
# %bb.177:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.178:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.179:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_180
# %bb.430:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_676
# %bb.431:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_676
.LBB1_180:
	xor	esi, esi
.LBB1_1085:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1087
.LBB1_1086:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1086
.LBB1_1087:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1088:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1088
	jmp	.LBB1_1461
.LBB1_181:
	cmp	edi, 7
	je	.LBB1_337
# %bb.182:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.183:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.184:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_185
# %bb.433:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_679
# %bb.434:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_679
.LBB1_185:
	xor	ecx, ecx
.LBB1_1093:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1095
.LBB1_1094:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1094
.LBB1_1095:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1096:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1096
	jmp	.LBB1_1461
.LBB1_186:
	cmp	edi, 7
	je	.LBB1_340
# %bb.187:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.188:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.189:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_190
# %bb.436:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_682
# %bb.437:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_682
.LBB1_190:
	xor	ecx, ecx
.LBB1_1101:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1103
.LBB1_1102:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1102
.LBB1_1103:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1104:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1104
	jmp	.LBB1_1461
.LBB1_191:
	cmp	edi, 7
	je	.LBB1_343
# %bb.192:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.193:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.194:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_195
# %bb.439:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_685
# %bb.440:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_685
.LBB1_195:
	xor	esi, esi
.LBB1_1109:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1111
.LBB1_1110:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1110
.LBB1_1111:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1112:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1112
	jmp	.LBB1_1461
.LBB1_196:
	cmp	edi, 7
	je	.LBB1_346
# %bb.197:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.198:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.199:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_200
# %bb.442:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_688
# %bb.443:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_688
.LBB1_200:
	xor	esi, esi
.LBB1_1117:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1119
.LBB1_1118:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1118
.LBB1_1119:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1120:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1120
	jmp	.LBB1_1461
.LBB1_201:
	cmp	edi, 7
	je	.LBB1_349
# %bb.202:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.203:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.204:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_205
# %bb.445:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_691
# %bb.446:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_691
.LBB1_205:
	xor	esi, esi
.LBB1_1125:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1127
.LBB1_1126:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1126
.LBB1_1127:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1128:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1128
	jmp	.LBB1_1461
.LBB1_206:
	cmp	edi, 7
	je	.LBB1_352
# %bb.207:
	cmp	edi, 8
	jne	.LBB1_1461
# %bb.208:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.209:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_210
# %bb.448:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_694
# %bb.449:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_694
.LBB1_210:
	xor	esi, esi
.LBB1_1133:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1135
.LBB1_1134:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1134
.LBB1_1135:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1136:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1136
	jmp	.LBB1_1461
.LBB1_211:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.212:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_213
# %bb.451:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_697
# %bb.452:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_697
.LBB1_213:
	xor	esi, esi
.LBB1_1141:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1143
.LBB1_1142:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1142
.LBB1_1143:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1144:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1144
	jmp	.LBB1_1461
.LBB1_214:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.215:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_216
# %bb.454:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_700
# %bb.455:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_700
.LBB1_216:
	xor	esi, esi
.LBB1_1149:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1151
.LBB1_1150:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1150
.LBB1_1151:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1152:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1152
	jmp	.LBB1_1461
.LBB1_217:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.218:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_219
# %bb.457:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_703
# %bb.458:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_703
.LBB1_219:
	xor	esi, esi
.LBB1_1157:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1159
.LBB1_1158:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1158
.LBB1_1159:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1160:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1160
	jmp	.LBB1_1461
.LBB1_220:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.221:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_222
# %bb.460:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_706
# %bb.461:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_706
.LBB1_222:
	xor	esi, esi
.LBB1_1165:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1167
.LBB1_1166:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1166
.LBB1_1167:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1168:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1168
	jmp	.LBB1_1461
.LBB1_223:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.224:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_225
# %bb.463:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_709
# %bb.464:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_709
.LBB1_225:
	xor	ecx, ecx
.LBB1_879:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_881
.LBB1_880:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_880
.LBB1_881:
	cmp	rax, 3
	jb	.LBB1_1461
.LBB1_882:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], ax
	movzx	eax, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], ax
	movzx	eax, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], ax
	movzx	eax, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_882
	jmp	.LBB1_1461
.LBB1_226:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.227:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_228
# %bb.466:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_711
# %bb.467:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_711
.LBB1_228:
	xor	ecx, ecx
.LBB1_1173:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1175
# %bb.1174:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1175:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_1176:                             # =>This Inner Loop Header: Depth=1
	movsx	esi, word ptr [rdx + 2*rcx]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx], si
	movsx	esi, word ptr [rdx + 2*rcx + 2]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx + 2], si
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_1176
	jmp	.LBB1_1461
.LBB1_229:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.230:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_231
# %bb.469:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_714
# %bb.470:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_714
.LBB1_231:
	xor	ecx, ecx
.LBB1_889:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_891
.LBB1_890:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_890
.LBB1_891:
	cmp	rax, 3
	jb	.LBB1_1461
.LBB1_892:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], ax
	movzx	eax, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], ax
	movzx	eax, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], ax
	movzx	eax, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_892
	jmp	.LBB1_1461
.LBB1_232:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.233:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_234
# %bb.472:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_716
# %bb.473:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_716
.LBB1_234:
	xor	ecx, ecx
.LBB1_1181:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1183
# %bb.1182:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1183:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_1184:                             # =>This Inner Loop Header: Depth=1
	movsx	esi, word ptr [rdx + 2*rcx]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx], si
	movsx	esi, word ptr [rdx + 2*rcx + 2]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx + 2], si
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_1184
	jmp	.LBB1_1461
.LBB1_235:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.236:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_237
# %bb.475:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_719
# %bb.476:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_719
.LBB1_237:
	xor	esi, esi
.LBB1_1189:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1191
.LBB1_1190:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1190
.LBB1_1191:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1192:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1192
	jmp	.LBB1_1461
.LBB1_238:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.239:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_240
# %bb.478:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_722
# %bb.479:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_722
.LBB1_240:
	xor	esi, esi
.LBB1_1197:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1199
.LBB1_1198:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1198
.LBB1_1199:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1200:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1200
	jmp	.LBB1_1461
.LBB1_241:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.242:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_243
# %bb.481:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_725
# %bb.482:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_725
.LBB1_243:
	xor	esi, esi
.LBB1_1205:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1207
.LBB1_1206:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1206
.LBB1_1207:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1208:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1208
	jmp	.LBB1_1461
.LBB1_244:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.245:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_246
# %bb.484:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_728
# %bb.485:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_728
.LBB1_246:
	xor	esi, esi
.LBB1_1213:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1215
.LBB1_1214:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1214
.LBB1_1215:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1216:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1216
	jmp	.LBB1_1461
.LBB1_247:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.248:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_249
# %bb.487:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_731
# %bb.488:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_731
.LBB1_249:
	xor	esi, esi
.LBB1_1221:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1223
.LBB1_1222:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1222
.LBB1_1223:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1224:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1224
	jmp	.LBB1_1461
.LBB1_250:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.251:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_252
# %bb.490:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_734
# %bb.491:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_734
.LBB1_252:
	xor	esi, esi
.LBB1_1229:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1231
.LBB1_1230:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1230
.LBB1_1231:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1232:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1232
	jmp	.LBB1_1461
.LBB1_253:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.254:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_255
# %bb.493:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_737
# %bb.494:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_737
.LBB1_255:
	xor	esi, esi
.LBB1_1237:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1239
.LBB1_1238:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1238
.LBB1_1239:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1240:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1240
	jmp	.LBB1_1461
.LBB1_256:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.257:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_258
# %bb.496:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_740
# %bb.497:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_740
.LBB1_258:
	xor	esi, esi
.LBB1_1245:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1247
.LBB1_1246:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1246
.LBB1_1247:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1248:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 2]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 2], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 4]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 4], cx
	movzx	ecx, word ptr [rdx + 2*rsi + 6]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi + 6], cx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1248
	jmp	.LBB1_1461
.LBB1_259:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.260:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_261
# %bb.499:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_743
# %bb.500:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_743
.LBB1_261:
	xor	esi, esi
.LBB1_1253:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1255
.LBB1_1254:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1254
.LBB1_1255:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1256:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1256
	jmp	.LBB1_1461
.LBB1_262:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.263:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_264
# %bb.502:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_746
# %bb.503:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_746
.LBB1_264:
	xor	ecx, ecx
.LBB1_1261:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1263
.LBB1_1262:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1262
.LBB1_1263:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1264:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1264
	jmp	.LBB1_1461
.LBB1_265:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.266:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_267
# %bb.505:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_749
# %bb.506:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_749
.LBB1_267:
	xor	esi, esi
.LBB1_1269:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1271
.LBB1_1270:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1270
.LBB1_1271:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1272:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1272
	jmp	.LBB1_1461
.LBB1_268:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.269:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_270
# %bb.508:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_752
# %bb.509:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_752
.LBB1_270:
	xor	ecx, ecx
.LBB1_1277:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1279
.LBB1_1278:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1278
.LBB1_1279:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1280:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1280
	jmp	.LBB1_1461
.LBB1_271:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.272:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_273
# %bb.511:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_755
# %bb.512:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_755
.LBB1_273:
	xor	ecx, ecx
.LBB1_758:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_760
# %bb.759:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_760:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_761:                              # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rdi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_761
	jmp	.LBB1_1461
.LBB1_274:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.275:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_276
# %bb.514:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_762
# %bb.515:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_762
.LBB1_276:
	xor	ecx, ecx
.LBB1_1285:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1288
# %bb.1286:
	mov	esi, 2147483647
.LBB1_1287:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1287
.LBB1_1288:
	cmp	r9, 3
	jb	.LBB1_1461
# %bb.1289:
	mov	esi, 2147483647
.LBB1_1290:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1290
	jmp	.LBB1_1461
.LBB1_277:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.278:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_279
# %bb.517:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_765
# %bb.518:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_765
.LBB1_279:
	xor	ecx, ecx
.LBB1_768:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_770
# %bb.769:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_770:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_771:                              # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rdi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_771
	jmp	.LBB1_1461
.LBB1_280:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.281:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_282
# %bb.520:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_772
# %bb.521:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_772
.LBB1_282:
	xor	ecx, ecx
.LBB1_1295:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1298
# %bb.1296:
	mov	esi, 2147483647
.LBB1_1297:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1297
.LBB1_1298:
	cmp	r9, 3
	jb	.LBB1_1461
# %bb.1299:
	mov	esi, 2147483647
.LBB1_1300:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1300
	jmp	.LBB1_1461
.LBB1_283:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.284:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_285
# %bb.523:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_775
# %bb.524:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_775
.LBB1_285:
	xor	esi, esi
.LBB1_1305:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1307
.LBB1_1306:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1306
.LBB1_1307:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1308:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1308
	jmp	.LBB1_1461
.LBB1_286:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.287:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_288
# %bb.526:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_778
# %bb.527:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_778
.LBB1_288:
	xor	ecx, ecx
.LBB1_1313:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1315
.LBB1_1314:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1314
.LBB1_1315:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1316:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 4]
	vmovss	dword ptr [r8 + 4*rcx + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 8]
	vmovss	dword ptr [r8 + 4*rcx + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 12]
	vmovss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1316
	jmp	.LBB1_1461
.LBB1_289:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.290:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_291
# %bb.529:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_781
# %bb.530:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_781
.LBB1_291:
	xor	esi, esi
.LBB1_1321:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1323
.LBB1_1322:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1322
.LBB1_1323:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1324:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1324
	jmp	.LBB1_1461
.LBB1_292:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.293:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_294
# %bb.532:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_784
# %bb.533:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_784
.LBB1_294:
	xor	ecx, ecx
.LBB1_1329:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1331
.LBB1_1330:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1330
.LBB1_1331:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1332:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 4]
	vmovss	dword ptr [r8 + 4*rcx + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 8]
	vmovss	dword ptr [r8 + 4*rcx + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 12]
	vmovss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1332
	jmp	.LBB1_1461
.LBB1_295:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.296:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_297
# %bb.535:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_787
# %bb.536:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_787
.LBB1_297:
	xor	esi, esi
.LBB1_1337:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1339
.LBB1_1338:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1338
.LBB1_1339:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1340:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1340
	jmp	.LBB1_1461
.LBB1_298:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.299:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_300
# %bb.538:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_790
# %bb.539:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_790
.LBB1_300:
	xor	ecx, ecx
.LBB1_1345:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1347
.LBB1_1346:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1346
.LBB1_1347:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1348:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 4]
	vmovss	dword ptr [r8 + 4*rcx + 4], xmm1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 8]
	vmovss	dword ptr [r8 + 4*rcx + 8], xmm1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 12]
	vmovss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1348
	jmp	.LBB1_1461
.LBB1_301:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.302:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_303
# %bb.541:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_793
# %bb.542:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_793
.LBB1_303:
	xor	esi, esi
.LBB1_1353:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1355
.LBB1_1354:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1354
.LBB1_1355:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1356:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rsi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rcx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1356
	jmp	.LBB1_1461
.LBB1_304:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.305:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_306
# %bb.544:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_796
# %bb.545:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_796
.LBB1_306:
	xor	ecx, ecx
.LBB1_1361:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1363
.LBB1_1362:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1362
.LBB1_1363:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1364:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 4]
	vmovss	dword ptr [r8 + 4*rcx + 4], xmm1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 8]
	vmovss	dword ptr [r8 + 4*rcx + 8], xmm1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx + 12]
	vmovss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1364
	jmp	.LBB1_1461
.LBB1_307:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.308:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_309
# %bb.547:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_799
# %bb.548:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_799
.LBB1_309:
	xor	esi, esi
.LBB1_1369:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1371
.LBB1_1370:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1370
.LBB1_1371:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1372:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1372
	jmp	.LBB1_1461
.LBB1_310:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.311:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_312
# %bb.550:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_802
# %bb.551:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_802
.LBB1_312:
	xor	esi, esi
.LBB1_1377:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1379
.LBB1_1378:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1378
.LBB1_1379:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1380:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	sub	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1380
	jmp	.LBB1_1461
.LBB1_313:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.314:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_315
# %bb.553:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_805
# %bb.554:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_805
.LBB1_315:
	xor	ecx, ecx
.LBB1_1385:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1387
.LBB1_1386:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1386
.LBB1_1387:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1388:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	movzx	eax, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	movzx	eax, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	mov	byte ptr [r8 + rcx + 3], al
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1388
	jmp	.LBB1_1461
.LBB1_316:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.317:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_318
# %bb.556:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_808
# %bb.557:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_808
.LBB1_318:
	xor	ecx, ecx
.LBB1_1393:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1395
.LBB1_1394:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1394
.LBB1_1395:
	cmp	rsi, 3
	jb	.LBB1_1461
.LBB1_1396:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	movzx	eax, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	movzx	eax, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	mov	byte ptr [r8 + rcx + 3], al
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1396
	jmp	.LBB1_1461
.LBB1_319:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.320:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_321
# %bb.559:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_811
# %bb.560:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_811
.LBB1_321:
	xor	esi, esi
.LBB1_1401:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1403
.LBB1_1402:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1402
.LBB1_1403:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1404:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	add	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	add	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	add	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1404
	jmp	.LBB1_1461
.LBB1_322:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.323:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_324
# %bb.562:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_814
# %bb.563:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_814
.LBB1_324:
	xor	esi, esi
.LBB1_1409:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1411
.LBB1_1410:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1410
.LBB1_1411:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1412:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	movzx	ecx, byte ptr [rdx + rsi + 1]
	add	cl, al
	mov	byte ptr [r8 + rsi + 1], cl
	movzx	ecx, byte ptr [rdx + rsi + 2]
	add	cl, al
	mov	byte ptr [r8 + rsi + 2], cl
	movzx	ecx, byte ptr [rdx + rsi + 3]
	add	cl, al
	mov	byte ptr [r8 + rsi + 3], cl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1412
	jmp	.LBB1_1461
.LBB1_325:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.326:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_327
# %bb.565:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_817
# %bb.566:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_817
.LBB1_327:
	xor	edi, edi
.LBB1_899:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_901
.LBB1_900:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_900
.LBB1_901:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_902:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rdx + rdi + 1]
	mul	cl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rdx + rdi + 2]
	mul	cl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rdx + rdi + 3]
	mul	cl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB1_902
	jmp	.LBB1_1461
.LBB1_328:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.329:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_330
# %bb.568:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_819
# %bb.569:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_819
.LBB1_330:
	xor	edi, edi
.LBB1_909:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_911
.LBB1_910:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_910
.LBB1_911:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_912:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rdx + rdi + 1]
	mul	cl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rdx + rdi + 2]
	mul	cl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rdx + rdi + 3]
	mul	cl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB1_912
	jmp	.LBB1_1461
.LBB1_331:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.332:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_333
# %bb.571:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_821
# %bb.572:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_821
.LBB1_333:
	xor	esi, esi
.LBB1_1417:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1419
.LBB1_1418:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1418
.LBB1_1419:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1420:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1420
	jmp	.LBB1_1461
.LBB1_334:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.335:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_336
# %bb.574:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_824
# %bb.575:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_824
.LBB1_336:
	xor	esi, esi
.LBB1_1425:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1427
.LBB1_1426:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1426
.LBB1_1427:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1428:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1428
	jmp	.LBB1_1461
.LBB1_337:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.338:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_339
# %bb.577:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_827
# %bb.578:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_827
.LBB1_339:
	xor	ecx, ecx
.LBB1_830:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_832
# %bb.831:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_832:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_833:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx], edi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx + 4], edi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_833
	jmp	.LBB1_1461
.LBB1_340:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.341:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_342
# %bb.580:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_834
# %bb.581:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_834
.LBB1_342:
	xor	ecx, ecx
.LBB1_837:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_839
# %bb.838:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_839:
	add	rsi, rax
	je	.LBB1_1461
.LBB1_840:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx], edi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx + 4], edi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB1_840
	jmp	.LBB1_1461
.LBB1_343:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.344:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_345
# %bb.583:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_841
# %bb.584:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_841
.LBB1_345:
	xor	esi, esi
.LBB1_1433:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1435
.LBB1_1434:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1434
.LBB1_1435:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1436:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1436
	jmp	.LBB1_1461
.LBB1_346:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.347:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_348
# %bb.586:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_844
# %bb.587:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_844
.LBB1_348:
	xor	esi, esi
.LBB1_1441:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1443
.LBB1_1442:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1442
.LBB1_1443:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1444:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1444
	jmp	.LBB1_1461
.LBB1_349:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.350:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_351
# %bb.589:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_847
# %bb.590:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_847
.LBB1_351:
	xor	esi, esi
.LBB1_1449:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1451
.LBB1_1450:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1450
.LBB1_1451:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1452:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1452
	jmp	.LBB1_1461
.LBB1_352:
	test	r9d, r9d
	jle	.LBB1_1461
# %bb.353:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_354
# %bb.592:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_850
# %bb.593:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_850
.LBB1_354:
	xor	esi, esi
.LBB1_1457:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1459
.LBB1_1458:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1458
.LBB1_1459:
	cmp	r9, 3
	jb	.LBB1_1461
.LBB1_1460:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 4]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 4], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 8]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 8], ecx
	mov	ecx, dword ptr [rdx + 4*rsi + 12]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi + 12], ecx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_1460
	jmp	.LBB1_1461
.LBB1_595:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_913
# %bb.596:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_597:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_597
	jmp	.LBB1_914
.LBB1_598:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_921
# %bb.599:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_600:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_600
	jmp	.LBB1_922
.LBB1_601:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_929
# %bb.602:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_603:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_603
	jmp	.LBB1_930
.LBB1_604:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_937
# %bb.605:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_606:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_606
	jmp	.LBB1_938
.LBB1_607:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_945
# %bb.608:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_609:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_609
	jmp	.LBB1_946
.LBB1_610:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_953
# %bb.611:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_612:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_612
	jmp	.LBB1_954
.LBB1_613:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_961
# %bb.614:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_615:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_615
	jmp	.LBB1_962
.LBB1_616:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_969
# %bb.617:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_618:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_618
	jmp	.LBB1_970
.LBB1_619:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_977
# %bb.620:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_621:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm2, ymmword ptr [rdx + 8*rsi]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rsi + 32]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rsi + 64]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rsi + 96]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm5, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm5
	vmovupd	ymm2, ymmword ptr [rdx + 8*rsi + 128]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rsi + 160]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rsi + 192]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rsi + 224]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm5, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 224], ymm5
	add	rsi, 32
	add	rdi, 2
	jne	.LBB1_621
	jmp	.LBB1_978
.LBB1_622:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_985
# %bb.623:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_624:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm2, ymmword ptr [rdx + 8*rsi]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rsi + 32]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rsi + 64]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rsi + 96]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm5, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm5
	vmovupd	ymm2, ymmword ptr [rdx + 8*rsi + 128]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rsi + 160]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rsi + 192]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rsi + 224]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm5, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 224], ymm5
	add	rsi, 32
	add	rdi, 2
	jne	.LBB1_624
	jmp	.LBB1_986
.LBB1_625:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB1_993
# %bb.626:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB1_627:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_627
	jmp	.LBB1_994
.LBB1_628:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB1_1001
# %bb.629:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB1_630:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_630
	jmp	.LBB1_1002
.LBB1_631:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1009
# %bb.632:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_633:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_633
	jmp	.LBB1_1010
.LBB1_634:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1017
# %bb.635:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_636:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_636
	jmp	.LBB1_1018
.LBB1_637:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1025
# %bb.638:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_639:                              # =>This Inner Loop Header: Depth=1
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 192]
	vmulpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_639
	jmp	.LBB1_1026
.LBB1_640:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1033
# %bb.641:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_642:                              # =>This Inner Loop Header: Depth=1
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 192]
	vmulpd	ymm5, ymm1, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_642
	jmp	.LBB1_1034
.LBB1_643:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1041
# %bb.644:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_645:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 224]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_645
	jmp	.LBB1_1042
.LBB1_646:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1049
# %bb.647:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_648:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 224]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_648
	jmp	.LBB1_1050
.LBB1_649:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_3] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB1_650:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rsi]
	vpmovsxbd	ymm2, qword ptr [rdx + rsi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rsi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rsi + 24]
	vpsrad	ymm5, ymm1, 7
	vpsrad	ymm6, ymm2, 7
	vpsrad	ymm7, ymm3, 7
	vpsrad	ymm8, ymm4, 7
	vpaddd	ymm1, ymm5, ymm1
	vpaddd	ymm2, ymm6, ymm2
	vpaddd	ymm3, ymm7, ymm3
	vpaddd	ymm4, ymm8, ymm4
	vpxor	ymm1, ymm1, ymm5
	vpxor	ymm2, ymm2, ymm6
	vpxor	ymm3, ymm3, ymm7
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm1, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm5        # xmm1 = xmm1[0],xmm5[0],xmm1[1],xmm5[1]
	vextracti128	xmm5, ymm2, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm2, xmm2, xmm0
	vpunpckldq	xmm2, xmm2, xmm5        # xmm2 = xmm2[0],xmm5[0],xmm2[1],xmm5[1]
	vextracti128	xmm5, ymm3, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0],xmm3[1],xmm5[1]
	vextracti128	xmm5, ymm4, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB1_650
# %bb.651:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_652
.LBB1_656:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_3] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB1_657:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rsi]
	vpmovsxbd	ymm2, qword ptr [rdx + rsi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rsi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rsi + 24]
	vpsrad	ymm5, ymm1, 7
	vpsrad	ymm6, ymm2, 7
	vpsrad	ymm7, ymm3, 7
	vpsrad	ymm8, ymm4, 7
	vpaddd	ymm1, ymm5, ymm1
	vpaddd	ymm2, ymm6, ymm2
	vpaddd	ymm3, ymm7, ymm3
	vpaddd	ymm4, ymm8, ymm4
	vpxor	ymm1, ymm1, ymm5
	vpxor	ymm2, ymm2, ymm6
	vpxor	ymm3, ymm3, ymm7
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm1, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm5        # xmm1 = xmm1[0],xmm5[0],xmm1[1],xmm5[1]
	vextracti128	xmm5, ymm2, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm2, xmm2, xmm0
	vpunpckldq	xmm2, xmm2, xmm5        # xmm2 = xmm2[0],xmm5[0],xmm2[1],xmm5[1]
	vextracti128	xmm5, ymm3, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0],xmm3[1],xmm5[1]
	vextracti128	xmm5, ymm4, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB1_657
# %bb.658:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_659
.LBB1_663:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1057
# %bb.664:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_665:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_665
	jmp	.LBB1_1058
.LBB1_666:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1065
# %bb.667:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_668:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_668
	jmp	.LBB1_1066
.LBB1_669:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, ecx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB1_853
# %bb.670:
	xor	esi, esi
	jmp	.LBB1_855
.LBB1_671:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, ecx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB1_863
# %bb.672:
	xor	esi, esi
	jmp	.LBB1_865
.LBB1_673:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1073
# %bb.674:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_675:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 224]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_675
	jmp	.LBB1_1074
.LBB1_676:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1081
# %bb.677:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_678:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 224]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_678
	jmp	.LBB1_1082
.LBB1_679:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1089
# %bb.680:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_681:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [r8 + 8*rdi], ymm0
	vmovups	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 8*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_681
	jmp	.LBB1_1090
.LBB1_682:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1097
# %bb.683:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_684:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [r8 + 8*rdi], ymm0
	vmovups	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 8*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_684
	jmp	.LBB1_1098
.LBB1_685:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1105
# %bb.686:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_687:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_687
	jmp	.LBB1_1106
.LBB1_688:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1113
# %bb.689:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_690:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_690
	jmp	.LBB1_1114
.LBB1_691:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rcx, rcx
	je	.LBB1_1121
# %bb.692:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_693:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_693
	jmp	.LBB1_1122
.LBB1_694:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rcx, rcx
	je	.LBB1_1129
# %bb.695:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_696:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_696
	jmp	.LBB1_1130
.LBB1_697:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1137
# %bb.698:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_699:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_699
	jmp	.LBB1_1138
.LBB1_700:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1145
# %bb.701:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_702:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_702
	jmp	.LBB1_1146
.LBB1_703:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1153
# %bb.704:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_705:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_705
	jmp	.LBB1_1154
.LBB1_706:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1161
# %bb.707:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_708:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_708
	jmp	.LBB1_1162
.LBB1_709:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB1_873
# %bb.710:
	xor	eax, eax
	jmp	.LBB1_875
.LBB1_711:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1169
# %bb.712:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI1_2] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB1_713:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 48]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm1
	add	rsi, 32
	add	rdi, 2
	jne	.LBB1_713
	jmp	.LBB1_1170
.LBB1_714:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB1_883
# %bb.715:
	xor	eax, eax
	jmp	.LBB1_885
.LBB1_716:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1177
# %bb.717:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI1_2] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB1_718:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 48]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm1
	add	rsi, 32
	add	rdi, 2
	jne	.LBB1_718
	jmp	.LBB1_1178
.LBB1_719:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1185
# %bb.720:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_721:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_721
	jmp	.LBB1_1186
.LBB1_722:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1193
# %bb.723:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_724:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_724
	jmp	.LBB1_1194
.LBB1_725:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1201
# %bb.726:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_727:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_727
	jmp	.LBB1_1202
.LBB1_728:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1209
# %bb.729:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_730:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_730
	jmp	.LBB1_1210
.LBB1_731:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1217
# %bb.732:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_733:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_733
	jmp	.LBB1_1218
.LBB1_734:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1225
# %bb.735:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_736:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_736
	jmp	.LBB1_1226
.LBB1_737:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1233
# %bb.738:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_739:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_739
	jmp	.LBB1_1234
.LBB1_740:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1241
# %bb.741:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_742:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_742
	jmp	.LBB1_1242
.LBB1_743:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1249
# %bb.744:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_745:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 224]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_745
	jmp	.LBB1_1250
.LBB1_746:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1257
# %bb.747:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_748:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm2, ymmword ptr [rdx + 4*rsi]
	vmovups	ymm3, ymmword ptr [rdx + 4*rsi + 32]
	vmovups	ymm4, ymmword ptr [rdx + 4*rsi + 64]
	vmovups	ymm5, ymmword ptr [rdx + 4*rsi + 96]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm5, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm5
	vmovups	ymm2, ymmword ptr [rdx + 4*rsi + 128]
	vmovups	ymm3, ymmword ptr [rdx + 4*rsi + 160]
	vmovups	ymm4, ymmword ptr [rdx + 4*rsi + 192]
	vmovups	ymm5, ymmword ptr [rdx + 4*rsi + 224]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm5, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 224], ymm5
	add	rsi, 64
	add	rdi, 2
	jne	.LBB1_748
	jmp	.LBB1_1258
.LBB1_749:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1265
# %bb.750:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_751:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 224]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_751
	jmp	.LBB1_1266
.LBB1_752:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1273
# %bb.753:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_754:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm2, ymmword ptr [rdx + 4*rsi]
	vmovups	ymm3, ymmword ptr [rdx + 4*rsi + 32]
	vmovups	ymm4, ymmword ptr [rdx + 4*rsi + 64]
	vmovups	ymm5, ymmword ptr [rdx + 4*rsi + 96]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm5, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm5
	vmovups	ymm2, ymmword ptr [rdx + 4*rsi + 128]
	vmovups	ymm3, ymmword ptr [rdx + 4*rsi + 160]
	vmovups	ymm4, ymmword ptr [rdx + 4*rsi + 192]
	vmovups	ymm5, ymmword ptr [rdx + 4*rsi + 224]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm5, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 224], ymm5
	add	rsi, 64
	add	rdi, 2
	jne	.LBB1_754
	jmp	.LBB1_1274
.LBB1_755:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB1_756:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rsi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rsi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rsi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rsi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm4
	add	rsi, 16
	cmp	rcx, rsi
	jne	.LBB1_756
# %bb.757:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_758
.LBB1_762:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1281
# %bb.763:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB1_764:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_764
	jmp	.LBB1_1282
.LBB1_765:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB1_766:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rsi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rsi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rsi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rsi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm4
	add	rsi, 16
	cmp	rcx, rsi
	jne	.LBB1_766
# %bb.767:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_768
.LBB1_772:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1291
# %bb.773:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB1_774:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_774
	jmp	.LBB1_1292
.LBB1_775:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1301
# %bb.776:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_777:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_777
	jmp	.LBB1_1302
.LBB1_778:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1309
# %bb.779:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_780:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_780
	jmp	.LBB1_1310
.LBB1_781:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1317
# %bb.782:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_783:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_783
	jmp	.LBB1_1318
.LBB1_784:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1325
# %bb.785:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_786:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_786
	jmp	.LBB1_1326
.LBB1_787:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rcx, rcx
	je	.LBB1_1333
# %bb.788:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_789:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_789
	jmp	.LBB1_1334
.LBB1_790:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1341
# %bb.791:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_792:                              # =>This Inner Loop Header: Depth=1
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 192]
	vmulps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_792
	jmp	.LBB1_1342
.LBB1_793:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rcx, rcx
	je	.LBB1_1349
# %bb.794:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_795:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_795
	jmp	.LBB1_1350
.LBB1_796:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1357
# %bb.797:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_798:                              # =>This Inner Loop Header: Depth=1
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 192]
	vmulps	ymm5, ymm1, ymmword ptr [rdx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_798
	jmp	.LBB1_1358
.LBB1_799:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1365
# %bb.800:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_801:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 224]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_801
	jmp	.LBB1_1366
.LBB1_802:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1373
# %bb.803:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_804:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 224]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_804
	jmp	.LBB1_1374
.LBB1_805:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1381
# %bb.806:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_807:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [r8 + rdi], ymm0
	vmovups	ymmword ptr [r8 + rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm3
	add	rdi, 256
	add	rsi, 2
	jne	.LBB1_807
	jmp	.LBB1_1382
.LBB1_808:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1389
# %bb.809:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_810:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [r8 + rdi], ymm0
	vmovups	ymmword ptr [r8 + rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm3
	add	rdi, 256
	add	rsi, 2
	jne	.LBB1_810
	jmp	.LBB1_1390
.LBB1_811:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1397
# %bb.812:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_813:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_813
	jmp	.LBB1_1398
.LBB1_814:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1405
# %bb.815:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_816:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rcx, 2
	jne	.LBB1_816
	jmp	.LBB1_1406
.LBB1_817:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, ecx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB1_893
# %bb.818:
	xor	esi, esi
	jmp	.LBB1_895
.LBB1_819:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, ecx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB1_903
# %bb.820:
	xor	esi, esi
	jmp	.LBB1_905
.LBB1_821:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1413
# %bb.822:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_823:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_823
	jmp	.LBB1_1414
.LBB1_824:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1421
# %bb.825:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_826:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_826
	jmp	.LBB1_1422
.LBB1_827:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB1_828:                              # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rsi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rsi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rsi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rsi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm3
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB1_828
# %bb.829:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_830
.LBB1_834:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB1_835:                              # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rsi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rsi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rsi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rsi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm3
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB1_835
# %bb.836:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_837
.LBB1_841:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1429
# %bb.842:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_843:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_843
	jmp	.LBB1_1430
.LBB1_844:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1437
# %bb.845:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_846:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_846
	jmp	.LBB1_1438
.LBB1_847:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1445
# %bb.848:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_849:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_849
	jmp	.LBB1_1446
.LBB1_850:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1453
# %bb.851:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_852:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_852
	jmp	.LBB1_1454
.LBB1_853:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_854:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB1_854
.LBB1_855:
	test	r9, r9
	je	.LBB1_858
# %bb.856:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_857:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB1_857
.LBB1_858:
	cmp	rdi, r10
	je	.LBB1_1461
	jmp	.LBB1_859
.LBB1_863:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_864:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB1_864
.LBB1_865:
	test	r9, r9
	je	.LBB1_868
# %bb.866:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_867:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB1_867
.LBB1_868:
	cmp	rdi, r10
	je	.LBB1_1461
	jmp	.LBB1_869
.LBB1_873:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_874:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [r8 + 2*rax], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [r8 + 2*rax + 64], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [r8 + 2*rax + 128], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 160], ymm1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovdqu	ymmword ptr [r8 + 2*rax + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB1_874
.LBB1_875:
	test	rsi, rsi
	je	.LBB1_878
# %bb.876:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB1_877:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB1_877
.LBB1_878:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_879
.LBB1_883:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_884:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [r8 + 2*rax], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [r8 + 2*rax + 64], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [r8 + 2*rax + 128], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 160], ymm1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovdqu	ymmword ptr [r8 + 2*rax + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB1_884
.LBB1_885:
	test	rsi, rsi
	je	.LBB1_888
# %bb.886:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB1_887:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB1_887
.LBB1_888:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_889
.LBB1_893:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_894:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB1_894
.LBB1_895:
	test	r9, r9
	je	.LBB1_898
# %bb.896:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_897:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB1_897
.LBB1_898:
	cmp	rdi, r10
	je	.LBB1_1461
	jmp	.LBB1_899
.LBB1_903:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_904:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rdx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB1_904
.LBB1_905:
	test	r9, r9
	je	.LBB1_908
# %bb.906:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_907:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB1_907
.LBB1_908:
	cmp	rdi, r10
	je	.LBB1_1461
	jmp	.LBB1_909
.LBB1_913:
	xor	edi, edi
.LBB1_914:
	test	r9b, 1
	je	.LBB1_916
# %bb.915:
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_916:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_917
.LBB1_921:
	xor	edi, edi
.LBB1_922:
	test	r9b, 1
	je	.LBB1_924
# %bb.923:
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_924:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_925
.LBB1_929:
	xor	edi, edi
.LBB1_930:
	test	r9b, 1
	je	.LBB1_932
# %bb.931:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB1_932:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_933
.LBB1_937:
	xor	edi, edi
.LBB1_938:
	test	r9b, 1
	je	.LBB1_940
# %bb.939:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB1_940:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_941
.LBB1_945:
	xor	edi, edi
.LBB1_946:
	test	r9b, 1
	je	.LBB1_948
# %bb.947:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_948:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_949
.LBB1_953:
	xor	edi, edi
.LBB1_954:
	test	r9b, 1
	je	.LBB1_956
# %bb.955:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_956:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_957
.LBB1_961:
	xor	edi, edi
.LBB1_962:
	test	r9b, 1
	je	.LBB1_964
# %bb.963:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_964:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_965
.LBB1_969:
	xor	edi, edi
.LBB1_970:
	test	r9b, 1
	je	.LBB1_972
# %bb.971:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_972:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_973
.LBB1_977:
	xor	esi, esi
.LBB1_978:
	test	r9b, 1
	je	.LBB1_980
# %bb.979:
	vmovupd	ymm2, ymmword ptr [rdx + 8*rsi]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rsi + 32]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rsi + 64]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rsi + 96]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm1, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
.LBB1_980:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_981
.LBB1_985:
	xor	esi, esi
.LBB1_986:
	test	r9b, 1
	je	.LBB1_988
# %bb.987:
	vmovupd	ymm2, ymmword ptr [rdx + 8*rsi]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rsi + 32]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rsi + 64]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rsi + 96]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm1, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
.LBB1_988:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_989
.LBB1_993:
	xor	edi, edi
.LBB1_994:
	test	r9b, 1
	je	.LBB1_996
# %bb.995:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_996:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_997
.LBB1_1001:
	xor	edi, edi
.LBB1_1002:
	test	r9b, 1
	je	.LBB1_1004
# %bb.1003:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1004:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_1005
.LBB1_1009:
	xor	edi, edi
.LBB1_1010:
	test	r9b, 1
	je	.LBB1_1012
# %bb.1011:
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1012:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1013
.LBB1_1017:
	xor	edi, edi
.LBB1_1018:
	test	r9b, 1
	je	.LBB1_1020
# %bb.1019:
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1020:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1021
.LBB1_1025:
	xor	edi, edi
.LBB1_1026:
	test	r9b, 1
	je	.LBB1_1028
# %bb.1027:
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1028:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1029
.LBB1_1033:
	xor	edi, edi
.LBB1_1034:
	test	r9b, 1
	je	.LBB1_1036
# %bb.1035:
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1036:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1037
.LBB1_1041:
	xor	edi, edi
.LBB1_1042:
	test	r9b, 1
	je	.LBB1_1044
# %bb.1043:
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1044:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1045
.LBB1_1049:
	xor	edi, edi
.LBB1_1050:
	test	r9b, 1
	je	.LBB1_1052
# %bb.1051:
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1052:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1053
.LBB1_1057:
	xor	edi, edi
.LBB1_1058:
	test	r9b, 1
	je	.LBB1_1060
# %bb.1059:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1060:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1061
.LBB1_1065:
	xor	edi, edi
.LBB1_1066:
	test	r9b, 1
	je	.LBB1_1068
# %bb.1067:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1068:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1069
.LBB1_1073:
	xor	edi, edi
.LBB1_1074:
	test	r9b, 1
	je	.LBB1_1076
# %bb.1075:
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1076:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1077
.LBB1_1081:
	xor	edi, edi
.LBB1_1082:
	test	r9b, 1
	je	.LBB1_1084
# %bb.1083:
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1084:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1085
.LBB1_1089:
	xor	edi, edi
.LBB1_1090:
	test	r9b, 1
	je	.LBB1_1092
# %bb.1091:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB1_1092:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1093
.LBB1_1097:
	xor	edi, edi
.LBB1_1098:
	test	r9b, 1
	je	.LBB1_1100
# %bb.1099:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB1_1100:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1101
.LBB1_1105:
	xor	edi, edi
.LBB1_1106:
	test	r9b, 1
	je	.LBB1_1108
# %bb.1107:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1108:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1109
.LBB1_1113:
	xor	edi, edi
.LBB1_1114:
	test	r9b, 1
	je	.LBB1_1116
# %bb.1115:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1116:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1117
.LBB1_1121:
	xor	edi, edi
.LBB1_1122:
	test	r9b, 1
	je	.LBB1_1124
# %bb.1123:
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1124:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1125
.LBB1_1129:
	xor	edi, edi
.LBB1_1130:
	test	r9b, 1
	je	.LBB1_1132
# %bb.1131:
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1132:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1133
.LBB1_1137:
	xor	edi, edi
.LBB1_1138:
	test	r9b, 1
	je	.LBB1_1140
# %bb.1139:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1140:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1141
.LBB1_1145:
	xor	edi, edi
.LBB1_1146:
	test	r9b, 1
	je	.LBB1_1148
# %bb.1147:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1148:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1149
.LBB1_1153:
	xor	edi, edi
.LBB1_1154:
	test	r9b, 1
	je	.LBB1_1156
# %bb.1155:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1156:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1157
.LBB1_1161:
	xor	edi, edi
.LBB1_1162:
	test	r9b, 1
	je	.LBB1_1164
# %bb.1163:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1164:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1165
.LBB1_1169:
	xor	esi, esi
.LBB1_1170:
	test	r9b, 1
	je	.LBB1_1172
# %bb.1171:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_2] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB1_1172:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1173
.LBB1_1177:
	xor	esi, esi
.LBB1_1178:
	test	r9b, 1
	je	.LBB1_1180
# %bb.1179:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_2] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB1_1180:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1181
.LBB1_1185:
	xor	edi, edi
.LBB1_1186:
	test	r9b, 1
	je	.LBB1_1188
# %bb.1187:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1188:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1189
.LBB1_1193:
	xor	edi, edi
.LBB1_1194:
	test	r9b, 1
	je	.LBB1_1196
# %bb.1195:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1196:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1197
.LBB1_1201:
	xor	edi, edi
.LBB1_1202:
	test	r9b, 1
	je	.LBB1_1204
# %bb.1203:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1204:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1205
.LBB1_1209:
	xor	edi, edi
.LBB1_1210:
	test	r9b, 1
	je	.LBB1_1212
# %bb.1211:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1212:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1213
.LBB1_1217:
	xor	edi, edi
.LBB1_1218:
	test	r9b, 1
	je	.LBB1_1220
# %bb.1219:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1220:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1221
.LBB1_1225:
	xor	edi, edi
.LBB1_1226:
	test	r9b, 1
	je	.LBB1_1228
# %bb.1227:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1228:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1229
.LBB1_1233:
	xor	edi, edi
.LBB1_1234:
	test	r9b, 1
	je	.LBB1_1236
# %bb.1235:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1236:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1237
.LBB1_1241:
	xor	edi, edi
.LBB1_1242:
	test	r9b, 1
	je	.LBB1_1244
# %bb.1243:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1244:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1245
.LBB1_1249:
	xor	edi, edi
.LBB1_1250:
	test	r9b, 1
	je	.LBB1_1252
# %bb.1251:
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1252:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1253
.LBB1_1257:
	xor	esi, esi
.LBB1_1258:
	test	r9b, 1
	je	.LBB1_1260
# %bb.1259:
	vmovups	ymm2, ymmword ptr [rdx + 4*rsi]
	vmovups	ymm3, ymmword ptr [rdx + 4*rsi + 32]
	vmovups	ymm4, ymmword ptr [rdx + 4*rsi + 64]
	vmovups	ymm5, ymmword ptr [rdx + 4*rsi + 96]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm1, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
.LBB1_1260:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1261
.LBB1_1265:
	xor	edi, edi
.LBB1_1266:
	test	r9b, 1
	je	.LBB1_1268
# %bb.1267:
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1268:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1269
.LBB1_1273:
	xor	esi, esi
.LBB1_1274:
	test	r9b, 1
	je	.LBB1_1276
# %bb.1275:
	vmovups	ymm2, ymmword ptr [rdx + 4*rsi]
	vmovups	ymm3, ymmword ptr [rdx + 4*rsi + 32]
	vmovups	ymm4, ymmword ptr [rdx + 4*rsi + 64]
	vmovups	ymm5, ymmword ptr [rdx + 4*rsi + 96]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm1, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
.LBB1_1276:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1277
.LBB1_1281:
	xor	edi, edi
.LBB1_1282:
	test	r9b, 1
	je	.LBB1_1284
# %bb.1283:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1284:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_1285
.LBB1_1291:
	xor	edi, edi
.LBB1_1292:
	test	r9b, 1
	je	.LBB1_1294
# %bb.1293:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1294:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_1295
.LBB1_1301:
	xor	edi, edi
.LBB1_1302:
	test	r9b, 1
	je	.LBB1_1304
# %bb.1303:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1304:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1305
.LBB1_1309:
	xor	edi, edi
.LBB1_1310:
	test	r9b, 1
	je	.LBB1_1312
# %bb.1311:
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1312:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1313
.LBB1_1317:
	xor	edi, edi
.LBB1_1318:
	test	r9b, 1
	je	.LBB1_1320
# %bb.1319:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1320:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1321
.LBB1_1325:
	xor	edi, edi
.LBB1_1326:
	test	r9b, 1
	je	.LBB1_1328
# %bb.1327:
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1328:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1329
.LBB1_1333:
	xor	edi, edi
.LBB1_1334:
	test	r9b, 1
	je	.LBB1_1336
# %bb.1335:
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1336:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1337
.LBB1_1341:
	xor	edi, edi
.LBB1_1342:
	test	r9b, 1
	je	.LBB1_1344
# %bb.1343:
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1344:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1345
.LBB1_1349:
	xor	edi, edi
.LBB1_1350:
	test	r9b, 1
	je	.LBB1_1352
# %bb.1351:
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1352:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1353
.LBB1_1357:
	xor	edi, edi
.LBB1_1358:
	test	r9b, 1
	je	.LBB1_1360
# %bb.1359:
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1360:
	cmp	rcx, rax
	je	.LBB1_1461
	jmp	.LBB1_1361
.LBB1_1365:
	xor	edi, edi
.LBB1_1366:
	test	r9b, 1
	je	.LBB1_1368
# %bb.1367:
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1368:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1369
.LBB1_1373:
	xor	edi, edi
.LBB1_1374:
	test	r9b, 1
	je	.LBB1_1376
# %bb.1375:
	vmovdqu	ymm1, ymmword ptr [rdx + rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1376:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1377
.LBB1_1381:
	xor	edi, edi
.LBB1_1382:
	test	r9b, 1
	je	.LBB1_1384
# %bb.1383:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB1_1384:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_1385
.LBB1_1389:
	xor	edi, edi
.LBB1_1390:
	test	r9b, 1
	je	.LBB1_1392
# %bb.1391:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB1_1392:
	cmp	rcx, r10
	je	.LBB1_1461
	jmp	.LBB1_1393
.LBB1_1397:
	xor	edi, edi
.LBB1_1398:
	test	r9b, 1
	je	.LBB1_1400
# %bb.1399:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1400:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1401
.LBB1_1405:
	xor	edi, edi
.LBB1_1406:
	test	r9b, 1
	je	.LBB1_1408
# %bb.1407:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1408:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1409
.LBB1_1413:
	xor	edi, edi
.LBB1_1414:
	test	r9b, 1
	je	.LBB1_1416
# %bb.1415:
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1416:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1417
.LBB1_1421:
	xor	edi, edi
.LBB1_1422:
	test	r9b, 1
	je	.LBB1_1424
# %bb.1423:
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1424:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1425
.LBB1_1429:
	xor	edi, edi
.LBB1_1430:
	test	r9b, 1
	je	.LBB1_1432
# %bb.1431:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1432:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1433
.LBB1_1437:
	xor	edi, edi
.LBB1_1438:
	test	r9b, 1
	je	.LBB1_1440
# %bb.1439:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1440:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1441
.LBB1_1445:
	xor	edi, edi
.LBB1_1446:
	test	r9b, 1
	je	.LBB1_1448
# %bb.1447:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1448:
	cmp	rsi, r10
	je	.LBB1_1461
	jmp	.LBB1_1449
.LBB1_1453:
	xor	edi, edi
.LBB1_1454:
	test	r9b, 1
	je	.LBB1_1456
# %bb.1455:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1456:
	cmp	rsi, r10
	jne	.LBB1_1457
.LBB1_1461:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end1:
	.size	arithmetic_arr_scalar_avx2, .Lfunc_end1-arithmetic_arr_scalar_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_scalar_arr_avx2
.LCPI2_0:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI2_1:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI2_2:
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
	.byte	16                              # 0x10
	.byte	17                              # 0x11
	.byte	20                              # 0x14
	.byte	21                              # 0x15
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	24                              # 0x18
	.byte	25                              # 0x19
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	28                              # 0x1c
	.byte	29                              # 0x1d
	.byte	30                              # 0x1e
	.byte	31                              # 0x1f
.LCPI2_4:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI2_3:
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
	.globl	arithmetic_scalar_arr_avx2
	.p2align	4, 0x90
	.type	arithmetic_scalar_arr_avx2,@function
arithmetic_scalar_arr_avx2:             # @arithmetic_scalar_arr_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 4
	jg	.LBB2_12
# %bb.1:
	cmp	sil, 1
	jg	.LBB2_23
# %bb.2:
	test	sil, sil
	je	.LBB2_43
# %bb.3:
	cmp	sil, 1
	jne	.LBB2_1461
# %bb.4:
	cmp	edi, 6
	jg	.LBB2_75
# %bb.5:
	cmp	edi, 3
	jle	.LBB2_131
# %bb.6:
	cmp	edi, 4
	je	.LBB2_211
# %bb.7:
	cmp	edi, 5
	je	.LBB2_214
# %bb.8:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.9:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.10:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_11
# %bb.355:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_595
# %bb.356:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_595
.LBB2_11:
	xor	esi, esi
.LBB2_917:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_919
.LBB2_918:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_918
.LBB2_919:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_920:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_920
	jmp	.LBB2_1461
.LBB2_12:
	cmp	sil, 6
	jg	.LBB2_33
# %bb.13:
	cmp	sil, 5
	je	.LBB2_51
# %bb.14:
	cmp	sil, 6
	jne	.LBB2_1461
# %bb.15:
	cmp	edi, 6
	jg	.LBB2_82
# %bb.16:
	cmp	edi, 3
	jle	.LBB2_136
# %bb.17:
	cmp	edi, 4
	je	.LBB2_217
# %bb.18:
	cmp	edi, 5
	je	.LBB2_220
# %bb.19:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.20:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.21:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_22
# %bb.358:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_598
# %bb.359:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_598
.LBB2_22:
	xor	esi, esi
.LBB2_925:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_927
.LBB2_926:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_926
.LBB2_927:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_928:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_928
	jmp	.LBB2_1461
.LBB2_23:
	cmp	sil, 2
	je	.LBB2_59
# %bb.24:
	cmp	sil, 4
	jne	.LBB2_1461
# %bb.25:
	cmp	edi, 6
	jg	.LBB2_89
# %bb.26:
	cmp	edi, 3
	jle	.LBB2_141
# %bb.27:
	cmp	edi, 4
	je	.LBB2_223
# %bb.28:
	cmp	edi, 5
	je	.LBB2_226
# %bb.29:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.30:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.31:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_32
# %bb.361:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_601
# %bb.362:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_601
.LBB2_32:
	xor	ecx, ecx
.LBB2_933:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_935
.LBB2_934:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_934
.LBB2_935:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_936:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	mov	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	mov	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_936
	jmp	.LBB2_1461
.LBB2_33:
	cmp	sil, 7
	je	.LBB2_67
# %bb.34:
	cmp	sil, 9
	jne	.LBB2_1461
# %bb.35:
	cmp	edi, 6
	jg	.LBB2_96
# %bb.36:
	cmp	edi, 3
	jle	.LBB2_146
# %bb.37:
	cmp	edi, 4
	je	.LBB2_229
# %bb.38:
	cmp	edi, 5
	je	.LBB2_232
# %bb.39:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.40:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.41:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_42
# %bb.364:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_604
# %bb.365:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_604
.LBB2_42:
	xor	ecx, ecx
.LBB2_941:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_943
.LBB2_942:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_942
.LBB2_943:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_944:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	mov	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	mov	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_944
	jmp	.LBB2_1461
.LBB2_43:
	cmp	edi, 6
	jg	.LBB2_103
# %bb.44:
	cmp	edi, 3
	jle	.LBB2_151
# %bb.45:
	cmp	edi, 4
	je	.LBB2_235
# %bb.46:
	cmp	edi, 5
	je	.LBB2_238
# %bb.47:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.48:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.49:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_50
# %bb.367:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_607
# %bb.368:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_607
.LBB2_50:
	xor	esi, esi
.LBB2_949:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_951
.LBB2_950:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_950
.LBB2_951:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_952:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_952
	jmp	.LBB2_1461
.LBB2_51:
	cmp	edi, 6
	jg	.LBB2_110
# %bb.52:
	cmp	edi, 3
	jle	.LBB2_156
# %bb.53:
	cmp	edi, 4
	je	.LBB2_241
# %bb.54:
	cmp	edi, 5
	je	.LBB2_244
# %bb.55:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.56:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.57:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_58
# %bb.370:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_610
# %bb.371:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_610
.LBB2_58:
	xor	esi, esi
.LBB2_957:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_959
.LBB2_958:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_958
.LBB2_959:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_960:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_960
	jmp	.LBB2_1461
.LBB2_59:
	cmp	edi, 6
	jg	.LBB2_117
# %bb.60:
	cmp	edi, 3
	jle	.LBB2_161
# %bb.61:
	cmp	edi, 4
	je	.LBB2_247
# %bb.62:
	cmp	edi, 5
	je	.LBB2_250
# %bb.63:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.64:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.65:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_66
# %bb.373:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_613
# %bb.374:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_613
.LBB2_66:
	xor	esi, esi
.LBB2_965:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_967
.LBB2_966:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_966
.LBB2_967:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_968:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_968
	jmp	.LBB2_1461
.LBB2_67:
	cmp	edi, 6
	jg	.LBB2_124
# %bb.68:
	cmp	edi, 3
	jle	.LBB2_166
# %bb.69:
	cmp	edi, 4
	je	.LBB2_253
# %bb.70:
	cmp	edi, 5
	je	.LBB2_256
# %bb.71:
	cmp	edi, 6
	jne	.LBB2_1461
# %bb.72:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.73:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_74
# %bb.376:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_616
# %bb.377:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_616
.LBB2_74:
	xor	esi, esi
.LBB2_973:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_975
.LBB2_974:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_974
.LBB2_975:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_976:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_976
	jmp	.LBB2_1461
.LBB2_75:
	cmp	edi, 8
	jle	.LBB2_171
# %bb.76:
	cmp	edi, 9
	je	.LBB2_259
# %bb.77:
	cmp	edi, 11
	je	.LBB2_262
# %bb.78:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.79:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.80:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_81
# %bb.379:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_619
# %bb.380:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_619
.LBB2_81:
	xor	edx, edx
.LBB2_981:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_983
.LBB2_982:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_982
.LBB2_983:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_984:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 8]
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 16]
	vmovsd	qword ptr [r8 + 8*rdx + 16], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 24]
	vmovsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_984
	jmp	.LBB2_1461
.LBB2_82:
	cmp	edi, 8
	jle	.LBB2_176
# %bb.83:
	cmp	edi, 9
	je	.LBB2_265
# %bb.84:
	cmp	edi, 11
	je	.LBB2_268
# %bb.85:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.86:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.87:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_88
# %bb.382:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_622
# %bb.383:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_622
.LBB2_88:
	xor	edx, edx
.LBB2_989:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_991
.LBB2_990:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_990
.LBB2_991:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_992:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 8]
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 16]
	vmovsd	qword ptr [r8 + 8*rdx + 16], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 24]
	vmovsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_992
	jmp	.LBB2_1461
.LBB2_89:
	cmp	edi, 8
	jle	.LBB2_181
# %bb.90:
	cmp	edi, 9
	je	.LBB2_271
# %bb.91:
	cmp	edi, 11
	je	.LBB2_274
# %bb.92:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.93:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.94:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_95
# %bb.385:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_625
# %bb.386:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_625
.LBB2_95:
	xor	ecx, ecx
.LBB2_997:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_999
.LBB2_998:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_998
.LBB2_999:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1000:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1000
	jmp	.LBB2_1461
.LBB2_96:
	cmp	edi, 8
	jle	.LBB2_186
# %bb.97:
	cmp	edi, 9
	je	.LBB2_277
# %bb.98:
	cmp	edi, 11
	je	.LBB2_280
# %bb.99:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.100:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.101:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_102
# %bb.388:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_628
# %bb.389:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_628
.LBB2_102:
	xor	ecx, ecx
.LBB2_1005:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_1007
.LBB2_1006:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_1006
.LBB2_1007:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1008:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	and	rax, rsi
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1008
	jmp	.LBB2_1461
.LBB2_103:
	cmp	edi, 8
	jle	.LBB2_191
# %bb.104:
	cmp	edi, 9
	je	.LBB2_283
# %bb.105:
	cmp	edi, 11
	je	.LBB2_286
# %bb.106:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.107:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.108:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_109
# %bb.391:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_631
# %bb.392:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_631
.LBB2_109:
	xor	edx, edx
.LBB2_1013:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1015
.LBB2_1014:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1014
.LBB2_1015:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1016:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 8]
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 16]
	vmovsd	qword ptr [r8 + 8*rdx + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 24]
	vmovsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1016
	jmp	.LBB2_1461
.LBB2_110:
	cmp	edi, 8
	jle	.LBB2_196
# %bb.111:
	cmp	edi, 9
	je	.LBB2_289
# %bb.112:
	cmp	edi, 11
	je	.LBB2_292
# %bb.113:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.114:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.115:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_116
# %bb.394:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_634
# %bb.395:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_634
.LBB2_116:
	xor	edx, edx
.LBB2_1021:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1023
.LBB2_1022:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1022
.LBB2_1023:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1024:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 8]
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 16]
	vmovsd	qword ptr [r8 + 8*rdx + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 24]
	vmovsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1024
	jmp	.LBB2_1461
.LBB2_117:
	cmp	edi, 8
	jle	.LBB2_201
# %bb.118:
	cmp	edi, 9
	je	.LBB2_295
# %bb.119:
	cmp	edi, 11
	je	.LBB2_298
# %bb.120:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.121:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.122:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_123
# %bb.397:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_637
# %bb.398:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_637
.LBB2_123:
	xor	edx, edx
.LBB2_1029:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1031
.LBB2_1030:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1030
.LBB2_1031:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1032:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 8]
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 16]
	vmovsd	qword ptr [r8 + 8*rdx + 16], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 24]
	vmovsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1032
	jmp	.LBB2_1461
.LBB2_124:
	cmp	edi, 8
	jle	.LBB2_206
# %bb.125:
	cmp	edi, 9
	je	.LBB2_301
# %bb.126:
	cmp	edi, 11
	je	.LBB2_304
# %bb.127:
	cmp	edi, 12
	jne	.LBB2_1461
# %bb.128:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.129:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_130
# %bb.400:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_640
# %bb.401:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_640
.LBB2_130:
	xor	edx, edx
.LBB2_1037:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1039
.LBB2_1038:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1038
.LBB2_1039:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1040:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 8]
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 16]
	vmovsd	qword ptr [r8 + 8*rdx + 16], xmm1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx + 24]
	vmovsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1040
	jmp	.LBB2_1461
.LBB2_131:
	cmp	edi, 2
	je	.LBB2_307
# %bb.132:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.133:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.134:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_135
# %bb.403:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_643
# %bb.404:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_643
.LBB2_135:
	xor	esi, esi
.LBB2_1045:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1047
.LBB2_1046:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1046
.LBB2_1047:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1048:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1048
	jmp	.LBB2_1461
.LBB2_136:
	cmp	edi, 2
	je	.LBB2_310
# %bb.137:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.138:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.139:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_140
# %bb.406:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_646
# %bb.407:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_646
.LBB2_140:
	xor	esi, esi
.LBB2_1053:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1055
.LBB2_1054:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1054
.LBB2_1055:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1056:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1056
	jmp	.LBB2_1461
.LBB2_141:
	cmp	edi, 2
	je	.LBB2_313
# %bb.142:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.143:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.144:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_145
# %bb.409:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB2_649
# %bb.410:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB2_649
.LBB2_145:
	xor	ecx, ecx
.LBB2_652:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_654
# %bb.653:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_654:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_655:                              # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdx + rcx]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx], sil
	movsx	esi, byte ptr [rdx + rcx + 1]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx + 1], sil
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_655
	jmp	.LBB2_1461
.LBB2_146:
	cmp	edi, 2
	je	.LBB2_316
# %bb.147:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.148:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.149:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_150
# %bb.412:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB2_656
# %bb.413:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB2_656
.LBB2_150:
	xor	ecx, ecx
.LBB2_659:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_661
# %bb.660:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_661:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_662:                              # =>This Inner Loop Header: Depth=1
	movsx	esi, byte ptr [rdx + rcx]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx], sil
	movsx	esi, byte ptr [rdx + rcx + 1]
	mov	edi, esi
	sar	edi, 7
	add	esi, edi
	xor	esi, edi
	mov	byte ptr [r8 + rcx + 1], sil
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_662
	jmp	.LBB2_1461
.LBB2_151:
	cmp	edi, 2
	je	.LBB2_319
# %bb.152:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.153:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.154:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_155
# %bb.415:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_663
# %bb.416:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_663
.LBB2_155:
	xor	esi, esi
.LBB2_1061:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1063
.LBB2_1062:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1062
.LBB2_1063:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1064:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	movzx	edx, byte ptr [rcx + rsi + 1]
	add	dl, al
	mov	byte ptr [r8 + rsi + 1], dl
	movzx	edx, byte ptr [rcx + rsi + 2]
	add	dl, al
	mov	byte ptr [r8 + rsi + 2], dl
	movzx	edx, byte ptr [rcx + rsi + 3]
	add	dl, al
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1064
	jmp	.LBB2_1461
.LBB2_156:
	cmp	edi, 2
	je	.LBB2_322
# %bb.157:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.158:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.159:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_160
# %bb.418:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_666
# %bb.419:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_666
.LBB2_160:
	xor	esi, esi
.LBB2_1069:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1071
.LBB2_1070:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1070
.LBB2_1071:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1072:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	movzx	edx, byte ptr [rcx + rsi + 1]
	add	dl, al
	mov	byte ptr [r8 + rsi + 1], dl
	movzx	edx, byte ptr [rcx + rsi + 2]
	add	dl, al
	mov	byte ptr [r8 + rsi + 2], dl
	movzx	edx, byte ptr [rcx + rsi + 3]
	add	dl, al
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1072
	jmp	.LBB2_1461
.LBB2_161:
	cmp	edi, 2
	je	.LBB2_325
# %bb.162:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.163:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.164:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_165
# %bb.421:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_669
# %bb.422:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_669
.LBB2_165:
	xor	edi, edi
.LBB2_859:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_861
.LBB2_860:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_860
.LBB2_861:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_862:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	dl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	dl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	dl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB2_862
	jmp	.LBB2_1461
.LBB2_166:
	cmp	edi, 2
	je	.LBB2_328
# %bb.167:
	cmp	edi, 3
	jne	.LBB2_1461
# %bb.168:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.169:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_170
# %bb.424:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_671
# %bb.425:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_671
.LBB2_170:
	xor	edi, edi
.LBB2_869:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_871
.LBB2_870:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_870
.LBB2_871:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_872:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	dl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	dl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	dl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB2_872
	jmp	.LBB2_1461
.LBB2_171:
	cmp	edi, 7
	je	.LBB2_331
# %bb.172:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.173:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.174:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_175
# %bb.427:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_673
# %bb.428:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_673
.LBB2_175:
	xor	esi, esi
.LBB2_1077:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1079
.LBB2_1078:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1078
.LBB2_1079:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_1080:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1080
	jmp	.LBB2_1461
.LBB2_176:
	cmp	edi, 7
	je	.LBB2_334
# %bb.177:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.178:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.179:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_180
# %bb.430:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_676
# %bb.431:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_676
.LBB2_180:
	xor	esi, esi
.LBB2_1085:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1087
.LBB2_1086:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1086
.LBB2_1087:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_1088:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1088
	jmp	.LBB2_1461
.LBB2_181:
	cmp	edi, 7
	je	.LBB2_337
# %bb.182:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.183:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.184:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_185
# %bb.433:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_679
# %bb.434:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_679
.LBB2_185:
	xor	ecx, ecx
.LBB2_1093:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1095
.LBB2_1094:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1094
.LBB2_1095:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1096:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1096
	jmp	.LBB2_1461
.LBB2_186:
	cmp	edi, 7
	je	.LBB2_340
# %bb.187:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.188:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.189:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_190
# %bb.436:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_682
# %bb.437:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_682
.LBB2_190:
	xor	ecx, ecx
.LBB2_1101:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1103
.LBB2_1102:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1102
.LBB2_1103:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1104:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	mov	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1104
	jmp	.LBB2_1461
.LBB2_191:
	cmp	edi, 7
	je	.LBB2_343
# %bb.192:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.193:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.194:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_195
# %bb.439:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_685
# %bb.440:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_685
.LBB2_195:
	xor	esi, esi
.LBB2_1109:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1111
.LBB2_1110:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1110
.LBB2_1111:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1112:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1112
	jmp	.LBB2_1461
.LBB2_196:
	cmp	edi, 7
	je	.LBB2_346
# %bb.197:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.198:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.199:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_200
# %bb.442:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_688
# %bb.443:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_688
.LBB2_200:
	xor	esi, esi
.LBB2_1117:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1119
.LBB2_1118:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1118
.LBB2_1119:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1120:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1120
	jmp	.LBB2_1461
.LBB2_201:
	cmp	edi, 7
	je	.LBB2_349
# %bb.202:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.203:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.204:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_205
# %bb.445:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_691
# %bb.446:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_691
.LBB2_205:
	xor	esi, esi
.LBB2_1125:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1127
.LBB2_1126:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1126
.LBB2_1127:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1128:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1128
	jmp	.LBB2_1461
.LBB2_206:
	cmp	edi, 7
	je	.LBB2_352
# %bb.207:
	cmp	edi, 8
	jne	.LBB2_1461
# %bb.208:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.209:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_210
# %bb.448:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_694
# %bb.449:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_694
.LBB2_210:
	xor	esi, esi
.LBB2_1133:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1135
.LBB2_1134:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1134
.LBB2_1135:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1136:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1136
	jmp	.LBB2_1461
.LBB2_211:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.212:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_213
# %bb.451:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_697
# %bb.452:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_697
.LBB2_213:
	xor	esi, esi
.LBB2_1141:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1143
.LBB2_1142:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1142
.LBB2_1143:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1144:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1144
	jmp	.LBB2_1461
.LBB2_214:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.215:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_216
# %bb.454:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_700
# %bb.455:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_700
.LBB2_216:
	xor	esi, esi
.LBB2_1149:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1151
.LBB2_1150:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1150
.LBB2_1151:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1152:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1152
	jmp	.LBB2_1461
.LBB2_217:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.218:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_219
# %bb.457:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_703
# %bb.458:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_703
.LBB2_219:
	xor	esi, esi
.LBB2_1157:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1159
.LBB2_1158:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1158
.LBB2_1159:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1160:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1160
	jmp	.LBB2_1461
.LBB2_220:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.221:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_222
# %bb.460:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_706
# %bb.461:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_706
.LBB2_222:
	xor	esi, esi
.LBB2_1165:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1167
.LBB2_1166:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1166
.LBB2_1167:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1168:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], dx
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1168
	jmp	.LBB2_1461
.LBB2_223:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.224:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_225
# %bb.463:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_709
# %bb.464:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_709
.LBB2_225:
	xor	ecx, ecx
.LBB2_879:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_881
.LBB2_880:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_880
.LBB2_881:
	cmp	rax, 3
	jb	.LBB2_1461
.LBB2_882:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], ax
	movzx	eax, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], ax
	movzx	eax, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], ax
	movzx	eax, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_882
	jmp	.LBB2_1461
.LBB2_226:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.227:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_228
# %bb.466:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_711
# %bb.467:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_711
.LBB2_228:
	xor	ecx, ecx
.LBB2_1173:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1175
# %bb.1174:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1175:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_1176:                             # =>This Inner Loop Header: Depth=1
	movsx	esi, word ptr [rdx + 2*rcx]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx], si
	movsx	esi, word ptr [rdx + 2*rcx + 2]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx + 2], si
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_1176
	jmp	.LBB2_1461
.LBB2_229:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.230:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_231
# %bb.469:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_714
# %bb.470:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_714
.LBB2_231:
	xor	ecx, ecx
.LBB2_889:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_891
.LBB2_890:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_890
.LBB2_891:
	cmp	rax, 3
	jb	.LBB2_1461
.LBB2_892:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], ax
	movzx	eax, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], ax
	movzx	eax, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], ax
	movzx	eax, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], ax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_892
	jmp	.LBB2_1461
.LBB2_232:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.233:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_234
# %bb.472:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_716
# %bb.473:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_716
.LBB2_234:
	xor	ecx, ecx
.LBB2_1181:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1183
# %bb.1182:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1183:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_1184:                             # =>This Inner Loop Header: Depth=1
	movsx	esi, word ptr [rdx + 2*rcx]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx], si
	movsx	esi, word ptr [rdx + 2*rcx + 2]
	mov	edi, esi
	sar	edi, 15
	add	esi, edi
	xor	esi, edi
	mov	word ptr [r8 + 2*rcx + 2], si
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_1184
	jmp	.LBB2_1461
.LBB2_235:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.236:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_237
# %bb.475:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_719
# %bb.476:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_719
.LBB2_237:
	xor	esi, esi
.LBB2_1189:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1191
.LBB2_1190:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1190
.LBB2_1191:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1192:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1192
	jmp	.LBB2_1461
.LBB2_238:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.239:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_240
# %bb.478:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_722
# %bb.479:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_722
.LBB2_240:
	xor	esi, esi
.LBB2_1197:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1199
.LBB2_1198:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1198
.LBB2_1199:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1200:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1200
	jmp	.LBB2_1461
.LBB2_241:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.242:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_243
# %bb.481:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_725
# %bb.482:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_725
.LBB2_243:
	xor	esi, esi
.LBB2_1205:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1207
.LBB2_1206:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1206
.LBB2_1207:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1208:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1208
	jmp	.LBB2_1461
.LBB2_244:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.245:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_246
# %bb.484:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_728
# %bb.485:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_728
.LBB2_246:
	xor	esi, esi
.LBB2_1213:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1215
.LBB2_1214:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1214
.LBB2_1215:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1216:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1216
	jmp	.LBB2_1461
.LBB2_247:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.248:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_249
# %bb.487:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_731
# %bb.488:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_731
.LBB2_249:
	xor	esi, esi
.LBB2_1221:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1223
.LBB2_1222:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1222
.LBB2_1223:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1224:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1224
	jmp	.LBB2_1461
.LBB2_250:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.251:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_252
# %bb.490:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_734
# %bb.491:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_734
.LBB2_252:
	xor	esi, esi
.LBB2_1229:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1231
.LBB2_1230:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1230
.LBB2_1231:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1232:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1232
	jmp	.LBB2_1461
.LBB2_253:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.254:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_255
# %bb.493:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_737
# %bb.494:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_737
.LBB2_255:
	xor	esi, esi
.LBB2_1237:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1239
.LBB2_1238:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1238
.LBB2_1239:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1240:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1240
	jmp	.LBB2_1461
.LBB2_256:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.257:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_258
# %bb.496:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_740
# %bb.497:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_740
.LBB2_258:
	xor	esi, esi
.LBB2_1245:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1247
.LBB2_1246:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1246
.LBB2_1247:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1248:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	movzx	edx, word ptr [rcx + 2*rsi + 2]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 2], dx
	movzx	edx, word ptr [rcx + 2*rsi + 4]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 4], dx
	movzx	edx, word ptr [rcx + 2*rsi + 6]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi + 6], dx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1248
	jmp	.LBB2_1461
.LBB2_259:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.260:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_261
# %bb.499:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_743
# %bb.500:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_743
.LBB2_261:
	xor	esi, esi
.LBB2_1253:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1255
.LBB2_1254:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1254
.LBB2_1255:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_1256:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1256
	jmp	.LBB2_1461
.LBB2_262:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.263:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_264
# %bb.502:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_746
# %bb.503:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_746
.LBB2_264:
	xor	edx, edx
.LBB2_1261:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1263
.LBB2_1262:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1262
.LBB2_1263:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1264:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 4]
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 8]
	vmovss	dword ptr [r8 + 4*rdx + 8], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 12]
	vmovss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1264
	jmp	.LBB2_1461
.LBB2_265:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.266:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_267
# %bb.505:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_749
# %bb.506:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_749
.LBB2_267:
	xor	esi, esi
.LBB2_1269:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1271
.LBB2_1270:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1270
.LBB2_1271:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_1272:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1272
	jmp	.LBB2_1461
.LBB2_268:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.269:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_270
# %bb.508:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_752
# %bb.509:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_752
.LBB2_270:
	xor	edx, edx
.LBB2_1277:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1279
.LBB2_1278:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1278
.LBB2_1279:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1280:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 4]
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 8]
	vmovss	dword ptr [r8 + 4*rdx + 8], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 12]
	vmovss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1280
	jmp	.LBB2_1461
.LBB2_271:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.272:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_273
# %bb.511:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_755
# %bb.512:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_755
.LBB2_273:
	xor	ecx, ecx
.LBB2_758:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_760
# %bb.759:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_760:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_761:                              # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rdi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_761
	jmp	.LBB2_1461
.LBB2_274:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.275:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_276
# %bb.514:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_762
# %bb.515:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_762
.LBB2_276:
	xor	ecx, ecx
.LBB2_1285:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1288
# %bb.1286:
	mov	esi, 2147483647
.LBB2_1287:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1287
.LBB2_1288:
	cmp	r9, 3
	jb	.LBB2_1461
# %bb.1289:
	mov	esi, 2147483647
.LBB2_1290:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1290
	jmp	.LBB2_1461
.LBB2_277:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.278:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_279
# %bb.517:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_765
# %bb.518:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_765
.LBB2_279:
	xor	ecx, ecx
.LBB2_768:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_770
# %bb.769:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_770:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_771:                              # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	mov	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	rdi, rsi
	neg	rdi
	cmovl	rdi, rsi
	mov	qword ptr [r8 + 8*rcx + 8], rdi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_771
	jmp	.LBB2_1461
.LBB2_280:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.281:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_282
# %bb.520:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_772
# %bb.521:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_772
.LBB2_282:
	xor	ecx, ecx
.LBB2_1295:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1298
# %bb.1296:
	mov	esi, 2147483647
.LBB2_1297:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1297
.LBB2_1298:
	cmp	r9, 3
	jb	.LBB2_1461
# %bb.1299:
	mov	esi, 2147483647
.LBB2_1300:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1300
	jmp	.LBB2_1461
.LBB2_283:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.284:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_285
# %bb.523:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_775
# %bb.524:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_775
.LBB2_285:
	xor	esi, esi
.LBB2_1305:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1307
.LBB2_1306:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1306
.LBB2_1307:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1308:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1308
	jmp	.LBB2_1461
.LBB2_286:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.287:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_288
# %bb.526:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_778
# %bb.527:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_778
.LBB2_288:
	xor	edx, edx
.LBB2_1313:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1315
.LBB2_1314:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1314
.LBB2_1315:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1316:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 4]
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 8]
	vmovss	dword ptr [r8 + 4*rdx + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 12]
	vmovss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1316
	jmp	.LBB2_1461
.LBB2_289:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.290:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_291
# %bb.529:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_781
# %bb.530:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_781
.LBB2_291:
	xor	esi, esi
.LBB2_1321:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1323
.LBB2_1322:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1322
.LBB2_1323:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1324:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1324
	jmp	.LBB2_1461
.LBB2_292:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.293:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_294
# %bb.532:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_784
# %bb.533:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_784
.LBB2_294:
	xor	edx, edx
.LBB2_1329:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1331
.LBB2_1330:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1330
.LBB2_1331:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1332:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 4]
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 8]
	vmovss	dword ptr [r8 + 4*rdx + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 12]
	vmovss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1332
	jmp	.LBB2_1461
.LBB2_295:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.296:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_297
# %bb.535:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_787
# %bb.536:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_787
.LBB2_297:
	xor	esi, esi
.LBB2_1337:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1339
.LBB2_1338:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1338
.LBB2_1339:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1340:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1340
	jmp	.LBB2_1461
.LBB2_298:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.299:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_300
# %bb.538:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_790
# %bb.539:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_790
.LBB2_300:
	xor	edx, edx
.LBB2_1345:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1347
.LBB2_1346:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1346
.LBB2_1347:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1348:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 4]
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 8]
	vmovss	dword ptr [r8 + 4*rdx + 8], xmm1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 12]
	vmovss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1348
	jmp	.LBB2_1461
.LBB2_301:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.302:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_303
# %bb.541:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_793
# %bb.542:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_793
.LBB2_303:
	xor	esi, esi
.LBB2_1353:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1355
.LBB2_1354:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1354
.LBB2_1355:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1356:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rsi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi + 24], rdx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1356
	jmp	.LBB2_1461
.LBB2_304:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.305:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_306
# %bb.544:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_796
# %bb.545:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_796
.LBB2_306:
	xor	edx, edx
.LBB2_1361:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1363
.LBB2_1362:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1362
.LBB2_1363:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1364:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 4]
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 8]
	vmovss	dword ptr [r8 + 4*rdx + 8], xmm1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx + 12]
	vmovss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1364
	jmp	.LBB2_1461
.LBB2_307:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.308:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_309
# %bb.547:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_799
# %bb.548:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_799
.LBB2_309:
	xor	esi, esi
.LBB2_1369:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1371
.LBB2_1370:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1370
.LBB2_1371:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1372:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1372
	jmp	.LBB2_1461
.LBB2_310:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.311:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_312
# %bb.550:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_802
# %bb.551:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_802
.LBB2_312:
	xor	esi, esi
.LBB2_1377:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1379
.LBB2_1378:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1378
.LBB2_1379:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1380:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], dl
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1380
	jmp	.LBB2_1461
.LBB2_313:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.314:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_315
# %bb.553:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_805
# %bb.554:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_805
.LBB2_315:
	xor	ecx, ecx
.LBB2_1385:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1387
.LBB2_1386:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1386
.LBB2_1387:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1388:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	movzx	eax, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	movzx	eax, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	mov	byte ptr [r8 + rcx + 3], al
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1388
	jmp	.LBB2_1461
.LBB2_316:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.317:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_318
# %bb.556:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_808
# %bb.557:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_808
.LBB2_318:
	xor	ecx, ecx
.LBB2_1393:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1395
.LBB2_1394:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1394
.LBB2_1395:
	cmp	rsi, 3
	jb	.LBB2_1461
.LBB2_1396:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	movzx	eax, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	movzx	eax, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	mov	byte ptr [r8 + rcx + 3], al
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1396
	jmp	.LBB2_1461
.LBB2_319:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.320:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_321
# %bb.559:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_811
# %bb.560:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_811
.LBB2_321:
	xor	esi, esi
.LBB2_1401:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1403
.LBB2_1402:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1402
.LBB2_1403:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1404:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	movzx	edx, byte ptr [rcx + rsi + 1]
	add	dl, al
	mov	byte ptr [r8 + rsi + 1], dl
	movzx	edx, byte ptr [rcx + rsi + 2]
	add	dl, al
	mov	byte ptr [r8 + rsi + 2], dl
	movzx	edx, byte ptr [rcx + rsi + 3]
	add	dl, al
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1404
	jmp	.LBB2_1461
.LBB2_322:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.323:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_324
# %bb.562:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_814
# %bb.563:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_814
.LBB2_324:
	xor	esi, esi
.LBB2_1409:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1411
.LBB2_1410:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1410
.LBB2_1411:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1412:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	movzx	edx, byte ptr [rcx + rsi + 1]
	add	dl, al
	mov	byte ptr [r8 + rsi + 1], dl
	movzx	edx, byte ptr [rcx + rsi + 2]
	add	dl, al
	mov	byte ptr [r8 + rsi + 2], dl
	movzx	edx, byte ptr [rcx + rsi + 3]
	add	dl, al
	mov	byte ptr [r8 + rsi + 3], dl
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1412
	jmp	.LBB2_1461
.LBB2_325:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.326:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_327
# %bb.565:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_817
# %bb.566:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_817
.LBB2_327:
	xor	edi, edi
.LBB2_899:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_901
.LBB2_900:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_900
.LBB2_901:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_902:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	dl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	dl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	dl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB2_902
	jmp	.LBB2_1461
.LBB2_328:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.329:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_330
# %bb.568:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_819
# %bb.569:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_819
.LBB2_330:
	xor	edi, edi
.LBB2_909:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_911
.LBB2_910:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_910
.LBB2_911:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_912:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	movzx	eax, byte ptr [rcx + rdi + 1]
	mul	dl
	mov	byte ptr [r8 + rdi + 1], al
	movzx	eax, byte ptr [rcx + rdi + 2]
	mul	dl
	mov	byte ptr [r8 + rdi + 2], al
	movzx	eax, byte ptr [rcx + rdi + 3]
	mul	dl
	mov	byte ptr [r8 + rdi + 3], al
	add	rdi, 4
	cmp	r10, rdi
	jne	.LBB2_912
	jmp	.LBB2_1461
.LBB2_331:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.332:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_333
# %bb.571:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_821
# %bb.572:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_821
.LBB2_333:
	xor	esi, esi
.LBB2_1417:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1419
.LBB2_1418:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1418
.LBB2_1419:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_1420:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1420
	jmp	.LBB2_1461
.LBB2_334:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.335:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_336
# %bb.574:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_824
# %bb.575:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_824
.LBB2_336:
	xor	esi, esi
.LBB2_1425:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1427
.LBB2_1426:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1426
.LBB2_1427:
	cmp	rdx, 3
	jb	.LBB2_1461
.LBB2_1428:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1428
	jmp	.LBB2_1461
.LBB2_337:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.338:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_339
# %bb.577:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_827
# %bb.578:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_827
.LBB2_339:
	xor	ecx, ecx
.LBB2_830:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_832
# %bb.831:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_832:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_833:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx], edi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx + 4], edi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_833
	jmp	.LBB2_1461
.LBB2_340:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.341:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_342
# %bb.580:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_834
# %bb.581:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_834
.LBB2_342:
	xor	ecx, ecx
.LBB2_837:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_839
# %bb.838:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_839:
	add	rsi, rax
	je	.LBB2_1461
.LBB2_840:                              # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx], edi
	mov	esi, dword ptr [rdx + 4*rcx + 4]
	mov	edi, esi
	neg	edi
	cmovl	edi, esi
	mov	dword ptr [r8 + 4*rcx + 4], edi
	add	rcx, 2
	cmp	rax, rcx
	jne	.LBB2_840
	jmp	.LBB2_1461
.LBB2_343:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.344:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_345
# %bb.583:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_841
# %bb.584:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_841
.LBB2_345:
	xor	esi, esi
.LBB2_1433:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1435
.LBB2_1434:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1434
.LBB2_1435:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1436:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1436
	jmp	.LBB2_1461
.LBB2_346:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.347:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_348
# %bb.586:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_844
# %bb.587:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_844
.LBB2_348:
	xor	esi, esi
.LBB2_1441:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1443
.LBB2_1442:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1442
.LBB2_1443:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1444:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1444
	jmp	.LBB2_1461
.LBB2_349:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.350:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_351
# %bb.589:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_847
# %bb.590:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_847
.LBB2_351:
	xor	esi, esi
.LBB2_1449:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1451
.LBB2_1450:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1450
.LBB2_1451:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1452:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1452
	jmp	.LBB2_1461
.LBB2_352:
	test	r9d, r9d
	jle	.LBB2_1461
# %bb.353:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_354
# %bb.592:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_850
# %bb.593:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_850
.LBB2_354:
	xor	esi, esi
.LBB2_1457:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1459
.LBB2_1458:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1458
.LBB2_1459:
	cmp	r9, 3
	jb	.LBB2_1461
.LBB2_1460:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	mov	edx, dword ptr [rcx + 4*rsi + 4]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 4], edx
	mov	edx, dword ptr [rcx + 4*rsi + 8]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 8], edx
	mov	edx, dword ptr [rcx + 4*rsi + 12]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi + 12], edx
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1460
	jmp	.LBB2_1461
.LBB2_595:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_913
# %bb.596:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_597:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_597
	jmp	.LBB2_914
.LBB2_598:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_921
# %bb.599:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_600:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_600
	jmp	.LBB2_922
.LBB2_601:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_929
# %bb.602:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_603:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_603
	jmp	.LBB2_930
.LBB2_604:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_937
# %bb.605:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_606:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm0
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_606
	jmp	.LBB2_938
.LBB2_607:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_945
# %bb.608:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_609:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_609
	jmp	.LBB2_946
.LBB2_610:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_953
# %bb.611:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_612:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_612
	jmp	.LBB2_954
.LBB2_613:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_961
# %bb.614:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_615:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_615
	jmp	.LBB2_962
.LBB2_616:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_969
# %bb.617:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_618:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_618
	jmp	.LBB2_970
.LBB2_619:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_977
# %bb.620:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_621:                              # =>This Inner Loop Header: Depth=1
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi + 128]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 160]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 192]
	vsubpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_621
	jmp	.LBB2_978
.LBB2_622:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_985
# %bb.623:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_624:                              # =>This Inner Loop Header: Depth=1
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi + 128]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 160]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 192]
	vsubpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_624
	jmp	.LBB2_986
.LBB2_625:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB2_993
# %bb.626:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB2_627:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_627
	jmp	.LBB2_994
.LBB2_628:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB2_1001
# %bb.629:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB2_630:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_630
	jmp	.LBB2_1002
.LBB2_631:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1009
# %bb.632:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_633:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_633
	jmp	.LBB2_1010
.LBB2_634:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1017
# %bb.635:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_636:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_636
	jmp	.LBB2_1018
.LBB2_637:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1025
# %bb.638:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_639:                              # =>This Inner Loop Header: Depth=1
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi + 128]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 160]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 192]
	vmulpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_639
	jmp	.LBB2_1026
.LBB2_640:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1033
# %bb.641:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_642:                              # =>This Inner Loop Header: Depth=1
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi + 128]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 160]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 192]
	vmulpd	ymm5, ymm1, ymmword ptr [rcx + 8*rdi + 224]
	vmovupd	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_642
	jmp	.LBB2_1034
.LBB2_643:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1041
# %bb.644:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_645:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_645
	jmp	.LBB2_1042
.LBB2_646:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1049
# %bb.647:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_648:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_648
	jmp	.LBB2_1050
.LBB2_649:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_3] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB2_650:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rsi]
	vpmovsxbd	ymm2, qword ptr [rdx + rsi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rsi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rsi + 24]
	vpsrad	ymm5, ymm1, 7
	vpsrad	ymm6, ymm2, 7
	vpsrad	ymm7, ymm3, 7
	vpsrad	ymm8, ymm4, 7
	vpaddd	ymm1, ymm5, ymm1
	vpaddd	ymm2, ymm6, ymm2
	vpaddd	ymm3, ymm7, ymm3
	vpaddd	ymm4, ymm8, ymm4
	vpxor	ymm1, ymm1, ymm5
	vpxor	ymm2, ymm2, ymm6
	vpxor	ymm3, ymm3, ymm7
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm1, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm5        # xmm1 = xmm1[0],xmm5[0],xmm1[1],xmm5[1]
	vextracti128	xmm5, ymm2, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm2, xmm2, xmm0
	vpunpckldq	xmm2, xmm2, xmm5        # xmm2 = xmm2[0],xmm5[0],xmm2[1],xmm5[1]
	vextracti128	xmm5, ymm3, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0],xmm3[1],xmm5[1]
	vextracti128	xmm5, ymm4, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB2_650
# %bb.651:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_652
.LBB2_656:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_3] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB2_657:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rsi]
	vpmovsxbd	ymm2, qword ptr [rdx + rsi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rsi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rsi + 24]
	vpsrad	ymm5, ymm1, 7
	vpsrad	ymm6, ymm2, 7
	vpsrad	ymm7, ymm3, 7
	vpsrad	ymm8, ymm4, 7
	vpaddd	ymm1, ymm5, ymm1
	vpaddd	ymm2, ymm6, ymm2
	vpaddd	ymm3, ymm7, ymm3
	vpaddd	ymm4, ymm8, ymm4
	vpxor	ymm1, ymm1, ymm5
	vpxor	ymm2, ymm2, ymm6
	vpxor	ymm3, ymm3, ymm7
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm1, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm5        # xmm1 = xmm1[0],xmm5[0],xmm1[1],xmm5[1]
	vextracti128	xmm5, ymm2, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm2, xmm2, xmm0
	vpunpckldq	xmm2, xmm2, xmm5        # xmm2 = xmm2[0],xmm5[0],xmm2[1],xmm5[1]
	vextracti128	xmm5, ymm3, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0],xmm3[1],xmm5[1]
	vextracti128	xmm5, ymm4, 1
	vpshufb	xmm5, xmm5, xmm0
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB2_657
# %bb.658:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_659
.LBB2_663:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1057
# %bb.664:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_665:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_665
	jmp	.LBB2_1058
.LBB2_666:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1065
# %bb.667:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_668:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_668
	jmp	.LBB2_1066
.LBB2_669:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, edx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB2_853
# %bb.670:
	xor	esi, esi
	jmp	.LBB2_855
.LBB2_671:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, edx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB2_863
# %bb.672:
	xor	esi, esi
	jmp	.LBB2_865
.LBB2_673:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1073
# %bb.674:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_675:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_675
	jmp	.LBB2_1074
.LBB2_676:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1081
# %bb.677:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_678:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_678
	jmp	.LBB2_1082
.LBB2_679:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1089
# %bb.680:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_681:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [r8 + 8*rdi], ymm0
	vmovups	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 8*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_681
	jmp	.LBB2_1090
.LBB2_682:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1097
# %bb.683:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_684:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [r8 + 8*rdi], ymm0
	vmovups	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + 8*rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_684
	jmp	.LBB2_1098
.LBB2_685:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1105
# %bb.686:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_687:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_687
	jmp	.LBB2_1106
.LBB2_688:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1113
# %bb.689:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_690:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_690
	jmp	.LBB2_1114
.LBB2_691:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rdx, rdx
	je	.LBB2_1121
# %bb.692:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_693:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_693
	jmp	.LBB2_1122
.LBB2_694:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rdx, rdx
	je	.LBB2_1129
# %bb.695:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_696:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_696
	jmp	.LBB2_1130
.LBB2_697:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1137
# %bb.698:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_699:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_699
	jmp	.LBB2_1138
.LBB2_700:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1145
# %bb.701:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_702:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_702
	jmp	.LBB2_1146
.LBB2_703:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1153
# %bb.704:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_705:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_705
	jmp	.LBB2_1154
.LBB2_706:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1161
# %bb.707:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_708:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_708
	jmp	.LBB2_1162
.LBB2_709:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB2_873
# %bb.710:
	xor	eax, eax
	jmp	.LBB2_875
.LBB2_711:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1169
# %bb.712:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_2] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB2_713:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 48]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm1
	add	rsi, 32
	add	rdi, 2
	jne	.LBB2_713
	jmp	.LBB2_1170
.LBB2_714:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB2_883
# %bb.715:
	xor	eax, eax
	jmp	.LBB2_885
.LBB2_716:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1177
# %bb.717:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_2] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB2_718:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rsi + 48]
	vpsrad	ymm3, ymm2, 15
	vpsrad	ymm4, ymm1, 15
	vpaddd	ymm1, ymm4, ymm1
	vpaddd	ymm2, ymm3, ymm2
	vpxor	ymm2, ymm2, ymm3
	vpxor	ymm1, ymm1, ymm4
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm1
	add	rsi, 32
	add	rdi, 2
	jne	.LBB2_718
	jmp	.LBB2_1178
.LBB2_719:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1185
# %bb.720:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_721:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_721
	jmp	.LBB2_1186
.LBB2_722:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1193
# %bb.723:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_724:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_724
	jmp	.LBB2_1194
.LBB2_725:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1201
# %bb.726:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_727:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_727
	jmp	.LBB2_1202
.LBB2_728:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1209
# %bb.729:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_730:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_730
	jmp	.LBB2_1210
.LBB2_731:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1217
# %bb.732:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_733:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_733
	jmp	.LBB2_1218
.LBB2_734:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1225
# %bb.735:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_736:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_736
	jmp	.LBB2_1226
.LBB2_737:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1233
# %bb.738:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_739:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_739
	jmp	.LBB2_1234
.LBB2_740:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1241
# %bb.741:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_742:                              # =>This Inner Loop Header: Depth=1
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpmullw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_742
	jmp	.LBB2_1242
.LBB2_743:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1249
# %bb.744:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_745:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_745
	jmp	.LBB2_1250
.LBB2_746:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1257
# %bb.747:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_748:                              # =>This Inner Loop Header: Depth=1
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi + 128]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 160]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 192]
	vsubps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_748
	jmp	.LBB2_1258
.LBB2_749:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1265
# %bb.750:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_751:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_751
	jmp	.LBB2_1266
.LBB2_752:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1273
# %bb.753:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_754:                              # =>This Inner Loop Header: Depth=1
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi + 128]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 160]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 192]
	vsubps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_754
	jmp	.LBB2_1274
.LBB2_755:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB2_756:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rsi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rsi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rsi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rsi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm4
	add	rsi, 16
	cmp	rcx, rsi
	jne	.LBB2_756
# %bb.757:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_758
.LBB2_762:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1281
# %bb.763:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB2_764:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_764
	jmp	.LBB2_1282
.LBB2_765:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB2_766:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rsi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rsi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rsi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rsi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm4
	add	rsi, 16
	cmp	rcx, rsi
	jne	.LBB2_766
# %bb.767:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_768
.LBB2_772:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1291
# %bb.773:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB2_774:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpand	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_774
	jmp	.LBB2_1292
.LBB2_775:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1301
# %bb.776:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_777:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_777
	jmp	.LBB2_1302
.LBB2_778:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1309
# %bb.779:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_780:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_780
	jmp	.LBB2_1310
.LBB2_781:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1317
# %bb.782:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_783:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_783
	jmp	.LBB2_1318
.LBB2_784:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1325
# %bb.785:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_786:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_786
	jmp	.LBB2_1326
.LBB2_787:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rdx, rdx
	je	.LBB2_1333
# %bb.788:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_789:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_789
	jmp	.LBB2_1334
.LBB2_790:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1341
# %bb.791:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_792:                              # =>This Inner Loop Header: Depth=1
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi + 128]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 160]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 192]
	vmulps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_792
	jmp	.LBB2_1342
.LBB2_793:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	vpsrlq	ymm1, ymm0, 32
	test	rdx, rdx
	je	.LBB2_1349
# %bb.794:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_795:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm5
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi + 128]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 160]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 192]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 224]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm6, ymm5, ymm1
	vpsrlq	ymm7, ymm5, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm5, ymm5, ymm0
	vpaddq	ymm5, ymm5, ymm6
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm5
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_795
	jmp	.LBB2_1350
.LBB2_796:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1357
# %bb.797:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_798:                              # =>This Inner Loop Header: Depth=1
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm5
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi + 128]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 160]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 192]
	vmulps	ymm5, ymm1, ymmword ptr [rcx + 4*rdi + 224]
	vmovups	ymmword ptr [r8 + 4*rdi + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 224], ymm5
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_798
	jmp	.LBB2_1358
.LBB2_799:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1365
# %bb.800:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_801:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_801
	jmp	.LBB2_1366
.LBB2_802:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1373
# %bb.803:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_804:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_804
	jmp	.LBB2_1374
.LBB2_805:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1381
# %bb.806:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_807:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [r8 + rdi], ymm0
	vmovups	ymmword ptr [r8 + rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm3
	add	rdi, 256
	add	rsi, 2
	jne	.LBB2_807
	jmp	.LBB2_1382
.LBB2_808:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1389
# %bb.809:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_810:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [r8 + rdi], ymm0
	vmovups	ymmword ptr [r8 + rdi + 32], ymm1
	vmovups	ymmword ptr [r8 + rdi + 64], ymm2
	vmovups	ymmword ptr [r8 + rdi + 96], ymm3
	vmovdqu	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm3
	add	rdi, 256
	add	rsi, 2
	jne	.LBB2_810
	jmp	.LBB2_1390
.LBB2_811:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1397
# %bb.812:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_813:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_813
	jmp	.LBB2_1398
.LBB2_814:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1405
# %bb.815:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_816:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rdx, 2
	jne	.LBB2_816
	jmp	.LBB2_1406
.LBB2_817:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, edx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB2_893
# %bb.818:
	xor	esi, esi
	jmp	.LBB2_895
.LBB2_819:
	mov	edi, r10d
	and	edi, -32
	vmovd	xmm0, edx
	vpbroadcastb	ymm0, xmm0
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB2_903
# %bb.820:
	xor	esi, esi
	jmp	.LBB2_905
.LBB2_821:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1413
# %bb.822:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_823:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_823
	jmp	.LBB2_1414
.LBB2_824:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1421
# %bb.825:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_826:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_826
	jmp	.LBB2_1422
.LBB2_827:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB2_828:                              # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rsi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rsi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rsi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rsi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm3
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB2_828
# %bb.829:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_830
.LBB2_834:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB2_835:                              # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rsi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rsi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rsi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rsi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm3
	add	rsi, 32
	cmp	rcx, rsi
	jne	.LBB2_835
# %bb.836:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_837
.LBB2_841:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1429
# %bb.842:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_843:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_843
	jmp	.LBB2_1430
.LBB2_844:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1437
# %bb.845:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_846:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_846
	jmp	.LBB2_1438
.LBB2_847:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1445
# %bb.848:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_849:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_849
	jmp	.LBB2_1446
.LBB2_850:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1453
# %bb.851:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_852:                              # =>This Inner Loop Header: Depth=1
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi + 128]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 160]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 192]
	vpmulld	ymm4, ymm0, ymmword ptr [rcx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_852
	jmp	.LBB2_1454
.LBB2_853:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_854:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB2_854
.LBB2_855:
	test	r9, r9
	je	.LBB2_858
# %bb.856:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_857:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB2_857
.LBB2_858:
	cmp	rdi, r10
	je	.LBB2_1461
	jmp	.LBB2_859
.LBB2_863:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_864:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB2_864
.LBB2_865:
	test	r9, r9
	je	.LBB2_868
# %bb.866:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_867:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB2_867
.LBB2_868:
	cmp	rdi, r10
	je	.LBB2_1461
	jmp	.LBB2_869
.LBB2_873:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_874:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [r8 + 2*rax], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [r8 + 2*rax + 64], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [r8 + 2*rax + 128], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 160], ymm1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovdqu	ymmword ptr [r8 + 2*rax + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB2_874
.LBB2_875:
	test	rsi, rsi
	je	.LBB2_878
# %bb.876:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB2_877:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB2_877
.LBB2_878:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_879
.LBB2_883:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_884:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [r8 + 2*rax], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [r8 + 2*rax + 64], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [r8 + 2*rax + 128], ymm0
	vmovups	ymmword ptr [r8 + 2*rax + 160], ymm1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovdqu	ymmword ptr [r8 + 2*rax + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB2_884
.LBB2_885:
	test	rsi, rsi
	je	.LBB2_888
# %bb.886:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB2_887:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB2_887
.LBB2_888:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_889
.LBB2_893:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_894:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB2_894
.LBB2_895:
	test	r9, r9
	je	.LBB2_898
# %bb.896:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_897:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB2_897
.LBB2_898:
	cmp	rdi, r10
	je	.LBB2_1461
	jmp	.LBB2_899
.LBB2_903:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_904:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 32]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 64]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 96]
	vpunpckhbw	ymm5, ymm4, ymm4        # ymm5 = ymm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm2
	vpunpcklbw	ymm4, ymm4, ymm4        # ymm4 = ymm4[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm2
	vpackuswb	ymm4, ymm4, ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm4
	sub	rsi, -128
	add	rax, 4
	jne	.LBB2_904
.LBB2_905:
	test	r9, r9
	je	.LBB2_908
# %bb.906:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_4] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_907:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + rsi]
	vpunpckhbw	ymm4, ymm3, ymm3        # ymm4 = ymm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vpmullw	ymm4, ymm4, ymm1
	vpand	ymm4, ymm4, ymm2
	vpunpcklbw	ymm3, ymm3, ymm3        # ymm3 = ymm3[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
	vpmullw	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm2
	vpackuswb	ymm3, ymm3, ymm4
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	inc	r9
	jne	.LBB2_907
.LBB2_908:
	cmp	rdi, r10
	je	.LBB2_1461
	jmp	.LBB2_909
.LBB2_913:
	xor	edi, edi
.LBB2_914:
	test	r9b, 1
	je	.LBB2_916
# %bb.915:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_916:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_917
.LBB2_921:
	xor	edi, edi
.LBB2_922:
	test	r9b, 1
	je	.LBB2_924
# %bb.923:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_924:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_925
.LBB2_929:
	xor	edi, edi
.LBB2_930:
	test	r9b, 1
	je	.LBB2_932
# %bb.931:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB2_932:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_933
.LBB2_937:
	xor	edi, edi
.LBB2_938:
	test	r9b, 1
	je	.LBB2_940
# %bb.939:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB2_940:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_941
.LBB2_945:
	xor	edi, edi
.LBB2_946:
	test	r9b, 1
	je	.LBB2_948
# %bb.947:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_948:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_949
.LBB2_953:
	xor	edi, edi
.LBB2_954:
	test	r9b, 1
	je	.LBB2_956
# %bb.955:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_956:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_957
.LBB2_961:
	xor	edi, edi
.LBB2_962:
	test	r9b, 1
	je	.LBB2_964
# %bb.963:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_964:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_965
.LBB2_969:
	xor	edi, edi
.LBB2_970:
	test	r9b, 1
	je	.LBB2_972
# %bb.971:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_972:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_973
.LBB2_977:
	xor	edi, edi
.LBB2_978:
	test	r9b, 1
	je	.LBB2_980
# %bb.979:
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_980:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_981
.LBB2_985:
	xor	edi, edi
.LBB2_986:
	test	r9b, 1
	je	.LBB2_988
# %bb.987:
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_988:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_989
.LBB2_993:
	xor	edi, edi
.LBB2_994:
	test	r9b, 1
	je	.LBB2_996
# %bb.995:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_996:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_997
.LBB2_1001:
	xor	edi, edi
.LBB2_1002:
	test	r9b, 1
	je	.LBB2_1004
# %bb.1003:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1004:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_1005
.LBB2_1009:
	xor	edi, edi
.LBB2_1010:
	test	r9b, 1
	je	.LBB2_1012
# %bb.1011:
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1012:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1013
.LBB2_1017:
	xor	edi, edi
.LBB2_1018:
	test	r9b, 1
	je	.LBB2_1020
# %bb.1019:
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1020:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1021
.LBB2_1025:
	xor	edi, edi
.LBB2_1026:
	test	r9b, 1
	je	.LBB2_1028
# %bb.1027:
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1028:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1029
.LBB2_1033:
	xor	edi, edi
.LBB2_1034:
	test	r9b, 1
	je	.LBB2_1036
# %bb.1035:
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1036:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1037
.LBB2_1041:
	xor	edi, edi
.LBB2_1042:
	test	r9b, 1
	je	.LBB2_1044
# %bb.1043:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1044:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1045
.LBB2_1049:
	xor	edi, edi
.LBB2_1050:
	test	r9b, 1
	je	.LBB2_1052
# %bb.1051:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1052:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1053
.LBB2_1057:
	xor	edi, edi
.LBB2_1058:
	test	r9b, 1
	je	.LBB2_1060
# %bb.1059:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1060:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1061
.LBB2_1065:
	xor	edi, edi
.LBB2_1066:
	test	r9b, 1
	je	.LBB2_1068
# %bb.1067:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1068:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1069
.LBB2_1073:
	xor	edi, edi
.LBB2_1074:
	test	r9b, 1
	je	.LBB2_1076
# %bb.1075:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1076:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1077
.LBB2_1081:
	xor	edi, edi
.LBB2_1082:
	test	r9b, 1
	je	.LBB2_1084
# %bb.1083:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1084:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1085
.LBB2_1089:
	xor	edi, edi
.LBB2_1090:
	test	r9b, 1
	je	.LBB2_1092
# %bb.1091:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB2_1092:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_1093
.LBB2_1097:
	xor	edi, edi
.LBB2_1098:
	test	r9b, 1
	je	.LBB2_1100
# %bb.1099:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB2_1100:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_1101
.LBB2_1105:
	xor	edi, edi
.LBB2_1106:
	test	r9b, 1
	je	.LBB2_1108
# %bb.1107:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1108:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1109
.LBB2_1113:
	xor	edi, edi
.LBB2_1114:
	test	r9b, 1
	je	.LBB2_1116
# %bb.1115:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1116:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1117
.LBB2_1121:
	xor	edi, edi
.LBB2_1122:
	test	r9b, 1
	je	.LBB2_1124
# %bb.1123:
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1124:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1125
.LBB2_1129:
	xor	edi, edi
.LBB2_1130:
	test	r9b, 1
	je	.LBB2_1132
# %bb.1131:
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1132:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1133
.LBB2_1137:
	xor	edi, edi
.LBB2_1138:
	test	r9b, 1
	je	.LBB2_1140
# %bb.1139:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1140:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1141
.LBB2_1145:
	xor	edi, edi
.LBB2_1146:
	test	r9b, 1
	je	.LBB2_1148
# %bb.1147:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1148:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1149
.LBB2_1153:
	xor	edi, edi
.LBB2_1154:
	test	r9b, 1
	je	.LBB2_1156
# %bb.1155:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1156:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1157
.LBB2_1161:
	xor	edi, edi
.LBB2_1162:
	test	r9b, 1
	je	.LBB2_1164
# %bb.1163:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1164:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1165
.LBB2_1169:
	xor	esi, esi
.LBB2_1170:
	test	r9b, 1
	je	.LBB2_1172
# %bb.1171:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_2] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB2_1172:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_1173
.LBB2_1177:
	xor	esi, esi
.LBB2_1178:
	test	r9b, 1
	je	.LBB2_1180
# %bb.1179:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_2] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB2_1180:
	cmp	rcx, rax
	je	.LBB2_1461
	jmp	.LBB2_1181
.LBB2_1185:
	xor	edi, edi
.LBB2_1186:
	test	r9b, 1
	je	.LBB2_1188
# %bb.1187:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1188:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1189
.LBB2_1193:
	xor	edi, edi
.LBB2_1194:
	test	r9b, 1
	je	.LBB2_1196
# %bb.1195:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1196:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1197
.LBB2_1201:
	xor	edi, edi
.LBB2_1202:
	test	r9b, 1
	je	.LBB2_1204
# %bb.1203:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1204:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1205
.LBB2_1209:
	xor	edi, edi
.LBB2_1210:
	test	r9b, 1
	je	.LBB2_1212
# %bb.1211:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1212:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1213
.LBB2_1217:
	xor	edi, edi
.LBB2_1218:
	test	r9b, 1
	je	.LBB2_1220
# %bb.1219:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1220:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1221
.LBB2_1225:
	xor	edi, edi
.LBB2_1226:
	test	r9b, 1
	je	.LBB2_1228
# %bb.1227:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1228:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1229
.LBB2_1233:
	xor	edi, edi
.LBB2_1234:
	test	r9b, 1
	je	.LBB2_1236
# %bb.1235:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1236:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1237
.LBB2_1241:
	xor	edi, edi
.LBB2_1242:
	test	r9b, 1
	je	.LBB2_1244
# %bb.1243:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1244:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1245
.LBB2_1249:
	xor	edi, edi
.LBB2_1250:
	test	r9b, 1
	je	.LBB2_1252
# %bb.1251:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1252:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1253
.LBB2_1257:
	xor	edi, edi
.LBB2_1258:
	test	r9b, 1
	je	.LBB2_1260
# %bb.1259:
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1260:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1261
.LBB2_1265:
	xor	edi, edi
.LBB2_1266:
	test	r9b, 1
	je	.LBB2_1268
# %bb.1267:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1268:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1269
.LBB2_1273:
	xor	edi, edi
.LBB2_1274:
	test	r9b, 1
	je	.LBB2_1276
# %bb.1275:
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1276:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1277
.LBB2_1281:
	xor	edi, edi
.LBB2_1282:
	test	r9b, 1
	je	.LBB2_1284
# %bb.1283:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1284:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_1285
.LBB2_1291:
	xor	edi, edi
.LBB2_1292:
	test	r9b, 1
	je	.LBB2_1294
# %bb.1293:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_1] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1294:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_1295
.LBB2_1301:
	xor	edi, edi
.LBB2_1302:
	test	r9b, 1
	je	.LBB2_1304
# %bb.1303:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1304:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1305
.LBB2_1309:
	xor	edi, edi
.LBB2_1310:
	test	r9b, 1
	je	.LBB2_1312
# %bb.1311:
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1312:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1313
.LBB2_1317:
	xor	edi, edi
.LBB2_1318:
	test	r9b, 1
	je	.LBB2_1320
# %bb.1319:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1320:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1321
.LBB2_1325:
	xor	edi, edi
.LBB2_1326:
	test	r9b, 1
	je	.LBB2_1328
# %bb.1327:
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1328:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1329
.LBB2_1333:
	xor	edi, edi
.LBB2_1334:
	test	r9b, 1
	je	.LBB2_1336
# %bb.1335:
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1336:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1337
.LBB2_1341:
	xor	edi, edi
.LBB2_1342:
	test	r9b, 1
	je	.LBB2_1344
# %bb.1343:
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1344:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1345
.LBB2_1349:
	xor	edi, edi
.LBB2_1350:
	test	r9b, 1
	je	.LBB2_1352
# %bb.1351:
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rdi]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rdi + 96]
	vpmuludq	ymm6, ymm2, ymm1
	vpsrlq	ymm7, ymm2, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm2, ymm2, ymm0
	vpaddq	ymm2, ymm2, ymm6
	vpmuludq	ymm6, ymm3, ymm1
	vpsrlq	ymm7, ymm3, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm3, ymm3, ymm0
	vpaddq	ymm3, ymm3, ymm6
	vpmuludq	ymm6, ymm4, ymm1
	vpsrlq	ymm7, ymm4, 32
	vpmuludq	ymm7, ymm7, ymm0
	vpaddq	ymm6, ymm6, ymm7
	vpsllq	ymm6, ymm6, 32
	vpmuludq	ymm4, ymm4, ymm0
	vpaddq	ymm4, ymm4, ymm6
	vpmuludq	ymm1, ymm5, ymm1
	vpsrlq	ymm6, ymm5, 32
	vpmuludq	ymm6, ymm6, ymm0
	vpaddq	ymm1, ymm1, ymm6
	vpsllq	ymm1, ymm1, 32
	vpmuludq	ymm0, ymm5, ymm0
	vpaddq	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1352:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1353
.LBB2_1357:
	xor	edi, edi
.LBB2_1358:
	test	r9b, 1
	je	.LBB2_1360
# %bb.1359:
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1360:
	cmp	rdx, rax
	je	.LBB2_1461
	jmp	.LBB2_1361
.LBB2_1365:
	xor	edi, edi
.LBB2_1366:
	test	r9b, 1
	je	.LBB2_1368
# %bb.1367:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1368:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1369
.LBB2_1373:
	xor	edi, edi
.LBB2_1374:
	test	r9b, 1
	je	.LBB2_1376
# %bb.1375:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1376:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1377
.LBB2_1381:
	xor	edi, edi
.LBB2_1382:
	test	r9b, 1
	je	.LBB2_1384
# %bb.1383:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB2_1384:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_1385
.LBB2_1389:
	xor	edi, edi
.LBB2_1390:
	test	r9b, 1
	je	.LBB2_1392
# %bb.1391:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB2_1392:
	cmp	rcx, r10
	je	.LBB2_1461
	jmp	.LBB2_1393
.LBB2_1397:
	xor	edi, edi
.LBB2_1398:
	test	r9b, 1
	je	.LBB2_1400
# %bb.1399:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1400:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1401
.LBB2_1405:
	xor	edi, edi
.LBB2_1406:
	test	r9b, 1
	je	.LBB2_1408
# %bb.1407:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1408:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1409
.LBB2_1413:
	xor	edi, edi
.LBB2_1414:
	test	r9b, 1
	je	.LBB2_1416
# %bb.1415:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1416:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1417
.LBB2_1421:
	xor	edi, edi
.LBB2_1422:
	test	r9b, 1
	je	.LBB2_1424
# %bb.1423:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1424:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1425
.LBB2_1429:
	xor	edi, edi
.LBB2_1430:
	test	r9b, 1
	je	.LBB2_1432
# %bb.1431:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1432:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1433
.LBB2_1437:
	xor	edi, edi
.LBB2_1438:
	test	r9b, 1
	je	.LBB2_1440
# %bb.1439:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1440:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1441
.LBB2_1445:
	xor	edi, edi
.LBB2_1446:
	test	r9b, 1
	je	.LBB2_1448
# %bb.1447:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1448:
	cmp	rsi, r10
	je	.LBB2_1461
	jmp	.LBB2_1449
.LBB2_1453:
	xor	edi, edi
.LBB2_1454:
	test	r9b, 1
	je	.LBB2_1456
# %bb.1455:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1456:
	cmp	rsi, r10
	jne	.LBB2_1457
.LBB2_1461:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end2:
	.size	arithmetic_scalar_arr_avx2, .Lfunc_end2-arithmetic_scalar_arr_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
