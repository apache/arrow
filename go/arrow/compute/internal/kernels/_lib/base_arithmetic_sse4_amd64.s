	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_sse4
.LCPI0_0:
	.long	0x00000000
	.long	0x80000000              # double -0
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI0_1:
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
.LCPI0_2:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI0_3:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI0_4:
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
.LCPI0_5:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	arithmetic_sse4
	.p2align	4, 0x90
	.type	arithmetic_sse4,@function
arithmetic_sse4:                        # @arithmetic_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 6
	jg	.LBB0_13
# %bb.1:
	cmp	sil, 1
	jle	.LBB0_25
# %bb.2:
	cmp	sil, 2
	je	.LBB0_53
# %bb.3:
	cmp	sil, 4
	je	.LBB0_65
# %bb.4:
	cmp	sil, 5
	jne	.LBB0_1751
# %bb.5:
	cmp	edi, 6
	jg	.LBB0_117
# %bb.6:
	cmp	edi, 3
	jle	.LBB0_211
# %bb.7:
	cmp	edi, 4
	je	.LBB0_351
# %bb.8:
	cmp	edi, 5
	je	.LBB0_354
# %bb.9:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.10:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.11:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_12
# %bb.667:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1083
# %bb.668:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1083
.LBB0_12:
	xor	ecx, ecx
.LBB0_1519:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1521
.LBB0_1520:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1520
.LBB0_1521:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1522:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1522
	jmp	.LBB0_1751
.LBB0_13:
	cmp	sil, 8
	jle	.LBB0_39
# %bb.14:
	cmp	sil, 9
	je	.LBB0_73
# %bb.15:
	cmp	sil, 11
	je	.LBB0_85
# %bb.16:
	cmp	sil, 12
	jne	.LBB0_1751
# %bb.17:
	cmp	edi, 6
	jg	.LBB0_124
# %bb.18:
	cmp	edi, 3
	jle	.LBB0_216
# %bb.19:
	cmp	edi, 4
	je	.LBB0_357
# %bb.20:
	cmp	edi, 5
	je	.LBB0_360
# %bb.21:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.22:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.23:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB0_670
# %bb.24:
	xor	ecx, ecx
	jmp	.LBB0_1092
.LBB0_25:
	test	sil, sil
	je	.LBB0_93
# %bb.26:
	cmp	sil, 1
	jne	.LBB0_1751
# %bb.27:
	cmp	edi, 6
	jg	.LBB0_131
# %bb.28:
	cmp	edi, 3
	jle	.LBB0_221
# %bb.29:
	cmp	edi, 4
	je	.LBB0_363
# %bb.30:
	cmp	edi, 5
	je	.LBB0_370
# %bb.31:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.32:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.33:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_672
# %bb.34:
	xor	esi, esi
.LBB0_35:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_37
.LBB0_36:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_36
.LBB0_37:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_38:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_38
	jmp	.LBB0_1751
.LBB0_39:
	cmp	sil, 7
	je	.LBB0_105
# %bb.40:
	cmp	sil, 8
	jne	.LBB0_1751
# %bb.41:
	cmp	edi, 6
	jg	.LBB0_142
# %bb.42:
	cmp	edi, 3
	jle	.LBB0_230
# %bb.43:
	cmp	edi, 4
	je	.LBB0_377
# %bb.44:
	cmp	edi, 5
	je	.LBB0_384
# %bb.45:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.46:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.47:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_677
# %bb.48:
	xor	esi, esi
.LBB0_49:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_51
.LBB0_50:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_50
.LBB0_51:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_52:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_52
	jmp	.LBB0_1751
.LBB0_53:
	cmp	edi, 6
	jg	.LBB0_153
# %bb.54:
	cmp	edi, 3
	jle	.LBB0_239
# %bb.55:
	cmp	edi, 4
	je	.LBB0_391
# %bb.56:
	cmp	edi, 5
	je	.LBB0_398
# %bb.57:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.58:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.59:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_682
# %bb.60:
	xor	esi, esi
.LBB0_61:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_63
.LBB0_62:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_62
.LBB0_63:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_64:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_64
	jmp	.LBB0_1751
.LBB0_65:
	cmp	edi, 6
	jg	.LBB0_164
# %bb.66:
	cmp	edi, 3
	jle	.LBB0_248
# %bb.67:
	cmp	edi, 4
	je	.LBB0_405
# %bb.68:
	cmp	edi, 5
	je	.LBB0_408
# %bb.69:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.70:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.71:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_72
# %bb.687:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB0_1093
# %bb.688:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB0_1093
.LBB0_72:
	xor	ecx, ecx
.LBB0_1217:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1219
.LBB0_1218:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1218
.LBB0_1219:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_1220:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1220
	jmp	.LBB0_1751
.LBB0_73:
	cmp	edi, 6
	jg	.LBB0_171
# %bb.74:
	cmp	edi, 3
	jle	.LBB0_253
# %bb.75:
	cmp	edi, 4
	je	.LBB0_411
# %bb.76:
	cmp	edi, 5
	je	.LBB0_418
# %bb.77:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.78:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.79:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_690
# %bb.80:
	xor	esi, esi
.LBB0_81:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_83
.LBB0_82:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_82
.LBB0_83:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_84:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_84
	jmp	.LBB0_1751
.LBB0_85:
	cmp	edi, 6
	jg	.LBB0_182
# %bb.86:
	cmp	edi, 3
	jle	.LBB0_262
# %bb.87:
	cmp	edi, 4
	je	.LBB0_425
# %bb.88:
	cmp	edi, 5
	je	.LBB0_428
# %bb.89:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.90:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.91:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_92
# %bb.695:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB0_1095
# %bb.696:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB0_1095
.LBB0_92:
	xor	ecx, ecx
.LBB0_1227:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1229
.LBB0_1228:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1228
.LBB0_1229:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_1230:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1230
	jmp	.LBB0_1751
.LBB0_93:
	cmp	edi, 6
	jg	.LBB0_189
# %bb.94:
	cmp	edi, 3
	jle	.LBB0_267
# %bb.95:
	cmp	edi, 4
	je	.LBB0_431
# %bb.96:
	cmp	edi, 5
	je	.LBB0_438
# %bb.97:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.98:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.99:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_698
# %bb.100:
	xor	esi, esi
.LBB0_101:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_103
.LBB0_102:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_102
.LBB0_103:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_104:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_104
	jmp	.LBB0_1751
.LBB0_105:
	cmp	edi, 6
	jg	.LBB0_200
# %bb.106:
	cmp	edi, 3
	jle	.LBB0_276
# %bb.107:
	cmp	edi, 4
	je	.LBB0_445
# %bb.108:
	cmp	edi, 5
	je	.LBB0_452
# %bb.109:
	cmp	edi, 6
	jne	.LBB0_1751
# %bb.110:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.111:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_703
# %bb.112:
	xor	esi, esi
.LBB0_113:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_115
.LBB0_114:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_114
.LBB0_115:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_116:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_116
	jmp	.LBB0_1751
.LBB0_117:
	cmp	edi, 8
	jle	.LBB0_285
# %bb.118:
	cmp	edi, 9
	je	.LBB0_459
# %bb.119:
	cmp	edi, 11
	je	.LBB0_462
# %bb.120:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.121:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.122:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_123
# %bb.708:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1097
# %bb.709:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1097
.LBB0_123:
	xor	ecx, ecx
.LBB0_1527:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1530
# %bb.1528:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1529:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1529
.LBB0_1530:
	cmp	rsi, 3
	jb	.LBB0_1751
# %bb.1531:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1532:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 8], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 16], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1532
	jmp	.LBB0_1751
.LBB0_124:
	cmp	edi, 8
	jle	.LBB0_290
# %bb.125:
	cmp	edi, 9
	je	.LBB0_465
# %bb.126:
	cmp	edi, 11
	je	.LBB0_468
# %bb.127:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.128:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.129:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_130
# %bb.711:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1100
# %bb.712:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1100
.LBB0_130:
	xor	ecx, ecx
.LBB0_1537:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1540
# %bb.1538:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1539:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1539
.LBB0_1540:
	cmp	rsi, 3
	jb	.LBB0_1751
# %bb.1541:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1542:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 8], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 16], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1542
	jmp	.LBB0_1751
.LBB0_131:
	cmp	edi, 8
	jle	.LBB0_295
# %bb.132:
	cmp	edi, 9
	je	.LBB0_471
# %bb.133:
	cmp	edi, 11
	je	.LBB0_478
# %bb.134:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.135:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.136:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_714
# %bb.137:
	xor	esi, esi
.LBB0_138:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_140
.LBB0_139:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_139
.LBB0_140:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_141:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	movsd	xmm0, qword ptr [rdx + 8*rsi + 8] # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm0
	movsd	xmm0, qword ptr [rdx + 8*rsi + 16] # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm0
	movsd	xmm0, qword ptr [rdx + 8*rsi + 24] # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_141
	jmp	.LBB0_1751
.LBB0_142:
	cmp	edi, 8
	jle	.LBB0_304
# %bb.143:
	cmp	edi, 9
	je	.LBB0_485
# %bb.144:
	cmp	edi, 11
	je	.LBB0_492
# %bb.145:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.146:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.147:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_719
# %bb.148:
	xor	esi, esi
.LBB0_149:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_151
.LBB0_150:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_150
.LBB0_151:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_152:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	movsd	xmm0, qword ptr [rdx + 8*rsi + 8] # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm0
	movsd	xmm0, qword ptr [rdx + 8*rsi + 16] # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm0
	movsd	xmm0, qword ptr [rdx + 8*rsi + 24] # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_152
	jmp	.LBB0_1751
.LBB0_153:
	cmp	edi, 8
	jle	.LBB0_313
# %bb.154:
	cmp	edi, 9
	je	.LBB0_499
# %bb.155:
	cmp	edi, 11
	je	.LBB0_502
# %bb.156:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.157:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.158:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_724
# %bb.159:
	xor	esi, esi
.LBB0_160:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_162
.LBB0_161:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_161
.LBB0_162:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_163:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_163
	jmp	.LBB0_1751
.LBB0_164:
	cmp	edi, 8
	jle	.LBB0_318
# %bb.165:
	cmp	edi, 9
	je	.LBB0_509
# %bb.166:
	cmp	edi, 11
	je	.LBB0_512
# %bb.167:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.168:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.169:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_170
# %bb.729:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_1103
# %bb.730:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_1103
.LBB0_170:
	xor	ecx, ecx
.LBB0_1547:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1549
.LBB0_1548:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1548
.LBB0_1549:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1550:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1550
	jmp	.LBB0_1751
.LBB0_171:
	cmp	edi, 8
	jle	.LBB0_323
# %bb.172:
	cmp	edi, 9
	je	.LBB0_515
# %bb.173:
	cmp	edi, 11
	je	.LBB0_518
# %bb.174:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.175:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.176:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_732
# %bb.177:
	xor	esi, esi
.LBB0_178:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_180
.LBB0_179:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_179
.LBB0_180:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_181:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_181
	jmp	.LBB0_1751
.LBB0_182:
	cmp	edi, 8
	jle	.LBB0_328
# %bb.183:
	cmp	edi, 9
	je	.LBB0_525
# %bb.184:
	cmp	edi, 11
	je	.LBB0_528
# %bb.185:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.186:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.187:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_188
# %bb.737:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_1106
# %bb.738:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_1106
.LBB0_188:
	xor	ecx, ecx
.LBB0_1555:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1557
.LBB0_1556:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1556
.LBB0_1557:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1558:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1558
	jmp	.LBB0_1751
.LBB0_189:
	cmp	edi, 8
	jle	.LBB0_333
# %bb.190:
	cmp	edi, 9
	je	.LBB0_531
# %bb.191:
	cmp	edi, 11
	je	.LBB0_538
# %bb.192:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.193:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.194:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_740
# %bb.195:
	xor	esi, esi
.LBB0_196:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_198
.LBB0_197:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_197
.LBB0_198:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_199:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_199
	jmp	.LBB0_1751
.LBB0_200:
	cmp	edi, 8
	jle	.LBB0_342
# %bb.201:
	cmp	edi, 9
	je	.LBB0_545
# %bb.202:
	cmp	edi, 11
	je	.LBB0_552
# %bb.203:
	cmp	edi, 12
	jne	.LBB0_1751
# %bb.204:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.205:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_745
# %bb.206:
	xor	esi, esi
.LBB0_207:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_209
.LBB0_208:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_208
.LBB0_209:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_210:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 16] # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_210
	jmp	.LBB0_1751
.LBB0_211:
	cmp	edi, 2
	je	.LBB0_559
# %bb.212:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.213:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.214:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_215
# %bb.750:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1109
# %bb.751:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1109
.LBB0_215:
	xor	ecx, ecx
.LBB0_1563:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1565
.LBB0_1564:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1564
.LBB0_1565:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1566:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1566
	jmp	.LBB0_1751
.LBB0_216:
	cmp	edi, 2
	je	.LBB0_562
# %bb.217:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.218:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.219:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_220
# %bb.753:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1112
# %bb.754:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1112
.LBB0_220:
	xor	ecx, ecx
.LBB0_1571:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1573
.LBB0_1572:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1572
.LBB0_1573:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1574:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1574
	jmp	.LBB0_1751
.LBB0_221:
	cmp	edi, 2
	je	.LBB0_565
# %bb.222:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.223:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.224:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_756
# %bb.225:
	xor	esi, esi
.LBB0_226:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_228
.LBB0_227:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_227
.LBB0_228:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_229:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_229
	jmp	.LBB0_1751
.LBB0_230:
	cmp	edi, 2
	je	.LBB0_572
# %bb.231:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.232:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.233:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_761
# %bb.234:
	xor	esi, esi
.LBB0_235:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_237
.LBB0_236:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_236
.LBB0_237:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_238:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_238
	jmp	.LBB0_1751
.LBB0_239:
	cmp	edi, 2
	je	.LBB0_579
# %bb.240:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.241:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.242:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_766
# %bb.243:
	xor	edi, edi
.LBB0_244:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_246
.LBB0_245:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_245
.LBB0_246:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_247:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_247
	jmp	.LBB0_1751
.LBB0_248:
	cmp	edi, 2
	je	.LBB0_586
# %bb.249:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.250:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.251:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_252
# %bb.771:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1115
# %bb.772:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1115
.LBB0_252:
	xor	ecx, ecx
.LBB0_1579:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB0_1581
# %bb.1580:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_1581:
	add	rsi, r10
	je	.LBB0_1751
.LBB0_1582:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rcx]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx], al
	movsx	eax, byte ptr [rdx + rcx + 1]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx + 1], al
	add	rcx, 2
	cmp	r10, rcx
	jne	.LBB0_1582
	jmp	.LBB0_1751
.LBB0_253:
	cmp	edi, 2
	je	.LBB0_589
# %bb.254:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.255:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.256:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_774
# %bb.257:
	xor	edi, edi
.LBB0_258:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_260
.LBB0_259:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_259
.LBB0_260:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_261:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_261
	jmp	.LBB0_1751
.LBB0_262:
	cmp	edi, 2
	je	.LBB0_596
# %bb.263:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.264:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.265:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_266
# %bb.779:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1118
# %bb.780:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1118
.LBB0_266:
	xor	ecx, ecx
.LBB0_1587:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB0_1589
# %bb.1588:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_1589:
	add	rsi, r10
	je	.LBB0_1751
.LBB0_1590:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rcx]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx], al
	movsx	eax, byte ptr [rdx + rcx + 1]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx + 1], al
	add	rcx, 2
	cmp	r10, rcx
	jne	.LBB0_1590
	jmp	.LBB0_1751
.LBB0_267:
	cmp	edi, 2
	je	.LBB0_599
# %bb.268:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.269:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.270:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_782
# %bb.271:
	xor	esi, esi
.LBB0_272:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_274
.LBB0_273:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_273
.LBB0_274:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_275:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_275
	jmp	.LBB0_1751
.LBB0_276:
	cmp	edi, 2
	je	.LBB0_606
# %bb.277:
	cmp	edi, 3
	jne	.LBB0_1751
# %bb.278:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.279:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_787
# %bb.280:
	xor	esi, esi
.LBB0_281:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_283
.LBB0_282:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_282
.LBB0_283:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_284:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_284
	jmp	.LBB0_1751
.LBB0_285:
	cmp	edi, 7
	je	.LBB0_613
# %bb.286:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.287:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.288:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_289
# %bb.792:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1121
# %bb.793:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1121
.LBB0_289:
	xor	ecx, ecx
.LBB0_1595:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1597
.LBB0_1596:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1596
.LBB0_1597:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1598:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1598
	jmp	.LBB0_1751
.LBB0_290:
	cmp	edi, 7
	je	.LBB0_616
# %bb.291:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.292:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.293:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB0_795
# %bb.294:
	xor	ecx, ecx
	jmp	.LBB0_1130
.LBB0_295:
	cmp	edi, 7
	je	.LBB0_619
# %bb.296:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.297:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.298:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_797
# %bb.299:
	xor	esi, esi
.LBB0_300:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_302
.LBB0_301:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_301
.LBB0_302:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_303:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_303
	jmp	.LBB0_1751
.LBB0_304:
	cmp	edi, 7
	je	.LBB0_626
# %bb.305:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.306:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.307:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_802
# %bb.308:
	xor	esi, esi
.LBB0_309:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_311
.LBB0_310:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_310
.LBB0_311:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_312:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_312
	jmp	.LBB0_1751
.LBB0_313:
	cmp	edi, 7
	je	.LBB0_633
# %bb.314:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.315:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.316:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_807
# %bb.317:
	xor	edi, edi
	jmp	.LBB0_809
.LBB0_318:
	cmp	edi, 7
	je	.LBB0_640
# %bb.319:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.320:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.321:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_322
# %bb.812:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_1131
# %bb.813:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_1131
.LBB0_322:
	xor	ecx, ecx
.LBB0_1237:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1239
.LBB0_1238:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1238
.LBB0_1239:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_1240:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1240
	jmp	.LBB0_1751
.LBB0_323:
	cmp	edi, 7
	je	.LBB0_643
# %bb.324:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.325:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.326:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_815
# %bb.327:
	xor	edi, edi
	jmp	.LBB0_817
.LBB0_328:
	cmp	edi, 7
	je	.LBB0_650
# %bb.329:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.330:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.331:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_332
# %bb.820:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_1133
# %bb.821:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_1133
.LBB0_332:
	xor	ecx, ecx
.LBB0_1247:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1249
.LBB0_1248:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1248
.LBB0_1249:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_1250:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1250
	jmp	.LBB0_1751
.LBB0_333:
	cmp	edi, 7
	je	.LBB0_653
# %bb.334:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.335:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.336:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_823
# %bb.337:
	xor	esi, esi
.LBB0_338:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_340
.LBB0_339:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_339
.LBB0_340:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_341:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_341
	jmp	.LBB0_1751
.LBB0_342:
	cmp	edi, 7
	je	.LBB0_660
# %bb.343:
	cmp	edi, 8
	jne	.LBB0_1751
# %bb.344:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.345:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_828
# %bb.346:
	xor	esi, esi
.LBB0_347:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_349
.LBB0_348:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_348
.LBB0_349:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_350:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_350
	jmp	.LBB0_1751
.LBB0_351:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.352:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_353
# %bb.833:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1135
# %bb.834:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1135
.LBB0_353:
	xor	ecx, ecx
.LBB0_1603:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1605
.LBB0_1604:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1604
.LBB0_1605:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1606:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1606
	jmp	.LBB0_1751
.LBB0_354:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.355:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_356
# %bb.836:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1138
# %bb.837:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1138
.LBB0_356:
	xor	ecx, ecx
.LBB0_1611:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1613
.LBB0_1612:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1612
.LBB0_1613:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1614:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1614
	jmp	.LBB0_1751
.LBB0_357:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.358:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB0_839
# %bb.359:
	xor	ecx, ecx
	jmp	.LBB0_1147
.LBB0_360:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.361:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_362
# %bb.841:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1148
# %bb.842:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1148
.LBB0_362:
	xor	ecx, ecx
.LBB0_1619:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1621
.LBB0_1620:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1620
.LBB0_1621:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1622:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1622
	jmp	.LBB0_1751
.LBB0_363:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.364:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_844
# %bb.365:
	xor	esi, esi
.LBB0_366:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_368
.LBB0_367:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_367
.LBB0_368:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_369:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_369
	jmp	.LBB0_1751
.LBB0_370:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.371:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_849
# %bb.372:
	xor	esi, esi
.LBB0_373:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_375
.LBB0_374:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_374
.LBB0_375:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_376:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_376
	jmp	.LBB0_1751
.LBB0_377:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.378:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_854
# %bb.379:
	xor	esi, esi
.LBB0_380:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_382
.LBB0_381:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_381
.LBB0_382:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_383:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_383
	jmp	.LBB0_1751
.LBB0_384:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.385:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_859
# %bb.386:
	xor	esi, esi
.LBB0_387:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_389
.LBB0_388:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_388
.LBB0_389:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_390:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_390
	jmp	.LBB0_1751
.LBB0_391:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.392:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_864
# %bb.393:
	xor	esi, esi
.LBB0_394:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_396
.LBB0_395:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_395
.LBB0_396:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_397:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_397
	jmp	.LBB0_1751
.LBB0_398:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.399:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_869
# %bb.400:
	xor	esi, esi
.LBB0_401:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_403
.LBB0_402:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_402
.LBB0_403:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_404:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_404
	jmp	.LBB0_1751
.LBB0_405:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.406:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_407
# %bb.874:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_1151
# %bb.875:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_1151
.LBB0_407:
	xor	ecx, ecx
.LBB0_1257:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1259
.LBB0_1258:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1258
.LBB0_1259:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_1260:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1260
	jmp	.LBB0_1751
.LBB0_408:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.409:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_410
# %bb.877:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1153
# %bb.878:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1153
.LBB0_410:
	xor	ecx, ecx
.LBB0_1627:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1629
# %bb.1628:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1629:
	add	rsi, rax
	je	.LBB0_1751
.LBB0_1630:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1630
	jmp	.LBB0_1751
.LBB0_411:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.412:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_880
# %bb.413:
	xor	esi, esi
.LBB0_414:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_416
.LBB0_415:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_415
.LBB0_416:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_417:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_417
	jmp	.LBB0_1751
.LBB0_418:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.419:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_885
# %bb.420:
	xor	esi, esi
.LBB0_421:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_423
.LBB0_422:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_422
.LBB0_423:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_424:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_424
	jmp	.LBB0_1751
.LBB0_425:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.426:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_427
# %bb.890:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_1156
# %bb.891:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_1156
.LBB0_427:
	xor	ecx, ecx
.LBB0_1267:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1269
.LBB0_1268:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1268
.LBB0_1269:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_1270:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1270
	jmp	.LBB0_1751
.LBB0_428:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.429:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_430
# %bb.893:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1158
# %bb.894:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1158
.LBB0_430:
	xor	ecx, ecx
.LBB0_1635:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1637
# %bb.1636:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1637:
	add	rsi, rax
	je	.LBB0_1751
.LBB0_1638:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1638
	jmp	.LBB0_1751
.LBB0_431:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.432:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_896
# %bb.433:
	xor	esi, esi
.LBB0_434:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_436
.LBB0_435:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_435
.LBB0_436:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_437:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_437
	jmp	.LBB0_1751
.LBB0_438:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.439:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_901
# %bb.440:
	xor	esi, esi
.LBB0_441:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_443
.LBB0_442:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_442
.LBB0_443:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_444:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_444
	jmp	.LBB0_1751
.LBB0_445:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.446:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_906
# %bb.447:
	xor	esi, esi
.LBB0_448:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_450
.LBB0_449:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_449
.LBB0_450:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_451:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_451
	jmp	.LBB0_1751
.LBB0_452:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.453:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_911
# %bb.454:
	xor	esi, esi
.LBB0_455:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_457
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_456
.LBB0_457:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_458:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_458
	jmp	.LBB0_1751
.LBB0_459:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.460:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_461
# %bb.916:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1161
# %bb.917:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1161
.LBB0_461:
	xor	ecx, ecx
.LBB0_1643:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1645
.LBB0_1644:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1644
.LBB0_1645:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1646:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1646
	jmp	.LBB0_1751
.LBB0_462:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.463:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_464
# %bb.919:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1164
# %bb.920:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1164
.LBB0_464:
	xor	ecx, ecx
.LBB0_1651:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1654
# %bb.1652:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1653:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1653
.LBB0_1654:
	cmp	rsi, 3
	jb	.LBB0_1751
# %bb.1655:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1656:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 4], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 8], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1656
	jmp	.LBB0_1751
.LBB0_465:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.466:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_467
# %bb.922:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1167
# %bb.923:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1167
.LBB0_467:
	xor	ecx, ecx
.LBB0_1661:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1663
.LBB0_1662:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1662
.LBB0_1663:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1664:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1664
	jmp	.LBB0_1751
.LBB0_468:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.469:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_470
# %bb.925:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1170
# %bb.926:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1170
.LBB0_470:
	xor	ecx, ecx
.LBB0_1669:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1672
# %bb.1670:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1671:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1671
.LBB0_1672:
	cmp	rsi, 3
	jb	.LBB0_1751
# %bb.1673:
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1674:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 4], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 8], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1674
	jmp	.LBB0_1751
.LBB0_471:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.472:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_928
# %bb.473:
	xor	esi, esi
.LBB0_474:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_476
.LBB0_475:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_475
.LBB0_476:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_477:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_477
	jmp	.LBB0_1751
.LBB0_478:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.479:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_933
# %bb.480:
	xor	esi, esi
.LBB0_481:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_483
.LBB0_482:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_482
.LBB0_483:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_484:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	movss	xmm0, dword ptr [rdx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm0
	movss	xmm0, dword ptr [rdx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm0
	movss	xmm0, dword ptr [rdx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_484
	jmp	.LBB0_1751
.LBB0_485:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.486:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_938
# %bb.487:
	xor	esi, esi
.LBB0_488:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_490
.LBB0_489:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_489
.LBB0_490:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_491:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_491
	jmp	.LBB0_1751
.LBB0_492:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.493:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_943
# %bb.494:
	xor	esi, esi
.LBB0_495:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_497
.LBB0_496:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_496
.LBB0_497:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_498:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	movss	xmm0, dword ptr [rdx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm0
	movss	xmm0, dword ptr [rdx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm0
	movss	xmm0, dword ptr [rdx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_498
	jmp	.LBB0_1751
.LBB0_499:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.500:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_948
# %bb.501:
	xor	edi, edi
	jmp	.LBB0_950
.LBB0_502:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.503:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_953
# %bb.504:
	xor	esi, esi
.LBB0_505:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_507
.LBB0_506:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_506
.LBB0_507:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_508:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_508
	jmp	.LBB0_1751
.LBB0_509:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.510:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_511
# %bb.958:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1173
# %bb.959:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1173
.LBB0_511:
	xor	ecx, ecx
.LBB0_1679:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1681
# %bb.1680:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1681:
	add	rsi, rax
	je	.LBB0_1751
.LBB0_1682:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1682
	jmp	.LBB0_1751
.LBB0_512:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.513:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_514
# %bb.961:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_1176
# %bb.962:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_1176
.LBB0_514:
	xor	ecx, ecx
.LBB0_1687:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1690
# %bb.1688:
	mov	esi, 2147483647
.LBB0_1689:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1689
.LBB0_1690:
	cmp	r9, 3
	jb	.LBB0_1751
# %bb.1691:
	mov	esi, 2147483647
.LBB0_1692:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1692
	jmp	.LBB0_1751
.LBB0_515:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.516:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_964
# %bb.517:
	xor	edi, edi
	jmp	.LBB0_966
.LBB0_518:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.519:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_969
# %bb.520:
	xor	esi, esi
.LBB0_521:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_523
.LBB0_522:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_522
.LBB0_523:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_524:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_524
	jmp	.LBB0_1751
.LBB0_525:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.526:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_527
# %bb.974:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1179
# %bb.975:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1179
.LBB0_527:
	xor	ecx, ecx
.LBB0_1697:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1699
# %bb.1698:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1699:
	add	rsi, rax
	je	.LBB0_1751
.LBB0_1700:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1700
	jmp	.LBB0_1751
.LBB0_528:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.529:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_530
# %bb.977:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_1182
# %bb.978:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_1182
.LBB0_530:
	xor	ecx, ecx
.LBB0_1705:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1708
# %bb.1706:
	mov	esi, 2147483647
.LBB0_1707:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1707
.LBB0_1708:
	cmp	r9, 3
	jb	.LBB0_1751
# %bb.1709:
	mov	esi, 2147483647
.LBB0_1710:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1710
	jmp	.LBB0_1751
.LBB0_531:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.532:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_980
# %bb.533:
	xor	esi, esi
.LBB0_534:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_536
.LBB0_535:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_535
.LBB0_536:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_537:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_537
	jmp	.LBB0_1751
.LBB0_538:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.539:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_985
# %bb.540:
	xor	esi, esi
.LBB0_541:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_543
.LBB0_542:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_542
.LBB0_543:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_544:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_544
	jmp	.LBB0_1751
.LBB0_545:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.546:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_990
# %bb.547:
	xor	esi, esi
.LBB0_548:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_550
.LBB0_549:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_549
.LBB0_550:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_551:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_551
	jmp	.LBB0_1751
.LBB0_552:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.553:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_995
# %bb.554:
	xor	esi, esi
.LBB0_555:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_557
.LBB0_556:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_556
.LBB0_557:
	cmp	rax, 3
	jb	.LBB0_1751
.LBB0_558:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 4] # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 8] # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm0
	movss	xmm0, dword ptr [rcx + 4*rsi + 12] # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB0_558
	jmp	.LBB0_1751
.LBB0_559:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.560:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_561
# %bb.1000:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1185
# %bb.1001:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1185
.LBB0_561:
	xor	ecx, ecx
.LBB0_1715:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1717
.LBB0_1716:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1716
.LBB0_1717:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1718:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB0_1718
	jmp	.LBB0_1751
.LBB0_562:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.563:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB0_1003
# %bb.564:
	xor	ecx, ecx
	jmp	.LBB0_1194
.LBB0_565:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.566:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1005
# %bb.567:
	xor	esi, esi
.LBB0_568:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_570
.LBB0_569:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_569
.LBB0_570:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_571:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_571
	jmp	.LBB0_1751
.LBB0_572:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.573:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1010
# %bb.574:
	xor	esi, esi
.LBB0_575:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_577
.LBB0_576:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_576
.LBB0_577:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_578:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_578
	jmp	.LBB0_1751
.LBB0_579:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.580:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1015
# %bb.581:
	xor	edi, edi
.LBB0_582:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_584
.LBB0_583:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_583
.LBB0_584:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_585:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_585
	jmp	.LBB0_1751
.LBB0_586:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.587:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_588
# %bb.1020:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB0_1195
# %bb.1021:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB0_1195
.LBB0_588:
	xor	ecx, ecx
.LBB0_1277:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1279
.LBB0_1278:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1278
.LBB0_1279:
	cmp	rdi, 3
	jb	.LBB0_1751
.LBB0_1280:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1280
	jmp	.LBB0_1751
.LBB0_589:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.590:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1023
# %bb.591:
	xor	edi, edi
.LBB0_592:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_594
.LBB0_593:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_593
.LBB0_594:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_595:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_595
	jmp	.LBB0_1751
.LBB0_596:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.597:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_598
# %bb.1028:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB0_1197
# %bb.1029:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB0_1197
.LBB0_598:
	xor	ecx, ecx
.LBB0_1287:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1289
.LBB0_1288:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1288
.LBB0_1289:
	cmp	rdi, 3
	jb	.LBB0_1751
.LBB0_1290:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1290
	jmp	.LBB0_1751
.LBB0_599:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.600:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1031
# %bb.601:
	xor	esi, esi
.LBB0_602:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_604
.LBB0_603:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_603
.LBB0_604:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_605
	jmp	.LBB0_1751
.LBB0_606:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.607:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1036
# %bb.608:
	xor	esi, esi
.LBB0_609:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_611
.LBB0_610:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_610
.LBB0_611:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_612:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_612
	jmp	.LBB0_1751
.LBB0_613:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.614:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_615
# %bb.1041:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1199
# %bb.1042:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1199
.LBB0_615:
	xor	ecx, ecx
.LBB0_1723:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1725
.LBB0_1724:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1724
.LBB0_1725:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1726:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1726
	jmp	.LBB0_1751
.LBB0_616:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.617:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_618
# %bb.1044:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1202
# %bb.1045:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1202
.LBB0_618:
	xor	ecx, ecx
.LBB0_1731:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1733
.LBB0_1732:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1732
.LBB0_1733:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_1734:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1734
	jmp	.LBB0_1751
.LBB0_619:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.620:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_1047
# %bb.621:
	xor	esi, esi
.LBB0_622:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_624
.LBB0_623:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_623
.LBB0_624:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_625:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_625
	jmp	.LBB0_1751
.LBB0_626:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.627:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_1052
# %bb.628:
	xor	esi, esi
.LBB0_629:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_631
.LBB0_630:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_630
.LBB0_631:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_632:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_632
	jmp	.LBB0_1751
.LBB0_633:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.634:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_1057
# %bb.635:
	xor	esi, esi
.LBB0_636:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_638
.LBB0_637:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_637
.LBB0_638:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_639:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_639
	jmp	.LBB0_1751
.LBB0_640:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.641:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_642
# %bb.1062:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1205
# %bb.1063:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1205
.LBB0_642:
	xor	ecx, ecx
.LBB0_1739:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1741
# %bb.1740:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1741:
	add	rsi, rax
	je	.LBB0_1751
.LBB0_1742:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1742
	jmp	.LBB0_1751
.LBB0_643:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.644:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_1065
# %bb.645:
	xor	esi, esi
.LBB0_646:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_648
.LBB0_647:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_647
.LBB0_648:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_649:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_649
	jmp	.LBB0_1751
.LBB0_650:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.651:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_652
# %bb.1070:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1208
# %bb.1071:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1208
.LBB0_652:
	xor	ecx, ecx
.LBB0_1747:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1749
# %bb.1748:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1749:
	add	rsi, rax
	je	.LBB0_1751
.LBB0_1750:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1750
	jmp	.LBB0_1751
.LBB0_653:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.654:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_1073
# %bb.655:
	xor	esi, esi
.LBB0_656:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_658
.LBB0_657:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_657
.LBB0_658:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_659:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_659
	jmp	.LBB0_1751
.LBB0_660:
	test	r9d, r9d
	jle	.LBB0_1751
# %bb.661:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_1078
# %bb.662:
	xor	esi, esi
.LBB0_663:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_665
.LBB0_664:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_664
.LBB0_665:
	cmp	r9, 3
	jb	.LBB0_1751
.LBB0_666:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_666
	jmp	.LBB0_1751
.LBB0_670:
	mov	ecx, eax
	and	ecx, -8
	lea	rdi, [rcx - 8]
	mov	rsi, rdi
	shr	rsi, 3
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 56
	jae	.LBB0_1086
# %bb.671:
	xor	edi, edi
	jmp	.LBB0_1088
.LBB0_672:
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
	jne	.LBB0_35
# %bb.673:
	and	al, dil
	jne	.LBB0_35
# %bb.674:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1291
# %bb.675:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_676:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_676
	jmp	.LBB0_1292
.LBB0_677:
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
	jne	.LBB0_49
# %bb.678:
	and	al, dil
	jne	.LBB0_49
# %bb.679:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1295
# %bb.680:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_681
	jmp	.LBB0_1296
.LBB0_682:
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
	jne	.LBB0_61
# %bb.683:
	and	al, dil
	jne	.LBB0_61
# %bb.684:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1299
# %bb.685:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_686:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_686
	jmp	.LBB0_1300
.LBB0_690:
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
	jne	.LBB0_81
# %bb.691:
	and	al, dil
	jne	.LBB0_81
# %bb.692:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1303
# %bb.693:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_694:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_694
	jmp	.LBB0_1304
.LBB0_698:
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
	jne	.LBB0_101
# %bb.699:
	and	al, dil
	jne	.LBB0_101
# %bb.700:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1307
# %bb.701:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_702:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_702
	jmp	.LBB0_1308
.LBB0_703:
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
	jne	.LBB0_113
# %bb.704:
	and	al, dil
	jne	.LBB0_113
# %bb.705:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1311
# %bb.706:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_707:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_707
	jmp	.LBB0_1312
.LBB0_714:
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
	jne	.LBB0_138
# %bb.715:
	and	al, dil
	jne	.LBB0_138
# %bb.716:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1315
# %bb.717:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_718:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_718
	jmp	.LBB0_1316
.LBB0_719:
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
	jne	.LBB0_149
# %bb.720:
	and	al, dil
	jne	.LBB0_149
# %bb.721:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1319
# %bb.722:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_723:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_723
	jmp	.LBB0_1320
.LBB0_724:
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
	jne	.LBB0_160
# %bb.725:
	and	al, dil
	jne	.LBB0_160
# %bb.726:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1323
# %bb.727:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_728:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_728
	jmp	.LBB0_1324
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
	jne	.LBB0_178
# %bb.733:
	and	al, dil
	jne	.LBB0_178
# %bb.734:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1327
# %bb.735:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_736:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_736
	jmp	.LBB0_1328
.LBB0_740:
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
	jne	.LBB0_196
# %bb.741:
	and	al, dil
	jne	.LBB0_196
# %bb.742:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1331
# %bb.743:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_744:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_744
	jmp	.LBB0_1332
.LBB0_745:
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
	jne	.LBB0_207
# %bb.746:
	and	al, dil
	jne	.LBB0_207
# %bb.747:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1335
# %bb.748:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_749:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_749
	jmp	.LBB0_1336
.LBB0_756:
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
	jne	.LBB0_226
# %bb.757:
	and	al, dil
	jne	.LBB0_226
# %bb.758:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1339
# %bb.759:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_760:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_760
	jmp	.LBB0_1340
.LBB0_761:
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
	jne	.LBB0_235
# %bb.762:
	and	al, dil
	jne	.LBB0_235
# %bb.763:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1343
# %bb.764:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_765:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_765
	jmp	.LBB0_1344
.LBB0_766:
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
	jne	.LBB0_244
# %bb.767:
	and	al, sil
	jne	.LBB0_244
# %bb.768:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1347
# %bb.769:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_770:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm4, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax], xmm6
	movdqu	xmmword ptr [r8 + rax + 16], xmm3
	movdqu	xmm1, xmmword ptr [rdx + rax + 32]
	movdqu	xmm2, xmmword ptr [rdx + rax + 48]
	movdqu	xmm3, xmmword ptr [rcx + rax + 32]
	movdqu	xmm4, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax + 32], xmm6
	movdqu	xmmword ptr [r8 + rax + 48], xmm3
	add	rax, 64
	add	rsi, 2
	jne	.LBB0_770
	jmp	.LBB0_1348
.LBB0_774:
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
	jne	.LBB0_258
# %bb.775:
	and	al, sil
	jne	.LBB0_258
# %bb.776:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1351
# %bb.777:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_778:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm4, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax], xmm6
	movdqu	xmmword ptr [r8 + rax + 16], xmm3
	movdqu	xmm1, xmmword ptr [rdx + rax + 32]
	movdqu	xmm2, xmmword ptr [rdx + rax + 48]
	movdqu	xmm3, xmmword ptr [rcx + rax + 32]
	movdqu	xmm4, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax + 32], xmm6
	movdqu	xmmword ptr [r8 + rax + 48], xmm3
	add	rax, 64
	add	rsi, 2
	jne	.LBB0_778
	jmp	.LBB0_1352
.LBB0_782:
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
	jne	.LBB0_272
# %bb.783:
	and	al, dil
	jne	.LBB0_272
# %bb.784:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1355
# %bb.785:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_786:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_786
	jmp	.LBB0_1356
.LBB0_787:
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
	jne	.LBB0_281
# %bb.788:
	and	al, dil
	jne	.LBB0_281
# %bb.789:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1359
# %bb.790:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_791:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_791
	jmp	.LBB0_1360
.LBB0_795:
	mov	ecx, eax
	and	ecx, -4
	lea	rdi, [rcx - 4]
	mov	rsi, rdi
	shr	rsi, 2
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 28
	jae	.LBB0_1124
# %bb.796:
	xor	edi, edi
	jmp	.LBB0_1126
.LBB0_797:
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
	jne	.LBB0_300
# %bb.798:
	and	al, dil
	jne	.LBB0_300
# %bb.799:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1363
# %bb.800:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_801:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_801
	jmp	.LBB0_1364
.LBB0_802:
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
	jne	.LBB0_309
# %bb.803:
	and	al, dil
	jne	.LBB0_309
# %bb.804:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1367
# %bb.805:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_806:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_806
	jmp	.LBB0_1368
.LBB0_807:
	and	esi, -4
	xor	edi, edi
.LBB0_808:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [r8 + 8*rdi], rax
	mov	rax, qword ptr [rcx + 8*rdi + 8]
	imul	rax, qword ptr [rdx + 8*rdi + 8]
	mov	qword ptr [r8 + 8*rdi + 8], rax
	mov	rax, qword ptr [rcx + 8*rdi + 16]
	imul	rax, qword ptr [rdx + 8*rdi + 16]
	mov	qword ptr [r8 + 8*rdi + 16], rax
	mov	rax, qword ptr [rcx + 8*rdi + 24]
	imul	rax, qword ptr [rdx + 8*rdi + 24]
	mov	qword ptr [r8 + 8*rdi + 24], rax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_808
.LBB0_809:
	test	r9, r9
	je	.LBB0_1751
# %bb.810:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_811:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_811
	jmp	.LBB0_1751
.LBB0_815:
	and	esi, -4
	xor	edi, edi
.LBB0_816:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [r8 + 8*rdi], rax
	mov	rax, qword ptr [rcx + 8*rdi + 8]
	imul	rax, qword ptr [rdx + 8*rdi + 8]
	mov	qword ptr [r8 + 8*rdi + 8], rax
	mov	rax, qword ptr [rcx + 8*rdi + 16]
	imul	rax, qword ptr [rdx + 8*rdi + 16]
	mov	qword ptr [r8 + 8*rdi + 16], rax
	mov	rax, qword ptr [rcx + 8*rdi + 24]
	imul	rax, qword ptr [rdx + 8*rdi + 24]
	mov	qword ptr [r8 + 8*rdi + 24], rax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_816
.LBB0_817:
	test	r9, r9
	je	.LBB0_1751
# %bb.818:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_819:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_819
	jmp	.LBB0_1751
.LBB0_823:
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
	jne	.LBB0_338
# %bb.824:
	and	al, dil
	jne	.LBB0_338
# %bb.825:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1371
# %bb.826:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_827:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_827
	jmp	.LBB0_1372
.LBB0_828:
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
	jne	.LBB0_347
# %bb.829:
	and	al, dil
	jne	.LBB0_347
# %bb.830:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1375
# %bb.831:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_832:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_832
	jmp	.LBB0_1376
.LBB0_839:
	mov	ecx, eax
	and	ecx, -16
	lea	rdi, [rcx - 16]
	mov	rsi, rdi
	shr	rsi, 4
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 112
	jae	.LBB0_1141
# %bb.840:
	xor	edi, edi
	jmp	.LBB0_1143
.LBB0_844:
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
	jne	.LBB0_366
# %bb.845:
	and	al, dil
	jne	.LBB0_366
# %bb.846:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1379
# %bb.847:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_848:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_848
	jmp	.LBB0_1380
.LBB0_849:
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
	jne	.LBB0_373
# %bb.850:
	and	al, dil
	jne	.LBB0_373
# %bb.851:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1383
# %bb.852:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_853:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_853
	jmp	.LBB0_1384
.LBB0_854:
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
	jne	.LBB0_380
# %bb.855:
	and	al, dil
	jne	.LBB0_380
# %bb.856:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1387
# %bb.857:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_858:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_858
	jmp	.LBB0_1388
.LBB0_859:
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
	jne	.LBB0_387
# %bb.860:
	and	al, dil
	jne	.LBB0_387
# %bb.861:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1391
# %bb.862:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_863:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_863
	jmp	.LBB0_1392
.LBB0_864:
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
	jne	.LBB0_394
# %bb.865:
	and	al, dil
	jne	.LBB0_394
# %bb.866:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1395
# %bb.867:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_868:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_868
	jmp	.LBB0_1396
.LBB0_869:
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
	jne	.LBB0_401
# %bb.870:
	and	al, dil
	jne	.LBB0_401
# %bb.871:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1399
# %bb.872:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_873:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_873
	jmp	.LBB0_1400
.LBB0_880:
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
	jne	.LBB0_414
# %bb.881:
	and	al, dil
	jne	.LBB0_414
# %bb.882:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1403
# %bb.883:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_884:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_884
	jmp	.LBB0_1404
.LBB0_885:
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
	jne	.LBB0_421
# %bb.886:
	and	al, dil
	jne	.LBB0_421
# %bb.887:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1407
# %bb.888:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_889:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_889
	jmp	.LBB0_1408
.LBB0_896:
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
	jne	.LBB0_434
# %bb.897:
	and	al, dil
	jne	.LBB0_434
# %bb.898:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1411
# %bb.899:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_900:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_900
	jmp	.LBB0_1412
.LBB0_901:
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
	jne	.LBB0_441
# %bb.902:
	and	al, dil
	jne	.LBB0_441
# %bb.903:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1415
# %bb.904:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_905:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_905
	jmp	.LBB0_1416
.LBB0_906:
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
	jne	.LBB0_448
# %bb.907:
	and	al, dil
	jne	.LBB0_448
# %bb.908:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1419
# %bb.909:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_910:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_910
	jmp	.LBB0_1420
.LBB0_911:
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
	jne	.LBB0_455
# %bb.912:
	and	al, dil
	jne	.LBB0_455
# %bb.913:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1423
# %bb.914:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_915:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_915
	jmp	.LBB0_1424
.LBB0_928:
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
# %bb.929:
	and	al, dil
	jne	.LBB0_474
# %bb.930:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1427
# %bb.931:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_932:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_932
	jmp	.LBB0_1428
.LBB0_933:
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
# %bb.934:
	and	al, dil
	jne	.LBB0_481
# %bb.935:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1431
# %bb.936:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_937:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_937
	jmp	.LBB0_1432
.LBB0_938:
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
# %bb.939:
	and	al, dil
	jne	.LBB0_488
# %bb.940:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1435
# %bb.941:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_942:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_942
	jmp	.LBB0_1436
.LBB0_943:
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
# %bb.944:
	and	al, dil
	jne	.LBB0_495
# %bb.945:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1439
# %bb.946:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_947:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_947
	jmp	.LBB0_1440
.LBB0_948:
	and	esi, -4
	xor	edi, edi
.LBB0_949:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [r8 + 8*rdi], rax
	mov	rax, qword ptr [rcx + 8*rdi + 8]
	imul	rax, qword ptr [rdx + 8*rdi + 8]
	mov	qword ptr [r8 + 8*rdi + 8], rax
	mov	rax, qword ptr [rcx + 8*rdi + 16]
	imul	rax, qword ptr [rdx + 8*rdi + 16]
	mov	qword ptr [r8 + 8*rdi + 16], rax
	mov	rax, qword ptr [rcx + 8*rdi + 24]
	imul	rax, qword ptr [rdx + 8*rdi + 24]
	mov	qword ptr [r8 + 8*rdi + 24], rax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_949
.LBB0_950:
	test	r9, r9
	je	.LBB0_1751
# %bb.951:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_952:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_952
	jmp	.LBB0_1751
.LBB0_953:
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
	jne	.LBB0_505
# %bb.954:
	and	al, dil
	jne	.LBB0_505
# %bb.955:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1443
# %bb.956:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_957:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_957
	jmp	.LBB0_1444
.LBB0_964:
	and	esi, -4
	xor	edi, edi
.LBB0_965:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [r8 + 8*rdi], rax
	mov	rax, qword ptr [rcx + 8*rdi + 8]
	imul	rax, qword ptr [rdx + 8*rdi + 8]
	mov	qword ptr [r8 + 8*rdi + 8], rax
	mov	rax, qword ptr [rcx + 8*rdi + 16]
	imul	rax, qword ptr [rdx + 8*rdi + 16]
	mov	qword ptr [r8 + 8*rdi + 16], rax
	mov	rax, qword ptr [rcx + 8*rdi + 24]
	imul	rax, qword ptr [rdx + 8*rdi + 24]
	mov	qword ptr [r8 + 8*rdi + 24], rax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_965
.LBB0_966:
	test	r9, r9
	je	.LBB0_1751
# %bb.967:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_968:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_968
	jmp	.LBB0_1751
.LBB0_969:
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
	jne	.LBB0_521
# %bb.970:
	and	al, dil
	jne	.LBB0_521
# %bb.971:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1447
# %bb.972:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_973:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_973
	jmp	.LBB0_1448
.LBB0_980:
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
	jne	.LBB0_534
# %bb.981:
	and	al, dil
	jne	.LBB0_534
# %bb.982:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1451
# %bb.983:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_984:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_984
	jmp	.LBB0_1452
.LBB0_985:
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
	jne	.LBB0_541
# %bb.986:
	and	al, dil
	jne	.LBB0_541
# %bb.987:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1455
# %bb.988:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_989:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_989
	jmp	.LBB0_1456
.LBB0_990:
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
	jne	.LBB0_548
# %bb.991:
	and	al, dil
	jne	.LBB0_548
# %bb.992:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1459
# %bb.993:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_994:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_994
	jmp	.LBB0_1460
.LBB0_995:
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
	jne	.LBB0_555
# %bb.996:
	and	al, dil
	jne	.LBB0_555
# %bb.997:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1463
# %bb.998:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_999:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_999
	jmp	.LBB0_1464
.LBB0_1003:
	mov	ecx, eax
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 224
	jae	.LBB0_1188
# %bb.1004:
	xor	edi, edi
	jmp	.LBB0_1190
.LBB0_1005:
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
	jne	.LBB0_568
# %bb.1006:
	and	al, dil
	jne	.LBB0_568
# %bb.1007:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1467
# %bb.1008:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1009:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_1009
	jmp	.LBB0_1468
.LBB0_1010:
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
	jne	.LBB0_575
# %bb.1011:
	and	al, dil
	jne	.LBB0_575
# %bb.1012:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1471
# %bb.1013:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1014:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_1014
	jmp	.LBB0_1472
.LBB0_1015:
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
	jne	.LBB0_582
# %bb.1016:
	and	al, sil
	jne	.LBB0_582
# %bb.1017:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1475
# %bb.1018:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_1019:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm4, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax], xmm6
	movdqu	xmmword ptr [r8 + rax + 16], xmm3
	movdqu	xmm1, xmmword ptr [rdx + rax + 32]
	movdqu	xmm2, xmmword ptr [rdx + rax + 48]
	movdqu	xmm3, xmmword ptr [rcx + rax + 32]
	movdqu	xmm4, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax + 32], xmm6
	movdqu	xmmword ptr [r8 + rax + 48], xmm3
	add	rax, 64
	add	rsi, 2
	jne	.LBB0_1019
	jmp	.LBB0_1476
.LBB0_1023:
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
	jne	.LBB0_592
# %bb.1024:
	and	al, sil
	jne	.LBB0_592
# %bb.1025:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1479
# %bb.1026:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_1027:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm4, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax], xmm6
	movdqu	xmmword ptr [r8 + rax + 16], xmm3
	movdqu	xmm1, xmmword ptr [rdx + rax + 32]
	movdqu	xmm2, xmmword ptr [rdx + rax + 48]
	movdqu	xmm3, xmmword ptr [rcx + rax + 32]
	movdqu	xmm4, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm5, xmm1                      # xmm5 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm6, xmm3                      # xmm6 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	pmullw	xmm6, xmm5
	pand	xmm6, xmm0
	packuswb	xmm6, xmm3
	pmovzxbw	xmm1, xmm2                      # xmm1 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm3, xmm4                      # xmm3 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm4, xmm2
	pand	xmm4, xmm0
	pmullw	xmm3, xmm1
	pand	xmm3, xmm0
	packuswb	xmm3, xmm4
	movdqu	xmmword ptr [r8 + rax + 32], xmm6
	movdqu	xmmword ptr [r8 + rax + 48], xmm3
	add	rax, 64
	add	rsi, 2
	jne	.LBB0_1027
	jmp	.LBB0_1480
.LBB0_1031:
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
	jne	.LBB0_602
# %bb.1032:
	and	al, dil
	jne	.LBB0_602
# %bb.1033:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1483
# %bb.1034:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1035:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_1035
	jmp	.LBB0_1484
.LBB0_1036:
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
	jne	.LBB0_609
# %bb.1037:
	and	al, dil
	jne	.LBB0_609
# %bb.1038:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1487
# %bb.1039:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1040:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_1040
	jmp	.LBB0_1488
.LBB0_1047:
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
	jne	.LBB0_622
# %bb.1048:
	and	al, dil
	jne	.LBB0_622
# %bb.1049:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1491
# %bb.1050:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1051:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_1051
	jmp	.LBB0_1492
.LBB0_1052:
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
	jne	.LBB0_629
# %bb.1053:
	and	al, dil
	jne	.LBB0_629
# %bb.1054:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1495
# %bb.1055:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1056:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_1056
	jmp	.LBB0_1496
.LBB0_1057:
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
	jne	.LBB0_636
# %bb.1058:
	and	al, dil
	jne	.LBB0_636
# %bb.1059:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1499
# %bb.1060:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1061:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_1061
	jmp	.LBB0_1500
.LBB0_1065:
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
	jne	.LBB0_646
# %bb.1066:
	and	al, dil
	jne	.LBB0_646
# %bb.1067:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1503
# %bb.1068:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1069:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_1069
	jmp	.LBB0_1504
.LBB0_1073:
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
	jne	.LBB0_656
# %bb.1074:
	and	al, dil
	jne	.LBB0_656
# %bb.1075:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1507
# %bb.1076:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1077:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_1077
	jmp	.LBB0_1508
.LBB0_1078:
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
	jne	.LBB0_663
# %bb.1079:
	and	al, dil
	jne	.LBB0_663
# %bb.1080:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1511
# %bb.1081:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1082:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_1082
	jmp	.LBB0_1512
.LBB0_1083:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1515
# %bb.1084:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1085:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1085
	jmp	.LBB0_1516
.LBB0_1093:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB0_1211
# %bb.1094:
	xor	eax, eax
	jmp	.LBB0_1213
.LBB0_1095:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB0_1221
# %bb.1096:
	xor	eax, eax
	jmp	.LBB0_1223
.LBB0_1097:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1523
# %bb.1098:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1099:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1099
	jmp	.LBB0_1524
.LBB0_1100:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1533
# %bb.1101:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1102:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1102
	jmp	.LBB0_1534
.LBB0_1103:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1543
# %bb.1104:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_2] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB0_1105:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1105
	jmp	.LBB0_1544
.LBB0_1106:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1551
# %bb.1107:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_2] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB0_1108:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1108
	jmp	.LBB0_1552
.LBB0_1109:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1559
# %bb.1110:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1111:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1111
	jmp	.LBB0_1560
.LBB0_1112:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1567
# %bb.1113:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1114:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1114
	jmp	.LBB0_1568
.LBB0_1115:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1575
# %bb.1116:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI0_4] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB0_1117:                             # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rsi]
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm3
	psrad	xmm6, 7
	movdqa	xmm7, xmm1
	psrad	xmm7, 7
	movdqa	xmm0, xmm4
	psrad	xmm0, 7
	paddd	xmm4, xmm0
	paddd	xmm1, xmm7
	paddd	xmm3, xmm6
	paddd	xmm2, xmm5
	pxor	xmm2, xmm5
	pxor	xmm3, xmm6
	pxor	xmm1, xmm7
	pxor	xmm4, xmm0
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 16]
	movdqa	xmm0, xmm2
	psrad	xmm0, 7
	movdqa	xmm5, xmm3
	psrad	xmm5, 7
	movdqa	xmm6, xmm1
	psrad	xmm6, 7
	movdqa	xmm7, xmm4
	psrad	xmm7, 7
	paddd	xmm4, xmm7
	paddd	xmm1, xmm6
	paddd	xmm3, xmm5
	paddd	xmm2, xmm0
	pxor	xmm2, xmm0
	pxor	xmm3, xmm5
	pxor	xmm1, xmm6
	pxor	xmm4, xmm7
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi + 16], xmm2
	add	rsi, 32
	add	rdi, 2
	jne	.LBB0_1117
	jmp	.LBB0_1576
.LBB0_1118:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1583
# %bb.1119:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI0_4] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB0_1120:                             # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rsi]
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm3
	psrad	xmm6, 7
	movdqa	xmm7, xmm1
	psrad	xmm7, 7
	movdqa	xmm0, xmm4
	psrad	xmm0, 7
	paddd	xmm4, xmm0
	paddd	xmm1, xmm7
	paddd	xmm3, xmm6
	paddd	xmm2, xmm5
	pxor	xmm2, xmm5
	pxor	xmm3, xmm6
	pxor	xmm1, xmm7
	pxor	xmm4, xmm0
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 16]
	movdqa	xmm0, xmm2
	psrad	xmm0, 7
	movdqa	xmm5, xmm3
	psrad	xmm5, 7
	movdqa	xmm6, xmm1
	psrad	xmm6, 7
	movdqa	xmm7, xmm4
	psrad	xmm7, 7
	paddd	xmm4, xmm7
	paddd	xmm1, xmm6
	paddd	xmm3, xmm5
	paddd	xmm2, xmm0
	pxor	xmm2, xmm0
	pxor	xmm3, xmm5
	pxor	xmm1, xmm6
	pxor	xmm4, xmm7
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi + 16], xmm2
	add	rsi, 32
	add	rdi, 2
	jne	.LBB0_1120
	jmp	.LBB0_1584
.LBB0_1121:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1591
# %bb.1122:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1123:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1123
	jmp	.LBB0_1592
.LBB0_1131:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB0_1231
# %bb.1132:
	xor	eax, eax
	jmp	.LBB0_1233
.LBB0_1133:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB0_1241
# %bb.1134:
	xor	eax, eax
	jmp	.LBB0_1243
.LBB0_1135:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1599
# %bb.1136:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1137:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1137
	jmp	.LBB0_1600
.LBB0_1138:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1607
# %bb.1139:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1140:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1140
	jmp	.LBB0_1608
.LBB0_1148:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1615
# %bb.1149:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1150:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1150
	jmp	.LBB0_1616
.LBB0_1151:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB0_1251
# %bb.1152:
	xor	eax, eax
	jmp	.LBB0_1253
.LBB0_1153:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1623
# %bb.1154:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB0_1155:                             # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi + 16]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	add	rsi, 16
	add	rdi, 2
	jne	.LBB0_1155
	jmp	.LBB0_1624
.LBB0_1156:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB0_1261
# %bb.1157:
	xor	eax, eax
	jmp	.LBB0_1263
.LBB0_1158:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1631
# %bb.1159:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB0_1160:                             # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi + 16]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	add	rsi, 16
	add	rdi, 2
	jne	.LBB0_1160
	jmp	.LBB0_1632
.LBB0_1161:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1639
# %bb.1162:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1163:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1163
	jmp	.LBB0_1640
.LBB0_1164:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1647
# %bb.1165:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1166:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1166
	jmp	.LBB0_1648
.LBB0_1167:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1657
# %bb.1168:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1169:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB0_1169
	jmp	.LBB0_1658
.LBB0_1170:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1665
# %bb.1171:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1172:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1172
	jmp	.LBB0_1666
.LBB0_1173:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1675
# %bb.1174:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB0_1175:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB0_1175
	jmp	.LBB0_1676
.LBB0_1176:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1683
# %bb.1177:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB0_1178:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1178
	jmp	.LBB0_1684
.LBB0_1179:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1693
# %bb.1180:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB0_1181:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB0_1181
	jmp	.LBB0_1694
.LBB0_1182:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1701
# %bb.1183:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB0_1184:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1184
	jmp	.LBB0_1702
.LBB0_1185:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1711
# %bb.1186:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1187:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1187
	jmp	.LBB0_1712
.LBB0_1195:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB0_1271
# %bb.1196:
	xor	edi, edi
	jmp	.LBB0_1273
.LBB0_1197:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB0_1281
# %bb.1198:
	xor	edi, edi
	jmp	.LBB0_1283
.LBB0_1199:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1719
# %bb.1200:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1201:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1201
	jmp	.LBB0_1720
.LBB0_1202:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1727
# %bb.1203:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1204:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1204
	jmp	.LBB0_1728
.LBB0_1205:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1735
# %bb.1206:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1207:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1207
	jmp	.LBB0_1736
.LBB0_1208:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1743
# %bb.1209:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1210:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rsi, 2
	jne	.LBB0_1210
	jmp	.LBB0_1744
.LBB0_1086:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB0_1087:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 240], xmm0
	add	rdi, 64
	add	rsi, 8
	jne	.LBB0_1087
.LBB0_1088:
	test	rdx, rdx
	je	.LBB0_1091
# %bb.1089:
	lea	rsi, [r8 + 4*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB0_1090:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB0_1090
.LBB0_1091:
	cmp	rcx, rax
	je	.LBB0_1751
.LBB0_1092:                             # =>This Inner Loop Header: Depth=1
	mov	dword ptr [r8 + 4*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1092
	jmp	.LBB0_1751
.LBB0_1124:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB0_1125:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 240], xmm0
	add	rdi, 32
	add	rsi, 8
	jne	.LBB0_1125
.LBB0_1126:
	test	rdx, rdx
	je	.LBB0_1129
# %bb.1127:
	lea	rsi, [r8 + 8*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB0_1128:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB0_1128
.LBB0_1129:
	cmp	rcx, rax
	je	.LBB0_1751
.LBB0_1130:                             # =>This Inner Loop Header: Depth=1
	mov	qword ptr [r8 + 8*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1130
	jmp	.LBB0_1751
.LBB0_1141:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB0_1142:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 240], xmm0
	sub	rdi, -128
	add	rsi, 8
	jne	.LBB0_1142
.LBB0_1143:
	test	rdx, rdx
	je	.LBB0_1146
# %bb.1144:
	lea	rsi, [r8 + 2*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB0_1145:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB0_1145
.LBB0_1146:
	cmp	rcx, rax
	je	.LBB0_1751
.LBB0_1147:                             # =>This Inner Loop Header: Depth=1
	mov	word ptr [r8 + 2*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1147
	jmp	.LBB0_1751
.LBB0_1188:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB0_1189:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + rdi + 240], xmm0
	add	rdi, 256
	add	rsi, 8
	jne	.LBB0_1189
.LBB0_1190:
	test	rdx, rdx
	je	.LBB0_1193
# %bb.1191:
	lea	rsi, [rdi + r8]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB0_1192:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB0_1192
.LBB0_1193:
	cmp	rcx, rax
	je	.LBB0_1751
.LBB0_1194:                             # =>This Inner Loop Header: Depth=1
	mov	byte ptr [r8 + rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1194
.LBB0_1751:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_1211:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1212:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [r8 + 4*rax + 64], xmm0
	movups	xmmword ptr [r8 + 4*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movdqu	xmmword ptr [r8 + 4*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1212
.LBB0_1213:
	test	rsi, rsi
	je	.LBB0_1216
# %bb.1214:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB0_1215:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1215
.LBB0_1216:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1217
.LBB0_1221:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1222:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [r8 + 4*rax + 64], xmm0
	movups	xmmword ptr [r8 + 4*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movdqu	xmmword ptr [r8 + 4*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1222
.LBB0_1223:
	test	rsi, rsi
	je	.LBB0_1226
# %bb.1224:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB0_1225:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1225
.LBB0_1226:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1227
.LBB0_1231:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1232:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [r8 + 8*rax], xmm0
	movups	xmmword ptr [r8 + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [r8 + 8*rax + 32], xmm0
	movups	xmmword ptr [r8 + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [r8 + 8*rax + 64], xmm0
	movups	xmmword ptr [r8 + 8*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movdqu	xmmword ptr [r8 + 8*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1232
.LBB0_1233:
	test	rsi, rsi
	je	.LBB0_1236
# %bb.1234:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB0_1235:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1235
.LBB0_1236:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1237
.LBB0_1241:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1242:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [r8 + 8*rax], xmm0
	movups	xmmword ptr [r8 + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [r8 + 8*rax + 32], xmm0
	movups	xmmword ptr [r8 + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [r8 + 8*rax + 64], xmm0
	movups	xmmword ptr [r8 + 8*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movdqu	xmmword ptr [r8 + 8*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1242
.LBB0_1243:
	test	rsi, rsi
	je	.LBB0_1246
# %bb.1244:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB0_1245:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1245
.LBB0_1246:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1247
.LBB0_1251:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1252:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [r8 + 2*rax], xmm0
	movups	xmmword ptr [r8 + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [r8 + 2*rax + 32], xmm0
	movups	xmmword ptr [r8 + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [r8 + 2*rax + 64], xmm0
	movups	xmmword ptr [r8 + 2*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movdqu	xmmword ptr [r8 + 2*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB0_1252
.LBB0_1253:
	test	rsi, rsi
	je	.LBB0_1256
# %bb.1254:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB0_1255:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1255
.LBB0_1256:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1257
.LBB0_1261:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1262:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [r8 + 2*rax], xmm0
	movups	xmmword ptr [r8 + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [r8 + 2*rax + 32], xmm0
	movups	xmmword ptr [r8 + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [r8 + 2*rax + 64], xmm0
	movups	xmmword ptr [r8 + 2*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movdqu	xmmword ptr [r8 + 2*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB0_1262
.LBB0_1263:
	test	rsi, rsi
	je	.LBB0_1266
# %bb.1264:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB0_1265:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1265
.LBB0_1266:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1267
.LBB0_1271:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB0_1272:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rdi]
	movups	xmm1, xmmword ptr [rdx + rdi + 16]
	movups	xmmword ptr [r8 + rdi], xmm0
	movups	xmmword ptr [r8 + rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 32]
	movups	xmm1, xmmword ptr [rdx + rdi + 48]
	movups	xmmword ptr [r8 + rdi + 32], xmm0
	movups	xmmword ptr [r8 + rdi + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 64]
	movups	xmm1, xmmword ptr [rdx + rdi + 80]
	movups	xmmword ptr [r8 + rdi + 64], xmm0
	movups	xmmword ptr [r8 + rdi + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 96]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 112]
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm1
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB0_1272
.LBB0_1273:
	test	rax, rax
	je	.LBB0_1276
# %bb.1274:
	add	rdi, 16
	neg	rax
.LBB0_1275:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB0_1275
.LBB0_1276:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1277
.LBB0_1281:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB0_1282:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rdi]
	movups	xmm1, xmmword ptr [rdx + rdi + 16]
	movups	xmmword ptr [r8 + rdi], xmm0
	movups	xmmword ptr [r8 + rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 32]
	movups	xmm1, xmmword ptr [rdx + rdi + 48]
	movups	xmmword ptr [r8 + rdi + 32], xmm0
	movups	xmmword ptr [r8 + rdi + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 64]
	movups	xmm1, xmmword ptr [rdx + rdi + 80]
	movups	xmmword ptr [r8 + rdi + 64], xmm0
	movups	xmmword ptr [r8 + rdi + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 96]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 112]
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm1
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB0_1282
.LBB0_1283:
	test	rax, rax
	je	.LBB0_1286
# %bb.1284:
	add	rdi, 16
	neg	rax
.LBB0_1285:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB0_1285
.LBB0_1286:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1287
.LBB0_1291:
	xor	edi, edi
.LBB0_1292:
	test	r9b, 1
	je	.LBB0_1294
# %bb.1293:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1294:
	cmp	rsi, r10
	jne	.LBB0_35
	jmp	.LBB0_1751
.LBB0_1295:
	xor	edi, edi
.LBB0_1296:
	test	r9b, 1
	je	.LBB0_1298
# %bb.1297:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1298:
	cmp	rsi, r10
	jne	.LBB0_49
	jmp	.LBB0_1751
.LBB0_1299:
	xor	edi, edi
.LBB0_1300:
	test	r9b, 1
	je	.LBB0_1302
# %bb.1301:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1302:
	cmp	rsi, r10
	jne	.LBB0_61
	jmp	.LBB0_1751
.LBB0_1303:
	xor	edi, edi
.LBB0_1304:
	test	r9b, 1
	je	.LBB0_1306
# %bb.1305:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1306:
	cmp	rsi, r10
	jne	.LBB0_81
	jmp	.LBB0_1751
.LBB0_1307:
	xor	edi, edi
.LBB0_1308:
	test	r9b, 1
	je	.LBB0_1310
# %bb.1309:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1310:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_101
.LBB0_1311:
	xor	edi, edi
.LBB0_1312:
	test	r9b, 1
	je	.LBB0_1314
# %bb.1313:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1314:
	cmp	rsi, r10
	jne	.LBB0_113
	jmp	.LBB0_1751
.LBB0_1315:
	xor	edi, edi
.LBB0_1316:
	test	r9b, 1
	je	.LBB0_1318
# %bb.1317:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1318:
	cmp	rsi, r10
	jne	.LBB0_138
	jmp	.LBB0_1751
.LBB0_1319:
	xor	edi, edi
.LBB0_1320:
	test	r9b, 1
	je	.LBB0_1322
# %bb.1321:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1322:
	cmp	rsi, r10
	jne	.LBB0_149
	jmp	.LBB0_1751
.LBB0_1323:
	xor	edi, edi
.LBB0_1324:
	test	r9b, 1
	je	.LBB0_1326
# %bb.1325:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1326:
	cmp	rsi, r10
	jne	.LBB0_160
	jmp	.LBB0_1751
.LBB0_1327:
	xor	edi, edi
.LBB0_1328:
	test	r9b, 1
	je	.LBB0_1330
# %bb.1329:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1330:
	cmp	rsi, r10
	jne	.LBB0_178
	jmp	.LBB0_1751
.LBB0_1331:
	xor	edi, edi
.LBB0_1332:
	test	r9b, 1
	je	.LBB0_1334
# %bb.1333:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1334:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_196
.LBB0_1335:
	xor	edi, edi
.LBB0_1336:
	test	r9b, 1
	je	.LBB0_1338
# %bb.1337:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1338:
	cmp	rsi, r10
	jne	.LBB0_207
	jmp	.LBB0_1751
.LBB0_1339:
	xor	edi, edi
.LBB0_1340:
	test	r9b, 1
	je	.LBB0_1342
# %bb.1341:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1342:
	cmp	rsi, r10
	jne	.LBB0_226
	jmp	.LBB0_1751
.LBB0_1343:
	xor	edi, edi
.LBB0_1344:
	test	r9b, 1
	je	.LBB0_1346
# %bb.1345:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1346:
	cmp	rsi, r10
	jne	.LBB0_235
	jmp	.LBB0_1751
.LBB0_1347:
	xor	eax, eax
.LBB0_1348:
	test	r9b, 1
	je	.LBB0_1350
# %bb.1349:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_5] # xmm1 = [255,255,255,255,255,255,255,255]
	pand	xmm3, xmm1
	pmullw	xmm5, xmm4
	pand	xmm5, xmm1
	packuswb	xmm5, xmm3
	pmovzxbw	xmm3, xmm2                      # xmm3 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm4, xmm0                      # xmm4 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm0, xmm2
	pand	xmm0, xmm1
	pmullw	xmm4, xmm3
	pand	xmm4, xmm1
	packuswb	xmm4, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm4
.LBB0_1350:
	cmp	rdi, r10
	jne	.LBB0_244
	jmp	.LBB0_1751
.LBB0_1351:
	xor	eax, eax
.LBB0_1352:
	test	r9b, 1
	je	.LBB0_1354
# %bb.1353:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_5] # xmm1 = [255,255,255,255,255,255,255,255]
	pand	xmm3, xmm1
	pmullw	xmm5, xmm4
	pand	xmm5, xmm1
	packuswb	xmm5, xmm3
	pmovzxbw	xmm3, xmm2                      # xmm3 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm4, xmm0                      # xmm4 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm0, xmm2
	pand	xmm0, xmm1
	pmullw	xmm4, xmm3
	pand	xmm4, xmm1
	packuswb	xmm4, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm4
.LBB0_1354:
	cmp	rdi, r10
	jne	.LBB0_258
	jmp	.LBB0_1751
.LBB0_1355:
	xor	edi, edi
.LBB0_1356:
	test	r9b, 1
	je	.LBB0_1358
# %bb.1357:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1358:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_272
.LBB0_1359:
	xor	edi, edi
.LBB0_1360:
	test	r9b, 1
	je	.LBB0_1362
# %bb.1361:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1362:
	cmp	rsi, r10
	jne	.LBB0_281
	jmp	.LBB0_1751
.LBB0_1363:
	xor	edi, edi
.LBB0_1364:
	test	r9b, 1
	je	.LBB0_1366
# %bb.1365:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1366:
	cmp	rsi, r10
	jne	.LBB0_300
	jmp	.LBB0_1751
.LBB0_1367:
	xor	edi, edi
.LBB0_1368:
	test	r9b, 1
	je	.LBB0_1370
# %bb.1369:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1370:
	cmp	rsi, r10
	jne	.LBB0_309
	jmp	.LBB0_1751
.LBB0_1371:
	xor	edi, edi
.LBB0_1372:
	test	r9b, 1
	je	.LBB0_1374
# %bb.1373:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1374:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_338
.LBB0_1375:
	xor	edi, edi
.LBB0_1376:
	test	r9b, 1
	je	.LBB0_1378
# %bb.1377:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1378:
	cmp	rsi, r10
	jne	.LBB0_347
	jmp	.LBB0_1751
.LBB0_1379:
	xor	edi, edi
.LBB0_1380:
	test	r9b, 1
	je	.LBB0_1382
# %bb.1381:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1382:
	cmp	rsi, r10
	jne	.LBB0_366
	jmp	.LBB0_1751
.LBB0_1383:
	xor	edi, edi
.LBB0_1384:
	test	r9b, 1
	je	.LBB0_1386
# %bb.1385:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1386:
	cmp	rsi, r10
	jne	.LBB0_373
	jmp	.LBB0_1751
.LBB0_1387:
	xor	edi, edi
.LBB0_1388:
	test	r9b, 1
	je	.LBB0_1390
# %bb.1389:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1390:
	cmp	rsi, r10
	jne	.LBB0_380
	jmp	.LBB0_1751
.LBB0_1391:
	xor	edi, edi
.LBB0_1392:
	test	r9b, 1
	je	.LBB0_1394
# %bb.1393:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1394:
	cmp	rsi, r10
	jne	.LBB0_387
	jmp	.LBB0_1751
.LBB0_1395:
	xor	edi, edi
.LBB0_1396:
	test	r9b, 1
	je	.LBB0_1398
# %bb.1397:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1398:
	cmp	rsi, r10
	jne	.LBB0_394
	jmp	.LBB0_1751
.LBB0_1399:
	xor	edi, edi
.LBB0_1400:
	test	r9b, 1
	je	.LBB0_1402
# %bb.1401:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1402:
	cmp	rsi, r10
	jne	.LBB0_401
	jmp	.LBB0_1751
.LBB0_1403:
	xor	edi, edi
.LBB0_1404:
	test	r9b, 1
	je	.LBB0_1406
# %bb.1405:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1406:
	cmp	rsi, r10
	jne	.LBB0_414
	jmp	.LBB0_1751
.LBB0_1407:
	xor	edi, edi
.LBB0_1408:
	test	r9b, 1
	je	.LBB0_1410
# %bb.1409:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1410:
	cmp	rsi, r10
	jne	.LBB0_421
	jmp	.LBB0_1751
.LBB0_1411:
	xor	edi, edi
.LBB0_1412:
	test	r9b, 1
	je	.LBB0_1414
# %bb.1413:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1414:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_434
.LBB0_1415:
	xor	edi, edi
.LBB0_1416:
	test	r9b, 1
	je	.LBB0_1418
# %bb.1417:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1418:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_441
.LBB0_1419:
	xor	edi, edi
.LBB0_1420:
	test	r9b, 1
	je	.LBB0_1422
# %bb.1421:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1422:
	cmp	rsi, r10
	jne	.LBB0_448
	jmp	.LBB0_1751
.LBB0_1423:
	xor	edi, edi
.LBB0_1424:
	test	r9b, 1
	je	.LBB0_1426
# %bb.1425:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1426:
	cmp	rsi, r10
	jne	.LBB0_455
	jmp	.LBB0_1751
.LBB0_1427:
	xor	edi, edi
.LBB0_1428:
	test	r9b, 1
	je	.LBB0_1430
# %bb.1429:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1430:
	cmp	rsi, r10
	jne	.LBB0_474
	jmp	.LBB0_1751
.LBB0_1431:
	xor	edi, edi
.LBB0_1432:
	test	r9b, 1
	je	.LBB0_1434
# %bb.1433:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1434:
	cmp	rsi, r10
	jne	.LBB0_481
	jmp	.LBB0_1751
.LBB0_1435:
	xor	edi, edi
.LBB0_1436:
	test	r9b, 1
	je	.LBB0_1438
# %bb.1437:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1438:
	cmp	rsi, r10
	jne	.LBB0_488
	jmp	.LBB0_1751
.LBB0_1439:
	xor	edi, edi
.LBB0_1440:
	test	r9b, 1
	je	.LBB0_1442
# %bb.1441:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1442:
	cmp	rsi, r10
	jne	.LBB0_495
	jmp	.LBB0_1751
.LBB0_1443:
	xor	edi, edi
.LBB0_1444:
	test	r9b, 1
	je	.LBB0_1446
# %bb.1445:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1446:
	cmp	rsi, r10
	jne	.LBB0_505
	jmp	.LBB0_1751
.LBB0_1447:
	xor	edi, edi
.LBB0_1448:
	test	r9b, 1
	je	.LBB0_1450
# %bb.1449:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1450:
	cmp	rsi, r10
	jne	.LBB0_521
	jmp	.LBB0_1751
.LBB0_1451:
	xor	edi, edi
.LBB0_1452:
	test	r9b, 1
	je	.LBB0_1454
# %bb.1453:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1454:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_534
.LBB0_1455:
	xor	edi, edi
.LBB0_1456:
	test	r9b, 1
	je	.LBB0_1458
# %bb.1457:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1458:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_541
.LBB0_1459:
	xor	edi, edi
.LBB0_1460:
	test	r9b, 1
	je	.LBB0_1462
# %bb.1461:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1462:
	cmp	rsi, r10
	jne	.LBB0_548
	jmp	.LBB0_1751
.LBB0_1463:
	xor	edi, edi
.LBB0_1464:
	test	r9b, 1
	je	.LBB0_1466
# %bb.1465:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1466:
	cmp	rsi, r10
	jne	.LBB0_555
	jmp	.LBB0_1751
.LBB0_1467:
	xor	edi, edi
.LBB0_1468:
	test	r9b, 1
	je	.LBB0_1470
# %bb.1469:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1470:
	cmp	rsi, r10
	jne	.LBB0_568
	jmp	.LBB0_1751
.LBB0_1471:
	xor	edi, edi
.LBB0_1472:
	test	r9b, 1
	je	.LBB0_1474
# %bb.1473:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1474:
	cmp	rsi, r10
	jne	.LBB0_575
	jmp	.LBB0_1751
.LBB0_1475:
	xor	eax, eax
.LBB0_1476:
	test	r9b, 1
	je	.LBB0_1478
# %bb.1477:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_5] # xmm1 = [255,255,255,255,255,255,255,255]
	pand	xmm3, xmm1
	pmullw	xmm5, xmm4
	pand	xmm5, xmm1
	packuswb	xmm5, xmm3
	pmovzxbw	xmm3, xmm2                      # xmm3 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm4, xmm0                      # xmm4 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm0, xmm2
	pand	xmm0, xmm1
	pmullw	xmm4, xmm3
	pand	xmm4, xmm1
	packuswb	xmm4, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm4
.LBB0_1478:
	cmp	rdi, r10
	jne	.LBB0_582
	jmp	.LBB0_1751
.LBB0_1479:
	xor	eax, eax
.LBB0_1480:
	test	r9b, 1
	je	.LBB0_1482
# %bb.1481:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_5] # xmm1 = [255,255,255,255,255,255,255,255]
	pand	xmm3, xmm1
	pmullw	xmm5, xmm4
	pand	xmm5, xmm1
	packuswb	xmm5, xmm3
	pmovzxbw	xmm3, xmm2                      # xmm3 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm4, xmm0                      # xmm4 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm0, xmm2
	pand	xmm0, xmm1
	pmullw	xmm4, xmm3
	pand	xmm4, xmm1
	packuswb	xmm4, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm4
.LBB0_1482:
	cmp	rdi, r10
	jne	.LBB0_592
	jmp	.LBB0_1751
.LBB0_1483:
	xor	edi, edi
.LBB0_1484:
	test	r9b, 1
	je	.LBB0_1486
# %bb.1485:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1486:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_602
.LBB0_1487:
	xor	edi, edi
.LBB0_1488:
	test	r9b, 1
	je	.LBB0_1490
# %bb.1489:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1490:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_609
.LBB0_1491:
	xor	edi, edi
.LBB0_1492:
	test	r9b, 1
	je	.LBB0_1494
# %bb.1493:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1494:
	cmp	rsi, r10
	jne	.LBB0_622
	jmp	.LBB0_1751
.LBB0_1495:
	xor	edi, edi
.LBB0_1496:
	test	r9b, 1
	je	.LBB0_1498
# %bb.1497:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1498:
	cmp	rsi, r10
	jne	.LBB0_629
	jmp	.LBB0_1751
.LBB0_1499:
	xor	edi, edi
.LBB0_1500:
	test	r9b, 1
	je	.LBB0_1502
# %bb.1501:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1502:
	cmp	rsi, r10
	jne	.LBB0_636
	jmp	.LBB0_1751
.LBB0_1503:
	xor	edi, edi
.LBB0_1504:
	test	r9b, 1
	je	.LBB0_1506
# %bb.1505:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1506:
	cmp	rsi, r10
	jne	.LBB0_646
	jmp	.LBB0_1751
.LBB0_1507:
	xor	edi, edi
.LBB0_1508:
	test	r9b, 1
	je	.LBB0_1510
# %bb.1509:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1510:
	cmp	rsi, r10
	je	.LBB0_1751
	jmp	.LBB0_656
.LBB0_1511:
	xor	edi, edi
.LBB0_1512:
	test	r9b, 1
	je	.LBB0_1514
# %bb.1513:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1514:
	cmp	rsi, r10
	jne	.LBB0_663
	jmp	.LBB0_1751
.LBB0_1515:
	xor	edi, edi
.LBB0_1516:
	test	r9b, 1
	je	.LBB0_1518
# %bb.1517:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB0_1518:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1519
.LBB0_1523:
	xor	edi, edi
.LBB0_1524:
	test	r9b, 1
	je	.LBB0_1526
# %bb.1525:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_0] # xmm2 = [-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1526:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1527
.LBB0_1533:
	xor	edi, edi
.LBB0_1534:
	test	r9b, 1
	je	.LBB0_1536
# %bb.1535:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_0] # xmm2 = [-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1536:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1537
.LBB0_1543:
	xor	edi, edi
.LBB0_1544:
	test	r9b, 1
	je	.LBB0_1546
# %bb.1545:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_2] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1546:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1547
.LBB0_1551:
	xor	edi, edi
.LBB0_1552:
	test	r9b, 1
	je	.LBB0_1554
# %bb.1553:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_2] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1554:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1555
.LBB0_1559:
	xor	edi, edi
.LBB0_1560:
	test	r9b, 1
	je	.LBB0_1562
# %bb.1561:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB0_1562:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1563
.LBB0_1567:
	xor	edi, edi
.LBB0_1568:
	test	r9b, 1
	je	.LBB0_1570
# %bb.1569:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB0_1570:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1571
.LBB0_1575:
	xor	esi, esi
.LBB0_1576:
	test	r9b, 1
	je	.LBB0_1578
# %bb.1577:
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rsi]
	movdqa	xmm4, xmm1
	psrad	xmm4, 7
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm0
	psrad	xmm6, 7
	movdqa	xmm7, xmm3
	psrad	xmm7, 7
	paddd	xmm3, xmm7
	paddd	xmm0, xmm6
	paddd	xmm2, xmm5
	paddd	xmm1, xmm4
	pxor	xmm1, xmm4
	pxor	xmm2, xmm5
	pxor	xmm0, xmm6
	pxor	xmm3, xmm7
	movdqa	xmm4, xmmword ptr [rip + .LCPI0_4] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB0_1578:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1579
.LBB0_1583:
	xor	esi, esi
.LBB0_1584:
	test	r9b, 1
	je	.LBB0_1586
# %bb.1585:
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rsi]
	movdqa	xmm4, xmm1
	psrad	xmm4, 7
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm0
	psrad	xmm6, 7
	movdqa	xmm7, xmm3
	psrad	xmm7, 7
	paddd	xmm3, xmm7
	paddd	xmm0, xmm6
	paddd	xmm2, xmm5
	paddd	xmm1, xmm4
	pxor	xmm1, xmm4
	pxor	xmm2, xmm5
	pxor	xmm0, xmm6
	pxor	xmm3, xmm7
	movdqa	xmm4, xmmword ptr [rip + .LCPI0_4] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB0_1586:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1587
.LBB0_1591:
	xor	edi, edi
.LBB0_1592:
	test	r9b, 1
	je	.LBB0_1594
# %bb.1593:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB0_1594:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1595
.LBB0_1599:
	xor	edi, edi
.LBB0_1600:
	test	r9b, 1
	je	.LBB0_1602
# %bb.1601:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB0_1602:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1603
.LBB0_1607:
	xor	edi, edi
.LBB0_1608:
	test	r9b, 1
	je	.LBB0_1610
# %bb.1609:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB0_1610:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1611
.LBB0_1615:
	xor	edi, edi
.LBB0_1616:
	test	r9b, 1
	je	.LBB0_1618
# %bb.1617:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB0_1618:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1619
.LBB0_1623:
	xor	esi, esi
.LBB0_1624:
	test	r9b, 1
	je	.LBB0_1626
# %bb.1625:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi]
	movdqa	xmm2, xmm1
	psrad	xmm2, 15
	movdqa	xmm3, xmm0
	psrad	xmm3, 15
	paddd	xmm0, xmm3
	paddd	xmm1, xmm2
	pxor	xmm1, xmm2
	pxor	xmm0, xmm3
	pxor	xmm2, xmm2
	pblendw	xmm0, xmm2, 170                 # xmm0 = xmm0[0],xmm2[1],xmm0[2],xmm2[3],xmm0[4],xmm2[5],xmm0[6],xmm2[7]
	pblendw	xmm1, xmm2, 170                 # xmm1 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	packusdw	xmm1, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm1
.LBB0_1626:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1627
.LBB0_1631:
	xor	esi, esi
.LBB0_1632:
	test	r9b, 1
	je	.LBB0_1634
# %bb.1633:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi]
	movdqa	xmm2, xmm1
	psrad	xmm2, 15
	movdqa	xmm3, xmm0
	psrad	xmm3, 15
	paddd	xmm0, xmm3
	paddd	xmm1, xmm2
	pxor	xmm1, xmm2
	pxor	xmm0, xmm3
	pxor	xmm2, xmm2
	pblendw	xmm0, xmm2, 170                 # xmm0 = xmm0[0],xmm2[1],xmm0[2],xmm2[3],xmm0[4],xmm2[5],xmm0[6],xmm2[7]
	pblendw	xmm1, xmm2, 170                 # xmm1 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	packusdw	xmm1, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm1
.LBB0_1634:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1635
.LBB0_1639:
	xor	edi, edi
.LBB0_1640:
	test	r9b, 1
	je	.LBB0_1642
# %bb.1641:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB0_1642:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1643
.LBB0_1647:
	xor	edi, edi
.LBB0_1648:
	test	r9b, 1
	je	.LBB0_1650
# %bb.1649:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_1] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1650:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1651
.LBB0_1657:
	xor	edi, edi
.LBB0_1658:
	test	r9b, 1
	je	.LBB0_1660
# %bb.1659:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB0_1660:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1661
.LBB0_1665:
	xor	edi, edi
.LBB0_1666:
	test	r9b, 1
	je	.LBB0_1668
# %bb.1667:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_1] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1668:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1669
.LBB0_1675:
	xor	esi, esi
.LBB0_1676:
	test	r9b, 1
	je	.LBB0_1678
# %bb.1677:
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
.LBB0_1678:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1679
.LBB0_1683:
	xor	edi, edi
.LBB0_1684:
	test	r9b, 1
	je	.LBB0_1686
# %bb.1685:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_3] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1686:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1687
.LBB0_1693:
	xor	esi, esi
.LBB0_1694:
	test	r9b, 1
	je	.LBB0_1696
# %bb.1695:
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
.LBB0_1696:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1697
.LBB0_1701:
	xor	edi, edi
.LBB0_1702:
	test	r9b, 1
	je	.LBB0_1704
# %bb.1703:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_3] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1704:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1705
.LBB0_1711:
	xor	edi, edi
.LBB0_1712:
	test	r9b, 1
	je	.LBB0_1714
# %bb.1713:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB0_1714:
	cmp	rcx, r10
	je	.LBB0_1751
	jmp	.LBB0_1715
.LBB0_1719:
	xor	edi, edi
.LBB0_1720:
	test	r9b, 1
	je	.LBB0_1722
# %bb.1721:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB0_1722:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1723
.LBB0_1727:
	xor	edi, edi
.LBB0_1728:
	test	r9b, 1
	je	.LBB0_1730
# %bb.1729:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB0_1730:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1731
.LBB0_1735:
	xor	edi, edi
.LBB0_1736:
	test	r9b, 1
	je	.LBB0_1738
# %bb.1737:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1738:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1739
.LBB0_1743:
	xor	edi, edi
.LBB0_1744:
	test	r9b, 1
	je	.LBB0_1746
# %bb.1745:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1746:
	cmp	rcx, rax
	je	.LBB0_1751
	jmp	.LBB0_1747
.Lfunc_end0:
	.size	arithmetic_sse4, .Lfunc_end0-arithmetic_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_arr_scalar_sse4
.LCPI1_0:
	.long	0x00000000
	.long	0x80000000              # double -0
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI1_1:
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
.LCPI1_2:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI1_3:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI1_4:
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
.LCPI1_5:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	arithmetic_arr_scalar_sse4
	.p2align	4, 0x90
	.type	arithmetic_arr_scalar_sse4,@function
arithmetic_arr_scalar_sse4:             # @arithmetic_arr_scalar_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 6
	jg	.LBB1_13
# %bb.1:
	cmp	sil, 1
	jle	.LBB1_25
# %bb.2:
	cmp	sil, 2
	je	.LBB1_45
# %bb.3:
	cmp	sil, 4
	je	.LBB1_53
# %bb.4:
	cmp	sil, 5
	jne	.LBB1_1807
# %bb.5:
	cmp	edi, 6
	jg	.LBB1_93
# %bb.6:
	cmp	edi, 3
	jle	.LBB1_163
# %bb.7:
	cmp	edi, 4
	je	.LBB1_263
# %bb.8:
	cmp	edi, 5
	je	.LBB1_266
# %bb.9:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.10:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.11:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_12
# %bb.443:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_747
# %bb.444:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_747
.LBB1_12:
	xor	ecx, ecx
.LBB1_1127:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1129
.LBB1_1128:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1128
.LBB1_1129:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1130:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1130
	jmp	.LBB1_1807
.LBB1_13:
	cmp	sil, 8
	jle	.LBB1_35
# %bb.14:
	cmp	sil, 9
	je	.LBB1_61
# %bb.15:
	cmp	sil, 11
	je	.LBB1_69
# %bb.16:
	cmp	sil, 12
	jne	.LBB1_1807
# %bb.17:
	cmp	edi, 6
	jg	.LBB1_100
# %bb.18:
	cmp	edi, 3
	jle	.LBB1_168
# %bb.19:
	cmp	edi, 4
	je	.LBB1_269
# %bb.20:
	cmp	edi, 5
	je	.LBB1_272
# %bb.21:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.22:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.23:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB1_446
# %bb.24:
	xor	ecx, ecx
	jmp	.LBB1_756
.LBB1_25:
	test	sil, sil
	je	.LBB1_77
# %bb.26:
	cmp	sil, 1
	jne	.LBB1_1807
# %bb.27:
	cmp	edi, 6
	jg	.LBB1_107
# %bb.28:
	cmp	edi, 3
	jle	.LBB1_173
# %bb.29:
	cmp	edi, 4
	je	.LBB1_275
# %bb.30:
	cmp	edi, 5
	je	.LBB1_278
# %bb.31:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.32:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.33:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_34
# %bb.448:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_757
# %bb.449:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_757
.LBB1_34:
	xor	esi, esi
.LBB1_1135:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1137
.LBB1_1136:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1136
.LBB1_1137:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1138:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1138
	jmp	.LBB1_1807
.LBB1_35:
	cmp	sil, 7
	je	.LBB1_85
# %bb.36:
	cmp	sil, 8
	jne	.LBB1_1807
# %bb.37:
	cmp	edi, 6
	jg	.LBB1_114
# %bb.38:
	cmp	edi, 3
	jle	.LBB1_178
# %bb.39:
	cmp	edi, 4
	je	.LBB1_281
# %bb.40:
	cmp	edi, 5
	je	.LBB1_284
# %bb.41:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.42:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.43:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_44
# %bb.451:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_760
# %bb.452:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_760
.LBB1_44:
	xor	esi, esi
.LBB1_1143:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1145
.LBB1_1144:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1144
.LBB1_1145:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1146:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1146
	jmp	.LBB1_1807
.LBB1_45:
	cmp	edi, 6
	jg	.LBB1_121
# %bb.46:
	cmp	edi, 3
	jle	.LBB1_183
# %bb.47:
	cmp	edi, 4
	je	.LBB1_287
# %bb.48:
	cmp	edi, 5
	je	.LBB1_290
# %bb.49:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.50:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.51:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_52
# %bb.454:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_763
# %bb.455:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_763
.LBB1_52:
	xor	esi, esi
.LBB1_1151:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1153
.LBB1_1152:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1152
.LBB1_1153:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1154:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1154
	jmp	.LBB1_1807
.LBB1_53:
	cmp	edi, 6
	jg	.LBB1_128
# %bb.54:
	cmp	edi, 3
	jle	.LBB1_188
# %bb.55:
	cmp	edi, 4
	je	.LBB1_293
# %bb.56:
	cmp	edi, 5
	je	.LBB1_296
# %bb.57:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.58:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.59:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_60
# %bb.457:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_766
# %bb.458:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_766
.LBB1_60:
	xor	ecx, ecx
.LBB1_1049:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1051
.LBB1_1050:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1050
.LBB1_1051:
	cmp	rax, 3
	jb	.LBB1_1807
.LBB1_1052:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1052
	jmp	.LBB1_1807
.LBB1_61:
	cmp	edi, 6
	jg	.LBB1_135
# %bb.62:
	cmp	edi, 3
	jle	.LBB1_193
# %bb.63:
	cmp	edi, 4
	je	.LBB1_299
# %bb.64:
	cmp	edi, 5
	je	.LBB1_302
# %bb.65:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.66:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.67:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_68
# %bb.460:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_768
# %bb.461:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_768
.LBB1_68:
	xor	esi, esi
.LBB1_1159:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1161
.LBB1_1160:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1160
.LBB1_1161:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1162:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1162
	jmp	.LBB1_1807
.LBB1_69:
	cmp	edi, 6
	jg	.LBB1_142
# %bb.70:
	cmp	edi, 3
	jle	.LBB1_198
# %bb.71:
	cmp	edi, 4
	je	.LBB1_305
# %bb.72:
	cmp	edi, 5
	je	.LBB1_308
# %bb.73:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.74:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.75:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_76
# %bb.463:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_771
# %bb.464:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_771
.LBB1_76:
	xor	ecx, ecx
.LBB1_1059:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1061
.LBB1_1060:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1060
.LBB1_1061:
	cmp	rax, 3
	jb	.LBB1_1807
.LBB1_1062:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1062
	jmp	.LBB1_1807
.LBB1_77:
	cmp	edi, 6
	jg	.LBB1_149
# %bb.78:
	cmp	edi, 3
	jle	.LBB1_203
# %bb.79:
	cmp	edi, 4
	je	.LBB1_311
# %bb.80:
	cmp	edi, 5
	je	.LBB1_314
# %bb.81:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.82:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.83:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_84
# %bb.466:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_773
# %bb.467:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_773
.LBB1_84:
	xor	esi, esi
.LBB1_1167:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1169
.LBB1_1168:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1168
.LBB1_1169:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1170:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1170
	jmp	.LBB1_1807
.LBB1_85:
	cmp	edi, 6
	jg	.LBB1_156
# %bb.86:
	cmp	edi, 3
	jle	.LBB1_208
# %bb.87:
	cmp	edi, 4
	je	.LBB1_317
# %bb.88:
	cmp	edi, 5
	je	.LBB1_320
# %bb.89:
	cmp	edi, 6
	jne	.LBB1_1807
# %bb.90:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.91:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_92
# %bb.469:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_776
# %bb.470:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_776
.LBB1_92:
	xor	esi, esi
.LBB1_1175:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1177
.LBB1_1176:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1176
.LBB1_1177:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1178:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1178
	jmp	.LBB1_1807
.LBB1_93:
	cmp	edi, 8
	jle	.LBB1_213
# %bb.94:
	cmp	edi, 9
	je	.LBB1_323
# %bb.95:
	cmp	edi, 11
	je	.LBB1_326
# %bb.96:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.97:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.98:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_99
# %bb.472:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_779
# %bb.473:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_779
.LBB1_99:
	xor	ecx, ecx
.LBB1_1183:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1186
# %bb.1184:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1185:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1185
.LBB1_1186:
	cmp	rsi, 3
	jb	.LBB1_1807
# %bb.1187:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1188:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 8], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 16], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1188
	jmp	.LBB1_1807
.LBB1_100:
	cmp	edi, 8
	jle	.LBB1_218
# %bb.101:
	cmp	edi, 9
	je	.LBB1_329
# %bb.102:
	cmp	edi, 11
	je	.LBB1_332
# %bb.103:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.104:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.105:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_106
# %bb.475:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_782
# %bb.476:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_782
.LBB1_106:
	xor	ecx, ecx
.LBB1_1193:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1196
# %bb.1194:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1195:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1195
.LBB1_1196:
	cmp	rsi, 3
	jb	.LBB1_1807
# %bb.1197:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1198:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 8], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 16], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1198
	jmp	.LBB1_1807
.LBB1_107:
	cmp	edi, 8
	jle	.LBB1_223
# %bb.108:
	cmp	edi, 9
	je	.LBB1_335
# %bb.109:
	cmp	edi, 11
	je	.LBB1_338
# %bb.110:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.111:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.112:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_113
# %bb.478:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_785
# %bb.479:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_785
.LBB1_113:
	xor	ecx, ecx
.LBB1_1203:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1205
.LBB1_1204:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1204
.LBB1_1205:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1206:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1206
	jmp	.LBB1_1807
.LBB1_114:
	cmp	edi, 8
	jle	.LBB1_228
# %bb.115:
	cmp	edi, 9
	je	.LBB1_341
# %bb.116:
	cmp	edi, 11
	je	.LBB1_344
# %bb.117:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.118:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.119:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_120
# %bb.481:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_788
# %bb.482:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_788
.LBB1_120:
	xor	ecx, ecx
.LBB1_1211:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1213
.LBB1_1212:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1212
.LBB1_1213:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1214:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1214
	jmp	.LBB1_1807
.LBB1_121:
	cmp	edi, 8
	jle	.LBB1_233
# %bb.122:
	cmp	edi, 9
	je	.LBB1_347
# %bb.123:
	cmp	edi, 11
	je	.LBB1_350
# %bb.124:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.125:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.126:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_127
# %bb.484:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_791
# %bb.485:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_791
.LBB1_127:
	xor	ecx, ecx
.LBB1_1219:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1221
.LBB1_1220:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1220
.LBB1_1221:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1222:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1222
	jmp	.LBB1_1807
.LBB1_128:
	cmp	edi, 8
	jle	.LBB1_238
# %bb.129:
	cmp	edi, 9
	je	.LBB1_353
# %bb.130:
	cmp	edi, 11
	je	.LBB1_356
# %bb.131:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.132:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.133:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_134
# %bb.487:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_794
# %bb.488:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_794
.LBB1_134:
	xor	ecx, ecx
.LBB1_1227:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_1229
.LBB1_1228:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_1228
.LBB1_1229:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1230:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1230
	jmp	.LBB1_1807
.LBB1_135:
	cmp	edi, 8
	jle	.LBB1_243
# %bb.136:
	cmp	edi, 9
	je	.LBB1_359
# %bb.137:
	cmp	edi, 11
	je	.LBB1_362
# %bb.138:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.139:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.140:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_141
# %bb.490:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_797
# %bb.491:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_797
.LBB1_141:
	xor	ecx, ecx
.LBB1_1235:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1237
.LBB1_1236:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1236
.LBB1_1237:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1238:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1238
	jmp	.LBB1_1807
.LBB1_142:
	cmp	edi, 8
	jle	.LBB1_248
# %bb.143:
	cmp	edi, 9
	je	.LBB1_365
# %bb.144:
	cmp	edi, 11
	je	.LBB1_368
# %bb.145:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.146:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.147:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_148
# %bb.493:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_800
# %bb.494:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_800
.LBB1_148:
	xor	ecx, ecx
.LBB1_1243:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_1245
.LBB1_1244:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_1244
.LBB1_1245:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1246:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1246
	jmp	.LBB1_1807
.LBB1_149:
	cmp	edi, 8
	jle	.LBB1_253
# %bb.150:
	cmp	edi, 9
	je	.LBB1_371
# %bb.151:
	cmp	edi, 11
	je	.LBB1_374
# %bb.152:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.153:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.154:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_155
# %bb.496:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_803
# %bb.497:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_803
.LBB1_155:
	xor	ecx, ecx
.LBB1_1251:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1253
.LBB1_1252:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1252
.LBB1_1253:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1254:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1254
	jmp	.LBB1_1807
.LBB1_156:
	cmp	edi, 8
	jle	.LBB1_258
# %bb.157:
	cmp	edi, 9
	je	.LBB1_377
# %bb.158:
	cmp	edi, 11
	je	.LBB1_380
# %bb.159:
	cmp	edi, 12
	jne	.LBB1_1807
# %bb.160:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.161:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_162
# %bb.499:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_806
# %bb.500:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_806
.LBB1_162:
	xor	ecx, ecx
.LBB1_1259:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1261
.LBB1_1260:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1260
.LBB1_1261:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1262:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1262
	jmp	.LBB1_1807
.LBB1_163:
	cmp	edi, 2
	je	.LBB1_383
# %bb.164:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.165:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.166:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_167
# %bb.502:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_809
# %bb.503:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_809
.LBB1_167:
	xor	ecx, ecx
.LBB1_1267:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1269
.LBB1_1268:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1268
.LBB1_1269:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1270:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1270
	jmp	.LBB1_1807
.LBB1_168:
	cmp	edi, 2
	je	.LBB1_386
# %bb.169:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.170:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.171:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_172
# %bb.505:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_812
# %bb.506:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_812
.LBB1_172:
	xor	ecx, ecx
.LBB1_1275:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1277
.LBB1_1276:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1276
.LBB1_1277:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1278:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1278
	jmp	.LBB1_1807
.LBB1_173:
	cmp	edi, 2
	je	.LBB1_389
# %bb.174:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.175:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.176:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_177
# %bb.508:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_815
# %bb.509:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_815
.LBB1_177:
	xor	esi, esi
.LBB1_1283:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1285
.LBB1_1284:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1284
.LBB1_1285:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1286:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1286
	jmp	.LBB1_1807
.LBB1_178:
	cmp	edi, 2
	je	.LBB1_392
# %bb.179:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.180:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.181:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_182
# %bb.511:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_818
# %bb.512:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_818
.LBB1_182:
	xor	esi, esi
.LBB1_1291:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1293
.LBB1_1292:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1292
.LBB1_1293:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1294:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1294
	jmp	.LBB1_1807
.LBB1_183:
	cmp	edi, 2
	je	.LBB1_395
# %bb.184:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.185:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.186:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_187
# %bb.514:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_821
# %bb.515:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_821
.LBB1_187:
	xor	edi, edi
.LBB1_1299:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1301
.LBB1_1300:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1300
.LBB1_1301:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1302:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1302
	jmp	.LBB1_1807
.LBB1_188:
	cmp	edi, 2
	je	.LBB1_398
# %bb.189:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.190:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.191:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_192
# %bb.517:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_824
# %bb.518:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_824
.LBB1_192:
	xor	ecx, ecx
.LBB1_1307:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB1_1309
# %bb.1308:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_1309:
	add	rsi, r10
	je	.LBB1_1807
.LBB1_1310:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rcx]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx], al
	movsx	eax, byte ptr [rdx + rcx + 1]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx + 1], al
	add	rcx, 2
	cmp	r10, rcx
	jne	.LBB1_1310
	jmp	.LBB1_1807
.LBB1_193:
	cmp	edi, 2
	je	.LBB1_401
# %bb.194:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.195:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.196:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_197
# %bb.520:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_827
# %bb.521:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_827
.LBB1_197:
	xor	edi, edi
.LBB1_1315:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1317
.LBB1_1316:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1316
.LBB1_1317:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1318:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1318
	jmp	.LBB1_1807
.LBB1_198:
	cmp	edi, 2
	je	.LBB1_404
# %bb.199:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.200:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.201:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_202
# %bb.523:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_830
# %bb.524:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_830
.LBB1_202:
	xor	ecx, ecx
.LBB1_1323:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB1_1325
# %bb.1324:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_1325:
	add	rsi, r10
	je	.LBB1_1807
.LBB1_1326:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rcx]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx], al
	movsx	eax, byte ptr [rdx + rcx + 1]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx + 1], al
	add	rcx, 2
	cmp	r10, rcx
	jne	.LBB1_1326
	jmp	.LBB1_1807
.LBB1_203:
	cmp	edi, 2
	je	.LBB1_407
# %bb.204:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.205:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.206:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_207
# %bb.526:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_833
# %bb.527:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_833
.LBB1_207:
	xor	esi, esi
.LBB1_1331:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1333
.LBB1_1332:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1332
.LBB1_1333:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1334:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1334
	jmp	.LBB1_1807
.LBB1_208:
	cmp	edi, 2
	je	.LBB1_410
# %bb.209:
	cmp	edi, 3
	jne	.LBB1_1807
# %bb.210:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.211:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_212
# %bb.529:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_836
# %bb.530:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_836
.LBB1_212:
	xor	esi, esi
.LBB1_1339:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1341
.LBB1_1340:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1340
.LBB1_1341:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1342:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1342
	jmp	.LBB1_1807
.LBB1_213:
	cmp	edi, 7
	je	.LBB1_413
# %bb.214:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.215:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.216:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_217
# %bb.532:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_839
# %bb.533:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_839
.LBB1_217:
	xor	ecx, ecx
.LBB1_1347:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1349
.LBB1_1348:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1348
.LBB1_1349:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1350:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1350
	jmp	.LBB1_1807
.LBB1_218:
	cmp	edi, 7
	je	.LBB1_416
# %bb.219:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.220:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.221:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB1_535
# %bb.222:
	xor	ecx, ecx
	jmp	.LBB1_848
.LBB1_223:
	cmp	edi, 7
	je	.LBB1_419
# %bb.224:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.225:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.226:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_227
# %bb.537:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_849
# %bb.538:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_849
.LBB1_227:
	xor	esi, esi
.LBB1_1355:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1357
.LBB1_1356:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1356
.LBB1_1357:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1358:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1358
	jmp	.LBB1_1807
.LBB1_228:
	cmp	edi, 7
	je	.LBB1_422
# %bb.229:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.230:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.231:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_232
# %bb.540:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_852
# %bb.541:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_852
.LBB1_232:
	xor	esi, esi
.LBB1_1363:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1365
.LBB1_1364:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1364
.LBB1_1365:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1366:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1366
	jmp	.LBB1_1807
.LBB1_233:
	cmp	edi, 7
	je	.LBB1_425
# %bb.234:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.235:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.236:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_543
# %bb.237:
	xor	edi, edi
	jmp	.LBB1_545
.LBB1_238:
	cmp	edi, 7
	je	.LBB1_428
# %bb.239:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.240:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.241:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_242
# %bb.548:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_855
# %bb.549:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_855
.LBB1_242:
	xor	ecx, ecx
.LBB1_1069:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1071
.LBB1_1070:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1070
.LBB1_1071:
	cmp	rax, 3
	jb	.LBB1_1807
.LBB1_1072:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1072
	jmp	.LBB1_1807
.LBB1_243:
	cmp	edi, 7
	je	.LBB1_431
# %bb.244:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.245:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.246:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_551
# %bb.247:
	xor	edi, edi
	jmp	.LBB1_553
.LBB1_248:
	cmp	edi, 7
	je	.LBB1_434
# %bb.249:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.250:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.251:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_252
# %bb.556:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_857
# %bb.557:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_857
.LBB1_252:
	xor	ecx, ecx
.LBB1_1079:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1081
.LBB1_1080:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1080
.LBB1_1081:
	cmp	rax, 3
	jb	.LBB1_1807
.LBB1_1082:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1082
	jmp	.LBB1_1807
.LBB1_253:
	cmp	edi, 7
	je	.LBB1_437
# %bb.254:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.255:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.256:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_257
# %bb.559:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_859
# %bb.560:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_859
.LBB1_257:
	xor	esi, esi
.LBB1_1371:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1373
.LBB1_1372:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1372
.LBB1_1373:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1374:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1374
	jmp	.LBB1_1807
.LBB1_258:
	cmp	edi, 7
	je	.LBB1_440
# %bb.259:
	cmp	edi, 8
	jne	.LBB1_1807
# %bb.260:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.261:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_262
# %bb.562:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_862
# %bb.563:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_862
.LBB1_262:
	xor	esi, esi
.LBB1_1379:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1381
.LBB1_1380:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1380
.LBB1_1381:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1382:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1382
	jmp	.LBB1_1807
.LBB1_263:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.264:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_265
# %bb.565:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_865
# %bb.566:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_865
.LBB1_265:
	xor	ecx, ecx
.LBB1_1387:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1389
.LBB1_1388:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1388
.LBB1_1389:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1390:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1390
	jmp	.LBB1_1807
.LBB1_266:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.267:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_268
# %bb.568:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_868
# %bb.569:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_868
.LBB1_268:
	xor	ecx, ecx
.LBB1_1395:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1397
.LBB1_1396:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1396
.LBB1_1397:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1398:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1398
	jmp	.LBB1_1807
.LBB1_269:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.270:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB1_571
# %bb.271:
	xor	ecx, ecx
	jmp	.LBB1_877
.LBB1_272:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.273:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_274
# %bb.573:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_878
# %bb.574:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_878
.LBB1_274:
	xor	ecx, ecx
.LBB1_1403:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1405
.LBB1_1404:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1404
.LBB1_1405:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1406:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1406
	jmp	.LBB1_1807
.LBB1_275:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.276:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_277
# %bb.576:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_881
# %bb.577:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_881
.LBB1_277:
	xor	esi, esi
.LBB1_1411:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1413
.LBB1_1412:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1412
.LBB1_1413:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1414:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1414
	jmp	.LBB1_1807
.LBB1_278:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.279:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_280
# %bb.579:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_884
# %bb.580:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_884
.LBB1_280:
	xor	esi, esi
.LBB1_1419:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1421
.LBB1_1420:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1420
.LBB1_1421:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1422:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1422
	jmp	.LBB1_1807
.LBB1_281:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.282:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_283
# %bb.582:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_887
# %bb.583:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_887
.LBB1_283:
	xor	esi, esi
.LBB1_1427:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1429
.LBB1_1428:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1428
.LBB1_1429:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1430:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1430
	jmp	.LBB1_1807
.LBB1_284:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.285:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_286
# %bb.585:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_890
# %bb.586:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_890
.LBB1_286:
	xor	esi, esi
.LBB1_1435:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1437
.LBB1_1436:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1436
.LBB1_1437:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1438:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1438
	jmp	.LBB1_1807
.LBB1_287:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.288:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_289
# %bb.588:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_893
# %bb.589:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_893
.LBB1_289:
	xor	esi, esi
.LBB1_1443:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1445
.LBB1_1444:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1444
.LBB1_1445:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1446:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1446
	jmp	.LBB1_1807
.LBB1_290:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.291:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_292
# %bb.591:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_896
# %bb.592:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_896
.LBB1_292:
	xor	esi, esi
.LBB1_1451:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1453
.LBB1_1452:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1452
.LBB1_1453:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1454:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1454
	jmp	.LBB1_1807
.LBB1_293:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.294:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_295
# %bb.594:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_899
# %bb.595:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_899
.LBB1_295:
	xor	ecx, ecx
.LBB1_1089:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1091
.LBB1_1090:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1090
.LBB1_1091:
	cmp	rax, 3
	jb	.LBB1_1807
.LBB1_1092:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1092
	jmp	.LBB1_1807
.LBB1_296:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.297:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_298
# %bb.597:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_901
# %bb.598:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_901
.LBB1_298:
	xor	ecx, ecx
.LBB1_1459:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1461
# %bb.1460:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1461:
	add	rsi, rax
	je	.LBB1_1807
.LBB1_1462:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1462
	jmp	.LBB1_1807
.LBB1_299:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.300:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_301
# %bb.600:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_904
# %bb.601:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_904
.LBB1_301:
	xor	esi, esi
.LBB1_1467:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1469
.LBB1_1468:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1468
.LBB1_1469:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1470:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1470
	jmp	.LBB1_1807
.LBB1_302:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.303:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_304
# %bb.603:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_907
# %bb.604:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_907
.LBB1_304:
	xor	esi, esi
.LBB1_1475:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1477
.LBB1_1476:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1476
.LBB1_1477:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1478:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1478
	jmp	.LBB1_1807
.LBB1_305:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.306:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_307
# %bb.606:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_910
# %bb.607:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_910
.LBB1_307:
	xor	ecx, ecx
.LBB1_1099:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1101
.LBB1_1100:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1100
.LBB1_1101:
	cmp	rax, 3
	jb	.LBB1_1807
.LBB1_1102:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1102
	jmp	.LBB1_1807
.LBB1_308:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.309:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_310
# %bb.609:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_912
# %bb.610:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_912
.LBB1_310:
	xor	ecx, ecx
.LBB1_1483:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1485
# %bb.1484:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1485:
	add	rsi, rax
	je	.LBB1_1807
.LBB1_1486:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1486
	jmp	.LBB1_1807
.LBB1_311:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.312:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_313
# %bb.612:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_915
# %bb.613:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_915
.LBB1_313:
	xor	esi, esi
.LBB1_1491:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1493
.LBB1_1492:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1492
.LBB1_1493:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1494:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1494
	jmp	.LBB1_1807
.LBB1_314:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.315:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_316
# %bb.615:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_918
# %bb.616:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_918
.LBB1_316:
	xor	esi, esi
.LBB1_1499:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1501
.LBB1_1500:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1500
.LBB1_1501:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1502:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1502
	jmp	.LBB1_1807
.LBB1_317:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.318:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_319
# %bb.618:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_921
# %bb.619:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_921
.LBB1_319:
	xor	esi, esi
.LBB1_1507:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1509
.LBB1_1508:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1508
.LBB1_1509:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1510:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1510
	jmp	.LBB1_1807
.LBB1_320:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.321:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_322
# %bb.621:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_924
# %bb.622:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_924
.LBB1_322:
	xor	esi, esi
.LBB1_1515:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1517
.LBB1_1516:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1516
.LBB1_1517:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1518:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1518
	jmp	.LBB1_1807
.LBB1_323:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.324:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_325
# %bb.624:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_927
# %bb.625:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_927
.LBB1_325:
	xor	ecx, ecx
.LBB1_1523:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1525
.LBB1_1524:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1524
.LBB1_1525:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1526:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1526
	jmp	.LBB1_1807
.LBB1_326:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.327:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_328
# %bb.627:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_930
# %bb.628:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_930
.LBB1_328:
	xor	ecx, ecx
.LBB1_1531:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1534
# %bb.1532:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1533:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1533
.LBB1_1534:
	cmp	rsi, 3
	jb	.LBB1_1807
# %bb.1535:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1536:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 4], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 8], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1536
	jmp	.LBB1_1807
.LBB1_329:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.330:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_331
# %bb.630:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_933
# %bb.631:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_933
.LBB1_331:
	xor	ecx, ecx
.LBB1_1541:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1543
.LBB1_1542:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1542
.LBB1_1543:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1544:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1544
	jmp	.LBB1_1807
.LBB1_332:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.333:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_334
# %bb.633:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_936
# %bb.634:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_936
.LBB1_334:
	xor	ecx, ecx
.LBB1_1549:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1552
# %bb.1550:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1551:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1551
.LBB1_1552:
	cmp	rsi, 3
	jb	.LBB1_1807
# %bb.1553:
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1554:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 4], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 8], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1554
	jmp	.LBB1_1807
.LBB1_335:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.336:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_337
# %bb.636:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_939
# %bb.637:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_939
.LBB1_337:
	xor	esi, esi
.LBB1_1559:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1561
.LBB1_1560:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1560
.LBB1_1561:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1562:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1562
	jmp	.LBB1_1807
.LBB1_338:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.339:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_340
# %bb.639:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_942
# %bb.640:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_942
.LBB1_340:
	xor	ecx, ecx
.LBB1_1567:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1569
.LBB1_1568:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1568
.LBB1_1569:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1570:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1570
	jmp	.LBB1_1807
.LBB1_341:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.342:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_343
# %bb.642:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_945
# %bb.643:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_945
.LBB1_343:
	xor	esi, esi
.LBB1_1575:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1577
.LBB1_1576:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1576
.LBB1_1577:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1578:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1578
	jmp	.LBB1_1807
.LBB1_344:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.345:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_346
# %bb.645:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_948
# %bb.646:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_948
.LBB1_346:
	xor	ecx, ecx
.LBB1_1583:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1585
.LBB1_1584:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1584
.LBB1_1585:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1586:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1586
	jmp	.LBB1_1807
.LBB1_347:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.348:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_648
# %bb.349:
	xor	edi, edi
	jmp	.LBB1_650
.LBB1_350:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.351:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_352
# %bb.653:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_951
# %bb.654:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_951
.LBB1_352:
	xor	ecx, ecx
.LBB1_1591:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1593
.LBB1_1592:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1592
.LBB1_1593:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1594:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1594
	jmp	.LBB1_1807
.LBB1_353:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.354:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_355
# %bb.656:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_954
# %bb.657:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_954
.LBB1_355:
	xor	ecx, ecx
.LBB1_1599:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1601
# %bb.1600:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_1601:
	add	rsi, rax
	je	.LBB1_1807
.LBB1_1602:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1602
	jmp	.LBB1_1807
.LBB1_356:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.357:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_358
# %bb.659:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_957
# %bb.660:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_957
.LBB1_358:
	xor	ecx, ecx
.LBB1_1607:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1610
# %bb.1608:
	mov	esi, 2147483647
.LBB1_1609:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1609
.LBB1_1610:
	cmp	r9, 3
	jb	.LBB1_1807
# %bb.1611:
	mov	esi, 2147483647
.LBB1_1612:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1612
	jmp	.LBB1_1807
.LBB1_359:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.360:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_662
# %bb.361:
	xor	edi, edi
	jmp	.LBB1_664
.LBB1_362:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.363:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_364
# %bb.667:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_960
# %bb.668:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_960
.LBB1_364:
	xor	ecx, ecx
.LBB1_1617:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1619
.LBB1_1618:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1618
.LBB1_1619:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1620:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1620
	jmp	.LBB1_1807
.LBB1_365:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.366:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_367
# %bb.670:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_963
# %bb.671:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_963
.LBB1_367:
	xor	ecx, ecx
.LBB1_1625:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1627
# %bb.1626:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_1627:
	add	rsi, rax
	je	.LBB1_1807
.LBB1_1628:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1628
	jmp	.LBB1_1807
.LBB1_368:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.369:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_370
# %bb.673:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_966
# %bb.674:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_966
.LBB1_370:
	xor	ecx, ecx
.LBB1_1633:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1636
# %bb.1634:
	mov	esi, 2147483647
.LBB1_1635:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1635
.LBB1_1636:
	cmp	r9, 3
	jb	.LBB1_1807
# %bb.1637:
	mov	esi, 2147483647
.LBB1_1638:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1638
	jmp	.LBB1_1807
.LBB1_371:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.372:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_373
# %bb.676:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_969
# %bb.677:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_969
.LBB1_373:
	xor	esi, esi
.LBB1_1643:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1645
.LBB1_1644:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1644
.LBB1_1645:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1646:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1646
	jmp	.LBB1_1807
.LBB1_374:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.375:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_376
# %bb.679:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_972
# %bb.680:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_972
.LBB1_376:
	xor	ecx, ecx
.LBB1_1651:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1653
.LBB1_1652:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1652
.LBB1_1653:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1654:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1654
	jmp	.LBB1_1807
.LBB1_377:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.378:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_379
# %bb.682:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_975
# %bb.683:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_975
.LBB1_379:
	xor	esi, esi
.LBB1_1659:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1661
.LBB1_1660:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1660
.LBB1_1661:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1662:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1662
	jmp	.LBB1_1807
.LBB1_380:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.381:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_382
# %bb.685:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_978
# %bb.686:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_978
.LBB1_382:
	xor	ecx, ecx
.LBB1_1667:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1669
.LBB1_1668:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1668
.LBB1_1669:
	cmp	rsi, 3
	jb	.LBB1_1807
.LBB1_1670:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1670
	jmp	.LBB1_1807
.LBB1_383:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.384:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_385
# %bb.688:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_981
# %bb.689:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_981
.LBB1_385:
	xor	ecx, ecx
.LBB1_1675:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1677
.LBB1_1676:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1676
.LBB1_1677:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1678:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB1_1678
	jmp	.LBB1_1807
.LBB1_386:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.387:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB1_691
# %bb.388:
	xor	ecx, ecx
	jmp	.LBB1_990
.LBB1_389:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.390:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_391
# %bb.693:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_991
# %bb.694:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_991
.LBB1_391:
	xor	esi, esi
.LBB1_1683:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1685
.LBB1_1684:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1684
.LBB1_1685:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1686:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1686
	jmp	.LBB1_1807
.LBB1_392:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.393:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_394
# %bb.696:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_994
# %bb.697:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_994
.LBB1_394:
	xor	esi, esi
.LBB1_1691:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1693
.LBB1_1692:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1692
.LBB1_1693:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1694:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1694
	jmp	.LBB1_1807
.LBB1_395:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.396:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_397
# %bb.699:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_997
# %bb.700:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_997
.LBB1_397:
	xor	edi, edi
.LBB1_1699:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1701
.LBB1_1700:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1700
.LBB1_1701:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1702:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1702
	jmp	.LBB1_1807
.LBB1_398:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.399:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_400
# %bb.702:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_1000
# %bb.703:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_1000
.LBB1_400:
	xor	ecx, ecx
.LBB1_1109:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1111
.LBB1_1110:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1110
.LBB1_1111:
	cmp	rdi, 3
	jb	.LBB1_1807
.LBB1_1112:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1112
	jmp	.LBB1_1807
.LBB1_401:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.402:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_403
# %bb.705:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_1002
# %bb.706:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_1002
.LBB1_403:
	xor	edi, edi
.LBB1_1707:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1709
.LBB1_1708:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1708
.LBB1_1709:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1710:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1710
	jmp	.LBB1_1807
.LBB1_404:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.405:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_406
# %bb.708:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_1005
# %bb.709:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_1005
.LBB1_406:
	xor	ecx, ecx
.LBB1_1119:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1121
.LBB1_1120:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1120
.LBB1_1121:
	cmp	rdi, 3
	jb	.LBB1_1807
.LBB1_1122:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1122
	jmp	.LBB1_1807
.LBB1_407:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.408:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_409
# %bb.711:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1007
# %bb.712:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1007
.LBB1_409:
	xor	esi, esi
.LBB1_1715:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1717
.LBB1_1716:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1716
.LBB1_1717:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1718:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1718
	jmp	.LBB1_1807
.LBB1_410:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.411:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_412
# %bb.714:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1010
# %bb.715:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1010
.LBB1_412:
	xor	esi, esi
.LBB1_1723:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1725
.LBB1_1724:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1724
.LBB1_1725:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1726:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1726
	jmp	.LBB1_1807
.LBB1_413:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.414:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_415
# %bb.717:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1013
# %bb.718:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1013
.LBB1_415:
	xor	ecx, ecx
.LBB1_1731:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1733
.LBB1_1732:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1732
.LBB1_1733:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1734:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1734
	jmp	.LBB1_1807
.LBB1_416:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.417:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_418
# %bb.720:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1016
# %bb.721:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1016
.LBB1_418:
	xor	ecx, ecx
.LBB1_1739:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1741
.LBB1_1740:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1740
.LBB1_1741:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1742:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1742
	jmp	.LBB1_1807
.LBB1_419:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.420:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_421
# %bb.723:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1019
# %bb.724:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1019
.LBB1_421:
	xor	esi, esi
.LBB1_1747:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1749
.LBB1_1748:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1748
.LBB1_1749:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1750:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1750
	jmp	.LBB1_1807
.LBB1_422:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.423:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_424
# %bb.726:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1022
# %bb.727:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1022
.LBB1_424:
	xor	esi, esi
.LBB1_1755:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1757
.LBB1_1756:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1756
.LBB1_1757:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1758:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1758
	jmp	.LBB1_1807
.LBB1_425:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.426:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_427
# %bb.729:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1025
# %bb.730:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1025
.LBB1_427:
	xor	esi, esi
.LBB1_1763:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1765
.LBB1_1764:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1764
.LBB1_1765:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1766:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1766
	jmp	.LBB1_1807
.LBB1_428:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.429:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_430
# %bb.732:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1028
# %bb.733:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1028
.LBB1_430:
	xor	ecx, ecx
.LBB1_1771:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1773
# %bb.1772:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_1773:
	add	rsi, rax
	je	.LBB1_1807
.LBB1_1774:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1774
	jmp	.LBB1_1807
.LBB1_431:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.432:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_433
# %bb.735:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1031
# %bb.736:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1031
.LBB1_433:
	xor	esi, esi
.LBB1_1779:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1781
.LBB1_1780:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1780
.LBB1_1781:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1782:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1782
	jmp	.LBB1_1807
.LBB1_434:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.435:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_436
# %bb.738:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1034
# %bb.739:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1034
.LBB1_436:
	xor	ecx, ecx
.LBB1_1787:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1789
# %bb.1788:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_1789:
	add	rsi, rax
	je	.LBB1_1807
.LBB1_1790:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1790
	jmp	.LBB1_1807
.LBB1_437:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.438:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_439
# %bb.741:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1037
# %bb.742:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1037
.LBB1_439:
	xor	esi, esi
.LBB1_1795:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1797
.LBB1_1796:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1796
.LBB1_1797:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1798:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1798
	jmp	.LBB1_1807
.LBB1_440:
	test	r9d, r9d
	jle	.LBB1_1807
# %bb.441:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_442
# %bb.744:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1040
# %bb.745:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1040
.LBB1_442:
	xor	esi, esi
.LBB1_1803:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1805
.LBB1_1804:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1804
.LBB1_1805:
	cmp	r9, 3
	jb	.LBB1_1807
.LBB1_1806:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1806
	jmp	.LBB1_1807
.LBB1_446:
	mov	ecx, eax
	and	ecx, -8
	lea	rdi, [rcx - 8]
	mov	rsi, rdi
	shr	rsi, 3
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 56
	jae	.LBB1_750
# %bb.447:
	xor	edi, edi
	jmp	.LBB1_752
.LBB1_535:
	mov	ecx, eax
	and	ecx, -4
	lea	rdi, [rcx - 4]
	mov	rsi, rdi
	shr	rsi, 2
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 28
	jae	.LBB1_842
# %bb.536:
	xor	edi, edi
	jmp	.LBB1_844
.LBB1_543:
	and	esi, -4
	xor	edi, edi
.LBB1_544:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rcx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB1_544
.LBB1_545:
	test	r9, r9
	je	.LBB1_1807
# %bb.546:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_547:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_547
	jmp	.LBB1_1807
.LBB1_551:
	and	esi, -4
	xor	edi, edi
.LBB1_552:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rcx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB1_552
.LBB1_553:
	test	r9, r9
	je	.LBB1_1807
# %bb.554:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_555:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_555
	jmp	.LBB1_1807
.LBB1_571:
	mov	ecx, eax
	and	ecx, -16
	lea	rdi, [rcx - 16]
	mov	rsi, rdi
	shr	rsi, 4
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 112
	jae	.LBB1_871
# %bb.572:
	xor	edi, edi
	jmp	.LBB1_873
.LBB1_648:
	and	esi, -4
	xor	edi, edi
.LBB1_649:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rcx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB1_649
.LBB1_650:
	test	r9, r9
	je	.LBB1_1807
# %bb.651:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_652:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_652
	jmp	.LBB1_1807
.LBB1_662:
	and	esi, -4
	xor	edi, edi
.LBB1_663:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 8]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 16]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rcx
	mov	rcx, qword ptr [rdx + 8*rdi + 24]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rcx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB1_663
.LBB1_664:
	test	r9, r9
	je	.LBB1_1807
# %bb.665:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_666:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_666
	jmp	.LBB1_1807
.LBB1_691:
	mov	ecx, eax
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 224
	jae	.LBB1_984
# %bb.692:
	xor	edi, edi
	jmp	.LBB1_986
.LBB1_747:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1123
# %bb.748:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_749:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_749
	jmp	.LBB1_1124
.LBB1_757:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1131
# %bb.758:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_759:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_759
	jmp	.LBB1_1132
.LBB1_760:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1139
# %bb.761:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_762:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_762
	jmp	.LBB1_1140
.LBB1_763:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1147
# %bb.764:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_765:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_765
	jmp	.LBB1_1148
.LBB1_766:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB1_1043
# %bb.767:
	xor	eax, eax
	jmp	.LBB1_1045
.LBB1_768:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1155
# %bb.769:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_770:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_770
	jmp	.LBB1_1156
.LBB1_771:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB1_1053
# %bb.772:
	xor	eax, eax
	jmp	.LBB1_1055
.LBB1_773:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1163
# %bb.774:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_775:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_775
	jmp	.LBB1_1164
.LBB1_776:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1171
# %bb.777:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_778:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_778
	jmp	.LBB1_1172
.LBB1_779:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1179
# %bb.780:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_781:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_781
	jmp	.LBB1_1180
.LBB1_782:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1189
# %bb.783:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_784:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_784
	jmp	.LBB1_1190
.LBB1_785:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1199
# %bb.786:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_787:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_787
	jmp	.LBB1_1200
.LBB1_788:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1207
# %bb.789:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_790:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_790
	jmp	.LBB1_1208
.LBB1_791:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1215
# %bb.792:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_793:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_793
	jmp	.LBB1_1216
.LBB1_794:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB1_1223
# %bb.795:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_2] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB1_796:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_796
	jmp	.LBB1_1224
.LBB1_797:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1231
# %bb.798:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_799:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_799
	jmp	.LBB1_1232
.LBB1_800:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB1_1239
# %bb.801:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_2] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB1_802:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_802
	jmp	.LBB1_1240
.LBB1_803:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1247
# %bb.804:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_805:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_805
	jmp	.LBB1_1248
.LBB1_806:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1255
# %bb.807:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_808:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_808
	jmp	.LBB1_1256
.LBB1_809:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1263
# %bb.810:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_811:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_811
	jmp	.LBB1_1264
.LBB1_812:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1271
# %bb.813:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_814:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_814
	jmp	.LBB1_1272
.LBB1_815:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1279
# %bb.816:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_817:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_817
	jmp	.LBB1_1280
.LBB1_818:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1287
# %bb.819:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_820:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_820
	jmp	.LBB1_1288
.LBB1_821:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, cl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB1_1295
# %bb.822:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_823:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + rax]
	movdqu	xmm6, xmmword ptr [rdx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + rax + 32]
	movdqu	xmm6, xmmword ptr [rdx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB1_823
	jmp	.LBB1_1296
.LBB1_824:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1303
# %bb.825:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI1_4] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB1_826:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rsi]
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm3
	psrad	xmm6, 7
	movdqa	xmm7, xmm1
	psrad	xmm7, 7
	movdqa	xmm0, xmm4
	psrad	xmm0, 7
	paddd	xmm4, xmm0
	paddd	xmm1, xmm7
	paddd	xmm3, xmm6
	paddd	xmm2, xmm5
	pxor	xmm2, xmm5
	pxor	xmm3, xmm6
	pxor	xmm1, xmm7
	pxor	xmm4, xmm0
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 16]
	movdqa	xmm0, xmm2
	psrad	xmm0, 7
	movdqa	xmm5, xmm3
	psrad	xmm5, 7
	movdqa	xmm6, xmm1
	psrad	xmm6, 7
	movdqa	xmm7, xmm4
	psrad	xmm7, 7
	paddd	xmm4, xmm7
	paddd	xmm1, xmm6
	paddd	xmm3, xmm5
	paddd	xmm2, xmm0
	pxor	xmm2, xmm0
	pxor	xmm3, xmm5
	pxor	xmm1, xmm6
	pxor	xmm4, xmm7
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi + 16], xmm2
	add	rsi, 32
	add	rdi, 2
	jne	.LBB1_826
	jmp	.LBB1_1304
.LBB1_827:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, cl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB1_1311
# %bb.828:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_829:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + rax]
	movdqu	xmm6, xmmword ptr [rdx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + rax + 32]
	movdqu	xmm6, xmmword ptr [rdx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB1_829
	jmp	.LBB1_1312
.LBB1_830:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1319
# %bb.831:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI1_4] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB1_832:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rsi]
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm3
	psrad	xmm6, 7
	movdqa	xmm7, xmm1
	psrad	xmm7, 7
	movdqa	xmm0, xmm4
	psrad	xmm0, 7
	paddd	xmm4, xmm0
	paddd	xmm1, xmm7
	paddd	xmm3, xmm6
	paddd	xmm2, xmm5
	pxor	xmm2, xmm5
	pxor	xmm3, xmm6
	pxor	xmm1, xmm7
	pxor	xmm4, xmm0
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 16]
	movdqa	xmm0, xmm2
	psrad	xmm0, 7
	movdqa	xmm5, xmm3
	psrad	xmm5, 7
	movdqa	xmm6, xmm1
	psrad	xmm6, 7
	movdqa	xmm7, xmm4
	psrad	xmm7, 7
	paddd	xmm4, xmm7
	paddd	xmm1, xmm6
	paddd	xmm3, xmm5
	paddd	xmm2, xmm0
	pxor	xmm2, xmm0
	pxor	xmm3, xmm5
	pxor	xmm1, xmm6
	pxor	xmm4, xmm7
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi + 16], xmm2
	add	rsi, 32
	add	rdi, 2
	jne	.LBB1_832
	jmp	.LBB1_1320
.LBB1_833:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1327
# %bb.834:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_835:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_835
	jmp	.LBB1_1328
.LBB1_836:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1335
# %bb.837:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_838:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_838
	jmp	.LBB1_1336
.LBB1_839:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1343
# %bb.840:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_841:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_841
	jmp	.LBB1_1344
.LBB1_849:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1351
# %bb.850:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_851:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_851
	jmp	.LBB1_1352
.LBB1_852:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1359
# %bb.853:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_854:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_854
	jmp	.LBB1_1360
.LBB1_855:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB1_1063
# %bb.856:
	xor	eax, eax
	jmp	.LBB1_1065
.LBB1_857:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB1_1073
# %bb.858:
	xor	eax, eax
	jmp	.LBB1_1075
.LBB1_859:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1367
# %bb.860:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_861:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_861
	jmp	.LBB1_1368
.LBB1_862:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1375
# %bb.863:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_864:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_864
	jmp	.LBB1_1376
.LBB1_865:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1383
# %bb.866:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_867:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_867
	jmp	.LBB1_1384
.LBB1_868:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1391
# %bb.869:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_870:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_870
	jmp	.LBB1_1392
.LBB1_878:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1399
# %bb.879:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_880:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_880
	jmp	.LBB1_1400
.LBB1_881:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1407
# %bb.882:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_883:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_883
	jmp	.LBB1_1408
.LBB1_884:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1415
# %bb.885:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_886:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_886
	jmp	.LBB1_1416
.LBB1_887:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1423
# %bb.888:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_889:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_889
	jmp	.LBB1_1424
.LBB1_890:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1431
# %bb.891:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_892:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_892
	jmp	.LBB1_1432
.LBB1_893:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1439
# %bb.894:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_895:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_895
	jmp	.LBB1_1440
.LBB1_896:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1447
# %bb.897:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_898:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_898
	jmp	.LBB1_1448
.LBB1_899:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB1_1083
# %bb.900:
	xor	eax, eax
	jmp	.LBB1_1085
.LBB1_901:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1455
# %bb.902:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB1_903:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi + 16]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	add	rsi, 16
	add	rdi, 2
	jne	.LBB1_903
	jmp	.LBB1_1456
.LBB1_904:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1463
# %bb.905:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_906:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_906
	jmp	.LBB1_1464
.LBB1_907:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1471
# %bb.908:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_909:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_909
	jmp	.LBB1_1472
.LBB1_910:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB1_1093
# %bb.911:
	xor	eax, eax
	jmp	.LBB1_1095
.LBB1_912:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1479
# %bb.913:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB1_914:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi + 16]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	add	rsi, 16
	add	rdi, 2
	jne	.LBB1_914
	jmp	.LBB1_1480
.LBB1_915:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1487
# %bb.916:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_917:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_917
	jmp	.LBB1_1488
.LBB1_918:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1495
# %bb.919:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_920:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_920
	jmp	.LBB1_1496
.LBB1_921:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1503
# %bb.922:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_923:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_923
	jmp	.LBB1_1504
.LBB1_924:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1511
# %bb.925:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_926:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rcx, 2
	jne	.LBB1_926
	jmp	.LBB1_1512
.LBB1_927:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1519
# %bb.928:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_929:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_929
	jmp	.LBB1_1520
.LBB1_930:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1527
# %bb.931:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_932:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_932
	jmp	.LBB1_1528
.LBB1_933:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1537
# %bb.934:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_935:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB1_935
	jmp	.LBB1_1538
.LBB1_936:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1545
# %bb.937:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_938:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_938
	jmp	.LBB1_1546
.LBB1_939:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1555
# %bb.940:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_941:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_941
	jmp	.LBB1_1556
.LBB1_942:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1563
# %bb.943:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_944:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_944
	jmp	.LBB1_1564
.LBB1_945:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1571
# %bb.946:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_947:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_947
	jmp	.LBB1_1572
.LBB1_948:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1579
# %bb.949:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_950:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_950
	jmp	.LBB1_1580
.LBB1_951:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1587
# %bb.952:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_953:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_953
	jmp	.LBB1_1588
.LBB1_954:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1595
# %bb.955:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_956:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB1_956
	jmp	.LBB1_1596
.LBB1_957:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1603
# %bb.958:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_3] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB1_959:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_959
	jmp	.LBB1_1604
.LBB1_960:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1613
# %bb.961:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_962:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_962
	jmp	.LBB1_1614
.LBB1_963:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1621
# %bb.964:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_965:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB1_965
	jmp	.LBB1_1622
.LBB1_966:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1629
# %bb.967:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_3] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB1_968:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_968
	jmp	.LBB1_1630
.LBB1_969:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1639
# %bb.970:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_971:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_971
	jmp	.LBB1_1640
.LBB1_972:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1647
# %bb.973:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_974:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_974
	jmp	.LBB1_1648
.LBB1_975:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1655
# %bb.976:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_977:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rcx, 2
	jne	.LBB1_977
	jmp	.LBB1_1656
.LBB1_978:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1663
# %bb.979:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_980:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_980
	jmp	.LBB1_1664
.LBB1_981:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1671
# %bb.982:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_983:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_983
	jmp	.LBB1_1672
.LBB1_991:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1679
# %bb.992:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_993:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_993
	jmp	.LBB1_1680
.LBB1_994:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1687
# %bb.995:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_996:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_996
	jmp	.LBB1_1688
.LBB1_997:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, cl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB1_1695
# %bb.998:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_999:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + rax]
	movdqu	xmm6, xmmword ptr [rdx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + rax + 32]
	movdqu	xmm6, xmmword ptr [rdx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB1_999
	jmp	.LBB1_1696
.LBB1_1000:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB1_1103
# %bb.1001:
	xor	edi, edi
	jmp	.LBB1_1105
.LBB1_1002:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, cl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB1_1703
# %bb.1003:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_1004:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + rax]
	movdqu	xmm6, xmmword ptr [rdx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + rax + 32]
	movdqu	xmm6, xmmword ptr [rdx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB1_1004
	jmp	.LBB1_1704
.LBB1_1005:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB1_1113
# %bb.1006:
	xor	edi, edi
	jmp	.LBB1_1115
.LBB1_1007:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1711
# %bb.1008:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1009:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_1009
	jmp	.LBB1_1712
.LBB1_1010:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1719
# %bb.1011:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1012:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rcx, 2
	jne	.LBB1_1012
	jmp	.LBB1_1720
.LBB1_1013:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1727
# %bb.1014:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1015:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_1015
	jmp	.LBB1_1728
.LBB1_1016:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1735
# %bb.1017:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1018:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_1018
	jmp	.LBB1_1736
.LBB1_1019:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1743
# %bb.1020:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1021:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_1021
	jmp	.LBB1_1744
.LBB1_1022:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1751
# %bb.1023:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1024:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_1024
	jmp	.LBB1_1752
.LBB1_1025:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1759
# %bb.1026:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1027:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_1027
	jmp	.LBB1_1760
.LBB1_1028:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1767
# %bb.1029:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1030:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_1030
	jmp	.LBB1_1768
.LBB1_1031:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1775
# %bb.1032:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1033:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_1033
	jmp	.LBB1_1776
.LBB1_1034:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1783
# %bb.1035:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1036:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rsi, 2
	jne	.LBB1_1036
	jmp	.LBB1_1784
.LBB1_1037:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1791
# %bb.1038:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1039:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_1039
	jmp	.LBB1_1792
.LBB1_1040:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1799
# %bb.1041:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1042:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rcx, 2
	jne	.LBB1_1042
	jmp	.LBB1_1800
.LBB1_750:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB1_751:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 240], xmm0
	add	rdi, 64
	add	rsi, 8
	jne	.LBB1_751
.LBB1_752:
	test	rdx, rdx
	je	.LBB1_755
# %bb.753:
	lea	rsi, [r8 + 4*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB1_754:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB1_754
.LBB1_755:
	cmp	rcx, rax
	je	.LBB1_1807
.LBB1_756:                              # =>This Inner Loop Header: Depth=1
	mov	dword ptr [r8 + 4*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_756
	jmp	.LBB1_1807
.LBB1_842:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB1_843:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 240], xmm0
	add	rdi, 32
	add	rsi, 8
	jne	.LBB1_843
.LBB1_844:
	test	rdx, rdx
	je	.LBB1_847
# %bb.845:
	lea	rsi, [r8 + 8*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB1_846:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB1_846
.LBB1_847:
	cmp	rcx, rax
	je	.LBB1_1807
.LBB1_848:                              # =>This Inner Loop Header: Depth=1
	mov	qword ptr [r8 + 8*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_848
	jmp	.LBB1_1807
.LBB1_871:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB1_872:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 240], xmm0
	sub	rdi, -128
	add	rsi, 8
	jne	.LBB1_872
.LBB1_873:
	test	rdx, rdx
	je	.LBB1_876
# %bb.874:
	lea	rsi, [r8 + 2*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB1_875:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB1_875
.LBB1_876:
	cmp	rcx, rax
	je	.LBB1_1807
.LBB1_877:                              # =>This Inner Loop Header: Depth=1
	mov	word ptr [r8 + 2*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_877
	jmp	.LBB1_1807
.LBB1_984:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB1_985:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + rdi + 240], xmm0
	add	rdi, 256
	add	rsi, 8
	jne	.LBB1_985
.LBB1_986:
	test	rdx, rdx
	je	.LBB1_989
# %bb.987:
	lea	rsi, [rdi + r8]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB1_988:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB1_988
.LBB1_989:
	cmp	rcx, rax
	je	.LBB1_1807
.LBB1_990:                              # =>This Inner Loop Header: Depth=1
	mov	byte ptr [r8 + rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_990
.LBB1_1807:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB1_1043:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1044:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [r8 + 4*rax + 64], xmm0
	movups	xmmword ptr [r8 + 4*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movdqu	xmmword ptr [r8 + 4*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB1_1044
.LBB1_1045:
	test	rsi, rsi
	je	.LBB1_1048
# %bb.1046:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB1_1047:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_1047
.LBB1_1048:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1049
.LBB1_1053:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1054:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [r8 + 4*rax + 64], xmm0
	movups	xmmword ptr [r8 + 4*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movdqu	xmmword ptr [r8 + 4*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB1_1054
.LBB1_1055:
	test	rsi, rsi
	je	.LBB1_1058
# %bb.1056:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB1_1057:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_1057
.LBB1_1058:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1059
.LBB1_1063:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1064:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [r8 + 8*rax], xmm0
	movups	xmmword ptr [r8 + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [r8 + 8*rax + 32], xmm0
	movups	xmmword ptr [r8 + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [r8 + 8*rax + 64], xmm0
	movups	xmmword ptr [r8 + 8*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movdqu	xmmword ptr [r8 + 8*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB1_1064
.LBB1_1065:
	test	rsi, rsi
	je	.LBB1_1068
# %bb.1066:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB1_1067:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_1067
.LBB1_1068:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1069
.LBB1_1073:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1074:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [r8 + 8*rax], xmm0
	movups	xmmword ptr [r8 + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [r8 + 8*rax + 32], xmm0
	movups	xmmword ptr [r8 + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [r8 + 8*rax + 64], xmm0
	movups	xmmword ptr [r8 + 8*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movdqu	xmmword ptr [r8 + 8*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB1_1074
.LBB1_1075:
	test	rsi, rsi
	je	.LBB1_1078
# %bb.1076:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB1_1077:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_1077
.LBB1_1078:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1079
.LBB1_1083:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1084:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [r8 + 2*rax], xmm0
	movups	xmmword ptr [r8 + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [r8 + 2*rax + 32], xmm0
	movups	xmmword ptr [r8 + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [r8 + 2*rax + 64], xmm0
	movups	xmmword ptr [r8 + 2*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movdqu	xmmword ptr [r8 + 2*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB1_1084
.LBB1_1085:
	test	rsi, rsi
	je	.LBB1_1088
# %bb.1086:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB1_1087:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_1087
.LBB1_1088:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1089
.LBB1_1093:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1094:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [r8 + 2*rax], xmm0
	movups	xmmword ptr [r8 + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [r8 + 2*rax + 32], xmm0
	movups	xmmword ptr [r8 + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [r8 + 2*rax + 64], xmm0
	movups	xmmword ptr [r8 + 2*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movdqu	xmmword ptr [r8 + 2*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB1_1094
.LBB1_1095:
	test	rsi, rsi
	je	.LBB1_1098
# %bb.1096:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB1_1097:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_1097
.LBB1_1098:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1099
.LBB1_1103:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB1_1104:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rdi]
	movups	xmm1, xmmword ptr [rdx + rdi + 16]
	movups	xmmword ptr [r8 + rdi], xmm0
	movups	xmmword ptr [r8 + rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 32]
	movups	xmm1, xmmword ptr [rdx + rdi + 48]
	movups	xmmword ptr [r8 + rdi + 32], xmm0
	movups	xmmword ptr [r8 + rdi + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 64]
	movups	xmm1, xmmword ptr [rdx + rdi + 80]
	movups	xmmword ptr [r8 + rdi + 64], xmm0
	movups	xmmword ptr [r8 + rdi + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 96]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 112]
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm1
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB1_1104
.LBB1_1105:
	test	rax, rax
	je	.LBB1_1108
# %bb.1106:
	add	rdi, 16
	neg	rax
.LBB1_1107:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB1_1107
.LBB1_1108:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1109
.LBB1_1113:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB1_1114:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rdi]
	movups	xmm1, xmmword ptr [rdx + rdi + 16]
	movups	xmmword ptr [r8 + rdi], xmm0
	movups	xmmword ptr [r8 + rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 32]
	movups	xmm1, xmmword ptr [rdx + rdi + 48]
	movups	xmmword ptr [r8 + rdi + 32], xmm0
	movups	xmmword ptr [r8 + rdi + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 64]
	movups	xmm1, xmmword ptr [rdx + rdi + 80]
	movups	xmmword ptr [r8 + rdi + 64], xmm0
	movups	xmmword ptr [r8 + rdi + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 96]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 112]
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm1
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB1_1114
.LBB1_1115:
	test	rax, rax
	je	.LBB1_1118
# %bb.1116:
	add	rdi, 16
	neg	rax
.LBB1_1117:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB1_1117
.LBB1_1118:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1119
.LBB1_1123:
	xor	edi, edi
.LBB1_1124:
	test	r9b, 1
	je	.LBB1_1126
# %bb.1125:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1126:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1127
.LBB1_1131:
	xor	edi, edi
.LBB1_1132:
	test	r9b, 1
	je	.LBB1_1134
# %bb.1133:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1134:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1135
.LBB1_1139:
	xor	edi, edi
.LBB1_1140:
	test	r9b, 1
	je	.LBB1_1142
# %bb.1141:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1142:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1143
.LBB1_1147:
	xor	edi, edi
.LBB1_1148:
	test	r9b, 1
	je	.LBB1_1150
# %bb.1149:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1150:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1151
.LBB1_1155:
	xor	edi, edi
.LBB1_1156:
	test	r9b, 1
	je	.LBB1_1158
# %bb.1157:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1158:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1159
.LBB1_1163:
	xor	edi, edi
.LBB1_1164:
	test	r9b, 1
	je	.LBB1_1166
# %bb.1165:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1166:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1167
.LBB1_1171:
	xor	edi, edi
.LBB1_1172:
	test	r9b, 1
	je	.LBB1_1174
# %bb.1173:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1174:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1175
.LBB1_1179:
	xor	edi, edi
.LBB1_1180:
	test	r9b, 1
	je	.LBB1_1182
# %bb.1181:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_0] # xmm2 = [-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB1_1182:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1183
.LBB1_1189:
	xor	edi, edi
.LBB1_1190:
	test	r9b, 1
	je	.LBB1_1192
# %bb.1191:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_0] # xmm2 = [-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB1_1192:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1193
.LBB1_1199:
	xor	edi, edi
.LBB1_1200:
	test	r9b, 1
	je	.LBB1_1202
# %bb.1201:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1202:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1203
.LBB1_1207:
	xor	edi, edi
.LBB1_1208:
	test	r9b, 1
	je	.LBB1_1210
# %bb.1209:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1210:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1211
.LBB1_1215:
	xor	edi, edi
.LBB1_1216:
	test	r9b, 1
	je	.LBB1_1218
# %bb.1217:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1218:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1219
.LBB1_1223:
	xor	edi, edi
.LBB1_1224:
	test	r9b, 1
	je	.LBB1_1226
# %bb.1225:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_2] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB1_1226:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1227
.LBB1_1231:
	xor	edi, edi
.LBB1_1232:
	test	r9b, 1
	je	.LBB1_1234
# %bb.1233:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1234:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1235
.LBB1_1239:
	xor	edi, edi
.LBB1_1240:
	test	r9b, 1
	je	.LBB1_1242
# %bb.1241:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_2] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB1_1242:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1243
.LBB1_1247:
	xor	edi, edi
.LBB1_1248:
	test	r9b, 1
	je	.LBB1_1250
# %bb.1249:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1250:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1251
.LBB1_1255:
	xor	edi, edi
.LBB1_1256:
	test	r9b, 1
	je	.LBB1_1258
# %bb.1257:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1258:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1259
.LBB1_1263:
	xor	edi, edi
.LBB1_1264:
	test	r9b, 1
	je	.LBB1_1266
# %bb.1265:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1266:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1267
.LBB1_1271:
	xor	edi, edi
.LBB1_1272:
	test	r9b, 1
	je	.LBB1_1274
# %bb.1273:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1274:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1275
.LBB1_1279:
	xor	edi, edi
.LBB1_1280:
	test	r9b, 1
	je	.LBB1_1282
# %bb.1281:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1282:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1283
.LBB1_1287:
	xor	edi, edi
.LBB1_1288:
	test	r9b, 1
	je	.LBB1_1290
# %bb.1289:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1290:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1291
.LBB1_1295:
	xor	eax, eax
.LBB1_1296:
	test	r9b, 1
	je	.LBB1_1298
# %bb.1297:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB1_1298:
	cmp	rdi, r10
	je	.LBB1_1807
	jmp	.LBB1_1299
.LBB1_1303:
	xor	esi, esi
.LBB1_1304:
	test	r9b, 1
	je	.LBB1_1306
# %bb.1305:
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rsi]
	movdqa	xmm4, xmm1
	psrad	xmm4, 7
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm0
	psrad	xmm6, 7
	movdqa	xmm7, xmm3
	psrad	xmm7, 7
	paddd	xmm3, xmm7
	paddd	xmm0, xmm6
	paddd	xmm2, xmm5
	paddd	xmm1, xmm4
	pxor	xmm1, xmm4
	pxor	xmm2, xmm5
	pxor	xmm0, xmm6
	pxor	xmm3, xmm7
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_4] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB1_1306:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1307
.LBB1_1311:
	xor	eax, eax
.LBB1_1312:
	test	r9b, 1
	je	.LBB1_1314
# %bb.1313:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB1_1314:
	cmp	rdi, r10
	je	.LBB1_1807
	jmp	.LBB1_1315
.LBB1_1319:
	xor	esi, esi
.LBB1_1320:
	test	r9b, 1
	je	.LBB1_1322
# %bb.1321:
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rsi]
	movdqa	xmm4, xmm1
	psrad	xmm4, 7
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm0
	psrad	xmm6, 7
	movdqa	xmm7, xmm3
	psrad	xmm7, 7
	paddd	xmm3, xmm7
	paddd	xmm0, xmm6
	paddd	xmm2, xmm5
	paddd	xmm1, xmm4
	pxor	xmm1, xmm4
	pxor	xmm2, xmm5
	pxor	xmm0, xmm6
	pxor	xmm3, xmm7
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_4] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB1_1322:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1323
.LBB1_1327:
	xor	edi, edi
.LBB1_1328:
	test	r9b, 1
	je	.LBB1_1330
# %bb.1329:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1330:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1331
.LBB1_1335:
	xor	edi, edi
.LBB1_1336:
	test	r9b, 1
	je	.LBB1_1338
# %bb.1337:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1338:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1339
.LBB1_1343:
	xor	edi, edi
.LBB1_1344:
	test	r9b, 1
	je	.LBB1_1346
# %bb.1345:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1346:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1347
.LBB1_1351:
	xor	edi, edi
.LBB1_1352:
	test	r9b, 1
	je	.LBB1_1354
# %bb.1353:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1354:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1355
.LBB1_1359:
	xor	edi, edi
.LBB1_1360:
	test	r9b, 1
	je	.LBB1_1362
# %bb.1361:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1362:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1363
.LBB1_1367:
	xor	edi, edi
.LBB1_1368:
	test	r9b, 1
	je	.LBB1_1370
# %bb.1369:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1370:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1371
.LBB1_1375:
	xor	edi, edi
.LBB1_1376:
	test	r9b, 1
	je	.LBB1_1378
# %bb.1377:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1378:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1379
.LBB1_1383:
	xor	edi, edi
.LBB1_1384:
	test	r9b, 1
	je	.LBB1_1386
# %bb.1385:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1386:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1387
.LBB1_1391:
	xor	edi, edi
.LBB1_1392:
	test	r9b, 1
	je	.LBB1_1394
# %bb.1393:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1394:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1395
.LBB1_1399:
	xor	edi, edi
.LBB1_1400:
	test	r9b, 1
	je	.LBB1_1402
# %bb.1401:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1402:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1403
.LBB1_1407:
	xor	edi, edi
.LBB1_1408:
	test	r9b, 1
	je	.LBB1_1410
# %bb.1409:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1410:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1411
.LBB1_1415:
	xor	edi, edi
.LBB1_1416:
	test	r9b, 1
	je	.LBB1_1418
# %bb.1417:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1418:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1419
.LBB1_1423:
	xor	edi, edi
.LBB1_1424:
	test	r9b, 1
	je	.LBB1_1426
# %bb.1425:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1426:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1427
.LBB1_1431:
	xor	edi, edi
.LBB1_1432:
	test	r9b, 1
	je	.LBB1_1434
# %bb.1433:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1434:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1435
.LBB1_1439:
	xor	edi, edi
.LBB1_1440:
	test	r9b, 1
	je	.LBB1_1442
# %bb.1441:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1442:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1443
.LBB1_1447:
	xor	edi, edi
.LBB1_1448:
	test	r9b, 1
	je	.LBB1_1450
# %bb.1449:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1450:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1451
.LBB1_1455:
	xor	esi, esi
.LBB1_1456:
	test	r9b, 1
	je	.LBB1_1458
# %bb.1457:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi]
	movdqa	xmm2, xmm1
	psrad	xmm2, 15
	movdqa	xmm3, xmm0
	psrad	xmm3, 15
	paddd	xmm0, xmm3
	paddd	xmm1, xmm2
	pxor	xmm1, xmm2
	pxor	xmm0, xmm3
	pxor	xmm2, xmm2
	pblendw	xmm0, xmm2, 170                 # xmm0 = xmm0[0],xmm2[1],xmm0[2],xmm2[3],xmm0[4],xmm2[5],xmm0[6],xmm2[7]
	pblendw	xmm1, xmm2, 170                 # xmm1 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	packusdw	xmm1, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm1
.LBB1_1458:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1459
.LBB1_1463:
	xor	edi, edi
.LBB1_1464:
	test	r9b, 1
	je	.LBB1_1466
# %bb.1465:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1466:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1467
.LBB1_1471:
	xor	edi, edi
.LBB1_1472:
	test	r9b, 1
	je	.LBB1_1474
# %bb.1473:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1474:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1475
.LBB1_1479:
	xor	esi, esi
.LBB1_1480:
	test	r9b, 1
	je	.LBB1_1482
# %bb.1481:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi]
	movdqa	xmm2, xmm1
	psrad	xmm2, 15
	movdqa	xmm3, xmm0
	psrad	xmm3, 15
	paddd	xmm0, xmm3
	paddd	xmm1, xmm2
	pxor	xmm1, xmm2
	pxor	xmm0, xmm3
	pxor	xmm2, xmm2
	pblendw	xmm0, xmm2, 170                 # xmm0 = xmm0[0],xmm2[1],xmm0[2],xmm2[3],xmm0[4],xmm2[5],xmm0[6],xmm2[7]
	pblendw	xmm1, xmm2, 170                 # xmm1 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	packusdw	xmm1, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm1
.LBB1_1482:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1483
.LBB1_1487:
	xor	edi, edi
.LBB1_1488:
	test	r9b, 1
	je	.LBB1_1490
# %bb.1489:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1490:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1491
.LBB1_1495:
	xor	edi, edi
.LBB1_1496:
	test	r9b, 1
	je	.LBB1_1498
# %bb.1497:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1498:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1499
.LBB1_1503:
	xor	edi, edi
.LBB1_1504:
	test	r9b, 1
	je	.LBB1_1506
# %bb.1505:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1506:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1507
.LBB1_1511:
	xor	edi, edi
.LBB1_1512:
	test	r9b, 1
	je	.LBB1_1514
# %bb.1513:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1514:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1515
.LBB1_1519:
	xor	edi, edi
.LBB1_1520:
	test	r9b, 1
	je	.LBB1_1522
# %bb.1521:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1522:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1523
.LBB1_1527:
	xor	edi, edi
.LBB1_1528:
	test	r9b, 1
	je	.LBB1_1530
# %bb.1529:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_1] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1530:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1531
.LBB1_1537:
	xor	edi, edi
.LBB1_1538:
	test	r9b, 1
	je	.LBB1_1540
# %bb.1539:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1540:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1541
.LBB1_1545:
	xor	edi, edi
.LBB1_1546:
	test	r9b, 1
	je	.LBB1_1548
# %bb.1547:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_1] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1548:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1549
.LBB1_1555:
	xor	edi, edi
.LBB1_1556:
	test	r9b, 1
	je	.LBB1_1558
# %bb.1557:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1558:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1559
.LBB1_1563:
	xor	edi, edi
.LBB1_1564:
	test	r9b, 1
	je	.LBB1_1566
# %bb.1565:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1566:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1567
.LBB1_1571:
	xor	edi, edi
.LBB1_1572:
	test	r9b, 1
	je	.LBB1_1574
# %bb.1573:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1574:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1575
.LBB1_1579:
	xor	edi, edi
.LBB1_1580:
	test	r9b, 1
	je	.LBB1_1582
# %bb.1581:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1582:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1583
.LBB1_1587:
	xor	edi, edi
.LBB1_1588:
	test	r9b, 1
	je	.LBB1_1590
# %bb.1589:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1590:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1591
.LBB1_1595:
	xor	esi, esi
.LBB1_1596:
	test	r9b, 1
	je	.LBB1_1598
# %bb.1597:
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
.LBB1_1598:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1599
.LBB1_1603:
	xor	edi, edi
.LBB1_1604:
	test	r9b, 1
	je	.LBB1_1606
# %bb.1605:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_3] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1606:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1607
.LBB1_1613:
	xor	edi, edi
.LBB1_1614:
	test	r9b, 1
	je	.LBB1_1616
# %bb.1615:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1616:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1617
.LBB1_1621:
	xor	esi, esi
.LBB1_1622:
	test	r9b, 1
	je	.LBB1_1624
# %bb.1623:
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
.LBB1_1624:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1625
.LBB1_1629:
	xor	edi, edi
.LBB1_1630:
	test	r9b, 1
	je	.LBB1_1632
# %bb.1631:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_3] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1632:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1633
.LBB1_1639:
	xor	edi, edi
.LBB1_1640:
	test	r9b, 1
	je	.LBB1_1642
# %bb.1641:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1642:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1643
.LBB1_1647:
	xor	edi, edi
.LBB1_1648:
	test	r9b, 1
	je	.LBB1_1650
# %bb.1649:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1650:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1651
.LBB1_1655:
	xor	edi, edi
.LBB1_1656:
	test	r9b, 1
	je	.LBB1_1658
# %bb.1657:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1658:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1659
.LBB1_1663:
	xor	edi, edi
.LBB1_1664:
	test	r9b, 1
	je	.LBB1_1666
# %bb.1665:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1666:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1667
.LBB1_1671:
	xor	edi, edi
.LBB1_1672:
	test	r9b, 1
	je	.LBB1_1674
# %bb.1673:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1674:
	cmp	rcx, r10
	je	.LBB1_1807
	jmp	.LBB1_1675
.LBB1_1679:
	xor	edi, edi
.LBB1_1680:
	test	r9b, 1
	je	.LBB1_1682
# %bb.1681:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1682:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1683
.LBB1_1687:
	xor	edi, edi
.LBB1_1688:
	test	r9b, 1
	je	.LBB1_1690
# %bb.1689:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1690:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1691
.LBB1_1695:
	xor	eax, eax
.LBB1_1696:
	test	r9b, 1
	je	.LBB1_1698
# %bb.1697:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB1_1698:
	cmp	rdi, r10
	je	.LBB1_1807
	jmp	.LBB1_1699
.LBB1_1703:
	xor	eax, eax
.LBB1_1704:
	test	r9b, 1
	je	.LBB1_1706
# %bb.1705:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB1_1706:
	cmp	rdi, r10
	je	.LBB1_1807
	jmp	.LBB1_1707
.LBB1_1711:
	xor	edi, edi
.LBB1_1712:
	test	r9b, 1
	je	.LBB1_1714
# %bb.1713:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1714:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1715
.LBB1_1719:
	xor	edi, edi
.LBB1_1720:
	test	r9b, 1
	je	.LBB1_1722
# %bb.1721:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1722:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1723
.LBB1_1727:
	xor	edi, edi
.LBB1_1728:
	test	r9b, 1
	je	.LBB1_1730
# %bb.1729:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1730:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1731
.LBB1_1735:
	xor	edi, edi
.LBB1_1736:
	test	r9b, 1
	je	.LBB1_1738
# %bb.1737:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1738:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1739
.LBB1_1743:
	xor	edi, edi
.LBB1_1744:
	test	r9b, 1
	je	.LBB1_1746
# %bb.1745:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1746:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1747
.LBB1_1751:
	xor	edi, edi
.LBB1_1752:
	test	r9b, 1
	je	.LBB1_1754
# %bb.1753:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1754:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1755
.LBB1_1759:
	xor	edi, edi
.LBB1_1760:
	test	r9b, 1
	je	.LBB1_1762
# %bb.1761:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1762:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1763
.LBB1_1767:
	xor	edi, edi
.LBB1_1768:
	test	r9b, 1
	je	.LBB1_1770
# %bb.1769:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1770:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1771
.LBB1_1775:
	xor	edi, edi
.LBB1_1776:
	test	r9b, 1
	je	.LBB1_1778
# %bb.1777:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1778:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1779
.LBB1_1783:
	xor	edi, edi
.LBB1_1784:
	test	r9b, 1
	je	.LBB1_1786
# %bb.1785:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1786:
	cmp	rcx, rax
	je	.LBB1_1807
	jmp	.LBB1_1787
.LBB1_1791:
	xor	edi, edi
.LBB1_1792:
	test	r9b, 1
	je	.LBB1_1794
# %bb.1793:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1794:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1795
.LBB1_1799:
	xor	edi, edi
.LBB1_1800:
	test	r9b, 1
	je	.LBB1_1802
# %bb.1801:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1802:
	cmp	rsi, r10
	je	.LBB1_1807
	jmp	.LBB1_1803
.Lfunc_end1:
	.size	arithmetic_arr_scalar_sse4, .Lfunc_end1-arithmetic_arr_scalar_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_scalar_arr_sse4
.LCPI2_0:
	.long	0x00000000
	.long	0x80000000              # double -0
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI2_1:
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
.LCPI2_2:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI2_3:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI2_4:
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	255                             # 0xff
	.byte	0                               # 0x0
	.byte	0                               # 0x0
	.byte	0                               # 0x0
.LCPI2_5:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	arithmetic_scalar_arr_sse4
	.p2align	4, 0x90
	.type	arithmetic_scalar_arr_sse4,@function
arithmetic_scalar_arr_sse4:             # @arithmetic_scalar_arr_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 6
	jg	.LBB2_13
# %bb.1:
	cmp	sil, 1
	jle	.LBB2_25
# %bb.2:
	cmp	sil, 2
	je	.LBB2_45
# %bb.3:
	cmp	sil, 4
	je	.LBB2_53
# %bb.4:
	cmp	sil, 5
	jne	.LBB2_1807
# %bb.5:
	cmp	edi, 6
	jg	.LBB2_93
# %bb.6:
	cmp	edi, 3
	jle	.LBB2_163
# %bb.7:
	cmp	edi, 4
	je	.LBB2_263
# %bb.8:
	cmp	edi, 5
	je	.LBB2_266
# %bb.9:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.10:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.11:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_12
# %bb.443:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_747
# %bb.444:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_747
.LBB2_12:
	xor	ecx, ecx
.LBB2_1127:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1129
.LBB2_1128:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1128
.LBB2_1129:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1130:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1130
	jmp	.LBB2_1807
.LBB2_13:
	cmp	sil, 8
	jle	.LBB2_35
# %bb.14:
	cmp	sil, 9
	je	.LBB2_61
# %bb.15:
	cmp	sil, 11
	je	.LBB2_69
# %bb.16:
	cmp	sil, 12
	jne	.LBB2_1807
# %bb.17:
	cmp	edi, 6
	jg	.LBB2_100
# %bb.18:
	cmp	edi, 3
	jle	.LBB2_168
# %bb.19:
	cmp	edi, 4
	je	.LBB2_269
# %bb.20:
	cmp	edi, 5
	je	.LBB2_272
# %bb.21:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.22:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.23:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB2_446
# %bb.24:
	xor	ecx, ecx
	jmp	.LBB2_756
.LBB2_25:
	test	sil, sil
	je	.LBB2_77
# %bb.26:
	cmp	sil, 1
	jne	.LBB2_1807
# %bb.27:
	cmp	edi, 6
	jg	.LBB2_107
# %bb.28:
	cmp	edi, 3
	jle	.LBB2_173
# %bb.29:
	cmp	edi, 4
	je	.LBB2_275
# %bb.30:
	cmp	edi, 5
	je	.LBB2_278
# %bb.31:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.32:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.33:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_34
# %bb.448:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_757
# %bb.449:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_757
.LBB2_34:
	xor	esi, esi
.LBB2_1135:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1137
.LBB2_1136:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1136
.LBB2_1137:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1138:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1138
	jmp	.LBB2_1807
.LBB2_35:
	cmp	sil, 7
	je	.LBB2_85
# %bb.36:
	cmp	sil, 8
	jne	.LBB2_1807
# %bb.37:
	cmp	edi, 6
	jg	.LBB2_114
# %bb.38:
	cmp	edi, 3
	jle	.LBB2_178
# %bb.39:
	cmp	edi, 4
	je	.LBB2_281
# %bb.40:
	cmp	edi, 5
	je	.LBB2_284
# %bb.41:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.42:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.43:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_44
# %bb.451:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_760
# %bb.452:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_760
.LBB2_44:
	xor	esi, esi
.LBB2_1143:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1145
.LBB2_1144:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1144
.LBB2_1145:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1146:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1146
	jmp	.LBB2_1807
.LBB2_45:
	cmp	edi, 6
	jg	.LBB2_121
# %bb.46:
	cmp	edi, 3
	jle	.LBB2_183
# %bb.47:
	cmp	edi, 4
	je	.LBB2_287
# %bb.48:
	cmp	edi, 5
	je	.LBB2_290
# %bb.49:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.50:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.51:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_52
# %bb.454:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_763
# %bb.455:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_763
.LBB2_52:
	xor	esi, esi
.LBB2_1151:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1153
.LBB2_1152:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1152
.LBB2_1153:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1154:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1154
	jmp	.LBB2_1807
.LBB2_53:
	cmp	edi, 6
	jg	.LBB2_128
# %bb.54:
	cmp	edi, 3
	jle	.LBB2_188
# %bb.55:
	cmp	edi, 4
	je	.LBB2_293
# %bb.56:
	cmp	edi, 5
	je	.LBB2_296
# %bb.57:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.58:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.59:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_60
# %bb.457:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_766
# %bb.458:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB2_766
.LBB2_60:
	xor	ecx, ecx
.LBB2_1049:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1051
.LBB2_1050:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1050
.LBB2_1051:
	cmp	rax, 3
	jb	.LBB2_1807
.LBB2_1052:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1052
	jmp	.LBB2_1807
.LBB2_61:
	cmp	edi, 6
	jg	.LBB2_135
# %bb.62:
	cmp	edi, 3
	jle	.LBB2_193
# %bb.63:
	cmp	edi, 4
	je	.LBB2_299
# %bb.64:
	cmp	edi, 5
	je	.LBB2_302
# %bb.65:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.66:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.67:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_68
# %bb.460:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_768
# %bb.461:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_768
.LBB2_68:
	xor	esi, esi
.LBB2_1159:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1161
.LBB2_1160:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1160
.LBB2_1161:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1162:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1162
	jmp	.LBB2_1807
.LBB2_69:
	cmp	edi, 6
	jg	.LBB2_142
# %bb.70:
	cmp	edi, 3
	jle	.LBB2_198
# %bb.71:
	cmp	edi, 4
	je	.LBB2_305
# %bb.72:
	cmp	edi, 5
	je	.LBB2_308
# %bb.73:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.74:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.75:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_76
# %bb.463:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_771
# %bb.464:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB2_771
.LBB2_76:
	xor	ecx, ecx
.LBB2_1059:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1061
.LBB2_1060:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1060
.LBB2_1061:
	cmp	rax, 3
	jb	.LBB2_1807
.LBB2_1062:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], eax
	mov	eax, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], eax
	mov	eax, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], eax
	mov	eax, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], eax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1062
	jmp	.LBB2_1807
.LBB2_77:
	cmp	edi, 6
	jg	.LBB2_149
# %bb.78:
	cmp	edi, 3
	jle	.LBB2_203
# %bb.79:
	cmp	edi, 4
	je	.LBB2_311
# %bb.80:
	cmp	edi, 5
	je	.LBB2_314
# %bb.81:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.82:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.83:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_84
# %bb.466:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_773
# %bb.467:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_773
.LBB2_84:
	xor	esi, esi
.LBB2_1167:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1169
.LBB2_1168:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1168
.LBB2_1169:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1170:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1170
	jmp	.LBB2_1807
.LBB2_85:
	cmp	edi, 6
	jg	.LBB2_156
# %bb.86:
	cmp	edi, 3
	jle	.LBB2_208
# %bb.87:
	cmp	edi, 4
	je	.LBB2_317
# %bb.88:
	cmp	edi, 5
	je	.LBB2_320
# %bb.89:
	cmp	edi, 6
	jne	.LBB2_1807
# %bb.90:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.91:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_92
# %bb.469:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_776
# %bb.470:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_776
.LBB2_92:
	xor	esi, esi
.LBB2_1175:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1177
.LBB2_1176:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1176
.LBB2_1177:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1178:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1178
	jmp	.LBB2_1807
.LBB2_93:
	cmp	edi, 8
	jle	.LBB2_213
# %bb.94:
	cmp	edi, 9
	je	.LBB2_323
# %bb.95:
	cmp	edi, 11
	je	.LBB2_326
# %bb.96:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.97:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.98:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_99
# %bb.472:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_779
# %bb.473:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_779
.LBB2_99:
	xor	ecx, ecx
.LBB2_1183:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1186
# %bb.1184:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1185:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1185
.LBB2_1186:
	cmp	rsi, 3
	jb	.LBB2_1807
# %bb.1187:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1188:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 8], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 16], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1188
	jmp	.LBB2_1807
.LBB2_100:
	cmp	edi, 8
	jle	.LBB2_218
# %bb.101:
	cmp	edi, 9
	je	.LBB2_329
# %bb.102:
	cmp	edi, 11
	je	.LBB2_332
# %bb.103:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.104:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.105:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_106
# %bb.475:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_782
# %bb.476:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_782
.LBB2_106:
	xor	ecx, ecx
.LBB2_1193:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1196
# %bb.1194:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1195:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1195
.LBB2_1196:
	cmp	rsi, 3
	jb	.LBB2_1807
# %bb.1197:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1198:                             # =>This Inner Loop Header: Depth=1
	movq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 8], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 16], xmm1
	movq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	pxor	xmm1, xmm0
	movq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1198
	jmp	.LBB2_1807
.LBB2_107:
	cmp	edi, 8
	jle	.LBB2_223
# %bb.108:
	cmp	edi, 9
	je	.LBB2_335
# %bb.109:
	cmp	edi, 11
	je	.LBB2_338
# %bb.110:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.111:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.112:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_113
# %bb.478:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_785
# %bb.479:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_785
.LBB2_113:
	xor	edx, edx
.LBB2_1203:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1205
.LBB2_1204:                             # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1204
.LBB2_1205:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1206:                             # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx + 8]
	movsd	qword ptr [r8 + 8*rdx + 8], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx + 16]
	movsd	qword ptr [r8 + 8*rdx + 16], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx + 24]
	movsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1206
	jmp	.LBB2_1807
.LBB2_114:
	cmp	edi, 8
	jle	.LBB2_228
# %bb.115:
	cmp	edi, 9
	je	.LBB2_341
# %bb.116:
	cmp	edi, 11
	je	.LBB2_344
# %bb.117:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.118:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.119:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_120
# %bb.481:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_788
# %bb.482:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_788
.LBB2_120:
	xor	edx, edx
.LBB2_1211:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1213
.LBB2_1212:                             # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1212
.LBB2_1213:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1214:                             # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx + 8]
	movsd	qword ptr [r8 + 8*rdx + 8], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx + 16]
	movsd	qword ptr [r8 + 8*rdx + 16], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx + 24]
	movsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1214
	jmp	.LBB2_1807
.LBB2_121:
	cmp	edi, 8
	jle	.LBB2_233
# %bb.122:
	cmp	edi, 9
	je	.LBB2_347
# %bb.123:
	cmp	edi, 11
	je	.LBB2_350
# %bb.124:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.125:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.126:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_127
# %bb.484:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_791
# %bb.485:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_791
.LBB2_127:
	xor	edx, edx
.LBB2_1219:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1221
.LBB2_1220:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1220
.LBB2_1221:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1222:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 8] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 8], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 16] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 16], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 24] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1222
	jmp	.LBB2_1807
.LBB2_128:
	cmp	edi, 8
	jle	.LBB2_238
# %bb.129:
	cmp	edi, 9
	je	.LBB2_353
# %bb.130:
	cmp	edi, 11
	je	.LBB2_356
# %bb.131:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.132:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.133:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_134
# %bb.487:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_794
# %bb.488:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_794
.LBB2_134:
	xor	ecx, ecx
.LBB2_1227:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_1229
.LBB2_1228:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_1228
.LBB2_1229:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1230:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1230
	jmp	.LBB2_1807
.LBB2_135:
	cmp	edi, 8
	jle	.LBB2_243
# %bb.136:
	cmp	edi, 9
	je	.LBB2_359
# %bb.137:
	cmp	edi, 11
	je	.LBB2_362
# %bb.138:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.139:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.140:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_141
# %bb.490:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_797
# %bb.491:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_797
.LBB2_141:
	xor	edx, edx
.LBB2_1235:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1237
.LBB2_1236:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1236
.LBB2_1237:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1238:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 8] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 8], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 16] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 16], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 24] # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1238
	jmp	.LBB2_1807
.LBB2_142:
	cmp	edi, 8
	jle	.LBB2_248
# %bb.143:
	cmp	edi, 9
	je	.LBB2_365
# %bb.144:
	cmp	edi, 11
	je	.LBB2_368
# %bb.145:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.146:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.147:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_148
# %bb.493:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_800
# %bb.494:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_800
.LBB2_148:
	xor	ecx, ecx
.LBB2_1243:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_1245
.LBB2_1244:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_1244
.LBB2_1245:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1246:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1246
	jmp	.LBB2_1807
.LBB2_149:
	cmp	edi, 8
	jle	.LBB2_253
# %bb.150:
	cmp	edi, 9
	je	.LBB2_371
# %bb.151:
	cmp	edi, 11
	je	.LBB2_374
# %bb.152:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.153:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.154:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_155
# %bb.496:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_803
# %bb.497:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_803
.LBB2_155:
	xor	edx, edx
.LBB2_1251:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1253
.LBB2_1252:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1252
.LBB2_1253:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1254:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 8], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 16], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1254
	jmp	.LBB2_1807
.LBB2_156:
	cmp	edi, 8
	jle	.LBB2_258
# %bb.157:
	cmp	edi, 9
	je	.LBB2_377
# %bb.158:
	cmp	edi, 11
	je	.LBB2_380
# %bb.159:
	cmp	edi, 12
	jne	.LBB2_1807
# %bb.160:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.161:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_162
# %bb.499:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_806
# %bb.500:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_806
.LBB2_162:
	xor	edx, edx
.LBB2_1259:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1261
.LBB2_1260:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1260
.LBB2_1261:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1262:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 8], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 16], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rdx + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1262
	jmp	.LBB2_1807
.LBB2_163:
	cmp	edi, 2
	je	.LBB2_383
# %bb.164:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.165:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.166:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_167
# %bb.502:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_809
# %bb.503:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_809
.LBB2_167:
	xor	ecx, ecx
.LBB2_1267:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1269
.LBB2_1268:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1268
.LBB2_1269:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1270:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1270
	jmp	.LBB2_1807
.LBB2_168:
	cmp	edi, 2
	je	.LBB2_386
# %bb.169:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.170:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.171:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_172
# %bb.505:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_812
# %bb.506:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_812
.LBB2_172:
	xor	ecx, ecx
.LBB2_1275:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1277
.LBB2_1276:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1276
.LBB2_1277:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1278:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1278
	jmp	.LBB2_1807
.LBB2_173:
	cmp	edi, 2
	je	.LBB2_389
# %bb.174:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.175:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.176:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_177
# %bb.508:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_815
# %bb.509:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_815
.LBB2_177:
	xor	esi, esi
.LBB2_1283:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1285
.LBB2_1284:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1284
.LBB2_1285:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1286:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1286
	jmp	.LBB2_1807
.LBB2_178:
	cmp	edi, 2
	je	.LBB2_392
# %bb.179:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.180:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.181:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_182
# %bb.511:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_818
# %bb.512:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_818
.LBB2_182:
	xor	esi, esi
.LBB2_1291:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1293
.LBB2_1292:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1292
.LBB2_1293:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1294:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1294
	jmp	.LBB2_1807
.LBB2_183:
	cmp	edi, 2
	je	.LBB2_395
# %bb.184:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.185:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.186:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_187
# %bb.514:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_821
# %bb.515:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_821
.LBB2_187:
	xor	edi, edi
.LBB2_1299:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1301
.LBB2_1300:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1300
.LBB2_1301:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1302:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1302
	jmp	.LBB2_1807
.LBB2_188:
	cmp	edi, 2
	je	.LBB2_398
# %bb.189:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.190:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.191:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_192
# %bb.517:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_824
# %bb.518:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_824
.LBB2_192:
	xor	ecx, ecx
.LBB2_1307:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB2_1309
# %bb.1308:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_1309:
	add	rsi, r10
	je	.LBB2_1807
.LBB2_1310:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rcx]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx], al
	movsx	eax, byte ptr [rdx + rcx + 1]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx + 1], al
	add	rcx, 2
	cmp	r10, rcx
	jne	.LBB2_1310
	jmp	.LBB2_1807
.LBB2_193:
	cmp	edi, 2
	je	.LBB2_401
# %bb.194:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.195:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.196:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_197
# %bb.520:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_827
# %bb.521:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_827
.LBB2_197:
	xor	edi, edi
.LBB2_1315:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1317
.LBB2_1316:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1316
.LBB2_1317:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1318:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1318
	jmp	.LBB2_1807
.LBB2_198:
	cmp	edi, 2
	je	.LBB2_404
# %bb.199:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.200:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.201:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_202
# %bb.523:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_830
# %bb.524:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_830
.LBB2_202:
	xor	ecx, ecx
.LBB2_1323:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB2_1325
# %bb.1324:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_1325:
	add	rsi, r10
	je	.LBB2_1807
.LBB2_1326:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rcx]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx], al
	movsx	eax, byte ptr [rdx + rcx + 1]
	mov	esi, eax
	sar	esi, 7
	add	eax, esi
	xor	eax, esi
	mov	byte ptr [r8 + rcx + 1], al
	add	rcx, 2
	cmp	r10, rcx
	jne	.LBB2_1326
	jmp	.LBB2_1807
.LBB2_203:
	cmp	edi, 2
	je	.LBB2_407
# %bb.204:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.205:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.206:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_207
# %bb.526:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_833
# %bb.527:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_833
.LBB2_207:
	xor	esi, esi
.LBB2_1331:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1333
.LBB2_1332:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1332
.LBB2_1333:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1334:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1334
	jmp	.LBB2_1807
.LBB2_208:
	cmp	edi, 2
	je	.LBB2_410
# %bb.209:
	cmp	edi, 3
	jne	.LBB2_1807
# %bb.210:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.211:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_212
# %bb.529:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_836
# %bb.530:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_836
.LBB2_212:
	xor	esi, esi
.LBB2_1339:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1341
.LBB2_1340:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1340
.LBB2_1341:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1342:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1342
	jmp	.LBB2_1807
.LBB2_213:
	cmp	edi, 7
	je	.LBB2_413
# %bb.214:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.215:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.216:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_217
# %bb.532:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_839
# %bb.533:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_839
.LBB2_217:
	xor	ecx, ecx
.LBB2_1347:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1349
.LBB2_1348:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1348
.LBB2_1349:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1350:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1350
	jmp	.LBB2_1807
.LBB2_218:
	cmp	edi, 7
	je	.LBB2_416
# %bb.219:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.220:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.221:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB2_535
# %bb.222:
	xor	ecx, ecx
	jmp	.LBB2_848
.LBB2_223:
	cmp	edi, 7
	je	.LBB2_419
# %bb.224:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.225:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.226:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_227
# %bb.537:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_849
# %bb.538:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_849
.LBB2_227:
	xor	esi, esi
.LBB2_1355:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1357
.LBB2_1356:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1356
.LBB2_1357:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1358:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1358
	jmp	.LBB2_1807
.LBB2_228:
	cmp	edi, 7
	je	.LBB2_422
# %bb.229:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.230:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.231:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_232
# %bb.540:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_852
# %bb.541:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_852
.LBB2_232:
	xor	esi, esi
.LBB2_1363:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1365
.LBB2_1364:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1364
.LBB2_1365:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1366:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1366
	jmp	.LBB2_1807
.LBB2_233:
	cmp	edi, 7
	je	.LBB2_425
# %bb.234:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.235:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.236:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_543
# %bb.237:
	xor	edi, edi
	jmp	.LBB2_545
.LBB2_238:
	cmp	edi, 7
	je	.LBB2_428
# %bb.239:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.240:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.241:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_242
# %bb.548:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_855
# %bb.549:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_855
.LBB2_242:
	xor	ecx, ecx
.LBB2_1069:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1071
.LBB2_1070:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1070
.LBB2_1071:
	cmp	rax, 3
	jb	.LBB2_1807
.LBB2_1072:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1072
	jmp	.LBB2_1807
.LBB2_243:
	cmp	edi, 7
	je	.LBB2_431
# %bb.244:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.245:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.246:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_551
# %bb.247:
	xor	edi, edi
	jmp	.LBB2_553
.LBB2_248:
	cmp	edi, 7
	je	.LBB2_434
# %bb.249:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.250:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.251:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_252
# %bb.556:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_857
# %bb.557:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_857
.LBB2_252:
	xor	ecx, ecx
.LBB2_1079:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1081
.LBB2_1080:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1080
.LBB2_1081:
	cmp	rax, 3
	jb	.LBB2_1807
.LBB2_1082:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rax
	mov	rax, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rax
	mov	rax, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rax
	mov	rax, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rax
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1082
	jmp	.LBB2_1807
.LBB2_253:
	cmp	edi, 7
	je	.LBB2_437
# %bb.254:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.255:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.256:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_257
# %bb.559:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_859
# %bb.560:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_859
.LBB2_257:
	xor	esi, esi
.LBB2_1371:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1373
.LBB2_1372:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1372
.LBB2_1373:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1374:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1374
	jmp	.LBB2_1807
.LBB2_258:
	cmp	edi, 7
	je	.LBB2_440
# %bb.259:
	cmp	edi, 8
	jne	.LBB2_1807
# %bb.260:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.261:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_262
# %bb.562:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_862
# %bb.563:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_862
.LBB2_262:
	xor	esi, esi
.LBB2_1379:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1381
.LBB2_1380:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1380
.LBB2_1381:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1382:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1382
	jmp	.LBB2_1807
.LBB2_263:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.264:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_265
# %bb.565:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_865
# %bb.566:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_865
.LBB2_265:
	xor	ecx, ecx
.LBB2_1387:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1389
.LBB2_1388:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1388
.LBB2_1389:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1390:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1390
	jmp	.LBB2_1807
.LBB2_266:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.267:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_268
# %bb.568:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_868
# %bb.569:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_868
.LBB2_268:
	xor	ecx, ecx
.LBB2_1395:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1397
.LBB2_1396:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1396
.LBB2_1397:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1398:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1398
	jmp	.LBB2_1807
.LBB2_269:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.270:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB2_571
# %bb.271:
	xor	ecx, ecx
	jmp	.LBB2_877
.LBB2_272:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.273:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_274
# %bb.573:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_878
# %bb.574:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_878
.LBB2_274:
	xor	ecx, ecx
.LBB2_1403:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1405
.LBB2_1404:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1404
.LBB2_1405:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1406:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 2]
	mov	word ptr [r8 + 2*rcx + 2], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 4]
	mov	word ptr [r8 + 2*rcx + 4], si
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx + 6]
	mov	word ptr [r8 + 2*rcx + 6], si
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1406
	jmp	.LBB2_1807
.LBB2_275:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.276:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_277
# %bb.576:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_881
# %bb.577:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_881
.LBB2_277:
	xor	esi, esi
.LBB2_1411:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1413
.LBB2_1412:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1412
.LBB2_1413:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1414:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1414
	jmp	.LBB2_1807
.LBB2_278:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.279:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_280
# %bb.579:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_884
# %bb.580:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_884
.LBB2_280:
	xor	esi, esi
.LBB2_1419:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1421
.LBB2_1420:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1420
.LBB2_1421:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1422:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1422
	jmp	.LBB2_1807
.LBB2_281:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.282:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_283
# %bb.582:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_887
# %bb.583:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_887
.LBB2_283:
	xor	esi, esi
.LBB2_1427:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1429
.LBB2_1428:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1428
.LBB2_1429:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1430:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1430
	jmp	.LBB2_1807
.LBB2_284:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.285:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_286
# %bb.585:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_890
# %bb.586:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_890
.LBB2_286:
	xor	esi, esi
.LBB2_1435:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1437
.LBB2_1436:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1436
.LBB2_1437:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1438:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1438
	jmp	.LBB2_1807
.LBB2_287:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.288:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_289
# %bb.588:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_893
# %bb.589:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_893
.LBB2_289:
	xor	esi, esi
.LBB2_1443:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1445
.LBB2_1444:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1444
.LBB2_1445:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1446:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1446
	jmp	.LBB2_1807
.LBB2_290:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.291:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_292
# %bb.591:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_896
# %bb.592:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_896
.LBB2_292:
	xor	esi, esi
.LBB2_1451:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1453
.LBB2_1452:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1452
.LBB2_1453:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1454:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1454
	jmp	.LBB2_1807
.LBB2_293:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.294:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_295
# %bb.594:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_899
# %bb.595:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_899
.LBB2_295:
	xor	ecx, ecx
.LBB2_1089:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1091
.LBB2_1090:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1090
.LBB2_1091:
	cmp	rax, 3
	jb	.LBB2_1807
.LBB2_1092:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1092
	jmp	.LBB2_1807
.LBB2_296:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.297:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_298
# %bb.597:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_901
# %bb.598:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_901
.LBB2_298:
	xor	ecx, ecx
.LBB2_1459:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1461
# %bb.1460:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1461:
	add	rsi, rax
	je	.LBB2_1807
.LBB2_1462:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1462
	jmp	.LBB2_1807
.LBB2_299:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.300:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_301
# %bb.600:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_904
# %bb.601:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_904
.LBB2_301:
	xor	esi, esi
.LBB2_1467:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1469
.LBB2_1468:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1468
.LBB2_1469:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1470:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1470
	jmp	.LBB2_1807
.LBB2_302:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.303:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_304
# %bb.603:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_907
# %bb.604:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_907
.LBB2_304:
	xor	esi, esi
.LBB2_1475:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1477
.LBB2_1476:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1476
.LBB2_1477:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1478:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1478
	jmp	.LBB2_1807
.LBB2_305:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.306:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_307
# %bb.606:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_910
# %bb.607:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_910
.LBB2_307:
	xor	ecx, ecx
.LBB2_1099:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1101
.LBB2_1100:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1100
.LBB2_1101:
	cmp	rax, 3
	jb	.LBB2_1807
.LBB2_1102:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1102
	jmp	.LBB2_1807
.LBB2_308:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.309:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_310
# %bb.609:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_912
# %bb.610:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_912
.LBB2_310:
	xor	ecx, ecx
.LBB2_1483:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1485
# %bb.1484:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1485:
	add	rsi, rax
	je	.LBB2_1807
.LBB2_1486:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1486
	jmp	.LBB2_1807
.LBB2_311:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.312:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_313
# %bb.612:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_915
# %bb.613:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_915
.LBB2_313:
	xor	esi, esi
.LBB2_1491:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1493
.LBB2_1492:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1492
.LBB2_1493:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1494:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1494
	jmp	.LBB2_1807
.LBB2_314:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.315:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_316
# %bb.615:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_918
# %bb.616:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_918
.LBB2_316:
	xor	esi, esi
.LBB2_1499:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1501
.LBB2_1500:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1500
.LBB2_1501:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1502:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1502
	jmp	.LBB2_1807
.LBB2_317:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.318:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_319
# %bb.618:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_921
# %bb.619:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_921
.LBB2_319:
	xor	esi, esi
.LBB2_1507:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1509
.LBB2_1508:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1508
.LBB2_1509:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1510:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1510
	jmp	.LBB2_1807
.LBB2_320:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.321:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_322
# %bb.621:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_924
# %bb.622:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_924
.LBB2_322:
	xor	esi, esi
.LBB2_1515:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1517
.LBB2_1516:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1516
.LBB2_1517:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1518:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1518
	jmp	.LBB2_1807
.LBB2_323:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.324:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_325
# %bb.624:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_927
# %bb.625:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_927
.LBB2_325:
	xor	ecx, ecx
.LBB2_1523:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1525
.LBB2_1524:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1524
.LBB2_1525:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1526:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1526
	jmp	.LBB2_1807
.LBB2_326:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.327:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_328
# %bb.627:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_930
# %bb.628:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_930
.LBB2_328:
	xor	ecx, ecx
.LBB2_1531:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1534
# %bb.1532:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1533:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1533
.LBB2_1534:
	cmp	rsi, 3
	jb	.LBB2_1807
# %bb.1535:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1536:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 4], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 8], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1536
	jmp	.LBB2_1807
.LBB2_329:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.330:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_331
# %bb.630:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_933
# %bb.631:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_933
.LBB2_331:
	xor	ecx, ecx
.LBB2_1541:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1543
.LBB2_1542:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1542
.LBB2_1543:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1544:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 8]
	mov	qword ptr [r8 + 8*rcx + 8], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 16]
	mov	qword ptr [r8 + 8*rcx + 16], rsi
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx + 24]
	mov	qword ptr [r8 + 8*rcx + 24], rsi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1544
	jmp	.LBB2_1807
.LBB2_332:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.333:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_334
# %bb.633:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_936
# %bb.634:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_936
.LBB2_334:
	xor	ecx, ecx
.LBB2_1549:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1552
# %bb.1550:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1551:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1551
.LBB2_1552:
	cmp	rsi, 3
	jb	.LBB2_1807
# %bb.1553:
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1554:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 4], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 8], xmm1
	movd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm1, xmm0
	movd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1554
	jmp	.LBB2_1807
.LBB2_335:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.336:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_337
# %bb.636:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_939
# %bb.637:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_939
.LBB2_337:
	xor	esi, esi
.LBB2_1559:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1561
.LBB2_1560:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1560
.LBB2_1561:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1562:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1562
	jmp	.LBB2_1807
.LBB2_338:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.339:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_340
# %bb.639:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_942
# %bb.640:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_942
.LBB2_340:
	xor	edx, edx
.LBB2_1567:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1569
.LBB2_1568:                             # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1568
.LBB2_1569:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1570:                             # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx + 4]
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx + 8]
	movss	dword ptr [r8 + 4*rdx + 8], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx + 12]
	movss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1570
	jmp	.LBB2_1807
.LBB2_341:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.342:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_343
# %bb.642:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_945
# %bb.643:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_945
.LBB2_343:
	xor	esi, esi
.LBB2_1575:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1577
.LBB2_1576:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1576
.LBB2_1577:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1578:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1578
	jmp	.LBB2_1807
.LBB2_344:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.345:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_346
# %bb.645:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_948
# %bb.646:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_948
.LBB2_346:
	xor	edx, edx
.LBB2_1583:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1585
.LBB2_1584:                             # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1584
.LBB2_1585:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1586:                             # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx + 4]
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx + 8]
	movss	dword ptr [r8 + 4*rdx + 8], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx + 12]
	movss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1586
	jmp	.LBB2_1807
.LBB2_347:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.348:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_648
# %bb.349:
	xor	edi, edi
	jmp	.LBB2_650
.LBB2_350:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.351:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_352
# %bb.653:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_951
# %bb.654:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_951
.LBB2_352:
	xor	edx, edx
.LBB2_1591:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1593
.LBB2_1592:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1592
.LBB2_1593:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1594:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 8] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 8], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 12] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1594
	jmp	.LBB2_1807
.LBB2_353:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.354:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_355
# %bb.656:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_954
# %bb.657:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_954
.LBB2_355:
	xor	ecx, ecx
.LBB2_1599:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1601
# %bb.1600:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_1601:
	add	rsi, rax
	je	.LBB2_1807
.LBB2_1602:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1602
	jmp	.LBB2_1807
.LBB2_356:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.357:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_358
# %bb.659:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_957
# %bb.660:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_957
.LBB2_358:
	xor	ecx, ecx
.LBB2_1607:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1610
# %bb.1608:
	mov	esi, 2147483647
.LBB2_1609:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1609
.LBB2_1610:
	cmp	r9, 3
	jb	.LBB2_1807
# %bb.1611:
	mov	esi, 2147483647
.LBB2_1612:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1612
	jmp	.LBB2_1807
.LBB2_359:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.360:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_662
# %bb.361:
	xor	edi, edi
	jmp	.LBB2_664
.LBB2_362:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.363:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_364
# %bb.667:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_960
# %bb.668:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_960
.LBB2_364:
	xor	edx, edx
.LBB2_1617:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1619
.LBB2_1618:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1618
.LBB2_1619:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1620:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 8] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 8], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 12] # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1620
	jmp	.LBB2_1807
.LBB2_365:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.366:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_367
# %bb.670:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_963
# %bb.671:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_963
.LBB2_367:
	xor	ecx, ecx
.LBB2_1625:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1627
# %bb.1626:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_1627:
	add	rsi, rax
	je	.LBB2_1807
.LBB2_1628:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1628
	jmp	.LBB2_1807
.LBB2_368:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.369:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_370
# %bb.673:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_966
# %bb.674:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_966
.LBB2_370:
	xor	ecx, ecx
.LBB2_1633:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1636
# %bb.1634:
	mov	esi, 2147483647
.LBB2_1635:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1635
.LBB2_1636:
	cmp	r9, 3
	jb	.LBB2_1807
# %bb.1637:
	mov	esi, 2147483647
.LBB2_1638:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1638
	jmp	.LBB2_1807
.LBB2_371:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.372:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_373
# %bb.676:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_969
# %bb.677:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_969
.LBB2_373:
	xor	esi, esi
.LBB2_1643:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1645
.LBB2_1644:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1644
.LBB2_1645:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1646:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1646
	jmp	.LBB2_1807
.LBB2_374:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.375:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_376
# %bb.679:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_972
# %bb.680:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_972
.LBB2_376:
	xor	edx, edx
.LBB2_1651:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1653
.LBB2_1652:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1652
.LBB2_1653:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1654:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 8], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1654
	jmp	.LBB2_1807
.LBB2_377:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.378:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_379
# %bb.682:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_975
# %bb.683:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_975
.LBB2_379:
	xor	esi, esi
.LBB2_1659:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1661
.LBB2_1660:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1660
.LBB2_1661:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1662:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1662
	jmp	.LBB2_1807
.LBB2_380:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.381:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_382
# %bb.685:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_978
# %bb.686:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_978
.LBB2_382:
	xor	edx, edx
.LBB2_1667:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1669
.LBB2_1668:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1668
.LBB2_1669:
	cmp	rsi, 3
	jb	.LBB2_1807
.LBB2_1670:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 8], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB2_1670
	jmp	.LBB2_1807
.LBB2_383:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.384:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_385
# %bb.688:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_981
# %bb.689:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_981
.LBB2_385:
	xor	ecx, ecx
.LBB2_1675:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1677
.LBB2_1676:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1676
.LBB2_1677:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1678:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 1]
	mov	byte ptr [r8 + rcx + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rcx + 2]
	mov	byte ptr [r8 + rcx + 2], al
	movzx	eax, byte ptr [rdx + rcx + 3]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx + 3], sil
	add	rcx, 4
	cmp	r10, rcx
	jne	.LBB2_1678
	jmp	.LBB2_1807
.LBB2_386:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.387:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB2_691
# %bb.388:
	xor	ecx, ecx
	jmp	.LBB2_990
.LBB2_389:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.390:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_391
# %bb.693:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_991
# %bb.694:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_991
.LBB2_391:
	xor	esi, esi
.LBB2_1683:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1685
.LBB2_1684:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1684
.LBB2_1685:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1686:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1686
	jmp	.LBB2_1807
.LBB2_392:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.393:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_394
# %bb.696:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_994
# %bb.697:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_994
.LBB2_394:
	xor	esi, esi
.LBB2_1691:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1693
.LBB2_1692:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1692
.LBB2_1693:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1694:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_1694
	jmp	.LBB2_1807
.LBB2_395:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.396:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_397
# %bb.699:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_997
# %bb.700:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_997
.LBB2_397:
	xor	edi, edi
.LBB2_1699:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1701
.LBB2_1700:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1700
.LBB2_1701:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1702:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1702
	jmp	.LBB2_1807
.LBB2_398:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.399:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_400
# %bb.702:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB2_1000
# %bb.703:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB2_1000
.LBB2_400:
	xor	ecx, ecx
.LBB2_1109:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1111
.LBB2_1110:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1110
.LBB2_1111:
	cmp	rdi, 3
	jb	.LBB2_1807
.LBB2_1112:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1112
	jmp	.LBB2_1807
.LBB2_401:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.402:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_403
# %bb.705:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_1002
# %bb.706:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_1002
.LBB2_403:
	xor	edi, edi
.LBB2_1707:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1709
.LBB2_1708:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1708
.LBB2_1709:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1710:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1710
	jmp	.LBB2_1807
.LBB2_404:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.405:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_406
# %bb.708:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB2_1005
# %bb.709:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB2_1005
.LBB2_406:
	xor	ecx, ecx
.LBB2_1119:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1121
.LBB2_1120:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1120
.LBB2_1121:
	cmp	rdi, 3
	jb	.LBB2_1807
.LBB2_1122:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1122
	jmp	.LBB2_1807
.LBB2_407:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.408:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_409
# %bb.711:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_1007
# %bb.712:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_1007
.LBB2_409:
	xor	esi, esi
.LBB2_1715:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1717
.LBB2_1716:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1716
.LBB2_1717:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1718:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1718
	jmp	.LBB2_1807
.LBB2_410:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.411:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_412
# %bb.714:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_1010
# %bb.715:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_1010
.LBB2_412:
	xor	esi, esi
.LBB2_1723:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1725
.LBB2_1724:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1724
.LBB2_1725:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1726:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1726
	jmp	.LBB2_1807
.LBB2_413:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.414:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_415
# %bb.717:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1013
# %bb.718:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1013
.LBB2_415:
	xor	ecx, ecx
.LBB2_1731:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1733
.LBB2_1732:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1732
.LBB2_1733:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1734:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1734
	jmp	.LBB2_1807
.LBB2_416:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.417:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_418
# %bb.720:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1016
# %bb.721:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1016
.LBB2_418:
	xor	ecx, ecx
.LBB2_1739:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1741
.LBB2_1740:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1740
.LBB2_1741:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1742:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 4]
	mov	dword ptr [r8 + 4*rcx + 4], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 8]
	mov	dword ptr [r8 + 4*rcx + 8], esi
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx + 12]
	mov	dword ptr [r8 + 4*rcx + 12], esi
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1742
	jmp	.LBB2_1807
.LBB2_419:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.420:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_421
# %bb.723:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1019
# %bb.724:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1019
.LBB2_421:
	xor	esi, esi
.LBB2_1747:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1749
.LBB2_1748:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1748
.LBB2_1749:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1750:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1750
	jmp	.LBB2_1807
.LBB2_422:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.423:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_424
# %bb.726:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1022
# %bb.727:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1022
.LBB2_424:
	xor	esi, esi
.LBB2_1755:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1757
.LBB2_1756:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1756
.LBB2_1757:
	cmp	rdx, 3
	jb	.LBB2_1807
.LBB2_1758:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1758
	jmp	.LBB2_1807
.LBB2_425:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.426:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_427
# %bb.729:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1025
# %bb.730:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1025
.LBB2_427:
	xor	esi, esi
.LBB2_1763:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1765
.LBB2_1764:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1764
.LBB2_1765:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1766:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1766
	jmp	.LBB2_1807
.LBB2_428:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.429:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_430
# %bb.732:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1028
# %bb.733:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1028
.LBB2_430:
	xor	ecx, ecx
.LBB2_1771:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1773
# %bb.1772:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_1773:
	add	rsi, rax
	je	.LBB2_1807
.LBB2_1774:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1774
	jmp	.LBB2_1807
.LBB2_431:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.432:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_433
# %bb.735:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1031
# %bb.736:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1031
.LBB2_433:
	xor	esi, esi
.LBB2_1779:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1781
.LBB2_1780:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1780
.LBB2_1781:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1782:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1782
	jmp	.LBB2_1807
.LBB2_434:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.435:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_436
# %bb.738:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1034
# %bb.739:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1034
.LBB2_436:
	xor	ecx, ecx
.LBB2_1787:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1789
# %bb.1788:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_1789:
	add	rsi, rax
	je	.LBB2_1807
.LBB2_1790:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1790
	jmp	.LBB2_1807
.LBB2_437:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.438:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_439
# %bb.741:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1037
# %bb.742:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1037
.LBB2_439:
	xor	esi, esi
.LBB2_1795:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1797
.LBB2_1796:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1796
.LBB2_1797:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1798:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1798
	jmp	.LBB2_1807
.LBB2_440:
	test	r9d, r9d
	jle	.LBB2_1807
# %bb.441:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_442
# %bb.744:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1040
# %bb.745:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1040
.LBB2_442:
	xor	esi, esi
.LBB2_1803:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1805
.LBB2_1804:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1804
.LBB2_1805:
	cmp	r9, 3
	jb	.LBB2_1807
.LBB2_1806:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1806
	jmp	.LBB2_1807
.LBB2_446:
	mov	ecx, eax
	and	ecx, -8
	lea	rdi, [rcx - 8]
	mov	rsi, rdi
	shr	rsi, 3
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 56
	jae	.LBB2_750
# %bb.447:
	xor	edi, edi
	jmp	.LBB2_752
.LBB2_535:
	mov	ecx, eax
	and	ecx, -4
	lea	rdi, [rcx - 4]
	mov	rsi, rdi
	shr	rsi, 2
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 28
	jae	.LBB2_842
# %bb.536:
	xor	edi, edi
	jmp	.LBB2_844
.LBB2_543:
	and	esi, -4
	xor	edi, edi
.LBB2_544:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rdx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB2_544
.LBB2_545:
	test	r9, r9
	je	.LBB2_1807
# %bb.546:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_547:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_547
	jmp	.LBB2_1807
.LBB2_551:
	and	esi, -4
	xor	edi, edi
.LBB2_552:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rdx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB2_552
.LBB2_553:
	test	r9, r9
	je	.LBB2_1807
# %bb.554:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_555:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_555
	jmp	.LBB2_1807
.LBB2_571:
	mov	ecx, eax
	and	ecx, -16
	lea	rdi, [rcx - 16]
	mov	rsi, rdi
	shr	rsi, 4
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 112
	jae	.LBB2_871
# %bb.572:
	xor	edi, edi
	jmp	.LBB2_873
.LBB2_648:
	and	esi, -4
	xor	edi, edi
.LBB2_649:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rdx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB2_649
.LBB2_650:
	test	r9, r9
	je	.LBB2_1807
# %bb.651:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_652:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_652
	jmp	.LBB2_1807
.LBB2_662:
	and	esi, -4
	xor	edi, edi
.LBB2_663:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 8]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 8], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 16]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 16], rdx
	mov	rdx, qword ptr [rcx + 8*rdi + 24]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rdi + 24], rdx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB2_663
.LBB2_664:
	test	r9, r9
	je	.LBB2_1807
# %bb.665:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_666:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_666
	jmp	.LBB2_1807
.LBB2_691:
	mov	ecx, eax
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	edx, esi
	and	edx, 7
	cmp	rdi, 224
	jae	.LBB2_984
# %bb.692:
	xor	edi, edi
	jmp	.LBB2_986
.LBB2_747:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1123
# %bb.748:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_749:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_749
	jmp	.LBB2_1124
.LBB2_757:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1131
# %bb.758:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_759:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_759
	jmp	.LBB2_1132
.LBB2_760:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1139
# %bb.761:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_762:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_762
	jmp	.LBB2_1140
.LBB2_763:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1147
# %bb.764:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_765:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_765
	jmp	.LBB2_1148
.LBB2_766:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB2_1043
# %bb.767:
	xor	eax, eax
	jmp	.LBB2_1045
.LBB2_768:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1155
# %bb.769:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_770:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_770
	jmp	.LBB2_1156
.LBB2_771:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB2_1053
# %bb.772:
	xor	eax, eax
	jmp	.LBB2_1055
.LBB2_773:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1163
# %bb.774:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_775:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_775
	jmp	.LBB2_1164
.LBB2_776:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1171
# %bb.777:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_778:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_778
	jmp	.LBB2_1172
.LBB2_779:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1179
# %bb.780:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_781:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_781
	jmp	.LBB2_1180
.LBB2_782:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1189
# %bb.783:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_784:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_784
	jmp	.LBB2_1190
.LBB2_785:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1199
# %bb.786:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_787:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	movapd	xmm2, xmm1
	subpd	xmm2, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 48]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	movapd	xmm2, xmm1
	subpd	xmm2, xmm3
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_787
	jmp	.LBB2_1200
.LBB2_788:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1207
# %bb.789:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_790:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	movapd	xmm2, xmm1
	subpd	xmm2, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 48]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	movapd	xmm2, xmm1
	subpd	xmm2, xmm3
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_790
	jmp	.LBB2_1208
.LBB2_791:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1215
# %bb.792:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_793:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 48]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_793
	jmp	.LBB2_1216
.LBB2_794:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB2_1223
# %bb.795:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_2] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB2_796:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_796
	jmp	.LBB2_1224
.LBB2_797:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1231
# %bb.798:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_799:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 48]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_799
	jmp	.LBB2_1232
.LBB2_800:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB2_1239
# %bb.801:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_2] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB2_802:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_802
	jmp	.LBB2_1240
.LBB2_803:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1247
# %bb.804:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_805:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_805
	jmp	.LBB2_1248
.LBB2_806:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1255
# %bb.807:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_808:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_808
	jmp	.LBB2_1256
.LBB2_809:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1263
# %bb.810:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_811:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_811
	jmp	.LBB2_1264
.LBB2_812:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1271
# %bb.813:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_814:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_814
	jmp	.LBB2_1272
.LBB2_815:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, r11b
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1279
# %bb.816:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_817:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_817
	jmp	.LBB2_1280
.LBB2_818:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, r11b
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1287
# %bb.819:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_820:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_820
	jmp	.LBB2_1288
.LBB2_821:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, dl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB2_1295
# %bb.822:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_823:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + rax]
	movdqu	xmm6, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB2_823
	jmp	.LBB2_1296
.LBB2_824:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1303
# %bb.825:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI2_4] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB2_826:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rsi]
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm3
	psrad	xmm6, 7
	movdqa	xmm7, xmm1
	psrad	xmm7, 7
	movdqa	xmm0, xmm4
	psrad	xmm0, 7
	paddd	xmm4, xmm0
	paddd	xmm1, xmm7
	paddd	xmm3, xmm6
	paddd	xmm2, xmm5
	pxor	xmm2, xmm5
	pxor	xmm3, xmm6
	pxor	xmm1, xmm7
	pxor	xmm4, xmm0
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 16]
	movdqa	xmm0, xmm2
	psrad	xmm0, 7
	movdqa	xmm5, xmm3
	psrad	xmm5, 7
	movdqa	xmm6, xmm1
	psrad	xmm6, 7
	movdqa	xmm7, xmm4
	psrad	xmm7, 7
	paddd	xmm4, xmm7
	paddd	xmm1, xmm6
	paddd	xmm3, xmm5
	paddd	xmm2, xmm0
	pxor	xmm2, xmm0
	pxor	xmm3, xmm5
	pxor	xmm1, xmm6
	pxor	xmm4, xmm7
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi + 16], xmm2
	add	rsi, 32
	add	rdi, 2
	jne	.LBB2_826
	jmp	.LBB2_1304
.LBB2_827:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, dl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB2_1311
# %bb.828:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_829:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + rax]
	movdqu	xmm6, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB2_829
	jmp	.LBB2_1312
.LBB2_830:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1319
# %bb.831:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI2_4] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB2_832:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rsi]
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm3
	psrad	xmm6, 7
	movdqa	xmm7, xmm1
	psrad	xmm7, 7
	movdqa	xmm0, xmm4
	psrad	xmm0, 7
	paddd	xmm4, xmm0
	paddd	xmm1, xmm7
	paddd	xmm3, xmm6
	paddd	xmm2, xmm5
	pxor	xmm2, xmm5
	pxor	xmm3, xmm6
	pxor	xmm1, xmm7
	pxor	xmm4, xmm0
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rsi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rsi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 16]
	movdqa	xmm0, xmm2
	psrad	xmm0, 7
	movdqa	xmm5, xmm3
	psrad	xmm5, 7
	movdqa	xmm6, xmm1
	psrad	xmm6, 7
	movdqa	xmm7, xmm4
	psrad	xmm7, 7
	paddd	xmm4, xmm7
	paddd	xmm1, xmm6
	paddd	xmm3, xmm5
	paddd	xmm2, xmm0
	pxor	xmm2, xmm0
	pxor	xmm3, xmm5
	pxor	xmm1, xmm6
	pxor	xmm4, xmm7
	pand	xmm4, xmm8
	pand	xmm1, xmm8
	packusdw	xmm1, xmm4
	pand	xmm3, xmm8
	pand	xmm2, xmm8
	packusdw	xmm2, xmm3
	packuswb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rsi + 16], xmm2
	add	rsi, 32
	add	rdi, 2
	jne	.LBB2_832
	jmp	.LBB2_1320
.LBB2_833:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, al
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1327
# %bb.834:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_835:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_835
	jmp	.LBB2_1328
.LBB2_836:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, al
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1335
# %bb.837:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_838:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_838
	jmp	.LBB2_1336
.LBB2_839:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1343
# %bb.840:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_841:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_841
	jmp	.LBB2_1344
.LBB2_849:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1351
# %bb.850:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_851:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_851
	jmp	.LBB2_1352
.LBB2_852:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1359
# %bb.853:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_854:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_854
	jmp	.LBB2_1360
.LBB2_855:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB2_1063
# %bb.856:
	xor	eax, eax
	jmp	.LBB2_1065
.LBB2_857:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB2_1073
# %bb.858:
	xor	eax, eax
	jmp	.LBB2_1075
.LBB2_859:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1367
# %bb.860:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_861:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_861
	jmp	.LBB2_1368
.LBB2_862:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1375
# %bb.863:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_864:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_864
	jmp	.LBB2_1376
.LBB2_865:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1383
# %bb.866:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_867:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_867
	jmp	.LBB2_1384
.LBB2_868:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1391
# %bb.869:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_870:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_870
	jmp	.LBB2_1392
.LBB2_878:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1399
# %bb.879:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_880:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_880
	jmp	.LBB2_1400
.LBB2_881:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1407
# %bb.882:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_883:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_883
	jmp	.LBB2_1408
.LBB2_884:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1415
# %bb.885:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_886:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_886
	jmp	.LBB2_1416
.LBB2_887:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1423
# %bb.888:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_889:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_889
	jmp	.LBB2_1424
.LBB2_890:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1431
# %bb.891:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_892:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_892
	jmp	.LBB2_1432
.LBB2_893:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1439
# %bb.894:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_895:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_895
	jmp	.LBB2_1440
.LBB2_896:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1447
# %bb.897:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_898:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_898
	jmp	.LBB2_1448
.LBB2_899:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB2_1083
# %bb.900:
	xor	eax, eax
	jmp	.LBB2_1085
.LBB2_901:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1455
# %bb.902:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB2_903:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi + 16]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	add	rsi, 16
	add	rdi, 2
	jne	.LBB2_903
	jmp	.LBB2_1456
.LBB2_904:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1463
# %bb.905:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_906:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_906
	jmp	.LBB2_1464
.LBB2_907:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1471
# %bb.908:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_909:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_909
	jmp	.LBB2_1472
.LBB2_910:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB2_1093
# %bb.911:
	xor	eax, eax
	jmp	.LBB2_1095
.LBB2_912:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1479
# %bb.913:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB2_914:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rsi + 16]
	movdqa	xmm3, xmm2
	psrad	xmm3, 15
	movdqa	xmm4, xmm1
	psrad	xmm4, 15
	paddd	xmm1, xmm4
	paddd	xmm2, xmm3
	pxor	xmm2, xmm3
	pxor	xmm1, xmm4
	pblendw	xmm1, xmm0, 170                 # xmm1 = xmm1[0],xmm0[1],xmm1[2],xmm0[3],xmm1[4],xmm0[5],xmm1[6],xmm0[7]
	pblendw	xmm2, xmm0, 170                 # xmm2 = xmm2[0],xmm0[1],xmm2[2],xmm0[3],xmm2[4],xmm0[5],xmm2[6],xmm0[7]
	packusdw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm2
	add	rsi, 16
	add	rdi, 2
	jne	.LBB2_914
	jmp	.LBB2_1480
.LBB2_915:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1487
# %bb.916:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_917:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_917
	jmp	.LBB2_1488
.LBB2_918:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1495
# %bb.919:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_920:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_920
	jmp	.LBB2_1496
.LBB2_921:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1503
# %bb.922:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_923:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_923
	jmp	.LBB2_1504
.LBB2_924:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, eax
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1511
# %bb.925:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_926:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm2
	add	rdi, 32
	add	rdx, 2
	jne	.LBB2_926
	jmp	.LBB2_1512
.LBB2_927:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1519
# %bb.928:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_929:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_929
	jmp	.LBB2_1520
.LBB2_930:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1527
# %bb.931:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_932:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_932
	jmp	.LBB2_1528
.LBB2_933:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1537
# %bb.934:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_935:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rsi, 2
	jne	.LBB2_935
	jmp	.LBB2_1538
.LBB2_936:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1545
# %bb.937:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_938:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm1, xmm0
	pxor	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_938
	jmp	.LBB2_1546
.LBB2_939:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1555
# %bb.940:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_941:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_941
	jmp	.LBB2_1556
.LBB2_942:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1563
# %bb.943:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_944:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	movaps	xmm2, xmm1
	subps	xmm2, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 48]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	movaps	xmm2, xmm1
	subps	xmm2, xmm3
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_944
	jmp	.LBB2_1564
.LBB2_945:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1571
# %bb.946:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_947:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_947
	jmp	.LBB2_1572
.LBB2_948:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1579
# %bb.949:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_950:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	movaps	xmm2, xmm1
	subps	xmm2, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 48]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	movaps	xmm2, xmm1
	subps	xmm2, xmm3
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_950
	jmp	.LBB2_1580
.LBB2_951:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1587
# %bb.952:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_953:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 48]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_953
	jmp	.LBB2_1588
.LBB2_954:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1595
# %bb.955:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB2_956:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB2_956
	jmp	.LBB2_1596
.LBB2_957:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1603
# %bb.958:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_3] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB2_959:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_959
	jmp	.LBB2_1604
.LBB2_960:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1613
# %bb.961:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_962:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 48]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_962
	jmp	.LBB2_1614
.LBB2_963:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1621
# %bb.964:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB2_965:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB2_965
	jmp	.LBB2_1622
.LBB2_966:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1629
# %bb.967:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_3] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB2_968:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pand	xmm1, xmm0
	pand	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_968
	jmp	.LBB2_1630
.LBB2_969:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1639
# %bb.970:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_971:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_971
	jmp	.LBB2_1640
.LBB2_972:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1647
# %bb.973:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_974:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_974
	jmp	.LBB2_1648
.LBB2_975:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1655
# %bb.976:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_977:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rdx, 2
	jne	.LBB2_977
	jmp	.LBB2_1656
.LBB2_978:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1663
# %bb.979:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_980:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_980
	jmp	.LBB2_1664
.LBB2_981:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1671
# %bb.982:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_983:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_983
	jmp	.LBB2_1672
.LBB2_991:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, r11b
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1679
# %bb.992:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_993:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_993
	jmp	.LBB2_1680
.LBB2_994:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, r11b
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1687
# %bb.995:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_996:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + rdi + 48], xmm1
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_996
	jmp	.LBB2_1688
.LBB2_997:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, dl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB2_1695
# %bb.998:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_999:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + rax]
	movdqu	xmm6, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB2_999
	jmp	.LBB2_1696
.LBB2_1000:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB2_1103
# %bb.1001:
	xor	edi, edi
	jmp	.LBB2_1105
.LBB2_1002:
	mov	edi, r10d
	and	edi, -32
	movzx	eax, dl
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	pmovzxbw	xmm1, xmm0                      # xmm1 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	test	rax, rax
	je	.LBB2_1703
# %bb.1003:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_5] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_1004:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + rax]
	movdqu	xmm6, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + rax + 48]
	pmovzxbw	xmm7, xmm5                      # xmm7 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	punpckhbw	xmm5, xmm5              # xmm5 = xmm5[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm5, xmm2
	pand	xmm5, xmm3
	pmullw	xmm7, xmm1
	pand	xmm7, xmm3
	packuswb	xmm7, xmm5
	pmovzxbw	xmm5, xmm6                      # xmm5 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	punpckhbw	xmm6, xmm6              # xmm6 = xmm6[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm6, xmm4
	pand	xmm6, xmm3
	pmullw	xmm5, xmm1
	pand	xmm5, xmm3
	packuswb	xmm5, xmm6
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rsi, 2
	jne	.LBB2_1004
	jmp	.LBB2_1704
.LBB2_1005:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB2_1113
# %bb.1006:
	xor	edi, edi
	jmp	.LBB2_1115
.LBB2_1007:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, al
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1711
# %bb.1008:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1009:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_1009
	jmp	.LBB2_1712
.LBB2_1010:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, al
	movd	xmm0, edx
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1719
# %bb.1011:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1012:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + rdi + 48], xmm2
	add	rdi, 64
	add	rdx, 2
	jne	.LBB2_1012
	jmp	.LBB2_1720
.LBB2_1013:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1727
# %bb.1014:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1015:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_1015
	jmp	.LBB2_1728
.LBB2_1016:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1735
# %bb.1017:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1018:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_1018
	jmp	.LBB2_1736
.LBB2_1019:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1743
# %bb.1020:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1021:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_1021
	jmp	.LBB2_1744
.LBB2_1022:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1751
# %bb.1023:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1024:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_1024
	jmp	.LBB2_1752
.LBB2_1025:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1759
# %bb.1026:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1027:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_1027
	jmp	.LBB2_1760
.LBB2_1028:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1767
# %bb.1029:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1030:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_1030
	jmp	.LBB2_1768
.LBB2_1031:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1775
# %bb.1032:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1033:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_1033
	jmp	.LBB2_1776
.LBB2_1034:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1783
# %bb.1035:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1036:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rsi, 2
	jne	.LBB2_1036
	jmp	.LBB2_1784
.LBB2_1037:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1791
# %bb.1038:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1039:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_1039
	jmp	.LBB2_1792
.LBB2_1040:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1799
# %bb.1041:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1042:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rdx, 2
	jne	.LBB2_1042
	jmp	.LBB2_1800
.LBB2_750:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB2_751:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 240], xmm0
	add	rdi, 64
	add	rsi, 8
	jne	.LBB2_751
.LBB2_752:
	test	rdx, rdx
	je	.LBB2_755
# %bb.753:
	lea	rsi, [r8 + 4*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB2_754:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB2_754
.LBB2_755:
	cmp	rcx, rax
	je	.LBB2_1807
.LBB2_756:                              # =>This Inner Loop Header: Depth=1
	mov	dword ptr [r8 + 4*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_756
	jmp	.LBB2_1807
.LBB2_842:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB2_843:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 240], xmm0
	add	rdi, 32
	add	rsi, 8
	jne	.LBB2_843
.LBB2_844:
	test	rdx, rdx
	je	.LBB2_847
# %bb.845:
	lea	rsi, [r8 + 8*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB2_846:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB2_846
.LBB2_847:
	cmp	rcx, rax
	je	.LBB2_1807
.LBB2_848:                              # =>This Inner Loop Header: Depth=1
	mov	qword ptr [r8 + 8*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_848
	jmp	.LBB2_1807
.LBB2_871:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB2_872:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 240], xmm0
	sub	rdi, -128
	add	rsi, 8
	jne	.LBB2_872
.LBB2_873:
	test	rdx, rdx
	je	.LBB2_876
# %bb.874:
	lea	rsi, [r8 + 2*rdi]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB2_875:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB2_875
.LBB2_876:
	cmp	rcx, rax
	je	.LBB2_1807
.LBB2_877:                              # =>This Inner Loop Header: Depth=1
	mov	word ptr [r8 + 2*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_877
	jmp	.LBB2_1807
.LBB2_984:
	and	rsi, -8
	neg	rsi
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB2_985:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
	movdqu	xmmword ptr [r8 + rdi + 32], xmm0
	movdqu	xmmword ptr [r8 + rdi + 48], xmm0
	movdqu	xmmword ptr [r8 + rdi + 64], xmm0
	movdqu	xmmword ptr [r8 + rdi + 80], xmm0
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm0
	movdqu	xmmword ptr [r8 + rdi + 128], xmm0
	movdqu	xmmword ptr [r8 + rdi + 144], xmm0
	movdqu	xmmword ptr [r8 + rdi + 160], xmm0
	movdqu	xmmword ptr [r8 + rdi + 176], xmm0
	movdqu	xmmword ptr [r8 + rdi + 192], xmm0
	movdqu	xmmword ptr [r8 + rdi + 208], xmm0
	movdqu	xmmword ptr [r8 + rdi + 224], xmm0
	movdqu	xmmword ptr [r8 + rdi + 240], xmm0
	add	rdi, 256
	add	rsi, 8
	jne	.LBB2_985
.LBB2_986:
	test	rdx, rdx
	je	.LBB2_989
# %bb.987:
	lea	rsi, [rdi + r8]
	add	rsi, 16
	neg	rdx
	pxor	xmm0, xmm0
.LBB2_988:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmmword ptr [rsi - 16], xmm0
	movdqu	xmmword ptr [rsi], xmm0
	add	rsi, 32
	inc	rdx
	jne	.LBB2_988
.LBB2_989:
	cmp	rcx, rax
	je	.LBB2_1807
.LBB2_990:                              # =>This Inner Loop Header: Depth=1
	mov	byte ptr [r8 + rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_990
.LBB2_1807:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB2_1043:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1044:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [r8 + 4*rax + 64], xmm0
	movups	xmmword ptr [r8 + 4*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movdqu	xmmword ptr [r8 + 4*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB2_1044
.LBB2_1045:
	test	rsi, rsi
	je	.LBB2_1048
# %bb.1046:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB2_1047:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_1047
.LBB2_1048:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1049
.LBB2_1053:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1054:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [r8 + 4*rax + 64], xmm0
	movups	xmmword ptr [r8 + 4*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movdqu	xmmword ptr [r8 + 4*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB2_1054
.LBB2_1055:
	test	rsi, rsi
	je	.LBB2_1058
# %bb.1056:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB2_1057:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_1057
.LBB2_1058:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1059
.LBB2_1063:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1064:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [r8 + 8*rax], xmm0
	movups	xmmword ptr [r8 + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [r8 + 8*rax + 32], xmm0
	movups	xmmword ptr [r8 + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [r8 + 8*rax + 64], xmm0
	movups	xmmword ptr [r8 + 8*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movdqu	xmmword ptr [r8 + 8*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB2_1064
.LBB2_1065:
	test	rsi, rsi
	je	.LBB2_1068
# %bb.1066:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB2_1067:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_1067
.LBB2_1068:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1069
.LBB2_1073:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1074:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [r8 + 8*rax], xmm0
	movups	xmmword ptr [r8 + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [r8 + 8*rax + 32], xmm0
	movups	xmmword ptr [r8 + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [r8 + 8*rax + 64], xmm0
	movups	xmmword ptr [r8 + 8*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movdqu	xmmword ptr [r8 + 8*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB2_1074
.LBB2_1075:
	test	rsi, rsi
	je	.LBB2_1078
# %bb.1076:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB2_1077:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_1077
.LBB2_1078:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1079
.LBB2_1083:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1084:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [r8 + 2*rax], xmm0
	movups	xmmword ptr [r8 + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [r8 + 2*rax + 32], xmm0
	movups	xmmword ptr [r8 + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [r8 + 2*rax + 64], xmm0
	movups	xmmword ptr [r8 + 2*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movdqu	xmmword ptr [r8 + 2*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB2_1084
.LBB2_1085:
	test	rsi, rsi
	je	.LBB2_1088
# %bb.1086:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB2_1087:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_1087
.LBB2_1088:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1089
.LBB2_1093:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1094:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [r8 + 2*rax], xmm0
	movups	xmmword ptr [r8 + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [r8 + 2*rax + 32], xmm0
	movups	xmmword ptr [r8 + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [r8 + 2*rax + 64], xmm0
	movups	xmmword ptr [r8 + 2*rax + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movdqu	xmmword ptr [r8 + 2*rax + 96], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB2_1094
.LBB2_1095:
	test	rsi, rsi
	je	.LBB2_1098
# %bb.1096:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB2_1097:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_1097
.LBB2_1098:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1099
.LBB2_1103:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB2_1104:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rdi]
	movups	xmm1, xmmword ptr [rdx + rdi + 16]
	movups	xmmword ptr [r8 + rdi], xmm0
	movups	xmmword ptr [r8 + rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 32]
	movups	xmm1, xmmword ptr [rdx + rdi + 48]
	movups	xmmword ptr [r8 + rdi + 32], xmm0
	movups	xmmword ptr [r8 + rdi + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 64]
	movups	xmm1, xmmword ptr [rdx + rdi + 80]
	movups	xmmword ptr [r8 + rdi + 64], xmm0
	movups	xmmword ptr [r8 + rdi + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 96]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 112]
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm1
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB2_1104
.LBB2_1105:
	test	rax, rax
	je	.LBB2_1108
# %bb.1106:
	add	rdi, 16
	neg	rax
.LBB2_1107:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB2_1107
.LBB2_1108:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1109
.LBB2_1113:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB2_1114:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rdi]
	movups	xmm1, xmmword ptr [rdx + rdi + 16]
	movups	xmmword ptr [r8 + rdi], xmm0
	movups	xmmword ptr [r8 + rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 32]
	movups	xmm1, xmmword ptr [rdx + rdi + 48]
	movups	xmmword ptr [r8 + rdi + 32], xmm0
	movups	xmmword ptr [r8 + rdi + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rdi + 64]
	movups	xmm1, xmmword ptr [rdx + rdi + 80]
	movups	xmmword ptr [r8 + rdi + 64], xmm0
	movups	xmmword ptr [r8 + rdi + 80], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rdi + 96]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 112]
	movdqu	xmmword ptr [r8 + rdi + 96], xmm0
	movdqu	xmmword ptr [r8 + rdi + 112], xmm1
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB2_1114
.LBB2_1115:
	test	rax, rax
	je	.LBB2_1118
# %bb.1116:
	add	rdi, 16
	neg	rax
.LBB2_1117:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB2_1117
.LBB2_1118:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1119
.LBB2_1123:
	xor	edi, edi
.LBB2_1124:
	test	r9b, 1
	je	.LBB2_1126
# %bb.1125:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1126:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1127
.LBB2_1131:
	xor	edi, edi
.LBB2_1132:
	test	r9b, 1
	je	.LBB2_1134
# %bb.1133:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1134:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1135
.LBB2_1139:
	xor	edi, edi
.LBB2_1140:
	test	r9b, 1
	je	.LBB2_1142
# %bb.1141:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1142:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1143
.LBB2_1147:
	xor	edi, edi
.LBB2_1148:
	test	r9b, 1
	je	.LBB2_1150
# %bb.1149:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1150:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1151
.LBB2_1155:
	xor	edi, edi
.LBB2_1156:
	test	r9b, 1
	je	.LBB2_1158
# %bb.1157:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1158:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1159
.LBB2_1163:
	xor	edi, edi
.LBB2_1164:
	test	r9b, 1
	je	.LBB2_1166
# %bb.1165:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1166:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1167
.LBB2_1171:
	xor	edi, edi
.LBB2_1172:
	test	r9b, 1
	je	.LBB2_1174
# %bb.1173:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1174:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1175
.LBB2_1179:
	xor	edi, edi
.LBB2_1180:
	test	r9b, 1
	je	.LBB2_1182
# %bb.1181:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_0] # xmm2 = [-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_1182:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1183
.LBB2_1189:
	xor	edi, edi
.LBB2_1190:
	test	r9b, 1
	je	.LBB2_1192
# %bb.1191:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_0] # xmm2 = [-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_1192:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1193
.LBB2_1199:
	xor	edi, edi
.LBB2_1200:
	test	r9b, 1
	je	.LBB2_1202
# %bb.1201:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_1202:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1203
.LBB2_1207:
	xor	edi, edi
.LBB2_1208:
	test	r9b, 1
	je	.LBB2_1210
# %bb.1209:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_1210:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1211
.LBB2_1215:
	xor	edi, edi
.LBB2_1216:
	test	r9b, 1
	je	.LBB2_1218
# %bb.1217:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_1218:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1219
.LBB2_1223:
	xor	edi, edi
.LBB2_1224:
	test	r9b, 1
	je	.LBB2_1226
# %bb.1225:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_2] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_1226:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1227
.LBB2_1231:
	xor	edi, edi
.LBB2_1232:
	test	r9b, 1
	je	.LBB2_1234
# %bb.1233:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_1234:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1235
.LBB2_1239:
	xor	edi, edi
.LBB2_1240:
	test	r9b, 1
	je	.LBB2_1242
# %bb.1241:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_2] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_1242:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1243
.LBB2_1247:
	xor	edi, edi
.LBB2_1248:
	test	r9b, 1
	je	.LBB2_1250
# %bb.1249:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_1250:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1251
.LBB2_1255:
	xor	edi, edi
.LBB2_1256:
	test	r9b, 1
	je	.LBB2_1258
# %bb.1257:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_1258:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1259
.LBB2_1263:
	xor	edi, edi
.LBB2_1264:
	test	r9b, 1
	je	.LBB2_1266
# %bb.1265:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1266:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1267
.LBB2_1271:
	xor	edi, edi
.LBB2_1272:
	test	r9b, 1
	je	.LBB2_1274
# %bb.1273:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1274:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1275
.LBB2_1279:
	xor	edi, edi
.LBB2_1280:
	test	r9b, 1
	je	.LBB2_1282
# %bb.1281:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1282:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1283
.LBB2_1287:
	xor	edi, edi
.LBB2_1288:
	test	r9b, 1
	je	.LBB2_1290
# %bb.1289:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1290:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1291
.LBB2_1295:
	xor	eax, eax
.LBB2_1296:
	test	r9b, 1
	je	.LBB2_1298
# %bb.1297:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB2_1298:
	cmp	rdi, r10
	je	.LBB2_1807
	jmp	.LBB2_1299
.LBB2_1303:
	xor	esi, esi
.LBB2_1304:
	test	r9b, 1
	je	.LBB2_1306
# %bb.1305:
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rsi]
	movdqa	xmm4, xmm1
	psrad	xmm4, 7
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm0
	psrad	xmm6, 7
	movdqa	xmm7, xmm3
	psrad	xmm7, 7
	paddd	xmm3, xmm7
	paddd	xmm0, xmm6
	paddd	xmm2, xmm5
	paddd	xmm1, xmm4
	pxor	xmm1, xmm4
	pxor	xmm2, xmm5
	pxor	xmm0, xmm6
	pxor	xmm3, xmm7
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_4] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB2_1306:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1307
.LBB2_1311:
	xor	eax, eax
.LBB2_1312:
	test	r9b, 1
	je	.LBB2_1314
# %bb.1313:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB2_1314:
	cmp	rdi, r10
	je	.LBB2_1807
	jmp	.LBB2_1315
.LBB2_1319:
	xor	esi, esi
.LBB2_1320:
	test	r9b, 1
	je	.LBB2_1322
# %bb.1321:
	pmovsxbd	xmm3, dword ptr [rdx + rsi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rsi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rsi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rsi]
	movdqa	xmm4, xmm1
	psrad	xmm4, 7
	movdqa	xmm5, xmm2
	psrad	xmm5, 7
	movdqa	xmm6, xmm0
	psrad	xmm6, 7
	movdqa	xmm7, xmm3
	psrad	xmm7, 7
	paddd	xmm3, xmm7
	paddd	xmm0, xmm6
	paddd	xmm2, xmm5
	paddd	xmm1, xmm4
	pxor	xmm1, xmm4
	pxor	xmm2, xmm5
	pxor	xmm0, xmm6
	pxor	xmm3, xmm7
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_4] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB2_1322:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1323
.LBB2_1327:
	xor	edi, edi
.LBB2_1328:
	test	r9b, 1
	je	.LBB2_1330
# %bb.1329:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1330:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1331
.LBB2_1335:
	xor	edi, edi
.LBB2_1336:
	test	r9b, 1
	je	.LBB2_1338
# %bb.1337:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1338:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1339
.LBB2_1343:
	xor	edi, edi
.LBB2_1344:
	test	r9b, 1
	je	.LBB2_1346
# %bb.1345:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1346:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1347
.LBB2_1351:
	xor	edi, edi
.LBB2_1352:
	test	r9b, 1
	je	.LBB2_1354
# %bb.1353:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1354:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1355
.LBB2_1359:
	xor	edi, edi
.LBB2_1360:
	test	r9b, 1
	je	.LBB2_1362
# %bb.1361:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1362:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1363
.LBB2_1367:
	xor	edi, edi
.LBB2_1368:
	test	r9b, 1
	je	.LBB2_1370
# %bb.1369:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1370:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1371
.LBB2_1375:
	xor	edi, edi
.LBB2_1376:
	test	r9b, 1
	je	.LBB2_1378
# %bb.1377:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1378:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1379
.LBB2_1383:
	xor	edi, edi
.LBB2_1384:
	test	r9b, 1
	je	.LBB2_1386
# %bb.1385:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1386:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1387
.LBB2_1391:
	xor	edi, edi
.LBB2_1392:
	test	r9b, 1
	je	.LBB2_1394
# %bb.1393:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1394:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1395
.LBB2_1399:
	xor	edi, edi
.LBB2_1400:
	test	r9b, 1
	je	.LBB2_1402
# %bb.1401:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1402:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1403
.LBB2_1407:
	xor	edi, edi
.LBB2_1408:
	test	r9b, 1
	je	.LBB2_1410
# %bb.1409:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1410:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1411
.LBB2_1415:
	xor	edi, edi
.LBB2_1416:
	test	r9b, 1
	je	.LBB2_1418
# %bb.1417:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1418:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1419
.LBB2_1423:
	xor	edi, edi
.LBB2_1424:
	test	r9b, 1
	je	.LBB2_1426
# %bb.1425:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1426:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1427
.LBB2_1431:
	xor	edi, edi
.LBB2_1432:
	test	r9b, 1
	je	.LBB2_1434
# %bb.1433:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1434:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1435
.LBB2_1439:
	xor	edi, edi
.LBB2_1440:
	test	r9b, 1
	je	.LBB2_1442
# %bb.1441:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1442:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1443
.LBB2_1447:
	xor	edi, edi
.LBB2_1448:
	test	r9b, 1
	je	.LBB2_1450
# %bb.1449:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1450:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1451
.LBB2_1455:
	xor	esi, esi
.LBB2_1456:
	test	r9b, 1
	je	.LBB2_1458
# %bb.1457:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi]
	movdqa	xmm2, xmm1
	psrad	xmm2, 15
	movdqa	xmm3, xmm0
	psrad	xmm3, 15
	paddd	xmm0, xmm3
	paddd	xmm1, xmm2
	pxor	xmm1, xmm2
	pxor	xmm0, xmm3
	pxor	xmm2, xmm2
	pblendw	xmm0, xmm2, 170                 # xmm0 = xmm0[0],xmm2[1],xmm0[2],xmm2[3],xmm0[4],xmm2[5],xmm0[6],xmm2[7]
	pblendw	xmm1, xmm2, 170                 # xmm1 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	packusdw	xmm1, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm1
.LBB2_1458:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1459
.LBB2_1463:
	xor	edi, edi
.LBB2_1464:
	test	r9b, 1
	je	.LBB2_1466
# %bb.1465:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1466:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1467
.LBB2_1471:
	xor	edi, edi
.LBB2_1472:
	test	r9b, 1
	je	.LBB2_1474
# %bb.1473:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1474:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1475
.LBB2_1479:
	xor	esi, esi
.LBB2_1480:
	test	r9b, 1
	je	.LBB2_1482
# %bb.1481:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rsi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rsi]
	movdqa	xmm2, xmm1
	psrad	xmm2, 15
	movdqa	xmm3, xmm0
	psrad	xmm3, 15
	paddd	xmm0, xmm3
	paddd	xmm1, xmm2
	pxor	xmm1, xmm2
	pxor	xmm0, xmm3
	pxor	xmm2, xmm2
	pblendw	xmm0, xmm2, 170                 # xmm0 = xmm0[0],xmm2[1],xmm0[2],xmm2[3],xmm0[4],xmm2[5],xmm0[6],xmm2[7]
	pblendw	xmm1, xmm2, 170                 # xmm1 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	packusdw	xmm1, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm1
.LBB2_1482:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1483
.LBB2_1487:
	xor	edi, edi
.LBB2_1488:
	test	r9b, 1
	je	.LBB2_1490
# %bb.1489:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1490:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1491
.LBB2_1495:
	xor	edi, edi
.LBB2_1496:
	test	r9b, 1
	je	.LBB2_1498
# %bb.1497:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1498:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1499
.LBB2_1503:
	xor	edi, edi
.LBB2_1504:
	test	r9b, 1
	je	.LBB2_1506
# %bb.1505:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1506:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1507
.LBB2_1511:
	xor	edi, edi
.LBB2_1512:
	test	r9b, 1
	je	.LBB2_1514
# %bb.1513:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1514:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1515
.LBB2_1519:
	xor	edi, edi
.LBB2_1520:
	test	r9b, 1
	je	.LBB2_1522
# %bb.1521:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1522:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1523
.LBB2_1527:
	xor	edi, edi
.LBB2_1528:
	test	r9b, 1
	je	.LBB2_1530
# %bb.1529:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_1] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1530:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1531
.LBB2_1537:
	xor	edi, edi
.LBB2_1538:
	test	r9b, 1
	je	.LBB2_1540
# %bb.1539:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1540:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1541
.LBB2_1545:
	xor	edi, edi
.LBB2_1546:
	test	r9b, 1
	je	.LBB2_1548
# %bb.1547:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_1] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	pxor	xmm0, xmm2
	pxor	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1548:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1549
.LBB2_1555:
	xor	edi, edi
.LBB2_1556:
	test	r9b, 1
	je	.LBB2_1558
# %bb.1557:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1558:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1559
.LBB2_1563:
	xor	edi, edi
.LBB2_1564:
	test	r9b, 1
	je	.LBB2_1566
# %bb.1565:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1566:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1567
.LBB2_1571:
	xor	edi, edi
.LBB2_1572:
	test	r9b, 1
	je	.LBB2_1574
# %bb.1573:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1574:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1575
.LBB2_1579:
	xor	edi, edi
.LBB2_1580:
	test	r9b, 1
	je	.LBB2_1582
# %bb.1581:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1582:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1583
.LBB2_1587:
	xor	edi, edi
.LBB2_1588:
	test	r9b, 1
	je	.LBB2_1590
# %bb.1589:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1590:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1591
.LBB2_1595:
	xor	esi, esi
.LBB2_1596:
	test	r9b, 1
	je	.LBB2_1598
# %bb.1597:
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
.LBB2_1598:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1599
.LBB2_1603:
	xor	edi, edi
.LBB2_1604:
	test	r9b, 1
	je	.LBB2_1606
# %bb.1605:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_3] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1606:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1607
.LBB2_1613:
	xor	edi, edi
.LBB2_1614:
	test	r9b, 1
	je	.LBB2_1616
# %bb.1615:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1616:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1617
.LBB2_1621:
	xor	esi, esi
.LBB2_1622:
	test	r9b, 1
	je	.LBB2_1624
# %bb.1623:
	movdqu	xmm1, xmmword ptr [rdx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rsi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm1
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm2
.LBB2_1624:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1625
.LBB2_1629:
	xor	edi, edi
.LBB2_1630:
	test	r9b, 1
	je	.LBB2_1632
# %bb.1631:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_3] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1632:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1633
.LBB2_1639:
	xor	edi, edi
.LBB2_1640:
	test	r9b, 1
	je	.LBB2_1642
# %bb.1641:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1642:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1643
.LBB2_1647:
	xor	edi, edi
.LBB2_1648:
	test	r9b, 1
	je	.LBB2_1650
# %bb.1649:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1650:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1651
.LBB2_1655:
	xor	edi, edi
.LBB2_1656:
	test	r9b, 1
	je	.LBB2_1658
# %bb.1657:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1658:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1659
.LBB2_1663:
	xor	edi, edi
.LBB2_1664:
	test	r9b, 1
	je	.LBB2_1666
# %bb.1665:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1666:
	cmp	rdx, rax
	je	.LBB2_1807
	jmp	.LBB2_1667
.LBB2_1671:
	xor	edi, edi
.LBB2_1672:
	test	r9b, 1
	je	.LBB2_1674
# %bb.1673:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1674:
	cmp	rcx, r10
	je	.LBB2_1807
	jmp	.LBB2_1675
.LBB2_1679:
	xor	edi, edi
.LBB2_1680:
	test	r9b, 1
	je	.LBB2_1682
# %bb.1681:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1682:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1683
.LBB2_1687:
	xor	edi, edi
.LBB2_1688:
	test	r9b, 1
	je	.LBB2_1690
# %bb.1689:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1690:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1691
.LBB2_1695:
	xor	eax, eax
.LBB2_1696:
	test	r9b, 1
	je	.LBB2_1698
# %bb.1697:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB2_1698:
	cmp	rdi, r10
	je	.LBB2_1807
	jmp	.LBB2_1699
.LBB2_1703:
	xor	eax, eax
.LBB2_1704:
	test	r9b, 1
	je	.LBB2_1706
# %bb.1705:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_5] # xmm4 = [255,255,255,255,255,255,255,255]
	pand	xmm2, xmm4
	pmullw	xmm5, xmm1
	pand	xmm5, xmm4
	packuswb	xmm5, xmm2
	punpckhbw	xmm0, xmm0              # xmm0 = xmm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm2, xmm3                      # xmm2 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm0
	pand	xmm3, xmm4
	pmullw	xmm2, xmm1
	pand	xmm2, xmm4
	packuswb	xmm2, xmm3
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmmword ptr [r8 + rax + 16], xmm2
.LBB2_1706:
	cmp	rdi, r10
	je	.LBB2_1807
	jmp	.LBB2_1707
.LBB2_1711:
	xor	edi, edi
.LBB2_1712:
	test	r9b, 1
	je	.LBB2_1714
# %bb.1713:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1714:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1715
.LBB2_1719:
	xor	edi, edi
.LBB2_1720:
	test	r9b, 1
	je	.LBB2_1722
# %bb.1721:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1722:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1723
.LBB2_1727:
	xor	edi, edi
.LBB2_1728:
	test	r9b, 1
	je	.LBB2_1730
# %bb.1729:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1730:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1731
.LBB2_1735:
	xor	edi, edi
.LBB2_1736:
	test	r9b, 1
	je	.LBB2_1738
# %bb.1737:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1738:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1739
.LBB2_1743:
	xor	edi, edi
.LBB2_1744:
	test	r9b, 1
	je	.LBB2_1746
# %bb.1745:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1746:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1747
.LBB2_1751:
	xor	edi, edi
.LBB2_1752:
	test	r9b, 1
	je	.LBB2_1754
# %bb.1753:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1754:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1755
.LBB2_1759:
	xor	edi, edi
.LBB2_1760:
	test	r9b, 1
	je	.LBB2_1762
# %bb.1761:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1762:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1763
.LBB2_1767:
	xor	edi, edi
.LBB2_1768:
	test	r9b, 1
	je	.LBB2_1770
# %bb.1769:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1770:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1771
.LBB2_1775:
	xor	edi, edi
.LBB2_1776:
	test	r9b, 1
	je	.LBB2_1778
# %bb.1777:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1778:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1779
.LBB2_1783:
	xor	edi, edi
.LBB2_1784:
	test	r9b, 1
	je	.LBB2_1786
# %bb.1785:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1786:
	cmp	rcx, rax
	je	.LBB2_1807
	jmp	.LBB2_1787
.LBB2_1791:
	xor	edi, edi
.LBB2_1792:
	test	r9b, 1
	je	.LBB2_1794
# %bb.1793:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1794:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1795
.LBB2_1799:
	xor	edi, edi
.LBB2_1800:
	test	r9b, 1
	je	.LBB2_1802
# %bb.1801:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1802:
	cmp	rsi, r10
	je	.LBB2_1807
	jmp	.LBB2_1803
.Lfunc_end2:
	.size	arithmetic_scalar_arr_sse4, .Lfunc_end2-arithmetic_scalar_arr_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
