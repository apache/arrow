	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_avx2
.LCPI0_0:
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI0_3:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI0_1:
	.long	0x00000000
	.long	0x80000000              # double -0
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI0_6:
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
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI0_2:
	.long	0x80000000                      # float -0
.LCPI0_4:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI0_5:
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
.LCPI0_7:
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
	.text
	.globl	arithmetic_avx2
	.p2align	4, 0x90
	.type	arithmetic_avx2,@function
arithmetic_avx2:                        # @arithmetic_avx2
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
	jne	.LBB0_1533
# %bb.5:
	cmp	edi, 6
	jg	.LBB0_117
# %bb.6:
	cmp	edi, 3
	jle	.LBB0_211
# %bb.7:
	cmp	edi, 4
	je	.LBB0_359
# %bb.8:
	cmp	edi, 5
	je	.LBB0_362
# %bb.9:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.10:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.11:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_12
# %bb.683:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1095
# %bb.684:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1095
.LBB0_12:
	xor	ecx, ecx
.LBB0_1301:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1303
.LBB0_1302:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1302
.LBB0_1303:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1304:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1304
	jmp	.LBB0_1533
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
	jne	.LBB0_1533
# %bb.17:
	cmp	edi, 6
	jg	.LBB0_124
# %bb.18:
	cmp	edi, 3
	jle	.LBB0_216
# %bb.19:
	cmp	edi, 4
	je	.LBB0_365
# %bb.20:
	cmp	edi, 5
	je	.LBB0_368
# %bb.21:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.22:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.23:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB0_686
# %bb.24:
	xor	ecx, ecx
	jmp	.LBB0_1104
.LBB0_25:
	test	sil, sil
	je	.LBB0_93
# %bb.26:
	cmp	sil, 1
	jne	.LBB0_1533
# %bb.27:
	cmp	edi, 6
	jg	.LBB0_131
# %bb.28:
	cmp	edi, 3
	jle	.LBB0_221
# %bb.29:
	cmp	edi, 4
	je	.LBB0_371
# %bb.30:
	cmp	edi, 5
	je	.LBB0_378
# %bb.31:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.32:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.33:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_688
# %bb.34:
	xor	esi, esi
.LBB0_35:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_37
.LBB0_36:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_36
.LBB0_37:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_39:
	cmp	sil, 7
	je	.LBB0_105
# %bb.40:
	cmp	sil, 8
	jne	.LBB0_1533
# %bb.41:
	cmp	edi, 6
	jg	.LBB0_142
# %bb.42:
	cmp	edi, 3
	jle	.LBB0_230
# %bb.43:
	cmp	edi, 4
	je	.LBB0_385
# %bb.44:
	cmp	edi, 5
	je	.LBB0_392
# %bb.45:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.46:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.47:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_693
# %bb.48:
	xor	esi, esi
.LBB0_49:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_51
.LBB0_50:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_50
.LBB0_51:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_53:
	cmp	edi, 6
	jg	.LBB0_153
# %bb.54:
	cmp	edi, 3
	jle	.LBB0_239
# %bb.55:
	cmp	edi, 4
	je	.LBB0_399
# %bb.56:
	cmp	edi, 5
	je	.LBB0_406
# %bb.57:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.58:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.59:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_698
# %bb.60:
	xor	esi, esi
.LBB0_61:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_63
.LBB0_62:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_62
.LBB0_63:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_65:
	cmp	edi, 6
	jg	.LBB0_164
# %bb.66:
	cmp	edi, 3
	jle	.LBB0_248
# %bb.67:
	cmp	edi, 4
	je	.LBB0_413
# %bb.68:
	cmp	edi, 5
	je	.LBB0_416
# %bb.69:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.70:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.71:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_72
# %bb.703:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1105
# %bb.704:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1105
.LBB0_72:
	xor	ecx, ecx
.LBB0_1309:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1311
.LBB0_1310:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1310
.LBB0_1311:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1312:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1312
	jmp	.LBB0_1533
.LBB0_73:
	cmp	edi, 6
	jg	.LBB0_171
# %bb.74:
	cmp	edi, 3
	jle	.LBB0_253
# %bb.75:
	cmp	edi, 4
	je	.LBB0_419
# %bb.76:
	cmp	edi, 5
	je	.LBB0_426
# %bb.77:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.78:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.79:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_706
# %bb.80:
	xor	esi, esi
.LBB0_81:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_83
.LBB0_82:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_82
.LBB0_83:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_85:
	cmp	edi, 6
	jg	.LBB0_182
# %bb.86:
	cmp	edi, 3
	jle	.LBB0_262
# %bb.87:
	cmp	edi, 4
	je	.LBB0_433
# %bb.88:
	cmp	edi, 5
	je	.LBB0_436
# %bb.89:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.90:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.91:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_92
# %bb.711:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1108
# %bb.712:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1108
.LBB0_92:
	xor	ecx, ecx
.LBB0_1317:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1319
.LBB0_1318:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1318
.LBB0_1319:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1320:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1320
	jmp	.LBB0_1533
.LBB0_93:
	cmp	edi, 6
	jg	.LBB0_189
# %bb.94:
	cmp	edi, 3
	jle	.LBB0_267
# %bb.95:
	cmp	edi, 4
	je	.LBB0_439
# %bb.96:
	cmp	edi, 5
	je	.LBB0_446
# %bb.97:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.98:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.99:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_714
# %bb.100:
	xor	esi, esi
.LBB0_101:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_103
.LBB0_102:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_102
.LBB0_103:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_105:
	cmp	edi, 6
	jg	.LBB0_200
# %bb.106:
	cmp	edi, 3
	jle	.LBB0_276
# %bb.107:
	cmp	edi, 4
	je	.LBB0_453
# %bb.108:
	cmp	edi, 5
	je	.LBB0_460
# %bb.109:
	cmp	edi, 6
	jne	.LBB0_1533
# %bb.110:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.111:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_719
# %bb.112:
	xor	esi, esi
.LBB0_113:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_115
.LBB0_114:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_114
.LBB0_115:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_117:
	cmp	edi, 8
	jle	.LBB0_285
# %bb.118:
	cmp	edi, 9
	je	.LBB0_467
# %bb.119:
	cmp	edi, 11
	je	.LBB0_470
# %bb.120:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.121:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.122:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_123
# %bb.724:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1111
# %bb.725:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1111
.LBB0_123:
	xor	ecx, ecx
.LBB0_1325:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1328
# %bb.1326:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1327:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1327
.LBB0_1328:
	cmp	rsi, 3
	jb	.LBB0_1533
# %bb.1329:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1330:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1330
	jmp	.LBB0_1533
.LBB0_124:
	cmp	edi, 8
	jle	.LBB0_290
# %bb.125:
	cmp	edi, 9
	je	.LBB0_473
# %bb.126:
	cmp	edi, 11
	je	.LBB0_476
# %bb.127:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.128:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.129:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_130
# %bb.727:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1114
# %bb.728:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1114
.LBB0_130:
	xor	ecx, ecx
.LBB0_1335:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1338
# %bb.1336:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1337:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1337
.LBB0_1338:
	cmp	rsi, 3
	jb	.LBB0_1533
# %bb.1339:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB0_1340:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1340
	jmp	.LBB0_1533
.LBB0_131:
	cmp	edi, 8
	jle	.LBB0_295
# %bb.132:
	cmp	edi, 9
	je	.LBB0_479
# %bb.133:
	cmp	edi, 11
	je	.LBB0_486
# %bb.134:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.135:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.136:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_730
# %bb.137:
	xor	esi, esi
.LBB0_138:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_140
.LBB0_139:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_139
.LBB0_140:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_141:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_141
	jmp	.LBB0_1533
.LBB0_142:
	cmp	edi, 8
	jle	.LBB0_304
# %bb.143:
	cmp	edi, 9
	je	.LBB0_493
# %bb.144:
	cmp	edi, 11
	je	.LBB0_500
# %bb.145:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.146:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.147:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_735
# %bb.148:
	xor	esi, esi
.LBB0_149:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_151
.LBB0_150:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_150
.LBB0_151:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_152:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_152
	jmp	.LBB0_1533
.LBB0_153:
	cmp	edi, 8
	jle	.LBB0_313
# %bb.154:
	cmp	edi, 9
	je	.LBB0_507
# %bb.155:
	cmp	edi, 11
	je	.LBB0_514
# %bb.156:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.157:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.158:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_740
# %bb.159:
	xor	esi, esi
.LBB0_160:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_162
.LBB0_161:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_161
.LBB0_162:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_163:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_163
	jmp	.LBB0_1533
.LBB0_164:
	cmp	edi, 8
	jle	.LBB0_322
# %bb.165:
	cmp	edi, 9
	je	.LBB0_521
# %bb.166:
	cmp	edi, 11
	je	.LBB0_524
# %bb.167:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.168:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.169:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_170
# %bb.745:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_1117
# %bb.746:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_1117
.LBB0_170:
	xor	ecx, ecx
.LBB0_1345:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1347
.LBB0_1346:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1346
.LBB0_1347:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1348:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1348
	jmp	.LBB0_1533
.LBB0_171:
	cmp	edi, 8
	jle	.LBB0_327
# %bb.172:
	cmp	edi, 9
	je	.LBB0_527
# %bb.173:
	cmp	edi, 11
	je	.LBB0_534
# %bb.174:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.175:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.176:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_748
# %bb.177:
	xor	esi, esi
.LBB0_178:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_180
.LBB0_179:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_179
.LBB0_180:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_181:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_181
	jmp	.LBB0_1533
.LBB0_182:
	cmp	edi, 8
	jle	.LBB0_336
# %bb.183:
	cmp	edi, 9
	je	.LBB0_541
# %bb.184:
	cmp	edi, 11
	je	.LBB0_544
# %bb.185:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.186:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.187:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_188
# %bb.753:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_1120
# %bb.754:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_1120
.LBB0_188:
	xor	ecx, ecx
.LBB0_1353:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1355
.LBB0_1354:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1354
.LBB0_1355:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1356:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1356
	jmp	.LBB0_1533
.LBB0_189:
	cmp	edi, 8
	jle	.LBB0_341
# %bb.190:
	cmp	edi, 9
	je	.LBB0_547
# %bb.191:
	cmp	edi, 11
	je	.LBB0_554
# %bb.192:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.193:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.194:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_756
# %bb.195:
	xor	esi, esi
.LBB0_196:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_198
.LBB0_197:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_197
.LBB0_198:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_199:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_199
	jmp	.LBB0_1533
.LBB0_200:
	cmp	edi, 8
	jle	.LBB0_350
# %bb.201:
	cmp	edi, 9
	je	.LBB0_561
# %bb.202:
	cmp	edi, 11
	je	.LBB0_568
# %bb.203:
	cmp	edi, 12
	jne	.LBB0_1533
# %bb.204:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.205:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_761
# %bb.206:
	xor	esi, esi
.LBB0_207:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_209
.LBB0_208:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_208
.LBB0_209:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_210:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_210
	jmp	.LBB0_1533
.LBB0_211:
	cmp	edi, 2
	je	.LBB0_575
# %bb.212:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.213:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.214:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_215
# %bb.766:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1123
# %bb.767:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1123
.LBB0_215:
	xor	ecx, ecx
.LBB0_1361:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1363
.LBB0_1362:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1362
.LBB0_1363:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1364:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1364
	jmp	.LBB0_1533
.LBB0_216:
	cmp	edi, 2
	je	.LBB0_578
# %bb.217:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.218:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.219:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_220
# %bb.769:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1126
# %bb.770:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1126
.LBB0_220:
	xor	ecx, ecx
.LBB0_1369:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1371
.LBB0_1370:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1370
.LBB0_1371:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1372:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1372
	jmp	.LBB0_1533
.LBB0_221:
	cmp	edi, 2
	je	.LBB0_581
# %bb.222:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.223:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.224:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_772
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
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_230:
	cmp	edi, 2
	je	.LBB0_588
# %bb.231:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.232:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.233:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_777
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
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_239:
	cmp	edi, 2
	je	.LBB0_595
# %bb.240:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.241:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.242:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_782
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
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_248:
	cmp	edi, 2
	je	.LBB0_602
# %bb.249:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.250:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.251:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_252
# %bb.786:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB0_1129
# %bb.787:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB0_1129
.LBB0_252:
	xor	ecx, ecx
.LBB0_1132:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1134
# %bb.1133:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_1134:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1135:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1135
	jmp	.LBB0_1533
.LBB0_253:
	cmp	edi, 2
	je	.LBB0_605
# %bb.254:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.255:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.256:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_789
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
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_262:
	cmp	edi, 2
	je	.LBB0_612
# %bb.263:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.264:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.265:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_266
# %bb.793:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB0_1136
# %bb.794:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB0_1136
.LBB0_266:
	xor	ecx, ecx
.LBB0_1139:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1141
# %bb.1140:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_1141:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1142:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1142
	jmp	.LBB0_1533
.LBB0_267:
	cmp	edi, 2
	je	.LBB0_615
# %bb.268:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.269:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.270:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_796
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
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_276:
	cmp	edi, 2
	je	.LBB0_622
# %bb.277:
	cmp	edi, 3
	jne	.LBB0_1533
# %bb.278:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.279:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_801
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
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_285:
	cmp	edi, 7
	je	.LBB0_629
# %bb.286:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.287:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.288:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_289
# %bb.806:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1143
# %bb.807:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1143
.LBB0_289:
	xor	ecx, ecx
.LBB0_1377:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1379
.LBB0_1378:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1378
.LBB0_1379:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1380:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1380
	jmp	.LBB0_1533
.LBB0_290:
	cmp	edi, 7
	je	.LBB0_632
# %bb.291:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.292:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.293:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB0_809
# %bb.294:
	xor	ecx, ecx
	jmp	.LBB0_1152
.LBB0_295:
	cmp	edi, 7
	je	.LBB0_635
# %bb.296:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.297:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.298:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_811
# %bb.299:
	xor	esi, esi
.LBB0_300:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_302
.LBB0_301:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_301
.LBB0_302:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_304:
	cmp	edi, 7
	je	.LBB0_642
# %bb.305:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.306:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.307:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_816
# %bb.308:
	xor	esi, esi
.LBB0_309:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_311
.LBB0_310:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_310
.LBB0_311:
	cmp	r9, 3
	jb	.LBB0_1533
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
	jmp	.LBB0_1533
.LBB0_313:
	cmp	edi, 7
	je	.LBB0_649
# %bb.314:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.315:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.316:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_821
# %bb.317:
	xor	esi, esi
.LBB0_318:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_320
.LBB0_319:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_319
.LBB0_320:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_321:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_321
	jmp	.LBB0_1533
.LBB0_322:
	cmp	edi, 7
	je	.LBB0_656
# %bb.323:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.324:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.325:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_326
# %bb.826:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1153
# %bb.827:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1153
.LBB0_326:
	xor	ecx, ecx
.LBB0_1385:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1387
.LBB0_1386:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1386
.LBB0_1387:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1388:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1388
	jmp	.LBB0_1533
.LBB0_327:
	cmp	edi, 7
	je	.LBB0_659
# %bb.328:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.329:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.330:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_829
# %bb.331:
	xor	esi, esi
.LBB0_332:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_334
.LBB0_333:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_333
.LBB0_334:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_335:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_335
	jmp	.LBB0_1533
.LBB0_336:
	cmp	edi, 7
	je	.LBB0_666
# %bb.337:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.338:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.339:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_340
# %bb.834:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1156
# %bb.835:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1156
.LBB0_340:
	xor	ecx, ecx
.LBB0_1393:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1395
.LBB0_1394:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1394
.LBB0_1395:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1396:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1396
	jmp	.LBB0_1533
.LBB0_341:
	cmp	edi, 7
	je	.LBB0_669
# %bb.342:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.343:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.344:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_837
# %bb.345:
	xor	esi, esi
.LBB0_346:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_348
.LBB0_347:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_347
.LBB0_348:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_349:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_349
	jmp	.LBB0_1533
.LBB0_350:
	cmp	edi, 7
	je	.LBB0_676
# %bb.351:
	cmp	edi, 8
	jne	.LBB0_1533
# %bb.352:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.353:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_842
# %bb.354:
	xor	esi, esi
.LBB0_355:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_357
.LBB0_356:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_356
.LBB0_357:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_358:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_358
	jmp	.LBB0_1533
.LBB0_359:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.360:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_361
# %bb.847:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1159
# %bb.848:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1159
.LBB0_361:
	xor	ecx, ecx
.LBB0_1401:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1403
.LBB0_1402:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1402
.LBB0_1403:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1404:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1404
	jmp	.LBB0_1533
.LBB0_362:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.363:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_364
# %bb.850:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1162
# %bb.851:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1162
.LBB0_364:
	xor	ecx, ecx
.LBB0_1409:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1411
.LBB0_1410:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1410
.LBB0_1411:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1412:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1412
	jmp	.LBB0_1533
.LBB0_365:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.366:
	mov	eax, r9d
	cmp	r9d, 64
	jae	.LBB0_853
# %bb.367:
	xor	ecx, ecx
	jmp	.LBB0_1171
.LBB0_368:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.369:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_370
# %bb.855:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1172
# %bb.856:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1172
.LBB0_370:
	xor	ecx, ecx
.LBB0_1417:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1419
.LBB0_1418:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1418
.LBB0_1419:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1420:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1420
	jmp	.LBB0_1533
.LBB0_371:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.372:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_858
# %bb.373:
	xor	esi, esi
.LBB0_374:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_376
.LBB0_375:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_375
.LBB0_376:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_377
	jmp	.LBB0_1533
.LBB0_378:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.379:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_863
# %bb.380:
	xor	esi, esi
.LBB0_381:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_383
.LBB0_382:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_382
.LBB0_383:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_384:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_384
	jmp	.LBB0_1533
.LBB0_385:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.386:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_868
# %bb.387:
	xor	esi, esi
.LBB0_388:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_390
.LBB0_389:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_389
.LBB0_390:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_391:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_391
	jmp	.LBB0_1533
.LBB0_392:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.393:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_873
# %bb.394:
	xor	esi, esi
.LBB0_395:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_397
.LBB0_396:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_396
.LBB0_397:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_398:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_398
	jmp	.LBB0_1533
.LBB0_399:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.400:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_878
# %bb.401:
	xor	esi, esi
.LBB0_402:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_404
.LBB0_403:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_403
.LBB0_404:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_405:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_405
	jmp	.LBB0_1533
.LBB0_406:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.407:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_883
# %bb.408:
	xor	esi, esi
.LBB0_409:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_411
.LBB0_410:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_410
.LBB0_411:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_412:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_412
	jmp	.LBB0_1533
.LBB0_413:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.414:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_415
# %bb.888:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_1175
# %bb.889:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_1175
.LBB0_415:
	xor	ecx, ecx
.LBB0_1283:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1285
.LBB0_1284:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1284
.LBB0_1285:
	cmp	rax, 3
	jb	.LBB0_1533
.LBB0_1286:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1286
	jmp	.LBB0_1533
.LBB0_416:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.417:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_418
# %bb.891:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1177
# %bb.892:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1177
.LBB0_418:
	xor	ecx, ecx
.LBB0_1425:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1427
# %bb.1426:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1427:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1428:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1428
	jmp	.LBB0_1533
.LBB0_419:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.420:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_894
# %bb.421:
	xor	esi, esi
.LBB0_422:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_424
.LBB0_423:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_423
.LBB0_424:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_425:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_425
	jmp	.LBB0_1533
.LBB0_426:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.427:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_899
# %bb.428:
	xor	esi, esi
.LBB0_429:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_431
.LBB0_430:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_430
.LBB0_431:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_432:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_432
	jmp	.LBB0_1533
.LBB0_433:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.434:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_435
# %bb.904:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_1180
# %bb.905:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_1180
.LBB0_435:
	xor	ecx, ecx
.LBB0_1293:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1295
.LBB0_1294:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1294
.LBB0_1295:
	cmp	rax, 3
	jb	.LBB0_1533
.LBB0_1296:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1296
	jmp	.LBB0_1533
.LBB0_436:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.437:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_438
# %bb.907:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_1182
# %bb.908:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1182
.LBB0_438:
	xor	ecx, ecx
.LBB0_1433:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1435
# %bb.1434:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1435:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1436:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1436
	jmp	.LBB0_1533
.LBB0_439:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.440:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_910
# %bb.441:
	xor	esi, esi
.LBB0_442:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_444
.LBB0_443:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_443
.LBB0_444:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_445:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_445
	jmp	.LBB0_1533
.LBB0_446:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.447:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_915
# %bb.448:
	xor	esi, esi
.LBB0_449:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_451
.LBB0_450:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_450
.LBB0_451:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_452:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_452
	jmp	.LBB0_1533
.LBB0_453:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.454:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_920
# %bb.455:
	xor	esi, esi
.LBB0_456:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_458
.LBB0_457:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_457
.LBB0_458:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_459:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_459
	jmp	.LBB0_1533
.LBB0_460:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.461:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_925
# %bb.462:
	xor	esi, esi
.LBB0_463:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_465
.LBB0_464:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_464
.LBB0_465:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_466:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_466
	jmp	.LBB0_1533
.LBB0_467:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.468:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_469
# %bb.930:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1185
# %bb.931:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1185
.LBB0_469:
	xor	ecx, ecx
.LBB0_1441:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1443
.LBB0_1442:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1442
.LBB0_1443:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1444:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1444
	jmp	.LBB0_1533
.LBB0_470:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.471:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_472
# %bb.933:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1188
# %bb.934:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1188
.LBB0_472:
	xor	ecx, ecx
.LBB0_1449:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1452
# %bb.1450:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1451:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1451
.LBB0_1452:
	cmp	rsi, 3
	jb	.LBB0_1533
# %bb.1453:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1454:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1454
	jmp	.LBB0_1533
.LBB0_473:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.474:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_475
# %bb.936:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1191
# %bb.937:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1191
.LBB0_475:
	xor	ecx, ecx
.LBB0_1459:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1461
.LBB0_1460:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1460
.LBB0_1461:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1462:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1462
	jmp	.LBB0_1533
.LBB0_476:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.477:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_478
# %bb.939:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1194
# %bb.940:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1194
.LBB0_478:
	xor	ecx, ecx
.LBB0_1467:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1470
# %bb.1468:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1469:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1469
.LBB0_1470:
	cmp	rsi, 3
	jb	.LBB0_1533
# %bb.1471:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1472:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB0_1472
	jmp	.LBB0_1533
.LBB0_479:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.480:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_942
# %bb.481:
	xor	esi, esi
.LBB0_482:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_484
.LBB0_483:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_483
.LBB0_484:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_485:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_485
	jmp	.LBB0_1533
.LBB0_486:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.487:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_947
# %bb.488:
	xor	esi, esi
.LBB0_489:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_491
.LBB0_490:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_490
.LBB0_491:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_492:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_492
	jmp	.LBB0_1533
.LBB0_493:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.494:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_952
# %bb.495:
	xor	esi, esi
.LBB0_496:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_498
.LBB0_497:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_497
.LBB0_498:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_499:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_499
	jmp	.LBB0_1533
.LBB0_500:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.501:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_957
# %bb.502:
	xor	esi, esi
.LBB0_503:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_505
.LBB0_504:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_504
.LBB0_505:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_506:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_506
	jmp	.LBB0_1533
.LBB0_507:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.508:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_962
# %bb.509:
	xor	esi, esi
.LBB0_510:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_512
.LBB0_511:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_511
.LBB0_512:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_513:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_513
	jmp	.LBB0_1533
.LBB0_514:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.515:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_967
# %bb.516:
	xor	esi, esi
.LBB0_517:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_519
.LBB0_518:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_518
.LBB0_519:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_520:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_520
	jmp	.LBB0_1533
.LBB0_521:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.522:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_523
# %bb.972:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1197
# %bb.973:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1197
.LBB0_523:
	xor	ecx, ecx
.LBB0_1200:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1202
# %bb.1201:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1202:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1203:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1203
	jmp	.LBB0_1533
.LBB0_524:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.525:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_526
# %bb.975:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_1204
# %bb.976:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_1204
.LBB0_526:
	xor	ecx, ecx
.LBB0_1477:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1480
# %bb.1478:
	mov	esi, 2147483647
.LBB0_1479:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1479
.LBB0_1480:
	cmp	r9, 3
	jb	.LBB0_1533
# %bb.1481:
	mov	esi, 2147483647
.LBB0_1482:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1482
	jmp	.LBB0_1533
.LBB0_527:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.528:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_978
# %bb.529:
	xor	esi, esi
.LBB0_530:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_532
.LBB0_531:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_531
.LBB0_532:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_533:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_533
	jmp	.LBB0_1533
.LBB0_534:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.535:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_983
# %bb.536:
	xor	esi, esi
.LBB0_537:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_539
.LBB0_538:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_538
.LBB0_539:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_540:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_540
	jmp	.LBB0_1533
.LBB0_541:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.542:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB0_543
# %bb.988:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_1207
# %bb.989:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1207
.LBB0_543:
	xor	ecx, ecx
.LBB0_1210:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1212
# %bb.1211:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1212:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1213:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1213
	jmp	.LBB0_1533
.LBB0_544:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.545:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_546
# %bb.991:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_1214
# %bb.992:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_1214
.LBB0_546:
	xor	ecx, ecx
.LBB0_1487:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1490
# %bb.1488:
	mov	esi, 2147483647
.LBB0_1489:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1489
.LBB0_1490:
	cmp	r9, 3
	jb	.LBB0_1533
# %bb.1491:
	mov	esi, 2147483647
.LBB0_1492:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1492
	jmp	.LBB0_1533
.LBB0_547:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.548:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_994
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
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_551
.LBB0_552:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_553:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_553
	jmp	.LBB0_1533
.LBB0_554:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.555:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_999
# %bb.556:
	xor	esi, esi
.LBB0_557:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_559
.LBB0_558:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_558
.LBB0_559:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_560:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_560
	jmp	.LBB0_1533
.LBB0_561:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.562:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_1004
# %bb.563:
	xor	esi, esi
.LBB0_564:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_566
.LBB0_565:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_565
.LBB0_566:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_567:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_567
	jmp	.LBB0_1533
.LBB0_568:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.569:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1009
# %bb.570:
	xor	esi, esi
.LBB0_571:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_573
.LBB0_572:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_572
.LBB0_573:
	cmp	rdi, 3
	jb	.LBB0_1533
.LBB0_574:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_574
	jmp	.LBB0_1533
.LBB0_575:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.576:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_577
# %bb.1014:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1217
# %bb.1015:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1217
.LBB0_577:
	xor	ecx, ecx
.LBB0_1497:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1499
.LBB0_1498:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1498
.LBB0_1499:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1500:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1500
	jmp	.LBB0_1533
.LBB0_578:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.579:
	mov	eax, r9d
	cmp	r9d, 128
	jae	.LBB0_1017
# %bb.580:
	xor	ecx, ecx
	jmp	.LBB0_1226
.LBB0_581:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.582:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_1019
# %bb.583:
	xor	esi, esi
.LBB0_584:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_586
.LBB0_585:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_585
.LBB0_586:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_587:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_587
	jmp	.LBB0_1533
.LBB0_588:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.589:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_1024
# %bb.590:
	xor	esi, esi
.LBB0_591:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_593
.LBB0_592:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_592
.LBB0_593:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_594:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_594
	jmp	.LBB0_1533
.LBB0_595:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.596:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1029
# %bb.597:
	xor	edi, edi
.LBB0_598:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_600
.LBB0_599:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_599
.LBB0_600:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_601:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_601
	jmp	.LBB0_1533
.LBB0_602:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.603:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_604
# %bb.1033:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1227
# %bb.1034:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1227
.LBB0_604:
	xor	ecx, ecx
.LBB0_1505:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1507
.LBB0_1506:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1506
.LBB0_1507:
	cmp	rsi, 3
	jb	.LBB0_1533
.LBB0_1508:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1508
	jmp	.LBB0_1533
.LBB0_605:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.606:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1036
# %bb.607:
	xor	edi, edi
.LBB0_608:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_610
.LBB0_609:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_609
.LBB0_610:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_611:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_611
	jmp	.LBB0_1533
.LBB0_612:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.613:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB0_614
# %bb.1040:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_1230
# %bb.1041:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_1230
.LBB0_614:
	xor	ecx, ecx
.LBB0_1513:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1515
.LBB0_1514:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1514
.LBB0_1515:
	cmp	rsi, 3
	jb	.LBB0_1533
.LBB0_1516:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1516
	jmp	.LBB0_1533
.LBB0_615:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.616:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_1043
# %bb.617:
	xor	esi, esi
.LBB0_618:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_620
.LBB0_619:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_619
.LBB0_620:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_621:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_621
	jmp	.LBB0_1533
.LBB0_622:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.623:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_1048
# %bb.624:
	xor	esi, esi
.LBB0_625:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_627
.LBB0_626:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_626
.LBB0_627:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_628:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_628
	jmp	.LBB0_1533
.LBB0_629:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.630:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_631
# %bb.1053:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1233
# %bb.1054:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1233
.LBB0_631:
	xor	ecx, ecx
.LBB0_1521:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1523
.LBB0_1522:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1522
.LBB0_1523:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1524:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1524
	jmp	.LBB0_1533
.LBB0_632:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.633:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_634
# %bb.1056:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1236
# %bb.1057:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1236
.LBB0_634:
	xor	ecx, ecx
.LBB0_1529:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB0_1531
.LBB0_1530:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1530
.LBB0_1531:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_1532:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1532
	jmp	.LBB0_1533
.LBB0_635:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.636:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1059
# %bb.637:
	xor	esi, esi
.LBB0_638:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_640
.LBB0_639:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_639
.LBB0_640:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_641:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_641
	jmp	.LBB0_1533
.LBB0_642:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.643:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1064
# %bb.644:
	xor	esi, esi
.LBB0_645:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_647
.LBB0_646:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_646
.LBB0_647:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_648:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_648
	jmp	.LBB0_1533
.LBB0_649:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.650:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1069
# %bb.651:
	xor	esi, esi
.LBB0_652:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_654
.LBB0_653:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_653
.LBB0_654:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_655:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_655
	jmp	.LBB0_1533
.LBB0_656:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.657:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_658
# %bb.1074:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1239
# %bb.1075:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1239
.LBB0_658:
	xor	ecx, ecx
.LBB0_1242:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1244
# %bb.1243:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1244:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1245:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1245
	jmp	.LBB0_1533
.LBB0_659:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.660:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1077
# %bb.661:
	xor	esi, esi
.LBB0_662:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_664
.LBB0_663:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_663
.LBB0_664:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_665:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_665
	jmp	.LBB0_1533
.LBB0_666:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.667:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB0_668
# %bb.1082:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_1246
# %bb.1083:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_1246
.LBB0_668:
	xor	ecx, ecx
.LBB0_1249:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1251
# %bb.1250:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1251:
	add	rsi, rax
	je	.LBB0_1533
.LBB0_1252:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1252
	jmp	.LBB0_1533
.LBB0_669:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.670:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1085
# %bb.671:
	xor	esi, esi
.LBB0_672:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_674
.LBB0_673:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_673
.LBB0_674:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_675:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_675
	jmp	.LBB0_1533
.LBB0_676:
	test	r9d, r9d
	jle	.LBB0_1533
# %bb.677:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_1090
# %bb.678:
	xor	esi, esi
.LBB0_679:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_681
.LBB0_680:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_680
.LBB0_681:
	cmp	r9, 3
	jb	.LBB0_1533
.LBB0_682:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_682
	jmp	.LBB0_1533
.LBB0_686:
	mov	ecx, eax
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 96
	jae	.LBB0_1098
# %bb.687:
	xor	edi, edi
	jmp	.LBB0_1100
.LBB0_688:
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
# %bb.689:
	and	al, dil
	jne	.LBB0_35
# %bb.690:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_691:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_691
# %bb.692:
	cmp	rsi, r10
	jne	.LBB0_35
	jmp	.LBB0_1533
.LBB0_693:
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
# %bb.694:
	and	al, dil
	jne	.LBB0_49
# %bb.695:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_696:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_696
# %bb.697:
	cmp	rsi, r10
	jne	.LBB0_49
	jmp	.LBB0_1533
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
	jne	.LBB0_61
# %bb.699:
	and	al, dil
	jne	.LBB0_61
# %bb.700:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_701:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_701
# %bb.702:
	cmp	rsi, r10
	jne	.LBB0_61
	jmp	.LBB0_1533
.LBB0_706:
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
# %bb.707:
	and	al, dil
	jne	.LBB0_81
# %bb.708:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_709:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_709
# %bb.710:
	cmp	rsi, r10
	jne	.LBB0_81
	jmp	.LBB0_1533
.LBB0_714:
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
# %bb.715:
	and	al, dil
	jne	.LBB0_101
# %bb.716:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_717:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_717
# %bb.718:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_101
.LBB0_719:
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
# %bb.720:
	and	al, dil
	jne	.LBB0_113
# %bb.721:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_722:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_722
# %bb.723:
	cmp	rsi, r10
	jne	.LBB0_113
	jmp	.LBB0_1533
.LBB0_730:
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
# %bb.731:
	and	al, dil
	jne	.LBB0_138
# %bb.732:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_733:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_733
# %bb.734:
	cmp	rsi, r10
	jne	.LBB0_138
	jmp	.LBB0_1533
.LBB0_735:
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
# %bb.736:
	and	al, dil
	jne	.LBB0_149
# %bb.737:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_738:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_738
# %bb.739:
	cmp	rsi, r10
	jne	.LBB0_149
	jmp	.LBB0_1533
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
	jne	.LBB0_160
# %bb.741:
	and	al, dil
	jne	.LBB0_160
# %bb.742:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_743:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_743
# %bb.744:
	cmp	rsi, r10
	jne	.LBB0_160
	jmp	.LBB0_1533
.LBB0_748:
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
# %bb.749:
	and	al, dil
	jne	.LBB0_178
# %bb.750:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_751:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_751
# %bb.752:
	cmp	rsi, r10
	jne	.LBB0_178
	jmp	.LBB0_1533
.LBB0_756:
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
# %bb.757:
	and	al, dil
	jne	.LBB0_196
# %bb.758:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_759:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_759
# %bb.760:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_196
.LBB0_761:
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
# %bb.762:
	and	al, dil
	jne	.LBB0_207
# %bb.763:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_764
# %bb.765:
	cmp	rsi, r10
	jne	.LBB0_207
	jmp	.LBB0_1533
.LBB0_772:
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
# %bb.773:
	and	al, dil
	jne	.LBB0_226
# %bb.774:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_775:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_775
# %bb.776:
	cmp	rsi, r10
	jne	.LBB0_226
	jmp	.LBB0_1533
.LBB0_777:
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
# %bb.778:
	and	al, dil
	jne	.LBB0_235
# %bb.779:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_780:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_780
# %bb.781:
	cmp	rsi, r10
	jne	.LBB0_235
	jmp	.LBB0_1533
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
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_244
# %bb.783:
	and	al, sil
	jne	.LBB0_244
# %bb.784:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1253
# %bb.785:
	xor	esi, esi
	jmp	.LBB0_1255
.LBB0_789:
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
# %bb.790:
	and	al, sil
	jne	.LBB0_258
# %bb.791:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1259
# %bb.792:
	xor	esi, esi
	jmp	.LBB0_1261
.LBB0_796:
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
# %bb.797:
	and	al, dil
	jne	.LBB0_272
# %bb.798:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_799:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_799
# %bb.800:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_272
.LBB0_801:
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
# %bb.802:
	and	al, dil
	jne	.LBB0_281
# %bb.803:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_804
# %bb.805:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_281
.LBB0_809:
	mov	ecx, eax
	and	ecx, -16
	lea	rdi, [rcx - 16]
	mov	rsi, rdi
	shr	rsi, 4
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 48
	jae	.LBB0_1146
# %bb.810:
	xor	edi, edi
	jmp	.LBB0_1148
.LBB0_811:
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
# %bb.812:
	and	al, dil
	jne	.LBB0_300
# %bb.813:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_814:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_814
# %bb.815:
	cmp	rsi, r10
	jne	.LBB0_300
	jmp	.LBB0_1533
.LBB0_816:
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
# %bb.817:
	and	al, dil
	jne	.LBB0_309
# %bb.818:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_819:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_819
# %bb.820:
	cmp	rsi, r10
	jne	.LBB0_309
	jmp	.LBB0_1533
.LBB0_821:
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
	jne	.LBB0_318
# %bb.822:
	and	al, dil
	jne	.LBB0_318
# %bb.823:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_824:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_824
# %bb.825:
	cmp	rsi, r10
	jne	.LBB0_318
	jmp	.LBB0_1533
.LBB0_829:
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
	jne	.LBB0_332
# %bb.830:
	and	al, dil
	jne	.LBB0_332
# %bb.831:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_832:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_832
# %bb.833:
	cmp	rsi, r10
	jne	.LBB0_332
	jmp	.LBB0_1533
.LBB0_837:
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
	jne	.LBB0_346
# %bb.838:
	and	al, dil
	jne	.LBB0_346
# %bb.839:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_840:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_840
# %bb.841:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_346
.LBB0_842:
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
	jne	.LBB0_355
# %bb.843:
	and	al, dil
	jne	.LBB0_355
# %bb.844:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_845:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_845
# %bb.846:
	cmp	rsi, r10
	jne	.LBB0_355
	jmp	.LBB0_1533
.LBB0_853:
	mov	ecx, eax
	and	ecx, -64
	lea	rdi, [rcx - 64]
	mov	rsi, rdi
	shr	rsi, 6
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 192
	jae	.LBB0_1165
# %bb.854:
	xor	edi, edi
	jmp	.LBB0_1167
.LBB0_858:
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
	jne	.LBB0_374
# %bb.859:
	and	al, dil
	jne	.LBB0_374
# %bb.860:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_861:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_861
# %bb.862:
	cmp	rsi, r10
	jne	.LBB0_374
	jmp	.LBB0_1533
.LBB0_863:
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
	jne	.LBB0_381
# %bb.864:
	and	al, dil
	jne	.LBB0_381
# %bb.865:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_866:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_866
# %bb.867:
	cmp	rsi, r10
	jne	.LBB0_381
	jmp	.LBB0_1533
.LBB0_868:
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
	jne	.LBB0_388
# %bb.869:
	and	al, dil
	jne	.LBB0_388
# %bb.870:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_871:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_871
# %bb.872:
	cmp	rsi, r10
	jne	.LBB0_388
	jmp	.LBB0_1533
.LBB0_873:
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
	jne	.LBB0_395
# %bb.874:
	and	al, dil
	jne	.LBB0_395
# %bb.875:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_876:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_876
# %bb.877:
	cmp	rsi, r10
	jne	.LBB0_395
	jmp	.LBB0_1533
.LBB0_878:
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
	jne	.LBB0_402
# %bb.879:
	and	al, dil
	jne	.LBB0_402
# %bb.880:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_881:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_881
# %bb.882:
	cmp	rsi, r10
	jne	.LBB0_402
	jmp	.LBB0_1533
.LBB0_883:
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
	jne	.LBB0_409
# %bb.884:
	and	al, dil
	jne	.LBB0_409
# %bb.885:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_886:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_886
# %bb.887:
	cmp	rsi, r10
	jne	.LBB0_409
	jmp	.LBB0_1533
.LBB0_894:
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
	jne	.LBB0_422
# %bb.895:
	and	al, dil
	jne	.LBB0_422
# %bb.896:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_897:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_897
# %bb.898:
	cmp	rsi, r10
	jne	.LBB0_422
	jmp	.LBB0_1533
.LBB0_899:
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
	jne	.LBB0_429
# %bb.900:
	and	al, dil
	jne	.LBB0_429
# %bb.901:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_902:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_902
# %bb.903:
	cmp	rsi, r10
	jne	.LBB0_429
	jmp	.LBB0_1533
.LBB0_910:
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
	jne	.LBB0_442
# %bb.911:
	and	al, dil
	jne	.LBB0_442
# %bb.912:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_913:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_913
# %bb.914:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_442
.LBB0_915:
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
	jne	.LBB0_449
# %bb.916:
	and	al, dil
	jne	.LBB0_449
# %bb.917:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_918:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_918
# %bb.919:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_449
.LBB0_920:
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
	jne	.LBB0_456
# %bb.921:
	and	al, dil
	jne	.LBB0_456
# %bb.922:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_923:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_923
# %bb.924:
	cmp	rsi, r10
	jne	.LBB0_456
	jmp	.LBB0_1533
.LBB0_925:
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
	jne	.LBB0_463
# %bb.926:
	and	al, dil
	jne	.LBB0_463
# %bb.927:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_928:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_928
# %bb.929:
	cmp	rsi, r10
	jne	.LBB0_463
	jmp	.LBB0_1533
.LBB0_942:
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
	jne	.LBB0_482
# %bb.943:
	and	al, dil
	jne	.LBB0_482
# %bb.944:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_945:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_945
# %bb.946:
	cmp	rsi, r10
	jne	.LBB0_482
	jmp	.LBB0_1533
.LBB0_947:
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
	jne	.LBB0_489
# %bb.948:
	and	al, dil
	jne	.LBB0_489
# %bb.949:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_950:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_950
# %bb.951:
	cmp	rsi, r10
	jne	.LBB0_489
	jmp	.LBB0_1533
.LBB0_952:
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
	jne	.LBB0_496
# %bb.953:
	and	al, dil
	jne	.LBB0_496
# %bb.954:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_955:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_955
# %bb.956:
	cmp	rsi, r10
	jne	.LBB0_496
	jmp	.LBB0_1533
.LBB0_957:
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
	jne	.LBB0_503
# %bb.958:
	and	al, dil
	jne	.LBB0_503
# %bb.959:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_960:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_960
# %bb.961:
	cmp	rsi, r10
	jne	.LBB0_503
	jmp	.LBB0_1533
.LBB0_962:
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
	jne	.LBB0_510
# %bb.963:
	and	al, dil
	jne	.LBB0_510
# %bb.964:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_965:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_965
# %bb.966:
	cmp	rsi, r10
	jne	.LBB0_510
	jmp	.LBB0_1533
.LBB0_967:
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
	jne	.LBB0_517
# %bb.968:
	and	al, dil
	jne	.LBB0_517
# %bb.969:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_970:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_970
# %bb.971:
	cmp	rsi, r10
	jne	.LBB0_517
	jmp	.LBB0_1533
.LBB0_978:
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
	jne	.LBB0_530
# %bb.979:
	and	al, dil
	jne	.LBB0_530
# %bb.980:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_981:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_981
# %bb.982:
	cmp	rsi, r10
	jne	.LBB0_530
	jmp	.LBB0_1533
.LBB0_983:
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
	jne	.LBB0_537
# %bb.984:
	and	al, dil
	jne	.LBB0_537
# %bb.985:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_986:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_986
# %bb.987:
	cmp	rsi, r10
	jne	.LBB0_537
	jmp	.LBB0_1533
.LBB0_994:
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
	jne	.LBB0_550
# %bb.995:
	and	al, dil
	jne	.LBB0_550
# %bb.996:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_997:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_997
# %bb.998:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_550
.LBB0_999:
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
# %bb.1000:
	and	al, dil
	jne	.LBB0_557
# %bb.1001:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1002:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1002
# %bb.1003:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_557
.LBB0_1004:
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
	jne	.LBB0_564
# %bb.1005:
	and	al, dil
	jne	.LBB0_564
# %bb.1006:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_1007:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1007
# %bb.1008:
	cmp	rsi, r10
	jne	.LBB0_564
	jmp	.LBB0_1533
.LBB0_1009:
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
	jne	.LBB0_571
# %bb.1010:
	and	al, dil
	jne	.LBB0_571
# %bb.1011:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1012
# %bb.1013:
	cmp	rsi, r10
	jne	.LBB0_571
	jmp	.LBB0_1533
.LBB0_1017:
	mov	ecx, eax
	and	ecx, -128
	lea	rdi, [rcx - 128]
	mov	rsi, rdi
	shr	rsi, 7
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 384
	jae	.LBB0_1220
# %bb.1018:
	xor	edi, edi
	jmp	.LBB0_1222
.LBB0_1019:
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
	jne	.LBB0_584
# %bb.1020:
	and	al, dil
	jne	.LBB0_584
# %bb.1021:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_1022:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1022
# %bb.1023:
	cmp	rsi, r10
	jne	.LBB0_584
	jmp	.LBB0_1533
.LBB0_1024:
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
	jne	.LBB0_591
# %bb.1025:
	and	al, dil
	jne	.LBB0_591
# %bb.1026:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_1027:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1027
# %bb.1028:
	cmp	rsi, r10
	jne	.LBB0_591
	jmp	.LBB0_1533
.LBB0_1029:
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
	jne	.LBB0_598
# %bb.1030:
	and	al, sil
	jne	.LBB0_598
# %bb.1031:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1265
# %bb.1032:
	xor	esi, esi
	jmp	.LBB0_1267
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
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_608
# %bb.1037:
	and	al, sil
	jne	.LBB0_608
# %bb.1038:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_1271
# %bb.1039:
	xor	esi, esi
	jmp	.LBB0_1273
.LBB0_1043:
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
	jne	.LBB0_618
# %bb.1044:
	and	al, dil
	jne	.LBB0_618
# %bb.1045:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_1046:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1046
# %bb.1047:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_618
.LBB0_1048:
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
	jne	.LBB0_625
# %bb.1049:
	and	al, dil
	jne	.LBB0_625
# %bb.1050:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_1051:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1051
# %bb.1052:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_625
.LBB0_1059:
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
	jne	.LBB0_638
# %bb.1060:
	and	al, dil
	jne	.LBB0_638
# %bb.1061:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1062:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1062
# %bb.1063:
	cmp	rsi, r10
	jne	.LBB0_638
	jmp	.LBB0_1533
.LBB0_1064:
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
	jne	.LBB0_645
# %bb.1065:
	and	al, dil
	jne	.LBB0_645
# %bb.1066:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1067:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1067
# %bb.1068:
	cmp	rsi, r10
	jne	.LBB0_645
	jmp	.LBB0_1533
.LBB0_1069:
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
	jne	.LBB0_652
# %bb.1070:
	and	al, dil
	jne	.LBB0_652
# %bb.1071:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1072:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1072
# %bb.1073:
	cmp	rsi, r10
	jne	.LBB0_652
	jmp	.LBB0_1533
.LBB0_1077:
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
	jne	.LBB0_662
# %bb.1078:
	and	al, dil
	jne	.LBB0_662
# %bb.1079:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1080:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1080
# %bb.1081:
	cmp	rsi, r10
	jne	.LBB0_662
	jmp	.LBB0_1533
.LBB0_1085:
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
	jne	.LBB0_672
# %bb.1086:
	and	al, dil
	jne	.LBB0_672
# %bb.1087:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1088:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1088
# %bb.1089:
	cmp	rsi, r10
	je	.LBB0_1533
	jmp	.LBB0_672
.LBB0_1090:
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
	jne	.LBB0_679
# %bb.1091:
	and	al, dil
	jne	.LBB0_679
# %bb.1092:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_1093:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1093
# %bb.1094:
	cmp	rsi, r10
	jne	.LBB0_679
	jmp	.LBB0_1533
.LBB0_1095:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1297
# %bb.1096:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1097:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1097
	jmp	.LBB0_1298
.LBB0_1105:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1305
# %bb.1106:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1107:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1107
	jmp	.LBB0_1306
.LBB0_1108:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1313
# %bb.1109:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1110:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1110
	jmp	.LBB0_1314
.LBB0_1111:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1321
# %bb.1112:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1113:                             # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1113
	jmp	.LBB0_1322
.LBB0_1114:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1331
# %bb.1115:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1116:                             # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1116
	jmp	.LBB0_1332
.LBB0_1117:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1341
# %bb.1118:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB0_1119:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1119
	jmp	.LBB0_1342
.LBB0_1120:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1349
# %bb.1121:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB0_1122:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1122
	jmp	.LBB0_1350
.LBB0_1123:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1357
# %bb.1124:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1125:                             # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB0_1125
	jmp	.LBB0_1358
.LBB0_1126:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1365
# %bb.1127:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1128:                             # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB0_1128
	jmp	.LBB0_1366
.LBB0_1129:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_6] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_1130:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1130
# %bb.1131:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1132
.LBB0_1136:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_6] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_1137:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1137
# %bb.1138:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1139
.LBB0_1143:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1373
# %bb.1144:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1145:                             # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1145
	jmp	.LBB0_1374
.LBB0_1153:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1381
# %bb.1154:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1155:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1155
	jmp	.LBB0_1382
.LBB0_1156:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1389
# %bb.1157:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1158:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1158
	jmp	.LBB0_1390
.LBB0_1159:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1397
# %bb.1160:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1161:                             # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1161
	jmp	.LBB0_1398
.LBB0_1162:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1405
# %bb.1163:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1164:                             # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1164
	jmp	.LBB0_1406
.LBB0_1172:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1413
# %bb.1173:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1174:                             # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1174
	jmp	.LBB0_1414
.LBB0_1175:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB0_1277
# %bb.1176:
	xor	eax, eax
	jmp	.LBB0_1279
.LBB0_1177:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1421
# %bb.1178:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_5] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_1179:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1179
	jmp	.LBB0_1422
.LBB0_1180:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB0_1287
# %bb.1181:
	xor	eax, eax
	jmp	.LBB0_1289
.LBB0_1182:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1429
# %bb.1183:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_5] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_1184:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1184
	jmp	.LBB0_1430
.LBB0_1185:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1437
# %bb.1186:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1187:                             # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1187
	jmp	.LBB0_1438
.LBB0_1188:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1445
# %bb.1189:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1190:                             # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1190
	jmp	.LBB0_1446
.LBB0_1191:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1455
# %bb.1192:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1193:                             # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB0_1193
	jmp	.LBB0_1456
.LBB0_1194:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1463
# %bb.1195:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB0_1196:                             # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1196
	jmp	.LBB0_1464
.LBB0_1197:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1198:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1198
# %bb.1199:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1200
.LBB0_1204:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1473
# %bb.1205:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB0_1206:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1206
	jmp	.LBB0_1474
.LBB0_1207:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1208:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1208
# %bb.1209:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1210
.LBB0_1214:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1483
# %bb.1215:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB0_1216:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1216
	jmp	.LBB0_1484
.LBB0_1217:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1493
# %bb.1218:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1219:                             # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB0_1219
	jmp	.LBB0_1494
.LBB0_1227:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1501
# %bb.1228:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1229:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1229
	jmp	.LBB0_1502
.LBB0_1230:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1509
# %bb.1231:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_1232:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1232
	jmp	.LBB0_1510
.LBB0_1233:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1517
# %bb.1234:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1235:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1235
	jmp	.LBB0_1518
.LBB0_1236:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1525
# %bb.1237:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1238:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB0_1238
	jmp	.LBB0_1526
.LBB0_1239:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB0_1240:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1240
# %bb.1241:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1242
.LBB0_1246:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB0_1247:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1247
# %bb.1248:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1249
.LBB0_1098:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1099:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 480], ymm0
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB0_1099
.LBB0_1100:
	test	rdx, rdx
	je	.LBB0_1103
# %bb.1101:
	lea	rsi, [r8 + 4*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB0_1102:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB0_1102
.LBB0_1103:
	cmp	rcx, rax
	je	.LBB0_1533
.LBB0_1104:                             # =>This Inner Loop Header: Depth=1
	mov	dword ptr [r8 + 4*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1104
	jmp	.LBB0_1533
.LBB0_1146:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1147:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 480], ymm0
	add	rdi, 64
	add	rsi, 4
	jne	.LBB0_1147
.LBB0_1148:
	test	rdx, rdx
	je	.LBB0_1151
# %bb.1149:
	lea	rsi, [r8 + 8*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB0_1150:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB0_1150
.LBB0_1151:
	cmp	rcx, rax
	je	.LBB0_1533
.LBB0_1152:                             # =>This Inner Loop Header: Depth=1
	mov	qword ptr [r8 + 8*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1152
	jmp	.LBB0_1533
.LBB0_1165:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1166:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 480], ymm0
	add	rdi, 256
	add	rsi, 4
	jne	.LBB0_1166
.LBB0_1167:
	test	rdx, rdx
	je	.LBB0_1170
# %bb.1168:
	lea	rsi, [r8 + 2*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB0_1169:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB0_1169
.LBB0_1170:
	cmp	rcx, rax
	je	.LBB0_1533
.LBB0_1171:                             # =>This Inner Loop Header: Depth=1
	mov	word ptr [r8 + 2*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1171
	jmp	.LBB0_1533
.LBB0_1220:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_1221:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 480], ymm0
	add	rdi, 512
	add	rsi, 4
	jne	.LBB0_1221
.LBB0_1222:
	test	rdx, rdx
	je	.LBB0_1225
# %bb.1223:
	lea	rsi, [rdi + r8]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB0_1224:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB0_1224
.LBB0_1225:
	cmp	rcx, rax
	je	.LBB0_1533
.LBB0_1226:                             # =>This Inner Loop Header: Depth=1
	mov	byte ptr [r8 + rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB0_1226
.LBB0_1533:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB0_1253:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1254:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1254
.LBB0_1255:
	test	r9, r9
	je	.LBB0_1258
# %bb.1256:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1257:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1257
.LBB0_1258:
	cmp	rdi, r10
	jne	.LBB0_244
	jmp	.LBB0_1533
.LBB0_1259:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1260:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1260
.LBB0_1261:
	test	r9, r9
	je	.LBB0_1264
# %bb.1262:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1263:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1263
.LBB0_1264:
	cmp	rdi, r10
	jne	.LBB0_258
	jmp	.LBB0_1533
.LBB0_1265:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1266:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1266
.LBB0_1267:
	test	r9, r9
	je	.LBB0_1270
# %bb.1268:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1269:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1269
.LBB0_1270:
	cmp	rdi, r10
	jne	.LBB0_598
	jmp	.LBB0_1533
.LBB0_1271:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1272:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1272
.LBB0_1273:
	test	r9, r9
	je	.LBB0_1276
# %bb.1274:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_7] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_1275:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1275
.LBB0_1276:
	cmp	rdi, r10
	jne	.LBB0_608
	jmp	.LBB0_1533
.LBB0_1277:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1278:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1278
.LBB0_1279:
	test	rsi, rsi
	je	.LBB0_1282
# %bb.1280:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB0_1281:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB0_1281
.LBB0_1282:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1283
.LBB0_1287:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1288:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1288
.LBB0_1289:
	test	rsi, rsi
	je	.LBB0_1292
# %bb.1290:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB0_1291:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB0_1291
.LBB0_1292:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1293
.LBB0_1297:
	xor	edi, edi
.LBB0_1298:
	test	r9b, 1
	je	.LBB0_1300
# %bb.1299:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1300:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1301
.LBB0_1305:
	xor	edi, edi
.LBB0_1306:
	test	r9b, 1
	je	.LBB0_1308
# %bb.1307:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB0_1308:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1309
.LBB0_1313:
	xor	edi, edi
.LBB0_1314:
	test	r9b, 1
	je	.LBB0_1316
# %bb.1315:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB0_1316:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1317
.LBB0_1321:
	xor	edi, edi
.LBB0_1322:
	test	r9b, 1
	je	.LBB0_1324
# %bb.1323:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1324:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1325
.LBB0_1331:
	xor	edi, edi
.LBB0_1332:
	test	r9b, 1
	je	.LBB0_1334
# %bb.1333:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1334:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1335
.LBB0_1341:
	xor	edi, edi
.LBB0_1342:
	test	r9b, 1
	je	.LBB0_1344
# %bb.1343:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1344:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1345
.LBB0_1349:
	xor	edi, edi
.LBB0_1350:
	test	r9b, 1
	je	.LBB0_1352
# %bb.1351:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1352:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1353
.LBB0_1357:
	xor	edi, edi
.LBB0_1358:
	test	r9b, 1
	je	.LBB0_1360
# %bb.1359:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB0_1360:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1361
.LBB0_1365:
	xor	edi, edi
.LBB0_1366:
	test	r9b, 1
	je	.LBB0_1368
# %bb.1367:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB0_1368:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1369
.LBB0_1373:
	xor	edi, edi
.LBB0_1374:
	test	r9b, 1
	je	.LBB0_1376
# %bb.1375:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1376:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1377
.LBB0_1381:
	xor	edi, edi
.LBB0_1382:
	test	r9b, 1
	je	.LBB0_1384
# %bb.1383:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB0_1384:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1385
.LBB0_1389:
	xor	edi, edi
.LBB0_1390:
	test	r9b, 1
	je	.LBB0_1392
# %bb.1391:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB0_1392:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1393
.LBB0_1397:
	xor	edi, edi
.LBB0_1398:
	test	r9b, 1
	je	.LBB0_1400
# %bb.1399:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB0_1400:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1401
.LBB0_1405:
	xor	edi, edi
.LBB0_1406:
	test	r9b, 1
	je	.LBB0_1408
# %bb.1407:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB0_1408:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1409
.LBB0_1413:
	xor	edi, edi
.LBB0_1414:
	test	r9b, 1
	je	.LBB0_1416
# %bb.1415:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB0_1416:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1417
.LBB0_1421:
	xor	esi, esi
.LBB0_1422:
	test	r9b, 1
	je	.LBB0_1424
# %bb.1423:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_5] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB0_1424:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1425
.LBB0_1429:
	xor	esi, esi
.LBB0_1430:
	test	r9b, 1
	je	.LBB0_1432
# %bb.1431:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_5] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB0_1432:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1433
.LBB0_1437:
	xor	edi, edi
.LBB0_1438:
	test	r9b, 1
	je	.LBB0_1440
# %bb.1439:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1440:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1441
.LBB0_1445:
	xor	edi, edi
.LBB0_1446:
	test	r9b, 1
	je	.LBB0_1448
# %bb.1447:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1448:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1449
.LBB0_1455:
	xor	edi, edi
.LBB0_1456:
	test	r9b, 1
	je	.LBB0_1458
# %bb.1457:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB0_1458:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1459
.LBB0_1463:
	xor	edi, edi
.LBB0_1464:
	test	r9b, 1
	je	.LBB0_1466
# %bb.1465:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1466:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1467
.LBB0_1473:
	xor	edi, edi
.LBB0_1474:
	test	r9b, 1
	je	.LBB0_1476
# %bb.1475:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1476:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1477
.LBB0_1483:
	xor	edi, edi
.LBB0_1484:
	test	r9b, 1
	je	.LBB0_1486
# %bb.1485:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1486:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1487
.LBB0_1493:
	xor	edi, edi
.LBB0_1494:
	test	r9b, 1
	je	.LBB0_1496
# %bb.1495:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB0_1496:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1497
.LBB0_1501:
	xor	edi, edi
.LBB0_1502:
	test	r9b, 1
	je	.LBB0_1504
# %bb.1503:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB0_1504:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1505
.LBB0_1509:
	xor	edi, edi
.LBB0_1510:
	test	r9b, 1
	je	.LBB0_1512
# %bb.1511:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB0_1512:
	cmp	rcx, r10
	je	.LBB0_1533
	jmp	.LBB0_1513
.LBB0_1517:
	xor	edi, edi
.LBB0_1518:
	test	r9b, 1
	je	.LBB0_1520
# %bb.1519:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1520:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1521
.LBB0_1525:
	xor	edi, edi
.LBB0_1526:
	test	r9b, 1
	je	.LBB0_1528
# %bb.1527:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB0_1528:
	cmp	rcx, rax
	je	.LBB0_1533
	jmp	.LBB0_1529
.Lfunc_end0:
	.size	arithmetic_avx2, .Lfunc_end0-arithmetic_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_arr_scalar_avx2
.LCPI1_0:
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI1_3:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI1_1:
	.long	0x00000000
	.long	0x80000000              # double -0
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI1_6:
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
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI1_2:
	.long	0x80000000                      # float -0
.LCPI1_4:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI1_5:
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
.LCPI1_7:
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
	.text
	.globl	arithmetic_arr_scalar_avx2
	.p2align	4, 0x90
	.type	arithmetic_arr_scalar_avx2,@function
arithmetic_arr_scalar_avx2:             # @arithmetic_arr_scalar_avx2
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
	jne	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.10:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.11:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_12
# %bb.443:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_739
# %bb.444:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_739
.LBB1_12:
	xor	ecx, ecx
.LBB1_1137:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1139
.LBB1_1138:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1138
.LBB1_1139:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1140:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1140
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.22:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.23:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB1_446
# %bb.24:
	xor	ecx, ecx
	jmp	.LBB1_748
.LBB1_25:
	test	sil, sil
	je	.LBB1_77
# %bb.26:
	cmp	sil, 1
	jne	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.32:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.33:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_34
# %bb.448:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_749
# %bb.449:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_749
.LBB1_34:
	xor	esi, esi
.LBB1_1145:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1147
.LBB1_1146:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1146
.LBB1_1147:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1148:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1148
	jmp	.LBB1_1817
.LBB1_35:
	cmp	sil, 7
	je	.LBB1_85
# %bb.36:
	cmp	sil, 8
	jne	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.42:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.43:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_44
# %bb.451:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_752
# %bb.452:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_752
.LBB1_44:
	xor	esi, esi
.LBB1_1153:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1155
.LBB1_1154:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1154
.LBB1_1155:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1156:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1156
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.50:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.51:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_52
# %bb.454:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_755
# %bb.455:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_755
.LBB1_52:
	xor	esi, esi
.LBB1_1161:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1163
.LBB1_1162:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1162
.LBB1_1163:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1164:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1164
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.58:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.59:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_60
# %bb.457:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_758
# %bb.458:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_758
.LBB1_60:
	xor	ecx, ecx
.LBB1_1169:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1171
.LBB1_1170:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1170
.LBB1_1171:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1172:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1172
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.66:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.67:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_68
# %bb.460:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_761
# %bb.461:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_761
.LBB1_68:
	xor	esi, esi
.LBB1_1177:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1179
.LBB1_1178:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1178
.LBB1_1179:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1180:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1180
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.74:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.75:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_76
# %bb.463:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_764
# %bb.464:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_764
.LBB1_76:
	xor	ecx, ecx
.LBB1_1185:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1187
.LBB1_1186:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1186
.LBB1_1187:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1188:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1188
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.82:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.83:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_84
# %bb.466:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_767
# %bb.467:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_767
.LBB1_84:
	xor	esi, esi
.LBB1_1193:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1195
.LBB1_1194:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1194
.LBB1_1195:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1196:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1196
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.90:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.91:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_92
# %bb.469:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_770
# %bb.470:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_770
.LBB1_92:
	xor	esi, esi
.LBB1_1201:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1203
.LBB1_1202:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1202
.LBB1_1203:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1204:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1204
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.97:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.98:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_99
# %bb.472:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_773
# %bb.473:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_773
.LBB1_99:
	xor	ecx, ecx
.LBB1_1209:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1212
# %bb.1210:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1211:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1211
.LBB1_1212:
	cmp	rsi, 3
	jb	.LBB1_1817
# %bb.1213:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1214:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1214
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.104:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.105:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_106
# %bb.475:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_776
# %bb.476:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_776
.LBB1_106:
	xor	ecx, ecx
.LBB1_1219:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1222
# %bb.1220:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1221:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1221
.LBB1_1222:
	cmp	rsi, 3
	jb	.LBB1_1817
# %bb.1223:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB1_1224:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1224
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.111:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.112:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_113
# %bb.478:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_779
# %bb.479:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_779
.LBB1_113:
	xor	ecx, ecx
.LBB1_1229:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1231
.LBB1_1230:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1230
.LBB1_1231:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1232:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1232
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.118:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.119:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_120
# %bb.481:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_782
# %bb.482:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_782
.LBB1_120:
	xor	ecx, ecx
.LBB1_1237:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1239
.LBB1_1238:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1238
.LBB1_1239:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1240:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1240
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.125:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.126:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_127
# %bb.484:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_785
# %bb.485:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_785
.LBB1_127:
	xor	ecx, ecx
.LBB1_1245:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1247
.LBB1_1246:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1246
.LBB1_1247:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1248:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1248
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.132:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.133:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_134
# %bb.487:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_788
# %bb.488:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_788
.LBB1_134:
	xor	ecx, ecx
.LBB1_1253:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_1255
.LBB1_1254:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_1254
.LBB1_1255:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1256:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1256
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.139:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.140:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_141
# %bb.490:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_791
# %bb.491:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_791
.LBB1_141:
	xor	ecx, ecx
.LBB1_1261:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1263
.LBB1_1262:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1262
.LBB1_1263:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1264:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1264
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.146:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.147:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_148
# %bb.493:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_794
# %bb.494:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_794
.LBB1_148:
	xor	ecx, ecx
.LBB1_1269:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_1271
.LBB1_1270:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_1270
.LBB1_1271:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1272:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1272
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.153:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.154:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_155
# %bb.496:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_797
# %bb.497:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_797
.LBB1_155:
	xor	ecx, ecx
.LBB1_1277:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1279
.LBB1_1278:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1278
.LBB1_1279:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1280:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1280
	jmp	.LBB1_1817
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
	jne	.LBB1_1817
# %bb.160:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.161:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_162
# %bb.499:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_800
# %bb.500:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_800
.LBB1_162:
	xor	ecx, ecx
.LBB1_1285:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1287
.LBB1_1286:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1286
.LBB1_1287:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1288:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1288
	jmp	.LBB1_1817
.LBB1_163:
	cmp	edi, 2
	je	.LBB1_383
# %bb.164:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.165:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.166:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_167
# %bb.502:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_803
# %bb.503:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_803
.LBB1_167:
	xor	ecx, ecx
.LBB1_1293:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1295
.LBB1_1294:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1294
.LBB1_1295:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1296:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1296
	jmp	.LBB1_1817
.LBB1_168:
	cmp	edi, 2
	je	.LBB1_386
# %bb.169:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.170:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.171:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_172
# %bb.505:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_806
# %bb.506:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_806
.LBB1_172:
	xor	ecx, ecx
.LBB1_1301:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1303
.LBB1_1302:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1302
.LBB1_1303:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1304:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1304
	jmp	.LBB1_1817
.LBB1_173:
	cmp	edi, 2
	je	.LBB1_389
# %bb.174:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.175:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.176:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_177
# %bb.508:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_809
# %bb.509:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_809
.LBB1_177:
	xor	esi, esi
.LBB1_1309:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1311
.LBB1_1310:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1310
.LBB1_1311:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1312:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1312
	jmp	.LBB1_1817
.LBB1_178:
	cmp	edi, 2
	je	.LBB1_392
# %bb.179:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.180:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.181:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_182
# %bb.511:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_812
# %bb.512:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_812
.LBB1_182:
	xor	esi, esi
.LBB1_1317:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1319
.LBB1_1318:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1318
.LBB1_1319:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1320:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1320
	jmp	.LBB1_1817
.LBB1_183:
	cmp	edi, 2
	je	.LBB1_395
# %bb.184:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.185:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.186:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_187
# %bb.514:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_815
# %bb.515:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_815
.LBB1_187:
	xor	edi, edi
.LBB1_1079:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1081
.LBB1_1080:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1080
.LBB1_1081:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1082:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1082
	jmp	.LBB1_1817
.LBB1_188:
	cmp	edi, 2
	je	.LBB1_398
# %bb.189:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.190:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.191:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_192
# %bb.517:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB1_817
# %bb.518:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB1_817
.LBB1_192:
	xor	ecx, ecx
.LBB1_820:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_822
# %bb.821:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_822:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_823:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_823
	jmp	.LBB1_1817
.LBB1_193:
	cmp	edi, 2
	je	.LBB1_401
# %bb.194:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.195:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.196:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_197
# %bb.520:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_824
# %bb.521:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_824
.LBB1_197:
	xor	edi, edi
.LBB1_1089:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1091
.LBB1_1090:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1090
.LBB1_1091:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1092:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1092
	jmp	.LBB1_1817
.LBB1_198:
	cmp	edi, 2
	je	.LBB1_404
# %bb.199:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.200:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.201:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_202
# %bb.523:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB1_826
# %bb.524:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB1_826
.LBB1_202:
	xor	ecx, ecx
.LBB1_829:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_831
# %bb.830:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_831:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_832:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_832
	jmp	.LBB1_1817
.LBB1_203:
	cmp	edi, 2
	je	.LBB1_407
# %bb.204:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.205:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.206:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
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
.LBB1_1325:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1327
.LBB1_1326:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1326
.LBB1_1327:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1328:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1328
	jmp	.LBB1_1817
.LBB1_208:
	cmp	edi, 2
	je	.LBB1_410
# %bb.209:
	cmp	edi, 3
	jne	.LBB1_1817
# %bb.210:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.211:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
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
.LBB1_1333:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1335
.LBB1_1334:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1334
.LBB1_1335:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1336:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1336
	jmp	.LBB1_1817
.LBB1_213:
	cmp	edi, 7
	je	.LBB1_413
# %bb.214:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.215:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.216:
	mov	eax, r9d
	cmp	r9d, 16
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
.LBB1_1341:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1343
.LBB1_1342:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1342
.LBB1_1343:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1344:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1344
	jmp	.LBB1_1817
.LBB1_218:
	cmp	edi, 7
	je	.LBB1_416
# %bb.219:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.220:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.221:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB1_535
# %bb.222:
	xor	ecx, ecx
	jmp	.LBB1_848
.LBB1_223:
	cmp	edi, 7
	je	.LBB1_419
# %bb.224:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.225:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.226:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
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
.LBB1_1349:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1351
.LBB1_1350:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1350
.LBB1_1351:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1352:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1352
	jmp	.LBB1_1817
.LBB1_228:
	cmp	edi, 7
	je	.LBB1_422
# %bb.229:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.230:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.231:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
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
.LBB1_1357:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1359
.LBB1_1358:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1358
.LBB1_1359:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1360:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1360
	jmp	.LBB1_1817
.LBB1_233:
	cmp	edi, 7
	je	.LBB1_425
# %bb.234:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.235:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.236:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_237
# %bb.543:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_855
# %bb.544:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_855
.LBB1_237:
	xor	esi, esi
.LBB1_1365:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1367
.LBB1_1366:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1366
.LBB1_1367:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1368:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1368
	jmp	.LBB1_1817
.LBB1_238:
	cmp	edi, 7
	je	.LBB1_428
# %bb.239:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.240:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.241:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_242
# %bb.546:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_858
# %bb.547:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_858
.LBB1_242:
	xor	ecx, ecx
.LBB1_1373:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1375
.LBB1_1374:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1374
.LBB1_1375:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1376:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1376
	jmp	.LBB1_1817
.LBB1_243:
	cmp	edi, 7
	je	.LBB1_431
# %bb.244:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.245:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.246:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_247
# %bb.549:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_861
# %bb.550:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_861
.LBB1_247:
	xor	esi, esi
.LBB1_1381:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1383
.LBB1_1382:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1382
.LBB1_1383:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1384:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1384
	jmp	.LBB1_1817
.LBB1_248:
	cmp	edi, 7
	je	.LBB1_434
# %bb.249:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.250:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.251:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_252
# %bb.552:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_864
# %bb.553:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_864
.LBB1_252:
	xor	ecx, ecx
.LBB1_1389:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1391
.LBB1_1390:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1390
.LBB1_1391:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1392:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1392
	jmp	.LBB1_1817
.LBB1_253:
	cmp	edi, 7
	je	.LBB1_437
# %bb.254:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.255:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.256:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_257
# %bb.555:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_867
# %bb.556:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_867
.LBB1_257:
	xor	esi, esi
.LBB1_1397:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1399
.LBB1_1398:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1398
.LBB1_1399:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1400:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1400
	jmp	.LBB1_1817
.LBB1_258:
	cmp	edi, 7
	je	.LBB1_440
# %bb.259:
	cmp	edi, 8
	jne	.LBB1_1817
# %bb.260:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.261:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_262
# %bb.558:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_870
# %bb.559:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_870
.LBB1_262:
	xor	esi, esi
.LBB1_1405:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1407
.LBB1_1406:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1406
.LBB1_1407:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1408:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1408
	jmp	.LBB1_1817
.LBB1_263:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.264:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_265
# %bb.561:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_873
# %bb.562:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_873
.LBB1_265:
	xor	ecx, ecx
.LBB1_1413:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1415
.LBB1_1414:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1414
.LBB1_1415:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1416:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1416
	jmp	.LBB1_1817
.LBB1_266:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.267:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_268
# %bb.564:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_876
# %bb.565:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_876
.LBB1_268:
	xor	ecx, ecx
.LBB1_1421:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1423
.LBB1_1422:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1422
.LBB1_1423:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1424:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1424
	jmp	.LBB1_1817
.LBB1_269:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.270:
	mov	eax, r9d
	cmp	r9d, 64
	jae	.LBB1_567
# %bb.271:
	xor	ecx, ecx
	jmp	.LBB1_885
.LBB1_272:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.273:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_274
# %bb.569:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_886
# %bb.570:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_886
.LBB1_274:
	xor	ecx, ecx
.LBB1_1429:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1431
.LBB1_1430:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1430
.LBB1_1431:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1432:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1432
	jmp	.LBB1_1817
.LBB1_275:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.276:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_277
# %bb.572:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_889
# %bb.573:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_889
.LBB1_277:
	xor	esi, esi
.LBB1_1437:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1439
.LBB1_1438:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1438
.LBB1_1439:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1440:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1440
	jmp	.LBB1_1817
.LBB1_278:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.279:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_280
# %bb.575:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_892
# %bb.576:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_892
.LBB1_280:
	xor	esi, esi
.LBB1_1445:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1447
.LBB1_1446:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1446
.LBB1_1447:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1448:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1448
	jmp	.LBB1_1817
.LBB1_281:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.282:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_283
# %bb.578:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_895
# %bb.579:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_895
.LBB1_283:
	xor	esi, esi
.LBB1_1453:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1455
.LBB1_1454:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1454
.LBB1_1455:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1456:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1456
	jmp	.LBB1_1817
.LBB1_284:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.285:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_286
# %bb.581:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_898
# %bb.582:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_898
.LBB1_286:
	xor	esi, esi
.LBB1_1461:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1463
.LBB1_1462:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1462
.LBB1_1463:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1464:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1464
	jmp	.LBB1_1817
.LBB1_287:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.288:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_289
# %bb.584:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_901
# %bb.585:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_901
.LBB1_289:
	xor	esi, esi
.LBB1_1469:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1471
.LBB1_1470:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1470
.LBB1_1471:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1472:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1472
	jmp	.LBB1_1817
.LBB1_290:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.291:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_292
# %bb.587:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_904
# %bb.588:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_904
.LBB1_292:
	xor	esi, esi
.LBB1_1477:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1479
.LBB1_1478:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1478
.LBB1_1479:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1480:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1480
	jmp	.LBB1_1817
.LBB1_293:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.294:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_295
# %bb.590:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_907
# %bb.591:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_907
.LBB1_295:
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
	jb	.LBB1_1817
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
	jmp	.LBB1_1817
.LBB1_296:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.297:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_298
# %bb.593:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_909
# %bb.594:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_909
.LBB1_298:
	xor	ecx, ecx
.LBB1_1485:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1487
# %bb.1486:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1487:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_1488:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1488
	jmp	.LBB1_1817
.LBB1_299:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.300:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_301
# %bb.596:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_912
# %bb.597:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_912
.LBB1_301:
	xor	esi, esi
.LBB1_1493:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1495
.LBB1_1494:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1494
.LBB1_1495:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1496:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1496
	jmp	.LBB1_1817
.LBB1_302:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.303:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_304
# %bb.599:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_915
# %bb.600:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_915
.LBB1_304:
	xor	esi, esi
.LBB1_1501:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1503
.LBB1_1502:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1502
.LBB1_1503:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1504:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1504
	jmp	.LBB1_1817
.LBB1_305:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.306:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_307
# %bb.602:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_918
# %bb.603:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_918
.LBB1_307:
	xor	ecx, ecx
.LBB1_1109:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1111
.LBB1_1110:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_1110
.LBB1_1111:
	cmp	rax, 3
	jb	.LBB1_1817
.LBB1_1112:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1112
	jmp	.LBB1_1817
.LBB1_308:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.309:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_310
# %bb.605:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_920
# %bb.606:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_920
.LBB1_310:
	xor	ecx, ecx
.LBB1_1509:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1511
# %bb.1510:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1511:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_1512:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1512
	jmp	.LBB1_1817
.LBB1_311:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.312:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_313
# %bb.608:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_923
# %bb.609:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_923
.LBB1_313:
	xor	esi, esi
.LBB1_1517:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1519
.LBB1_1518:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1518
.LBB1_1519:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1520:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1520
	jmp	.LBB1_1817
.LBB1_314:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.315:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_316
# %bb.611:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_926
# %bb.612:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_926
.LBB1_316:
	xor	esi, esi
.LBB1_1525:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1527
.LBB1_1526:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1526
.LBB1_1527:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1528:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1528
	jmp	.LBB1_1817
.LBB1_317:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.318:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_319
# %bb.614:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_929
# %bb.615:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_929
.LBB1_319:
	xor	esi, esi
.LBB1_1533:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1535
.LBB1_1534:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1534
.LBB1_1535:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1536:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1536
	jmp	.LBB1_1817
.LBB1_320:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.321:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_322
# %bb.617:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_932
# %bb.618:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_932
.LBB1_322:
	xor	esi, esi
.LBB1_1541:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_1543
.LBB1_1542:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_1542
.LBB1_1543:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1544:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1544
	jmp	.LBB1_1817
.LBB1_323:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.324:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_325
# %bb.620:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_935
# %bb.621:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_935
.LBB1_325:
	xor	ecx, ecx
.LBB1_1549:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1551
.LBB1_1550:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1550
.LBB1_1551:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1552:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1552
	jmp	.LBB1_1817
.LBB1_326:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.327:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_328
# %bb.623:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_938
# %bb.624:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_938
.LBB1_328:
	xor	ecx, ecx
.LBB1_1557:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1560
# %bb.1558:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI1_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1559:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1559
.LBB1_1560:
	cmp	rsi, 3
	jb	.LBB1_1817
# %bb.1561:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI1_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1562:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1562
	jmp	.LBB1_1817
.LBB1_329:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.330:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_331
# %bb.626:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_941
# %bb.627:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_941
.LBB1_331:
	xor	ecx, ecx
.LBB1_1567:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1569
.LBB1_1568:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1568
.LBB1_1569:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1570:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1570
	jmp	.LBB1_1817
.LBB1_332:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.333:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_334
# %bb.629:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_944
# %bb.630:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_944
.LBB1_334:
	xor	ecx, ecx
.LBB1_1575:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1578
# %bb.1576:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI1_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1577:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1577
.LBB1_1578:
	cmp	rsi, 3
	jb	.LBB1_1817
# %bb.1579:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI1_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_1580:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB1_1580
	jmp	.LBB1_1817
.LBB1_335:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.336:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_337
# %bb.632:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_947
# %bb.633:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_947
.LBB1_337:
	xor	esi, esi
.LBB1_1585:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1587
.LBB1_1586:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1586
.LBB1_1587:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1588:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1588
	jmp	.LBB1_1817
.LBB1_338:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.339:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_340
# %bb.635:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_950
# %bb.636:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_950
.LBB1_340:
	xor	ecx, ecx
.LBB1_1593:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1595
.LBB1_1594:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1594
.LBB1_1595:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1596:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1596
	jmp	.LBB1_1817
.LBB1_341:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.342:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_343
# %bb.638:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_953
# %bb.639:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_953
.LBB1_343:
	xor	esi, esi
.LBB1_1601:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1603
.LBB1_1602:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1602
.LBB1_1603:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1604:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1604
	jmp	.LBB1_1817
.LBB1_344:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.345:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_346
# %bb.641:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_956
# %bb.642:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_956
.LBB1_346:
	xor	ecx, ecx
.LBB1_1609:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1611
.LBB1_1610:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1610
.LBB1_1611:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1612:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1612
	jmp	.LBB1_1817
.LBB1_347:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.348:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_349
# %bb.644:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_959
# %bb.645:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_959
.LBB1_349:
	xor	esi, esi
.LBB1_1617:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1619
.LBB1_1618:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1618
.LBB1_1619:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1620:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1620
	jmp	.LBB1_1817
.LBB1_350:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.351:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_352
# %bb.647:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_962
# %bb.648:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_962
.LBB1_352:
	xor	ecx, ecx
.LBB1_1625:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1627
.LBB1_1626:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1626
.LBB1_1627:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1628:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1628
	jmp	.LBB1_1817
.LBB1_353:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.354:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_355
# %bb.650:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_965
# %bb.651:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_965
.LBB1_355:
	xor	ecx, ecx
.LBB1_968:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_970
# %bb.969:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_970:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_971:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_971
	jmp	.LBB1_1817
.LBB1_356:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.357:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_358
# %bb.653:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_972
# %bb.654:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_972
.LBB1_358:
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
	jb	.LBB1_1817
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
	jmp	.LBB1_1817
.LBB1_359:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.360:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_361
# %bb.656:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_975
# %bb.657:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_975
.LBB1_361:
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
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1644
.LBB1_1645:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1646:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1646
	jmp	.LBB1_1817
.LBB1_362:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.363:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_364
# %bb.659:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_978
# %bb.660:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_978
.LBB1_364:
	xor	ecx, ecx
.LBB1_1651:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1653
.LBB1_1652:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1652
.LBB1_1653:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1654:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1654
	jmp	.LBB1_1817
.LBB1_365:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.366:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_367
# %bb.662:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_981
# %bb.663:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_981
.LBB1_367:
	xor	ecx, ecx
.LBB1_984:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_986
# %bb.985:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_986:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_987:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_987
	jmp	.LBB1_1817
.LBB1_368:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.369:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_370
# %bb.665:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_988
# %bb.666:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_988
.LBB1_370:
	xor	ecx, ecx
.LBB1_1659:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1662
# %bb.1660:
	mov	esi, 2147483647
.LBB1_1661:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1661
.LBB1_1662:
	cmp	r9, 3
	jb	.LBB1_1817
# %bb.1663:
	mov	esi, 2147483647
.LBB1_1664:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1664
	jmp	.LBB1_1817
.LBB1_371:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.372:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_373
# %bb.668:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_991
# %bb.669:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_991
.LBB1_373:
	xor	esi, esi
.LBB1_1669:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1671
.LBB1_1670:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1670
.LBB1_1671:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1672:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1672
	jmp	.LBB1_1817
.LBB1_374:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.375:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_376
# %bb.671:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_994
# %bb.672:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_994
.LBB1_376:
	xor	ecx, ecx
.LBB1_1677:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1679
.LBB1_1678:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1678
.LBB1_1679:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1680:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1680
	jmp	.LBB1_1817
.LBB1_377:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.378:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_379
# %bb.674:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_997
# %bb.675:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_997
.LBB1_379:
	xor	esi, esi
.LBB1_1685:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1687
.LBB1_1686:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1686
.LBB1_1687:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1688:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1688
	jmp	.LBB1_1817
.LBB1_380:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.381:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_382
# %bb.677:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1000
# %bb.678:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1000
.LBB1_382:
	xor	ecx, ecx
.LBB1_1693:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1695
.LBB1_1694:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1694
.LBB1_1695:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1696:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1696
	jmp	.LBB1_1817
.LBB1_383:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.384:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_385
# %bb.680:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1003
# %bb.681:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1003
.LBB1_385:
	xor	ecx, ecx
.LBB1_1701:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1703
.LBB1_1702:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1702
.LBB1_1703:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1704:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1704
	jmp	.LBB1_1817
.LBB1_386:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.387:
	mov	eax, r9d
	cmp	r9d, 128
	jae	.LBB1_683
# %bb.388:
	xor	ecx, ecx
	jmp	.LBB1_1012
.LBB1_389:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.390:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_391
# %bb.685:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1013
# %bb.686:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1013
.LBB1_391:
	xor	esi, esi
.LBB1_1709:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1711
.LBB1_1710:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1710
.LBB1_1711:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1712:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1712
	jmp	.LBB1_1817
.LBB1_392:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.393:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_394
# %bb.688:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1016
# %bb.689:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1016
.LBB1_394:
	xor	esi, esi
.LBB1_1717:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1719
.LBB1_1718:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1718
.LBB1_1719:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1720:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1720
	jmp	.LBB1_1817
.LBB1_395:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.396:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_397
# %bb.691:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_1019
# %bb.692:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_1019
.LBB1_397:
	xor	edi, edi
.LBB1_1119:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1121
.LBB1_1120:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1120
.LBB1_1121:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1122:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1122
	jmp	.LBB1_1817
.LBB1_398:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.399:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_400
# %bb.694:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1021
# %bb.695:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1021
.LBB1_400:
	xor	ecx, ecx
.LBB1_1725:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1727
.LBB1_1726:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1726
.LBB1_1727:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1728:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1728
	jmp	.LBB1_1817
.LBB1_401:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.402:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_403
# %bb.697:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_1024
# %bb.698:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_1024
.LBB1_403:
	xor	edi, edi
.LBB1_1129:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1131
.LBB1_1130:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1130
.LBB1_1131:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1132:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1132
	jmp	.LBB1_1817
.LBB1_404:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.405:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_406
# %bb.700:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1026
# %bb.701:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1026
.LBB1_406:
	xor	ecx, ecx
.LBB1_1733:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1735
.LBB1_1734:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1734
.LBB1_1735:
	cmp	rsi, 3
	jb	.LBB1_1817
.LBB1_1736:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1736
	jmp	.LBB1_1817
.LBB1_407:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.408:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_409
# %bb.703:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1029
# %bb.704:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1029
.LBB1_409:
	xor	esi, esi
.LBB1_1741:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1743
.LBB1_1742:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1742
.LBB1_1743:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1744:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1744
	jmp	.LBB1_1817
.LBB1_410:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.411:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_412
# %bb.706:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_1032
# %bb.707:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_1032
.LBB1_412:
	xor	esi, esi
.LBB1_1749:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1751
.LBB1_1750:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1750
.LBB1_1751:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1752:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1752
	jmp	.LBB1_1817
.LBB1_413:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.414:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_415
# %bb.709:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1035
# %bb.710:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1035
.LBB1_415:
	xor	ecx, ecx
.LBB1_1757:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1759
.LBB1_1758:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1758
.LBB1_1759:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1760:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1760
	jmp	.LBB1_1817
.LBB1_416:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.417:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_418
# %bb.712:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1038
# %bb.713:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1038
.LBB1_418:
	xor	ecx, ecx
.LBB1_1765:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1767
.LBB1_1766:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1766
.LBB1_1767:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1768:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1768
	jmp	.LBB1_1817
.LBB1_419:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.420:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_421
# %bb.715:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1041
# %bb.716:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1041
.LBB1_421:
	xor	esi, esi
.LBB1_1773:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1775
.LBB1_1774:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1774
.LBB1_1775:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1776:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1776
	jmp	.LBB1_1817
.LBB1_422:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.423:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_424
# %bb.718:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1044
# %bb.719:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1044
.LBB1_424:
	xor	esi, esi
.LBB1_1781:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1783
.LBB1_1782:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1782
.LBB1_1783:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1784:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1784
	jmp	.LBB1_1817
.LBB1_425:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.426:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_427
# %bb.721:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1047
# %bb.722:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1047
.LBB1_427:
	xor	esi, esi
.LBB1_1789:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1791
.LBB1_1790:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1790
.LBB1_1791:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1792:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1792
	jmp	.LBB1_1817
.LBB1_428:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.429:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_430
# %bb.724:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1050
# %bb.725:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1050
.LBB1_430:
	xor	ecx, ecx
.LBB1_1053:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1055
# %bb.1054:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_1055:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_1056:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1056
	jmp	.LBB1_1817
.LBB1_431:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.432:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_433
# %bb.727:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1057
# %bb.728:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1057
.LBB1_433:
	xor	esi, esi
.LBB1_1797:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1799
.LBB1_1798:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1798
.LBB1_1799:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1800:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1800
	jmp	.LBB1_1817
.LBB1_434:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.435:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_436
# %bb.730:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_1060
# %bb.731:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_1060
.LBB1_436:
	xor	ecx, ecx
.LBB1_1063:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1065
# %bb.1064:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_1065:
	add	rsi, rax
	je	.LBB1_1817
.LBB1_1066:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1066
	jmp	.LBB1_1817
.LBB1_437:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.438:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_439
# %bb.733:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1067
# %bb.734:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1067
.LBB1_439:
	xor	esi, esi
.LBB1_1805:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1807
.LBB1_1806:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1806
.LBB1_1807:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1808:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1808
	jmp	.LBB1_1817
.LBB1_440:
	test	r9d, r9d
	jle	.LBB1_1817
# %bb.441:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_442
# %bb.736:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_1070
# %bb.737:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_1070
.LBB1_442:
	xor	esi, esi
.LBB1_1813:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1815
.LBB1_1814:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1814
.LBB1_1815:
	cmp	r9, 3
	jb	.LBB1_1817
.LBB1_1816:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1816
	jmp	.LBB1_1817
.LBB1_446:
	mov	ecx, eax
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 96
	jae	.LBB1_742
# %bb.447:
	xor	edi, edi
	jmp	.LBB1_744
.LBB1_535:
	mov	ecx, eax
	and	ecx, -16
	lea	rdi, [rcx - 16]
	mov	rsi, rdi
	shr	rsi, 4
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 48
	jae	.LBB1_842
# %bb.536:
	xor	edi, edi
	jmp	.LBB1_844
.LBB1_567:
	mov	ecx, eax
	and	ecx, -64
	lea	rdi, [rcx - 64]
	mov	rsi, rdi
	shr	rsi, 6
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 192
	jae	.LBB1_879
# %bb.568:
	xor	edi, edi
	jmp	.LBB1_881
.LBB1_683:
	mov	ecx, eax
	and	ecx, -128
	lea	rdi, [rcx - 128]
	mov	rsi, rdi
	shr	rsi, 7
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 384
	jae	.LBB1_1006
# %bb.684:
	xor	edi, edi
	jmp	.LBB1_1008
.LBB1_739:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1133
# %bb.740:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_741:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_741
	jmp	.LBB1_1134
.LBB1_749:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1141
# %bb.750:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_751:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_751
	jmp	.LBB1_1142
.LBB1_752:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1149
# %bb.753:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_754:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_754
	jmp	.LBB1_1150
.LBB1_755:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1157
# %bb.756:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_757:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_757
	jmp	.LBB1_1158
.LBB1_758:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1165
# %bb.759:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_760:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_760
	jmp	.LBB1_1166
.LBB1_761:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1173
# %bb.762:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_763:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_763
	jmp	.LBB1_1174
.LBB1_764:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1181
# %bb.765:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_766:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_766
	jmp	.LBB1_1182
.LBB1_767:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1189
# %bb.768:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_769:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_769
	jmp	.LBB1_1190
.LBB1_770:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1197
# %bb.771:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_772:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_772
	jmp	.LBB1_1198
.LBB1_773:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1205
# %bb.774:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_775:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_775
	jmp	.LBB1_1206
.LBB1_776:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1215
# %bb.777:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_778:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_778
	jmp	.LBB1_1216
.LBB1_779:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1225
# %bb.780:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_781:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_781
	jmp	.LBB1_1226
.LBB1_782:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1233
# %bb.783:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_784:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_784
	jmp	.LBB1_1234
.LBB1_785:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1241
# %bb.786:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_787:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_787
	jmp	.LBB1_1242
.LBB1_788:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB1_1249
# %bb.789:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB1_790:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_790
	jmp	.LBB1_1250
.LBB1_791:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1257
# %bb.792:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_793:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_793
	jmp	.LBB1_1258
.LBB1_794:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB1_1265
# %bb.795:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB1_796:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_796
	jmp	.LBB1_1266
.LBB1_797:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1273
# %bb.798:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_799:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_799
	jmp	.LBB1_1274
.LBB1_800:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1281
# %bb.801:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_802:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_802
	jmp	.LBB1_1282
.LBB1_803:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1289
# %bb.804:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_805:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB1_805
	jmp	.LBB1_1290
.LBB1_806:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1297
# %bb.807:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_808:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB1_808
	jmp	.LBB1_1298
.LBB1_809:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1305
# %bb.810:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_811:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_811
	jmp	.LBB1_1306
.LBB1_812:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1313
# %bb.813:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_814:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_814
	jmp	.LBB1_1314
.LBB1_815:
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
	jae	.LBB1_1073
# %bb.816:
	xor	esi, esi
	jmp	.LBB1_1075
.LBB1_817:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_6] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB1_818:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_818
# %bb.819:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_820
.LBB1_824:
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
	jae	.LBB1_1083
# %bb.825:
	xor	esi, esi
	jmp	.LBB1_1085
.LBB1_826:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI1_6] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB1_827:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_827
# %bb.828:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_829
.LBB1_833:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1321
# %bb.834:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_835:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_835
	jmp	.LBB1_1322
.LBB1_836:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1329
# %bb.837:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_838:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_838
	jmp	.LBB1_1330
.LBB1_839:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1337
# %bb.840:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_841:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_841
	jmp	.LBB1_1338
.LBB1_849:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1345
# %bb.850:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_851:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_851
	jmp	.LBB1_1346
.LBB1_852:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1353
# %bb.853:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_854:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_854
	jmp	.LBB1_1354
.LBB1_855:
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
	je	.LBB1_1361
# %bb.856:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_857:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_857
	jmp	.LBB1_1362
.LBB1_858:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1369
# %bb.859:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_860:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_860
	jmp	.LBB1_1370
.LBB1_861:
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
	je	.LBB1_1377
# %bb.862:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_863:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_863
	jmp	.LBB1_1378
.LBB1_864:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1385
# %bb.865:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_866:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_866
	jmp	.LBB1_1386
.LBB1_867:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1393
# %bb.868:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_869:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_869
	jmp	.LBB1_1394
.LBB1_870:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1401
# %bb.871:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_872:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_872
	jmp	.LBB1_1402
.LBB1_873:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1409
# %bb.874:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_875:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_875
	jmp	.LBB1_1410
.LBB1_876:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1417
# %bb.877:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_878:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_878
	jmp	.LBB1_1418
.LBB1_886:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1425
# %bb.887:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_888:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_888
	jmp	.LBB1_1426
.LBB1_889:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1433
# %bb.890:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_891:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_891
	jmp	.LBB1_1434
.LBB1_892:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1441
# %bb.893:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_894:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_894
	jmp	.LBB1_1442
.LBB1_895:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1449
# %bb.896:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_897:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_897
	jmp	.LBB1_1450
.LBB1_898:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1457
# %bb.899:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_900:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_900
	jmp	.LBB1_1458
.LBB1_901:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1465
# %bb.902:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_903:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_903
	jmp	.LBB1_1466
.LBB1_904:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1473
# %bb.905:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_906:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_906
	jmp	.LBB1_1474
.LBB1_907:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB1_1093
# %bb.908:
	xor	eax, eax
	jmp	.LBB1_1095
.LBB1_909:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1481
# %bb.910:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI1_5] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB1_911:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_911
	jmp	.LBB1_1482
.LBB1_912:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1489
# %bb.913:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_914:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_914
	jmp	.LBB1_1490
.LBB1_915:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1497
# %bb.916:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_917:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_917
	jmp	.LBB1_1498
.LBB1_918:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB1_1103
# %bb.919:
	xor	eax, eax
	jmp	.LBB1_1105
.LBB1_920:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1505
# %bb.921:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI1_5] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB1_922:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_922
	jmp	.LBB1_1506
.LBB1_923:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1513
# %bb.924:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_925:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_925
	jmp	.LBB1_1514
.LBB1_926:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1521
# %bb.927:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_928:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_928
	jmp	.LBB1_1522
.LBB1_929:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1529
# %bb.930:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_931:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_931
	jmp	.LBB1_1530
.LBB1_932:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1537
# %bb.933:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_934:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_934
	jmp	.LBB1_1538
.LBB1_935:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1545
# %bb.936:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_937:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_937
	jmp	.LBB1_1546
.LBB1_938:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1553
# %bb.939:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_940:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_940
	jmp	.LBB1_1554
.LBB1_941:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1563
# %bb.942:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_943:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB1_943
	jmp	.LBB1_1564
.LBB1_944:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1571
# %bb.945:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB1_946:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_946
	jmp	.LBB1_1572
.LBB1_947:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1581
# %bb.948:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_949:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_949
	jmp	.LBB1_1582
.LBB1_950:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1589
# %bb.951:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_952:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_952
	jmp	.LBB1_1590
.LBB1_953:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1597
# %bb.954:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_955:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_955
	jmp	.LBB1_1598
.LBB1_956:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1605
# %bb.957:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_958:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_958
	jmp	.LBB1_1606
.LBB1_959:
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
	je	.LBB1_1613
# %bb.960:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_961:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_961
	jmp	.LBB1_1614
.LBB1_962:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1621
# %bb.963:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_964:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_964
	jmp	.LBB1_1622
.LBB1_965:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB1_966:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_966
# %bb.967:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_968
.LBB1_972:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1629
# %bb.973:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB1_974:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_974
	jmp	.LBB1_1630
.LBB1_975:
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
	je	.LBB1_1639
# %bb.976:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_977:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_977
	jmp	.LBB1_1640
.LBB1_978:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1647
# %bb.979:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_980
	jmp	.LBB1_1648
.LBB1_981:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB1_982:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_982
# %bb.983:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_984
.LBB1_988:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1655
# %bb.989:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB1_990:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_990
	jmp	.LBB1_1656
.LBB1_991:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1665
# %bb.992:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_993:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_993
	jmp	.LBB1_1666
.LBB1_994:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1673
# %bb.995:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_996:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_996
	jmp	.LBB1_1674
.LBB1_997:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1681
# %bb.998:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_999:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_999
	jmp	.LBB1_1682
.LBB1_1000:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1689
# %bb.1001:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1002:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1002
	jmp	.LBB1_1690
.LBB1_1003:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1697
# %bb.1004:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_1005:                             # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB1_1005
	jmp	.LBB1_1698
.LBB1_1013:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1705
# %bb.1014:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1015:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1015
	jmp	.LBB1_1706
.LBB1_1016:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1713
# %bb.1017:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1018:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1018
	jmp	.LBB1_1714
.LBB1_1019:
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
	jae	.LBB1_1113
# %bb.1020:
	xor	esi, esi
	jmp	.LBB1_1115
.LBB1_1021:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1721
# %bb.1022:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1023:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1023
	jmp	.LBB1_1722
.LBB1_1024:
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
	jae	.LBB1_1123
# %bb.1025:
	xor	esi, esi
	jmp	.LBB1_1125
.LBB1_1026:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1729
# %bb.1027:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_1028:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1028
	jmp	.LBB1_1730
.LBB1_1029:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1737
# %bb.1030:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1031:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1031
	jmp	.LBB1_1738
.LBB1_1032:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1745
# %bb.1033:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1034:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1034
	jmp	.LBB1_1746
.LBB1_1035:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1753
# %bb.1036:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_1037:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_1037
	jmp	.LBB1_1754
.LBB1_1038:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1761
# %bb.1039:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_1040:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB1_1040
	jmp	.LBB1_1762
.LBB1_1041:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1769
# %bb.1042:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1043:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1043
	jmp	.LBB1_1770
.LBB1_1044:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1777
# %bb.1045:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1046:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1046
	jmp	.LBB1_1778
.LBB1_1047:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1785
# %bb.1048:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1049:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1049
	jmp	.LBB1_1786
.LBB1_1050:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB1_1051:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1051
# %bb.1052:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1053
.LBB1_1057:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1793
# %bb.1058:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1059:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1059
	jmp	.LBB1_1794
.LBB1_1060:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB1_1061:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1061
# %bb.1062:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1063
.LBB1_1067:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1801
# %bb.1068:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1069:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1069
	jmp	.LBB1_1802
.LBB1_1070:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1809
# %bb.1071:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_1072:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1072
	jmp	.LBB1_1810
.LBB1_742:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_743:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 480], ymm0
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB1_743
.LBB1_744:
	test	rdx, rdx
	je	.LBB1_747
# %bb.745:
	lea	rsi, [r8 + 4*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB1_746:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB1_746
.LBB1_747:
	cmp	rcx, rax
	je	.LBB1_1817
.LBB1_748:                              # =>This Inner Loop Header: Depth=1
	mov	dword ptr [r8 + 4*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_748
	jmp	.LBB1_1817
.LBB1_842:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_843:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 480], ymm0
	add	rdi, 64
	add	rsi, 4
	jne	.LBB1_843
.LBB1_844:
	test	rdx, rdx
	je	.LBB1_847
# %bb.845:
	lea	rsi, [r8 + 8*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB1_846:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB1_846
.LBB1_847:
	cmp	rcx, rax
	je	.LBB1_1817
.LBB1_848:                              # =>This Inner Loop Header: Depth=1
	mov	qword ptr [r8 + 8*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_848
	jmp	.LBB1_1817
.LBB1_879:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_880:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 480], ymm0
	add	rdi, 256
	add	rsi, 4
	jne	.LBB1_880
.LBB1_881:
	test	rdx, rdx
	je	.LBB1_884
# %bb.882:
	lea	rsi, [r8 + 2*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB1_883:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB1_883
.LBB1_884:
	cmp	rcx, rax
	je	.LBB1_1817
.LBB1_885:                              # =>This Inner Loop Header: Depth=1
	mov	word ptr [r8 + 2*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_885
	jmp	.LBB1_1817
.LBB1_1006:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB1_1007:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 480], ymm0
	add	rdi, 512
	add	rsi, 4
	jne	.LBB1_1007
.LBB1_1008:
	test	rdx, rdx
	je	.LBB1_1011
# %bb.1009:
	lea	rsi, [rdi + r8]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB1_1010:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB1_1010
.LBB1_1011:
	cmp	rcx, rax
	je	.LBB1_1817
.LBB1_1012:                             # =>This Inner Loop Header: Depth=1
	mov	byte ptr [r8 + rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB1_1012
.LBB1_1817:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB1_1073:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1074:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1074
.LBB1_1075:
	test	r9, r9
	je	.LBB1_1078
# %bb.1076:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1077:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1077
.LBB1_1078:
	cmp	rdi, r10
	je	.LBB1_1817
	jmp	.LBB1_1079
.LBB1_1083:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1084:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1084
.LBB1_1085:
	test	r9, r9
	je	.LBB1_1088
# %bb.1086:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1087:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1087
.LBB1_1088:
	cmp	rdi, r10
	je	.LBB1_1817
	jmp	.LBB1_1089
.LBB1_1093:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1094:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1094
.LBB1_1095:
	test	rsi, rsi
	je	.LBB1_1098
# %bb.1096:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB1_1097:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB1_1097
.LBB1_1098:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1099
.LBB1_1103:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_1104:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1104
.LBB1_1105:
	test	rsi, rsi
	je	.LBB1_1108
# %bb.1106:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB1_1107:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB1_1107
.LBB1_1108:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1109
.LBB1_1113:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1114:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1114
.LBB1_1115:
	test	r9, r9
	je	.LBB1_1118
# %bb.1116:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1117:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1117
.LBB1_1118:
	cmp	rdi, r10
	je	.LBB1_1817
	jmp	.LBB1_1119
.LBB1_1123:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1124:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1124
.LBB1_1125:
	test	r9, r9
	je	.LBB1_1128
# %bb.1126:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_1127:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1127
.LBB1_1128:
	cmp	rdi, r10
	je	.LBB1_1817
	jmp	.LBB1_1129
.LBB1_1133:
	xor	edi, edi
.LBB1_1134:
	test	r9b, 1
	je	.LBB1_1136
# %bb.1135:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1136:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1137
.LBB1_1141:
	xor	edi, edi
.LBB1_1142:
	test	r9b, 1
	je	.LBB1_1144
# %bb.1143:
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
.LBB1_1144:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1145
.LBB1_1149:
	xor	edi, edi
.LBB1_1150:
	test	r9b, 1
	je	.LBB1_1152
# %bb.1151:
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
.LBB1_1152:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1153
.LBB1_1157:
	xor	edi, edi
.LBB1_1158:
	test	r9b, 1
	je	.LBB1_1160
# %bb.1159:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1160:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1161
.LBB1_1165:
	xor	edi, edi
.LBB1_1166:
	test	r9b, 1
	je	.LBB1_1168
# %bb.1167:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB1_1168:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1169
.LBB1_1173:
	xor	edi, edi
.LBB1_1174:
	test	r9b, 1
	je	.LBB1_1176
# %bb.1175:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1176:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1177
.LBB1_1181:
	xor	edi, edi
.LBB1_1182:
	test	r9b, 1
	je	.LBB1_1184
# %bb.1183:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB1_1184:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1185
.LBB1_1189:
	xor	edi, edi
.LBB1_1190:
	test	r9b, 1
	je	.LBB1_1192
# %bb.1191:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1192:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1193
.LBB1_1197:
	xor	edi, edi
.LBB1_1198:
	test	r9b, 1
	je	.LBB1_1200
# %bb.1199:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1200:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1201
.LBB1_1205:
	xor	edi, edi
.LBB1_1206:
	test	r9b, 1
	je	.LBB1_1208
# %bb.1207:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1208:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1209
.LBB1_1215:
	xor	edi, edi
.LBB1_1216:
	test	r9b, 1
	je	.LBB1_1218
# %bb.1217:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1218:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1219
.LBB1_1225:
	xor	esi, esi
.LBB1_1226:
	test	r9b, 1
	je	.LBB1_1228
# %bb.1227:
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
.LBB1_1228:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1229
.LBB1_1233:
	xor	esi, esi
.LBB1_1234:
	test	r9b, 1
	je	.LBB1_1236
# %bb.1235:
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
.LBB1_1236:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1237
.LBB1_1241:
	xor	edi, edi
.LBB1_1242:
	test	r9b, 1
	je	.LBB1_1244
# %bb.1243:
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1244:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1245
.LBB1_1249:
	xor	edi, edi
.LBB1_1250:
	test	r9b, 1
	je	.LBB1_1252
# %bb.1251:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1252:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1253
.LBB1_1257:
	xor	edi, edi
.LBB1_1258:
	test	r9b, 1
	je	.LBB1_1260
# %bb.1259:
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1260:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1261
.LBB1_1265:
	xor	edi, edi
.LBB1_1266:
	test	r9b, 1
	je	.LBB1_1268
# %bb.1267:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI1_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1268:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1269
.LBB1_1273:
	xor	edi, edi
.LBB1_1274:
	test	r9b, 1
	je	.LBB1_1276
# %bb.1275:
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1276:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1277
.LBB1_1281:
	xor	edi, edi
.LBB1_1282:
	test	r9b, 1
	je	.LBB1_1284
# %bb.1283:
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_1284:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1285
.LBB1_1289:
	xor	edi, edi
.LBB1_1290:
	test	r9b, 1
	je	.LBB1_1292
# %bb.1291:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1292:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1293
.LBB1_1297:
	xor	edi, edi
.LBB1_1298:
	test	r9b, 1
	je	.LBB1_1300
# %bb.1299:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1300:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1301
.LBB1_1305:
	xor	edi, edi
.LBB1_1306:
	test	r9b, 1
	je	.LBB1_1308
# %bb.1307:
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
.LBB1_1308:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1309
.LBB1_1313:
	xor	edi, edi
.LBB1_1314:
	test	r9b, 1
	je	.LBB1_1316
# %bb.1315:
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
.LBB1_1316:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1317
.LBB1_1321:
	xor	edi, edi
.LBB1_1322:
	test	r9b, 1
	je	.LBB1_1324
# %bb.1323:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1324:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1325
.LBB1_1329:
	xor	edi, edi
.LBB1_1330:
	test	r9b, 1
	je	.LBB1_1332
# %bb.1331:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1332:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1333
.LBB1_1337:
	xor	edi, edi
.LBB1_1338:
	test	r9b, 1
	je	.LBB1_1340
# %bb.1339:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1340:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1341
.LBB1_1345:
	xor	edi, edi
.LBB1_1346:
	test	r9b, 1
	je	.LBB1_1348
# %bb.1347:
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
.LBB1_1348:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1349
.LBB1_1353:
	xor	edi, edi
.LBB1_1354:
	test	r9b, 1
	je	.LBB1_1356
# %bb.1355:
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
.LBB1_1356:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1357
.LBB1_1361:
	xor	edi, edi
.LBB1_1362:
	test	r9b, 1
	je	.LBB1_1364
# %bb.1363:
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
.LBB1_1364:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1365
.LBB1_1369:
	xor	edi, edi
.LBB1_1370:
	test	r9b, 1
	je	.LBB1_1372
# %bb.1371:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB1_1372:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1373
.LBB1_1377:
	xor	edi, edi
.LBB1_1378:
	test	r9b, 1
	je	.LBB1_1380
# %bb.1379:
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
.LBB1_1380:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1381
.LBB1_1385:
	xor	edi, edi
.LBB1_1386:
	test	r9b, 1
	je	.LBB1_1388
# %bb.1387:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB1_1388:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1389
.LBB1_1393:
	xor	edi, edi
.LBB1_1394:
	test	r9b, 1
	je	.LBB1_1396
# %bb.1395:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1396:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1397
.LBB1_1401:
	xor	edi, edi
.LBB1_1402:
	test	r9b, 1
	je	.LBB1_1404
# %bb.1403:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1404:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1405
.LBB1_1409:
	xor	edi, edi
.LBB1_1410:
	test	r9b, 1
	je	.LBB1_1412
# %bb.1411:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1412:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1413
.LBB1_1417:
	xor	edi, edi
.LBB1_1418:
	test	r9b, 1
	je	.LBB1_1420
# %bb.1419:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1420:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1421
.LBB1_1425:
	xor	edi, edi
.LBB1_1426:
	test	r9b, 1
	je	.LBB1_1428
# %bb.1427:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1428:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1429
.LBB1_1433:
	xor	edi, edi
.LBB1_1434:
	test	r9b, 1
	je	.LBB1_1436
# %bb.1435:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1436:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1437
.LBB1_1441:
	xor	edi, edi
.LBB1_1442:
	test	r9b, 1
	je	.LBB1_1444
# %bb.1443:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1444:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1445
.LBB1_1449:
	xor	edi, edi
.LBB1_1450:
	test	r9b, 1
	je	.LBB1_1452
# %bb.1451:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1452:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1453
.LBB1_1457:
	xor	edi, edi
.LBB1_1458:
	test	r9b, 1
	je	.LBB1_1460
# %bb.1459:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1460:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1461
.LBB1_1465:
	xor	edi, edi
.LBB1_1466:
	test	r9b, 1
	je	.LBB1_1468
# %bb.1467:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1468:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1469
.LBB1_1473:
	xor	edi, edi
.LBB1_1474:
	test	r9b, 1
	je	.LBB1_1476
# %bb.1475:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1476:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1477
.LBB1_1481:
	xor	esi, esi
.LBB1_1482:
	test	r9b, 1
	je	.LBB1_1484
# %bb.1483:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_5] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB1_1484:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1485
.LBB1_1489:
	xor	edi, edi
.LBB1_1490:
	test	r9b, 1
	je	.LBB1_1492
# %bb.1491:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1492:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1493
.LBB1_1497:
	xor	edi, edi
.LBB1_1498:
	test	r9b, 1
	je	.LBB1_1500
# %bb.1499:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1500:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1501
.LBB1_1505:
	xor	esi, esi
.LBB1_1506:
	test	r9b, 1
	je	.LBB1_1508
# %bb.1507:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_5] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB1_1508:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1509
.LBB1_1513:
	xor	edi, edi
.LBB1_1514:
	test	r9b, 1
	je	.LBB1_1516
# %bb.1515:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1516:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1517
.LBB1_1521:
	xor	edi, edi
.LBB1_1522:
	test	r9b, 1
	je	.LBB1_1524
# %bb.1523:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1524:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1525
.LBB1_1529:
	xor	edi, edi
.LBB1_1530:
	test	r9b, 1
	je	.LBB1_1532
# %bb.1531:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1532:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1533
.LBB1_1537:
	xor	edi, edi
.LBB1_1538:
	test	r9b, 1
	je	.LBB1_1540
# %bb.1539:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_1540:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1541
.LBB1_1545:
	xor	edi, edi
.LBB1_1546:
	test	r9b, 1
	je	.LBB1_1548
# %bb.1547:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1548:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1549
.LBB1_1553:
	xor	edi, edi
.LBB1_1554:
	test	r9b, 1
	je	.LBB1_1556
# %bb.1555:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1556:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1557
.LBB1_1563:
	xor	edi, edi
.LBB1_1564:
	test	r9b, 1
	je	.LBB1_1566
# %bb.1565:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1566:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1567
.LBB1_1571:
	xor	edi, edi
.LBB1_1572:
	test	r9b, 1
	je	.LBB1_1574
# %bb.1573:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1574:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1575
.LBB1_1581:
	xor	edi, edi
.LBB1_1582:
	test	r9b, 1
	je	.LBB1_1584
# %bb.1583:
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
.LBB1_1584:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1585
.LBB1_1589:
	xor	esi, esi
.LBB1_1590:
	test	r9b, 1
	je	.LBB1_1592
# %bb.1591:
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
.LBB1_1592:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1593
.LBB1_1597:
	xor	edi, edi
.LBB1_1598:
	test	r9b, 1
	je	.LBB1_1600
# %bb.1599:
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
.LBB1_1600:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1601
.LBB1_1605:
	xor	esi, esi
.LBB1_1606:
	test	r9b, 1
	je	.LBB1_1608
# %bb.1607:
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
.LBB1_1608:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1609
.LBB1_1613:
	xor	edi, edi
.LBB1_1614:
	test	r9b, 1
	je	.LBB1_1616
# %bb.1615:
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
.LBB1_1616:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1617
.LBB1_1621:
	xor	edi, edi
.LBB1_1622:
	test	r9b, 1
	je	.LBB1_1624
# %bb.1623:
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1624:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1625
.LBB1_1629:
	xor	edi, edi
.LBB1_1630:
	test	r9b, 1
	je	.LBB1_1632
# %bb.1631:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1632:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1633
.LBB1_1639:
	xor	edi, edi
.LBB1_1640:
	test	r9b, 1
	je	.LBB1_1642
# %bb.1641:
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
.LBB1_1642:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1643
.LBB1_1647:
	xor	edi, edi
.LBB1_1648:
	test	r9b, 1
	je	.LBB1_1650
# %bb.1649:
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1650:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1651
.LBB1_1655:
	xor	edi, edi
.LBB1_1656:
	test	r9b, 1
	je	.LBB1_1658
# %bb.1657:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI1_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1658:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1659
.LBB1_1665:
	xor	edi, edi
.LBB1_1666:
	test	r9b, 1
	je	.LBB1_1668
# %bb.1667:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1668:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1669
.LBB1_1673:
	xor	edi, edi
.LBB1_1674:
	test	r9b, 1
	je	.LBB1_1676
# %bb.1675:
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1676:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1677
.LBB1_1681:
	xor	edi, edi
.LBB1_1682:
	test	r9b, 1
	je	.LBB1_1684
# %bb.1683:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1684:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1685
.LBB1_1689:
	xor	edi, edi
.LBB1_1690:
	test	r9b, 1
	je	.LBB1_1692
# %bb.1691:
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1692:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1693
.LBB1_1697:
	xor	edi, edi
.LBB1_1698:
	test	r9b, 1
	je	.LBB1_1700
# %bb.1699:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1700:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1701
.LBB1_1705:
	xor	edi, edi
.LBB1_1706:
	test	r9b, 1
	je	.LBB1_1708
# %bb.1707:
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
.LBB1_1708:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1709
.LBB1_1713:
	xor	edi, edi
.LBB1_1714:
	test	r9b, 1
	je	.LBB1_1716
# %bb.1715:
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
.LBB1_1716:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1717
.LBB1_1721:
	xor	edi, edi
.LBB1_1722:
	test	r9b, 1
	je	.LBB1_1724
# %bb.1723:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB1_1724:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1725
.LBB1_1729:
	xor	edi, edi
.LBB1_1730:
	test	r9b, 1
	je	.LBB1_1732
# %bb.1731:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB1_1732:
	cmp	rcx, r10
	je	.LBB1_1817
	jmp	.LBB1_1733
.LBB1_1737:
	xor	edi, edi
.LBB1_1738:
	test	r9b, 1
	je	.LBB1_1740
# %bb.1739:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1740:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1741
.LBB1_1745:
	xor	edi, edi
.LBB1_1746:
	test	r9b, 1
	je	.LBB1_1748
# %bb.1747:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1748:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1749
.LBB1_1753:
	xor	edi, edi
.LBB1_1754:
	test	r9b, 1
	je	.LBB1_1756
# %bb.1755:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1756:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1757
.LBB1_1761:
	xor	edi, edi
.LBB1_1762:
	test	r9b, 1
	je	.LBB1_1764
# %bb.1763:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1764:
	cmp	rcx, rax
	je	.LBB1_1817
	jmp	.LBB1_1765
.LBB1_1769:
	xor	edi, edi
.LBB1_1770:
	test	r9b, 1
	je	.LBB1_1772
# %bb.1771:
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
.LBB1_1772:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1773
.LBB1_1777:
	xor	edi, edi
.LBB1_1778:
	test	r9b, 1
	je	.LBB1_1780
# %bb.1779:
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
.LBB1_1780:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1781
.LBB1_1785:
	xor	edi, edi
.LBB1_1786:
	test	r9b, 1
	je	.LBB1_1788
# %bb.1787:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1788:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1789
.LBB1_1793:
	xor	edi, edi
.LBB1_1794:
	test	r9b, 1
	je	.LBB1_1796
# %bb.1795:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1796:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1797
.LBB1_1801:
	xor	edi, edi
.LBB1_1802:
	test	r9b, 1
	je	.LBB1_1804
# %bb.1803:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1804:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1805
.LBB1_1809:
	xor	edi, edi
.LBB1_1810:
	test	r9b, 1
	je	.LBB1_1812
# %bb.1811:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1812:
	cmp	rsi, r10
	je	.LBB1_1817
	jmp	.LBB1_1813
.Lfunc_end1:
	.size	arithmetic_arr_scalar_avx2, .Lfunc_end1-arithmetic_arr_scalar_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_scalar_arr_avx2
.LCPI2_0:
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI2_3:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI2_1:
	.long	0x00000000
	.long	0x80000000              # double -0
	.long	0x00000000
	.long	0x80000000              # double -0
.LCPI2_6:
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
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI2_2:
	.long	0x80000000                      # float -0
.LCPI2_4:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI2_5:
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
.LCPI2_7:
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
	.text
	.globl	arithmetic_scalar_arr_avx2
	.p2align	4, 0x90
	.type	arithmetic_scalar_arr_avx2,@function
arithmetic_scalar_arr_avx2:             # @arithmetic_scalar_arr_avx2
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
	jne	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.10:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.11:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_12
# %bb.443:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_739
# %bb.444:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_739
.LBB2_12:
	xor	ecx, ecx
.LBB2_1137:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1139
.LBB2_1138:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1138
.LBB2_1139:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1140:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1140
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.22:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.23:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB2_446
# %bb.24:
	xor	ecx, ecx
	jmp	.LBB2_748
.LBB2_25:
	test	sil, sil
	je	.LBB2_77
# %bb.26:
	cmp	sil, 1
	jne	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.32:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.33:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_34
# %bb.448:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_749
# %bb.449:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_749
.LBB2_34:
	xor	esi, esi
.LBB2_1145:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1147
.LBB2_1146:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1146
.LBB2_1147:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1148:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1148
	jmp	.LBB2_1817
.LBB2_35:
	cmp	sil, 7
	je	.LBB2_85
# %bb.36:
	cmp	sil, 8
	jne	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.42:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.43:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_44
# %bb.451:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_752
# %bb.452:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_752
.LBB2_44:
	xor	esi, esi
.LBB2_1153:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1155
.LBB2_1154:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1154
.LBB2_1155:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1156:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1156
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.50:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.51:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_52
# %bb.454:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_755
# %bb.455:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_755
.LBB2_52:
	xor	esi, esi
.LBB2_1161:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1163
.LBB2_1162:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1162
.LBB2_1163:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1164:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1164
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.58:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.59:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_60
# %bb.457:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_758
# %bb.458:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_758
.LBB2_60:
	xor	ecx, ecx
.LBB2_1169:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1171
.LBB2_1170:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1170
.LBB2_1171:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1172:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1172
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.66:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.67:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_68
# %bb.460:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_761
# %bb.461:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_761
.LBB2_68:
	xor	esi, esi
.LBB2_1177:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1179
.LBB2_1178:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1178
.LBB2_1179:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1180:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1180
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.74:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.75:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_76
# %bb.463:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_764
# %bb.464:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_764
.LBB2_76:
	xor	ecx, ecx
.LBB2_1185:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1187
.LBB2_1186:                             # =>This Inner Loop Header: Depth=1
	mov	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1186
.LBB2_1187:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1188:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1188
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.82:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.83:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_84
# %bb.466:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_767
# %bb.467:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_767
.LBB2_84:
	xor	esi, esi
.LBB2_1193:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1195
.LBB2_1194:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1194
.LBB2_1195:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1196:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1196
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.90:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.91:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_92
# %bb.469:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_770
# %bb.470:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_770
.LBB2_92:
	xor	esi, esi
.LBB2_1201:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1203
.LBB2_1202:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1202
.LBB2_1203:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1204:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1204
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.97:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.98:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_99
# %bb.472:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_773
# %bb.473:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_773
.LBB2_99:
	xor	ecx, ecx
.LBB2_1209:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1212
# %bb.1210:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1211:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1211
.LBB2_1212:
	cmp	rsi, 3
	jb	.LBB2_1817
# %bb.1213:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1214:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1214
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.104:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.105:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_106
# %bb.475:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_776
# %bb.476:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_776
.LBB2_106:
	xor	ecx, ecx
.LBB2_1219:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1222
# %bb.1220:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1221:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1221
.LBB2_1222:
	cmp	rsi, 3
	jb	.LBB2_1817
# %bb.1223:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB2_1224:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 8] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 8], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 16] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 16], xmm1
	vmovq	xmm1, qword ptr [rdx + 8*rcx + 24] # xmm1 = mem[0],zero
	vpxor	xmm1, xmm1, xmm0
	vmovq	qword ptr [r8 + 8*rcx + 24], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1224
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.111:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.112:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_113
# %bb.478:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_779
# %bb.479:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_779
.LBB2_113:
	xor	edx, edx
.LBB2_1229:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1231
.LBB2_1230:                             # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1230
.LBB2_1231:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1232:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1232
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.118:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.119:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_120
# %bb.481:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_782
# %bb.482:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_782
.LBB2_120:
	xor	edx, edx
.LBB2_1237:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1239
.LBB2_1238:                             # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1238
.LBB2_1239:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1240:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1240
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.125:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.126:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_127
# %bb.484:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_785
# %bb.485:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_785
.LBB2_127:
	xor	edx, edx
.LBB2_1245:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1247
.LBB2_1246:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1246
.LBB2_1247:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1248:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1248
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.132:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.133:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_134
# %bb.487:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_788
# %bb.488:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_788
.LBB2_134:
	xor	ecx, ecx
.LBB2_1253:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_1255
.LBB2_1254:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_1254
.LBB2_1255:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1256:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1256
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.139:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.140:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_141
# %bb.490:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_791
# %bb.491:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_791
.LBB2_141:
	xor	edx, edx
.LBB2_1261:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1263
.LBB2_1262:                             # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1262
.LBB2_1263:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1264:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1264
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.146:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.147:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_148
# %bb.493:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_794
# %bb.494:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_794
.LBB2_148:
	xor	ecx, ecx
.LBB2_1269:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_1271
.LBB2_1270:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_1270
.LBB2_1271:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1272:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1272
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.153:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.154:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_155
# %bb.496:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_797
# %bb.497:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_797
.LBB2_155:
	xor	edx, edx
.LBB2_1277:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1279
.LBB2_1278:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1278
.LBB2_1279:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1280:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1280
	jmp	.LBB2_1817
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
	jne	.LBB2_1817
# %bb.160:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.161:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_162
# %bb.499:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_800
# %bb.500:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_800
.LBB2_162:
	xor	edx, edx
.LBB2_1285:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1287
.LBB2_1286:                             # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1286
.LBB2_1287:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1288:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1288
	jmp	.LBB2_1817
.LBB2_163:
	cmp	edi, 2
	je	.LBB2_383
# %bb.164:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.165:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.166:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_167
# %bb.502:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_803
# %bb.503:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_803
.LBB2_167:
	xor	ecx, ecx
.LBB2_1293:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1295
.LBB2_1294:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1294
.LBB2_1295:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1296:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1296
	jmp	.LBB2_1817
.LBB2_168:
	cmp	edi, 2
	je	.LBB2_386
# %bb.169:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.170:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.171:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_172
# %bb.505:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_806
# %bb.506:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_806
.LBB2_172:
	xor	ecx, ecx
.LBB2_1301:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1303
.LBB2_1302:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1302
.LBB2_1303:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1304:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1304
	jmp	.LBB2_1817
.LBB2_173:
	cmp	edi, 2
	je	.LBB2_389
# %bb.174:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.175:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.176:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_177
# %bb.508:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_809
# %bb.509:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_809
.LBB2_177:
	xor	esi, esi
.LBB2_1309:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1311
.LBB2_1310:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1310
.LBB2_1311:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1312:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1312
	jmp	.LBB2_1817
.LBB2_178:
	cmp	edi, 2
	je	.LBB2_392
# %bb.179:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.180:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.181:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_182
# %bb.511:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_812
# %bb.512:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_812
.LBB2_182:
	xor	esi, esi
.LBB2_1317:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1319
.LBB2_1318:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1318
.LBB2_1319:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1320:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1320
	jmp	.LBB2_1817
.LBB2_183:
	cmp	edi, 2
	je	.LBB2_395
# %bb.184:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.185:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.186:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_187
# %bb.514:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_815
# %bb.515:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_815
.LBB2_187:
	xor	edi, edi
.LBB2_1079:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1081
.LBB2_1080:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1080
.LBB2_1081:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1082:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1082
	jmp	.LBB2_1817
.LBB2_188:
	cmp	edi, 2
	je	.LBB2_398
# %bb.189:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.190:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.191:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_192
# %bb.517:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB2_817
# %bb.518:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB2_817
.LBB2_192:
	xor	ecx, ecx
.LBB2_820:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_822
# %bb.821:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_822:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_823:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_823
	jmp	.LBB2_1817
.LBB2_193:
	cmp	edi, 2
	je	.LBB2_401
# %bb.194:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.195:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.196:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_197
# %bb.520:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_824
# %bb.521:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_824
.LBB2_197:
	xor	edi, edi
.LBB2_1089:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1091
.LBB2_1090:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1090
.LBB2_1091:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1092:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1092
	jmp	.LBB2_1817
.LBB2_198:
	cmp	edi, 2
	je	.LBB2_404
# %bb.199:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.200:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.201:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_202
# %bb.523:
	lea	rcx, [rdx + rax]
	cmp	rcx, r8
	jbe	.LBB2_826
# %bb.524:
	lea	rcx, [r8 + rax]
	cmp	rcx, rdx
	jbe	.LBB2_826
.LBB2_202:
	xor	ecx, ecx
.LBB2_829:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_831
# %bb.830:
	movsx	edi, byte ptr [rdx + rcx]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_831:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_832:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_832
	jmp	.LBB2_1817
.LBB2_203:
	cmp	edi, 2
	je	.LBB2_407
# %bb.204:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.205:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.206:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
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
.LBB2_1325:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1327
.LBB2_1326:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1326
.LBB2_1327:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1328:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1328
	jmp	.LBB2_1817
.LBB2_208:
	cmp	edi, 2
	je	.LBB2_410
# %bb.209:
	cmp	edi, 3
	jne	.LBB2_1817
# %bb.210:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.211:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
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
.LBB2_1333:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1335
.LBB2_1334:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1334
.LBB2_1335:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1336:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1336
	jmp	.LBB2_1817
.LBB2_213:
	cmp	edi, 7
	je	.LBB2_413
# %bb.214:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.215:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.216:
	mov	eax, r9d
	cmp	r9d, 16
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
.LBB2_1341:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1343
.LBB2_1342:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1342
.LBB2_1343:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1344:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1344
	jmp	.LBB2_1817
.LBB2_218:
	cmp	edi, 7
	je	.LBB2_416
# %bb.219:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.220:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.221:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB2_535
# %bb.222:
	xor	ecx, ecx
	jmp	.LBB2_848
.LBB2_223:
	cmp	edi, 7
	je	.LBB2_419
# %bb.224:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.225:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.226:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
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
.LBB2_1349:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1351
.LBB2_1350:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1350
.LBB2_1351:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1352:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1352
	jmp	.LBB2_1817
.LBB2_228:
	cmp	edi, 7
	je	.LBB2_422
# %bb.229:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.230:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.231:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
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
.LBB2_1357:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1359
.LBB2_1358:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1358
.LBB2_1359:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1360:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1360
	jmp	.LBB2_1817
.LBB2_233:
	cmp	edi, 7
	je	.LBB2_425
# %bb.234:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.235:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.236:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_237
# %bb.543:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_855
# %bb.544:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_855
.LBB2_237:
	xor	esi, esi
.LBB2_1365:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1367
.LBB2_1366:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1366
.LBB2_1367:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1368:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1368
	jmp	.LBB2_1817
.LBB2_238:
	cmp	edi, 7
	je	.LBB2_428
# %bb.239:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.240:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.241:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_242
# %bb.546:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_858
# %bb.547:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_858
.LBB2_242:
	xor	ecx, ecx
.LBB2_1373:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1375
.LBB2_1374:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1374
.LBB2_1375:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1376:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1376
	jmp	.LBB2_1817
.LBB2_243:
	cmp	edi, 7
	je	.LBB2_431
# %bb.244:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.245:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.246:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_247
# %bb.549:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_861
# %bb.550:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_861
.LBB2_247:
	xor	esi, esi
.LBB2_1381:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1383
.LBB2_1382:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1382
.LBB2_1383:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1384:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1384
	jmp	.LBB2_1817
.LBB2_248:
	cmp	edi, 7
	je	.LBB2_434
# %bb.249:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.250:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.251:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_252
# %bb.552:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_864
# %bb.553:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_864
.LBB2_252:
	xor	ecx, ecx
.LBB2_1389:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1391
.LBB2_1390:                             # =>This Inner Loop Header: Depth=1
	mov	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1390
.LBB2_1391:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1392:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1392
	jmp	.LBB2_1817
.LBB2_253:
	cmp	edi, 7
	je	.LBB2_437
# %bb.254:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.255:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.256:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_257
# %bb.555:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_867
# %bb.556:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_867
.LBB2_257:
	xor	esi, esi
.LBB2_1397:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1399
.LBB2_1398:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1398
.LBB2_1399:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1400:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1400
	jmp	.LBB2_1817
.LBB2_258:
	cmp	edi, 7
	je	.LBB2_440
# %bb.259:
	cmp	edi, 8
	jne	.LBB2_1817
# %bb.260:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.261:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_262
# %bb.558:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_870
# %bb.559:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_870
.LBB2_262:
	xor	esi, esi
.LBB2_1405:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1407
.LBB2_1406:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1406
.LBB2_1407:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1408:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1408
	jmp	.LBB2_1817
.LBB2_263:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.264:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_265
# %bb.561:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_873
# %bb.562:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_873
.LBB2_265:
	xor	ecx, ecx
.LBB2_1413:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1415
.LBB2_1414:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1414
.LBB2_1415:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1416:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1416
	jmp	.LBB2_1817
.LBB2_266:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.267:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_268
# %bb.564:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_876
# %bb.565:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_876
.LBB2_268:
	xor	ecx, ecx
.LBB2_1421:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1423
.LBB2_1422:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1422
.LBB2_1423:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1424:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1424
	jmp	.LBB2_1817
.LBB2_269:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.270:
	mov	eax, r9d
	cmp	r9d, 64
	jae	.LBB2_567
# %bb.271:
	xor	ecx, ecx
	jmp	.LBB2_885
.LBB2_272:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.273:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_274
# %bb.569:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_886
# %bb.570:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_886
.LBB2_274:
	xor	ecx, ecx
.LBB2_1429:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1431
.LBB2_1430:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	si, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], si
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1430
.LBB2_1431:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1432:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1432
	jmp	.LBB2_1817
.LBB2_275:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.276:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_277
# %bb.572:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_889
# %bb.573:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_889
.LBB2_277:
	xor	esi, esi
.LBB2_1437:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1439
.LBB2_1438:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1438
.LBB2_1439:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1440:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1440
	jmp	.LBB2_1817
.LBB2_278:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.279:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_280
# %bb.575:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_892
# %bb.576:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_892
.LBB2_280:
	xor	esi, esi
.LBB2_1445:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1447
.LBB2_1446:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1446
.LBB2_1447:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1448:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1448
	jmp	.LBB2_1817
.LBB2_281:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.282:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_283
# %bb.578:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_895
# %bb.579:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_895
.LBB2_283:
	xor	esi, esi
.LBB2_1453:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1455
.LBB2_1454:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1454
.LBB2_1455:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1456:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1456
	jmp	.LBB2_1817
.LBB2_284:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.285:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_286
# %bb.581:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_898
# %bb.582:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_898
.LBB2_286:
	xor	esi, esi
.LBB2_1461:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1463
.LBB2_1462:                             # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1462
.LBB2_1463:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1464:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1464
	jmp	.LBB2_1817
.LBB2_287:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.288:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_289
# %bb.584:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_901
# %bb.585:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_901
.LBB2_289:
	xor	esi, esi
.LBB2_1469:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1471
.LBB2_1470:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1470
.LBB2_1471:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1472:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1472
	jmp	.LBB2_1817
.LBB2_290:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.291:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_292
# %bb.587:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_904
# %bb.588:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_904
.LBB2_292:
	xor	esi, esi
.LBB2_1477:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1479
.LBB2_1478:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1478
.LBB2_1479:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1480:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1480
	jmp	.LBB2_1817
.LBB2_293:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.294:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_295
# %bb.590:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_907
# %bb.591:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_907
.LBB2_295:
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
	jb	.LBB2_1817
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
	jmp	.LBB2_1817
.LBB2_296:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.297:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_298
# %bb.593:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_909
# %bb.594:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_909
.LBB2_298:
	xor	ecx, ecx
.LBB2_1485:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1487
# %bb.1486:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1487:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_1488:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1488
	jmp	.LBB2_1817
.LBB2_299:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.300:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_301
# %bb.596:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_912
# %bb.597:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_912
.LBB2_301:
	xor	esi, esi
.LBB2_1493:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1495
.LBB2_1494:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1494
.LBB2_1495:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1496:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1496
	jmp	.LBB2_1817
.LBB2_302:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.303:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_304
# %bb.599:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_915
# %bb.600:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_915
.LBB2_304:
	xor	esi, esi
.LBB2_1501:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1503
.LBB2_1502:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1502
.LBB2_1503:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1504:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1504
	jmp	.LBB2_1817
.LBB2_305:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.306:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_307
# %bb.602:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_918
# %bb.603:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_918
.LBB2_307:
	xor	ecx, ecx
.LBB2_1109:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1111
.LBB2_1110:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_1110
.LBB2_1111:
	cmp	rax, 3
	jb	.LBB2_1817
.LBB2_1112:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1112
	jmp	.LBB2_1817
.LBB2_308:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.309:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_310
# %bb.605:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_920
# %bb.606:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_920
.LBB2_310:
	xor	ecx, ecx
.LBB2_1509:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1511
# %bb.1510:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1511:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_1512:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1512
	jmp	.LBB2_1817
.LBB2_311:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.312:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_313
# %bb.608:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_923
# %bb.609:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_923
.LBB2_313:
	xor	esi, esi
.LBB2_1517:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1519
.LBB2_1518:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1518
.LBB2_1519:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1520:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1520
	jmp	.LBB2_1817
.LBB2_314:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.315:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_316
# %bb.611:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_926
# %bb.612:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_926
.LBB2_316:
	xor	esi, esi
.LBB2_1525:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1527
.LBB2_1526:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1526
.LBB2_1527:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1528:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1528
	jmp	.LBB2_1817
.LBB2_317:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.318:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_319
# %bb.614:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_929
# %bb.615:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_929
.LBB2_319:
	xor	esi, esi
.LBB2_1533:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1535
.LBB2_1534:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1534
.LBB2_1535:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1536:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1536
	jmp	.LBB2_1817
.LBB2_320:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.321:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_322
# %bb.617:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_932
# %bb.618:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_932
.LBB2_322:
	xor	esi, esi
.LBB2_1541:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_1543
.LBB2_1542:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_1542
.LBB2_1543:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1544:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1544
	jmp	.LBB2_1817
.LBB2_323:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.324:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_325
# %bb.620:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_935
# %bb.621:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_935
.LBB2_325:
	xor	ecx, ecx
.LBB2_1549:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1551
.LBB2_1550:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1550
.LBB2_1551:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1552:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1552
	jmp	.LBB2_1817
.LBB2_326:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.327:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_328
# %bb.623:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_938
# %bb.624:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_938
.LBB2_328:
	xor	ecx, ecx
.LBB2_1557:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1560
# %bb.1558:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI2_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1559:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1559
.LBB2_1560:
	cmp	rsi, 3
	jb	.LBB2_1817
# %bb.1561:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI2_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1562:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1562
	jmp	.LBB2_1817
.LBB2_329:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.330:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_331
# %bb.626:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_941
# %bb.627:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_941
.LBB2_331:
	xor	ecx, ecx
.LBB2_1567:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1569
.LBB2_1568:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	rsi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rsi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1568
.LBB2_1569:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1570:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1570
	jmp	.LBB2_1817
.LBB2_332:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.333:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_334
# %bb.629:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_944
# %bb.630:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_944
.LBB2_334:
	xor	ecx, ecx
.LBB2_1575:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1578
# %bb.1576:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI2_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1577:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1577
.LBB2_1578:
	cmp	rsi, 3
	jb	.LBB2_1817
# %bb.1579:
	vpbroadcastd	xmm0, dword ptr [rip + .LCPI2_2] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_1580:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 4] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 4], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 8] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 8], xmm1
	vmovd	xmm1, dword ptr [rdx + 4*rcx + 12] # xmm1 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm0
	vmovd	dword ptr [r8 + 4*rcx + 12], xmm1
	add	rcx, 4
	cmp	rax, rcx
	jne	.LBB2_1580
	jmp	.LBB2_1817
.LBB2_335:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.336:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_337
# %bb.632:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_947
# %bb.633:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_947
.LBB2_337:
	xor	esi, esi
.LBB2_1585:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1587
.LBB2_1586:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1586
.LBB2_1587:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1588:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1588
	jmp	.LBB2_1817
.LBB2_338:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.339:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_340
# %bb.635:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_950
# %bb.636:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_950
.LBB2_340:
	xor	edx, edx
.LBB2_1593:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1595
.LBB2_1594:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1594
.LBB2_1595:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1596:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1596
	jmp	.LBB2_1817
.LBB2_341:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.342:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_343
# %bb.638:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_953
# %bb.639:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_953
.LBB2_343:
	xor	esi, esi
.LBB2_1601:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1603
.LBB2_1602:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1602
.LBB2_1603:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1604:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1604
	jmp	.LBB2_1817
.LBB2_344:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.345:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_346
# %bb.641:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_956
# %bb.642:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_956
.LBB2_346:
	xor	edx, edx
.LBB2_1609:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1611
.LBB2_1610:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1610
.LBB2_1611:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1612:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1612
	jmp	.LBB2_1817
.LBB2_347:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.348:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_349
# %bb.644:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_959
# %bb.645:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_959
.LBB2_349:
	xor	esi, esi
.LBB2_1617:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1619
.LBB2_1618:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1618
.LBB2_1619:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1620:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1620
	jmp	.LBB2_1817
.LBB2_350:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.351:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_352
# %bb.647:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_962
# %bb.648:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_962
.LBB2_352:
	xor	edx, edx
.LBB2_1625:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1627
.LBB2_1626:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1626
.LBB2_1627:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1628:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1628
	jmp	.LBB2_1817
.LBB2_353:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.354:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_355
# %bb.650:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_965
# %bb.651:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_965
.LBB2_355:
	xor	ecx, ecx
.LBB2_968:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_970
# %bb.969:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_970:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_971:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_971
	jmp	.LBB2_1817
.LBB2_356:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.357:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_358
# %bb.653:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_972
# %bb.654:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_972
.LBB2_358:
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
	jb	.LBB2_1817
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
	jmp	.LBB2_1817
.LBB2_359:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.360:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_361
# %bb.656:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_975
# %bb.657:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_975
.LBB2_361:
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
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1644
.LBB2_1645:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1646:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1646
	jmp	.LBB2_1817
.LBB2_362:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.363:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_364
# %bb.659:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_978
# %bb.660:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_978
.LBB2_364:
	xor	edx, edx
.LBB2_1651:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1653
.LBB2_1652:                             # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1652
.LBB2_1653:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1654:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1654
	jmp	.LBB2_1817
.LBB2_365:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.366:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_367
# %bb.662:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_981
# %bb.663:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_981
.LBB2_367:
	xor	ecx, ecx
.LBB2_984:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_986
# %bb.985:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_986:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_987:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_987
	jmp	.LBB2_1817
.LBB2_368:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.369:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_370
# %bb.665:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_988
# %bb.666:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_988
.LBB2_370:
	xor	ecx, ecx
.LBB2_1659:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1662
# %bb.1660:
	mov	esi, 2147483647
.LBB2_1661:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1661
.LBB2_1662:
	cmp	r9, 3
	jb	.LBB2_1817
# %bb.1663:
	mov	esi, 2147483647
.LBB2_1664:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1664
	jmp	.LBB2_1817
.LBB2_371:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.372:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_373
# %bb.668:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_991
# %bb.669:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_991
.LBB2_373:
	xor	esi, esi
.LBB2_1669:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1671
.LBB2_1670:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1670
.LBB2_1671:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1672:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1672
	jmp	.LBB2_1817
.LBB2_374:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.375:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_376
# %bb.671:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_994
# %bb.672:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_994
.LBB2_376:
	xor	edx, edx
.LBB2_1677:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1679
.LBB2_1678:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1678
.LBB2_1679:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1680:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1680
	jmp	.LBB2_1817
.LBB2_377:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.378:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_379
# %bb.674:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_997
# %bb.675:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_997
.LBB2_379:
	xor	esi, esi
.LBB2_1685:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1687
.LBB2_1686:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1686
.LBB2_1687:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1688:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1688
	jmp	.LBB2_1817
.LBB2_380:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.381:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_382
# %bb.677:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_1000
# %bb.678:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_1000
.LBB2_382:
	xor	edx, edx
.LBB2_1693:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1695
.LBB2_1694:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1694
.LBB2_1695:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1696:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1696
	jmp	.LBB2_1817
.LBB2_383:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.384:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_385
# %bb.680:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_1003
# %bb.681:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_1003
.LBB2_385:
	xor	ecx, ecx
.LBB2_1701:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1703
.LBB2_1702:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	xor	esi, esi
	sub	sil, al
	mov	byte ptr [r8 + rcx], sil
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1702
.LBB2_1703:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1704:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1704
	jmp	.LBB2_1817
.LBB2_386:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.387:
	mov	eax, r9d
	cmp	r9d, 128
	jae	.LBB2_683
# %bb.388:
	xor	ecx, ecx
	jmp	.LBB2_1012
.LBB2_389:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.390:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_391
# %bb.685:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_1013
# %bb.686:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_1013
.LBB2_391:
	xor	esi, esi
.LBB2_1709:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1711
.LBB2_1710:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1710
.LBB2_1711:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1712:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1712
	jmp	.LBB2_1817
.LBB2_392:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.393:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_394
# %bb.688:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_1016
# %bb.689:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_1016
.LBB2_394:
	xor	esi, esi
.LBB2_1717:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1719
.LBB2_1718:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1718
.LBB2_1719:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1720:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1720
	jmp	.LBB2_1817
.LBB2_395:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.396:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_397
# %bb.691:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_1019
# %bb.692:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_1019
.LBB2_397:
	xor	edi, edi
.LBB2_1119:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1121
.LBB2_1120:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1120
.LBB2_1121:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1122:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1122
	jmp	.LBB2_1817
.LBB2_398:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.399:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_400
# %bb.694:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_1021
# %bb.695:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_1021
.LBB2_400:
	xor	ecx, ecx
.LBB2_1725:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1727
.LBB2_1726:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1726
.LBB2_1727:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1728:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1728
	jmp	.LBB2_1817
.LBB2_401:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.402:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_403
# %bb.697:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_1024
# %bb.698:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_1024
.LBB2_403:
	xor	edi, edi
.LBB2_1129:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1131
.LBB2_1130:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1130
.LBB2_1131:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1132:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1132
	jmp	.LBB2_1817
.LBB2_404:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.405:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_406
# %bb.700:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_1026
# %bb.701:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_1026
.LBB2_406:
	xor	ecx, ecx
.LBB2_1733:
	mov	rsi, rcx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1735
.LBB2_1734:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1734
.LBB2_1735:
	cmp	rsi, 3
	jb	.LBB2_1817
.LBB2_1736:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1736
	jmp	.LBB2_1817
.LBB2_407:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.408:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_409
# %bb.703:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_1029
# %bb.704:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_1029
.LBB2_409:
	xor	esi, esi
.LBB2_1741:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1743
.LBB2_1742:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1742
.LBB2_1743:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1744:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1744
	jmp	.LBB2_1817
.LBB2_410:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.411:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_412
# %bb.706:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_1032
# %bb.707:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_1032
.LBB2_412:
	xor	esi, esi
.LBB2_1749:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1751
.LBB2_1750:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1750
.LBB2_1751:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1752:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1752
	jmp	.LBB2_1817
.LBB2_413:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.414:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_415
# %bb.709:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1035
# %bb.710:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1035
.LBB2_415:
	xor	ecx, ecx
.LBB2_1757:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1759
.LBB2_1758:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1758
.LBB2_1759:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1760:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1760
	jmp	.LBB2_1817
.LBB2_416:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.417:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_418
# %bb.712:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1038
# %bb.713:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1038
.LBB2_418:
	xor	ecx, ecx
.LBB2_1765:
	mov	r9, rcx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1767
.LBB2_1766:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	sub	esi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], esi
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1766
.LBB2_1767:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1768:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1768
	jmp	.LBB2_1817
.LBB2_419:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.420:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_421
# %bb.715:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1041
# %bb.716:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1041
.LBB2_421:
	xor	esi, esi
.LBB2_1773:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1775
.LBB2_1774:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1774
.LBB2_1775:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1776:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1776
	jmp	.LBB2_1817
.LBB2_422:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.423:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_424
# %bb.718:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1044
# %bb.719:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1044
.LBB2_424:
	xor	esi, esi
.LBB2_1781:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1783
.LBB2_1782:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1782
.LBB2_1783:
	cmp	rdx, 3
	jb	.LBB2_1817
.LBB2_1784:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1784
	jmp	.LBB2_1817
.LBB2_425:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.426:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_427
# %bb.721:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1047
# %bb.722:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1047
.LBB2_427:
	xor	esi, esi
.LBB2_1789:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1791
.LBB2_1790:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1790
.LBB2_1791:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1792:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1792
	jmp	.LBB2_1817
.LBB2_428:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.429:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_430
# %bb.724:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1050
# %bb.725:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1050
.LBB2_430:
	xor	ecx, ecx
.LBB2_1053:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1055
# %bb.1054:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_1055:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_1056:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1056
	jmp	.LBB2_1817
.LBB2_431:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.432:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_433
# %bb.727:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1057
# %bb.728:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1057
.LBB2_433:
	xor	esi, esi
.LBB2_1797:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1799
.LBB2_1798:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1798
.LBB2_1799:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1800:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1800
	jmp	.LBB2_1817
.LBB2_434:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.435:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_436
# %bb.730:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_1060
# %bb.731:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_1060
.LBB2_436:
	xor	ecx, ecx
.LBB2_1063:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1065
# %bb.1064:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_1065:
	add	rsi, rax
	je	.LBB2_1817
.LBB2_1066:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1066
	jmp	.LBB2_1817
.LBB2_437:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.438:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_439
# %bb.733:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1067
# %bb.734:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1067
.LBB2_439:
	xor	esi, esi
.LBB2_1805:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1807
.LBB2_1806:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1806
.LBB2_1807:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1808:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1808
	jmp	.LBB2_1817
.LBB2_440:
	test	r9d, r9d
	jle	.LBB2_1817
# %bb.441:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_442
# %bb.736:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_1070
# %bb.737:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_1070
.LBB2_442:
	xor	esi, esi
.LBB2_1813:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1815
.LBB2_1814:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1814
.LBB2_1815:
	cmp	r9, 3
	jb	.LBB2_1817
.LBB2_1816:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1816
	jmp	.LBB2_1817
.LBB2_446:
	mov	ecx, eax
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 96
	jae	.LBB2_742
# %bb.447:
	xor	edi, edi
	jmp	.LBB2_744
.LBB2_535:
	mov	ecx, eax
	and	ecx, -16
	lea	rdi, [rcx - 16]
	mov	rsi, rdi
	shr	rsi, 4
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 48
	jae	.LBB2_842
# %bb.536:
	xor	edi, edi
	jmp	.LBB2_844
.LBB2_567:
	mov	ecx, eax
	and	ecx, -64
	lea	rdi, [rcx - 64]
	mov	rsi, rdi
	shr	rsi, 6
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 192
	jae	.LBB2_879
# %bb.568:
	xor	edi, edi
	jmp	.LBB2_881
.LBB2_683:
	mov	ecx, eax
	and	ecx, -128
	lea	rdi, [rcx - 128]
	mov	rsi, rdi
	shr	rsi, 7
	add	rsi, 1
	mov	edx, esi
	and	edx, 3
	cmp	rdi, 384
	jae	.LBB2_1006
# %bb.684:
	xor	edi, edi
	jmp	.LBB2_1008
.LBB2_739:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1133
# %bb.740:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_741:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_741
	jmp	.LBB2_1134
.LBB2_749:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1141
# %bb.750:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_751:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_751
	jmp	.LBB2_1142
.LBB2_752:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1149
# %bb.753:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_754:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_754
	jmp	.LBB2_1150
.LBB2_755:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1157
# %bb.756:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_757:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_757
	jmp	.LBB2_1158
.LBB2_758:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1165
# %bb.759:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_760:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_760
	jmp	.LBB2_1166
.LBB2_761:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1173
# %bb.762:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_763:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_763
	jmp	.LBB2_1174
.LBB2_764:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1181
# %bb.765:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_766:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_766
	jmp	.LBB2_1182
.LBB2_767:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1189
# %bb.768:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_769:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_769
	jmp	.LBB2_1190
.LBB2_770:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1197
# %bb.771:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_772:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_772
	jmp	.LBB2_1198
.LBB2_773:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1205
# %bb.774:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_775:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_775
	jmp	.LBB2_1206
.LBB2_776:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1215
# %bb.777:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_778:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_778
	jmp	.LBB2_1216
.LBB2_779:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1225
# %bb.780:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_781:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_781
	jmp	.LBB2_1226
.LBB2_782:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1233
# %bb.783:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_784:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_784
	jmp	.LBB2_1234
.LBB2_785:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1241
# %bb.786:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_787:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_787
	jmp	.LBB2_1242
.LBB2_788:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB2_1249
# %bb.789:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB2_790:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_790
	jmp	.LBB2_1250
.LBB2_791:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1257
# %bb.792:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_793:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_793
	jmp	.LBB2_1258
.LBB2_794:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB2_1265
# %bb.795:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB2_796:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_796
	jmp	.LBB2_1266
.LBB2_797:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1273
# %bb.798:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_799:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_799
	jmp	.LBB2_1274
.LBB2_800:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1281
# %bb.801:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_802:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_802
	jmp	.LBB2_1282
.LBB2_803:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1289
# %bb.804:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_805:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB2_805
	jmp	.LBB2_1290
.LBB2_806:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1297
# %bb.807:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_808:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB2_808
	jmp	.LBB2_1298
.LBB2_809:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1305
# %bb.810:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_811:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_811
	jmp	.LBB2_1306
.LBB2_812:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1313
# %bb.813:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_814:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_814
	jmp	.LBB2_1314
.LBB2_815:
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
	jae	.LBB2_1073
# %bb.816:
	xor	esi, esi
	jmp	.LBB2_1075
.LBB2_817:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_6] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB2_818:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_818
# %bb.819:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_820
.LBB2_824:
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
	jae	.LBB2_1083
# %bb.825:
	xor	esi, esi
	jmp	.LBB2_1085
.LBB2_826:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI2_6] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB2_827:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_827
# %bb.828:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_829
.LBB2_833:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1321
# %bb.834:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_835:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_835
	jmp	.LBB2_1322
.LBB2_836:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1329
# %bb.837:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_838:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_838
	jmp	.LBB2_1330
.LBB2_839:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1337
# %bb.840:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_841:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_841
	jmp	.LBB2_1338
.LBB2_849:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1345
# %bb.850:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_851:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_851
	jmp	.LBB2_1346
.LBB2_852:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1353
# %bb.853:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_854:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_854
	jmp	.LBB2_1354
.LBB2_855:
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
	je	.LBB2_1361
# %bb.856:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_857:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_857
	jmp	.LBB2_1362
.LBB2_858:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1369
# %bb.859:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_860:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_860
	jmp	.LBB2_1370
.LBB2_861:
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
	je	.LBB2_1377
# %bb.862:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_863:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_863
	jmp	.LBB2_1378
.LBB2_864:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1385
# %bb.865:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_866:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_866
	jmp	.LBB2_1386
.LBB2_867:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1393
# %bb.868:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_869:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_869
	jmp	.LBB2_1394
.LBB2_870:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1401
# %bb.871:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_872:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_872
	jmp	.LBB2_1402
.LBB2_873:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1409
# %bb.874:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_875:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_875
	jmp	.LBB2_1410
.LBB2_876:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1417
# %bb.877:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_878:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_878
	jmp	.LBB2_1418
.LBB2_886:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1425
# %bb.887:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_888:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_888
	jmp	.LBB2_1426
.LBB2_889:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1433
# %bb.890:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_891:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_891
	jmp	.LBB2_1434
.LBB2_892:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1441
# %bb.893:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_894:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_894
	jmp	.LBB2_1442
.LBB2_895:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1449
# %bb.896:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_897:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_897
	jmp	.LBB2_1450
.LBB2_898:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1457
# %bb.899:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_900:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_900
	jmp	.LBB2_1458
.LBB2_901:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1465
# %bb.902:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_903:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_903
	jmp	.LBB2_1466
.LBB2_904:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1473
# %bb.905:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_906:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_906
	jmp	.LBB2_1474
.LBB2_907:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB2_1093
# %bb.908:
	xor	eax, eax
	jmp	.LBB2_1095
.LBB2_909:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1481
# %bb.910:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_5] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB2_911:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_911
	jmp	.LBB2_1482
.LBB2_912:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1489
# %bb.913:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_914:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_914
	jmp	.LBB2_1490
.LBB2_915:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1497
# %bb.916:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_917:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_917
	jmp	.LBB2_1498
.LBB2_918:
	mov	ecx, r10d
	and	ecx, -32
	lea	rax, [rcx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB2_1103
# %bb.919:
	xor	eax, eax
	jmp	.LBB2_1105
.LBB2_920:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1505
# %bb.921:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI2_5] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB2_922:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_922
	jmp	.LBB2_1506
.LBB2_923:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1513
# %bb.924:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_925:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_925
	jmp	.LBB2_1514
.LBB2_926:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1521
# %bb.927:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_928:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_928
	jmp	.LBB2_1522
.LBB2_929:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1529
# %bb.930:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_931:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_931
	jmp	.LBB2_1530
.LBB2_932:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1537
# %bb.933:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_934:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_934
	jmp	.LBB2_1538
.LBB2_935:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1545
# %bb.936:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_937:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_937
	jmp	.LBB2_1546
.LBB2_938:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1553
# %bb.939:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_940:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_940
	jmp	.LBB2_1554
.LBB2_941:
	mov	ecx, eax
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1563
# %bb.942:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_943:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rsi, 2
	jne	.LBB2_943
	jmp	.LBB2_1564
.LBB2_944:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1571
# %bb.945:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB2_946:                              # =>This Inner Loop Header: Depth=1
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpxor	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_946
	jmp	.LBB2_1572
.LBB2_947:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1581
# %bb.948:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_949:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_949
	jmp	.LBB2_1582
.LBB2_950:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1589
# %bb.951:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_952:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_952
	jmp	.LBB2_1590
.LBB2_953:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1597
# %bb.954:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_955:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_955
	jmp	.LBB2_1598
.LBB2_956:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1605
# %bb.957:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_958:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_958
	jmp	.LBB2_1606
.LBB2_959:
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
	je	.LBB2_1613
# %bb.960:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_961:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_961
	jmp	.LBB2_1614
.LBB2_962:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1621
# %bb.963:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_964:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_964
	jmp	.LBB2_1622
.LBB2_965:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB2_966:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_966
# %bb.967:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_968
.LBB2_972:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1629
# %bb.973:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB2_974:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_974
	jmp	.LBB2_1630
.LBB2_975:
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
	je	.LBB2_1639
# %bb.976:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_977:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_977
	jmp	.LBB2_1640
.LBB2_978:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1647
# %bb.979:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_980
	jmp	.LBB2_1648
.LBB2_981:
	mov	ecx, eax
	and	ecx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
.LBB2_982:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_982
# %bb.983:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_984
.LBB2_988:
	mov	ecx, r10d
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1655
# %bb.989:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB2_990:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_990
	jmp	.LBB2_1656
.LBB2_991:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1665
# %bb.992:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_993:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_993
	jmp	.LBB2_1666
.LBB2_994:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1673
# %bb.995:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_996:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_996
	jmp	.LBB2_1674
.LBB2_997:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1681
# %bb.998:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_999:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_999
	jmp	.LBB2_1682
.LBB2_1000:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1689
# %bb.1001:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1002:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1002
	jmp	.LBB2_1690
.LBB2_1003:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1697
# %bb.1004:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_1005:                             # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm4
	add	rdi, 256
	add	rsi, 2
	jne	.LBB2_1005
	jmp	.LBB2_1698
.LBB2_1013:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1705
# %bb.1014:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1015:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1015
	jmp	.LBB2_1706
.LBB2_1016:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1713
# %bb.1017:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1018:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1018
	jmp	.LBB2_1714
.LBB2_1019:
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
	jae	.LBB2_1113
# %bb.1020:
	xor	esi, esi
	jmp	.LBB2_1115
.LBB2_1021:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1721
# %bb.1022:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1023:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1023
	jmp	.LBB2_1722
.LBB2_1024:
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
	jae	.LBB2_1123
# %bb.1025:
	xor	esi, esi
	jmp	.LBB2_1125
.LBB2_1026:
	mov	ecx, r10d
	and	ecx, -128
	lea	rsi, [rcx - 128]
	mov	r9, rsi
	shr	r9, 7
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1729
# %bb.1027:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_1028:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1028
	jmp	.LBB2_1730
.LBB2_1029:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1737
# %bb.1030:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1031:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1031
	jmp	.LBB2_1738
.LBB2_1032:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1745
# %bb.1033:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1034:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1034
	jmp	.LBB2_1746
.LBB2_1035:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1753
# %bb.1036:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_1037:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_1037
	jmp	.LBB2_1754
.LBB2_1038:
	mov	ecx, eax
	and	ecx, -32
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1761
# %bb.1039:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_1040:                             # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rsi, 2
	jne	.LBB2_1040
	jmp	.LBB2_1762
.LBB2_1041:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1769
# %bb.1042:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1043:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1043
	jmp	.LBB2_1770
.LBB2_1044:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1777
# %bb.1045:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1046:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1046
	jmp	.LBB2_1778
.LBB2_1047:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1785
# %bb.1048:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1049:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1049
	jmp	.LBB2_1786
.LBB2_1050:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB2_1051:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1051
# %bb.1052:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1053
.LBB2_1057:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1793
# %bb.1058:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1059:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1059
	jmp	.LBB2_1794
.LBB2_1060:
	mov	ecx, eax
	and	ecx, -32
	xor	esi, esi
.LBB2_1061:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1061
# %bb.1062:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1063
.LBB2_1067:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1801
# %bb.1068:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1069:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1069
	jmp	.LBB2_1802
.LBB2_1070:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1809
# %bb.1071:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_1072:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1072
	jmp	.LBB2_1810
.LBB2_742:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_743:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 480], ymm0
	sub	rdi, -128
	add	rsi, 4
	jne	.LBB2_743
.LBB2_744:
	test	rdx, rdx
	je	.LBB2_747
# %bb.745:
	lea	rsi, [r8 + 4*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB2_746:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB2_746
.LBB2_747:
	cmp	rcx, rax
	je	.LBB2_1817
.LBB2_748:                              # =>This Inner Loop Header: Depth=1
	mov	dword ptr [r8 + 4*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_748
	jmp	.LBB2_1817
.LBB2_842:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_843:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 480], ymm0
	add	rdi, 64
	add	rsi, 4
	jne	.LBB2_843
.LBB2_844:
	test	rdx, rdx
	je	.LBB2_847
# %bb.845:
	lea	rsi, [r8 + 8*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB2_846:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB2_846
.LBB2_847:
	cmp	rcx, rax
	je	.LBB2_1817
.LBB2_848:                              # =>This Inner Loop Header: Depth=1
	mov	qword ptr [r8 + 8*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_848
	jmp	.LBB2_1817
.LBB2_879:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_880:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi + 480], ymm0
	add	rdi, 256
	add	rsi, 4
	jne	.LBB2_880
.LBB2_881:
	test	rdx, rdx
	je	.LBB2_884
# %bb.882:
	lea	rsi, [r8 + 2*rdi]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB2_883:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB2_883
.LBB2_884:
	cmp	rcx, rax
	je	.LBB2_1817
.LBB2_885:                              # =>This Inner Loop Header: Depth=1
	mov	word ptr [r8 + 2*rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_885
	jmp	.LBB2_1817
.LBB2_1006:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB2_1007:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 128], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 160], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 192], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 224], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 256], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 288], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 320], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 352], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 384], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 416], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 448], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 480], ymm0
	add	rdi, 512
	add	rsi, 4
	jne	.LBB2_1007
.LBB2_1008:
	test	rdx, rdx
	je	.LBB2_1011
# %bb.1009:
	lea	rsi, [rdi + r8]
	add	rsi, 96
	neg	rdx
	vpxor	xmm0, xmm0, xmm0
.LBB2_1010:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymmword ptr [rsi - 96], ymm0
	vmovdqu	ymmword ptr [rsi - 64], ymm0
	vmovdqu	ymmword ptr [rsi - 32], ymm0
	vmovdqu	ymmword ptr [rsi], ymm0
	sub	rsi, -128
	inc	rdx
	jne	.LBB2_1010
.LBB2_1011:
	cmp	rcx, rax
	je	.LBB2_1817
.LBB2_1012:                             # =>This Inner Loop Header: Depth=1
	mov	byte ptr [r8 + rcx], 0
	add	rcx, 1
	cmp	rax, rcx
	jne	.LBB2_1012
.LBB2_1817:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB2_1073:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1074:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1074
.LBB2_1075:
	test	r9, r9
	je	.LBB2_1078
# %bb.1076:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1077:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1077
.LBB2_1078:
	cmp	rdi, r10
	je	.LBB2_1817
	jmp	.LBB2_1079
.LBB2_1083:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1084:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1084
.LBB2_1085:
	test	r9, r9
	je	.LBB2_1088
# %bb.1086:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1087:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1087
.LBB2_1088:
	cmp	rdi, r10
	je	.LBB2_1817
	jmp	.LBB2_1089
.LBB2_1093:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1094:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1094
.LBB2_1095:
	test	rsi, rsi
	je	.LBB2_1098
# %bb.1096:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB2_1097:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB2_1097
.LBB2_1098:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1099
.LBB2_1103:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_1104:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1104
.LBB2_1105:
	test	rsi, rsi
	je	.LBB2_1108
# %bb.1106:
	add	rax, rax
	add	rax, 32
	neg	rsi
.LBB2_1107:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax - 32]
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymmword ptr [r8 + rax - 32], ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	add	rax, 64
	inc	rsi
	jne	.LBB2_1107
.LBB2_1108:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1109
.LBB2_1113:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1114:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1114
.LBB2_1115:
	test	r9, r9
	je	.LBB2_1118
# %bb.1116:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1117:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1117
.LBB2_1118:
	cmp	rdi, r10
	je	.LBB2_1817
	jmp	.LBB2_1119
.LBB2_1123:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1124:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1124
.LBB2_1125:
	test	r9, r9
	je	.LBB2_1128
# %bb.1126:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_7] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_1127:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1127
.LBB2_1128:
	cmp	rdi, r10
	je	.LBB2_1817
	jmp	.LBB2_1129
.LBB2_1133:
	xor	edi, edi
.LBB2_1134:
	test	r9b, 1
	je	.LBB2_1136
# %bb.1135:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1136:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1137
.LBB2_1141:
	xor	edi, edi
.LBB2_1142:
	test	r9b, 1
	je	.LBB2_1144
# %bb.1143:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1144:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1145
.LBB2_1149:
	xor	edi, edi
.LBB2_1150:
	test	r9b, 1
	je	.LBB2_1152
# %bb.1151:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1152:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1153
.LBB2_1157:
	xor	edi, edi
.LBB2_1158:
	test	r9b, 1
	je	.LBB2_1160
# %bb.1159:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1160:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1161
.LBB2_1165:
	xor	edi, edi
.LBB2_1166:
	test	r9b, 1
	je	.LBB2_1168
# %bb.1167:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB2_1168:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1169
.LBB2_1173:
	xor	edi, edi
.LBB2_1174:
	test	r9b, 1
	je	.LBB2_1176
# %bb.1175:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1176:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1177
.LBB2_1181:
	xor	edi, edi
.LBB2_1182:
	test	r9b, 1
	je	.LBB2_1184
# %bb.1183:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm3
.LBB2_1184:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1185
.LBB2_1189:
	xor	edi, edi
.LBB2_1190:
	test	r9b, 1
	je	.LBB2_1192
# %bb.1191:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1192:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1193
.LBB2_1197:
	xor	edi, edi
.LBB2_1198:
	test	r9b, 1
	je	.LBB2_1200
# %bb.1199:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1200:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1201
.LBB2_1205:
	xor	edi, edi
.LBB2_1206:
	test	r9b, 1
	je	.LBB2_1208
# %bb.1207:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1208:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1209
.LBB2_1215:
	xor	edi, edi
.LBB2_1216:
	test	r9b, 1
	je	.LBB2_1218
# %bb.1217:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1218:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1219
.LBB2_1225:
	xor	edi, edi
.LBB2_1226:
	test	r9b, 1
	je	.LBB2_1228
# %bb.1227:
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1228:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1229
.LBB2_1233:
	xor	edi, edi
.LBB2_1234:
	test	r9b, 1
	je	.LBB2_1236
# %bb.1235:
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1236:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1237
.LBB2_1241:
	xor	edi, edi
.LBB2_1242:
	test	r9b, 1
	je	.LBB2_1244
# %bb.1243:
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1244:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1245
.LBB2_1249:
	xor	edi, edi
.LBB2_1250:
	test	r9b, 1
	je	.LBB2_1252
# %bb.1251:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1252:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1253
.LBB2_1257:
	xor	edi, edi
.LBB2_1258:
	test	r9b, 1
	je	.LBB2_1260
# %bb.1259:
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1260:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1261
.LBB2_1265:
	xor	edi, edi
.LBB2_1266:
	test	r9b, 1
	je	.LBB2_1268
# %bb.1267:
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI2_3] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1268:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1269
.LBB2_1273:
	xor	edi, edi
.LBB2_1274:
	test	r9b, 1
	je	.LBB2_1276
# %bb.1275:
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1276:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1277
.LBB2_1281:
	xor	edi, edi
.LBB2_1282:
	test	r9b, 1
	je	.LBB2_1284
# %bb.1283:
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_1284:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1285
.LBB2_1289:
	xor	edi, edi
.LBB2_1290:
	test	r9b, 1
	je	.LBB2_1292
# %bb.1291:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1292:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1293
.LBB2_1297:
	xor	edi, edi
.LBB2_1298:
	test	r9b, 1
	je	.LBB2_1300
# %bb.1299:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1300:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1301
.LBB2_1305:
	xor	edi, edi
.LBB2_1306:
	test	r9b, 1
	je	.LBB2_1308
# %bb.1307:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1308:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1309
.LBB2_1313:
	xor	edi, edi
.LBB2_1314:
	test	r9b, 1
	je	.LBB2_1316
# %bb.1315:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1316:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1317
.LBB2_1321:
	xor	edi, edi
.LBB2_1322:
	test	r9b, 1
	je	.LBB2_1324
# %bb.1323:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1324:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1325
.LBB2_1329:
	xor	edi, edi
.LBB2_1330:
	test	r9b, 1
	je	.LBB2_1332
# %bb.1331:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1332:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1333
.LBB2_1337:
	xor	edi, edi
.LBB2_1338:
	test	r9b, 1
	je	.LBB2_1340
# %bb.1339:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1340:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1341
.LBB2_1345:
	xor	edi, edi
.LBB2_1346:
	test	r9b, 1
	je	.LBB2_1348
# %bb.1347:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1348:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1349
.LBB2_1353:
	xor	edi, edi
.LBB2_1354:
	test	r9b, 1
	je	.LBB2_1356
# %bb.1355:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1356:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1357
.LBB2_1361:
	xor	edi, edi
.LBB2_1362:
	test	r9b, 1
	je	.LBB2_1364
# %bb.1363:
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
.LBB2_1364:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1365
.LBB2_1369:
	xor	edi, edi
.LBB2_1370:
	test	r9b, 1
	je	.LBB2_1372
# %bb.1371:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB2_1372:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1373
.LBB2_1377:
	xor	edi, edi
.LBB2_1378:
	test	r9b, 1
	je	.LBB2_1380
# %bb.1379:
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
.LBB2_1380:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1381
.LBB2_1385:
	xor	edi, edi
.LBB2_1386:
	test	r9b, 1
	je	.LBB2_1388
# %bb.1387:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm3
.LBB2_1388:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1389
.LBB2_1393:
	xor	edi, edi
.LBB2_1394:
	test	r9b, 1
	je	.LBB2_1396
# %bb.1395:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1396:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1397
.LBB2_1401:
	xor	edi, edi
.LBB2_1402:
	test	r9b, 1
	je	.LBB2_1404
# %bb.1403:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1404:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1405
.LBB2_1409:
	xor	edi, edi
.LBB2_1410:
	test	r9b, 1
	je	.LBB2_1412
# %bb.1411:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1412:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1413
.LBB2_1417:
	xor	edi, edi
.LBB2_1418:
	test	r9b, 1
	je	.LBB2_1420
# %bb.1419:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1420:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1421
.LBB2_1425:
	xor	edi, edi
.LBB2_1426:
	test	r9b, 1
	je	.LBB2_1428
# %bb.1427:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1428:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1429
.LBB2_1433:
	xor	edi, edi
.LBB2_1434:
	test	r9b, 1
	je	.LBB2_1436
# %bb.1435:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1436:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1437
.LBB2_1441:
	xor	edi, edi
.LBB2_1442:
	test	r9b, 1
	je	.LBB2_1444
# %bb.1443:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1444:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1445
.LBB2_1449:
	xor	edi, edi
.LBB2_1450:
	test	r9b, 1
	je	.LBB2_1452
# %bb.1451:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1452:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1453
.LBB2_1457:
	xor	edi, edi
.LBB2_1458:
	test	r9b, 1
	je	.LBB2_1460
# %bb.1459:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1460:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1461
.LBB2_1465:
	xor	edi, edi
.LBB2_1466:
	test	r9b, 1
	je	.LBB2_1468
# %bb.1467:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1468:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1469
.LBB2_1473:
	xor	edi, edi
.LBB2_1474:
	test	r9b, 1
	je	.LBB2_1476
# %bb.1475:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1476:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1477
.LBB2_1481:
	xor	esi, esi
.LBB2_1482:
	test	r9b, 1
	je	.LBB2_1484
# %bb.1483:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_5] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB2_1484:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1485
.LBB2_1489:
	xor	edi, edi
.LBB2_1490:
	test	r9b, 1
	je	.LBB2_1492
# %bb.1491:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1492:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1493
.LBB2_1497:
	xor	edi, edi
.LBB2_1498:
	test	r9b, 1
	je	.LBB2_1500
# %bb.1499:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1500:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1501
.LBB2_1505:
	xor	esi, esi
.LBB2_1506:
	test	r9b, 1
	je	.LBB2_1508
# %bb.1507:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rsi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rsi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_5] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB2_1508:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1509
.LBB2_1513:
	xor	edi, edi
.LBB2_1514:
	test	r9b, 1
	je	.LBB2_1516
# %bb.1515:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1516:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1517
.LBB2_1521:
	xor	edi, edi
.LBB2_1522:
	test	r9b, 1
	je	.LBB2_1524
# %bb.1523:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1524:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1525
.LBB2_1529:
	xor	edi, edi
.LBB2_1530:
	test	r9b, 1
	je	.LBB2_1532
# %bb.1531:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1532:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1533
.LBB2_1537:
	xor	edi, edi
.LBB2_1538:
	test	r9b, 1
	je	.LBB2_1540
# %bb.1539:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_1540:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1541
.LBB2_1545:
	xor	edi, edi
.LBB2_1546:
	test	r9b, 1
	je	.LBB2_1548
# %bb.1547:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1548:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1549
.LBB2_1553:
	xor	edi, edi
.LBB2_1554:
	test	r9b, 1
	je	.LBB2_1556
# %bb.1555:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1556:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1557
.LBB2_1563:
	xor	edi, edi
.LBB2_1564:
	test	r9b, 1
	je	.LBB2_1566
# %bb.1565:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1566:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1567
.LBB2_1571:
	xor	edi, edi
.LBB2_1572:
	test	r9b, 1
	je	.LBB2_1574
# %bb.1573:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_2] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpxor	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpxor	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpxor	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpxor	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1574:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1575
.LBB2_1581:
	xor	edi, edi
.LBB2_1582:
	test	r9b, 1
	je	.LBB2_1584
# %bb.1583:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1584:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1585
.LBB2_1589:
	xor	edi, edi
.LBB2_1590:
	test	r9b, 1
	je	.LBB2_1592
# %bb.1591:
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1592:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1593
.LBB2_1597:
	xor	edi, edi
.LBB2_1598:
	test	r9b, 1
	je	.LBB2_1600
# %bb.1599:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1600:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1601
.LBB2_1605:
	xor	edi, edi
.LBB2_1606:
	test	r9b, 1
	je	.LBB2_1608
# %bb.1607:
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1608:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1609
.LBB2_1613:
	xor	edi, edi
.LBB2_1614:
	test	r9b, 1
	je	.LBB2_1616
# %bb.1615:
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
.LBB2_1616:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1617
.LBB2_1621:
	xor	edi, edi
.LBB2_1622:
	test	r9b, 1
	je	.LBB2_1624
# %bb.1623:
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1624:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1625
.LBB2_1629:
	xor	edi, edi
.LBB2_1630:
	test	r9b, 1
	je	.LBB2_1632
# %bb.1631:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1632:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1633
.LBB2_1639:
	xor	edi, edi
.LBB2_1640:
	test	r9b, 1
	je	.LBB2_1642
# %bb.1641:
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
.LBB2_1642:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1643
.LBB2_1647:
	xor	edi, edi
.LBB2_1648:
	test	r9b, 1
	je	.LBB2_1650
# %bb.1649:
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1650:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1651
.LBB2_1655:
	xor	edi, edi
.LBB2_1656:
	test	r9b, 1
	je	.LBB2_1658
# %bb.1657:
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI2_4] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpand	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpand	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpand	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1658:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1659
.LBB2_1665:
	xor	edi, edi
.LBB2_1666:
	test	r9b, 1
	je	.LBB2_1668
# %bb.1667:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1668:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1669
.LBB2_1673:
	xor	edi, edi
.LBB2_1674:
	test	r9b, 1
	je	.LBB2_1676
# %bb.1675:
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1676:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1677
.LBB2_1681:
	xor	edi, edi
.LBB2_1682:
	test	r9b, 1
	je	.LBB2_1684
# %bb.1683:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1684:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1685
.LBB2_1689:
	xor	edi, edi
.LBB2_1690:
	test	r9b, 1
	je	.LBB2_1692
# %bb.1691:
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1692:
	cmp	rdx, rax
	je	.LBB2_1817
	jmp	.LBB2_1693
.LBB2_1697:
	xor	edi, edi
.LBB2_1698:
	test	r9b, 1
	je	.LBB2_1700
# %bb.1699:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1700:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1701
.LBB2_1705:
	xor	edi, edi
.LBB2_1706:
	test	r9b, 1
	je	.LBB2_1708
# %bb.1707:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1708:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1709
.LBB2_1713:
	xor	edi, edi
.LBB2_1714:
	test	r9b, 1
	je	.LBB2_1716
# %bb.1715:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1716:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1717
.LBB2_1721:
	xor	edi, edi
.LBB2_1722:
	test	r9b, 1
	je	.LBB2_1724
# %bb.1723:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB2_1724:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1725
.LBB2_1729:
	xor	edi, edi
.LBB2_1730:
	test	r9b, 1
	je	.LBB2_1732
# %bb.1731:
	vmovdqu	ymm0, ymmword ptr [rdx + rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm0
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm3
.LBB2_1732:
	cmp	rcx, r10
	je	.LBB2_1817
	jmp	.LBB2_1733
.LBB2_1737:
	xor	edi, edi
.LBB2_1738:
	test	r9b, 1
	je	.LBB2_1740
# %bb.1739:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1740:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1741
.LBB2_1745:
	xor	edi, edi
.LBB2_1746:
	test	r9b, 1
	je	.LBB2_1748
# %bb.1747:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1748:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1749
.LBB2_1753:
	xor	edi, edi
.LBB2_1754:
	test	r9b, 1
	je	.LBB2_1756
# %bb.1755:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1756:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1757
.LBB2_1761:
	xor	edi, edi
.LBB2_1762:
	test	r9b, 1
	je	.LBB2_1764
# %bb.1763:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1764:
	cmp	rcx, rax
	je	.LBB2_1817
	jmp	.LBB2_1765
.LBB2_1769:
	xor	edi, edi
.LBB2_1770:
	test	r9b, 1
	je	.LBB2_1772
# %bb.1771:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1772:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1773
.LBB2_1777:
	xor	edi, edi
.LBB2_1778:
	test	r9b, 1
	je	.LBB2_1780
# %bb.1779:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1780:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1781
.LBB2_1785:
	xor	edi, edi
.LBB2_1786:
	test	r9b, 1
	je	.LBB2_1788
# %bb.1787:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1788:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1789
.LBB2_1793:
	xor	edi, edi
.LBB2_1794:
	test	r9b, 1
	je	.LBB2_1796
# %bb.1795:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1796:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1797
.LBB2_1801:
	xor	edi, edi
.LBB2_1802:
	test	r9b, 1
	je	.LBB2_1804
# %bb.1803:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1804:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1805
.LBB2_1809:
	xor	edi, edi
.LBB2_1810:
	test	r9b, 1
	je	.LBB2_1812
# %bb.1811:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1812:
	cmp	rsi, r10
	je	.LBB2_1817
	jmp	.LBB2_1813
.Lfunc_end2:
	.size	arithmetic_scalar_arr_avx2, .Lfunc_end2-arithmetic_scalar_arr_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
