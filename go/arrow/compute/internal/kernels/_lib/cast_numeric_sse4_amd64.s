	.text
	.intel_syntax noprefix
	.file	"cast_numeric.cc"
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_type_numeric_sse4
.LCPI0_0:
	.quad	0x43e0000000000000              # double 9.2233720368547758E+18
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI0_1:
	.byte	0                               # 0x0
	.byte	4                               # 0x4
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
	.zero	1
	.zero	1
.LCPI0_3:
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
.LCPI0_4:
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
	.long	2147483648                      # 0x80000000
.LCPI0_5:
	.byte	0                               # 0x0
	.byte	8                               # 0x8
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
	.zero	1
	.zero	1
.LCPI0_6:
	.quad	4841369599423283200             # 0x4330000000000000
	.quad	4841369599423283200             # 0x4330000000000000
.LCPI0_7:
	.quad	4985484787499139072             # 0x4530000000000000
	.quad	4985484787499139072             # 0x4530000000000000
.LCPI0_8:
	.quad	0x4530000000100000              # double 1.9342813118337666E+25
	.quad	0x4530000000100000              # double 1.9342813118337666E+25
.LCPI0_9:
	.long	1127219200                      # 0x43300000
	.long	1160773632                      # 0x45300000
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_10:
	.quad	0x4330000000000000              # double 4503599627370496
	.quad	0x4530000000000000              # double 1.9342813113834067E+25
.LCPI0_11:
	.quad	1                               # 0x1
	.quad	1                               # 0x1
.LCPI0_12:
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
.LCPI0_13:
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
.LCPI0_14:
	.long	1258291200                      # 0x4b000000
	.long	1258291200                      # 0x4b000000
	.long	1258291200                      # 0x4b000000
	.long	1258291200                      # 0x4b000000
.LCPI0_15:
	.long	1392508928                      # 0x53000000
	.long	1392508928                      # 0x53000000
	.long	1392508928                      # 0x53000000
	.long	1392508928                      # 0x53000000
.LCPI0_16:
	.long	0x53000080                      # float 5.49764202E+11
	.long	0x53000080                      # float 5.49764202E+11
	.long	0x53000080                      # float 5.49764202E+11
	.long	0x53000080                      # float 5.49764202E+11
.LCPI0_17:
	.byte	0                               # 0x0
	.byte	2                               # 0x2
	.byte	4                               # 0x4
	.byte	6                               # 0x6
	.byte	8                               # 0x8
	.byte	10                              # 0xa
	.byte	12                              # 0xc
	.byte	14                              # 0xe
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
	.long	0x5f000000                      # float 9.22337203E+18
	.text
	.globl	cast_type_numeric_sse4
	.p2align	4, 0x90
	.type	cast_type_numeric_sse4,@function
cast_type_numeric_sse4:                 # @cast_type_numeric_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edi, 6
	jg	.LBB0_13
# %bb.1:
	cmp	edi, 3
	jle	.LBB0_25
# %bb.2:
	cmp	edi, 4
	je	.LBB0_45
# %bb.3:
	cmp	edi, 5
	je	.LBB0_53
# %bb.4:
	cmp	edi, 6
	jne	.LBB0_1526
# %bb.5:
	cmp	esi, 6
	jg	.LBB0_93
# %bb.6:
	cmp	esi, 3
	jle	.LBB0_163
# %bb.7:
	cmp	esi, 4
	je	.LBB0_263
# %bb.8:
	cmp	esi, 5
	je	.LBB0_266
# %bb.9:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.10:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.11:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_12
# %bb.443:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_761
# %bb.444:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_761
.LBB0_12:
	xor	esi, esi
.LBB0_1104:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1106
.LBB0_1105:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1105
.LBB0_1106:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1107:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1107
	jmp	.LBB0_1526
.LBB0_13:
	cmp	edi, 8
	jle	.LBB0_35
# %bb.14:
	cmp	edi, 9
	je	.LBB0_61
# %bb.15:
	cmp	edi, 11
	je	.LBB0_69
# %bb.16:
	cmp	edi, 12
	jne	.LBB0_1526
# %bb.17:
	cmp	esi, 6
	jg	.LBB0_100
# %bb.18:
	cmp	esi, 3
	jle	.LBB0_168
# %bb.19:
	cmp	esi, 4
	je	.LBB0_269
# %bb.20:
	cmp	esi, 5
	je	.LBB0_272
# %bb.21:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.22:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.23:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_446
# %bb.24:
	xor	edi, edi
	jmp	.LBB0_448
.LBB0_25:
	cmp	edi, 2
	je	.LBB0_77
# %bb.26:
	cmp	edi, 3
	jne	.LBB0_1526
# %bb.27:
	cmp	esi, 6
	jg	.LBB0_107
# %bb.28:
	cmp	esi, 3
	jle	.LBB0_173
# %bb.29:
	cmp	esi, 4
	je	.LBB0_275
# %bb.30:
	cmp	esi, 5
	je	.LBB0_278
# %bb.31:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.32:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.33:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_34
# %bb.451:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_763
# %bb.452:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_763
.LBB0_34:
	xor	esi, esi
.LBB0_1482:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1484
.LBB0_1483:                             # =>This Inner Loop Header: Depth=1
	movsx	edi, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1483
.LBB0_1484:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1485:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	movsx	eax, byte ptr [rdx + rsi + 2]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	movsx	eax, byte ptr [rdx + rsi + 3]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1485
	jmp	.LBB0_1526
.LBB0_35:
	cmp	edi, 7
	je	.LBB0_85
# %bb.36:
	cmp	edi, 8
	jne	.LBB0_1526
# %bb.37:
	cmp	esi, 6
	jg	.LBB0_114
# %bb.38:
	cmp	esi, 3
	jle	.LBB0_178
# %bb.39:
	cmp	esi, 4
	je	.LBB0_281
# %bb.40:
	cmp	esi, 5
	je	.LBB0_284
# %bb.41:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.42:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.43:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_454
# %bb.44:
	xor	esi, esi
	jmp	.LBB0_948
.LBB0_45:
	cmp	esi, 6
	jg	.LBB0_121
# %bb.46:
	cmp	esi, 3
	jle	.LBB0_183
# %bb.47:
	cmp	esi, 4
	je	.LBB0_287
# %bb.48:
	cmp	esi, 5
	je	.LBB0_290
# %bb.49:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.50:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.51:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_457
# %bb.52:
	xor	esi, esi
	jmp	.LBB0_953
.LBB0_53:
	cmp	esi, 6
	jg	.LBB0_128
# %bb.54:
	cmp	esi, 3
	jle	.LBB0_188
# %bb.55:
	cmp	esi, 4
	je	.LBB0_293
# %bb.56:
	cmp	esi, 5
	je	.LBB0_296
# %bb.57:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.58:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.59:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_460
# %bb.60:
	xor	esi, esi
	jmp	.LBB0_958
.LBB0_61:
	cmp	esi, 6
	jg	.LBB0_135
# %bb.62:
	cmp	esi, 3
	jle	.LBB0_193
# %bb.63:
	cmp	esi, 4
	je	.LBB0_299
# %bb.64:
	cmp	esi, 5
	je	.LBB0_302
# %bb.65:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.66:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.67:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_463
# %bb.68:
	xor	esi, esi
	jmp	.LBB0_963
.LBB0_69:
	cmp	esi, 6
	jg	.LBB0_142
# %bb.70:
	cmp	esi, 3
	jle	.LBB0_198
# %bb.71:
	cmp	esi, 4
	je	.LBB0_305
# %bb.72:
	cmp	esi, 5
	je	.LBB0_308
# %bb.73:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.74:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.75:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_466
# %bb.76:
	xor	esi, esi
	jmp	.LBB0_968
.LBB0_77:
	cmp	esi, 6
	jg	.LBB0_149
# %bb.78:
	cmp	esi, 3
	jle	.LBB0_203
# %bb.79:
	cmp	esi, 4
	je	.LBB0_311
# %bb.80:
	cmp	esi, 5
	je	.LBB0_314
# %bb.81:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.82:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.83:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_84
# %bb.469:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_766
# %bb.470:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_766
.LBB0_84:
	xor	esi, esi
.LBB0_1490:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1492
.LBB0_1491:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1491
.LBB0_1492:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1493:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1493
	jmp	.LBB0_1526
.LBB0_85:
	cmp	esi, 6
	jg	.LBB0_156
# %bb.86:
	cmp	esi, 3
	jle	.LBB0_208
# %bb.87:
	cmp	esi, 4
	je	.LBB0_317
# %bb.88:
	cmp	esi, 5
	je	.LBB0_320
# %bb.89:
	cmp	esi, 6
	jne	.LBB0_1526
# %bb.90:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.91:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_92
# %bb.472:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_769
# %bb.473:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_769
.LBB0_92:
	xor	esi, esi
.LBB0_1114:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1116
.LBB0_1115:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1115
.LBB0_1116:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1117:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1117
	jmp	.LBB0_1526
.LBB0_93:
	cmp	esi, 8
	jle	.LBB0_213
# %bb.94:
	cmp	esi, 9
	je	.LBB0_323
# %bb.95:
	cmp	esi, 11
	je	.LBB0_326
# %bb.96:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.97:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.98:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_475
# %bb.99:
	xor	edi, edi
	jmp	.LBB0_477
.LBB0_100:
	cmp	esi, 8
	jle	.LBB0_218
# %bb.101:
	cmp	esi, 9
	je	.LBB0_329
# %bb.102:
	cmp	esi, 11
	je	.LBB0_332
# %bb.103:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.104:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.105:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_106
# %bb.480:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_771
# %bb.481:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_771
.LBB0_106:
	xor	esi, esi
.LBB0_1124:
	mov	edi, r9d
	sub	edi, esi
	mov	r8, rsi
	not	r8
	add	r8, r9
	and	rdi, 7
	je	.LBB0_1126
.LBB0_1125:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1125
.LBB0_1126:
	cmp	r8, 7
	jb	.LBB0_1526
.LBB0_1127:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	mov	rax, qword ptr [rdx + 8*rsi + 32]
	mov	qword ptr [rcx + 8*rsi + 32], rax
	mov	rax, qword ptr [rdx + 8*rsi + 40]
	mov	qword ptr [rcx + 8*rsi + 40], rax
	mov	rax, qword ptr [rdx + 8*rsi + 48]
	mov	qword ptr [rcx + 8*rsi + 48], rax
	mov	rax, qword ptr [rdx + 8*rsi + 56]
	mov	qword ptr [rcx + 8*rsi + 56], rax
	add	rsi, 8
	cmp	r9, rsi
	jne	.LBB0_1127
	jmp	.LBB0_1526
.LBB0_107:
	cmp	esi, 8
	jle	.LBB0_223
# %bb.108:
	cmp	esi, 9
	je	.LBB0_335
# %bb.109:
	cmp	esi, 11
	je	.LBB0_338
# %bb.110:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.111:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.112:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_483
# %bb.113:
	xor	edi, edi
	jmp	.LBB0_485
.LBB0_114:
	cmp	esi, 8
	jle	.LBB0_228
# %bb.115:
	cmp	esi, 9
	je	.LBB0_341
# %bb.116:
	cmp	esi, 11
	je	.LBB0_344
# %bb.117:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.118:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.119:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_488
# %bb.120:
	xor	esi, esi
	jmp	.LBB0_973
.LBB0_121:
	cmp	esi, 8
	jle	.LBB0_233
# %bb.122:
	cmp	esi, 9
	je	.LBB0_347
# %bb.123:
	cmp	esi, 11
	je	.LBB0_350
# %bb.124:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.125:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.126:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_491
# %bb.127:
	xor	edi, edi
	jmp	.LBB0_493
.LBB0_128:
	cmp	esi, 8
	jle	.LBB0_238
# %bb.129:
	cmp	esi, 9
	je	.LBB0_353
# %bb.130:
	cmp	esi, 11
	je	.LBB0_356
# %bb.131:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.132:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.133:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_496
# %bb.134:
	xor	edi, edi
	jmp	.LBB0_498
.LBB0_135:
	cmp	esi, 8
	jle	.LBB0_243
# %bb.136:
	cmp	esi, 9
	je	.LBB0_359
# %bb.137:
	cmp	esi, 11
	je	.LBB0_362
# %bb.138:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.139:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.140:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 3
	jae	.LBB0_501
# %bb.141:
	xor	edi, edi
	jmp	.LBB0_503
.LBB0_142:
	cmp	esi, 8
	jle	.LBB0_248
# %bb.143:
	cmp	esi, 9
	je	.LBB0_365
# %bb.144:
	cmp	esi, 11
	je	.LBB0_368
# %bb.145:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.146:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.147:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_506
# %bb.148:
	xor	esi, esi
	jmp	.LBB0_979
.LBB0_149:
	cmp	esi, 8
	jle	.LBB0_253
# %bb.150:
	cmp	esi, 9
	je	.LBB0_371
# %bb.151:
	cmp	esi, 11
	je	.LBB0_374
# %bb.152:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.153:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.154:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_509
# %bb.155:
	xor	edi, edi
	jmp	.LBB0_511
.LBB0_156:
	cmp	esi, 8
	jle	.LBB0_258
# %bb.157:
	cmp	esi, 9
	je	.LBB0_377
# %bb.158:
	cmp	esi, 11
	je	.LBB0_380
# %bb.159:
	cmp	esi, 12
	jne	.LBB0_1526
# %bb.160:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.161:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 3
	jae	.LBB0_514
# %bb.162:
	xor	edi, edi
	jmp	.LBB0_516
.LBB0_163:
	cmp	esi, 2
	je	.LBB0_383
# %bb.164:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.165:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.166:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_167
# %bb.519:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_773
# %bb.520:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_773
.LBB0_167:
	xor	esi, esi
.LBB0_1498:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1500
.LBB0_1499:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1499
.LBB0_1500:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1501:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1501
	jmp	.LBB0_1526
.LBB0_168:
	cmp	esi, 2
	je	.LBB0_386
# %bb.169:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.170:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.171:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_172
# %bb.522:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_776
# %bb.523:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_776
.LBB0_172:
	xor	esi, esi
.LBB0_1506:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1508
.LBB0_1507:                             # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1507
.LBB0_1508:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1509:                             # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	cvttsd2si	eax, qword ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	cvttsd2si	eax, qword ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	cvttsd2si	eax, qword ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1509
	jmp	.LBB0_1526
.LBB0_173:
	cmp	esi, 2
	je	.LBB0_389
# %bb.174:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.175:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.176:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_177
# %bb.525:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_779
# %bb.526:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_779
.LBB0_177:
	xor	esi, esi
.LBB0_1134:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1136
.LBB0_1135:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1135
.LBB0_1136:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1137:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1137
	jmp	.LBB0_1526
.LBB0_178:
	cmp	esi, 2
	je	.LBB0_392
# %bb.179:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.180:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.181:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_182
# %bb.528:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_781
# %bb.529:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_781
.LBB0_182:
	xor	esi, esi
.LBB0_1322:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1324
.LBB0_1323:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1323
.LBB0_1324:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1325:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1325
	jmp	.LBB0_1526
.LBB0_183:
	cmp	esi, 2
	je	.LBB0_395
# %bb.184:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.185:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.186:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_187
# %bb.531:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_784
# %bb.532:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_784
.LBB0_187:
	xor	esi, esi
.LBB0_1330:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1332
.LBB0_1331:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1331
.LBB0_1332:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1333:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 2*rsi + 2]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 2*rsi + 4]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 2*rsi + 6]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1333
	jmp	.LBB0_1526
.LBB0_188:
	cmp	esi, 2
	je	.LBB0_398
# %bb.189:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.190:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.191:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_192
# %bb.534:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_787
# %bb.535:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_787
.LBB0_192:
	xor	esi, esi
.LBB0_1514:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1516
.LBB0_1515:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1515
.LBB0_1516:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1517:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 2*rsi + 2]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 2*rsi + 4]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 2*rsi + 6]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1517
	jmp	.LBB0_1526
.LBB0_193:
	cmp	esi, 2
	je	.LBB0_401
# %bb.194:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.195:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.196:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_197
# %bb.537:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_790
# %bb.538:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_790
.LBB0_197:
	xor	esi, esi
.LBB0_1338:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1340
.LBB0_1339:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1339
.LBB0_1340:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1341:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1341
	jmp	.LBB0_1526
.LBB0_198:
	cmp	esi, 2
	je	.LBB0_404
# %bb.199:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.200:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.201:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_202
# %bb.540:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_793
# %bb.541:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_793
.LBB0_202:
	xor	esi, esi
.LBB0_1522:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1524
.LBB0_1523:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1523
.LBB0_1524:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1525:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	cvttss2si	eax, dword ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	cvttss2si	eax, dword ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	cvttss2si	eax, dword ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1525
	jmp	.LBB0_1526
.LBB0_203:
	cmp	esi, 2
	je	.LBB0_407
# %bb.204:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.205:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.206:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_207
# %bb.543:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_796
# %bb.544:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_796
.LBB0_207:
	xor	esi, esi
.LBB0_1144:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1146
.LBB0_1145:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1145
.LBB0_1146:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1147:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1147
	jmp	.LBB0_1526
.LBB0_208:
	cmp	esi, 2
	je	.LBB0_410
# %bb.209:
	cmp	esi, 3
	jne	.LBB0_1526
# %bb.210:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.211:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_212
# %bb.546:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_798
# %bb.547:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_798
.LBB0_212:
	xor	esi, esi
.LBB0_1346:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1348
.LBB0_1347:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1347
.LBB0_1348:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1349:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1349
	jmp	.LBB0_1526
.LBB0_213:
	cmp	esi, 7
	je	.LBB0_413
# %bb.214:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.215:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.216:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_549
# %bb.217:
	xor	esi, esi
	jmp	.LBB0_807
.LBB0_218:
	cmp	esi, 7
	je	.LBB0_416
# %bb.219:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.220:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.221:
	mov	r9d, r8d
	lea	rax, [r9 - 1]
	mov	r8d, r9d
	and	r8d, 3
	movabs	r10, -9223372036854775808
	cmp	rax, 3
	jae	.LBB0_551
# %bb.222:
	xor	eax, eax
	jmp	.LBB0_553
.LBB0_223:
	cmp	esi, 7
	je	.LBB0_419
# %bb.224:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.225:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.226:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_227
# %bb.556:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_808
# %bb.557:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_808
.LBB0_227:
	xor	esi, esi
.LBB0_1154:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1156
.LBB0_1155:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1155
.LBB0_1156:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1157:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	movsx	rax, byte ptr [rdx + rsi + 1]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	movsx	rax, byte ptr [rdx + rsi + 2]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	movsx	rax, byte ptr [rdx + rsi + 3]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1157
	jmp	.LBB0_1526
.LBB0_228:
	cmp	esi, 7
	je	.LBB0_422
# %bb.229:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.230:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.231:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_232
# %bb.559:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_810
# %bb.560:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_810
.LBB0_232:
	xor	esi, esi
.LBB0_1164:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1166
.LBB0_1165:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1165
.LBB0_1166:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1167:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1167
	jmp	.LBB0_1526
.LBB0_233:
	cmp	esi, 7
	je	.LBB0_425
# %bb.234:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.235:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.236:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_562
# %bb.237:
	xor	esi, esi
	jmp	.LBB0_818
.LBB0_238:
	cmp	esi, 7
	je	.LBB0_428
# %bb.239:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.240:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.241:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_564
# %bb.242:
	xor	esi, esi
	jmp	.LBB0_825
.LBB0_243:
	cmp	esi, 7
	je	.LBB0_431
# %bb.244:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.245:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.246:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_247
# %bb.566:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_826
# %bb.567:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_826
.LBB0_247:
	xor	esi, esi
.LBB0_1174:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1176
.LBB0_1175:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1175
.LBB0_1176:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1177:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1177
	jmp	.LBB0_1526
.LBB0_248:
	cmp	esi, 7
	je	.LBB0_434
# %bb.249:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.250:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.251:
	mov	r9d, r8d
	lea	rax, [r9 - 1]
	mov	r8d, r9d
	and	r8d, 3
	cmp	rax, 3
	jae	.LBB0_569
# %bb.252:
	xor	edi, edi
	jmp	.LBB0_571
.LBB0_253:
	cmp	esi, 7
	je	.LBB0_437
# %bb.254:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.255:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.256:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_257
# %bb.574:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_828
# %bb.575:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_828
.LBB0_257:
	xor	esi, esi
.LBB0_1184:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1186
.LBB0_1185:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1185
.LBB0_1186:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1187:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1187
	jmp	.LBB0_1526
.LBB0_258:
	cmp	esi, 7
	je	.LBB0_440
# %bb.259:
	cmp	esi, 8
	jne	.LBB0_1526
# %bb.260:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.261:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_577
# %bb.262:
	xor	esi, esi
	jmp	.LBB0_836
.LBB0_263:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.264:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_579
# %bb.265:
	xor	esi, esi
	jmp	.LBB0_984
.LBB0_266:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.267:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_582
# %bb.268:
	xor	esi, esi
	jmp	.LBB0_989
.LBB0_269:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.270:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_585
# %bb.271:
	xor	esi, esi
	jmp	.LBB0_994
.LBB0_272:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.273:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_588
# %bb.274:
	xor	esi, esi
	jmp	.LBB0_999
.LBB0_275:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.276:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_277
# %bb.591:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_837
# %bb.592:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_837
.LBB0_277:
	xor	esi, esi
.LBB0_1354:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1356
.LBB0_1355:                             # =>This Inner Loop Header: Depth=1
	movsx	edi, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1355
.LBB0_1356:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1357:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movsx	eax, byte ptr [rdx + rsi + 2]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movsx	eax, byte ptr [rdx + rsi + 3]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1357
	jmp	.LBB0_1526
.LBB0_278:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.279:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_280
# %bb.594:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_840
# %bb.595:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_840
.LBB0_280:
	xor	esi, esi
.LBB0_1362:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1364
.LBB0_1363:                             # =>This Inner Loop Header: Depth=1
	movsx	edi, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1363
.LBB0_1364:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1365:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movsx	eax, byte ptr [rdx + rsi + 2]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movsx	eax, byte ptr [rdx + rsi + 3]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1365
	jmp	.LBB0_1526
.LBB0_281:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.282:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_597
# %bb.283:
	xor	esi, esi
	jmp	.LBB0_1004
.LBB0_284:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.285:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_600
# %bb.286:
	xor	esi, esi
	jmp	.LBB0_1009
.LBB0_287:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.288:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_289
# %bb.603:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_843
# %bb.604:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_843
.LBB0_289:
	xor	esi, esi
.LBB0_1194:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1196
.LBB0_1195:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1195
.LBB0_1196:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1197:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1197
	jmp	.LBB0_1526
.LBB0_290:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.291:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_292
# %bb.606:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_845
# %bb.607:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_845
.LBB0_292:
	xor	esi, esi
.LBB0_1204:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1206
.LBB0_1205:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1205
.LBB0_1206:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1207:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1207
	jmp	.LBB0_1526
.LBB0_293:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.294:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_295
# %bb.609:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_847
# %bb.610:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_847
.LBB0_295:
	xor	esi, esi
.LBB0_1214:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1216
.LBB0_1215:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1215
.LBB0_1216:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1217:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1217
	jmp	.LBB0_1526
.LBB0_296:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.297:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_298
# %bb.612:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_849
# %bb.613:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_849
.LBB0_298:
	xor	esi, esi
.LBB0_1224:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1226
.LBB0_1225:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1225
.LBB0_1226:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1227:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1227
	jmp	.LBB0_1526
.LBB0_299:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.300:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_615
# %bb.301:
	xor	esi, esi
	jmp	.LBB0_1014
.LBB0_302:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.303:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_618
# %bb.304:
	xor	esi, esi
	jmp	.LBB0_1019
.LBB0_305:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.306:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_621
# %bb.307:
	xor	esi, esi
	jmp	.LBB0_1024
.LBB0_308:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.309:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_624
# %bb.310:
	xor	esi, esi
	jmp	.LBB0_1029
.LBB0_311:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.312:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_313
# %bb.627:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_851
# %bb.628:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_851
.LBB0_313:
	xor	esi, esi
.LBB0_1370:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1372
.LBB0_1371:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1371
.LBB0_1372:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1373:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1373
	jmp	.LBB0_1526
.LBB0_314:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.315:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_316
# %bb.630:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_854
# %bb.631:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_854
.LBB0_316:
	xor	esi, esi
.LBB0_1378:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1380
.LBB0_1379:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1379
.LBB0_1380:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1381:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	word ptr [rcx + 2*rsi + 2], ax
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	word ptr [rcx + 2*rsi + 4], ax
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1381
	jmp	.LBB0_1526
.LBB0_317:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.318:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_633
# %bb.319:
	xor	esi, esi
	jmp	.LBB0_1034
.LBB0_320:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.321:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_636
# %bb.322:
	xor	esi, esi
	jmp	.LBB0_1039
.LBB0_323:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.324:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_639
# %bb.325:
	xor	esi, esi
	jmp	.LBB0_863
.LBB0_326:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.327:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_641
# %bb.328:
	xor	esi, esi
	jmp	.LBB0_1044
.LBB0_329:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.330:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_644
# %bb.331:
	xor	edi, edi
	jmp	.LBB0_646
.LBB0_332:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.333:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_649
# %bb.334:
	xor	esi, esi
	jmp	.LBB0_1049
.LBB0_335:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.336:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_337
# %bb.652:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_864
# %bb.653:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_864
.LBB0_337:
	xor	esi, esi
.LBB0_1234:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1236
.LBB0_1235:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1235
.LBB0_1236:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1237:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	movsx	rax, byte ptr [rdx + rsi + 1]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	movsx	rax, byte ptr [rdx + rsi + 2]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	movsx	rax, byte ptr [rdx + rsi + 3]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1237
	jmp	.LBB0_1526
.LBB0_338:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.339:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_340
# %bb.655:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_866
# %bb.656:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_866
.LBB0_340:
	xor	esi, esi
.LBB0_1386:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1388
.LBB0_1387:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1387
.LBB0_1388:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1389:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi], xmm0
	movsx	eax, byte ptr [rdx + rsi + 1]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi + 4], xmm0
	movsx	eax, byte ptr [rdx + rsi + 2]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi + 8], xmm0
	movsx	eax, byte ptr [rdx + rsi + 3]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1389
	jmp	.LBB0_1526
.LBB0_341:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.342:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_343
# %bb.658:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_869
# %bb.659:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_869
.LBB0_343:
	xor	esi, esi
.LBB0_1244:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1246
.LBB0_1245:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1245
.LBB0_1246:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1247:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1247
	jmp	.LBB0_1526
.LBB0_344:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.345:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_661
# %bb.346:
	xor	esi, esi
	jmp	.LBB0_1056
.LBB0_347:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.348:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_664
# %bb.349:
	xor	esi, esi
	jmp	.LBB0_877
.LBB0_350:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.351:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_666
# %bb.352:
	xor	esi, esi
	jmp	.LBB0_1062
.LBB0_353:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.354:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_669
# %bb.355:
	xor	esi, esi
	jmp	.LBB0_884
.LBB0_356:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.357:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_671
# %bb.358:
	xor	esi, esi
	jmp	.LBB0_1067
.LBB0_359:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.360:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_361
# %bb.674:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_885
# %bb.675:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_885
.LBB0_361:
	xor	esi, esi
.LBB0_1254:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1256
.LBB0_1255:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1255
.LBB0_1256:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1257:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1257
	jmp	.LBB0_1526
.LBB0_362:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.363:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 3
	jae	.LBB0_677
# %bb.364:
	xor	edi, edi
	jmp	.LBB0_679
.LBB0_365:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.366:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	r8d, esi
	and	r8d, 3
	cmp	rdi, 3
	jae	.LBB0_682
# %bb.367:
	xor	edi, edi
	jmp	.LBB0_684
.LBB0_368:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.369:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_370
# %bb.687:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_887
# %bb.688:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_887
.LBB0_370:
	xor	esi, esi
.LBB0_1264:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 7
	je	.LBB0_1266
.LBB0_1265:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1265
.LBB0_1266:
	cmp	r8, 7
	jb	.LBB0_1526
.LBB0_1267:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	mov	eax, dword ptr [rdx + 4*rsi + 16]
	mov	dword ptr [rcx + 4*rsi + 16], eax
	mov	eax, dword ptr [rdx + 4*rsi + 20]
	mov	dword ptr [rcx + 4*rsi + 20], eax
	mov	eax, dword ptr [rdx + 4*rsi + 24]
	mov	dword ptr [rcx + 4*rsi + 24], eax
	mov	eax, dword ptr [rdx + 4*rsi + 28]
	mov	dword ptr [rcx + 4*rsi + 28], eax
	add	rsi, 8
	cmp	r9, rsi
	jne	.LBB0_1267
	jmp	.LBB0_1526
.LBB0_371:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.372:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_373
# %bb.690:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_889
# %bb.691:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_889
.LBB0_373:
	xor	esi, esi
.LBB0_1274:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1276
.LBB0_1275:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1275
.LBB0_1276:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1277:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1277
	jmp	.LBB0_1526
.LBB0_374:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.375:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_376
# %bb.693:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_891
# %bb.694:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_891
.LBB0_376:
	xor	esi, esi
.LBB0_1394:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1396
.LBB0_1395:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1395
.LBB0_1396:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1397:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi], xmm0
	movzx	eax, byte ptr [rdx + rsi + 1]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi + 4], xmm0
	movzx	eax, byte ptr [rdx + rsi + 2]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi + 8], xmm0
	movzx	eax, byte ptr [rdx + rsi + 3]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1397
	jmp	.LBB0_1526
.LBB0_377:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.378:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_696
# %bb.379:
	xor	esi, esi
	jmp	.LBB0_900
.LBB0_380:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.381:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_698
# %bb.382:
	xor	esi, esi
	jmp	.LBB0_1072
.LBB0_383:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.384:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_385
# %bb.701:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_901
# %bb.702:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_901
.LBB0_385:
	xor	esi, esi
.LBB0_1402:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1404
.LBB0_1403:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1403
.LBB0_1404:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1405:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1405
	jmp	.LBB0_1526
.LBB0_386:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.387:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_388
# %bb.704:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_904
# %bb.705:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_904
.LBB0_388:
	xor	esi, esi
.LBB0_1410:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1412
.LBB0_1411:                             # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1411
.LBB0_1412:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1413:                             # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	cvttsd2si	eax, qword ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	cvttsd2si	eax, qword ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	cvttsd2si	eax, qword ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1413
	jmp	.LBB0_1526
.LBB0_389:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.390:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_391
# %bb.707:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_907
# %bb.708:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_907
.LBB0_391:
	xor	esi, esi
.LBB0_1284:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1286
.LBB0_1285:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1285
.LBB0_1286:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1287:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1287
	jmp	.LBB0_1526
.LBB0_392:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.393:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_394
# %bb.710:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_909
# %bb.711:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_909
.LBB0_394:
	xor	esi, esi
.LBB0_1418:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1420
.LBB0_1419:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1419
.LBB0_1420:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1421:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1421
	jmp	.LBB0_1526
.LBB0_395:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.396:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_397
# %bb.713:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_912
# %bb.714:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_912
.LBB0_397:
	xor	esi, esi
.LBB0_1426:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1428
.LBB0_1427:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1427
.LBB0_1428:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1429:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 2*rsi + 2]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 2*rsi + 4]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 2*rsi + 6]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1429
	jmp	.LBB0_1526
.LBB0_398:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.399:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_400
# %bb.716:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_915
# %bb.717:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_915
.LBB0_400:
	xor	esi, esi
.LBB0_1434:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1436
.LBB0_1435:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1435
.LBB0_1436:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1437:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 2*rsi + 2]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 2*rsi + 4]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 2*rsi + 6]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1437
	jmp	.LBB0_1526
.LBB0_401:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.402:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB0_403
# %bb.719:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_918
# %bb.720:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_918
.LBB0_403:
	xor	esi, esi
.LBB0_1442:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1444
.LBB0_1443:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1443
.LBB0_1444:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1445:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1445
	jmp	.LBB0_1526
.LBB0_404:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.405:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_406
# %bb.722:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_921
# %bb.723:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_921
.LBB0_406:
	xor	esi, esi
.LBB0_1450:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1452
.LBB0_1451:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1451
.LBB0_1452:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1453:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	cvttss2si	eax, dword ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	cvttss2si	eax, dword ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	cvttss2si	eax, dword ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1453
	jmp	.LBB0_1526
.LBB0_407:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.408:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_409
# %bb.725:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_924
# %bb.726:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_924
.LBB0_409:
	xor	esi, esi
.LBB0_1294:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1296
.LBB0_1295:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1295
.LBB0_1296:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1297:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1297
	jmp	.LBB0_1526
.LBB0_410:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.411:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_412
# %bb.728:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_926
# %bb.729:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_926
.LBB0_412:
	xor	esi, esi
.LBB0_1458:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1460
.LBB0_1459:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1459
.LBB0_1460:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1461:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	movzx	eax, byte ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	movzx	eax, byte ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1461
	jmp	.LBB0_1526
.LBB0_413:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.414:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_415
# %bb.731:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_929
# %bb.732:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_929
.LBB0_415:
	xor	esi, esi
.LBB0_1304:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1306
.LBB0_1305:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1305
.LBB0_1306:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1307:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1307
	jmp	.LBB0_1526
.LBB0_416:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.417:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_734
# %bb.418:
	xor	esi, esi
	jmp	.LBB0_1077
.LBB0_419:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.420:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_421
# %bb.737:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_931
# %bb.738:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_931
.LBB0_421:
	xor	esi, esi
.LBB0_1466:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1468
.LBB0_1467:                             # =>This Inner Loop Header: Depth=1
	movsx	edi, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1467
.LBB0_1468:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1469:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	movsx	eax, byte ptr [rdx + rsi + 2]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	movsx	eax, byte ptr [rdx + rsi + 3]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1469
	jmp	.LBB0_1526
.LBB0_422:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.423:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_740
# %bb.424:
	xor	esi, esi
	jmp	.LBB0_943
.LBB0_425:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.426:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_743
# %bb.427:
	xor	esi, esi
	jmp	.LBB0_1082
.LBB0_428:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.429:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_746
# %bb.430:
	xor	esi, esi
	jmp	.LBB0_1087
.LBB0_431:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.432:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_749
# %bb.433:
	xor	esi, esi
	jmp	.LBB0_1092
.LBB0_434:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.435:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB0_752
# %bb.436:
	xor	esi, esi
	jmp	.LBB0_1097
.LBB0_437:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.438:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_439
# %bb.755:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_934
# %bb.756:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_934
.LBB0_439:
	xor	esi, esi
.LBB0_1474:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB0_1476
.LBB0_1475:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_1475
.LBB0_1476:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1477:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	movzx	eax, byte ptr [rdx + rsi + 1]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	movzx	eax, byte ptr [rdx + rsi + 2]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	movzx	eax, byte ptr [rdx + rsi + 3]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1477
	jmp	.LBB0_1526
.LBB0_440:
	test	r8d, r8d
	jle	.LBB0_1526
# %bb.441:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB0_442
# %bb.758:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_937
# %bb.759:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_937
.LBB0_442:
	xor	esi, esi
.LBB0_1314:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1316
.LBB0_1315:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1315
.LBB0_1316:
	cmp	r8, 3
	jb	.LBB0_1526
.LBB0_1317:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1317
	jmp	.LBB0_1526
.LBB0_446:
	and	esi, -4
	xor	edi, edi
.LBB0_447:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	rax, qword ptr [rdx + 8*rdi]
	mov	dword ptr [rcx + 4*rdi], eax
	cvttsd2si	rax, qword ptr [rdx + 8*rdi + 8]
	mov	dword ptr [rcx + 4*rdi + 4], eax
	cvttsd2si	rax, qword ptr [rdx + 8*rdi + 16]
	mov	dword ptr [rcx + 4*rdi + 8], eax
	cvttsd2si	rax, qword ptr [rdx + 8*rdi + 24]
	mov	dword ptr [rcx + 4*rdi + 12], eax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_447
.LBB0_448:
	test	r8, r8
	je	.LBB0_1526
# %bb.449:
	lea	rcx, [rcx + 4*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	esi, esi
.LBB0_450:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	rax, qword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_450
	jmp	.LBB0_1526
.LBB0_454:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_944
# %bb.455:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_456
	jmp	.LBB0_945
.LBB0_457:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_949
# %bb.458:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_459:                              # =>This Inner Loop Header: Depth=1
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_459
	jmp	.LBB0_950
.LBB0_460:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_954
# %bb.461:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_462:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi + 16]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 24]
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_462
	jmp	.LBB0_955
.LBB0_463:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_959
# %bb.464:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_465:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_465
	jmp	.LBB0_960
.LBB0_466:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_964
# %bb.467:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movaps	xmm1, xmmword ptr [rip + .LCPI0_3] # xmm1 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	movaps	xmm2, xmmword ptr [rip + .LCPI0_4] # xmm2 = [2147483648,2147483648,2147483648,2147483648]
.LBB0_468:                              # =>This Inner Loop Header: Depth=1
	movups	xmm3, xmmword ptr [rdx + 4*rdi]
	movups	xmm4, xmmword ptr [rdx + 4*rdi + 16]
	movaps	xmm0, xmm3
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm5, xmm3
	subps	xmm3, xmm1
	cvttps2dq	xmm3, xmm3
	xorps	xmm3, xmm2
	blendvps	xmm3, xmm5, xmm0
	movaps	xmm0, xmm4
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm5, xmm4
	subps	xmm4, xmm1
	cvttps2dq	xmm4, xmm4
	xorps	xmm4, xmm2
	blendvps	xmm4, xmm5, xmm0
	movups	xmmword ptr [rcx + 4*rdi], xmm3
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm4
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	movaps	xmm0, xmm3
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm4, xmm3
	subps	xmm3, xmm1
	cvttps2dq	xmm3, xmm3
	xorps	xmm3, xmm2
	blendvps	xmm3, xmm4, xmm0
	movups	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	movaps	xmm0, xmm4
	cmpltps	xmm0, xmm1
	cvttps2dq	xmm5, xmm4
	subps	xmm4, xmm1
	cvttps2dq	xmm4, xmm4
	xorps	xmm4, xmm2
	blendvps	xmm4, xmm5, xmm0
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm3
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm4
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_468
	jmp	.LBB0_965
.LBB0_475:
	and	esi, -4
	xor	edi, edi
.LBB0_476:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rdi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rcx + 8*rdi], xmm0
	mov	eax, dword ptr [rdx + 4*rdi + 4]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	mov	eax, dword ptr [rdx + 4*rdi + 8]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	mov	eax, dword ptr [rdx + 4*rdi + 12]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_476
.LBB0_477:
	test	r8, r8
	je	.LBB0_1526
# %bb.478:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 4*rdi]
	xor	esi, esi
.LBB0_479:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, rax
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_479
	jmp	.LBB0_1526
.LBB0_483:
	and	esi, -4
	xor	edi, edi
.LBB0_484:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rdi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi], xmm0
	movsx	eax, byte ptr [rdx + rdi + 1]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	movsx	eax, byte ptr [rdx + rdi + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	movsx	eax, byte ptr [rdx + rdi + 3]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_484
.LBB0_485:
	test	r8, r8
	je	.LBB0_1526
# %bb.486:
	lea	rcx, [rcx + 8*rdi]
	add	rdx, rdi
	xor	esi, esi
.LBB0_487:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_487
	jmp	.LBB0_1526
.LBB0_488:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_969
# %bb.489:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_6] # xmm1 = [4841369599423283200,4841369599423283200]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_7] # xmm2 = [4985484787499139072,4985484787499139072]
	movapd	xmm3, xmmword ptr [rip + .LCPI0_8] # xmm3 = [1.9342813118337666E+25,1.9342813118337666E+25]
.LBB0_490:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm4, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm5, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm6, xmm4
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm4, 32
	por	xmm4, xmm2
	subpd	xmm4, xmm3
	addpd	xmm4, xmm6
	movdqa	xmm6, xmm5
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm5, 32
	por	xmm5, xmm2
	subpd	xmm5, xmm3
	addpd	xmm5, xmm6
	movupd	xmmword ptr [rcx + 8*rdi], xmm4
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm5
	movdqu	xmm4, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm5, xmmword ptr [rdx + 8*rdi + 48]
	movdqa	xmm6, xmm4
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm4, 32
	por	xmm4, xmm2
	subpd	xmm4, xmm3
	addpd	xmm4, xmm6
	movdqa	xmm6, xmm5
	pblendw	xmm6, xmm0, 204                 # xmm6 = xmm6[0,1],xmm0[2,3],xmm6[4,5],xmm0[6,7]
	por	xmm6, xmm1
	psrlq	xmm5, 32
	por	xmm5, xmm2
	subpd	xmm5, xmm3
	addpd	xmm5, xmm6
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm4
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm5
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_490
	jmp	.LBB0_970
.LBB0_491:
	and	esi, -4
	xor	edi, edi
.LBB0_492:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rdi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi], xmm0
	movzx	eax, word ptr [rdx + 2*rdi + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	movzx	eax, word ptr [rdx + 2*rdi + 4]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	movzx	eax, word ptr [rdx + 2*rdi + 6]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_492
.LBB0_493:
	test	r8, r8
	je	.LBB0_1526
# %bb.494:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 2*rdi]
	xor	esi, esi
.LBB0_495:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_495
	jmp	.LBB0_1526
.LBB0_496:
	and	esi, -4
	xor	edi, edi
.LBB0_497:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rdi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi], xmm0
	movsx	eax, word ptr [rdx + 2*rdi + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	movsx	eax, word ptr [rdx + 2*rdi + 4]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	movsx	eax, word ptr [rdx + 2*rdi + 6]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_497
.LBB0_498:
	test	r8, r8
	je	.LBB0_1526
# %bb.499:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 2*rdi]
	xor	esi, esi
.LBB0_500:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_500
	jmp	.LBB0_1526
.LBB0_501:
	and	esi, -4
	xor	edi, edi
.LBB0_502:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdx + 8*rdi]
	movsd	qword ptr [rcx + 8*rdi], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdx + 8*rdi + 8]
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdx + 8*rdi + 16]
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdx + 8*rdi + 24]
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_502
.LBB0_503:
	test	rax, rax
	je	.LBB0_1526
# %bb.504:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	esi, esi
.LBB0_505:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB0_505
	jmp	.LBB0_1526
.LBB0_506:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_975
# %bb.507:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_508:                              # =>This Inner Loop Header: Depth=1
	cvtps2pd	xmm0, qword ptr [rdx + 4*rdi]
	cvtps2pd	xmm1, qword ptr [rdx + 4*rdi + 8]
	movups	xmmword ptr [rcx + 8*rdi], xmm0
	movups	xmmword ptr [rcx + 8*rdi + 16], xmm1
	cvtps2pd	xmm0, qword ptr [rdx + 4*rdi + 16]
	cvtps2pd	xmm1, qword ptr [rdx + 4*rdi + 24]
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_508
	jmp	.LBB0_976
.LBB0_509:
	and	esi, -4
	xor	edi, edi
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi], xmm0
	movzx	eax, byte ptr [rdx + rdi + 1]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	movzx	eax, byte ptr [rdx + rdi + 2]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	movzx	eax, byte ptr [rdx + rdi + 3]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_510
.LBB0_511:
	test	r8, r8
	je	.LBB0_1526
# %bb.512:
	lea	rcx, [rcx + 8*rdi]
	add	rdx, rdi
	xor	esi, esi
.LBB0_513:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, eax
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_513
	jmp	.LBB0_1526
.LBB0_514:
	and	esi, -4
	xor	edi, edi
.LBB0_515:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdx + 4*rdi]
	movsd	qword ptr [rcx + 8*rdi], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdx + 4*rdi + 4]
	movsd	qword ptr [rcx + 8*rdi + 8], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdx + 4*rdi + 8]
	movsd	qword ptr [rcx + 8*rdi + 16], xmm0
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdx + 4*rdi + 12]
	movsd	qword ptr [rcx + 8*rdi + 24], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_515
.LBB0_516:
	test	rax, rax
	je	.LBB0_1526
# %bb.517:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 4*rdi]
	xor	esi, esi
.LBB0_518:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2sd	xmm0, dword ptr [rdx + 4*rsi]
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB0_518
	jmp	.LBB0_1526
.LBB0_549:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_801
# %bb.550:
	xor	eax, eax
	jmp	.LBB0_803
.LBB0_551:
	and	r9d, -4
	xor	eax, eax
	movsd	xmm0, qword ptr [rip + .LCPI0_0] # xmm0 = mem[0],zero
.LBB0_552:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rax]   # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rdi, xmm2
	xor	rdi, r10
	cvttsd2si	rsi, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rsi, rdi
	mov	qword ptr [rcx + 8*rax], rsi
	movsd	xmm1, qword ptr [rdx + 8*rax + 8] # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rsi, xmm2
	xor	rsi, r10
	cvttsd2si	rdi, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdi, rsi
	mov	qword ptr [rcx + 8*rax + 8], rdi
	movsd	xmm1, qword ptr [rdx + 8*rax + 16] # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rsi, xmm2
	xor	rsi, r10
	cvttsd2si	rdi, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdi, rsi
	mov	qword ptr [rcx + 8*rax + 16], rdi
	movsd	xmm1, qword ptr [rdx + 8*rax + 24] # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rsi, xmm2
	xor	rsi, r10
	cvttsd2si	rdi, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdi, rsi
	mov	qword ptr [rcx + 8*rax + 24], rdi
	add	rax, 4
	cmp	r9, rax
	jne	.LBB0_552
.LBB0_553:
	test	r8, r8
	je	.LBB0_1526
# %bb.554:
	lea	rcx, [rcx + 8*rax]
	lea	rax, [rdx + 8*rax]
	xor	edx, edx
	movsd	xmm0, qword ptr [rip + .LCPI0_0] # xmm0 = mem[0],zero
.LBB0_555:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rax + 8*rdx]   # xmm1 = mem[0],zero
	movapd	xmm2, xmm1
	subsd	xmm2, xmm0
	cvttsd2si	rsi, xmm2
	xor	rsi, r10
	cvttsd2si	rdi, xmm1
	ucomisd	xmm0, xmm1
	cmovbe	rdi, rsi
	mov	qword ptr [rcx + 8*rdx], rdi
	add	rdx, 1
	cmp	r8, rdx
	jne	.LBB0_555
	jmp	.LBB0_1526
.LBB0_562:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_812
# %bb.563:
	xor	eax, eax
	jmp	.LBB0_814
.LBB0_564:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_819
# %bb.565:
	xor	eax, eax
	jmp	.LBB0_821
.LBB0_569:
	and	r9d, -4
	xor	edi, edi
	movss	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = mem[0],zero,zero,zero
	movabs	r10, -9223372036854775808
.LBB0_570:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rdi]   # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rsi, xmm2
	xor	rsi, r10
	cvttss2si	rax, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rax, rsi
	mov	qword ptr [rcx + 8*rdi], rax
	movss	xmm1, dword ptr [rdx + 4*rdi + 4] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rax, xmm2
	xor	rax, r10
	cvttss2si	rsi, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rsi, rax
	mov	qword ptr [rcx + 8*rdi + 8], rsi
	movss	xmm1, dword ptr [rdx + 4*rdi + 8] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rax, xmm2
	xor	rax, r10
	cvttss2si	rsi, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rsi, rax
	mov	qword ptr [rcx + 8*rdi + 16], rsi
	movss	xmm1, dword ptr [rdx + 4*rdi + 12] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rax, xmm2
	xor	rax, r10
	cvttss2si	rsi, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rsi, rax
	mov	qword ptr [rcx + 8*rdi + 24], rsi
	add	rdi, 4
	cmp	r9, rdi
	jne	.LBB0_570
.LBB0_571:
	test	r8, r8
	je	.LBB0_1526
# %bb.572:
	lea	rax, [rcx + 8*rdi]
	lea	rcx, [rdx + 4*rdi]
	xor	edx, edx
	movss	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = mem[0],zero,zero,zero
	movabs	r9, -9223372036854775808
.LBB0_573:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm1
	subss	xmm2, xmm0
	cvttss2si	rdi, xmm2
	xor	rdi, r9
	cvttss2si	rsi, xmm1
	ucomiss	xmm0, xmm1
	cmovbe	rsi, rdi
	mov	qword ptr [rax + 8*rdx], rsi
	add	rdx, 1
	cmp	r8, rdx
	jne	.LBB0_573
	jmp	.LBB0_1526
.LBB0_577:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_830
# %bb.578:
	xor	eax, eax
	jmp	.LBB0_832
.LBB0_579:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_980
# %bb.580:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
.LBB0_581:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_581
	jmp	.LBB0_981
.LBB0_582:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_985
# %bb.583:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
.LBB0_584:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_584
	jmp	.LBB0_986
.LBB0_585:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_990
# %bb.586:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_587:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	cvttpd2dq	xmm0, xmm0
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi + 8], xmm0
	movd	dword ptr [rcx + 2*rdi + 12], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_587
	jmp	.LBB0_991
.LBB0_588:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_995
# %bb.589:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_590:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	cvttpd2dq	xmm0, xmm0
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi + 8], xmm0
	movd	dword ptr [rcx + 2*rdi + 12], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_590
	jmp	.LBB0_996
.LBB0_597:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1000
# %bb.598:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_599:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi + 8], xmm0
	movd	dword ptr [rcx + 2*rdi + 12], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_599
	jmp	.LBB0_1001
.LBB0_600:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1005
# %bb.601:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_602:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi + 8], xmm0
	movd	dword ptr [rcx + 2*rdi + 12], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_602
	jmp	.LBB0_1006
.LBB0_615:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1010
# %bb.616:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_617:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi + 8], xmm0
	movd	dword ptr [rcx + 2*rdi + 12], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_617
	jmp	.LBB0_1011
.LBB0_618:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1015
# %bb.619:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_620:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi + 8], xmm0
	movd	dword ptr [rcx + 2*rdi + 12], xmm1
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_620
	jmp	.LBB0_1016
.LBB0_621:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1020
# %bb.622:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_623:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_623
	jmp	.LBB0_1021
.LBB0_624:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1025
# %bb.625:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_626:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_626
	jmp	.LBB0_1026
.LBB0_633:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1030
# %bb.634:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
.LBB0_635:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_635
	jmp	.LBB0_1031
.LBB0_636:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1035
# %bb.637:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
.LBB0_638:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_638
	jmp	.LBB0_1036
.LBB0_639:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_857
# %bb.640:
	xor	eax, eax
	jmp	.LBB0_859
.LBB0_641:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1040
# %bb.642:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_14] # xmm0 = [1258291200,1258291200,1258291200,1258291200]
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_15] # xmm1 = [1392508928,1392508928,1392508928,1392508928]
	movaps	xmm2, xmmword ptr [rip + .LCPI0_16] # xmm2 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
.LBB0_643:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm4, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm5, xmm3
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm3, 16
	pblendw	xmm3, xmm1, 170                 # xmm3 = xmm3[0],xmm1[1],xmm3[2],xmm1[3],xmm3[4],xmm1[5],xmm3[6],xmm1[7]
	subps	xmm3, xmm2
	addps	xmm3, xmm5
	movdqa	xmm5, xmm4
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm4, 16
	pblendw	xmm4, xmm1, 170                 # xmm4 = xmm4[0],xmm1[1],xmm4[2],xmm1[3],xmm4[4],xmm1[5],xmm4[6],xmm1[7]
	subps	xmm4, xmm2
	addps	xmm4, xmm5
	movups	xmmword ptr [rcx + 4*rdi], xmm3
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm4
	movdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	movdqa	xmm5, xmm3
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm3, 16
	pblendw	xmm3, xmm1, 170                 # xmm3 = xmm3[0],xmm1[1],xmm3[2],xmm1[3],xmm3[4],xmm1[5],xmm3[6],xmm1[7]
	subps	xmm3, xmm2
	addps	xmm3, xmm5
	movdqa	xmm5, xmm4
	pblendw	xmm5, xmm0, 170                 # xmm5 = xmm5[0],xmm0[1],xmm5[2],xmm0[3],xmm5[4],xmm0[5],xmm5[6],xmm0[7]
	psrld	xmm4, 16
	pblendw	xmm4, xmm1, 170                 # xmm4 = xmm4[0],xmm1[1],xmm4[2],xmm1[3],xmm4[4],xmm1[5],xmm4[6],xmm1[7]
	subps	xmm4, xmm2
	addps	xmm4, xmm5
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm3
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm4
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_643
	jmp	.LBB0_1041
.LBB0_644:
	and	esi, -4
	xor	edi, edi
.LBB0_645:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rcx + 8*rdi], rax
	cvttsd2si	rax, qword ptr [rdx + 8*rdi + 8]
	mov	qword ptr [rcx + 8*rdi + 8], rax
	cvttsd2si	rax, qword ptr [rdx + 8*rdi + 16]
	mov	qword ptr [rcx + 8*rdi + 16], rax
	cvttsd2si	rax, qword ptr [rdx + 8*rdi + 24]
	mov	qword ptr [rcx + 8*rdi + 24], rax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_645
.LBB0_646:
	test	r8, r8
	je	.LBB0_1526
# %bb.647:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	esi, esi
.LBB0_648:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_648
	jmp	.LBB0_1526
.LBB0_649:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1045
# %bb.650:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_651:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvtpd2ps	xmm0, xmm0
	cvtpd2ps	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	cvtpd2ps	xmm0, xmm0
	cvtpd2ps	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_651
	jmp	.LBB0_1046
.LBB0_661:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1050
# %bb.662:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edi, edi
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_11] # xmm2 = [1,1]
.LBB0_663:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqa	xmm1, xmm0
	pand	xmm1, xmm2
	movdqa	xmm3, xmm0
	psrlq	xmm3, 1
	por	xmm3, xmm1
	pxor	xmm4, xmm4
	pcmpgtq	xmm4, xmm0
	blendvpd	xmm0, xmm3, xmm0
	pextrq	rax, xmm0, 1
	xorps	xmm5, xmm5
	cvtsi2ss	xmm5, rax
	movq	rax, xmm0
	xorps	xmm3, xmm3
	cvtsi2ss	xmm3, rax
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	insertps	xmm3, xmm5, 28                  # xmm3 = xmm3[0],xmm5[0],zero,zero
	movaps	xmm5, xmm3
	addps	xmm5, xmm3
	pshufd	xmm0, xmm4, 237                 # xmm0 = xmm4[1,3,2,3]
	blendvps	xmm3, xmm5, xmm0
	movdqa	xmm0, xmm1
	pand	xmm0, xmm2
	movdqa	xmm4, xmm1
	psrlq	xmm4, 1
	por	xmm4, xmm0
	xorps	xmm5, xmm5
	pcmpgtq	xmm5, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	pextrq	rax, xmm1, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movq	rax, xmm1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, rax
	insertps	xmm1, xmm0, 28                  # xmm1 = xmm1[0],xmm0[0],zero,zero
	movaps	xmm4, xmm1
	addps	xmm4, xmm1
	pshufd	xmm0, xmm5, 237                 # xmm0 = xmm5[1,3,2,3]
	blendvps	xmm1, xmm4, xmm0
	movlhps	xmm3, xmm1                      # xmm3 = xmm3[0],xmm1[0]
	movups	xmmword ptr [rcx + 4*rdi], xmm3
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqa	xmm1, xmm0
	pand	xmm1, xmm2
	movdqa	xmm3, xmm0
	psrlq	xmm3, 1
	por	xmm3, xmm1
	xorps	xmm4, xmm4
	pcmpgtq	xmm4, xmm0
	blendvpd	xmm0, xmm3, xmm0
	pextrq	rax, xmm0, 1
	xorps	xmm5, xmm5
	cvtsi2ss	xmm5, rax
	movq	rax, xmm0
	xorps	xmm3, xmm3
	cvtsi2ss	xmm3, rax
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	insertps	xmm3, xmm5, 28                  # xmm3 = xmm3[0],xmm5[0],zero,zero
	movaps	xmm5, xmm3
	addps	xmm5, xmm3
	pshufd	xmm0, xmm4, 237                 # xmm0 = xmm4[1,3,2,3]
	blendvps	xmm3, xmm5, xmm0
	movdqa	xmm0, xmm1
	pand	xmm0, xmm2
	movdqa	xmm4, xmm1
	psrlq	xmm4, 1
	por	xmm4, xmm0
	xorps	xmm5, xmm5
	pcmpgtq	xmm5, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	pextrq	rax, xmm1, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movq	rax, xmm1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, rax
	insertps	xmm1, xmm0, 28                  # xmm1 = xmm1[0],xmm0[0],zero,zero
	movaps	xmm4, xmm1
	addps	xmm4, xmm1
	pshufd	xmm0, xmm5, 237                 # xmm0 = xmm5[1,3,2,3]
	blendvps	xmm1, xmm4, xmm0
	movlhps	xmm3, xmm1                      # xmm3 = xmm3[0],xmm1[0]
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm3
	add	rdi, 8
	add	r10, 2
	jne	.LBB0_663
	jmp	.LBB0_1051
.LBB0_664:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_871
# %bb.665:
	xor	eax, eax
	jmp	.LBB0_873
.LBB0_666:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1058
# %bb.667:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_668:                              # =>This Inner Loop Header: Depth=1
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_668
	jmp	.LBB0_1059
.LBB0_669:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_878
# %bb.670:
	xor	eax, eax
	jmp	.LBB0_880
.LBB0_671:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1063
# %bb.672:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_673:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi + 16]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 24]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_673
	jmp	.LBB0_1064
.LBB0_677:
	and	esi, -4
	xor	edi, edi
.LBB0_678:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdx + 8*rdi]
	movss	dword ptr [rcx + 4*rdi], xmm0
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdx + 8*rdi + 8]
	movss	dword ptr [rcx + 4*rdi + 4], xmm0
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdx + 8*rdi + 16]
	movss	dword ptr [rcx + 4*rdi + 8], xmm0
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdx + 8*rdi + 24]
	movss	dword ptr [rcx + 4*rdi + 12], xmm0
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_678
.LBB0_679:
	test	rax, rax
	je	.LBB0_1526
# %bb.680:
	lea	rcx, [rcx + 4*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	esi, esi
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, qword ptr [rdx + 8*rsi]
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB0_681
	jmp	.LBB0_1526
.LBB0_682:
	and	esi, -4
	xor	edi, edi
.LBB0_683:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	rax, dword ptr [rdx + 4*rdi]
	mov	qword ptr [rcx + 8*rdi], rax
	cvttss2si	rax, dword ptr [rdx + 4*rdi + 4]
	mov	qword ptr [rcx + 8*rdi + 8], rax
	cvttss2si	rax, dword ptr [rdx + 4*rdi + 8]
	mov	qword ptr [rcx + 8*rdi + 16], rax
	cvttss2si	rax, dword ptr [rdx + 4*rdi + 12]
	mov	qword ptr [rcx + 8*rdi + 24], rax
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_683
.LBB0_684:
	test	r8, r8
	je	.LBB0_1526
# %bb.685:
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 4*rdi]
	xor	esi, esi
.LBB0_686:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	rax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r8, rsi
	jne	.LBB0_686
	jmp	.LBB0_1526
.LBB0_696:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_894
# %bb.697:
	xor	eax, eax
	jmp	.LBB0_896
.LBB0_698:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1068
# %bb.699:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_700:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_700
	jmp	.LBB0_1069
.LBB0_734:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1073
# %bb.735:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_736:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_736
	jmp	.LBB0_1074
.LBB0_740:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_939
# %bb.741:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_742:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_742
	jmp	.LBB0_940
.LBB0_743:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1078
# %bb.744:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_745:                              # =>This Inner Loop Header: Depth=1
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_745
	jmp	.LBB0_1079
.LBB0_746:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1083
# %bb.747:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_748:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi + 16]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 24]
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_748
	jmp	.LBB0_1084
.LBB0_749:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1088
# %bb.750:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_751:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_751
	jmp	.LBB0_1089
.LBB0_752:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1093
# %bb.753:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_754:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_754
	jmp	.LBB0_1094
.LBB0_761:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB0_1098
# %bb.762:
	xor	eax, eax
	jmp	.LBB0_1100
.LBB0_763:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1478
# %bb.764:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_765:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm0, dword ptr [rdx + rdi]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovsxbd	xmm0, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 12]
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_765
	jmp	.LBB0_1479
.LBB0_766:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1486
# %bb.767:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_768:                              # =>This Inner Loop Header: Depth=1
	pmovzxbd	xmm0, dword ptr [rdx + rdi]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovzxbd	xmm0, dword ptr [rdx + rdi + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_768
	jmp	.LBB0_1487
.LBB0_769:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB0_1108
# %bb.770:
	xor	eax, eax
	jmp	.LBB0_1110
.LBB0_771:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1118
# %bb.772:
	xor	eax, eax
	jmp	.LBB0_1120
.LBB0_773:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1494
# %bb.774:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_13] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_775:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi], xmm1
	movd	dword ptr [rcx + rdi + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi + 8], xmm1
	movd	dword ptr [rcx + rdi + 12], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_775
	jmp	.LBB0_1495
.LBB0_776:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1502
# %bb.777:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_778:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 8*rdi]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 2], xmm2, 0
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 6], xmm2, 0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_778
	jmp	.LBB0_1503
.LBB0_779:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1128
# %bb.780:
	xor	eax, eax
	jmp	.LBB0_1130
.LBB0_781:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1318
# %bb.782:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_783:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 6], xmm2, 0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_783
	jmp	.LBB0_1319
.LBB0_784:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1326
# %bb.785:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_17] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
.LBB0_786:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_786
	jmp	.LBB0_1327
.LBB0_787:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1510
# %bb.788:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_17] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
.LBB0_789:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_789
	jmp	.LBB0_1511
.LBB0_790:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1334
# %bb.791:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_792:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 6], xmm2, 0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_792
	jmp	.LBB0_1335
.LBB0_793:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1518
# %bb.794:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_795:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	cvttps2dq	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movd	dword ptr [rcx + rdi + 8], xmm0
	movd	dword ptr [rcx + rdi + 12], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_795
	jmp	.LBB0_1519
.LBB0_796:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1138
# %bb.797:
	xor	eax, eax
	jmp	.LBB0_1140
.LBB0_798:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1342
# %bb.799:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_13] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_800:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi], xmm1
	movd	dword ptr [rcx + rdi + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi + 8], xmm1
	movd	dword ptr [rcx + rdi + 12], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_800
	jmp	.LBB0_1343
.LBB0_808:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1148
# %bb.809:
	xor	eax, eax
	jmp	.LBB0_1150
.LBB0_810:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1158
# %bb.811:
	xor	eax, eax
	jmp	.LBB0_1160
.LBB0_826:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1168
# %bb.827:
	xor	eax, eax
	jmp	.LBB0_1170
.LBB0_828:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1178
# %bb.829:
	xor	eax, eax
	jmp	.LBB0_1180
.LBB0_837:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1350
# %bb.838:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_839:                              # =>This Inner Loop Header: Depth=1
	pmovsxbw	xmm0, qword ptr [rdx + rdi]
	pmovsxbw	xmm1, qword ptr [rdx + rdi + 8]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	pmovsxbw	xmm0, qword ptr [rdx + rdi + 16]
	pmovsxbw	xmm1, qword ptr [rdx + rdi + 24]
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_839
	jmp	.LBB0_1351
.LBB0_840:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1358
# %bb.841:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_842:                              # =>This Inner Loop Header: Depth=1
	pmovsxbw	xmm0, qword ptr [rdx + rdi]
	pmovsxbw	xmm1, qword ptr [rdx + rdi + 8]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	pmovsxbw	xmm0, qword ptr [rdx + rdi + 16]
	pmovsxbw	xmm1, qword ptr [rdx + rdi + 24]
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_842
	jmp	.LBB0_1359
.LBB0_843:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 48
	jae	.LBB0_1188
# %bb.844:
	xor	eax, eax
	jmp	.LBB0_1190
.LBB0_845:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 48
	jae	.LBB0_1198
# %bb.846:
	xor	eax, eax
	jmp	.LBB0_1200
.LBB0_847:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 48
	jae	.LBB0_1208
# %bb.848:
	xor	eax, eax
	jmp	.LBB0_1210
.LBB0_849:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 48
	jae	.LBB0_1218
# %bb.850:
	xor	eax, eax
	jmp	.LBB0_1220
.LBB0_851:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1366
# %bb.852:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_853:                              # =>This Inner Loop Header: Depth=1
	pmovzxbw	xmm0, qword ptr [rdx + rdi]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdx + rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	pmovzxbw	xmm0, qword ptr [rdx + rdi + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdx + rdi + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_853
	jmp	.LBB0_1367
.LBB0_854:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1374
# %bb.855:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_856:                              # =>This Inner Loop Header: Depth=1
	pmovzxbw	xmm0, qword ptr [rdx + rdi]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdx + rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	pmovzxbw	xmm0, qword ptr [rdx + rdi + 16] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdx + rdi + 24] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_856
	jmp	.LBB0_1375
.LBB0_864:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1228
# %bb.865:
	xor	eax, eax
	jmp	.LBB0_1230
.LBB0_866:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1382
# %bb.867:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_868:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm0, dword ptr [rdx + rdi]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovsxbd	xmm0, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 12]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_868
	jmp	.LBB0_1383
.LBB0_869:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1238
# %bb.870:
	xor	eax, eax
	jmp	.LBB0_1240
.LBB0_885:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1248
# %bb.886:
	xor	eax, eax
	jmp	.LBB0_1250
.LBB0_887:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB0_1258
# %bb.888:
	xor	eax, eax
	jmp	.LBB0_1260
.LBB0_889:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_1268
# %bb.890:
	xor	eax, eax
	jmp	.LBB0_1270
.LBB0_891:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1390
# %bb.892:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_893:                              # =>This Inner Loop Header: Depth=1
	pmovzxbd	xmm0, dword ptr [rdx + rdi]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovzxbd	xmm0, dword ptr [rdx + rdi + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_893
	jmp	.LBB0_1391
.LBB0_901:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1398
# %bb.902:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_13] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_903:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi], xmm1
	movd	dword ptr [rcx + rdi + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi + 8], xmm1
	movd	dword ptr [rcx + rdi + 12], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_903
	jmp	.LBB0_1399
.LBB0_904:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1406
# %bb.905:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_906:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 8*rdi]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 2], xmm2, 0
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	cvttpd2dq	xmm1, xmm1
	cvttpd2dq	xmm2, xmm2
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 6], xmm2, 0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_906
	jmp	.LBB0_1407
.LBB0_907:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1278
# %bb.908:
	xor	eax, eax
	jmp	.LBB0_1280
.LBB0_909:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1414
# %bb.910:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_911:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 6], xmm2, 0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_911
	jmp	.LBB0_1415
.LBB0_912:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1422
# %bb.913:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_17] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
.LBB0_914:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_914
	jmp	.LBB0_1423
.LBB0_915:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1430
# %bb.916:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_17] # xmm0 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
.LBB0_917:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi], xmm1
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	punpcklqdq	xmm1, xmm2              # xmm1 = xmm1[0],xmm2[0]
	movdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_917
	jmp	.LBB0_1431
.LBB0_918:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB0_1438
# %bb.919:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_5] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_920:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 2], xmm2, 0
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pshufb	xmm1, xmm0
	pextrw	word ptr [rcx + rdi + 4], xmm1, 0
	pshufb	xmm2, xmm0
	pextrw	word ptr [rcx + rdi + 6], xmm2, 0
	add	rdi, 8
	add	rax, 2
	jne	.LBB0_920
	jmp	.LBB0_1439
.LBB0_921:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1446
# %bb.922:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_923:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	packusdw	xmm0, xmm0
	packuswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm1, xmm1
	packuswb	xmm1, xmm1
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	cvttps2dq	xmm0, xmm0
	packusdw	xmm0, xmm0
	packuswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm1, xmm1
	packuswb	xmm1, xmm1
	movd	dword ptr [rcx + rdi + 8], xmm0
	movd	dword ptr [rcx + rdi + 12], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_923
	jmp	.LBB0_1447
.LBB0_924:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1288
# %bb.925:
	xor	eax, eax
	jmp	.LBB0_1290
.LBB0_926:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1454
# %bb.927:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_13] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_928:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi], xmm1
	movd	dword ptr [rcx + rdi + 4], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	pshufb	xmm1, xmm0
	pshufb	xmm2, xmm0
	movd	dword ptr [rcx + rdi + 8], xmm1
	movd	dword ptr [rcx + rdi + 12], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_928
	jmp	.LBB0_1455
.LBB0_929:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB0_1298
# %bb.930:
	xor	eax, eax
	jmp	.LBB0_1300
.LBB0_931:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1462
# %bb.932:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_933:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm0, dword ptr [rdx + rdi]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovsxbd	xmm0, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 12]
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_933
	jmp	.LBB0_1463
.LBB0_934:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB0_1470
# %bb.935:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_936:                              # =>This Inner Loop Header: Depth=1
	pmovzxbd	xmm0, dword ptr [rdx + rdi]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	pmovzxbd	xmm0, dword ptr [rdx + rdi + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB0_936
	jmp	.LBB0_1471
.LBB0_937:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB0_1308
# %bb.938:
	xor	eax, eax
	jmp	.LBB0_1310
.LBB0_801:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_802:                              # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax]   # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 8] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax + 16] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 24] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax + 32] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 40] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax + 48] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 56] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_802
.LBB0_803:
	test	r8, r8
	je	.LBB0_806
# %bb.804:
	lea	rax, [4*rax + 8]
	neg	r8
.LBB0_805:                              # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdx + rax - 8] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + rax]     # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rcx + 2*rax], xmm1
	add	rax, 16
	inc	r8
	jne	.LBB0_805
.LBB0_806:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_807:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_807
	jmp	.LBB0_1526
.LBB0_812:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_813:                              # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax]   # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax + 16] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 20] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax + 24] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 28] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_813
.LBB0_814:
	test	r8, r8
	je	.LBB0_817
# %bb.815:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rdx + 2*rax]
	add	r10, 4
	xor	eax, eax
.LBB0_816:                              # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [r10 + 8*rax - 4] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [r10 + 8*rax]   # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_816
.LBB0_817:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_818:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_818
	jmp	.LBB0_1526
.LBB0_819:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_820:                              # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 4]
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax + 8]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 12]
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax + 16]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 20]
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax + 24]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 28]
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_820
.LBB0_821:
	test	r8, r8
	je	.LBB0_824
# %bb.822:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rdx + 2*rax]
	add	r10, 4
	xor	eax, eax
.LBB0_823:                              # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [r10 + 8*rax - 4]
	pmovsxwq	xmm1, dword ptr [r10 + 8*rax]
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_823
.LBB0_824:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_825:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_825
	jmp	.LBB0_1526
.LBB0_830:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_831:                              # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 8]
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax + 16]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 24]
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax + 32]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 40]
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax + 48]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 56]
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_831
.LBB0_832:
	test	r8, r8
	je	.LBB0_835
# %bb.833:
	lea	rax, [4*rax + 8]
	neg	r8
.LBB0_834:                              # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdx + rax - 8]
	pmovsxdq	xmm1, qword ptr [rdx + rax]
	movdqu	xmmword ptr [rcx + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rcx + 2*rax], xmm1
	add	rax, 16
	inc	r8
	jne	.LBB0_834
.LBB0_835:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_836:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_836
	jmp	.LBB0_1526
.LBB0_857:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_858:                              # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax]   # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 8] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax + 16] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 24] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax + 32] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 40] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovzxdq	xmm0, qword ptr [rdx + 4*rax + 48] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + 4*rax + 56] # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_858
.LBB0_859:
	test	r8, r8
	je	.LBB0_862
# %bb.860:
	lea	rax, [4*rax + 8]
	neg	r8
.LBB0_861:                              # =>This Inner Loop Header: Depth=1
	pmovzxdq	xmm0, qword ptr [rdx + rax - 8] # xmm0 = mem[0],zero,mem[1],zero
	pmovzxdq	xmm1, qword ptr [rdx + rax]     # xmm1 = mem[0],zero,mem[1],zero
	movdqu	xmmword ptr [rcx + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rcx + 2*rax], xmm1
	add	rax, 16
	inc	r8
	jne	.LBB0_861
.LBB0_862:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_863:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_863
	jmp	.LBB0_1526
.LBB0_871:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_872:                              # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax]   # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax + 8] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 12] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax + 16] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 20] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovzxwq	xmm0, dword ptr [rdx + 2*rax + 24] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [rdx + 2*rax + 28] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_872
.LBB0_873:
	test	r8, r8
	je	.LBB0_876
# %bb.874:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rdx + 2*rax]
	add	r10, 4
	xor	eax, eax
.LBB0_875:                              # =>This Inner Loop Header: Depth=1
	pmovzxwq	xmm0, dword ptr [r10 + 8*rax - 4] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	pmovzxwq	xmm1, dword ptr [r10 + 8*rax]   # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_875
.LBB0_876:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_877:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_877
	jmp	.LBB0_1526
.LBB0_878:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_879:                              # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 4]
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax + 8]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 12]
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax + 16]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 20]
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovsxwq	xmm0, dword ptr [rdx + 2*rax + 24]
	pmovsxwq	xmm1, dword ptr [rdx + 2*rax + 28]
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_879
.LBB0_880:
	test	r8, r8
	je	.LBB0_883
# %bb.881:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rdx + 2*rax]
	add	r10, 4
	xor	eax, eax
.LBB0_882:                              # =>This Inner Loop Header: Depth=1
	pmovsxwq	xmm0, dword ptr [r10 + 8*rax - 4]
	pmovsxwq	xmm1, dword ptr [r10 + 8*rax]
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_882
.LBB0_883:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_884:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_884
	jmp	.LBB0_1526
.LBB0_894:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_895:                              # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 8]
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax + 16]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 24]
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax + 32]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 40]
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovsxdq	xmm0, qword ptr [rdx + 4*rax + 48]
	pmovsxdq	xmm1, qword ptr [rdx + 4*rax + 56]
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_895
.LBB0_896:
	test	r8, r8
	je	.LBB0_899
# %bb.897:
	lea	rax, [4*rax + 8]
	neg	r8
.LBB0_898:                              # =>This Inner Loop Header: Depth=1
	pmovsxdq	xmm0, qword ptr [rdx + rax - 8]
	pmovsxdq	xmm1, qword ptr [rdx + rax]
	movdqu	xmmword ptr [rcx + 2*rax - 16], xmm0
	movdqu	xmmword ptr [rcx + 2*rax], xmm1
	add	rax, 16
	inc	r8
	jne	.LBB0_898
.LBB0_899:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_900:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_900
	jmp	.LBB0_1526
.LBB0_939:
	xor	edi, edi
.LBB0_940:
	test	r8b, 1
	je	.LBB0_942
# %bb.941:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
.LBB0_942:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_943:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_943
	jmp	.LBB0_1526
.LBB0_944:
	xor	edi, edi
.LBB0_945:
	test	r8b, 1
	je	.LBB0_947
# %bb.946:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
.LBB0_947:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_948:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_948
	jmp	.LBB0_1526
.LBB0_949:
	xor	edi, edi
.LBB0_950:
	test	r8b, 1
	je	.LBB0_952
# %bb.951:
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_952:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_953:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_953
	jmp	.LBB0_1526
.LBB0_954:
	xor	edi, edi
.LBB0_955:
	test	r8b, 1
	je	.LBB0_957
# %bb.956:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_957:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_958:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_958
	jmp	.LBB0_1526
.LBB0_959:
	xor	edi, edi
.LBB0_960:
	test	r8b, 1
	je	.LBB0_962
# %bb.961:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
.LBB0_962:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_963:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_963
	jmp	.LBB0_1526
.LBB0_964:
	xor	edi, edi
.LBB0_965:
	test	r8b, 1
	je	.LBB0_967
# %bb.966:
	movups	xmm1, xmmword ptr [rdx + 4*rdi]
	movups	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	movaps	xmm3, xmmword ptr [rip + .LCPI0_3] # xmm3 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	movaps	xmm0, xmm1
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm4, xmm1
	subps	xmm1, xmm3
	cvttps2dq	xmm1, xmm1
	movaps	xmm5, xmmword ptr [rip + .LCPI0_4] # xmm5 = [2147483648,2147483648,2147483648,2147483648]
	xorps	xmm1, xmm5
	blendvps	xmm1, xmm4, xmm0
	movaps	xmm0, xmm2
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm4, xmm2
	subps	xmm2, xmm3
	cvttps2dq	xmm2, xmm2
	xorps	xmm2, xmm5
	blendvps	xmm2, xmm4, xmm0
	movups	xmmword ptr [rcx + 4*rdi], xmm1
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm2
.LBB0_967:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_968:                              # =>This Inner Loop Header: Depth=1
	cvttss2si	rax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_968
	jmp	.LBB0_1526
.LBB0_969:
	xor	edi, edi
.LBB0_970:
	test	r8b, 1
	je	.LBB0_972
# %bb.971:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	movdqa	xmm3, xmm0
	pblendw	xmm3, xmm2, 204                 # xmm3 = xmm3[0,1],xmm2[2,3],xmm3[4,5],xmm2[6,7]
	movdqa	xmm4, xmmword ptr [rip + .LCPI0_6] # xmm4 = [4841369599423283200,4841369599423283200]
	por	xmm3, xmm4
	psrlq	xmm0, 32
	movdqa	xmm5, xmmword ptr [rip + .LCPI0_7] # xmm5 = [4985484787499139072,4985484787499139072]
	por	xmm0, xmm5
	movapd	xmm6, xmmword ptr [rip + .LCPI0_8] # xmm6 = [1.9342813118337666E+25,1.9342813118337666E+25]
	subpd	xmm0, xmm6
	addpd	xmm0, xmm3
	pblendw	xmm2, xmm1, 51                  # xmm2 = xmm1[0,1],xmm2[2,3],xmm1[4,5],xmm2[6,7]
	por	xmm2, xmm4
	psrlq	xmm1, 32
	por	xmm1, xmm5
	subpd	xmm1, xmm6
	addpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB0_972:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_973:
	movapd	xmm0, xmmword ptr [rip + .LCPI0_9] # xmm0 = [1127219200,1160773632,0,0]
	movapd	xmm1, xmmword ptr [rip + .LCPI0_10] # xmm1 = [4.503599627370496E+15,1.9342813113834067E+25]
.LBB0_974:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm2, qword ptr [rdx + 8*rsi]   # xmm2 = mem[0],zero
	unpcklps	xmm2, xmm0                      # xmm2 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	subpd	xmm2, xmm1
	movapd	xmm3, xmm2
	unpckhpd	xmm3, xmm2                      # xmm3 = xmm3[1],xmm2[1]
	addsd	xmm3, xmm2
	movsd	qword ptr [rcx + 8*rsi], xmm3
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_974
	jmp	.LBB0_1526
.LBB0_975:
	xor	edi, edi
.LBB0_976:
	test	r8b, 1
	je	.LBB0_978
# %bb.977:
	cvtps2pd	xmm0, qword ptr [rdx + 4*rdi]
	cvtps2pd	xmm1, qword ptr [rdx + 4*rdi + 8]
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB0_978:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_979:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	cvtss2sd	xmm0, xmm0
	movsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_979
	jmp	.LBB0_1526
.LBB0_980:
	xor	edi, edi
.LBB0_981:
	test	r8b, 1
	je	.LBB0_983
# %bb.982:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_12] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB0_983:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_984:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_984
	jmp	.LBB0_1526
.LBB0_985:
	xor	edi, edi
.LBB0_986:
	test	r8b, 1
	je	.LBB0_988
# %bb.987:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_12] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB0_988:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_989:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_989
	jmp	.LBB0_1526
.LBB0_990:
	xor	edi, edi
.LBB0_991:
	test	r8b, 1
	je	.LBB0_993
# %bb.992:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
.LBB0_993:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_994:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_994
	jmp	.LBB0_1526
.LBB0_995:
	xor	edi, edi
.LBB0_996:
	test	r8b, 1
	je	.LBB0_998
# %bb.997:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
.LBB0_998:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_999:                              # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_999
	jmp	.LBB0_1526
.LBB0_1000:
	xor	edi, edi
.LBB0_1001:
	test	r8b, 1
	je	.LBB0_1003
# %bb.1002:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
.LBB0_1003:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1004:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1004
	jmp	.LBB0_1526
.LBB0_1005:
	xor	edi, edi
.LBB0_1006:
	test	r8b, 1
	je	.LBB0_1008
# %bb.1007:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
.LBB0_1008:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1009:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1009
	jmp	.LBB0_1526
.LBB0_1010:
	xor	edi, edi
.LBB0_1011:
	test	r8b, 1
	je	.LBB0_1013
# %bb.1012:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
.LBB0_1013:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1014:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1014
	jmp	.LBB0_1526
.LBB0_1015:
	xor	edi, edi
.LBB0_1016:
	test	r8b, 1
	je	.LBB0_1018
# %bb.1017:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshuflw	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3,4,5,6,7]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pshuflw	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3,4,5,6,7]
	movd	dword ptr [rcx + 2*rdi], xmm0
	movd	dword ptr [rcx + 2*rdi + 4], xmm1
.LBB0_1018:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1019:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1019
	jmp	.LBB0_1526
.LBB0_1020:
	xor	edi, edi
.LBB0_1021:
	test	r8b, 1
	je	.LBB0_1023
# %bb.1022:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB0_1023:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1024:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1024
	jmp	.LBB0_1526
.LBB0_1025:
	xor	edi, edi
.LBB0_1026:
	test	r8b, 1
	je	.LBB0_1028
# %bb.1027:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB0_1028:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1029:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1029
	jmp	.LBB0_1526
.LBB0_1030:
	xor	edi, edi
.LBB0_1031:
	test	r8b, 1
	je	.LBB0_1033
# %bb.1032:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_12] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB0_1033:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1034:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1034
	jmp	.LBB0_1526
.LBB0_1035:
	xor	edi, edi
.LBB0_1036:
	test	r8b, 1
	je	.LBB0_1038
# %bb.1037:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_12] # xmm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15]
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB0_1038:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1039:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1039
	jmp	.LBB0_1526
.LBB0_1040:
	xor	edi, edi
.LBB0_1041:
	test	r8b, 1
	je	.LBB0_1043
# %bb.1042:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_14] # xmm2 = [1258291200,1258291200,1258291200,1258291200]
	movdqa	xmm3, xmm0
	pblendw	xmm3, xmm2, 170                 # xmm3 = xmm3[0],xmm2[1],xmm3[2],xmm2[3],xmm3[4],xmm2[5],xmm3[6],xmm2[7]
	psrld	xmm0, 16
	movdqa	xmm4, xmmword ptr [rip + .LCPI0_15] # xmm4 = [1392508928,1392508928,1392508928,1392508928]
	pblendw	xmm0, xmm4, 170                 # xmm0 = xmm0[0],xmm4[1],xmm0[2],xmm4[3],xmm0[4],xmm4[5],xmm0[6],xmm4[7]
	movaps	xmm5, xmmword ptr [rip + .LCPI0_16] # xmm5 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
	subps	xmm0, xmm5
	addps	xmm0, xmm3
	pblendw	xmm2, xmm1, 85                  # xmm2 = xmm1[0],xmm2[1],xmm1[2],xmm2[3],xmm1[4],xmm2[5],xmm1[6],xmm2[7]
	psrld	xmm1, 16
	pblendw	xmm1, xmm4, 170                 # xmm1 = xmm1[0],xmm4[1],xmm1[2],xmm4[3],xmm1[4],xmm4[5],xmm1[6],xmm4[7]
	subps	xmm1, xmm5
	addps	xmm1, xmm2
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1043:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1044:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1044
	jmp	.LBB0_1526
.LBB0_1045:
	xor	edi, edi
.LBB0_1046:
	test	r8b, 1
	je	.LBB0_1048
# %bb.1047:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvtpd2ps	xmm0, xmm0
	cvtpd2ps	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
.LBB0_1048:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1049:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	cvtsd2ss	xmm0, xmm0
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1049
	jmp	.LBB0_1526
.LBB0_1050:
	xor	edi, edi
.LBB0_1051:
	test	r8b, 1
	je	.LBB0_1053
# %bb.1052:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqa	xmm3, xmmword ptr [rip + .LCPI0_11] # xmm3 = [1,1]
	movdqa	xmm1, xmm0
	movdqa	xmm2, xmm0
	movdqa	xmm4, xmm0
	pand	xmm4, xmm3
	psrlq	xmm1, 1
	por	xmm1, xmm4
	blendvpd	xmm2, xmm1, xmm0
	pextrq	rax, xmm2, 1
	xorps	xmm4, xmm4
	cvtsi2ss	xmm4, rax
	movq	rax, xmm2
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, rax
	pxor	xmm5, xmm5
	pcmpgtq	xmm5, xmm0
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	insertps	xmm2, xmm4, 28                  # xmm2 = xmm2[0],xmm4[0],zero,zero
	movaps	xmm4, xmm2
	addps	xmm4, xmm2
	pxor	xmm6, xmm6
	pshufd	xmm0, xmm5, 237                 # xmm0 = xmm5[1,3,2,3]
	blendvps	xmm2, xmm4, xmm0
	pand	xmm3, xmm1
	movdqa	xmm4, xmm1
	psrlq	xmm4, 1
	por	xmm4, xmm3
	pcmpgtq	xmm6, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	pextrq	rax, xmm1, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movq	rax, xmm1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, rax
	insertps	xmm1, xmm0, 28                  # xmm1 = xmm1[0],xmm0[0],zero,zero
	movaps	xmm3, xmm1
	addps	xmm3, xmm1
	pshufd	xmm0, xmm6, 237                 # xmm0 = xmm6[1,3,2,3]
	blendvps	xmm1, xmm3, xmm0
	movlhps	xmm2, xmm1                      # xmm2 = xmm2[0],xmm1[0]
	movups	xmmword ptr [rcx + 4*rdi], xmm2
.LBB0_1053:
	cmp	rsi, r9
	jne	.LBB0_1056
	jmp	.LBB0_1526
.LBB0_1054:                             #   in Loop: Header=BB0_1056 Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	je	.LBB0_1526
.LBB0_1056:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	test	rax, rax
	jns	.LBB0_1054
# %bb.1057:                             #   in Loop: Header=BB0_1056 Depth=1
	mov	rdi, rax
	shr	rdi
	and	eax, 1
	or	rax, rdi
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, rax
	addss	xmm0, xmm0
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1056
	jmp	.LBB0_1526
.LBB0_1058:
	xor	edi, edi
.LBB0_1059:
	test	r8b, 1
	je	.LBB0_1061
# %bb.1060:
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1061:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1062:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1062
	jmp	.LBB0_1526
.LBB0_1063:
	xor	edi, edi
.LBB0_1064:
	test	r8b, 1
	je	.LBB0_1066
# %bb.1065:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1066:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1067:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, eax
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1067
	jmp	.LBB0_1526
.LBB0_1068:
	xor	edi, edi
.LBB0_1069:
	test	r8b, 1
	je	.LBB0_1071
# %bb.1070:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1071:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1072:                             # =>This Inner Loop Header: Depth=1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1072
	jmp	.LBB0_1526
.LBB0_1073:
	xor	edi, edi
.LBB0_1074:
	test	r8b, 1
	je	.LBB0_1076
# %bb.1075:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	unpcklpd	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
.LBB0_1076:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1077:                             # =>This Inner Loop Header: Depth=1
	cvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1077
	jmp	.LBB0_1526
.LBB0_1078:
	xor	edi, edi
.LBB0_1079:
	test	r8b, 1
	je	.LBB0_1081
# %bb.1080:
	pmovzxwd	xmm0, qword ptr [rdx + 2*rdi]   # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	pmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1081:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1082:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1082
	jmp	.LBB0_1526
.LBB0_1083:
	xor	edi, edi
.LBB0_1084:
	test	r8b, 1
	je	.LBB0_1086
# %bb.1085:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1086:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1087:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1087
	jmp	.LBB0_1526
.LBB0_1088:
	xor	edi, edi
.LBB0_1089:
	test	r8b, 1
	je	.LBB0_1091
# %bb.1090:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
.LBB0_1091:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1092:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1092
	jmp	.LBB0_1526
.LBB0_1093:
	xor	edi, edi
.LBB0_1094:
	test	r8b, 1
	je	.LBB0_1096
# %bb.1095:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1096:
	cmp	rsi, r9
	je	.LBB0_1526
.LBB0_1097:                             # =>This Inner Loop Header: Depth=1
	cvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1097
.LBB0_1526:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_1098:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1099:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [rcx + 4*rax], xmm0
	movups	xmmword ptr [rcx + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [rcx + 4*rax + 32], xmm0
	movups	xmmword ptr [rcx + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [rcx + 4*rax + 64], xmm0
	movups	xmmword ptr [rcx + 4*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movupd	xmmword ptr [rcx + 4*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1099
.LBB0_1100:
	test	r8, r8
	je	.LBB0_1103
# %bb.1101:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB0_1102:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1102
.LBB0_1103:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1104
.LBB0_1108:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1109:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [rcx + 4*rax], xmm0
	movups	xmmword ptr [rcx + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [rcx + 4*rax + 32], xmm0
	movups	xmmword ptr [rcx + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [rcx + 4*rax + 64], xmm0
	movups	xmmword ptr [rcx + 4*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movupd	xmmword ptr [rcx + 4*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1109
.LBB0_1110:
	test	r8, r8
	je	.LBB0_1113
# %bb.1111:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB0_1112:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1112
.LBB0_1113:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1114
.LBB0_1118:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1119:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [rcx + 8*rax], xmm0
	movups	xmmword ptr [rcx + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [rcx + 8*rax + 32], xmm0
	movups	xmmword ptr [rcx + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [rcx + 8*rax + 64], xmm0
	movups	xmmword ptr [rcx + 8*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movupd	xmmword ptr [rcx + 8*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1119
.LBB0_1120:
	test	r8, r8
	je	.LBB0_1123
# %bb.1121:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB0_1122:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1122
.LBB0_1123:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1124
.LBB0_1128:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1129:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rax]
	movups	xmm1, xmmword ptr [rdx + rax + 16]
	movups	xmmword ptr [rcx + rax], xmm0
	movups	xmmword ptr [rcx + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 32]
	movups	xmm1, xmmword ptr [rdx + rax + 48]
	movups	xmmword ptr [rcx + rax + 32], xmm0
	movups	xmmword ptr [rcx + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 64]
	movups	xmm1, xmmword ptr [rdx + rax + 80]
	movups	xmmword ptr [rcx + rax + 64], xmm0
	movups	xmmword ptr [rcx + rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + rax + 96]
	movupd	xmm1, xmmword ptr [rdx + rax + 112]
	movupd	xmmword ptr [rcx + rax + 96], xmm0
	movupd	xmmword ptr [rcx + rax + 112], xmm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1129
.LBB0_1130:
	test	r8, r8
	je	.LBB0_1133
# %bb.1131:
	add	rax, 16
	neg	r8
.LBB0_1132:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1132
.LBB0_1133:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1134
.LBB0_1138:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1139:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rax]
	movups	xmm1, xmmword ptr [rdx + rax + 16]
	movups	xmmword ptr [rcx + rax], xmm0
	movups	xmmword ptr [rcx + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 32]
	movups	xmm1, xmmword ptr [rdx + rax + 48]
	movups	xmmword ptr [rcx + rax + 32], xmm0
	movups	xmmword ptr [rcx + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 64]
	movups	xmm1, xmmword ptr [rdx + rax + 80]
	movups	xmmword ptr [rcx + rax + 64], xmm0
	movups	xmmword ptr [rcx + rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + rax + 96]
	movupd	xmm1, xmmword ptr [rdx + rax + 112]
	movupd	xmmword ptr [rcx + rax + 96], xmm0
	movupd	xmmword ptr [rcx + rax + 112], xmm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1139
.LBB0_1140:
	test	r8, r8
	je	.LBB0_1143
# %bb.1141:
	add	rax, 16
	neg	r8
.LBB0_1142:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1142
.LBB0_1143:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1144
.LBB0_1148:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1149:                             # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [rdx + rax]
	pmovsxbq	xmm1, word ptr [rdx + rax + 2]
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovsxbq	xmm0, word ptr [rdx + rax + 4]
	pmovsxbq	xmm1, word ptr [rdx + rax + 6]
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovsxbq	xmm0, word ptr [rdx + rax + 8]
	pmovsxbq	xmm1, word ptr [rdx + rax + 10]
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovsxbq	xmm0, word ptr [rdx + rax + 12]
	pmovsxbq	xmm1, word ptr [rdx + rax + 14]
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1149
.LBB0_1150:
	test	r8, r8
	je	.LBB0_1153
# %bb.1151:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rax + rdx]
	add	r10, 2
	xor	eax, eax
.LBB0_1152:                             # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [r10 + 4*rax - 2]
	pmovsxbq	xmm1, word ptr [r10 + 4*rax]
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_1152
.LBB0_1153:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1154
.LBB0_1158:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1159:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [rcx + 8*rax], xmm0
	movups	xmmword ptr [rcx + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [rcx + 8*rax + 32], xmm0
	movups	xmmword ptr [rcx + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [rcx + 8*rax + 64], xmm0
	movups	xmmword ptr [rcx + 8*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movupd	xmmword ptr [rcx + 8*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1159
.LBB0_1160:
	test	r8, r8
	je	.LBB0_1163
# %bb.1161:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB0_1162:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1162
.LBB0_1163:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1164
.LBB0_1168:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1169:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [rcx + 8*rax], xmm0
	movups	xmmword ptr [rcx + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [rcx + 8*rax + 32], xmm0
	movups	xmmword ptr [rcx + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [rcx + 8*rax + 64], xmm0
	movups	xmmword ptr [rcx + 8*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movupd	xmmword ptr [rcx + 8*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1169
.LBB0_1170:
	test	r8, r8
	je	.LBB0_1173
# %bb.1171:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB0_1172:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1172
.LBB0_1173:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1174
.LBB0_1178:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1179:                             # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [rdx + rax]      # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 2]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovzxbq	xmm0, word ptr [rdx + rax + 4]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 6]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovzxbq	xmm0, word ptr [rdx + rax + 8]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 10] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovzxbq	xmm0, word ptr [rdx + rax + 12] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 14] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1179
.LBB0_1180:
	test	r8, r8
	je	.LBB0_1183
# %bb.1181:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rax + rdx]
	add	r10, 2
	xor	eax, eax
.LBB0_1182:                             # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [r10 + 4*rax - 2] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [r10 + 4*rax]    # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_1182
.LBB0_1183:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1184
.LBB0_1188:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1189:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [rcx + 2*rax], xmm0
	movups	xmmword ptr [rcx + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [rcx + 2*rax + 32], xmm0
	movups	xmmword ptr [rcx + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [rcx + 2*rax + 64], xmm0
	movups	xmmword ptr [rcx + 2*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movupd	xmmword ptr [rcx + 2*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB0_1189
.LBB0_1190:
	test	r8, r8
	je	.LBB0_1193
# %bb.1191:
	add	rax, rax
	add	rax, 16
	neg	r8
.LBB0_1192:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1192
.LBB0_1193:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1194
.LBB0_1198:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1199:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [rcx + 2*rax], xmm0
	movups	xmmword ptr [rcx + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [rcx + 2*rax + 32], xmm0
	movups	xmmword ptr [rcx + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [rcx + 2*rax + 64], xmm0
	movups	xmmword ptr [rcx + 2*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movupd	xmmword ptr [rcx + 2*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB0_1199
.LBB0_1200:
	test	r8, r8
	je	.LBB0_1203
# %bb.1201:
	add	rax, rax
	add	rax, 16
	neg	r8
.LBB0_1202:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1202
.LBB0_1203:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1204
.LBB0_1208:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1209:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [rcx + 2*rax], xmm0
	movups	xmmword ptr [rcx + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [rcx + 2*rax + 32], xmm0
	movups	xmmword ptr [rcx + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [rcx + 2*rax + 64], xmm0
	movups	xmmword ptr [rcx + 2*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movupd	xmmword ptr [rcx + 2*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB0_1209
.LBB0_1210:
	test	r8, r8
	je	.LBB0_1213
# %bb.1211:
	add	rax, rax
	add	rax, 16
	neg	r8
.LBB0_1212:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1212
.LBB0_1213:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1214
.LBB0_1218:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1219:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 2*rax]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movups	xmmword ptr [rcx + 2*rax], xmm0
	movups	xmmword ptr [rcx + 2*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movups	xmmword ptr [rcx + 2*rax + 32], xmm0
	movups	xmmword ptr [rcx + 2*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 2*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 2*rax + 80]
	movups	xmmword ptr [rcx + 2*rax + 64], xmm0
	movups	xmmword ptr [rcx + 2*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 2*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 2*rax + 112]
	movupd	xmmword ptr [rcx + 2*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 112], xmm1
	add	rax, 64
	add	rdi, 4
	jne	.LBB0_1219
.LBB0_1220:
	test	r8, r8
	je	.LBB0_1223
# %bb.1221:
	add	rax, rax
	add	rax, 16
	neg	r8
.LBB0_1222:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1222
.LBB0_1223:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1224
.LBB0_1228:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1229:                             # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [rdx + rax]
	pmovsxbq	xmm1, word ptr [rdx + rax + 2]
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovsxbq	xmm0, word ptr [rdx + rax + 4]
	pmovsxbq	xmm1, word ptr [rdx + rax + 6]
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovsxbq	xmm0, word ptr [rdx + rax + 8]
	pmovsxbq	xmm1, word ptr [rdx + rax + 10]
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovsxbq	xmm0, word ptr [rdx + rax + 12]
	pmovsxbq	xmm1, word ptr [rdx + rax + 14]
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1229
.LBB0_1230:
	test	r8, r8
	je	.LBB0_1233
# %bb.1231:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rax + rdx]
	add	r10, 2
	xor	eax, eax
.LBB0_1232:                             # =>This Inner Loop Header: Depth=1
	pmovsxbq	xmm0, word ptr [r10 + 4*rax - 2]
	pmovsxbq	xmm1, word ptr [r10 + 4*rax]
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_1232
.LBB0_1233:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1234
.LBB0_1238:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1239:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [rcx + 8*rax], xmm0
	movups	xmmword ptr [rcx + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [rcx + 8*rax + 32], xmm0
	movups	xmmword ptr [rcx + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [rcx + 8*rax + 64], xmm0
	movups	xmmword ptr [rcx + 8*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movupd	xmmword ptr [rcx + 8*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1239
.LBB0_1240:
	test	r8, r8
	je	.LBB0_1243
# %bb.1241:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB0_1242:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1242
.LBB0_1243:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1244
.LBB0_1248:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1249:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 8*rax]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movups	xmmword ptr [rcx + 8*rax], xmm0
	movups	xmmword ptr [rcx + 8*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movups	xmmword ptr [rcx + 8*rax + 32], xmm0
	movups	xmmword ptr [rcx + 8*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 8*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 8*rax + 80]
	movups	xmmword ptr [rcx + 8*rax + 64], xmm0
	movups	xmmword ptr [rcx + 8*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 112]
	movupd	xmmword ptr [rcx + 8*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1249
.LBB0_1250:
	test	r8, r8
	je	.LBB0_1253
# %bb.1251:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB0_1252:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1252
.LBB0_1253:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1254
.LBB0_1258:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1259:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [rcx + 4*rax], xmm0
	movups	xmmword ptr [rcx + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [rcx + 4*rax + 32], xmm0
	movups	xmmword ptr [rcx + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [rcx + 4*rax + 64], xmm0
	movups	xmmword ptr [rcx + 4*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movupd	xmmword ptr [rcx + 4*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1259
.LBB0_1260:
	test	r8, r8
	je	.LBB0_1263
# %bb.1261:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB0_1262:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1262
.LBB0_1263:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1264
.LBB0_1268:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1269:                             # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [rdx + rax]      # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 2]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 16], xmm1
	pmovzxbq	xmm0, word ptr [rdx + rax + 4]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 6]  # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 32], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 48], xmm1
	pmovzxbq	xmm0, word ptr [rdx + rax + 8]  # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 10] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 64], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 80], xmm1
	pmovzxbq	xmm0, word ptr [rdx + rax + 12] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [rdx + rax + 14] # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rcx + 8*rax + 96], xmm0
	movdqu	xmmword ptr [rcx + 8*rax + 112], xmm1
	add	rax, 16
	add	rdi, 4
	jne	.LBB0_1269
.LBB0_1270:
	test	r8, r8
	je	.LBB0_1273
# %bb.1271:
	lea	rdi, [rcx + 8*rax]
	add	rdi, 16
	lea	r10, [rax + rdx]
	add	r10, 2
	xor	eax, eax
.LBB0_1272:                             # =>This Inner Loop Header: Depth=1
	pmovzxbq	xmm0, word ptr [r10 + 4*rax - 2] # xmm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	pmovzxbq	xmm1, word ptr [r10 + 4*rax]    # xmm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero
	movdqu	xmmword ptr [rdi - 16], xmm0
	movdqu	xmmword ptr [rdi], xmm1
	add	rdi, 32
	add	rax, 1
	cmp	r8, rax
	jne	.LBB0_1272
.LBB0_1273:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1274
.LBB0_1278:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1279:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rax]
	movups	xmm1, xmmword ptr [rdx + rax + 16]
	movups	xmmword ptr [rcx + rax], xmm0
	movups	xmmword ptr [rcx + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 32]
	movups	xmm1, xmmword ptr [rdx + rax + 48]
	movups	xmmword ptr [rcx + rax + 32], xmm0
	movups	xmmword ptr [rcx + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 64]
	movups	xmm1, xmmword ptr [rdx + rax + 80]
	movups	xmmword ptr [rcx + rax + 64], xmm0
	movups	xmmword ptr [rcx + rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + rax + 96]
	movupd	xmm1, xmmword ptr [rdx + rax + 112]
	movupd	xmmword ptr [rcx + rax + 96], xmm0
	movupd	xmmword ptr [rcx + rax + 112], xmm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1279
.LBB0_1280:
	test	r8, r8
	je	.LBB0_1283
# %bb.1281:
	add	rax, 16
	neg	r8
.LBB0_1282:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1282
.LBB0_1283:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1284
.LBB0_1288:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1289:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + rax]
	movups	xmm1, xmmword ptr [rdx + rax + 16]
	movups	xmmword ptr [rcx + rax], xmm0
	movups	xmmword ptr [rcx + rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 32]
	movups	xmm1, xmmword ptr [rdx + rax + 48]
	movups	xmmword ptr [rcx + rax + 32], xmm0
	movups	xmmword ptr [rcx + rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + rax + 64]
	movups	xmm1, xmmword ptr [rdx + rax + 80]
	movups	xmmword ptr [rcx + rax + 64], xmm0
	movups	xmmword ptr [rcx + rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + rax + 96]
	movupd	xmm1, xmmword ptr [rdx + rax + 112]
	movupd	xmmword ptr [rcx + rax + 96], xmm0
	movupd	xmmword ptr [rcx + rax + 112], xmm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1289
.LBB0_1290:
	test	r8, r8
	je	.LBB0_1293
# %bb.1291:
	add	rax, 16
	neg	r8
.LBB0_1292:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1292
.LBB0_1293:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1294
.LBB0_1298:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1299:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [rcx + 4*rax], xmm0
	movups	xmmword ptr [rcx + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [rcx + 4*rax + 32], xmm0
	movups	xmmword ptr [rcx + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [rcx + 4*rax + 64], xmm0
	movups	xmmword ptr [rcx + 4*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movupd	xmmword ptr [rcx + 4*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1299
.LBB0_1300:
	test	r8, r8
	je	.LBB0_1303
# %bb.1301:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB0_1302:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1302
.LBB0_1303:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1304
.LBB0_1308:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1309:                             # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmmword ptr [rcx + 4*rax], xmm0
	movups	xmmword ptr [rcx + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmmword ptr [rcx + 4*rax + 32], xmm0
	movups	xmmword ptr [rcx + 4*rax + 48], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 64]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 80]
	movups	xmmword ptr [rcx + 4*rax + 64], xmm0
	movups	xmmword ptr [rcx + 4*rax + 80], xmm1
	movupd	xmm0, xmmword ptr [rdx + 4*rax + 96]
	movupd	xmm1, xmmword ptr [rdx + 4*rax + 112]
	movupd	xmmword ptr [rcx + 4*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 112], xmm1
	add	rax, 32
	add	rdi, 4
	jne	.LBB0_1309
.LBB0_1310:
	test	r8, r8
	je	.LBB0_1313
# %bb.1311:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB0_1312:                             # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_1312
.LBB0_1313:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1314
.LBB0_1318:
	xor	edi, edi
.LBB0_1319:
	test	r8b, 1
	je	.LBB0_1321
# %bb.1320:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_5] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rcx + rdi], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rcx + rdi + 2], xmm1, 0
.LBB0_1321:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1322
.LBB0_1326:
	xor	edi, edi
.LBB0_1327:
	test	r8b, 1
	je	.LBB0_1329
# %bb.1328:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_17] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1329:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1330
.LBB0_1334:
	xor	edi, edi
.LBB0_1335:
	test	r8b, 1
	je	.LBB0_1337
# %bb.1336:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_5] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rcx + rdi], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rcx + rdi + 2], xmm1, 0
.LBB0_1337:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1338
.LBB0_1342:
	xor	edi, edi
.LBB0_1343:
	test	r8b, 1
	je	.LBB0_1345
# %bb.1344:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_13] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
.LBB0_1345:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1346
.LBB0_1350:
	xor	edi, edi
.LBB0_1351:
	test	r8b, 1
	je	.LBB0_1353
# %bb.1352:
	pmovsxbw	xmm0, qword ptr [rdx + rdi]
	pmovsxbw	xmm1, qword ptr [rdx + rdi + 8]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
.LBB0_1353:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1354
.LBB0_1358:
	xor	edi, edi
.LBB0_1359:
	test	r8b, 1
	je	.LBB0_1361
# %bb.1360:
	pmovsxbw	xmm0, qword ptr [rdx + rdi]
	pmovsxbw	xmm1, qword ptr [rdx + rdi + 8]
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
.LBB0_1361:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1362
.LBB0_1366:
	xor	edi, edi
.LBB0_1367:
	test	r8b, 1
	je	.LBB0_1369
# %bb.1368:
	pmovzxbw	xmm0, qword ptr [rdx + rdi]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdx + rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
.LBB0_1369:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1370
.LBB0_1374:
	xor	edi, edi
.LBB0_1375:
	test	r8b, 1
	je	.LBB0_1377
# %bb.1376:
	pmovzxbw	xmm0, qword ptr [rdx + rdi]     # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	pmovzxbw	xmm1, qword ptr [rdx + rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
.LBB0_1377:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1378
.LBB0_1382:
	xor	edi, edi
.LBB0_1383:
	test	r8b, 1
	je	.LBB0_1385
# %bb.1384:
	pmovsxbd	xmm0, dword ptr [rdx + rdi]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1385:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1386
.LBB0_1390:
	xor	edi, edi
.LBB0_1391:
	test	r8b, 1
	je	.LBB0_1393
# %bb.1392:
	pmovzxbd	xmm0, dword ptr [rdx + rdi]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	cvtdq2ps	xmm0, xmm0
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [rcx + 4*rdi], xmm0
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1393:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1394
.LBB0_1398:
	xor	edi, edi
.LBB0_1399:
	test	r8b, 1
	je	.LBB0_1401
# %bb.1400:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_13] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
.LBB0_1401:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1402
.LBB0_1406:
	xor	edi, edi
.LBB0_1407:
	test	r8b, 1
	je	.LBB0_1409
# %bb.1408:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	cvttpd2dq	xmm0, xmm0
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_1] # xmm2 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	cvttpd2dq	xmm1, xmm1
	pshufb	xmm0, xmm2
	pextrw	word ptr [rcx + rdi], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rcx + rdi + 2], xmm1, 0
.LBB0_1409:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1410
.LBB0_1414:
	xor	edi, edi
.LBB0_1415:
	test	r8b, 1
	je	.LBB0_1417
# %bb.1416:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_5] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rcx + rdi], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rcx + rdi + 2], xmm1, 0
.LBB0_1417:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1418
.LBB0_1422:
	xor	edi, edi
.LBB0_1423:
	test	r8b, 1
	je	.LBB0_1425
# %bb.1424:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_17] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1425:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1426
.LBB0_1430:
	xor	edi, edi
.LBB0_1431:
	test	r8b, 1
	je	.LBB0_1433
# %bb.1432:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_17] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1433:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1434
.LBB0_1438:
	xor	edi, edi
.LBB0_1439:
	test	r8b, 1
	je	.LBB0_1441
# %bb.1440:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_5] # xmm2 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pextrw	word ptr [rcx + rdi], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rcx + rdi + 2], xmm1, 0
.LBB0_1441:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1442
.LBB0_1446:
	xor	edi, edi
.LBB0_1447:
	test	r8b, 1
	je	.LBB0_1449
# %bb.1448:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	packusdw	xmm0, xmm0
	packuswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packusdw	xmm1, xmm1
	packuswb	xmm1, xmm1
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
.LBB0_1449:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1450
.LBB0_1454:
	xor	edi, edi
.LBB0_1455:
	test	r8b, 1
	je	.LBB0_1457
# %bb.1456:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_13] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
.LBB0_1457:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1458
.LBB0_1462:
	xor	edi, edi
.LBB0_1463:
	test	r8b, 1
	je	.LBB0_1465
# %bb.1464:
	pmovsxbd	xmm0, dword ptr [rdx + rdi]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1465:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1466
.LBB0_1470:
	xor	edi, edi
.LBB0_1471:
	test	r8b, 1
	je	.LBB0_1473
# %bb.1472:
	pmovzxbd	xmm0, dword ptr [rdx + rdi]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1473:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1474
.LBB0_1478:
	xor	edi, edi
.LBB0_1479:
	test	r8b, 1
	je	.LBB0_1481
# %bb.1480:
	pmovsxbd	xmm0, dword ptr [rdx + rdi]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1481:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1482
.LBB0_1486:
	xor	edi, edi
.LBB0_1487:
	test	r8b, 1
	je	.LBB0_1489
# %bb.1488:
	pmovzxbd	xmm0, dword ptr [rdx + rdi]     # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	pmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB0_1489:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1490
.LBB0_1494:
	xor	edi, edi
.LBB0_1495:
	test	r8b, 1
	je	.LBB0_1497
# %bb.1496:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_13] # xmm2 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
.LBB0_1497:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1498
.LBB0_1502:
	xor	edi, edi
.LBB0_1503:
	test	r8b, 1
	je	.LBB0_1505
# %bb.1504:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	cvttpd2dq	xmm0, xmm0
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_1] # xmm2 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	cvttpd2dq	xmm1, xmm1
	pshufb	xmm0, xmm2
	pextrw	word ptr [rcx + rdi], xmm0, 0
	pshufb	xmm1, xmm2
	pextrw	word ptr [rcx + rdi + 2], xmm1, 0
.LBB0_1505:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1506
.LBB0_1510:
	xor	edi, edi
.LBB0_1511:
	test	r8b, 1
	je	.LBB0_1513
# %bb.1512:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_17] # xmm2 = <0,2,4,6,8,10,12,14,u,u,u,u,u,u,u,u>
	pshufb	xmm0, xmm2
	pshufb	xmm1, xmm2
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1513:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1514
.LBB0_1518:
	xor	edi, edi
.LBB0_1519:
	test	r8b, 1
	je	.LBB0_1521
# %bb.1520:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	cvttps2dq	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	cvttps2dq	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movd	dword ptr [rcx + rdi], xmm0
	movd	dword ptr [rcx + rdi + 4], xmm1
.LBB0_1521:
	cmp	rsi, r9
	je	.LBB0_1526
	jmp	.LBB0_1522
.Lfunc_end0:
	.size	cast_type_numeric_sse4, .Lfunc_end0-cast_type_numeric_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
