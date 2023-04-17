	.text
	.intel_syntax noprefix
	.file	"cast_numeric.cc"
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function cast_type_numeric_avx2
.LCPI0_0:
	.quad	0x43e0000000000000              # double 9.2233720368547758E+18
.LCPI0_5:
	.quad	4841369599423283200             # 0x4330000000000000
.LCPI0_6:
	.quad	4985484787499139072             # 0x4530000000000000
.LCPI0_7:
	.quad	0x4530000000100000              # double 1.9342813118337666E+25
.LCPI0_10:
	.quad	1                               # 0x1
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI0_1:
	.long	0x5f000000                      # float 9.22337203E+18
.LCPI0_2:
	.long	0x4f000000                      # float 2.14748365E+9
.LCPI0_3:
	.long	2147483648                      # 0x80000000
.LCPI0_13:
	.long	1258291200                      # 0x4b000000
.LCPI0_14:
	.long	1392508928                      # 0x53000000
.LCPI0_15:
	.long	0x53000080                      # float 5.49764202E+11
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI0_4:
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
.LCPI0_8:
	.long	1127219200                      # 0x43300000
	.long	1160773632                      # 0x45300000
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_9:
	.quad	0x4330000000000000              # double 4503599627370496
	.quad	0x4530000000000000              # double 1.9342813113834067E+25
.LCPI0_12:
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
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI0_11:
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
.LCPI0_16:
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
	.globl	cast_type_numeric_avx2
	.p2align	4, 0x90
	.type	cast_type_numeric_avx2,@function
cast_type_numeric_avx2:                 # @cast_type_numeric_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r14
	push	rbx
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
	jne	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.10:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.11:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_12
# %bb.443:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_742
# %bb.444:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_742
.LBB0_12:
	xor	esi, esi
.LBB0_1189:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1191
.LBB0_1190:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1190
.LBB0_1191:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1192:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1192
	jmp	.LBB0_1553
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
	jne	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.22:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.23:
	mov	esi, r8d
	lea	rdi, [rsi - 1]
	mov	eax, esi
	and	eax, 3
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
	jne	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.32:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.33:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_34
# %bb.451:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_745
# %bb.452:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_745
.LBB0_34:
	xor	esi, esi
.LBB0_1197:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1199
.LBB0_1198:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1198
.LBB0_1199:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1200:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1200
	jmp	.LBB0_1553
.LBB0_35:
	cmp	edi, 7
	je	.LBB0_85
# %bb.36:
	cmp	edi, 8
	jne	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.42:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.43:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_454
# %bb.44:
	xor	esi, esi
	jmp	.LBB0_918
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
	jne	.LBB0_1553
# %bb.50:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.51:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_457
# %bb.52:
	xor	esi, esi
	jmp	.LBB0_1024
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
	jne	.LBB0_1553
# %bb.58:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.59:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_460
# %bb.60:
	xor	esi, esi
	jmp	.LBB0_1029
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
	jne	.LBB0_1553
# %bb.66:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.67:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_463
# %bb.68:
	xor	esi, esi
	jmp	.LBB0_1034
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
	jne	.LBB0_1553
# %bb.74:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.75:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_466
# %bb.76:
	xor	esi, esi
	jmp	.LBB0_1039
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
	jne	.LBB0_1553
# %bb.82:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.83:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_84
# %bb.469:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_748
# %bb.470:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_748
.LBB0_84:
	xor	esi, esi
.LBB0_1205:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1207
.LBB0_1206:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1206
.LBB0_1207:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1208:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1208
	jmp	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.90:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.91:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_92
# %bb.472:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_751
# %bb.473:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_751
.LBB0_92:
	xor	esi, esi
.LBB0_1213:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1215
.LBB0_1214:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1214
.LBB0_1215:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1216:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1216
	jmp	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.97:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.98:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_475
# %bb.99:
	xor	esi, esi
	jmp	.LBB0_1044
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
	jne	.LBB0_1553
# %bb.104:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.105:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_106
# %bb.478:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_754
# %bb.479:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_754
.LBB0_106:
	xor	esi, esi
.LBB0_1221:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 7
	je	.LBB0_1223
.LBB0_1222:                             # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rbx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1222
.LBB0_1223:
	cmp	rax, 7
	jb	.LBB0_1553
.LBB0_1224:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1224
	jmp	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.111:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.112:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_113
# %bb.481:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_757
# %bb.482:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_757
.LBB0_113:
	xor	esi, esi
.LBB0_1229:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1231
.LBB0_1230:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1230
.LBB0_1231:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1232:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	movsx	eax, byte ptr [rdx + rsi + 1]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi + 8], xmm0
	movsx	eax, byte ptr [rdx + rsi + 2]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi + 16], xmm0
	movsx	eax, byte ptr [rdx + rsi + 3]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1232
	jmp	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.118:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.119:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_484
# %bb.120:
	xor	esi, esi
	jmp	.LBB0_923
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
	jne	.LBB0_1553
# %bb.125:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.126:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_487
# %bb.127:
	xor	esi, esi
	jmp	.LBB0_1049
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
	jne	.LBB0_1553
# %bb.132:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.133:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_490
# %bb.134:
	xor	esi, esi
	jmp	.LBB0_1054
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
	jne	.LBB0_1553
# %bb.139:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.140:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_493
# %bb.141:
	xor	esi, esi
	jmp	.LBB0_1059
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
	jne	.LBB0_1553
# %bb.146:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.147:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_496
# %bb.148:
	xor	esi, esi
	jmp	.LBB0_1064
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
	jne	.LBB0_1553
# %bb.153:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.154:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_155
# %bb.499:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_760
# %bb.500:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_760
.LBB0_155:
	xor	esi, esi
.LBB0_1237:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1239
.LBB0_1238:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1238
.LBB0_1239:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1240:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	movzx	eax, byte ptr [rdx + rsi + 1]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi + 8], xmm0
	movzx	eax, byte ptr [rdx + rsi + 2]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi + 16], xmm0
	movzx	eax, byte ptr [rdx + rsi + 3]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi + 24], xmm0
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1240
	jmp	.LBB0_1553
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
	jne	.LBB0_1553
# %bb.160:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.161:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_502
# %bb.162:
	xor	esi, esi
	jmp	.LBB0_929
.LBB0_163:
	cmp	esi, 2
	je	.LBB0_383
# %bb.164:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.165:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.166:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_167
# %bb.505:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_763
# %bb.506:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_763
.LBB0_167:
	xor	esi, esi
.LBB0_1245:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1247
.LBB0_1246:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1246
.LBB0_1247:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1248:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1248
	jmp	.LBB0_1553
.LBB0_168:
	cmp	esi, 2
	je	.LBB0_386
# %bb.169:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.170:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.171:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_172
# %bb.508:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_766
# %bb.509:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_766
.LBB0_172:
	xor	esi, esi
.LBB0_1253:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1255
.LBB0_1254:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	ebx, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], bl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1254
.LBB0_1255:
	cmp	rax, 3
	jb	.LBB0_1553
.LBB0_1256:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1256
	jmp	.LBB0_1553
.LBB0_173:
	cmp	esi, 2
	je	.LBB0_389
# %bb.174:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.175:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.176:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB0_177
# %bb.511:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_769
# %bb.512:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_769
.LBB0_177:
	xor	esi, esi
.LBB0_1261:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1263
.LBB0_1262:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1262
.LBB0_1263:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1264:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1264
	jmp	.LBB0_1553
.LBB0_178:
	cmp	esi, 2
	je	.LBB0_392
# %bb.179:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.180:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.181:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_182
# %bb.514:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_772
# %bb.515:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_772
.LBB0_182:
	xor	esi, esi
.LBB0_1269:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1271
.LBB0_1270:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1270
.LBB0_1271:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1272:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1272
	jmp	.LBB0_1553
.LBB0_183:
	cmp	esi, 2
	je	.LBB0_395
# %bb.184:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.185:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.186:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_187
# %bb.517:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_775
# %bb.518:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_775
.LBB0_187:
	xor	esi, esi
.LBB0_1277:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1279
.LBB0_1278:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1278
.LBB0_1279:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1280:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1280
	jmp	.LBB0_1553
.LBB0_188:
	cmp	esi, 2
	je	.LBB0_398
# %bb.189:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.190:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.191:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_192
# %bb.520:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_778
# %bb.521:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_778
.LBB0_192:
	xor	esi, esi
.LBB0_1285:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1287
.LBB0_1286:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1286
.LBB0_1287:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1288:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1288
	jmp	.LBB0_1553
.LBB0_193:
	cmp	esi, 2
	je	.LBB0_401
# %bb.194:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.195:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.196:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_197
# %bb.523:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_781
# %bb.524:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_781
.LBB0_197:
	xor	esi, esi
.LBB0_1293:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1295
.LBB0_1294:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1294
.LBB0_1295:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1296:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1296
	jmp	.LBB0_1553
.LBB0_198:
	cmp	esi, 2
	je	.LBB0_404
# %bb.199:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.200:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.201:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_202
# %bb.526:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_784
# %bb.527:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_784
.LBB0_202:
	xor	esi, esi
.LBB0_1301:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1303
.LBB0_1302:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1302
.LBB0_1303:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1304:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	vcvttss2si	eax, dword ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	vcvttss2si	eax, dword ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	vcvttss2si	eax, dword ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1304
	jmp	.LBB0_1553
.LBB0_203:
	cmp	esi, 2
	je	.LBB0_407
# %bb.204:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.205:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.206:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB0_207
# %bb.529:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_787
# %bb.530:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_787
.LBB0_207:
	xor	esi, esi
.LBB0_1309:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1311
.LBB0_1310:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1310
.LBB0_1311:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1312:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1312
	jmp	.LBB0_1553
.LBB0_208:
	cmp	esi, 2
	je	.LBB0_410
# %bb.209:
	cmp	esi, 3
	jne	.LBB0_1553
# %bb.210:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.211:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_212
# %bb.532:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_790
# %bb.533:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_790
.LBB0_212:
	xor	esi, esi
.LBB0_1317:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1319
.LBB0_1318:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1318
.LBB0_1319:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1320:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1320
	jmp	.LBB0_1553
.LBB0_213:
	cmp	esi, 7
	je	.LBB0_413
# %bb.214:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.215:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.216:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_535
# %bb.217:
	xor	esi, esi
	jmp	.LBB0_934
.LBB0_218:
	cmp	esi, 7
	je	.LBB0_416
# %bb.219:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.220:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.221:
	mov	r9d, r8d
	movabs	r11, -9223372036854775808
	cmp	r8d, 4
	jae	.LBB0_538
# %bb.222:
	xor	r14d, r14d
	jmp	.LBB0_799
.LBB0_223:
	cmp	esi, 7
	je	.LBB0_419
# %bb.224:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.225:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.226:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_227
# %bb.540:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_801
# %bb.541:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_801
.LBB0_227:
	xor	esi, esi
.LBB0_1325:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1327
.LBB0_1326:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1326
.LBB0_1327:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1328:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1328
	jmp	.LBB0_1553
.LBB0_228:
	cmp	esi, 7
	je	.LBB0_422
# %bb.229:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.230:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.231:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_232
# %bb.543:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_804
# %bb.544:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_804
.LBB0_232:
	xor	esi, esi
.LBB0_1333:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1335
.LBB0_1334:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1334
.LBB0_1335:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1336:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1336
	jmp	.LBB0_1553
.LBB0_233:
	cmp	esi, 7
	je	.LBB0_425
# %bb.234:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.235:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.236:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_546
# %bb.237:
	xor	esi, esi
	jmp	.LBB0_939
.LBB0_238:
	cmp	esi, 7
	je	.LBB0_428
# %bb.239:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.240:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.241:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_549
# %bb.242:
	xor	esi, esi
	jmp	.LBB0_944
.LBB0_243:
	cmp	esi, 7
	je	.LBB0_431
# %bb.244:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.245:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.246:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_247
# %bb.552:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_807
# %bb.553:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_807
.LBB0_247:
	xor	esi, esi
.LBB0_1341:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1343
.LBB0_1342:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1342
.LBB0_1343:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1344:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1344
	jmp	.LBB0_1553
.LBB0_248:
	cmp	esi, 7
	je	.LBB0_434
# %bb.249:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.250:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.251:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_555
# %bb.252:
	xor	r14d, r14d
	jmp	.LBB0_816
.LBB0_253:
	cmp	esi, 7
	je	.LBB0_437
# %bb.254:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.255:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.256:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_257
# %bb.557:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_818
# %bb.558:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_818
.LBB0_257:
	xor	esi, esi
.LBB0_1349:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1351
.LBB0_1350:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1350
.LBB0_1351:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1352:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1352
	jmp	.LBB0_1553
.LBB0_258:
	cmp	esi, 7
	je	.LBB0_440
# %bb.259:
	cmp	esi, 8
	jne	.LBB0_1553
# %bb.260:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.261:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_560
# %bb.262:
	xor	esi, esi
	jmp	.LBB0_949
.LBB0_263:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.264:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_563
# %bb.265:
	xor	esi, esi
	jmp	.LBB0_1069
.LBB0_266:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.267:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_566
# %bb.268:
	xor	esi, esi
	jmp	.LBB0_1074
.LBB0_269:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.270:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_569
# %bb.271:
	xor	esi, esi
	jmp	.LBB0_1079
.LBB0_272:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.273:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_572
# %bb.274:
	xor	esi, esi
	jmp	.LBB0_1084
.LBB0_275:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.276:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_277
# %bb.575:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_821
# %bb.576:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_821
.LBB0_277:
	xor	esi, esi
.LBB0_1357:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1359
.LBB0_1358:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1358
.LBB0_1359:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1360:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1360
	jmp	.LBB0_1553
.LBB0_278:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.279:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_280
# %bb.578:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_824
# %bb.579:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_824
.LBB0_280:
	xor	esi, esi
.LBB0_1365:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1367
.LBB0_1366:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1366
.LBB0_1367:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1368:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1368
	jmp	.LBB0_1553
.LBB0_281:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.282:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_581
# %bb.283:
	xor	esi, esi
	jmp	.LBB0_954
.LBB0_284:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.285:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_584
# %bb.286:
	xor	esi, esi
	jmp	.LBB0_959
.LBB0_287:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.288:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_289
# %bb.587:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_827
# %bb.588:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_827
.LBB0_289:
	xor	esi, esi
.LBB0_1151:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1153
.LBB0_1152:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1152
.LBB0_1153:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1154:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1154
	jmp	.LBB0_1553
.LBB0_290:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.291:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_292
# %bb.590:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_829
# %bb.591:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_829
.LBB0_292:
	xor	esi, esi
.LBB0_1161:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1163
.LBB0_1162:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1162
.LBB0_1163:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1164:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1164
	jmp	.LBB0_1553
.LBB0_293:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.294:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_295
# %bb.593:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_831
# %bb.594:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_831
.LBB0_295:
	xor	esi, esi
.LBB0_1171:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1173
.LBB0_1172:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1172
.LBB0_1173:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1174:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1174
	jmp	.LBB0_1553
.LBB0_296:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.297:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_298
# %bb.596:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_833
# %bb.597:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_833
.LBB0_298:
	xor	esi, esi
.LBB0_1181:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1183
.LBB0_1182:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1182
.LBB0_1183:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1184:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1184
	jmp	.LBB0_1553
.LBB0_299:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.300:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_599
# %bb.301:
	xor	esi, esi
	jmp	.LBB0_964
.LBB0_302:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.303:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_602
# %bb.304:
	xor	esi, esi
	jmp	.LBB0_1089
.LBB0_305:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.306:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_605
# %bb.307:
	xor	esi, esi
	jmp	.LBB0_1094
.LBB0_308:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.309:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_608
# %bb.310:
	xor	esi, esi
	jmp	.LBB0_1099
.LBB0_311:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.312:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_313
# %bb.611:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_835
# %bb.612:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_835
.LBB0_313:
	xor	esi, esi
.LBB0_1373:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1375
.LBB0_1374:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1374
.LBB0_1375:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1376:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1376
	jmp	.LBB0_1553
.LBB0_314:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.315:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_316
# %bb.614:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_838
# %bb.615:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB0_838
.LBB0_316:
	xor	esi, esi
.LBB0_1381:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1383
.LBB0_1382:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1382
.LBB0_1383:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1384:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1384
	jmp	.LBB0_1553
.LBB0_317:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.318:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_617
# %bb.319:
	xor	esi, esi
	jmp	.LBB0_969
.LBB0_320:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.321:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_620
# %bb.322:
	xor	esi, esi
	jmp	.LBB0_974
.LBB0_323:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.324:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_623
# %bb.325:
	xor	esi, esi
	jmp	.LBB0_1104
.LBB0_326:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.327:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_626
# %bb.328:
	xor	esi, esi
	jmp	.LBB0_1109
.LBB0_329:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.330:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_629
# %bb.331:
	xor	esi, esi
	jmp	.LBB0_1114
.LBB0_332:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.333:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_632
# %bb.334:
	xor	esi, esi
	jmp	.LBB0_1119
.LBB0_335:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.336:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_337
# %bb.635:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_841
# %bb.636:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_841
.LBB0_337:
	xor	esi, esi
.LBB0_1389:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1391
.LBB0_1390:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1390
.LBB0_1391:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1392:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1392
	jmp	.LBB0_1553
.LBB0_338:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.339:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_340
# %bb.638:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_844
# %bb.639:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_844
.LBB0_340:
	xor	esi, esi
.LBB0_1397:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1399
.LBB0_1398:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1398
.LBB0_1399:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1400:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	movsx	eax, byte ptr [rdx + rsi + 1]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi + 4], xmm0
	movsx	eax, byte ptr [rdx + rsi + 2]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi + 8], xmm0
	movsx	eax, byte ptr [rdx + rsi + 3]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1400
	jmp	.LBB0_1553
.LBB0_341:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.342:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_343
# %bb.641:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_847
# %bb.642:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_847
.LBB0_343:
	xor	esi, esi
.LBB0_1405:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1407
.LBB0_1406:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1406
.LBB0_1407:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1408:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1408
	jmp	.LBB0_1553
.LBB0_344:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.345:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB0_644
# %bb.346:
	xor	esi, esi
	jmp	.LBB0_858
.LBB0_347:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.348:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_646
# %bb.349:
	xor	esi, esi
	jmp	.LBB0_979
.LBB0_350:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.351:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_649
# %bb.352:
	xor	esi, esi
	jmp	.LBB0_1124
.LBB0_353:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.354:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_652
# %bb.355:
	xor	esi, esi
	jmp	.LBB0_1129
.LBB0_356:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.357:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_655
# %bb.358:
	xor	esi, esi
	jmp	.LBB0_1134
.LBB0_359:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.360:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_361
# %bb.658:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_860
# %bb.659:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_860
.LBB0_361:
	xor	esi, esi
.LBB0_1413:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1415
.LBB0_1414:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1414
.LBB0_1415:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1416:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1416
	jmp	.LBB0_1553
.LBB0_362:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.363:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_661
# %bb.364:
	xor	esi, esi
	jmp	.LBB0_1139
.LBB0_365:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.366:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_664
# %bb.367:
	xor	esi, esi
	jmp	.LBB0_1144
.LBB0_368:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.369:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_370
# %bb.667:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_863
# %bb.668:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_863
.LBB0_370:
	xor	esi, esi
.LBB0_1421:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 7
	je	.LBB0_1423
.LBB0_1422:                             # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], ebx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1422
.LBB0_1423:
	cmp	rax, 7
	jb	.LBB0_1553
.LBB0_1424:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1424
	jmp	.LBB0_1553
.LBB0_371:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.372:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_373
# %bb.670:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_866
# %bb.671:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB0_866
.LBB0_373:
	xor	esi, esi
.LBB0_1429:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1431
.LBB0_1430:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1430
.LBB0_1431:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1432:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1432
	jmp	.LBB0_1553
.LBB0_374:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.375:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_376
# %bb.673:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_869
# %bb.674:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_869
.LBB0_376:
	xor	esi, esi
.LBB0_1437:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1439
.LBB0_1438:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1438
.LBB0_1439:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1440:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	movzx	eax, byte ptr [rdx + rsi + 1]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi + 4], xmm0
	movzx	eax, byte ptr [rdx + rsi + 2]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi + 8], xmm0
	movzx	eax, byte ptr [rdx + rsi + 3]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi + 12], xmm0
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1440
	jmp	.LBB0_1553
.LBB0_377:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.378:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_676
# %bb.379:
	xor	esi, esi
	jmp	.LBB0_984
.LBB0_380:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.381:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_679
# %bb.382:
	xor	esi, esi
	jmp	.LBB0_989
.LBB0_383:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.384:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_385
# %bb.682:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_872
# %bb.683:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_872
.LBB0_385:
	xor	esi, esi
.LBB0_1445:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1447
.LBB0_1446:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1446
.LBB0_1447:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1448:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1448
	jmp	.LBB0_1553
.LBB0_386:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.387:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_388
# %bb.685:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_875
# %bb.686:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_875
.LBB0_388:
	xor	esi, esi
.LBB0_1453:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1455
.LBB0_1454:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	ebx, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], bl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1454
.LBB0_1455:
	cmp	rax, 3
	jb	.LBB0_1553
.LBB0_1456:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi + 8]
	mov	byte ptr [rcx + rsi + 1], al
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi + 16]
	mov	byte ptr [rcx + rsi + 2], al
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi + 24]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1456
	jmp	.LBB0_1553
.LBB0_389:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.390:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB0_391
# %bb.688:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_878
# %bb.689:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_878
.LBB0_391:
	xor	esi, esi
.LBB0_1461:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1463
.LBB0_1462:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1462
.LBB0_1463:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1464:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1464
	jmp	.LBB0_1553
.LBB0_392:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.393:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_394
# %bb.691:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_881
# %bb.692:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_881
.LBB0_394:
	xor	esi, esi
.LBB0_1469:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1471
.LBB0_1470:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1470
.LBB0_1471:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1472:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1472
	jmp	.LBB0_1553
.LBB0_395:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.396:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_397
# %bb.694:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_884
# %bb.695:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_884
.LBB0_397:
	xor	esi, esi
.LBB0_1477:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1479
.LBB0_1478:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1478
.LBB0_1479:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1480:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1480
	jmp	.LBB0_1553
.LBB0_398:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.399:
	mov	r9d, r8d
	cmp	r8d, 64
	jb	.LBB0_400
# %bb.697:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB0_887
# %bb.698:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_887
.LBB0_400:
	xor	esi, esi
.LBB0_1485:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1487
.LBB0_1486:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 2*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1486
.LBB0_1487:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1488:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1488
	jmp	.LBB0_1553
.LBB0_401:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.402:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB0_403
# %bb.700:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB0_890
# %bb.701:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_890
.LBB0_403:
	xor	esi, esi
.LBB0_1493:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1495
.LBB0_1494:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 8*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1494
.LBB0_1495:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1496:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1496
	jmp	.LBB0_1553
.LBB0_404:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.405:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_406
# %bb.703:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_893
# %bb.704:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_893
.LBB0_406:
	xor	esi, esi
.LBB0_1501:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1503
.LBB0_1502:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1502
.LBB0_1503:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1504:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	vcvttss2si	eax, dword ptr [rdx + 4*rsi + 4]
	mov	byte ptr [rcx + rsi + 1], al
	vcvttss2si	eax, dword ptr [rdx + 4*rsi + 8]
	mov	byte ptr [rcx + rsi + 2], al
	vcvttss2si	eax, dword ptr [rdx + 4*rsi + 12]
	mov	byte ptr [rcx + rsi + 3], al
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB0_1504
	jmp	.LBB0_1553
.LBB0_407:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.408:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB0_409
# %bb.706:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_896
# %bb.707:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_896
.LBB0_409:
	xor	esi, esi
.LBB0_1509:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1511
.LBB0_1510:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1510
.LBB0_1511:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1512:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1512
	jmp	.LBB0_1553
.LBB0_410:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.411:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_412
# %bb.709:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_899
# %bb.710:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB0_899
.LBB0_412:
	xor	esi, esi
.LBB0_1517:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1519
.LBB0_1518:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + 4*rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1518
.LBB0_1519:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1520:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1520
	jmp	.LBB0_1553
.LBB0_413:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.414:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_415
# %bb.712:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_902
# %bb.713:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_902
.LBB0_415:
	xor	esi, esi
.LBB0_1525:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1527
.LBB0_1526:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1526
.LBB0_1527:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1528:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1528
	jmp	.LBB0_1553
.LBB0_416:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.417:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_715
# %bb.418:
	xor	esi, esi
	jmp	.LBB0_994
.LBB0_419:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.420:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_421
# %bb.718:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_905
# %bb.719:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_905
.LBB0_421:
	xor	esi, esi
.LBB0_1533:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1535
.LBB0_1534:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1534
.LBB0_1535:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1536:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1536
	jmp	.LBB0_1553
.LBB0_422:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.423:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_721
# %bb.424:
	xor	esi, esi
	jmp	.LBB0_999
.LBB0_425:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.426:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_724
# %bb.427:
	xor	esi, esi
	jmp	.LBB0_1004
.LBB0_428:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.429:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_727
# %bb.430:
	xor	esi, esi
	jmp	.LBB0_1009
.LBB0_431:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.432:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB0_730
# %bb.433:
	xor	esi, esi
	jmp	.LBB0_1014
.LBB0_434:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.435:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB0_733
# %bb.436:
	xor	esi, esi
	jmp	.LBB0_1019
.LBB0_437:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.438:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_439
# %bb.736:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB0_908
# %bb.737:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_908
.LBB0_439:
	xor	esi, esi
.LBB0_1541:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1543
.LBB0_1542:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1542
.LBB0_1543:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1544:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1544
	jmp	.LBB0_1553
.LBB0_440:
	test	r8d, r8d
	jle	.LBB0_1553
# %bb.441:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB0_442
# %bb.739:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB0_911
# %bb.740:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB0_911
.LBB0_442:
	xor	esi, esi
.LBB0_1549:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB0_1551
.LBB0_1550:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1550
.LBB0_1551:
	cmp	r8, 3
	jb	.LBB0_1553
.LBB0_1552:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1552
	jmp	.LBB0_1553
.LBB0_446:
	and	esi, -4
	xor	edi, edi
.LBB0_447:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi]
	mov	dword ptr [rcx + 4*rdi], ebx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 8]
	mov	dword ptr [rcx + 4*rdi + 4], ebx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 16]
	mov	dword ptr [rcx + 4*rdi + 8], ebx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 24]
	mov	dword ptr [rcx + 4*rdi + 12], ebx
	add	rdi, 4
	cmp	rsi, rdi
	jne	.LBB0_447
.LBB0_448:
	test	rax, rax
	je	.LBB0_1553
# %bb.449:
	lea	rcx, [rcx + 4*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	esi, esi
.LBB0_450:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rdi, qword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB0_450
	jmp	.LBB0_1553
.LBB0_454:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_914
# %bb.455:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi + 128]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 160]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 192]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 80], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 96], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_456
	jmp	.LBB0_915
.LBB0_457:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1020
# %bb.458:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_459:                              # =>This Inner Loop Header: Depth=1
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_459
	jmp	.LBB0_1021
.LBB0_460:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1025
# %bb.461:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_462:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi + 64]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 80]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 96]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 112]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_462
	jmp	.LBB0_1026
.LBB0_463:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1030
# %bb.464:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_465:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi + 128]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 160]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 192]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 80], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 96], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_465
	jmp	.LBB0_1031
.LBB0_466:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1035
# %bb.467:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastss	xmm0, dword ptr [rip + .LCPI0_2] # xmm0 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	vbroadcastss	xmm1, dword ptr [rip + .LCPI0_3] # xmm1 = [2147483648,2147483648,2147483648,2147483648]
.LBB0_468:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm2, xmmword ptr [rdx + 4*rdi]
	vmovups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	vmovups	xmm4, xmmword ptr [rdx + 4*rdi + 32]
	vcmpltps	xmm5, xmm2, xmm0
	vsubps	xmm6, xmm2, xmm0
	vcvttps2dq	xmm6, xmm6
	vxorps	xmm6, xmm6, xmm1
	vcvttps2dq	xmm2, xmm2
	vblendvps	xmm2, xmm6, xmm2, xmm5
	vmovups	xmm5, xmmword ptr [rdx + 4*rdi + 48]
	vcmpltps	xmm6, xmm3, xmm0
	vsubps	xmm7, xmm3, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm3, xmm3
	vblendvps	xmm3, xmm7, xmm3, xmm6
	vcmpltps	xmm6, xmm4, xmm0
	vsubps	xmm7, xmm4, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm4, xmm4
	vblendvps	xmm4, xmm7, xmm4, xmm6
	vcmpltps	xmm6, xmm5, xmm0
	vsubps	xmm7, xmm5, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm5, xmm5
	vblendvps	xmm5, xmm7, xmm5, xmm6
	vmovups	xmmword ptr [rcx + 4*rdi], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm3
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm4
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm5
	vmovups	xmm2, xmmword ptr [rdx + 4*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vmovups	xmm4, xmmword ptr [rdx + 4*rdi + 96]
	vcmpltps	xmm5, xmm2, xmm0
	vsubps	xmm6, xmm2, xmm0
	vcvttps2dq	xmm6, xmm6
	vxorps	xmm6, xmm6, xmm1
	vcvttps2dq	xmm2, xmm2
	vblendvps	xmm2, xmm6, xmm2, xmm5
	vmovups	xmm5, xmmword ptr [rdx + 4*rdi + 112]
	vcmpltps	xmm6, xmm3, xmm0
	vsubps	xmm7, xmm3, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm3, xmm3
	vblendvps	xmm3, xmm7, xmm3, xmm6
	vcmpltps	xmm6, xmm4, xmm0
	vsubps	xmm7, xmm4, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm4, xmm4
	vblendvps	xmm4, xmm7, xmm4, xmm6
	vcmpltps	xmm6, xmm5, xmm0
	vsubps	xmm7, xmm5, xmm0
	vcvttps2dq	xmm7, xmm7
	vxorps	xmm7, xmm7, xmm1
	vcvttps2dq	xmm5, xmm5
	vblendvps	xmm5, xmm7, xmm5, xmm6
	vmovups	xmmword ptr [rcx + 4*rdi + 64], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 80], xmm3
	vmovups	xmmword ptr [rcx + 4*rdi + 96], xmm4
	vmovups	xmmword ptr [rcx + 4*rdi + 112], xmm5
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_468
	jmp	.LBB0_1036
.LBB0_475:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1040
# %bb.476:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_5] # ymm0 = [4841369599423283200,4841369599423283200,4841369599423283200,4841369599423283200]
.LBB0_477:                              # =>This Inner Loop Header: Depth=1
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 16] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 32] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm4, xmmword ptr [rdx + 4*rdi + 48] # ymm4 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpor	ymm1, ymm1, ymm0
	vsubpd	ymm1, ymm1, ymm0
	vpor	ymm2, ymm2, ymm0
	vsubpd	ymm2, ymm2, ymm0
	vpor	ymm3, ymm3, ymm0
	vsubpd	ymm3, ymm3, ymm0
	vpor	ymm4, ymm4, ymm0
	vsubpd	ymm4, ymm4, ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 64] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 80] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 96] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm4, xmmword ptr [rdx + 4*rdi + 112] # ymm4 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpor	ymm1, ymm1, ymm0
	vsubpd	ymm1, ymm1, ymm0
	vpor	ymm2, ymm2, ymm0
	vsubpd	ymm2, ymm2, ymm0
	vpor	ymm3, ymm3, ymm0
	vsubpd	ymm3, ymm3, ymm0
	vpor	ymm4, ymm4, ymm0
	vsubpd	ymm4, ymm4, ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_477
	jmp	.LBB0_1041
.LBB0_484:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_919
# %bb.485:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_5] # ymm0 = [4841369599423283200,4841369599423283200,4841369599423283200,4841369599423283200]
	vpxor	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI0_6] # ymm2 = [4985484787499139072,4985484787499139072,4985484787499139072,4985484787499139072]
	vbroadcastsd	ymm3, qword ptr [rip + .LCPI0_7] # ymm3 = [1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25]
.LBB0_486:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm6, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm7, ymmword ptr [rdx + 8*rdi + 96]
	vpblendd	ymm8, ymm4, ymm1, 170           # ymm8 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm4, ymm4, 32
	vpor	ymm4, ymm4, ymm2
	vsubpd	ymm4, ymm4, ymm3
	vaddpd	ymm4, ymm8, ymm4
	vpblendd	ymm8, ymm5, ymm1, 170           # ymm8 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm5, ymm5, 32
	vpor	ymm5, ymm5, ymm2
	vsubpd	ymm5, ymm5, ymm3
	vaddpd	ymm5, ymm8, ymm5
	vpblendd	ymm8, ymm6, ymm1, 170           # ymm8 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm6, ymm6, 32
	vpor	ymm6, ymm6, ymm2
	vsubpd	ymm6, ymm6, ymm3
	vaddpd	ymm6, ymm8, ymm6
	vpblendd	ymm8, ymm7, ymm1, 170           # ymm8 = ymm7[0],ymm1[1],ymm7[2],ymm1[3],ymm7[4],ymm1[5],ymm7[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm7, ymm7, 32
	vpor	ymm7, ymm7, ymm2
	vsubpd	ymm7, ymm7, ymm3
	vaddpd	ymm7, ymm8, ymm7
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm5
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm6
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm7
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 128]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 160]
	vmovdqu	ymm6, ymmword ptr [rdx + 8*rdi + 192]
	vmovdqu	ymm7, ymmword ptr [rdx + 8*rdi + 224]
	vpblendd	ymm8, ymm4, ymm1, 170           # ymm8 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm4, ymm4, 32
	vpor	ymm4, ymm4, ymm2
	vsubpd	ymm4, ymm4, ymm3
	vaddpd	ymm4, ymm8, ymm4
	vpblendd	ymm8, ymm5, ymm1, 170           # ymm8 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm5, ymm5, 32
	vpor	ymm5, ymm5, ymm2
	vsubpd	ymm5, ymm5, ymm3
	vaddpd	ymm5, ymm8, ymm5
	vpblendd	ymm8, ymm6, ymm1, 170           # ymm8 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm6, ymm6, 32
	vpor	ymm6, ymm6, ymm2
	vsubpd	ymm6, ymm6, ymm3
	vaddpd	ymm6, ymm8, ymm6
	vpblendd	ymm8, ymm7, ymm1, 170           # ymm8 = ymm7[0],ymm1[1],ymm7[2],ymm1[3],ymm7[4],ymm1[5],ymm7[6],ymm1[7]
	vpor	ymm8, ymm8, ymm0
	vpsrlq	ymm7, ymm7, 32
	vpor	ymm7, ymm7, ymm2
	vsubpd	ymm7, ymm7, ymm3
	vaddpd	ymm7, ymm8, ymm7
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm5
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm6
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm7
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_486
	jmp	.LBB0_920
.LBB0_487:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1045
# %bb.488:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_489:                              # =>This Inner Loop Header: Depth=1
	vpmovzxwd	xmm0, qword ptr [rdx + 2*rdi] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm2, qword ptr [rdx + 2*rdi + 16] # xmm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm3, qword ptr [rdx + 2*rdi + 24] # xmm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxwd	xmm0, qword ptr [rdx + 2*rdi + 32] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 40] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm2, qword ptr [rdx + 2*rdi + 48] # xmm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm3, qword ptr [rdx + 2*rdi + 56] # xmm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_489
	jmp	.LBB0_1046
.LBB0_490:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1050
# %bb.491:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_492:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	vpmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	vpmovsxwd	xmm2, qword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	xmm3, qword ptr [rdx + 2*rdi + 24]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxwd	xmm0, qword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 40]
	vpmovsxwd	xmm2, qword ptr [rdx + 2*rdi + 48]
	vpmovsxwd	xmm3, qword ptr [rdx + 2*rdi + 56]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_492
	jmp	.LBB0_1051
.LBB0_493:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1055
# %bb.494:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edi, edi
.LBB0_495:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	vpextrq	rax, xmm0, 1
	vcvtsi2sd	xmm2, xmm11, rax
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovq	rax, xmm0
	vcvtsi2sd	xmm0, xmm11, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 48]
	vmovq	rax, xmm1
	vcvtsi2sd	xmm1, xmm11, rax
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm8, xmm0, xmm2        # xmm8 = xmm0[0],xmm2[0]
	vmovq	rax, xmm5
	vcvtsi2sd	xmm2, xmm11, rax
	vpextrq	rax, xmm3, 1
	vcvtsi2sd	xmm5, xmm11, rax
	vunpcklpd	xmm10, xmm1, xmm4       # xmm10 = xmm1[0],xmm4[0]
	vmovq	rax, xmm3
	vcvtsi2sd	xmm3, xmm11, rax
	vunpcklpd	xmm9, xmm2, xmm6        # xmm9 = xmm2[0],xmm6[0]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 80]
	vpextrq	rax, xmm4, 1
	vunpcklpd	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0]
	vcvtsi2sd	xmm5, xmm11, rax
	vmovq	rax, xmm4
	vcvtsi2sd	xmm4, xmm11, rax
	vunpcklpd	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0]
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 64]
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vmovq	rax, xmm5
	vcvtsi2sd	xmm5, xmm11, rax
	vmovdqu	xmm7, xmmword ptr [rdx + 8*rdi + 112]
	vpextrq	rax, xmm7, 1
	vcvtsi2sd	xmm0, xmm11, rax
	vmovq	rax, xmm7
	vcvtsi2sd	xmm7, xmm11, rax
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 96]
	vpextrq	rax, xmm2, 1
	vcvtsi2sd	xmm1, xmm11, rax
	vunpcklpd	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vmovq	rax, xmm2
	vcvtsi2sd	xmm2, xmm11, rax
	vunpcklpd	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vunpcklpd	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovupd	xmmword ptr [rcx + 8*rdi + 16], xmm10
	vmovupd	xmmword ptr [rcx + 8*rdi], xmm8
	vmovupd	xmmword ptr [rcx + 8*rdi + 32], xmm3
	vmovupd	xmmword ptr [rcx + 8*rdi + 48], xmm9
	vmovupd	xmmword ptr [rcx + 8*rdi + 64], xmm5
	vmovupd	xmmword ptr [rcx + 8*rdi + 80], xmm4
	vmovupd	xmmword ptr [rcx + 8*rdi + 96], xmm1
	vmovupd	xmmword ptr [rcx + 8*rdi + 112], xmm0
	vmovdqu	xmm0, xmmword ptr [rdx + 8*rdi + 128]
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 144]
	vpextrq	rax, xmm0, 1
	vcvtsi2sd	xmm2, xmm11, rax
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 160]
	vmovq	rax, xmm0
	vcvtsi2sd	xmm0, xmm11, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 176]
	vmovq	rax, xmm1
	vcvtsi2sd	xmm1, xmm11, rax
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm8, xmm0, xmm2        # xmm8 = xmm0[0],xmm2[0]
	vmovq	rax, xmm5
	vcvtsi2sd	xmm2, xmm11, rax
	vpextrq	rax, xmm3, 1
	vcvtsi2sd	xmm5, xmm11, rax
	vunpcklpd	xmm10, xmm1, xmm4       # xmm10 = xmm1[0],xmm4[0]
	vmovq	rax, xmm3
	vcvtsi2sd	xmm3, xmm11, rax
	vunpcklpd	xmm9, xmm2, xmm6        # xmm9 = xmm2[0],xmm6[0]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 208]
	vpextrq	rax, xmm4, 1
	vunpcklpd	xmm3, xmm3, xmm5        # xmm3 = xmm3[0],xmm5[0]
	vcvtsi2sd	xmm5, xmm11, rax
	vmovq	rax, xmm4
	vcvtsi2sd	xmm4, xmm11, rax
	vunpcklpd	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0]
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 192]
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vmovq	rax, xmm5
	vcvtsi2sd	xmm5, xmm11, rax
	vmovdqu	xmm7, xmmword ptr [rdx + 8*rdi + 240]
	vpextrq	rax, xmm7, 1
	vcvtsi2sd	xmm0, xmm11, rax
	vmovq	rax, xmm7
	vcvtsi2sd	xmm7, xmm11, rax
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 224]
	vpextrq	rax, xmm2, 1
	vcvtsi2sd	xmm1, xmm11, rax
	vunpcklpd	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vmovq	rax, xmm2
	vcvtsi2sd	xmm2, xmm11, rax
	vunpcklpd	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vunpcklpd	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovupd	xmmword ptr [rcx + 8*rdi + 144], xmm10
	vmovupd	xmmword ptr [rcx + 8*rdi + 128], xmm8
	vmovupd	xmmword ptr [rcx + 8*rdi + 160], xmm3
	vmovupd	xmmword ptr [rcx + 8*rdi + 176], xmm9
	vmovupd	xmmword ptr [rcx + 8*rdi + 192], xmm5
	vmovupd	xmmword ptr [rcx + 8*rdi + 208], xmm4
	vmovupd	xmmword ptr [rcx + 8*rdi + 224], xmm1
	vmovupd	xmmword ptr [rcx + 8*rdi + 240], xmm0
	add	rdi, 32
	add	r10, 2
	jne	.LBB0_495
	jmp	.LBB0_1056
.LBB0_496:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1060
# %bb.497:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_498:                              # =>This Inner Loop Header: Depth=1
	vcvtps2pd	ymm0, xmmword ptr [rdx + 4*rdi]
	vcvtps2pd	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vcvtps2pd	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vcvtps2pd	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vcvtps2pd	ymm0, xmmword ptr [rdx + 4*rdi + 64]
	vcvtps2pd	ymm1, xmmword ptr [rdx + 4*rdi + 80]
	vcvtps2pd	ymm2, xmmword ptr [rdx + 4*rdi + 96]
	vcvtps2pd	ymm3, xmmword ptr [rdx + 4*rdi + 112]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_498
	jmp	.LBB0_1061
.LBB0_502:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_925
# %bb.503:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_504:                              # =>This Inner Loop Header: Depth=1
	vcvtdq2pd	ymm0, xmmword ptr [rdx + 4*rdi]
	vcvtdq2pd	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vcvtdq2pd	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vcvtdq2pd	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vcvtdq2pd	ymm0, xmmword ptr [rdx + 4*rdi + 64]
	vcvtdq2pd	ymm1, xmmword ptr [rdx + 4*rdi + 80]
	vcvtdq2pd	ymm2, xmmword ptr [rdx + 4*rdi + 96]
	vcvtdq2pd	ymm3, xmmword ptr [rdx + 4*rdi + 112]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_504
	jmp	.LBB0_926
.LBB0_535:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_930
# %bb.536:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_537:                              # =>This Inner Loop Header: Depth=1
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_537
	jmp	.LBB0_931
.LBB0_538:
	mov	r14d, r9d
	and	r14d, -4
	lea	rax, [r14 - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	mov	r8d, r10d
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_793
# %bb.539:
	xor	eax, eax
	jmp	.LBB0_795
.LBB0_546:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_935
# %bb.547:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_548:                              # =>This Inner Loop Header: Depth=1
	vpmovzxwq	ymm0, qword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdx + 2*rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdx + 2*rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdx + 2*rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxwq	ymm0, qword ptr [rdx + 2*rdi + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdx + 2*rdi + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdx + 2*rdi + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdx + 2*rdi + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_548
	jmp	.LBB0_936
.LBB0_549:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_940
# %bb.550:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_551:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwq	ymm0, qword ptr [rdx + 2*rdi]
	vpmovsxwq	ymm1, qword ptr [rdx + 2*rdi + 8]
	vpmovsxwq	ymm2, qword ptr [rdx + 2*rdi + 16]
	vpmovsxwq	ymm3, qword ptr [rdx + 2*rdi + 24]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxwq	ymm0, qword ptr [rdx + 2*rdi + 32]
	vpmovsxwq	ymm1, qword ptr [rdx + 2*rdi + 40]
	vpmovsxwq	ymm2, qword ptr [rdx + 2*rdi + 48]
	vpmovsxwq	ymm3, qword ptr [rdx + 2*rdi + 56]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_551
	jmp	.LBB0_941
.LBB0_555:
	mov	r14d, r9d
	and	r14d, -4
	lea	rax, [r14 - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	mov	r8d, r10d
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_810
# %bb.556:
	xor	eax, eax
	jmp	.LBB0_812
.LBB0_560:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_945
# %bb.561:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_562:                              # =>This Inner Loop Header: Depth=1
	vpmovsxdq	ymm0, xmmword ptr [rdx + 4*rdi]
	vpmovsxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxdq	ymm0, xmmword ptr [rdx + 4*rdi + 64]
	vpmovsxdq	ymm1, xmmword ptr [rdx + 4*rdi + 80]
	vpmovsxdq	ymm2, xmmword ptr [rdx + 4*rdi + 96]
	vpmovsxdq	ymm3, xmmword ptr [rdx + 4*rdi + 112]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_562
	jmp	.LBB0_946
.LBB0_563:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1065
# %bb.564:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_11] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_565:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 112], xmm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_565
	jmp	.LBB0_1066
.LBB0_566:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1070
# %bb.567:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_11] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_568:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 112], xmm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_568
	jmp	.LBB0_1071
.LBB0_569:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1075
# %bb.570:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_571:                              # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackusdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackusdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 160]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 192]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 224]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackusdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackusdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_571
	jmp	.LBB0_1076
.LBB0_572:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1080
# %bb.573:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_574:                              # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackssdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackssdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 160]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 192]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 224]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackssdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackssdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_574
	jmp	.LBB0_1081
.LBB0_581:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_950
# %bb.582:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_583:                              # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_583
	jmp	.LBB0_951
.LBB0_584:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_955
# %bb.585:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_586:                              # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_586
	jmp	.LBB0_956
.LBB0_599:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_960
# %bb.600:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_601:                              # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_601
	jmp	.LBB0_961
.LBB0_602:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1085
# %bb.603:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB0_604:                              # =>This Inner Loop Header: Depth=1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vpblendw	xmm8, xmm0, xmmword ptr [rdx + 8*rdi + 128], 17 # xmm8 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 144], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 160], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 176], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 192], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 208], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 224], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi + 240], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm1, ymm6, xmm1, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm1, ymm5, ymm1
	vpackusdw	ymm1, ymm1, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm3, ymm8, xmm3, 1
	vpackusdw	ymm2, ymm3, ymm2
	vpackusdw	ymm2, ymm2, ymm0
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_604
	jmp	.LBB0_1086
.LBB0_605:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1090
# %bb.606:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_607:                              # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vpackusdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vextracti128	xmm2, ymm1, 1
	vpackusdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vextracti128	xmm3, ymm2, 1
	vpackusdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vextracti128	xmm4, ymm3, 1
	vpackusdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vextracti128	xmm1, ymm0, 1
	vpackusdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vextracti128	xmm2, ymm1, 1
	vpackusdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vextracti128	xmm3, ymm2, 1
	vpackusdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vextracti128	xmm4, ymm3, 1
	vpackusdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rcx + 2*rdi + 64], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 80], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 96], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 112], xmm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_607
	jmp	.LBB0_1091
.LBB0_608:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1095
# %bb.609:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_610:                              # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rcx + 2*rdi + 64], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 80], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 96], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 112], xmm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_610
	jmp	.LBB0_1096
.LBB0_617:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_965
# %bb.618:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_11] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_619:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 112], xmm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_619
	jmp	.LBB0_966
.LBB0_620:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_970
# %bb.621:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_11] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB0_622:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpshufb	ymm1, ymm1, ymm0
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm0
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm0
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vpshufb	ymm4, ymm4, ymm0
	vpermq	ymm4, ymm4, 232                 # ymm4 = ymm4[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + 2*rdi + 112], xmm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_622
	jmp	.LBB0_971
.LBB0_623:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1100
# %bb.624:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_625:                              # =>This Inner Loop Header: Depth=1
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_625
	jmp	.LBB0_1101
.LBB0_626:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1105
# %bb.627:
	mov	rax, r8
	and	rax, -2
	neg	rax
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI0_13] # ymm0 = [1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200]
	xor	edi, edi
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI0_14] # ymm1 = [1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928]
	vbroadcastss	ymm2, dword ptr [rip + .LCPI0_15] # ymm2 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
.LBB0_628:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm5, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm6, ymmword ptr [rdx + 4*rdi + 96]
	vpblendw	ymm7, ymm3, ymm0, 170           # ymm7 = ymm3[0],ymm0[1],ymm3[2],ymm0[3],ymm3[4],ymm0[5],ymm3[6],ymm0[7],ymm3[8],ymm0[9],ymm3[10],ymm0[11],ymm3[12],ymm0[13],ymm3[14],ymm0[15]
	vpsrld	ymm3, ymm3, 16
	vpblendw	ymm3, ymm3, ymm1, 170           # ymm3 = ymm3[0],ymm1[1],ymm3[2],ymm1[3],ymm3[4],ymm1[5],ymm3[6],ymm1[7],ymm3[8],ymm1[9],ymm3[10],ymm1[11],ymm3[12],ymm1[13],ymm3[14],ymm1[15]
	vsubps	ymm3, ymm3, ymm2
	vaddps	ymm3, ymm7, ymm3
	vpblendw	ymm7, ymm4, ymm0, 170           # ymm7 = ymm4[0],ymm0[1],ymm4[2],ymm0[3],ymm4[4],ymm0[5],ymm4[6],ymm0[7],ymm4[8],ymm0[9],ymm4[10],ymm0[11],ymm4[12],ymm0[13],ymm4[14],ymm0[15]
	vpsrld	ymm4, ymm4, 16
	vpblendw	ymm4, ymm4, ymm1, 170           # ymm4 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7],ymm4[8],ymm1[9],ymm4[10],ymm1[11],ymm4[12],ymm1[13],ymm4[14],ymm1[15]
	vsubps	ymm4, ymm4, ymm2
	vaddps	ymm4, ymm7, ymm4
	vpblendw	ymm7, ymm5, ymm0, 170           # ymm7 = ymm5[0],ymm0[1],ymm5[2],ymm0[3],ymm5[4],ymm0[5],ymm5[6],ymm0[7],ymm5[8],ymm0[9],ymm5[10],ymm0[11],ymm5[12],ymm0[13],ymm5[14],ymm0[15]
	vpsrld	ymm5, ymm5, 16
	vpblendw	ymm5, ymm5, ymm1, 170           # ymm5 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7],ymm5[8],ymm1[9],ymm5[10],ymm1[11],ymm5[12],ymm1[13],ymm5[14],ymm1[15]
	vsubps	ymm5, ymm5, ymm2
	vaddps	ymm5, ymm7, ymm5
	vpblendw	ymm7, ymm6, ymm0, 170           # ymm7 = ymm6[0],ymm0[1],ymm6[2],ymm0[3],ymm6[4],ymm0[5],ymm6[6],ymm0[7],ymm6[8],ymm0[9],ymm6[10],ymm0[11],ymm6[12],ymm0[13],ymm6[14],ymm0[15]
	vpsrld	ymm6, ymm6, 16
	vpblendw	ymm6, ymm6, ymm1, 170           # ymm6 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7],ymm6[8],ymm1[9],ymm6[10],ymm1[11],ymm6[12],ymm1[13],ymm6[14],ymm1[15]
	vsubps	ymm6, ymm6, ymm2
	vaddps	ymm6, ymm7, ymm6
	vmovups	ymmword ptr [rcx + 4*rdi], ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm4
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm5
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm6
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 128]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 160]
	vmovdqu	ymm5, ymmword ptr [rdx + 4*rdi + 192]
	vmovdqu	ymm6, ymmword ptr [rdx + 4*rdi + 224]
	vpblendw	ymm7, ymm3, ymm0, 170           # ymm7 = ymm3[0],ymm0[1],ymm3[2],ymm0[3],ymm3[4],ymm0[5],ymm3[6],ymm0[7],ymm3[8],ymm0[9],ymm3[10],ymm0[11],ymm3[12],ymm0[13],ymm3[14],ymm0[15]
	vpsrld	ymm3, ymm3, 16
	vpblendw	ymm3, ymm3, ymm1, 170           # ymm3 = ymm3[0],ymm1[1],ymm3[2],ymm1[3],ymm3[4],ymm1[5],ymm3[6],ymm1[7],ymm3[8],ymm1[9],ymm3[10],ymm1[11],ymm3[12],ymm1[13],ymm3[14],ymm1[15]
	vsubps	ymm3, ymm3, ymm2
	vaddps	ymm3, ymm7, ymm3
	vpblendw	ymm7, ymm4, ymm0, 170           # ymm7 = ymm4[0],ymm0[1],ymm4[2],ymm0[3],ymm4[4],ymm0[5],ymm4[6],ymm0[7],ymm4[8],ymm0[9],ymm4[10],ymm0[11],ymm4[12],ymm0[13],ymm4[14],ymm0[15]
	vpsrld	ymm4, ymm4, 16
	vpblendw	ymm4, ymm4, ymm1, 170           # ymm4 = ymm4[0],ymm1[1],ymm4[2],ymm1[3],ymm4[4],ymm1[5],ymm4[6],ymm1[7],ymm4[8],ymm1[9],ymm4[10],ymm1[11],ymm4[12],ymm1[13],ymm4[14],ymm1[15]
	vsubps	ymm4, ymm4, ymm2
	vaddps	ymm4, ymm7, ymm4
	vpblendw	ymm7, ymm5, ymm0, 170           # ymm7 = ymm5[0],ymm0[1],ymm5[2],ymm0[3],ymm5[4],ymm0[5],ymm5[6],ymm0[7],ymm5[8],ymm0[9],ymm5[10],ymm0[11],ymm5[12],ymm0[13],ymm5[14],ymm0[15]
	vpsrld	ymm5, ymm5, 16
	vpblendw	ymm5, ymm5, ymm1, 170           # ymm5 = ymm5[0],ymm1[1],ymm5[2],ymm1[3],ymm5[4],ymm1[5],ymm5[6],ymm1[7],ymm5[8],ymm1[9],ymm5[10],ymm1[11],ymm5[12],ymm1[13],ymm5[14],ymm1[15]
	vsubps	ymm5, ymm5, ymm2
	vaddps	ymm5, ymm7, ymm5
	vpblendw	ymm7, ymm6, ymm0, 170           # ymm7 = ymm6[0],ymm0[1],ymm6[2],ymm0[3],ymm6[4],ymm0[5],ymm6[6],ymm0[7],ymm6[8],ymm0[9],ymm6[10],ymm0[11],ymm6[12],ymm0[13],ymm6[14],ymm0[15]
	vpsrld	ymm6, ymm6, 16
	vpblendw	ymm6, ymm6, ymm1, 170           # ymm6 = ymm6[0],ymm1[1],ymm6[2],ymm1[3],ymm6[4],ymm1[5],ymm6[6],ymm1[7],ymm6[8],ymm1[9],ymm6[10],ymm1[11],ymm6[12],ymm1[13],ymm6[14],ymm1[15]
	vsubps	ymm6, ymm6, ymm2
	vaddps	ymm6, ymm7, ymm6
	vmovups	ymmword ptr [rcx + 4*rdi + 128], ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 160], ymm4
	vmovups	ymmword ptr [rcx + 4*rdi + 192], ymm5
	vmovups	ymmword ptr [rcx + 4*rdi + 224], ymm6
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_628
	jmp	.LBB0_1106
.LBB0_629:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1110
# %bb.630:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edi, edi
.LBB0_631:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 8]
	vmovq	xmm0, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi]
	vmovq	xmm1, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 24]
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 16]
	vmovq	xmm1, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 56]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 48]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovq	xmm2, rbx
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 40]
	vmovq	xmm3, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 32]
	vmovq	xmm4, rax
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 88]
	vmovq	xmm4, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 80]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 72]
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 64]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 120]
	vmovq	xmm6, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 112]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vmovq	xmm6, rax
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 104]
	vmovq	xmm7, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 96]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rcx + 8*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 8*rdi], xmm8
	vmovdqu	xmmword ptr [rcx + 8*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 8*rdi + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rdi + 64], xmm5
	vmovdqu	xmmword ptr [rcx + 8*rdi + 80], xmm4
	vmovdqu	xmmword ptr [rcx + 8*rdi + 96], xmm0
	vmovdqu	xmmword ptr [rcx + 8*rdi + 112], xmm6
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 136]
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 128]
	vmovq	xmm0, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 152]
	vmovq	xmm1, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 144]
	vmovq	xmm2, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vmovq	xmm1, rbx
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 184]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 176]
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 168]
	vmovq	xmm3, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 160]
	vmovq	xmm4, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 216]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 208]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 200]
	vmovq	xmm7, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 192]
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vpunpcklqdq	xmm4, xmm6, xmm5        # xmm4 = xmm6[0],xmm5[0]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm5, xmm5, xmm7        # xmm5 = xmm5[0],xmm7[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 248]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 240]
	vmovq	xmm7, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 232]
	vmovq	xmm7, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 224]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rcx + 8*rdi + 144], xmm1
	vmovdqu	xmmword ptr [rcx + 8*rdi + 128], xmm8
	vmovdqu	xmmword ptr [rcx + 8*rdi + 160], xmm3
	vmovdqu	xmmword ptr [rcx + 8*rdi + 176], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rdi + 192], xmm5
	vmovdqu	xmmword ptr [rcx + 8*rdi + 208], xmm4
	vmovdqu	xmmword ptr [rcx + 8*rdi + 224], xmm0
	vmovdqu	xmmword ptr [rcx + 8*rdi + 240], xmm6
	add	rdi, 32
	add	r10, 2
	jne	.LBB0_631
	jmp	.LBB0_1111
.LBB0_632:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1115
# %bb.633:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_634:                              # =>This Inner Loop Header: Depth=1
	vcvtpd2ps	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvtpd2ps	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvtpd2ps	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvtpd2ps	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	xmmword ptr [rcx + 4*rdi], xmm0
	vmovupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovupd	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovupd	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vcvtpd2ps	xmm0, ymmword ptr [rdx + 8*rdi + 128]
	vcvtpd2ps	xmm1, ymmword ptr [rdx + 8*rdi + 160]
	vcvtpd2ps	xmm2, ymmword ptr [rdx + 8*rdi + 192]
	vcvtpd2ps	xmm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovupd	xmmword ptr [rcx + 4*rdi + 80], xmm1
	vmovupd	xmmword ptr [rcx + 4*rdi + 96], xmm2
	vmovupd	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_634
	jmp	.LBB0_1116
.LBB0_644:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	mov	r8d, r10d
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB0_850
# %bb.645:
	xor	eax, eax
	jmp	.LBB0_852
.LBB0_646:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_975
# %bb.647:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_648:                              # =>This Inner Loop Header: Depth=1
	vpmovzxwq	ymm0, qword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdx + 2*rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdx + 2*rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdx + 2*rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxwq	ymm0, qword ptr [rdx + 2*rdi + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdx + 2*rdi + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdx + 2*rdi + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdx + 2*rdi + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_648
	jmp	.LBB0_976
.LBB0_649:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1120
# %bb.650:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_651:                              # =>This Inner Loop Header: Depth=1
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_651
	jmp	.LBB0_1121
.LBB0_652:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1125
# %bb.653:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_654:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwq	ymm0, qword ptr [rdx + 2*rdi]
	vpmovsxwq	ymm1, qword ptr [rdx + 2*rdi + 8]
	vpmovsxwq	ymm2, qword ptr [rdx + 2*rdi + 16]
	vpmovsxwq	ymm3, qword ptr [rdx + 2*rdi + 24]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxwq	ymm0, qword ptr [rdx + 2*rdi + 32]
	vpmovsxwq	ymm1, qword ptr [rdx + 2*rdi + 40]
	vpmovsxwq	ymm2, qword ptr [rdx + 2*rdi + 48]
	vpmovsxwq	ymm3, qword ptr [rdx + 2*rdi + 56]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_654
	jmp	.LBB0_1126
.LBB0_655:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1130
# %bb.656:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_657:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi + 64]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 80]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 96]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 112]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_657
	jmp	.LBB0_1131
.LBB0_661:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1135
# %bb.662:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edi, edi
.LBB0_663:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	vpextrq	rax, xmm0, 1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm0
	vcvtsi2ss	xmm0, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm3, xmm8, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2ss	xmm1, xmm8, rax
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 32]
	vpextrq	rax, xmm4, 1
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 48]
	vcvtsi2ss	xmm6, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm5
	vcvtsi2ss	xmm7, xmm8, rax
	vinsertps	xmm0, xmm0, xmm2, 16    # xmm0 = xmm0[0],xmm2[0],xmm0[2,3]
	vinsertps	xmm0, xmm0, xmm3, 32    # xmm0 = xmm0[0,1],xmm3[0],xmm0[3]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm0, xmm0, xmm1, 48    # xmm0 = xmm0[0,1,2],xmm1[0]
	vcvtsi2ss	xmm1, xmm8, rax
	vinsertps	xmm2, xmm4, xmm6, 16    # xmm2 = xmm4[0],xmm6[0],xmm4[2,3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpextrq	rax, xmm3, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 80]
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm2, xmm2, xmm7, 32    # xmm2 = xmm2[0,1],xmm7[0],xmm2[3]
	vinsertps	xmm1, xmm2, xmm1, 48    # xmm1 = xmm2[0,1,2],xmm1[0]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm2, xmm3, xmm4, 16    # xmm2 = xmm3[0],xmm4[0],xmm3[2,3]
	vcvtsi2ss	xmm3, xmm8, rax
	vinsertps	xmm2, xmm2, xmm6, 32    # xmm2 = xmm2[0,1],xmm6[0],xmm2[3]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpextrq	rax, xmm4, 1
	vcvtsi2ss	xmm5, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vmovdqu	xmm6, xmmword ptr [rdx + 8*rdi + 112]
	vmovq	rax, xmm6
	vcvtsi2ss	xmm7, xmm8, rax
	vinsertps	xmm2, xmm2, xmm3, 48    # xmm2 = xmm2[0,1,2],xmm3[0]
	vinsertps	xmm3, xmm4, xmm5, 16    # xmm3 = xmm4[0],xmm5[0],xmm4[2,3]
	vpextrq	rax, xmm6, 1
	vinsertps	xmm3, xmm3, xmm7, 32    # xmm3 = xmm3[0,1],xmm7[0],xmm3[3]
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm3, xmm3, xmm4, 48    # xmm3 = xmm3[0,1,2],xmm4[0]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vmovdqu	xmm0, xmmword ptr [rdx + 8*rdi + 128]
	vpextrq	rax, xmm0, 1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 144]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm0
	vcvtsi2ss	xmm0, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm3, xmm8, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2ss	xmm1, xmm8, rax
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 160]
	vpextrq	rax, xmm4, 1
	vcvtsi2ss	xmm5, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm0, xmm0, xmm2, 16    # xmm0 = xmm0[0],xmm2[0],xmm0[2,3]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 176]
	vpextrq	r11, xmm2, 1
	vmovq	rax, xmm2
	vcvtsi2ss	xmm2, xmm8, rax
	vinsertps	xmm0, xmm0, xmm3, 32    # xmm0 = xmm0[0,1],xmm3[0],xmm0[3]
	vcvtsi2ss	xmm3, xmm8, r11
	vinsertps	xmm0, xmm0, xmm1, 48    # xmm0 = xmm0[0,1,2],xmm1[0]
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 192]
	vpextrq	rax, xmm1, 1
	vinsertps	xmm4, xmm4, xmm5, 16    # xmm4 = xmm4[0],xmm5[0],xmm4[2,3]
	vcvtsi2ss	xmm5, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm1, xmm8, rax
	vinsertps	xmm2, xmm4, xmm2, 32    # xmm2 = xmm4[0,1],xmm2[0],xmm4[3]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 208]
	vpextrq	r11, xmm4, 1
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm2, xmm2, xmm3, 48    # xmm2 = xmm2[0,1,2],xmm3[0]
	vcvtsi2ss	xmm3, xmm8, r11
	vinsertps	xmm1, xmm1, xmm5, 16    # xmm1 = xmm1[0],xmm5[0],xmm1[2,3]
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 224]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm1, xmm1, xmm4, 32    # xmm1 = xmm1[0,1],xmm4[0],xmm1[3]
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm5
	vcvtsi2ss	xmm5, xmm8, rax
	vinsertps	xmm1, xmm1, xmm3, 48    # xmm1 = xmm1[0,1,2],xmm3[0]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 240]
	vpextrq	r11, xmm3, 1
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vinsertps	xmm4, xmm5, xmm4, 16    # xmm4 = xmm5[0],xmm4[0],xmm5[2,3]
	vcvtsi2ss	xmm5, xmm8, r11
	vinsertps	xmm3, xmm4, xmm3, 32    # xmm3 = xmm4[0,1],xmm3[0],xmm4[3]
	vinsertps	xmm3, xmm3, xmm5, 48    # xmm3 = xmm3[0,1,2],xmm5[0]
	vmovups	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 80], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 96], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	r10, 2
	jne	.LBB0_663
	jmp	.LBB0_1136
.LBB0_664:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1140
# %bb.665:
	mov	r10, r8
	and	r10, -2
	neg	r10
	xor	edi, edi
.LBB0_666:                              # =>This Inner Loop Header: Depth=1
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 4]
	vmovq	xmm0, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi]
	vmovq	xmm1, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 12]
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 8]
	vmovq	xmm1, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 28]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 24]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovq	xmm2, rbx
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 20]
	vmovq	xmm3, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 16]
	vmovq	xmm4, rax
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 44]
	vmovq	xmm4, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 40]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 36]
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 32]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 60]
	vmovq	xmm6, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 56]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vmovq	xmm6, rax
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 52]
	vmovq	xmm7, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 48]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rcx + 8*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 8*rdi], xmm8
	vmovdqu	xmmword ptr [rcx + 8*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 8*rdi + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rdi + 64], xmm5
	vmovdqu	xmmword ptr [rcx + 8*rdi + 80], xmm4
	vmovdqu	xmmword ptr [rcx + 8*rdi + 96], xmm0
	vmovdqu	xmmword ptr [rcx + 8*rdi + 112], xmm6
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 68]
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 64]
	vmovq	xmm0, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 76]
	vmovq	xmm1, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 72]
	vmovq	xmm2, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vmovq	xmm1, rbx
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 92]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 88]
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 84]
	vmovq	xmm3, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 80]
	vmovq	xmm4, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 108]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 104]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 100]
	vmovq	xmm7, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 96]
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vpunpcklqdq	xmm4, xmm6, xmm5        # xmm4 = xmm6[0],xmm5[0]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm5, xmm5, xmm7        # xmm5 = xmm5[0],xmm7[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 124]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 120]
	vmovq	xmm7, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 116]
	vmovq	xmm7, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 112]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm0, xmm7        # xmm0 = xmm0[0],xmm7[0]
	vmovdqu	xmmword ptr [rcx + 8*rdi + 144], xmm1
	vmovdqu	xmmword ptr [rcx + 8*rdi + 128], xmm8
	vmovdqu	xmmword ptr [rcx + 8*rdi + 160], xmm3
	vmovdqu	xmmword ptr [rcx + 8*rdi + 176], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rdi + 192], xmm5
	vmovdqu	xmmword ptr [rcx + 8*rdi + 208], xmm4
	vmovdqu	xmmword ptr [rcx + 8*rdi + 224], xmm0
	vmovdqu	xmmword ptr [rcx + 8*rdi + 240], xmm6
	add	rdi, 32
	add	r10, 2
	jne	.LBB0_666
	jmp	.LBB0_1141
.LBB0_676:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_980
# %bb.677:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_678:                              # =>This Inner Loop Header: Depth=1
	vpmovsxdq	ymm0, xmmword ptr [rdx + 4*rdi]
	vpmovsxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxdq	ymm0, xmmword ptr [rdx + 4*rdi + 64]
	vpmovsxdq	ymm1, xmmword ptr [rdx + 4*rdi + 80]
	vpmovsxdq	ymm2, xmmword ptr [rdx + 4*rdi + 96]
	vpmovsxdq	ymm3, xmmword ptr [rdx + 4*rdi + 112]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_678
	jmp	.LBB0_981
.LBB0_679:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_985
# %bb.680:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
	vcvtdq2ps	ymm0, ymmword ptr [rdx + 4*rdi]
	vcvtdq2ps	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vcvtdq2ps	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vcvtdq2ps	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vcvtdq2ps	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vcvtdq2ps	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vcvtdq2ps	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vcvtdq2ps	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovups	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_681
	jmp	.LBB0_986
.LBB0_715:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_990
# %bb.716:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_717:                              # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	xmmword ptr [rcx + 4*rdi], xmm0
	vmovupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovupd	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovupd	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 160]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 192]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovupd	xmmword ptr [rcx + 4*rdi + 80], xmm1
	vmovupd	xmmword ptr [rcx + 4*rdi + 96], xmm2
	vmovupd	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_717
	jmp	.LBB0_991
.LBB0_721:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_995
# %bb.722:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_723:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi + 128]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 160]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 192]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 80], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 96], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_723
	jmp	.LBB0_996
.LBB0_724:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1000
# %bb.725:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_726:                              # =>This Inner Loop Header: Depth=1
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_726
	jmp	.LBB0_1001
.LBB0_727:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1005
# %bb.728:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_729:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi + 64]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 80]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 96]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 112]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_729
	jmp	.LBB0_1006
.LBB0_730:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1010
# %bb.731:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_732:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi + 128]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 160]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 192]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 224]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 144], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 176], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 208], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 240], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi + 64], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 80], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 96], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 112], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_732
	jmp	.LBB0_1011
.LBB0_733:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1015
# %bb.734:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_735:                              # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_735
	jmp	.LBB0_1016
.LBB0_742:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1185
# %bb.743:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_744:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_744
	jmp	.LBB0_1186
.LBB0_745:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1193
# %bb.746:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_747:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm0, qword ptr [rdx + rdi]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 24]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovsxbd	ymm0, qword ptr [rdx + rdi + 32]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 40]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 48]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 56]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_747
	jmp	.LBB0_1194
.LBB0_748:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1201
# %bb.749:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_750:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	ymm0, qword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovzxbd	ymm0, qword ptr [rdx + rdi + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_750
	jmp	.LBB0_1202
.LBB0_751:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1209
# %bb.752:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_753:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_753
	jmp	.LBB0_1210
.LBB0_754:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1217
# %bb.755:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_756:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_756
	jmp	.LBB0_1218
.LBB0_757:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1225
# %bb.758:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_759:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	xmm0, dword ptr [rdx + rdi]
	vpmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	vpmovsxbd	xmm2, dword ptr [rdx + rdi + 8]
	vpmovsxbd	xmm3, dword ptr [rdx + rdi + 12]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxbd	xmm0, dword ptr [rdx + rdi + 16]
	vpmovsxbd	xmm1, dword ptr [rdx + rdi + 20]
	vpmovsxbd	xmm2, dword ptr [rdx + rdi + 24]
	vpmovsxbd	xmm3, dword ptr [rdx + rdi + 28]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_759
	jmp	.LBB0_1226
.LBB0_760:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1233
# %bb.761:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_762:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	xmm0, dword ptr [rdx + rdi] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm2, dword ptr [rdx + rdi + 8] # xmm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm3, dword ptr [rdx + rdi + 12] # xmm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxbd	xmm0, dword ptr [rdx + rdi + 16] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm1, dword ptr [rdx + rdi + 20] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm2, dword ptr [rdx + rdi + 24] # xmm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm3, dword ptr [rdx + rdi + 28] # xmm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_762
	jmp	.LBB0_1234
.LBB0_763:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1241
# %bb.764:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_765:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_765
	jmp	.LBB0_1242
.LBB0_766:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1249
# %bb.767:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_768:                              # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vpackssdw	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vpacksswb	xmm0, xmm0, xmm0
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 64]
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 96]
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 160]
	vpackssdw	xmm0, xmm0, xmm0
	vpacksswb	xmm0, xmm0, xmm0
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 192]
	vpackssdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 224]
	vpacksswb	xmm1, xmm1, xmm1
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_768
	jmp	.LBB0_1250
.LBB0_769:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB0_1257
# %bb.770:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_771:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [rcx + rdi], ymm0
	vmovups	ymmword ptr [rcx + rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovupd	ymmword ptr [rcx + rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 224], ymm3
	add	rdi, 256
	add	rax, 2
	jne	.LBB0_771
	jmp	.LBB0_1258
.LBB0_772:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1265
# %bb.773:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_774:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_774
	jmp	.LBB0_1266
.LBB0_775:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1273
# %bb.776:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_777:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 112], xmm4
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_777
	jmp	.LBB0_1274
.LBB0_778:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1281
# %bb.779:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_780:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 112], xmm4
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_780
	jmp	.LBB0_1282
.LBB0_781:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1289
# %bb.782:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_783:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_783
	jmp	.LBB0_1290
.LBB0_784:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1297
# %bb.785:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_786:                              # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpacksswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpacksswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpacksswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpacksswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_786
	jmp	.LBB0_1298
.LBB0_787:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB0_1305
# %bb.788:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_789:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [rcx + rdi], ymm0
	vmovups	ymmword ptr [rcx + rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovupd	ymmword ptr [rcx + rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 224], ymm3
	add	rdi, 256
	add	rax, 2
	jne	.LBB0_789
	jmp	.LBB0_1306
.LBB0_790:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1313
# %bb.791:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_792:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_792
	jmp	.LBB0_1314
.LBB0_801:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1321
# %bb.802:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_803:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbq	ymm0, dword ptr [rdx + rdi]
	vpmovsxbq	ymm1, dword ptr [rdx + rdi + 4]
	vpmovsxbq	ymm2, dword ptr [rdx + rdi + 8]
	vpmovsxbq	ymm3, dword ptr [rdx + rdi + 12]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxbq	ymm0, dword ptr [rdx + rdi + 16]
	vpmovsxbq	ymm1, dword ptr [rdx + rdi + 20]
	vpmovsxbq	ymm2, dword ptr [rdx + rdi + 24]
	vpmovsxbq	ymm3, dword ptr [rdx + rdi + 28]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_803
	jmp	.LBB0_1322
.LBB0_804:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1329
# %bb.805:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_806:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_806
	jmp	.LBB0_1330
.LBB0_807:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1337
# %bb.808:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_809:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_809
	jmp	.LBB0_1338
.LBB0_818:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1345
# %bb.819:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_820:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbq	ymm0, dword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdx + rdi + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdx + rdi + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdx + rdi + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxbq	ymm0, dword ptr [rdx + rdi + 16] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdx + rdi + 20] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdx + rdi + 24] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdx + rdi + 28] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_820
	jmp	.LBB0_1346
.LBB0_821:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1353
# %bb.822:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_823:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbw	ymm0, xmmword ptr [rdx + rdi]
	vpmovsxbw	ymm1, xmmword ptr [rdx + rdi + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdx + rdi + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdx + rdi + 48]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
	vpmovsxbw	ymm0, xmmword ptr [rdx + rdi + 64]
	vpmovsxbw	ymm1, xmmword ptr [rdx + rdi + 80]
	vpmovsxbw	ymm2, xmmword ptr [rdx + rdi + 96]
	vpmovsxbw	ymm3, xmmword ptr [rdx + rdi + 112]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 224], ymm3
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_823
	jmp	.LBB0_1354
.LBB0_824:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1361
# %bb.825:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_826:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbw	ymm0, xmmword ptr [rdx + rdi]
	vpmovsxbw	ymm1, xmmword ptr [rdx + rdi + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdx + rdi + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdx + rdi + 48]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
	vpmovsxbw	ymm0, xmmword ptr [rdx + rdi + 64]
	vpmovsxbw	ymm1, xmmword ptr [rdx + rdi + 80]
	vpmovsxbw	ymm2, xmmword ptr [rdx + rdi + 96]
	vpmovsxbw	ymm3, xmmword ptr [rdx + rdi + 112]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 224], ymm3
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_826
	jmp	.LBB0_1362
.LBB0_827:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1145
# %bb.828:
	xor	eax, eax
	jmp	.LBB0_1147
.LBB0_829:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1155
# %bb.830:
	xor	eax, eax
	jmp	.LBB0_1157
.LBB0_831:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1165
# %bb.832:
	xor	eax, eax
	jmp	.LBB0_1167
.LBB0_833:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB0_1175
# %bb.834:
	xor	eax, eax
	jmp	.LBB0_1177
.LBB0_835:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1369
# %bb.836:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_837:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbw	ymm0, xmmword ptr [rdx + rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdx + rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdx + rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdx + rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
	vpmovzxbw	ymm0, xmmword ptr [rdx + rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdx + rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdx + rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdx + rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rcx + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 224], ymm3
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_837
	jmp	.LBB0_1370
.LBB0_838:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1377
# %bb.839:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_840:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbw	ymm0, xmmword ptr [rdx + rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdx + rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdx + rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdx + rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
	vpmovzxbw	ymm0, xmmword ptr [rdx + rdi + 64] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdx + rdi + 80] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdx + rdi + 96] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdx + rdi + 112] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rcx + 2*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 224], ymm3
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_840
	jmp	.LBB0_1378
.LBB0_841:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1385
# %bb.842:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_843:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbq	ymm0, dword ptr [rdx + rdi]
	vpmovsxbq	ymm1, dword ptr [rdx + rdi + 4]
	vpmovsxbq	ymm2, dword ptr [rdx + rdi + 8]
	vpmovsxbq	ymm3, dword ptr [rdx + rdi + 12]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovsxbq	ymm0, dword ptr [rdx + rdi + 16]
	vpmovsxbq	ymm1, dword ptr [rdx + rdi + 20]
	vpmovsxbq	ymm2, dword ptr [rdx + rdi + 24]
	vpmovsxbq	ymm3, dword ptr [rdx + rdi + 28]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_843
	jmp	.LBB0_1386
.LBB0_844:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1393
# %bb.845:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_846:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm0, qword ptr [rdx + rdi]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 24]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovsxbd	ymm0, qword ptr [rdx + rdi + 32]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 40]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 48]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 56]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_846
	jmp	.LBB0_1394
.LBB0_847:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1401
# %bb.848:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_849:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_849
	jmp	.LBB0_1402
.LBB0_860:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1409
# %bb.861:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_862:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovups	ymmword ptr [rcx + 8*rdi], ymm0
	vmovups	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_862
	jmp	.LBB0_1410
.LBB0_863:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1417
# %bb.864:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_865:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_865
	jmp	.LBB0_1418
.LBB0_866:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1425
# %bb.867:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_868:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbq	ymm0, dword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdx + rdi + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdx + rdi + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdx + rdi + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
	vpmovzxbq	ymm0, dword ptr [rdx + rdi + 16] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdx + rdi + 20] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdx + rdi + 24] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdx + rdi + 28] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_868
	jmp	.LBB0_1426
.LBB0_869:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1433
# %bb.870:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_871:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	ymm0, qword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovzxbd	ymm0, qword ptr [rdx + rdi + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_871
	jmp	.LBB0_1434
.LBB0_872:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1441
# %bb.873:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_874:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_874
	jmp	.LBB0_1442
.LBB0_875:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1449
# %bb.876:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_877:                              # =>This Inner Loop Header: Depth=1
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vpackusdw	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vpackuswb	xmm0, xmm0, xmm0
	vpackusdw	xmm1, xmm1, xmm1
	vpackuswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 64]
	vpackusdw	xmm1, xmm1, xmm1
	vpackuswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 96]
	vpackusdw	xmm2, xmm2, xmm2
	vpackuswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi + 128]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 160]
	vpackusdw	xmm0, xmm0, xmm0
	vpackuswb	xmm0, xmm0, xmm0
	vpackusdw	xmm1, xmm1, xmm1
	vpackuswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 192]
	vpackusdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 224]
	vpackuswb	xmm1, xmm1, xmm1
	vpackusdw	xmm2, xmm2, xmm2
	vpackuswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_877
	jmp	.LBB0_1450
.LBB0_878:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB0_1457
# %bb.879:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_880:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [rcx + rdi], ymm0
	vmovups	ymmword ptr [rcx + rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovupd	ymmword ptr [rcx + rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 224], ymm3
	add	rdi, 256
	add	rax, 2
	jne	.LBB0_880
	jmp	.LBB0_1458
.LBB0_881:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1465
# %bb.882:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_883:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_883
	jmp	.LBB0_1466
.LBB0_884:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1473
# %bb.885:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_886:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 112], xmm4
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_886
	jmp	.LBB0_1474
.LBB0_887:
	mov	esi, r9d
	and	esi, -64
	lea	rax, [rsi - 64]
	mov	r8, rax
	shr	r8, 6
	add	r8, 1
	test	rax, rax
	je	.LBB0_1481
# %bb.888:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_889:                              # =>This Inner Loop Header: Depth=1
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm4
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 128]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 160]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 192]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm4, ymm0, ymmword ptr [rdx + 2*rdi + 224]
	vextracti128	xmm5, ymm4, 1
	vpackuswb	xmm4, xmm4, xmm5
	vmovdqu	xmmword ptr [rcx + rdi + 64], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 80], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 96], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 112], xmm4
	sub	rdi, -128
	add	rax, 2
	jne	.LBB0_889
	jmp	.LBB0_1482
.LBB0_890:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB0_1489
# %bb.891:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_892:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 208]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 192]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 240]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 224]
	vpshufb	xmm4, xmm4, xmm0
	vpunpcklwd	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1],xmm4[2],xmm3[2],xmm4[3],xmm3[3]
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB0_892
	jmp	.LBB0_1490
.LBB0_893:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1497
# %bb.894:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_895:                              # =>This Inner Loop Header: Depth=1
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 32]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 64]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpackuswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpackuswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vextracti128	xmm1, ymm0, 1
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 160]
	vpackssdw	xmm0, xmm0, xmm1
	vextracti128	xmm1, ymm2, 1
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 192]
	vpackssdw	xmm1, xmm2, xmm1
	vextracti128	xmm2, ymm3, 1
	vcvttps2dq	ymm4, ymmword ptr [rdx + 4*rdi + 224]
	vpackssdw	xmm2, xmm3, xmm2
	vextracti128	xmm3, ymm4, 1
	vpackssdw	xmm3, xmm4, xmm3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpackuswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpackuswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_895
	jmp	.LBB0_1498
.LBB0_896:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB0_1505
# %bb.897:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_898:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + rdi]
	vmovups	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovups	ymmword ptr [rcx + rdi], ymm0
	vmovups	ymmword ptr [rcx + rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 224]
	vmovupd	ymmword ptr [rcx + rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 224], ymm3
	add	rdi, 256
	add	rax, 2
	jne	.LBB0_898
	jmp	.LBB0_1506
.LBB0_899:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1513
# %bb.900:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB0_901:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi + 128]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 144]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 160]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 176]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 208]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 192]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 240]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 224]
	vpshufb	xmm5, xmm5, xmm0
	vpunpckldq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0],xmm5[1],xmm4[1]
	vinserti128	ymm3, ymm3, xmm4, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm1, ymm1, ymm3        # ymm1 = ymm1[0],ymm3[0],ymm1[2],ymm3[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm1
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_901
	jmp	.LBB0_1514
.LBB0_902:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1521
# %bb.903:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_904:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_904
	jmp	.LBB0_1522
.LBB0_905:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1529
# %bb.906:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_907:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm0, qword ptr [rdx + rdi]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 24]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovsxbd	ymm0, qword ptr [rdx + rdi + 32]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 40]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 48]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 56]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_907
	jmp	.LBB0_1530
.LBB0_908:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1537
# %bb.909:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_910:                              # =>This Inner Loop Header: Depth=1
	vpmovzxbd	ymm0, qword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vpmovzxbd	ymm0, qword ptr [rdx + rdi + 32] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 40] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 48] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 56] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_910
	jmp	.LBB0_1538
.LBB0_911:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB0_1545
# %bb.912:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_913:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovups	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 160]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 192]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB0_913
	jmp	.LBB0_1546
.LBB0_793:
	and	r10, -4
	neg	r10
	xor	eax, eax
	vmovsd	xmm0, qword ptr [rip + .LCPI0_0] # xmm0 = mem[0],zero
.LBB0_794:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rax + 8] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rbx, xmm2
	vmovsd	xmm2, qword ptr [rdx + 8*rax]   # xmm2 = mem[0],zero
	xor	rbx, r11
	vcvttsd2si	rsi, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rsi, rbx
	vsubsd	xmm1, xmm2, xmm0
	vcvttsd2si	rbx, xmm1
	xor	rbx, r11
	vcvttsd2si	rdi, xmm2
	vucomisd	xmm2, xmm0
	vmovq	xmm1, rsi
	cmovae	rdi, rbx
	vmovq	xmm2, rdi
	vmovsd	xmm3, qword ptr [rdx + 8*rax + 24] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rsi, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rsi, r11
	vcvttsd2si	rdi, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdi, rsi
	vmovsd	xmm2, qword ptr [rdx + 8*rax + 16] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rsi, xmm3
	vmovq	xmm3, rdi
	xor	rsi, r11
	vcvttsd2si	rdi, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rax + 40] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rsi, xmm2
	vmovsd	xmm2, qword ptr [rdx + 8*rax + 32] # xmm2 = mem[0],zero
	xor	rsi, r11
	vcvttsd2si	rdi, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdi, rsi
	vsubsd	xmm1, xmm2, xmm0
	vcvttsd2si	rsi, xmm1
	xor	rsi, r11
	vcvttsd2si	rbx, xmm2
	vucomisd	xmm2, xmm0
	vmovq	xmm1, rdi
	cmovae	rbx, rsi
	vmovq	xmm2, rbx
	vmovsd	xmm3, qword ptr [rdx + 8*rax + 56] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rsi, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rsi, r11
	vcvttsd2si	rdi, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdi, rsi
	vmovsd	xmm2, qword ptr [rdx + 8*rax + 48] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rsi, xmm3
	vmovq	xmm3, rdi
	xor	rsi, r11
	vcvttsd2si	rdi, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax + 32], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rax + 72] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rsi, xmm2
	vmovsd	xmm2, qword ptr [rdx + 8*rax + 64] # xmm2 = mem[0],zero
	xor	rsi, r11
	vcvttsd2si	rdi, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdi, rsi
	vsubsd	xmm1, xmm2, xmm0
	vcvttsd2si	rsi, xmm1
	xor	rsi, r11
	vcvttsd2si	rbx, xmm2
	vucomisd	xmm2, xmm0
	vmovq	xmm1, rdi
	cmovae	rbx, rsi
	vmovq	xmm2, rbx
	vmovsd	xmm3, qword ptr [rdx + 8*rax + 88] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rsi, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rsi, r11
	vcvttsd2si	rdi, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdi, rsi
	vmovsd	xmm2, qword ptr [rdx + 8*rax + 80] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rsi, xmm3
	vmovq	xmm3, rdi
	xor	rsi, r11
	vcvttsd2si	rdi, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 80], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax + 64], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rax + 104] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rsi, xmm2
	xor	rsi, r11
	vcvttsd2si	rdi, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdi, rsi
	vmovsd	xmm1, qword ptr [rdx + 8*rax + 96] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rsi, xmm2
	xor	rsi, r11
	vcvttsd2si	rbx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rbx, rsi
	vmovq	xmm1, rdi
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovsd	xmm2, qword ptr [rdx + 8*rax + 120] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rsi, xmm3
	xor	rsi, r11
	vcvttsd2si	rdi, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vmovsd	xmm3, qword ptr [rdx + 8*rax + 112] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rsi, xmm4
	xor	rsi, r11
	vcvttsd2si	rdi, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdi, rsi
	vmovq	xmm3, rdi
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 112], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax + 96], xmm1
	add	rax, 16
	add	r10, 4
	jne	.LBB0_794
.LBB0_795:
	test	r8, r8
	je	.LBB0_798
# %bb.796:
	shl	rax, 3
	neg	r8
	vmovsd	xmm0, qword ptr [rip + .LCPI0_0] # xmm0 = mem[0],zero
.LBB0_797:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + rax + 8] # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rsi, xmm2
	xor	rsi, r11
	vcvttsd2si	rdi, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rdi, rsi
	vmovsd	xmm1, qword ptr [rdx + rax]     # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rsi, xmm2
	xor	rsi, r11
	vcvttsd2si	rbx, xmm1
	vucomisd	xmm1, xmm0
	cmovae	rbx, rsi
	vmovq	xmm1, rdi
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovsd	xmm2, qword ptr [rdx + rax + 24] # xmm2 = mem[0],zero
	vsubsd	xmm3, xmm2, xmm0
	vcvttsd2si	rsi, xmm3
	xor	rsi, r11
	vcvttsd2si	rdi, xmm2
	vucomisd	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vmovsd	xmm3, qword ptr [rdx + rax + 16] # xmm3 = mem[0],zero
	vsubsd	xmm4, xmm3, xmm0
	vcvttsd2si	rsi, xmm4
	xor	rsi, r11
	vcvttsd2si	rdi, xmm3
	vucomisd	xmm3, xmm0
	cmovae	rdi, rsi
	vmovq	xmm3, rdi
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + rax + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB0_797
.LBB0_798:
	cmp	r14, r9
	je	.LBB0_1553
.LBB0_799:
	vmovsd	xmm0, qword ptr [rip + .LCPI0_0] # xmm0 = mem[0],zero
.LBB0_800:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*r14]   # xmm1 = mem[0],zero
	vsubsd	xmm2, xmm1, xmm0
	vcvttsd2si	rax, xmm2
	xor	rax, r11
	vcvttsd2si	rsi, xmm1
	vucomisd	xmm0, xmm1
	cmovbe	rsi, rax
	mov	qword ptr [rcx + 8*r14], rsi
	add	r14, 1
	cmp	r9, r14
	jne	.LBB0_800
	jmp	.LBB0_1553
.LBB0_810:
	and	r10, -4
	neg	r10
	xor	eax, eax
	vmovss	xmm0, dword ptr [rip + .LCPI0_1] # xmm0 = mem[0],zero,zero,zero
	movabs	r11, -9223372036854775808
.LBB0_811:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rax + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rdi, xmm2
	vmovss	xmm2, dword ptr [rdx + 4*rax]   # xmm2 = mem[0],zero,zero,zero
	xor	rdi, r11
	vcvttss2si	rbx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rbx, rdi
	vsubss	xmm1, xmm2, xmm0
	vcvttss2si	rdi, xmm1
	xor	rdi, r11
	vcvttss2si	rsi, xmm2
	vucomiss	xmm2, xmm0
	vmovq	xmm1, rbx
	cmovae	rsi, rdi
	vmovq	xmm2, rsi
	vmovss	xmm3, dword ptr [rdx + 4*rax + 12] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rsi, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rsi, r11
	vcvttss2si	rdi, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdi, rsi
	vmovss	xmm2, dword ptr [rdx + 4*rax + 8] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rsi, xmm3
	vmovq	xmm3, rdi
	xor	rsi, r11
	vcvttss2si	rdi, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rax + 20] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	vmovss	xmm2, dword ptr [rdx + 4*rax + 16] # xmm2 = mem[0],zero,zero,zero
	xor	rsi, r11
	vcvttss2si	rdi, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdi, rsi
	vsubss	xmm1, xmm2, xmm0
	vcvttss2si	rsi, xmm1
	xor	rsi, r11
	vcvttss2si	rbx, xmm2
	vucomiss	xmm2, xmm0
	vmovq	xmm1, rdi
	cmovae	rbx, rsi
	vmovq	xmm2, rbx
	vmovss	xmm3, dword ptr [rdx + 4*rax + 28] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rsi, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rsi, r11
	vcvttss2si	rdi, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdi, rsi
	vmovss	xmm2, dword ptr [rdx + 4*rax + 24] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rsi, xmm3
	vmovq	xmm3, rdi
	xor	rsi, r11
	vcvttss2si	rdi, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax + 32], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rax + 36] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	vmovss	xmm2, dword ptr [rdx + 4*rax + 32] # xmm2 = mem[0],zero,zero,zero
	xor	rsi, r11
	vcvttss2si	rdi, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdi, rsi
	vsubss	xmm1, xmm2, xmm0
	vcvttss2si	rsi, xmm1
	xor	rsi, r11
	vcvttss2si	rbx, xmm2
	vucomiss	xmm2, xmm0
	vmovq	xmm1, rdi
	cmovae	rbx, rsi
	vmovq	xmm2, rbx
	vmovss	xmm3, dword ptr [rdx + 4*rax + 44] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rsi, xmm4
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	xor	rsi, r11
	vcvttss2si	rdi, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdi, rsi
	vmovss	xmm2, dword ptr [rdx + 4*rax + 40] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rsi, xmm3
	vmovq	xmm3, rdi
	xor	rsi, r11
	vcvttss2si	rdi, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 80], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax + 64], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rax + 52] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	xor	rsi, r11
	vcvttss2si	rdi, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdi, rsi
	vmovss	xmm1, dword ptr [rdx + 4*rax + 48] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	xor	rsi, r11
	vcvttss2si	rbx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rbx, rsi
	vmovq	xmm1, rdi
	vmovq	xmm2, rbx
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovss	xmm2, dword ptr [rdx + 4*rax + 60] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rsi, xmm3
	xor	rsi, r11
	vcvttss2si	rdi, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vmovss	xmm3, dword ptr [rdx + 4*rax + 56] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rsi, xmm4
	xor	rsi, r11
	vcvttss2si	rdi, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdi, rsi
	vmovq	xmm3, rdi
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + 8*rax + 112], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rax + 96], xmm1
	add	rax, 16
	add	r10, 4
	jne	.LBB0_811
.LBB0_812:
	test	r8, r8
	je	.LBB0_815
# %bb.813:
	shl	rax, 2
	neg	r8
	vmovss	xmm0, dword ptr [rip + .LCPI0_1] # xmm0 = mem[0],zero,zero,zero
	movabs	r10, -9223372036854775808
.LBB0_814:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + rax + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	xor	rsi, r10
	vcvttss2si	rbx, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rbx, rsi
	vmovss	xmm1, dword ptr [rdx + rax]     # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	xor	rsi, r10
	vcvttss2si	rdi, xmm1
	vucomiss	xmm1, xmm0
	cmovae	rdi, rsi
	vmovq	xmm1, rbx
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovss	xmm2, dword ptr [rdx + rax + 12] # xmm2 = mem[0],zero,zero,zero
	vsubss	xmm3, xmm2, xmm0
	vcvttss2si	rsi, xmm3
	xor	rsi, r10
	vcvttss2si	rdi, xmm2
	vucomiss	xmm2, xmm0
	cmovae	rdi, rsi
	vmovq	xmm2, rdi
	vmovss	xmm3, dword ptr [rdx + rax + 8] # xmm3 = mem[0],zero,zero,zero
	vsubss	xmm4, xmm3, xmm0
	vcvttss2si	rsi, xmm4
	xor	rsi, r10
	vcvttss2si	rdi, xmm3
	vucomiss	xmm3, xmm0
	cmovae	rdi, rsi
	vmovq	xmm3, rdi
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovdqu	xmmword ptr [rcx + 2*rax + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rax], xmm1
	add	rax, 16
	inc	r8
	jne	.LBB0_814
.LBB0_815:
	cmp	r14, r9
	je	.LBB0_1553
.LBB0_816:
	vmovss	xmm0, dword ptr [rip + .LCPI0_1] # xmm0 = mem[0],zero,zero,zero
	movabs	rax, -9223372036854775808
.LBB0_817:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*r14]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm1, xmm0
	vcvttss2si	rsi, xmm2
	xor	rsi, rax
	vcvttss2si	rdi, xmm1
	vucomiss	xmm0, xmm1
	cmovbe	rdi, rsi
	mov	qword ptr [rcx + 8*r14], rdi
	add	r14, 1
	cmp	r9, r14
	jne	.LBB0_817
	jmp	.LBB0_1553
.LBB0_850:
	and	r10, -4
	neg	r10
	xor	eax, eax
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_10] # ymm0 = [1,1,1,1]
.LBB0_851:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdi, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdi
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm3, xmm5, rdi
	vextracti128	xmm1, ymm1, 1
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm4, xmm5, rdi
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vpextrq	rdi, xmm1, 1
	vinsertps	xmm1, xmm2, xmm4, 32    # xmm1 = xmm2[0,1],xmm4[0],xmm2[3]
	vcvtsi2ss	xmm2, xmm5, rdi
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rax]
	vpackssdw	xmm2, xmm2, xmmword ptr [rdx + 8*rax + 16]
	vaddps	xmm3, xmm1, xmm1
	vblendvps	xmm1, xmm1, xmm3, xmm2
	vmovups	xmmword ptr [rcx + 4*rax], xmm1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 32]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdi, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdi
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm3, xmm5, rdi
	vextracti128	xmm1, ymm1, 1
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm4, xmm5, rdi
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vpextrq	rdi, xmm1, 1
	vinsertps	xmm1, xmm2, xmm4, 32    # xmm1 = xmm2[0,1],xmm4[0],xmm2[3]
	vcvtsi2ss	xmm2, xmm5, rdi
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rax + 32]
	vpackssdw	xmm2, xmm2, xmmword ptr [rdx + 8*rax + 48]
	vaddps	xmm3, xmm1, xmm1
	vblendvps	xmm1, xmm1, xmm3, xmm2
	vmovups	xmmword ptr [rcx + 4*rax + 16], xmm1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 64]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdi, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdi
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm3, xmm5, rdi
	vextracti128	xmm1, ymm1, 1
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm4, xmm5, rdi
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vpextrq	rdi, xmm1, 1
	vinsertps	xmm1, xmm2, xmm4, 32    # xmm1 = xmm2[0,1],xmm4[0],xmm2[3]
	vcvtsi2ss	xmm2, xmm5, rdi
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rax + 64]
	vpackssdw	xmm2, xmm2, xmmword ptr [rdx + 8*rax + 80]
	vaddps	xmm3, xmm1, xmm1
	vblendvps	xmm1, xmm1, xmm3, xmm2
	vmovups	xmmword ptr [rcx + 4*rax + 32], xmm1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 96]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdi, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdi
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm3, xmm5, rdi
	vextracti128	xmm1, ymm1, 1
	vpextrq	r11, xmm1, 1
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm1, xmm5, rdi
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vcvtsi2ss	xmm3, xmm5, r11
	vinsertps	xmm1, xmm2, xmm1, 32    # xmm1 = xmm2[0,1],xmm1[0],xmm2[3]
	vinsertps	xmm1, xmm1, xmm3, 48    # xmm1 = xmm1[0,1,2],xmm3[0]
	vaddps	xmm2, xmm1, xmm1
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rax + 96]
	vpackssdw	xmm3, xmm3, xmmword ptr [rdx + 8*rax + 112]
	vblendvps	xmm1, xmm1, xmm2, xmm3
	vmovups	xmmword ptr [rcx + 4*rax + 48], xmm1
	add	rax, 16
	add	r10, 4
	jne	.LBB0_851
.LBB0_852:
	test	r8, r8
	je	.LBB0_855
# %bb.853:
	shl	rax, 2
	neg	r8
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_10] # ymm0 = [1,1,1,1]
.LBB0_854:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax]
	vpand	ymm2, ymm1, ymm0
	vpsrlq	ymm3, ymm1, 1
	vpor	ymm2, ymm3, ymm2
	vblendvpd	ymm1, ymm1, ymm2, ymm1
	vpextrq	rdi, xmm1, 1
	vcvtsi2ss	xmm2, xmm5, rdi
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm3, xmm5, rdi
	vextracti128	xmm1, ymm1, 1
	vpextrq	r10, xmm1, 1
	vmovq	rdi, xmm1
	vcvtsi2ss	xmm1, xmm5, rdi
	vinsertps	xmm2, xmm3, xmm2, 16    # xmm2 = xmm3[0],xmm2[0],xmm3[2,3]
	vcvtsi2ss	xmm3, xmm5, r10
	vinsertps	xmm1, xmm2, xmm1, 32    # xmm1 = xmm2[0,1],xmm1[0],xmm2[3]
	vinsertps	xmm1, xmm1, xmm3, 48    # xmm1 = xmm1[0,1,2],xmm3[0]
	vaddps	xmm2, xmm1, xmm1
	vmovdqu	xmm3, xmmword ptr [rdx + 2*rax]
	vpackssdw	xmm3, xmm3, xmmword ptr [rdx + 2*rax + 16]
	vblendvps	xmm1, xmm1, xmm2, xmm3
	vmovups	xmmword ptr [rcx + rax], xmm1
	add	rax, 16
	inc	r8
	jne	.LBB0_854
.LBB0_855:
	cmp	rsi, r9
	jne	.LBB0_858
	jmp	.LBB0_1553
.LBB0_856:                              #   in Loop: Header=BB0_858 Depth=1
	vcvtsi2ss	xmm0, xmm5, rax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	je	.LBB0_1553
.LBB0_858:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	test	rax, rax
	jns	.LBB0_856
# %bb.859:                              #   in Loop: Header=BB0_858 Depth=1
	mov	rdi, rax
	shr	rdi
	and	eax, 1
	or	rax, rdi
	vcvtsi2ss	xmm0, xmm5, rax
	vaddss	xmm0, xmm0, xmm0
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_858
	jmp	.LBB0_1553
.LBB0_914:
	xor	edi, edi
.LBB0_915:
	test	r8b, 1
	je	.LBB0_917
# %bb.916:
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_917:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_918:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_918
	jmp	.LBB0_1553
.LBB0_919:
	xor	edi, edi
.LBB0_920:
	test	r8b, 1
	je	.LBB0_922
# %bb.921:
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vxorpd	xmm4, xmm4, xmm4
	vpblendd	ymm5, ymm0, ymm4, 170           # ymm5 = ymm0[0],ymm4[1],ymm0[2],ymm4[3],ymm0[4],ymm4[5],ymm0[6],ymm4[7]
	vpbroadcastq	ymm6, qword ptr [rip + .LCPI0_5] # ymm6 = [4841369599423283200,4841369599423283200,4841369599423283200,4841369599423283200]
	vpor	ymm5, ymm5, ymm6
	vpsrlq	ymm0, ymm0, 32
	vpbroadcastq	ymm7, qword ptr [rip + .LCPI0_6] # ymm7 = [4985484787499139072,4985484787499139072,4985484787499139072,4985484787499139072]
	vpor	ymm0, ymm0, ymm7
	vbroadcastsd	ymm8, qword ptr [rip + .LCPI0_7] # ymm8 = [1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25,1.9342813118337666E+25]
	vsubpd	ymm0, ymm0, ymm8
	vaddpd	ymm0, ymm5, ymm0
	vpblendd	ymm5, ymm1, ymm4, 170           # ymm5 = ymm1[0],ymm4[1],ymm1[2],ymm4[3],ymm1[4],ymm4[5],ymm1[6],ymm4[7]
	vpor	ymm5, ymm5, ymm6
	vpsrlq	ymm1, ymm1, 32
	vpor	ymm1, ymm1, ymm7
	vsubpd	ymm1, ymm1, ymm8
	vaddpd	ymm1, ymm5, ymm1
	vpblendd	ymm5, ymm2, ymm4, 170           # ymm5 = ymm2[0],ymm4[1],ymm2[2],ymm4[3],ymm2[4],ymm4[5],ymm2[6],ymm4[7]
	vpor	ymm5, ymm5, ymm6
	vpsrlq	ymm2, ymm2, 32
	vpor	ymm2, ymm2, ymm7
	vsubpd	ymm2, ymm2, ymm8
	vaddpd	ymm2, ymm5, ymm2
	vpblendd	ymm4, ymm3, ymm4, 170           # ymm4 = ymm3[0],ymm4[1],ymm3[2],ymm4[3],ymm3[4],ymm4[5],ymm3[6],ymm4[7]
	vpor	ymm4, ymm4, ymm6
	vpsrlq	ymm3, ymm3, 32
	vpor	ymm3, ymm3, ymm7
	vsubpd	ymm3, ymm3, ymm8
	vaddpd	ymm3, ymm4, ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_922:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_923:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI0_8] # xmm0 = [1127219200,1160773632,0,0]
	vmovapd	xmm1, xmmword ptr [rip + .LCPI0_9] # xmm1 = [4.503599627370496E+15,1.9342813113834067E+25]
.LBB0_924:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm2, qword ptr [rdx + 8*rsi]   # xmm2 = mem[0],zero
	vunpcklps	xmm2, xmm2, xmm0        # xmm2 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vsubpd	xmm2, xmm2, xmm1
	vpermilpd	xmm3, xmm2, 1           # xmm3 = xmm2[1,0]
	vaddsd	xmm2, xmm3, xmm2
	vmovsd	qword ptr [rcx + 8*rsi], xmm2
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_924
	jmp	.LBB0_1553
.LBB0_925:
	xor	edi, edi
.LBB0_926:
	test	r8b, 1
	je	.LBB0_928
# %bb.927:
	vcvtdq2pd	ymm0, xmmword ptr [rdx + 4*rdi]
	vcvtdq2pd	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vcvtdq2pd	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vcvtdq2pd	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_928:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_929:                              # =>This Inner Loop Header: Depth=1
	vcvtsi2sd	xmm0, xmm4, dword ptr [rdx + 4*rsi]
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_929
	jmp	.LBB0_1553
.LBB0_930:
	xor	edi, edi
.LBB0_931:
	test	r8b, 1
	je	.LBB0_933
# %bb.932:
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_933:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_934:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_934
	jmp	.LBB0_1553
.LBB0_935:
	xor	edi, edi
.LBB0_936:
	test	r8b, 1
	je	.LBB0_938
# %bb.937:
	vpmovzxwq	ymm0, qword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdx + 2*rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdx + 2*rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdx + 2*rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_938:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_939:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_939
	jmp	.LBB0_1553
.LBB0_940:
	xor	edi, edi
.LBB0_941:
	test	r8b, 1
	je	.LBB0_943
# %bb.942:
	vpmovsxwq	ymm0, qword ptr [rdx + 2*rdi]
	vpmovsxwq	ymm1, qword ptr [rdx + 2*rdi + 8]
	vpmovsxwq	ymm2, qword ptr [rdx + 2*rdi + 16]
	vpmovsxwq	ymm3, qword ptr [rdx + 2*rdi + 24]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_943:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_944:                              # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_944
	jmp	.LBB0_1553
.LBB0_945:
	xor	edi, edi
.LBB0_946:
	test	r8b, 1
	je	.LBB0_948
# %bb.947:
	vpmovsxdq	ymm0, xmmword ptr [rdx + 4*rdi]
	vpmovsxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_948:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_949:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_949
	jmp	.LBB0_1553
.LBB0_950:
	xor	edi, edi
.LBB0_951:
	test	r8b, 1
	je	.LBB0_953
# %bb.952:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
.LBB0_953:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_954:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_954
	jmp	.LBB0_1553
.LBB0_955:
	xor	edi, edi
.LBB0_956:
	test	r8b, 1
	je	.LBB0_958
# %bb.957:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
.LBB0_958:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_959:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_959
	jmp	.LBB0_1553
.LBB0_960:
	xor	edi, edi
.LBB0_961:
	test	r8b, 1
	je	.LBB0_963
# %bb.962:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
.LBB0_963:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_964:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_964
	jmp	.LBB0_1553
.LBB0_965:
	xor	edi, edi
.LBB0_966:
	test	r8b, 1
	je	.LBB0_968
# %bb.967:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_11] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
.LBB0_968:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_969:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_969
	jmp	.LBB0_1553
.LBB0_970:
	xor	edi, edi
.LBB0_971:
	test	r8b, 1
	je	.LBB0_973
# %bb.972:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_11] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
.LBB0_973:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_974:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_974
	jmp	.LBB0_1553
.LBB0_975:
	xor	edi, edi
.LBB0_976:
	test	r8b, 1
	je	.LBB0_978
# %bb.977:
	vpmovzxwq	ymm0, qword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm1, qword ptr [rdx + 2*rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm2, qword ptr [rdx + 2*rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxwq	ymm3, qword ptr [rdx + 2*rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_978:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_979:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_979
	jmp	.LBB0_1553
.LBB0_980:
	xor	edi, edi
.LBB0_981:
	test	r8b, 1
	je	.LBB0_983
# %bb.982:
	vpmovsxdq	ymm0, xmmword ptr [rdx + 4*rdi]
	vpmovsxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vpmovsxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vpmovsxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_983:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_984:                              # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_984
	jmp	.LBB0_1553
.LBB0_985:
	xor	edi, edi
.LBB0_986:
	test	r8b, 1
	je	.LBB0_988
# %bb.987:
	vcvtdq2ps	ymm0, ymmword ptr [rdx + 4*rdi]
	vcvtdq2ps	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vcvtdq2ps	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vcvtdq2ps	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_988:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_989:                              # =>This Inner Loop Header: Depth=1
	vcvtsi2ss	xmm0, xmm4, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_989
	jmp	.LBB0_1553
.LBB0_990:
	xor	edi, edi
.LBB0_991:
	test	r8b, 1
	je	.LBB0_993
# %bb.992:
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	xmmword ptr [rcx + 4*rdi], xmm0
	vmovupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovupd	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovupd	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_993:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_994:                              # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_994
	jmp	.LBB0_1553
.LBB0_995:
	xor	edi, edi
.LBB0_996:
	test	r8b, 1
	je	.LBB0_998
# %bb.997:
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_998:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_999:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_999
	jmp	.LBB0_1553
.LBB0_1000:
	xor	edi, edi
.LBB0_1001:
	test	r8b, 1
	je	.LBB0_1003
# %bb.1002:
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1003:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1004:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1004
	jmp	.LBB0_1553
.LBB0_1005:
	xor	edi, edi
.LBB0_1006:
	test	r8b, 1
	je	.LBB0_1008
# %bb.1007:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1008:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1009:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1009
	jmp	.LBB0_1553
.LBB0_1010:
	xor	edi, edi
.LBB0_1011:
	test	r8b, 1
	je	.LBB0_1013
# %bb.1012:
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_1013:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1014:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1014
	jmp	.LBB0_1553
.LBB0_1015:
	xor	edi, edi
.LBB0_1016:
	test	r8b, 1
	je	.LBB0_1018
# %bb.1017:
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1018:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1019:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1019
	jmp	.LBB0_1553
.LBB0_1020:
	xor	edi, edi
.LBB0_1021:
	test	r8b, 1
	je	.LBB0_1023
# %bb.1022:
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1023:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1024:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1024
	jmp	.LBB0_1553
.LBB0_1025:
	xor	edi, edi
.LBB0_1026:
	test	r8b, 1
	je	.LBB0_1028
# %bb.1027:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1028:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1029:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1029
	jmp	.LBB0_1553
.LBB0_1030:
	xor	edi, edi
.LBB0_1031:
	test	r8b, 1
	je	.LBB0_1033
# %bb.1032:
	vmovups	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovups	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	vmovups	xmm2, xmmword ptr [rdx + 8*rdi + 64]
	vmovups	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vshufps	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 16], 136 # xmm0 = xmm0[0,2],mem[0,2]
	vshufps	xmm1, xmm1, xmmword ptr [rdx + 8*rdi + 48], 136 # xmm1 = xmm1[0,2],mem[0,2]
	vshufps	xmm2, xmm2, xmmword ptr [rdx + 8*rdi + 80], 136 # xmm2 = xmm2[0,2],mem[0,2]
	vshufps	xmm3, xmm3, xmmword ptr [rdx + 8*rdi + 112], 136 # xmm3 = xmm3[0,2],mem[0,2]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_1033:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1034:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 8*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1034
	jmp	.LBB0_1553
.LBB0_1035:
	xor	edi, edi
.LBB0_1036:
	test	r8b, 1
	je	.LBB0_1038
# %bb.1037:
	vmovups	xmm0, xmmword ptr [rdx + 4*rdi]
	vbroadcastss	xmm1, dword ptr [rip + .LCPI0_2] # xmm1 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	vcmpltps	xmm2, xmm0, xmm1
	vsubps	xmm3, xmm0, xmm1
	vcvttps2dq	xmm3, xmm3
	vbroadcastss	xmm4, dword ptr [rip + .LCPI0_3] # xmm4 = [2147483648,2147483648,2147483648,2147483648]
	vxorps	xmm3, xmm3, xmm4
	vcvttps2dq	xmm0, xmm0
	vblendvps	xmm0, xmm3, xmm0, xmm2
	vmovups	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vcmpltps	xmm3, xmm2, xmm1
	vsubps	xmm5, xmm2, xmm1
	vcvttps2dq	xmm5, xmm5
	vxorps	xmm5, xmm5, xmm4
	vcvttps2dq	xmm2, xmm2
	vblendvps	xmm2, xmm5, xmm2, xmm3
	vmovups	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vcmpltps	xmm5, xmm3, xmm1
	vsubps	xmm6, xmm3, xmm1
	vcvttps2dq	xmm6, xmm6
	vxorps	xmm6, xmm6, xmm4
	vcvttps2dq	xmm3, xmm3
	vblendvps	xmm3, xmm6, xmm3, xmm5
	vmovups	xmm5, xmmword ptr [rdx + 4*rdi + 48]
	vcmpltps	xmm6, xmm5, xmm1
	vsubps	xmm1, xmm5, xmm1
	vcvttps2dq	xmm1, xmm1
	vxorps	xmm1, xmm1, xmm4
	vcvttps2dq	xmm4, xmm5
	vblendvps	xmm1, xmm1, xmm4, xmm6
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm3
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm1
.LBB0_1038:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1039:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	rax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1039
	jmp	.LBB0_1553
.LBB0_1040:
	xor	edi, edi
.LBB0_1041:
	test	r8b, 1
	je	.LBB0_1043
# %bb.1042:
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpbroadcastq	ymm4, qword ptr [rip + .LCPI0_5] # ymm4 = [4841369599423283200,4841369599423283200,4841369599423283200,4841369599423283200]
	vpor	ymm0, ymm0, ymm4
	vsubpd	ymm0, ymm0, ymm4
	vpor	ymm1, ymm1, ymm4
	vsubpd	ymm1, ymm1, ymm4
	vpor	ymm2, ymm2, ymm4
	vsubpd	ymm2, ymm2, ymm4
	vpor	ymm3, ymm3, ymm4
	vsubpd	ymm3, ymm3, ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1043:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1044:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	vcvtsi2sd	xmm0, xmm5, rax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1044
	jmp	.LBB0_1553
.LBB0_1045:
	xor	edi, edi
.LBB0_1046:
	test	r8b, 1
	je	.LBB0_1048
# %bb.1047:
	vpmovzxwd	xmm0, qword ptr [rdx + 2*rdi] # xmm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm1, qword ptr [rdx + 2*rdi + 8] # xmm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm2, qword ptr [rdx + 2*rdi + 16] # xmm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxwd	xmm3, qword ptr [rdx + 2*rdi + 24] # xmm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1048:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1049:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1049
	jmp	.LBB0_1553
.LBB0_1050:
	xor	edi, edi
.LBB0_1051:
	test	r8b, 1
	je	.LBB0_1053
# %bb.1052:
	vpmovsxwd	xmm0, qword ptr [rdx + 2*rdi]
	vpmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	vpmovsxwd	xmm2, qword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	xmm3, qword ptr [rdx + 2*rdi + 24]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1053:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1054:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	vcvtsi2sd	xmm0, xmm4, eax
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1054
	jmp	.LBB0_1553
.LBB0_1055:
	xor	edi, edi
.LBB0_1056:
	test	r8b, 1
	je	.LBB0_1058
# %bb.1057:
	vmovdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	vpextrq	rax, xmm0, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm0
	vcvtsi2sd	xmm0, xmm11, rax
	vunpcklpd	xmm8, xmm0, xmm4        # xmm8 = xmm0[0],xmm4[0]
	vpextrq	rax, xmm1, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm1
	vcvtsi2sd	xmm1, xmm11, rax
	vunpcklpd	xmm1, xmm1, xmm4        # xmm1 = xmm1[0],xmm4[0]
	vpextrq	rax, xmm2, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm2
	vcvtsi2sd	xmm2, xmm11, rax
	vunpcklpd	xmm2, xmm2, xmm4        # xmm2 = xmm2[0],xmm4[0]
	vpextrq	rax, xmm3, 1
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm3
	vcvtsi2sd	xmm3, xmm11, rax
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 80]
	vpextrq	rax, xmm5, 1
	vcvtsi2sd	xmm6, xmm11, rax
	vmovq	rax, xmm5
	vcvtsi2sd	xmm5, xmm11, rax
	vmovdqu	xmm7, xmmword ptr [rdx + 8*rdi + 64]
	vpextrq	rax, xmm7, 1
	vcvtsi2sd	xmm0, xmm11, rax
	vunpcklpd	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0]
	vmovq	rax, xmm7
	vcvtsi2sd	xmm4, xmm11, rax
	vunpcklpd	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vmovdqu	xmm6, xmmword ptr [rdx + 8*rdi + 112]
	vpextrq	rax, xmm6, 1
	vunpcklpd	xmm0, xmm4, xmm0        # xmm0 = xmm4[0],xmm0[0]
	vcvtsi2sd	xmm4, xmm11, rax
	vmovq	rax, xmm6
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm4, xmm6, xmm4        # xmm4 = xmm6[0],xmm4[0]
	vmovdqu	xmm6, xmmword ptr [rdx + 8*rdi + 96]
	vpextrq	rax, xmm6, 1
	vcvtsi2sd	xmm7, xmm11, rax
	vmovq	rax, xmm6
	vcvtsi2sd	xmm6, xmm11, rax
	vunpcklpd	xmm6, xmm6, xmm7        # xmm6 = xmm6[0],xmm7[0]
	vmovupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
	vmovupd	xmmword ptr [rcx + 8*rdi], xmm8
	vmovupd	xmmword ptr [rcx + 8*rdi + 32], xmm3
	vmovupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	vmovupd	xmmword ptr [rcx + 8*rdi + 64], xmm0
	vmovupd	xmmword ptr [rcx + 8*rdi + 80], xmm5
	vmovupd	xmmword ptr [rcx + 8*rdi + 96], xmm6
	vmovupd	xmmword ptr [rcx + 8*rdi + 112], xmm4
.LBB0_1058:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1059:                             # =>This Inner Loop Header: Depth=1
	vcvtsi2sd	xmm0, xmm11, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1059
	jmp	.LBB0_1553
.LBB0_1060:
	xor	edi, edi
.LBB0_1061:
	test	r8b, 1
	je	.LBB0_1063
# %bb.1062:
	vcvtps2pd	ymm0, xmmword ptr [rdx + 4*rdi]
	vcvtps2pd	ymm1, xmmword ptr [rdx + 4*rdi + 16]
	vcvtps2pd	ymm2, xmmword ptr [rdx + 4*rdi + 32]
	vcvtps2pd	ymm3, xmmword ptr [rdx + 4*rdi + 48]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1063:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1064:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vcvtss2sd	xmm0, xmm0, xmm0
	vmovsd	qword ptr [rcx + 8*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1064
	jmp	.LBB0_1553
.LBB0_1065:
	xor	edi, edi
.LBB0_1066:
	test	r8b, 1
	je	.LBB0_1068
# %bb.1067:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_11] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
.LBB0_1068:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1069:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1069
	jmp	.LBB0_1553
.LBB0_1070:
	xor	edi, edi
.LBB0_1071:
	test	r8b, 1
	je	.LBB0_1073
# %bb.1072:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_11] # ymm4 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm4
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm4
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vpshufb	ymm2, ymm2, ymm4
	vpermq	ymm2, ymm2, 232                 # ymm2 = ymm2[0,2,2,3]
	vpshufb	ymm3, ymm3, ymm4
	vpermq	ymm3, ymm3, 232                 # ymm3 = ymm3[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
.LBB0_1073:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1074:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1074
	jmp	.LBB0_1553
.LBB0_1075:
	xor	edi, edi
.LBB0_1076:
	test	r8b, 1
	je	.LBB0_1078
# %bb.1077:
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackusdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackusdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
.LBB0_1078:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1079:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1079
	jmp	.LBB0_1553
.LBB0_1080:
	xor	edi, edi
.LBB0_1081:
	test	r8b, 1
	je	.LBB0_1083
# %bb.1082:
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvttpd2dq	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vinsertf128	ymm2, ymm2, xmm3, 1
	vpackssdw	ymm2, ymm2, ymm0
	vinsertf128	ymm0, ymm0, xmm1, 1
	vpackssdw	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
.LBB0_1083:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1084:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	eax, qword ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1084
	jmp	.LBB0_1553
.LBB0_1085:
	xor	edi, edi
.LBB0_1086:
	test	r8b, 1
	je	.LBB0_1088
# %bb.1087:
	vpxor	xmm0, xmm0, xmm0
	vpblendw	xmm1, xmm0, xmmword ptr [rdx + 8*rdi], 17 # xmm1 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm2, xmm0, xmmword ptr [rdx + 8*rdi + 16], 17 # xmm2 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm3, xmm0, xmmword ptr [rdx + 8*rdi + 32], 17 # xmm3 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm4, xmm0, xmmword ptr [rdx + 8*rdi + 48], 17 # xmm4 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm5, xmm0, xmmword ptr [rdx + 8*rdi + 64], 17 # xmm5 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm6, xmm0, xmmword ptr [rdx + 8*rdi + 80], 17 # xmm6 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm7, xmm0, xmmword ptr [rdx + 8*rdi + 96], 17 # xmm7 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vpblendw	xmm0, xmm0, xmmword ptr [rdx + 8*rdi + 112], 17 # xmm0 = mem[0],xmm0[1,2,3],mem[4],xmm0[5,6,7]
	vinserti128	ymm0, ymm6, xmm0, 1
	vinserti128	ymm5, ymm5, xmm7, 1
	vpackusdw	ymm0, ymm5, ymm0
	vpackusdw	ymm0, ymm0, ymm0
	vinserti128	ymm2, ymm2, xmm4, 1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpackusdw	ymm1, ymm1, ymm2
	vpackusdw	ymm1, ymm1, ymm0
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
.LBB0_1088:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1089:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 8*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1089
	jmp	.LBB0_1553
.LBB0_1090:
	xor	edi, edi
.LBB0_1091:
	test	r8b, 1
	je	.LBB0_1093
# %bb.1092:
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vpackusdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vextracti128	xmm2, ymm1, 1
	vpackusdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vextracti128	xmm3, ymm2, 1
	vpackusdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vextracti128	xmm4, ymm3, 1
	vpackusdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
.LBB0_1093:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1094:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1094
	jmp	.LBB0_1553
.LBB0_1095:
	xor	edi, edi
.LBB0_1096:
	test	r8b, 1
	je	.LBB0_1098
# %bb.1097:
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
.LBB0_1098:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1099:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	eax, dword ptr [rdx + 4*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1099
	jmp	.LBB0_1553
.LBB0_1100:
	xor	edi, edi
.LBB0_1101:
	test	r8b, 1
	je	.LBB0_1103
# %bb.1102:
	vpmovzxdq	ymm0, xmmword ptr [rdx + 4*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm1, xmmword ptr [rdx + 4*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm2, xmmword ptr [rdx + 4*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vpmovzxdq	ymm3, xmmword ptr [rdx + 4*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1103:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1104:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1104
	jmp	.LBB0_1553
.LBB0_1105:
	xor	edi, edi
.LBB0_1106:
	test	r8b, 1
	je	.LBB0_1108
# %bb.1107:
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpbroadcastd	ymm3, dword ptr [rip + .LCPI0_13] # ymm3 = [1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200,1258291200]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 96]
	vpblendw	ymm5, ymm0, ymm3, 170           # ymm5 = ymm0[0],ymm3[1],ymm0[2],ymm3[3],ymm0[4],ymm3[5],ymm0[6],ymm3[7],ymm0[8],ymm3[9],ymm0[10],ymm3[11],ymm0[12],ymm3[13],ymm0[14],ymm3[15]
	vpbroadcastd	ymm6, dword ptr [rip + .LCPI0_14] # ymm6 = [1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928,1392508928]
	vpsrld	ymm0, ymm0, 16
	vpblendw	ymm0, ymm0, ymm6, 170           # ymm0 = ymm0[0],ymm6[1],ymm0[2],ymm6[3],ymm0[4],ymm6[5],ymm0[6],ymm6[7],ymm0[8],ymm6[9],ymm0[10],ymm6[11],ymm0[12],ymm6[13],ymm0[14],ymm6[15]
	vbroadcastss	ymm7, dword ptr [rip + .LCPI0_15] # ymm7 = [5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11,5.49764202E+11]
	vsubps	ymm0, ymm0, ymm7
	vaddps	ymm0, ymm5, ymm0
	vpblendw	ymm5, ymm1, ymm3, 170           # ymm5 = ymm1[0],ymm3[1],ymm1[2],ymm3[3],ymm1[4],ymm3[5],ymm1[6],ymm3[7],ymm1[8],ymm3[9],ymm1[10],ymm3[11],ymm1[12],ymm3[13],ymm1[14],ymm3[15]
	vpsrld	ymm1, ymm1, 16
	vpblendw	ymm1, ymm1, ymm6, 170           # ymm1 = ymm1[0],ymm6[1],ymm1[2],ymm6[3],ymm1[4],ymm6[5],ymm1[6],ymm6[7],ymm1[8],ymm6[9],ymm1[10],ymm6[11],ymm1[12],ymm6[13],ymm1[14],ymm6[15]
	vsubps	ymm1, ymm1, ymm7
	vaddps	ymm1, ymm5, ymm1
	vpblendw	ymm5, ymm2, ymm3, 170           # ymm5 = ymm2[0],ymm3[1],ymm2[2],ymm3[3],ymm2[4],ymm3[5],ymm2[6],ymm3[7],ymm2[8],ymm3[9],ymm2[10],ymm3[11],ymm2[12],ymm3[13],ymm2[14],ymm3[15]
	vpsrld	ymm2, ymm2, 16
	vpblendw	ymm2, ymm2, ymm6, 170           # ymm2 = ymm2[0],ymm6[1],ymm2[2],ymm6[3],ymm2[4],ymm6[5],ymm2[6],ymm6[7],ymm2[8],ymm6[9],ymm2[10],ymm6[11],ymm2[12],ymm6[13],ymm2[14],ymm6[15]
	vsubps	ymm2, ymm2, ymm7
	vaddps	ymm2, ymm5, ymm2
	vpblendw	ymm3, ymm4, ymm3, 170           # ymm3 = ymm4[0],ymm3[1],ymm4[2],ymm3[3],ymm4[4],ymm3[5],ymm4[6],ymm3[7],ymm4[8],ymm3[9],ymm4[10],ymm3[11],ymm4[12],ymm3[13],ymm4[14],ymm3[15]
	vpsrld	ymm4, ymm4, 16
	vpblendw	ymm4, ymm4, ymm6, 170           # ymm4 = ymm4[0],ymm6[1],ymm4[2],ymm6[3],ymm4[4],ymm6[5],ymm4[6],ymm6[7],ymm4[8],ymm6[9],ymm4[10],ymm6[11],ymm4[12],ymm6[13],ymm4[14],ymm6[15]
	vsubps	ymm4, ymm4, ymm7
	vaddps	ymm3, ymm3, ymm4
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1108:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1109:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	vcvtsi2ss	xmm0, xmm8, rax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1109
	jmp	.LBB0_1553
.LBB0_1110:
	xor	edi, edi
.LBB0_1111:
	test	r8b, 1
	je	.LBB0_1113
# %bb.1112:
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 8]
	vmovq	xmm0, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi]
	vmovq	xmm1, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 24]
	vmovq	xmm1, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 16]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 56]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 48]
	vmovq	xmm2, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 40]
	vmovq	xmm3, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 32]
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovq	xmm3, rax
	vmovq	xmm4, rbx
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 88]
	vmovq	xmm4, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 80]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 72]
	vmovq	xmm5, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 64]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 120]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 112]
	vmovq	xmm6, rax
	vcvttsd2si	rax, qword ptr [rdx + 8*rdi + 104]
	vmovq	xmm7, rbx
	vcvttsd2si	rbx, qword ptr [rdx + 8*rdi + 96]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vmovdqu	xmmword ptr [rcx + 8*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 8*rdi], xmm8
	vmovdqu	xmmword ptr [rcx + 8*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 8*rdi + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rdi + 64], xmm5
	vmovdqu	xmmword ptr [rcx + 8*rdi + 80], xmm4
	vmovdqu	xmmword ptr [rcx + 8*rdi + 96], xmm0
	vmovdqu	xmmword ptr [rcx + 8*rdi + 112], xmm6
.LBB0_1113:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1114:                             # =>This Inner Loop Header: Depth=1
	vcvttsd2si	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1114
	jmp	.LBB0_1553
.LBB0_1115:
	xor	edi, edi
.LBB0_1116:
	test	r8b, 1
	je	.LBB0_1118
# %bb.1117:
	vcvtpd2ps	xmm0, ymmword ptr [rdx + 8*rdi]
	vcvtpd2ps	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vcvtpd2ps	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vcvtpd2ps	xmm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	xmmword ptr [rcx + 4*rdi], xmm0
	vmovupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovupd	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovupd	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_1118:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1119:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vcvtsd2ss	xmm0, xmm0, xmm0
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1119
	jmp	.LBB0_1553
.LBB0_1120:
	xor	edi, edi
.LBB0_1121:
	test	r8b, 1
	je	.LBB0_1123
# %bb.1122:
	vpmovzxwd	ymm0, xmmword ptr [rdx + 2*rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vpmovzxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1123:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1124:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1124
	jmp	.LBB0_1553
.LBB0_1125:
	xor	edi, edi
.LBB0_1126:
	test	r8b, 1
	je	.LBB0_1128
# %bb.1127:
	vpmovsxwq	ymm0, qword ptr [rdx + 2*rdi]
	vpmovsxwq	ymm1, qword ptr [rdx + 2*rdi + 8]
	vpmovsxwq	ymm2, qword ptr [rdx + 2*rdi + 16]
	vpmovsxwq	ymm3, qword ptr [rdx + 2*rdi + 24]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1128:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1129:                             # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdx + 2*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1129
	jmp	.LBB0_1553
.LBB0_1130:
	xor	edi, edi
.LBB0_1131:
	test	r8b, 1
	je	.LBB0_1133
# %bb.1132:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm3, xmmword ptr [rdx + 2*rdi + 48]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1133:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1134:                             # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	vcvtsi2ss	xmm0, xmm4, eax
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1134
	jmp	.LBB0_1553
.LBB0_1135:
	xor	edi, edi
.LBB0_1136:
	test	r8b, 1
	je	.LBB0_1138
# %bb.1137:
	vmovdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	vpextrq	rax, xmm0, 1
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm0
	vcvtsi2ss	xmm0, xmm8, rax
	vmovq	rax, xmm1
	vcvtsi2ss	xmm3, xmm8, rax
	vpextrq	rax, xmm1, 1
	vcvtsi2ss	xmm1, xmm8, rax
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 48]
	vpextrq	rax, xmm4, 1
	vinsertps	xmm0, xmm0, xmm2, 16    # xmm0 = xmm0[0],xmm2[0],xmm0[2,3]
	vcvtsi2ss	xmm2, xmm8, rax
	vmovq	rax, xmm4
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm0, xmm0, xmm3, 32    # xmm0 = xmm0[0,1],xmm3[0],xmm0[3]
	vinsertps	xmm0, xmm0, xmm1, 48    # xmm0 = xmm0[0,1,2],xmm1[0]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm1, xmm4, xmm2, 16    # xmm1 = xmm4[0],xmm2[0],xmm4[2,3]
	vcvtsi2ss	xmm2, xmm8, rax
	vinsertps	xmm1, xmm1, xmm6, 32    # xmm1 = xmm1[0,1],xmm6[0],xmm1[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpextrq	rax, xmm3, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 80]
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm1, xmm1, xmm2, 48    # xmm1 = xmm1[0,1,2],xmm2[0]
	vinsertps	xmm2, xmm3, xmm4, 16    # xmm2 = xmm3[0],xmm4[0],xmm3[2,3]
	vpextrq	rax, xmm5, 1
	vinsertps	xmm2, xmm2, xmm6, 32    # xmm2 = xmm2[0,1],xmm6[0],xmm2[3]
	vcvtsi2ss	xmm3, xmm8, rax
	vinsertps	xmm2, xmm2, xmm3, 48    # xmm2 = xmm2[0,1,2],xmm3[0]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 96]
	vpextrq	rax, xmm3, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vmovq	rax, xmm3
	vcvtsi2ss	xmm3, xmm8, rax
	vmovdqu	xmm5, xmmword ptr [rdx + 8*rdi + 112]
	vmovq	rax, xmm5
	vcvtsi2ss	xmm6, xmm8, rax
	vinsertps	xmm3, xmm3, xmm4, 16    # xmm3 = xmm3[0],xmm4[0],xmm3[2,3]
	vinsertps	xmm3, xmm3, xmm6, 32    # xmm3 = xmm3[0,1],xmm6[0],xmm3[3]
	vpextrq	rax, xmm5, 1
	vcvtsi2ss	xmm4, xmm8, rax
	vinsertps	xmm3, xmm3, xmm4, 48    # xmm3 = xmm3[0,1,2],xmm4[0]
	vmovups	xmmword ptr [rcx + 4*rdi], xmm0
	vmovups	xmmword ptr [rcx + 4*rdi + 16], xmm1
	vmovups	xmmword ptr [rcx + 4*rdi + 32], xmm2
	vmovups	xmmword ptr [rcx + 4*rdi + 48], xmm3
.LBB0_1138:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1139:                             # =>This Inner Loop Header: Depth=1
	vcvtsi2ss	xmm0, xmm8, qword ptr [rdx + 8*rsi]
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1139
	jmp	.LBB0_1553
.LBB0_1140:
	xor	edi, edi
.LBB0_1141:
	test	r8b, 1
	je	.LBB0_1143
# %bb.1142:
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 4]
	vmovq	xmm0, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi]
	vmovq	xmm1, rax
	vpunpcklqdq	xmm8, xmm1, xmm0        # xmm8 = xmm1[0],xmm0[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 12]
	vmovq	xmm1, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 8]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 28]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 24]
	vmovq	xmm2, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 20]
	vmovq	xmm3, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 16]
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vmovq	xmm3, rax
	vmovq	xmm4, rbx
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 44]
	vmovq	xmm4, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 40]
	vmovq	xmm5, rax
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 36]
	vmovq	xmm5, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 32]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 60]
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 56]
	vmovq	xmm6, rax
	vcvttss2si	rax, dword ptr [rdx + 4*rdi + 52]
	vmovq	xmm7, rbx
	vcvttss2si	rbx, dword ptr [rdx + 4*rdi + 48]
	vmovq	xmm0, rax
	vpunpcklqdq	xmm6, xmm7, xmm6        # xmm6 = xmm7[0],xmm6[0]
	vmovq	xmm7, rbx
	vpunpcklqdq	xmm0, xmm7, xmm0        # xmm0 = xmm7[0],xmm0[0]
	vmovdqu	xmmword ptr [rcx + 8*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 8*rdi], xmm8
	vmovdqu	xmmword ptr [rcx + 8*rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + 8*rdi + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 8*rdi + 64], xmm5
	vmovdqu	xmmword ptr [rcx + 8*rdi + 80], xmm4
	vmovdqu	xmmword ptr [rcx + 8*rdi + 96], xmm0
	vmovdqu	xmmword ptr [rcx + 8*rdi + 112], xmm6
.LBB0_1143:
	cmp	rsi, r9
	je	.LBB0_1553
.LBB0_1144:                             # =>This Inner Loop Header: Depth=1
	vcvttss2si	rax, dword ptr [rdx + 4*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	cmp	r9, rsi
	jne	.LBB0_1144
.LBB0_1553:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	vzeroupper
	ret
.LBB0_1145:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1146:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [rcx + 2*rax], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [rcx + 2*rax + 64], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [rcx + 2*rax + 128], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 160], ymm1
	vmovupd	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovupd	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovupd	ymmword ptr [rcx + 2*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1146
.LBB0_1147:
	test	r8, r8
	je	.LBB0_1150
# %bb.1148:
	add	rax, rax
	add	rax, 32
	neg	r8
.LBB0_1149:                             # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + rax - 32]
	vmovupd	ymm1, ymmword ptr [rdx + rax]
	vmovupd	ymmword ptr [rcx + rax - 32], ymm0
	vmovupd	ymmword ptr [rcx + rax], ymm1
	add	rax, 64
	inc	r8
	jne	.LBB0_1149
.LBB0_1150:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1151
.LBB0_1155:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1156:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [rcx + 2*rax], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [rcx + 2*rax + 64], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [rcx + 2*rax + 128], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 160], ymm1
	vmovupd	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovupd	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovupd	ymmword ptr [rcx + 2*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1156
.LBB0_1157:
	test	r8, r8
	je	.LBB0_1160
# %bb.1158:
	add	rax, rax
	add	rax, 32
	neg	r8
.LBB0_1159:                             # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + rax - 32]
	vmovupd	ymm1, ymmword ptr [rdx + rax]
	vmovupd	ymmword ptr [rcx + rax - 32], ymm0
	vmovupd	ymmword ptr [rcx + rax], ymm1
	add	rax, 64
	inc	r8
	jne	.LBB0_1159
.LBB0_1160:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1161
.LBB0_1165:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1166:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [rcx + 2*rax], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [rcx + 2*rax + 64], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [rcx + 2*rax + 128], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 160], ymm1
	vmovupd	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovupd	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovupd	ymmword ptr [rcx + 2*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1166
.LBB0_1167:
	test	r8, r8
	je	.LBB0_1170
# %bb.1168:
	add	rax, rax
	add	rax, 32
	neg	r8
.LBB0_1169:                             # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + rax - 32]
	vmovupd	ymm1, ymmword ptr [rdx + rax]
	vmovupd	ymmword ptr [rcx + rax - 32], ymm0
	vmovupd	ymmword ptr [rcx + rax], ymm1
	add	rax, 64
	inc	r8
	jne	.LBB0_1169
.LBB0_1170:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1171
.LBB0_1175:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1176:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovups	ymmword ptr [rcx + 2*rax], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 32], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 64]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 96]
	vmovups	ymmword ptr [rcx + 2*rax + 64], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 96], ymm1
	vmovups	ymm0, ymmword ptr [rdx + 2*rax + 128]
	vmovups	ymm1, ymmword ptr [rdx + 2*rax + 160]
	vmovups	ymmword ptr [rcx + 2*rax + 128], ymm0
	vmovups	ymmword ptr [rcx + 2*rax + 160], ymm1
	vmovupd	ymm0, ymmword ptr [rdx + 2*rax + 192]
	vmovupd	ymm1, ymmword ptr [rdx + 2*rax + 224]
	vmovupd	ymmword ptr [rcx + 2*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 224], ymm1
	sub	rax, -128
	add	rdi, 4
	jne	.LBB0_1176
.LBB0_1177:
	test	r8, r8
	je	.LBB0_1180
# %bb.1178:
	add	rax, rax
	add	rax, 32
	neg	r8
.LBB0_1179:                             # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + rax - 32]
	vmovupd	ymm1, ymmword ptr [rdx + rax]
	vmovupd	ymmword ptr [rcx + rax - 32], ymm0
	vmovupd	ymmword ptr [rcx + rax], ymm1
	add	rax, 64
	inc	r8
	jne	.LBB0_1179
.LBB0_1180:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1181
.LBB0_1185:
	xor	edi, edi
.LBB0_1186:
	test	r8b, 1
	je	.LBB0_1188
# %bb.1187:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1188:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1189
.LBB0_1193:
	xor	edi, edi
.LBB0_1194:
	test	r8b, 1
	je	.LBB0_1196
# %bb.1195:
	vpmovsxbd	ymm0, qword ptr [rdx + rdi]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 24]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1196:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1197
.LBB0_1201:
	xor	edi, edi
.LBB0_1202:
	test	r8b, 1
	je	.LBB0_1204
# %bb.1203:
	vpmovzxbd	ymm0, qword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1204:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1205
.LBB0_1209:
	xor	edi, edi
.LBB0_1210:
	test	r8b, 1
	je	.LBB0_1212
# %bb.1211:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1212:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1213
.LBB0_1217:
	xor	edi, edi
.LBB0_1218:
	test	r8b, 1
	je	.LBB0_1220
# %bb.1219:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1220:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1221
.LBB0_1225:
	xor	edi, edi
.LBB0_1226:
	test	r8b, 1
	je	.LBB0_1228
# %bb.1227:
	vpmovsxbd	xmm0, dword ptr [rdx + rdi]
	vpmovsxbd	xmm1, dword ptr [rdx + rdi + 4]
	vpmovsxbd	xmm2, dword ptr [rdx + rdi + 8]
	vpmovsxbd	xmm3, dword ptr [rdx + rdi + 12]
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1228:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1229
.LBB0_1233:
	xor	edi, edi
.LBB0_1234:
	test	r8b, 1
	je	.LBB0_1236
# %bb.1235:
	vpmovzxbd	xmm0, dword ptr [rdx + rdi] # xmm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm1, dword ptr [rdx + rdi + 4] # xmm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm2, dword ptr [rdx + rdi + 8] # xmm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vpmovzxbd	xmm3, dword ptr [rdx + rdi + 12] # xmm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero
	vcvtdq2pd	ymm0, xmm0
	vcvtdq2pd	ymm1, xmm1
	vcvtdq2pd	ymm2, xmm2
	vcvtdq2pd	ymm3, xmm3
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1236:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1237
.LBB0_1241:
	xor	edi, edi
.LBB0_1242:
	test	r8b, 1
	je	.LBB0_1244
# %bb.1243:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
.LBB0_1244:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1245
.LBB0_1249:
	xor	edi, edi
.LBB0_1250:
	test	r8b, 1
	je	.LBB0_1252
# %bb.1251:
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vpackssdw	xmm0, xmm0, xmm0
	vpacksswb	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vpackssdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vpacksswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vpackssdw	xmm1, xmm2, xmm2
	vpacksswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 96]
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1252:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1253
.LBB0_1257:
	xor	edi, edi
.LBB0_1258:
	test	r8b, 1
	je	.LBB0_1260
# %bb.1259:
	vmovupd	ymm0, ymmword ptr [rdx + rdi]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovupd	ymmword ptr [rcx + rdi], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 96], ymm3
.LBB0_1260:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1261
.LBB0_1265:
	xor	edi, edi
.LBB0_1266:
	test	r8b, 1
	je	.LBB0_1268
# %bb.1267:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1268:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1269
.LBB0_1273:
	xor	edi, edi
.LBB0_1274:
	test	r8b, 1
	je	.LBB0_1276
# %bb.1275:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm0
.LBB0_1276:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1277
.LBB0_1281:
	xor	edi, edi
.LBB0_1282:
	test	r8b, 1
	je	.LBB0_1284
# %bb.1283:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm0
.LBB0_1284:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1285
.LBB0_1289:
	xor	edi, edi
.LBB0_1290:
	test	r8b, 1
	je	.LBB0_1292
# %bb.1291:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1292:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1293
.LBB0_1297:
	xor	edi, edi
.LBB0_1298:
	test	r8b, 1
	je	.LBB0_1300
# %bb.1299:
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vinserti128	ymm2, ymm2, xmm3, 1
	vpacksswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpacksswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
.LBB0_1300:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1301
.LBB0_1305:
	xor	edi, edi
.LBB0_1306:
	test	r8b, 1
	je	.LBB0_1308
# %bb.1307:
	vmovupd	ymm0, ymmword ptr [rdx + rdi]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovupd	ymmword ptr [rcx + rdi], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 96], ymm3
.LBB0_1308:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1309
.LBB0_1313:
	xor	edi, edi
.LBB0_1314:
	test	r8b, 1
	je	.LBB0_1316
# %bb.1315:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
.LBB0_1316:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1317
.LBB0_1321:
	xor	edi, edi
.LBB0_1322:
	test	r8b, 1
	je	.LBB0_1324
# %bb.1323:
	vpmovsxbq	ymm0, dword ptr [rdx + rdi]
	vpmovsxbq	ymm1, dword ptr [rdx + rdi + 4]
	vpmovsxbq	ymm2, dword ptr [rdx + rdi + 8]
	vpmovsxbq	ymm3, dword ptr [rdx + rdi + 12]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1324:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1325
.LBB0_1329:
	xor	edi, edi
.LBB0_1330:
	test	r8b, 1
	je	.LBB0_1332
# %bb.1331:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1332:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1333
.LBB0_1337:
	xor	edi, edi
.LBB0_1338:
	test	r8b, 1
	je	.LBB0_1340
# %bb.1339:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1340:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1341
.LBB0_1345:
	xor	edi, edi
.LBB0_1346:
	test	r8b, 1
	je	.LBB0_1348
# %bb.1347:
	vpmovzxbq	ymm0, dword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdx + rdi + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdx + rdi + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdx + rdi + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1348:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1349
.LBB0_1353:
	xor	edi, edi
.LBB0_1354:
	test	r8b, 1
	je	.LBB0_1356
# %bb.1355:
	vpmovsxbw	ymm0, xmmword ptr [rdx + rdi]
	vpmovsxbw	ymm1, xmmword ptr [rdx + rdi + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdx + rdi + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdx + rdi + 48]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
.LBB0_1356:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1357
.LBB0_1361:
	xor	edi, edi
.LBB0_1362:
	test	r8b, 1
	je	.LBB0_1364
# %bb.1363:
	vpmovsxbw	ymm0, xmmword ptr [rdx + rdi]
	vpmovsxbw	ymm1, xmmword ptr [rdx + rdi + 16]
	vpmovsxbw	ymm2, xmmword ptr [rdx + rdi + 32]
	vpmovsxbw	ymm3, xmmword ptr [rdx + rdi + 48]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
.LBB0_1364:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1365
.LBB0_1369:
	xor	edi, edi
.LBB0_1370:
	test	r8b, 1
	je	.LBB0_1372
# %bb.1371:
	vpmovzxbw	ymm0, xmmword ptr [rdx + rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdx + rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdx + rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdx + rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
.LBB0_1372:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1373
.LBB0_1377:
	xor	edi, edi
.LBB0_1378:
	test	r8b, 1
	je	.LBB0_1380
# %bb.1379:
	vpmovzxbw	ymm0, xmmword ptr [rdx + rdi] # ymm0 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm1, xmmword ptr [rdx + rdi + 16] # ymm1 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm2, xmmword ptr [rdx + rdi + 32] # ymm2 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vpmovzxbw	ymm3, xmmword ptr [rdx + rdi + 48] # ymm3 = mem[0],zero,mem[1],zero,mem[2],zero,mem[3],zero,mem[4],zero,mem[5],zero,mem[6],zero,mem[7],zero,mem[8],zero,mem[9],zero,mem[10],zero,mem[11],zero,mem[12],zero,mem[13],zero,mem[14],zero,mem[15],zero
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
.LBB0_1380:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1381
.LBB0_1385:
	xor	edi, edi
.LBB0_1386:
	test	r8b, 1
	je	.LBB0_1388
# %bb.1387:
	vpmovsxbq	ymm0, dword ptr [rdx + rdi]
	vpmovsxbq	ymm1, dword ptr [rdx + rdi + 4]
	vpmovsxbq	ymm2, dword ptr [rdx + rdi + 8]
	vpmovsxbq	ymm3, dword ptr [rdx + rdi + 12]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1388:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1389
.LBB0_1393:
	xor	edi, edi
.LBB0_1394:
	test	r8b, 1
	je	.LBB0_1396
# %bb.1395:
	vpmovsxbd	ymm0, qword ptr [rdx + rdi]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 24]
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1396:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1397
.LBB0_1401:
	xor	edi, edi
.LBB0_1402:
	test	r8b, 1
	je	.LBB0_1404
# %bb.1403:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1404:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1405
.LBB0_1409:
	xor	edi, edi
.LBB0_1410:
	test	r8b, 1
	je	.LBB0_1412
# %bb.1411:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1412:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1413
.LBB0_1417:
	xor	edi, edi
.LBB0_1418:
	test	r8b, 1
	je	.LBB0_1420
# %bb.1419:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1420:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1421
.LBB0_1425:
	xor	edi, edi
.LBB0_1426:
	test	r8b, 1
	je	.LBB0_1428
# %bb.1427:
	vpmovzxbq	ymm0, dword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm1, dword ptr [rdx + rdi + 4] # ymm1 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm2, dword ptr [rdx + rdi + 8] # ymm2 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vpmovzxbq	ymm3, dword ptr [rdx + rdi + 12] # ymm3 = mem[0],zero,zero,zero,zero,zero,zero,zero,mem[1],zero,zero,zero,zero,zero,zero,zero,mem[2],zero,zero,zero,zero,zero,zero,zero,mem[3],zero,zero,zero,zero,zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB0_1428:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1429
.LBB0_1433:
	xor	edi, edi
.LBB0_1434:
	test	r8b, 1
	je	.LBB0_1436
# %bb.1435:
	vpmovzxbd	ymm0, qword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vcvtdq2ps	ymm0, ymm0
	vcvtdq2ps	ymm1, ymm1
	vcvtdq2ps	ymm2, ymm2
	vcvtdq2ps	ymm3, ymm3
	vmovups	ymmword ptr [rcx + 4*rdi], ymm0
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1436:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1437
.LBB0_1441:
	xor	edi, edi
.LBB0_1442:
	test	r8b, 1
	je	.LBB0_1444
# %bb.1443:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
.LBB0_1444:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1445
.LBB0_1449:
	xor	edi, edi
.LBB0_1450:
	test	r8b, 1
	je	.LBB0_1452
# %bb.1451:
	vcvttpd2dq	xmm0, ymmword ptr [rdx + 8*rdi]
	vpackusdw	xmm0, xmm0, xmm0
	vpackuswb	xmm0, xmm0, xmm0
	vcvttpd2dq	xmm1, ymmword ptr [rdx + 8*rdi + 32]
	vpackusdw	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 64]
	vpackuswb	xmm1, xmm1, xmm1
	vpunpckldq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0],xmm0[1],xmm1[1]
	vpackusdw	xmm1, xmm2, xmm2
	vpackuswb	xmm1, xmm1, xmm1
	vcvttpd2dq	xmm2, ymmword ptr [rdx + 8*rdi + 96]
	vpackusdw	xmm2, xmm2, xmm2
	vpackuswb	xmm2, xmm2, xmm2
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpunpcklqdq	xmm0, xmm0, xmm1        # xmm0 = xmm0[0],xmm1[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1452:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1453
.LBB0_1457:
	xor	edi, edi
.LBB0_1458:
	test	r8b, 1
	je	.LBB0_1460
# %bb.1459:
	vmovupd	ymm0, ymmword ptr [rdx + rdi]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovupd	ymmword ptr [rcx + rdi], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 96], ymm3
.LBB0_1460:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1461
.LBB0_1465:
	xor	edi, edi
.LBB0_1466:
	test	r8b, 1
	je	.LBB0_1468
# %bb.1467:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1468:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1469
.LBB0_1473:
	xor	edi, edi
.LBB0_1474:
	test	r8b, 1
	je	.LBB0_1476
# %bb.1475:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm0
.LBB0_1476:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1477
.LBB0_1481:
	xor	edi, edi
.LBB0_1482:
	test	r8b, 1
	je	.LBB0_1484
# %bb.1483:
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_16] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpand	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vextracti128	xmm2, ymm1, 1
	vpackuswb	xmm1, xmm1, xmm2
	vpand	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vextracti128	xmm3, ymm2, 1
	vpackuswb	xmm2, xmm2, xmm3
	vpand	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vextracti128	xmm4, ymm3, 1
	vpackuswb	xmm3, xmm3, xmm4
	vpand	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vextracti128	xmm4, ymm0, 1
	vpackuswb	xmm0, xmm0, xmm4
	vmovdqu	xmmword ptr [rcx + rdi], xmm1
	vmovdqu	xmmword ptr [rcx + rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + rdi + 32], xmm3
	vmovdqu	xmmword ptr [rcx + rdi + 48], xmm0
.LBB0_1484:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1485
.LBB0_1489:
	xor	edi, edi
.LBB0_1490:
	test	r8b, 1
	je	.LBB0_1492
# %bb.1491:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_4] # xmm0 = <0,8,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpcklwd	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1],xmm1[2],xmm2[2],xmm1[3],xmm2[3]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vmovdqu	xmm2, xmmword ptr [rdx + 8*rdi + 80]
	vpshufb	xmm2, xmm2, xmm0
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 64]
	vpshufb	xmm3, xmm3, xmm0
	vpunpcklwd	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1],xmm3[2],xmm2[2],xmm3[3],xmm2[3]
	vmovdqu	xmm3, xmmword ptr [rdx + 8*rdi + 112]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 8*rdi + 96]
	vpshufb	xmm0, xmm4, xmm0
	vpunpcklwd	xmm0, xmm0, xmm3        # xmm0 = xmm0[0],xmm3[0],xmm0[1],xmm3[1],xmm0[2],xmm3[2],xmm0[3],xmm3[3]
	vpunpckldq	xmm0, xmm2, xmm0        # xmm0 = xmm2[0],xmm0[0],xmm2[1],xmm0[1]
	vpunpcklqdq	xmm0, xmm1, xmm0        # xmm0 = xmm1[0],xmm0[0]
	vmovdqu	xmmword ptr [rcx + rdi], xmm0
.LBB0_1492:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1493
.LBB0_1497:
	xor	edi, edi
.LBB0_1498:
	test	r8b, 1
	je	.LBB0_1500
# %bb.1499:
	vcvttps2dq	ymm0, ymmword ptr [rdx + 4*rdi]
	vextracti128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vcvttps2dq	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vcvttps2dq	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vcvttps2dq	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vinserti128	ymm2, ymm2, xmm3, 1
	vpackuswb	ymm2, ymm2, ymm0
	vinserti128	ymm0, ymm0, xmm1, 1
	vpackuswb	ymm0, ymm0, ymm0
	vpunpcklqdq	ymm0, ymm0, ymm2        # ymm0 = ymm0[0],ymm2[0],ymm0[2],ymm2[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
.LBB0_1500:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1501
.LBB0_1505:
	xor	edi, edi
.LBB0_1506:
	test	r8b, 1
	je	.LBB0_1508
# %bb.1507:
	vmovupd	ymm0, ymmword ptr [rdx + rdi]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovupd	ymmword ptr [rcx + rdi], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 96], ymm3
.LBB0_1508:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1509
.LBB0_1513:
	xor	edi, edi
.LBB0_1514:
	test	r8b, 1
	je	.LBB0_1516
# %bb.1515:
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI0_12] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
	vmovdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	vmovdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 32]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 48]
	vpshufb	xmm2, xmm2, xmm0
	vpshufb	xmm1, xmm1, xmm0
	vpunpckldq	xmm1, xmm1, xmm2        # xmm1 = xmm1[0],xmm2[0],xmm1[1],xmm2[1]
	vpshufb	xmm2, xmm4, xmm0
	vpshufb	xmm3, xmm3, xmm0
	vpunpckldq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0],xmm3[1],xmm2[1]
	vmovdqu	xmm3, xmmword ptr [rdx + 4*rdi + 80]
	vpshufb	xmm3, xmm3, xmm0
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 64]
	vpshufb	xmm4, xmm4, xmm0
	vpunpckldq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0],xmm4[1],xmm3[1]
	vmovdqu	xmm4, xmmword ptr [rdx + 4*rdi + 112]
	vpshufb	xmm4, xmm4, xmm0
	vmovdqu	xmm5, xmmword ptr [rdx + 4*rdi + 96]
	vpshufb	xmm0, xmm5, xmm0
	vpunpckldq	xmm0, xmm0, xmm4        # xmm0 = xmm0[0],xmm4[0],xmm0[1],xmm4[1]
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [rcx + rdi], ymm0
.LBB0_1516:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1517
.LBB0_1521:
	xor	edi, edi
.LBB0_1522:
	test	r8b, 1
	je	.LBB0_1524
# %bb.1523:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1524:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1525
.LBB0_1529:
	xor	edi, edi
.LBB0_1530:
	test	r8b, 1
	je	.LBB0_1532
# %bb.1531:
	vpmovsxbd	ymm0, qword ptr [rdx + rdi]
	vpmovsxbd	ymm1, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 24]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1532:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1533
.LBB0_1537:
	xor	edi, edi
.LBB0_1538:
	test	r8b, 1
	je	.LBB0_1540
# %bb.1539:
	vpmovzxbd	ymm0, qword ptr [rdx + rdi] # ymm0 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm1, qword ptr [rdx + rdi + 8] # ymm1 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm2, qword ptr [rdx + rdi + 16] # ymm2 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vpmovzxbd	ymm3, qword ptr [rdx + rdi + 24] # ymm3 = mem[0],zero,zero,zero,mem[1],zero,zero,zero,mem[2],zero,zero,zero,mem[3],zero,zero,zero,mem[4],zero,zero,zero,mem[5],zero,zero,zero,mem[6],zero,zero,zero,mem[7],zero,zero,zero
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1540:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1541
.LBB0_1545:
	xor	edi, edi
.LBB0_1546:
	test	r8b, 1
	je	.LBB0_1548
# %bb.1547:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB0_1548:
	cmp	rsi, r9
	je	.LBB0_1553
	jmp	.LBB0_1549
.Lfunc_end0:
	.size	cast_type_numeric_avx2, .Lfunc_end0-cast_type_numeric_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
