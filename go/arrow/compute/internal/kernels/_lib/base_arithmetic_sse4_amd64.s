	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_sse4
.LCPI0_0:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI0_1:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI0_2:
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
.LCPI0_3:
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
	jne	.LBB0_1395
# %bb.4:
	cmp	edi, 6
	jg	.LBB0_99
# %bb.5:
	cmp	edi, 3
	jle	.LBB0_179
# %bb.6:
	cmp	edi, 4
	je	.LBB0_299
# %bb.7:
	cmp	edi, 5
	je	.LBB0_306
# %bb.8:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.9:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.10:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_579
# %bb.11:
	xor	esi, esi
.LBB0_12:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_14
.LBB0_13:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_13
.LBB0_14:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_16:
	cmp	sil, 6
	jg	.LBB0_41
# %bb.17:
	cmp	sil, 5
	je	.LBB0_63
# %bb.18:
	cmp	sil, 6
	jne	.LBB0_1395
# %bb.19:
	cmp	edi, 6
	jg	.LBB0_110
# %bb.20:
	cmp	edi, 3
	jle	.LBB0_188
# %bb.21:
	cmp	edi, 4
	je	.LBB0_313
# %bb.22:
	cmp	edi, 5
	je	.LBB0_320
# %bb.23:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.24:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.25:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_584
# %bb.26:
	xor	esi, esi
.LBB0_27:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_29
.LBB0_28:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_28
.LBB0_29:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_31:
	cmp	sil, 2
	je	.LBB0_75
# %bb.32:
	cmp	sil, 4
	jne	.LBB0_1395
# %bb.33:
	cmp	edi, 6
	jg	.LBB0_121
# %bb.34:
	cmp	edi, 3
	jle	.LBB0_197
# %bb.35:
	cmp	edi, 4
	je	.LBB0_327
# %bb.36:
	cmp	edi, 5
	je	.LBB0_330
# %bb.37:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.38:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.39:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_40
# %bb.589:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB0_939
# %bb.590:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB0_939
.LBB0_40:
	xor	ecx, ecx
.LBB0_997:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_999
.LBB0_998:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_998
.LBB0_999:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_1000:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1000
	jmp	.LBB0_1395
.LBB0_41:
	cmp	sil, 7
	je	.LBB0_87
# %bb.42:
	cmp	sil, 9
	jne	.LBB0_1395
# %bb.43:
	cmp	edi, 6
	jg	.LBB0_128
# %bb.44:
	cmp	edi, 3
	jle	.LBB0_202
# %bb.45:
	cmp	edi, 4
	je	.LBB0_333
# %bb.46:
	cmp	edi, 5
	je	.LBB0_336
# %bb.47:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.48:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.49:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_50
# %bb.592:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB0_941
# %bb.593:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB0_941
.LBB0_50:
	xor	ecx, ecx
.LBB0_1007:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1009
.LBB0_1008:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1008
.LBB0_1009:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_1010:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1010
	jmp	.LBB0_1395
.LBB0_51:
	cmp	edi, 6
	jg	.LBB0_135
# %bb.52:
	cmp	edi, 3
	jle	.LBB0_207
# %bb.53:
	cmp	edi, 4
	je	.LBB0_339
# %bb.54:
	cmp	edi, 5
	je	.LBB0_346
# %bb.55:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.56:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.57:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_595
# %bb.58:
	xor	esi, esi
.LBB0_59:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_61
.LBB0_60:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_60
.LBB0_61:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_63:
	cmp	edi, 6
	jg	.LBB0_146
# %bb.64:
	cmp	edi, 3
	jle	.LBB0_216
# %bb.65:
	cmp	edi, 4
	je	.LBB0_353
# %bb.66:
	cmp	edi, 5
	je	.LBB0_360
# %bb.67:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.68:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.69:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_600
# %bb.70:
	xor	esi, esi
.LBB0_71:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_73
.LBB0_72:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_72
.LBB0_73:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_75:
	cmp	edi, 6
	jg	.LBB0_157
# %bb.76:
	cmp	edi, 3
	jle	.LBB0_225
# %bb.77:
	cmp	edi, 4
	je	.LBB0_367
# %bb.78:
	cmp	edi, 5
	je	.LBB0_374
# %bb.79:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.80:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.81:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_605
# %bb.82:
	xor	esi, esi
.LBB0_83:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_85
.LBB0_84:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_84
.LBB0_85:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_87:
	cmp	edi, 6
	jg	.LBB0_168
# %bb.88:
	cmp	edi, 3
	jle	.LBB0_234
# %bb.89:
	cmp	edi, 4
	je	.LBB0_381
# %bb.90:
	cmp	edi, 5
	je	.LBB0_388
# %bb.91:
	cmp	edi, 6
	jne	.LBB0_1395
# %bb.92:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.93:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_610
# %bb.94:
	xor	esi, esi
.LBB0_95:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_97
.LBB0_96:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_96
.LBB0_97:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_99:
	cmp	edi, 8
	jle	.LBB0_243
# %bb.100:
	cmp	edi, 9
	je	.LBB0_395
# %bb.101:
	cmp	edi, 11
	je	.LBB0_402
# %bb.102:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.103:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.104:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_615
# %bb.105:
	xor	esi, esi
.LBB0_106:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_108
.LBB0_107:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_107
.LBB0_108:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_109:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_109
	jmp	.LBB0_1395
.LBB0_110:
	cmp	edi, 8
	jle	.LBB0_252
# %bb.111:
	cmp	edi, 9
	je	.LBB0_409
# %bb.112:
	cmp	edi, 11
	je	.LBB0_416
# %bb.113:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.114:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.115:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_620
# %bb.116:
	xor	esi, esi
.LBB0_117:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_119
.LBB0_118:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_118
.LBB0_119:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_120:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_120
	jmp	.LBB0_1395
.LBB0_121:
	cmp	edi, 8
	jle	.LBB0_261
# %bb.122:
	cmp	edi, 9
	je	.LBB0_423
# %bb.123:
	cmp	edi, 11
	je	.LBB0_426
# %bb.124:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.125:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.126:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_127
# %bb.625:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_943
# %bb.626:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_943
.LBB0_127:
	xor	ecx, ecx
.LBB0_1299:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1301
.LBB0_1300:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1300
.LBB0_1301:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_1302:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1302
	jmp	.LBB0_1395
.LBB0_128:
	cmp	edi, 8
	jle	.LBB0_266
# %bb.129:
	cmp	edi, 9
	je	.LBB0_429
# %bb.130:
	cmp	edi, 11
	je	.LBB0_432
# %bb.131:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.132:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.133:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_134
# %bb.628:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_946
# %bb.629:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_946
.LBB0_134:
	xor	ecx, ecx
.LBB0_1307:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_1309
.LBB0_1308:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB0_1308
.LBB0_1309:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_1310:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1310
	jmp	.LBB0_1395
.LBB0_135:
	cmp	edi, 8
	jle	.LBB0_271
# %bb.136:
	cmp	edi, 9
	je	.LBB0_435
# %bb.137:
	cmp	edi, 11
	je	.LBB0_442
# %bb.138:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.139:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.140:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_631
# %bb.141:
	xor	esi, esi
.LBB0_142:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_144
.LBB0_143:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_143
.LBB0_144:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_145:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_145
	jmp	.LBB0_1395
.LBB0_146:
	cmp	edi, 8
	jle	.LBB0_280
# %bb.147:
	cmp	edi, 9
	je	.LBB0_449
# %bb.148:
	cmp	edi, 11
	je	.LBB0_456
# %bb.149:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.150:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.151:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_636
# %bb.152:
	xor	esi, esi
.LBB0_153:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_155
.LBB0_154:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_154
.LBB0_155:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_156:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_156
	jmp	.LBB0_1395
.LBB0_157:
	cmp	edi, 8
	jle	.LBB0_289
# %bb.158:
	cmp	edi, 9
	je	.LBB0_463
# %bb.159:
	cmp	edi, 11
	je	.LBB0_466
# %bb.160:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.161:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.162:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_641
# %bb.163:
	xor	esi, esi
.LBB0_164:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_166
.LBB0_165:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_165
.LBB0_166:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_167:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_167
	jmp	.LBB0_1395
.LBB0_168:
	cmp	edi, 8
	jle	.LBB0_294
# %bb.169:
	cmp	edi, 9
	je	.LBB0_473
# %bb.170:
	cmp	edi, 11
	je	.LBB0_476
# %bb.171:
	cmp	edi, 12
	jne	.LBB0_1395
# %bb.172:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.173:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_646
# %bb.174:
	xor	esi, esi
.LBB0_175:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_177
.LBB0_176:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_176
.LBB0_177:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_178:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_178
	jmp	.LBB0_1395
.LBB0_179:
	cmp	edi, 2
	je	.LBB0_483
# %bb.180:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.181:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.182:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_651
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
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_188:
	cmp	edi, 2
	je	.LBB0_490
# %bb.189:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.190:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.191:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_656
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
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_197:
	cmp	edi, 2
	je	.LBB0_497
# %bb.198:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.199:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.200:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_201
# %bb.661:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_949
# %bb.662:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_949
.LBB0_201:
	xor	ecx, ecx
.LBB0_1315:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB0_1317
# %bb.1316:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_1317:
	add	rsi, r10
	je	.LBB0_1395
.LBB0_1318:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1318
	jmp	.LBB0_1395
.LBB0_202:
	cmp	edi, 2
	je	.LBB0_500
# %bb.203:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.204:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.205:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_206
# %bb.664:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB0_952
# %bb.665:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB0_952
.LBB0_206:
	xor	ecx, ecx
.LBB0_1323:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB0_1325
# %bb.1324:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB0_1325:
	add	rsi, r10
	je	.LBB0_1395
.LBB0_1326:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1326
	jmp	.LBB0_1395
.LBB0_207:
	cmp	edi, 2
	je	.LBB0_503
# %bb.208:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.209:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.210:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_667
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
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_216:
	cmp	edi, 2
	je	.LBB0_510
# %bb.217:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.218:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.219:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_672
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
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_225:
	cmp	edi, 2
	je	.LBB0_517
# %bb.226:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.227:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.228:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_677
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
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_234:
	cmp	edi, 2
	je	.LBB0_524
# %bb.235:
	cmp	edi, 3
	jne	.LBB0_1395
# %bb.236:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.237:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_682
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
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_243:
	cmp	edi, 7
	je	.LBB0_531
# %bb.244:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.245:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.246:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_687
# %bb.247:
	xor	esi, esi
.LBB0_248:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_250
.LBB0_249:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_249
.LBB0_250:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_252:
	cmp	edi, 7
	je	.LBB0_538
# %bb.253:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.254:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.255:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_692
# %bb.256:
	xor	esi, esi
.LBB0_257:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_259
.LBB0_258:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_258
.LBB0_259:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_261:
	cmp	edi, 7
	je	.LBB0_545
# %bb.262:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.263:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.264:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_265
# %bb.697:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_955
# %bb.698:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_955
.LBB0_265:
	xor	ecx, ecx
.LBB0_1017:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1019
.LBB0_1018:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1018
.LBB0_1019:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_1020:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1020
	jmp	.LBB0_1395
.LBB0_266:
	cmp	edi, 7
	je	.LBB0_548
# %bb.267:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.268:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.269:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB0_270
# %bb.700:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB0_957
# %bb.701:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB0_957
.LBB0_270:
	xor	ecx, ecx
.LBB0_1027:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1029
.LBB0_1028:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1028
.LBB0_1029:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_1030:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1030
	jmp	.LBB0_1395
.LBB0_271:
	cmp	edi, 7
	je	.LBB0_551
# %bb.272:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.273:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.274:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_703
# %bb.275:
	xor	esi, esi
.LBB0_276:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_278
.LBB0_277:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_277
.LBB0_278:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_280:
	cmp	edi, 7
	je	.LBB0_558
# %bb.281:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.282:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.283:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_708
# %bb.284:
	xor	esi, esi
.LBB0_285:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_287
.LBB0_286:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_286
.LBB0_287:
	cmp	r9, 3
	jb	.LBB0_1395
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
	jmp	.LBB0_1395
.LBB0_289:
	cmp	edi, 7
	je	.LBB0_565
# %bb.290:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.291:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.292:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_713
# %bb.293:
	xor	edi, edi
	jmp	.LBB0_715
.LBB0_294:
	cmp	edi, 7
	je	.LBB0_572
# %bb.295:
	cmp	edi, 8
	jne	.LBB0_1395
# %bb.296:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.297:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_718
# %bb.298:
	xor	edi, edi
	jmp	.LBB0_720
.LBB0_299:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.300:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_723
# %bb.301:
	xor	esi, esi
.LBB0_302:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_304
.LBB0_303:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_303
.LBB0_304:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_305:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_305
	jmp	.LBB0_1395
.LBB0_306:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.307:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_728
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
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_310
.LBB0_311:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_312:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_312
	jmp	.LBB0_1395
.LBB0_313:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.314:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_733
# %bb.315:
	xor	esi, esi
.LBB0_316:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_318
.LBB0_317:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_317
.LBB0_318:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_319:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_319
	jmp	.LBB0_1395
.LBB0_320:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.321:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_738
# %bb.322:
	xor	esi, esi
.LBB0_323:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_325
.LBB0_324:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_324
.LBB0_325:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_326:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_326
	jmp	.LBB0_1395
.LBB0_327:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.328:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_329
# %bb.743:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_959
# %bb.744:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_959
.LBB0_329:
	xor	ecx, ecx
.LBB0_1037:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1039
.LBB0_1038:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1038
.LBB0_1039:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_1040:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1040
	jmp	.LBB0_1395
.LBB0_330:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.331:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_332
# %bb.746:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_961
# %bb.747:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_961
.LBB0_332:
	xor	ecx, ecx
.LBB0_1331:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1333
# %bb.1332:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1333:
	add	rsi, rax
	je	.LBB0_1395
.LBB0_1334:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1334
	jmp	.LBB0_1395
.LBB0_333:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.334:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB0_335
# %bb.749:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB0_964
# %bb.750:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB0_964
.LBB0_335:
	xor	ecx, ecx
.LBB0_1047:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1049
.LBB0_1048:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1048
.LBB0_1049:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_1050:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1050
	jmp	.LBB0_1395
.LBB0_336:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.337:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_338
# %bb.752:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB0_966
# %bb.753:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB0_966
.LBB0_338:
	xor	ecx, ecx
.LBB0_1339:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1341
# %bb.1340:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB0_1341:
	add	rsi, rax
	je	.LBB0_1395
.LBB0_1342:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1342
	jmp	.LBB0_1395
.LBB0_339:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.340:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_755
# %bb.341:
	xor	esi, esi
.LBB0_342:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_344
.LBB0_343:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_343
.LBB0_344:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_345:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_345
	jmp	.LBB0_1395
.LBB0_346:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.347:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_760
# %bb.348:
	xor	esi, esi
.LBB0_349:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_351
.LBB0_350:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_350
.LBB0_351:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_352:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_352
	jmp	.LBB0_1395
.LBB0_353:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.354:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_765
# %bb.355:
	xor	esi, esi
.LBB0_356:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_358
.LBB0_357:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_357
.LBB0_358:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_359:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_359
	jmp	.LBB0_1395
.LBB0_360:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.361:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_770
# %bb.362:
	xor	esi, esi
.LBB0_363:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_365
.LBB0_364:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_364
.LBB0_365:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_366:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_366
	jmp	.LBB0_1395
.LBB0_367:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.368:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_775
# %bb.369:
	xor	esi, esi
.LBB0_370:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_372
.LBB0_371:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_371
.LBB0_372:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_373:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_373
	jmp	.LBB0_1395
.LBB0_374:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.375:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_780
# %bb.376:
	xor	esi, esi
.LBB0_377:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_379
.LBB0_378:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_378
.LBB0_379:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_380:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_380
	jmp	.LBB0_1395
.LBB0_381:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.382:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_785
# %bb.383:
	xor	esi, esi
.LBB0_384:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_386
.LBB0_385:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_385
.LBB0_386:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_387:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_387
	jmp	.LBB0_1395
.LBB0_388:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.389:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_790
# %bb.390:
	xor	esi, esi
.LBB0_391:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_393
.LBB0_392:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_392
.LBB0_393:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_394:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_394
	jmp	.LBB0_1395
.LBB0_395:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.396:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_795
# %bb.397:
	xor	esi, esi
.LBB0_398:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_400
.LBB0_399:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_399
.LBB0_400:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_401:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_401
	jmp	.LBB0_1395
.LBB0_402:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.403:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_800
# %bb.404:
	xor	esi, esi
.LBB0_405:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_407
.LBB0_406:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_406
.LBB0_407:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_408:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_408
	jmp	.LBB0_1395
.LBB0_409:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.410:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_805
# %bb.411:
	xor	esi, esi
.LBB0_412:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_414
.LBB0_413:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_413
.LBB0_414:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_415:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_415
	jmp	.LBB0_1395
.LBB0_416:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.417:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_810
# %bb.418:
	xor	esi, esi
.LBB0_419:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_421
.LBB0_420:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_420
.LBB0_421:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_422:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_422
	jmp	.LBB0_1395
.LBB0_423:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.424:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_425
# %bb.815:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_969
# %bb.816:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_969
.LBB0_425:
	xor	ecx, ecx
.LBB0_1347:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1349
# %bb.1348:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1349:
	add	rsi, rax
	je	.LBB0_1395
.LBB0_1350:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1350
	jmp	.LBB0_1395
.LBB0_426:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.427:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_428
# %bb.818:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_972
# %bb.819:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_972
.LBB0_428:
	xor	ecx, ecx
.LBB0_1355:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1358
# %bb.1356:
	mov	esi, 2147483647
.LBB0_1357:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1357
.LBB0_1358:
	cmp	r9, 3
	jb	.LBB0_1395
# %bb.1359:
	mov	esi, 2147483647
.LBB0_1360:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1360
	jmp	.LBB0_1395
.LBB0_429:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.430:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB0_431
# %bb.821:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB0_975
# %bb.822:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB0_975
.LBB0_431:
	xor	ecx, ecx
.LBB0_1365:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1367
# %bb.1366:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB0_1367:
	add	rsi, rax
	je	.LBB0_1395
.LBB0_1368:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1368
	jmp	.LBB0_1395
.LBB0_432:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.433:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB0_434
# %bb.824:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB0_978
# %bb.825:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB0_978
.LBB0_434:
	xor	ecx, ecx
.LBB0_1373:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1376
# %bb.1374:
	mov	esi, 2147483647
.LBB0_1375:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB0_1375
.LBB0_1376:
	cmp	r9, 3
	jb	.LBB0_1395
# %bb.1377:
	mov	esi, 2147483647
.LBB0_1378:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1378
	jmp	.LBB0_1395
.LBB0_435:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.436:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_827
# %bb.437:
	xor	esi, esi
.LBB0_438:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_440
.LBB0_439:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_439
.LBB0_440:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_441:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_441
	jmp	.LBB0_1395
.LBB0_442:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.443:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_832
# %bb.444:
	xor	esi, esi
.LBB0_445:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_447
.LBB0_446:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_446
.LBB0_447:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_448:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_448
	jmp	.LBB0_1395
.LBB0_449:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.450:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_837
# %bb.451:
	xor	esi, esi
.LBB0_452:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_454
.LBB0_453:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_453
.LBB0_454:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_455:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_455
	jmp	.LBB0_1395
.LBB0_456:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.457:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_842
# %bb.458:
	xor	esi, esi
.LBB0_459:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_461
.LBB0_460:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_460
.LBB0_461:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_462:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_462
	jmp	.LBB0_1395
.LBB0_463:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.464:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_847
# %bb.465:
	xor	edi, edi
	jmp	.LBB0_849
.LBB0_466:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.467:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_852
# %bb.468:
	xor	esi, esi
.LBB0_469:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_471
.LBB0_470:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_470
.LBB0_471:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_472:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_472
	jmp	.LBB0_1395
.LBB0_473:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.474:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_857
# %bb.475:
	xor	edi, edi
	jmp	.LBB0_859
.LBB0_476:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.477:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_862
# %bb.478:
	xor	esi, esi
.LBB0_479:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_481
.LBB0_480:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_480
.LBB0_481:
	cmp	rax, 3
	jb	.LBB0_1395
.LBB0_482:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_482
	jmp	.LBB0_1395
.LBB0_483:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.484:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_867
# %bb.485:
	xor	esi, esi
.LBB0_486:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_488
.LBB0_487:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_487
.LBB0_488:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_489:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_489
	jmp	.LBB0_1395
.LBB0_490:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.491:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_872
# %bb.492:
	xor	esi, esi
.LBB0_493:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_495
.LBB0_494:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_494
.LBB0_495:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_496:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_496
	jmp	.LBB0_1395
.LBB0_497:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.498:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_499
# %bb.877:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB0_981
# %bb.878:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB0_981
.LBB0_499:
	xor	ecx, ecx
.LBB0_1057:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1059
.LBB0_1058:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1058
.LBB0_1059:
	cmp	rdi, 3
	jb	.LBB0_1395
.LBB0_1060:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1060
	jmp	.LBB0_1395
.LBB0_500:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.501:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB0_502
# %bb.880:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB0_983
# %bb.881:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB0_983
.LBB0_502:
	xor	ecx, ecx
.LBB0_1067:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_1069
.LBB0_1068:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB0_1068
.LBB0_1069:
	cmp	rdi, 3
	jb	.LBB0_1395
.LBB0_1070:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1070
	jmp	.LBB0_1395
.LBB0_503:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.504:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_883
# %bb.505:
	xor	esi, esi
.LBB0_506:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_508
.LBB0_507:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_507
.LBB0_508:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_509:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_509
	jmp	.LBB0_1395
.LBB0_510:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.511:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_888
# %bb.512:
	xor	esi, esi
.LBB0_513:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_515
.LBB0_514:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_514
.LBB0_515:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_516:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_516
	jmp	.LBB0_1395
.LBB0_517:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.518:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_893
# %bb.519:
	xor	edi, edi
.LBB0_520:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_522
.LBB0_521:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_521
.LBB0_522:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_523:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_523
	jmp	.LBB0_1395
.LBB0_524:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.525:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_898
# %bb.526:
	xor	edi, edi
.LBB0_527:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_529
.LBB0_528:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_528
.LBB0_529:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_530:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_530
	jmp	.LBB0_1395
.LBB0_531:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.532:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_903
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
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_535
.LBB0_536:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_537:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_537
	jmp	.LBB0_1395
.LBB0_538:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.539:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_908
# %bb.540:
	xor	esi, esi
.LBB0_541:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_543
.LBB0_542:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_542
.LBB0_543:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_544:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_544
	jmp	.LBB0_1395
.LBB0_545:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.546:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_547
# %bb.913:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_985
# %bb.914:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_985
.LBB0_547:
	xor	ecx, ecx
.LBB0_1383:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1385
# %bb.1384:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1385:
	add	rsi, rax
	je	.LBB0_1395
.LBB0_1386:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1386
	jmp	.LBB0_1395
.LBB0_548:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.549:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB0_550
# %bb.916:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB0_988
# %bb.917:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB0_988
.LBB0_550:
	xor	ecx, ecx
.LBB0_1391:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB0_1393
# %bb.1392:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB0_1393:
	add	rsi, rax
	je	.LBB0_1395
.LBB0_1394:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1394
	jmp	.LBB0_1395
.LBB0_551:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.552:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_919
# %bb.553:
	xor	esi, esi
.LBB0_554:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_556
.LBB0_555:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_555
.LBB0_556:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_557:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_557
	jmp	.LBB0_1395
.LBB0_558:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.559:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_924
# %bb.560:
	xor	esi, esi
.LBB0_561:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_563
.LBB0_562:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_562
.LBB0_563:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_564:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_564
	jmp	.LBB0_1395
.LBB0_565:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.566:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_929
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
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_569
.LBB0_570:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_571:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_571
	jmp	.LBB0_1395
.LBB0_572:
	test	r9d, r9d
	jle	.LBB0_1395
# %bb.573:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_934
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
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_576
.LBB0_577:
	cmp	r9, 3
	jb	.LBB0_1395
.LBB0_578:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_578
	jmp	.LBB0_1395
.LBB0_579:
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
# %bb.580:
	and	al, dil
	jne	.LBB0_12
# %bb.581:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1071
# %bb.582:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_583:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_583
	jmp	.LBB0_1072
.LBB0_584:
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
# %bb.585:
	and	al, dil
	jne	.LBB0_27
# %bb.586:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1075
# %bb.587:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_588:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_588
	jmp	.LBB0_1076
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
	jne	.LBB0_59
# %bb.596:
	and	al, dil
	jne	.LBB0_59
# %bb.597:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1079
# %bb.598:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_599:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_599
	jmp	.LBB0_1080
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
	jne	.LBB0_71
# %bb.601:
	and	al, dil
	jne	.LBB0_71
# %bb.602:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1083
# %bb.603:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_604:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_604
	jmp	.LBB0_1084
.LBB0_605:
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
# %bb.606:
	and	al, dil
	jne	.LBB0_83
# %bb.607:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1087
# %bb.608:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_609:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_609
	jmp	.LBB0_1088
.LBB0_610:
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
# %bb.611:
	and	al, dil
	jne	.LBB0_95
# %bb.612:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1091
# %bb.613:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_614:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_614
	jmp	.LBB0_1092
.LBB0_615:
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
# %bb.616:
	and	al, dil
	jne	.LBB0_106
# %bb.617:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1095
# %bb.618:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_619:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_619
	jmp	.LBB0_1096
.LBB0_620:
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
# %bb.621:
	and	al, dil
	jne	.LBB0_117
# %bb.622:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1099
# %bb.623:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_624:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_624
	jmp	.LBB0_1100
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
	jne	.LBB0_142
# %bb.632:
	and	al, dil
	jne	.LBB0_142
# %bb.633:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1103
# %bb.634:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_635:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_635
	jmp	.LBB0_1104
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
	jne	.LBB0_153
# %bb.637:
	and	al, dil
	jne	.LBB0_153
# %bb.638:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1107
# %bb.639:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_640:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_640
	jmp	.LBB0_1108
.LBB0_641:
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
# %bb.642:
	and	al, dil
	jne	.LBB0_164
# %bb.643:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1111
# %bb.644:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_645:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_645
	jmp	.LBB0_1112
.LBB0_646:
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
# %bb.647:
	and	al, dil
	jne	.LBB0_175
# %bb.648:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1115
# %bb.649:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_650:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_650
	jmp	.LBB0_1116
.LBB0_651:
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
# %bb.652:
	and	al, dil
	jne	.LBB0_184
# %bb.653:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1119
# %bb.654:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_655:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_655
	jmp	.LBB0_1120
.LBB0_656:
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
# %bb.657:
	and	al, dil
	jne	.LBB0_193
# %bb.658:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1123
# %bb.659:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_660
	jmp	.LBB0_1124
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
	jne	.LBB0_212
# %bb.668:
	and	al, dil
	jne	.LBB0_212
# %bb.669:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1127
# %bb.670:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_671:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_671
	jmp	.LBB0_1128
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
	jne	.LBB0_221
# %bb.673:
	and	al, dil
	jne	.LBB0_221
# %bb.674:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1131
# %bb.675:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_676
	jmp	.LBB0_1132
.LBB0_677:
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
# %bb.678:
	and	al, sil
	jne	.LBB0_230
# %bb.679:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1135
# %bb.680:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_681
	jmp	.LBB0_1136
.LBB0_682:
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
# %bb.683:
	and	al, sil
	jne	.LBB0_239
# %bb.684:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1139
# %bb.685:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_686:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_686
	jmp	.LBB0_1140
.LBB0_687:
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
# %bb.688:
	and	al, dil
	jne	.LBB0_248
# %bb.689:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1143
# %bb.690:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_691:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_691
	jmp	.LBB0_1144
.LBB0_692:
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
# %bb.693:
	and	al, dil
	jne	.LBB0_257
# %bb.694:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1147
# %bb.695:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_696:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_696
	jmp	.LBB0_1148
.LBB0_703:
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
# %bb.704:
	and	al, dil
	jne	.LBB0_276
# %bb.705:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1151
# %bb.706:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_707:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_707
	jmp	.LBB0_1152
.LBB0_708:
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
# %bb.709:
	and	al, dil
	jne	.LBB0_285
# %bb.710:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1155
# %bb.711:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_712:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_712
	jmp	.LBB0_1156
.LBB0_713:
	and	esi, -4
	xor	edi, edi
.LBB0_714:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_714
.LBB0_715:
	test	r9, r9
	je	.LBB0_1395
# %bb.716:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_717:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_717
	jmp	.LBB0_1395
.LBB0_718:
	and	esi, -4
	xor	edi, edi
.LBB0_719:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_719
.LBB0_720:
	test	r9, r9
	je	.LBB0_1395
# %bb.721:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_722:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_722
	jmp	.LBB0_1395
.LBB0_723:
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
	jne	.LBB0_302
# %bb.724:
	and	al, dil
	jne	.LBB0_302
# %bb.725:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1159
# %bb.726:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_727:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_727
	jmp	.LBB0_1160
.LBB0_728:
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
	jne	.LBB0_309
# %bb.729:
	and	al, dil
	jne	.LBB0_309
# %bb.730:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1163
# %bb.731:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_732:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_732
	jmp	.LBB0_1164
.LBB0_733:
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
	jne	.LBB0_316
# %bb.734:
	and	al, dil
	jne	.LBB0_316
# %bb.735:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1167
# %bb.736:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_737:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_737
	jmp	.LBB0_1168
.LBB0_738:
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
	jne	.LBB0_323
# %bb.739:
	and	al, dil
	jne	.LBB0_323
# %bb.740:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1171
# %bb.741:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_742:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_742
	jmp	.LBB0_1172
.LBB0_755:
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
	jne	.LBB0_342
# %bb.756:
	and	al, dil
	jne	.LBB0_342
# %bb.757:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1175
# %bb.758:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_759:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_759
	jmp	.LBB0_1176
.LBB0_760:
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
	jne	.LBB0_349
# %bb.761:
	and	al, dil
	jne	.LBB0_349
# %bb.762:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1179
# %bb.763:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_764
	jmp	.LBB0_1180
.LBB0_765:
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
	jne	.LBB0_356
# %bb.766:
	and	al, dil
	jne	.LBB0_356
# %bb.767:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1183
# %bb.768:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_769:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_769
	jmp	.LBB0_1184
.LBB0_770:
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
	jne	.LBB0_363
# %bb.771:
	and	al, dil
	jne	.LBB0_363
# %bb.772:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1187
# %bb.773:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_774:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_774
	jmp	.LBB0_1188
.LBB0_775:
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
	jne	.LBB0_370
# %bb.776:
	and	al, dil
	jne	.LBB0_370
# %bb.777:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1191
# %bb.778:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_779:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_779
	jmp	.LBB0_1192
.LBB0_780:
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
	jne	.LBB0_377
# %bb.781:
	and	al, dil
	jne	.LBB0_377
# %bb.782:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1195
# %bb.783:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_784:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_784
	jmp	.LBB0_1196
.LBB0_785:
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
	jne	.LBB0_384
# %bb.786:
	and	al, dil
	jne	.LBB0_384
# %bb.787:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1199
# %bb.788:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_789:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_789
	jmp	.LBB0_1200
.LBB0_790:
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
	jne	.LBB0_391
# %bb.791:
	and	al, dil
	jne	.LBB0_391
# %bb.792:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_1203
# %bb.793:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_794:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_794
	jmp	.LBB0_1204
.LBB0_795:
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
	jne	.LBB0_398
# %bb.796:
	and	al, dil
	jne	.LBB0_398
# %bb.797:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1207
# %bb.798:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_799:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_799
	jmp	.LBB0_1208
.LBB0_800:
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
	jne	.LBB0_405
# %bb.801:
	and	al, dil
	jne	.LBB0_405
# %bb.802:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1211
# %bb.803:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_804
	jmp	.LBB0_1212
.LBB0_805:
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
	jne	.LBB0_412
# %bb.806:
	and	al, dil
	jne	.LBB0_412
# %bb.807:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1215
# %bb.808:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_809:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_809
	jmp	.LBB0_1216
.LBB0_810:
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
	jne	.LBB0_419
# %bb.811:
	and	al, dil
	jne	.LBB0_419
# %bb.812:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1219
# %bb.813:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_814:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_814
	jmp	.LBB0_1220
.LBB0_827:
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
	jne	.LBB0_438
# %bb.828:
	and	al, dil
	jne	.LBB0_438
# %bb.829:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1223
# %bb.830:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_831:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_831
	jmp	.LBB0_1224
.LBB0_832:
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
	jne	.LBB0_445
# %bb.833:
	and	al, dil
	jne	.LBB0_445
# %bb.834:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1227
# %bb.835:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_836:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_836
	jmp	.LBB0_1228
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
	jne	.LBB0_452
# %bb.838:
	and	al, dil
	jne	.LBB0_452
# %bb.839:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1231
# %bb.840:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_841:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_841
	jmp	.LBB0_1232
.LBB0_842:
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
	jne	.LBB0_459
# %bb.843:
	and	al, dil
	jne	.LBB0_459
# %bb.844:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1235
# %bb.845:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_846:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_846
	jmp	.LBB0_1236
.LBB0_847:
	and	esi, -4
	xor	edi, edi
.LBB0_848:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_848
.LBB0_849:
	test	r9, r9
	je	.LBB0_1395
# %bb.850:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_851:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_851
	jmp	.LBB0_1395
.LBB0_852:
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
	jne	.LBB0_469
# %bb.853:
	and	al, dil
	jne	.LBB0_469
# %bb.854:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1239
# %bb.855:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_856:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_856
	jmp	.LBB0_1240
.LBB0_857:
	and	esi, -4
	xor	edi, edi
.LBB0_858:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_858
.LBB0_859:
	test	r9, r9
	je	.LBB0_1395
# %bb.860:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_861:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_861
.LBB0_1395:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_862:
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
	jne	.LBB0_479
# %bb.863:
	and	al, dil
	jne	.LBB0_479
# %bb.864:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1243
# %bb.865:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_866:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_866
	jmp	.LBB0_1244
.LBB0_867:
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
	jne	.LBB0_486
# %bb.868:
	and	al, dil
	jne	.LBB0_486
# %bb.869:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1247
# %bb.870:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_871:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_871
	jmp	.LBB0_1248
.LBB0_872:
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
	jne	.LBB0_493
# %bb.873:
	and	al, dil
	jne	.LBB0_493
# %bb.874:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1251
# %bb.875:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_876:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_876
	jmp	.LBB0_1252
.LBB0_883:
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
	jne	.LBB0_506
# %bb.884:
	and	al, dil
	jne	.LBB0_506
# %bb.885:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1255
# %bb.886:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_887:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_887
	jmp	.LBB0_1256
.LBB0_888:
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
	jne	.LBB0_513
# %bb.889:
	and	al, dil
	jne	.LBB0_513
# %bb.890:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1259
# %bb.891:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_892:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_892
	jmp	.LBB0_1260
.LBB0_893:
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
	jne	.LBB0_520
# %bb.894:
	and	al, sil
	jne	.LBB0_520
# %bb.895:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1263
# %bb.896:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_897:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_897
	jmp	.LBB0_1264
.LBB0_898:
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
	jne	.LBB0_527
# %bb.899:
	and	al, sil
	jne	.LBB0_527
# %bb.900:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_1267
# %bb.901:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_3] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_902:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_902
	jmp	.LBB0_1268
.LBB0_903:
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
	jne	.LBB0_534
# %bb.904:
	and	al, dil
	jne	.LBB0_534
# %bb.905:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1271
# %bb.906:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_907:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_907
	jmp	.LBB0_1272
.LBB0_908:
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
# %bb.909:
	and	al, dil
	jne	.LBB0_541
# %bb.910:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1275
# %bb.911:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_912:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_912
	jmp	.LBB0_1276
.LBB0_919:
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
	jne	.LBB0_554
# %bb.920:
	and	al, dil
	jne	.LBB0_554
# %bb.921:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1279
# %bb.922:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_923:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_923
	jmp	.LBB0_1280
.LBB0_924:
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
	jne	.LBB0_561
# %bb.925:
	and	al, dil
	jne	.LBB0_561
# %bb.926:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1283
# %bb.927:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_928:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_928
	jmp	.LBB0_1284
.LBB0_929:
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
	jne	.LBB0_568
# %bb.930:
	and	al, dil
	jne	.LBB0_568
# %bb.931:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1287
# %bb.932:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_933:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_933
	jmp	.LBB0_1288
.LBB0_934:
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
	jne	.LBB0_575
# %bb.935:
	and	al, dil
	jne	.LBB0_575
# %bb.936:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_1291
# %bb.937:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_938:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_938
	jmp	.LBB0_1292
.LBB0_939:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB0_991
# %bb.940:
	xor	eax, eax
	jmp	.LBB0_993
.LBB0_941:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB0_1001
# %bb.942:
	xor	eax, eax
	jmp	.LBB0_1003
.LBB0_943:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1295
# %bb.944:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB0_945:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_945
	jmp	.LBB0_1296
.LBB0_946:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1303
# %bb.947:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB0_948:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_948
	jmp	.LBB0_1304
.LBB0_949:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1311
# %bb.950:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI0_2] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB0_951:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_951
	jmp	.LBB0_1312
.LBB0_952:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1319
# %bb.953:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI0_2] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB0_954:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_954
	jmp	.LBB0_1320
.LBB0_955:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB0_1011
# %bb.956:
	xor	eax, eax
	jmp	.LBB0_1013
.LBB0_957:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB0_1021
# %bb.958:
	xor	eax, eax
	jmp	.LBB0_1023
.LBB0_959:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB0_1031
# %bb.960:
	xor	eax, eax
	jmp	.LBB0_1033
.LBB0_961:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1327
# %bb.962:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB0_963:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_963
	jmp	.LBB0_1328
.LBB0_964:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB0_1041
# %bb.965:
	xor	eax, eax
	jmp	.LBB0_1043
.LBB0_966:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1335
# %bb.967:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB0_968:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_968
	jmp	.LBB0_1336
.LBB0_969:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1343
# %bb.970:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB0_971:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_971
	jmp	.LBB0_1344
.LBB0_972:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1351
# %bb.973:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB0_974:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_974
	jmp	.LBB0_1352
.LBB0_975:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1361
# %bb.976:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB0_977:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_977
	jmp	.LBB0_1362
.LBB0_978:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1369
# %bb.979:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB0_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_980
	jmp	.LBB0_1370
.LBB0_981:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB0_1051
# %bb.982:
	xor	edi, edi
	jmp	.LBB0_1053
.LBB0_983:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB0_1061
# %bb.984:
	xor	edi, edi
	jmp	.LBB0_1063
.LBB0_985:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1379
# %bb.986:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_987:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_987
	jmp	.LBB0_1380
.LBB0_988:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB0_1387
# %bb.989:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB0_990:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_990
	jmp	.LBB0_1388
.LBB0_991:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_992:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_992
.LBB0_993:
	test	rsi, rsi
	je	.LBB0_996
# %bb.994:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB0_995:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_995
.LBB0_996:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_997
.LBB0_1001:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1002:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1002
.LBB0_1003:
	test	rsi, rsi
	je	.LBB0_1006
# %bb.1004:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB0_1005:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1005
.LBB0_1006:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1007
.LBB0_1011:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1012
.LBB0_1013:
	test	rsi, rsi
	je	.LBB0_1016
# %bb.1014:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB0_1015:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1015
.LBB0_1016:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1017
.LBB0_1021:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1022:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1022
.LBB0_1023:
	test	rsi, rsi
	je	.LBB0_1026
# %bb.1024:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB0_1025:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1025
.LBB0_1026:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1027
.LBB0_1031:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1032:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1032
.LBB0_1033:
	test	rsi, rsi
	je	.LBB0_1036
# %bb.1034:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB0_1035:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1035
.LBB0_1036:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1037
.LBB0_1041:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB0_1042:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1042
.LBB0_1043:
	test	rsi, rsi
	je	.LBB0_1046
# %bb.1044:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB0_1045:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB0_1045
.LBB0_1046:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1047
.LBB0_1051:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB0_1052:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1052
.LBB0_1053:
	test	rax, rax
	je	.LBB0_1056
# %bb.1054:
	add	rdi, 16
	neg	rax
.LBB0_1055:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB0_1055
.LBB0_1056:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1057
.LBB0_1061:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB0_1062:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1062
.LBB0_1063:
	test	rax, rax
	je	.LBB0_1066
# %bb.1064:
	add	rdi, 16
	neg	rax
.LBB0_1065:                             # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB0_1065
.LBB0_1066:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1067
.LBB0_1071:
	xor	edi, edi
.LBB0_1072:
	test	r9b, 1
	je	.LBB0_1074
# %bb.1073:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1074:
	cmp	rsi, r10
	jne	.LBB0_12
	jmp	.LBB0_1395
.LBB0_1075:
	xor	edi, edi
.LBB0_1076:
	test	r9b, 1
	je	.LBB0_1078
# %bb.1077:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1078:
	cmp	rsi, r10
	jne	.LBB0_27
	jmp	.LBB0_1395
.LBB0_1079:
	xor	edi, edi
.LBB0_1080:
	test	r9b, 1
	je	.LBB0_1082
# %bb.1081:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1082:
	cmp	rsi, r10
	jne	.LBB0_59
	jmp	.LBB0_1395
.LBB0_1083:
	xor	edi, edi
.LBB0_1084:
	test	r9b, 1
	je	.LBB0_1086
# %bb.1085:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1086:
	cmp	rsi, r10
	jne	.LBB0_71
	jmp	.LBB0_1395
.LBB0_1087:
	xor	edi, edi
.LBB0_1088:
	test	r9b, 1
	je	.LBB0_1090
# %bb.1089:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1090:
	cmp	rsi, r10
	jne	.LBB0_83
	jmp	.LBB0_1395
.LBB0_1091:
	xor	edi, edi
.LBB0_1092:
	test	r9b, 1
	je	.LBB0_1094
# %bb.1093:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1094:
	cmp	rsi, r10
	jne	.LBB0_95
	jmp	.LBB0_1395
.LBB0_1095:
	xor	edi, edi
.LBB0_1096:
	test	r9b, 1
	je	.LBB0_1098
# %bb.1097:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1098:
	cmp	rsi, r10
	jne	.LBB0_106
	jmp	.LBB0_1395
.LBB0_1099:
	xor	edi, edi
.LBB0_1100:
	test	r9b, 1
	je	.LBB0_1102
# %bb.1101:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1102:
	cmp	rsi, r10
	jne	.LBB0_117
	jmp	.LBB0_1395
.LBB0_1103:
	xor	edi, edi
.LBB0_1104:
	test	r9b, 1
	je	.LBB0_1106
# %bb.1105:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1106:
	cmp	rsi, r10
	jne	.LBB0_142
	jmp	.LBB0_1395
.LBB0_1107:
	xor	edi, edi
.LBB0_1108:
	test	r9b, 1
	je	.LBB0_1110
# %bb.1109:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1110:
	cmp	rsi, r10
	jne	.LBB0_153
	jmp	.LBB0_1395
.LBB0_1111:
	xor	edi, edi
.LBB0_1112:
	test	r9b, 1
	je	.LBB0_1114
# %bb.1113:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1114:
	cmp	rsi, r10
	jne	.LBB0_164
	jmp	.LBB0_1395
.LBB0_1115:
	xor	edi, edi
.LBB0_1116:
	test	r9b, 1
	je	.LBB0_1118
# %bb.1117:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1118:
	cmp	rsi, r10
	jne	.LBB0_175
	jmp	.LBB0_1395
.LBB0_1119:
	xor	edi, edi
.LBB0_1120:
	test	r9b, 1
	je	.LBB0_1122
# %bb.1121:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1122:
	cmp	rsi, r10
	jne	.LBB0_184
	jmp	.LBB0_1395
.LBB0_1123:
	xor	edi, edi
.LBB0_1124:
	test	r9b, 1
	je	.LBB0_1126
# %bb.1125:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1126:
	cmp	rsi, r10
	jne	.LBB0_193
	jmp	.LBB0_1395
.LBB0_1127:
	xor	edi, edi
.LBB0_1128:
	test	r9b, 1
	je	.LBB0_1130
# %bb.1129:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1130:
	cmp	rsi, r10
	jne	.LBB0_212
	jmp	.LBB0_1395
.LBB0_1131:
	xor	edi, edi
.LBB0_1132:
	test	r9b, 1
	je	.LBB0_1134
# %bb.1133:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1134:
	cmp	rsi, r10
	jne	.LBB0_221
	jmp	.LBB0_1395
.LBB0_1135:
	xor	eax, eax
.LBB0_1136:
	test	r9b, 1
	je	.LBB0_1138
# %bb.1137:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_3] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_1138:
	cmp	rdi, r10
	jne	.LBB0_230
	jmp	.LBB0_1395
.LBB0_1139:
	xor	eax, eax
.LBB0_1140:
	test	r9b, 1
	je	.LBB0_1142
# %bb.1141:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_3] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_1142:
	cmp	rdi, r10
	jne	.LBB0_239
	jmp	.LBB0_1395
.LBB0_1143:
	xor	edi, edi
.LBB0_1144:
	test	r9b, 1
	je	.LBB0_1146
# %bb.1145:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1146:
	cmp	rsi, r10
	jne	.LBB0_248
	jmp	.LBB0_1395
.LBB0_1147:
	xor	edi, edi
.LBB0_1148:
	test	r9b, 1
	je	.LBB0_1150
# %bb.1149:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1150:
	cmp	rsi, r10
	jne	.LBB0_257
	jmp	.LBB0_1395
.LBB0_1151:
	xor	edi, edi
.LBB0_1152:
	test	r9b, 1
	je	.LBB0_1154
# %bb.1153:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1154:
	cmp	rsi, r10
	jne	.LBB0_276
	jmp	.LBB0_1395
.LBB0_1155:
	xor	edi, edi
.LBB0_1156:
	test	r9b, 1
	je	.LBB0_1158
# %bb.1157:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1158:
	cmp	rsi, r10
	jne	.LBB0_285
	jmp	.LBB0_1395
.LBB0_1159:
	xor	edi, edi
.LBB0_1160:
	test	r9b, 1
	je	.LBB0_1162
# %bb.1161:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1162:
	cmp	rsi, r10
	jne	.LBB0_302
	jmp	.LBB0_1395
.LBB0_1163:
	xor	edi, edi
.LBB0_1164:
	test	r9b, 1
	je	.LBB0_1166
# %bb.1165:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1166:
	cmp	rsi, r10
	jne	.LBB0_309
	jmp	.LBB0_1395
.LBB0_1167:
	xor	edi, edi
.LBB0_1168:
	test	r9b, 1
	je	.LBB0_1170
# %bb.1169:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1170:
	cmp	rsi, r10
	jne	.LBB0_316
	jmp	.LBB0_1395
.LBB0_1171:
	xor	edi, edi
.LBB0_1172:
	test	r9b, 1
	je	.LBB0_1174
# %bb.1173:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_1174:
	cmp	rsi, r10
	jne	.LBB0_323
	jmp	.LBB0_1395
.LBB0_1175:
	xor	edi, edi
.LBB0_1176:
	test	r9b, 1
	je	.LBB0_1178
# %bb.1177:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1178:
	cmp	rsi, r10
	jne	.LBB0_342
	jmp	.LBB0_1395
.LBB0_1179:
	xor	edi, edi
.LBB0_1180:
	test	r9b, 1
	je	.LBB0_1182
# %bb.1181:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1182:
	cmp	rsi, r10
	jne	.LBB0_349
	jmp	.LBB0_1395
.LBB0_1183:
	xor	edi, edi
.LBB0_1184:
	test	r9b, 1
	je	.LBB0_1186
# %bb.1185:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1186:
	cmp	rsi, r10
	jne	.LBB0_356
	jmp	.LBB0_1395
.LBB0_1187:
	xor	edi, edi
.LBB0_1188:
	test	r9b, 1
	je	.LBB0_1190
# %bb.1189:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1190:
	cmp	rsi, r10
	jne	.LBB0_363
	jmp	.LBB0_1395
.LBB0_1191:
	xor	edi, edi
.LBB0_1192:
	test	r9b, 1
	je	.LBB0_1194
# %bb.1193:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1194:
	cmp	rsi, r10
	jne	.LBB0_370
	jmp	.LBB0_1395
.LBB0_1195:
	xor	edi, edi
.LBB0_1196:
	test	r9b, 1
	je	.LBB0_1198
# %bb.1197:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1198:
	cmp	rsi, r10
	jne	.LBB0_377
	jmp	.LBB0_1395
.LBB0_1199:
	xor	edi, edi
.LBB0_1200:
	test	r9b, 1
	je	.LBB0_1202
# %bb.1201:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1202:
	cmp	rsi, r10
	jne	.LBB0_384
	jmp	.LBB0_1395
.LBB0_1203:
	xor	edi, edi
.LBB0_1204:
	test	r9b, 1
	je	.LBB0_1206
# %bb.1205:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_1206:
	cmp	rsi, r10
	jne	.LBB0_391
	jmp	.LBB0_1395
.LBB0_1207:
	xor	edi, edi
.LBB0_1208:
	test	r9b, 1
	je	.LBB0_1210
# %bb.1209:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1210:
	cmp	rsi, r10
	jne	.LBB0_398
	jmp	.LBB0_1395
.LBB0_1211:
	xor	edi, edi
.LBB0_1212:
	test	r9b, 1
	je	.LBB0_1214
# %bb.1213:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1214:
	cmp	rsi, r10
	jne	.LBB0_405
	jmp	.LBB0_1395
.LBB0_1215:
	xor	edi, edi
.LBB0_1216:
	test	r9b, 1
	je	.LBB0_1218
# %bb.1217:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1218:
	cmp	rsi, r10
	jne	.LBB0_412
	jmp	.LBB0_1395
.LBB0_1219:
	xor	edi, edi
.LBB0_1220:
	test	r9b, 1
	je	.LBB0_1222
# %bb.1221:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1222:
	cmp	rsi, r10
	jne	.LBB0_419
	jmp	.LBB0_1395
.LBB0_1223:
	xor	edi, edi
.LBB0_1224:
	test	r9b, 1
	je	.LBB0_1226
# %bb.1225:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1226:
	cmp	rsi, r10
	jne	.LBB0_438
	jmp	.LBB0_1395
.LBB0_1227:
	xor	edi, edi
.LBB0_1228:
	test	r9b, 1
	je	.LBB0_1230
# %bb.1229:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1230:
	cmp	rsi, r10
	jne	.LBB0_445
	jmp	.LBB0_1395
.LBB0_1231:
	xor	edi, edi
.LBB0_1232:
	test	r9b, 1
	je	.LBB0_1234
# %bb.1233:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1234:
	cmp	rsi, r10
	jne	.LBB0_452
	jmp	.LBB0_1395
.LBB0_1235:
	xor	edi, edi
.LBB0_1236:
	test	r9b, 1
	je	.LBB0_1238
# %bb.1237:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1238:
	cmp	rsi, r10
	jne	.LBB0_459
	jmp	.LBB0_1395
.LBB0_1239:
	xor	edi, edi
.LBB0_1240:
	test	r9b, 1
	je	.LBB0_1242
# %bb.1241:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1242:
	cmp	rsi, r10
	jne	.LBB0_469
	jmp	.LBB0_1395
.LBB0_1243:
	xor	edi, edi
.LBB0_1244:
	test	r9b, 1
	je	.LBB0_1246
# %bb.1245:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1246:
	cmp	rsi, r10
	jne	.LBB0_479
	jmp	.LBB0_1395
.LBB0_1247:
	xor	edi, edi
.LBB0_1248:
	test	r9b, 1
	je	.LBB0_1250
# %bb.1249:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1250:
	cmp	rsi, r10
	jne	.LBB0_486
	jmp	.LBB0_1395
.LBB0_1251:
	xor	edi, edi
.LBB0_1252:
	test	r9b, 1
	je	.LBB0_1254
# %bb.1253:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_1254:
	cmp	rsi, r10
	jne	.LBB0_493
	jmp	.LBB0_1395
.LBB0_1255:
	xor	edi, edi
.LBB0_1256:
	test	r9b, 1
	je	.LBB0_1258
# %bb.1257:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1258:
	cmp	rsi, r10
	jne	.LBB0_506
	jmp	.LBB0_1395
.LBB0_1259:
	xor	edi, edi
.LBB0_1260:
	test	r9b, 1
	je	.LBB0_1262
# %bb.1261:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_1262:
	cmp	rsi, r10
	jne	.LBB0_513
	jmp	.LBB0_1395
.LBB0_1263:
	xor	eax, eax
.LBB0_1264:
	test	r9b, 1
	je	.LBB0_1266
# %bb.1265:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_3] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_1266:
	cmp	rdi, r10
	jne	.LBB0_520
	jmp	.LBB0_1395
.LBB0_1267:
	xor	eax, eax
.LBB0_1268:
	test	r9b, 1
	je	.LBB0_1270
# %bb.1269:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_3] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_1270:
	cmp	rdi, r10
	jne	.LBB0_527
	jmp	.LBB0_1395
.LBB0_1271:
	xor	edi, edi
.LBB0_1272:
	test	r9b, 1
	je	.LBB0_1274
# %bb.1273:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1274:
	cmp	rsi, r10
	jne	.LBB0_534
	jmp	.LBB0_1395
.LBB0_1275:
	xor	edi, edi
.LBB0_1276:
	test	r9b, 1
	je	.LBB0_1278
# %bb.1277:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1278:
	cmp	rsi, r10
	jne	.LBB0_541
	jmp	.LBB0_1395
.LBB0_1279:
	xor	edi, edi
.LBB0_1280:
	test	r9b, 1
	je	.LBB0_1282
# %bb.1281:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1282:
	cmp	rsi, r10
	jne	.LBB0_554
	jmp	.LBB0_1395
.LBB0_1283:
	xor	edi, edi
.LBB0_1284:
	test	r9b, 1
	je	.LBB0_1286
# %bb.1285:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1286:
	cmp	rsi, r10
	jne	.LBB0_561
	jmp	.LBB0_1395
.LBB0_1287:
	xor	edi, edi
.LBB0_1288:
	test	r9b, 1
	je	.LBB0_1290
# %bb.1289:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1290:
	cmp	rsi, r10
	jne	.LBB0_568
	jmp	.LBB0_1395
.LBB0_1291:
	xor	edi, edi
.LBB0_1292:
	test	r9b, 1
	je	.LBB0_1294
# %bb.1293:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_1294:
	cmp	rsi, r10
	jne	.LBB0_575
	jmp	.LBB0_1395
.LBB0_1295:
	xor	edi, edi
.LBB0_1296:
	test	r9b, 1
	je	.LBB0_1298
# %bb.1297:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_0] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1298:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1299
.LBB0_1303:
	xor	edi, edi
.LBB0_1304:
	test	r9b, 1
	je	.LBB0_1306
# %bb.1305:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_0] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_1306:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1307
.LBB0_1311:
	xor	esi, esi
.LBB0_1312:
	test	r9b, 1
	je	.LBB0_1314
# %bb.1313:
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI0_2] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB0_1314:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1315
.LBB0_1319:
	xor	esi, esi
.LBB0_1320:
	test	r9b, 1
	je	.LBB0_1322
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI0_2] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB0_1322:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1323
.LBB0_1327:
	xor	esi, esi
.LBB0_1328:
	test	r9b, 1
	je	.LBB0_1330
# %bb.1329:
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
.LBB0_1330:
	cmp	rcx, rax
	je	.LBB0_1395
	jmp	.LBB0_1331
.LBB0_1335:
	xor	esi, esi
.LBB0_1336:
	test	r9b, 1
	je	.LBB0_1338
# %bb.1337:
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
.LBB0_1338:
	cmp	rcx, rax
	je	.LBB0_1395
	jmp	.LBB0_1339
.LBB0_1343:
	xor	esi, esi
.LBB0_1344:
	test	r9b, 1
	je	.LBB0_1346
# %bb.1345:
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
.LBB0_1346:
	cmp	rcx, rax
	je	.LBB0_1395
	jmp	.LBB0_1347
.LBB0_1351:
	xor	edi, edi
.LBB0_1352:
	test	r9b, 1
	je	.LBB0_1354
# %bb.1353:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_1] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1354:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1355
.LBB0_1361:
	xor	esi, esi
.LBB0_1362:
	test	r9b, 1
	je	.LBB0_1364
# %bb.1363:
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
.LBB0_1364:
	cmp	rcx, rax
	je	.LBB0_1395
	jmp	.LBB0_1365
.LBB0_1369:
	xor	edi, edi
.LBB0_1370:
	test	r9b, 1
	je	.LBB0_1372
# %bb.1371:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI0_1] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1372:
	cmp	rcx, r10
	je	.LBB0_1395
	jmp	.LBB0_1373
.LBB0_1379:
	xor	edi, edi
.LBB0_1380:
	test	r9b, 1
	je	.LBB0_1382
# %bb.1381:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1382:
	cmp	rcx, rax
	je	.LBB0_1395
	jmp	.LBB0_1383
.LBB0_1387:
	xor	edi, edi
.LBB0_1388:
	test	r9b, 1
	je	.LBB0_1390
# %bb.1389:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_1390:
	cmp	rcx, rax
	je	.LBB0_1395
	jmp	.LBB0_1391
.Lfunc_end0:
	.size	arithmetic_sse4, .Lfunc_end0-arithmetic_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_arr_scalar_sse4
.LCPI1_0:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI1_1:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI1_2:
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
.LCPI1_3:
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
	jne	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.9:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.10:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_11
# %bb.355:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_603
# %bb.356:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_603
.LBB1_11:
	xor	esi, esi
.LBB1_907:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_909
.LBB1_908:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_908
.LBB1_909:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_910:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_910
	jmp	.LBB1_1451
.LBB1_12:
	cmp	sil, 6
	jg	.LBB1_33
# %bb.13:
	cmp	sil, 5
	je	.LBB1_51
# %bb.14:
	cmp	sil, 6
	jne	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.20:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.21:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_22
# %bb.358:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_606
# %bb.359:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_606
.LBB1_22:
	xor	esi, esi
.LBB1_915:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_917
.LBB1_916:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_916
.LBB1_917:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_918:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_918
	jmp	.LBB1_1451
.LBB1_23:
	cmp	sil, 2
	je	.LBB1_59
# %bb.24:
	cmp	sil, 4
	jne	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.30:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.31:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_32
# %bb.361:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_609
# %bb.362:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_609
.LBB1_32:
	xor	ecx, ecx
.LBB1_829:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_831
.LBB1_830:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_830
.LBB1_831:
	cmp	rax, 3
	jb	.LBB1_1451
.LBB1_832:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_832
	jmp	.LBB1_1451
.LBB1_33:
	cmp	sil, 7
	je	.LBB1_67
# %bb.34:
	cmp	sil, 9
	jne	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.40:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.41:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_42
# %bb.364:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_611
# %bb.365:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_611
.LBB1_42:
	xor	ecx, ecx
.LBB1_839:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_841
.LBB1_840:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_840
.LBB1_841:
	cmp	rax, 3
	jb	.LBB1_1451
.LBB1_842:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_842
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.48:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.49:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_50
# %bb.367:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_613
# %bb.368:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_613
.LBB1_50:
	xor	esi, esi
.LBB1_923:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_925
.LBB1_924:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_924
.LBB1_925:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_926:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_926
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.56:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.57:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_58
# %bb.370:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_616
# %bb.371:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_616
.LBB1_58:
	xor	esi, esi
.LBB1_931:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_933
.LBB1_932:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_932
.LBB1_933:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_934:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_934
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.64:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.65:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_66
# %bb.373:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_619
# %bb.374:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_619
.LBB1_66:
	xor	esi, esi
.LBB1_939:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_941
.LBB1_940:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_940
.LBB1_941:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_942:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_942
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.72:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.73:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_74
# %bb.376:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_622
# %bb.377:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_622
.LBB1_74:
	xor	esi, esi
.LBB1_947:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_949
.LBB1_948:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_948
.LBB1_949:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_950:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_950
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.79:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.80:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_81
# %bb.379:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_625
# %bb.380:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_625
.LBB1_81:
	xor	ecx, ecx
.LBB1_955:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_957
.LBB1_956:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_956
.LBB1_957:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_958:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_958
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.86:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.87:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_88
# %bb.382:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_628
# %bb.383:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_628
.LBB1_88:
	xor	ecx, ecx
.LBB1_963:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_965
.LBB1_964:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_964
.LBB1_965:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_966:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_966
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.93:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.94:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_95
# %bb.385:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_631
# %bb.386:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_631
.LBB1_95:
	xor	ecx, ecx
.LBB1_971:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_973
.LBB1_972:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_972
.LBB1_973:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_974:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_974
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.100:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.101:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_102
# %bb.388:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_634
# %bb.389:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_634
.LBB1_102:
	xor	ecx, ecx
.LBB1_979:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB1_981
.LBB1_980:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB1_980
.LBB1_981:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_982:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_982
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.107:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.108:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_109
# %bb.391:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_637
# %bb.392:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_637
.LBB1_109:
	xor	ecx, ecx
.LBB1_987:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_989
.LBB1_988:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_988
.LBB1_989:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_990:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_990
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.114:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.115:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_116
# %bb.394:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_640
# %bb.395:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_640
.LBB1_116:
	xor	ecx, ecx
.LBB1_995:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_997
.LBB1_996:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_996
.LBB1_997:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_998:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_998
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.121:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.122:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_123
# %bb.397:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_643
# %bb.398:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_643
.LBB1_123:
	xor	ecx, ecx
.LBB1_1003:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1005
.LBB1_1004:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1004
.LBB1_1005:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1006:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1006
	jmp	.LBB1_1451
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
	jne	.LBB1_1451
# %bb.128:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.129:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_130
# %bb.400:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_646
# %bb.401:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_646
.LBB1_130:
	xor	ecx, ecx
.LBB1_1011:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1013
.LBB1_1012:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1012
.LBB1_1013:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1014:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1014
	jmp	.LBB1_1451
.LBB1_131:
	cmp	edi, 2
	je	.LBB1_307
# %bb.132:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.133:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.134:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_135
# %bb.403:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_649
# %bb.404:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_649
.LBB1_135:
	xor	esi, esi
.LBB1_1019:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1021
.LBB1_1020:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1020
.LBB1_1021:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1022:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1022
	jmp	.LBB1_1451
.LBB1_136:
	cmp	edi, 2
	je	.LBB1_310
# %bb.137:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.138:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.139:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_140
# %bb.406:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_652
# %bb.407:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_652
.LBB1_140:
	xor	esi, esi
.LBB1_1027:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1029
.LBB1_1028:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1028
.LBB1_1029:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1030:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1030
	jmp	.LBB1_1451
.LBB1_141:
	cmp	edi, 2
	je	.LBB1_313
# %bb.142:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.143:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.144:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_145
# %bb.409:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_655
# %bb.410:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_655
.LBB1_145:
	xor	ecx, ecx
.LBB1_1035:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB1_1037
# %bb.1036:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_1037:
	add	rsi, r10
	je	.LBB1_1451
.LBB1_1038:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1038
	jmp	.LBB1_1451
.LBB1_146:
	cmp	edi, 2
	je	.LBB1_316
# %bb.147:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.148:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.149:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_150
# %bb.412:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_658
# %bb.413:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_658
.LBB1_150:
	xor	ecx, ecx
.LBB1_1043:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB1_1045
# %bb.1044:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB1_1045:
	add	rsi, r10
	je	.LBB1_1451
.LBB1_1046:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1046
	jmp	.LBB1_1451
.LBB1_151:
	cmp	edi, 2
	je	.LBB1_319
# %bb.152:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.153:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.154:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_155
# %bb.415:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_661
# %bb.416:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_661
.LBB1_155:
	xor	esi, esi
.LBB1_1051:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1053
.LBB1_1052:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1052
.LBB1_1053:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1054:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1054
	jmp	.LBB1_1451
.LBB1_156:
	cmp	edi, 2
	je	.LBB1_322
# %bb.157:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.158:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.159:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_160
# %bb.418:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_664
# %bb.419:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_664
.LBB1_160:
	xor	esi, esi
.LBB1_1059:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1061
.LBB1_1060:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1060
.LBB1_1061:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1062:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1062
	jmp	.LBB1_1451
.LBB1_161:
	cmp	edi, 2
	je	.LBB1_325
# %bb.162:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.163:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.164:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_165
# %bb.421:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_667
# %bb.422:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_667
.LBB1_165:
	xor	edi, edi
.LBB1_1067:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1069
.LBB1_1068:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1068
.LBB1_1069:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1070:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1070
	jmp	.LBB1_1451
.LBB1_166:
	cmp	edi, 2
	je	.LBB1_328
# %bb.167:
	cmp	edi, 3
	jne	.LBB1_1451
# %bb.168:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.169:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_170
# %bb.424:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_670
# %bb.425:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_670
.LBB1_170:
	xor	edi, edi
.LBB1_1075:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1077
.LBB1_1076:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1076
.LBB1_1077:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1078:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1078
	jmp	.LBB1_1451
.LBB1_171:
	cmp	edi, 7
	je	.LBB1_331
# %bb.172:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.173:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.174:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
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
.LBB1_1083:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1085
.LBB1_1084:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1084
.LBB1_1085:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1086:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1086
	jmp	.LBB1_1451
.LBB1_176:
	cmp	edi, 7
	je	.LBB1_334
# %bb.177:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.178:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.179:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
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
.LBB1_1091:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1093
.LBB1_1092:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1092
.LBB1_1093:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1094:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1094
	jmp	.LBB1_1451
.LBB1_181:
	cmp	edi, 7
	je	.LBB1_337
# %bb.182:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.183:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.184:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_185
# %bb.433:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_679
# %bb.434:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_679
.LBB1_185:
	xor	ecx, ecx
.LBB1_849:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_851
.LBB1_850:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_850
.LBB1_851:
	cmp	rax, 3
	jb	.LBB1_1451
.LBB1_852:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_852
	jmp	.LBB1_1451
.LBB1_186:
	cmp	edi, 7
	je	.LBB1_340
# %bb.187:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.188:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.189:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_190
# %bb.436:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_681
# %bb.437:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_681
.LBB1_190:
	xor	ecx, ecx
.LBB1_859:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_861
.LBB1_860:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_860
.LBB1_861:
	cmp	rax, 3
	jb	.LBB1_1451
.LBB1_862:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_862
	jmp	.LBB1_1451
.LBB1_191:
	cmp	edi, 7
	je	.LBB1_343
# %bb.192:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.193:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.194:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_195
# %bb.439:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_683
# %bb.440:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_683
.LBB1_195:
	xor	esi, esi
.LBB1_1099:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1101
.LBB1_1100:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1100
.LBB1_1101:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1102:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1102
	jmp	.LBB1_1451
.LBB1_196:
	cmp	edi, 7
	je	.LBB1_346
# %bb.197:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.198:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.199:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_200
# %bb.442:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_686
# %bb.443:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_686
.LBB1_200:
	xor	esi, esi
.LBB1_1107:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1109
.LBB1_1108:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1108
.LBB1_1109:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1110:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1110
	jmp	.LBB1_1451
.LBB1_201:
	cmp	edi, 7
	je	.LBB1_349
# %bb.202:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.203:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.204:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_445
# %bb.205:
	xor	edi, edi
	jmp	.LBB1_447
.LBB1_206:
	cmp	edi, 7
	je	.LBB1_352
# %bb.207:
	cmp	edi, 8
	jne	.LBB1_1451
# %bb.208:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.209:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_450
# %bb.210:
	xor	edi, edi
	jmp	.LBB1_452
.LBB1_211:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.212:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_213
# %bb.455:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_689
# %bb.456:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_689
.LBB1_213:
	xor	esi, esi
.LBB1_1115:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1117
.LBB1_1116:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1116
.LBB1_1117:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1118:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1118
	jmp	.LBB1_1451
.LBB1_214:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.215:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_216
# %bb.458:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_692
# %bb.459:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_692
.LBB1_216:
	xor	esi, esi
.LBB1_1123:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1125
.LBB1_1124:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1124
.LBB1_1125:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1126:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1126
	jmp	.LBB1_1451
.LBB1_217:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.218:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_219
# %bb.461:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_695
# %bb.462:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_695
.LBB1_219:
	xor	esi, esi
.LBB1_1131:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1133
.LBB1_1132:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1132
.LBB1_1133:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1134:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1134
	jmp	.LBB1_1451
.LBB1_220:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.221:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_222
# %bb.464:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_698
# %bb.465:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_698
.LBB1_222:
	xor	esi, esi
.LBB1_1139:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1141
.LBB1_1140:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1140
.LBB1_1141:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1142:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1142
	jmp	.LBB1_1451
.LBB1_223:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.224:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_225
# %bb.467:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_701
# %bb.468:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_701
.LBB1_225:
	xor	ecx, ecx
.LBB1_869:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_871
.LBB1_870:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_870
.LBB1_871:
	cmp	rax, 3
	jb	.LBB1_1451
.LBB1_872:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_872
	jmp	.LBB1_1451
.LBB1_226:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.227:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_228
# %bb.470:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_703
# %bb.471:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_703
.LBB1_228:
	xor	ecx, ecx
.LBB1_1147:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1149
# %bb.1148:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1149:
	add	rsi, rax
	je	.LBB1_1451
.LBB1_1150:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1150
	jmp	.LBB1_1451
.LBB1_229:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.230:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_231
# %bb.473:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_706
# %bb.474:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_706
.LBB1_231:
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
	jb	.LBB1_1451
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
	jmp	.LBB1_1451
.LBB1_232:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.233:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_234
# %bb.476:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB1_708
# %bb.477:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB1_708
.LBB1_234:
	xor	ecx, ecx
.LBB1_1155:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1157
# %bb.1156:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB1_1157:
	add	rsi, rax
	je	.LBB1_1451
.LBB1_1158:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1158
	jmp	.LBB1_1451
.LBB1_235:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.236:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_237
# %bb.479:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_711
# %bb.480:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_711
.LBB1_237:
	xor	esi, esi
.LBB1_1163:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1165
.LBB1_1164:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1164
.LBB1_1165:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1166:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1166
	jmp	.LBB1_1451
.LBB1_238:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.239:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_240
# %bb.482:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_714
# %bb.483:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_714
.LBB1_240:
	xor	esi, esi
.LBB1_1171:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1173
.LBB1_1172:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1172
.LBB1_1173:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1174:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1174
	jmp	.LBB1_1451
.LBB1_241:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.242:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_243
# %bb.485:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_717
# %bb.486:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_717
.LBB1_243:
	xor	esi, esi
.LBB1_1179:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1181
.LBB1_1180:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1180
.LBB1_1181:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1182:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1182
	jmp	.LBB1_1451
.LBB1_244:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.245:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_246
# %bb.488:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_720
# %bb.489:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_720
.LBB1_246:
	xor	esi, esi
.LBB1_1187:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1189
.LBB1_1188:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1188
.LBB1_1189:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1190:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1190
	jmp	.LBB1_1451
.LBB1_247:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.248:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_249
# %bb.491:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_723
# %bb.492:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_723
.LBB1_249:
	xor	esi, esi
.LBB1_1195:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1197
.LBB1_1196:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1196
.LBB1_1197:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1198:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1198
	jmp	.LBB1_1451
.LBB1_250:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.251:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_252
# %bb.494:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_726
# %bb.495:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_726
.LBB1_252:
	xor	esi, esi
.LBB1_1203:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1205
.LBB1_1204:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1204
.LBB1_1205:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1206:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1206
	jmp	.LBB1_1451
.LBB1_253:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.254:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_255
# %bb.497:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_729
# %bb.498:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_729
.LBB1_255:
	xor	esi, esi
.LBB1_1211:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1213
.LBB1_1212:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1212
.LBB1_1213:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1214:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1214
	jmp	.LBB1_1451
.LBB1_256:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.257:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_258
# %bb.500:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_732
# %bb.501:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_732
.LBB1_258:
	xor	esi, esi
.LBB1_1219:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1221
.LBB1_1220:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1220
.LBB1_1221:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1222:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1222
	jmp	.LBB1_1451
.LBB1_259:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.260:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_261
# %bb.503:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_735
# %bb.504:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_735
.LBB1_261:
	xor	esi, esi
.LBB1_1227:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1229
.LBB1_1228:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1228
.LBB1_1229:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1230:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1230
	jmp	.LBB1_1451
.LBB1_262:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.263:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_264
# %bb.506:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_738
# %bb.507:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_738
.LBB1_264:
	xor	ecx, ecx
.LBB1_1235:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1237
.LBB1_1236:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1236
.LBB1_1237:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1238:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1238
	jmp	.LBB1_1451
.LBB1_265:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.266:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_267
# %bb.509:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_741
# %bb.510:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_741
.LBB1_267:
	xor	esi, esi
.LBB1_1243:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1245
.LBB1_1244:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1244
.LBB1_1245:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1246:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1246
	jmp	.LBB1_1451
.LBB1_268:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.269:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_270
# %bb.512:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_744
# %bb.513:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_744
.LBB1_270:
	xor	ecx, ecx
.LBB1_1251:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1253
.LBB1_1252:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1252
.LBB1_1253:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1254:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1254
	jmp	.LBB1_1451
.LBB1_271:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.272:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_273
# %bb.515:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_747
# %bb.516:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_747
.LBB1_273:
	xor	ecx, ecx
.LBB1_1259:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1261
# %bb.1260:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_1261:
	add	rsi, rax
	je	.LBB1_1451
.LBB1_1262:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1262
	jmp	.LBB1_1451
.LBB1_274:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.275:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_276
# %bb.518:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_750
# %bb.519:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_750
.LBB1_276:
	xor	ecx, ecx
.LBB1_1267:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1270
# %bb.1268:
	mov	esi, 2147483647
.LBB1_1269:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1269
.LBB1_1270:
	cmp	r9, 3
	jb	.LBB1_1451
# %bb.1271:
	mov	esi, 2147483647
.LBB1_1272:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1272
	jmp	.LBB1_1451
.LBB1_277:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.278:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_279
# %bb.521:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_753
# %bb.522:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_753
.LBB1_279:
	xor	ecx, ecx
.LBB1_1277:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1279
# %bb.1278:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB1_1279:
	add	rsi, rax
	je	.LBB1_1451
.LBB1_1280:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1280
	jmp	.LBB1_1451
.LBB1_280:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.281:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_282
# %bb.524:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_756
# %bb.525:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_756
.LBB1_282:
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
	jb	.LBB1_1451
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
	jmp	.LBB1_1451
.LBB1_283:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.284:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_285
# %bb.527:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_759
# %bb.528:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_759
.LBB1_285:
	xor	esi, esi
.LBB1_1295:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1297
.LBB1_1296:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1296
.LBB1_1297:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1298:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1298
	jmp	.LBB1_1451
.LBB1_286:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.287:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_288
# %bb.530:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_762
# %bb.531:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_762
.LBB1_288:
	xor	ecx, ecx
.LBB1_1303:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1305
.LBB1_1304:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1304
.LBB1_1305:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1306:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1306
	jmp	.LBB1_1451
.LBB1_289:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.290:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_291
# %bb.533:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_765
# %bb.534:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_765
.LBB1_291:
	xor	esi, esi
.LBB1_1311:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1313
.LBB1_1312:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1312
.LBB1_1313:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1314:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1314
	jmp	.LBB1_1451
.LBB1_292:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.293:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_294
# %bb.536:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_768
# %bb.537:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_768
.LBB1_294:
	xor	ecx, ecx
.LBB1_1319:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1321
.LBB1_1320:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1320
.LBB1_1321:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1322:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1322
	jmp	.LBB1_1451
.LBB1_295:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.296:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_539
# %bb.297:
	xor	edi, edi
	jmp	.LBB1_541
.LBB1_298:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.299:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_300
# %bb.544:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_771
# %bb.545:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_771
.LBB1_300:
	xor	ecx, ecx
.LBB1_1327:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1329
.LBB1_1328:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1328
.LBB1_1329:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1330:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1330
	jmp	.LBB1_1451
.LBB1_301:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.302:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_547
# %bb.303:
	xor	edi, edi
	jmp	.LBB1_549
.LBB1_304:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.305:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_306
# %bb.552:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_774
# %bb.553:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_774
.LBB1_306:
	xor	ecx, ecx
.LBB1_1335:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1337
.LBB1_1336:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1336
.LBB1_1337:
	cmp	rsi, 3
	jb	.LBB1_1451
.LBB1_1338:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1338
	jmp	.LBB1_1451
.LBB1_307:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.308:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_309
# %bb.555:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_777
# %bb.556:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_777
.LBB1_309:
	xor	esi, esi
.LBB1_1343:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1345
.LBB1_1344:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1344
.LBB1_1345:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1346:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1346
	jmp	.LBB1_1451
.LBB1_310:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.311:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_312
# %bb.558:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_780
# %bb.559:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_780
.LBB1_312:
	xor	esi, esi
.LBB1_1351:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1353
.LBB1_1352:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1352
.LBB1_1353:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1354:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1354
	jmp	.LBB1_1451
.LBB1_313:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.314:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_315
# %bb.561:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_783
# %bb.562:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_783
.LBB1_315:
	xor	ecx, ecx
.LBB1_889:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_891
.LBB1_890:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_890
.LBB1_891:
	cmp	rdi, 3
	jb	.LBB1_1451
.LBB1_892:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_892
	jmp	.LBB1_1451
.LBB1_316:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.317:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_318
# %bb.564:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_785
# %bb.565:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_785
.LBB1_318:
	xor	ecx, ecx
.LBB1_899:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_901
.LBB1_900:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB1_900
.LBB1_901:
	cmp	rdi, 3
	jb	.LBB1_1451
.LBB1_902:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_902
	jmp	.LBB1_1451
.LBB1_319:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.320:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_321
# %bb.567:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_787
# %bb.568:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_787
.LBB1_321:
	xor	esi, esi
.LBB1_1359:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1361
.LBB1_1360:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1360
.LBB1_1361:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1362:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1362
	jmp	.LBB1_1451
.LBB1_322:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.323:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_324
# %bb.570:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_790
# %bb.571:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_790
.LBB1_324:
	xor	esi, esi
.LBB1_1367:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1369
.LBB1_1368:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1368
.LBB1_1369:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1370:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1370
	jmp	.LBB1_1451
.LBB1_325:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.326:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_327
# %bb.573:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_793
# %bb.574:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_793
.LBB1_327:
	xor	edi, edi
.LBB1_1375:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1377
.LBB1_1376:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1376
.LBB1_1377:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1378:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1378
	jmp	.LBB1_1451
.LBB1_328:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.329:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_330
# %bb.576:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_796
# %bb.577:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_796
.LBB1_330:
	xor	edi, edi
.LBB1_1383:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_1385
.LBB1_1384:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_1384
.LBB1_1385:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1386:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1386
	jmp	.LBB1_1451
.LBB1_331:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.332:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_333
# %bb.579:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_799
# %bb.580:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_799
.LBB1_333:
	xor	esi, esi
.LBB1_1391:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1393
.LBB1_1392:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1392
.LBB1_1393:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1394:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1394
	jmp	.LBB1_1451
.LBB1_334:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.335:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_336
# %bb.582:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_802
# %bb.583:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_802
.LBB1_336:
	xor	esi, esi
.LBB1_1399:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1401
.LBB1_1400:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1400
.LBB1_1401:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1402:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1402
	jmp	.LBB1_1451
.LBB1_337:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.338:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_339
# %bb.585:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_805
# %bb.586:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_805
.LBB1_339:
	xor	ecx, ecx
.LBB1_1407:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1409
# %bb.1408:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_1409:
	add	rsi, rax
	je	.LBB1_1451
.LBB1_1410:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1410
	jmp	.LBB1_1451
.LBB1_340:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.341:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_342
# %bb.588:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_808
# %bb.589:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_808
.LBB1_342:
	xor	ecx, ecx
.LBB1_1415:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB1_1417
# %bb.1416:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB1_1417:
	add	rsi, rax
	je	.LBB1_1451
.LBB1_1418:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1418
	jmp	.LBB1_1451
.LBB1_343:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.344:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_345
# %bb.591:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_811
# %bb.592:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_811
.LBB1_345:
	xor	esi, esi
.LBB1_1423:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1425
.LBB1_1424:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1424
.LBB1_1425:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1426:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1426
	jmp	.LBB1_1451
.LBB1_346:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.347:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_348
# %bb.594:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_814
# %bb.595:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_814
.LBB1_348:
	xor	esi, esi
.LBB1_1431:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1433
.LBB1_1432:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1432
.LBB1_1433:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1434:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1434
	jmp	.LBB1_1451
.LBB1_349:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.350:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_351
# %bb.597:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_817
# %bb.598:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_817
.LBB1_351:
	xor	esi, esi
.LBB1_1439:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1441
.LBB1_1440:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1440
.LBB1_1441:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1442:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1442
	jmp	.LBB1_1451
.LBB1_352:
	test	r9d, r9d
	jle	.LBB1_1451
# %bb.353:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_354
# %bb.600:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_820
# %bb.601:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_820
.LBB1_354:
	xor	esi, esi
.LBB1_1447:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1449
.LBB1_1448:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1448
.LBB1_1449:
	cmp	r9, 3
	jb	.LBB1_1451
.LBB1_1450:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1450
	jmp	.LBB1_1451
.LBB1_445:
	and	esi, -4
	xor	edi, edi
.LBB1_446:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_446
.LBB1_447:
	test	r9, r9
	je	.LBB1_1451
# %bb.448:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_449:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_449
	jmp	.LBB1_1451
.LBB1_450:
	and	esi, -4
	xor	edi, edi
.LBB1_451:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_451
.LBB1_452:
	test	r9, r9
	je	.LBB1_1451
# %bb.453:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_454:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_454
	jmp	.LBB1_1451
.LBB1_539:
	and	esi, -4
	xor	edi, edi
.LBB1_540:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_540
.LBB1_541:
	test	r9, r9
	je	.LBB1_1451
# %bb.542:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_543:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_543
	jmp	.LBB1_1451
.LBB1_547:
	and	esi, -4
	xor	edi, edi
.LBB1_548:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_548
.LBB1_549:
	test	r9, r9
	je	.LBB1_1451
# %bb.550:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_551:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_551
.LBB1_1451:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB1_603:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_903
# %bb.604:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_605
	jmp	.LBB1_904
.LBB1_606:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_911
# %bb.607:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_608
	jmp	.LBB1_912
.LBB1_609:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB1_823
# %bb.610:
	xor	eax, eax
	jmp	.LBB1_825
.LBB1_611:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB1_833
# %bb.612:
	xor	eax, eax
	jmp	.LBB1_835
.LBB1_613:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_919
# %bb.614:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_615:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_615
	jmp	.LBB1_920
.LBB1_616:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_927
# %bb.617:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_618:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_618
	jmp	.LBB1_928
.LBB1_619:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_935
# %bb.620:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_621:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_621
	jmp	.LBB1_936
.LBB1_622:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_943
# %bb.623:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_624:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_624
	jmp	.LBB1_944
.LBB1_625:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_951
# %bb.626:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_627:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_627
	jmp	.LBB1_952
.LBB1_628:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_959
# %bb.629:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_630:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_630
	jmp	.LBB1_960
.LBB1_631:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB1_967
# %bb.632:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB1_633:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_633
	jmp	.LBB1_968
.LBB1_634:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB1_975
# %bb.635:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_0] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB1_636:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_636
	jmp	.LBB1_976
.LBB1_637:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_983
# %bb.638:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_639:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_639
	jmp	.LBB1_984
.LBB1_640:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_991
# %bb.641:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_642:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_642
	jmp	.LBB1_992
.LBB1_643:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_999
# %bb.644:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_645:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_645
	jmp	.LBB1_1000
.LBB1_646:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1007
# %bb.647:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_648:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_648
	jmp	.LBB1_1008
.LBB1_649:
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
	je	.LBB1_1015
# %bb.650:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_651:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_651
	jmp	.LBB1_1016
.LBB1_652:
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
	je	.LBB1_1023
# %bb.653:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_654:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_654
	jmp	.LBB1_1024
.LBB1_655:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1031
# %bb.656:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI1_2] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB1_657:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_657
	jmp	.LBB1_1032
.LBB1_658:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1039
# %bb.659:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI1_2] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB1_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_660
	jmp	.LBB1_1040
.LBB1_661:
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
	je	.LBB1_1047
# %bb.662:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_663:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_663
	jmp	.LBB1_1048
.LBB1_664:
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
	je	.LBB1_1055
# %bb.665:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_666:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_666
	jmp	.LBB1_1056
.LBB1_667:
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
	je	.LBB1_1063
# %bb.668:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_669:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_669
	jmp	.LBB1_1064
.LBB1_670:
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
	je	.LBB1_1071
# %bb.671:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_672:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_672
	jmp	.LBB1_1072
.LBB1_673:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1079
# %bb.674:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_675:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_675
	jmp	.LBB1_1080
.LBB1_676:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1087
# %bb.677:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_678:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_678
	jmp	.LBB1_1088
.LBB1_679:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB1_843
# %bb.680:
	xor	eax, eax
	jmp	.LBB1_845
.LBB1_681:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB1_853
# %bb.682:
	xor	eax, eax
	jmp	.LBB1_855
.LBB1_683:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1095
# %bb.684:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_685:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_685
	jmp	.LBB1_1096
.LBB1_686:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1103
# %bb.687:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_688:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_688
	jmp	.LBB1_1104
.LBB1_689:
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
	je	.LBB1_1111
# %bb.690:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_691:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_691
	jmp	.LBB1_1112
.LBB1_692:
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
	je	.LBB1_1119
# %bb.693:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_694:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_694
	jmp	.LBB1_1120
.LBB1_695:
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
	je	.LBB1_1127
# %bb.696:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_697:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_697
	jmp	.LBB1_1128
.LBB1_698:
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
	je	.LBB1_1135
# %bb.699:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_700:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_700
	jmp	.LBB1_1136
.LBB1_701:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB1_863
# %bb.702:
	xor	eax, eax
	jmp	.LBB1_865
.LBB1_703:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1143
# %bb.704:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB1_705:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_705
	jmp	.LBB1_1144
.LBB1_706:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB1_873
# %bb.707:
	xor	eax, eax
	jmp	.LBB1_875
.LBB1_708:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1151
# %bb.709:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB1_710:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_710
	jmp	.LBB1_1152
.LBB1_711:
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
	je	.LBB1_1159
# %bb.712:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_713:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_713
	jmp	.LBB1_1160
.LBB1_714:
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
	je	.LBB1_1167
# %bb.715:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_716:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_716
	jmp	.LBB1_1168
.LBB1_717:
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
	je	.LBB1_1175
# %bb.718:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_719:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_719
	jmp	.LBB1_1176
.LBB1_720:
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
	je	.LBB1_1183
# %bb.721:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_722:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_722
	jmp	.LBB1_1184
.LBB1_723:
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
	je	.LBB1_1191
# %bb.724:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_725:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_725
	jmp	.LBB1_1192
.LBB1_726:
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
	je	.LBB1_1199
# %bb.727:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_728:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_728
	jmp	.LBB1_1200
.LBB1_729:
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
	je	.LBB1_1207
# %bb.730:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_731:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_731
	jmp	.LBB1_1208
.LBB1_732:
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
	je	.LBB1_1215
# %bb.733:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_734:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_734
	jmp	.LBB1_1216
.LBB1_735:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1223
# %bb.736:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_737:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_737
	jmp	.LBB1_1224
.LBB1_738:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1231
# %bb.739:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_740:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_740
	jmp	.LBB1_1232
.LBB1_741:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1239
# %bb.742:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_743:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_743
	jmp	.LBB1_1240
.LBB1_744:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1247
# %bb.745:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_746:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_746
	jmp	.LBB1_1248
.LBB1_747:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1255
# %bb.748:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_749:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_749
	jmp	.LBB1_1256
.LBB1_750:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1263
# %bb.751:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB1_752:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_752
	jmp	.LBB1_1264
.LBB1_753:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1273
# %bb.754:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_755:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_755
	jmp	.LBB1_1274
.LBB1_756:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1281
# %bb.757:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI1_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB1_758:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_758
	jmp	.LBB1_1282
.LBB1_759:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1291
# %bb.760:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_761:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_761
	jmp	.LBB1_1292
.LBB1_762:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1299
# %bb.763:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_764
	jmp	.LBB1_1300
.LBB1_765:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1307
# %bb.766:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_767:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_767
	jmp	.LBB1_1308
.LBB1_768:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1315
# %bb.769:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_770:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_770
	jmp	.LBB1_1316
.LBB1_771:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1323
# %bb.772:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_773:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_773
	jmp	.LBB1_1324
.LBB1_774:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1331
# %bb.775:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_776:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_776
	jmp	.LBB1_1332
.LBB1_777:
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
	je	.LBB1_1339
# %bb.778:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_779:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_779
	jmp	.LBB1_1340
.LBB1_780:
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
	je	.LBB1_1347
# %bb.781:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_782:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_782
	jmp	.LBB1_1348
.LBB1_783:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB1_883
# %bb.784:
	xor	edi, edi
	jmp	.LBB1_885
.LBB1_785:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB1_893
# %bb.786:
	xor	edi, edi
	jmp	.LBB1_895
.LBB1_787:
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
	je	.LBB1_1355
# %bb.788:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_789:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_789
	jmp	.LBB1_1356
.LBB1_790:
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
	je	.LBB1_1363
# %bb.791:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_792:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_792
	jmp	.LBB1_1364
.LBB1_793:
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
	je	.LBB1_1371
# %bb.794:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_795:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_795
	jmp	.LBB1_1372
.LBB1_796:
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
	je	.LBB1_1379
# %bb.797:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_798:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_798
	jmp	.LBB1_1380
.LBB1_799:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1387
# %bb.800:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_801:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_801
	jmp	.LBB1_1388
.LBB1_802:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1395
# %bb.803:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_804
	jmp	.LBB1_1396
.LBB1_805:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1403
# %bb.806:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_807:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_807
	jmp	.LBB1_1404
.LBB1_808:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1411
# %bb.809:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_810:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_810
	jmp	.LBB1_1412
.LBB1_811:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1419
# %bb.812:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_813:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_813
	jmp	.LBB1_1420
.LBB1_814:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1427
# %bb.815:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_816:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_816
	jmp	.LBB1_1428
.LBB1_817:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1435
# %bb.818:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_819:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_819
	jmp	.LBB1_1436
.LBB1_820:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1443
# %bb.821:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_822:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_822
	jmp	.LBB1_1444
.LBB1_823:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_824:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_824
.LBB1_825:
	test	rsi, rsi
	je	.LBB1_828
# %bb.826:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB1_827:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_827
.LBB1_828:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_829
.LBB1_833:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_834:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_834
.LBB1_835:
	test	rsi, rsi
	je	.LBB1_838
# %bb.836:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB1_837:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_837
.LBB1_838:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_839
.LBB1_843:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_844:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_844
.LBB1_845:
	test	rsi, rsi
	je	.LBB1_848
# %bb.846:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB1_847:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_847
.LBB1_848:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_849
.LBB1_853:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_854:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_854
.LBB1_855:
	test	rsi, rsi
	je	.LBB1_858
# %bb.856:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB1_857:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_857
.LBB1_858:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_859
.LBB1_863:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_864:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_864
.LBB1_865:
	test	rsi, rsi
	je	.LBB1_868
# %bb.866:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB1_867:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_867
.LBB1_868:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_869
.LBB1_873:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB1_874:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_874
.LBB1_875:
	test	rsi, rsi
	je	.LBB1_878
# %bb.876:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB1_877:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB1_877
.LBB1_878:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_879
.LBB1_883:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB1_884:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_884
.LBB1_885:
	test	rax, rax
	je	.LBB1_888
# %bb.886:
	add	rdi, 16
	neg	rax
.LBB1_887:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB1_887
.LBB1_888:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_889
.LBB1_893:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB1_894:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_894
.LBB1_895:
	test	rax, rax
	je	.LBB1_898
# %bb.896:
	add	rdi, 16
	neg	rax
.LBB1_897:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB1_897
.LBB1_898:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_899
.LBB1_903:
	xor	edi, edi
.LBB1_904:
	test	r9b, 1
	je	.LBB1_906
# %bb.905:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_906:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_907
.LBB1_911:
	xor	edi, edi
.LBB1_912:
	test	r9b, 1
	je	.LBB1_914
# %bb.913:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_914:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_915
.LBB1_919:
	xor	edi, edi
.LBB1_920:
	test	r9b, 1
	je	.LBB1_922
# %bb.921:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_922:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_923
.LBB1_927:
	xor	edi, edi
.LBB1_928:
	test	r9b, 1
	je	.LBB1_930
# %bb.929:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_930:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_931
.LBB1_935:
	xor	edi, edi
.LBB1_936:
	test	r9b, 1
	je	.LBB1_938
# %bb.937:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_938:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_939
.LBB1_943:
	xor	edi, edi
.LBB1_944:
	test	r9b, 1
	je	.LBB1_946
# %bb.945:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_946:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_947
.LBB1_951:
	xor	edi, edi
.LBB1_952:
	test	r9b, 1
	je	.LBB1_954
# %bb.953:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_954:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_955
.LBB1_959:
	xor	edi, edi
.LBB1_960:
	test	r9b, 1
	je	.LBB1_962
# %bb.961:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_962:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_963
.LBB1_967:
	xor	edi, edi
.LBB1_968:
	test	r9b, 1
	je	.LBB1_970
# %bb.969:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_0] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB1_970:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_971
.LBB1_975:
	xor	edi, edi
.LBB1_976:
	test	r9b, 1
	je	.LBB1_978
# %bb.977:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_0] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB1_978:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_979
.LBB1_983:
	xor	edi, edi
.LBB1_984:
	test	r9b, 1
	je	.LBB1_986
# %bb.985:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_986:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_987
.LBB1_991:
	xor	edi, edi
.LBB1_992:
	test	r9b, 1
	je	.LBB1_994
# %bb.993:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_994:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_995
.LBB1_999:
	xor	edi, edi
.LBB1_1000:
	test	r9b, 1
	je	.LBB1_1002
# %bb.1001:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1002:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1003
.LBB1_1007:
	xor	edi, edi
.LBB1_1008:
	test	r9b, 1
	je	.LBB1_1010
# %bb.1009:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_1010:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1011
.LBB1_1015:
	xor	edi, edi
.LBB1_1016:
	test	r9b, 1
	je	.LBB1_1018
# %bb.1017:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1018:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1019
.LBB1_1023:
	xor	edi, edi
.LBB1_1024:
	test	r9b, 1
	je	.LBB1_1026
# %bb.1025:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1026:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1027
.LBB1_1031:
	xor	esi, esi
.LBB1_1032:
	test	r9b, 1
	je	.LBB1_1034
# %bb.1033:
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_2] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB1_1034:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_1035
.LBB1_1039:
	xor	esi, esi
.LBB1_1040:
	test	r9b, 1
	je	.LBB1_1042
# %bb.1041:
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_2] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB1_1042:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_1043
.LBB1_1047:
	xor	edi, edi
.LBB1_1048:
	test	r9b, 1
	je	.LBB1_1050
# %bb.1049:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1050:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1051
.LBB1_1055:
	xor	edi, edi
.LBB1_1056:
	test	r9b, 1
	je	.LBB1_1058
# %bb.1057:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1058:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1059
.LBB1_1063:
	xor	eax, eax
.LBB1_1064:
	test	r9b, 1
	je	.LBB1_1066
# %bb.1065:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_1066:
	cmp	rdi, r10
	je	.LBB1_1451
	jmp	.LBB1_1067
.LBB1_1071:
	xor	eax, eax
.LBB1_1072:
	test	r9b, 1
	je	.LBB1_1074
# %bb.1073:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_1074:
	cmp	rdi, r10
	je	.LBB1_1451
	jmp	.LBB1_1075
.LBB1_1079:
	xor	edi, edi
.LBB1_1080:
	test	r9b, 1
	je	.LBB1_1082
# %bb.1081:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1082:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1083
.LBB1_1087:
	xor	edi, edi
.LBB1_1088:
	test	r9b, 1
	je	.LBB1_1090
# %bb.1089:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1090:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1091
.LBB1_1095:
	xor	edi, edi
.LBB1_1096:
	test	r9b, 1
	je	.LBB1_1098
# %bb.1097:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1098:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1099
.LBB1_1103:
	xor	edi, edi
.LBB1_1104:
	test	r9b, 1
	je	.LBB1_1106
# %bb.1105:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1106:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1107
.LBB1_1111:
	xor	edi, edi
.LBB1_1112:
	test	r9b, 1
	je	.LBB1_1114
# %bb.1113:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1114:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1115
.LBB1_1119:
	xor	edi, edi
.LBB1_1120:
	test	r9b, 1
	je	.LBB1_1122
# %bb.1121:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1122:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1123
.LBB1_1127:
	xor	edi, edi
.LBB1_1128:
	test	r9b, 1
	je	.LBB1_1130
# %bb.1129:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1130:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1131
.LBB1_1135:
	xor	edi, edi
.LBB1_1136:
	test	r9b, 1
	je	.LBB1_1138
# %bb.1137:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1138:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1139
.LBB1_1143:
	xor	esi, esi
.LBB1_1144:
	test	r9b, 1
	je	.LBB1_1146
# %bb.1145:
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
.LBB1_1146:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1147
.LBB1_1151:
	xor	esi, esi
.LBB1_1152:
	test	r9b, 1
	je	.LBB1_1154
# %bb.1153:
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
.LBB1_1154:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1155
.LBB1_1159:
	xor	edi, edi
.LBB1_1160:
	test	r9b, 1
	je	.LBB1_1162
# %bb.1161:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1162:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1163
.LBB1_1167:
	xor	edi, edi
.LBB1_1168:
	test	r9b, 1
	je	.LBB1_1170
# %bb.1169:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1170:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1171
.LBB1_1175:
	xor	edi, edi
.LBB1_1176:
	test	r9b, 1
	je	.LBB1_1178
# %bb.1177:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1178:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1179
.LBB1_1183:
	xor	edi, edi
.LBB1_1184:
	test	r9b, 1
	je	.LBB1_1186
# %bb.1185:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1186:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1187
.LBB1_1191:
	xor	edi, edi
.LBB1_1192:
	test	r9b, 1
	je	.LBB1_1194
# %bb.1193:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1194:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1195
.LBB1_1199:
	xor	edi, edi
.LBB1_1200:
	test	r9b, 1
	je	.LBB1_1202
# %bb.1201:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1202:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1203
.LBB1_1207:
	xor	edi, edi
.LBB1_1208:
	test	r9b, 1
	je	.LBB1_1210
# %bb.1209:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1210:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1211
.LBB1_1215:
	xor	edi, edi
.LBB1_1216:
	test	r9b, 1
	je	.LBB1_1218
# %bb.1217:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_1218:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1219
.LBB1_1223:
	xor	edi, edi
.LBB1_1224:
	test	r9b, 1
	je	.LBB1_1226
# %bb.1225:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1226:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1227
.LBB1_1231:
	xor	edi, edi
.LBB1_1232:
	test	r9b, 1
	je	.LBB1_1234
# %bb.1233:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1234:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1235
.LBB1_1239:
	xor	edi, edi
.LBB1_1240:
	test	r9b, 1
	je	.LBB1_1242
# %bb.1241:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1242:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1243
.LBB1_1247:
	xor	edi, edi
.LBB1_1248:
	test	r9b, 1
	je	.LBB1_1250
# %bb.1249:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1250:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1251
.LBB1_1255:
	xor	esi, esi
.LBB1_1256:
	test	r9b, 1
	je	.LBB1_1258
# %bb.1257:
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
.LBB1_1258:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1259
.LBB1_1263:
	xor	edi, edi
.LBB1_1264:
	test	r9b, 1
	je	.LBB1_1266
# %bb.1265:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_1] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1266:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_1267
.LBB1_1273:
	xor	esi, esi
.LBB1_1274:
	test	r9b, 1
	je	.LBB1_1276
# %bb.1275:
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
.LBB1_1276:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1277
.LBB1_1281:
	xor	edi, edi
.LBB1_1282:
	test	r9b, 1
	je	.LBB1_1284
# %bb.1283:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI1_1] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1284:
	cmp	rcx, r10
	je	.LBB1_1451
	jmp	.LBB1_1285
.LBB1_1291:
	xor	edi, edi
.LBB1_1292:
	test	r9b, 1
	je	.LBB1_1294
# %bb.1293:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1294:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1295
.LBB1_1299:
	xor	edi, edi
.LBB1_1300:
	test	r9b, 1
	je	.LBB1_1302
# %bb.1301:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1302:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1303
.LBB1_1307:
	xor	edi, edi
.LBB1_1308:
	test	r9b, 1
	je	.LBB1_1310
# %bb.1309:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_1310:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1311
.LBB1_1315:
	xor	edi, edi
.LBB1_1316:
	test	r9b, 1
	je	.LBB1_1318
# %bb.1317:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1318:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1319
.LBB1_1323:
	xor	edi, edi
.LBB1_1324:
	test	r9b, 1
	je	.LBB1_1326
# %bb.1325:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1326:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1327
.LBB1_1331:
	xor	edi, edi
.LBB1_1332:
	test	r9b, 1
	je	.LBB1_1334
# %bb.1333:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_1334:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1335
.LBB1_1339:
	xor	edi, edi
.LBB1_1340:
	test	r9b, 1
	je	.LBB1_1342
# %bb.1341:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1342:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1343
.LBB1_1347:
	xor	edi, edi
.LBB1_1348:
	test	r9b, 1
	je	.LBB1_1350
# %bb.1349:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1350:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1351
.LBB1_1355:
	xor	edi, edi
.LBB1_1356:
	test	r9b, 1
	je	.LBB1_1358
# %bb.1357:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1358:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1359
.LBB1_1363:
	xor	edi, edi
.LBB1_1364:
	test	r9b, 1
	je	.LBB1_1366
# %bb.1365:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1366:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1367
.LBB1_1371:
	xor	eax, eax
.LBB1_1372:
	test	r9b, 1
	je	.LBB1_1374
# %bb.1373:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_1374:
	cmp	rdi, r10
	je	.LBB1_1451
	jmp	.LBB1_1375
.LBB1_1379:
	xor	eax, eax
.LBB1_1380:
	test	r9b, 1
	je	.LBB1_1382
# %bb.1381:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_1382:
	cmp	rdi, r10
	je	.LBB1_1451
	jmp	.LBB1_1383
.LBB1_1387:
	xor	edi, edi
.LBB1_1388:
	test	r9b, 1
	je	.LBB1_1390
# %bb.1389:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1390:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1391
.LBB1_1395:
	xor	edi, edi
.LBB1_1396:
	test	r9b, 1
	je	.LBB1_1398
# %bb.1397:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1398:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1399
.LBB1_1403:
	xor	edi, edi
.LBB1_1404:
	test	r9b, 1
	je	.LBB1_1406
# %bb.1405:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1406:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1407
.LBB1_1411:
	xor	edi, edi
.LBB1_1412:
	test	r9b, 1
	je	.LBB1_1414
# %bb.1413:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB1_1414:
	cmp	rcx, rax
	je	.LBB1_1451
	jmp	.LBB1_1415
.LBB1_1419:
	xor	edi, edi
.LBB1_1420:
	test	r9b, 1
	je	.LBB1_1422
# %bb.1421:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1422:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1423
.LBB1_1427:
	xor	edi, edi
.LBB1_1428:
	test	r9b, 1
	je	.LBB1_1430
# %bb.1429:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1430:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1431
.LBB1_1435:
	xor	edi, edi
.LBB1_1436:
	test	r9b, 1
	je	.LBB1_1438
# %bb.1437:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1438:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1439
.LBB1_1443:
	xor	edi, edi
.LBB1_1444:
	test	r9b, 1
	je	.LBB1_1446
# %bb.1445:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1446:
	cmp	rsi, r10
	je	.LBB1_1451
	jmp	.LBB1_1447
.Lfunc_end1:
	.size	arithmetic_arr_scalar_sse4, .Lfunc_end1-arithmetic_arr_scalar_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_scalar_arr_sse4
.LCPI2_0:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI2_1:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI2_2:
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
.LCPI2_3:
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
	jne	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.9:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.10:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_11
# %bb.355:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_603
# %bb.356:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_603
.LBB2_11:
	xor	esi, esi
.LBB2_907:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_909
.LBB2_908:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_908
.LBB2_909:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_910:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_910
	jmp	.LBB2_1451
.LBB2_12:
	cmp	sil, 6
	jg	.LBB2_33
# %bb.13:
	cmp	sil, 5
	je	.LBB2_51
# %bb.14:
	cmp	sil, 6
	jne	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.20:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.21:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_22
# %bb.358:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_606
# %bb.359:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_606
.LBB2_22:
	xor	esi, esi
.LBB2_915:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_917
.LBB2_916:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_916
.LBB2_917:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_918:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_918
	jmp	.LBB2_1451
.LBB2_23:
	cmp	sil, 2
	je	.LBB2_59
# %bb.24:
	cmp	sil, 4
	jne	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.30:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.31:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_32
# %bb.361:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_609
# %bb.362:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB2_609
.LBB2_32:
	xor	ecx, ecx
.LBB2_829:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_831
.LBB2_830:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_830
.LBB2_831:
	cmp	rax, 3
	jb	.LBB2_1451
.LBB2_832:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_832
	jmp	.LBB2_1451
.LBB2_33:
	cmp	sil, 7
	je	.LBB2_67
# %bb.34:
	cmp	sil, 9
	jne	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.40:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.41:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_42
# %bb.364:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_611
# %bb.365:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB2_611
.LBB2_42:
	xor	ecx, ecx
.LBB2_839:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_841
.LBB2_840:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rcx]
	mov	dword ptr [r8 + 4*rcx], edi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_840
.LBB2_841:
	cmp	rax, 3
	jb	.LBB2_1451
.LBB2_842:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_842
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.48:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.49:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_50
# %bb.367:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_613
# %bb.368:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_613
.LBB2_50:
	xor	esi, esi
.LBB2_923:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_925
.LBB2_924:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_924
.LBB2_925:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_926:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_926
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.56:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.57:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_58
# %bb.370:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_616
# %bb.371:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_616
.LBB2_58:
	xor	esi, esi
.LBB2_931:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_933
.LBB2_932:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_932
.LBB2_933:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_934:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_934
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.64:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.65:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_66
# %bb.373:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_619
# %bb.374:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_619
.LBB2_66:
	xor	esi, esi
.LBB2_939:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_941
.LBB2_940:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_940
.LBB2_941:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_942:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_942
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.72:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.73:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_74
# %bb.376:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_622
# %bb.377:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_622
.LBB2_74:
	xor	esi, esi
.LBB2_947:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_949
.LBB2_948:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_948
.LBB2_949:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_950:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_950
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.79:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.80:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_81
# %bb.379:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_625
# %bb.380:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_625
.LBB2_81:
	xor	edx, edx
.LBB2_955:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_957
.LBB2_956:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_956
.LBB2_957:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_958:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_958
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.86:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.87:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_88
# %bb.382:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_628
# %bb.383:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_628
.LBB2_88:
	xor	edx, edx
.LBB2_963:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_965
.LBB2_964:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_964
.LBB2_965:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_966:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_966
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.93:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.94:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_95
# %bb.385:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_631
# %bb.386:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_631
.LBB2_95:
	xor	ecx, ecx
.LBB2_971:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_973
.LBB2_972:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_972
.LBB2_973:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_974:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_974
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.100:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.101:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_102
# %bb.388:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_634
# %bb.389:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_634
.LBB2_102:
	xor	ecx, ecx
.LBB2_979:
	movabs	rsi, 9223372036854775807
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB2_981
.LBB2_980:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	and	rdi, rsi
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rax, -1
	jne	.LBB2_980
.LBB2_981:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_982:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_982
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.107:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.108:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_109
# %bb.391:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_637
# %bb.392:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_637
.LBB2_109:
	xor	edx, edx
.LBB2_987:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_989
.LBB2_988:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_988
.LBB2_989:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_990:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_990
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.114:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.115:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_116
# %bb.394:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_640
# %bb.395:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_640
.LBB2_116:
	xor	edx, edx
.LBB2_995:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_997
.LBB2_996:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_996
.LBB2_997:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_998:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_998
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.121:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.122:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_123
# %bb.397:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_643
# %bb.398:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_643
.LBB2_123:
	xor	edx, edx
.LBB2_1003:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1005
.LBB2_1004:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1004
.LBB2_1005:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1006:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1006
	jmp	.LBB2_1451
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
	jne	.LBB2_1451
# %bb.128:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.129:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_130
# %bb.400:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_646
# %bb.401:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_646
.LBB2_130:
	xor	edx, edx
.LBB2_1011:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1013
.LBB2_1012:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1012
.LBB2_1013:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1014:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1014
	jmp	.LBB2_1451
.LBB2_131:
	cmp	edi, 2
	je	.LBB2_307
# %bb.132:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.133:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.134:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_135
# %bb.403:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_649
# %bb.404:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_649
.LBB2_135:
	xor	esi, esi
.LBB2_1019:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1021
.LBB2_1020:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1020
.LBB2_1021:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1022:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1022
	jmp	.LBB2_1451
.LBB2_136:
	cmp	edi, 2
	je	.LBB2_310
# %bb.137:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.138:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.139:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_140
# %bb.406:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_652
# %bb.407:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_652
.LBB2_140:
	xor	esi, esi
.LBB2_1027:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1029
.LBB2_1028:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1028
.LBB2_1029:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1030:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1030
	jmp	.LBB2_1451
.LBB2_141:
	cmp	edi, 2
	je	.LBB2_313
# %bb.142:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.143:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.144:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_145
# %bb.409:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_655
# %bb.410:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_655
.LBB2_145:
	xor	ecx, ecx
.LBB2_1035:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB2_1037
# %bb.1036:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_1037:
	add	rsi, r10
	je	.LBB2_1451
.LBB2_1038:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1038
	jmp	.LBB2_1451
.LBB2_146:
	cmp	edi, 2
	je	.LBB2_316
# %bb.147:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.148:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.149:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_150
# %bb.412:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB2_658
# %bb.413:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB2_658
.LBB2_150:
	xor	ecx, ecx
.LBB2_1043:
	mov	rsi, rcx
	not	rsi
	test	r10b, 1
	je	.LBB2_1045
# %bb.1044:
	movsx	edi, byte ptr [rdx + rcx]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [r8 + rcx], dil
	or	rcx, 1
.LBB2_1045:
	add	rsi, r10
	je	.LBB2_1451
.LBB2_1046:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1046
	jmp	.LBB2_1451
.LBB2_151:
	cmp	edi, 2
	je	.LBB2_319
# %bb.152:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.153:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.154:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_155
# %bb.415:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_661
# %bb.416:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_661
.LBB2_155:
	xor	esi, esi
.LBB2_1051:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1053
.LBB2_1052:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1052
.LBB2_1053:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1054:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1054
	jmp	.LBB2_1451
.LBB2_156:
	cmp	edi, 2
	je	.LBB2_322
# %bb.157:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.158:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.159:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_160
# %bb.418:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_664
# %bb.419:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_664
.LBB2_160:
	xor	esi, esi
.LBB2_1059:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1061
.LBB2_1060:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1060
.LBB2_1061:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1062:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1062
	jmp	.LBB2_1451
.LBB2_161:
	cmp	edi, 2
	je	.LBB2_325
# %bb.162:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.163:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.164:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_165
# %bb.421:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_667
# %bb.422:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_667
.LBB2_165:
	xor	edi, edi
.LBB2_1067:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1069
.LBB2_1068:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1068
.LBB2_1069:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1070:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1070
	jmp	.LBB2_1451
.LBB2_166:
	cmp	edi, 2
	je	.LBB2_328
# %bb.167:
	cmp	edi, 3
	jne	.LBB2_1451
# %bb.168:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.169:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_170
# %bb.424:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_670
# %bb.425:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_670
.LBB2_170:
	xor	edi, edi
.LBB2_1075:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1077
.LBB2_1076:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1076
.LBB2_1077:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1078:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1078
	jmp	.LBB2_1451
.LBB2_171:
	cmp	edi, 7
	je	.LBB2_331
# %bb.172:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.173:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.174:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
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
.LBB2_1083:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1085
.LBB2_1084:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1084
.LBB2_1085:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1086:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1086
	jmp	.LBB2_1451
.LBB2_176:
	cmp	edi, 7
	je	.LBB2_334
# %bb.177:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.178:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.179:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
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
.LBB2_1091:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1093
.LBB2_1092:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1092
.LBB2_1093:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1094:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1094
	jmp	.LBB2_1451
.LBB2_181:
	cmp	edi, 7
	je	.LBB2_337
# %bb.182:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.183:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.184:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_185
# %bb.433:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_679
# %bb.434:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_679
.LBB2_185:
	xor	ecx, ecx
.LBB2_849:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_851
.LBB2_850:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_850
.LBB2_851:
	cmp	rax, 3
	jb	.LBB2_1451
.LBB2_852:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_852
	jmp	.LBB2_1451
.LBB2_186:
	cmp	edi, 7
	je	.LBB2_340
# %bb.187:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.188:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.189:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_190
# %bb.436:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_681
# %bb.437:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB2_681
.LBB2_190:
	xor	ecx, ecx
.LBB2_859:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_861
.LBB2_860:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rcx]
	mov	qword ptr [r8 + 8*rcx], rdi
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_860
.LBB2_861:
	cmp	rax, 3
	jb	.LBB2_1451
.LBB2_862:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_862
	jmp	.LBB2_1451
.LBB2_191:
	cmp	edi, 7
	je	.LBB2_343
# %bb.192:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.193:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.194:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_195
# %bb.439:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_683
# %bb.440:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_683
.LBB2_195:
	xor	esi, esi
.LBB2_1099:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1101
.LBB2_1100:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1100
.LBB2_1101:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1102:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1102
	jmp	.LBB2_1451
.LBB2_196:
	cmp	edi, 7
	je	.LBB2_346
# %bb.197:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.198:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.199:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_200
# %bb.442:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_686
# %bb.443:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_686
.LBB2_200:
	xor	esi, esi
.LBB2_1107:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1109
.LBB2_1108:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1108
.LBB2_1109:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1110:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1110
	jmp	.LBB2_1451
.LBB2_201:
	cmp	edi, 7
	je	.LBB2_349
# %bb.202:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.203:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.204:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_445
# %bb.205:
	xor	edi, edi
	jmp	.LBB2_447
.LBB2_206:
	cmp	edi, 7
	je	.LBB2_352
# %bb.207:
	cmp	edi, 8
	jne	.LBB2_1451
# %bb.208:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.209:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_450
# %bb.210:
	xor	edi, edi
	jmp	.LBB2_452
.LBB2_211:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.212:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_213
# %bb.455:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_689
# %bb.456:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_689
.LBB2_213:
	xor	esi, esi
.LBB2_1115:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1117
.LBB2_1116:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1116
.LBB2_1117:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1118:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1118
	jmp	.LBB2_1451
.LBB2_214:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.215:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_216
# %bb.458:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_692
# %bb.459:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_692
.LBB2_216:
	xor	esi, esi
.LBB2_1123:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1125
.LBB2_1124:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1124
.LBB2_1125:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1126:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1126
	jmp	.LBB2_1451
.LBB2_217:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.218:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_219
# %bb.461:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_695
# %bb.462:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_695
.LBB2_219:
	xor	esi, esi
.LBB2_1131:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1133
.LBB2_1132:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1132
.LBB2_1133:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1134:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1134
	jmp	.LBB2_1451
.LBB2_220:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.221:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_222
# %bb.464:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_698
# %bb.465:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_698
.LBB2_222:
	xor	esi, esi
.LBB2_1139:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1141
.LBB2_1140:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1140
.LBB2_1141:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1142:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1142
	jmp	.LBB2_1451
.LBB2_223:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.224:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_225
# %bb.467:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_701
# %bb.468:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_701
.LBB2_225:
	xor	ecx, ecx
.LBB2_869:
	mov	rax, rcx
	not	rax
	add	rax, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_871
.LBB2_870:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rcx]
	mov	word ptr [r8 + 2*rcx], di
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_870
.LBB2_871:
	cmp	rax, 3
	jb	.LBB2_1451
.LBB2_872:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_872
	jmp	.LBB2_1451
.LBB2_226:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.227:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_228
# %bb.470:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_703
# %bb.471:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_703
.LBB2_228:
	xor	ecx, ecx
.LBB2_1147:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1149
# %bb.1148:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1149:
	add	rsi, rax
	je	.LBB2_1451
.LBB2_1150:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1150
	jmp	.LBB2_1451
.LBB2_229:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.230:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_231
# %bb.473:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_706
# %bb.474:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB2_706
.LBB2_231:
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
	jb	.LBB2_1451
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
	jmp	.LBB2_1451
.LBB2_232:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.233:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_234
# %bb.476:
	lea	rcx, [rdx + 2*rax]
	cmp	rcx, r8
	jbe	.LBB2_708
# %bb.477:
	lea	rcx, [r8 + 2*rax]
	cmp	rcx, rdx
	jbe	.LBB2_708
.LBB2_234:
	xor	ecx, ecx
.LBB2_1155:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1157
# %bb.1156:
	movsx	edi, word ptr [rdx + 2*rcx]
	mov	r9d, edi
	sar	r9d, 15
	add	edi, r9d
	xor	edi, r9d
	mov	word ptr [r8 + 2*rcx], di
	or	rcx, 1
.LBB2_1157:
	add	rsi, rax
	je	.LBB2_1451
.LBB2_1158:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1158
	jmp	.LBB2_1451
.LBB2_235:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.236:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_237
# %bb.479:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_711
# %bb.480:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_711
.LBB2_237:
	xor	esi, esi
.LBB2_1163:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1165
.LBB2_1164:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1164
.LBB2_1165:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1166:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1166
	jmp	.LBB2_1451
.LBB2_238:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.239:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_240
# %bb.482:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_714
# %bb.483:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_714
.LBB2_240:
	xor	esi, esi
.LBB2_1171:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1173
.LBB2_1172:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1172
.LBB2_1173:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1174:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1174
	jmp	.LBB2_1451
.LBB2_241:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.242:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_243
# %bb.485:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_717
# %bb.486:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_717
.LBB2_243:
	xor	esi, esi
.LBB2_1179:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1181
.LBB2_1180:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1180
.LBB2_1181:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1182:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1182
	jmp	.LBB2_1451
.LBB2_244:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.245:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_246
# %bb.488:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_720
# %bb.489:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_720
.LBB2_246:
	xor	esi, esi
.LBB2_1187:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1189
.LBB2_1188:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1188
.LBB2_1189:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1190:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1190
	jmp	.LBB2_1451
.LBB2_247:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.248:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_249
# %bb.491:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_723
# %bb.492:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_723
.LBB2_249:
	xor	esi, esi
.LBB2_1195:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1197
.LBB2_1196:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1196
.LBB2_1197:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1198:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1198
	jmp	.LBB2_1451
.LBB2_250:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.251:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_252
# %bb.494:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_726
# %bb.495:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_726
.LBB2_252:
	xor	esi, esi
.LBB2_1203:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1205
.LBB2_1204:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1204
.LBB2_1205:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1206:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1206
	jmp	.LBB2_1451
.LBB2_253:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.254:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_255
# %bb.497:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_729
# %bb.498:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_729
.LBB2_255:
	xor	esi, esi
.LBB2_1211:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1213
.LBB2_1212:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1212
.LBB2_1213:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1214:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1214
	jmp	.LBB2_1451
.LBB2_256:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.257:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_258
# %bb.500:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_732
# %bb.501:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_732
.LBB2_258:
	xor	esi, esi
.LBB2_1219:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1221
.LBB2_1220:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1220
.LBB2_1221:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1222:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1222
	jmp	.LBB2_1451
.LBB2_259:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.260:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_261
# %bb.503:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_735
# %bb.504:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_735
.LBB2_261:
	xor	esi, esi
.LBB2_1227:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1229
.LBB2_1228:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1228
.LBB2_1229:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1230:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1230
	jmp	.LBB2_1451
.LBB2_262:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.263:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_264
# %bb.506:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_738
# %bb.507:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_738
.LBB2_264:
	xor	edx, edx
.LBB2_1235:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1237
.LBB2_1236:                             # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1236
.LBB2_1237:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1238:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1238
	jmp	.LBB2_1451
.LBB2_265:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.266:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_267
# %bb.509:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_741
# %bb.510:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_741
.LBB2_267:
	xor	esi, esi
.LBB2_1243:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1245
.LBB2_1244:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1244
.LBB2_1245:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1246:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1246
	jmp	.LBB2_1451
.LBB2_268:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.269:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_270
# %bb.512:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_744
# %bb.513:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_744
.LBB2_270:
	xor	edx, edx
.LBB2_1251:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1253
.LBB2_1252:                             # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1252
.LBB2_1253:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1254:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1254
	jmp	.LBB2_1451
.LBB2_271:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.272:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_273
# %bb.515:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_747
# %bb.516:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_747
.LBB2_273:
	xor	ecx, ecx
.LBB2_1259:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1261
# %bb.1260:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_1261:
	add	rsi, rax
	je	.LBB2_1451
.LBB2_1262:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1262
	jmp	.LBB2_1451
.LBB2_274:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.275:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_276
# %bb.518:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_750
# %bb.519:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_750
.LBB2_276:
	xor	ecx, ecx
.LBB2_1267:
	mov	r9, rcx
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1270
# %bb.1268:
	mov	esi, 2147483647
.LBB2_1269:                             # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rcx]
	and	eax, esi
	mov	dword ptr [r8 + 4*rcx], eax
	add	rcx, 1
	add	rdi, -1
	jne	.LBB2_1269
.LBB2_1270:
	cmp	r9, 3
	jb	.LBB2_1451
# %bb.1271:
	mov	esi, 2147483647
.LBB2_1272:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1272
	jmp	.LBB2_1451
.LBB2_277:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.278:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_279
# %bb.521:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB2_753
# %bb.522:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB2_753
.LBB2_279:
	xor	ecx, ecx
.LBB2_1277:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1279
# %bb.1278:
	mov	r9, qword ptr [rdx + 8*rcx]
	mov	rdi, r9
	neg	rdi
	cmovl	rdi, r9
	mov	qword ptr [r8 + 8*rcx], rdi
	or	rcx, 1
.LBB2_1279:
	add	rsi, rax
	je	.LBB2_1451
.LBB2_1280:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1280
	jmp	.LBB2_1451
.LBB2_280:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.281:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_282
# %bb.524:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB2_756
# %bb.525:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB2_756
.LBB2_282:
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
	jb	.LBB2_1451
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
	jmp	.LBB2_1451
.LBB2_283:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.284:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_285
# %bb.527:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_759
# %bb.528:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_759
.LBB2_285:
	xor	esi, esi
.LBB2_1295:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1297
.LBB2_1296:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1296
.LBB2_1297:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1298:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1298
	jmp	.LBB2_1451
.LBB2_286:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.287:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_288
# %bb.530:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_762
# %bb.531:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_762
.LBB2_288:
	xor	edx, edx
.LBB2_1303:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1305
.LBB2_1304:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1304
.LBB2_1305:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1306:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1306
	jmp	.LBB2_1451
.LBB2_289:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.290:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_291
# %bb.533:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_765
# %bb.534:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_765
.LBB2_291:
	xor	esi, esi
.LBB2_1311:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1313
.LBB2_1312:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1312
.LBB2_1313:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1314:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1314
	jmp	.LBB2_1451
.LBB2_292:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.293:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_294
# %bb.536:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_768
# %bb.537:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_768
.LBB2_294:
	xor	edx, edx
.LBB2_1319:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1321
.LBB2_1320:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1320
.LBB2_1321:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1322:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1322
	jmp	.LBB2_1451
.LBB2_295:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.296:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_539
# %bb.297:
	xor	edi, edi
	jmp	.LBB2_541
.LBB2_298:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.299:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_300
# %bb.544:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_771
# %bb.545:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_771
.LBB2_300:
	xor	edx, edx
.LBB2_1327:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1329
.LBB2_1328:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1328
.LBB2_1329:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1330:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1330
	jmp	.LBB2_1451
.LBB2_301:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.302:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_547
# %bb.303:
	xor	edi, edi
	jmp	.LBB2_549
.LBB2_304:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.305:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_306
# %bb.552:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_774
# %bb.553:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_774
.LBB2_306:
	xor	edx, edx
.LBB2_1335:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1337
.LBB2_1336:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1336
.LBB2_1337:
	cmp	rsi, 3
	jb	.LBB2_1451
.LBB2_1338:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1338
	jmp	.LBB2_1451
.LBB2_307:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.308:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_309
# %bb.555:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_777
# %bb.556:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_777
.LBB2_309:
	xor	esi, esi
.LBB2_1343:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1345
.LBB2_1344:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1344
.LBB2_1345:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1346:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1346
	jmp	.LBB2_1451
.LBB2_310:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.311:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_312
# %bb.558:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_780
# %bb.559:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_780
.LBB2_312:
	xor	esi, esi
.LBB2_1351:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1353
.LBB2_1352:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1352
.LBB2_1353:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1354:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1354
	jmp	.LBB2_1451
.LBB2_313:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.314:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_315
# %bb.561:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB2_783
# %bb.562:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB2_783
.LBB2_315:
	xor	ecx, ecx
.LBB2_889:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_891
.LBB2_890:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_890
.LBB2_891:
	cmp	rdi, 3
	jb	.LBB2_1451
.LBB2_892:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_892
	jmp	.LBB2_1451
.LBB2_316:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.317:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_318
# %bb.564:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB2_785
# %bb.565:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB2_785
.LBB2_318:
	xor	ecx, ecx
.LBB2_899:
	mov	rdi, rcx
	not	rdi
	add	rdi, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_901
.LBB2_900:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rcx]
	mov	byte ptr [r8 + rcx], al
	add	rcx, 1
	add	rsi, -1
	jne	.LBB2_900
.LBB2_901:
	cmp	rdi, 3
	jb	.LBB2_1451
.LBB2_902:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_902
	jmp	.LBB2_1451
.LBB2_319:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.320:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_321
# %bb.567:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_787
# %bb.568:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_787
.LBB2_321:
	xor	esi, esi
.LBB2_1359:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1361
.LBB2_1360:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1360
.LBB2_1361:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1362:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1362
	jmp	.LBB2_1451
.LBB2_322:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.323:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_324
# %bb.570:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_790
# %bb.571:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_790
.LBB2_324:
	xor	esi, esi
.LBB2_1367:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1369
.LBB2_1368:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1368
.LBB2_1369:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1370:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1370
	jmp	.LBB2_1451
.LBB2_325:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.326:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_327
# %bb.573:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_793
# %bb.574:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_793
.LBB2_327:
	xor	edi, edi
.LBB2_1375:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1377
.LBB2_1376:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1376
.LBB2_1377:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1378:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1378
	jmp	.LBB2_1451
.LBB2_328:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.329:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_330
# %bb.576:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_796
# %bb.577:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_796
.LBB2_330:
	xor	edi, edi
.LBB2_1383:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_1385
.LBB2_1384:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_1384
.LBB2_1385:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1386:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1386
	jmp	.LBB2_1451
.LBB2_331:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.332:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_333
# %bb.579:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_799
# %bb.580:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_799
.LBB2_333:
	xor	esi, esi
.LBB2_1391:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1393
.LBB2_1392:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1392
.LBB2_1393:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1394:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1394
	jmp	.LBB2_1451
.LBB2_334:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.335:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_336
# %bb.582:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_802
# %bb.583:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_802
.LBB2_336:
	xor	esi, esi
.LBB2_1399:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1401
.LBB2_1400:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1400
.LBB2_1401:
	cmp	rdx, 3
	jb	.LBB2_1451
.LBB2_1402:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1402
	jmp	.LBB2_1451
.LBB2_337:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.338:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_339
# %bb.585:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_805
# %bb.586:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_805
.LBB2_339:
	xor	ecx, ecx
.LBB2_1407:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1409
# %bb.1408:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_1409:
	add	rsi, rax
	je	.LBB2_1451
.LBB2_1410:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1410
	jmp	.LBB2_1451
.LBB2_340:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.341:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_342
# %bb.588:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB2_808
# %bb.589:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB2_808
.LBB2_342:
	xor	ecx, ecx
.LBB2_1415:
	mov	rsi, rcx
	not	rsi
	test	al, 1
	je	.LBB2_1417
# %bb.1416:
	mov	r9d, dword ptr [rdx + 4*rcx]
	mov	edi, r9d
	neg	edi
	cmovl	edi, r9d
	mov	dword ptr [r8 + 4*rcx], edi
	or	rcx, 1
.LBB2_1417:
	add	rsi, rax
	je	.LBB2_1451
.LBB2_1418:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1418
	jmp	.LBB2_1451
.LBB2_343:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.344:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_345
# %bb.591:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_811
# %bb.592:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_811
.LBB2_345:
	xor	esi, esi
.LBB2_1423:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1425
.LBB2_1424:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1424
.LBB2_1425:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1426:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1426
	jmp	.LBB2_1451
.LBB2_346:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.347:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_348
# %bb.594:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_814
# %bb.595:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_814
.LBB2_348:
	xor	esi, esi
.LBB2_1431:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1433
.LBB2_1432:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1432
.LBB2_1433:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1434:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1434
	jmp	.LBB2_1451
.LBB2_349:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.350:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_351
# %bb.597:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_817
# %bb.598:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_817
.LBB2_351:
	xor	esi, esi
.LBB2_1439:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1441
.LBB2_1440:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1440
.LBB2_1441:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1442:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1442
	jmp	.LBB2_1451
.LBB2_352:
	test	r9d, r9d
	jle	.LBB2_1451
# %bb.353:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_354
# %bb.600:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_820
# %bb.601:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_820
.LBB2_354:
	xor	esi, esi
.LBB2_1447:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1449
.LBB2_1448:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1448
.LBB2_1449:
	cmp	r9, 3
	jb	.LBB2_1451
.LBB2_1450:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1450
	jmp	.LBB2_1451
.LBB2_445:
	and	esi, -4
	xor	edi, edi
.LBB2_446:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_446
.LBB2_447:
	test	r9, r9
	je	.LBB2_1451
# %bb.448:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_449:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_449
	jmp	.LBB2_1451
.LBB2_450:
	and	esi, -4
	xor	edi, edi
.LBB2_451:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_451
.LBB2_452:
	test	r9, r9
	je	.LBB2_1451
# %bb.453:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_454:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_454
	jmp	.LBB2_1451
.LBB2_539:
	and	esi, -4
	xor	edi, edi
.LBB2_540:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_540
.LBB2_541:
	test	r9, r9
	je	.LBB2_1451
# %bb.542:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_543:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_543
	jmp	.LBB2_1451
.LBB2_547:
	and	esi, -4
	xor	edi, edi
.LBB2_548:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_548
.LBB2_549:
	test	r9, r9
	je	.LBB2_1451
# %bb.550:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_551:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_551
.LBB2_1451:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB2_603:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_903
# %bb.604:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_605
	jmp	.LBB2_904
.LBB2_606:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_911
# %bb.607:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_608
	jmp	.LBB2_912
.LBB2_609:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB2_823
# %bb.610:
	xor	eax, eax
	jmp	.LBB2_825
.LBB2_611:
	mov	ecx, r10d
	and	ecx, -8
	lea	rax, [rcx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 24
	jae	.LBB2_833
# %bb.612:
	xor	eax, eax
	jmp	.LBB2_835
.LBB2_613:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_919
# %bb.614:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_615:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_615
	jmp	.LBB2_920
.LBB2_616:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_927
# %bb.617:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_618:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_618
	jmp	.LBB2_928
.LBB2_619:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_935
# %bb.620:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_621:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_621
	jmp	.LBB2_936
.LBB2_622:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_943
# %bb.623:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_624:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_624
	jmp	.LBB2_944
.LBB2_625:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_951
# %bb.626:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_627:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_627
	jmp	.LBB2_952
.LBB2_628:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_959
# %bb.629:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_630:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_630
	jmp	.LBB2_960
.LBB2_631:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB2_967
# %bb.632:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB2_633:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_633
	jmp	.LBB2_968
.LBB2_634:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB2_975
# %bb.635:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_0] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB2_636:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_636
	jmp	.LBB2_976
.LBB2_637:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_983
# %bb.638:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_639:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_639
	jmp	.LBB2_984
.LBB2_640:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_991
# %bb.641:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_642:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_642
	jmp	.LBB2_992
.LBB2_643:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_999
# %bb.644:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_645:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_645
	jmp	.LBB2_1000
.LBB2_646:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1007
# %bb.647:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_648:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_648
	jmp	.LBB2_1008
.LBB2_649:
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
	je	.LBB2_1015
# %bb.650:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_651:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_651
	jmp	.LBB2_1016
.LBB2_652:
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
	je	.LBB2_1023
# %bb.653:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_654:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_654
	jmp	.LBB2_1024
.LBB2_655:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1031
# %bb.656:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI2_2] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB2_657:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_657
	jmp	.LBB2_1032
.LBB2_658:
	mov	ecx, r10d
	and	ecx, -16
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1039
# %bb.659:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	movdqa	xmm8, xmmword ptr [rip + .LCPI2_2] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB2_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_660
	jmp	.LBB2_1040
.LBB2_661:
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
	je	.LBB2_1047
# %bb.662:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_663:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_663
	jmp	.LBB2_1048
.LBB2_664:
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
	je	.LBB2_1055
# %bb.665:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_666:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_666
	jmp	.LBB2_1056
.LBB2_667:
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
	je	.LBB2_1063
# %bb.668:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_669:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_669
	jmp	.LBB2_1064
.LBB2_670:
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
	je	.LBB2_1071
# %bb.671:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_672:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_672
	jmp	.LBB2_1072
.LBB2_673:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1079
# %bb.674:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_675:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_675
	jmp	.LBB2_1080
.LBB2_676:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1087
# %bb.677:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_678:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_678
	jmp	.LBB2_1088
.LBB2_679:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB2_843
# %bb.680:
	xor	eax, eax
	jmp	.LBB2_845
.LBB2_681:
	mov	ecx, r10d
	and	ecx, -4
	lea	rax, [rcx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 12
	jae	.LBB2_853
# %bb.682:
	xor	eax, eax
	jmp	.LBB2_855
.LBB2_683:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1095
# %bb.684:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_685:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_685
	jmp	.LBB2_1096
.LBB2_686:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1103
# %bb.687:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_688:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_688
	jmp	.LBB2_1104
.LBB2_689:
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
	je	.LBB2_1111
# %bb.690:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_691:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_691
	jmp	.LBB2_1112
.LBB2_692:
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
	je	.LBB2_1119
# %bb.693:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_694:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_694
	jmp	.LBB2_1120
.LBB2_695:
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
	je	.LBB2_1127
# %bb.696:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_697:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_697
	jmp	.LBB2_1128
.LBB2_698:
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
	je	.LBB2_1135
# %bb.699:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_700:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_700
	jmp	.LBB2_1136
.LBB2_701:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB2_863
# %bb.702:
	xor	eax, eax
	jmp	.LBB2_865
.LBB2_703:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1143
# %bb.704:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB2_705:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_705
	jmp	.LBB2_1144
.LBB2_706:
	mov	ecx, r10d
	and	ecx, -16
	lea	rax, [rcx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB2_873
# %bb.707:
	xor	eax, eax
	jmp	.LBB2_875
.LBB2_708:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1151
# %bb.709:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
.LBB2_710:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_710
	jmp	.LBB2_1152
.LBB2_711:
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
	je	.LBB2_1159
# %bb.712:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_713:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_713
	jmp	.LBB2_1160
.LBB2_714:
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
	je	.LBB2_1167
# %bb.715:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_716:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_716
	jmp	.LBB2_1168
.LBB2_717:
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
	je	.LBB2_1175
# %bb.718:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_719:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_719
	jmp	.LBB2_1176
.LBB2_720:
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
	je	.LBB2_1183
# %bb.721:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_722:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_722
	jmp	.LBB2_1184
.LBB2_723:
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
	je	.LBB2_1191
# %bb.724:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_725:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_725
	jmp	.LBB2_1192
.LBB2_726:
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
	je	.LBB2_1199
# %bb.727:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_728:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_728
	jmp	.LBB2_1200
.LBB2_729:
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
	je	.LBB2_1207
# %bb.730:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_731:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_731
	jmp	.LBB2_1208
.LBB2_732:
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
	je	.LBB2_1215
# %bb.733:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_734:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_734
	jmp	.LBB2_1216
.LBB2_735:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1223
# %bb.736:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_737:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_737
	jmp	.LBB2_1224
.LBB2_738:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1231
# %bb.739:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_740:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_740
	jmp	.LBB2_1232
.LBB2_741:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1239
# %bb.742:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_743:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_743
	jmp	.LBB2_1240
.LBB2_744:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1247
# %bb.745:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_746:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_746
	jmp	.LBB2_1248
.LBB2_747:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1255
# %bb.748:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB2_749:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_749
	jmp	.LBB2_1256
.LBB2_750:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1263
# %bb.751:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB2_752:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_752
	jmp	.LBB2_1264
.LBB2_753:
	mov	ecx, eax
	and	ecx, -4
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1273
# %bb.754:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB2_755:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_755
	jmp	.LBB2_1274
.LBB2_756:
	mov	ecx, r10d
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1281
# %bb.757:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	movdqa	xmm0, xmmword ptr [rip + .LCPI2_1] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB2_758:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_758
	jmp	.LBB2_1282
.LBB2_759:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1291
# %bb.760:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_761:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_761
	jmp	.LBB2_1292
.LBB2_762:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1299
# %bb.763:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_764
	jmp	.LBB2_1300
.LBB2_765:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1307
# %bb.766:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_767:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_767
	jmp	.LBB2_1308
.LBB2_768:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1315
# %bb.769:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_770:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_770
	jmp	.LBB2_1316
.LBB2_771:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1323
# %bb.772:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_773:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_773
	jmp	.LBB2_1324
.LBB2_774:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1331
# %bb.775:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_776:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_776
	jmp	.LBB2_1332
.LBB2_777:
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
	je	.LBB2_1339
# %bb.778:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_779:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_779
	jmp	.LBB2_1340
.LBB2_780:
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
	je	.LBB2_1347
# %bb.781:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_782:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_782
	jmp	.LBB2_1348
.LBB2_783:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB2_883
# %bb.784:
	xor	edi, edi
	jmp	.LBB2_885
.LBB2_785:
	mov	ecx, r10d
	and	ecx, -32
	lea	rdi, [rcx - 32]
	mov	rsi, rdi
	shr	rsi, 5
	add	rsi, 1
	mov	eax, esi
	and	eax, 3
	cmp	rdi, 96
	jae	.LBB2_893
# %bb.786:
	xor	edi, edi
	jmp	.LBB2_895
.LBB2_787:
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
	je	.LBB2_1355
# %bb.788:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_789:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_789
	jmp	.LBB2_1356
.LBB2_790:
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
	je	.LBB2_1363
# %bb.791:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_792:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_792
	jmp	.LBB2_1364
.LBB2_793:
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
	je	.LBB2_1371
# %bb.794:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_795:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_795
	jmp	.LBB2_1372
.LBB2_796:
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
	je	.LBB2_1379
# %bb.797:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_3] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_798:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_798
	jmp	.LBB2_1380
.LBB2_799:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1387
# %bb.800:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_801:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_801
	jmp	.LBB2_1388
.LBB2_802:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1395
# %bb.803:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_804
	jmp	.LBB2_1396
.LBB2_805:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1403
# %bb.806:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_807:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_807
	jmp	.LBB2_1404
.LBB2_808:
	mov	ecx, eax
	and	ecx, -8
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1411
# %bb.809:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_810:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_810
	jmp	.LBB2_1412
.LBB2_811:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1419
# %bb.812:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_813:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_813
	jmp	.LBB2_1420
.LBB2_814:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1427
# %bb.815:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_816:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_816
	jmp	.LBB2_1428
.LBB2_817:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1435
# %bb.818:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_819:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_819
	jmp	.LBB2_1436
.LBB2_820:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1443
# %bb.821:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_822:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_822
	jmp	.LBB2_1444
.LBB2_823:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_824:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_824
.LBB2_825:
	test	rsi, rsi
	je	.LBB2_828
# %bb.826:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB2_827:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_827
.LBB2_828:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_829
.LBB2_833:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_834:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_834
.LBB2_835:
	test	rsi, rsi
	je	.LBB2_838
# %bb.836:
	lea	rax, [4*rax + 16]
	neg	rsi
.LBB2_837:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_837
.LBB2_838:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_839
.LBB2_843:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_844:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_844
.LBB2_845:
	test	rsi, rsi
	je	.LBB2_848
# %bb.846:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB2_847:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_847
.LBB2_848:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_849
.LBB2_853:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_854:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_854
.LBB2_855:
	test	rsi, rsi
	je	.LBB2_858
# %bb.856:
	lea	rax, [8*rax + 16]
	neg	rsi
.LBB2_857:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_857
.LBB2_858:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_859
.LBB2_863:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_864:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_864
.LBB2_865:
	test	rsi, rsi
	je	.LBB2_868
# %bb.866:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB2_867:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_867
.LBB2_868:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_869
.LBB2_873:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB2_874:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_874
.LBB2_875:
	test	rsi, rsi
	je	.LBB2_878
# %bb.876:
	add	rax, rax
	add	rax, 16
	neg	rsi
.LBB2_877:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax - 16]
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmmword ptr [r8 + rax - 16], xmm0
	movdqu	xmmword ptr [r8 + rax], xmm1
	add	rax, 32
	inc	rsi
	jne	.LBB2_877
.LBB2_878:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_879
.LBB2_883:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB2_884:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_884
.LBB2_885:
	test	rax, rax
	je	.LBB2_888
# %bb.886:
	add	rdi, 16
	neg	rax
.LBB2_887:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB2_887
.LBB2_888:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_889
.LBB2_893:
	and	rsi, -4
	neg	rsi
	xor	edi, edi
.LBB2_894:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_894
.LBB2_895:
	test	rax, rax
	je	.LBB2_898
# %bb.896:
	add	rdi, 16
	neg	rax
.LBB2_897:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi - 16]
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmmword ptr [r8 + rdi - 16], xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	add	rdi, 32
	inc	rax
	jne	.LBB2_897
.LBB2_898:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_899
.LBB2_903:
	xor	edi, edi
.LBB2_904:
	test	r9b, 1
	je	.LBB2_906
# %bb.905:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_906:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_907
.LBB2_911:
	xor	edi, edi
.LBB2_912:
	test	r9b, 1
	je	.LBB2_914
# %bb.913:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_914:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_915
.LBB2_919:
	xor	edi, edi
.LBB2_920:
	test	r9b, 1
	je	.LBB2_922
# %bb.921:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_922:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_923
.LBB2_927:
	xor	edi, edi
.LBB2_928:
	test	r9b, 1
	je	.LBB2_930
# %bb.929:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_930:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_931
.LBB2_935:
	xor	edi, edi
.LBB2_936:
	test	r9b, 1
	je	.LBB2_938
# %bb.937:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_938:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_939
.LBB2_943:
	xor	edi, edi
.LBB2_944:
	test	r9b, 1
	je	.LBB2_946
# %bb.945:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_946:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_947
.LBB2_951:
	xor	edi, edi
.LBB2_952:
	test	r9b, 1
	je	.LBB2_954
# %bb.953:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_954:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_955
.LBB2_959:
	xor	edi, edi
.LBB2_960:
	test	r9b, 1
	je	.LBB2_962
# %bb.961:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_962:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_963
.LBB2_967:
	xor	edi, edi
.LBB2_968:
	test	r9b, 1
	je	.LBB2_970
# %bb.969:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_0] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_970:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_971
.LBB2_975:
	xor	edi, edi
.LBB2_976:
	test	r9b, 1
	je	.LBB2_978
# %bb.977:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_0] # xmm2 = [9223372036854775807,9223372036854775807]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_978:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_979
.LBB2_983:
	xor	edi, edi
.LBB2_984:
	test	r9b, 1
	je	.LBB2_986
# %bb.985:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_986:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_987
.LBB2_991:
	xor	edi, edi
.LBB2_992:
	test	r9b, 1
	je	.LBB2_994
# %bb.993:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_994:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_995
.LBB2_999:
	xor	edi, edi
.LBB2_1000:
	test	r9b, 1
	je	.LBB2_1002
# %bb.1001:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_1002:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1003
.LBB2_1007:
	xor	edi, edi
.LBB2_1008:
	test	r9b, 1
	je	.LBB2_1010
# %bb.1009:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_1010:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1011
.LBB2_1015:
	xor	edi, edi
.LBB2_1016:
	test	r9b, 1
	je	.LBB2_1018
# %bb.1017:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1018:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1019
.LBB2_1023:
	xor	edi, edi
.LBB2_1024:
	test	r9b, 1
	je	.LBB2_1026
# %bb.1025:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1026:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1027
.LBB2_1031:
	xor	esi, esi
.LBB2_1032:
	test	r9b, 1
	je	.LBB2_1034
# %bb.1033:
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_2] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB2_1034:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_1035
.LBB2_1039:
	xor	esi, esi
.LBB2_1040:
	test	r9b, 1
	je	.LBB2_1042
# %bb.1041:
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_2] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [r8 + rsi], xmm1
.LBB2_1042:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_1043
.LBB2_1047:
	xor	edi, edi
.LBB2_1048:
	test	r9b, 1
	je	.LBB2_1050
# %bb.1049:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1050:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1051
.LBB2_1055:
	xor	edi, edi
.LBB2_1056:
	test	r9b, 1
	je	.LBB2_1058
# %bb.1057:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1058:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1059
.LBB2_1063:
	xor	eax, eax
.LBB2_1064:
	test	r9b, 1
	je	.LBB2_1066
# %bb.1065:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_1066:
	cmp	rdi, r10
	je	.LBB2_1451
	jmp	.LBB2_1067
.LBB2_1071:
	xor	eax, eax
.LBB2_1072:
	test	r9b, 1
	je	.LBB2_1074
# %bb.1073:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_1074:
	cmp	rdi, r10
	je	.LBB2_1451
	jmp	.LBB2_1075
.LBB2_1079:
	xor	edi, edi
.LBB2_1080:
	test	r9b, 1
	je	.LBB2_1082
# %bb.1081:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1082:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1083
.LBB2_1087:
	xor	edi, edi
.LBB2_1088:
	test	r9b, 1
	je	.LBB2_1090
# %bb.1089:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1090:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1091
.LBB2_1095:
	xor	edi, edi
.LBB2_1096:
	test	r9b, 1
	je	.LBB2_1098
# %bb.1097:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1098:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1099
.LBB2_1103:
	xor	edi, edi
.LBB2_1104:
	test	r9b, 1
	je	.LBB2_1106
# %bb.1105:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1106:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1107
.LBB2_1111:
	xor	edi, edi
.LBB2_1112:
	test	r9b, 1
	je	.LBB2_1114
# %bb.1113:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1114:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1115
.LBB2_1119:
	xor	edi, edi
.LBB2_1120:
	test	r9b, 1
	je	.LBB2_1122
# %bb.1121:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1122:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1123
.LBB2_1127:
	xor	edi, edi
.LBB2_1128:
	test	r9b, 1
	je	.LBB2_1130
# %bb.1129:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1130:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1131
.LBB2_1135:
	xor	edi, edi
.LBB2_1136:
	test	r9b, 1
	je	.LBB2_1138
# %bb.1137:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_1138:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1139
.LBB2_1143:
	xor	esi, esi
.LBB2_1144:
	test	r9b, 1
	je	.LBB2_1146
# %bb.1145:
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
.LBB2_1146:
	cmp	rcx, rax
	je	.LBB2_1451
	jmp	.LBB2_1147
.LBB2_1151:
	xor	esi, esi
.LBB2_1152:
	test	r9b, 1
	je	.LBB2_1154
# %bb.1153:
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
.LBB2_1154:
	cmp	rcx, rax
	je	.LBB2_1451
	jmp	.LBB2_1155
.LBB2_1159:
	xor	edi, edi
.LBB2_1160:
	test	r9b, 1
	je	.LBB2_1162
# %bb.1161:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1162:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1163
.LBB2_1167:
	xor	edi, edi
.LBB2_1168:
	test	r9b, 1
	je	.LBB2_1170
# %bb.1169:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1170:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1171
.LBB2_1175:
	xor	edi, edi
.LBB2_1176:
	test	r9b, 1
	je	.LBB2_1178
# %bb.1177:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1178:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1179
.LBB2_1183:
	xor	edi, edi
.LBB2_1184:
	test	r9b, 1
	je	.LBB2_1186
# %bb.1185:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1186:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1187
.LBB2_1191:
	xor	edi, edi
.LBB2_1192:
	test	r9b, 1
	je	.LBB2_1194
# %bb.1193:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1194:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1195
.LBB2_1199:
	xor	edi, edi
.LBB2_1200:
	test	r9b, 1
	je	.LBB2_1202
# %bb.1201:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1202:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1203
.LBB2_1207:
	xor	edi, edi
.LBB2_1208:
	test	r9b, 1
	je	.LBB2_1210
# %bb.1209:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1210:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1211
.LBB2_1215:
	xor	edi, edi
.LBB2_1216:
	test	r9b, 1
	je	.LBB2_1218
# %bb.1217:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_1218:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1219
.LBB2_1223:
	xor	edi, edi
.LBB2_1224:
	test	r9b, 1
	je	.LBB2_1226
# %bb.1225:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1226:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1227
.LBB2_1231:
	xor	edi, edi
.LBB2_1232:
	test	r9b, 1
	je	.LBB2_1234
# %bb.1233:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1234:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1235
.LBB2_1239:
	xor	edi, edi
.LBB2_1240:
	test	r9b, 1
	je	.LBB2_1242
# %bb.1241:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_1242:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1243
.LBB2_1247:
	xor	edi, edi
.LBB2_1248:
	test	r9b, 1
	je	.LBB2_1250
# %bb.1249:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1250:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1251
.LBB2_1255:
	xor	esi, esi
.LBB2_1256:
	test	r9b, 1
	je	.LBB2_1258
# %bb.1257:
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
.LBB2_1258:
	cmp	rcx, rax
	je	.LBB2_1451
	jmp	.LBB2_1259
.LBB2_1263:
	xor	edi, edi
.LBB2_1264:
	test	r9b, 1
	je	.LBB2_1266
# %bb.1265:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_1] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1266:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_1267
.LBB2_1273:
	xor	esi, esi
.LBB2_1274:
	test	r9b, 1
	je	.LBB2_1276
# %bb.1275:
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
.LBB2_1276:
	cmp	rcx, rax
	je	.LBB2_1451
	jmp	.LBB2_1277
.LBB2_1281:
	xor	edi, edi
.LBB2_1282:
	test	r9b, 1
	je	.LBB2_1284
# %bb.1283:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm2, xmmword ptr [rip + .LCPI2_1] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	pand	xmm0, xmm2
	pand	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1284:
	cmp	rcx, r10
	je	.LBB2_1451
	jmp	.LBB2_1285
.LBB2_1291:
	xor	edi, edi
.LBB2_1292:
	test	r9b, 1
	je	.LBB2_1294
# %bb.1293:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1294:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1295
.LBB2_1299:
	xor	edi, edi
.LBB2_1300:
	test	r9b, 1
	je	.LBB2_1302
# %bb.1301:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1302:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1303
.LBB2_1307:
	xor	edi, edi
.LBB2_1308:
	test	r9b, 1
	je	.LBB2_1310
# %bb.1309:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_1310:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1311
.LBB2_1315:
	xor	edi, edi
.LBB2_1316:
	test	r9b, 1
	je	.LBB2_1318
# %bb.1317:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1318:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1319
.LBB2_1323:
	xor	edi, edi
.LBB2_1324:
	test	r9b, 1
	je	.LBB2_1326
# %bb.1325:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1326:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1327
.LBB2_1331:
	xor	edi, edi
.LBB2_1332:
	test	r9b, 1
	je	.LBB2_1334
# %bb.1333:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_1334:
	cmp	rdx, rax
	je	.LBB2_1451
	jmp	.LBB2_1335
.LBB2_1339:
	xor	edi, edi
.LBB2_1340:
	test	r9b, 1
	je	.LBB2_1342
# %bb.1341:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1342:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1343
.LBB2_1347:
	xor	edi, edi
.LBB2_1348:
	test	r9b, 1
	je	.LBB2_1350
# %bb.1349:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1350:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1351
.LBB2_1355:
	xor	edi, edi
.LBB2_1356:
	test	r9b, 1
	je	.LBB2_1358
# %bb.1357:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1358:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1359
.LBB2_1363:
	xor	edi, edi
.LBB2_1364:
	test	r9b, 1
	je	.LBB2_1366
# %bb.1365:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1366:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1367
.LBB2_1371:
	xor	eax, eax
.LBB2_1372:
	test	r9b, 1
	je	.LBB2_1374
# %bb.1373:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_1374:
	cmp	rdi, r10
	je	.LBB2_1451
	jmp	.LBB2_1375
.LBB2_1379:
	xor	eax, eax
.LBB2_1380:
	test	r9b, 1
	je	.LBB2_1382
# %bb.1381:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_3] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_1382:
	cmp	rdi, r10
	je	.LBB2_1451
	jmp	.LBB2_1383
.LBB2_1387:
	xor	edi, edi
.LBB2_1388:
	test	r9b, 1
	je	.LBB2_1390
# %bb.1389:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1390:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1391
.LBB2_1395:
	xor	edi, edi
.LBB2_1396:
	test	r9b, 1
	je	.LBB2_1398
# %bb.1397:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1398:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1399
.LBB2_1403:
	xor	edi, edi
.LBB2_1404:
	test	r9b, 1
	je	.LBB2_1406
# %bb.1405:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1406:
	cmp	rcx, rax
	je	.LBB2_1451
	jmp	.LBB2_1407
.LBB2_1411:
	xor	edi, edi
.LBB2_1412:
	test	r9b, 1
	je	.LBB2_1414
# %bb.1413:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_1414:
	cmp	rcx, rax
	je	.LBB2_1451
	jmp	.LBB2_1415
.LBB2_1419:
	xor	edi, edi
.LBB2_1420:
	test	r9b, 1
	je	.LBB2_1422
# %bb.1421:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1422:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1423
.LBB2_1427:
	xor	edi, edi
.LBB2_1428:
	test	r9b, 1
	je	.LBB2_1430
# %bb.1429:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1430:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1431
.LBB2_1435:
	xor	edi, edi
.LBB2_1436:
	test	r9b, 1
	je	.LBB2_1438
# %bb.1437:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1438:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1439
.LBB2_1443:
	xor	edi, edi
.LBB2_1444:
	test	r9b, 1
	je	.LBB2_1446
# %bb.1445:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1446:
	cmp	rsi, r10
	je	.LBB2_1451
	jmp	.LBB2_1447
.Lfunc_end2:
	.size	arithmetic_scalar_arr_sse4, .Lfunc_end2-arithmetic_scalar_arr_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
