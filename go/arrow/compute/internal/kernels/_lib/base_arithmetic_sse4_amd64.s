	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_binary_sse4
.LCPI0_0:
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.short	255                             # 0xff
	.text
	.globl	arithmetic_binary_sse4
	.p2align	4, 0x90
	.type	arithmetic_binary_sse4,@function
arithmetic_binary_sse4:                 # @arithmetic_binary_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 20
	jg	.LBB0_11
# %bb.1:
	test	sil, sil
	je	.LBB0_21
# %bb.2:
	cmp	sil, 1
	je	.LBB0_367
# %bb.3:
	cmp	sil, 2
	jne	.LBB0_1013
# %bb.4:
	cmp	edi, 6
	jg	.LBB0_719
# %bb.5:
	cmp	edi, 3
	jle	.LBB0_6
# %bb.713:
	cmp	edi, 4
	je	.LBB0_760
# %bb.714:
	cmp	edi, 5
	je	.LBB0_776
# %bb.715:
	cmp	edi, 6
	jne	.LBB0_1013
# %bb.716:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.717:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_792
# %bb.718:
	xor	esi, esi
.LBB0_801:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_803
.LBB0_802:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_802
.LBB0_803:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_804
	jmp	.LBB0_1013
.LBB0_11:
	cmp	sil, 21
	je	.LBB0_194
# %bb.12:
	cmp	sil, 22
	je	.LBB0_540
# %bb.13:
	cmp	sil, 23
	jne	.LBB0_1013
# %bb.14:
	cmp	edi, 6
	jg	.LBB0_869
# %bb.15:
	cmp	edi, 3
	jle	.LBB0_16
# %bb.863:
	cmp	edi, 4
	je	.LBB0_910
# %bb.864:
	cmp	edi, 5
	je	.LBB0_926
# %bb.865:
	cmp	edi, 6
	jne	.LBB0_1013
# %bb.866:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.867:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_942
# %bb.868:
	xor	esi, esi
.LBB0_951:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_953
.LBB0_952:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_952
.LBB0_953:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_954:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_954
	jmp	.LBB0_1013
.LBB0_21:
	cmp	edi, 6
	jg	.LBB0_34
# %bb.22:
	cmp	edi, 3
	jle	.LBB0_23
# %bb.28:
	cmp	edi, 4
	je	.LBB0_75
# %bb.29:
	cmp	edi, 5
	je	.LBB0_91
# %bb.30:
	cmp	edi, 6
	jne	.LBB0_1013
# %bb.31:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.32:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_107
# %bb.33:
	xor	esi, esi
.LBB0_116:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_118
.LBB0_117:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_117
.LBB0_118:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_119:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_119
	jmp	.LBB0_1013
.LBB0_367:
	cmp	edi, 6
	jg	.LBB0_380
# %bb.368:
	cmp	edi, 3
	jle	.LBB0_369
# %bb.374:
	cmp	edi, 4
	je	.LBB0_421
# %bb.375:
	cmp	edi, 5
	je	.LBB0_437
# %bb.376:
	cmp	edi, 6
	jne	.LBB0_1013
# %bb.377:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.378:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_453
# %bb.379:
	xor	esi, esi
.LBB0_462:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_464
.LBB0_463:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_463
.LBB0_464:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_465:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_465
	jmp	.LBB0_1013
.LBB0_194:
	cmp	edi, 6
	jg	.LBB0_207
# %bb.195:
	cmp	edi, 3
	jle	.LBB0_196
# %bb.201:
	cmp	edi, 4
	je	.LBB0_248
# %bb.202:
	cmp	edi, 5
	je	.LBB0_264
# %bb.203:
	cmp	edi, 6
	jne	.LBB0_1013
# %bb.204:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.205:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_280
# %bb.206:
	xor	esi, esi
.LBB0_289:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_291
.LBB0_290:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_290
.LBB0_291:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_292:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_292
	jmp	.LBB0_1013
.LBB0_540:
	cmp	edi, 6
	jg	.LBB0_553
# %bb.541:
	cmp	edi, 3
	jle	.LBB0_542
# %bb.547:
	cmp	edi, 4
	je	.LBB0_594
# %bb.548:
	cmp	edi, 5
	je	.LBB0_610
# %bb.549:
	cmp	edi, 6
	jne	.LBB0_1013
# %bb.550:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.551:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_626
# %bb.552:
	xor	esi, esi
.LBB0_635:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_637
.LBB0_636:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_636
.LBB0_637:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_638:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_638
	jmp	.LBB0_1013
.LBB0_719:
	cmp	edi, 8
	jle	.LBB0_720
# %bb.725:
	cmp	edi, 9
	je	.LBB0_826
# %bb.726:
	cmp	edi, 11
	je	.LBB0_834
# %bb.727:
	cmp	edi, 12
	jne	.LBB0_1013
# %bb.728:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.729:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_850
# %bb.730:
	xor	esi, esi
.LBB0_859:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_861
.LBB0_860:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_860
.LBB0_861:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_862:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_862
	jmp	.LBB0_1013
.LBB0_869:
	cmp	edi, 8
	jle	.LBB0_870
# %bb.875:
	cmp	edi, 9
	je	.LBB0_976
# %bb.876:
	cmp	edi, 11
	je	.LBB0_984
# %bb.877:
	cmp	edi, 12
	jne	.LBB0_1013
# %bb.878:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.879:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_1000
# %bb.880:
	xor	esi, esi
.LBB0_1009:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_1011
.LBB0_1010:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	mulsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_1010
.LBB0_1011:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1012
	jmp	.LBB0_1013
.LBB0_34:
	cmp	edi, 8
	jle	.LBB0_35
# %bb.40:
	cmp	edi, 9
	je	.LBB0_149
# %bb.41:
	cmp	edi, 11
	je	.LBB0_165
# %bb.42:
	cmp	edi, 12
	jne	.LBB0_1013
# %bb.43:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.44:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_181
# %bb.45:
	xor	esi, esi
.LBB0_190:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_192
.LBB0_191:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_191
.LBB0_192:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_193:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_193
	jmp	.LBB0_1013
.LBB0_380:
	cmp	edi, 8
	jle	.LBB0_381
# %bb.386:
	cmp	edi, 9
	je	.LBB0_495
# %bb.387:
	cmp	edi, 11
	je	.LBB0_511
# %bb.388:
	cmp	edi, 12
	jne	.LBB0_1013
# %bb.389:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.390:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_527
# %bb.391:
	xor	esi, esi
.LBB0_536:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_538
.LBB0_537:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_537
.LBB0_538:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_539:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_539
	jmp	.LBB0_1013
.LBB0_207:
	cmp	edi, 8
	jle	.LBB0_208
# %bb.213:
	cmp	edi, 9
	je	.LBB0_322
# %bb.214:
	cmp	edi, 11
	je	.LBB0_338
# %bb.215:
	cmp	edi, 12
	jne	.LBB0_1013
# %bb.216:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.217:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_354
# %bb.218:
	xor	esi, esi
.LBB0_363:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_365
.LBB0_364:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_364
.LBB0_365:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_366:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_366
	jmp	.LBB0_1013
.LBB0_553:
	cmp	edi, 8
	jle	.LBB0_554
# %bb.559:
	cmp	edi, 9
	je	.LBB0_668
# %bb.560:
	cmp	edi, 11
	je	.LBB0_684
# %bb.561:
	cmp	edi, 12
	jne	.LBB0_1013
# %bb.562:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.563:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_700
# %bb.564:
	xor	esi, esi
.LBB0_709:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_711
.LBB0_710:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_710
.LBB0_711:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_712:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_712
	jmp	.LBB0_1013
.LBB0_6:
	cmp	edi, 2
	je	.LBB0_731
# %bb.7:
	cmp	edi, 3
	jne	.LBB0_1013
# %bb.8:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.9:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_747
# %bb.10:
	xor	edi, edi
.LBB0_756:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_758
.LBB0_757:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_757
.LBB0_758:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_759:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_759
	jmp	.LBB0_1013
.LBB0_16:
	cmp	edi, 2
	je	.LBB0_881
# %bb.17:
	cmp	edi, 3
	jne	.LBB0_1013
# %bb.18:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.19:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_897
# %bb.20:
	xor	edi, edi
.LBB0_906:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_908
.LBB0_907:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_907
.LBB0_908:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_909:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_909
	jmp	.LBB0_1013
.LBB0_23:
	cmp	edi, 2
	je	.LBB0_46
# %bb.24:
	cmp	edi, 3
	jne	.LBB0_1013
# %bb.25:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.26:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_62
# %bb.27:
	xor	esi, esi
.LBB0_71:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_73
.LBB0_72:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_72
.LBB0_73:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_74:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_74
	jmp	.LBB0_1013
.LBB0_369:
	cmp	edi, 2
	je	.LBB0_392
# %bb.370:
	cmp	edi, 3
	jne	.LBB0_1013
# %bb.371:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.372:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_408
# %bb.373:
	xor	esi, esi
.LBB0_417:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_419
.LBB0_418:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_418
.LBB0_419:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_420:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_420
	jmp	.LBB0_1013
.LBB0_196:
	cmp	edi, 2
	je	.LBB0_219
# %bb.197:
	cmp	edi, 3
	jne	.LBB0_1013
# %bb.198:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.199:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_235
# %bb.200:
	xor	esi, esi
.LBB0_244:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_246
.LBB0_245:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_245
.LBB0_246:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_247:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_247
	jmp	.LBB0_1013
.LBB0_542:
	cmp	edi, 2
	je	.LBB0_565
# %bb.543:
	cmp	edi, 3
	jne	.LBB0_1013
# %bb.544:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.545:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_581
# %bb.546:
	xor	esi, esi
.LBB0_590:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_592
.LBB0_591:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_591
.LBB0_592:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_593
	jmp	.LBB0_1013
.LBB0_720:
	cmp	edi, 7
	je	.LBB0_805
# %bb.721:
	cmp	edi, 8
	jne	.LBB0_1013
# %bb.722:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.723:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_821
# %bb.724:
	xor	edi, edi
	jmp	.LBB0_823
.LBB0_870:
	cmp	edi, 7
	je	.LBB0_955
# %bb.871:
	cmp	edi, 8
	jne	.LBB0_1013
# %bb.872:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.873:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_971
# %bb.874:
	xor	edi, edi
	jmp	.LBB0_973
.LBB0_35:
	cmp	edi, 7
	je	.LBB0_120
# %bb.36:
	cmp	edi, 8
	jne	.LBB0_1013
# %bb.37:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.38:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_136
# %bb.39:
	xor	esi, esi
.LBB0_145:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_147
.LBB0_146:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_146
.LBB0_147:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_148:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_148
	jmp	.LBB0_1013
.LBB0_381:
	cmp	edi, 7
	je	.LBB0_466
# %bb.382:
	cmp	edi, 8
	jne	.LBB0_1013
# %bb.383:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.384:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_482
# %bb.385:
	xor	esi, esi
.LBB0_491:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_493
.LBB0_492:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_492
.LBB0_493:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_494:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_494
	jmp	.LBB0_1013
.LBB0_208:
	cmp	edi, 7
	je	.LBB0_293
# %bb.209:
	cmp	edi, 8
	jne	.LBB0_1013
# %bb.210:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.211:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_309
# %bb.212:
	xor	esi, esi
.LBB0_318:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_320
.LBB0_319:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_319
.LBB0_320:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_321:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_321
	jmp	.LBB0_1013
.LBB0_554:
	cmp	edi, 7
	je	.LBB0_639
# %bb.555:
	cmp	edi, 8
	jne	.LBB0_1013
# %bb.556:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.557:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_655
# %bb.558:
	xor	esi, esi
.LBB0_664:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_666
.LBB0_665:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_665
.LBB0_666:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_667:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_667
	jmp	.LBB0_1013
.LBB0_760:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.761:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_763
# %bb.762:
	xor	esi, esi
.LBB0_772:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_774
.LBB0_773:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_773
.LBB0_774:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_775:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_775
	jmp	.LBB0_1013
.LBB0_776:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.777:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_779
# %bb.778:
	xor	esi, esi
.LBB0_788:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_790
.LBB0_789:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_789
.LBB0_790:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_791:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_791
	jmp	.LBB0_1013
.LBB0_910:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.911:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_913
# %bb.912:
	xor	esi, esi
.LBB0_922:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_924
.LBB0_923:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_923
.LBB0_924:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_925:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_925
	jmp	.LBB0_1013
.LBB0_926:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.927:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_929
# %bb.928:
	xor	esi, esi
.LBB0_938:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_940
.LBB0_939:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	imul	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_939
.LBB0_940:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_941:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_941
	jmp	.LBB0_1013
.LBB0_75:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.76:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_78
# %bb.77:
	xor	esi, esi
.LBB0_87:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_89
.LBB0_88:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_88
.LBB0_89:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_90:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_90
	jmp	.LBB0_1013
.LBB0_91:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.92:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_94
# %bb.93:
	xor	esi, esi
.LBB0_103:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_105
.LBB0_104:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_104
.LBB0_105:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_106:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_106
	jmp	.LBB0_1013
.LBB0_421:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.422:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_424
# %bb.423:
	xor	esi, esi
.LBB0_433:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_435
.LBB0_434:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_434
.LBB0_435:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_436:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_436
	jmp	.LBB0_1013
.LBB0_437:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.438:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_440
# %bb.439:
	xor	esi, esi
.LBB0_449:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_451
.LBB0_450:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_450
.LBB0_451:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_452:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_452
	jmp	.LBB0_1013
.LBB0_248:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.249:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_251
# %bb.250:
	xor	esi, esi
.LBB0_260:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_262
.LBB0_261:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_261
.LBB0_262:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_263:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_263
	jmp	.LBB0_1013
.LBB0_264:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.265:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_267
# %bb.266:
	xor	esi, esi
.LBB0_276:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_278
.LBB0_277:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_277
.LBB0_278:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_279:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_279
	jmp	.LBB0_1013
.LBB0_594:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.595:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_597
# %bb.596:
	xor	esi, esi
.LBB0_606:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_608
.LBB0_607:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_607
.LBB0_608:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_609:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_609
	jmp	.LBB0_1013
.LBB0_610:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.611:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_613
# %bb.612:
	xor	esi, esi
.LBB0_622:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_624
.LBB0_623:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_623
.LBB0_624:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_625:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_625
	jmp	.LBB0_1013
.LBB0_826:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.827:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_829
# %bb.828:
	xor	edi, edi
	jmp	.LBB0_831
.LBB0_834:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.835:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_837
# %bb.836:
	xor	esi, esi
.LBB0_846:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_848
.LBB0_847:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_847
.LBB0_848:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_849:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_849
	jmp	.LBB0_1013
.LBB0_976:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.977:
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB0_979
# %bb.978:
	xor	edi, edi
	jmp	.LBB0_981
.LBB0_984:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.985:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_987
# %bb.986:
	xor	esi, esi
.LBB0_996:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_998
.LBB0_997:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	mulss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_997
.LBB0_998:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_999:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_999
	jmp	.LBB0_1013
.LBB0_149:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.150:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_152
# %bb.151:
	xor	esi, esi
.LBB0_161:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_163
.LBB0_162:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_162
.LBB0_163:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_164:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_164
	jmp	.LBB0_1013
.LBB0_165:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.166:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_168
# %bb.167:
	xor	esi, esi
.LBB0_177:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_179
.LBB0_178:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_178
.LBB0_179:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_180:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_180
	jmp	.LBB0_1013
.LBB0_495:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.496:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_498
# %bb.497:
	xor	esi, esi
.LBB0_507:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_509
.LBB0_508:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_508
.LBB0_509:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_510
	jmp	.LBB0_1013
.LBB0_511:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.512:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_514
# %bb.513:
	xor	esi, esi
.LBB0_523:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_525
.LBB0_524:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_524
.LBB0_525:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_526:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_526
	jmp	.LBB0_1013
.LBB0_322:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.323:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_325
# %bb.324:
	xor	esi, esi
.LBB0_334:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_336
.LBB0_335:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_335
.LBB0_336:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_337:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_337
	jmp	.LBB0_1013
.LBB0_338:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.339:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_341
# %bb.340:
	xor	esi, esi
.LBB0_350:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_352
.LBB0_351:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_351
.LBB0_352:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_353:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_353
	jmp	.LBB0_1013
.LBB0_668:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.669:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_671
# %bb.670:
	xor	esi, esi
.LBB0_680:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_682
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_681
.LBB0_682:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_683:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_683
	jmp	.LBB0_1013
.LBB0_684:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.685:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_687
# %bb.686:
	xor	esi, esi
.LBB0_696:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_698
.LBB0_697:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_697
.LBB0_698:
	cmp	rax, 3
	jb	.LBB0_1013
.LBB0_699:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_699
	jmp	.LBB0_1013
.LBB0_731:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.732:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_734
# %bb.733:
	xor	edi, edi
.LBB0_743:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_745
.LBB0_744:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_744
.LBB0_745:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_746:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_746
	jmp	.LBB0_1013
.LBB0_881:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.882:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_884
# %bb.883:
	xor	edi, edi
.LBB0_893:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_895
.LBB0_894:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_894
.LBB0_895:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_896:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_896
	jmp	.LBB0_1013
.LBB0_46:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.47:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_49
# %bb.48:
	xor	esi, esi
.LBB0_58:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_60
.LBB0_59:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_59
.LBB0_60:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_61:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_61
	jmp	.LBB0_1013
.LBB0_392:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.393:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_395
# %bb.394:
	xor	esi, esi
.LBB0_404:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_406
.LBB0_405:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_405
.LBB0_406:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_407:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_407
	jmp	.LBB0_1013
.LBB0_219:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.220:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_222
# %bb.221:
	xor	esi, esi
.LBB0_231:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_233
.LBB0_232:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_232
.LBB0_233:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_234:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_234
	jmp	.LBB0_1013
.LBB0_565:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.566:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_568
# %bb.567:
	xor	esi, esi
.LBB0_577:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_579
.LBB0_578:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_578
.LBB0_579:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_580:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_580
	jmp	.LBB0_1013
.LBB0_805:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.806:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_808
# %bb.807:
	xor	esi, esi
.LBB0_817:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_819
.LBB0_818:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_818
.LBB0_819:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_820:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_820
	jmp	.LBB0_1013
.LBB0_955:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.956:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_958
# %bb.957:
	xor	esi, esi
.LBB0_967:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_969
.LBB0_968:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	imul	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_968
.LBB0_969:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_970:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_970
	jmp	.LBB0_1013
.LBB0_120:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.121:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_123
# %bb.122:
	xor	esi, esi
.LBB0_132:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_134
.LBB0_133:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_133
.LBB0_134:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_135:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_135
	jmp	.LBB0_1013
.LBB0_466:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.467:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_469
# %bb.468:
	xor	esi, esi
.LBB0_478:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_480
.LBB0_479:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_479
.LBB0_480:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_481:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_481
	jmp	.LBB0_1013
.LBB0_293:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.294:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_296
# %bb.295:
	xor	esi, esi
.LBB0_305:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_307
.LBB0_306:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_306
.LBB0_307:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_308:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_308
	jmp	.LBB0_1013
.LBB0_639:
	test	r9d, r9d
	jle	.LBB0_1013
# %bb.640:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_642
# %bb.641:
	xor	esi, esi
.LBB0_651:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_653
.LBB0_652:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_652
.LBB0_653:
	cmp	r9, 3
	jb	.LBB0_1013
.LBB0_654:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_654
	jmp	.LBB0_1013
.LBB0_792:
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
	jne	.LBB0_801
# %bb.793:
	and	al, dil
	jne	.LBB0_801
# %bb.794:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_795
# %bb.796:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_797:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_797
	jmp	.LBB0_798
.LBB0_942:
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
	jne	.LBB0_951
# %bb.943:
	and	al, dil
	jne	.LBB0_951
# %bb.944:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_945
# %bb.946:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_947:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_947
	jmp	.LBB0_948
.LBB0_107:
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
	jne	.LBB0_116
# %bb.108:
	and	al, dil
	jne	.LBB0_116
# %bb.109:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_110
# %bb.111:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_112:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_112
	jmp	.LBB0_113
.LBB0_453:
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
	jne	.LBB0_462
# %bb.454:
	and	al, dil
	jne	.LBB0_462
# %bb.455:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_456
# %bb.457:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_458:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_458
	jmp	.LBB0_459
.LBB0_280:
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
	jne	.LBB0_289
# %bb.281:
	and	al, dil
	jne	.LBB0_289
# %bb.282:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_283
# %bb.284:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_285:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_285
	jmp	.LBB0_286
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
	jne	.LBB0_635
# %bb.627:
	and	al, dil
	jne	.LBB0_635
# %bb.628:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_629
# %bb.630:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_631:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_631
	jmp	.LBB0_632
.LBB0_850:
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
	jne	.LBB0_859
# %bb.851:
	and	al, dil
	jne	.LBB0_859
# %bb.852:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_853
# %bb.854:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_855:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_855
	jmp	.LBB0_856
.LBB0_1000:
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
	jne	.LBB0_1009
# %bb.1001:
	and	al, dil
	jne	.LBB0_1009
# %bb.1002:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_1003
# %bb.1004:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_1005:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_1005
	jmp	.LBB0_1006
.LBB0_181:
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
	jne	.LBB0_190
# %bb.182:
	and	al, dil
	jne	.LBB0_190
# %bb.183:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_184
# %bb.185:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_186:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_186
	jmp	.LBB0_187
.LBB0_527:
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
	jne	.LBB0_536
# %bb.528:
	and	al, dil
	jne	.LBB0_536
# %bb.529:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_530
# %bb.531:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_532:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_532
	jmp	.LBB0_533
.LBB0_354:
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
	jne	.LBB0_363
# %bb.355:
	and	al, dil
	jne	.LBB0_363
# %bb.356:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_357
# %bb.358:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_359:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_359
	jmp	.LBB0_360
.LBB0_700:
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
	jne	.LBB0_709
# %bb.701:
	and	al, dil
	jne	.LBB0_709
# %bb.702:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_703
# %bb.704:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_705:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_705
	jmp	.LBB0_706
.LBB0_747:
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
	jne	.LBB0_756
# %bb.748:
	and	al, sil
	jne	.LBB0_756
# %bb.749:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_750
# %bb.751:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_752:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_752
	jmp	.LBB0_753
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
	seta	sil
	xor	edi, edi
	test	r9b, r11b
	jne	.LBB0_906
# %bb.898:
	and	al, sil
	jne	.LBB0_906
# %bb.899:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_900
# %bb.901:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [255,255,255,255,255,255,255,255]
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
	jmp	.LBB0_903
.LBB0_62:
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
	jne	.LBB0_71
# %bb.63:
	and	al, dil
	jne	.LBB0_71
# %bb.64:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_65
# %bb.66:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_67:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_67
	jmp	.LBB0_68
.LBB0_408:
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
	jne	.LBB0_417
# %bb.409:
	and	al, dil
	jne	.LBB0_417
# %bb.410:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_411
# %bb.412:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_413:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_413
	jmp	.LBB0_414
.LBB0_235:
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
	jne	.LBB0_244
# %bb.236:
	and	al, dil
	jne	.LBB0_244
# %bb.237:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_238
# %bb.239:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_240:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_240
	jmp	.LBB0_241
.LBB0_581:
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
	jne	.LBB0_590
# %bb.582:
	and	al, dil
	jne	.LBB0_590
# %bb.583:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_584
# %bb.585:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_586:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_586
	jmp	.LBB0_587
.LBB0_821:
	and	esi, -4
	xor	edi, edi
.LBB0_822:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_822
.LBB0_823:
	test	r9, r9
	je	.LBB0_1013
# %bb.824:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_825:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_825
	jmp	.LBB0_1013
.LBB0_971:
	and	esi, -4
	xor	edi, edi
.LBB0_972:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_972
.LBB0_973:
	test	r9, r9
	je	.LBB0_1013
# %bb.974:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_975:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_975
	jmp	.LBB0_1013
.LBB0_136:
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
	jne	.LBB0_145
# %bb.137:
	and	al, dil
	jne	.LBB0_145
# %bb.138:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_139
# %bb.140:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_141:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_141
	jmp	.LBB0_142
.LBB0_482:
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
	jne	.LBB0_491
# %bb.483:
	and	al, dil
	jne	.LBB0_491
# %bb.484:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_485
# %bb.486:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_487:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_487
	jmp	.LBB0_488
.LBB0_309:
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
# %bb.310:
	and	al, dil
	jne	.LBB0_318
# %bb.311:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_312
# %bb.313:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_314:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_314
	jmp	.LBB0_315
.LBB0_655:
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
	jne	.LBB0_664
# %bb.656:
	and	al, dil
	jne	.LBB0_664
# %bb.657:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_658
# %bb.659:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_660
	jmp	.LBB0_661
.LBB0_763:
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
	jne	.LBB0_772
# %bb.764:
	and	al, dil
	jne	.LBB0_772
# %bb.765:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_766
# %bb.767:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_768:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_768
	jmp	.LBB0_769
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
	jne	.LBB0_788
# %bb.780:
	and	al, dil
	jne	.LBB0_788
# %bb.781:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_782
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
	jmp	.LBB0_785
.LBB0_913:
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
	jne	.LBB0_922
# %bb.914:
	and	al, dil
	jne	.LBB0_922
# %bb.915:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_916
# %bb.917:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_918:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_918
	jmp	.LBB0_919
.LBB0_929:
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
	jne	.LBB0_938
# %bb.930:
	and	al, dil
	jne	.LBB0_938
# %bb.931:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_932
# %bb.933:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_934:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_934
	jmp	.LBB0_935
.LBB0_78:
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
	jne	.LBB0_87
# %bb.79:
	and	al, dil
	jne	.LBB0_87
# %bb.80:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_81
# %bb.82:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_83:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_83
	jmp	.LBB0_84
.LBB0_94:
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
	jne	.LBB0_103
# %bb.95:
	and	al, dil
	jne	.LBB0_103
# %bb.96:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_97
# %bb.98:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_99:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_99
	jmp	.LBB0_100
.LBB0_424:
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
	jne	.LBB0_433
# %bb.425:
	and	al, dil
	jne	.LBB0_433
# %bb.426:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_427
# %bb.428:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_429:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_429
	jmp	.LBB0_430
.LBB0_440:
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
# %bb.441:
	and	al, dil
	jne	.LBB0_449
# %bb.442:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_443
# %bb.444:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_445:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_445
	jmp	.LBB0_446
.LBB0_251:
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
	jne	.LBB0_260
# %bb.252:
	and	al, dil
	jne	.LBB0_260
# %bb.253:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_254
# %bb.255:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_256:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_256
	jmp	.LBB0_257
.LBB0_267:
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
	jne	.LBB0_276
# %bb.268:
	and	al, dil
	jne	.LBB0_276
# %bb.269:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_270
# %bb.271:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_272:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_272
	jmp	.LBB0_273
.LBB0_597:
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
	jne	.LBB0_606
# %bb.598:
	and	al, dil
	jne	.LBB0_606
# %bb.599:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_600
# %bb.601:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_602:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_602
	jmp	.LBB0_603
.LBB0_613:
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
	jne	.LBB0_622
# %bb.614:
	and	al, dil
	jne	.LBB0_622
# %bb.615:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_616
# %bb.617:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_618:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_618
	jmp	.LBB0_619
.LBB0_829:
	and	esi, -4
	xor	edi, edi
.LBB0_830:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_830
.LBB0_831:
	test	r9, r9
	je	.LBB0_1013
# %bb.832:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_833:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_833
	jmp	.LBB0_1013
.LBB0_837:
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
	jne	.LBB0_846
# %bb.838:
	and	al, dil
	jne	.LBB0_846
# %bb.839:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_840
# %bb.841:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_842:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_842
	jmp	.LBB0_843
.LBB0_979:
	and	esi, -4
	xor	edi, edi
.LBB0_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_980
.LBB0_981:
	test	r9, r9
	je	.LBB0_1013
# %bb.982:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB0_983:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rdi]
	imul	rax, qword ptr [rdx + 8*rdi]
	mov	qword ptr [rsi + 8*rdi], rax
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB0_983
.LBB0_1013:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB0_987:
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
	jne	.LBB0_996
# %bb.988:
	and	al, dil
	jne	.LBB0_996
# %bb.989:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_990
# %bb.991:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_992:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_992
	jmp	.LBB0_993
.LBB0_152:
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
	jne	.LBB0_161
# %bb.153:
	and	al, dil
	jne	.LBB0_161
# %bb.154:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_155
# %bb.156:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_157:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_157
	jmp	.LBB0_158
.LBB0_168:
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
	jne	.LBB0_177
# %bb.169:
	and	al, dil
	jne	.LBB0_177
# %bb.170:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_171
# %bb.172:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_173:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_173
	jmp	.LBB0_174
.LBB0_498:
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
	jne	.LBB0_507
# %bb.499:
	and	al, dil
	jne	.LBB0_507
# %bb.500:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_501
# %bb.502:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_503:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_503
	jmp	.LBB0_504
.LBB0_514:
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
	jne	.LBB0_523
# %bb.515:
	and	al, dil
	jne	.LBB0_523
# %bb.516:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_517
# %bb.518:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_519:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_519
	jmp	.LBB0_520
.LBB0_325:
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
	jne	.LBB0_334
# %bb.326:
	and	al, dil
	jne	.LBB0_334
# %bb.327:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_328
# %bb.329:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_330:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_330
	jmp	.LBB0_331
.LBB0_341:
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
	jne	.LBB0_350
# %bb.342:
	and	al, dil
	jne	.LBB0_350
# %bb.343:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_344
# %bb.345:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_346:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_346
	jmp	.LBB0_347
.LBB0_671:
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
	jne	.LBB0_680
# %bb.672:
	and	al, dil
	jne	.LBB0_680
# %bb.673:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_674
# %bb.675:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_676
	jmp	.LBB0_677
.LBB0_687:
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
	jne	.LBB0_696
# %bb.688:
	and	al, dil
	jne	.LBB0_696
# %bb.689:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_690
# %bb.691:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_692:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_692
	jmp	.LBB0_693
.LBB0_734:
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
	jne	.LBB0_743
# %bb.735:
	and	al, sil
	jne	.LBB0_743
# %bb.736:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_737
# %bb.738:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_739:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_739
	jmp	.LBB0_740
.LBB0_884:
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
	jne	.LBB0_893
# %bb.885:
	and	al, sil
	jne	.LBB0_893
# %bb.886:
	mov	edi, r10d
	and	edi, -32
	lea	rax, [rdi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_887
# %bb.888:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm0, xmmword ptr [rip + .LCPI0_0] # xmm0 = [255,255,255,255,255,255,255,255]
.LBB0_889:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_889
	jmp	.LBB0_890
.LBB0_49:
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
	jne	.LBB0_58
# %bb.50:
	and	al, dil
	jne	.LBB0_58
# %bb.51:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_52
# %bb.53:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_54:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_54
	jmp	.LBB0_55
.LBB0_395:
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
	jne	.LBB0_404
# %bb.396:
	and	al, dil
	jne	.LBB0_404
# %bb.397:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_398
# %bb.399:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_400:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_400
	jmp	.LBB0_401
.LBB0_222:
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
	jne	.LBB0_231
# %bb.223:
	and	al, dil
	jne	.LBB0_231
# %bb.224:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_225
# %bb.226:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_227:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_227
	jmp	.LBB0_228
.LBB0_568:
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
	jne	.LBB0_577
# %bb.569:
	and	al, dil
	jne	.LBB0_577
# %bb.570:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_571
# %bb.572:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_573:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_573
	jmp	.LBB0_574
.LBB0_808:
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
	jne	.LBB0_817
# %bb.809:
	and	al, dil
	jne	.LBB0_817
# %bb.810:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_811
# %bb.812:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_813:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_813
	jmp	.LBB0_814
.LBB0_958:
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
	jne	.LBB0_967
# %bb.959:
	and	al, dil
	jne	.LBB0_967
# %bb.960:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_961
# %bb.962:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_963:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_963
	jmp	.LBB0_964
.LBB0_123:
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
	jne	.LBB0_132
# %bb.124:
	and	al, dil
	jne	.LBB0_132
# %bb.125:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_126
# %bb.127:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_128:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_128
	jmp	.LBB0_129
.LBB0_469:
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
	jne	.LBB0_478
# %bb.470:
	and	al, dil
	jne	.LBB0_478
# %bb.471:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_472
# %bb.473:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_474:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_474
	jmp	.LBB0_475
.LBB0_296:
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
	jne	.LBB0_305
# %bb.297:
	and	al, dil
	jne	.LBB0_305
# %bb.298:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_299
# %bb.300:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_301:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_301
	jmp	.LBB0_302
.LBB0_642:
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
	jne	.LBB0_651
# %bb.643:
	and	al, dil
	jne	.LBB0_651
# %bb.644:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_645
# %bb.646:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_647:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_647
	jmp	.LBB0_648
.LBB0_795:
	xor	edi, edi
.LBB0_798:
	test	r9b, 1
	je	.LBB0_800
# %bb.799:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_800:
	cmp	rsi, r10
	jne	.LBB0_801
	jmp	.LBB0_1013
.LBB0_945:
	xor	edi, edi
.LBB0_948:
	test	r9b, 1
	je	.LBB0_950
# %bb.949:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_950:
	cmp	rsi, r10
	jne	.LBB0_951
	jmp	.LBB0_1013
.LBB0_110:
	xor	edi, edi
.LBB0_113:
	test	r9b, 1
	je	.LBB0_115
# %bb.114:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_115:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_116
.LBB0_456:
	xor	edi, edi
.LBB0_459:
	test	r9b, 1
	je	.LBB0_461
# %bb.460:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_461:
	cmp	rsi, r10
	jne	.LBB0_462
	jmp	.LBB0_1013
.LBB0_283:
	xor	edi, edi
.LBB0_286:
	test	r9b, 1
	je	.LBB0_288
# %bb.287:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_288:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_289
.LBB0_629:
	xor	edi, edi
.LBB0_632:
	test	r9b, 1
	je	.LBB0_634
# %bb.633:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_634:
	cmp	rsi, r10
	jne	.LBB0_635
	jmp	.LBB0_1013
.LBB0_853:
	xor	edi, edi
.LBB0_856:
	test	r9b, 1
	je	.LBB0_858
# %bb.857:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_858:
	cmp	rsi, r10
	jne	.LBB0_859
	jmp	.LBB0_1013
.LBB0_1003:
	xor	edi, edi
.LBB0_1006:
	test	r9b, 1
	je	.LBB0_1008
# %bb.1007:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	mulpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_1008:
	cmp	rsi, r10
	jne	.LBB0_1009
	jmp	.LBB0_1013
.LBB0_184:
	xor	edi, edi
.LBB0_187:
	test	r9b, 1
	je	.LBB0_189
# %bb.188:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_189:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_190
.LBB0_530:
	xor	edi, edi
.LBB0_533:
	test	r9b, 1
	je	.LBB0_535
# %bb.534:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_535:
	cmp	rsi, r10
	jne	.LBB0_536
	jmp	.LBB0_1013
.LBB0_357:
	xor	edi, edi
.LBB0_360:
	test	r9b, 1
	je	.LBB0_362
# %bb.361:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_362:
	cmp	rsi, r10
	jne	.LBB0_363
	jmp	.LBB0_1013
.LBB0_703:
	xor	edi, edi
.LBB0_706:
	test	r9b, 1
	je	.LBB0_708
# %bb.707:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_708:
	cmp	rsi, r10
	jne	.LBB0_709
	jmp	.LBB0_1013
.LBB0_750:
	xor	eax, eax
.LBB0_753:
	test	r9b, 1
	je	.LBB0_755
# %bb.754:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_0] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_755:
	cmp	rdi, r10
	jne	.LBB0_756
	jmp	.LBB0_1013
.LBB0_900:
	xor	eax, eax
.LBB0_903:
	test	r9b, 1
	je	.LBB0_905
# %bb.904:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_0] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_905:
	cmp	rdi, r10
	jne	.LBB0_906
	jmp	.LBB0_1013
.LBB0_65:
	xor	edi, edi
.LBB0_68:
	test	r9b, 1
	je	.LBB0_70
# %bb.69:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_70:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_71
.LBB0_411:
	xor	edi, edi
.LBB0_414:
	test	r9b, 1
	je	.LBB0_416
# %bb.415:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_416:
	cmp	rsi, r10
	jne	.LBB0_417
	jmp	.LBB0_1013
.LBB0_238:
	xor	edi, edi
.LBB0_241:
	test	r9b, 1
	je	.LBB0_243
# %bb.242:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_243:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_244
.LBB0_584:
	xor	edi, edi
.LBB0_587:
	test	r9b, 1
	je	.LBB0_589
# %bb.588:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_589:
	cmp	rsi, r10
	jne	.LBB0_590
	jmp	.LBB0_1013
.LBB0_139:
	xor	edi, edi
.LBB0_142:
	test	r9b, 1
	je	.LBB0_144
# %bb.143:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_144:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_145
.LBB0_485:
	xor	edi, edi
.LBB0_488:
	test	r9b, 1
	je	.LBB0_490
# %bb.489:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_490:
	cmp	rsi, r10
	jne	.LBB0_491
	jmp	.LBB0_1013
.LBB0_312:
	xor	edi, edi
.LBB0_315:
	test	r9b, 1
	je	.LBB0_317
# %bb.316:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_317:
	cmp	rsi, r10
	jne	.LBB0_318
	jmp	.LBB0_1013
.LBB0_658:
	xor	edi, edi
.LBB0_661:
	test	r9b, 1
	je	.LBB0_663
# %bb.662:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_663:
	cmp	rsi, r10
	jne	.LBB0_664
	jmp	.LBB0_1013
.LBB0_766:
	xor	edi, edi
.LBB0_769:
	test	r9b, 1
	je	.LBB0_771
# %bb.770:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_771:
	cmp	rsi, r10
	jne	.LBB0_772
	jmp	.LBB0_1013
.LBB0_782:
	xor	edi, edi
.LBB0_785:
	test	r9b, 1
	je	.LBB0_787
# %bb.786:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_787:
	cmp	rsi, r10
	jne	.LBB0_788
	jmp	.LBB0_1013
.LBB0_916:
	xor	edi, edi
.LBB0_919:
	test	r9b, 1
	je	.LBB0_921
# %bb.920:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_921:
	cmp	rsi, r10
	jne	.LBB0_922
	jmp	.LBB0_1013
.LBB0_932:
	xor	edi, edi
.LBB0_935:
	test	r9b, 1
	je	.LBB0_937
# %bb.936:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	pmullw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_937:
	cmp	rsi, r10
	jne	.LBB0_938
	jmp	.LBB0_1013
.LBB0_81:
	xor	edi, edi
.LBB0_84:
	test	r9b, 1
	je	.LBB0_86
# %bb.85:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_86:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_87
.LBB0_97:
	xor	edi, edi
.LBB0_100:
	test	r9b, 1
	je	.LBB0_102
# %bb.101:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_102:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_103
.LBB0_427:
	xor	edi, edi
.LBB0_430:
	test	r9b, 1
	je	.LBB0_432
# %bb.431:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_432:
	cmp	rsi, r10
	jne	.LBB0_433
	jmp	.LBB0_1013
.LBB0_443:
	xor	edi, edi
.LBB0_446:
	test	r9b, 1
	je	.LBB0_448
# %bb.447:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_448:
	cmp	rsi, r10
	jne	.LBB0_449
	jmp	.LBB0_1013
.LBB0_254:
	xor	edi, edi
.LBB0_257:
	test	r9b, 1
	je	.LBB0_259
# %bb.258:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_259:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_260
.LBB0_270:
	xor	edi, edi
.LBB0_273:
	test	r9b, 1
	je	.LBB0_275
# %bb.274:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_275:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_276
.LBB0_600:
	xor	edi, edi
.LBB0_603:
	test	r9b, 1
	je	.LBB0_605
# %bb.604:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_605:
	cmp	rsi, r10
	jne	.LBB0_606
	jmp	.LBB0_1013
.LBB0_616:
	xor	edi, edi
.LBB0_619:
	test	r9b, 1
	je	.LBB0_621
# %bb.620:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_621:
	cmp	rsi, r10
	jne	.LBB0_622
	jmp	.LBB0_1013
.LBB0_840:
	xor	edi, edi
.LBB0_843:
	test	r9b, 1
	je	.LBB0_845
# %bb.844:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_845:
	cmp	rsi, r10
	jne	.LBB0_846
	jmp	.LBB0_1013
.LBB0_990:
	xor	edi, edi
.LBB0_993:
	test	r9b, 1
	je	.LBB0_995
# %bb.994:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	mulps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_995:
	cmp	rsi, r10
	jne	.LBB0_996
	jmp	.LBB0_1013
.LBB0_155:
	xor	edi, edi
.LBB0_158:
	test	r9b, 1
	je	.LBB0_160
# %bb.159:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_160:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_161
.LBB0_171:
	xor	edi, edi
.LBB0_174:
	test	r9b, 1
	je	.LBB0_176
# %bb.175:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_176:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_177
.LBB0_501:
	xor	edi, edi
.LBB0_504:
	test	r9b, 1
	je	.LBB0_506
# %bb.505:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_506:
	cmp	rsi, r10
	jne	.LBB0_507
	jmp	.LBB0_1013
.LBB0_517:
	xor	edi, edi
.LBB0_520:
	test	r9b, 1
	je	.LBB0_522
# %bb.521:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_522:
	cmp	rsi, r10
	jne	.LBB0_523
	jmp	.LBB0_1013
.LBB0_328:
	xor	edi, edi
.LBB0_331:
	test	r9b, 1
	je	.LBB0_333
# %bb.332:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_333:
	cmp	rsi, r10
	jne	.LBB0_334
	jmp	.LBB0_1013
.LBB0_344:
	xor	edi, edi
.LBB0_347:
	test	r9b, 1
	je	.LBB0_349
# %bb.348:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_349:
	cmp	rsi, r10
	jne	.LBB0_350
	jmp	.LBB0_1013
.LBB0_674:
	xor	edi, edi
.LBB0_677:
	test	r9b, 1
	je	.LBB0_679
# %bb.678:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_679:
	cmp	rsi, r10
	jne	.LBB0_680
	jmp	.LBB0_1013
.LBB0_690:
	xor	edi, edi
.LBB0_693:
	test	r9b, 1
	je	.LBB0_695
# %bb.694:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_695:
	cmp	rsi, r10
	jne	.LBB0_696
	jmp	.LBB0_1013
.LBB0_737:
	xor	eax, eax
.LBB0_740:
	test	r9b, 1
	je	.LBB0_742
# %bb.741:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_0] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_742:
	cmp	rdi, r10
	jne	.LBB0_743
	jmp	.LBB0_1013
.LBB0_887:
	xor	eax, eax
.LBB0_890:
	test	r9b, 1
	je	.LBB0_892
# %bb.891:
	movdqu	xmm1, xmmword ptr [rdx + rax]
	movdqu	xmm2, xmmword ptr [rdx + rax + 16]
	movdqu	xmm3, xmmword ptr [rcx + rax]
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	pmovzxbw	xmm4, xmm1                      # xmm4 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	punpckhbw	xmm1, xmm1              # xmm1 = xmm1[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm3                      # xmm5 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	punpckhbw	xmm3, xmm3              # xmm3 = xmm3[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm3, xmm1
	movdqa	xmm1, xmmword ptr [rip + .LCPI0_0] # xmm1 = [255,255,255,255,255,255,255,255]
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
.LBB0_892:
	cmp	rdi, r10
	jne	.LBB0_893
	jmp	.LBB0_1013
.LBB0_52:
	xor	edi, edi
.LBB0_55:
	test	r9b, 1
	je	.LBB0_57
# %bb.56:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_57:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_58
.LBB0_398:
	xor	edi, edi
.LBB0_401:
	test	r9b, 1
	je	.LBB0_403
# %bb.402:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_403:
	cmp	rsi, r10
	jne	.LBB0_404
	jmp	.LBB0_1013
.LBB0_225:
	xor	edi, edi
.LBB0_228:
	test	r9b, 1
	je	.LBB0_230
# %bb.229:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_230:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_231
.LBB0_571:
	xor	edi, edi
.LBB0_574:
	test	r9b, 1
	je	.LBB0_576
# %bb.575:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_576:
	cmp	rsi, r10
	jne	.LBB0_577
	jmp	.LBB0_1013
.LBB0_811:
	xor	edi, edi
.LBB0_814:
	test	r9b, 1
	je	.LBB0_816
# %bb.815:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_816:
	cmp	rsi, r10
	jne	.LBB0_817
	jmp	.LBB0_1013
.LBB0_961:
	xor	edi, edi
.LBB0_964:
	test	r9b, 1
	je	.LBB0_966
# %bb.965:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	pmulld	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_966:
	cmp	rsi, r10
	jne	.LBB0_967
	jmp	.LBB0_1013
.LBB0_126:
	xor	edi, edi
.LBB0_129:
	test	r9b, 1
	je	.LBB0_131
# %bb.130:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_131:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_132
.LBB0_472:
	xor	edi, edi
.LBB0_475:
	test	r9b, 1
	je	.LBB0_477
# %bb.476:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_477:
	cmp	rsi, r10
	jne	.LBB0_478
	jmp	.LBB0_1013
.LBB0_299:
	xor	edi, edi
.LBB0_302:
	test	r9b, 1
	je	.LBB0_304
# %bb.303:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_304:
	cmp	rsi, r10
	je	.LBB0_1013
	jmp	.LBB0_305
.LBB0_645:
	xor	edi, edi
.LBB0_648:
	test	r9b, 1
	je	.LBB0_650
# %bb.649:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_650:
	cmp	rsi, r10
	jne	.LBB0_651
	jmp	.LBB0_1013
.Lfunc_end0:
	.size	arithmetic_binary_sse4, .Lfunc_end0-arithmetic_binary_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_arr_scalar_sse4
.LCPI1_0:
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
	cmp	sil, 20
	jg	.LBB1_12
# %bb.1:
	test	sil, sil
	je	.LBB1_23
# %bb.2:
	cmp	sil, 1
	je	.LBB1_31
# %bb.3:
	cmp	sil, 2
	jne	.LBB1_1069
# %bb.4:
	cmp	edi, 6
	jg	.LBB1_55
# %bb.5:
	cmp	edi, 3
	jle	.LBB1_97
# %bb.6:
	cmp	edi, 4
	je	.LBB1_157
# %bb.7:
	cmp	edi, 5
	je	.LBB1_160
# %bb.8:
	cmp	edi, 6
	jne	.LBB1_1069
# %bb.9:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.10:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_11
# %bb.265:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_453
# %bb.266:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_453
.LBB1_11:
	xor	esi, esi
.LBB1_625:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_627
.LBB1_626:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_626
.LBB1_627:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_628:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_628
	jmp	.LBB1_1069
.LBB1_12:
	cmp	sil, 21
	je	.LBB1_39
# %bb.13:
	cmp	sil, 22
	je	.LBB1_47
# %bb.14:
	cmp	sil, 23
	jne	.LBB1_1069
# %bb.15:
	cmp	edi, 6
	jg	.LBB1_62
# %bb.16:
	cmp	edi, 3
	jle	.LBB1_102
# %bb.17:
	cmp	edi, 4
	je	.LBB1_163
# %bb.18:
	cmp	edi, 5
	je	.LBB1_166
# %bb.19:
	cmp	edi, 6
	jne	.LBB1_1069
# %bb.20:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.21:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_22
# %bb.268:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_456
# %bb.269:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_456
.LBB1_22:
	xor	esi, esi
.LBB1_633:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_635
.LBB1_634:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_634
.LBB1_635:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_636:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_636
	jmp	.LBB1_1069
.LBB1_23:
	cmp	edi, 6
	jg	.LBB1_69
# %bb.24:
	cmp	edi, 3
	jle	.LBB1_107
# %bb.25:
	cmp	edi, 4
	je	.LBB1_169
# %bb.26:
	cmp	edi, 5
	je	.LBB1_172
# %bb.27:
	cmp	edi, 6
	jne	.LBB1_1069
# %bb.28:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.29:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_30
# %bb.271:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_459
# %bb.272:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_459
.LBB1_30:
	xor	esi, esi
.LBB1_641:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_643
.LBB1_642:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_642
.LBB1_643:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_644:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_644
	jmp	.LBB1_1069
.LBB1_31:
	cmp	edi, 6
	jg	.LBB1_76
# %bb.32:
	cmp	edi, 3
	jle	.LBB1_112
# %bb.33:
	cmp	edi, 4
	je	.LBB1_175
# %bb.34:
	cmp	edi, 5
	je	.LBB1_178
# %bb.35:
	cmp	edi, 6
	jne	.LBB1_1069
# %bb.36:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.37:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_38
# %bb.274:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_462
# %bb.275:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_462
.LBB1_38:
	xor	esi, esi
.LBB1_649:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_651
.LBB1_650:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_650
.LBB1_651:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_652:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_652
	jmp	.LBB1_1069
.LBB1_39:
	cmp	edi, 6
	jg	.LBB1_83
# %bb.40:
	cmp	edi, 3
	jle	.LBB1_117
# %bb.41:
	cmp	edi, 4
	je	.LBB1_181
# %bb.42:
	cmp	edi, 5
	je	.LBB1_184
# %bb.43:
	cmp	edi, 6
	jne	.LBB1_1069
# %bb.44:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.45:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_46
# %bb.277:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_465
# %bb.278:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_465
.LBB1_46:
	xor	esi, esi
.LBB1_657:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_659
.LBB1_658:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_658
.LBB1_659:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_660
	jmp	.LBB1_1069
.LBB1_47:
	cmp	edi, 6
	jg	.LBB1_90
# %bb.48:
	cmp	edi, 3
	jle	.LBB1_122
# %bb.49:
	cmp	edi, 4
	je	.LBB1_187
# %bb.50:
	cmp	edi, 5
	je	.LBB1_190
# %bb.51:
	cmp	edi, 6
	jne	.LBB1_1069
# %bb.52:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.53:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_54
# %bb.280:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_468
# %bb.281:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_468
.LBB1_54:
	xor	esi, esi
.LBB1_665:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_667
.LBB1_666:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_666
.LBB1_667:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_668:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_668
	jmp	.LBB1_1069
.LBB1_55:
	cmp	edi, 8
	jle	.LBB1_127
# %bb.56:
	cmp	edi, 9
	je	.LBB1_193
# %bb.57:
	cmp	edi, 11
	je	.LBB1_196
# %bb.58:
	cmp	edi, 12
	jne	.LBB1_1069
# %bb.59:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.60:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_61
# %bb.283:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_471
# %bb.284:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_471
.LBB1_61:
	xor	ecx, ecx
.LBB1_673:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_675
.LBB1_674:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_674
.LBB1_675:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_676
	jmp	.LBB1_1069
.LBB1_62:
	cmp	edi, 8
	jle	.LBB1_132
# %bb.63:
	cmp	edi, 9
	je	.LBB1_199
# %bb.64:
	cmp	edi, 11
	je	.LBB1_202
# %bb.65:
	cmp	edi, 12
	jne	.LBB1_1069
# %bb.66:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.67:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_68
# %bb.286:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_474
# %bb.287:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_474
.LBB1_68:
	xor	ecx, ecx
.LBB1_681:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_683
.LBB1_682:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_682
.LBB1_683:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_684:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_684
	jmp	.LBB1_1069
.LBB1_69:
	cmp	edi, 8
	jle	.LBB1_137
# %bb.70:
	cmp	edi, 9
	je	.LBB1_205
# %bb.71:
	cmp	edi, 11
	je	.LBB1_208
# %bb.72:
	cmp	edi, 12
	jne	.LBB1_1069
# %bb.73:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.74:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_75
# %bb.289:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_477
# %bb.290:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_477
.LBB1_75:
	xor	ecx, ecx
.LBB1_689:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_691
.LBB1_690:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_690
.LBB1_691:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_692:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_692
	jmp	.LBB1_1069
.LBB1_76:
	cmp	edi, 8
	jle	.LBB1_142
# %bb.77:
	cmp	edi, 9
	je	.LBB1_211
# %bb.78:
	cmp	edi, 11
	je	.LBB1_214
# %bb.79:
	cmp	edi, 12
	jne	.LBB1_1069
# %bb.80:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.81:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_82
# %bb.292:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_480
# %bb.293:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_480
.LBB1_82:
	xor	ecx, ecx
.LBB1_697:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_699
.LBB1_698:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_698
.LBB1_699:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_700:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_700
	jmp	.LBB1_1069
.LBB1_83:
	cmp	edi, 8
	jle	.LBB1_147
# %bb.84:
	cmp	edi, 9
	je	.LBB1_217
# %bb.85:
	cmp	edi, 11
	je	.LBB1_220
# %bb.86:
	cmp	edi, 12
	jne	.LBB1_1069
# %bb.87:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.88:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_89
# %bb.295:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_483
# %bb.296:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_483
.LBB1_89:
	xor	ecx, ecx
.LBB1_705:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_707
.LBB1_706:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_706
.LBB1_707:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_708:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_708
	jmp	.LBB1_1069
.LBB1_90:
	cmp	edi, 8
	jle	.LBB1_152
# %bb.91:
	cmp	edi, 9
	je	.LBB1_223
# %bb.92:
	cmp	edi, 11
	je	.LBB1_226
# %bb.93:
	cmp	edi, 12
	jne	.LBB1_1069
# %bb.94:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.95:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_96
# %bb.298:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_486
# %bb.299:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_486
.LBB1_96:
	xor	ecx, ecx
.LBB1_713:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_715
.LBB1_714:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_714
.LBB1_715:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_716:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_716
	jmp	.LBB1_1069
.LBB1_97:
	cmp	edi, 2
	je	.LBB1_229
# %bb.98:
	cmp	edi, 3
	jne	.LBB1_1069
# %bb.99:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.100:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_101
# %bb.301:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_489
# %bb.302:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_489
.LBB1_101:
	xor	edi, edi
.LBB1_721:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_723
.LBB1_722:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_722
.LBB1_723:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_724:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_724
	jmp	.LBB1_1069
.LBB1_102:
	cmp	edi, 2
	je	.LBB1_232
# %bb.103:
	cmp	edi, 3
	jne	.LBB1_1069
# %bb.104:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.105:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_106
# %bb.304:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_492
# %bb.305:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_492
.LBB1_106:
	xor	edi, edi
.LBB1_729:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_731
.LBB1_730:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_730
.LBB1_731:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_732:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_732
	jmp	.LBB1_1069
.LBB1_107:
	cmp	edi, 2
	je	.LBB1_235
# %bb.108:
	cmp	edi, 3
	jne	.LBB1_1069
# %bb.109:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.110:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_111
# %bb.307:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_495
# %bb.308:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_495
.LBB1_111:
	xor	esi, esi
.LBB1_737:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_739
.LBB1_738:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_738
.LBB1_739:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_740:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_740
	jmp	.LBB1_1069
.LBB1_112:
	cmp	edi, 2
	je	.LBB1_238
# %bb.113:
	cmp	edi, 3
	jne	.LBB1_1069
# %bb.114:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.115:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_116
# %bb.310:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_498
# %bb.311:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_498
.LBB1_116:
	xor	esi, esi
.LBB1_745:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_747
.LBB1_746:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_746
.LBB1_747:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_748:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_748
	jmp	.LBB1_1069
.LBB1_117:
	cmp	edi, 2
	je	.LBB1_241
# %bb.118:
	cmp	edi, 3
	jne	.LBB1_1069
# %bb.119:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.120:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_121
# %bb.313:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_501
# %bb.314:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_501
.LBB1_121:
	xor	esi, esi
.LBB1_753:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_755
.LBB1_754:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_754
.LBB1_755:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_756:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_756
	jmp	.LBB1_1069
.LBB1_122:
	cmp	edi, 2
	je	.LBB1_244
# %bb.123:
	cmp	edi, 3
	jne	.LBB1_1069
# %bb.124:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.125:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_126
# %bb.316:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_504
# %bb.317:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_504
.LBB1_126:
	xor	esi, esi
.LBB1_761:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_763
.LBB1_762:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_762
.LBB1_763:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_764
	jmp	.LBB1_1069
.LBB1_127:
	cmp	edi, 7
	je	.LBB1_247
# %bb.128:
	cmp	edi, 8
	jne	.LBB1_1069
# %bb.129:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.130:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_319
# %bb.131:
	xor	edi, edi
	jmp	.LBB1_321
.LBB1_132:
	cmp	edi, 7
	je	.LBB1_250
# %bb.133:
	cmp	edi, 8
	jne	.LBB1_1069
# %bb.134:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.135:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_324
# %bb.136:
	xor	edi, edi
	jmp	.LBB1_326
.LBB1_137:
	cmp	edi, 7
	je	.LBB1_253
# %bb.138:
	cmp	edi, 8
	jne	.LBB1_1069
# %bb.139:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.140:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_141
# %bb.329:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_507
# %bb.330:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_507
.LBB1_141:
	xor	esi, esi
.LBB1_769:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_771
.LBB1_770:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_770
.LBB1_771:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_772:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_772
	jmp	.LBB1_1069
.LBB1_142:
	cmp	edi, 7
	je	.LBB1_256
# %bb.143:
	cmp	edi, 8
	jne	.LBB1_1069
# %bb.144:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.145:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_146
# %bb.332:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_510
# %bb.333:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_510
.LBB1_146:
	xor	esi, esi
.LBB1_777:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_779
.LBB1_778:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_778
.LBB1_779:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_780:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_780
	jmp	.LBB1_1069
.LBB1_147:
	cmp	edi, 7
	je	.LBB1_259
# %bb.148:
	cmp	edi, 8
	jne	.LBB1_1069
# %bb.149:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.150:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_151
# %bb.335:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_513
# %bb.336:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_513
.LBB1_151:
	xor	esi, esi
.LBB1_785:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_787
.LBB1_786:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_786
.LBB1_787:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_788:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_788
	jmp	.LBB1_1069
.LBB1_152:
	cmp	edi, 7
	je	.LBB1_262
# %bb.153:
	cmp	edi, 8
	jne	.LBB1_1069
# %bb.154:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.155:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_156
# %bb.338:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_516
# %bb.339:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_516
.LBB1_156:
	xor	esi, esi
.LBB1_793:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_795
.LBB1_794:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_794
.LBB1_795:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_796:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_796
	jmp	.LBB1_1069
.LBB1_157:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.158:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_159
# %bb.341:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_519
# %bb.342:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_519
.LBB1_159:
	xor	esi, esi
.LBB1_801:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_803
.LBB1_802:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_802
.LBB1_803:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_804
	jmp	.LBB1_1069
.LBB1_160:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.161:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_162
# %bb.344:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_522
# %bb.345:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_522
.LBB1_162:
	xor	esi, esi
.LBB1_809:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_811
.LBB1_810:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_810
.LBB1_811:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_812:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_812
	jmp	.LBB1_1069
.LBB1_163:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.164:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_165
# %bb.347:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_525
# %bb.348:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_525
.LBB1_165:
	xor	esi, esi
.LBB1_817:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_819
.LBB1_818:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_818
.LBB1_819:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_820:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_820
	jmp	.LBB1_1069
.LBB1_166:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.167:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_168
# %bb.350:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_528
# %bb.351:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_528
.LBB1_168:
	xor	esi, esi
.LBB1_825:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_827
.LBB1_826:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	imul	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_826
.LBB1_827:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_828:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_828
	jmp	.LBB1_1069
.LBB1_169:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.170:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_171
# %bb.353:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_531
# %bb.354:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_531
.LBB1_171:
	xor	esi, esi
.LBB1_833:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_835
.LBB1_834:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_834
.LBB1_835:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_836:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_836
	jmp	.LBB1_1069
.LBB1_172:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.173:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_174
# %bb.356:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_534
# %bb.357:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_534
.LBB1_174:
	xor	esi, esi
.LBB1_841:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_843
.LBB1_842:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_842
.LBB1_843:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_844:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_844
	jmp	.LBB1_1069
.LBB1_175:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.176:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_177
# %bb.359:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_537
# %bb.360:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_537
.LBB1_177:
	xor	esi, esi
.LBB1_849:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_851
.LBB1_850:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_850
.LBB1_851:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_852:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_852
	jmp	.LBB1_1069
.LBB1_178:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.179:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_180
# %bb.362:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_540
# %bb.363:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_540
.LBB1_180:
	xor	esi, esi
.LBB1_857:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_859
.LBB1_858:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_858
.LBB1_859:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_860:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_860
	jmp	.LBB1_1069
.LBB1_181:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.182:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_183
# %bb.365:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_543
# %bb.366:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_543
.LBB1_183:
	xor	esi, esi
.LBB1_865:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_867
.LBB1_866:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_866
.LBB1_867:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_868:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_868
	jmp	.LBB1_1069
.LBB1_184:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.185:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_186
# %bb.368:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_546
# %bb.369:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_546
.LBB1_186:
	xor	esi, esi
.LBB1_873:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_875
.LBB1_874:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_874
.LBB1_875:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_876:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_876
	jmp	.LBB1_1069
.LBB1_187:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.188:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_189
# %bb.371:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_549
# %bb.372:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_549
.LBB1_189:
	xor	esi, esi
.LBB1_881:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_883
.LBB1_882:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_882
.LBB1_883:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_884:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_884
	jmp	.LBB1_1069
.LBB1_190:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.191:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_192
# %bb.374:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_552
# %bb.375:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_552
.LBB1_192:
	xor	esi, esi
.LBB1_889:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_891
.LBB1_890:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_890
.LBB1_891:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_892:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_892
	jmp	.LBB1_1069
.LBB1_193:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.194:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_377
# %bb.195:
	xor	edi, edi
	jmp	.LBB1_379
.LBB1_196:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.197:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_198
# %bb.382:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_555
# %bb.383:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_555
.LBB1_198:
	xor	ecx, ecx
.LBB1_897:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_899
.LBB1_898:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_898
.LBB1_899:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_900:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_900
	jmp	.LBB1_1069
.LBB1_199:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.200:
	mov	rax, qword ptr [rcx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB1_385
# %bb.201:
	xor	edi, edi
	jmp	.LBB1_387
.LBB1_202:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.203:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_204
# %bb.390:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_558
# %bb.391:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_558
.LBB1_204:
	xor	ecx, ecx
.LBB1_905:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_907
.LBB1_906:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_906
.LBB1_907:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_908:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_908
	jmp	.LBB1_1069
.LBB1_205:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.206:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_207
# %bb.393:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_561
# %bb.394:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_561
.LBB1_207:
	xor	esi, esi
.LBB1_913:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_915
.LBB1_914:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_914
.LBB1_915:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_916:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_916
	jmp	.LBB1_1069
.LBB1_208:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.209:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_210
# %bb.396:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_564
# %bb.397:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_564
.LBB1_210:
	xor	ecx, ecx
.LBB1_921:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_923
.LBB1_922:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_922
.LBB1_923:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_924:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_924
	jmp	.LBB1_1069
.LBB1_211:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.212:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_213
# %bb.399:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_567
# %bb.400:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_567
.LBB1_213:
	xor	esi, esi
.LBB1_929:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_931
.LBB1_930:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_930
.LBB1_931:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_932:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_932
	jmp	.LBB1_1069
.LBB1_214:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.215:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_216
# %bb.402:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_570
# %bb.403:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_570
.LBB1_216:
	xor	ecx, ecx
.LBB1_937:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_939
.LBB1_938:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_938
.LBB1_939:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_940:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_940
	jmp	.LBB1_1069
.LBB1_217:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.218:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_219
# %bb.405:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_573
# %bb.406:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_573
.LBB1_219:
	xor	esi, esi
.LBB1_945:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_947
.LBB1_946:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_946
.LBB1_947:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_948:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_948
	jmp	.LBB1_1069
.LBB1_220:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.221:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_222
# %bb.408:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_576
# %bb.409:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_576
.LBB1_222:
	xor	ecx, ecx
.LBB1_953:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_955
.LBB1_954:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_954
.LBB1_955:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_956:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_956
	jmp	.LBB1_1069
.LBB1_223:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.224:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_225
# %bb.411:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_579
# %bb.412:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_579
.LBB1_225:
	xor	esi, esi
.LBB1_961:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_963
.LBB1_962:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_962
.LBB1_963:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_964:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_964
	jmp	.LBB1_1069
.LBB1_226:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.227:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_228
# %bb.414:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_582
# %bb.415:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_582
.LBB1_228:
	xor	ecx, ecx
.LBB1_969:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_971
.LBB1_970:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_970
.LBB1_971:
	cmp	rsi, 3
	jb	.LBB1_1069
.LBB1_972:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_972
	jmp	.LBB1_1069
.LBB1_229:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.230:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_231
# %bb.417:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_585
# %bb.418:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_585
.LBB1_231:
	xor	edi, edi
.LBB1_977:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_979
.LBB1_978:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_978
.LBB1_979:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_980
	jmp	.LBB1_1069
.LBB1_232:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.233:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_234
# %bb.420:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_588
# %bb.421:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_588
.LBB1_234:
	xor	edi, edi
.LBB1_985:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_987
.LBB1_986:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_986
.LBB1_987:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_988:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_988
	jmp	.LBB1_1069
.LBB1_235:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.236:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_237
# %bb.423:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_591
# %bb.424:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_591
.LBB1_237:
	xor	esi, esi
.LBB1_993:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_995
.LBB1_994:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_994
.LBB1_995:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_996:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_996
	jmp	.LBB1_1069
.LBB1_238:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.239:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_240
# %bb.426:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_594
# %bb.427:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_594
.LBB1_240:
	xor	esi, esi
.LBB1_1001:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1003
.LBB1_1002:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1002
.LBB1_1003:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1004:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1004
	jmp	.LBB1_1069
.LBB1_241:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.242:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_243
# %bb.429:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_597
# %bb.430:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_597
.LBB1_243:
	xor	esi, esi
.LBB1_1009:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1011
.LBB1_1010:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1010
.LBB1_1011:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1012
	jmp	.LBB1_1069
.LBB1_244:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.245:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_246
# %bb.432:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_600
# %bb.433:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_600
.LBB1_246:
	xor	esi, esi
.LBB1_1017:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1019
.LBB1_1018:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1018
.LBB1_1019:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1020:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1020
	jmp	.LBB1_1069
.LBB1_247:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.248:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_249
# %bb.435:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_603
# %bb.436:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_603
.LBB1_249:
	xor	esi, esi
.LBB1_1025:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1027
.LBB1_1026:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1026
.LBB1_1027:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1028:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1028
	jmp	.LBB1_1069
.LBB1_250:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.251:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_252
# %bb.438:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_606
# %bb.439:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_606
.LBB1_252:
	xor	esi, esi
.LBB1_1033:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1035
.LBB1_1034:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1034
.LBB1_1035:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1036:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1036
	jmp	.LBB1_1069
.LBB1_253:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.254:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_255
# %bb.441:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_609
# %bb.442:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_609
.LBB1_255:
	xor	esi, esi
.LBB1_1041:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1043
.LBB1_1042:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1042
.LBB1_1043:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1044:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1044
	jmp	.LBB1_1069
.LBB1_256:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.257:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_258
# %bb.444:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_612
# %bb.445:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_612
.LBB1_258:
	xor	esi, esi
.LBB1_1049:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1051
.LBB1_1050:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1050
.LBB1_1051:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1052:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1052
	jmp	.LBB1_1069
.LBB1_259:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.260:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_261
# %bb.447:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_615
# %bb.448:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_615
.LBB1_261:
	xor	esi, esi
.LBB1_1057:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1059
.LBB1_1058:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1058
.LBB1_1059:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1060:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1060
	jmp	.LBB1_1069
.LBB1_262:
	test	r9d, r9d
	jle	.LBB1_1069
# %bb.263:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_264
# %bb.450:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_618
# %bb.451:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_618
.LBB1_264:
	xor	esi, esi
.LBB1_1065:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1067
.LBB1_1066:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1066
.LBB1_1067:
	cmp	r9, 3
	jb	.LBB1_1069
.LBB1_1068:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1068
	jmp	.LBB1_1069
.LBB1_319:
	and	esi, -4
	xor	edi, edi
.LBB1_320:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_320
.LBB1_321:
	test	r9, r9
	je	.LBB1_1069
# %bb.322:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_323:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_323
	jmp	.LBB1_1069
.LBB1_324:
	and	esi, -4
	xor	edi, edi
.LBB1_325:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_325
.LBB1_326:
	test	r9, r9
	je	.LBB1_1069
# %bb.327:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_328:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_328
	jmp	.LBB1_1069
.LBB1_377:
	and	esi, -4
	xor	edi, edi
.LBB1_378:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_378
.LBB1_379:
	test	r9, r9
	je	.LBB1_1069
# %bb.380:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_381:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_381
	jmp	.LBB1_1069
.LBB1_385:
	and	esi, -4
	xor	edi, edi
.LBB1_386:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_386
.LBB1_387:
	test	r9, r9
	je	.LBB1_1069
# %bb.388:
	lea	rsi, [r8 + 8*rdi]
	lea	rdx, [rdx + 8*rdi]
	xor	edi, edi
.LBB1_389:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rdi]
	imul	rcx, rax
	mov	qword ptr [rsi + 8*rdi], rcx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB1_389
.LBB1_1069:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB1_453:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_621
# %bb.454:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_455:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_455
	jmp	.LBB1_622
.LBB1_456:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_629
# %bb.457:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_458:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_458
	jmp	.LBB1_630
.LBB1_459:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_637
# %bb.460:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_461:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_461
	jmp	.LBB1_638
.LBB1_462:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_645
# %bb.463:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_464:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_464
	jmp	.LBB1_646
.LBB1_465:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_653
# %bb.466:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_467:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_467
	jmp	.LBB1_654
.LBB1_468:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_661
# %bb.469:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_470:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_470
	jmp	.LBB1_662
.LBB1_471:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_669
# %bb.472:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_473:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_473
	jmp	.LBB1_670
.LBB1_474:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_677
# %bb.475:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_476:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_476
	jmp	.LBB1_678
.LBB1_477:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_685
# %bb.478:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_479:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_479
	jmp	.LBB1_686
.LBB1_480:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_693
# %bb.481:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_482:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_482
	jmp	.LBB1_694
.LBB1_483:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_701
# %bb.484:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_485:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_485
	jmp	.LBB1_702
.LBB1_486:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_709
# %bb.487:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_488:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_488
	jmp	.LBB1_710
.LBB1_489:
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
	je	.LBB1_717
# %bb.490:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_491:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_491
	jmp	.LBB1_718
.LBB1_492:
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
	je	.LBB1_725
# %bb.493:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_494:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_494
	jmp	.LBB1_726
.LBB1_495:
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
	je	.LBB1_733
# %bb.496:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_497:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_497
	jmp	.LBB1_734
.LBB1_498:
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
	je	.LBB1_741
# %bb.499:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_500:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_500
	jmp	.LBB1_742
.LBB1_501:
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
	je	.LBB1_749
# %bb.502:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_503:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_503
	jmp	.LBB1_750
.LBB1_504:
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
	je	.LBB1_757
# %bb.505:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_506:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_506
	jmp	.LBB1_758
.LBB1_507:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_765
# %bb.508:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_509:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_509
	jmp	.LBB1_766
.LBB1_510:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_773
# %bb.511:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_512:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_512
	jmp	.LBB1_774
.LBB1_513:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_781
# %bb.514:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_515:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_515
	jmp	.LBB1_782
.LBB1_516:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_789
# %bb.517:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_518:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_518
	jmp	.LBB1_790
.LBB1_519:
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
	je	.LBB1_797
# %bb.520:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_521:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_521
	jmp	.LBB1_798
.LBB1_522:
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
	je	.LBB1_805
# %bb.523:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_524:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_524
	jmp	.LBB1_806
.LBB1_525:
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
	je	.LBB1_813
# %bb.526:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_527:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_527
	jmp	.LBB1_814
.LBB1_528:
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
	je	.LBB1_821
# %bb.529:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_530:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_530
	jmp	.LBB1_822
.LBB1_531:
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
	je	.LBB1_829
# %bb.532:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_533:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_533
	jmp	.LBB1_830
.LBB1_534:
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
	je	.LBB1_837
# %bb.535:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_536:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_536
	jmp	.LBB1_838
.LBB1_537:
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
	je	.LBB1_845
# %bb.538:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_539:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_539
	jmp	.LBB1_846
.LBB1_540:
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
	je	.LBB1_853
# %bb.541:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_542:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_542
	jmp	.LBB1_854
.LBB1_543:
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
	je	.LBB1_861
# %bb.544:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_545:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_545
	jmp	.LBB1_862
.LBB1_546:
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
	je	.LBB1_869
# %bb.547:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_548:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_548
	jmp	.LBB1_870
.LBB1_549:
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
	je	.LBB1_877
# %bb.550:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_551:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_551
	jmp	.LBB1_878
.LBB1_552:
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
	je	.LBB1_885
# %bb.553:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_554:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_554
	jmp	.LBB1_886
.LBB1_555:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_893
# %bb.556:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_557:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_557
	jmp	.LBB1_894
.LBB1_558:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_901
# %bb.559:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_560:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_560
	jmp	.LBB1_902
.LBB1_561:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_909
# %bb.562:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_563:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_563
	jmp	.LBB1_910
.LBB1_564:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_917
# %bb.565:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_566:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_566
	jmp	.LBB1_918
.LBB1_567:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_925
# %bb.568:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_569:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_569
	jmp	.LBB1_926
.LBB1_570:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_933
# %bb.571:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_572:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_572
	jmp	.LBB1_934
.LBB1_573:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_941
# %bb.574:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_575:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_575
	jmp	.LBB1_942
.LBB1_576:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_949
# %bb.577:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_578:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_578
	jmp	.LBB1_950
.LBB1_579:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_957
# %bb.580:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_581:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_581
	jmp	.LBB1_958
.LBB1_582:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_965
# %bb.583:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_584:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_584
	jmp	.LBB1_966
.LBB1_585:
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
	je	.LBB1_973
# %bb.586:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_587:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_587
	jmp	.LBB1_974
.LBB1_588:
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
	je	.LBB1_981
# %bb.589:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI1_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB1_590:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_590
	jmp	.LBB1_982
.LBB1_591:
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
	je	.LBB1_989
# %bb.592:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_593
	jmp	.LBB1_990
.LBB1_594:
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
	je	.LBB1_997
# %bb.595:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_596:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_596
	jmp	.LBB1_998
.LBB1_597:
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
	je	.LBB1_1005
# %bb.598:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_599:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_599
	jmp	.LBB1_1006
.LBB1_600:
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
	je	.LBB1_1013
# %bb.601:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_602:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_602
	jmp	.LBB1_1014
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
	je	.LBB1_1021
# %bb.604:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_605
	jmp	.LBB1_1022
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
	je	.LBB1_1029
# %bb.607:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_608
	jmp	.LBB1_1030
.LBB1_609:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1037
# %bb.610:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_611:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_611
	jmp	.LBB1_1038
.LBB1_612:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1045
# %bb.613:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_614:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_614
	jmp	.LBB1_1046
.LBB1_615:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1053
# %bb.616:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_617:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_617
	jmp	.LBB1_1054
.LBB1_618:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1061
# %bb.619:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_620:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_620
	jmp	.LBB1_1062
.LBB1_621:
	xor	edi, edi
.LBB1_622:
	test	r9b, 1
	je	.LBB1_624
# %bb.623:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_624:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_625
.LBB1_629:
	xor	edi, edi
.LBB1_630:
	test	r9b, 1
	je	.LBB1_632
# %bb.631:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_632:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_633
.LBB1_637:
	xor	edi, edi
.LBB1_638:
	test	r9b, 1
	je	.LBB1_640
# %bb.639:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_640:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_641
.LBB1_645:
	xor	edi, edi
.LBB1_646:
	test	r9b, 1
	je	.LBB1_648
# %bb.647:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_648:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_649
.LBB1_653:
	xor	edi, edi
.LBB1_654:
	test	r9b, 1
	je	.LBB1_656
# %bb.655:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_656:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_657
.LBB1_661:
	xor	edi, edi
.LBB1_662:
	test	r9b, 1
	je	.LBB1_664
# %bb.663:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_664:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_665
.LBB1_669:
	xor	edi, edi
.LBB1_670:
	test	r9b, 1
	je	.LBB1_672
# %bb.671:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_672:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_673
.LBB1_677:
	xor	edi, edi
.LBB1_678:
	test	r9b, 1
	je	.LBB1_680
# %bb.679:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_680:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_681
.LBB1_685:
	xor	edi, edi
.LBB1_686:
	test	r9b, 1
	je	.LBB1_688
# %bb.687:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_688:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_689
.LBB1_693:
	xor	edi, edi
.LBB1_694:
	test	r9b, 1
	je	.LBB1_696
# %bb.695:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_696:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_697
.LBB1_701:
	xor	edi, edi
.LBB1_702:
	test	r9b, 1
	je	.LBB1_704
# %bb.703:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_704:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_705
.LBB1_709:
	xor	edi, edi
.LBB1_710:
	test	r9b, 1
	je	.LBB1_712
# %bb.711:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_712:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_713
.LBB1_717:
	xor	eax, eax
.LBB1_718:
	test	r9b, 1
	je	.LBB1_720
# %bb.719:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_720:
	cmp	rdi, r10
	je	.LBB1_1069
	jmp	.LBB1_721
.LBB1_725:
	xor	eax, eax
.LBB1_726:
	test	r9b, 1
	je	.LBB1_728
# %bb.727:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_728:
	cmp	rdi, r10
	je	.LBB1_1069
	jmp	.LBB1_729
.LBB1_733:
	xor	edi, edi
.LBB1_734:
	test	r9b, 1
	je	.LBB1_736
# %bb.735:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_736:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_737
.LBB1_741:
	xor	edi, edi
.LBB1_742:
	test	r9b, 1
	je	.LBB1_744
# %bb.743:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_744:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_745
.LBB1_749:
	xor	edi, edi
.LBB1_750:
	test	r9b, 1
	je	.LBB1_752
# %bb.751:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_752:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_753
.LBB1_757:
	xor	edi, edi
.LBB1_758:
	test	r9b, 1
	je	.LBB1_760
# %bb.759:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_760:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_761
.LBB1_765:
	xor	edi, edi
.LBB1_766:
	test	r9b, 1
	je	.LBB1_768
# %bb.767:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_768:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_769
.LBB1_773:
	xor	edi, edi
.LBB1_774:
	test	r9b, 1
	je	.LBB1_776
# %bb.775:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_776:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_777
.LBB1_781:
	xor	edi, edi
.LBB1_782:
	test	r9b, 1
	je	.LBB1_784
# %bb.783:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_784:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_785
.LBB1_789:
	xor	edi, edi
.LBB1_790:
	test	r9b, 1
	je	.LBB1_792
# %bb.791:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_792:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_793
.LBB1_797:
	xor	edi, edi
.LBB1_798:
	test	r9b, 1
	je	.LBB1_800
# %bb.799:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_800:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_801
.LBB1_805:
	xor	edi, edi
.LBB1_806:
	test	r9b, 1
	je	.LBB1_808
# %bb.807:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_808:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_809
.LBB1_813:
	xor	edi, edi
.LBB1_814:
	test	r9b, 1
	je	.LBB1_816
# %bb.815:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_816:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_817
.LBB1_821:
	xor	edi, edi
.LBB1_822:
	test	r9b, 1
	je	.LBB1_824
# %bb.823:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_824:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_825
.LBB1_829:
	xor	edi, edi
.LBB1_830:
	test	r9b, 1
	je	.LBB1_832
# %bb.831:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_832:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_833
.LBB1_837:
	xor	edi, edi
.LBB1_838:
	test	r9b, 1
	je	.LBB1_840
# %bb.839:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_840:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_841
.LBB1_845:
	xor	edi, edi
.LBB1_846:
	test	r9b, 1
	je	.LBB1_848
# %bb.847:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_848:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_849
.LBB1_853:
	xor	edi, edi
.LBB1_854:
	test	r9b, 1
	je	.LBB1_856
# %bb.855:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_856:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_857
.LBB1_861:
	xor	edi, edi
.LBB1_862:
	test	r9b, 1
	je	.LBB1_864
# %bb.863:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_864:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_865
.LBB1_869:
	xor	edi, edi
.LBB1_870:
	test	r9b, 1
	je	.LBB1_872
# %bb.871:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_872:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_873
.LBB1_877:
	xor	edi, edi
.LBB1_878:
	test	r9b, 1
	je	.LBB1_880
# %bb.879:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_880:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_881
.LBB1_885:
	xor	edi, edi
.LBB1_886:
	test	r9b, 1
	je	.LBB1_888
# %bb.887:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_888:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_889
.LBB1_893:
	xor	edi, edi
.LBB1_894:
	test	r9b, 1
	je	.LBB1_896
# %bb.895:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_896:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_897
.LBB1_901:
	xor	edi, edi
.LBB1_902:
	test	r9b, 1
	je	.LBB1_904
# %bb.903:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_904:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_905
.LBB1_909:
	xor	edi, edi
.LBB1_910:
	test	r9b, 1
	je	.LBB1_912
# %bb.911:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_912:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_913
.LBB1_917:
	xor	edi, edi
.LBB1_918:
	test	r9b, 1
	je	.LBB1_920
# %bb.919:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_920:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_921
.LBB1_925:
	xor	edi, edi
.LBB1_926:
	test	r9b, 1
	je	.LBB1_928
# %bb.927:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_928:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_929
.LBB1_933:
	xor	edi, edi
.LBB1_934:
	test	r9b, 1
	je	.LBB1_936
# %bb.935:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_936:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_937
.LBB1_941:
	xor	edi, edi
.LBB1_942:
	test	r9b, 1
	je	.LBB1_944
# %bb.943:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_944:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_945
.LBB1_949:
	xor	edi, edi
.LBB1_950:
	test	r9b, 1
	je	.LBB1_952
# %bb.951:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_952:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_953
.LBB1_957:
	xor	edi, edi
.LBB1_958:
	test	r9b, 1
	je	.LBB1_960
# %bb.959:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_960:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_961
.LBB1_965:
	xor	edi, edi
.LBB1_966:
	test	r9b, 1
	je	.LBB1_968
# %bb.967:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_968:
	cmp	rcx, rax
	je	.LBB1_1069
	jmp	.LBB1_969
.LBB1_973:
	xor	eax, eax
.LBB1_974:
	test	r9b, 1
	je	.LBB1_976
# %bb.975:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_976:
	cmp	rdi, r10
	je	.LBB1_1069
	jmp	.LBB1_977
.LBB1_981:
	xor	eax, eax
.LBB1_982:
	test	r9b, 1
	je	.LBB1_984
# %bb.983:
	movdqu	xmm2, xmmword ptr [rdx + rax]
	movdqu	xmm3, xmmword ptr [rdx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI1_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB1_984:
	cmp	rdi, r10
	je	.LBB1_1069
	jmp	.LBB1_985
.LBB1_989:
	xor	edi, edi
.LBB1_990:
	test	r9b, 1
	je	.LBB1_992
# %bb.991:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_992:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_993
.LBB1_997:
	xor	edi, edi
.LBB1_998:
	test	r9b, 1
	je	.LBB1_1000
# %bb.999:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1000:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1001
.LBB1_1005:
	xor	edi, edi
.LBB1_1006:
	test	r9b, 1
	je	.LBB1_1008
# %bb.1007:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1008:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1009
.LBB1_1013:
	xor	edi, edi
.LBB1_1014:
	test	r9b, 1
	je	.LBB1_1016
# %bb.1015:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_1016:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1017
.LBB1_1021:
	xor	edi, edi
.LBB1_1022:
	test	r9b, 1
	je	.LBB1_1024
# %bb.1023:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1024:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1025
.LBB1_1029:
	xor	edi, edi
.LBB1_1030:
	test	r9b, 1
	je	.LBB1_1032
# %bb.1031:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1032:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1033
.LBB1_1037:
	xor	edi, edi
.LBB1_1038:
	test	r9b, 1
	je	.LBB1_1040
# %bb.1039:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1040:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1041
.LBB1_1045:
	xor	edi, edi
.LBB1_1046:
	test	r9b, 1
	je	.LBB1_1048
# %bb.1047:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1048:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1049
.LBB1_1053:
	xor	edi, edi
.LBB1_1054:
	test	r9b, 1
	je	.LBB1_1056
# %bb.1055:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1056:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1057
.LBB1_1061:
	xor	edi, edi
.LBB1_1062:
	test	r9b, 1
	je	.LBB1_1064
# %bb.1063:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_1064:
	cmp	rsi, r10
	je	.LBB1_1069
	jmp	.LBB1_1065
.Lfunc_end1:
	.size	arithmetic_arr_scalar_sse4, .Lfunc_end1-arithmetic_arr_scalar_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_scalar_arr_sse4
.LCPI2_0:
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
	cmp	sil, 20
	jg	.LBB2_12
# %bb.1:
	test	sil, sil
	je	.LBB2_23
# %bb.2:
	cmp	sil, 1
	je	.LBB2_31
# %bb.3:
	cmp	sil, 2
	jne	.LBB2_1069
# %bb.4:
	cmp	edi, 6
	jg	.LBB2_55
# %bb.5:
	cmp	edi, 3
	jle	.LBB2_97
# %bb.6:
	cmp	edi, 4
	je	.LBB2_157
# %bb.7:
	cmp	edi, 5
	je	.LBB2_160
# %bb.8:
	cmp	edi, 6
	jne	.LBB2_1069
# %bb.9:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.10:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_11
# %bb.265:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_453
# %bb.266:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_453
.LBB2_11:
	xor	esi, esi
.LBB2_625:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_627
.LBB2_626:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_626
.LBB2_627:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_628:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_628
	jmp	.LBB2_1069
.LBB2_12:
	cmp	sil, 21
	je	.LBB2_39
# %bb.13:
	cmp	sil, 22
	je	.LBB2_47
# %bb.14:
	cmp	sil, 23
	jne	.LBB2_1069
# %bb.15:
	cmp	edi, 6
	jg	.LBB2_62
# %bb.16:
	cmp	edi, 3
	jle	.LBB2_102
# %bb.17:
	cmp	edi, 4
	je	.LBB2_163
# %bb.18:
	cmp	edi, 5
	je	.LBB2_166
# %bb.19:
	cmp	edi, 6
	jne	.LBB2_1069
# %bb.20:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.21:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_22
# %bb.268:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_456
# %bb.269:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_456
.LBB2_22:
	xor	esi, esi
.LBB2_633:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_635
.LBB2_634:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_634
.LBB2_635:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_636:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_636
	jmp	.LBB2_1069
.LBB2_23:
	cmp	edi, 6
	jg	.LBB2_69
# %bb.24:
	cmp	edi, 3
	jle	.LBB2_107
# %bb.25:
	cmp	edi, 4
	je	.LBB2_169
# %bb.26:
	cmp	edi, 5
	je	.LBB2_172
# %bb.27:
	cmp	edi, 6
	jne	.LBB2_1069
# %bb.28:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.29:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_30
# %bb.271:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_459
# %bb.272:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_459
.LBB2_30:
	xor	esi, esi
.LBB2_641:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_643
.LBB2_642:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_642
.LBB2_643:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_644:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_644
	jmp	.LBB2_1069
.LBB2_31:
	cmp	edi, 6
	jg	.LBB2_76
# %bb.32:
	cmp	edi, 3
	jle	.LBB2_112
# %bb.33:
	cmp	edi, 4
	je	.LBB2_175
# %bb.34:
	cmp	edi, 5
	je	.LBB2_178
# %bb.35:
	cmp	edi, 6
	jne	.LBB2_1069
# %bb.36:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.37:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_38
# %bb.274:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_462
# %bb.275:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_462
.LBB2_38:
	xor	esi, esi
.LBB2_649:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_651
.LBB2_650:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_650
.LBB2_651:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_652:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_652
	jmp	.LBB2_1069
.LBB2_39:
	cmp	edi, 6
	jg	.LBB2_83
# %bb.40:
	cmp	edi, 3
	jle	.LBB2_117
# %bb.41:
	cmp	edi, 4
	je	.LBB2_181
# %bb.42:
	cmp	edi, 5
	je	.LBB2_184
# %bb.43:
	cmp	edi, 6
	jne	.LBB2_1069
# %bb.44:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.45:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_46
# %bb.277:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_465
# %bb.278:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_465
.LBB2_46:
	xor	esi, esi
.LBB2_657:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_659
.LBB2_658:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_658
.LBB2_659:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_660
	jmp	.LBB2_1069
.LBB2_47:
	cmp	edi, 6
	jg	.LBB2_90
# %bb.48:
	cmp	edi, 3
	jle	.LBB2_122
# %bb.49:
	cmp	edi, 4
	je	.LBB2_187
# %bb.50:
	cmp	edi, 5
	je	.LBB2_190
# %bb.51:
	cmp	edi, 6
	jne	.LBB2_1069
# %bb.52:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.53:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_54
# %bb.280:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_468
# %bb.281:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_468
.LBB2_54:
	xor	esi, esi
.LBB2_665:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_667
.LBB2_666:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_666
.LBB2_667:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_668:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_668
	jmp	.LBB2_1069
.LBB2_55:
	cmp	edi, 8
	jle	.LBB2_127
# %bb.56:
	cmp	edi, 9
	je	.LBB2_193
# %bb.57:
	cmp	edi, 11
	je	.LBB2_196
# %bb.58:
	cmp	edi, 12
	jne	.LBB2_1069
# %bb.59:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.60:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_61
# %bb.283:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_471
# %bb.284:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_471
.LBB2_61:
	xor	edx, edx
.LBB2_673:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_675
.LBB2_674:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_674
.LBB2_675:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_676
	jmp	.LBB2_1069
.LBB2_62:
	cmp	edi, 8
	jle	.LBB2_132
# %bb.63:
	cmp	edi, 9
	je	.LBB2_199
# %bb.64:
	cmp	edi, 11
	je	.LBB2_202
# %bb.65:
	cmp	edi, 12
	jne	.LBB2_1069
# %bb.66:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.67:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_68
# %bb.286:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_474
# %bb.287:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_474
.LBB2_68:
	xor	edx, edx
.LBB2_681:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_683
.LBB2_682:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	mulsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_682
.LBB2_683:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_684:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_684
	jmp	.LBB2_1069
.LBB2_69:
	cmp	edi, 8
	jle	.LBB2_137
# %bb.70:
	cmp	edi, 9
	je	.LBB2_205
# %bb.71:
	cmp	edi, 11
	je	.LBB2_208
# %bb.72:
	cmp	edi, 12
	jne	.LBB2_1069
# %bb.73:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.74:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_75
# %bb.289:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_477
# %bb.290:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_477
.LBB2_75:
	xor	edx, edx
.LBB2_689:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_691
.LBB2_690:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_690
.LBB2_691:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_692:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_692
	jmp	.LBB2_1069
.LBB2_76:
	cmp	edi, 8
	jle	.LBB2_142
# %bb.77:
	cmp	edi, 9
	je	.LBB2_211
# %bb.78:
	cmp	edi, 11
	je	.LBB2_214
# %bb.79:
	cmp	edi, 12
	jne	.LBB2_1069
# %bb.80:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.81:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_82
# %bb.292:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_480
# %bb.293:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_480
.LBB2_82:
	xor	edx, edx
.LBB2_697:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_699
.LBB2_698:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_698
.LBB2_699:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_700:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_700
	jmp	.LBB2_1069
.LBB2_83:
	cmp	edi, 8
	jle	.LBB2_147
# %bb.84:
	cmp	edi, 9
	je	.LBB2_217
# %bb.85:
	cmp	edi, 11
	je	.LBB2_220
# %bb.86:
	cmp	edi, 12
	jne	.LBB2_1069
# %bb.87:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.88:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_89
# %bb.295:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_483
# %bb.296:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_483
.LBB2_89:
	xor	edx, edx
.LBB2_705:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_707
.LBB2_706:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_706
.LBB2_707:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_708:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_708
	jmp	.LBB2_1069
.LBB2_90:
	cmp	edi, 8
	jle	.LBB2_152
# %bb.91:
	cmp	edi, 9
	je	.LBB2_223
# %bb.92:
	cmp	edi, 11
	je	.LBB2_226
# %bb.93:
	cmp	edi, 12
	jne	.LBB2_1069
# %bb.94:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.95:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_96
# %bb.298:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_486
# %bb.299:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_486
.LBB2_96:
	xor	edx, edx
.LBB2_713:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_715
.LBB2_714:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_714
.LBB2_715:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_716:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_716
	jmp	.LBB2_1069
.LBB2_97:
	cmp	edi, 2
	je	.LBB2_229
# %bb.98:
	cmp	edi, 3
	jne	.LBB2_1069
# %bb.99:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.100:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_101
# %bb.301:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_489
# %bb.302:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_489
.LBB2_101:
	xor	edi, edi
.LBB2_721:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_723
.LBB2_722:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_722
.LBB2_723:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_724:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_724
	jmp	.LBB2_1069
.LBB2_102:
	cmp	edi, 2
	je	.LBB2_232
# %bb.103:
	cmp	edi, 3
	jne	.LBB2_1069
# %bb.104:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.105:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_106
# %bb.304:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_492
# %bb.305:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_492
.LBB2_106:
	xor	edi, edi
.LBB2_729:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_731
.LBB2_730:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_730
.LBB2_731:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_732:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_732
	jmp	.LBB2_1069
.LBB2_107:
	cmp	edi, 2
	je	.LBB2_235
# %bb.108:
	cmp	edi, 3
	jne	.LBB2_1069
# %bb.109:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.110:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_111
# %bb.307:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_495
# %bb.308:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_495
.LBB2_111:
	xor	esi, esi
.LBB2_737:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_739
.LBB2_738:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_738
.LBB2_739:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_740:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_740
	jmp	.LBB2_1069
.LBB2_112:
	cmp	edi, 2
	je	.LBB2_238
# %bb.113:
	cmp	edi, 3
	jne	.LBB2_1069
# %bb.114:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.115:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_116
# %bb.310:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_498
# %bb.311:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_498
.LBB2_116:
	xor	esi, esi
.LBB2_745:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_747
.LBB2_746:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_746
.LBB2_747:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_748:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_748
	jmp	.LBB2_1069
.LBB2_117:
	cmp	edi, 2
	je	.LBB2_241
# %bb.118:
	cmp	edi, 3
	jne	.LBB2_1069
# %bb.119:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.120:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_121
# %bb.313:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_501
# %bb.314:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_501
.LBB2_121:
	xor	esi, esi
.LBB2_753:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_755
.LBB2_754:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_754
.LBB2_755:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_756:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_756
	jmp	.LBB2_1069
.LBB2_122:
	cmp	edi, 2
	je	.LBB2_244
# %bb.123:
	cmp	edi, 3
	jne	.LBB2_1069
# %bb.124:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.125:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_126
# %bb.316:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_504
# %bb.317:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_504
.LBB2_126:
	xor	esi, esi
.LBB2_761:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_763
.LBB2_762:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_762
.LBB2_763:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_764
	jmp	.LBB2_1069
.LBB2_127:
	cmp	edi, 7
	je	.LBB2_247
# %bb.128:
	cmp	edi, 8
	jne	.LBB2_1069
# %bb.129:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.130:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_319
# %bb.131:
	xor	edi, edi
	jmp	.LBB2_321
.LBB2_132:
	cmp	edi, 7
	je	.LBB2_250
# %bb.133:
	cmp	edi, 8
	jne	.LBB2_1069
# %bb.134:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.135:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_324
# %bb.136:
	xor	edi, edi
	jmp	.LBB2_326
.LBB2_137:
	cmp	edi, 7
	je	.LBB2_253
# %bb.138:
	cmp	edi, 8
	jne	.LBB2_1069
# %bb.139:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.140:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_141
# %bb.329:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_507
# %bb.330:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_507
.LBB2_141:
	xor	esi, esi
.LBB2_769:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_771
.LBB2_770:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_770
.LBB2_771:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_772:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_772
	jmp	.LBB2_1069
.LBB2_142:
	cmp	edi, 7
	je	.LBB2_256
# %bb.143:
	cmp	edi, 8
	jne	.LBB2_1069
# %bb.144:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.145:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_146
# %bb.332:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_510
# %bb.333:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_510
.LBB2_146:
	xor	esi, esi
.LBB2_777:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_779
.LBB2_778:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_778
.LBB2_779:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_780:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_780
	jmp	.LBB2_1069
.LBB2_147:
	cmp	edi, 7
	je	.LBB2_259
# %bb.148:
	cmp	edi, 8
	jne	.LBB2_1069
# %bb.149:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.150:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_151
# %bb.335:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_513
# %bb.336:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_513
.LBB2_151:
	xor	esi, esi
.LBB2_785:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_787
.LBB2_786:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_786
.LBB2_787:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_788:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_788
	jmp	.LBB2_1069
.LBB2_152:
	cmp	edi, 7
	je	.LBB2_262
# %bb.153:
	cmp	edi, 8
	jne	.LBB2_1069
# %bb.154:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.155:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_156
# %bb.338:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_516
# %bb.339:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_516
.LBB2_156:
	xor	esi, esi
.LBB2_793:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_795
.LBB2_794:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_794
.LBB2_795:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_796:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_796
	jmp	.LBB2_1069
.LBB2_157:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.158:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_159
# %bb.341:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_519
# %bb.342:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_519
.LBB2_159:
	xor	esi, esi
.LBB2_801:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_803
.LBB2_802:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_802
.LBB2_803:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_804
	jmp	.LBB2_1069
.LBB2_160:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.161:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_162
# %bb.344:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_522
# %bb.345:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_522
.LBB2_162:
	xor	esi, esi
.LBB2_809:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_811
.LBB2_810:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_810
.LBB2_811:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_812:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_812
	jmp	.LBB2_1069
.LBB2_163:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.164:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_165
# %bb.347:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_525
# %bb.348:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_525
.LBB2_165:
	xor	esi, esi
.LBB2_817:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_819
.LBB2_818:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_818
.LBB2_819:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_820:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_820
	jmp	.LBB2_1069
.LBB2_166:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.167:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_168
# %bb.350:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_528
# %bb.351:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_528
.LBB2_168:
	xor	esi, esi
.LBB2_825:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_827
.LBB2_826:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	imul	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_826
.LBB2_827:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_828:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_828
	jmp	.LBB2_1069
.LBB2_169:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.170:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_171
# %bb.353:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_531
# %bb.354:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_531
.LBB2_171:
	xor	esi, esi
.LBB2_833:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_835
.LBB2_834:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_834
.LBB2_835:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_836:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_836
	jmp	.LBB2_1069
.LBB2_172:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.173:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_174
# %bb.356:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_534
# %bb.357:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_534
.LBB2_174:
	xor	esi, esi
.LBB2_841:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_843
.LBB2_842:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_842
.LBB2_843:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_844:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_844
	jmp	.LBB2_1069
.LBB2_175:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.176:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_177
# %bb.359:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_537
# %bb.360:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_537
.LBB2_177:
	xor	esi, esi
.LBB2_849:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_851
.LBB2_850:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_850
.LBB2_851:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_852:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_852
	jmp	.LBB2_1069
.LBB2_178:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.179:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_180
# %bb.362:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_540
# %bb.363:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_540
.LBB2_180:
	xor	esi, esi
.LBB2_857:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_859
.LBB2_858:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_858
.LBB2_859:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_860:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_860
	jmp	.LBB2_1069
.LBB2_181:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.182:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_183
# %bb.365:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_543
# %bb.366:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_543
.LBB2_183:
	xor	esi, esi
.LBB2_865:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_867
.LBB2_866:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_866
.LBB2_867:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_868:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_868
	jmp	.LBB2_1069
.LBB2_184:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.185:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_186
# %bb.368:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_546
# %bb.369:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_546
.LBB2_186:
	xor	esi, esi
.LBB2_873:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_875
.LBB2_874:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_874
.LBB2_875:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_876:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_876
	jmp	.LBB2_1069
.LBB2_187:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.188:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_189
# %bb.371:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_549
# %bb.372:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_549
.LBB2_189:
	xor	esi, esi
.LBB2_881:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_883
.LBB2_882:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_882
.LBB2_883:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_884:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_884
	jmp	.LBB2_1069
.LBB2_190:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.191:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_192
# %bb.374:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_552
# %bb.375:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_552
.LBB2_192:
	xor	esi, esi
.LBB2_889:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_891
.LBB2_890:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_890
.LBB2_891:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_892:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_892
	jmp	.LBB2_1069
.LBB2_193:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.194:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_377
# %bb.195:
	xor	edi, edi
	jmp	.LBB2_379
.LBB2_196:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.197:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_198
# %bb.382:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_555
# %bb.383:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_555
.LBB2_198:
	xor	edx, edx
.LBB2_897:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_899
.LBB2_898:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_898
.LBB2_899:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_900:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_900
	jmp	.LBB2_1069
.LBB2_199:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.200:
	mov	rax, qword ptr [rdx]
	mov	esi, r9d
	lea	rdi, [rsi - 1]
	mov	r9d, esi
	and	r9d, 3
	cmp	rdi, 3
	jae	.LBB2_385
# %bb.201:
	xor	edi, edi
	jmp	.LBB2_387
.LBB2_202:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.203:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_204
# %bb.390:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_558
# %bb.391:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_558
.LBB2_204:
	xor	edx, edx
.LBB2_905:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_907
.LBB2_906:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	mulss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_906
.LBB2_907:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_908:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_908
	jmp	.LBB2_1069
.LBB2_205:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.206:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_207
# %bb.393:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_561
# %bb.394:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_561
.LBB2_207:
	xor	esi, esi
.LBB2_913:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_915
.LBB2_914:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_914
.LBB2_915:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_916:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_916
	jmp	.LBB2_1069
.LBB2_208:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.209:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_210
# %bb.396:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_564
# %bb.397:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_564
.LBB2_210:
	xor	edx, edx
.LBB2_921:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_923
.LBB2_922:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_922
.LBB2_923:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_924:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_924
	jmp	.LBB2_1069
.LBB2_211:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.212:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_213
# %bb.399:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_567
# %bb.400:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_567
.LBB2_213:
	xor	esi, esi
.LBB2_929:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_931
.LBB2_930:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_930
.LBB2_931:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_932:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_932
	jmp	.LBB2_1069
.LBB2_214:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.215:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_216
# %bb.402:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_570
# %bb.403:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_570
.LBB2_216:
	xor	edx, edx
.LBB2_937:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_939
.LBB2_938:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_938
.LBB2_939:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_940:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_940
	jmp	.LBB2_1069
.LBB2_217:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.218:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_219
# %bb.405:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_573
# %bb.406:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_573
.LBB2_219:
	xor	esi, esi
.LBB2_945:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_947
.LBB2_946:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_946
.LBB2_947:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_948:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_948
	jmp	.LBB2_1069
.LBB2_220:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.221:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_222
# %bb.408:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_576
# %bb.409:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_576
.LBB2_222:
	xor	edx, edx
.LBB2_953:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_955
.LBB2_954:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_954
.LBB2_955:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_956:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_956
	jmp	.LBB2_1069
.LBB2_223:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.224:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_225
# %bb.411:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_579
# %bb.412:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_579
.LBB2_225:
	xor	esi, esi
.LBB2_961:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_963
.LBB2_962:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_962
.LBB2_963:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_964:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_964
	jmp	.LBB2_1069
.LBB2_226:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.227:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_228
# %bb.414:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_582
# %bb.415:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_582
.LBB2_228:
	xor	edx, edx
.LBB2_969:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_971
.LBB2_970:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_970
.LBB2_971:
	cmp	rsi, 3
	jb	.LBB2_1069
.LBB2_972:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_972
	jmp	.LBB2_1069
.LBB2_229:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.230:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_231
# %bb.417:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_585
# %bb.418:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_585
.LBB2_231:
	xor	edi, edi
.LBB2_977:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_979
.LBB2_978:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_978
.LBB2_979:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_980
	jmp	.LBB2_1069
.LBB2_232:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.233:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_234
# %bb.420:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_588
# %bb.421:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_588
.LBB2_234:
	xor	edi, edi
.LBB2_985:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_987
.LBB2_986:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_986
.LBB2_987:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_988:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_988
	jmp	.LBB2_1069
.LBB2_235:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.236:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_237
# %bb.423:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_591
# %bb.424:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_591
.LBB2_237:
	xor	esi, esi
.LBB2_993:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_995
.LBB2_994:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_994
.LBB2_995:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_996:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_996
	jmp	.LBB2_1069
.LBB2_238:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.239:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_240
# %bb.426:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_594
# %bb.427:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_594
.LBB2_240:
	xor	esi, esi
.LBB2_1001:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1003
.LBB2_1002:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1002
.LBB2_1003:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_1004:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1004
	jmp	.LBB2_1069
.LBB2_241:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.242:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_243
# %bb.429:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_597
# %bb.430:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_597
.LBB2_243:
	xor	esi, esi
.LBB2_1009:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1011
.LBB2_1010:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1010
.LBB2_1011:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1012
	jmp	.LBB2_1069
.LBB2_244:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.245:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_246
# %bb.432:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_600
# %bb.433:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_600
.LBB2_246:
	xor	esi, esi
.LBB2_1017:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1019
.LBB2_1018:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1018
.LBB2_1019:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_1020:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1020
	jmp	.LBB2_1069
.LBB2_247:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.248:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_249
# %bb.435:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_603
# %bb.436:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_603
.LBB2_249:
	xor	esi, esi
.LBB2_1025:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1027
.LBB2_1026:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1026
.LBB2_1027:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_1028:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1028
	jmp	.LBB2_1069
.LBB2_250:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.251:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_252
# %bb.438:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_606
# %bb.439:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_606
.LBB2_252:
	xor	esi, esi
.LBB2_1033:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1035
.LBB2_1034:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1034
.LBB2_1035:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_1036:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1036
	jmp	.LBB2_1069
.LBB2_253:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.254:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_255
# %bb.441:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_609
# %bb.442:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_609
.LBB2_255:
	xor	esi, esi
.LBB2_1041:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1043
.LBB2_1042:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1042
.LBB2_1043:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_1044:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1044
	jmp	.LBB2_1069
.LBB2_256:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.257:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_258
# %bb.444:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_612
# %bb.445:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_612
.LBB2_258:
	xor	esi, esi
.LBB2_1049:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1051
.LBB2_1050:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1050
.LBB2_1051:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_1052:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1052
	jmp	.LBB2_1069
.LBB2_259:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.260:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_261
# %bb.447:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_615
# %bb.448:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_615
.LBB2_261:
	xor	esi, esi
.LBB2_1057:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1059
.LBB2_1058:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1058
.LBB2_1059:
	cmp	r9, 3
	jb	.LBB2_1069
.LBB2_1060:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1060
	jmp	.LBB2_1069
.LBB2_262:
	test	r9d, r9d
	jle	.LBB2_1069
# %bb.263:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_264
# %bb.450:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_618
# %bb.451:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_618
.LBB2_264:
	xor	esi, esi
.LBB2_1065:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1067
.LBB2_1066:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1066
.LBB2_1067:
	cmp	rdx, 3
	jb	.LBB2_1069
.LBB2_1068:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1068
	jmp	.LBB2_1069
.LBB2_319:
	and	esi, -4
	xor	edi, edi
.LBB2_320:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_320
.LBB2_321:
	test	r9, r9
	je	.LBB2_1069
# %bb.322:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_323:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_323
	jmp	.LBB2_1069
.LBB2_324:
	and	esi, -4
	xor	edi, edi
.LBB2_325:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_325
.LBB2_326:
	test	r9, r9
	je	.LBB2_1069
# %bb.327:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_328:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_328
	jmp	.LBB2_1069
.LBB2_377:
	and	esi, -4
	xor	edi, edi
.LBB2_378:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_378
.LBB2_379:
	test	r9, r9
	je	.LBB2_1069
# %bb.380:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_381:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_381
	jmp	.LBB2_1069
.LBB2_385:
	and	esi, -4
	xor	edi, edi
.LBB2_386:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_386
.LBB2_387:
	test	r9, r9
	je	.LBB2_1069
# %bb.388:
	lea	rsi, [r8 + 8*rdi]
	lea	rcx, [rcx + 8*rdi]
	xor	edi, edi
.LBB2_389:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rdi]
	imul	rdx, rax
	mov	qword ptr [rsi + 8*rdi], rdx
	add	rdi, 1
	cmp	r9, rdi
	jne	.LBB2_389
.LBB2_1069:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB2_453:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_621
# %bb.454:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_455:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_455
	jmp	.LBB2_622
.LBB2_456:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_629
# %bb.457:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_458:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_458
	jmp	.LBB2_630
.LBB2_459:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_637
# %bb.460:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_461:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_461
	jmp	.LBB2_638
.LBB2_462:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_645
# %bb.463:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_464:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_464
	jmp	.LBB2_646
.LBB2_465:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_653
# %bb.466:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_467:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_467
	jmp	.LBB2_654
.LBB2_468:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_661
# %bb.469:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_470:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_470
	jmp	.LBB2_662
.LBB2_471:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_669
# %bb.472:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_473:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_473
	jmp	.LBB2_670
.LBB2_474:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_677
# %bb.475:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_476:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_476
	jmp	.LBB2_678
.LBB2_477:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_685
# %bb.478:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_479:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_479
	jmp	.LBB2_686
.LBB2_480:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_693
# %bb.481:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_482:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_482
	jmp	.LBB2_694
.LBB2_483:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_701
# %bb.484:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_485:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_485
	jmp	.LBB2_702
.LBB2_486:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_709
# %bb.487:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_488:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_488
	jmp	.LBB2_710
.LBB2_489:
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
	je	.LBB2_717
# %bb.490:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_491:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_491
	jmp	.LBB2_718
.LBB2_492:
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
	je	.LBB2_725
# %bb.493:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_494:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_494
	jmp	.LBB2_726
.LBB2_495:
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
	je	.LBB2_733
# %bb.496:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_497:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_497
	jmp	.LBB2_734
.LBB2_498:
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
	je	.LBB2_741
# %bb.499:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_500:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_500
	jmp	.LBB2_742
.LBB2_501:
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
	je	.LBB2_749
# %bb.502:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_503:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_503
	jmp	.LBB2_750
.LBB2_504:
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
	je	.LBB2_757
# %bb.505:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_506:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_506
	jmp	.LBB2_758
.LBB2_507:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_765
# %bb.508:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_509:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_509
	jmp	.LBB2_766
.LBB2_510:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_773
# %bb.511:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_512:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_512
	jmp	.LBB2_774
.LBB2_513:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_781
# %bb.514:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_515:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_515
	jmp	.LBB2_782
.LBB2_516:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_789
# %bb.517:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_518:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_518
	jmp	.LBB2_790
.LBB2_519:
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
	je	.LBB2_797
# %bb.520:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_521:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_521
	jmp	.LBB2_798
.LBB2_522:
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
	je	.LBB2_805
# %bb.523:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_524:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_524
	jmp	.LBB2_806
.LBB2_525:
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
	je	.LBB2_813
# %bb.526:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_527:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_527
	jmp	.LBB2_814
.LBB2_528:
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
	je	.LBB2_821
# %bb.529:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_530:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_530
	jmp	.LBB2_822
.LBB2_531:
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
	je	.LBB2_829
# %bb.532:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_533:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_533
	jmp	.LBB2_830
.LBB2_534:
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
	je	.LBB2_837
# %bb.535:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_536:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_536
	jmp	.LBB2_838
.LBB2_537:
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
	je	.LBB2_845
# %bb.538:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_539:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_539
	jmp	.LBB2_846
.LBB2_540:
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
	je	.LBB2_853
# %bb.541:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_542:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_542
	jmp	.LBB2_854
.LBB2_543:
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
	je	.LBB2_861
# %bb.544:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_545:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_545
	jmp	.LBB2_862
.LBB2_546:
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
	je	.LBB2_869
# %bb.547:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_548:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_548
	jmp	.LBB2_870
.LBB2_549:
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
	je	.LBB2_877
# %bb.550:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_551:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_551
	jmp	.LBB2_878
.LBB2_552:
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
	je	.LBB2_885
# %bb.553:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_554:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_554
	jmp	.LBB2_886
.LBB2_555:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_893
# %bb.556:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_557:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_557
	jmp	.LBB2_894
.LBB2_558:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_901
# %bb.559:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_560:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_560
	jmp	.LBB2_902
.LBB2_561:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_909
# %bb.562:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_563:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_563
	jmp	.LBB2_910
.LBB2_564:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_917
# %bb.565:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_566:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_566
	jmp	.LBB2_918
.LBB2_567:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_925
# %bb.568:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_569:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_569
	jmp	.LBB2_926
.LBB2_570:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_933
# %bb.571:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_572:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_572
	jmp	.LBB2_934
.LBB2_573:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_941
# %bb.574:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_575:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_575
	jmp	.LBB2_942
.LBB2_576:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_949
# %bb.577:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_578:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_578
	jmp	.LBB2_950
.LBB2_579:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_957
# %bb.580:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_581:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_581
	jmp	.LBB2_958
.LBB2_582:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_965
# %bb.583:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_584:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_584
	jmp	.LBB2_966
.LBB2_585:
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
	je	.LBB2_973
# %bb.586:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_587:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_587
	jmp	.LBB2_974
.LBB2_588:
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
	je	.LBB2_981
# %bb.589:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	eax, eax
	movdqa	xmm2, xmm0
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	movdqa	xmm3, xmmword ptr [rip + .LCPI2_0] # xmm3 = [255,255,255,255,255,255,255,255]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
.LBB2_590:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_590
	jmp	.LBB2_982
.LBB2_591:
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
	je	.LBB2_989
# %bb.592:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_593
	jmp	.LBB2_990
.LBB2_594:
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
	je	.LBB2_997
# %bb.595:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_596:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_596
	jmp	.LBB2_998
.LBB2_597:
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
	je	.LBB2_1005
# %bb.598:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_599:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_599
	jmp	.LBB2_1006
.LBB2_600:
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
	je	.LBB2_1013
# %bb.601:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_602:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_602
	jmp	.LBB2_1014
.LBB2_603:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1021
# %bb.604:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_605
	jmp	.LBB2_1022
.LBB2_606:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1029
# %bb.607:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_608
	jmp	.LBB2_1030
.LBB2_609:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1037
# %bb.610:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_611:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_611
	jmp	.LBB2_1038
.LBB2_612:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1045
# %bb.613:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_614:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_614
	jmp	.LBB2_1046
.LBB2_615:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1053
# %bb.616:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_617:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_617
	jmp	.LBB2_1054
.LBB2_618:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1061
# %bb.619:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_620:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_620
	jmp	.LBB2_1062
.LBB2_621:
	xor	edi, edi
.LBB2_622:
	test	r9b, 1
	je	.LBB2_624
# %bb.623:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_624:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_625
.LBB2_629:
	xor	edi, edi
.LBB2_630:
	test	r9b, 1
	je	.LBB2_632
# %bb.631:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_632:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_633
.LBB2_637:
	xor	edi, edi
.LBB2_638:
	test	r9b, 1
	je	.LBB2_640
# %bb.639:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_640:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_641
.LBB2_645:
	xor	edi, edi
.LBB2_646:
	test	r9b, 1
	je	.LBB2_648
# %bb.647:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_648:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_649
.LBB2_653:
	xor	edi, edi
.LBB2_654:
	test	r9b, 1
	je	.LBB2_656
# %bb.655:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_656:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_657
.LBB2_661:
	xor	edi, edi
.LBB2_662:
	test	r9b, 1
	je	.LBB2_664
# %bb.663:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_664:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_665
.LBB2_669:
	xor	edi, edi
.LBB2_670:
	test	r9b, 1
	je	.LBB2_672
# %bb.671:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_672:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_673
.LBB2_677:
	xor	edi, edi
.LBB2_678:
	test	r9b, 1
	je	.LBB2_680
# %bb.679:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	mulpd	xmm2, xmm1
	mulpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_680:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_681
.LBB2_685:
	xor	edi, edi
.LBB2_686:
	test	r9b, 1
	je	.LBB2_688
# %bb.687:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_688:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_689
.LBB2_693:
	xor	edi, edi
.LBB2_694:
	test	r9b, 1
	je	.LBB2_696
# %bb.695:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_696:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_697
.LBB2_701:
	xor	edi, edi
.LBB2_702:
	test	r9b, 1
	je	.LBB2_704
# %bb.703:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_704:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_705
.LBB2_709:
	xor	edi, edi
.LBB2_710:
	test	r9b, 1
	je	.LBB2_712
# %bb.711:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_712:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_713
.LBB2_717:
	xor	eax, eax
.LBB2_718:
	test	r9b, 1
	je	.LBB2_720
# %bb.719:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_720:
	cmp	rdi, r10
	je	.LBB2_1069
	jmp	.LBB2_721
.LBB2_725:
	xor	eax, eax
.LBB2_726:
	test	r9b, 1
	je	.LBB2_728
# %bb.727:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_728:
	cmp	rdi, r10
	je	.LBB2_1069
	jmp	.LBB2_729
.LBB2_733:
	xor	edi, edi
.LBB2_734:
	test	r9b, 1
	je	.LBB2_736
# %bb.735:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_736:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_737
.LBB2_741:
	xor	edi, edi
.LBB2_742:
	test	r9b, 1
	je	.LBB2_744
# %bb.743:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_744:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_745
.LBB2_749:
	xor	edi, edi
.LBB2_750:
	test	r9b, 1
	je	.LBB2_752
# %bb.751:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_752:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_753
.LBB2_757:
	xor	edi, edi
.LBB2_758:
	test	r9b, 1
	je	.LBB2_760
# %bb.759:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_760:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_761
.LBB2_765:
	xor	edi, edi
.LBB2_766:
	test	r9b, 1
	je	.LBB2_768
# %bb.767:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_768:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_769
.LBB2_773:
	xor	edi, edi
.LBB2_774:
	test	r9b, 1
	je	.LBB2_776
# %bb.775:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_776:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_777
.LBB2_781:
	xor	edi, edi
.LBB2_782:
	test	r9b, 1
	je	.LBB2_784
# %bb.783:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_784:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_785
.LBB2_789:
	xor	edi, edi
.LBB2_790:
	test	r9b, 1
	je	.LBB2_792
# %bb.791:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_792:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_793
.LBB2_797:
	xor	edi, edi
.LBB2_798:
	test	r9b, 1
	je	.LBB2_800
# %bb.799:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_800:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_801
.LBB2_805:
	xor	edi, edi
.LBB2_806:
	test	r9b, 1
	je	.LBB2_808
# %bb.807:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_808:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_809
.LBB2_813:
	xor	edi, edi
.LBB2_814:
	test	r9b, 1
	je	.LBB2_816
# %bb.815:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_816:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_817
.LBB2_821:
	xor	edi, edi
.LBB2_822:
	test	r9b, 1
	je	.LBB2_824
# %bb.823:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	pmullw	xmm1, xmm0
	pmullw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_824:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_825
.LBB2_829:
	xor	edi, edi
.LBB2_830:
	test	r9b, 1
	je	.LBB2_832
# %bb.831:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_832:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_833
.LBB2_837:
	xor	edi, edi
.LBB2_838:
	test	r9b, 1
	je	.LBB2_840
# %bb.839:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_840:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_841
.LBB2_845:
	xor	edi, edi
.LBB2_846:
	test	r9b, 1
	je	.LBB2_848
# %bb.847:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_848:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_849
.LBB2_853:
	xor	edi, edi
.LBB2_854:
	test	r9b, 1
	je	.LBB2_856
# %bb.855:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_856:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_857
.LBB2_861:
	xor	edi, edi
.LBB2_862:
	test	r9b, 1
	je	.LBB2_864
# %bb.863:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_864:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_865
.LBB2_869:
	xor	edi, edi
.LBB2_870:
	test	r9b, 1
	je	.LBB2_872
# %bb.871:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_872:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_873
.LBB2_877:
	xor	edi, edi
.LBB2_878:
	test	r9b, 1
	je	.LBB2_880
# %bb.879:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_880:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_881
.LBB2_885:
	xor	edi, edi
.LBB2_886:
	test	r9b, 1
	je	.LBB2_888
# %bb.887:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_888:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_889
.LBB2_893:
	xor	edi, edi
.LBB2_894:
	test	r9b, 1
	je	.LBB2_896
# %bb.895:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_896:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_897
.LBB2_901:
	xor	edi, edi
.LBB2_902:
	test	r9b, 1
	je	.LBB2_904
# %bb.903:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	mulps	xmm2, xmm1
	mulps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_904:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_905
.LBB2_909:
	xor	edi, edi
.LBB2_910:
	test	r9b, 1
	je	.LBB2_912
# %bb.911:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_912:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_913
.LBB2_917:
	xor	edi, edi
.LBB2_918:
	test	r9b, 1
	je	.LBB2_920
# %bb.919:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_920:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_921
.LBB2_925:
	xor	edi, edi
.LBB2_926:
	test	r9b, 1
	je	.LBB2_928
# %bb.927:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_928:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_929
.LBB2_933:
	xor	edi, edi
.LBB2_934:
	test	r9b, 1
	je	.LBB2_936
# %bb.935:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_936:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_937
.LBB2_941:
	xor	edi, edi
.LBB2_942:
	test	r9b, 1
	je	.LBB2_944
# %bb.943:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_944:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_945
.LBB2_949:
	xor	edi, edi
.LBB2_950:
	test	r9b, 1
	je	.LBB2_952
# %bb.951:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_952:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_953
.LBB2_957:
	xor	edi, edi
.LBB2_958:
	test	r9b, 1
	je	.LBB2_960
# %bb.959:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_960:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_961
.LBB2_965:
	xor	edi, edi
.LBB2_966:
	test	r9b, 1
	je	.LBB2_968
# %bb.967:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_968:
	cmp	rdx, rax
	je	.LBB2_1069
	jmp	.LBB2_969
.LBB2_973:
	xor	eax, eax
.LBB2_974:
	test	r9b, 1
	je	.LBB2_976
# %bb.975:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_976:
	cmp	rdi, r10
	je	.LBB2_1069
	jmp	.LBB2_977
.LBB2_981:
	xor	eax, eax
.LBB2_982:
	test	r9b, 1
	je	.LBB2_984
# %bb.983:
	movdqu	xmm2, xmmword ptr [rcx + rax]
	movdqu	xmm3, xmmword ptr [rcx + rax + 16]
	movdqa	xmm4, xmm0
	punpckhbw	xmm4, xmm4              # xmm4 = xmm4[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmovzxbw	xmm5, xmm2                      # xmm5 = xmm2[0],zero,xmm2[1],zero,xmm2[2],zero,xmm2[3],zero,xmm2[4],zero,xmm2[5],zero,xmm2[6],zero,xmm2[7],zero
	punpckhbw	xmm2, xmm2              # xmm2 = xmm2[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15]
	pmullw	xmm2, xmm4
	movdqa	xmm4, xmmword ptr [rip + .LCPI2_0] # xmm4 = [255,255,255,255,255,255,255,255]
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
.LBB2_984:
	cmp	rdi, r10
	je	.LBB2_1069
	jmp	.LBB2_985
.LBB2_989:
	xor	edi, edi
.LBB2_990:
	test	r9b, 1
	je	.LBB2_992
# %bb.991:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_992:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_993
.LBB2_997:
	xor	edi, edi
.LBB2_998:
	test	r9b, 1
	je	.LBB2_1000
# %bb.999:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1000:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1001
.LBB2_1005:
	xor	edi, edi
.LBB2_1006:
	test	r9b, 1
	je	.LBB2_1008
# %bb.1007:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_1008:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1009
.LBB2_1013:
	xor	edi, edi
.LBB2_1014:
	test	r9b, 1
	je	.LBB2_1016
# %bb.1015:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_1016:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1017
.LBB2_1021:
	xor	edi, edi
.LBB2_1022:
	test	r9b, 1
	je	.LBB2_1024
# %bb.1023:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1024:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1025
.LBB2_1029:
	xor	edi, edi
.LBB2_1030:
	test	r9b, 1
	je	.LBB2_1032
# %bb.1031:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	pmulld	xmm1, xmm0
	pmulld	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1032:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1033
.LBB2_1037:
	xor	edi, edi
.LBB2_1038:
	test	r9b, 1
	je	.LBB2_1040
# %bb.1039:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1040:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1041
.LBB2_1045:
	xor	edi, edi
.LBB2_1046:
	test	r9b, 1
	je	.LBB2_1048
# %bb.1047:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1048:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1049
.LBB2_1053:
	xor	edi, edi
.LBB2_1054:
	test	r9b, 1
	je	.LBB2_1056
# %bb.1055:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_1056:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1057
.LBB2_1061:
	xor	edi, edi
.LBB2_1062:
	test	r9b, 1
	je	.LBB2_1064
# %bb.1063:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_1064:
	cmp	rsi, r10
	je	.LBB2_1069
	jmp	.LBB2_1065
.Lfunc_end2:
	.size	arithmetic_scalar_arr_sse4, .Lfunc_end2-arithmetic_scalar_arr_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_unary_same_types_sse4
.LCPI3_0:
	.quad	0x8000000000000000              # double -0
	.quad	0x8000000000000000              # double -0
.LCPI3_1:
	.quad	0x3ff0000000000000              # double 1
	.quad	0x3ff0000000000000              # double 1
.LCPI3_3:
	.long	1                               # 0x1
	.long	1                               # 0x1
	.long	1                               # 0x1
	.long	1                               # 0x1
.LCPI3_4:
	.quad	1                               # 0x1
	.quad	1                               # 0x1
.LCPI3_5:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
.LCPI3_6:
	.zero	16,1
.LCPI3_7:
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
.LCPI3_8:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.quad	9223372036854775807             # 0x7fffffffffffffff
.LCPI3_9:
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
	.long	2147483647                      # 0x7fffffff
.LCPI3_10:
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
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3
.LCPI3_2:
	.quad	0x3ff0000000000000              # double 1
	.text
	.globl	arithmetic_unary_same_types_sse4
	.p2align	4, 0x90
	.type	arithmetic_unary_same_types_sse4,@function
arithmetic_unary_same_types_sse4:       # @arithmetic_unary_same_types_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 19
	jle	.LBB3_12
# %bb.1:
	cmp	sil, 20
	je	.LBB3_22
# %bb.2:
	cmp	sil, 25
	je	.LBB3_30
# %bb.3:
	cmp	sil, 26
	jne	.LBB3_923
# %bb.4:
	cmp	edi, 6
	jg	.LBB3_46
# %bb.5:
	cmp	edi, 3
	jle	.LBB3_81
# %bb.6:
	cmp	edi, 4
	je	.LBB3_131
# %bb.7:
	cmp	edi, 5
	je	.LBB3_134
# %bb.8:
	cmp	edi, 6
	jne	.LBB3_923
# %bb.9:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.10:
	mov	r9d, r8d
	cmp	r8d, 8
	jae	.LBB3_221
# %bb.11:
	xor	edx, edx
	jmp	.LBB3_373
.LBB3_12:
	cmp	sil, 4
	je	.LBB3_38
# %bb.13:
	cmp	sil, 5
	jne	.LBB3_923
# %bb.14:
	cmp	edi, 6
	jg	.LBB3_53
# %bb.15:
	cmp	edi, 3
	jle	.LBB3_86
# %bb.16:
	cmp	edi, 4
	je	.LBB3_137
# %bb.17:
	cmp	edi, 5
	je	.LBB3_140
# %bb.18:
	cmp	edi, 6
	jne	.LBB3_923
# %bb.19:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.20:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_21
# %bb.223:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_374
# %bb.224:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_374
.LBB3_21:
	xor	esi, esi
.LBB3_614:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_616
.LBB3_615:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_615
.LBB3_616:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_617:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_617
	jmp	.LBB3_923
.LBB3_22:
	cmp	edi, 6
	jg	.LBB3_60
# %bb.23:
	cmp	edi, 3
	jle	.LBB3_91
# %bb.24:
	cmp	edi, 4
	je	.LBB3_143
# %bb.25:
	cmp	edi, 5
	je	.LBB3_146
# %bb.26:
	cmp	edi, 6
	jne	.LBB3_923
# %bb.27:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.28:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_29
# %bb.226:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_377
# %bb.227:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_377
.LBB3_29:
	xor	esi, esi
.LBB3_622:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_624
.LBB3_623:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	dword ptr [rdx + 4*rsi], 0
	setne	al
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_623
.LBB3_624:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_625:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	dword ptr [rdx + 4*rsi], 0
	setne	al
	mov	dword ptr [rcx + 4*rsi], eax
	xor	eax, eax
	cmp	dword ptr [rdx + 4*rsi + 4], 0
	setne	al
	mov	dword ptr [rcx + 4*rsi + 4], eax
	xor	eax, eax
	cmp	dword ptr [rdx + 4*rsi + 8], 0
	setne	al
	mov	dword ptr [rcx + 4*rsi + 8], eax
	xor	eax, eax
	cmp	dword ptr [rdx + 4*rsi + 12], 0
	setne	al
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_625
	jmp	.LBB3_923
.LBB3_30:
	cmp	edi, 6
	jg	.LBB3_67
# %bb.31:
	cmp	edi, 3
	jle	.LBB3_96
# %bb.32:
	cmp	edi, 4
	je	.LBB3_149
# %bb.33:
	cmp	edi, 5
	je	.LBB3_152
# %bb.34:
	cmp	edi, 6
	jne	.LBB3_923
# %bb.35:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.36:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_37
# %bb.229:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_380
# %bb.230:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_380
.LBB3_37:
	xor	esi, esi
.LBB3_536:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_538
.LBB3_537:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_537
.LBB3_538:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_539:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_539
	jmp	.LBB3_923
.LBB3_38:
	cmp	edi, 6
	jg	.LBB3_74
# %bb.39:
	cmp	edi, 3
	jle	.LBB3_101
# %bb.40:
	cmp	edi, 4
	je	.LBB3_155
# %bb.41:
	cmp	edi, 5
	je	.LBB3_158
# %bb.42:
	cmp	edi, 6
	jne	.LBB3_923
# %bb.43:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.44:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_45
# %bb.232:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_382
# %bb.233:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_382
.LBB3_45:
	xor	esi, esi
.LBB3_546:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_548
.LBB3_547:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_547
.LBB3_548:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_549:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_549
	jmp	.LBB3_923
.LBB3_46:
	cmp	edi, 8
	jle	.LBB3_106
# %bb.47:
	cmp	edi, 9
	je	.LBB3_161
# %bb.48:
	cmp	edi, 11
	je	.LBB3_164
# %bb.49:
	cmp	edi, 12
	jne	.LBB3_923
# %bb.50:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.51:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_52
# %bb.235:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_384
# %bb.236:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_384
.LBB3_52:
	xor	esi, esi
.LBB3_630:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_633
# %bb.631:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_632:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_632
.LBB3_633:
	cmp	rax, 3
	jb	.LBB3_923
# %bb.634:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_635:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_635
	jmp	.LBB3_923
.LBB3_53:
	cmp	edi, 8
	jle	.LBB3_111
# %bb.54:
	cmp	edi, 9
	je	.LBB3_167
# %bb.55:
	cmp	edi, 11
	je	.LBB3_170
# %bb.56:
	cmp	edi, 12
	jne	.LBB3_923
# %bb.57:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.58:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_59
# %bb.238:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_387
# %bb.239:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_387
.LBB3_59:
	xor	esi, esi
.LBB3_640:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_643
# %bb.641:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_642:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_642
.LBB3_643:
	cmp	rax, 3
	jb	.LBB3_923
# %bb.644:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_645:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	xorpd	xmm1, xmm0
	movlpd	qword ptr [rcx + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_645
	jmp	.LBB3_923
.LBB3_60:
	cmp	edi, 8
	jle	.LBB3_116
# %bb.61:
	cmp	edi, 9
	je	.LBB3_173
# %bb.62:
	cmp	edi, 11
	je	.LBB3_176
# %bb.63:
	cmp	edi, 12
	jne	.LBB3_923
# %bb.64:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.65:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_66
# %bb.241:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_390
# %bb.242:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_390
.LBB3_66:
	xor	esi, esi
.LBB3_650:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_652
# %bb.651:
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	movapd	xmm1, xmmword ptr [rip + .LCPI3_0] # xmm1 = [-0.0E+0,-0.0E+0]
	andpd	xmm1, xmm0
	movsd	xmm2, qword ptr [rip + .LCPI3_2] # xmm2 = mem[0],zero
	orpd	xmm2, xmm1
	xorpd	xmm1, xmm1
	cmpeqsd	xmm1, xmm0
	andnpd	xmm1, xmm2
	movlpd	qword ptr [rcx + 8*rsi], xmm1
	or	rsi, 1
.LBB3_652:
	add	rax, r9
	je	.LBB3_923
# %bb.653:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
	movsd	xmm1, qword ptr [rip + .LCPI3_2] # xmm1 = mem[0],zero
	xorpd	xmm2, xmm2
.LBB3_654:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rdx + 8*rsi]   # xmm3 = mem[0],zero
	movapd	xmm4, xmm3
	andpd	xmm4, xmm0
	orpd	xmm4, xmm1
	cmpeqsd	xmm3, xmm2
	andnpd	xmm3, xmm4
	movlpd	qword ptr [rcx + 8*rsi], xmm3
	movsd	xmm3, qword ptr [rdx + 8*rsi + 8] # xmm3 = mem[0],zero
	movapd	xmm4, xmm3
	andpd	xmm4, xmm0
	orpd	xmm4, xmm1
	cmpeqsd	xmm3, xmm2
	andnpd	xmm3, xmm4
	movlpd	qword ptr [rcx + 8*rsi + 8], xmm3
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_654
	jmp	.LBB3_923
.LBB3_67:
	cmp	edi, 8
	jle	.LBB3_121
# %bb.68:
	cmp	edi, 9
	je	.LBB3_179
# %bb.69:
	cmp	edi, 11
	je	.LBB3_182
# %bb.70:
	cmp	edi, 12
	jne	.LBB3_923
# %bb.71:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.72:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_73
# %bb.244:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_393
# %bb.245:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_393
.LBB3_73:
	xor	esi, esi
.LBB3_659:
	movabs	r10, 9223372036854775807
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_661
.LBB3_660:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	and	rdi, r10
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_660
.LBB3_661:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_662:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_662
	jmp	.LBB3_923
.LBB3_74:
	cmp	edi, 8
	jle	.LBB3_126
# %bb.75:
	cmp	edi, 9
	je	.LBB3_185
# %bb.76:
	cmp	edi, 11
	je	.LBB3_188
# %bb.77:
	cmp	edi, 12
	jne	.LBB3_923
# %bb.78:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.79:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_80
# %bb.247:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_396
# %bb.248:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_396
.LBB3_80:
	xor	esi, esi
.LBB3_667:
	movabs	r10, 9223372036854775807
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_669
.LBB3_668:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	and	rdi, r10
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_668
.LBB3_669:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_670:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	and	rax, r10
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_670
	jmp	.LBB3_923
.LBB3_81:
	cmp	edi, 2
	je	.LBB3_191
# %bb.82:
	cmp	edi, 3
	jne	.LBB3_923
# %bb.83:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.84:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_85
# %bb.250:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_399
# %bb.251:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_399
.LBB3_85:
	xor	esi, esi
.LBB3_675:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_677
.LBB3_676:                              # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdx + rsi]
	xor	eax, eax
	sub	al, r10b
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_676
.LBB3_677:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_678:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	xor	edi, edi
	sub	dil, al
	mov	byte ptr [rcx + rsi + 3], dil
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_678
	jmp	.LBB3_923
.LBB3_86:
	cmp	edi, 2
	je	.LBB3_194
# %bb.87:
	cmp	edi, 3
	jne	.LBB3_923
# %bb.88:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.89:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_90
# %bb.253:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_402
# %bb.254:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_402
.LBB3_90:
	xor	esi, esi
.LBB3_683:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_685
.LBB3_684:                              # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdx + rsi]
	xor	eax, eax
	sub	al, r10b
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_684
.LBB3_685:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_686:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	xor	edi, edi
	sub	dil, al
	mov	byte ptr [rcx + rsi + 3], dil
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_686
	jmp	.LBB3_923
.LBB3_91:
	cmp	edi, 2
	je	.LBB3_197
# %bb.92:
	cmp	edi, 3
	jne	.LBB3_923
# %bb.93:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.94:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_95
# %bb.256:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_405
# %bb.257:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_405
.LBB3_95:
	xor	esi, esi
.LBB3_691:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_693
# %bb.692:
	mov	dil, byte ptr [rdx + rsi]
	test	dil, dil
	setne	r8b
	neg	r8b
	test	dil, dil
	movzx	r8d, r8b
	mov	edi, 1
	cmovle	edi, r8d
	mov	byte ptr [rcx + rsi], dil
	or	rsi, 1
.LBB3_693:
	add	rax, r9
	je	.LBB3_923
# %bb.694:
	mov	edi, 1
.LBB3_695:                              # =>This Inner Loop Header: Depth=1
	movzx	r8d, byte ptr [rdx + rsi]
	test	r8b, r8b
	setne	al
	neg	al
	test	r8b, r8b
	movzx	eax, al
	cmovg	eax, edi
	mov	byte ptr [rcx + rsi], al
	movzx	r8d, byte ptr [rdx + rsi + 1]
	test	r8b, r8b
	setne	al
	neg	al
	test	r8b, r8b
	movzx	eax, al
	cmovg	eax, edi
	mov	byte ptr [rcx + rsi + 1], al
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_695
	jmp	.LBB3_923
.LBB3_96:
	cmp	edi, 2
	je	.LBB3_200
# %bb.97:
	cmp	edi, 3
	jne	.LBB3_923
# %bb.98:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.99:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_100
# %bb.259:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_408
# %bb.260:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_408
.LBB3_100:
	xor	esi, esi
.LBB3_700:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_702
# %bb.701:
	movsx	edi, byte ptr [rdx + rsi]
	mov	r8d, edi
	sar	r8d, 7
	add	edi, r8d
	xor	edi, r8d
	mov	byte ptr [rcx + rsi], dil
	or	rsi, 1
.LBB3_702:
	add	rax, r9
	je	.LBB3_923
.LBB3_703:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	edi, eax
	sar	edi, 7
	add	eax, edi
	xor	eax, edi
	mov	byte ptr [rcx + rsi], al
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	edi, eax
	sar	edi, 7
	add	eax, edi
	xor	eax, edi
	mov	byte ptr [rcx + rsi + 1], al
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_703
	jmp	.LBB3_923
.LBB3_101:
	cmp	edi, 2
	je	.LBB3_203
# %bb.102:
	cmp	edi, 3
	jne	.LBB3_923
# %bb.103:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.104:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_105
# %bb.262:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_411
# %bb.263:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_411
.LBB3_105:
	xor	esi, esi
.LBB3_708:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_710
# %bb.709:
	movsx	edi, byte ptr [rdx + rsi]
	mov	r8d, edi
	sar	r8d, 7
	add	edi, r8d
	xor	edi, r8d
	mov	byte ptr [rcx + rsi], dil
	or	rsi, 1
.LBB3_710:
	add	rax, r9
	je	.LBB3_923
.LBB3_711:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, byte ptr [rdx + rsi]
	mov	edi, eax
	sar	edi, 7
	add	eax, edi
	xor	eax, edi
	mov	byte ptr [rcx + rsi], al
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	edi, eax
	sar	edi, 7
	add	eax, edi
	xor	eax, edi
	mov	byte ptr [rcx + rsi + 1], al
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_711
	jmp	.LBB3_923
.LBB3_106:
	cmp	edi, 7
	je	.LBB3_206
# %bb.107:
	cmp	edi, 8
	jne	.LBB3_923
# %bb.108:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.109:
	mov	r9d, r8d
	cmp	r8d, 4
	jae	.LBB3_265
# %bb.110:
	xor	edx, edx
	jmp	.LBB3_420
.LBB3_111:
	cmp	edi, 7
	je	.LBB3_209
# %bb.112:
	cmp	edi, 8
	jne	.LBB3_923
# %bb.113:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.114:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_115
# %bb.267:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_421
# %bb.268:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_421
.LBB3_115:
	xor	esi, esi
.LBB3_716:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_718
.LBB3_717:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_717
.LBB3_718:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_719:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_719
	jmp	.LBB3_923
.LBB3_116:
	cmp	edi, 7
	je	.LBB3_212
# %bb.117:
	cmp	edi, 8
	jne	.LBB3_923
# %bb.118:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.119:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_120
# %bb.270:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_424
# %bb.271:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_424
.LBB3_120:
	xor	esi, esi
.LBB3_724:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_726
.LBB3_725:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	qword ptr [rdx + 8*rsi], 0
	setne	al
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_725
.LBB3_726:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_727:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	qword ptr [rdx + 8*rsi], 0
	setne	al
	mov	qword ptr [rcx + 8*rsi], rax
	xor	eax, eax
	cmp	qword ptr [rdx + 8*rsi + 8], 0
	setne	al
	mov	qword ptr [rcx + 8*rsi + 8], rax
	xor	eax, eax
	cmp	qword ptr [rdx + 8*rsi + 16], 0
	setne	al
	mov	qword ptr [rcx + 8*rsi + 16], rax
	xor	eax, eax
	cmp	qword ptr [rdx + 8*rsi + 24], 0
	setne	al
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_727
	jmp	.LBB3_923
.LBB3_121:
	cmp	edi, 7
	je	.LBB3_215
# %bb.122:
	cmp	edi, 8
	jne	.LBB3_923
# %bb.123:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.124:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_125
# %bb.273:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_427
# %bb.274:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_427
.LBB3_125:
	xor	esi, esi
.LBB3_556:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_558
.LBB3_557:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_557
.LBB3_558:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_559:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_559
	jmp	.LBB3_923
.LBB3_126:
	cmp	edi, 7
	je	.LBB3_218
# %bb.127:
	cmp	edi, 8
	jne	.LBB3_923
# %bb.128:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.129:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_130
# %bb.276:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_429
# %bb.277:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_429
.LBB3_130:
	xor	esi, esi
.LBB3_566:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_568
.LBB3_567:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_567
.LBB3_568:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_569:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_569
	jmp	.LBB3_923
.LBB3_131:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.132:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB3_279
# %bb.133:
	xor	edx, edx
	jmp	.LBB3_437
.LBB3_134:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.135:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_136
# %bb.281:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_438
# %bb.282:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_438
.LBB3_136:
	xor	esi, esi
.LBB3_732:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_734
.LBB3_733:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_733
.LBB3_734:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_735:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_735
	jmp	.LBB3_923
.LBB3_137:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.138:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_139
# %bb.284:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_441
# %bb.285:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_441
.LBB3_139:
	xor	esi, esi
.LBB3_740:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_742
.LBB3_741:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_741
.LBB3_742:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_743:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_743
	jmp	.LBB3_923
.LBB3_140:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.141:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_142
# %bb.287:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_444
# %bb.288:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_444
.LBB3_142:
	xor	esi, esi
.LBB3_748:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_750
.LBB3_749:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_749
.LBB3_750:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_751:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 2]
	mov	word ptr [rcx + 2*rsi + 2], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 4]
	mov	word ptr [rcx + 2*rsi + 4], ax
	xor	eax, eax
	sub	ax, word ptr [rdx + 2*rsi + 6]
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_751
	jmp	.LBB3_923
.LBB3_143:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.144:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_145
# %bb.290:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_447
# %bb.291:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_447
.LBB3_145:
	xor	esi, esi
.LBB3_756:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_758
.LBB3_757:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	word ptr [rdx + 2*rsi], 0
	setne	al
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_757
.LBB3_758:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_759:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	word ptr [rdx + 2*rsi], 0
	setne	al
	mov	word ptr [rcx + 2*rsi], ax
	xor	eax, eax
	cmp	word ptr [rdx + 2*rsi + 2], 0
	setne	al
	mov	word ptr [rcx + 2*rsi + 2], ax
	xor	eax, eax
	cmp	word ptr [rdx + 2*rsi + 4], 0
	setne	al
	mov	word ptr [rcx + 2*rsi + 4], ax
	xor	eax, eax
	cmp	word ptr [rdx + 2*rsi + 6], 0
	setne	al
	mov	word ptr [rcx + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_759
	jmp	.LBB3_923
.LBB3_146:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.147:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_148
# %bb.293:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_450
# %bb.294:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_450
.LBB3_148:
	xor	esi, esi
.LBB3_764:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_766
# %bb.765:
	movzx	r8d, word ptr [rdx + 2*rsi]
	xor	r10d, r10d
	test	r8w, r8w
	setne	r10b
	neg	r10d
	test	r8w, r8w
	mov	edi, 1
	cmovle	edi, r10d
	mov	word ptr [rcx + 2*rsi], di
	or	rsi, 1
.LBB3_766:
	add	rax, r9
	je	.LBB3_923
# %bb.767:
	mov	r8d, 1
.LBB3_768:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	xor	eax, eax
	test	di, di
	setne	al
	neg	eax
	test	di, di
	cmovg	eax, r8d
	mov	word ptr [rcx + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	xor	edi, edi
	test	ax, ax
	setne	dil
	neg	edi
	test	ax, ax
	cmovg	edi, r8d
	mov	word ptr [rcx + 2*rsi + 2], di
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_768
	jmp	.LBB3_923
.LBB3_149:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.150:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_151
# %bb.296:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_453
# %bb.297:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_453
.LBB3_151:
	xor	esi, esi
.LBB3_576:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_578
.LBB3_577:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_577
.LBB3_578:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_579:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_579
	jmp	.LBB3_923
.LBB3_152:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.153:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_154
# %bb.299:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_455
# %bb.300:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_455
.LBB3_154:
	xor	esi, esi
.LBB3_773:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_775
# %bb.774:
	movsx	edi, word ptr [rdx + 2*rsi]
	mov	r8d, edi
	sar	r8d, 15
	add	edi, r8d
	xor	edi, r8d
	mov	word ptr [rcx + 2*rsi], di
	or	rsi, 1
.LBB3_775:
	add	rax, r9
	je	.LBB3_923
.LBB3_776:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	mov	edi, eax
	sar	edi, 15
	add	eax, edi
	xor	eax, edi
	mov	word ptr [rcx + 2*rsi], ax
	movsx	eax, word ptr [rdx + 2*rsi + 2]
	mov	edi, eax
	sar	edi, 15
	add	eax, edi
	xor	eax, edi
	mov	word ptr [rcx + 2*rsi + 2], ax
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_776
	jmp	.LBB3_923
.LBB3_155:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.156:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_157
# %bb.302:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_458
# %bb.303:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_458
.LBB3_157:
	xor	esi, esi
.LBB3_586:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_588
.LBB3_587:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_587
.LBB3_588:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_589:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_589
	jmp	.LBB3_923
.LBB3_158:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.159:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_160
# %bb.305:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_460
# %bb.306:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_460
.LBB3_160:
	xor	esi, esi
.LBB3_781:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_783
# %bb.782:
	movsx	edi, word ptr [rdx + 2*rsi]
	mov	r8d, edi
	sar	r8d, 15
	add	edi, r8d
	xor	edi, r8d
	mov	word ptr [rcx + 2*rsi], di
	or	rsi, 1
.LBB3_783:
	add	rax, r9
	je	.LBB3_923
.LBB3_784:                              # =>This Inner Loop Header: Depth=1
	movsx	eax, word ptr [rdx + 2*rsi]
	mov	edi, eax
	sar	edi, 15
	add	eax, edi
	xor	eax, edi
	mov	word ptr [rcx + 2*rsi], ax
	movsx	eax, word ptr [rdx + 2*rsi + 2]
	mov	edi, eax
	sar	edi, 15
	add	eax, edi
	xor	eax, edi
	mov	word ptr [rcx + 2*rsi + 2], ax
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_784
	jmp	.LBB3_923
.LBB3_161:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.162:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_163
# %bb.308:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_463
# %bb.309:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_463
.LBB3_163:
	xor	esi, esi
.LBB3_789:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_791
.LBB3_790:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_790
.LBB3_791:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_792:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_792
	jmp	.LBB3_923
.LBB3_164:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.165:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_166
# %bb.311:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_466
# %bb.312:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_466
.LBB3_166:
	xor	esi, esi
.LBB3_797:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_800
# %bb.798:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_799:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_799
.LBB3_800:
	cmp	rax, 3
	jb	.LBB3_923
# %bb.801:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_802:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_802
	jmp	.LBB3_923
.LBB3_167:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.168:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_169
# %bb.314:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_469
# %bb.315:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_469
.LBB3_169:
	xor	esi, esi
.LBB3_807:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_809
.LBB3_808:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_808
.LBB3_809:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_810:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 8]
	mov	qword ptr [rcx + 8*rsi + 8], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 16]
	mov	qword ptr [rcx + 8*rsi + 16], rax
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi + 24]
	mov	qword ptr [rcx + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_810
	jmp	.LBB3_923
.LBB3_170:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.171:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_172
# %bb.317:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_472
# %bb.318:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_472
.LBB3_172:
	xor	esi, esi
.LBB3_815:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_818
# %bb.816:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_817:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_817
.LBB3_818:
	cmp	rax, 3
	jb	.LBB3_923
# %bb.819:
	movapd	xmm0, xmmword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_820:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm0
	movss	dword ptr [rcx + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_820
	jmp	.LBB3_923
.LBB3_173:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.174:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_175
# %bb.320:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_475
# %bb.321:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_475
.LBB3_175:
	xor	esi, esi
.LBB3_825:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_827
# %bb.826:
	mov	r8, qword ptr [rdx + 8*rsi]
	xor	r10d, r10d
	test	r8, r8
	setne	r10b
	neg	r10
	test	r8, r8
	mov	edi, 1
	cmovle	rdi, r10
	mov	qword ptr [rcx + 8*rsi], rdi
	or	rsi, 1
.LBB3_827:
	add	rax, r9
	je	.LBB3_923
# %bb.828:
	mov	r8d, 1
.LBB3_829:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	rax
	test	rdi, rdi
	cmovg	rax, r8
	mov	qword ptr [rcx + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	xor	edi, edi
	test	rax, rax
	setne	dil
	neg	rdi
	test	rax, rax
	cmovg	rdi, r8
	mov	qword ptr [rcx + 8*rsi + 8], rdi
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_829
	jmp	.LBB3_923
.LBB3_176:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.177:
	mov	eax, r8d
	cmp	r8d, 8
	jb	.LBB3_178
# %bb.323:
	lea	rsi, [rdx + 4*rax]
	cmp	rsi, rcx
	jbe	.LBB3_478
# %bb.324:
	lea	rsi, [rcx + 4*rax]
	cmp	rsi, rdx
	jbe	.LBB3_478
.LBB3_178:
	xor	esi, esi
.LBB3_481:
	mov	r8, rsi
	not	r8
	test	al, 1
	je	.LBB3_483
# %bb.482:
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	movmskps	edi, xmm0
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, edi
	xorps	xmm2, xmm2
	cmpeqss	xmm2, xmm0
	andnps	xmm2, xmm1
	movss	dword ptr [rcx + 4*rsi], xmm2
	or	rsi, 1
.LBB3_483:
	add	r8, rax
	je	.LBB3_923
# %bb.484:
	xorps	xmm0, xmm0
.LBB3_485:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	movmskps	edi, xmm1
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, edi
	cmpeqss	xmm1, xmm0
	andnps	xmm1, xmm2
	movss	dword ptr [rcx + 4*rsi], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	movmskps	edi, xmm1
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, edi
	cmpeqss	xmm1, xmm0
	andnps	xmm1, xmm2
	movss	dword ptr [rcx + 4*rsi + 4], xmm1
	add	rsi, 2
	cmp	rax, rsi
	jne	.LBB3_485
	jmp	.LBB3_923
.LBB3_179:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.180:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_181
# %bb.326:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_486
# %bb.327:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_486
.LBB3_181:
	xor	esi, esi
.LBB3_834:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_836
# %bb.835:
	mov	r8, qword ptr [rdx + 8*rsi]
	mov	rdi, r8
	neg	rdi
	cmovl	rdi, r8
	mov	qword ptr [rcx + 8*rsi], rdi
	or	rsi, 1
.LBB3_836:
	add	rax, r9
	je	.LBB3_923
.LBB3_837:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	rdi, rax
	neg	rdi
	cmovl	rdi, rax
	mov	qword ptr [rcx + 8*rsi], rdi
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	rdi, rax
	neg	rdi
	cmovl	rdi, rax
	mov	qword ptr [rcx + 8*rsi + 8], rdi
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_837
	jmp	.LBB3_923
.LBB3_182:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.183:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_184
# %bb.329:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_489
# %bb.330:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_489
.LBB3_184:
	xor	esi, esi
.LBB3_842:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_845
# %bb.843:
	mov	r10d, 2147483647
.LBB3_844:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	and	eax, r10d
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_844
.LBB3_845:
	cmp	r8, 3
	jb	.LBB3_923
# %bb.846:
	mov	eax, 2147483647
.LBB3_847:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi], edi
	mov	edi, dword ptr [rdx + 4*rsi + 4]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi + 4], edi
	mov	edi, dword ptr [rdx + 4*rsi + 8]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi + 8], edi
	mov	edi, dword ptr [rdx + 4*rsi + 12]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi + 12], edi
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_847
	jmp	.LBB3_923
.LBB3_185:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.186:
	mov	r9d, r8d
	cmp	r8d, 4
	jb	.LBB3_187
# %bb.332:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_492
# %bb.333:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_492
.LBB3_187:
	xor	esi, esi
.LBB3_852:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_854
# %bb.853:
	mov	r8, qword ptr [rdx + 8*rsi]
	mov	rdi, r8
	neg	rdi
	cmovl	rdi, r8
	mov	qword ptr [rcx + 8*rsi], rdi
	or	rsi, 1
.LBB3_854:
	add	rax, r9
	je	.LBB3_923
.LBB3_855:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	rdi, rax
	neg	rdi
	cmovl	rdi, rax
	mov	qword ptr [rcx + 8*rsi], rdi
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	mov	rdi, rax
	neg	rdi
	cmovl	rdi, rax
	mov	qword ptr [rcx + 8*rsi + 8], rdi
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_855
	jmp	.LBB3_923
.LBB3_188:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.189:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_190
# %bb.335:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_495
# %bb.336:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_495
.LBB3_190:
	xor	esi, esi
.LBB3_860:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_863
# %bb.861:
	mov	r10d, 2147483647
.LBB3_862:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	and	eax, r10d
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_862
.LBB3_863:
	cmp	r8, 3
	jb	.LBB3_923
# %bb.864:
	mov	eax, 2147483647
.LBB3_865:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi], edi
	mov	edi, dword ptr [rdx + 4*rsi + 4]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi + 4], edi
	mov	edi, dword ptr [rdx + 4*rsi + 8]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi + 8], edi
	mov	edi, dword ptr [rdx + 4*rsi + 12]
	and	edi, eax
	mov	dword ptr [rcx + 4*rsi + 12], edi
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_865
	jmp	.LBB3_923
.LBB3_191:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.192:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB3_338
# %bb.193:
	xor	edx, edx
	jmp	.LBB3_504
.LBB3_194:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.195:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_196
# %bb.340:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_505
# %bb.341:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_505
.LBB3_196:
	xor	esi, esi
.LBB3_870:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_872
.LBB3_871:                              # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdx + rsi]
	xor	eax, eax
	sub	al, r10b
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_871
.LBB3_872:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_873:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi + 1]
	mov	byte ptr [rcx + rsi + 1], al
	xor	eax, eax
	sub	al, byte ptr [rdx + rsi + 2]
	mov	byte ptr [rcx + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	xor	edi, edi
	sub	dil, al
	mov	byte ptr [rcx + rsi + 3], dil
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_873
	jmp	.LBB3_923
.LBB3_197:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.198:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_199
# %bb.343:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_508
# %bb.344:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_508
.LBB3_199:
	xor	esi, esi
.LBB3_878:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_880
.LBB3_879:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rdx + rsi], 0
	setne	byte ptr [rcx + rsi]
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_879
.LBB3_880:
	cmp	rax, 3
	jb	.LBB3_923
.LBB3_881:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rdx + rsi], 0
	setne	byte ptr [rcx + rsi]
	cmp	byte ptr [rdx + rsi + 1], 0
	setne	byte ptr [rcx + rsi + 1]
	cmp	byte ptr [rdx + rsi + 2], 0
	setne	byte ptr [rcx + rsi + 2]
	cmp	byte ptr [rdx + rsi + 3], 0
	setne	byte ptr [rcx + rsi + 3]
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_881
	jmp	.LBB3_923
.LBB3_200:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.201:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_202
# %bb.346:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_511
# %bb.347:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_511
.LBB3_202:
	xor	esi, esi
.LBB3_596:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_598
.LBB3_597:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_597
.LBB3_598:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_599:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_599
	jmp	.LBB3_923
.LBB3_203:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.204:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_205
# %bb.349:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_513
# %bb.350:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_513
.LBB3_205:
	xor	esi, esi
.LBB3_606:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_608
.LBB3_607:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_607
.LBB3_608:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_609:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_609
	jmp	.LBB3_923
.LBB3_206:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.207:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_208
# %bb.352:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_515
# %bb.353:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_515
.LBB3_208:
	xor	esi, esi
.LBB3_886:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_888
.LBB3_887:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_887
.LBB3_888:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_889:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_889
	jmp	.LBB3_923
.LBB3_209:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.210:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_211
# %bb.355:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_518
# %bb.356:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_518
.LBB3_211:
	xor	esi, esi
.LBB3_894:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_896
.LBB3_895:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_895
.LBB3_896:
	cmp	r8, 3
	jb	.LBB3_923
.LBB3_897:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 4]
	mov	dword ptr [rcx + 4*rsi + 4], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 8]
	mov	dword ptr [rcx + 4*rsi + 8], eax
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi + 12]
	mov	dword ptr [rcx + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_897
	jmp	.LBB3_923
.LBB3_212:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.213:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_214
# %bb.358:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_521
# %bb.359:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_521
.LBB3_214:
	xor	esi, esi
.LBB3_902:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_904
# %bb.903:
	mov	r8d, dword ptr [rdx + 4*rsi]
	xor	r10d, r10d
	test	r8d, r8d
	setne	r10b
	neg	r10d
	test	r8d, r8d
	mov	edi, 1
	cmovle	edi, r10d
	mov	dword ptr [rcx + 4*rsi], edi
	or	rsi, 1
.LBB3_904:
	add	rax, r9
	je	.LBB3_923
# %bb.905:
	mov	r8d, 1
.LBB3_906:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	eax
	test	edi, edi
	cmovg	eax, r8d
	mov	dword ptr [rcx + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	xor	edi, edi
	test	eax, eax
	setne	dil
	neg	edi
	test	eax, eax
	cmovg	edi, r8d
	mov	dword ptr [rcx + 4*rsi + 4], edi
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_906
	jmp	.LBB3_923
.LBB3_215:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.216:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_217
# %bb.361:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_524
# %bb.362:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_524
.LBB3_217:
	xor	esi, esi
.LBB3_911:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_913
# %bb.912:
	mov	r8d, dword ptr [rdx + 4*rsi]
	mov	edi, r8d
	neg	edi
	cmovl	edi, r8d
	mov	dword ptr [rcx + 4*rsi], edi
	or	rsi, 1
.LBB3_913:
	add	rax, r9
	je	.LBB3_923
.LBB3_914:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	edi, eax
	neg	edi
	cmovl	edi, eax
	mov	dword ptr [rcx + 4*rsi], edi
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	edi, eax
	neg	edi
	cmovl	edi, eax
	mov	dword ptr [rcx + 4*rsi + 4], edi
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_914
	jmp	.LBB3_923
.LBB3_218:
	test	r8d, r8d
	jle	.LBB3_923
# %bb.219:
	mov	r9d, r8d
	cmp	r8d, 8
	jb	.LBB3_220
# %bb.364:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_527
# %bb.365:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_527
.LBB3_220:
	xor	esi, esi
.LBB3_919:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_921
# %bb.920:
	mov	r8d, dword ptr [rdx + 4*rsi]
	mov	edi, r8d
	neg	edi
	cmovl	edi, r8d
	mov	dword ptr [rcx + 4*rsi], edi
	or	rsi, 1
.LBB3_921:
	add	rax, r9
	je	.LBB3_923
.LBB3_922:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	edi, eax
	neg	edi
	cmovl	edi, eax
	mov	dword ptr [rcx + 4*rsi], edi
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	mov	edi, eax
	neg	edi
	cmovl	edi, eax
	mov	dword ptr [rcx + 4*rsi + 4], edi
	add	rsi, 2
	cmp	r9, rsi
	jne	.LBB3_922
	jmp	.LBB3_923
.LBB3_221:
	mov	edx, r9d
	and	edx, -8
	lea	rax, [rdx - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	esi, edi
	and	esi, 7
	cmp	rax, 56
	jae	.LBB3_367
# %bb.222:
	xor	eax, eax
	jmp	.LBB3_369
.LBB3_265:
	mov	edx, r9d
	and	edx, -4
	lea	rax, [rdx - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	esi, edi
	and	esi, 7
	cmp	rax, 28
	jae	.LBB3_414
# %bb.266:
	xor	eax, eax
	jmp	.LBB3_416
.LBB3_279:
	mov	edx, r9d
	and	edx, -16
	lea	rax, [rdx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 7
	cmp	rax, 112
	jae	.LBB3_431
# %bb.280:
	xor	eax, eax
	jmp	.LBB3_433
.LBB3_338:
	mov	edx, r9d
	and	edx, -32
	lea	rax, [rdx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 7
	cmp	rax, 224
	jae	.LBB3_498
# %bb.339:
	xor	eax, eax
	jmp	.LBB3_500
.LBB3_374:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_610
# %bb.375:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_376:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_376
	jmp	.LBB3_611
.LBB3_377:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_618
# %bb.378:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI3_3] # xmm1 = [1,1,1,1]
.LBB3_379:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm3, xmmword ptr [rdx + 4*rdi + 48]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm3
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_379
	jmp	.LBB3_619
.LBB3_380:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB3_530
# %bb.381:
	xor	eax, eax
	jmp	.LBB3_532
.LBB3_382:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	rdi, rax
	shr	rdi, 3
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 24
	jae	.LBB3_540
# %bb.383:
	xor	eax, eax
	jmp	.LBB3_542
.LBB3_384:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_626
# %bb.385:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_386:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 8*rdi]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_386
	jmp	.LBB3_627
.LBB3_387:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_636
# %bb.388:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_0] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_389:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 8*rdi]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_389
	jmp	.LBB3_637
.LBB3_390:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_646
# %bb.391:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI3_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_1] # xmm2 = [1.0E+0,1.0E+0]
.LBB3_392:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm3, xmmword ptr [rdx + 8*rdi]
	movupd	xmm4, xmmword ptr [rdx + 8*rdi + 16]
	movapd	xmm5, xmm3
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm4
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	cmpneqpd	xmm3, xmm0
	andpd	xmm3, xmm5
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm6
	movupd	xmmword ptr [rcx + 8*rdi], xmm3
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm4
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm4, xmmword ptr [rdx + 8*rdi + 48]
	movapd	xmm5, xmm3
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm4
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	cmpneqpd	xmm3, xmm0
	andpd	xmm3, xmm5
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm6
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm3
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm4
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_392
	jmp	.LBB3_647
.LBB3_393:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_655
# %bb.394:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_8] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB3_395:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 8*rdi]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_395
	jmp	.LBB3_656
.LBB3_396:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_663
# %bb.397:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_8] # xmm0 = [9223372036854775807,9223372036854775807]
.LBB3_398:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 8*rdi]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_398
	jmp	.LBB3_664
.LBB3_399:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_671
# %bb.400:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_401:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm2
	movdqu	xmmword ptr [rcx + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [rcx + rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_401
	jmp	.LBB3_672
.LBB3_402:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_679
# %bb.403:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_404:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm2
	movdqu	xmmword ptr [rcx + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [rcx + rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_404
	jmp	.LBB3_680
.LBB3_405:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_687
# %bb.406:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_6] # xmm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB3_407:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + rdi]
	movdqu	xmm6, xmmword ptr [rdx + rdi + 16]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm5
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm6
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [rcx + rdi], xmm7
	movdqu	xmmword ptr [rcx + rdi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm6, xmmword ptr [rdx + rdi + 48]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm5
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm6
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [rcx + rdi + 32], xmm7
	movdqu	xmmword ptr [rcx + rdi + 48], xmm5
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_407
	jmp	.LBB3_688
.LBB3_408:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_696
# %bb.409:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm8, xmmword ptr [rip + .LCPI3_10] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB3_410:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rdi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rdi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rdi]
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
	movdqu	xmmword ptr [rcx + rdi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rdi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rdi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rdi + 16]
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
	movdqu	xmmword ptr [rcx + rdi + 16], xmm2
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_410
	jmp	.LBB3_697
.LBB3_411:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_704
# %bb.412:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movdqa	xmm8, xmmword ptr [rip + .LCPI3_10] # xmm8 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
.LBB3_413:                              # =>This Inner Loop Header: Depth=1
	pmovsxbd	xmm4, dword ptr [rdx + rdi + 12]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm3, dword ptr [rdx + rdi + 4]
	pmovsxbd	xmm2, dword ptr [rdx + rdi]
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
	movdqu	xmmword ptr [rcx + rdi], xmm2
	pmovsxbd	xmm4, dword ptr [rdx + rdi + 28]
	pmovsxbd	xmm1, dword ptr [rdx + rdi + 24]
	pmovsxbd	xmm3, dword ptr [rdx + rdi + 20]
	pmovsxbd	xmm2, dword ptr [rdx + rdi + 16]
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
	movdqu	xmmword ptr [rcx + rdi + 16], xmm2
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_413
	jmp	.LBB3_705
.LBB3_421:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_712
# %bb.422:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_423:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_423
	jmp	.LBB3_713
.LBB3_424:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_720
# %bb.425:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI3_4] # xmm1 = [1,1]
.LBB3_426:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	pcmpeqq	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm3, xmmword ptr [rdx + 8*rdi + 48]
	pcmpeqq	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 48], xmm3
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_426
	jmp	.LBB3_721
.LBB3_427:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB3_550
# %bb.428:
	xor	eax, eax
	jmp	.LBB3_552
.LBB3_429:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	rdi, rax
	shr	rdi, 2
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 12
	jae	.LBB3_560
# %bb.430:
	xor	eax, eax
	jmp	.LBB3_562
.LBB3_438:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_728
# %bb.439:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_440:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_440
	jmp	.LBB3_729
.LBB3_441:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_736
# %bb.442:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_443:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_443
	jmp	.LBB3_737
.LBB3_444:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_744
# %bb.445:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_446:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 48]
	pxor	xmm2, xmm2
	psubw	xmm2, xmm0
	pxor	xmm0, xmm0
	psubw	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm0
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_446
	jmp	.LBB3_745
.LBB3_447:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_752
# %bb.448:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI3_5] # xmm1 = [1,1,1,1,1,1,1,1]
.LBB3_449:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm3, xmmword ptr [rdx + 2*rdi + 16]
	pcmpeqw	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqw	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm3, xmmword ptr [rdx + 2*rdi + 48]
	pcmpeqw	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqw	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm3
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_449
	jmp	.LBB3_753
.LBB3_450:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_760
# %bb.451:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_5] # xmm4 = [1,1,1,1,1,1,1,1]
.LBB3_452:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm6, xmmword ptr [rdx + 2*rdi + 16]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm5
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm6
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [rcx + 2*rdi], xmm7
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + 2*rdi + 32]
	movdqu	xmm6, xmmword ptr [rdx + 2*rdi + 48]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm5
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm6
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 32], xmm7
	movdqu	xmmword ptr [rcx + 2*rdi + 48], xmm5
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_452
	jmp	.LBB3_761
.LBB3_453:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 48
	jae	.LBB3_570
# %bb.454:
	xor	eax, eax
	jmp	.LBB3_572
.LBB3_455:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_769
# %bb.456:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB3_457:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rdi]
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
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rdi + 16]
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
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_457
	jmp	.LBB3_770
.LBB3_458:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 48
	jae	.LBB3_580
# %bb.459:
	xor	eax, eax
	jmp	.LBB3_582
.LBB3_460:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_777
# %bb.461:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
.LBB3_462:                              # =>This Inner Loop Header: Depth=1
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 8]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rdi]
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
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi + 24]
	pmovsxwd	xmm2, qword ptr [rdx + 2*rdi + 16]
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
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_462
	jmp	.LBB3_778
.LBB3_463:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_785
# %bb.464:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_465:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_465
	jmp	.LBB3_786
.LBB3_466:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_793
# %bb.467:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_468:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 4*rdi]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_468
	jmp	.LBB3_794
.LBB3_469:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_803
# %bb.470:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_471:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm2, xmm2
	psubq	xmm2, xmm0
	pxor	xmm0, xmm0
	psubq	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 8*rdi + 48], xmm0
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_471
	jmp	.LBB3_804
.LBB3_472:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_811
# %bb.473:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_474:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 4*rdi]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	xorpd	xmm1, xmm0
	xorpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_474
	jmp	.LBB3_812
.LBB3_475:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_821
# %bb.476:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_4] # xmm4 = [1,1]
.LBB3_477:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm6, xmmword ptr [rdx + 8*rdi + 16]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm6
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvpd	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvpd	xmm5, xmm6, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm7
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm6, xmmword ptr [rdx + 8*rdi + 48]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm6
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvpd	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvpd	xmm5, xmm6, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm7
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm5
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_477
	jmp	.LBB3_822
.LBB3_478:
	mov	esi, eax
	and	esi, -8
	xor	edi, edi
	xorps	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI3_3] # xmm1 = [1,1,1,1]
.LBB3_479:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm4, xmm2
	psrad	xmm4, 31
	por	xmm4, xmm1
	movdqa	xmm5, xmm3
	psrad	xmm5, 31
	por	xmm5, xmm1
	cvtdq2ps	xmm4, xmm4
	cvtdq2ps	xmm5, xmm5
	cmpneqps	xmm2, xmm0
	andps	xmm2, xmm4
	cmpneqps	xmm3, xmm0
	andps	xmm3, xmm5
	movups	xmmword ptr [rcx + 4*rdi], xmm2
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm3
	add	rdi, 8
	cmp	rsi, rdi
	jne	.LBB3_479
# %bb.480:
	cmp	rsi, rax
	je	.LBB3_923
	jmp	.LBB3_481
.LBB3_486:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_830
# %bb.487:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_488:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_488
	jmp	.LBB3_831
.LBB3_489:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_838
# %bb.490:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_9] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB3_491:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 4*rdi]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_491
	jmp	.LBB3_839
.LBB3_492:
	mov	esi, r9d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r8, rax
	shr	r8, 2
	add	r8, 1
	test	rax, rax
	je	.LBB3_848
# %bb.493:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_494:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 48]
	pxor	xmm3, xmm3
	psubq	xmm3, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm3, xmm0
	pxor	xmm3, xmm3
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 48], xmm2
	add	rdi, 8
	add	rax, 2
	jne	.LBB3_494
	jmp	.LBB3_849
.LBB3_495:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_856
# %bb.496:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	movapd	xmm0, xmmword ptr [rip + .LCPI3_9] # xmm0 = [2147483647,2147483647,2147483647,2147483647]
.LBB3_497:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm1, xmmword ptr [rdx + 4*rdi]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm2
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 32]
	movupd	xmm2, xmmword ptr [rdx + 4*rdi + 48]
	andpd	xmm1, xmm0
	andpd	xmm2, xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 32], xmm1
	movupd	xmmword ptr [rcx + 4*rdi + 48], xmm2
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_497
	jmp	.LBB3_857
.LBB3_505:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_866
# %bb.506:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_507:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm2
	movdqu	xmmword ptr [rcx + rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 48]
	pxor	xmm2, xmm2
	psubb	xmm2, xmm0
	pxor	xmm0, xmm0
	psubb	xmm0, xmm1
	movdqu	xmmword ptr [rcx + rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + rdi + 48], xmm0
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_507
	jmp	.LBB3_867
.LBB3_508:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_874
# %bb.509:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI3_6] # xmm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB3_510:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rdx + rdi]
	movdqu	xmm3, xmmword ptr [rdx + rdi + 16]
	pcmpeqb	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqb	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm2
	movdqu	xmmword ptr [rcx + rdi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rdx + rdi + 32]
	movdqu	xmm3, xmmword ptr [rdx + rdi + 48]
	pcmpeqb	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqb	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [rcx + rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + rdi + 48], xmm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_510
	jmp	.LBB3_875
.LBB3_511:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB3_590
# %bb.512:
	xor	eax, eax
	jmp	.LBB3_592
.LBB3_513:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB3_600
# %bb.514:
	xor	eax, eax
	jmp	.LBB3_602
.LBB3_515:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_882
# %bb.516:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_517:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_517
	jmp	.LBB3_883
.LBB3_518:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_890
# %bb.519:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_520:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pxor	xmm2, xmm2
	psubd	xmm2, xmm0
	pxor	xmm0, xmm0
	psubd	xmm0, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm2
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm0
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_520
	jmp	.LBB3_891
.LBB3_521:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_898
# %bb.522:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_3] # xmm4 = [1,1,1,1]
.LBB3_523:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm6, xmmword ptr [rdx + 4*rdi + 16]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm5
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm6
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvps	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvps	xmm5, xmm6, xmm0
	movups	xmmword ptr [rcx + 4*rdi], xmm7
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm6, xmmword ptr [rdx + 4*rdi + 48]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm5
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm6
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvps	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvps	xmm5, xmm6, xmm0
	movups	xmmword ptr [rcx + 4*rdi + 32], xmm7
	movups	xmmword ptr [rcx + 4*rdi + 48], xmm5
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_523
	jmp	.LBB3_899
.LBB3_524:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_907
# %bb.525:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_526:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_526
	jmp	.LBB3_908
.LBB3_527:
	mov	esi, r9d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r8, rax
	shr	r8, 3
	add	r8, 1
	test	rax, rax
	je	.LBB3_915
# %bb.528:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_529:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 48]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi + 32], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 48], xmm1
	add	rdi, 16
	add	rax, 2
	jne	.LBB3_529
	jmp	.LBB3_916
.LBB3_367:
	and	rdi, -8
	neg	rdi
	xor	eax, eax
	xorpd	xmm0, xmm0
.LBB3_368:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rcx + 4*rax], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 16], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 32], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 48], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 64], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 80], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 112], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 128], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 144], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 160], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 176], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 192], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 208], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 224], xmm0
	movupd	xmmword ptr [rcx + 4*rax + 240], xmm0
	add	rax, 64
	add	rdi, 8
	jne	.LBB3_368
.LBB3_369:
	test	rsi, rsi
	je	.LBB3_372
# %bb.370:
	lea	rax, [rcx + 4*rax]
	add	rax, 16
	neg	rsi
	xorpd	xmm0, xmm0
.LBB3_371:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rax - 16], xmm0
	movupd	xmmword ptr [rax], xmm0
	add	rax, 32
	inc	rsi
	jne	.LBB3_371
.LBB3_372:
	cmp	rdx, r9
	je	.LBB3_923
	.p2align	4, 0x90
.LBB3_373:                              # =>This Inner Loop Header: Depth=1
	mov	dword ptr [rcx + 4*rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_373
	jmp	.LBB3_923
.LBB3_414:
	and	rdi, -8
	neg	rdi
	xor	eax, eax
	xorpd	xmm0, xmm0
.LBB3_415:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rcx + 8*rax], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 16], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 32], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 48], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 64], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 80], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 112], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 128], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 144], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 160], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 176], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 192], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 208], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 224], xmm0
	movupd	xmmword ptr [rcx + 8*rax + 240], xmm0
	add	rax, 32
	add	rdi, 8
	jne	.LBB3_415
.LBB3_416:
	test	rsi, rsi
	je	.LBB3_419
# %bb.417:
	lea	rax, [rcx + 8*rax]
	add	rax, 16
	neg	rsi
	xorpd	xmm0, xmm0
.LBB3_418:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rax - 16], xmm0
	movupd	xmmword ptr [rax], xmm0
	add	rax, 32
	inc	rsi
	jne	.LBB3_418
.LBB3_419:
	cmp	rdx, r9
	je	.LBB3_923
	.p2align	4, 0x90
.LBB3_420:                              # =>This Inner Loop Header: Depth=1
	mov	qword ptr [rcx + 8*rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_420
	jmp	.LBB3_923
.LBB3_431:
	and	rdi, -8
	neg	rdi
	xor	eax, eax
	xorpd	xmm0, xmm0
.LBB3_432:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rcx + 2*rax], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 16], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 32], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 48], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 64], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 80], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 96], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 112], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 128], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 144], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 160], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 176], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 192], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 208], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 224], xmm0
	movupd	xmmword ptr [rcx + 2*rax + 240], xmm0
	sub	rax, -128
	add	rdi, 8
	jne	.LBB3_432
.LBB3_433:
	test	rsi, rsi
	je	.LBB3_436
# %bb.434:
	lea	rax, [rcx + 2*rax]
	add	rax, 16
	neg	rsi
	xorpd	xmm0, xmm0
.LBB3_435:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rax - 16], xmm0
	movupd	xmmword ptr [rax], xmm0
	add	rax, 32
	inc	rsi
	jne	.LBB3_435
.LBB3_436:
	cmp	rdx, r9
	je	.LBB3_923
	.p2align	4, 0x90
.LBB3_437:                              # =>This Inner Loop Header: Depth=1
	mov	word ptr [rcx + 2*rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_437
	jmp	.LBB3_923
.LBB3_498:
	and	rdi, -8
	neg	rdi
	xor	eax, eax
	xorpd	xmm0, xmm0
.LBB3_499:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rcx + rax], xmm0
	movupd	xmmword ptr [rcx + rax + 16], xmm0
	movupd	xmmword ptr [rcx + rax + 32], xmm0
	movupd	xmmword ptr [rcx + rax + 48], xmm0
	movupd	xmmword ptr [rcx + rax + 64], xmm0
	movupd	xmmword ptr [rcx + rax + 80], xmm0
	movupd	xmmword ptr [rcx + rax + 96], xmm0
	movupd	xmmword ptr [rcx + rax + 112], xmm0
	movupd	xmmword ptr [rcx + rax + 128], xmm0
	movupd	xmmword ptr [rcx + rax + 144], xmm0
	movupd	xmmword ptr [rcx + rax + 160], xmm0
	movupd	xmmword ptr [rcx + rax + 176], xmm0
	movupd	xmmword ptr [rcx + rax + 192], xmm0
	movupd	xmmword ptr [rcx + rax + 208], xmm0
	movupd	xmmword ptr [rcx + rax + 224], xmm0
	movupd	xmmword ptr [rcx + rax + 240], xmm0
	add	rax, 256
	add	rdi, 8
	jne	.LBB3_499
.LBB3_500:
	test	rsi, rsi
	je	.LBB3_503
# %bb.501:
	add	rax, rcx
	add	rax, 16
	neg	rsi
	xorpd	xmm0, xmm0
.LBB3_502:                              # =>This Inner Loop Header: Depth=1
	movupd	xmmword ptr [rax - 16], xmm0
	movupd	xmmword ptr [rax], xmm0
	add	rax, 32
	inc	rsi
	jne	.LBB3_502
.LBB3_503:
	cmp	rdx, r9
	je	.LBB3_923
	.p2align	4, 0x90
.LBB3_504:                              # =>This Inner Loop Header: Depth=1
	mov	byte ptr [rcx + rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_504
.LBB3_923:
	mov	rsp, rbp
	pop	rbp
	ret
.LBB3_530:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_531:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_531
.LBB3_532:
	test	r8, r8
	je	.LBB3_535
# %bb.533:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB3_534:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_534
.LBB3_535:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_536
.LBB3_540:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_541:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_541
.LBB3_542:
	test	r8, r8
	je	.LBB3_545
# %bb.543:
	lea	rax, [4*rax + 16]
	neg	r8
.LBB3_544:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_544
.LBB3_545:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_546
.LBB3_550:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_551:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_551
.LBB3_552:
	test	r8, r8
	je	.LBB3_555
# %bb.553:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB3_554:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_554
.LBB3_555:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_556
.LBB3_560:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_561:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_561
.LBB3_562:
	test	r8, r8
	je	.LBB3_565
# %bb.563:
	lea	rax, [8*rax + 16]
	neg	r8
.LBB3_564:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_564
.LBB3_565:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_566
.LBB3_570:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_571:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_571
.LBB3_572:
	test	r8, r8
	je	.LBB3_575
# %bb.573:
	add	rax, rax
	add	rax, 16
	neg	r8
.LBB3_574:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_574
.LBB3_575:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_576
.LBB3_580:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_581:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_581
.LBB3_582:
	test	r8, r8
	je	.LBB3_585
# %bb.583:
	add	rax, rax
	add	rax, 16
	neg	r8
.LBB3_584:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_584
.LBB3_585:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_586
.LBB3_590:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_591:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_591
.LBB3_592:
	test	r8, r8
	je	.LBB3_595
# %bb.593:
	add	rax, 16
	neg	r8
.LBB3_594:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_594
.LBB3_595:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_596
.LBB3_600:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_601:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_601
.LBB3_602:
	test	r8, r8
	je	.LBB3_605
# %bb.603:
	add	rax, 16
	neg	r8
.LBB3_604:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + rax - 16]
	movupd	xmm1, xmmword ptr [rdx + rax]
	movupd	xmmword ptr [rcx + rax - 16], xmm0
	movupd	xmmword ptr [rcx + rax], xmm1
	add	rax, 32
	inc	r8
	jne	.LBB3_604
.LBB3_605:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_606
.LBB3_610:
	xor	edi, edi
.LBB3_611:
	test	r8b, 1
	je	.LBB3_613
# %bb.612:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm3
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm2
.LBB3_613:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_614
.LBB3_618:
	xor	edi, edi
.LBB3_619:
	test	r8b, 1
	je	.LBB3_621
# %bb.620:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI3_3] # xmm3 = [1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqd	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_621:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_622
.LBB3_626:
	xor	edi, edi
.LBB3_627:
	test	r8b, 1
	je	.LBB3_629
# %bb.628:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_0] # xmm2 = [-0.0E+0,-0.0E+0]
	xorpd	xmm0, xmm2
	xorpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB3_629:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_630
.LBB3_636:
	xor	edi, edi
.LBB3_637:
	test	r8b, 1
	je	.LBB3_639
# %bb.638:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_0] # xmm2 = [-0.0E+0,-0.0E+0]
	xorpd	xmm0, xmm2
	xorpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB3_639:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_640
.LBB3_646:
	xor	edi, edi
.LBB3_647:
	test	r8b, 1
	je	.LBB3_649
# %bb.648:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI3_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmm0
	andpd	xmm4, xmm3
	movapd	xmm5, xmmword ptr [rip + .LCPI3_1] # xmm5 = [1.0E+0,1.0E+0]
	orpd	xmm4, xmm5
	andpd	xmm3, xmm1
	orpd	xmm3, xmm5
	cmpneqpd	xmm0, xmm2
	andpd	xmm0, xmm4
	cmpneqpd	xmm1, xmm2
	andpd	xmm1, xmm3
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB3_649:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_650
.LBB3_655:
	xor	edi, edi
.LBB3_656:
	test	r8b, 1
	je	.LBB3_658
# %bb.657:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_8] # xmm2 = [9223372036854775807,9223372036854775807]
	andpd	xmm0, xmm2
	andpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB3_658:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_659
.LBB3_663:
	xor	edi, edi
.LBB3_664:
	test	r8b, 1
	je	.LBB3_666
# %bb.665:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_8] # xmm2 = [9223372036854775807,9223372036854775807]
	andpd	xmm0, xmm2
	andpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 8*rdi], xmm0
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB3_666:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_667
.LBB3_671:
	xor	edi, edi
.LBB3_672:
	test	r8b, 1
	je	.LBB3_674
# %bb.673:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm3
	movdqu	xmmword ptr [rcx + rdi + 16], xmm2
.LBB3_674:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_675
.LBB3_679:
	xor	edi, edi
.LBB3_680:
	test	r8b, 1
	je	.LBB3_682
# %bb.681:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm3
	movdqu	xmmword ptr [rcx + rdi + 16], xmm2
.LBB3_682:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_683
.LBB3_687:
	xor	edi, edi
.LBB3_688:
	test	r8b, 1
	je	.LBB3_690
# %bb.689:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_6] # xmm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqb	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqb	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm2
	movdqa	xmm2, xmm4
	pblendvb	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm3, xmm0
	movdqu	xmmword ptr [rcx + rdi], xmm2
	movdqu	xmmword ptr [rcx + rdi + 16], xmm4
.LBB3_690:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_691
.LBB3_696:
	xor	edi, edi
.LBB3_697:
	test	r8b, 1
	je	.LBB3_699
# %bb.698:
	pmovsxbd	xmm3, dword ptr [rdx + rdi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rdi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rdi]
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_10] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [rcx + rdi], xmm1
.LBB3_699:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_700
.LBB3_704:
	xor	edi, edi
.LBB3_705:
	test	r8b, 1
	je	.LBB3_707
# %bb.706:
	pmovsxbd	xmm3, dword ptr [rdx + rdi + 12]
	pmovsxbd	xmm0, dword ptr [rdx + rdi + 8]
	pmovsxbd	xmm2, dword ptr [rdx + rdi + 4]
	pmovsxbd	xmm1, dword ptr [rdx + rdi]
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
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_10] # xmm4 = [255,0,0,0,255,0,0,0,255,0,0,0,255,0,0,0]
	pand	xmm3, xmm4
	pand	xmm0, xmm4
	packusdw	xmm0, xmm3
	pand	xmm2, xmm4
	pand	xmm1, xmm4
	packusdw	xmm1, xmm2
	packuswb	xmm1, xmm0
	movdqu	xmmword ptr [rcx + rdi], xmm1
.LBB3_707:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_708
.LBB3_712:
	xor	edi, edi
.LBB3_713:
	test	r8b, 1
	je	.LBB3_715
# %bb.714:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm3
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm2
.LBB3_715:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_716
.LBB3_720:
	xor	edi, edi
.LBB3_721:
	test	r8b, 1
	je	.LBB3_723
# %bb.722:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI3_4] # xmm3 = [1,1]
	pandn	xmm0, xmm3
	pcmpeqq	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [rcx + 8*rdi], xmm0
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm1
.LBB3_723:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_724
.LBB3_728:
	xor	edi, edi
.LBB3_729:
	test	r8b, 1
	je	.LBB3_731
# %bb.730:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm3
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
.LBB3_731:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_732
.LBB3_736:
	xor	edi, edi
.LBB3_737:
	test	r8b, 1
	je	.LBB3_739
# %bb.738:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm3
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
.LBB3_739:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_740
.LBB3_744:
	xor	edi, edi
.LBB3_745:
	test	r8b, 1
	je	.LBB3_747
# %bb.746:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubw	xmm3, xmm0
	psubw	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 2*rdi], xmm3
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
.LBB3_747:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_748
.LBB3_752:
	xor	edi, edi
.LBB3_753:
	test	r8b, 1
	je	.LBB3_755
# %bb.754:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI3_5] # xmm3 = [1,1,1,1,1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqw	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [rcx + 2*rdi], xmm0
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
.LBB3_755:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_756
.LBB3_760:
	xor	edi, edi
.LBB3_761:
	test	r8b, 1
	je	.LBB3_763
# %bb.762:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_5] # xmm4 = [1,1,1,1,1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqw	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqw	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm2
	movdqa	xmm2, xmm4
	pblendvb	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm3, xmm0
	movdqu	xmmword ptr [rcx + 2*rdi], xmm2
	movdqu	xmmword ptr [rcx + 2*rdi + 16], xmm4
.LBB3_763:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_764
.LBB3_769:
	xor	edi, edi
.LBB3_770:
	test	r8b, 1
	je	.LBB3_772
# %bb.771:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi]
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
	movdqu	xmmword ptr [rcx + 2*rdi], xmm1
.LBB3_772:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_773
.LBB3_777:
	xor	edi, edi
.LBB3_778:
	test	r8b, 1
	je	.LBB3_780
# %bb.779:
	pmovsxwd	xmm0, qword ptr [rdx + 2*rdi + 8]
	pmovsxwd	xmm1, qword ptr [rdx + 2*rdi]
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
	movdqu	xmmword ptr [rcx + 2*rdi], xmm1
.LBB3_780:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_781
.LBB3_785:
	xor	edi, edi
.LBB3_786:
	test	r8b, 1
	je	.LBB3_788
# %bb.787:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm3
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm2
.LBB3_788:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_789
.LBB3_793:
	xor	edi, edi
.LBB3_794:
	test	r8b, 1
	je	.LBB3_796
# %bb.795:
	movupd	xmm0, xmmword ptr [rdx + 4*rdi]
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_7] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	xorpd	xmm0, xmm2
	xorpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_796:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_797
.LBB3_803:
	xor	edi, edi
.LBB3_804:
	test	r8b, 1
	je	.LBB3_806
# %bb.805:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubq	xmm3, xmm0
	psubq	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 8*rdi], xmm3
	movdqu	xmmword ptr [rcx + 8*rdi + 16], xmm2
.LBB3_806:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_807
.LBB3_811:
	xor	edi, edi
.LBB3_812:
	test	r8b, 1
	je	.LBB3_814
# %bb.813:
	movupd	xmm0, xmmword ptr [rdx + 4*rdi]
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_7] # xmm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	xorpd	xmm0, xmm2
	xorpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_814:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_815
.LBB3_821:
	xor	edi, edi
.LBB3_822:
	test	r8b, 1
	je	.LBB3_824
# %bb.823:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_4] # xmm4 = [1,1]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqq	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqq	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm2
	movdqa	xmm2, xmm4
	blendvpd	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm4, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm2
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm4
.LBB3_824:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_825
.LBB3_830:
	xor	edi, edi
.LBB3_831:
	test	r8b, 1
	je	.LBB3_833
# %bb.832:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
.LBB3_833:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_834
.LBB3_838:
	xor	edi, edi
.LBB3_839:
	test	r8b, 1
	je	.LBB3_841
# %bb.840:
	movupd	xmm0, xmmword ptr [rdx + 4*rdi]
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_9] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	andpd	xmm0, xmm2
	andpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_841:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_842
.LBB3_848:
	xor	edi, edi
.LBB3_849:
	test	r8b, 1
	je	.LBB3_851
# %bb.850:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	pxor	xmm3, xmm3
	pxor	xmm4, xmm4
	psubq	xmm4, xmm1
	movdqa	xmm0, xmm1
	blendvpd	xmm1, xmm4, xmm0
	psubq	xmm3, xmm2
	movdqa	xmm0, xmm2
	blendvpd	xmm2, xmm3, xmm0
	movupd	xmmword ptr [rcx + 8*rdi], xmm1
	movupd	xmmword ptr [rcx + 8*rdi + 16], xmm2
.LBB3_851:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_852
.LBB3_856:
	xor	edi, edi
.LBB3_857:
	test	r8b, 1
	je	.LBB3_859
# %bb.858:
	movupd	xmm0, xmmword ptr [rdx + 4*rdi]
	movupd	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movapd	xmm2, xmmword ptr [rip + .LCPI3_9] # xmm2 = [2147483647,2147483647,2147483647,2147483647]
	andpd	xmm0, xmm2
	andpd	xmm1, xmm2
	movupd	xmmword ptr [rcx + 4*rdi], xmm0
	movupd	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_859:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_860
.LBB3_866:
	xor	edi, edi
.LBB3_867:
	test	r8b, 1
	je	.LBB3_869
# %bb.868:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubb	xmm3, xmm0
	psubb	xmm2, xmm1
	movdqu	xmmword ptr [rcx + rdi], xmm3
	movdqu	xmmword ptr [rcx + rdi + 16], xmm2
.LBB3_869:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_870
.LBB3_874:
	xor	edi, edi
.LBB3_875:
	test	r8b, 1
	je	.LBB3_877
# %bb.876:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI3_6] # xmm3 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqb	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [rcx + rdi], xmm0
	movdqu	xmmword ptr [rcx + rdi + 16], xmm1
.LBB3_877:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_878
.LBB3_882:
	xor	edi, edi
.LBB3_883:
	test	r8b, 1
	je	.LBB3_885
# %bb.884:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm3
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm2
.LBB3_885:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_886
.LBB3_890:
	xor	edi, edi
.LBB3_891:
	test	r8b, 1
	je	.LBB3_893
# %bb.892:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm2, xmm2
	pxor	xmm3, xmm3
	psubd	xmm3, xmm0
	psubd	xmm2, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm3
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm2
.LBB3_893:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_894
.LBB3_898:
	xor	edi, edi
.LBB3_899:
	test	r8b, 1
	je	.LBB3_901
# %bb.900:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI3_3] # xmm4 = [1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqd	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqd	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm2
	movdqa	xmm2, xmm4
	blendvps	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm4, xmm3, xmm0
	movups	xmmword ptr [rcx + 4*rdi], xmm2
	movups	xmmword ptr [rcx + 4*rdi + 16], xmm4
.LBB3_901:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_902
.LBB3_907:
	xor	edi, edi
.LBB3_908:
	test	r8b, 1
	je	.LBB3_910
# %bb.909:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_910:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_911
.LBB3_915:
	xor	edi, edi
.LBB3_916:
	test	r8b, 1
	je	.LBB3_918
# %bb.917:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	pabsd	xmm0, xmm0
	pabsd	xmm1, xmm1
	movdqu	xmmword ptr [rcx + 4*rdi], xmm0
	movdqu	xmmword ptr [rcx + 4*rdi + 16], xmm1
.LBB3_918:
	cmp	rsi, r9
	je	.LBB3_923
	jmp	.LBB3_919
.Lfunc_end3:
	.size	arithmetic_unary_same_types_sse4, .Lfunc_end3-arithmetic_unary_same_types_sse4
                                        # -- End function
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_unary_diff_type_sse4
.LCPI4_0:
	.quad	0x8000000000000000              # double -0
	.quad	0x8000000000000000              # double -0
.LCPI4_1:
	.quad	0x3ff0000000000000              # double 1
	.quad	0x3ff0000000000000              # double 1
.LCPI4_3:
	.long	0x7fffffff                      # float NaN
	.long	0x7fffffff                      # float NaN
	.long	0x7fffffff                      # float NaN
	.long	0x7fffffff                      # float NaN
.LCPI4_4:
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
	.long	0x80000000                      # float -0
.LCPI4_7:
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
.LCPI4_8:
	.long	1                               # 0x1
	.long	1                               # 0x1
	.long	1                               # 0x1
	.long	1                               # 0x1
.LCPI4_10:
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
	.long	0x4f000000                      # float 2.14748365E+9
.LCPI4_11:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.zero	2
	.zero	2
	.zero	2
	.zero	2
.LCPI4_12:
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
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
.LCPI4_15:
	.quad	1                               # 0x1
	.quad	1                               # 0x1
.LCPI4_16:
	.long	1                               # 0x1
	.long	1                               # 0x1
	.zero	4
	.zero	4
.LCPI4_17:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.zero	2
	.zero	2
	.zero	2
	.zero	2
	.zero	2
	.zero	2
.LCPI4_18:
	.byte	1                               # 0x1
	.byte	1                               # 0x1
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
.LCPI4_19:
	.long	0x3f800000                      # float 1
	.long	0x3f800000                      # float 1
	.long	0x3f800000                      # float 1
	.long	0x3f800000                      # float 1
.LCPI4_20:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
.LCPI4_21:
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.byte	1                               # 0x1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
	.zero	1
.LCPI4_22:
	.zero	16,1
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3
.LCPI4_2:
	.quad	0x3ff0000000000000              # double 1
.LCPI4_6:
	.quad	0x43e0000000000000              # double 9.2233720368547758E+18
.LCPI4_13:
	.quad	0xbff0000000000000              # double -1
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI4_5:
	.long	0x3f800000                      # float 1
.LCPI4_9:
	.long	0x5f000000                      # float 9.22337203E+18
.LCPI4_14:
	.long	0xbf800000                      # float -1
	.text
	.globl	arithmetic_unary_diff_type_sse4
	.p2align	4, 0x90
	.type	arithmetic_unary_diff_type_sse4,@function
arithmetic_unary_diff_type_sse4:        # @arithmetic_unary_diff_type_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r14
	push	rbx
	and	rsp, -8
	cmp	dl, 20
	jne	.LBB4_1655
# %bb.1:
	cmp	edi, 6
	jg	.LBB4_14
# %bb.2:
	cmp	edi, 3
	jle	.LBB4_26
# %bb.3:
	cmp	edi, 4
	je	.LBB4_46
# %bb.4:
	cmp	edi, 5
	je	.LBB4_54
# %bb.5:
	cmp	edi, 6
	jne	.LBB4_1655
# %bb.6:
	cmp	esi, 6
	jg	.LBB4_94
# %bb.7:
	cmp	esi, 3
	jle	.LBB4_200
# %bb.8:
	cmp	esi, 4
	je	.LBB4_303
# %bb.9:
	cmp	esi, 5
	je	.LBB4_306
# %bb.10:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.11:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.12:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_13
# %bb.494:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_496
# %bb.495:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB4_496
.LBB4_13:
	xor	edx, edx
.LBB4_1232:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1234
.LBB4_1233:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1233
.LBB4_1234:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1235:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx + 4], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 4], eax
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx + 8], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 8], eax
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx + 12], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 12], eax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1235
	jmp	.LBB4_1655
.LBB4_14:
	cmp	edi, 8
	jle	.LBB4_36
# %bb.15:
	cmp	edi, 9
	je	.LBB4_62
# %bb.16:
	cmp	edi, 11
	je	.LBB4_70
# %bb.17:
	cmp	edi, 12
	jne	.LBB4_1655
# %bb.18:
	cmp	esi, 6
	jg	.LBB4_106
# %bb.19:
	cmp	esi, 3
	jle	.LBB4_205
# %bb.20:
	cmp	esi, 4
	je	.LBB4_309
# %bb.21:
	cmp	esi, 5
	je	.LBB4_312
# %bb.22:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.23:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.24:
	mov	r11d, r9d
	xor	r10d, r10d
	cmp	r9d, 4
	jae	.LBB4_499
# %bb.25:
	xor	esi, esi
	jmp	.LBB4_1110
.LBB4_26:
	cmp	edi, 2
	je	.LBB4_78
# %bb.27:
	cmp	edi, 3
	jne	.LBB4_1655
# %bb.28:
	cmp	esi, 6
	jg	.LBB4_113
# %bb.29:
	cmp	esi, 3
	jle	.LBB4_210
# %bb.30:
	cmp	esi, 4
	je	.LBB4_315
# %bb.31:
	cmp	esi, 5
	je	.LBB4_318
# %bb.32:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.33:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.34:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_35
# %bb.502:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_504
# %bb.503:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB4_504
.LBB4_35:
	xor	edx, edx
.LBB4_1240:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1242
# %bb.1241:
	mov	r9b, byte ptr [rcx + rdx]
	xor	edi, edi
	test	r9b, r9b
	setne	dil
	neg	edi
	test	r9b, r9b
	mov	eax, 1
	cmovle	eax, edi
	mov	dword ptr [r8 + 4*rdx], eax
	or	rdx, 1
.LBB4_1242:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1243:
	mov	esi, 1
.LBB4_1244:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	dword ptr [r8 + 4*rdx], edi
	movzx	eax, byte ptr [rcx + rdx + 1]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	dword ptr [r8 + 4*rdx + 4], edi
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1244
	jmp	.LBB4_1655
.LBB4_36:
	cmp	edi, 7
	je	.LBB4_86
# %bb.37:
	cmp	edi, 8
	jne	.LBB4_1655
# %bb.38:
	cmp	esi, 6
	jg	.LBB4_123
# %bb.39:
	cmp	esi, 3
	jle	.LBB4_215
# %bb.40:
	cmp	esi, 4
	je	.LBB4_321
# %bb.41:
	cmp	esi, 5
	je	.LBB4_324
# %bb.42:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.43:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.44:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_507
# %bb.45:
	xor	edx, edx
	jmp	.LBB4_998
.LBB4_46:
	cmp	esi, 6
	jg	.LBB4_135
# %bb.47:
	cmp	esi, 3
	jle	.LBB4_220
# %bb.48:
	cmp	esi, 4
	je	.LBB4_327
# %bb.49:
	cmp	esi, 5
	je	.LBB4_330
# %bb.50:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.51:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.52:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_510
# %bb.53:
	xor	edx, edx
	jmp	.LBB4_1116
.LBB4_54:
	cmp	esi, 6
	jg	.LBB4_147
# %bb.55:
	cmp	esi, 3
	jle	.LBB4_225
# %bb.56:
	cmp	esi, 4
	je	.LBB4_333
# %bb.57:
	cmp	esi, 5
	je	.LBB4_336
# %bb.58:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.59:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.60:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB4_513
# %bb.61:
	xor	edx, edx
	jmp	.LBB4_1121
.LBB4_62:
	cmp	esi, 6
	jg	.LBB4_157
# %bb.63:
	cmp	esi, 3
	jle	.LBB4_230
# %bb.64:
	cmp	esi, 4
	je	.LBB4_339
# %bb.65:
	cmp	esi, 5
	je	.LBB4_342
# %bb.66:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.67:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.68:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_516
# %bb.69:
	xor	edx, edx
	jmp	.LBB4_1127
.LBB4_70:
	cmp	esi, 6
	jg	.LBB4_167
# %bb.71:
	cmp	esi, 3
	jle	.LBB4_235
# %bb.72:
	cmp	esi, 4
	je	.LBB4_345
# %bb.73:
	cmp	esi, 5
	je	.LBB4_348
# %bb.74:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.75:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.76:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_519
# %bb.77:
	xor	edx, edx
	jmp	.LBB4_1133
.LBB4_78:
	cmp	esi, 6
	jg	.LBB4_178
# %bb.79:
	cmp	esi, 3
	jle	.LBB4_240
# %bb.80:
	cmp	esi, 4
	je	.LBB4_351
# %bb.81:
	cmp	esi, 5
	je	.LBB4_354
# %bb.82:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.83:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.84:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_85
# %bb.522:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_524
# %bb.523:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB4_524
.LBB4_85:
	xor	edx, edx
.LBB4_1249:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1251
.LBB4_1250:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1250
.LBB4_1251:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1252:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 4], eax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 8], eax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 12], eax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1252
	jmp	.LBB4_1655
.LBB4_86:
	cmp	esi, 6
	jg	.LBB4_190
# %bb.87:
	cmp	esi, 3
	jle	.LBB4_245
# %bb.88:
	cmp	esi, 4
	je	.LBB4_357
# %bb.89:
	cmp	esi, 5
	je	.LBB4_360
# %bb.90:
	cmp	esi, 6
	jne	.LBB4_1655
# %bb.91:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.92:
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB4_93
# %bb.527:
	lea	rdx, [rcx + 4*r11]
	cmp	rdx, r8
	jbe	.LBB4_529
# %bb.528:
	lea	rdx, [r8 + 4*r11]
	cmp	rdx, rcx
	jbe	.LBB4_529
.LBB4_93:
	xor	edx, edx
.LBB4_1257:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1259
# %bb.1258:
	mov	r9d, dword ptr [rcx + 4*rdx]
	xor	r10d, r10d
	test	r9d, r9d
	setne	r10b
	neg	r10d
	test	r9d, r9d
	mov	edi, 1
	cmovle	edi, r10d
	mov	dword ptr [r8 + 4*rdx], edi
	or	rdx, 1
.LBB4_1259:
	add	rsi, r11
	je	.LBB4_1655
# %bb.1260:
	mov	esi, 1
.LBB4_1261:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rdx]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	eax
	test	edi, edi
	cmovg	eax, esi
	mov	dword ptr [r8 + 4*rdx], eax
	mov	eax, dword ptr [rcx + 4*rdx + 4]
	xor	edi, edi
	test	eax, eax
	setne	dil
	neg	edi
	test	eax, eax
	cmovg	edi, esi
	mov	dword ptr [r8 + 4*rdx + 4], edi
	add	rdx, 2
	cmp	r11, rdx
	jne	.LBB4_1261
	jmp	.LBB4_1655
.LBB4_94:
	cmp	esi, 8
	jle	.LBB4_250
# %bb.95:
	cmp	esi, 9
	je	.LBB4_363
# %bb.96:
	cmp	esi, 11
	je	.LBB4_366
# %bb.97:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.98:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.99:
	mov	edx, r9d
	lea	rsi, [rdx - 1]
	mov	eax, edx
	and	eax, 3
	cmp	rsi, 3
	jae	.LBB4_532
# %bb.100:
	xor	esi, esi
.LBB4_101:
	test	rax, rax
	je	.LBB4_1655
# %bb.102:
	lea	rdx, [r8 + 8*rsi]
	lea	rcx, [rcx + 4*rsi]
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_104
.LBB4_103:                              #   in Loop: Header=BB4_104 Depth=1
	movsd	qword ptr [rdx + 8*rsi], xmm1
	add	rsi, 1
	cmp	rax, rsi
	je	.LBB4_1655
.LBB4_104:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_103
# %bb.105:                              #   in Loop: Header=BB4_104 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_103
.LBB4_106:
	cmp	esi, 8
	jle	.LBB4_255
# %bb.107:
	cmp	esi, 9
	je	.LBB4_369
# %bb.108:
	cmp	esi, 11
	je	.LBB4_372
# %bb.109:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.110:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.111:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB4_112
# %bb.542:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_544
# %bb.543:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_544
.LBB4_112:
	xor	edx, edx
.LBB4_1266:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1268
# %bb.1267:
	movsd	xmm0, qword ptr [rcx + 8*rdx]   # xmm0 = mem[0],zero
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	andpd	xmm1, xmm0
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
	orpd	xmm2, xmm1
	xorpd	xmm1, xmm1
	cmpeqsd	xmm1, xmm0
	andnpd	xmm1, xmm2
	movlpd	qword ptr [r8 + 8*rdx], xmm1
	or	rdx, 1
.LBB4_1268:
	add	rsi, rax
	je	.LBB4_1655
# %bb.1269:
	movapd	xmm0, xmmword ptr [rip + .LCPI4_0] # xmm0 = [-0.0E+0,-0.0E+0]
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	xorpd	xmm2, xmm2
.LBB4_1270:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	movapd	xmm4, xmm3
	andpd	xmm4, xmm0
	orpd	xmm4, xmm1
	cmpeqsd	xmm3, xmm2
	andnpd	xmm3, xmm4
	movlpd	qword ptr [r8 + 8*rdx], xmm3
	movsd	xmm3, qword ptr [rcx + 8*rdx + 8] # xmm3 = mem[0],zero
	movapd	xmm4, xmm3
	andpd	xmm4, xmm0
	orpd	xmm4, xmm1
	cmpeqsd	xmm3, xmm2
	andnpd	xmm3, xmm4
	movlpd	qword ptr [r8 + 8*rdx + 8], xmm3
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_1270
	jmp	.LBB4_1655
.LBB4_113:
	cmp	esi, 8
	jle	.LBB4_260
# %bb.114:
	cmp	esi, 9
	je	.LBB4_375
# %bb.115:
	cmp	esi, 11
	je	.LBB4_378
# %bb.116:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.117:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.118:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_547
# %bb.119:
	xor	eax, eax
.LBB4_120:
	test	dl, 1
	je	.LBB4_1655
# %bb.121:
	cmp	byte ptr [rcx + rax], 0
	jne	.LBB4_982
.LBB4_122:
	xorpd	xmm0, xmm0
	jmp	.LBB4_983
.LBB4_123:
	cmp	esi, 8
	jle	.LBB4_265
# %bb.124:
	cmp	esi, 9
	je	.LBB4_381
# %bb.125:
	cmp	esi, 11
	je	.LBB4_384
# %bb.126:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.127:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.128:
	mov	edx, r9d
	lea	rsi, [rdx - 1]
	mov	eax, edx
	and	eax, 3
	cmp	rsi, 3
	jae	.LBB4_557
# %bb.129:
	xor	esi, esi
.LBB4_130:
	test	rax, rax
	je	.LBB4_1655
# %bb.131:
	lea	rdx, [r8 + 8*rsi]
	lea	rcx, [rcx + 8*rsi]
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_133
.LBB4_132:                              #   in Loop: Header=BB4_133 Depth=1
	movsd	qword ptr [rdx + 8*rsi], xmm1
	add	rsi, 1
	cmp	rax, rsi
	je	.LBB4_1655
.LBB4_133:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_132
# %bb.134:                              #   in Loop: Header=BB4_133 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_132
.LBB4_135:
	cmp	esi, 8
	jle	.LBB4_270
# %bb.136:
	cmp	esi, 9
	je	.LBB4_392
# %bb.137:
	cmp	esi, 11
	je	.LBB4_395
# %bb.138:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.139:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.140:
	mov	edx, r9d
	lea	rsi, [rdx - 1]
	mov	eax, edx
	and	eax, 3
	cmp	rsi, 3
	jae	.LBB4_567
# %bb.141:
	xor	esi, esi
.LBB4_142:
	test	rax, rax
	je	.LBB4_1655
# %bb.143:
	lea	rdx, [r8 + 8*rsi]
	lea	rcx, [rcx + 2*rsi]
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_145
.LBB4_144:                              #   in Loop: Header=BB4_145 Depth=1
	movsd	qword ptr [rdx + 8*rsi], xmm1
	add	rsi, 1
	cmp	rax, rsi
	je	.LBB4_1655
.LBB4_145:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_144
# %bb.146:                              #   in Loop: Header=BB4_145 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_144
.LBB4_147:
	cmp	esi, 8
	jle	.LBB4_275
# %bb.148:
	cmp	esi, 9
	je	.LBB4_398
# %bb.149:
	cmp	esi, 11
	je	.LBB4_401
# %bb.150:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.151:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.152:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_577
# %bb.153:
	xor	eax, eax
.LBB4_154:
	test	dl, 1
	je	.LBB4_1655
# %bb.155:
	cmp	word ptr [rcx + 2*rax], 0
	je	.LBB4_122
.LBB4_982:
	movsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
.LBB4_983:
	jle	.LBB4_985
# %bb.984:
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
.LBB4_985:
	movsd	qword ptr [r8 + 8*rax], xmm0
	jmp	.LBB4_1655
.LBB4_157:
	cmp	esi, 8
	jle	.LBB4_280
# %bb.158:
	cmp	esi, 9
	je	.LBB4_404
# %bb.159:
	cmp	esi, 11
	je	.LBB4_407
# %bb.160:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.161:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.162:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_587
# %bb.163:
	xor	eax, eax
.LBB4_164:
	test	dl, 1
	je	.LBB4_1655
# %bb.165:
	cmp	qword ptr [rcx + 8*rax], 0
	je	.LBB4_122
	jmp	.LBB4_982
.LBB4_167:
	cmp	esi, 8
	jle	.LBB4_285
# %bb.168:
	cmp	esi, 9
	je	.LBB4_413
# %bb.169:
	cmp	esi, 11
	je	.LBB4_419
# %bb.170:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.171:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.172:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_597
# %bb.173:
	xor	eax, eax
.LBB4_174:
	test	dl, 1
	je	.LBB4_1655
# %bb.175:
	movss	xmm1, dword ptr [rcx + 4*rax]   # xmm1 = mem[0],zero,zero,zero
	xorps	xmm0, xmm0
	xorps	xmm2, xmm2
	ucomiss	xmm2, xmm1
	je	.LBB4_177
# %bb.176:
	movmskps	ecx, xmm1
	and	ecx, 1
	neg	ecx
	or	ecx, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, ecx
	cvtss2sd	xmm0, xmm0
.LBB4_177:
	movsd	qword ptr [r8 + 8*rax], xmm0
	jmp	.LBB4_1655
.LBB4_178:
	cmp	esi, 8
	jle	.LBB4_293
# %bb.179:
	cmp	esi, 9
	je	.LBB4_422
# %bb.180:
	cmp	esi, 11
	je	.LBB4_425
# %bb.181:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.182:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.183:
	mov	edx, r9d
	lea	rsi, [rdx - 1]
	mov	eax, edx
	and	eax, 3
	cmp	rsi, 3
	jae	.LBB4_603
# %bb.184:
	xor	esi, esi
.LBB4_185:
	test	rax, rax
	je	.LBB4_1655
# %bb.186:
	lea	rdx, [r8 + 8*rsi]
	add	rcx, rsi
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_188
.LBB4_187:                              #   in Loop: Header=BB4_188 Depth=1
	movsd	qword ptr [rdx + 8*rsi], xmm1
	add	rsi, 1
	cmp	rax, rsi
	je	.LBB4_1655
.LBB4_188:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_187
# %bb.189:                              #   in Loop: Header=BB4_188 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_187
.LBB4_190:
	cmp	esi, 8
	jle	.LBB4_298
# %bb.191:
	cmp	esi, 9
	je	.LBB4_428
# %bb.192:
	cmp	esi, 11
	je	.LBB4_431
# %bb.193:
	cmp	esi, 12
	jne	.LBB4_1655
# %bb.194:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.195:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_613
# %bb.196:
	xor	eax, eax
.LBB4_197:
	test	dl, 1
	je	.LBB4_1655
# %bb.198:
	cmp	dword ptr [rcx + 4*rax], 0
	je	.LBB4_122
	jmp	.LBB4_982
.LBB4_200:
	cmp	esi, 2
	je	.LBB4_434
# %bb.201:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.202:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.203:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB4_204
# %bb.623:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_625
# %bb.624:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_625
.LBB4_204:
	xor	edx, edx
.LBB4_1275:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1277
.LBB4_1276:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1276
.LBB4_1277:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1278:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	dword ptr [rcx + 4*rdx + 4], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	dword ptr [rcx + 4*rdx + 8], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	dword ptr [rcx + 4*rdx + 12], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1278
	jmp	.LBB4_1655
.LBB4_205:
	cmp	esi, 2
	je	.LBB4_437
# %bb.206:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.207:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.208:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB4_209
# %bb.628:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_630
# %bb.629:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_630
.LBB4_209:
	xor	edx, edx
.LBB4_1283:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1285
# %bb.1284:
	movsd	xmm0, qword ptr [rcx + 8*rdx]   # xmm0 = mem[0],zero
	xor	r9d, r9d
	pxor	xmm1, xmm1
	ucomisd	xmm1, xmm0
	andpd	xmm0, xmmword ptr [rip + .LCPI4_0]
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	orpd	xmm1, xmm0
	cvttsd2si	edi, xmm1
	cmove	edi, r9d
	mov	byte ptr [r8 + rdx], dil
	or	rdx, 1
.LBB4_1285:
	add	rsi, rax
	je	.LBB4_1655
# %bb.1286:
	xor	esi, esi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1287:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx], dil
	movsd	xmm3, qword ptr [rcx + 8*rdx + 8] # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx + 1], dil
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_1287
	jmp	.LBB4_1655
.LBB4_210:
	cmp	esi, 2
	je	.LBB4_440
# %bb.211:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.212:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.213:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB4_214
# %bb.633:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_635
# %bb.634:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_635
.LBB4_214:
	xor	esi, esi
.LBB4_1292:
	mov	rax, rsi
	not	rax
	test	r10b, 1
	je	.LBB4_1294
# %bb.1293:
	mov	dil, byte ptr [rcx + rsi]
	test	dil, dil
	setne	r9b
	neg	r9b
	test	dil, dil
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1294:
	add	rax, r10
	je	.LBB4_1655
# %bb.1295:
	mov	edi, 1
.LBB4_1296:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1296
	jmp	.LBB4_1655
.LBB4_215:
	cmp	esi, 2
	je	.LBB4_443
# %bb.216:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.217:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.218:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB4_219
# %bb.638:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_640
# %bb.639:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_640
.LBB4_219:
	xor	edx, edx
.LBB4_1301:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1303
.LBB4_1302:                             # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1302
.LBB4_1303:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1304:                             # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	qword ptr [rcx + 8*rdx + 8], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	qword ptr [rcx + 8*rdx + 16], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	qword ptr [rcx + 8*rdx + 24], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1304
	jmp	.LBB4_1655
.LBB4_220:
	cmp	esi, 2
	je	.LBB4_446
# %bb.221:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.222:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.223:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_224
# %bb.643:
	lea	rdx, [rcx + 2*rax]
	cmp	rdx, r8
	jbe	.LBB4_645
# %bb.644:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_645
.LBB4_224:
	xor	edx, edx
.LBB4_1309:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1311
.LBB4_1310:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1310
.LBB4_1311:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1312:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	word ptr [rcx + 2*rdx + 2], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	word ptr [rcx + 2*rdx + 4], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	word ptr [rcx + 2*rdx + 6], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1312
	jmp	.LBB4_1655
.LBB4_225:
	cmp	esi, 2
	je	.LBB4_449
# %bb.226:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.227:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.228:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_229
# %bb.648:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB4_650
# %bb.649:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_650
.LBB4_229:
	xor	esi, esi
.LBB4_1317:
	mov	rax, rsi
	not	rax
	test	r10b, 1
	je	.LBB4_1319
# %bb.1318:
	movzx	edi, word ptr [rcx + 2*rsi]
	test	di, di
	setne	r9b
	neg	r9b
	test	di, di
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1319:
	add	rax, r10
	je	.LBB4_1655
# %bb.1320:
	mov	r9d, 1
.LBB4_1321:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	test	di, di
	setne	al
	neg	al
	test	di, di
	movzx	eax, al
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi], al
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	test	ax, ax
	setne	dl
	neg	dl
	test	ax, ax
	movzx	eax, dl
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1321
	jmp	.LBB4_1655
.LBB4_230:
	cmp	esi, 2
	je	.LBB4_452
# %bb.231:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.232:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.233:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_234
# %bb.653:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB4_655
# %bb.654:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_655
.LBB4_234:
	xor	esi, esi
.LBB4_1326:
	mov	rdx, rsi
	not	rdx
	test	r10b, 1
	je	.LBB4_1328
# %bb.1327:
	mov	rdi, qword ptr [rcx + 8*rsi]
	test	rdi, rdi
	setne	al
	neg	al
	test	rdi, rdi
	movzx	eax, al
	mov	edi, 1
	cmovle	edi, eax
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1328:
	add	rdx, r10
	je	.LBB4_1655
# %bb.1329:
	mov	edi, 1
.LBB4_1330:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	test	rax, rax
	setne	dl
	neg	dl
	test	rax, rax
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi], al
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	test	rax, rax
	setne	dl
	neg	dl
	test	rax, rax
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1330
	jmp	.LBB4_1655
.LBB4_235:
	cmp	esi, 2
	je	.LBB4_455
# %bb.236:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.237:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.238:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_239
# %bb.658:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_660
# %bb.659:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_660
.LBB4_239:
	xor	edx, edx
.LBB4_1335:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1337
# %bb.1336:
	movd	xmm0, dword ptr [rcx + 4*rdx]   # xmm0 = mem[0],zero,zero,zero
	movd	edi, xmm0
	test	edi, edi
	setns	al
	add	al, al
	add	al, -1
	xor	edi, edi
	pxor	xmm1, xmm1
	ucomiss	xmm1, xmm0
	movzx	eax, al
	cmove	eax, edi
	mov	byte ptr [r8 + rdx], al
	or	rdx, 1
.LBB4_1337:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1338:
	xor	esi, esi
	xorps	xmm0, xmm0
.LBB4_1339:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	movd	eax, xmm1
	test	eax, eax
	setns	al
	add	al, al
	add	al, -1
	ucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx], al
	movd	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	movd	eax, xmm1
	test	eax, eax
	setns	al
	add	al, al
	add	al, -1
	ucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx + 1], al
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1339
	jmp	.LBB4_1655
.LBB4_240:
	cmp	esi, 2
	je	.LBB4_458
# %bb.241:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.242:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.243:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_244
# %bb.663:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_665
# %bb.664:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_665
.LBB4_244:
	xor	edx, edx
.LBB4_1344:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1346
.LBB4_1345:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1345
.LBB4_1346:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1347:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1347
	jmp	.LBB4_1655
.LBB4_245:
	cmp	esi, 2
	je	.LBB4_461
# %bb.246:
	cmp	esi, 3
	jne	.LBB4_1655
# %bb.247:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.248:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_249
# %bb.668:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_670
# %bb.669:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_670
.LBB4_249:
	xor	esi, esi
.LBB4_1352:
	mov	rax, rsi
	not	rax
	test	r10b, 1
	je	.LBB4_1354
# %bb.1353:
	mov	edi, dword ptr [rcx + 4*rsi]
	test	edi, edi
	setne	r9b
	neg	r9b
	test	edi, edi
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1354:
	add	rax, r10
	je	.LBB4_1655
# %bb.1355:
	mov	r9d, 1
.LBB4_1356:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	test	edi, edi
	setne	al
	neg	al
	test	edi, edi
	movzx	eax, al
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi], al
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	test	eax, eax
	setne	dl
	neg	dl
	test	eax, eax
	movzx	eax, dl
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1356
	jmp	.LBB4_1655
.LBB4_250:
	cmp	esi, 7
	je	.LBB4_464
# %bb.251:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.252:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.253:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_673
# %bb.254:
	xor	edx, edx
	jmp	.LBB4_1003
.LBB4_255:
	cmp	esi, 7
	je	.LBB4_467
# %bb.256:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.257:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.258:
	mov	r10d, r9d
	movabs	r11, -9223372036854775808
	cmp	r9d, 1
	jne	.LBB4_676
# %bb.259:
	xor	esi, esi
	jmp	.LBB4_1008
.LBB4_260:
	cmp	esi, 7
	je	.LBB4_470
# %bb.261:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.262:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.263:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_264
# %bb.679:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_681
# %bb.680:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB4_681
.LBB4_264:
	xor	edx, edx
.LBB4_1361:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1363
# %bb.1362:
	mov	al, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	rdi
	test	al, al
	mov	eax, 1
	cmovle	rax, rdi
	mov	qword ptr [r8 + 8*rdx], rax
	or	rdx, 1
.LBB4_1363:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1364:
	mov	esi, 1
.LBB4_1365:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	rdi
	test	al, al
	cmovg	rdi, rsi
	mov	qword ptr [r8 + 8*rdx], rdi
	movzx	eax, byte ptr [rcx + rdx + 1]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	rdi
	test	al, al
	cmovg	rdi, rsi
	mov	qword ptr [r8 + 8*rdx + 8], rdi
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1365
	jmp	.LBB4_1655
.LBB4_265:
	cmp	esi, 7
	je	.LBB4_473
# %bb.266:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.267:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.268:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_269
# %bb.684:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB4_686
# %bb.685:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB4_686
.LBB4_269:
	xor	edx, edx
.LBB4_1370:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1372
.LBB4_1371:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1371
.LBB4_1372:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1373:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx + 8], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 8], rax
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx + 16], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 16], rax
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx + 24], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 24], rax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1373
	jmp	.LBB4_1655
.LBB4_270:
	cmp	esi, 7
	je	.LBB4_476
# %bb.271:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.272:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.273:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_689
# %bb.274:
	xor	edx, edx
	jmp	.LBB4_1014
.LBB4_275:
	cmp	esi, 7
	je	.LBB4_479
# %bb.276:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.277:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.278:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_692
# %bb.279:
	xor	edx, edx
	jmp	.LBB4_1019
.LBB4_280:
	cmp	esi, 7
	je	.LBB4_482
# %bb.281:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.282:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.283:
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB4_284
# %bb.695:
	lea	rdx, [rcx + 8*r11]
	cmp	rdx, r8
	jbe	.LBB4_697
# %bb.696:
	lea	rdx, [r8 + 8*r11]
	cmp	rdx, rcx
	jbe	.LBB4_697
.LBB4_284:
	xor	edx, edx
.LBB4_1378:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1380
# %bb.1379:
	mov	r9, qword ptr [rcx + 8*rdx]
	xor	r10d, r10d
	test	r9, r9
	setne	r10b
	neg	r10
	test	r9, r9
	mov	edi, 1
	cmovle	rdi, r10
	mov	qword ptr [r8 + 8*rdx], rdi
	or	rdx, 1
.LBB4_1380:
	add	rsi, r11
	je	.LBB4_1655
# %bb.1381:
	mov	esi, 1
.LBB4_1382:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rdx]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	rax
	test	rdi, rdi
	cmovg	rax, rsi
	mov	qword ptr [r8 + 8*rdx], rax
	mov	rax, qword ptr [rcx + 8*rdx + 8]
	xor	edi, edi
	test	rax, rax
	setne	dil
	neg	rdi
	test	rax, rax
	cmovg	rdi, rsi
	mov	qword ptr [r8 + 8*rdx + 8], rdi
	add	rdx, 2
	cmp	r11, rdx
	jne	.LBB4_1382
	jmp	.LBB4_1655
.LBB4_285:
	cmp	esi, 7
	je	.LBB4_485
# %bb.286:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.287:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.288:
	mov	r10d, r9d
	cmp	r9d, 1
	jne	.LBB4_700
# %bb.289:
	xor	eax, eax
	jmp	.LBB4_290
.LBB4_293:
	cmp	esi, 7
	je	.LBB4_488
# %bb.294:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.295:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.296:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_297
# %bb.708:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_710
# %bb.709:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB4_710
.LBB4_297:
	xor	edx, edx
.LBB4_1387:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1389
.LBB4_1388:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1388
.LBB4_1389:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1390:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 8], rax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 16], rax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 24], rax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1390
	jmp	.LBB4_1655
.LBB4_298:
	cmp	esi, 7
	je	.LBB4_491
# %bb.299:
	cmp	esi, 8
	jne	.LBB4_1655
# %bb.300:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.301:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_713
# %bb.302:
	xor	edx, edx
	jmp	.LBB4_1025
.LBB4_303:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.304:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_716
# %bb.305:
	xor	edx, edx
	jmp	.LBB4_1141
.LBB4_306:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.307:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_719
# %bb.308:
	xor	edx, edx
	jmp	.LBB4_1146
.LBB4_309:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.310:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 4
	jae	.LBB4_722
# %bb.311:
	xor	esi, esi
	jmp	.LBB4_1151
.LBB4_312:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.313:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 4
	jae	.LBB4_725
# %bb.314:
	xor	esi, esi
	jmp	.LBB4_1157
.LBB4_315:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.316:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_317
# %bb.728:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_730
# %bb.729:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB4_730
.LBB4_317:
	xor	edx, edx
.LBB4_1395:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1397
# %bb.1396:
	mov	r9b, byte ptr [rcx + rdx]
	xor	edi, edi
	test	r9b, r9b
	setne	dil
	neg	edi
	test	r9b, r9b
	mov	eax, 1
	cmovle	eax, edi
	mov	word ptr [r8 + 2*rdx], ax
	or	rdx, 1
.LBB4_1397:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1398:
	mov	esi, 1
.LBB4_1399:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	word ptr [r8 + 2*rdx], di
	movzx	eax, byte ptr [rcx + rdx + 1]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	word ptr [r8 + 2*rdx + 2], di
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1399
	jmp	.LBB4_1655
.LBB4_318:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.319:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_320
# %bb.733:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_735
# %bb.734:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB4_735
.LBB4_320:
	xor	edx, edx
.LBB4_1404:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1406
# %bb.1405:
	mov	r9b, byte ptr [rcx + rdx]
	xor	edi, edi
	test	r9b, r9b
	setne	dil
	neg	edi
	test	r9b, r9b
	mov	eax, 1
	cmovle	eax, edi
	mov	word ptr [r8 + 2*rdx], ax
	or	rdx, 1
.LBB4_1406:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1407:
	mov	esi, 1
.LBB4_1408:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	word ptr [r8 + 2*rdx], di
	movzx	eax, byte ptr [rcx + rdx + 1]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	word ptr [r8 + 2*rdx + 2], di
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1408
	jmp	.LBB4_1655
.LBB4_321:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.322:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_738
# %bb.323:
	xor	edx, edx
	jmp	.LBB4_1031
.LBB4_324:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.325:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_741
# %bb.326:
	xor	edx, edx
	jmp	.LBB4_1036
.LBB4_327:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.328:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_329
# %bb.744:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB4_746
# %bb.745:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB4_746
.LBB4_329:
	xor	edx, edx
.LBB4_1413:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1415
.LBB4_1414:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1414
.LBB4_1415:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1416:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx + 2], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 2], ax
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx + 4], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 4], ax
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx + 6], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 6], ax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1416
	jmp	.LBB4_1655
.LBB4_330:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.331:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_332
# %bb.749:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB4_751
# %bb.750:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB4_751
.LBB4_332:
	xor	edx, edx
.LBB4_1421:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1423
.LBB4_1422:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1422
.LBB4_1423:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1424:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx + 2], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 2], ax
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx + 4], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 4], ax
	xor	eax, eax
	cmp	word ptr [rcx + 2*rdx + 6], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 6], ax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1424
	jmp	.LBB4_1655
.LBB4_333:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.334:
	mov	r11d, r9d
	cmp	r9d, 16
	jb	.LBB4_335
# %bb.754:
	lea	rdx, [rcx + 2*r11]
	cmp	rdx, r8
	jbe	.LBB4_756
# %bb.755:
	lea	rdx, [r8 + 2*r11]
	cmp	rdx, rcx
	jbe	.LBB4_756
.LBB4_335:
	xor	edx, edx
.LBB4_1429:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1431
# %bb.1430:
	movzx	r9d, word ptr [rcx + 2*rdx]
	xor	r10d, r10d
	test	r9w, r9w
	setne	r10b
	neg	r10d
	test	r9w, r9w
	mov	edi, 1
	cmovle	edi, r10d
	mov	word ptr [r8 + 2*rdx], di
	or	rdx, 1
.LBB4_1431:
	add	rsi, r11
	je	.LBB4_1655
# %bb.1432:
	mov	esi, 1
.LBB4_1433:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rdx]
	xor	eax, eax
	test	di, di
	setne	al
	neg	eax
	test	di, di
	cmovg	eax, esi
	mov	word ptr [r8 + 2*rdx], ax
	movzx	eax, word ptr [rcx + 2*rdx + 2]
	xor	edi, edi
	test	ax, ax
	setne	dil
	neg	edi
	test	ax, ax
	cmovg	edi, esi
	mov	word ptr [r8 + 2*rdx + 2], di
	add	rdx, 2
	cmp	r11, rdx
	jne	.LBB4_1433
	jmp	.LBB4_1655
.LBB4_336:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.337:
	mov	r11d, r9d
	cmp	r9d, 16
	jb	.LBB4_338
# %bb.759:
	lea	rdx, [rcx + 2*r11]
	cmp	rdx, r8
	jbe	.LBB4_761
# %bb.760:
	lea	rdx, [r8 + 2*r11]
	cmp	rdx, rcx
	jbe	.LBB4_761
.LBB4_338:
	xor	edx, edx
.LBB4_1438:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1440
# %bb.1439:
	movzx	r9d, word ptr [rcx + 2*rdx]
	xor	r10d, r10d
	test	r9w, r9w
	setne	r10b
	neg	r10d
	test	r9w, r9w
	mov	edi, 1
	cmovle	edi, r10d
	mov	word ptr [r8 + 2*rdx], di
	or	rdx, 1
.LBB4_1440:
	add	rsi, r11
	je	.LBB4_1655
# %bb.1441:
	mov	esi, 1
.LBB4_1442:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rdx]
	xor	eax, eax
	test	di, di
	setne	al
	neg	eax
	test	di, di
	cmovg	eax, esi
	mov	word ptr [r8 + 2*rdx], ax
	movzx	eax, word ptr [rcx + 2*rdx + 2]
	xor	edi, edi
	test	ax, ax
	setne	dil
	neg	edi
	test	ax, ax
	cmovg	edi, esi
	mov	word ptr [r8 + 2*rdx + 2], di
	add	rdx, 2
	cmp	r11, rdx
	jne	.LBB4_1442
	jmp	.LBB4_1655
.LBB4_339:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.340:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_764
# %bb.341:
	xor	edx, edx
	jmp	.LBB4_1041
.LBB4_342:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.343:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_767
# %bb.344:
	xor	edx, edx
	jmp	.LBB4_1163
.LBB4_345:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.346:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 8
	jae	.LBB4_770
# %bb.347:
	xor	esi, esi
	jmp	.LBB4_1169
.LBB4_348:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.349:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 8
	jae	.LBB4_773
# %bb.350:
	xor	esi, esi
	jmp	.LBB4_1175
.LBB4_351:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.352:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_353
# %bb.776:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_778
# %bb.777:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB4_778
.LBB4_353:
	xor	edx, edx
.LBB4_1447:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1449
.LBB4_1448:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1448
.LBB4_1449:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1450:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 2], ax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 4], ax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 6], ax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1450
	jmp	.LBB4_1655
.LBB4_354:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.355:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_356
# %bb.781:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_783
# %bb.782:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB4_783
.LBB4_356:
	xor	edx, edx
.LBB4_1455:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1457
.LBB4_1456:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1456
.LBB4_1457:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1458:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	word ptr [r8 + 2*rdx], ax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 2], ax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 4], ax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	al
	mov	word ptr [r8 + 2*rdx + 6], ax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1458
	jmp	.LBB4_1655
.LBB4_357:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.358:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB4_786
# %bb.359:
	xor	edx, edx
	jmp	.LBB4_1047
.LBB4_360:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.361:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB4_789
# %bb.362:
	xor	edx, edx
	jmp	.LBB4_1053
.LBB4_363:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.364:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_792
# %bb.365:
	xor	edx, edx
	jmp	.LBB4_1181
.LBB4_366:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.367:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_795
# %bb.368:
	xor	edx, edx
	jmp	.LBB4_1186
.LBB4_369:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.370:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_798
# %bb.371:
	xor	edx, edx
	jmp	.LBB4_1194
.LBB4_372:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.373:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_801
# %bb.374:
	xor	edx, edx
	jmp	.LBB4_1200
.LBB4_375:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.376:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_377
# %bb.804:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_806
# %bb.805:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB4_806
.LBB4_377:
	xor	edx, edx
.LBB4_1463:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1465
# %bb.1464:
	mov	al, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	rdi
	test	al, al
	mov	eax, 1
	cmovle	rax, rdi
	mov	qword ptr [r8 + 8*rdx], rax
	or	rdx, 1
.LBB4_1465:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1466:
	mov	esi, 1
.LBB4_1467:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	rdi
	test	al, al
	cmovg	rdi, rsi
	mov	qword ptr [r8 + 8*rdx], rdi
	movzx	eax, byte ptr [rcx + rdx + 1]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	rdi
	test	al, al
	cmovg	rdi, rsi
	mov	qword ptr [r8 + 8*rdx + 8], rdi
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1467
	jmp	.LBB4_1655
.LBB4_378:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.379:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB4_380
# %bb.809:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_811
# %bb.810:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_811
.LBB4_380:
	xor	edx, edx
.LBB4_1472:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1479
# %bb.1473:
	cmp	byte ptr [rcx + rdx], 0
	jne	.LBB4_1475
# %bb.1474:
	pxor	xmm0, xmm0
	jmp	.LBB4_1476
.LBB4_381:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.382:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_383
# %bb.814:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB4_816
# %bb.815:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB4_816
.LBB4_383:
	xor	edx, edx
.LBB4_1494:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1496
.LBB4_1495:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1495
.LBB4_1496:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1497:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx + 8], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 8], rax
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx + 16], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 16], rax
	xor	eax, eax
	cmp	qword ptr [rcx + 8*rdx + 24], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 24], rax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1497
	jmp	.LBB4_1655
.LBB4_384:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.385:
	mov	edx, r9d
	lea	rsi, [rdx - 1]
	mov	eax, edx
	and	eax, 3
	cmp	rsi, 3
	jae	.LBB4_819
# %bb.386:
	xor	esi, esi
.LBB4_387:
	test	rax, rax
	je	.LBB4_1655
# %bb.388:
	lea	rdx, [r8 + 4*rsi]
	lea	rcx, [rcx + 8*rsi]
	xor	esi, esi
	movss	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_390
.LBB4_389:                              #   in Loop: Header=BB4_390 Depth=1
	movss	dword ptr [rdx + 4*rsi], xmm1
	add	rsi, 1
	cmp	rax, rsi
	je	.LBB4_1655
.LBB4_390:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_389
# %bb.391:                              #   in Loop: Header=BB4_390 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_389
.LBB4_392:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.393:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_829
# %bb.394:
	xor	edx, edx
	jmp	.LBB4_1059
.LBB4_395:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.396:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_832
# %bb.397:
	xor	edx, edx
	jmp	.LBB4_1208
.LBB4_398:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.399:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_835
# %bb.400:
	xor	edx, edx
	jmp	.LBB4_1216
.LBB4_401:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.402:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_838
# %bb.403:
	xor	edx, edx
	jmp	.LBB4_1222
.LBB4_404:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.405:
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB4_406
# %bb.841:
	lea	rdx, [rcx + 8*r11]
	cmp	rdx, r8
	jbe	.LBB4_843
# %bb.842:
	lea	rdx, [r8 + 8*r11]
	cmp	rdx, rcx
	jbe	.LBB4_843
.LBB4_406:
	xor	edx, edx
.LBB4_1502:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1504
# %bb.1503:
	mov	r9, qword ptr [rcx + 8*rdx]
	xor	r10d, r10d
	test	r9, r9
	setne	r10b
	neg	r10
	test	r9, r9
	mov	edi, 1
	cmovle	rdi, r10
	mov	qword ptr [r8 + 8*rdx], rdi
	or	rdx, 1
.LBB4_1504:
	add	rsi, r11
	je	.LBB4_1655
# %bb.1505:
	mov	esi, 1
.LBB4_1506:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rdx]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	rax
	test	rdi, rdi
	cmovg	rax, rsi
	mov	qword ptr [r8 + 8*rdx], rax
	mov	rax, qword ptr [rcx + 8*rdx + 8]
	xor	edi, edi
	test	rax, rax
	setne	dil
	neg	rdi
	test	rax, rax
	cmovg	rdi, rsi
	mov	qword ptr [r8 + 8*rdx + 8], rdi
	add	rdx, 2
	cmp	r11, rdx
	jne	.LBB4_1506
	jmp	.LBB4_1655
.LBB4_407:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.408:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_846
# %bb.409:
	xor	eax, eax
.LBB4_410:
	test	dl, 1
	je	.LBB4_1655
# %bb.411:
	cmp	qword ptr [rcx + 8*rax], 0
	jne	.LBB4_989
# %bb.412:
	xorpd	xmm0, xmm0
	jmp	.LBB4_990
.LBB4_413:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.414:
	mov	edx, r9d
	cmp	r9d, 1
	jne	.LBB4_856
# %bb.415:
	xor	eax, eax
	jmp	.LBB4_416
.LBB4_419:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.420:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB4_421
# %bb.864:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_866
# %bb.865:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_866
.LBB4_421:
	xor	edx, edx
.LBB4_869:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_871
# %bb.870:
	movss	xmm0, dword ptr [rcx + 4*rdx]   # xmm0 = mem[0],zero,zero,zero
	movmskps	edi, xmm0
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, edi
	xorps	xmm2, xmm2
	cmpeqss	xmm2, xmm0
	andnps	xmm2, xmm1
	movss	dword ptr [r8 + 4*rdx], xmm2
	or	rdx, 1
.LBB4_871:
	add	rsi, rax
	je	.LBB4_1655
# %bb.872:
	xorps	xmm0, xmm0
.LBB4_873:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	movmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, esi
	cmpeqss	xmm1, xmm0
	andnps	xmm1, xmm2
	movss	dword ptr [r8 + 4*rdx], xmm1
	movss	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	movmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, esi
	cmpeqss	xmm1, xmm0
	andnps	xmm1, xmm2
	movss	dword ptr [r8 + 4*rdx + 4], xmm1
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_873
	jmp	.LBB4_1655
.LBB4_422:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.423:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_424
# %bb.874:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_876
# %bb.875:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB4_876
.LBB4_424:
	xor	edx, edx
.LBB4_1511:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1513
.LBB4_1512:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1512
.LBB4_1513:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1514:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx], rax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 8], rax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 16], rax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	al
	mov	qword ptr [r8 + 8*rdx + 24], rax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1514
	jmp	.LBB4_1655
.LBB4_425:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.426:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB4_427
# %bb.879:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_881
# %bb.880:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_881
.LBB4_427:
	xor	edx, edx
.LBB4_1519:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1524
# %bb.1520:
	movd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_1522
.LBB4_1521:                             #   in Loop: Header=BB4_1522 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	je	.LBB4_1524
.LBB4_1522:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1521
# %bb.1523:                             #   in Loop: Header=BB4_1522 Depth=1
	pxor	xmm1, xmm1
	jmp	.LBB4_1521
.LBB4_428:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.429:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_884
# %bb.430:
	xor	edx, edx
	jmp	.LBB4_1064
.LBB4_431:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.432:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_887
# %bb.433:
	xor	edx, edx
	jmp	.LBB4_1070
.LBB4_434:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.435:
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB4_436
# %bb.890:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_892
# %bb.891:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_892
.LBB4_436:
	xor	edx, edx
.LBB4_1539:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1541
.LBB4_1540:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1540
.LBB4_1541:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1542:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	dword ptr [rcx + 4*rdx + 4], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	dword ptr [rcx + 4*rdx + 8], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	dword ptr [rcx + 4*rdx + 12], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1542
	jmp	.LBB4_1655
.LBB4_437:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.438:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB4_439
# %bb.895:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_897
# %bb.896:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_897
.LBB4_439:
	xor	edx, edx
.LBB4_1547:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1549
# %bb.1548:
	movsd	xmm0, qword ptr [rcx + 8*rdx]   # xmm0 = mem[0],zero
	xor	r9d, r9d
	pxor	xmm1, xmm1
	ucomisd	xmm1, xmm0
	andpd	xmm0, xmmword ptr [rip + .LCPI4_0]
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	orpd	xmm1, xmm0
	cvttsd2si	edi, xmm1
	cmove	edi, r9d
	mov	byte ptr [r8 + rdx], dil
	or	rdx, 1
.LBB4_1549:
	add	rsi, rax
	je	.LBB4_1655
# %bb.1550:
	xor	esi, esi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1551:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx], dil
	movsd	xmm3, qword ptr [rcx + 8*rdx + 8] # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx + 1], dil
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_1551
	jmp	.LBB4_1655
.LBB4_440:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.441:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB4_442
# %bb.900:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_902
# %bb.901:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_902
.LBB4_442:
	xor	esi, esi
.LBB4_1556:
	mov	rax, rsi
	not	rax
	test	r10b, 1
	je	.LBB4_1558
# %bb.1557:
	mov	dil, byte ptr [rcx + rsi]
	test	dil, dil
	setne	r9b
	neg	r9b
	test	dil, dil
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1558:
	add	rax, r10
	je	.LBB4_1655
# %bb.1559:
	mov	edi, 1
.LBB4_1560:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1560
	jmp	.LBB4_1655
.LBB4_443:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.444:
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB4_445
# %bb.905:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_907
# %bb.906:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_907
.LBB4_445:
	xor	edx, edx
.LBB4_1565:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1567
.LBB4_1566:                             # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1566
.LBB4_1567:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1568:                             # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	qword ptr [rcx + 8*rdx + 8], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	qword ptr [rcx + 8*rdx + 16], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	qword ptr [rcx + 8*rdx + 24], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1568
	jmp	.LBB4_1655
.LBB4_446:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.447:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_448
# %bb.910:
	lea	rdx, [rcx + 2*rax]
	cmp	rdx, r8
	jbe	.LBB4_912
# %bb.911:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_912
.LBB4_448:
	xor	edx, edx
.LBB4_1573:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1575
.LBB4_1574:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1574
.LBB4_1575:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1576:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	word ptr [rcx + 2*rdx + 2], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	word ptr [rcx + 2*rdx + 4], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	word ptr [rcx + 2*rdx + 6], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1576
	jmp	.LBB4_1655
.LBB4_449:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.450:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_451
# %bb.915:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB4_917
# %bb.916:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_917
.LBB4_451:
	xor	esi, esi
.LBB4_1581:
	mov	rax, rsi
	not	rax
	test	r10b, 1
	je	.LBB4_1583
# %bb.1582:
	movzx	edi, word ptr [rcx + 2*rsi]
	test	di, di
	setne	r9b
	neg	r9b
	test	di, di
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1583:
	add	rax, r10
	je	.LBB4_1655
# %bb.1584:
	mov	r9d, 1
.LBB4_1585:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	test	di, di
	setne	al
	neg	al
	test	di, di
	movzx	eax, al
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi], al
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	test	ax, ax
	setne	dl
	neg	dl
	test	ax, ax
	movzx	eax, dl
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1585
	jmp	.LBB4_1655
.LBB4_452:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.453:
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB4_454
# %bb.920:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB4_922
# %bb.921:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_922
.LBB4_454:
	xor	esi, esi
.LBB4_1590:
	mov	rdx, rsi
	not	rdx
	test	r10b, 1
	je	.LBB4_1592
# %bb.1591:
	mov	rdi, qword ptr [rcx + 8*rsi]
	test	rdi, rdi
	setne	al
	neg	al
	test	rdi, rdi
	movzx	eax, al
	mov	edi, 1
	cmovle	edi, eax
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1592:
	add	rdx, r10
	je	.LBB4_1655
# %bb.1593:
	mov	edi, 1
.LBB4_1594:                             # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	test	rax, rax
	setne	dl
	neg	dl
	test	rax, rax
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi], al
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	test	rax, rax
	setne	dl
	neg	dl
	test	rax, rax
	movzx	eax, dl
	cmovg	eax, edi
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1594
	jmp	.LBB4_1655
.LBB4_455:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.456:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_457
# %bb.925:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_927
# %bb.926:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_927
.LBB4_457:
	xor	edx, edx
.LBB4_1599:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1601
# %bb.1600:
	movd	xmm0, dword ptr [rcx + 4*rdx]   # xmm0 = mem[0],zero,zero,zero
	movd	edi, xmm0
	test	edi, edi
	setns	al
	add	al, al
	add	al, -1
	xor	edi, edi
	pxor	xmm1, xmm1
	ucomiss	xmm1, xmm0
	movzx	eax, al
	cmove	eax, edi
	mov	byte ptr [r8 + rdx], al
	or	rdx, 1
.LBB4_1601:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1602:
	xor	esi, esi
	xorps	xmm0, xmm0
.LBB4_1603:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	movd	eax, xmm1
	test	eax, eax
	setns	al
	add	al, al
	add	al, -1
	ucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx], al
	movd	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	movd	eax, xmm1
	test	eax, eax
	setns	al
	add	al, al
	add	al, -1
	ucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx + 1], al
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1603
	jmp	.LBB4_1655
.LBB4_458:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.459:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_460
# %bb.930:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_932
# %bb.931:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_932
.LBB4_460:
	xor	edx, edx
.LBB4_1608:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1610
.LBB4_1609:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1609
.LBB4_1610:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1611:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	setne	byte ptr [r8 + rdx]
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	byte ptr [r8 + rdx + 1]
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	byte ptr [r8 + rdx + 2]
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	byte ptr [r8 + rdx + 3]
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1611
	jmp	.LBB4_1655
.LBB4_461:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.462:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_463
# %bb.935:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_937
# %bb.936:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_937
.LBB4_463:
	xor	esi, esi
.LBB4_1616:
	mov	rax, rsi
	not	rax
	test	r10b, 1
	je	.LBB4_1618
# %bb.1617:
	mov	edi, dword ptr [rcx + 4*rsi]
	test	edi, edi
	setne	r9b
	neg	r9b
	test	edi, edi
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + rsi], dil
	or	rsi, 1
.LBB4_1618:
	add	rax, r10
	je	.LBB4_1655
# %bb.1619:
	mov	r9d, 1
.LBB4_1620:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	test	edi, edi
	setne	al
	neg	al
	test	edi, edi
	movzx	eax, al
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi], al
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	test	eax, eax
	setne	dl
	neg	dl
	test	eax, eax
	movzx	eax, dl
	cmovg	eax, r9d
	mov	byte ptr [r8 + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB4_1620
	jmp	.LBB4_1655
.LBB4_464:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.465:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_466
# %bb.940:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_942
# %bb.941:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB4_942
.LBB4_466:
	xor	edx, edx
.LBB4_1625:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1627
.LBB4_1626:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1626
.LBB4_1627:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1628:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx + 4], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 4], eax
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx + 8], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 8], eax
	xor	eax, eax
	cmp	dword ptr [rcx + 4*rdx + 12], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 12], eax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1628
	jmp	.LBB4_1655
.LBB4_467:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.468:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 4
	jae	.LBB4_945
# %bb.469:
	xor	esi, esi
	jmp	.LBB4_1080
.LBB4_470:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.471:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_472
# %bb.948:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_950
# %bb.949:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB4_950
.LBB4_472:
	xor	edx, edx
.LBB4_1633:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1635
# %bb.1634:
	mov	r9b, byte ptr [rcx + rdx]
	xor	edi, edi
	test	r9b, r9b
	setne	dil
	neg	edi
	test	r9b, r9b
	mov	eax, 1
	cmovle	eax, edi
	mov	dword ptr [r8 + 4*rdx], eax
	or	rdx, 1
.LBB4_1635:
	add	rsi, r10
	je	.LBB4_1655
# %bb.1636:
	mov	esi, 1
.LBB4_1637:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdx]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	dword ptr [r8 + 4*rdx], edi
	movzx	eax, byte ptr [rcx + rdx + 1]
	xor	edi, edi
	test	al, al
	setne	dil
	neg	edi
	test	al, al
	cmovg	edi, esi
	mov	dword ptr [r8 + 4*rdx + 4], edi
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1637
	jmp	.LBB4_1655
.LBB4_473:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.474:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_953
# %bb.475:
	xor	edx, edx
	jmp	.LBB4_1086
.LBB4_476:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.477:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_956
# %bb.478:
	xor	edx, edx
	jmp	.LBB4_1091
.LBB4_479:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.480:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB4_959
# %bb.481:
	xor	edx, edx
	jmp	.LBB4_1096
.LBB4_482:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.483:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_962
# %bb.484:
	xor	edx, edx
	jmp	.LBB4_1102
.LBB4_485:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.486:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_965
# %bb.487:
	xor	edx, edx
	jmp	.LBB4_968
.LBB4_488:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.489:
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB4_490
# %bb.972:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_974
# %bb.973:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB4_974
.LBB4_490:
	xor	edx, edx
.LBB4_1642:
	mov	rsi, rdx
	not	rsi
	add	rsi, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB4_1644
.LBB4_1643:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1643
.LBB4_1644:
	cmp	rsi, 3
	jb	.LBB4_1655
.LBB4_1645:                             # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	cmp	byte ptr [rcx + rdx], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx], eax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 4], eax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 8], eax
	xor	eax, eax
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	al
	mov	dword ptr [r8 + 4*rdx + 12], eax
	add	rdx, 4
	cmp	r10, rdx
	jne	.LBB4_1645
	jmp	.LBB4_1655
.LBB4_491:
	test	r9d, r9d
	jle	.LBB4_1655
# %bb.492:
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB4_493
# %bb.977:
	lea	rdx, [rcx + 4*r11]
	cmp	rdx, r8
	jbe	.LBB4_979
# %bb.978:
	lea	rdx, [r8 + 4*r11]
	cmp	rdx, rcx
	jbe	.LBB4_979
.LBB4_493:
	xor	edx, edx
.LBB4_1650:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1652
# %bb.1651:
	mov	r9d, dword ptr [rcx + 4*rdx]
	xor	r10d, r10d
	test	r9d, r9d
	setne	r10b
	neg	r10d
	test	r9d, r9d
	mov	edi, 1
	cmovle	edi, r10d
	mov	dword ptr [r8 + 4*rdx], edi
	or	rdx, 1
.LBB4_1652:
	add	rsi, r11
	je	.LBB4_1655
# %bb.1653:
	mov	esi, 1
.LBB4_1654:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rdx]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	eax
	test	edi, edi
	cmovg	eax, esi
	mov	dword ptr [r8 + 4*rdx], eax
	mov	eax, dword ptr [rcx + 4*rdx + 4]
	xor	edi, edi
	test	eax, eax
	setne	dil
	neg	edi
	test	eax, eax
	cmovg	edi, esi
	mov	dword ptr [r8 + 4*rdx + 4], edi
	add	rdx, 2
	cmp	r11, rdx
	jne	.LBB4_1654
	jmp	.LBB4_1655
.LBB4_1524:
	cmp	rsi, 3
	jb	.LBB4_1655
# %bb.1525:
	movd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_1527
.LBB4_1526:                             #   in Loop: Header=BB4_1527 Depth=1
	movd	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1527:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1528
# %bb.1531:                             #   in Loop: Header=BB4_1527 Depth=1
	pxor	xmm1, xmm1
	movd	dword ptr [r8 + 4*rdx], xmm1
	cmp	byte ptr [rcx + rdx + 1], 0
	movdqa	xmm1, xmm0
	je	.LBB4_1532
.LBB4_1529:                             #   in Loop: Header=BB4_1527 Depth=1
	movd	dword ptr [r8 + 4*rdx + 4], xmm1
	cmp	byte ptr [rcx + rdx + 2], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1530
.LBB4_1533:                             #   in Loop: Header=BB4_1527 Depth=1
	pxor	xmm1, xmm1
	movd	dword ptr [r8 + 4*rdx + 8], xmm1
	cmp	byte ptr [rcx + rdx + 3], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1526
	jmp	.LBB4_1534
.LBB4_1528:                             #   in Loop: Header=BB4_1527 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm1
	cmp	byte ptr [rcx + rdx + 1], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1529
.LBB4_1532:                             #   in Loop: Header=BB4_1527 Depth=1
	pxor	xmm1, xmm1
	movd	dword ptr [r8 + 4*rdx + 4], xmm1
	cmp	byte ptr [rcx + rdx + 2], 0
	movdqa	xmm1, xmm0
	je	.LBB4_1533
.LBB4_1530:                             #   in Loop: Header=BB4_1527 Depth=1
	movd	dword ptr [r8 + 4*rdx + 8], xmm1
	cmp	byte ptr [rcx + rdx + 3], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1526
.LBB4_1534:                             #   in Loop: Header=BB4_1527 Depth=1
	pxor	xmm1, xmm1
	jmp	.LBB4_1526
.LBB4_499:
	mov	esi, r11d
	and	esi, -4
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1106
# %bb.500:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movapd	xmm2, xmmword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
.LBB4_501:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm5, xmmword ptr [rcx + 8*rdi]
	movupd	xmm6, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm3, xmm5
	cmpeqpd	xmm3, xmm0
	shufps	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	movapd	xmm4, xmm6
	cmpeqpd	xmm4, xmm0
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	pshufd	xmm7, xmm5, 238                 # xmm7 = xmm5[2,3,2,3]
	cvttsd2si	rax, xmm7
	cvttsd2si	rbx, xmm5
	movd	xmm5, ebx
	pinsrd	xmm5, eax, 1
	pshufd	xmm7, xmm6, 238                 # xmm7 = xmm6[2,3,2,3]
	cvttsd2si	rax, xmm7
	cvttsd2si	rbx, xmm6
	shufps	xmm4, xmm4, 232                 # xmm4 = xmm4[0,2,2,3]
	movd	xmm6, ebx
	pinsrd	xmm6, eax, 1
	andnps	xmm3, xmm5
	andnps	xmm4, xmm6
	movlhps	xmm3, xmm4                      # xmm3 = xmm3[0],xmm4[0]
	movups	xmmword ptr [r8 + 4*rdi], xmm3
	movupd	xmm5, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm6, xmmword ptr [rcx + 8*rdi + 48]
	movapd	xmm3, xmm5
	cmpeqpd	xmm3, xmm0
	shufps	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	movapd	xmm4, xmm6
	cmpeqpd	xmm4, xmm0
	shufps	xmm4, xmm4, 232                 # xmm4 = xmm4[0,2,2,3]
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	andpd	xmm6, xmm1
	pshufd	xmm7, xmm5, 238                 # xmm7 = xmm5[2,3,2,3]
	cvttsd2si	rax, xmm7
	orpd	xmm6, xmm2
	cvttsd2si	rbx, xmm5
	movd	xmm5, ebx
	pinsrd	xmm5, eax, 1
	andnps	xmm3, xmm5
	pshufd	xmm5, xmm6, 238                 # xmm5 = xmm6[2,3,2,3]
	cvttsd2si	rax, xmm5
	cvttsd2si	rbx, xmm6
	movd	xmm5, ebx
	pinsrd	xmm5, eax, 1
	andnps	xmm4, xmm5
	movlhps	xmm3, xmm4                      # xmm3 = xmm3[0],xmm4[0]
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
	add	rdi, 8
	add	rdx, 2
	jne	.LBB4_501
	jmp	.LBB4_1107
.LBB4_507:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_994
# %bb.508:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_16] # xmm1 = <1,1,u,u>
.LBB4_509:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm2, xmm0
	pshufd	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pshufd	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	pandn	xmm3, xmm1
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + 4*rsi], xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm2, xmm0
	pshufd	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pshufd	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	pandn	xmm3, xmm1
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_509
	jmp	.LBB4_995
.LBB4_510:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1112
# %bb.511:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_512:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + 2*rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 2*rsi + 24] # xmm4 = mem[0],zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_512
	jmp	.LBB4_1113
.LBB4_513:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1117
# %bb.514:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
.LBB4_515:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + 2*rsi]   # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 2*rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwd	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwd	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_515
	jmp	.LBB4_1118
.LBB4_516:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1123
# %bb.517:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_16] # xmm4 = <1,1,u,u>
.LBB4_518:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pcmpeqq	xmm5, xmm2
	pshufd	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3]
	pxor	xmm5, xmm3
	pcmpeqq	xmm6, xmm2
	pshufd	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	pxor	xmm6, xmm3
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movlhps	xmm5, xmm6                      # xmm5 = xmm5[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pcmpeqq	xmm5, xmm2
	pshufd	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3]
	pxor	xmm5, xmm3
	pcmpeqq	xmm6, xmm2
	pshufd	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	pxor	xmm6, xmm3
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movlhps	xmm5, xmm6                      # xmm5 = xmm5[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm5
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_518
	jmp	.LBB4_1124
.LBB4_519:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1129
# %bb.520:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorps	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
	movaps	xmm3, xmmword ptr [rip + .LCPI4_10] # xmm3 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	movaps	xmm4, xmmword ptr [rip + .LCPI4_4] # xmm4 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB4_521:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi]
	movdqa	xmm0, xmm5
	psrad	xmm0, 31
	por	xmm0, xmm2
	cvtdq2ps	xmm6, xmm0
	movaps	xmm0, xmm6
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm7, xmm6
	subps	xmm6, xmm3
	cvttps2dq	xmm6, xmm6
	xorps	xmm6, xmm4
	blendvps	xmm6, xmm7, xmm0
	cmpneqps	xmm5, xmm1
	andps	xmm5, xmm6
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm0, xmm5
	psrad	xmm0, 31
	por	xmm0, xmm2
	cvtdq2ps	xmm6, xmm0
	movaps	xmm0, xmm6
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm7, xmm6
	subps	xmm6, xmm3
	cvttps2dq	xmm6, xmm6
	xorps	xmm6, xmm4
	blendvps	xmm6, xmm7, xmm0
	cmpneqps	xmm5, xmm1
	andps	xmm5, xmm6
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm5
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_521
	jmp	.LBB4_1130
.LBB4_532:
	and	edx, -4
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_534
.LBB4_533:                              #   in Loop: Header=BB4_534 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	rdx, rsi
	je	.LBB4_101
.LBB4_534:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_535
# %bb.538:                              #   in Loop: Header=BB4_534 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	dword ptr [rcx + 4*rsi + 4], 0
	movapd	xmm1, xmm0
	je	.LBB4_539
.LBB4_536:                              #   in Loop: Header=BB4_534 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	dword ptr [rcx + 4*rsi + 8], 0
	movapd	xmm1, xmm0
	jne	.LBB4_537
.LBB4_540:                              #   in Loop: Header=BB4_534 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	dword ptr [rcx + 4*rsi + 12], 0
	movapd	xmm1, xmm0
	jne	.LBB4_533
	jmp	.LBB4_541
.LBB4_535:                              #   in Loop: Header=BB4_534 Depth=1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	dword ptr [rcx + 4*rsi + 4], 0
	movapd	xmm1, xmm0
	jne	.LBB4_536
.LBB4_539:                              #   in Loop: Header=BB4_534 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	dword ptr [rcx + 4*rsi + 8], 0
	movapd	xmm1, xmm0
	je	.LBB4_540
.LBB4_537:                              #   in Loop: Header=BB4_534 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	dword ptr [rcx + 4*rsi + 12], 0
	movapd	xmm1, xmm0
	jne	.LBB4_533
.LBB4_541:                              #   in Loop: Header=BB4_534 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_533
.LBB4_547:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	movsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	jmp	.LBB4_549
.LBB4_548:                              #   in Loop: Header=BB4_549 Depth=1
	movsd	qword ptr [r8 + 8*rax + 8], xmm3
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_120
.LBB4_549:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rax], 0
	movapd	xmm2, xmm0
	jne	.LBB4_550
# %bb.553:                              #   in Loop: Header=BB4_549 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jle	.LBB4_554
.LBB4_551:                              #   in Loop: Header=BB4_549 Depth=1
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	byte ptr [rcx + rax + 1], 0
	movapd	xmm2, xmm0
	jne	.LBB4_552
.LBB4_555:                              #   in Loop: Header=BB4_549 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jg	.LBB4_548
	jmp	.LBB4_556
.LBB4_550:                              #   in Loop: Header=BB4_549 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_551
.LBB4_554:                              #   in Loop: Header=BB4_549 Depth=1
	movapd	xmm3, xmm2
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	byte ptr [rcx + rax + 1], 0
	movapd	xmm2, xmm0
	je	.LBB4_555
.LBB4_552:                              #   in Loop: Header=BB4_549 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_548
.LBB4_556:                              #   in Loop: Header=BB4_549 Depth=1
	movapd	xmm3, xmm2
	jmp	.LBB4_548
.LBB4_557:
	and	edx, -4
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_559
.LBB4_558:                              #   in Loop: Header=BB4_559 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	rdx, rsi
	je	.LBB4_130
.LBB4_559:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_560
# %bb.563:                              #   in Loop: Header=BB4_559 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	qword ptr [rcx + 8*rsi + 8], 0
	movapd	xmm1, xmm0
	je	.LBB4_564
.LBB4_561:                              #   in Loop: Header=BB4_559 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	qword ptr [rcx + 8*rsi + 16], 0
	movapd	xmm1, xmm0
	jne	.LBB4_562
.LBB4_565:                              #   in Loop: Header=BB4_559 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	qword ptr [rcx + 8*rsi + 24], 0
	movapd	xmm1, xmm0
	jne	.LBB4_558
	jmp	.LBB4_566
.LBB4_560:                              #   in Loop: Header=BB4_559 Depth=1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	qword ptr [rcx + 8*rsi + 8], 0
	movapd	xmm1, xmm0
	jne	.LBB4_561
.LBB4_564:                              #   in Loop: Header=BB4_559 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	qword ptr [rcx + 8*rsi + 16], 0
	movapd	xmm1, xmm0
	je	.LBB4_565
.LBB4_562:                              #   in Loop: Header=BB4_559 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	qword ptr [rcx + 8*rsi + 24], 0
	movapd	xmm1, xmm0
	jne	.LBB4_558
.LBB4_566:                              #   in Loop: Header=BB4_559 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_558
.LBB4_567:
	and	edx, -4
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_569
.LBB4_568:                              #   in Loop: Header=BB4_569 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	rdx, rsi
	je	.LBB4_142
.LBB4_569:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_570
# %bb.573:                              #   in Loop: Header=BB4_569 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	word ptr [rcx + 2*rsi + 2], 0
	movapd	xmm1, xmm0
	je	.LBB4_574
.LBB4_571:                              #   in Loop: Header=BB4_569 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	word ptr [rcx + 2*rsi + 4], 0
	movapd	xmm1, xmm0
	jne	.LBB4_572
.LBB4_575:                              #   in Loop: Header=BB4_569 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	word ptr [rcx + 2*rsi + 6], 0
	movapd	xmm1, xmm0
	jne	.LBB4_568
	jmp	.LBB4_576
.LBB4_570:                              #   in Loop: Header=BB4_569 Depth=1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	word ptr [rcx + 2*rsi + 2], 0
	movapd	xmm1, xmm0
	jne	.LBB4_571
.LBB4_574:                              #   in Loop: Header=BB4_569 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	word ptr [rcx + 2*rsi + 4], 0
	movapd	xmm1, xmm0
	je	.LBB4_575
.LBB4_572:                              #   in Loop: Header=BB4_569 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	word ptr [rcx + 2*rsi + 6], 0
	movapd	xmm1, xmm0
	jne	.LBB4_568
.LBB4_576:                              #   in Loop: Header=BB4_569 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_568
.LBB4_577:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	movsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	jmp	.LBB4_579
.LBB4_578:                              #   in Loop: Header=BB4_579 Depth=1
	movsd	qword ptr [r8 + 8*rax + 8], xmm3
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_154
.LBB4_579:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rax], 0
	movapd	xmm2, xmm0
	jne	.LBB4_580
# %bb.583:                              #   in Loop: Header=BB4_579 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jle	.LBB4_584
.LBB4_581:                              #   in Loop: Header=BB4_579 Depth=1
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	word ptr [rcx + 2*rax + 2], 0
	movapd	xmm2, xmm0
	jne	.LBB4_582
.LBB4_585:                              #   in Loop: Header=BB4_579 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jg	.LBB4_578
	jmp	.LBB4_586
.LBB4_580:                              #   in Loop: Header=BB4_579 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_581
.LBB4_584:                              #   in Loop: Header=BB4_579 Depth=1
	movapd	xmm3, xmm2
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	word ptr [rcx + 2*rax + 2], 0
	movapd	xmm2, xmm0
	je	.LBB4_585
.LBB4_582:                              #   in Loop: Header=BB4_579 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_578
.LBB4_586:                              #   in Loop: Header=BB4_579 Depth=1
	movapd	xmm3, xmm2
	jmp	.LBB4_578
.LBB4_587:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	movsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	jmp	.LBB4_589
.LBB4_588:                              #   in Loop: Header=BB4_589 Depth=1
	movsd	qword ptr [r8 + 8*rax + 8], xmm3
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_164
.LBB4_589:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rax], 0
	movapd	xmm2, xmm0
	jne	.LBB4_590
# %bb.593:                              #   in Loop: Header=BB4_589 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jle	.LBB4_594
.LBB4_591:                              #   in Loop: Header=BB4_589 Depth=1
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	qword ptr [rcx + 8*rax + 8], 0
	movapd	xmm2, xmm0
	jne	.LBB4_592
.LBB4_595:                              #   in Loop: Header=BB4_589 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jg	.LBB4_588
	jmp	.LBB4_596
.LBB4_590:                              #   in Loop: Header=BB4_589 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_591
.LBB4_594:                              #   in Loop: Header=BB4_589 Depth=1
	movapd	xmm3, xmm2
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	qword ptr [rcx + 8*rax + 8], 0
	movapd	xmm2, xmm0
	je	.LBB4_595
.LBB4_592:                              #   in Loop: Header=BB4_589 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_588
.LBB4_596:                              #   in Loop: Header=BB4_589 Depth=1
	movapd	xmm3, xmm2
	jmp	.LBB4_588
.LBB4_597:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	xorps	xmm0, xmm0
	jmp	.LBB4_599
.LBB4_598:                              #   in Loop: Header=BB4_599 Depth=1
	movsd	qword ptr [r8 + 8*rax + 8], xmm1
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_174
.LBB4_599:                              # =>This Inner Loop Header: Depth=1
	movss	xmm2, dword ptr [rcx + 4*rax]   # xmm2 = mem[0],zero,zero,zero
	xorpd	xmm1, xmm1
	ucomiss	xmm0, xmm2
	xorpd	xmm3, xmm3
	je	.LBB4_601
# %bb.600:                              #   in Loop: Header=BB4_599 Depth=1
	movmskps	edi, xmm2
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, edi
	xorps	xmm3, xmm3
	cvtss2sd	xmm3, xmm2
.LBB4_601:                              #   in Loop: Header=BB4_599 Depth=1
	movsd	qword ptr [r8 + 8*rax], xmm3
	movss	xmm2, dword ptr [rcx + 4*rax + 4] # xmm2 = mem[0],zero,zero,zero
	ucomiss	xmm0, xmm2
	je	.LBB4_598
# %bb.602:                              #   in Loop: Header=BB4_599 Depth=1
	movmskps	edi, xmm2
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, edi
	cvtss2sd	xmm1, xmm1
	jmp	.LBB4_598
.LBB4_603:
	and	edx, -4
	xor	esi, esi
	movsd	xmm0, qword ptr [rip + .LCPI4_2] # xmm0 = mem[0],zero
	jmp	.LBB4_605
.LBB4_604:                              #   in Loop: Header=BB4_605 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	rdx, rsi
	je	.LBB4_185
.LBB4_605:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_606
# %bb.609:                              #   in Loop: Header=BB4_605 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	byte ptr [rcx + rsi + 1], 0
	movapd	xmm1, xmm0
	je	.LBB4_610
.LBB4_607:                              #   in Loop: Header=BB4_605 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	byte ptr [rcx + rsi + 2], 0
	movapd	xmm1, xmm0
	jne	.LBB4_608
.LBB4_611:                              #   in Loop: Header=BB4_605 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	byte ptr [rcx + rsi + 3], 0
	movapd	xmm1, xmm0
	jne	.LBB4_604
	jmp	.LBB4_612
.LBB4_606:                              #   in Loop: Header=BB4_605 Depth=1
	movsd	qword ptr [r8 + 8*rsi], xmm1
	cmp	byte ptr [rcx + rsi + 1], 0
	movapd	xmm1, xmm0
	jne	.LBB4_607
.LBB4_610:                              #   in Loop: Header=BB4_605 Depth=1
	xorpd	xmm1, xmm1
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	cmp	byte ptr [rcx + rsi + 2], 0
	movapd	xmm1, xmm0
	je	.LBB4_611
.LBB4_608:                              #   in Loop: Header=BB4_605 Depth=1
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	cmp	byte ptr [rcx + rsi + 3], 0
	movapd	xmm1, xmm0
	jne	.LBB4_604
.LBB4_612:                              #   in Loop: Header=BB4_605 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_604
.LBB4_613:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	movsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	jmp	.LBB4_615
.LBB4_614:                              #   in Loop: Header=BB4_615 Depth=1
	movsd	qword ptr [r8 + 8*rax + 8], xmm3
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_197
.LBB4_615:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rax], 0
	movapd	xmm2, xmm0
	jne	.LBB4_616
# %bb.619:                              #   in Loop: Header=BB4_615 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jle	.LBB4_620
.LBB4_617:                              #   in Loop: Header=BB4_615 Depth=1
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	dword ptr [rcx + 4*rax + 4], 0
	movapd	xmm2, xmm0
	jne	.LBB4_618
.LBB4_621:                              #   in Loop: Header=BB4_615 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jg	.LBB4_614
	jmp	.LBB4_622
.LBB4_616:                              #   in Loop: Header=BB4_615 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_617
.LBB4_620:                              #   in Loop: Header=BB4_615 Depth=1
	movapd	xmm3, xmm2
	movsd	qword ptr [r8 + 8*rax], xmm3
	cmp	dword ptr [rcx + 4*rax + 4], 0
	movapd	xmm2, xmm0
	je	.LBB4_621
.LBB4_618:                              #   in Loop: Header=BB4_615 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_614
.LBB4_622:                              #   in Loop: Header=BB4_615 Depth=1
	movapd	xmm3, xmm2
	jmp	.LBB4_614
.LBB4_673:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_999
# %bb.674:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_15] # xmm2 = [1,1]
.LBB4_675:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + 4*rsi]   # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 4*rsi + 8] # xmm4 = mem[0],zero
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxdq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxdq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + 4*rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 4*rsi + 24] # xmm4 = mem[0],zero
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxdq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxdq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_675
	jmp	.LBB4_1000
.LBB4_676:
	mov	esi, r10d
	and	esi, -2
	lea	rax, [rsi - 2]
	mov	r9, rax
	shr	r9
	add	r9, 1
	test	rax, rax
	je	.LBB4_1004
# %bb.677:
	mov	r14, r9
	and	r14, -2
	neg	r14
	xor	edi, edi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movapd	xmm2, xmmword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
	movsd	xmm3, qword ptr [rip + .LCPI4_6] # xmm3 = mem[0],zero
.LBB4_678:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm4, xmmword ptr [rcx + 8*rdi]
	movapd	xmm5, xmm4
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm5
	subsd	xmm6, xmm3
	cvttsd2si	rbx, xmm6
	xor	rbx, r11
	cvttsd2si	rdx, xmm5
	ucomisd	xmm5, xmm3
	cmovae	rdx, rbx
	pshufd	xmm5, xmm5, 238                 # xmm5 = xmm5[2,3,2,3]
	movdqa	xmm6, xmm5
	subsd	xmm6, xmm3
	cvttsd2si	rbx, xmm6
	xor	rbx, r11
	cvttsd2si	rax, xmm5
	ucomisd	xmm5, xmm3
	cmovae	rax, rbx
	movq	xmm5, rdx
	movq	xmm6, rax
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm5
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmm4, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm5, xmm4
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm5
	subsd	xmm6, xmm3
	cvttsd2si	rax, xmm6
	xor	rax, r11
	cvttsd2si	rdx, xmm5
	ucomisd	xmm5, xmm3
	cmovae	rdx, rax
	pshufd	xmm5, xmm5, 238                 # xmm5 = xmm5[2,3,2,3]
	movdqa	xmm6, xmm5
	subsd	xmm6, xmm3
	cvttsd2si	rax, xmm6
	xor	rax, r11
	cvttsd2si	rbx, xmm5
	ucomisd	xmm5, xmm3
	cmovae	rbx, rax
	movq	xmm5, rdx
	movq	xmm6, rbx
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm5
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm4
	add	rdi, 4
	add	r14, 2
	jne	.LBB4_678
	jmp	.LBB4_1005
.LBB4_689:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1010
# %bb.690:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_15] # xmm2 = [1,1]
.LBB4_691:                              # =>This Inner Loop Header: Depth=1
	movd	xmm3, dword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + 2*rsi + 4] # xmm4 = mem[0],zero,zero,zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movd	xmm3, dword ptr [rcx + 2*rsi + 8] # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + 2*rsi + 12] # xmm4 = mem[0],zero,zero,zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_691
	jmp	.LBB4_1011
.LBB4_692:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1015
# %bb.693:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_694:                              # =>This Inner Loop Header: Depth=1
	movd	xmm5, dword ptr [rcx + 2*rsi]   # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + 2*rsi + 4] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwq	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwq	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm6
	movd	xmm5, dword ptr [rcx + 2*rsi + 8] # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + 2*rsi + 12] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwq	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwq	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_694
	jmp	.LBB4_1016
.LBB4_700:
	mov	esi, r10d
	and	esi, -2
	xor	eax, eax
	xorps	xmm0, xmm0
	movss	xmm1, dword ptr [rip + .LCPI4_9] # xmm1 = mem[0],zero,zero,zero
	movabs	r9, -9223372036854775808
	jmp	.LBB4_703
.LBB4_701:                              #   in Loop: Header=BB4_703 Depth=1
	movmskps	edx, xmm2
	and	edx, 1
	neg	edx
	or	edx, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, edx
	movaps	xmm3, xmm2
	subss	xmm3, xmm1
	cvttss2si	rdi, xmm3
	xor	rdi, r9
	cvttss2si	rdx, xmm2
	ucomiss	xmm2, xmm1
	cmovae	rdx, rdi
	mov	qword ptr [r8 + 8*rax + 8], rdx
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_290
.LBB4_703:                              # =>This Inner Loop Header: Depth=1
	movss	xmm2, dword ptr [rcx + 4*rax]   # xmm2 = mem[0],zero,zero,zero
	ucomiss	xmm0, xmm2
	jne	.LBB4_705
# %bb.704:                              #   in Loop: Header=BB4_703 Depth=1
	xor	edx, edx
	jmp	.LBB4_706
.LBB4_705:                              #   in Loop: Header=BB4_703 Depth=1
	movmskps	edx, xmm2
	and	edx, 1
	neg	edx
	or	edx, 1
	xorps	xmm2, xmm2
	cvtsi2ss	xmm2, edx
	movaps	xmm3, xmm2
	subss	xmm3, xmm1
	cvttss2si	rdi, xmm3
	xor	rdi, r9
	cvttss2si	rdx, xmm2
	ucomiss	xmm2, xmm1
	cmovae	rdx, rdi
.LBB4_706:                              #   in Loop: Header=BB4_703 Depth=1
	mov	qword ptr [r8 + 8*rax], rdx
	movss	xmm2, dword ptr [rcx + 4*rax + 4] # xmm2 = mem[0],zero,zero,zero
	ucomiss	xmm0, xmm2
	jne	.LBB4_701
# %bb.707:                              #   in Loop: Header=BB4_703 Depth=1
	xor	edx, edx
	mov	qword ptr [r8 + 8*rax + 8], rdx
	add	rax, 2
	cmp	rsi, rax
	jne	.LBB4_703
.LBB4_290:
	test	r10b, 1
	je	.LBB4_1655
# %bb.291:
	movss	xmm0, dword ptr [rcx + 4*rax]   # xmm0 = mem[0],zero,zero,zero
	xorps	xmm1, xmm1
	ucomiss	xmm1, xmm0
	jne	.LBB4_993
# %bb.292:
	xor	ecx, ecx
	mov	qword ptr [r8 + 8*rax], rcx
	jmp	.LBB4_1655
.LBB4_713:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1021
# %bb.714:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_715:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + 4*rsi]   # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 4*rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	pmovsxdq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	pmovsxdq	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxdq	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxdq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + 4*rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 4*rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	pmovsxdq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	pmovsxdq	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxdq	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxdq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_715
	jmp	.LBB4_1022
.LBB4_716:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1137
# %bb.717:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_11] # xmm2 = <1,1,1,1,u,u,u,u>
.LBB4_718:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm3
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_718
	jmp	.LBB4_1138
.LBB4_719:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1142
# %bb.720:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_11] # xmm2 = <1,1,1,1,u,u,u,u>
.LBB4_721:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm3
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_721
	jmp	.LBB4_1143
.LBB4_722:
	mov	esi, eax
	and	esi, -4
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1147
# %bb.723:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI4_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmmword ptr [rip + .LCPI4_1] # xmm4 = [1.0E+0,1.0E+0]
.LBB4_724:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm5, xmmword ptr [rcx + 8*rdi]
	movupd	xmm6, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm0, xmm5
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movapd	xmm1, xmm6
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	andpd	xmm5, xmm3
	orpd	xmm5, xmm4
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	cvttpd2dq	xmm5, xmm5
	pshuflw	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm6, xmm6
	pshuflw	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3,4,5,6,7]
	pblendvb	xmm5, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm2, xmm0
	movd	dword ptr [r8 + 2*rdi], xmm5
	movd	dword ptr [r8 + 2*rdi + 4], xmm6
	movupd	xmm5, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm6, xmmword ptr [rcx + 8*rdi + 48]
	movapd	xmm0, xmm5
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movapd	xmm1, xmm6
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	andpd	xmm5, xmm3
	orpd	xmm5, xmm4
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	cvttpd2dq	xmm5, xmm5
	pshuflw	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm6, xmm6
	pshuflw	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3,4,5,6,7]
	pblendvb	xmm5, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm2, xmm0
	movd	dword ptr [r8 + 2*rdi + 8], xmm5
	movd	dword ptr [r8 + 2*rdi + 12], xmm6
	add	rdi, 8
	add	rdx, 2
	jne	.LBB4_724
	jmp	.LBB4_1148
.LBB4_725:
	mov	esi, eax
	and	esi, -4
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1153
# %bb.726:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI4_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmmword ptr [rip + .LCPI4_1] # xmm4 = [1.0E+0,1.0E+0]
.LBB4_727:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm5, xmmword ptr [rcx + 8*rdi]
	movupd	xmm6, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm0, xmm5
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movapd	xmm1, xmm6
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	andpd	xmm5, xmm3
	orpd	xmm5, xmm4
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	cvttpd2dq	xmm5, xmm5
	pshuflw	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm6, xmm6
	pshuflw	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3,4,5,6,7]
	pblendvb	xmm5, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm2, xmm0
	movd	dword ptr [r8 + 2*rdi], xmm5
	movd	dword ptr [r8 + 2*rdi + 4], xmm6
	movupd	xmm5, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm6, xmmword ptr [rcx + 8*rdi + 48]
	movapd	xmm0, xmm5
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movapd	xmm1, xmm6
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	andpd	xmm5, xmm3
	orpd	xmm5, xmm4
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	cvttpd2dq	xmm5, xmm5
	pshuflw	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3,4,5,6,7]
	cvttpd2dq	xmm6, xmm6
	pshuflw	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3,4,5,6,7]
	pblendvb	xmm5, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm2, xmm0
	movd	dword ptr [r8 + 2*rdi + 8], xmm5
	movd	dword ptr [r8 + 2*rdi + 12], xmm6
	add	rdi, 8
	add	rdx, 2
	jne	.LBB4_727
	jmp	.LBB4_1154
.LBB4_738:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1027
# %bb.739:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_17] # xmm2 = <1,1,u,u,u,u,u,u>
.LBB4_740:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + 2*rsi], xmm3
	movd	dword ptr [r8 + 2*rsi + 4], xmm4
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + 2*rsi + 8], xmm3
	movd	dword ptr [r8 + 2*rsi + 12], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_740
	jmp	.LBB4_1028
.LBB4_741:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1032
# %bb.742:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_17] # xmm2 = <1,1,u,u,u,u,u,u>
.LBB4_743:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + 2*rsi], xmm3
	movd	dword ptr [r8 + 2*rsi + 4], xmm4
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + 2*rsi + 8], xmm3
	movd	dword ptr [r8 + 2*rsi + 12], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_743
	jmp	.LBB4_1033
.LBB4_764:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1037
# %bb.765:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_17] # xmm4 = <1,1,u,u,u,u,u,u>
.LBB4_766:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + 2*rsi], xmm5
	movd	dword ptr [r8 + 2*rsi + 4], xmm6
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + 2*rsi + 8], xmm5
	movd	dword ptr [r8 + 2*rsi + 12], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_766
	jmp	.LBB4_1038
.LBB4_767:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1159
# %bb.768:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_17] # xmm4 = <1,1,u,u,u,u,u,u>
.LBB4_769:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + 2*rsi], xmm5
	movd	dword ptr [r8 + 2*rsi + 4], xmm6
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + 2*rsi + 8], xmm5
	movd	dword ptr [r8 + 2*rsi + 12], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_769
	jmp	.LBB4_1160
.LBB4_770:
	mov	esi, eax
	and	esi, -8
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1165
# %bb.771:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	xorps	xmm4, xmm4
	pcmpeqd	xmm8, xmm8
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_11] # xmm6 = <1,1,1,1,u,u,u,u>
.LBB4_772:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rcx + 4*rdi]
	movups	xmm1, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	packssdw	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	pcmpeqd	xmm5, xmm5
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	punpcklqdq	xmm7, xmm5              # xmm7 = xmm7[0],xmm5[0]
	movdqu	xmmword ptr [r8 + 2*rdi], xmm7
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rcx + 4*rdi + 48]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	pcmpeqd	xmm5, xmm5
	pblendvb	xmm5, xmm6, xmm0
	packssdw	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm7, xmm4, xmm0
	punpcklqdq	xmm5, xmm7              # xmm5 = xmm5[0],xmm7[0]
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm5
	add	rdi, 16
	add	rdx, 2
	jne	.LBB4_772
	jmp	.LBB4_1166
.LBB4_773:
	mov	esi, eax
	and	esi, -8
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1171
# %bb.774:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	xorps	xmm4, xmm4
	pcmpeqd	xmm8, xmm8
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_11] # xmm6 = <1,1,1,1,u,u,u,u>
.LBB4_775:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rcx + 4*rdi]
	movups	xmm1, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	packssdw	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	pcmpeqd	xmm5, xmm5
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	punpcklqdq	xmm7, xmm5              # xmm7 = xmm7[0],xmm5[0]
	movdqu	xmmword ptr [r8 + 2*rdi], xmm7
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 32]
	movups	xmm1, xmmword ptr [rcx + 4*rdi + 48]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	pcmpeqd	xmm5, xmm5
	pblendvb	xmm5, xmm6, xmm0
	packssdw	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm7, xmm4, xmm0
	punpcklqdq	xmm5, xmm7              # xmm5 = xmm5[0],xmm7[0]
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm5
	add	rdi, 16
	add	rdx, 2
	jne	.LBB4_775
	jmp	.LBB4_1172
.LBB4_786:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1043
# %bb.787:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_11] # xmm4 = <1,1,1,1,u,u,u,u>
.LBB4_788:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm5
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_788
	jmp	.LBB4_1044
.LBB4_789:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1049
# %bb.790:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_11] # xmm4 = <1,1,1,1,u,u,u,u>
.LBB4_791:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm5
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_791
	jmp	.LBB4_1050
.LBB4_792:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1177
# %bb.793:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_15] # xmm2 = [1,1]
.LBB4_794:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + 4*rsi]   # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 4*rsi + 8] # xmm4 = mem[0],zero
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxdq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxdq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + 4*rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 4*rsi + 24] # xmm4 = mem[0],zero
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxdq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxdq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_794
	jmp	.LBB4_1178
.LBB4_795:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1182
# %bb.796:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_19] # xmm1 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_797:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 4*rsi], xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm3
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_797
	jmp	.LBB4_1183
.LBB4_798:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1190
# %bb.799:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movapd	xmm2, xmmword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
.LBB4_800:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm3, xmmword ptr [rcx + 8*rsi]
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	movapd	xmm5, xmm3
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm4
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	cvttsd2si	rbx, xmm5
	movq	xmm7, rbx
	pshufd	xmm5, xmm5, 238                 # xmm5 = xmm5[2,3,2,3]
	cvttsd2si	rbx, xmm5
	movq	xmm5, rbx
	punpcklqdq	xmm7, xmm5              # xmm7 = xmm7[0],xmm5[0]
	cvttsd2si	rbx, xmm6
	movq	xmm5, rbx
	pshufd	xmm6, xmm6, 238                 # xmm6 = xmm6[2,3,2,3]
	cvttsd2si	rbx, xmm6
	movq	xmm6, rbx
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	cmpneqpd	xmm3, xmm0
	andpd	xmm3, xmm7
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm5
	movupd	xmmword ptr [r8 + 8*rsi], xmm3
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movupd	xmm3, xmmword ptr [rcx + 8*rsi + 32]
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 48]
	movapd	xmm5, xmm3
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm4
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	cvttsd2si	rbx, xmm5
	movq	xmm7, rbx
	pshufd	xmm5, xmm5, 238                 # xmm5 = xmm5[2,3,2,3]
	cvttsd2si	rbx, xmm5
	movq	xmm5, rbx
	punpcklqdq	xmm7, xmm5              # xmm7 = xmm7[0],xmm5[0]
	cvttsd2si	rbx, xmm6
	movq	xmm5, rbx
	pshufd	xmm6, xmm6, 238                 # xmm6 = xmm6[2,3,2,3]
	cvttsd2si	rbx, xmm6
	movq	xmm6, rbx
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	cmpneqpd	xmm3, xmm0
	andpd	xmm3, xmm7
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_800
	jmp	.LBB4_1191
.LBB4_801:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1196
# %bb.802:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorpd	xmm8, xmm8
	cvtpd2ps	xmm1, xmmword ptr [rip + .LCPI4_1]
	movaps	xmm9, xmmword ptr [rip + .LCPI4_3] # xmm9 = [NaN,NaN,NaN,NaN]
	movshdup	xmm3, xmm1                      # xmm3 = xmm1[1,1,3,3]
	andps	xmm3, xmm9
	andps	xmm1, xmm9
.LBB4_803:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm4, xmmword ptr [rcx + 8*rsi]
	movupd	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	xorps	xmm5, xmm5
	cvtsd2ss	xmm5, xmm4
	cmpeqpd	xmm4, xmm8
	shufps	xmm4, xmm4, 232                 # xmm4 = xmm4[0,2,2,3]
	xorps	xmm7, xmm7
	cvtsd2ss	xmm7, xmm6
	cmpeqpd	xmm6, xmm8
	shufps	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	movsd	xmm0, qword ptr [rcx + 8*rsi + 8] # xmm0 = mem[0],zero
	cvtsd2ss	xmm0, xmm0
	movaps	xmm2, xmm9
	andnps	xmm2, xmm0
	orps	xmm2, xmm3
	movaps	xmm0, xmm9
	andnps	xmm0, xmm5
	orps	xmm0, xmm1
	unpcklps	xmm0, xmm2                      # xmm0 = xmm0[0],xmm2[0],xmm0[1],xmm2[1]
	andnps	xmm4, xmm0
	movsd	xmm0, qword ptr [rcx + 8*rsi + 24] # xmm0 = mem[0],zero
	cvtsd2ss	xmm0, xmm0
	movaps	xmm2, xmm9
	andnps	xmm2, xmm0
	orps	xmm2, xmm3
	movaps	xmm0, xmm9
	andnps	xmm0, xmm7
	orps	xmm0, xmm1
	unpcklps	xmm0, xmm2                      # xmm0 = xmm0[0],xmm2[0],xmm0[1],xmm2[1]
	andnps	xmm6, xmm0
	movlhps	xmm4, xmm6                      # xmm4 = xmm4[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rsi], xmm4
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 32]
	movupd	xmm0, xmmword ptr [rcx + 8*rsi + 48]
	xorps	xmm2, xmm2
	cvtsd2ss	xmm2, xmm4
	cmpeqpd	xmm4, xmm8
	shufps	xmm4, xmm4, 232                 # xmm4 = xmm4[0,2,2,3]
	xorps	xmm5, xmm5
	cvtsd2ss	xmm5, xmm0
	cmpeqpd	xmm0, xmm8
	movsd	xmm6, qword ptr [rcx + 8*rsi + 40] # xmm6 = mem[0],zero
	cvtsd2ss	xmm6, xmm6
	shufps	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movaps	xmm7, xmm9
	andnps	xmm7, xmm6
	orps	xmm7, xmm3
	movaps	xmm6, xmm9
	andnps	xmm6, xmm2
	orps	xmm6, xmm1
	unpcklps	xmm6, xmm7                      # xmm6 = xmm6[0],xmm7[0],xmm6[1],xmm7[1]
	andnps	xmm4, xmm6
	movsd	xmm2, qword ptr [rcx + 8*rsi + 56] # xmm2 = mem[0],zero
	cvtsd2ss	xmm2, xmm2
	movaps	xmm6, xmm9
	andnps	xmm6, xmm2
	orps	xmm6, xmm3
	movaps	xmm2, xmm9
	andnps	xmm2, xmm5
	orps	xmm2, xmm1
	unpcklps	xmm2, xmm6                      # xmm2 = xmm2[0],xmm6[0],xmm2[1],xmm6[1]
	andnps	xmm0, xmm2
	movlhps	xmm4, xmm0                      # xmm4 = xmm4[0],xmm0[0]
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_803
	jmp	.LBB4_1197
.LBB4_819:
	and	edx, -4
	xor	esi, esi
	movss	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_821
.LBB4_820:                              #   in Loop: Header=BB4_821 Depth=1
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	rdx, rsi
	je	.LBB4_387
.LBB4_821:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rsi], 0
	movapd	xmm1, xmm0
	jne	.LBB4_822
# %bb.825:                              #   in Loop: Header=BB4_821 Depth=1
	xorpd	xmm1, xmm1
	movss	dword ptr [r8 + 4*rsi], xmm1
	cmp	qword ptr [rcx + 8*rsi + 8], 0
	movapd	xmm1, xmm0
	je	.LBB4_826
.LBB4_823:                              #   in Loop: Header=BB4_821 Depth=1
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	cmp	qword ptr [rcx + 8*rsi + 16], 0
	movapd	xmm1, xmm0
	jne	.LBB4_824
.LBB4_827:                              #   in Loop: Header=BB4_821 Depth=1
	xorpd	xmm1, xmm1
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	cmp	qword ptr [rcx + 8*rsi + 24], 0
	movapd	xmm1, xmm0
	jne	.LBB4_820
	jmp	.LBB4_828
.LBB4_822:                              #   in Loop: Header=BB4_821 Depth=1
	movss	dword ptr [r8 + 4*rsi], xmm1
	cmp	qword ptr [rcx + 8*rsi + 8], 0
	movapd	xmm1, xmm0
	jne	.LBB4_823
.LBB4_826:                              #   in Loop: Header=BB4_821 Depth=1
	xorpd	xmm1, xmm1
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	cmp	qword ptr [rcx + 8*rsi + 16], 0
	movapd	xmm1, xmm0
	je	.LBB4_827
.LBB4_824:                              #   in Loop: Header=BB4_821 Depth=1
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	cmp	qword ptr [rcx + 8*rsi + 24], 0
	movapd	xmm1, xmm0
	jne	.LBB4_820
.LBB4_828:                              #   in Loop: Header=BB4_821 Depth=1
	xorpd	xmm1, xmm1
	jmp	.LBB4_820
.LBB4_829:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1055
# %bb.830:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_15] # xmm2 = [1,1]
.LBB4_831:                              # =>This Inner Loop Header: Depth=1
	movd	xmm3, dword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + 2*rsi + 4] # xmm4 = mem[0],zero,zero,zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movd	xmm3, dword ptr [rcx + 2*rsi + 8] # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + 2*rsi + 12] # xmm4 = mem[0],zero,zero,zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_831
	jmp	.LBB4_1056
.LBB4_832:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1204
# %bb.833:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_834:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	pand	xmm3, xmm2
	cvtdq2ps	xmm3, xmm3
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	pand	xmm4, xmm2
	cvtdq2ps	xmm4, xmm4
	movups	xmmword ptr [r8 + 4*rsi], xmm3
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + 2*rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 2*rsi + 24] # xmm4 = mem[0],zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	pand	xmm3, xmm2
	cvtdq2ps	xmm3, xmm3
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	pand	xmm4, xmm2
	cvtdq2ps	xmm4, xmm4
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm3
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_834
	jmp	.LBB4_1205
.LBB4_835:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1212
# %bb.836:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_837:                              # =>This Inner Loop Header: Depth=1
	movd	xmm5, dword ptr [rcx + 2*rsi]   # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + 2*rsi + 4] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwq	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwq	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm6
	movd	xmm5, dword ptr [rcx + 2*rsi + 8] # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + 2*rsi + 12] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwq	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwq	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_837
	jmp	.LBB4_1213
.LBB4_838:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1218
# %bb.839:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_19] # xmm4 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_840:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + 2*rsi]   # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 2*rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwd	xmm5, xmm5
	cvtdq2ps	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwd	xmm6, xmm6
	cvtdq2ps	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwd	xmm5, xmm5
	cvtdq2ps	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwd	xmm6, xmm6
	cvtdq2ps	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_840
	jmp	.LBB4_1219
.LBB4_846:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	movss	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	movss	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_848
.LBB4_847:                              #   in Loop: Header=BB4_848 Depth=1
	movss	dword ptr [r8 + 4*rax + 4], xmm3
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_410
.LBB4_848:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rax], 0
	movapd	xmm2, xmm0
	jne	.LBB4_849
# %bb.852:                              #   in Loop: Header=BB4_848 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jle	.LBB4_853
.LBB4_850:                              #   in Loop: Header=BB4_848 Depth=1
	movss	dword ptr [r8 + 4*rax], xmm3
	cmp	qword ptr [rcx + 8*rax + 8], 0
	movapd	xmm2, xmm0
	jne	.LBB4_851
.LBB4_854:                              #   in Loop: Header=BB4_848 Depth=1
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm1
	jg	.LBB4_847
	jmp	.LBB4_855
.LBB4_849:                              #   in Loop: Header=BB4_848 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_850
.LBB4_853:                              #   in Loop: Header=BB4_848 Depth=1
	movapd	xmm3, xmm2
	movss	dword ptr [r8 + 4*rax], xmm3
	cmp	qword ptr [rcx + 8*rax + 8], 0
	movapd	xmm2, xmm0
	je	.LBB4_854
.LBB4_851:                              #   in Loop: Header=BB4_848 Depth=1
	movapd	xmm3, xmm1
	jg	.LBB4_847
.LBB4_855:                              #   in Loop: Header=BB4_848 Depth=1
	movapd	xmm3, xmm2
	jmp	.LBB4_847
.LBB4_856:
	mov	esi, edx
	and	esi, -2
	xor	eax, eax
	xorps	xmm0, xmm0
	jmp	.LBB4_859
.LBB4_857:                              #   in Loop: Header=BB4_859 Depth=1
	movmskps	edi, xmm1
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, edi
	cvttss2si	rdi, xmm1
	mov	qword ptr [r8 + 8*rax + 8], rdi
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_416
.LBB4_859:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rax]   # xmm1 = mem[0],zero,zero,zero
	ucomiss	xmm0, xmm1
	jne	.LBB4_861
# %bb.860:                              #   in Loop: Header=BB4_859 Depth=1
	xor	edi, edi
	jmp	.LBB4_862
.LBB4_861:                              #   in Loop: Header=BB4_859 Depth=1
	movmskps	edi, xmm1
	and	edi, 1
	neg	edi
	or	edi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, edi
	cvttss2si	rdi, xmm1
.LBB4_862:                              #   in Loop: Header=BB4_859 Depth=1
	mov	qword ptr [r8 + 8*rax], rdi
	movss	xmm1, dword ptr [rcx + 4*rax + 4] # xmm1 = mem[0],zero,zero,zero
	ucomiss	xmm0, xmm1
	jne	.LBB4_857
# %bb.863:                              #   in Loop: Header=BB4_859 Depth=1
	xor	edi, edi
	mov	qword ptr [r8 + 8*rax + 8], rdi
	add	rax, 2
	cmp	rsi, rax
	jne	.LBB4_859
.LBB4_416:
	test	dl, 1
	je	.LBB4_1655
# %bb.417:
	movss	xmm0, dword ptr [rcx + 4*rax]   # xmm0 = mem[0],zero,zero,zero
	xorps	xmm1, xmm1
	ucomiss	xmm1, xmm0
	jne	.LBB4_1104
# %bb.418:
	xor	ecx, ecx
	jmp	.LBB4_1105
.LBB4_884:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1060
# %bb.885:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_886:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + 4*rsi]   # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 4*rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	pmovsxdq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	pmovsxdq	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxdq	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxdq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + 4*rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 4*rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	pmovsxdq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	pmovsxdq	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxdq	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxdq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_886
	jmp	.LBB4_1061
.LBB4_887:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1066
# %bb.888:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_19] # xmm4 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_889:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	cvtdq2ps	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	cvtdq2ps	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	cvtdq2ps	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	cvtdq2ps	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_889
	jmp	.LBB4_1067
.LBB4_945:
	mov	esi, eax
	and	esi, -4
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1076
# %bb.946:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movapd	xmm2, xmmword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
.LBB4_947:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm3, xmmword ptr [rcx + 8*rdi]
	movupd	xmm4, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm5, xmm3
	cmpeqpd	xmm5, xmm0
	shufps	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3]
	movapd	xmm6, xmm4
	cmpeqpd	xmm6, xmm0
	shufps	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	andpd	xmm4, xmm1
	orpd	xmm4, xmm2
	cvttpd2dq	xmm3, xmm3
	cvttpd2dq	xmm4, xmm4
	andnps	xmm5, xmm3
	andnps	xmm6, xmm4
	movlhps	xmm5, xmm6                      # xmm5 = xmm5[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rdi], xmm5
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 32]
	movupd	xmm4, xmmword ptr [rcx + 8*rdi + 48]
	movapd	xmm5, xmm3
	cmpeqpd	xmm5, xmm0
	shufps	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3]
	movapd	xmm6, xmm4
	cmpeqpd	xmm6, xmm0
	shufps	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	andpd	xmm4, xmm1
	orpd	xmm4, xmm2
	cvttpd2dq	xmm3, xmm3
	andnps	xmm5, xmm3
	cvttpd2dq	xmm3, xmm4
	andnps	xmm6, xmm3
	movlhps	xmm5, xmm6                      # xmm5 = xmm5[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm5
	add	rdi, 8
	add	rdx, 2
	jne	.LBB4_947
	jmp	.LBB4_1077
.LBB4_953:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1082
# %bb.954:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_16] # xmm1 = <1,1,u,u>
.LBB4_955:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm2, xmm0
	pshufd	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pshufd	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	pandn	xmm3, xmm1
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + 4*rsi], xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm2, xmm0
	pshufd	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pshufd	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	pandn	xmm3, xmm1
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_955
	jmp	.LBB4_1083
.LBB4_956:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1087
# %bb.957:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_958:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + 2*rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + 2*rsi + 24] # xmm4 = mem[0],zero
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxwd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxwd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_958
	jmp	.LBB4_1088
.LBB4_959:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1092
# %bb.960:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
.LBB4_961:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + 2*rsi]   # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 2*rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwd	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxwd	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxwd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_961
	jmp	.LBB4_1093
.LBB4_962:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1098
# %bb.963:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_16] # xmm4 = <1,1,u,u>
.LBB4_964:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pcmpeqq	xmm5, xmm2
	pshufd	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3]
	pxor	xmm5, xmm3
	pcmpeqq	xmm6, xmm2
	pshufd	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	pxor	xmm6, xmm3
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movlhps	xmm5, xmm6                      # xmm5 = xmm5[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 48]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pcmpeqq	xmm5, xmm2
	pshufd	xmm5, xmm5, 232                 # xmm5 = xmm5[0,2,2,3]
	pxor	xmm5, xmm3
	pcmpeqq	xmm6, xmm2
	pshufd	xmm6, xmm6, 232                 # xmm6 = xmm6[0,2,2,3]
	pxor	xmm6, xmm3
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movlhps	xmm5, xmm6                      # xmm5 = xmm5[0],xmm6[0]
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm5
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_964
	jmp	.LBB4_1099
.LBB4_965:
	mov	edx, eax
	and	edx, -8
	xor	esi, esi
	xorps	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_966:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm4, xmm2
	psrad	xmm4, 31
	por	xmm4, xmm1
	movdqa	xmm5, xmm3
	psrad	xmm5, 31
	por	xmm5, xmm1
	cvtdq2ps	xmm4, xmm4
	cvtdq2ps	xmm5, xmm5
	cvttps2dq	xmm4, xmm4
	cvttps2dq	xmm5, xmm5
	cmpneqps	xmm2, xmm0
	andps	xmm2, xmm4
	cmpneqps	xmm3, xmm0
	andps	xmm3, xmm5
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
	add	rsi, 8
	cmp	rdx, rsi
	jne	.LBB4_966
# %bb.967:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_968:
	xorps	xmm0, xmm0
	jmp	.LBB4_970
.LBB4_969:                              #   in Loop: Header=BB4_970 Depth=1
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_970:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	xor	esi, esi
	ucomiss	xmm0, xmm1
	je	.LBB4_969
# %bb.971:                              #   in Loop: Header=BB4_970 Depth=1
	movmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, esi
	cvttss2si	esi, xmm1
	jmp	.LBB4_969
.LBB4_496:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1228
# %bb.497:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_498:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 4*rsi], xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm3
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_498
	jmp	.LBB4_1229
.LBB4_504:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1236
# %bb.505:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
.LBB4_506:                              # =>This Inner Loop Header: Depth=1
	movd	xmm5, dword ptr [rcx + rsi]     # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + rsi + 4] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbd	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbd	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_506
	jmp	.LBB4_1237
.LBB4_524:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1245
# %bb.525:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_526:                              # =>This Inner Loop Header: Depth=1
	movd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	movd	xmm3, dword ptr [rcx + rsi + 8] # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + rsi + 12] # xmm4 = mem[0],zero,zero,zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_526
	jmp	.LBB4_1246
.LBB4_529:
	mov	edx, r11d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1253
# %bb.530:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
.LBB4_531:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm5
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm6
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvps	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvps	xmm5, xmm6, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm7
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm5
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm6
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvps	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvps	xmm5, xmm6, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm7
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm5
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_531
	jmp	.LBB4_1254
.LBB4_544:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1262
# %bb.545:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movapd	xmm2, xmmword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
.LBB4_546:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm3, xmmword ptr [rcx + 8*rsi]
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	movapd	xmm5, xmm3
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm4
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	cmpneqpd	xmm3, xmm0
	andpd	xmm3, xmm5
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm6
	movupd	xmmword ptr [r8 + 8*rsi], xmm3
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movupd	xmm3, xmmword ptr [rcx + 8*rsi + 32]
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 48]
	movapd	xmm5, xmm3
	andpd	xmm5, xmm1
	orpd	xmm5, xmm2
	movapd	xmm6, xmm4
	andpd	xmm6, xmm1
	orpd	xmm6, xmm2
	cmpneqpd	xmm3, xmm0
	andpd	xmm3, xmm5
	cmpneqpd	xmm4, xmm0
	andpd	xmm4, xmm6
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_546
	jmp	.LBB4_1263
.LBB4_625:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1271
# %bb.626:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_12] # xmm2 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_627:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + rsi], xmm3
	movd	dword ptr [r8 + rsi + 4], xmm4
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + rsi + 8], xmm3
	movd	dword ptr [r8 + rsi + 12], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_627
	jmp	.LBB4_1272
.LBB4_630:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1279
# %bb.631:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI4_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmmword ptr [rip + .LCPI4_1] # xmm4 = [1.0E+0,1.0E+0]
	movdqa	xmm5, xmmword ptr [rip + .LCPI4_7] # xmm5 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_632:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm6, xmmword ptr [rcx + 8*rsi]
	movupd	xmm7, xmmword ptr [rcx + 8*rsi + 16]
	movapd	xmm0, xmm6
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movapd	xmm1, xmm7
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	andpd	xmm7, xmm3
	orpd	xmm7, xmm4
	cvttpd2dq	xmm6, xmm6
	pshufb	xmm6, xmm5
	cvttpd2dq	xmm7, xmm7
	pshufb	xmm7, xmm5
	pblendvb	xmm6, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm2, xmm0
	pextrw	word ptr [r8 + rsi], xmm6, 0
	pextrw	word ptr [r8 + rsi + 2], xmm7, 0
	movupd	xmm6, xmmword ptr [rcx + 8*rsi + 32]
	movupd	xmm7, xmmword ptr [rcx + 8*rsi + 48]
	movapd	xmm0, xmm6
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movapd	xmm1, xmm7
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	andpd	xmm7, xmm3
	orpd	xmm7, xmm4
	cvttpd2dq	xmm6, xmm6
	pshufb	xmm6, xmm5
	cvttpd2dq	xmm7, xmm7
	pshufb	xmm7, xmm5
	pblendvb	xmm6, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm2, xmm0
	pextrw	word ptr [r8 + rsi + 4], xmm6, 0
	pextrw	word ptr [r8 + rsi + 6], xmm7, 0
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_632
	jmp	.LBB4_1280
.LBB4_635:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB4_1288
# %bb.636:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_22] # xmm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_637:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + rax]
	movdqu	xmm6, xmmword ptr [rcx + rax + 16]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm5
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm6
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + rax + 48]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm5
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm6
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rdi, 2
	jne	.LBB4_637
	jmp	.LBB4_1289
.LBB4_640:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1297
# %bb.641:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_18] # xmm2 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_642:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pextrw	word ptr [r8 + rsi], xmm3, 0
	pand	xmm4, xmm2
	pextrw	word ptr [r8 + rsi + 2], xmm4, 0
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pextrw	word ptr [r8 + rsi + 4], xmm3, 0
	pand	xmm4, xmm2
	pextrw	word ptr [r8 + rsi + 6], xmm4, 0
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_642
	jmp	.LBB4_1298
.LBB4_645:
	mov	edx, eax
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1305
# %bb.646:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_21] # xmm2 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_647:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 2*rsi + 16]
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + rsi], xmm3
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 2*rsi + 48]
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + rsi + 16], xmm3
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_647
	jmp	.LBB4_1306
.LBB4_650:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB4_1313
# %bb.651:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_21] # xmm4 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_652:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 2*rax]
	movdqu	xmm6, xmmword ptr [rcx + 2*rax + 16]
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	packsswb	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	packsswb	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 2*rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + 2*rax + 48]
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	packsswb	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	packsswb	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	add	rax, 32
	add	rdi, 2
	jne	.LBB4_652
	jmp	.LBB4_1314
.LBB4_655:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB4_1322
# %bb.656:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_18] # xmm4 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_657:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rax]
	movdqu	xmm6, xmmword ptr [rcx + 8*rax + 16]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	pextrw	word ptr [r8 + rax], xmm5, 0
	pextrw	word ptr [r8 + rax + 2], xmm6, 0
	movdqu	xmm5, xmmword ptr [rcx + 8*rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rax + 48]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	pextrw	word ptr [r8 + rax + 4], xmm5, 0
	pextrw	word ptr [r8 + rax + 6], xmm6, 0
	add	rax, 8
	add	rdi, 2
	jne	.LBB4_657
	jmp	.LBB4_1323
.LBB4_660:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1331
# %bb.661:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorps	xmm4, xmm4
	pcmpeqd	xmm8, xmm8
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_12] # xmm6 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_662:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rcx + 4*rsi]
	movups	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	pcmpeqd	xmm5, xmm5
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	movd	dword ptr [r8 + rsi], xmm7
	movd	dword ptr [r8 + rsi + 4], xmm5
	movups	xmm0, xmmword ptr [rcx + 4*rsi + 32]
	movups	xmm1, xmmword ptr [rcx + 4*rsi + 48]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	packssdw	xmm1, xmm1
	pcmpeqd	xmm5, xmm5
	pblendvb	xmm5, xmm6, xmm0
	packsswb	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm7, xmm4, xmm0
	movd	dword ptr [r8 + rsi + 8], xmm5
	movd	dword ptr [r8 + rsi + 12], xmm7
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_662
	jmp	.LBB4_1332
.LBB4_665:
	mov	edx, eax
	and	edx, -32
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1340
# %bb.666:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_22] # xmm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_667:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + rsi]
	movdqu	xmm3, xmmword ptr [rcx + rsi + 16]
	pcmpeqb	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqb	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	movdqu	xmmword ptr [r8 + rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + rsi + 48]
	pcmpeqb	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqb	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + rsi + 48], xmm3
	add	rsi, 64
	add	rdi, 2
	jne	.LBB4_667
	jmp	.LBB4_1341
.LBB4_670:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB4_1348
# %bb.671:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_12] # xmm4 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_672:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rax]
	movdqu	xmm6, xmmword ptr [rcx + 4*rax + 16]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + rax], xmm5
	movd	dword ptr [r8 + rax + 4], xmm6
	movdqu	xmm5, xmmword ptr [rcx + 4*rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rax + 48]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + rax + 8], xmm5
	movd	dword ptr [r8 + rax + 12], xmm6
	add	rax, 16
	add	rdi, 2
	jne	.LBB4_672
	jmp	.LBB4_1349
.LBB4_681:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1357
# %bb.682:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_683:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm5, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm6, eax
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbq	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbq	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm6
	movzx	eax, word ptr [rcx + rsi + 4]
	movd	xmm5, eax
	movzx	eax, word ptr [rcx + rsi + 6]
	movd	xmm6, eax
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbq	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbq	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_683
	jmp	.LBB4_1358
.LBB4_686:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1366
# %bb.687:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_15] # xmm1 = [1,1]
.LBB4_688:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 8*rsi], xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm3
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_688
	jmp	.LBB4_1367
.LBB4_697:
	mov	edx, r11d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1374
# %bb.698:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_699:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm6
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvpd	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvpd	xmm5, xmm6, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm7
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 48]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm6
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvpd	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvpd	xmm5, xmm6, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm7
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm5
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_699
	jmp	.LBB4_1375
.LBB4_710:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1383
# %bb.711:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_15] # xmm2 = [1,1]
.LBB4_712:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm3, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm4, eax
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,zero,zero,zero,zero,xmm3[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,zero,zero,zero,zero,xmm4[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movzx	eax, word ptr [rcx + rsi + 4]
	movd	xmm3, eax
	movzx	eax, word ptr [rcx + rsi + 6]
	movd	xmm4, eax
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,zero,zero,zero,zero,xmm3[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,zero,zero,zero,zero,xmm4[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_712
	jmp	.LBB4_1384
.LBB4_730:
	mov	edx, r10d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1391
# %bb.731:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
.LBB4_732:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + rsi]     # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbw	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbw	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm5
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbw	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbw	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm5
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm6
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_732
	jmp	.LBB4_1392
.LBB4_735:
	mov	edx, r10d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1400
# %bb.736:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
.LBB4_737:                              # =>This Inner Loop Header: Depth=1
	movq	xmm5, qword ptr [rcx + rsi]     # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + rsi + 8] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbw	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbw	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm5
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm6
	movq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	movq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbw	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbw	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbw	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbw	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm5
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm6
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_737
	jmp	.LBB4_1401
.LBB4_746:
	mov	edx, r10d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1409
# %bb.747:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_20] # xmm1 = [1,1,1,1,1,1,1,1]
.LBB4_748:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi + 16]
	pcmpeqw	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqw	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 2*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi + 48]
	pcmpeqw	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqw	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm3
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_748
	jmp	.LBB4_1410
.LBB4_751:
	mov	edx, r10d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1417
# %bb.752:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_20] # xmm1 = [1,1,1,1,1,1,1,1]
.LBB4_753:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi + 16]
	pcmpeqw	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqw	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 2*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi + 48]
	pcmpeqw	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqw	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm3
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_753
	jmp	.LBB4_1418
.LBB4_756:
	mov	edx, r11d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1425
# %bb.757:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
.LBB4_758:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 2*rsi + 16]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm5
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm6
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm7
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 2*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 2*rsi + 48]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm5
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm6
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm7
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm5
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_758
	jmp	.LBB4_1426
.LBB4_761:
	mov	edx, r11d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1434
# %bb.762:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
.LBB4_763:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 2*rsi + 16]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm5
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm6
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm7
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 2*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 2*rsi + 48]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm5
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm6
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm7
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm5
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_763
	jmp	.LBB4_1435
.LBB4_778:
	mov	edx, r10d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1443
# %bb.779:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_20] # xmm2 = [1,1,1,1,1,1,1,1]
.LBB4_780:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbw	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbw	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 2*rsi], xmm3
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + rsi + 24] # xmm4 = mem[0],zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbw	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbw	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm4
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_780
	jmp	.LBB4_1444
.LBB4_783:
	mov	edx, r10d
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1451
# %bb.784:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_20] # xmm2 = [1,1,1,1,1,1,1,1]
.LBB4_785:                              # =>This Inner Loop Header: Depth=1
	movq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbw	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbw	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 2*rsi], xmm3
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm4
	movq	xmm3, qword ptr [rcx + rsi + 16] # xmm3 = mem[0],zero
	movq	xmm4, qword ptr [rcx + rsi + 24] # xmm4 = mem[0],zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbw	xmm3, xmm3                      # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbw	xmm4, xmm4                      # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rsi + 48], xmm4
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_785
	jmp	.LBB4_1452
.LBB4_806:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1459
# %bb.807:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_808:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm5, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm6, eax
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbq	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbq	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm6
	movzx	eax, word ptr [rcx + rsi + 4]
	movd	xmm5, eax
	movzx	eax, word ptr [rcx + rsi + 6]
	movd	xmm6, eax
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbq	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbq	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbq	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbq	xmm6, xmm6
	blendvpd	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm6, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm5
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm6
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_808
	jmp	.LBB4_1460
.LBB4_811:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1468
# %bb.812:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_19] # xmm4 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_813:                              # =>This Inner Loop Header: Depth=1
	movd	xmm5, dword ptr [rcx + rsi]     # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + rsi + 4] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbd	xmm5, xmm5
	cvtdq2ps	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbd	xmm6, xmm6
	cvtdq2ps	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbd	xmm5, xmm5
	cvtdq2ps	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbd	xmm6, xmm6
	cvtdq2ps	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_813
	jmp	.LBB4_1469
.LBB4_816:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1490
# %bb.817:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_15] # xmm1 = [1,1]
.LBB4_818:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 8*rsi], xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqq	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm3
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_818
	jmp	.LBB4_1491
.LBB4_843:
	mov	edx, r11d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1498
# %bb.844:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
.LBB4_845:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 16]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm6
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvpd	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvpd	xmm5, xmm6, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm7
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rsi + 48]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm5
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm6
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvpd	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvpd	xmm5, xmm6, xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 32], xmm7
	movupd	xmmword ptr [r8 + 8*rsi + 48], xmm5
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_845
	jmp	.LBB4_1499
.LBB4_989:
	movss	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
.LBB4_990:
	jle	.LBB4_992
# %bb.991:
	movss	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
.LBB4_992:
	movss	dword ptr [r8 + 4*rax], xmm0
	jmp	.LBB4_1655
.LBB4_866:
	mov	edx, eax
	and	edx, -8
	xor	esi, esi
	xorps	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_867:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm4, xmm2
	psrad	xmm4, 31
	por	xmm4, xmm1
	movdqa	xmm5, xmm3
	psrad	xmm5, 31
	por	xmm5, xmm1
	cvtdq2ps	xmm4, xmm4
	cvtdq2ps	xmm5, xmm5
	cmpneqps	xmm2, xmm0
	andps	xmm2, xmm4
	cmpneqps	xmm3, xmm0
	andps	xmm3, xmm5
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
	add	rsi, 8
	cmp	rdx, rsi
	jne	.LBB4_867
# %bb.868:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_869
.LBB4_876:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1507
# %bb.877:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_15] # xmm2 = [1,1]
.LBB4_878:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm3, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm4, eax
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,zero,zero,zero,zero,xmm3[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,zero,zero,zero,zero,xmm4[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm4
	movzx	eax, word ptr [rcx + rsi + 4]
	movd	xmm3, eax
	movzx	eax, word ptr [rcx + rsi + 6]
	movd	xmm4, eax
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbq	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,zero,zero,zero,zero,xmm3[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbq	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,zero,zero,zero,zero,xmm4[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 8*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rsi + 48], xmm4
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_878
	jmp	.LBB4_1508
.LBB4_881:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1515
# %bb.882:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_883:                              # =>This Inner Loop Header: Depth=1
	movd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	pand	xmm3, xmm2
	cvtdq2ps	xmm3, xmm3
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	pand	xmm4, xmm2
	cvtdq2ps	xmm4, xmm4
	movups	xmmword ptr [r8 + 4*rsi], xmm3
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm4
	movd	xmm3, dword ptr [rcx + rsi + 8] # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + rsi + 12] # xmm4 = mem[0],zero,zero,zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	pand	xmm3, xmm2
	cvtdq2ps	xmm3, xmm3
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	pand	xmm4, xmm2
	cvtdq2ps	xmm4, xmm4
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm3
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_883
	jmp	.LBB4_1516
.LBB4_892:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1535
# %bb.893:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_12] # xmm2 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_894:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + rsi], xmm3
	movd	dword ptr [r8 + rsi + 4], xmm4
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqd	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	movd	dword ptr [r8 + rsi + 8], xmm3
	movd	dword ptr [r8 + rsi + 12], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_894
	jmp	.LBB4_1536
.LBB4_897:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1543
# %bb.898:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI4_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmmword ptr [rip + .LCPI4_1] # xmm4 = [1.0E+0,1.0E+0]
	movdqa	xmm5, xmmword ptr [rip + .LCPI4_7] # xmm5 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_899:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm6, xmmword ptr [rcx + 8*rsi]
	movupd	xmm7, xmmword ptr [rcx + 8*rsi + 16]
	movapd	xmm0, xmm6
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movapd	xmm1, xmm7
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	andpd	xmm7, xmm3
	orpd	xmm7, xmm4
	cvttpd2dq	xmm6, xmm6
	pshufb	xmm6, xmm5
	cvttpd2dq	xmm7, xmm7
	pshufb	xmm7, xmm5
	pblendvb	xmm6, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm2, xmm0
	pextrw	word ptr [r8 + rsi], xmm6, 0
	pextrw	word ptr [r8 + rsi + 2], xmm7, 0
	movupd	xmm6, xmmword ptr [rcx + 8*rsi + 32]
	movupd	xmm7, xmmword ptr [rcx + 8*rsi + 48]
	movapd	xmm0, xmm6
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movapd	xmm1, xmm7
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	andpd	xmm6, xmm3
	orpd	xmm6, xmm4
	andpd	xmm7, xmm3
	orpd	xmm7, xmm4
	cvttpd2dq	xmm6, xmm6
	pshufb	xmm6, xmm5
	cvttpd2dq	xmm7, xmm7
	pshufb	xmm7, xmm5
	pblendvb	xmm6, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm2, xmm0
	pextrw	word ptr [r8 + rsi + 4], xmm6, 0
	pextrw	word ptr [r8 + rsi + 6], xmm7, 0
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_899
	jmp	.LBB4_1544
.LBB4_902:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB4_1552
# %bb.903:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_22] # xmm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_904:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + rax]
	movdqu	xmm6, xmmword ptr [rcx + rax + 16]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm5
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm6
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm7
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + rax + 48]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm5
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm6
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	pblendvb	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqu	xmmword ptr [r8 + rax + 32], xmm7
	movdqu	xmmword ptr [r8 + rax + 48], xmm5
	add	rax, 64
	add	rdi, 2
	jne	.LBB4_904
	jmp	.LBB4_1553
.LBB4_907:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1561
# %bb.908:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_18] # xmm2 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_909:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pextrw	word ptr [r8 + rsi], xmm3, 0
	pand	xmm4, xmm2
	pextrw	word ptr [r8 + rsi + 2], xmm4, 0
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 8*rsi + 48]
	pcmpeqq	xmm3, xmm0
	pxor	xmm3, xmm1
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqq	xmm4, xmm0
	pxor	xmm4, xmm1
	packssdw	xmm4, xmm4
	packssdw	xmm4, xmm4
	packsswb	xmm4, xmm4
	pextrw	word ptr [r8 + rsi + 4], xmm3, 0
	pand	xmm4, xmm2
	pextrw	word ptr [r8 + rsi + 6], xmm4, 0
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_909
	jmp	.LBB4_1562
.LBB4_912:
	mov	edx, eax
	and	edx, -16
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1569
# %bb.913:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_21] # xmm2 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_914:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm4, xmmword ptr [rcx + 2*rsi + 16]
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + rsi], xmm3
	movdqu	xmm3, xmmword ptr [rcx + 2*rsi + 32]
	movdqu	xmm4, xmmword ptr [rcx + 2*rsi + 48]
	pcmpeqw	xmm3, xmm0
	pxor	xmm3, xmm1
	packsswb	xmm3, xmm3
	pand	xmm3, xmm2
	pcmpeqw	xmm4, xmm0
	pxor	xmm4, xmm1
	packsswb	xmm4, xmm4
	pand	xmm4, xmm2
	punpcklqdq	xmm3, xmm4              # xmm3 = xmm3[0],xmm4[0]
	movdqu	xmmword ptr [r8 + rsi + 16], xmm3
	add	rsi, 32
	add	rdi, 2
	jne	.LBB4_914
	jmp	.LBB4_1570
.LBB4_917:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB4_1577
# %bb.918:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_21] # xmm4 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_919:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 2*rax]
	movdqu	xmm6, xmmword ptr [rcx + 2*rax + 16]
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	packsswb	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	packsswb	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + rax], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 2*rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + 2*rax + 48]
	movdqa	xmm0, xmm5
	pcmpgtw	xmm0, xmm2
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtw	xmm1, xmm2
	packsswb	xmm1, xmm1
	pcmpeqw	xmm5, xmm2
	pxor	xmm5, xmm3
	packsswb	xmm5, xmm5
	pcmpeqw	xmm6, xmm2
	pxor	xmm6, xmm3
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	punpcklqdq	xmm5, xmm6              # xmm5 = xmm5[0],xmm6[0]
	movdqu	xmmword ptr [r8 + rax + 16], xmm5
	add	rax, 32
	add	rdi, 2
	jne	.LBB4_919
	jmp	.LBB4_1578
.LBB4_922:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB4_1586
# %bb.923:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_18] # xmm4 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_924:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 8*rax]
	movdqu	xmm6, xmmword ptr [rcx + 8*rax + 16]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	pextrw	word ptr [r8 + rax], xmm5, 0
	pextrw	word ptr [r8 + rax + 2], xmm6, 0
	movdqu	xmm5, xmmword ptr [rcx + 8*rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + 8*rax + 48]
	movdqa	xmm0, xmm5
	pcmpgtq	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtq	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqq	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqq	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	pextrw	word ptr [r8 + rax + 4], xmm5, 0
	pextrw	word ptr [r8 + rax + 6], xmm6, 0
	add	rax, 8
	add	rdi, 2
	jne	.LBB4_924
	jmp	.LBB4_1587
.LBB4_927:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1595
# %bb.928:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	xorps	xmm4, xmm4
	pcmpeqd	xmm8, xmm8
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_12] # xmm6 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_929:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rcx + 4*rsi]
	movups	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	pcmpeqd	xmm5, xmm5
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	movd	dword ptr [r8 + rsi], xmm7
	movd	dword ptr [r8 + rsi + 4], xmm5
	movups	xmm0, xmmword ptr [rcx + 4*rsi + 32]
	movups	xmm1, xmmword ptr [rcx + 4*rsi + 48]
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pcmpgtd	xmm0, xmm8
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	pcmpgtd	xmm1, xmm8
	packssdw	xmm1, xmm1
	pcmpeqd	xmm5, xmm5
	pblendvb	xmm5, xmm6, xmm0
	packsswb	xmm1, xmm1
	pcmpeqd	xmm7, xmm7
	movdqa	xmm0, xmm1
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm7, xmm4, xmm0
	movd	dword ptr [r8 + rsi + 8], xmm5
	movd	dword ptr [r8 + rsi + 12], xmm7
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_929
	jmp	.LBB4_1596
.LBB4_932:
	mov	edx, eax
	and	edx, -32
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1604
# %bb.933:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_22] # xmm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_934:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + rsi]
	movdqu	xmm3, xmmword ptr [rcx + rsi + 16]
	pcmpeqb	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqb	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + rsi], xmm2
	movdqu	xmmword ptr [r8 + rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + rsi + 48]
	pcmpeqb	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqb	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + rsi + 48], xmm3
	add	rsi, 64
	add	rdi, 2
	jne	.LBB4_934
	jmp	.LBB4_1605
.LBB4_937:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB4_1612
# %bb.938:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	eax, eax
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_12] # xmm4 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_939:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rax]
	movdqu	xmm6, xmmword ptr [rcx + 4*rax + 16]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + rax], xmm5
	movd	dword ptr [r8 + rax + 4], xmm6
	movdqu	xmm5, xmmword ptr [rcx + 4*rax + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rax + 48]
	movdqa	xmm0, xmm5
	pcmpgtd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	packssdw	xmm5, xmm5
	packsswb	xmm5, xmm5
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	packssdw	xmm6, xmm6
	packsswb	xmm6, xmm6
	pblendvb	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm6, xmm4, xmm0
	movd	dword ptr [r8 + rax + 8], xmm5
	movd	dword ptr [r8 + rax + 12], xmm6
	add	rax, 16
	add	rdi, 2
	jne	.LBB4_939
	jmp	.LBB4_1613
.LBB4_942:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1621
# %bb.943:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	movdqa	xmm1, xmmword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_944:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 4*rsi], xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm3
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 48]
	pcmpeqd	xmm2, xmm0
	pandn	xmm2, xmm1
	pcmpeqd	xmm3, xmm0
	pandn	xmm3, xmm1
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm3
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_944
	jmp	.LBB4_1622
.LBB4_950:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1629
# %bb.951:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
.LBB4_952:                              # =>This Inner Loop Header: Depth=1
	movd	xmm5, dword ptr [rcx + rsi]     # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + rsi + 4] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbd	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	movd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	movd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	movdqa	xmm0, xmm5
	pcmpgtb	xmm0, xmm2
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm6
	pcmpgtb	xmm1, xmm2
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm5, xmm2
	pxor	xmm5, xmm3
	pmovsxbd	xmm5, xmm5
	pcmpeqb	xmm6, xmm2
	pxor	xmm6, xmm3
	pmovsxbd	xmm6, xmm6
	blendvps	xmm5, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm6, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_952
	jmp	.LBB4_1630
.LBB4_974:
	mov	edx, r10d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1638
# %bb.975:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm0, xmm0
	pcmpeqd	xmm1, xmm1
	movdqa	xmm2, xmmword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_976:                              # =>This Inner Loop Header: Depth=1
	movd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	movd	xmm3, dword ptr [rcx + rsi + 8] # xmm3 = mem[0],zero,zero,zero
	movd	xmm4, dword ptr [rcx + rsi + 12] # xmm4 = mem[0],zero,zero,zero
	pcmpeqb	xmm3, xmm0
	pxor	xmm3, xmm1
	pmovzxbd	xmm3, xmm3                      # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	pand	xmm3, xmm2
	pcmpeqb	xmm4, xmm0
	pxor	xmm4, xmm1
	pmovzxbd	xmm4, xmm4                      # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	pand	xmm4, xmm2
	movdqu	xmmword ptr [r8 + 4*rsi + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rsi + 48], xmm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_976
	jmp	.LBB4_1639
.LBB4_979:
	mov	edx, r11d
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1646
# %bb.980:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	pxor	xmm2, xmm2
	pcmpeqd	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
.LBB4_981:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 16]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm5
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm6
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvps	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvps	xmm5, xmm6, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm7
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm5
	movdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	movdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm5
	pcmpeqd	xmm5, xmm2
	pxor	xmm5, xmm3
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm6
	pcmpeqd	xmm6, xmm2
	pxor	xmm6, xmm3
	movdqa	xmm7, xmm4
	blendvps	xmm7, xmm5, xmm0
	movdqa	xmm5, xmm4
	movdqa	xmm0, xmm1
	blendvps	xmm5, xmm6, xmm0
	movups	xmmword ptr [r8 + 4*rsi + 32], xmm7
	movups	xmmword ptr [r8 + 4*rsi + 48], xmm5
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_981
	jmp	.LBB4_1647
.LBB4_1475:
	movd	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
.LBB4_1476:
	jle	.LBB4_1478
# %bb.1477:
	movd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
.LBB4_1478:
	movd	dword ptr [r8 + 4*rdx], xmm0
	or	rdx, 1
.LBB4_1479:
	add	rsi, rax
	je	.LBB4_1655
# %bb.1480:
	movd	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_1482
.LBB4_1481:                             #   in Loop: Header=BB4_1482 Depth=1
	movd	dword ptr [r8 + 4*rdx + 4], xmm3
	add	rdx, 2
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1482:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	movdqa	xmm2, xmm0
	jne	.LBB4_1483
# %bb.1486:                             #   in Loop: Header=BB4_1482 Depth=1
	pxor	xmm2, xmm2
	movdqa	xmm3, xmm1
	jle	.LBB4_1487
.LBB4_1484:                             #   in Loop: Header=BB4_1482 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm3
	cmp	byte ptr [rcx + rdx + 1], 0
	movdqa	xmm2, xmm0
	jne	.LBB4_1485
.LBB4_1488:                             #   in Loop: Header=BB4_1482 Depth=1
	pxor	xmm2, xmm2
	movdqa	xmm3, xmm1
	jg	.LBB4_1481
	jmp	.LBB4_1489
.LBB4_1483:                             #   in Loop: Header=BB4_1482 Depth=1
	movdqa	xmm3, xmm1
	jg	.LBB4_1484
.LBB4_1487:                             #   in Loop: Header=BB4_1482 Depth=1
	movdqa	xmm3, xmm2
	movd	dword ptr [r8 + 4*rdx], xmm3
	cmp	byte ptr [rcx + rdx + 1], 0
	movdqa	xmm2, xmm0
	je	.LBB4_1488
.LBB4_1485:                             #   in Loop: Header=BB4_1482 Depth=1
	movdqa	xmm3, xmm1
	jg	.LBB4_1481
.LBB4_1489:                             #   in Loop: Header=BB4_1482 Depth=1
	movdqa	xmm3, xmm2
	jmp	.LBB4_1481
.LBB4_994:
	xor	esi, esi
.LBB4_995:
	test	r9b, 1
	je	.LBB4_997
# %bb.996:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_16] # xmm3 = <1,1,u,u>
	pandn	xmm0, xmm3
	pcmpeqq	xmm1, xmm2
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pandn	xmm1, xmm3
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
.LBB4_997:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_998:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_998
	jmp	.LBB4_1655
.LBB4_999:
	xor	esi, esi
.LBB4_1000:
	test	r9b, 1
	je	.LBB4_1002
# %bb.1001:
	movq	xmm0, qword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + 4*rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxdq	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	pand	xmm0, xmm4
	pcmpeqd	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxdq	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1002:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1003:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1003
	jmp	.LBB4_1655
.LBB4_1004:
	xor	edi, edi
.LBB4_1005:
	test	r9b, 1
	je	.LBB4_1007
# %bb.1006:
	movupd	xmm0, xmmword ptr [rcx + 8*rdi]
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	andpd	xmm1, xmm0
	orpd	xmm1, xmmword ptr [rip + .LCPI4_1]
	movsd	xmm2, qword ptr [rip + .LCPI4_6] # xmm2 = mem[0],zero
	movapd	xmm3, xmm1
	subsd	xmm3, xmm2
	cvttsd2si	rax, xmm3
	xor	rax, r11
	cvttsd2si	rdx, xmm1
	ucomisd	xmm1, xmm2
	cmovae	rdx, rax
	movq	xmm3, rdx
	pshufd	xmm1, xmm1, 238                 # xmm1 = xmm1[2,3,2,3]
	movdqa	xmm4, xmm1
	subsd	xmm4, xmm2
	cvttsd2si	rax, xmm4
	xor	rax, r11
	cvttsd2si	rdx, xmm1
	ucomisd	xmm1, xmm2
	xorpd	xmm1, xmm1
	cmovae	rdx, rax
	movq	xmm2, rdx
	punpcklqdq	xmm3, xmm2              # xmm3 = xmm3[0],xmm2[0]
	cmpneqpd	xmm1, xmm0
	andpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm1
.LBB4_1007:
	cmp	rsi, r10
	je	.LBB4_1655
.LBB4_1008:
	movapd	xmm0, xmmword ptr [rip + .LCPI4_0] # xmm0 = [-0.0E+0,-0.0E+0]
	movsd	xmm1, qword ptr [rip + .LCPI4_2] # xmm1 = mem[0],zero
	movsd	xmm2, qword ptr [rip + .LCPI4_6] # xmm2 = mem[0],zero
	xor	eax, eax
	xorpd	xmm3, xmm3
.LBB4_1009:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm4, qword ptr [rcx + 8*rsi]   # xmm4 = mem[0],zero
	movapd	xmm5, xmm4
	andpd	xmm5, xmm0
	orpd	xmm5, xmm1
	movapd	xmm6, xmm5
	subsd	xmm6, xmm2
	cvttsd2si	rdx, xmm6
	xor	rdx, r11
	cvttsd2si	rdi, xmm5
	ucomisd	xmm5, xmm2
	cmovae	rdi, rdx
	ucomisd	xmm3, xmm4
	cmove	rdi, rax
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	cmp	r10, rsi
	jne	.LBB4_1009
	jmp	.LBB4_1655
.LBB4_1010:
	xor	esi, esi
.LBB4_1011:
	test	r9b, 1
	je	.LBB4_1013
# %bb.1012:
	movd	xmm0, dword ptr [rcx + 2*rsi]   # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rcx + 2*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxwq	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	pand	xmm0, xmm4
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxwq	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1013:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1014:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1014
	jmp	.LBB4_1655
.LBB4_1015:
	xor	esi, esi
.LBB4_1016:
	test	r9b, 1
	je	.LBB4_1018
# %bb.1017:
	movd	xmm2, dword ptr [rcx + 2*rsi]   # xmm2 = mem[0],zero,zero,zero
	movd	xmm3, dword ptr [rcx + 2*rsi + 4] # xmm3 = mem[0],zero,zero,zero
	xorpd	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	pmovsxwq	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	pmovsxwq	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxwq	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxwq	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	blendvpd	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm3
.LBB4_1018:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1019:
	mov	esi, 1
.LBB4_1020:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rdx]
	xor	eax, eax
	test	di, di
	setne	al
	neg	rax
	test	di, di
	cmovg	rax, rsi
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1020
	jmp	.LBB4_1655
.LBB4_993:
	movmskps	ecx, xmm0
	and	ecx, 1
	neg	ecx
	or	ecx, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, ecx
	movss	xmm1, dword ptr [rip + .LCPI4_9] # xmm1 = mem[0],zero,zero,zero
	movaps	xmm2, xmm0
	subss	xmm2, xmm1
	cvttss2si	rcx, xmm2
	movabs	rdx, -9223372036854775808
	xor	rdx, rcx
	cvttss2si	rcx, xmm0
	ucomiss	xmm0, xmm1
	cmovae	rcx, rdx
	mov	qword ptr [r8 + 8*rax], rcx
	jmp	.LBB4_1655
.LBB4_1021:
	xor	esi, esi
.LBB4_1022:
	test	r9b, 1
	je	.LBB4_1024
# %bb.1023:
	movq	xmm2, qword ptr [rcx + 4*rsi]   # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + 4*rsi + 8] # xmm3 = mem[0],zero
	xorpd	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	pmovsxdq	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	pmovsxdq	xmm1, xmm1
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxdq	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxdq	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	blendvpd	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm3
.LBB4_1024:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1025:
	mov	esi, 1
.LBB4_1026:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rdx]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	rax
	test	edi, edi
	cmovg	rax, rsi
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1026
	jmp	.LBB4_1655
.LBB4_1027:
	xor	esi, esi
.LBB4_1028:
	test	r9b, 1
	je	.LBB4_1030
# %bb.1029:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_17] # xmm4 = <1,1,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqq	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pand	xmm1, xmm4
	movd	dword ptr [r8 + 2*rsi], xmm0
	movd	dword ptr [r8 + 2*rsi + 4], xmm1
.LBB4_1030:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1031:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1031
	jmp	.LBB4_1655
.LBB4_1032:
	xor	esi, esi
.LBB4_1033:
	test	r9b, 1
	je	.LBB4_1035
# %bb.1034:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_17] # xmm4 = <1,1,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqq	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pand	xmm1, xmm4
	movd	dword ptr [r8 + 2*rsi], xmm0
	movd	dword ptr [r8 + 2*rsi + 4], xmm1
.LBB4_1035:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1036:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1036
	jmp	.LBB4_1655
.LBB4_1037:
	xor	esi, esi
.LBB4_1038:
	test	r9b, 1
	je	.LBB4_1040
# %bb.1039:
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm4
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtq	xmm1, xmm4
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pcmpeqq	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	packssdw	xmm2, xmm2
	pcmpeqq	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_17] # xmm4 = <1,1,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movd	dword ptr [r8 + 2*rsi], xmm2
	movd	dword ptr [r8 + 2*rsi + 4], xmm3
.LBB4_1040:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1041:
	mov	esi, 1
.LBB4_1042:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rdx]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	eax
	test	rdi, rdi
	cmovg	eax, esi
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1042
	jmp	.LBB4_1655
.LBB4_1043:
	xor	esi, esi
.LBB4_1044:
	test	r9b, 1
	je	.LBB4_1046
# %bb.1045:
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	packssdw	xmm1, xmm1
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_11] # xmm4 = <1,1,1,1,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
.LBB4_1046:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1047:
	mov	esi, 1
.LBB4_1048:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rdx]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	eax
	test	edi, edi
	cmovg	eax, esi
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1048
	jmp	.LBB4_1655
.LBB4_1049:
	xor	esi, esi
.LBB4_1050:
	test	r9b, 1
	je	.LBB4_1052
# %bb.1051:
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	packssdw	xmm1, xmm1
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_11] # xmm4 = <1,1,1,1,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
.LBB4_1052:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1053:
	mov	esi, 1
.LBB4_1054:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rdx]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	eax
	test	edi, edi
	cmovg	eax, esi
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1054
	jmp	.LBB4_1655
.LBB4_1055:
	xor	esi, esi
.LBB4_1056:
	test	r9b, 1
	je	.LBB4_1058
# %bb.1057:
	movd	xmm0, dword ptr [rcx + 2*rsi]   # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rcx + 2*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxwq	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	pand	xmm0, xmm4
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxwq	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1058:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1059:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1059
	jmp	.LBB4_1655
.LBB4_1060:
	xor	esi, esi
.LBB4_1061:
	test	r9b, 1
	je	.LBB4_1063
# %bb.1062:
	movq	xmm2, qword ptr [rcx + 4*rsi]   # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + 4*rsi + 8] # xmm3 = mem[0],zero
	xorpd	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	pmovsxdq	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	pmovsxdq	xmm1, xmm1
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxdq	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxdq	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	blendvpd	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm3
.LBB4_1063:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1064:
	mov	esi, 1
.LBB4_1065:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rdx]
	xor	eax, eax
	test	edi, edi
	setne	al
	neg	rax
	test	edi, edi
	cmovg	rax, rsi
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1065
	jmp	.LBB4_1655
.LBB4_1066:
	xor	esi, esi
.LBB4_1067:
	test	r9b, 1
	je	.LBB4_1069
# %bb.1068:
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	cvtdq2ps	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	cvtdq2ps	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_19] # xmm4 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1069:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1070:
	movd	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_1072
.LBB4_1071:                             #   in Loop: Header=BB4_1072 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1072:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	movdqa	xmm2, xmm0
	jne	.LBB4_1074
# %bb.1073:                             #   in Loop: Header=BB4_1072 Depth=1
	pxor	xmm2, xmm2
.LBB4_1074:                             #   in Loop: Header=BB4_1072 Depth=1
	movdqa	xmm3, xmm1
	jg	.LBB4_1071
# %bb.1075:                             #   in Loop: Header=BB4_1072 Depth=1
	movdqa	xmm3, xmm2
	jmp	.LBB4_1071
.LBB4_1076:
	xor	edi, edi
.LBB4_1077:
	test	r9b, 1
	je	.LBB4_1079
# %bb.1078:
	movupd	xmm0, xmmword ptr [rcx + 8*rdi]
	movupd	xmm1, xmmword ptr [rcx + 8*rdi + 16]
	xorpd	xmm2, xmm2
	movapd	xmm3, xmm0
	cmpeqpd	xmm3, xmm2
	shufps	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	cmpeqpd	xmm2, xmm1
	shufps	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	movapd	xmm4, xmmword ptr [rip + .LCPI4_0] # xmm4 = [-0.0E+0,-0.0E+0]
	andpd	xmm0, xmm4
	movapd	xmm5, xmmword ptr [rip + .LCPI4_1] # xmm5 = [1.0E+0,1.0E+0]
	orpd	xmm0, xmm5
	andpd	xmm1, xmm4
	orpd	xmm1, xmm5
	cvttpd2dq	xmm0, xmm0
	cvttpd2dq	xmm1, xmm1
	andnps	xmm3, xmm0
	andnps	xmm2, xmm1
	movlhps	xmm3, xmm2                      # xmm3 = xmm3[0],xmm2[0]
	movups	xmmword ptr [r8 + 4*rdi], xmm3
.LBB4_1079:
	cmp	rsi, rax
	je	.LBB4_1655
.LBB4_1080:
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1081:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edx, xmm3
	cmove	edx, r10d
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_1081
	jmp	.LBB4_1655
.LBB4_1082:
	xor	esi, esi
.LBB4_1083:
	test	r9b, 1
	je	.LBB4_1085
# %bb.1084:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_16] # xmm3 = <1,1,u,u>
	pandn	xmm0, xmm3
	pcmpeqq	xmm1, xmm2
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pandn	xmm1, xmm3
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
.LBB4_1085:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1086:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1086
	jmp	.LBB4_1655
.LBB4_1087:
	xor	esi, esi
.LBB4_1088:
	test	r9b, 1
	je	.LBB4_1090
# %bb.1089:
	movq	xmm0, qword ptr [rcx + 2*rsi]   # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + 2*rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxwd	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	pand	xmm0, xmm4
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxwd	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1090:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1091:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1091
	jmp	.LBB4_1655
.LBB4_1092:
	xor	esi, esi
.LBB4_1093:
	test	r9b, 1
	je	.LBB4_1095
# %bb.1094:
	movq	xmm2, qword ptr [rcx + 2*rsi]   # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + 2*rsi + 8] # xmm3 = mem[0],zero
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxwd	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxwd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1095:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1096:
	mov	esi, 1
.LBB4_1097:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rdx]
	xor	eax, eax
	test	di, di
	setne	al
	neg	eax
	test	di, di
	cmovg	eax, esi
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1097
	jmp	.LBB4_1655
.LBB4_1098:
	xor	esi, esi
.LBB4_1099:
	test	r9b, 1
	je	.LBB4_1101
# %bb.1100:
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm4
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm1, xmm3
	pcmpgtq	xmm1, xmm4
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pcmpeqq	xmm2, xmm4
	pshufd	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pcmpeqq	xmm3, xmm4
	pshufd	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	pxor	xmm3, xmm5
	movaps	xmm4, xmmword ptr [rip + .LCPI4_16] # xmm4 = <1,1,u,u>
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movlhps	xmm2, xmm3                      # xmm2 = xmm2[0],xmm3[0]
	movups	xmmword ptr [r8 + 4*rsi], xmm2
.LBB4_1101:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1102:
	mov	esi, 1
.LBB4_1103:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rdx]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	eax
	test	rdi, rdi
	cmovg	eax, esi
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1103
	jmp	.LBB4_1655
.LBB4_1106:
	xor	edi, edi
.LBB4_1107:
	test	r9b, 1
	je	.LBB4_1109
# %bb.1108:
	movupd	xmm3, xmmword ptr [rcx + 8*rdi]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	xorpd	xmm1, xmm1
	movapd	xmm0, xmm3
	cmpeqpd	xmm0, xmm1
	shufps	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	cmpeqpd	xmm1, xmm2
	movapd	xmm4, xmmword ptr [rip + .LCPI4_0] # xmm4 = [-0.0E+0,-0.0E+0]
	andpd	xmm3, xmm4
	movapd	xmm5, xmmword ptr [rip + .LCPI4_1] # xmm5 = [1.0E+0,1.0E+0]
	orpd	xmm3, xmm5
	andpd	xmm2, xmm4
	orpd	xmm2, xmm5
	pshufd	xmm4, xmm3, 238                 # xmm4 = xmm3[2,3,2,3]
	cvttsd2si	rax, xmm4
	cvttsd2si	rdx, xmm3
	movd	xmm3, edx
	pinsrd	xmm3, eax, 1
	pshufd	xmm4, xmm2, 238                 # xmm4 = xmm2[2,3,2,3]
	cvttsd2si	rax, xmm4
	cvttsd2si	rdx, xmm2
	shufps	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	movd	xmm2, edx
	pinsrd	xmm2, eax, 1
	andnps	xmm0, xmm3
	andnps	xmm1, xmm2
	movlhps	xmm0, xmm1                      # xmm0 = xmm0[0],xmm1[0]
	movups	xmmword ptr [r8 + 4*rdi], xmm0
.LBB4_1109:
	cmp	rsi, r11
	je	.LBB4_1655
.LBB4_1110:
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1111:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	rax, xmm3
	cmove	eax, r10d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	cmp	r11, rsi
	jne	.LBB4_1111
	jmp	.LBB4_1655
.LBB4_1112:
	xor	esi, esi
.LBB4_1113:
	test	r9b, 1
	je	.LBB4_1115
# %bb.1114:
	movq	xmm0, qword ptr [rcx + 2*rsi]   # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + 2*rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxwd	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	pand	xmm0, xmm4
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxwd	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1115:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1116:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1116
	jmp	.LBB4_1655
.LBB4_1117:
	xor	esi, esi
.LBB4_1118:
	test	r9b, 1
	je	.LBB4_1120
# %bb.1119:
	movq	xmm2, qword ptr [rcx + 2*rsi]   # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + 2*rsi + 8] # xmm3 = mem[0],zero
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxwd	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxwd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1120:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1121:
	mov	esi, 1
.LBB4_1122:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rdx]
	xor	eax, eax
	test	di, di
	setne	al
	neg	eax
	test	di, di
	cmovg	eax, esi
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1122
	jmp	.LBB4_1655
.LBB4_1123:
	xor	esi, esi
.LBB4_1124:
	test	r9b, 1
	je	.LBB4_1126
# %bb.1125:
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm4
	pshufd	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movdqa	xmm1, xmm3
	pcmpgtq	xmm1, xmm4
	pshufd	xmm1, xmm1, 232                 # xmm1 = xmm1[0,2,2,3]
	pcmpeqq	xmm2, xmm4
	pshufd	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pcmpeqq	xmm3, xmm4
	pshufd	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3]
	pxor	xmm3, xmm5
	movaps	xmm4, xmmword ptr [rip + .LCPI4_16] # xmm4 = <1,1,u,u>
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movlhps	xmm2, xmm3                      # xmm2 = xmm2[0],xmm3[0]
	movups	xmmword ptr [r8 + 4*rsi], xmm2
.LBB4_1126:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1127:
	mov	esi, 1
.LBB4_1128:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rdx]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	eax
	test	rdi, rdi
	cmovg	eax, esi
	mov	dword ptr [r8 + 4*rdx], eax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1128
	jmp	.LBB4_1655
.LBB4_1129:
	xor	esi, esi
.LBB4_1130:
	test	r9b, 1
	je	.LBB4_1132
# %bb.1131:
	movups	xmm0, xmmword ptr [rcx + 4*rsi]
	xorps	xmm1, xmm1
	cmpneqps	xmm1, xmm0
	psrad	xmm0, 31
	por	xmm0, xmmword ptr [rip + .LCPI4_8]
	cvtdq2ps	xmm2, xmm0
	movaps	xmm3, xmmword ptr [rip + .LCPI4_10] # xmm3 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	movaps	xmm0, xmm2
	cmpltps	xmm0, xmm3
	cvttps2dq	xmm4, xmm2
	subps	xmm2, xmm3
	cvttps2dq	xmm2, xmm2
	xorps	xmm2, xmmword ptr [rip + .LCPI4_4]
	blendvps	xmm2, xmm4, xmm0
	andps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rsi], xmm1
.LBB4_1132:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1133:
	xorps	xmm0, xmm0
	jmp	.LBB4_1135
.LBB4_1134:                             #   in Loop: Header=BB4_1135 Depth=1
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1135:                             # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	xor	esi, esi
	ucomiss	xmm0, xmm1
	je	.LBB4_1134
# %bb.1136:                             #   in Loop: Header=BB4_1135 Depth=1
	movmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	xorps	xmm1, xmm1
	cvtsi2ss	xmm1, esi
	cvttss2si	rsi, xmm1
	jmp	.LBB4_1134
.LBB4_1137:
	xor	esi, esi
.LBB4_1138:
	test	r9b, 1
	je	.LBB4_1140
# %bb.1139:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_11] # xmm4 = <1,1,1,1,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqd	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	pand	xmm1, xmm4
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB4_1140:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1141:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1141
	jmp	.LBB4_1655
.LBB4_1142:
	xor	esi, esi
.LBB4_1143:
	test	r9b, 1
	je	.LBB4_1145
# %bb.1144:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_11] # xmm4 = <1,1,1,1,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqd	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	pand	xmm1, xmm4
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [r8 + 2*rsi], xmm0
.LBB4_1145:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1146:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1146
	jmp	.LBB4_1655
.LBB4_1147:
	xor	edi, edi
.LBB4_1148:
	test	r9b, 1
	je	.LBB4_1150
# %bb.1149:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	xorpd	xmm4, xmm4
	movapd	xmm0, xmm2
	cmpeqpd	xmm0, xmm4
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movapd	xmm1, xmm3
	cmpeqpd	xmm1, xmm4
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	movapd	xmm5, xmmword ptr [rip + .LCPI4_0] # xmm5 = [-0.0E+0,-0.0E+0]
	andpd	xmm2, xmm5
	movapd	xmm6, xmmword ptr [rip + .LCPI4_1] # xmm6 = [1.0E+0,1.0E+0]
	orpd	xmm2, xmm6
	andpd	xmm3, xmm5
	orpd	xmm3, xmm6
	cvttpd2dq	xmm2, xmm2
	cvttpd2dq	xmm3, xmm3
	pshuflw	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3,4,5,6,7]
	pshuflw	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3,4,5,6,7]
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movd	dword ptr [r8 + 2*rdi], xmm2
	movd	dword ptr [r8 + 2*rdi + 4], xmm3
.LBB4_1150:
	cmp	rsi, rax
	je	.LBB4_1655
.LBB4_1151:
	pxor	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1152:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edx, xmm3
	cmove	edx, r10d
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_1152
	jmp	.LBB4_1655
.LBB4_1153:
	xor	edi, edi
.LBB4_1154:
	test	r9b, 1
	je	.LBB4_1156
# %bb.1155:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	xorpd	xmm4, xmm4
	movapd	xmm0, xmm2
	cmpeqpd	xmm0, xmm4
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movapd	xmm1, xmm3
	cmpeqpd	xmm1, xmm4
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	movapd	xmm5, xmmword ptr [rip + .LCPI4_0] # xmm5 = [-0.0E+0,-0.0E+0]
	andpd	xmm2, xmm5
	movapd	xmm6, xmmword ptr [rip + .LCPI4_1] # xmm6 = [1.0E+0,1.0E+0]
	orpd	xmm2, xmm6
	andpd	xmm3, xmm5
	orpd	xmm3, xmm6
	cvttpd2dq	xmm2, xmm2
	cvttpd2dq	xmm3, xmm3
	pshuflw	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3,4,5,6,7]
	pshuflw	xmm3, xmm3, 232                 # xmm3 = xmm3[0,2,2,3,4,5,6,7]
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movd	dword ptr [r8 + 2*rdi], xmm2
	movd	dword ptr [r8 + 2*rdi + 4], xmm3
.LBB4_1156:
	cmp	rsi, rax
	je	.LBB4_1655
.LBB4_1157:
	pxor	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1158:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	edx, xmm3
	cmove	edx, r10d
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_1158
	jmp	.LBB4_1655
.LBB4_1159:
	xor	esi, esi
.LBB4_1160:
	test	r9b, 1
	je	.LBB4_1162
# %bb.1161:
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm3, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm4
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtq	xmm1, xmm4
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	pcmpeqq	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	packssdw	xmm2, xmm2
	pcmpeqq	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_17] # xmm4 = <1,1,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movd	dword ptr [r8 + 2*rsi], xmm2
	movd	dword ptr [r8 + 2*rsi + 4], xmm3
.LBB4_1162:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1163:
	mov	esi, 1
.LBB4_1164:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rdx]
	xor	eax, eax
	test	rdi, rdi
	setne	al
	neg	eax
	test	rdi, rdi
	cmovg	eax, esi
	mov	word ptr [r8 + 2*rdx], ax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1164
	jmp	.LBB4_1655
.LBB4_1165:
	xor	edi, edi
.LBB4_1166:
	test	r9b, 1
	je	.LBB4_1168
# %bb.1167:
	movups	xmm0, xmmword ptr [rcx + 4*rdi]
	movups	xmm1, xmmword ptr [rcx + 4*rdi + 16]
	xorps	xmm4, xmm4
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	pcmpeqd	xmm5, xmm5
	pcmpgtd	xmm0, xmm5
	packssdw	xmm0, xmm0
	pcmpgtd	xmm1, xmm5
	packssdw	xmm1, xmm1
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_11] # xmm6 = <1,1,1,1,u,u,u,u>
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	punpcklqdq	xmm7, xmm5              # xmm7 = xmm7[0],xmm5[0]
	movdqu	xmmword ptr [r8 + 2*rdi], xmm7
.LBB4_1168:
	cmp	rsi, rax
	je	.LBB4_1655
.LBB4_1169:
	pxor	xmm0, xmm0
.LBB4_1170:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	movd	edx, xmm1
	xor	edi, edi
	test	edx, edx
	setns	dil
	ucomiss	xmm0, xmm1
	lea	edx, [rdi + rdi - 1]
	cmove	edx, r10d
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_1170
	jmp	.LBB4_1655
.LBB4_1171:
	xor	edi, edi
.LBB4_1172:
	test	r9b, 1
	je	.LBB4_1174
# %bb.1173:
	movups	xmm0, xmmword ptr [rcx + 4*rdi]
	movups	xmm1, xmmword ptr [rcx + 4*rdi + 16]
	xorps	xmm4, xmm4
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	pcmpeqd	xmm5, xmm5
	pcmpgtd	xmm0, xmm5
	packssdw	xmm0, xmm0
	pcmpgtd	xmm1, xmm5
	packssdw	xmm1, xmm1
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_11] # xmm6 = <1,1,1,1,u,u,u,u>
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	punpcklqdq	xmm7, xmm5              # xmm7 = xmm7[0],xmm5[0]
	movdqu	xmmword ptr [r8 + 2*rdi], xmm7
.LBB4_1174:
	cmp	rsi, rax
	je	.LBB4_1655
.LBB4_1175:
	pxor	xmm0, xmm0
.LBB4_1176:                             # =>This Inner Loop Header: Depth=1
	movd	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	movd	edx, xmm1
	xor	edi, edi
	test	edx, edx
	setns	dil
	ucomiss	xmm0, xmm1
	lea	edx, [rdi + rdi - 1]
	cmove	edx, r10d
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_1176
	jmp	.LBB4_1655
.LBB4_1177:
	xor	esi, esi
.LBB4_1178:
	test	r9b, 1
	je	.LBB4_1180
# %bb.1179:
	movq	xmm0, qword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + 4*rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxdq	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	pand	xmm0, xmm4
	pcmpeqd	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxdq	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1180:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1181:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1181
	jmp	.LBB4_1655
.LBB4_1182:
	xor	esi, esi
.LBB4_1183:
	test	r9b, 1
	je	.LBB4_1185
# %bb.1184:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_19] # xmm3 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	pandn	xmm0, xmm3
	pcmpeqd	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1185:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1186:
	movd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_1188
.LBB4_1187:                             #   in Loop: Header=BB4_1188 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1188:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1187
# %bb.1189:                             #   in Loop: Header=BB4_1188 Depth=1
	pxor	xmm1, xmm1
	jmp	.LBB4_1187
.LBB4_1190:
	xor	esi, esi
.LBB4_1191:
	test	r9b, 1
	je	.LBB4_1193
# %bb.1192:
	movupd	xmm0, xmmword ptr [rcx + 8*rsi]
	movupd	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI4_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmm0
	andpd	xmm4, xmm3
	movapd	xmm5, xmmword ptr [rip + .LCPI4_1] # xmm5 = [1.0E+0,1.0E+0]
	orpd	xmm4, xmm5
	andpd	xmm3, xmm1
	orpd	xmm3, xmm5
	cvttsd2si	rdi, xmm4
	movq	xmm5, rdi
	pshufd	xmm4, xmm4, 238                 # xmm4 = xmm4[2,3,2,3]
	cvttsd2si	rdi, xmm4
	movq	xmm4, rdi
	punpcklqdq	xmm5, xmm4              # xmm5 = xmm5[0],xmm4[0]
	cvttsd2si	rdi, xmm3
	movq	xmm4, rdi
	pshufd	xmm3, xmm3, 238                 # xmm3 = xmm3[2,3,2,3]
	cvttsd2si	rdi, xmm3
	movq	xmm3, rdi
	punpcklqdq	xmm4, xmm3              # xmm4 = xmm4[0],xmm3[0]
	cmpneqpd	xmm0, xmm2
	andpd	xmm0, xmm5
	cmpneqpd	xmm1, xmm2
	andpd	xmm1, xmm4
	movupd	xmmword ptr [r8 + 8*rsi], xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1193:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1194:
	xor	esi, esi
	xorpd	xmm0, xmm0
	movapd	xmm1, xmmword ptr [rip + .LCPI4_0] # xmm1 = [-0.0E+0,-0.0E+0]
	movsd	xmm2, qword ptr [rip + .LCPI4_2] # xmm2 = mem[0],zero
.LBB4_1195:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	ucomisd	xmm0, xmm3
	andpd	xmm3, xmm1
	orpd	xmm3, xmm2
	cvttsd2si	rdi, xmm3
	cmove	rdi, rsi
	mov	qword ptr [r8 + 8*rdx], rdi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1195
	jmp	.LBB4_1655
.LBB4_1196:
	xor	esi, esi
.LBB4_1197:
	test	r9b, 1
	je	.LBB4_1199
# %bb.1198:
	movupd	xmm2, xmmword ptr [rcx + 8*rsi]
	movupd	xmm8, xmmword ptr [rcx + 8*rsi + 16]
	xorps	xmm0, xmm0
	cvtsd2ss	xmm3, xmm2
	cmpeqpd	xmm2, xmm0
	shufps	xmm2, xmm2, 232                 # xmm2 = xmm2[0,2,2,3]
	cvtpd2ps	xmm4, xmmword ptr [rip + .LCPI4_1]
	cmpeqpd	xmm0, xmm8
	movsd	xmm5, qword ptr [rcx + 8*rsi + 8] # xmm5 = mem[0],zero
	cvtsd2ss	xmm5, xmm5
	shufps	xmm0, xmm0, 232                 # xmm0 = xmm0[0,2,2,3]
	movaps	xmm6, xmmword ptr [rip + .LCPI4_3] # xmm6 = [NaN,NaN,NaN,NaN]
	movaps	xmm7, xmm6
	andnps	xmm7, xmm5
	movshdup	xmm5, xmm4                      # xmm5 = xmm4[1,1,3,3]
	andps	xmm5, xmm6
	orps	xmm7, xmm5
	movaps	xmm1, xmm6
	andnps	xmm1, xmm3
	andps	xmm4, xmm6
	orps	xmm1, xmm4
	unpcklps	xmm1, xmm7                      # xmm1 = xmm1[0],xmm7[0],xmm1[1],xmm7[1]
	andnps	xmm2, xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 24] # xmm1 = mem[0],zero
	cvtsd2ss	xmm1, xmm1
	movaps	xmm3, xmm6
	andnps	xmm3, xmm1
	orps	xmm3, xmm5
	xorps	xmm1, xmm1
	cvtsd2ss	xmm1, xmm8
	andnps	xmm6, xmm1
	orps	xmm6, xmm4
	unpcklps	xmm6, xmm3                      # xmm6 = xmm6[0],xmm3[0],xmm6[1],xmm3[1]
	andnps	xmm0, xmm6
	movlhps	xmm2, xmm0                      # xmm2 = xmm2[0],xmm0[0]
	movups	xmmword ptr [r8 + 4*rsi], xmm2
.LBB4_1199:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1200:
	xorps	xmm0, xmm0
	movaps	xmm1, xmmword ptr [rip + .LCPI4_4] # xmm1 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	movss	xmm2, dword ptr [rip + .LCPI4_5] # xmm2 = mem[0],zero,zero,zero
	jmp	.LBB4_1202
.LBB4_1201:                             #   in Loop: Header=BB4_1202 Depth=1
	movss	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1202:                             # =>This Inner Loop Header: Depth=1
	movsd	xmm4, qword ptr [rcx + 8*rdx]   # xmm4 = mem[0],zero
	ucomisd	xmm0, xmm4
	xorps	xmm3, xmm3
	je	.LBB4_1201
# %bb.1203:                             #   in Loop: Header=BB4_1202 Depth=1
	xorps	xmm3, xmm3
	cvtsd2ss	xmm3, xmm4
	andps	xmm3, xmm1
	orps	xmm3, xmm2
	jmp	.LBB4_1201
.LBB4_1204:
	xor	esi, esi
.LBB4_1205:
	test	r9b, 1
	je	.LBB4_1207
# %bb.1206:
	movq	xmm0, qword ptr [rcx + 2*rsi]   # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + 2*rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxwd	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	pand	xmm0, xmm4
	cvtdq2ps	xmm0, xmm0
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxwd	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero
	pand	xmm1, xmm4
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [r8 + 4*rsi], xmm0
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1207:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1208:
	movd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_1210
.LBB4_1209:                             #   in Loop: Header=BB4_1210 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1210:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	movdqa	xmm1, xmm0
	jne	.LBB4_1209
# %bb.1211:                             #   in Loop: Header=BB4_1210 Depth=1
	pxor	xmm1, xmm1
	jmp	.LBB4_1209
.LBB4_1212:
	xor	esi, esi
.LBB4_1213:
	test	r9b, 1
	je	.LBB4_1215
# %bb.1214:
	movd	xmm2, dword ptr [rcx + 2*rsi]   # xmm2 = mem[0],zero,zero,zero
	movd	xmm3, dword ptr [rcx + 2*rsi + 4] # xmm3 = mem[0],zero,zero,zero
	xorpd	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	pmovsxwq	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	pmovsxwq	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxwq	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxwq	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	blendvpd	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm3
.LBB4_1215:
	cmp	rdx, r10
	je	.LBB4_1655
.LBB4_1216:
	mov	esi, 1
.LBB4_1217:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rdx]
	xor	eax, eax
	test	di, di
	setne	al
	neg	rax
	test	di, di
	cmovg	rax, rsi
	mov	qword ptr [r8 + 8*rdx], rax
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1217
	jmp	.LBB4_1655
.LBB4_1218:
	xor	esi, esi
.LBB4_1219:
	test	r9b, 1
	je	.LBB4_1221
# %bb.1220:
	movq	xmm2, qword ptr [rcx + 2*rsi]   # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + 2*rsi + 8] # xmm3 = mem[0],zero
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	pmovsxwd	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	pmovsxwd	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxwd	xmm2, xmm2
	cvtdq2ps	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxwd	xmm3, xmm3
	cvtdq2ps	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_19] # xmm4 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1221:
	cmp	rdx, rax
	je	.LBB4_1655
.LBB4_1222:
	movd	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_1224
.LBB4_1223:                             #   in Loop: Header=BB4_1224 Depth=1
	movd	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1655
.LBB4_1224:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	movdqa	xmm2, xmm0
	jne	.LBB4_1226
# %bb.1225:                             #   in Loop: Header=BB4_1224 Depth=1
	pxor	xmm2, xmm2
.LBB4_1226:                             #   in Loop: Header=BB4_1224 Depth=1
	movdqa	xmm3, xmm1
	jg	.LBB4_1223
# %bb.1227:                             #   in Loop: Header=BB4_1224 Depth=1
	movdqa	xmm3, xmm2
	jmp	.LBB4_1223
.LBB4_1104:
	movmskps	ecx, xmm0
	and	ecx, 1
	neg	ecx
	or	ecx, 1
	xorps	xmm0, xmm0
	cvtsi2ss	xmm0, ecx
	cvttss2si	rcx, xmm0
.LBB4_1105:
	mov	qword ptr [r8 + 8*rax], rcx
.LBB4_1655:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	ret
.LBB4_1228:
	xor	esi, esi
.LBB4_1229:
	test	r9b, 1
	je	.LBB4_1231
# %bb.1230:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_8] # xmm3 = [1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqd	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1231:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1232
.LBB4_1236:
	xor	esi, esi
.LBB4_1237:
	test	r9b, 1
	je	.LBB4_1239
# %bb.1238:
	movd	xmm2, dword ptr [rcx + rsi]     # xmm2 = mem[0],zero,zero,zero
	movd	xmm3, dword ptr [rcx + rsi + 4] # xmm3 = mem[0],zero,zero,zero
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbd	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1239:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1240
.LBB4_1245:
	xor	esi, esi
.LBB4_1246:
	test	r9b, 1
	je	.LBB4_1248
# %bb.1247:
	movd	xmm0, dword ptr [rcx + rsi]     # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rcx + rsi + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbd	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero,xmm0[2],zero,zero,zero,xmm0[3],zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	pand	xmm0, xmm4
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbd	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero,xmm1[2],zero,zero,zero,xmm1[3],zero,zero,zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1248:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1249
.LBB4_1253:
	xor	esi, esi
.LBB4_1254:
	test	r9b, 1
	je	.LBB4_1256
# %bb.1255:
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqd	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqd	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm2
	movdqa	xmm2, xmm4
	blendvps	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm4, xmm3, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm4
.LBB4_1256:
	cmp	rdx, r11
	je	.LBB4_1655
	jmp	.LBB4_1257
.LBB4_1262:
	xor	esi, esi
.LBB4_1263:
	test	r9b, 1
	je	.LBB4_1265
# %bb.1264:
	movupd	xmm0, xmmword ptr [rcx + 8*rsi]
	movupd	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	xorpd	xmm2, xmm2
	movapd	xmm3, xmmword ptr [rip + .LCPI4_0] # xmm3 = [-0.0E+0,-0.0E+0]
	movapd	xmm4, xmm0
	andpd	xmm4, xmm3
	movapd	xmm5, xmmword ptr [rip + .LCPI4_1] # xmm5 = [1.0E+0,1.0E+0]
	orpd	xmm4, xmm5
	andpd	xmm3, xmm1
	orpd	xmm3, xmm5
	cmpneqpd	xmm0, xmm2
	andpd	xmm0, xmm4
	cmpneqpd	xmm1, xmm2
	andpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rsi], xmm0
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1265:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1266
.LBB4_1271:
	xor	esi, esi
.LBB4_1272:
	test	r9b, 1
	je	.LBB4_1274
# %bb.1273:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_12] # xmm4 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqd	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pand	xmm1, xmm4
	movd	dword ptr [r8 + rsi], xmm0
	movd	dword ptr [r8 + rsi + 4], xmm1
.LBB4_1274:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1275
.LBB4_1279:
	xor	esi, esi
.LBB4_1280:
	test	r9b, 1
	je	.LBB4_1282
# %bb.1281:
	movupd	xmm3, xmmword ptr [rcx + 8*rsi]
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	xorpd	xmm2, xmm2
	movapd	xmm0, xmm3
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movapd	xmm1, xmm4
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movapd	xmm5, xmmword ptr [rip + .LCPI4_0] # xmm5 = [-0.0E+0,-0.0E+0]
	andpd	xmm3, xmm5
	movapd	xmm6, xmmword ptr [rip + .LCPI4_1] # xmm6 = [1.0E+0,1.0E+0]
	orpd	xmm3, xmm6
	andpd	xmm4, xmm5
	orpd	xmm4, xmm6
	cvttpd2dq	xmm3, xmm3
	movdqa	xmm5, xmmword ptr [rip + .LCPI4_7] # xmm5 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm3, xmm5
	cvttpd2dq	xmm4, xmm4
	pshufb	xmm4, xmm5
	pblendvb	xmm3, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm2, xmm0
	pextrw	word ptr [r8 + rsi], xmm3, 0
	pextrw	word ptr [r8 + rsi + 2], xmm4, 0
.LBB4_1282:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1283
.LBB4_1288:
	xor	eax, eax
.LBB4_1289:
	test	r9b, 1
	je	.LBB4_1291
# %bb.1290:
	movdqu	xmm1, xmmword ptr [rcx + rax]
	movdqu	xmm2, xmmword ptr [rcx + rax + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_22] # xmm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqb	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqb	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm2
	movdqa	xmm2, xmm4
	pblendvb	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm3, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm4
.LBB4_1291:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1292
.LBB4_1297:
	xor	esi, esi
.LBB4_1298:
	test	r9b, 1
	je	.LBB4_1300
# %bb.1299:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_18] # xmm4 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqq	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pextrw	word ptr [r8 + rsi], xmm0, 0
	pand	xmm1, xmm4
	pextrw	word ptr [r8 + rsi + 2], xmm1, 0
.LBB4_1300:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1301
.LBB4_1305:
	xor	esi, esi
.LBB4_1306:
	test	r9b, 1
	je	.LBB4_1308
# %bb.1307:
	movdqu	xmm0, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 2*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packsswb	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_21] # xmm4 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	packsswb	xmm1, xmm1
	pand	xmm1, xmm4
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [r8 + rsi], xmm0
.LBB4_1308:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1309
.LBB4_1313:
	xor	eax, eax
.LBB4_1314:
	test	r9b, 1
	je	.LBB4_1316
# %bb.1315:
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	movdqu	xmm3, xmmword ptr [rcx + 2*rax + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	packsswb	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packsswb	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	packsswb	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_21] # xmm4 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + rax], xmm2
.LBB4_1316:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1317
.LBB4_1322:
	xor	eax, eax
.LBB4_1323:
	test	r9b, 1
	je	.LBB4_1325
# %bb.1324:
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	movdqu	xmm3, xmmword ptr [rcx + 8*rax + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm4
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtq	xmm1, xmm4
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqq	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	pcmpeqq	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_18] # xmm4 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	pextrw	word ptr [r8 + rax], xmm2, 0
	pextrw	word ptr [r8 + rax + 2], xmm3, 0
.LBB4_1325:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1326
.LBB4_1331:
	xor	esi, esi
.LBB4_1332:
	test	r9b, 1
	je	.LBB4_1334
# %bb.1333:
	movups	xmm0, xmmword ptr [rcx + 4*rsi]
	movups	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	xorps	xmm4, xmm4
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pcmpeqd	xmm5, xmm5
	pcmpgtd	xmm0, xmm5
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	pcmpgtd	xmm1, xmm5
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_12] # xmm6 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	movd	dword ptr [r8 + rsi], xmm7
	movd	dword ptr [r8 + rsi + 4], xmm5
.LBB4_1334:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1335
.LBB4_1340:
	xor	esi, esi
.LBB4_1341:
	test	r9b, 1
	je	.LBB4_1343
# %bb.1342:
	movdqu	xmm0, xmmword ptr [rcx + rsi]
	movdqu	xmm1, xmmword ptr [rcx + rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_22] # xmm3 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqb	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + rsi], xmm0
	movdqu	xmmword ptr [r8 + rsi + 16], xmm1
.LBB4_1343:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1344
.LBB4_1348:
	xor	eax, eax
.LBB4_1349:
	test	r9b, 1
	je	.LBB4_1351
# %bb.1350:
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	movdqu	xmm3, xmmword ptr [rcx + 4*rax + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_12] # xmm4 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movd	dword ptr [r8 + rax], xmm2
	movd	dword ptr [r8 + rax + 4], xmm3
.LBB4_1351:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1352
.LBB4_1357:
	xor	esi, esi
.LBB4_1358:
	test	r9b, 1
	je	.LBB4_1360
# %bb.1359:
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm2, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm3, eax
	xorpd	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbq	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbq	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbq	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbq	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	blendvpd	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm3
.LBB4_1360:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1361
.LBB4_1366:
	xor	esi, esi
.LBB4_1367:
	test	r9b, 1
	je	.LBB4_1369
# %bb.1368:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_15] # xmm3 = [1,1]
	pandn	xmm0, xmm3
	pcmpeqq	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1369:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1370
.LBB4_1374:
	xor	esi, esi
.LBB4_1375:
	test	r9b, 1
	je	.LBB4_1377
# %bb.1376:
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqq	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqq	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm2
	movdqa	xmm2, xmm4
	blendvpd	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm4, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm4
.LBB4_1377:
	cmp	rdx, r11
	je	.LBB4_1655
	jmp	.LBB4_1378
.LBB4_1383:
	xor	esi, esi
.LBB4_1384:
	test	r9b, 1
	je	.LBB4_1386
# %bb.1385:
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm0, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm1, eax
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbq	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,zero,zero,zero,zero,xmm0[1],zero,zero,zero,zero,zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	pand	xmm0, xmm4
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbq	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,zero,zero,zero,zero,xmm1[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1386:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1387
.LBB4_1391:
	xor	esi, esi
.LBB4_1392:
	test	r9b, 1
	je	.LBB4_1394
# %bb.1393:
	movq	xmm2, qword ptr [rcx + rsi]     # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + rsi + 8] # xmm3 = mem[0],zero
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbw	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbw	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbw	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbw	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
.LBB4_1394:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1395
.LBB4_1400:
	xor	esi, esi
.LBB4_1401:
	test	r9b, 1
	je	.LBB4_1403
# %bb.1402:
	movq	xmm2, qword ptr [rcx + rsi]     # xmm2 = mem[0],zero
	movq	xmm3, qword ptr [rcx + rsi + 8] # xmm3 = mem[0],zero
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbw	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbw	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbw	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbw	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
.LBB4_1403:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1404
.LBB4_1409:
	xor	esi, esi
.LBB4_1410:
	test	r9b, 1
	je	.LBB4_1412
# %bb.1411:
	movdqu	xmm0, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 2*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_20] # xmm3 = [1,1,1,1,1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqw	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 2*rsi], xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
.LBB4_1412:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1413
.LBB4_1417:
	xor	esi, esi
.LBB4_1418:
	test	r9b, 1
	je	.LBB4_1420
# %bb.1419:
	movdqu	xmm0, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 2*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_20] # xmm3 = [1,1,1,1,1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqw	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 2*rsi], xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
.LBB4_1420:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1421
.LBB4_1425:
	xor	esi, esi
.LBB4_1426:
	test	r9b, 1
	je	.LBB4_1428
# %bb.1427:
	movdqu	xmm1, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rsi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqw	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqw	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm2
	movdqa	xmm2, xmm4
	pblendvb	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm3, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm4
.LBB4_1428:
	cmp	rdx, r11
	je	.LBB4_1655
	jmp	.LBB4_1429
.LBB4_1434:
	xor	esi, esi
.LBB4_1435:
	test	r9b, 1
	je	.LBB4_1437
# %bb.1436:
	movdqu	xmm1, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rsi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtw	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqw	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqw	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtw	xmm1, xmm2
	movdqa	xmm2, xmm4
	pblendvb	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm3, xmm0
	movdqu	xmmword ptr [r8 + 2*rsi], xmm2
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm4
.LBB4_1437:
	cmp	rdx, r11
	je	.LBB4_1655
	jmp	.LBB4_1438
.LBB4_1443:
	xor	esi, esi
.LBB4_1444:
	test	r9b, 1
	je	.LBB4_1446
# %bb.1445:
	movq	xmm0, qword ptr [rcx + rsi]     # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbw	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
	pand	xmm0, xmm4
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbw	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 2*rsi], xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
.LBB4_1446:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1447
.LBB4_1451:
	xor	esi, esi
.LBB4_1452:
	test	r9b, 1
	je	.LBB4_1454
# %bb.1453:
	movq	xmm0, qword ptr [rcx + rsi]     # xmm0 = mem[0],zero
	movq	xmm1, qword ptr [rcx + rsi + 8] # xmm1 = mem[0],zero
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbw	xmm0, xmm0                      # xmm0 = xmm0[0],zero,xmm0[1],zero,xmm0[2],zero,xmm0[3],zero,xmm0[4],zero,xmm0[5],zero,xmm0[6],zero,xmm0[7],zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_20] # xmm4 = [1,1,1,1,1,1,1,1]
	pand	xmm0, xmm4
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbw	xmm1, xmm1                      # xmm1 = xmm1[0],zero,xmm1[1],zero,xmm1[2],zero,xmm1[3],zero,xmm1[4],zero,xmm1[5],zero,xmm1[6],zero,xmm1[7],zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 2*rsi], xmm0
	movdqu	xmmword ptr [r8 + 2*rsi + 16], xmm1
.LBB4_1454:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1455
.LBB4_1459:
	xor	esi, esi
.LBB4_1460:
	test	r9b, 1
	je	.LBB4_1462
# %bb.1461:
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm2, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm3, eax
	xorpd	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbq	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbq	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbq	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbq	xmm3, xmm3
	movapd	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	blendvpd	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm3, xmm4, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm3
.LBB4_1462:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1463
.LBB4_1468:
	xor	esi, esi
.LBB4_1469:
	test	r9b, 1
	je	.LBB4_1471
# %bb.1470:
	movd	xmm2, dword ptr [rcx + rsi]     # xmm2 = mem[0],zero,zero,zero
	movd	xmm3, dword ptr [rcx + rsi + 4] # xmm3 = mem[0],zero,zero,zero
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbd	xmm2, xmm2
	cvtdq2ps	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbd	xmm3, xmm3
	cvtdq2ps	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_19] # xmm4 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1471:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1472
.LBB4_1490:
	xor	esi, esi
.LBB4_1491:
	test	r9b, 1
	je	.LBB4_1493
# %bb.1492:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_15] # xmm3 = [1,1]
	pandn	xmm0, xmm3
	pcmpeqq	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1493:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1494
.LBB4_1498:
	xor	esi, esi
.LBB4_1499:
	test	r9b, 1
	je	.LBB4_1501
# %bb.1500:
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	movdqa	xmm0, xmm4
	pcmpgtq	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqq	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqq	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtq	xmm1, xmm2
	movdqa	xmm2, xmm4
	blendvpd	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	blendvpd	xmm4, xmm3, xmm0
	movupd	xmmword ptr [r8 + 8*rsi], xmm2
	movupd	xmmword ptr [r8 + 8*rsi + 16], xmm4
.LBB4_1501:
	cmp	rdx, r11
	je	.LBB4_1655
	jmp	.LBB4_1502
.LBB4_1507:
	xor	esi, esi
.LBB4_1508:
	test	r9b, 1
	je	.LBB4_1510
# %bb.1509:
	movzx	eax, word ptr [rcx + rsi]
	movd	xmm0, eax
	movzx	eax, word ptr [rcx + rsi + 2]
	movd	xmm1, eax
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbq	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,zero,zero,zero,zero,xmm0[1],zero,zero,zero,zero,zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_15] # xmm4 = [1,1]
	pand	xmm0, xmm4
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbq	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,zero,zero,zero,zero,xmm1[1],zero,zero,zero,zero,zero,zero,zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 8*rsi], xmm0
	movdqu	xmmword ptr [r8 + 8*rsi + 16], xmm1
.LBB4_1510:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1511
.LBB4_1515:
	xor	esi, esi
.LBB4_1516:
	test	r9b, 1
	je	.LBB4_1518
# %bb.1517:
	movd	xmm0, dword ptr [rcx + rsi]     # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rcx + rsi + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbd	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero,xmm0[2],zero,zero,zero,xmm0[3],zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	pand	xmm0, xmm4
	cvtdq2ps	xmm0, xmm0
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbd	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero,xmm1[2],zero,zero,zero,xmm1[3],zero,zero,zero
	pand	xmm1, xmm4
	cvtdq2ps	xmm1, xmm1
	movups	xmmword ptr [r8 + 4*rsi], xmm0
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1518:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1519
.LBB4_1535:
	xor	esi, esi
.LBB4_1536:
	test	r9b, 1
	je	.LBB4_1538
# %bb.1537:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_12] # xmm4 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqd	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pand	xmm1, xmm4
	movd	dword ptr [r8 + rsi], xmm0
	movd	dword ptr [r8 + rsi + 4], xmm1
.LBB4_1538:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1539
.LBB4_1543:
	xor	esi, esi
.LBB4_1544:
	test	r9b, 1
	je	.LBB4_1546
# %bb.1545:
	movupd	xmm3, xmmword ptr [rcx + 8*rsi]
	movupd	xmm4, xmmword ptr [rcx + 8*rsi + 16]
	xorpd	xmm2, xmm2
	movapd	xmm0, xmm3
	cmpeqpd	xmm0, xmm2
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movapd	xmm1, xmm4
	cmpeqpd	xmm1, xmm2
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movapd	xmm5, xmmword ptr [rip + .LCPI4_0] # xmm5 = [-0.0E+0,-0.0E+0]
	andpd	xmm3, xmm5
	movapd	xmm6, xmmword ptr [rip + .LCPI4_1] # xmm6 = [1.0E+0,1.0E+0]
	orpd	xmm3, xmm6
	andpd	xmm4, xmm5
	orpd	xmm4, xmm6
	cvttpd2dq	xmm3, xmm3
	movdqa	xmm5, xmmword ptr [rip + .LCPI4_7] # xmm5 = <0,4,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pshufb	xmm3, xmm5
	cvttpd2dq	xmm4, xmm4
	pshufb	xmm4, xmm5
	pblendvb	xmm3, xmm2, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm2, xmm0
	pextrw	word ptr [r8 + rsi], xmm3, 0
	pextrw	word ptr [r8 + rsi + 2], xmm4, 0
.LBB4_1546:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1547
.LBB4_1552:
	xor	eax, eax
.LBB4_1553:
	test	r9b, 1
	je	.LBB4_1555
# %bb.1554:
	movdqu	xmm1, xmmword ptr [rcx + rax]
	movdqu	xmm2, xmmword ptr [rcx + rax + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_22] # xmm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtb	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqb	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqb	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtb	xmm1, xmm2
	movdqa	xmm2, xmm4
	pblendvb	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm4, xmm3, xmm0
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm4
.LBB4_1555:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1556
.LBB4_1561:
	xor	esi, esi
.LBB4_1562:
	test	r9b, 1
	je	.LBB4_1564
# %bb.1563:
	movdqu	xmm0, xmmword ptr [rcx + 8*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 8*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqq	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_18] # xmm4 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqq	xmm1, xmm2
	pxor	xmm1, xmm3
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pextrw	word ptr [r8 + rsi], xmm0, 0
	pand	xmm1, xmm4
	pextrw	word ptr [r8 + rsi + 2], xmm1, 0
.LBB4_1564:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1565
.LBB4_1569:
	xor	esi, esi
.LBB4_1570:
	test	r9b, 1
	je	.LBB4_1572
# %bb.1571:
	movdqu	xmm0, xmmword ptr [rcx + 2*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 2*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqw	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	packsswb	xmm0, xmm0
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_21] # xmm4 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
	pand	xmm0, xmm4
	pcmpeqw	xmm1, xmm2
	pxor	xmm1, xmm3
	packsswb	xmm1, xmm1
	pand	xmm1, xmm4
	punpcklqdq	xmm0, xmm1              # xmm0 = xmm0[0],xmm1[0]
	movdqu	xmmword ptr [r8 + rsi], xmm0
.LBB4_1572:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1573
.LBB4_1577:
	xor	eax, eax
.LBB4_1578:
	test	r9b, 1
	je	.LBB4_1580
# %bb.1579:
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	movdqu	xmm3, xmmword ptr [rcx + 2*rax + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtw	xmm0, xmm4
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtw	xmm1, xmm4
	packsswb	xmm1, xmm1
	pcmpeqw	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packsswb	xmm2, xmm2
	pcmpeqw	xmm3, xmm4
	pxor	xmm3, xmm5
	packsswb	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_21] # xmm4 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	punpcklqdq	xmm2, xmm3              # xmm2 = xmm2[0],xmm3[0]
	movdqu	xmmword ptr [r8 + rax], xmm2
.LBB4_1580:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1581
.LBB4_1586:
	xor	eax, eax
.LBB4_1587:
	test	r9b, 1
	je	.LBB4_1589
# %bb.1588:
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	movdqu	xmm3, xmmword ptr [rcx + 8*rax + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtq	xmm0, xmm4
	packssdw	xmm0, xmm0
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtq	xmm1, xmm4
	packssdw	xmm1, xmm1
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqq	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	pcmpeqq	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_18] # xmm4 = <1,1,u,u,u,u,u,u,u,u,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	pextrw	word ptr [r8 + rax], xmm2, 0
	pextrw	word ptr [r8 + rax + 2], xmm3, 0
.LBB4_1589:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1590
.LBB4_1595:
	xor	esi, esi
.LBB4_1596:
	test	r9b, 1
	je	.LBB4_1598
# %bb.1597:
	movups	xmm0, xmmword ptr [rcx + 4*rsi]
	movups	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	xorps	xmm4, xmm4
	movaps	xmm2, xmm0
	cmpeqps	xmm2, xmm4
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	movaps	xmm3, xmm1
	cmpeqps	xmm3, xmm4
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	pcmpeqd	xmm5, xmm5
	pcmpgtd	xmm0, xmm5
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	pcmpgtd	xmm1, xmm5
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	movdqa	xmm6, xmmword ptr [rip + .LCPI4_12] # xmm6 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
	pcmpeqd	xmm7, xmm7
	pblendvb	xmm7, xmm6, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm5, xmm6, xmm0
	movdqa	xmm0, xmm2
	pblendvb	xmm7, xmm4, xmm0
	movdqa	xmm0, xmm3
	pblendvb	xmm5, xmm4, xmm0
	movd	dword ptr [r8 + rsi], xmm7
	movd	dword ptr [r8 + rsi + 4], xmm5
.LBB4_1598:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1599
.LBB4_1604:
	xor	esi, esi
.LBB4_1605:
	test	r9b, 1
	je	.LBB4_1607
# %bb.1606:
	movdqu	xmm0, xmmword ptr [rcx + rsi]
	movdqu	xmm1, xmmword ptr [rcx + rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_22] # xmm3 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqb	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + rsi], xmm0
	movdqu	xmmword ptr [r8 + rsi + 16], xmm1
.LBB4_1607:
	cmp	rdx, rax
	je	.LBB4_1655
	jmp	.LBB4_1608
.LBB4_1612:
	xor	eax, eax
.LBB4_1613:
	test	r9b, 1
	je	.LBB4_1615
# %bb.1614:
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	movdqu	xmm3, xmmword ptr [rcx + 4*rax + 16]
	pxor	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtd	xmm0, xmm4
	packssdw	xmm0, xmm0
	packsswb	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtd	xmm1, xmm4
	packssdw	xmm1, xmm1
	packsswb	xmm1, xmm1
	pcmpeqd	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	packssdw	xmm2, xmm2
	packsswb	xmm2, xmm2
	pcmpeqd	xmm3, xmm4
	pxor	xmm3, xmm5
	packssdw	xmm3, xmm3
	packsswb	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_12] # xmm4 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
	pblendvb	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	pblendvb	xmm3, xmm4, xmm0
	movd	dword ptr [r8 + rax], xmm2
	movd	dword ptr [r8 + rax + 4], xmm3
.LBB4_1615:
	cmp	rsi, r10
	je	.LBB4_1655
	jmp	.LBB4_1616
.LBB4_1621:
	xor	esi, esi
.LBB4_1622:
	test	r9b, 1
	je	.LBB4_1624
# %bb.1623:
	movdqu	xmm0, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm2, xmm2
	pcmpeqd	xmm0, xmm2
	movdqa	xmm3, xmmword ptr [rip + .LCPI4_8] # xmm3 = [1,1,1,1]
	pandn	xmm0, xmm3
	pcmpeqd	xmm1, xmm2
	pandn	xmm1, xmm3
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1624:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1625
.LBB4_1629:
	xor	esi, esi
.LBB4_1630:
	test	r9b, 1
	je	.LBB4_1632
# %bb.1631:
	movd	xmm2, dword ptr [rcx + rsi]     # xmm2 = mem[0],zero,zero,zero
	movd	xmm3, dword ptr [rcx + rsi + 4] # xmm3 = mem[0],zero,zero,zero
	xorps	xmm4, xmm4
	movdqa	xmm0, xmm2
	pcmpgtb	xmm0, xmm4
	pmovsxbd	xmm0, xmm0
	movdqa	xmm1, xmm3
	pcmpgtb	xmm1, xmm4
	pmovsxbd	xmm1, xmm1
	pcmpeqb	xmm2, xmm4
	pcmpeqd	xmm5, xmm5
	pxor	xmm2, xmm5
	pmovsxbd	xmm2, xmm2
	pcmpeqb	xmm3, xmm4
	pxor	xmm3, xmm5
	pmovsxbd	xmm3, xmm3
	movaps	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	blendvps	xmm2, xmm4, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm3, xmm4, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm3
.LBB4_1632:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1633
.LBB4_1638:
	xor	esi, esi
.LBB4_1639:
	test	r9b, 1
	je	.LBB4_1641
# %bb.1640:
	movd	xmm0, dword ptr [rcx + rsi]     # xmm0 = mem[0],zero,zero,zero
	movd	xmm1, dword ptr [rcx + rsi + 4] # xmm1 = mem[0],zero,zero,zero
	pxor	xmm2, xmm2
	pcmpeqb	xmm0, xmm2
	pcmpeqd	xmm3, xmm3
	pxor	xmm0, xmm3
	pmovzxbd	xmm0, xmm0                      # xmm0 = xmm0[0],zero,zero,zero,xmm0[1],zero,zero,zero,xmm0[2],zero,zero,zero,xmm0[3],zero,zero,zero
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	pand	xmm0, xmm4
	pcmpeqb	xmm1, xmm2
	pxor	xmm1, xmm3
	pmovzxbd	xmm1, xmm1                      # xmm1 = xmm1[0],zero,zero,zero,xmm1[1],zero,zero,zero,xmm1[2],zero,zero,zero,xmm1[3],zero,zero,zero
	pand	xmm1, xmm4
	movdqu	xmmword ptr [r8 + 4*rsi], xmm0
	movdqu	xmmword ptr [r8 + 4*rsi + 16], xmm1
.LBB4_1641:
	cmp	rdx, r10
	je	.LBB4_1655
	jmp	.LBB4_1642
.LBB4_1646:
	xor	esi, esi
.LBB4_1647:
	test	r9b, 1
	je	.LBB4_1649
# %bb.1648:
	movdqu	xmm1, xmmword ptr [rcx + 4*rsi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rsi + 16]
	pxor	xmm3, xmm3
	movdqa	xmm4, xmmword ptr [rip + .LCPI4_8] # xmm4 = [1,1,1,1]
	movdqa	xmm0, xmm4
	pcmpgtd	xmm0, xmm1
	movdqa	xmm5, xmm1
	pcmpeqd	xmm5, xmm3
	pcmpeqd	xmm1, xmm1
	pxor	xmm5, xmm1
	pcmpeqd	xmm3, xmm2
	pxor	xmm3, xmm1
	movdqa	xmm1, xmm4
	pcmpgtd	xmm1, xmm2
	movdqa	xmm2, xmm4
	blendvps	xmm2, xmm5, xmm0
	movdqa	xmm0, xmm1
	blendvps	xmm4, xmm3, xmm0
	movups	xmmword ptr [r8 + 4*rsi], xmm2
	movups	xmmword ptr [r8 + 4*rsi + 16], xmm4
.LBB4_1649:
	cmp	rdx, r11
	je	.LBB4_1655
	jmp	.LBB4_1650
.Lfunc_end4:
	.size	arithmetic_unary_diff_type_sse4, .Lfunc_end4-arithmetic_unary_diff_type_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
