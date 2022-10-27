	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4                               # -- Begin function arithmetic_sse4
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
	.globl	arithmetic_sse4
	.p2align	4, 0x90
	.type	arithmetic_sse4,@function
arithmetic_sse4:                        # @arithmetic_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 3
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
	cmp	sil, 4
	je	.LBB0_194
# %bb.12:
	cmp	sil, 5
	je	.LBB0_540
# %bb.13:
	cmp	sil, 6
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
	.size	arithmetic_sse4, .Lfunc_end0-arithmetic_sse4
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
	cmp	sil, 3
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
	cmp	sil, 4
	je	.LBB1_39
# %bb.13:
	cmp	sil, 5
	je	.LBB1_47
# %bb.14:
	cmp	sil, 6
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
	cmp	sil, 3
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
	cmp	sil, 4
	je	.LBB2_39
# %bb.13:
	cmp	sil, 5
	je	.LBB2_47
# %bb.14:
	cmp	sil, 6
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
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
