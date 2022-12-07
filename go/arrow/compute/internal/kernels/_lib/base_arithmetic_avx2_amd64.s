	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function arithmetic_binary_avx2
.LCPI0_0:
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
	.globl	arithmetic_binary_avx2
	.p2align	4, 0x90
	.type	arithmetic_binary_avx2,@function
arithmetic_binary_avx2:                 # @arithmetic_binary_avx2
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
	je	.LBB0_287
# %bb.3:
	cmp	sil, 2
	jne	.LBB0_825
# %bb.4:
	cmp	edi, 6
	jg	.LBB0_559
# %bb.5:
	cmp	edi, 3
	jle	.LBB0_6
# %bb.553:
	cmp	edi, 4
	je	.LBB0_602
# %bb.554:
	cmp	edi, 5
	je	.LBB0_614
# %bb.555:
	cmp	edi, 6
	jne	.LBB0_825
# %bb.556:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.557:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_626
# %bb.558:
	xor	esi, esi
.LBB0_631:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_633
.LBB0_632:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_632
.LBB0_633:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_634:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_634
	jmp	.LBB0_825
.LBB0_11:
	cmp	sil, 21
	je	.LBB0_154
# %bb.12:
	cmp	sil, 22
	je	.LBB0_420
# %bb.13:
	cmp	sil, 23
	jne	.LBB0_825
# %bb.14:
	cmp	edi, 6
	jg	.LBB0_695
# %bb.15:
	cmp	edi, 3
	jle	.LBB0_16
# %bb.689:
	cmp	edi, 4
	je	.LBB0_738
# %bb.690:
	cmp	edi, 5
	je	.LBB0_750
# %bb.691:
	cmp	edi, 6
	jne	.LBB0_825
# %bb.692:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.693:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_762
# %bb.694:
	xor	esi, esi
.LBB0_767:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_769
.LBB0_768:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_768
.LBB0_769:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_770:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_770
	jmp	.LBB0_825
.LBB0_21:
	cmp	edi, 6
	jg	.LBB0_34
# %bb.22:
	cmp	edi, 3
	jle	.LBB0_23
# %bb.28:
	cmp	edi, 4
	je	.LBB0_67
# %bb.29:
	cmp	edi, 5
	je	.LBB0_79
# %bb.30:
	cmp	edi, 6
	jne	.LBB0_825
# %bb.31:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.32:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_91
# %bb.33:
	xor	esi, esi
	jmp	.LBB0_96
.LBB0_287:
	cmp	edi, 6
	jg	.LBB0_300
# %bb.288:
	cmp	edi, 3
	jle	.LBB0_289
# %bb.294:
	cmp	edi, 4
	je	.LBB0_333
# %bb.295:
	cmp	edi, 5
	je	.LBB0_345
# %bb.296:
	cmp	edi, 6
	jne	.LBB0_825
# %bb.297:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.298:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_357
# %bb.299:
	xor	esi, esi
.LBB0_362:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_364
.LBB0_363:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_363
.LBB0_364:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_365:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_365
	jmp	.LBB0_825
.LBB0_154:
	cmp	edi, 6
	jg	.LBB0_167
# %bb.155:
	cmp	edi, 3
	jle	.LBB0_156
# %bb.161:
	cmp	edi, 4
	je	.LBB0_200
# %bb.162:
	cmp	edi, 5
	je	.LBB0_212
# %bb.163:
	cmp	edi, 6
	jne	.LBB0_825
# %bb.164:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.165:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_224
# %bb.166:
	xor	esi, esi
	jmp	.LBB0_229
.LBB0_420:
	cmp	edi, 6
	jg	.LBB0_433
# %bb.421:
	cmp	edi, 3
	jle	.LBB0_422
# %bb.427:
	cmp	edi, 4
	je	.LBB0_466
# %bb.428:
	cmp	edi, 5
	je	.LBB0_478
# %bb.429:
	cmp	edi, 6
	jne	.LBB0_825
# %bb.430:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.431:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_490
# %bb.432:
	xor	esi, esi
.LBB0_495:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_497
.LBB0_496:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_496
.LBB0_497:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_498:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_498
	jmp	.LBB0_825
.LBB0_559:
	cmp	edi, 8
	jle	.LBB0_560
# %bb.565:
	cmp	edi, 9
	je	.LBB0_656
# %bb.566:
	cmp	edi, 11
	je	.LBB0_668
# %bb.567:
	cmp	edi, 12
	jne	.LBB0_825
# %bb.568:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.569:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_680
# %bb.570:
	xor	esi, esi
.LBB0_685:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_687
.LBB0_686:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_686
.LBB0_687:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_688:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_688
	jmp	.LBB0_825
.LBB0_695:
	cmp	edi, 8
	jle	.LBB0_696
# %bb.701:
	cmp	edi, 9
	je	.LBB0_792
# %bb.702:
	cmp	edi, 11
	je	.LBB0_804
# %bb.703:
	cmp	edi, 12
	jne	.LBB0_825
# %bb.704:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.705:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_816
# %bb.706:
	xor	esi, esi
.LBB0_821:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_823
.LBB0_822:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vmulsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_822
.LBB0_823:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_824:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_824
	jmp	.LBB0_825
.LBB0_34:
	cmp	edi, 8
	jle	.LBB0_35
# %bb.40:
	cmp	edi, 9
	je	.LBB0_121
# %bb.41:
	cmp	edi, 11
	je	.LBB0_133
# %bb.42:
	cmp	edi, 12
	jne	.LBB0_825
# %bb.43:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.44:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_145
# %bb.45:
	xor	esi, esi
	jmp	.LBB0_150
.LBB0_300:
	cmp	edi, 8
	jle	.LBB0_301
# %bb.306:
	cmp	edi, 9
	je	.LBB0_387
# %bb.307:
	cmp	edi, 11
	je	.LBB0_399
# %bb.308:
	cmp	edi, 12
	jne	.LBB0_825
# %bb.309:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.310:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_411
# %bb.311:
	xor	esi, esi
.LBB0_416:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_418
.LBB0_417:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_417
.LBB0_418:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_419:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_419
	jmp	.LBB0_825
.LBB0_167:
	cmp	edi, 8
	jle	.LBB0_168
# %bb.173:
	cmp	edi, 9
	je	.LBB0_254
# %bb.174:
	cmp	edi, 11
	je	.LBB0_266
# %bb.175:
	cmp	edi, 12
	jne	.LBB0_825
# %bb.176:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.177:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_278
# %bb.178:
	xor	esi, esi
	jmp	.LBB0_283
.LBB0_433:
	cmp	edi, 8
	jle	.LBB0_434
# %bb.439:
	cmp	edi, 9
	je	.LBB0_520
# %bb.440:
	cmp	edi, 11
	je	.LBB0_532
# %bb.441:
	cmp	edi, 12
	jne	.LBB0_825
# %bb.442:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.443:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_544
# %bb.444:
	xor	esi, esi
.LBB0_549:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_551
.LBB0_550:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_550
.LBB0_551:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_552:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_552
	jmp	.LBB0_825
.LBB0_6:
	cmp	edi, 2
	je	.LBB0_571
# %bb.7:
	cmp	edi, 3
	jne	.LBB0_825
# %bb.8:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.9:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_588
# %bb.10:
	xor	edi, edi
	jmp	.LBB0_598
.LBB0_16:
	cmp	edi, 2
	je	.LBB0_707
# %bb.17:
	cmp	edi, 3
	jne	.LBB0_825
# %bb.18:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.19:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_724
# %bb.20:
	xor	edi, edi
	jmp	.LBB0_734
.LBB0_23:
	cmp	edi, 2
	je	.LBB0_46
# %bb.24:
	cmp	edi, 3
	jne	.LBB0_825
# %bb.25:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.26:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_58
# %bb.27:
	xor	esi, esi
	jmp	.LBB0_63
.LBB0_289:
	cmp	edi, 2
	je	.LBB0_312
# %bb.290:
	cmp	edi, 3
	jne	.LBB0_825
# %bb.291:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.292:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_324
# %bb.293:
	xor	esi, esi
	jmp	.LBB0_329
.LBB0_156:
	cmp	edi, 2
	je	.LBB0_179
# %bb.157:
	cmp	edi, 3
	jne	.LBB0_825
# %bb.158:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.159:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_191
# %bb.160:
	xor	esi, esi
	jmp	.LBB0_196
.LBB0_422:
	cmp	edi, 2
	je	.LBB0_445
# %bb.423:
	cmp	edi, 3
	jne	.LBB0_825
# %bb.424:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.425:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_457
# %bb.426:
	xor	esi, esi
	jmp	.LBB0_462
.LBB0_560:
	cmp	edi, 7
	je	.LBB0_635
# %bb.561:
	cmp	edi, 8
	jne	.LBB0_825
# %bb.562:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.563:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_647
# %bb.564:
	xor	esi, esi
	jmp	.LBB0_652
.LBB0_696:
	cmp	edi, 7
	je	.LBB0_771
# %bb.697:
	cmp	edi, 8
	jne	.LBB0_825
# %bb.698:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.699:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_783
# %bb.700:
	xor	esi, esi
	jmp	.LBB0_788
.LBB0_35:
	cmp	edi, 7
	je	.LBB0_100
# %bb.36:
	cmp	edi, 8
	jne	.LBB0_825
# %bb.37:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.38:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_112
# %bb.39:
	xor	esi, esi
	jmp	.LBB0_117
.LBB0_301:
	cmp	edi, 7
	je	.LBB0_366
# %bb.302:
	cmp	edi, 8
	jne	.LBB0_825
# %bb.303:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.304:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_378
# %bb.305:
	xor	esi, esi
	jmp	.LBB0_383
.LBB0_168:
	cmp	edi, 7
	je	.LBB0_233
# %bb.169:
	cmp	edi, 8
	jne	.LBB0_825
# %bb.170:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.171:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_245
# %bb.172:
	xor	esi, esi
	jmp	.LBB0_250
.LBB0_434:
	cmp	edi, 7
	je	.LBB0_499
# %bb.435:
	cmp	edi, 8
	jne	.LBB0_825
# %bb.436:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.437:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_511
# %bb.438:
	xor	esi, esi
	jmp	.LBB0_516
.LBB0_602:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.603:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_605
# %bb.604:
	xor	esi, esi
.LBB0_610:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_612
.LBB0_611:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_611
.LBB0_612:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_613:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_613
	jmp	.LBB0_825
.LBB0_614:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.615:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_617
# %bb.616:
	xor	esi, esi
.LBB0_622:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_624
.LBB0_623:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_623
.LBB0_624:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_625:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_625
	jmp	.LBB0_825
.LBB0_738:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.739:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_741
# %bb.740:
	xor	esi, esi
.LBB0_746:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_748
.LBB0_747:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_747
.LBB0_748:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_749:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_749
	jmp	.LBB0_825
.LBB0_750:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.751:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_753
# %bb.752:
	xor	esi, esi
.LBB0_758:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_760
.LBB0_759:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_759
.LBB0_760:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_761:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_761
	jmp	.LBB0_825
.LBB0_67:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.68:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_70
# %bb.69:
	xor	esi, esi
	jmp	.LBB0_75
.LBB0_79:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.80:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_82
# %bb.81:
	xor	esi, esi
	jmp	.LBB0_87
.LBB0_333:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.334:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_336
# %bb.335:
	xor	esi, esi
.LBB0_341:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_343
.LBB0_342:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_342
.LBB0_343:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_344:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_344
	jmp	.LBB0_825
.LBB0_345:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.346:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_348
# %bb.347:
	xor	esi, esi
.LBB0_353:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_355
.LBB0_354:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_354
.LBB0_355:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_356:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_356
	jmp	.LBB0_825
.LBB0_200:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.201:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_203
# %bb.202:
	xor	esi, esi
	jmp	.LBB0_208
.LBB0_212:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.213:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_215
# %bb.214:
	xor	esi, esi
	jmp	.LBB0_220
.LBB0_466:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.467:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_469
# %bb.468:
	xor	esi, esi
.LBB0_474:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_476
.LBB0_475:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_475
.LBB0_476:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_477:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_477
	jmp	.LBB0_825
.LBB0_478:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.479:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_481
# %bb.480:
	xor	esi, esi
.LBB0_486:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_488
.LBB0_487:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_487
.LBB0_488:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_489:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_489
	jmp	.LBB0_825
.LBB0_656:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.657:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_659
# %bb.658:
	xor	esi, esi
.LBB0_664:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_666
.LBB0_665:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_665
.LBB0_666:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_667:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_667
	jmp	.LBB0_825
.LBB0_668:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.669:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_671
# %bb.670:
	xor	esi, esi
.LBB0_676:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_678
.LBB0_677:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_677
.LBB0_678:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_679:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_679
	jmp	.LBB0_825
.LBB0_792:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.793:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_795
# %bb.794:
	xor	esi, esi
.LBB0_800:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_802
.LBB0_801:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_801
.LBB0_802:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_803:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_803
	jmp	.LBB0_825
.LBB0_804:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.805:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_807
# %bb.806:
	xor	esi, esi
.LBB0_812:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_814
.LBB0_813:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmulss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_813
.LBB0_814:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_815:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_815
	jmp	.LBB0_825
.LBB0_121:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.122:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_124
# %bb.123:
	xor	esi, esi
	jmp	.LBB0_129
.LBB0_133:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.134:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_136
# %bb.135:
	xor	esi, esi
	jmp	.LBB0_141
.LBB0_387:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.388:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_390
# %bb.389:
	xor	esi, esi
.LBB0_395:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_397
.LBB0_396:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_396
.LBB0_397:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_398:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_398
	jmp	.LBB0_825
.LBB0_399:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.400:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_402
# %bb.401:
	xor	esi, esi
.LBB0_407:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_409
.LBB0_408:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_408
.LBB0_409:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_410:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_410
	jmp	.LBB0_825
.LBB0_254:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.255:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_257
# %bb.256:
	xor	esi, esi
	jmp	.LBB0_262
.LBB0_266:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.267:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_269
# %bb.268:
	xor	esi, esi
	jmp	.LBB0_274
.LBB0_520:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.521:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_523
# %bb.522:
	xor	esi, esi
.LBB0_528:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_530
.LBB0_529:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_529
.LBB0_530:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_531:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_531
	jmp	.LBB0_825
.LBB0_532:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.533:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_535
# %bb.534:
	xor	esi, esi
.LBB0_540:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_542
.LBB0_541:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_541
.LBB0_542:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_543:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_543
	jmp	.LBB0_825
.LBB0_571:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.572:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_574
# %bb.573:
	xor	edi, edi
	jmp	.LBB0_584
.LBB0_707:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.708:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_710
# %bb.709:
	xor	edi, edi
	jmp	.LBB0_720
.LBB0_46:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.47:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_49
# %bb.48:
	xor	esi, esi
	jmp	.LBB0_54
.LBB0_312:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.313:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_315
# %bb.314:
	xor	esi, esi
	jmp	.LBB0_320
.LBB0_179:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.180:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_182
# %bb.181:
	xor	esi, esi
	jmp	.LBB0_187
.LBB0_445:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.446:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_448
# %bb.447:
	xor	esi, esi
	jmp	.LBB0_453
.LBB0_635:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.636:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_638
# %bb.637:
	xor	esi, esi
	jmp	.LBB0_643
.LBB0_771:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.772:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_774
# %bb.773:
	xor	esi, esi
	jmp	.LBB0_779
.LBB0_100:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.101:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_103
# %bb.102:
	xor	esi, esi
	jmp	.LBB0_108
.LBB0_366:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.367:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_369
# %bb.368:
	xor	esi, esi
	jmp	.LBB0_374
.LBB0_233:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.234:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_236
# %bb.235:
	xor	esi, esi
	jmp	.LBB0_241
.LBB0_499:
	test	r9d, r9d
	jle	.LBB0_825
# %bb.500:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_502
# %bb.501:
	xor	esi, esi
	jmp	.LBB0_507
.LBB0_91:
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
	jne	.LBB0_96
# %bb.92:
	and	al, dil
	jne	.LBB0_96
# %bb.93:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_94:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_94
# %bb.95:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_96:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_98
.LBB0_97:                               # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_97
.LBB0_98:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_99:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_99
	jmp	.LBB0_825
.LBB0_224:
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
	jne	.LBB0_229
# %bb.225:
	and	al, dil
	jne	.LBB0_229
# %bb.226:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_227:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_227
# %bb.228:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_229:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_231
.LBB0_230:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_230
.LBB0_231:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_232:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_232
	jmp	.LBB0_825
.LBB0_145:
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
	jne	.LBB0_150
# %bb.146:
	and	al, dil
	jne	.LBB0_150
# %bb.147:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_148:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_148
# %bb.149:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_150:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_152
.LBB0_151:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_151
.LBB0_152:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_153:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_153
	jmp	.LBB0_825
.LBB0_278:
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
	jne	.LBB0_283
# %bb.279:
	and	al, dil
	jne	.LBB0_283
# %bb.280:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_281:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_281
# %bb.282:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_283:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_285
.LBB0_284:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_284
.LBB0_285:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_286:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_286
	jmp	.LBB0_825
.LBB0_588:
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
# %bb.589:
	and	al, sil
	jne	.LBB0_598
# %bb.590:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_592
# %bb.591:
	xor	esi, esi
	jmp	.LBB0_594
.LBB0_724:
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
	jne	.LBB0_734
# %bb.725:
	and	al, sil
	jne	.LBB0_734
# %bb.726:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_728
# %bb.727:
	xor	esi, esi
	jmp	.LBB0_730
.LBB0_58:
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
	jne	.LBB0_63
# %bb.59:
	and	al, dil
	jne	.LBB0_63
# %bb.60:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_61:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_61
# %bb.62:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_63:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_65
.LBB0_64:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_64
.LBB0_65:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_66:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_66
	jmp	.LBB0_825
.LBB0_324:
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
	jne	.LBB0_329
# %bb.325:
	and	al, dil
	jne	.LBB0_329
# %bb.326:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_327:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_327
# %bb.328:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_329:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_331
.LBB0_330:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_330
.LBB0_331:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_332:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_332
	jmp	.LBB0_825
.LBB0_191:
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
	jne	.LBB0_196
# %bb.192:
	and	al, dil
	jne	.LBB0_196
# %bb.193:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_194:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_194
# %bb.195:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_196:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_198
.LBB0_197:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_197
.LBB0_198:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_199:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_199
	jmp	.LBB0_825
.LBB0_457:
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
	jne	.LBB0_462
# %bb.458:
	and	al, dil
	jne	.LBB0_462
# %bb.459:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_460:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_460
# %bb.461:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_462:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_464
.LBB0_463:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_463
.LBB0_464:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_465:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_465
	jmp	.LBB0_825
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
	jne	.LBB0_652
# %bb.648:
	and	al, dil
	jne	.LBB0_652
# %bb.649:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_650:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_650
# %bb.651:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_652:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_654
.LBB0_653:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_653
.LBB0_654:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_655:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_655
	jmp	.LBB0_825
.LBB0_783:
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
	jne	.LBB0_788
# %bb.784:
	and	al, dil
	jne	.LBB0_788
# %bb.785:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_786:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_786
# %bb.787:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_788:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_790
.LBB0_789:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	imul	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_789
.LBB0_790:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_791:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_791
	jmp	.LBB0_825
.LBB0_112:
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
# %bb.113:
	and	al, dil
	jne	.LBB0_117
# %bb.114:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_115:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_115
# %bb.116:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_117:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_119
.LBB0_118:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_118
.LBB0_119:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_120:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_120
	jmp	.LBB0_825
.LBB0_378:
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
	jne	.LBB0_383
# %bb.379:
	and	al, dil
	jne	.LBB0_383
# %bb.380:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_381:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_381
# %bb.382:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_383:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_385
.LBB0_384:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_384
.LBB0_385:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_386:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_386
	jmp	.LBB0_825
.LBB0_245:
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
	jne	.LBB0_250
# %bb.246:
	and	al, dil
	jne	.LBB0_250
# %bb.247:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_248:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_248
# %bb.249:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_250:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_252
.LBB0_251:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_251
.LBB0_252:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_253:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_253
	jmp	.LBB0_825
.LBB0_511:
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
	jne	.LBB0_516
# %bb.512:
	and	al, dil
	jne	.LBB0_516
# %bb.513:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_514:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_514
# %bb.515:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_516:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_518
.LBB0_517:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	sub	rdi, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_517
.LBB0_518:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_519:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_519
	jmp	.LBB0_825
.LBB0_70:
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
	jne	.LBB0_75
# %bb.71:
	and	al, dil
	jne	.LBB0_75
# %bb.72:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_73:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_73
# %bb.74:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_75:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_77
.LBB0_76:                               # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_76
.LBB0_77:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_78:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_78
	jmp	.LBB0_825
.LBB0_82:
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
# %bb.83:
	and	al, dil
	jne	.LBB0_87
# %bb.84:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_85:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_85
# %bb.86:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_87:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_89
.LBB0_88:                               # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_88
.LBB0_89:
	cmp	r9, 3
	jb	.LBB0_825
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
	jmp	.LBB0_825
.LBB0_203:
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
	jne	.LBB0_208
# %bb.204:
	and	al, dil
	jne	.LBB0_208
# %bb.205:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_206:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_206
# %bb.207:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_208:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_210
.LBB0_209:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_209
.LBB0_210:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_211:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_211
	jmp	.LBB0_825
.LBB0_215:
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
	jne	.LBB0_220
# %bb.216:
	and	al, dil
	jne	.LBB0_220
# %bb.217:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_218:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_218
# %bb.219:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_220:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_222
.LBB0_221:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_221
.LBB0_222:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_223:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_223
	jmp	.LBB0_825
.LBB0_124:
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
	jne	.LBB0_129
# %bb.125:
	and	al, dil
	jne	.LBB0_129
# %bb.126:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_127:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_127
# %bb.128:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_129:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_131
.LBB0_130:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_130
.LBB0_131:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_132:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_132
	jmp	.LBB0_825
.LBB0_136:
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
	jne	.LBB0_141
# %bb.137:
	and	al, dil
	jne	.LBB0_141
# %bb.138:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_139:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_139
# %bb.140:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_141:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_143
.LBB0_142:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_142
.LBB0_143:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_144:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_144
	jmp	.LBB0_825
.LBB0_257:
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
	jne	.LBB0_262
# %bb.258:
	and	al, dil
	jne	.LBB0_262
# %bb.259:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_260:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_260
# %bb.261:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_262:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_264
.LBB0_263:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*rsi]
	add	rdi, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_263
.LBB0_264:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_265:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_265
	jmp	.LBB0_825
.LBB0_269:
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
	jne	.LBB0_274
# %bb.270:
	and	al, dil
	jne	.LBB0_274
# %bb.271:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_272:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_272
# %bb.273:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_274:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_276
.LBB0_275:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_275
.LBB0_276:
	cmp	rdi, 3
	jb	.LBB0_825
.LBB0_277:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_277
	jmp	.LBB0_825
.LBB0_574:
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
	jne	.LBB0_584
# %bb.575:
	and	al, sil
	jne	.LBB0_584
# %bb.576:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_578
# %bb.577:
	xor	esi, esi
	jmp	.LBB0_580
.LBB0_710:
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
	jne	.LBB0_720
# %bb.711:
	and	al, sil
	jne	.LBB0_720
# %bb.712:
	mov	edi, r10d
	and	edi, -32
	lea	rsi, [rdi - 32]
	mov	rax, rsi
	shr	rax, 5
	add	rax, 1
	mov	r9d, eax
	and	r9d, 3
	cmp	rsi, 96
	jae	.LBB0_714
# %bb.713:
	xor	esi, esi
	jmp	.LBB0_716
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
	jne	.LBB0_54
# %bb.50:
	and	al, dil
	jne	.LBB0_54
# %bb.51:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_52:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_52
# %bb.53:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_54:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_56
.LBB0_55:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_55
.LBB0_56:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_57:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_57
	jmp	.LBB0_825
.LBB0_315:
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
	jne	.LBB0_320
# %bb.316:
	and	al, dil
	jne	.LBB0_320
# %bb.317:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_318:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_318
# %bb.319:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_320:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_322
.LBB0_321:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_321
.LBB0_322:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_323:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_323
	jmp	.LBB0_825
.LBB0_182:
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
	jne	.LBB0_187
# %bb.183:
	and	al, dil
	jne	.LBB0_187
# %bb.184:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_185:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_185
# %bb.186:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_187:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_189
.LBB0_188:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_188
.LBB0_189:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_190:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_190
	jmp	.LBB0_825
.LBB0_448:
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
	jne	.LBB0_453
# %bb.449:
	and	al, dil
	jne	.LBB0_453
# %bb.450:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
.LBB0_451:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_451
# %bb.452:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_453:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_455
.LBB0_454:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_454
.LBB0_455:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_456
	jmp	.LBB0_825
.LBB0_638:
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
	jne	.LBB0_643
# %bb.639:
	and	al, dil
	jne	.LBB0_643
# %bb.640:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_641:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_641
# %bb.642:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_643:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_645
.LBB0_644:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_644
.LBB0_645:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_646:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_646
	jmp	.LBB0_825
.LBB0_774:
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
	jne	.LBB0_779
# %bb.775:
	and	al, dil
	jne	.LBB0_779
# %bb.776:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_777:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_777
# %bb.778:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_779:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_781
.LBB0_780:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	imul	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_780
.LBB0_781:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_782:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_782
	jmp	.LBB0_825
.LBB0_103:
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
	jne	.LBB0_108
# %bb.104:
	and	al, dil
	jne	.LBB0_108
# %bb.105:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_106:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_106
# %bb.107:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_108:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_110
.LBB0_109:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_109
.LBB0_110:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_111:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_111
	jmp	.LBB0_825
.LBB0_369:
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
	jne	.LBB0_374
# %bb.370:
	and	al, dil
	jne	.LBB0_374
# %bb.371:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_372:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_372
# %bb.373:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_374:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_376
.LBB0_375:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_375
.LBB0_376:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_377
	jmp	.LBB0_825
.LBB0_236:
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
	jne	.LBB0_241
# %bb.237:
	and	al, dil
	jne	.LBB0_241
# %bb.238:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_239:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_239
# %bb.240:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_241:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_243
.LBB0_242:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*rsi]
	add	edi, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_242
.LBB0_243:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_244:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_244
	jmp	.LBB0_825
.LBB0_502:
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
	jne	.LBB0_507
# %bb.503:
	and	al, dil
	jne	.LBB0_507
# %bb.504:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_505:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_505
# %bb.506:
	cmp	rsi, r10
	je	.LBB0_825
.LBB0_507:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_509
.LBB0_508:                              # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rdx + 4*rsi]
	sub	edi, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_508
.LBB0_509:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_510
	jmp	.LBB0_825
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
	jne	.LBB0_631
# %bb.627:
	and	al, dil
	jne	.LBB0_631
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
	jne	.LBB0_631
	jmp	.LBB0_825
.LBB0_762:
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
	jne	.LBB0_767
# %bb.763:
	and	al, dil
	jne	.LBB0_767
# %bb.764:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_765:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_765
# %bb.766:
	cmp	rsi, r10
	jne	.LBB0_767
	jmp	.LBB0_825
.LBB0_357:
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
	jne	.LBB0_362
# %bb.358:
	and	al, dil
	jne	.LBB0_362
# %bb.359:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_360:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_360
# %bb.361:
	cmp	rsi, r10
	jne	.LBB0_362
	jmp	.LBB0_825
.LBB0_490:
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
# %bb.491:
	and	al, dil
	jne	.LBB0_495
# %bb.492:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_493:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_493
# %bb.494:
	cmp	rsi, r10
	jne	.LBB0_495
	jmp	.LBB0_825
.LBB0_680:
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
	jne	.LBB0_685
# %bb.681:
	and	al, dil
	jne	.LBB0_685
# %bb.682:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_683:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_683
# %bb.684:
	cmp	rsi, r10
	jne	.LBB0_685
	jmp	.LBB0_825
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
	jne	.LBB0_821
# %bb.817:
	and	al, dil
	jne	.LBB0_821
# %bb.818:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_819:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_819
# %bb.820:
	cmp	rsi, r10
	jne	.LBB0_821
	jmp	.LBB0_825
.LBB0_411:
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
	jne	.LBB0_416
# %bb.412:
	and	al, dil
	jne	.LBB0_416
# %bb.413:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_414:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_414
# %bb.415:
	cmp	rsi, r10
	jne	.LBB0_416
	jmp	.LBB0_825
.LBB0_544:
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
	jne	.LBB0_549
# %bb.545:
	and	al, dil
	jne	.LBB0_549
# %bb.546:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_547:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_547
# %bb.548:
	cmp	rsi, r10
	jne	.LBB0_549
	jmp	.LBB0_825
.LBB0_605:
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
	jne	.LBB0_610
# %bb.606:
	and	al, dil
	jne	.LBB0_610
# %bb.607:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_608
# %bb.609:
	cmp	rsi, r10
	jne	.LBB0_610
	jmp	.LBB0_825
.LBB0_617:
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
# %bb.618:
	and	al, dil
	jne	.LBB0_622
# %bb.619:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_620:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_620
# %bb.621:
	cmp	rsi, r10
	jne	.LBB0_622
	jmp	.LBB0_825
.LBB0_741:
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
	jne	.LBB0_746
# %bb.742:
	and	al, dil
	jne	.LBB0_746
# %bb.743:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_744:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_744
# %bb.745:
	cmp	rsi, r10
	jne	.LBB0_746
	jmp	.LBB0_825
.LBB0_753:
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
	jne	.LBB0_758
# %bb.754:
	and	al, dil
	jne	.LBB0_758
# %bb.755:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_756:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_756
# %bb.757:
	cmp	rsi, r10
	jne	.LBB0_758
	jmp	.LBB0_825
.LBB0_336:
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
	jne	.LBB0_341
# %bb.337:
	and	al, dil
	jne	.LBB0_341
# %bb.338:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_339:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_339
# %bb.340:
	cmp	rsi, r10
	jne	.LBB0_341
	jmp	.LBB0_825
.LBB0_348:
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
	jne	.LBB0_353
# %bb.349:
	and	al, dil
	jne	.LBB0_353
# %bb.350:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_351:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_351
# %bb.352:
	cmp	rsi, r10
	jne	.LBB0_353
	jmp	.LBB0_825
.LBB0_469:
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
	jne	.LBB0_474
# %bb.470:
	and	al, dil
	jne	.LBB0_474
# %bb.471:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_472:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_472
# %bb.473:
	cmp	rsi, r10
	jne	.LBB0_474
	jmp	.LBB0_825
.LBB0_481:
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
	jne	.LBB0_486
# %bb.482:
	and	al, dil
	jne	.LBB0_486
# %bb.483:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
.LBB0_484:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_484
# %bb.485:
	cmp	rsi, r10
	jne	.LBB0_486
	jmp	.LBB0_825
.LBB0_659:
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
# %bb.660:
	and	al, dil
	jne	.LBB0_664
# %bb.661:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_662:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_662
# %bb.663:
	cmp	rsi, r10
	jne	.LBB0_664
	jmp	.LBB0_825
.LBB0_671:
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
	jne	.LBB0_676
# %bb.672:
	and	al, dil
	jne	.LBB0_676
# %bb.673:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_674:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_674
# %bb.675:
	cmp	rsi, r10
	jne	.LBB0_676
	jmp	.LBB0_825
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
	jne	.LBB0_800
# %bb.796:
	and	al, dil
	jne	.LBB0_800
# %bb.797:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_798:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_798
# %bb.799:
	cmp	rsi, r10
	jne	.LBB0_800
	jmp	.LBB0_825
.LBB0_807:
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
	jne	.LBB0_812
# %bb.808:
	and	al, dil
	jne	.LBB0_812
# %bb.809:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_810:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_810
# %bb.811:
	cmp	rsi, r10
	jne	.LBB0_812
	jmp	.LBB0_825
.LBB0_390:
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
	jne	.LBB0_395
# %bb.391:
	and	al, dil
	jne	.LBB0_395
# %bb.392:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_393:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_393
# %bb.394:
	cmp	rsi, r10
	jne	.LBB0_395
	jmp	.LBB0_825
.LBB0_402:
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
	jne	.LBB0_407
# %bb.403:
	and	al, dil
	jne	.LBB0_407
# %bb.404:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_405:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_405
# %bb.406:
	cmp	rsi, r10
	jne	.LBB0_407
	jmp	.LBB0_825
.LBB0_523:
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
	jne	.LBB0_528
# %bb.524:
	and	al, dil
	jne	.LBB0_528
# %bb.525:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
.LBB0_526:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_526
# %bb.527:
	cmp	rsi, r10
	jne	.LBB0_528
	jmp	.LBB0_825
.LBB0_535:
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
	jne	.LBB0_540
# %bb.536:
	and	al, dil
	jne	.LBB0_540
# %bb.537:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB0_538:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_538
# %bb.539:
	cmp	rsi, r10
	jne	.LBB0_540
	jmp	.LBB0_825
.LBB0_592:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_593
.LBB0_594:
	test	r9, r9
	je	.LBB0_597
# %bb.595:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_596:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_596
.LBB0_597:
	cmp	rdi, r10
	je	.LBB0_825
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
	jb	.LBB0_825
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
	jmp	.LBB0_825
.LBB0_728:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_729:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_729
.LBB0_730:
	test	r9, r9
	je	.LBB0_733
# %bb.731:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_732:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_732
.LBB0_733:
	cmp	rdi, r10
	je	.LBB0_825
.LBB0_734:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_736
.LBB0_735:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_735
.LBB0_736:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_737:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_737
	jmp	.LBB0_825
.LBB0_578:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_579:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_579
.LBB0_580:
	test	r9, r9
	je	.LBB0_583
# %bb.581:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_582:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_582
.LBB0_583:
	cmp	rdi, r10
	je	.LBB0_825
.LBB0_584:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_586
.LBB0_585:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_585
.LBB0_586:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_587:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_587
	jmp	.LBB0_825
.LBB0_714:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_715:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_715
.LBB0_716:
	test	r9, r9
	je	.LBB0_719
# %bb.717:
	neg	r9
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_0] # ymm0 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
.LBB0_718:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_718
.LBB0_719:
	cmp	rdi, r10
	je	.LBB0_825
.LBB0_720:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB0_722
.LBB0_721:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	byte ptr [rdx + rdi]
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB0_721
.LBB0_722:
	cmp	r9, 3
	jb	.LBB0_825
.LBB0_723:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_723
.LBB0_825:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end0:
	.size	arithmetic_binary_avx2, .Lfunc_end0-arithmetic_binary_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function arithmetic_arr_scalar_avx2
.LCPI1_0:
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
	jne	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.9:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.10:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_11
# %bb.265:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_445
# %bb.266:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_445
.LBB1_11:
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
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_666
.LBB1_667:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_668:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_668
	jmp	.LBB1_1109
.LBB1_12:
	cmp	sil, 21
	je	.LBB1_39
# %bb.13:
	cmp	sil, 22
	je	.LBB1_47
# %bb.14:
	cmp	sil, 23
	jne	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.20:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.21:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_22
# %bb.268:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_448
# %bb.269:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_448
.LBB1_22:
	xor	esi, esi
.LBB1_673:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_675
.LBB1_674:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_674
.LBB1_675:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_676
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.28:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.29:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_30
# %bb.271:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_451
# %bb.272:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_451
.LBB1_30:
	xor	esi, esi
.LBB1_681:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_683
.LBB1_682:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_682
.LBB1_683:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_684:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_684
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.36:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.37:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_38
# %bb.274:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_454
# %bb.275:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_454
.LBB1_38:
	xor	esi, esi
.LBB1_689:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_691
.LBB1_690:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_690
.LBB1_691:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_692:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_692
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.44:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.45:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_46
# %bb.277:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_457
# %bb.278:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_457
.LBB1_46:
	xor	esi, esi
.LBB1_697:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_699
.LBB1_698:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_698
.LBB1_699:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_700:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_700
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.52:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.53:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_54
# %bb.280:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_460
# %bb.281:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_460
.LBB1_54:
	xor	esi, esi
.LBB1_705:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_707
.LBB1_706:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_706
.LBB1_707:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_708:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_708
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.59:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.60:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_61
# %bb.283:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_463
# %bb.284:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_463
.LBB1_61:
	xor	ecx, ecx
.LBB1_713:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_715
.LBB1_714:                              # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_714
.LBB1_715:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_716:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_716
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.66:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.67:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_68
# %bb.286:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_466
# %bb.287:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_466
.LBB1_68:
	xor	ecx, ecx
.LBB1_721:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_723
.LBB1_722:                              # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_722
.LBB1_723:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_724:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_724
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.73:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.74:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_75
# %bb.289:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_469
# %bb.290:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_469
.LBB1_75:
	xor	ecx, ecx
.LBB1_729:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_731
.LBB1_730:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_730
.LBB1_731:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_732:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_732
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.80:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.81:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_82
# %bb.292:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_472
# %bb.293:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_472
.LBB1_82:
	xor	ecx, ecx
.LBB1_737:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_739
.LBB1_738:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_738
.LBB1_739:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_740:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_740
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.87:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.88:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_89
# %bb.295:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_475
# %bb.296:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_475
.LBB1_89:
	xor	ecx, ecx
.LBB1_745:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_747
.LBB1_746:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rcx]
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_746
.LBB1_747:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_748:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_748
	jmp	.LBB1_1109
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
	jne	.LBB1_1109
# %bb.94:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.95:
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB1_96
# %bb.298:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_478
# %bb.299:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_478
.LBB1_96:
	xor	ecx, ecx
.LBB1_753:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_755
.LBB1_754:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_754
.LBB1_755:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_756:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_756
	jmp	.LBB1_1109
.LBB1_97:
	cmp	edi, 2
	je	.LBB1_229
# %bb.98:
	cmp	edi, 3
	jne	.LBB1_1109
# %bb.99:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.100:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_101
# %bb.301:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_481
# %bb.302:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_481
.LBB1_101:
	xor	edi, edi
.LBB1_627:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_629
.LBB1_628:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_628
.LBB1_629:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_630:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_630
	jmp	.LBB1_1109
.LBB1_102:
	cmp	edi, 2
	je	.LBB1_232
# %bb.103:
	cmp	edi, 3
	jne	.LBB1_1109
# %bb.104:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.105:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_106
# %bb.304:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_483
# %bb.305:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_483
.LBB1_106:
	xor	edi, edi
.LBB1_637:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_639
.LBB1_638:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_638
.LBB1_639:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_640:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_640
	jmp	.LBB1_1109
.LBB1_107:
	cmp	edi, 2
	je	.LBB1_235
# %bb.108:
	cmp	edi, 3
	jne	.LBB1_1109
# %bb.109:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.110:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_111
# %bb.307:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_485
# %bb.308:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_485
.LBB1_111:
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
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_762
.LBB1_763:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_764
	jmp	.LBB1_1109
.LBB1_112:
	cmp	edi, 2
	je	.LBB1_238
# %bb.113:
	cmp	edi, 3
	jne	.LBB1_1109
# %bb.114:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.115:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_116
# %bb.310:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_488
# %bb.311:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_488
.LBB1_116:
	xor	esi, esi
.LBB1_769:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_771
.LBB1_770:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_770
.LBB1_771:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_772:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_772
	jmp	.LBB1_1109
.LBB1_117:
	cmp	edi, 2
	je	.LBB1_241
# %bb.118:
	cmp	edi, 3
	jne	.LBB1_1109
# %bb.119:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.120:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_121
# %bb.313:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_491
# %bb.314:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_491
.LBB1_121:
	xor	esi, esi
.LBB1_777:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_779
.LBB1_778:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_778
.LBB1_779:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_780:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_780
	jmp	.LBB1_1109
.LBB1_122:
	cmp	edi, 2
	je	.LBB1_244
# %bb.123:
	cmp	edi, 3
	jne	.LBB1_1109
# %bb.124:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.125:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_126
# %bb.316:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_494
# %bb.317:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_494
.LBB1_126:
	xor	esi, esi
.LBB1_785:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_787
.LBB1_786:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_786
.LBB1_787:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_788:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_788
	jmp	.LBB1_1109
.LBB1_127:
	cmp	edi, 7
	je	.LBB1_247
# %bb.128:
	cmp	edi, 8
	jne	.LBB1_1109
# %bb.129:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.130:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_131
# %bb.319:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_497
# %bb.320:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_497
.LBB1_131:
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
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_794
.LBB1_795:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_796:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_796
	jmp	.LBB1_1109
.LBB1_132:
	cmp	edi, 7
	je	.LBB1_250
# %bb.133:
	cmp	edi, 8
	jne	.LBB1_1109
# %bb.134:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.135:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_136
# %bb.322:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_500
# %bb.323:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_500
.LBB1_136:
	xor	esi, esi
.LBB1_801:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_803
.LBB1_802:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_802
.LBB1_803:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_804
	jmp	.LBB1_1109
.LBB1_137:
	cmp	edi, 7
	je	.LBB1_253
# %bb.138:
	cmp	edi, 8
	jne	.LBB1_1109
# %bb.139:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.140:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_141
# %bb.325:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_503
# %bb.326:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_503
.LBB1_141:
	xor	esi, esi
.LBB1_809:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_811
.LBB1_810:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_810
.LBB1_811:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_812:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_812
	jmp	.LBB1_1109
.LBB1_142:
	cmp	edi, 7
	je	.LBB1_256
# %bb.143:
	cmp	edi, 8
	jne	.LBB1_1109
# %bb.144:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.145:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_146
# %bb.328:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_506
# %bb.329:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_506
.LBB1_146:
	xor	esi, esi
.LBB1_817:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_819
.LBB1_818:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_818
.LBB1_819:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_820:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_820
	jmp	.LBB1_1109
.LBB1_147:
	cmp	edi, 7
	je	.LBB1_259
# %bb.148:
	cmp	edi, 8
	jne	.LBB1_1109
# %bb.149:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.150:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_151
# %bb.331:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_509
# %bb.332:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_509
.LBB1_151:
	xor	esi, esi
.LBB1_825:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_827
.LBB1_826:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_826
.LBB1_827:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_828:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_828
	jmp	.LBB1_1109
.LBB1_152:
	cmp	edi, 7
	je	.LBB1_262
# %bb.153:
	cmp	edi, 8
	jne	.LBB1_1109
# %bb.154:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.155:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_156
# %bb.334:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_512
# %bb.335:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_512
.LBB1_156:
	xor	esi, esi
.LBB1_833:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_835
.LBB1_834:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_834
.LBB1_835:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_836:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_836
	jmp	.LBB1_1109
.LBB1_157:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.158:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_159
# %bb.337:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_515
# %bb.338:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_515
.LBB1_159:
	xor	esi, esi
.LBB1_841:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_843
.LBB1_842:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_842
.LBB1_843:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_844:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_844
	jmp	.LBB1_1109
.LBB1_160:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.161:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_162
# %bb.340:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_518
# %bb.341:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_518
.LBB1_162:
	xor	esi, esi
.LBB1_849:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_851
.LBB1_850:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_850
.LBB1_851:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_852:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_852
	jmp	.LBB1_1109
.LBB1_163:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.164:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_165
# %bb.343:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_521
# %bb.344:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_521
.LBB1_165:
	xor	esi, esi
.LBB1_857:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_859
.LBB1_858:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_858
.LBB1_859:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_860:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_860
	jmp	.LBB1_1109
.LBB1_166:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.167:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_168
# %bb.346:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_524
# %bb.347:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_524
.LBB1_168:
	xor	esi, esi
.LBB1_865:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_867
.LBB1_866:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_866
.LBB1_867:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_868:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_868
	jmp	.LBB1_1109
.LBB1_169:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.170:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_171
# %bb.349:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_527
# %bb.350:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_527
.LBB1_171:
	xor	esi, esi
.LBB1_873:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_875
.LBB1_874:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_874
.LBB1_875:
	cmp	r9, 3
	jb	.LBB1_1109
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
	jmp	.LBB1_1109
.LBB1_172:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.173:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_174
# %bb.352:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_530
# %bb.353:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_530
.LBB1_174:
	xor	esi, esi
.LBB1_881:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_883
.LBB1_882:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_882
.LBB1_883:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_884:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_884
	jmp	.LBB1_1109
.LBB1_175:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.176:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_177
# %bb.355:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_533
# %bb.356:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_533
.LBB1_177:
	xor	esi, esi
.LBB1_889:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_891
.LBB1_890:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_890
.LBB1_891:
	cmp	r9, 3
	jb	.LBB1_1109
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
	jmp	.LBB1_1109
.LBB1_178:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.179:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_180
# %bb.358:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_536
# %bb.359:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_536
.LBB1_180:
	xor	esi, esi
.LBB1_897:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_899
.LBB1_898:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_898
.LBB1_899:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_900:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_900
	jmp	.LBB1_1109
.LBB1_181:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.182:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_183
# %bb.361:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_539
# %bb.362:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_539
.LBB1_183:
	xor	esi, esi
.LBB1_905:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_907
.LBB1_906:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_906
.LBB1_907:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_908:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_908
	jmp	.LBB1_1109
.LBB1_184:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.185:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_186
# %bb.364:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_542
# %bb.365:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_542
.LBB1_186:
	xor	esi, esi
.LBB1_913:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_915
.LBB1_914:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_914
.LBB1_915:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_916:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_916
	jmp	.LBB1_1109
.LBB1_187:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.188:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_189
# %bb.367:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_545
# %bb.368:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_545
.LBB1_189:
	xor	esi, esi
.LBB1_921:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_923
.LBB1_922:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_922
.LBB1_923:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_924:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_924
	jmp	.LBB1_1109
.LBB1_190:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.191:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_192
# %bb.370:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_548
# %bb.371:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_548
.LBB1_192:
	xor	esi, esi
.LBB1_929:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_931
.LBB1_930:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rdx + 2*rsi]
	sub	edi, eax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_930
.LBB1_931:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_932:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_932
	jmp	.LBB1_1109
.LBB1_193:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.194:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_195
# %bb.373:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_551
# %bb.374:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_551
.LBB1_195:
	xor	esi, esi
.LBB1_937:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_939
.LBB1_938:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_938
.LBB1_939:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_940:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_940
	jmp	.LBB1_1109
.LBB1_196:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.197:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_198
# %bb.376:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_554
# %bb.377:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_554
.LBB1_198:
	xor	ecx, ecx
.LBB1_945:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_947
.LBB1_946:                              # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_946
.LBB1_947:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_948:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_948
	jmp	.LBB1_1109
.LBB1_199:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.200:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_201
# %bb.379:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_557
# %bb.380:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_557
.LBB1_201:
	xor	esi, esi
.LBB1_953:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_955
.LBB1_954:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	imul	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_954
.LBB1_955:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_956:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_956
	jmp	.LBB1_1109
.LBB1_202:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.203:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_204
# %bb.382:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_560
# %bb.383:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_560
.LBB1_204:
	xor	ecx, ecx
.LBB1_961:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_963
.LBB1_962:                              # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_962
.LBB1_963:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_964:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_964
	jmp	.LBB1_1109
.LBB1_205:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.206:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_207
# %bb.385:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_563
# %bb.386:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_563
.LBB1_207:
	xor	esi, esi
.LBB1_969:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_971
.LBB1_970:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_970
.LBB1_971:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_972:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_972
	jmp	.LBB1_1109
.LBB1_208:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.209:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_210
# %bb.388:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_566
# %bb.389:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_566
.LBB1_210:
	xor	ecx, ecx
.LBB1_977:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_979
.LBB1_978:                              # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_978
.LBB1_979:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_980
	jmp	.LBB1_1109
.LBB1_211:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.212:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_213
# %bb.391:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_569
# %bb.392:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_569
.LBB1_213:
	xor	esi, esi
.LBB1_985:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_987
.LBB1_986:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_986
.LBB1_987:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_988:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_988
	jmp	.LBB1_1109
.LBB1_214:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.215:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_216
# %bb.394:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_572
# %bb.395:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_572
.LBB1_216:
	xor	ecx, ecx
.LBB1_993:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_995
.LBB1_994:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_994
.LBB1_995:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_996:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_996
	jmp	.LBB1_1109
.LBB1_217:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.218:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_219
# %bb.397:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_575
# %bb.398:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_575
.LBB1_219:
	xor	esi, esi
.LBB1_1001:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1003
.LBB1_1002:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1002
.LBB1_1003:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1004:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1004
	jmp	.LBB1_1109
.LBB1_220:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.221:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_222
# %bb.400:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_578
# %bb.401:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_578
.LBB1_222:
	xor	ecx, ecx
.LBB1_1009:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1011
.LBB1_1010:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rcx]
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1010
.LBB1_1011:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1012
	jmp	.LBB1_1109
.LBB1_223:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.224:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_225
# %bb.403:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_581
# %bb.404:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_581
.LBB1_225:
	xor	esi, esi
.LBB1_1017:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1019
.LBB1_1018:                             # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1018
.LBB1_1019:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1020:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1020
	jmp	.LBB1_1109
.LBB1_226:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.227:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB1_228
# %bb.406:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_584
# %bb.407:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_584
.LBB1_228:
	xor	ecx, ecx
.LBB1_1025:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_1027
.LBB1_1026:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_1026
.LBB1_1027:
	cmp	rsi, 3
	jb	.LBB1_1109
.LBB1_1028:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1028
	jmp	.LBB1_1109
.LBB1_229:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.230:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_231
# %bb.409:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_587
# %bb.410:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_587
.LBB1_231:
	xor	edi, edi
.LBB1_647:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_649
.LBB1_648:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_648
.LBB1_649:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_650:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_650
	jmp	.LBB1_1109
.LBB1_232:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.233:
	mov	cl, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_234
# %bb.412:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_589
# %bb.413:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_589
.LBB1_234:
	xor	edi, edi
.LBB1_657:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB1_659
.LBB1_658:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rdi]
	mul	cl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB1_658
.LBB1_659:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_660
	jmp	.LBB1_1109
.LBB1_235:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.236:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_237
# %bb.415:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_591
# %bb.416:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_591
.LBB1_237:
	xor	esi, esi
.LBB1_1033:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1035
.LBB1_1034:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1034
.LBB1_1035:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1036:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1036
	jmp	.LBB1_1109
.LBB1_238:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.239:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_240
# %bb.418:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_594
# %bb.419:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_594
.LBB1_240:
	xor	esi, esi
.LBB1_1041:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1043
.LBB1_1042:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1042
.LBB1_1043:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1044:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1044
	jmp	.LBB1_1109
.LBB1_241:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.242:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_243
# %bb.421:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_597
# %bb.422:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_597
.LBB1_243:
	xor	esi, esi
.LBB1_1049:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1051
.LBB1_1050:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1050
.LBB1_1051:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1052:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1052
	jmp	.LBB1_1109
.LBB1_244:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.245:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB1_246
# %bb.424:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_600
# %bb.425:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_600
.LBB1_246:
	xor	esi, esi
.LBB1_1057:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1059
.LBB1_1058:                             # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1058
.LBB1_1059:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1060:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1060
	jmp	.LBB1_1109
.LBB1_247:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.248:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_249
# %bb.427:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_603
# %bb.428:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_603
.LBB1_249:
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
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1066
.LBB1_1067:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1068:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1068
	jmp	.LBB1_1109
.LBB1_250:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.251:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_252
# %bb.430:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_606
# %bb.431:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_606
.LBB1_252:
	xor	esi, esi
.LBB1_1073:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1075
.LBB1_1074:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	imul	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1074
.LBB1_1075:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1076:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1076
	jmp	.LBB1_1109
.LBB1_253:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.254:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_255
# %bb.433:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_609
# %bb.434:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_609
.LBB1_255:
	xor	esi, esi
.LBB1_1081:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1083
.LBB1_1082:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1082
.LBB1_1083:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1084:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1084
	jmp	.LBB1_1109
.LBB1_256:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.257:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_258
# %bb.436:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_612
# %bb.437:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_612
.LBB1_258:
	xor	esi, esi
.LBB1_1089:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1091
.LBB1_1090:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1090
.LBB1_1091:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1092:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1092
	jmp	.LBB1_1109
.LBB1_259:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.260:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_261
# %bb.439:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_615
# %bb.440:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_615
.LBB1_261:
	xor	esi, esi
.LBB1_1097:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1099
.LBB1_1098:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1098
.LBB1_1099:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1100:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1100
	jmp	.LBB1_1109
.LBB1_262:
	test	r9d, r9d
	jle	.LBB1_1109
# %bb.263:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_264
# %bb.442:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_618
# %bb.443:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_618
.LBB1_264:
	xor	esi, esi
.LBB1_1105:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_1107
.LBB1_1106:                             # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_1106
.LBB1_1107:
	cmp	r9, 3
	jb	.LBB1_1109
.LBB1_1108:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_1108
	jmp	.LBB1_1109
.LBB1_445:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_661
# %bb.446:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_447:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_447
	jmp	.LBB1_662
.LBB1_448:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_669
# %bb.449:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_450:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_450
	jmp	.LBB1_670
.LBB1_451:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_677
# %bb.452:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_453:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_453
	jmp	.LBB1_678
.LBB1_454:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_685
# %bb.455:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_456
	jmp	.LBB1_686
.LBB1_457:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_693
# %bb.458:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_459:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_459
	jmp	.LBB1_694
.LBB1_460:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_701
# %bb.461:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_462:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_462
	jmp	.LBB1_702
.LBB1_463:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_709
# %bb.464:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_465:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_465
	jmp	.LBB1_710
.LBB1_466:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_717
# %bb.467:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_468:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_468
	jmp	.LBB1_718
.LBB1_469:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_725
# %bb.470:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_471:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_471
	jmp	.LBB1_726
.LBB1_472:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_733
# %bb.473:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_474:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_474
	jmp	.LBB1_734
.LBB1_475:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_741
# %bb.476:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_477:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_477
	jmp	.LBB1_742
.LBB1_478:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_749
# %bb.479:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_480:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_480
	jmp	.LBB1_750
.LBB1_481:
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
	jae	.LBB1_621
# %bb.482:
	xor	esi, esi
	jmp	.LBB1_623
.LBB1_483:
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
	jae	.LBB1_631
# %bb.484:
	xor	esi, esi
	jmp	.LBB1_633
.LBB1_485:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_757
# %bb.486:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_487:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_487
	jmp	.LBB1_758
.LBB1_488:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_765
# %bb.489:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_490:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_490
	jmp	.LBB1_766
.LBB1_491:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_773
# %bb.492:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_493:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_493
	jmp	.LBB1_774
.LBB1_494:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_781
# %bb.495:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_496:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_496
	jmp	.LBB1_782
.LBB1_497:
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
	je	.LBB1_789
# %bb.498:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_499:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_499
	jmp	.LBB1_790
.LBB1_500:
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
	je	.LBB1_797
# %bb.501:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_502:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_502
	jmp	.LBB1_798
.LBB1_503:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_805
# %bb.504:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_505:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_505
	jmp	.LBB1_806
.LBB1_506:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_813
# %bb.507:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_508:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_508
	jmp	.LBB1_814
.LBB1_509:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_821
# %bb.510:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_511:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_511
	jmp	.LBB1_822
.LBB1_512:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_829
# %bb.513:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_514:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_514
	jmp	.LBB1_830
.LBB1_515:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_837
# %bb.516:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_517:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_517
	jmp	.LBB1_838
.LBB1_518:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_845
# %bb.519:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_520:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_520
	jmp	.LBB1_846
.LBB1_521:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_853
# %bb.522:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_523:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_523
	jmp	.LBB1_854
.LBB1_524:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_861
# %bb.525:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_526:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_526
	jmp	.LBB1_862
.LBB1_527:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_869
# %bb.528:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_529:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_529
	jmp	.LBB1_870
.LBB1_530:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_877
# %bb.531:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_532:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_532
	jmp	.LBB1_878
.LBB1_533:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_885
# %bb.534:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_535:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_535
	jmp	.LBB1_886
.LBB1_536:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_893
# %bb.537:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_538:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_538
	jmp	.LBB1_894
.LBB1_539:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_901
# %bb.540:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_541:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_541
	jmp	.LBB1_902
.LBB1_542:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_909
# %bb.543:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_544:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_544
	jmp	.LBB1_910
.LBB1_545:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_917
# %bb.546:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_547:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_547
	jmp	.LBB1_918
.LBB1_548:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_925
# %bb.549:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_550:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_550
	jmp	.LBB1_926
.LBB1_551:
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
	je	.LBB1_933
# %bb.552:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_553:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_553
	jmp	.LBB1_934
.LBB1_554:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_941
# %bb.555:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_556:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_556
	jmp	.LBB1_942
.LBB1_557:
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
	je	.LBB1_949
# %bb.558:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_559:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_559
	jmp	.LBB1_950
.LBB1_560:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_957
# %bb.561:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_562:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_562
	jmp	.LBB1_958
.LBB1_563:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_965
# %bb.564:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_565:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_565
	jmp	.LBB1_966
.LBB1_566:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_973
# %bb.567:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_568:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_568
	jmp	.LBB1_974
.LBB1_569:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_981
# %bb.570:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_571:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_571
	jmp	.LBB1_982
.LBB1_572:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_989
# %bb.573:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_574:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_574
	jmp	.LBB1_990
.LBB1_575:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_997
# %bb.576:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_577:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_577
	jmp	.LBB1_998
.LBB1_578:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1005
# %bb.579:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_580:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_580
	jmp	.LBB1_1006
.LBB1_581:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1013
# %bb.582:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_583:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_583
	jmp	.LBB1_1014
.LBB1_584:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_1021
# %bb.585:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_586:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_586
	jmp	.LBB1_1022
.LBB1_587:
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
	jae	.LBB1_641
# %bb.588:
	xor	esi, esi
	jmp	.LBB1_643
.LBB1_589:
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
	jae	.LBB1_651
# %bb.590:
	xor	esi, esi
	jmp	.LBB1_653
.LBB1_591:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1029
# %bb.592:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_593
	jmp	.LBB1_1030
.LBB1_594:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1037
# %bb.595:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_596:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_596
	jmp	.LBB1_1038
.LBB1_597:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1045
# %bb.598:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_599:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_599
	jmp	.LBB1_1046
.LBB1_600:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1053
# %bb.601:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_602:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_602
	jmp	.LBB1_1054
.LBB1_603:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1061
# %bb.604:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_605
	jmp	.LBB1_1062
.LBB1_606:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1069
# %bb.607:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_608
	jmp	.LBB1_1070
.LBB1_609:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1077
# %bb.610:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_611:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_611
	jmp	.LBB1_1078
.LBB1_612:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1085
# %bb.613:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_614:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_614
	jmp	.LBB1_1086
.LBB1_615:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1093
# %bb.616:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_617:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_617
	jmp	.LBB1_1094
.LBB1_618:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_1101
# %bb.619:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_620:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_620
	jmp	.LBB1_1102
.LBB1_621:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_622:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_622
.LBB1_623:
	test	r9, r9
	je	.LBB1_626
# %bb.624:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_625:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_625
.LBB1_626:
	cmp	rdi, r10
	je	.LBB1_1109
	jmp	.LBB1_627
.LBB1_631:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_632:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_632
.LBB1_633:
	test	r9, r9
	je	.LBB1_636
# %bb.634:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_635:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_635
.LBB1_636:
	cmp	rdi, r10
	je	.LBB1_1109
	jmp	.LBB1_637
.LBB1_641:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_642:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_642
.LBB1_643:
	test	r9, r9
	je	.LBB1_646
# %bb.644:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_645:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_645
.LBB1_646:
	cmp	rdi, r10
	je	.LBB1_1109
	jmp	.LBB1_647
.LBB1_651:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_652:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_652
.LBB1_653:
	test	r9, r9
	je	.LBB1_656
# %bb.654:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI1_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB1_655:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_655
.LBB1_656:
	cmp	rdi, r10
	je	.LBB1_1109
	jmp	.LBB1_657
.LBB1_661:
	xor	edi, edi
.LBB1_662:
	test	r9b, 1
	je	.LBB1_664
# %bb.663:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_664:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_665
.LBB1_669:
	xor	edi, edi
.LBB1_670:
	test	r9b, 1
	je	.LBB1_672
# %bb.671:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_672:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_673
.LBB1_677:
	xor	edi, edi
.LBB1_678:
	test	r9b, 1
	je	.LBB1_680
# %bb.679:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_680:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_681
.LBB1_685:
	xor	edi, edi
.LBB1_686:
	test	r9b, 1
	je	.LBB1_688
# %bb.687:
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
.LBB1_688:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_689
.LBB1_693:
	xor	edi, edi
.LBB1_694:
	test	r9b, 1
	je	.LBB1_696
# %bb.695:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_696:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_697
.LBB1_701:
	xor	edi, edi
.LBB1_702:
	test	r9b, 1
	je	.LBB1_704
# %bb.703:
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
.LBB1_704:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_705
.LBB1_709:
	xor	edi, edi
.LBB1_710:
	test	r9b, 1
	je	.LBB1_712
# %bb.711:
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_712:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_713
.LBB1_717:
	xor	edi, edi
.LBB1_718:
	test	r9b, 1
	je	.LBB1_720
# %bb.719:
	vmulpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_720:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_721
.LBB1_725:
	xor	edi, edi
.LBB1_726:
	test	r9b, 1
	je	.LBB1_728
# %bb.727:
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_728:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_729
.LBB1_733:
	xor	esi, esi
.LBB1_734:
	test	r9b, 1
	je	.LBB1_736
# %bb.735:
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
.LBB1_736:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_737
.LBB1_741:
	xor	edi, edi
.LBB1_742:
	test	r9b, 1
	je	.LBB1_744
# %bb.743:
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB1_744:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_745
.LBB1_749:
	xor	esi, esi
.LBB1_750:
	test	r9b, 1
	je	.LBB1_752
# %bb.751:
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
.LBB1_752:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_753
.LBB1_757:
	xor	edi, edi
.LBB1_758:
	test	r9b, 1
	je	.LBB1_760
# %bb.759:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_760:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_761
.LBB1_765:
	xor	edi, edi
.LBB1_766:
	test	r9b, 1
	je	.LBB1_768
# %bb.767:
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
.LBB1_768:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_769
.LBB1_773:
	xor	edi, edi
.LBB1_774:
	test	r9b, 1
	je	.LBB1_776
# %bb.775:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_776:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_777
.LBB1_781:
	xor	edi, edi
.LBB1_782:
	test	r9b, 1
	je	.LBB1_784
# %bb.783:
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
.LBB1_784:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_785
.LBB1_789:
	xor	edi, edi
.LBB1_790:
	test	r9b, 1
	je	.LBB1_792
# %bb.791:
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
.LBB1_792:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_793
.LBB1_797:
	xor	edi, edi
.LBB1_798:
	test	r9b, 1
	je	.LBB1_800
# %bb.799:
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
.LBB1_800:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_801
.LBB1_805:
	xor	edi, edi
.LBB1_806:
	test	r9b, 1
	je	.LBB1_808
# %bb.807:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_808:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_809
.LBB1_813:
	xor	edi, edi
.LBB1_814:
	test	r9b, 1
	je	.LBB1_816
# %bb.815:
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
.LBB1_816:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_817
.LBB1_821:
	xor	edi, edi
.LBB1_822:
	test	r9b, 1
	je	.LBB1_824
# %bb.823:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_824:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_825
.LBB1_829:
	xor	edi, edi
.LBB1_830:
	test	r9b, 1
	je	.LBB1_832
# %bb.831:
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
.LBB1_832:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_833
.LBB1_837:
	xor	edi, edi
.LBB1_838:
	test	r9b, 1
	je	.LBB1_840
# %bb.839:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_840:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_841
.LBB1_845:
	xor	edi, edi
.LBB1_846:
	test	r9b, 1
	je	.LBB1_848
# %bb.847:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_848:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_849
.LBB1_853:
	xor	edi, edi
.LBB1_854:
	test	r9b, 1
	je	.LBB1_856
# %bb.855:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_856:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_857
.LBB1_861:
	xor	edi, edi
.LBB1_862:
	test	r9b, 1
	je	.LBB1_864
# %bb.863:
	vpmullw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_864:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_865
.LBB1_869:
	xor	edi, edi
.LBB1_870:
	test	r9b, 1
	je	.LBB1_872
# %bb.871:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_872:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_873
.LBB1_877:
	xor	edi, edi
.LBB1_878:
	test	r9b, 1
	je	.LBB1_880
# %bb.879:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_880:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_881
.LBB1_885:
	xor	edi, edi
.LBB1_886:
	test	r9b, 1
	je	.LBB1_888
# %bb.887:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_888:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_889
.LBB1_893:
	xor	edi, edi
.LBB1_894:
	test	r9b, 1
	je	.LBB1_896
# %bb.895:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_896:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_897
.LBB1_901:
	xor	edi, edi
.LBB1_902:
	test	r9b, 1
	je	.LBB1_904
# %bb.903:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_904:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_905
.LBB1_909:
	xor	edi, edi
.LBB1_910:
	test	r9b, 1
	je	.LBB1_912
# %bb.911:
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_912:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_913
.LBB1_917:
	xor	edi, edi
.LBB1_918:
	test	r9b, 1
	je	.LBB1_920
# %bb.919:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_920:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_921
.LBB1_925:
	xor	edi, edi
.LBB1_926:
	test	r9b, 1
	je	.LBB1_928
# %bb.927:
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rdi + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB1_928:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_929
.LBB1_933:
	xor	edi, edi
.LBB1_934:
	test	r9b, 1
	je	.LBB1_936
# %bb.935:
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
.LBB1_936:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_937
.LBB1_941:
	xor	edi, edi
.LBB1_942:
	test	r9b, 1
	je	.LBB1_944
# %bb.943:
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_944:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_945
.LBB1_949:
	xor	edi, edi
.LBB1_950:
	test	r9b, 1
	je	.LBB1_952
# %bb.951:
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
.LBB1_952:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_953
.LBB1_957:
	xor	edi, edi
.LBB1_958:
	test	r9b, 1
	je	.LBB1_960
# %bb.959:
	vmulps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_960:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_961
.LBB1_965:
	xor	edi, edi
.LBB1_966:
	test	r9b, 1
	je	.LBB1_968
# %bb.967:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_968:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_969
.LBB1_973:
	xor	edi, edi
.LBB1_974:
	test	r9b, 1
	je	.LBB1_976
# %bb.975:
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_976:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_977
.LBB1_981:
	xor	edi, edi
.LBB1_982:
	test	r9b, 1
	je	.LBB1_984
# %bb.983:
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
.LBB1_984:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_985
.LBB1_989:
	xor	esi, esi
.LBB1_990:
	test	r9b, 1
	je	.LBB1_992
# %bb.991:
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
.LBB1_992:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_993
.LBB1_997:
	xor	edi, edi
.LBB1_998:
	test	r9b, 1
	je	.LBB1_1000
# %bb.999:
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB1_1000:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1001
.LBB1_1005:
	xor	edi, edi
.LBB1_1006:
	test	r9b, 1
	je	.LBB1_1008
# %bb.1007:
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB1_1008:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_1009
.LBB1_1013:
	xor	edi, edi
.LBB1_1014:
	test	r9b, 1
	je	.LBB1_1016
# %bb.1015:
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
.LBB1_1016:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1017
.LBB1_1021:
	xor	esi, esi
.LBB1_1022:
	test	r9b, 1
	je	.LBB1_1024
# %bb.1023:
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
.LBB1_1024:
	cmp	rcx, rax
	je	.LBB1_1109
	jmp	.LBB1_1025
.LBB1_1029:
	xor	edi, edi
.LBB1_1030:
	test	r9b, 1
	je	.LBB1_1032
# %bb.1031:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1032:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1033
.LBB1_1037:
	xor	edi, edi
.LBB1_1038:
	test	r9b, 1
	je	.LBB1_1040
# %bb.1039:
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
.LBB1_1040:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1041
.LBB1_1045:
	xor	edi, edi
.LBB1_1046:
	test	r9b, 1
	je	.LBB1_1048
# %bb.1047:
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB1_1048:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1049
.LBB1_1053:
	xor	edi, edi
.LBB1_1054:
	test	r9b, 1
	je	.LBB1_1056
# %bb.1055:
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
.LBB1_1056:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1057
.LBB1_1061:
	xor	edi, edi
.LBB1_1062:
	test	r9b, 1
	je	.LBB1_1064
# %bb.1063:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1064:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1065
.LBB1_1069:
	xor	edi, edi
.LBB1_1070:
	test	r9b, 1
	je	.LBB1_1072
# %bb.1071:
	vpmulld	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1072:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1073
.LBB1_1077:
	xor	edi, edi
.LBB1_1078:
	test	r9b, 1
	je	.LBB1_1080
# %bb.1079:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1080:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1081
.LBB1_1085:
	xor	edi, edi
.LBB1_1086:
	test	r9b, 1
	je	.LBB1_1088
# %bb.1087:
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
.LBB1_1088:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1089
.LBB1_1093:
	xor	edi, edi
.LBB1_1094:
	test	r9b, 1
	je	.LBB1_1096
# %bb.1095:
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB1_1096:
	cmp	rsi, r10
	je	.LBB1_1109
	jmp	.LBB1_1097
.LBB1_1101:
	xor	edi, edi
.LBB1_1102:
	test	r9b, 1
	je	.LBB1_1104
# %bb.1103:
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
.LBB1_1104:
	cmp	rsi, r10
	jne	.LBB1_1105
.LBB1_1109:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end1:
	.size	arithmetic_arr_scalar_avx2, .Lfunc_end1-arithmetic_arr_scalar_avx2
                                        # -- End function
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5                               # -- Begin function arithmetic_scalar_arr_avx2
.LCPI2_0:
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
	jne	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.9:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.10:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_11
# %bb.265:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_445
# %bb.266:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_445
.LBB2_11:
	xor	esi, esi
.LBB2_665:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_667
.LBB2_666:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_666
.LBB2_667:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_668:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_668
	jmp	.LBB2_1109
.LBB2_12:
	cmp	sil, 21
	je	.LBB2_39
# %bb.13:
	cmp	sil, 22
	je	.LBB2_47
# %bb.14:
	cmp	sil, 23
	jne	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.20:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.21:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_22
# %bb.268:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_448
# %bb.269:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_448
.LBB2_22:
	xor	esi, esi
.LBB2_673:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_675
.LBB2_674:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_674
.LBB2_675:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_676
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.28:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.29:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_30
# %bb.271:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_451
# %bb.272:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_451
.LBB2_30:
	xor	esi, esi
.LBB2_681:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_683
.LBB2_682:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_682
.LBB2_683:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_684:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_684
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.36:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.37:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_38
# %bb.274:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_454
# %bb.275:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_454
.LBB2_38:
	xor	esi, esi
.LBB2_689:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_691
.LBB2_690:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_690
.LBB2_691:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_692:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_692
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.44:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.45:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_46
# %bb.277:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_457
# %bb.278:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_457
.LBB2_46:
	xor	esi, esi
.LBB2_697:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_699
.LBB2_698:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_698
.LBB2_699:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_700:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_700
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.52:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.53:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_54
# %bb.280:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_460
# %bb.281:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_460
.LBB2_54:
	xor	esi, esi
.LBB2_705:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_707
.LBB2_706:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_706
.LBB2_707:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_708:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_708
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.59:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.60:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_61
# %bb.283:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_463
# %bb.284:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_463
.LBB2_61:
	xor	edx, edx
.LBB2_713:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_715
.LBB2_714:                              # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_714
.LBB2_715:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_716:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_716
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.66:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.67:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_68
# %bb.286:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_466
# %bb.287:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_466
.LBB2_68:
	xor	edx, edx
.LBB2_721:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_723
.LBB2_722:                              # =>This Inner Loop Header: Depth=1
	vmulsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_722
.LBB2_723:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_724:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_724
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.73:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.74:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_75
# %bb.289:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_469
# %bb.290:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_469
.LBB2_75:
	xor	edx, edx
.LBB2_729:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_731
.LBB2_730:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_730
.LBB2_731:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_732:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_732
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.80:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.81:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_82
# %bb.292:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_472
# %bb.293:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_472
.LBB2_82:
	xor	edx, edx
.LBB2_737:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_739
.LBB2_738:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_738
.LBB2_739:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_740:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_740
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.87:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.88:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_89
# %bb.295:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_475
# %bb.296:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_475
.LBB2_89:
	xor	edx, edx
.LBB2_745:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_747
.LBB2_746:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_746
.LBB2_747:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_748:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_748
	jmp	.LBB2_1109
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
	jne	.LBB2_1109
# %bb.94:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.95:
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB2_96
# %bb.298:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_478
# %bb.299:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_478
.LBB2_96:
	xor	edx, edx
.LBB2_753:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_755
.LBB2_754:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_754
.LBB2_755:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_756:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_756
	jmp	.LBB2_1109
.LBB2_97:
	cmp	edi, 2
	je	.LBB2_229
# %bb.98:
	cmp	edi, 3
	jne	.LBB2_1109
# %bb.99:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.100:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_101
# %bb.301:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_481
# %bb.302:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_481
.LBB2_101:
	xor	edi, edi
.LBB2_627:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_629
.LBB2_628:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_628
.LBB2_629:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_630:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_630
	jmp	.LBB2_1109
.LBB2_102:
	cmp	edi, 2
	je	.LBB2_232
# %bb.103:
	cmp	edi, 3
	jne	.LBB2_1109
# %bb.104:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.105:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_106
# %bb.304:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_483
# %bb.305:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_483
.LBB2_106:
	xor	edi, edi
.LBB2_637:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_639
.LBB2_638:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_638
.LBB2_639:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_640:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_640
	jmp	.LBB2_1109
.LBB2_107:
	cmp	edi, 2
	je	.LBB2_235
# %bb.108:
	cmp	edi, 3
	jne	.LBB2_1109
# %bb.109:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.110:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_111
# %bb.307:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_485
# %bb.308:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_485
.LBB2_111:
	xor	esi, esi
.LBB2_761:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_763
.LBB2_762:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_762
.LBB2_763:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_764:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_764
	jmp	.LBB2_1109
.LBB2_112:
	cmp	edi, 2
	je	.LBB2_238
# %bb.113:
	cmp	edi, 3
	jne	.LBB2_1109
# %bb.114:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.115:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_116
# %bb.310:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_488
# %bb.311:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_488
.LBB2_116:
	xor	esi, esi
.LBB2_769:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_771
.LBB2_770:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_770
.LBB2_771:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_772:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_772
	jmp	.LBB2_1109
.LBB2_117:
	cmp	edi, 2
	je	.LBB2_241
# %bb.118:
	cmp	edi, 3
	jne	.LBB2_1109
# %bb.119:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.120:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_121
# %bb.313:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_491
# %bb.314:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_491
.LBB2_121:
	xor	esi, esi
.LBB2_777:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_779
.LBB2_778:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_778
.LBB2_779:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_780:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_780
	jmp	.LBB2_1109
.LBB2_122:
	cmp	edi, 2
	je	.LBB2_244
# %bb.123:
	cmp	edi, 3
	jne	.LBB2_1109
# %bb.124:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.125:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_126
# %bb.316:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_494
# %bb.317:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_494
.LBB2_126:
	xor	esi, esi
.LBB2_785:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_787
.LBB2_786:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_786
.LBB2_787:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_788:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_788
	jmp	.LBB2_1109
.LBB2_127:
	cmp	edi, 7
	je	.LBB2_247
# %bb.128:
	cmp	edi, 8
	jne	.LBB2_1109
# %bb.129:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.130:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_131
# %bb.319:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_497
# %bb.320:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_497
.LBB2_131:
	xor	esi, esi
.LBB2_793:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_795
.LBB2_794:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_794
.LBB2_795:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_796:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_796
	jmp	.LBB2_1109
.LBB2_132:
	cmp	edi, 7
	je	.LBB2_250
# %bb.133:
	cmp	edi, 8
	jne	.LBB2_1109
# %bb.134:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.135:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_136
# %bb.322:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_500
# %bb.323:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_500
.LBB2_136:
	xor	esi, esi
.LBB2_801:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_803
.LBB2_802:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_802
.LBB2_803:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_804:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_804
	jmp	.LBB2_1109
.LBB2_137:
	cmp	edi, 7
	je	.LBB2_253
# %bb.138:
	cmp	edi, 8
	jne	.LBB2_1109
# %bb.139:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.140:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_141
# %bb.325:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_503
# %bb.326:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_503
.LBB2_141:
	xor	esi, esi
.LBB2_809:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_811
.LBB2_810:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_810
.LBB2_811:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_812:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_812
	jmp	.LBB2_1109
.LBB2_142:
	cmp	edi, 7
	je	.LBB2_256
# %bb.143:
	cmp	edi, 8
	jne	.LBB2_1109
# %bb.144:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.145:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_146
# %bb.328:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_506
# %bb.329:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_506
.LBB2_146:
	xor	esi, esi
.LBB2_817:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_819
.LBB2_818:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_818
.LBB2_819:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_820:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_820
	jmp	.LBB2_1109
.LBB2_147:
	cmp	edi, 7
	je	.LBB2_259
# %bb.148:
	cmp	edi, 8
	jne	.LBB2_1109
# %bb.149:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.150:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_151
# %bb.331:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_509
# %bb.332:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_509
.LBB2_151:
	xor	esi, esi
.LBB2_825:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_827
.LBB2_826:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_826
.LBB2_827:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_828:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_828
	jmp	.LBB2_1109
.LBB2_152:
	cmp	edi, 7
	je	.LBB2_262
# %bb.153:
	cmp	edi, 8
	jne	.LBB2_1109
# %bb.154:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.155:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_156
# %bb.334:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_512
# %bb.335:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_512
.LBB2_156:
	xor	esi, esi
.LBB2_833:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_835
.LBB2_834:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_834
.LBB2_835:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_836:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_836
	jmp	.LBB2_1109
.LBB2_157:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.158:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_159
# %bb.337:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_515
# %bb.338:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_515
.LBB2_159:
	xor	esi, esi
.LBB2_841:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_843
.LBB2_842:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_842
.LBB2_843:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_844:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_844
	jmp	.LBB2_1109
.LBB2_160:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.161:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_162
# %bb.340:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_518
# %bb.341:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_518
.LBB2_162:
	xor	esi, esi
.LBB2_849:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_851
.LBB2_850:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_850
.LBB2_851:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_852:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_852
	jmp	.LBB2_1109
.LBB2_163:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.164:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_165
# %bb.343:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_521
# %bb.344:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_521
.LBB2_165:
	xor	esi, esi
.LBB2_857:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_859
.LBB2_858:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_858
.LBB2_859:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_860:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_860
	jmp	.LBB2_1109
.LBB2_166:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.167:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_168
# %bb.346:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_524
# %bb.347:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_524
.LBB2_168:
	xor	esi, esi
.LBB2_865:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_867
.LBB2_866:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	imul	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_866
.LBB2_867:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_868:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_868
	jmp	.LBB2_1109
.LBB2_169:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.170:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_171
# %bb.349:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_527
# %bb.350:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_527
.LBB2_171:
	xor	esi, esi
.LBB2_873:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_875
.LBB2_874:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_874
.LBB2_875:
	cmp	r9, 3
	jb	.LBB2_1109
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
	jmp	.LBB2_1109
.LBB2_172:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.173:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_174
# %bb.352:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_530
# %bb.353:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_530
.LBB2_174:
	xor	esi, esi
.LBB2_881:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_883
.LBB2_882:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_882
.LBB2_883:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_884:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_884
	jmp	.LBB2_1109
.LBB2_175:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.176:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_177
# %bb.355:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_533
# %bb.356:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_533
.LBB2_177:
	xor	esi, esi
.LBB2_889:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_891
.LBB2_890:                              # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_890
.LBB2_891:
	cmp	r9, 3
	jb	.LBB2_1109
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
	jmp	.LBB2_1109
.LBB2_178:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.179:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_180
# %bb.358:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_536
# %bb.359:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_536
.LBB2_180:
	xor	esi, esi
.LBB2_897:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_899
.LBB2_898:                              # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_898
.LBB2_899:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_900:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_900
	jmp	.LBB2_1109
.LBB2_181:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.182:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_183
# %bb.361:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_539
# %bb.362:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_539
.LBB2_183:
	xor	esi, esi
.LBB2_905:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_907
.LBB2_906:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_906
.LBB2_907:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_908:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_908
	jmp	.LBB2_1109
.LBB2_184:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.185:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_186
# %bb.364:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_542
# %bb.365:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_542
.LBB2_186:
	xor	esi, esi
.LBB2_913:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_915
.LBB2_914:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*rsi]
	add	di, ax
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_914
.LBB2_915:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_916:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_916
	jmp	.LBB2_1109
.LBB2_187:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.188:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_189
# %bb.367:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_545
# %bb.368:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_545
.LBB2_189:
	xor	esi, esi
.LBB2_921:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_923
.LBB2_922:                              # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_922
.LBB2_923:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_924:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_924
	jmp	.LBB2_1109
.LBB2_190:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.191:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_192
# %bb.370:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_548
# %bb.371:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_548
.LBB2_192:
	xor	esi, esi
.LBB2_929:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_931
.LBB2_930:                              # =>This Inner Loop Header: Depth=1
	mov	edi, eax
	sub	di, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_930
.LBB2_931:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_932:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_932
	jmp	.LBB2_1109
.LBB2_193:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.194:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_195
# %bb.373:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_551
# %bb.374:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_551
.LBB2_195:
	xor	esi, esi
.LBB2_937:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_939
.LBB2_938:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_938
.LBB2_939:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_940:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_940
	jmp	.LBB2_1109
.LBB2_196:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.197:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_198
# %bb.376:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_554
# %bb.377:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_554
.LBB2_198:
	xor	edx, edx
.LBB2_945:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_947
.LBB2_946:                              # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_946
.LBB2_947:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_948:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_948
	jmp	.LBB2_1109
.LBB2_199:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.200:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_201
# %bb.379:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_557
# %bb.380:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_557
.LBB2_201:
	xor	esi, esi
.LBB2_953:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_955
.LBB2_954:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	imul	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_954
.LBB2_955:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_956:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_956
	jmp	.LBB2_1109
.LBB2_202:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.203:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_204
# %bb.382:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_560
# %bb.383:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_560
.LBB2_204:
	xor	edx, edx
.LBB2_961:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_963
.LBB2_962:                              # =>This Inner Loop Header: Depth=1
	vmulss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_962
.LBB2_963:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_964:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_964
	jmp	.LBB2_1109
.LBB2_205:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.206:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_207
# %bb.385:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_563
# %bb.386:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_563
.LBB2_207:
	xor	esi, esi
.LBB2_969:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_971
.LBB2_970:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_970
.LBB2_971:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_972:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_972
	jmp	.LBB2_1109
.LBB2_208:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.209:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_210
# %bb.388:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_566
# %bb.389:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_566
.LBB2_210:
	xor	edx, edx
.LBB2_977:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_979
.LBB2_978:                              # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_978
.LBB2_979:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_980:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_980
	jmp	.LBB2_1109
.LBB2_211:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.212:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_213
# %bb.391:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_569
# %bb.392:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_569
.LBB2_213:
	xor	esi, esi
.LBB2_985:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_987
.LBB2_986:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_986
.LBB2_987:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_988:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_988
	jmp	.LBB2_1109
.LBB2_214:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.215:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_216
# %bb.394:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_572
# %bb.395:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_572
.LBB2_216:
	xor	edx, edx
.LBB2_993:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_995
.LBB2_994:                              # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_994
.LBB2_995:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_996:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_996
	jmp	.LBB2_1109
.LBB2_217:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.218:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_219
# %bb.397:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_575
# %bb.398:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_575
.LBB2_219:
	xor	esi, esi
.LBB2_1001:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1003
.LBB2_1002:                             # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1002
.LBB2_1003:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1004:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1004
	jmp	.LBB2_1109
.LBB2_220:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.221:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_222
# %bb.400:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_578
# %bb.401:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_578
.LBB2_222:
	xor	edx, edx
.LBB2_1009:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1011
.LBB2_1010:                             # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1010
.LBB2_1011:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_1012:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1012
	jmp	.LBB2_1109
.LBB2_223:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.224:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_225
# %bb.403:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_581
# %bb.404:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_581
.LBB2_225:
	xor	esi, esi
.LBB2_1017:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1019
.LBB2_1018:                             # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1018
.LBB2_1019:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_1020:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1020
	jmp	.LBB2_1109
.LBB2_226:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.227:
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB2_228
# %bb.406:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_584
# %bb.407:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_584
.LBB2_228:
	xor	edx, edx
.LBB2_1025:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_1027
.LBB2_1026:                             # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_1026
.LBB2_1027:
	cmp	rsi, 3
	jb	.LBB2_1109
.LBB2_1028:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1028
	jmp	.LBB2_1109
.LBB2_229:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.230:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_231
# %bb.409:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_587
# %bb.410:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_587
.LBB2_231:
	xor	edi, edi
.LBB2_647:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_649
.LBB2_648:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_648
.LBB2_649:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_650:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_650
	jmp	.LBB2_1109
.LBB2_232:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.233:
	mov	dl, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_234
# %bb.412:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_589
# %bb.413:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_589
.LBB2_234:
	xor	edi, edi
.LBB2_657:
	mov	r9, rdi
	not	r9
	add	r9, r10
	mov	rsi, r10
	and	rsi, 3
	je	.LBB2_659
.LBB2_658:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rdi]
	mul	dl
	mov	byte ptr [r8 + rdi], al
	add	rdi, 1
	add	rsi, -1
	jne	.LBB2_658
.LBB2_659:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_660:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_660
	jmp	.LBB2_1109
.LBB2_235:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.236:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_237
# %bb.415:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_591
# %bb.416:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_591
.LBB2_237:
	xor	esi, esi
.LBB2_1033:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1035
.LBB2_1034:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1034
.LBB2_1035:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1036:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1036
	jmp	.LBB2_1109
.LBB2_238:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.239:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_240
# %bb.418:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_594
# %bb.419:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_594
.LBB2_240:
	xor	esi, esi
.LBB2_1041:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1043
.LBB2_1042:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1042
.LBB2_1043:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1044:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1044
	jmp	.LBB2_1109
.LBB2_241:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.242:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_243
# %bb.421:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_597
# %bb.422:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_597
.LBB2_243:
	xor	esi, esi
.LBB2_1049:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1051
.LBB2_1050:                             # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1050
.LBB2_1051:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1052:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1052
	jmp	.LBB2_1109
.LBB2_244:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.245:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB2_246
# %bb.424:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_600
# %bb.425:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_600
.LBB2_246:
	xor	esi, esi
.LBB2_1057:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1059
.LBB2_1058:                             # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1058
.LBB2_1059:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1060:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1060
	jmp	.LBB2_1109
.LBB2_247:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.248:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_249
# %bb.427:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_603
# %bb.428:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_603
.LBB2_249:
	xor	esi, esi
.LBB2_1065:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1067
.LBB2_1066:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1066
.LBB2_1067:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1068:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1068
	jmp	.LBB2_1109
.LBB2_250:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.251:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_252
# %bb.430:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_606
# %bb.431:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_606
.LBB2_252:
	xor	esi, esi
.LBB2_1073:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1075
.LBB2_1074:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	imul	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1074
.LBB2_1075:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1076:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1076
	jmp	.LBB2_1109
.LBB2_253:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.254:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_255
# %bb.433:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_609
# %bb.434:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_609
.LBB2_255:
	xor	esi, esi
.LBB2_1081:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1083
.LBB2_1082:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1082
.LBB2_1083:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1084:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1084
	jmp	.LBB2_1109
.LBB2_256:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.257:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_258
# %bb.436:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_612
# %bb.437:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_612
.LBB2_258:
	xor	esi, esi
.LBB2_1089:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1091
.LBB2_1090:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1090
.LBB2_1091:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_1092:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1092
	jmp	.LBB2_1109
.LBB2_259:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.260:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_261
# %bb.439:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_615
# %bb.440:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_615
.LBB2_261:
	xor	esi, esi
.LBB2_1097:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1099
.LBB2_1098:                             # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1098
.LBB2_1099:
	cmp	r9, 3
	jb	.LBB2_1109
.LBB2_1100:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1100
	jmp	.LBB2_1109
.LBB2_262:
	test	r9d, r9d
	jle	.LBB2_1109
# %bb.263:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_264
# %bb.442:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_618
# %bb.443:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_618
.LBB2_264:
	xor	esi, esi
.LBB2_1105:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_1107
.LBB2_1106:                             # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_1106
.LBB2_1107:
	cmp	rdx, 3
	jb	.LBB2_1109
.LBB2_1108:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_1108
	jmp	.LBB2_1109
.LBB2_445:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_661
# %bb.446:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_447:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_447
	jmp	.LBB2_662
.LBB2_448:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_669
# %bb.449:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_450:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_450
	jmp	.LBB2_670
.LBB2_451:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_677
# %bb.452:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_453:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_453
	jmp	.LBB2_678
.LBB2_454:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_685
# %bb.455:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_456
	jmp	.LBB2_686
.LBB2_457:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_693
# %bb.458:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_459:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_459
	jmp	.LBB2_694
.LBB2_460:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_701
# %bb.461:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_462:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_462
	jmp	.LBB2_702
.LBB2_463:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_709
# %bb.464:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_465:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_465
	jmp	.LBB2_710
.LBB2_466:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_717
# %bb.467:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_468:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_468
	jmp	.LBB2_718
.LBB2_469:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_725
# %bb.470:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_471:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_471
	jmp	.LBB2_726
.LBB2_472:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_733
# %bb.473:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_474:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_474
	jmp	.LBB2_734
.LBB2_475:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_741
# %bb.476:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_477:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_477
	jmp	.LBB2_742
.LBB2_478:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	r9, rsi
	shr	r9, 4
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_749
# %bb.479:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_480:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_480
	jmp	.LBB2_750
.LBB2_481:
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
	jae	.LBB2_621
# %bb.482:
	xor	esi, esi
	jmp	.LBB2_623
.LBB2_483:
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
	jae	.LBB2_631
# %bb.484:
	xor	esi, esi
	jmp	.LBB2_633
.LBB2_485:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_757
# %bb.486:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_487:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_487
	jmp	.LBB2_758
.LBB2_488:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_765
# %bb.489:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_490:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_490
	jmp	.LBB2_766
.LBB2_491:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_773
# %bb.492:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_493:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_493
	jmp	.LBB2_774
.LBB2_494:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_781
# %bb.495:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_496:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_496
	jmp	.LBB2_782
.LBB2_497:
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
	je	.LBB2_789
# %bb.498:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_499:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_499
	jmp	.LBB2_790
.LBB2_500:
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
	je	.LBB2_797
# %bb.501:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_502:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_502
	jmp	.LBB2_798
.LBB2_503:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_805
# %bb.504:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_505:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_505
	jmp	.LBB2_806
.LBB2_506:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_813
# %bb.507:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_508:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_508
	jmp	.LBB2_814
.LBB2_509:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_821
# %bb.510:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_511:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_511
	jmp	.LBB2_822
.LBB2_512:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_829
# %bb.513:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_514:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_514
	jmp	.LBB2_830
.LBB2_515:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_837
# %bb.516:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_517:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_517
	jmp	.LBB2_838
.LBB2_518:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_845
# %bb.519:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_520:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_520
	jmp	.LBB2_846
.LBB2_521:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_853
# %bb.522:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_523:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_523
	jmp	.LBB2_854
.LBB2_524:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_861
# %bb.525:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_526:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_526
	jmp	.LBB2_862
.LBB2_527:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_869
# %bb.528:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_529:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_529
	jmp	.LBB2_870
.LBB2_530:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_877
# %bb.531:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_532:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_532
	jmp	.LBB2_878
.LBB2_533:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_885
# %bb.534:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_535:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_535
	jmp	.LBB2_886
.LBB2_536:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_893
# %bb.537:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_538:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_538
	jmp	.LBB2_894
.LBB2_539:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_901
# %bb.540:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_541:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_541
	jmp	.LBB2_902
.LBB2_542:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_909
# %bb.543:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_544:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_544
	jmp	.LBB2_910
.LBB2_545:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_917
# %bb.546:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_547:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_547
	jmp	.LBB2_918
.LBB2_548:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_925
# %bb.549:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_550:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_550
	jmp	.LBB2_926
.LBB2_551:
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
	je	.LBB2_933
# %bb.552:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_553:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_553
	jmp	.LBB2_934
.LBB2_554:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_941
# %bb.555:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_556:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_556
	jmp	.LBB2_942
.LBB2_557:
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
	je	.LBB2_949
# %bb.558:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_559:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_559
	jmp	.LBB2_950
.LBB2_560:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_957
# %bb.561:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_562:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_562
	jmp	.LBB2_958
.LBB2_563:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_965
# %bb.564:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_565:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_565
	jmp	.LBB2_966
.LBB2_566:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_973
# %bb.567:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_568:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_568
	jmp	.LBB2_974
.LBB2_569:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_981
# %bb.570:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_571:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_571
	jmp	.LBB2_982
.LBB2_572:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_989
# %bb.573:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_574:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_574
	jmp	.LBB2_990
.LBB2_575:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_997
# %bb.576:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_577:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_577
	jmp	.LBB2_998
.LBB2_578:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1005
# %bb.579:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_580:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_580
	jmp	.LBB2_1006
.LBB2_581:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1013
# %bb.582:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_583:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_583
	jmp	.LBB2_1014
.LBB2_584:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_1021
# %bb.585:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_586:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_586
	jmp	.LBB2_1022
.LBB2_587:
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
	jae	.LBB2_641
# %bb.588:
	xor	esi, esi
	jmp	.LBB2_643
.LBB2_589:
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
	jae	.LBB2_651
# %bb.590:
	xor	esi, esi
	jmp	.LBB2_653
.LBB2_591:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1029
# %bb.592:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_593
	jmp	.LBB2_1030
.LBB2_594:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1037
# %bb.595:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_596:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_596
	jmp	.LBB2_1038
.LBB2_597:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1045
# %bb.598:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_599:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_599
	jmp	.LBB2_1046
.LBB2_600:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1053
# %bb.601:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_602:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_602
	jmp	.LBB2_1054
.LBB2_603:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1061
# %bb.604:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_605:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_605
	jmp	.LBB2_1062
.LBB2_606:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1069
# %bb.607:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_608
	jmp	.LBB2_1070
.LBB2_609:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1077
# %bb.610:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_611:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_611
	jmp	.LBB2_1078
.LBB2_612:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1085
# %bb.613:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_614:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_614
	jmp	.LBB2_1086
.LBB2_615:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1093
# %bb.616:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_617:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_617
	jmp	.LBB2_1094
.LBB2_618:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_1101
# %bb.619:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_620:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_620
	jmp	.LBB2_1102
.LBB2_621:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_622:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_622
.LBB2_623:
	test	r9, r9
	je	.LBB2_626
# %bb.624:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_625:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_625
.LBB2_626:
	cmp	rdi, r10
	je	.LBB2_1109
	jmp	.LBB2_627
.LBB2_631:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_632:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_632
.LBB2_633:
	test	r9, r9
	je	.LBB2_636
# %bb.634:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_635:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_635
.LBB2_636:
	cmp	rdi, r10
	je	.LBB2_1109
	jmp	.LBB2_637
.LBB2_641:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_642:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_642
.LBB2_643:
	test	r9, r9
	je	.LBB2_646
# %bb.644:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_645:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_645
.LBB2_646:
	cmp	rdi, r10
	je	.LBB2_1109
	jmp	.LBB2_647
.LBB2_651:
	and	rax, -4
	neg	rax
	xor	esi, esi
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm3, ymm0, ymm0        # ymm3 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_652:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_652
.LBB2_653:
	test	r9, r9
	je	.LBB2_656
# %bb.654:
	neg	r9
	vpunpckhbw	ymm1, ymm0, ymm0        # ymm1 = ymm0[8,8,9,9,10,10,11,11,12,12,13,13,14,14,15,15,24,24,25,25,26,26,27,27,28,28,29,29,30,30,31,31]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI2_0] # ymm2 = [255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255]
	vpunpcklbw	ymm0, ymm0, ymm0        # ymm0 = ymm0[0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,16,16,17,17,18,18,19,19,20,20,21,21,22,22,23,23]
.LBB2_655:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_655
.LBB2_656:
	cmp	rdi, r10
	je	.LBB2_1109
	jmp	.LBB2_657
.LBB2_661:
	xor	edi, edi
.LBB2_662:
	test	r9b, 1
	je	.LBB2_664
# %bb.663:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_664:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_665
.LBB2_669:
	xor	edi, edi
.LBB2_670:
	test	r9b, 1
	je	.LBB2_672
# %bb.671:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_672:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_673
.LBB2_677:
	xor	edi, edi
.LBB2_678:
	test	r9b, 1
	je	.LBB2_680
# %bb.679:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_680:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_681
.LBB2_685:
	xor	edi, edi
.LBB2_686:
	test	r9b, 1
	je	.LBB2_688
# %bb.687:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_688:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_689
.LBB2_693:
	xor	edi, edi
.LBB2_694:
	test	r9b, 1
	je	.LBB2_696
# %bb.695:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_696:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_697
.LBB2_701:
	xor	edi, edi
.LBB2_702:
	test	r9b, 1
	je	.LBB2_704
# %bb.703:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_704:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_705
.LBB2_709:
	xor	edi, edi
.LBB2_710:
	test	r9b, 1
	je	.LBB2_712
# %bb.711:
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_712:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_713
.LBB2_717:
	xor	edi, edi
.LBB2_718:
	test	r9b, 1
	je	.LBB2_720
# %bb.719:
	vmulpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vmulpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vmulpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vmulpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_720:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_721
.LBB2_725:
	xor	edi, edi
.LBB2_726:
	test	r9b, 1
	je	.LBB2_728
# %bb.727:
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_728:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_729
.LBB2_733:
	xor	edi, edi
.LBB2_734:
	test	r9b, 1
	je	.LBB2_736
# %bb.735:
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_736:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_737
.LBB2_741:
	xor	edi, edi
.LBB2_742:
	test	r9b, 1
	je	.LBB2_744
# %bb.743:
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_744:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_745
.LBB2_749:
	xor	edi, edi
.LBB2_750:
	test	r9b, 1
	je	.LBB2_752
# %bb.751:
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
.LBB2_752:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_753
.LBB2_757:
	xor	edi, edi
.LBB2_758:
	test	r9b, 1
	je	.LBB2_760
# %bb.759:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_760:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_761
.LBB2_765:
	xor	edi, edi
.LBB2_766:
	test	r9b, 1
	je	.LBB2_768
# %bb.767:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_768:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_769
.LBB2_773:
	xor	edi, edi
.LBB2_774:
	test	r9b, 1
	je	.LBB2_776
# %bb.775:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_776:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_777
.LBB2_781:
	xor	edi, edi
.LBB2_782:
	test	r9b, 1
	je	.LBB2_784
# %bb.783:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_784:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_785
.LBB2_789:
	xor	edi, edi
.LBB2_790:
	test	r9b, 1
	je	.LBB2_792
# %bb.791:
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
.LBB2_792:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_793
.LBB2_797:
	xor	edi, edi
.LBB2_798:
	test	r9b, 1
	je	.LBB2_800
# %bb.799:
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
.LBB2_800:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_801
.LBB2_805:
	xor	edi, edi
.LBB2_806:
	test	r9b, 1
	je	.LBB2_808
# %bb.807:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_808:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_809
.LBB2_813:
	xor	edi, edi
.LBB2_814:
	test	r9b, 1
	je	.LBB2_816
# %bb.815:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_816:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_817
.LBB2_821:
	xor	edi, edi
.LBB2_822:
	test	r9b, 1
	je	.LBB2_824
# %bb.823:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_824:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_825
.LBB2_829:
	xor	edi, edi
.LBB2_830:
	test	r9b, 1
	je	.LBB2_832
# %bb.831:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_832:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_833
.LBB2_837:
	xor	edi, edi
.LBB2_838:
	test	r9b, 1
	je	.LBB2_840
# %bb.839:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_840:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_841
.LBB2_845:
	xor	edi, edi
.LBB2_846:
	test	r9b, 1
	je	.LBB2_848
# %bb.847:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_848:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_849
.LBB2_853:
	xor	edi, edi
.LBB2_854:
	test	r9b, 1
	je	.LBB2_856
# %bb.855:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_856:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_857
.LBB2_861:
	xor	edi, edi
.LBB2_862:
	test	r9b, 1
	je	.LBB2_864
# %bb.863:
	vpmullw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpmullw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_864:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_865
.LBB2_869:
	xor	edi, edi
.LBB2_870:
	test	r9b, 1
	je	.LBB2_872
# %bb.871:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_872:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_873
.LBB2_877:
	xor	edi, edi
.LBB2_878:
	test	r9b, 1
	je	.LBB2_880
# %bb.879:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_880:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_881
.LBB2_885:
	xor	edi, edi
.LBB2_886:
	test	r9b, 1
	je	.LBB2_888
# %bb.887:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_888:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_889
.LBB2_893:
	xor	edi, edi
.LBB2_894:
	test	r9b, 1
	je	.LBB2_896
# %bb.895:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_896:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_897
.LBB2_901:
	xor	edi, edi
.LBB2_902:
	test	r9b, 1
	je	.LBB2_904
# %bb.903:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_904:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_905
.LBB2_909:
	xor	edi, edi
.LBB2_910:
	test	r9b, 1
	je	.LBB2_912
# %bb.911:
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_912:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_913
.LBB2_917:
	xor	edi, edi
.LBB2_918:
	test	r9b, 1
	je	.LBB2_920
# %bb.919:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_920:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_921
.LBB2_925:
	xor	edi, edi
.LBB2_926:
	test	r9b, 1
	je	.LBB2_928
# %bb.927:
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB2_928:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_929
.LBB2_933:
	xor	edi, edi
.LBB2_934:
	test	r9b, 1
	je	.LBB2_936
# %bb.935:
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
.LBB2_936:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_937
.LBB2_941:
	xor	edi, edi
.LBB2_942:
	test	r9b, 1
	je	.LBB2_944
# %bb.943:
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_944:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_945
.LBB2_949:
	xor	edi, edi
.LBB2_950:
	test	r9b, 1
	je	.LBB2_952
# %bb.951:
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
.LBB2_952:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_953
.LBB2_957:
	xor	edi, edi
.LBB2_958:
	test	r9b, 1
	je	.LBB2_960
# %bb.959:
	vmulps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vmulps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vmulps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vmulps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_960:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_961
.LBB2_965:
	xor	edi, edi
.LBB2_966:
	test	r9b, 1
	je	.LBB2_968
# %bb.967:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_968:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_969
.LBB2_973:
	xor	edi, edi
.LBB2_974:
	test	r9b, 1
	je	.LBB2_976
# %bb.975:
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_976:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_977
.LBB2_981:
	xor	edi, edi
.LBB2_982:
	test	r9b, 1
	je	.LBB2_984
# %bb.983:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_984:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_985
.LBB2_989:
	xor	edi, edi
.LBB2_990:
	test	r9b, 1
	je	.LBB2_992
# %bb.991:
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_992:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_993
.LBB2_997:
	xor	edi, edi
.LBB2_998:
	test	r9b, 1
	je	.LBB2_1000
# %bb.999:
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1000:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1001
.LBB2_1005:
	xor	edi, edi
.LBB2_1006:
	test	r9b, 1
	je	.LBB2_1008
# %bb.1007:
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1008:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_1009
.LBB2_1013:
	xor	edi, edi
.LBB2_1014:
	test	r9b, 1
	je	.LBB2_1016
# %bb.1015:
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 8*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rdi + 96], ymm0
.LBB2_1016:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1017
.LBB2_1021:
	xor	edi, edi
.LBB2_1022:
	test	r9b, 1
	je	.LBB2_1024
# %bb.1023:
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
.LBB2_1024:
	cmp	rdx, rax
	je	.LBB2_1109
	jmp	.LBB2_1025
.LBB2_1029:
	xor	edi, edi
.LBB2_1030:
	test	r9b, 1
	je	.LBB2_1032
# %bb.1031:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1032:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1033
.LBB2_1037:
	xor	edi, edi
.LBB2_1038:
	test	r9b, 1
	je	.LBB2_1040
# %bb.1039:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1040:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1041
.LBB2_1045:
	xor	edi, edi
.LBB2_1046:
	test	r9b, 1
	je	.LBB2_1048
# %bb.1047:
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1048:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1049
.LBB2_1053:
	xor	edi, edi
.LBB2_1054:
	test	r9b, 1
	je	.LBB2_1056
# %bb.1055:
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rdi + 96]
	vmovdqu	ymmword ptr [r8 + rdi], ymm1
	vmovdqu	ymmword ptr [r8 + rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rdi + 96], ymm0
.LBB2_1056:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1057
.LBB2_1061:
	xor	edi, edi
.LBB2_1062:
	test	r9b, 1
	je	.LBB2_1064
# %bb.1063:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1064:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1065
.LBB2_1069:
	xor	edi, edi
.LBB2_1070:
	test	r9b, 1
	je	.LBB2_1072
# %bb.1071:
	vpmulld	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpmulld	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpmulld	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpmulld	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1072:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1073
.LBB2_1077:
	xor	edi, edi
.LBB2_1078:
	test	r9b, 1
	je	.LBB2_1080
# %bb.1079:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1080:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1081
.LBB2_1085:
	xor	edi, edi
.LBB2_1086:
	test	r9b, 1
	je	.LBB2_1088
# %bb.1087:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1088:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1089
.LBB2_1093:
	xor	edi, edi
.LBB2_1094:
	test	r9b, 1
	je	.LBB2_1096
# %bb.1095:
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1096:
	cmp	rsi, r10
	je	.LBB2_1109
	jmp	.LBB2_1097
.LBB2_1101:
	xor	edi, edi
.LBB2_1102:
	test	r9b, 1
	je	.LBB2_1104
# %bb.1103:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rdi + 96]
	vmovdqu	ymmword ptr [r8 + 4*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rdi + 96], ymm0
.LBB2_1104:
	cmp	rsi, r10
	jne	.LBB2_1105
.LBB2_1109:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.Lfunc_end2:
	.size	arithmetic_scalar_arr_avx2, .Lfunc_end2-arithmetic_scalar_arr_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_unary_same_types_avx2
.LCPI3_0:
	.quad	0x8000000000000000              # double -0
.LCPI3_1:
	.quad	0x3ff0000000000000              # double 1
.LCPI3_4:
	.quad	1                               # 0x1
.LCPI3_8:
	.quad	9223372036854775807             # 0x7fffffffffffffff
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI3_2:
	.quad	0x8000000000000000              # double -0
	.quad	0x8000000000000000              # double -0
.LCPI3_11:
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
.LCPI3_3:
	.long	1                               # 0x1
.LCPI3_7:
	.long	0x80000000                      # float -0
.LCPI3_9:
	.long	2147483647                      # 0x7fffffff
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI3_5:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
.LCPI3_6:
	.zero	32,1
.LCPI3_10:
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
	.text
	.globl	arithmetic_unary_same_types_avx2
	.p2align	4, 0x90
	.type	arithmetic_unary_same_types_avx2,@function
arithmetic_unary_same_types_avx2:       # @arithmetic_unary_same_types_avx2
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
	jne	.LBB3_865
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
	jne	.LBB3_865
# %bb.9:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.10:
	mov	r9d, r8d
	cmp	r8d, 32
	jae	.LBB3_221
# %bb.11:
	xor	edx, edx
	jmp	.LBB3_373
.LBB3_12:
	cmp	sil, 4
	je	.LBB3_38
# %bb.13:
	cmp	sil, 5
	jne	.LBB3_865
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
	jne	.LBB3_865
# %bb.19:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.20:
	mov	r9d, r8d
	cmp	r8d, 32
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
.LBB3_616:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_618
.LBB3_617:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_617
.LBB3_618:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_619:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_619
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.27:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.28:
	mov	r9d, r8d
	cmp	r8d, 32
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
.LBB3_380:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_382
.LBB3_381:                              # =>This Inner Loop Header: Depth=1
	xor	edi, edi
	cmp	dword ptr [rdx + 4*rsi], 0
	setne	dil
	mov	dword ptr [rcx + 4*rsi], edi
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_381
.LBB3_382:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_383:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_383
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.35:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.36:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_37
# %bb.229:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_384
# %bb.230:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_384
.LBB3_37:
	xor	esi, esi
.LBB3_624:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_626
.LBB3_625:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_625
.LBB3_626:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_627:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_627
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.43:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.44:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_45
# %bb.232:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_387
# %bb.233:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_387
.LBB3_45:
	xor	esi, esi
.LBB3_632:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_634
.LBB3_633:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_633
.LBB3_634:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_635:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_635
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.50:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.51:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_52
# %bb.235:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_390
# %bb.236:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_390
.LBB3_52:
	xor	esi, esi
.LBB3_640:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_643
# %bb.641:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI3_2] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_642:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_642
.LBB3_643:
	cmp	rax, 3
	jb	.LBB3_865
# %bb.644:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI3_2] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_645:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi + 8], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi + 16], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_645
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.57:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.58:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_59
# %bb.238:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_393
# %bb.239:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_393
.LBB3_59:
	xor	esi, esi
.LBB3_650:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_653
# %bb.651:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI3_2] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_652:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_652
.LBB3_653:
	cmp	rax, 3
	jb	.LBB3_865
# %bb.654:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI3_2] # xmm0 = [-0.0E+0,-0.0E+0]
.LBB3_655:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi + 8], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi + 16], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	vxorpd	xmm1, xmm1, xmm0
	vmovlpd	qword ptr [rcx + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_655
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.64:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.65:
	mov	eax, r8d
	cmp	r8d, 16
	jb	.LBB3_66
# %bb.241:
	lea	rsi, [rdx + 8*rax]
	cmp	rsi, rcx
	jbe	.LBB3_396
# %bb.242:
	lea	rsi, [rcx + 8*rax]
	cmp	rsi, rdx
	jbe	.LBB3_396
.LBB3_66:
	xor	esi, esi
.LBB3_399:
	mov	rdi, rsi
	not	rdi
	test	al, 1
	je	.LBB3_401
# %bb.400:
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vandpd	xmm1, xmm0, xmmword ptr [rip + .LCPI3_2]
	vmovddup	xmm2, qword ptr [rip + .LCPI3_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
	vorpd	xmm1, xmm2, xmm1
	vxorpd	xmm2, xmm2, xmm2
	vcmpeqsd	xmm0, xmm0, xmm2
	vandnpd	xmm0, xmm0, xmm1
	vmovlpd	qword ptr [rcx + 8*rsi], xmm0
	or	rsi, 1
.LBB3_401:
	add	rdi, rax
	je	.LBB3_865
# %bb.402:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI3_2] # xmm0 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm1, qword ptr [rip + .LCPI3_1] # xmm1 = [1.0E+0,1.0E+0]
                                        # xmm1 = mem[0,0]
	vxorpd	xmm2, xmm2, xmm2
.LBB3_403:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rdx + 8*rsi]   # xmm3 = mem[0],zero
	vandpd	xmm4, xmm3, xmm0
	vorpd	xmm4, xmm1, xmm4
	vcmpeqsd	xmm3, xmm3, xmm2
	vandnpd	xmm3, xmm3, xmm4
	vmovlpd	qword ptr [rcx + 8*rsi], xmm3
	vmovsd	xmm3, qword ptr [rdx + 8*rsi + 8] # xmm3 = mem[0],zero
	vandpd	xmm4, xmm3, xmm0
	vorpd	xmm4, xmm1, xmm4
	vcmpeqsd	xmm3, xmm3, xmm2
	vandnpd	xmm3, xmm3, xmm4
	vmovlpd	qword ptr [rcx + 8*rsi + 8], xmm3
	add	rsi, 2
	cmp	rax, rsi
	jne	.LBB3_403
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.71:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.72:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_73
# %bb.244:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_404
# %bb.245:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_404
.LBB3_73:
	xor	esi, esi
.LBB3_660:
	movabs	r10, 9223372036854775807
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_662
.LBB3_661:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	and	rdi, r10
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_661
.LBB3_662:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_663:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_663
	jmp	.LBB3_865
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
	jne	.LBB3_865
# %bb.78:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.79:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_80
# %bb.247:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_407
# %bb.248:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_407
.LBB3_80:
	xor	esi, esi
.LBB3_668:
	movabs	r10, 9223372036854775807
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_670
.LBB3_669:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rdx + 8*rsi]
	and	rdi, r10
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_669
.LBB3_670:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_671:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_671
	jmp	.LBB3_865
.LBB3_81:
	cmp	edi, 2
	je	.LBB3_191
# %bb.82:
	cmp	edi, 3
	jne	.LBB3_865
# %bb.83:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.84:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB3_85
# %bb.250:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_410
# %bb.251:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_410
.LBB3_85:
	xor	esi, esi
.LBB3_676:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_678
.LBB3_677:                              # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdx + rsi]
	xor	eax, eax
	sub	al, r10b
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_677
.LBB3_678:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_679:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_679
	jmp	.LBB3_865
.LBB3_86:
	cmp	edi, 2
	je	.LBB3_194
# %bb.87:
	cmp	edi, 3
	jne	.LBB3_865
# %bb.88:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.89:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB3_90
# %bb.253:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_413
# %bb.254:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_413
.LBB3_90:
	xor	esi, esi
.LBB3_684:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_686
.LBB3_685:                              # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdx + rsi]
	xor	eax, eax
	sub	al, r10b
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_685
.LBB3_686:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_687:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_687
	jmp	.LBB3_865
.LBB3_91:
	cmp	edi, 2
	je	.LBB3_197
# %bb.92:
	cmp	edi, 3
	jne	.LBB3_865
# %bb.93:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.94:
	mov	r11d, r8d
	cmp	r8d, 128
	jb	.LBB3_95
# %bb.256:
	lea	rsi, [rdx + r11]
	cmp	rsi, rcx
	jbe	.LBB3_416
# %bb.257:
	lea	rsi, [rcx + r11]
	cmp	rsi, rdx
	jbe	.LBB3_416
.LBB3_95:
	xor	esi, esi
.LBB3_419:
	mov	r10, rsi
	not	r10
	test	r11b, 1
	je	.LBB3_421
# %bb.420:
	mov	r8b, byte ptr [rdx + rsi]
	test	r8b, r8b
	setne	r9b
	neg	r9b
	test	r8b, r8b
	movzx	r8d, r9b
	mov	edi, 1
	cmovle	edi, r8d
	mov	byte ptr [rcx + rsi], dil
	or	rsi, 1
.LBB3_421:
	add	r10, r11
	je	.LBB3_865
# %bb.422:
	mov	edi, 1
.LBB3_423:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB3_423
	jmp	.LBB3_865
.LBB3_96:
	cmp	edi, 2
	je	.LBB3_200
# %bb.97:
	cmp	edi, 3
	jne	.LBB3_865
# %bb.98:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.99:
	mov	r10d, r8d
	cmp	r8d, 32
	jb	.LBB3_100
# %bb.259:
	lea	rsi, [rdx + r10]
	cmp	rsi, rcx
	jbe	.LBB3_424
# %bb.260:
	lea	rsi, [rcx + r10]
	cmp	rsi, rdx
	jbe	.LBB3_424
.LBB3_100:
	xor	esi, esi
.LBB3_427:
	mov	r8, rsi
	not	r8
	test	r10b, 1
	je	.LBB3_429
# %bb.428:
	movsx	edi, byte ptr [rdx + rsi]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [rcx + rsi], dil
	or	rsi, 1
.LBB3_429:
	add	r8, r10
	je	.LBB3_865
.LBB3_430:                              # =>This Inner Loop Header: Depth=1
	movsx	edi, byte ptr [rdx + rsi]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [rcx + rsi], dil
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	edi, eax
	sar	edi, 7
	add	eax, edi
	xor	eax, edi
	mov	byte ptr [rcx + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB3_430
	jmp	.LBB3_865
.LBB3_101:
	cmp	edi, 2
	je	.LBB3_203
# %bb.102:
	cmp	edi, 3
	jne	.LBB3_865
# %bb.103:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.104:
	mov	r10d, r8d
	cmp	r8d, 32
	jb	.LBB3_105
# %bb.262:
	lea	rsi, [rdx + r10]
	cmp	rsi, rcx
	jbe	.LBB3_431
# %bb.263:
	lea	rsi, [rcx + r10]
	cmp	rsi, rdx
	jbe	.LBB3_431
.LBB3_105:
	xor	esi, esi
.LBB3_434:
	mov	r8, rsi
	not	r8
	test	r10b, 1
	je	.LBB3_436
# %bb.435:
	movsx	edi, byte ptr [rdx + rsi]
	mov	r9d, edi
	sar	r9d, 7
	add	edi, r9d
	xor	edi, r9d
	mov	byte ptr [rcx + rsi], dil
	or	rsi, 1
.LBB3_436:
	add	r8, r10
	je	.LBB3_865
.LBB3_437:                              # =>This Inner Loop Header: Depth=1
	movsx	edi, byte ptr [rdx + rsi]
	mov	eax, edi
	sar	eax, 7
	add	edi, eax
	xor	edi, eax
	mov	byte ptr [rcx + rsi], dil
	movsx	eax, byte ptr [rdx + rsi + 1]
	mov	edi, eax
	sar	edi, 7
	add	eax, edi
	xor	eax, edi
	mov	byte ptr [rcx + rsi + 1], al
	add	rsi, 2
	cmp	r10, rsi
	jne	.LBB3_437
	jmp	.LBB3_865
.LBB3_106:
	cmp	edi, 7
	je	.LBB3_206
# %bb.107:
	cmp	edi, 8
	jne	.LBB3_865
# %bb.108:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.109:
	mov	r9d, r8d
	cmp	r8d, 16
	jae	.LBB3_265
# %bb.110:
	xor	edx, edx
	jmp	.LBB3_444
.LBB3_111:
	cmp	edi, 7
	je	.LBB3_209
# %bb.112:
	cmp	edi, 8
	jne	.LBB3_865
# %bb.113:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.114:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_115
# %bb.267:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_445
# %bb.268:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_445
.LBB3_115:
	xor	esi, esi
.LBB3_692:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_694
.LBB3_693:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_693
.LBB3_694:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_695:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_695
	jmp	.LBB3_865
.LBB3_116:
	cmp	edi, 7
	je	.LBB3_212
# %bb.117:
	cmp	edi, 8
	jne	.LBB3_865
# %bb.118:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.119:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_120
# %bb.270:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_448
# %bb.271:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_448
.LBB3_120:
	xor	esi, esi
.LBB3_451:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_453
.LBB3_452:                              # =>This Inner Loop Header: Depth=1
	xor	edi, edi
	cmp	qword ptr [rdx + 8*rsi], 0
	setne	dil
	mov	qword ptr [rcx + 8*rsi], rdi
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_452
.LBB3_453:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_454:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_454
	jmp	.LBB3_865
.LBB3_121:
	cmp	edi, 7
	je	.LBB3_215
# %bb.122:
	cmp	edi, 8
	jne	.LBB3_865
# %bb.123:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.124:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_125
# %bb.273:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_455
# %bb.274:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_455
.LBB3_125:
	xor	esi, esi
.LBB3_700:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_702
.LBB3_701:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_701
.LBB3_702:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_703:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_703
	jmp	.LBB3_865
.LBB3_126:
	cmp	edi, 7
	je	.LBB3_218
# %bb.127:
	cmp	edi, 8
	jne	.LBB3_865
# %bb.128:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.129:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_130
# %bb.276:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_458
# %bb.277:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_458
.LBB3_130:
	xor	esi, esi
.LBB3_708:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_710
.LBB3_709:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_709
.LBB3_710:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_711:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_711
	jmp	.LBB3_865
.LBB3_131:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.132:
	mov	r9d, r8d
	cmp	r8d, 64
	jae	.LBB3_279
# %bb.133:
	xor	edx, edx
	jmp	.LBB3_467
.LBB3_134:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.135:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_136
# %bb.281:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_468
# %bb.282:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_468
.LBB3_136:
	xor	esi, esi
.LBB3_716:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_718
.LBB3_717:                              # =>This Inner Loop Header: Depth=1
	xor	edi, edi
	sub	di, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_717
.LBB3_718:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_719:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_719
	jmp	.LBB3_865
.LBB3_137:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.138:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_139
# %bb.284:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_471
# %bb.285:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_471
.LBB3_139:
	xor	esi, esi
.LBB3_724:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_726
.LBB3_725:                              # =>This Inner Loop Header: Depth=1
	xor	edi, edi
	sub	di, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_725
.LBB3_726:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_727:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_727
	jmp	.LBB3_865
.LBB3_140:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.141:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_142
# %bb.287:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_474
# %bb.288:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_474
.LBB3_142:
	xor	esi, esi
.LBB3_732:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_734
.LBB3_733:                              # =>This Inner Loop Header: Depth=1
	xor	edi, edi
	sub	di, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_733
.LBB3_734:
	cmp	r8, 3
	jb	.LBB3_865
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
	jmp	.LBB3_865
.LBB3_143:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.144:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_145
# %bb.290:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_477
# %bb.291:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_477
.LBB3_145:
	xor	esi, esi
.LBB3_740:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_742
.LBB3_741:                              # =>This Inner Loop Header: Depth=1
	xor	edi, edi
	cmp	word ptr [rdx + 2*rsi], 0
	setne	dil
	mov	word ptr [rcx + 2*rsi], di
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_741
.LBB3_742:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_743:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_743
	jmp	.LBB3_865
.LBB3_146:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.147:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_148
# %bb.293:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_480
# %bb.294:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_480
.LBB3_148:
	xor	esi, esi
.LBB3_748:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_750
# %bb.749:
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
.LBB3_750:
	add	rax, r9
	je	.LBB3_865
# %bb.751:
	mov	r8d, 1
.LBB3_752:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_752
	jmp	.LBB3_865
.LBB3_149:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.150:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_151
# %bb.296:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_483
# %bb.297:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_483
.LBB3_151:
	xor	esi, esi
.LBB3_598:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_600
.LBB3_599:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_599
.LBB3_600:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_601:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_601
	jmp	.LBB3_865
.LBB3_152:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.153:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_154
# %bb.299:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_485
# %bb.300:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_485
.LBB3_154:
	xor	esi, esi
.LBB3_757:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_759
# %bb.758:
	movsx	edi, word ptr [rdx + 2*rsi]
	mov	r8d, edi
	sar	r8d, 15
	add	edi, r8d
	xor	edi, r8d
	mov	word ptr [rcx + 2*rsi], di
	or	rsi, 1
.LBB3_759:
	add	rax, r9
	je	.LBB3_865
.LBB3_760:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_760
	jmp	.LBB3_865
.LBB3_155:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.156:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_157
# %bb.302:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_488
# %bb.303:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_488
.LBB3_157:
	xor	esi, esi
.LBB3_608:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_610
.LBB3_609:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	mov	word ptr [rcx + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_609
.LBB3_610:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_611:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_611
	jmp	.LBB3_865
.LBB3_158:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.159:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_160
# %bb.305:
	lea	rax, [rdx + 2*r9]
	cmp	rax, rcx
	jbe	.LBB3_490
# %bb.306:
	lea	rax, [rcx + 2*r9]
	cmp	rax, rdx
	jbe	.LBB3_490
.LBB3_160:
	xor	esi, esi
.LBB3_765:
	mov	rax, rsi
	not	rax
	test	r9b, 1
	je	.LBB3_767
# %bb.766:
	movsx	edi, word ptr [rdx + 2*rsi]
	mov	r8d, edi
	sar	r8d, 15
	add	edi, r8d
	xor	edi, r8d
	mov	word ptr [rcx + 2*rsi], di
	or	rsi, 1
.LBB3_767:
	add	rax, r9
	je	.LBB3_865
.LBB3_768:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_768
	jmp	.LBB3_865
.LBB3_161:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.162:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_163
# %bb.308:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_493
# %bb.309:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_493
.LBB3_163:
	xor	esi, esi
.LBB3_773:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_775
.LBB3_774:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_774
.LBB3_775:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_776:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_776
	jmp	.LBB3_865
.LBB3_164:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.165:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_166
# %bb.311:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_496
# %bb.312:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_496
.LBB3_166:
	xor	esi, esi
.LBB3_781:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_784
# %bb.782:
	vbroadcastss	xmm0, dword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_783:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_783
.LBB3_784:
	cmp	rax, 3
	jb	.LBB3_865
# %bb.785:
	vbroadcastss	xmm0, dword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_786:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi + 4], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi + 8], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_786
	jmp	.LBB3_865
.LBB3_167:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.168:
	mov	r9d, r8d
	cmp	r8d, 16
	jb	.LBB3_169
# %bb.314:
	lea	rax, [rdx + 8*r9]
	cmp	rax, rcx
	jbe	.LBB3_499
# %bb.315:
	lea	rax, [rcx + 8*r9]
	cmp	rax, rdx
	jbe	.LBB3_499
.LBB3_169:
	xor	esi, esi
.LBB3_791:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_793
.LBB3_792:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [rcx + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_792
.LBB3_793:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_794:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_794
	jmp	.LBB3_865
.LBB3_170:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.171:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_172
# %bb.317:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_502
# %bb.318:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_502
.LBB3_172:
	xor	esi, esi
.LBB3_799:
	mov	rax, rsi
	not	rax
	add	rax, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_802
# %bb.800:
	vbroadcastss	xmm0, dword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_801:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi], xmm1
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_801
.LBB3_802:
	cmp	rax, 3
	jb	.LBB3_865
# %bb.803:
	vbroadcastss	xmm0, dword ptr [rip + .LCPI3_7] # xmm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_804:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi + 4], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi + 8], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	vxorpd	xmm1, xmm1, xmm0
	vmovss	dword ptr [rcx + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r9, rsi
	jne	.LBB3_804
	jmp	.LBB3_865
.LBB3_173:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.174:
	mov	r11d, r8d
	cmp	r8d, 16
	jb	.LBB3_175
# %bb.320:
	lea	rsi, [rdx + 8*r11]
	cmp	rsi, rcx
	jbe	.LBB3_505
# %bb.321:
	lea	rsi, [rcx + 8*r11]
	cmp	rsi, rdx
	jbe	.LBB3_505
.LBB3_175:
	xor	esi, esi
.LBB3_508:
	mov	r10, rsi
	not	r10
	test	r11b, 1
	je	.LBB3_510
# %bb.509:
	mov	r8, qword ptr [rdx + 8*rsi]
	xor	r9d, r9d
	test	r8, r8
	setne	r9b
	neg	r9
	test	r8, r8
	mov	edi, 1
	cmovle	rdi, r9
	mov	qword ptr [rcx + 8*rsi], rdi
	or	rsi, 1
.LBB3_510:
	add	r10, r11
	je	.LBB3_865
# %bb.511:
	mov	r8d, 1
.LBB3_512:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	xor	edi, edi
	test	rax, rax
	setne	dil
	neg	rdi
	test	rax, rax
	cmovg	rdi, r8
	mov	qword ptr [rcx + 8*rsi], rdi
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	xor	edi, edi
	test	rax, rax
	setne	dil
	neg	rdi
	test	rax, rax
	cmovg	rdi, r8
	mov	qword ptr [rcx + 8*rsi + 8], rdi
	add	rsi, 2
	cmp	r11, rsi
	jne	.LBB3_512
	jmp	.LBB3_865
.LBB3_176:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.177:
	mov	eax, r8d
	cmp	r8d, 32
	jb	.LBB3_178
# %bb.323:
	lea	rsi, [rdx + 4*rax]
	cmp	rsi, rcx
	jbe	.LBB3_513
# %bb.324:
	lea	rsi, [rcx + 4*rax]
	cmp	rsi, rdx
	jbe	.LBB3_513
.LBB3_178:
	xor	esi, esi
.LBB3_516:
	mov	r8, rsi
	not	r8
	test	al, 1
	je	.LBB3_518
# %bb.517:
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vmovmskps	edi, xmm0
	and	edi, 1
	neg	edi
	or	edi, 1
	vcvtsi2ss	xmm1, xmm10, edi
	vxorps	xmm2, xmm2, xmm2
	vcmpeqss	xmm0, xmm0, xmm2
	vandnps	xmm0, xmm0, xmm1
	vmovss	dword ptr [rcx + 4*rsi], xmm0
	or	rsi, 1
.LBB3_518:
	add	r8, rax
	je	.LBB3_865
# %bb.519:
	vxorps	xmm0, xmm0, xmm0
.LBB3_520:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vmovmskps	edi, xmm1
	and	edi, 1
	neg	edi
	or	edi, 1
	vcvtsi2ss	xmm2, xmm10, edi
	vcmpeqss	xmm1, xmm1, xmm0
	vandnps	xmm1, xmm1, xmm2
	vmovss	dword ptr [rcx + 4*rsi], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	vmovmskps	edi, xmm1
	and	edi, 1
	neg	edi
	or	edi, 1
	vcvtsi2ss	xmm2, xmm10, edi
	vcmpeqss	xmm1, xmm1, xmm0
	vandnps	xmm1, xmm1, xmm2
	vmovss	dword ptr [rcx + 4*rsi + 4], xmm1
	add	rsi, 2
	cmp	rax, rsi
	jne	.LBB3_520
	jmp	.LBB3_865
.LBB3_179:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.180:
	mov	r10d, r8d
	cmp	r8d, 16
	jb	.LBB3_181
# %bb.326:
	lea	rsi, [rdx + 8*r10]
	cmp	rsi, rcx
	jbe	.LBB3_521
# %bb.327:
	lea	rsi, [rcx + 8*r10]
	cmp	rsi, rdx
	jbe	.LBB3_521
.LBB3_181:
	xor	esi, esi
.LBB3_524:
	mov	r9, rsi
	not	r9
	test	r10b, 1
	je	.LBB3_526
# %bb.525:
	mov	r8, qword ptr [rdx + 8*rsi]
	mov	rdi, r8
	neg	rdi
	cmovl	rdi, r8
	mov	qword ptr [rcx + 8*rsi], rdi
	or	rsi, 1
.LBB3_526:
	add	r9, r10
	je	.LBB3_865
.LBB3_527:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r10, rsi
	jne	.LBB3_527
	jmp	.LBB3_865
.LBB3_182:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.183:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_184
# %bb.329:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_528
# %bb.330:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_528
.LBB3_184:
	xor	esi, esi
.LBB3_809:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_812
# %bb.810:
	mov	r10d, 2147483647
.LBB3_811:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	and	eax, r10d
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_811
.LBB3_812:
	cmp	r8, 3
	jb	.LBB3_865
# %bb.813:
	mov	eax, 2147483647
.LBB3_814:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_814
	jmp	.LBB3_865
.LBB3_185:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.186:
	mov	r10d, r8d
	cmp	r8d, 16
	jb	.LBB3_187
# %bb.332:
	lea	rsi, [rdx + 8*r10]
	cmp	rsi, rcx
	jbe	.LBB3_531
# %bb.333:
	lea	rsi, [rcx + 8*r10]
	cmp	rsi, rdx
	jbe	.LBB3_531
.LBB3_187:
	xor	esi, esi
.LBB3_534:
	mov	r9, rsi
	not	r9
	test	r10b, 1
	je	.LBB3_536
# %bb.535:
	mov	r8, qword ptr [rdx + 8*rsi]
	mov	rdi, r8
	neg	rdi
	cmovl	rdi, r8
	mov	qword ptr [rcx + 8*rsi], rdi
	or	rsi, 1
.LBB3_536:
	add	r9, r10
	je	.LBB3_865
.LBB3_537:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r10, rsi
	jne	.LBB3_537
	jmp	.LBB3_865
.LBB3_188:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.189:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_190
# %bb.335:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_538
# %bb.336:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_538
.LBB3_190:
	xor	esi, esi
.LBB3_819:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_822
# %bb.820:
	mov	r10d, 2147483647
.LBB3_821:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	and	eax, r10d
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_821
.LBB3_822:
	cmp	r8, 3
	jb	.LBB3_865
# %bb.823:
	mov	eax, 2147483647
.LBB3_824:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_824
	jmp	.LBB3_865
.LBB3_191:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.192:
	mov	r9d, r8d
	cmp	r8d, 128
	jae	.LBB3_338
# %bb.193:
	xor	edx, edx
	jmp	.LBB3_547
.LBB3_194:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.195:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB3_196
# %bb.340:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_548
# %bb.341:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_548
.LBB3_196:
	xor	esi, esi
.LBB3_829:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_831
.LBB3_830:                              # =>This Inner Loop Header: Depth=1
	movzx	r10d, byte ptr [rdx + rsi]
	xor	eax, eax
	sub	al, r10b
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_830
.LBB3_831:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_832:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_832
	jmp	.LBB3_865
.LBB3_197:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.198:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB3_199
# %bb.343:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_551
# %bb.344:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_551
.LBB3_199:
	xor	esi, esi
.LBB3_554:
	mov	rdi, rsi
	not	rdi
	add	rdi, r9
	mov	rax, r9
	and	rax, 3
	je	.LBB3_556
.LBB3_555:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rdx + rsi], 0
	setne	byte ptr [rcx + rsi]
	add	rsi, 1
	add	rax, -1
	jne	.LBB3_555
.LBB3_556:
	cmp	rdi, 3
	jb	.LBB3_865
.LBB3_557:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_557
	jmp	.LBB3_865
.LBB3_200:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.201:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB3_202
# %bb.346:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_558
# %bb.347:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_558
.LBB3_202:
	xor	esi, esi
.LBB3_837:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_839
.LBB3_838:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_838
.LBB3_839:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_840:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_840
	jmp	.LBB3_865
.LBB3_203:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.204:
	mov	r9d, r8d
	cmp	r8d, 128
	jb	.LBB3_205
# %bb.349:
	lea	rax, [rdx + r9]
	cmp	rax, rcx
	jbe	.LBB3_561
# %bb.350:
	lea	rax, [rcx + r9]
	cmp	rax, rdx
	jbe	.LBB3_561
.LBB3_205:
	xor	esi, esi
.LBB3_845:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_847
.LBB3_846:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	mov	byte ptr [rcx + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_846
.LBB3_847:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_848:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_848
	jmp	.LBB3_865
.LBB3_206:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.207:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_208
# %bb.352:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_564
# %bb.353:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_564
.LBB3_208:
	xor	esi, esi
.LBB3_853:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_855
.LBB3_854:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_854
.LBB3_855:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_856:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_856
	jmp	.LBB3_865
.LBB3_209:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.210:
	mov	r9d, r8d
	cmp	r8d, 32
	jb	.LBB3_211
# %bb.355:
	lea	rax, [rdx + 4*r9]
	cmp	rax, rcx
	jbe	.LBB3_567
# %bb.356:
	lea	rax, [rcx + 4*r9]
	cmp	rax, rdx
	jbe	.LBB3_567
.LBB3_211:
	xor	esi, esi
.LBB3_861:
	mov	r8, rsi
	not	r8
	add	r8, r9
	mov	rdi, r9
	and	rdi, 3
	je	.LBB3_863
.LBB3_862:                              # =>This Inner Loop Header: Depth=1
	xor	eax, eax
	sub	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [rcx + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB3_862
.LBB3_863:
	cmp	r8, 3
	jb	.LBB3_865
.LBB3_864:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_864
	jmp	.LBB3_865
.LBB3_212:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.213:
	mov	r11d, r8d
	cmp	r8d, 32
	jb	.LBB3_214
# %bb.358:
	lea	rsi, [rdx + 4*r11]
	cmp	rsi, rcx
	jbe	.LBB3_570
# %bb.359:
	lea	rsi, [rcx + 4*r11]
	cmp	rsi, rdx
	jbe	.LBB3_570
.LBB3_214:
	xor	esi, esi
.LBB3_573:
	mov	r10, rsi
	not	r10
	test	r11b, 1
	je	.LBB3_575
# %bb.574:
	mov	r8d, dword ptr [rdx + 4*rsi]
	xor	r9d, r9d
	test	r8d, r8d
	setne	r9b
	neg	r9d
	test	r8d, r8d
	mov	edi, 1
	cmovle	edi, r9d
	mov	dword ptr [rcx + 4*rsi], edi
	or	rsi, 1
.LBB3_575:
	add	r10, r11
	je	.LBB3_865
# %bb.576:
	mov	r8d, 1
.LBB3_577:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	xor	edi, edi
	test	eax, eax
	setne	dil
	neg	edi
	test	eax, eax
	cmovg	edi, r8d
	mov	dword ptr [rcx + 4*rsi], edi
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	xor	edi, edi
	test	eax, eax
	setne	dil
	neg	edi
	test	eax, eax
	cmovg	edi, r8d
	mov	dword ptr [rcx + 4*rsi + 4], edi
	add	rsi, 2
	cmp	r11, rsi
	jne	.LBB3_577
	jmp	.LBB3_865
.LBB3_215:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.216:
	mov	r10d, r8d
	cmp	r8d, 32
	jb	.LBB3_217
# %bb.361:
	lea	rsi, [rdx + 4*r10]
	cmp	rsi, rcx
	jbe	.LBB3_578
# %bb.362:
	lea	rsi, [rcx + 4*r10]
	cmp	rsi, rdx
	jbe	.LBB3_578
.LBB3_217:
	xor	esi, esi
.LBB3_581:
	mov	r9, rsi
	not	r9
	test	r10b, 1
	je	.LBB3_583
# %bb.582:
	mov	r8d, dword ptr [rdx + 4*rsi]
	mov	edi, r8d
	neg	edi
	cmovl	edi, r8d
	mov	dword ptr [rcx + 4*rsi], edi
	or	rsi, 1
.LBB3_583:
	add	r9, r10
	je	.LBB3_865
.LBB3_584:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r10, rsi
	jne	.LBB3_584
	jmp	.LBB3_865
.LBB3_218:
	test	r8d, r8d
	jle	.LBB3_865
# %bb.219:
	mov	r10d, r8d
	cmp	r8d, 32
	jb	.LBB3_220
# %bb.364:
	lea	rsi, [rdx + 4*r10]
	cmp	rsi, rcx
	jbe	.LBB3_585
# %bb.365:
	lea	rsi, [rcx + 4*r10]
	cmp	rsi, rdx
	jbe	.LBB3_585
.LBB3_220:
	xor	esi, esi
.LBB3_588:
	mov	r9, rsi
	not	r9
	test	r10b, 1
	je	.LBB3_590
# %bb.589:
	mov	r8d, dword ptr [rdx + 4*rsi]
	mov	edi, r8d
	neg	edi
	cmovl	edi, r8d
	mov	dword ptr [rcx + 4*rsi], edi
	or	rsi, 1
.LBB3_590:
	add	r9, r10
	je	.LBB3_865
.LBB3_591:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r10, rsi
	jne	.LBB3_591
	jmp	.LBB3_865
.LBB3_221:
	mov	edx, r9d
	and	edx, -32
	lea	rax, [rdx - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 96
	jae	.LBB3_367
# %bb.222:
	xor	eax, eax
	jmp	.LBB3_369
.LBB3_265:
	mov	edx, r9d
	and	edx, -16
	lea	rax, [rdx - 16]
	mov	rdi, rax
	shr	rdi, 4
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 48
	jae	.LBB3_438
# %bb.266:
	xor	eax, eax
	jmp	.LBB3_440
.LBB3_279:
	mov	edx, r9d
	and	edx, -64
	lea	rax, [rdx - 64]
	mov	rdi, rax
	shr	rdi, 6
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 192
	jae	.LBB3_461
# %bb.280:
	xor	eax, eax
	jmp	.LBB3_463
.LBB3_338:
	mov	edx, r9d
	and	edx, -128
	lea	rax, [rdx - 128]
	mov	rdi, rax
	shr	rdi, 7
	add	rdi, 1
	mov	esi, edi
	and	esi, 3
	cmp	rax, 384
	jae	.LBB3_541
# %bb.339:
	xor	eax, eax
	jmp	.LBB3_543
.LBB3_374:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_612
# %bb.375:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_376:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_376
	jmp	.LBB3_613
.LBB3_377:
	mov	esi, r9d
	and	esi, -32
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI3_3] # ymm1 = [1,1,1,1,1,1,1,1]
.LBB3_378:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm4
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm5
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_378
# %bb.379:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_380
.LBB3_384:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_620
# %bb.385:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_386:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_386
	jmp	.LBB3_621
.LBB3_387:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_628
# %bb.388:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_389:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_389
	jmp	.LBB3_629
.LBB3_390:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_636
# %bb.391:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_392:                              # =>This Inner Loop Header: Depth=1
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_392
	jmp	.LBB3_637
.LBB3_393:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_646
# %bb.394:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_395:                              # =>This Inner Loop Header: Depth=1
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_395
	jmp	.LBB3_647
.LBB3_396:
	mov	esi, eax
	and	esi, -16
	xor	edi, edi
	vxorpd	xmm0, xmm0, xmm0
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI3_0] # ymm1 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI3_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB3_397:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm6, ymmword ptr [rdx + 8*rdi + 96]
	vandpd	ymm7, ymm3, ymm1
	vorpd	ymm7, ymm2, ymm7
	vandpd	ymm8, ymm4, ymm1
	vorpd	ymm8, ymm8, ymm2
	vandpd	ymm9, ymm5, ymm1
	vorpd	ymm9, ymm9, ymm2
	vandpd	ymm10, ymm6, ymm1
	vorpd	ymm10, ymm10, ymm2
	vcmpneqpd	ymm3, ymm3, ymm0
	vandpd	ymm3, ymm3, ymm7
	vcmpneqpd	ymm4, ymm4, ymm0
	vandpd	ymm4, ymm8, ymm4
	vcmpneqpd	ymm5, ymm5, ymm0
	vandpd	ymm5, ymm9, ymm5
	vcmpneqpd	ymm6, ymm6, ymm0
	vandpd	ymm6, ymm10, ymm6
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm5
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm6
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB3_397
# %bb.398:
	cmp	rsi, rax
	je	.LBB3_865
	jmp	.LBB3_399
.LBB3_404:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_656
# %bb.405:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_8] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB3_406:                              # =>This Inner Loop Header: Depth=1
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_406
	jmp	.LBB3_657
.LBB3_407:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_664
# %bb.408:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_8] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
.LBB3_409:                              # =>This Inner Loop Header: Depth=1
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovupd	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_409
	jmp	.LBB3_665
.LBB3_410:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB3_672
# %bb.411:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_412:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [rcx + rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 224], ymm4
	add	rdi, 256
	add	rax, 2
	jne	.LBB3_412
	jmp	.LBB3_673
.LBB3_413:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB3_680
# %bb.414:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_415:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [rcx + rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 224], ymm4
	add	rdi, 256
	add	rax, 2
	jne	.LBB3_415
	jmp	.LBB3_681
.LBB3_416:
	mov	esi, r11d
	and	esi, -128
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI3_6] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB3_417:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + rdi]
	vmovdqu	ymm4, ymmword ptr [rdx + rdi + 32]
	vmovdqu	ymm5, ymmword ptr [rdx + rdi + 64]
	vmovdqu	ymm6, ymmword ptr [rdx + rdi + 96]
	vpcmpeqb	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqb	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqb	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqb	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtb	ymm3, ymm2, ymm3
	vpcmpgtb	ymm4, ymm2, ymm4
	vpcmpgtb	ymm5, ymm2, ymm5
	vpcmpgtb	ymm6, ymm2, ymm6
	vpblendvb	ymm3, ymm2, ymm7, ymm3
	vpblendvb	ymm4, ymm2, ymm8, ymm4
	vpblendvb	ymm5, ymm2, ymm9, ymm5
	vpblendvb	ymm6, ymm2, ymm10, ymm6
	vmovdqu	ymmword ptr [rcx + rdi], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm4
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm5
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm6
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB3_417
# %bb.418:
	cmp	rsi, r11
	je	.LBB3_865
	jmp	.LBB3_419
.LBB3_424:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI3_11] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB3_425:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rdi]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rdi + 24]
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
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_425
# %bb.426:
	cmp	rsi, r10
	je	.LBB3_865
	jmp	.LBB3_427
.LBB3_431:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
	vmovdqa	xmm0, xmmword ptr [rip + .LCPI3_11] # xmm0 = <0,4,8,12,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB3_432:                              # =>This Inner Loop Header: Depth=1
	vpmovsxbd	ymm1, qword ptr [rdx + rdi]
	vpmovsxbd	ymm2, qword ptr [rdx + rdi + 8]
	vpmovsxbd	ymm3, qword ptr [rdx + rdi + 16]
	vpmovsxbd	ymm4, qword ptr [rdx + rdi + 24]
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
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_432
# %bb.433:
	cmp	rsi, r10
	je	.LBB3_865
	jmp	.LBB3_434
.LBB3_445:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_688
# %bb.446:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_447:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_447
	jmp	.LBB3_689
.LBB3_448:
	mov	esi, r9d
	and	esi, -16
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI3_4] # ymm1 = [1,1,1,1]
.LBB3_449:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm4
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm5
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB3_449
# %bb.450:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_451
.LBB3_455:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_696
# %bb.456:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_457:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_457
	jmp	.LBB3_697
.LBB3_458:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_704
# %bb.459:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_460:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_460
	jmp	.LBB3_705
.LBB3_468:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_712
# %bb.469:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_470:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_470
	jmp	.LBB3_713
.LBB3_471:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_720
# %bb.472:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_473:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_473
	jmp	.LBB3_721
.LBB3_474:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_728
# %bb.475:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_476:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm2
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_476
	jmp	.LBB3_729
.LBB3_477:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_736
# %bb.478:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI3_5] # ymm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB3_479:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vpandn	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm3
	vpcmpeqw	ymm2, ymm0, ymmword ptr [rdx + 2*rdi + 64]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rdx + 2*rdi + 96]
	vpandn	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm3
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_479
	jmp	.LBB3_737
.LBB3_480:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_744
# %bb.481:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI3_5] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB3_482:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm4, ymmword ptr [rdx + 2*rdi + 32]
	vpcmpeqw	ymm5, ymm3, ymm0
	vpxor	ymm5, ymm5, ymm1
	vpcmpeqw	ymm6, ymm4, ymm0
	vpxor	ymm6, ymm6, ymm1
	vpcmpgtw	ymm3, ymm2, ymm3
	vpcmpgtw	ymm4, ymm2, ymm4
	vpblendvb	ymm3, ymm2, ymm5, ymm3
	vpblendvb	ymm4, ymm2, ymm6, ymm4
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm3
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm4
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rdi + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 2*rdi + 96]
	vpcmpeqw	ymm5, ymm3, ymm0
	vpxor	ymm5, ymm5, ymm1
	vpcmpeqw	ymm6, ymm4, ymm0
	vpxor	ymm6, ymm6, ymm1
	vpcmpgtw	ymm3, ymm2, ymm3
	vpcmpgtw	ymm4, ymm2, ymm4
	vpblendvb	ymm3, ymm2, ymm5, ymm3
	vpblendvb	ymm4, ymm2, ymm6, ymm4
	vmovdqu	ymmword ptr [rcx + 2*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 2*rdi + 96], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_482
	jmp	.LBB3_745
.LBB3_483:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB3_592
# %bb.484:
	xor	eax, eax
	jmp	.LBB3_594
.LBB3_485:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_753
# %bb.486:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI3_10] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB3_487:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 16]
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
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 48]
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
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_487
	jmp	.LBB3_754
.LBB3_488:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	rdi, rax
	shr	rdi, 5
	add	rdi, 1
	mov	r8d, edi
	and	r8d, 3
	cmp	rax, 96
	jae	.LBB3_602
# %bb.489:
	xor	eax, eax
	jmp	.LBB3_604
.LBB3_490:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_761
# %bb.491:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI3_10] # ymm0 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
.LBB3_492:                              # =>This Inner Loop Header: Depth=1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 16]
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
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm1
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 32]
	vpmovsxwd	ymm2, xmmword ptr [rdx + 2*rdi + 48]
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
	vmovdqu	xmmword ptr [rcx + 2*rdi + 48], xmm2
	vmovdqu	xmmword ptr [rcx + 2*rdi + 32], xmm1
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_492
	jmp	.LBB3_762
.LBB3_493:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_769
# %bb.494:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_495:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_495
	jmp	.LBB3_770
.LBB3_496:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_777
# %bb.497:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_7] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_498:                              # =>This Inner Loop Header: Depth=1
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_498
	jmp	.LBB3_778
.LBB3_499:
	mov	esi, r9d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r8, rax
	shr	r8, 4
	add	r8, 1
	test	rax, rax
	je	.LBB3_787
# %bb.500:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_501:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rdx + 8*rdi + 224]
	vmovdqu	ymmword ptr [rcx + 8*rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 224], ymm4
	add	rdi, 32
	add	rax, 2
	jne	.LBB3_501
	jmp	.LBB3_788
.LBB3_502:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_795
# %bb.503:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_7] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB3_504:                              # =>This Inner Loop Header: Depth=1
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vxorpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_504
	jmp	.LBB3_796
.LBB3_505:
	mov	esi, r11d
	and	esi, -16
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI3_4] # ymm2 = [1,1,1,1]
.LBB3_506:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm5, ymmword ptr [rdx + 8*rdi + 64]
	vmovdqu	ymm6, ymmword ptr [rdx + 8*rdi + 96]
	vpcmpeqq	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqq	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqq	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqq	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtq	ymm3, ymm2, ymm3
	vpcmpgtq	ymm4, ymm2, ymm4
	vpcmpgtq	ymm5, ymm2, ymm5
	vpcmpgtq	ymm6, ymm2, ymm6
	vblendvpd	ymm3, ymm2, ymm7, ymm3
	vblendvpd	ymm4, ymm2, ymm8, ymm4
	vblendvpd	ymm5, ymm2, ymm9, ymm5
	vblendvpd	ymm6, ymm2, ymm10, ymm6
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm5
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm6
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB3_506
# %bb.507:
	cmp	rsi, r11
	je	.LBB3_865
	jmp	.LBB3_508
.LBB3_513:
	mov	esi, eax
	and	esi, -32
	xor	edi, edi
	vxorps	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI3_3] # ymm1 = [1,1,1,1,1,1,1,1]
.LBB3_514:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm5, ymmword ptr [rdx + 4*rdi + 96]
	vpsrad	ymm6, ymm2, 31
	vpor	ymm6, ymm6, ymm1
	vpsrad	ymm7, ymm3, 31
	vpor	ymm7, ymm7, ymm1
	vpsrad	ymm8, ymm4, 31
	vpor	ymm8, ymm8, ymm1
	vpsrad	ymm9, ymm5, 31
	vpor	ymm9, ymm9, ymm1
	vcvtdq2ps	ymm6, ymm6
	vcvtdq2ps	ymm7, ymm7
	vcvtdq2ps	ymm8, ymm8
	vcvtdq2ps	ymm9, ymm9
	vcmpneqps	ymm2, ymm2, ymm0
	vandps	ymm2, ymm2, ymm6
	vcmpneqps	ymm3, ymm3, ymm0
	vandps	ymm3, ymm3, ymm7
	vcmpneqps	ymm4, ymm4, ymm0
	vandps	ymm4, ymm8, ymm4
	vcmpneqps	ymm5, ymm5, ymm0
	vandps	ymm5, ymm9, ymm5
	vmovups	ymmword ptr [rcx + 4*rdi], ymm2
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm5
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_514
# %bb.515:
	cmp	rsi, rax
	je	.LBB3_865
	jmp	.LBB3_516
.LBB3_521:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_522:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB3_522
# %bb.523:
	cmp	rsi, r10
	je	.LBB3_865
	jmp	.LBB3_524
.LBB3_528:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_805
# %bb.529:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_9] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB3_530:                              # =>This Inner Loop Header: Depth=1
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_530
	jmp	.LBB3_806
.LBB3_531:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_532:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rdi]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rdi + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm4, ymm0, ymm1
	vblendvpd	ymm1, ymm1, ymm4, ymm1
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rdi + 96]
	vpsubq	ymm5, ymm0, ymm2
	vblendvpd	ymm2, ymm2, ymm5, ymm2
	vpsubq	ymm5, ymm0, ymm3
	vblendvpd	ymm3, ymm3, ymm5, ymm3
	vpsubq	ymm5, ymm0, ymm4
	vblendvpd	ymm4, ymm4, ymm5, ymm4
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm4
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB3_532
# %bb.533:
	cmp	rsi, r10
	je	.LBB3_865
	jmp	.LBB3_534
.LBB3_538:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_815
# %bb.539:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_9] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
.LBB3_540:                              # =>This Inner Loop Header: Depth=1
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vandpd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovupd	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_540
	jmp	.LBB3_816
.LBB3_548:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB3_825
# %bb.549:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_550:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rdx + rdi + 224]
	vmovdqu	ymmword ptr [rcx + rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 224], ymm4
	add	rdi, 256
	add	rax, 2
	jne	.LBB3_550
	jmp	.LBB3_826
.LBB3_551:
	mov	esi, r9d
	and	esi, -128
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI3_6] # ymm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB3_552:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqb	ymm2, ymm0, ymmword ptr [rdx + rdi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqb	ymm3, ymm0, ymmword ptr [rdx + rdi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqb	ymm4, ymm0, ymmword ptr [rdx + rdi + 64]
	vpcmpeqb	ymm5, ymm0, ymmword ptr [rdx + rdi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [rcx + rdi], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm4
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm5
	sub	rdi, -128
	cmp	rsi, rdi
	jne	.LBB3_552
# %bb.553:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_554
.LBB3_558:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB3_833
# %bb.559:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_560:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_560
	jmp	.LBB3_834
.LBB3_561:
	mov	esi, r9d
	and	esi, -128
	lea	rax, [rsi - 128]
	mov	r8, rax
	shr	r8, 7
	add	r8, 1
	test	rax, rax
	je	.LBB3_841
# %bb.562:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB3_563:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_563
	jmp	.LBB3_842
.LBB3_564:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_849
# %bb.565:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_566:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_566
	jmp	.LBB3_850
.LBB3_567:
	mov	esi, r9d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r8, rax
	shr	r8, 5
	add	r8, 1
	test	rax, rax
	je	.LBB3_857
# %bb.568:
	mov	rax, r8
	and	rax, -2
	neg	rax
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
.LBB3_569:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rdx + 4*rdi + 224]
	vmovdqu	ymmword ptr [rcx + 4*rdi + 128], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 160], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 192], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 224], ymm4
	add	rdi, 64
	add	rax, 2
	jne	.LBB3_569
	jmp	.LBB3_858
.LBB3_570:
	mov	esi, r11d
	and	esi, -32
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI3_3] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB3_571:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rdi]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rdi + 32]
	vmovdqu	ymm5, ymmword ptr [rdx + 4*rdi + 64]
	vmovdqu	ymm6, ymmword ptr [rdx + 4*rdi + 96]
	vpcmpeqd	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqd	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqd	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqd	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtd	ymm3, ymm2, ymm3
	vpcmpgtd	ymm4, ymm2, ymm4
	vpcmpgtd	ymm5, ymm2, ymm5
	vpcmpgtd	ymm6, ymm2, ymm6
	vblendvps	ymm3, ymm2, ymm7, ymm3
	vblendvps	ymm4, ymm2, ymm8, ymm4
	vblendvps	ymm5, ymm2, ymm9, ymm5
	vblendvps	ymm6, ymm2, ymm10, ymm6
	vmovups	ymmword ptr [rcx + 4*rdi], ymm3
	vmovups	ymmword ptr [rcx + 4*rdi + 32], ymm4
	vmovups	ymmword ptr [rcx + 4*rdi + 64], ymm5
	vmovups	ymmword ptr [rcx + 4*rdi + 96], ymm6
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_571
# %bb.572:
	cmp	rsi, r11
	je	.LBB3_865
	jmp	.LBB3_573
.LBB3_578:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB3_579:                              # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rdi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_579
# %bb.580:
	cmp	rsi, r10
	je	.LBB3_865
	jmp	.LBB3_581
.LBB3_585:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
.LBB3_586:                              # =>This Inner Loop Header: Depth=1
	vpabsd	ymm0, ymmword ptr [rdx + 4*rdi]
	vpabsd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vpabsd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vpabsd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB3_586
# %bb.587:
	cmp	rsi, r10
	je	.LBB3_865
	jmp	.LBB3_588
.LBB3_367:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
	vxorpd	xmm0, xmm0, xmm0
.LBB3_368:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rcx + 4*rax], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 32], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 64], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 96], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 128], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 160], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 224], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 256], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 288], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 320], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 352], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 384], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 416], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 448], ymm0
	vmovupd	ymmword ptr [rcx + 4*rax + 480], ymm0
	sub	rax, -128
	add	rdi, 4
	jne	.LBB3_368
.LBB3_369:
	test	rsi, rsi
	je	.LBB3_372
# %bb.370:
	lea	rax, [rcx + 4*rax]
	add	rax, 96
	neg	rsi
	vxorpd	xmm0, xmm0, xmm0
.LBB3_371:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rax - 96], ymm0
	vmovupd	ymmword ptr [rax - 64], ymm0
	vmovupd	ymmword ptr [rax - 32], ymm0
	vmovupd	ymmword ptr [rax], ymm0
	sub	rax, -128
	inc	rsi
	jne	.LBB3_371
.LBB3_372:
	cmp	rdx, r9
	je	.LBB3_865
	.p2align	4, 0x90
.LBB3_373:                              # =>This Inner Loop Header: Depth=1
	mov	dword ptr [rcx + 4*rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_373
	jmp	.LBB3_865
.LBB3_438:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
	vxorpd	xmm0, xmm0, xmm0
.LBB3_439:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rcx + 8*rax], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 32], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 64], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 96], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 128], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 160], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 224], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 256], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 288], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 320], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 352], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 384], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 416], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 448], ymm0
	vmovupd	ymmword ptr [rcx + 8*rax + 480], ymm0
	add	rax, 64
	add	rdi, 4
	jne	.LBB3_439
.LBB3_440:
	test	rsi, rsi
	je	.LBB3_443
# %bb.441:
	lea	rax, [rcx + 8*rax]
	add	rax, 96
	neg	rsi
	vxorpd	xmm0, xmm0, xmm0
.LBB3_442:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rax - 96], ymm0
	vmovupd	ymmword ptr [rax - 64], ymm0
	vmovupd	ymmword ptr [rax - 32], ymm0
	vmovupd	ymmword ptr [rax], ymm0
	sub	rax, -128
	inc	rsi
	jne	.LBB3_442
.LBB3_443:
	cmp	rdx, r9
	je	.LBB3_865
	.p2align	4, 0x90
.LBB3_444:                              # =>This Inner Loop Header: Depth=1
	mov	qword ptr [rcx + 8*rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_444
	jmp	.LBB3_865
.LBB3_461:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
	vxorpd	xmm0, xmm0, xmm0
.LBB3_462:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rcx + 2*rax], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 32], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 64], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 96], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 128], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 160], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 224], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 256], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 288], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 320], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 352], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 384], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 416], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 448], ymm0
	vmovupd	ymmword ptr [rcx + 2*rax + 480], ymm0
	add	rax, 256
	add	rdi, 4
	jne	.LBB3_462
.LBB3_463:
	test	rsi, rsi
	je	.LBB3_466
# %bb.464:
	lea	rax, [rcx + 2*rax]
	add	rax, 96
	neg	rsi
	vxorpd	xmm0, xmm0, xmm0
.LBB3_465:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rax - 96], ymm0
	vmovupd	ymmword ptr [rax - 64], ymm0
	vmovupd	ymmword ptr [rax - 32], ymm0
	vmovupd	ymmword ptr [rax], ymm0
	sub	rax, -128
	inc	rsi
	jne	.LBB3_465
.LBB3_466:
	cmp	rdx, r9
	je	.LBB3_865
	.p2align	4, 0x90
.LBB3_467:                              # =>This Inner Loop Header: Depth=1
	mov	word ptr [rcx + 2*rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_467
	jmp	.LBB3_865
.LBB3_541:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
	vxorpd	xmm0, xmm0, xmm0
.LBB3_542:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rcx + rax], ymm0
	vmovupd	ymmword ptr [rcx + rax + 32], ymm0
	vmovupd	ymmword ptr [rcx + rax + 64], ymm0
	vmovupd	ymmword ptr [rcx + rax + 96], ymm0
	vmovupd	ymmword ptr [rcx + rax + 128], ymm0
	vmovupd	ymmword ptr [rcx + rax + 160], ymm0
	vmovupd	ymmword ptr [rcx + rax + 192], ymm0
	vmovupd	ymmword ptr [rcx + rax + 224], ymm0
	vmovupd	ymmword ptr [rcx + rax + 256], ymm0
	vmovupd	ymmword ptr [rcx + rax + 288], ymm0
	vmovupd	ymmword ptr [rcx + rax + 320], ymm0
	vmovupd	ymmword ptr [rcx + rax + 352], ymm0
	vmovupd	ymmword ptr [rcx + rax + 384], ymm0
	vmovupd	ymmword ptr [rcx + rax + 416], ymm0
	vmovupd	ymmword ptr [rcx + rax + 448], ymm0
	vmovupd	ymmword ptr [rcx + rax + 480], ymm0
	add	rax, 512
	add	rdi, 4
	jne	.LBB3_542
.LBB3_543:
	test	rsi, rsi
	je	.LBB3_546
# %bb.544:
	add	rax, rcx
	add	rax, 96
	neg	rsi
	vxorpd	xmm0, xmm0, xmm0
.LBB3_545:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymmword ptr [rax - 96], ymm0
	vmovupd	ymmword ptr [rax - 64], ymm0
	vmovupd	ymmword ptr [rax - 32], ymm0
	vmovupd	ymmword ptr [rax], ymm0
	sub	rax, -128
	inc	rsi
	jne	.LBB3_545
.LBB3_546:
	cmp	rdx, r9
	je	.LBB3_865
	.p2align	4, 0x90
.LBB3_547:                              # =>This Inner Loop Header: Depth=1
	mov	byte ptr [rcx + rdx], 0
	add	rdx, 1
	cmp	r9, rdx
	jne	.LBB3_547
.LBB3_865:
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB3_592:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_593
.LBB3_594:
	test	r8, r8
	je	.LBB3_597
# %bb.595:
	add	rax, rax
	add	rax, 32
	neg	r8
.LBB3_596:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + rax - 32]
	vmovupd	ymm1, ymmword ptr [rdx + rax]
	vmovupd	ymmword ptr [rcx + rax - 32], ymm0
	vmovupd	ymmword ptr [rcx + rax], ymm1
	add	rax, 64
	inc	r8
	jne	.LBB3_596
.LBB3_597:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_598
.LBB3_602:
	and	rdi, -4
	neg	rdi
	xor	eax, eax
.LBB3_603:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB3_603
.LBB3_604:
	test	r8, r8
	je	.LBB3_607
# %bb.605:
	add	rax, rax
	add	rax, 32
	neg	r8
.LBB3_606:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + rax - 32]
	vmovupd	ymm1, ymmword ptr [rdx + rax]
	vmovupd	ymmword ptr [rcx + rax - 32], ymm0
	vmovupd	ymmword ptr [rcx + rax], ymm1
	add	rax, 64
	inc	r8
	jne	.LBB3_606
.LBB3_607:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_608
.LBB3_612:
	xor	edi, edi
.LBB3_613:
	test	r8b, 1
	je	.LBB3_615
# %bb.614:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_615:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_616
.LBB3_620:
	xor	edi, edi
.LBB3_621:
	test	r8b, 1
	je	.LBB3_623
# %bb.622:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB3_623:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_624
.LBB3_628:
	xor	edi, edi
.LBB3_629:
	test	r8b, 1
	je	.LBB3_631
# %bb.630:
	vmovupd	ymm0, ymmword ptr [rdx + 4*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 4*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 4*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm3
.LBB3_631:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_632
.LBB3_636:
	xor	edi, edi
.LBB3_637:
	test	r8b, 1
	je	.LBB3_639
# %bb.638:
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vxorpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_639:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_640
.LBB3_646:
	xor	edi, edi
.LBB3_647:
	test	r8b, 1
	je	.LBB3_649
# %bb.648:
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vxorpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_649:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_650
.LBB3_656:
	xor	edi, edi
.LBB3_657:
	test	r8b, 1
	je	.LBB3_659
# %bb.658:
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_8] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vandpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_659:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_660
.LBB3_664:
	xor	edi, edi
.LBB3_665:
	test	r8b, 1
	je	.LBB3_667
# %bb.666:
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI3_8] # ymm0 = [9223372036854775807,9223372036854775807,9223372036854775807,9223372036854775807]
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vandpd	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_667:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_668
.LBB3_672:
	xor	edi, edi
.LBB3_673:
	test	r8b, 1
	je	.LBB3_675
# %bb.674:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm0
.LBB3_675:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_676
.LBB3_680:
	xor	edi, edi
.LBB3_681:
	test	r8b, 1
	je	.LBB3_683
# %bb.682:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm0
.LBB3_683:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_684
.LBB3_688:
	xor	edi, edi
.LBB3_689:
	test	r8b, 1
	je	.LBB3_691
# %bb.690:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_691:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_692
.LBB3_696:
	xor	edi, edi
.LBB3_697:
	test	r8b, 1
	je	.LBB3_699
# %bb.698:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB3_699:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_700
.LBB3_704:
	xor	edi, edi
.LBB3_705:
	test	r8b, 1
	je	.LBB3_707
# %bb.706:
	vmovupd	ymm0, ymmword ptr [rdx + 8*rdi]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rdi + 96]
	vmovupd	ymmword ptr [rcx + 8*rdi], ymm0
	vmovupd	ymmword ptr [rcx + 8*rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + 8*rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + 8*rdi + 96], ymm3
.LBB3_707:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_708
.LBB3_712:
	xor	edi, edi
.LBB3_713:
	test	r8b, 1
	je	.LBB3_715
# %bb.714:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm0
.LBB3_715:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_716
.LBB3_720:
	xor	edi, edi
.LBB3_721:
	test	r8b, 1
	je	.LBB3_723
# %bb.722:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm0
.LBB3_723:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_724
.LBB3_728:
	xor	edi, edi
.LBB3_729:
	test	r8b, 1
	je	.LBB3_731
# %bb.730:
	vpxor	xmm0, xmm0, xmm0
	vpsubw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vpsubw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm0
.LBB3_731:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_732
.LBB3_736:
	xor	edi, edi
.LBB3_737:
	test	r8b, 1
	je	.LBB3_739
# %bb.738:
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqw	ymm1, ymm0, ymmword ptr [rdx + 2*rdi]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI3_5] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpcmpeqw	ymm0, ymm0, ymmword ptr [rdx + 2*rdi + 32]
	vpandn	ymm1, ymm1, ymm2
	vpandn	ymm0, ymm0, ymm2
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm0
.LBB3_739:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_740
.LBB3_744:
	xor	edi, edi
.LBB3_745:
	test	r8b, 1
	je	.LBB3_747
# %bb.746:
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rdi]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rdi + 32]
	vpxor	xmm2, xmm2, xmm2
	vpcmpeqw	ymm3, ymm0, ymm2
	vpcmpeqd	ymm4, ymm4, ymm4
	vpxor	ymm3, ymm3, ymm4
	vpcmpeqw	ymm2, ymm1, ymm2
	vpxor	ymm2, ymm2, ymm4
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI3_5] # ymm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpcmpgtw	ymm0, ymm4, ymm0
	vpcmpgtw	ymm1, ymm4, ymm1
	vpblendvb	ymm0, ymm4, ymm3, ymm0
	vpblendvb	ymm1, ymm4, ymm2, ymm1
	vmovdqu	ymmword ptr [rcx + 2*rdi], ymm0
	vmovdqu	ymmword ptr [rcx + 2*rdi + 32], ymm1
.LBB3_747:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_748
.LBB3_753:
	xor	edi, edi
.LBB3_754:
	test	r8b, 1
	je	.LBB3_756
# %bb.755:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI3_10] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB3_756:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_757
.LBB3_761:
	xor	edi, edi
.LBB3_762:
	test	r8b, 1
	je	.LBB3_764
# %bb.763:
	vpmovsxwd	ymm0, xmmword ptr [rdx + 2*rdi]
	vpmovsxwd	ymm1, xmmword ptr [rdx + 2*rdi + 16]
	vpsrad	ymm2, ymm1, 15
	vpsrad	ymm3, ymm0, 15
	vpaddd	ymm0, ymm3, ymm0
	vpaddd	ymm1, ymm2, ymm1
	vpxor	ymm1, ymm1, ymm2
	vpxor	ymm0, ymm0, ymm3
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI3_10] # ymm2 = [0,1,4,5,8,9,12,13,8,9,12,13,12,13,14,15,16,17,20,21,24,25,28,29,24,25,28,29,28,29,30,31]
	vpshufb	ymm0, ymm0, ymm2
	vpermq	ymm0, ymm0, 232                 # ymm0 = ymm0[0,2,2,3]
	vpshufb	ymm1, ymm1, ymm2
	vpermq	ymm1, ymm1, 232                 # ymm1 = ymm1[0,2,2,3]
	vmovdqu	xmmword ptr [rcx + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [rcx + 2*rdi], xmm0
.LBB3_764:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_765
.LBB3_769:
	xor	edi, edi
.LBB3_770:
	test	r8b, 1
	je	.LBB3_772
# %bb.771:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_772:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_773
.LBB3_777:
	xor	edi, edi
.LBB3_778:
	test	r8b, 1
	je	.LBB3_780
# %bb.779:
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_7] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vxorpd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_780:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_781
.LBB3_787:
	xor	edi, edi
.LBB3_788:
	test	r8b, 1
	je	.LBB3_790
# %bb.789:
	vpxor	xmm0, xmm0, xmm0
	vpsubq	ymm1, ymm0, ymmword ptr [rdx + 8*rdi]
	vpsubq	ymm2, ymm0, ymmword ptr [rdx + 8*rdi + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rdx + 8*rdi + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rdx + 8*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 8*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 8*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 8*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 8*rdi + 96], ymm0
.LBB3_790:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_791
.LBB3_795:
	xor	edi, edi
.LBB3_796:
	test	r8b, 1
	je	.LBB3_798
# %bb.797:
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_7] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vxorpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vxorpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vxorpd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_798:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_799
.LBB3_805:
	xor	edi, edi
.LBB3_806:
	test	r8b, 1
	je	.LBB3_808
# %bb.807:
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_9] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vandpd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_808:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_809
.LBB3_815:
	xor	edi, edi
.LBB3_816:
	test	r8b, 1
	je	.LBB3_818
# %bb.817:
	vbroadcastss	ymm0, dword ptr [rip + .LCPI3_9] # ymm0 = [2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647,2147483647]
	vandpd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vandpd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vandpd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vandpd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovupd	ymmword ptr [rcx + 4*rdi], ymm1
	vmovupd	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovupd	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovupd	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_818:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_819
.LBB3_825:
	xor	edi, edi
.LBB3_826:
	test	r8b, 1
	je	.LBB3_828
# %bb.827:
	vpxor	xmm0, xmm0, xmm0
	vpsubb	ymm1, ymm0, ymmword ptr [rdx + rdi]
	vpsubb	ymm2, ymm0, ymmword ptr [rdx + rdi + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rdx + rdi + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rdx + rdi + 96]
	vmovdqu	ymmword ptr [rcx + rdi], ymm1
	vmovdqu	ymmword ptr [rcx + rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + rdi + 96], ymm0
.LBB3_828:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_829
.LBB3_833:
	xor	edi, edi
.LBB3_834:
	test	r8b, 1
	je	.LBB3_836
# %bb.835:
	vmovupd	ymm0, ymmword ptr [rdx + rdi]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovupd	ymmword ptr [rcx + rdi], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 96], ymm3
.LBB3_836:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_837
.LBB3_841:
	xor	edi, edi
.LBB3_842:
	test	r8b, 1
	je	.LBB3_844
# %bb.843:
	vmovupd	ymm0, ymmword ptr [rdx + rdi]
	vmovupd	ymm1, ymmword ptr [rdx + rdi + 32]
	vmovupd	ymm2, ymmword ptr [rdx + rdi + 64]
	vmovupd	ymm3, ymmword ptr [rdx + rdi + 96]
	vmovupd	ymmword ptr [rcx + rdi], ymm0
	vmovupd	ymmword ptr [rcx + rdi + 32], ymm1
	vmovupd	ymmword ptr [rcx + rdi + 64], ymm2
	vmovupd	ymmword ptr [rcx + rdi + 96], ymm3
.LBB3_844:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_845
.LBB3_849:
	xor	edi, edi
.LBB3_850:
	test	r8b, 1
	je	.LBB3_852
# %bb.851:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_852:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_853
.LBB3_857:
	xor	edi, edi
.LBB3_858:
	test	r8b, 1
	je	.LBB3_860
# %bb.859:
	vpxor	xmm0, xmm0, xmm0
	vpsubd	ymm1, ymm0, ymmword ptr [rdx + 4*rdi]
	vpsubd	ymm2, ymm0, ymmword ptr [rdx + 4*rdi + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rdx + 4*rdi + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rdx + 4*rdi + 96]
	vmovdqu	ymmword ptr [rcx + 4*rdi], ymm1
	vmovdqu	ymmword ptr [rcx + 4*rdi + 32], ymm2
	vmovdqu	ymmword ptr [rcx + 4*rdi + 64], ymm3
	vmovdqu	ymmword ptr [rcx + 4*rdi + 96], ymm0
.LBB3_860:
	cmp	rsi, r9
	je	.LBB3_865
	jmp	.LBB3_861
.Lfunc_end3:
	.size	arithmetic_unary_same_types_avx2, .Lfunc_end3-arithmetic_unary_same_types_avx2
                                        # -- End function
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function arithmetic_unary_diff_type_avx2
.LCPI4_0:
	.quad	0x8000000000000000              # double -0
.LCPI4_1:
	.quad	0x3ff0000000000000              # double 1
.LCPI4_6:
	.quad	0x43e0000000000000              # double 9.2233720368547758E+18
.LCPI4_7:
	.quad	0x41e0000000000000              # double 2147483648
.LCPI4_13:
	.quad	0xbff0000000000000              # double -1
.LCPI4_15:
	.quad	1                               # 0x1
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI4_2:
	.quad	0x8000000000000000              # double -0
	.quad	0x8000000000000000              # double -0
.LCPI4_11:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
.LCPI4_12:
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
.LCPI4_16:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.zero	2
	.zero	2
	.zero	2
	.zero	2
.LCPI4_17:
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
.LCPI4_19:
	.zero	16,1
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI4_3:
	.long	0x7fffffff                      # float NaN
.LCPI4_4:
	.long	0x80000000                      # float -0
.LCPI4_5:
	.long	0x3f800000                      # float 1
.LCPI4_8:
	.long	1                               # 0x1
.LCPI4_9:
	.long	0x5f000000                      # float 9.22337203E+18
.LCPI4_10:
	.long	0x4f000000                      # float 2.14748365E+9
.LCPI4_14:
	.long	0xbf800000                      # float -1
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI4_18:
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
	.short	1                               # 0x1
.LCPI4_20:
	.zero	32,1
	.text
	.globl	arithmetic_unary_diff_type_avx2
	.p2align	4, 0x90
	.type	arithmetic_unary_diff_type_avx2,@function
arithmetic_unary_diff_type_avx2:        # @arithmetic_unary_diff_type_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r14
	push	rbx
	and	rsp, -8
	cmp	dl, 20
	jne	.LBB4_1351
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
	jne	.LBB4_1351
# %bb.6:
	cmp	esi, 6
	jg	.LBB4_94
# %bb.7:
	cmp	esi, 3
	jle	.LBB4_164
# %bb.8:
	cmp	esi, 4
	je	.LBB4_267
# %bb.9:
	cmp	esi, 5
	je	.LBB4_270
# %bb.10:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.11:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.12:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_13
# %bb.447:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_870
# %bb.448:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_870
.LBB4_13:
	xor	edx, edx
.LBB4_873:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_875
.LBB4_874:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_874
.LBB4_875:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_876:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx + 4], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 4], esi
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx + 8], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 8], esi
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx + 12], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 12], esi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_876
	jmp	.LBB4_1351
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
	jne	.LBB4_1351
# %bb.18:
	cmp	esi, 6
	jg	.LBB4_101
# %bb.19:
	cmp	esi, 3
	jle	.LBB4_169
# %bb.20:
	cmp	esi, 4
	je	.LBB4_273
# %bb.21:
	cmp	esi, 5
	je	.LBB4_276
# %bb.22:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.23:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.24:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 4
	jae	.LBB4_450
# %bb.25:
	xor	esi, esi
	jmp	.LBB4_1292
.LBB4_26:
	cmp	edi, 2
	je	.LBB4_78
# %bb.27:
	cmp	edi, 3
	jne	.LBB4_1351
# %bb.28:
	cmp	esi, 6
	jg	.LBB4_108
# %bb.29:
	cmp	esi, 3
	jle	.LBB4_174
# %bb.30:
	cmp	esi, 4
	je	.LBB4_279
# %bb.31:
	cmp	esi, 5
	je	.LBB4_282
# %bb.32:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.33:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.34:
	mov	r11d, r9d
	cmp	r9d, 32
	jb	.LBB4_35
# %bb.453:
	lea	rdx, [rcx + r11]
	cmp	rdx, r8
	jbe	.LBB4_877
# %bb.454:
	lea	rdx, [r8 + 4*r11]
	cmp	rdx, rcx
	jbe	.LBB4_877
.LBB4_35:
	xor	edx, edx
.LBB4_880:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_882
# %bb.881:
	mov	r9b, byte ptr [rcx + rdx]
	xor	r10d, r10d
	test	r9b, r9b
	setne	r10b
	neg	r10d
	test	r9b, r9b
	mov	edi, 1
	cmovle	edi, r10d
	mov	dword ptr [r8 + 4*rdx], edi
	or	rdx, 1
.LBB4_882:
	add	rsi, r11
	je	.LBB4_1351
# %bb.883:
	mov	esi, 1
.LBB4_884:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rdx
	jne	.LBB4_884
	jmp	.LBB4_1351
.LBB4_36:
	cmp	edi, 7
	je	.LBB4_86
# %bb.37:
	cmp	edi, 8
	jne	.LBB4_1351
# %bb.38:
	cmp	esi, 6
	jg	.LBB4_115
# %bb.39:
	cmp	esi, 3
	jle	.LBB4_179
# %bb.40:
	cmp	esi, 4
	je	.LBB4_285
# %bb.41:
	cmp	esi, 5
	je	.LBB4_288
# %bb.42:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.43:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.44:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_456
# %bb.45:
	xor	edx, edx
	jmp	.LBB4_459
.LBB4_46:
	cmp	esi, 6
	jg	.LBB4_122
# %bb.47:
	cmp	esi, 3
	jle	.LBB4_184
# %bb.48:
	cmp	esi, 4
	je	.LBB4_291
# %bb.49:
	cmp	esi, 5
	je	.LBB4_294
# %bb.50:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.51:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.52:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_460
# %bb.53:
	xor	edx, edx
	jmp	.LBB4_463
.LBB4_54:
	cmp	esi, 6
	jg	.LBB4_129
# %bb.55:
	cmp	esi, 3
	jle	.LBB4_189
# %bb.56:
	cmp	esi, 4
	je	.LBB4_297
# %bb.57:
	cmp	esi, 5
	je	.LBB4_300
# %bb.58:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.59:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.60:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB4_464
# %bb.61:
	xor	edx, edx
	jmp	.LBB4_467
.LBB4_62:
	cmp	esi, 6
	jg	.LBB4_136
# %bb.63:
	cmp	esi, 3
	jle	.LBB4_194
# %bb.64:
	cmp	esi, 4
	je	.LBB4_303
# %bb.65:
	cmp	esi, 5
	je	.LBB4_306
# %bb.66:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.67:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.68:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_469
# %bb.69:
	xor	edx, edx
	jmp	.LBB4_472
.LBB4_70:
	cmp	esi, 6
	jg	.LBB4_143
# %bb.71:
	cmp	esi, 3
	jle	.LBB4_199
# %bb.72:
	cmp	esi, 4
	je	.LBB4_309
# %bb.73:
	cmp	esi, 5
	je	.LBB4_312
# %bb.74:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.75:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.76:
	mov	eax, r9d
	cmp	r9d, 8
	jae	.LBB4_474
# %bb.77:
	xor	edx, edx
	jmp	.LBB4_1298
.LBB4_78:
	cmp	esi, 6
	jg	.LBB4_150
# %bb.79:
	cmp	esi, 3
	jle	.LBB4_204
# %bb.80:
	cmp	esi, 4
	je	.LBB4_315
# %bb.81:
	cmp	esi, 5
	je	.LBB4_318
# %bb.82:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.83:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.84:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_85
# %bb.477:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_885
# %bb.478:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_885
.LBB4_85:
	xor	edx, edx
.LBB4_888:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_890
.LBB4_889:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_889
.LBB4_890:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_891:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 4], esi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 8], esi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 12], esi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_891
	jmp	.LBB4_1351
.LBB4_86:
	cmp	esi, 6
	jg	.LBB4_157
# %bb.87:
	cmp	esi, 3
	jle	.LBB4_209
# %bb.88:
	cmp	esi, 4
	je	.LBB4_321
# %bb.89:
	cmp	esi, 5
	je	.LBB4_324
# %bb.90:
	cmp	esi, 6
	jne	.LBB4_1351
# %bb.91:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.92:
	mov	r11d, r9d
	cmp	r9d, 32
	jb	.LBB4_93
# %bb.480:
	lea	rdx, [rcx + 4*r11]
	cmp	rdx, r8
	jbe	.LBB4_892
# %bb.481:
	lea	rdx, [r8 + 4*r11]
	cmp	rdx, rcx
	jbe	.LBB4_892
.LBB4_93:
	xor	edx, edx
.LBB4_895:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_897
# %bb.896:
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
.LBB4_897:
	add	rsi, r11
	je	.LBB4_1351
# %bb.898:
	mov	esi, 1
.LBB4_899:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_899
	jmp	.LBB4_1351
.LBB4_94:
	cmp	esi, 8
	jle	.LBB4_214
# %bb.95:
	cmp	esi, 9
	je	.LBB4_327
# %bb.96:
	cmp	esi, 11
	je	.LBB4_330
# %bb.97:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.98:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.99:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_483
# %bb.100:
	xor	edx, edx
	jmp	.LBB4_486
.LBB4_101:
	cmp	esi, 8
	jle	.LBB4_219
# %bb.102:
	cmp	esi, 9
	je	.LBB4_333
# %bb.103:
	cmp	esi, 11
	je	.LBB4_336
# %bb.104:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.105:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.106:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_107
# %bb.490:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_900
# %bb.491:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_900
.LBB4_107:
	xor	edx, edx
.LBB4_903:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_905
# %bb.904:
	vmovsd	xmm0, qword ptr [rcx + 8*rdx]   # xmm0 = mem[0],zero
	vandpd	xmm1, xmm0, xmmword ptr [rip + .LCPI4_2]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
	vorpd	xmm1, xmm2, xmm1
	vxorpd	xmm2, xmm2, xmm2
	vcmpeqsd	xmm0, xmm0, xmm2
	vandnpd	xmm0, xmm0, xmm1
	vmovlpd	qword ptr [r8 + 8*rdx], xmm0
	or	rdx, 1
.LBB4_905:
	add	rsi, rax
	je	.LBB4_1351
# %bb.906:
	vmovapd	xmm0, xmmword ptr [rip + .LCPI4_2] # xmm0 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = [1.0E+0,1.0E+0]
                                        # xmm1 = mem[0,0]
	vxorpd	xmm2, xmm2, xmm2
.LBB4_907:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	vandpd	xmm4, xmm3, xmm0
	vorpd	xmm4, xmm1, xmm4
	vcmpeqsd	xmm3, xmm3, xmm2
	vandnpd	xmm3, xmm3, xmm4
	vmovlpd	qword ptr [r8 + 8*rdx], xmm3
	vmovsd	xmm3, qword ptr [rcx + 8*rdx + 8] # xmm3 = mem[0],zero
	vandpd	xmm4, xmm3, xmm0
	vorpd	xmm4, xmm1, xmm4
	vcmpeqsd	xmm3, xmm3, xmm2
	vandnpd	xmm3, xmm3, xmm4
	vmovlpd	qword ptr [r8 + 8*rdx + 8], xmm3
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_907
	jmp	.LBB4_1351
.LBB4_108:
	cmp	esi, 8
	jle	.LBB4_224
# %bb.109:
	cmp	esi, 9
	je	.LBB4_339
# %bb.110:
	cmp	esi, 11
	je	.LBB4_342
# %bb.111:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.112:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.113:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_114
# %bb.493:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_908
# %bb.494:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_908
.LBB4_114:
	xor	edx, edx
.LBB4_911:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1254
# %bb.912:
	cmp	byte ptr [rcx + rdx], 0
	jne	.LBB4_1250
# %bb.913:
	vpxor	xmm0, xmm0, xmm0
	jmp	.LBB4_1251
.LBB4_115:
	cmp	esi, 8
	jle	.LBB4_229
# %bb.116:
	cmp	esi, 9
	je	.LBB4_345
# %bb.117:
	cmp	esi, 11
	je	.LBB4_348
# %bb.118:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.119:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.120:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_496
# %bb.121:
	xor	edx, edx
	jmp	.LBB4_499
.LBB4_122:
	cmp	esi, 8
	jle	.LBB4_234
# %bb.123:
	cmp	esi, 9
	je	.LBB4_351
# %bb.124:
	cmp	esi, 11
	je	.LBB4_354
# %bb.125:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.126:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.127:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_503
# %bb.128:
	xor	edx, edx
	jmp	.LBB4_506
.LBB4_129:
	cmp	esi, 8
	jle	.LBB4_239
# %bb.130:
	cmp	esi, 9
	je	.LBB4_357
# %bb.131:
	cmp	esi, 11
	je	.LBB4_360
# %bb.132:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.133:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.134:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_510
# %bb.135:
	xor	edx, edx
	jmp	.LBB4_513
.LBB4_136:
	cmp	esi, 8
	jle	.LBB4_244
# %bb.137:
	cmp	esi, 9
	je	.LBB4_363
# %bb.138:
	cmp	esi, 11
	je	.LBB4_366
# %bb.139:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.140:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.141:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_519
# %bb.142:
	xor	edx, edx
	jmp	.LBB4_522
.LBB4_143:
	cmp	esi, 8
	jle	.LBB4_249
# %bb.144:
	cmp	esi, 9
	je	.LBB4_369
# %bb.145:
	cmp	esi, 11
	je	.LBB4_372
# %bb.146:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.147:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.148:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_528
# %bb.149:
	xor	edx, edx
	jmp	.LBB4_531
.LBB4_150:
	cmp	esi, 8
	jle	.LBB4_257
# %bb.151:
	cmp	esi, 9
	je	.LBB4_375
# %bb.152:
	cmp	esi, 11
	je	.LBB4_378
# %bb.153:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.154:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.155:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_156
# %bb.535:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_914
# %bb.536:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_914
.LBB4_156:
	xor	edx, edx
.LBB4_917:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_922
# %bb.918:
	vmovq	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = mem[0],zero
	jmp	.LBB4_920
.LBB4_919:                              #   in Loop: Header=BB4_920 Depth=1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	je	.LBB4_922
.LBB4_920:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_919
# %bb.921:                              #   in Loop: Header=BB4_920 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_919
.LBB4_157:
	cmp	esi, 8
	jle	.LBB4_262
# %bb.158:
	cmp	esi, 9
	je	.LBB4_381
# %bb.159:
	cmp	esi, 11
	je	.LBB4_384
# %bb.160:
	cmp	esi, 12
	jne	.LBB4_1351
# %bb.161:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.162:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_538
# %bb.163:
	xor	edx, edx
	jmp	.LBB4_541
.LBB4_164:
	cmp	esi, 2
	je	.LBB4_387
# %bb.165:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.166:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.167:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_168
# %bb.547:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_933
# %bb.548:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_933
.LBB4_168:
	xor	edx, edx
.LBB4_936:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_938
.LBB4_937:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_937
.LBB4_938:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_939:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_939
	jmp	.LBB4_1351
.LBB4_169:
	cmp	esi, 2
	je	.LBB4_390
# %bb.170:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.171:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.172:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_173
# %bb.550:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_940
# %bb.551:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_940
.LBB4_173:
	xor	edx, edx
.LBB4_943:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_945
# %bb.944:
	vmovsd	xmm0, qword ptr [rcx + 8*rdx]   # xmm0 = mem[0],zero
	xor	edi, edi
	vpxor	xmm1, xmm1, xmm1
	vucomisd	xmm1, xmm0
	vandpd	xmm0, xmm0, xmmword ptr [rip + .LCPI4_2]
	vmovddup	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = [1.0E+0,1.0E+0]
                                        # xmm1 = mem[0,0]
	vorpd	xmm0, xmm1, xmm0
	vcvttsd2si	ebx, xmm0
	cmove	ebx, edi
	mov	byte ptr [r8 + rdx], bl
	or	rdx, 1
.LBB4_945:
	add	rsi, rax
	je	.LBB4_1351
# %bb.946:
	xor	esi, esi
	vxorpd	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_947:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx], dil
	vmovsd	xmm3, qword ptr [rcx + 8*rdx + 8] # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx + 1], dil
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_947
	jmp	.LBB4_1351
.LBB4_174:
	cmp	esi, 2
	je	.LBB4_393
# %bb.175:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.176:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.177:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB4_178
# %bb.553:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_948
# %bb.554:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_948
.LBB4_178:
	xor	r11d, r11d
.LBB4_951:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_953
# %bb.952:
	mov	dil, byte ptr [rcx + r11]
	test	dil, dil
	setne	r9b
	neg	r9b
	test	dil, dil
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_953:
	add	rsi, r10
	je	.LBB4_1351
# %bb.954:
	mov	esi, 1
.LBB4_955:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + r11]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	movzx	eax, byte ptr [rcx + r11 + 1]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_955
	jmp	.LBB4_1351
.LBB4_179:
	cmp	esi, 2
	je	.LBB4_396
# %bb.180:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.181:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.182:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_183
# %bb.556:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_956
# %bb.557:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_956
.LBB4_183:
	xor	edx, edx
.LBB4_959:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_961
.LBB4_960:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_960
.LBB4_961:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_962:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_962
	jmp	.LBB4_1351
.LBB4_184:
	cmp	esi, 2
	je	.LBB4_399
# %bb.185:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.186:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.187:
	mov	eax, r9d
	cmp	r9d, 64
	jb	.LBB4_188
# %bb.559:
	lea	rdx, [rcx + 2*rax]
	cmp	rdx, r8
	jbe	.LBB4_963
# %bb.560:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_963
.LBB4_188:
	xor	edx, edx
.LBB4_966:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_968
.LBB4_967:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_967
.LBB4_968:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_969:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_969
	jmp	.LBB4_1351
.LBB4_189:
	cmp	esi, 2
	je	.LBB4_402
# %bb.190:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.191:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.192:
	mov	r10d, r9d
	cmp	r9d, 64
	jb	.LBB4_193
# %bb.562:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB4_970
# %bb.563:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_970
.LBB4_193:
	xor	r11d, r11d
.LBB4_973:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_975
# %bb.974:
	movzx	edi, word ptr [rcx + 2*r11]
	test	di, di
	setne	r9b
	neg	r9b
	test	di, di
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_975:
	add	rsi, r10
	je	.LBB4_1351
# %bb.976:
	mov	esi, 1
.LBB4_977:                              # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*r11]
	test	di, di
	setne	al
	neg	al
	test	di, di
	movzx	eax, al
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	movzx	eax, word ptr [rcx + 2*r11 + 2]
	test	ax, ax
	setne	dl
	neg	dl
	test	ax, ax
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_977
	jmp	.LBB4_1351
.LBB4_194:
	cmp	esi, 2
	je	.LBB4_405
# %bb.195:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.196:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.197:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_198
# %bb.565:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB4_978
# %bb.566:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_978
.LBB4_198:
	xor	r11d, r11d
.LBB4_981:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_983
# %bb.982:
	mov	rdi, qword ptr [rcx + 8*r11]
	test	rdi, rdi
	setne	r9b
	neg	r9b
	test	rdi, rdi
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_983:
	add	rsi, r10
	je	.LBB4_1351
# %bb.984:
	mov	esi, 1
.LBB4_985:                              # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*r11]
	test	rdi, rdi
	setne	al
	neg	al
	test	rdi, rdi
	movzx	eax, al
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	mov	rax, qword ptr [rcx + 8*r11 + 8]
	test	rax, rax
	setne	dl
	neg	dl
	test	rax, rax
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_985
	jmp	.LBB4_1351
.LBB4_199:
	cmp	esi, 2
	je	.LBB4_408
# %bb.200:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.201:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.202:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB4_203
# %bb.568:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_986
# %bb.569:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_986
.LBB4_203:
	xor	edx, edx
.LBB4_989:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_991
# %bb.990:
	vmovd	xmm0, dword ptr [rcx + 4*rdx]   # xmm0 = mem[0],zero,zero,zero
	vmovd	edi, xmm0
	test	edi, edi
	setns	dil
	add	dil, dil
	add	dil, -1
	xor	r9d, r9d
	vpxor	xmm1, xmm1, xmm1
	vucomiss	xmm1, xmm0
	movzx	edi, dil
	cmove	edi, r9d
	mov	byte ptr [r8 + rdx], dil
	or	rdx, 1
.LBB4_991:
	add	rsi, r10
	je	.LBB4_1351
# %bb.992:
	xor	esi, esi
	vxorps	xmm0, xmm0, xmm0
.LBB4_993:                              # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	vmovd	edi, xmm1
	test	edi, edi
	setns	al
	add	al, al
	add	al, -1
	vucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx], al
	vmovd	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	vmovd	eax, xmm1
	test	eax, eax
	setns	al
	add	al, al
	add	al, -1
	vucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx + 1], al
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_993
	jmp	.LBB4_1351
.LBB4_204:
	cmp	esi, 2
	je	.LBB4_411
# %bb.205:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.206:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.207:
	mov	eax, r9d
	cmp	r9d, 128
	jb	.LBB4_208
# %bb.571:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_994
# %bb.572:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_994
.LBB4_208:
	xor	edx, edx
.LBB4_997:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_999
.LBB4_998:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_998
.LBB4_999:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_1000:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1000
	jmp	.LBB4_1351
.LBB4_209:
	cmp	esi, 2
	je	.LBB4_414
# %bb.210:
	cmp	esi, 3
	jne	.LBB4_1351
# %bb.211:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.212:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB4_213
# %bb.574:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_1001
# %bb.575:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_1001
.LBB4_213:
	xor	r11d, r11d
.LBB4_1004:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_1006
# %bb.1005:
	mov	edi, dword ptr [rcx + 4*r11]
	test	edi, edi
	setne	r9b
	neg	r9b
	test	edi, edi
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_1006:
	add	rsi, r10
	je	.LBB4_1351
# %bb.1007:
	mov	esi, 1
.LBB4_1008:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*r11]
	test	edi, edi
	setne	al
	neg	al
	test	edi, edi
	movzx	eax, al
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	mov	eax, dword ptr [rcx + 4*r11 + 4]
	test	eax, eax
	setne	dl
	neg	dl
	test	eax, eax
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_1008
	jmp	.LBB4_1351
.LBB4_214:
	cmp	esi, 7
	je	.LBB4_417
# %bb.215:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.216:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.217:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_577
# %bb.218:
	xor	edx, edx
	jmp	.LBB4_580
.LBB4_219:
	cmp	esi, 7
	je	.LBB4_420
# %bb.220:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.221:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.222:
	mov	r10d, r9d
	movabs	r11, -9223372036854775808
	cmp	r9d, 4
	jae	.LBB4_581
# %bb.223:
	xor	esi, esi
	jmp	.LBB4_1286
.LBB4_224:
	cmp	esi, 7
	je	.LBB4_423
# %bb.225:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.226:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.227:
	mov	r11d, r9d
	cmp	r9d, 16
	jb	.LBB4_228
# %bb.584:
	lea	rdx, [rcx + r11]
	cmp	rdx, r8
	jbe	.LBB4_1009
# %bb.585:
	lea	rdx, [r8 + 8*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1009
.LBB4_228:
	xor	edx, edx
.LBB4_1012:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1014
# %bb.1013:
	mov	r9b, byte ptr [rcx + rdx]
	xor	r10d, r10d
	test	r9b, r9b
	setne	r10b
	neg	r10
	test	r9b, r9b
	mov	edi, 1
	cmovle	rdi, r10
	mov	qword ptr [r8 + 8*rdx], rdi
	or	rdx, 1
.LBB4_1014:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1015:
	mov	esi, 1
.LBB4_1016:                             # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rdx
	jne	.LBB4_1016
	jmp	.LBB4_1351
.LBB4_229:
	cmp	esi, 7
	je	.LBB4_426
# %bb.230:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.231:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.232:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_233
# %bb.587:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_1017
# %bb.588:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1017
.LBB4_233:
	xor	edx, edx
.LBB4_1020:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1022
.LBB4_1021:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1021
.LBB4_1022:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1023:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx + 8], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 8], rsi
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx + 16], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 16], rsi
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx + 24], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 24], rsi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1023
	jmp	.LBB4_1351
.LBB4_234:
	cmp	esi, 7
	je	.LBB4_429
# %bb.235:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.236:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.237:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_590
# %bb.238:
	xor	edx, edx
	jmp	.LBB4_593
.LBB4_239:
	cmp	esi, 7
	je	.LBB4_432
# %bb.240:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.241:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.242:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_594
# %bb.243:
	xor	edx, edx
	jmp	.LBB4_597
.LBB4_244:
	cmp	esi, 7
	je	.LBB4_435
# %bb.245:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.246:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.247:
	mov	r11d, r9d
	cmp	r9d, 16
	jb	.LBB4_248
# %bb.599:
	lea	rdx, [rcx + 8*r11]
	cmp	rdx, r8
	jbe	.LBB4_1024
# %bb.600:
	lea	rdx, [r8 + 8*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1024
.LBB4_248:
	xor	edx, edx
.LBB4_1027:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1029
# %bb.1028:
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
.LBB4_1029:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1030:
	mov	esi, 1
.LBB4_1031:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1031
	jmp	.LBB4_1351
.LBB4_249:
	cmp	esi, 7
	je	.LBB4_438
# %bb.250:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.251:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.252:
	mov	r10d, r9d
	cmp	r9d, 1
	jne	.LBB4_602
# %bb.253:
	xor	eax, eax
	jmp	.LBB4_254
.LBB4_257:
	cmp	esi, 7
	je	.LBB4_441
# %bb.258:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.259:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.260:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_261
# %bb.610:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1032
# %bb.611:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1032
.LBB4_261:
	xor	edx, edx
.LBB4_1035:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1037
.LBB4_1036:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1036
.LBB4_1037:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1038:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 8], rsi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 16], rsi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 24], rsi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1038
	jmp	.LBB4_1351
.LBB4_262:
	cmp	esi, 7
	je	.LBB4_444
# %bb.263:
	cmp	esi, 8
	jne	.LBB4_1351
# %bb.264:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.265:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_613
# %bb.266:
	xor	edx, edx
	jmp	.LBB4_616
.LBB4_267:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.268:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_618
# %bb.269:
	xor	edx, edx
	jmp	.LBB4_621
.LBB4_270:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.271:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_622
# %bb.272:
	xor	edx, edx
	jmp	.LBB4_625
.LBB4_273:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.274:
	mov	eax, r9d
	xor	edx, edx
	cmp	r9d, 16
	jae	.LBB4_626
# %bb.275:
	xor	esi, esi
	jmp	.LBB4_629
.LBB4_276:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.277:
	mov	eax, r9d
	xor	edx, edx
	cmp	r9d, 16
	jae	.LBB4_631
# %bb.278:
	xor	esi, esi
	jmp	.LBB4_634
.LBB4_279:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.280:
	mov	r11d, r9d
	cmp	r9d, 64
	jb	.LBB4_281
# %bb.636:
	lea	rdx, [rcx + r11]
	cmp	rdx, r8
	jbe	.LBB4_1039
# %bb.637:
	lea	rdx, [r8 + 2*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1039
.LBB4_281:
	xor	edx, edx
.LBB4_1042:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1044
# %bb.1043:
	mov	r9b, byte ptr [rcx + rdx]
	xor	r10d, r10d
	test	r9b, r9b
	setne	r10b
	neg	r10d
	test	r9b, r9b
	mov	edi, 1
	cmovle	edi, r10d
	mov	word ptr [r8 + 2*rdx], di
	or	rdx, 1
.LBB4_1044:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1045:
	mov	esi, 1
.LBB4_1046:                             # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rdx
	jne	.LBB4_1046
	jmp	.LBB4_1351
.LBB4_282:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.283:
	mov	r11d, r9d
	cmp	r9d, 64
	jb	.LBB4_284
# %bb.639:
	lea	rdx, [rcx + r11]
	cmp	rdx, r8
	jbe	.LBB4_1047
# %bb.640:
	lea	rdx, [r8 + 2*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1047
.LBB4_284:
	xor	edx, edx
.LBB4_1050:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1052
# %bb.1051:
	mov	r9b, byte ptr [rcx + rdx]
	xor	r10d, r10d
	test	r9b, r9b
	setne	r10b
	neg	r10d
	test	r9b, r9b
	mov	edi, 1
	cmovle	edi, r10d
	mov	word ptr [r8 + 2*rdx], di
	or	rdx, 1
.LBB4_1052:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1053:
	mov	esi, 1
.LBB4_1054:                             # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rdx
	jne	.LBB4_1054
	jmp	.LBB4_1351
.LBB4_285:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.286:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_642
# %bb.287:
	xor	edx, edx
	jmp	.LBB4_645
.LBB4_288:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.289:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_646
# %bb.290:
	xor	edx, edx
	jmp	.LBB4_649
.LBB4_291:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.292:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_293
# %bb.650:
	lea	rdx, [rcx + 2*rax]
	cmp	rdx, r8
	jbe	.LBB4_1055
# %bb.651:
	lea	rdx, [r8 + 2*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1055
.LBB4_293:
	xor	edx, edx
.LBB4_1321:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1323
.LBB4_1322:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1322
.LBB4_1323:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1324:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx + 2], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 2], si
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx + 4], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 4], si
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx + 6], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 6], si
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1324
	jmp	.LBB4_1351
.LBB4_294:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.295:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_296
# %bb.653:
	lea	rdx, [rcx + 2*rax]
	cmp	rdx, r8
	jbe	.LBB4_1058
# %bb.654:
	lea	rdx, [r8 + 2*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1058
.LBB4_296:
	xor	edx, edx
.LBB4_1329:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1331
.LBB4_1330:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1330
.LBB4_1331:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1332:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx + 2], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 2], si
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx + 4], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 4], si
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx + 6], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 6], si
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1332
	jmp	.LBB4_1351
.LBB4_297:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.298:
	mov	r11d, r9d
	cmp	r9d, 32
	jb	.LBB4_299
# %bb.656:
	lea	rdx, [rcx + 2*r11]
	cmp	rdx, r8
	jbe	.LBB4_1061
# %bb.657:
	lea	rdx, [r8 + 2*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1061
.LBB4_299:
	xor	edx, edx
.LBB4_1337:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1339
# %bb.1338:
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
.LBB4_1339:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1340:
	mov	esi, 1
.LBB4_1341:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1341
	jmp	.LBB4_1351
.LBB4_300:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.301:
	mov	r11d, r9d
	cmp	r9d, 32
	jb	.LBB4_302
# %bb.659:
	lea	rdx, [rcx + 2*r11]
	cmp	rdx, r8
	jbe	.LBB4_1064
# %bb.660:
	lea	rdx, [r8 + 2*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1064
.LBB4_302:
	xor	edx, edx
.LBB4_1346:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1348
# %bb.1347:
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
.LBB4_1348:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1349:
	mov	esi, 1
.LBB4_1350:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1350
	jmp	.LBB4_1351
.LBB4_303:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.304:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_662
# %bb.305:
	xor	edx, edx
	jmp	.LBB4_665
.LBB4_306:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.307:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_667
# %bb.308:
	xor	edx, edx
	jmp	.LBB4_670
.LBB4_309:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.310:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 32
	jae	.LBB4_672
# %bb.311:
	xor	esi, esi
	jmp	.LBB4_675
.LBB4_312:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.313:
	mov	eax, r9d
	xor	r10d, r10d
	cmp	r9d, 32
	jae	.LBB4_677
# %bb.314:
	xor	esi, esi
	jmp	.LBB4_680
.LBB4_315:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.316:
	mov	eax, r9d
	cmp	r9d, 64
	jb	.LBB4_317
# %bb.682:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1067
# %bb.683:
	lea	rdx, [r8 + 2*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1067
.LBB4_317:
	xor	edx, edx
.LBB4_1070:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1072
.LBB4_1071:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1071
.LBB4_1072:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1073:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 2], si
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 4], si
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 6], si
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1073
	jmp	.LBB4_1351
.LBB4_318:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.319:
	mov	eax, r9d
	cmp	r9d, 64
	jb	.LBB4_320
# %bb.685:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1074
# %bb.686:
	lea	rdx, [r8 + 2*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1074
.LBB4_320:
	xor	edx, edx
.LBB4_1077:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1079
.LBB4_1078:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1078
.LBB4_1079:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1080:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 2], si
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 4], si
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx + 6], si
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1080
	jmp	.LBB4_1351
.LBB4_321:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.322:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB4_688
# %bb.323:
	xor	edx, edx
	jmp	.LBB4_691
.LBB4_324:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.325:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB4_693
# %bb.326:
	xor	edx, edx
	jmp	.LBB4_696
.LBB4_327:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.328:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_698
# %bb.329:
	xor	edx, edx
	jmp	.LBB4_701
.LBB4_330:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.331:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_702
# %bb.332:
	xor	edx, edx
	jmp	.LBB4_705
.LBB4_333:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.334:
	mov	eax, r9d
	cmp	r9d, 4
	jae	.LBB4_709
# %bb.335:
	xor	edx, edx
	jmp	.LBB4_1306
.LBB4_336:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.337:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_712
# %bb.338:
	xor	edx, edx
	jmp	.LBB4_715
.LBB4_339:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.340:
	mov	r11d, r9d
	cmp	r9d, 16
	jb	.LBB4_341
# %bb.719:
	lea	rdx, [rcx + r11]
	cmp	rdx, r8
	jbe	.LBB4_1081
# %bb.720:
	lea	rdx, [r8 + 8*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1081
.LBB4_341:
	xor	edx, edx
.LBB4_1084:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1086
# %bb.1085:
	mov	r9b, byte ptr [rcx + rdx]
	xor	r10d, r10d
	test	r9b, r9b
	setne	r10b
	neg	r10
	test	r9b, r9b
	mov	edi, 1
	cmovle	rdi, r10
	mov	qword ptr [r8 + 8*rdx], rdi
	or	rdx, 1
.LBB4_1086:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1087:
	mov	esi, 1
.LBB4_1088:                             # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rdx
	jne	.LBB4_1088
	jmp	.LBB4_1351
.LBB4_342:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.343:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_344
# %bb.722:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1089
# %bb.723:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1089
.LBB4_344:
	xor	edx, edx
.LBB4_1092:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1269
# %bb.1093:
	cmp	byte ptr [rcx + rdx], 0
	jne	.LBB4_1265
# %bb.1094:
	vpxor	xmm0, xmm0, xmm0
	jmp	.LBB4_1266
.LBB4_345:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.346:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_347
# %bb.725:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_1095
# %bb.726:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1095
.LBB4_347:
	xor	edx, edx
.LBB4_1098:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1100
.LBB4_1099:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1099
.LBB4_1100:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1101:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx + 8], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 8], rsi
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx + 16], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 16], rsi
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx + 24], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 24], rsi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1101
	jmp	.LBB4_1351
.LBB4_348:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.349:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_728
# %bb.350:
	xor	edx, edx
	jmp	.LBB4_731
.LBB4_351:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.352:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_735
# %bb.353:
	xor	edx, edx
	jmp	.LBB4_738
.LBB4_354:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.355:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_739
# %bb.356:
	xor	edx, edx
	jmp	.LBB4_742
.LBB4_357:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.358:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_746
# %bb.359:
	xor	edx, edx
	jmp	.LBB4_749
.LBB4_360:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.361:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_751
# %bb.362:
	xor	edx, edx
	jmp	.LBB4_754
.LBB4_363:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.364:
	mov	r11d, r9d
	cmp	r9d, 16
	jb	.LBB4_365
# %bb.760:
	lea	rdx, [rcx + 8*r11]
	cmp	rdx, r8
	jbe	.LBB4_1102
# %bb.761:
	lea	rdx, [r8 + 8*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1102
.LBB4_365:
	xor	edx, edx
.LBB4_1105:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1107
# %bb.1106:
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
.LBB4_1107:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1108:
	mov	esi, 1
.LBB4_1109:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1109
	jmp	.LBB4_1351
.LBB4_366:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.367:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_763
# %bb.368:
	xor	edx, edx
	jmp	.LBB4_766
.LBB4_369:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.370:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB4_772
# %bb.371:
	xor	edx, edx
	jmp	.LBB4_1312
.LBB4_372:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.373:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_374
# %bb.775:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_1110
# %bb.776:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1110
.LBB4_374:
	xor	edx, edx
.LBB4_1113:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1115
# %bb.1114:
	vmovss	xmm0, dword ptr [rcx + 4*rdx]   # xmm0 = mem[0],zero,zero,zero
	vmovmskps	edi, xmm0
	and	edi, 1
	neg	edi
	or	edi, 1
	vcvtsi2ss	xmm1, xmm10, edi
	vxorps	xmm2, xmm2, xmm2
	vcmpeqss	xmm0, xmm0, xmm2
	vandnps	xmm0, xmm0, xmm1
	vmovss	dword ptr [r8 + 4*rdx], xmm0
	or	rdx, 1
.LBB4_1115:
	add	rsi, rax
	je	.LBB4_1351
# %bb.1116:
	vxorps	xmm0, xmm0, xmm0
.LBB4_1117:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	vmovmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	vcvtsi2ss	xmm2, xmm10, esi
	vcmpeqss	xmm1, xmm1, xmm0
	vandnps	xmm1, xmm1, xmm2
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	vmovss	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	vmovmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	vcvtsi2ss	xmm2, xmm10, esi
	vcmpeqss	xmm1, xmm1, xmm0
	vandnps	xmm1, xmm1, xmm2
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm1
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_1117
	jmp	.LBB4_1351
.LBB4_375:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.376:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_377
# %bb.778:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1118
# %bb.779:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1118
.LBB4_377:
	xor	edx, edx
.LBB4_1121:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1123
.LBB4_1122:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1122
.LBB4_1123:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1124:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 8], rsi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 16], rsi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx + 24], rsi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1124
	jmp	.LBB4_1351
.LBB4_378:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.379:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_380
# %bb.781:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1125
# %bb.782:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1125
.LBB4_380:
	xor	edx, edx
.LBB4_1128:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1133
# %bb.1129:
	vmovd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_1131
.LBB4_1130:                             #   in Loop: Header=BB4_1131 Depth=1
	vmovd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	je	.LBB4_1133
.LBB4_1131:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_1130
# %bb.1132:                             #   in Loop: Header=BB4_1131 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_1130
.LBB4_381:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.382:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_784
# %bb.383:
	xor	edx, edx
	jmp	.LBB4_787
.LBB4_384:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.385:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_789
# %bb.386:
	xor	edx, edx
	jmp	.LBB4_792
.LBB4_387:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.388:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_389
# %bb.798:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_1144
# %bb.799:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_1144
.LBB4_389:
	xor	edx, edx
.LBB4_1147:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1149
.LBB4_1148:                             # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1148
.LBB4_1149:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_1150:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1150
	jmp	.LBB4_1351
.LBB4_390:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.391:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_392
# %bb.801:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_1151
# %bb.802:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_1151
.LBB4_392:
	xor	edx, edx
.LBB4_1154:
	mov	rsi, rdx
	not	rsi
	test	al, 1
	je	.LBB4_1156
# %bb.1155:
	vmovsd	xmm0, qword ptr [rcx + 8*rdx]   # xmm0 = mem[0],zero
	xor	edi, edi
	vpxor	xmm1, xmm1, xmm1
	vucomisd	xmm1, xmm0
	vandpd	xmm0, xmm0, xmmword ptr [rip + .LCPI4_2]
	vmovddup	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = [1.0E+0,1.0E+0]
                                        # xmm1 = mem[0,0]
	vorpd	xmm0, xmm1, xmm0
	vcvttsd2si	ebx, xmm0
	cmove	ebx, edi
	mov	byte ptr [r8 + rdx], bl
	or	rdx, 1
.LBB4_1156:
	add	rsi, rax
	je	.LBB4_1351
# %bb.1157:
	xor	esi, esi
	vxorpd	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_1158:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx], dil
	vmovsd	xmm3, qword ptr [rcx + 8*rdx + 8] # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, esi
	mov	byte ptr [r8 + rdx + 1], dil
	add	rdx, 2
	cmp	rax, rdx
	jne	.LBB4_1158
	jmp	.LBB4_1351
.LBB4_393:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.394:
	mov	r10d, r9d
	cmp	r9d, 128
	jb	.LBB4_395
# %bb.804:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB4_1159
# %bb.805:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_1159
.LBB4_395:
	xor	r11d, r11d
.LBB4_1162:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_1164
# %bb.1163:
	mov	dil, byte ptr [rcx + r11]
	test	dil, dil
	setne	r9b
	neg	r9b
	test	dil, dil
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_1164:
	add	rsi, r10
	je	.LBB4_1351
# %bb.1165:
	mov	esi, 1
.LBB4_1166:                             # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + r11]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	movzx	eax, byte ptr [rcx + r11 + 1]
	test	al, al
	setne	dl
	neg	dl
	test	al, al
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_1166
	jmp	.LBB4_1351
.LBB4_396:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.397:
	mov	eax, r9d
	cmp	r9d, 16
	jb	.LBB4_398
# %bb.807:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB4_1167
# %bb.808:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_1167
.LBB4_398:
	xor	edx, edx
.LBB4_1170:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1172
.LBB4_1171:                             # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1171
.LBB4_1172:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_1173:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1173
	jmp	.LBB4_1351
.LBB4_399:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.400:
	mov	eax, r9d
	cmp	r9d, 64
	jb	.LBB4_401
# %bb.810:
	lea	rdx, [rcx + 2*rax]
	cmp	rdx, r8
	jbe	.LBB4_1174
# %bb.811:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_1174
.LBB4_401:
	xor	edx, edx
.LBB4_1177:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1179
.LBB4_1178:                             # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1178
.LBB4_1179:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_1180:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1180
	jmp	.LBB4_1351
.LBB4_402:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.403:
	mov	r10d, r9d
	cmp	r9d, 64
	jb	.LBB4_404
# %bb.813:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB4_1181
# %bb.814:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_1181
.LBB4_404:
	xor	r11d, r11d
.LBB4_1184:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_1186
# %bb.1185:
	movzx	edi, word ptr [rcx + 2*r11]
	test	di, di
	setne	r9b
	neg	r9b
	test	di, di
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_1186:
	add	rsi, r10
	je	.LBB4_1351
# %bb.1187:
	mov	esi, 1
.LBB4_1188:                             # =>This Inner Loop Header: Depth=1
	movzx	edi, word ptr [rcx + 2*r11]
	test	di, di
	setne	al
	neg	al
	test	di, di
	movzx	eax, al
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	movzx	eax, word ptr [rcx + 2*r11 + 2]
	test	ax, ax
	setne	dl
	neg	dl
	test	ax, ax
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_1188
	jmp	.LBB4_1351
.LBB4_405:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.406:
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB4_407
# %bb.816:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB4_1189
# %bb.817:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_1189
.LBB4_407:
	xor	r11d, r11d
.LBB4_1192:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_1194
# %bb.1193:
	mov	rdi, qword ptr [rcx + 8*r11]
	test	rdi, rdi
	setne	r9b
	neg	r9b
	test	rdi, rdi
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_1194:
	add	rsi, r10
	je	.LBB4_1351
# %bb.1195:
	mov	esi, 1
.LBB4_1196:                             # =>This Inner Loop Header: Depth=1
	mov	rdi, qword ptr [rcx + 8*r11]
	test	rdi, rdi
	setne	al
	neg	al
	test	rdi, rdi
	movzx	eax, al
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	mov	rax, qword ptr [rcx + 8*r11 + 8]
	test	rax, rax
	setne	dl
	neg	dl
	test	rax, rax
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_1196
	jmp	.LBB4_1351
.LBB4_408:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.409:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB4_410
# %bb.819:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_1197
# %bb.820:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_1197
.LBB4_410:
	xor	edx, edx
.LBB4_1200:
	mov	rsi, rdx
	not	rsi
	test	r10b, 1
	je	.LBB4_1202
# %bb.1201:
	vmovd	xmm0, dword ptr [rcx + 4*rdx]   # xmm0 = mem[0],zero,zero,zero
	vmovd	edi, xmm0
	test	edi, edi
	setns	dil
	add	dil, dil
	add	dil, -1
	xor	r9d, r9d
	vpxor	xmm1, xmm1, xmm1
	vucomiss	xmm1, xmm0
	movzx	edi, dil
	cmove	edi, r9d
	mov	byte ptr [r8 + rdx], dil
	or	rdx, 1
.LBB4_1202:
	add	rsi, r10
	je	.LBB4_1351
# %bb.1203:
	xor	esi, esi
	vxorps	xmm0, xmm0, xmm0
.LBB4_1204:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	vmovd	edi, xmm1
	test	edi, edi
	setns	al
	add	al, al
	add	al, -1
	vucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx], al
	vmovd	xmm1, dword ptr [rcx + 4*rdx + 4] # xmm1 = mem[0],zero,zero,zero
	vmovd	eax, xmm1
	test	eax, eax
	setns	al
	add	al, al
	add	al, -1
	vucomiss	xmm0, xmm1
	movzx	eax, al
	cmove	eax, esi
	mov	byte ptr [r8 + rdx + 1], al
	add	rdx, 2
	cmp	r10, rdx
	jne	.LBB4_1204
	jmp	.LBB4_1351
.LBB4_411:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.412:
	mov	eax, r9d
	cmp	r9d, 128
	jb	.LBB4_413
# %bb.822:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1205
# %bb.823:
	lea	rdx, [r8 + rax]
	cmp	rdx, rcx
	jbe	.LBB4_1205
.LBB4_413:
	xor	edx, edx
.LBB4_1208:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1210
.LBB4_1209:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	setne	byte ptr [r8 + rdx]
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1209
.LBB4_1210:
	cmp	rsi, 3
	jb	.LBB4_1351
.LBB4_1211:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1211
	jmp	.LBB4_1351
.LBB4_414:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.415:
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB4_416
# %bb.825:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB4_1212
# %bb.826:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB4_1212
.LBB4_416:
	xor	r11d, r11d
.LBB4_1215:
	mov	rsi, r11
	not	rsi
	test	r10b, 1
	je	.LBB4_1217
# %bb.1216:
	mov	edi, dword ptr [rcx + 4*r11]
	test	edi, edi
	setne	r9b
	neg	r9b
	test	edi, edi
	movzx	r9d, r9b
	mov	edi, 1
	cmovle	edi, r9d
	mov	byte ptr [r8 + r11], dil
	or	r11, 1
.LBB4_1217:
	add	rsi, r10
	je	.LBB4_1351
# %bb.1218:
	mov	esi, 1
.LBB4_1219:                             # =>This Inner Loop Header: Depth=1
	mov	edi, dword ptr [rcx + 4*r11]
	test	edi, edi
	setne	al
	neg	al
	test	edi, edi
	movzx	eax, al
	cmovg	eax, esi
	mov	byte ptr [r8 + r11], al
	mov	eax, dword ptr [rcx + 4*r11 + 4]
	test	eax, eax
	setne	dl
	neg	dl
	test	eax, eax
	movzx	eax, dl
	cmovg	eax, esi
	mov	byte ptr [r8 + r11 + 1], al
	add	r11, 2
	cmp	r10, r11
	jne	.LBB4_1219
	jmp	.LBB4_1351
.LBB4_417:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.418:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_419
# %bb.828:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB4_1220
# %bb.829:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1220
.LBB4_419:
	xor	edx, edx
.LBB4_1223:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1225
.LBB4_1224:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1224
.LBB4_1225:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1226:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx + 4], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 4], esi
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx + 8], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 8], esi
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx + 12], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 12], esi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1226
	jmp	.LBB4_1351
.LBB4_420:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.421:
	mov	eax, r9d
	xor	edx, edx
	cmp	r9d, 16
	jae	.LBB4_831
# %bb.422:
	xor	esi, esi
	jmp	.LBB4_834
.LBB4_423:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.424:
	mov	r11d, r9d
	cmp	r9d, 32
	jb	.LBB4_425
# %bb.836:
	lea	rdx, [rcx + r11]
	cmp	rdx, r8
	jbe	.LBB4_1227
# %bb.837:
	lea	rdx, [r8 + 4*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1227
.LBB4_425:
	xor	edx, edx
.LBB4_1230:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1232
# %bb.1231:
	mov	r9b, byte ptr [rcx + rdx]
	xor	r10d, r10d
	test	r9b, r9b
	setne	r10b
	neg	r10d
	test	r9b, r9b
	mov	edi, 1
	cmovle	edi, r10d
	mov	dword ptr [r8 + 4*rdx], edi
	or	rdx, 1
.LBB4_1232:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1233:
	mov	esi, 1
.LBB4_1234:                             # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rdx
	jne	.LBB4_1234
	jmp	.LBB4_1351
.LBB4_426:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.427:
	mov	eax, r9d
	cmp	r9d, 16
	jae	.LBB4_839
# %bb.428:
	xor	edx, edx
	jmp	.LBB4_842
.LBB4_429:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.430:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_843
# %bb.431:
	xor	edx, edx
	jmp	.LBB4_846
.LBB4_432:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.433:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB4_847
# %bb.434:
	xor	edx, edx
	jmp	.LBB4_850
.LBB4_435:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.436:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB4_852
# %bb.437:
	xor	edx, edx
	jmp	.LBB4_855
.LBB4_438:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.439:
	mov	eax, r9d
	cmp	r9d, 32
	jae	.LBB4_857
# %bb.440:
	xor	edx, edx
	jmp	.LBB4_860
.LBB4_441:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.442:
	mov	eax, r9d
	cmp	r9d, 32
	jb	.LBB4_443
# %bb.864:
	lea	rdx, [rcx + rax]
	cmp	rdx, r8
	jbe	.LBB4_1235
# %bb.865:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB4_1235
.LBB4_443:
	xor	edx, edx
.LBB4_1238:
	mov	r9, rdx
	not	r9
	add	r9, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB4_1240
.LBB4_1239:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	add	rdi, -1
	jne	.LBB4_1239
.LBB4_1240:
	cmp	r9, 3
	jb	.LBB4_1351
.LBB4_1241:                             # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	byte ptr [rcx + rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 1], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 4], esi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 2], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 8], esi
	xor	esi, esi
	cmp	byte ptr [rcx + rdx + 3], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx + 12], esi
	add	rdx, 4
	cmp	rax, rdx
	jne	.LBB4_1241
	jmp	.LBB4_1351
.LBB4_444:
	test	r9d, r9d
	jle	.LBB4_1351
# %bb.445:
	mov	r11d, r9d
	cmp	r9d, 32
	jb	.LBB4_446
# %bb.867:
	lea	rdx, [rcx + 4*r11]
	cmp	rdx, r8
	jbe	.LBB4_1242
# %bb.868:
	lea	rdx, [r8 + 4*r11]
	cmp	rdx, rcx
	jbe	.LBB4_1242
.LBB4_446:
	xor	edx, edx
.LBB4_1245:
	mov	rsi, rdx
	not	rsi
	test	r11b, 1
	je	.LBB4_1247
# %bb.1246:
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
.LBB4_1247:
	add	rsi, r11
	je	.LBB4_1351
# %bb.1248:
	mov	esi, 1
.LBB4_1249:                             # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_1249
	jmp	.LBB4_1351
.LBB4_922:
	cmp	rsi, 3
	jb	.LBB4_1351
# %bb.923:
	vmovq	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = mem[0],zero
	jmp	.LBB4_925
.LBB4_924:                              #   in Loop: Header=BB4_925 Depth=1
	vmovq	qword ptr [r8 + 8*rdx + 24], xmm1
	add	rdx, 4
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_925:                              # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_926
# %bb.929:                              #   in Loop: Header=BB4_925 Depth=1
	vpxor	xmm1, xmm1, xmm1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovdqa	xmm1, xmm0
	je	.LBB4_930
.LBB4_927:                              #   in Loop: Header=BB4_925 Depth=1
	vmovq	qword ptr [r8 + 8*rdx + 8], xmm1
	cmp	byte ptr [rcx + rdx + 2], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_928
.LBB4_931:                              #   in Loop: Header=BB4_925 Depth=1
	vpxor	xmm1, xmm1, xmm1
	vmovq	qword ptr [r8 + 8*rdx + 16], xmm1
	cmp	byte ptr [rcx + rdx + 3], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_924
	jmp	.LBB4_932
.LBB4_926:                              #   in Loop: Header=BB4_925 Depth=1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_927
.LBB4_930:                              #   in Loop: Header=BB4_925 Depth=1
	vpxor	xmm1, xmm1, xmm1
	vmovq	qword ptr [r8 + 8*rdx + 8], xmm1
	cmp	byte ptr [rcx + rdx + 2], 0
	vmovdqa	xmm1, xmm0
	je	.LBB4_931
.LBB4_928:                              #   in Loop: Header=BB4_925 Depth=1
	vmovq	qword ptr [r8 + 8*rdx + 16], xmm1
	cmp	byte ptr [rcx + rdx + 3], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_924
.LBB4_932:                              #   in Loop: Header=BB4_925 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_924
.LBB4_1133:
	cmp	rsi, 3
	jb	.LBB4_1351
# %bb.1134:
	vmovd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_1136
.LBB4_1135:                             #   in Loop: Header=BB4_1136 Depth=1
	vmovd	dword ptr [r8 + 4*rdx + 12], xmm1
	add	rdx, 4
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_1136:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_1137
# %bb.1140:                             #   in Loop: Header=BB4_1136 Depth=1
	vpxor	xmm1, xmm1, xmm1
	vmovd	dword ptr [r8 + 4*rdx], xmm1
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovdqa	xmm1, xmm0
	je	.LBB4_1141
.LBB4_1138:                             #   in Loop: Header=BB4_1136 Depth=1
	vmovd	dword ptr [r8 + 4*rdx + 4], xmm1
	cmp	byte ptr [rcx + rdx + 2], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_1139
.LBB4_1142:                             #   in Loop: Header=BB4_1136 Depth=1
	vpxor	xmm1, xmm1, xmm1
	vmovd	dword ptr [r8 + 4*rdx + 8], xmm1
	cmp	byte ptr [rcx + rdx + 3], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_1135
	jmp	.LBB4_1143
.LBB4_1137:                             #   in Loop: Header=BB4_1136 Depth=1
	vmovd	dword ptr [r8 + 4*rdx], xmm1
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_1138
.LBB4_1141:                             #   in Loop: Header=BB4_1136 Depth=1
	vpxor	xmm1, xmm1, xmm1
	vmovd	dword ptr [r8 + 4*rdx + 4], xmm1
	cmp	byte ptr [rcx + rdx + 2], 0
	vmovdqa	xmm1, xmm0
	je	.LBB4_1142
.LBB4_1139:                             #   in Loop: Header=BB4_1136 Depth=1
	vmovd	dword ptr [r8 + 4*rdx + 8], xmm1
	cmp	byte ptr [rcx + rdx + 3], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_1135
.LBB4_1143:                             #   in Loop: Header=BB4_1136 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_1135
.LBB4_450:
	mov	esi, eax
	and	esi, -4
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB4_1288
# %bb.451:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI4_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI4_1] # ymm1 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vxorpd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm3, qword ptr [rip + .LCPI4_7] # ymm3 = [2.147483648E+9,2.147483648E+9,2.147483648E+9,2.147483648E+9]
	vbroadcastss	xmm4, dword ptr [rip + .LCPI4_4] # xmm4 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB4_452:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm5, ymmword ptr [rcx + 8*rdi]
	vcmpeqpd	ymm6, ymm8, ymm5
	vandpd	ymm5, ymm5, ymm0
	vorpd	ymm5, ymm1, ymm5
	vcmpltpd	ymm7, ymm5, ymm3
	vextractf128	xmm2, ymm7, 1
	vpackssdw	xmm2, xmm7, xmm2
	vsubpd	ymm7, ymm5, ymm3
	vcvttpd2dq	xmm7, ymm7
	vcvttpd2dq	xmm5, ymm5
	vxorpd	xmm7, xmm7, xmm4
	vblendvps	xmm2, xmm7, xmm5, xmm2
	vextractf128	xmm5, ymm6, 1
	vpackssdw	xmm5, xmm6, xmm5
	vpandn	xmm2, xmm5, xmm2
	vmovdqu	xmmword ptr [r8 + 4*rdi], xmm2
	vmovupd	ymm2, ymmword ptr [rcx + 8*rdi + 32]
	vcmpeqpd	ymm5, ymm8, ymm2
	vextractf128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vandpd	ymm2, ymm2, ymm0
	vorpd	ymm2, ymm1, ymm2
	vcmpltpd	ymm6, ymm2, ymm3
	vextractf128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vsubpd	ymm7, ymm2, ymm3
	vcvttpd2dq	xmm7, ymm7
	vxorpd	xmm7, xmm7, xmm4
	vcvttpd2dq	xmm2, ymm2
	vblendvps	xmm2, xmm7, xmm2, xmm6
	vpandn	xmm2, xmm5, xmm2
	vmovdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
	add	rdi, 8
	add	rdx, 2
	jne	.LBB4_452
	jmp	.LBB4_1289
.LBB4_456:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_457:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpand	xmm4, xmm4, xmm2
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpand	xmm5, xmm5, xmm2
	vpcmpeqq	ymm6, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpand	xmm6, xmm6, xmm2
	vmovdqu	xmmword ptr [r8 + 4*rsi], xmm3
	vmovdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 4*rsi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_457
# %bb.458:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_459:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_459
	jmp	.LBB4_1351
.LBB4_460:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_461:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqw	xmm3, xmm0, xmmword ptr [rcx + 2*rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxwd	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	vpcmpeqw	xmm4, xmm0, xmmword ptr [rcx + 2*rsi + 16]
	vpand	ymm3, ymm3, ymm2
	vpxor	xmm4, xmm4, xmm1
	vpmovzxwd	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqw	xmm5, xmm0, xmmword ptr [rcx + 2*rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxwd	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqw	xmm6, xmm0, xmmword ptr [rcx + 2*rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxwd	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_461
# %bb.462:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_463:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_463
	jmp	.LBB4_1351
.LBB4_464:
	mov	edx, r10d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_465:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + 2*rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + 2*rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + 2*rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + 2*rsi + 48]
	vpcmpgtw	xmm7, xmm3, xmm0
	vpmovsxwd	ymm9, xmm7
	vpcmpgtw	xmm1, xmm4, xmm0
	vpmovsxwd	ymm10, xmm1
	vpcmpgtw	xmm7, xmm5, xmm0
	vpmovsxwd	ymm7, xmm7
	vpcmpgtw	xmm1, xmm6, xmm0
	vpmovsxwd	ymm1, xmm1
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxwd	ymm3, xmm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxwd	ymm4, xmm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxwd	ymm5, xmm5
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxwd	ymm6, xmm6
	vblendvps	ymm3, ymm3, ymm2, ymm9
	vblendvps	ymm4, ymm4, ymm2, ymm10
	vblendvps	ymm5, ymm5, ymm2, ymm7
	vblendvps	ymm1, ymm6, ymm2, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_465
# %bb.466:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_467:
	mov	esi, 1
.LBB4_468:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_468
	jmp	.LBB4_1351
.LBB4_469:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm8, ymm8, ymm8
	vbroadcastss	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_470:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm3, ymm4, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm9, xmm3, xmm1
	vpcmpgtq	ymm1, ymm5, ymm0
	vextracti128	xmm3, ymm1, 1
	vpackssdw	xmm10, xmm1, xmm3
	vpcmpgtq	ymm3, ymm6, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm11, xmm3, xmm1
	vpcmpgtq	ymm3, ymm7, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm1, xmm3, xmm1
	vpcmpeqq	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm8, ymm3
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpcmpeqq	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpcmpeqq	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm8, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpcmpeqq	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm8, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vblendvps	xmm3, xmm3, xmm2, xmm9
	vblendvps	xmm4, xmm4, xmm2, xmm10
	vblendvps	xmm5, xmm5, xmm2, xmm11
	vblendvps	xmm1, xmm6, xmm2, xmm1
	vmovups	xmmword ptr [r8 + 4*rsi], xmm3
	vmovups	xmmword ptr [r8 + 4*rsi + 16], xmm4
	vmovups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	vmovups	xmmword ptr [r8 + 4*rsi + 48], xmm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_470
# %bb.471:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_472:
	mov	esi, 1
.LBB4_473:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_473
	jmp	.LBB4_1351
.LBB4_474:
	mov	edx, eax
	and	edx, -8
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1294
# %bb.475:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vpbroadcastd	ymm0, dword ptr [rip + .LCPI4_8] # ymm0 = [1,1,1,1,1,1,1,1]
	vxorps	xmm1, xmm1, xmm1
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_10] # ymm2 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	vbroadcastss	ymm3, dword ptr [rip + .LCPI4_4] # ymm3 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
.LBB4_476:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi]
	vpsrad	ymm5, ymm4, 31
	vpor	ymm5, ymm5, ymm0
	vcvtdq2ps	ymm5, ymm5
	vcmpltps	ymm6, ymm5, ymm2
	vsubps	ymm7, ymm5, ymm2
	vcvttps2dq	ymm7, ymm7
	vxorps	ymm7, ymm7, ymm3
	vcvttps2dq	ymm5, ymm5
	vblendvps	ymm5, ymm7, ymm5, ymm6
	vcmpneqps	ymm4, ymm4, ymm1
	vandps	ymm4, ymm4, ymm5
	vmovups	ymmword ptr [r8 + 4*rsi], ymm4
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 32]
	vpsrad	ymm5, ymm4, 31
	vpor	ymm5, ymm5, ymm0
	vcvtdq2ps	ymm5, ymm5
	vcmpltps	ymm6, ymm5, ymm2
	vsubps	ymm7, ymm5, ymm2
	vcvttps2dq	ymm7, ymm7
	vxorps	ymm7, ymm7, ymm3
	vcvttps2dq	ymm5, ymm5
	vblendvps	ymm5, ymm7, ymm5, ymm6
	vcmpneqps	ymm4, ymm4, ymm1
	vandps	ymm4, ymm4, ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	add	rsi, 16
	add	rdi, 2
	jne	.LBB4_476
	jmp	.LBB4_1295
.LBB4_483:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastd	xmm1, dword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_484:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	xmm2, xmm0, xmmword ptr [rcx + 4*rsi]
	vpandn	xmm2, xmm2, xmm1
	vcvtdq2pd	ymm2, xmm2
	vpcmpeqd	xmm3, xmm0, xmmword ptr [rcx + 4*rsi + 16]
	vpandn	xmm3, xmm3, xmm1
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqd	xmm4, xmm0, xmmword ptr [rcx + 4*rsi + 32]
	vpandn	xmm4, xmm4, xmm1
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqd	xmm5, xmm0, xmmword ptr [rcx + 4*rsi + 48]
	vpandn	xmm5, xmm5, xmm1
	vcvtdq2pd	ymm5, xmm5
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm5
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_484
# %bb.485:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_486:
	vmovq	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = mem[0],zero
	jmp	.LBB4_488
.LBB4_487:                              #   in Loop: Header=BB4_488 Depth=1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_488:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_487
# %bb.489:                              #   in Loop: Header=BB4_488 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_487
.LBB4_496:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_497:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpand	xmm3, xmm3, xmm2
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpand	xmm4, xmm4, xmm2
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpand	xmm5, xmm5, xmm2
	vcvtdq2pd	ymm5, xmm5
	vpcmpeqq	ymm6, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpand	xmm6, xmm6, xmm2
	vcvtdq2pd	ymm6, xmm6
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_497
# %bb.498:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_499:
	vmovq	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = mem[0],zero
	jmp	.LBB4_501
.LBB4_500:                              #   in Loop: Header=BB4_501 Depth=1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_501:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_500
# %bb.502:                              #   in Loop: Header=BB4_501 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_500
.LBB4_503:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_504:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxwd	xmm3, xmm3              # xmm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	vpand	xmm3, xmm3, xmm2
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxwd	xmm4, xmm4              # xmm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	vpand	xmm4, xmm4, xmm2
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxwd	xmm5, xmm5              # xmm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero
	vpand	xmm5, xmm5, xmm2
	vcvtdq2pd	ymm5, xmm5
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxwd	xmm6, xmm6              # xmm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero
	vpand	xmm6, xmm6, xmm2
	vcvtdq2pd	ymm6, xmm6
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_504
# %bb.505:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_506:
	vmovq	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = mem[0],zero
	jmp	.LBB4_508
.LBB4_507:                              #   in Loop: Header=BB4_508 Depth=1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_508:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_507
# %bb.509:                              #   in Loop: Header=BB4_508 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_507
.LBB4_510:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_511:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	vpcmpgtw	xmm7, xmm3, xmm0
	vpmovsxwq	ymm9, xmm7
	vpcmpgtw	xmm1, xmm4, xmm0
	vpmovsxwq	ymm10, xmm1
	vpcmpgtw	xmm7, xmm5, xmm0
	vpmovsxwq	ymm7, xmm7
	vpcmpgtw	xmm1, xmm6, xmm0
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxwd	xmm3, xmm3
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxwd	xmm4, xmm4
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxwd	xmm5, xmm5
	vcvtdq2pd	ymm5, xmm5
	vpmovsxwq	ymm1, xmm1
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxwd	xmm6, xmm6
	vcvtdq2pd	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_511
# %bb.512:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_513:
	vmovsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	vmovsd	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = mem[0],zero
	jmp	.LBB4_515
.LBB4_514:                              #   in Loop: Header=BB4_515 Depth=1
	vmovsd	qword ptr [r8 + 8*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_515:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	vmovapd	xmm2, xmm0
	jne	.LBB4_517
# %bb.516:                              #   in Loop: Header=BB4_515 Depth=1
	vxorpd	xmm2, xmm2, xmm2
.LBB4_517:                              #   in Loop: Header=BB4_515 Depth=1
	vmovapd	xmm3, xmm1
	jg	.LBB4_514
# %bb.518:                              #   in Loop: Header=BB4_515 Depth=1
	vmovapd	xmm3, xmm2
	jmp	.LBB4_514
.LBB4_519:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm11, ymm11, ymm11
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_520:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm7, ymm3, ymm0
	vpcmpgtq	ymm8, ymm4, ymm0
	vpcmpgtq	ymm9, ymm5, ymm0
	vpcmpgtq	ymm10, ymm6, ymm0
	vpcmpeqq	ymm3, ymm3, ymm0
	vpxor	ymm3, ymm11, ymm3
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm1, xmm3, xmm1
	vcvtdq2pd	ymm1, xmm1
	vpcmpeqq	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm11, ymm3
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqq	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm11, ymm4
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqq	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm11, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vcvtdq2pd	ymm5, xmm5
	vblendvpd	ymm1, ymm1, ymm2, ymm7
	vblendvpd	ymm3, ymm3, ymm2, ymm8
	vblendvpd	ymm4, ymm4, ymm2, ymm9
	vblendvpd	ymm5, ymm5, ymm2, ymm10
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm5
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_520
# %bb.521:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_522:
	vmovsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	vmovsd	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = mem[0],zero
	jmp	.LBB4_524
.LBB4_523:                              #   in Loop: Header=BB4_524 Depth=1
	vmovsd	qword ptr [r8 + 8*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_524:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	vmovapd	xmm2, xmm0
	jne	.LBB4_526
# %bb.525:                              #   in Loop: Header=BB4_524 Depth=1
	vxorpd	xmm2, xmm2, xmm2
.LBB4_526:                              #   in Loop: Header=BB4_524 Depth=1
	vmovapd	xmm3, xmm1
	jg	.LBB4_523
# %bb.527:                              #   in Loop: Header=BB4_524 Depth=1
	vmovapd	xmm3, xmm2
	jmp	.LBB4_523
.LBB4_528:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vxorps	xmm8, xmm8, xmm8
	vpbroadcastd	xmm1, dword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_529:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm2, xmmword ptr [rcx + 4*rsi]
	vmovups	xmm3, xmmword ptr [rcx + 4*rsi + 16]
	vmovups	xmm4, xmmword ptr [rcx + 4*rsi + 32]
	vmovups	xmm5, xmmword ptr [rcx + 4*rsi + 48]
	vcmpeqps	xmm6, xmm8, xmm2
	vpmovsxdq	ymm6, xmm6
	vcmpeqps	xmm7, xmm8, xmm3
	vpmovsxdq	ymm7, xmm7
	vcmpeqps	xmm0, xmm8, xmm4
	vpmovsxdq	ymm9, xmm0
	vcmpeqps	xmm0, xmm8, xmm5
	vpmovsxdq	ymm0, xmm0
	vpsrad	xmm2, xmm2, 31
	vpor	xmm2, xmm2, xmm1
	vpsrad	xmm3, xmm3, 31
	vpor	xmm3, xmm3, xmm1
	vpsrad	xmm4, xmm4, 31
	vpor	xmm4, xmm4, xmm1
	vpsrad	xmm5, xmm5, 31
	vpor	xmm5, xmm5, xmm1
	vcvtdq2ps	xmm2, xmm2
	vcvtdq2ps	xmm3, xmm3
	vcvtdq2ps	xmm4, xmm4
	vcvtdq2ps	xmm5, xmm5
	vcvtps2pd	ymm2, xmm2
	vpandn	ymm2, ymm6, ymm2
	vcvtps2pd	ymm3, xmm3
	vpandn	ymm3, ymm7, ymm3
	vcvtps2pd	ymm4, xmm4
	vcvtps2pd	ymm5, xmm5
	vpandn	ymm4, ymm9, ymm4
	vpandn	ymm0, ymm0, ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm0
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_529
# %bb.530:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_531:
	vpxor	xmm0, xmm0, xmm0
	jmp	.LBB4_533
.LBB4_532:                              #   in Loop: Header=BB4_533 Depth=1
	vmovq	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_533:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm2, dword ptr [rcx + 4*rdx]   # xmm2 = mem[0],zero,zero,zero
	vpxor	xmm1, xmm1, xmm1
	vucomiss	xmm0, xmm2
	je	.LBB4_532
# %bb.534:                              #   in Loop: Header=BB4_533 Depth=1
	vmovmskps	esi, xmm2
	and	esi, 1
	neg	esi
	or	esi, 1
	vcvtsi2ss	xmm1, xmm10, esi
	vcvtss2sd	xmm1, xmm1, xmm1
	jmp	.LBB4_532
.LBB4_538:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_539:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	vpcmpgtd	xmm7, xmm3, xmm0
	vpmovsxdq	ymm9, xmm7
	vpcmpgtd	xmm1, xmm4, xmm0
	vpmovsxdq	ymm10, xmm1
	vpcmpgtd	xmm7, xmm5, xmm0
	vpmovsxdq	ymm7, xmm7
	vpcmpgtd	xmm1, xmm6, xmm0
	vpmovsxdq	ymm1, xmm1
	vpcmpeqd	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqd	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqd	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vcvtdq2pd	ymm5, xmm5
	vpcmpeqd	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vcvtdq2pd	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_539
# %bb.540:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_541:
	vmovsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	vmovsd	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = mem[0],zero
	jmp	.LBB4_543
.LBB4_542:                              #   in Loop: Header=BB4_543 Depth=1
	vmovsd	qword ptr [r8 + 8*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_543:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	vmovapd	xmm2, xmm0
	jne	.LBB4_545
# %bb.544:                              #   in Loop: Header=BB4_543 Depth=1
	vxorpd	xmm2, xmm2, xmm2
.LBB4_545:                              #   in Loop: Header=BB4_543 Depth=1
	vmovapd	xmm3, xmm1
	jg	.LBB4_542
# %bb.546:                              #   in Loop: Header=BB4_543 Depth=1
	vmovapd	xmm3, xmm2
	jmp	.LBB4_542
.LBB4_577:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_578:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	xmm3, xmm0, xmmword ptr [rcx + 4*rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxdq	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	vpcmpeqd	xmm4, xmm0, xmmword ptr [rcx + 4*rsi + 16]
	vpand	ymm3, ymm3, ymm2
	vpxor	xmm4, xmm4, xmm1
	vpmovzxdq	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqd	xmm5, xmm0, xmmword ptr [rcx + 4*rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxdq	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqd	xmm6, xmm0, xmmword ptr [rcx + 4*rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxdq	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_578
# %bb.579:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_580:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_580
	jmp	.LBB4_1351
.LBB4_581:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB4_1282
# %bb.582:
	mov	r14, r9
	and	r14, -2
	neg	r14
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI4_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vmovsd	xmm3, qword ptr [rip + .LCPI4_6] # xmm3 = mem[0],zero
.LBB4_583:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm4, ymmword ptr [rcx + 8*rdi]
	vandpd	ymm5, ymm4, ymm0
	vorpd	ymm5, ymm2, ymm5
	vextractf128	xmm6, ymm5, 1
	vsubsd	xmm7, xmm6, xmm3
	vcvttsd2si	rbx, xmm7
	xor	rbx, r11
	vcvttsd2si	rdx, xmm6
	vucomisd	xmm6, xmm3
	cmovae	rdx, rbx
	vpermilps	xmm6, xmm6, 78          # xmm6 = xmm6[2,3,0,1]
	vsubsd	xmm7, xmm6, xmm3
	vcvttsd2si	rbx, xmm7
	xor	rbx, r11
	vcvttsd2si	rax, xmm6
	vucomisd	xmm6, xmm3
	vmovq	xmm6, rdx
	cmovae	rax, rbx
	vmovq	xmm7, rax
	vsubsd	xmm1, xmm5, xmm3
	vcvttsd2si	rax, xmm1
	xor	rax, r11
	vcvttsd2si	rdx, xmm5
	vucomisd	xmm5, xmm3
	cmovae	rdx, rax
	vpermilps	xmm1, xmm5, 78          # xmm1 = xmm5[2,3,0,1]
	vsubsd	xmm5, xmm1, xmm3
	vcvttsd2si	rax, xmm5
	vmovq	xmm5, rdx
	xor	rax, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm3
	vpunpcklqdq	xmm1, xmm6, xmm7        # xmm1 = xmm6[0],xmm7[0]
	cmovae	rdx, rax
	vmovq	xmm6, rdx
	vpunpcklqdq	xmm5, xmm5, xmm6        # xmm5 = xmm5[0],xmm6[0]
	vinserti128	ymm1, ymm5, xmm1, 1
	vcmpneqpd	ymm4, ymm8, ymm4
	vandpd	ymm1, ymm4, ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm1
	vmovupd	ymm4, ymmword ptr [rcx + 8*rdi + 32]
	vandpd	ymm1, ymm4, ymm0
	vorpd	ymm1, ymm2, ymm1
	vextractf128	xmm5, ymm1, 1
	vsubsd	xmm6, xmm5, xmm3
	vcvttsd2si	rax, xmm6
	xor	rax, r11
	vcvttsd2si	rdx, xmm5
	vucomisd	xmm5, xmm3
	cmovae	rdx, rax
	vpermilps	xmm5, xmm5, 78          # xmm5 = xmm5[2,3,0,1]
	vsubsd	xmm6, xmm5, xmm3
	vcvttsd2si	rax, xmm6
	vmovq	xmm6, rdx
	xor	rax, r11
	vcvttsd2si	rdx, xmm5
	vucomisd	xmm5, xmm3
	cmovae	rdx, rax
	vmovq	xmm5, rdx
	vsubsd	xmm7, xmm1, xmm3
	vcvttsd2si	rax, xmm7
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	xor	rax, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm3
	cmovae	rdx, rax
	vpermilps	xmm1, xmm1, 78          # xmm1 = xmm1[2,3,0,1]
	vsubsd	xmm6, xmm1, xmm3
	vcvttsd2si	rax, xmm6
	vmovq	xmm6, rdx
	xor	rax, r11
	vcvttsd2si	rdx, xmm1
	vucomisd	xmm1, xmm3
	cmovae	rdx, rax
	vmovq	xmm1, rdx
	vpunpcklqdq	xmm1, xmm6, xmm1        # xmm1 = xmm6[0],xmm1[0]
	vinserti128	ymm1, ymm1, xmm5, 1
	vcmpneqpd	ymm4, ymm8, ymm4
	vandpd	ymm1, ymm4, ymm1
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm1
	add	rdi, 8
	add	r14, 2
	jne	.LBB4_583
	jmp	.LBB4_1283
.LBB4_590:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_591:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxwq	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxwq	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero,xmm5[2],zero,zero,zero,xmm5[3],zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxwq	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero,xmm6[2],zero,zero,zero,xmm6[3],zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_591
# %bb.592:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_593:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_593
	jmp	.LBB4_1351
.LBB4_594:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_595:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	vpcmpgtw	xmm7, xmm3, xmm0
	vpmovsxwq	ymm9, xmm7
	vpcmpgtw	xmm1, xmm4, xmm0
	vpmovsxwq	ymm10, xmm1
	vpcmpgtw	xmm7, xmm5, xmm0
	vpmovsxwq	ymm7, xmm7
	vpcmpgtw	xmm1, xmm6, xmm0
	vpmovsxwq	ymm1, xmm1
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxwq	ymm3, xmm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxwq	ymm4, xmm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxwq	ymm5, xmm5
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxwq	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_595
# %bb.596:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_597:
	mov	esi, 1
.LBB4_598:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_598
	jmp	.LBB4_1351
.LBB4_602:
	mov	esi, r10d
	and	esi, -2
	xor	eax, eax
	vxorps	xmm0, xmm0, xmm0
	vmovss	xmm1, dword ptr [rip + .LCPI4_9] # xmm1 = mem[0],zero,zero,zero
	movabs	r9, -9223372036854775808
	jmp	.LBB4_605
.LBB4_603:                              #   in Loop: Header=BB4_605 Depth=1
	vmovmskps	edx, xmm2
	and	edx, 1
	neg	edx
	or	edx, 1
	vcvtsi2ss	xmm2, xmm4, edx
	vsubss	xmm3, xmm2, xmm1
	vcvttss2si	rdi, xmm3
	xor	rdi, r9
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm1
	cmovae	rdx, rdi
	mov	qword ptr [r8 + 8*rax + 8], rdx
	add	rax, 2
	cmp	rsi, rax
	je	.LBB4_254
.LBB4_605:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm2, dword ptr [rcx + 4*rax]   # xmm2 = mem[0],zero,zero,zero
	vucomiss	xmm0, xmm2
	jne	.LBB4_607
# %bb.606:                              #   in Loop: Header=BB4_605 Depth=1
	xor	edx, edx
	jmp	.LBB4_608
.LBB4_607:                              #   in Loop: Header=BB4_605 Depth=1
	vmovmskps	edx, xmm2
	and	edx, 1
	neg	edx
	or	edx, 1
	vcvtsi2ss	xmm2, xmm4, edx
	vsubss	xmm3, xmm2, xmm1
	vcvttss2si	rdi, xmm3
	xor	rdi, r9
	vcvttss2si	rdx, xmm2
	vucomiss	xmm2, xmm1
	cmovae	rdx, rdi
.LBB4_608:                              #   in Loop: Header=BB4_605 Depth=1
	mov	qword ptr [r8 + 8*rax], rdx
	vmovss	xmm2, dword ptr [rcx + 4*rax + 4] # xmm2 = mem[0],zero,zero,zero
	vucomiss	xmm0, xmm2
	jne	.LBB4_603
# %bb.609:                              #   in Loop: Header=BB4_605 Depth=1
	xor	edx, edx
	mov	qword ptr [r8 + 8*rax + 8], rdx
	add	rax, 2
	cmp	rsi, rax
	jne	.LBB4_605
.LBB4_254:
	test	r10b, 1
	je	.LBB4_1351
# %bb.255:
	vmovss	xmm0, dword ptr [rcx + 4*rax]   # xmm0 = mem[0],zero,zero,zero
	vxorps	xmm1, xmm1, xmm1
	vucomiss	xmm1, xmm0
	jne	.LBB4_1280
# %bb.256:
	xor	ecx, ecx
	jmp	.LBB4_1281
.LBB4_613:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_614:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	vpcmpgtd	xmm7, xmm3, xmm0
	vpmovsxdq	ymm9, xmm7
	vpcmpgtd	xmm1, xmm4, xmm0
	vpmovsxdq	ymm10, xmm1
	vpcmpgtd	xmm7, xmm5, xmm0
	vpmovsxdq	ymm7, xmm7
	vpcmpgtd	xmm1, xmm6, xmm0
	vpmovsxdq	ymm1, xmm1
	vpcmpeqd	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxdq	ymm3, xmm3
	vpcmpeqd	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxdq	ymm4, xmm4
	vpcmpeqd	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxdq	ymm5, xmm5
	vpcmpeqd	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxdq	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_614
# %bb.615:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_616:
	mov	esi, 1
.LBB4_617:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_617
	jmp	.LBB4_1351
.LBB4_618:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
.LBB4_619:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm2, ymm0, ymmword ptr [rcx + 4*rsi]
	vpxor	ymm2, ymm2, ymm1
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpsrlw	xmm2, xmm2, 15
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpsrlw	xmm3, xmm3, 15
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpsrlw	xmm4, xmm4, 15
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpsrlw	xmm5, xmm5, 15
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm4
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_619
# %bb.620:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_621:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_621
	jmp	.LBB4_1351
.LBB4_622:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
.LBB4_623:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm2, ymm0, ymmword ptr [rcx + 4*rsi]
	vpxor	ymm2, ymm2, ymm1
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpsrlw	xmm2, xmm2, 15
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpsrlw	xmm3, xmm3, 15
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpsrlw	xmm4, xmm4, 15
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpsrlw	xmm5, xmm5, 15
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm3
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm4
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_623
# %bb.624:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_625:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_625
	jmp	.LBB4_1351
.LBB4_626:
	mov	esi, eax
	and	esi, -16
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI4_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	xmm9, xmm9, xmm9
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vpxor	xmm10, xmm10, xmm10
.LBB4_627:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm4, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm5, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm6, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm7, ymmword ptr [rcx + 8*rdi + 96]
	vcmpeqpd	ymm8, ymm9, ymm4
	vextractf128	xmm1, ymm8, 1
	vpackssdw	xmm1, xmm8, xmm1
	vpackssdw	xmm11, xmm1, xmm1
	vcmpeqpd	ymm8, ymm9, ymm5
	vextractf128	xmm3, ymm8, 1
	vpackssdw	xmm3, xmm8, xmm3
	vpackssdw	xmm12, xmm3, xmm3
	vcmpeqpd	ymm8, ymm9, ymm6
	vextractf128	xmm1, ymm8, 1
	vpackssdw	xmm1, xmm8, xmm1
	vpackssdw	xmm1, xmm1, xmm1
	vcmpeqpd	ymm8, ymm9, ymm7
	vextractf128	xmm3, ymm8, 1
	vpackssdw	xmm3, xmm8, xmm3
	vpackssdw	xmm3, xmm3, xmm3
	vandpd	ymm4, ymm4, ymm0
	vorpd	ymm4, ymm2, ymm4
	vandpd	ymm5, ymm5, ymm0
	vorpd	ymm5, ymm2, ymm5
	vandpd	ymm6, ymm6, ymm0
	vorpd	ymm6, ymm2, ymm6
	vandpd	ymm7, ymm7, ymm0
	vorpd	ymm7, ymm2, ymm7
	vcvttpd2dq	xmm4, ymm4
	vcvttpd2dq	xmm5, ymm5
	vpackusdw	xmm4, xmm4, xmm4
	vpackusdw	xmm5, xmm5, xmm5
	vcvttpd2dq	xmm6, ymm6
	vpackusdw	xmm6, xmm6, xmm6
	vcvttpd2dq	xmm7, ymm7
	vpackusdw	xmm7, xmm7, xmm7
	vpblendvb	xmm4, xmm4, xmm10, xmm11
	vpblendvb	xmm5, xmm5, xmm10, xmm12
	vpblendvb	xmm1, xmm6, xmm10, xmm1
	vpblendvb	xmm3, xmm7, xmm10, xmm3
	vinserti128	ymm1, ymm1, xmm3, 1
	vinserti128	ymm3, ymm4, xmm5, 1
	vpunpcklqdq	ymm1, ymm3, ymm1        # ymm1 = ymm3[0],ymm1[0],ymm3[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB4_627
# %bb.628:
	cmp	rsi, rax
	je	.LBB4_1351
.LBB4_629:
	vxorpd	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_630:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, edx
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_630
	jmp	.LBB4_1351
.LBB4_631:
	mov	esi, eax
	and	esi, -16
	xor	edi, edi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI4_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	xmm9, xmm9, xmm9
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vpxor	xmm10, xmm10, xmm10
.LBB4_632:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm4, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm5, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm6, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm7, ymmword ptr [rcx + 8*rdi + 96]
	vcmpeqpd	ymm8, ymm9, ymm4
	vextractf128	xmm1, ymm8, 1
	vpackssdw	xmm1, xmm8, xmm1
	vpackssdw	xmm11, xmm1, xmm1
	vcmpeqpd	ymm8, ymm9, ymm5
	vextractf128	xmm3, ymm8, 1
	vpackssdw	xmm3, xmm8, xmm3
	vpackssdw	xmm12, xmm3, xmm3
	vcmpeqpd	ymm8, ymm9, ymm6
	vextractf128	xmm1, ymm8, 1
	vpackssdw	xmm1, xmm8, xmm1
	vpackssdw	xmm1, xmm1, xmm1
	vcmpeqpd	ymm8, ymm9, ymm7
	vextractf128	xmm3, ymm8, 1
	vpackssdw	xmm3, xmm8, xmm3
	vpackssdw	xmm3, xmm3, xmm3
	vandpd	ymm4, ymm4, ymm0
	vorpd	ymm4, ymm2, ymm4
	vandpd	ymm5, ymm5, ymm0
	vorpd	ymm5, ymm2, ymm5
	vandpd	ymm6, ymm6, ymm0
	vorpd	ymm6, ymm2, ymm6
	vandpd	ymm7, ymm7, ymm0
	vorpd	ymm7, ymm2, ymm7
	vcvttpd2dq	xmm4, ymm4
	vcvttpd2dq	xmm5, ymm5
	vpackssdw	xmm4, xmm4, xmm4
	vpackssdw	xmm5, xmm5, xmm5
	vcvttpd2dq	xmm6, ymm6
	vpackssdw	xmm6, xmm6, xmm6
	vcvttpd2dq	xmm7, ymm7
	vpackssdw	xmm7, xmm7, xmm7
	vpblendvb	xmm4, xmm4, xmm10, xmm11
	vpblendvb	xmm5, xmm5, xmm10, xmm12
	vpblendvb	xmm1, xmm6, xmm10, xmm1
	vpblendvb	xmm3, xmm7, xmm10, xmm3
	vinserti128	ymm1, ymm1, xmm3, 1
	vinserti128	ymm3, ymm4, xmm5, 1
	vpunpcklqdq	ymm1, ymm3, ymm1        # ymm1 = ymm3[0],ymm1[0],ymm3[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB4_632
# %bb.633:
	cmp	rsi, rax
	je	.LBB4_1351
.LBB4_634:
	vxorpd	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_635:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, edx
	mov	word ptr [r8 + 2*rsi], di
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_635
	jmp	.LBB4_1351
.LBB4_642:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_16] # xmm2 = <1,1,1,1,u,u,u,u>
.LBB4_643:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpackssdw	xmm3, xmm3, xmm3
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpand	xmm4, xmm4, xmm2
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpackssdw	xmm5, xmm5, xmm5
	vpand	xmm5, xmm5, xmm2
	vpcmpeqq	ymm6, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpackssdw	xmm6, xmm6, xmm6
	vpand	xmm6, xmm6, xmm2
	vinserti128	ymm5, ymm5, xmm6, 1
	vinserti128	ymm3, ymm3, xmm4, 1
	vpunpcklqdq	ymm3, ymm3, ymm5        # ymm3 = ymm3[0],ymm5[0],ymm3[2],ymm5[2]
	vpermq	ymm3, ymm3, 216                 # ymm3 = ymm3[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_643
# %bb.644:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_645:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_645
	jmp	.LBB4_1351
.LBB4_646:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_16] # xmm2 = <1,1,1,1,u,u,u,u>
.LBB4_647:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpackssdw	xmm3, xmm3, xmm3
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpand	xmm4, xmm4, xmm2
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpackssdw	xmm5, xmm5, xmm5
	vpand	xmm5, xmm5, xmm2
	vpcmpeqq	ymm6, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpackssdw	xmm6, xmm6, xmm6
	vpand	xmm6, xmm6, xmm2
	vinserti128	ymm5, ymm5, xmm6, 1
	vinserti128	ymm3, ymm3, xmm4, 1
	vpunpcklqdq	ymm3, ymm3, ymm5        # ymm3 = ymm3[0],ymm5[0],ymm3[2],ymm5[2]
	vpermq	ymm3, ymm3, 216                 # ymm3 = ymm3[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_647
# %bb.648:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_649:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	word ptr [r8 + 2*rdx], si
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_649
	jmp	.LBB4_1351
.LBB4_662:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vmovdqa	xmm10, xmmword ptr [rip + .LCPI4_16] # xmm10 = <1,1,1,1,u,u,u,u>
.LBB4_663:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm8, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm3, ymm7, ymm0
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpackssdw	xmm11, xmm3, xmm3
	vpcmpgtq	ymm5, ymm8, ymm0
	vextracti128	xmm1, ymm5, 1
	vpackssdw	xmm1, xmm5, xmm1
	vpackssdw	xmm12, xmm1, xmm1
	vpcmpgtq	ymm1, ymm6, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpcmpgtq	ymm2, ymm4, ymm0
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpackssdw	xmm2, xmm2, xmm2
	vpcmpeqq	ymm3, ymm7, ymm0
	vpxor	ymm3, ymm9, ymm3
	vextracti128	xmm7, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm7
	vpackssdw	xmm3, xmm3, xmm3
	vpcmpeqq	ymm7, ymm8, ymm0
	vpxor	ymm7, ymm9, ymm7
	vextracti128	xmm5, ymm7, 1
	vpackssdw	xmm5, xmm7, xmm5
	vpackssdw	xmm5, xmm5, xmm5
	vpcmpeqq	ymm6, ymm6, ymm0
	vpxor	ymm6, ymm9, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpackssdw	xmm6, xmm6, xmm6
	vpcmpeqq	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm7, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm7
	vpackssdw	xmm4, xmm4, xmm4
	vpblendvb	xmm3, xmm3, xmm10, xmm11
	vpblendvb	xmm5, xmm5, xmm10, xmm12
	vpblendvb	xmm1, xmm6, xmm10, xmm1
	vpblendvb	xmm2, xmm4, xmm10, xmm2
	vinserti128	ymm1, ymm1, xmm2, 1
	vinserti128	ymm2, ymm3, xmm5, 1
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_663
# %bb.664:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_665:
	mov	esi, 1
.LBB4_666:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_666
	jmp	.LBB4_1351
.LBB4_667:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vmovdqa	xmm10, xmmword ptr [rip + .LCPI4_16] # xmm10 = <1,1,1,1,u,u,u,u>
.LBB4_668:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm8, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm3, ymm7, ymm0
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpackssdw	xmm11, xmm3, xmm3
	vpcmpgtq	ymm5, ymm8, ymm0
	vextracti128	xmm1, ymm5, 1
	vpackssdw	xmm1, xmm5, xmm1
	vpackssdw	xmm12, xmm1, xmm1
	vpcmpgtq	ymm1, ymm6, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpcmpgtq	ymm2, ymm4, ymm0
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpackssdw	xmm2, xmm2, xmm2
	vpcmpeqq	ymm3, ymm7, ymm0
	vpxor	ymm3, ymm9, ymm3
	vextracti128	xmm7, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm7
	vpackssdw	xmm3, xmm3, xmm3
	vpcmpeqq	ymm7, ymm8, ymm0
	vpxor	ymm7, ymm9, ymm7
	vextracti128	xmm5, ymm7, 1
	vpackssdw	xmm5, xmm7, xmm5
	vpackssdw	xmm5, xmm5, xmm5
	vpcmpeqq	ymm6, ymm6, ymm0
	vpxor	ymm6, ymm9, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpackssdw	xmm6, xmm6, xmm6
	vpcmpeqq	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm7, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm7
	vpackssdw	xmm4, xmm4, xmm4
	vpblendvb	xmm3, xmm3, xmm10, xmm11
	vpblendvb	xmm5, xmm5, xmm10, xmm12
	vpblendvb	xmm1, xmm6, xmm10, xmm1
	vpblendvb	xmm2, xmm4, xmm10, xmm2
	vinserti128	ymm1, ymm1, xmm2, 1
	vinserti128	ymm2, ymm3, xmm5, 1
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_668
# %bb.669:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_670:
	mov	esi, 1
.LBB4_671:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_671
	jmp	.LBB4_1351
.LBB4_672:
	mov	esi, eax
	and	esi, -32
	xor	edi, edi
	vxorps	xmm9, xmm9, xmm9
	vpcmpeqd	ymm10, ymm10, ymm10
	vmovdqa	xmm11, xmmword ptr [rip + .LCPI4_11] # xmm11 = [1,1,1,1,1,1,1,1]
	vpcmpeqd	xmm12, xmm12, xmm12
.LBB4_673:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm4, ymmword ptr [rcx + 4*rdi]
	vmovups	ymm5, ymmword ptr [rcx + 4*rdi + 32]
	vmovups	ymm6, ymmword ptr [rcx + 4*rdi + 64]
	vmovups	ymm7, ymmword ptr [rcx + 4*rdi + 96]
	vcmpeqps	ymm8, ymm9, ymm4
	vextractf128	xmm0, ymm8, 1
	vpackssdw	xmm13, xmm8, xmm0
	vcmpeqps	ymm8, ymm9, ymm5
	vextractf128	xmm1, ymm8, 1
	vpackssdw	xmm1, xmm8, xmm1
	vcmpeqps	ymm8, ymm9, ymm6
	vextractf128	xmm2, ymm8, 1
	vpackssdw	xmm2, xmm8, xmm2
	vcmpeqps	ymm8, ymm9, ymm7
	vextractf128	xmm3, ymm8, 1
	vpackssdw	xmm3, xmm8, xmm3
	vpcmpgtd	ymm4, ymm4, ymm10
	vextracti128	xmm0, ymm4, 1
	vpackssdw	xmm0, xmm4, xmm0
	vpcmpgtd	ymm4, ymm5, ymm10
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpcmpgtd	ymm5, ymm6, ymm10
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpcmpgtd	ymm6, ymm7, ymm10
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpblendvb	xmm0, xmm12, xmm11, xmm0
	vpandn	xmm0, xmm13, xmm0
	vpblendvb	xmm4, xmm12, xmm11, xmm4
	vpandn	xmm1, xmm1, xmm4
	vpblendvb	xmm4, xmm12, xmm11, xmm5
	vpblendvb	xmm5, xmm12, xmm11, xmm6
	vpandn	xmm2, xmm2, xmm4
	vpandn	xmm3, xmm3, xmm5
	vmovdqu	xmmword ptr [r8 + 2*rdi], xmm0
	vmovdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rdi + 48], xmm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB4_673
# %bb.674:
	cmp	rsi, rax
	je	.LBB4_1351
.LBB4_675:
	vpxor	xmm0, xmm0, xmm0
.LBB4_676:                              # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vmovd	edi, xmm1
	xor	edx, edx
	test	edi, edi
	setns	dl
	vucomiss	xmm0, xmm1
	lea	edx, [rdx + rdx - 1]
	cmove	edx, r10d
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_676
	jmp	.LBB4_1351
.LBB4_677:
	mov	esi, eax
	and	esi, -32
	xor	edi, edi
	vxorps	xmm9, xmm9, xmm9
	vpcmpeqd	ymm10, ymm10, ymm10
	vmovdqa	xmm11, xmmword ptr [rip + .LCPI4_11] # xmm11 = [1,1,1,1,1,1,1,1]
	vpcmpeqd	xmm12, xmm12, xmm12
.LBB4_678:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm4, ymmword ptr [rcx + 4*rdi]
	vmovups	ymm5, ymmword ptr [rcx + 4*rdi + 32]
	vmovups	ymm6, ymmword ptr [rcx + 4*rdi + 64]
	vmovups	ymm7, ymmword ptr [rcx + 4*rdi + 96]
	vcmpeqps	ymm8, ymm9, ymm4
	vextractf128	xmm0, ymm8, 1
	vpackssdw	xmm13, xmm8, xmm0
	vcmpeqps	ymm8, ymm9, ymm5
	vextractf128	xmm1, ymm8, 1
	vpackssdw	xmm1, xmm8, xmm1
	vcmpeqps	ymm8, ymm9, ymm6
	vextractf128	xmm2, ymm8, 1
	vpackssdw	xmm2, xmm8, xmm2
	vcmpeqps	ymm8, ymm9, ymm7
	vextractf128	xmm3, ymm8, 1
	vpackssdw	xmm3, xmm8, xmm3
	vpcmpgtd	ymm4, ymm4, ymm10
	vextracti128	xmm0, ymm4, 1
	vpackssdw	xmm0, xmm4, xmm0
	vpcmpgtd	ymm4, ymm5, ymm10
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpcmpgtd	ymm5, ymm6, ymm10
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpcmpgtd	ymm6, ymm7, ymm10
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpblendvb	xmm0, xmm12, xmm11, xmm0
	vpandn	xmm0, xmm13, xmm0
	vpblendvb	xmm4, xmm12, xmm11, xmm4
	vpandn	xmm1, xmm1, xmm4
	vpblendvb	xmm4, xmm12, xmm11, xmm5
	vpblendvb	xmm5, xmm12, xmm11, xmm6
	vpandn	xmm2, xmm2, xmm4
	vpandn	xmm3, xmm3, xmm5
	vmovdqu	xmmword ptr [r8 + 2*rdi], xmm0
	vmovdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
	vmovdqu	xmmword ptr [r8 + 2*rdi + 32], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rdi + 48], xmm3
	add	rdi, 32
	cmp	rsi, rdi
	jne	.LBB4_678
# %bb.679:
	cmp	rsi, rax
	je	.LBB4_1351
.LBB4_680:
	vpxor	xmm0, xmm0, xmm0
.LBB4_681:                              # =>This Inner Loop Header: Depth=1
	vmovd	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vmovd	edi, xmm1
	xor	edx, edx
	test	edi, edi
	setns	dl
	vucomiss	xmm0, xmm1
	lea	edx, [rdx + rdx - 1]
	cmove	edx, r10d
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_681
	jmp	.LBB4_1351
.LBB4_688:
	mov	edx, r10d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm8, ymm8, ymm8
	vmovdqa	xmm9, xmmword ptr [rip + .LCPI4_11] # xmm9 = [1,1,1,1,1,1,1,1]
.LBB4_689:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpgtd	ymm3, ymm4, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm10, xmm3, xmm1
	vpcmpgtd	ymm1, ymm5, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm11, xmm1, xmm2
	vpcmpgtd	ymm2, ymm6, ymm0
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpcmpgtd	ymm3, ymm7, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm1, xmm3, xmm1
	vpcmpeqd	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm8, ymm3
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpcmpeqd	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpcmpeqd	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm8, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpcmpeqd	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm8, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpblendvb	xmm3, xmm3, xmm9, xmm10
	vpblendvb	xmm4, xmm4, xmm9, xmm11
	vpblendvb	xmm2, xmm5, xmm9, xmm2
	vpblendvb	xmm1, xmm6, xmm9, xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm3
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_689
# %bb.690:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_691:
	mov	esi, 1
.LBB4_692:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_692
	jmp	.LBB4_1351
.LBB4_693:
	mov	edx, r10d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm8, ymm8, ymm8
	vmovdqa	xmm9, xmmword ptr [rip + .LCPI4_11] # xmm9 = [1,1,1,1,1,1,1,1]
.LBB4_694:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpgtd	ymm3, ymm4, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm10, xmm3, xmm1
	vpcmpgtd	ymm1, ymm5, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm11, xmm1, xmm2
	vpcmpgtd	ymm2, ymm6, ymm0
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpcmpgtd	ymm3, ymm7, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm1, xmm3, xmm1
	vpcmpeqd	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm8, ymm3
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpcmpeqd	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpcmpeqd	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm8, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpcmpeqd	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm8, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpblendvb	xmm3, xmm3, xmm9, xmm10
	vpblendvb	xmm4, xmm4, xmm9, xmm11
	vpblendvb	xmm2, xmm5, xmm9, xmm2
	vpblendvb	xmm1, xmm6, xmm9, xmm1
	vmovdqu	xmmword ptr [r8 + 2*rsi], xmm3
	vmovdqu	xmmword ptr [r8 + 2*rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 2*rsi + 32], xmm2
	vmovdqu	xmmword ptr [r8 + 2*rsi + 48], xmm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_694
# %bb.695:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_696:
	mov	esi, 1
.LBB4_697:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_697
	jmp	.LBB4_1351
.LBB4_698:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_699:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	xmm3, xmm0, xmmword ptr [rcx + 4*rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxdq	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero
	vpcmpeqd	xmm4, xmm0, xmmword ptr [rcx + 4*rsi + 16]
	vpand	ymm3, ymm3, ymm2
	vpxor	xmm4, xmm4, xmm1
	vpmovzxdq	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqd	xmm5, xmm0, xmmword ptr [rcx + 4*rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxdq	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqd	xmm6, xmm0, xmmword ptr [rcx + 4*rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxdq	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_699
# %bb.700:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_701:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	dword ptr [rcx + 4*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_701
	jmp	.LBB4_1351
.LBB4_702:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI4_5] # ymm1 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_703:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm2, ymm0, ymmword ptr [rcx + 4*rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_703
# %bb.704:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_705:
	vmovd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_707
.LBB4_706:                              #   in Loop: Header=BB4_707 Depth=1
	vmovd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_707:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_706
# %bb.708:                              #   in Loop: Header=BB4_707 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_706
.LBB4_709:
	mov	edx, eax
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1302
# %bb.710:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vxorpd	xmm0, xmm0, xmm0
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI4_0] # ymm1 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_711:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm3, ymmword ptr [rcx + 8*rsi]
	vandpd	ymm4, ymm3, ymm1
	vorpd	ymm4, ymm2, ymm4
	vextractf128	xmm5, ymm4, 1
	vcvttsd2si	rbx, xmm5
	vmovq	xmm6, rbx
	vpermilps	xmm5, xmm5, 78          # xmm5 = xmm5[2,3,0,1]
	vcvttsd2si	rbx, xmm5
	vmovq	xmm5, rbx
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vcvttsd2si	rbx, xmm4
	vmovq	xmm6, rbx
	vpermilps	xmm4, xmm4, 78          # xmm4 = xmm4[2,3,0,1]
	vcvttsd2si	rbx, xmm4
	vmovq	xmm4, rbx
	vpunpcklqdq	xmm4, xmm6, xmm4        # xmm4 = xmm6[0],xmm4[0]
	vinserti128	ymm4, ymm4, xmm5, 1
	vcmpneqpd	ymm3, ymm3, ymm0
	vandpd	ymm3, ymm3, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymm3, ymmword ptr [rcx + 8*rsi + 32]
	vandpd	ymm4, ymm3, ymm1
	vorpd	ymm4, ymm2, ymm4
	vextractf128	xmm5, ymm4, 1
	vcvttsd2si	rbx, xmm5
	vmovq	xmm6, rbx
	vpermilps	xmm5, xmm5, 78          # xmm5 = xmm5[2,3,0,1]
	vcvttsd2si	rbx, xmm5
	vmovq	xmm5, rbx
	vpunpcklqdq	xmm5, xmm6, xmm5        # xmm5 = xmm6[0],xmm5[0]
	vcvttsd2si	rbx, xmm4
	vmovq	xmm6, rbx
	vpermilps	xmm4, xmm4, 78          # xmm4 = xmm4[2,3,0,1]
	vcvttsd2si	rbx, xmm4
	vmovq	xmm4, rbx
	vpunpcklqdq	xmm4, xmm6, xmm4        # xmm4 = xmm6[0],xmm4[0]
	vinserti128	ymm4, ymm4, xmm5, 1
	vcmpneqpd	ymm3, ymm3, ymm0
	vandpd	ymm3, ymm3, ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm3
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_711
	jmp	.LBB4_1303
.LBB4_712:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vbroadcastsd	ymm0, qword ptr [rip + .LCPI4_1] # ymm0 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vcvtpd2ps	xmm2, ymm0
	vxorpd	xmm8, xmm8, xmm8
	vbroadcastss	xmm1, dword ptr [rip + .LCPI4_3] # xmm1 = [NaN,NaN,NaN,NaN]
	vandpd	xmm2, xmm2, xmm1
.LBB4_713:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm3, ymmword ptr [rcx + 8*rsi]
	vmovupd	ymm4, ymmword ptr [rcx + 8*rsi + 32]
	vmovupd	ymm5, ymmword ptr [rcx + 8*rsi + 64]
	vmovupd	ymm6, ymmword ptr [rcx + 8*rsi + 96]
	vcmpeqpd	ymm7, ymm8, ymm3
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm9, xmm7, xmm0
	vcmpeqpd	ymm7, ymm8, ymm4
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm10, xmm7, xmm0
	vcmpeqpd	ymm7, ymm8, ymm5
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm11, xmm7, xmm0
	vcmpeqpd	ymm7, ymm8, ymm6
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm0, xmm7, xmm0
	vcvtpd2ps	xmm3, ymm3
	vandnpd	xmm3, xmm1, xmm3
	vorpd	xmm3, xmm2, xmm3
	vcvtpd2ps	xmm4, ymm4
	vpandn	xmm3, xmm9, xmm3
	vandnpd	xmm4, xmm1, xmm4
	vorpd	xmm4, xmm2, xmm4
	vpandn	xmm4, xmm10, xmm4
	vcvtpd2ps	xmm5, ymm5
	vandnpd	xmm5, xmm1, xmm5
	vorpd	xmm5, xmm2, xmm5
	vpandn	xmm5, xmm11, xmm5
	vcvtpd2ps	xmm6, ymm6
	vandnpd	xmm6, xmm1, xmm6
	vorpd	xmm6, xmm2, xmm6
	vpandn	xmm0, xmm0, xmm6
	vmovdqu	xmmword ptr [r8 + 4*rsi], xmm3
	vmovdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 4*rsi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + 4*rsi + 48], xmm0
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_713
# %bb.714:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_715:
	vxorpd	xmm0, xmm0, xmm0
	vpbroadcastd	xmm1, dword ptr [rip + .LCPI4_4] # xmm1 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_5] # xmm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	jmp	.LBB4_717
.LBB4_716:                              #   in Loop: Header=BB4_717 Depth=1
	vmovd	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_717:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm4, qword ptr [rcx + 8*rdx]   # xmm4 = mem[0],zero
	vucomisd	xmm0, xmm4
	vpxor	xmm3, xmm3, xmm3
	je	.LBB4_716
# %bb.718:                              #   in Loop: Header=BB4_717 Depth=1
	vcvtsd2ss	xmm3, xmm4, xmm4
	vpand	xmm3, xmm3, xmm1
	vpor	xmm3, xmm2, xmm3
	jmp	.LBB4_716
.LBB4_728:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_5] # xmm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_729:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpand	xmm4, xmm4, xmm2
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpand	xmm5, xmm5, xmm2
	vpcmpeqq	ymm6, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpand	xmm6, xmm6, xmm2
	vmovdqu	xmmword ptr [r8 + 4*rsi], xmm3
	vmovdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 4*rsi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_729
# %bb.730:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_731:
	vmovd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_733
.LBB4_732:                              #   in Loop: Header=BB4_733 Depth=1
	vmovd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_733:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_732
# %bb.734:                              #   in Loop: Header=BB4_733 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_732
.LBB4_735:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_736:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxwq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxwq	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxwq	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero,xmm5[2],zero,zero,zero,xmm5[3],zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxwq	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero,xmm6[2],zero,zero,zero,xmm6[3],zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_736
# %bb.737:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_738:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_738
	jmp	.LBB4_1351
.LBB4_739:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_740:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqw	xmm3, xmm0, xmmword ptr [rcx + 2*rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxwd	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	vpand	ymm3, ymm3, ymm2
	vcvtdq2ps	ymm3, ymm3
	vpcmpeqw	xmm4, xmm0, xmmword ptr [rcx + 2*rsi + 16]
	vpxor	xmm4, xmm4, xmm1
	vpmovzxwd	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	vpand	ymm4, ymm4, ymm2
	vcvtdq2ps	ymm4, ymm4
	vpcmpeqw	xmm5, xmm0, xmmword ptr [rcx + 2*rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxwd	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	vpand	ymm5, ymm5, ymm2
	vcvtdq2ps	ymm5, ymm5
	vpcmpeqw	xmm6, xmm0, xmmword ptr [rcx + 2*rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxwd	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	vpand	ymm6, ymm6, ymm2
	vcvtdq2ps	ymm6, ymm6
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_740
# %bb.741:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_742:
	vmovd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
	jmp	.LBB4_744
.LBB4_743:                              #   in Loop: Header=BB4_744 Depth=1
	vmovd	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_744:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	vmovdqa	xmm1, xmm0
	jne	.LBB4_743
# %bb.745:                              #   in Loop: Header=BB4_744 Depth=1
	vpxor	xmm1, xmm1, xmm1
	jmp	.LBB4_743
.LBB4_746:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_747:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + 2*rsi]   # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + 2*rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + 2*rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + 2*rsi + 24] # xmm6 = mem[0],zero
	vpcmpgtw	xmm7, xmm3, xmm0
	vpmovsxwq	ymm9, xmm7
	vpcmpgtw	xmm1, xmm4, xmm0
	vpmovsxwq	ymm10, xmm1
	vpcmpgtw	xmm7, xmm5, xmm0
	vpmovsxwq	ymm7, xmm7
	vpcmpgtw	xmm1, xmm6, xmm0
	vpmovsxwq	ymm1, xmm1
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxwq	ymm3, xmm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxwq	ymm4, xmm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxwq	ymm5, xmm5
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxwq	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_747
# %bb.748:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_749:
	mov	esi, 1
.LBB4_750:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_750
	jmp	.LBB4_1351
.LBB4_751:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_5] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_752:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + 2*rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + 2*rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + 2*rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + 2*rsi + 48]
	vpcmpgtw	xmm7, xmm3, xmm0
	vpmovsxwd	ymm9, xmm7
	vpcmpgtw	xmm1, xmm4, xmm0
	vpmovsxwd	ymm10, xmm1
	vpcmpgtw	xmm7, xmm5, xmm0
	vpmovsxwd	ymm7, xmm7
	vpcmpgtw	xmm1, xmm6, xmm0
	vpmovsxwd	ymm1, xmm1
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxwd	ymm3, xmm3
	vcvtdq2ps	ymm3, ymm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxwd	ymm4, xmm4
	vcvtdq2ps	ymm4, ymm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxwd	ymm5, xmm5
	vcvtdq2ps	ymm5, ymm5
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxwd	ymm6, xmm6
	vcvtdq2ps	ymm6, ymm6
	vblendvps	ymm3, ymm3, ymm2, ymm9
	vblendvps	ymm4, ymm4, ymm2, ymm10
	vblendvps	ymm5, ymm5, ymm2, ymm7
	vblendvps	ymm1, ymm6, ymm2, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_752
# %bb.753:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_754:
	vmovss	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	vmovss	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_756
.LBB4_755:                              #   in Loop: Header=BB4_756 Depth=1
	vmovss	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_756:                              # =>This Inner Loop Header: Depth=1
	cmp	word ptr [rcx + 2*rdx], 0
	vmovaps	xmm2, xmm0
	jne	.LBB4_758
# %bb.757:                              #   in Loop: Header=BB4_756 Depth=1
	vxorps	xmm2, xmm2, xmm2
.LBB4_758:                              #   in Loop: Header=BB4_756 Depth=1
	vmovaps	xmm3, xmm1
	jg	.LBB4_755
# %bb.759:                              #   in Loop: Header=BB4_756 Depth=1
	vmovaps	xmm3, xmm2
	jmp	.LBB4_755
.LBB4_763:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vbroadcastss	xmm2, dword ptr [rip + .LCPI4_5] # xmm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_764:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm8, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm3, ymm6, ymm0
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm10, xmm3, xmm5
	vpcmpgtq	ymm5, ymm7, ymm0
	vextracti128	xmm1, ymm5, 1
	vpackssdw	xmm11, xmm5, xmm1
	vpcmpgtq	ymm1, ymm8, ymm0
	vextracti128	xmm3, ymm1, 1
	vpackssdw	xmm12, xmm1, xmm3
	vpcmpgtq	ymm3, ymm4, ymm0
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpcmpeqq	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm9, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vcvtdq2ps	xmm5, xmm5
	vpcmpeqq	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm9, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vcvtdq2ps	xmm6, xmm6
	vpcmpeqq	ymm7, ymm8, ymm0
	vpxor	ymm7, ymm9, ymm7
	vextracti128	xmm1, ymm7, 1
	vpackssdw	xmm1, xmm7, xmm1
	vcvtdq2ps	xmm1, xmm1
	vpcmpeqq	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm7, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm7
	vcvtdq2ps	xmm4, xmm4
	vblendvps	xmm5, xmm5, xmm2, xmm10
	vblendvps	xmm6, xmm6, xmm2, xmm11
	vblendvps	xmm1, xmm1, xmm2, xmm12
	vblendvps	xmm3, xmm4, xmm2, xmm3
	vmovups	xmmword ptr [r8 + 4*rsi], xmm5
	vmovups	xmmword ptr [r8 + 4*rsi + 16], xmm6
	vmovups	xmmword ptr [r8 + 4*rsi + 32], xmm1
	vmovups	xmmword ptr [r8 + 4*rsi + 48], xmm3
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_764
# %bb.765:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_766:
	vmovss	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	vmovss	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_768
.LBB4_767:                              #   in Loop: Header=BB4_768 Depth=1
	vmovss	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_768:                              # =>This Inner Loop Header: Depth=1
	cmp	qword ptr [rcx + 8*rdx], 0
	vmovaps	xmm2, xmm0
	jne	.LBB4_770
# %bb.769:                              #   in Loop: Header=BB4_768 Depth=1
	vxorps	xmm2, xmm2, xmm2
.LBB4_770:                              #   in Loop: Header=BB4_768 Depth=1
	vmovaps	xmm3, xmm1
	jg	.LBB4_767
# %bb.771:                              #   in Loop: Header=BB4_768 Depth=1
	vmovaps	xmm3, xmm2
	jmp	.LBB4_767
.LBB4_772:
	mov	edx, r10d
	and	edx, -4
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1308
# %bb.773:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vxorps	xmm0, xmm0, xmm0
	vpbroadcastd	xmm1, dword ptr [rip + .LCPI4_8] # xmm1 = [1,1,1,1]
.LBB4_774:                              # =>This Inner Loop Header: Depth=1
	vmovups	xmm2, xmmword ptr [rcx + 4*rsi]
	vcmpeqps	xmm3, xmm2, xmm0
	vpmovsxdq	ymm3, xmm3
	vpsrad	xmm2, xmm2, 31
	vpor	xmm2, xmm2, xmm1
	vcvtdq2ps	xmm2, xmm2
	vpermilps	xmm4, xmm2, 231         # xmm4 = xmm2[3,1,2,3]
	vcvttss2si	rax, xmm4
	vmovq	xmm4, rax
	vpermilpd	xmm5, xmm2, 1           # xmm5 = xmm2[1,0]
	vcvttss2si	rax, xmm5
	vmovq	xmm5, rax
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vcvttss2si	rax, xmm2
	vmovq	xmm5, rax
	vmovshdup	xmm2, xmm2              # xmm2 = xmm2[1,1,3,3]
	vcvttss2si	rax, xmm2
	vmovq	xmm2, rax
	vpunpcklqdq	xmm2, xmm5, xmm2        # xmm2 = xmm5[0],xmm2[0]
	vinserti128	ymm2, ymm2, xmm4, 1
	vpandn	ymm2, ymm3, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm2
	vmovdqu	xmm2, xmmword ptr [rcx + 4*rsi + 16]
	vpsrad	xmm3, xmm2, 31
	vpor	xmm3, xmm3, xmm1
	vcvtdq2ps	xmm3, xmm3
	vpermilps	xmm4, xmm3, 231         # xmm4 = xmm3[3,1,2,3]
	vcvttss2si	rax, xmm4
	vpermilpd	xmm4, xmm3, 1           # xmm4 = xmm3[1,0]
	vcvttss2si	r11, xmm4
	vcvttss2si	rbx, xmm3
	vmovq	xmm4, rax
	vmovshdup	xmm3, xmm3              # xmm3 = xmm3[1,1,3,3]
	vcvttss2si	rax, xmm3
	vmovq	xmm3, r11
	vmovq	xmm5, rbx
	vcmpeqps	xmm2, xmm2, xmm0
	vpmovsxdq	ymm2, xmm2
	vpunpcklqdq	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0]
	vmovq	xmm4, rax
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vinserti128	ymm3, ymm4, xmm3, 1
	vpandn	ymm2, ymm2, ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm2
	add	rsi, 8
	add	rdi, 2
	jne	.LBB4_774
	jmp	.LBB4_1309
.LBB4_784:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_785:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + 4*rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + 4*rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + 4*rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + 4*rsi + 48]
	vpcmpgtd	xmm7, xmm3, xmm0
	vpmovsxdq	ymm9, xmm7
	vpcmpgtd	xmm1, xmm4, xmm0
	vpmovsxdq	ymm10, xmm1
	vpcmpgtd	xmm7, xmm5, xmm0
	vpmovsxdq	ymm7, xmm7
	vpcmpgtd	xmm1, xmm6, xmm0
	vpmovsxdq	ymm1, xmm1
	vpcmpeqd	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxdq	ymm3, xmm3
	vpcmpeqd	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxdq	ymm4, xmm4
	vpcmpeqd	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxdq	ymm5, xmm5
	vpcmpeqd	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxdq	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_785
# %bb.786:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_787:
	mov	esi, 1
.LBB4_788:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_788
	jmp	.LBB4_1351
.LBB4_789:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_5] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_790:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpgtd	ymm7, ymm3, ymm0
	vpcmpgtd	ymm8, ymm4, ymm0
	vpcmpgtd	ymm9, ymm5, ymm0
	vpcmpgtd	ymm10, ymm6, ymm0
	vpcmpeqd	ymm3, ymm3, ymm0
	vpxor	ymm3, ymm3, ymm1
	vcvtdq2ps	ymm3, ymm3
	vpcmpeqd	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm4, ymm1
	vcvtdq2ps	ymm4, ymm4
	vpcmpeqd	ymm5, ymm5, ymm0
	vpxor	ymm5, ymm5, ymm1
	vcvtdq2ps	ymm5, ymm5
	vpcmpeqd	ymm6, ymm6, ymm0
	vpxor	ymm6, ymm6, ymm1
	vcvtdq2ps	ymm6, ymm6
	vblendvps	ymm3, ymm3, ymm2, ymm7
	vblendvps	ymm4, ymm4, ymm2, ymm8
	vblendvps	ymm5, ymm5, ymm2, ymm9
	vblendvps	ymm6, ymm6, ymm2, ymm10
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_790
# %bb.791:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_792:
	vmovss	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	vmovss	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_794
.LBB4_793:                              #   in Loop: Header=BB4_794 Depth=1
	vmovss	dword ptr [r8 + 4*rdx], xmm3
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_794:                              # =>This Inner Loop Header: Depth=1
	cmp	dword ptr [rcx + 4*rdx], 0
	vmovaps	xmm2, xmm0
	jne	.LBB4_796
# %bb.795:                              #   in Loop: Header=BB4_794 Depth=1
	vxorps	xmm2, xmm2, xmm2
.LBB4_796:                              #   in Loop: Header=BB4_794 Depth=1
	vmovaps	xmm3, xmm1
	jg	.LBB4_793
# %bb.797:                              #   in Loop: Header=BB4_794 Depth=1
	vmovaps	xmm3, xmm2
	jmp	.LBB4_793
.LBB4_831:
	mov	esi, eax
	and	esi, -16
	xor	edi, edi
	vxorpd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI4_0] # ymm1 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_832:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm3, ymmword ptr [rcx + 8*rdi]
	vmovupd	ymm4, ymmword ptr [rcx + 8*rdi + 32]
	vmovupd	ymm5, ymmword ptr [rcx + 8*rdi + 64]
	vmovupd	ymm6, ymmword ptr [rcx + 8*rdi + 96]
	vcmpeqpd	ymm7, ymm8, ymm3
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm9, xmm7, xmm0
	vcmpeqpd	ymm7, ymm8, ymm4
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm10, xmm7, xmm0
	vcmpeqpd	ymm7, ymm8, ymm5
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm11, xmm7, xmm0
	vcmpeqpd	ymm7, ymm8, ymm6
	vextractf128	xmm0, ymm7, 1
	vpackssdw	xmm0, xmm7, xmm0
	vandpd	ymm3, ymm3, ymm1
	vorpd	ymm3, ymm2, ymm3
	vandpd	ymm4, ymm4, ymm1
	vorpd	ymm4, ymm2, ymm4
	vandpd	ymm5, ymm5, ymm1
	vorpd	ymm5, ymm2, ymm5
	vandpd	ymm6, ymm6, ymm1
	vorpd	ymm6, ymm2, ymm6
	vcvttpd2dq	xmm3, ymm3
	vpandn	xmm3, xmm9, xmm3
	vcvttpd2dq	xmm4, ymm4
	vpandn	xmm4, xmm10, xmm4
	vcvttpd2dq	xmm5, ymm5
	vcvttpd2dq	xmm6, ymm6
	vpandn	xmm5, xmm11, xmm5
	vpandn	xmm0, xmm0, xmm6
	vmovdqu	xmmword ptr [r8 + 4*rdi], xmm3
	vmovdqu	xmmword ptr [r8 + 4*rdi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 4*rdi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + 4*rdi + 48], xmm0
	add	rdi, 16
	cmp	rsi, rdi
	jne	.LBB4_832
# %bb.833:
	cmp	rsi, rax
	je	.LBB4_1351
.LBB4_834:
	vpxor	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_835:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	edi, xmm3
	cmove	edi, edx
	mov	dword ptr [r8 + 4*rsi], edi
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_835
	jmp	.LBB4_1351
.LBB4_839:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_840:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpand	xmm4, xmm4, xmm2
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpand	xmm5, xmm5, xmm2
	vpcmpeqq	ymm6, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpand	xmm6, xmm6, xmm2
	vmovdqu	xmmword ptr [r8 + 4*rsi], xmm3
	vmovdqu	xmmword ptr [r8 + 4*rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + 4*rsi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + 4*rsi + 48], xmm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_840
# %bb.841:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_842:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	qword ptr [rcx + 8*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_842
	jmp	.LBB4_1351
.LBB4_843:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_844:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqw	xmm3, xmm0, xmmword ptr [rcx + 2*rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxwd	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero
	vpcmpeqw	xmm4, xmm0, xmmword ptr [rcx + 2*rsi + 16]
	vpand	ymm3, ymm3, ymm2
	vpxor	xmm4, xmm4, xmm1
	vpmovzxwd	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqw	xmm5, xmm0, xmmword ptr [rcx + 2*rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxwd	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqw	xmm6, xmm0, xmmword ptr [rcx + 2*rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxwd	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_844
# %bb.845:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_846:                              # =>This Inner Loop Header: Depth=1
	xor	esi, esi
	cmp	word ptr [rcx + 2*rdx], 0
	setne	sil
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_846
	jmp	.LBB4_1351
.LBB4_847:
	mov	edx, r10d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_848:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + 2*rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + 2*rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + 2*rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + 2*rsi + 48]
	vpcmpgtw	xmm7, xmm3, xmm0
	vpmovsxwd	ymm9, xmm7
	vpcmpgtw	xmm1, xmm4, xmm0
	vpmovsxwd	ymm10, xmm1
	vpcmpgtw	xmm7, xmm5, xmm0
	vpmovsxwd	ymm7, xmm7
	vpcmpgtw	xmm1, xmm6, xmm0
	vpmovsxwd	ymm1, xmm1
	vpcmpeqw	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxwd	ymm3, xmm3
	vpcmpeqw	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxwd	ymm4, xmm4
	vpcmpeqw	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxwd	ymm5, xmm5
	vpcmpeqw	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxwd	ymm6, xmm6
	vblendvps	ymm3, ymm3, ymm2, ymm9
	vblendvps	ymm4, ymm4, ymm2, ymm10
	vblendvps	ymm5, ymm5, ymm2, ymm7
	vblendvps	ymm1, ymm6, ymm2, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_848
# %bb.849:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_850:
	mov	esi, 1
.LBB4_851:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_851
	jmp	.LBB4_1351
.LBB4_852:
	mov	edx, r10d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm8, ymm8, ymm8
	vbroadcastss	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_853:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm3, ymm4, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm9, xmm3, xmm1
	vpcmpgtq	ymm1, ymm5, ymm0
	vextracti128	xmm3, ymm1, 1
	vpackssdw	xmm10, xmm1, xmm3
	vpcmpgtq	ymm3, ymm6, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm11, xmm3, xmm1
	vpcmpgtq	ymm3, ymm7, ymm0
	vextracti128	xmm1, ymm3, 1
	vpackssdw	xmm1, xmm3, xmm1
	vpcmpeqq	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm8, ymm3
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpcmpeqq	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpcmpeqq	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm8, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpcmpeqq	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm8, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vblendvps	xmm3, xmm3, xmm2, xmm9
	vblendvps	xmm4, xmm4, xmm2, xmm10
	vblendvps	xmm5, xmm5, xmm2, xmm11
	vblendvps	xmm1, xmm6, xmm2, xmm1
	vmovups	xmmword ptr [r8 + 4*rsi], xmm3
	vmovups	xmmword ptr [r8 + 4*rsi + 16], xmm4
	vmovups	xmmword ptr [r8 + 4*rsi + 32], xmm5
	vmovups	xmmword ptr [r8 + 4*rsi + 48], xmm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_853
# %bb.854:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_855:
	mov	esi, 1
.LBB4_856:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB4_856
	jmp	.LBB4_1351
.LBB4_857:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vxorps	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI4_8] # ymm1 = [1,1,1,1,1,1,1,1]
.LBB4_858:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 96]
	vpsrad	ymm6, ymm2, 31
	vpor	ymm6, ymm6, ymm1
	vpsrad	ymm7, ymm3, 31
	vpor	ymm7, ymm7, ymm1
	vpsrad	ymm8, ymm4, 31
	vpor	ymm8, ymm8, ymm1
	vpsrad	ymm9, ymm5, 31
	vpor	ymm9, ymm9, ymm1
	vcvtdq2ps	ymm6, ymm6
	vcvtdq2ps	ymm7, ymm7
	vcvtdq2ps	ymm8, ymm8
	vcvtdq2ps	ymm9, ymm9
	vcvttps2dq	ymm6, ymm6
	vcvttps2dq	ymm7, ymm7
	vcvttps2dq	ymm8, ymm8
	vcvttps2dq	ymm9, ymm9
	vcmpneqps	ymm2, ymm2, ymm0
	vandps	ymm2, ymm2, ymm6
	vcmpneqps	ymm3, ymm3, ymm0
	vandps	ymm3, ymm3, ymm7
	vcmpneqps	ymm4, ymm4, ymm0
	vandps	ymm4, ymm8, ymm4
	vcmpneqps	ymm5, ymm5, ymm0
	vandps	ymm5, ymm9, ymm5
	vmovups	ymmword ptr [r8 + 4*rsi], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_858
# %bb.859:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_860:
	vxorps	xmm0, xmm0, xmm0
	jmp	.LBB4_862
.LBB4_861:                              #   in Loop: Header=BB4_862 Depth=1
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_862:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	xor	esi, esi
	vucomiss	xmm0, xmm1
	je	.LBB4_861
# %bb.863:                              #   in Loop: Header=BB4_862 Depth=1
	vmovmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	vcvtsi2ss	xmm1, xmm10, esi
	vcvttss2si	esi, xmm1
	jmp	.LBB4_861
.LBB4_870:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI4_8] # ymm1 = [1,1,1,1,1,1,1,1]
.LBB4_871:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm2, ymm0, ymmword ptr [rcx + 4*rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_871
# %bb.872:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_873
.LBB4_877:
	mov	edx, r11d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_878:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbd	ymm9, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbd	ymm10, xmm1
	vpcmpgtb	xmm7, xmm5, xmm0
	vpmovsxbd	ymm7, xmm7
	vpcmpgtb	xmm1, xmm6, xmm0
	vpmovsxbd	ymm1, xmm1
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbd	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbd	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbd	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbd	ymm6, xmm6
	vblendvps	ymm3, ymm3, ymm2, ymm9
	vblendvps	ymm4, ymm4, ymm2, ymm10
	vblendvps	ymm5, ymm5, ymm2, ymm7
	vblendvps	ymm1, ymm6, ymm2, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_878
# %bb.879:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_880
.LBB4_885:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_886:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbd	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero,xmm3[4],zero,zero,zero,xmm3[5],zero,zero,zero,xmm3[6],zero,zero,zero,xmm3[7],zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbd	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero,xmm4[4],zero,zero,zero,xmm4[5],zero,zero,zero,xmm4[6],zero,zero,zero,xmm4[7],zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbd	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero,xmm5[2],zero,zero,zero,xmm5[3],zero,zero,zero,xmm5[4],zero,zero,zero,xmm5[5],zero,zero,zero,xmm5[6],zero,zero,zero,xmm5[7],zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbd	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero,xmm6[2],zero,zero,zero,xmm6[3],zero,zero,zero,xmm6[4],zero,zero,zero,xmm6[5],zero,zero,zero,xmm6[6],zero,zero,zero,xmm6[7],zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_886
# %bb.887:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_888
.LBB4_892:
	mov	edx, r11d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_893:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpeqd	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqd	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqd	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqd	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtd	ymm3, ymm2, ymm3
	vpcmpgtd	ymm4, ymm2, ymm4
	vpcmpgtd	ymm5, ymm2, ymm5
	vpcmpgtd	ymm6, ymm2, ymm6
	vblendvps	ymm3, ymm2, ymm7, ymm3
	vblendvps	ymm4, ymm2, ymm8, ymm4
	vblendvps	ymm5, ymm2, ymm9, ymm5
	vblendvps	ymm6, ymm2, ymm10, ymm6
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_893
# %bb.894:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_895
.LBB4_900:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vxorpd	xmm0, xmm0, xmm0
	vbroadcastsd	ymm1, qword ptr [rip + .LCPI4_0] # ymm1 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_901:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm3, ymmword ptr [rcx + 8*rsi]
	vmovupd	ymm4, ymmword ptr [rcx + 8*rsi + 32]
	vmovupd	ymm5, ymmword ptr [rcx + 8*rsi + 64]
	vmovupd	ymm6, ymmword ptr [rcx + 8*rsi + 96]
	vandpd	ymm7, ymm3, ymm1
	vorpd	ymm7, ymm2, ymm7
	vandpd	ymm8, ymm4, ymm1
	vorpd	ymm8, ymm8, ymm2
	vandpd	ymm9, ymm5, ymm1
	vorpd	ymm9, ymm9, ymm2
	vandpd	ymm10, ymm6, ymm1
	vorpd	ymm10, ymm10, ymm2
	vcmpneqpd	ymm3, ymm3, ymm0
	vandpd	ymm3, ymm3, ymm7
	vcmpneqpd	ymm4, ymm4, ymm0
	vandpd	ymm4, ymm8, ymm4
	vcmpneqpd	ymm5, ymm5, ymm0
	vandpd	ymm5, ymm9, ymm5
	vcmpneqpd	ymm6, ymm6, ymm0
	vandpd	ymm6, ymm10, ymm6
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_901
# %bb.902:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_903
.LBB4_908:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_909:                              # =>This Inner Loop Header: Depth=1
	vmovd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	vmovd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	vmovd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	vmovd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbq	ymm9, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbq	ymm10, xmm1
	vpcmpgtb	xmm7, xmm5, xmm0
	vpmovsxbq	ymm7, xmm7
	vpcmpgtb	xmm1, xmm6, xmm0
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbd	xmm3, xmm3
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbd	xmm4, xmm4
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbd	xmm5, xmm5
	vcvtdq2pd	ymm5, xmm5
	vpmovsxbq	ymm1, xmm1
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbd	xmm6, xmm6
	vcvtdq2pd	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_909
# %bb.910:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_911
.LBB4_914:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
.LBB4_915:                              # =>This Inner Loop Header: Depth=1
	vmovd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	vmovd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	vmovd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	vmovd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbd	xmm3, xmm3              # xmm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero
	vpand	xmm3, xmm3, xmm2
	vcvtdq2pd	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbd	xmm4, xmm4              # xmm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero
	vpand	xmm4, xmm4, xmm2
	vcvtdq2pd	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbd	xmm5, xmm5              # xmm5 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero,xmm5[2],zero,zero,zero,xmm5[3],zero,zero,zero
	vpand	xmm5, xmm5, xmm2
	vcvtdq2pd	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbd	xmm6, xmm6              # xmm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero,xmm6[2],zero,zero,zero,xmm6[3],zero,zero,zero
	vpand	xmm6, xmm6, xmm2
	vcvtdq2pd	ymm6, xmm6
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_915
# %bb.916:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_917
.LBB4_933:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_12] # xmm2 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_934:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpacksswb	xmm3, xmm3, xmm3
	vpand	xmm3, xmm3, xmm2
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpacksswb	xmm4, xmm4, xmm4
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpand	xmm4, xmm4, xmm2
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpacksswb	xmm5, xmm5, xmm5
	vpand	xmm5, xmm5, xmm2
	vpcmpeqd	ymm6, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpacksswb	xmm6, xmm6, xmm6
	vpand	xmm6, xmm6, xmm2
	vinserti128	ymm5, ymm5, xmm6, 1
	vinserti128	ymm3, ymm3, xmm4, 1
	vpunpcklqdq	ymm3, ymm3, ymm5        # ymm3 = ymm3[0],ymm5[0],ymm3[2],ymm5[2]
	vpermq	ymm3, ymm3, 216                 # ymm3 = ymm3[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_934
# %bb.935:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_936
.LBB4_940:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI4_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	xmm10, xmm10, xmm10
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vpxor	xmm11, xmm11, xmm11
.LBB4_941:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm6, ymmword ptr [rcx + 8*rsi]
	vmovupd	ymm7, ymmword ptr [rcx + 8*rsi + 32]
	vmovupd	ymm8, ymmword ptr [rcx + 8*rsi + 64]
	vmovupd	ymm9, ymmword ptr [rcx + 8*rsi + 96]
	vcmpeqpd	ymm4, ymm10, ymm6
	vextractf128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm12, xmm4, xmm4
	vcmpeqpd	ymm5, ymm10, ymm7
	vextractf128	xmm1, ymm5, 1
	vpackssdw	xmm1, xmm5, xmm1
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm13, xmm1, xmm1
	vcmpeqpd	ymm1, ymm8, ymm10
	vextractf128	xmm3, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm3
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vcmpeqpd	ymm3, ymm9, ymm10
	vextractf128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vandpd	ymm4, ymm6, ymm0
	vorpd	ymm4, ymm2, ymm4
	vandpd	ymm6, ymm7, ymm0
	vorpd	ymm6, ymm2, ymm6
	vandpd	ymm7, ymm8, ymm0
	vorpd	ymm7, ymm2, ymm7
	vandpd	ymm8, ymm9, ymm0
	vorpd	ymm8, ymm8, ymm2
	vcvttpd2dq	xmm4, ymm4
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vcvttpd2dq	xmm6, ymm6
	vpackssdw	xmm6, xmm6, xmm6
	vpacksswb	xmm6, xmm6, xmm6
	vcvttpd2dq	xmm7, ymm7
	vpackssdw	xmm7, xmm7, xmm7
	vpacksswb	xmm7, xmm7, xmm7
	vcvttpd2dq	xmm5, ymm8
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpblendvb	xmm4, xmm4, xmm11, xmm12
	vpblendvb	xmm6, xmm6, xmm11, xmm13
	vpblendvb	xmm1, xmm7, xmm11, xmm1
	vpunpckldq	xmm4, xmm4, xmm6        # xmm4 = xmm4[0],xmm6[0],xmm4[1],xmm6[1]
	vpblendvb	xmm3, xmm5, xmm11, xmm3
	vpunpckldq	xmm1, xmm1, xmm3        # xmm1 = xmm1[0],xmm3[0],xmm1[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm4, xmm1        # xmm1 = xmm4[0],xmm1[0]
	vmovdqu	xmmword ptr [r8 + rsi], xmm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_941
# %bb.942:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_943
.LBB4_948:
	mov	r11d, r10d
	and	r11d, -128
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_20] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_949:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + rsi + 96]
	vpcmpeqb	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqb	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqb	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqb	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtb	ymm3, ymm2, ymm3
	vpcmpgtb	ymm4, ymm2, ymm4
	vpcmpgtb	ymm5, ymm2, ymm5
	vpcmpgtb	ymm6, ymm2, ymm6
	vpblendvb	ymm3, ymm2, ymm7, ymm3
	vpblendvb	ymm4, ymm2, ymm8, ymm4
	vpblendvb	ymm5, ymm2, ymm9, ymm5
	vpblendvb	ymm6, ymm2, ymm10, ymm6
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm6
	sub	rsi, -128
	cmp	r11, rsi
	jne	.LBB4_949
# %bb.950:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_951
.LBB4_956:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_17] # xmm2 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_957:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vpand	xmm4, xmm4, xmm2
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpunpckldq	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0],xmm3[1],xmm4[1]
	vpxor	ymm4, ymm5, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpand	xmm4, xmm4, xmm2
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpand	xmm5, xmm5, xmm2
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vpunpcklqdq	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0]
	vmovdqu	xmmword ptr [r8 + rsi], xmm3
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_957
# %bb.958:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_959
.LBB4_963:
	mov	edx, eax
	and	edx, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_19] # xmm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_964:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rcx + 2*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpacksswb	xmm3, xmm3, xmm4
	vpand	xmm3, xmm3, xmm2
	vpcmpeqw	ymm4, ymm0, ymmword ptr [rcx + 2*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpacksswb	xmm4, xmm4, xmm5
	vpand	xmm4, xmm4, xmm2
	vpcmpeqw	ymm5, ymm0, ymmword ptr [rcx + 2*rsi + 64]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpacksswb	xmm5, xmm5, xmm6
	vpand	xmm5, xmm5, xmm2
	vpcmpeqw	ymm6, ymm0, ymmword ptr [rcx + 2*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpacksswb	xmm6, xmm6, xmm7
	vpand	xmm6, xmm6, xmm2
	vmovdqu	xmmword ptr [r8 + rsi], xmm3
	vmovdqu	xmmword ptr [r8 + rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + rsi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + rsi + 48], xmm6
	add	rsi, 64
	cmp	rdx, rsi
	jne	.LBB4_964
# %bb.965:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_966
.LBB4_970:
	mov	r11d, r10d
	and	r11d, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm8, ymm8, ymm8
	vmovdqa	xmm9, xmmword ptr [rip + .LCPI4_19] # xmm9 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_971:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 2*rsi]
	vmovdqu	ymm5, ymmword ptr [rcx + 2*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 2*rsi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 2*rsi + 96]
	vpcmpgtw	ymm3, ymm4, ymm0
	vextracti128	xmm1, ymm3, 1
	vpacksswb	xmm10, xmm3, xmm1
	vpcmpgtw	ymm1, ymm5, ymm0
	vextracti128	xmm2, ymm1, 1
	vpacksswb	xmm11, xmm1, xmm2
	vpcmpgtw	ymm2, ymm6, ymm0
	vextracti128	xmm3, ymm2, 1
	vpacksswb	xmm2, xmm2, xmm3
	vpcmpgtw	ymm3, ymm7, ymm0
	vextracti128	xmm1, ymm3, 1
	vpacksswb	xmm1, xmm3, xmm1
	vpcmpeqw	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm8, ymm3
	vextracti128	xmm4, ymm3, 1
	vpacksswb	xmm3, xmm3, xmm4
	vpcmpeqw	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm4, 1
	vpacksswb	xmm4, xmm4, xmm5
	vpcmpeqw	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm8, ymm5
	vextracti128	xmm6, ymm5, 1
	vpacksswb	xmm5, xmm5, xmm6
	vpcmpeqw	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm8, ymm6
	vextracti128	xmm7, ymm6, 1
	vpacksswb	xmm6, xmm6, xmm7
	vpblendvb	xmm3, xmm3, xmm9, xmm10
	vpblendvb	xmm4, xmm4, xmm9, xmm11
	vpblendvb	xmm2, xmm5, xmm9, xmm2
	vpblendvb	xmm1, xmm6, xmm9, xmm1
	vmovdqu	xmmword ptr [r8 + rsi], xmm3
	vmovdqu	xmmword ptr [r8 + rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + rsi + 32], xmm2
	vmovdqu	xmmword ptr [r8 + rsi + 48], xmm1
	add	rsi, 64
	cmp	r11, rsi
	jne	.LBB4_971
# %bb.972:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_973
.LBB4_978:
	mov	r11d, r10d
	and	r11d, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vmovdqa	xmm11, xmmword ptr [rip + .LCPI4_17] # xmm11 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_979:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm10, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm8, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm1, ymm10, ymm0
	vextracti128	xmm3, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm3
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm12, xmm1, xmm1
	vpcmpgtq	ymm1, ymm8, ymm0
	vextracti128	xmm5, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm5
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm13, xmm1, xmm1
	vpcmpgtq	ymm1, ymm6, ymm0
	vextracti128	xmm7, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm7
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm7, xmm1, xmm1
	vpcmpgtq	ymm1, ymm4, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpcmpeqq	ymm2, ymm10, ymm0
	vpxor	ymm2, ymm9, ymm2
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpcmpeqq	ymm3, ymm8, ymm0
	vpxor	ymm3, ymm9, ymm3
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpcmpeqq	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm9, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpcmpeqq	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm6, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm6
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vpblendvb	xmm2, xmm2, xmm11, xmm12
	vpblendvb	xmm3, xmm3, xmm11, xmm13
	vpblendvb	xmm5, xmm5, xmm11, xmm7
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpblendvb	xmm1, xmm4, xmm11, xmm1
	vpunpckldq	xmm1, xmm5, xmm1        # xmm1 = xmm5[0],xmm1[0],xmm5[1],xmm1[1]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovdqu	xmmword ptr [r8 + rsi], xmm1
	add	rsi, 16
	cmp	r11, rsi
	jne	.LBB4_979
# %bb.980:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_981
.LBB4_986:
	mov	edx, r10d
	and	edx, -32
	xor	esi, esi
	vxorps	xmm12, xmm12, xmm12
	vpcmpeqd	ymm13, ymm13, ymm13
	vmovdqa	xmm14, xmmword ptr [rip + .LCPI4_12] # xmm14 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
	vpcmpeqd	xmm15, xmm15, xmm15
.LBB4_987:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm9, ymmword ptr [rcx + 4*rsi]
	vmovups	ymm10, ymmword ptr [rcx + 4*rsi + 32]
	vmovups	ymm11, ymmword ptr [rcx + 4*rsi + 64]
	vmovups	ymm7, ymmword ptr [rcx + 4*rsi + 96]
	vcmpeqps	ymm4, ymm9, ymm12
	vextractf128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpacksswb	xmm8, xmm4, xmm4
	vcmpeqps	ymm4, ymm10, ymm12
	vextractf128	xmm6, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm6
	vpacksswb	xmm6, xmm4, xmm4
	vcmpeqps	ymm4, ymm11, ymm12
	vextractf128	xmm0, ymm4, 1
	vpackssdw	xmm0, xmm4, xmm0
	vpacksswb	xmm4, xmm0, xmm0
	vcmpeqps	ymm0, ymm12, ymm7
	vextractf128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vpacksswb	xmm0, xmm0, xmm0
	vpcmpgtd	ymm1, ymm9, ymm13
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpacksswb	xmm1, xmm1, xmm1
	vpcmpgtd	ymm2, ymm10, ymm13
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpacksswb	xmm2, xmm2, xmm2
	vpcmpgtd	ymm3, ymm11, ymm13
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpacksswb	xmm3, xmm3, xmm3
	vpcmpgtd	ymm5, ymm7, ymm13
	vextracti128	xmm7, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm7
	vpblendvb	xmm1, xmm15, xmm14, xmm1
	vpacksswb	xmm5, xmm5, xmm5
	vpandn	xmm1, xmm8, xmm1
	vpblendvb	xmm2, xmm15, xmm14, xmm2
	vpblendvb	xmm3, xmm15, xmm14, xmm3
	vpblendvb	xmm5, xmm15, xmm14, xmm5
	vpxor	xmm7, xmm7, xmm7
	vpblendvb	xmm2, xmm2, xmm7, xmm6
	vpblendvb	xmm0, xmm5, xmm7, xmm0
	vpandn	xmm3, xmm4, xmm3
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm0
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_987
# %bb.988:
	cmp	rdx, r10
	je	.LBB4_1351
	jmp	.LBB4_989
.LBB4_994:
	mov	edx, eax
	and	edx, -128
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI4_20] # ymm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_995:                              # =>This Inner Loop Header: Depth=1
	vpcmpeqb	ymm2, ymm0, ymmword ptr [rcx + rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqb	ymm3, ymm0, ymmword ptr [rcx + rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqb	ymm4, ymm0, ymmword ptr [rcx + rsi + 64]
	vpcmpeqb	ymm5, ymm0, ymmword ptr [rcx + rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + rsi], ymm2
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm5
	sub	rsi, -128
	cmp	rdx, rsi
	jne	.LBB4_995
# %bb.996:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_997
.LBB4_1001:
	mov	r11d, r10d
	and	r11d, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vmovdqa	xmm10, xmmword ptr [rip + .LCPI4_12] # xmm10 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_1002:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm7, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm8, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpgtd	ymm3, ymm7, ymm0
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpacksswb	xmm11, xmm3, xmm3
	vpcmpgtd	ymm5, ymm8, ymm0
	vextracti128	xmm1, ymm5, 1
	vpackssdw	xmm1, xmm5, xmm1
	vpacksswb	xmm12, xmm1, xmm1
	vpcmpgtd	ymm1, ymm6, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpacksswb	xmm1, xmm1, xmm1
	vpcmpgtd	ymm2, ymm4, ymm0
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpacksswb	xmm2, xmm2, xmm2
	vpcmpeqd	ymm3, ymm7, ymm0
	vpxor	ymm3, ymm9, ymm3
	vextracti128	xmm7, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm7
	vpacksswb	xmm3, xmm3, xmm3
	vpcmpeqd	ymm7, ymm8, ymm0
	vpxor	ymm7, ymm9, ymm7
	vextracti128	xmm5, ymm7, 1
	vpackssdw	xmm5, xmm7, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpcmpeqd	ymm6, ymm6, ymm0
	vpxor	ymm6, ymm9, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpacksswb	xmm6, xmm6, xmm6
	vpcmpeqd	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm7, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm7
	vpacksswb	xmm4, xmm4, xmm4
	vpblendvb	xmm3, xmm3, xmm10, xmm11
	vpblendvb	xmm5, xmm5, xmm10, xmm12
	vpblendvb	xmm1, xmm6, xmm10, xmm1
	vpblendvb	xmm2, xmm4, xmm10, xmm2
	vinserti128	ymm1, ymm1, xmm2, 1
	vinserti128	ymm2, ymm3, xmm5, 1
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	r11, rsi
	jne	.LBB4_1002
# %bb.1003:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_1004
.LBB4_1009:
	mov	edx, r11d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_1010:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	vmovd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	vmovd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	vmovd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbq	ymm9, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbq	ymm10, xmm1
	vpcmpgtb	xmm7, xmm5, xmm0
	vpmovsxbq	ymm7, xmm7
	vpcmpgtb	xmm1, xmm6, xmm0
	vpmovsxbq	ymm1, xmm1
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbq	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbq	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbq	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbq	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1010
# %bb.1011:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1012
.LBB4_1017:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI4_15] # ymm1 = [1,1,1,1]
.LBB4_1018:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm2, ymm0, ymmword ptr [rcx + 8*rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm5
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1018
# %bb.1019:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1020
.LBB4_1024:
	mov	edx, r11d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_1025:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpeqq	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqq	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqq	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqq	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtq	ymm3, ymm2, ymm3
	vpcmpgtq	ymm4, ymm2, ymm4
	vpcmpgtq	ymm5, ymm2, ymm5
	vpcmpgtq	ymm6, ymm2, ymm6
	vblendvpd	ymm3, ymm2, ymm7, ymm3
	vblendvpd	ymm4, ymm2, ymm8, ymm4
	vblendvpd	ymm5, ymm2, ymm9, ymm5
	vblendvpd	ymm6, ymm2, ymm10, ymm6
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1025
# %bb.1026:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1027
.LBB4_1032:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_1033:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	vmovd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	vmovd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	vmovd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,zero,zero,zero,zero,xmm3[1],zero,zero,zero,zero,zero,zero,zero,xmm3[2],zero,zero,zero,zero,zero,zero,zero,xmm3[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbq	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,zero,zero,zero,zero,xmm4[1],zero,zero,zero,zero,zero,zero,zero,xmm4[2],zero,zero,zero,zero,zero,zero,zero,xmm4[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbq	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,zero,zero,zero,zero,xmm5[1],zero,zero,zero,zero,zero,zero,zero,xmm5[2],zero,zero,zero,zero,zero,zero,zero,xmm5[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbq	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,zero,zero,zero,zero,xmm6[1],zero,zero,zero,zero,zero,zero,zero,xmm6[2],zero,zero,zero,zero,zero,zero,zero,xmm6[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1033
# %bb.1034:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1035
.LBB4_1039:
	mov	edx, r11d
	and	edx, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI4_18] # ymm9 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1040:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + rsi + 48]
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbw	ymm10, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbw	ymm1, xmm1
	vpcmpgtb	xmm2, xmm5, xmm0
	vpmovsxbw	ymm2, xmm2
	vpcmpgtb	xmm7, xmm6, xmm0
	vpmovsxbw	ymm7, xmm7
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbw	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbw	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbw	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbw	ymm6, xmm6
	vpblendvb	ymm3, ymm3, ymm9, ymm10
	vpblendvb	ymm1, ymm4, ymm9, ymm1
	vpblendvb	ymm2, ymm5, ymm9, ymm2
	vpblendvb	ymm4, ymm6, ymm9, ymm7
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rsi + 96], ymm4
	add	rsi, 64
	cmp	rdx, rsi
	jne	.LBB4_1040
# %bb.1041:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1042
.LBB4_1047:
	mov	edx, r11d
	and	edx, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI4_18] # ymm9 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1048:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm3, xmmword ptr [rcx + rsi]
	vmovdqu	xmm4, xmmword ptr [rcx + rsi + 16]
	vmovdqu	xmm5, xmmword ptr [rcx + rsi + 32]
	vmovdqu	xmm6, xmmword ptr [rcx + rsi + 48]
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbw	ymm10, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbw	ymm1, xmm1
	vpcmpgtb	xmm2, xmm5, xmm0
	vpmovsxbw	ymm2, xmm2
	vpcmpgtb	xmm7, xmm6, xmm0
	vpmovsxbw	ymm7, xmm7
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbw	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbw	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbw	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbw	ymm6, xmm6
	vpblendvb	ymm3, ymm3, ymm9, ymm10
	vpblendvb	ymm1, ymm4, ymm9, ymm1
	vpblendvb	ymm2, ymm5, ymm9, ymm2
	vpblendvb	ymm4, ymm6, ymm9, ymm7
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rsi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rsi + 96], ymm4
	add	rsi, 64
	cmp	rdx, rsi
	jne	.LBB4_1048
# %bb.1049:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1050
.LBB4_1055:
	mov	edx, eax
	and	edx, -32
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1317
# %bb.1056:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI4_18] # ymm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1057:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vpandn	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm3
	vpcmpeqw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vpandn	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB4_1057
	jmp	.LBB4_1318
.LBB4_1058:
	mov	edx, eax
	and	edx, -32
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1325
# %bb.1059:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI4_18] # ymm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1060:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vpandn	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm3
	vpcmpeqw	ymm2, ymm0, ymmword ptr [rcx + 2*rdi + 64]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rcx + 2*rdi + 96]
	vpandn	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi + 96], ymm3
	add	rdi, 64
	add	rsi, 2
	jne	.LBB4_1060
	jmp	.LBB4_1326
.LBB4_1061:
	mov	edx, r11d
	and	edx, -32
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1333
# %bb.1062:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_18] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1063:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 2*rsi + 32]
	vpcmpeqw	ymm5, ymm3, ymm0
	vpxor	ymm5, ymm5, ymm1
	vpcmpeqw	ymm6, ymm4, ymm0
	vpxor	ymm6, ymm6, ymm1
	vpcmpgtw	ymm3, ymm2, ymm3
	vpcmpgtw	ymm4, ymm2, ymm4
	vpblendvb	ymm3, ymm2, ymm5, ymm3
	vpblendvb	ymm4, ymm2, ymm6, ymm4
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm4
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 2*rsi + 96]
	vpcmpeqw	ymm5, ymm3, ymm0
	vpxor	ymm5, ymm5, ymm1
	vpcmpeqw	ymm6, ymm4, ymm0
	vpxor	ymm6, ymm6, ymm1
	vpcmpgtw	ymm3, ymm2, ymm3
	vpcmpgtw	ymm4, ymm2, ymm4
	vpblendvb	ymm3, ymm2, ymm5, ymm3
	vpblendvb	ymm4, ymm2, ymm6, ymm4
	vmovdqu	ymmword ptr [r8 + 2*rsi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 96], ymm4
	add	rsi, 64
	add	rdi, 2
	jne	.LBB4_1063
	jmp	.LBB4_1334
.LBB4_1064:
	mov	edx, r11d
	and	edx, -32
	lea	rsi, [rdx - 32]
	mov	r9, rsi
	shr	r9, 5
	add	r9, 1
	test	rsi, rsi
	je	.LBB4_1342
# %bb.1065:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_18] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1066:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 2*rsi + 32]
	vpcmpeqw	ymm5, ymm3, ymm0
	vpxor	ymm5, ymm5, ymm1
	vpcmpeqw	ymm6, ymm4, ymm0
	vpxor	ymm6, ymm6, ymm1
	vpcmpgtw	ymm3, ymm2, ymm3
	vpcmpgtw	ymm4, ymm2, ymm4
	vpblendvb	ymm3, ymm2, ymm5, ymm3
	vpblendvb	ymm4, ymm2, ymm6, ymm4
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm4
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 2*rsi + 96]
	vpcmpeqw	ymm5, ymm3, ymm0
	vpxor	ymm5, ymm5, ymm1
	vpcmpeqw	ymm6, ymm4, ymm0
	vpxor	ymm6, ymm6, ymm1
	vpcmpgtw	ymm3, ymm2, ymm3
	vpcmpgtw	ymm4, ymm2, ymm4
	vpblendvb	ymm3, ymm2, ymm5, ymm3
	vpblendvb	ymm4, ymm2, ymm6, ymm4
	vmovdqu	ymmword ptr [r8 + 2*rsi + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 96], ymm4
	add	rsi, 64
	add	rdi, 2
	jne	.LBB4_1066
	jmp	.LBB4_1343
.LBB4_1067:
	mov	edx, eax
	and	edx, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_18] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1068:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqb	xmm3, xmm0, xmmword ptr [rcx + rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbw	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero,xmm3[8],zero,xmm3[9],zero,xmm3[10],zero,xmm3[11],zero,xmm3[12],zero,xmm3[13],zero,xmm3[14],zero,xmm3[15],zero
	vpcmpeqb	xmm4, xmm0, xmmword ptr [rcx + rsi + 16]
	vpand	ymm3, ymm3, ymm2
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbw	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero,xmm4[8],zero,xmm4[9],zero,xmm4[10],zero,xmm4[11],zero,xmm4[12],zero,xmm4[13],zero,xmm4[14],zero,xmm4[15],zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqb	xmm5, xmm0, xmmword ptr [rcx + rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbw	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero,xmm5[8],zero,xmm5[9],zero,xmm5[10],zero,xmm5[11],zero,xmm5[12],zero,xmm5[13],zero,xmm5[14],zero,xmm5[15],zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqb	xmm6, xmm0, xmmword ptr [rcx + rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbw	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero,xmm6[8],zero,xmm6[9],zero,xmm6[10],zero,xmm6[11],zero,xmm6[12],zero,xmm6[13],zero,xmm6[14],zero,xmm6[15],zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 2*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 2*rsi + 96], ymm6
	add	rsi, 64
	cmp	rdx, rsi
	jne	.LBB4_1068
# %bb.1069:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1070
.LBB4_1074:
	mov	edx, eax
	and	edx, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_18] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1075:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqb	xmm3, xmm0, xmmword ptr [rcx + rsi]
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbw	ymm3, xmm3              # ymm3 = xmm3[0],zero,xmm3[1],zero,xmm3[2],zero,xmm3[3],zero,xmm3[4],zero,xmm3[5],zero,xmm3[6],zero,xmm3[7],zero,xmm3[8],zero,xmm3[9],zero,xmm3[10],zero,xmm3[11],zero,xmm3[12],zero,xmm3[13],zero,xmm3[14],zero,xmm3[15],zero
	vpcmpeqb	xmm4, xmm0, xmmword ptr [rcx + rsi + 16]
	vpand	ymm3, ymm3, ymm2
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbw	ymm4, xmm4              # ymm4 = xmm4[0],zero,xmm4[1],zero,xmm4[2],zero,xmm4[3],zero,xmm4[4],zero,xmm4[5],zero,xmm4[6],zero,xmm4[7],zero,xmm4[8],zero,xmm4[9],zero,xmm4[10],zero,xmm4[11],zero,xmm4[12],zero,xmm4[13],zero,xmm4[14],zero,xmm4[15],zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqb	xmm5, xmm0, xmmword ptr [rcx + rsi + 32]
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbw	ymm5, xmm5              # ymm5 = xmm5[0],zero,xmm5[1],zero,xmm5[2],zero,xmm5[3],zero,xmm5[4],zero,xmm5[5],zero,xmm5[6],zero,xmm5[7],zero,xmm5[8],zero,xmm5[9],zero,xmm5[10],zero,xmm5[11],zero,xmm5[12],zero,xmm5[13],zero,xmm5[14],zero,xmm5[15],zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqb	xmm6, xmm0, xmmword ptr [rcx + rsi + 48]
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbw	ymm6, xmm6              # ymm6 = xmm6[0],zero,xmm6[1],zero,xmm6[2],zero,xmm6[3],zero,xmm6[4],zero,xmm6[5],zero,xmm6[6],zero,xmm6[7],zero,xmm6[8],zero,xmm6[9],zero,xmm6[10],zero,xmm6[11],zero,xmm6[12],zero,xmm6[13],zero,xmm6[14],zero,xmm6[15],zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 2*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 2*rsi + 96], ymm6
	add	rsi, 64
	cmp	rdx, rsi
	jne	.LBB4_1075
# %bb.1076:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1077
.LBB4_1081:
	mov	edx, r11d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_1082:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	vmovd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	vmovd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	vmovd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbq	ymm9, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbq	ymm10, xmm1
	vpcmpgtb	xmm7, xmm5, xmm0
	vpmovsxbq	ymm7, xmm7
	vpcmpgtb	xmm1, xmm6, xmm0
	vpmovsxbq	ymm1, xmm1
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbq	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbq	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbq	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbq	ymm6, xmm6
	vblendvpd	ymm3, ymm3, ymm2, ymm9
	vblendvpd	ymm4, ymm4, ymm2, ymm10
	vblendvpd	ymm5, ymm5, ymm2, ymm7
	vblendvpd	ymm1, ymm6, ymm2, ymm1
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1082
# %bb.1083:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1084
.LBB4_1089:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_5] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0,1.0E+0]
.LBB4_1090:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbd	ymm9, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbd	ymm10, xmm1
	vpcmpgtb	xmm7, xmm5, xmm0
	vpmovsxbd	ymm7, xmm7
	vpcmpgtb	xmm1, xmm6, xmm0
	vpmovsxbd	ymm1, xmm1
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbd	ymm3, xmm3
	vcvtdq2ps	ymm3, ymm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbd	ymm4, xmm4
	vcvtdq2ps	ymm4, ymm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbd	ymm5, xmm5
	vcvtdq2ps	ymm5, ymm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbd	ymm6, xmm6
	vcvtdq2ps	ymm6, ymm6
	vblendvps	ymm3, ymm3, ymm2, ymm9
	vblendvps	ymm4, ymm4, ymm2, ymm10
	vblendvps	ymm5, ymm5, ymm2, ymm7
	vblendvps	ymm1, ymm6, ymm2, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1090
# %bb.1091:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1092
.LBB4_1095:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI4_15] # ymm1 = [1,1,1,1]
.LBB4_1096:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm2, ymm0, ymmword ptr [rcx + 8*rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm5
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1096
# %bb.1097:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1098
.LBB4_1102:
	mov	edx, r11d
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_1103:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpeqq	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqq	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqq	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqq	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtq	ymm3, ymm2, ymm3
	vpcmpgtq	ymm4, ymm2, ymm4
	vpcmpgtq	ymm5, ymm2, ymm5
	vpcmpgtq	ymm6, ymm2, ymm6
	vblendvpd	ymm3, ymm2, ymm7, ymm3
	vblendvpd	ymm4, ymm2, ymm8, ymm4
	vblendvpd	ymm5, ymm2, ymm9, ymm5
	vblendvpd	ymm6, ymm2, ymm10, ymm6
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm3
	vmovupd	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovupd	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovupd	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1103
# %bb.1104:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1105
.LBB4_1110:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vxorps	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI4_8] # ymm1 = [1,1,1,1,1,1,1,1]
.LBB4_1111:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 96]
	vpsrad	ymm6, ymm2, 31
	vpor	ymm6, ymm6, ymm1
	vpsrad	ymm7, ymm3, 31
	vpor	ymm7, ymm7, ymm1
	vpsrad	ymm8, ymm4, 31
	vpor	ymm8, ymm8, ymm1
	vpsrad	ymm9, ymm5, 31
	vpor	ymm9, ymm9, ymm1
	vcvtdq2ps	ymm6, ymm6
	vcvtdq2ps	ymm7, ymm7
	vcvtdq2ps	ymm8, ymm8
	vcvtdq2ps	ymm9, ymm9
	vcmpneqps	ymm2, ymm2, ymm0
	vandps	ymm2, ymm2, ymm6
	vcmpneqps	ymm3, ymm3, ymm0
	vandps	ymm3, ymm3, ymm7
	vcmpneqps	ymm4, ymm4, ymm0
	vandps	ymm4, ymm8, ymm4
	vcmpneqps	ymm5, ymm5, ymm0
	vandps	ymm5, ymm9, ymm5
	vmovups	ymmword ptr [r8 + 4*rsi], ymm2
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1111
# %bb.1112:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1113
.LBB4_1118:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastq	ymm2, qword ptr [rip + .LCPI4_15] # ymm2 = [1,1,1,1]
.LBB4_1119:                             # =>This Inner Loop Header: Depth=1
	vmovd	xmm3, dword ptr [rcx + rsi]     # xmm3 = mem[0],zero,zero,zero
	vmovd	xmm4, dword ptr [rcx + rsi + 4] # xmm4 = mem[0],zero,zero,zero
	vmovd	xmm5, dword ptr [rcx + rsi + 8] # xmm5 = mem[0],zero,zero,zero
	vmovd	xmm6, dword ptr [rcx + rsi + 12] # xmm6 = mem[0],zero,zero,zero
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbq	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,zero,zero,zero,zero,xmm3[1],zero,zero,zero,zero,zero,zero,zero,xmm3[2],zero,zero,zero,zero,zero,zero,zero,xmm3[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbq	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,zero,zero,zero,zero,xmm4[1],zero,zero,zero,zero,zero,zero,zero,xmm4[2],zero,zero,zero,zero,zero,zero,zero,xmm4[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbq	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,zero,zero,zero,zero,xmm5[1],zero,zero,zero,zero,zero,zero,zero,xmm5[2],zero,zero,zero,zero,zero,zero,zero,xmm5[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbq	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,zero,zero,zero,zero,xmm6[1],zero,zero,zero,zero,zero,zero,zero,xmm6[2],zero,zero,zero,zero,zero,zero,zero,xmm6[3],zero,zero,zero,zero,zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 8*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 8*rsi + 96], ymm6
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1119
# %bb.1120:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1121
.LBB4_1125:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_1126:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbd	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero,xmm3[4],zero,zero,zero,xmm3[5],zero,zero,zero,xmm3[6],zero,zero,zero,xmm3[7],zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vcvtdq2ps	ymm3, ymm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbd	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero,xmm4[4],zero,zero,zero,xmm4[5],zero,zero,zero,xmm4[6],zero,zero,zero,xmm4[7],zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vcvtdq2ps	ymm4, ymm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbd	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero,xmm5[2],zero,zero,zero,xmm5[3],zero,zero,zero,xmm5[4],zero,zero,zero,xmm5[5],zero,zero,zero,xmm5[6],zero,zero,zero,xmm5[7],zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vcvtdq2ps	ymm5, ymm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbd	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero,xmm6[2],zero,zero,zero,xmm6[3],zero,zero,zero,xmm6[4],zero,zero,zero,xmm6[5],zero,zero,zero,xmm6[6],zero,zero,zero,xmm6[7],zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vcvtdq2ps	ymm6, ymm6
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1126
# %bb.1127:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1128
.LBB4_1144:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_12] # xmm2 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_1145:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpacksswb	xmm3, xmm3, xmm3
	vpand	xmm3, xmm3, xmm2
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpacksswb	xmm4, xmm4, xmm4
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpand	xmm4, xmm4, xmm2
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpacksswb	xmm5, xmm5, xmm5
	vpand	xmm5, xmm5, xmm2
	vpcmpeqd	ymm6, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpacksswb	xmm6, xmm6, xmm6
	vpand	xmm6, xmm6, xmm2
	vinserti128	ymm5, ymm5, xmm6, 1
	vinserti128	ymm3, ymm3, xmm4, 1
	vpunpcklqdq	ymm3, ymm3, ymm5        # ymm3 = ymm3[0],ymm5[0],ymm3[2],ymm5[2]
	vpermq	ymm3, ymm3, 216                 # ymm3 = ymm3[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1145
# %bb.1146:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1147
.LBB4_1151:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI4_0] # ymm0 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorpd	xmm10, xmm10, xmm10
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vpxor	xmm11, xmm11, xmm11
.LBB4_1152:                             # =>This Inner Loop Header: Depth=1
	vmovupd	ymm6, ymmword ptr [rcx + 8*rsi]
	vmovupd	ymm7, ymmword ptr [rcx + 8*rsi + 32]
	vmovupd	ymm8, ymmword ptr [rcx + 8*rsi + 64]
	vmovupd	ymm9, ymmword ptr [rcx + 8*rsi + 96]
	vcmpeqpd	ymm4, ymm10, ymm6
	vextractf128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm12, xmm4, xmm4
	vcmpeqpd	ymm5, ymm10, ymm7
	vextractf128	xmm1, ymm5, 1
	vpackssdw	xmm1, xmm5, xmm1
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm13, xmm1, xmm1
	vcmpeqpd	ymm1, ymm8, ymm10
	vextractf128	xmm3, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm3
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vcmpeqpd	ymm3, ymm9, ymm10
	vextractf128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vandpd	ymm4, ymm6, ymm0
	vorpd	ymm4, ymm2, ymm4
	vandpd	ymm6, ymm7, ymm0
	vorpd	ymm6, ymm2, ymm6
	vandpd	ymm7, ymm8, ymm0
	vorpd	ymm7, ymm2, ymm7
	vandpd	ymm8, ymm9, ymm0
	vorpd	ymm8, ymm8, ymm2
	vcvttpd2dq	xmm4, ymm4
	vpackusdw	xmm4, xmm4, xmm4
	vpackuswb	xmm4, xmm4, xmm4
	vcvttpd2dq	xmm6, ymm6
	vpackusdw	xmm6, xmm6, xmm6
	vpackuswb	xmm6, xmm6, xmm6
	vcvttpd2dq	xmm7, ymm7
	vpackusdw	xmm7, xmm7, xmm7
	vpackuswb	xmm7, xmm7, xmm7
	vcvttpd2dq	xmm5, ymm8
	vpackusdw	xmm5, xmm5, xmm5
	vpackuswb	xmm5, xmm5, xmm5
	vpblendvb	xmm4, xmm4, xmm11, xmm12
	vpblendvb	xmm6, xmm6, xmm11, xmm13
	vpblendvb	xmm1, xmm7, xmm11, xmm1
	vpunpckldq	xmm4, xmm4, xmm6        # xmm4 = xmm4[0],xmm6[0],xmm4[1],xmm6[1]
	vpblendvb	xmm3, xmm5, xmm11, xmm3
	vpunpckldq	xmm1, xmm1, xmm3        # xmm1 = xmm1[0],xmm3[0],xmm1[1],xmm3[1]
	vpunpcklqdq	xmm1, xmm4, xmm1        # xmm1 = xmm4[0],xmm1[0]
	vmovdqu	xmmword ptr [r8 + rsi], xmm1
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1152
# %bb.1153:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1154
.LBB4_1159:
	mov	r11d, r10d
	and	r11d, -128
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_20] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1160:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + rsi + 96]
	vpcmpeqb	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqb	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqb	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqb	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtb	ymm3, ymm2, ymm3
	vpcmpgtb	ymm4, ymm2, ymm4
	vpcmpgtb	ymm5, ymm2, ymm5
	vpcmpgtb	ymm6, ymm2, ymm6
	vpblendvb	ymm3, ymm2, ymm7, ymm3
	vpblendvb	ymm4, ymm2, ymm8, ymm4
	vpblendvb	ymm5, ymm2, ymm9, ymm5
	vpblendvb	ymm6, ymm2, ymm10, ymm6
	vmovdqu	ymmword ptr [r8 + rsi], ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm6
	sub	rsi, -128
	cmp	r11, rsi
	jne	.LBB4_1160
# %bb.1161:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_1162
.LBB4_1167:
	mov	edx, eax
	and	edx, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_17] # xmm2 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_1168:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqq	ymm3, ymm0, ymmword ptr [rcx + 8*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm4
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpand	xmm3, xmm3, xmm2
	vpcmpeqq	ymm4, ymm0, ymmword ptr [rcx + 8*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vpand	xmm4, xmm4, xmm2
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 64]
	vpunpckldq	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0],xmm3[1],xmm4[1]
	vpxor	ymm4, ymm5, ymm1
	vextracti128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vpcmpeqq	ymm5, ymm0, ymmword ptr [rcx + 8*rsi + 96]
	vpand	xmm4, xmm4, xmm2
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpand	xmm5, xmm5, xmm2
	vpunpckldq	xmm4, xmm4, xmm5        # xmm4 = xmm4[0],xmm5[0],xmm4[1],xmm5[1]
	vpunpcklqdq	xmm3, xmm3, xmm4        # xmm3 = xmm3[0],xmm4[0]
	vmovdqu	xmmword ptr [r8 + rsi], xmm3
	add	rsi, 16
	cmp	rdx, rsi
	jne	.LBB4_1168
# %bb.1169:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1170
.LBB4_1174:
	mov	edx, eax
	and	edx, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI4_19] # xmm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1175:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqw	ymm3, ymm0, ymmword ptr [rcx + 2*rsi]
	vpxor	ymm3, ymm3, ymm1
	vextracti128	xmm4, ymm3, 1
	vpacksswb	xmm3, xmm3, xmm4
	vpand	xmm3, xmm3, xmm2
	vpcmpeqw	ymm4, ymm0, ymmword ptr [rcx + 2*rsi + 32]
	vpxor	ymm4, ymm4, ymm1
	vextracti128	xmm5, ymm4, 1
	vpacksswb	xmm4, xmm4, xmm5
	vpand	xmm4, xmm4, xmm2
	vpcmpeqw	ymm5, ymm0, ymmword ptr [rcx + 2*rsi + 64]
	vpxor	ymm5, ymm5, ymm1
	vextracti128	xmm6, ymm5, 1
	vpacksswb	xmm5, xmm5, xmm6
	vpand	xmm5, xmm5, xmm2
	vpcmpeqw	ymm6, ymm0, ymmword ptr [rcx + 2*rsi + 96]
	vpxor	ymm6, ymm6, ymm1
	vextracti128	xmm7, ymm6, 1
	vpacksswb	xmm6, xmm6, xmm7
	vpand	xmm6, xmm6, xmm2
	vmovdqu	xmmword ptr [r8 + rsi], xmm3
	vmovdqu	xmmword ptr [r8 + rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + rsi + 32], xmm5
	vmovdqu	xmmword ptr [r8 + rsi + 48], xmm6
	add	rsi, 64
	cmp	rdx, rsi
	jne	.LBB4_1175
# %bb.1176:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1177
.LBB4_1181:
	mov	r11d, r10d
	and	r11d, -64
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm8, ymm8, ymm8
	vmovdqa	xmm9, xmmword ptr [rip + .LCPI4_19] # xmm9 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1182:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm4, ymmword ptr [rcx + 2*rsi]
	vmovdqu	ymm5, ymmword ptr [rcx + 2*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 2*rsi + 64]
	vmovdqu	ymm7, ymmword ptr [rcx + 2*rsi + 96]
	vpcmpgtw	ymm3, ymm4, ymm0
	vextracti128	xmm1, ymm3, 1
	vpacksswb	xmm10, xmm3, xmm1
	vpcmpgtw	ymm1, ymm5, ymm0
	vextracti128	xmm2, ymm1, 1
	vpacksswb	xmm11, xmm1, xmm2
	vpcmpgtw	ymm2, ymm6, ymm0
	vextracti128	xmm3, ymm2, 1
	vpacksswb	xmm2, xmm2, xmm3
	vpcmpgtw	ymm3, ymm7, ymm0
	vextracti128	xmm1, ymm3, 1
	vpacksswb	xmm1, xmm3, xmm1
	vpcmpeqw	ymm3, ymm4, ymm0
	vpxor	ymm3, ymm8, ymm3
	vextracti128	xmm4, ymm3, 1
	vpacksswb	xmm3, xmm3, xmm4
	vpcmpeqw	ymm4, ymm5, ymm0
	vpxor	ymm4, ymm8, ymm4
	vextracti128	xmm5, ymm4, 1
	vpacksswb	xmm4, xmm4, xmm5
	vpcmpeqw	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm8, ymm5
	vextracti128	xmm6, ymm5, 1
	vpacksswb	xmm5, xmm5, xmm6
	vpcmpeqw	ymm6, ymm7, ymm0
	vpxor	ymm6, ymm8, ymm6
	vextracti128	xmm7, ymm6, 1
	vpacksswb	xmm6, xmm6, xmm7
	vpblendvb	xmm3, xmm3, xmm9, xmm10
	vpblendvb	xmm4, xmm4, xmm9, xmm11
	vpblendvb	xmm2, xmm5, xmm9, xmm2
	vpblendvb	xmm1, xmm6, xmm9, xmm1
	vmovdqu	xmmword ptr [r8 + rsi], xmm3
	vmovdqu	xmmword ptr [r8 + rsi + 16], xmm4
	vmovdqu	xmmword ptr [r8 + rsi + 32], xmm2
	vmovdqu	xmmword ptr [r8 + rsi + 48], xmm1
	add	rsi, 64
	cmp	r11, rsi
	jne	.LBB4_1182
# %bb.1183:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_1184
.LBB4_1189:
	mov	r11d, r10d
	and	r11d, -16
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vmovdqa	xmm11, xmmword ptr [rip + .LCPI4_17] # xmm11 = <1,1,1,1,u,u,u,u,u,u,u,u,u,u,u,u>
.LBB4_1190:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm10, ymmword ptr [rcx + 8*rsi]
	vmovdqu	ymm8, ymmword ptr [rcx + 8*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 8*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 8*rsi + 96]
	vpcmpgtq	ymm1, ymm10, ymm0
	vextracti128	xmm3, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm3
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm12, xmm1, xmm1
	vpcmpgtq	ymm1, ymm8, ymm0
	vextracti128	xmm5, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm5
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm13, xmm1, xmm1
	vpcmpgtq	ymm1, ymm6, ymm0
	vextracti128	xmm7, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm7
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm7, xmm1, xmm1
	vpcmpgtq	ymm1, ymm4, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpackssdw	xmm1, xmm1, xmm1
	vpacksswb	xmm1, xmm1, xmm1
	vpcmpeqq	ymm2, ymm10, ymm0
	vpxor	ymm2, ymm9, ymm2
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpackssdw	xmm2, xmm2, xmm2
	vpacksswb	xmm2, xmm2, xmm2
	vpcmpeqq	ymm3, ymm8, ymm0
	vpxor	ymm3, ymm9, ymm3
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpackssdw	xmm3, xmm3, xmm3
	vpacksswb	xmm3, xmm3, xmm3
	vpcmpeqq	ymm5, ymm6, ymm0
	vpxor	ymm5, ymm9, ymm5
	vextracti128	xmm6, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm6
	vpackssdw	xmm5, xmm5, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpcmpeqq	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm6, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm6
	vpackssdw	xmm4, xmm4, xmm4
	vpacksswb	xmm4, xmm4, xmm4
	vpblendvb	xmm2, xmm2, xmm11, xmm12
	vpblendvb	xmm3, xmm3, xmm11, xmm13
	vpblendvb	xmm5, xmm5, xmm11, xmm7
	vpunpckldq	xmm2, xmm2, xmm3        # xmm2 = xmm2[0],xmm3[0],xmm2[1],xmm3[1]
	vpblendvb	xmm1, xmm4, xmm11, xmm1
	vpunpckldq	xmm1, xmm5, xmm1        # xmm1 = xmm5[0],xmm1[0],xmm5[1],xmm1[1]
	vpunpcklqdq	xmm1, xmm2, xmm1        # xmm1 = xmm2[0],xmm1[0]
	vmovdqu	xmmword ptr [r8 + rsi], xmm1
	add	rsi, 16
	cmp	r11, rsi
	jne	.LBB4_1190
# %bb.1191:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_1192
.LBB4_1197:
	mov	edx, r10d
	and	edx, -32
	xor	esi, esi
	vxorps	xmm12, xmm12, xmm12
	vpcmpeqd	ymm13, ymm13, ymm13
	vmovdqa	xmm14, xmmword ptr [rip + .LCPI4_12] # xmm14 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
	vpcmpeqd	xmm15, xmm15, xmm15
.LBB4_1198:                             # =>This Inner Loop Header: Depth=1
	vmovups	ymm9, ymmword ptr [rcx + 4*rsi]
	vmovups	ymm10, ymmword ptr [rcx + 4*rsi + 32]
	vmovups	ymm11, ymmword ptr [rcx + 4*rsi + 64]
	vmovups	ymm7, ymmword ptr [rcx + 4*rsi + 96]
	vcmpeqps	ymm4, ymm9, ymm12
	vextractf128	xmm5, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm5
	vpacksswb	xmm8, xmm4, xmm4
	vcmpeqps	ymm4, ymm10, ymm12
	vextractf128	xmm6, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm6
	vpacksswb	xmm6, xmm4, xmm4
	vcmpeqps	ymm4, ymm11, ymm12
	vextractf128	xmm0, ymm4, 1
	vpackssdw	xmm0, xmm4, xmm0
	vpacksswb	xmm4, xmm0, xmm0
	vcmpeqps	ymm0, ymm12, ymm7
	vextractf128	xmm1, ymm0, 1
	vpackssdw	xmm0, xmm0, xmm1
	vpacksswb	xmm0, xmm0, xmm0
	vpcmpgtd	ymm1, ymm9, ymm13
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpacksswb	xmm1, xmm1, xmm1
	vpcmpgtd	ymm2, ymm10, ymm13
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpacksswb	xmm2, xmm2, xmm2
	vpcmpgtd	ymm3, ymm11, ymm13
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpacksswb	xmm3, xmm3, xmm3
	vpcmpgtd	ymm5, ymm7, ymm13
	vextracti128	xmm7, ymm5, 1
	vpackssdw	xmm5, xmm5, xmm7
	vpblendvb	xmm1, xmm15, xmm14, xmm1
	vpacksswb	xmm5, xmm5, xmm5
	vpandn	xmm1, xmm8, xmm1
	vpblendvb	xmm2, xmm15, xmm14, xmm2
	vpblendvb	xmm3, xmm15, xmm14, xmm3
	vpblendvb	xmm5, xmm15, xmm14, xmm5
	vpxor	xmm7, xmm7, xmm7
	vpblendvb	xmm2, xmm2, xmm7, xmm6
	vpblendvb	xmm0, xmm5, xmm7, xmm0
	vpandn	xmm3, xmm4, xmm3
	vinserti128	ymm0, ymm3, xmm0, 1
	vinserti128	ymm1, ymm1, xmm2, 1
	vpunpcklqdq	ymm0, ymm1, ymm0        # ymm0 = ymm1[0],ymm0[0],ymm1[2],ymm0[2]
	vpermq	ymm0, ymm0, 216                 # ymm0 = ymm0[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm0
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1198
# %bb.1199:
	cmp	rdx, r10
	je	.LBB4_1351
	jmp	.LBB4_1200
.LBB4_1205:
	mov	edx, eax
	and	edx, -128
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI4_20] # ymm1 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
.LBB4_1206:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqb	ymm2, ymm0, ymmword ptr [rcx + rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqb	ymm3, ymm0, ymmword ptr [rcx + rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqb	ymm4, ymm0, ymmword ptr [rcx + rsi + 64]
	vpcmpeqb	ymm5, ymm0, ymmword ptr [rcx + rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + rsi], ymm2
	vmovdqu	ymmword ptr [r8 + rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + rsi + 96], ymm5
	sub	rsi, -128
	cmp	rdx, rsi
	jne	.LBB4_1206
# %bb.1207:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1208
.LBB4_1212:
	mov	r11d, r10d
	and	r11d, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm9, ymm9, ymm9
	vmovdqa	xmm10, xmmword ptr [rip + .LCPI4_12] # xmm10 = <1,1,1,1,1,1,1,1,u,u,u,u,u,u,u,u>
.LBB4_1213:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm7, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm8, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpgtd	ymm3, ymm7, ymm0
	vextracti128	xmm5, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm5
	vpacksswb	xmm11, xmm3, xmm3
	vpcmpgtd	ymm5, ymm8, ymm0
	vextracti128	xmm1, ymm5, 1
	vpackssdw	xmm1, xmm5, xmm1
	vpacksswb	xmm12, xmm1, xmm1
	vpcmpgtd	ymm1, ymm6, ymm0
	vextracti128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vpacksswb	xmm1, xmm1, xmm1
	vpcmpgtd	ymm2, ymm4, ymm0
	vextracti128	xmm3, ymm2, 1
	vpackssdw	xmm2, xmm2, xmm3
	vpacksswb	xmm2, xmm2, xmm2
	vpcmpeqd	ymm3, ymm7, ymm0
	vpxor	ymm3, ymm9, ymm3
	vextracti128	xmm7, ymm3, 1
	vpackssdw	xmm3, xmm3, xmm7
	vpacksswb	xmm3, xmm3, xmm3
	vpcmpeqd	ymm7, ymm8, ymm0
	vpxor	ymm7, ymm9, ymm7
	vextracti128	xmm5, ymm7, 1
	vpackssdw	xmm5, xmm7, xmm5
	vpacksswb	xmm5, xmm5, xmm5
	vpcmpeqd	ymm6, ymm6, ymm0
	vpxor	ymm6, ymm9, ymm6
	vextracti128	xmm7, ymm6, 1
	vpackssdw	xmm6, xmm6, xmm7
	vpacksswb	xmm6, xmm6, xmm6
	vpcmpeqd	ymm4, ymm4, ymm0
	vpxor	ymm4, ymm9, ymm4
	vextracti128	xmm7, ymm4, 1
	vpackssdw	xmm4, xmm4, xmm7
	vpacksswb	xmm4, xmm4, xmm4
	vpblendvb	xmm3, xmm3, xmm10, xmm11
	vpblendvb	xmm5, xmm5, xmm10, xmm12
	vpblendvb	xmm1, xmm6, xmm10, xmm1
	vpblendvb	xmm2, xmm4, xmm10, xmm2
	vinserti128	ymm1, ymm1, xmm2, 1
	vinserti128	ymm2, ymm3, xmm5, 1
	vpunpcklqdq	ymm1, ymm2, ymm1        # ymm1 = ymm2[0],ymm1[0],ymm2[2],ymm1[2]
	vpermq	ymm1, ymm1, 216                 # ymm1 = ymm1[0,2,1,3]
	vmovdqu	ymmword ptr [r8 + rsi], ymm1
	add	rsi, 32
	cmp	r11, rsi
	jne	.LBB4_1213
# %bb.1214:
	cmp	r11, r10
	je	.LBB4_1351
	jmp	.LBB4_1215
.LBB4_1220:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI4_8] # ymm1 = [1,1,1,1,1,1,1,1]
.LBB4_1221:                             # =>This Inner Loop Header: Depth=1
	vpcmpeqd	ymm2, ymm0, ymmword ptr [rcx + 4*rsi]
	vpandn	ymm2, ymm2, ymm1
	vpcmpeqd	ymm3, ymm0, ymmword ptr [rcx + 4*rsi + 32]
	vpandn	ymm3, ymm3, ymm1
	vpcmpeqd	ymm4, ymm0, ymmword ptr [rcx + 4*rsi + 64]
	vpcmpeqd	ymm5, ymm0, ymmword ptr [rcx + 4*rsi + 96]
	vpandn	ymm4, ymm4, ymm1
	vpandn	ymm5, ymm5, ymm1
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm5
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1221
# %bb.1222:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1223
.LBB4_1227:
	mov	edx, r11d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm8, xmm8, xmm8
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_1228:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	vpcmpgtb	xmm7, xmm3, xmm0
	vpmovsxbd	ymm9, xmm7
	vpcmpgtb	xmm1, xmm4, xmm0
	vpmovsxbd	ymm10, xmm1
	vpcmpgtb	xmm7, xmm5, xmm0
	vpmovsxbd	ymm7, xmm7
	vpcmpgtb	xmm1, xmm6, xmm0
	vpmovsxbd	ymm1, xmm1
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm8, xmm3
	vpmovsxbd	ymm3, xmm3
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm8, xmm4
	vpmovsxbd	ymm4, xmm4
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm8, xmm5
	vpmovsxbd	ymm5, xmm5
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm8, xmm6
	vpmovsxbd	ymm6, xmm6
	vblendvps	ymm3, ymm3, ymm2, ymm9
	vblendvps	ymm4, ymm4, ymm2, ymm10
	vblendvps	ymm5, ymm5, ymm2, ymm7
	vblendvps	ymm1, ymm6, ymm2, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm1
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1228
# %bb.1229:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1230
.LBB4_1235:
	mov	edx, eax
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	xmm1, xmm1, xmm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_1236:                             # =>This Inner Loop Header: Depth=1
	vmovq	xmm3, qword ptr [rcx + rsi]     # xmm3 = mem[0],zero
	vmovq	xmm4, qword ptr [rcx + rsi + 8] # xmm4 = mem[0],zero
	vmovq	xmm5, qword ptr [rcx + rsi + 16] # xmm5 = mem[0],zero
	vmovq	xmm6, qword ptr [rcx + rsi + 24] # xmm6 = mem[0],zero
	vpcmpeqb	xmm3, xmm3, xmm0
	vpxor	xmm3, xmm3, xmm1
	vpmovzxbd	ymm3, xmm3              # ymm3 = xmm3[0],zero,zero,zero,xmm3[1],zero,zero,zero,xmm3[2],zero,zero,zero,xmm3[3],zero,zero,zero,xmm3[4],zero,zero,zero,xmm3[5],zero,zero,zero,xmm3[6],zero,zero,zero,xmm3[7],zero,zero,zero
	vpand	ymm3, ymm3, ymm2
	vpcmpeqb	xmm4, xmm4, xmm0
	vpxor	xmm4, xmm4, xmm1
	vpmovzxbd	ymm4, xmm4              # ymm4 = xmm4[0],zero,zero,zero,xmm4[1],zero,zero,zero,xmm4[2],zero,zero,zero,xmm4[3],zero,zero,zero,xmm4[4],zero,zero,zero,xmm4[5],zero,zero,zero,xmm4[6],zero,zero,zero,xmm4[7],zero,zero,zero
	vpand	ymm4, ymm4, ymm2
	vpcmpeqb	xmm5, xmm5, xmm0
	vpxor	xmm5, xmm5, xmm1
	vpmovzxbd	ymm5, xmm5              # ymm5 = xmm5[0],zero,zero,zero,xmm5[1],zero,zero,zero,xmm5[2],zero,zero,zero,xmm5[3],zero,zero,zero,xmm5[4],zero,zero,zero,xmm5[5],zero,zero,zero,xmm5[6],zero,zero,zero,xmm5[7],zero,zero,zero
	vpand	ymm5, ymm5, ymm2
	vpcmpeqb	xmm6, xmm6, xmm0
	vpxor	xmm6, xmm6, xmm1
	vpmovzxbd	ymm6, xmm6              # ymm6 = xmm6[0],zero,zero,zero,xmm6[1],zero,zero,zero,xmm6[2],zero,zero,zero,xmm6[3],zero,zero,zero,xmm6[4],zero,zero,zero,xmm6[5],zero,zero,zero,xmm6[6],zero,zero,zero,xmm6[7],zero,zero,zero
	vpand	ymm6, ymm6, ymm2
	vmovdqu	ymmword ptr [r8 + 4*rsi], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovdqu	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovdqu	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1236
# %bb.1237:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1238
.LBB4_1242:
	mov	edx, r11d
	and	edx, -32
	xor	esi, esi
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqd	ymm1, ymm1, ymm1
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
.LBB4_1243:                             # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rsi]
	vmovdqu	ymm4, ymmword ptr [rcx + 4*rsi + 32]
	vmovdqu	ymm5, ymmword ptr [rcx + 4*rsi + 64]
	vmovdqu	ymm6, ymmword ptr [rcx + 4*rsi + 96]
	vpcmpeqd	ymm7, ymm3, ymm0
	vpxor	ymm7, ymm7, ymm1
	vpcmpeqd	ymm8, ymm4, ymm0
	vpxor	ymm8, ymm8, ymm1
	vpcmpeqd	ymm9, ymm5, ymm0
	vpxor	ymm9, ymm9, ymm1
	vpcmpeqd	ymm10, ymm6, ymm0
	vpxor	ymm10, ymm10, ymm1
	vpcmpgtd	ymm3, ymm2, ymm3
	vpcmpgtd	ymm4, ymm2, ymm4
	vpcmpgtd	ymm5, ymm2, ymm5
	vpcmpgtd	ymm6, ymm2, ymm6
	vblendvps	ymm3, ymm2, ymm7, ymm3
	vblendvps	ymm4, ymm2, ymm8, ymm4
	vblendvps	ymm5, ymm2, ymm9, ymm5
	vblendvps	ymm6, ymm2, ymm10, ymm6
	vmovups	ymmword ptr [r8 + 4*rsi], ymm3
	vmovups	ymmword ptr [r8 + 4*rsi + 32], ymm4
	vmovups	ymmword ptr [r8 + 4*rsi + 64], ymm5
	vmovups	ymmword ptr [r8 + 4*rsi + 96], ymm6
	add	rsi, 32
	cmp	rdx, rsi
	jne	.LBB4_1243
# %bb.1244:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1245
.LBB4_1250:
	vmovq	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
.LBB4_1251:
	jle	.LBB4_1253
# %bb.1252:
	vmovq	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = mem[0],zero
.LBB4_1253:
	vmovq	qword ptr [r8 + 8*rdx], xmm0
	or	rdx, 1
.LBB4_1254:
	add	rsi, rax
	je	.LBB4_1351
# %bb.1255:
	vmovsd	xmm0, qword ptr [rip + .LCPI4_13] # xmm0 = mem[0],zero
	vmovsd	xmm1, qword ptr [rip + .LCPI4_1] # xmm1 = mem[0],zero
	jmp	.LBB4_1257
.LBB4_1256:                             #   in Loop: Header=BB4_1257 Depth=1
	vmovsd	qword ptr [r8 + 8*rdx + 8], xmm3
	add	rdx, 2
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_1257:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	vmovapd	xmm2, xmm0
	jne	.LBB4_1258
# %bb.1261:                             #   in Loop: Header=BB4_1257 Depth=1
	vxorpd	xmm2, xmm2, xmm2
	vmovapd	xmm3, xmm1
	jle	.LBB4_1262
.LBB4_1259:                             #   in Loop: Header=BB4_1257 Depth=1
	vmovsd	qword ptr [r8 + 8*rdx], xmm3
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovapd	xmm2, xmm0
	jne	.LBB4_1260
.LBB4_1263:                             #   in Loop: Header=BB4_1257 Depth=1
	vxorpd	xmm2, xmm2, xmm2
	vmovapd	xmm3, xmm1
	jg	.LBB4_1256
	jmp	.LBB4_1264
.LBB4_1258:                             #   in Loop: Header=BB4_1257 Depth=1
	vmovapd	xmm3, xmm1
	jg	.LBB4_1259
.LBB4_1262:                             #   in Loop: Header=BB4_1257 Depth=1
	vmovapd	xmm3, xmm2
	vmovsd	qword ptr [r8 + 8*rdx], xmm3
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovapd	xmm2, xmm0
	je	.LBB4_1263
.LBB4_1260:                             #   in Loop: Header=BB4_1257 Depth=1
	vmovapd	xmm3, xmm1
	jg	.LBB4_1256
.LBB4_1264:                             #   in Loop: Header=BB4_1257 Depth=1
	vmovapd	xmm3, xmm2
	jmp	.LBB4_1256
.LBB4_1265:
	vmovd	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
.LBB4_1266:
	jle	.LBB4_1268
# %bb.1267:
	vmovd	xmm0, dword ptr [rip + .LCPI4_5] # xmm0 = mem[0],zero,zero,zero
.LBB4_1268:
	vmovd	dword ptr [r8 + 4*rdx], xmm0
	or	rdx, 1
.LBB4_1269:
	add	rsi, rax
	je	.LBB4_1351
# %bb.1270:
	vmovss	xmm0, dword ptr [rip + .LCPI4_14] # xmm0 = mem[0],zero,zero,zero
	vmovss	xmm1, dword ptr [rip + .LCPI4_5] # xmm1 = mem[0],zero,zero,zero
	jmp	.LBB4_1272
.LBB4_1271:                             #   in Loop: Header=BB4_1272 Depth=1
	vmovss	dword ptr [r8 + 4*rdx + 4], xmm3
	add	rdx, 2
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_1272:                             # =>This Inner Loop Header: Depth=1
	cmp	byte ptr [rcx + rdx], 0
	vmovaps	xmm2, xmm0
	jne	.LBB4_1273
# %bb.1276:                             #   in Loop: Header=BB4_1272 Depth=1
	vxorps	xmm2, xmm2, xmm2
	vmovaps	xmm3, xmm1
	jle	.LBB4_1277
.LBB4_1274:                             #   in Loop: Header=BB4_1272 Depth=1
	vmovss	dword ptr [r8 + 4*rdx], xmm3
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovaps	xmm2, xmm0
	jne	.LBB4_1275
.LBB4_1278:                             #   in Loop: Header=BB4_1272 Depth=1
	vxorps	xmm2, xmm2, xmm2
	vmovaps	xmm3, xmm1
	jg	.LBB4_1271
	jmp	.LBB4_1279
.LBB4_1273:                             #   in Loop: Header=BB4_1272 Depth=1
	vmovaps	xmm3, xmm1
	jg	.LBB4_1274
.LBB4_1277:                             #   in Loop: Header=BB4_1272 Depth=1
	vmovaps	xmm3, xmm2
	vmovss	dword ptr [r8 + 4*rdx], xmm3
	cmp	byte ptr [rcx + rdx + 1], 0
	vmovaps	xmm2, xmm0
	je	.LBB4_1278
.LBB4_1275:                             #   in Loop: Header=BB4_1272 Depth=1
	vmovaps	xmm3, xmm1
	jg	.LBB4_1271
.LBB4_1279:                             #   in Loop: Header=BB4_1272 Depth=1
	vmovaps	xmm3, xmm2
	jmp	.LBB4_1271
.LBB4_1282:
	xor	edi, edi
.LBB4_1283:
	test	r9b, 1
	je	.LBB4_1285
# %bb.1284:
	vmovupd	ymm0, ymmword ptr [rcx + 8*rdi]
	vxorpd	xmm1, xmm1, xmm1
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_0] # ymm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vbroadcastsd	ymm3, qword ptr [rip + .LCPI4_1] # ymm3 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vandpd	ymm2, ymm0, ymm2
	vorpd	ymm3, ymm3, ymm2
	vextractf128	xmm4, ymm3, 1
	vmovsd	xmm2, qword ptr [rip + .LCPI4_6] # xmm2 = mem[0],zero
	vsubsd	xmm5, xmm4, xmm2
	vcvttsd2si	rax, xmm5
	xor	rax, r11
	vcvttsd2si	rdx, xmm4
	vucomisd	xmm4, xmm2
	cmovae	rdx, rax
	vmovq	xmm5, rdx
	vpermilps	xmm4, xmm4, 78          # xmm4 = xmm4[2,3,0,1]
	vsubsd	xmm6, xmm4, xmm2
	vcvttsd2si	rax, xmm6
	xor	rax, r11
	vcvttsd2si	rdx, xmm4
	vucomisd	xmm4, xmm2
	cmovae	rdx, rax
	vmovq	xmm4, rdx
	vpunpcklqdq	xmm4, xmm5, xmm4        # xmm4 = xmm5[0],xmm4[0]
	vsubsd	xmm5, xmm3, xmm2
	vcvttsd2si	rax, xmm5
	xor	rax, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm2
	cmovae	rdx, rax
	vmovq	xmm5, rdx
	vpermilps	xmm3, xmm3, 78          # xmm3 = xmm3[2,3,0,1]
	vsubsd	xmm6, xmm3, xmm2
	vcvttsd2si	rax, xmm6
	xor	rax, r11
	vcvttsd2si	rdx, xmm3
	vucomisd	xmm3, xmm2
	cmovae	rdx, rax
	vmovq	xmm2, rdx
	vpunpcklqdq	xmm2, xmm5, xmm2        # xmm2 = xmm5[0],xmm2[0]
	vinserti128	ymm2, ymm2, xmm4, 1
	vcmpneqpd	ymm0, ymm0, ymm1
	vandpd	ymm0, ymm0, ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm0
.LBB4_1285:
	cmp	rsi, r10
	je	.LBB4_1351
.LBB4_1286:
	vmovddup	xmm0, qword ptr [rip + .LCPI4_1] # xmm0 = [1.0E+0,1.0E+0]
                                        # xmm0 = mem[0,0]
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovsd	xmm2, qword ptr [rip + .LCPI4_6] # xmm2 = mem[0],zero
	xor	eax, eax
	vxorpd	xmm3, xmm3, xmm3
.LBB4_1287:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm4, qword ptr [rcx + 8*rsi]   # xmm4 = mem[0],zero
	vandpd	xmm5, xmm4, xmm1
	vorpd	xmm5, xmm0, xmm5
	vsubsd	xmm6, xmm5, xmm2
	vcvttsd2si	rdx, xmm6
	xor	rdx, r11
	vcvttsd2si	rdi, xmm5
	vucomisd	xmm5, xmm2
	cmovae	rdi, rdx
	vucomisd	xmm3, xmm4
	cmove	rdi, rax
	mov	qword ptr [r8 + 8*rsi], rdi
	add	rsi, 1
	cmp	r10, rsi
	jne	.LBB4_1287
	jmp	.LBB4_1351
.LBB4_1280:
	vmovmskps	ecx, xmm0
	and	ecx, 1
	neg	ecx
	or	ecx, 1
	vcvtsi2ss	xmm0, xmm4, ecx
	vmovss	xmm1, dword ptr [rip + .LCPI4_9] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm2, xmm0, xmm1
	vcvttss2si	rcx, xmm2
	movabs	rdx, -9223372036854775808
	xor	rdx, rcx
	vcvttss2si	rcx, xmm0
	vucomiss	xmm0, xmm1
	cmovae	rcx, rdx
.LBB4_1281:
	mov	qword ptr [r8 + 8*rax], rcx
	jmp	.LBB4_1351
.LBB4_1288:
	xor	edi, edi
.LBB4_1289:
	test	r9b, 1
	je	.LBB4_1291
# %bb.1290:
	vmovupd	ymm0, ymmword ptr [rcx + 8*rdi]
	vxorpd	xmm1, xmm1, xmm1
	vcmpeqpd	ymm1, ymm0, ymm1
	vextractf128	xmm2, ymm1, 1
	vpackssdw	xmm1, xmm1, xmm2
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_0] # ymm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vandpd	ymm0, ymm0, ymm2
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_1] # ymm2 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vorpd	ymm0, ymm2, ymm0
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_7] # ymm2 = [2.147483648E+9,2.147483648E+9,2.147483648E+9,2.147483648E+9]
	vcmpltpd	ymm3, ymm0, ymm2
	vextractf128	xmm4, ymm3, 1
	vsubpd	ymm2, ymm0, ymm2
	vcvttpd2dq	xmm2, ymm2
	vbroadcastss	xmm5, dword ptr [rip + .LCPI4_4] # xmm5 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vpackssdw	xmm3, xmm3, xmm4
	vxorpd	xmm2, xmm2, xmm5
	vcvttpd2dq	xmm0, ymm0
	vblendvps	xmm0, xmm2, xmm0, xmm3
	vpandn	xmm0, xmm1, xmm0
	vmovdqu	xmmword ptr [r8 + 4*rdi], xmm0
.LBB4_1291:
	cmp	rsi, rax
	je	.LBB4_1351
.LBB4_1292:
	vxorpd	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_1293:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rsi]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	rdx, xmm3
	cmove	edx, r10d
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	cmp	rax, rsi
	jne	.LBB4_1293
	jmp	.LBB4_1351
.LBB4_1294:
	xor	esi, esi
.LBB4_1295:
	test	r9b, 1
	je	.LBB4_1297
# %bb.1296:
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rsi]
	vpsrad	ymm1, ymm0, 31
	vpbroadcastd	ymm2, dword ptr [rip + .LCPI4_8] # ymm2 = [1,1,1,1,1,1,1,1]
	vpor	ymm1, ymm1, ymm2
	vcvtdq2ps	ymm1, ymm1
	vbroadcastss	ymm2, dword ptr [rip + .LCPI4_10] # ymm2 = [2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9,2.14748365E+9]
	vcmpltps	ymm3, ymm1, ymm2
	vsubps	ymm2, ymm1, ymm2
	vcvttps2dq	ymm2, ymm2
	vbroadcastss	ymm4, dword ptr [rip + .LCPI4_4] # ymm4 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vxorps	ymm2, ymm2, ymm4
	vcvttps2dq	ymm1, ymm1
	vblendvps	ymm1, ymm2, ymm1, ymm3
	vxorps	xmm2, xmm2, xmm2
	vcmpneqps	ymm0, ymm0, ymm2
	vandps	ymm0, ymm0, ymm1
	vmovups	ymmword ptr [r8 + 4*rsi], ymm0
.LBB4_1297:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_1298:
	vpxor	xmm0, xmm0, xmm0
	jmp	.LBB4_1300
.LBB4_1299:                             #   in Loop: Header=BB4_1300 Depth=1
	mov	dword ptr [r8 + 4*rdx], esi
	add	rdx, 1
	cmp	rax, rdx
	je	.LBB4_1351
.LBB4_1300:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	xor	esi, esi
	vucomiss	xmm0, xmm1
	je	.LBB4_1299
# %bb.1301:                             #   in Loop: Header=BB4_1300 Depth=1
	vmovmskps	esi, xmm1
	and	esi, 1
	neg	esi
	or	esi, 1
	vcvtsi2ss	xmm1, xmm8, esi
	vcvttss2si	rsi, xmm1
	jmp	.LBB4_1299
.LBB4_1302:
	xor	esi, esi
.LBB4_1303:
	test	r9b, 1
	je	.LBB4_1305
# %bb.1304:
	vmovupd	ymm0, ymmword ptr [rcx + 8*rsi]
	vxorpd	xmm1, xmm1, xmm1
	vbroadcastsd	ymm2, qword ptr [rip + .LCPI4_0] # ymm2 = [-0.0E+0,-0.0E+0,-0.0E+0,-0.0E+0]
	vandpd	ymm2, ymm0, ymm2
	vbroadcastsd	ymm3, qword ptr [rip + .LCPI4_1] # ymm3 = [1.0E+0,1.0E+0,1.0E+0,1.0E+0]
	vorpd	ymm2, ymm3, ymm2
	vextractf128	xmm3, ymm2, 1
	vcvttsd2si	rdi, xmm3
	vmovq	xmm4, rdi
	vpermilps	xmm3, xmm3, 78          # xmm3 = xmm3[2,3,0,1]
	vcvttsd2si	rdi, xmm3
	vmovq	xmm3, rdi
	vpunpcklqdq	xmm3, xmm4, xmm3        # xmm3 = xmm4[0],xmm3[0]
	vcvttsd2si	rdi, xmm2
	vmovq	xmm4, rdi
	vpermilps	xmm2, xmm2, 78          # xmm2 = xmm2[2,3,0,1]
	vcvttsd2si	rdi, xmm2
	vmovq	xmm2, rdi
	vpunpcklqdq	xmm2, xmm4, xmm2        # xmm2 = xmm4[0],xmm2[0]
	vinserti128	ymm2, ymm2, xmm3, 1
	vcmpneqpd	ymm0, ymm0, ymm1
	vandpd	ymm0, ymm0, ymm2
	vmovupd	ymmword ptr [r8 + 8*rsi], ymm0
.LBB4_1305:
	cmp	rdx, rax
	je	.LBB4_1351
.LBB4_1306:
	xor	esi, esi
	vxorpd	xmm0, xmm0, xmm0
	vmovapd	xmm1, xmmword ptr [rip + .LCPI4_2] # xmm1 = [-0.0E+0,-0.0E+0]
	vmovddup	xmm2, qword ptr [rip + .LCPI4_1] # xmm2 = [1.0E+0,1.0E+0]
                                        # xmm2 = mem[0,0]
.LBB4_1307:                             # =>This Inner Loop Header: Depth=1
	vmovsd	xmm3, qword ptr [rcx + 8*rdx]   # xmm3 = mem[0],zero
	vucomisd	xmm0, xmm3
	vandpd	xmm3, xmm3, xmm1
	vorpd	xmm3, xmm2, xmm3
	vcvttsd2si	rdi, xmm3
	cmove	rdi, rsi
	mov	qword ptr [r8 + 8*rdx], rdi
	add	rdx, 1
	cmp	rax, rdx
	jne	.LBB4_1307
	jmp	.LBB4_1351
.LBB4_1308:
	xor	esi, esi
.LBB4_1309:
	test	r9b, 1
	je	.LBB4_1311
# %bb.1310:
	vmovups	xmm0, xmmword ptr [rcx + 4*rsi]
	vpxor	xmm1, xmm1, xmm1
	vcmpeqps	xmm1, xmm0, xmm1
	vpmovsxdq	ymm1, xmm1
	vpsrad	xmm0, xmm0, 31
	vpbroadcastd	xmm2, dword ptr [rip + .LCPI4_8] # xmm2 = [1,1,1,1]
	vpor	xmm0, xmm0, xmm2
	vcvtdq2ps	xmm0, xmm0
	vpermilps	xmm2, xmm0, 231         # xmm2 = xmm0[3,1,2,3]
	vcvttss2si	rax, xmm2
	vmovq	xmm2, rax
	vpermilpd	xmm3, xmm0, 1           # xmm3 = xmm0[1,0]
	vcvttss2si	rax, xmm3
	vmovq	xmm3, rax
	vpunpcklqdq	xmm2, xmm3, xmm2        # xmm2 = xmm3[0],xmm2[0]
	vcvttss2si	rax, xmm0
	vmovq	xmm3, rax
	vmovshdup	xmm0, xmm0              # xmm0 = xmm0[1,1,3,3]
	vcvttss2si	rax, xmm0
	vmovq	xmm0, rax
	vpunpcklqdq	xmm0, xmm3, xmm0        # xmm0 = xmm3[0],xmm0[0]
	vinserti128	ymm0, ymm0, xmm2, 1
	vpandn	ymm0, ymm1, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rsi], ymm0
.LBB4_1311:
	cmp	rdx, r10
	je	.LBB4_1351
.LBB4_1312:
	vxorps	xmm0, xmm0, xmm0
	jmp	.LBB4_1315
.LBB4_1313:                             #   in Loop: Header=BB4_1315 Depth=1
	vmovmskps	eax, xmm1
	and	eax, 1
	neg	eax
	or	eax, 1
	vcvtsi2ss	xmm1, xmm6, eax
	vcvttss2si	rsi, xmm1
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	r10, rdx
	je	.LBB4_1351
.LBB4_1315:                             # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	vucomiss	xmm0, xmm1
	jne	.LBB4_1313
# %bb.1316:                             #   in Loop: Header=BB4_1315 Depth=1
	xor	esi, esi
	mov	qword ptr [r8 + 8*rdx], rsi
	add	rdx, 1
	cmp	r10, rdx
	jne	.LBB4_1315
	jmp	.LBB4_1351
.LBB4_1317:
	xor	edi, edi
.LBB4_1318:
	test	r9b, 1
	je	.LBB4_1320
# %bb.1319:
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_18] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpcmpeqw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vpandn	ymm1, ymm1, ymm2
	vpandn	ymm0, ymm0, ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB4_1320:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1321
.LBB4_1325:
	xor	edi, edi
.LBB4_1326:
	test	r9b, 1
	je	.LBB4_1328
# %bb.1327:
	vpxor	xmm0, xmm0, xmm0
	vpcmpeqw	ymm1, ymm0, ymmword ptr [rcx + 2*rdi]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI4_18] # ymm2 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpcmpeqw	ymm0, ymm0, ymmword ptr [rcx + 2*rdi + 32]
	vpandn	ymm1, ymm1, ymm2
	vpandn	ymm0, ymm0, ymm2
	vmovdqu	ymmword ptr [r8 + 2*rdi], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rdi + 32], ymm0
.LBB4_1328:
	cmp	rdx, rax
	je	.LBB4_1351
	jmp	.LBB4_1329
.LBB4_1333:
	xor	esi, esi
.LBB4_1334:
	test	r9b, 1
	je	.LBB4_1336
# %bb.1335:
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rsi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rsi + 32]
	vpxor	xmm2, xmm2, xmm2
	vpcmpeqw	ymm3, ymm0, ymm2
	vpcmpeqd	ymm4, ymm4, ymm4
	vpxor	ymm3, ymm3, ymm4
	vpcmpeqw	ymm2, ymm1, ymm2
	vpxor	ymm2, ymm2, ymm4
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI4_18] # ymm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpcmpgtw	ymm0, ymm4, ymm0
	vpcmpgtw	ymm1, ymm4, ymm1
	vpblendvb	ymm0, ymm4, ymm3, ymm0
	vpblendvb	ymm1, ymm4, ymm2, ymm1
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm1
.LBB4_1336:
	cmp	rdx, r11
	je	.LBB4_1351
	jmp	.LBB4_1337
.LBB4_1342:
	xor	esi, esi
.LBB4_1343:
	test	r9b, 1
	je	.LBB4_1345
# %bb.1344:
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rsi]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rsi + 32]
	vpxor	xmm2, xmm2, xmm2
	vpcmpeqw	ymm3, ymm0, ymm2
	vpcmpeqd	ymm4, ymm4, ymm4
	vpxor	ymm3, ymm3, ymm4
	vpcmpeqw	ymm2, ymm1, ymm2
	vpxor	ymm2, ymm2, ymm4
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI4_18] # ymm4 = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
	vpcmpgtw	ymm0, ymm4, ymm0
	vpcmpgtw	ymm1, ymm4, ymm1
	vpblendvb	ymm0, ymm4, ymm3, ymm0
	vpblendvb	ymm1, ymm4, ymm2, ymm1
	vmovdqu	ymmword ptr [r8 + 2*rsi], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rsi + 32], ymm1
.LBB4_1345:
	cmp	rdx, r11
	jne	.LBB4_1346
.LBB4_1351:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	vzeroupper
	ret
.Lfunc_end4:
	.size	arithmetic_unary_diff_type_avx2, .Lfunc_end4-arithmetic_unary_diff_type_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
