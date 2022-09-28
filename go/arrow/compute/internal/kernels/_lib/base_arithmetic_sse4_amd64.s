	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.globl	arithmetic_sse4                 # -- Begin function arithmetic_sse4
	.p2align	4, 0x90
	.type	arithmetic_sse4,@function
arithmetic_sse4:                        # @arithmetic_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 1
	jg	.LBB0_10
# %bb.1:
	test	sil, sil
	je	.LBB0_19
# %bb.2:
	cmp	sil, 1
	jne	.LBB0_697
# %bb.3:
	cmp	edi, 6
	jg	.LBB0_371
# %bb.4:
	cmp	edi, 3
	jle	.LBB0_5
# %bb.365:
	cmp	edi, 4
	je	.LBB0_412
# %bb.366:
	cmp	edi, 5
	je	.LBB0_428
# %bb.367:
	cmp	edi, 6
	jne	.LBB0_697
# %bb.368:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.369:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_444
# %bb.370:
	xor	esi, esi
.LBB0_453:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_455
.LBB0_454:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_454
.LBB0_455:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_456
	jmp	.LBB0_697
.LBB0_10:
	cmp	sil, 2
	je	.LBB0_192
# %bb.11:
	cmp	sil, 3
	jne	.LBB0_697
# %bb.12:
	cmp	edi, 6
	jg	.LBB0_537
# %bb.13:
	cmp	edi, 3
	jle	.LBB0_14
# %bb.531:
	cmp	edi, 4
	je	.LBB0_578
# %bb.532:
	cmp	edi, 5
	je	.LBB0_594
# %bb.533:
	cmp	edi, 6
	jne	.LBB0_697
# %bb.534:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.535:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_610
# %bb.536:
	xor	esi, esi
.LBB0_619:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_621
.LBB0_620:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_620
.LBB0_621:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_622:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_622
	jmp	.LBB0_697
.LBB0_19:
	cmp	edi, 6
	jg	.LBB0_32
# %bb.20:
	cmp	edi, 3
	jle	.LBB0_21
# %bb.26:
	cmp	edi, 4
	je	.LBB0_73
# %bb.27:
	cmp	edi, 5
	je	.LBB0_89
# %bb.28:
	cmp	edi, 6
	jne	.LBB0_697
# %bb.29:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.30:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_105
# %bb.31:
	xor	esi, esi
.LBB0_114:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_116
.LBB0_115:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_115
.LBB0_116:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_117:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_117
	jmp	.LBB0_697
.LBB0_192:
	cmp	edi, 6
	jg	.LBB0_205
# %bb.193:
	cmp	edi, 3
	jle	.LBB0_194
# %bb.199:
	cmp	edi, 4
	je	.LBB0_246
# %bb.200:
	cmp	edi, 5
	je	.LBB0_262
# %bb.201:
	cmp	edi, 6
	jne	.LBB0_697
# %bb.202:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.203:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_278
# %bb.204:
	xor	esi, esi
.LBB0_287:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_289
.LBB0_288:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_288
.LBB0_289:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_290:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_290
	jmp	.LBB0_697
.LBB0_371:
	cmp	edi, 8
	jle	.LBB0_372
# %bb.377:
	cmp	edi, 9
	je	.LBB0_486
# %bb.378:
	cmp	edi, 11
	je	.LBB0_502
# %bb.379:
	cmp	edi, 12
	jne	.LBB0_697
# %bb.380:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.381:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_518
# %bb.382:
	xor	esi, esi
.LBB0_527:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_529
.LBB0_528:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_528
.LBB0_529:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_530:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_530
	jmp	.LBB0_697
.LBB0_537:
	cmp	edi, 8
	jle	.LBB0_538
# %bb.543:
	cmp	edi, 9
	je	.LBB0_652
# %bb.544:
	cmp	edi, 11
	je	.LBB0_668
# %bb.545:
	cmp	edi, 12
	jne	.LBB0_697
# %bb.546:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.547:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_684
# %bb.548:
	xor	esi, esi
.LBB0_693:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_695
.LBB0_694:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_694
.LBB0_695:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_696:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_696
	jmp	.LBB0_697
.LBB0_32:
	cmp	edi, 8
	jle	.LBB0_33
# %bb.38:
	cmp	edi, 9
	je	.LBB0_147
# %bb.39:
	cmp	edi, 11
	je	.LBB0_163
# %bb.40:
	cmp	edi, 12
	jne	.LBB0_697
# %bb.41:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.42:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_179
# %bb.43:
	xor	esi, esi
.LBB0_188:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_190
.LBB0_189:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_189
.LBB0_190:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_191:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_191
	jmp	.LBB0_697
.LBB0_205:
	cmp	edi, 8
	jle	.LBB0_206
# %bb.211:
	cmp	edi, 9
	je	.LBB0_320
# %bb.212:
	cmp	edi, 11
	je	.LBB0_336
# %bb.213:
	cmp	edi, 12
	jne	.LBB0_697
# %bb.214:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.215:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_352
# %bb.216:
	xor	esi, esi
.LBB0_361:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_363
.LBB0_362:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_362
.LBB0_363:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_364:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_364
	jmp	.LBB0_697
.LBB0_5:
	cmp	edi, 2
	je	.LBB0_383
# %bb.6:
	cmp	edi, 3
	jne	.LBB0_697
# %bb.7:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.8:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_399
# %bb.9:
	xor	esi, esi
.LBB0_408:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_410
.LBB0_409:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_409
.LBB0_410:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_411:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_411
	jmp	.LBB0_697
.LBB0_14:
	cmp	edi, 2
	je	.LBB0_549
# %bb.15:
	cmp	edi, 3
	jne	.LBB0_697
# %bb.16:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.17:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_565
# %bb.18:
	xor	esi, esi
.LBB0_574:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_576
.LBB0_575:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_575
.LBB0_576:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_577:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_577
	jmp	.LBB0_697
.LBB0_21:
	cmp	edi, 2
	je	.LBB0_44
# %bb.22:
	cmp	edi, 3
	jne	.LBB0_697
# %bb.23:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.24:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_60
# %bb.25:
	xor	esi, esi
.LBB0_69:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_71
.LBB0_70:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_70
.LBB0_71:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_72:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_72
	jmp	.LBB0_697
.LBB0_194:
	cmp	edi, 2
	je	.LBB0_217
# %bb.195:
	cmp	edi, 3
	jne	.LBB0_697
# %bb.196:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.197:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_233
# %bb.198:
	xor	esi, esi
.LBB0_242:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_244
.LBB0_243:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_243
.LBB0_244:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_245:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_245
	jmp	.LBB0_697
.LBB0_372:
	cmp	edi, 7
	je	.LBB0_457
# %bb.373:
	cmp	edi, 8
	jne	.LBB0_697
# %bb.374:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.375:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_473
# %bb.376:
	xor	esi, esi
.LBB0_482:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_484
.LBB0_483:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_483
.LBB0_484:
	cmp	r9, 3
	jb	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_538:
	cmp	edi, 7
	je	.LBB0_623
# %bb.539:
	cmp	edi, 8
	jne	.LBB0_697
# %bb.540:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.541:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_639
# %bb.542:
	xor	esi, esi
.LBB0_648:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_650
.LBB0_649:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_649
.LBB0_650:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_651:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_651
	jmp	.LBB0_697
.LBB0_33:
	cmp	edi, 7
	je	.LBB0_118
# %bb.34:
	cmp	edi, 8
	jne	.LBB0_697
# %bb.35:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.36:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_134
# %bb.37:
	xor	esi, esi
.LBB0_143:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_145
.LBB0_144:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_144
.LBB0_145:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_146:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_146
	jmp	.LBB0_697
.LBB0_206:
	cmp	edi, 7
	je	.LBB0_291
# %bb.207:
	cmp	edi, 8
	jne	.LBB0_697
# %bb.208:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.209:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_307
# %bb.210:
	xor	esi, esi
.LBB0_316:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_318
.LBB0_317:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_317
.LBB0_318:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_319:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_319
	jmp	.LBB0_697
.LBB0_412:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.413:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_415
# %bb.414:
	xor	esi, esi
.LBB0_424:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_426
.LBB0_425:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_425
.LBB0_426:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_427:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_427
	jmp	.LBB0_697
.LBB0_428:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.429:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_431
# %bb.430:
	xor	esi, esi
.LBB0_440:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_442
.LBB0_441:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_441
.LBB0_442:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_443:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_443
	jmp	.LBB0_697
.LBB0_578:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.579:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_581
# %bb.580:
	xor	esi, esi
.LBB0_590:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_592
.LBB0_591:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_591
.LBB0_592:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_593:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_593
	jmp	.LBB0_697
.LBB0_594:
	test	r9d, r9d
	jle	.LBB0_697
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
	jb	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_73:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.74:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_76
# %bb.75:
	xor	esi, esi
.LBB0_85:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_87
.LBB0_86:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_86
.LBB0_87:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_88:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_88
	jmp	.LBB0_697
.LBB0_89:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.90:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_92
# %bb.91:
	xor	esi, esi
.LBB0_101:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_103
.LBB0_102:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_102
.LBB0_103:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_104:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_104
	jmp	.LBB0_697
.LBB0_246:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.247:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_249
# %bb.248:
	xor	esi, esi
.LBB0_258:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_260
.LBB0_259:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_259
.LBB0_260:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_261:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_261
	jmp	.LBB0_697
.LBB0_262:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.263:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_265
# %bb.264:
	xor	esi, esi
.LBB0_274:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_276
.LBB0_275:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_275
.LBB0_276:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_277:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_277
	jmp	.LBB0_697
.LBB0_486:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.487:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_489
# %bb.488:
	xor	esi, esi
.LBB0_498:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_500
.LBB0_499:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_499
.LBB0_500:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_501:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_501
	jmp	.LBB0_697
.LBB0_502:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.503:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_505
# %bb.504:
	xor	esi, esi
.LBB0_514:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_516
.LBB0_515:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_515
.LBB0_516:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_517:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_517
	jmp	.LBB0_697
.LBB0_652:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.653:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_655
# %bb.654:
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
	jb	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_668:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.669:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_671
# %bb.670:
	xor	esi, esi
.LBB0_680:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_682
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_681
.LBB0_682:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_683:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_683
	jmp	.LBB0_697
.LBB0_147:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.148:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_150
# %bb.149:
	xor	esi, esi
.LBB0_159:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_161
.LBB0_160:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_160
.LBB0_161:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_162:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_162
	jmp	.LBB0_697
.LBB0_163:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.164:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_166
# %bb.165:
	xor	esi, esi
.LBB0_175:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_177
.LBB0_176:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_176
.LBB0_177:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_178:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_178
	jmp	.LBB0_697
.LBB0_320:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.321:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_323
# %bb.322:
	xor	esi, esi
.LBB0_332:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_334
.LBB0_333:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_333
.LBB0_334:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_335:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_335
	jmp	.LBB0_697
.LBB0_336:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.337:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_339
# %bb.338:
	xor	esi, esi
.LBB0_348:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_350
.LBB0_349:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_349
.LBB0_350:
	cmp	rax, 3
	jb	.LBB0_697
.LBB0_351:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_351
	jmp	.LBB0_697
.LBB0_383:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.384:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_386
# %bb.385:
	xor	esi, esi
.LBB0_395:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_397
.LBB0_396:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_396
.LBB0_397:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_398:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_398
	jmp	.LBB0_697
.LBB0_549:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.550:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_552
# %bb.551:
	xor	esi, esi
.LBB0_561:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_563
.LBB0_562:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_562
.LBB0_563:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_564:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_564
	jmp	.LBB0_697
.LBB0_44:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.45:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_47
# %bb.46:
	xor	esi, esi
.LBB0_56:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_58
.LBB0_57:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_57
.LBB0_58:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_59:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_59
	jmp	.LBB0_697
.LBB0_217:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.218:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_220
# %bb.219:
	xor	esi, esi
.LBB0_229:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_231
.LBB0_230:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_230
.LBB0_231:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_232:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_232
	jmp	.LBB0_697
.LBB0_457:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.458:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_460
# %bb.459:
	xor	esi, esi
.LBB0_469:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_471
.LBB0_470:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_470
.LBB0_471:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_472:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_472
	jmp	.LBB0_697
.LBB0_623:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.624:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_626
# %bb.625:
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
	jb	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_118:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.119:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_121
# %bb.120:
	xor	esi, esi
.LBB0_130:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_132
.LBB0_131:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_131
.LBB0_132:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_133:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_133
	jmp	.LBB0_697
.LBB0_291:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.292:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_294
# %bb.293:
	xor	esi, esi
.LBB0_303:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB0_305
.LBB0_304:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB0_304
.LBB0_305:
	cmp	r9, 3
	jb	.LBB0_697
.LBB0_306:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_306
	jmp	.LBB0_697
.LBB0_444:
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
# %bb.445:
	and	al, dil
	jne	.LBB0_453
# %bb.446:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_447
# %bb.448:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_449:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_449
	jmp	.LBB0_450
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
	jne	.LBB0_619
# %bb.611:
	and	al, dil
	jne	.LBB0_619
# %bb.612:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_613
# %bb.614:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_615:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_615
	jmp	.LBB0_616
.LBB0_105:
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
	jne	.LBB0_114
# %bb.106:
	and	al, dil
	jne	.LBB0_114
# %bb.107:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_108
# %bb.109:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_110:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_110
	jmp	.LBB0_111
.LBB0_278:
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
	jne	.LBB0_287
# %bb.279:
	and	al, dil
	jne	.LBB0_287
# %bb.280:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_281
# %bb.282:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_283:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_283
	jmp	.LBB0_284
.LBB0_518:
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
	jne	.LBB0_527
# %bb.519:
	and	al, dil
	jne	.LBB0_527
# %bb.520:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_521
# %bb.522:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_523:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_523
	jmp	.LBB0_524
.LBB0_684:
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
	jne	.LBB0_693
# %bb.685:
	and	al, dil
	jne	.LBB0_693
# %bb.686:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_687
# %bb.688:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_689:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_689
	jmp	.LBB0_690
.LBB0_179:
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
	jne	.LBB0_188
# %bb.180:
	and	al, dil
	jne	.LBB0_188
# %bb.181:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_182
# %bb.183:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_184:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_184
	jmp	.LBB0_185
.LBB0_352:
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
	jne	.LBB0_361
# %bb.353:
	and	al, dil
	jne	.LBB0_361
# %bb.354:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_355
# %bb.356:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_357:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_357
	jmp	.LBB0_358
.LBB0_399:
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
	jne	.LBB0_408
# %bb.400:
	and	al, dil
	jne	.LBB0_408
# %bb.401:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_402
# %bb.403:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_404:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_404
	jmp	.LBB0_405
.LBB0_565:
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
	jne	.LBB0_574
# %bb.566:
	and	al, dil
	jne	.LBB0_574
# %bb.567:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_568
# %bb.569:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_570:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_570
	jmp	.LBB0_571
.LBB0_60:
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
	jne	.LBB0_69
# %bb.61:
	and	al, dil
	jne	.LBB0_69
# %bb.62:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_63
# %bb.64:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_65:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_65
	jmp	.LBB0_66
.LBB0_233:
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
	jne	.LBB0_242
# %bb.234:
	and	al, dil
	jne	.LBB0_242
# %bb.235:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_236
# %bb.237:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_238:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_238
	jmp	.LBB0_239
.LBB0_473:
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
# %bb.474:
	and	al, dil
	jne	.LBB0_482
# %bb.475:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_476
# %bb.477:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_478:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_478
	jmp	.LBB0_479
.LBB0_639:
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
	jne	.LBB0_648
# %bb.640:
	and	al, dil
	jne	.LBB0_648
# %bb.641:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_642
# %bb.643:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_644:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_644
	jmp	.LBB0_645
.LBB0_134:
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
	jne	.LBB0_143
# %bb.135:
	and	al, dil
	jne	.LBB0_143
# %bb.136:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_137
# %bb.138:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_139:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_139
	jmp	.LBB0_140
.LBB0_307:
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
	jne	.LBB0_316
# %bb.308:
	and	al, dil
	jne	.LBB0_316
# %bb.309:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_310
# %bb.311:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_312:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_312
	jmp	.LBB0_313
.LBB0_415:
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
	jne	.LBB0_424
# %bb.416:
	and	al, dil
	jne	.LBB0_424
# %bb.417:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_418
# %bb.419:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_420:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_420
	jmp	.LBB0_421
.LBB0_431:
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
	jne	.LBB0_440
# %bb.432:
	and	al, dil
	jne	.LBB0_440
# %bb.433:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_434
# %bb.435:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_436:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_436
	jmp	.LBB0_437
.LBB0_581:
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
	jne	.LBB0_590
# %bb.582:
	and	al, dil
	jne	.LBB0_590
# %bb.583:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_584
# %bb.585:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_586:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_586
	jmp	.LBB0_587
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
.LBB0_76:
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
	jne	.LBB0_85
# %bb.77:
	and	al, dil
	jne	.LBB0_85
# %bb.78:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_79
# %bb.80:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_81:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_81
	jmp	.LBB0_82
.LBB0_92:
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
	jne	.LBB0_101
# %bb.93:
	and	al, dil
	jne	.LBB0_101
# %bb.94:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_95
# %bb.96:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_97:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_97
	jmp	.LBB0_98
.LBB0_249:
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
	jne	.LBB0_258
# %bb.250:
	and	al, dil
	jne	.LBB0_258
# %bb.251:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_252
# %bb.253:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_254:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_254
	jmp	.LBB0_255
.LBB0_265:
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
	jne	.LBB0_274
# %bb.266:
	and	al, dil
	jne	.LBB0_274
# %bb.267:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r9, rax
	shr	r9, 4
	add	r9, 1
	test	rax, rax
	je	.LBB0_268
# %bb.269:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_270:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_270
	jmp	.LBB0_271
.LBB0_489:
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
	jne	.LBB0_498
# %bb.490:
	and	al, dil
	jne	.LBB0_498
# %bb.491:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_492
# %bb.493:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_494:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_494
	jmp	.LBB0_495
.LBB0_505:
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
	jne	.LBB0_514
# %bb.506:
	and	al, dil
	jne	.LBB0_514
# %bb.507:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_508
# %bb.509:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_510
	jmp	.LBB0_511
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
	jne	.LBB0_680
# %bb.672:
	and	al, dil
	jne	.LBB0_680
# %bb.673:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_674
# %bb.675:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_676:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_676
	jmp	.LBB0_677
.LBB0_150:
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
	jne	.LBB0_159
# %bb.151:
	and	al, dil
	jne	.LBB0_159
# %bb.152:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_153
# %bb.154:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_155:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_155
	jmp	.LBB0_156
.LBB0_166:
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
	jne	.LBB0_175
# %bb.167:
	and	al, dil
	jne	.LBB0_175
# %bb.168:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_169
# %bb.170:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_171:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_171
	jmp	.LBB0_172
.LBB0_323:
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
# %bb.324:
	and	al, dil
	jne	.LBB0_332
# %bb.325:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r9, rax
	shr	r9, 2
	add	r9, 1
	test	rax, rax
	je	.LBB0_326
# %bb.327:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_328:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_328
	jmp	.LBB0_329
.LBB0_339:
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
	jne	.LBB0_348
# %bb.340:
	and	al, dil
	jne	.LBB0_348
# %bb.341:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_342
# %bb.343:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_344:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_344
	jmp	.LBB0_345
.LBB0_386:
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
	jne	.LBB0_395
# %bb.387:
	and	al, dil
	jne	.LBB0_395
# %bb.388:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_389
# %bb.390:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_391:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_391
	jmp	.LBB0_392
.LBB0_552:
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
	jne	.LBB0_561
# %bb.553:
	and	al, dil
	jne	.LBB0_561
# %bb.554:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_555
# %bb.556:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_557:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_557
	jmp	.LBB0_558
.LBB0_47:
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
	jne	.LBB0_56
# %bb.48:
	and	al, dil
	jne	.LBB0_56
# %bb.49:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_50
# %bb.51:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_52:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_52
	jmp	.LBB0_53
.LBB0_220:
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
	jne	.LBB0_229
# %bb.221:
	and	al, dil
	jne	.LBB0_229
# %bb.222:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r9, rax
	shr	r9, 5
	add	r9, 1
	test	rax, rax
	je	.LBB0_223
# %bb.224:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_225:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_225
	jmp	.LBB0_226
.LBB0_460:
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
# %bb.461:
	and	al, dil
	jne	.LBB0_469
# %bb.462:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_463
# %bb.464:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_465:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_465
	jmp	.LBB0_466
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
.LBB0_121:
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
	jne	.LBB0_130
# %bb.122:
	and	al, dil
	jne	.LBB0_130
# %bb.123:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_124
# %bb.125:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_126:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_126
	jmp	.LBB0_127
.LBB0_294:
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
	jne	.LBB0_303
# %bb.295:
	and	al, dil
	jne	.LBB0_303
# %bb.296:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r9, rax
	shr	r9, 3
	add	r9, 1
	test	rax, rax
	je	.LBB0_297
# %bb.298:
	mov	rax, r9
	and	rax, -2
	neg	rax
	xor	edi, edi
.LBB0_299:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_299
	jmp	.LBB0_300
.LBB0_447:
	xor	edi, edi
.LBB0_450:
	test	r9b, 1
	je	.LBB0_452
# %bb.451:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_452:
	cmp	rsi, r10
	jne	.LBB0_453
	jmp	.LBB0_697
.LBB0_613:
	xor	edi, edi
.LBB0_616:
	test	r9b, 1
	je	.LBB0_618
# %bb.617:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_618:
	cmp	rsi, r10
	jne	.LBB0_619
	jmp	.LBB0_697
.LBB0_108:
	xor	edi, edi
.LBB0_111:
	test	r9b, 1
	je	.LBB0_113
# %bb.112:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_113:
	cmp	rsi, r10
	jne	.LBB0_114
	jmp	.LBB0_697
.LBB0_281:
	xor	edi, edi
.LBB0_284:
	test	r9b, 1
	je	.LBB0_286
# %bb.285:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_286:
	cmp	rsi, r10
	jne	.LBB0_287
	jmp	.LBB0_697
.LBB0_521:
	xor	edi, edi
.LBB0_524:
	test	r9b, 1
	je	.LBB0_526
# %bb.525:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_526:
	cmp	rsi, r10
	jne	.LBB0_527
	jmp	.LBB0_697
.LBB0_687:
	xor	edi, edi
.LBB0_690:
	test	r9b, 1
	je	.LBB0_692
# %bb.691:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rdi], xmm0
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_692:
	cmp	rsi, r10
	jne	.LBB0_693
	jmp	.LBB0_697
.LBB0_182:
	xor	edi, edi
.LBB0_185:
	test	r9b, 1
	je	.LBB0_187
# %bb.186:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_187:
	cmp	rsi, r10
	jne	.LBB0_188
	jmp	.LBB0_697
.LBB0_355:
	xor	edi, edi
.LBB0_358:
	test	r9b, 1
	je	.LBB0_360
# %bb.359:
	movupd	xmm0, xmmword ptr [rdx + 8*rdi]
	movupd	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_360:
	cmp	rsi, r10
	jne	.LBB0_361
	jmp	.LBB0_697
.LBB0_402:
	xor	edi, edi
.LBB0_405:
	test	r9b, 1
	je	.LBB0_407
# %bb.406:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_407:
	cmp	rsi, r10
	jne	.LBB0_408
	jmp	.LBB0_697
.LBB0_568:
	xor	edi, edi
.LBB0_571:
	test	r9b, 1
	je	.LBB0_573
# %bb.572:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_573:
	cmp	rsi, r10
	jne	.LBB0_574
	jmp	.LBB0_697
.LBB0_63:
	xor	edi, edi
.LBB0_66:
	test	r9b, 1
	je	.LBB0_68
# %bb.67:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_68:
	cmp	rsi, r10
	jne	.LBB0_69
	jmp	.LBB0_697
.LBB0_236:
	xor	edi, edi
.LBB0_239:
	test	r9b, 1
	je	.LBB0_241
# %bb.240:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_241:
	cmp	rsi, r10
	jne	.LBB0_242
	jmp	.LBB0_697
.LBB0_476:
	xor	edi, edi
.LBB0_479:
	test	r9b, 1
	je	.LBB0_481
# %bb.480:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_481:
	cmp	rsi, r10
	jne	.LBB0_482
	jmp	.LBB0_697
.LBB0_642:
	xor	edi, edi
.LBB0_645:
	test	r9b, 1
	je	.LBB0_647
# %bb.646:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_647:
	cmp	rsi, r10
	jne	.LBB0_648
	jmp	.LBB0_697
.LBB0_137:
	xor	edi, edi
.LBB0_140:
	test	r9b, 1
	je	.LBB0_142
# %bb.141:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_142:
	cmp	rsi, r10
	jne	.LBB0_143
	jmp	.LBB0_697
.LBB0_310:
	xor	edi, edi
.LBB0_313:
	test	r9b, 1
	je	.LBB0_315
# %bb.314:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_315:
	cmp	rsi, r10
	jne	.LBB0_316
	jmp	.LBB0_697
.LBB0_418:
	xor	edi, edi
.LBB0_421:
	test	r9b, 1
	je	.LBB0_423
# %bb.422:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_423:
	cmp	rsi, r10
	jne	.LBB0_424
	jmp	.LBB0_697
.LBB0_434:
	xor	edi, edi
.LBB0_437:
	test	r9b, 1
	je	.LBB0_439
# %bb.438:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_439:
	cmp	rsi, r10
	jne	.LBB0_440
	jmp	.LBB0_697
.LBB0_584:
	xor	edi, edi
.LBB0_587:
	test	r9b, 1
	je	.LBB0_589
# %bb.588:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm0
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm1
.LBB0_589:
	cmp	rsi, r10
	jne	.LBB0_590
	jmp	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_79:
	xor	edi, edi
.LBB0_82:
	test	r9b, 1
	je	.LBB0_84
# %bb.83:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_84:
	cmp	rsi, r10
	jne	.LBB0_85
	jmp	.LBB0_697
.LBB0_95:
	xor	edi, edi
.LBB0_98:
	test	r9b, 1
	je	.LBB0_100
# %bb.99:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_100:
	cmp	rsi, r10
	jne	.LBB0_101
	jmp	.LBB0_697
.LBB0_252:
	xor	edi, edi
.LBB0_255:
	test	r9b, 1
	je	.LBB0_257
# %bb.256:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_257:
	cmp	rsi, r10
	jne	.LBB0_258
	jmp	.LBB0_697
.LBB0_268:
	xor	edi, edi
.LBB0_271:
	test	r9b, 1
	je	.LBB0_273
# %bb.272:
	movdqu	xmm0, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rdi], xmm2
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB0_273:
	cmp	rsi, r10
	jne	.LBB0_274
	jmp	.LBB0_697
.LBB0_492:
	xor	edi, edi
.LBB0_495:
	test	r9b, 1
	je	.LBB0_497
# %bb.496:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm0
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB0_497:
	cmp	rsi, r10
	jne	.LBB0_498
	jmp	.LBB0_697
.LBB0_508:
	xor	edi, edi
.LBB0_511:
	test	r9b, 1
	je	.LBB0_513
# %bb.512:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_513:
	cmp	rsi, r10
	jne	.LBB0_514
	jmp	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_674:
	xor	edi, edi
.LBB0_677:
	test	r9b, 1
	je	.LBB0_679
# %bb.678:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rdi], xmm0
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_679:
	cmp	rsi, r10
	jne	.LBB0_680
	jmp	.LBB0_697
.LBB0_153:
	xor	edi, edi
.LBB0_156:
	test	r9b, 1
	je	.LBB0_158
# %bb.157:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_158:
	cmp	rsi, r10
	jne	.LBB0_159
	jmp	.LBB0_697
.LBB0_169:
	xor	edi, edi
.LBB0_172:
	test	r9b, 1
	je	.LBB0_174
# %bb.173:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_174:
	cmp	rsi, r10
	jne	.LBB0_175
	jmp	.LBB0_697
.LBB0_326:
	xor	edi, edi
.LBB0_329:
	test	r9b, 1
	je	.LBB0_331
# %bb.330:
	movdqu	xmm0, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rdi], xmm2
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB0_331:
	cmp	rsi, r10
	jne	.LBB0_332
	jmp	.LBB0_697
.LBB0_342:
	xor	edi, edi
.LBB0_345:
	test	r9b, 1
	je	.LBB0_347
# %bb.346:
	movups	xmm0, xmmword ptr [rdx + 4*rdi]
	movups	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_347:
	cmp	rsi, r10
	jne	.LBB0_348
	jmp	.LBB0_697
.LBB0_389:
	xor	edi, edi
.LBB0_392:
	test	r9b, 1
	je	.LBB0_394
# %bb.393:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_394:
	cmp	rsi, r10
	jne	.LBB0_395
	jmp	.LBB0_697
.LBB0_555:
	xor	edi, edi
.LBB0_558:
	test	r9b, 1
	je	.LBB0_560
# %bb.559:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm0
	movdqu	xmmword ptr [r8 + rdi + 16], xmm1
.LBB0_560:
	cmp	rsi, r10
	jne	.LBB0_561
	jmp	.LBB0_697
.LBB0_50:
	xor	edi, edi
.LBB0_53:
	test	r9b, 1
	je	.LBB0_55
# %bb.54:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_55:
	cmp	rsi, r10
	jne	.LBB0_56
	jmp	.LBB0_697
.LBB0_223:
	xor	edi, edi
.LBB0_226:
	test	r9b, 1
	je	.LBB0_228
# %bb.227:
	movdqu	xmm0, xmmword ptr [rdx + rdi]
	movdqu	xmm1, xmmword ptr [rdx + rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + rdi]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rdi + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rdi], xmm2
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB0_228:
	cmp	rsi, r10
	jne	.LBB0_229
	jmp	.LBB0_697
.LBB0_463:
	xor	edi, edi
.LBB0_466:
	test	r9b, 1
	je	.LBB0_468
# %bb.467:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm0
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB0_468:
	cmp	rsi, r10
	jne	.LBB0_469
	jmp	.LBB0_697
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
	jmp	.LBB0_697
.LBB0_124:
	xor	edi, edi
.LBB0_127:
	test	r9b, 1
	je	.LBB0_129
# %bb.128:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_129:
	cmp	rsi, r10
	jne	.LBB0_130
	jmp	.LBB0_697
.LBB0_297:
	xor	edi, edi
.LBB0_300:
	test	r9b, 1
	je	.LBB0_302
# %bb.301:
	movdqu	xmm0, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rdi], xmm2
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB0_302:
	cmp	rsi, r10
	jne	.LBB0_303
.LBB0_697:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	arithmetic_sse4, .Lfunc_end0-arithmetic_sse4
                                        # -- End function
	.globl	arithmetic_arr_scalar_sse4      # -- Begin function arithmetic_arr_scalar_sse4
	.p2align	4, 0x90
	.type	arithmetic_arr_scalar_sse4,@function
arithmetic_arr_scalar_sse4:             # @arithmetic_arr_scalar_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 1
	jg	.LBB1_11
# %bb.1:
	test	sil, sil
	je	.LBB1_21
# %bb.2:
	cmp	sil, 1
	jne	.LBB1_737
# %bb.3:
	cmp	edi, 6
	jg	.LBB1_37
# %bb.4:
	cmp	edi, 3
	jle	.LBB1_65
# %bb.5:
	cmp	edi, 4
	je	.LBB1_105
# %bb.6:
	cmp	edi, 5
	je	.LBB1_108
# %bb.7:
	cmp	edi, 6
	jne	.LBB1_737
# %bb.8:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.9:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_10
# %bb.177:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_297
# %bb.178:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_297
.LBB1_10:
	xor	esi, esi
.LBB1_421:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_423
.LBB1_422:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_422
.LBB1_423:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_424:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_424
	jmp	.LBB1_737
.LBB1_11:
	cmp	sil, 2
	je	.LBB1_29
# %bb.12:
	cmp	sil, 3
	jne	.LBB1_737
# %bb.13:
	cmp	edi, 6
	jg	.LBB1_44
# %bb.14:
	cmp	edi, 3
	jle	.LBB1_70
# %bb.15:
	cmp	edi, 4
	je	.LBB1_111
# %bb.16:
	cmp	edi, 5
	je	.LBB1_114
# %bb.17:
	cmp	edi, 6
	jne	.LBB1_737
# %bb.18:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.19:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_20
# %bb.180:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_300
# %bb.181:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_300
.LBB1_20:
	xor	esi, esi
.LBB1_429:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_431
.LBB1_430:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_430
.LBB1_431:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_432:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_432
	jmp	.LBB1_737
.LBB1_21:
	cmp	edi, 6
	jg	.LBB1_51
# %bb.22:
	cmp	edi, 3
	jle	.LBB1_75
# %bb.23:
	cmp	edi, 4
	je	.LBB1_117
# %bb.24:
	cmp	edi, 5
	je	.LBB1_120
# %bb.25:
	cmp	edi, 6
	jne	.LBB1_737
# %bb.26:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.27:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_28
# %bb.183:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_303
# %bb.184:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_303
.LBB1_28:
	xor	esi, esi
.LBB1_437:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_439
.LBB1_438:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_438
.LBB1_439:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_440:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_440
	jmp	.LBB1_737
.LBB1_29:
	cmp	edi, 6
	jg	.LBB1_58
# %bb.30:
	cmp	edi, 3
	jle	.LBB1_80
# %bb.31:
	cmp	edi, 4
	je	.LBB1_123
# %bb.32:
	cmp	edi, 5
	je	.LBB1_126
# %bb.33:
	cmp	edi, 6
	jne	.LBB1_737
# %bb.34:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.35:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_36
# %bb.186:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_306
# %bb.187:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_306
.LBB1_36:
	xor	esi, esi
.LBB1_445:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_447
.LBB1_446:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_446
.LBB1_447:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_448:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_448
	jmp	.LBB1_737
.LBB1_37:
	cmp	edi, 8
	jle	.LBB1_85
# %bb.38:
	cmp	edi, 9
	je	.LBB1_129
# %bb.39:
	cmp	edi, 11
	je	.LBB1_132
# %bb.40:
	cmp	edi, 12
	jne	.LBB1_737
# %bb.41:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.42:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_43
# %bb.189:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_309
# %bb.190:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_309
.LBB1_43:
	xor	ecx, ecx
.LBB1_453:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_455
.LBB1_454:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_454
.LBB1_455:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_456
	jmp	.LBB1_737
.LBB1_44:
	cmp	edi, 8
	jle	.LBB1_90
# %bb.45:
	cmp	edi, 9
	je	.LBB1_135
# %bb.46:
	cmp	edi, 11
	je	.LBB1_138
# %bb.47:
	cmp	edi, 12
	jne	.LBB1_737
# %bb.48:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.49:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_50
# %bb.192:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_312
# %bb.193:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_312
.LBB1_50:
	xor	ecx, ecx
.LBB1_461:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_463
.LBB1_462:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_462
.LBB1_463:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_464:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_464
	jmp	.LBB1_737
.LBB1_51:
	cmp	edi, 8
	jle	.LBB1_95
# %bb.52:
	cmp	edi, 9
	je	.LBB1_141
# %bb.53:
	cmp	edi, 11
	je	.LBB1_144
# %bb.54:
	cmp	edi, 12
	jne	.LBB1_737
# %bb.55:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.56:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_57
# %bb.195:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_315
# %bb.196:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_315
.LBB1_57:
	xor	ecx, ecx
.LBB1_469:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_471
.LBB1_470:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_470
.LBB1_471:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_472:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_472
	jmp	.LBB1_737
.LBB1_58:
	cmp	edi, 8
	jle	.LBB1_100
# %bb.59:
	cmp	edi, 9
	je	.LBB1_147
# %bb.60:
	cmp	edi, 11
	je	.LBB1_150
# %bb.61:
	cmp	edi, 12
	jne	.LBB1_737
# %bb.62:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.63:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_64
# %bb.198:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_318
# %bb.199:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_318
.LBB1_64:
	xor	ecx, ecx
.LBB1_477:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_479
.LBB1_478:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_478
.LBB1_479:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_480:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_480
	jmp	.LBB1_737
.LBB1_65:
	cmp	edi, 2
	je	.LBB1_153
# %bb.66:
	cmp	edi, 3
	jne	.LBB1_737
# %bb.67:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.68:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_69
# %bb.201:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_321
# %bb.202:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_321
.LBB1_69:
	xor	esi, esi
.LBB1_485:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_487
.LBB1_486:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_486
.LBB1_487:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_488:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_488
	jmp	.LBB1_737
.LBB1_70:
	cmp	edi, 2
	je	.LBB1_156
# %bb.71:
	cmp	edi, 3
	jne	.LBB1_737
# %bb.72:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.73:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_74
# %bb.204:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_324
# %bb.205:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_324
.LBB1_74:
	xor	esi, esi
.LBB1_493:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_495
.LBB1_494:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_494
.LBB1_495:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_496:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_496
	jmp	.LBB1_737
.LBB1_75:
	cmp	edi, 2
	je	.LBB1_159
# %bb.76:
	cmp	edi, 3
	jne	.LBB1_737
# %bb.77:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.78:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_79
# %bb.207:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_327
# %bb.208:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_327
.LBB1_79:
	xor	esi, esi
.LBB1_501:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_503
.LBB1_502:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_502
.LBB1_503:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_504:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_504
	jmp	.LBB1_737
.LBB1_80:
	cmp	edi, 2
	je	.LBB1_162
# %bb.81:
	cmp	edi, 3
	jne	.LBB1_737
# %bb.82:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.83:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_84
# %bb.210:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_330
# %bb.211:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_330
.LBB1_84:
	xor	esi, esi
.LBB1_509:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_511
.LBB1_510:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_510
.LBB1_511:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_512:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_512
	jmp	.LBB1_737
.LBB1_85:
	cmp	edi, 7
	je	.LBB1_165
# %bb.86:
	cmp	edi, 8
	jne	.LBB1_737
# %bb.87:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.88:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_89
# %bb.213:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_333
# %bb.214:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_333
.LBB1_89:
	xor	esi, esi
.LBB1_517:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_519
.LBB1_518:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_518
.LBB1_519:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_520:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_520
	jmp	.LBB1_737
.LBB1_90:
	cmp	edi, 7
	je	.LBB1_168
# %bb.91:
	cmp	edi, 8
	jne	.LBB1_737
# %bb.92:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.93:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_94
# %bb.216:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_336
# %bb.217:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_336
.LBB1_94:
	xor	esi, esi
.LBB1_525:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_527
.LBB1_526:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_526
.LBB1_527:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_528:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_528
	jmp	.LBB1_737
.LBB1_95:
	cmp	edi, 7
	je	.LBB1_171
# %bb.96:
	cmp	edi, 8
	jne	.LBB1_737
# %bb.97:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.98:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_99
# %bb.219:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_339
# %bb.220:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_339
.LBB1_99:
	xor	esi, esi
.LBB1_533:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_535
.LBB1_534:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_534
.LBB1_535:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_536:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_536
	jmp	.LBB1_737
.LBB1_100:
	cmp	edi, 7
	je	.LBB1_174
# %bb.101:
	cmp	edi, 8
	jne	.LBB1_737
# %bb.102:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.103:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_104
# %bb.222:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_342
# %bb.223:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_342
.LBB1_104:
	xor	esi, esi
.LBB1_541:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_543
.LBB1_542:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_542
.LBB1_543:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_544:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_544
	jmp	.LBB1_737
.LBB1_105:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.106:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_107
# %bb.225:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_345
# %bb.226:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_345
.LBB1_107:
	xor	esi, esi
.LBB1_549:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_551
.LBB1_550:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_550
.LBB1_551:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_552:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_552
	jmp	.LBB1_737
.LBB1_108:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.109:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_110
# %bb.228:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_348
# %bb.229:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_348
.LBB1_110:
	xor	esi, esi
.LBB1_557:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_559
.LBB1_558:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_558
.LBB1_559:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_560:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_560
	jmp	.LBB1_737
.LBB1_111:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.112:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_113
# %bb.231:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_351
# %bb.232:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_351
.LBB1_113:
	xor	esi, esi
.LBB1_565:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_567
.LBB1_566:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_566
.LBB1_567:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_568:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_568
	jmp	.LBB1_737
.LBB1_114:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.115:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_116
# %bb.234:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_354
# %bb.235:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_354
.LBB1_116:
	xor	esi, esi
.LBB1_573:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_575
.LBB1_574:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	sub	ecx, eax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_574
.LBB1_575:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_576:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_576
	jmp	.LBB1_737
.LBB1_117:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.118:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_119
# %bb.237:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_357
# %bb.238:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_357
.LBB1_119:
	xor	esi, esi
.LBB1_581:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_583
.LBB1_582:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_582
.LBB1_583:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_584:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_584
	jmp	.LBB1_737
.LBB1_120:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.121:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_122
# %bb.240:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_360
# %bb.241:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_360
.LBB1_122:
	xor	esi, esi
.LBB1_589:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_591
.LBB1_590:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_590
.LBB1_591:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_592:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_592
	jmp	.LBB1_737
.LBB1_123:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.124:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_125
# %bb.243:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_363
# %bb.244:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_363
.LBB1_125:
	xor	esi, esi
.LBB1_597:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_599
.LBB1_598:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_598
.LBB1_599:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_600:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_600
	jmp	.LBB1_737
.LBB1_126:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.127:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_128
# %bb.246:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_366
# %bb.247:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_366
.LBB1_128:
	xor	esi, esi
.LBB1_605:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_607
.LBB1_606:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, word ptr [rdx + 2*rsi]
	add	cx, ax
	mov	word ptr [r8 + 2*rsi], cx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_606
.LBB1_607:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_608
	jmp	.LBB1_737
.LBB1_129:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.130:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_131
# %bb.249:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_369
# %bb.250:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_369
.LBB1_131:
	xor	esi, esi
.LBB1_613:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_615
.LBB1_614:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_614
.LBB1_615:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_616:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_616
	jmp	.LBB1_737
.LBB1_132:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.133:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_134
# %bb.252:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_372
# %bb.253:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_372
.LBB1_134:
	xor	ecx, ecx
.LBB1_621:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_623
.LBB1_622:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_622
.LBB1_623:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_624:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_624
	jmp	.LBB1_737
.LBB1_135:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.136:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_137
# %bb.255:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_375
# %bb.256:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_375
.LBB1_137:
	xor	esi, esi
.LBB1_629:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_631
.LBB1_630:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	sub	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_630
.LBB1_631:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_632:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_632
	jmp	.LBB1_737
.LBB1_138:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.139:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_140
# %bb.258:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_378
# %bb.259:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_378
.LBB1_140:
	xor	ecx, ecx
.LBB1_637:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_639
.LBB1_638:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_638
.LBB1_639:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_640:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_640
	jmp	.LBB1_737
.LBB1_141:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.142:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_143
# %bb.261:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_381
# %bb.262:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_381
.LBB1_143:
	xor	esi, esi
.LBB1_645:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_647
.LBB1_646:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_646
.LBB1_647:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_648:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_648
	jmp	.LBB1_737
.LBB1_144:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.145:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_146
# %bb.264:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_384
# %bb.265:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_384
.LBB1_146:
	xor	ecx, ecx
.LBB1_653:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_655
.LBB1_654:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_654
.LBB1_655:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_656:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_656
	jmp	.LBB1_737
.LBB1_147:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.148:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_149
# %bb.267:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_387
# %bb.268:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_387
.LBB1_149:
	xor	esi, esi
.LBB1_661:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_663
.LBB1_662:                              # =>This Inner Loop Header: Depth=1
	mov	rcx, qword ptr [rdx + 8*rsi]
	add	rcx, rax
	mov	qword ptr [r8 + 8*rsi], rcx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_662
.LBB1_663:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_664:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_664
	jmp	.LBB1_737
.LBB1_150:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.151:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_152
# %bb.270:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_390
# %bb.271:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_390
.LBB1_152:
	xor	ecx, ecx
.LBB1_669:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_671
.LBB1_670:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_670
.LBB1_671:
	cmp	rsi, 3
	jb	.LBB1_737
.LBB1_672:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_672
	jmp	.LBB1_737
.LBB1_153:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.154:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_155
# %bb.273:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_393
# %bb.274:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_393
.LBB1_155:
	xor	esi, esi
.LBB1_677:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_679
.LBB1_678:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_678
.LBB1_679:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_680:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_680
	jmp	.LBB1_737
.LBB1_156:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.157:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_158
# %bb.276:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_396
# %bb.277:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_396
.LBB1_158:
	xor	esi, esi
.LBB1_685:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_687
.LBB1_686:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	sub	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_686
.LBB1_687:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_688:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_688
	jmp	.LBB1_737
.LBB1_159:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.160:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_161
# %bb.279:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_399
# %bb.280:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_399
.LBB1_161:
	xor	esi, esi
.LBB1_693:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_695
.LBB1_694:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_694
.LBB1_695:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_696:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_696
	jmp	.LBB1_737
.LBB1_162:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.163:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_164
# %bb.282:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_402
# %bb.283:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_402
.LBB1_164:
	xor	esi, esi
.LBB1_701:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_703
.LBB1_702:                              # =>This Inner Loop Header: Depth=1
	movzx	ecx, byte ptr [rdx + rsi]
	add	cl, al
	mov	byte ptr [r8 + rsi], cl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_702
.LBB1_703:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_704:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_704
	jmp	.LBB1_737
.LBB1_165:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.166:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_167
# %bb.285:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_405
# %bb.286:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_405
.LBB1_167:
	xor	esi, esi
.LBB1_709:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_711
.LBB1_710:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_710
.LBB1_711:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_712:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_712
	jmp	.LBB1_737
.LBB1_168:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.169:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_170
# %bb.288:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_408
# %bb.289:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_408
.LBB1_170:
	xor	esi, esi
.LBB1_717:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_719
.LBB1_718:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	sub	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_718
.LBB1_719:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_720:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_720
	jmp	.LBB1_737
.LBB1_171:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.172:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_173
# %bb.291:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_411
# %bb.292:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_411
.LBB1_173:
	xor	esi, esi
.LBB1_725:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_727
.LBB1_726:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_726
.LBB1_727:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_728:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_728
	jmp	.LBB1_737
.LBB1_174:
	test	r9d, r9d
	jle	.LBB1_737
# %bb.175:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_176
# %bb.294:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_414
# %bb.295:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_414
.LBB1_176:
	xor	esi, esi
.LBB1_733:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB1_735
.LBB1_734:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rdx + 4*rsi]
	add	ecx, eax
	mov	dword ptr [r8 + 4*rsi], ecx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB1_734
.LBB1_735:
	cmp	r9, 3
	jb	.LBB1_737
.LBB1_736:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_736
	jmp	.LBB1_737
.LBB1_297:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_417
# %bb.298:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_299:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_299
	jmp	.LBB1_418
.LBB1_300:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_425
# %bb.301:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_302:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_302
	jmp	.LBB1_426
.LBB1_303:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_433
# %bb.304:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_305:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_305
	jmp	.LBB1_434
.LBB1_306:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_441
# %bb.307:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_308:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_308
	jmp	.LBB1_442
.LBB1_309:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_449
# %bb.310:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_311:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_311
	jmp	.LBB1_450
.LBB1_312:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_457
# %bb.313:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_314:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_314
	jmp	.LBB1_458
.LBB1_315:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_465
# %bb.316:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_317:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_317
	jmp	.LBB1_466
.LBB1_318:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_473
# %bb.319:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_320:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_320
	jmp	.LBB1_474
.LBB1_321:
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
	je	.LBB1_481
# %bb.322:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_323:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_323
	jmp	.LBB1_482
.LBB1_324:
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
	je	.LBB1_489
# %bb.325:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_326:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_326
	jmp	.LBB1_490
.LBB1_327:
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
	je	.LBB1_497
# %bb.328:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_329:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_329
	jmp	.LBB1_498
.LBB1_330:
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
	je	.LBB1_505
# %bb.331:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_332:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_332
	jmp	.LBB1_506
.LBB1_333:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_513
# %bb.334:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_335:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_335
	jmp	.LBB1_514
.LBB1_336:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_521
# %bb.337:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_338:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_338
	jmp	.LBB1_522
.LBB1_339:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_529
# %bb.340:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_341:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_341
	jmp	.LBB1_530
.LBB1_342:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_537
# %bb.343:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_344:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_344
	jmp	.LBB1_538
.LBB1_345:
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
	je	.LBB1_545
# %bb.346:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_347:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_347
	jmp	.LBB1_546
.LBB1_348:
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
	je	.LBB1_553
# %bb.349:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_350:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_350
	jmp	.LBB1_554
.LBB1_351:
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
	je	.LBB1_561
# %bb.352:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_353:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_353
	jmp	.LBB1_562
.LBB1_354:
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
	je	.LBB1_569
# %bb.355:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_356:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_356
	jmp	.LBB1_570
.LBB1_357:
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
	je	.LBB1_577
# %bb.358:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_359:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_359
	jmp	.LBB1_578
.LBB1_360:
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
	je	.LBB1_585
# %bb.361:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_362:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_362
	jmp	.LBB1_586
.LBB1_363:
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
	je	.LBB1_593
# %bb.364:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_365:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_365
	jmp	.LBB1_594
.LBB1_366:
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
	je	.LBB1_601
# %bb.367:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_368:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_368
	jmp	.LBB1_602
.LBB1_369:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_609
# %bb.370:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_371:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_371
	jmp	.LBB1_610
.LBB1_372:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_617
# %bb.373:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_374:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_374
	jmp	.LBB1_618
.LBB1_375:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_625
# %bb.376:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_377
	jmp	.LBB1_626
.LBB1_378:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_633
# %bb.379:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_380:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_380
	jmp	.LBB1_634
.LBB1_381:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_641
# %bb.382:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_383:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_383
	jmp	.LBB1_642
.LBB1_384:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_649
# %bb.385:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_386:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_386
	jmp	.LBB1_650
.LBB1_387:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_657
# %bb.388:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_389:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_389
	jmp	.LBB1_658
.LBB1_390:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB1_665
# %bb.391:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_392:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_392
	jmp	.LBB1_666
.LBB1_393:
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
	je	.LBB1_673
# %bb.394:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_395:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_395
	jmp	.LBB1_674
.LBB1_396:
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
	je	.LBB1_681
# %bb.397:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_398:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_398
	jmp	.LBB1_682
.LBB1_399:
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
	je	.LBB1_689
# %bb.400:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_401:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_401
	jmp	.LBB1_690
.LBB1_402:
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
	je	.LBB1_697
# %bb.403:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_404:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_404
	jmp	.LBB1_698
.LBB1_405:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_705
# %bb.406:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_407:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_407
	jmp	.LBB1_706
.LBB1_408:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_713
# %bb.409:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_410:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_410
	jmp	.LBB1_714
.LBB1_411:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_721
# %bb.412:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_413:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_413
	jmp	.LBB1_722
.LBB1_414:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_729
# %bb.415:
	mov	rcx, r9
	and	rcx, -2
	neg	rcx
	xor	edi, edi
.LBB1_416:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_416
	jmp	.LBB1_730
.LBB1_417:
	xor	edi, edi
.LBB1_418:
	test	r9b, 1
	je	.LBB1_420
# %bb.419:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_420:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_421
.LBB1_425:
	xor	edi, edi
.LBB1_426:
	test	r9b, 1
	je	.LBB1_428
# %bb.427:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_428:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_429
.LBB1_433:
	xor	edi, edi
.LBB1_434:
	test	r9b, 1
	je	.LBB1_436
# %bb.435:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_436:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_437
.LBB1_441:
	xor	edi, edi
.LBB1_442:
	test	r9b, 1
	je	.LBB1_444
# %bb.443:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_444:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_445
.LBB1_449:
	xor	edi, edi
.LBB1_450:
	test	r9b, 1
	je	.LBB1_452
# %bb.451:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_452:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_453
.LBB1_457:
	xor	edi, edi
.LBB1_458:
	test	r9b, 1
	je	.LBB1_460
# %bb.459:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_460:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_461
.LBB1_465:
	xor	edi, edi
.LBB1_466:
	test	r9b, 1
	je	.LBB1_468
# %bb.467:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_468:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_469
.LBB1_473:
	xor	edi, edi
.LBB1_474:
	test	r9b, 1
	je	.LBB1_476
# %bb.475:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_476:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_477
.LBB1_481:
	xor	edi, edi
.LBB1_482:
	test	r9b, 1
	je	.LBB1_484
# %bb.483:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_484:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_485
.LBB1_489:
	xor	edi, edi
.LBB1_490:
	test	r9b, 1
	je	.LBB1_492
# %bb.491:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_492:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_493
.LBB1_497:
	xor	edi, edi
.LBB1_498:
	test	r9b, 1
	je	.LBB1_500
# %bb.499:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_500:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_501
.LBB1_505:
	xor	edi, edi
.LBB1_506:
	test	r9b, 1
	je	.LBB1_508
# %bb.507:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_508:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_509
.LBB1_513:
	xor	edi, edi
.LBB1_514:
	test	r9b, 1
	je	.LBB1_516
# %bb.515:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_516:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_517
.LBB1_521:
	xor	edi, edi
.LBB1_522:
	test	r9b, 1
	je	.LBB1_524
# %bb.523:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_524:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_525
.LBB1_529:
	xor	edi, edi
.LBB1_530:
	test	r9b, 1
	je	.LBB1_532
# %bb.531:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_532:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_533
.LBB1_537:
	xor	edi, edi
.LBB1_538:
	test	r9b, 1
	je	.LBB1_540
# %bb.539:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_540:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_541
.LBB1_545:
	xor	edi, edi
.LBB1_546:
	test	r9b, 1
	je	.LBB1_548
# %bb.547:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_548:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_549
.LBB1_553:
	xor	edi, edi
.LBB1_554:
	test	r9b, 1
	je	.LBB1_556
# %bb.555:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_556:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_557
.LBB1_561:
	xor	edi, edi
.LBB1_562:
	test	r9b, 1
	je	.LBB1_564
# %bb.563:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_564:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_565
.LBB1_569:
	xor	edi, edi
.LBB1_570:
	test	r9b, 1
	je	.LBB1_572
# %bb.571:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_572:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_573
.LBB1_577:
	xor	edi, edi
.LBB1_578:
	test	r9b, 1
	je	.LBB1_580
# %bb.579:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_580:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_581
.LBB1_585:
	xor	edi, edi
.LBB1_586:
	test	r9b, 1
	je	.LBB1_588
# %bb.587:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_588:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_589
.LBB1_593:
	xor	edi, edi
.LBB1_594:
	test	r9b, 1
	je	.LBB1_596
# %bb.595:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_596:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_597
.LBB1_601:
	xor	edi, edi
.LBB1_602:
	test	r9b, 1
	je	.LBB1_604
# %bb.603:
	movdqu	xmm1, xmmword ptr [rdx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB1_604:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_605
.LBB1_609:
	xor	edi, edi
.LBB1_610:
	test	r9b, 1
	je	.LBB1_612
# %bb.611:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_612:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_613
.LBB1_617:
	xor	edi, edi
.LBB1_618:
	test	r9b, 1
	je	.LBB1_620
# %bb.619:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_620:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_621
.LBB1_625:
	xor	edi, edi
.LBB1_626:
	test	r9b, 1
	je	.LBB1_628
# %bb.627:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_628:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_629
.LBB1_633:
	xor	edi, edi
.LBB1_634:
	test	r9b, 1
	je	.LBB1_636
# %bb.635:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_636:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_637
.LBB1_641:
	xor	edi, edi
.LBB1_642:
	test	r9b, 1
	je	.LBB1_644
# %bb.643:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_644:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_645
.LBB1_649:
	xor	edi, edi
.LBB1_650:
	test	r9b, 1
	je	.LBB1_652
# %bb.651:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_652:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_653
.LBB1_657:
	xor	edi, edi
.LBB1_658:
	test	r9b, 1
	je	.LBB1_660
# %bb.659:
	movdqu	xmm1, xmmword ptr [rdx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB1_660:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_661
.LBB1_665:
	xor	edi, edi
.LBB1_666:
	test	r9b, 1
	je	.LBB1_668
# %bb.667:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_668:
	cmp	rcx, rax
	je	.LBB1_737
	jmp	.LBB1_669
.LBB1_673:
	xor	edi, edi
.LBB1_674:
	test	r9b, 1
	je	.LBB1_676
# %bb.675:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_676:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_677
.LBB1_681:
	xor	edi, edi
.LBB1_682:
	test	r9b, 1
	je	.LBB1_684
# %bb.683:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_684:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_685
.LBB1_689:
	xor	edi, edi
.LBB1_690:
	test	r9b, 1
	je	.LBB1_692
# %bb.691:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_692:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_693
.LBB1_697:
	xor	edi, edi
.LBB1_698:
	test	r9b, 1
	je	.LBB1_700
# %bb.699:
	movdqu	xmm1, xmmword ptr [rdx + rdi]
	movdqu	xmm2, xmmword ptr [rdx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB1_700:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_701
.LBB1_705:
	xor	edi, edi
.LBB1_706:
	test	r9b, 1
	je	.LBB1_708
# %bb.707:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_708:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_709
.LBB1_713:
	xor	edi, edi
.LBB1_714:
	test	r9b, 1
	je	.LBB1_716
# %bb.715:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_716:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_717
.LBB1_721:
	xor	edi, edi
.LBB1_722:
	test	r9b, 1
	je	.LBB1_724
# %bb.723:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_724:
	cmp	rsi, r10
	je	.LBB1_737
	jmp	.LBB1_725
.LBB1_729:
	xor	edi, edi
.LBB1_730:
	test	r9b, 1
	je	.LBB1_732
# %bb.731:
	movdqu	xmm1, xmmword ptr [rdx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rdx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB1_732:
	cmp	rsi, r10
	jne	.LBB1_733
.LBB1_737:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end1:
	.size	arithmetic_arr_scalar_sse4, .Lfunc_end1-arithmetic_arr_scalar_sse4
                                        # -- End function
	.globl	arithmetic_scalar_arr_sse4      # -- Begin function arithmetic_scalar_arr_sse4
	.p2align	4, 0x90
	.type	arithmetic_scalar_arr_sse4,@function
arithmetic_scalar_arr_sse4:             # @arithmetic_scalar_arr_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	sil, 1
	jg	.LBB2_11
# %bb.1:
	test	sil, sil
	je	.LBB2_21
# %bb.2:
	cmp	sil, 1
	jne	.LBB2_737
# %bb.3:
	cmp	edi, 6
	jg	.LBB2_37
# %bb.4:
	cmp	edi, 3
	jle	.LBB2_65
# %bb.5:
	cmp	edi, 4
	je	.LBB2_105
# %bb.6:
	cmp	edi, 5
	je	.LBB2_108
# %bb.7:
	cmp	edi, 6
	jne	.LBB2_737
# %bb.8:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.9:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_10
# %bb.177:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_297
# %bb.178:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_297
.LBB2_10:
	xor	esi, esi
.LBB2_421:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_423
.LBB2_422:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_422
.LBB2_423:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_424:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_424
	jmp	.LBB2_737
.LBB2_11:
	cmp	sil, 2
	je	.LBB2_29
# %bb.12:
	cmp	sil, 3
	jne	.LBB2_737
# %bb.13:
	cmp	edi, 6
	jg	.LBB2_44
# %bb.14:
	cmp	edi, 3
	jle	.LBB2_70
# %bb.15:
	cmp	edi, 4
	je	.LBB2_111
# %bb.16:
	cmp	edi, 5
	je	.LBB2_114
# %bb.17:
	cmp	edi, 6
	jne	.LBB2_737
# %bb.18:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.19:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_20
# %bb.180:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_300
# %bb.181:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_300
.LBB2_20:
	xor	esi, esi
.LBB2_429:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_431
.LBB2_430:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_430
.LBB2_431:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_432:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_432
	jmp	.LBB2_737
.LBB2_21:
	cmp	edi, 6
	jg	.LBB2_51
# %bb.22:
	cmp	edi, 3
	jle	.LBB2_75
# %bb.23:
	cmp	edi, 4
	je	.LBB2_117
# %bb.24:
	cmp	edi, 5
	je	.LBB2_120
# %bb.25:
	cmp	edi, 6
	jne	.LBB2_737
# %bb.26:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.27:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_28
# %bb.183:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_303
# %bb.184:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_303
.LBB2_28:
	xor	esi, esi
.LBB2_437:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_439
.LBB2_438:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_438
.LBB2_439:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_440:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_440
	jmp	.LBB2_737
.LBB2_29:
	cmp	edi, 6
	jg	.LBB2_58
# %bb.30:
	cmp	edi, 3
	jle	.LBB2_80
# %bb.31:
	cmp	edi, 4
	je	.LBB2_123
# %bb.32:
	cmp	edi, 5
	je	.LBB2_126
# %bb.33:
	cmp	edi, 6
	jne	.LBB2_737
# %bb.34:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.35:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_36
# %bb.186:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_306
# %bb.187:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_306
.LBB2_36:
	xor	esi, esi
.LBB2_445:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_447
.LBB2_446:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_446
.LBB2_447:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_448:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_448
	jmp	.LBB2_737
.LBB2_37:
	cmp	edi, 8
	jle	.LBB2_85
# %bb.38:
	cmp	edi, 9
	je	.LBB2_129
# %bb.39:
	cmp	edi, 11
	je	.LBB2_132
# %bb.40:
	cmp	edi, 12
	jne	.LBB2_737
# %bb.41:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.42:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_43
# %bb.189:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_309
# %bb.190:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_309
.LBB2_43:
	xor	edx, edx
.LBB2_453:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_455
.LBB2_454:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_454
.LBB2_455:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_456
	jmp	.LBB2_737
.LBB2_44:
	cmp	edi, 8
	jle	.LBB2_90
# %bb.45:
	cmp	edi, 9
	je	.LBB2_135
# %bb.46:
	cmp	edi, 11
	je	.LBB2_138
# %bb.47:
	cmp	edi, 12
	jne	.LBB2_737
# %bb.48:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.49:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_50
# %bb.192:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_312
# %bb.193:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_312
.LBB2_50:
	xor	edx, edx
.LBB2_461:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_463
.LBB2_462:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_462
.LBB2_463:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_464:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_464
	jmp	.LBB2_737
.LBB2_51:
	cmp	edi, 8
	jle	.LBB2_95
# %bb.52:
	cmp	edi, 9
	je	.LBB2_141
# %bb.53:
	cmp	edi, 11
	je	.LBB2_144
# %bb.54:
	cmp	edi, 12
	jne	.LBB2_737
# %bb.55:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.56:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_57
# %bb.195:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_315
# %bb.196:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_315
.LBB2_57:
	xor	edx, edx
.LBB2_469:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_471
.LBB2_470:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_470
.LBB2_471:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_472:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_472
	jmp	.LBB2_737
.LBB2_58:
	cmp	edi, 8
	jle	.LBB2_100
# %bb.59:
	cmp	edi, 9
	je	.LBB2_147
# %bb.60:
	cmp	edi, 11
	je	.LBB2_150
# %bb.61:
	cmp	edi, 12
	jne	.LBB2_737
# %bb.62:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.63:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_64
# %bb.198:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_318
# %bb.199:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_318
.LBB2_64:
	xor	edx, edx
.LBB2_477:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_479
.LBB2_478:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rdx]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_478
.LBB2_479:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_480:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_480
	jmp	.LBB2_737
.LBB2_65:
	cmp	edi, 2
	je	.LBB2_153
# %bb.66:
	cmp	edi, 3
	jne	.LBB2_737
# %bb.67:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.68:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_69
# %bb.201:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_321
# %bb.202:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_321
.LBB2_69:
	xor	esi, esi
.LBB2_485:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_487
.LBB2_486:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_486
.LBB2_487:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_488:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_488
	jmp	.LBB2_737
.LBB2_70:
	cmp	edi, 2
	je	.LBB2_156
# %bb.71:
	cmp	edi, 3
	jne	.LBB2_737
# %bb.72:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.73:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_74
# %bb.204:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_324
# %bb.205:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_324
.LBB2_74:
	xor	esi, esi
.LBB2_493:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_495
.LBB2_494:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_494
.LBB2_495:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_496:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_496
	jmp	.LBB2_737
.LBB2_75:
	cmp	edi, 2
	je	.LBB2_159
# %bb.76:
	cmp	edi, 3
	jne	.LBB2_737
# %bb.77:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.78:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_79
# %bb.207:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_327
# %bb.208:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_327
.LBB2_79:
	xor	esi, esi
.LBB2_501:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_503
.LBB2_502:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_502
.LBB2_503:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_504:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_504
	jmp	.LBB2_737
.LBB2_80:
	cmp	edi, 2
	je	.LBB2_162
# %bb.81:
	cmp	edi, 3
	jne	.LBB2_737
# %bb.82:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.83:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_84
# %bb.210:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_330
# %bb.211:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_330
.LBB2_84:
	xor	esi, esi
.LBB2_509:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_511
.LBB2_510:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_510
.LBB2_511:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_512:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_512
	jmp	.LBB2_737
.LBB2_85:
	cmp	edi, 7
	je	.LBB2_165
# %bb.86:
	cmp	edi, 8
	jne	.LBB2_737
# %bb.87:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.88:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_89
# %bb.213:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_333
# %bb.214:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_333
.LBB2_89:
	xor	esi, esi
.LBB2_517:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_519
.LBB2_518:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_518
.LBB2_519:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_520:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_520
	jmp	.LBB2_737
.LBB2_90:
	cmp	edi, 7
	je	.LBB2_168
# %bb.91:
	cmp	edi, 8
	jne	.LBB2_737
# %bb.92:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.93:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_94
# %bb.216:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_336
# %bb.217:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_336
.LBB2_94:
	xor	esi, esi
.LBB2_525:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_527
.LBB2_526:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_526
.LBB2_527:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_528:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_528
	jmp	.LBB2_737
.LBB2_95:
	cmp	edi, 7
	je	.LBB2_171
# %bb.96:
	cmp	edi, 8
	jne	.LBB2_737
# %bb.97:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.98:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_99
# %bb.219:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_339
# %bb.220:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_339
.LBB2_99:
	xor	esi, esi
.LBB2_533:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_535
.LBB2_534:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_534
.LBB2_535:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_536:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_536
	jmp	.LBB2_737
.LBB2_100:
	cmp	edi, 7
	je	.LBB2_174
# %bb.101:
	cmp	edi, 8
	jne	.LBB2_737
# %bb.102:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.103:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_104
# %bb.222:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_342
# %bb.223:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_342
.LBB2_104:
	xor	esi, esi
.LBB2_541:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_543
.LBB2_542:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_542
.LBB2_543:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_544:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_544
	jmp	.LBB2_737
.LBB2_105:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.106:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_107
# %bb.225:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_345
# %bb.226:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_345
.LBB2_107:
	xor	esi, esi
.LBB2_549:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_551
.LBB2_550:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_550
.LBB2_551:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_552:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_552
	jmp	.LBB2_737
.LBB2_108:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.109:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_110
# %bb.228:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_348
# %bb.229:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_348
.LBB2_110:
	xor	esi, esi
.LBB2_557:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_559
.LBB2_558:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_558
.LBB2_559:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_560:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_560
	jmp	.LBB2_737
.LBB2_111:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.112:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_113
# %bb.231:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_351
# %bb.232:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_351
.LBB2_113:
	xor	esi, esi
.LBB2_565:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_567
.LBB2_566:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_566
.LBB2_567:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_568:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_568
	jmp	.LBB2_737
.LBB2_114:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.115:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_116
# %bb.234:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_354
# %bb.235:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_354
.LBB2_116:
	xor	esi, esi
.LBB2_573:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_575
.LBB2_574:                              # =>This Inner Loop Header: Depth=1
	mov	edx, eax
	sub	dx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_574
.LBB2_575:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_576:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_576
	jmp	.LBB2_737
.LBB2_117:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.118:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_119
# %bb.237:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_357
# %bb.238:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_357
.LBB2_119:
	xor	esi, esi
.LBB2_581:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_583
.LBB2_582:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_582
.LBB2_583:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_584:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_584
	jmp	.LBB2_737
.LBB2_120:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.121:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_122
# %bb.240:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_360
# %bb.241:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_360
.LBB2_122:
	xor	esi, esi
.LBB2_589:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_591
.LBB2_590:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_590
.LBB2_591:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_592:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_592
	jmp	.LBB2_737
.LBB2_123:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.124:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_125
# %bb.243:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_363
# %bb.244:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_363
.LBB2_125:
	xor	esi, esi
.LBB2_597:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_599
.LBB2_598:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_598
.LBB2_599:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_600:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_600
	jmp	.LBB2_737
.LBB2_126:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.127:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_128
# %bb.246:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_366
# %bb.247:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_366
.LBB2_128:
	xor	esi, esi
.LBB2_605:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_607
.LBB2_606:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, word ptr [rcx + 2*rsi]
	add	dx, ax
	mov	word ptr [r8 + 2*rsi], dx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_606
.LBB2_607:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_608:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_608
	jmp	.LBB2_737
.LBB2_129:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.130:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_131
# %bb.249:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_369
# %bb.250:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_369
.LBB2_131:
	xor	esi, esi
.LBB2_613:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_615
.LBB2_614:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_614
.LBB2_615:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_616:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_616
	jmp	.LBB2_737
.LBB2_132:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.133:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_134
# %bb.252:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_372
# %bb.253:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_372
.LBB2_134:
	xor	edx, edx
.LBB2_621:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_623
.LBB2_622:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_622
.LBB2_623:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_624:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_624
	jmp	.LBB2_737
.LBB2_135:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.136:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_137
# %bb.255:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_375
# %bb.256:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_375
.LBB2_137:
	xor	esi, esi
.LBB2_629:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_631
.LBB2_630:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_630
.LBB2_631:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_632:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_632
	jmp	.LBB2_737
.LBB2_138:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.139:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_140
# %bb.258:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_378
# %bb.259:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_378
.LBB2_140:
	xor	edx, edx
.LBB2_637:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_639
.LBB2_638:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_638
.LBB2_639:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_640:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_640
	jmp	.LBB2_737
.LBB2_141:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.142:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_143
# %bb.261:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_381
# %bb.262:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_381
.LBB2_143:
	xor	esi, esi
.LBB2_645:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_647
.LBB2_646:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_646
.LBB2_647:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_648:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_648
	jmp	.LBB2_737
.LBB2_144:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.145:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_146
# %bb.264:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_384
# %bb.265:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_384
.LBB2_146:
	xor	edx, edx
.LBB2_653:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_655
.LBB2_654:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_654
.LBB2_655:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_656:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_656
	jmp	.LBB2_737
.LBB2_147:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.148:
	mov	rax, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_149
# %bb.267:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_387
# %bb.268:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_387
.LBB2_149:
	xor	esi, esi
.LBB2_661:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_663
.LBB2_662:                              # =>This Inner Loop Header: Depth=1
	mov	rdx, qword ptr [rcx + 8*rsi]
	add	rdx, rax
	mov	qword ptr [r8 + 8*rsi], rdx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_662
.LBB2_663:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_664:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_664
	jmp	.LBB2_737
.LBB2_150:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.151:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_152
# %bb.270:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_390
# %bb.271:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_390
.LBB2_152:
	xor	edx, edx
.LBB2_669:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_671
.LBB2_670:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rdx]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_670
.LBB2_671:
	cmp	rsi, 3
	jb	.LBB2_737
.LBB2_672:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_672
	jmp	.LBB2_737
.LBB2_153:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.154:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_155
# %bb.273:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_393
# %bb.274:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_393
.LBB2_155:
	xor	esi, esi
.LBB2_677:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_679
.LBB2_678:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_678
.LBB2_679:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_680:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_680
	jmp	.LBB2_737
.LBB2_156:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.157:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_158
# %bb.276:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_396
# %bb.277:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_396
.LBB2_158:
	xor	esi, esi
.LBB2_685:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_687
.LBB2_686:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_686
.LBB2_687:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_688:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_688
	jmp	.LBB2_737
.LBB2_159:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.160:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_161
# %bb.279:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_399
# %bb.280:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_399
.LBB2_161:
	xor	esi, esi
.LBB2_693:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_695
.LBB2_694:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_694
.LBB2_695:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_696:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_696
	jmp	.LBB2_737
.LBB2_162:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.163:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_164
# %bb.282:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_402
# %bb.283:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_402
.LBB2_164:
	xor	esi, esi
.LBB2_701:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_703
.LBB2_702:                              # =>This Inner Loop Header: Depth=1
	movzx	edx, byte ptr [rcx + rsi]
	add	dl, al
	mov	byte ptr [r8 + rsi], dl
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_702
.LBB2_703:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_704:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_704
	jmp	.LBB2_737
.LBB2_165:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.166:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_167
# %bb.285:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_405
# %bb.286:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_405
.LBB2_167:
	xor	esi, esi
.LBB2_709:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_711
.LBB2_710:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_710
.LBB2_711:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_712:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_712
	jmp	.LBB2_737
.LBB2_168:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.169:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_170
# %bb.288:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_408
# %bb.289:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_408
.LBB2_170:
	xor	esi, esi
.LBB2_717:
	mov	rdx, rsi
	not	rdx
	add	rdx, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_719
.LBB2_718:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_718
.LBB2_719:
	cmp	rdx, 3
	jb	.LBB2_737
.LBB2_720:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_720
	jmp	.LBB2_737
.LBB2_171:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.172:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_173
# %bb.291:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_411
# %bb.292:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_411
.LBB2_173:
	xor	esi, esi
.LBB2_725:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_727
.LBB2_726:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_726
.LBB2_727:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_728:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_728
	jmp	.LBB2_737
.LBB2_174:
	test	r9d, r9d
	jle	.LBB2_737
# %bb.175:
	mov	eax, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_176
# %bb.294:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_414
# %bb.295:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_414
.LBB2_176:
	xor	esi, esi
.LBB2_733:
	mov	r9, rsi
	not	r9
	add	r9, r10
	mov	rdi, r10
	and	rdi, 3
	je	.LBB2_735
.LBB2_734:                              # =>This Inner Loop Header: Depth=1
	mov	edx, dword ptr [rcx + 4*rsi]
	add	edx, eax
	mov	dword ptr [r8 + 4*rsi], edx
	add	rsi, 1
	add	rdi, -1
	jne	.LBB2_734
.LBB2_735:
	cmp	r9, 3
	jb	.LBB2_737
.LBB2_736:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_736
	jmp	.LBB2_737
.LBB2_297:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_417
# %bb.298:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_299:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_299
	jmp	.LBB2_418
.LBB2_300:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_425
# %bb.301:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_302:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_302
	jmp	.LBB2_426
.LBB2_303:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_433
# %bb.304:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_305:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_305
	jmp	.LBB2_434
.LBB2_306:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_441
# %bb.307:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_308:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_308
	jmp	.LBB2_442
.LBB2_309:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_449
# %bb.310:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_311:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_311
	jmp	.LBB2_450
.LBB2_312:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_457
# %bb.313:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_314:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_314
	jmp	.LBB2_458
.LBB2_315:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_465
# %bb.316:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_317:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_317
	jmp	.LBB2_466
.LBB2_318:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	r9, rsi
	shr	r9, 2
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_473
# %bb.319:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_320:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_320
	jmp	.LBB2_474
.LBB2_321:
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
	je	.LBB2_481
# %bb.322:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_323:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_323
	jmp	.LBB2_482
.LBB2_324:
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
	je	.LBB2_489
# %bb.325:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_326:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_326
	jmp	.LBB2_490
.LBB2_327:
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
	je	.LBB2_497
# %bb.328:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_329:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_329
	jmp	.LBB2_498
.LBB2_330:
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
	je	.LBB2_505
# %bb.331:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_332:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_332
	jmp	.LBB2_506
.LBB2_333:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_513
# %bb.334:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_335:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_335
	jmp	.LBB2_514
.LBB2_336:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_521
# %bb.337:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_338:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_338
	jmp	.LBB2_522
.LBB2_339:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_529
# %bb.340:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_341:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_341
	jmp	.LBB2_530
.LBB2_342:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_537
# %bb.343:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_344:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_344
	jmp	.LBB2_538
.LBB2_345:
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
	je	.LBB2_545
# %bb.346:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_347:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_347
	jmp	.LBB2_546
.LBB2_348:
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
	je	.LBB2_553
# %bb.349:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_350:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_350
	jmp	.LBB2_554
.LBB2_351:
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
	je	.LBB2_561
# %bb.352:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_353:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_353
	jmp	.LBB2_562
.LBB2_354:
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
	je	.LBB2_569
# %bb.355:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_356:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_356
	jmp	.LBB2_570
.LBB2_357:
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
	je	.LBB2_577
# %bb.358:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_359:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_359
	jmp	.LBB2_578
.LBB2_360:
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
	je	.LBB2_585
# %bb.361:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_362:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_362
	jmp	.LBB2_586
.LBB2_363:
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
	je	.LBB2_593
# %bb.364:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_365:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_365
	jmp	.LBB2_594
.LBB2_366:
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
	je	.LBB2_601
# %bb.367:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_368:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_368
	jmp	.LBB2_602
.LBB2_369:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_609
# %bb.370:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_371:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_371
	jmp	.LBB2_610
.LBB2_372:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_617
# %bb.373:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_374:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_374
	jmp	.LBB2_618
.LBB2_375:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_625
# %bb.376:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_377
	jmp	.LBB2_626
.LBB2_378:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_633
# %bb.379:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_380:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_380
	jmp	.LBB2_634
.LBB2_381:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_641
# %bb.382:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_383:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_383
	jmp	.LBB2_642
.LBB2_384:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_649
# %bb.385:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_386:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_386
	jmp	.LBB2_650
.LBB2_387:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_657
# %bb.388:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_389:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_389
	jmp	.LBB2_658
.LBB2_390:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	r9, rsi
	shr	r9, 3
	add	r9, 1
	test	rsi, rsi
	je	.LBB2_665
# %bb.391:
	mov	rsi, r9
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_392:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_392
	jmp	.LBB2_666
.LBB2_393:
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
	je	.LBB2_673
# %bb.394:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_395:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_395
	jmp	.LBB2_674
.LBB2_396:
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
	je	.LBB2_681
# %bb.397:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_398:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_398
	jmp	.LBB2_682
.LBB2_399:
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
	je	.LBB2_689
# %bb.400:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_401:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_401
	jmp	.LBB2_690
.LBB2_402:
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
	je	.LBB2_697
# %bb.403:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_404:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_404
	jmp	.LBB2_698
.LBB2_405:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_705
# %bb.406:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_407:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_407
	jmp	.LBB2_706
.LBB2_408:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_713
# %bb.409:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_410:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_410
	jmp	.LBB2_714
.LBB2_411:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_721
# %bb.412:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_413:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_413
	jmp	.LBB2_722
.LBB2_414:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_729
# %bb.415:
	mov	rdx, r9
	and	rdx, -2
	neg	rdx
	xor	edi, edi
.LBB2_416:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_416
	jmp	.LBB2_730
.LBB2_417:
	xor	edi, edi
.LBB2_418:
	test	r9b, 1
	je	.LBB2_420
# %bb.419:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_420:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_421
.LBB2_425:
	xor	edi, edi
.LBB2_426:
	test	r9b, 1
	je	.LBB2_428
# %bb.427:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_428:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_429
.LBB2_433:
	xor	edi, edi
.LBB2_434:
	test	r9b, 1
	je	.LBB2_436
# %bb.435:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_436:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_437
.LBB2_441:
	xor	edi, edi
.LBB2_442:
	test	r9b, 1
	je	.LBB2_444
# %bb.443:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_444:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_445
.LBB2_449:
	xor	edi, edi
.LBB2_450:
	test	r9b, 1
	je	.LBB2_452
# %bb.451:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_452:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_453
.LBB2_457:
	xor	edi, edi
.LBB2_458:
	test	r9b, 1
	je	.LBB2_460
# %bb.459:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_460:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_461
.LBB2_465:
	xor	edi, edi
.LBB2_466:
	test	r9b, 1
	je	.LBB2_468
# %bb.467:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_468:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_469
.LBB2_473:
	xor	edi, edi
.LBB2_474:
	test	r9b, 1
	je	.LBB2_476
# %bb.475:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB2_476:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_477
.LBB2_481:
	xor	edi, edi
.LBB2_482:
	test	r9b, 1
	je	.LBB2_484
# %bb.483:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_484:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_485
.LBB2_489:
	xor	edi, edi
.LBB2_490:
	test	r9b, 1
	je	.LBB2_492
# %bb.491:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_492:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_493
.LBB2_497:
	xor	edi, edi
.LBB2_498:
	test	r9b, 1
	je	.LBB2_500
# %bb.499:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_500:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_501
.LBB2_505:
	xor	edi, edi
.LBB2_506:
	test	r9b, 1
	je	.LBB2_508
# %bb.507:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_508:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_509
.LBB2_513:
	xor	edi, edi
.LBB2_514:
	test	r9b, 1
	je	.LBB2_516
# %bb.515:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_516:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_517
.LBB2_521:
	xor	edi, edi
.LBB2_522:
	test	r9b, 1
	je	.LBB2_524
# %bb.523:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_524:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_525
.LBB2_529:
	xor	edi, edi
.LBB2_530:
	test	r9b, 1
	je	.LBB2_532
# %bb.531:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_532:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_533
.LBB2_537:
	xor	edi, edi
.LBB2_538:
	test	r9b, 1
	je	.LBB2_540
# %bb.539:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_540:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_541
.LBB2_545:
	xor	edi, edi
.LBB2_546:
	test	r9b, 1
	je	.LBB2_548
# %bb.547:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_548:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_549
.LBB2_553:
	xor	edi, edi
.LBB2_554:
	test	r9b, 1
	je	.LBB2_556
# %bb.555:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_556:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_557
.LBB2_561:
	xor	edi, edi
.LBB2_562:
	test	r9b, 1
	je	.LBB2_564
# %bb.563:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_564:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_565
.LBB2_569:
	xor	edi, edi
.LBB2_570:
	test	r9b, 1
	je	.LBB2_572
# %bb.571:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rdi], xmm3
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm0
.LBB2_572:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_573
.LBB2_577:
	xor	edi, edi
.LBB2_578:
	test	r9b, 1
	je	.LBB2_580
# %bb.579:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_580:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_581
.LBB2_585:
	xor	edi, edi
.LBB2_586:
	test	r9b, 1
	je	.LBB2_588
# %bb.587:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_588:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_589
.LBB2_593:
	xor	edi, edi
.LBB2_594:
	test	r9b, 1
	je	.LBB2_596
# %bb.595:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_596:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_597
.LBB2_601:
	xor	edi, edi
.LBB2_602:
	test	r9b, 1
	je	.LBB2_604
# %bb.603:
	movdqu	xmm1, xmmword ptr [rcx + 2*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 2*rdi + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rdi], xmm1
	movdqu	xmmword ptr [r8 + 2*rdi + 16], xmm2
.LBB2_604:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_605
.LBB2_609:
	xor	edi, edi
.LBB2_610:
	test	r9b, 1
	je	.LBB2_612
# %bb.611:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_612:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_613
.LBB2_617:
	xor	edi, edi
.LBB2_618:
	test	r9b, 1
	je	.LBB2_620
# %bb.619:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_620:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_621
.LBB2_625:
	xor	edi, edi
.LBB2_626:
	test	r9b, 1
	je	.LBB2_628
# %bb.627:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rdi], xmm3
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm0
.LBB2_628:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_629
.LBB2_633:
	xor	edi, edi
.LBB2_634:
	test	r9b, 1
	je	.LBB2_636
# %bb.635:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_636:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_637
.LBB2_641:
	xor	edi, edi
.LBB2_642:
	test	r9b, 1
	je	.LBB2_644
# %bb.643:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_644:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_645
.LBB2_649:
	xor	edi, edi
.LBB2_650:
	test	r9b, 1
	je	.LBB2_652
# %bb.651:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_652:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_653
.LBB2_657:
	xor	edi, edi
.LBB2_658:
	test	r9b, 1
	je	.LBB2_660
# %bb.659:
	movdqu	xmm1, xmmword ptr [rcx + 8*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 8*rdi + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rdi], xmm1
	movdqu	xmmword ptr [r8 + 8*rdi + 16], xmm2
.LBB2_660:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_661
.LBB2_665:
	xor	edi, edi
.LBB2_666:
	test	r9b, 1
	je	.LBB2_668
# %bb.667:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB2_668:
	cmp	rdx, rax
	je	.LBB2_737
	jmp	.LBB2_669
.LBB2_673:
	xor	edi, edi
.LBB2_674:
	test	r9b, 1
	je	.LBB2_676
# %bb.675:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_676:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_677
.LBB2_681:
	xor	edi, edi
.LBB2_682:
	test	r9b, 1
	je	.LBB2_684
# %bb.683:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rdi], xmm3
	movdqu	xmmword ptr [r8 + rdi + 16], xmm0
.LBB2_684:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_685
.LBB2_689:
	xor	edi, edi
.LBB2_690:
	test	r9b, 1
	je	.LBB2_692
# %bb.691:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_692:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_693
.LBB2_697:
	xor	edi, edi
.LBB2_698:
	test	r9b, 1
	je	.LBB2_700
# %bb.699:
	movdqu	xmm1, xmmword ptr [rcx + rdi]
	movdqu	xmm2, xmmword ptr [rcx + rdi + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rdi], xmm1
	movdqu	xmmword ptr [r8 + rdi + 16], xmm2
.LBB2_700:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_701
.LBB2_705:
	xor	edi, edi
.LBB2_706:
	test	r9b, 1
	je	.LBB2_708
# %bb.707:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_708:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_709
.LBB2_713:
	xor	edi, edi
.LBB2_714:
	test	r9b, 1
	je	.LBB2_716
# %bb.715:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rdi], xmm3
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm0
.LBB2_716:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_717
.LBB2_721:
	xor	edi, edi
.LBB2_722:
	test	r9b, 1
	je	.LBB2_724
# %bb.723:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_724:
	cmp	rsi, r10
	je	.LBB2_737
	jmp	.LBB2_725
.LBB2_729:
	xor	edi, edi
.LBB2_730:
	test	r9b, 1
	je	.LBB2_732
# %bb.731:
	movdqu	xmm1, xmmword ptr [rcx + 4*rdi]
	movdqu	xmm2, xmmword ptr [rcx + 4*rdi + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rdi], xmm1
	movdqu	xmmword ptr [r8 + 4*rdi + 16], xmm2
.LBB2_732:
	cmp	rsi, r10
	jne	.LBB2_733
.LBB2_737:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end2:
	.size	arithmetic_scalar_arr_sse4, .Lfunc_end2-arithmetic_scalar_arr_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
