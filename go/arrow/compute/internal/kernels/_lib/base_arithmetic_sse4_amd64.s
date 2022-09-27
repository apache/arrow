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
	push	r14
	push	rbx
	and	rsp, -8
	cmp	sil, 1
	jg	.LBB0_3
# %bb.1:
	test	sil, sil
	je	.LBB0_5
# %bb.2:
	cmp	sil, 1
	jne	.LBB0_697
.LBB0_178:
	cmp	edi, 6
	jg	.LBB0_191
# %bb.179:
	cmp	edi, 3
	jle	.LBB0_180
# %bb.185:
	cmp	edi, 4
	je	.LBB0_232
# %bb.186:
	cmp	edi, 5
	je	.LBB0_248
# %bb.187:
	cmp	edi, 6
	jne	.LBB0_351
# %bb.188:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.189:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_264
# %bb.190:
	xor	esi, esi
.LBB0_273:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_275
	.p2align	4, 0x90
.LBB0_274:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_274
.LBB0_275:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_276:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_276
	jmp	.LBB0_351
.LBB0_3:
	cmp	sil, 2
	je	.LBB0_351
# %bb.4:
	cmp	sil, 3
	jne	.LBB0_697
.LBB0_524:
	cmp	edi, 6
	jg	.LBB0_537
# %bb.525:
	cmp	edi, 3
	jle	.LBB0_526
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_621
	.p2align	4, 0x90
.LBB0_620:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_620
.LBB0_621:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_695
	.p2align	4, 0x90
.LBB0_694:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_694
.LBB0_695:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
.LBB0_5:
	cmp	edi, 6
	jg	.LBB0_18
# %bb.6:
	cmp	edi, 3
	jle	.LBB0_7
# %bb.12:
	cmp	edi, 4
	je	.LBB0_59
# %bb.13:
	cmp	edi, 5
	je	.LBB0_75
# %bb.14:
	cmp	edi, 6
	jne	.LBB0_178
# %bb.15:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.16:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_91
# %bb.17:
	xor	esi, esi
.LBB0_100:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_102
.LBB0_101:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_101
.LBB0_102:
	cmp	r11, 3
	jb	.LBB0_178
.LBB0_103:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_103
	jmp	.LBB0_178
.LBB0_191:
	cmp	edi, 8
	jle	.LBB0_192
# %bb.197:
	cmp	edi, 9
	je	.LBB0_306
# %bb.198:
	cmp	edi, 11
	je	.LBB0_322
# %bb.199:
	cmp	edi, 12
	jne	.LBB0_351
# %bb.200:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.201:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_338
# %bb.202:
	xor	esi, esi
.LBB0_347:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_349
	.p2align	4, 0x90
.LBB0_348:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_348
.LBB0_349:
	cmp	rax, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_350:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_350
	jmp	.LBB0_351
.LBB0_18:
	cmp	edi, 8
	jle	.LBB0_19
# %bb.24:
	cmp	edi, 9
	je	.LBB0_133
# %bb.25:
	cmp	edi, 11
	je	.LBB0_149
# %bb.26:
	cmp	edi, 12
	jne	.LBB0_178
# %bb.27:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.28:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_165
# %bb.29:
	xor	esi, esi
.LBB0_174:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_176
.LBB0_175:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	addsd	xmm0, qword ptr [rdx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_175
.LBB0_176:
	cmp	rax, 3
	jb	.LBB0_178
.LBB0_177:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_177
	jmp	.LBB0_178
.LBB0_526:
	cmp	edi, 2
	je	.LBB0_549
# %bb.527:
	cmp	edi, 3
	jne	.LBB0_697
# %bb.528:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.529:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_565
# %bb.530:
	xor	esi, esi
.LBB0_574:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_576
	.p2align	4, 0x90
.LBB0_575:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_575
.LBB0_576:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_650
	.p2align	4, 0x90
.LBB0_649:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_649
.LBB0_650:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
.LBB0_180:
	cmp	edi, 2
	je	.LBB0_203
# %bb.181:
	cmp	edi, 3
	jne	.LBB0_351
# %bb.182:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.183:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_219
# %bb.184:
	xor	esi, esi
.LBB0_228:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_230
	.p2align	4, 0x90
.LBB0_229:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_229
.LBB0_230:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_231:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_231
	jmp	.LBB0_351
.LBB0_192:
	cmp	edi, 7
	je	.LBB0_277
# %bb.193:
	cmp	edi, 8
	jne	.LBB0_351
# %bb.194:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.195:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_293
# %bb.196:
	xor	esi, esi
.LBB0_302:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_304
	.p2align	4, 0x90
.LBB0_303:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_303
.LBB0_304:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_305:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_305
	jmp	.LBB0_351
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_592
	.p2align	4, 0x90
.LBB0_591:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_591
.LBB0_592:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_608
	.p2align	4, 0x90
.LBB0_607:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_607
.LBB0_608:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_666
	.p2align	4, 0x90
.LBB0_665:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_665
.LBB0_666:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_682
	.p2align	4, 0x90
.LBB0_681:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_681
.LBB0_682:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_563
	.p2align	4, 0x90
.LBB0_562:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_562
.LBB0_563:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_637
	.p2align	4, 0x90
.LBB0_636:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_636
.LBB0_637:
	cmp	rdi, 3
	jb	.LBB0_697
	.p2align	4, 0x90
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
.LBB0_7:
	cmp	edi, 2
	je	.LBB0_30
# %bb.8:
	cmp	edi, 3
	jne	.LBB0_178
# %bb.9:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.10:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_46
# %bb.11:
	xor	esi, esi
.LBB0_55:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_57
.LBB0_56:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_56
.LBB0_57:
	cmp	r11, 3
	jb	.LBB0_178
.LBB0_58:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_58
	jmp	.LBB0_178
.LBB0_19:
	cmp	edi, 7
	je	.LBB0_104
# %bb.20:
	cmp	edi, 8
	jne	.LBB0_178
# %bb.21:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.22:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_120
# %bb.23:
	xor	esi, esi
.LBB0_129:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_131
.LBB0_130:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_130
.LBB0_131:
	cmp	r11, 3
	jb	.LBB0_178
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
	jmp	.LBB0_178
.LBB0_232:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.233:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_235
# %bb.234:
	xor	esi, esi
.LBB0_244:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_246
	.p2align	4, 0x90
.LBB0_245:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_245
.LBB0_246:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_247:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_247
	jmp	.LBB0_351
.LBB0_248:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.249:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_251
# %bb.250:
	xor	esi, esi
.LBB0_260:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_262
	.p2align	4, 0x90
.LBB0_261:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_261
.LBB0_262:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
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
	jmp	.LBB0_351
.LBB0_306:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.307:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_309
# %bb.308:
	xor	esi, esi
.LBB0_318:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_320
	.p2align	4, 0x90
.LBB0_319:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_319
.LBB0_320:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
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
	jmp	.LBB0_351
.LBB0_322:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.323:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_325
# %bb.324:
	xor	esi, esi
.LBB0_334:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_336
	.p2align	4, 0x90
.LBB0_335:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_335
.LBB0_336:
	cmp	rax, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_337:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_337
	jmp	.LBB0_351
.LBB0_203:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.204:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_206
# %bb.205:
	xor	esi, esi
.LBB0_215:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_217
	.p2align	4, 0x90
.LBB0_216:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_216
.LBB0_217:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
.LBB0_218:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_218
	jmp	.LBB0_351
.LBB0_277:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.278:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_280
# %bb.279:
	xor	esi, esi
.LBB0_289:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_291
	.p2align	4, 0x90
.LBB0_290:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_290
.LBB0_291:
	cmp	r11, 3
	jb	.LBB0_351
	.p2align	4, 0x90
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
	jmp	.LBB0_351
.LBB0_59:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.60:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_62
# %bb.61:
	xor	esi, esi
.LBB0_71:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_73
.LBB0_72:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_72
.LBB0_73:
	cmp	r11, 3
	jb	.LBB0_178
.LBB0_74:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_74
	jmp	.LBB0_178
.LBB0_75:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.76:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_78
# %bb.77:
	xor	esi, esi
.LBB0_87:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_89
.LBB0_88:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_88
.LBB0_89:
	cmp	r11, 3
	jb	.LBB0_178
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
	jmp	.LBB0_178
.LBB0_133:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.134:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_136
# %bb.135:
	xor	esi, esi
.LBB0_145:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_147
.LBB0_146:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_146
.LBB0_147:
	cmp	r11, 3
	jb	.LBB0_178
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
	jmp	.LBB0_178
.LBB0_149:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.150:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_152
# %bb.151:
	xor	esi, esi
.LBB0_161:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_163
.LBB0_162:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	addss	xmm0, dword ptr [rdx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_162
.LBB0_163:
	cmp	rax, 3
	jb	.LBB0_178
.LBB0_164:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_164
	jmp	.LBB0_178
.LBB0_30:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.31:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_33
# %bb.32:
	xor	esi, esi
.LBB0_42:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_44
.LBB0_43:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_43
.LBB0_44:
	cmp	r11, 3
	jb	.LBB0_178
.LBB0_45:                               # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_45
	jmp	.LBB0_178
.LBB0_104:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.105:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_107
# %bb.106:
	xor	esi, esi
.LBB0_116:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_118
.LBB0_117:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_117
.LBB0_118:
	cmp	r11, 3
	jb	.LBB0_178
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
	jmp	.LBB0_178
.LBB0_610:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_615
	jmp	.LBB0_616
.LBB0_684:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_689
	jmp	.LBB0_690
.LBB0_565:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_570
	jmp	.LBB0_571
.LBB0_639:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_644
	jmp	.LBB0_645
.LBB0_581:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_586
	jmp	.LBB0_587
.LBB0_597:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_602
	jmp	.LBB0_603
.LBB0_655:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_660
	jmp	.LBB0_661
.LBB0_671:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_676
	jmp	.LBB0_677
.LBB0_552:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_557
	jmp	.LBB0_558
.LBB0_626:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r9b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	dil
	xor	esi, esi
	test	r9b, bl
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
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
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
	add	rbx, 2
	jne	.LBB0_631
	jmp	.LBB0_632
.LBB0_264:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_273
# %bb.265:
	and	al, r11b
	jne	.LBB0_273
# %bb.266:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_267
# %bb.268:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_269:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 48], xmm0
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_269
	jmp	.LBB0_270
.LBB0_338:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_347
# %bb.339:
	and	al, r11b
	jne	.LBB0_347
# %bb.340:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_341
# %bb.342:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_343:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rax]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rax]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rax + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rax], xmm2
	movupd	xmmword ptr [r8 + 8*rax + 16], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rax + 32]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rax + 48]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rax + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rax + 48], xmm0
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_343
	jmp	.LBB0_344
.LBB0_219:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_228
# %bb.220:
	and	al, r11b
	jne	.LBB0_228
# %bb.221:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB0_222
# %bb.223:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_224:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + rax + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax + 32], xmm2
	movdqu	xmmword ptr [r8 + rax + 48], xmm0
	add	rax, 64
	add	rbx, 2
	jne	.LBB0_224
	jmp	.LBB0_225
.LBB0_293:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_302
# %bb.294:
	and	al, r11b
	jne	.LBB0_302
# %bb.295:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_296
# %bb.297:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_298:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 48], xmm0
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_298
	jmp	.LBB0_299
.LBB0_235:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_244
# %bb.236:
	and	al, r11b
	jne	.LBB0_244
# %bb.237:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB0_238
# %bb.239:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_240:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 48], xmm0
	add	rax, 32
	add	rbx, 2
	jne	.LBB0_240
	jmp	.LBB0_241
.LBB0_251:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_260
# %bb.252:
	and	al, r11b
	jne	.LBB0_260
# %bb.253:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB0_254
# %bb.255:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_256:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 48], xmm0
	add	rax, 32
	add	rbx, 2
	jne	.LBB0_256
	jmp	.LBB0_257
.LBB0_309:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_318
# %bb.310:
	and	al, r11b
	jne	.LBB0_318
# %bb.311:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_312
# %bb.313:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_314:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 48], xmm0
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_314
	jmp	.LBB0_315
.LBB0_325:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_334
# %bb.326:
	and	al, r11b
	jne	.LBB0_334
# %bb.327:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_328
# %bb.329:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_330:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rax]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rax + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rax], xmm2
	movups	xmmword ptr [r8 + 4*rax + 16], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rax + 32]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rax + 48]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rax + 32], xmm2
	movups	xmmword ptr [r8 + 4*rax + 48], xmm0
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_330
	jmp	.LBB0_331
.LBB0_206:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_215
# %bb.207:
	and	al, r11b
	jne	.LBB0_215
# %bb.208:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB0_209
# %bb.210:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_211:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + rax + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax + 32], xmm2
	movdqu	xmmword ptr [r8 + rax + 48], xmm0
	add	rax, 64
	add	rbx, 2
	jne	.LBB0_211
	jmp	.LBB0_212
.LBB0_280:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_289
# %bb.281:
	and	al, r11b
	jne	.LBB0_289
# %bb.282:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_283
# %bb.284:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_285:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 48], xmm0
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_285
	jmp	.LBB0_286
.LBB0_91:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_100
# %bb.92:
	and	al, r11b
	jne	.LBB0_100
# %bb.93:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_94
# %bb.95:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_96:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 48], xmm0
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_96
	jmp	.LBB0_97
.LBB0_165:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_174
# %bb.166:
	and	al, r11b
	jne	.LBB0_174
# %bb.167:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_168
# %bb.169:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_170:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rax]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rax]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rax + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rax], xmm2
	movupd	xmmword ptr [r8 + 8*rax + 16], xmm0
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rax + 32]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rax + 48]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rax + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rax + 48], xmm0
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_170
	jmp	.LBB0_171
.LBB0_46:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_55
# %bb.47:
	and	al, r11b
	jne	.LBB0_55
# %bb.48:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB0_49
# %bb.50:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_51:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + rax + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax + 32], xmm2
	movdqu	xmmword ptr [r8 + rax + 48], xmm0
	add	rax, 64
	add	rbx, 2
	jne	.LBB0_51
	jmp	.LBB0_52
.LBB0_120:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_129
# %bb.121:
	and	al, r11b
	jne	.LBB0_129
# %bb.122:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_123
# %bb.124:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_125:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 48], xmm0
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_125
	jmp	.LBB0_126
.LBB0_62:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_71
# %bb.63:
	and	al, r11b
	jne	.LBB0_71
# %bb.64:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB0_65
# %bb.66:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_67:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 48], xmm0
	add	rax, 32
	add	rbx, 2
	jne	.LBB0_67
	jmp	.LBB0_68
.LBB0_78:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_87
# %bb.79:
	and	al, r11b
	jne	.LBB0_87
# %bb.80:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB0_81
# %bb.82:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_83:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 32]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 48]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 48], xmm0
	add	rax, 32
	add	rbx, 2
	jne	.LBB0_83
	jmp	.LBB0_84
.LBB0_136:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_145
# %bb.137:
	and	al, r11b
	jne	.LBB0_145
# %bb.138:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_139
# %bb.140:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_141:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 32]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 48]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 48], xmm0
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_141
	jmp	.LBB0_142
.LBB0_152:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_161
# %bb.153:
	and	al, r11b
	jne	.LBB0_161
# %bb.154:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_155
# %bb.156:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_157:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rax]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rax + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rax], xmm2
	movups	xmmword ptr [r8 + 4*rax + 16], xmm0
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rax + 32]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rax + 48]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rax + 32], xmm2
	movups	xmmword ptr [r8 + 4*rax + 48], xmm0
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_157
	jmp	.LBB0_158
.LBB0_33:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_42
# %bb.34:
	and	al, r11b
	jne	.LBB0_42
# %bb.35:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB0_36
# %bb.37:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_38:                               # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + rax + 32]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 48]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax + 32], xmm2
	movdqu	xmmword ptr [r8 + rax + 48], xmm0
	add	rax, 64
	add	rbx, 2
	jne	.LBB0_38
	jmp	.LBB0_39
.LBB0_107:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_116
# %bb.108:
	and	al, r11b
	jne	.LBB0_116
# %bb.109:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_110
# %bb.111:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_112:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 32]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 48]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax + 32], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 48], xmm0
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_112
	jmp	.LBB0_113
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
.LBB0_267:
	xor	eax, eax
.LBB0_270:
	test	r11b, 1
	je	.LBB0_272
# %bb.271:
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
.LBB0_272:
	cmp	rsi, r10
	jne	.LBB0_273
	jmp	.LBB0_351
.LBB0_341:
	xor	eax, eax
.LBB0_344:
	test	r11b, 1
	je	.LBB0_346
# %bb.345:
	movupd	xmm0, xmmword ptr [rdx + 8*rax]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rax]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rax + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rax], xmm2
	movupd	xmmword ptr [r8 + 8*rax + 16], xmm0
.LBB0_346:
	cmp	rsi, r10
	jne	.LBB0_347
	jmp	.LBB0_351
.LBB0_222:
	xor	eax, eax
.LBB0_225:
	test	r11b, 1
	je	.LBB0_227
# %bb.226:
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
.LBB0_227:
	cmp	rsi, r10
	jne	.LBB0_228
	jmp	.LBB0_351
.LBB0_296:
	xor	eax, eax
.LBB0_299:
	test	r11b, 1
	je	.LBB0_301
# %bb.300:
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
.LBB0_301:
	cmp	rsi, r10
	jne	.LBB0_302
	jmp	.LBB0_351
.LBB0_238:
	xor	eax, eax
.LBB0_241:
	test	r11b, 1
	je	.LBB0_243
# %bb.242:
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
.LBB0_243:
	cmp	rsi, r10
	jne	.LBB0_244
	jmp	.LBB0_351
.LBB0_254:
	xor	eax, eax
.LBB0_257:
	test	r11b, 1
	je	.LBB0_259
# %bb.258:
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
.LBB0_259:
	cmp	rsi, r10
	jne	.LBB0_260
	jmp	.LBB0_351
.LBB0_312:
	xor	eax, eax
.LBB0_315:
	test	r11b, 1
	je	.LBB0_317
# %bb.316:
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
.LBB0_317:
	cmp	rsi, r10
	jne	.LBB0_318
	jmp	.LBB0_351
.LBB0_328:
	xor	eax, eax
.LBB0_331:
	test	r11b, 1
	je	.LBB0_333
# %bb.332:
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rax]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rax + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rax], xmm2
	movups	xmmword ptr [r8 + 4*rax + 16], xmm0
.LBB0_333:
	cmp	rsi, r10
	jne	.LBB0_334
	jmp	.LBB0_351
.LBB0_209:
	xor	eax, eax
.LBB0_212:
	test	r11b, 1
	je	.LBB0_214
# %bb.213:
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
.LBB0_214:
	cmp	rsi, r10
	jne	.LBB0_215
	jmp	.LBB0_351
.LBB0_283:
	xor	eax, eax
.LBB0_286:
	test	r11b, 1
	je	.LBB0_288
# %bb.287:
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
.LBB0_288:
	cmp	rsi, r10
	jne	.LBB0_289
.LBB0_351:
	cmp	edi, 6
	jg	.LBB0_364
# %bb.352:
	cmp	edi, 3
	jle	.LBB0_353
# %bb.358:
	cmp	edi, 4
	je	.LBB0_405
# %bb.359:
	cmp	edi, 5
	je	.LBB0_421
# %bb.360:
	cmp	edi, 6
	jne	.LBB0_524
# %bb.361:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.362:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_437
# %bb.363:
	xor	esi, esi
	jmp	.LBB0_446
.LBB0_364:
	cmp	edi, 8
	jle	.LBB0_365
# %bb.370:
	cmp	edi, 9
	je	.LBB0_479
# %bb.371:
	cmp	edi, 11
	je	.LBB0_495
# %bb.372:
	cmp	edi, 12
	jne	.LBB0_524
# %bb.373:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.374:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_511
# %bb.375:
	xor	esi, esi
	jmp	.LBB0_520
.LBB0_353:
	cmp	edi, 2
	je	.LBB0_376
# %bb.354:
	cmp	edi, 3
	jne	.LBB0_524
# %bb.355:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.356:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_392
# %bb.357:
	xor	esi, esi
	jmp	.LBB0_401
.LBB0_365:
	cmp	edi, 7
	je	.LBB0_450
# %bb.366:
	cmp	edi, 8
	jne	.LBB0_524
# %bb.367:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.368:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_466
# %bb.369:
	xor	esi, esi
	jmp	.LBB0_475
.LBB0_405:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.406:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_408
# %bb.407:
	xor	esi, esi
	jmp	.LBB0_417
.LBB0_421:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.422:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_424
# %bb.423:
	xor	esi, esi
	jmp	.LBB0_433
.LBB0_479:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.480:
	mov	r10d, r9d
	cmp	r9d, 4
	jae	.LBB0_482
# %bb.481:
	xor	esi, esi
	jmp	.LBB0_491
.LBB0_495:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.496:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_498
# %bb.497:
	xor	esi, esi
	jmp	.LBB0_507
.LBB0_376:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.377:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_379
# %bb.378:
	xor	esi, esi
	jmp	.LBB0_388
.LBB0_450:
	test	r9d, r9d
	jle	.LBB0_697
# %bb.451:
	mov	r10d, r9d
	cmp	r9d, 8
	jae	.LBB0_453
# %bb.452:
	xor	esi, esi
	jmp	.LBB0_462
.LBB0_697:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	ret
.LBB0_437:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_446
# %bb.438:
	and	al, r11b
	jne	.LBB0_446
# %bb.439:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_440
# %bb.441:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_442:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rax], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 32]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 48]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 48], xmm1
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_442
	jmp	.LBB0_443
.LBB0_511:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_520
# %bb.512:
	and	al, r11b
	jne	.LBB0_520
# %bb.513:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_514
# %bb.515:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_516:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm0, xmmword ptr [rdx + 8*rax]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rax]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rax + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rax], xmm0
	movupd	xmmword ptr [r8 + 8*rax + 16], xmm1
	movupd	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movupd	xmm2, xmmword ptr [rcx + 8*rax + 32]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rax + 48]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rax + 32], xmm0
	movupd	xmmword ptr [r8 + 8*rax + 48], xmm1
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_516
	jmp	.LBB0_517
.LBB0_392:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_401
# %bb.393:
	and	al, r11b
	jne	.LBB0_401
# %bb.394:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB0_395
# %bb.396:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_397:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rax + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rax], xmm0
	movdqu	xmmword ptr [r8 + rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + rax + 32]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rax + 48]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rax + 32], xmm0
	movdqu	xmmword ptr [r8 + rax + 48], xmm1
	add	rax, 64
	add	rbx, 2
	jne	.LBB0_397
	jmp	.LBB0_398
.LBB0_466:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_475
# %bb.467:
	and	al, r11b
	jne	.LBB0_475
# %bb.468:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_469
# %bb.470:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_471:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rax], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 32]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 48]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 48], xmm1
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_471
	jmp	.LBB0_472
.LBB0_408:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_417
# %bb.409:
	and	al, r11b
	jne	.LBB0_417
# %bb.410:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB0_411
# %bb.412:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_413:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rax], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 32]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 48]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 48], xmm1
	add	rax, 32
	add	rbx, 2
	jne	.LBB0_413
	jmp	.LBB0_414
.LBB0_424:
	lea	rsi, [r8 + 2*r10]
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_433
# %bb.425:
	and	al, r11b
	jne	.LBB0_433
# %bb.426:
	mov	esi, r10d
	and	esi, -16
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB0_427
# %bb.428:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_429:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rax], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 2*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 32]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 48]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 32], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 48], xmm1
	add	rax, 32
	add	rbx, 2
	jne	.LBB0_429
	jmp	.LBB0_430
.LBB0_482:
	lea	rsi, [r8 + 8*r10]
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_491
# %bb.483:
	and	al, r11b
	jne	.LBB0_491
# %bb.484:
	mov	esi, r10d
	and	esi, -4
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB0_485
# %bb.486:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_487:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rax], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 8*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 32]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 48]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 32], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 48], xmm1
	add	rax, 8
	add	rbx, 2
	jne	.LBB0_487
	jmp	.LBB0_488
.LBB0_498:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_507
# %bb.499:
	and	al, r11b
	jne	.LBB0_507
# %bb.500:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_501
# %bb.502:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_503:                              # =>This Inner Loop Header: Depth=1
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rax]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rax + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
	movups	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movups	xmm2, xmmword ptr [rcx + 4*rax + 32]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rax + 48]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rax + 32], xmm0
	movups	xmmword ptr [r8 + 4*rax + 48], xmm1
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_503
	jmp	.LBB0_504
.LBB0_379:
	lea	rsi, [r8 + r10]
	lea	rax, [rdx + r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_388
# %bb.380:
	and	al, r11b
	jne	.LBB0_388
# %bb.381:
	mov	esi, r10d
	and	esi, -32
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB0_382
# %bb.383:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_384:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rax + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rax], xmm0
	movdqu	xmmword ptr [r8 + rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + rax + 32]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rax + 48]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rax + 32], xmm0
	movdqu	xmmword ptr [r8 + rax + 48], xmm1
	add	rax, 64
	add	rbx, 2
	jne	.LBB0_384
	jmp	.LBB0_385
.LBB0_453:
	lea	rsi, [r8 + 4*r10]
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r10]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r11b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_462
# %bb.454:
	and	al, r11b
	jne	.LBB0_462
# %bb.455:
	mov	esi, r10d
	and	esi, -8
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB0_456
# %bb.457:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB0_458:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rax], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm1
	movdqu	xmm0, xmmword ptr [rdx + 4*rax + 32]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 48]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 32]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 48]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 32], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 48], xmm1
	add	rax, 16
	add	rbx, 2
	jne	.LBB0_458
	jmp	.LBB0_459
.LBB0_440:
	xor	eax, eax
.LBB0_443:
	test	r11b, 1
	je	.LBB0_445
# %bb.444:
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rax], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm1
.LBB0_445:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_446:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_448
	.p2align	4, 0x90
.LBB0_447:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_447
.LBB0_448:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_449:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_449
	jmp	.LBB0_524
.LBB0_514:
	xor	eax, eax
.LBB0_517:
	test	r11b, 1
	je	.LBB0_519
# %bb.518:
	movupd	xmm0, xmmword ptr [rdx + 8*rax]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rax]
	subpd	xmm0, xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rax + 16]
	subpd	xmm1, xmm2
	movupd	xmmword ptr [r8 + 8*rax], xmm0
	movupd	xmmword ptr [r8 + 8*rax + 16], xmm1
.LBB0_519:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_520:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_522
	.p2align	4, 0x90
.LBB0_521:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	subsd	xmm0, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_521
.LBB0_522:
	cmp	rax, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_523:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_523
	jmp	.LBB0_524
.LBB0_395:
	xor	eax, eax
.LBB0_398:
	test	r11b, 1
	je	.LBB0_400
# %bb.399:
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rax + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rax], xmm0
	movdqu	xmmword ptr [r8 + rax + 16], xmm1
.LBB0_400:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_401:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_403
	.p2align	4, 0x90
.LBB0_402:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_402
.LBB0_403:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_404:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_404
	jmp	.LBB0_524
.LBB0_469:
	xor	eax, eax
.LBB0_472:
	test	r11b, 1
	je	.LBB0_474
# %bb.473:
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rax], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm1
.LBB0_474:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_475:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_477
	.p2align	4, 0x90
.LBB0_476:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_476
.LBB0_477:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_478:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_478
	jmp	.LBB0_524
.LBB0_411:
	xor	eax, eax
.LBB0_414:
	test	r11b, 1
	je	.LBB0_416
# %bb.415:
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rax], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm1
.LBB0_416:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_417:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_419
	.p2align	4, 0x90
.LBB0_418:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_418
.LBB0_419:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_420:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_420
	jmp	.LBB0_524
.LBB0_427:
	xor	eax, eax
.LBB0_430:
	test	r11b, 1
	je	.LBB0_432
# %bb.431:
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	psubw	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 2*rax + 16]
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rax], xmm0
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm1
.LBB0_432:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_433:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_435
	.p2align	4, 0x90
.LBB0_434:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_434
.LBB0_435:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
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
	jmp	.LBB0_524
.LBB0_485:
	xor	eax, eax
.LBB0_488:
	test	r11b, 1
	je	.LBB0_490
# %bb.489:
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	psubq	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 8*rax + 16]
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rax], xmm0
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm1
.LBB0_490:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_491:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_493
	.p2align	4, 0x90
.LBB0_492:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_492
.LBB0_493:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
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
	jmp	.LBB0_524
.LBB0_501:
	xor	eax, eax
.LBB0_504:
	test	r11b, 1
	je	.LBB0_506
# %bb.505:
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rax]
	subps	xmm0, xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rax + 16]
	subps	xmm1, xmm2
	movups	xmmword ptr [r8 + 4*rax], xmm0
	movups	xmmword ptr [r8 + 4*rax + 16], xmm1
.LBB0_506:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_507:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_509
	.p2align	4, 0x90
.LBB0_508:                              # =>This Inner Loop Header: Depth=1
	movss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	subss	xmm0, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_508
.LBB0_509:
	cmp	rax, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_510
	jmp	.LBB0_524
.LBB0_382:
	xor	eax, eax
.LBB0_385:
	test	r11b, 1
	je	.LBB0_387
# %bb.386:
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	psubb	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + rax + 16]
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rax], xmm0
	movdqu	xmmword ptr [r8 + rax + 16], xmm1
.LBB0_387:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_388:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_390
	.p2align	4, 0x90
.LBB0_389:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_389
.LBB0_390:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
.LBB0_391:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_391
	jmp	.LBB0_524
.LBB0_456:
	xor	eax, eax
.LBB0_459:
	test	r11b, 1
	je	.LBB0_461
# %bb.460:
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	psubd	xmm0, xmm2
	movdqu	xmm2, xmmword ptr [rcx + 4*rax + 16]
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rax], xmm0
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm1
.LBB0_461:
	cmp	rsi, r10
	je	.LBB0_524
.LBB0_462:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB0_464
	.p2align	4, 0x90
.LBB0_463:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB0_463
.LBB0_464:
	cmp	r11, 3
	jb	.LBB0_524
	.p2align	4, 0x90
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
	jmp	.LBB0_524
.LBB0_94:
	xor	eax, eax
.LBB0_97:
	test	r11b, 1
	je	.LBB0_99
# %bb.98:
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
.LBB0_99:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_100
.LBB0_168:
	xor	eax, eax
.LBB0_171:
	test	r11b, 1
	je	.LBB0_173
# %bb.172:
	movupd	xmm0, xmmword ptr [rdx + 8*rax]
	movupd	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movupd	xmm2, xmmword ptr [rcx + 8*rax]
	addpd	xmm2, xmm0
	movupd	xmm0, xmmword ptr [rcx + 8*rax + 16]
	addpd	xmm0, xmm1
	movupd	xmmword ptr [r8 + 8*rax], xmm2
	movupd	xmmword ptr [r8 + 8*rax + 16], xmm0
.LBB0_173:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_174
.LBB0_49:
	xor	eax, eax
.LBB0_52:
	test	r11b, 1
	je	.LBB0_54
# %bb.53:
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
.LBB0_54:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_55
.LBB0_123:
	xor	eax, eax
.LBB0_126:
	test	r11b, 1
	je	.LBB0_128
# %bb.127:
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
.LBB0_128:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_129
.LBB0_65:
	xor	eax, eax
.LBB0_68:
	test	r11b, 1
	je	.LBB0_70
# %bb.69:
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
.LBB0_70:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_71
.LBB0_81:
	xor	eax, eax
.LBB0_84:
	test	r11b, 1
	je	.LBB0_86
# %bb.85:
	movdqu	xmm0, xmmword ptr [rdx + 2*rax]
	movdqu	xmm1, xmmword ptr [rdx + 2*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 2*rax]
	paddw	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 2*rax + 16]
	paddw	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 2*rax], xmm2
	movdqu	xmmword ptr [r8 + 2*rax + 16], xmm0
.LBB0_86:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_87
.LBB0_139:
	xor	eax, eax
.LBB0_142:
	test	r11b, 1
	je	.LBB0_144
# %bb.143:
	movdqu	xmm0, xmmword ptr [rdx + 8*rax]
	movdqu	xmm1, xmmword ptr [rdx + 8*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 8*rax]
	paddq	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 8*rax + 16]
	paddq	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 8*rax], xmm2
	movdqu	xmmword ptr [r8 + 8*rax + 16], xmm0
.LBB0_144:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_145
.LBB0_155:
	xor	eax, eax
.LBB0_158:
	test	r11b, 1
	je	.LBB0_160
# %bb.159:
	movups	xmm0, xmmword ptr [rdx + 4*rax]
	movups	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movups	xmm2, xmmword ptr [rcx + 4*rax]
	addps	xmm2, xmm0
	movups	xmm0, xmmword ptr [rcx + 4*rax + 16]
	addps	xmm0, xmm1
	movups	xmmword ptr [r8 + 4*rax], xmm2
	movups	xmmword ptr [r8 + 4*rax + 16], xmm0
.LBB0_160:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_161
.LBB0_36:
	xor	eax, eax
.LBB0_39:
	test	r11b, 1
	je	.LBB0_41
# %bb.40:
	movdqu	xmm0, xmmword ptr [rdx + rax]
	movdqu	xmm1, xmmword ptr [rdx + rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + rax]
	paddb	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + rax + 16]
	paddb	xmm0, xmm1
	movdqu	xmmword ptr [r8 + rax], xmm2
	movdqu	xmmword ptr [r8 + rax + 16], xmm0
.LBB0_41:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_42
.LBB0_110:
	xor	eax, eax
.LBB0_113:
	test	r11b, 1
	je	.LBB0_115
# %bb.114:
	movdqu	xmm0, xmmword ptr [rdx + 4*rax]
	movdqu	xmm1, xmmword ptr [rdx + 4*rax + 16]
	movdqu	xmm2, xmmword ptr [rcx + 4*rax]
	paddd	xmm2, xmm0
	movdqu	xmm0, xmmword ptr [rcx + 4*rax + 16]
	paddd	xmm0, xmm1
	movdqu	xmmword ptr [r8 + 4*rax], xmm2
	movdqu	xmmword ptr [r8 + 4*rax + 16], xmm0
.LBB0_115:
	cmp	rsi, r10
	je	.LBB0_178
	jmp	.LBB0_116
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
	push	r14
	push	rbx
	and	rsp, -8
	cmp	sil, 1
	jg	.LBB1_11
# %bb.1:
	test	sil, sil
	je	.LBB1_28
# %bb.2:
	cmp	sil, 1
	jne	.LBB1_517
.LBB1_3:
	cmp	edi, 6
	jg	.LBB1_36
# %bb.4:
	cmp	edi, 3
	jle	.LBB1_60
# %bb.5:
	cmp	edi, 4
	je	.LBB1_98
# %bb.6:
	cmp	edi, 5
	je	.LBB1_101
# %bb.7:
	cmp	edi, 6
	jne	.LBB1_474
# %bb.8:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.9:
	mov	r14d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_10
# %bb.164:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_254
# %bb.165:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_254
.LBB1_10:
	xor	esi, esi
.LBB1_398:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_400
	.p2align	4, 0x90
.LBB1_399:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_399
.LBB1_400:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_401:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_401
	jmp	.LBB1_474
.LBB1_11:
	cmp	sil, 2
	je	.LBB1_474
# %bb.12:
	cmp	sil, 3
	jne	.LBB1_517
.LBB1_13:
	cmp	edi, 6
	jg	.LBB1_21
# %bb.14:
	cmp	edi, 3
	jle	.LBB1_50
# %bb.15:
	cmp	edi, 4
	je	.LBB1_70
# %bb.16:
	cmp	edi, 5
	je	.LBB1_73
# %bb.17:
	cmp	edi, 6
	jne	.LBB1_517
# %bb.18:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.19:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_20
# %bb.134:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_194
# %bb.135:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_194
.LBB1_20:
	xor	esi, esi
.LBB1_318:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_320
	.p2align	4, 0x90
.LBB1_319:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, eax
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_319
.LBB1_320:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_321:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_321
	jmp	.LBB1_517
.LBB1_21:
	cmp	edi, 8
	jle	.LBB1_55
# %bb.22:
	cmp	edi, 9
	je	.LBB1_76
# %bb.23:
	cmp	edi, 11
	je	.LBB1_79
# %bb.24:
	cmp	edi, 12
	jne	.LBB1_517
# %bb.25:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.26:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB1_27
# %bb.137:
	lea	rcx, [rdx + 8*rax]
	cmp	rcx, r8
	jbe	.LBB1_197
# %bb.138:
	lea	rcx, [r8 + 8*rax]
	cmp	rcx, rdx
	jbe	.LBB1_197
.LBB1_27:
	xor	ecx, ecx
.LBB1_326:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_328
	.p2align	4, 0x90
.LBB1_327:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_327
.LBB1_328:
	cmp	rsi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_329:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_329
	jmp	.LBB1_517
.LBB1_28:
	cmp	edi, 6
	jg	.LBB1_43
# %bb.29:
	cmp	edi, 3
	jle	.LBB1_88
# %bb.30:
	cmp	edi, 4
	je	.LBB1_116
# %bb.31:
	cmp	edi, 5
	je	.LBB1_119
# %bb.32:
	cmp	edi, 6
	jne	.LBB1_3
# %bb.33:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.34:
	mov	r14d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_35
# %bb.224:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_284
# %bb.225:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_284
.LBB1_35:
	xor	esi, esi
.LBB1_662:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_664
.LBB1_663:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_663
.LBB1_664:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_665:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_665
	jmp	.LBB1_3
.LBB1_36:
	cmp	edi, 8
	jle	.LBB1_65
# %bb.37:
	cmp	edi, 9
	je	.LBB1_104
# %bb.38:
	cmp	edi, 11
	je	.LBB1_107
# %bb.39:
	cmp	edi, 12
	jne	.LBB1_474
# %bb.40:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.41:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB1_42
# %bb.167:
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	jbe	.LBB1_257
# %bb.168:
	lea	rax, [r8 + 8*r11]
	cmp	rax, rdx
	jbe	.LBB1_257
.LBB1_42:
	xor	esi, esi
.LBB1_406:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB1_408
	.p2align	4, 0x90
.LBB1_407:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_407
.LBB1_408:
	cmp	rax, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_409:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB1_409
	jmp	.LBB1_474
.LBB1_43:
	cmp	edi, 8
	jle	.LBB1_93
# %bb.44:
	cmp	edi, 9
	je	.LBB1_122
# %bb.45:
	cmp	edi, 11
	je	.LBB1_125
# %bb.46:
	cmp	edi, 12
	jne	.LBB1_3
# %bb.47:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.48:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB1_49
# %bb.227:
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	jbe	.LBB1_287
# %bb.228:
	lea	rax, [r8 + 8*r11]
	cmp	rax, rdx
	jbe	.LBB1_287
.LBB1_49:
	xor	esi, esi
.LBB1_670:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB1_672
.LBB1_671:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_671
.LBB1_672:
	cmp	rax, 3
	jb	.LBB1_3
.LBB1_673:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB1_673
	jmp	.LBB1_3
.LBB1_50:
	cmp	edi, 2
	je	.LBB1_82
# %bb.51:
	cmp	edi, 3
	jne	.LBB1_517
# %bb.52:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.53:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_54
# %bb.140:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_200
# %bb.141:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_200
.LBB1_54:
	xor	esi, esi
.LBB1_334:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_336
	.p2align	4, 0x90
.LBB1_335:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, al
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_335
.LBB1_336:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_337:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_337
	jmp	.LBB1_517
.LBB1_55:
	cmp	edi, 7
	je	.LBB1_85
# %bb.56:
	cmp	edi, 8
	jne	.LBB1_517
# %bb.57:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.58:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_59
# %bb.143:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_203
# %bb.144:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_203
.LBB1_59:
	xor	esi, esi
.LBB1_342:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_344
	.p2align	4, 0x90
.LBB1_343:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, rax
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_343
.LBB1_344:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_345:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_345
	jmp	.LBB1_517
.LBB1_60:
	cmp	edi, 2
	je	.LBB1_110
# %bb.61:
	cmp	edi, 3
	jne	.LBB1_474
# %bb.62:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.63:
	mov	r14b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_64
# %bb.170:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_260
# %bb.171:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_260
.LBB1_64:
	xor	esi, esi
.LBB1_414:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_416
	.p2align	4, 0x90
.LBB1_415:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_415
.LBB1_416:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_417:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_417
	jmp	.LBB1_474
.LBB1_65:
	cmp	edi, 7
	je	.LBB1_113
# %bb.66:
	cmp	edi, 8
	jne	.LBB1_474
# %bb.67:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.68:
	mov	r14, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_69
# %bb.173:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_263
# %bb.174:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_263
.LBB1_69:
	xor	esi, esi
.LBB1_422:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_424
	.p2align	4, 0x90
.LBB1_423:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_423
.LBB1_424:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_425:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_425
	jmp	.LBB1_474
.LBB1_70:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.71:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_72
# %bb.146:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_206
# %bb.147:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_206
.LBB1_72:
	xor	esi, esi
.LBB1_350:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_352
	.p2align	4, 0x90
.LBB1_351:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	ebx, eax
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_351
.LBB1_352:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_353:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_353
	jmp	.LBB1_517
.LBB1_73:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.74:
	movzx	eax, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_75
# %bb.149:
	lea	rcx, [rdx + 2*r10]
	cmp	rcx, r8
	jbe	.LBB1_209
# %bb.150:
	lea	rcx, [r8 + 2*r10]
	cmp	rcx, rdx
	jbe	.LBB1_209
.LBB1_75:
	xor	esi, esi
.LBB1_358:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_360
	.p2align	4, 0x90
.LBB1_359:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	ebx, eax
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_359
.LBB1_360:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_361:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_361
	jmp	.LBB1_517
.LBB1_76:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.77:
	mov	rax, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_78
# %bb.152:
	lea	rcx, [rdx + 8*r10]
	cmp	rcx, r8
	jbe	.LBB1_212
# %bb.153:
	lea	rcx, [r8 + 8*r10]
	cmp	rcx, rdx
	jbe	.LBB1_212
.LBB1_78:
	xor	esi, esi
.LBB1_366:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_368
	.p2align	4, 0x90
.LBB1_367:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, rax
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_367
.LBB1_368:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_369:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_369
	jmp	.LBB1_517
.LBB1_79:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.80:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB1_81
# %bb.155:
	lea	rcx, [rdx + 4*rax]
	cmp	rcx, r8
	jbe	.LBB1_215
# %bb.156:
	lea	rcx, [r8 + 4*rax]
	cmp	rcx, rdx
	jbe	.LBB1_215
.LBB1_81:
	xor	ecx, ecx
.LBB1_374:
	mov	rsi, rcx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB1_376
	.p2align	4, 0x90
.LBB1_375:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_375
.LBB1_376:
	cmp	rsi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_377
	jmp	.LBB1_517
.LBB1_82:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.83:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_84
# %bb.158:
	lea	rcx, [rdx + r10]
	cmp	rcx, r8
	jbe	.LBB1_218
# %bb.159:
	lea	rcx, [r8 + r10]
	cmp	rcx, rdx
	jbe	.LBB1_218
.LBB1_84:
	xor	esi, esi
.LBB1_382:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_384
	.p2align	4, 0x90
.LBB1_383:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, al
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_383
.LBB1_384:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_385:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_385
	jmp	.LBB1_517
.LBB1_85:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.86:
	mov	eax, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_87
# %bb.161:
	lea	rcx, [rdx + 4*r10]
	cmp	rcx, r8
	jbe	.LBB1_221
# %bb.162:
	lea	rcx, [r8 + 4*r10]
	cmp	rcx, rdx
	jbe	.LBB1_221
.LBB1_87:
	xor	esi, esi
.LBB1_390:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rcx, r10
	and	rcx, 3
	je	.LBB1_392
	.p2align	4, 0x90
.LBB1_391:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, eax
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rcx, -1
	jne	.LBB1_391
.LBB1_392:
	cmp	rdi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_393:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_393
	jmp	.LBB1_517
.LBB1_88:
	cmp	edi, 2
	je	.LBB1_128
# %bb.89:
	cmp	edi, 3
	jne	.LBB1_3
# %bb.90:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.91:
	mov	r14b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_92
# %bb.230:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_290
# %bb.231:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_290
.LBB1_92:
	xor	esi, esi
.LBB1_678:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_680
.LBB1_679:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_679
.LBB1_680:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_681:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_681
	jmp	.LBB1_3
.LBB1_93:
	cmp	edi, 7
	je	.LBB1_131
# %bb.94:
	cmp	edi, 8
	jne	.LBB1_3
# %bb.95:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.96:
	mov	r14, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_97
# %bb.233:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_293
# %bb.234:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_293
.LBB1_97:
	xor	esi, esi
.LBB1_686:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_688
.LBB1_687:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_687
.LBB1_688:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_689:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_689
	jmp	.LBB1_3
.LBB1_98:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.99:
	movzx	r14d, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_100
# %bb.176:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_266
# %bb.177:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_266
.LBB1_100:
	xor	esi, esi
.LBB1_430:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_432
	.p2align	4, 0x90
.LBB1_431:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_431
.LBB1_432:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_433:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_433
	jmp	.LBB1_474
.LBB1_101:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.102:
	movzx	r14d, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_103
# %bb.179:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_269
# %bb.180:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_269
.LBB1_103:
	xor	esi, esi
.LBB1_438:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_440
	.p2align	4, 0x90
.LBB1_439:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_439
.LBB1_440:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_441:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_441
	jmp	.LBB1_474
.LBB1_104:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.105:
	mov	r14, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_106
# %bb.182:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_272
# %bb.183:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_272
.LBB1_106:
	xor	esi, esi
.LBB1_446:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_448
	.p2align	4, 0x90
.LBB1_447:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_447
.LBB1_448:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_449:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_449
	jmp	.LBB1_474
.LBB1_107:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.108:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB1_109
# %bb.185:
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	jbe	.LBB1_275
# %bb.186:
	lea	rax, [r8 + 4*r11]
	cmp	rax, rdx
	jbe	.LBB1_275
.LBB1_109:
	xor	esi, esi
.LBB1_454:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB1_456
	.p2align	4, 0x90
.LBB1_455:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_455
.LBB1_456:
	cmp	rax, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_457:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB1_457
	jmp	.LBB1_474
.LBB1_110:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.111:
	mov	r14b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_112
# %bb.188:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_278
# %bb.189:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_278
.LBB1_112:
	xor	esi, esi
.LBB1_462:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_464
	.p2align	4, 0x90
.LBB1_463:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_463
.LBB1_464:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_465:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_465
	jmp	.LBB1_474
.LBB1_113:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.114:
	mov	r14d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_115
# %bb.191:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_281
# %bb.192:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_281
.LBB1_115:
	xor	esi, esi
.LBB1_470:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_472
	.p2align	4, 0x90
.LBB1_471:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_471
.LBB1_472:
	cmp	r11, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_473:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_473
	jmp	.LBB1_474
.LBB1_116:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.117:
	movzx	r14d, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_118
# %bb.236:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_296
# %bb.237:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_296
.LBB1_118:
	xor	esi, esi
.LBB1_694:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_696
.LBB1_695:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_695
.LBB1_696:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_697:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_697
	jmp	.LBB1_3
.LBB1_119:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.120:
	movzx	r14d, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_121
# %bb.239:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_299
# %bb.240:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_299
.LBB1_121:
	xor	esi, esi
.LBB1_702:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_704
.LBB1_703:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_703
.LBB1_704:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_705:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_705
	jmp	.LBB1_3
.LBB1_122:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.123:
	mov	r14, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_124
# %bb.242:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_302
# %bb.243:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_302
.LBB1_124:
	xor	esi, esi
.LBB1_710:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_712
.LBB1_711:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_711
.LBB1_712:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_713:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_713
	jmp	.LBB1_3
.LBB1_125:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.126:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB1_127
# %bb.245:
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	jbe	.LBB1_305
# %bb.246:
	lea	rax, [r8 + 4*r11]
	cmp	rax, rdx
	jbe	.LBB1_305
.LBB1_127:
	xor	esi, esi
.LBB1_718:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB1_720
.LBB1_719:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_719
.LBB1_720:
	cmp	rax, 3
	jb	.LBB1_3
.LBB1_721:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB1_721
	jmp	.LBB1_3
.LBB1_128:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.129:
	mov	r14b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_130
# %bb.248:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_308
# %bb.249:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_308
.LBB1_130:
	xor	esi, esi
.LBB1_726:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_728
.LBB1_727:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_727
.LBB1_728:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_729:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_729
	jmp	.LBB1_3
.LBB1_131:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.132:
	mov	r14d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_133
# %bb.251:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_311
# %bb.252:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_311
.LBB1_133:
	xor	esi, esi
.LBB1_734:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_736
.LBB1_735:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_735
.LBB1_736:
	cmp	r11, 3
	jb	.LBB1_3
.LBB1_737:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_737
	jmp	.LBB1_3
.LBB1_194:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_314
# %bb.195:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_196:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rdi, 2
	jne	.LBB1_196
	jmp	.LBB1_315
.LBB1_197:
	mov	ecx, eax
	and	ecx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rcx - 4]
	mov	rbx, rsi
	shr	rbx, 2
	add	rbx, 1
	test	rsi, rsi
	je	.LBB1_322
# %bb.198:
	mov	rsi, rbx
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_199:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_199
	jmp	.LBB1_323
.LBB1_200:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_330
# %bb.201:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_202:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rdi, 2
	jne	.LBB1_202
	jmp	.LBB1_331
.LBB1_203:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_338
# %bb.204:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_205:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rdi, 2
	jne	.LBB1_205
	jmp	.LBB1_339
.LBB1_206:
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
	je	.LBB1_346
# %bb.207:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_208:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rdi, 2
	jne	.LBB1_208
	jmp	.LBB1_347
.LBB1_209:
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
	je	.LBB1_354
# %bb.210:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_211:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rdi, 2
	jne	.LBB1_211
	jmp	.LBB1_355
.LBB1_212:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, rax
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rcx, [rsi - 4]
	mov	r9, rcx
	shr	r9, 2
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_362
# %bb.213:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_214:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rdi, 2
	jne	.LBB1_214
	jmp	.LBB1_363
.LBB1_215:
	mov	ecx, eax
	and	ecx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rcx - 8]
	mov	rbx, rsi
	shr	rbx, 3
	add	rbx, 1
	test	rsi, rsi
	je	.LBB1_370
# %bb.216:
	mov	rsi, rbx
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB1_217:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_217
	jmp	.LBB1_371
.LBB1_218:
	mov	esi, r10d
	and	esi, -32
	movzx	ecx, al
	movd	xmm0, ecx
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_378
# %bb.219:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_220:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rdi, 2
	jne	.LBB1_220
	jmp	.LBB1_379
.LBB1_221:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, eax
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rcx, [rsi - 8]
	mov	r9, rcx
	shr	r9, 3
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_386
# %bb.222:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_223:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rdi, 2
	jne	.LBB1_223
	jmp	.LBB1_387
.LBB1_254:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB1_394
# %bb.255:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_256:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_256
	jmp	.LBB1_395
.LBB1_257:
	mov	esi, r11d
	and	esi, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	test	rax, rax
	je	.LBB1_402
# %bb.258:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_259:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rbx]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rbx + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 48], xmm3
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_259
	jmp	.LBB1_403
.LBB1_260:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_410
# %bb.261:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_262:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_262
	jmp	.LBB1_411
.LBB1_263:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB1_418
# %bb.264:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_265:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_265
	jmp	.LBB1_419
.LBB1_266:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_426
# %bb.267:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_268:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_268
	jmp	.LBB1_427
.LBB1_269:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_434
# %bb.270:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_271:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_271
	jmp	.LBB1_435
.LBB1_272:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB1_442
# %bb.273:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_274:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_274
	jmp	.LBB1_443
.LBB1_275:
	mov	esi, r11d
	and	esi, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rax, [rsi - 8]
	mov	r10, rax
	shr	r10, 3
	add	r10, 1
	test	rax, rax
	je	.LBB1_450
# %bb.276:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_277:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rbx]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rbx + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx + 32], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 48], xmm3
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_277
	jmp	.LBB1_451
.LBB1_278:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_458
# %bb.279:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_280:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_280
	jmp	.LBB1_459
.LBB1_281:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB1_466
# %bb.282:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_283:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_283
	jmp	.LBB1_467
.LBB1_284:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB1_658
# %bb.285:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_286:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_286
	jmp	.LBB1_659
.LBB1_287:
	mov	esi, r11d
	and	esi, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	test	rax, rax
	je	.LBB1_666
# %bb.288:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_289:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rbx]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rbx + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 48], xmm3
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_289
	jmp	.LBB1_667
.LBB1_290:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_674
# %bb.291:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_292:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_292
	jmp	.LBB1_675
.LBB1_293:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB1_682
# %bb.294:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_295:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_295
	jmp	.LBB1_683
.LBB1_296:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_690
# %bb.297:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_298:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_298
	jmp	.LBB1_691
.LBB1_299:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_698
# %bb.300:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_301:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_301
	jmp	.LBB1_699
.LBB1_302:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB1_706
# %bb.303:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_304:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_304
	jmp	.LBB1_707
.LBB1_305:
	mov	esi, r11d
	and	esi, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rax, [rsi - 8]
	mov	r10, rax
	shr	r10, 3
	add	r10, 1
	test	rax, rax
	je	.LBB1_714
# %bb.306:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_307:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rbx]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rbx + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx + 32], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 48], xmm3
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_307
	jmp	.LBB1_715
.LBB1_308:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_722
# %bb.309:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_310:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_310
	jmp	.LBB1_723
.LBB1_311:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB1_730
# %bb.312:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_313:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_313
	jmp	.LBB1_731
.LBB1_314:
	xor	ebx, ebx
.LBB1_315:
	test	r9b, 1
	je	.LBB1_317
# %bb.316:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_317:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_318
.LBB1_322:
	xor	edi, edi
.LBB1_323:
	test	bl, 1
	je	.LBB1_325
# %bb.324:
	movupd	xmm2, xmmword ptr [rdx + 8*rdi]
	movupd	xmm3, xmmword ptr [rdx + 8*rdi + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rdi], xmm2
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm3
.LBB1_325:
	cmp	rcx, rax
	je	.LBB1_517
	jmp	.LBB1_326
.LBB1_330:
	xor	ebx, ebx
.LBB1_331:
	test	r9b, 1
	je	.LBB1_333
# %bb.332:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_333:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_334
.LBB1_338:
	xor	ebx, ebx
.LBB1_339:
	test	r9b, 1
	je	.LBB1_341
# %bb.340:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_341:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_342
.LBB1_346:
	xor	ebx, ebx
.LBB1_347:
	test	r9b, 1
	je	.LBB1_349
# %bb.348:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_349:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_350
.LBB1_354:
	xor	ebx, ebx
.LBB1_355:
	test	r9b, 1
	je	.LBB1_357
# %bb.356:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_357:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_358
.LBB1_362:
	xor	ebx, ebx
.LBB1_363:
	test	r9b, 1
	je	.LBB1_365
# %bb.364:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_365:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_366
.LBB1_370:
	xor	edi, edi
.LBB1_371:
	test	bl, 1
	je	.LBB1_373
# %bb.372:
	movups	xmm2, xmmword ptr [rdx + 4*rdi]
	movups	xmm3, xmmword ptr [rdx + 4*rdi + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rdi], xmm2
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm3
.LBB1_373:
	cmp	rcx, rax
	je	.LBB1_517
	jmp	.LBB1_374
.LBB1_378:
	xor	ebx, ebx
.LBB1_379:
	test	r9b, 1
	je	.LBB1_381
# %bb.380:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_381:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_382
.LBB1_386:
	xor	ebx, ebx
.LBB1_387:
	test	r9b, 1
	je	.LBB1_389
# %bb.388:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_389:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_390
.LBB1_394:
	xor	ebx, ebx
.LBB1_395:
	test	r11b, 1
	je	.LBB1_397
# %bb.396:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_397:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_398
.LBB1_402:
	xor	ebx, ebx
.LBB1_403:
	test	r10b, 1
	je	.LBB1_405
# %bb.404:
	movupd	xmm2, xmmword ptr [rdx + 8*rbx]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
.LBB1_405:
	cmp	rsi, r11
	je	.LBB1_474
	jmp	.LBB1_406
.LBB1_410:
	xor	ebx, ebx
.LBB1_411:
	test	r11b, 1
	je	.LBB1_413
# %bb.412:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_413:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_414
.LBB1_418:
	xor	ebx, ebx
.LBB1_419:
	test	r11b, 1
	je	.LBB1_421
# %bb.420:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_421:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_422
.LBB1_426:
	xor	ebx, ebx
.LBB1_427:
	test	r11b, 1
	je	.LBB1_429
# %bb.428:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_429:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_430
.LBB1_434:
	xor	ebx, ebx
.LBB1_435:
	test	r11b, 1
	je	.LBB1_437
# %bb.436:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_437:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_438
.LBB1_442:
	xor	ebx, ebx
.LBB1_443:
	test	r11b, 1
	je	.LBB1_445
# %bb.444:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_445:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_446
.LBB1_450:
	xor	ebx, ebx
.LBB1_451:
	test	r10b, 1
	je	.LBB1_453
# %bb.452:
	movups	xmm2, xmmword ptr [rdx + 4*rbx]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
.LBB1_453:
	cmp	rsi, r11
	je	.LBB1_474
	jmp	.LBB1_454
.LBB1_458:
	xor	ebx, ebx
.LBB1_459:
	test	r11b, 1
	je	.LBB1_461
# %bb.460:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_461:
	cmp	rsi, r10
	je	.LBB1_474
	jmp	.LBB1_462
.LBB1_466:
	xor	ebx, ebx
.LBB1_467:
	test	r11b, 1
	je	.LBB1_469
# %bb.468:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_469:
	cmp	rsi, r10
	jne	.LBB1_470
.LBB1_474:
	cmp	edi, 6
	jg	.LBB1_482
# %bb.475:
	cmp	edi, 3
	jle	.LBB1_489
# %bb.476:
	cmp	edi, 4
	je	.LBB1_499
# %bb.477:
	cmp	edi, 5
	je	.LBB1_502
# %bb.478:
	cmp	edi, 6
	jne	.LBB1_13
# %bb.479:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.480:
	mov	r14d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_481
# %bb.518:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_548
# %bb.519:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_548
.LBB1_481:
	xor	esi, esi
.LBB1_582:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_584
	.p2align	4, 0x90
.LBB1_583:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_583
.LBB1_584:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_585:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_585
	jmp	.LBB1_13
.LBB1_482:
	cmp	edi, 8
	jle	.LBB1_494
# %bb.483:
	cmp	edi, 9
	je	.LBB1_505
# %bb.484:
	cmp	edi, 11
	je	.LBB1_508
# %bb.485:
	cmp	edi, 12
	jne	.LBB1_13
# %bb.486:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.487:
	movsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB1_488
# %bb.521:
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	jbe	.LBB1_551
# %bb.522:
	lea	rax, [r8 + 8*r11]
	cmp	rax, rdx
	jbe	.LBB1_551
.LBB1_488:
	xor	esi, esi
.LBB1_590:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB1_592
	.p2align	4, 0x90
.LBB1_591:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_591
.LBB1_592:
	cmp	rax, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_593:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	subsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB1_593
	jmp	.LBB1_13
.LBB1_489:
	cmp	edi, 2
	je	.LBB1_511
# %bb.490:
	cmp	edi, 3
	jne	.LBB1_13
# %bb.491:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.492:
	mov	r14b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_493
# %bb.524:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_554
# %bb.525:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_554
.LBB1_493:
	xor	esi, esi
.LBB1_598:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_600
	.p2align	4, 0x90
.LBB1_599:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_599
.LBB1_600:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_601:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_601
	jmp	.LBB1_13
.LBB1_494:
	cmp	edi, 7
	je	.LBB1_514
# %bb.495:
	cmp	edi, 8
	jne	.LBB1_13
# %bb.496:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.497:
	mov	r14, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_498
# %bb.527:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_557
# %bb.528:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_557
.LBB1_498:
	xor	esi, esi
.LBB1_606:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_608
	.p2align	4, 0x90
.LBB1_607:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_607
.LBB1_608:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_609:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_609
	jmp	.LBB1_13
.LBB1_499:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.500:
	movzx	r14d, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_501
# %bb.530:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_560
# %bb.531:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_560
.LBB1_501:
	xor	esi, esi
.LBB1_614:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_616
	.p2align	4, 0x90
.LBB1_615:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_615
.LBB1_616:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_617:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_617
	jmp	.LBB1_13
.LBB1_502:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.503:
	movzx	r14d, word ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_504
# %bb.533:
	lea	rax, [rdx + 2*r10]
	cmp	rax, r8
	jbe	.LBB1_563
# %bb.534:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rdx
	jbe	.LBB1_563
.LBB1_504:
	xor	esi, esi
.LBB1_622:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_624
	.p2align	4, 0x90
.LBB1_623:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_623
.LBB1_624:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_625:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdx + 2*rsi]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rdx + 2*rsi + 2]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rdx + 2*rsi + 4]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rdx + 2*rsi + 6]
	sub	eax, r14d
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_625
	jmp	.LBB1_13
.LBB1_505:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.506:
	mov	r14, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB1_507
# %bb.536:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_566
# %bb.537:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_566
.LBB1_507:
	xor	esi, esi
.LBB1_630:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_632
	.p2align	4, 0x90
.LBB1_631:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_631
.LBB1_632:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_633:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_633
	jmp	.LBB1_13
.LBB1_508:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.509:
	movss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB1_510
# %bb.539:
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	jbe	.LBB1_569
# %bb.540:
	lea	rax, [r8 + 4*r11]
	cmp	rax, rdx
	jbe	.LBB1_569
.LBB1_510:
	xor	esi, esi
.LBB1_638:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB1_640
	.p2align	4, 0x90
.LBB1_639:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_639
.LBB1_640:
	cmp	rax, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_641:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	subss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB1_641
	jmp	.LBB1_13
.LBB1_511:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.512:
	mov	r14b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_513
# %bb.542:
	lea	rax, [rdx + r10]
	cmp	rax, r8
	jbe	.LBB1_572
# %bb.543:
	lea	rax, [r8 + r10]
	cmp	rax, rdx
	jbe	.LBB1_572
.LBB1_513:
	xor	esi, esi
.LBB1_646:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_648
	.p2align	4, 0x90
.LBB1_647:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_647
.LBB1_648:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_649:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_649
	jmp	.LBB1_13
.LBB1_514:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.515:
	mov	r14d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB1_516
# %bb.545:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_575
# %bb.546:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_575
.LBB1_516:
	xor	esi, esi
.LBB1_654:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_656
	.p2align	4, 0x90
.LBB1_655:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_655
.LBB1_656:
	cmp	r11, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_657:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_657
	jmp	.LBB1_13
.LBB1_517:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	ret
.LBB1_548:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB1_578
# %bb.549:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_550:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_550
	jmp	.LBB1_579
.LBB1_551:
	mov	esi, r11d
	and	esi, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	test	rax, rax
	je	.LBB1_586
# %bb.552:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_553:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rdx + 8*rbx]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
	movupd	xmm2, xmmword ptr [rdx + 8*rbx + 32]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 48]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 48], xmm3
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_553
	jmp	.LBB1_587
.LBB1_554:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_594
# %bb.555:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_556:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_556
	jmp	.LBB1_595
.LBB1_557:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB1_602
# %bb.558:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_559:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_559
	jmp	.LBB1_603
.LBB1_560:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_610
# %bb.561:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_562:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_562
	jmp	.LBB1_611
.LBB1_563:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_618
# %bb.564:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_565:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 48]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_565
	jmp	.LBB1_619
.LBB1_566:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB1_626
# %bb.567:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_568:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 48]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB1_568
	jmp	.LBB1_627
.LBB1_569:
	mov	esi, r11d
	and	esi, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rax, [rsi - 8]
	mov	r10, rax
	shr	r10, 3
	add	r10, 1
	test	rax, rax
	je	.LBB1_634
# %bb.570:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_571:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rdx + 4*rbx]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
	movups	xmm2, xmmword ptr [rdx + 4*rbx + 32]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 48]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx + 32], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 48], xmm3
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_571
	jmp	.LBB1_635
.LBB1_572:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_642
# %bb.573:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_574:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 48]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_574
	jmp	.LBB1_643
.LBB1_575:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB1_650
# %bb.576:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_577:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 48]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB1_577
	jmp	.LBB1_651
.LBB1_578:
	xor	ebx, ebx
.LBB1_579:
	test	r11b, 1
	je	.LBB1_581
# %bb.580:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_581:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_582
.LBB1_586:
	xor	ebx, ebx
.LBB1_587:
	test	r10b, 1
	je	.LBB1_589
# %bb.588:
	movupd	xmm2, xmmword ptr [rdx + 8*rbx]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 16]
	subpd	xmm2, xmm1
	subpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
.LBB1_589:
	cmp	rsi, r11
	je	.LBB1_13
	jmp	.LBB1_590
.LBB1_594:
	xor	ebx, ebx
.LBB1_595:
	test	r11b, 1
	je	.LBB1_597
# %bb.596:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_597:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_598
.LBB1_602:
	xor	ebx, ebx
.LBB1_603:
	test	r11b, 1
	je	.LBB1_605
# %bb.604:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_605:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_606
.LBB1_610:
	xor	ebx, ebx
.LBB1_611:
	test	r11b, 1
	je	.LBB1_613
# %bb.612:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_613:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_614
.LBB1_618:
	xor	ebx, ebx
.LBB1_619:
	test	r11b, 1
	je	.LBB1_621
# %bb.620:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	psubw	xmm1, xmm0
	psubw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_621:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_622
.LBB1_626:
	xor	ebx, ebx
.LBB1_627:
	test	r11b, 1
	je	.LBB1_629
# %bb.628:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	psubq	xmm1, xmm0
	psubq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_629:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_630
.LBB1_634:
	xor	ebx, ebx
.LBB1_635:
	test	r10b, 1
	je	.LBB1_637
# %bb.636:
	movups	xmm2, xmmword ptr [rdx + 4*rbx]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 16]
	subps	xmm2, xmm1
	subps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
.LBB1_637:
	cmp	rsi, r11
	je	.LBB1_13
	jmp	.LBB1_638
.LBB1_642:
	xor	ebx, ebx
.LBB1_643:
	test	r11b, 1
	je	.LBB1_645
# %bb.644:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	psubb	xmm1, xmm0
	psubb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_645:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_646
.LBB1_650:
	xor	ebx, ebx
.LBB1_651:
	test	r11b, 1
	je	.LBB1_653
# %bb.652:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	psubd	xmm1, xmm0
	psubd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_653:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_654
.LBB1_658:
	xor	ebx, ebx
.LBB1_659:
	test	r11b, 1
	je	.LBB1_661
# %bb.660:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_661:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_662
.LBB1_666:
	xor	ebx, ebx
.LBB1_667:
	test	r10b, 1
	je	.LBB1_669
# %bb.668:
	movupd	xmm2, xmmword ptr [rdx + 8*rbx]
	movupd	xmm3, xmmword ptr [rdx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
.LBB1_669:
	cmp	rsi, r11
	je	.LBB1_3
	jmp	.LBB1_670
.LBB1_674:
	xor	ebx, ebx
.LBB1_675:
	test	r11b, 1
	je	.LBB1_677
# %bb.676:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_677:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_678
.LBB1_682:
	xor	ebx, ebx
.LBB1_683:
	test	r11b, 1
	je	.LBB1_685
# %bb.684:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_685:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_686
.LBB1_690:
	xor	ebx, ebx
.LBB1_691:
	test	r11b, 1
	je	.LBB1_693
# %bb.692:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_693:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_694
.LBB1_698:
	xor	ebx, ebx
.LBB1_699:
	test	r11b, 1
	je	.LBB1_701
# %bb.700:
	movdqu	xmm1, xmmword ptr [rdx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB1_701:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_702
.LBB1_706:
	xor	ebx, ebx
.LBB1_707:
	test	r11b, 1
	je	.LBB1_709
# %bb.708:
	movdqu	xmm1, xmmword ptr [rdx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB1_709:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_710
.LBB1_714:
	xor	ebx, ebx
.LBB1_715:
	test	r10b, 1
	je	.LBB1_717
# %bb.716:
	movups	xmm2, xmmword ptr [rdx + 4*rbx]
	movups	xmm3, xmmword ptr [rdx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
.LBB1_717:
	cmp	rsi, r11
	je	.LBB1_3
	jmp	.LBB1_718
.LBB1_722:
	xor	ebx, ebx
.LBB1_723:
	test	r11b, 1
	je	.LBB1_725
# %bb.724:
	movdqu	xmm1, xmmword ptr [rdx + rbx]
	movdqu	xmm2, xmmword ptr [rdx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB1_725:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_726
.LBB1_730:
	xor	ebx, ebx
.LBB1_731:
	test	r11b, 1
	je	.LBB1_733
# %bb.732:
	movdqu	xmm1, xmmword ptr [rdx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rdx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB1_733:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_734
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
	push	r14
	push	rbx
	and	rsp, -8
	cmp	sil, 1
	jg	.LBB2_11
# %bb.1:
	test	sil, sil
	je	.LBB2_28
# %bb.2:
	cmp	sil, 1
	jne	.LBB2_517
.LBB2_3:
	cmp	edi, 6
	jg	.LBB2_36
# %bb.4:
	cmp	edi, 3
	jle	.LBB2_60
# %bb.5:
	cmp	edi, 4
	je	.LBB2_98
# %bb.6:
	cmp	edi, 5
	je	.LBB2_101
# %bb.7:
	cmp	edi, 6
	jne	.LBB2_474
# %bb.8:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.9:
	mov	r14d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_10
# %bb.164:
	lea	rax, [rcx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_254
# %bb.165:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rcx
	jbe	.LBB2_254
.LBB2_10:
	xor	esi, esi
.LBB2_398:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_400
	.p2align	4, 0x90
.LBB2_399:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_399
.LBB2_400:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_401:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_401
	jmp	.LBB2_474
.LBB2_11:
	cmp	sil, 2
	je	.LBB2_474
# %bb.12:
	cmp	sil, 3
	jne	.LBB2_517
.LBB2_13:
	cmp	edi, 6
	jg	.LBB2_21
# %bb.14:
	cmp	edi, 3
	jle	.LBB2_50
# %bb.15:
	cmp	edi, 4
	je	.LBB2_70
# %bb.16:
	cmp	edi, 5
	je	.LBB2_73
# %bb.17:
	cmp	edi, 6
	jne	.LBB2_517
# %bb.18:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.19:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_20
# %bb.134:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_194
# %bb.135:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_194
.LBB2_20:
	xor	esi, esi
.LBB2_318:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_320
	.p2align	4, 0x90
.LBB2_319:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_319
.LBB2_320:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_321:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_321
	jmp	.LBB2_517
.LBB2_21:
	cmp	edi, 8
	jle	.LBB2_55
# %bb.22:
	cmp	edi, 9
	je	.LBB2_76
# %bb.23:
	cmp	edi, 11
	je	.LBB2_79
# %bb.24:
	cmp	edi, 12
	jne	.LBB2_517
# %bb.25:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.26:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 4
	jb	.LBB2_27
# %bb.137:
	lea	rdx, [rcx + 8*rax]
	cmp	rdx, r8
	jbe	.LBB2_197
# %bb.138:
	lea	rdx, [r8 + 8*rax]
	cmp	rdx, rcx
	jbe	.LBB2_197
.LBB2_27:
	xor	edx, edx
.LBB2_326:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_328
	.p2align	4, 0x90
.LBB2_327:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rdx]
	movsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_327
.LBB2_328:
	cmp	rsi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_329:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_329
	jmp	.LBB2_517
.LBB2_28:
	cmp	edi, 6
	jg	.LBB2_43
# %bb.29:
	cmp	edi, 3
	jle	.LBB2_88
# %bb.30:
	cmp	edi, 4
	je	.LBB2_116
# %bb.31:
	cmp	edi, 5
	je	.LBB2_119
# %bb.32:
	cmp	edi, 6
	jne	.LBB2_3
# %bb.33:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.34:
	mov	r14d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_35
# %bb.224:
	lea	rax, [rcx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_284
# %bb.225:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rcx
	jbe	.LBB2_284
.LBB2_35:
	xor	esi, esi
.LBB2_662:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_664
.LBB2_663:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_663
.LBB2_664:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_665:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_665
	jmp	.LBB2_3
.LBB2_36:
	cmp	edi, 8
	jle	.LBB2_65
# %bb.37:
	cmp	edi, 9
	je	.LBB2_104
# %bb.38:
	cmp	edi, 11
	je	.LBB2_107
# %bb.39:
	cmp	edi, 12
	jne	.LBB2_474
# %bb.40:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.41:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB2_42
# %bb.167:
	lea	rax, [rcx + 8*r11]
	cmp	rax, r8
	jbe	.LBB2_257
# %bb.168:
	lea	rax, [r8 + 8*r11]
	cmp	rax, rcx
	jbe	.LBB2_257
.LBB2_42:
	xor	esi, esi
.LBB2_406:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB2_408
	.p2align	4, 0x90
.LBB2_407:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_407
.LBB2_408:
	cmp	rax, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_409:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB2_409
	jmp	.LBB2_474
.LBB2_43:
	cmp	edi, 8
	jle	.LBB2_93
# %bb.44:
	cmp	edi, 9
	je	.LBB2_122
# %bb.45:
	cmp	edi, 11
	je	.LBB2_125
# %bb.46:
	cmp	edi, 12
	jne	.LBB2_3
# %bb.47:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.48:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB2_49
# %bb.227:
	lea	rax, [rcx + 8*r11]
	cmp	rax, r8
	jbe	.LBB2_287
# %bb.228:
	lea	rax, [r8 + 8*r11]
	cmp	rax, rcx
	jbe	.LBB2_287
.LBB2_49:
	xor	esi, esi
.LBB2_670:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB2_672
.LBB2_671:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_671
.LBB2_672:
	cmp	rax, 3
	jb	.LBB2_3
.LBB2_673:                              # =>This Inner Loop Header: Depth=1
	movsd	xmm1, qword ptr [rcx + 8*rsi]   # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 8] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 16] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	movsd	xmm1, qword ptr [rcx + 8*rsi + 24] # xmm1 = mem[0],zero
	addsd	xmm1, xmm0
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB2_673
	jmp	.LBB2_3
.LBB2_50:
	cmp	edi, 2
	je	.LBB2_82
# %bb.51:
	cmp	edi, 3
	jne	.LBB2_517
# %bb.52:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.53:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_54
# %bb.140:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_200
# %bb.141:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_200
.LBB2_54:
	xor	esi, esi
.LBB2_334:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_336
	.p2align	4, 0x90
.LBB2_335:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_335
.LBB2_336:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_337:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_337
	jmp	.LBB2_517
.LBB2_55:
	cmp	edi, 7
	je	.LBB2_85
# %bb.56:
	cmp	edi, 8
	jne	.LBB2_517
# %bb.57:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.58:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_59
# %bb.143:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_203
# %bb.144:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_203
.LBB2_59:
	xor	esi, esi
.LBB2_342:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_344
	.p2align	4, 0x90
.LBB2_343:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_343
.LBB2_344:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_345:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_345
	jmp	.LBB2_517
.LBB2_60:
	cmp	edi, 2
	je	.LBB2_110
# %bb.61:
	cmp	edi, 3
	jne	.LBB2_474
# %bb.62:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.63:
	mov	r14b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_64
# %bb.170:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_260
# %bb.171:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_260
.LBB2_64:
	xor	esi, esi
.LBB2_414:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_416
	.p2align	4, 0x90
.LBB2_415:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_415
.LBB2_416:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_417:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_417
	jmp	.LBB2_474
.LBB2_65:
	cmp	edi, 7
	je	.LBB2_113
# %bb.66:
	cmp	edi, 8
	jne	.LBB2_474
# %bb.67:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.68:
	mov	r14, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_69
# %bb.173:
	lea	rax, [rcx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_263
# %bb.174:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rcx
	jbe	.LBB2_263
.LBB2_69:
	xor	esi, esi
.LBB2_422:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_424
	.p2align	4, 0x90
.LBB2_423:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_423
.LBB2_424:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_425:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_425
	jmp	.LBB2_474
.LBB2_70:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.71:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_72
# %bb.146:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_206
# %bb.147:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_206
.LBB2_72:
	xor	esi, esi
.LBB2_350:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_352
	.p2align	4, 0x90
.LBB2_351:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, eax
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_351
.LBB2_352:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_353:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_353
	jmp	.LBB2_517
.LBB2_73:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.74:
	movzx	eax, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_75
# %bb.149:
	lea	rdx, [rcx + 2*r10]
	cmp	rdx, r8
	jbe	.LBB2_209
# %bb.150:
	lea	rdx, [r8 + 2*r10]
	cmp	rdx, rcx
	jbe	.LBB2_209
.LBB2_75:
	xor	esi, esi
.LBB2_358:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_360
	.p2align	4, 0x90
.LBB2_359:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, eax
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_359
.LBB2_360:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_361:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_361
	jmp	.LBB2_517
.LBB2_76:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.77:
	mov	r11, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_78
# %bb.152:
	lea	rdx, [rcx + 8*r10]
	cmp	rdx, r8
	jbe	.LBB2_212
# %bb.153:
	lea	rdx, [r8 + 8*r10]
	cmp	rdx, rcx
	jbe	.LBB2_212
.LBB2_78:
	xor	esi, esi
.LBB2_366:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_368
	.p2align	4, 0x90
.LBB2_367:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r11
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_367
.LBB2_368:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_369:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_369
	jmp	.LBB2_517
.LBB2_79:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.80:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 8
	jb	.LBB2_81
# %bb.155:
	lea	rdx, [rcx + 4*rax]
	cmp	rdx, r8
	jbe	.LBB2_215
# %bb.156:
	lea	rdx, [r8 + 4*rax]
	cmp	rdx, rcx
	jbe	.LBB2_215
.LBB2_81:
	xor	edx, edx
.LBB2_374:
	mov	rsi, rdx
	not	rsi
	add	rsi, rax
	mov	rdi, rax
	and	rdi, 3
	je	.LBB2_376
	.p2align	4, 0x90
.LBB2_375:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rdx]
	movss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_375
.LBB2_376:
	cmp	rsi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_377
	jmp	.LBB2_517
.LBB2_82:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.83:
	mov	r11b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_84
# %bb.158:
	lea	rdx, [rcx + r10]
	cmp	rdx, r8
	jbe	.LBB2_218
# %bb.159:
	lea	rdx, [r8 + r10]
	cmp	rdx, rcx
	jbe	.LBB2_218
.LBB2_84:
	xor	esi, esi
.LBB2_382:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_384
	.p2align	4, 0x90
.LBB2_383:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_383
.LBB2_384:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_385:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_385
	jmp	.LBB2_517
.LBB2_85:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.86:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_87
# %bb.161:
	lea	rdx, [rcx + 4*r10]
	cmp	rdx, r8
	jbe	.LBB2_221
# %bb.162:
	lea	rdx, [r8 + 4*r10]
	cmp	rdx, rcx
	jbe	.LBB2_221
.LBB2_87:
	xor	esi, esi
.LBB2_390:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rdx, r10
	and	rdx, 3
	je	.LBB2_392
	.p2align	4, 0x90
.LBB2_391:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r11d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_391
.LBB2_392:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_393:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_393
	jmp	.LBB2_517
.LBB2_88:
	cmp	edi, 2
	je	.LBB2_128
# %bb.89:
	cmp	edi, 3
	jne	.LBB2_3
# %bb.90:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.91:
	mov	r14b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_92
# %bb.230:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_290
# %bb.231:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_290
.LBB2_92:
	xor	esi, esi
.LBB2_678:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_680
.LBB2_679:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_679
.LBB2_680:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_681:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_681
	jmp	.LBB2_3
.LBB2_93:
	cmp	edi, 7
	je	.LBB2_131
# %bb.94:
	cmp	edi, 8
	jne	.LBB2_3
# %bb.95:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.96:
	mov	r14, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_97
# %bb.233:
	lea	rax, [rcx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_293
# %bb.234:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rcx
	jbe	.LBB2_293
.LBB2_97:
	xor	esi, esi
.LBB2_686:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_688
.LBB2_687:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_687
.LBB2_688:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_689:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_689
	jmp	.LBB2_3
.LBB2_98:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.99:
	movzx	r14d, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_100
# %bb.176:
	lea	rax, [rcx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_266
# %bb.177:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rcx
	jbe	.LBB2_266
.LBB2_100:
	xor	esi, esi
.LBB2_430:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_432
	.p2align	4, 0x90
.LBB2_431:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_431
.LBB2_432:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_433:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_433
	jmp	.LBB2_474
.LBB2_101:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.102:
	movzx	r14d, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_103
# %bb.179:
	lea	rax, [rcx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_269
# %bb.180:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rcx
	jbe	.LBB2_269
.LBB2_103:
	xor	esi, esi
.LBB2_438:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_440
	.p2align	4, 0x90
.LBB2_439:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_439
.LBB2_440:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_441:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_441
	jmp	.LBB2_474
.LBB2_104:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.105:
	mov	r14, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_106
# %bb.182:
	lea	rax, [rcx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_272
# %bb.183:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rcx
	jbe	.LBB2_272
.LBB2_106:
	xor	esi, esi
.LBB2_446:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_448
	.p2align	4, 0x90
.LBB2_447:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_447
.LBB2_448:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_449:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_449
	jmp	.LBB2_474
.LBB2_107:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.108:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB2_109
# %bb.185:
	lea	rax, [rcx + 4*r11]
	cmp	rax, r8
	jbe	.LBB2_275
# %bb.186:
	lea	rax, [r8 + 4*r11]
	cmp	rax, rcx
	jbe	.LBB2_275
.LBB2_109:
	xor	esi, esi
.LBB2_454:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB2_456
	.p2align	4, 0x90
.LBB2_455:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_455
.LBB2_456:
	cmp	rax, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_457:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	movss	xmm1, dword ptr [rcx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rcx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rcx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB2_457
	jmp	.LBB2_474
.LBB2_110:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.111:
	mov	r14b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_112
# %bb.188:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_278
# %bb.189:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_278
.LBB2_112:
	xor	esi, esi
.LBB2_462:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_464
	.p2align	4, 0x90
.LBB2_463:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_463
.LBB2_464:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_465:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_465
	jmp	.LBB2_474
.LBB2_113:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.114:
	mov	r14d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_115
# %bb.191:
	lea	rax, [rcx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_281
# %bb.192:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rcx
	jbe	.LBB2_281
.LBB2_115:
	xor	esi, esi
.LBB2_470:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_472
	.p2align	4, 0x90
.LBB2_471:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_471
.LBB2_472:
	cmp	r11, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_473:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_473
	jmp	.LBB2_474
.LBB2_116:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.117:
	movzx	r14d, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_118
# %bb.236:
	lea	rax, [rcx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_296
# %bb.237:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rcx
	jbe	.LBB2_296
.LBB2_118:
	xor	esi, esi
.LBB2_694:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_696
.LBB2_695:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_695
.LBB2_696:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_697:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_697
	jmp	.LBB2_3
.LBB2_119:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.120:
	movzx	r14d, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_121
# %bb.239:
	lea	rax, [rcx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_299
# %bb.240:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rcx
	jbe	.LBB2_299
.LBB2_121:
	xor	esi, esi
.LBB2_702:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_704
.LBB2_703:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_703
.LBB2_704:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_705:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rcx + 2*rsi]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi], ax
	movzx	eax, word ptr [rcx + 2*rsi + 2]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 2], ax
	movzx	eax, word ptr [rcx + 2*rsi + 4]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 4], ax
	movzx	eax, word ptr [rcx + 2*rsi + 6]
	add	ax, r14w
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_705
	jmp	.LBB2_3
.LBB2_122:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.123:
	mov	r14, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_124
# %bb.242:
	lea	rax, [rcx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_302
# %bb.243:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rcx
	jbe	.LBB2_302
.LBB2_124:
	xor	esi, esi
.LBB2_710:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_712
.LBB2_711:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_711
.LBB2_712:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_713:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rcx + 8*rsi]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rcx + 8*rsi + 8]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rcx + 8*rsi + 16]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rcx + 8*rsi + 24]
	add	rax, r14
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_713
	jmp	.LBB2_3
.LBB2_125:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.126:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB2_127
# %bb.245:
	lea	rax, [rcx + 4*r11]
	cmp	rax, r8
	jbe	.LBB2_305
# %bb.246:
	lea	rax, [r8 + 4*r11]
	cmp	rax, rcx
	jbe	.LBB2_305
.LBB2_127:
	xor	esi, esi
.LBB2_718:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB2_720
.LBB2_719:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_719
.LBB2_720:
	cmp	rax, 3
	jb	.LBB2_3
.LBB2_721:                              # =>This Inner Loop Header: Depth=1
	movss	xmm1, dword ptr [rcx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi], xmm1
	movss	xmm1, dword ptr [rcx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	movss	xmm1, dword ptr [rcx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	movss	xmm1, dword ptr [rcx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	addss	xmm1, xmm0
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB2_721
	jmp	.LBB2_3
.LBB2_128:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.129:
	mov	r14b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_130
# %bb.248:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_308
# %bb.249:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_308
.LBB2_130:
	xor	esi, esi
.LBB2_726:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_728
.LBB2_727:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_727
.LBB2_728:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_729:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rcx + rsi]
	add	al, r14b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rcx + rsi + 1]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rcx + rsi + 2]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rcx + rsi + 3]
	add	al, r14b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_729
	jmp	.LBB2_3
.LBB2_131:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.132:
	mov	r14d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_133
# %bb.251:
	lea	rax, [rcx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_311
# %bb.252:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rcx
	jbe	.LBB2_311
.LBB2_133:
	xor	esi, esi
.LBB2_734:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_736
.LBB2_735:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_735
.LBB2_736:
	cmp	r11, 3
	jb	.LBB2_3
.LBB2_737:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rcx + 4*rsi]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rcx + 4*rsi + 4]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rcx + 4*rsi + 8]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rcx + 4*rsi + 12]
	add	eax, r14d
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_737
	jmp	.LBB2_3
.LBB2_194:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_314
# %bb.195:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_196:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm1
	add	rbx, 16
	add	rdi, 2
	jne	.LBB2_196
	jmp	.LBB2_315
.LBB2_197:
	mov	edx, eax
	and	edx, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rsi, [rdx - 4]
	mov	rbx, rsi
	shr	rbx, 2
	add	rbx, 1
	test	rsi, rsi
	je	.LBB2_322
# %bb.198:
	mov	rsi, rbx
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_199:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_199
	jmp	.LBB2_323
.LBB2_200:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, r11b
	movd	xmm0, edx
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_330
# %bb.201:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_202:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + rbx + 48], xmm1
	add	rbx, 64
	add	rdi, 2
	jne	.LBB2_202
	jmp	.LBB2_331
.LBB2_203:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_338
# %bb.204:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_205:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm1
	add	rbx, 8
	add	rdi, 2
	jne	.LBB2_205
	jmp	.LBB2_339
.LBB2_206:
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
	je	.LBB2_346
# %bb.207:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_208:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm1
	add	rbx, 32
	add	rdi, 2
	jne	.LBB2_208
	jmp	.LBB2_347
.LBB2_209:
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
	je	.LBB2_354
# %bb.210:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_211:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm1
	add	rbx, 32
	add	rdi, 2
	jne	.LBB2_211
	jmp	.LBB2_355
.LBB2_212:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r11
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rdx, [rsi - 4]
	mov	r9, rdx
	shr	r9, 2
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_362
# %bb.213:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_214:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm1
	add	rbx, 8
	add	rdi, 2
	jne	.LBB2_214
	jmp	.LBB2_363
.LBB2_215:
	mov	edx, eax
	and	edx, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rsi, [rdx - 8]
	mov	rbx, rsi
	shr	rbx, 3
	add	rbx, 1
	test	rsi, rsi
	je	.LBB2_370
# %bb.216:
	mov	rsi, rbx
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_217:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_217
	jmp	.LBB2_371
.LBB2_218:
	mov	esi, r10d
	and	esi, -32
	movzx	edx, r11b
	movd	xmm0, edx
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_378
# %bb.219:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_220:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + rbx + 48], xmm1
	add	rbx, 64
	add	rdi, 2
	jne	.LBB2_220
	jmp	.LBB2_379
.LBB2_221:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r11d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rdx, [rsi - 8]
	mov	r9, rdx
	shr	r9, 3
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_386
# %bb.222:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_223:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm1
	add	rbx, 16
	add	rdi, 2
	jne	.LBB2_223
	jmp	.LBB2_387
.LBB2_254:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB2_394
# %bb.255:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_256:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_256
	jmp	.LBB2_395
.LBB2_257:
	mov	esi, r11d
	and	esi, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	test	rax, rax
	je	.LBB2_402
# %bb.258:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_259:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rbx]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
	movupd	xmm2, xmmword ptr [rcx + 8*rbx + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 48], xmm3
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_259
	jmp	.LBB2_403
.LBB2_260:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_410
# %bb.261:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_262:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_262
	jmp	.LBB2_411
.LBB2_263:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB2_418
# %bb.264:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_265:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_265
	jmp	.LBB2_419
.LBB2_266:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_426
# %bb.267:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_268:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_268
	jmp	.LBB2_427
.LBB2_269:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_434
# %bb.270:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_271:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_271
	jmp	.LBB2_435
.LBB2_272:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB2_442
# %bb.273:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_274:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_274
	jmp	.LBB2_443
.LBB2_275:
	mov	esi, r11d
	and	esi, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rax, [rsi - 8]
	mov	r10, rax
	shr	r10, 3
	add	r10, 1
	test	rax, rax
	je	.LBB2_450
# %bb.276:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_277:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rbx]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
	movups	xmm2, xmmword ptr [rcx + 4*rbx + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx + 32], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 48], xmm3
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_277
	jmp	.LBB2_451
.LBB2_278:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_458
# %bb.279:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_280:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_280
	jmp	.LBB2_459
.LBB2_281:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB2_466
# %bb.282:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_283:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_283
	jmp	.LBB2_467
.LBB2_284:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB2_658
# %bb.285:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_286:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_286
	jmp	.LBB2_659
.LBB2_287:
	mov	esi, r11d
	and	esi, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	test	rax, rax
	je	.LBB2_666
# %bb.288:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_289:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rbx]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
	movupd	xmm2, xmmword ptr [rcx + 8*rbx + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 48]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx + 32], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 48], xmm3
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_289
	jmp	.LBB2_667
.LBB2_290:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_674
# %bb.291:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_292:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_292
	jmp	.LBB2_675
.LBB2_293:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB2_682
# %bb.294:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_295:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_295
	jmp	.LBB2_683
.LBB2_296:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_690
# %bb.297:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_298:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_298
	jmp	.LBB2_691
.LBB2_299:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_698
# %bb.300:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_301:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm2
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_301
	jmp	.LBB2_699
.LBB2_302:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB2_706
# %bb.303:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_304:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_304
	jmp	.LBB2_707
.LBB2_305:
	mov	esi, r11d
	and	esi, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rax, [rsi - 8]
	mov	r10, rax
	shr	r10, 3
	add	r10, 1
	test	rax, rax
	je	.LBB2_714
# %bb.306:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_307:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rbx]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
	movups	xmm2, xmmword ptr [rcx + 4*rbx + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 48]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx + 32], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 48], xmm3
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_307
	jmp	.LBB2_715
.LBB2_308:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	pxor	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_722
# %bb.309:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_310:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + rbx + 48], xmm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_310
	jmp	.LBB2_723
.LBB2_311:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB2_730
# %bb.312:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_313:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_313
	jmp	.LBB2_731
.LBB2_314:
	xor	ebx, ebx
.LBB2_315:
	test	r9b, 1
	je	.LBB2_317
# %bb.316:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm0
.LBB2_317:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_318
.LBB2_322:
	xor	edi, edi
.LBB2_323:
	test	bl, 1
	je	.LBB2_325
# %bb.324:
	movupd	xmm2, xmmword ptr [rcx + 8*rdi]
	movupd	xmm3, xmmword ptr [rcx + 8*rdi + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rdi], xmm4
	movupd	xmmword ptr [r8 + 8*rdi + 16], xmm1
.LBB2_325:
	cmp	rdx, rax
	je	.LBB2_517
	jmp	.LBB2_326
.LBB2_330:
	xor	ebx, ebx
.LBB2_331:
	test	r9b, 1
	je	.LBB2_333
# %bb.332:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm0
.LBB2_333:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_334
.LBB2_338:
	xor	ebx, ebx
.LBB2_339:
	test	r9b, 1
	je	.LBB2_341
# %bb.340:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm0
.LBB2_341:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_342
.LBB2_346:
	xor	ebx, ebx
.LBB2_347:
	test	r9b, 1
	je	.LBB2_349
# %bb.348:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm0
.LBB2_349:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_350
.LBB2_354:
	xor	ebx, ebx
.LBB2_355:
	test	r9b, 1
	je	.LBB2_357
# %bb.356:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm0
.LBB2_357:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_358
.LBB2_362:
	xor	ebx, ebx
.LBB2_363:
	test	r9b, 1
	je	.LBB2_365
# %bb.364:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm0
.LBB2_365:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_366
.LBB2_370:
	xor	edi, edi
.LBB2_371:
	test	bl, 1
	je	.LBB2_373
# %bb.372:
	movups	xmm2, xmmword ptr [rcx + 4*rdi]
	movups	xmm3, xmmword ptr [rcx + 4*rdi + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rdi], xmm4
	movups	xmmword ptr [r8 + 4*rdi + 16], xmm1
.LBB2_373:
	cmp	rdx, rax
	je	.LBB2_517
	jmp	.LBB2_374
.LBB2_378:
	xor	ebx, ebx
.LBB2_379:
	test	r9b, 1
	je	.LBB2_381
# %bb.380:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm0
.LBB2_381:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_382
.LBB2_386:
	xor	ebx, ebx
.LBB2_387:
	test	r9b, 1
	je	.LBB2_389
# %bb.388:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm0
.LBB2_389:
	cmp	rsi, r10
	je	.LBB2_517
	jmp	.LBB2_390
.LBB2_394:
	xor	ebx, ebx
.LBB2_395:
	test	r11b, 1
	je	.LBB2_397
# %bb.396:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB2_397:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_398
.LBB2_402:
	xor	ebx, ebx
.LBB2_403:
	test	r10b, 1
	je	.LBB2_405
# %bb.404:
	movupd	xmm2, xmmword ptr [rcx + 8*rbx]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
.LBB2_405:
	cmp	rsi, r11
	je	.LBB2_474
	jmp	.LBB2_406
.LBB2_410:
	xor	ebx, ebx
.LBB2_411:
	test	r11b, 1
	je	.LBB2_413
# %bb.412:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB2_413:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_414
.LBB2_418:
	xor	ebx, ebx
.LBB2_419:
	test	r11b, 1
	je	.LBB2_421
# %bb.420:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB2_421:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_422
.LBB2_426:
	xor	ebx, ebx
.LBB2_427:
	test	r11b, 1
	je	.LBB2_429
# %bb.428:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB2_429:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_430
.LBB2_434:
	xor	ebx, ebx
.LBB2_435:
	test	r11b, 1
	je	.LBB2_437
# %bb.436:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB2_437:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_438
.LBB2_442:
	xor	ebx, ebx
.LBB2_443:
	test	r11b, 1
	je	.LBB2_445
# %bb.444:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB2_445:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_446
.LBB2_450:
	xor	ebx, ebx
.LBB2_451:
	test	r10b, 1
	je	.LBB2_453
# %bb.452:
	movups	xmm2, xmmword ptr [rcx + 4*rbx]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
.LBB2_453:
	cmp	rsi, r11
	je	.LBB2_474
	jmp	.LBB2_454
.LBB2_458:
	xor	ebx, ebx
.LBB2_459:
	test	r11b, 1
	je	.LBB2_461
# %bb.460:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB2_461:
	cmp	rsi, r10
	je	.LBB2_474
	jmp	.LBB2_462
.LBB2_466:
	xor	ebx, ebx
.LBB2_467:
	test	r11b, 1
	je	.LBB2_469
# %bb.468:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB2_469:
	cmp	rsi, r10
	jne	.LBB2_470
.LBB2_474:
	cmp	edi, 6
	jg	.LBB2_482
# %bb.475:
	cmp	edi, 3
	jle	.LBB2_489
# %bb.476:
	cmp	edi, 4
	je	.LBB2_499
# %bb.477:
	cmp	edi, 5
	je	.LBB2_502
# %bb.478:
	cmp	edi, 6
	jne	.LBB2_13
# %bb.479:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.480:
	mov	r14d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_481
# %bb.518:
	lea	rax, [rcx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_548
# %bb.519:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rcx
	jbe	.LBB2_548
.LBB2_481:
	xor	esi, esi
.LBB2_582:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_584
	.p2align	4, 0x90
.LBB2_583:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_583
.LBB2_584:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_585:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_585
	jmp	.LBB2_13
.LBB2_482:
	cmp	edi, 8
	jle	.LBB2_494
# %bb.483:
	cmp	edi, 9
	je	.LBB2_505
# %bb.484:
	cmp	edi, 11
	je	.LBB2_508
# %bb.485:
	cmp	edi, 12
	jne	.LBB2_13
# %bb.486:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.487:
	movsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 4
	jb	.LBB2_488
# %bb.521:
	lea	rax, [rcx + 8*r11]
	cmp	rax, r8
	jbe	.LBB2_551
# %bb.522:
	lea	rax, [r8 + 8*r11]
	cmp	rax, rcx
	jbe	.LBB2_551
.LBB2_488:
	xor	esi, esi
.LBB2_590:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB2_592
	.p2align	4, 0x90
.LBB2_591:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_591
.LBB2_592:
	cmp	rax, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_593:                              # =>This Inner Loop Header: Depth=1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rsi]
	movsd	qword ptr [r8 + 8*rsi], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rsi + 8]
	movsd	qword ptr [r8 + 8*rsi + 8], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rsi + 16]
	movsd	qword ptr [r8 + 8*rsi + 16], xmm1
	movapd	xmm1, xmm0
	subsd	xmm1, qword ptr [rcx + 8*rsi + 24]
	movsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB2_593
	jmp	.LBB2_13
.LBB2_489:
	cmp	edi, 2
	je	.LBB2_511
# %bb.490:
	cmp	edi, 3
	jne	.LBB2_13
# %bb.491:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.492:
	mov	r14b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_493
# %bb.524:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_554
# %bb.525:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_554
.LBB2_493:
	xor	esi, esi
.LBB2_598:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_600
	.p2align	4, 0x90
.LBB2_599:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_599
.LBB2_600:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_601:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_601
	jmp	.LBB2_13
.LBB2_494:
	cmp	edi, 7
	je	.LBB2_514
# %bb.495:
	cmp	edi, 8
	jne	.LBB2_13
# %bb.496:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.497:
	mov	r14, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_498
# %bb.527:
	lea	rax, [rcx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_557
# %bb.528:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rcx
	jbe	.LBB2_557
.LBB2_498:
	xor	esi, esi
.LBB2_606:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_608
	.p2align	4, 0x90
.LBB2_607:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_607
.LBB2_608:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_609:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_609
	jmp	.LBB2_13
.LBB2_499:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.500:
	movzx	r14d, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_501
# %bb.530:
	lea	rax, [rcx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_560
# %bb.531:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rcx
	jbe	.LBB2_560
.LBB2_501:
	xor	esi, esi
.LBB2_614:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_616
	.p2align	4, 0x90
.LBB2_615:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_615
.LBB2_616:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_617:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_617
	jmp	.LBB2_13
.LBB2_502:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.503:
	movzx	r14d, word ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB2_504
# %bb.533:
	lea	rax, [rcx + 2*r10]
	cmp	rax, r8
	jbe	.LBB2_563
# %bb.534:
	lea	rax, [r8 + 2*r10]
	cmp	rax, rcx
	jbe	.LBB2_563
.LBB2_504:
	xor	esi, esi
.LBB2_622:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_624
	.p2align	4, 0x90
.LBB2_623:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_623
.LBB2_624:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_625:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], ax
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi + 2]
	mov	word ptr [r8 + 2*rsi + 2], ax
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi + 4]
	mov	word ptr [r8 + 2*rsi + 4], ax
	mov	eax, r14d
	sub	ax, word ptr [rcx + 2*rsi + 6]
	mov	word ptr [r8 + 2*rsi + 6], ax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_625
	jmp	.LBB2_13
.LBB2_505:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.506:
	mov	r14, qword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 4
	jb	.LBB2_507
# %bb.536:
	lea	rax, [rcx + 8*r10]
	cmp	rax, r8
	jbe	.LBB2_566
# %bb.537:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rcx
	jbe	.LBB2_566
.LBB2_507:
	xor	esi, esi
.LBB2_630:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_632
	.p2align	4, 0x90
.LBB2_631:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_631
.LBB2_632:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_633:                              # =>This Inner Loop Header: Depth=1
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi + 8]
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi + 16]
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, r14
	sub	rax, qword ptr [rcx + 8*rsi + 24]
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_633
	jmp	.LBB2_13
.LBB2_508:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.509:
	movss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 8
	jb	.LBB2_510
# %bb.539:
	lea	rax, [rcx + 4*r11]
	cmp	rax, r8
	jbe	.LBB2_569
# %bb.540:
	lea	rax, [r8 + 4*r11]
	cmp	rax, rcx
	jbe	.LBB2_569
.LBB2_510:
	xor	esi, esi
.LBB2_638:
	mov	rax, rsi
	not	rax
	add	rax, r11
	mov	rbx, r11
	and	rbx, 3
	je	.LBB2_640
	.p2align	4, 0x90
.LBB2_639:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_639
.LBB2_640:
	cmp	rax, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_641:                              # =>This Inner Loop Header: Depth=1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rsi]
	movss	dword ptr [r8 + 4*rsi], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rsi + 4]
	movss	dword ptr [r8 + 4*rsi + 4], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rsi + 8]
	movss	dword ptr [r8 + 4*rsi + 8], xmm1
	movaps	xmm1, xmm0
	subss	xmm1, dword ptr [rcx + 4*rsi + 12]
	movss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r11, rsi
	jne	.LBB2_641
	jmp	.LBB2_13
.LBB2_511:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.512:
	mov	r14b, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB2_513
# %bb.542:
	lea	rax, [rcx + r10]
	cmp	rax, r8
	jbe	.LBB2_572
# %bb.543:
	lea	rax, [r8 + r10]
	cmp	rax, rcx
	jbe	.LBB2_572
.LBB2_513:
	xor	esi, esi
.LBB2_646:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_648
	.p2align	4, 0x90
.LBB2_647:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_647
.LBB2_648:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_649:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], al
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi + 1]
	mov	byte ptr [r8 + rsi + 1], al
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi + 2]
	mov	byte ptr [r8 + rsi + 2], al
	mov	eax, r14d
	sub	al, byte ptr [rcx + rsi + 3]
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_649
	jmp	.LBB2_13
.LBB2_514:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.515:
	mov	r14d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 8
	jb	.LBB2_516
# %bb.545:
	lea	rax, [rcx + 4*r10]
	cmp	rax, r8
	jbe	.LBB2_575
# %bb.546:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rcx
	jbe	.LBB2_575
.LBB2_516:
	xor	esi, esi
.LBB2_654:
	mov	r11, rsi
	not	r11
	add	r11, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB2_656
	.p2align	4, 0x90
.LBB2_655:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_655
.LBB2_656:
	cmp	r11, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_657:                              # =>This Inner Loop Header: Depth=1
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi + 4]
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi + 8]
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, r14d
	sub	eax, dword ptr [rcx + 4*rsi + 12]
	mov	dword ptr [r8 + 4*rsi + 12], eax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB2_657
	jmp	.LBB2_13
.LBB2_517:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	ret
.LBB2_548:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB2_578
# %bb.549:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_550:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm1
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_550
	jmp	.LBB2_579
.LBB2_551:
	mov	esi, r11d
	and	esi, -4
	movddup	xmm1, xmm0                      # xmm1 = xmm0[0,0]
	lea	rax, [rsi - 4]
	mov	r10, rax
	shr	r10, 2
	add	r10, 1
	test	rax, rax
	je	.LBB2_586
# %bb.552:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_553:                              # =>This Inner Loop Header: Depth=1
	movupd	xmm2, xmmword ptr [rcx + 8*rbx]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	movapd	xmm2, xmm1
	subpd	xmm2, xmm3
	movupd	xmmword ptr [r8 + 8*rbx], xmm4
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm2
	movupd	xmm2, xmmword ptr [rcx + 8*rbx + 32]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 48]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	movapd	xmm2, xmm1
	subpd	xmm2, xmm3
	movupd	xmmword ptr [r8 + 8*rbx + 32], xmm4
	movupd	xmmword ptr [r8 + 8*rbx + 48], xmm2
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_553
	jmp	.LBB2_587
.LBB2_554:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_594
# %bb.555:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_556:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + rbx + 48], xmm1
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_556
	jmp	.LBB2_595
.LBB2_557:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB2_602
# %bb.558:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_559:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm1
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_559
	jmp	.LBB2_603
.LBB2_560:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_610
# %bb.561:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_562:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm1
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_562
	jmp	.LBB2_611
.LBB2_563:
	mov	esi, r10d
	and	esi, -16
	movd	xmm0, r14d
	pshuflw	xmm0, xmm0, 224                 # xmm0 = xmm0[0,0,2,3,4,5,6,7]
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_618
# %bb.564:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_565:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 48]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubw	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 48], xmm1
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_565
	jmp	.LBB2_619
.LBB2_566:
	mov	esi, r10d
	and	esi, -4
	movq	xmm0, r14
	pshufd	xmm0, xmm0, 68                  # xmm0 = xmm0[0,1,0,1]
	lea	rax, [rsi - 4]
	mov	r11, rax
	shr	r11, 2
	add	r11, 1
	test	rax, rax
	je	.LBB2_626
# %bb.567:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_568:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 48]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubq	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 48], xmm1
	add	rbx, 8
	add	rax, 2
	jne	.LBB2_568
	jmp	.LBB2_627
.LBB2_569:
	mov	esi, r11d
	and	esi, -8
	movaps	xmm1, xmm0
	shufps	xmm1, xmm0, 0                   # xmm1 = xmm1[0,0],xmm0[0,0]
	lea	rax, [rsi - 8]
	mov	r10, rax
	shr	r10, 3
	add	r10, 1
	test	rax, rax
	je	.LBB2_634
# %bb.570:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_571:                              # =>This Inner Loop Header: Depth=1
	movups	xmm2, xmmword ptr [rcx + 4*rbx]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	movaps	xmm2, xmm1
	subps	xmm2, xmm3
	movups	xmmword ptr [r8 + 4*rbx], xmm4
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm2
	movups	xmm2, xmmword ptr [rcx + 4*rbx + 32]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 48]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	movaps	xmm2, xmm1
	subps	xmm2, xmm3
	movups	xmmword ptr [r8 + 4*rbx + 32], xmm4
	movups	xmmword ptr [r8 + 4*rbx + 48], xmm2
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_571
	jmp	.LBB2_635
.LBB2_572:
	mov	esi, r10d
	and	esi, -32
	movzx	eax, r14b
	movd	xmm0, eax
	xorpd	xmm1, xmm1
	pshufb	xmm0, xmm1
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_642
# %bb.573:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_574:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 48]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubb	xmm1, xmm2
	movdqu	xmmword ptr [r8 + rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + rbx + 48], xmm1
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_574
	jmp	.LBB2_643
.LBB2_575:
	mov	esi, r10d
	and	esi, -8
	movd	xmm0, r14d
	pshufd	xmm0, xmm0, 0                   # xmm0 = xmm0[0,0,0,0]
	lea	rax, [rsi - 8]
	mov	r11, rax
	shr	r11, 3
	add	r11, 1
	test	rax, rax
	je	.LBB2_650
# %bb.576:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_577:                              # =>This Inner Loop Header: Depth=1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm1
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx + 32]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 48]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	movdqa	xmm1, xmm0
	psubd	xmm1, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx + 32], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 48], xmm1
	add	rbx, 16
	add	rax, 2
	jne	.LBB2_577
	jmp	.LBB2_651
.LBB2_578:
	xor	ebx, ebx
.LBB2_579:
	test	r11b, 1
	je	.LBB2_581
# %bb.580:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm0
.LBB2_581:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_582
.LBB2_586:
	xor	ebx, ebx
.LBB2_587:
	test	r10b, 1
	je	.LBB2_589
# %bb.588:
	movupd	xmm2, xmmword ptr [rcx + 8*rbx]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 16]
	movapd	xmm4, xmm1
	subpd	xmm4, xmm2
	subpd	xmm1, xmm3
	movupd	xmmword ptr [r8 + 8*rbx], xmm4
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm1
.LBB2_589:
	cmp	rsi, r11
	je	.LBB2_13
	jmp	.LBB2_590
.LBB2_594:
	xor	ebx, ebx
.LBB2_595:
	test	r11b, 1
	je	.LBB2_597
# %bb.596:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm0
.LBB2_597:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_598
.LBB2_602:
	xor	ebx, ebx
.LBB2_603:
	test	r11b, 1
	je	.LBB2_605
# %bb.604:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm0
.LBB2_605:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_606
.LBB2_610:
	xor	ebx, ebx
.LBB2_611:
	test	r11b, 1
	je	.LBB2_613
# %bb.612:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm0
.LBB2_613:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_614
.LBB2_618:
	xor	ebx, ebx
.LBB2_619:
	test	r11b, 1
	je	.LBB2_621
# %bb.620:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	movdqa	xmm3, xmm0
	psubw	xmm3, xmm1
	psubw	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 2*rbx], xmm3
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm0
.LBB2_621:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_622
.LBB2_626:
	xor	ebx, ebx
.LBB2_627:
	test	r11b, 1
	je	.LBB2_629
# %bb.628:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	movdqa	xmm3, xmm0
	psubq	xmm3, xmm1
	psubq	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 8*rbx], xmm3
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm0
.LBB2_629:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_630
.LBB2_634:
	xor	ebx, ebx
.LBB2_635:
	test	r10b, 1
	je	.LBB2_637
# %bb.636:
	movups	xmm2, xmmword ptr [rcx + 4*rbx]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 16]
	movaps	xmm4, xmm1
	subps	xmm4, xmm2
	subps	xmm1, xmm3
	movups	xmmword ptr [r8 + 4*rbx], xmm4
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm1
.LBB2_637:
	cmp	rsi, r11
	je	.LBB2_13
	jmp	.LBB2_638
.LBB2_642:
	xor	ebx, ebx
.LBB2_643:
	test	r11b, 1
	je	.LBB2_645
# %bb.644:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	movdqa	xmm3, xmm0
	psubb	xmm3, xmm1
	psubb	xmm0, xmm2
	movdqu	xmmword ptr [r8 + rbx], xmm3
	movdqu	xmmword ptr [r8 + rbx + 16], xmm0
.LBB2_645:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_646
.LBB2_650:
	xor	ebx, ebx
.LBB2_651:
	test	r11b, 1
	je	.LBB2_653
# %bb.652:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	movdqa	xmm3, xmm0
	psubd	xmm3, xmm1
	psubd	xmm0, xmm2
	movdqu	xmmword ptr [r8 + 4*rbx], xmm3
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm0
.LBB2_653:
	cmp	rsi, r10
	je	.LBB2_13
	jmp	.LBB2_654
.LBB2_658:
	xor	ebx, ebx
.LBB2_659:
	test	r11b, 1
	je	.LBB2_661
# %bb.660:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB2_661:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_662
.LBB2_666:
	xor	ebx, ebx
.LBB2_667:
	test	r10b, 1
	je	.LBB2_669
# %bb.668:
	movupd	xmm2, xmmword ptr [rcx + 8*rbx]
	movupd	xmm3, xmmword ptr [rcx + 8*rbx + 16]
	addpd	xmm2, xmm1
	addpd	xmm3, xmm1
	movupd	xmmword ptr [r8 + 8*rbx], xmm2
	movupd	xmmword ptr [r8 + 8*rbx + 16], xmm3
.LBB2_669:
	cmp	rsi, r11
	je	.LBB2_3
	jmp	.LBB2_670
.LBB2_674:
	xor	ebx, ebx
.LBB2_675:
	test	r11b, 1
	je	.LBB2_677
# %bb.676:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB2_677:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_678
.LBB2_682:
	xor	ebx, ebx
.LBB2_683:
	test	r11b, 1
	je	.LBB2_685
# %bb.684:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB2_685:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_686
.LBB2_690:
	xor	ebx, ebx
.LBB2_691:
	test	r11b, 1
	je	.LBB2_693
# %bb.692:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB2_693:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_694
.LBB2_698:
	xor	ebx, ebx
.LBB2_699:
	test	r11b, 1
	je	.LBB2_701
# %bb.700:
	movdqu	xmm1, xmmword ptr [rcx + 2*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 2*rbx + 16]
	paddw	xmm1, xmm0
	paddw	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 2*rbx], xmm1
	movdqu	xmmword ptr [r8 + 2*rbx + 16], xmm2
.LBB2_701:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_702
.LBB2_706:
	xor	ebx, ebx
.LBB2_707:
	test	r11b, 1
	je	.LBB2_709
# %bb.708:
	movdqu	xmm1, xmmword ptr [rcx + 8*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 8*rbx + 16]
	paddq	xmm1, xmm0
	paddq	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 8*rbx], xmm1
	movdqu	xmmword ptr [r8 + 8*rbx + 16], xmm2
.LBB2_709:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_710
.LBB2_714:
	xor	ebx, ebx
.LBB2_715:
	test	r10b, 1
	je	.LBB2_717
# %bb.716:
	movups	xmm2, xmmword ptr [rcx + 4*rbx]
	movups	xmm3, xmmword ptr [rcx + 4*rbx + 16]
	addps	xmm2, xmm1
	addps	xmm3, xmm1
	movups	xmmword ptr [r8 + 4*rbx], xmm2
	movups	xmmword ptr [r8 + 4*rbx + 16], xmm3
.LBB2_717:
	cmp	rsi, r11
	je	.LBB2_3
	jmp	.LBB2_718
.LBB2_722:
	xor	ebx, ebx
.LBB2_723:
	test	r11b, 1
	je	.LBB2_725
# %bb.724:
	movdqu	xmm1, xmmword ptr [rcx + rbx]
	movdqu	xmm2, xmmword ptr [rcx + rbx + 16]
	paddb	xmm1, xmm0
	paddb	xmm2, xmm0
	movdqu	xmmword ptr [r8 + rbx], xmm1
	movdqu	xmmword ptr [r8 + rbx + 16], xmm2
.LBB2_725:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_726
.LBB2_730:
	xor	ebx, ebx
.LBB2_731:
	test	r11b, 1
	je	.LBB2_733
# %bb.732:
	movdqu	xmm1, xmmword ptr [rcx + 4*rbx]
	movdqu	xmm2, xmmword ptr [rcx + 4*rbx + 16]
	paddd	xmm1, xmm0
	paddd	xmm2, xmm0
	movdqu	xmmword ptr [r8 + 4*rbx], xmm1
	movdqu	xmmword ptr [r8 + 4*rbx + 16], xmm2
.LBB2_733:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_734
.Lfunc_end2:
	.size	arithmetic_scalar_arr_sse4, .Lfunc_end2-arithmetic_scalar_arr_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
