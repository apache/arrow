	.text
	.intel_syntax noprefix
	.file	"base_arithmetic.cc"
	.globl	arithmetic_avx2                 # -- Begin function arithmetic_avx2
	.p2align	4, 0x90
	.type	arithmetic_avx2,@function
arithmetic_avx2:                        # @arithmetic_avx2
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
	jne	.LBB0_537
.LBB0_138:
	cmp	edi, 6
	jg	.LBB0_151
# %bb.139:
	cmp	edi, 3
	jle	.LBB0_140
# %bb.145:
	cmp	edi, 4
	je	.LBB0_184
# %bb.146:
	cmp	edi, 5
	je	.LBB0_196
# %bb.147:
	cmp	edi, 6
	jne	.LBB0_271
# %bb.148:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.149:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_208
# %bb.150:
	xor	esi, esi
.LBB0_213:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_215
	.p2align	4, 0x90
.LBB0_214:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rcx + 4*rsi]
	add	ebx, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_214
.LBB0_215:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_216:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_216
	jmp	.LBB0_271
.LBB0_3:
	cmp	sil, 2
	je	.LBB0_271
# %bb.4:
	cmp	sil, 3
	jne	.LBB0_537
.LBB0_404:
	cmp	edi, 6
	jg	.LBB0_417
# %bb.405:
	cmp	edi, 3
	jle	.LBB0_406
# %bb.411:
	cmp	edi, 4
	je	.LBB0_450
# %bb.412:
	cmp	edi, 5
	je	.LBB0_462
# %bb.413:
	cmp	edi, 6
	jne	.LBB0_537
# %bb.414:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.415:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_474
# %bb.416:
	xor	esi, esi
	jmp	.LBB0_479
.LBB0_417:
	cmp	edi, 8
	jle	.LBB0_418
# %bb.423:
	cmp	edi, 9
	je	.LBB0_504
# %bb.424:
	cmp	edi, 11
	je	.LBB0_516
# %bb.425:
	cmp	edi, 12
	jne	.LBB0_537
# %bb.426:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.427:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_528
# %bb.428:
	xor	esi, esi
	jmp	.LBB0_533
.LBB0_5:
	cmp	edi, 6
	jg	.LBB0_18
# %bb.6:
	cmp	edi, 3
	jle	.LBB0_7
# %bb.12:
	cmp	edi, 4
	je	.LBB0_51
# %bb.13:
	cmp	edi, 5
	je	.LBB0_63
# %bb.14:
	cmp	edi, 6
	jne	.LBB0_138
# %bb.15:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.16:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_75
# %bb.17:
	xor	esi, esi
	jmp	.LBB0_80
.LBB0_151:
	cmp	edi, 8
	jle	.LBB0_152
# %bb.157:
	cmp	edi, 9
	je	.LBB0_238
# %bb.158:
	cmp	edi, 11
	je	.LBB0_250
# %bb.159:
	cmp	edi, 12
	jne	.LBB0_271
# %bb.160:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.161:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_262
# %bb.162:
	xor	esi, esi
.LBB0_267:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_269
	.p2align	4, 0x90
.LBB0_268:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_268
.LBB0_269:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_270:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_270
	jmp	.LBB0_271
.LBB0_18:
	cmp	edi, 8
	jle	.LBB0_19
# %bb.24:
	cmp	edi, 9
	je	.LBB0_105
# %bb.25:
	cmp	edi, 11
	je	.LBB0_117
# %bb.26:
	cmp	edi, 12
	jne	.LBB0_138
# %bb.27:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.28:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_129
# %bb.29:
	xor	esi, esi
	jmp	.LBB0_134
.LBB0_406:
	cmp	edi, 2
	je	.LBB0_429
# %bb.407:
	cmp	edi, 3
	jne	.LBB0_537
# %bb.408:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.409:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_441
# %bb.410:
	xor	esi, esi
	jmp	.LBB0_446
.LBB0_418:
	cmp	edi, 7
	je	.LBB0_483
# %bb.419:
	cmp	edi, 8
	jne	.LBB0_537
# %bb.420:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.421:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_495
# %bb.422:
	xor	esi, esi
	jmp	.LBB0_500
.LBB0_140:
	cmp	edi, 2
	je	.LBB0_163
# %bb.141:
	cmp	edi, 3
	jne	.LBB0_271
# %bb.142:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.143:
	mov	r11d, r9d
	cmp	r9d, 128
	jae	.LBB0_175
# %bb.144:
	xor	esi, esi
.LBB0_180:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_182
	.p2align	4, 0x90
.LBB0_181:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rcx + rsi]
	add	bl, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_181
.LBB0_182:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_183:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_183
	jmp	.LBB0_271
.LBB0_152:
	cmp	edi, 7
	je	.LBB0_217
# %bb.153:
	cmp	edi, 8
	jne	.LBB0_271
# %bb.154:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.155:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_229
# %bb.156:
	xor	esi, esi
.LBB0_234:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_236
	.p2align	4, 0x90
.LBB0_235:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rcx + 8*rsi]
	add	rbx, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_235
.LBB0_236:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_237:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_237
	jmp	.LBB0_271
.LBB0_450:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.451:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_453
# %bb.452:
	xor	esi, esi
	jmp	.LBB0_458
.LBB0_462:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.463:
	mov	r10d, r9d
	cmp	r9d, 64
	jae	.LBB0_465
# %bb.464:
	xor	esi, esi
	jmp	.LBB0_470
.LBB0_504:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.505:
	mov	r10d, r9d
	cmp	r9d, 16
	jae	.LBB0_507
# %bb.506:
	xor	esi, esi
	jmp	.LBB0_512
.LBB0_516:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.517:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_519
# %bb.518:
	xor	esi, esi
	jmp	.LBB0_524
.LBB0_429:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.430:
	mov	r10d, r9d
	cmp	r9d, 128
	jae	.LBB0_432
# %bb.431:
	xor	esi, esi
	jmp	.LBB0_437
.LBB0_483:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.484:
	mov	r10d, r9d
	cmp	r9d, 32
	jae	.LBB0_486
# %bb.485:
	xor	esi, esi
	jmp	.LBB0_491
.LBB0_7:
	cmp	edi, 2
	je	.LBB0_30
# %bb.8:
	cmp	edi, 3
	jne	.LBB0_138
# %bb.9:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.10:
	mov	r11d, r9d
	cmp	r9d, 128
	jae	.LBB0_42
# %bb.11:
	xor	esi, esi
	jmp	.LBB0_47
.LBB0_19:
	cmp	edi, 7
	je	.LBB0_84
# %bb.20:
	cmp	edi, 8
	jne	.LBB0_138
# %bb.21:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.22:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_96
# %bb.23:
	xor	esi, esi
	jmp	.LBB0_101
.LBB0_184:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.185:
	mov	r11d, r9d
	cmp	r9d, 64
	jae	.LBB0_187
# %bb.186:
	xor	esi, esi
.LBB0_192:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_194
	.p2align	4, 0x90
.LBB0_193:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_193
.LBB0_194:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_195:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_195
	jmp	.LBB0_271
.LBB0_196:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.197:
	mov	r11d, r9d
	cmp	r9d, 64
	jae	.LBB0_199
# %bb.198:
	xor	esi, esi
.LBB0_204:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_206
	.p2align	4, 0x90
.LBB0_205:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_205
.LBB0_206:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_207:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_207
	jmp	.LBB0_271
.LBB0_238:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.239:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_241
# %bb.240:
	xor	esi, esi
.LBB0_246:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_248
	.p2align	4, 0x90
.LBB0_247:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rcx + 8*rsi]
	add	rbx, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_247
.LBB0_248:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_249:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_249
	jmp	.LBB0_271
.LBB0_250:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.251:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_253
# %bb.252:
	xor	esi, esi
.LBB0_258:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_260
	.p2align	4, 0x90
.LBB0_259:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_259
.LBB0_260:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_261:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_261
	jmp	.LBB0_271
.LBB0_163:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.164:
	mov	r11d, r9d
	cmp	r9d, 128
	jae	.LBB0_166
# %bb.165:
	xor	esi, esi
.LBB0_171:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_173
	.p2align	4, 0x90
.LBB0_172:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rcx + rsi]
	add	bl, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_172
.LBB0_173:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_174:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_174
	jmp	.LBB0_271
.LBB0_217:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.218:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_220
# %bb.219:
	xor	esi, esi
.LBB0_225:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_227
	.p2align	4, 0x90
.LBB0_226:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rcx + 4*rsi]
	add	ebx, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_226
.LBB0_227:
	cmp	r10, 3
	jb	.LBB0_271
	.p2align	4, 0x90
.LBB0_228:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_228
	jmp	.LBB0_271
.LBB0_51:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.52:
	mov	r11d, r9d
	cmp	r9d, 64
	jae	.LBB0_54
# %bb.53:
	xor	esi, esi
	jmp	.LBB0_59
.LBB0_63:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.64:
	mov	r11d, r9d
	cmp	r9d, 64
	jae	.LBB0_66
# %bb.65:
	xor	esi, esi
	jmp	.LBB0_71
.LBB0_105:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.106:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_108
# %bb.107:
	xor	esi, esi
	jmp	.LBB0_113
.LBB0_117:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.118:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_120
# %bb.119:
	xor	esi, esi
	jmp	.LBB0_125
.LBB0_30:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.31:
	mov	r11d, r9d
	cmp	r9d, 128
	jae	.LBB0_33
# %bb.32:
	xor	esi, esi
	jmp	.LBB0_38
.LBB0_84:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.85:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_87
# %bb.86:
	xor	esi, esi
	jmp	.LBB0_92
.LBB0_474:
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
	jne	.LBB0_479
# %bb.475:
	and	al, dil
	jne	.LBB0_479
# %bb.476:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_477:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_477
# %bb.478:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_479:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_481
	.p2align	4, 0x90
.LBB0_480:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_480
.LBB0_481:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_482:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_482
	jmp	.LBB0_537
.LBB0_528:
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
	jne	.LBB0_533
# %bb.529:
	and	al, dil
	jne	.LBB0_533
# %bb.530:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_531:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_531
# %bb.532:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_533:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_535
	.p2align	4, 0x90
.LBB0_534:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_534
.LBB0_535:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_536:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_536
	jmp	.LBB0_537
.LBB0_441:
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
	jne	.LBB0_446
# %bb.442:
	and	al, dil
	jne	.LBB0_446
# %bb.443:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_444:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_444
# %bb.445:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_446:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_448
	.p2align	4, 0x90
.LBB0_447:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_447
.LBB0_448:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_449:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_449
	jmp	.LBB0_537
.LBB0_495:
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
	jne	.LBB0_500
# %bb.496:
	and	al, dil
	jne	.LBB0_500
# %bb.497:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_498:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_498
# %bb.499:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_500:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_502
	.p2align	4, 0x90
.LBB0_501:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_501
.LBB0_502:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_503:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_503
	jmp	.LBB0_537
.LBB0_453:
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
	jne	.LBB0_458
# %bb.454:
	and	al, dil
	jne	.LBB0_458
# %bb.455:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_456:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_456
# %bb.457:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_458:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_460
	.p2align	4, 0x90
.LBB0_459:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_459
.LBB0_460:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_461:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_461
	jmp	.LBB0_537
.LBB0_465:
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
	jne	.LBB0_470
# %bb.466:
	and	al, dil
	jne	.LBB0_470
# %bb.467:
	mov	esi, r10d
	and	esi, -64
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_468:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_468
# %bb.469:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_470:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_472
	.p2align	4, 0x90
.LBB0_471:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_471
.LBB0_472:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_473:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_473
	jmp	.LBB0_537
.LBB0_507:
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
	jne	.LBB0_512
# %bb.508:
	and	al, dil
	jne	.LBB0_512
# %bb.509:
	mov	esi, r10d
	and	esi, -16
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_510:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_510
# %bb.511:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_512:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_514
	.p2align	4, 0x90
.LBB0_513:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_513
.LBB0_514:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_515:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_515
	jmp	.LBB0_537
.LBB0_519:
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
	jne	.LBB0_524
# %bb.520:
	and	al, dil
	jne	.LBB0_524
# %bb.521:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_522:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_522
# %bb.523:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_524:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_526
	.p2align	4, 0x90
.LBB0_525:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_525
.LBB0_526:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_527:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_527
	jmp	.LBB0_537
.LBB0_432:
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
	jne	.LBB0_437
# %bb.433:
	and	al, dil
	jne	.LBB0_437
# %bb.434:
	mov	esi, r10d
	and	esi, -128
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_435:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_435
# %bb.436:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_437:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_439
	.p2align	4, 0x90
.LBB0_438:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_438
.LBB0_439:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_440:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_440
	jmp	.LBB0_537
.LBB0_486:
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
	jne	.LBB0_491
# %bb.487:
	and	al, dil
	jne	.LBB0_491
# %bb.488:
	mov	esi, r10d
	and	esi, -32
	xor	edi, edi
	.p2align	4, 0x90
.LBB0_489:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_489
# %bb.490:
	cmp	rsi, r10
	je	.LBB0_537
.LBB0_491:
	mov	rdi, rsi
	not	rdi
	add	rdi, r10
	mov	rax, r10
	and	rax, 3
	je	.LBB0_493
	.p2align	4, 0x90
.LBB0_492:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_492
.LBB0_493:
	cmp	rdi, 3
	jb	.LBB0_537
	.p2align	4, 0x90
.LBB0_494:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB0_494
	jmp	.LBB0_537
.LBB0_208:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_213
# %bb.209:
	and	al, r10b
	jne	.LBB0_213
# %bb.210:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_211:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rax + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rax]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rax + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rax + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rax + 96]
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_211
# %bb.212:
	cmp	rsi, r11
	jne	.LBB0_213
	jmp	.LBB0_271
.LBB0_262:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_267
# %bb.263:
	and	al, r10b
	jne	.LBB0_267
# %bb.264:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_265:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rcx + 8*rax]
	vmovupd	ymm1, ymmword ptr [rcx + 8*rax + 32]
	vmovupd	ymm2, ymmword ptr [rcx + 8*rax + 64]
	vmovupd	ymm3, ymmword ptr [rcx + 8*rax + 96]
	vaddpd	ymm0, ymm0, ymmword ptr [rdx + 8*rax]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rax + 32]
	vaddpd	ymm2, ymm2, ymmword ptr [rdx + 8*rax + 64]
	vaddpd	ymm3, ymm3, ymmword ptr [rdx + 8*rax + 96]
	vmovupd	ymmword ptr [r8 + 8*rax], ymm0
	vmovupd	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_265
# %bb.266:
	cmp	rsi, r11
	jne	.LBB0_267
	jmp	.LBB0_271
.LBB0_175:
	lea	rsi, [r8 + r11]
	lea	rax, [rdx + r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_180
# %bb.176:
	and	al, r10b
	jne	.LBB0_180
# %bb.177:
	mov	esi, r11d
	and	esi, -128
	xor	eax, eax
.LBB0_178:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rax]
	vmovdqu	ymm1, ymmword ptr [rcx + rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rax + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rax]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rax + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rax + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rax + 96]
	vmovdqu	ymmword ptr [r8 + rax], ymm0
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm3
	sub	rax, -128
	cmp	rsi, rax
	jne	.LBB0_178
# %bb.179:
	cmp	rsi, r11
	jne	.LBB0_180
	jmp	.LBB0_271
.LBB0_229:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_234
# %bb.230:
	and	al, r10b
	jne	.LBB0_234
# %bb.231:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_232:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rax + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rax]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rax + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rax + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rax + 96]
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_232
# %bb.233:
	cmp	rsi, r11
	jne	.LBB0_234
	jmp	.LBB0_271
.LBB0_187:
	lea	rsi, [r8 + 2*r11]
	lea	rax, [rdx + 2*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_192
# %bb.188:
	and	al, r10b
	jne	.LBB0_192
# %bb.189:
	mov	esi, r11d
	and	esi, -64
	xor	eax, eax
.LBB0_190:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rax + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rax]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rax + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rax + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rax + 96]
	vmovdqu	ymmword ptr [r8 + 2*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rax + 96], ymm3
	add	rax, 64
	cmp	rsi, rax
	jne	.LBB0_190
# %bb.191:
	cmp	rsi, r11
	jne	.LBB0_192
	jmp	.LBB0_271
.LBB0_199:
	lea	rsi, [r8 + 2*r11]
	lea	rax, [rdx + 2*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_204
# %bb.200:
	and	al, r10b
	jne	.LBB0_204
# %bb.201:
	mov	esi, r11d
	and	esi, -64
	xor	eax, eax
.LBB0_202:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rax + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rax]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rax + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rax + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rax + 96]
	vmovdqu	ymmword ptr [r8 + 2*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rax + 96], ymm3
	add	rax, 64
	cmp	rsi, rax
	jne	.LBB0_202
# %bb.203:
	cmp	rsi, r11
	jne	.LBB0_204
	jmp	.LBB0_271
.LBB0_241:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_246
# %bb.242:
	and	al, r10b
	jne	.LBB0_246
# %bb.243:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_244:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rax + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rax]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rax + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rax + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rax + 96]
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_244
# %bb.245:
	cmp	rsi, r11
	jne	.LBB0_246
	jmp	.LBB0_271
.LBB0_253:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_258
# %bb.254:
	and	al, r10b
	jne	.LBB0_258
# %bb.255:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_256:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rcx + 4*rax]
	vmovups	ymm1, ymmword ptr [rcx + 4*rax + 32]
	vmovups	ymm2, ymmword ptr [rcx + 4*rax + 64]
	vmovups	ymm3, ymmword ptr [rcx + 4*rax + 96]
	vaddps	ymm0, ymm0, ymmword ptr [rdx + 4*rax]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rax + 32]
	vaddps	ymm2, ymm2, ymmword ptr [rdx + 4*rax + 64]
	vaddps	ymm3, ymm3, ymmword ptr [rdx + 4*rax + 96]
	vmovups	ymmword ptr [r8 + 4*rax], ymm0
	vmovups	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_256
# %bb.257:
	cmp	rsi, r11
	jne	.LBB0_258
	jmp	.LBB0_271
.LBB0_166:
	lea	rsi, [r8 + r11]
	lea	rax, [rdx + r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_171
# %bb.167:
	and	al, r10b
	jne	.LBB0_171
# %bb.168:
	mov	esi, r11d
	and	esi, -128
	xor	eax, eax
.LBB0_169:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rax]
	vmovdqu	ymm1, ymmword ptr [rcx + rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rax + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rax]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rax + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rax + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rax + 96]
	vmovdqu	ymmword ptr [r8 + rax], ymm0
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm3
	sub	rax, -128
	cmp	rsi, rax
	jne	.LBB0_169
# %bb.170:
	cmp	rsi, r11
	jne	.LBB0_171
	jmp	.LBB0_271
.LBB0_220:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_225
# %bb.221:
	and	al, r10b
	jne	.LBB0_225
# %bb.222:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_223:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rax + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rax]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rax + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rax + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rax + 96]
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_223
# %bb.224:
	cmp	rsi, r11
	jne	.LBB0_225
.LBB0_271:
	cmp	edi, 6
	jg	.LBB0_284
# %bb.272:
	cmp	edi, 3
	jle	.LBB0_273
# %bb.278:
	cmp	edi, 4
	je	.LBB0_317
# %bb.279:
	cmp	edi, 5
	je	.LBB0_329
# %bb.280:
	cmp	edi, 6
	jne	.LBB0_404
# %bb.281:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.282:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_341
# %bb.283:
	xor	esi, esi
	jmp	.LBB0_346
.LBB0_284:
	cmp	edi, 8
	jle	.LBB0_285
# %bb.290:
	cmp	edi, 9
	je	.LBB0_371
# %bb.291:
	cmp	edi, 11
	je	.LBB0_383
# %bb.292:
	cmp	edi, 12
	jne	.LBB0_404
# %bb.293:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.294:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_395
# %bb.295:
	xor	esi, esi
	jmp	.LBB0_400
.LBB0_273:
	cmp	edi, 2
	je	.LBB0_296
# %bb.274:
	cmp	edi, 3
	jne	.LBB0_404
# %bb.275:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.276:
	mov	r11d, r9d
	cmp	r9d, 128
	jae	.LBB0_308
# %bb.277:
	xor	esi, esi
	jmp	.LBB0_313
.LBB0_285:
	cmp	edi, 7
	je	.LBB0_350
# %bb.286:
	cmp	edi, 8
	jne	.LBB0_404
# %bb.287:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.288:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_362
# %bb.289:
	xor	esi, esi
	jmp	.LBB0_367
.LBB0_317:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.318:
	mov	r11d, r9d
	cmp	r9d, 64
	jae	.LBB0_320
# %bb.319:
	xor	esi, esi
	jmp	.LBB0_325
.LBB0_329:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.330:
	mov	r11d, r9d
	cmp	r9d, 64
	jae	.LBB0_332
# %bb.331:
	xor	esi, esi
	jmp	.LBB0_337
.LBB0_371:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.372:
	mov	r11d, r9d
	cmp	r9d, 16
	jae	.LBB0_374
# %bb.373:
	xor	esi, esi
	jmp	.LBB0_379
.LBB0_383:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.384:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_386
# %bb.385:
	xor	esi, esi
	jmp	.LBB0_391
.LBB0_296:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.297:
	mov	r11d, r9d
	cmp	r9d, 128
	jae	.LBB0_299
# %bb.298:
	xor	esi, esi
	jmp	.LBB0_304
.LBB0_350:
	test	r9d, r9d
	jle	.LBB0_537
# %bb.351:
	mov	r11d, r9d
	cmp	r9d, 32
	jae	.LBB0_353
# %bb.352:
	xor	esi, esi
	jmp	.LBB0_358
.LBB0_537:
	lea	rsp, [rbp - 16]
	pop	rbx
	pop	r14
	pop	rbp
	vzeroupper
	ret
.LBB0_341:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_346
# %bb.342:
	and	al, r10b
	jne	.LBB0_346
# %bb.343:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_344:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rax]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 96]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rax]
	vpsubd	ymm1, ymm1, ymmword ptr [rcx + 4*rax + 32]
	vpsubd	ymm2, ymm2, ymmword ptr [rcx + 4*rax + 64]
	vpsubd	ymm3, ymm3, ymmword ptr [rcx + 4*rax + 96]
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_344
# %bb.345:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_346:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_348
	.p2align	4, 0x90
.LBB0_347:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_347
.LBB0_348:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_349:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_349
	jmp	.LBB0_404
.LBB0_395:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_400
# %bb.396:
	and	al, r10b
	jne	.LBB0_400
# %bb.397:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_398:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rdx + 8*rax]
	vmovupd	ymm1, ymmword ptr [rdx + 8*rax + 32]
	vmovupd	ymm2, ymmword ptr [rdx + 8*rax + 64]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rax + 96]
	vsubpd	ymm0, ymm0, ymmword ptr [rcx + 8*rax]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rax + 32]
	vsubpd	ymm2, ymm2, ymmword ptr [rcx + 8*rax + 64]
	vsubpd	ymm3, ymm3, ymmword ptr [rcx + 8*rax + 96]
	vmovupd	ymmword ptr [r8 + 8*rax], ymm0
	vmovupd	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_398
# %bb.399:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_400:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_402
	.p2align	4, 0x90
.LBB0_401:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rdx + 8*rsi]   # xmm0 = mem[0],zero
	vsubsd	xmm0, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_401
.LBB0_402:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_403:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_403
	jmp	.LBB0_404
.LBB0_308:
	lea	rsi, [r8 + r11]
	lea	rax, [rdx + r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_313
# %bb.309:
	and	al, r10b
	jne	.LBB0_313
# %bb.310:
	mov	esi, r11d
	and	esi, -128
	xor	eax, eax
.LBB0_311:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax]
	vmovdqu	ymm1, ymmword ptr [rdx + rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 96]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rax]
	vpsubb	ymm1, ymm1, ymmword ptr [rcx + rax + 32]
	vpsubb	ymm2, ymm2, ymmword ptr [rcx + rax + 64]
	vpsubb	ymm3, ymm3, ymmword ptr [rcx + rax + 96]
	vmovdqu	ymmword ptr [r8 + rax], ymm0
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm3
	sub	rax, -128
	cmp	rsi, rax
	jne	.LBB0_311
# %bb.312:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_313:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_315
	.p2align	4, 0x90
.LBB0_314:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_314
.LBB0_315:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_316:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_316
	jmp	.LBB0_404
.LBB0_362:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_367
# %bb.363:
	and	al, r10b
	jne	.LBB0_367
# %bb.364:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_365:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rax]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 96]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rax]
	vpsubq	ymm1, ymm1, ymmword ptr [rcx + 8*rax + 32]
	vpsubq	ymm2, ymm2, ymmword ptr [rcx + 8*rax + 64]
	vpsubq	ymm3, ymm3, ymmword ptr [rcx + 8*rax + 96]
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_365
# %bb.366:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_367:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_369
	.p2align	4, 0x90
.LBB0_368:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_368
.LBB0_369:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_370:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_370
	jmp	.LBB0_404
.LBB0_320:
	lea	rsi, [r8 + 2*r11]
	lea	rax, [rdx + 2*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_325
# %bb.321:
	and	al, r10b
	jne	.LBB0_325
# %bb.322:
	mov	esi, r11d
	and	esi, -64
	xor	eax, eax
.LBB0_323:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rax + 96]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rax]
	vpsubw	ymm1, ymm1, ymmword ptr [rcx + 2*rax + 32]
	vpsubw	ymm2, ymm2, ymmword ptr [rcx + 2*rax + 64]
	vpsubw	ymm3, ymm3, ymmword ptr [rcx + 2*rax + 96]
	vmovdqu	ymmword ptr [r8 + 2*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rax + 96], ymm3
	add	rax, 64
	cmp	rsi, rax
	jne	.LBB0_323
# %bb.324:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_325:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_327
	.p2align	4, 0x90
.LBB0_326:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_326
.LBB0_327:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_328:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_328
	jmp	.LBB0_404
.LBB0_332:
	lea	rsi, [r8 + 2*r11]
	lea	rax, [rdx + 2*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_337
# %bb.333:
	and	al, r10b
	jne	.LBB0_337
# %bb.334:
	mov	esi, r11d
	and	esi, -64
	xor	eax, eax
.LBB0_335:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 2*rax]
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 2*rax + 96]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rax]
	vpsubw	ymm1, ymm1, ymmword ptr [rcx + 2*rax + 32]
	vpsubw	ymm2, ymm2, ymmword ptr [rcx + 2*rax + 64]
	vpsubw	ymm3, ymm3, ymmword ptr [rcx + 2*rax + 96]
	vmovdqu	ymmword ptr [r8 + 2*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rax + 96], ymm3
	add	rax, 64
	cmp	rsi, rax
	jne	.LBB0_335
# %bb.336:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_337:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_339
	.p2align	4, 0x90
.LBB0_338:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_338
.LBB0_339:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_340:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_340
	jmp	.LBB0_404
.LBB0_374:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_379
# %bb.375:
	and	al, r10b
	jne	.LBB0_379
# %bb.376:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_377:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 8*rax]
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 96]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rax]
	vpsubq	ymm1, ymm1, ymmword ptr [rcx + 8*rax + 32]
	vpsubq	ymm2, ymm2, ymmword ptr [rcx + 8*rax + 64]
	vpsubq	ymm3, ymm3, ymmword ptr [rcx + 8*rax + 96]
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_377
# %bb.378:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_379:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_381
	.p2align	4, 0x90
.LBB0_380:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rdx + 8*rsi]
	sub	rbx, qword ptr [rcx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_380
.LBB0_381:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_382:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_382
	jmp	.LBB0_404
.LBB0_386:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_391
# %bb.387:
	and	al, r10b
	jne	.LBB0_391
# %bb.388:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_389:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rdx + 4*rax]
	vmovups	ymm1, ymmword ptr [rdx + 4*rax + 32]
	vmovups	ymm2, ymmword ptr [rdx + 4*rax + 64]
	vmovups	ymm3, ymmword ptr [rdx + 4*rax + 96]
	vsubps	ymm0, ymm0, ymmword ptr [rcx + 4*rax]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rax + 32]
	vsubps	ymm2, ymm2, ymmword ptr [rcx + 4*rax + 64]
	vsubps	ymm3, ymm3, ymmword ptr [rcx + 4*rax + 96]
	vmovups	ymmword ptr [r8 + 4*rax], ymm0
	vmovups	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_389
# %bb.390:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_391:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_393
	.p2align	4, 0x90
.LBB0_392:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rdx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vsubss	xmm0, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_392
.LBB0_393:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_394:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_394
	jmp	.LBB0_404
.LBB0_299:
	lea	rsi, [r8 + r11]
	lea	rax, [rdx + r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_304
# %bb.300:
	and	al, r10b
	jne	.LBB0_304
# %bb.301:
	mov	esi, r11d
	and	esi, -128
	xor	eax, eax
.LBB0_302:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + rax]
	vmovdqu	ymm1, ymmword ptr [rdx + rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 96]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rax]
	vpsubb	ymm1, ymm1, ymmword ptr [rcx + rax + 32]
	vpsubb	ymm2, ymm2, ymmword ptr [rcx + rax + 64]
	vpsubb	ymm3, ymm3, ymmword ptr [rcx + rax + 96]
	vmovdqu	ymmword ptr [r8 + rax], ymm0
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm3
	sub	rax, -128
	cmp	rsi, rax
	jne	.LBB0_302
# %bb.303:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_304:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_306
	.p2align	4, 0x90
.LBB0_305:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rdx + rsi]
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_305
.LBB0_306:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_307:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_307
	jmp	.LBB0_404
.LBB0_353:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_358
# %bb.354:
	and	al, r10b
	jne	.LBB0_358
# %bb.355:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_356:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rdx + 4*rax]
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 96]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rax]
	vpsubd	ymm1, ymm1, ymmword ptr [rcx + 4*rax + 32]
	vpsubd	ymm2, ymm2, ymmword ptr [rcx + 4*rax + 64]
	vpsubd	ymm3, ymm3, ymmword ptr [rcx + 4*rax + 96]
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_356
# %bb.357:
	cmp	rsi, r11
	je	.LBB0_404
.LBB0_358:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_360
	.p2align	4, 0x90
.LBB0_359:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rdx + 4*rsi]
	sub	ebx, dword ptr [rcx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_359
.LBB0_360:
	cmp	r10, 3
	jb	.LBB0_404
	.p2align	4, 0x90
.LBB0_361:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_361
	jmp	.LBB0_404
.LBB0_75:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_80
# %bb.76:
	and	al, r10b
	jne	.LBB0_80
# %bb.77:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_78:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rax + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rax]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rax + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rax + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rax + 96]
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_78
# %bb.79:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_80:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_82
.LBB0_81:                               # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rcx + 4*rsi]
	add	ebx, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_81
.LBB0_82:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_83:                               # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_83
	jmp	.LBB0_138
.LBB0_129:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_134
# %bb.130:
	and	al, r10b
	jne	.LBB0_134
# %bb.131:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_132:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm0, ymmword ptr [rcx + 8*rax]
	vmovupd	ymm1, ymmword ptr [rcx + 8*rax + 32]
	vmovupd	ymm2, ymmword ptr [rcx + 8*rax + 64]
	vmovupd	ymm3, ymmword ptr [rcx + 8*rax + 96]
	vaddpd	ymm0, ymm0, ymmword ptr [rdx + 8*rax]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rax + 32]
	vaddpd	ymm2, ymm2, ymmword ptr [rdx + 8*rax + 64]
	vaddpd	ymm3, ymm3, ymmword ptr [rdx + 8*rax + 96]
	vmovupd	ymmword ptr [r8 + 8*rax], ymm0
	vmovupd	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovupd	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovupd	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_132
# %bb.133:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_134:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_136
.LBB0_135:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm0, qword ptr [rcx + 8*rsi]   # xmm0 = mem[0],zero
	vaddsd	xmm0, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_135
.LBB0_136:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_137:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_137
	jmp	.LBB0_138
.LBB0_42:
	lea	rsi, [r8 + r11]
	lea	rax, [rdx + r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_47
# %bb.43:
	and	al, r10b
	jne	.LBB0_47
# %bb.44:
	mov	esi, r11d
	and	esi, -128
	xor	eax, eax
.LBB0_45:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rax]
	vmovdqu	ymm1, ymmword ptr [rcx + rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rax + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rax]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rax + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rax + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rax + 96]
	vmovdqu	ymmword ptr [r8 + rax], ymm0
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm3
	sub	rax, -128
	cmp	rsi, rax
	jne	.LBB0_45
# %bb.46:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_47:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_49
.LBB0_48:                               # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rcx + rsi]
	add	bl, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_48
.LBB0_49:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_50:                               # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_50
	jmp	.LBB0_138
.LBB0_96:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_101
# %bb.97:
	and	al, r10b
	jne	.LBB0_101
# %bb.98:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_99:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rax + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rax]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rax + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rax + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rax + 96]
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_99
# %bb.100:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_101:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_103
.LBB0_102:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rcx + 8*rsi]
	add	rbx, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_102
.LBB0_103:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_104:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_104
	jmp	.LBB0_138
.LBB0_54:
	lea	rsi, [r8 + 2*r11]
	lea	rax, [rdx + 2*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_59
# %bb.55:
	and	al, r10b
	jne	.LBB0_59
# %bb.56:
	mov	esi, r11d
	and	esi, -64
	xor	eax, eax
.LBB0_57:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rax + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rax]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rax + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rax + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rax + 96]
	vmovdqu	ymmword ptr [r8 + 2*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rax + 96], ymm3
	add	rax, 64
	cmp	rsi, rax
	jne	.LBB0_57
# %bb.58:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_59:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_61
.LBB0_60:                               # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_60
.LBB0_61:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_62:                               # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_62
	jmp	.LBB0_138
.LBB0_66:
	lea	rsi, [r8 + 2*r11]
	lea	rax, [rdx + 2*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 2*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_71
# %bb.67:
	and	al, r10b
	jne	.LBB0_71
# %bb.68:
	mov	esi, r11d
	and	esi, -64
	xor	eax, eax
.LBB0_69:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 2*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 2*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 2*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 2*rax + 96]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rax]
	vpaddw	ymm1, ymm1, ymmword ptr [rdx + 2*rax + 32]
	vpaddw	ymm2, ymm2, ymmword ptr [rdx + 2*rax + 64]
	vpaddw	ymm3, ymm3, ymmword ptr [rdx + 2*rax + 96]
	vmovdqu	ymmword ptr [r8 + 2*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 2*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 2*rax + 96], ymm3
	add	rax, 64
	cmp	rsi, rax
	jne	.LBB0_69
# %bb.70:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_71:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_73
.LBB0_72:                               # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, word ptr [rdx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_72
.LBB0_73:
	cmp	r10, 3
	jb	.LBB0_138
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
	cmp	r11, rsi
	jne	.LBB0_74
	jmp	.LBB0_138
.LBB0_108:
	lea	rsi, [r8 + 8*r11]
	lea	rax, [rdx + 8*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 8*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_113
# %bb.109:
	and	al, r10b
	jne	.LBB0_113
# %bb.110:
	mov	esi, r11d
	and	esi, -16
	xor	eax, eax
.LBB0_111:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 8*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 8*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 8*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 8*rax + 96]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rax]
	vpaddq	ymm1, ymm1, ymmword ptr [rdx + 8*rax + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdx + 8*rax + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdx + 8*rax + 96]
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm3
	add	rax, 16
	cmp	rsi, rax
	jne	.LBB0_111
# %bb.112:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_113:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_115
.LBB0_114:                              # =>This Inner Loop Header: Depth=1
	mov	rbx, qword ptr [rcx + 8*rsi]
	add	rbx, qword ptr [rdx + 8*rsi]
	mov	qword ptr [r8 + 8*rsi], rbx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_114
.LBB0_115:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_116:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_116
	jmp	.LBB0_138
.LBB0_120:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_125
# %bb.121:
	and	al, r10b
	jne	.LBB0_125
# %bb.122:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_123:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm0, ymmword ptr [rcx + 4*rax]
	vmovups	ymm1, ymmword ptr [rcx + 4*rax + 32]
	vmovups	ymm2, ymmword ptr [rcx + 4*rax + 64]
	vmovups	ymm3, ymmword ptr [rcx + 4*rax + 96]
	vaddps	ymm0, ymm0, ymmword ptr [rdx + 4*rax]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rax + 32]
	vaddps	ymm2, ymm2, ymmword ptr [rdx + 4*rax + 64]
	vaddps	ymm3, ymm3, ymmword ptr [rdx + 4*rax + 96]
	vmovups	ymmword ptr [r8 + 4*rax], ymm0
	vmovups	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovups	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovups	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_123
# %bb.124:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_125:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_127
.LBB0_126:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm0, dword ptr [rcx + 4*rsi]   # xmm0 = mem[0],zero,zero,zero
	vaddss	xmm0, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm0
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_126
.LBB0_127:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_128:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_128
	jmp	.LBB0_138
.LBB0_33:
	lea	rsi, [r8 + r11]
	lea	rax, [rdx + r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_38
# %bb.34:
	and	al, r10b
	jne	.LBB0_38
# %bb.35:
	mov	esi, r11d
	and	esi, -128
	xor	eax, eax
.LBB0_36:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + rax]
	vmovdqu	ymm1, ymmword ptr [rcx + rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + rax + 96]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rax]
	vpaddb	ymm1, ymm1, ymmword ptr [rdx + rax + 32]
	vpaddb	ymm2, ymm2, ymmword ptr [rdx + rax + 64]
	vpaddb	ymm3, ymm3, ymmword ptr [rdx + rax + 96]
	vmovdqu	ymmword ptr [r8 + rax], ymm0
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm3
	sub	rax, -128
	cmp	rsi, rax
	jne	.LBB0_36
# %bb.37:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_38:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_40
.LBB0_39:                               # =>This Inner Loop Header: Depth=1
	movzx	ebx, byte ptr [rcx + rsi]
	add	bl, byte ptr [rdx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_39
.LBB0_40:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_41:                               # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_41
	jmp	.LBB0_138
.LBB0_87:
	lea	rsi, [r8 + 4*r11]
	lea	rax, [rdx + 4*r11]
	cmp	rax, r8
	seta	r14b
	lea	rax, [rcx + 4*r11]
	cmp	rsi, rdx
	seta	bl
	cmp	rax, r8
	seta	al
	cmp	rsi, rcx
	seta	r10b
	xor	esi, esi
	test	r14b, bl
	jne	.LBB0_92
# %bb.88:
	and	al, r10b
	jne	.LBB0_92
# %bb.89:
	mov	esi, r11d
	and	esi, -32
	xor	eax, eax
.LBB0_90:                               # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm0, ymmword ptr [rcx + 4*rax]
	vmovdqu	ymm1, ymmword ptr [rcx + 4*rax + 32]
	vmovdqu	ymm2, ymmword ptr [rcx + 4*rax + 64]
	vmovdqu	ymm3, ymmword ptr [rcx + 4*rax + 96]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rax]
	vpaddd	ymm1, ymm1, ymmword ptr [rdx + 4*rax + 32]
	vpaddd	ymm2, ymm2, ymmword ptr [rdx + 4*rax + 64]
	vpaddd	ymm3, ymm3, ymmword ptr [rdx + 4*rax + 96]
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm3
	add	rax, 32
	cmp	rsi, rax
	jne	.LBB0_90
# %bb.91:
	cmp	rsi, r11
	je	.LBB0_138
.LBB0_92:
	mov	r10, rsi
	not	r10
	add	r10, r11
	mov	rax, r11
	and	rax, 3
	je	.LBB0_94
.LBB0_93:                               # =>This Inner Loop Header: Depth=1
	mov	ebx, dword ptr [rcx + 4*rsi]
	add	ebx, dword ptr [rdx + 4*rsi]
	mov	dword ptr [r8 + 4*rsi], ebx
	add	rsi, 1
	add	rax, -1
	jne	.LBB0_93
.LBB0_94:
	cmp	r10, 3
	jb	.LBB0_138
.LBB0_95:                               # =>This Inner Loop Header: Depth=1
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
	cmp	r11, rsi
	jne	.LBB0_95
	jmp	.LBB0_138
.Lfunc_end0:
	.size	arithmetic_avx2, .Lfunc_end0-arithmetic_avx2
                                        # -- End function
	.globl	arithmetic_arr_scalar_avx2      # -- Begin function arithmetic_arr_scalar_avx2
	.p2align	4, 0x90
	.type	arithmetic_arr_scalar_avx2,@function
arithmetic_arr_scalar_avx2:             # @arithmetic_arr_scalar_avx2
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
	cmp	r9d, 32
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
	cmp	r9d, 32
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
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
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
	vmovsd	xmm1, qword ptr [rdx + 8*rcx]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_327
.LBB1_328:
	cmp	rsi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_329:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r9d, 32
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
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 16
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
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_407
.LBB1_408:
	cmp	rax, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_409:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm1
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
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 16
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
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_671
.LBB1_672:
	cmp	rax, 3
	jb	.LBB1_3
.LBB1_673:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rdx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 16
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
	cmp	r9d, 128
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
	cmp	r9d, 16
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
	cmp	r9d, 32
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
	cmp	r9d, 32
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
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
	vmovss	xmm1, dword ptr [rdx + 4*rcx]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rcx], xmm1
	add	rcx, 1
	add	rdi, -1
	jne	.LBB1_375
.LBB1_376:
	cmp	rsi, 3
	jb	.LBB1_517
	.p2align	4, 0x90
.LBB1_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_377
	jmp	.LBB1_517
.LBB1_82:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.83:
	mov	al, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
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
	cmp	r9d, 32
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
	cmp	r9d, 128
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
	cmp	r9d, 16
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB1_432
	.p2align	4, 0x90
.LBB1_431:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB1_440
	.p2align	4, 0x90
.LBB1_439:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 32
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
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_455
.LBB1_456:
	cmp	rax, 3
	jb	.LBB1_474
	.p2align	4, 0x90
.LBB1_457:                              # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 32
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB1_696
.LBB1_695:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB1_704
.LBB1_703:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 32
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
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_719
.LBB1_720:
	cmp	rax, 3
	jb	.LBB1_3
.LBB1_721:                              # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rdx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 32
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
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_314
# %bb.195:
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
	xor	edi, edi
.LBB1_196:                              # =>This Inner Loop Header: Depth=1
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
	add	rbx, 2
	jne	.LBB1_196
	jmp	.LBB1_315
.LBB1_197:
	mov	ecx, eax
	and	ecx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rcx - 16]
	mov	rbx, rsi
	shr	rbx, 4
	add	rbx, 1
	test	rsi, rsi
	je	.LBB1_322
# %bb.198:
	mov	rdi, rbx
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_199:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_199
	jmp	.LBB1_323
.LBB1_200:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_330
# %bb.201:
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
	xor	edi, edi
.LBB1_202:                              # =>This Inner Loop Header: Depth=1
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
	add	rbx, 2
	jne	.LBB1_202
	jmp	.LBB1_331
.LBB1_203:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_338
# %bb.204:
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
	xor	edi, edi
.LBB1_205:                              # =>This Inner Loop Header: Depth=1
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
	add	rbx, 2
	jne	.LBB1_205
	jmp	.LBB1_339
.LBB1_206:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_346
# %bb.207:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_208:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rdi, 2
	jne	.LBB1_208
	jmp	.LBB1_347
.LBB1_209:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_354
# %bb.210:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB1_211:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rdi, 2
	jne	.LBB1_211
	jmp	.LBB1_355
.LBB1_212:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, rax
	vpbroadcastq	ymm0, xmm0
	lea	rcx, [rsi - 16]
	mov	r9, rcx
	shr	r9, 4
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_362
# %bb.213:
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
	xor	edi, edi
.LBB1_214:                              # =>This Inner Loop Header: Depth=1
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
	add	rbx, 2
	jne	.LBB1_214
	jmp	.LBB1_363
.LBB1_215:
	mov	ecx, eax
	and	ecx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rcx - 32]
	mov	rbx, rsi
	shr	rbx, 5
	add	rbx, 1
	test	rsi, rsi
	je	.LBB1_370
# %bb.216:
	mov	rdi, rbx
	and	rdi, -2
	neg	rdi
	xor	esi, esi
.LBB1_217:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB1_217
	jmp	.LBB1_371
.LBB1_218:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rcx, [rsi - 128]
	mov	r9, rcx
	shr	r9, 7
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_378
# %bb.219:
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
	xor	edi, edi
.LBB1_220:                              # =>This Inner Loop Header: Depth=1
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
	add	rbx, 2
	jne	.LBB1_220
	jmp	.LBB1_379
.LBB1_221:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastd	ymm0, xmm0
	lea	rcx, [rsi - 32]
	mov	r9, rcx
	shr	r9, 5
	add	r9, 1
	test	rcx, rcx
	je	.LBB1_386
# %bb.222:
	mov	rbx, r9
	and	rbx, -2
	neg	rbx
	xor	edi, edi
.LBB1_223:                              # =>This Inner Loop Header: Depth=1
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
	add	rbx, 2
	jne	.LBB1_223
	jmp	.LBB1_387
.LBB1_254:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_394
# %bb.255:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_256:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_256
	jmp	.LBB1_395
.LBB1_257:
	mov	esi, r11d
	and	esi, -16
	vbroadcastsd	ymm1, xmm0
	lea	rax, [rsi - 16]
	mov	r10, rax
	shr	r10, 4
	add	r10, 1
	test	rax, rax
	je	.LBB1_402
# %bb.258:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_259:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rbx + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rbx + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rbx + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rbx + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rbx + 224]
	vmovupd	ymmword ptr [r8 + 8*rbx + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 224], ymm5
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_259
	jmp	.LBB1_403
.LBB1_260:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB1_410
# %bb.261:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_262:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB1_262
	jmp	.LBB1_411
.LBB1_263:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_418
# %bb.264:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_265:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_265
	jmp	.LBB1_419
.LBB1_266:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_426
# %bb.267:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_268:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_268
	jmp	.LBB1_427
.LBB1_269:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_434
# %bb.270:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_271:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_271
	jmp	.LBB1_435
.LBB1_272:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_442
# %bb.273:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_274:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_274
	jmp	.LBB1_443
.LBB1_275:
	mov	esi, r11d
	and	esi, -32
	vbroadcastss	ymm1, xmm0
	lea	rax, [rsi - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	test	rax, rax
	je	.LBB1_450
# %bb.276:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_277:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rbx + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rbx + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rbx + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rbx + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rbx + 224]
	vmovups	ymmword ptr [r8 + 4*rbx + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 224], ymm5
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_277
	jmp	.LBB1_451
.LBB1_278:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB1_458
# %bb.279:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_280:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB1_280
	jmp	.LBB1_459
.LBB1_281:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_466
# %bb.282:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_283:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_283
	jmp	.LBB1_467
.LBB1_284:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_658
# %bb.285:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_286:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_286
	jmp	.LBB1_659
.LBB1_287:
	mov	esi, r11d
	and	esi, -16
	vbroadcastsd	ymm1, xmm0
	lea	rax, [rsi - 16]
	mov	r10, rax
	shr	r10, 4
	add	r10, 1
	test	rax, rax
	je	.LBB1_666
# %bb.288:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_289:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rbx + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rbx + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rbx + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rbx + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rdx + 8*rbx + 224]
	vmovupd	ymmword ptr [r8 + 8*rbx + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 224], ymm5
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_289
	jmp	.LBB1_667
.LBB1_290:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB1_674
# %bb.291:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_292:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB1_292
	jmp	.LBB1_675
.LBB1_293:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_682
# %bb.294:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_295:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_295
	jmp	.LBB1_683
.LBB1_296:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_690
# %bb.297:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_298:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_298
	jmp	.LBB1_691
.LBB1_299:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_698
# %bb.300:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_301:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rdx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_301
	jmp	.LBB1_699
.LBB1_302:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_706
# %bb.303:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_304:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rdx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB1_304
	jmp	.LBB1_707
.LBB1_305:
	mov	esi, r11d
	and	esi, -32
	vbroadcastss	ymm1, xmm0
	lea	rax, [rsi - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	test	rax, rax
	je	.LBB1_714
# %bb.306:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_307:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rbx + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rbx + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rbx + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rbx + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rdx + 4*rbx + 224]
	vmovups	ymmword ptr [r8 + 4*rbx + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 224], ymm5
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_307
	jmp	.LBB1_715
.LBB1_308:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB1_722
# %bb.309:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_310:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rdx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB1_310
	jmp	.LBB1_723
.LBB1_311:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_730
# %bb.312:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_313:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rdx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_313
	jmp	.LBB1_731
.LBB1_314:
	xor	edi, edi
.LBB1_315:
	test	r9b, 1
	je	.LBB1_317
# %bb.316:
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
.LBB1_317:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_318
.LBB1_322:
	xor	esi, esi
.LBB1_323:
	test	bl, 1
	je	.LBB1_325
# %bb.324:
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
.LBB1_325:
	cmp	rcx, rax
	je	.LBB1_517
	jmp	.LBB1_326
.LBB1_330:
	xor	edi, edi
.LBB1_331:
	test	r9b, 1
	je	.LBB1_333
# %bb.332:
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
.LBB1_333:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_334
.LBB1_338:
	xor	edi, edi
.LBB1_339:
	test	r9b, 1
	je	.LBB1_341
# %bb.340:
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
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
.LBB1_357:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_358
.LBB1_362:
	xor	edi, edi
.LBB1_363:
	test	r9b, 1
	je	.LBB1_365
# %bb.364:
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
.LBB1_365:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_366
.LBB1_370:
	xor	esi, esi
.LBB1_371:
	test	bl, 1
	je	.LBB1_373
# %bb.372:
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
.LBB1_373:
	cmp	rcx, rax
	je	.LBB1_517
	jmp	.LBB1_374
.LBB1_378:
	xor	edi, edi
.LBB1_379:
	test	r9b, 1
	je	.LBB1_381
# %bb.380:
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
.LBB1_381:
	cmp	rsi, r10
	je	.LBB1_517
	jmp	.LBB1_382
.LBB1_386:
	xor	edi, edi
.LBB1_387:
	test	r9b, 1
	je	.LBB1_389
# %bb.388:
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
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rbx + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rbx + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	mov	r11d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
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
	mov	r14, rsi
	not	r14
	add	r14, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_584
	.p2align	4, 0x90
.LBB1_583:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_583
.LBB1_584:
	cmp	r14, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_585:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, r11d
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
	vmovsd	xmm0, qword ptr [rcx]           # xmm0 = mem[0],zero
	mov	r10d, r9d
	cmp	r9d, 16
	jb	.LBB1_488
# %bb.521:
	lea	rax, [rdx + 8*r10]
	cmp	rax, r8
	jbe	.LBB1_551
# %bb.522:
	lea	rax, [r8 + 8*r10]
	cmp	rax, rdx
	jbe	.LBB1_551
.LBB1_488:
	xor	esi, esi
.LBB1_590:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_592
	.p2align	4, 0x90
.LBB1_591:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_591
.LBB1_592:
	cmp	rax, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_593:                              # =>This Inner Loop Header: Depth=1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi]   # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 8] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 16] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm1
	vmovsd	xmm1, qword ptr [rdx + 8*rsi + 24] # xmm1 = mem[0],zero
	vsubsd	xmm1, xmm1, xmm0
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm1
	add	rsi, 4
	cmp	r10, rsi
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
	mov	r11b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
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
	mov	r14, rsi
	not	r14
	add	r14, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_600
	.p2align	4, 0x90
.LBB1_599:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r11b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_599
.LBB1_600:
	cmp	r14, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_601:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r11b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, r11b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, r11b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, r11b
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
	mov	r11, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
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
	mov	r14, rsi
	not	r14
	add	r14, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_608
	.p2align	4, 0x90
.LBB1_607:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_607
.LBB1_608:
	cmp	r14, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_609:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, r11
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB1_616
	.p2align	4, 0x90
.LBB1_615:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	ebx, r14d
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB1_624
	.p2align	4, 0x90
.LBB1_623:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rdx + 2*rsi]
	sub	ebx, r14d
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	mov	r11, qword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 16
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
	mov	r14, rsi
	not	r14
	add	r14, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_632
	.p2align	4, 0x90
.LBB1_631:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi], rax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_631
.LBB1_632:
	cmp	r14, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_633:                              # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdx + 8*rsi]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi], rax
	mov	rax, qword ptr [rdx + 8*rsi + 8]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi + 8], rax
	mov	rax, qword ptr [rdx + 8*rsi + 16]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi + 16], rax
	mov	rax, qword ptr [rdx + 8*rsi + 24]
	sub	rax, r11
	mov	qword ptr [r8 + 8*rsi + 24], rax
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_633
	jmp	.LBB1_13
.LBB1_508:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.509:
	vmovss	xmm0, dword ptr [rcx]           # xmm0 = mem[0],zero,zero,zero
	mov	r10d, r9d
	cmp	r9d, 32
	jb	.LBB1_510
# %bb.539:
	lea	rax, [rdx + 4*r10]
	cmp	rax, r8
	jbe	.LBB1_569
# %bb.540:
	lea	rax, [r8 + 4*r10]
	cmp	rax, rdx
	jbe	.LBB1_569
.LBB1_510:
	xor	esi, esi
.LBB1_638:
	mov	rax, rsi
	not	rax
	add	rax, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_640
	.p2align	4, 0x90
.LBB1_639:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_639
.LBB1_640:
	cmp	rax, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_641:                              # =>This Inner Loop Header: Depth=1
	vmovss	xmm1, dword ptr [rdx + 4*rsi]   # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 4] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 8] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm1
	vmovss	xmm1, dword ptr [rdx + 4*rsi + 12] # xmm1 = mem[0],zero,zero,zero
	vsubss	xmm1, xmm1, xmm0
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm1
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_641
	jmp	.LBB1_13
.LBB1_511:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.512:
	mov	r11b, byte ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 128
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
	mov	r14, rsi
	not	r14
	add	r14, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_648
	.p2align	4, 0x90
.LBB1_647:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r11b
	mov	byte ptr [r8 + rsi], al
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_647
.LBB1_648:
	cmp	r14, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_649:                              # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdx + rsi]
	sub	al, r11b
	mov	byte ptr [r8 + rsi], al
	movzx	eax, byte ptr [rdx + rsi + 1]
	sub	al, r11b
	mov	byte ptr [r8 + rsi + 1], al
	movzx	eax, byte ptr [rdx + rsi + 2]
	sub	al, r11b
	mov	byte ptr [r8 + rsi + 2], al
	movzx	eax, byte ptr [rdx + rsi + 3]
	sub	al, r11b
	mov	byte ptr [r8 + rsi + 3], al
	add	rsi, 4
	cmp	r10, rsi
	jne	.LBB1_649
	jmp	.LBB1_13
.LBB1_514:
	test	r9d, r9d
	jle	.LBB1_517
# %bb.515:
	mov	r11d, dword ptr [rcx]
	mov	r10d, r9d
	cmp	r9d, 32
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
	mov	r14, rsi
	not	r14
	add	r14, r10
	mov	rbx, r10
	and	rbx, 3
	je	.LBB1_656
	.p2align	4, 0x90
.LBB1_655:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi], eax
	add	rsi, 1
	add	rbx, -1
	jne	.LBB1_655
.LBB1_656:
	cmp	r14, 3
	jb	.LBB1_13
	.p2align	4, 0x90
.LBB1_657:                              # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdx + 4*rsi]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi], eax
	mov	eax, dword ptr [rdx + 4*rsi + 4]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi + 4], eax
	mov	eax, dword ptr [rdx + 4*rsi + 8]
	sub	eax, r11d
	mov	dword ptr [r8 + 4*rsi + 8], eax
	mov	eax, dword ptr [rdx + 4*rsi + 12]
	sub	eax, r11d
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
	vzeroupper
	ret
.LBB1_548:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r14, rax
	shr	r14, 5
	add	r14, 1
	test	rax, rax
	je	.LBB1_578
# %bb.549:
	mov	rbx, r14
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_550:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rax + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rax + 224]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rax + 224], ymm4
	add	rax, 64
	add	rbx, 2
	jne	.LBB1_550
	jmp	.LBB1_579
.LBB1_551:
	mov	esi, r10d
	and	esi, -16
	vbroadcastsd	ymm1, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB1_586
# %bb.552:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_553:                              # =>This Inner Loop Header: Depth=1
	vmovupd	ymm2, ymmword ptr [rdx + 8*rax]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rax + 32]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rax + 64]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rax + 96]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm5, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rax], ymm2
	vmovupd	ymmword ptr [r8 + 8*rax + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rax + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rax + 96], ymm5
	vmovupd	ymm2, ymmword ptr [rdx + 8*rax + 128]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rax + 160]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rax + 192]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rax + 224]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm5, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rax + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rax + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rax + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rax + 224], ymm5
	add	rax, 32
	add	rbx, 2
	jne	.LBB1_553
	jmp	.LBB1_587
.LBB1_554:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r11d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r14, rax
	shr	r14, 7
	add	r14, 1
	test	rax, rax
	je	.LBB1_594
# %bb.555:
	mov	rbx, r14
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_556:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rax + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + rax + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + rax + 224]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rax + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rax + 224], ymm4
	add	rax, 256
	add	rbx, 2
	jne	.LBB1_556
	jmp	.LBB1_595
.LBB1_557:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r14, rax
	shr	r14, 4
	add	r14, 1
	test	rax, rax
	je	.LBB1_602
# %bb.558:
	mov	rbx, r14
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_559:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rax + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rax + 224]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rax + 224], ymm4
	add	rax, 32
	add	rbx, 2
	jne	.LBB1_559
	jmp	.LBB1_603
.LBB1_560:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_610
# %bb.561:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_562:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_562
	jmp	.LBB1_611
.LBB1_563:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_618
# %bb.564:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB1_565:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx + 64]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 96]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB1_565
	jmp	.LBB1_619
.LBB1_566:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r14, rax
	shr	r14, 4
	add	r14, 1
	test	rax, rax
	je	.LBB1_626
# %bb.567:
	mov	rbx, r14
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_568:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rax + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rax + 224]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rax + 224], ymm4
	add	rax, 32
	add	rbx, 2
	jne	.LBB1_568
	jmp	.LBB1_627
.LBB1_569:
	mov	esi, r10d
	and	esi, -32
	vbroadcastss	ymm1, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB1_634
# %bb.570:
	mov	rbx, r11
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_571:                              # =>This Inner Loop Header: Depth=1
	vmovups	ymm2, ymmword ptr [rdx + 4*rax]
	vmovups	ymm3, ymmword ptr [rdx + 4*rax + 32]
	vmovups	ymm4, ymmword ptr [rdx + 4*rax + 64]
	vmovups	ymm5, ymmword ptr [rdx + 4*rax + 96]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm5, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rax], ymm2
	vmovups	ymmword ptr [r8 + 4*rax + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rax + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rax + 96], ymm5
	vmovups	ymm2, ymmword ptr [rdx + 4*rax + 128]
	vmovups	ymm3, ymmword ptr [rdx + 4*rax + 160]
	vmovups	ymm4, ymmword ptr [rdx + 4*rax + 192]
	vmovups	ymm5, ymmword ptr [rdx + 4*rax + 224]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm5, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rax + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rax + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rax + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rax + 224], ymm5
	add	rax, 64
	add	rbx, 2
	jne	.LBB1_571
	jmp	.LBB1_635
.LBB1_572:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r11d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r14, rax
	shr	r14, 7
	add	r14, 1
	test	rax, rax
	je	.LBB1_642
# %bb.573:
	mov	rbx, r14
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_574:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rax + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + rax + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + rax + 224]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rax + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rax + 224], ymm4
	add	rax, 256
	add	rbx, 2
	jne	.LBB1_574
	jmp	.LBB1_643
.LBB1_575:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r14, rax
	shr	r14, 5
	add	r14, 1
	test	rax, rax
	je	.LBB1_650
# %bb.576:
	mov	rbx, r14
	and	rbx, -2
	neg	rbx
	xor	eax, eax
.LBB1_577:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rax + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm4
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax + 128]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 160]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 192]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rax + 224]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rax + 224], ymm4
	add	rax, 64
	add	rbx, 2
	jne	.LBB1_577
	jmp	.LBB1_651
.LBB1_578:
	xor	eax, eax
.LBB1_579:
	test	r14b, 1
	je	.LBB1_581
# %bb.580:
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rax + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm0
.LBB1_581:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_582
.LBB1_586:
	xor	eax, eax
.LBB1_587:
	test	r11b, 1
	je	.LBB1_589
# %bb.588:
	vmovupd	ymm2, ymmword ptr [rdx + 8*rax]
	vmovupd	ymm3, ymmword ptr [rdx + 8*rax + 32]
	vmovupd	ymm4, ymmword ptr [rdx + 8*rax + 64]
	vmovupd	ymm5, ymmword ptr [rdx + 8*rax + 96]
	vsubpd	ymm2, ymm2, ymm1
	vsubpd	ymm3, ymm3, ymm1
	vsubpd	ymm4, ymm4, ymm1
	vsubpd	ymm1, ymm5, ymm1
	vmovupd	ymmword ptr [r8 + 8*rax], ymm2
	vmovupd	ymmword ptr [r8 + 8*rax + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rax + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rax + 96], ymm1
.LBB1_589:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_590
.LBB1_594:
	xor	eax, eax
.LBB1_595:
	test	r14b, 1
	je	.LBB1_597
# %bb.596:
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rax + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm0
.LBB1_597:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_598
.LBB1_602:
	xor	eax, eax
.LBB1_603:
	test	r14b, 1
	je	.LBB1_605
# %bb.604:
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rax + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm0
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
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vmovdqu	ymm1, ymmword ptr [rdx + 2*rbx]
	vmovdqu	ymm2, ymmword ptr [rdx + 2*rbx + 32]
	vpsubw	ymm1, ymm1, ymm0
	vpsubw	ymm0, ymm2, ymm0
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
.LBB1_621:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_622
.LBB1_626:
	xor	eax, eax
.LBB1_627:
	test	r14b, 1
	je	.LBB1_629
# %bb.628:
	vmovdqu	ymm1, ymmword ptr [rdx + 8*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 8*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 8*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 8*rax + 96]
	vpsubq	ymm1, ymm1, ymm0
	vpsubq	ymm2, ymm2, ymm0
	vpsubq	ymm3, ymm3, ymm0
	vpsubq	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 8*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rax + 96], ymm0
.LBB1_629:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_630
.LBB1_634:
	xor	eax, eax
.LBB1_635:
	test	r11b, 1
	je	.LBB1_637
# %bb.636:
	vmovups	ymm2, ymmword ptr [rdx + 4*rax]
	vmovups	ymm3, ymmword ptr [rdx + 4*rax + 32]
	vmovups	ymm4, ymmword ptr [rdx + 4*rax + 64]
	vmovups	ymm5, ymmword ptr [rdx + 4*rax + 96]
	vsubps	ymm2, ymm2, ymm1
	vsubps	ymm3, ymm3, ymm1
	vsubps	ymm4, ymm4, ymm1
	vsubps	ymm1, ymm5, ymm1
	vmovups	ymmword ptr [r8 + 4*rax], ymm2
	vmovups	ymmword ptr [r8 + 4*rax + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rax + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rax + 96], ymm1
.LBB1_637:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_638
.LBB1_642:
	xor	eax, eax
.LBB1_643:
	test	r14b, 1
	je	.LBB1_645
# %bb.644:
	vmovdqu	ymm1, ymmword ptr [rdx + rax]
	vmovdqu	ymm2, ymmword ptr [rdx + rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + rax + 96]
	vpsubb	ymm1, ymm1, ymm0
	vpsubb	ymm2, ymm2, ymm0
	vpsubb	ymm3, ymm3, ymm0
	vpsubb	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + rax], ymm1
	vmovdqu	ymmword ptr [r8 + rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rax + 96], ymm0
.LBB1_645:
	cmp	rsi, r10
	je	.LBB1_13
	jmp	.LBB1_646
.LBB1_650:
	xor	eax, eax
.LBB1_651:
	test	r14b, 1
	je	.LBB1_653
# %bb.652:
	vmovdqu	ymm1, ymmword ptr [rdx + 4*rax]
	vmovdqu	ymm2, ymmword ptr [rdx + 4*rax + 32]
	vmovdqu	ymm3, ymmword ptr [rdx + 4*rax + 64]
	vmovdqu	ymm4, ymmword ptr [rdx + 4*rax + 96]
	vpsubd	ymm1, ymm1, ymm0
	vpsubd	ymm2, ymm2, ymm0
	vpsubd	ymm3, ymm3, ymm0
	vpsubd	ymm0, ymm4, ymm0
	vmovdqu	ymmword ptr [r8 + 4*rax], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rax + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rax + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rax + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vaddpd	ymm2, ymm1, ymmword ptr [rdx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rdx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rdx + 8*rbx + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rdx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rdx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rdx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rdx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rdx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rdx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rdx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vaddps	ymm2, ymm1, ymmword ptr [rdx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rdx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rdx + 4*rbx + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rdx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rdx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rdx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rdx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rdx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rdx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rdx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rdx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rdx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
.LBB1_733:
	cmp	rsi, r10
	je	.LBB1_3
	jmp	.LBB1_734
.Lfunc_end1:
	.size	arithmetic_arr_scalar_avx2, .Lfunc_end1-arithmetic_arr_scalar_avx2
                                        # -- End function
	.globl	arithmetic_scalar_arr_avx2      # -- Begin function arithmetic_scalar_arr_avx2
	.p2align	4, 0x90
	.type	arithmetic_scalar_arr_avx2,@function
arithmetic_scalar_arr_avx2:             # @arithmetic_scalar_arr_avx2
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
	cmp	r9d, 32
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
	cmp	r9d, 32
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
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	eax, r9d
	cmp	r9d, 16
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
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rdx]
	vmovsd	qword ptr [r8 + 8*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_327
.LBB2_328:
	cmp	rsi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_329:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r9d, 32
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
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 16
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
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_407
.LBB2_408:
	cmp	rax, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_409:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm1
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
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 16
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
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_671
.LBB2_672:
	cmp	rax, 3
	jb	.LBB2_3
.LBB2_673:                              # =>This Inner Loop Header: Depth=1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm1
	vaddsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm1
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
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
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
	mov	ebx, eax
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_335
.LBB2_336:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_337:                              # =>This Inner Loop Header: Depth=1
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
	cmp	r9d, 16
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
	cmp	r9d, 128
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
	cmp	r9d, 16
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
	cmp	r9d, 32
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
	cmp	r9d, 32
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	eax, r9d
	cmp	r9d, 32
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
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rdx]
	vmovss	dword ptr [r8 + 4*rdx], xmm1
	add	rdx, 1
	add	rdi, -1
	jne	.LBB2_375
.LBB2_376:
	cmp	rsi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_377:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_377
	jmp	.LBB2_517
.LBB2_82:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.83:
	mov	al, byte ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 128
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
	mov	ebx, eax
	sub	bl, byte ptr [rcx + rsi]
	mov	byte ptr [r8 + rsi], bl
	add	rsi, 1
	add	rdx, -1
	jne	.LBB2_383
.LBB2_384:
	cmp	rdi, 3
	jb	.LBB2_517
	.p2align	4, 0x90
.LBB2_385:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_385
	jmp	.LBB2_517
.LBB2_85:
	test	r9d, r9d
	jle	.LBB2_517
# %bb.86:
	mov	r11d, dword ptr [rdx]
	mov	r10d, r9d
	cmp	r9d, 32
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
	cmp	r9d, 128
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
	cmp	r9d, 16
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB2_432
	.p2align	4, 0x90
.LBB2_431:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB2_440
	.p2align	4, 0x90
.LBB2_439:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 32
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
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_455
.LBB2_456:
	cmp	rax, 3
	jb	.LBB2_474
	.p2align	4, 0x90
.LBB2_457:                              # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 32
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB2_696
.LBB2_695:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB2_704
.LBB2_703:                              # =>This Inner Loop Header: Depth=1
	movzx	ebx, word ptr [rcx + 2*rsi]
	add	bx, r14w
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 32
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
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_719
.LBB2_720:
	cmp	rax, 3
	jb	.LBB2_3
.LBB2_721:                              # =>This Inner Loop Header: Depth=1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm1
	vaddss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 32
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
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_314
# %bb.195:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_196:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rdi, 2
	jne	.LBB2_196
	jmp	.LBB2_315
.LBB2_197:
	mov	edx, eax
	and	edx, -16
	vbroadcastsd	ymm1, xmm0
	lea	rsi, [rdx - 16]
	mov	rbx, rsi
	shr	rbx, 4
	add	rbx, 1
	test	rsi, rsi
	je	.LBB2_322
# %bb.198:
	mov	rsi, rbx
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_199:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_199
	jmp	.LBB2_323
.LBB2_200:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_330
# %bb.201:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_202:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rdi, 2
	jne	.LBB2_202
	jmp	.LBB2_331
.LBB2_203:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_338
# %bb.204:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_205:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rdi, 2
	jne	.LBB2_205
	jmp	.LBB2_339
.LBB2_206:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_346
# %bb.207:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_208:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rdi, 2
	jne	.LBB2_208
	jmp	.LBB2_347
.LBB2_209:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, eax
	vpbroadcastw	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_354
# %bb.210:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_211:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rdi, 2
	jne	.LBB2_211
	jmp	.LBB2_355
.LBB2_212:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r11
	vpbroadcastq	ymm0, xmm0
	lea	rdx, [rsi - 16]
	mov	r9, rdx
	shr	r9, 4
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_362
# %bb.213:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_214:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rdi, 2
	jne	.LBB2_214
	jmp	.LBB2_363
.LBB2_215:
	mov	edx, eax
	and	edx, -32
	vbroadcastss	ymm1, xmm0
	lea	rsi, [rdx - 32]
	mov	rbx, rsi
	shr	rbx, 5
	add	rbx, 1
	test	rsi, rsi
	je	.LBB2_370
# %bb.216:
	mov	rsi, rbx
	and	rsi, -2
	neg	rsi
	xor	edi, edi
.LBB2_217:                              # =>This Inner Loop Header: Depth=1
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
	jne	.LBB2_217
	jmp	.LBB2_371
.LBB2_218:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, eax
	vpbroadcastb	ymm0, xmm0
	lea	rdx, [rsi - 128]
	mov	r9, rdx
	shr	r9, 7
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_378
# %bb.219:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_220:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rdi, 2
	jne	.LBB2_220
	jmp	.LBB2_379
.LBB2_221:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r11d
	vpbroadcastd	ymm0, xmm0
	lea	rdx, [rsi - 32]
	mov	r9, rdx
	shr	r9, 5
	add	r9, 1
	test	rdx, rdx
	je	.LBB2_386
# %bb.222:
	mov	rdi, r9
	and	rdi, -2
	neg	rdi
	xor	ebx, ebx
.LBB2_223:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rdi, 2
	jne	.LBB2_223
	jmp	.LBB2_387
.LBB2_254:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_394
# %bb.255:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_256:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_256
	jmp	.LBB2_395
.LBB2_257:
	mov	esi, r11d
	and	esi, -16
	vbroadcastsd	ymm1, xmm0
	lea	rax, [rsi - 16]
	mov	r10, rax
	shr	r10, 4
	add	r10, 1
	test	rax, rax
	je	.LBB2_402
# %bb.258:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_259:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rbx + 224]
	vmovupd	ymmword ptr [r8 + 8*rbx + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 224], ymm5
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_259
	jmp	.LBB2_403
.LBB2_260:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB2_410
# %bb.261:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_262:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB2_262
	jmp	.LBB2_411
.LBB2_263:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_418
# %bb.264:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_265:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_265
	jmp	.LBB2_419
.LBB2_266:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_426
# %bb.267:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_268:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_268
	jmp	.LBB2_427
.LBB2_269:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_434
# %bb.270:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_271:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_271
	jmp	.LBB2_435
.LBB2_272:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_442
# %bb.273:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_274:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_274
	jmp	.LBB2_443
.LBB2_275:
	mov	esi, r11d
	and	esi, -32
	vbroadcastss	ymm1, xmm0
	lea	rax, [rsi - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	test	rax, rax
	je	.LBB2_450
# %bb.276:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_277:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rbx + 224]
	vmovups	ymmword ptr [r8 + 4*rbx + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 224], ymm5
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_277
	jmp	.LBB2_451
.LBB2_278:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB2_458
# %bb.279:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_280:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB2_280
	jmp	.LBB2_459
.LBB2_281:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_466
# %bb.282:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_283:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_283
	jmp	.LBB2_467
.LBB2_284:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_658
# %bb.285:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_286:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_286
	jmp	.LBB2_659
.LBB2_287:
	mov	esi, r11d
	and	esi, -16
	vbroadcastsd	ymm1, xmm0
	lea	rax, [rsi - 16]
	mov	r10, rax
	shr	r10, 4
	add	r10, 1
	test	rax, rax
	je	.LBB2_666
# %bb.288:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_289:                              # =>This Inner Loop Header: Depth=1
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 64]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm5
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx + 128]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 160]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 192]
	vaddpd	ymm5, ymm1, ymmword ptr [rcx + 8*rbx + 224]
	vmovupd	ymmword ptr [r8 + 8*rbx + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 224], ymm5
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_289
	jmp	.LBB2_667
.LBB2_290:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB2_674
# %bb.291:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_292:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB2_292
	jmp	.LBB2_675
.LBB2_293:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_682
# %bb.294:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_295:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_295
	jmp	.LBB2_683
.LBB2_296:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_690
# %bb.297:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_298:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_298
	jmp	.LBB2_691
.LBB2_299:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_698
# %bb.300:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_301:                              # =>This Inner Loop Header: Depth=1
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpaddw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_301
	jmp	.LBB2_699
.LBB2_302:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_706
# %bb.303:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_304:                              # =>This Inner Loop Header: Depth=1
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpaddq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_304
	jmp	.LBB2_707
.LBB2_305:
	mov	esi, r11d
	and	esi, -32
	vbroadcastss	ymm1, xmm0
	lea	rax, [rsi - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	test	rax, rax
	je	.LBB2_714
# %bb.306:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_307:                              # =>This Inner Loop Header: Depth=1
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 64]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm5
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx + 128]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 160]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 192]
	vaddps	ymm5, ymm1, ymmword ptr [rcx + 4*rbx + 224]
	vmovups	ymmword ptr [r8 + 4*rbx + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 224], ymm5
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_307
	jmp	.LBB2_715
.LBB2_308:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB2_722
# %bb.309:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_310:                              # =>This Inner Loop Header: Depth=1
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpaddb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB2_310
	jmp	.LBB2_723
.LBB2_311:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_730
# %bb.312:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_313:                              # =>This Inner Loop Header: Depth=1
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpaddd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_313
	jmp	.LBB2_731
.LBB2_314:
	xor	ebx, ebx
.LBB2_315:
	test	r9b, 1
	je	.LBB2_317
# %bb.316:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rdi]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rdi + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rdi + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rdi + 96]
	vmovupd	ymmword ptr [r8 + 8*rdi], ymm2
	vmovupd	ymmword ptr [r8 + 8*rdi + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rdi + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rdi + 96], ymm1
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
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rdi]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rdi + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rdi + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rdi + 96]
	vmovups	ymmword ptr [r8 + 4*rdi], ymm2
	vmovups	ymmword ptr [r8 + 4*rdi + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rdi + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rdi + 96], ymm1
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
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	cmp	r9d, 32
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
	vmovsd	xmm0, qword ptr [rdx]           # xmm0 = mem[0],zero
	mov	r11d, r9d
	cmp	r9d, 16
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
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_591
.LBB2_592:
	cmp	rax, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_593:                              # =>This Inner Loop Header: Depth=1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rsi]
	vmovsd	qword ptr [r8 + 8*rsi], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 8]
	vmovsd	qword ptr [r8 + 8*rsi + 8], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 16]
	vmovsd	qword ptr [r8 + 8*rsi + 16], xmm1
	vsubsd	xmm1, xmm0, qword ptr [rcx + 8*rsi + 24]
	vmovsd	qword ptr [r8 + 8*rsi + 24], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 16
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB2_616
	.p2align	4, 0x90
.LBB2_615:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, r14d
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 32
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
	mov	rax, r10
	and	rax, 3
	je	.LBB2_624
	.p2align	4, 0x90
.LBB2_623:                              # =>This Inner Loop Header: Depth=1
	mov	ebx, r14d
	sub	bx, word ptr [rcx + 2*rsi]
	mov	word ptr [r8 + 2*rsi], bx
	add	rsi, 1
	add	rax, -1
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
	cmp	r9d, 16
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
	vmovss	xmm0, dword ptr [rdx]           # xmm0 = mem[0],zero,zero,zero
	mov	r11d, r9d
	cmp	r9d, 32
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
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	add	rsi, 1
	add	rbx, -1
	jne	.LBB2_639
.LBB2_640:
	cmp	rax, 3
	jb	.LBB2_13
	.p2align	4, 0x90
.LBB2_641:                              # =>This Inner Loop Header: Depth=1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rsi]
	vmovss	dword ptr [r8 + 4*rsi], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 4]
	vmovss	dword ptr [r8 + 4*rsi + 4], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 8]
	vmovss	dword ptr [r8 + 4*rsi + 8], xmm1
	vsubss	xmm1, xmm0, dword ptr [rcx + 4*rsi + 12]
	vmovss	dword ptr [r8 + 4*rsi + 12], xmm1
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
	cmp	r9d, 128
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
	cmp	r9d, 32
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
	vzeroupper
	ret
.LBB2_548:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_578
# %bb.549:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_550:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_550
	jmp	.LBB2_579
.LBB2_551:
	mov	esi, r11d
	and	esi, -16
	vbroadcastsd	ymm1, xmm0
	lea	rax, [rsi - 16]
	mov	r10, rax
	shr	r10, 4
	add	r10, 1
	test	rax, rax
	je	.LBB2_586
# %bb.552:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_553:                              # =>This Inner Loop Header: Depth=1
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 64]
	vsubpd	ymm5, ymm1, ymmword ptr [rcx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm5
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx + 128]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 160]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 192]
	vsubpd	ymm5, ymm1, ymmword ptr [rcx + 8*rbx + 224]
	vmovupd	ymmword ptr [r8 + 8*rbx + 128], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 160], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 192], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 224], ymm5
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_553
	jmp	.LBB2_587
.LBB2_554:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB2_594
# %bb.555:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_556:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB2_556
	jmp	.LBB2_595
.LBB2_557:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_602
# %bb.558:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_559:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_559
	jmp	.LBB2_603
.LBB2_560:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_610
# %bb.561:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_562:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_562
	jmp	.LBB2_611
.LBB2_563:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastw	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_618
# %bb.564:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_565:                              # =>This Inner Loop Header: Depth=1
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm2
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx + 64]
	vpsubw	ymm2, ymm0, ymmword ptr [rcx + 2*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 2*rbx + 64], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 96], ymm2
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_565
	jmp	.LBB2_619
.LBB2_566:
	mov	esi, r10d
	and	esi, -16
	vmovq	xmm0, r14
	vpbroadcastq	ymm0, xmm0
	lea	rax, [rsi - 16]
	mov	r11, rax
	shr	r11, 4
	add	r11, 1
	test	rax, rax
	je	.LBB2_626
# %bb.567:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_568:                              # =>This Inner Loop Header: Depth=1
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm4
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx + 128]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 160]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 192]
	vpsubq	ymm4, ymm0, ymmword ptr [rcx + 8*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 8*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 224], ymm4
	add	rbx, 32
	add	rax, 2
	jne	.LBB2_568
	jmp	.LBB2_627
.LBB2_569:
	mov	esi, r11d
	and	esi, -32
	vbroadcastss	ymm1, xmm0
	lea	rax, [rsi - 32]
	mov	r10, rax
	shr	r10, 5
	add	r10, 1
	test	rax, rax
	je	.LBB2_634
# %bb.570:
	mov	rax, r10
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_571:                              # =>This Inner Loop Header: Depth=1
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 64]
	vsubps	ymm5, ymm1, ymmword ptr [rcx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm5
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx + 128]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 160]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 192]
	vsubps	ymm5, ymm1, ymmword ptr [rcx + 4*rbx + 224]
	vmovups	ymmword ptr [r8 + 4*rbx + 128], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 160], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 192], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 224], ymm5
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_571
	jmp	.LBB2_635
.LBB2_572:
	mov	esi, r10d
	and	esi, -128
	vmovd	xmm0, r14d
	vpbroadcastb	ymm0, xmm0
	lea	rax, [rsi - 128]
	mov	r11, rax
	shr	r11, 7
	add	r11, 1
	test	rax, rax
	je	.LBB2_642
# %bb.573:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_574:                              # =>This Inner Loop Header: Depth=1
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm4
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx + 128]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 160]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 192]
	vpsubb	ymm4, ymm0, ymmword ptr [rcx + rbx + 224]
	vmovdqu	ymmword ptr [r8 + rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 224], ymm4
	add	rbx, 256
	add	rax, 2
	jne	.LBB2_574
	jmp	.LBB2_643
.LBB2_575:
	mov	esi, r10d
	and	esi, -32
	vmovd	xmm0, r14d
	vpbroadcastd	ymm0, xmm0
	lea	rax, [rsi - 32]
	mov	r11, rax
	shr	r11, 5
	add	r11, 1
	test	rax, rax
	je	.LBB2_650
# %bb.576:
	mov	rax, r11
	and	rax, -2
	neg	rax
	xor	ebx, ebx
.LBB2_577:                              # =>This Inner Loop Header: Depth=1
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm4
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx + 128]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 160]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 192]
	vpsubd	ymm4, ymm0, ymmword ptr [rcx + 4*rbx + 224]
	vmovdqu	ymmword ptr [r8 + 4*rbx + 128], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 160], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 192], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 224], ymm4
	add	rbx, 64
	add	rax, 2
	jne	.LBB2_577
	jmp	.LBB2_651
.LBB2_578:
	xor	ebx, ebx
.LBB2_579:
	test	r11b, 1
	je	.LBB2_581
# %bb.580:
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vsubpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx]
	vsubpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 32]
	vsubpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 64]
	vsubpd	ymm1, ymm1, ymmword ptr [rcx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm1
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
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpsubw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpsubw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpsubq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpsubq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpsubq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpsubq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vsubps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx]
	vsubps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 32]
	vsubps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 64]
	vsubps	ymm1, ymm1, ymmword ptr [rcx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm1
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
	vpsubb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpsubb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpsubb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpsubb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpsubd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpsubd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpsubd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpsubd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
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
	vaddpd	ymm2, ymm1, ymmword ptr [rcx + 8*rbx]
	vaddpd	ymm3, ymm1, ymmword ptr [rcx + 8*rbx + 32]
	vaddpd	ymm4, ymm1, ymmword ptr [rcx + 8*rbx + 64]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 8*rbx + 96]
	vmovupd	ymmword ptr [r8 + 8*rbx], ymm2
	vmovupd	ymmword ptr [r8 + 8*rbx + 32], ymm3
	vmovupd	ymmword ptr [r8 + 8*rbx + 64], ymm4
	vmovupd	ymmword ptr [r8 + 8*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddw	ymm1, ymm0, ymmword ptr [rcx + 2*rbx]
	vpaddw	ymm0, ymm0, ymmword ptr [rcx + 2*rbx + 32]
	vmovdqu	ymmword ptr [r8 + 2*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 2*rbx + 32], ymm0
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
	vpaddq	ymm1, ymm0, ymmword ptr [rcx + 8*rbx]
	vpaddq	ymm2, ymm0, ymmword ptr [rcx + 8*rbx + 32]
	vpaddq	ymm3, ymm0, ymmword ptr [rcx + 8*rbx + 64]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx + 8*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 8*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 8*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 8*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 8*rbx + 96], ymm0
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
	vaddps	ymm2, ymm1, ymmword ptr [rcx + 4*rbx]
	vaddps	ymm3, ymm1, ymmword ptr [rcx + 4*rbx + 32]
	vaddps	ymm4, ymm1, ymmword ptr [rcx + 4*rbx + 64]
	vaddps	ymm1, ymm1, ymmword ptr [rcx + 4*rbx + 96]
	vmovups	ymmword ptr [r8 + 4*rbx], ymm2
	vmovups	ymmword ptr [r8 + 4*rbx + 32], ymm3
	vmovups	ymmword ptr [r8 + 4*rbx + 64], ymm4
	vmovups	ymmword ptr [r8 + 4*rbx + 96], ymm1
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
	vpaddb	ymm1, ymm0, ymmword ptr [rcx + rbx]
	vpaddb	ymm2, ymm0, ymmword ptr [rcx + rbx + 32]
	vpaddb	ymm3, ymm0, ymmword ptr [rcx + rbx + 64]
	vpaddb	ymm0, ymm0, ymmword ptr [rcx + rbx + 96]
	vmovdqu	ymmword ptr [r8 + rbx], ymm1
	vmovdqu	ymmword ptr [r8 + rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + rbx + 96], ymm0
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
	vpaddd	ymm1, ymm0, ymmword ptr [rcx + 4*rbx]
	vpaddd	ymm2, ymm0, ymmword ptr [rcx + 4*rbx + 32]
	vpaddd	ymm3, ymm0, ymmword ptr [rcx + 4*rbx + 64]
	vpaddd	ymm0, ymm0, ymmword ptr [rcx + 4*rbx + 96]
	vmovdqu	ymmword ptr [r8 + 4*rbx], ymm1
	vmovdqu	ymmword ptr [r8 + 4*rbx + 32], ymm2
	vmovdqu	ymmword ptr [r8 + 4*rbx + 64], ymm3
	vmovdqu	ymmword ptr [r8 + 4*rbx + 96], ymm0
.LBB2_733:
	cmp	rsi, r10
	je	.LBB2_3
	jmp	.LBB2_734
.Lfunc_end2:
	.size	arithmetic_scalar_arr_avx2, .Lfunc_end2-arithmetic_scalar_arr_avx2
                                        # -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	.addrsig
