	.text
	.intel_syntax noprefix
	.file	"_lib/int64.c"
	.globl	sum_int64_avx2
	.p2align	4, 0x90
	.type	sum_int64_avx2,@function
sum_int64_avx2:                         # @sum_int64_avx2
# BB#0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	test	rsi, rsi
	je	.LBB0_1
# BB#2:
	cmp	rsi, 31
	jbe	.LBB0_3
# BB#6:
	mov	r9, rsi
	and	r9, -32
	je	.LBB0_3
# BB#7:
	lea	r8, [r9 - 32]
	mov	eax, r8d
	shr	eax, 5
	inc	eax
	and	rax, 7
	je	.LBB0_8
# BB#9:
	neg	rax
	vpxor	ymm0, ymm0, ymm0
	xor	ecx, ecx
	vpxor	ymm1, ymm1, ymm1
	vpxor	ymm2, ymm2, ymm2
	vpxor	ymm3, ymm3, ymm3
	vpxor	ymm4, ymm4, ymm4
	vpxor	ymm5, ymm5, ymm5
	vpxor	ymm6, ymm6, ymm6
	vpxor	ymm7, ymm7, ymm7
	.p2align	4, 0x90
.LBB0_10:                               # =>This Inner Loop Header: Depth=1
	vpaddq	ymm0, ymm0, ymmword ptr [rdi + 8*rcx]
	vpaddq	ymm1, ymm1, ymmword ptr [rdi + 8*rcx + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rdi + 8*rcx + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rdi + 8*rcx + 96]
	vpaddq	ymm4, ymm4, ymmword ptr [rdi + 8*rcx + 128]
	vpaddq	ymm5, ymm5, ymmword ptr [rdi + 8*rcx + 160]
	vpaddq	ymm6, ymm6, ymmword ptr [rdi + 8*rcx + 192]
	vpaddq	ymm7, ymm7, ymmword ptr [rdi + 8*rcx + 224]
	add	rcx, 32
	inc	rax
	jne	.LBB0_10
	jmp	.LBB0_11
.LBB0_3:
	xor	r9d, r9d
	xor	eax, eax
.LBB0_4:
	lea	rcx, [rdi + 8*r9]
	sub	rsi, r9
	.p2align	4, 0x90
.LBB0_5:                                # =>This Inner Loop Header: Depth=1
	add	rax, qword ptr [rcx]
	add	rcx, 8
	dec	rsi
	jne	.LBB0_5
	jmp	.LBB0_15
.LBB0_1:
	xor	eax, eax
.LBB0_15:
	mov	qword ptr [rdx], rax
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB0_8:
	xor	ecx, ecx
	vpxor	ymm0, ymm0, ymm0
	vpxor	ymm1, ymm1, ymm1
	vpxor	ymm2, ymm2, ymm2
	vpxor	ymm3, ymm3, ymm3
	vpxor	ymm4, ymm4, ymm4
	vpxor	ymm5, ymm5, ymm5
	vpxor	ymm6, ymm6, ymm6
	vpxor	ymm7, ymm7, ymm7
.LBB0_11:
	cmp	r8, 224
	jb	.LBB0_14
# BB#12:
	mov	rax, r9
	sub	rax, rcx
	lea	rcx, [rdi + 8*rcx + 1792]
	.p2align	4, 0x90
.LBB0_13:                               # =>This Inner Loop Header: Depth=1
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 1568]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 1600]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 1632]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 1664]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 1696]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 1728]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 1760]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 1792]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 1536]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 1504]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 1472]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 1440]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 1408]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 1376]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 1344]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 1312]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 1056]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 1088]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 1120]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 1152]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 1184]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 1216]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 1248]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 1280]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 1024]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 992]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 960]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 928]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 896]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 864]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 832]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 800]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 544]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 576]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 608]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 640]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 672]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 704]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 736]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 768]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 512]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 480]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 448]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 416]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 384]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 352]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 320]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 288]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx - 32]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx - 64]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx - 96]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx - 128]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx - 160]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx - 192]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx - 224]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx - 256]
	vpaddq	ymm0, ymm0, ymmword ptr [rcx]
	vpaddq	ymm1, ymm1, ymmword ptr [rcx + 32]
	vpaddq	ymm2, ymm2, ymmword ptr [rcx + 64]
	vpaddq	ymm3, ymm3, ymmword ptr [rcx + 96]
	vpaddq	ymm4, ymm4, ymmword ptr [rcx + 128]
	vpaddq	ymm5, ymm5, ymmword ptr [rcx + 160]
	vpaddq	ymm6, ymm6, ymmword ptr [rcx + 192]
	vpaddq	ymm7, ymm7, ymmword ptr [rcx + 224]
	add	rcx, 2048
	add	rax, -256
	jne	.LBB0_13
.LBB0_14:
	vpaddq	ymm1, ymm1, ymm5
	vpaddq	ymm3, ymm3, ymm7
	vpaddq	ymm0, ymm0, ymm4
	vpaddq	ymm2, ymm2, ymm6
	vpaddq	ymm0, ymm0, ymm2
	vpaddq	ymm1, ymm1, ymm3
	vpaddq	ymm0, ymm0, ymm1
	vextracti128	xmm1, ymm0, 1
	vpaddq	ymm0, ymm0, ymm1
	vpshufd	xmm1, xmm0, 78          # xmm1 = xmm0[2,3,0,1]
	vpaddq	ymm0, ymm0, ymm1
	vmovq	rax, xmm0
	cmp	r9, rsi
	jne	.LBB0_4
	jmp	.LBB0_15
.Lfunc_end0:
	.size	sum_int64_avx2, .Lfunc_end0-sum_int64_avx2


	.ident	"Apple LLVM version 9.0.0 (clang-900.0.39.2)"
	.section	".note.GNU-stack","",@progbits
