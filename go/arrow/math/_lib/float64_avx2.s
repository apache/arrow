	.text
	.intel_syntax noprefix
	.file	"_lib/float64.c"
	.globl	sum_float64_avx2
	.p2align	4, 0x90
	.type	sum_float64_avx2,@function
sum_float64_avx2:                       # @sum_float64_avx2
# BB#0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	vxorpd	xmm0, xmm0, xmm0
	test	rsi, rsi
	je	.LBB0_14
# BB#1:
	cmp	rsi, 31
	jbe	.LBB0_2
# BB#5:
	mov	r9, rsi
	and	r9, -32
	je	.LBB0_2
# BB#6:
	lea	r8, [r9 - 32]
	mov	eax, r8d
	shr	eax, 5
	inc	eax
	and	rax, 7
	je	.LBB0_7
# BB#8:
	neg	rax
	vxorpd	ymm0, ymm0, ymm0
	xor	ecx, ecx
	vxorpd	ymm1, ymm1, ymm1
	vxorpd	ymm2, ymm2, ymm2
	vxorpd	ymm3, ymm3, ymm3
	vxorpd	ymm4, ymm4, ymm4
	vxorpd	ymm5, ymm5, ymm5
	vxorpd	ymm6, ymm6, ymm6
	vxorpd	ymm7, ymm7, ymm7
	.p2align	4, 0x90
.LBB0_9:                                # =>This Inner Loop Header: Depth=1
	vaddpd	ymm0, ymm0, ymmword ptr [rdi + 8*rcx]
	vaddpd	ymm1, ymm1, ymmword ptr [rdi + 8*rcx + 32]
	vaddpd	ymm2, ymm2, ymmword ptr [rdi + 8*rcx + 64]
	vaddpd	ymm3, ymm3, ymmword ptr [rdi + 8*rcx + 96]
	vaddpd	ymm4, ymm4, ymmword ptr [rdi + 8*rcx + 128]
	vaddpd	ymm5, ymm5, ymmword ptr [rdi + 8*rcx + 160]
	vaddpd	ymm6, ymm6, ymmword ptr [rdi + 8*rcx + 192]
	vaddpd	ymm7, ymm7, ymmword ptr [rdi + 8*rcx + 224]
	add	rcx, 32
	inc	rax
	jne	.LBB0_9
	jmp	.LBB0_10
.LBB0_2:
	xor	r9d, r9d
.LBB0_3:
	lea	rax, [rdi + 8*r9]
	sub	rsi, r9
	.p2align	4, 0x90
.LBB0_4:                                # =>This Inner Loop Header: Depth=1
	vaddsd	xmm0, xmm0, qword ptr [rax]
	add	rax, 8
	dec	rsi
	jne	.LBB0_4
.LBB0_14:
	vmovsd	qword ptr [rdx], xmm0
	mov	rsp, rbp
	pop	rbp
	vzeroupper
	ret
.LBB0_7:
	xor	ecx, ecx
	vxorpd	ymm0, ymm0, ymm0
	vxorpd	ymm1, ymm1, ymm1
	vxorpd	ymm2, ymm2, ymm2
	vxorpd	ymm3, ymm3, ymm3
	vxorpd	ymm4, ymm4, ymm4
	vxorpd	ymm5, ymm5, ymm5
	vxorpd	ymm6, ymm6, ymm6
	vxorpd	ymm7, ymm7, ymm7
.LBB0_10:
	cmp	r8, 224
	jb	.LBB0_13
# BB#11:
	mov	rax, r9
	sub	rax, rcx
	lea	rcx, [rdi + 8*rcx + 1792]
	.p2align	4, 0x90
.LBB0_12:                               # =>This Inner Loop Header: Depth=1
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 1568]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 1600]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 1632]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 1664]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 1696]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 1728]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 1760]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 1792]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 1536]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 1504]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 1472]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 1440]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 1408]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 1376]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 1344]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 1312]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 1056]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 1088]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 1120]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 1152]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 1184]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 1216]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 1248]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 1280]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 1024]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 992]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 960]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 928]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 896]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 864]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 832]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 800]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 544]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 576]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 608]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 640]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 672]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 704]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 736]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 768]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 512]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 480]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 448]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 416]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 384]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 352]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 320]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 288]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx - 32]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx - 64]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx - 96]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx - 128]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx - 160]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx - 192]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx - 224]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx - 256]
	vaddpd	ymm0, ymm0, ymmword ptr [rcx]
	vaddpd	ymm1, ymm1, ymmword ptr [rcx + 32]
	vaddpd	ymm2, ymm2, ymmword ptr [rcx + 64]
	vaddpd	ymm3, ymm3, ymmword ptr [rcx + 96]
	vaddpd	ymm4, ymm4, ymmword ptr [rcx + 128]
	vaddpd	ymm5, ymm5, ymmword ptr [rcx + 160]
	vaddpd	ymm6, ymm6, ymmword ptr [rcx + 192]
	vaddpd	ymm7, ymm7, ymmword ptr [rcx + 224]
	add	rcx, 2048
	add	rax, -256
	jne	.LBB0_12
.LBB0_13:
	vaddpd	ymm1, ymm1, ymm5
	vaddpd	ymm3, ymm3, ymm7
	vaddpd	ymm0, ymm0, ymm4
	vaddpd	ymm2, ymm2, ymm6
	vaddpd	ymm0, ymm0, ymm2
	vaddpd	ymm1, ymm1, ymm3
	vaddpd	ymm0, ymm0, ymm1
	vextractf128	xmm1, ymm0, 1
	vaddpd	ymm0, ymm0, ymm1
	vhaddpd	ymm0, ymm0, ymm0
	cmp	r9, rsi
	jne	.LBB0_3
	jmp	.LBB0_14
.Lfunc_end0:
	.size	sum_float64_avx2, .Lfunc_end0-sum_float64_avx2


	.ident	"Apple LLVM version 9.0.0 (clang-900.0.39.2)"
	.section	".note.GNU-stack","",@progbits
