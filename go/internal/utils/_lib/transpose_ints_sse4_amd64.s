	.text
	.intel_syntax noprefix
	.file	"transpose_ints.c"
	.globl	transpose_uint8_uint8_sse4      # -- Begin function transpose_uint8_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_uint8_sse4,@function
transpose_uint8_uint8_sse4:             # @transpose_uint8_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB0_1
	.p2align	4, 0x90
.LBB0_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movzx	edx, byte ptr [rdi + 1]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movzx	edx, byte ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movzx	edx, byte ptr [rdi + 3]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB0_5
.LBB0_1:
	test	edx, edx
	jle	.LBB0_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB0_3:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB0_3
.LBB0_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end0:
	.size	transpose_uint8_uint8_sse4, .Lfunc_end0-transpose_uint8_uint8_sse4
                                        # -- End function
	.globl	transpose_int8_uint8_sse4       # -- Begin function transpose_int8_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_int8_uint8_sse4,@function
transpose_int8_uint8_sse4:              # @transpose_int8_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB1_1
	.p2align	4, 0x90
.LBB1_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movsx	rdx, byte ptr [rdi + 1]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movsx	rdx, byte ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movsx	rdx, byte ptr [rdi + 3]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB1_5
.LBB1_1:
	test	edx, edx
	jle	.LBB1_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB1_3:                                # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB1_3
.LBB1_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end1:
	.size	transpose_int8_uint8_sse4, .Lfunc_end1-transpose_int8_uint8_sse4
                                        # -- End function
	.globl	transpose_uint16_uint8_sse4     # -- Begin function transpose_uint16_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_uint8_sse4,@function
transpose_uint16_uint8_sse4:            # @transpose_uint16_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB2_1
	.p2align	4, 0x90
.LBB2_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movzx	edx, word ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movzx	edx, word ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movzx	edx, word ptr [rdi + 6]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB2_5
.LBB2_1:
	test	edx, edx
	jle	.LBB2_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB2_3:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB2_3
.LBB2_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end2:
	.size	transpose_uint16_uint8_sse4, .Lfunc_end2-transpose_uint16_uint8_sse4
                                        # -- End function
	.globl	transpose_int16_uint8_sse4      # -- Begin function transpose_int16_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_int16_uint8_sse4,@function
transpose_int16_uint8_sse4:             # @transpose_int16_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB3_1
	.p2align	4, 0x90
.LBB3_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movsx	rdx, word ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movsx	rdx, word ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movsx	rdx, word ptr [rdi + 6]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB3_5
.LBB3_1:
	test	edx, edx
	jle	.LBB3_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB3_3:                                # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + 2*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB3_3
.LBB3_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end3:
	.size	transpose_int16_uint8_sse4, .Lfunc_end3-transpose_int16_uint8_sse4
                                        # -- End function
	.globl	transpose_uint32_uint8_sse4     # -- Begin function transpose_uint32_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_uint8_sse4,@function
transpose_uint32_uint8_sse4:            # @transpose_uint32_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB4_1
	.p2align	4, 0x90
.LBB4_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	mov	edx, dword ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	mov	edx, dword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	mov	edx, dword ptr [rdi + 12]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB4_5
.LBB4_1:
	test	edx, edx
	jle	.LBB4_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB4_3:                                # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB4_3
.LBB4_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end4:
	.size	transpose_uint32_uint8_sse4, .Lfunc_end4-transpose_uint32_uint8_sse4
                                        # -- End function
	.globl	transpose_int32_uint8_sse4      # -- Begin function transpose_int32_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_int32_uint8_sse4,@function
transpose_int32_uint8_sse4:             # @transpose_int32_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB5_1
	.p2align	4, 0x90
.LBB5_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movsxd	rdx, dword ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movsxd	rdx, dword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movsxd	rdx, dword ptr [rdi + 12]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB5_5
.LBB5_1:
	test	edx, edx
	jle	.LBB5_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB5_3:                                # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB5_3
.LBB5_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end5:
	.size	transpose_int32_uint8_sse4, .Lfunc_end5-transpose_int32_uint8_sse4
                                        # -- End function
	.globl	transpose_uint64_uint8_sse4     # -- Begin function transpose_uint64_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_uint8_sse4,@function
transpose_uint64_uint8_sse4:            # @transpose_uint64_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB6_1
	.p2align	4, 0x90
.LBB6_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB6_5
.LBB6_1:
	test	edx, edx
	jle	.LBB6_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB6_3:                                # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB6_3
.LBB6_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end6:
	.size	transpose_uint64_uint8_sse4, .Lfunc_end6-transpose_uint64_uint8_sse4
                                        # -- End function
	.globl	transpose_int64_uint8_sse4      # -- Begin function transpose_int64_uint8_sse4
	.p2align	4, 0x90
	.type	transpose_int64_uint8_sse4,@function
transpose_int64_uint8_sse4:             # @transpose_int64_uint8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB7_1
	.p2align	4, 0x90
.LBB7_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB7_5
.LBB7_1:
	test	edx, edx
	jle	.LBB7_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB7_3:                                # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB7_3
.LBB7_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end7:
	.size	transpose_int64_uint8_sse4, .Lfunc_end7-transpose_int64_uint8_sse4
                                        # -- End function
	.globl	transpose_uint8_int8_sse4       # -- Begin function transpose_uint8_int8_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_int8_sse4,@function
transpose_uint8_int8_sse4:              # @transpose_uint8_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB8_1
	.p2align	4, 0x90
.LBB8_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movzx	edx, byte ptr [rdi + 1]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movzx	edx, byte ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movzx	edx, byte ptr [rdi + 3]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB8_5
.LBB8_1:
	test	edx, edx
	jle	.LBB8_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB8_3:                                # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB8_3
.LBB8_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end8:
	.size	transpose_uint8_int8_sse4, .Lfunc_end8-transpose_uint8_int8_sse4
                                        # -- End function
	.globl	transpose_int8_int8_sse4        # -- Begin function transpose_int8_int8_sse4
	.p2align	4, 0x90
	.type	transpose_int8_int8_sse4,@function
transpose_int8_int8_sse4:               # @transpose_int8_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB9_1
	.p2align	4, 0x90
.LBB9_5:                                # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movsx	rdx, byte ptr [rdi + 1]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movsx	rdx, byte ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movsx	rdx, byte ptr [rdi + 3]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB9_5
.LBB9_1:
	test	edx, edx
	jle	.LBB9_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB9_3:                                # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB9_3
.LBB9_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end9:
	.size	transpose_int8_int8_sse4, .Lfunc_end9-transpose_int8_int8_sse4
                                        # -- End function
	.globl	transpose_uint16_int8_sse4      # -- Begin function transpose_uint16_int8_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_int8_sse4,@function
transpose_uint16_int8_sse4:             # @transpose_uint16_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB10_1
	.p2align	4, 0x90
.LBB10_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movzx	edx, word ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movzx	edx, word ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movzx	edx, word ptr [rdi + 6]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB10_5
.LBB10_1:
	test	edx, edx
	jle	.LBB10_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB10_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + 2*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB10_3
.LBB10_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end10:
	.size	transpose_uint16_int8_sse4, .Lfunc_end10-transpose_uint16_int8_sse4
                                        # -- End function
	.globl	transpose_int16_int8_sse4       # -- Begin function transpose_int16_int8_sse4
	.p2align	4, 0x90
	.type	transpose_int16_int8_sse4,@function
transpose_int16_int8_sse4:              # @transpose_int16_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB11_1
	.p2align	4, 0x90
.LBB11_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movsx	rdx, word ptr [rdi + 2]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movsx	rdx, word ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movsx	rdx, word ptr [rdi + 6]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB11_5
.LBB11_1:
	test	edx, edx
	jle	.LBB11_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB11_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + 2*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB11_3
.LBB11_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end11:
	.size	transpose_int16_int8_sse4, .Lfunc_end11-transpose_int16_int8_sse4
                                        # -- End function
	.globl	transpose_uint32_int8_sse4      # -- Begin function transpose_uint32_int8_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_int8_sse4,@function
transpose_uint32_int8_sse4:             # @transpose_uint32_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB12_1
	.p2align	4, 0x90
.LBB12_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	mov	edx, dword ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	mov	edx, dword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	mov	edx, dword ptr [rdi + 12]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB12_5
.LBB12_1:
	test	edx, edx
	jle	.LBB12_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB12_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 4*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB12_3
.LBB12_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end12:
	.size	transpose_uint32_int8_sse4, .Lfunc_end12-transpose_uint32_int8_sse4
                                        # -- End function
	.globl	transpose_int32_int8_sse4       # -- Begin function transpose_int32_int8_sse4
	.p2align	4, 0x90
	.type	transpose_int32_int8_sse4,@function
transpose_int32_int8_sse4:              # @transpose_int32_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB13_1
	.p2align	4, 0x90
.LBB13_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	movsxd	rdx, dword ptr [rdi + 4]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	movsxd	rdx, dword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	movsxd	rdx, dword ptr [rdi + 12]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB13_5
.LBB13_1:
	test	edx, edx
	jle	.LBB13_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB13_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 4*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB13_3
.LBB13_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end13:
	.size	transpose_int32_int8_sse4, .Lfunc_end13-transpose_int32_int8_sse4
                                        # -- End function
	.globl	transpose_uint64_int8_sse4      # -- Begin function transpose_uint64_int8_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_int8_sse4,@function
transpose_uint64_int8_sse4:             # @transpose_uint64_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB14_1
	.p2align	4, 0x90
.LBB14_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB14_5
.LBB14_1:
	test	edx, edx
	jle	.LBB14_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB14_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB14_3
.LBB14_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end14:
	.size	transpose_uint64_int8_sse4, .Lfunc_end14-transpose_uint64_int8_sse4
                                        # -- End function
	.globl	transpose_int64_int8_sse4       # -- Begin function transpose_int64_int8_sse4
	.p2align	4, 0x90
	.type	transpose_int64_int8_sse4,@function
transpose_int64_int8_sse4:              # @transpose_int64_int8_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB15_1
	.p2align	4, 0x90
.LBB15_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi], dl
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 1], dl
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 2], dl
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, byte ptr [rcx + 4*rdx]
	mov	byte ptr [rsi + 3], dl
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 4
	cmp	eax, 7
	jg	.LBB15_5
.LBB15_1:
	test	edx, edx
	jle	.LBB15_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB15_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 8*r8]
	movzx	eax, byte ptr [rcx + 4*rax]
	mov	byte ptr [rsi + r8], al
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB15_3
.LBB15_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end15:
	.size	transpose_int64_int8_sse4, .Lfunc_end15-transpose_int64_int8_sse4
                                        # -- End function
	.globl	transpose_uint8_uint16_sse4     # -- Begin function transpose_uint8_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_uint16_sse4,@function
transpose_uint8_uint16_sse4:            # @transpose_uint8_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB16_1
	.p2align	4, 0x90
.LBB16_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movzx	edx, byte ptr [rdi + 1]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movzx	edx, byte ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movzx	edx, byte ptr [rdi + 3]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB16_5
.LBB16_1:
	test	edx, edx
	jle	.LBB16_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB16_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + 2*r8], ax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB16_3
.LBB16_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end16:
	.size	transpose_uint8_uint16_sse4, .Lfunc_end16-transpose_uint8_uint16_sse4
                                        # -- End function
	.globl	transpose_int8_uint16_sse4      # -- Begin function transpose_int8_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_int8_uint16_sse4,@function
transpose_int8_uint16_sse4:             # @transpose_int8_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB17_1
	.p2align	4, 0x90
.LBB17_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movsx	rdx, byte ptr [rdi + 1]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movsx	rdx, byte ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movsx	rdx, byte ptr [rdi + 3]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB17_5
.LBB17_1:
	test	edx, edx
	jle	.LBB17_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB17_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + 2*r8], ax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB17_3
.LBB17_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end17:
	.size	transpose_int8_uint16_sse4, .Lfunc_end17-transpose_int8_uint16_sse4
                                        # -- End function
	.globl	transpose_uint16_uint16_sse4    # -- Begin function transpose_uint16_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_uint16_sse4,@function
transpose_uint16_uint16_sse4:           # @transpose_uint16_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB18_1
	.p2align	4, 0x90
.LBB18_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movzx	edx, word ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movzx	edx, word ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movzx	edx, word ptr [rdi + 6]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB18_5
.LBB18_1:
	test	edx, edx
	jle	.LBB18_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB18_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB18_3
.LBB18_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end18:
	.size	transpose_uint16_uint16_sse4, .Lfunc_end18-transpose_uint16_uint16_sse4
                                        # -- End function
	.globl	transpose_int16_uint16_sse4     # -- Begin function transpose_int16_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_int16_uint16_sse4,@function
transpose_int16_uint16_sse4:            # @transpose_int16_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB19_1
	.p2align	4, 0x90
.LBB19_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movsx	rdx, word ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movsx	rdx, word ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movsx	rdx, word ptr [rdi + 6]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB19_5
.LBB19_1:
	test	edx, edx
	jle	.LBB19_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB19_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB19_3
.LBB19_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end19:
	.size	transpose_int16_uint16_sse4, .Lfunc_end19-transpose_int16_uint16_sse4
                                        # -- End function
	.globl	transpose_uint32_uint16_sse4    # -- Begin function transpose_uint32_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_uint16_sse4,@function
transpose_uint32_uint16_sse4:           # @transpose_uint32_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB20_1
	.p2align	4, 0x90
.LBB20_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	mov	edx, dword ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	mov	edx, dword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	mov	edx, dword ptr [rdi + 12]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB20_5
.LBB20_1:
	test	edx, edx
	jle	.LBB20_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB20_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 2*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB20_3
.LBB20_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end20:
	.size	transpose_uint32_uint16_sse4, .Lfunc_end20-transpose_uint32_uint16_sse4
                                        # -- End function
	.globl	transpose_int32_uint16_sse4     # -- Begin function transpose_int32_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_int32_uint16_sse4,@function
transpose_int32_uint16_sse4:            # @transpose_int32_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB21_1
	.p2align	4, 0x90
.LBB21_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movsxd	rdx, dword ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movsxd	rdx, dword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movsxd	rdx, dword ptr [rdi + 12]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB21_5
.LBB21_1:
	test	edx, edx
	jle	.LBB21_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB21_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 2*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB21_3
.LBB21_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end21:
	.size	transpose_int32_uint16_sse4, .Lfunc_end21-transpose_int32_uint16_sse4
                                        # -- End function
	.globl	transpose_uint64_uint16_sse4    # -- Begin function transpose_uint64_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_uint16_sse4,@function
transpose_uint64_uint16_sse4:           # @transpose_uint64_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB22_1
	.p2align	4, 0x90
.LBB22_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB22_5
.LBB22_1:
	test	edx, edx
	jle	.LBB22_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB22_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 4*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB22_3
.LBB22_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end22:
	.size	transpose_uint64_uint16_sse4, .Lfunc_end22-transpose_uint64_uint16_sse4
                                        # -- End function
	.globl	transpose_int64_uint16_sse4     # -- Begin function transpose_int64_uint16_sse4
	.p2align	4, 0x90
	.type	transpose_int64_uint16_sse4,@function
transpose_int64_uint16_sse4:            # @transpose_int64_uint16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB23_1
	.p2align	4, 0x90
.LBB23_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB23_5
.LBB23_1:
	test	edx, edx
	jle	.LBB23_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB23_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 4*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB23_3
.LBB23_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end23:
	.size	transpose_int64_uint16_sse4, .Lfunc_end23-transpose_int64_uint16_sse4
                                        # -- End function
	.globl	transpose_uint8_int16_sse4      # -- Begin function transpose_uint8_int16_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_int16_sse4,@function
transpose_uint8_int16_sse4:             # @transpose_uint8_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB24_1
	.p2align	4, 0x90
.LBB24_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movzx	edx, byte ptr [rdi + 1]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movzx	edx, byte ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movzx	edx, byte ptr [rdi + 3]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB24_5
.LBB24_1:
	test	edx, edx
	jle	.LBB24_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB24_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + 2*r8], ax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB24_3
.LBB24_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end24:
	.size	transpose_uint8_int16_sse4, .Lfunc_end24-transpose_uint8_int16_sse4
                                        # -- End function
	.globl	transpose_int8_int16_sse4       # -- Begin function transpose_int8_int16_sse4
	.p2align	4, 0x90
	.type	transpose_int8_int16_sse4,@function
transpose_int8_int16_sse4:              # @transpose_int8_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB25_1
	.p2align	4, 0x90
.LBB25_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movsx	rdx, byte ptr [rdi + 1]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movsx	rdx, byte ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movsx	rdx, byte ptr [rdi + 3]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB25_5
.LBB25_1:
	test	edx, edx
	jle	.LBB25_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB25_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + 2*r8], ax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB25_3
.LBB25_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end25:
	.size	transpose_int8_int16_sse4, .Lfunc_end25-transpose_int8_int16_sse4
                                        # -- End function
	.globl	transpose_uint16_int16_sse4     # -- Begin function transpose_uint16_int16_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_int16_sse4,@function
transpose_uint16_int16_sse4:            # @transpose_uint16_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB26_1
	.p2align	4, 0x90
.LBB26_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movzx	edx, word ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movzx	edx, word ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movzx	edx, word ptr [rdi + 6]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB26_5
.LBB26_1:
	test	edx, edx
	jle	.LBB26_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB26_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB26_3
.LBB26_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end26:
	.size	transpose_uint16_int16_sse4, .Lfunc_end26-transpose_uint16_int16_sse4
                                        # -- End function
	.globl	transpose_int16_int16_sse4      # -- Begin function transpose_int16_int16_sse4
	.p2align	4, 0x90
	.type	transpose_int16_int16_sse4,@function
transpose_int16_int16_sse4:             # @transpose_int16_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB27_1
	.p2align	4, 0x90
.LBB27_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movsx	rdx, word ptr [rdi + 2]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movsx	rdx, word ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movsx	rdx, word ptr [rdi + 6]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB27_5
.LBB27_1:
	test	edx, edx
	jle	.LBB27_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB27_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB27_3
.LBB27_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end27:
	.size	transpose_int16_int16_sse4, .Lfunc_end27-transpose_int16_int16_sse4
                                        # -- End function
	.globl	transpose_uint32_int16_sse4     # -- Begin function transpose_uint32_int16_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_int16_sse4,@function
transpose_uint32_int16_sse4:            # @transpose_uint32_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB28_1
	.p2align	4, 0x90
.LBB28_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	mov	edx, dword ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	mov	edx, dword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	mov	edx, dword ptr [rdi + 12]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB28_5
.LBB28_1:
	test	edx, edx
	jle	.LBB28_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB28_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + 2*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB28_3
.LBB28_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end28:
	.size	transpose_uint32_int16_sse4, .Lfunc_end28-transpose_uint32_int16_sse4
                                        # -- End function
	.globl	transpose_int32_int16_sse4      # -- Begin function transpose_int32_int16_sse4
	.p2align	4, 0x90
	.type	transpose_int32_int16_sse4,@function
transpose_int32_int16_sse4:             # @transpose_int32_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB29_1
	.p2align	4, 0x90
.LBB29_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	movsxd	rdx, dword ptr [rdi + 4]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	movsxd	rdx, dword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	movsxd	rdx, dword ptr [rdi + 12]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB29_5
.LBB29_1:
	test	edx, edx
	jle	.LBB29_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB29_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + 2*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB29_3
.LBB29_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end29:
	.size	transpose_int32_int16_sse4, .Lfunc_end29-transpose_int32_int16_sse4
                                        # -- End function
	.globl	transpose_uint64_int16_sse4     # -- Begin function transpose_uint64_int16_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_int16_sse4,@function
transpose_uint64_int16_sse4:            # @transpose_uint64_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB30_1
	.p2align	4, 0x90
.LBB30_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB30_5
.LBB30_1:
	test	edx, edx
	jle	.LBB30_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB30_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 4*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB30_3
.LBB30_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end30:
	.size	transpose_uint64_int16_sse4, .Lfunc_end30-transpose_uint64_int16_sse4
                                        # -- End function
	.globl	transpose_int64_int16_sse4      # -- Begin function transpose_int64_int16_sse4
	.p2align	4, 0x90
	.type	transpose_int64_int16_sse4,@function
transpose_int64_int16_sse4:             # @transpose_int64_int16_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB31_1
	.p2align	4, 0x90
.LBB31_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi], dx
	mov	rdx, qword ptr [rdi + 8]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 2], dx
	mov	rdx, qword ptr [rdi + 16]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 4], dx
	mov	rdx, qword ptr [rdi + 24]
	movzx	edx, word ptr [rcx + 4*rdx]
	mov	word ptr [rsi + 6], dx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 8
	cmp	eax, 7
	jg	.LBB31_5
.LBB31_1:
	test	edx, edx
	jle	.LBB31_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB31_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 4*r8]
	movzx	eax, word ptr [rcx + 4*rax]
	mov	word ptr [rsi + r8], ax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB31_3
.LBB31_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end31:
	.size	transpose_int64_int16_sse4, .Lfunc_end31-transpose_int64_int16_sse4
                                        # -- End function
	.globl	transpose_uint8_uint32_sse4     # -- Begin function transpose_uint8_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_uint32_sse4,@function
transpose_uint8_uint32_sse4:            # @transpose_uint8_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB32_1
	.p2align	4, 0x90
.LBB32_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movzx	edx, byte ptr [rdi + 1]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movzx	edx, byte ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movzx	edx, byte ptr [rdi + 3]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB32_5
.LBB32_1:
	test	edx, edx
	jle	.LBB32_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB32_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 4*r8], eax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB32_3
.LBB32_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end32:
	.size	transpose_uint8_uint32_sse4, .Lfunc_end32-transpose_uint8_uint32_sse4
                                        # -- End function
	.globl	transpose_int8_uint32_sse4      # -- Begin function transpose_int8_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_int8_uint32_sse4,@function
transpose_int8_uint32_sse4:             # @transpose_int8_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB33_1
	.p2align	4, 0x90
.LBB33_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movsx	rdx, byte ptr [rdi + 1]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movsx	rdx, byte ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movsx	rdx, byte ptr [rdi + 3]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB33_5
.LBB33_1:
	test	edx, edx
	jle	.LBB33_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB33_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 4*r8], eax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB33_3
.LBB33_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end33:
	.size	transpose_int8_uint32_sse4, .Lfunc_end33-transpose_int8_uint32_sse4
                                        # -- End function
	.globl	transpose_uint16_uint32_sse4    # -- Begin function transpose_uint16_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_uint32_sse4,@function
transpose_uint16_uint32_sse4:           # @transpose_uint16_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB34_1
	.p2align	4, 0x90
.LBB34_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movzx	edx, word ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movzx	edx, word ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movzx	edx, word ptr [rdi + 6]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB34_5
.LBB34_1:
	test	edx, edx
	jle	.LBB34_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB34_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 2*r8], eax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB34_3
.LBB34_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end34:
	.size	transpose_uint16_uint32_sse4, .Lfunc_end34-transpose_uint16_uint32_sse4
                                        # -- End function
	.globl	transpose_int16_uint32_sse4     # -- Begin function transpose_int16_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_int16_uint32_sse4,@function
transpose_int16_uint32_sse4:            # @transpose_int16_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB35_1
	.p2align	4, 0x90
.LBB35_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movsx	rdx, word ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movsx	rdx, word ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movsx	rdx, word ptr [rdi + 6]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB35_5
.LBB35_1:
	test	edx, edx
	jle	.LBB35_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB35_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 2*r8], eax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB35_3
.LBB35_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end35:
	.size	transpose_int16_uint32_sse4, .Lfunc_end35-transpose_int16_uint32_sse4
                                        # -- End function
	.globl	transpose_uint32_uint32_sse4    # -- Begin function transpose_uint32_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_uint32_sse4,@function
transpose_uint32_uint32_sse4:           # @transpose_uint32_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB36_1
	.p2align	4, 0x90
.LBB36_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	mov	edx, dword ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	mov	edx, dword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	mov	edx, dword ptr [rdi + 12]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB36_5
.LBB36_1:
	test	edx, edx
	jle	.LBB36_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB36_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB36_3
.LBB36_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end36:
	.size	transpose_uint32_uint32_sse4, .Lfunc_end36-transpose_uint32_uint32_sse4
                                        # -- End function
	.globl	transpose_int32_uint32_sse4     # -- Begin function transpose_int32_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_int32_uint32_sse4,@function
transpose_int32_uint32_sse4:            # @transpose_int32_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB37_1
	.p2align	4, 0x90
.LBB37_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movsxd	rdx, dword ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movsxd	rdx, dword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movsxd	rdx, dword ptr [rdi + 12]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB37_5
.LBB37_1:
	test	edx, edx
	jle	.LBB37_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB37_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB37_3
.LBB37_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end37:
	.size	transpose_int32_uint32_sse4, .Lfunc_end37-transpose_int32_uint32_sse4
                                        # -- End function
	.globl	transpose_uint64_uint32_sse4    # -- Begin function transpose_uint64_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_uint32_sse4,@function
transpose_uint64_uint32_sse4:           # @transpose_uint64_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB38_1
	.p2align	4, 0x90
.LBB38_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	mov	rdx, qword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	mov	rdx, qword ptr [rdi + 16]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	mov	rdx, qword ptr [rdi + 24]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB38_5
.LBB38_1:
	test	edx, edx
	jle	.LBB38_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB38_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 2*r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB38_3
.LBB38_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end38:
	.size	transpose_uint64_uint32_sse4, .Lfunc_end38-transpose_uint64_uint32_sse4
                                        # -- End function
	.globl	transpose_int64_uint32_sse4     # -- Begin function transpose_int64_uint32_sse4
	.p2align	4, 0x90
	.type	transpose_int64_uint32_sse4,@function
transpose_int64_uint32_sse4:            # @transpose_int64_uint32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB39_1
	.p2align	4, 0x90
.LBB39_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	mov	rdx, qword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	mov	rdx, qword ptr [rdi + 16]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	mov	rdx, qword ptr [rdi + 24]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB39_5
.LBB39_1:
	test	edx, edx
	jle	.LBB39_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB39_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 2*r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB39_3
.LBB39_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end39:
	.size	transpose_int64_uint32_sse4, .Lfunc_end39-transpose_int64_uint32_sse4
                                        # -- End function
	.globl	transpose_uint8_int32_sse4      # -- Begin function transpose_uint8_int32_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_int32_sse4,@function
transpose_uint8_int32_sse4:             # @transpose_uint8_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB40_1
	.p2align	4, 0x90
.LBB40_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movzx	edx, byte ptr [rdi + 1]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movzx	edx, byte ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movzx	edx, byte ptr [rdi + 3]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB40_5
.LBB40_1:
	test	edx, edx
	jle	.LBB40_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB40_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 4*r8], eax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB40_3
.LBB40_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end40:
	.size	transpose_uint8_int32_sse4, .Lfunc_end40-transpose_uint8_int32_sse4
                                        # -- End function
	.globl	transpose_int8_int32_sse4       # -- Begin function transpose_int8_int32_sse4
	.p2align	4, 0x90
	.type	transpose_int8_int32_sse4,@function
transpose_int8_int32_sse4:              # @transpose_int8_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB41_1
	.p2align	4, 0x90
.LBB41_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movsx	rdx, byte ptr [rdi + 1]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movsx	rdx, byte ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movsx	rdx, byte ptr [rdi + 3]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB41_5
.LBB41_1:
	test	edx, edx
	jle	.LBB41_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB41_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 4*r8], eax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB41_3
.LBB41_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end41:
	.size	transpose_int8_int32_sse4, .Lfunc_end41-transpose_int8_int32_sse4
                                        # -- End function
	.globl	transpose_uint16_int32_sse4     # -- Begin function transpose_uint16_int32_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_int32_sse4,@function
transpose_uint16_int32_sse4:            # @transpose_uint16_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB42_1
	.p2align	4, 0x90
.LBB42_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movzx	edx, word ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movzx	edx, word ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movzx	edx, word ptr [rdi + 6]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB42_5
.LBB42_1:
	test	edx, edx
	jle	.LBB42_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB42_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 2*r8], eax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB42_3
.LBB42_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end42:
	.size	transpose_uint16_int32_sse4, .Lfunc_end42-transpose_uint16_int32_sse4
                                        # -- End function
	.globl	transpose_int16_int32_sse4      # -- Begin function transpose_int16_int32_sse4
	.p2align	4, 0x90
	.type	transpose_int16_int32_sse4,@function
transpose_int16_int32_sse4:             # @transpose_int16_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB43_1
	.p2align	4, 0x90
.LBB43_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movsx	rdx, word ptr [rdi + 2]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movsx	rdx, word ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movsx	rdx, word ptr [rdi + 6]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB43_5
.LBB43_1:
	test	edx, edx
	jle	.LBB43_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB43_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + 2*r8], eax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB43_3
.LBB43_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end43:
	.size	transpose_int16_int32_sse4, .Lfunc_end43-transpose_int16_int32_sse4
                                        # -- End function
	.globl	transpose_uint32_int32_sse4     # -- Begin function transpose_uint32_int32_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_int32_sse4,@function
transpose_uint32_int32_sse4:            # @transpose_uint32_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB44_1
	.p2align	4, 0x90
.LBB44_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	mov	edx, dword ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	mov	edx, dword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	mov	edx, dword ptr [rdi + 12]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB44_5
.LBB44_1:
	test	edx, edx
	jle	.LBB44_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB44_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB44_3
.LBB44_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end44:
	.size	transpose_uint32_int32_sse4, .Lfunc_end44-transpose_uint32_int32_sse4
                                        # -- End function
	.globl	transpose_int32_int32_sse4      # -- Begin function transpose_int32_int32_sse4
	.p2align	4, 0x90
	.type	transpose_int32_int32_sse4,@function
transpose_int32_int32_sse4:             # @transpose_int32_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB45_1
	.p2align	4, 0x90
.LBB45_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	movsxd	rdx, dword ptr [rdi + 4]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	movsxd	rdx, dword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	movsxd	rdx, dword ptr [rdi + 12]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB45_5
.LBB45_1:
	test	edx, edx
	jle	.LBB45_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB45_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB45_3
.LBB45_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end45:
	.size	transpose_int32_int32_sse4, .Lfunc_end45-transpose_int32_int32_sse4
                                        # -- End function
	.globl	transpose_uint64_int32_sse4     # -- Begin function transpose_uint64_int32_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_int32_sse4,@function
transpose_uint64_int32_sse4:            # @transpose_uint64_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB46_1
	.p2align	4, 0x90
.LBB46_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	mov	rdx, qword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	mov	rdx, qword ptr [rdi + 16]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	mov	rdx, qword ptr [rdi + 24]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB46_5
.LBB46_1:
	test	edx, edx
	jle	.LBB46_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB46_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 2*r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB46_3
.LBB46_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end46:
	.size	transpose_uint64_int32_sse4, .Lfunc_end46-transpose_uint64_int32_sse4
                                        # -- End function
	.globl	transpose_int64_int32_sse4      # -- Begin function transpose_int64_int32_sse4
	.p2align	4, 0x90
	.type	transpose_int64_int32_sse4,@function
transpose_int64_int32_sse4:             # @transpose_int64_int32_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB47_1
	.p2align	4, 0x90
.LBB47_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi], edx
	mov	rdx, qword ptr [rdi + 8]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 4], edx
	mov	rdx, qword ptr [rdi + 16]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 8], edx
	mov	rdx, qword ptr [rdi + 24]
	mov	edx, dword ptr [rcx + 4*rdx]
	mov	dword ptr [rsi + 12], edx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 16
	cmp	eax, 7
	jg	.LBB47_5
.LBB47_1:
	test	edx, edx
	jle	.LBB47_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB47_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + 2*r8]
	mov	eax, dword ptr [rcx + 4*rax]
	mov	dword ptr [rsi + r8], eax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB47_3
.LBB47_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end47:
	.size	transpose_int64_int32_sse4, .Lfunc_end47-transpose_int64_int32_sse4
                                        # -- End function
	.globl	transpose_uint8_uint64_sse4     # -- Begin function transpose_uint8_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_uint64_sse4,@function
transpose_uint8_uint64_sse4:            # @transpose_uint8_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB48_1
	.p2align	4, 0x90
.LBB48_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movzx	edx, byte ptr [rdi + 1]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movzx	edx, byte ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movzx	edx, byte ptr [rdi + 3]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB48_5
.LBB48_1:
	test	edx, edx
	jle	.LBB48_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB48_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 8*r8], rax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB48_3
.LBB48_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end48:
	.size	transpose_uint8_uint64_sse4, .Lfunc_end48-transpose_uint8_uint64_sse4
                                        # -- End function
	.globl	transpose_int8_uint64_sse4      # -- Begin function transpose_int8_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_int8_uint64_sse4,@function
transpose_int8_uint64_sse4:             # @transpose_int8_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB49_1
	.p2align	4, 0x90
.LBB49_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movsx	rdx, byte ptr [rdi + 1]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movsx	rdx, byte ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movsx	rdx, byte ptr [rdi + 3]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB49_5
.LBB49_1:
	test	edx, edx
	jle	.LBB49_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB49_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 8*r8], rax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB49_3
.LBB49_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end49:
	.size	transpose_int8_uint64_sse4, .Lfunc_end49-transpose_int8_uint64_sse4
                                        # -- End function
	.globl	transpose_uint16_uint64_sse4    # -- Begin function transpose_uint16_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_uint64_sse4,@function
transpose_uint16_uint64_sse4:           # @transpose_uint16_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB50_1
	.p2align	4, 0x90
.LBB50_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movzx	edx, word ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movzx	edx, word ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movzx	edx, word ptr [rdi + 6]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB50_5
.LBB50_1:
	test	edx, edx
	jle	.LBB50_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB50_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 4*r8], rax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB50_3
.LBB50_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end50:
	.size	transpose_uint16_uint64_sse4, .Lfunc_end50-transpose_uint16_uint64_sse4
                                        # -- End function
	.globl	transpose_int16_uint64_sse4     # -- Begin function transpose_int16_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_int16_uint64_sse4,@function
transpose_int16_uint64_sse4:            # @transpose_int16_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB51_1
	.p2align	4, 0x90
.LBB51_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movsx	rdx, word ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movsx	rdx, word ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movsx	rdx, word ptr [rdi + 6]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB51_5
.LBB51_1:
	test	edx, edx
	jle	.LBB51_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB51_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 4*r8], rax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB51_3
.LBB51_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end51:
	.size	transpose_int16_uint64_sse4, .Lfunc_end51-transpose_int16_uint64_sse4
                                        # -- End function
	.globl	transpose_uint32_uint64_sse4    # -- Begin function transpose_uint32_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_uint64_sse4,@function
transpose_uint32_uint64_sse4:           # @transpose_uint32_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB52_1
	.p2align	4, 0x90
.LBB52_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	mov	edx, dword ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	mov	edx, dword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	mov	edx, dword ptr [rdi + 12]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB52_5
.LBB52_1:
	test	edx, edx
	jle	.LBB52_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB52_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 2*r8], rax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB52_3
.LBB52_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end52:
	.size	transpose_uint32_uint64_sse4, .Lfunc_end52-transpose_uint32_uint64_sse4
                                        # -- End function
	.globl	transpose_int32_uint64_sse4     # -- Begin function transpose_int32_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_int32_uint64_sse4,@function
transpose_int32_uint64_sse4:            # @transpose_int32_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB53_1
	.p2align	4, 0x90
.LBB53_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movsxd	rdx, dword ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movsxd	rdx, dword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movsxd	rdx, dword ptr [rdi + 12]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB53_5
.LBB53_1:
	test	edx, edx
	jle	.LBB53_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB53_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 2*r8], rax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB53_3
.LBB53_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end53:
	.size	transpose_int32_uint64_sse4, .Lfunc_end53-transpose_int32_uint64_sse4
                                        # -- End function
	.globl	transpose_uint64_uint64_sse4    # -- Begin function transpose_uint64_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_uint64_sse4,@function
transpose_uint64_uint64_sse4:           # @transpose_uint64_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB54_1
	.p2align	4, 0x90
.LBB54_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	mov	rdx, qword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	mov	rdx, qword ptr [rdi + 16]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	mov	rdx, qword ptr [rdi + 24]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB54_5
.LBB54_1:
	test	edx, edx
	jle	.LBB54_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB54_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + r8], rax
	add	r8, 8
	add	edx, -1
	cmp	edx, 1
	jg	.LBB54_3
.LBB54_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end54:
	.size	transpose_uint64_uint64_sse4, .Lfunc_end54-transpose_uint64_uint64_sse4
                                        # -- End function
	.globl	transpose_int64_uint64_sse4     # -- Begin function transpose_int64_uint64_sse4
	.p2align	4, 0x90
	.type	transpose_int64_uint64_sse4,@function
transpose_int64_uint64_sse4:            # @transpose_int64_uint64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB55_1
	.p2align	4, 0x90
.LBB55_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	mov	rdx, qword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	mov	rdx, qword ptr [rdi + 16]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	mov	rdx, qword ptr [rdi + 24]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB55_5
.LBB55_1:
	test	edx, edx
	jle	.LBB55_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB55_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + r8], rax
	add	r8, 8
	add	edx, -1
	cmp	edx, 1
	jg	.LBB55_3
.LBB55_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end55:
	.size	transpose_int64_uint64_sse4, .Lfunc_end55-transpose_int64_uint64_sse4
                                        # -- End function
	.globl	transpose_uint8_int64_sse4      # -- Begin function transpose_uint8_int64_sse4
	.p2align	4, 0x90
	.type	transpose_uint8_int64_sse4,@function
transpose_uint8_int64_sse4:             # @transpose_uint8_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB56_1
	.p2align	4, 0x90
.LBB56_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, byte ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movzx	edx, byte ptr [rdi + 1]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movzx	edx, byte ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movzx	edx, byte ptr [rdi + 3]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB56_5
.LBB56_1:
	test	edx, edx
	jle	.LBB56_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB56_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, byte ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 8*r8], rax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB56_3
.LBB56_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end56:
	.size	transpose_uint8_int64_sse4, .Lfunc_end56-transpose_uint8_int64_sse4
                                        # -- End function
	.globl	transpose_int8_int64_sse4       # -- Begin function transpose_int8_int64_sse4
	.p2align	4, 0x90
	.type	transpose_int8_int64_sse4,@function
transpose_int8_int64_sse4:              # @transpose_int8_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB57_1
	.p2align	4, 0x90
.LBB57_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, byte ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movsx	rdx, byte ptr [rdi + 1]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movsx	rdx, byte ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movsx	rdx, byte ptr [rdi + 3]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 4
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB57_5
.LBB57_1:
	test	edx, edx
	jle	.LBB57_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB57_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, byte ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 8*r8], rax
	add	r8, 1
	add	edx, -1
	cmp	edx, 1
	jg	.LBB57_3
.LBB57_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end57:
	.size	transpose_int8_int64_sse4, .Lfunc_end57-transpose_int8_int64_sse4
                                        # -- End function
	.globl	transpose_uint16_int64_sse4     # -- Begin function transpose_uint16_int64_sse4
	.p2align	4, 0x90
	.type	transpose_uint16_int64_sse4,@function
transpose_uint16_int64_sse4:            # @transpose_uint16_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB58_1
	.p2align	4, 0x90
.LBB58_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movzx	edx, word ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movzx	edx, word ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movzx	edx, word ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movzx	edx, word ptr [rdi + 6]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB58_5
.LBB58_1:
	test	edx, edx
	jle	.LBB58_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB58_3:                               # =>This Inner Loop Header: Depth=1
	movzx	eax, word ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 4*r8], rax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB58_3
.LBB58_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end58:
	.size	transpose_uint16_int64_sse4, .Lfunc_end58-transpose_uint16_int64_sse4
                                        # -- End function
	.globl	transpose_int16_int64_sse4      # -- Begin function transpose_int16_int64_sse4
	.p2align	4, 0x90
	.type	transpose_int16_int64_sse4,@function
transpose_int16_int64_sse4:             # @transpose_int16_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB59_1
	.p2align	4, 0x90
.LBB59_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsx	rdx, word ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movsx	rdx, word ptr [rdi + 2]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movsx	rdx, word ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movsx	rdx, word ptr [rdi + 6]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 8
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB59_5
.LBB59_1:
	test	edx, edx
	jle	.LBB59_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB59_3:                               # =>This Inner Loop Header: Depth=1
	movsx	rax, word ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 4*r8], rax
	add	r8, 2
	add	edx, -1
	cmp	edx, 1
	jg	.LBB59_3
.LBB59_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end59:
	.size	transpose_int16_int64_sse4, .Lfunc_end59-transpose_int16_int64_sse4
                                        # -- End function
	.globl	transpose_uint32_int64_sse4     # -- Begin function transpose_uint32_int64_sse4
	.p2align	4, 0x90
	.type	transpose_uint32_int64_sse4,@function
transpose_uint32_int64_sse4:            # @transpose_uint32_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB60_1
	.p2align	4, 0x90
.LBB60_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	edx, dword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	mov	edx, dword ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	mov	edx, dword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	mov	edx, dword ptr [rdi + 12]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB60_5
.LBB60_1:
	test	edx, edx
	jle	.LBB60_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB60_3:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 2*r8], rax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB60_3
.LBB60_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end60:
	.size	transpose_uint32_int64_sse4, .Lfunc_end60-transpose_uint32_int64_sse4
                                        # -- End function
	.globl	transpose_int32_int64_sse4      # -- Begin function transpose_int32_int64_sse4
	.p2align	4, 0x90
	.type	transpose_int32_int64_sse4,@function
transpose_int32_int64_sse4:             # @transpose_int32_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB61_1
	.p2align	4, 0x90
.LBB61_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	movsxd	rdx, dword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	movsxd	rdx, dword ptr [rdi + 4]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	movsxd	rdx, dword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	movsxd	rdx, dword ptr [rdi + 12]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 16
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB61_5
.LBB61_1:
	test	edx, edx
	jle	.LBB61_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB61_3:                               # =>This Inner Loop Header: Depth=1
	movsxd	rax, dword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + 2*r8], rax
	add	r8, 4
	add	edx, -1
	cmp	edx, 1
	jg	.LBB61_3
.LBB61_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end61:
	.size	transpose_int32_int64_sse4, .Lfunc_end61-transpose_int32_int64_sse4
                                        # -- End function
	.globl	transpose_uint64_int64_sse4     # -- Begin function transpose_uint64_int64_sse4
	.p2align	4, 0x90
	.type	transpose_uint64_int64_sse4,@function
transpose_uint64_int64_sse4:            # @transpose_uint64_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB62_1
	.p2align	4, 0x90
.LBB62_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	mov	rdx, qword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	mov	rdx, qword ptr [rdi + 16]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	mov	rdx, qword ptr [rdi + 24]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB62_5
.LBB62_1:
	test	edx, edx
	jle	.LBB62_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB62_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + r8], rax
	add	r8, 8
	add	edx, -1
	cmp	edx, 1
	jg	.LBB62_3
.LBB62_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end62:
	.size	transpose_uint64_int64_sse4, .Lfunc_end62-transpose_uint64_int64_sse4
                                        # -- End function
	.globl	transpose_int64_int64_sse4      # -- Begin function transpose_int64_int64_sse4
	.p2align	4, 0x90
	.type	transpose_int64_int64_sse4,@function
transpose_int64_int64_sse4:             # @transpose_int64_int64_sse4
# %bb.0:
	push	rbp
	mov	rbp, rsp
	and	rsp, -8
	cmp	edx, 4
	jl	.LBB63_1
	.p2align	4, 0x90
.LBB63_5:                               # =>This Inner Loop Header: Depth=1
	mov	eax, edx
	mov	rdx, qword ptr [rdi]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi], rdx
	mov	rdx, qword ptr [rdi + 8]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 8], rdx
	mov	rdx, qword ptr [rdi + 16]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 16], rdx
	mov	rdx, qword ptr [rdi + 24]
	movsxd	rdx, dword ptr [rcx + 4*rdx]
	mov	qword ptr [rsi + 24], rdx
	lea	edx, [rax - 4]
	add	rdi, 32
	add	rsi, 32
	cmp	eax, 7
	jg	.LBB63_5
.LBB63_1:
	test	edx, edx
	jle	.LBB63_4
# %bb.2:
	add	edx, 1
	xor	r8d, r8d
	.p2align	4, 0x90
.LBB63_3:                               # =>This Inner Loop Header: Depth=1
	mov	rax, qword ptr [rdi + r8]
	movsxd	rax, dword ptr [rcx + 4*rax]
	mov	qword ptr [rsi + r8], rax
	add	r8, 8
	add	edx, -1
	cmp	edx, 1
	jg	.LBB63_3
.LBB63_4:
	mov	rsp, rbp
	pop	rbp
	ret
.Lfunc_end63:
	.size	transpose_int64_int64_sse4, .Lfunc_end63-transpose_int64_int64_sse4
                                        # -- End function
	.ident	"Ubuntu clang version 11.0.0-2~ubuntu20.04.1"
	.section	".note.GNU-stack","",@progbits
	.addrsig
