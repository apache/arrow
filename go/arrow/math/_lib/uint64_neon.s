	.text
	.file	"uint64.c"
	.globl	sum_uint64_neon         // -- Begin function sum_uint64_neon
	.p2align	2
	.type	sum_uint64_neon,@function
sum_uint64_neon:                        // @sum_uint64_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	mov	x29, sp
	cbz	x1, .LBB0_3
// %bb.1:
	cmp	x1, #3                  // =3
	b.hi	.LBB0_4
// %bb.2:
	mov	x8, xzr
	mov	x9, xzr
	b	.LBB0_7
.LBB0_3:
	mov	x9, xzr
	str	x9, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.LBB0_4:
	and	x8, x1, #0xfffffffffffffffc
	add	x9, x0, #16             // =16
	movi	v0.2d, #0000000000000000
	mov	x10, x8
	movi	v1.2d, #0000000000000000
.LBB0_5:                                // =>This Inner Loop Header: Depth=1
	ldp	q2, q3, [x9, #-16]
	subs	x10, x10, #4            // =4
	add	x9, x9, #32             // =32
	add	v0.2d, v2.2d, v0.2d
	add	v1.2d, v3.2d, v1.2d
	b.ne	.LBB0_5
// %bb.6:
	add	v0.2d, v1.2d, v0.2d
	addp	d0, v0.2d
	cmp	x8, x1
	fmov	x9, d0
	b.eq	.LBB0_9
.LBB0_7:
	add	x10, x0, x8, lsl #3
	sub	x8, x1, x8
.LBB0_8:                                // =>This Inner Loop Header: Depth=1
	ldr	x11, [x10], #8
	subs	x8, x8, #1              // =1
	add	x9, x11, x9
	b.ne	.LBB0_8
.LBB0_9:
	str	x9, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end0:
	.size	sum_uint64_neon, .Lfunc_end0-sum_uint64_neon
                                        // -- End function

	.ident	"clang version 9.0.1-12 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
