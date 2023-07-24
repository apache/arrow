	.text
	.file	"min_max.c"
	.globl	int32_max_min_neon      // -- Begin function int32_max_min_neon
	.p2align	2
	.type	int32_max_min_neon,@function
int32_max_min_neon:                     // @int32_max_min_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	cmp	w1, #1                  // =1
	mov	x29, sp
	b.lt	.LBB0_3
// %bb.1:
	cmp	w1, #3                  // =3
	mov	w8, w1
	b.hi	.LBB0_4
// %bb.2:
	mov	x9, xzr
	mov	w11, #-2147483648
	mov	w10, #2147483647
	b	.LBB0_7
.LBB0_3:
	mov	w10, #2147483647
	mov	w11, #-2147483648
	str	w11, [x3]
	str	w10, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.LBB0_4:
	and	x9, x8, #0xfffffffc
	add	x10, x0, #8             // =8
	movi	v2.2s, #128, lsl #24
	mvni	v0.2s, #128, lsl #24
	mvni	v1.2s, #128, lsl #24
	mov	x11, x9
	movi	v3.2s, #128, lsl #24
.LBB0_5:                                // =>This Inner Loop Header: Depth=1
	ldp	d4, d5, [x10, #-8]
	subs	x11, x11, #4            // =4
	add	x10, x10, #16           // =16
	smin	v0.2s, v0.2s, v4.2s
	smin	v1.2s, v1.2s, v5.2s
	smax	v2.2s, v2.2s, v4.2s
	smax	v3.2s, v3.2s, v5.2s
	b.ne	.LBB0_5
// %bb.6:
	smax	v2.2s, v2.2s, v3.2s
	smin	v0.2s, v0.2s, v1.2s
	dup	v1.2s, v2.s[1]
	dup	v3.2s, v0.s[1]
	smax	v1.2s, v2.2s, v1.2s
	smin	v0.2s, v0.2s, v3.2s
	cmp	x9, x8
	fmov	w11, s1
	fmov	w10, s0
	b.eq	.LBB0_9
.LBB0_7:
	add	x12, x0, x9, lsl #2
	sub	x8, x8, x9
.LBB0_8:                                // =>This Inner Loop Header: Depth=1
	ldr	w9, [x12], #4
	cmp	w10, w9
	csel	w10, w10, w9, lt
	cmp	w11, w9
	csel	w11, w11, w9, gt
	subs	x8, x8, #1              // =1
	b.ne	.LBB0_8
.LBB0_9:
	str	w11, [x3]
	str	w10, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end0:
	.size	int32_max_min_neon, .Lfunc_end0-int32_max_min_neon
                                        // -- End function
	.globl	uint32_max_min_neon     // -- Begin function uint32_max_min_neon
	.p2align	2
	.type	uint32_max_min_neon,@function
uint32_max_min_neon:                    // @uint32_max_min_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	cmp	w1, #1                  // =1
	mov	x29, sp
	b.lt	.LBB1_3
// %bb.1:
	cmp	w1, #3                  // =3
	mov	w8, w1
	b.hi	.LBB1_4
// %bb.2:
	mov	x9, xzr
	mov	w10, wzr
	mov	w11, #-1
	b	.LBB1_7
.LBB1_3:
	mov	w10, wzr
	mov	w11, #-1
	str	w10, [x3]
	str	w11, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.LBB1_4:
	and	x9, x8, #0xfffffffc
	movi	v1.2d, #0000000000000000
	movi	v0.2d, #0xffffffffffffffff
	add	x10, x0, #8             // =8
	movi	v2.2d, #0xffffffffffffffff
	mov	x11, x9
	movi	v3.2d, #0000000000000000
.LBB1_5:                                // =>This Inner Loop Header: Depth=1
	ldp	d4, d5, [x10, #-8]
	subs	x11, x11, #4            // =4
	add	x10, x10, #16           // =16
	umin	v0.2s, v0.2s, v4.2s
	umin	v2.2s, v2.2s, v5.2s
	umax	v1.2s, v1.2s, v4.2s
	umax	v3.2s, v3.2s, v5.2s
	b.ne	.LBB1_5
// %bb.6:
	umax	v1.2s, v1.2s, v3.2s
	umin	v0.2s, v0.2s, v2.2s
	dup	v2.2s, v1.s[1]
	dup	v3.2s, v0.s[1]
	umax	v1.2s, v1.2s, v2.2s
	umin	v0.2s, v0.2s, v3.2s
	cmp	x9, x8
	fmov	w10, s1
	fmov	w11, s0
	b.eq	.LBB1_9
.LBB1_7:
	add	x12, x0, x9, lsl #2
	sub	x8, x8, x9
.LBB1_8:                                // =>This Inner Loop Header: Depth=1
	ldr	w9, [x12], #4
	cmp	w11, w9
	csel	w11, w11, w9, lo
	cmp	w10, w9
	csel	w10, w10, w9, hi
	subs	x8, x8, #1              // =1
	b.ne	.LBB1_8
.LBB1_9:
	str	w10, [x3]
	str	w11, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end1:
	.size	uint32_max_min_neon, .Lfunc_end1-uint32_max_min_neon
                                        // -- End function
	.globl	int64_max_min_neon      // -- Begin function int64_max_min_neon
	.p2align	2
	.type	int64_max_min_neon,@function
int64_max_min_neon:                     // @int64_max_min_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	cmp	w1, #1                  // =1
	mov	x29, sp
	b.lt	.LBB2_3
// %bb.1:
	mov	w8, w1
	mov	x11, #-9223372036854775808
	cmp	w1, #3                  // =3
	mov	x10, #9223372036854775807
	b.hi	.LBB2_4
// %bb.2:
	mov	x9, xzr
	b	.LBB2_7
.LBB2_3:
	mov	x10, #9223372036854775807
	mov	x11, #-9223372036854775808
	str	x11, [x3]
	str	x10, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.LBB2_4:
	and	x9, x8, #0xfffffffc
	dup	v1.2d, x11
	dup	v0.2d, x10
	add	x10, x0, #16            // =16
	mov	x11, x9
	mov	v2.16b, v0.16b
	mov	v3.16b, v1.16b
.LBB2_5:                                // =>This Inner Loop Header: Depth=1
	ldp	q4, q5, [x10, #-16]
	mov	v6.16b, v3.16b
	mov	v7.16b, v1.16b
	mov	v3.16b, v2.16b
	mov	v1.16b, v0.16b
	cmgt	v0.2d, v4.2d, v0.2d
	cmgt	v2.2d, v5.2d, v2.2d
	bsl	v0.16b, v1.16b, v4.16b
	cmgt	v1.2d, v7.2d, v4.2d
	bsl	v2.16b, v3.16b, v5.16b
	cmgt	v3.2d, v6.2d, v5.2d
	subs	x11, x11, #4            // =4
	bsl	v1.16b, v7.16b, v4.16b
	bsl	v3.16b, v6.16b, v5.16b
	add	x10, x10, #32           // =32
	b.ne	.LBB2_5
// %bb.6:
	cmgt	v4.2d, v1.2d, v3.2d
	cmgt	v5.2d, v2.2d, v0.2d
	bsl	v4.16b, v1.16b, v3.16b
	bsl	v5.16b, v0.16b, v2.16b
	dup	v0.2d, v4.d[1]
	dup	v1.2d, v5.d[1]
	cmgt	v2.2d, v4.2d, v0.2d
	cmgt	v3.2d, v1.2d, v5.2d
	bsl	v2.16b, v4.16b, v0.16b
	bsl	v3.16b, v5.16b, v1.16b
	cmp	x9, x8
	fmov	x11, d2
	fmov	x10, d3
	b.eq	.LBB2_9
.LBB2_7:
	add	x12, x0, x9, lsl #3
	sub	x8, x8, x9
.LBB2_8:                                // =>This Inner Loop Header: Depth=1
	ldr	x9, [x12], #8
	cmp	x10, x9
	csel	x10, x10, x9, lt
	cmp	x11, x9
	csel	x11, x11, x9, gt
	subs	x8, x8, #1              // =1
	b.ne	.LBB2_8
.LBB2_9:
	str	x11, [x3]
	str	x10, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end2:
	.size	int64_max_min_neon, .Lfunc_end2-int64_max_min_neon
                                        // -- End function
	.globl	uint64_max_min_neon     // -- Begin function uint64_max_min_neon
	.p2align	2
	.type	uint64_max_min_neon,@function
uint64_max_min_neon:                    // @uint64_max_min_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	cmp	w1, #1                  // =1
	mov	x29, sp
	b.lt	.LBB3_3
// %bb.1:
	cmp	w1, #3                  // =3
	mov	w8, w1
	b.hi	.LBB3_4
// %bb.2:
	mov	x9, xzr
	mov	x10, xzr
	mov	x11, #-1
	b	.LBB3_7
.LBB3_3:
	mov	x10, xzr
	mov	x11, #-1
	str	x10, [x3]
	str	x11, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.LBB3_4:
	and	x9, x8, #0xfffffffc
	add	x10, x0, #16            // =16
	movi	v1.2d, #0000000000000000
	movi	v0.2d, #0xffffffffffffffff
	movi	v2.2d, #0xffffffffffffffff
	mov	x11, x9
	movi	v3.2d, #0000000000000000
.LBB3_5:                                // =>This Inner Loop Header: Depth=1
	ldp	q4, q5, [x10, #-16]
	mov	v6.16b, v3.16b
	mov	v7.16b, v1.16b
	mov	v3.16b, v2.16b
	mov	v1.16b, v0.16b
	cmhi	v0.2d, v4.2d, v0.2d
	cmhi	v2.2d, v5.2d, v2.2d
	bsl	v0.16b, v1.16b, v4.16b
	cmhi	v1.2d, v7.2d, v4.2d
	bsl	v2.16b, v3.16b, v5.16b
	cmhi	v3.2d, v6.2d, v5.2d
	subs	x11, x11, #4            // =4
	bsl	v1.16b, v7.16b, v4.16b
	bsl	v3.16b, v6.16b, v5.16b
	add	x10, x10, #32           // =32
	b.ne	.LBB3_5
// %bb.6:
	cmhi	v4.2d, v1.2d, v3.2d
	cmhi	v5.2d, v2.2d, v0.2d
	bsl	v4.16b, v1.16b, v3.16b
	bsl	v5.16b, v0.16b, v2.16b
	dup	v0.2d, v4.d[1]
	dup	v1.2d, v5.d[1]
	cmhi	v2.2d, v4.2d, v0.2d
	cmhi	v3.2d, v1.2d, v5.2d
	bsl	v2.16b, v4.16b, v0.16b
	bsl	v3.16b, v5.16b, v1.16b
	cmp	x9, x8
	fmov	x10, d2
	fmov	x11, d3
	b.eq	.LBB3_9
.LBB3_7:
	add	x12, x0, x9, lsl #3
	sub	x8, x8, x9
.LBB3_8:                                // =>This Inner Loop Header: Depth=1
	ldr	x9, [x12], #8
	cmp	x11, x9
	csel	x11, x11, x9, lo
	cmp	x10, x9
	csel	x10, x10, x9, hi
	subs	x8, x8, #1              // =1
	b.ne	.LBB3_8
.LBB3_9:
	str	x10, [x3]
	str	x11, [x2]
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end3:
	.size	uint64_max_min_neon, .Lfunc_end3-uint64_max_min_neon
                                        // -- End function

	.ident	"clang version 9.0.1-12 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
