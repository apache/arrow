	.text
	.file	"bitmap_bmi2.c"
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4               // -- Begin function levels_to_bitmap_neon
.LCPI1_0:
	.xword	0                       // 0x0
	.xword	1                       // 0x1
	.text
	.globl	levels_to_bitmap_neon
	.p2align	2
	.type	levels_to_bitmap_neon,@function
levels_to_bitmap_neon:                  // @levels_to_bitmap_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	cmp	w1, #1                  // =1
	mov	x29, sp
	b.lt	.LBB1_3
// %bb.1:
	cmp	w1, #3                  // =3
	mov	w9, w1
	b.hi	.LBB1_4
// %bb.2:
	mov	x10, xzr
	mov	x8, xzr
	b	.LBB1_7
.LBB1_3:
	mov	x8, xzr
	b	.LBB1_8
.LBB1_4:
	adrp	x11, .LCPI1_0
	ldr	q1, [x11, :lo12:.LCPI1_0]
	mov	w11, #2
	dup	v3.2s, w2
	dup	v2.2d, x11
	mov	w11, #1
	and	x10, x9, #0xfffffffc
	shl	v4.2s, v3.2s, #16
	dup	v3.2d, x11
	mov	w11, #4
	add	x8, x0, #4              // =4
	movi	v0.2d, #0000000000000000
	sshr	v4.2s, v4.2s, #16
	dup	v5.2d, x11
	mov	x11, x10
	movi	v6.2d, #0000000000000000
.LBB1_5:                                // =>This Inner Loop Header: Depth=1
	ldursh	w12, [x8, #-4]
	ldrsh	w13, [x8]
	ldursh	w14, [x8, #-2]
	add	v17.2d, v1.2d, v2.2d
	fmov	s7, w12
	ldrsh	w12, [x8, #2]
	fmov	s16, w13
	mov	v7.s[1], w14
	cmgt	v7.2s, v7.2s, v4.2s
	mov	v16.s[1], w12
	cmgt	v16.2s, v16.2s, v4.2s
	ushll	v7.2d, v7.2s, #0
	ushll	v16.2d, v16.2s, #0
	and	v7.16b, v7.16b, v3.16b
	and	v16.16b, v16.16b, v3.16b
	ushl	v7.2d, v7.2d, v1.2d
	ushl	v16.2d, v16.2d, v17.2d
	subs	x11, x11, #4            // =4
	add	v1.2d, v1.2d, v5.2d
	orr	v0.16b, v7.16b, v0.16b
	orr	v6.16b, v16.16b, v6.16b
	add	x8, x8, #8              // =8
	b.ne	.LBB1_5
// %bb.6:
	orr	v0.16b, v6.16b, v0.16b
	dup	v1.2d, v0.d[1]
	orr	v0.16b, v0.16b, v1.16b
	cmp	x10, x9
	fmov	x8, d0
	b.eq	.LBB1_8
.LBB1_7:                                // =>This Inner Loop Header: Depth=1
	ldrsh	w11, [x0, x10, lsl #1]
	cmp	w11, w2, sxth
	cset	w11, gt
	lsl	x11, x11, x10
	add	x10, x10, #1            // =1
	cmp	x9, x10
	orr	x8, x11, x8
	b.ne	.LBB1_7
.LBB1_8:
	mov	x0, x8
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end1:
	.size	levels_to_bitmap_neon, .Lfunc_end1-levels_to_bitmap_neon
                                        // -- End function
	.ident	"clang version 10.0.0-4ubuntu1 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
