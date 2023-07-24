	.text
	.file	"unpack_bool.c"
	.globl	bytes_to_bools_neon     // -- Begin function bytes_to_bools_neon
	.p2align	2
	.type	bytes_to_bools_neon,@function
bytes_to_bools_neon:                    // @bytes_to_bools_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	cmp	w1, #1                  // =1
	mov	x29, sp
	b.lt	.LBB0_12
// %bb.1:
	mov	w9, w1
	mov	x8, xzr
	lsl	x9, x9, #3
	mov	w10, #5
	b	.LBB0_3
.LBB0_2:                                //   in Loop: Header=BB0_3 Depth=1
	add	x8, x8, #8              // =8
	cmp	x9, x8
	add	x0, x0, #1              // =1
	b.eq	.LBB0_12
.LBB0_3:                                // =>This Inner Loop Header: Depth=1
	cmp	w8, w3
	b.ge	.LBB0_2
// %bb.4:                               //   in Loop: Header=BB0_3 Depth=1
	ldrb	w12, [x0]
	and	x11, x8, #0xffffffff
	orr	x13, x11, #0x1
	cmp	w13, w3
	and	w12, w12, #0x1
	strb	w12, [x2, x11]
	b.ge	.LBB0_2
// %bb.5:                               //   in Loop: Header=BB0_3 Depth=1
	ldrb	w14, [x0]
	orr	x12, x11, #0x2
	cmp	w12, w3
	ubfx	w14, w14, #1, #1
	strb	w14, [x2, x13]
	b.ge	.LBB0_2
// %bb.6:                               //   in Loop: Header=BB0_3 Depth=1
	ldrb	w14, [x0]
	orr	x13, x11, #0x3
	cmp	w13, w3
	ubfx	w14, w14, #2, #1
	strb	w14, [x2, x12]
	b.ge	.LBB0_2
// %bb.7:                               //   in Loop: Header=BB0_3 Depth=1
	ldrb	w14, [x0]
	orr	x12, x11, #0x4
	cmp	w12, w3
	ubfx	w14, w14, #3, #1
	strb	w14, [x2, x13]
	b.ge	.LBB0_2
// %bb.8:                               //   in Loop: Header=BB0_3 Depth=1
	ldrb	w14, [x0]
	orr	x13, x11, x10
	cmp	w13, w3
	ubfx	w14, w14, #4, #1
	strb	w14, [x2, x12]
	b.ge	.LBB0_2
// %bb.9:                               //   in Loop: Header=BB0_3 Depth=1
	ldrb	w14, [x0]
	orr	x12, x11, #0x6
	cmp	w12, w3
	ubfx	w14, w14, #5, #1
	strb	w14, [x2, x13]
	b.ge	.LBB0_2
// %bb.10:                              //   in Loop: Header=BB0_3 Depth=1
	ldrb	w13, [x0]
	orr	x11, x11, #0x7
	cmp	w11, w3
	ubfx	w13, w13, #6, #1
	strb	w13, [x2, x12]
	b.ge	.LBB0_2
// %bb.11:                              //   in Loop: Header=BB0_3 Depth=1
	ldrb	w12, [x0]
	lsr	w12, w12, #7
	strb	w12, [x2, x11]
	b	.LBB0_2
.LBB0_12:
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end0:
	.size	bytes_to_bools_neon, .Lfunc_end0-bytes_to_bools_neon
                                        // -- End function
	.ident	"clang version 10.0.0-4ubuntu1 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
