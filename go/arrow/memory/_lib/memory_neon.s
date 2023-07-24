	.text
	.file	"memory.c"
	.globl	memset_neon             // -- Begin function memset_neon
	.p2align	2
	.type	memset_neon,@function
memset_neon:                            // @memset_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!   // 16-byte Folded Spill
	add	x8, x0, x1
	cmp	x8, x0
	mov	x29, sp
	b.ls	.LBB0_7
// %bb.1:
	cmp	x1, #32                 // =32
	b.hs	.LBB0_3
// %bb.2:
	mov	x9, x0
	b	.LBB0_6
.LBB0_3:
	and	x10, x1, #0xffffffffffffffe0
	dup	v0.16b, w2
	add	x9, x0, x10
	add	x11, x0, #16            // =16
	mov	x12, x10
.LBB0_4:                                // =>This Inner Loop Header: Depth=1
	stp	q0, q0, [x11, #-16]
	subs	x12, x12, #32           // =32
	add	x11, x11, #32           // =32
	b.ne	.LBB0_4
// %bb.5:
	cmp	x10, x1
	b.eq	.LBB0_7
.LBB0_6:                                // =>This Inner Loop Header: Depth=1
	strb	w2, [x9], #1
	cmp	x8, x9
	b.ne	.LBB0_6
.LBB0_7:
	ldp	x29, x30, [sp], #16     // 16-byte Folded Reload
	ret
.Lfunc_end0:
	.size	memset_neon, .Lfunc_end0-memset_neon
                                        // -- End function

	.ident	"clang version 9.0.1-12 "
	.section	".note.GNU-stack","",@progbits
	.addrsig
