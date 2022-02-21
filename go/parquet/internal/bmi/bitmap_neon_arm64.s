//+build !noasm !appengine

// (C2GOASM doesn't work correctly for Arm64)
TEXT ·_extract_bits_neon(SB), $0-24

    MOVD bitmap+0(FP), R0
    MOVD selectBitmap+8(FP), R1

    WORD $0xa9bf7bfd // stp    x29, x30, [sp, #-16]!
    WORD $0x910003fd // mov    x29, sp
    WORD $0xb4000001 // cbz    x1, LBB0_4
    WORD $0xaa0003e8 // mov    x8, x0
    WORD $0xaa1f03e0 // mov    x0, xzr
    WORD $0x52800029 // mov    w9, #1
LBB0_2:
    WORD $0x8a08002a // and    x10, x1, x8
    WORD $0xcb0103eb // neg    x11, x1
    WORD $0xea0b015f // tst    x10, x11
    WORD $0xd100042c // sub    x12, x1, #1
    WORD $0x9a8903ea // csel    x10, xzr, x9, eq
    WORD $0xea010181 // ands    x1, x12, x1
    WORD $0xaa000140 // orr    x0, x10, x0
    WORD $0xd37ff929 // lsl    x9, x9, #1
    BNE LBB0_2
    WORD $0xa8c17bfd // ldp    x29, x30, [sp], #16
    RET
LBB0_4:
    WORD $0xaa1f03e0 // mov    x0, xzr
    WORD $0xa8c17bfd // ldp    x29, x30, [sp], #16
    RET
