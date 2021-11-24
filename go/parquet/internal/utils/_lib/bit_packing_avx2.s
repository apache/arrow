	.text
	.intel_syntax noprefix
	.file	"bit_packing_avx2.c"
	.section	.rodata.cst8,"aM",@progbits,8
	.p2align	3                               # -- Begin function unpack32_avx2
.LCPI0_0:
	.quad	9223372034707292159             # 0x7fffffff7fffffff
.LCPI0_8:
	.quad	4611686015206162431             # 0x3fffffff3fffffff
.LCPI0_12:
	.quad	2305843005455597567             # 0x1fffffff1fffffff
.LCPI0_23:
	.quad	1152921500580315135             # 0xfffffff0fffffff
.LCPI0_25:
	.quad	576460748142673919              # 0x7ffffff07ffffff
.LCPI0_34:
	.quad	288230371923853311              # 0x3ffffff03ffffff
.LCPI0_35:
	.quad	42949672976                     # 0xa00000010
.LCPI0_36:
	.quad	94489280528                     # 0x1600000010
.LCPI0_38:
	.quad	144115183814443007              # 0x1ffffff01ffffff
.LCPI0_49:
	.quad	36028792732385279               # 0x7fffff007fffff
.LCPI0_56:
	.quad	18014394218708991               # 0x3fffff003fffff
.LCPI0_59:
	.quad	9007194961870847                # 0x1fffff001fffff
.LCPI0_66:
	.quad	4503595333451775                # 0xfffff000fffff
.LCPI0_68:
	.quad	2251795519242239                # 0x7ffff0007ffff
.LCPI0_73:
	.quad	1125895612137471                # 0x3ffff0003ffff
.LCPI0_76:
	.quad	562945658585087                 # 0x1ffff0001ffff
.LCPI0_80:
	.quad	68719476736                     # 0x1000000000
.LCPI0_82:
	.quad	140733193420799                 # 0x7fff00007fff
.LCPI0_87:
	.quad	70364449226751                  # 0x3fff00003fff
.LCPI0_90:
	.quad	35180077129727                  # 0x1fff00001fff
.LCPI0_95:
	.quad	17587891081215                  # 0xfff00000fff
.LCPI0_97:
	.quad	8791798056959                   # 0x7ff000007ff
.LCPI0_102:
	.quad	4393751544831                   # 0x3ff000003ff
.LCPI0_105:
	.quad	2194728288767                   # 0x1ff000001ff
.LCPI0_112:
	.quad	545460846719                    # 0x7f0000007f
.LCPI0_117:
	.quad	270582939711                    # 0x3f0000003f
.LCPI0_120:
	.quad	133143986207                    # 0x1f0000001f
.LCPI0_125:
	.quad	64424509455                     # 0xf0000000f
.LCPI0_127:
	.quad	30064771079                     # 0x700000007
.LCPI0_132:
	.quad	12884901891                     # 0x300000003
.LCPI0_135:
	.quad	4294967297                      # 0x100000001
	.section	.rodata.cst32,"aM",@progbits,32
	.p2align	5
.LCPI0_1:
	.long	24                              # 0x18
	.long	23                              # 0x17
	.long	22                              # 0x16
	.long	21                              # 0x15
	.long	20                              # 0x14
	.long	19                              # 0x13
	.long	18                              # 0x12
	.long	17                              # 0x11
.LCPI0_2:
	.long	8                               # 0x8
	.long	9                               # 0x9
	.long	10                              # 0xa
	.long	11                              # 0xb
	.long	12                              # 0xc
	.long	13                              # 0xd
	.long	14                              # 0xe
	.long	15                              # 0xf
.LCPI0_3:
	.long	16                              # 0x10
	.long	15                              # 0xf
	.long	14                              # 0xe
	.long	13                              # 0xd
	.long	12                              # 0xc
	.long	11                              # 0xb
	.long	10                              # 0xa
	.long	9                               # 0x9
.LCPI0_4:
	.long	16                              # 0x10
	.long	17                              # 0x11
	.long	18                              # 0x12
	.long	19                              # 0x13
	.long	20                              # 0x14
	.long	21                              # 0x15
	.long	22                              # 0x16
	.long	23                              # 0x17
.LCPI0_7:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	1                               # 0x1
.LCPI0_11:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
.LCPI0_15:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_18:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_21:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	3                               # 0x3
.LCPI0_22:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
.LCPI0_24:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
.LCPI0_28:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_31:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_32:
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	5                               # 0x5
.LCPI0_33:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_37:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	6                               # 0x6
.LCPI0_39:
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_42:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
.LCPI0_45:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	7                               # 0x7
.LCPI0_48:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	1                               # 0x1
.LCPI0_52:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
.LCPI0_53:
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_54:
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	9                               # 0x9
.LCPI0_55:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
.LCPI0_57:
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	10                              # 0xa
.LCPI0_58:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	9                               # 0x9
	.long	0                               # 0x0
	.long	0                               # 0x0
.LCPI0_60:
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
.LCPI0_61:
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	3                               # 0x3
.LCPI0_64:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	0                               # 0x0
	.long	11                              # 0xb
.LCPI0_65:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	12                              # 0xc
.LCPI0_67:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	5                               # 0x5
.LCPI0_69:
	.long	0                               # 0x0
	.long	11                              # 0xb
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
.LCPI0_70:
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	0                               # 0x0
	.long	9                               # 0x9
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
.LCPI0_71:
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	0                               # 0x0
	.long	13                              # 0xd
.LCPI0_72:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
.LCPI0_74:
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
	.long	14                              # 0xe
.LCPI0_75:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
.LCPI0_77:
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	14                              # 0xe
	.long	0                               # 0x0
.LCPI0_78:
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	0                               # 0x0
	.long	7                               # 0x7
.LCPI0_79:
	.long	0                               # 0x0
	.long	9                               # 0x9
	.long	0                               # 0x0
	.long	11                              # 0xb
	.long	0                               # 0x0
	.long	13                              # 0xd
	.long	0                               # 0x0
	.long	15                              # 0xf
.LCPI0_81:
	.long	0                               # 0x0
	.long	15                              # 0xf
	.long	0                               # 0x0
	.long	13                              # 0xd
	.long	0                               # 0x0
	.long	11                              # 0xb
	.long	0                               # 0x0
	.long	9                               # 0x9
.LCPI0_83:
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	0                               # 0x0
	.long	1                               # 0x1
.LCPI0_84:
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	14                              # 0xe
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
.LCPI0_85:
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	17                              # 0x11
.LCPI0_86:
	.long	0                               # 0x0
	.long	14                              # 0xe
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	0                               # 0x0
	.long	2                               # 0x2
.LCPI0_88:
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	18                              # 0x12
.LCPI0_89:
	.long	0                               # 0x0
	.long	13                              # 0xd
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	14                              # 0xe
	.long	0                               # 0x0
.LCPI0_91:
	.long	8                               # 0x8
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	15                              # 0xf
	.long	0                               # 0x0
	.long	9                               # 0x9
	.long	0                               # 0x0
	.long	3                               # 0x3
.LCPI0_92:
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	17                              # 0x11
	.long	0                               # 0x0
	.long	11                              # 0xb
.LCPI0_93:
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	18                              # 0x12
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	19                              # 0x13
.LCPI0_94:
	.long	0                               # 0x0
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	20                              # 0x14
.LCPI0_96:
	.long	0                               # 0x0
	.long	11                              # 0xb
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	12                              # 0xc
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	13                              # 0xd
.LCPI0_98:
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	14                              # 0xe
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	15                              # 0xf
	.long	0                               # 0x0
	.long	5                               # 0x5
.LCPI0_99:
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	17                              # 0x11
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	18                              # 0x12
	.long	0                               # 0x0
.LCPI0_100:
	.long	8                               # 0x8
	.long	19                              # 0x13
	.long	0                               # 0x0
	.long	9                               # 0x9
	.long	20                              # 0x14
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	21                              # 0x15
.LCPI0_101:
	.long	0                               # 0x0
	.long	10                              # 0xa
	.long	20                              # 0x14
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	18                              # 0x12
	.long	0                               # 0x0
	.long	6                               # 0x6
.LCPI0_103:
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	14                              # 0xe
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	12                              # 0xc
	.long	22                              # 0x16
.LCPI0_104:
	.long	0                               # 0x0
	.long	9                               # 0x9
	.long	18                              # 0x12
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	13                              # 0xd
	.long	22                              # 0x16
	.long	0                               # 0x0
.LCPI0_106:
	.long	8                               # 0x8
	.long	17                              # 0x11
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	12                              # 0xc
	.long	21                              # 0x15
	.long	0                               # 0x0
	.long	7                               # 0x7
.LCPI0_107:
	.long	16                              # 0x10
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	11                              # 0xb
	.long	20                              # 0x14
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	15                              # 0xf
.LCPI0_108:
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	10                              # 0xa
	.long	19                              # 0x13
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	14                              # 0xe
	.long	23                              # 0x17
.LCPI0_111:
	.long	0                               # 0x0
	.long	7                               # 0x7
	.long	14                              # 0xe
	.long	21                              # 0x15
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	10                              # 0xa
	.long	17                              # 0x11
.LCPI0_113:
	.long	24                              # 0x18
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	13                              # 0xd
	.long	20                              # 0x14
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	9                               # 0x9
.LCPI0_114:
	.long	16                              # 0x10
	.long	23                              # 0x17
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	12                              # 0xc
	.long	19                              # 0x13
	.long	0                               # 0x0
	.long	1                               # 0x1
.LCPI0_115:
	.long	8                               # 0x8
	.long	15                              # 0xf
	.long	22                              # 0x16
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	11                              # 0xb
	.long	18                              # 0x12
	.long	25                              # 0x19
.LCPI0_116:
	.long	0                               # 0x0
	.long	6                               # 0x6
	.long	12                              # 0xc
	.long	18                              # 0x12
	.long	24                              # 0x18
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	10                              # 0xa
.LCPI0_118:
	.long	16                              # 0x10
	.long	22                              # 0x16
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	8                               # 0x8
	.long	14                              # 0xe
	.long	20                              # 0x14
	.long	26                              # 0x1a
.LCPI0_119:
	.long	0                               # 0x0
	.long	5                               # 0x5
	.long	10                              # 0xa
	.long	15                              # 0xf
	.long	20                              # 0x14
	.long	25                              # 0x19
	.long	0                               # 0x0
	.long	3                               # 0x3
.LCPI0_121:
	.long	8                               # 0x8
	.long	13                              # 0xd
	.long	18                              # 0x12
	.long	23                              # 0x17
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	6                               # 0x6
	.long	11                              # 0xb
.LCPI0_122:
	.long	16                              # 0x10
	.long	21                              # 0x15
	.long	26                              # 0x1a
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	9                               # 0x9
	.long	14                              # 0xe
	.long	19                              # 0x13
.LCPI0_123:
	.long	24                              # 0x18
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	7                               # 0x7
	.long	12                              # 0xc
	.long	17                              # 0x11
	.long	22                              # 0x16
	.long	27                              # 0x1b
.LCPI0_124:
	.long	0                               # 0x0
	.long	4                               # 0x4
	.long	8                               # 0x8
	.long	12                              # 0xc
	.long	16                              # 0x10
	.long	20                              # 0x14
	.long	24                              # 0x18
	.long	28                              # 0x1c
.LCPI0_126:
	.long	0                               # 0x0
	.long	3                               # 0x3
	.long	6                               # 0x6
	.long	9                               # 0x9
	.long	12                              # 0xc
	.long	15                              # 0xf
	.long	18                              # 0x12
	.long	21                              # 0x15
.LCPI0_128:
	.long	24                              # 0x18
	.long	27                              # 0x1b
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	4                               # 0x4
	.long	7                               # 0x7
	.long	10                              # 0xa
	.long	13                              # 0xd
.LCPI0_129:
	.long	16                              # 0x10
	.long	19                              # 0x13
	.long	22                              # 0x16
	.long	25                              # 0x19
	.long	28                              # 0x1c
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	5                               # 0x5
.LCPI0_130:
	.long	8                               # 0x8
	.long	11                              # 0xb
	.long	14                              # 0xe
	.long	17                              # 0x11
	.long	20                              # 0x14
	.long	23                              # 0x17
	.long	26                              # 0x1a
	.long	29                              # 0x1d
.LCPI0_131:
	.long	0                               # 0x0
	.long	2                               # 0x2
	.long	4                               # 0x4
	.long	6                               # 0x6
	.long	8                               # 0x8
	.long	10                              # 0xa
	.long	12                              # 0xc
	.long	14                              # 0xe
.LCPI0_133:
	.long	16                              # 0x10
	.long	18                              # 0x12
	.long	20                              # 0x14
	.long	22                              # 0x16
	.long	24                              # 0x18
	.long	26                              # 0x1a
	.long	28                              # 0x1c
	.long	30                              # 0x1e
.LCPI0_134:
	.long	0                               # 0x0
	.long	1                               # 0x1
	.long	2                               # 0x2
	.long	3                               # 0x3
	.long	4                               # 0x4
	.long	5                               # 0x5
	.long	6                               # 0x6
	.long	7                               # 0x7
.LCPI0_136:
	.long	24                              # 0x18
	.long	25                              # 0x19
	.long	26                              # 0x1a
	.long	27                              # 0x1b
	.long	28                              # 0x1c
	.long	29                              # 0x1d
	.long	30                              # 0x1e
	.long	31                              # 0x1f
	.section	.rodata.cst16,"aM",@progbits,16
	.p2align	4
.LCPI0_5:
	.long	8                               # 0x8
	.long	7                               # 0x7
	.long	6                               # 0x6
	.long	5                               # 0x5
.LCPI0_6:
	.long	24                              # 0x18
	.long	25                              # 0x19
	.long	26                              # 0x1a
	.long	27                              # 0x1b
.LCPI0_9:
	.long	16                              # 0x10
	.long	14                              # 0xe
	.long	12                              # 0xc
	.long	10                              # 0xa
.LCPI0_10:
	.long	16                              # 0x10
	.long	18                              # 0x12
	.long	20                              # 0x14
	.long	22                              # 0x16
.LCPI0_13:
	.long	8                               # 0x8
	.long	5                               # 0x5
	.zero	4
	.zero	4
.LCPI0_14:
	.long	24                              # 0x18
	.long	27                              # 0x1b
	.zero	4
	.zero	4
.LCPI0_16:
	.long	16                              # 0x10
	.long	13                              # 0xd
	.long	10                              # 0xa
	.long	7                               # 0x7
.LCPI0_17:
	.long	16                              # 0x10
	.long	19                              # 0x13
	.long	22                              # 0x16
	.long	25                              # 0x19
.LCPI0_19:
	.long	24                              # 0x18
	.long	21                              # 0x15
	.long	18                              # 0x12
	.long	15                              # 0xf
.LCPI0_20:
	.long	8                               # 0x8
	.long	11                              # 0xb
	.long	14                              # 0xe
	.long	17                              # 0x11
.LCPI0_26:
	.long	24                              # 0x18
	.long	19                              # 0x13
	.long	14                              # 0xe
	.long	9                               # 0x9
.LCPI0_27:
	.long	8                               # 0x8
	.long	13                              # 0xd
	.long	18                              # 0x12
	.long	23                              # 0x17
.LCPI0_29:
	.long	16                              # 0x10
	.long	11                              # 0xb
	.zero	4
	.zero	4
.LCPI0_30:
	.long	16                              # 0x10
	.long	21                              # 0x15
	.zero	4
	.zero	4
.LCPI0_40:
	.long	16                              # 0x10
	.long	9                               # 0x9
	.zero	4
	.zero	4
.LCPI0_41:
	.long	16                              # 0x10
	.long	23                              # 0x17
	.zero	4
	.zero	4
.LCPI0_43:
	.long	24                              # 0x18
	.long	17                              # 0x11
	.zero	4
	.zero	4
.LCPI0_44:
	.long	8                               # 0x8
	.long	15                              # 0xf
	.zero	4
	.zero	4
.LCPI0_46:
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	0                               # 0x0
	.long	8                               # 0x8
.LCPI0_50:
	.long	24                              # 0x18
	.long	15                              # 0xf
	.zero	4
	.zero	4
.LCPI0_51:
	.long	8                               # 0x8
	.long	17                              # 0x11
	.zero	4
	.zero	4
.LCPI0_62:
	.long	24                              # 0x18
	.long	13                              # 0xd
	.zero	4
	.zero	4
.LCPI0_63:
	.long	8                               # 0x8
	.long	19                              # 0x13
	.zero	4
	.zero	4
.LCPI0_109:
	.long	0                               # 0x0
	.long	8                               # 0x8
	.long	16                              # 0x10
	.long	24                              # 0x18
	.section	.rodata.cst4,"aM",@progbits,4
	.p2align	2
.LCPI0_47:
	.long	16777215                        # 0xffffff
.LCPI0_110:
	.long	255                             # 0xff
	.text
	.globl	unpack32_avx2
	.p2align	4, 0x90
	.type	unpack32_avx2,@function
unpack32_avx2:                          # @unpack32_avx2
# %bb.0:
	push	rbp
	mov	rbp, rsp
	push	r15
	push	r14
	push	r12
	push	rbx
	and	rsp, -16
                                        # kill: def $edx killed $edx def $rdx
	mov	r15, rsi
	mov	rbx, rdi
	lea	r14d, [rdx + 31]
	test	edx, edx
	cmovns	r14d, edx
	sar	r14d, 5
	cmp	ecx, 15
	jle	.LBB0_1
# %bb.48:
	cmp	ecx, 23
	jle	.LBB0_49
# %bb.72:
	cmp	ecx, 27
	jle	.LBB0_73
# %bb.84:
	cmp	ecx, 29
	jle	.LBB0_85
# %bb.90:
	cmp	ecx, 30
	je	.LBB0_99
# %bb.91:
	cmp	ecx, 31
	je	.LBB0_96
# %bb.92:
	cmp	ecx, 32
	jne	.LBB0_147
# %bb.93:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.94:
	mov	r12d, r14d
	.p2align	4, 0x90
.LBB0_95:                               # =>This Inner Loop Header: Depth=1
	mov	edx, 128
	mov	rdi, r15
	mov	rsi, rbx
	call	clib·_memcpy(SB)
	sub	rbx, -128
	sub	r15, -128
	add	r12, -1
	jne	.LBB0_95
	jmp	.LBB0_147
.LBB0_1:
	cmp	ecx, 7
	jg	.LBB0_25
# %bb.2:
	cmp	ecx, 3
	jg	.LBB0_14
# %bb.3:
	cmp	ecx, 1
	jg	.LBB0_9
# %bb.4:
	test	ecx, ecx
	je	.LBB0_144
# %bb.5:
	cmp	ecx, 1
	jne	.LBB0_147
# %bb.6:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.7:
	mov	eax, r14d
	add	r15, 96
	xor	ecx, ecx
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_135] # ymm0 = [4294967297,4294967297,4294967297,4294967297]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_134] # ymm1 = [0,1,2,3,4,5,6,7]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_2] # ymm2 = [8,9,10,11,12,13,14,15]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_4] # ymm3 = [16,17,18,19,20,21,22,23]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_136] # ymm4 = [24,25,26,27,28,29,30,31]
	.p2align	4, 0x90
.LBB0_8:                                # =>This Inner Loop Header: Depth=1
	vpbroadcastd	ymm5, dword ptr [rbx + 4*rcx]
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	vpbroadcastd	ymm5, dword ptr [rbx + 4*rcx]
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	vpbroadcastd	ymm5, dword ptr [rbx + 4*rcx]
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	vpbroadcastd	ymm5, dword ptr [rbx + 4*rcx]
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	add	rcx, 1
	sub	r15, -128
	cmp	rax, rcx
	jne	.LBB0_8
	jmp	.LBB0_147
.LBB0_49:
	cmp	ecx, 19
	jg	.LBB0_61
# %bb.50:
	cmp	ecx, 17
	jg	.LBB0_56
# %bb.51:
	cmp	ecx, 16
	je	.LBB0_120
# %bb.52:
	cmp	ecx, 17
	jne	.LBB0_147
# %bb.53:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.54:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 64
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_76] # ymm0 = [562945658585087,562945658585087,562945658585087,562945658585087]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_75] # ymm1 = [0,0,2,0,4,0,6,0]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_77] # ymm2 = [8,0,10,0,12,0,14,0]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_78] # ymm3 = [0,1,0,3,0,5,0,7]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_79] # ymm4 = [0,9,0,11,0,13,0,15]
	.p2align	4, 0x90
.LBB0_55:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 52]
	mov	r10d, dword ptr [rbx - 48]
	shld	r10d, ecx, 9
	mov	esi, dword ptr [rbx - 56]
	mov	edi, ecx
	shld	edi, esi, 11
	mov	r9d, dword ptr [rbx - 64]
	mov	edx, dword ptr [rbx - 60]
	mov	eax, edx
	shld	eax, r9d, 15
	vmovd	xmm5, esi
	shld	esi, edx, 13
	vpinsrd	xmm5, xmm5, edi, 1
	vpinsrd	xmm5, xmm5, ecx, 2
	vpinsrd	xmm5, xmm5, r10d, 3
	vmovd	xmm6, r9d
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, esi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	eax, dword ptr [rbx - 36]
	mov	r10d, dword ptr [rbx - 32]
	shld	r10d, eax, 1
	mov	edx, dword ptr [rbx - 40]
	mov	esi, eax
	shld	esi, edx, 3
	mov	r9d, dword ptr [rbx - 48]
	mov	ecx, dword ptr [rbx - 44]
	mov	edi, ecx
	shld	edi, r9d, 7
	vmovd	xmm5, edx
	shld	edx, ecx, 5
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, r10d, 3
	vmovd	xmm6, r9d
	vpinsrd	xmm6, xmm6, edi, 1
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	r9d, dword ptr [rbx - 16]
	mov	r11d, dword ptr [rbx - 20]
	mov	edx, r9d
	shld	edx, r11d, 10
	mov	r10d, dword ptr [rbx - 24]
	mov	edi, r11d
	shld	edi, r10d, 12
	mov	eax, dword ptr [rbx - 28]
	mov	esi, r10d
	shld	esi, eax, 14
	mov	ecx, dword ptr [rbx - 32]
	shrd	ecx, eax, 16
	vmovd	xmm5, edi
	vpinsrd	xmm5, xmm5, r11d, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm6, xmm6, esi, 2
	vpinsrd	xmm6, xmm6, r10d, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	r9d, dword ptr [rbx]
	mov	r11d, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, r11d, 2
	mov	r10d, dword ptr [rbx - 8]
	mov	edi, r11d
	shld	edi, r10d, 4
	mov	eax, dword ptr [rbx - 16]
	mov	esi, dword ptr [rbx - 12]
	mov	ecx, r10d
	shld	ecx, esi, 6
	shrd	eax, esi, 24
	vmovd	xmm5, edi
	vpinsrd	xmm5, xmm5, r11d, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, eax
	vpinsrd	xmm6, xmm6, esi, 1
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm6, xmm6, r10d, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 68
	add	r8, -1
	jne	.LBB0_55
	jmp	.LBB0_147
.LBB0_25:
	cmp	ecx, 11
	jg	.LBB0_37
# %bb.26:
	cmp	ecx, 9
	jg	.LBB0_32
# %bb.27:
	cmp	ecx, 8
	je	.LBB0_132
# %bb.28:
	cmp	ecx, 9
	jne	.LBB0_147
# %bb.29:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.30:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 32
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_105] # ymm0 = [2194728288767,2194728288767,2194728288767,2194728288767]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_104] # ymm1 = [0,9,18,0,4,13,22,0]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_106] # ymm2 = [8,17,0,3,12,21,0,7]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_107] # ymm3 = [16,0,2,11,20,0,6,15]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_108] # ymm4 = [0,1,10,19,0,5,14,23]
	.p2align	4, 0x90
.LBB0_31:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 32]
	mov	edx, dword ptr [rbx - 28]
	mov	esi, dword ptr [rbx - 24]
	shld	esi, edx, 1
	vmovd	xmm5, edx
	vpinsrd	xmm5, xmm5, edx, 1
	vpinsrd	xmm5, xmm5, edx, 2
	shld	edx, ecx, 5
	vpinsrd	xmm5, xmm5, esi, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	ecx, dword ptr [rbx - 16]
	mov	edx, dword ptr [rbx - 24]
	mov	esi, dword ptr [rbx - 20]
	mov	edi, ecx
	shld	edi, esi, 2
	mov	eax, esi
	shld	eax, edx, 6
	vmovd	xmm5, esi
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, edi, 2
	vpinsrd	xmm5, xmm5, ecx, 3
	vmovd	xmm6, edx
	vpinsrd	xmm6, xmm6, edx, 1
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm6, xmm6, esi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	eax, dword ptr [rbx - 8]
	mov	ecx, dword ptr [rbx - 16]
	mov	edx, dword ptr [rbx - 12]
	mov	esi, eax
	shld	esi, edx, 3
	mov	edi, edx
	shld	edi, ecx, 7
	vmovd	xmm5, edx
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, edi, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	eax, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 8]
	mov	edx, dword ptr [rbx - 4]
	mov	esi, eax
	shld	esi, edx, 4
	shrd	ecx, edx, 24
	vmovd	xmm5, esi
	vpinsrd	xmm5, xmm5, eax, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, edx, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 36
	add	r8, -1
	jne	.LBB0_31
	jmp	.LBB0_147
.LBB0_73:
	cmp	ecx, 25
	jg	.LBB0_79
# %bb.74:
	cmp	ecx, 24
	je	.LBB0_108
# %bb.75:
	cmp	ecx, 25
	jne	.LBB0_147
# %bb.76:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.77:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 96
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_38] # ymm0 = [144115183814443007,144115183814443007,144115183814443007,144115183814443007]
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI0_28] # ymm9 = [0,0,0,0,4,0,0,0]
	vmovdqa	ymm10, ymmword ptr [rip + .LCPI0_39] # ymm10 = [0,1,0,0,0,5,0,0]
	vmovdqa	xmm11, xmmword ptr [rip + .LCPI0_40] # xmm11 = <16,9,u,u>
	vmovdqa	xmm4, xmmword ptr [rip + .LCPI0_41] # xmm4 = <16,23,u,u>
	vmovdqa	ymm5, ymmword ptr [rip + .LCPI0_42] # ymm5 = [0,0,2,0,0,0,6,0]
	vmovdqa	xmm6, xmmword ptr [rip + .LCPI0_43] # xmm6 = <24,17,u,u>
	vmovdqa	xmm7, xmmword ptr [rip + .LCPI0_44] # xmm7 = <8,15,u,u>
	vmovdqa	ymm8, ymmword ptr [rip + .LCPI0_45] # ymm8 = [0,0,0,3,0,0,0,7]
	.p2align	4, 0x90
.LBB0_78:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 76]
	mov	r9d, dword ptr [rbx - 72]
	shld	r9d, ecx, 17
	mov	esi, dword ptr [rbx - 80]
	shld	ecx, esi, 10
	mov	edi, dword ptr [rbx - 84]
	shld	esi, edi, 3
	mov	eax, dword ptr [rbx - 88]
	vmovd	xmm1, edi
	shld	edi, eax, 21
	mov	r10d, dword ptr [rbx - 96]
	mov	edx, dword ptr [rbx - 92]
	shld	eax, edx, 14
	shld	edx, r10d, 7
	vpinsrd	xmm1, xmm1, esi, 1
	vmovd	xmm2, r10d
	vpinsrd	xmm1, xmm1, ecx, 2
	vpinsrd	xmm2, xmm2, edx, 1
	vpinsrd	xmm1, xmm1, r9d, 3
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm2, xmm2, edi, 3
	vinserti128	ymm1, ymm2, xmm1, 1
	vpsrlvd	ymm1, ymm1, ymm9
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm1
	mov	r11d, dword ptr [rbx - 52]
	mov	r9d, dword ptr [rbx - 48]
	shld	r9d, r11d, 9
	mov	r10d, dword ptr [rbx - 56]
	shld	r11d, r10d, 2
	mov	esi, dword ptr [rbx - 60]
	mov	edi, r10d
	mov	ecx, dword ptr [rbx - 64]
	shld	edi, esi, 20
	mov	edx, dword ptr [rbx - 72]
	mov	eax, dword ptr [rbx - 68]
	shld	esi, ecx, 13
	shrd	edx, eax, 8
	shld	ecx, eax, 6
	vmovd	xmm1, edi
	vpinsrd	xmm1, xmm1, r10d, 1
	vmovd	xmm2, edx
	vpinsrd	xmm1, xmm1, r11d, 2
	vpinsrd	xmm2, xmm2, eax, 1
	vpinsrd	xmm1, xmm1, r9d, 3
	vpinsrd	xmm2, xmm2, ecx, 2
	vpinsrd	xmm2, xmm2, esi, 3
	vinserti128	ymm1, ymm2, xmm1, 1
	vpsrlvd	ymm1, ymm1, ymm10
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm1
	mov	eax, dword ptr [rbx - 28]
	mov	r9d, dword ptr [rbx - 24]
	shld	r9d, eax, 1
	mov	edx, dword ptr [rbx - 32]
	mov	esi, eax
	shld	esi, edx, 19
	mov	edi, dword ptr [rbx - 40]
	mov	ecx, dword ptr [rbx - 36]
	shld	edx, ecx, 12
	shld	ecx, edi, 5
	vmovq	xmm1, qword ptr [rbx - 48]      # xmm1 = mem[0],zero
	vpsrlvd	xmm2, xmm1, xmm11
	vpshufd	xmm1, xmm1, 229                 # xmm1 = xmm1[1,1,2,3]
	vpinsrd	xmm1, xmm1, edi, 1
	vpsllvd	xmm1, xmm1, xmm4
	vpor	xmm1, xmm2, xmm1
	vmovd	xmm2, edx
	vpinsrd	xmm2, xmm2, esi, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm1, xmm1, edi, 2
	vpinsrd	xmm1, xmm1, ecx, 3
	vinserti128	ymm1, ymm1, xmm2, 1
	vpsrlvd	ymm1, ymm1, ymm5
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm1
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, ecx, 18
	mov	esi, dword ptr [rbx - 8]
	shld	ecx, esi, 11
	mov	r10d, dword ptr [rbx - 16]
	mov	edi, dword ptr [rbx - 12]
	shld	esi, edi, 4
	mov	eax, edi
	shld	eax, r10d, 22
	vmovq	xmm1, qword ptr [rbx - 24]      # xmm1 = mem[0],zero
	vpsrlvd	xmm2, xmm1, xmm6
	vpshufd	xmm1, xmm1, 229                 # xmm1 = xmm1[1,1,2,3]
	vpinsrd	xmm1, xmm1, r10d, 1
	vpsllvd	xmm1, xmm1, xmm7
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, ecx, 1
	vpor	xmm1, xmm2, xmm1
	vpinsrd	xmm2, xmm3, edx, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm1, xmm1, eax, 2
	vpinsrd	xmm1, xmm1, edi, 3
	vinserti128	ymm1, ymm1, xmm2, 1
	vpsrlvd	ymm1, ymm1, ymm8
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15], ymm1
	sub	r15, -128
	add	rbx, 100
	add	r8, -1
	jne	.LBB0_78
	jmp	.LBB0_147
.LBB0_14:
	cmp	ecx, 5
	jg	.LBB0_20
# %bb.15:
	cmp	ecx, 4
	je	.LBB0_138
# %bb.16:
	cmp	ecx, 5
	jne	.LBB0_147
# %bb.17:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.18:
	mov	eax, r14d
	add	r15, 96
	add	rbx, 16
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_120] # ymm0 = [133143986207,133143986207,133143986207,133143986207]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_119] # ymm1 = [0,5,10,15,20,25,0,3]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_121] # ymm2 = [8,13,18,23,0,1,6,11]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_122] # ymm3 = [16,21,26,0,4,9,14,19]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_123] # ymm4 = [24,0,2,7,12,17,22,27]
	.p2align	4, 0x90
.LBB0_19:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 16]
	mov	edx, dword ptr [rbx - 12]
	mov	esi, edx
	shld	esi, ecx, 2
	vmovd	xmm5, ecx
	vpbroadcastd	xmm6, xmm5
	vpinsrd	xmm5, xmm5, ecx, 1
	vpinsrd	xmm5, xmm5, esi, 2
	vpinsrd	xmm5, xmm5, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	ecx, dword ptr [rbx - 12]
	mov	edx, dword ptr [rbx - 8]
	mov	esi, edx
	shld	esi, ecx, 4
	vmovd	xmm5, ecx
	vpbroadcastd	xmm5, xmm5
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, edx, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	ecx, dword ptr [rbx - 8]
	mov	edx, dword ptr [rbx - 4]
	vmovd	xmm5, edx
	shld	edx, ecx, 1
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vpbroadcastd	xmm5, xmm5
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, dword ptr [rbx]
	mov	esi, edx
	shld	esi, ecx, 3
	vmovd	xmm5, ecx
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, edx, 3
	vmovd	xmm6, edx
	vpbroadcastd	xmm6, xmm6
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 20
	add	rax, -1
	jne	.LBB0_19
	jmp	.LBB0_147
.LBB0_61:
	cmp	ecx, 21
	jg	.LBB0_67
# %bb.62:
	cmp	ecx, 20
	je	.LBB0_114
# %bb.63:
	cmp	ecx, 21
	jne	.LBB0_147
# %bb.64:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.65:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 80
	vmovdqa	ymm8, ymmword ptr [rip + .LCPI0_58] # ymm8 = [0,0,10,0,0,9,0,0]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_59] # ymm1 = [9007194961870847,9007194961870847,9007194961870847,9007194961870847]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_60] # ymm2 = [8,0,0,7,0,0,6,0]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_61] # ymm3 = [0,5,0,0,4,0,0,3]
	vmovdqa	xmm4, xmmword ptr [rip + .LCPI0_62] # xmm4 = <24,13,u,u>
	vmovdqa	xmm5, xmmword ptr [rip + .LCPI0_63] # xmm5 = <8,19,u,u>
	vmovdqa	ymm6, ymmword ptr [rip + .LCPI0_64] # ymm6 = [0,0,2,0,0,1,0,11]
	.p2align	4, 0x90
.LBB0_66:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 64]
	mov	r9d, dword ptr [rbx - 60]
	shld	r9d, ecx, 13
	mov	r11d, dword ptr [rbx - 68]
	shld	ecx, r11d, 2
	mov	edi, dword ptr [rbx - 72]
	mov	esi, r11d
	shld	esi, edi, 12
	mov	r10d, dword ptr [rbx - 80]
	mov	eax, dword ptr [rbx - 76]
	shld	edi, eax, 1
	mov	edx, eax
	shld	edx, r10d, 11
	vmovd	xmm7, r10d
	vmovd	xmm0, esi
	vpinsrd	xmm7, xmm7, edx, 1
	vpinsrd	xmm0, xmm0, r11d, 1
	vpinsrd	xmm7, xmm7, eax, 2
	vpinsrd	xmm0, xmm0, ecx, 2
	vpinsrd	xmm7, xmm7, edi, 3
	vpinsrd	xmm0, xmm0, r9d, 3
	vinserti128	ymm0, ymm7, xmm0, 1
	vpsrlvd	ymm0, ymm0, ymm8
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm0
	mov	r10d, dword ptr [rbx - 44]
	mov	r9d, dword ptr [rbx - 40]
	shld	r9d, r10d, 5
	mov	edx, dword ptr [rbx - 48]
	mov	esi, r10d
	shld	esi, edx, 15
	mov	ecx, dword ptr [rbx - 52]
	shld	edx, ecx, 4
	mov	r11d, dword ptr [rbx - 60]
	mov	eax, dword ptr [rbx - 56]
	mov	edi, ecx
	shld	edi, eax, 14
	shld	eax, r11d, 3
	vmovd	xmm0, r11d
	vmovd	xmm7, edx
	vpinsrd	xmm0, xmm0, eax, 1
	vpinsrd	xmm7, xmm7, esi, 1
	vpinsrd	xmm0, xmm0, edi, 2
	vpinsrd	xmm7, xmm7, r10d, 2
	vpinsrd	xmm0, xmm0, ecx, 3
	vpinsrd	xmm7, xmm7, r9d, 3
	vinserti128	ymm0, ymm0, xmm7, 1
	vpsrlvd	ymm0, ymm0, ymm2
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm0
	mov	r9d, dword ptr [rbx - 20]
	mov	ecx, dword ptr [rbx - 24]
	mov	r10d, r9d
	shld	r10d, ecx, 18
	mov	esi, dword ptr [rbx - 28]
	shld	ecx, esi, 7
	mov	edi, dword ptr [rbx - 32]
	vmovd	xmm0, esi
	shld	esi, edi, 17
	mov	eax, dword ptr [rbx - 40]
	mov	edx, dword ptr [rbx - 36]
	shld	edi, edx, 6
	shrd	eax, edx, 16
	vpinsrd	xmm0, xmm0, ecx, 1
	vmovd	xmm7, eax
	vpinsrd	xmm0, xmm0, r10d, 2
	vpinsrd	xmm7, xmm7, edx, 1
	vpinsrd	xmm0, xmm0, r9d, 3
	vpinsrd	xmm7, xmm7, edi, 2
	vpinsrd	xmm7, xmm7, esi, 3
	vinserti128	ymm0, ymm7, xmm0, 1
	vpsrlvd	ymm0, ymm0, ymm3
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm0
	mov	r9d, dword ptr [rbx]
	mov	eax, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, eax, 10
	mov	esi, dword ptr [rbx - 12]
	mov	edi, dword ptr [rbx - 8]
	mov	ecx, eax
	shld	ecx, edi, 20
	shld	edi, esi, 9
	vmovq	xmm0, qword ptr [rbx - 20]      # xmm0 = mem[0],zero
	vpsrlvd	xmm7, xmm0, xmm4
	vpshufd	xmm0, xmm0, 229                 # xmm0 = xmm0[1,1,2,3]
	vpinsrd	xmm0, xmm0, esi, 1
	vpsllvd	xmm0, xmm0, xmm5
	vpor	xmm0, xmm7, xmm0
	vmovd	xmm7, ecx
	vpinsrd	xmm7, xmm7, eax, 1
	vpinsrd	xmm7, xmm7, edx, 2
	vpinsrd	xmm7, xmm7, r9d, 3
	vpinsrd	xmm0, xmm0, esi, 2
	vpinsrd	xmm0, xmm0, edi, 3
	vinserti128	ymm0, ymm0, xmm7, 1
	vpsrlvd	ymm0, ymm0, ymm6
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15], ymm0
	sub	r15, -128
	add	rbx, 84
	add	r8, -1
	jne	.LBB0_66
	jmp	.LBB0_147
.LBB0_37:
	cmp	ecx, 13
	jg	.LBB0_43
# %bb.38:
	cmp	ecx, 12
	je	.LBB0_126
# %bb.39:
	cmp	ecx, 13
	jne	.LBB0_147
# %bb.40:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.41:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 48
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_90] # ymm0 = [35180077129727,35180077129727,35180077129727,35180077129727]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_89] # ymm1 = [0,13,0,7,0,1,14,0]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_91] # ymm2 = [8,0,2,15,0,9,0,3]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_92] # ymm3 = [16,0,10,0,4,17,0,11]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_93] # ymm4 = [0,5,18,0,12,0,6,19]
	.p2align	4, 0x90
.LBB0_42:                               # =>This Inner Loop Header: Depth=1
	mov	eax, dword ptr [rbx - 40]
	mov	r9d, dword ptr [rbx - 36]
	shld	r9d, eax, 5
	mov	esi, dword ptr [rbx - 48]
	mov	edx, dword ptr [rbx - 44]
	mov	ecx, eax
	shld	ecx, edx, 12
	mov	edi, edx
	shld	edi, esi, 6
	vmovd	xmm5, ecx
	vpinsrd	xmm5, xmm5, eax, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, esi, 1
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	r9d, dword ptr [rbx - 24]
	mov	ecx, dword ptr [rbx - 28]
	mov	edx, r9d
	shld	edx, ecx, 10
	mov	esi, dword ptr [rbx - 32]
	mov	edi, ecx
	shld	edi, esi, 4
	mov	r10d, dword ptr [rbx - 36]
	mov	eax, esi
	shld	eax, r10d, 11
	vmovd	xmm5, edi
	vpinsrd	xmm5, xmm5, ecx, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, r10d
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm6, xmm6, esi, 2
	vpinsrd	xmm6, xmm6, esi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	r9d, dword ptr [rbx - 12]
	mov	ecx, dword ptr [rbx - 16]
	mov	edx, r9d
	shld	edx, ecx, 2
	mov	esi, dword ptr [rbx - 24]
	mov	eax, dword ptr [rbx - 20]
	vmovd	xmm5, ecx
	vpinsrd	xmm5, xmm5, ecx, 1
	shld	ecx, eax, 9
	mov	edi, eax
	shld	edi, esi, 3
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, edi, 1
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm6, xmm6, ecx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	eax, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, eax
	shld	edx, ecx, 7
	mov	esi, dword ptr [rbx - 8]
	vmovd	xmm5, ecx
	shld	ecx, esi, 1
	mov	edi, dword ptr [rbx - 12]
	shrd	edi, esi, 24
	vmovd	xmm6, edi
	vpinsrd	xmm6, xmm6, esi, 1
	vpinsrd	xmm6, xmm6, esi, 2
	vpinsrd	xmm6, xmm6, ecx, 3
	vpinsrd	xmm5, xmm5, edx, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 52
	add	r8, -1
	jne	.LBB0_42
	jmp	.LBB0_147
.LBB0_85:
	cmp	ecx, 28
	je	.LBB0_102
# %bb.86:
	cmp	ecx, 29
	jne	.LBB0_147
# %bb.87:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.88:
	mov	r8d, r14d
	add	r15, 96
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_12] # ymm0 = [2305843005455597567,2305843005455597567,2305843005455597567,2305843005455597567]
	vmovdqa	xmm8, xmmword ptr [rip + .LCPI0_13] # xmm8 = <8,5,u,u>
	vmovdqa	xmm10, xmmword ptr [rip + .LCPI0_14] # xmm10 = <24,27,u,u>
	vmovdqa	ymm11, ymmword ptr [rip + .LCPI0_15] # ymm11 = [0,0,2,0,0,0,0,0]
	vmovdqa	xmm12, xmmword ptr [rip + .LCPI0_16] # xmm12 = [16,13,10,7]
	vmovdqa	xmm5, xmmword ptr [rip + .LCPI0_17] # xmm5 = [16,19,22,25]
	vmovdqa	ymm6, ymmword ptr [rip + .LCPI0_18] # ymm6 = [0,0,0,0,0,1,0,0]
	vmovdqa	xmm7, xmmword ptr [rip + .LCPI0_19] # xmm7 = [24,21,18,15]
	vmovdqa	xmm1, xmmword ptr [rip + .LCPI0_20] # xmm1 = [8,11,14,17]
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI0_21] # ymm9 = [0,0,0,0,0,0,0,3]
	.p2align	4, 0x90
.LBB0_89:                               # =>This Inner Loop Header: Depth=1
	mov	r11d, dword ptr [rbx + 24]
	mov	r9d, dword ptr [rbx + 28]
	shld	r9d, r11d, 21
	mov	esi, dword ptr [rbx + 20]
	shld	r11d, esi, 18
	mov	edi, dword ptr [rbx + 16]
	shld	esi, edi, 15
	mov	eax, dword ptr [rbx + 12]
	shld	edi, eax, 12
	mov	edx, dword ptr [rbx + 8]
	shld	eax, edx, 9
	mov	r10d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx + 4]
	shld	edx, ecx, 6
	shld	ecx, r10d, 3
	vmovd	xmm2, r10d
	vmovd	xmm3, edi
	vpinsrd	xmm2, xmm2, ecx, 1
	vpinsrd	xmm3, xmm3, esi, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm3, xmm3, r11d, 2
	vpinsrd	xmm2, xmm2, eax, 3
	vpinsrd	xmm3, xmm3, r9d, 3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpand	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm2
	mov	eax, dword ptr [rbx + 52]
	mov	r9d, dword ptr [rbx + 56]
	shld	r9d, eax, 13
	mov	edx, dword ptr [rbx + 48]
	shld	eax, edx, 10
	mov	esi, dword ptr [rbx + 44]
	shld	edx, esi, 7
	mov	edi, dword ptr [rbx + 36]
	mov	ecx, dword ptr [rbx + 40]
	shld	esi, ecx, 4
	shld	ecx, edi, 1
	vmovq	xmm2, qword ptr [rbx + 28]      # xmm2 = mem[0],zero
	vpsrlvd	xmm3, xmm2, xmm8
	vpshufd	xmm2, xmm2, 229                 # xmm2 = xmm2[1,1,2,3]
	vpinsrd	xmm2, xmm2, edi, 1
	vpsllvd	xmm2, xmm2, xmm10
	vpor	xmm2, xmm3, xmm2
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, edx, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, r9d, 3
	vpinsrd	xmm2, xmm2, edi, 2
	vpinsrd	xmm2, xmm2, ecx, 3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsrlvd	ymm2, ymm2, ymm11
	vpand	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm2
	mov	eax, dword ptr [rbx + 80]
	mov	ecx, dword ptr [rbx + 84]
	shld	ecx, eax, 5
	mov	edx, dword ptr [rbx + 76]
	mov	esi, dword ptr [rbx + 72]
	shld	eax, edx, 2
	mov	edi, edx
	shld	edi, esi, 28
	vmovdqu	xmm2, xmmword ptr [rbx + 56]
	vpsrlvd	xmm3, xmm2, xmm12
	vpshufd	xmm2, xmm2, 249                 # xmm2 = xmm2[1,2,3,3]
	vpinsrd	xmm2, xmm2, esi, 3
	vmovd	xmm4, edi
	vpinsrd	xmm4, xmm4, edx, 1
	vpinsrd	xmm4, xmm4, eax, 2
	vpsllvd	xmm2, xmm2, xmm5
	vpinsrd	xmm4, xmm4, ecx, 3
	vpor	xmm2, xmm3, xmm2
	vinserti128	ymm2, ymm2, xmm4, 1
	vpsrlvd	ymm2, ymm2, ymm6
	vpand	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm2
	mov	eax, dword ptr [rbx + 112]
	mov	ecx, dword ptr [rbx + 108]
	mov	edx, eax
	shld	edx, ecx, 26
	mov	esi, dword ptr [rbx + 104]
	shld	ecx, esi, 23
	mov	edi, dword ptr [rbx + 100]
	vmovdqu	xmm2, xmmword ptr [rbx + 84]
	shld	esi, edi, 20
	vpsrlvd	xmm3, xmm2, xmm7
	vpshufd	xmm2, xmm2, 249                 # xmm2 = xmm2[1,2,3,3]
	vpinsrd	xmm2, xmm2, edi, 3
	vmovd	xmm4, esi
	vpinsrd	xmm4, xmm4, ecx, 1
	vpsllvd	xmm2, xmm2, xmm1
	vpinsrd	xmm4, xmm4, edx, 2
	vpinsrd	xmm4, xmm4, eax, 3
	vpor	xmm2, xmm3, xmm2
	vinserti128	ymm2, ymm2, xmm4, 1
	vpsrlvd	ymm2, ymm2, ymm9
	vpand	ymm2, ymm2, ymm0
	vmovdqu	ymmword ptr [r15], ymm2
	add	rbx, 116
	sub	r15, -128
	add	r8, -1
	jne	.LBB0_89
	jmp	.LBB0_147
.LBB0_9:
	cmp	ecx, 2
	je	.LBB0_141
# %bb.10:
	cmp	ecx, 3
	jne	.LBB0_147
# %bb.11:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.12:
	mov	eax, r14d
	add	r15, 96
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_127] # ymm0 = [30064771079,30064771079,30064771079,30064771079]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_126] # ymm1 = [0,3,6,9,12,15,18,21]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_128] # ymm2 = [24,27,0,1,4,7,10,13]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_129] # ymm3 = [16,19,22,25,28,0,2,5]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_130] # ymm4 = [8,11,14,17,20,23,26,29]
	.p2align	4, 0x90
.LBB0_13:                               # =>This Inner Loop Header: Depth=1
	vpbroadcastd	ymm5, dword ptr [rbx]
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	ecx, dword ptr [rbx]
	mov	edx, dword ptr [rbx + 4]
	mov	esi, edx
	shld	esi, ecx, 2
	vmovd	xmm5, ecx
	vpinsrd	xmm5, xmm5, ecx, 1
	vpinsrd	xmm5, xmm5, esi, 2
	vpinsrd	xmm5, xmm5, edx, 3
	vmovd	xmm6, edx
	vpbroadcastd	xmm6, xmm6
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	ecx, dword ptr [rbx + 4]
	mov	edx, dword ptr [rbx + 8]
	mov	esi, edx
	shld	esi, ecx, 1
	vmovd	xmm5, ecx
	vpbroadcastd	xmm6, xmm5
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	vpbroadcastd	ymm5, dword ptr [rbx + 8]
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 12
	add	rax, -1
	jne	.LBB0_13
	jmp	.LBB0_147
.LBB0_56:
	cmp	ecx, 18
	je	.LBB0_117
# %bb.57:
	cmp	ecx, 19
	jne	.LBB0_147
# %bb.58:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.59:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 72
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_68] # ymm0 = [2251795519242239,2251795519242239,2251795519242239,2251795519242239]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_67] # ymm1 = [0,0,6,0,12,0,0,5]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_69] # ymm2 = [0,11,0,0,4,0,10,0]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_70] # ymm3 = [0,3,0,9,0,0,2,0]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_71] # ymm4 = [8,0,0,1,0,7,0,13]
	.p2align	4, 0x90
.LBB0_60:                               # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 56]
	mov	edx, dword ptr [rbx - 60]
	mov	esi, r9d
	shld	esi, edx, 14
	mov	edi, dword ptr [rbx - 64]
	mov	r10d, dword ptr [rbx - 72]
	shld	edx, edi, 1
	mov	eax, dword ptr [rbx - 68]
	mov	ecx, eax
	shld	ecx, r10d, 13
	vmovd	xmm5, edi
	shld	edi, eax, 7
	vpinsrd	xmm5, xmm5, edx, 1
	vmovd	xmm6, r10d
	vpinsrd	xmm5, xmm5, esi, 2
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm5, xmm5, r9d, 3
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm6, xmm6, edi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	r10d, dword ptr [rbx - 40]
	mov	r9d, dword ptr [rbx - 36]
	shld	r9d, r10d, 3
	mov	edx, dword ptr [rbx - 44]
	mov	esi, r10d
	shld	esi, edx, 9
	mov	edi, dword ptr [rbx - 48]
	vmovd	xmm5, edx
	shld	edx, edi, 15
	mov	ecx, dword ptr [rbx - 56]
	mov	eax, dword ptr [rbx - 52]
	shld	edi, eax, 2
	shrd	ecx, eax, 24
	vpinsrd	xmm5, xmm5, esi, 1
	vmovd	xmm6, ecx
	vpinsrd	xmm5, xmm5, r10d, 2
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm5, xmm5, r9d, 3
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	r10d, dword ptr [rbx - 20]
	mov	r9d, dword ptr [rbx - 16]
	shld	r9d, r10d, 11
	mov	edx, dword ptr [rbx - 24]
	mov	esi, r10d
	mov	r11d, dword ptr [rbx - 28]
	shld	esi, edx, 17
	mov	ecx, dword ptr [rbx - 36]
	mov	eax, dword ptr [rbx - 32]
	shld	edx, r11d, 4
	mov	edi, r11d
	shld	edi, eax, 10
	shrd	ecx, eax, 16
	vmovd	xmm5, edx
	vpinsrd	xmm5, xmm5, esi, 1
	vmovd	xmm6, ecx
	vpinsrd	xmm5, xmm5, r10d, 2
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm5, xmm5, r9d, 3
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, r11d, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	r9d, dword ptr [rbx]
	mov	r11d, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, r11d, 6
	mov	ecx, dword ptr [rbx - 8]
	mov	edi, r11d
	shld	edi, ecx, 12
	mov	r10d, dword ptr [rbx - 16]
	mov	eax, dword ptr [rbx - 12]
	mov	esi, ecx
	shld	esi, eax, 18
	shld	eax, r10d, 5
	vmovd	xmm5, r10d
	vmovd	xmm6, edi
	vpinsrd	xmm5, xmm5, eax, 1
	vpinsrd	xmm6, xmm6, r11d, 1
	vpinsrd	xmm5, xmm5, esi, 2
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm5, xmm5, ecx, 3
	vpinsrd	xmm6, xmm6, r9d, 3
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 76
	add	r8, -1
	jne	.LBB0_60
	jmp	.LBB0_147
.LBB0_32:
	cmp	ecx, 10
	je	.LBB0_129
# %bb.33:
	cmp	ecx, 11
	jne	.LBB0_147
# %bb.34:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.35:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 40
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_97] # ymm0 = [8791798056959,8791798056959,8791798056959,8791798056959]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_96] # ymm1 = [0,11,0,1,12,0,2,13]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_98] # ymm2 = [0,3,14,0,4,15,0,5]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_99] # ymm3 = [16,0,6,17,0,7,18,0]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_100] # ymm4 = [8,19,0,9,20,0,10,21]
	.p2align	4, 0x90
.LBB0_36:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 32]
	mov	edx, dword ptr [rbx - 40]
	mov	esi, dword ptr [rbx - 36]
	mov	edi, ecx
	shld	edi, esi, 9
	mov	eax, esi
	shld	eax, edx, 10
	vmovd	xmm5, esi
	vpinsrd	xmm5, xmm5, edi, 1
	vpinsrd	xmm5, xmm5, ecx, 2
	vpinsrd	xmm5, xmm5, ecx, 3
	vmovd	xmm6, edx
	vpinsrd	xmm6, xmm6, edx, 1
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm6, xmm6, esi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	eax, dword ptr [rbx - 20]
	mov	ecx, dword ptr [rbx - 24]
	mov	edx, eax
	shld	edx, ecx, 6
	mov	esi, dword ptr [rbx - 32]
	mov	edi, dword ptr [rbx - 28]
	vmovd	xmm5, ecx
	vpinsrd	xmm5, xmm5, ecx, 1
	shld	ecx, edi, 7
	shrd	esi, edi, 24
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, edi, 1
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, ecx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	eax, dword ptr [rbx - 12]
	mov	ecx, dword ptr [rbx - 8]
	shld	ecx, eax, 3
	mov	r9d, dword ptr [rbx - 20]
	mov	esi, dword ptr [rbx - 16]
	mov	edi, eax
	shld	edi, esi, 4
	mov	edx, esi
	shld	edx, r9d, 5
	vmovd	xmm5, edi
	vpinsrd	xmm5, xmm5, eax, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, ecx, 3
	vmovd	xmm6, r9d
	vpinsrd	xmm6, xmm6, edx, 1
	vpinsrd	xmm6, xmm6, esi, 2
	vpinsrd	xmm6, xmm6, esi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	eax, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 8]
	mov	edx, dword ptr [rbx - 4]
	mov	esi, eax
	shld	esi, edx, 1
	mov	edi, edx
	shld	edi, ecx, 2
	vmovd	xmm5, edx
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 44
	add	r8, -1
	jne	.LBB0_36
	jmp	.LBB0_147
.LBB0_79:
	cmp	ecx, 26
	je	.LBB0_105
# %bb.80:
	cmp	ecx, 27
	jne	.LBB0_147
# %bb.81:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.82:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 104
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_25] # ymm0 = [576460748142673919,576460748142673919,576460748142673919,576460748142673919]
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI0_24] # ymm9 = [0,0,0,0,0,0,2,0]
	vmovdqa	xmm10, xmmword ptr [rip + .LCPI0_26] # xmm10 = [24,19,14,9]
	vmovdqa	xmm11, xmmword ptr [rip + .LCPI0_27] # xmm11 = [8,13,18,23]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_28] # ymm4 = [0,0,0,0,4,0,0,0]
	vmovdqa	xmm5, xmmword ptr [rip + .LCPI0_29] # xmm5 = <16,11,u,u>
	vmovdqa	xmm6, xmmword ptr [rip + .LCPI0_30] # xmm6 = <16,21,u,u>
	vmovdqa	ymm7, ymmword ptr [rip + .LCPI0_31] # ymm7 = [0,0,0,1,0,0,0,0]
	vmovdqa	ymm8, ymmword ptr [rip + .LCPI0_32] # ymm8 = [0,3,0,0,0,0,0,5]
	.p2align	4, 0x90
.LBB0_83:                               # =>This Inner Loop Header: Depth=1
	mov	r10d, dword ptr [rbx - 84]
	mov	r9d, dword ptr [rbx - 80]
	shld	r9d, r10d, 3
	mov	esi, dword ptr [rbx - 88]
	mov	edi, r10d
	shld	edi, esi, 25
	mov	eax, dword ptr [rbx - 92]
	shld	esi, eax, 20
	mov	edx, dword ptr [rbx - 96]
	shld	eax, edx, 15
	mov	r11d, dword ptr [rbx - 104]
	mov	ecx, dword ptr [rbx - 100]
	shld	edx, ecx, 10
	shld	ecx, r11d, 5
	vmovd	xmm1, r11d
	vmovd	xmm2, esi
	vpinsrd	xmm1, xmm1, ecx, 1
	vpinsrd	xmm2, xmm2, edi, 1
	vpinsrd	xmm1, xmm1, edx, 2
	vpinsrd	xmm2, xmm2, r10d, 2
	vpinsrd	xmm1, xmm1, eax, 3
	vpinsrd	xmm2, xmm2, r9d, 3
	vinserti128	ymm1, ymm1, xmm2, 1
	vpsrlvd	ymm1, ymm1, ymm9
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm1
	mov	eax, dword ptr [rbx - 56]
	mov	ecx, dword ptr [rbx - 52]
	shld	ecx, eax, 11
	mov	edx, dword ptr [rbx - 60]
	mov	esi, dword ptr [rbx - 64]
	shld	eax, edx, 6
	shld	edx, esi, 1
	vmovdqu	xmm1, xmmword ptr [rbx - 80]
	vpsrlvd	xmm2, xmm1, xmm10
	vpshufd	xmm1, xmm1, 249                 # xmm1 = xmm1[1,2,3,3]
	vmovd	xmm3, esi
	vpinsrd	xmm1, xmm1, esi, 3
	vpinsrd	xmm3, xmm3, edx, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpsllvd	xmm1, xmm1, xmm11
	vpinsrd	xmm3, xmm3, ecx, 3
	vpor	xmm1, xmm2, xmm1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpsrlvd	ymm1, ymm1, ymm4
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm1
	mov	eax, dword ptr [rbx - 28]
	mov	r9d, dword ptr [rbx - 24]
	shld	r9d, eax, 19
	mov	edx, dword ptr [rbx - 32]
	shld	eax, edx, 14
	mov	esi, dword ptr [rbx - 36]
	shld	edx, esi, 9
	mov	r10d, dword ptr [rbx - 44]
	mov	edi, dword ptr [rbx - 40]
	shld	esi, edi, 4
	mov	ecx, edi
	shld	ecx, r10d, 26
	vmovq	xmm1, qword ptr [rbx - 52]      # xmm1 = mem[0],zero
	vpsrlvd	xmm2, xmm1, xmm5
	vpshufd	xmm1, xmm1, 229                 # xmm1 = xmm1[1,1,2,3]
	vpinsrd	xmm1, xmm1, r10d, 1
	vpsllvd	xmm1, xmm1, xmm6
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, edx, 1
	vpor	xmm1, xmm2, xmm1
	vpinsrd	xmm2, xmm3, eax, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm1, xmm1, ecx, 2
	vpinsrd	xmm1, xmm1, edi, 3
	vinserti128	ymm1, ymm1, xmm2, 1
	vpsrlvd	ymm1, ymm1, ymm7
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm1
	mov	r9d, dword ptr [rbx]
	mov	r11d, dword ptr [rbx - 4]
	mov	r10d, r9d
	shld	r10d, r11d, 22
	mov	esi, dword ptr [rbx - 8]
	shld	r11d, esi, 17
	mov	edi, dword ptr [rbx - 12]
	mov	eax, dword ptr [rbx - 16]
	shld	esi, edi, 12
	mov	edx, dword ptr [rbx - 24]
	mov	ecx, dword ptr [rbx - 20]
	shld	edi, eax, 7
	shrd	edx, ecx, 8
	shld	eax, ecx, 2
	vmovd	xmm1, esi
	vpinsrd	xmm1, xmm1, r11d, 1
	vmovd	xmm2, edx
	vpinsrd	xmm1, xmm1, r10d, 2
	vpinsrd	xmm2, xmm2, ecx, 1
	vpinsrd	xmm1, xmm1, r9d, 3
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm2, xmm2, edi, 3
	vinserti128	ymm1, ymm2, xmm1, 1
	vpsrlvd	ymm1, ymm1, ymm8
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15], ymm1
	sub	r15, -128
	add	rbx, 108
	add	r8, -1
	jne	.LBB0_83
	jmp	.LBB0_147
.LBB0_20:
	cmp	ecx, 6
	je	.LBB0_135
# %bb.21:
	cmp	ecx, 7
	jne	.LBB0_147
# %bb.22:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.23:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 24
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_112] # ymm0 = [545460846719,545460846719,545460846719,545460846719]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_111] # ymm1 = [0,7,14,21,0,3,10,17]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_113] # ymm2 = [24,0,6,13,20,0,2,9]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_114] # ymm3 = [16,23,0,5,12,19,0,1]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_115] # ymm4 = [8,15,22,0,4,11,18,25]
	.p2align	4, 0x90
.LBB0_24:                               # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 24]
	mov	edx, dword ptr [rbx - 20]
	mov	esi, edx
	shld	esi, ecx, 4
	vmovd	xmm5, ecx
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, edx, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vpbroadcastd	xmm5, xmm5
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	ecx, dword ptr [rbx - 12]
	mov	edx, dword ptr [rbx - 20]
	mov	esi, dword ptr [rbx - 16]
	mov	edi, ecx
	shld	edi, esi, 5
	mov	eax, esi
	shld	eax, edx, 1
	vmovd	xmm5, esi
	vpinsrd	xmm5, xmm5, edi, 1
	vpinsrd	xmm5, xmm5, ecx, 2
	vpinsrd	xmm5, xmm5, ecx, 3
	vmovd	xmm6, edx
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm6, xmm6, esi, 2
	vpinsrd	xmm6, xmm6, esi, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	eax, dword ptr [rbx - 4]
	mov	ecx, dword ptr [rbx - 12]
	mov	edx, dword ptr [rbx - 8]
	mov	esi, eax
	shld	esi, edx, 6
	mov	edi, edx
	shld	edi, ecx, 2
	vmovd	xmm5, edx
	vpinsrd	xmm5, xmm5, edx, 1
	vpinsrd	xmm5, xmm5, esi, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	eax, dword ptr [rbx - 4]
	mov	ecx, dword ptr [rbx]
	mov	edx, ecx
	shld	edx, eax, 3
	vmovd	xmm5, ecx
	vmovd	xmm6, eax
	vpinsrd	xmm6, xmm6, eax, 1
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vpbroadcastd	xmm5, xmm5
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 28
	add	r8, -1
	jne	.LBB0_24
	jmp	.LBB0_147
.LBB0_67:
	cmp	ecx, 22
	je	.LBB0_111
# %bb.68:
	cmp	ecx, 23
	jne	.LBB0_147
# %bb.69:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.70:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 88
	vmovdqa	ymm8, ymmword ptr [rip + .LCPI0_48] # ymm8 = [0,0,0,5,0,0,0,1]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_49] # ymm1 = [36028792732385279,36028792732385279,36028792732385279,36028792732385279]
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI0_50] # xmm2 = <24,15,u,u>
	vmovdqa	xmm3, xmmword ptr [rip + .LCPI0_51] # xmm3 = <8,17,u,u>
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_52] # ymm4 = [0,0,6,0,0,0,2,0]
	vmovdqa	ymm5, ymmword ptr [rip + .LCPI0_53] # ymm5 = [0,7,0,0,0,3,0,0]
	vmovdqa	ymm6, ymmword ptr [rip + .LCPI0_54] # ymm6 = [8,0,0,0,4,0,0,9]
	.p2align	4, 0x90
.LBB0_71:                               # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 68]
	mov	edx, dword ptr [rbx - 72]
	mov	r11d, r9d
	shld	r11d, edx, 22
	mov	edi, dword ptr [rbx - 76]
	shld	edx, edi, 13
	mov	esi, dword ptr [rbx - 80]
	shld	edi, esi, 4
	mov	r10d, dword ptr [rbx - 88]
	mov	ecx, dword ptr [rbx - 84]
	mov	eax, esi
	shld	eax, ecx, 18
	shld	ecx, r10d, 9
	vmovd	xmm7, r10d
	vmovd	xmm0, edi
	vpinsrd	xmm7, xmm7, ecx, 1
	vpinsrd	xmm0, xmm0, edx, 1
	vpinsrd	xmm7, xmm7, eax, 2
	vpinsrd	xmm0, xmm0, r11d, 2
	vpinsrd	xmm7, xmm7, esi, 3
	vpinsrd	xmm0, xmm0, r9d, 3
	vinserti128	ymm0, ymm7, xmm0, 1
	vpsrlvd	ymm0, ymm0, ymm8
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm0
	mov	eax, dword ptr [rbx - 48]
	mov	r9d, dword ptr [rbx - 44]
	shld	r9d, eax, 7
	mov	edx, dword ptr [rbx - 52]
	mov	esi, eax
	shld	esi, edx, 21
	mov	edi, dword ptr [rbx - 60]
	mov	ecx, dword ptr [rbx - 56]
	shld	edx, ecx, 12
	shld	ecx, edi, 3
	vmovq	xmm0, qword ptr [rbx - 68]      # xmm0 = mem[0],zero
	vpsrlvd	xmm7, xmm0, xmm2
	vpshufd	xmm0, xmm0, 229                 # xmm0 = xmm0[1,1,2,3]
	vpinsrd	xmm0, xmm0, edi, 1
	vpsllvd	xmm0, xmm0, xmm3
	vpor	xmm0, xmm7, xmm0
	vmovd	xmm7, edx
	vpinsrd	xmm7, xmm7, esi, 1
	vpinsrd	xmm7, xmm7, eax, 2
	vpinsrd	xmm7, xmm7, r9d, 3
	vpinsrd	xmm0, xmm0, edi, 2
	vpinsrd	xmm0, xmm0, ecx, 3
	vinserti128	ymm0, ymm0, xmm7, 1
	vpsrlvd	ymm0, ymm0, ymm4
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm0
	mov	r11d, dword ptr [rbx - 24]
	mov	r9d, dword ptr [rbx - 20]
	shld	r9d, r11d, 15
	mov	r10d, dword ptr [rbx - 28]
	shld	r11d, r10d, 6
	mov	esi, dword ptr [rbx - 32]
	mov	edi, r10d
	mov	ecx, dword ptr [rbx - 36]
	shld	edi, esi, 20
	mov	edx, dword ptr [rbx - 44]
	mov	eax, dword ptr [rbx - 40]
	shld	esi, ecx, 11
	shrd	edx, eax, 16
	shld	ecx, eax, 2
	vmovd	xmm0, edi
	vpinsrd	xmm0, xmm0, r10d, 1
	vmovd	xmm7, edx
	vpinsrd	xmm0, xmm0, r11d, 2
	vpinsrd	xmm7, xmm7, eax, 1
	vpinsrd	xmm0, xmm0, r9d, 3
	vpinsrd	xmm7, xmm7, ecx, 2
	vpinsrd	xmm7, xmm7, esi, 3
	vinserti128	ymm0, ymm7, xmm0, 1
	vpsrlvd	ymm0, ymm0, ymm5
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm0
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, ecx, 14
	mov	esi, dword ptr [rbx - 8]
	shld	ecx, esi, 5
	mov	edi, dword ptr [rbx - 12]
	vmovd	xmm0, esi
	shld	esi, edi, 19
	mov	r10d, dword ptr [rbx - 20]
	mov	eax, dword ptr [rbx - 16]
	shld	edi, eax, 10
	shld	eax, r10d, 1
	vpinsrd	xmm0, xmm0, ecx, 1
	vmovd	xmm7, r10d
	vpinsrd	xmm0, xmm0, edx, 2
	vpinsrd	xmm7, xmm7, eax, 1
	vpinsrd	xmm0, xmm0, r9d, 3
	vpinsrd	xmm7, xmm7, edi, 2
	vpinsrd	xmm7, xmm7, esi, 3
	vinserti128	ymm0, ymm7, xmm0, 1
	vpsrlvd	ymm0, ymm0, ymm6
	vpand	ymm0, ymm0, ymm1
	vmovdqu	ymmword ptr [r15], ymm0
	sub	r15, -128
	add	rbx, 92
	add	r8, -1
	jne	.LBB0_71
	jmp	.LBB0_147
.LBB0_43:
	cmp	ecx, 14
	je	.LBB0_123
# %bb.44:
	cmp	ecx, 15
	jne	.LBB0_147
# %bb.45:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.46:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 56
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_82] # ymm0 = [140733193420799,140733193420799,140733193420799,140733193420799]
	vmovdqa	ymm1, ymmword ptr [rip + .LCPI0_81] # ymm1 = [0,15,0,13,0,11,0,9]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_83] # ymm2 = [0,7,0,5,0,3,0,1]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_84] # ymm3 = [16,0,14,0,12,0,10,0]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_85] # ymm4 = [8,0,6,0,4,0,2,17]
	.p2align	4, 0x90
.LBB0_47:                               # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 44]
	mov	eax, dword ptr [rbx - 48]
	mov	esi, r9d
	shld	esi, eax, 6
	mov	r10d, dword ptr [rbx - 52]
	mov	edx, eax
	shld	edx, r10d, 4
	mov	ecx, dword ptr [rbx - 56]
	mov	edi, r10d
	shld	edi, ecx, 2
	vmovd	xmm5, edx
	vpinsrd	xmm5, xmm5, eax, 1
	vpinsrd	xmm5, xmm5, esi, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, ecx
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, edi, 2
	vpinsrd	xmm6, xmm6, r10d, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm1
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	r9d, dword ptr [rbx - 28]
	mov	r11d, dword ptr [rbx - 32]
	mov	edx, r9d
	shld	edx, r11d, 14
	mov	r10d, dword ptr [rbx - 36]
	mov	edi, r11d
	shld	edi, r10d, 12
	mov	eax, dword ptr [rbx - 44]
	mov	esi, dword ptr [rbx - 40]
	mov	ecx, r10d
	shld	ecx, esi, 10
	shrd	eax, esi, 24
	vmovd	xmm5, edi
	vpinsrd	xmm5, xmm5, r11d, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vmovd	xmm6, eax
	vpinsrd	xmm6, xmm6, esi, 1
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm6, xmm6, r10d, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	eax, dword ptr [rbx - 16]
	mov	r10d, dword ptr [rbx - 12]
	shld	r10d, eax, 7
	mov	edx, dword ptr [rbx - 20]
	mov	esi, eax
	shld	esi, edx, 5
	mov	r9d, dword ptr [rbx - 28]
	mov	ecx, dword ptr [rbx - 24]
	mov	edi, ecx
	shld	edi, r9d, 1
	vmovd	xmm5, edx
	shld	edx, ecx, 3
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm5, xmm5, eax, 2
	vpinsrd	xmm5, xmm5, r10d, 3
	vmovd	xmm6, r9d
	vpinsrd	xmm6, xmm6, edi, 1
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm6, xmm6, edx, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm3
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, ecx, 13
	mov	eax, dword ptr [rbx - 8]
	vmovd	xmm5, ecx
	shld	ecx, eax, 11
	mov	edi, dword ptr [rbx - 12]
	mov	esi, eax
	shld	esi, edi, 9
	vmovd	xmm6, edi
	vpinsrd	xmm6, xmm6, esi, 1
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm6, xmm6, ecx, 3
	vpinsrd	xmm5, xmm5, edx, 1
	vpinsrd	xmm5, xmm5, r9d, 2
	vpinsrd	xmm5, xmm5, r9d, 3
	vinserti128	ymm5, ymm6, xmm5, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 60
	add	r8, -1
	jne	.LBB0_47
	jmp	.LBB0_147
.LBB0_96:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.97:
	mov	r8d, r14d
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_0] # ymm0 = [9223372034707292159,9223372034707292159,9223372034707292159,9223372034707292159]
	add	r15, 96
	vmovdqa	ymm8, ymmword ptr [rip + .LCPI0_1] # ymm8 = [24,23,22,21,20,19,18,17]
	vmovdqa	ymm9, ymmword ptr [rip + .LCPI0_2] # ymm9 = [8,9,10,11,12,13,14,15]
	vmovdqa	ymm10, ymmword ptr [rip + .LCPI0_3] # ymm10 = [16,15,14,13,12,11,10,9]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_4] # ymm4 = [16,17,18,19,20,21,22,23]
	vmovdqa	xmm5, xmmword ptr [rip + .LCPI0_5] # xmm5 = [8,7,6,5]
	vmovdqa	xmm6, xmmword ptr [rip + .LCPI0_6] # xmm6 = [24,25,26,27]
	vmovdqa	ymm7, ymmword ptr [rip + .LCPI0_7] # ymm7 = [0,0,0,0,0,0,0,1]
	.p2align	4, 0x90
.LBB0_98:                               # =>This Inner Loop Header: Depth=1
	mov	r10d, dword ptr [rbx + 24]
	mov	r9d, dword ptr [rbx + 28]
	shld	r9d, r10d, 7
	mov	esi, dword ptr [rbx + 20]
	shld	r10d, esi, 6
	mov	edi, dword ptr [rbx + 16]
	shld	esi, edi, 5
	mov	eax, dword ptr [rbx + 12]
	shld	edi, eax, 4
	mov	edx, dword ptr [rbx + 8]
	shld	eax, edx, 3
	mov	ecx, dword ptr [rbx + 4]
	shld	edx, ecx, 2
	mov	r11d, dword ptr [rbx]
	shld	ecx, r11d, 1
	vmovd	xmm1, edi
	vpinsrd	xmm1, xmm1, esi, 1
	vpinsrd	xmm1, xmm1, r10d, 2
	vpinsrd	xmm1, xmm1, r9d, 3
	vmovd	xmm2, r11d
	vpinsrd	xmm2, xmm2, ecx, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm2, xmm2, eax, 3
	vinserti128	ymm1, ymm2, xmm1, 1
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm1
	vmovdqu	ymm1, ymmword ptr [rbx + 28]
	vpsrlvd	ymm1, ymm1, ymm8
	vmovdqu	xmm2, xmmword ptr [rbx + 44]
	vpshufd	xmm3, xmm2, 249                 # xmm3 = xmm2[1,2,3,3]
	vpinsrd	xmm3, xmm3, dword ptr [rbx + 60], 3
	vpalignr	xmm2, xmm2, xmmword ptr [rbx + 28], 4 # xmm2 = mem[4,5,6,7,8,9,10,11,12,13,14,15],xmm2[0,1,2,3]
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsllvd	ymm2, ymm2, ymm9
	vpor	ymm1, ymm1, ymm2
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm1
	vmovdqu	ymm1, ymmword ptr [rbx + 60]
	vmovdqu	xmm2, xmmword ptr [rbx + 76]
	vpshufd	xmm3, xmm2, 249                 # xmm3 = xmm2[1,2,3,3]
	vpinsrd	xmm3, xmm3, dword ptr [rbx + 92], 3
	vpsrlvd	ymm1, ymm1, ymm10
	vpalignr	xmm2, xmm2, xmmword ptr [rbx + 60], 4 # xmm2 = mem[4,5,6,7,8,9,10,11,12,13,14,15],xmm2[0,1,2,3]
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsllvd	ymm2, ymm2, ymm4
	vpor	ymm1, ymm1, ymm2
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm1
	mov	eax, dword ptr [rbx + 120]
	mov	ecx, dword ptr [rbx + 116]
	mov	edx, eax
	shld	edx, ecx, 30
	mov	esi, dword ptr [rbx + 112]
	shld	ecx, esi, 29
	mov	edi, dword ptr [rbx + 108]
	shld	esi, edi, 28
	vmovdqu	xmm1, xmmword ptr [rbx + 92]
	vpsrlvd	xmm2, xmm1, xmm5
	vpshufd	xmm1, xmm1, 249                 # xmm1 = xmm1[1,2,3,3]
	vpinsrd	xmm1, xmm1, edi, 3
	vpsllvd	xmm1, xmm1, xmm6
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, edx, 2
	vpinsrd	xmm3, xmm3, eax, 3
	vpor	xmm1, xmm2, xmm1
	vinserti128	ymm1, ymm1, xmm3, 1
	vpsrlvd	ymm1, ymm1, ymm7
	vpand	ymm1, ymm1, ymm0
	vmovdqu	ymmword ptr [r15], ymm1
	add	rbx, 124
	sub	r15, -128
	add	r8, -1
	jne	.LBB0_98
	jmp	.LBB0_147
.LBB0_144:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.145:
	mov	ebx, r14d
	.p2align	4, 0x90
.LBB0_146:                              # =>This Inner Loop Header: Depth=1
	mov	edx, 128
	mov	rdi, r15
	xor	esi, esi
	call	clib·_memset(SB)
	sub	r15, -128
	add	rbx, -1
	jne	.LBB0_146
	jmp	.LBB0_147
.LBB0_120:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.121:
	mov	eax, r14d
	xor	ecx, ecx
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_80] # ymm0 = [68719476736,68719476736,68719476736,68719476736]
	vpxor	xmm1, xmm1, xmm1
	.p2align	4, 0x90
.LBB0_122:                              # =>This Inner Loop Header: Depth=1
	vmovdqu	xmm2, xmmword ptr [rbx + rcx]
	vpermq	ymm2, ymm2, 216                 # ymm2 = ymm2[0,2,1,3]
	vpshufd	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1,4,4,5,5]
	vpsrlvd	ymm2, ymm2, ymm0
	vpblendw	ymm2, ymm2, ymm1, 170           # ymm2 = ymm2[0],ymm1[1],ymm2[2],ymm1[3],ymm2[4],ymm1[5],ymm2[6],ymm1[7],ymm2[8],ymm1[9],ymm2[10],ymm1[11],ymm2[12],ymm1[13],ymm2[14],ymm1[15]
	vmovdqu	ymmword ptr [r15 + 2*rcx], ymm2
	vmovdqu	xmm2, xmmword ptr [rbx + rcx + 16]
	vpermq	ymm2, ymm2, 216                 # ymm2 = ymm2[0,2,1,3]
	vpshufd	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1,4,4,5,5]
	vpsrlvd	ymm2, ymm2, ymm0
	vpblendw	ymm2, ymm2, ymm1, 170           # ymm2 = ymm2[0],ymm1[1],ymm2[2],ymm1[3],ymm2[4],ymm1[5],ymm2[6],ymm1[7],ymm2[8],ymm1[9],ymm2[10],ymm1[11],ymm2[12],ymm1[13],ymm2[14],ymm1[15]
	vmovdqu	ymmword ptr [r15 + 2*rcx + 32], ymm2
	vmovdqu	xmm2, xmmword ptr [rbx + rcx + 32]
	vpermq	ymm2, ymm2, 216                 # ymm2 = ymm2[0,2,1,3]
	vpshufd	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1,4,4,5,5]
	vpsrlvd	ymm2, ymm2, ymm0
	vpblendw	ymm2, ymm2, ymm1, 170           # ymm2 = ymm2[0],ymm1[1],ymm2[2],ymm1[3],ymm2[4],ymm1[5],ymm2[6],ymm1[7],ymm2[8],ymm1[9],ymm2[10],ymm1[11],ymm2[12],ymm1[13],ymm2[14],ymm1[15]
	vmovdqu	ymmword ptr [r15 + 2*rcx + 64], ymm2
	vmovdqu	xmm2, xmmword ptr [rbx + rcx + 48]
	vpermq	ymm2, ymm2, 216                 # ymm2 = ymm2[0,2,1,3]
	vpshufd	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1,4,4,5,5]
	vpsrlvd	ymm2, ymm2, ymm0
	vpblendw	ymm2, ymm2, ymm1, 170           # ymm2 = ymm2[0],ymm1[1],ymm2[2],ymm1[3],ymm2[4],ymm1[5],ymm2[6],ymm1[7],ymm2[8],ymm1[9],ymm2[10],ymm1[11],ymm2[12],ymm1[13],ymm2[14],ymm1[15]
	vmovdqu	ymmword ptr [r15 + 2*rcx + 96], ymm2
	add	rcx, 64
	add	rax, -1
	jne	.LBB0_122
	jmp	.LBB0_147
.LBB0_132:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.133:
	mov	eax, r14d
	xor	ecx, ecx
	vbroadcasti128	ymm0, xmmword ptr [rip + .LCPI0_109] # ymm0 = [0,8,16,24,0,8,16,24]
                                        # ymm0 = mem[0,1,0,1]
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI0_110] # ymm1 = [255,255,255,255,255,255,255,255]
	.p2align	4, 0x90
.LBB0_134:                              # =>This Inner Loop Header: Depth=1
	vmovq	xmm2, qword ptr [rbx + rcx]     # xmm2 = mem[0],zero
	vpshufd	xmm2, xmm2, 80                  # xmm2 = xmm2[0,0,1,1]
	vpermq	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 4*rcx], ymm2
	vmovq	xmm2, qword ptr [rbx + rcx + 8] # xmm2 = mem[0],zero
	vpshufd	xmm2, xmm2, 80                  # xmm2 = xmm2[0,0,1,1]
	vpermq	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 4*rcx + 32], ymm2
	vmovq	xmm2, qword ptr [rbx + rcx + 16] # xmm2 = mem[0],zero
	vpshufd	xmm2, xmm2, 80                  # xmm2 = xmm2[0,0,1,1]
	vpermq	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 4*rcx + 64], ymm2
	vmovq	xmm2, qword ptr [rbx + rcx + 24] # xmm2 = mem[0],zero
	vpshufd	xmm2, xmm2, 80                  # xmm2 = xmm2[0,0,1,1]
	vpermq	ymm2, ymm2, 80                  # ymm2 = ymm2[0,0,1,1]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 4*rcx + 96], ymm2
	add	rcx, 32
	add	rax, -1
	jne	.LBB0_134
	jmp	.LBB0_147
.LBB0_108:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.109:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 92
	vbroadcasti128	ymm0, xmmword ptr [rip + .LCPI0_46] # ymm0 = [0,0,0,8,0,0,0,8]
                                        # ymm0 = mem[0,1,0,1]
	vpbroadcastd	ymm1, dword ptr [rip + .LCPI0_47] # ymm1 = [16777215,16777215,16777215,16777215,16777215,16777215,16777215,16777215]
	.p2align	4, 0x90
.LBB0_110:                              # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 72]
	mov	edx, dword ptr [rbx - 76]
	mov	esi, r9d
	mov	edi, dword ptr [rbx - 80]
	mov	r10d, dword ptr [rbx - 84]
	shld	esi, edx, 16
	mov	r11d, dword ptr [rbx - 92]
	mov	eax, dword ptr [rbx - 88]
	shld	edx, edi, 8
	mov	ecx, r10d
	shld	ecx, eax, 16
	shld	eax, r11d, 8
	vmovd	xmm2, edi
	vmovd	xmm3, r11d
	vpinsrd	xmm2, xmm2, edx, 1
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm2, xmm2, esi, 2
	vpinsrd	xmm3, xmm3, ecx, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm3, xmm3, r10d, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm2
	mov	r9d, dword ptr [rbx - 48]
	mov	ecx, dword ptr [rbx - 52]
	mov	edx, r9d
	mov	esi, dword ptr [rbx - 56]
	mov	r10d, dword ptr [rbx - 60]
	shld	edx, ecx, 16
	mov	r11d, dword ptr [rbx - 68]
	mov	edi, dword ptr [rbx - 64]
	shld	ecx, esi, 8
	mov	eax, r10d
	shld	eax, edi, 16
	shld	edi, r11d, 8
	vmovd	xmm2, esi
	vmovd	xmm3, r11d
	vpinsrd	xmm2, xmm2, ecx, 1
	vpinsrd	xmm3, xmm3, edi, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm3, xmm3, r10d, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm2
	mov	r9d, dword ptr [rbx - 24]
	mov	ecx, dword ptr [rbx - 28]
	mov	edx, r9d
	mov	esi, dword ptr [rbx - 32]
	mov	r10d, dword ptr [rbx - 36]
	shld	edx, ecx, 16
	mov	r11d, dword ptr [rbx - 44]
	mov	edi, dword ptr [rbx - 40]
	shld	ecx, esi, 8
	mov	eax, r10d
	shld	eax, edi, 16
	shld	edi, r11d, 8
	vmovd	xmm2, esi
	vmovd	xmm3, r11d
	vpinsrd	xmm2, xmm2, ecx, 1
	vpinsrd	xmm3, xmm3, edi, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm3, xmm3, r10d, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm2
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, r9d
	mov	esi, dword ptr [rbx - 8]
	mov	r10d, dword ptr [rbx - 12]
	shld	edx, ecx, 16
	mov	r11d, dword ptr [rbx - 20]
	mov	edi, dword ptr [rbx - 16]
	shld	ecx, esi, 8
	mov	eax, r10d
	shld	eax, edi, 16
	shld	edi, r11d, 8
	vmovd	xmm2, esi
	vpinsrd	xmm2, xmm2, ecx, 1
	vmovd	xmm3, r11d
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm3, xmm3, edi, 1
	vpinsrd	xmm2, xmm2, r9d, 3
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, r10d, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15], ymm2
	sub	r15, -128
	add	rbx, 96
	add	r8, -1
	jne	.LBB0_110
	jmp	.LBB0_147
.LBB0_138:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.139:
	mov	eax, r14d
	xor	ecx, ecx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_124] # ymm0 = [0,4,8,12,16,20,24,28]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_125] # ymm1 = [64424509455,64424509455,64424509455,64424509455]
	.p2align	4, 0x90
.LBB0_140:                              # =>This Inner Loop Header: Depth=1
	vpbroadcastd	ymm2, dword ptr [rbx + rcx]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 8*rcx], ymm2
	vpbroadcastd	ymm2, dword ptr [rbx + rcx + 4]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 8*rcx + 32], ymm2
	vpbroadcastd	ymm2, dword ptr [rbx + rcx + 8]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 8*rcx + 64], ymm2
	vpbroadcastd	ymm2, dword ptr [rbx + rcx + 12]
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 + 8*rcx + 96], ymm2
	add	rcx, 16
	add	rax, -1
	jne	.LBB0_140
	jmp	.LBB0_147
.LBB0_114:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.115:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 76
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_65] # ymm0 = [0,0,8,0,0,4,0,12]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_66] # ymm1 = [4503595333451775,4503595333451775,4503595333451775,4503595333451775]
	.p2align	4, 0x90
.LBB0_116:                              # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 60]
	mov	r11d, dword ptr [rbx - 64]
	mov	esi, r9d
	shld	esi, r11d, 8
	mov	edi, dword ptr [rbx - 68]
	mov	edx, r11d
	shld	edx, edi, 16
	mov	eax, dword ptr [rbx - 72]
	shld	edi, eax, 4
	mov	r10d, dword ptr [rbx - 76]
	mov	ecx, eax
	shld	ecx, r10d, 12
	vmovd	xmm2, edx
	vpinsrd	xmm2, xmm2, r11d, 1
	vpinsrd	xmm2, xmm2, esi, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vmovd	xmm3, r10d
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, edi, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm2
	mov	r9d, dword ptr [rbx - 40]
	mov	r11d, dword ptr [rbx - 44]
	mov	edx, r9d
	shld	edx, r11d, 8
	mov	esi, dword ptr [rbx - 48]
	mov	edi, r11d
	shld	edi, esi, 16
	mov	r10d, dword ptr [rbx - 56]
	mov	ecx, dword ptr [rbx - 52]
	shld	esi, ecx, 4
	mov	eax, ecx
	shld	eax, r10d, 12
	vmovd	xmm2, edi
	vpinsrd	xmm2, xmm2, r11d, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vmovd	xmm3, r10d
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm3, xmm3, ecx, 2
	vpinsrd	xmm3, xmm3, esi, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm2
	mov	r9d, dword ptr [rbx - 20]
	mov	r11d, dword ptr [rbx - 24]
	mov	edx, r9d
	shld	edx, r11d, 8
	mov	esi, dword ptr [rbx - 28]
	mov	edi, r11d
	shld	edi, esi, 16
	mov	ecx, dword ptr [rbx - 32]
	shld	esi, ecx, 4
	mov	r10d, dword ptr [rbx - 36]
	mov	eax, ecx
	shld	eax, r10d, 12
	vmovd	xmm2, edi
	vpinsrd	xmm2, xmm2, r11d, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vmovd	xmm3, r10d
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm3, xmm3, ecx, 2
	vpinsrd	xmm3, xmm3, esi, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm2
	mov	r9d, dword ptr [rbx]
	mov	r11d, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, r11d, 8
	mov	esi, dword ptr [rbx - 8]
	mov	edi, r11d
	shld	edi, esi, 16
	mov	r10d, dword ptr [rbx - 16]
	mov	ecx, dword ptr [rbx - 12]
	shld	esi, ecx, 4
	mov	eax, ecx
	shld	eax, r10d, 12
	vmovd	xmm2, edi
	vpinsrd	xmm2, xmm2, r11d, 1
	vpinsrd	xmm2, xmm2, edx, 2
	vpinsrd	xmm2, xmm2, r9d, 3
	vmovd	xmm3, r10d
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm3, xmm3, ecx, 2
	vpinsrd	xmm3, xmm3, esi, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15], ymm2
	sub	r15, -128
	add	rbx, 80
	add	r8, -1
	jne	.LBB0_116
	jmp	.LBB0_147
.LBB0_126:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.127:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 44
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_94] # ymm0 = [0,12,0,4,16,0,8,20]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_95] # ymm1 = [17587891081215,17587891081215,17587891081215,17587891081215]
	.p2align	4, 0x90
.LBB0_128:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 36]
	mov	edx, dword ptr [rbx - 44]
	mov	esi, dword ptr [rbx - 40]
	mov	edi, ecx
	shld	edi, esi, 4
	mov	eax, esi
	shld	eax, edx, 8
	vmovd	xmm2, esi
	vpinsrd	xmm2, xmm2, edi, 1
	vpinsrd	xmm2, xmm2, ecx, 2
	vpinsrd	xmm2, xmm2, ecx, 3
	vmovd	xmm3, edx
	vpinsrd	xmm3, xmm3, edx, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, esi, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm2
	mov	eax, dword ptr [rbx - 24]
	mov	ecx, dword ptr [rbx - 32]
	mov	edx, dword ptr [rbx - 28]
	mov	esi, eax
	shld	esi, edx, 4
	mov	edi, edx
	shld	edi, ecx, 8
	vmovd	xmm2, edx
	vpinsrd	xmm2, xmm2, esi, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm2, xmm2, eax, 3
	vmovd	xmm3, ecx
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, edi, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm2
	mov	eax, dword ptr [rbx - 12]
	mov	ecx, dword ptr [rbx - 20]
	mov	edx, dword ptr [rbx - 16]
	mov	esi, eax
	shld	esi, edx, 4
	mov	edi, edx
	shld	edi, ecx, 8
	vmovd	xmm2, edx
	vpinsrd	xmm2, xmm2, esi, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm2, xmm2, eax, 3
	vmovd	xmm3, ecx
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, edi, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm2
	mov	eax, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 8]
	mov	edx, dword ptr [rbx - 4]
	mov	esi, eax
	shld	esi, edx, 4
	mov	edi, edx
	shld	edi, ecx, 8
	vmovd	xmm2, edx
	vpinsrd	xmm2, xmm2, esi, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm2, xmm2, eax, 3
	vmovd	xmm3, ecx
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, edi, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vinserti128	ymm2, ymm3, xmm2, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15], ymm2
	sub	r15, -128
	add	rbx, 48
	add	r8, -1
	jne	.LBB0_128
	jmp	.LBB0_147
.LBB0_102:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.103:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 108
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_22] # ymm0 = [0,0,0,0,0,0,0,4]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_23] # ymm1 = [1152921500580315135,1152921500580315135,1152921500580315135,1152921500580315135]
	.p2align	4, 0x90
.LBB0_104:                              # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 84]
	mov	edx, dword ptr [rbx - 88]
	mov	r10d, r9d
	shld	r10d, edx, 24
	mov	edi, dword ptr [rbx - 92]
	shld	edx, edi, 20
	mov	eax, dword ptr [rbx - 96]
	shld	edi, eax, 16
	mov	ecx, dword ptr [rbx - 100]
	shld	eax, ecx, 12
	mov	r11d, dword ptr [rbx - 108]
	mov	esi, dword ptr [rbx - 104]
	shld	ecx, esi, 8
	shld	esi, r11d, 4
	vmovd	xmm2, r11d
	vmovd	xmm3, edi
	vpinsrd	xmm2, xmm2, esi, 1
	vpinsrd	xmm3, xmm3, edx, 1
	vpinsrd	xmm2, xmm2, ecx, 2
	vpinsrd	xmm3, xmm3, r10d, 2
	vpinsrd	xmm2, xmm2, eax, 3
	vpinsrd	xmm3, xmm3, r9d, 3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm2
	mov	r9d, dword ptr [rbx - 56]
	mov	ecx, dword ptr [rbx - 60]
	mov	r10d, r9d
	shld	r10d, ecx, 24
	mov	esi, dword ptr [rbx - 64]
	shld	ecx, esi, 20
	mov	edi, dword ptr [rbx - 68]
	shld	esi, edi, 16
	mov	eax, dword ptr [rbx - 72]
	shld	edi, eax, 12
	mov	r11d, dword ptr [rbx - 80]
	mov	edx, dword ptr [rbx - 76]
	shld	eax, edx, 8
	shld	edx, r11d, 4
	vmovd	xmm2, r11d
	vmovd	xmm3, esi
	vpinsrd	xmm2, xmm2, edx, 1
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm3, xmm3, r10d, 2
	vpinsrd	xmm2, xmm2, edi, 3
	vpinsrd	xmm3, xmm3, r9d, 3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm2
	mov	r9d, dword ptr [rbx - 28]
	mov	ecx, dword ptr [rbx - 32]
	mov	r10d, r9d
	shld	r10d, ecx, 24
	mov	esi, dword ptr [rbx - 36]
	shld	ecx, esi, 20
	mov	edi, dword ptr [rbx - 40]
	shld	esi, edi, 16
	mov	eax, dword ptr [rbx - 44]
	shld	edi, eax, 12
	mov	r11d, dword ptr [rbx - 52]
	mov	edx, dword ptr [rbx - 48]
	shld	eax, edx, 8
	shld	edx, r11d, 4
	vmovd	xmm2, r11d
	vmovd	xmm3, esi
	vpinsrd	xmm2, xmm2, edx, 1
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm3, xmm3, r10d, 2
	vpinsrd	xmm2, xmm2, edi, 3
	vpinsrd	xmm3, xmm3, r9d, 3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm2
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	r10d, r9d
	shld	r10d, ecx, 24
	mov	esi, dword ptr [rbx - 8]
	shld	ecx, esi, 20
	mov	edi, dword ptr [rbx - 12]
	shld	esi, edi, 16
	mov	eax, dword ptr [rbx - 16]
	shld	edi, eax, 12
	mov	r11d, dword ptr [rbx - 24]
	mov	edx, dword ptr [rbx - 20]
	shld	eax, edx, 8
	shld	edx, r11d, 4
	vmovd	xmm2, r11d
	vmovd	xmm3, esi
	vpinsrd	xmm2, xmm2, edx, 1
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm2, xmm2, eax, 2
	vpinsrd	xmm3, xmm3, r10d, 2
	vpinsrd	xmm2, xmm2, edi, 3
	vpinsrd	xmm3, xmm3, r9d, 3
	vinserti128	ymm2, ymm2, xmm3, 1
	vpsrlvd	ymm2, ymm2, ymm0
	vpand	ymm2, ymm2, ymm1
	vmovdqu	ymmword ptr [r15], ymm2
	sub	r15, -128
	add	rbx, 112
	add	r8, -1
	jne	.LBB0_104
	jmp	.LBB0_147
.LBB0_141:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.142:
	mov	eax, r14d
	add	r15, 96
	xor	ecx, ecx
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_131] # ymm0 = [0,2,4,6,8,10,12,14]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_132] # ymm1 = [12884901891,12884901891,12884901891,12884901891]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_133] # ymm2 = [16,18,20,22,24,26,28,30]
	.p2align	4, 0x90
.LBB0_143:                              # =>This Inner Loop Header: Depth=1
	vpbroadcastd	ymm3, dword ptr [rbx + 8*rcx]
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm3
	vpbroadcastd	ymm3, dword ptr [rbx + 8*rcx]
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm3
	vpbroadcastd	ymm3, dword ptr [rbx + 8*rcx + 4]
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm3
	vpbroadcastd	ymm3, dword ptr [rbx + 8*rcx + 4]
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15], ymm3
	add	rcx, 1
	sub	r15, -128
	cmp	rax, rcx
	jne	.LBB0_143
	jmp	.LBB0_147
.LBB0_117:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.118:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 68
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_72] # ymm0 = [0,0,4,0,8,0,12,0]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_73] # ymm1 = [1125895612137471,1125895612137471,1125895612137471,1125895612137471]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_74] # ymm2 = [0,2,0,6,0,10,0,14]
	.p2align	4, 0x90
.LBB0_119:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 56]
	mov	r10d, dword ptr [rbx - 52]
	shld	r10d, ecx, 2
	mov	esi, dword ptr [rbx - 60]
	mov	edi, ecx
	shld	edi, esi, 6
	mov	r9d, dword ptr [rbx - 68]
	mov	edx, dword ptr [rbx - 64]
	mov	eax, edx
	shld	eax, r9d, 14
	vmovd	xmm3, esi
	shld	esi, edx, 10
	vpinsrd	xmm3, xmm3, edi, 1
	vpinsrd	xmm3, xmm3, ecx, 2
	vpinsrd	xmm3, xmm3, r10d, 3
	vmovd	xmm4, r9d
	vpinsrd	xmm4, xmm4, eax, 1
	vpinsrd	xmm4, xmm4, edx, 2
	vpinsrd	xmm4, xmm4, esi, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm3
	mov	r9d, dword ptr [rbx - 36]
	mov	r11d, dword ptr [rbx - 40]
	mov	edx, r9d
	shld	edx, r11d, 4
	mov	r10d, dword ptr [rbx - 44]
	mov	edi, r11d
	shld	edi, r10d, 8
	mov	eax, dword ptr [rbx - 52]
	mov	esi, dword ptr [rbx - 48]
	mov	ecx, r10d
	shld	ecx, esi, 12
	shrd	eax, esi, 16
	vmovd	xmm3, edi
	vpinsrd	xmm3, xmm3, r11d, 1
	vpinsrd	xmm3, xmm3, edx, 2
	vpinsrd	xmm3, xmm3, r9d, 3
	vmovd	xmm4, eax
	vpinsrd	xmm4, xmm4, esi, 1
	vpinsrd	xmm4, xmm4, ecx, 2
	vpinsrd	xmm4, xmm4, r10d, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm3
	mov	eax, dword ptr [rbx - 20]
	mov	r10d, dword ptr [rbx - 16]
	shld	r10d, eax, 2
	mov	edx, dword ptr [rbx - 24]
	mov	esi, eax
	shld	esi, edx, 6
	mov	r9d, dword ptr [rbx - 32]
	mov	ecx, dword ptr [rbx - 28]
	mov	edi, ecx
	shld	edi, r9d, 14
	vmovd	xmm3, edx
	shld	edx, ecx, 10
	vpinsrd	xmm3, xmm3, esi, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, r10d, 3
	vmovd	xmm4, r9d
	vpinsrd	xmm4, xmm4, edi, 1
	vpinsrd	xmm4, xmm4, ecx, 2
	vpinsrd	xmm4, xmm4, edx, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm3
	mov	r9d, dword ptr [rbx]
	mov	r11d, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, r11d, 4
	mov	r10d, dword ptr [rbx - 8]
	mov	edi, r11d
	shld	edi, r10d, 8
	mov	eax, dword ptr [rbx - 16]
	mov	esi, dword ptr [rbx - 12]
	mov	ecx, r10d
	shld	ecx, esi, 12
	shrd	eax, esi, 16
	vmovd	xmm3, edi
	vpinsrd	xmm3, xmm3, r11d, 1
	vpinsrd	xmm3, xmm3, edx, 2
	vpinsrd	xmm3, xmm3, r9d, 3
	vmovd	xmm4, eax
	vpinsrd	xmm4, xmm4, esi, 1
	vpinsrd	xmm4, xmm4, ecx, 2
	vpinsrd	xmm4, xmm4, r10d, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15], ymm3
	sub	r15, -128
	add	rbx, 72
	add	r8, -1
	jne	.LBB0_119
	jmp	.LBB0_147
.LBB0_129:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.130:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 36
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_101] # ymm0 = [0,10,20,0,8,18,0,6]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_102] # ymm1 = [4393751544831,4393751544831,4393751544831,4393751544831]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_103] # ymm2 = [16,0,4,14,0,2,12,22]
	.p2align	4, 0x90
.LBB0_131:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 28]
	mov	edx, dword ptr [rbx - 36]
	mov	esi, dword ptr [rbx - 32]
	mov	edi, ecx
	shld	edi, esi, 4
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, esi, 1
	shld	esi, edx, 2
	vpinsrd	xmm3, xmm3, edi, 2
	vpinsrd	xmm3, xmm3, ecx, 3
	vmovd	xmm4, edx
	vpinsrd	xmm4, xmm4, edx, 1
	vpinsrd	xmm4, xmm4, edx, 2
	vpinsrd	xmm4, xmm4, esi, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm3
	mov	ecx, dword ptr [rbx - 20]
	mov	edx, dword ptr [rbx - 24]
	mov	esi, ecx
	shld	esi, edx, 8
	mov	edi, dword ptr [rbx - 28]
	mov	eax, edx
	shld	eax, edi, 6
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, ecx, 2
	vpinsrd	xmm3, xmm3, ecx, 3
	vmovd	xmm4, edi
	vpinsrd	xmm4, xmm4, eax, 1
	vpinsrd	xmm4, xmm4, edx, 2
	vpinsrd	xmm4, xmm4, edx, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm3
	mov	eax, dword ptr [rbx - 8]
	mov	ecx, dword ptr [rbx - 16]
	mov	edx, dword ptr [rbx - 12]
	mov	esi, eax
	shld	esi, edx, 4
	vmovd	xmm3, edx
	vpinsrd	xmm3, xmm3, edx, 1
	shld	edx, ecx, 2
	vpinsrd	xmm3, xmm3, esi, 2
	vpinsrd	xmm3, xmm3, eax, 3
	vmovd	xmm4, ecx
	vpinsrd	xmm4, xmm4, ecx, 1
	vpinsrd	xmm4, xmm4, ecx, 2
	vpinsrd	xmm4, xmm4, edx, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm3
	mov	eax, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 8]
	mov	edx, dword ptr [rbx - 4]
	mov	esi, eax
	shld	esi, edx, 8
	mov	edi, edx
	shld	edi, ecx, 6
	vmovd	xmm3, esi
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, eax, 3
	vmovd	xmm4, ecx
	vpinsrd	xmm4, xmm4, edi, 1
	vpinsrd	xmm4, xmm4, edx, 2
	vpinsrd	xmm4, xmm4, edx, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15], ymm3
	sub	r15, -128
	add	rbx, 40
	add	r8, -1
	jne	.LBB0_131
	jmp	.LBB0_147
.LBB0_105:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.106:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 100
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_34] # ymm0 = [288230371923853311,288230371923853311,288230371923853311,288230371923853311]
	vpbroadcastq	xmm1, qword ptr [rip + .LCPI0_35] # xmm1 = [42949672976,42949672976]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_33] # ymm2 = [0,0,0,0,0,2,0,0]
	vpbroadcastq	xmm3, qword ptr [rip + .LCPI0_36] # xmm3 = [94489280528,94489280528]
	vmovdqa	ymm4, ymmword ptr [rip + .LCPI0_37] # ymm4 = [0,0,4,0,0,0,0,6]
	.p2align	4, 0x90
.LBB0_107:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 80]
	mov	r9d, dword ptr [rbx - 76]
	shld	r9d, ecx, 10
	mov	r11d, dword ptr [rbx - 84]
	shld	ecx, r11d, 4
	mov	edi, dword ptr [rbx - 88]
	mov	esi, r11d
	shld	esi, edi, 24
	mov	edx, dword ptr [rbx - 92]
	shld	edi, edx, 18
	mov	r10d, dword ptr [rbx - 100]
	mov	eax, dword ptr [rbx - 96]
	shld	edx, eax, 12
	shld	eax, r10d, 6
	vmovd	xmm5, r10d
	vmovd	xmm6, esi
	vpinsrd	xmm5, xmm5, eax, 1
	vpinsrd	xmm6, xmm6, r11d, 1
	vpinsrd	xmm5, xmm5, edx, 2
	vpinsrd	xmm6, xmm6, ecx, 2
	vpinsrd	xmm5, xmm5, edi, 3
	vpinsrd	xmm6, xmm6, r9d, 3
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm5
	mov	r9d, dword ptr [rbx - 52]
	mov	ecx, dword ptr [rbx - 56]
	mov	edx, r9d
	shld	edx, ecx, 20
	mov	esi, dword ptr [rbx - 60]
	shld	ecx, esi, 14
	mov	edi, dword ptr [rbx - 68]
	mov	eax, dword ptr [rbx - 64]
	shld	esi, eax, 8
	shld	eax, edi, 2
	vmovq	xmm5, qword ptr [rbx - 76]      # xmm5 = mem[0],zero
	vpsrlvd	xmm6, xmm5, xmm1
	vpshufd	xmm5, xmm5, 229                 # xmm5 = xmm5[1,1,2,3]
	vpinsrd	xmm5, xmm5, edi, 1
	vpsllvd	xmm5, xmm5, xmm3
	vpor	xmm5, xmm6, xmm5
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, r9d, 3
	vpinsrd	xmm5, xmm5, edi, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm5
	mov	eax, dword ptr [rbx - 28]
	mov	r9d, dword ptr [rbx - 24]
	shld	r9d, eax, 10
	mov	r11d, dword ptr [rbx - 32]
	shld	eax, r11d, 4
	mov	esi, dword ptr [rbx - 36]
	mov	edi, r11d
	shld	edi, esi, 24
	mov	ecx, dword ptr [rbx - 40]
	shld	esi, ecx, 18
	mov	r10d, dword ptr [rbx - 48]
	mov	edx, dword ptr [rbx - 44]
	shld	ecx, edx, 12
	shld	edx, r10d, 6
	vmovd	xmm5, r10d
	vmovd	xmm6, edi
	vpinsrd	xmm5, xmm5, edx, 1
	vpinsrd	xmm6, xmm6, r11d, 1
	vpinsrd	xmm5, xmm5, ecx, 2
	vpinsrd	xmm6, xmm6, eax, 2
	vpinsrd	xmm5, xmm5, esi, 3
	vpinsrd	xmm6, xmm6, r9d, 3
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm2
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm5
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, ecx, 20
	mov	esi, dword ptr [rbx - 8]
	shld	ecx, esi, 14
	mov	edi, dword ptr [rbx - 16]
	mov	eax, dword ptr [rbx - 12]
	shld	esi, eax, 8
	shld	eax, edi, 2
	vmovq	xmm5, qword ptr [rbx - 24]      # xmm5 = mem[0],zero
	vpsrlvd	xmm6, xmm5, xmm1
	vpshufd	xmm5, xmm5, 229                 # xmm5 = xmm5[1,1,2,3]
	vpinsrd	xmm5, xmm5, edi, 1
	vpsllvd	xmm5, xmm5, xmm3
	vpor	xmm5, xmm6, xmm5
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, r9d, 3
	vpinsrd	xmm5, xmm5, edi, 2
	vpinsrd	xmm5, xmm5, eax, 3
	vinserti128	ymm5, ymm5, xmm6, 1
	vpsrlvd	ymm5, ymm5, ymm4
	vpand	ymm5, ymm5, ymm0
	vmovdqu	ymmword ptr [r15], ymm5
	sub	r15, -128
	add	rbx, 104
	add	r8, -1
	jne	.LBB0_107
	jmp	.LBB0_147
.LBB0_135:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.136:
	mov	eax, r14d
	add	r15, 96
	add	rbx, 20
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_116] # ymm0 = [0,6,12,18,24,0,4,10]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_117] # ymm1 = [270582939711,270582939711,270582939711,270582939711]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_118] # ymm2 = [16,22,0,2,8,14,20,26]
	.p2align	4, 0x90
.LBB0_137:                              # =>This Inner Loop Header: Depth=1
	mov	ecx, dword ptr [rbx - 20]
	mov	edx, dword ptr [rbx - 16]
	mov	esi, edx
	shld	esi, ecx, 2
	vmovd	xmm3, ecx
	vpbroadcastd	xmm4, xmm3
	vpinsrd	xmm3, xmm3, esi, 1
	vpinsrd	xmm3, xmm3, edx, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm3
	mov	ecx, dword ptr [rbx - 16]
	mov	edx, dword ptr [rbx - 12]
	mov	esi, edx
	shld	esi, ecx, 4
	vmovd	xmm3, ecx
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, esi, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vmovd	xmm4, edx
	vpbroadcastd	xmm4, xmm4
	vinserti128	ymm3, ymm3, xmm4, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm3
	mov	ecx, dword ptr [rbx - 8]
	mov	edx, dword ptr [rbx - 4]
	mov	esi, edx
	shld	esi, ecx, 2
	vmovd	xmm3, ecx
	vpinsrd	xmm4, xmm3, esi, 1
	vpinsrd	xmm4, xmm4, edx, 2
	vpbroadcastd	xmm3, xmm3
	vpinsrd	xmm4, xmm4, edx, 3
	vinserti128	ymm3, ymm3, xmm4, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm3
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, dword ptr [rbx]
	mov	esi, edx
	shld	esi, ecx, 4
	vmovd	xmm3, ecx
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, esi, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vmovd	xmm4, edx
	vpbroadcastd	xmm4, xmm4
	vinserti128	ymm3, ymm3, xmm4, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15], ymm3
	sub	r15, -128
	add	rbx, 24
	add	rax, -1
	jne	.LBB0_137
	jmp	.LBB0_147
.LBB0_111:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.112:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 84
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_55] # ymm0 = [0,0,0,2,0,0,4,0]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_56] # ymm1 = [18014394218708991,18014394218708991,18014394218708991,18014394218708991]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_57] # ymm2 = [0,6,0,0,8,0,0,10]
	.p2align	4, 0x90
.LBB0_113:                              # =>This Inner Loop Header: Depth=1
	mov	r10d, dword ptr [rbx - 68]
	mov	r9d, dword ptr [rbx - 64]
	shld	r9d, r10d, 6
	mov	esi, dword ptr [rbx - 72]
	mov	edi, r10d
	shld	edi, esi, 18
	mov	edx, dword ptr [rbx - 76]
	shld	esi, edx, 8
	mov	r11d, dword ptr [rbx - 84]
	mov	ecx, dword ptr [rbx - 80]
	mov	eax, edx
	shld	eax, ecx, 20
	shld	ecx, r11d, 10
	vmovd	xmm3, r11d
	vmovd	xmm4, esi
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm4, xmm4, edi, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm4, xmm4, r10d, 2
	vpinsrd	xmm3, xmm3, edx, 3
	vpinsrd	xmm4, xmm4, r9d, 3
	vinserti128	ymm3, ymm3, xmm4, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm3
	mov	r9d, dword ptr [rbx - 44]
	mov	ecx, dword ptr [rbx - 48]
	mov	r10d, r9d
	shld	r10d, ecx, 12
	mov	esi, dword ptr [rbx - 52]
	shld	ecx, esi, 2
	mov	edi, dword ptr [rbx - 56]
	vmovd	xmm3, esi
	shld	esi, edi, 14
	mov	eax, dword ptr [rbx - 64]
	mov	edx, dword ptr [rbx - 60]
	shld	edi, edx, 4
	shrd	eax, edx, 16
	vpinsrd	xmm3, xmm3, ecx, 1
	vmovd	xmm4, eax
	vpinsrd	xmm3, xmm3, r10d, 2
	vpinsrd	xmm4, xmm4, edx, 1
	vpinsrd	xmm3, xmm3, r9d, 3
	vpinsrd	xmm4, xmm4, edi, 2
	vpinsrd	xmm4, xmm4, esi, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm3
	mov	r10d, dword ptr [rbx - 24]
	mov	r9d, dword ptr [rbx - 20]
	shld	r9d, r10d, 6
	mov	edx, dword ptr [rbx - 28]
	mov	esi, r10d
	shld	esi, edx, 18
	mov	ecx, dword ptr [rbx - 32]
	shld	edx, ecx, 8
	mov	r11d, dword ptr [rbx - 40]
	mov	eax, dword ptr [rbx - 36]
	mov	edi, ecx
	shld	edi, eax, 20
	shld	eax, r11d, 10
	vmovd	xmm3, r11d
	vmovd	xmm4, edx
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm4, xmm4, esi, 1
	vpinsrd	xmm3, xmm3, edi, 2
	vpinsrd	xmm4, xmm4, r10d, 2
	vpinsrd	xmm3, xmm3, ecx, 3
	vpinsrd	xmm4, xmm4, r9d, 3
	vinserti128	ymm3, ymm3, xmm4, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm3
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	r10d, r9d
	shld	r10d, ecx, 12
	mov	esi, dword ptr [rbx - 8]
	shld	ecx, esi, 2
	mov	edi, dword ptr [rbx - 12]
	vmovd	xmm3, esi
	shld	esi, edi, 14
	mov	eax, dword ptr [rbx - 20]
	mov	edx, dword ptr [rbx - 16]
	shld	edi, edx, 4
	shrd	eax, edx, 16
	vpinsrd	xmm3, xmm3, ecx, 1
	vmovd	xmm4, eax
	vpinsrd	xmm3, xmm3, r10d, 2
	vpinsrd	xmm4, xmm4, edx, 1
	vpinsrd	xmm3, xmm3, r9d, 3
	vpinsrd	xmm4, xmm4, edi, 2
	vpinsrd	xmm4, xmm4, esi, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15], ymm3
	sub	r15, -128
	add	rbx, 88
	add	r8, -1
	jne	.LBB0_113
	jmp	.LBB0_147
.LBB0_123:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.124:
	mov	r8d, r14d
	add	r15, 96
	add	rbx, 52
	vmovdqa	ymm0, ymmword ptr [rip + .LCPI0_86] # ymm0 = [0,14,0,10,0,6,0,2]
	vpbroadcastq	ymm1, qword ptr [rip + .LCPI0_87] # ymm1 = [70364449226751,70364449226751,70364449226751,70364449226751]
	vmovdqa	ymm2, ymmword ptr [rip + .LCPI0_88] # ymm2 = [16,0,12,0,8,0,4,18]
	.p2align	4, 0x90
.LBB0_125:                              # =>This Inner Loop Header: Depth=1
	mov	r9d, dword ptr [rbx - 40]
	mov	ecx, dword ptr [rbx - 44]
	mov	esi, r9d
	shld	esi, ecx, 12
	mov	edi, dword ptr [rbx - 52]
	mov	r10d, dword ptr [rbx - 48]
	mov	edx, ecx
	shld	edx, r10d, 8
	mov	eax, r10d
	shld	eax, edi, 4
	vmovd	xmm3, edx
	vpinsrd	xmm3, xmm3, ecx, 1
	vpinsrd	xmm3, xmm3, esi, 2
	vpinsrd	xmm3, xmm3, r9d, 3
	vmovd	xmm4, edi
	vpinsrd	xmm4, xmm4, edi, 1
	vpinsrd	xmm4, xmm4, eax, 2
	vpinsrd	xmm4, xmm4, r10d, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 96], ymm3
	mov	eax, dword ptr [rbx - 28]
	mov	ecx, dword ptr [rbx - 32]
	mov	edx, eax
	shld	edx, ecx, 10
	mov	r9d, dword ptr [rbx - 40]
	mov	esi, dword ptr [rbx - 36]
	vmovd	xmm3, ecx
	shld	ecx, esi, 6
	mov	edi, esi
	shld	edi, r9d, 2
	vmovd	xmm4, r9d
	vpinsrd	xmm4, xmm4, edi, 1
	vpinsrd	xmm4, xmm4, esi, 2
	vpinsrd	xmm4, xmm4, ecx, 3
	vpinsrd	xmm3, xmm3, edx, 1
	vpinsrd	xmm3, xmm3, eax, 2
	vpinsrd	xmm3, xmm3, eax, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 64], ymm3
	mov	r9d, dword ptr [rbx - 12]
	mov	eax, dword ptr [rbx - 16]
	mov	edx, r9d
	shld	edx, eax, 12
	mov	esi, dword ptr [rbx - 24]
	mov	r10d, dword ptr [rbx - 20]
	mov	ecx, eax
	shld	ecx, r10d, 8
	mov	edi, r10d
	shld	edi, esi, 4
	vmovd	xmm3, ecx
	vpinsrd	xmm3, xmm3, eax, 1
	vpinsrd	xmm3, xmm3, edx, 2
	vpinsrd	xmm3, xmm3, r9d, 3
	vmovd	xmm4, esi
	vpinsrd	xmm4, xmm4, esi, 1
	vpinsrd	xmm4, xmm4, edi, 2
	vpinsrd	xmm4, xmm4, r10d, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm0
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15 - 32], ymm3
	mov	r9d, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, r9d
	shld	edx, ecx, 10
	mov	eax, dword ptr [rbx - 8]
	vmovd	xmm3, ecx
	shld	ecx, eax, 6
	mov	edi, dword ptr [rbx - 12]
	mov	esi, eax
	shld	esi, edi, 2
	vmovd	xmm4, edi
	vpinsrd	xmm4, xmm4, esi, 1
	vpinsrd	xmm4, xmm4, eax, 2
	vpinsrd	xmm4, xmm4, ecx, 3
	vpinsrd	xmm3, xmm3, edx, 1
	vpinsrd	xmm3, xmm3, r9d, 2
	vpinsrd	xmm3, xmm3, r9d, 3
	vinserti128	ymm3, ymm4, xmm3, 1
	vpsrlvd	ymm3, ymm3, ymm2
	vpand	ymm3, ymm3, ymm1
	vmovdqu	ymmword ptr [r15], ymm3
	sub	r15, -128
	add	rbx, 56
	add	r8, -1
	jne	.LBB0_125
	jmp	.LBB0_147
.LBB0_99:
	cmp	edx, 32
	jl	.LBB0_147
# %bb.100:
	mov	r8d, r14d
	add	r15, 96
	vpbroadcastq	ymm0, qword ptr [rip + .LCPI0_8] # ymm0 = [4611686015206162431,4611686015206162431,4611686015206162431,4611686015206162431]
	add	rbx, 116
	vmovdqa	xmm1, xmmword ptr [rip + .LCPI0_9] # xmm1 = [16,14,12,10]
	vmovdqa	xmm2, xmmword ptr [rip + .LCPI0_10] # xmm2 = [16,18,20,22]
	vmovdqa	ymm3, ymmword ptr [rip + .LCPI0_11] # ymm3 = [0,0,0,0,0,0,0,2]
	.p2align	4, 0x90
.LBB0_101:                              # =>This Inner Loop Header: Depth=1
	mov	r11d, dword ptr [rbx - 92]
	mov	r9d, dword ptr [rbx - 88]
	shld	r9d, r11d, 14
	mov	esi, dword ptr [rbx - 96]
	shld	r11d, esi, 12
	mov	edi, dword ptr [rbx - 100]
	shld	esi, edi, 10
	mov	eax, dword ptr [rbx - 104]
	shld	edi, eax, 8
	mov	edx, dword ptr [rbx - 108]
	shld	eax, edx, 6
	mov	r10d, dword ptr [rbx - 116]
	mov	ecx, dword ptr [rbx - 112]
	shld	edx, ecx, 4
	shld	ecx, r10d, 2
	vmovd	xmm4, r10d
	vmovd	xmm5, edi
	vpinsrd	xmm4, xmm4, ecx, 1
	vpinsrd	xmm5, xmm5, esi, 1
	vpinsrd	xmm4, xmm4, edx, 2
	vpinsrd	xmm5, xmm5, r11d, 2
	vpinsrd	xmm4, xmm4, eax, 3
	vpinsrd	xmm5, xmm5, r9d, 3
	vinserti128	ymm4, ymm4, xmm5, 1
	vpand	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r15 - 96], ymm4
	mov	eax, dword ptr [rbx - 60]
	mov	ecx, dword ptr [rbx - 64]
	mov	edx, eax
	shld	edx, ecx, 28
	mov	esi, dword ptr [rbx - 68]
	mov	edi, dword ptr [rbx - 72]
	shld	ecx, esi, 26
	shld	esi, edi, 24
	vmovdqu	xmm4, xmmword ptr [rbx - 88]
	vpsrlvd	xmm5, xmm4, xmm1
	vpshufd	xmm4, xmm4, 249                 # xmm4 = xmm4[1,2,3,3]
	vpinsrd	xmm4, xmm4, edi, 3
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, ecx, 1
	vpinsrd	xmm6, xmm6, edx, 2
	vpsllvd	xmm4, xmm4, xmm2
	vpinsrd	xmm6, xmm6, eax, 3
	vpor	xmm4, xmm5, xmm4
	vinserti128	ymm4, ymm4, xmm6, 1
	vpsrlvd	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r15 - 64], ymm4
	mov	r11d, dword ptr [rbx - 32]
	mov	r9d, dword ptr [rbx - 28]
	shld	r9d, r11d, 14
	mov	edx, dword ptr [rbx - 36]
	shld	r11d, edx, 12
	mov	esi, dword ptr [rbx - 40]
	shld	edx, esi, 10
	mov	edi, dword ptr [rbx - 44]
	shld	esi, edi, 8
	mov	ecx, dword ptr [rbx - 48]
	shld	edi, ecx, 6
	mov	r10d, dword ptr [rbx - 56]
	mov	eax, dword ptr [rbx - 52]
	shld	ecx, eax, 4
	shld	eax, r10d, 2
	vmovd	xmm4, r10d
	vmovd	xmm5, esi
	vpinsrd	xmm4, xmm4, eax, 1
	vpinsrd	xmm5, xmm5, edx, 1
	vpinsrd	xmm4, xmm4, ecx, 2
	vpinsrd	xmm5, xmm5, r11d, 2
	vpinsrd	xmm4, xmm4, edi, 3
	vpinsrd	xmm5, xmm5, r9d, 3
	vinserti128	ymm4, ymm4, xmm5, 1
	vpand	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r15 - 32], ymm4
	mov	eax, dword ptr [rbx]
	mov	ecx, dword ptr [rbx - 4]
	mov	edx, eax
	shld	edx, ecx, 28
	mov	esi, dword ptr [rbx - 8]
	shld	ecx, esi, 26
	mov	edi, dword ptr [rbx - 12]
	vmovdqu	xmm4, xmmword ptr [rbx - 28]
	shld	esi, edi, 24
	vpsrlvd	xmm5, xmm4, xmm1
	vpshufd	xmm4, xmm4, 249                 # xmm4 = xmm4[1,2,3,3]
	vpinsrd	xmm4, xmm4, edi, 3
	vmovd	xmm6, esi
	vpinsrd	xmm6, xmm6, ecx, 1
	vpsllvd	xmm4, xmm4, xmm2
	vpinsrd	xmm6, xmm6, edx, 2
	vpinsrd	xmm6, xmm6, eax, 3
	vpor	xmm4, xmm5, xmm4
	vinserti128	ymm4, ymm4, xmm6, 1
	vpsrlvd	ymm4, ymm4, ymm3
	vpand	ymm4, ymm4, ymm0
	vmovdqu	ymmword ptr [r15], ymm4
	sub	r15, -128
	add	rbx, 120
	add	r8, -1
	jne	.LBB0_101
.LBB0_147:
	shl	r14d, 5
	mov	eax, r14d
	lea	rsp, [rbp - 32]
	pop	rbx
	pop	r12
	pop	r14
	pop	r15
	pop	rbp
	vzeroupper
	ret
.Lfunc_end0:
	.size	unpack32_avx2, .Lfunc_end0-unpack32_avx2
                                        # -- End function
	.ident	"Debian clang version 11.1.0-++20210428103820+1fdec59bffc1-1~exp1~20210428204437.162"
	.section	".note.GNU-stack","",@progbits
	.addrsig
