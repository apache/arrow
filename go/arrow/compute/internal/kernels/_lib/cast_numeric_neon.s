	.text
	.file	"cast_numeric.cc"
	.globl	cast_type_numeric_neon          // -- Begin function cast_type_numeric_neon
	.p2align	2
	.type	cast_type_numeric_neon,@function
cast_type_numeric_neon:                 // @cast_type_numeric_neon
// %bb.0:
	stp	x29, x30, [sp, #-16]!           // 16-byte Folded Spill
	cmp	w0, #6                          // =6
	mov	x29, sp
	b.gt	.LBB0_17
// %bb.1:
	cmp	w0, #3                          // =3
	b.le	.LBB0_29
// %bb.2:
	cmp	w0, #4                          // =4
	b.eq	.LBB0_53
// %bb.3:
	cmp	w0, #5                          // =5
	b.eq	.LBB0_61
// %bb.4:
	cmp	w0, #6                          // =6
	b.ne	.LBB0_893
// %bb.5:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_109
// %bb.6:
	cmp	w1, #3                          // =3
	b.le	.LBB0_191
// %bb.7:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_347
// %bb.8:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_350
// %bb.9:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.10:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.11:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_14
// %bb.12:
	lsl	x9, x8, #2
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_894
// %bb.13:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_894
.LBB0_14:
	mov	x9, xzr
.LBB0_15:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_16:                               // =>This Inner Loop Header: Depth=1
	ldr	w11, [x10], #4
	subs	x8, x8, #1                      // =1
	str	w11, [x9], #4
	b.ne	.LBB0_16
	b	.LBB0_893
.LBB0_17:
	cmp	w0, #8                          // =8
	b.le	.LBB0_43
// %bb.18:
	cmp	w0, #9                          // =9
	b.eq	.LBB0_69
// %bb.19:
	cmp	w0, #11                         // =11
	b.eq	.LBB0_77
// %bb.20:
	cmp	w0, #12                         // =12
	b.ne	.LBB0_893
// %bb.21:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_116
// %bb.22:
	cmp	w1, #3                          // =3
	b.le	.LBB0_200
// %bb.23:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_353
// %bb.24:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_356
// %bb.25:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.26:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.27:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_643
// %bb.28:
	mov	x9, xzr
	b	.LBB0_646
.LBB0_29:
	cmp	w0, #2                          // =2
	b.eq	.LBB0_85
// %bb.30:
	cmp	w0, #3                          // =3
	b.ne	.LBB0_893
// %bb.31:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_127
// %bb.32:
	cmp	w1, #3                          // =3
	b.le	.LBB0_209
// %bb.33:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_359
// %bb.34:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_366
// %bb.35:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.36:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.37:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_40
// %bb.38:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_897
// %bb.39:
	add	x9, x3, x8, lsl #2
	cmp	x9, x2
	b.ls	.LBB0_897
.LBB0_40:
	mov	x9, xzr
.LBB0_41:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9
.LBB0_42:                               // =>This Inner Loop Header: Depth=1
	ldrsb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_42
	b	.LBB0_893
.LBB0_43:
	cmp	w0, #7                          // =7
	b.eq	.LBB0_97
// %bb.44:
	cmp	w0, #8                          // =8
	b.ne	.LBB0_893
// %bb.45:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_138
// %bb.46:
	cmp	w1, #3                          // =3
	b.le	.LBB0_218
// %bb.47:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_373
// %bb.48:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_376
// %bb.49:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.50:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.51:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_648
// %bb.52:
	mov	x9, xzr
	b	.LBB0_651
.LBB0_53:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_145
// %bb.54:
	cmp	w1, #3                          // =3
	b.le	.LBB0_227
// %bb.55:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_379
// %bb.56:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_386
// %bb.57:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.58:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.59:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_653
// %bb.60:
	mov	x9, xzr
	b	.LBB0_656
.LBB0_61:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_152
// %bb.62:
	cmp	w1, #3                          // =3
	b.le	.LBB0_236
// %bb.63:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_393
// %bb.64:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_400
// %bb.65:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.66:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.67:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_658
// %bb.68:
	mov	x9, xzr
	b	.LBB0_661
.LBB0_69:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_159
// %bb.70:
	cmp	w1, #3                          // =3
	b.le	.LBB0_245
// %bb.71:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_407
// %bb.72:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_410
// %bb.73:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.74:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.75:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_663
// %bb.76:
	mov	x9, xzr
	b	.LBB0_666
.LBB0_77:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_166
// %bb.78:
	cmp	w1, #3                          // =3
	b.le	.LBB0_254
// %bb.79:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_413
// %bb.80:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_416
// %bb.81:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.82:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.83:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_668
// %bb.84:
	mov	x9, xzr
	b	.LBB0_671
.LBB0_85:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_173
// %bb.86:
	cmp	w1, #3                          // =3
	b.le	.LBB0_263
// %bb.87:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_419
// %bb.88:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_426
// %bb.89:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.90:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.91:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_94
// %bb.92:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_900
// %bb.93:
	add	x9, x3, x8, lsl #2
	cmp	x9, x2
	b.ls	.LBB0_900
.LBB0_94:
	mov	x9, xzr
.LBB0_95:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9
.LBB0_96:                               // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_96
	b	.LBB0_893
.LBB0_97:
	cmp	w1, #6                          // =6
	b.gt	.LBB0_184
// %bb.98:
	cmp	w1, #3                          // =3
	b.le	.LBB0_272
// %bb.99:
	cmp	w1, #4                          // =4
	b.eq	.LBB0_433
// %bb.100:
	cmp	w1, #5                          // =5
	b.eq	.LBB0_436
// %bb.101:
	cmp	w1, #6                          // =6
	b.ne	.LBB0_893
// %bb.102:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.103:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_106
// %bb.104:
	lsl	x9, x8, #2
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_903
// %bb.105:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_903
.LBB0_106:
	mov	x9, xzr
.LBB0_107:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_108:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x10], #4
	subs	x8, x8, #1                      // =1
	str	w11, [x9], #4
	b.ne	.LBB0_108
	b	.LBB0_893
.LBB0_109:
	cmp	w1, #8                          // =8
	b.le	.LBB0_281
// %bb.110:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_439
// %bb.111:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_442
// %bb.112:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.113:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.114:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_673
// %bb.115:
	mov	x9, xzr
	b	.LBB0_676
.LBB0_116:
	cmp	w1, #8                          // =8
	b.le	.LBB0_286
// %bb.117:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_445
// %bb.118:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_448
// %bb.119:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.120:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.121:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_124
// %bb.122:
	lsl	x9, x8, #3
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_906
// %bb.123:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_906
.LBB0_124:
	mov	x9, xzr
.LBB0_125:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_126:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x10], #8
	subs	x8, x8, #1                      // =1
	str	x11, [x9], #8
	b.ne	.LBB0_126
	b	.LBB0_893
.LBB0_127:
	cmp	w1, #8                          // =8
	b.le	.LBB0_291
// %bb.128:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_451
// %bb.129:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_458
// %bb.130:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.131:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.132:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_135
// %bb.133:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_909
// %bb.134:
	add	x9, x3, x8, lsl #3
	cmp	x9, x2
	b.ls	.LBB0_909
.LBB0_135:
	mov	x9, xzr
.LBB0_136:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9
.LBB0_137:                              // =>This Inner Loop Header: Depth=1
	ldrsb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	scvtf	d0, w11
	str	d0, [x10], #8
	b.ne	.LBB0_137
	b	.LBB0_893
.LBB0_138:
	cmp	w1, #8                          // =8
	b.le	.LBB0_300
// %bb.139:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_465
// %bb.140:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_472
// %bb.141:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.142:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.143:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_678
// %bb.144:
	mov	x9, xzr
	b	.LBB0_681
.LBB0_145:
	cmp	w1, #8                          // =8
	b.le	.LBB0_309
// %bb.146:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_475
// %bb.147:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_478
// %bb.148:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.149:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.150:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_683
// %bb.151:
	mov	x9, xzr
	b	.LBB0_686
.LBB0_152:
	cmp	w1, #8                          // =8
	b.le	.LBB0_314
// %bb.153:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_481
// %bb.154:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_484
// %bb.155:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.156:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.157:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_688
// %bb.158:
	mov	x9, xzr
	b	.LBB0_691
.LBB0_159:
	cmp	w1, #8                          // =8
	b.le	.LBB0_319
// %bb.160:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_487
// %bb.161:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_494
// %bb.162:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.163:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.164:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_693
// %bb.165:
	mov	x9, xzr
	b	.LBB0_696
.LBB0_166:
	cmp	w1, #8                          // =8
	b.le	.LBB0_328
// %bb.167:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_497
// %bb.168:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_500
// %bb.169:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.170:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.171:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_698
// %bb.172:
	mov	x9, xzr
	b	.LBB0_701
.LBB0_173:
	cmp	w1, #8                          // =8
	b.le	.LBB0_333
// %bb.174:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_507
// %bb.175:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_514
// %bb.176:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.177:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.178:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_181
// %bb.179:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_912
// %bb.180:
	add	x9, x3, x8, lsl #3
	cmp	x9, x2
	b.ls	.LBB0_912
.LBB0_181:
	mov	x9, xzr
.LBB0_182:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9
.LBB0_183:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	ucvtf	d0, w11
	str	d0, [x10], #8
	b.ne	.LBB0_183
	b	.LBB0_893
.LBB0_184:
	cmp	w1, #8                          // =8
	b.le	.LBB0_342
// %bb.185:
	cmp	w1, #9                          // =9
	b.eq	.LBB0_521
// %bb.186:
	cmp	w1, #11                         // =11
	b.eq	.LBB0_524
// %bb.187:
	cmp	w1, #12                         // =12
	b.ne	.LBB0_893
// %bb.188:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.189:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_703
// %bb.190:
	mov	x9, xzr
	b	.LBB0_706
.LBB0_191:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_527
// %bb.192:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.193:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.194:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_197
// %bb.195:
	add	x9, x2, x8, lsl #2
	cmp	x9, x3
	b.ls	.LBB0_915
// %bb.196:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_915
.LBB0_197:
	mov	x9, xzr
.LBB0_198:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #2
.LBB0_199:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_199
	b	.LBB0_893
.LBB0_200:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_534
// %bb.201:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.202:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.203:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_206
// %bb.204:
	add	x9, x2, x8, lsl #3
	cmp	x9, x3
	b.ls	.LBB0_918
// %bb.205:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_918
.LBB0_206:
	mov	x9, xzr
.LBB0_207:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #3
.LBB0_208:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, d0
	strb	w11, [x10], #1
	b.ne	.LBB0_208
	b	.LBB0_893
.LBB0_209:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_541
// %bb.210:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.211:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.212:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_215
// %bb.213:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_921
// %bb.214:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_921
.LBB0_215:
	mov	x9, xzr
.LBB0_216:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9
.LBB0_217:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_217
	b	.LBB0_893
.LBB0_218:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_548
// %bb.219:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.220:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.221:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_224
// %bb.222:
	add	x9, x2, x8, lsl #3
	cmp	x9, x3
	b.ls	.LBB0_924
// %bb.223:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_924
.LBB0_224:
	mov	x9, xzr
.LBB0_225:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #3
.LBB0_226:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_226
	b	.LBB0_893
.LBB0_227:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_555
// %bb.228:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.229:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.230:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_233
// %bb.231:
	add	x9, x2, x8, lsl #1
	cmp	x9, x3
	b.ls	.LBB0_927
// %bb.232:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_927
.LBB0_233:
	mov	x9, xzr
.LBB0_234:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #1
.LBB0_235:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_235
	b	.LBB0_893
.LBB0_236:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_562
// %bb.237:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.238:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.239:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_242
// %bb.240:
	add	x9, x2, x8, lsl #1
	cmp	x9, x3
	b.ls	.LBB0_930
// %bb.241:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_930
.LBB0_242:
	mov	x9, xzr
.LBB0_243:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #1
.LBB0_244:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_244
	b	.LBB0_893
.LBB0_245:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_569
// %bb.246:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.247:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.248:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_251
// %bb.249:
	add	x9, x2, x8, lsl #3
	cmp	x9, x3
	b.ls	.LBB0_933
// %bb.250:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_933
.LBB0_251:
	mov	x9, xzr
.LBB0_252:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #3
.LBB0_253:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_253
	b	.LBB0_893
.LBB0_254:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_576
// %bb.255:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.256:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.257:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_260
// %bb.258:
	add	x9, x2, x8, lsl #2
	cmp	x9, x3
	b.ls	.LBB0_936
// %bb.259:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_936
.LBB0_260:
	mov	x9, xzr
.LBB0_261:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #2
.LBB0_262:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, s0
	strb	w11, [x10], #1
	b.ne	.LBB0_262
	b	.LBB0_893
.LBB0_263:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_583
// %bb.264:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.265:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.266:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_269
// %bb.267:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_939
// %bb.268:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_939
.LBB0_269:
	mov	x9, xzr
.LBB0_270:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9
.LBB0_271:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_271
	b	.LBB0_893
.LBB0_272:
	cmp	w1, #2                          // =2
	b.eq	.LBB0_590
// %bb.273:
	cmp	w1, #3                          // =3
	b.ne	.LBB0_893
// %bb.274:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.275:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_278
// %bb.276:
	add	x9, x2, x8, lsl #2
	cmp	x9, x3
	b.ls	.LBB0_942
// %bb.277:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_942
.LBB0_278:
	mov	x9, xzr
.LBB0_279:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #2
.LBB0_280:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_280
	b	.LBB0_893
.LBB0_281:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_597
// %bb.282:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.283:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.284:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_708
// %bb.285:
	mov	x9, xzr
	b	.LBB0_711
.LBB0_286:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_604
// %bb.287:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.288:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.289:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_713
// %bb.290:
	mov	x9, xzr
	b	.LBB0_716
.LBB0_291:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_607
// %bb.292:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.293:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.294:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_297
// %bb.295:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_945
// %bb.296:
	add	x9, x3, x8, lsl #3
	cmp	x9, x2
	b.ls	.LBB0_945
.LBB0_297:
	mov	x9, xzr
.LBB0_298:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9
.LBB0_299:                              // =>This Inner Loop Header: Depth=1
	ldrsb	x11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_299
	b	.LBB0_893
.LBB0_300:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_614
// %bb.301:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.302:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.303:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_306
// %bb.304:
	lsl	x9, x8, #3
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_948
// %bb.305:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_948
.LBB0_306:
	mov	x9, xzr
.LBB0_307:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_308:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x10], #8
	subs	x8, x8, #1                      // =1
	str	x11, [x9], #8
	b.ne	.LBB0_308
	b	.LBB0_893
.LBB0_309:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_617
// %bb.310:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.311:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.312:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_718
// %bb.313:
	mov	x9, xzr
	b	.LBB0_721
.LBB0_314:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_620
// %bb.315:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.316:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.317:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_723
// %bb.318:
	mov	x9, xzr
	b	.LBB0_726
.LBB0_319:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_623
// %bb.320:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.321:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.322:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_325
// %bb.323:
	lsl	x9, x8, #3
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_951
// %bb.324:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_951
.LBB0_325:
	mov	x9, xzr
.LBB0_326:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_327:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x10], #8
	subs	x8, x8, #1                      // =1
	str	x11, [x9], #8
	b.ne	.LBB0_327
	b	.LBB0_893
.LBB0_328:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_626
// %bb.329:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.330:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.331:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_728
// %bb.332:
	mov	x9, xzr
	b	.LBB0_731
.LBB0_333:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_629
// %bb.334:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.335:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.336:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_339
// %bb.337:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_954
// %bb.338:
	add	x9, x3, x8, lsl #3
	cmp	x9, x2
	b.ls	.LBB0_954
.LBB0_339:
	mov	x9, xzr
.LBB0_340:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9
.LBB0_341:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_341
	b	.LBB0_893
.LBB0_342:
	cmp	w1, #7                          // =7
	b.eq	.LBB0_636
// %bb.343:
	cmp	w1, #8                          // =8
	b.ne	.LBB0_893
// %bb.344:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.345:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_733
// %bb.346:
	mov	x9, xzr
	b	.LBB0_736
.LBB0_347:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.348:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_738
// %bb.349:
	mov	x9, xzr
	b	.LBB0_741
.LBB0_350:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.351:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_743
// %bb.352:
	mov	x9, xzr
	b	.LBB0_746
.LBB0_353:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.354:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_748
// %bb.355:
	mov	x9, xzr
	b	.LBB0_751
.LBB0_356:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.357:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_753
// %bb.358:
	mov	x9, xzr
	b	.LBB0_756
.LBB0_359:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.360:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_363
// %bb.361:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_957
// %bb.362:
	add	x9, x3, x8, lsl #1
	cmp	x9, x2
	b.ls	.LBB0_957
.LBB0_363:
	mov	x9, xzr
.LBB0_364:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9
.LBB0_365:                              // =>This Inner Loop Header: Depth=1
	ldrsb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_365
	b	.LBB0_893
.LBB0_366:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.367:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_370
// %bb.368:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_960
// %bb.369:
	add	x9, x3, x8, lsl #1
	cmp	x9, x2
	b.ls	.LBB0_960
.LBB0_370:
	mov	x9, xzr
.LBB0_371:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9
.LBB0_372:                              // =>This Inner Loop Header: Depth=1
	ldrsb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_372
	b	.LBB0_893
.LBB0_373:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.374:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_758
// %bb.375:
	mov	x9, xzr
	b	.LBB0_761
.LBB0_376:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.377:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_763
// %bb.378:
	mov	x9, xzr
	b	.LBB0_766
.LBB0_379:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.380:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_383
// %bb.381:
	lsl	x9, x8, #1
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_963
// %bb.382:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_963
.LBB0_383:
	mov	x9, xzr
.LBB0_384:
	lsl	x10, x9, #1
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_385:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x10], #2
	subs	x8, x8, #1                      // =1
	strh	w11, [x9], #2
	b.ne	.LBB0_385
	b	.LBB0_893
.LBB0_386:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.387:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_390
// %bb.388:
	lsl	x9, x8, #1
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_966
// %bb.389:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_966
.LBB0_390:
	mov	x9, xzr
.LBB0_391:
	lsl	x10, x9, #1
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_392:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x10], #2
	subs	x8, x8, #1                      // =1
	strh	w11, [x9], #2
	b.ne	.LBB0_392
	b	.LBB0_893
.LBB0_393:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.394:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_397
// %bb.395:
	lsl	x9, x8, #1
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_969
// %bb.396:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_969
.LBB0_397:
	mov	x9, xzr
.LBB0_398:
	lsl	x10, x9, #1
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_399:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x10], #2
	subs	x8, x8, #1                      // =1
	strh	w11, [x9], #2
	b.ne	.LBB0_399
	b	.LBB0_893
.LBB0_400:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.401:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_404
// %bb.402:
	lsl	x9, x8, #1
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_972
// %bb.403:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_972
.LBB0_404:
	mov	x9, xzr
.LBB0_405:
	lsl	x10, x9, #1
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_406:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x10], #2
	subs	x8, x8, #1                      // =1
	strh	w11, [x9], #2
	b.ne	.LBB0_406
	b	.LBB0_893
.LBB0_407:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.408:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_768
// %bb.409:
	mov	x9, xzr
	b	.LBB0_771
.LBB0_410:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.411:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_773
// %bb.412:
	mov	x9, xzr
	b	.LBB0_776
.LBB0_413:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.414:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_778
// %bb.415:
	mov	x9, xzr
	b	.LBB0_781
.LBB0_416:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.417:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_783
// %bb.418:
	mov	x9, xzr
	b	.LBB0_786
.LBB0_419:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.420:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_423
// %bb.421:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_975
// %bb.422:
	add	x9, x3, x8, lsl #1
	cmp	x9, x2
	b.ls	.LBB0_975
.LBB0_423:
	mov	x9, xzr
.LBB0_424:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9
.LBB0_425:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_425
	b	.LBB0_893
.LBB0_426:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.427:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_430
// %bb.428:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_978
// %bb.429:
	add	x9, x3, x8, lsl #1
	cmp	x9, x2
	b.ls	.LBB0_978
.LBB0_430:
	mov	x9, xzr
.LBB0_431:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9
.LBB0_432:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_432
	b	.LBB0_893
.LBB0_433:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.434:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_788
// %bb.435:
	mov	x9, xzr
	b	.LBB0_791
.LBB0_436:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.437:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_793
// %bb.438:
	mov	x9, xzr
	b	.LBB0_796
.LBB0_439:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.440:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_798
// %bb.441:
	mov	x9, xzr
	b	.LBB0_801
.LBB0_442:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.443:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_803
// %bb.444:
	mov	x9, xzr
	b	.LBB0_806
.LBB0_445:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.446:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_808
// %bb.447:
	mov	x9, xzr
	b	.LBB0_811
.LBB0_448:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.449:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_813
// %bb.450:
	mov	x9, xzr
	b	.LBB0_816
.LBB0_451:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.452:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_455
// %bb.453:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_981
// %bb.454:
	add	x9, x3, x8, lsl #3
	cmp	x9, x2
	b.ls	.LBB0_981
.LBB0_455:
	mov	x9, xzr
.LBB0_456:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9
.LBB0_457:                              // =>This Inner Loop Header: Depth=1
	ldrsb	x11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_457
	b	.LBB0_893
.LBB0_458:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.459:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_462
// %bb.460:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_984
// %bb.461:
	add	x9, x3, x8, lsl #2
	cmp	x9, x2
	b.ls	.LBB0_984
.LBB0_462:
	mov	x9, xzr
.LBB0_463:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9
.LBB0_464:                              // =>This Inner Loop Header: Depth=1
	ldrsb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	scvtf	s0, w11
	str	s0, [x10], #4
	b.ne	.LBB0_464
	b	.LBB0_893
.LBB0_465:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.466:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_469
// %bb.467:
	lsl	x9, x8, #3
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_987
// %bb.468:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_987
.LBB0_469:
	mov	x9, xzr
.LBB0_470:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_471:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x10], #8
	subs	x8, x8, #1                      // =1
	str	x11, [x9], #8
	b.ne	.LBB0_471
	b	.LBB0_893
.LBB0_472:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.473:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_818
// %bb.474:
	mov	x9, xzr
	b	.LBB0_821
.LBB0_475:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.476:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_823
// %bb.477:
	mov	x9, xzr
	b	.LBB0_826
.LBB0_478:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.479:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_828
// %bb.480:
	mov	x9, xzr
	b	.LBB0_831
.LBB0_481:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.482:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_833
// %bb.483:
	mov	x9, xzr
	b	.LBB0_836
.LBB0_484:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.485:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_838
// %bb.486:
	mov	x9, xzr
	b	.LBB0_841
.LBB0_487:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.488:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_491
// %bb.489:
	lsl	x9, x8, #3
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_990
// %bb.490:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_990
.LBB0_491:
	mov	x9, xzr
.LBB0_492:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_493:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x10], #8
	subs	x8, x8, #1                      // =1
	str	x11, [x9], #8
	b.ne	.LBB0_493
	b	.LBB0_893
.LBB0_494:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.495:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_843
// %bb.496:
	mov	x9, xzr
	b	.LBB0_846
.LBB0_497:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.498:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_848
// %bb.499:
	mov	x9, xzr
	b	.LBB0_851
.LBB0_500:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.501:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_504
// %bb.502:
	lsl	x9, x8, #2
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_993
// %bb.503:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_993
.LBB0_504:
	mov	x9, xzr
.LBB0_505:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_506:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x10], #4
	subs	x8, x8, #1                      // =1
	str	w11, [x9], #4
	b.ne	.LBB0_506
	b	.LBB0_893
.LBB0_507:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.508:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_511
// %bb.509:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_996
// %bb.510:
	add	x9, x3, x8, lsl #3
	cmp	x9, x2
	b.ls	.LBB0_996
.LBB0_511:
	mov	x9, xzr
.LBB0_512:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9
.LBB0_513:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_513
	b	.LBB0_893
.LBB0_514:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.515:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_518
// %bb.516:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_999
// %bb.517:
	add	x9, x3, x8, lsl #2
	cmp	x9, x2
	b.ls	.LBB0_999
.LBB0_518:
	mov	x9, xzr
.LBB0_519:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9
.LBB0_520:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	ucvtf	s0, w11
	str	s0, [x10], #4
	b.ne	.LBB0_520
	b	.LBB0_893
.LBB0_521:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.522:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_853
// %bb.523:
	mov	x9, xzr
	b	.LBB0_856
.LBB0_524:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.525:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_858
// %bb.526:
	mov	x9, xzr
	b	.LBB0_861
.LBB0_527:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.528:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_531
// %bb.529:
	add	x9, x2, x8, lsl #2
	cmp	x9, x3
	b.ls	.LBB0_1002
// %bb.530:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1002
.LBB0_531:
	mov	x9, xzr
.LBB0_532:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #2
.LBB0_533:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_533
	b	.LBB0_893
.LBB0_534:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.535:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.lo	.LBB0_538
// %bb.536:
	add	x9, x2, x8, lsl #3
	cmp	x9, x3
	b.ls	.LBB0_1005
// %bb.537:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1005
.LBB0_538:
	mov	x9, xzr
.LBB0_539:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #3
.LBB0_540:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, d0
	strb	w11, [x10], #1
	b.ne	.LBB0_540
	b	.LBB0_893
.LBB0_541:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.542:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_545
// %bb.543:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_1008
// %bb.544:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1008
.LBB0_545:
	mov	x9, xzr
.LBB0_546:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9
.LBB0_547:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_547
	b	.LBB0_893
.LBB0_548:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.549:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_552
// %bb.550:
	add	x9, x2, x8, lsl #3
	cmp	x9, x3
	b.ls	.LBB0_1011
// %bb.551:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1011
.LBB0_552:
	mov	x9, xzr
.LBB0_553:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #3
.LBB0_554:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_554
	b	.LBB0_893
.LBB0_555:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.556:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_559
// %bb.557:
	add	x9, x2, x8, lsl #1
	cmp	x9, x3
	b.ls	.LBB0_1014
// %bb.558:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1014
.LBB0_559:
	mov	x9, xzr
.LBB0_560:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #1
.LBB0_561:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_561
	b	.LBB0_893
.LBB0_562:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.563:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_566
// %bb.564:
	add	x9, x2, x8, lsl #1
	cmp	x9, x3
	b.ls	.LBB0_1017
// %bb.565:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1017
.LBB0_566:
	mov	x9, xzr
.LBB0_567:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #1
.LBB0_568:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_568
	b	.LBB0_893
.LBB0_569:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.570:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_573
// %bb.571:
	add	x9, x2, x8, lsl #3
	cmp	x9, x3
	b.ls	.LBB0_1020
// %bb.572:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1020
.LBB0_573:
	mov	x9, xzr
.LBB0_574:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #3
.LBB0_575:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_575
	b	.LBB0_893
.LBB0_576:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.577:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_580
// %bb.578:
	add	x9, x2, x8, lsl #2
	cmp	x9, x3
	b.ls	.LBB0_1023
// %bb.579:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1023
.LBB0_580:
	mov	x9, xzr
.LBB0_581:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #2
.LBB0_582:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, s0
	strb	w11, [x10], #1
	b.ne	.LBB0_582
	b	.LBB0_893
.LBB0_583:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.584:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_587
// %bb.585:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_1026
// %bb.586:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1026
.LBB0_587:
	mov	x9, xzr
.LBB0_588:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9
.LBB0_589:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_589
	b	.LBB0_893
.LBB0_590:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.591:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_594
// %bb.592:
	add	x9, x2, x8, lsl #2
	cmp	x9, x3
	b.ls	.LBB0_1029
// %bb.593:
	add	x9, x3, x8
	cmp	x9, x2
	b.ls	.LBB0_1029
.LBB0_594:
	mov	x9, xzr
.LBB0_595:
	sub	x8, x8, x9
	add	x10, x3, x9
	add	x9, x2, x9, lsl #2
.LBB0_596:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strb	w11, [x10], #1
	b.ne	.LBB0_596
	b	.LBB0_893
.LBB0_597:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.598:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_601
// %bb.599:
	lsl	x9, x8, #2
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_1032
// %bb.600:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_1032
.LBB0_601:
	mov	x9, xzr
.LBB0_602:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_603:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x10], #4
	subs	x8, x8, #1                      // =1
	str	w11, [x9], #4
	b.ne	.LBB0_603
	b	.LBB0_893
.LBB0_604:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.605:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_863
// %bb.606:
	mov	x9, xzr
	b	.LBB0_866
.LBB0_607:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.608:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_611
// %bb.609:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_1035
// %bb.610:
	add	x9, x3, x8, lsl #2
	cmp	x9, x2
	b.ls	.LBB0_1035
.LBB0_611:
	mov	x9, xzr
.LBB0_612:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9
.LBB0_613:                              // =>This Inner Loop Header: Depth=1
	ldrsb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_613
	b	.LBB0_893
.LBB0_614:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.615:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_868
// %bb.616:
	mov	x9, xzr
	b	.LBB0_871
.LBB0_617:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.618:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_873
// %bb.619:
	mov	x9, xzr
	b	.LBB0_876
.LBB0_620:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.621:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_878
// %bb.622:
	mov	x9, xzr
	b	.LBB0_881
.LBB0_623:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.624:
	cmp	w4, #16                         // =16
	mov	w8, w4
	b.hs	.LBB0_883
// %bb.625:
	mov	x9, xzr
	b	.LBB0_886
.LBB0_626:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.627:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.hs	.LBB0_888
// %bb.628:
	mov	x9, xzr
	b	.LBB0_891
.LBB0_629:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.630:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_633
// %bb.631:
	add	x9, x2, x8
	cmp	x9, x3
	b.ls	.LBB0_1038
// %bb.632:
	add	x9, x3, x8, lsl #2
	cmp	x9, x2
	b.ls	.LBB0_1038
.LBB0_633:
	mov	x9, xzr
.LBB0_634:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9
.LBB0_635:                              // =>This Inner Loop Header: Depth=1
	ldrb	w11, [x9], #1
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_635
	b	.LBB0_893
.LBB0_636:
	cmp	w4, #1                          // =1
	b.lt	.LBB0_893
// %bb.637:
	cmp	w4, #32                         // =32
	mov	w8, w4
	b.lo	.LBB0_640
// %bb.638:
	lsl	x9, x8, #2
	add	x10, x2, x9
	cmp	x10, x3
	b.ls	.LBB0_1041
// %bb.639:
	add	x9, x3, x9
	cmp	x9, x2
	b.ls	.LBB0_1041
.LBB0_640:
	mov	x9, xzr
.LBB0_641:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_642:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x10], #4
	subs	x8, x8, #1                      // =1
	str	w11, [x9], #4
	b.ne	.LBB0_642
	b	.LBB0_893
.LBB0_643:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_644:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	fcvtzu	v1.2d, v1.2d
	fcvtzu	v3.2d, v3.2d
	fcvtzu	v5.2d, v5.2d
	fcvtzu	v7.2d, v7.2d
	fcvtzu	v4.2d, v4.2d
	fcvtzu	v6.2d, v6.2d
	fcvtzu	v2.2d, v2.2d
	fcvtzu	v0.2d, v0.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn	v3.2s, v3.2d
	xtn	v1.2s, v1.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_644
// %bb.645:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_646:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_647:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvtzu	w11, d0
	str	w11, [x10], #4
	b.ne	.LBB0_647
	b	.LBB0_893
.LBB0_648:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_649:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_649
// %bb.650:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_651:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_652:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_652
	b	.LBB0_893
.LBB0_653:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_654:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	ushll	v4.4s, v1.4h, #0
	ushll	v5.4s, v0.4h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v6.4s, v3.4h, #0
	ushll	v7.4s, v2.4h, #0
	ushll2	v3.4s, v3.8h, #0
	ushll2	v2.4s, v2.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q1, [x11, #-64]
	stp	q7, q2, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_654
// %bb.655:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_656:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #1
.LBB0_657:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_657
	b	.LBB0_893
.LBB0_658:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_659:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	sshll	v4.4s, v1.4h, #0
	sshll	v5.4s, v0.4h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v6.4s, v3.4h, #0
	sshll	v7.4s, v2.4h, #0
	sshll2	v3.4s, v3.8h, #0
	sshll2	v2.4s, v2.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q1, [x11, #-64]
	stp	q7, q2, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_659
// %bb.660:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_661:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #1
.LBB0_662:                              // =>This Inner Loop Header: Depth=1
	ldrsh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_662
	b	.LBB0_893
.LBB0_663:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_664:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_664
// %bb.665:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_666:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_667:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_667
	b	.LBB0_893
.LBB0_668:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_669:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-32]
	ldp	q2, q3, [x10, #-64]
	ldp	q4, q5, [x10, #32]
	ldp	q6, q7, [x10], #128
	fcvtzu	v1.4s, v1.4s
	fcvtzu	v3.4s, v3.4s
	fcvtzu	v2.4s, v2.4s
	fcvtzu	v0.4s, v0.4s
	fcvtzu	v7.4s, v7.4s
	fcvtzu	v6.4s, v6.4s
	fcvtzu	v5.4s, v5.4s
	fcvtzu	v4.4s, v4.4s
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-32]
	stp	q2, q3, [x11, #-64]
	stp	q4, q5, [x11, #32]
	stp	q6, q7, [x11], #128
	b.ne	.LBB0_669
// %bb.670:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_671:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_672:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x10], #4
	subs	x8, x8, #1                      // =1
	fcvtzu	w11, s0
	str	w11, [x9], #4
	b.ne	.LBB0_672
	b	.LBB0_893
.LBB0_673:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_674:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	ushll	v4.2d, v1.2s, #0
	ushll	v5.2d, v0.2s, #0
	ushll	v6.2d, v3.2s, #0
	ushll	v7.2d, v2.2s, #0
	ushll2	v1.2d, v1.4s, #0
	ushll2	v0.2d, v0.4s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll2	v2.2d, v2.4s, #0
	ucvtf	v4.2d, v4.2d
	ucvtf	v5.2d, v5.2d
	ucvtf	v6.2d, v6.2d
	ucvtf	v7.2d, v7.2d
	ucvtf	v1.2d, v1.2d
	ucvtf	v0.2d, v0.2d
	ucvtf	v3.2d, v3.2d
	ucvtf	v2.2d, v2.2d
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_674
// %bb.675:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_676:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_677:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	ucvtf	d0, w11
	str	d0, [x10], #8
	b.ne	.LBB0_677
	b	.LBB0_893
.LBB0_678:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_679:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12, #96]
	ldp	q2, q3, [x12, #64]
	ldp	q4, q5, [x12]
	ldp	q6, q7, [x12, #32]
	ucvtf	v1.2d, v1.2d
	ucvtf	v3.2d, v3.2d
	ucvtf	v5.2d, v5.2d
	ucvtf	v4.2d, v4.2d
	ucvtf	v7.2d, v7.2d
	ucvtf	v6.2d, v6.2d
	ucvtf	v2.2d, v2.2d
	ucvtf	v0.2d, v0.2d
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q0, q1, [x11, #96]
	stp	q2, q3, [x11, #64]
	stp	q6, q7, [x11, #32]
	stp	q4, q5, [x11], #128
	b.ne	.LBB0_679
// %bb.680:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_681:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_682:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x10], #8
	subs	x8, x8, #1                      // =1
	ucvtf	d0, d0
	str	d0, [x9], #8
	b.ne	.LBB0_682
	b	.LBB0_893
.LBB0_683:
	and	x9, x8, #0xfffffff0
	movi	d0, #0x00ffff0000ffff
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_684:                              // =>This Inner Loop Header: Depth=1
	ldp	q4, q2, [x12], #32
	subs	x10, x10, #16                   // =16
	ext	v1.16b, v4.16b, v4.16b, #8
	umov	w13, v4.h[0]
	umov	w15, v4.h[2]
	ext	v3.16b, v2.16b, v2.16b, #8
	umov	w14, v4.h[1]
	umov	w16, v4.h[3]
	umov	w17, v2.h[0]
	umov	w18, v2.h[1]
	umov	w0, v2.h[2]
	fmov	s4, w13
	umov	w13, v2.h[3]
	fmov	s2, w15
	umov	w15, v1.h[0]
	fmov	s6, w0
	umov	w0, v1.h[2]
	fmov	s7, w15
	umov	w15, v3.h[0]
	fmov	s16, w0
	umov	w0, v3.h[2]
	fmov	s17, w15
	mov	v6.s[1], w13
	umov	w13, v3.h[1]
	fmov	s5, w17
	umov	w17, v1.h[1]
	umov	w15, v1.h[3]
	fmov	s1, w0
	mov	v17.s[1], w13
	umov	w13, v3.h[3]
	mov	v4.s[1], w14
	mov	v2.s[1], w16
	mov	v5.s[1], w18
	mov	v7.s[1], w17
	mov	v16.s[1], w15
	mov	v1.s[1], w13
	and	v3.8b, v4.8b, v0.8b
	and	v2.8b, v2.8b, v0.8b
	and	v4.8b, v5.8b, v0.8b
	and	v5.8b, v6.8b, v0.8b
	and	v6.8b, v7.8b, v0.8b
	and	v7.8b, v16.8b, v0.8b
	and	v16.8b, v17.8b, v0.8b
	and	v1.8b, v1.8b, v0.8b
	ushll	v3.2d, v3.2s, #0
	ushll	v2.2d, v2.2s, #0
	ushll	v4.2d, v4.2s, #0
	ushll	v5.2d, v5.2s, #0
	ushll	v6.2d, v6.2s, #0
	ushll	v7.2d, v7.2s, #0
	ushll	v16.2d, v16.2s, #0
	ushll	v1.2d, v1.2s, #0
	ucvtf	v3.2d, v3.2d
	ucvtf	v2.2d, v2.2d
	ucvtf	v4.2d, v4.2d
	ucvtf	v5.2d, v5.2d
	ucvtf	v6.2d, v6.2d
	ucvtf	v7.2d, v7.2d
	ucvtf	v16.2d, v16.2d
	ucvtf	v1.2d, v1.2d
	stp	q4, q5, [x11, #64]
	stp	q3, q2, [x11]
	stp	q16, q1, [x11, #96]
	stp	q6, q7, [x11, #32]
	add	x11, x11, #128                  // =128
	b.ne	.LBB0_684
// %bb.685:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_686:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #1
.LBB0_687:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	ucvtf	d0, w11
	str	d0, [x10], #8
	b.ne	.LBB0_687
	b	.LBB0_893
.LBB0_688:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_689:                              // =>This Inner Loop Header: Depth=1
	ldp	q3, q1, [x12], #32
	subs	x10, x10, #16                   // =16
	ext	v0.16b, v3.16b, v3.16b, #8
	umov	w13, v3.h[0]
	umov	w15, v3.h[2]
	ext	v2.16b, v1.16b, v1.16b, #8
	umov	w14, v3.h[1]
	umov	w16, v3.h[3]
	umov	w17, v1.h[0]
	umov	w18, v1.h[1]
	umov	w0, v1.h[2]
	fmov	s3, w13
	umov	w13, v1.h[3]
	fmov	s1, w15
	umov	w15, v0.h[0]
	fmov	s5, w0
	umov	w0, v0.h[2]
	fmov	s6, w15
	umov	w15, v2.h[0]
	fmov	s7, w0
	umov	w0, v2.h[2]
	fmov	s16, w15
	mov	v5.s[1], w13
	umov	w13, v2.h[1]
	fmov	s4, w17
	umov	w17, v0.h[1]
	umov	w15, v0.h[3]
	fmov	s0, w0
	mov	v16.s[1], w13
	umov	w13, v2.h[3]
	mov	v3.s[1], w14
	mov	v1.s[1], w16
	mov	v4.s[1], w18
	mov	v6.s[1], w17
	mov	v7.s[1], w15
	mov	v0.s[1], w13
	shl	v2.2s, v3.2s, #16
	shl	v1.2s, v1.2s, #16
	shl	v3.2s, v4.2s, #16
	shl	v4.2s, v5.2s, #16
	shl	v5.2s, v6.2s, #16
	shl	v6.2s, v7.2s, #16
	shl	v7.2s, v16.2s, #16
	shl	v0.2s, v0.2s, #16
	sshr	v2.2s, v2.2s, #16
	sshr	v1.2s, v1.2s, #16
	sshr	v3.2s, v3.2s, #16
	sshr	v4.2s, v4.2s, #16
	sshr	v5.2s, v5.2s, #16
	sshr	v6.2s, v6.2s, #16
	sshr	v7.2s, v7.2s, #16
	sshr	v0.2s, v0.2s, #16
	sshll	v2.2d, v2.2s, #0
	sshll	v1.2d, v1.2s, #0
	sshll	v3.2d, v3.2s, #0
	sshll	v4.2d, v4.2s, #0
	sshll	v5.2d, v5.2s, #0
	sshll	v6.2d, v6.2s, #0
	sshll	v7.2d, v7.2s, #0
	sshll	v0.2d, v0.2s, #0
	scvtf	v2.2d, v2.2d
	scvtf	v1.2d, v1.2d
	scvtf	v3.2d, v3.2d
	scvtf	v4.2d, v4.2d
	scvtf	v5.2d, v5.2d
	scvtf	v6.2d, v6.2d
	scvtf	v7.2d, v7.2d
	scvtf	v0.2d, v0.2d
	stp	q3, q4, [x11, #64]
	stp	q2, q1, [x11]
	stp	q7, q0, [x11, #96]
	stp	q5, q6, [x11, #32]
	add	x11, x11, #128                  // =128
	b.ne	.LBB0_689
// %bb.690:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_691:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #1
.LBB0_692:                              // =>This Inner Loop Header: Depth=1
	ldrsh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	scvtf	d0, w11
	str	d0, [x10], #8
	b.ne	.LBB0_692
	b	.LBB0_893
.LBB0_693:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_694:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12, #96]
	ldp	q2, q3, [x12, #64]
	ldp	q4, q5, [x12]
	ldp	q6, q7, [x12, #32]
	scvtf	v1.2d, v1.2d
	scvtf	v3.2d, v3.2d
	scvtf	v5.2d, v5.2d
	scvtf	v4.2d, v4.2d
	scvtf	v7.2d, v7.2d
	scvtf	v6.2d, v6.2d
	scvtf	v2.2d, v2.2d
	scvtf	v0.2d, v0.2d
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q0, q1, [x11, #96]
	stp	q2, q3, [x11, #64]
	stp	q6, q7, [x11, #32]
	stp	q4, q5, [x11], #128
	b.ne	.LBB0_694
// %bb.695:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_696:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_697:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x10], #8
	subs	x8, x8, #1                      // =1
	scvtf	d0, d0
	str	d0, [x9], #8
	b.ne	.LBB0_697
	b	.LBB0_893
.LBB0_698:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_699:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	fcvtl	v4.2d, v1.2s
	fcvtl	v5.2d, v0.2s
	fcvtl	v6.2d, v3.2s
	fcvtl	v7.2d, v2.2s
	fcvtl2	v1.2d, v1.4s
	fcvtl2	v0.2d, v0.4s
	fcvtl2	v3.2d, v3.4s
	fcvtl2	v2.2d, v2.4s
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_699
// %bb.700:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_701:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_702:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvt	d0, s0
	str	d0, [x10], #8
	b.ne	.LBB0_702
	b	.LBB0_893
.LBB0_703:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_704:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	sshll	v4.2d, v1.2s, #0
	sshll	v5.2d, v0.2s, #0
	sshll	v6.2d, v3.2s, #0
	sshll	v7.2d, v2.2s, #0
	sshll2	v1.2d, v1.4s, #0
	sshll2	v0.2d, v0.4s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll2	v2.2d, v2.4s, #0
	scvtf	v4.2d, v4.2d
	scvtf	v5.2d, v5.2d
	scvtf	v6.2d, v6.2d
	scvtf	v7.2d, v7.2d
	scvtf	v1.2d, v1.2d
	scvtf	v0.2d, v0.2d
	scvtf	v3.2d, v3.2d
	scvtf	v2.2d, v2.2d
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_704
// %bb.705:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_706:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_707:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	scvtf	d0, w11
	str	d0, [x10], #8
	b.ne	.LBB0_707
	b	.LBB0_893
.LBB0_708:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_709:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	ushll	v4.2d, v1.2s, #0
	ushll	v5.2d, v0.2s, #0
	ushll	v6.2d, v3.2s, #0
	ushll	v7.2d, v2.2s, #0
	ushll2	v1.2d, v1.4s, #0
	ushll2	v0.2d, v0.4s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll2	v2.2d, v2.4s, #0
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_709
// %bb.710:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_711:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_712:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_712
	b	.LBB0_893
.LBB0_713:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_714:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12, #96]
	ldp	q2, q3, [x12, #64]
	ldp	q4, q5, [x12]
	ldp	q6, q7, [x12, #32]
	fcvtzu	v1.2d, v1.2d
	fcvtzu	v3.2d, v3.2d
	fcvtzu	v5.2d, v5.2d
	fcvtzu	v4.2d, v4.2d
	fcvtzu	v7.2d, v7.2d
	fcvtzu	v6.2d, v6.2d
	fcvtzu	v2.2d, v2.2d
	fcvtzu	v0.2d, v0.2d
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q0, q1, [x11, #96]
	stp	q2, q3, [x11, #64]
	stp	q6, q7, [x11, #32]
	stp	q4, q5, [x11], #128
	b.ne	.LBB0_714
// %bb.715:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_716:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_717:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x10], #8
	subs	x8, x8, #1                      // =1
	fcvtzu	x11, d0
	str	x11, [x9], #8
	b.ne	.LBB0_717
	b	.LBB0_893
.LBB0_718:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_719:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12], #32
	subs	x10, x10, #16                   // =16
	ushll	v2.4s, v0.4h, #0
	ushll	v3.4s, v1.4h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll	v4.2d, v2.2s, #0
	ushll	v5.2d, v3.2s, #0
	ushll2	v2.2d, v2.4s, #0
	ushll	v6.2d, v0.2s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll	v7.2d, v1.2s, #0
	ushll2	v0.2d, v0.4s, #0
	ushll2	v1.2d, v1.4s, #0
	stp	q7, q1, [x11, #96]
	stp	q6, q0, [x11, #32]
	stp	q5, q3, [x11, #64]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_719
// %bb.720:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_721:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #1
.LBB0_722:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_722
	b	.LBB0_893
.LBB0_723:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_724:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12], #32
	subs	x10, x10, #16                   // =16
	sshll	v2.4s, v0.4h, #0
	sshll	v3.4s, v1.4h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll	v4.2d, v2.2s, #0
	sshll	v5.2d, v3.2s, #0
	sshll2	v2.2d, v2.4s, #0
	sshll	v6.2d, v0.2s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll	v7.2d, v1.2s, #0
	sshll2	v0.2d, v0.4s, #0
	sshll2	v1.2d, v1.4s, #0
	stp	q7, q1, [x11, #96]
	stp	q6, q0, [x11, #32]
	stp	q5, q3, [x11, #64]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_724
// %bb.725:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_726:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #1
.LBB0_727:                              // =>This Inner Loop Header: Depth=1
	ldrsh	x11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_727
	b	.LBB0_893
.LBB0_728:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_729:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	fcvtl	v4.2d, v1.2s
	fcvtl	v5.2d, v0.2s
	fcvtl	v6.2d, v3.2s
	fcvtl	v7.2d, v2.2s
	fcvtl2	v1.2d, v1.4s
	fcvtl2	v0.2d, v0.4s
	fcvtl2	v3.2d, v3.4s
	fcvtl2	v2.2d, v2.4s
	fcvtzu	v4.2d, v4.2d
	fcvtzu	v5.2d, v5.2d
	fcvtzu	v6.2d, v6.2d
	fcvtzu	v7.2d, v7.2d
	fcvtzu	v1.2d, v1.2d
	fcvtzu	v0.2d, v0.2d
	fcvtzu	v3.2d, v3.2d
	fcvtzu	v2.2d, v2.2d
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_729
// %bb.730:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_731:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_732:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvtzu	x11, s0
	str	x11, [x10], #8
	b.ne	.LBB0_732
	b	.LBB0_893
.LBB0_733:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_734:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	sshll	v4.2d, v1.2s, #0
	sshll	v5.2d, v0.2s, #0
	sshll	v6.2d, v3.2s, #0
	sshll	v7.2d, v2.2s, #0
	sshll2	v1.2d, v1.4s, #0
	sshll2	v0.2d, v0.4s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll2	v2.2d, v2.4s, #0
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_734
// %bb.735:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_736:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_737:                              // =>This Inner Loop Header: Depth=1
	ldrsw	x11, [x9], #4
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_737
	b	.LBB0_893
.LBB0_738:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_739:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn	v5.4h, v5.4s
	xtn	v7.4h, v7.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q1, q3, [x11, #-32]
	stp	q5, q7, [x11], #64
	b.ne	.LBB0_739
// %bb.740:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_741:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #2
.LBB0_742:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_742
	b	.LBB0_893
.LBB0_743:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_744:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn	v5.4h, v5.4s
	xtn	v7.4h, v7.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q1, q3, [x11, #-32]
	stp	q5, q7, [x11], #64
	b.ne	.LBB0_744
// %bb.745:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_746:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #2
.LBB0_747:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_747
	b	.LBB0_893
.LBB0_748:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_749:                              // =>This Inner Loop Header: Depth=1
	ldp	q3, q2, [x12]
	ldp	q7, q6, [x12, #64]
	ldp	q5, q4, [x12, #32]
	ldp	q1, q0, [x12, #96]
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v2.2d, v2.2d
	fcvtzs	v7.2d, v7.2d
	xtn	v3.2s, v3.2d
	fcvtzs	v6.2d, v6.2d
	xtn	v2.2s, v2.2d
	xtn	v7.2s, v7.2d
	mov	w13, v3.s[1]
	fcvtzs	v5.2d, v5.2d
	xtn	v6.2s, v6.2d
	fmov	w15, s2
	mov	v3.h[1], w13
	mov	w13, v7.s[1]
	fcvtzs	v1.2d, v1.2d
	xtn	v5.2s, v5.2d
	mov	w14, v2.s[1]
	mov	v7.h[1], w13
	mov	v3.h[2], w15
	fmov	w15, s6
	fcvtzs	v4.2d, v4.2d
	xtn	v1.2s, v1.2d
	mov	w13, v6.s[1]
	mov	v7.h[2], w15
	mov	v3.h[3], w14
	fmov	w14, s5
	fcvtzs	v0.2d, v0.2d
	xtn	v4.2s, v4.2d
	mov	w15, v5.s[1]
	mov	v7.h[3], w13
	mov	v3.h[4], w14
	fmov	w14, s1
	xtn	v0.2s, v0.2d
	mov	w13, v1.s[1]
	mov	v7.h[4], w14
	mov	v3.h[5], w15
	fmov	w15, s4
	mov	v7.h[5], w13
	mov	v3.h[6], w15
	fmov	w15, s0
	mov	w14, v4.s[1]
	mov	w13, v0.s[1]
	mov	v7.h[6], w15
	mov	v3.h[7], w14
	mov	v7.h[7], w13
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q3, q7, [x11], #32
	b.ne	.LBB0_749
// %bb.750:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_751:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #3
.LBB0_752:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, d0
	strh	w11, [x10], #2
	b.ne	.LBB0_752
	b	.LBB0_893
.LBB0_753:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_754:                              // =>This Inner Loop Header: Depth=1
	ldp	q3, q2, [x12]
	ldp	q7, q6, [x12, #64]
	ldp	q5, q4, [x12, #32]
	ldp	q1, q0, [x12, #96]
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v2.2d, v2.2d
	fcvtzs	v7.2d, v7.2d
	xtn	v3.2s, v3.2d
	fcvtzs	v6.2d, v6.2d
	xtn	v2.2s, v2.2d
	xtn	v7.2s, v7.2d
	mov	w13, v3.s[1]
	fcvtzs	v5.2d, v5.2d
	xtn	v6.2s, v6.2d
	fmov	w15, s2
	mov	v3.h[1], w13
	mov	w13, v7.s[1]
	fcvtzs	v1.2d, v1.2d
	xtn	v5.2s, v5.2d
	mov	w14, v2.s[1]
	mov	v7.h[1], w13
	mov	v3.h[2], w15
	fmov	w15, s6
	fcvtzs	v4.2d, v4.2d
	xtn	v1.2s, v1.2d
	mov	w13, v6.s[1]
	mov	v7.h[2], w15
	mov	v3.h[3], w14
	fmov	w14, s5
	fcvtzs	v0.2d, v0.2d
	xtn	v4.2s, v4.2d
	mov	w15, v5.s[1]
	mov	v7.h[3], w13
	mov	v3.h[4], w14
	fmov	w14, s1
	xtn	v0.2s, v0.2d
	mov	w13, v1.s[1]
	mov	v7.h[4], w14
	mov	v3.h[5], w15
	fmov	w15, s4
	mov	v7.h[5], w13
	mov	v3.h[6], w15
	fmov	w15, s0
	mov	w14, v4.s[1]
	mov	w13, v0.s[1]
	mov	v7.h[6], w15
	mov	v3.h[7], w14
	mov	v7.h[7], w13
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q3, q7, [x11], #32
	b.ne	.LBB0_754
// %bb.755:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_756:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #3
.LBB0_757:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, d0
	strh	w11, [x10], #2
	b.ne	.LBB0_757
	b	.LBB0_893
.LBB0_758:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_759:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn	v4.4h, v7.4s
	xtn2	v4.8h, v5.4s
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	xtn	v0.4h, v1.4s
	xtn2	v0.8h, v3.4s
	subs	x10, x10, #16                   // =16
	stp	q4, q0, [x11], #32
	b.ne	.LBB0_759
// %bb.760:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_761:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #3
.LBB0_762:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_762
	b	.LBB0_893
.LBB0_763:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_764:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn	v4.4h, v7.4s
	xtn2	v4.8h, v5.4s
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	xtn	v0.4h, v1.4s
	xtn2	v0.8h, v3.4s
	subs	x10, x10, #16                   // =16
	stp	q4, q0, [x11], #32
	b.ne	.LBB0_764
// %bb.765:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_766:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #3
.LBB0_767:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_767
	b	.LBB0_893
.LBB0_768:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_769:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn	v4.4h, v7.4s
	xtn2	v4.8h, v5.4s
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	xtn	v0.4h, v1.4s
	xtn2	v0.8h, v3.4s
	subs	x10, x10, #16                   // =16
	stp	q4, q0, [x11], #32
	b.ne	.LBB0_769
// %bb.770:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_771:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #3
.LBB0_772:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_772
	b	.LBB0_893
.LBB0_773:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_774:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn	v4.4h, v7.4s
	xtn2	v4.8h, v5.4s
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	xtn	v0.4h, v1.4s
	xtn2	v0.8h, v3.4s
	subs	x10, x10, #16                   // =16
	stp	q4, q0, [x11], #32
	b.ne	.LBB0_774
// %bb.775:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_776:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #3
.LBB0_777:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_777
	b	.LBB0_893
.LBB0_778:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_779:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	fcvtzu	v1.4s, v1.4s
	fcvtzu	v3.4s, v3.4s
	fcvtzu	v5.4s, v5.4s
	fcvtzu	v7.4s, v7.4s
	fcvtzu	v2.4s, v2.4s
	fcvtzu	v0.4s, v0.4s
	fcvtzu	v6.4s, v6.4s
	fcvtzu	v4.4s, v4.4s
	xtn	v3.4h, v3.4s
	xtn	v1.4h, v1.4s
	xtn	v7.4h, v7.4s
	xtn	v5.4h, v5.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q1, q3, [x11, #-32]
	stp	q5, q7, [x11], #64
	b.ne	.LBB0_779
// %bb.780:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_781:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #2
.LBB0_782:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, s0
	strh	w11, [x10], #2
	b.ne	.LBB0_782
	b	.LBB0_893
.LBB0_783:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_784:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	fcvtzs	v1.4s, v1.4s
	fcvtzs	v3.4s, v3.4s
	fcvtzs	v5.4s, v5.4s
	fcvtzs	v7.4s, v7.4s
	fcvtzs	v2.4s, v2.4s
	fcvtzs	v0.4s, v0.4s
	fcvtzs	v6.4s, v6.4s
	fcvtzs	v4.4s, v4.4s
	xtn	v3.4h, v3.4s
	xtn	v1.4h, v1.4s
	xtn	v7.4h, v7.4s
	xtn	v5.4h, v5.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q1, q3, [x11, #-32]
	stp	q5, q7, [x11], #64
	b.ne	.LBB0_784
// %bb.785:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_786:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #2
.LBB0_787:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, s0
	strh	w11, [x10], #2
	b.ne	.LBB0_787
	b	.LBB0_893
.LBB0_788:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_789:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn	v5.4h, v5.4s
	xtn	v7.4h, v7.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q1, q3, [x11, #-32]
	stp	q5, q7, [x11], #64
	b.ne	.LBB0_789
// %bb.790:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_791:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #2
.LBB0_792:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_792
	b	.LBB0_893
.LBB0_793:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_794:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn	v5.4h, v5.4s
	xtn	v7.4h, v7.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q1, q3, [x11, #-32]
	stp	q5, q7, [x11], #64
	b.ne	.LBB0_794
// %bb.795:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_796:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #1
	add	x9, x2, x9, lsl #2
.LBB0_797:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	strh	w11, [x10], #2
	b.ne	.LBB0_797
	b	.LBB0_893
.LBB0_798:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_799:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	ushll	v4.2d, v1.2s, #0
	ushll	v5.2d, v0.2s, #0
	ushll	v6.2d, v3.2s, #0
	ushll	v7.2d, v2.2s, #0
	ushll2	v1.2d, v1.4s, #0
	ushll2	v0.2d, v0.4s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll2	v2.2d, v2.4s, #0
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_799
// %bb.800:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_801:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_802:                              // =>This Inner Loop Header: Depth=1
	ldr	w11, [x9], #4
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_802
	b	.LBB0_893
.LBB0_803:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_804:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-32]
	ldp	q2, q3, [x10, #-64]
	ldp	q4, q5, [x10, #32]
	ldp	q6, q7, [x10], #128
	ucvtf	v1.4s, v1.4s
	ucvtf	v3.4s, v3.4s
	ucvtf	v2.4s, v2.4s
	ucvtf	v0.4s, v0.4s
	ucvtf	v7.4s, v7.4s
	ucvtf	v6.4s, v6.4s
	ucvtf	v5.4s, v5.4s
	ucvtf	v4.4s, v4.4s
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-32]
	stp	q2, q3, [x11, #-64]
	stp	q4, q5, [x11, #32]
	stp	q6, q7, [x11], #128
	b.ne	.LBB0_804
// %bb.805:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_806:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_807:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x10], #4
	subs	x8, x8, #1                      // =1
	ucvtf	s0, s0
	str	s0, [x9], #4
	b.ne	.LBB0_807
	b	.LBB0_893
.LBB0_808:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_809:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12, #96]
	ldp	q2, q3, [x12, #64]
	ldp	q4, q5, [x12]
	ldp	q6, q7, [x12, #32]
	fcvtzs	v1.2d, v1.2d
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v5.2d, v5.2d
	fcvtzs	v4.2d, v4.2d
	fcvtzs	v7.2d, v7.2d
	fcvtzs	v6.2d, v6.2d
	fcvtzs	v2.2d, v2.2d
	fcvtzs	v0.2d, v0.2d
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q0, q1, [x11, #96]
	stp	q2, q3, [x11, #64]
	stp	q6, q7, [x11, #32]
	stp	q4, q5, [x11], #128
	b.ne	.LBB0_809
// %bb.810:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_811:
	lsl	x10, x9, #3
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_812:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x10], #8
	subs	x8, x8, #1                      // =1
	fcvtzs	x11, d0
	str	x11, [x9], #8
	b.ne	.LBB0_812
	b	.LBB0_893
.LBB0_813:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_814:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	fcvtn	v1.2s, v1.2d
	fcvtn	v3.2s, v3.2d
	fcvtn	v5.2s, v5.2d
	fcvtn	v7.2s, v7.2d
	fcvtn2	v5.4s, v4.2d
	fcvtn2	v7.4s, v6.2d
	fcvtn2	v3.4s, v2.2d
	fcvtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_814
// %bb.815:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_816:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_817:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvt	s0, d0
	str	s0, [x10], #4
	b.ne	.LBB0_817
	b	.LBB0_893
.LBB0_818:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_819:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	ucvtf	v1.2d, v1.2d
	ucvtf	v3.2d, v3.2d
	ucvtf	v5.2d, v5.2d
	ucvtf	v7.2d, v7.2d
	ucvtf	v4.2d, v4.2d
	ucvtf	v6.2d, v6.2d
	ucvtf	v2.2d, v2.2d
	ucvtf	v0.2d, v0.2d
	fcvtn	v5.2s, v5.2d
	fcvtn	v7.2s, v7.2d
	fcvtn	v3.2s, v3.2d
	fcvtn	v1.2s, v1.2d
	fcvtn2	v5.4s, v4.2d
	fcvtn2	v7.4s, v6.2d
	fcvtn2	v3.4s, v2.2d
	fcvtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_819
// %bb.820:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_821:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_822:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	ucvtf	s0, x11
	str	s0, [x10], #4
	b.ne	.LBB0_822
	b	.LBB0_893
.LBB0_823:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_824:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12], #32
	subs	x10, x10, #16                   // =16
	ushll	v2.4s, v0.4h, #0
	ushll	v3.4s, v1.4h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll	v4.2d, v2.2s, #0
	ushll	v5.2d, v3.2s, #0
	ushll2	v2.2d, v2.4s, #0
	ushll	v6.2d, v0.2s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll	v7.2d, v1.2s, #0
	ushll2	v0.2d, v0.4s, #0
	ushll2	v1.2d, v1.4s, #0
	stp	q7, q1, [x11, #96]
	stp	q6, q0, [x11, #32]
	stp	q5, q3, [x11, #64]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_824
// %bb.825:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_826:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #1
.LBB0_827:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_827
	b	.LBB0_893
.LBB0_828:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_829:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	ushll	v4.4s, v1.4h, #0
	ushll	v5.4s, v0.4h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v6.4s, v3.4h, #0
	ushll	v7.4s, v2.4h, #0
	ushll2	v3.4s, v3.8h, #0
	ushll2	v2.4s, v2.8h, #0
	ucvtf	v4.4s, v4.4s
	ucvtf	v5.4s, v5.4s
	ucvtf	v1.4s, v1.4s
	ucvtf	v0.4s, v0.4s
	ucvtf	v6.4s, v6.4s
	ucvtf	v7.4s, v7.4s
	ucvtf	v3.4s, v3.4s
	ucvtf	v2.4s, v2.4s
	stp	q5, q0, [x11, #-32]
	stp	q4, q1, [x11, #-64]
	stp	q7, q2, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_829
// %bb.830:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_831:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #1
.LBB0_832:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	ucvtf	s0, w11
	str	s0, [x10], #4
	b.ne	.LBB0_832
	b	.LBB0_893
.LBB0_833:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_834:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x12], #32
	subs	x10, x10, #16                   // =16
	sshll	v2.4s, v0.4h, #0
	sshll	v3.4s, v1.4h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll	v4.2d, v2.2s, #0
	sshll	v5.2d, v3.2s, #0
	sshll2	v2.2d, v2.4s, #0
	sshll	v6.2d, v0.2s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll	v7.2d, v1.2s, #0
	sshll2	v0.2d, v0.4s, #0
	sshll2	v1.2d, v1.4s, #0
	stp	q7, q1, [x11, #96]
	stp	q6, q0, [x11, #32]
	stp	q5, q3, [x11, #64]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_834
// %bb.835:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_836:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #1
.LBB0_837:                              // =>This Inner Loop Header: Depth=1
	ldrsh	x11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_837
	b	.LBB0_893
.LBB0_838:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_839:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	sshll	v4.4s, v1.4h, #0
	sshll	v5.4s, v0.4h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v6.4s, v3.4h, #0
	sshll	v7.4s, v2.4h, #0
	sshll2	v3.4s, v3.8h, #0
	sshll2	v2.4s, v2.8h, #0
	scvtf	v4.4s, v4.4s
	scvtf	v5.4s, v5.4s
	scvtf	v1.4s, v1.4s
	scvtf	v0.4s, v0.4s
	scvtf	v6.4s, v6.4s
	scvtf	v7.4s, v7.4s
	scvtf	v3.4s, v3.4s
	scvtf	v2.4s, v2.4s
	stp	q5, q0, [x11, #-32]
	stp	q4, q1, [x11, #-64]
	stp	q7, q2, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_839
// %bb.840:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_841:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #1
.LBB0_842:                              // =>This Inner Loop Header: Depth=1
	ldrsh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	scvtf	s0, w11
	str	s0, [x10], #4
	b.ne	.LBB0_842
	b	.LBB0_893
.LBB0_843:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_844:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	scvtf	v1.2d, v1.2d
	scvtf	v3.2d, v3.2d
	scvtf	v5.2d, v5.2d
	scvtf	v7.2d, v7.2d
	scvtf	v4.2d, v4.2d
	scvtf	v6.2d, v6.2d
	scvtf	v2.2d, v2.2d
	scvtf	v0.2d, v0.2d
	fcvtn	v5.2s, v5.2d
	fcvtn	v7.2s, v7.2d
	fcvtn	v3.2s, v3.2d
	fcvtn	v1.2s, v1.2d
	fcvtn2	v5.4s, v4.2d
	fcvtn2	v7.4s, v6.2d
	fcvtn2	v3.4s, v2.2d
	fcvtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_844
// %bb.845:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_846:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_847:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	scvtf	s0, x11
	str	s0, [x10], #4
	b.ne	.LBB0_847
	b	.LBB0_893
.LBB0_848:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_849:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	fcvtl	v4.2d, v1.2s
	fcvtl	v5.2d, v0.2s
	fcvtl	v6.2d, v3.2s
	fcvtl	v7.2d, v2.2s
	fcvtl2	v1.2d, v1.4s
	fcvtl2	v0.2d, v0.4s
	fcvtl2	v3.2d, v3.4s
	fcvtl2	v2.2d, v2.4s
	fcvtzs	v4.2d, v4.2d
	fcvtzs	v5.2d, v5.2d
	fcvtzs	v6.2d, v6.2d
	fcvtzs	v7.2d, v7.2d
	fcvtzs	v1.2d, v1.2d
	fcvtzs	v0.2d, v0.2d
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v2.2d, v2.2d
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_849
// %bb.850:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_851:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_852:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x9], #4
	subs	x8, x8, #1                      // =1
	fcvtzs	x11, s0
	str	x11, [x10], #8
	b.ne	.LBB0_852
	b	.LBB0_893
.LBB0_853:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_854:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	add	x12, x12, #64                   // =64
	subs	x10, x10, #16                   // =16
	sshll	v4.2d, v1.2s, #0
	sshll	v5.2d, v0.2s, #0
	sshll	v6.2d, v3.2s, #0
	sshll	v7.2d, v2.2s, #0
	sshll2	v1.2d, v1.4s, #0
	sshll2	v0.2d, v0.4s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll2	v2.2d, v2.4s, #0
	stp	q7, q2, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q0, [x11, #32]
	stp	q4, q1, [x11], #128
	b.ne	.LBB0_854
// %bb.855:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_856:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #3
	add	x9, x2, x9, lsl #2
.LBB0_857:                              // =>This Inner Loop Header: Depth=1
	ldrsw	x11, [x9], #4
	subs	x8, x8, #1                      // =1
	str	x11, [x10], #8
	b.ne	.LBB0_857
	b	.LBB0_893
.LBB0_858:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_859:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-32]
	ldp	q2, q3, [x10, #-64]
	ldp	q4, q5, [x10, #32]
	ldp	q6, q7, [x10], #128
	scvtf	v1.4s, v1.4s
	scvtf	v3.4s, v3.4s
	scvtf	v2.4s, v2.4s
	scvtf	v0.4s, v0.4s
	scvtf	v7.4s, v7.4s
	scvtf	v6.4s, v6.4s
	scvtf	v5.4s, v5.4s
	scvtf	v4.4s, v4.4s
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-32]
	stp	q2, q3, [x11, #-64]
	stp	q4, q5, [x11, #32]
	stp	q6, q7, [x11], #128
	b.ne	.LBB0_859
// %bb.860:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_861:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_862:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x10], #4
	subs	x8, x8, #1                      // =1
	scvtf	s0, s0
	str	s0, [x9], #4
	b.ne	.LBB0_862
	b	.LBB0_893
.LBB0_863:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_864:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	fcvtzs	v1.2d, v1.2d
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v5.2d, v5.2d
	fcvtzs	v7.2d, v7.2d
	fcvtzs	v4.2d, v4.2d
	fcvtzs	v6.2d, v6.2d
	fcvtzs	v2.2d, v2.2d
	fcvtzs	v0.2d, v0.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn	v3.2s, v3.2d
	xtn	v1.2s, v1.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_864
// %bb.865:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_866:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_867:                              // =>This Inner Loop Header: Depth=1
	ldr	d0, [x9], #8
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, d0
	str	w11, [x10], #4
	b.ne	.LBB0_867
	b	.LBB0_893
.LBB0_868:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_869:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_869
// %bb.870:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_871:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_872:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_872
	b	.LBB0_893
.LBB0_873:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_874:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	ushll	v4.4s, v1.4h, #0
	ushll	v5.4s, v0.4h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v6.4s, v3.4h, #0
	ushll	v7.4s, v2.4h, #0
	ushll2	v3.4s, v3.8h, #0
	ushll2	v2.4s, v2.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q1, [x11, #-64]
	stp	q7, q2, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_874
// %bb.875:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_876:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #1
.LBB0_877:                              // =>This Inner Loop Header: Depth=1
	ldrh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_877
	b	.LBB0_893
.LBB0_878:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_879:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	sshll	v4.4s, v1.4h, #0
	sshll	v5.4s, v0.4h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v6.4s, v3.4h, #0
	sshll	v7.4s, v2.4h, #0
	sshll2	v3.4s, v3.8h, #0
	sshll2	v2.4s, v2.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q1, [x11, #-64]
	stp	q7, q2, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_879
// %bb.880:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_881:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #1
.LBB0_882:                              // =>This Inner Loop Header: Depth=1
	ldrsh	w11, [x9], #2
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_882
	b	.LBB0_893
.LBB0_883:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_884:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12, #64]
	ldp	q3, q2, [x12, #96]
	ldp	q5, q4, [x12, #32]
	ldp	q7, q6, [x12], #128
	xtn	v1.2s, v1.2d
	xtn	v3.2s, v3.2d
	xtn	v5.2s, v5.2d
	xtn	v7.2s, v7.2d
	xtn2	v5.4s, v4.2d
	xtn2	v7.4s, v6.2d
	xtn2	v3.4s, v2.2d
	xtn2	v1.4s, v0.2d
	subs	x10, x10, #16                   // =16
	stp	q1, q3, [x11, #32]
	stp	q7, q5, [x11], #64
	b.ne	.LBB0_884
// %bb.885:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_886:
	sub	x8, x8, x9
	add	x10, x3, x9, lsl #2
	add	x9, x2, x9, lsl #3
.LBB0_887:                              // =>This Inner Loop Header: Depth=1
	ldr	x11, [x9], #8
	subs	x8, x8, #1                      // =1
	str	w11, [x10], #4
	b.ne	.LBB0_887
	b	.LBB0_893
.LBB0_888:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_889:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-32]
	ldp	q2, q3, [x10, #-64]
	ldp	q4, q5, [x10, #32]
	ldp	q6, q7, [x10], #128
	fcvtzs	v1.4s, v1.4s
	fcvtzs	v3.4s, v3.4s
	fcvtzs	v2.4s, v2.4s
	fcvtzs	v0.4s, v0.4s
	fcvtzs	v7.4s, v7.4s
	fcvtzs	v6.4s, v6.4s
	fcvtzs	v5.4s, v5.4s
	fcvtzs	v4.4s, v4.4s
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-32]
	stp	q2, q3, [x11, #-64]
	stp	q4, q5, [x11, #32]
	stp	q6, q7, [x11], #128
	b.ne	.LBB0_889
// %bb.890:
	cmp	x9, x8
	b.eq	.LBB0_893
.LBB0_891:
	lsl	x10, x9, #2
	sub	x8, x8, x9
	add	x9, x3, x10
	add	x10, x2, x10
.LBB0_892:                              // =>This Inner Loop Header: Depth=1
	ldr	s0, [x10], #4
	subs	x8, x8, #1                      // =1
	fcvtzs	w11, s0
	str	w11, [x9], #4
	b.ne	.LBB0_892
.LBB0_893:
	ldp	x29, x30, [sp], #16             // 16-byte Folded Reload
	ret
.LBB0_894:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_895:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q3, q2, [x11, #-32]
	stp	q1, q0, [x11, #-64]
	stp	q7, q6, [x11, #32]
	stp	q5, q4, [x11], #128
	b.ne	.LBB0_895
// %bb.896:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_15
.LBB0_897:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_898:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	sshll	v2.8h, v0.8b, #0
	sshll2	v0.8h, v0.16b, #0
	sshll	v3.8h, v1.8b, #0
	sshll2	v1.8h, v1.16b, #0
	sshll	v4.4s, v2.4h, #0
	sshll2	v2.4s, v2.8h, #0
	sshll	v5.4s, v0.4h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v6.4s, v3.4h, #0
	sshll2	v3.4s, v3.8h, #0
	sshll	v7.4s, v1.4h, #0
	sshll2	v1.4s, v1.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q2, [x11, #-64]
	stp	q7, q1, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_898
// %bb.899:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_41
.LBB0_900:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_901:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	ushll	v2.8h, v0.8b, #0
	ushll2	v0.8h, v0.16b, #0
	ushll	v3.8h, v1.8b, #0
	ushll2	v1.8h, v1.16b, #0
	ushll	v4.4s, v2.4h, #0
	ushll2	v2.4s, v2.8h, #0
	ushll	v5.4s, v0.4h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v6.4s, v3.4h, #0
	ushll2	v3.4s, v3.8h, #0
	ushll	v7.4s, v1.4h, #0
	ushll2	v1.4s, v1.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q2, [x11, #-64]
	stp	q7, q1, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_901
// %bb.902:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_95
.LBB0_903:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_904:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q3, q2, [x11, #-32]
	stp	q1, q0, [x11, #-64]
	stp	q7, q6, [x11, #32]
	stp	q5, q4, [x11], #128
	b.ne	.LBB0_904
// %bb.905:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_107
.LBB0_906:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_907:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	ldp	q5, q4, [x12, #96]
	ldp	q7, q6, [x12, #64]
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q5, q4, [x11, #96]
	stp	q7, q6, [x11, #64]
	stp	q3, q2, [x11, #32]
	stp	q1, q0, [x11], #128
	b.ne	.LBB0_907
// %bb.908:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_125
.LBB0_909:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_910:                              // =>This Inner Loop Header: Depth=1
	ldr	q1, [x12], #16
	subs	x10, x10, #16                   // =16
	ext	v0.16b, v1.16b, v1.16b, #8
	umov	w13, v1.b[0]
	umov	w15, v1.b[2]
	umov	w17, v1.b[4]
	fmov	s2, w13
	umov	w13, v1.b[5]
	fmov	s3, w15
	umov	w15, v1.b[6]
	fmov	s4, w17
	umov	w17, v0.b[0]
	fmov	s5, w15
	umov	w15, v0.b[2]
	fmov	s6, w17
	mov	v4.s[1], w13
	umov	w13, v0.b[1]
	umov	w17, v0.b[4]
	fmov	s7, w15
	mov	v6.s[1], w13
	umov	w13, v0.b[3]
	umov	w15, v0.b[6]
	fmov	s16, w17
	mov	v7.s[1], w13
	umov	w13, v0.b[5]
	umov	w14, v1.b[1]
	umov	w16, v1.b[3]
	umov	w17, v1.b[7]
	fmov	s1, w15
	mov	v16.s[1], w13
	umov	w13, v0.b[7]
	mov	v2.s[1], w14
	mov	v3.s[1], w16
	mov	v5.s[1], w17
	mov	v1.s[1], w13
	shl	v0.2s, v2.2s, #24
	shl	v2.2s, v3.2s, #24
	shl	v3.2s, v4.2s, #24
	shl	v4.2s, v5.2s, #24
	shl	v5.2s, v6.2s, #24
	shl	v6.2s, v7.2s, #24
	shl	v7.2s, v16.2s, #24
	shl	v1.2s, v1.2s, #24
	sshr	v0.2s, v0.2s, #24
	sshr	v2.2s, v2.2s, #24
	sshr	v3.2s, v3.2s, #24
	sshr	v4.2s, v4.2s, #24
	sshr	v5.2s, v5.2s, #24
	sshr	v6.2s, v6.2s, #24
	sshr	v7.2s, v7.2s, #24
	sshr	v1.2s, v1.2s, #24
	sshll	v0.2d, v0.2s, #0
	sshll	v2.2d, v2.2s, #0
	sshll	v3.2d, v3.2s, #0
	sshll	v4.2d, v4.2s, #0
	sshll	v5.2d, v5.2s, #0
	sshll	v6.2d, v6.2s, #0
	sshll	v7.2d, v7.2s, #0
	sshll	v1.2d, v1.2s, #0
	scvtf	v0.2d, v0.2d
	scvtf	v2.2d, v2.2d
	scvtf	v3.2d, v3.2d
	scvtf	v4.2d, v4.2d
	scvtf	v5.2d, v5.2d
	scvtf	v6.2d, v6.2d
	scvtf	v7.2d, v7.2d
	scvtf	v1.2d, v1.2d
	stp	q3, q4, [x11, #32]
	stp	q0, q2, [x11]
	stp	q7, q1, [x11, #96]
	stp	q5, q6, [x11, #64]
	add	x11, x11, #128                  // =128
	b.ne	.LBB0_910
// %bb.911:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_136
.LBB0_912:
	and	x9, x8, #0xfffffff0
	movi	d0, #0x0000ff000000ff
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_913:                              // =>This Inner Loop Header: Depth=1
	ldr	q2, [x12], #16
	subs	x10, x10, #16                   // =16
	ext	v1.16b, v2.16b, v2.16b, #8
	umov	w13, v2.b[0]
	umov	w15, v2.b[2]
	umov	w17, v2.b[4]
	fmov	s3, w13
	umov	w13, v2.b[5]
	fmov	s4, w15
	umov	w15, v2.b[6]
	fmov	s5, w17
	umov	w17, v1.b[0]
	fmov	s6, w15
	umov	w15, v1.b[2]
	fmov	s7, w17
	mov	v5.s[1], w13
	umov	w13, v1.b[1]
	umov	w17, v1.b[4]
	fmov	s16, w15
	mov	v7.s[1], w13
	umov	w13, v1.b[3]
	umov	w15, v1.b[6]
	fmov	s17, w17
	mov	v16.s[1], w13
	umov	w13, v1.b[5]
	umov	w14, v2.b[1]
	umov	w16, v2.b[3]
	umov	w17, v2.b[7]
	fmov	s2, w15
	mov	v17.s[1], w13
	umov	w13, v1.b[7]
	mov	v3.s[1], w14
	mov	v4.s[1], w16
	mov	v6.s[1], w17
	mov	v2.s[1], w13
	and	v1.8b, v3.8b, v0.8b
	and	v3.8b, v4.8b, v0.8b
	and	v4.8b, v5.8b, v0.8b
	and	v5.8b, v6.8b, v0.8b
	and	v6.8b, v7.8b, v0.8b
	and	v7.8b, v16.8b, v0.8b
	and	v16.8b, v17.8b, v0.8b
	and	v2.8b, v2.8b, v0.8b
	ushll	v1.2d, v1.2s, #0
	ushll	v3.2d, v3.2s, #0
	ushll	v4.2d, v4.2s, #0
	ushll	v5.2d, v5.2s, #0
	ushll	v6.2d, v6.2s, #0
	ushll	v7.2d, v7.2s, #0
	ushll	v16.2d, v16.2s, #0
	ushll	v2.2d, v2.2s, #0
	ucvtf	v1.2d, v1.2d
	ucvtf	v3.2d, v3.2d
	ucvtf	v4.2d, v4.2d
	ucvtf	v5.2d, v5.2d
	ucvtf	v6.2d, v6.2d
	ucvtf	v7.2d, v7.2d
	ucvtf	v16.2d, v16.2d
	ucvtf	v2.2d, v2.2d
	stp	q4, q5, [x11, #32]
	stp	q1, q3, [x11]
	stp	q16, q2, [x11, #96]
	stp	q6, q7, [x11, #64]
	add	x11, x11, #128                  // =128
	b.ne	.LBB0_913
// %bb.914:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_182
.LBB0_915:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_916:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn	v7.4h, v7.4s
	xtn	v5.4h, v5.4s
	xtn	v0.8b, v1.8h
	xtn2	v0.16b, v3.8h
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	xtn	v1.8b, v5.8h
	xtn2	v1.16b, v7.8h
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_916
// %bb.917:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_198
.LBB0_918:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_919:                              // =>This Inner Loop Header: Depth=1
	ldp	q4, q7, [x12]
	ldp	q0, q1, [x12, #96]
	ldp	q5, q6, [x12, #32]
	ldp	q2, q3, [x12, #64]
	fcvtzs	v7.2d, v7.2d
	fcvtzs	v4.2d, v4.2d
	xtn	v7.2s, v7.2d
	xtn	v4.2s, v4.2d
	fcvtzs	v1.2d, v1.2d
	fcvtzs	v0.2d, v0.2d
	uzp1	v4.4h, v4.4h, v7.4h
	xtn	v1.2s, v1.2d
	xtn	v0.2s, v0.2d
	umov	w13, v4.h[0]
	fcvtzs	v6.2d, v6.2d
	fcvtzs	v5.2d, v5.2d
	uzp1	v0.4h, v0.4h, v1.4h
	umov	w14, v4.h[1]
	fmov	s1, w13
	xtn	v6.2s, v6.2d
	xtn	v5.2s, v5.2d
	umov	w13, v4.h[2]
	mov	v1.b[1], w14
	uzp1	v5.4h, v5.4h, v6.4h
	umov	w14, v4.h[3]
	mov	v1.b[2], w13
	umov	w13, v5.h[0]
	mov	v1.b[3], w14
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v2.2d, v2.2d
	umov	w14, v5.h[1]
	mov	v1.b[4], w13
	xtn	v3.2s, v3.2d
	xtn	v2.2s, v2.2d
	umov	w13, v5.h[2]
	mov	v1.b[5], w14
	uzp1	v2.4h, v2.4h, v3.4h
	umov	w14, v5.h[3]
	mov	v1.b[6], w13
	umov	w13, v2.h[0]
	mov	v1.b[7], w14
	umov	w14, v2.h[1]
	mov	v1.b[8], w13
	umov	w13, v2.h[2]
	mov	v1.b[9], w14
	umov	w14, v2.h[3]
	mov	v1.b[10], w13
	umov	w13, v0.h[0]
	mov	v1.b[11], w14
	umov	w14, v0.h[1]
	mov	v1.b[12], w13
	umov	w13, v0.h[2]
	mov	v1.b[13], w14
	umov	w14, v0.h[3]
	mov	v1.b[14], w13
	mov	v1.b[15], w14
	subs	x10, x10, #16                   // =16
	str	q1, [x11], #16
	add	x12, x12, #128                  // =128
	b.ne	.LBB0_919
// %bb.920:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_207
.LBB0_921:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_922:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_922
// %bb.923:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_216
.LBB0_924:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #128                   // =128
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_925:                              // =>This Inner Loop Header: Depth=1
	ldp	q17, q5, [x10, #-64]
	ldp	q20, q7, [x10, #-32]
	ldp	q4, q0, [x10, #-128]
	ldp	q6, q1, [x10, #-96]
	xtn	v17.2s, v17.2d
	xtn	v20.2s, v20.2d
	ldp	q19, q2, [x10]
	ldp	q21, q3, [x10, #32]
	ldp	q22, q16, [x10, #64]
	ldp	q23, q18, [x10, #96]
	xtn2	v20.4s, v7.2d
	xtn2	v17.4s, v5.2d
	xtn	v6.2s, v6.2d
	xtn	v4.2s, v4.2d
	xtn	v5.4h, v17.4s
	xtn2	v5.8h, v20.4s
	xtn2	v6.4s, v1.2d
	xtn2	v4.4s, v0.2d
	xtn	v0.4h, v4.4s
	xtn2	v0.8h, v6.4s
	xtn	v23.2s, v23.2d
	xtn	v22.2s, v22.2d
	xtn	v0.8b, v0.8h
	xtn2	v0.16b, v5.8h
	xtn2	v23.4s, v18.2d
	xtn2	v22.4s, v16.2d
	xtn	v21.2s, v21.2d
	xtn	v19.2s, v19.2d
	xtn	v1.4h, v22.4s
	xtn2	v1.8h, v23.4s
	xtn2	v21.4s, v3.2d
	xtn2	v19.4s, v2.2d
	xtn	v2.4h, v19.4s
	xtn2	v2.8h, v21.4s
	xtn	v2.8b, v2.8h
	xtn2	v2.16b, v1.8h
	add	x10, x10, #256                  // =256
	subs	x12, x12, #32                   // =32
	stp	q0, q2, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_925
// %bb.926:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_225
.LBB0_927:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_928:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	xtn	v1.8b, v1.8h
	xtn	v3.8b, v3.8h
	xtn2	v1.16b, v0.8h
	xtn2	v3.16b, v2.8h
	stp	q1, q3, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_928
// %bb.929:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_234
.LBB0_930:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_931:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	xtn	v1.8b, v1.8h
	xtn	v3.8b, v3.8h
	xtn2	v1.16b, v0.8h
	xtn2	v3.16b, v2.8h
	stp	q1, q3, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_931
// %bb.932:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_243
.LBB0_933:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #128                   // =128
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_934:                              // =>This Inner Loop Header: Depth=1
	ldp	q17, q5, [x10, #-64]
	ldp	q20, q7, [x10, #-32]
	ldp	q4, q0, [x10, #-128]
	ldp	q6, q1, [x10, #-96]
	xtn	v17.2s, v17.2d
	xtn	v20.2s, v20.2d
	ldp	q19, q2, [x10]
	ldp	q21, q3, [x10, #32]
	ldp	q22, q16, [x10, #64]
	ldp	q23, q18, [x10, #96]
	xtn2	v20.4s, v7.2d
	xtn2	v17.4s, v5.2d
	xtn	v6.2s, v6.2d
	xtn	v4.2s, v4.2d
	xtn	v5.4h, v17.4s
	xtn2	v5.8h, v20.4s
	xtn2	v6.4s, v1.2d
	xtn2	v4.4s, v0.2d
	xtn	v0.4h, v4.4s
	xtn2	v0.8h, v6.4s
	xtn	v23.2s, v23.2d
	xtn	v22.2s, v22.2d
	xtn	v0.8b, v0.8h
	xtn2	v0.16b, v5.8h
	xtn2	v23.4s, v18.2d
	xtn2	v22.4s, v16.2d
	xtn	v21.2s, v21.2d
	xtn	v19.2s, v19.2d
	xtn	v1.4h, v22.4s
	xtn2	v1.8h, v23.4s
	xtn2	v21.4s, v3.2d
	xtn2	v19.4s, v2.2d
	xtn	v2.4h, v19.4s
	xtn2	v2.8h, v21.4s
	xtn	v2.8b, v2.8h
	xtn2	v2.16b, v1.8h
	add	x10, x10, #256                  // =256
	subs	x12, x12, #32                   // =32
	stp	q0, q2, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_934
// %bb.935:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_252
.LBB0_936:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_937:                              // =>This Inner Loop Header: Depth=1
	ldp	q4, q0, [x10, #-64]
	ldp	q5, q1, [x10]
	ldp	q3, q2, [x10, #-32]
	subs	x12, x12, #32                   // =32
	fcvtzs	v4.4s, v4.4s
	fcvtzs	v5.4s, v5.4s
	xtn	v7.4h, v4.4s
	xtn	v6.4h, v5.4s
	umov	w13, v7.h[0]
	umov	w14, v6.h[0]
	fmov	s4, w13
	umov	w15, v7.h[1]
	fmov	s5, w14
	umov	w13, v7.h[2]
	mov	v4.b[1], w15
	umov	w14, v6.h[1]
	fcvtzs	v0.4s, v0.4s
	mov	v5.b[1], w14
	umov	w14, v7.h[3]
	ldp	q16, q7, [x10, #32]
	fcvtzs	v1.4s, v1.4s
	xtn	v0.4h, v0.4s
	mov	v4.b[2], w13
	umov	w13, v6.h[2]
	xtn	v1.4h, v1.4s
	mov	v5.b[2], w13
	umov	w13, v0.h[0]
	mov	v4.b[3], w14
	umov	w14, v6.h[3]
	mov	v5.b[3], w14
	umov	w14, v0.h[1]
	mov	v4.b[4], w13
	umov	w13, v1.h[0]
	fcvtzs	v3.4s, v3.4s
	mov	v5.b[4], w13
	umov	w13, v0.h[2]
	mov	v4.b[5], w14
	umov	w14, v1.h[1]
	fcvtzs	v16.4s, v16.4s
	xtn	v3.4h, v3.4s
	mov	v5.b[5], w14
	umov	w14, v0.h[3]
	mov	v4.b[6], w13
	umov	w13, v1.h[2]
	xtn	v16.4h, v16.4s
	mov	v5.b[6], w13
	umov	w13, v3.h[0]
	mov	v4.b[7], w14
	umov	w14, v1.h[3]
	mov	v5.b[7], w14
	umov	w14, v3.h[1]
	mov	v4.b[8], w13
	umov	w13, v16.h[0]
	fcvtzs	v2.4s, v2.4s
	mov	v5.b[8], w13
	umov	w13, v3.h[2]
	mov	v4.b[9], w14
	umov	w14, v16.h[1]
	fcvtzs	v7.4s, v7.4s
	xtn	v2.4h, v2.4s
	mov	v5.b[9], w14
	umov	w14, v3.h[3]
	mov	v4.b[10], w13
	umov	w13, v16.h[2]
	xtn	v7.4h, v7.4s
	mov	v5.b[10], w13
	umov	w13, v2.h[0]
	mov	v4.b[11], w14
	umov	w14, v16.h[3]
	mov	v5.b[11], w14
	umov	w14, v2.h[1]
	mov	v4.b[12], w13
	umov	w13, v7.h[0]
	mov	v5.b[12], w13
	umov	w13, v2.h[2]
	mov	v4.b[13], w14
	umov	w14, v7.h[1]
	mov	v5.b[13], w14
	mov	v4.b[14], w13
	umov	w13, v7.h[2]
	umov	w14, v2.h[3]
	mov	v5.b[14], w13
	umov	w13, v7.h[3]
	mov	v4.b[15], w14
	mov	v5.b[15], w13
	add	x10, x10, #128                  // =128
	stp	q4, q5, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_937
// %bb.938:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_261
.LBB0_939:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_940:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_940
// %bb.941:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_270
.LBB0_942:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_943:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn	v7.4h, v7.4s
	xtn	v5.4h, v5.4s
	xtn	v0.8b, v1.8h
	xtn2	v0.16b, v3.8h
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	xtn	v1.8b, v5.8h
	xtn2	v1.16b, v7.8h
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_943
// %bb.944:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_279
.LBB0_945:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_946:                              // =>This Inner Loop Header: Depth=1
	ldr	q0, [x12], #16
	subs	x10, x10, #16                   // =16
	sshll	v1.8h, v0.8b, #0
	sshll2	v0.8h, v0.16b, #0
	sshll	v2.4s, v1.4h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll	v3.4s, v0.4h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v4.2d, v2.2s, #0
	sshll2	v2.2d, v2.4s, #0
	sshll	v5.2d, v1.2s, #0
	sshll	v6.2d, v3.2s, #0
	sshll2	v1.2d, v1.4s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll	v7.2d, v0.2s, #0
	sshll2	v0.2d, v0.4s, #0
	stp	q7, q0, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q1, [x11, #32]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_946
// %bb.947:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_298
.LBB0_948:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_949:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	ldp	q5, q4, [x12, #96]
	ldp	q7, q6, [x12, #64]
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q5, q4, [x11, #96]
	stp	q7, q6, [x11, #64]
	stp	q3, q2, [x11, #32]
	stp	q1, q0, [x11], #128
	b.ne	.LBB0_949
// %bb.950:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_307
.LBB0_951:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_952:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	ldp	q5, q4, [x12, #96]
	ldp	q7, q6, [x12, #64]
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q5, q4, [x11, #96]
	stp	q7, q6, [x11, #64]
	stp	q3, q2, [x11, #32]
	stp	q1, q0, [x11], #128
	b.ne	.LBB0_952
// %bb.953:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_326
.LBB0_954:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_955:                              // =>This Inner Loop Header: Depth=1
	ldr	q0, [x12], #16
	subs	x10, x10, #16                   // =16
	ushll	v1.8h, v0.8b, #0
	ushll2	v0.8h, v0.16b, #0
	ushll	v2.4s, v1.4h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll	v3.4s, v0.4h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v4.2d, v2.2s, #0
	ushll2	v2.2d, v2.4s, #0
	ushll	v5.2d, v1.2s, #0
	ushll	v6.2d, v3.2s, #0
	ushll2	v1.2d, v1.4s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll	v7.2d, v0.2s, #0
	ushll2	v0.2d, v0.4s, #0
	stp	q7, q0, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q1, [x11, #32]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_955
// %bb.956:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_340
.LBB0_957:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_958:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	sshll	v2.8h, v0.8b, #0
	sshll2	v0.8h, v0.16b, #0
	sshll	v3.8h, v1.8b, #0
	sshll2	v1.8h, v1.16b, #0
	stp	q2, q0, [x11, #-32]
	stp	q3, q1, [x11], #64
	b.ne	.LBB0_958
// %bb.959:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_364
.LBB0_960:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_961:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	sshll	v2.8h, v0.8b, #0
	sshll2	v0.8h, v0.16b, #0
	sshll	v3.8h, v1.8b, #0
	sshll2	v1.8h, v1.16b, #0
	stp	q2, q0, [x11, #-32]
	stp	q3, q1, [x11], #64
	b.ne	.LBB0_961
// %bb.962:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_371
.LBB0_963:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_964:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	stp	q1, q0, [x11, #-32]
	stp	q3, q2, [x11], #64
	b.ne	.LBB0_964
// %bb.965:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_384
.LBB0_966:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_967:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	stp	q1, q0, [x11, #-32]
	stp	q3, q2, [x11], #64
	b.ne	.LBB0_967
// %bb.968:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_391
.LBB0_969:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_970:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	stp	q1, q0, [x11, #-32]
	stp	q3, q2, [x11], #64
	b.ne	.LBB0_970
// %bb.971:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_398
.LBB0_972:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_973:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	stp	q1, q0, [x11, #-32]
	stp	q3, q2, [x11], #64
	b.ne	.LBB0_973
// %bb.974:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_405
.LBB0_975:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_976:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	ushll	v2.8h, v0.8b, #0
	ushll2	v0.8h, v0.16b, #0
	ushll	v3.8h, v1.8b, #0
	ushll2	v1.8h, v1.16b, #0
	stp	q2, q0, [x11, #-32]
	stp	q3, q1, [x11], #64
	b.ne	.LBB0_976
// %bb.977:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_424
.LBB0_978:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #32                    // =32
	mov	x12, x9
.LBB0_979:                              // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	ushll	v2.8h, v0.8b, #0
	ushll2	v0.8h, v0.16b, #0
	ushll	v3.8h, v1.8b, #0
	ushll2	v1.8h, v1.16b, #0
	stp	q2, q0, [x11, #-32]
	stp	q3, q1, [x11], #64
	b.ne	.LBB0_979
// %bb.980:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_431
.LBB0_981:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_982:                              // =>This Inner Loop Header: Depth=1
	ldr	q0, [x12], #16
	subs	x10, x10, #16                   // =16
	sshll	v1.8h, v0.8b, #0
	sshll2	v0.8h, v0.16b, #0
	sshll	v2.4s, v1.4h, #0
	sshll2	v1.4s, v1.8h, #0
	sshll	v3.4s, v0.4h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v4.2d, v2.2s, #0
	sshll2	v2.2d, v2.4s, #0
	sshll	v5.2d, v1.2s, #0
	sshll	v6.2d, v3.2s, #0
	sshll2	v1.2d, v1.4s, #0
	sshll2	v3.2d, v3.4s, #0
	sshll	v7.2d, v0.2s, #0
	sshll2	v0.2d, v0.4s, #0
	stp	q7, q0, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q1, [x11, #32]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_982
// %bb.983:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_456
.LBB0_984:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_985:                              // =>This Inner Loop Header: Depth=1
	ldr	q0, [x12], #16
	subs	x10, x10, #16                   // =16
	ext	v1.16b, v0.16b, v0.16b, #8
	zip1	v2.8b, v0.8b, v0.8b
	zip2	v0.8b, v0.8b, v0.8b
	zip1	v3.8b, v1.8b, v0.8b
	zip2	v1.8b, v1.8b, v0.8b
	shl	v2.4h, v2.4h, #8
	shl	v0.4h, v0.4h, #8
	shl	v3.4h, v3.4h, #8
	shl	v1.4h, v1.4h, #8
	sshr	v2.4h, v2.4h, #8
	sshr	v0.4h, v0.4h, #8
	sshr	v3.4h, v3.4h, #8
	sshr	v1.4h, v1.4h, #8
	sshll	v2.4s, v2.4h, #0
	sshll	v0.4s, v0.4h, #0
	sshll	v3.4s, v3.4h, #0
	sshll	v1.4s, v1.4h, #0
	scvtf	v2.4s, v2.4s
	scvtf	v0.4s, v0.4s
	scvtf	v3.4s, v3.4s
	scvtf	v1.4s, v1.4s
	stp	q2, q0, [x11]
	stp	q3, q1, [x11, #32]
	add	x11, x11, #64                   // =64
	b.ne	.LBB0_985
// %bb.986:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_463
.LBB0_987:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_988:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	ldp	q5, q4, [x12, #96]
	ldp	q7, q6, [x12, #64]
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q5, q4, [x11, #96]
	stp	q7, q6, [x11, #64]
	stp	q3, q2, [x11, #32]
	stp	q1, q0, [x11], #128
	b.ne	.LBB0_988
// %bb.989:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_470
.LBB0_990:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_991:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x12]
	ldp	q3, q2, [x12, #32]
	ldp	q5, q4, [x12, #96]
	ldp	q7, q6, [x12, #64]
	add	x12, x12, #128                  // =128
	subs	x10, x10, #16                   // =16
	stp	q5, q4, [x11, #96]
	stp	q7, q6, [x11, #64]
	stp	q3, q2, [x11, #32]
	stp	q1, q0, [x11], #128
	b.ne	.LBB0_991
// %bb.992:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_492
.LBB0_993:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_994:                              // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q3, q2, [x11, #-32]
	stp	q1, q0, [x11, #-64]
	stp	q7, q6, [x11, #32]
	stp	q5, q4, [x11], #128
	b.ne	.LBB0_994
// %bb.995:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_505
.LBB0_996:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_997:                              // =>This Inner Loop Header: Depth=1
	ldr	q0, [x12], #16
	subs	x10, x10, #16                   // =16
	ushll	v1.8h, v0.8b, #0
	ushll2	v0.8h, v0.16b, #0
	ushll	v2.4s, v1.4h, #0
	ushll2	v1.4s, v1.8h, #0
	ushll	v3.4s, v0.4h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v4.2d, v2.2s, #0
	ushll2	v2.2d, v2.4s, #0
	ushll	v5.2d, v1.2s, #0
	ushll	v6.2d, v3.2s, #0
	ushll2	v1.2d, v1.4s, #0
	ushll2	v3.2d, v3.4s, #0
	ushll	v7.2d, v0.2s, #0
	ushll2	v0.2d, v0.4s, #0
	stp	q7, q0, [x11, #96]
	stp	q6, q3, [x11, #64]
	stp	q5, q1, [x11, #32]
	stp	q4, q2, [x11], #128
	b.ne	.LBB0_997
// %bb.998:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_512
.LBB0_999:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_1000:                             // =>This Inner Loop Header: Depth=1
	ldr	q0, [x12], #16
	subs	x10, x10, #16                   // =16
	ext	v1.16b, v0.16b, v0.16b, #8
	zip1	v2.8b, v0.8b, v0.8b
	zip2	v0.8b, v0.8b, v0.8b
	zip1	v3.8b, v1.8b, v0.8b
	zip2	v1.8b, v1.8b, v0.8b
	bic	v2.4h, #255, lsl #8
	bic	v0.4h, #255, lsl #8
	bic	v3.4h, #255, lsl #8
	bic	v1.4h, #255, lsl #8
	ushll	v2.4s, v2.4h, #0
	ushll	v0.4s, v0.4h, #0
	ushll	v3.4s, v3.4h, #0
	ushll	v1.4s, v1.4h, #0
	ucvtf	v2.4s, v2.4s
	ucvtf	v0.4s, v0.4s
	ucvtf	v3.4s, v3.4s
	ucvtf	v1.4s, v1.4s
	stp	q2, q0, [x11]
	stp	q3, q1, [x11, #32]
	add	x11, x11, #64                   // =64
	b.ne	.LBB0_1000
// %bb.1001:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_519
.LBB0_1002:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1003:                             // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn	v7.4h, v7.4s
	xtn	v5.4h, v5.4s
	xtn	v0.8b, v1.8h
	xtn2	v0.16b, v3.8h
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	xtn	v1.8b, v5.8h
	xtn2	v1.16b, v7.8h
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1003
// %bb.1004:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_532
.LBB0_1005:
	and	x9, x8, #0xfffffff0
	mov	x10, x9
	mov	x11, x3
	mov	x12, x2
.LBB0_1006:                             // =>This Inner Loop Header: Depth=1
	ldp	q4, q7, [x12]
	ldp	q0, q1, [x12, #96]
	ldp	q5, q6, [x12, #32]
	ldp	q2, q3, [x12, #64]
	fcvtzs	v7.2d, v7.2d
	fcvtzs	v4.2d, v4.2d
	xtn	v7.2s, v7.2d
	xtn	v4.2s, v4.2d
	fcvtzs	v1.2d, v1.2d
	fcvtzs	v0.2d, v0.2d
	uzp1	v4.4h, v4.4h, v7.4h
	xtn	v1.2s, v1.2d
	xtn	v0.2s, v0.2d
	umov	w13, v4.h[0]
	fcvtzs	v6.2d, v6.2d
	fcvtzs	v5.2d, v5.2d
	uzp1	v0.4h, v0.4h, v1.4h
	umov	w14, v4.h[1]
	fmov	s1, w13
	xtn	v6.2s, v6.2d
	xtn	v5.2s, v5.2d
	umov	w13, v4.h[2]
	mov	v1.b[1], w14
	uzp1	v5.4h, v5.4h, v6.4h
	umov	w14, v4.h[3]
	mov	v1.b[2], w13
	umov	w13, v5.h[0]
	mov	v1.b[3], w14
	fcvtzs	v3.2d, v3.2d
	fcvtzs	v2.2d, v2.2d
	umov	w14, v5.h[1]
	mov	v1.b[4], w13
	xtn	v3.2s, v3.2d
	xtn	v2.2s, v2.2d
	umov	w13, v5.h[2]
	mov	v1.b[5], w14
	uzp1	v2.4h, v2.4h, v3.4h
	umov	w14, v5.h[3]
	mov	v1.b[6], w13
	umov	w13, v2.h[0]
	mov	v1.b[7], w14
	umov	w14, v2.h[1]
	mov	v1.b[8], w13
	umov	w13, v2.h[2]
	mov	v1.b[9], w14
	umov	w14, v2.h[3]
	mov	v1.b[10], w13
	umov	w13, v0.h[0]
	mov	v1.b[11], w14
	umov	w14, v0.h[1]
	mov	v1.b[12], w13
	umov	w13, v0.h[2]
	mov	v1.b[13], w14
	umov	w14, v0.h[3]
	mov	v1.b[14], w13
	mov	v1.b[15], w14
	subs	x10, x10, #16                   // =16
	str	q1, [x11], #16
	add	x12, x12, #128                  // =128
	b.ne	.LBB0_1006
// %bb.1007:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_539
.LBB0_1008:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1009:                             // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1009
// %bb.1010:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_546
.LBB0_1011:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #128                   // =128
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1012:                             // =>This Inner Loop Header: Depth=1
	ldp	q17, q5, [x10, #-64]
	ldp	q20, q7, [x10, #-32]
	ldp	q4, q0, [x10, #-128]
	ldp	q6, q1, [x10, #-96]
	xtn	v17.2s, v17.2d
	xtn	v20.2s, v20.2d
	ldp	q19, q2, [x10]
	ldp	q21, q3, [x10, #32]
	ldp	q22, q16, [x10, #64]
	ldp	q23, q18, [x10, #96]
	xtn2	v20.4s, v7.2d
	xtn2	v17.4s, v5.2d
	xtn	v6.2s, v6.2d
	xtn	v4.2s, v4.2d
	xtn	v5.4h, v17.4s
	xtn2	v5.8h, v20.4s
	xtn2	v6.4s, v1.2d
	xtn2	v4.4s, v0.2d
	xtn	v0.4h, v4.4s
	xtn2	v0.8h, v6.4s
	xtn	v23.2s, v23.2d
	xtn	v22.2s, v22.2d
	xtn	v0.8b, v0.8h
	xtn2	v0.16b, v5.8h
	xtn2	v23.4s, v18.2d
	xtn2	v22.4s, v16.2d
	xtn	v21.2s, v21.2d
	xtn	v19.2s, v19.2d
	xtn	v1.4h, v22.4s
	xtn2	v1.8h, v23.4s
	xtn2	v21.4s, v3.2d
	xtn2	v19.4s, v2.2d
	xtn	v2.4h, v19.4s
	xtn2	v2.8h, v21.4s
	xtn	v2.8b, v2.8h
	xtn2	v2.16b, v1.8h
	add	x10, x10, #256                  // =256
	subs	x12, x12, #32                   // =32
	stp	q0, q2, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1012
// %bb.1013:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_553
.LBB0_1014:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1015:                             // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	xtn	v1.8b, v1.8h
	xtn	v3.8b, v3.8h
	xtn2	v1.16b, v0.8h
	xtn2	v3.16b, v2.8h
	stp	q1, q3, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1015
// %bb.1016:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_560
.LBB0_1017:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #32                    // =32
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1018:                             // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-32]
	ldp	q3, q2, [x10], #64
	subs	x12, x12, #32                   // =32
	xtn	v1.8b, v1.8h
	xtn	v3.8b, v3.8h
	xtn2	v1.16b, v0.8h
	xtn2	v3.16b, v2.8h
	stp	q1, q3, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1018
// %bb.1019:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_567
.LBB0_1020:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #128                   // =128
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1021:                             // =>This Inner Loop Header: Depth=1
	ldp	q17, q5, [x10, #-64]
	ldp	q20, q7, [x10, #-32]
	ldp	q4, q0, [x10, #-128]
	ldp	q6, q1, [x10, #-96]
	xtn	v17.2s, v17.2d
	xtn	v20.2s, v20.2d
	ldp	q19, q2, [x10]
	ldp	q21, q3, [x10, #32]
	ldp	q22, q16, [x10, #64]
	ldp	q23, q18, [x10, #96]
	xtn2	v20.4s, v7.2d
	xtn2	v17.4s, v5.2d
	xtn	v6.2s, v6.2d
	xtn	v4.2s, v4.2d
	xtn	v5.4h, v17.4s
	xtn2	v5.8h, v20.4s
	xtn2	v6.4s, v1.2d
	xtn2	v4.4s, v0.2d
	xtn	v0.4h, v4.4s
	xtn2	v0.8h, v6.4s
	xtn	v23.2s, v23.2d
	xtn	v22.2s, v22.2d
	xtn	v0.8b, v0.8h
	xtn2	v0.16b, v5.8h
	xtn2	v23.4s, v18.2d
	xtn2	v22.4s, v16.2d
	xtn	v21.2s, v21.2d
	xtn	v19.2s, v19.2d
	xtn	v1.4h, v22.4s
	xtn2	v1.8h, v23.4s
	xtn2	v21.4s, v3.2d
	xtn2	v19.4s, v2.2d
	xtn	v2.4h, v19.4s
	xtn2	v2.8h, v21.4s
	xtn	v2.8b, v2.8h
	xtn2	v2.16b, v1.8h
	add	x10, x10, #256                  // =256
	subs	x12, x12, #32                   // =32
	stp	q0, q2, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1021
// %bb.1022:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_574
.LBB0_1023:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1024:                             // =>This Inner Loop Header: Depth=1
	ldp	q4, q0, [x10, #-64]
	ldp	q5, q1, [x10]
	ldp	q3, q2, [x10, #-32]
	subs	x12, x12, #32                   // =32
	fcvtzs	v4.4s, v4.4s
	fcvtzs	v5.4s, v5.4s
	xtn	v7.4h, v4.4s
	xtn	v6.4h, v5.4s
	umov	w13, v7.h[0]
	umov	w14, v6.h[0]
	fmov	s4, w13
	umov	w15, v7.h[1]
	fmov	s5, w14
	umov	w13, v7.h[2]
	mov	v4.b[1], w15
	umov	w14, v6.h[1]
	fcvtzs	v0.4s, v0.4s
	mov	v5.b[1], w14
	umov	w14, v7.h[3]
	ldp	q16, q7, [x10, #32]
	fcvtzs	v1.4s, v1.4s
	xtn	v0.4h, v0.4s
	mov	v4.b[2], w13
	umov	w13, v6.h[2]
	xtn	v1.4h, v1.4s
	mov	v5.b[2], w13
	umov	w13, v0.h[0]
	mov	v4.b[3], w14
	umov	w14, v6.h[3]
	mov	v5.b[3], w14
	umov	w14, v0.h[1]
	mov	v4.b[4], w13
	umov	w13, v1.h[0]
	fcvtzs	v3.4s, v3.4s
	mov	v5.b[4], w13
	umov	w13, v0.h[2]
	mov	v4.b[5], w14
	umov	w14, v1.h[1]
	fcvtzs	v16.4s, v16.4s
	xtn	v3.4h, v3.4s
	mov	v5.b[5], w14
	umov	w14, v0.h[3]
	mov	v4.b[6], w13
	umov	w13, v1.h[2]
	xtn	v16.4h, v16.4s
	mov	v5.b[6], w13
	umov	w13, v3.h[0]
	mov	v4.b[7], w14
	umov	w14, v1.h[3]
	mov	v5.b[7], w14
	umov	w14, v3.h[1]
	mov	v4.b[8], w13
	umov	w13, v16.h[0]
	fcvtzs	v2.4s, v2.4s
	mov	v5.b[8], w13
	umov	w13, v3.h[2]
	mov	v4.b[9], w14
	umov	w14, v16.h[1]
	fcvtzs	v7.4s, v7.4s
	xtn	v2.4h, v2.4s
	mov	v5.b[9], w14
	umov	w14, v3.h[3]
	mov	v4.b[10], w13
	umov	w13, v16.h[2]
	xtn	v7.4h, v7.4s
	mov	v5.b[10], w13
	umov	w13, v2.h[0]
	mov	v4.b[11], w14
	umov	w14, v16.h[3]
	mov	v5.b[11], w14
	umov	w14, v2.h[1]
	mov	v4.b[12], w13
	umov	w13, v7.h[0]
	mov	v5.b[12], w13
	umov	w13, v2.h[2]
	mov	v4.b[13], w14
	umov	w14, v7.h[1]
	mov	v5.b[13], w14
	mov	v4.b[14], w13
	umov	w13, v7.h[2]
	umov	w14, v2.h[3]
	mov	v5.b[14], w13
	umov	w13, v7.h[3]
	mov	v4.b[15], w14
	mov	v5.b[15], w13
	add	x10, x10, #128                  // =128
	stp	q4, q5, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1024
// %bb.1025:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_581
.LBB0_1026:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1027:                             // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1027
// %bb.1028:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_588
.LBB0_1029:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #16                    // =16
	mov	x12, x9
.LBB0_1030:                             // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	xtn	v1.4h, v1.4s
	xtn	v3.4h, v3.4s
	xtn2	v3.8h, v2.4s
	xtn2	v1.8h, v0.4s
	xtn	v7.4h, v7.4s
	xtn	v5.4h, v5.4s
	xtn	v0.8b, v1.8h
	xtn2	v0.16b, v3.8h
	xtn2	v7.8h, v6.4s
	xtn2	v5.8h, v4.4s
	xtn	v1.8b, v5.8h
	xtn2	v1.16b, v7.8h
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q0, q1, [x11, #-16]
	add	x11, x11, #32                   // =32
	b.ne	.LBB0_1030
// %bb.1031:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_595
.LBB0_1032:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_1033:                             // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q3, q2, [x11, #-32]
	stp	q1, q0, [x11, #-64]
	stp	q7, q6, [x11, #32]
	stp	q5, q4, [x11], #128
	b.ne	.LBB0_1033
// %bb.1034:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_602
.LBB0_1035:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_1036:                             // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	sshll	v2.8h, v0.8b, #0
	sshll2	v0.8h, v0.16b, #0
	sshll	v3.8h, v1.8b, #0
	sshll2	v1.8h, v1.16b, #0
	sshll	v4.4s, v2.4h, #0
	sshll2	v2.4s, v2.8h, #0
	sshll	v5.4s, v0.4h, #0
	sshll2	v0.4s, v0.8h, #0
	sshll	v6.4s, v3.4h, #0
	sshll2	v3.4s, v3.8h, #0
	sshll	v7.4s, v1.4h, #0
	sshll2	v1.4s, v1.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q2, [x11, #-64]
	stp	q7, q1, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_1036
// %bb.1037:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_612
.LBB0_1038:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #16                    // =16
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_1039:                             // =>This Inner Loop Header: Depth=1
	ldp	q0, q1, [x10, #-16]
	add	x10, x10, #32                   // =32
	subs	x12, x12, #32                   // =32
	ushll	v2.8h, v0.8b, #0
	ushll2	v0.8h, v0.16b, #0
	ushll	v3.8h, v1.8b, #0
	ushll2	v1.8h, v1.16b, #0
	ushll	v4.4s, v2.4h, #0
	ushll2	v2.4s, v2.8h, #0
	ushll	v5.4s, v0.4h, #0
	ushll2	v0.4s, v0.8h, #0
	ushll	v6.4s, v3.4h, #0
	ushll2	v3.4s, v3.8h, #0
	ushll	v7.4s, v1.4h, #0
	ushll2	v1.4s, v1.8h, #0
	stp	q5, q0, [x11, #-32]
	stp	q4, q2, [x11, #-64]
	stp	q7, q1, [x11, #32]
	stp	q6, q3, [x11], #128
	b.ne	.LBB0_1039
// %bb.1040:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_634
.LBB0_1041:
	and	x9, x8, #0xffffffe0
	add	x10, x2, #64                    // =64
	add	x11, x3, #64                    // =64
	mov	x12, x9
.LBB0_1042:                             // =>This Inner Loop Header: Depth=1
	ldp	q1, q0, [x10, #-64]
	ldp	q3, q2, [x10, #-32]
	ldp	q5, q4, [x10]
	ldp	q7, q6, [x10, #32]
	add	x10, x10, #128                  // =128
	subs	x12, x12, #32                   // =32
	stp	q3, q2, [x11, #-32]
	stp	q1, q0, [x11, #-64]
	stp	q7, q6, [x11, #32]
	stp	q5, q4, [x11], #128
	b.ne	.LBB0_1042
// %bb.1043:
	cmp	x9, x8
	b.eq	.LBB0_893
	b	.LBB0_641
.Lfunc_end0:
	.size	cast_type_numeric_neon, .Lfunc_end0-cast_type_numeric_neon
                                        // -- End function
	.ident	"Ubuntu clang version 11.1.0-6"
	.section	".note.GNU-stack","",@progbits
	// .addrsig
