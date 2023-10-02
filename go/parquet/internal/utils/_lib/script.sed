s|WORD $0x54[0-9a-f]\+[[:space:]]\+//[[:space:]]\+b.\([leqgtnso]\+\)[[:space:]]\+.\(LBB0_[0-9]\+\)|B\U\1 \2|
s|WORD $0x14000000[[:space:]]\+//[[:space:]]\+b[[:space:]]\+.\(LBB0_[0-9]\+\)|JMP \1|
s|\(WORD $0x9[0-9a-f]\+ // adrp.*\)|// \1|
s|WORD $0x[0-9a-f]\+ // ldr[[:space:]]\+d\([0-9]\+\), \[x[0-9]\+, :lo[0-9]\+:.\(LCPI0_[0-9]\+\)\]|VMOVD \2, V\1|
s|WORD $0x[0-9a-f]\+ // ldr[[:space:]]\+q\([0-9]\+\), \[x[0-9]\+, :lo[0-9]\+:.\(LCPI0_[0-9]\+\)\]|VMOVQ \2L, \2H, V\1|
