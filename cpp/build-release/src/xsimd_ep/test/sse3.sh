#!/bin/sh
exit_code=0
for instr in `grep -o -E '^_mm_[a-z1Z0-9_]+' $0`
do
    if ! grep -q -r $instr ../include-refactoring
    then
        echo $instr
        exit_code=1
    fi
done
exit $exit_code

# Instructions below starting with a # are known to be unused in xsimd
#_mm_addsub_pd
#_mm_addsub_ps
_mm_hadd_pd
_mm_hadd_ps
#_mm_hsub_pd
#_mm_hsub_ps
#_mm_lddqu_si128
#_mm_loaddup_pd
#_mm_movedup_pd
#_mm_movehdup_ps
#_mm_moveldup_ps
