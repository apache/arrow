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
#_mm_cmpestra
#_mm_cmpestrc
#_mm_cmpestri
#_mm_cmpestrm
#_mm_cmpestro
#_mm_cmpestrs
#_mm_cmpestrz
_mm_cmpgt_epi64
#_mm_cmpistra
#_mm_cmpistrc
#_mm_cmpistri
#_mm_cmpistrm
#_mm_cmpistro
#_mm_cmpistrs
#_mm_cmpistrz
#_mm_crc32_u16
#_mm_crc32_u32
#_mm_crc32_u64
#_mm_crc32_u8
