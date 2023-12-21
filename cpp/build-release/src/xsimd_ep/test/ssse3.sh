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
_mm_abs_epi16
_mm_abs_epi32
_mm_abs_epi8
#_mm_abs_pi16
#_mm_abs_pi32
#_mm_abs_pi8
#_mm_alignr_epi8
#_mm_alignr_pi8
_mm_hadd_epi16
_mm_hadd_epi32
#_mm_hadd_pi16
#_mm_hadd_pi32
#_mm_hadds_epi16
#_mm_hadds_pi16
#_mm_hsub_epi16
#_mm_hsub_epi32
#_mm_hsub_pi16
#_mm_hsub_pi32
#_mm_hsubs_epi16
#_mm_hsubs_pi16
#_mm_maddubs_epi16
#_mm_maddubs_pi16
#_mm_mulhrs_epi16
#_mm_mulhrs_pi16
#_mm_shuffle_epi8
#_mm_shuffle_pi8
#_mm_sign_epi16
#_mm_sign_epi32
#_mm_sign_epi8
#_mm_sign_pi16
#_mm_sign_pi32
#_mm_sign_pi8
