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
#_mm_add_ss
_mm_and_ps
_mm_andnot_ps
#_mm_avg_pu16
#_mm_avg_pu8
_mm_cmpeq_ps
#_mm_cmpeq_ss
_mm_cmpge_ps
#_mm_cmpge_ss
_mm_cmpgt_ps
#_mm_cmpgt_ss
_mm_cmple_ps
#_mm_cmple_ss
_mm_cmplt_ps
#_mm_cmplt_ss
#_mm_cmpneq_ps
#_mm_cmpneq_ss
#_mm_cmpnge_ps
#_mm_cmpnge_ss
#_mm_cmpngt_ps
#_mm_cmpngt_ss
#_mm_cmpnle_ps
#_mm_cmpnle_ss
#_mm_cmpnlt_ps
#_mm_cmpnlt_ss
#_mm_cmpord_ps
#_mm_cmpord_ss
_mm_cmpunord_ps
#_mm_cmpunord_ss
#_mm_comieq_ss
#_mm_comige_ss
#_mm_comigt_ss
#_mm_comile_ss
#_mm_comilt_ss
#_mm_comineq_ss
#_mm_cvt_pi2ps
#_mm_cvt_ps2pi
#_mm_cvt_si2ss
#_mm_cvt_ss2si
#_mm_cvtpi16_ps
#_mm_cvtpi32_ps
#_mm_cvtpi32x2_ps
#_mm_cvtpi8_ps
#_mm_cvtps_pi16
#_mm_cvtps_pi32
#_mm_cvtps_pi8
#_mm_cvtpu16_ps
#_mm_cvtpu8_ps
#_mm_cvtsi32_ss
#_mm_cvtsi64_ss
#_mm_cvtss_f32
#_mm_cvtss_si32
#_mm_cvtss_si64
#_mm_cvtt_ps2pi
#_mm_cvtt_ss2si
#_mm_cvttps_pi32
#_mm_cvttss_si32
#_mm_cvttss_si64
_mm_div_ps
#_mm_div_ss
#_mm_extract_pi16
#_mm_free
#_mm_getcsr
#_mm_insert_pi16
_mm_load_ps
#_mm_load_ps1
#_mm_load_ss
#_mm_load1_ps
#_mm_loadh_pi
#_mm_loadl_pi
#_mm_loadr_ps
_mm_loadu_ps
#_mm_loadu_si16
#_mm_loadu_si64
#_mm_malloc
#_mm_maskmove_si64
#_mm_max_pi16
_mm_max_ps
#_mm_max_pu8
#_mm_max_ss
#_mm_min_pi16
_mm_min_ps
#_mm_min_pu8
#_mm_min_ss
#_mm_move_ss
#_mm_movehl_ps
#_mm_movelh_ps
#_mm_movemask_pi8
#_mm_movemask_ps
_mm_mul_ps
#_mm_mul_ss
#_mm_mulhi_pu16
_mm_or_ps
#_mm_prefetch
#_mm_rcp_ps
#_mm_rcp_ss
#_mm_rsqrt_ps
#_mm_rsqrt_ss
#_mm_sad_pu8
#_mm_set_ps
#_mm_set_ps1
#_mm_set_ss
#_mm_set1_ps
#_mm_setcsr
_mm_setr_ps
#_mm_setzero_ps
#_mm_sfence
#_mm_shuffle_pi16
#_mm_shuffle_ps
_mm_sqrt_ps
#_mm_sqrt_ss
_mm_store_ps
#_mm_store_ps1
#_mm_store_ss
#_mm_store1_ps
#_mm_storeh_pi
#_mm_storel_pi
#_mm_storer_ps
_mm_storeu_ps
#_mm_storeu_si16
#_mm_storeu_si64
#_mm_stream_pi
#_mm_stream_ps
_mm_sub_ps
#_mm_sub_ss
#_mm_ucomieq_ss
#_mm_ucomige_ss
#_mm_ucomigt_ss
#_mm_ucomile_ss
#_mm_ucomilt_ss
#_mm_ucomineq_ss
#_mm_undefined_ps
_mm_unpackhi_ps
_mm_unpacklo_ps
_mm_xor_ps
