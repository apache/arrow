// The 43-column integer corpus, defined in the Arrow tree so that this harness
// and cpp/src/parquet's PFOR comparison benchmark generate the same columns from
// one definition rather than from two copies.
#pragma once
#include "parquet/pfor_corpus_internal.h"
namespace corpus = parquet::corpus;
