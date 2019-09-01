/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 5.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-5.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow/ipc/api.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include <arrow-glib/array.hpp>
#include <arrow-glib/array-builder.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/buffer.hpp>


#include <sstream>

GArrowArray *array_qName1;
GArrowArray *array_flag1;
GArrowArray *array_rID1;
GArrowArray *array_beginPos1;
GArrowArray *array_mapQ1;
GArrowArray *array_cigar1;
GArrowArray *array_rNextId1;
GArrowArray *array_pNext1;
GArrowArray *array_tLen1;
GArrowArray *array_seq1;
GArrowArray *array_qual1;
GArrowArray *array_tags1;

GArrowStringArrayBuilder *builder_qName1;
GArrowInt32ArrayBuilder *builder_flag1;
GArrowInt32ArrayBuilder *builder_rID1;
GArrowInt32ArrayBuilder *builder_beginPos1;
GArrowInt32ArrayBuilder *builder_mapQ1;
GArrowStringArrayBuilder *builder_cigar1;
GArrowInt32ArrayBuilder *builder_rNextId1;
GArrowInt32ArrayBuilder *builder_pNext1;
GArrowInt32ArrayBuilder *builder_tLen1;
GArrowStringArrayBuilder *builder_seq1;
GArrowStringArrayBuilder *builder_qual1;
GArrowStringArrayBuilder *builder_tags1;
////////////////////////////////////////////////
GArrowArray *array_qName2;
GArrowArray *array_flag2;
GArrowArray *array_rID2;
GArrowArray *array_beginPos2;
GArrowArray *array_mapQ2;
GArrowArray *array_cigar2;
GArrowArray *array_rNextId2;
GArrowArray *array_pNext2;
GArrowArray *array_tLen2;
GArrowArray *array_seq2;
GArrowArray *array_qual2;
GArrowArray *array_tags2;

GArrowStringArrayBuilder *builder_qName2;
GArrowInt32ArrayBuilder *builder_flag2;
GArrowInt32ArrayBuilder *builder_rID2;
GArrowInt32ArrayBuilder *builder_beginPos2;
GArrowInt32ArrayBuilder *builder_mapQ2;
GArrowStringArrayBuilder *builder_cigar2;
GArrowInt32ArrayBuilder *builder_rNextId2;
GArrowInt32ArrayBuilder *builder_pNext2;
GArrowInt32ArrayBuilder *builder_tLen2;
GArrowStringArrayBuilder *builder_seq2;
GArrowStringArrayBuilder *builder_qual2;
GArrowStringArrayBuilder *builder_tags2;
////////////////////////////////////////////////
GArrowArray *array_qName3;
GArrowArray *array_flag3;
GArrowArray *array_rID3;
GArrowArray *array_beginPos3;
GArrowArray *array_mapQ3;
GArrowArray *array_cigar3;
GArrowArray *array_rNextId3;
GArrowArray *array_pNext3;
GArrowArray *array_tLen3;
GArrowArray *array_seq3;
GArrowArray *array_qual3;
GArrowArray *array_tags3;

GArrowStringArrayBuilder *builder_qName3;
GArrowInt32ArrayBuilder *builder_flag3;
GArrowInt32ArrayBuilder *builder_rID3;
GArrowInt32ArrayBuilder *builder_beginPos3;
GArrowInt32ArrayBuilder *builder_mapQ3;
GArrowStringArrayBuilder *builder_cigar3;
GArrowInt32ArrayBuilder *builder_rNextId3;
GArrowInt32ArrayBuilder *builder_pNext3;
GArrowInt32ArrayBuilder *builder_tLen3;
GArrowStringArrayBuilder *builder_seq3;
GArrowStringArrayBuilder *builder_qual3;
GArrowStringArrayBuilder *builder_tags3;
////////////////////////////////////////////////
GArrowArray *array_qName4;
GArrowArray *array_flag4;
GArrowArray *array_rID4;
GArrowArray *array_beginPos4;
GArrowArray *array_mapQ4;
GArrowArray *array_cigar4;
GArrowArray *array_rNextId4;
GArrowArray *array_pNext4;
GArrowArray *array_tLen4;
GArrowArray *array_seq4;
GArrowArray *array_qual4;
GArrowArray *array_tags4;

GArrowStringArrayBuilder *builder_qName4;
GArrowInt32ArrayBuilder *builder_flag4;
GArrowInt32ArrayBuilder *builder_rID4;
GArrowInt32ArrayBuilder *builder_beginPos4;
GArrowInt32ArrayBuilder *builder_mapQ4;
GArrowStringArrayBuilder *builder_cigar4;
GArrowInt32ArrayBuilder *builder_rNextId4;
GArrowInt32ArrayBuilder *builder_pNext4;
GArrowInt32ArrayBuilder *builder_tLen4;
GArrowStringArrayBuilder *builder_seq4;
GArrowStringArrayBuilder *builder_qual4;
GArrowStringArrayBuilder *builder_tags4;
////////////////////////////////////////////////
GArrowArray *array_qName5;
GArrowArray *array_flag5;
GArrowArray *array_rID5;
GArrowArray *array_beginPos5;
GArrowArray *array_mapQ5;
GArrowArray *array_cigar5;
GArrowArray *array_rNextId5;
GArrowArray *array_pNext5;
GArrowArray *array_tLen5;
GArrowArray *array_seq5;
GArrowArray *array_qual5;
GArrowArray *array_tags5;

GArrowStringArrayBuilder *builder_qName5;
GArrowInt32ArrayBuilder *builder_flag5;
GArrowInt32ArrayBuilder *builder_rID5;
GArrowInt32ArrayBuilder *builder_beginPos5;
GArrowInt32ArrayBuilder *builder_mapQ5;
GArrowStringArrayBuilder *builder_cigar5;
GArrowInt32ArrayBuilder *builder_rNextId5;
GArrowInt32ArrayBuilder *builder_pNext5;
GArrowInt32ArrayBuilder *builder_tLen5;
GArrowStringArrayBuilder *builder_seq5;
GArrowStringArrayBuilder *builder_qual5;
GArrowStringArrayBuilder *builder_tags5;
////////////////////////////////////////////////
GArrowArray *array_qName6;
GArrowArray *array_flag6;
GArrowArray *array_rID6;
GArrowArray *array_beginPos6;
GArrowArray *array_mapQ6;
GArrowArray *array_cigar6;
GArrowArray *array_rNextId6;
GArrowArray *array_pNext6;
GArrowArray *array_tLen6;
GArrowArray *array_seq6;
GArrowArray *array_qual6;
GArrowArray *array_tags6;

GArrowStringArrayBuilder *builder_qName6;
GArrowInt32ArrayBuilder *builder_flag6;
GArrowInt32ArrayBuilder *builder_rID6;
GArrowInt32ArrayBuilder *builder_beginPos6;
GArrowInt32ArrayBuilder *builder_mapQ6;
GArrowStringArrayBuilder *builder_cigar6;
GArrowInt32ArrayBuilder *builder_rNextId6;
GArrowInt32ArrayBuilder *builder_pNext6;
GArrowInt32ArrayBuilder *builder_tLen6;
GArrowStringArrayBuilder *builder_seq6;
GArrowStringArrayBuilder *builder_qual6;
GArrowStringArrayBuilder *builder_tags6;
////////////////////////////////////////////////
GArrowArray *array_qName7;
GArrowArray *array_flag7;
GArrowArray *array_rID7;
GArrowArray *array_beginPos7;
GArrowArray *array_mapQ7;
GArrowArray *array_cigar7;
GArrowArray *array_rNextId7;
GArrowArray *array_pNext7;
GArrowArray *array_tLen7;
GArrowArray *array_seq7;
GArrowArray *array_qual7;
GArrowArray *array_tags7;

GArrowStringArrayBuilder *builder_qName7;
GArrowInt32ArrayBuilder *builder_flag7;
GArrowInt32ArrayBuilder *builder_rID7;
GArrowInt32ArrayBuilder *builder_beginPos7;
GArrowInt32ArrayBuilder *builder_mapQ7;
GArrowStringArrayBuilder *builder_cigar7;
GArrowInt32ArrayBuilder *builder_rNextId7;
GArrowInt32ArrayBuilder *builder_pNext7;
GArrowInt32ArrayBuilder *builder_tLen7;
GArrowStringArrayBuilder *builder_seq7;
GArrowStringArrayBuilder *builder_qual7;
GArrowStringArrayBuilder *builder_tags7;
////////////////////////////////////////////////
GArrowArray *array_qName8;
GArrowArray *array_flag8;
GArrowArray *array_rID8;
GArrowArray *array_beginPos8;
GArrowArray *array_mapQ8;
GArrowArray *array_cigar8;
GArrowArray *array_rNextId8;
GArrowArray *array_pNext8;
GArrowArray *array_tLen8;
GArrowArray *array_seq8;
GArrowArray *array_qual8;
GArrowArray *array_tags8;

GArrowStringArrayBuilder *builder_qName8;
GArrowInt32ArrayBuilder *builder_flag8;
GArrowInt32ArrayBuilder *builder_rID8;
GArrowInt32ArrayBuilder *builder_beginPos8;
GArrowInt32ArrayBuilder *builder_mapQ8;
GArrowStringArrayBuilder *builder_cigar8;
GArrowInt32ArrayBuilder *builder_rNextId8;
GArrowInt32ArrayBuilder *builder_pNext8;
GArrowInt32ArrayBuilder *builder_tLen8;
GArrowStringArrayBuilder *builder_seq8;
GArrowStringArrayBuilder *builder_qual8;
GArrowStringArrayBuilder *builder_tags8;
////////////////////////////////////////////////
GArrowArray *array_qName9;
GArrowArray *array_flag9;
GArrowArray *array_rID9;
GArrowArray *array_beginPos9;
GArrowArray *array_mapQ9;
GArrowArray *array_cigar9;
GArrowArray *array_rNextId9;
GArrowArray *array_pNext9;
GArrowArray *array_tLen9;
GArrowArray *array_seq9;
GArrowArray *array_qual9;
GArrowArray *array_tags9;

GArrowStringArrayBuilder *builder_qName9;
GArrowInt32ArrayBuilder *builder_flag9;
GArrowInt32ArrayBuilder *builder_rID9;
GArrowInt32ArrayBuilder *builder_beginPos9;
GArrowInt32ArrayBuilder *builder_mapQ9;
GArrowStringArrayBuilder *builder_cigar9;
GArrowInt32ArrayBuilder *builder_rNextId9;
GArrowInt32ArrayBuilder *builder_pNext9;
GArrowInt32ArrayBuilder *builder_tLen9;
GArrowStringArrayBuilder *builder_seq9;
GArrowStringArrayBuilder *builder_qual9;
GArrowStringArrayBuilder *builder_tags9;
////////////////////////////////////////////////
GArrowArray *array_qName10;
GArrowArray *array_flag10;
GArrowArray *array_rID10;
GArrowArray *array_beginPos10;
GArrowArray *array_mapQ10;
GArrowArray *array_cigar10;
GArrowArray *array_rNextId10;
GArrowArray *array_pNext10;
GArrowArray *array_tLen10;
GArrowArray *array_seq10;
GArrowArray *array_qual10;
GArrowArray *array_tags10;

GArrowStringArrayBuilder *builder_qName10;
GArrowInt32ArrayBuilder *builder_flag10;
GArrowInt32ArrayBuilder *builder_rID10;
GArrowInt32ArrayBuilder *builder_beginPos10;
GArrowInt32ArrayBuilder *builder_mapQ10;
GArrowStringArrayBuilder *builder_cigar10;
GArrowInt32ArrayBuilder *builder_rNextId10;
GArrowInt32ArrayBuilder *builder_pNext10;
GArrowInt32ArrayBuilder *builder_tLen10;
GArrowStringArrayBuilder *builder_seq10;
GArrowStringArrayBuilder *builder_qual10;
GArrowStringArrayBuilder *builder_tags10;
////////////////////////////////////////////////
GArrowArray *array_qName11;
GArrowArray *array_flag11;
GArrowArray *array_rID11;
GArrowArray *array_beginPos11;
GArrowArray *array_mapQ11;
GArrowArray *array_cigar11;
GArrowArray *array_rNextId11;
GArrowArray *array_pNext11;
GArrowArray *array_tLen11;
GArrowArray *array_seq11;
GArrowArray *array_qual11;
GArrowArray *array_tags11;

GArrowStringArrayBuilder *builder_qName11;
GArrowInt32ArrayBuilder *builder_flag11;
GArrowInt32ArrayBuilder *builder_rID11;
GArrowInt32ArrayBuilder *builder_beginPos11;
GArrowInt32ArrayBuilder *builder_mapQ11;
GArrowStringArrayBuilder *builder_cigar11;
GArrowInt32ArrayBuilder *builder_rNextId11;
GArrowInt32ArrayBuilder *builder_pNext11;
GArrowInt32ArrayBuilder *builder_tLen11;
GArrowStringArrayBuilder *builder_seq11;
GArrowStringArrayBuilder *builder_qual11;
GArrowStringArrayBuilder *builder_tags11;
////////////////////////////////////////////////
GArrowArray *array_qName12;
GArrowArray *array_flag12;
GArrowArray *array_rID12;
GArrowArray *array_beginPos12;
GArrowArray *array_mapQ12;
GArrowArray *array_cigar12;
GArrowArray *array_rNextId12;
GArrowArray *array_pNext12;
GArrowArray *array_tLen12;
GArrowArray *array_seq12;
GArrowArray *array_qual12;
GArrowArray *array_tags12;

GArrowStringArrayBuilder *builder_qName12;
GArrowInt32ArrayBuilder *builder_flag12;
GArrowInt32ArrayBuilder *builder_rID12;
GArrowInt32ArrayBuilder *builder_beginPos12;
GArrowInt32ArrayBuilder *builder_mapQ12;
GArrowStringArrayBuilder *builder_cigar12;
GArrowInt32ArrayBuilder *builder_rNextId12;
GArrowInt32ArrayBuilder *builder_pNext12;
GArrowInt32ArrayBuilder *builder_tLen12;
GArrowStringArrayBuilder *builder_seq12;
GArrowStringArrayBuilder *builder_qual12;
GArrowStringArrayBuilder *builder_tags12;
////////////////////////////////////////////////
GArrowArray *array_qName13;
GArrowArray *array_flag13;
GArrowArray *array_rID13;
GArrowArray *array_beginPos13;
GArrowArray *array_mapQ13;
GArrowArray *array_cigar13;
GArrowArray *array_rNextId13;
GArrowArray *array_pNext13;
GArrowArray *array_tLen13;
GArrowArray *array_seq13;
GArrowArray *array_qual13;
GArrowArray *array_tags13;

GArrowStringArrayBuilder *builder_qName13;
GArrowInt32ArrayBuilder *builder_flag13;
GArrowInt32ArrayBuilder *builder_rID13;
GArrowInt32ArrayBuilder *builder_beginPos13;
GArrowInt32ArrayBuilder *builder_mapQ13;
GArrowStringArrayBuilder *builder_cigar13;
GArrowInt32ArrayBuilder *builder_rNextId13;
GArrowInt32ArrayBuilder *builder_pNext13;
GArrowInt32ArrayBuilder *builder_tLen13;
GArrowStringArrayBuilder *builder_seq13;
GArrowStringArrayBuilder *builder_qual13;
GArrowStringArrayBuilder *builder_tags13;
////////////////////////////////////////////////
GArrowArray *array_qName14;
GArrowArray *array_flag14;
GArrowArray *array_rID14;
GArrowArray *array_beginPos14;
GArrowArray *array_mapQ14;
GArrowArray *array_cigar14;
GArrowArray *array_rNextId14;
GArrowArray *array_pNext14;
GArrowArray *array_tLen14;
GArrowArray *array_seq14;
GArrowArray *array_qual14;
GArrowArray *array_tags14;

GArrowStringArrayBuilder *builder_qName14;
GArrowInt32ArrayBuilder *builder_flag14;
GArrowInt32ArrayBuilder *builder_rID14;
GArrowInt32ArrayBuilder *builder_beginPos14;
GArrowInt32ArrayBuilder *builder_mapQ14;
GArrowStringArrayBuilder *builder_cigar14;
GArrowInt32ArrayBuilder *builder_rNextId14;
GArrowInt32ArrayBuilder *builder_pNext14;
GArrowInt32ArrayBuilder *builder_tLen14;
GArrowStringArrayBuilder *builder_seq14;
GArrowStringArrayBuilder *builder_qual14;
GArrowStringArrayBuilder *builder_tags14;
////////////////////////////////////////////////
GArrowArray *array_qName15;
GArrowArray *array_flag15;
GArrowArray *array_rID15;
GArrowArray *array_beginPos15;
GArrowArray *array_mapQ15;
GArrowArray *array_cigar15;
GArrowArray *array_rNextId15;
GArrowArray *array_pNext15;
GArrowArray *array_tLen15;
GArrowArray *array_seq15;
GArrowArray *array_qual15;
GArrowArray *array_tags15;

GArrowStringArrayBuilder *builder_qName15;
GArrowInt32ArrayBuilder *builder_flag15;
GArrowInt32ArrayBuilder *builder_rID15;
GArrowInt32ArrayBuilder *builder_beginPos15;
GArrowInt32ArrayBuilder *builder_mapQ15;
GArrowStringArrayBuilder *builder_cigar15;
GArrowInt32ArrayBuilder *builder_rNextId15;
GArrowInt32ArrayBuilder *builder_pNext15;
GArrowInt32ArrayBuilder *builder_tLen15;
GArrowStringArrayBuilder *builder_seq15;
GArrowStringArrayBuilder *builder_qual15;
GArrowStringArrayBuilder *builder_tags15;
////////////////////////////////////////////////
GArrowArray *array_qName16;
GArrowArray *array_flag16;
GArrowArray *array_rID16;
GArrowArray *array_beginPos16;
GArrowArray *array_mapQ16;
GArrowArray *array_cigar16;
GArrowArray *array_rNextId16;
GArrowArray *array_pNext16;
GArrowArray *array_tLen16;
GArrowArray *array_seq16;
GArrowArray *array_qual16;
GArrowArray *array_tags16;

GArrowStringArrayBuilder *builder_qName16;
GArrowInt32ArrayBuilder *builder_flag16;
GArrowInt32ArrayBuilder *builder_rID16;
GArrowInt32ArrayBuilder *builder_beginPos16;
GArrowInt32ArrayBuilder *builder_mapQ16;
GArrowStringArrayBuilder *builder_cigar16;
GArrowInt32ArrayBuilder *builder_rNextId16;
GArrowInt32ArrayBuilder *builder_pNext16;
GArrowInt32ArrayBuilder *builder_tLen16;
GArrowStringArrayBuilder *builder_seq16;
GArrowStringArrayBuilder *builder_qual16;
GArrowStringArrayBuilder *builder_tags16;
////////////////////////////////////////////////
GArrowArray *array_qName17;
GArrowArray *array_flag17;
GArrowArray *array_rID17;
GArrowArray *array_beginPos17;
GArrowArray *array_mapQ17;
GArrowArray *array_cigar17;
GArrowArray *array_rNextId17;
GArrowArray *array_pNext17;
GArrowArray *array_tLen17;
GArrowArray *array_seq17;
GArrowArray *array_qual17;
GArrowArray *array_tags17;

GArrowStringArrayBuilder *builder_qName17;
GArrowInt32ArrayBuilder *builder_flag17;
GArrowInt32ArrayBuilder *builder_rID17;
GArrowInt32ArrayBuilder *builder_beginPos17;
GArrowInt32ArrayBuilder *builder_mapQ17;
GArrowStringArrayBuilder *builder_cigar17;
GArrowInt32ArrayBuilder *builder_rNextId17;
GArrowInt32ArrayBuilder *builder_pNext17;
GArrowInt32ArrayBuilder *builder_tLen17;
GArrowStringArrayBuilder *builder_seq17;
GArrowStringArrayBuilder *builder_qual17;
GArrowStringArrayBuilder *builder_tags17;
////////////////////////////////////////////////
GArrowArray *array_qName18;
GArrowArray *array_flag18;
GArrowArray *array_rID18;
GArrowArray *array_beginPos18;
GArrowArray *array_mapQ18;
GArrowArray *array_cigar18;
GArrowArray *array_rNextId18;
GArrowArray *array_pNext18;
GArrowArray *array_tLen18;
GArrowArray *array_seq18;
GArrowArray *array_qual18;
GArrowArray *array_tags18;

GArrowStringArrayBuilder *builder_qName18;
GArrowInt32ArrayBuilder *builder_flag18;
GArrowInt32ArrayBuilder *builder_rID18;
GArrowInt32ArrayBuilder *builder_beginPos18;
GArrowInt32ArrayBuilder *builder_mapQ18;
GArrowStringArrayBuilder *builder_cigar18;
GArrowInt32ArrayBuilder *builder_rNextId18;
GArrowInt32ArrayBuilder *builder_pNext18;
GArrowInt32ArrayBuilder *builder_tLen18;
GArrowStringArrayBuilder *builder_seq18;
GArrowStringArrayBuilder *builder_qual18;
GArrowStringArrayBuilder *builder_tags18;
////////////////////////////////////////////////
GArrowArray *array_qName19;
GArrowArray *array_flag19;
GArrowArray *array_rID19;
GArrowArray *array_beginPos19;
GArrowArray *array_mapQ19;
GArrowArray *array_cigar19;
GArrowArray *array_rNextId19;
GArrowArray *array_pNext19;
GArrowArray *array_tLen19;
GArrowArray *array_seq19;
GArrowArray *array_qual19;
GArrowArray *array_tags19;

GArrowStringArrayBuilder *builder_qName19;
GArrowInt32ArrayBuilder *builder_flag19;
GArrowInt32ArrayBuilder *builder_rID19;
GArrowInt32ArrayBuilder *builder_beginPos19;
GArrowInt32ArrayBuilder *builder_mapQ19;
GArrowStringArrayBuilder *builder_cigar19;
GArrowInt32ArrayBuilder *builder_rNextId19;
GArrowInt32ArrayBuilder *builder_pNext19;
GArrowInt32ArrayBuilder *builder_tLen19;
GArrowStringArrayBuilder *builder_seq19;
GArrowStringArrayBuilder *builder_qual19;
GArrowStringArrayBuilder *builder_tags19;
////////////////////////////////////////////////
GArrowArray *array_qName20;
GArrowArray *array_flag20;
GArrowArray *array_rID20;
GArrowArray *array_beginPos20;
GArrowArray *array_mapQ20;
GArrowArray *array_cigar20;
GArrowArray *array_rNextId20;
GArrowArray *array_pNext20;
GArrowArray *array_tLen20;
GArrowArray *array_seq20;
GArrowArray *array_qual20;
GArrowArray *array_tags20;

GArrowStringArrayBuilder *builder_qName20;
GArrowInt32ArrayBuilder *builder_flag20;
GArrowInt32ArrayBuilder *builder_rID20;
GArrowInt32ArrayBuilder *builder_beginPos20;
GArrowInt32ArrayBuilder *builder_mapQ20;
GArrowStringArrayBuilder *builder_cigar20;
GArrowInt32ArrayBuilder *builder_rNextId20;
GArrowInt32ArrayBuilder *builder_pNext20;
GArrowInt32ArrayBuilder *builder_tLen20;
GArrowStringArrayBuilder *builder_seq20;
GArrowStringArrayBuilder *builder_qual20;
GArrowStringArrayBuilder *builder_tags20;
////////////////////////////////////////////////
GArrowArray *array_qName21;
GArrowArray *array_flag21;
GArrowArray *array_rID21;
GArrowArray *array_beginPos21;
GArrowArray *array_mapQ21;
GArrowArray *array_cigar21;
GArrowArray *array_rNextId21;
GArrowArray *array_pNext21;
GArrowArray *array_tLen21;
GArrowArray *array_seq21;
GArrowArray *array_qual21;
GArrowArray *array_tags21;

GArrowStringArrayBuilder *builder_qName21;
GArrowInt32ArrayBuilder *builder_flag21;
GArrowInt32ArrayBuilder *builder_rID21;
GArrowInt32ArrayBuilder *builder_beginPos21;
GArrowInt32ArrayBuilder *builder_mapQ21;
GArrowStringArrayBuilder *builder_cigar21;
GArrowInt32ArrayBuilder *builder_rNextId21;
GArrowInt32ArrayBuilder *builder_pNext21;
GArrowInt32ArrayBuilder *builder_tLen21;
GArrowStringArrayBuilder *builder_seq21;
GArrowStringArrayBuilder *builder_qual21;
GArrowStringArrayBuilder *builder_tags21;
////////////////////////////////////////////////
GArrowArray *array_qName22;
GArrowArray *array_flag22;
GArrowArray *array_rID22;
GArrowArray *array_beginPos22;
GArrowArray *array_mapQ22;
GArrowArray *array_cigar22;
GArrowArray *array_rNextId22;
GArrowArray *array_pNext22;
GArrowArray *array_tLen22;
GArrowArray *array_seq22;
GArrowArray *array_qual22;
GArrowArray *array_tags22;

GArrowStringArrayBuilder *builder_qName22;
GArrowInt32ArrayBuilder *builder_flag22;
GArrowInt32ArrayBuilder *builder_rID22;
GArrowInt32ArrayBuilder *builder_beginPos22;
GArrowInt32ArrayBuilder *builder_mapQ22;
GArrowStringArrayBuilder *builder_cigar22;
GArrowInt32ArrayBuilder *builder_rNextId22;
GArrowInt32ArrayBuilder *builder_pNext22;
GArrowInt32ArrayBuilder *builder_tLen22;
GArrowStringArrayBuilder *builder_seq22;
GArrowStringArrayBuilder *builder_qual22;
GArrowStringArrayBuilder *builder_tags22;
////////////////////////////////////////////////
GArrowArray *array_qNameX;
GArrowArray *array_flagX;
GArrowArray *array_rIDX;
GArrowArray *array_beginPosX;
GArrowArray *array_mapQX;
GArrowArray *array_cigarX;
GArrowArray *array_rNextIdX;
GArrowArray *array_pNextX;
GArrowArray *array_tLenX;
GArrowArray *array_seqX;
GArrowArray *array_qualX;
GArrowArray *array_tagsX;

GArrowStringArrayBuilder *builder_qNameX;
GArrowInt32ArrayBuilder *builder_flagX;
GArrowInt32ArrayBuilder *builder_rIDX;
GArrowInt32ArrayBuilder *builder_beginPosX;
GArrowInt32ArrayBuilder *builder_mapQX;
GArrowStringArrayBuilder *builder_cigarX;
GArrowInt32ArrayBuilder *builder_rNextIdX;
GArrowInt32ArrayBuilder *builder_pNextX;
GArrowInt32ArrayBuilder *builder_tLenX;
GArrowStringArrayBuilder *builder_seqX;
GArrowStringArrayBuilder *builder_qualX;
GArrowStringArrayBuilder *builder_tagsX;
////////////////////////////////////////////////
GArrowArray *array_qNameY;
GArrowArray *array_flagY;
GArrowArray *array_rIDY;
GArrowArray *array_beginPosY;
GArrowArray *array_mapQY;
GArrowArray *array_cigarY;
GArrowArray *array_rNextIdY;
GArrowArray *array_pNextY;
GArrowArray *array_tLenY;
GArrowArray *array_seqY;
GArrowArray *array_qualY;
GArrowArray *array_tagsY;

GArrowStringArrayBuilder *builder_qNameY;
GArrowInt32ArrayBuilder *builder_flagY;
GArrowInt32ArrayBuilder *builder_rIDY;
GArrowInt32ArrayBuilder *builder_beginPosY;
GArrowInt32ArrayBuilder *builder_mapQY;
GArrowStringArrayBuilder *builder_cigarY;
GArrowInt32ArrayBuilder *builder_rNextIdY;
GArrowInt32ArrayBuilder *builder_pNextY;
GArrowInt32ArrayBuilder *builder_tLenY;
GArrowStringArrayBuilder *builder_seqY;
GArrowStringArrayBuilder *builder_qualY;
GArrowStringArrayBuilder *builder_tagsY;
////////////////////////////////////////////////
GArrowArray *array_qNameM;
GArrowArray *array_flagM;
GArrowArray *array_rIDM;
GArrowArray *array_beginPosM;
GArrowArray *array_mapQM;
GArrowArray *array_cigarM;
GArrowArray *array_rNextIdM;
GArrowArray *array_pNextM;
GArrowArray *array_tLenM;
GArrowArray *array_seqM;
GArrowArray *array_qualM;
GArrowArray *array_tagsM;

GArrowStringArrayBuilder *builder_qNameM;
GArrowInt32ArrayBuilder *builder_flagM;
GArrowInt32ArrayBuilder *builder_rIDM;
GArrowInt32ArrayBuilder *builder_beginPosM;
GArrowInt32ArrayBuilder *builder_mapQM;
GArrowStringArrayBuilder *builder_cigarM;
GArrowInt32ArrayBuilder *builder_rNextIdM;
GArrowInt32ArrayBuilder *builder_pNextM;
GArrowInt32ArrayBuilder *builder_tLenM;
GArrowStringArrayBuilder *builder_seqM;
GArrowStringArrayBuilder *builder_qualM;
GArrowStringArrayBuilder *builder_tagsM;
////////////////////////////////////////////////

static inline bool
garrow_record_batch_adjust_index(const std::shared_ptr<arrow::RecordBatch> arrow_record_batch,
                                 gint &i)
{
  auto n_columns = arrow_record_batch->num_columns();
  if (i < 0) {
    i += n_columns;
    if (i < 0) {
      return false;
    }
  }
  if (i >= n_columns) {
    return false;
  }
  return true;
}

G_BEGIN_DECLS

/**
 * SECTION: record-batch
 * @short_description: Record batch class
 *
 * #GArrowRecordBatch is a class for record batch. Record batch is
 * similar to #GArrowTable. Record batch also has also zero or more
 * columns and zero or more records.
 *
 * Record batch is used for shared memory IPC.
 */

typedef struct GArrowRecordBatchPrivate_ {
  std::shared_ptr<arrow::RecordBatch> record_batch;
} GArrowRecordBatchPrivate;

enum {
  PROP_0,
  PROP_RECORD_BATCH
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatch,
                           garrow_record_batch,
                           G_TYPE_OBJECT)

#define GARROW_RECORD_BATCH_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),               \
                               GARROW_TYPE_RECORD_BATCH,   \
                               GArrowRecordBatchPrivate))

static void
garrow_record_batch_finalize(GObject *object)
{
  GArrowRecordBatchPrivate *priv;

  priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);

  priv->record_batch = nullptr;

  G_OBJECT_CLASS(garrow_record_batch_parent_class)->finalize(object);
}

static void
garrow_record_batch_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  GArrowRecordBatchPrivate *priv;

  priv = GARROW_RECORD_BATCH_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RECORD_BATCH:
    priv->record_batch =
      *static_cast<std::shared_ptr<arrow::RecordBatch> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_get_property(GObject *object,
                          guint prop_id,
                          GValue *value,
                          GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_init(GArrowRecordBatch *object)
{
}

static void
garrow_record_batch_class_init(GArrowRecordBatchClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_finalize;
  gobject_class->set_property = garrow_record_batch_set_property;
  gobject_class->get_property = garrow_record_batch_get_property;

  spec = g_param_spec_pointer("record-batch",
                              "RecordBatch",
                              "The raw std::shared<arrow::RecordBatch> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RECORD_BATCH, spec);
}

/**
 * garrow_record_batch_new:
 * @schema: The schema of the record batch.
 * @n_rows: The number of the rows in the record batch.
 * @columns: (element-type GArrowArray): The columns in the record batch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowRecordBatch or %NULL on error.
 */
GArrowRecordBatch *
garrow_record_batch_new(GArrowSchema *schema,
                        guint32 n_rows,
                        GList *columns,
                        GError **error)
{
  std::vector<std::shared_ptr<arrow::Array>> arrow_columns;
  for (GList *node = columns; node; node = node->next) {
    GArrowArray *column = GARROW_ARRAY(node->data);
    arrow_columns.push_back(garrow_array_get_raw(column));
  }

  auto arrow_record_batch =
    arrow::RecordBatch::Make(garrow_schema_get_raw(schema),
                             n_rows, arrow_columns);
  auto status = arrow_record_batch->Validate();
  if (garrow_error_check(error, status, "[record-batch][new]")) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_equal:
 * @record_batch: A #GArrowRecordBatch.
 * @other_record_batch: A #GArrowRecordBatch to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.5.0
 */
gboolean
garrow_record_batch_equal(GArrowRecordBatch *record_batch,
                          GArrowRecordBatch *other_record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  const auto arrow_other_record_batch =
    garrow_record_batch_get_raw(other_record_batch);
  return arrow_record_batch->Equals(*arrow_other_record_batch);
}

/**
 * garrow_record_batch_get_schema:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: (transfer full): The schema of the record batch.
 */
GArrowSchema *
garrow_record_batch_get_schema(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_schema = arrow_record_batch->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_record_batch_get_column:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the target column. If it's negative, index is
 *   counted backward from the end of the columns. `-1` means the last
 *   column.
 *
 * Returns: (transfer full) (nullable): The i-th column in the record batch
 *   on success, %NULL on out of index.
 */
GArrowArray *
garrow_record_batch_get_column(GArrowRecordBatch *record_batch,
                               gint i)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  if (!garrow_record_batch_adjust_index(arrow_record_batch, i)) {
    return NULL;
  }
  auto arrow_column = arrow_record_batch->column(i);
  return garrow_array_new_raw(&arrow_column);
}

/**
 * garrow_record_batch_get_columns:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The columns in the record batch.
 */
GList *
garrow_record_batch_get_columns(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);

  GList *columns = NULL;
  for (int i = 0; i < arrow_record_batch->num_columns(); ++i) {
    auto arrow_column = arrow_record_batch->column(i);
    GArrowArray *column = garrow_array_new_raw(&arrow_column);
    columns = g_list_prepend(columns, column);
  }

  return g_list_reverse(columns);
}

/**
 * garrow_record_batch_get_column_name:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the target column. If it's negative, index is
 *   counted backward from the end of the columns. `-1` means the last
 *   column.
 *
 * Returns: (nullable): The name of the i-th column in the record batch
 *   on success, %NULL on out of index
 */
const gchar *
garrow_record_batch_get_column_name(GArrowRecordBatch *record_batch,
                                    gint i)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  if (!garrow_record_batch_adjust_index(arrow_record_batch, i)) {
    return NULL;
  }
  return arrow_record_batch->column_name(i).c_str();
}

/**
 * garrow_record_batch_get_n_columns:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: The number of columns in the record batch.
 */
guint
garrow_record_batch_get_n_columns(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  return arrow_record_batch->num_columns();
}

/**
 * garrow_record_batch_get_n_rows:
 * @record_batch: A #GArrowRecordBatch.
 *
 * Returns: The number of rows in the record batch.
 */
gint64
garrow_record_batch_get_n_rows(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  return arrow_record_batch->num_rows();
}

/**
 * garrow_record_batch_slice:
 * @record_batch: A #GArrowRecordBatch.
 * @offset: The offset of sub #GArrowRecordBatch.
 * @length: The length of sub #GArrowRecordBatch.
 *
 * Returns: (transfer full): The sub #GArrowRecordBatch. It covers
 *   only from `offset` to `offset + length` range. The sub
 *   #GArrowRecordBatch shares values with the base
 *   #GArrowRecordBatch.
 */
GArrowRecordBatch *
garrow_record_batch_slice(GArrowRecordBatch *record_batch,
                          gint64 offset,
                          gint64 length)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_sub_record_batch = arrow_record_batch->Slice(offset, length);
  return garrow_record_batch_new_raw(&arrow_sub_record_batch);
}

/**
 * garrow_record_batch_to_string:
 * @record_batch: A #GArrowRecordBatch.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The formatted record batch content or %NULL on error.
 *
 *   The returned string should be freed when with g_free() when no
 *   longer needed.
 *
 * Since: 0.5.0
 */
gchar *
garrow_record_batch_to_string(GArrowRecordBatch *record_batch, GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  std::stringstream sink;
  auto status = arrow::PrettyPrint(*arrow_record_batch, 0, &sink);
  if (garrow_error_check(error, status, "[record-batch][to-string]")) {
    return g_strdup(sink.str().c_str());
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_add_column:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the new column.
 * @field: The field to be added.
 * @column: The column to be added.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowRecordBatch that has a new column or %NULL on error.
 *
 * Since: 0.9.0
 */
GArrowRecordBatch *
garrow_record_batch_add_column(GArrowRecordBatch *record_batch,
                               guint i,
                               GArrowField *field,
                               GArrowArray *column,
                               GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  const auto arrow_field = garrow_field_get_raw(field);
  const auto arrow_column = garrow_array_get_raw(column);
  std::shared_ptr<arrow::RecordBatch> arrow_new_record_batch;
  auto status = arrow_record_batch->AddColumn(i, arrow_field, arrow_column, &arrow_new_record_batch);
  if (garrow_error_check(error, status, "[record-batch][add-column]")) {
    return garrow_record_batch_new_raw(&arrow_new_record_batch);
  } else {
    return NULL;
  }
}

GArrowBuffer * 
GSerializeRecordBatch(GArrowRecordBatch *record_batch)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);

  std::shared_ptr<arrow::ResizableBuffer> resizable_buffer;
  arrow::AllocateResizableBuffer(arrow::default_memory_pool(), 0, &resizable_buffer);

  std::shared_ptr<arrow::Buffer> buffer = std::dynamic_pointer_cast<arrow::Buffer>(resizable_buffer);
  arrow::ipc::SerializeRecordBatch(*arrow_record_batch, arrow::default_memory_pool(), &buffer);

  return garrow_buffer_new_raw(&buffer);

}

GArrowRecordBatch * 
GDeSerializeRecordBatch(GArrowBuffer *buffer, GArrowSchema *schema)
{

  std::shared_ptr<arrow::RecordBatch> arrow_new_record_batch;
  const auto arrow_schema = garrow_schema_get_raw(schema);

  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  //auto arrow_buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
  arrow::io::BufferReader buf_reader(arrow_buffer);

  arrow::ipc::ReadRecordBatch(arrow_schema, &buf_reader, &arrow_new_record_batch);
 
return garrow_record_batch_new_raw(&arrow_new_record_batch);

}

/**
 * garrow_record_batch_remove_column:
 * @record_batch: A #GArrowRecordBatch.
 * @i: The index of the new column.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The newly allocated
 *   #GArrowRecordBatch that doesn't have the column or %NULL on error.
 *
 * Since: 0.9.0
 */
GArrowRecordBatch *
garrow_record_batch_remove_column(GArrowRecordBatch *record_batch,
                                  guint i,
                                  GError **error)
{
  const auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  std::shared_ptr<arrow::RecordBatch> arrow_new_record_batch;
  auto status = arrow_record_batch->RemoveColumn(i, &arrow_new_record_batch);
  if (garrow_error_check(error, status, "[record-batch][remove-column]")) {
    return garrow_record_batch_new_raw(&arrow_new_record_batch);
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowRecordBatch *
garrow_record_batch_new_raw(std::shared_ptr<arrow::RecordBatch> *arrow_record_batch)
{
  auto record_batch =
    GARROW_RECORD_BATCH(g_object_new(GARROW_TYPE_RECORD_BATCH,
                                     "record-batch", arrow_record_batch,
                                     NULL));
  return record_batch;
}

std::shared_ptr<arrow::RecordBatch>
garrow_record_batch_get_raw(GArrowRecordBatch *record_batch)
{
  GArrowRecordBatchPrivate *priv;

  priv = GARROW_RECORD_BATCH_GET_PRIVATE(record_batch);
  return priv->record_batch;
}




GArrowSchema* getSchema(void)
{
    GArrowSchema *schema;
    //RecordBatch creation
    GArrowField *f0 = garrow_field_new("qNames", GARROW_DATA_TYPE(garrow_string_data_type_new()));
    GArrowField *f1 = garrow_field_new("flags", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f2 = garrow_field_new("rIDs", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f3 = garrow_field_new("beginPoss", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f4 = garrow_field_new("mapQs", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f11 = garrow_field_new("cigars", GARROW_DATA_TYPE(garrow_string_data_type_new()));
    GArrowField *f5 = garrow_field_new("rNextIds", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f6 = garrow_field_new("pNexts", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f7 = garrow_field_new("tLens", GARROW_DATA_TYPE(garrow_int32_data_type_new()));
    GArrowField *f8 = garrow_field_new("seqs", GARROW_DATA_TYPE(garrow_string_data_type_new()));
    GArrowField *f9 = garrow_field_new("quals", GARROW_DATA_TYPE(garrow_string_data_type_new()));
    GArrowField *f10 = garrow_field_new("tagss", GARROW_DATA_TYPE(garrow_string_data_type_new()));

    GList *fields = NULL;
    fields = g_list_append(fields, f0);
    fields = g_list_append(fields, f1);
    fields = g_list_append(fields, f2);
    fields = g_list_append(fields, f3);
    fields = g_list_append(fields, f4);
    fields = g_list_append(fields, f11);
    fields = g_list_append(fields, f5);
    fields = g_list_append(fields, f6);
    fields = g_list_append(fields, f7);
    fields = g_list_append(fields, f8);
    fields = g_list_append(fields, f9);
    fields = g_list_append(fields, f10);

    //Create schema and free unnecessary fields
    schema = garrow_schema_new(fields);
    g_list_free(fields);
    g_object_unref(f0);
    g_object_unref(f1);
    g_object_unref(f2);
    g_object_unref(f3);
    g_object_unref(f4);
    g_object_unref(f11);
    g_object_unref(f5);
    g_object_unref(f6);
    g_object_unref(f7);
    g_object_unref(f8);
    g_object_unref(f9);
    g_object_unref(f10);

    return schema;
}

GArrowRecordBatch * create_arrow_record_batch(gint64 count, GArrowArray *array_qName,GArrowArray *array_flag,GArrowArray *array_rID,GArrowArray *array_beginPos,GArrowArray *array_mapQ,
GArrowArray *array_cigar,GArrowArray *array_rNextId,GArrowArray *array_pNext,GArrowArray *array_tLen,GArrowArray *array_seq,GArrowArray *array_qual,GArrowArray *array_tags)
{
    GArrowSchema *schema;
    GArrowRecordBatch *batch_genomics;

    schema = getSchema();

    GList *columns_genomics;
    columns_genomics = g_list_append(columns_genomics,array_qName);
    columns_genomics = g_list_append(columns_genomics,array_flag);
    columns_genomics = g_list_append(columns_genomics,array_rID);
    columns_genomics = g_list_append(columns_genomics,array_beginPos);
    columns_genomics = g_list_append(columns_genomics,array_mapQ);
    columns_genomics = g_list_append(columns_genomics,array_cigar);
    columns_genomics = g_list_append(columns_genomics,array_rNextId);
    columns_genomics = g_list_append(columns_genomics,array_pNext);
    columns_genomics = g_list_append(columns_genomics,array_tLen);
    columns_genomics = g_list_append(columns_genomics,array_seq);
    columns_genomics = g_list_append(columns_genomics,array_qual);
    columns_genomics = g_list_append(columns_genomics,array_tags);

    batch_genomics = garrow_record_batch_new(schema,count,columns_genomics,NULL);

    g_list_free(columns_genomics);

    return batch_genomics;
}

void arrow_builders_start(void)
{
  builder_qName1 = garrow_string_array_builder_new();
  builder_flag1 = garrow_int32_array_builder_new();
  builder_rID1 = garrow_int32_array_builder_new();
  builder_beginPos1 = garrow_int32_array_builder_new();
  builder_mapQ1 = garrow_int32_array_builder_new();

  builder_cigar1 = garrow_string_array_builder_new();

  builder_rNextId1 = garrow_int32_array_builder_new();
  builder_pNext1 = garrow_int32_array_builder_new();
  builder_tLen1 = garrow_int32_array_builder_new();

  builder_seq1 = garrow_string_array_builder_new();
  builder_qual1 = garrow_string_array_builder_new();
  builder_tags1 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName2 = garrow_string_array_builder_new();
  builder_flag2 = garrow_int32_array_builder_new();
  builder_rID2 = garrow_int32_array_builder_new();
  builder_beginPos2 = garrow_int32_array_builder_new();
  builder_mapQ2 = garrow_int32_array_builder_new();

  builder_cigar2 = garrow_string_array_builder_new();

  builder_rNextId2 = garrow_int32_array_builder_new();
  builder_pNext2 = garrow_int32_array_builder_new();
  builder_tLen2 = garrow_int32_array_builder_new();

  builder_seq2 = garrow_string_array_builder_new();
  builder_qual2 = garrow_string_array_builder_new();
  builder_tags2 = garrow_string_array_builder_new();
///////////////////////////////////////////////////
  builder_qName3 = garrow_string_array_builder_new();
  builder_flag3 = garrow_int32_array_builder_new();
  builder_rID3 = garrow_int32_array_builder_new();
  builder_beginPos3 = garrow_int32_array_builder_new();
  builder_mapQ3 = garrow_int32_array_builder_new();

  builder_cigar3 = garrow_string_array_builder_new();

  builder_rNextId3 = garrow_int32_array_builder_new();
  builder_pNext3 = garrow_int32_array_builder_new();
  builder_tLen3 = garrow_int32_array_builder_new();

  builder_seq3 = garrow_string_array_builder_new();
  builder_qual3 = garrow_string_array_builder_new();
  builder_tags3 = garrow_string_array_builder_new();
///////////////////////////////////////////////////
  builder_qName4 = garrow_string_array_builder_new();
  builder_flag4 = garrow_int32_array_builder_new();
  builder_rID4 = garrow_int32_array_builder_new();
  builder_beginPos4 = garrow_int32_array_builder_new();
  builder_mapQ4 = garrow_int32_array_builder_new();

  builder_cigar4 = garrow_string_array_builder_new();

  builder_rNextId4 = garrow_int32_array_builder_new();
  builder_pNext4 = garrow_int32_array_builder_new();
  builder_tLen4 = garrow_int32_array_builder_new();

  builder_seq4 = garrow_string_array_builder_new();
  builder_qual4 = garrow_string_array_builder_new();
  builder_tags4 = garrow_string_array_builder_new();
///////////////////////////////////////////////////
  builder_qName5 = garrow_string_array_builder_new();
  builder_flag5 = garrow_int32_array_builder_new();
  builder_rID5 = garrow_int32_array_builder_new();
  builder_beginPos5 = garrow_int32_array_builder_new();
  builder_mapQ5 = garrow_int32_array_builder_new();

  builder_cigar5 = garrow_string_array_builder_new();

  builder_rNextId5 = garrow_int32_array_builder_new();
  builder_pNext5 = garrow_int32_array_builder_new();
  builder_tLen5 = garrow_int32_array_builder_new();

  builder_seq5 = garrow_string_array_builder_new();
  builder_qual5 = garrow_string_array_builder_new();
  builder_tags5 = garrow_string_array_builder_new();
///////////////////////////////////////////////////
  builder_qName6 = garrow_string_array_builder_new();
  builder_flag6 = garrow_int32_array_builder_new();
  builder_rID6 = garrow_int32_array_builder_new();
  builder_beginPos6 = garrow_int32_array_builder_new();
  builder_mapQ6 = garrow_int32_array_builder_new();

  builder_cigar6 = garrow_string_array_builder_new();

  builder_rNextId6 = garrow_int32_array_builder_new();
  builder_pNext6 = garrow_int32_array_builder_new();
  builder_tLen6 = garrow_int32_array_builder_new();

  builder_seq6 = garrow_string_array_builder_new();
  builder_qual6 = garrow_string_array_builder_new();
  builder_tags6 = garrow_string_array_builder_new();
///////////////////////////////////////////////////
  builder_qName7 = garrow_string_array_builder_new();
  builder_flag7 = garrow_int32_array_builder_new();
  builder_rID7 = garrow_int32_array_builder_new();
  builder_beginPos7 = garrow_int32_array_builder_new();
  builder_mapQ7 = garrow_int32_array_builder_new();

  builder_cigar7 = garrow_string_array_builder_new();

  builder_rNextId7 = garrow_int32_array_builder_new();
  builder_pNext7 = garrow_int32_array_builder_new();
  builder_tLen7 = garrow_int32_array_builder_new();

  builder_seq7 = garrow_string_array_builder_new();
  builder_qual7 = garrow_string_array_builder_new();
  builder_tags7 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName8 = garrow_string_array_builder_new();
  builder_flag8 = garrow_int32_array_builder_new();
  builder_rID8 = garrow_int32_array_builder_new();
  builder_beginPos8 = garrow_int32_array_builder_new();
  builder_mapQ8 = garrow_int32_array_builder_new();

  builder_cigar8 = garrow_string_array_builder_new();

  builder_rNextId8 = garrow_int32_array_builder_new();
  builder_pNext8 = garrow_int32_array_builder_new();
  builder_tLen8 = garrow_int32_array_builder_new();

  builder_seq8 = garrow_string_array_builder_new();
  builder_qual8 = garrow_string_array_builder_new();
  builder_tags8 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName9 = garrow_string_array_builder_new();
  builder_flag9 = garrow_int32_array_builder_new();
  builder_rID9 = garrow_int32_array_builder_new();
  builder_beginPos9 = garrow_int32_array_builder_new();
  builder_mapQ9 = garrow_int32_array_builder_new();

  builder_cigar9 = garrow_string_array_builder_new();

  builder_rNextId9 = garrow_int32_array_builder_new();
  builder_pNext9 = garrow_int32_array_builder_new();
  builder_tLen9 = garrow_int32_array_builder_new();

  builder_seq9 = garrow_string_array_builder_new();
  builder_qual9 = garrow_string_array_builder_new();
  builder_tags9 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName10 = garrow_string_array_builder_new();
  builder_flag10 = garrow_int32_array_builder_new();
  builder_rID10 = garrow_int32_array_builder_new();
  builder_beginPos10 = garrow_int32_array_builder_new();
  builder_mapQ10 = garrow_int32_array_builder_new();

  builder_cigar10 = garrow_string_array_builder_new();

  builder_rNextId10 = garrow_int32_array_builder_new();
  builder_pNext10 = garrow_int32_array_builder_new();
  builder_tLen10 = garrow_int32_array_builder_new();

  builder_seq10 = garrow_string_array_builder_new();
  builder_qual10 = garrow_string_array_builder_new();
  builder_tags10 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName11 = garrow_string_array_builder_new();
  builder_flag11 = garrow_int32_array_builder_new();
  builder_rID11 = garrow_int32_array_builder_new();
  builder_beginPos11 = garrow_int32_array_builder_new();
  builder_mapQ11 = garrow_int32_array_builder_new();

  builder_cigar11 = garrow_string_array_builder_new();

  builder_rNextId11 = garrow_int32_array_builder_new();
  builder_pNext11 = garrow_int32_array_builder_new();
  builder_tLen11 = garrow_int32_array_builder_new();

  builder_seq11 = garrow_string_array_builder_new();
  builder_qual11 = garrow_string_array_builder_new();
  builder_tags11 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName12 = garrow_string_array_builder_new();
  builder_flag12 = garrow_int32_array_builder_new();
  builder_rID12 = garrow_int32_array_builder_new();
  builder_beginPos12 = garrow_int32_array_builder_new();
  builder_mapQ12 = garrow_int32_array_builder_new();

  builder_cigar12 = garrow_string_array_builder_new();

  builder_rNextId12 = garrow_int32_array_builder_new();
  builder_pNext12 = garrow_int32_array_builder_new();
  builder_tLen12 = garrow_int32_array_builder_new();

  builder_seq12 = garrow_string_array_builder_new();
  builder_qual12 = garrow_string_array_builder_new();
  builder_tags12 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName13 = garrow_string_array_builder_new();
  builder_flag13 = garrow_int32_array_builder_new();
  builder_rID13 = garrow_int32_array_builder_new();
  builder_beginPos13 = garrow_int32_array_builder_new();
  builder_mapQ13 = garrow_int32_array_builder_new();

  builder_cigar13 = garrow_string_array_builder_new();

  builder_rNextId13 = garrow_int32_array_builder_new();
  builder_pNext13 = garrow_int32_array_builder_new();
  builder_tLen13 = garrow_int32_array_builder_new();

  builder_seq13 = garrow_string_array_builder_new();
  builder_qual13 = garrow_string_array_builder_new();
  builder_tags13 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName14 = garrow_string_array_builder_new();
  builder_flag14 = garrow_int32_array_builder_new();
  builder_rID14 = garrow_int32_array_builder_new();
  builder_beginPos14 = garrow_int32_array_builder_new();
  builder_mapQ14 = garrow_int32_array_builder_new();

  builder_cigar14 = garrow_string_array_builder_new();

  builder_rNextId14 = garrow_int32_array_builder_new();
  builder_pNext14 = garrow_int32_array_builder_new();
  builder_tLen14 = garrow_int32_array_builder_new();

  builder_seq14 = garrow_string_array_builder_new();
  builder_qual14 = garrow_string_array_builder_new();
  builder_tags14 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName15 = garrow_string_array_builder_new();
  builder_flag15 = garrow_int32_array_builder_new();
  builder_rID15 = garrow_int32_array_builder_new();
  builder_beginPos15 = garrow_int32_array_builder_new();
  builder_mapQ15 = garrow_int32_array_builder_new();

  builder_cigar15 = garrow_string_array_builder_new();

  builder_rNextId15 = garrow_int32_array_builder_new();
  builder_pNext15 = garrow_int32_array_builder_new();
  builder_tLen15 = garrow_int32_array_builder_new();

  builder_seq15 = garrow_string_array_builder_new();
  builder_qual15 = garrow_string_array_builder_new();
  builder_tags15 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName16 = garrow_string_array_builder_new();
  builder_flag16 = garrow_int32_array_builder_new();
  builder_rID16 = garrow_int32_array_builder_new();
  builder_beginPos16 = garrow_int32_array_builder_new();
  builder_mapQ16 = garrow_int32_array_builder_new();

  builder_cigar16 = garrow_string_array_builder_new();

  builder_rNextId16 = garrow_int32_array_builder_new();
  builder_pNext16 = garrow_int32_array_builder_new();
  builder_tLen16 = garrow_int32_array_builder_new();

  builder_seq16 = garrow_string_array_builder_new();
  builder_qual16 = garrow_string_array_builder_new();
  builder_tags16 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName17 = garrow_string_array_builder_new();
  builder_flag17 = garrow_int32_array_builder_new();
  builder_rID17 = garrow_int32_array_builder_new();
  builder_beginPos17 = garrow_int32_array_builder_new();
  builder_mapQ17 = garrow_int32_array_builder_new();

  builder_cigar17 = garrow_string_array_builder_new();

  builder_rNextId17 = garrow_int32_array_builder_new();
  builder_pNext17 = garrow_int32_array_builder_new();
  builder_tLen17 = garrow_int32_array_builder_new();

  builder_seq17 = garrow_string_array_builder_new();
  builder_qual17 = garrow_string_array_builder_new();
  builder_tags17 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName18 = garrow_string_array_builder_new();
  builder_flag18 = garrow_int32_array_builder_new();
  builder_rID18 = garrow_int32_array_builder_new();
  builder_beginPos18 = garrow_int32_array_builder_new();
  builder_mapQ18 = garrow_int32_array_builder_new();

  builder_cigar18 = garrow_string_array_builder_new();

  builder_rNextId18 = garrow_int32_array_builder_new();
  builder_pNext18 = garrow_int32_array_builder_new();
  builder_tLen18 = garrow_int32_array_builder_new();

  builder_seq18 = garrow_string_array_builder_new();
  builder_qual18 = garrow_string_array_builder_new();
  builder_tags18 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName19 = garrow_string_array_builder_new();
  builder_flag19 = garrow_int32_array_builder_new();
  builder_rID19 = garrow_int32_array_builder_new();
  builder_beginPos19 = garrow_int32_array_builder_new();
  builder_mapQ19 = garrow_int32_array_builder_new();

  builder_cigar19 = garrow_string_array_builder_new();

  builder_rNextId19 = garrow_int32_array_builder_new();
  builder_pNext19 = garrow_int32_array_builder_new();
  builder_tLen19 = garrow_int32_array_builder_new();

  builder_seq19 = garrow_string_array_builder_new();
  builder_qual19 = garrow_string_array_builder_new();
  builder_tags19 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName20 = garrow_string_array_builder_new();
  builder_flag20 = garrow_int32_array_builder_new();
  builder_rID20 = garrow_int32_array_builder_new();
  builder_beginPos20 = garrow_int32_array_builder_new();
  builder_mapQ20 = garrow_int32_array_builder_new();

  builder_cigar20 = garrow_string_array_builder_new();

  builder_rNextId20 = garrow_int32_array_builder_new();
  builder_pNext20 = garrow_int32_array_builder_new();
  builder_tLen20 = garrow_int32_array_builder_new();

  builder_seq20 = garrow_string_array_builder_new();
  builder_qual20 = garrow_string_array_builder_new();
  builder_tags20 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName21 = garrow_string_array_builder_new();
  builder_flag21 = garrow_int32_array_builder_new();
  builder_rID21 = garrow_int32_array_builder_new();
  builder_beginPos21 = garrow_int32_array_builder_new();
  builder_mapQ21 = garrow_int32_array_builder_new();

  builder_cigar21 = garrow_string_array_builder_new();

  builder_rNextId21 = garrow_int32_array_builder_new();
  builder_pNext21 = garrow_int32_array_builder_new();
  builder_tLen21 = garrow_int32_array_builder_new();

  builder_seq21 = garrow_string_array_builder_new();
  builder_qual21 = garrow_string_array_builder_new();
  builder_tags21 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qName22 = garrow_string_array_builder_new();
  builder_flag22 = garrow_int32_array_builder_new();
  builder_rID22 = garrow_int32_array_builder_new();
  builder_beginPos22 = garrow_int32_array_builder_new();
  builder_mapQ22 = garrow_int32_array_builder_new();

  builder_cigar22 = garrow_string_array_builder_new();

  builder_rNextId22 = garrow_int32_array_builder_new();
  builder_pNext22 = garrow_int32_array_builder_new();
  builder_tLen22 = garrow_int32_array_builder_new();

  builder_seq22 = garrow_string_array_builder_new();
  builder_qual22 = garrow_string_array_builder_new();
  builder_tags22 = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qNameX = garrow_string_array_builder_new();
  builder_flagX = garrow_int32_array_builder_new();
  builder_rIDX = garrow_int32_array_builder_new();
  builder_beginPosX = garrow_int32_array_builder_new();
  builder_mapQX = garrow_int32_array_builder_new();

  builder_cigarX = garrow_string_array_builder_new();

  builder_rNextIdX = garrow_int32_array_builder_new();
  builder_pNextX = garrow_int32_array_builder_new();
  builder_tLenX = garrow_int32_array_builder_new();

  builder_seqX = garrow_string_array_builder_new();
  builder_qualX = garrow_string_array_builder_new();
  builder_tagsX = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qNameY = garrow_string_array_builder_new();
  builder_flagY = garrow_int32_array_builder_new();
  builder_rIDY = garrow_int32_array_builder_new();
  builder_beginPosY = garrow_int32_array_builder_new();
  builder_mapQY = garrow_int32_array_builder_new();

  builder_cigarY = garrow_string_array_builder_new();

  builder_rNextIdY = garrow_int32_array_builder_new();
  builder_pNextY = garrow_int32_array_builder_new();
  builder_tLenY = garrow_int32_array_builder_new();

  builder_seqY = garrow_string_array_builder_new();
  builder_qualY = garrow_string_array_builder_new();
  builder_tagsY = garrow_string_array_builder_new();

///////////////////////////////////////////////////
  builder_qNameM = garrow_string_array_builder_new();
  builder_flagM = garrow_int32_array_builder_new();
  builder_rIDM = garrow_int32_array_builder_new();
  builder_beginPosM = garrow_int32_array_builder_new();
  builder_mapQM = garrow_int32_array_builder_new();

  builder_cigarM = garrow_string_array_builder_new();

  builder_rNextIdM = garrow_int32_array_builder_new();
  builder_pNextM = garrow_int32_array_builder_new();
  builder_tLenM = garrow_int32_array_builder_new();

  builder_seqM = garrow_string_array_builder_new();
  builder_qualM = garrow_string_array_builder_new();
  builder_tagsM = garrow_string_array_builder_new();

///////////////////////////////////////////////////
}

gboolean
arrow_builders_append(gint32 builder_id, const gchar *qName, gint32 flag, gint32 rID, gint32 beginPos, gint32 mapQ, const gchar *cigar, gint32 rNextId, gint32 pNext, gint32 tLen, const gchar *seq, const gchar *qual, const gchar *tags)
    {
        gboolean success = TRUE;
        GError *error = NULL;

        if(builder_id == 1) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName1, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag1, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID1, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos1, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ1, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar1, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId1, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext1, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen1, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq1, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual1, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags1, tags, &error);
            }
        }
        else if(builder_id == 2) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName2, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag2, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID2, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos2, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ2, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar2, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId2, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext2, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen2, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq2, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual2, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags2, tags, &error);
            }
        }
else if(builder_id == 3) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName3, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag3, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID3, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos3, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ3, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar3, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId3, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext3, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen3, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq3, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual3, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags3, tags, &error);
            }
        }
else if(builder_id == 4) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName4, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag4, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID4, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos4, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ4, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar4, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId4, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext4, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen4, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq4, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual4, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags4, tags, &error);
            }
        }
else if(builder_id == 5) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName5, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag5, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID5, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos5, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ5, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar5, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId5, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext5, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen5, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq5, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual5, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags5, tags, &error);
            }
        }
else if(builder_id == 6) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName6, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag6, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID6, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos6, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ6, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar6, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId6, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext6, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen6, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq6, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual6, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags6, tags, &error);
            }
        }
else if(builder_id == 7) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName7, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag7, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID7, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos7, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ7, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar7, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId7, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext7, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen7, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq7, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual7, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags7, tags, &error);
            }
        }
else if(builder_id == 8) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName8, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag8, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID8, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos8, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ8, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar8, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId8, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext8, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen8, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq8, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual8, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags8, tags, &error);
            }
        }
else if(builder_id == 9) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName9, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag9, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID9, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos9, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ9, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar9, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId9, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext9, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen9, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq9, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual9, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags9, tags, &error);
            }
        }
else if(builder_id == 10) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName10, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag10, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID10, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos10, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ10, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar10, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId10, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext10, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen10, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq10, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual10, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags10, tags, &error);
            }
        }
else if(builder_id == 11) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName11, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag11, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID11, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos11, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ11, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar11, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId11, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext11, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen11, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq11, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual11, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags11, tags, &error);
            }
        }
else if(builder_id == 12) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName12, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag12, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID12, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos12, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ12, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar12, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId12, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext12, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen12, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq12, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual12, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags12, tags, &error);
            }
        }
else if(builder_id == 13) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName13, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag13, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID13, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos13, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ13, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar13, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId13, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext13, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen13, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq13, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual13, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags13, tags, &error);
            }
        }
else if(builder_id == 14) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName14, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag14, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID14, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos14, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ14, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar14, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId14, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext14, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen14, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq14, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual14, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags14, tags, &error);
            }
        }
else if(builder_id == 15) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName15, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag15, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID15, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos15, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ15, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar15, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId15, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext15, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen15, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq15, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual15, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags15, tags, &error);
            }
        }
else if(builder_id == 16) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName16, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag16, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID16, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos16, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ16, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar16, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId16, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext16, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen16, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq16, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual16, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags16, tags, &error);
            }
        }
else if(builder_id == 17) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName17, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag17, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID17, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos17, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ17, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar17, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId17, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext17, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen17, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq17, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual17, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags17, tags, &error);
            }
        }
else if(builder_id == 18) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName18, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag18, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID18, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos18, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ18, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar18, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId18, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext18, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen18, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq18, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual18, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags18, tags, &error);
            }
        }
else if(builder_id == 19) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName19, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag19, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID19, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos19, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ19, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar19, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId19, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext19, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen19, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq19, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual19, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags19, tags, &error);
            }
        }
else if(builder_id == 20) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName20, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag20, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID20, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos20, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ20, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar20, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId20, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext20, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen20, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq20, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual20, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags20, tags, &error);
            }
        }
else if(builder_id == 21) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName21, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag21, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID21, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos21, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ21, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar21, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId21, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext21, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen21, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq21, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual21, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags21, tags, &error);
            }
        }
else if(builder_id == 22) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qName22, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flag22, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rID22, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPos22, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQ22, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigar22, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextId22, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNext22, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLen22, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seq22, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qual22, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tags22, tags, &error);
            }
        }
else if(builder_id == 23) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qNameX, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flagX, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rIDX, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosX, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQX, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigarX, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextIdX, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNextX, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLenX, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seqX, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qualX, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tagsX, tags, &error);
            }
        }
else if(builder_id == 24) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qNameY, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flagY, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rIDY, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosY, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQY, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigarY, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextIdY, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNextY, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLenY, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seqY, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qualY, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tagsY, tags, &error);
            }
        }
else if(builder_id == 25) {
            if (success) {
                success = garrow_string_array_builder_append(builder_qNameM, qName, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_flagM, flag, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_rIDM, rID, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_beginPosM, beginPos, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_mapQM, mapQ, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_cigarM, cigar, &error);
            }

            if (success) {
                success = garrow_int32_array_builder_append(builder_rNextIdM, rNextId, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_pNextM, pNext, &error);
            }
            if (success) {
                success = garrow_int32_array_builder_append(builder_tLenM, tLen, &error);
            }

            if (success) {
                success = garrow_string_array_builder_append(builder_seqM, seq, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_qualM, qual, &error);
            }
            if (success) {
                success = garrow_string_array_builder_append(builder_tagsM, tags, &error);
            }
        }

   return success;
}

GArrowRecordBatch *
arrow_builders_finish(gint32 builder_id, gint64 count)
{
 GError *error = NULL;
 GArrowRecordBatch *batch_genomics;
    if(builder_id == 1) {
        array_qName1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName1), &error);
        array_flag1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag1), &error);
        array_rID1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID1), &error);
        array_beginPos1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos1), &error);
        array_mapQ1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ1), &error);
        array_cigar1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar1), &error);
        array_rNextId1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId1), &error);
        array_pNext1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext1), &error);
        array_tLen1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen1), &error);
        array_seq1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq1), &error);
        array_qual1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual1), &error);
        array_tags1 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags1), &error);

        g_object_unref(builder_qName1);
        g_object_unref(builder_flag1);
        g_object_unref(builder_rID1);
        g_object_unref(builder_beginPos1);
        g_object_unref(builder_mapQ1);
        g_object_unref(builder_cigar1);
        g_object_unref(builder_rNextId1);
        g_object_unref(builder_pNext1);
        g_object_unref(builder_tLen1);
        g_object_unref(builder_seq1);
        g_object_unref(builder_qual1);
        g_object_unref(builder_tags1);

        batch_genomics = create_arrow_record_batch(count, array_qName1,array_flag1,array_rID1,array_beginPos1,array_mapQ1,
                                         array_cigar1,array_rNextId1,array_pNext1,array_tLen1,array_seq1,array_qual1,array_tags1);

        g_object_unref(array_qName1);
        g_object_unref(array_flag1);
        g_object_unref(array_rID1);
        g_object_unref(array_beginPos1);
        g_object_unref(array_mapQ1);
        g_object_unref(array_cigar1);
        g_object_unref(array_rNextId1);
        g_object_unref(array_pNext1);
        g_object_unref(array_tLen1);
        g_object_unref(array_seq1);
        g_object_unref(array_qual1);
        g_object_unref(array_tags1);
    }
    else if(builder_id == 2) {
        array_qName2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName2), &error);
        array_flag2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag2), &error);
        array_rID2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID2), &error);
        array_beginPos2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos2), &error);
        array_mapQ2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ2), &error);
        array_cigar2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar2), &error);
        array_rNextId2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId2), &error);
        array_pNext2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext2), &error);
        array_tLen2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen2), &error);
        array_seq2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq2), &error);
        array_qual2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual2), &error);
        array_tags2 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags2), &error);

        g_object_unref(builder_qName2);
        g_object_unref(builder_flag2);
        g_object_unref(builder_rID2);
        g_object_unref(builder_beginPos2);
        g_object_unref(builder_mapQ2);
        g_object_unref(builder_cigar2);
        g_object_unref(builder_rNextId2);
        g_object_unref(builder_pNext2);
        g_object_unref(builder_tLen2);
        g_object_unref(builder_seq2);
        g_object_unref(builder_qual2);
        g_object_unref(builder_tags2);

        batch_genomics = create_arrow_record_batch(count, array_qName2,array_flag2,array_rID2,array_beginPos2,array_mapQ2,
                                         array_cigar2,array_rNextId2,array_pNext2,array_tLen2,array_seq2,array_qual2,array_tags2);

        g_object_unref(array_qName2);
        g_object_unref(array_flag2);
        g_object_unref(array_rID2);
        g_object_unref(array_beginPos2);
        g_object_unref(array_mapQ2);
        g_object_unref(array_cigar2);
        g_object_unref(array_rNextId2);
        g_object_unref(array_pNext2);
        g_object_unref(array_tLen2);
        g_object_unref(array_seq2);
        g_object_unref(array_qual2);
        g_object_unref(array_tags2);
    }
else if(builder_id == 3) {
        array_qName3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName3), &error);
        array_flag3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag3), &error);
        array_rID3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID3), &error);
        array_beginPos3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos3), &error);
        array_mapQ3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ3), &error);
        array_cigar3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar3), &error);
        array_rNextId3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId3), &error);
        array_pNext3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext3), &error);
        array_tLen3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen3), &error);
        array_seq3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq3), &error);
        array_qual3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual3), &error);
        array_tags3 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags3), &error);

        g_object_unref(builder_qName3);
        g_object_unref(builder_flag3);
        g_object_unref(builder_rID3);
        g_object_unref(builder_beginPos3);
        g_object_unref(builder_mapQ3);
        g_object_unref(builder_cigar3);
        g_object_unref(builder_rNextId3);
        g_object_unref(builder_pNext3);
        g_object_unref(builder_tLen3);
        g_object_unref(builder_seq3);
        g_object_unref(builder_qual3);
        g_object_unref(builder_tags3);

        batch_genomics = create_arrow_record_batch(count, array_qName3,array_flag3,array_rID3,array_beginPos3,array_mapQ3,
                                         array_cigar3,array_rNextId3,array_pNext3,array_tLen3,array_seq3,array_qual3,array_tags3);

        g_object_unref(array_qName3);
        g_object_unref(array_flag3);
        g_object_unref(array_rID3);
        g_object_unref(array_beginPos3);
        g_object_unref(array_mapQ3);
        g_object_unref(array_cigar3);
        g_object_unref(array_rNextId3);
        g_object_unref(array_pNext3);
        g_object_unref(array_tLen3);
        g_object_unref(array_seq3);
        g_object_unref(array_qual3);
        g_object_unref(array_tags3);
    }
else if(builder_id == 4) {
        array_qName4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName4), &error);
        array_flag4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag4), &error);
        array_rID4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID4), &error);
        array_beginPos4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos4), &error);
        array_mapQ4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ4), &error);
        array_cigar4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar4), &error);
        array_rNextId4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId4), &error);
        array_pNext4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext4), &error);
        array_tLen4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen4), &error);
        array_seq4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq4), &error);
        array_qual4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual4), &error);
        array_tags4 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags4), &error);

        g_object_unref(builder_qName4);
        g_object_unref(builder_flag4);
        g_object_unref(builder_rID4);
        g_object_unref(builder_beginPos4);
        g_object_unref(builder_mapQ4);
        g_object_unref(builder_cigar4);
        g_object_unref(builder_rNextId4);
        g_object_unref(builder_pNext4);
        g_object_unref(builder_tLen4);
        g_object_unref(builder_seq4);
        g_object_unref(builder_qual4);
        g_object_unref(builder_tags4);

        batch_genomics = create_arrow_record_batch(count, array_qName4,array_flag4,array_rID4,array_beginPos4,array_mapQ4,
                                         array_cigar4,array_rNextId4,array_pNext4,array_tLen4,array_seq4,array_qual4,array_tags4);

        g_object_unref(array_qName4);
        g_object_unref(array_flag4);
        g_object_unref(array_rID4);
        g_object_unref(array_beginPos4);
        g_object_unref(array_mapQ4);
        g_object_unref(array_cigar4);
        g_object_unref(array_rNextId4);
        g_object_unref(array_pNext4);
        g_object_unref(array_tLen4);
        g_object_unref(array_seq4);
        g_object_unref(array_qual4);
        g_object_unref(array_tags4);
    }
else if(builder_id == 5) {
        array_qName5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName5), &error);
        array_flag5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag5), &error);
        array_rID5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID5), &error);
        array_beginPos5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos5), &error);
        array_mapQ5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ5), &error);
        array_cigar5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar5), &error);
        array_rNextId5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId5), &error);
        array_pNext5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext5), &error);
        array_tLen5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen5), &error);
        array_seq5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq5), &error);
        array_qual5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual5), &error);
        array_tags5 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags5), &error);

        g_object_unref(builder_qName5);
        g_object_unref(builder_flag5);
        g_object_unref(builder_rID5);
        g_object_unref(builder_beginPos5);
        g_object_unref(builder_mapQ5);
        g_object_unref(builder_cigar5);
        g_object_unref(builder_rNextId5);
        g_object_unref(builder_pNext5);
        g_object_unref(builder_tLen5);
        g_object_unref(builder_seq5);
        g_object_unref(builder_qual5);
        g_object_unref(builder_tags5);

        batch_genomics = create_arrow_record_batch(count, array_qName5,array_flag5,array_rID5,array_beginPos5,array_mapQ5,
                                         array_cigar5,array_rNextId5,array_pNext5,array_tLen5,array_seq5,array_qual5,array_tags5);

        g_object_unref(array_qName5);
        g_object_unref(array_flag5);
        g_object_unref(array_rID5);
        g_object_unref(array_beginPos5);
        g_object_unref(array_mapQ5);
        g_object_unref(array_cigar5);
        g_object_unref(array_rNextId5);
        g_object_unref(array_pNext5);
        g_object_unref(array_tLen5);
        g_object_unref(array_seq5);
        g_object_unref(array_qual5);
        g_object_unref(array_tags5);
    }
else if(builder_id == 6) {
        array_qName6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName6), &error);
        array_flag6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag6), &error);
        array_rID6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID6), &error);
        array_beginPos6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos6), &error);
        array_mapQ6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ6), &error);
        array_cigar6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar6), &error);
        array_rNextId6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId6), &error);
        array_pNext6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext6), &error);
        array_tLen6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen6), &error);
        array_seq6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq6), &error);
        array_qual6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual6), &error);
        array_tags6 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags6), &error);

        g_object_unref(builder_qName6);
        g_object_unref(builder_flag6);
        g_object_unref(builder_rID6);
        g_object_unref(builder_beginPos6);
        g_object_unref(builder_mapQ6);
        g_object_unref(builder_cigar6);
        g_object_unref(builder_rNextId6);
        g_object_unref(builder_pNext6);
        g_object_unref(builder_tLen6);
        g_object_unref(builder_seq6);
        g_object_unref(builder_qual6);
        g_object_unref(builder_tags6);

        batch_genomics = create_arrow_record_batch(count, array_qName6,array_flag6,array_rID6,array_beginPos6,array_mapQ6,
                                         array_cigar6,array_rNextId6,array_pNext6,array_tLen6,array_seq6,array_qual6,array_tags6);

        g_object_unref(array_qName6);
        g_object_unref(array_flag6);
        g_object_unref(array_rID6);
        g_object_unref(array_beginPos6);
        g_object_unref(array_mapQ6);
        g_object_unref(array_cigar6);
        g_object_unref(array_rNextId6);
        g_object_unref(array_pNext6);
        g_object_unref(array_tLen6);
        g_object_unref(array_seq6);
        g_object_unref(array_qual6);
        g_object_unref(array_tags6);
    }
else if(builder_id == 7) {
        array_qName7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName7), &error);
        array_flag7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag7), &error);
        array_rID7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID7), &error);
        array_beginPos7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos7), &error);
        array_mapQ7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ7), &error);
        array_cigar7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar7), &error);
        array_rNextId7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId7), &error);
        array_pNext7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext7), &error);
        array_tLen7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen7), &error);
        array_seq7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq7), &error);
        array_qual7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual7), &error);
        array_tags7 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags7), &error);

        g_object_unref(builder_qName7);
        g_object_unref(builder_flag7);
        g_object_unref(builder_rID7);
        g_object_unref(builder_beginPos7);
        g_object_unref(builder_mapQ7);
        g_object_unref(builder_cigar7);
        g_object_unref(builder_rNextId7);
        g_object_unref(builder_pNext7);
        g_object_unref(builder_tLen7);
        g_object_unref(builder_seq7);
        g_object_unref(builder_qual7);
        g_object_unref(builder_tags7);

        batch_genomics = create_arrow_record_batch(count, array_qName7,array_flag7,array_rID7,array_beginPos7,array_mapQ7,
                                         array_cigar7,array_rNextId7,array_pNext7,array_tLen7,array_seq7,array_qual7,array_tags7);

        g_object_unref(array_qName7);
        g_object_unref(array_flag7);
        g_object_unref(array_rID7);
        g_object_unref(array_beginPos7);
        g_object_unref(array_mapQ7);
        g_object_unref(array_cigar7);
        g_object_unref(array_rNextId7);
        g_object_unref(array_pNext7);
        g_object_unref(array_tLen7);
        g_object_unref(array_seq7);
        g_object_unref(array_qual7);
        g_object_unref(array_tags7);
    }
else if(builder_id == 8) {
        array_qName8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName8), &error);
        array_flag8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag8), &error);
        array_rID8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID8), &error);
        array_beginPos8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos8), &error);
        array_mapQ8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ8), &error);
        array_cigar8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar8), &error);
        array_rNextId8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId8), &error);
        array_pNext8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext8), &error);
        array_tLen8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen8), &error);
        array_seq8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq8), &error);
        array_qual8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual8), &error);
        array_tags8 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags8), &error);

        g_object_unref(builder_qName8);
        g_object_unref(builder_flag8);
        g_object_unref(builder_rID8);
        g_object_unref(builder_beginPos8);
        g_object_unref(builder_mapQ8);
        g_object_unref(builder_cigar8);
        g_object_unref(builder_rNextId8);
        g_object_unref(builder_pNext8);
        g_object_unref(builder_tLen8);
        g_object_unref(builder_seq8);
        g_object_unref(builder_qual8);
        g_object_unref(builder_tags8);

        batch_genomics = create_arrow_record_batch(count, array_qName8,array_flag8,array_rID8,array_beginPos8,array_mapQ8,
                                         array_cigar8,array_rNextId8,array_pNext8,array_tLen8,array_seq8,array_qual8,array_tags8);

        g_object_unref(array_qName8);
        g_object_unref(array_flag8);
        g_object_unref(array_rID8);
        g_object_unref(array_beginPos8);
        g_object_unref(array_mapQ8);
        g_object_unref(array_cigar8);
        g_object_unref(array_rNextId8);
        g_object_unref(array_pNext8);
        g_object_unref(array_tLen8);
        g_object_unref(array_seq8);
        g_object_unref(array_qual8);
        g_object_unref(array_tags8);
    }
else if(builder_id == 9) {
        array_qName9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName9), &error);
        array_flag9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag9), &error);
        array_rID9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID9), &error);
        array_beginPos9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos9), &error);
        array_mapQ9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ9), &error);
        array_cigar9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar9), &error);
        array_rNextId9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId9), &error);
        array_pNext9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext9), &error);
        array_tLen9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen9), &error);
        array_seq9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq9), &error);
        array_qual9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual9), &error);
        array_tags9 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags9), &error);

        g_object_unref(builder_qName9);
        g_object_unref(builder_flag9);
        g_object_unref(builder_rID9);
        g_object_unref(builder_beginPos9);
        g_object_unref(builder_mapQ9);
        g_object_unref(builder_cigar9);
        g_object_unref(builder_rNextId9);
        g_object_unref(builder_pNext9);
        g_object_unref(builder_tLen9);
        g_object_unref(builder_seq9);
        g_object_unref(builder_qual9);
        g_object_unref(builder_tags9);

        batch_genomics = create_arrow_record_batch(count, array_qName9,array_flag9,array_rID9,array_beginPos9,array_mapQ9,
                                         array_cigar9,array_rNextId9,array_pNext9,array_tLen9,array_seq9,array_qual9,array_tags9);

        g_object_unref(array_qName9);
        g_object_unref(array_flag9);
        g_object_unref(array_rID9);
        g_object_unref(array_beginPos9);
        g_object_unref(array_mapQ9);
        g_object_unref(array_cigar9);
        g_object_unref(array_rNextId9);
        g_object_unref(array_pNext9);
        g_object_unref(array_tLen9);
        g_object_unref(array_seq9);
        g_object_unref(array_qual9);
        g_object_unref(array_tags9);
    }
else if(builder_id == 10) {
        array_qName10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName10), &error);
        array_flag10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag10), &error);
        array_rID10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID10), &error);
        array_beginPos10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos10), &error);
        array_mapQ10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ10), &error);
        array_cigar10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar10), &error);
        array_rNextId10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId10), &error);
        array_pNext10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext10), &error);
        array_tLen10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen10), &error);
        array_seq10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq10), &error);
        array_qual10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual10), &error);
        array_tags10 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags10), &error);

        g_object_unref(builder_qName10);
        g_object_unref(builder_flag10);
        g_object_unref(builder_rID10);
        g_object_unref(builder_beginPos10);
        g_object_unref(builder_mapQ10);
        g_object_unref(builder_cigar10);
        g_object_unref(builder_rNextId10);
        g_object_unref(builder_pNext10);
        g_object_unref(builder_tLen10);
        g_object_unref(builder_seq10);
        g_object_unref(builder_qual10);
        g_object_unref(builder_tags10);

        batch_genomics = create_arrow_record_batch(count, array_qName10,array_flag10,array_rID10,array_beginPos10,array_mapQ10,
                                         array_cigar10,array_rNextId10,array_pNext10,array_tLen10,array_seq10,array_qual10,array_tags10);

        g_object_unref(array_qName10);
        g_object_unref(array_flag10);
        g_object_unref(array_rID10);
        g_object_unref(array_beginPos10);
        g_object_unref(array_mapQ10);
        g_object_unref(array_cigar10);
        g_object_unref(array_rNextId10);
        g_object_unref(array_pNext10);
        g_object_unref(array_tLen10);
        g_object_unref(array_seq10);
        g_object_unref(array_qual10);
        g_object_unref(array_tags10);
    }
else if(builder_id == 11) {
        array_qName11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName11), &error);
        array_flag11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag11), &error);
        array_rID11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID11), &error);
        array_beginPos11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos11), &error);
        array_mapQ11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ11), &error);
        array_cigar11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar11), &error);
        array_rNextId11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId11), &error);
        array_pNext11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext11), &error);
        array_tLen11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen11), &error);
        array_seq11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq11), &error);
        array_qual11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual11), &error);
        array_tags11 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags11), &error);

        g_object_unref(builder_qName11);
        g_object_unref(builder_flag11);
        g_object_unref(builder_rID11);
        g_object_unref(builder_beginPos11);
        g_object_unref(builder_mapQ11);
        g_object_unref(builder_cigar11);
        g_object_unref(builder_rNextId11);
        g_object_unref(builder_pNext11);
        g_object_unref(builder_tLen11);
        g_object_unref(builder_seq11);
        g_object_unref(builder_qual11);
        g_object_unref(builder_tags11);

        batch_genomics = create_arrow_record_batch(count, array_qName11,array_flag11,array_rID11,array_beginPos11,array_mapQ11,
                                         array_cigar11,array_rNextId11,array_pNext11,array_tLen11,array_seq11,array_qual11,array_tags11);

        g_object_unref(array_qName11);
        g_object_unref(array_flag11);
        g_object_unref(array_rID11);
        g_object_unref(array_beginPos11);
        g_object_unref(array_mapQ11);
        g_object_unref(array_cigar11);
        g_object_unref(array_rNextId11);
        g_object_unref(array_pNext11);
        g_object_unref(array_tLen11);
        g_object_unref(array_seq11);
        g_object_unref(array_qual11);
        g_object_unref(array_tags11);
    }
else if(builder_id == 12) {
        array_qName12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName12), &error);
        array_flag12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag12), &error);
        array_rID12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID12), &error);
        array_beginPos12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos12), &error);
        array_mapQ12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ12), &error);
        array_cigar12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar12), &error);
        array_rNextId12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId12), &error);
        array_pNext12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext12), &error);
        array_tLen12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen12), &error);
        array_seq12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq12), &error);
        array_qual12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual12), &error);
        array_tags12 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags12), &error);

        g_object_unref(builder_qName12);
        g_object_unref(builder_flag12);
        g_object_unref(builder_rID12);
        g_object_unref(builder_beginPos12);
        g_object_unref(builder_mapQ12);
        g_object_unref(builder_cigar12);
        g_object_unref(builder_rNextId12);
        g_object_unref(builder_pNext12);
        g_object_unref(builder_tLen12);
        g_object_unref(builder_seq12);
        g_object_unref(builder_qual12);
        g_object_unref(builder_tags12);

        batch_genomics = create_arrow_record_batch(count, array_qName12,array_flag12,array_rID12,array_beginPos12,array_mapQ12,
                                         array_cigar12,array_rNextId12,array_pNext12,array_tLen12,array_seq12,array_qual12,array_tags12);

        g_object_unref(array_qName12);
        g_object_unref(array_flag12);
        g_object_unref(array_rID12);
        g_object_unref(array_beginPos12);
        g_object_unref(array_mapQ12);
        g_object_unref(array_cigar12);
        g_object_unref(array_rNextId12);
        g_object_unref(array_pNext12);
        g_object_unref(array_tLen12);
        g_object_unref(array_seq12);
        g_object_unref(array_qual12);
        g_object_unref(array_tags12);
    }
else if(builder_id == 13) {
        array_qName13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName13), &error);
        array_flag13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag13), &error);
        array_rID13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID13), &error);
        array_beginPos13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos13), &error);
        array_mapQ13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ13), &error);
        array_cigar13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar13), &error);
        array_rNextId13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId13), &error);
        array_pNext13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext13), &error);
        array_tLen13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen13), &error);
        array_seq13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq13), &error);
        array_qual13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual13), &error);
        array_tags13 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags13), &error);

        g_object_unref(builder_qName13);
        g_object_unref(builder_flag13);
        g_object_unref(builder_rID13);
        g_object_unref(builder_beginPos13);
        g_object_unref(builder_mapQ13);
        g_object_unref(builder_cigar13);
        g_object_unref(builder_rNextId13);
        g_object_unref(builder_pNext13);
        g_object_unref(builder_tLen13);
        g_object_unref(builder_seq13);
        g_object_unref(builder_qual13);
        g_object_unref(builder_tags13);

        batch_genomics = create_arrow_record_batch(count, array_qName13,array_flag13,array_rID13,array_beginPos13,array_mapQ13,
                                         array_cigar13,array_rNextId13,array_pNext13,array_tLen13,array_seq13,array_qual13,array_tags13);

        g_object_unref(array_qName13);
        g_object_unref(array_flag13);
        g_object_unref(array_rID13);
        g_object_unref(array_beginPos13);
        g_object_unref(array_mapQ13);
        g_object_unref(array_cigar13);
        g_object_unref(array_rNextId13);
        g_object_unref(array_pNext13);
        g_object_unref(array_tLen13);
        g_object_unref(array_seq13);
        g_object_unref(array_qual13);
        g_object_unref(array_tags13);
    }
else if(builder_id == 14) {
        array_qName14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName14), &error);
        array_flag14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag14), &error);
        array_rID14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID14), &error);
        array_beginPos14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos14), &error);
        array_mapQ14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ14), &error);
        array_cigar14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar14), &error);
        array_rNextId14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId14), &error);
        array_pNext14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext14), &error);
        array_tLen14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen14), &error);
        array_seq14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq14), &error);
        array_qual14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual14), &error);
        array_tags14 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags14), &error);

        g_object_unref(builder_qName14);
        g_object_unref(builder_flag14);
        g_object_unref(builder_rID14);
        g_object_unref(builder_beginPos14);
        g_object_unref(builder_mapQ14);
        g_object_unref(builder_cigar14);
        g_object_unref(builder_rNextId14);
        g_object_unref(builder_pNext14);
        g_object_unref(builder_tLen14);
        g_object_unref(builder_seq14);
        g_object_unref(builder_qual14);
        g_object_unref(builder_tags14);

        batch_genomics = create_arrow_record_batch(count, array_qName14,array_flag14,array_rID14,array_beginPos14,array_mapQ14,
                                         array_cigar14,array_rNextId14,array_pNext14,array_tLen14,array_seq14,array_qual14,array_tags14);

        g_object_unref(array_qName14);
        g_object_unref(array_flag14);
        g_object_unref(array_rID14);
        g_object_unref(array_beginPos14);
        g_object_unref(array_mapQ14);
        g_object_unref(array_cigar14);
        g_object_unref(array_rNextId14);
        g_object_unref(array_pNext14);
        g_object_unref(array_tLen14);
        g_object_unref(array_seq14);
        g_object_unref(array_qual14);
        g_object_unref(array_tags14);
    }
else if(builder_id == 15) {
        array_qName15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName15), &error);
        array_flag15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag15), &error);
        array_rID15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID15), &error);
        array_beginPos15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos15), &error);
        array_mapQ15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ15), &error);
        array_cigar15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar15), &error);
        array_rNextId15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId15), &error);
        array_pNext15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext15), &error);
        array_tLen15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen15), &error);
        array_seq15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq15), &error);
        array_qual15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual15), &error);
        array_tags15 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags15), &error);

        g_object_unref(builder_qName15);
        g_object_unref(builder_flag15);
        g_object_unref(builder_rID15);
        g_object_unref(builder_beginPos15);
        g_object_unref(builder_mapQ15);
        g_object_unref(builder_cigar15);
        g_object_unref(builder_rNextId15);
        g_object_unref(builder_pNext15);
        g_object_unref(builder_tLen15);
        g_object_unref(builder_seq15);
        g_object_unref(builder_qual15);
        g_object_unref(builder_tags15);

        batch_genomics = create_arrow_record_batch(count, array_qName15,array_flag15,array_rID15,array_beginPos15,array_mapQ15,
                                         array_cigar15,array_rNextId15,array_pNext15,array_tLen15,array_seq15,array_qual15,array_tags15);

        g_object_unref(array_qName15);
        g_object_unref(array_flag15);
        g_object_unref(array_rID15);
        g_object_unref(array_beginPos15);
        g_object_unref(array_mapQ15);
        g_object_unref(array_cigar15);
        g_object_unref(array_rNextId15);
        g_object_unref(array_pNext15);
        g_object_unref(array_tLen15);
        g_object_unref(array_seq15);
        g_object_unref(array_qual15);
        g_object_unref(array_tags15);
    }
else if(builder_id == 16) {
        array_qName16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName16), &error);
        array_flag16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag16), &error);
        array_rID16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID16), &error);
        array_beginPos16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos16), &error);
        array_mapQ16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ16), &error);
        array_cigar16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar16), &error);
        array_rNextId16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId16), &error);
        array_pNext16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext16), &error);
        array_tLen16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen16), &error);
        array_seq16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq16), &error);
        array_qual16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual16), &error);
        array_tags16 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags16), &error);

        g_object_unref(builder_qName16);
        g_object_unref(builder_flag16);
        g_object_unref(builder_rID16);
        g_object_unref(builder_beginPos16);
        g_object_unref(builder_mapQ16);
        g_object_unref(builder_cigar16);
        g_object_unref(builder_rNextId16);
        g_object_unref(builder_pNext16);
        g_object_unref(builder_tLen16);
        g_object_unref(builder_seq16);
        g_object_unref(builder_qual16);
        g_object_unref(builder_tags16);

        batch_genomics = create_arrow_record_batch(count, array_qName16,array_flag16,array_rID16,array_beginPos16,array_mapQ16,
                                         array_cigar16,array_rNextId16,array_pNext16,array_tLen16,array_seq16,array_qual16,array_tags16);

        g_object_unref(array_qName16);
        g_object_unref(array_flag16);
        g_object_unref(array_rID16);
        g_object_unref(array_beginPos16);
        g_object_unref(array_mapQ16);
        g_object_unref(array_cigar16);
        g_object_unref(array_rNextId16);
        g_object_unref(array_pNext16);
        g_object_unref(array_tLen16);
        g_object_unref(array_seq16);
        g_object_unref(array_qual16);
        g_object_unref(array_tags16);
    }
else if(builder_id == 17) {
        array_qName17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName17), &error);
        array_flag17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag17), &error);
        array_rID17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID17), &error);
        array_beginPos17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos17), &error);
        array_mapQ17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ17), &error);
        array_cigar17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar17), &error);
        array_rNextId17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId17), &error);
        array_pNext17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext17), &error);
        array_tLen17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen17), &error);
        array_seq17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq17), &error);
        array_qual17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual17), &error);
        array_tags17 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags17), &error);

        g_object_unref(builder_qName17);
        g_object_unref(builder_flag17);
        g_object_unref(builder_rID17);
        g_object_unref(builder_beginPos17);
        g_object_unref(builder_mapQ17);
        g_object_unref(builder_cigar17);
        g_object_unref(builder_rNextId17);
        g_object_unref(builder_pNext17);
        g_object_unref(builder_tLen17);
        g_object_unref(builder_seq17);
        g_object_unref(builder_qual17);
        g_object_unref(builder_tags17);

        batch_genomics = create_arrow_record_batch(count, array_qName17,array_flag17,array_rID17,array_beginPos17,array_mapQ17,
                                         array_cigar17,array_rNextId17,array_pNext17,array_tLen17,array_seq17,array_qual17,array_tags17);

        g_object_unref(array_qName17);
        g_object_unref(array_flag17);
        g_object_unref(array_rID17);
        g_object_unref(array_beginPos17);
        g_object_unref(array_mapQ17);
        g_object_unref(array_cigar17);
        g_object_unref(array_rNextId17);
        g_object_unref(array_pNext17);
        g_object_unref(array_tLen17);
        g_object_unref(array_seq17);
        g_object_unref(array_qual17);
        g_object_unref(array_tags17);
    }
else if(builder_id == 18) {
        array_qName18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName18), &error);
        array_flag18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag18), &error);
        array_rID18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID18), &error);
        array_beginPos18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos18), &error);
        array_mapQ18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ18), &error);
        array_cigar18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar18), &error);
        array_rNextId18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId18), &error);
        array_pNext18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext18), &error);
        array_tLen18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen18), &error);
        array_seq18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq18), &error);
        array_qual18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual18), &error);
        array_tags18 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags18), &error);

        g_object_unref(builder_qName18);
        g_object_unref(builder_flag18);
        g_object_unref(builder_rID18);
        g_object_unref(builder_beginPos18);
        g_object_unref(builder_mapQ18);
        g_object_unref(builder_cigar18);
        g_object_unref(builder_rNextId18);
        g_object_unref(builder_pNext18);
        g_object_unref(builder_tLen18);
        g_object_unref(builder_seq18);
        g_object_unref(builder_qual18);
        g_object_unref(builder_tags18);

        batch_genomics = create_arrow_record_batch(count, array_qName18,array_flag18,array_rID18,array_beginPos18,array_mapQ18,
                                         array_cigar18,array_rNextId18,array_pNext18,array_tLen18,array_seq18,array_qual18,array_tags18);

        g_object_unref(array_qName18);
        g_object_unref(array_flag18);
        g_object_unref(array_rID18);
        g_object_unref(array_beginPos18);
        g_object_unref(array_mapQ18);
        g_object_unref(array_cigar18);
        g_object_unref(array_rNextId18);
        g_object_unref(array_pNext18);
        g_object_unref(array_tLen18);
        g_object_unref(array_seq18);
        g_object_unref(array_qual18);
        g_object_unref(array_tags18);
    }
else if(builder_id == 19) {
        array_qName19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName19), &error);
        array_flag19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag19), &error);
        array_rID19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID19), &error);
        array_beginPos19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos19), &error);
        array_mapQ19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ19), &error);
        array_cigar19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar19), &error);
        array_rNextId19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId19), &error);
        array_pNext19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext19), &error);
        array_tLen19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen19), &error);
        array_seq19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq19), &error);
        array_qual19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual19), &error);
        array_tags19 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags19), &error);

        g_object_unref(builder_qName19);
        g_object_unref(builder_flag19);
        g_object_unref(builder_rID19);
        g_object_unref(builder_beginPos19);
        g_object_unref(builder_mapQ19);
        g_object_unref(builder_cigar19);
        g_object_unref(builder_rNextId19);
        g_object_unref(builder_pNext19);
        g_object_unref(builder_tLen19);
        g_object_unref(builder_seq19);
        g_object_unref(builder_qual19);
        g_object_unref(builder_tags19);

        batch_genomics = create_arrow_record_batch(count, array_qName19,array_flag19,array_rID19,array_beginPos19,array_mapQ19,
                                         array_cigar19,array_rNextId19,array_pNext19,array_tLen19,array_seq19,array_qual19,array_tags19);

        g_object_unref(array_qName19);
        g_object_unref(array_flag19);
        g_object_unref(array_rID19);
        g_object_unref(array_beginPos19);
        g_object_unref(array_mapQ19);
        g_object_unref(array_cigar19);
        g_object_unref(array_rNextId19);
        g_object_unref(array_pNext19);
        g_object_unref(array_tLen19);
        g_object_unref(array_seq19);
        g_object_unref(array_qual19);
        g_object_unref(array_tags19);
    }
else if(builder_id == 20) {
        array_qName20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName20), &error);
        array_flag20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag20), &error);
        array_rID20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID20), &error);
        array_beginPos20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos20), &error);
        array_mapQ20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ20), &error);
        array_cigar20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar20), &error);
        array_rNextId20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId20), &error);
        array_pNext20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext20), &error);
        array_tLen20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen20), &error);
        array_seq20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq20), &error);
        array_qual20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual20), &error);
        array_tags20 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags20), &error);

        g_object_unref(builder_qName20);
        g_object_unref(builder_flag20);
        g_object_unref(builder_rID20);
        g_object_unref(builder_beginPos20);
        g_object_unref(builder_mapQ20);
        g_object_unref(builder_cigar20);
        g_object_unref(builder_rNextId20);
        g_object_unref(builder_pNext20);
        g_object_unref(builder_tLen20);
        g_object_unref(builder_seq20);
        g_object_unref(builder_qual20);
        g_object_unref(builder_tags20);

        batch_genomics = create_arrow_record_batch(count, array_qName20,array_flag20,array_rID20,array_beginPos20,array_mapQ20,
                                         array_cigar20,array_rNextId20,array_pNext20,array_tLen20,array_seq20,array_qual20,array_tags20);

        g_object_unref(array_qName20);
        g_object_unref(array_flag20);
        g_object_unref(array_rID20);
        g_object_unref(array_beginPos20);
        g_object_unref(array_mapQ20);
        g_object_unref(array_cigar20);
        g_object_unref(array_rNextId20);
        g_object_unref(array_pNext20);
        g_object_unref(array_tLen20);
        g_object_unref(array_seq20);
        g_object_unref(array_qual20);
        g_object_unref(array_tags20);
    }
else if(builder_id == 21) {
        array_qName21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName21), &error);
        array_flag21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag21), &error);
        array_rID21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID21), &error);
        array_beginPos21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos21), &error);
        array_mapQ21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ21), &error);
        array_cigar21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar21), &error);
        array_rNextId21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId21), &error);
        array_pNext21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext21), &error);
        array_tLen21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen21), &error);
        array_seq21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq21), &error);
        array_qual21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual21), &error);
        array_tags21 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags21), &error);

        g_object_unref(builder_qName21);
        g_object_unref(builder_flag21);
        g_object_unref(builder_rID21);
        g_object_unref(builder_beginPos21);
        g_object_unref(builder_mapQ21);
        g_object_unref(builder_cigar21);
        g_object_unref(builder_rNextId21);
        g_object_unref(builder_pNext21);
        g_object_unref(builder_tLen21);
        g_object_unref(builder_seq21);
        g_object_unref(builder_qual21);
        g_object_unref(builder_tags21);

        batch_genomics = create_arrow_record_batch(count, array_qName21,array_flag21,array_rID21,array_beginPos21,array_mapQ21,
                                         array_cigar21,array_rNextId21,array_pNext21,array_tLen21,array_seq21,array_qual21,array_tags21);

        g_object_unref(array_qName21);
        g_object_unref(array_flag21);
        g_object_unref(array_rID21);
        g_object_unref(array_beginPos21);
        g_object_unref(array_mapQ21);
        g_object_unref(array_cigar21);
        g_object_unref(array_rNextId21);
        g_object_unref(array_pNext21);
        g_object_unref(array_tLen21);
        g_object_unref(array_seq21);
        g_object_unref(array_qual21);
        g_object_unref(array_tags21);
    }
else if(builder_id == 22) {
        array_qName22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qName22), &error);
        array_flag22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flag22), &error);
        array_rID22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rID22), &error);
        array_beginPos22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPos22), &error);
        array_mapQ22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQ22), &error);
        array_cigar22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigar22), &error);
        array_rNextId22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextId22), &error);
        array_pNext22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNext22), &error);
        array_tLen22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLen22), &error);
        array_seq22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seq22), &error);
        array_qual22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qual22), &error);
        array_tags22 = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tags22), &error);

        g_object_unref(builder_qName22);
        g_object_unref(builder_flag22);
        g_object_unref(builder_rID22);
        g_object_unref(builder_beginPos22);
        g_object_unref(builder_mapQ22);
        g_object_unref(builder_cigar22);
        g_object_unref(builder_rNextId22);
        g_object_unref(builder_pNext22);
        g_object_unref(builder_tLen22);
        g_object_unref(builder_seq22);
        g_object_unref(builder_qual22);
        g_object_unref(builder_tags22);

        batch_genomics = create_arrow_record_batch(count, array_qName22,array_flag22,array_rID22,array_beginPos22,array_mapQ22,
                                         array_cigar22,array_rNextId22,array_pNext22,array_tLen22,array_seq22,array_qual22,array_tags22);

        g_object_unref(array_qName22);
        g_object_unref(array_flag22);
        g_object_unref(array_rID22);
        g_object_unref(array_beginPos22);
        g_object_unref(array_mapQ22);
        g_object_unref(array_cigar22);
        g_object_unref(array_rNextId22);
        g_object_unref(array_pNext22);
        g_object_unref(array_tLen22);
        g_object_unref(array_seq22);
        g_object_unref(array_qual22);
        g_object_unref(array_tags22);
    }
else if(builder_id == 23) {
        array_qNameX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qNameX), &error);
        array_flagX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flagX), &error);
        array_rIDX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rIDX), &error);
        array_beginPosX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosX), &error);
        array_mapQX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQX), &error);
        array_cigarX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigarX), &error);
        array_rNextIdX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextIdX), &error);
        array_pNextX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNextX), &error);
        array_tLenX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLenX), &error);
        array_seqX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seqX), &error);
        array_qualX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qualX), &error);
        array_tagsX = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tagsX), &error);

        g_object_unref(builder_qNameX);
        g_object_unref(builder_flagX);
        g_object_unref(builder_rIDX);
        g_object_unref(builder_beginPosX);
        g_object_unref(builder_mapQX);
        g_object_unref(builder_cigarX);
        g_object_unref(builder_rNextIdX);
        g_object_unref(builder_pNextX);
        g_object_unref(builder_tLenX);
        g_object_unref(builder_seqX);
        g_object_unref(builder_qualX);
        g_object_unref(builder_tagsX);

        batch_genomics = create_arrow_record_batch(count, array_qNameX,array_flagX,array_rIDX,array_beginPosX,array_mapQX,
                                         array_cigarX,array_rNextIdX,array_pNextX,array_tLenX,array_seqX,array_qualX,array_tagsX);

        g_object_unref(array_qNameX);
        g_object_unref(array_flagX);
        g_object_unref(array_rIDX);
        g_object_unref(array_beginPosX);
        g_object_unref(array_mapQX);
        g_object_unref(array_cigarX);
        g_object_unref(array_rNextIdX);
        g_object_unref(array_pNextX);
        g_object_unref(array_tLenX);
        g_object_unref(array_seqX);
        g_object_unref(array_qualX);
        g_object_unref(array_tagsX);
    }
else if(builder_id == 24) {
        array_qNameY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qNameY), &error);
        array_flagY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flagY), &error);
        array_rIDY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rIDY), &error);
        array_beginPosY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosY), &error);
        array_mapQY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQY), &error);
        array_cigarY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigarY), &error);
        array_rNextIdY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextIdY), &error);
        array_pNextY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNextY), &error);
        array_tLenY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLenY), &error);
        array_seqY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seqY), &error);
        array_qualY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qualY), &error);
        array_tagsY = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tagsY), &error);

        g_object_unref(builder_qNameY);
        g_object_unref(builder_flagY);
        g_object_unref(builder_rIDY);
        g_object_unref(builder_beginPosY);
        g_object_unref(builder_mapQY);
        g_object_unref(builder_cigarY);
        g_object_unref(builder_rNextIdY);
        g_object_unref(builder_pNextY);
        g_object_unref(builder_tLenY);
        g_object_unref(builder_seqY);
        g_object_unref(builder_qualY);
        g_object_unref(builder_tagsY);

        batch_genomics = create_arrow_record_batch(count, array_qNameY,array_flagY,array_rIDY,array_beginPosY,array_mapQY,
                                         array_cigarY,array_rNextIdY,array_pNextY,array_tLenY,array_seqY,array_qualY,array_tagsY);

        g_object_unref(array_qNameY);
        g_object_unref(array_flagY);
        g_object_unref(array_rIDY);
        g_object_unref(array_beginPosY);
        g_object_unref(array_mapQY);
        g_object_unref(array_cigarY);
        g_object_unref(array_rNextIdY);
        g_object_unref(array_pNextY);
        g_object_unref(array_tLenY);
        g_object_unref(array_seqY);
        g_object_unref(array_qualY);
        g_object_unref(array_tagsY);
    }
else if(builder_id == 25) {
        array_qNameM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qNameM), &error);
        array_flagM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_flagM), &error);
        array_rIDM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rIDM), &error);
        array_beginPosM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_beginPosM), &error);
        array_mapQM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_mapQM), &error);
        array_cigarM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_cigarM), &error);
        array_rNextIdM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_rNextIdM), &error);
        array_pNextM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_pNextM), &error);
        array_tLenM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tLenM), &error);
        array_seqM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_seqM), &error);
        array_qualM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_qualM), &error);
        array_tagsM = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(builder_tagsM), &error);

        g_object_unref(builder_qNameM);
        g_object_unref(builder_flagM);
        g_object_unref(builder_rIDM);
        g_object_unref(builder_beginPosM);
        g_object_unref(builder_mapQM);
        g_object_unref(builder_cigarM);
        g_object_unref(builder_rNextIdM);
        g_object_unref(builder_pNextM);
        g_object_unref(builder_tLenM);
        g_object_unref(builder_seqM);
        g_object_unref(builder_qualM);
        g_object_unref(builder_tagsM);

        batch_genomics = create_arrow_record_batch(count, array_qNameM,array_flagM,array_rIDM,array_beginPosM,array_mapQM,
                                         array_cigarM,array_rNextIdM,array_pNextM,array_tLenM,array_seqM,array_qualM,array_tagsM);

        g_object_unref(array_qNameM);
        g_object_unref(array_flagM);
        g_object_unref(array_rIDM);
        g_object_unref(array_beginPosM);
        g_object_unref(array_mapQM);
        g_object_unref(array_cigarM);
        g_object_unref(array_rNextIdM);
        g_object_unref(array_pNextM);
        g_object_unref(array_tLenM);
        g_object_unref(array_seqM);
        g_object_unref(array_qualM);
        g_object_unref(array_tagsM);
    }


    return batch_genomics;
}
