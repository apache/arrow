// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import {
    Data, Vector,
    Bool, List, Dictionary
} from 'apache-arrow';

import { instance as atVisitor } from 'apache-arrow/visitor/at';

const data_Bool = new Data(new Bool(), 0, 0);
const data_List_Bool = new Data(new List<Bool>(null as any), 0, 0);
const data_Dictionary_Bool = new Data(new Dictionary<Bool>(null!, null!), 0, 0);
const data_Dictionary_List_Bool = new Data(new Dictionary<List<Bool>>(null!, null!), 0, 0);

const boolVec = new Vector([data_Bool]);
// @ts-ignore
const boolVec_getRaw = boolVec.at(0);
// @ts-ignore
const boolVec_getVisit = atVisitor.visit(boolVec.data[0], 0);
// @ts-ignore
const boolVec_getFactory = atVisitor.getVisitFn(boolVec)(boolVec.data[0], 0);
// @ts-ignore
const boolVec_getFactoryData = atVisitor.getVisitFn(boolVec.data[0])(boolVec.data[0], 0);
// @ts-ignore
const boolVec_getFactoryType = atVisitor.getVisitFn(boolVec.type)(boolVec.data[0], 0);

const listVec = new Vector([data_List_Bool]);
// @ts-ignore
const listVec_getRaw = listVec.at(0);
// @ts-ignore
const listVec_getVisit = atVisitor.visit(listVec.data[0], 0);
// @ts-ignore
const listVec_getFactory = atVisitor.getVisitFn(listVec)(listVec.data[0], 0);
// @ts-ignore
const listVec_getFactoryData = atVisitor.getVisitFn(listVec.data[0])(listVec.data[0], 0);
// @ts-ignore
const listVec_getFactoryType = atVisitor.getVisitFn(listVec.type)(listVec.data[0], 0);

const dictVec = new Vector([data_Dictionary_Bool]);
// @ts-ignore
const dictVec_getRaw = dictVec.at(0);
// @ts-ignore
const dictVec_getVisit = atVisitor.visit(dictVec.data[0], 0);
// @ts-ignore
const dictVec_getFactory = atVisitor.atVisitFn(dictVec)(dictVec.data[0], 0);
// @ts-ignore
const dictVec_getFactoryData = atVisitor.getVisitFn(dictVec.data[0])(dictVec.data[0], 0);
// @ts-ignore
const dictVec_getFactoryType = atVisitor.getVisitFn(dictVec.type)(dictVec.data[0], 0);

const dictOfListVec = new Vector([data_Dictionary_List_Bool]);
// @ts-ignore
const dictOfListVec_getRaw = dictOfListVec.at(0);
// @ts-ignore
const dictOfListVec_getVisit = atVisitor.visit(dictOfListVec.data[0], 0);
// @ts-ignore
const dictOfListVec_getFactory = atVisitor.getVisitFn(dictOfListVec)(dictOfListVec.data[0], 0);
// @ts-ignore
const dictOfListVec_getFactoryData = atVisitor.getVisitFn(dictOfListVec.data[0])(dictOfListVec.data[0], 0);
// @ts-ignore
const dictOfListVec_getFactoryType = atVisitor.getVisitFn(dictOfListVec.type)(dictOfListVec.data[0], 0);
