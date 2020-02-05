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
} from '../../Arrow';

import { instance as getVisitor } from '../../../src/visitor/get';

const data_Bool = new Data(new Bool(), 0, 0);
const data_List_Bool = new Data(new List<Bool>(null as any), 0, 0);
const data_Dictionary_Bool = new Data(new Dictionary<Bool>(null!, null!), 0, 0);
const data_Dictionary_List_Bool = new Data(new Dictionary<List<Bool>>(null!, null!), 0, 0);

const boolVec = Vector.new(data_Bool);
const boolVec_getRaw = boolVec.get(0);
const boolVec_getVisit = getVisitor.visit(boolVec, 0);
const boolVec_getFactory = getVisitor.getVisitFn(boolVec)(boolVec, 0);
const boolVec_getFactoryData = getVisitor.getVisitFn(boolVec.data)(boolVec, 0);
const boolVec_getFactoryType = getVisitor.getVisitFn(boolVec.type)(boolVec, 0);

const listVec = Vector.new(data_List_Bool);
const listVec_getRaw = listVec.get(0);
const listVec_getVisit = getVisitor.visit(listVec, 0);
const listVec_getFactory = getVisitor.getVisitFn(listVec)(listVec, 0);
const listVec_getFactoryData = getVisitor.getVisitFn(listVec.data)(listVec, 0);
const listVec_getFactoryType = getVisitor.getVisitFn(listVec.type)(listVec, 0);

const dictVec = Vector.new(data_Dictionary_Bool);
const dictVec_getRaw = dictVec.get(0);
const dictVec_getVisit = getVisitor.visit(dictVec, 0);
const dictVec_getFactory = getVisitor.getVisitFn(dictVec)(dictVec, 0);
const dictVec_getFactoryData = getVisitor.getVisitFn(dictVec.data)(dictVec, 0);
const dictVec_getFactoryType = getVisitor.getVisitFn(dictVec.type)(dictVec, 0);

const dictOfListVec = Vector.new(data_Dictionary_List_Bool);
const dictOfListVec_getRaw = dictOfListVec.get(0);
const dictOfListVec_getVisit = getVisitor.visit(dictOfListVec, 0);
const dictOfListVec_getFactory = getVisitor.getVisitFn(dictOfListVec)(dictOfListVec, 0);
const dictOfListVec_getFactoryData = getVisitor.getVisitFn(dictOfListVec.data)(dictOfListVec, 0);
const dictOfListVec_getFactoryType = getVisitor.getVisitFn(dictOfListVec.type)(dictOfListVec, 0);
