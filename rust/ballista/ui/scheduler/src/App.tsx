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

import React, {useState, useEffect} from "react";
import {Box, Grid, VStack} from "@chakra-ui/react";
import {Header} from "./components/Header";
import { Summary} from "./components/Summary";
import {QueriesList, Query, QueryStatus} from "./components/QueriesList";
import {Footer} from "./components/Footer";

import "./App.css";

function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
    var r = (Math.random() * 16) | 0,
      v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

const getRandomQueries = (num: number): Query[] => {
  const nodes: Query[] = [];

  for (let i = 0; i < num; i++) {
    nodes.push({
      started: new Date().toISOString(),
      query: "SELECT \n" +
          "    employee.id,\n" +
          "    employee.first_name,\n" +
          "    employee.last_name,\n" +
          "    SUM(DATEDIFF(\"SECOND\", call.start_time, call.end_time)) AS call_duration_sum\n" +
          "FROM call\n" +
          "INNER JOIN employee ON call.employee_id = employee.id\n" +
          "GROUP BY\n" +
          "    employee.id,\n" +
          "    employee.first_name,\n" +
          "    employee.last_name\n" +
          "ORDER BY\n" +
          "    employee.id ASC;",
      status: QueryStatus.RUNNING,
      progress: Math.round(Math.random() * 100),
      uuid: uuidv4()
    });
  }
  return nodes;
};

const queries = getRandomQueries(17);

const App : React.FunctionComponent<any> = () => {

  const [schedulerState, setSchedulerState] = useState(undefined)

  function getSchedulerState() {
    return fetch(`/state`, {
      method: 'POST',
      headers: {
        'Accept': 'application/json'
      }
    })
      .then(res => res.json())
      .then(res => setSchedulerState(res));
  }

  useEffect(() => {
    getSchedulerState();
  }, []);

  return (
    <Box>
      <Grid minH="100vh">
        <VStack alignItems={"flex-start"} spacing={0} width={"100%"}>
          <Header schedulerState={schedulerState} />
          <Summary schedulerState={schedulerState} />
          <QueriesList queries={queries} />
          <Footer />
        </VStack>
      </Grid>
    </Box>
  );
}

export default App;
