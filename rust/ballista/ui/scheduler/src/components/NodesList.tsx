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

import React from "react";
import {Box } from "@chakra-ui/react";
import {Column, DateCell, DataTable} from "./DataTable";

export enum NodeStatus {
  RUNNING = "RUNNING",
  TERMINATED = "TERMINATED"
}

export interface NodeInfo {
  id: string;
  host: string;
  port: number;
  status: NodeStatus;
  started: string;
}

const columns : Column<any>[] = [
  {
    Header: "Node",
    accessor: "id",
  },
  {
    Header: "Host",
    accessor: "host",
  },
  {
    Header: "Port",
    accessor: "port",
  },
  {
    Header: "Status",
    accessor: "status",
  },
  {
    Header: "Started",
    accessor: "started",
    Cell: DateCell,
  },
];

interface NodesListProps {
  nodes:  NodeInfo[]
}

export const NodesList: React.FunctionComponent<NodesListProps> = ({
  nodes = [],
}) => {
  return (
    <Box flex={1}>
      <DataTable maxW={960} columns={columns} data={nodes} pageSize={4} />
    </Box>
  );
};
