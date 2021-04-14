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
