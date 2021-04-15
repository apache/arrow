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
import { Box, Text, Flex, VStack } from "@chakra-ui/react";
import { HiCheckCircle } from "react-icons/hi";
import TimeAgo from "react-timeago";
import { NodesList, NodeInfo } from "./NodesList";

const Label: React.FunctionComponent<React.PropsWithChildren<any>> = ({
  children,
}) => {
  return (
    <Text fontSize="md" fontWeight={"light"} width={90}>
      {children}
    </Text>
  );
};

export interface SchedulerState {
  status: string;
  started: string;
  version: string;
  executors: NodeInfo[];
}

export interface SummaryProps {
  schedulerState?: SchedulerState
}

export const Summary: React.FunctionComponent<SummaryProps> = ({schedulerState}) => {

  if (!schedulerState) {
    return <Text>Scheduler isn't running</Text>
  }

  return (
    <Flex bg={"gray.100"} padding={10} width={"100%"}>
      <Box width={"100%"}>
        <Flex paddingX={4}>
          <VStack
            minWidth={250}
            fontSize="md"
            alignItems={"flex-start"}
            fontWeight={"normal"}
          >
            <Text fontWeight={"light"} mb={2}>General Cluster Info</Text>
            <Flex>
              <Label>Status</Label>
              <Flex alignItems={"center"}>
                <HiCheckCircle color={"green"} />
                <Text pl={1}>Active</Text>
              </Flex>
            </Flex>
            <Flex>
              <Label>Nodes</Label>
              <Text>{schedulerState.executors?.length}</Text>
            </Flex>
            <Flex>
              <Label>Started</Label>
              <Text>
                <TimeAgo date={schedulerState.started} />
              </Text>
            </Flex>
            <Flex>
              <Label>Version</Label>
              <Text>{schedulerState.version}</Text>
            </Flex>
          </VStack>
          <NodesList nodes={schedulerState.executors} />
        </Flex>
      </Box>
    </Flex>
  );
};
