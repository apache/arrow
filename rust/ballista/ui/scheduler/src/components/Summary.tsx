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
