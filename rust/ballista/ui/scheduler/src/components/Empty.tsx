import React from "react";
import { Flex, Text } from "@chakra-ui/react";
interface EmptyProps {
  text: string;
}

export const Empty: React.FunctionComponent<EmptyProps> = ({ text }) => {
  return (
    <Flex
      minH={100}
      minW={200}
      flex={1}
      alignItems={"center"}
      justifyContent={"center"}
    >
      <Text>{text}</Text>
    </Flex>
  );
};
