import React from "react";
import {Flex, Text} from "@chakra-ui/react";


export const Footer: React.FunctionComponent = () => {
    return (
        <Flex borderTop={"1px solid #f1f1f1"} w={"100%"} p={4} justifyContent={"flex-end"}>
            <Text fontSize="md" fontStyle={"italic"}>Licensed under the Apache License, Version 2.0.</Text>
        </Flex>
    )
}