import React from "react";
import {Box, Flex, Text, Button} from "@chakra-ui/react";
import Logo from "./logo.svg";
import {AiFillGithub, HiDocumentText} from "react-icons/all";
import {SchedulerState} from "./Summary";

export const NavBarContainer: React.FunctionComponent<React.PropsWithChildren<any>> = ({children, ...props}) => {
    return (
        <Flex
            as="nav"
            align="center"
            justify="space-between"
            wrap="wrap"
            w="100%"
            padding={1}
            bg={["white"]}
            {...props}
        >
            {children}
        </Flex>
    );
};

interface HeaderProps {
    schedulerState?: SchedulerState
}

export const Header: React.FunctionComponent<HeaderProps> = ({schedulerState}) => {
    return (
        <NavBarContainer borderBottom={"1px"} borderBottomColor={"#f1f1f1"}>
            <Box w="100%" alignItems={"flex-start"}>
                <NavBarContainer>
                    <Flex flexDirection={"row"} alignItems={"center"}>
                        <img alt={"Ballista Logo"} src={Logo}/>
                        <Text
                            background={"aliceblue"}
                            ml={4}
                            fontSize="md"
                            padding={1}
                            borderRadius={4}
                        >
                            Version - {schedulerState?.version}
                        </Text>
                    </Flex>
                    <Flex>
                        <a rel={"noreferrer"} target={"_blank"} href={"https://ballistacompute.org/docs/"}>
                            <Button mr={4} colorScheme="blue" size="sm" rightIcon={<HiDocumentText/>}>
                                Docs
                            </Button>
                        </a>
                        <a
                            rel="noreferrer"
                            href={"https://github.com/apache/arrow/tree/master/rust/ballista"}
                            target={"_blank"}
                        >
                            <Button colorScheme="blue" size="sm" rightIcon={<AiFillGithub/>}>
                                Github
                            </Button>
                        </a>
                    </Flex>
                </NavBarContainer>
            </Box>
        </NavBarContainer>
    );
};
