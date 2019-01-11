FROM ubuntu:18.04

ENV LANG=en_US.UTF-8

RUN apt-get update -y && \
    apt-get install -y \
        git \
        locales-all \
        autoconf \
        build-essential \
        cmake \
        libboost-dev \
        libboost-filesystem-dev \
        libboost-regex-dev \
        libboost-system-dev \
        python3 \
        python3-pip

ENV PYTHON=python3
RUN pip3 install jira

ADD . /arrow

WORKDIR /arrow/dev/release
