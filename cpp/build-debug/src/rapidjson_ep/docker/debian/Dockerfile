# BUILD:  docker build -t rapidjson-debian .
# RUN:    docker run -it -v "$PWD"/../..:/rapidjson rapidjson-debian

FROM debian:jessie

RUN apt-get update && apt-get install -y g++ cmake doxygen valgrind

ENTRYPOINT ["/bin/bash"]
