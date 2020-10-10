# prep work
export ARROW_HOME=~/arrow2/arrow
export ARROW_VERSION=2.0.0-SNAPSHOT
export ARROW_JAVA_INTEGRATION_JAR=$ARROW_HOME/java/tools/target/arrow-tools-$ARROW_VERSION-jar-with-dependencies.jar

# build CPP testing binary
cd ./cpp
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_BUILD_INTEGRATION=ON -DARROW_BUILD_TESTS=ON ..
make
cd ../../

# build Java testing binary
cd ./java
mvn install -DskipTests
cd ../

# build Rust testing binary
cd ./rust && cargo build
cd ../

# run tests
rm integration-testing.log
archery integration --with-cpp=1 --with-rust=1 --with-java=0 --debug >> integration-testing.log