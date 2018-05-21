# Arrow Flight Java Package

Exposing Apache Arrow data on the wire.

[Protocol Description Slides](https://www.slideshare.net/JacquesNadeau5/apache-arrow-flight-overview)

[GRPC Protocol Definition](https://github.com/jacques-n/arrow/blob/flight/java/flight/src/main/protobuf/flight.proto)

## Example usage

* Compile the java tree:

    ```
    cd java
    mvn clean install -DskipTests
    ```

* Go Into the Flight tree

    ``` 
    cd flight
    ```


* Start the ExampleFlightServer (supports get/put of streams and listing these streams)

    ```
    mvn exec:exec
    ```

* In new terminal, run the TestExampleServer to populate the server with example data

    ```
    cd arrow/java/flight
    mvn surefire:test -DdisableServer=true -Dtest=TestExampleServer
    ```

## Python Example Usage

* Compile example python headers

    ```
    mkdir target/generated-python
    pip install grpcio-tools # or conda install grpcio
    python -m grpc_tools.protoc -I./src/main/protobuf/ --python_out=./target/generated-python --grpc_python_out=./target/generated-python src/main/protobuf/flight.proto
    ```

* Connect to the Flight Service

    ```
    cd target/generated-python
    python
    ```


    ```
    import grpc
    import flight_pb2
    import flight_pb2_grpc as flightrpc
    channel = grpc.insecure_channel('localhost:12233')
    service = flightrpc.FlightServiceStub(channel)
    ```

* List the Flight from Python

    ```
    for f in service.ListFlights(flight_pb2.Criteria()): f
    ```

* Try to Drop

    ```
    action = flight_pb2.Action()
    action.type="drop"
    service.DoAction(action)
    ```
