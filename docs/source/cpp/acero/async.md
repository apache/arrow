sequenceDiagram
    %% This is the source diagram for async.svg
    %% included here for future doc maintainers
    Thread Pool->>+CPU: Start Task
    CPU->>+IO: Read
    IO->>+CPU: Future<Buffer>
    activate IO
    CPU->>+CPU: Add Continuation
    CPU->>+Thread Pool: Finish Task
    Note right of IO: Blocked on IO
    Thread Pool->>+CPU: Other Task
    CPU->>+Thread Pool:
    Thread Pool->>+CPU: Other Task
    CPU->>+Thread Pool:
    Thread Pool->>+CPU: Other Task
    CPU->>+Thread Pool:
    deactivate IO
    IO->>+IO: Read Finished
    IO->>+IO: Run Continuation
    IO->>+Thread Pool: Schedule Task
    Thread Pool->>+CPU: Start Task
    CPU->>+CPU: Process Read Result
