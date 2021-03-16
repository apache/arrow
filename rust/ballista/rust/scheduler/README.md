# Ballista Scheduler
This crate contains the Ballista Scheduler. It can be used both as a library or as a binary.

## Run

```bash
$ RUST_LOG=info cargo run --release
...
[2021-02-11T05:29:30Z INFO  scheduler] Ballista v0.4.2-SNAPSHOT Scheduler listening on 0.0.0.0:50050
[2021-02-11T05:30:13Z INFO  ballista::scheduler] Received register_executor request for ExecutorMetadata { id: "6d10f5d2-c8c3-4e0f-afdb-1f6ec9171321", host: "localhost", port: 50051 }
```

By default, the scheduler will bind to `localhost` and listen on port `50051`.
