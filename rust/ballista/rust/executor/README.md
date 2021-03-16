# Ballista Executor - Rust
This crate contains the Ballista Executor. It can be used both as a library or as a binary.

## Run

```bash
RUST_LOG=info cargo run --release
...
[2021-02-11T05:30:13Z INFO  executor] Running with config: ExecutorConfig { host: "localhost", port: 50051, work_dir: "/var/folders/y8/fc61kyjd4n53tn444n72rjrm0000gn/T/.tmpv1LjN0", concurrent_tasks: 4 }
```

By default, the executor will bind to `localhost` and listen on port `50051`.