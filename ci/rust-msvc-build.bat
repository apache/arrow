cargo build --target %TARGET% || exit /B
cargo build --target %TARGET% --release || exit /B
cargo test --target %TARGET% || exit /B
cargo test --target %TARGET% --release || exit /B
