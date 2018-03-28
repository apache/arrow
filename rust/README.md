# Native Rust implementation of Apache Arrow

## Status

This is a starting point for a native Rust implementation of Arrow.

The current code demonstrates arrays of primitive types and structs.

Contiguous memory buffers are used but they are not aligned at 8-byte boundaries yet.

## Example

```rust
let _schema = Schema::new(vec![
    Field::new("a", DataType::Int32, false),
    Field::new("b", DataType::Float32, false),
]);

let a = Rc::new(Array::from(vec![1,2,3,4,5]));
let b = Rc::new(Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]));
let _ = Rc::new(Array::from(vec![a,b]));
```

## Run Tests

```bash
cargo test
```
