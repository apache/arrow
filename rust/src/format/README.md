# Generating Arrow Flatbuffers code in Rust

I compiled flatbuffers locally from commit 21591916afea4f50bb448fd071c3fccbc1d8034f and ran these commands to generate the source files:

```bash
flatc --rust ../format/File.fbs
flatc --rust ../format/Schema.fbs
flatc --rust ../format/Message.fbs
flatc --rust ../format/Tensor.fbs
```

There seems to be a bug in the current Flatbuffers code in the Rust implementation, so I had to manually search and replace to change `type__type` to `type_type`.

I also removed the generated namespace `org::apache::arrow::format` and had to manually add imports at the top of each file.