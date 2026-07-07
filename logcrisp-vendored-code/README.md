# LogCrisp vendored code

This repository contains the public trainer and compressor fragments used by
LogCrisp. It does not contain the paper's aggregation/query engine.

## Build (Ubuntu)

Install `g++`, `make`, and `libzstd-dev`, then run:

```bash
make -C LogCrisp_trainer_var
make -C LogCrisp_compression_var
make -C LogCrisp_compression_var/decompressTest start
```

## Minimal workflow

The paper trains Sketches on a 1% sample. For a small functional test:

```bash
./LogCrisp_trainer_var/Trainer -I sample.log -O output/template
./LogCrisp_compression_var/Compressor \
  -I input.log -O output/block0 -T output/template -P 0
mkdir -p output/decompressed
./LogCrisp_compression_var/decompressTest/DeCompressor \
  -I output/block0.zst -O output/decompressed/
```

The decompressor emits internal units rather than reconstructing the original
log stream. Full-text and aggregate query verification requires the unpublished
query component.
