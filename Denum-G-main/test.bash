#!/bin/bash
set -e
cd /mnt/e/START/日志压缩/Denum

echo "=== Cleaning ==="
rm -rf output/Apache decompress_output/Apache
mkdir -p output/Apache

echo "=== Compiling ==="
g++ -O3 -std=c++17 -o denum_compress denum_compress.cpp -lboost_iostreams -lpthread -lpcre2-8

echo "=== Compressing ==="
./denum_compress Apache 100000 1

echo "=== Decompressing ==="
cd Denum_python_package
python3 decompress.py Apache
cd ..

echo "=== Verifying ==="
cat decompress_output/Apache/*/DecompressedApache.log > decompress_output/Apache/combined.log
python3 -c "
with open('Logs/Apache/Apache.log', 'r') as f:
    orig = [l.rstrip('\n\r') for l in f.readlines()]
with open('decompress_output/Apache/combined.log', 'r') as f:
    dec = [l.rstrip('\n\r') for l in f.readlines()]
mismatch = sum(1 for i in range(len(orig)) if orig[i] != dec[i])
print(f'Original: {len(orig)} lines, Decompressed: {len(dec)} lines')
if mismatch == 0: print('RESULT: Fully lossless')
else: print(f'RESULT: {mismatch} mismatches')
"