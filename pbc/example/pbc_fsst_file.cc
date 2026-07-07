#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

extern "C" {
#include "compress-c.h"
}

namespace {

constexpr char kMagic[] = "PBCF1";

bool ReadFile(const char* path, std::string& data) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return false;
    }
    data.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    return true;
}

bool LoadContext(const char* pattern_path, void** context) {
    std::string pattern;
    if (!ReadFile(pattern_path, pattern) || pattern.empty()) {
        std::cerr << "Cannot read pattern file: " << pattern_path << '\n';
        return false;
    }
    *context = PBC_createCompressCtx(PBC_FSST);
    if (*context == nullptr ||
        PBC_isError(PBC_setPattern(*context, pattern.data(), pattern.size()))) {
        std::cerr << "Cannot initialize PBC-F context.\n";
        return false;
    }
    return true;
}

int CompressFile(const char* input_path, const char* pattern_path, const char* output_path) {
    std::string input;
    if (!ReadFile(input_path, input)) {
        std::cerr << "Cannot read input file: " << input_path << '\n';
        return 1;
    }

    void* context = nullptr;
    if (!LoadContext(pattern_path, &context)) {
        return 1;
    }

    std::ofstream output(output_path, std::ios::binary);
    if (!output) {
        std::cerr << "Cannot create output file: " << output_path << '\n';
        PBC_freePBCDict(context);
        return 1;
    }
    output.write(kMagic, sizeof(kMagic) - 1);

    size_t position = 0;
    size_t record_count = 0;
    uint64_t payload_size = 0;
    while (position < input.size()) {
        size_t end = input.find('\n', position);
        bool has_newline = end != std::string::npos;
        if (!has_newline) {
            end = input.size();
        }
        size_t record_size = end - position;
        std::vector<char> compressed(record_size * 2 + 1024);
        uint32_t compressed_size = 0;
        if (record_size != 0) {
            size_t result = PBC_compressUsingPattern(context, input.data() + position, record_size,
                                                     compressed.data());
            if (PBC_isError(result) || result > UINT32_MAX) {
                std::cerr << "Compression failed at record " << record_count << ".\n";
                PBC_freePBCDict(context);
                return 1;
            }
            compressed_size = static_cast<uint32_t>(result);
        }

        uint8_t newline_flag = has_newline ? 1 : 0;
        output.write(reinterpret_cast<const char*>(&compressed_size), sizeof(compressed_size));
        output.write(reinterpret_cast<const char*>(&newline_flag), sizeof(newline_flag));
        output.write(compressed.data(), compressed_size);
        if (!output) {
            std::cerr << "Writing compressed output failed.\n";
            PBC_freePBCDict(context);
            return 1;
        }
        payload_size += compressed_size;
        record_count++;
        position = end + (has_newline ? 1 : 0);
    }

    PBC_freePBCDict(context);
    std::cout << "records=" << record_count << " source_bytes=" << input.size()
              << " compressed_payload_bytes=" << payload_size << '\n';
    return 0;
}

int DecompressFile(const char* input_path, const char* pattern_path, const char* output_path) {
    std::ifstream input(input_path, std::ios::binary);
    std::ofstream output(output_path, std::ios::binary);
    if (!input || !output) {
        std::cerr << "Cannot open input or output file.\n";
        return 1;
    }

    char magic[sizeof(kMagic) - 1];
    input.read(magic, sizeof(magic));
    if (!input || std::memcmp(magic, kMagic, sizeof(magic)) != 0) {
        std::cerr << "Invalid PBC-F framed file.\n";
        return 1;
    }

    void* context = nullptr;
    if (!LoadContext(pattern_path, &context)) {
        return 1;
    }

    size_t record_count = 0;
    std::vector<char> decompressed(1024 * 1024);
    while (input.peek() != std::char_traits<char>::eof()) {
        uint32_t compressed_size = 0;
        uint8_t newline_flag = 0;
        input.read(reinterpret_cast<char*>(&compressed_size), sizeof(compressed_size));
        input.read(reinterpret_cast<char*>(&newline_flag), sizeof(newline_flag));
        std::vector<char> compressed(compressed_size);
        input.read(compressed.data(), compressed_size);
        if (!input || newline_flag > 1) {
            std::cerr << "Truncated or invalid frame at record " << record_count << ".\n";
            PBC_freePBCDict(context);
            return 1;
        }

        if (compressed_size != 0) {
            size_t result = PBC_decompressUsingPattern(context, compressed.data(), compressed_size,
                                                       decompressed.data());
            if (PBC_isError(result)) {
                std::cerr << "Decompression failed at record " << record_count << ".\n";
                PBC_freePBCDict(context);
                return 1;
            }
            output.write(decompressed.data(), result);
        }
        if (newline_flag != 0) {
            output.put('\n');
        }
        record_count++;
    }

    PBC_freePBCDict(context);
    std::cout << "records=" << record_count << '\n';
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5 || (std::strcmp(argv[1], "-c") != 0 && std::strcmp(argv[1], "-d") != 0)) {
        std::cerr << "Usage: pbc_fsst_file <-c|-d> INPUT PATTERN OUTPUT\n";
        return 2;
    }
    return std::strcmp(argv[1], "-c") == 0
               ? CompressFile(argv[2], argv[3], argv[4])
               : DecompressFile(argv[2], argv[3], argv[4]);
}
