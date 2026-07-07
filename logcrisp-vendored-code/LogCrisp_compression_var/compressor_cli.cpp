#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <unistd.h>

extern "C" int compressor_wrapper(const char* chunk, const char* output_path,
                                  const char* template_path, int prefix);

int main(int argc, char** argv) {
    std::string input_path;
    std::string output_path;
    std::string template_path;
    int prefix = 0;

    int option;
    while ((option = getopt(argc, argv, "hI:O:T:P:")) != -1) {
        switch (option) {
        case 'I':
            input_path = optarg;
            break;
        case 'O':
            output_path = optarg;
            break;
        case 'T':
            template_path = optarg;
            break;
        case 'P':
            prefix = std::stoi(optarg);
            break;
        case 'h':
            std::cout << "Usage: Compressor -I INPUT.log -O OUTPUT_PREFIX "
                         "-T TEMPLATE_PREFIX [-P BLOCK_ID]\n";
            return 0;
        default:
            return 2;
        }
    }

    if (input_path.empty() || output_path.empty() || template_path.empty()) {
        std::cerr << "Compressor requires -I, -O, and -T.\n";
        return 2;
    }

    std::ifstream input(input_path, std::ios::binary);
    if (!input) {
        std::cerr << "Cannot open input: " << input_path << '\n';
        return 1;
    }
    std::string chunk((std::istreambuf_iterator<char>(input)),
                      std::istreambuf_iterator<char>());
    if (chunk.empty()) {
        std::cerr << "Input log is empty.\n";
        return 1;
    }
    if (chunk.back() != '\n') {
        chunk.push_back('\n');
    }

    return compressor_wrapper(chunk.c_str(), output_path.c_str(),
                              template_path.c_str(), prefix);
}
