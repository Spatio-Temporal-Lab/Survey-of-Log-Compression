#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <unistd.h>

extern "C" int trainer_wrapper(const char* sample, const char* output_path);

int main(int argc, char** argv) {
    std::string input_path;
    std::string output_path;

    int option;
    while ((option = getopt(argc, argv, "hI:O:")) != -1) {
        switch (option) {
        case 'I':
            input_path = optarg;
            break;
        case 'O':
            output_path = optarg;
            break;
        case 'h':
            std::cout << "Usage: Trainer -I SAMPLE.log -O TEMPLATE_PREFIX\n";
            return 0;
        default:
            return 2;
        }
    }

    if (input_path.empty() || output_path.empty()) {
        std::cerr << "Trainer requires -I and -O.\n";
        return 2;
    }

    std::ifstream input(input_path, std::ios::binary);
    if (!input) {
        std::cerr << "Cannot open input: " << input_path << '\n';
        return 1;
    }
    std::string sample((std::istreambuf_iterator<char>(input)),
                       std::istreambuf_iterator<char>());
    if (sample.empty()) {
        std::cerr << "Input sample is empty.\n";
        return 1;
    }
    if (sample.back() != '\n') {
        sample.push_back('\n');
    }

    return trainer_wrapper(sample.c_str(), output_path.c_str());
}
