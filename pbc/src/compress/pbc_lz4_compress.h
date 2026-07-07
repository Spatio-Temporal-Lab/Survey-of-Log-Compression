/*
 * Copyright 2023 The PBC Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef SRC_COMPRESS_PBC_LZ4_COMPRESS_H_
#define SRC_COMPRESS_PBC_LZ4_COMPRESS_H_

extern "C" {
#include <lz4.h>
}

#include "compress/compress.h"

#define DEFAULT_LZ4_DICT_SIZE (64 * 1024)

namespace PBC {

class PBC_LZ4_Compress : public PBC_Compress {
public:
    explicit PBC_LZ4_Compress(size_t symbol_size = DEFAULT_SYMBOL_SIZE,
                              size_t buffer_size = DEFAULT_BUFFER_SIZE);
    ~PBC_LZ4_Compress();

protected:
    void InitSecondaryEncoderResource() override;
    void CleanSecondaryEncoderResource() override;
    void BuildSecondaryEncoder(const char* data, int64_t data_len, int64_t data_pos) override;
    size_t ApplySecondaryEncoding(const char* input_cstring, int input_cstring_len,
                                  char* output_cstring, int max_output_cstring_len) override;
    size_t ApplySecondaryDecoding(const char* input_cstring, int input_cstring_len,
                                  char* output_cstring, int max_output_cstring_len) override;

private:
    LZ4_stream_t* lz4_stream_ = nullptr;
    char* dict_buffer_ = nullptr;
    int dict_size_ = 0;
    const int acceleration_ = 1;
};
}  // namespace PBC

#endif  // SRC_COMPRESS_PBC_LZ4_COMPRESS_H_
