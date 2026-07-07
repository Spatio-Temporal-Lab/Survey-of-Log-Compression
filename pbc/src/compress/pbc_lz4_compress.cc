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

#include "compress/pbc_lz4_compress.h"

#include <algorithm>

#include "base/memcpy.h"
#include "common/utils.h"

namespace PBC {

PBC_LZ4_Compress::PBC_LZ4_Compress(size_t symbol_size, size_t buffer_size)
    : PBC_Compress(symbol_size, buffer_size) {
    InitSecondaryEncoderResource();
}

PBC_LZ4_Compress::~PBC_LZ4_Compress() { CleanSecondaryEncoderResource(); }

void PBC_LZ4_Compress::InitSecondaryEncoderResource() { lz4_stream_ = LZ4_createStream(); }

void PBC_LZ4_Compress::CleanSecondaryEncoderResource() {
    if (lz4_stream_) {
        LZ4_freeStream(lz4_stream_);
        lz4_stream_ = nullptr;
    }
    delete[] dict_buffer_;
    dict_buffer_ = nullptr;
    dict_size_ = 0;
}

void PBC_LZ4_Compress::BuildSecondaryEncoder(const char* data, int64_t data_len,
                                             int64_t data_pos) {
    delete[] dict_buffer_;
    dict_buffer_ = nullptr;
    dict_size_ = 0;

    int64_t remaining_len = data_len - data_pos;
    if (remaining_len <= 0) {
        return;
    }

    dict_size_ = static_cast<int>(std::min<int64_t>(remaining_len, DEFAULT_LZ4_DICT_SIZE));
    dict_buffer_ = new char[dict_size_];
    pbc_memcpy(dict_buffer_, data + data_len - dict_size_, dict_size_);
}

size_t PBC_LZ4_Compress::ApplySecondaryEncoding(const char* input_cstring, int input_cstring_len,
                                                char* output_cstring,
                                                int max_output_cstring_len) {
    if (input_cstring_len <= 0 || max_output_cstring_len <= 0) {
        return 0;
    }

    int compressed_size = 0;
    if (lz4_stream_ && dict_buffer_ && dict_size_ > 0) {
        LZ4_resetStream_fast(lz4_stream_);
        LZ4_loadDict(lz4_stream_, dict_buffer_, dict_size_);
        compressed_size =
            LZ4_compress_fast_continue(lz4_stream_, input_cstring, output_cstring,
                                       input_cstring_len, max_output_cstring_len, acceleration_);
    } else {
        compressed_size = LZ4_compress_default(input_cstring, output_cstring, input_cstring_len,
                                               max_output_cstring_len);
    }

    if (compressed_size <= 0) {
        return 0;
    }
    return static_cast<size_t>(compressed_size);
}

size_t PBC_LZ4_Compress::ApplySecondaryDecoding(const char* input_cstring, int input_cstring_len,
                                                char* output_cstring,
                                                int max_output_cstring_len) {
    if (input_cstring_len <= 0 || max_output_cstring_len <= 0) {
        return 0;
    }

    int decompressed_size = 0;
    if (dict_buffer_ && dict_size_ > 0) {
        decompressed_size = LZ4_decompress_safe_usingDict(
            input_cstring, output_cstring, input_cstring_len, max_output_cstring_len, dict_buffer_,
            dict_size_);
    } else {
        decompressed_size =
            LZ4_decompress_safe(input_cstring, output_cstring, input_cstring_len,
                                max_output_cstring_len);
    }

    if (decompressed_size < 0) {
        PBC_LOG(ERROR) << "LZ4_decompress_safe failed." << std::endl;
        return 0;
    }
    return static_cast<size_t>(decompressed_size);
}

}  // namespace PBC
