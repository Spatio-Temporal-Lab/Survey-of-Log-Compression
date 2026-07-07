/*
================================================================================
 Denum C++ 日志压缩器 — 完整数据流说明
================================================================================

 整体压缩流程（Pipeline）：
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ 原始日志文件 ──读取──► [分块] ──► [Part 2: replace_and_group]          │
 │  (Logs/X/X.log)           │      正则匹配IP/时间戳/日期 → 占位符<X>     │
 │                           │      纯数字匹配 → 占位符<a>/<b>/<c>/<da>..  │
 │                           │      数字值存入 list<int64_t>               │
 │                           │                                             │
 │                           ├──► [Part 3: variable_extract]               │
 │                           │      按分隔符切词 → 含数字的词 → <*>         │
 │                           │      提取的数字词存入 variableset           │
 │                           │                                             │
 │                           ├──► [store_content_with_ids]                 │
 │                           │      模板行 → Dict-ID编码  → allmapping.txt │
 │                           │      模板ID序列    → elastic编码→ allids.bin│
 │                           │      变量集        → Dict-ID编码  → vari-   │
 │                           │                                   ableset*  │
 │                           │                                             │
 │                           └──► [写入数字文件]                            │
 │                                  占位符<X>对应的数字列表                  │
 │                                  → delta变换(可选) → elastic编码 → .bin │
 │                                                                         │
 │ 最后: tar.xz 打包整个块目录 → compressed{N}.xz                          │
 └─────────────────────────────────────────────────────────────────────────┘

 解压流程（对应的 Python 端）：
 ┌─────────────────────────────────────────────────────────────────────────┐
 │ compressed{N}.xz ──解包──► 读取 allmapping.txt + allids.bin            │
 │                                   → 重建模板行列表(含<*>占位符)         │
 │                           读取 variablesetmapping.txt + ids.bin         │
 │                                   → 替换<*>为实际变量值                 │
 │                           读取 _fmt_.txt 格式描述文件                   │
 │                           读取 _I_.bin, _T_.bin, _a_.bin ...            │
 │                                   → 结构化模式按组宽拆分+格式模板重建   │
 │                                   → 纯数字直接替换占位符                │
 │                                   → 输出 Decompressed{X}.log            │
 └─────────────────────────────────────────────────────────────────────────┘

================================================================================
 Part One: Elastic Encoder / Decoder（弹性变长整数编码）
================================================================================

 原理：将 64 位有符号整数编码为 1~N 个字节的变长格式。
 - 每字节低 7 位存储数据，最高位为"继续标志"（1=还有后续字节，0=结束）
 - 先用 ZigZag 将有符号数映射为无符号数（绝对值小的数映射后也小）
 - 编码后小数字只需 1 字节，大数字最多约 10 字节
================================================================================
*/


#define PCRE2_CODE_UNIT_WIDTH 8
#include <iostream>
#include <unordered_set>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <unordered_map>
#include <list>
#include <fstream>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <unordered_set>
#include <pcre2.h>
#include <stdexcept>
#include <future>
#include <filesystem>
#include <cstdlib>
#include <algorithm>
#include <queue>
#include <cmath>
#include <sstream>
#include <sstream>

std::queue<std::string> lines;
std::mutex mtx;



/*
Part One: elastic encoder/decoder

METHODS:
zigzag_encode()
zigzag_decode()
elastic_encode()
elastic_decode()
elastic_decode_bytes()

DESCRIPTIONS: Elastic encoder/decoder, which is proposed by Wei in LogReducer. LogShrink also incoperated this techinology

*/


int64_t zigzag_encode(int64_t num) {
    return (num << 1) ^ (num >> 63);
}

int64_t zigzag_decode(int64_t num) {
    return (num >> 1) ^ -(num & 1);
}

std::vector<unsigned char> elastic_encode(int64_t num) {
    std::vector<unsigned char> buffer;
    uint64_t cur = zigzag_encode(num);
    while (true) {
        if (cur < 0x80) {
            buffer.push_back(static_cast<unsigned char>(cur));
            break;
        } else {
            buffer.push_back(static_cast<unsigned char>((cur & 0x7F) | 0x80));
            cur >>= 7;
        }
    }
    return buffer;
}

int64_t elastic_decode(const std::vector<unsigned char>& num_bytes) {
    int64_t ret = 0;
    int offset = 0;
    for (auto cur : num_bytes) {
        ret |= (static_cast<int64_t>(cur & 0x7F) << offset);
        if ((cur & 0x80) == 0) {
            break;
        }
        offset += 7;
    }
    return zigzag_decode(ret);
}

std::vector<int64_t> elastic_decode_bytes(const std::vector<unsigned char>& binary_bytes) {
    std::vector<int64_t> num_list;
    std::vector<unsigned char> num_byte;
    for (auto byt : binary_bytes) {
        num_byte.push_back(byt);
        if (byt < 128) {
            int64_t decode_num = elastic_decode(num_byte);
            num_list.push_back(decode_num);
            num_byte.clear();
        }
    }
    return num_list;
}


// ============================================================================
// Part Two: 纯数字和含数字特殊Token的处理
// ============================================================================
//
// 数据流：原始日志行 → 正则匹配IP/时间戳(替换为<I>/<T>) → 正则匹配纯数字(替换为<a>/<b>/<c>/<da>...)
//
// 两类匹配：
//   A. 结构化模式(IP/时间戳/日期)：使用捕获组提取各组数字，固定宽度填充后合并为一个整数存储。
//      同时提取组分隔符构建"格式模板"，存入 _fmt_.txt。
//      例如：IP "218.22.153.242" → 4组[218,22,153,242] → 填充[218,022,153,242] → 合并 218022153242 → <I>
//      格式模板：{}.{}.{}.{}
//   B. 纯数字(独立出现的数字)：按数字长度和首数字分类为<a>(1位)、<b>(2位)、<c>(3位)、<da>(4位首1)等。
//      例如：42 → <b>,  2005 → <dc>（4位数字,首位2→'c'）
//
// 关键修改（2024修复）：结构化模式使用固定宽度填充保留格式信息，解决了解压后IP/时间戳格式丢失问题。
// ============================================================================


struct RegexPattern {
    std::vector<std::string> patterns;
    std::vector<std::string> substitutions;
};

// ============================================================================
// LogProcessor 类：负责正则匹配与数字替换 (Part Two 的核心)
// ============================================================================
// 持有每个数据集对应的正则模式(regex_map)，将日志中的结构化数字(IP/时间戳/纯数字)
// 替换为占位符标签（<I>, <T>, <a>, <b>...），并将提取出的数字值存入 patterns map。
class LogProcessor {
public:
    std::string logname;                                           // 当前数据集名称(如 "Apache")
    std::unordered_map<std::string, RegexPattern> regex_map;       // 数据集 → 正则模式+替换标签的映射
    std::vector<pcre2_code *> compiled_patterns;                   // 预编译的结构化模式(IP/时间戳等)
    pcre2_code *re_num;                                            // 预编译的纯数字正则
    // ---- 格式信息存储（2024新增，用于无损还原） ----
    // pattern_format_templates[<I>] = "{}.{}.{}.{}"  格式模板(Python format语法)
    std::unordered_map<std::string, std::string> pattern_format_templates;
    // pattern_group_counts[<I>] = 4                   该模式的捕获组数量
    std::unordered_map<std::string, int> pattern_group_counts;
    // pattern_group_widths[<I>] = 3                   每组数字的固定填充宽度
    std::unordered_map<std::string, int> pattern_group_widths;

    // 构造函数：根据数据集名称初始化正则表达式（向后兼容硬编码模式）
    LogProcessor(const std::string &name) : logname(name) {
        compile_num(&re_num, R"((?<![a-zA-Z])\d+(?![a-zA-Z]))");
        initBuiltinPatterns();
        if (regex_map.find(logname) != regex_map.end()) {
            const auto &patterns = regex_map[logname].patterns;
            for (const auto &pattern : patterns) {
                pcre2_code *re = compile_pattern(pattern.c_str());
                compiled_patterns.push_back(re);
            }
        } else {
            // 未知数据集：不抛异常，使用纯数字+变量提取fallback
            std::cerr << "Warning: unknown logname '" << logname
                      << "'. No structured patterns loaded. "
                      << "Use --config <path> to provide auto-detected patterns, "
                      << "or run: python3 sampling_detector.py Logs/" << logname << "/" << logname << ".log"
                      << std::endl;
        }
    }

    // 构造函数：从自动检测配置文件加载模式（.cfg 多行格式）
    // 不调用 initBuiltinPatterns()，避免内置模式与配置模式冲突导致 substitutions 错位
    LogProcessor(const std::string &name, const std::string &config_path) : logname(name) {
        compile_num(&re_num, R"((?<![a-zA-Z])\d+(?![a-zA-Z]))");
        loadFromConfig(config_path);
    }

private:
    // 初始化硬编码的数据集模式（保留向后兼容）
    void initBuiltinPatterns() {
        regex_map["Apache"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d{2}) (\d+):(\d+):(\d+))"}, {"<I>", "<T>"} };
        regex_map["Android"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["BGL"] = { {R"((\d+)-(\d+)-(\d+)-(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+))",R"((\d+)\.(\d+)\.(\d+))"}, {"<E>", "<T>", "<F>"} };
        regex_map["Hadoop"] = { {R"((\d+)\-(\d+)\-(\d+))", R"((\d+):(\d+):(\d+),(\d+))"}, {"<D>", "<T>"} };
        regex_map["HDFS"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+),(\d+))"}, {"<I>", "<T>"} };
        regex_map["HealthApp"] = { { R"((\d+):(\d+):(\d+):(\d+))"}, {"<T>"} };
        regex_map["HPC"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Linux"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Mac"] = { {R"((\d+)-(\d+)-(\d+)-(\d+))", R"((\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<D>", "<T>"} };
        regex_map["OpenSSH"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)",R"(sshd\[(\d+)\]:)"}, {"<I>", "<T>", "<S>"} };
        regex_map["OpenStack"] = { { R"(\.(\d+)-(\d+)-(\d+)_(\d+):(\d+):(\d+))",R"((\d+)-(\d+)-(\d+).(\d+):(\d+):(\d+)\.(\d+))"}, { "<D>", "<T>"} };
        regex_map["Proxifier"] = { { R"((\d+)\.(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<T>"} };
        regex_map["Spark"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d{2})\/(\d{2})\/(\d{2}) (\d+):(\d+):(\d+))",R"((\d+)\.(\d{1}) MB)",R"((\d+)\.(\d{1}) KB)",R"((\d+)\.(\d{1}) GB)",R"((\d+)\.(\d{1}) B)"}, {"<I>", "<T>", "<M>", "<K>", "<G>", "<B>"} };
        regex_map["Thunderbird"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+):(\d+):(\d+))",R"((\d{4}})\.(\d+)\.(\d+))",R"(\[(\d+)\]:)"}, {"<I>","<T>","<A>","<B>"}};
        regex_map["Windows"] = { {  R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>","<T>","<D>"} };
        regex_map["Zookeeper"] = { {  R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+),(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>","<T>","<D>"} };
    }

    // =========================================================================
    // 从 .cfg 配置文件加载模式（多行格式，避免正则转义问题）
    // 格式：每 6 行为一个模式，以 --- 分隔
    //   ---
    //   <标签>
    //   <正则表达式>
    //   <格式模板>
    //   <捕获组数>
    //   <填充宽度>
    //   <跳过delta: 0/1>
    // =========================================================================
    void loadFromConfig(const std::string &config_path) {
        std::ifstream cf(config_path);
        if (!cf.is_open()) {
            std::cerr << "警告: 无法打开配置文件 '" << config_path << "'，回退到纯数字模式。" << std::endl;
            return;
        }
        std::string line;
        int loaded = 0;
        while (std::getline(cf, line)) {
            // 跳过注释和空行
            if (line.empty() || line[0] == '#') continue;
            // --- 标记一个新模式的开始
            if (line == "---") {
                std::string tag, regex_str, fmt_tmpl;
                std::string ng_str, gw_str, sd_str;
                if (!std::getline(cf, tag)) break;
                if (!std::getline(cf, regex_str)) break;
                if (!std::getline(cf, fmt_tmpl)) break;
                if (!std::getline(cf, ng_str)) break;
                if (!std::getline(cf, gw_str)) break;
                if (!std::getline(cf, sd_str)) break;

                int num_groups = std::stoi(ng_str);
                int group_width = std::stoi(gw_str);
                bool skip_delta = (std::stoi(sd_str) != 0);

                // 添加到 regex_map
                regex_map[logname].patterns.push_back(regex_str);
                regex_map[logname].substitutions.push_back(tag);
                // 编译正则
                pcre2_code *re = compile_pattern(regex_str.c_str());
                compiled_patterns.push_back(re);
                // 存储格式信息（预扫描阶段可能更新宽度）
                pattern_format_templates[tag] = fmt_tmpl;
                pattern_group_counts[tag] = num_groups;
                pattern_group_widths[tag] = group_width;
                loaded++;
            }
        }
        cf.close();
        std::cout << "从配置文件加载了 " << loaded << " 个模式: " << config_path << std::endl;
    }

public:

    // Destructor.
    ~LogProcessor() {
        // Release the compiled regular expression.
        for (auto re : compiled_patterns) {
            pcre2_code_free(re);
        }
        pcre2_code_free(re_num);
    }
    // =========================================================================
    // replace_and_group() — Part Two 的核心入口
    // =========================================================================
    // 输入：一个日志块（vector<string>，每条日志一行）
    // 处理流程：
    //   1.【预扫描】对每个结构化模式(IP/时间戳)，扫描前50行确定最大组宽度
    //   2.【结构化模式替换】按顺序用每个compiled_pattern匹配，提取各组数字，
    //      固定宽度填充后合并为一个整数，存入patterns[标签]。
    //      同时从第一个匹配提取"格式模板"（组分隔符），存入pattern_format_templates。
    //   3.【纯数字替换】用纯数字正则匹配剩余的数字，按长度和首数字分类为<a>/<b>/<c>/<da>等。
    // 输出：
    //   - replaced: 替换后的日志行列表（原始数字被<X>占位符替代）
    //   - patterns: 占位符标签 → 提取出的数字列表(已填充合并)
    //   同时填充了 this->pattern_format_templates / group_counts / group_widths
    // =========================================================================
    std::pair<std::vector<std::string>, std::unordered_map<std::string, std::list<int64_t>>> replace_and_group(const std::vector<std::string> &lst) {
        std::unordered_map<std::string, std::list<int64_t>> patterns;
        std::vector<std::string> replaced;

        const std::string alpha = "abcdefghijklmnopqrstuvwxyz";
        const auto &substitutions = regex_map[logname].substitutions;

        // ------ 阶段0：预扫描 ------
        // 对每个结构化模式，扫描前50行确定最大捕获组宽度。
        // 必要性：组宽度因IP/时间戳的具体值而异（如IP八位组可为1~3位数字）。
        // 预扫描确保所有匹配使用统一填充宽度，保证解压时能正确拆分大整数。
        // 例如：IP八位组最大值255→3位，预扫描捕获此宽度后，所有IP都以3位填充，
        // 即使遇到"1.2.3.4"也会填充为"001002003004"。
        for (size_t pi = 0; pi < compiled_patterns.size(); ++pi) {
            const std::string& tag = substitutions[pi];
            int max_width = 0;
            int scanned = 0;
            for (const auto& line : lst) {
                PCRE2_SPTR subject = (PCRE2_SPTR)line.c_str();
                size_t subject_length = strlen((char *)subject);
                pcre2_match_data *md = pcre2_match_data_create_from_pattern(compiled_patterns[pi], nullptr);
                int rc = pcre2_match(compiled_patterns[pi], subject, subject_length, 0, 0, md, nullptr);
                if (rc > 0) {
                    PCRE2_SIZE *ov = pcre2_get_ovector_pointer(md);
                    uint32_t oc = pcre2_get_ovector_count(md);
                    for (uint32_t g = 1; g < oc; g++) {
                        if (ov[2*g] != PCRE2_UNSET && ov[2*g+1] != PCRE2_UNSET) {
                            int gw = (int)(ov[2*g+1] - ov[2*g]);
                            if (gw > max_width) max_width = gw;
                        }
                    }
                }
                pcre2_match_data_free(md);
                scanned++;
                if (scanned >= 50) break;
            }
            if (max_width > 0) {
                pattern_group_widths[tag] = max_width;
            }
        }

        for (auto &item : lst) {
            std::string result = item;
            for (size_t i = 0; i < compiled_patterns.size(); ++i) {
                result = process_with_pattern(result, compiled_patterns[i], substitutions[i], patterns);
            }
            result = process_with_pattern(result, re_num, alpha, patterns, true);
            replaced.push_back(result);
        }

        return {replaced, patterns};
    }

private:
    pcre2_code* compile_pattern(const char *pattern) {
        int errornumber;
        PCRE2_SIZE erroroffset;
        pcre2_code *re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errornumber, &erroroffset, nullptr);
        if (re == nullptr) {
            throw std::runtime_error("Regex compilation failed");
        }
        return re;
    }
    void compile_num(pcre2_code **re, const char *pattern) {
        int errornumber;
        PCRE2_SIZE erroroffset;
        *re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errornumber, &erroroffset, nullptr);
        if (*re == nullptr) {
            throw std::runtime_error("Regex compilation failed");
        }
    }

    // =========================================================================
    // process_with_pattern() — 单种正则模式的处理
    // =========================================================================
    // 输入：
    //   - input: 当前日志行（可能已被前面模式部分替换）
    //   - re: 预编译的PCRE2正则
    //   - substitution: 匹配后的替换标签（如"<I>"、"<T>"或纯数字字母表）
    //   - patterns: 数字收集map（输出参数）
    //   - is_num: true=纯数字模式, false=结构化模式(IP/时间戳等)
    //
    // 结构化模式(is_num=false)的处理（2024修复版）：
    //   对每个正则匹配：
    //     a) 遍历所有捕获组，逐个提取组值(如IP的4个八位组)
    //     b) 使用预扫描确定的固定宽度(pad_width)左补零填充每组
    //     c) 将填充后的各组拼接为一个长整数 ← 存入patterns
    //     d) 首次匹配时：提取原匹配文本中各组间的分隔符，构建"格式模板"
    //        (如IP的模板"{}.{}.{}.{}"，时间戳模板"{:02d} {:02d}:{:02d}:{:02d}")
    //     e) 在原行中将匹配文本替换为占位符标签(如<I>)
    //
    // 纯数字模式(is_num=true)的处理：
    //   匹配每个独立的纯数字，按长度和首数字分类：
    //     len=1 → <a>,  len=2 → <b>,  len=3 → <c>
    //     len≥4 → <{len码}{首数字码}>  如2005→<dc>(d=4位,c=首数字2)
    //     len≥15 → 不替换（保留原数字，太长的数字不做压缩）
    // =========================================================================
    std::string process_with_pattern(const std::string &input, pcre2_code *re, const std::string &substitution, std::unordered_map<std::string, std::list<int64_t>> &patterns, bool is_num = false) {
        std::string result;
        PCRE2_SIZE last_pos = 0;

        PCRE2_SPTR subject = (PCRE2_SPTR)input.c_str();
        size_t subject_length = strlen((char *)subject);

        pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, nullptr);

        int rc;
        while ((rc = pcre2_match(re, subject, subject_length, last_pos, 0, match_data, nullptr)) > 0) {
            PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data);

            if (is_num) {
                std::string num = input.substr(ovector[0], ovector[1] - ovector[0]);
                size_t len = num.length();
                if (len < 15) {
                    // 修复: 前导零数字(如 hex 地址 000a, 权限位 007)保留原样
                    // 避免 std::stoll() 整数转换丢失前导零，导致解压后 000a→0a
                    if (len > 1 && num[0] == '0') {
                        result += input.substr(last_pos, ovector[0] - last_pos) + num;
                    } else {
                        std::string pattern_key = "<" + std::string(1, substitution[len - 1]) + (len >= 4 ? std::string(1, substitution[num[0] - '0']) : "") + ">";
                        patterns[pattern_key].push_back(std::stoll(num));
                        result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
                    }
                } else {
                    result += input.substr(last_pos, ovector[0] - last_pos) + num;
                }
            } else {
                // ============================================================
                // 结构化模式匹配分支（IP/时间戳/日期等）
                // ============================================================
                // ovector 布局： [全匹配起点,全匹配终点, 组1起点,组1终点, 组2起点,组2终点, ...]
                // ovector_count = 1 + 捕获组数量（组0是全匹配）
                uint32_t ovector_count = pcre2_get_ovector_count(match_data);
                int num_groups = (int)ovector_count - 1; // 排除全匹配(组0)

                if (num_groups <= 0) {
                    // 回退：正则没有捕获组，使用旧版逻辑（提取全部数字拼接）
                    // 这种情况格式信息会丢失，应尽量避免。
                    std::string match = input.substr(ovector[0], ovector[1] - ovector[0]);
                    std::string num_str;
                    for (char c : match) {
                        if (std::isdigit(c)) num_str += c;
                    }
                    patterns[substitution].push_back(std::stoll(num_str));
                    result += input.substr(last_pos, ovector[0] - last_pos) + substitution;
                } else {
                    // ============================================================
                    // 阶段A：构建填充合并数字 + 格式模板
                    // ============================================================
                    std::string padded_combined;
                    std::string format_tmpl;

                    // 使用预扫描确定的填充宽度（所有匹配统一宽度，保证解压可拆分）
                    int pad_width = 3; // 默认回退值
                    auto pw_it = pattern_group_widths.find(substitution);
                    if (pw_it != pattern_group_widths.end()) {
                        pad_width = pw_it->second;
                    }

                    // ----- 构建格式模板（仅首次匹配时） -----
                    // 从第一个匹配中提取各组间的"分隔符文本"，构造 Python str.format() 兼容的模板。
                    // 例如 IP "218.22.153.242" → 组间分隔符: ".", ".", "." → 模板 "{}.{}.{}.{}"
                    // 时间戳 "09 06:07:04" → 组间分隔符: " ", ":", ":" → 模板 "{:02d} {:02d}:{:02d}:{:02d}"
                    // 填充宽度≤2的模式（时间戳类）使用零填充格式{:0Nd}；>2的模式（IP类）使用{}。
                    bool first_match = (pattern_format_templates.find(substitution) == pattern_format_templates.end());
                    if (first_match) {
                        std::string match_str = input.substr(ovector[0], ovector[1] - ovector[0]);
                        PCRE2_SIZE prev_end = ovector[0];
                        // 窄组(≤2位)通常是时间分量(hh/mm/ss)，需要零填充显示
                        bool zero_pad = (pad_width <= 2);
                        for (uint32_t i = 1; i < ovector_count; i++) {
                            PCRE2_SIZE gstart = ovector[2 * i];
                            PCRE2_SIZE gend = ovector[2 * i + 1];
                            if (gstart == PCRE2_UNSET || gend == PCRE2_UNSET) continue;
                            // 提取上一组结束到本组开始之间的分隔符文本
                            if ((int)(gstart - prev_end) > 0) {
                                format_tmpl += match_str.substr(prev_end - ovector[0], gstart - prev_end);
                            }
                            // 填入Python格式化占位符：零填充{:0Nd} 或 普通{}
                            if (zero_pad) {
                                format_tmpl += "{:0" + std::to_string(pad_width) + "d}";
                            } else {
                                format_tmpl += "{}";
                            }
                            prev_end = gend;
                        }
                        // 最后一组之后的尾部文本（通常为空）
                        if ((int)(ovector[1] - prev_end) > 0) {
                            format_tmpl += match_str.substr(prev_end - ovector[0], ovector[1] - prev_end);
                        }
                        // 保存格式信息，供 processLogBlock 写入 _fmt_.txt
                        pattern_format_templates[substitution] = format_tmpl;
                        pattern_group_counts[substitution] = num_groups;
                        pattern_group_widths[substitution] = pad_width;
                    }

                    // ----- 构建填充合并数字 -----
                    // 将每个捕获组的数字值左补零到 pad_width 位，然后拼接为一个长整数。
                    // 例如 IP [218,22,153,242] pad=3 → "218"+"022"+"153"+"242" → 218022153242
                    for (uint32_t i = 1; i < ovector_count; i++) {
                        PCRE2_SIZE gstart = ovector[2 * i];
                        PCRE2_SIZE gend = ovector[2 * i + 1];
                        if (gstart == PCRE2_UNSET || gend == PCRE2_UNSET) continue;
                        std::string group_str = input.substr(gstart, gend - gstart);
                        // Pad to fixed width with leading zeros
                        while ((int)group_str.length() < pad_width) {
                            group_str = "0" + group_str;
                        }
                        padded_combined += group_str;
                    }

                    patterns[substitution].push_back(std::stoll(padded_combined));
                    result += input.substr(last_pos, ovector[0] - last_pos) + substitution;
                }
            }
            last_pos = ovector[1];
        }
        result += input.substr(last_pos);

        pcre2_match_data_free(match_data); // Release the memory used for regex matching.

        return result;
    }
};


// ============================================================================
// Part Three: 数值变量处理 (Numeric Variable Processing)
// ============================================================================
//
// 数据流：replace_and_group的输出日志行 → 分隔符挖掘 → 切词 → 含数字的词→<*>替换 → Dict-ID存储
//
// 为什么需要这一步？replace_and_group 只处理了"有明显模式"的数字（IP/时间戳/纯数字），
// 但日志中还有"嵌入式数字"——位于非标准位置的数字，如：
//   - "mod_security/1.9dev2" 中的 1、9、2（部分被纯数字正则捕获，部分可能漏过）
//   - "env.createBean2()" 中的 2
//   - "jk2_init()" 中的 2
// 这些数字与文本混合，无法用单一正则匹配，需要通过"分隔符切词+数字检测"的方式处理。
//
// 方法链：
//   variable_extract()
//     └─ delimeter_mining()       从样本中挖掘高频分隔符（空格、方括号、斜杠等）
//           └─ find_special_chars_with_high_freq()
//     └─ split_by_multiple_delimiters()  按分隔符将日志行切为 token 列表
//     └─ 遍历 token，含数字的 → 替换为 <*>，token 原文存入 variable_set
//     └─ store_content_with_ids()        将变量集和模板行做 Dict-ID 编码存储
// ============================================================================


// ============================================================================
// DenumLogProcessor 类：数值变量提取 + Dict-ID 模板存储
// ============================================================================
// 处理 replace_and_group 之后的日志行：
//   1. variable_extract(): 切词→找含数字的token→替换为<*>→收集变量值
//   2. store_content_with_ids(): 将变量集和模板行用 Dict-ID 编码后写入文件
class DenumLogProcessor {
private:
    std::string logname;

public:
    DenumLogProcessor(std::string name) : logname(name) {}

    // =========================================================================
    // variable_extract() — 提取嵌入式数值变量
    // =========================================================================
    // 输入：replace_and_group 的输出（已替换IP/时间戳/纯数字为<I>/<T>/<a>/<b>等占位符）
    // 流程：
    //   1. delimeter_mining(): 从样本中挖掘高频分隔符（空格、[、]、/、_等）
    //   2. 对每行日志，用分隔符正则 split_by_multiple_delimiters() 切词
    //   3. 遍历每个词，如果含有数字(0-9)，则替换为 <*>，原文加入 variable_set
    //   4. store_content_with_ids(variable_set, "variableset", ...) 将变量集存盘
    // 输出：所有数字都被<*>替代的日志行列表（只有固定文本和<I>/<T>/<a>/<b>等占位符）
    // =========================================================================
    std::vector<std::string> variable_extract(const std::vector<std::string>& logs, const std::string& chunkID) {
        std::vector<std::string> modified_lines;
        std::vector<std::string> variable_set;
        std::regex digit_pattern("\\d");
        std::regex regex_pattern;
        std::vector<std::string> delimiters;
        std::tie(regex_pattern, delimiters) = delimeter_mining(logs);
        std::string modified_line;
        std::vector<std::string> split;
        for (const auto& log : logs) {
            modified_line = "";
            split = split_by_multiple_delimiters(regex_pattern, log,true);
            for (const auto& word : split) {
                if (std::regex_search(word, digit_pattern)) {
                    // 修复(占位符泄漏): 若token已含占位符标签(<a>/<I>/<T>等),
                    // 说明其数字已被上层处理, 不应替换为<*>, 否则解压时标签被二次替换导致错位
                    if (word.find('<') != std::string::npos || word.find('>') != std::string::npos) {
                        modified_line += word;
                    } else {
                        modified_line += "<*>";
                        variable_set.push_back(word);
                    }
                } else {
                    modified_line += word;
                }
            }
            modified_lines.push_back(modified_line);
        }
        store_content_with_ids(variable_set, "variableset", chunkID, "lzma");
        return modified_lines;
    }
    
    void ensure_directory_exists(const std::string& dir) {
        struct stat buffer;
        if (stat(dir.c_str(), &buffer) != 0) { // Check if the directory exists.
            #ifdef _WIN32
            _mkdir(dir.c_str());  
            #else
            mkdir(dir.c_str(), 0777);  
            #endif
        }
    }


    // =========================================================================
    // store_content_with_ids() — Dict-ID 编码存储
    // =========================================================================
    // 将字符串序列做"字典编码"：为每个唯一字符串分配一个整数ID，存储ID序列和字典。
    // 产物：
    //   {output}ids.bin    — ID序列（elastic编码的二进制），解压时按ID从字典取回原文
    //   {output}mapping.txt — 字典文件（按ID排序的原文列表，一行一个字符串）
    // Dict-ID编码的优势：重复出现的字符串只存一份字典项+简短的ID引用，大幅压缩。
    // =========================================================================
    void store_content_with_ids(const std::vector<std::string>& input, const std::string& output, const std::string& chunkID, const std::string& compressor) {
    std::unordered_map<std::string, int> content_to_id;
    std::unordered_map<int, std::string> id_to_content;
    int id_counter = 1;
    std::vector<int> id_list;
    std::string id_dir = "output/" + logname + "/" + chunkID + "/";
    std::string ids_file_path = "output/" + logname + "/" + chunkID + "/" + logname + output + "ids.bin";
    std::string mapping_file_path = "output/" + logname + "/" + chunkID + "/" + logname + output + "mapping.txt";
    ensure_directory_exists(id_dir);

    for (const auto& line : input) {
        if (line.empty()) continue;
        if (content_to_id.find(line) == content_to_id.end()) {
            content_to_id[line] = id_counter;
            id_to_content[id_counter] = line;
            id_counter++;
        }
        id_list.push_back(content_to_id[line]);
    }

    // Create an ordered vector to store the contents of id_to_content.
    std::vector<std::pair<int, std::string>> sorted_content;
    for (const auto& pair : id_to_content) {
        sorted_content.push_back(pair);
    }

    // Sort by id_counter.
    std::sort(sorted_content.begin(), sorted_content.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    std::ofstream ids_file(ids_file_path, std::ios::binary);
    std::ofstream mapping_file(mapping_file_path);

    // Now write to mapping_file in the sorted order.
    for (const auto& pair : sorted_content) {
        mapping_file << pair.second << "\n";
    }

    for (int id : id_list) {
        auto encoded = elastic_encode(id);
        ids_file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
    }

    ids_file.close();
    mapping_file.close();
}

    std::string regex_escape(const std::string& pattern) {
            // List of special characters that need to be escaped.
            static const std::string special_chars = R"([-[\]{}()*+?.\\^$|])";

            // Construct the escaped pattern.
            std::string escaped_pattern;
            for (char c : pattern) {
                if (special_chars.find(c) != std::string::npos) {
                    escaped_pattern += '\\'; // Add escape characters.
                }
                escaped_pattern += c;
            }

            return escaped_pattern;
        }

    std::tuple<std::regex, std::vector<std::string>> delimeter_mining(const std::vector<std::string>& logs) {
        std::vector<std::string> temp = logs;
        std::random_shuffle(temp.begin(), temp.end());
        std::unordered_set<size_t> lengths;
        std::vector<std::string> sample;
        size_t iteration_count = 0;
        for (const auto& log : temp) {
            size_t log_len = log.size();
            if (lengths.find(log_len) == lengths.end()) {
                lengths.insert(log_len);
                sample.push_back(log);
            }
            iteration_count++;
            if (lengths.size() >= 10 || iteration_count >= 200) {
                break;
            }
        }
        std::vector<std::string> delimiters = find_special_chars_with_high_freq(sample);
        if (delimiters.empty()) {
            throw std::runtime_error("No delimiters found. Cannot create a valid regex pattern.");
        }

        std::string pattern_str = "(";
        for (const auto& delimiter : delimiters) {
            if (!delimiter.empty()) {
                pattern_str += regex_escape(delimiter) + "|";
            }
        }
        if (pattern_str.back() == '|') {
            pattern_str.pop_back(); // Remove the trailing "|".
        }
        pattern_str += ")";

        if (pattern_str == "()") {
            throw std::runtime_error("Invalid regex pattern: " + pattern_str);
        }
        return std::make_tuple(std::regex(pattern_str), delimiters);
    }

    std::vector<std::string> split_by_multiple_delimiters(const std::regex& pattern, const std::string& str, bool include_delimiters) {
        std::vector<std::string> result;
        auto words_begin = std::sregex_iterator(str.begin(), str.end(), pattern);
        auto words_end = std::sregex_iterator();

        size_t last_pos = 0;
        for (std::sregex_iterator iter = words_begin; iter != words_end; ++iter) {
            std::smatch match = *iter;
            size_t current_pos = match.position();
            if (current_pos > last_pos) {
                result.push_back(str.substr(last_pos, current_pos - last_pos));
            }
            if (include_delimiters) {
                result.push_back(match.str());
            }
            last_pos = current_pos + match.length();
        }

        if (last_pos < str.length()) {
            result.push_back(str.substr(last_pos));
        }

        return result;
    }


    std::vector<std::string> find_special_chars_with_high_freq(const std::vector<std::string>& str_list, size_t freq_threshold = 10) {
        std::vector<char> candidates = {',', ' ', '|', ';', '[', ']', '(', ')', '_', '/'};
        std::unordered_map<char, size_t> char_counter;
        for (const auto& s : str_list) {
            for (char c : s) {
                if (std::find(candidates.begin(), candidates.end(), c) != candidates.end()) {
                    char_counter[c]++;
                }
            }
        }

        std::vector<std::string> result;
        for (const auto& pair : char_counter) {
            if (pair.second > freq_threshold) {
                result.push_back(std::string(1, pair.first));
            }
        }

        // If the result is empty, add a space character.
        if (result.empty()) {
            result.push_back(" ");
        }

        return result;
    }
};
// ============================================================================
// Part Four: 块压缩实现 (Block Compression)
// ============================================================================
//
// 数据流（processLogBlock 内部）：
// ┌─────────────────────────────────────────────────────────────────────────┐
// │ 日志块(vector<string>)                                                 │
// │   │                                                                     │
// │   ├─► [1] log_processor.replace_and_group(block)                       │
// │   │       输出: final_output (带<X>占位符的行), final_patterns (数字map)│
// │   │       副作用: 填充 pattern_format_templates / group_counts / widths │
// │   │                                                                     │
// │   ├─► [2] denum_processor.variable_extract(final_output)               │
// │   │       输出: modified_logs (含<*>、<I>、<T>、<a>等占位符的行)       │
// │   │       副作用: 写入 variablesetids.bin + variablesetmapping.txt     │
// │   │                                                                     │
// │   ├─► [3] denum_processor.store_content_with_ids(modified_logs, "all") │
// │   │       写入: allids.bin (模板ID序列) + allmapping.txt (模板字典)     │
// │   │                                                                     │
// │   ├─► [4] 遍历 final_patterns，对每个占位符<X>:                       │
// │   │       - 可选 delta_transform (排除<I>/<a>/<b>/<c>)                 │
// │   │       - elastic_encode 每个数字                                    │
// │   │       - 写入 _X_.bin（如 _I_.bin, _T_.bin, _a_.bin...）           │
// │   │                                                                     │
// │   ├─► [5] 写入 _fmt_.txt 格式描述文件                                  │
// │   │       每行: <标签> <组数> <填充宽> <Python格式模板>                │
// │   │       例如: <I> 4 3 {}.{}.{}.{}                                    │
// │   │                                                                     │
// │   └─► [6] compressDirectory(): tar.xz 打包整个块目录                   │
// └─────────────────────────────────────────────────────────────────────────┘
//
// 产物清单（每个块目录下）：
//   _fmt_.txt                  - 格式描述文件（2024新增）
//   _I_.bin, _T_.bin, ...      - 结构化模式数字序列（elastic编码）
//   _a_.bin, _b_.bin, _c_.bin..- 纯数字序列
//   {logname}allmapping.txt    - 模板字典（Dict-ID映射）
//   {logname}allids.bin        - 模板ID序列（elastic编码）
//   {logname}variablesetmapping.txt - 变量字典
//   {logname}variablesetids.bin    - 变量ID序列
//
// 关键修复（2024）：
//   - 结构化模式数字使用固定宽度填充存储，配合 _fmt_.txt 实现无损还原
//   - delta_transform 排除列表包含 <c>（与Python解压端对齐）
// ============================================================================



void ensure_directory_exists(const std::string& dir) {
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) { // Check if the directory exists.
        #ifdef _WIN32
        _mkdir(dir.c_str());  // Create directory on Windows system.
        #else
        mkdir(dir.c_str(), 0777);  // Create directory on Unix/Linux system.
        #endif
    }
}


std::string sanitize_filename(std::string filename) {
    std::replace(filename.begin(), filename.end(), '<', '_');  // Replace '<' with '_'.
    std::replace(filename.begin(), filename.end(), '>', '_');  // Replace '>' with '_'.
    return filename;
}

// =========================================================================
// delta_transform() — 增量编码（Delta Encoding）
// =========================================================================
// 输入：[100, 105, 110, 130]
// 输出：[100, 5, 5, 20]  （首元素保留原值，后续元素存储与前一个的差值）
// 目的：对单调递增序列（如时间戳、序号），delta值比绝对值小得多，elastic编码后更省空间。
// 注意：不适用于非单调序列（如IP地址），这些模式在 processLogBlock 中被排除。
// =========================================================================
/*
delta_transform() : Calculate the difference between adjacent numbers.

input: number list

output: difference list
*/

std::list<int64_t> delta_transform(const std::list<int64_t>& num_list) {
    if (num_list.empty()) {
        return {}; // If the list is empty, return an empty list.
    }

    std::list<int64_t> new_list;

    auto it = num_list.begin();
    int64_t initial = *it;
    new_list.push_back(initial); // Add the initial element.
    int64_t last = initial;

    for (++it; it != num_list.end(); ++it) {
        int64_t delta = *it - last;
        new_list.push_back(delta);
        last = *it;
    }

    return new_list;
}


void compressDirectory(const std::string& output_dir, int block_id) {
    std::string directoryPath = output_dir + "/" + std::to_string(block_id);
    std::string command = "tar -cJf " + output_dir + "/compressed" + std::to_string(block_id) + ".xz " + directoryPath;
    int result = std::system(command.c_str());

    if (result != 0) {
        std::cerr << "Command failed with return code: " << result << std::endl;
    } else {
        std::cout << "Block " << block_id << " directory successfully compressed into compressed" << block_id << ".xz" << std::endl;
    }
}


// =========================================================================
// processLogBlock() — 处理单个日志块（压缩流水线）
// =========================================================================
// 这是压缩流程中每个并行任务的工作函数。参数：
//   - block:        原始日志行列表（大小 = BLOCK_SIZE，最后一块可能更小）
//   - block_id:     块序号（0-based，对应 compressed{N}.xz 的 N）
//   - output_dir:   输出根目录（如 "output/Apache"）
//   - log_processor: Part Two 的正则替换处理器
//   - denum_processor: Part Three 的变量提取处理器
//   - output_logs:  模式选择 "1"=默认完整压缩, "2"=仅输出无数字日志, "3"=压缩但不做Dict-ID
// =========================================================================
void processLogBlock(const std::vector<std::string>& block, int block_id, const std::string& output_dir, LogProcessor& log_processor, DenumLogProcessor& denum_processor, std::map<int, std::vector<std::string>>& final_outputs, const std::string& output_logs) {
    // ---------- 步骤1: 正则替换，提取结构化数字 ----------
    // final_output:  IP/时间戳/纯数字被替换为<I>/<T>/<a>/<b>等占位符的行
    // final_patterns: <I>→[数字列表], <T>→[数字列表], <a>→[数字列表], ...
    auto [final_output, final_patterns] = log_processor.replace_and_group(block);
    std::string logname_dir = output_dir + "/" + std::to_string(block_id) + "/";
    ensure_directory_exists(logname_dir);

    // ---------- 步骤2: 变量提取 ----------
    // 对已替换的行做分隔符切词，检测遗漏的嵌入式数字（如"mod_security/1.9dev2"中的数字）
    // modified_logs: 含<*>、<I>、<T>、<a>等占位符的行（所有数字均被替换）
    // 副作用：写入 variablesetids.bin + variablesetmapping.txt
    std::vector<std::string> modified_logs = denum_processor.variable_extract(final_output, std::to_string(block_id));

    // ---------- 步骤3: 模板存储（按模式选择） ----------
    if (output_logs == "1") {
        // 默认模式：Dict-ID编码存储模板行
        // 将 modified_logs 中的唯一行写入 allmapping.txt（字典），
        // 将每行对应的ID写入 allids.bin（序列），供Python解压时重建模板。
        denum_processor.store_content_with_ids(modified_logs, "all", std::to_string(block_id), "lzma");
    }
    else if (output_logs == "2") {
        // 模式2：仅收集无数字日志到内存，供研究使用（RQ3实验）
        final_outputs[block_id] = modified_logs;
    }
    else if (output_logs == "3") {
        // 模式3：直接写文本文件，不做Dict-ID编码（RQ4消融实验）
        std::ofstream final_log_file(logname_dir +  "logswithoutnums.log");
        if (!final_log_file.is_open()) {
            std::cerr << "Unable to open final log file for writing: " << logname_dir +  "logswithoutnums.log" << std::endl;
            return;
        }
        for (const auto& log : modified_logs) {
                final_log_file << log << std::endl;
            }
        final_log_file.close();
    }

    // ---------- 步骤4: 写入数字文件 ----------
    // 遍历所有占位符模式（<I>,<T>,<a>,<b>,<c>,<da>,...），将对应的数字列表写入 .bin 文件。
    // delta_transform 排除列表：<I>(IP无单调性)、<a>/<b>/<c>(短数字delta收益小且需与Python对齐)
    for (const auto &pair : final_patterns) {
        std::vector<unsigned char> encoded_buffer;
        std::string sanitized_filename = sanitize_filename(pair.first);
        std::string filename = logname_dir  + sanitized_filename + ".bin";
        std::ofstream file(filename, std::ios::out | std::ios::binary);
        std::list<int64_t>  transformed;
        // delta变换决策（需与Python解压端 strictly 对齐！）
        if (pair.first != "<I>" && pair.first != "<a>" && pair.first != "<b>"&& pair.first != "<c>") {
            transformed = delta_transform(pair.second);   // 做delta（适合单调序列如时间戳）
        } else {
            transformed = pair.second;                     // 不做delta（IP/短数字 无单调性）
        }
        // elastic_encode：变长整数编码，小数字→1字节，大数字→多字节
        for (const auto& num : transformed) {
            auto encoded = elastic_encode(num);
            file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        }
        file.close();
    }

    // ---------- 步骤5: 写入格式描述文件（2024新增） ----------
    // _fmt_.txt 记录了每个结构化模式的解析参数，供Python解压端重建原始格式。
    // 格式：<标签> <组数> <填充宽度> <Python格式化模板>
    // 例如：<I> 4 3 {}.{}.{}.{}
    if (!log_processor.pattern_format_templates.empty()) {
        std::string fmt_path = logname_dir + "_fmt_.txt";
        std::ofstream fmt_file(fmt_path);
        if (fmt_file.is_open()) {
            for (const auto& kv : log_processor.pattern_format_templates) {
                const std::string& tag = kv.first;
                const std::string& tmpl = kv.second;
                int ng = log_processor.pattern_group_counts[tag];
                int pw = log_processor.pattern_group_widths[tag];
                fmt_file << tag << " " << ng << " " << pw << " " << tmpl << "\n";
            }
            fmt_file.close();
        }
    }

    // ---------- 步骤6: tar.xz 打包 ----------
    // 将整个块目录压缩为 compressed{N}.xz，所有产物打包进一个文件。
    compressDirectory(output_dir, block_id);
}

// ============================================================================
// Part Five: main 函数 — 程序入口
// ============================================================================
// 用法：./denum_compress <数据集名> <块大小> <模式>
//   - argv[1]: 数据集名（Apache, Linux, BGL, Hadoop 等15种）
//   - argv[2]: 块大小（论文默认 100000 行/块）
//   - argv[3]: 模式 "1"=默认完整压缩, "2"=仅输出无数字日志, "3"=不做Dict-ID
//
// 数据流：
//   读取 Logs/<logname>/<logname>.log  →  分块(每块BLOCK_SIZE行)  →
//   多线程并行调用 processLogBlock()   →  输出 output/<logname>/compressed{N}.xz
//
// 输出指标：
//   - Compression speed (MB/s): 压缩速度
//   - Achieved size (Bytes): 压缩后总字节数
//   - Compression ratio: 原始大小 / 压缩后大小
// ============================================================================*/
int main(int argc, char* argv[]) {
    // =========================================================================
    // 命令行参数解析
    // 用法:
    //   1. 传统模式:  ./denum_compress <数据集> <块大小> <模式>
    //   2. 自动检测:  ./denum_compress --config <cfg路径> <数据集> <块大小> <模式>
    //   3. 通用回退:  ./denum_compress <任意名> <块大小> <模式>
    //                  （无硬编码模式时自动使用纯数字+变量提取）
    // =========================================================================
    if (argc < 4) {
        std::cerr << "用法: " << argv[0] << " [--config <cfg>] <数据集> <块大小> <模式>" << std::endl;
        std::cerr << "  数据集:     名称 (Apache, Linux, BGL...) 或任意自定义名" << std::endl;
        std::cerr << "  块大小:     每块行数 (论文默认100000)" << std::endl;
        std::cerr << "  模式:       1=默认 2=仅去数字 3=不做Dict-ID" << std::endl;
        std::cerr << "  --config:   自动检测配置文件路径（可选）" << std::endl;
        std::cerr << "              生成: python3 sampling_detector.py <日志文件>" << std::endl;
        return 1;
    }

    // 解析 --config 可选参数
    std::string config_path;
    int arg_offset = 1;
    if (std::string(argv[1]) == "--config") {
        if (argc < 6) {
            std::cerr << "错误: --config 需要后跟 <cfg路径> <数据集> <块大小> <模式>" << std::endl;
            return 1;
        }
        config_path = argv[2];
        arg_offset = 3;
    }

    const std::string logname = argv[arg_offset];
    size_t BLOCK_SIZE;
    const std::string output_logs = argv[arg_offset + 2];

    try {
        BLOCK_SIZE = std::stoul(argv[arg_offset + 1]);
    } catch (const std::invalid_argument& e) {
        std::cerr << "无效的块大小参数: " << argv[arg_offset + 1] << std::endl;
        return 1;
    } catch (const std::out_of_range& e) {
        std::cerr << "块大小值超出范围: " << argv[arg_offset + 1] << std::endl;
        return 1;
    }

    std::cout << "数据集: " << logname << ", 块大小: " << BLOCK_SIZE << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    // 日志文件路径：Logs/<数据集>/<数据集>.log
    const std::string log_path = "Logs/" + logname + "/" + logname + ".log";

    const int num_threads = 4; // 并行线程数，可根据CPU核心数调整
    std::vector<std::future<void>> futures;
    std::map<int, std::vector<std::string>> final_outputs;

    // 初始化两个核心处理器（支持 config 模式或传统模式）
    LogProcessor log_processor = config_path.empty()
        ? LogProcessor(logname)                     // 传统模式：按数据集名查找硬编码模式
        : LogProcessor(logname, config_path);       // 自动检测模式：从配置文件加载
    DenumLogProcessor denum_processor(logname);     // Part Three: 变量提取（始终通用）

    // ------ 打开日志文件 ------
    std::ifstream log_file(log_path);
    if (!log_file.is_open()) {
        std::cerr << "Unable to open log file: " << log_path << std::endl;
        return 1;
    }

    // ------ 清理旧输出并创建输出目录 ------
    std::string command = "rm -rf output/" + logname + "/* ";
    int result = std::system(command.c_str());
    ensure_directory_exists("output/" + logname);

    // ------ 分块读取 + 多线程并行压缩 ------
    std::vector<std::string> block;
    block.reserve(BLOCK_SIZE);
    std::string line;
    int block_index = 0;  // 0-based 块序号（与Python解压端 chunkID=0 对齐）

    while (std::getline(log_file, line)) {
        block.push_back(line);
        if (block.size() == BLOCK_SIZE) {
            // 块满 BLOCK_SIZE 行 → 启动异步任务压缩此块
            futures.push_back(std::async(std::launch::async, processLogBlock, block, block_index, "output/" + logname, std::ref(log_processor), std::ref(denum_processor), std::ref(final_outputs), output_logs));
            block.clear();
            ++block_index;
        }
    }

    // 处理最后不足一块的剩余行
    if (!block.empty()) {
        futures.push_back(std::async(std::launch::async, processLogBlock, block, block_index, "output/" + logname, std::ref(log_processor), std::ref(denum_processor), std::ref(final_outputs),output_logs));
    }

    // ------ 等待所有线程完成 ------
    for (auto& future : futures) {
        future.wait();
    }

    // ------ 计算并输出压缩指标 ------
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    std::uintmax_t fileSize = std::filesystem::file_size(log_path);
    double dataSizeInMB = static_cast<double>(fileSize) / (1024.0 * 1024.0);
    double speedInMBPerSecond = dataSizeInMB / duration.count() * 1000; // CS (Compression Speed)
    double totalSize = 0;     // 压缩后总MB
    double totalBytes = 0;    // 压缩后总字节
    for (int i = 0; i <= block_index; ++i) {
        std::string compressed_path = "output/" + logname + "/compressed" + std::to_string(i) + ".xz";
        std::uintmax_t achieved_fileSize = std::filesystem::file_size(compressed_path);
        double dataSizeInMB = static_cast<double>(achieved_fileSize) / (1024.0 * 1024.0);
        totalSize += dataSizeInMB;
        totalBytes += achieved_fileSize;
    }
    double CR = dataSizeInMB / totalSize;  // 压缩比 = 原始大小 / 压缩后大小

    // 输出结果（保留三位小数）
    std::cout << "=== Compression Statistics ===" << std::endl;
    std::cout << "Original log size: " << fileSize << " Bytes (" << std::fixed << std::setprecision(3) << dataSizeInMB << " MB)" << std::endl;
    std::cout << "Compressed size: " << std::fixed << std::setprecision(0) << totalBytes << " Bytes (" << std::fixed << std::setprecision(3) << totalSize << " MB)" << std::endl;
    std::cout << "Compression time: " << duration.count() << " ms" << std::endl;
    std::cout << "Compression speed: " << std::fixed << std::setprecision(3) << speedInMBPerSecond << " MB/s" << std::endl;
    std::cout << "Compression ratio: " << std::fixed << std::setprecision(3) << CR << std::endl;
    std::cout << "Space saving: " << std::fixed << std::setprecision(1) << (1 - 1.0/CR)*100 << "%" << std::endl;

    // 模式2：额外输出无数字日志文件（供RQ3实验）
    if (output_logs == "2") {
        std::ofstream final_log_file("output/" + logname + ".log");
        for (const auto& [block_id, output] : final_outputs) {
            std::size_t length = output.size();
            for (const auto& log : output) {
                final_log_file << log << std::endl;
            }
        }
        final_log_file.close();
    }
    return 0;
}