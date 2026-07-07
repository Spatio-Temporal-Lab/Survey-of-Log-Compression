#!/usr/bin/env python3
"""
Denum Python 解压入口 — 从 C++ 压缩产物还原日志。

用法:
  # 传统模式（数据集名需在 setting 字典中）
  python3 decompress.py Apache

  # 自动检测模式（从 JSON 配置文件读取数据集信息）
  python3 decompress.py --config ../output/MyApp_auto.json MyApp

  # 通用模式（无需预配置，自动查找压缩产物）
  python3 decompress.py MyApp

存储模型:
  - 解压器从压缩包中读取 _fmt_.txt（格式模板）恢复结构化数字
  - 无需外部配置文件即可完整还原
  - --config 仅作为补充（数据集路径等元信息）
"""

import os
import sys
import time
import json
import argparse
import Denum_simplel as LZ

# ==============================================================================
# 预定义数据集配置（向后兼容硬编码模式）
# 注意：这是可选的。新数据集无需在此添加，使用 --config 或自动搜索即可。
# ==============================================================================
setting = {
    'Apache':      {'dataset_name': 'Apache',      'input_path': '../Logs/Apache/Apache.log'},
    'OpenSSH':     {'dataset_name': 'OpenSSH',     'input_path': '../Logs/OpenSSH/OpenSSH.log'},
    'Linux':       {'dataset_name': 'Linux',       'input_path': '../Logs/Linux/Linux.log'},
    'Proxifier':   {'dataset_name': 'Proxifier',   'input_path': '../Logs/Proxifier/Proxifier.log'},
    'Zookeeper':   {'dataset_name': 'Zookeeper',   'input_path': '../Logs/Zookeeper/Zookeeper.log'},
    'Mac':         {'dataset_name': 'Mac',         'input_path': '../Logs/Mac/Mac.log'},
    'HDFS':        {'dataset_name': 'HDFS',        'input_path': '../Logs/HDFS/HDFS.log'},
    'Android':     {'dataset_name': 'Android',     'input_path': '../Logs/Android/Android.log'},
    'BGL':         {'dataset_name': 'BGL',         'input_path': '../Logs/BGL/BGL.log'},
    'HPC':         {'dataset_name': 'HPC',         'input_path': '../Logs/HPC/HPC.log'},
    'Spark':       {'dataset_name': 'Spark',       'input_path': '../Logs/Spark/Spark.log'},
    'Hadoop':      {'dataset_name': 'Hadoop',      'input_path': '../Logs/Hadoop/Hadoop.log'},
    'HealthApp':   {'dataset_name': 'HealthApp',   'input_path': '../Logs/HealthApp/HealthApp.log'},
    'OpenStack':   {'dataset_name': 'OpenStack',   'input_path': '../Logs/OpenStack/OpenStack.log'},
    'Windows':     {'dataset_name': 'Windows',     'input_path': '../Logs/Windows/Windows.log'},
    'Thunderbird': {'dataset_name': 'Thunderbird', 'input_path': '../Logs/Thunderbird/Thunderbird.log'},
}


def load_config_file(config_path: str) -> dict:
    """从 JSON 配置文件加载数据集设置（由 sampling_detector.py 生成）。"""
    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    return {
        'dataset_name': cfg.get('dataset', 'Unknown'),
        'input_path': cfg.get('input', ''),
    }


def find_log_path(dataset_name: str) -> str:
    """自动搜索日志文件路径。"""
    candidates = [
        f'../Logs/{dataset_name}/{dataset_name}.log',
        f'../Logs/{dataset_name}.log',
        f'Logs/{dataset_name}/{dataset_name}.log',
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return candidates[0]  # 返回默认路径（后续会报错提示）


def main():
    parser = argparse.ArgumentParser(
        description='Denum 解压器 — 从 C++ 压缩产物还原日志')
    parser.add_argument('setting_name', nargs='?', default=None,
                        help='数据集名称（如 Apache、Linux 等）或任意自定义名')
    parser.add_argument('--config', '-c', default=None,
                        help='自动检测配置文件路径（JSON，由 sampling_detector.py 生成）')
    parser.add_argument('--log-path', '-l', default=None,
                        help='原始日志文件路径（用于统计压缩比，可选）')
    args = parser.parse_args()

    # ---- 确定数据集配置 ----
    if args.config:
        # 从 JSON 配置文件加载
        if not os.path.exists(args.config):
            print(f"错误: 配置文件不存在: {args.config}")
            return
        applied_setting = load_config_file(args.config)
        # 数据集名优先用命令行参数，其次用配置文件中的
        if args.setting_name:
            applied_setting['dataset_name'] = args.setting_name
    elif args.setting_name and args.setting_name in setting:
        # 传统模式：使用预定义配置
        applied_setting = setting[args.setting_name]
    elif args.setting_name:
        # 通用模式：自动搜索日志路径
        applied_setting = {
            'dataset_name': args.setting_name,
            'input_path': find_log_path(args.setting_name),
        }
        print(f"数据集 '{args.setting_name}' 不在预定义列表中，使用自动搜索。")
        print(f"  日志路径: {applied_setting['input_path']}")
    else:
        print("错误: 请指定数据集名称或使用 --config 参数。")
        print("用法: python3 decompress.py [--config <cfg>] <数据集名>")
        return

    # 允许命令行覆盖日志路径
    if args.log_path:
        applied_setting['input_path'] = args.log_path

    # ---- 执行解压 ----
    time_start = time.perf_counter()
    compressor = LZ.dataloader(applied_setting)
    compressor.decompress()
    time_end = time.perf_counter()
    elapsed_ms = (time_end - time_start) * 1000.0
    elapsed_s = time_end - time_start

    # ---- 计算统计指标 ----
    log_path = applied_setting.get('input_path', '')
    if os.path.exists(log_path):
        original_size = os.path.getsize(log_path)
        original_mb = original_size / (1024.0 * 1024.0)
    else:
        original_size = 0
        original_mb = 0

    logname = applied_setting['dataset_name']
    compressed_total = 0
    chunk_id = 0
    while True:
        compressed_path = f'../output/{logname}/compressed{chunk_id}.xz'
        if os.path.exists(compressed_path):
            compressed_total += os.path.getsize(compressed_path)
            chunk_id += 1
        else:
            break

    if compressed_total > 0:
        compressed_mb = compressed_total / (1024.0 * 1024.0)
        ratio = original_size / compressed_total if original_size > 0 else 0
        speed_mbps = original_mb / elapsed_s if elapsed_s > 0 else 0
    else:
        compressed_mb = 0
        ratio = 0
        speed_mbps = 0

    # ---- 输出解压统计 ----
    print(f"解压完成，耗时 {elapsed_ms:.0f} 毫秒。")
    if speed_mbps > 0:
        print(f"解压速度: {speed_mbps:.3f} MB/s")
    print(f"原始日志大小: {original_size} Bytes ({original_mb:.3f} MB)")
    print(f"压缩后大小: {compressed_total} Bytes ({compressed_mb:.3f} MB)")
    if ratio > 0:
        print(f"压缩比: {ratio:.3f}")
        print(f"空间节省: {(1 - 1/ratio)*100:.1f}%")


if __name__ == "__main__":
    main()
