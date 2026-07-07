import pandas as pd
import numpy as np
import subprocess
import os
import sys
from tqdm import tqdm
import pickle as pkl
import time
import re
import ast
from datetime import datetime
import multiprocessing as mp
from subprocess import call
import chardet # 添加
import glob

sys.path.append('../')
from utils.config import Config
from utils.Logger import *
from utils.util_code import *
from LogMeta import LogMeta
# from LogCompression.backup.calculate_diff import *

def lr_log_parsing(
                input_path="",
                outdir="",
                template_path=""):
    
    print("step 2.1: log parsing... {}".format(input_path))

    filename = input_path.split("/")[-1].split('.')[0]
    input_dir = os.path.dirname(input_path)
    input_dir += '/'
    outdir += '/'
    print(outdir)
    # 修改：将 -D 改为 Z，没啥用
    parsing_cmd = "./parser/THULR -I " + input_dir + " -X " + filename + " -O " + outdir + " -T " + template_path + " -E " + " Z " + " -F " + os.path.join(template_path, "head.format")
    print(parsing_cmd)

    #
    if os.path.isdir(outdir):
        print("目录outdir验证存在！")
    else:
        print("目录outdir不存在！")

    proc = subprocess.run(parsing_cmd, shell=True, capture_output=True, text=True)
    res = proc.returncode
    print("res: {}".format(res))
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.stderr:
        print(proc.stderr.strip())
    if res != 0:
        err_msg = [
            "THULR log parser execution failed.",
            f"command: {parsing_cmd}",
            f"return code: {res}",
        ]
        if "GLIBC_" in (proc.stderr or ""):
            err_msg.append(
                "Detected GLIBC version mismatch. Please rebuild ./parser/THULR on your current machine "
                "(e.g., cd python_compression/parser && make clean && make)."
            )
        raise RuntimeError("\n".join(err_msg))

    #
    if os.path.isdir(outdir):
        print("THULR后，目录outdir验证存在！")
    else:
        print("THULR后，目录outdir不存在！")
    

def read_parsing_result(input_dir="",
                ds="",
                template_path=""):
    
    print("Step 2.2, log matching, parsing file: %s" % input_dir)
    t1 = time.time()
    header_list = []
    files = [i for i in os.listdir(input_dir) if i.endswith("head")]
    files = sorted(files)
    for i in files:
        header = read_col(os.path.join(input_dir, i))
        header_list.append(header)
        del header
            
    if len(header_list) == 0:
        raise RuntimeError(
            "No *.head files were produced by THULR. "
            "Please check parser errors in step 2.1."
        )
    header_df = pd.concat(header_list, axis=1)
    eids_df, df_var = read_var(os.path.join(input_dir, "var.var"))
    log_meta = LogMeta(headers=header_df.values, eids=eids_df.values, var_list=df_var.values)

    del eids_df, df_var, header_df
        
    log("info", "log parsing took %.3f s, parsing into %d templates" %
          (time.time() - t1, log_meta.template_num))
    
    return log_meta



def read_col(filename: str):
    ids = []
    # 添加
    # with open(filename, 'rb') as f:
    #     raw_data = f.read()
    #     encoding = chardet.detect(raw_data)['encoding']
    # print("文件名：{}".format(filename))
    # # 复制文件
    # shutil.copy2(filename, "./tempfile")
    # print("编码方式为：{}".format(encoding))
    # with open(filename, 'r', encoding=encoding) as f:
    #     for line in f.readlines():
    #         ids.append(line.strip())
    # 修改 添加编码utf-8,完全不行
    with open(filename, "r", encoding='latin-1') as f:
        for line in f.readlines():
            ids.append(line.strip())
    # with open(filename, "r") as f:
    #     for line in f.readlines():
    #         ids.append(line.strip())
    return pd.Series(ids)



def read_var(filename: str):
    # parameter_lists = pd.read_csv(filename, sep=",")
    parameter_lists = []
    eids = []
    with open(filename, "r") as f:
        for line in f.readlines():            
            parse_list = line.strip().split("||")
            eids.append(parse_list[0])
            parameter_lists.append(parse_list[1:-1])
    return pd.Series(eids).astype(int), pd.Series(parameter_lists)


def get_logsequence(series: pd.Series, h=50):
    print("Step 2.3, get log sequence list")

    sequences_list = []
    i = 0
    for i in range(int(np.floor(series.shape[0] / h))):
        sequences_list.append(series[i * h:(i + 1) * h].tolist())
    # TODO: handling the last window
    last = series[(i + 1) * h:].tolist()
    last.extend([-1] * (h - len(last)))
    sequences_list.append(last)
    sequences_list = np.array(sequences_list)

    log("info", "Total length is %d, cut into %d sequences" %
          (series.shape[0], len(sequences_list)))

    return sequences_list

# def get_diff_df(df: pd.DataFrame()):
#     if df.shape[0] == 0 or df.shape[1] == 0:
#         return df
#     df = df.astype(int)
#     diff = np.diff(df, axis=1)
#     diff = np.hstack((df[:,0].reshape(-1,1), diff))
#     diff = diff.astype(int)
#     return diff


def log_parsing(
    input_file=None,
    outdir="",
    ds="",
    template_path="",
    fileNo=-1
):
    results = {}
    for idx, file in enumerate(input_file):
        temp_dir = os.path.join(outdir, str(fileNo[idx]) + 'temp')
        mkdir(temp_dir)
        lr_log_parsing(input_path=file, 
                    outdir=temp_dir, 
                    template_path=template_path)
        
    return results

def preprocess(
    input_file=None,
    outdir="",
    ds="",
    template_path="",
    h=50
):
    # 1. log parsing
    temp_dir = os.path.join(outdir, 'temp')
    mkdir(temp_dir)
    # 添加：看文件是否创建
    if os.path.isdir(temp_dir):
        print("目录验证存在！")
    else:
        print("目录未创建成功！")

    lr_log_parsing(input_path=input_file, 
                   outdir=temp_dir, 
                   template_path=template_path)
    # keep processing failed log
    for failed_log in glob.glob(os.path.join(temp_dir, "*failed.log")):
        shutil.move(failed_log, outdir)
    # 2. read parsing result
    # 添加
    print("开始read_parsing_result")
    log_meta = read_parsing_result(input_dir=temp_dir,
                ds=ds,
                template_path=template_path)
    # 添加
    print("完成read_parsing_result")
    # rm template dir
    shutil.rmtree(temp_dir)
    # 3. cut into fixed-length log sequences
    seq_list = get_logsequence(log_meta.eids, h=h)
    return log_meta, seq_list