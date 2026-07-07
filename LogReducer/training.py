import sys
import os
from subprocess import call
import argparse
import util
import time
import logloader
import header
import parser as TemplateParser
#Types = ["Android"]
#Types = ["Android","Apache","Bgl","Hadoop","Hdfs","Hpc","Healthapp","Linux","Mac","Openstack","Proxifier","Spark","Ssh","Windows","Zookeeper","Thunderbird"]
#Types = ["Apsara", "Fastcgi", "Metering", "Monitor", "Ols", "Op", "Oss", "Pangu", "PAudit", "Presto", "PSummary", "Request", "Rpc", "Server", "Shennong", "Sinput", "Sla", "Sys", "Tubo"]
#Types = ["LogA", "LogB", "LogC", "LogD", "LogE", "LogF", "LogG", "LogH", "LogI", "LogJ", "LogK", "LogL", "LogM", "LogN", "LogO", "LogP", "LogQ", "LogR"]
#Path = "./"
Path = "./"
if __name__ == "__main__":
    parse = argparse.ArgumentParser()
    parse.add_argument("--LogPath", "-I", help="The path of log")
    parse.add_argument("--TemplatePath", "-T", help="The output path of log template")
    parse.add_argument("--TemplateLevel", "-TL", default="0", choices=["0", "N"], help="The template level.")
    parse.add_argument("--Mode", "-m", default="Nor", choices=["Nor", "Sam"], help="The mode of training. Nor for single large file, Sam for sample file")
    parse.add_argument("--SampleRate", "-R", default="0.001", help="Sample rate, default by 0.001")
    
    parse.add_argument("--HeadLength", "-L", default="4", help="The designated head length")
    parse.add_argument("--IsMulti", "-Mu", default="F", choices=["T", "F"]) # 修改，添加参数
    parse.add_argument("--HeadRegex", default=r"\[\d{4}\-\d{2}\-\d{2}") # 多行日志结束判断正则表达式

    args = parse.parse_args()
    input_path = args.LogPath
    template_level = args.TemplateLevel
    mode = args.Mode
    template_path = util.path_pro(args.TemplatePath) 
    sample_rate = args.SampleRate

    head_length = int(args.HeadLength)
    if (args.IsMulti == "T"):
        is_multi=True
    else:
        is_multi=False
    head_regex = args.HeadRegex

    if not os.path.exists(template_path):
        os.mkdir(template_path)
    
    s_time = time.time()

    # 采样
    if (mode == "Nor"): #python3 ./sampler.py /path/xx.log /path/xx.log.sample 0.001
        command = "python3 " + Path + "sampler.py "
        command += input_path + " "
        command += input_path + ".sample "
        command += str(sample_rate)
        print(command)
        res = call(command, shell=True)
        if (res != 0):
            print("Sample Error")

    # 读取采样日志数据
    max_length = 300000 # content最大长度
    loader = logloader.LogLoader(headLength=head_length, isMulti=is_multi, headRegex=head_regex, maxLength=max_length)
    LogData, Heads, HeadDelimers = loader.load_to_dataframe(input_path + ".sample")
    # 修改，打印看看Heads
    # print("----------------------------Heads start-------------------------------")
    # for k in list(Heads.keys())[:100]:
    #     print(k, Heads[k])
    # print("----------------------------Heads stop-------------------------------")
    
#print(Heads)
    #Extract head format
    Header = header.Header(head_length = head_length, is_multi=is_multi, head_bound=head_regex, template_path = template_path, heads = Heads, delimer=HeadDelimers)
    Header.outputFormat()

    #Extract templates and corrlation
    Parser = TemplateParser.LogParser(template_path=template_path, depth=8, maxChild=100, st=0.1, correlation_threshold=600, level=template_level)
    Parser.parseUnityEntry(log_dataframe=LogData)

    e_time = time.time()
    print("Total time cost: {}".format(e_time - s_time))

