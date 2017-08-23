# -*- encoding:utf-8 -*-

# Date : 2016-12-16

# 生成错误信息类、详细信息 text 文件
import os
import time


class ErrorLogsFile:

    errtext = "none"

    def __init__(self, errormessage):
        self.errtext = errormessage

    def saveerrorlog(self):
        # "."表示当前路径 ".." 表示当前路径的上一级
        path = os.path.abspath("..") + "\\errrorlog\\"
        filename = path + "errorlog%s.txt" % time.strftime("%Y-%m-%d", time.localtime())
        if os.path.exists(path):
            # r只读，w可写，a追加
            filetext = open(filename, "a")
            filetext.writelines(
                "%s : " % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "%s \r\n" % self.errtext)
            filetext.close()
        else:
            os.makedirs(path)
            # r只读，w可写，a追加
            filetext = open(filename, "a")
            filetext.writelines(
                "%s : " % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "%s \r\n" % self.errtext)
            filetext.close()
