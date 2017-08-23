# -*- coding:utf-8 -*-

import requests
import redis
import json
import time
import threading
import HTMLParser
from datetime import datetime
from errorlogs import ErrorLogsFile
import re
import sys
default_encoding = "utf-8"
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)


# 存放采集手机数据的redis key name
redis_key_phone_w = "dpc_category_url:start_url_CHUNTAO"
# 读取筛选无效的手机数据，过滤
redis_key_invalid_r = "dpc_category_url:item_invalid_CHUNTAO"
# 读取手机redis key name--"dpc_phone_keywords:item_keywords"
redis_key_phone_model = "dpc_category_url:item_keywords_CHUNTAO"
# 存放采集数据的redis key result
redis_key_phone_result = "dpc_category_url:result_CHUNTAO"
# 代理ip
redis_key_proxy = "proxy:iplist4"

# 连接redis
rconnection_yz = redis.Redis(host='117.122.192.50', port=6479, db=0)
# 存放数据，测试使用,正常发布任务，该处需要注释
rconnection_test = redis.Redis(host='117.122.192.50', port=6479, db=0)
# rconnection_test = redis.Redis(host='1.119.7.234', port=26479, db=0)
# rconnection_test = redis.Redis(host='192.168.2.245', port=6379, db=0)
# 2017-02-10 11:49111:35.608000
# 设置循环次数，如果超过该次数，则跳出
errorCount = 8

# 存放搜索无效的关键词
invalid_keywords = rconnection_test.lrange(redis_key_invalid_r, 0, -1)


CHUNTAO_url = "https://item.taobao.com/item.htm?id={0}&amp;areaid=&amp;"

search_CHUNTAO_url = "https://item.taobao.com/item.htm/search_product.hCHUN TAO?s=0&q={0}"


# 根据url查找村淘网数据
def search_CHUNTAO(totalpage, urls, category):
    # url = "https://list.CHUN TAOall.com/search_product.hCHUN TAO?cat=50936015&s=0"
    sesson = requests.session()
    isValue = True
    index = 0
    # print "总页数:%s" % totalye
    while isValue:
        if errorCount < index:
            print "%s: CHUN TAO not find this url : %s" % (datetime.now(), urls)
            break
        # 随机获取代理ip
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        # 重新赋值 url
        pagecount = 40*totalpage
        url = urls.format(pagecount)
        try:
            print "toale : %s ,url : %s"%(totalpage, url)
            req = sesson.get(url, timeout=30)
            hCHUNTAOl = req.text
            # print hCHUNTAOl
            req.close()
            isValue = False
            if hCHUNTAOl:
                # 查找商品名称、月销量、单价、旗舰店
                # tzurl = re.findall(r'<div class="item-info">[\s\S]*?</div>[\s\S]*?</div>[\s\S]*?</a>', hCHUNTAOl)
                tzurl = re.findall(r'<div class="item-info">[\s\S]*?</div>[\s\S]*?</div>[\s\S]*?</li>', hCHUNTAOl)
                wgh = 0
                if len(tzurl) == 0:
                    wgh = 1
                    tzurl = re.findall(r'<span class="volume">月销量(.*)</span>', hCHUNTAOl)
                if tzurl:
                    for i in tzurl:
                        if (wgh == 1):
                            ix = i[1]
                        else:
                            ix = i
                        if len(ix) > 0:

                            # 【村淘优选】判断
                            search_value = re.search('(?<=title=")村淘优选(?=")',str(ix))
                            if search_value == None:
                                continue
                            # 开始查找id号,
                            search_id = re.search('(id=(?P<dd>.*?)")', ix)
                            if search_id:
                                # 截取页面信息id
                                spid = search_id.group("dd")
                                # print "spid : %s"% spid
                                # 截取页面信息商品名称
                                if "title=" not in ix:
                                    continue
                                # 商品名称
                                search_spname = re.search('(?<=title=").*(?=".*target="_blank">)', ix)
                                if search_spname:
                                    spname = search_spname.group()
                                    # print "spname : %s" % spname
                                    # 判断是否无效,如果isspnameTrue 为True,则表示无效数据，过滤，如果为False，表示是有效数据
                                    isspnameTrue = False
                                    for ia in invalid_keywords:
                                        if ia in spname and "送" not in spname:
                                            isspnameTrue = True
                                    if isspnameTrue == True:
                                        continue
                                    else:
                                        # print ix
                                        # 查找月销量
                                        search_yxl = re.search('(?<=<span class="volume">月销量).*(?=</span>)', str(ix))
                                        if search_yxl:
                                            yxl = search_yxl.group()
                                            yxl = str(yxl).replace("&nbsp;", "")
                                            # print "search_yxl : %s" % yxl
                                            # 查找单价
                                            search_price = re.search('(?<=class="price-value">).*(?=<)', str(ix))
                                            if search_price:
                                                price = search_price.group()
                                                # print "price : %s" % price
                                                result_url=CHUNTAO_url.format(spid)
                                                result = '{"urlweb":"cun","urls":"%s","urlleibie":"%s","price":"%s","yxl":"%s","spname": "%s"}'% (result_url, category,price,yxl,spname)
                                                # 拼写json类型保存至redis
                                                rconnection_test.lpush(redis_key_phone_w, result)
                                            else:
                                                print "%s:can not find CHUN TAO price,please search regular is valid:%s" % (
                                                    datetime.now(), url)
                                                wr = ErrorLogsFile(
                                                    "can not find CHUN TAO price,please search regular is valid:%s" % (
                                                        url))
                                                wr.saveerrorlog()
                                        else:
                                            print "%s:can not find CHUN TAO yue xiao liang,please search regular is valid:%s" % (
                                            datetime.now(), url)
                                            wr = ErrorLogsFile(
                                                "can not find CHUN TAO yue xiao liang,please search regular is valid:%s" % (
                                                url))
                                            wr.saveerrorlog()
                                else:
                                    print "%s:can not find CHUN TAO spname,please search regular is valid:%s" % (datetime.now(),url)
                                    wr = ErrorLogsFile(
                                         "can not find CHUN TAO spname,please search regular is valid:%s" % ( url))
                                    wr.saveerrorlog()
                            else:
                                print "%s:can not find CHUN TAO id,please search regular is valid" % datetime.now()
                                wr = ErrorLogsFile("can not find CHUN TAO id,please search regular is valid:%s" % ( url))
                                wr.saveerrorlog()
                else:
                    print "%s:CHUN TAO url---the first regular is valid %s?"%(datetime.now(), url)
                    wr = ErrorLogsFile("CHUN TAO url---the first regular is valid:%s?" % (url))
                    wr.saveerrorlog()
        except Exception, e:
            isValue = True
            index += 1
            if index == errorCount:
                print "connection redis error: %s , %s" % (index, e)
                wr = ErrorLogsFile("connection redis error: url:%s ,errormessage:%s" % (url,e))
                wr.saveerrorlog()
            time.sleep(5)


# 采集总页数
def search_CHUNTAO_page(urlx, category):
    print urlx
    url = str(urlx).replace("s=0", "s={0}") #"https://list.CHUN TAOall.com/search_product.hCHUN TAO?cat=50936015&s={0}"
    sesson = requests.session()
    isValue = True
    index = 0
    # 先查找页面显示的 页数
    while isValue:
        if errorCount < index:
            print "%s: CHUN TAO not find this url : %s" % (datetime.now(), url)
            break
        # 随机获取代理ip
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        try:
            req = sesson.get(url, timeout=30)
            hCHUNTAOl = req.text
            req.close()
            isValue = False
            if hCHUNTAOl:
                # print hCHUNTAOl
                # 查找总页数
                totalye = re.search('(?<=data-totalPage=")\d+(?=")', str(hCHUNTAOl))
                # print totalye
                if totalye:
                    print totalye.group()
                    # search_CHUNTAO(1, url, category)
                    for int_page in range(int(totalye.group())):
                        search_CHUNTAO(int_page, url, category)
                else:
                    print "search the total page regular isvalid ?"
                    wr = ErrorLogsFile("search the total page regular isvalid ?: url:%s" % (url))
                    wr.saveerrorlog()
        except Exception, e:
            isValue = True
            index += 1
            print "errormessage: %s" % e
            if index == errorCount:
                print "search total page category error: %s , %s" % (index, e)
                wr = ErrorLogsFile("search  total page category error: url:%s,errormessage:%s" % (url, e))
                wr.saveerrorlog()
                return
            time.sleep(5)

def run_cjurl():
    keyisvalue = rconnection_test.keys(redis_key_phone_model)
    if keyisvalue:
        print keyisvalue
        # 读取品牌型号搜索
    else:
        print "没有找到key: %s"% redis_key_phone_model

    while True:
        axw = rconnection_test.lpop(redis_key_phone_model)
        if axw:
            modeljson = json.loads(axw)
            url = modeljson["url"]
            category = modeljson["category"]
            print url
            if url == "":
                url = search_CHUNTAO_url.format(category)
                print "continue: %s" % url
                search_CHUNTAO_page(url, category)
                # continue
            else:
                search_CHUNTAO_page(url, category)
        else:
            print "没有找到key, break"
            break


# 采集品牌、型号
def search_CHUNTAO_brand(urls, attributes):
    sesson = requests.session()
    isValue = True
    index = 0
    # print "总页数:%s" % totalye
    while isValue:
        if errorCount < index:
            print "%s: CHUN TAO not find this url : %s" % (datetime.now(), urls)
            break
        # 随机获取代理ip
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        try:
            req = sesson.get(urls, timeout=30)
            hCHUNTAOl = HTMLParser.HTMLParser().unescape(req.text)
            req.close()
            isValue = False
            if hCHUNTAOl:
                # print hCHUNTAOl
                # 查找型号
                model = ""
                tz_model = re.search(r'(?<=型号:).*(?=<)', str(hCHUNTAOl))
                if tz_model:
                    model = str(tz_model.group()).replace("&nbsp;", "").replace(" ", "")
                if model == "":
                    tz_model2 = re.search(r'(?<=货号:).*(?=<)', str(hCHUNTAOl))
                    if tz_model2:
                        model = str(tz_model2.group()).replace("&nbsp;", "").replace(" ", "")
                    else:
                        print "not find model,please search regular is valued?: %s , %s" % (datetime.now(), urls)
                        wr = ErrorLogsFile("not find model,please search regular is valued?: %s , %s" % (datetime.now(), urls))
                        wr.saveerrorlog()
                tz_brand = re.search(r'品牌[^"]{0,5}:(?P<dd>.*?)</li>', str(hCHUNTAOl))
                brand = ""
                if tz_brand:
                    brand = str(tz_brand.group("dd")).replace("&nbsp;", "").replace(" ", "")
                    brand = brand.replace(" ", "")
                else:
                    # print str(hCHUNTAOl)
                    print "not find brand,please search regular is valued?: %s , %s" % (datetime.now(), urls)
                    wr = ErrorLogsFile(
                        "not find brand,please search regular is valued?: %s , %s" % (datetime.now(), urls))
                    wr.saveerrorlog()
                isValue = False
                result = '{"urlweb":"cun","urls":"%s",' \
                             '"urlleibie":"%s","price":"%s",' \
                             '"yxl":"%s","spname": "%s",' \
                             '"brand":"%s","model":"%s"}' % \
                             (attributes["urls"], attributes["urlleibie"],
                              attributes["price"], attributes["yxl"], attributes["spname"], brand, model)
                # 拼写json类型保存至redis
                rconnection_test.lpush(redis_key_phone_result, result)
        except Exception, e:
            print "connection redis error: %s , %s" % (index, e)
            isValue = True
            index += 1
            if index == errorCount:
                print "connection redis error: %s , %s" % (index, e)
                wr = ErrorLogsFile("connection redis error: url:%s ,errormessage:%s" % (urls,e))
                wr.saveerrorlog()
            time.sleep(5)


def run_cjbrand():
    keyisvalue = rconnection_test.keys(redis_key_phone_w)
    if keyisvalue:
        print keyisvalue
        # 读取品牌型号搜索
    else:
        print "没有找到key: %s"% redis_key_phone_w

    while True:
        axw = rconnection_test.lpop(redis_key_phone_w)
        try:
            if axw:
                modeljson = json.loads(str(axw).replace("\\", "、").replace("	", ""))
                url = modeljson["urls"]
                # print url
                search_CHUNTAO_brand(url, modeljson)
            else:
                print "没有找到key, break"
                break
        except Exception, e:
            print "load url json error : %s" % e
            wr = ErrorLogsFile(
                "load url json error : %s ,,,,, error : %s " % (axw, e))
            wr.saveerrorlog()


if True:
    # url = "https://cunlist.taobao.com/?q=电饭煲&s=0&pSize=40"
    # attr={
    # "urlweb": "cun",
    # "urls": "https://item.taobao.com/item.htm?id=19900257351&amp;areaid=&amp;",
    # "urlleibie": "电磁炉",
    # "price": "220.00",
    # "yxl": "0",
    # "spname": "蒸煮焖炖高效节能真空压力炒锅健康炒锅煤气灶电磁炉通用炒锅"
    # }
    # search_CHUNTAO_brand(url, attr)
    # search_CHUNTAO(1, url,"电饭煲")

    print "begin thread : %s" % datetime.now()
    threads = []
    threadcount = 10
    for ai in range(threadcount):
        # threads.append(threading.Thread(target=run_cjurl, args=()))
        threads.append(threading.Thread(target=run_cjbrand, args=()))
    for tx in threads:
        tx.start()
    for tx in threads:
        tx.join()
    print "end thread : %s" % datetime.now()