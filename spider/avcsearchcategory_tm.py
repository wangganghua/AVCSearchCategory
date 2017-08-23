# -*- coding:utf-8 -*-

import requests
import redis
import random
import json
import time
import urllib2
import threading
from HTMLParser import HTMLParser
from datetime import datetime
from errorlogs import ErrorLogsFile
import re
import sys
import cookielib
default_encoding = "utf-8"
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)


# 存放采集手机数据的redis key name
redis_key_phone_w = "dpc_category_url:start_url_tm"
# 读取筛选无效的手机数据，过滤
redis_key_invalid_r = "dpc_category_url:item_invalid_tm"
# 读取手机redis key name--"dpc_phone_keywords:item_keywords"
redis_key_phone_model = "dpc_category_url:item_keywords_tm"
# 代理ip
redis_key_proxy = "proxy:iplist2"
proxykeys = ["proxy:iplist", "proxy:iplist2", "proxy:iplist4"]
# 天猫 cookie
redis_key_tm_cookies = "tm_cookies:list"
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

#
user_agent_list = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 UBrowser/5.6.13381.207 Safari/537.36"
]
cookie = 't=f1d06870685879a126f29aef70568571; cookie2=6f00a95927be6e9e5aea864ba5a014d2;'

TM_url = "http://detail.tmall.com/item.htm?id={0}&amp;areaid=&amp;"

search_TM_url = "https://list.tmall.com/search_product.htm?s=0&q={0}"


# 根据url查找天猫数据
def search_TM(totalpage, urls, category):
    # urls = "https://list.tmall.com/search_product.htm?s=0&q=%E7%94%B5%E5%AD%90%E7%A7%B0"

    sesson = requests.session()
    isValue = True
    index = 0
    # print "总页数:%s" % totalye
    while isValue:
        if errorCount < index:
            print "%s: TM not find this url : %s" % (datetime.now(), urls)
            break
        # 随机获取代理ip
        redis_key_proxy = random.choice(proxykeys)
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        # 随机获取 天猫cookie
        tmcookies = rconnection_test.srandmember(redis_key_tm_cookies)
        # tmcookiejson = json.loads(tmcookies)
        # tmcookie = tmcookiejson["cookie"]
        headers = {
            "User-Agent": "%s" % random.choice(user_agent_list),
            "Accept": "*/*",
            "Referer": "https://www.tmall.com/",
            "Cookie": cookie
        }
        print "begin aaaaaaaaaaaaaaaaaaaaaa"
        # 重新赋值 url
        pagecount = 60*totalpage
        url = urls.format(pagecount)
        try:
            print url
            req = sesson.get(url, headers=headers, timeout=30)
            html = req.text
            req.close()
            isValue = False
            if html:
                tzurl = re.findall(r'<p class="productTitle">[\s\S]*?</p>', html)
                wgh = 0
                if len(tzurl) == 0:
                    wgh = 1
                    tzurl = re.findall(r'(<a href="//detail.tmall.com/item.htm?)+(.*)(</a>)', html)
                    print html

                if tzurl:
                    for i in tzurl:
                        if (wgh == 1):
                            ix = i[1]
                        else:
                            ix = i
                        if len(ix) > 0:
                            # 开始查找id号,
                            search_id = re.search("(id=(?P<dd>.*?))+(&amp;skuId)", ix)
                            if search_id:
                                # 截取页面信息id
                                spid = search_id.group("dd")
                                # print spid
                                # 截取页面信息商品名称
                                if "title=" not in ix:
                                    continue
                                search_spname = re.search("(title=.*)+(>.*)", ix)
                                if search_spname:
                                    spname = search_spname.group()
                                    # 判断是否无效,如果isspnameTrue 为True,则表示无效数据，过滤，如果为False，表示是有效数据
                                    isspnameTrue = False
                                    for ia in invalid_keywords:
                                        if ia in spname and "送" not in spname:
                                            isspnameTrue = True
                                    if isspnameTrue == True:
                                        continue
                                    else:
                                        result_url=TM_url.format(spid)
                                        result = '{"Urlweb":"TM","Urls":"%s","Urlleibie":"%s","spbjpinpai": "",' \
                                                 '"spbjjixing": "",' \
                                                 '"pc": ""}'% (result_url, category)
                                        # 拼写json类型保存至redis
                                        rconnection_test.lpush(redis_key_phone_w, result)
                                else:
                                    print "%s:can not find TM spname,please search regular is valid:%s" % (datetime.now(),url)
                                    wr = ErrorLogsFile(
                                         "can not find TM spname,please search regular is valid:%s" % ( url))
                                    wr.saveerrorlog()
                            else:
                                print "%s:can not find TM id,please search regular is valid" % datetime.now()
                                wr = ErrorLogsFile("can not find TM id,please search regular is valid:%s" % ( url))
                                wr.saveerrorlog()
                else:
                    print "%s:TM url---the first regular is valid ?" % datetime.now()
                    wr = ErrorLogsFile("TM url---the first regular is valid:%s?" % (url))
                    wr.saveerrorlog()
                    time.sleep(5)
                    isValue = True
        except Exception, e:
            isValue = True
            index += 1
            if index == errorCount:
                print "connection redis error: %s , %s" % (index, e)
                wr = ErrorLogsFile("connection redis error: url:%s ,errormessage:%s" % (url,e))
                wr.saveerrorlog()
            time.sleep(5)
        time.sleep(5)


def search_TM_page(urlx, category):
    print urlx
    url = str(urlx).replace("s=0", "s={0}") #"https://list.tmall.com/search_product.htm?cat=50936015&s={0}"
    sesson = requests.session()
    isValue = True
    index = 0
    isvalued =0
    # 先查找页面显示的 页数
    while isValue:
        if errorCount < index:
            print "%s: TM not find this url : %s" % (datetime.now(), url)
            break
        # 随机获取代理ip
        redis_key_proxy = random.choice(proxykeys)
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        print proxiip
        sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        # 随机获取 天猫cookie
        tmcookies = rconnection_test.srandmember(redis_key_tm_cookies)
        # tmcookiejson = json.loads(tmcookies)
        # tmcookie = tmcookiejson["cookie"]
        headers = {
            "User-Agent": "%s" % random.choice(user_agent_list),
            "Accept": "*/*",
            "Referer": "https://www.tmall.com/",
            "Cookie": cookie
        }
        try:
            # time.sleep(10)
            req = sesson.get(url, headers=headers, timeout=30)
            html = req.text
            req.close()
            isValue = False
            if html:
                # print html
                totalye = re.search('(?<=共)\d+(?=页)', str(html))
                # print totalye
                if totalye:
                    print totalye.group()
                    page = totalye.group()
                    print "page %s" % page
                    for int_page in range(0, int(page)):
                        print "intpage: %s" % int_page
                        search_TM(int_page, url, category)
                else:
                    print "search the total page regular isvalid ?"
                    wr = ErrorLogsFile("search the total page regular isvalid ?: url:%s" % (url))
                    wr.saveerrorlog()
                    isValue = True
                    if isvalued == errorCount:
                        return
                    isvalued += 1
                    time.sleep(2)
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

def search_TM_urllib2(urlx, category):
    print urlx
    url = str(urlx).replace("s=0", "s={0}") #"https://list.tmall.com/search_product.htm?cat=50936015&s={0}"
    sesson = requests.session()
    isValue = True
    index = 0
    # 先查找页面显示的 页数
    while isValue:
        if errorCount < index:
            print "%s: TM not find this url : %s" % (datetime.now(), url)
            break
        # 随机获取代理ip
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        print proxiip
        prxyip = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        proxy_s = urllib2.ProxyHandler(prxyip)
        openner = urllib2.build_opener(proxy_s)
        urllib2.install_opener(openner)
        # sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        try:
            cj = cookielib.CookieJar()
            opener = urllib2.build_opener(proxy_s, urllib2.HTTPCookieProcessor(cj))
            urllib2.install_opener(opener)
            resp = urllib2.urlopen(url)
            print cj
            req = urllib2.urlopen(url)
            print "req : %s" % req.read()
            html = req
            # req = sesson.get(url, timeout=30)
            # html = req.text
            # cj = cookielib.CookieJar()
            # opener = urllib2.build_opener(proxy_s,urllib2.HTTPCookieProcessor(cj))
            # urllib2.install_opener(opener)
            # resp = urllib2.urlopen(url)
            # print cj
            # for index, cookie in enumerate(cj):
            #     print '[', index, ']', cookie;
            req.close()
            isValue = False
            if html:
                # print html
                totalye = re.search('(?<=共)\d+(?=页)', str(html))
                # print totalye
                if totalye:
                    print totalye.group()
                    for int_page in range(int(totalye.group())):
                        print "intpage: %s" % int_page
                        search_TM(int_page, url, category)
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

def search_TM_pagetest(urlx, category):
    print urlx
    url = str(urlx).replace("s=0", "s={0}") #"https://list.tmall.com/search_product.htm?cat=50936015&s={0}"
    sesson = requests.session()
    isValue = True
    index = 0
    indexcount = 0
    errorcount_t = 0
    isvalued =0
    # 先查找页面显示的 页数
    while isValue:
        # if errorCount < index:
        #     print "%s: TM not find this url : %s" % (datetime.now(), url)
        #     break
        # 随机获取代理ip
        proxy = rconnection_yz.srandmember(redis_key_proxy)
        proxyjson = json.loads(proxy)
        proxiip = proxyjson["ip"]
        print proxiip
        sesson.proxies = {'http': 'http://' + proxiip, 'https': 'https://' + proxiip}
        # 随机获取 天猫cookie
        tmcookies = rconnection_test.srandmember(redis_key_tm_cookies)
        tmcookiejson = json.loads(tmcookies)
        tmcookie = tmcookiejson["cookie"]
        # print tmcookie
        # if errorcount_t == 6:
        #     url = "https://mdskip.taobao.com/core/initItemDetail.htm?tmallBuySupport=true&sellerPreview=false&service3C=false&cartEnable=true&isRegionLevel=false&isForbidBuyItem=false&isAreaSell=false&queryMemberRight=true&showShopProm=false&offlineShop=false&addressLevel=2&isPurchaseMallPage=false&isSecKill=false&cachedTimestamp=1489628852674&itemId=520649771342&isApparel=false&isUseInventoryCenter=false&tryBeforeBuy=false&household=false&callback=setMdskip&timestamp=1489628853489"
        headers = {
            "User-Agent": 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) App leWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53'
        }
        # headers = {
        #     # "User-Agent": 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) App leWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53'
        #     "Accept": "*/*",
        #     "Referer": "https://www.tmall.com/"
        #     # # ,
        #     # # "Cookie":"t=f1d06870685879a126f29aef70568571; cookie2=6f00a95927be6e9e5aea864ba5a014d2;"
        # }
        try:
            # time.sleep(10)
            req = sesson.get(url, headers=headers, timeout=30)
            html = req.text
            req.close()
            isValue = False
            if html:
                print html
                if "setMdskip" in str(html) and "403 Forbidden" not in str(html):
                    print proxiip
                    print "%s : %s" % (indexcount, html)
                    print "url : %s " % url
                    return
                isValue = True
                indexcount += 1
                if "window.location.href=" in html:
                    errorcount_t += 1
                elif errorcount_t <= 6:
                    errorcount_t = 0
        except Exception, e:
            isValue = True
            index += 1
            print "errormessage: %s" % e
            # if index == errorCount:
            #     print "search total page category error: %s , %s" % (index, e)
            #     wr = ErrorLogsFile("search  total page category error: url:%s,errormessage:%s" % (url, e))
            #     wr.saveerrorlog()
            #     return
            time.sleep(5)


def run():
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
                url = search_TM_url.format(category)
                print "continue: %s" % url
                search_TM_page(url, category)
                # continue
            else:
                search_TM_page(url, category)
        else:
            print "没有找到key, break"
            break


if True:
  # range()
    # print tmcookie
    url = "https://list.tmall.com/search_product.htm?s=0&q=彩电"
    search_TM_page(url, "彩电")

    # times = time.ctime().split(" ")
    # jieguo = times[0] +" "+times[1]+" "+times[2]+" "+times[4]+" "+times[3]+" GMT+0800"
    # url = 'http://mdskip.taobao.com/core/initItemDetail.htm?isPurchaseMallPage=false&service3C=true&addressLevel=4&household=false&itemId=526959556403&cartEnable=true&tryBeforeBuy=false&isForbidBuyItem=false&isUseInventoryCenter=true&isRegionLevel=true&offlineShop=false&isSecKill=false&tmallBuySupport=true&showShopProm=false&queryMemberRight=true&isApparel=false&sellerPreview=false&isAreaSell=true&cachedTimestamp=1489660920387&callback=setMdskip&timestamp='+str(int(time.time() * 1000))+'&isg=ApeXu6aMcj8xGdDzSUd73d3XJhDh3Gs-&isg2=Am1tOJ9DyQf7Ma1rf73YWGXVcAlGcKGcpG-Vw69yqYRzJo3YdxqxbLt1wkA_&async="async"'
    # url = 'http://detail.tmall.com/item.htm?id=43648591887&amp;areaid=&amp;'
    # search_TM_pagetest(url, "电子称")
    # print "begin thread : %s" % datetime.now()
    # threads = []
    # threadcount = 10
    # for ai in range(threadcount):
    #     threads.append(threading.Thread(target=run, args=()))
    # for tx in threads:
    #     tx.start()
    # for tx in threads:
    #     tx.join()
    # print "end thread : %s" % datetime.now()