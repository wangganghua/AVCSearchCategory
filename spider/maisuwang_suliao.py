# -*- coding: utf-8 -*-
# 采集买塑网数据,http://www.buyplas.com/site/hqtoday
import datetime
import json
import sys
import time
import requests
import MySQLdb
import threading
import cookielib
import os
import re
import urllib2
import urllib
reload(sys)
sys.setdefaultencoding('utf-8')

# 买塑网
main_url = "http://www.buyplas.com/site/hqtoday/506.htm"
data_url = "http://www.buyplas.com/site/hqtoday/506.htm"

ms_host = "192.168.2.245"
ms_port = 3306
ms_user = "avc"
ms_passwd = "avc"
ms_DB = "areano"
ms_savetable = "suliaodata"    # 存放采集结果

headers = {
    "Host": "www.buyplas.com",
    "Referer": "http://www.buyplas.com/site/hqtoday",
    "Content-Type": "application/x-www-form-urlencoded;",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
    "X-CSRF-Token": "MlYzLUJuU1FiGmQeLSk3OEUsV3V6HWYeextUQw8UOgBaD0BAFzQZaQ==",
    "X-Requested-With": "XMLHttpRequest",
    "Cookie": "_csrf=d5a1a7d4b6e19c996a3a41887e3c3704ef95138ff9045fdf942b900f200d36f2a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22PLW3oGdiwzdX8s5OIMgnMziQhYsmUZJ8%22%3B%7D"
}


def searchweather(url, cityid):
    # 读取token
    fw = open("token.txt", "r")
    tokens = fw.readline()
    fw.close()
    headers = {
        "Referer": "http://www.buyplas.com/site/hqtoday",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
        "X-CSRF-Token": tokens,
        "X-Requested-With": "XMLHttpRequest"
    }
    gethtml = get_html(url, headers=headers)

    if(gethtml):
        gethtml = gethtml.replace("var fc40 =", "")
        try:
            getjson = json.loads(gethtml)
        except Exception, e:
            print "get json data error!!!"
            print e.message
            fw = open("wghjson.txt", "a")
            fw.writelines("get json data error: %s\r\n" % url)
            fw.close()
    else:
        print "no data ,check your url is valid ?"
        fw = open("wgh.txt", "a")
        fw.writelines("no data city id: %s" % cityid)
        fw.writelines("json :%s" % gethtml)
        fw.close()


def searchweather_test(url):
    # get_token(main_url)
    # get_cookie(main_url)
    # 读取token
    fw = open("token.txt", "r")
    tokens = fw.readline()
    fw.close()
    # headers = {
    #     "Referer": "http://www.buyplas.com/site/hqtoday",
    #     "Content-Type": "application/x-www-form-urlencoded;",
    #     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
    #     "X-CSRF-Token": "MlYzLUJuU1FiGmQeLSk3OEUsV3V6HWYeextUQw8UOgBaD0BAFzQZaQ==",
    #     "X-UA-Compatible": "IE=edge,chrome=1",
    #     "X-Requested-With": "XMLHttpRequest",
    #     "Cookie": "_csrf=d5a1a7d4b6e19c996a3a41887e3c3704ef95138ff9045fdf942b900f200d36f2a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22PLW3oGdiwzdX8s5OIMgnMziQhYsmUZJ8%22%3B%7D"
    # }

    #   获取主页需要采集的列表
    gethtml = get_html(main_url)
    if gethtml:
        #   查找key
        keylist = re.findall('<li  key="[0-9]+"[\s\S]+?</li>', gethtml)
        if keylist:
            for i in keylist:
                keyid = re.search('(?<=key=")[0-9]+(?=")', i).group()
                title = re.search('(?<=data-title=")[\s\S]+?(?=">)', i).group()
                post_data = urllib.urlencode({
                    "pid": keyid,
                    "end": "2017-08-18",
                    "start": "2017-07-18"
                })
                print keyid
                print title
                posthtml = post_html(data_url, headers, post_data)
                if(posthtml):
                    print json.loads(posthtml)["data"][0]
                    print json.loads(posthtml)["data"][1]
                    valuelist = []
                    index = 0
                    for dt in json.loads(posthtml)["data"][0]:
                        category = str(title.split("|")[0]).strip()
                        model = str(title.split("|")[1]).strip()
                        company = str(str(title.split("|")[2]).strip()).replace("（14）", "")
                        strdate = dt
                        price = json.loads(posthtml)["data"][1][index]
                        writetime = datetime.datetime.now()
                        index += 1
                        valuelist.append((category, model, company, strdate, price, writetime))
                    strsql = "INSERT INTO {0}(category , model, company, strdate, price, writetime) VALUES (%s,%s, %s,%s, %s,%s)".format(
                        ms_savetable)
                    save_mysql(strsql, valuelist)
                    valuelist = []
                else:
                    print "no data ,check your url is valid ?"
                    fw = open("wgh.txt", "a")
                    fw.writelines("no data city id: %s")
                    fw.writelines("json :%s" % posthtml)
                    fw.close()
    else:
        print "no url list ,check your url is valid ?"
        fw = open("wgh.txt", "a")
        fw.writelines("no url list!!")
        fw.writelines("json :%s" % gethtml)
        fw.close()


def get_html(urls, headers=None, charset='utf8'):
    html = ''
    for i in range(0, 5):
        try:
            if headers is not None:
                res = requests.get(urls, headers=headers)
            else:
                res = requests.get(urls, timeout=10)
            if charset == 'utf8':
                html = str(res.content).encode('utf8')
            else:
                html = str(res.content).decode('gbk').encode('utf-8')
            if html != '':
                break
        except:
            time.sleep(5)
    return html


def post_html(urls, headers=None, data=None, charset='utf8'):
    html = ''
    for i in range(0, 5):
        try:
            if headers is not None:
                req = requests.post(url=urls, data=data, headers=headers)
            else:
                req = requests.post(url=urls, data=data)
            if charset == 'utf8':
                html = str(req.text).encode('utf8')
            else:
                html = str(req.text).decode('gbk').encode('utf-8')
            if html != '':
                break
        except:
            time.sleep(5)
    return html


#  获取网页表头的：token
def get_token(url):
    htmls = get_html(url)
    time.sleep(2)
    if htmls:
        fx = open("wgh", "w")
        fx.writelines(htmls)
        fx.close()
        token = re.search('(?<=<meta name="csrf-token")[\s\S]+?(?=">)', htmls)
        if token:
            print token.group()
            token = str(token.group()).replace(' content="', "")
            fw = open("token.txt", "w")
            fw.writelines(token)
            fw.close()
        else:
            print "haven't find token !!!"
    else:
        print "haven't find token--main!"


def get_cookie(url):
    headers = {
        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        # "Accept-Encoding": "gzip, deflate",
        # "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        # "Cache-Control": "max-age=0",
        # "Connection": "keep-alive",
        # "Host": "www.buyplas.com",
        # "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0"
    }
    cj = cookielib.LWPCookieJar()
    cookie_support = urllib2.HTTPCookieProcessor(cj)
    opener = urllib2.build_opener(cookie_support, urllib2.HTTPHandler)
    urllib2.install_opener(opener)
    requestx = urllib2.Request(url, headers=headers)
    urllib2.urlopen(requestx)
    cookie = ''
    print cj
    for ck in cj:
        cookie = cookie + str(ck).split(' ')[1] + ";"
    print cookie


def select_mysql(sql_str):
    isTrue = True
    while isTrue:
        try:
            conn = MySQLdb.connect(
                host=ms_host,
                port=ms_port,
                user=ms_user,
                passwd=ms_passwd,
                db=ms_DB,
                charset='utf8',
            )
            cur = conn.cursor()
            cur.execute(sql_str)
            return_data = cur.fetchall()
            cur.close()
            conn.close()
            isTrue = False
            return return_data
        except MySQLdb.Error, e:
            print str(e)
            print "select_mysql error:%s" % e.message
            if "Can't connect to MySQL server on" in str(e):
                isTrue = True
                time.sleep(1)
            else:
                print "eeeeee"
                return []


def save_mysql(sql, values):
    isTrue = True
    while isTrue:
        try:
            conn = MySQLdb.connect(
                host=ms_host,
                port=ms_port,
                user=ms_user,
                passwd=ms_passwd,
                db=ms_DB,
                charset='utf8')
            cur = conn.cursor()
            try:
                cur.executemany(sql, values)
                conn.commit()
                isTrue = False
                print "access insert into mysql"
            except MySQLdb.Error, e:
                if "Can't connect to MySQL server on" in str(e):
                    isTrue = True
                    time.sleep(1)
                print "insert into mysql error!!"
            cur.close()
            conn.close()
        except Exception, e:
            if "Can't connect to MySQL server on" in str(e):
                isTrue = True
                time.sleep(1)
            print "save_mysql error:%s" % e.message


def run():
    global cityurlid
    print "begin select city id no. :%s" % datetime.datetime.now()
    #    年、月简写：201708
    yearmonth = str(str(datetime.date.today())[0:7]).replace("-", "")
    #   时间戳
    timenode = int(time.time() * 1000)
    #   下次采集时间,因为天气网只有 最多15天的数据，故 每15天 采集一次
    nexttime = datetime.date.today() + datetime.timedelta(days=15)
    #   年度
    year = datetime.date.today().year
    if cityurlid:
        url_list = str(cityurlid.pop())
        new_urls = new_url.format(year, url_list, yearmonth, timenode)
        print new_urls
        searchweather(new_urls, url_list)
    else:
        print "end "


if __name__ == "__main__":
    wgh = "20170730"
    print str(wgh[0:4])+'-'+str(wgh[4:6])+'-'+str(wgh[6:8])
    print wgh[4:6]
    # searchweather_test(data_url)

