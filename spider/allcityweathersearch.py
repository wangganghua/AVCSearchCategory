# -*- coding: utf-8 -*-

import datetime
import json
import sys
import time
import requests
import MySQLdb
import threading
reload(sys)
sys.setdefaultencoding('utf-8')

# 中国天气网
# url_weather_html = 'http://www.weather.com.cn/weather40d/%s.shtml'
# url_weather_html = 'http://www.weather.com.cn/weather40d/101010400.shtml'
url_weather_html = "http://d1.weather.com.cn/calendar_new/2017/101010400_201708.html?_=1501828919025"
new_url = "http://d1.weather.com.cn/calendar_new/{0}/{1}_{2}.html?_={3}"

ms_host = "192.168.2.245"
ms_port = 3306
ms_user = "avc"
ms_passwd = "avc"
ms_DB = "areano"
ms_selecttable = "weather_areaid1"  # 查找地区id 表
ms_savetable = "weatherdata"    # 存放采集结果


def searchweather(url, cityid):
    headers = {
        "Referer": "http://www.weather.com.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0"
    }
    gethtml = get_html(url, headers=headers)

    if(gethtml):
        gethtml = gethtml.replace("var fc40 =", "")
        try:
            getjson = json.loads(gethtml)
            valuelist = []
            for i in getjson:
                areaid = cityid
                weather = i["w1"]
                if weather == "":   # 天气情况为空，暂时不入库
                    continue
                wind = i["wd1"]
                maxw = i["max"]
                if maxw == "":
                    continue
                minw = i["min"]
                hmaxw = i["hmax"]
                hminw = i["hmin"]
                jsgl = i["hgl"]
                #strdate = i["date"]
                strdate = str(i["date"][0:4])+'-'+str(i["date"][4:6])+'-'+str(i["date"][6:8])
                writetime = datetime.datetime.now()
                valuelist.append((areaid, weather, wind, maxw, minw, hmaxw, hminw, jsgl, strdate, writetime))
            strsql = "INSERT INTO {0}(areaid, weather, wind, maxw, minw, hmaxw, hminw, jsgl,strdate, writetime) VALUES (%s,%s, %s,%s, %s,%s, %s,%s, %s,%s)".format(ms_savetable)
            save_mysql(strsql, valuelist)
            valuelist = []
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


def selectcityId():
    firsttime = datetime.datetime.now()
    strsql = "SELECT AREA_ID FROM {0}".format(ms_selecttable)
    returnid = select_mysql(strsql)
    global cityurlid
    cityurlid = []
    if returnid:
        for i in returnid:
            cityurlid.append(i[0])
        isTrue = True
        while isTrue:
            if(len(cityurlid)>0):
                threads = []
                threadcount = 10
                for ai in range(threadcount):
                    threads.append(threading.Thread(target=run, args=()))
                for tx in threads:
                    tx.start()
                for tx in threads:
                    tx.join()
                isTrue = True
            else:
                isTrue = False
                print "end --total time--:%s" % (datetime.datetime.now()-firsttime)

    else:
        print "select city id failed!!!"


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
    # nexttime = datetime.date.today() + datetime.timedelta(days=15)
    # print nexttime
    while True:
        start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if "09:00:00" in start:
            selectcityId()
        else:
            time.sleep(1)
            print datetime.datetime.now()

