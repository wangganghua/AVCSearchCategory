# encoding:utf8

import redis
import json
import time
from datetime import datetime
from lxml import etree
from selenium import webdriver
from HTMLParser import HTMLParser
from PIL import ImageGrab
# 2017-02-09 17:48:23.869000
redis_key_proxy = "proxy:iplist2"
# rconnection_test = redis.Redis(host='192.168.2.245', port=6379, db=0, charset="utf-8")
rconnection_test = redis.Redis(host='117.122.192.50', port=6479, db=0, charset="utf-8")
rconnection_yz = redis.Redis(host='117.122.192.50', port=6479, db=0)
# rconnection_test = redis.Redis(host='1.119.7.234', port=26479, db=0, charset="utf-8")


def texd():
    # url = "https://detail.tmall.com/item.htm?id=530268236010&ns=1&abbucket=4"
    url = " http://item.jd.com/10675542083.html"

    # driver = webdriver.PhantomJS()
    proxy = rconnection_yz.srandmember(redis_key_proxy)
    proxyjson = json.loads(proxy)
    proxiip = proxyjson["ip"]
    print proxiip

    driver = webdriver.PhantomJS(service_args=[
                            '--proxy=' + proxiip,
                            '--proxy-type=http'
                        ])
    try:
        print "get url"
        driver.get(url)
        # time.sleep(3)
        print "get url end"
        body = driver.page_source
        print "huoq div"
        driver.find_element_by_xpath('//div[@id="stock-address"]').click()
        time.sleep(5)
        print "drive click"
        # print "dl"
        # driver.find_element_by_xpath('//div[@data-level="0"]')
        # print "end dl"
        # //*[@id="stock-address"]/div/div[2]/dl[2]/dd/div/div/div[1]/li[1]/a
        xpth1 = "//a[contains(.,'北京')]" # 省份data-value="1"
        xpth2 = '//*[@id="stock-address"]/div/div[2]/dl[2]/dd/div/div/div[2]/li[1]/a' # 地市
        xpth3 = '//*[@id="stock-address"]/div/div[2]/dl[2]/dd/div/div/div[3]/li[1]/a' # 地区
        # xpth =  '//div[@data-level="1"]/li[1]' # 地市
        # xpth = ''
        print xpth1
        print driver.find_element_by_xpath(xpth1).text
        print "success"
    except Exception,e:
        print e
    driver.quit()

def retext1():
    wx = open("F:\wgh.txt")
    for i in wx:
        print i.decode("gbk")
        rconnection_test.lpush("price_test_time:start_urls", i.decode("gbk"))
    print "end"

    # 手机
def retext():
    wx = open("F:\/brandmodel.txt")
    for i in wx:
        print i.decode("gbk")
        rconnection_test.lpush("dpc_phone_url:item_keywords", i.decode("gbk"))
    print "end"
def retext2():
    wx = open("F:\collect_url.txt")
    for i in wx:
        print i

def recategory_tm():
    wx = open("F:\/searchcategorydata.txt")
    for i in wx:
        print i.decode("gbk")
        rconnection_test.lpush("dpc_category_url:item_keywords_tm", i.decode("gbk"))
    print "end"
# texd()
# retext()
# recategory()
def recategory_cuntao():
    wx = open("F:\/cuntaowang.txt")
    for i in wx:
        print i.decode("gbk")
        rconnection_test.lpush("dpc_category_url:item_keywords_CHUNTAO", i.decode("gbk"))
    print "end"
    wx.close()

def retxd():
    wx = open("F:\/wz1.txt", "r")
    xw = ''
    for i in wx:
        xw = xw + i
        # rconnection_test.lpush("dpc_category_url:item_keywords_CHUNTAO", i.decode("gbk"))
    print "end"
    # print xw
    root = etree.HTML(xw)
    print root
    root1 = etree.fromstring(xw)
    wx.close()

# retxd()
