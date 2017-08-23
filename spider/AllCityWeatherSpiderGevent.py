# -*- coding: utf-8 -*-
"""
Python 2.7.5
yum install python-devel mysql-community-devel mysql-devel MySQL-python -y
pip install mysql-python
pip install requests
pip install gevent
pip install pykafka
nohup python -u AllCityWeatherSpiderGevent.py >> spider.log 2>&1 &
* */1 * * * python -u RateBOCSpider.py  >> WeatherSpider.log # 每小时采集一次
"""
from datetime import datetime
import json
import logging
import re
import sys
import time

import MySQLdb
import requests
# import queue
# import spawn
from gevent import queue, spawn

reload(sys)
sys.setdefaultencoding('utf-8')


class AllCityWeatherSpiderGevent:
    def __init__(self, max_running=20, save_count=200):
        self.save_count = save_count
        self.max_running = max_running
        self.result = []
        self.time_format = '%Y-%m-%d %H:%M:%S'
        self.create_time = time.strftime(self.time_format, time.localtime())
        self.citylist = []
        self.weather_img_position_dict = {}
        self.headers = {
            'Referer': 'http://www.weather.com.cn/'
        }
        self.url_weather_sk = 'http://d1.weather.com.cn/sk_2d/%s.html?_=%d'
        self.url_weather_html = 'http://www.weather.com.cn/weather1d/%s.shtml'
        self.url_weather_warn = 'http://d1.weather.com.cn/dingzhi/%s.html?_=%d'
        self.url_tianqi_html = 'http://%s.tianqi.com/%s/today/'
        # mysql
        self.ms_host = '192.168.2.245'
        self.ms_port = 3306
        self.ms_user = 'avc'
        self.ms_passwd = 'avc'
        self.ms_DB = 'areano'
        self.ms_table = 'weather_areaid'
        self.ms_select_table = "weather_areaid1"

        # regex - weather img position
        self.reg_weather_img = 'big.(?P<code>[^{]*?){background-position:(?P<position>[^{]*?)}'
        # regex - sk_2d
        self.reg_weather = '(?<="weather":").*?(?=")'
        self.reg_weather_code = '(?<="weathercode":").*?(?=")'
        self.reg_temp = '(?<=temp":").*?(?=")'
        self.reg_wind_direction = '(?<="WD":").*?(?=")'
        self.reg_wind_power = '"WS":"(?P<ws>.*?)","wse":"(?P<wse>.*?)"'
        self.reg_wind_time = '(?<="time":").*?(?=")'
        # regex - other
        self.reg_temperature = '<span>(?P<dd>-*\d+)</span><em>°C</em>'
        self.reg_hour_list_block = '(?<=var hour3data=).*'
        self.reg_desc_block = '<div class="livezs">[\s\S]*?条形广告位begin'
        self.reg_desc = '<span>(?P<level>.*?)</span>[^<]*?<em>(?P<name>.*?)</em>[^<]*?<p>(?P<desc>.*?)</p>'
        self.reg_warn_desc = 'var alarm.*?=(?P<aa>.*?]})'
        self.reg_weather_html = 'id="hidden_title" value=".*?日(?P<sj>\d+)时(?P<weatehr>.*?)"'

        logging.basicConfig(level=logging.WARN,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            datefmt=self.time_format,
                            # filename='myapp.log',
                            # filemode='w'
                            )
        self.logger = logging.getLogger(__name__)

        # 获取城市列表
        self.get_global_data()
        self.logger.debug('共获取城市列表 %d 个' % len(self.citylist))
        # 待采集队列
        self.city_pendings = queue.Queue(-1)
        self.items_queue = queue.Queue(-1)
        for each_city in self.citylist:
            self.city_pendings.put(each_city)
            self.logger.debug('开始采集...')
        spawn(self.start_crawl).join()

    def get_global_data(self):
        # sql_str = "SELECT * FROM weather_areaid WHERE area_id='101010100'"
        sql_str = "SELECT * FROM %s" % self.ms_select_table
        self.citylist = self.select_mysql(self.ms_host, self.ms_port, self.ms_user, self.ms_passwd, self.ms_DB, sql_str)
        # sql_str = 'SELECT * FROM weather_code WHERE is_enable=1'
        # wc_list = self.select_mysql(self.ms_host, self.ms_port, self.ms_user, self.ms_passwd, self.ms_DB, sql_str)
        # for wc in wc_list:
        #     self.weather_code_dict[str(wc[1]).encode('utf-8')] = wc[0]

        # 获取weather img 的坐标
        html = self.get_html('http://www.weather.com.cn/weather1d/101010100.shtml#search')
        #
        weather_img_reg = re.findall(self.reg_weather_img, html)
        for info in weather_img_reg:
            self.weather_img_position_dict[str(info[0])] = info[1]

    def start_crawl(self):
        runnings = 0
        while runnings > 0 or not self.city_pendings.empty():
            try:
                while runnings < self.max_running:
                    cityinfo = self.city_pendings.get_nowait()
                    spawn(self.get_one_city, cityinfo)
                    runnings += 1
            except queue.Empty:
                pass
            iteminfo = self.items_queue.get()
            runnings -= 1
            self.handle_item(iteminfo)
        self.handle_item(item=None, islast=True)

    def get_one_city(self, cityinfo):
        self.logger.debug('crawl %s' % cityinfo[2])
        iteminfo = WeatherInfo.item
        iteminfo['area_id'] = cityinfo[0]
        iteminfo['area'] = cityinfo[2]
        skinfo = self.get_weather_sk_city(cityinfo)
        iteminfo['weather'] = skinfo[0]
        iteminfo['wind_direction'] = skinfo[1]
        iteminfo['wind_power'] = skinfo[2]
        iteminfo['time'] = skinfo[3]
        iteminfo['temperature'] = skinfo[4]
        iteminfo['weather_code'] = skinfo[5]
        weather_flag = iteminfo['weather'] == ''
        otherinfp = self.get_weather_html_city(cityinfo, weather_flag)
        iteminfo['temperature_high'] = otherinfp[0]
        iteminfo['temperature_low'] = otherinfp[1]
        iteminfo['desc'] = otherinfp[2]
        iteminfo['hour_list'] = otherinfp[3]
        if weather_flag:
            iteminfo['weather'] = otherinfp[4]
            iteminfo['time'] = otherinfp[5]
        iteminfo['warn_desc'] = self.get_weather_warn_city(cityinfo)
        iteminfo['create_time'] = self.create_time

        # TODO 如果中国气象局采集不到数据 则采集天气网数据
        # TODO 发送告警邮件
        # 如果中国气象局采集不到数据 则采集天气网数据
        # if iteminfo['weather'] == '':
        #     tianqi_info = self.get_weather_tianqi_city(cityinfo)
        #     # weather, wind_direction, wind_power, temperature_high, temperature_low, desc, wind_time
        #     iteminfo['weather'] = tianqi_info[0]
        #     iteminfo['wind_direction'] = tianqi_info[1] if iteminfo['wind_direction'] == ''  else iteminfo[
        #         'wind_direction']
        #     iteminfo['wind_power'] = tianqi_info[2] if iteminfo['wind_power'] == ''  else iteminfo['wind_power']
        #     iteminfo['temperature_high'] = tianqi_info[3] if iteminfo['temperature_high'] == ''  else iteminfo[
        #         'temperature_high']
        #     iteminfo['temperature_low'] = tianqi_info[4] if iteminfo['temperature_low'] == ''  else iteminfo[
        #         'temperature_low']
        #     iteminfo['desc'] = tianqi_info[5] if iteminfo['desc'] == ''  else iteminfo['desc']
        #     iteminfo['wind_time'] = tianqi_info[6] if iteminfo['wind_time'] == ''  else iteminfo['wind_time']
        #     iteminfo['temperature'] = tianqi_info[7] if iteminfo['temperature'] == ''  else iteminfo['temperature']
        iteminfo['weather_image'] = self.weather_img_position_dict.get(str(iteminfo['weather_code']), '')
        self.items_queue.put(iteminfo.copy())

    def get_weather_sk_city(self, cityinfo, trycnt=5):
        weather, wind_direction, wind_power, wind_time, temperature, weather_code = ('', '', '', '', '', '')
        urls = self.url_weather_sk % (cityinfo[0], time.time() * 1000)
        self.logger.debug('crawl urls %s' % urls)
        html = self.get_html(urls, headers=self.headers)
        for i in range(trycnt):
            if '<H2>您所请求的网址（URL）无法获取</H2>' in html:
                time.sleep(1)
                urls = self.url_weather_sk % (cityinfo[0], time.time() * 1000)
                html = self.get_html(urls, headers=self.headers)
            else:
                break
        self.logger.debug('download html.len %d' % len(html))
        # self.logger.debug('html: %s' % html)
        if '<BR><h1>拒绝访问</h1>' in html:
            self.logger.warning('拒绝访问 % s' % urls)
        weather_reg = re.search(self.reg_weather, html)
        if weather_reg:
            weather = weather_reg.group(0)
        weather_code_reg = re.search(self.reg_weather_code, html)
        if weather_code_reg:
            weather_code = weather_code_reg.group(0)
        temp_reg = re.search(self.reg_temp, html)
        if temp_reg:
            temperature = temp_reg.group(0)
        wind_direction_reg = re.search(self.reg_wind_direction, html)
        if wind_direction_reg:
            wind_direction = wind_direction_reg.group(0)
        wind_power_reg = re.search(self.reg_wind_power, html)
        if wind_power_reg:
            wind_power = wind_power_reg.group('ws') + ' ' + wind_power_reg.group('wse')
            wind_power = wind_power.replace('&lt;', '')
        wind_time_reg = re.search(self.reg_wind_time, html)
        if wind_time_reg:
            wind_time = wind_time_reg.group(0)
        return weather, wind_direction, wind_power, wind_time, temperature, weather_code

    def get_weather_html_city(self, cityinfo, weather_flag=False):
        temperature_high, temperature_low, desc, hour_list, weather, wind_time = ('', '', '', '', '', '')
        urls = self.url_weather_html % cityinfo[0]
        self.logger.debug('crawl urls %s' % urls)
        html = self.get_html(urls)
        self.logger.debug('download html.len %d' % len(html))
        temperature_reg = re.findall(self.reg_temperature, html)
        if len(temperature_reg) >= 2:
            temperature_high = temperature_reg[0]
            temperature_low = temperature_reg[1]
        desc_block_reg = re.search(self.reg_desc_block, html)
        if desc_block_reg:
            desc_block = desc_block_reg.group(0)
            desc_reg = re.findall(self.reg_desc, desc_block)
            for each in desc_reg:
                desc += each[1] + ':' + each[0] + ',' + each[2] + ';'
        hour_list_block_reg = re.search(self.reg_hour_list_block, html)
        if hour_list_block_reg:
            hour_list_block = hour_list_block_reg.group(0)
            hour_list_json = json.loads(hour_list_block)
            d1 = hour_list_json.get('1d', [])
            for each in d1:
                each_list = each.split(',')
                if len(each_list) >= 6:
                    item_hour_json = WeatherInfo.item_hour
                    item_hour_json['time'] = each_list[0]
                    item_hour_json['weather_code'] = each_list[1]
                    item_hour_json['weather'] = each_list[2]
                    item_hour_json['temperature'] = each_list[3]
                    item_hour_json['wind_direction'] = each_list[4]
                    item_hour_json['wind_power'] = each_list[5]
                    hour_list += json.dumps(item_hour_json) + ','
                else:
                    self.logger.error('ther hour_list.len<6 %s' % each)
        if hour_list != '':
            hour_list = hour_list.decode("unicode_escape").encode('utf-8')
            hour_list = '[' + hour_list[0:len(hour_list) - 1] + ']'
            hour_list = hour_list.replace('weather_code', 'weatherCode')
            hour_list = hour_list.replace('wind_direction', 'windDirection').replace('wind_power', 'windPower')
        # 如果weather_flag=True 在html中获取天气信息
        if weather_flag:
            weather_html_reg = re.search(self.reg_weather_html, html)
            if weather_html_reg:
                wind_time = weather_html_reg.group('sj') + ':00'
                weather_list = weather_html_reg.group('weatehr').strip().split(' ')
                while '' in weather_list:
                    weather_list.remove('')
                weather = weather_list[1]

        return temperature_high, temperature_low, desc, hour_list, weather, wind_time

    def get_weather_warn_city(self, cityinfo):
        warn_desc = ''
        urls = self.url_weather_warn % (cityinfo[0], time.time() * 1000)
        self.logger.debug('crawl urls %s' % urls)
        html = self.get_html(urls, headers=self.headers)
        self.logger.debug('download html.len %d' % len(html))
        if '<BR><h1>拒绝访问</h1>' in html:
            self.logger.warning('拒绝访问 %s' % urls)
        warn_desc_reg = re.search(self.reg_warn_desc, html)
        if warn_desc_reg:
            warn_desc_block = warn_desc_reg.group('aa')
            warn_desc_json = json.loads(warn_desc_block).get('w', '')
            for each in warn_desc_json:
                warn_desc += each.get('w5', '') + each.get('w7', '') + '预警;'
        return warn_desc

    def get_weather_tianqi_city(self, cityinfo):
        weather, wind_direction, wind_power, desc, temperature = ('', '', '', '', '')
        temperature_high, temperature_low, wind_time = ('', '', '08:00')
        urls = self.url_tianqi_html % (cityinfo[3], cityinfo[1])
        self.logger.debug('[tianqi] crawl urls %s' % urls)
        html = self.get_html(urls)
        reg_temp = re.search('(?<=rettemp"><strong>).*?(?=&)', html)
        if reg_temp:
            temperature = reg_temp.group(0)
        reg_temperature = re.search('id="t_temp">[^>]*?>(?P<temp_h>.*?)℃.*?">(?P<temp_l>.*?)℃', html)
        if reg_temperature:
            temperature_high = reg_temperature.group('temp_h')
            temperature_low = reg_temperature.group('temp_l')
        reg_weather = re.search('cDRed">(?P<dd>.*?)<', html)
        if reg_weather:
            weather = reg_weather.group('dd')
        reg_wind = re.search('cDRed">.*?>[^>]*?">(?P<wind_d>.*?) (?P<wind_p>.*?)<', html)
        if reg_wind:
            wind_direction = reg_wind.group('wind_d')
            wind_power = reg_wind.group('wind_p')
        reg_desc_list = re.findall('<li class="shzsbox.*?>(?P<name>.*?)：.*?>(?P<level>.*?)<.*?">(?P<desc>.*?)<', html)
        for each in reg_desc_list:
            desc += each[0] + ':' + each[1] + ',' + each[2] + ';'

        return weather, wind_direction, wind_power, temperature_high, temperature_low, desc, wind_time, temperature

    def get_html(self, urls, headers=None, charset='utf8'):
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

    def handle_item(self, item=None, islast=False):
        if item is not None:
            self.result.append(item.copy())

        # 保存到mysql
        if len(self.result) >= self.save_count or islast:
            sql_list = []
            print "%sbegin insert into mysql" % datetime.now()
            index = 1
            for each in self.result:
                print each.get("area")
                print index
                sql_str = "INSERT INTO " + self.ms_table + " (`area_id`,`area`,`weather_code`,`weather`,`wind_direction`,`wind_power`,`temperature_high`,`temperature_low`,`hour_list`,`desc`,`warn_desc`,`time`,`create_time`,`temperature`,`weather_image`) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    each.get('area_id'), each.get('area'), each.get('weather_code'), each.get('weather'),
                    each.get('wind_direction'), each.get('wind_power'), each.get('temperature_high'),
                    each.get('temperature_low'), each.get('hour_list'), each.get('desc'), each.get('warn_desc'),
                    each.get('time'), each.get('create_time'), each.get('temperature'), each.get('weather_image'))
                sql_list.append(sql_str)
                index += 1
            self.save_mysql(self.ms_host, self.ms_port, self.ms_user, self.ms_passwd, self.ms_DB, sql_list)
            self.result = []
            print "%send insert into mysql" % datetime.now()

    def print_dict(self, dict_temp):
        for each in dict_temp.keys():
            if dict_temp.get(each) != '':
                print each, '=', dict_temp.get(each)

    def select_mysql(self, ms_host, ms_port, ms_user, ms_passwd, ms_DB, sql_str):
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
            return return_data

        except Exception, e:
            self.logger.warning('select_mysql error:%s' % e.message)
            return []

    def save_mysql(self, ms_host, ms_port, ms_user, ms_passwd, ms_DB, sql_list):
        try:
            conn = MySQLdb.connect(
                host=ms_host,
                port=ms_port,
                user=ms_user,
                passwd=ms_passwd,
                db=ms_DB,
                charset='utf8'
            )
            cur = conn.cursor()
            for sql_str in sql_list:
                cur.execute(sql_str)
                conn.commit()
            cur.close()
            conn.close()
        except Exception, e:
            print e
            self.logger.warning('save_mysql error:%s' % e.message)


class WeatherInfo:
    item = {
        'area_id': '',  # 城市ID
        'area': '',  # 城市名称
        'weather_code': '',  # 天气编号
        'weather': '',  # 天气
        'weather_image': '',  # 天气img像素位置
        'wind_direction': '',  # 风向
        'wind_power': '',  # 风力
        'temperature': '',  # 当前温度
        'temperature_high': '',  # 最高温度
        'temperature_low': '',  # 最低温度
        'hour_list': '',  # 24小时天气，数据分装成json格式
        'desc': '',  # 天气描述
        'warn_desc': '',  # 预警信息
        'time': '',  # 气象发布时间
        'create_time': ''  # 采集时间
    }

    item_hour = {
        'weather_code': '',  # 天气编码
        'time': '',  # 预报时间
        'wind_direction': '',  # 风向
        'wind_power': '',  # 风力
        'weather': '',  # 天气名称
        'temperature': '',  # 温度
    }


if __name__ == '__main__':
    spider = AllCityWeatherSpiderGevent(max_running=20, save_count=200)
