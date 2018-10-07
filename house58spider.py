#!/usr/bin/env python
# _*_ coding: utf-8 _*_

from time import sleep
import random
from hashlib import sha1
import requests
import re
from bs4 import BeautifulSoup
from lxml import etree
from pymongo import MongoClient
import redis


class IpProxy(object):
    """
    获取并测试免费ip代理
    """
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3510.2 Safari/537.36'
    }
    def __init__(self):
        self.test_url = 'http://www.58.com/ershoufang/changecity/'
        self.timeout = 0.3
        self.proxy_url = 'http://www.xicidaili.com/nn/'

    def get_ip(self):
        """
        从西刺代理网站获取1到4页免费代理
        :return: 免费代理列表
        """
        all_ip = []
        for num in range(1, 4):
            self.proxy_url += str(num)
            sleep(random.uniform(0.1, 0.8))
            res = requests.get(self.proxy_url, headers=self.HEADERS).text
            soup = BeautifulSoup(res, 'lxml')
            ips = soup.find_all('', {'class': 'odd'})
            for ip in ips:
                ip = re.findall(r'<td>(.*)</td>', str(ip))
                all_ip.append(ip[0] + ':' + ip[1])
        return all_ip

    def test_ip(self):
        """
        测试获取的ip代理可用性
        :return: 检测可用的代理
        """
        all_ip = self.get_ip()
        ip_list = []
        for ip in all_ip:
            proxies = {'https': ip}
            n = 1
            while n < 3:
                try:
                    r = requests.get(self.test_url, proxies=proxies, timeout=self.timeout, headers=self.HEADERS)
                    if r.status_code == 200:
                        ip_list.append(ip)
                        break
                except:
                    print('%s测试失败%d次'%(ip,n))
                n += 1
        return ip_list


class SecondHandHouse(object):
    """
    从链接入口获取循环爬取房屋详情
    """
    BASE_URL = 'http://www.58.com/ershoufang/changecity/'
    HEADERS = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': 'id58=c5/njVuRCggin4wQAwPWAg==; 58tj_uuid=26878035-d201-4a89-8b35-a8219cfd8285; als=0; xxzl_deviceid=UE70dJ30hBkZQkEukwxO8FVii8Let7P70MLNueA01kXeLSIyGeytTShyEE47NgWd; new_session=1; new_uv=3; utm_source=; spm=; init_refer=; ppStore_fingerprint=04BBEA9E1DC4355B13653220B3556CD03C76B0B1A4888549%EF%BC%BF1536337400785',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    }
    # IPS = IpProxy().test_ip()  #免费代理ip

    def __init__(self):
        # self.proxies = {'http': 'http://' + random.choice(self.IPS),
        #                 'https': 'https://' + random.choice(self.IPS)}#抓取免费代理创建ip代理,需要时可调用,但可用度不高
        self.html = requests.get(url=self.BASE_URL, headers=self.HEADERS)
        self.req = self.html.text
        self.res = etree.HTML(self.req)
        self.all_city = self.res.xpath('//dl[@id="clist"]')[0]
        self.conn = MongoClient(host='127.0.0.1', port=27017)
        self.db = self.conn['SecondHandHouse']

    def get_html_url(self, url):
        """
        获取待解析对象
        :param url:
        :return: 待解析对象
        """
        response = requests.get(url=url, headers=self.HEADERS)
        html = etree.HTML(response.content.decode())
        return html

    def revise_url(self, url_list):
        """
        构造完整的url
        :param url_list:
        :return:
        """
        list1 = []
        for i in url_list:
            list1.append('http:' + i)
        return list1

    def all_city_url(self):
        """
        获取所有城市url
        :return: 所有城市url
        """
        url_lists = self.all_city.xpath('//dd[2]//a/@href')
        all_urls = self.revise_url(url_lists)
        return all_urls

    def select_city_url(self, pro_name, city_name):
        """
        根据省份和城市获取指定城市url
        :param pro_name: 省份名
        :param city_name: 城市名
        :return: 城市url
        """
        pro_name_lists = ["热门", "山东", "江苏", "浙江", "安徽", "广东", "福建", "广西", "海南", "河南", "湖北", "湖南", "江西", "辽宁", "黑龙江",
                          "吉林", "四川", "云南", "贵州", "西藏", "河北", "山西", "内蒙古", "陕西", "新疆", "甘肃", "宁夏", "青海", "其他"]
        pro_loc_lists = []
        for i in range(1, len(pro_name_lists) + 1):
            pro_loc_lists.append(i)
        pro_name_dict = dict(zip(pro_name_lists, pro_loc_lists))
        num = int(pro_name_dict[pro_name])
        url_list = self.all_city.xpath('//dd[%d]//a/@href' % num)
        url = self.revise_url(url_list)
        place_name = self.all_city.xpath('//dd[%d]//a/text()' % num)
        provice_dict = dict(zip(place_name, url))
        select_city_url = provice_dict[city_name]
        return select_city_url

    def get_detail_url(self, city_url):
        """
        获取房屋详情url
        :param city_url: 城市url
        :return: 房屋详情url列表
        """
        html = self.get_html_url(city_url)
        num = html.xpath('//div[@class="pager"]//text()')[-3]
        detail_urls = []
        detail_url = 'http://{0}.58.com/ershoufang/{1}x.shtml'
        for i in range(1, int(num) + 1):
            new_url = city_url + 'pn' + str(i)
            html = self.get_html_url(new_url)
            house_keys = html.xpath('//ul[@class="house-list-wrap"]//li/@logr')
            for n in range(0, len(house_keys)):
                number = house_keys[n].split('_')[3]
                city_id = city_url.split('/')[2].split('.')[0]
                detail_urls.append(detail_url.format(city_id, number))
        return detail_urls

    def set_map(self, f):
        """
        map()映射函数
        :param f:
        :return:去处头尾空格与换行符
        """
        return f.strip()

    def set_detail(self, details):
        """
        合并房屋描述列表信息
        :param details: 房屋描述列表
        :return: 房屋描述
        """
        detail = ''
        for i in details:
            detail += i
            if i != '':
                detail += '\n'
        return detail

    def get_info(self, detail_url):
        """
        获取房屋详情字段信息
        :param detail_url: 房屋展示url
        :return: 房屋信息
        """
        html = self.get_html_url(detail_url)
        title = html.xpath('//div[@class="house-title"]/h1/text()')[0]  # 标题
        name_url = html.xpath('//*[@id="houseChatEntry"]/div/p[4]/a/@href')[0]
        name = self.get_html_url(name_url).xpath('//div[@class="user-name"]/text()')[0]  # 经纪人姓名
        details = list(map(self.set_map, html.xpath('//*[@id="generalDesc"]//text()')))
        detail = self.set_detail(details)  # 房屋描述
        price = html.xpath('//ul[@class="general-item-left"]/li[1]/span[2]/text()')[0].strip()  # 价格
        house_type = html.xpath('//ul[@class="general-item-left"]/li[2]/span[2]/text()')[0]  # 户型
        area = html.xpath('//ul[@class="general-item-left"]/li[3]/span[2]/text()')[0]  # 面积
        try:
            build_name = html.xpath('//ul[@class="house-basic-item3"]/li[1]/span[2]/a[1]/text()')[0].strip()  # 楼盘
        except IndexError:
            build_name = html.xpath('//ul[@class="house-basic-item3"]/li[1]/span[2]/text()')[0].strip()
        area_location = html.xpath('//ul[@class="house-basic-item3"]/li[2]/span[2]/a[1]/text()')[0].strip()  # 位置
        address = html.xpath('//ul[@class="house-basic-item3"]/li[1]/span[2]/a[2]/text()')[0].replace('－',
                                                                                                      '').strip()  # 地址
        picture = html.xpath('//*[@id="leftImg"]//li/@data-value')  # 图片
        tel = html.xpath('//*[@id="houseChatEntry"]/div/p[3]/text()')[0]  # 电话
        item = {'title': title, 'name': name, 'price': price, 'house_type': house_type, 'area': area,
                'build_name': build_name, 'area_location': area_location, 'address': address,
                'picture': picture, 'tel': tel, 'detail': detail}
        return item


class MongoConnect(object):
    """
    连接mongodb存入数据
    """
    HOST = '127.0.0.1'
    PORT = 27017

    def __init__(self):
        self.conn = MongoClient(host=self.HOST, port=self.PORT)
        self.db = self.conn['SecondHandHouse']

    def error_url(self, url):
        """
        存储获取失败的url到mongodb
        :param url:获取失败的url
        :return:
        """
        url_lists = {'error_url': url}
        self.mongodb_save(url_lists, 'error_urls')

    def mongodb_save(self, item, col_name):
        """
        存储数据
        :param item: 待存储数据
        :param col_name:数据库集合名
        :return:
        """
        collection = self.db[col_name]
        collection.insert(item)


class UrlRedisFilter(object):
    """
    redis存储url,实现去重
    """
    REDIS_SET_NAME = 'tq58'
    REDIS_SET_HOST = '127.0.0.1'
    REDIS_SET_PORT = 6379

    def __init__(self):
        self.redis = redis.StrictRedis(host=self.REDIS_SET_HOST, port=self.REDIS_SET_PORT)
        self.name = self.REDIS_SET_NAME

    @staticmethod
    def encode(string):
        """
        编码非str的数据
        :param string:
        :return:
        """
        if isinstance(string, str):
            return string.encode("utf-8")
        else:
            return string

    def create_fp(self, stru):
        """
        创建指纹
        :param stre: 需创建指纹的url
        :return: 已创建指纹
        """
        sha = sha1()
        sha.update(self.encode(stru))
        fp = sha.hexdigest()
        return fp

    def add_fp(self, stru):
        """
        添加指纹
        :param stre: 需添加指纹的url
        :return:
        """
        fp = self.create_fp(stru)
        self.redis.sadd(self.name, fp)

    def exist(self, stru):
        """
        判断指纹是否存在
        :param stru: url
        :return: True 或者 False
        """
        fp = self.create_fp(stru)
        return self.redis.sismember(self.name, fp)


def main():
    redis_tq = UrlRedisFilter()
    house58 = SecondHandHouse()
    mongo_con = MongoConnect()
    count = 0
    num = 0
    url = house58.select_city_url('四川', '成都')  # 自定义获取指定省份和城市url
    # url_list = house58.all_city_url()  # 需要获取所有数据时调用获取所有城市url
    # for url in url_list:
    detail_urls = house58.get_detail_url(url)  # 获取房屋详情页面url列表
    for detail_url in detail_urls:
        # 判断指纹是否存在
        if redis_tq.exist(detail_url):
            pass
        else:
            try:
                item = house58.get_info(detail_url)
                # 存入字段到自定义命名的MongoDB集合中
                mongo_con.mongodb_save(item, 'chengdu_house')
                # 将获取成功的房屋详情页url添加到redis中
                redis_tq.add_fp(detail_url)
                count += 1
                print('已存入%d条数据,正在保存 %s' % (count, detail_url))
                sleep(random.uniform(0.2, 2))  #随机等待
            except IndexError:
                num += 1
                # print('%d条url未正常获取' % num)
                mongo_con.error_url(detail_url)  #将获取失败的房屋url保存到MongoDB

if __name__ == '__main__':
    main()
