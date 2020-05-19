import pandas as pd
import numpy as np
import pymysql
import urllib.request
from bs4 import BeautifulSoup
from tqdm import tqdm


class Crawler:
    def __init__(self, url):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
        }
        self.request = urllib.request.Request(url=url, headers=self.headers)
        self.response = urllib.request.urlopen(self.request).read()
        self.html = self.response.decode()
        self.soup = BeautifulSoup(self.html, 'lxml')
        self.title = ''
        self.text = ''

    def get_title(self):
        try:
            self.title = self.soup.h2.text.replace('\n', '').replace(' ', '')
        except:
            self.title = 'Missing Value'
        return self.title

    def get_text(self):
        try:
            all_text = self.soup.find_all('span')
            for item in all_text:
                self.text = self.text + item.text
            self.text = self.text.replace('\n', '').replace(' ', '')
        except:
            self.text = 'Missing Value'
        return self.text


class DatabaseIO:
    def __init__(self, host='localhost', port=3306, db='', user='root', password='root', charset='utf8'):
        # 建立连接
        self.conn = pymysql.connect(
            host=host, port=port, db=db, user=user, password=password, charset=charset)
        # 创建游标
        self.cur = self.conn.cursor()
    
    def __enter__(self):
        # 返回游标        
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 提交数据库并执行        
        self.conn.commit()
        # 关闭游标        
        self.cur.close()
        # 关闭数据库连接        
        self.conn.close()
