from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
import pymysql
from tqdm import tqdm


class OverallHrefs:
    def __init__(self, region, realm, level, ids):
        super().__init__()
        self._url = 'http://www.compassedu.hk/offer_' + \
            str(region) + '_' + str(realm) + '_' + str(level) + '_0'
        self._regions = {
            12: '美国',
            11: '香港',
            7: '英国',
            10: '新加坡',
            9: '澳大利亚'
        }
        self._realms = {
            6: '商科',
            1: '工科',
            5: '理科',
            4: '社科'
        }
        self._levels = {
            1: '985',
            2: '211',
            3: '普本',
            4: '海本'
        }
        self._region = self._regions[region]
        self._realm = self._realms[realm]
        self._level = self._levels[level]
        self._ids = ids
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        self._driver = webdriver.Chrome(options=option)
        self._driver.get(self._url)
        self._db = pymysql.connect(
            'localhost', 'root', 'password', 'zhinanzhe')
    
    def get_ids(self):
        return self._ids

    def scroll(self):
        while True:
            try:
                self._driver.find_element_by_css_selector('body > div.pad-con > div > div.succs-list > div.loadfailed').click()
            except:
                if self._driver.find_element_by_css_selector('body > div.pad-con > div > div.succs-list > div.scroll-line.loading.lll').text == '':
                    break
                self._driver.execute_script('window.scrollBy(0, 8000)')
                time.sleep(0.1)
        # print('Done!')
        return True

    def to_database(self, id_, url_):
        cursor = self._db.cursor()
        sql = "INSERT INTO Pre (id, region, realm, level, url) VALUES (%s, '%s', '%s', '%s', '%s')" % (
            id_, self._region, self._realm, self._level, url_)
        try:
            cursor.execute(sql)
            self._db.commit()
        except:
            self._db.rollback()

    def crawl(self):
        html = self._driver.page_source
        soup = BeautifulSoup(html, 'lxml')
        herfs = soup.find_all(
            'a', attrs={'class': ['all aif', 'lazya all hif']})
        if len(herfs) == 0:
            return 0
        else:
            for i in range(len(herfs)):
                url_ = herfs[i].get('href')
                self.to_database(i + self._ids, url_)
            self._db.close()
            self._ids = self._ids + i
            return i

    def commit(self):
        self.scroll()
        self.crawl()
        print(str(self._url) + ' Done!')


ids = 0
regions = [12, 11, 7, 10, 9]
realms = [6, 1, 5, 4]
for i in range(5):
    for j in range(4):
        for k in range(4):
            test = OverallHrefs(regions[i], realms[j], k + 1, ids)
            test.commit()
            ids = test.get_ids()
