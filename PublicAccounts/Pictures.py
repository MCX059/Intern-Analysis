import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
import os
from tqdm import tqdm


url = 'https://mp.weixin.qq.com/s?__biz=MzU2OTE0NzQ5OA==&mid=2247529513&idx=1&sn=47f02eb579797575c1fe58166ea2ce76&chksm=fc811d2ccbf6943a4064338e36476ee1b40c1c2dba26c59b9cc3d83a104849e47467780d6935&scene=27#wechat_redirect'


def get_links(url: str) -> list:
    """该函数获取当前网页中所有图片链接

    Arguments:
        url {str} -- 网页url

    Returns:
        list -- 所有图片链接
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
    }
    request = urllib.request.Request(url=url, headers=headers)
    response = urllib.request.urlopen(request).read()
    html = response.decode()
    soup = BeautifulSoup(html, 'lxml')
    pic_list = soup.find_all('img')
    pic_links_tmp = []
    pro_tmp = []
    for item in pic_list:
        tmp = item.get('data-src')
        pic_links_tmp.append(tmp)

    pic_links = [x for x in pic_links_tmp if x != None]
    return pic_links


def download_pics(pic_links: list, id_: int) -> int:
    """该函数下载给定列表链接的图片

    Arguments:
        pic_links {list} -- 包含图片链接的列表
        id_ {int} -- 当前网页序号

    Returns:
        int -- 该网页爬到的图片数
    """
    path = './PublicAccounts/Results/Pictures/ciwei/' + str(id_)
    if len(pic_links) >= 0:
        if not os.path.exists(path):
            os.mkdir(path)
        for link in range(len(pic_links)):
            resp = urllib.request.urlopen(pic_links[link])
            urllib.request.urlretrieve(
                pic_links[link], path + '/' + str(link) + '.png')
    return link


ciwei_article_list = pd.read_csv('PublicAccounts/Data/ciwei_article_list.csv')
for i in tqdm(range(len(ciwei_article_list))):
    url = ciwei_article_list[['url']].iloc[i, 0]
    pic_list = get_links(url)
    download_pics(pic_list, i)
