from Crawler import Crawler
import pandas as pd
from tqdm import tqdm

ciwei_article_list = pd.read_csv('./Data/ciwei_article_list.csv')
ciwei = pd.DataFrame(
    columns=['title', 'text', 'url'],
)

for i in tqdm(range(len(ciwei_article_list))):
    url = ciwei_article_list[['url']].iloc[i, 0]
    crawler = Crawler(url)
    text = crawler.get_text()
    title = crawler.get_title()
    ir = pd.DataFrame(
        columns=['title', 'text', 'url'],
        data = [[title, text, url]]
    )
    ciwei = ciwei.append(ir, ignore_index=True)

ciwei.insert(0, 'time', ciwei_article_list['publish_time'])
ciwei.to_csv('./Results/ciwei.csv')
print('CiWei Done!')

# url = 'http://mp.weixin.qq.com/s?__biz=MzI1MDg3NDQ5Nw==&mid=2247496386&idx=2&sn=ffd1b4305e0b5f2052d7883012ead4c7&chksm=e9f92d2cde8ea43ad9805464d781d48bf8a65bf2c7b120dd9dd7a2bd07c0f6526618ec62edd3&scene=27#wechat_redirect'
# crawler = Crawler(url)
# text = crawler.get_title()
# print(text)

jinri_article_list = pd.read_csv('./Data/jinri_article_list.csv')
jinri = pd.DataFrame(
    columns=['title', 'text', 'url'],
)

for i in tqdm(range(len(jinri_article_list))):
    url = jinri_article_list[['url']].iloc[i, 0]
    crawler = Crawler(url)
    text = crawler.get_text()
    title = crawler.get_title()
    ir = pd.DataFrame(
        columns=['title', 'text', 'url'],
        data = [[title, text, url]]
    )
    jinri = jinri.append(ir, ignore_index=True)

jinri.insert(0, 'time', jinri_article_list['publish_time'])
jinri.to_csv('./Results/jinri.csv')
print('JinRi Done!')
