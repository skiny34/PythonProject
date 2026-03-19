import time
import requests
import hashlib
import pandas as pd
import json
from datetime import datetime
from pyquery import PyQuery as pq
from bs4 import BeautifulSoup
import random
import os


def header():
    return {
        "user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        "cookie": '_ga=GA1.1.2064334317.1739942185; rd_v1.i18n_redirected=zh; rd_v1.theme=light; rd_v1.uuid=1430729d-51b6-4e78-8b33-84702139ac63; rd_v1.auth._token.local1=false; rd_v1.auth._token_expiration.local1=false; rd_v1.auth.strategy=local3; rd_v1.auth._token.local3=gmh2k8ba6f-144401-04-2vtbayde7w-1751245643254; rd_v1.auth._token_expiration.local3=1753837652983; _ga_TXPS04VGH2=GS2.1.s1751331263$o11$g1$t1751331489$j51$l0$h0'
    }

def get_data_list(url_list):
    proxy = {"http": "http://192.168.1.24:7890", "https": "http://192.168.1.24:7890"}
    for url in url_list:
        try:
            html = requests.get(url, proxies=proxy, headers=header(), timeout=60)
            print(f"正在处理 {url}")
            soup = BeautifulSoup(html.text, 'html.parser')
            item = {}
            name_tag = soup.select_one('h1.name')
            item['name'] = name_tag.get_text(strip=True) if name_tag else ''
            item['url'] = ''
            item['doc_url'] = ''
            for a in soup.select('div.links a'):
                href = a.get('href', '')
                label = a.get_text(strip=True).lower()
                if 'doc' in label or 'docs' in href:
                    item['doc_url'] = href
                elif any(keyword in href for keyword in ['twitter', 'linkedin', 'x.com']):
                    continue  # 跳过社交媒体链接
                elif href.startswith('http') and not item['url']:
                    item['url'] = href
            for i in soup.find_all('div', class_="detail_l col-sm-12 col-md-8 col-lg-9 col-xl-9 col-12"):
                item['simple_describe'] = i.find('p', class_="detail_intro").text.replace('\n', '').strip()
                item['describe'] = i.find('p', class_="pt-4").text.replace('\n', '').strip()
                item['label'] = i.find('div', class_="item d-flex flex-row flex-wrap align-center tag_item").text.replace('标签:', '').strip()
            item['found_year'] = ''
            for div in soup.select('div.item'):
                if '成立时间' in div.get_text():
                    item['found_year'] = div.select_one('span.info_text').get_text(strip=True)

            df = pd.DataFrame([item])
            file_exists = os.path.isfile('output.csv')
            df.to_csv('output.csv', mode='a', index=False, encoding='utf-8', header=not file_exists)
            print(f"✅ {item['name']}项目写入完成")


            time.sleep(random.uniform(3.0, 6.0))
        except Exception as e:
            print(f"❌ 处理 {url} 时出错: {e}")

with open("url_list.txt", encoding='utf-8') as f:
    urls = [line.strip() for line in f if line.strip()]
    get_data_list(urls)