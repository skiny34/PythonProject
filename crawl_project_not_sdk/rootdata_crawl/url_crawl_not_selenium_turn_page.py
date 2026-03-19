import requests
import time
import random
import pandas as pd
import os
import re
from urllib.parse import urljoin
from lxml import etree


class RootDataCrawler:
    def __init__(self, base_url, proxy=None, output_file="projects.csv"):
        self.base_url = base_url
        self.proxy = proxy
        self.output_file = output_file
        self.headers = {
            'user-agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/110.0.0.0 Safari/537.36'
            )
        }
        self.projects = []  # 保存 (name, url)

    def _get_page(self, url):
        try:
            response = requests.get(url, headers=self.headers, proxies=self.proxy, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"❌ 请求失败: {e}")
            return None

    def crawl(self):
        page = 1
        while True:
            page_url = f"{self.base_url}&page={page}"
            print(f"\n=== 正在抓取第 {page} 页: {page_url} ===")

            html_content = self._get_page(page_url)
            if not html_content:
                break

            tree = etree.HTML(html_content)
            titles = tree.xpath('//a[@class="list_name animation_underline"]/text()')
            urls = tree.xpath('//a[@class="list_name animation_underline"]/@href')

            if not urls:
                print("⚠️ 没有解析到项目，可能已到最后一页")
                break

            new_items = 0
            for t, u in zip(titles, urls):
                full_url = urljoin("https://www.rootdata.com", u)
                record = (t.strip(), full_url)
                if record not in self.projects:
                    self.projects.append(record)
                    new_items += 1

            print(f"✅ 第 {page} 页发现 {new_items} 个新项目，总计 {len(self.projects)}")

            # 边爬边保存
            df = pd.DataFrame(self.projects, columns=["project_name", "project_url"])
            df.to_csv(self.output_file, index=False, encoding="utf-8-sig")

            if new_items == 0:
                break

            page += 1
            time.sleep(random.uniform(3, 8))  # 随机等待

        print(f"\n🎉 爬取完成，共 {len(self.projects)} 个项目，结果已保存到 {self.output_file}")


if __name__ == "__main__":
    PROXY = {"http": "http://192.168.1.24:7988", "https": "http://192.168.1.24:7988"}
    OUTPUT_DIR = "D:/PythonProject/test/project11"

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 读取 Excel 文件
    df = pd.read_excel("rootdata.xlsx")

    for index, row in df.iterrows():
        url = row["url"]
        url_suffix = url.split("/")[-1]

        # 提取等号后的部分，并将 %20 替换为空格
        chain = re.sub(r'%20', ' ', url_suffix.split("=")[1])
        print(f"\n====== 开始爬取 {chain} ======")

        output_file = os.path.join(OUTPUT_DIR, f"{chain}_projects.csv")
        crawler = RootDataCrawler(base_url=url, proxy=PROXY, output_file=output_file)
        crawler.crawl()
